//! How lake code calls another member, or another lake's leader. Lake code reaches the others only
//! through here: a node sends the call over HTTP (`cluster::Http`), and a build with no doors reaches
//! no member at all (`Nobody`), so the browser build names no socket and no door.
use anyhow::{anyhow, Context, Result};
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;

/// The method a call makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verb {
    Get,
    Post,
    Delete,
}

/// One request to a member: a path under its address, and what it carries.
pub struct Call {
    pub verb: Verb,
    pub path: String,
    pub body: Option<Bytes>,
    /// Says the body is JSON (`Content-Type: application/json`).
    pub json: bool,
    pub timeout: Option<Duration>,
    /// A key to send instead of the member's own (a branch's key, another server's).
    pub bearer: Option<String>,
    /// The user a change is for, sent to this lake's own leader only (`auth::AS_USER`).
    pub user: Option<String>,
}

impl Call {
    fn new(verb: Verb, path: impl Into<String>, body: Option<Bytes>) -> Call {
        Call { verb, path: path.into(), body, json: false, timeout: None, bearer: None, user: None }
    }

    /// `GET path`; a query string goes in `path`.
    pub fn get(path: impl Into<String>) -> Call { Call::new(Verb::Get, path, None) }

    /// `DELETE path`.
    pub fn delete(path: impl Into<String>) -> Call { Call::new(Verb::Delete, path, None) }

    /// `POST path` with `body` as it is: no content type is said.
    pub fn post(path: impl Into<String>, body: impl Into<Bytes>) -> Call { Call::new(Verb::Post, path, Some(body.into())) }

    /// `POST path` with `b` encoded as JSON.
    pub fn json<T: Serialize + ?Sized>(path: impl Into<String>, b: &T) -> Result<Call> {
        Ok(Call { json: true, ..Call::post(path, serde_json::to_vec(b)?) })
    }

    /// Says the body is JSON, for a body that was encoded already.
    pub fn as_json(self) -> Call { Call { json: true, ..self } }

    pub fn timeout(self, d: Duration) -> Call { Call { timeout: Some(d), ..self } }

    pub fn bearer(self, key: String) -> Call { Call { bearer: Some(key), ..self } }

    pub fn as_user(self, user: String) -> Call { Call { user: Some(user), ..self } }
}

/// A member's answer: its status and SQLSTATE, and its body as it comes.
pub struct Reply {
    pub status: u16,
    pub sqlstate: Option<String>,
    /// The transport's own error for an answer that isn't a success (`checked` returns it).
    failed: Option<anyhow::Error>,
    body: BoxStream<'static, Result<Bytes>>,
}

impl Reply {
    /// An answer as a transport made it: `failed` is its own error when the status isn't a success.
    pub fn new(status: u16, sqlstate: Option<String>, failed: Option<anyhow::Error>, body: BoxStream<'static, Result<Bytes>>) -> Reply {
        Reply { status, sqlstate, failed, body }
    }

    /// Was the answer a success (2xx)?
    pub fn ok(&self) -> bool { (200..300).contains(&self.status) }

    /// The answer itself, or the transport's error for a status that isn't a success.
    pub fn checked(mut self) -> Result<Reply> {
        match self.failed.take() {
            Some(e) => Err(e),
            None => Ok(self),
        }
    }

    /// The whole body.
    pub async fn bytes(self) -> Result<Bytes> {
        let mut body = self.body;
        let mut all = BytesMut::new();
        while let Some(part) = body.next().await {
            all.extend_from_slice(&part?);
        }
        Ok(all.freeze())
    }

    /// The whole body as text (a word a refusal says, read as UTF-8).
    pub async fn text(self) -> Result<String> { Ok(String::from_utf8_lossy(&self.bytes().await?).into_owned()) }

    /// The whole body as JSON.
    pub async fn json<T: DeserializeOwned>(self) -> Result<T> {
        let bytes = self.bytes().await.context("error decoding response body")?;
        serde_json::from_slice(&bytes).context("error decoding response body")
    }

    /// The body a part at a time, as it arrives (a streamed reply).
    pub fn stream(self) -> BoxStream<'static, Result<Bytes>> { self.body }
}

/// How a member reaches another member, or another lake's leader.
pub trait Dial: Send + Sync {
    /// The URL `to` is reached at: a "host:port" as members name themselves, or a URL with its scheme, as given.
    fn url(&self, to: &str) -> String;

    /// Sends `call` to `to`. An error only when no answer came; a refusal is a `Reply`.
    fn call<'a>(&'a self, to: &'a str, call: Call) -> BoxFuture<'a, Result<Reply>>;

    /// Is `e` a call that got no answer (no connection, or none in time)?
    fn unreached(&self, e: &anyhow::Error) -> bool;
}

/// The dial this build uses: a node's HTTP, or none.
#[cfg(feature = "doors")]
pub fn dial() -> &'static dyn Dial { &crate::cluster::Http }

#[cfg(not(feature = "doors"))]
pub fn dial() -> &'static dyn Dial { &Nobody }

/// The URL `to` is reached at.
pub fn url(to: &str) -> String { dial().url(to) }

/// Sends `c` to `to` (see `Dial::call`).
pub async fn call(to: &str, c: Call) -> Result<Reply> { dial().call(to, c).await }

/// Is `e` a call that got no answer?
pub fn unreached(e: &anyhow::Error) -> bool { dial().unreached(e) }

/// The dial of a build with no doors: it reaches no member, and says so by name.
#[cfg_attr(feature = "doors", allow(dead_code))]
struct Nobody;

impl Dial for Nobody {
    fn url(&self, to: &str) -> String { to.to_string() }

    fn call<'a>(&'a self, to: &'a str, _call: Call) -> BoxFuture<'a, Result<Reply>> {
        let refused: Result<Reply> = Err(anyhow!("{to}: this form reaches no other member"));
        Box::pin(async move { refused })
    }

    fn unreached(&self, _e: &anyhow::Error) -> bool { false }
}

// The nodes' key: the key a lake's nodes call each other with when no admin token is set. It is the
// node's, not the process's, so the next node in a process (another lake) is never sent it.
crate::per_node!(static NODE_KEY: std::sync::RwLock<Option<String>> = std::sync::RwLock::new(None));

/// The key this node calls the others with: `PONDRA_NODE_KEY` as set, else the lake's own, once known.
pub fn node_key() -> Option<String> {
    crate::env::get("PONDRA_NODE_KEY").or_else(|| NODE_KEY.read().unwrap_or_else(|e| e.into_inner()).clone())
}

/// This node learned the lake's key for its nodes (`node.rs`).
pub fn set_node_key(k: String) { *NODE_KEY.write().unwrap_or_else(|e| e.into_inner()) = Some(k); }

/// The nodes' key in hand before this node calls its leader for a caller (`write::on_node_as`): a
/// follower that just started may sign a user in, its catalog holding the users, a moment before the
/// loop that reads the key (`cli.rs`) has it, and a call sent without it is refused as unsigned (CI's
/// `harness.py guarded`, "a change a follower forwards…"). It waits 10 s at most, and not at all for a
/// key sealed by a master key this node doesn't have: waiting won't help that one.
pub async fn signed(lake: &crate::store::Lake) {
    if crate::env::is_set("PONDRA_TOKEN") || crate::env::is_set("PONDRA_ADMIN_TOKEN") || node_key().is_some() {
        return;
    }
    for _ in 0..40 {
        match crate::users::node_key(lake).await {
            Ok(k) => return set_node_key(k),
            Err(e) if format!("{e:#}").contains("master key") => return,
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_build_with_no_doors_refuses_a_call_to_another_member_by_name() {
        let e = futures::executor::block_on(Nobody.call("node-2:7000", Call::get("/cluster/visible"))).err().expect("refused");
        assert_eq!(e.to_string(), "node-2:7000: this form reaches no other member");
        assert!(!Nobody.unreached(&e));
    }
}
