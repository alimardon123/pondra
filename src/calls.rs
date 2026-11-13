//! A call from one member to another, as lake code makes it (redesign §9: the trait is base's, a platform implements it):
//! what is sent (`Call`), what comes back (`Reply`), and the `Dial` a node sends with (`NodeState::dial`). The dials
//! themselves are the platform's: a node's HTTP (`cluster::Http`), a page's (`dial::Paged`, `dial::Joined`), the sim's
//! (`sim::net`). Lake code calls `dial::call`.
use anyhow::{Context, Result};
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
    /// Headers of its own (a follower's beat says what it holds: `cluster::said`).
    pub headers: Vec<(String, String)>,
}

impl Call {
    fn new(verb: Verb, path: impl Into<String>, body: Option<Bytes>) -> Call {
        Call { verb, path: path.into(), body, json: false, timeout: None, bearer: None, user: None, headers: vec![] }
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

    /// With header `name: value` too.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Call {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// A member's answer: its status and SQLSTATE, its headers, and its body as it comes.
pub struct Reply {
    pub status: u16,
    pub sqlstate: Option<String>,
    /// Its headers (a leader cut off from the bucket says so in one: `cluster::CUT_OFF_HEADER`).
    pub headers: http::HeaderMap,
    /// The transport's own error for an answer that isn't a success (`checked` returns it).
    failed: Option<anyhow::Error>,
    body: BoxStream<'static, Result<Bytes>>,
}

impl Reply {
    /// An answer as a transport made it: `failed` is its own error when the status isn't a success.
    pub fn new(status: u16, sqlstate: Option<String>, failed: Option<anyhow::Error>, body: BoxStream<'static, Result<Bytes>>) -> Reply {
        Reply { status, sqlstate, headers: http::HeaderMap::new(), failed, body }
    }

    /// The answer with `headers` as its own.
    pub fn with_headers(self, headers: http::HeaderMap) -> Reply { Reply { headers, ..self } }

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

    /// Sends `call` to `to`, from node `node`, which signs it with its key (`cluster::http`). An error only when no answer came;
    /// a refusal is a `Reply`.
    fn call<'a>(&'a self, node: &'a crate::node_state::NodeState, to: &'a str, call: Call) -> BoxFuture<'a, Result<Reply>>;

    /// Is `e` a call that got no answer (no connection, or none in time)?
    fn unreached(&self, e: &anyhow::Error) -> bool;
}

/// The platform's dial, as the hosts register it (`hosted`, in every form): the one a node has unless its host gives it
/// another (`NodeState::dial`).
#[derive(Default)]
pub struct Dials {
    /// A node's: HTTP between nodes, or a page's, which reaches no other member, in a build with no doors.
    pub native: Option<fn() -> std::sync::Arc<dyn Dial>>,
}

impl crate::registry::Section for Dials {
    const NAME: &'static str = "dials";
}

/// The dial a node has unless its host gives it another (`Dials::native`).
pub fn native() -> std::sync::Arc<dyn Dial> {
    let native = crate::registry::get().section::<Dials>().native.expect("the hosts register a dial (`hosted`)");
    native()
}
