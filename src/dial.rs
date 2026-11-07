//! How lake code calls another member, or another lake's leader. Lake code reaches the others only
//! through here: a node sends the call over HTTP (`cluster::Http`), and a build with no doors reaches
//! only the node a page joined, over `fetch` with the page's sign-in (`Paged`), so the browser build names no socket and no door.
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

/// The dial this build uses: a node's HTTP, or a page's (`Paged`) when it has no doors.
#[cfg(feature = "doors")]
pub fn dial() -> &'static dyn Dial { &crate::cluster::Http }

#[cfg(not(feature = "doors"))]
pub fn dial() -> &'static dyn Dial { &Paged }

/// The URL `to` is reached at.
pub fn url(to: &str) -> String { dial().url(to) }

/// Sends `c` to `to` (see `Dial::call`).
pub async fn call(to: &str, c: Call) -> Result<Reply> { dial().call(to, c).await }

/// Is `e` a call that got no answer?
pub fn unreached(e: &anyhow::Error) -> bool { dial().unreached(e) }

// The joined page's node and its sign-in, as `node::open_joined` sets them (`set_page`): the node's origin, as `members::Joined`
// holds it, and the `Authorization` its calls carry. Set back with the rest of the node's statics when the page's member ends.
crate::per_node!(static PAGE_NODE: std::sync::Mutex<Option<(String, String)>> = std::sync::Mutex::new(None));

/// A joined page's node is `node` (its origin, no trailing `/`), and the page's calls to it carry `auth`.
pub fn set_page(node: String, auth: String) { *PAGE_NODE.lock().unwrap_or_else(|e| e.into_inner()) = Some((node, auth)); }

/// The node a joined page follows and its sign-in, as `set_page` set them.
pub fn page_node() -> Option<(String, String)> { PAGE_NODE.lock().unwrap_or_else(|e| e.into_inner()).clone() }

// The slug of the user a joined page follows for, as its node's `/whoami` says it (`node::open_joined`): the folder its staged
// rows go in, `stage/<slug>/` (`write::staged`). None until the node has said it.
crate::per_node!(static PAGE_SLUG: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None));

/// The slug of a joined page's user, as its node said it (`node::open_joined`).
pub fn set_page_slug(slug: Option<String>) { *PAGE_SLUG.lock().unwrap_or_else(|e| e.into_inner()) = slug; }

/// The slug a joined page stages its rows under, if its node gave one.
pub fn page_slug() -> Option<String> { PAGE_SLUG.lock().unwrap_or_else(|e| e.into_inner()).clone() }

/// The dial of a build with no doors, which is a page's: it reaches its joined node over `fetch` with the page's sign-in, and
/// no other member. Unset (no page joined a node), it reaches no member at all, and says so by name.
#[cfg_attr(feature = "doors", allow(dead_code))]
pub struct Paged;

impl Dial for Paged {
    fn url(&self, to: &str) -> String { to.to_string() }

    fn call<'a>(&'a self, to: &'a str, c: Call) -> BoxFuture<'a, Result<Reply>> {
        let joined = PAGE_NODE.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let Some((node, auth)) = joined else {
            let refused: Result<Reply> = Err(anyhow!("{to}: this form reaches no other member"));
            return Box::pin(async move { refused });
        };
        Box::pin(async move { page_call(&node, &auth, to, c).await })
    }

    fn unreached(&self, e: &anyhow::Error) -> bool { e.downcast_ref::<NoAnswer>().is_some() }
}

/// A call a page sent that got no answer: its send failed, or it ran past its time limit (`Paged::unreached`).
#[derive(Debug)]
struct NoAnswer(String);

impl std::fmt::Display for NoAnswer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
}

impl std::error::Error for NoAnswer {}

/// One call of a page to its joined node (`Paged::call`), as `cluster::send_call` sends a node's, except for the sign-in: the
/// page's own, and no key or user of the nodes'. A refusal of the call is an error before anything is sent.
async fn page_call(node: &str, auth: &str, to: &str, c: Call) -> Result<Reply> {
    anyhow::ensure!(to.trim_end_matches('/') == node, "{to}: a joined page reaches its node ({node}) and no other member");
    anyhow::ensure!(c.path.starts_with('/'), "{}: a call's path starts with /", c.path);
    anyhow::ensure!(c.bearer.is_none() && c.user.is_none(), "a joined page's calls carry its own sign-in: a key or a user of the nodes' is refused");
    let verb = match c.verb {
        Verb::Get => reqwest::Method::GET,
        Verb::Post => reqwest::Method::POST,
        Verb::Delete => reqwest::Method::DELETE,
    };
    let url = format!("{node}{}", c.path);
    let mut req = crate::net::process().client(crate::net::Policy::ENDPOINT).request(verb, url.as_str()).header(reqwest::header::AUTHORIZATION, auth);
    if c.json {
        req = req.header(reqwest::header::CONTENT_TYPE, "application/json");
    }
    if let Some(body) = c.body {
        req = req.body(body);
    }
    let sent = crate::fetch::send(req);
    let answer = match c.timeout {
        Some(limit) => tokio::time::timeout(limit, sent).await.map_err(|_| anyhow::Error::new(NoAnswer(format!("{url}: no answer in {} s", limit.as_secs_f64()))))?,
        None => sent.await,
    };
    let answer = answer.map_err(|e| anyhow::Error::new(NoAnswer(format!("{url}: {e}"))))?;
    let status = answer.status().as_u16();
    let sqlstate = answer.headers().get("x-pondra-sqlstate").and_then(|v| v.to_str().ok()).map(str::to_string);
    let failed = answer.error_for_status_ref().map(anyhow::Error::from);
    let body = futures::TryStreamExt::map_err(answer.bytes_stream(), anyhow::Error::from);
    Ok(Reply::new(status, sqlstate, failed, body.boxed()))
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

    /// One test, as the page's node is one static: unset, a call is refused by name; set, a call goes only to the node and
    /// only with the page's own sign-in, and a call that gets no answer is `unreached`. No request is sent: each refusal is
    /// made before the send.
    #[test]
    fn a_page_reaches_its_joined_node_alone_and_refuses_every_other_call_by_name() {
        // (a Tokio runtime, as the program's: a call's time limit is a Tokio timer)
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
        let call = |to: &str, c: Call| rt.block_on(Paged.call(to, c));
        let e = call("node-2:7000", Call::get("/cluster/visible")).err().expect("refused while no node is joined");
        assert_eq!(e.to_string(), "node-2:7000: this form reaches no other member");
        assert!(!Paged.unreached(&e));

        set_page("https://n:7000".to_string(), "Bearer ps_test".to_string());
        let other = call("https://other:7000", Call::get("/cluster/visible")).err().expect("another member is refused");
        assert!(other.to_string().contains("reaches its node (https://n:7000) and no other member"), "{other:#}");
        let bare = call("https://n:7000/", Call::get("no-slash")).err().expect("a path starts with /");
        assert!(bare.to_string().contains("starts with /"), "{bare:#}");
        let keyed = call("https://n:7000", Call::get("/x").bearer("nk_key".into())).err().expect("a node's key is refused");
        assert!(keyed.to_string().contains("own sign-in"), "{keyed:#}");
        let as_user = call("https://n:7000", Call::get("/x").as_user("someone".into())).err().expect("a user of the nodes' is refused");
        assert!(as_user.to_string().contains("own sign-in"), "{as_user:#}");
        // (a send to a port nothing listens on is no answer: `unreached`, which a member's retry relies on)
        set_page("http://127.0.0.1:1".to_string(), "Bearer ps_test".to_string());
        let gone = call("http://127.0.0.1:1", Call::get("/cluster/visible").timeout(Duration::from_secs(5))).err().expect("no node answers");
        assert!(Paged.unreached(&gone), "{gone:#}");
        *PAGE_NODE.lock().unwrap_or_else(|e| e.into_inner()) = None; // (as `reset` sets it back)
    }
}
