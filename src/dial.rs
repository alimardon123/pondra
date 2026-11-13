//! How lake code calls another member, or another lake's leader. Lake code reaches the others only
//! through here, by the node's own dial (`NodeState::dial`, which its host gives it): a node sends the call over HTTP
//! (`cluster::Http`), a build with no doors reaches only the node a page joined, over `fetch` with the page's sign-in
//! (`Paged`, `Joined`), so the browser build names no socket and no door, and the sim's members call each other in its
//! process (redesign step 3, piece 3c: l7-wip/step3-sim-plan.md).
use anyhow::{anyhow, Result};
use futures::future::BoxFuture;
use futures::StreamExt;
use std::time::Duration;

pub use crate::calls::{Call, Dial, Reply, Verb};

/// The dial a node of this build has unless its host gives it another (`NodeState::dial`): HTTP, or a page's (`Paged`)
/// when the build has no doors. The hosts register it (`hosted`, `calls::Dials`).
#[cfg(feature = "doors")]
pub fn native() -> std::sync::Arc<dyn Dial> { std::sync::Arc::new(crate::cluster::Http) }

#[cfg(not(feature = "doors"))]
pub fn native() -> std::sync::Arc<dyn Dial> { std::sync::Arc::new(Paged) }

/// The URL `to` is reached at, by node `node`'s dial.
pub fn url(node: &crate::node_state::NodeState, to: &str) -> String { node.dial.url(to) }

/// Sends `c` to `to` from node `node`, by its dial (see `Dial::call`).
pub async fn call(node: &crate::node_state::NodeState, to: &str, c: Call) -> Result<Reply> { node.dial.call(node, to, c).await }

/// Is `e` a call of node `node`'s that got no answer?
pub fn unreached(node: &crate::node_state::NodeState, e: &anyhow::Error) -> bool { node.dial.unreached(e) }

/// The dial of a build with no doors, which is a page's: it reaches no member, and says so by name. A joined page reaches its
/// node with its own dial (`Joined`), from what its member holds (`members::Joined`), and no other way.
#[cfg_attr(feature = "doors", allow(dead_code))]
pub struct Paged;

impl Dial for Paged {
    fn url(&self, to: &str) -> String { to.to_string() }

    fn call<'a>(&'a self, _: &'a crate::node_state::NodeState, to: &'a str, _: Call) -> BoxFuture<'a, Result<Reply>> {
        Box::pin(async move { Err(anyhow!("{to}: this form reaches no other member")) })
    }

    fn unreached(&self, e: &anyhow::Error) -> bool { e.downcast_ref::<NoAnswer>().is_some() }
}

/// A joined page's dial: its node over `fetch`, with the page's own sign-in, and no other member. A page dials its node with it
/// in either build (`write::stage_insert`, `page_records`): a node's own dial (`cluster::Http`) carries the nodes' key, which a
/// page never holds.
pub struct Joined<'a> {
    /// The node's origin, with no trailing `/` (`members::Joined::node`).
    pub node: &'a str,
    /// The `Authorization` its calls carry (`members::Joined::auth`).
    pub auth: &'a str,
}

impl Dial for Joined<'_> {
    fn url(&self, to: &str) -> String { to.to_string() }

    fn call<'a>(&'a self, _: &'a crate::node_state::NodeState, to: &'a str, c: Call) -> BoxFuture<'a, Result<Reply>> { Box::pin(page_call(self.node, self.auth, to, c)) }

    fn unreached(&self, e: &anyhow::Error) -> bool { e.downcast_ref::<NoAnswer>().is_some() }
}

/// A call a page sent that got no answer: its send failed, or it ran past its time limit (`Paged::unreached`).
#[derive(Debug)]
struct NoAnswer(String);

impl std::fmt::Display for NoAnswer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
}

impl std::error::Error for NoAnswer {}

/// One call of a page to its joined node (`Joined::call`), as `cluster::send_call` sends a node's, except for the sign-in: the
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
    for (k, v) in &c.headers {
        req = req.header(k.as_str(), v.as_str());
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
    let headers = answer.headers().clone();
    let body = futures::TryStreamExt::map_err(answer.bytes_stream(), anyhow::Error::from);
    Ok(Reply::new(status, sqlstate, failed, body.boxed()).with_headers(headers))
}

/// The key node `node` calls the others with: `PONDRA_NODE_KEY` as set, else the lake's own, once known (`Calls`: the node's,
/// not the process's, so the next node in a process (another lake) is never sent it).
pub fn node_key(node: &crate::node_state::NodeState) -> Option<String> {
    crate::env::get("PONDRA_NODE_KEY").or_else(|| node.calls.key.read().unwrap_or_else(|e| e.into_inner()).clone())
}

/// This node learned the lake's key for its nodes (`node.rs`).
pub fn set_node_key(node: &crate::node_state::NodeState, k: String) { *node.calls.key.write().unwrap_or_else(|e| e.into_inner()) = Some(k); }

/// The nodes' key in hand before this node calls its leader for a caller (`write::on_node_as`): a
/// follower that just started may sign a user in, its catalog holding the users, a moment before the
/// loop that reads the key (`cli.rs`) has it, and a call sent without it is refused as unsigned (CI's
/// `harness.py guarded`, "a change a follower forwards…"). It waits 10 s at most, and not at all for a
/// key sealed by a master key this node doesn't have: waiting won't help that one.
pub async fn signed(lake: &crate::store::Lake) {
    if crate::env::is_set("PONDRA_TOKEN") || crate::env::is_set("PONDRA_ADMIN_TOKEN") || node_key(&lake.node).is_some() {
        return;
    }
    for _ in 0..40 {
        match crate::users::node_key(lake).await {
            Ok(k) => return set_node_key(&lake.node, k),
            Err(e) if format!("{e:#}").contains("master key") => return,
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The form's dial reaches no member, and says so by name; a joined page's reaches its node alone and only with the page's
    /// own sign-in, and a call of it that gets no answer is `unreached`. No request is sent: each refusal is made before the send.
    #[test]
    fn a_page_reaches_its_joined_node_alone_and_refuses_every_other_call_by_name() {
        // (a Tokio runtime, as the program's: a call's time limit is a Tokio timer)
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
        let node = crate::node_state::NodeState::unhosted();
        let e = rt.block_on(Paged.call(&node, "node-2:7000", Call::get("/cluster/visible"))).err().expect("the form's dial reaches no member");
        assert_eq!(e.to_string(), "node-2:7000: this form reaches no other member");
        assert!(!Paged.unreached(&e));

        let page = Joined { node: "https://n:7000", auth: "Bearer ps_test" };
        let call = |to: &str, c: Call| rt.block_on(page.call(&node, to, c));
        let other = call("https://other:7000", Call::get("/cluster/visible")).err().expect("another member is refused");
        assert!(other.to_string().contains("reaches its node (https://n:7000) and no other member"), "{other:#}");
        let bare = call("https://n:7000/", Call::get("no-slash")).err().expect("a path starts with /");
        assert!(bare.to_string().contains("starts with /"), "{bare:#}");
        let keyed = call("https://n:7000", Call::get("/x").bearer("nk_key".into())).err().expect("a node's key is refused");
        assert!(keyed.to_string().contains("own sign-in"), "{keyed:#}");
        let as_user = call("https://n:7000", Call::get("/x").as_user("someone".into())).err().expect("a user of the nodes' is refused");
        assert!(as_user.to_string().contains("own sign-in"), "{as_user:#}");
        // (a send to a port nothing listens on is no answer: `unreached`, which a member's retry relies on)
        let gone_page = Joined { node: "http://127.0.0.1:1", auth: "Bearer ps_test" };
        let gone = rt.block_on(gone_page.call(&node, "http://127.0.0.1:1", Call::get("/cluster/visible").timeout(Duration::from_secs(5)))).err().expect("no node answers");
        assert!(gone_page.unreached(&gone), "{gone:#}");
    }
}
