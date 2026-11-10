//! The sim's network (redesign step 3, piece 3e: l7-wip/step3-sim-plan.md): its members by name, each answered by its
//! own router in this process, as its HTTP door would answer it. A member's calls meet its faults (`Op::Call`) on the
//! way out, and the called member's on the way in, so a frozen member neither sends nor answers until it is thawed. A
//! link between two members can be cut both ways (a partition), and a member that is down answers nothing. Each call
//! a member answers runs on a task of the sim's, which the member's kill ends, as its process's end would.
use super::faults::{Faults, Op, Verdict};
use crate::dial::{Call, Dial, Reply, Verb};
use anyhow::{anyhow, Result};
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

/// What answers a member's address: its router while it runs (none while it starts or is down), its faults, and the calls
/// it is answering, which its kill ends.
struct Door {
    router: Arc<Mutex<Option<axum::Router>>>,
    faults: Arc<Faults>,
    answering: Mutex<Vec<tokio::task::AbortHandle>>,
    /// The address its peers see a call from it come from: one of this machine's, as the harness's members are on one
    /// machine, so the nodes' key in the clear is taken from it (`users::nodes_key`, `tls::Peer::own`).
    peer: std::net::SocketAddr,
}

/// A sim cluster's network.
#[derive(Default)]
pub struct Net {
    doors: Mutex<HashMap<String, Arc<Door>>>,
    /// The links cut, each both ways.
    cut: Mutex<HashSet<(String, String)>>,
}

impl Net {
    /// A network with no member on it.
    pub fn new() -> Arc<Net> { Arc::default() }

    /// Member `name` is reached at its name from now on: by `router` while it holds one, its calls meeting `faults`.
    pub fn join(&self, name: &str, router: Arc<Mutex<Option<axum::Router>>>, faults: Arc<Faults>) {
        let mut doors = lock(&self.doors);
        let peer = std::net::SocketAddr::from(([127, 0, 0, doors.len() as u8 + 2], 7000));
        doors.insert(name.to_string(), Arc::new(Door { router, faults, answering: Default::default(), peer }));
    }

    /// Member `name` is down: the calls it is answering end, unanswered, and nothing reaches it until it joins again.
    pub fn down(&self, name: &str) {
        if let Some(door) = lock(&self.doors).remove(name) {
            lock(&door.answering).drain(..).for_each(|a| a.abort());
        }
    }

    /// Cuts the link between `a` and `b` (`true`), both ways, or heals it (`false`).
    pub fn cut(&self, a: &str, b: &str, cut: bool) {
        let mut links = lock(&self.cut);
        for link in [(a.to_string(), b.to_string()), (b.to_string(), a.to_string())] {
            if cut {
                links.insert(link);
            } else {
                links.remove(&link);
            }
        }
    }

    /// Heals every link.
    pub fn heal(&self) { lock(&self.cut).clear(); }

    /// The dial member `me` calls the others with (`NodeState::dial`).
    pub fn dial(self: &Arc<Self>, me: &str) -> Arc<dyn Dial> { Arc::new(Dialer { net: self.clone(), me: me.to_string() }) }

    /// `c` from `me` (node `node`) to member `to`, answered by its router, or no answer.
    async fn call(&self, me: &str, node: &crate::node_state::NodeState, to: &str, c: Call) -> Result<Reply> {
        let none = |why: &str| crate::dial::no_answer(format!("{to}: {why} (the sim)"));
        let (mine, theirs) = {
            let doors = lock(&self.doors);
            (doors.get(me).cloned(), doors.get(to).cloned())
        };
        let shown = format!("{me} -> {to}{}", c.path);
        let lost = match mine.as_ref().map(|d| d.faults.clone()) {
            Some(f) => match f.meet(Op::Call, &shown).await {
                Verdict::Fail => return Err(none("its call failed")),
                v => v == Verdict::Lose,
            },
            None => false, // (a call made as a member starts, before it joins)
        };
        if lock(&self.cut).contains(&(me.to_string(), to.to_string())) {
            return Err(none("the link is cut"));
        }
        let Some(door) = theirs else { return Err(none("no member answers there")) };
        if door.faults.meet(Op::Call, &shown).await == Verdict::Fail {
            return Err(none("its answer failed"));
        }
        let Some(router) = lock(&door.router).clone() else { return Err(none("it is starting, or stopped")) };
        let from = mine.map_or(std::net::SocketAddr::from(([127, 0, 0, 1], 7000)), |d| d.peer);
        let req = request(node, from, c.verb, &c.path, c.json, c.bearer, c.user, c.headers, c.body)?;
        let answered = crate::spawns::untracked(async move {
            use tower::ServiceExt;
            router.oneshot(req).await.unwrap_or_else(|never| match never {})
        });
        lock(&door.answering).push(answered.abort_handle());
        let answer = async { answered.await.map_err(|_| none("it went down while answering")) };
        let res = match c.timeout {
            Some(limit) => tokio::time::timeout(limit, answer).await.map_err(|_| none(&format!("no answer in {} s", limit.as_secs_f64())))??,
            None => answer.await?,
        };
        if lost {
            return Err(none("its answer was lost"));
        }
        let status = res.status().as_u16();
        let sqlstate = res.headers().get("x-pondra-sqlstate").and_then(|v| v.to_str().ok()).map(str::to_string);
        let headers = res.headers().clone();
        let failed = (!res.status().is_success()).then(|| anyhow!("HTTP status {} for {shown}", res.status()));
        let body = res.into_body().into_data_stream().map_err(anyhow::Error::from).boxed();
        Ok(Reply::new(status, sqlstate, failed, body).with_headers(headers))
    }
}

/// A member's request as a node's HTTP client would send it (`cluster::send_call`, `cluster::client`): signed with its
/// token, saying who calls, from its own address.
#[allow(clippy::too_many_arguments)]
fn request(
    node: &crate::node_state::NodeState,
    from: std::net::SocketAddr,
    verb: Verb,
    path: &str,
    json: bool,
    bearer: Option<String>,
    user: Option<String>,
    headers: Vec<(String, String)>,
    body: Option<bytes::Bytes>,
) -> Result<http::Request<axum::body::Body>> {
    let method = match verb {
        Verb::Get => "GET",
        Verb::Post => "POST",
        Verb::Delete => "DELETE",
    };
    let mut req = http::Request::builder().method(method).uri(path);
    if let Some(token) = bearer.or_else(|| node.token()) {
        req = req.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
    }
    for (k, v) in crate::format::headers() {
        req = req.header(k, v);
    }
    if json {
        req = req.header(http::header::CONTENT_TYPE, "application/json");
    }
    if let Some(user) = user {
        req = req.header(crate::auth::AS_USER, user);
    }
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let mut req = req.body(axum::body::Body::from(body.unwrap_or_default()))?;
    req.extensions_mut().insert(axum::extract::ConnectInfo(crate::tls::Peer { addr: from, tls: false, signed: false, local: true }));
    Ok(req)
}

/// A member's dial: the sim's network, from its name.
struct Dialer {
    net: Arc<Net>,
    me: String,
}

impl Dial for Dialer {
    fn url(&self, to: &str) -> String { to.to_string() }

    fn call<'a>(&'a self, node: &'a crate::node_state::NodeState, to: &'a str, c: Call) -> BoxFuture<'a, Result<Reply>> { Box::pin(self.net.call(&self.me, node, to, c)) }

    fn unreached(&self, e: &anyhow::Error) -> bool { crate::dial::is_no_answer(e) }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> { m.lock().unwrap_or_else(PoisonError::into_inner) }
