//! Who a lake's member is, and who leads it: what a lake's modules ask of the form they run in, instead of the node's
//! cluster. A node answers with its `Cluster` (`cluster.rs`); a page, or a test, with `Alone`, its own leader and no
//! peers (`page-same-core.md` §7.3). The terms themselves are in `terms.rs`.
use crate::store::{Frame, Lake, Store};
use crate::terms::{self, Term};
use anyhow::{bail, Result};
use futures::{future::BoxFuture, Stream, StreamExt};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::task::JoinHandle;

/// Who this member is among its lake's members, and who leads.
pub trait Members: Send + Sync + 'static {
    /// Its address, as the others reach it and runs/history/audit name it ("" for a guest or a page).
    fn me(&self) -> &str;
    /// The term it leads or follows, fixed for its life: `n` and the leader's `addr`.
    fn leader(&self) -> &Term;
    /// It never leads, runs tasks or takes writes.
    fn reader(&self) -> bool;
    /// It keeps the lake's own records (`pondra$history`, `pondra$audit`, `pondra$runs`) and waits for the leader's view after its
    /// writes (`write::seen_here`). A node does; a joined page's member writes its rows and files through its node, which keeps them.
    fn keeps_records(&self) -> bool { true }
    /// It writes through the lake's inbox: its leader is alive but doesn't answer from here (`Cluster::guest`), so a
    /// statement's writes are sent to the leader through the bucket (`write::by_inbox`).
    fn by_inbox(&self) -> bool { false }
    /// Live members, sorted, itself among them when it leads or is alone; none while a follower hasn't heard its leader.
    fn nodes(&self) -> Vec<String>;
    /// It leads its term.
    fn is_leader(&self) -> bool { !self.reader() && self.leader().addr == self.me() }
    /// Shard `s` of a task (a feed's partition) runs here: nodes()[s % len] is me.
    fn runs_shard(&self, s: u32) -> bool {
        let nodes = self.nodes();
        nodes.iter().position(|n| *n == self.me()).is_some_and(|p| s as usize % nodes.len() == p)
    }
    /// A leader cut off from its store while another member could lead: requests are refused 57P03.
    fn cut_off(&self) -> bool { false }
    /// Leader: the newest format every live follower knows (None: no followers).
    fn least_known(&self) -> Option<u32> { None }
    /// The concrete member, for a door that needs a node's own (`cluster::of`).
    fn as_any(&self) -> &dyn std::any::Any;
    /// The node a joined page follows, and as whom (`Joined`); None for any other member.
    fn joined(&self) -> Option<&Joined> { None }
}

pub const CUT_OFF_SAYS: &str = "this node leads but can't reach the bucket: another node will lead in a moment, ask it";

/// One member alone, its own leader with no peers: a page's lake, and a test's.
pub struct Alone {
    term: Term,
}

impl Alone {
    /// Lead the lake on `store` alone: the next term with no address, claimed while no live term is ahead of it
    /// (`terms::claim_one_off`). The term is marked alive every 10 s by the returned task (`terms::mark_every`): the
    /// form aborts that task, and calls `terms::release` at its end.
    pub async fn lead(store: &Store, node: &crate::node_state::NodeState) -> Result<(Arc<Alone>, JoinHandle<()>)> {
        // (a live term refuses at once: nothing here waits for another leader to finish)
        let term = terms::claim_one_off(store, |t| {
            if t.addr.is_empty() {
                bail!("this lake is being written by a one-off writer (a backup, or another form alone): wait for it to finish, or lead another lake")
            }
            bail!("this lake is led by the node at {}, which this process can't lead beside: stop that node, or lead another lake", t.addr)
        }, &node.claims)
        .await?;
        Ok(led(store, term, &node.tasks))
    }

    /// Lead the lake on `store` alone, at once: the next term over the newest, alive or not (`terms::claim_owned`). Only for a
    /// store no other process writes (a kept page's folder, under its Web Lock). Its term is marked and released as `lead`'s.
    pub async fn lead_owned(store: &Store, node: &crate::node_state::NodeState) -> Result<(Arc<Alone>, JoinHandle<()>)> {
        Ok(led(store, terms::claim_owned(store, &node.claims).await?, &node.tasks))
    }

    /// One that claims nothing (a store with no create-if-absent put): term 0, its own leader.
    pub fn unclaimed() -> Arc<Alone> {
        Arc::new(Alone { term: Term { n: 0, addr: String::new(), lock: String::new(), bell: false } })
    }
}

/// The member led by term `term`, marked alive from now on (`terms::mark_every`): the tail of `lead` and `lead_owned`.
fn led(store: &Store, term: Term, tasks: &crate::spawns::Tasks) -> (Arc<Alone>, JoinHandle<()>) {
    let marks = terms::mark_every(tasks, store.clone(), term.n);
    (Arc::new(Alone { term }), marks)
}

/// What `follow`'s `open` answers when it has no stream: `Retry` waits and tries again (a node's every failure, a page's
/// network error), `Stop` ends the loop (a page's 401 or 403: its sign-in ended, so it may not follow any more).
pub enum Open {
    Retry,
    Stop,
}

/// Follow a lake's commit stream: a node's member and a page's both read it this way, one connection at a time. Each
/// change is held and each commit applied, and a connection that ends or fails is tried again 500 ms later. `frame` sees
/// every frame before the lake takes it (a node keeps its replica there), and `down` runs when a connection ends or
/// fails, before that wait. A change no newer than the last one is skipped: a reconnect replays some. A new term's
/// `Start` drops the changes the old leader left uncommitted, and starts the ids again; so does any `Start` after a
/// connection's first frame (a follower relays a new term's mid-stream, after the old term's frames its `recent` keeps,
/// while its own first `Start` may already name either term). With `resend`, each frame the lake took is relayed on to
/// the pages that follow it (`Catalog::relay`: a `Start` only when the term changes, and no commit or durable point
/// they have had). The loop ends when `lake` is gone (checked as a frame arrives) or `open` says `Stop`.
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (a node's `cluster::mirror` calls it; a page's join will too)
pub async fn follow(
    lake: Weak<Lake>,
    open: impl Fn() -> BoxFuture<'static, Result<crate::calls::Reply, Open>> + Send,
    mut frame: impl FnMut(&Frame, &Lake) + Send,
    mut down: impl FnMut() + Send,
    resend: bool,
) {
    let mut last = 0; // the last change we got (a reconnect replays some we have)
    let mut term = None; // the leader's term, as its `Start` says
    let mut relayed = Relayed::default(); // what was relayed (`relays`)
    let mut refused = 0; // (opens in a row refused, or that gave no frame)
    while lake.strong_count() > 0 {
        match open().await {
            Ok(r) => {
                refused += 1;
                let mut frames = Box::pin(frames(r.stream()));
                let mut first = true; // (this connection's first frame: its node's own `Start`)
                while let Some(Ok(f)) = frames.next().await {
                    refused = 0;
                    let Some(live) = lake.upgrade() else { return };
                    if let Frame::Start { term: t, .. } = &f {
                        let new = term.replace(*t) != Some(*t);
                        if new || !first {
                            last = 0;
                            live.drop_pending();
                        }
                    }
                    first = false;
                    if let Frame::Change(d) = &f {
                        if d.id <= last {
                            continue;
                        }
                        last = d.id;
                    }
                    frame(&f, &live);
                    match &f {
                        Frame::Change(d) => live.hold(d.clone()),
                        Frame::Committed(upto) => live.commit_upto(*upto),
                        Frame::Start { .. } | Frame::Durable(_) => {}
                    }
                    if resend && relays(&f, &mut relayed) {
                        live.cat.relay(f);
                    }
                }
            }
            Err(Open::Stop) => return,
            Err(Open::Retry) => refused += 1,
        }
        down();
        // (a leader that just took over serves a moment after its followers restart, and a follower's writes wait until its
        // stream has caught it up (`cluster::catch_up`): asked again every 50 ms for a second, then every half second)
        tokio::time::sleep(Duration::from_millis(if refused <= 20 { 50 } else { 500 })).await;
    }
}

/// What `follow` relayed (`resend`): the leader's term, and the last commit and durable point.
#[derive(Default)]
struct Relayed {
    term: Option<u64>,
    committed: u64,
    durable: u64,
}

/// Whether `follow` relays a frame the lake took (`resend`): every change (one at or below the last was skipped before it),
/// and a commit or durable point past the last one relayed, so a reconnect's replay isn't sent again. A `Start` only when the
/// term changes (the first is not: the follower's own feed writes its `Start`); it then goes into `recent` in order, so a page
/// reading the old term's frames there resets on it (`follow`), and the new term's points are new again.
fn relays(f: &Frame, relayed: &mut Relayed) -> bool {
    match f {
        Frame::Change(_) => true,
        Frame::Committed(upto) => {
            let fresh = *upto > relayed.committed;
            relayed.committed = relayed.committed.max(*upto);
            fresh
        }
        Frame::Durable(upto) => {
            let fresh = *upto > relayed.durable;
            relayed.durable = relayed.durable.max(*upto);
            fresh
        }
        Frame::Start { term, .. } => {
            let changed = relayed.term.replace(*term).is_some_and(|was| was != *term);
            if changed {
                (relayed.committed, relayed.durable) = (0, 0);
            }
            changed
        }
    }
}

/// A commit stream's frames, read from a body's chunks as each one completes. A chunk that fails, or a frame that doesn't
/// parse, ends the stream with that error: `follow` reads it (a node's mirror, and a joined page's follow).
pub fn frames<E>(chunks: impl Stream<Item = std::result::Result<bytes::Bytes, E>> + Send) -> impl Stream<Item = Result<Frame>> + Send
where
    E: Into<anyhow::Error> + Send + 'static,
{
    let chunks = Box::pin(chunks);
    futures::stream::try_unfold((chunks, bytes::BytesMut::new()), |(mut chunks, mut buf)| async move {
        loop {
            if let Some(f) = Frame::take(&mut buf)? {
                return anyhow::Ok(Some((f, (chunks, buf))));
            }
            match chunks.next().await {
                Some(chunk) => buf.extend_from_slice(&chunk.map_err(Into::into)?),
                None => return anyhow::Ok(None),
            }
        }
    })
}

impl Members for Alone {
    fn me(&self) -> &str { "" }
    fn leader(&self) -> &Term { &self.term }
    fn reader(&self) -> bool { false }
    fn nodes(&self) -> Vec<String> { vec!["".into()] }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

/// A page that joined a cluster (`node::open_joined`): a follower that never leads, runs tasks or keeps the lake's records. Its
/// leader is the node it joined, at term 0 (a term it claims nothing of: the cluster's terms start at 1), so it follows that
/// node's commit stream rather than a term's. It writes its own rows and files (`page::joined_part`), which its node commits.
/// It holds what the page is to its node: the sign-in its calls carry (`dial::Joined`) and its user's folder.
pub struct Joined {
    term: Term,
    auth: String,
    slug: std::sync::OnceLock<String>,
}

impl Joined {
    /// The member of the lake the cluster at `node` leads (its URL, as `https://host:7000`), followed with the sign-in `auth`
    /// (an `Authorization`: the page's own, never a key or a user of the nodes').
    pub fn new(node: &str, auth: &str) -> Joined {
        Joined { term: Term { n: 0, addr: node.trim_end_matches('/').to_string(), lock: String::new(), bell: false }, auth: auth.to_string(), slug: Default::default() }
    }

    /// The node it joined: its origin, with no trailing `/`.
    pub fn node(&self) -> &str { &self.term.addr }

    /// The `Authorization` its calls to its node carry.
    pub fn auth(&self) -> &str { &self.auth }

    /// The slug of its user, as its node's `/whoami` said it (`node::open_joined`): the folder its staged rows go in,
    /// `stage/<slug>/` (`write::stage_insert`). None until the node has said it.
    pub fn slug(&self) -> Option<&str> { self.slug.get().map(String::as_str) }

    /// Its node said its user's slug (`node::open_joined`): once, for the member's life.
    pub fn set_slug(&self, slug: String) { let _ = self.slug.set(slug); }
}

impl Members for Joined {
    fn me(&self) -> &str { "" }
    fn leader(&self) -> &Term { &self.term }
    fn reader(&self) -> bool { false }
    fn keeps_records(&self) -> bool { false }
    fn nodes(&self) -> Vec<String> { vec![] }
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn joined(&self) -> Option<&Joined> { Some(self) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn cut_off_is_cannot_connect_now() {
        assert_eq!(crate::codes::of(&anyhow::anyhow!("{}", CUT_OFF_SAYS)), "57P03");
    }

    /// `frames` reads whole frames out of chunks cut anywhere; a chunk that fails, or a frame of an unknown kind, ends it with
    /// that error.
    #[tokio::test]
    async fn frames_read_across_cut_chunks_and_a_failure_ends_them() {
        let mut wire = Frame::Start { term: 3, replicated: true }.encode().to_vec();
        wire.extend_from_slice(&Frame::Committed(9).encode());
        let cut: Vec<std::io::Result<bytes::Bytes>> = wire.chunks(3).map(|c| Ok(bytes::Bytes::copy_from_slice(c))).collect();
        let got: Vec<Result<Frame>> = frames(futures::stream::iter(cut)).collect().await;
        assert_eq!(got.len(), 2);
        assert!(matches!(got[0], Ok(Frame::Start { term: 3, replicated: true })));
        assert!(matches!(got[1], Ok(Frame::Committed(9))));

        let start = Frame::Start { term: 1, replicated: false }.encode();
        let failed = futures::stream::iter(vec![Ok(start.clone()), Err(std::io::Error::other("cut off")), Ok(start)]);
        let got: Vec<Result<Frame>> = frames(failed).collect().await;
        assert_eq!(got.len(), 2, "the failure is the last thing read");
        assert!(got[1].is_err());

        let unknown = futures::stream::iter(vec![Ok::<_, std::io::Error>(bytes::Bytes::from_static(&[1, 0, 0, 0, 9]))]);
        let got: Vec<Result<Frame>> = frames(unknown).collect().await;
        assert!(got.len() == 1 && got[0].is_err(), "a frame of an unknown kind ends it");
    }

    /// A relay sends each change, each commit or durable point past the last one sent, and a `Start` only when the term changes.
    #[test]
    fn a_relay_sends_changes_new_points_and_a_new_term() {
        let mut sent = Relayed::default();
        let change = Frame::Change(Arc::new(crate::store::Delta { id: 4, puts: vec![], deletes: vec![] }));
        assert!(!relays(&Frame::Start { term: 4, replicated: true }, &mut sent), "a follower's own feed writes its first Start");
        assert!(relays(&change, &mut sent));
        assert!(relays(&Frame::Committed(3), &mut sent));
        assert!(!relays(&Frame::Committed(3), &mut sent), "a reconnect's replay is not sent again");
        assert!(!relays(&Frame::Committed(2), &mut sent));
        assert!(relays(&Frame::Durable(3), &mut sent));
        assert!(!relays(&Frame::Durable(3), &mut sent));
        assert!(!relays(&Frame::Start { term: 4, replicated: true }, &mut sent), "a reconnect to the same term");
        assert!(relays(&Frame::Start { term: 5, replicated: true }, &mut sent), "a new term goes out mid-stream");
        assert!(relays(&Frame::Committed(3), &mut sent), "the new term's points are new");
        assert!(relays(&Frame::Durable(2), &mut sent));
    }

    #[tokio::test]
    async fn one_alone_leads_a_lake_until_it_lets_go() {
        let url = format!("memory://members-{}", uuid::Uuid::new_v4().simple());
        let store = crate::memory_lake::store(&url);
        let node = crate::node_state::NodeState::unhosted();
        let (first, marks) = Alone::lead(&store, &node).await.unwrap();
        assert_eq!(first.leader().n, 1);
        let asked = Instant::now();
        assert!(Alone::lead(&store, &node).await.is_err(), "a second is refused while the first lives");
        assert!(asked.elapsed() < Duration::from_secs(1), "the refusal waits for nothing");
        marks.abort(); // (a dropped handle aborts nothing: its mark would land again after the release)
        terms::release(&store, 1, &node.claims).await;
        let (second, marks) = Alone::lead(&store, &node).await.unwrap();
        assert_eq!(second.leader().n, 2, "let go, the next claim is the next term");
        marks.abort();
        crate::memory_lake::forget_memory(&url);
    }
}
