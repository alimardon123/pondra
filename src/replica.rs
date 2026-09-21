//! Replicated commits (`--ack replicated`): a write is acknowledged once `--replicas` nodes hold
//! it — the leader in memory, followers in a local file — and it reaches the bucket a moment
//! later. The bucket stays the source of truth; this only covers the window before a commit is
//! in it. A leader that takes over first collects, from every member, the commits they hold
//! beyond what the bucket has, and commits them again (`recover`) before it takes new writes.
use crate::store::{json, Delta, Frame, Lake};
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A follower's copies of the leader's changes that aren't in the bucket yet: in memory, and in
/// files under `dir` so they survive a restart (every leader change restarts the followers).
pub struct ReplicaLog {
    dir: PathBuf,
    st: Mutex<State>,
}

#[derive(Default)]
struct State {
    seen: u64,                           // the newest term heard of: never help an older leader again
    held: BTreeMap<u64, (u64, Bytes)>,   // change id -> (term, encoded frame)
    files: Vec<(PathBuf, u64, u64)>,     // closed files: (path, term, last change in it)
    open: Option<(std::fs::File, PathBuf, u64, u64, Instant)>, // the file being written: term, last change, opened
    run: (u64, u64),                     // the consecutive changes streamed to us, first to last
}

impl ReplicaLog {
    /// Open (and reload) this node's replica files.
    pub fn open(dir: PathBuf) -> Result<Arc<ReplicaLog>> {
        std::fs::create_dir_all(&dir)?;
        let mut st = State::default();
        for e in std::fs::read_dir(&dir)?.flatten() {
            let bytes = std::fs::read(e.path())?;
            let (mut buf, mut term, mut last) = (BytesMut::from(&bytes[..]), 0, 0);
            while buf.len() >= 8 {
                term = u64::from_le_bytes(buf.split_to(8)[..].try_into()?);
                let frame = buf.clone();
                let Ok(Some(Frame::Change(d))) = Frame::take(&mut buf) else { break }; // (a torn last write)
                let len = frame.len() - buf.len();
                last = last.max(d.id);
                keep(&mut st.held, d.id, term, frame.freeze().slice(..len));
                st.seen = st.seen.max(term);
            }
            st.files.push((e.path(), term, last));
        }
        Ok(Arc::new(ReplicaLog { dir, st: Mutex::new(st) }))
    }

    /// Keep a change from the leader of `term`, and say which consecutive run of changes we now
    /// hold (what an ack may claim: never one we missed). None if a newer leader exists: then
    /// nothing may be acknowledged (that leader may already have collected what we hold).
    pub fn hold(&self, term: u64, d: &Arc<Delta>) -> Result<Option<(u64, u64)>> {
        let mut st = self.st.lock().unwrap();
        if term < st.seen {
            return Ok(None);
        }
        st.seen = term;
        if st.open.as_ref().is_some_and(|o| o.2 != term || o.4.elapsed() > Duration::from_secs(2)) {
            let (_, path, t, last, _) = st.open.take().expect("open");
            st.files.push((path, t, last)); // a new file every 2 s, so old ones can be deleted whole
        }
        if st.open.is_none() {
            let path = self.dir.join(format!("{term:010}-{:020}.log", d.id));
            st.open = Some((std::fs::File::create(&path)?, path, term, 0, Instant::now()));
        }
        let frame = Frame::Change(d.clone()).encode();
        let o = st.open.as_mut().expect("open");
        o.0.write_all(&term.to_le_bytes())?;
        o.0.write_all(&frame)?; // (the page cache survives a process crash; `--fsync`: a power loss too)
        if std::env::var("PONDRA_FSYNC").is_ok_and(|v| v == "true") {
            o.0.sync_data()?;
        }
        o.3 = o.3.max(d.id);
        keep(&mut st.held, d.id, term, frame);
        st.run = if st.run.1 > 0 && d.id == st.run.1 + 1 { (st.run.0, d.id) } else { (d.id, d.id) };
        Ok(Some(st.run))
    }

    /// Changes up to `id` are in the bucket: drop our copies.
    pub fn prune(&self, id: u64) {
        let mut st = self.st.lock().unwrap();
        st.held = st.held.split_off(&(id + 1));
        if st.open.as_ref().is_some_and(|o| o.3 <= id) {
            let (_, path, t, last, _) = st.open.take().expect("open");
            st.files.push((path, t, last));
        }
        st.files.retain(|(path, _, last)| *last > id || std::fs::remove_file(path).is_err());
    }

    /// A leader of `term` has taken over: copies from older terms are either recovered by it or
    /// never were committed. Forget them, and never help an older leader again.
    pub fn start(&self, term: u64) {
        let mut st = self.st.lock().unwrap();
        st.seen = st.seen.max(term);
        st.held.retain(|_, (t, _)| *t >= term);
        if st.open.as_ref().is_some_and(|o| o.2 < term) {
            st.open = None;
        }
        st.files.retain(|(path, t, _)| *t >= term || std::fs::remove_file(path).is_err());
    }

    /// What we hold after change `after`, for the leader of `term` that is taking over (from now
    /// on we help no older leader): u64 term | frame, repeated.
    pub fn serve(&self, term: u64, after: u64) -> Vec<u8> {
        let mut st = self.st.lock().unwrap();
        st.seen = st.seen.max(term);
        let mut out = vec![];
        for (t, frame) in st.held.range(after + 1..).map(|(_, v)| v) {
            out.extend(t.to_le_bytes());
            out.extend_from_slice(frame);
        }
        out
    }
}

/// Keep the copy from the newest term.
fn keep(held: &mut BTreeMap<u64, (u64, Bytes)>, id: u64, term: u64, frame: Bytes) {
    if held.get(&id).is_none_or(|(t, _)| *t <= term) {
        held.insert(id, (term, frame));
    }
}

/// A new leader (replicated mode), before it takes writes: collect what the members hold beyond
/// the bucket's last commit, and commit it again — the longest run of consecutive changes,
/// never one from an older term after one from a newer term. Every member is asked (a committed
/// change is on at least `replicas - 1` of them); one that doesn't answer within 20 s is skipped.
pub async fn recover(lake: &Lake, me: &str, term: u64, own: Option<&ReplicaLog>) -> Result<u64> {
    let in_bucket = lake.cat.committed();
    let members: Vec<String> = lake.cat.get("m").await?.unwrap_or_default();
    let mut found: BTreeMap<u64, (u64, Arc<Delta>)> = BTreeMap::new();
    let mut merge = |bytes: &[u8]| -> Result<()> {
        let mut buf = BytesMut::from(bytes);
        while buf.len() >= 8 {
            let t = u64::from_le_bytes(buf.split_to(8)[..].try_into()?);
            let Some(Frame::Change(d)) = Frame::take(&mut buf)? else { anyhow::bail!("bad replica frame") };
            if found.get(&d.id).is_none_or(|(ft, _)| *ft <= t) {
                found.insert(d.id, (t, d));
            }
        }
        Ok(())
    };
    merge(&own.map(|o| o.serve(term, in_bucket)).unwrap_or_default())?;
    let deadline = Instant::now() + Duration::from_secs(20);
    for peer in members.iter().filter(|p| *p != me) {
        let url = format!("http://{peer}/cluster/replica?after={in_bucket}&term={term}");
        loop {
            match crate::cluster::http().get(&url).timeout(Duration::from_secs(5)).send().await.and_then(|r| r.error_for_status()) {
                Ok(r) => break merge(&r.bytes().await?).context("replica from a peer")?,
                Err(_) if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(250)).await,
                Err(e) => break eprintln!("recovering without {peer}: {e}"), // (then only it held what it held)
            }
        }
    }
    let (mut next, mut term_so_far, mut done) = (in_bucket + 1, 0, None);
    while let Some((t, d)) = found.get(&next).filter(|(t, _)| *t >= term_so_far) {
        let puts = d.puts.iter().filter(|(k, _)| k != "c").map(|(k, v)| (k.clone(), v.to_vec())).collect();
        done = Some(lake.cat.write(puts, &d.deletes).await?); // (pipelined: one bucket round trip for all)
        (term_so_far, next) = (*t, next + 1);
    }
    if lake.cat.replicas == 1 && !members.is_empty() {
        done = Some(lake.cat.write(vec![("m".into(), json(&Vec::<String>::new()))], &[]).await?); // no copies count now
    }
    if let Some(done) = done {
        done.await?;
    }
    lake.cat.wait_durable(lake.cat.committed()).await; // in the bucket before anything new
    let n = next - in_bucket - 1;
    if n > 0 {
        eprintln!("recovered {n} commits that followers held but the bucket didn't have yet");
    }
    Ok(n)
}

/// Leader, replicated mode: keep the list of followers whose copies count ("m") in step with
/// the live ones. A follower is listed — in the bucket — before its acks count, and stops counting
/// (with everything it helped commit in the bucket) before it leaves the list, so a new leader
/// always knows everyone it has to ask.
pub fn members(lake: Arc<Lake>, cluster: Arc<crate::cluster::Cluster>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let live: Vec<String> = cluster.nodes().into_iter().filter(|n| *n != cluster.addr).collect();
            let listed: Vec<String> = lake.cat.get("m").await.ok().flatten().unwrap_or_default();
            if live == listed {
                continue;
            }
            lake.cat.set_members(listed.into_iter().filter(|m| live.contains(m)).collect());
            lake.cat.wait_durable(lake.cat.committed()).await;
            if let Err(e) = lake.cat.commit(vec![("m".into(), json(&live))], &[]).await {
                eprintln!("listing the members: {e:#}");
                continue;
            }
            lake.cat.wait_durable(lake.cat.committed()).await;
            lake.cat.set_members(live);
        }
    });
}

/// Where this node keeps its replica files: next to its SSD tier, per lake and per node.
pub fn dir(lake_url: &str, addr: &str) -> PathBuf {
    let base = std::env::var("PONDRA_CACHE_DIR").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir().join("pondra-cache"));
    let clean = |s: &str| s.trim_start_matches("s3://").replace(['/', ':', '\\'], "_");
    base.join(format!("{}.replica", clean(lake_url))).join(clean(addr))
}
