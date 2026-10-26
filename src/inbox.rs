//! The bucket inbox: how a machine that can reach the bucket but not the leader — another
//! network, another company — still writes, exactly-once and with the same capabilities. It
//! leaves its request in `inbox/<id>.<kind>` and touches `inbox/bell`. The leader looks at the
//! bell every second (one HEAD, a cheap request class on S3 and R2: `cluster::listen`), marks its
//! term alive when it rang, records every request and leaves the answer in `inbox/<id>.out`. A
//! leader that doesn't mark itself in a few seconds is gone, and the writer leads (`cluster::deaf`).
//! Answers nobody collected are deleted after an hour.
use crate::log::Sequencer;
use crate::store::{Lake, Store};
use crate::write::{handle, Request};
use anyhow::{bail, Result};
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStoreExt};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub const BELL: &str = "inbox/bell";

/// Ring the bell (`id`, the body, tells one ring from the next). R2 takes one write a second to a key: a ring refused
/// (429) means it just rang, so it counts as rung; and the leader looks every 30 s anyway (C5).
pub async fn ring(store: &Store, id: &str) {
    let _ = store.put(&Path::from(BELL), id.as_bytes().to_vec().into()).await;
}

/// Writer: leave a request and wait for the answer. None if no leader is alive any more (the
/// caller then leads itself; the request is withdrawn, and a retry of it would be a duplicate).
/// `wake`: nobody leads the lake at that place, so once the request is in, a process of its own
/// leads it for a moment and answers it (`lead_once`). `holder`: the term whose leader is to answer,
/// which is gone if it doesn't mark itself when the bell rings (`cluster::Ear`).
pub async fn send(store: &Store, r: &Request, wake: Option<&str>, holder: Option<&crate::cluster::Term>) -> Result<Option<Value>> {
    let (kind, body) = r.inbox()?;
    let id = uuid::Uuid::new_v4();
    let (req, out) = (Path::from(format!("inbox/{id}.{kind}")), Path::from(format!("inbox/{id}.out")));
    store.put(&req, body.into()).await?;
    let mut ear = crate::cluster::Ear::ring(store, holder, &id.to_string()).await;
    let woke = wake.map(|dir| {
        let dir = dir.to_string();
        crate::panics::spawn(async move { lead_once(&dir).await })
    });
    for tick in 1.. {
        tokio::time::sleep(Duration::from_millis(500)).await;
        match store.get(&out).await {
            Ok(r) => {
                let answer: Value = serde_json::from_slice(&r.bytes().await?)?;
                let _ = store.delete(&out).await;
                if let Some(w) = woke {
                    // (it answers, then checkpoints and lets go: the caller's next step may need the
                    // lake free, as DROP DATABASE of a branch just unpinned does)
                    let _ = tokio::time::timeout(Duration::from_secs(60), w).await;
                }
                return match answer.get("error") {
                    Some(e) => bail!("the leader: {}", e.as_str().unwrap_or_default()),
                    None => Ok(Some(answer["ok"].clone())),
                };
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        // (a leader elsewhere that didn't mark itself when we rang is gone: withdraw, and the caller leads)
        if let Some(e) = &ear {
            match e.gone(store).await {
                Some(true) => {
                    let _ = store.delete(&req).await;
                    return Ok(None);
                }
                Some(false) => ear = None, // (it heard: its answer may take as long as the request does)
                None => {}
            }
        }
        if tick % 10 == 0 && !matches!(&crate::cluster::latest(store).await?, Some(t) if crate::cluster::alive(store, t).await) {
            let _ = store.delete(&req).await;
            return Ok(None);
        }
    }
    unreachable!()
}

/// Lead the lake at `dir` for a moment — making it, if nothing is there yet — and answer its
/// inbox, in a `pondra lead` process of its own. A node never leads another lake itself: that
/// lake's catalog writer would outlive the moment in the node, and a later leader of it would
/// fence it out.
pub async fn lead_once(dir: &str) -> Result<()> {
    // (another process would lead a lake of its own memory, gone when it ends)
    anyhow::ensure!(!crate::memory::in_memory(dir), "{dir} is in a process's memory, where a lake is made by opening it (pondra.open(), pondra :memory:) and led by that process alone: attach or make a database in a folder or a bucket");
    let out = tokio::process::Command::new(crate::library::exe()?).args(["lead", "--dir", dir]).envs(crate::env::given())
        .env_remove("PONDRA_JOB").env_remove("PONDRA_TRIES").env(crate::store::REACH_VAR, crate::store::reaches()) // (the keys this node reaches other places with)
        .stdin(std::process::Stdio::null()).output().await?;
    anyhow::ensure!(out.status.success(), "couldn't lead the lake at {dir}: {}", String::from_utf8_lossy(&out.stderr).trim());
    Ok(())
}

/// Leader: answer the inbox whenever the bell rings (the leader hears it as it marks its term:
/// `rings`, `cluster::listen`), every 30 s anyway, and once at start: requests may have waited
/// for a leader.
pub fn serve(lake: Arc<Lake>, seq: Arc<Sequencer>, lock: Arc<Mutex<()>>, mut rings: tokio::sync::watch::Receiver<u64>) {
    crate::panics::spawn(async move {
        loop {
            if let Err(e) = drain(&lake, &seq, &lock).await {
                eprintln!("inbox: {e:#}");
            }
            let _ = tokio::time::timeout(Duration::from_secs(30), async {
                if rings.changed().await.is_err() {
                    std::future::pending::<()>().await // (nobody hears the bell any more: every 30 s)
                }
            })
            .await;
        }
    });
}

/// Leader: record every request waiting in the inbox, and leave each its answer.
pub async fn drain(lake: &Lake, seq: &Sequencer, lock: &Mutex<()>) -> Result<()> {
    let store = &lake.store;
    let items: Vec<_> = store.list(Some(&Path::from("inbox"))).try_collect().await?;
    for o in items {
        let name = o.location.filename().unwrap_or_default().to_string();
        let Some((id, kind)) = name.split_once('.') else { continue }; // (the bell)
        let age = crate::log::now_ms() as i64 - o.last_modified.timestamp_millis();
        if kind == "out" {
            if age > 3_600_000 {
                let _ = store.delete(&o.location).await; // nobody came for it
            }
            continue;
        }
        let body = match store.get(&o.location).await {
            Ok(r) => r.bytes().await?,
            Err(_) => continue, // (withdrawn meanwhile)
        };
        let answer = match async { handle(lake, seq, lock, Request::from_inbox(kind, body)?).await }.await {
            Ok(v) => json!({"ok": v}),
            Err(e) => json!({"error": format!("{e:#}")}),
        };
        store.put(&Path::from(format!("inbox/{id}.out")), serde_json::to_vec(&answer)?.into()).await?;
        store.delete(&o.location).await?;
    }
    Ok(())
}
