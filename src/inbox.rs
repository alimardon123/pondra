//! The bucket inbox: how a machine that can reach the bucket but not the leader — another
//! network, another company — still writes, exactly-once and with the same capabilities. It
//! leaves its request in `inbox/<id>.<kind>` and touches `inbox/bell`. The leader looks at the
//! bell every second (one HEAD, a cheap request class on S3 and R2), and when it rang, records
//! every request and leaves the answer in `inbox/<id>.out`. Answers nobody collected are deleted
//! after an hour.
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

const BELL: &str = "inbox/bell";

/// Writer: leave a request and wait for the answer. None if no leader is alive any more (the
/// caller then leads itself; the request is withdrawn, and a retry of it would be a duplicate).
pub async fn send(store: &Store, r: &Request) -> Result<Option<Value>> {
    let (kind, body) = r.inbox()?;
    let id = uuid::Uuid::new_v4();
    let (req, out) = (Path::from(format!("inbox/{id}.{kind}")), Path::from(format!("inbox/{id}.out")));
    store.put(&req, body.into()).await?;
    store.put(&Path::from(BELL), id.to_string().into_bytes().into()).await?;
    for tick in 1.. {
        tokio::time::sleep(Duration::from_millis(500)).await;
        match store.get(&out).await {
            Ok(r) => {
                let answer: Value = serde_json::from_slice(&r.bytes().await?)?;
                let _ = store.delete(&out).await;
                return match answer.get("error") {
                    Some(e) => bail!("the leader: {}", e.as_str().unwrap_or_default()),
                    None => Ok(Some(answer["ok"].clone())),
                };
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        if tick % 10 == 0 && !matches!(&crate::cluster::latest(store).await?, Some(t) if crate::cluster::alive(store, t).await) {
            let _ = store.delete(&req).await;
            return Ok(None);
        }
    }
    unreachable!()
}

/// Leader: answer the inbox whenever the bell rings (and once at start: requests may have waited
/// for a leader).
pub fn serve(lake: Arc<Lake>, seq: Arc<Sequencer>, lock: Arc<Mutex<()>>) {
    tokio::spawn(async move {
        let mut heard = None;
        loop {
            let bell = lake.store.head(&Path::from(BELL)).await.ok().map(|m| (m.last_modified, m.e_tag));
            if heard.is_none() || bell != heard.clone().flatten() {
                if let Err(e) = drain(&lake, &seq, &lock).await {
                    eprintln!("inbox: {e:#}");
                }
                heard = Some(bell);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
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
