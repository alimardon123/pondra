//! Live queries (ADR-028): `GET /live?sql=…` (or `POST /live`, its body as `POST /sql` takes it,
//! with `$name` parameters) answers a query now, and again each time a commit changes a table it
//! reads — its own, through views, or an attached lake's — as JSON lines `{"at": commit, "rows":
//! […]}`. An answer that comes out the same isn't sent again, and a busy table is queried at most
//! once every `every_ms` (100). A live query is its open connection: once the client goes, nothing
//! runs for it.
use crate::server::App;
use crate::store::{seg_key, Segment, TableMeta};
use axum::body::Body;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
pub struct Params {
    sql: Option<String>,
    every_ms: Option<u64>,
}

/// What the query reads, and where the log was when it last looked.
struct Watch {
    app: App,
    sql: String,
    tables: Vec<String>,   // this lake's tables it reads (its own, and its views')
    others: Vec<String>,   // attached lakes it reads
    seen: u64,             // the last commit looked at
    print: Option<u64>,    // what the tables were then (their definitions and files)
    answer: Option<u64>,   // the last answer sent
    next: Instant,         // not queried again before this
    every: Duration,
    session: Option<String>, // its temporary tables' session (`temp.rs`)
    temps: u64,              // their changes when it last looked
    _open: Open,             // (counted in /stats while its client listens)
}

/// Live queries open on this node now (`/stats`: `live_queries`).
pub static OPEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

struct Open;

impl Open {
    fn new() -> Open {
        OPEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Open
    }
}

impl Drop for Open {
    fn drop(&mut self) { OPEN.fetch_sub(1, std::sync::atomic::Ordering::Relaxed); }
}

pub async fn live(State(app): State<App>, Query(p): Query<Params>, headers: axum::http::HeaderMap, body: bytes::Bytes) -> Response {
    let session = crate::temp::of(&headers);
    let start = crate::temp::SESSION.scope(session.clone(), async {
        let kind = headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or_default();
        let req = match &p.sql {
            Some(sql) => crate::routines::Request::read("", sql.as_bytes())?,
            None => crate::routines::Request::read(kind, &body)?,
        };
        anyhow::ensure!(req.tables.is_empty(), "a live query reads the lake: rows sent with it would never change");
        let sql = crate::routines::prepare(&app.lake, &req.sql, &req.params, &req.views).await?;
        anyhow::ensure!(crate::routines::split(&sql).len() == 1 && crate::write::parse(&sql).is_none(), "a live query is one query");
        let (tables, others) = reads(&app, &sql).await?;
        anyhow::Ok(Watch { seen: app.lake.visible(), app, sql, tables, others, print: None, answer: None, next: Instant::now(), every: Duration::from_millis(p.every_ms.unwrap_or(100).max(10)), session, temps: 0, _open: Open::new() })
    });
    let w = match start.await {
        Ok(w) => w,
        Err(e) => return (axum::http::StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(),
    };
    let lines = futures::stream::unfold(Some(w), |state| async move {
        let mut w = state?;
        let (mut hwm, mut temp) = (w.app.lake.hwm.subscribe(), crate::temp::changes());
        loop {
            if w.print.is_some() {
                tokio::time::sleep_until(w.next.into()).await; // (at most every `every_ms`)
                if w.app.lake.visible() == w.seen && crate::temp::version(w.session.as_deref()) == w.temps {
                    let changed = async { tokio::select! { _ = hwm.changed() => {}, _ = temp.changed() => {} } };
                    if tokio::time::timeout(Duration::from_secs(15), changed).await.is_err() {
                        return Some((Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"\n")), Some(w))); // (a line now and then: a client gone is noticed)
                    }
                    continue;
                }
            }
            let print = match print(&mut w).await {
                Ok(p) => p,
                Err(e) => return Some((Ok(error(e)), None)),
            };
            if w.print == Some(print) {
                continue; // (commits to other tables)
            }
            (w.print, w.next) = (Some(print), Instant::now() + w.every);
            let rows = crate::temp::SESSION.scope(w.session.clone(), w.app.query(&w.sql, None)).await.and_then(|b| crate::server::render(&b, None));
            let rows = match rows {
                Ok(r) => r,
                Err(e) => return Some((Ok(error(e)), None)),
            };
            let hash = std::hash::BuildHasher::hash_one(&std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default(), &rows);
            if w.answer == Some(hash) {
                continue; // (the same answer: nothing to send)
            }
            w.answer = Some(hash);
            let line = [format!("{{\"at\":{},\"rows\":", w.seen).as_bytes(), &rows, b"}\n"].concat();
            return Some((Ok(line.into()), Some(w)));
        }
    });
    ([("content-type", "application/x-ndjson")], Body::from_stream(lines)).into_response()
}

fn error(e: anyhow::Error) -> bytes::Bytes { format!("{}\n", serde_json::json!({"error": format!("{e:#}")})).into() }

/// The tables `sql` reads here (itself and through its views, the session's too) and the attached
/// lakes it names.
async fn reads(app: &App, sql: &str) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let temps = crate::temp::views(sql).into_iter().fold(sql.to_string(), |t, (_, s)| format!("{t} {s}"));
    let views = crate::query::stored_views(&app.lake, &temps, false).await?;
    let text = views.iter().fold(temps, |t, (_, s)| format!("{t} {s}"));
    let tables = app.lake.cat.scan::<TableMeta>("t/", "t0").await?.into_iter().map(|(k, _)| k[2..].to_string()).filter(|t| crate::ddl::mentions(&text, t)).collect();
    let others = app.lake.attached.read().unwrap().iter().map(|(n, _)| n.clone()).filter(|n| crate::ddl::mentions(&text, n)).collect();
    Ok((tables, others))
}

/// What the query's tables are now: a print of their definitions and files, of the commits since
/// the last look that touched them, of the attached lakes' catalogs, and of the session's
/// temporary tables. The same print, the same answer.
async fn print(w: &mut Watch) -> anyhow::Result<u64> {
    use std::hash::{Hash, Hasher};
    let lake = &w.app.lake;
    let now = lake.visible();
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for t in &w.tables {
        lake.cat.get_raw(&crate::store::table_key(t)).await?.hash(&mut h);
    }
    let mut touched = false;
    if now > w.seen {
        for (_, seg) in lake.cat.scan::<Segment>(&seg_key(w.seen + 1), &seg_key(now + 1)).await? {
            touched |= w.tables.iter().any(|t| seg.parts.contains_key(t) || seg.parts.contains_key(&crate::sys::deleted(t)));
        }
    }
    (touched.then_some(now)).hash(&mut h); // (a commit to its tables: a new print)
    for (name, other) in lake.attached.read().unwrap().iter().filter(|(n, _)| w.others.contains(n)) {
        (name, other.cat.version()).hash(&mut h);
    }
    w.temps = crate::temp::version(w.session.as_deref());
    w.temps.hash(&mut h);
    w.seen = now;
    Ok(h.finish())
}
