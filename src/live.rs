//! Live queries (ADR-028): `GET /live?sql=…` (or `POST /live`, its body as `POST /sql` takes it,
//! with `$name` parameters) answers a query now, and again each time a commit changes a table it
//! reads — its own, through views, or an attached lake's — as JSON lines `{"at": commit, "rows":
//! […]}`. An answer that comes out the same isn't sent again, and a busy table is queried at most
//! once every `every_ms` (100). A live query is its open connection: once the client goes, nothing
//! runs for it. `POST /live` with `{"queries": [{"id", "sql", "params"?, "session"?}]}` watches
//! several on one connection, each line naming its query's `id` (a page's: a browser opens six
//! connections to a host at most, and live answers each holding one left nothing for the rest).
use crate::server::App;
use crate::store::{seg_key, Segment, TableMeta};
use axum::body::Body;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::Deserialize;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
pub struct Params {
    sql: Option<String>,
    every_ms: Option<u64>,
}

/// Several live queries on one connection, each with its own session (a console tab's).
#[derive(Deserialize)]
struct Many {
    queries: Vec<One>,
}

#[derive(Deserialize)]
struct One {
    id: String,
    sql: String,
    #[serde(default)]
    params: serde_json::Map<String, serde_json::Value>,
    session: Option<String>,
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
    who: crate::auth::Principal, // whose it is: each answer as its grants are then
    temps: u64,              // their changes when it last looked
    tag: String,             // what its lines start with: `"id":…,` when it shares a connection
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
    let every = Duration::from_millis(p.every_ms.unwrap_or(100).max(10));
    let kind = headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
    if let Ok(Many { queries }) = serde_json::from_slice::<Many>(&body) {
        let mut all = Vec::with_capacity(queries.len());
        for q in queries {
            let mut h = headers.clone(); // (its own session: the header the query names, checked as a request's)
            match q.session.as_deref().and_then(|s| axum::http::HeaderValue::from_str(s).ok()) {
                Some(v) => h.insert("x-pondra-session", v),
                None => h.remove("x-pondra-session"),
            };
            let one = serde_json::to_vec(&serde_json::json!({"sql": q.sql, "params": q.params})).unwrap_or_default();
            let tag = format!("\"id\":{},", serde_json::Value::from(q.id));
            all.push(match watch(&app, &h, None, "application/json", &one, every, tag.clone()).await {
                Ok(w) => lines(w).boxed(),
                Err(e) => futures::stream::once(async move { Ok(error(e, &tag)) }).boxed(),
            });
        }
        return ([("content-type", "application/x-ndjson")], Body::from_stream(futures::stream::select_all(all))).into_response();
    }
    match watch(&app, &headers, p.sql.as_deref(), &kind, &body, every, String::new()).await {
        Ok(w) => ([("content-type", "application/x-ndjson")], Body::from_stream(lines(w))).into_response(),
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(),
    }
}

/// A live query made ready: its SQL prepared, the tables it reads found, as its session and caller.
async fn watch(app: &App, headers: &axum::http::HeaderMap, sql: Option<&str>, kind: &str, body: &[u8], every: Duration, tag: String) -> anyhow::Result<Watch> {
    let session = crate::temp::of(headers);
    crate::temp::SESSION.scope(session.clone(), async {
        let req = match sql {
            Some(sql) => crate::routines::Request::read("", sql.as_bytes())?,
            None => crate::routines::Request::read(kind, body)?,
        };
        anyhow::ensure!(req.tables.is_empty(), "a live query reads the lake: rows sent with it would never change");
        let sql = crate::routines::prepare(&app.lake, &req.sql, &req.params, &req.views).await?;
        anyhow::ensure!(crate::routines::split(&sql).len() == 1 && crate::write::parse(&sql).is_none(), "a live query is one query");
        let (tables, others) = reads(app, &sql).await?;
        anyhow::Ok(Watch { seen: app.lake.visible(), app: app.clone(), sql, tables, others, print: None, answer: None, next: Instant::now(), every, session, who: crate::auth::current().unwrap_or_else(|| crate::auth::Principal::of(crate::auth::Role::Admin)), temps: 0, tag, _open: Open::new() })
    })
    .await
}

/// Its answers as lines: the first at once, then one each time it changes; a line now and then
/// while nothing does, so a client gone is noticed.
fn lines(w: Watch) -> impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send {
    futures::stream::unfold(Some(w), |state| async move {
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
                Err(e) => return Some((Ok(error(e, &w.tag)), None)),
            };
            if w.print == Some(print) {
                continue; // (commits to other tables)
            }
            (w.print, w.next) = (Some(print), Instant::now() + w.every);
            let who = match w.who.access.is_some() { // (a user's grants as they are now: revoked, the answers end)
                true => crate::users::principal(&w.app.lake, &w.who.name).await.unwrap_or_else(|_| crate::auth::Principal { name: w.who.name.clone(), role: crate::auth::Role::None, access: Some(Default::default()), door: "http", from: None, operator: false }), // (gone: nothing)
                false => w.who.clone(),
            };
            let rows = crate::temp::SESSION.scope(w.session.clone(), crate::auth::WHO.scope(who, w.app.query(&w.sql, None))).await.and_then(|b| crate::server::render(&b, None));
            let rows = match rows {
                Ok(r) => r,
                Err(e) => return Some((Ok(error(e, &w.tag)), None)),
            };
            let hash = std::hash::BuildHasher::hash_one(&std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default(), &rows);
            if w.answer == Some(hash) {
                continue; // (the same answer: nothing to send)
            }
            w.answer = Some(hash);
            let line = [format!("{{{}\"at\":{},\"rows\":", w.tag, w.seen).as_bytes(), &rows, b"}\n"].concat();
            return Some((Ok(line.into()), Some(w)));
        }
    })
}

fn error(e: anyhow::Error, tag: &str) -> bytes::Bytes { format!("{{{tag}\"error\":{}}}\n", serde_json::Value::from(format!("{e:#}"))).into() }

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
