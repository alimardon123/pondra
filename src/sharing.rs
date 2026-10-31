//! The Delta Sharing door (ADR-046 §3): `/delta-sharing/…` on every node, the open protocol
//! Databricks' recipients, pandas, Spark, Power BI and Tableau speak. A recipient signs in with
//! its token (`shares.rs`) and lists the shares granted to it, their schemas and tables; for a
//! table it gets a version of the table's Delta log (`delta.rs`) as files with short-lived links
//! (`vend.rs`), and reads them with its own compute. A node answers a little JSON a query and
//! never reads the rows. Every request is a line of `pondra.audit`, refusals too.
//!
//! A table that needs nothing of a Delta reader is answered in the protocol's Parquet form (every
//! client reads it); one with deletion vectors or renamed columns only to a client that reads
//! Delta's (`responseformat=delta`): never handed out as files that would show deleted rows.
use crate::byte_range;
use crate::read_delta::Log;
use crate::reset::Once;
use crate::app::App;
use crate::shares::{Share, Shared};
use crate::sharing_client::endpoint;
use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::{request::Parts, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json as j, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------- where it is

/// This node's address as where recipients reach the door, unless `PONDRA_SHARING_URL` names another
/// (`sharing_client::endpoint`).
pub fn set_endpoint(addr: &str) { crate::sharing_client::ENDPOINT.set_once(crate::tls::url(&format!("{}/delta-sharing", addr.trim_end_matches('/')))); }

pub fn routes() -> Router<App> {
    let t = "/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}";
    Router::new()
        .route("/delta-sharing/shares", get(list_shares))
        .route("/delta-sharing/shares/{share}", get(get_share))
        .route("/delta-sharing/shares/{share}/schemas", get(list_schemas))
        .route("/delta-sharing/shares/{share}/schemas/{schema}/tables", get(list_tables))
        .route("/delta-sharing/shares/{share}/all-tables", get(all_tables))
        .route(&format!("{t}/version"), get(version).head(version))
        .route(&format!("{t}/metadata"), get(metadata))
        .route(&format!("{t}/query"), post(query))
        .route(&format!("{t}/changes"), get(changes))
        .route("/delta-sharing/files/{link}", get(file).head(file))
}

// ---------------------------------------------------------------- who asks

/// An answer the protocol's way: `{"errorCode", "message"}`.
struct Fail(StatusCode, &'static str, String);

impl IntoResponse for Fail {
    fn into_response(self) -> Response { (self.0, Json(j!({"errorCode": self.1, "message": self.2}))).into_response() }
}

// (a client is told an error as every door words it: files by their SQL names, no routine's code, `ext::said`)
fn internal(e: anyhow::Error) -> Fail { Fail(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", crate::ext::said(&e)) }
fn missing(what: String) -> Fail { Fail(StatusCode::NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", what) }
fn invalid(what: String) -> Fail { Fail(StatusCode::BAD_REQUEST, "INVALID_PARAMETER_VALUE", what) }

/// The recipient a request is from, the shares granted to it, and what its client reads.
struct Caller {
    name: String,
    shares: Vec<(String, Share)>,
    from: Option<std::net::SocketAddr>,
    delta: bool,   // reads Delta's own form (`responseformat=delta`)
    parquet: bool, // reads the Parquet form (the default)
}

impl FromRequestParts<App> for Caller {
    type Rejection = Response;
    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Response> {
        let from = parts.extensions.get::<axum::extract::ConnectInfo<crate::tls::Peer>>().map(|c| c.0.addr);
        let token = parts.headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|h| h.strip_prefix("Bearer ")).map(str::trim);
        let found = match token {
            Some(t) => crate::shares::recipient_of(&app.lake, t).await.map_err(|e| internal(e).into_response())?,
            None => None,
        };
        let Some((name, _)) = found else {
            crate::audit::refused(app, "", "sharing", from, &format!("{} {}", parts.method, parts.uri.path()), "no recipient's token, or one that ended").await;
            tokio::time::sleep(std::time::Duration::from_millis(400)).await; // (a guess costs time)
            return Err(Fail(StatusCode::UNAUTHORIZED, "UNAUTHENTICATED", "a recipient's token, as its profile gives it (bearerToken), or a new one: ask the provider to rotate it".into()).into_response());
        };
        let shares = crate::shares::shares(&app.lake).await.map_err(|e| internal(e).into_response())?.into_iter().filter(|(_, s)| s.recipients.contains(&name)).collect();
        // `delta-sharing-capabilities: responseformat=delta,parquet;readerfeatures=…`
        let caps = parts.headers.get("delta-sharing-capabilities").and_then(|v| v.to_str().ok()).unwrap_or_default().to_lowercase();
        let formats: Vec<&str> = caps.split(';').filter_map(|c| c.trim().strip_prefix("responseformat=")).flat_map(|f| f.split(',')).map(str::trim).collect();
        let (delta, parquet) = (formats.contains(&"delta"), formats.is_empty() || formats.contains(&"parquet"));
        Ok(Caller { name, shares, from, delta, parquet })
    }
}

impl Caller {
    fn share(&self, name: &str) -> Result<&Share, Fail> {
        self.shares.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, s)| s).ok_or_else(|| missing(format!("no share {name} for you")))
    }
    fn table(&self, share: &str, schema: &str, table: &str) -> Result<&Shared, Fail> {
        let s = self.share(share)?;
        s.tables.iter().find(|t| t.schema.eq_ignore_ascii_case(schema) && t.name.eq_ignore_ascii_case(table)).ok_or_else(|| missing(format!("no table {schema}.{table} in share {share}")))
    }
    /// Its request, in `pondra.audit` (`note`: what it was answered).
    fn audit<T>(&self, app: &App, what: &str, r: &Result<T, Fail>, note: impl Fn(&T) -> String) {
        match r {
            Ok(v) => crate::audit::shared(app, &self.name, self.from, what, "ok", Some(note(v)).filter(|n| !n.is_empty())),
            Err(Fail(code, _, m)) => crate::audit::shared(app, &self.name, self.from, what, if code.is_client_error() { "refused" } else { "failed" }, Some(m.clone())),
        }
    }
}

/// A list a page at a time (`maxResults`, and the `nextPageToken` it gave).
fn page(items: Vec<Value>, q: &HashMap<String, String>) -> Value {
    let from: usize = q.get("pageToken").and_then(|t| t.parse().ok()).unwrap_or(0);
    let most: usize = q.get("maxResults").and_then(|m| m.parse().ok()).filter(|m| *m > 0).unwrap_or(usize::MAX);
    let next = (from.saturating_add(most) < items.len()).then(|| (from + most).to_string());
    let items: Vec<Value> = items.into_iter().skip(from).take(most).collect();
    match next {
        Some(n) => j!({"items": items, "nextPageToken": n}),
        None => j!({"items": items}),
    }
}

// ---------------------------------------------------------------- lists

async fn list_shares(State(app): State<App>, c: Caller, Query(q): Query<HashMap<String, String>>) -> Response {
    let items = c.shares.iter().map(|(n, s)| j!({"name": n, "id": s.id})).collect();
    c.audit(&app, "list shares", &Ok::<_, Fail>(()), |_| String::new());
    Json(page(items, &q)).into_response()
}

async fn get_share(State(app): State<App>, c: Caller, Path(share): Path<String>) -> Response {
    let r = c.share(&share).map(|s| j!({"share": {"name": share, "id": s.id}}));
    c.audit(&app, &format!("get share {share}"), &r, |_| String::new());
    r.map(Json).into_response()
}

fn tables_of(share: &str, s: &Share, schema: Option<&str>) -> Vec<Value> {
    s.tables.iter().filter(|t| schema.is_none_or(|x| t.schema.eq_ignore_ascii_case(x))).map(|t| j!({"name": t.name, "schema": t.schema, "share": share, "shareId": s.id, "id": t.id})).collect()
}

async fn list_schemas(State(app): State<App>, c: Caller, Path(share): Path<String>, Query(q): Query<HashMap<String, String>>) -> Response {
    let r = c.share(&share).map(|s| {
        let mut names: Vec<&str> = s.tables.iter().map(|t| t.schema.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        page(names.into_iter().map(|n| j!({"name": n, "share": share})).collect(), &q)
    });
    c.audit(&app, &format!("list schemas of {share}"), &r, |_| String::new());
    r.map(Json).into_response()
}

async fn list_tables(State(app): State<App>, c: Caller, Path((share, schema)): Path<(String, String)>, Query(q): Query<HashMap<String, String>>) -> Response {
    let r = c.share(&share).and_then(|s| {
        let items = tables_of(&share, s, Some(&schema));
        if items.is_empty() { Err(missing(format!("no schema {schema} in share {share}"))) } else { Ok(page(items, &q)) }
    });
    c.audit(&app, &format!("list tables of {share}.{schema}"), &r, |_| String::new());
    r.map(Json).into_response()
}

async fn all_tables(State(app): State<App>, c: Caller, Path(share): Path<String>, Query(q): Query<HashMap<String, String>>) -> Response {
    let r = c.share(&share).map(|s| page(tables_of(&share, s, None), &q));
    c.audit(&app, &format!("list tables of {share}"), &r, |_| String::new());
    r.map(Json).into_response()
}

// ---------------------------------------------------------------- a table

/// The table's folder and its Delta log at a version (the newest if none).
async fn log_of(app: &App, t: &Shared, version: Option<i64>) -> Result<(String, Arc<Log>), Fail> {
    let lake: &crate::store::Lake = &app.lake; // (an `&Arc<Lake>` doesn't deref to the `&impl Reach` replay takes)
    let meta = lake.cat.meta(&crate::store::table_key(&t.table)).await.map_err(internal)?.ok_or_else(|| missing(format!("{}.{} is gone from the lake", t.schema, t.name)))?;
    let folder = format!("data/{}", meta.folder(&t.table));
    let root = format!("{}/{folder}", lake.url.trim_end_matches('/'));
    let log = crate::read_delta::replay(lake, &root, version).await.map_err(|e| match version {
        Some(v) => invalid(format!("{}.{} has no version {v} to read: {}", t.schema, t.name, crate::ext::said(&e))),
        None => Fail(StatusCode::SERVICE_UNAVAILABLE, "TEMPORARILY_UNAVAILABLE", format!("{}.{} isn't available to share yet; its newest rows are still being written to the open format, so try again shortly: {}", t.schema, t.name, crate::ext::said(&e))),
    })?;
    Ok((folder, log))
}

/// The versions of a table's log and when each was written (ms), oldest first.
async fn times(app: &App, t: &Shared) -> Result<Vec<(i64, u64)>, Fail> {
    let meta = app.lake.cat.meta(&crate::store::table_key(&t.table)).await.map_err(internal)?.ok_or_else(|| missing(format!("{}.{} is gone", t.schema, t.name)))?;
    let dir = format!("data/{}/_delta_log/", meta.folder(&t.table));
    let listed: Vec<_> = futures::TryStreamExt::try_collect(app.lake.store.list(Some(&object_store::path::Path::from(dir)))).await.map_err(|e| internal(e.into()))?;
    let mut out: Vec<(i64, u64)> = listed.iter().filter_map(|o| {
        let n = o.location.filename()?;
        let v = n.strip_suffix(".json").filter(|v| v.len() == 20)?.parse().ok()?;
        Some((v, o.last_modified.timestamp_millis() as u64))
    }).collect();
    out.sort_unstable();
    Ok(out)
}

fn millis(ts: &str) -> Result<u64, Fail> {
    chrono::DateTime::parse_from_rfc3339(ts).map(|d| d.timestamp_millis().max(0) as u64).map_err(|_| invalid(format!("{ts}: a timestamp as 2026-10-03T15:00:00Z")))
}

/// The version a time asks for: the last written by then, or (`starting`) the first written after.
async fn version_at(app: &App, t: &Shared, ts: &str, starting: bool) -> Result<i64, Fail> {
    let at = millis(ts)?;
    let all = times(app, t).await?;
    let v = if starting { all.iter().find(|(_, ms)| *ms >= at) } else { all.iter().rev().find(|(_, ms)| *ms <= at) };
    v.map(|(v, _)| *v).ok_or_else(|| invalid(format!("{}.{} has no version {} {ts}", t.schema, t.name, if starting { "after" } else { "at" })))
}

/// The form the answer takes: Parquet's when the table needs nothing of a Delta reader and the
/// client reads it, Delta's when the client reads that; otherwise refused, never answered wrong.
fn delta_form(c: &Caller, t: &Shared, log: &Log) -> Result<bool, Fail> {
    let needs = log.protocol["minReaderVersion"].as_i64().unwrap_or(1) > 1;
    match (c.delta, c.parquet) {
        (true, false) => Ok(true),
        (true, true) => Ok(needs),
        (false, _) if !needs => Ok(false),
        _ => Err(Fail(StatusCode::BAD_REQUEST, "INVALID_PARAMETER_VALUE", format!("{}.{} has deleted rows or renamed columns, which only a client reading Delta's form takes (responseformat=delta: delta-sharing 1.1 or later)", t.schema, t.name))),
    }
}

fn headers(version: i64, delta: Option<bool>) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("delta-table-version", version.into());
    if let Some(d) = delta {
        h.insert("delta-sharing-capabilities", (if d { "responseformat=delta" } else { "responseformat=parquet" }).parse().expect("a header"));
    }
    h
}

fn ndjson(h: HeaderMap, lines: &[Value]) -> Response {
    let body = lines.iter().map(Value::to_string).collect::<Vec<_>>().join("\n") + "\n";
    (h, [("content-type", "application/x-ndjson; charset=utf-8")], body).into_response()
}

/// The protocol and metadata lines, in the form chosen.
fn head(log: &Log, delta: bool) -> [Value; 2] {
    match delta {
        true => [j!({"protocol": {"deltaProtocol": log.protocol}}), j!({"metaData": {"deltaMetadata": log.metadata, "version": log.version}})],
        false => {
            let m = &log.metadata;
            let meta = j!({"id": m["id"], "format": {"provider": "parquet"}, "schemaString": m["schemaString"], "partitionColumns": m["partitionColumns"].as_array().cloned().unwrap_or_default(), "configuration": m["configuration"].as_object().cloned().unwrap_or_default()});
            [j!({"protocol": {"minReaderVersion": 1}}), j!({"metaData": meta})]
        }
    }
}

async fn version(State(app): State<App>, c: Caller, Path((share, schema, table)): Path<(String, String, String)>, Query(q): Query<HashMap<String, String>>) -> Response {
    let r: Result<i64, Fail> = async {
        let t = c.table(&share, &schema, &table)?;
        match q.get("startingTimestamp") {
            Some(ts) if !t.history => Err(invalid(format!("{schema}.{table} is shared without its history (ALTER SHARE … ADD TABLE … WITH HISTORY): no version {ts}"))),
            Some(ts) => version_at(&app, t, ts, true).await,
            None => Ok(log_of(&app, t, None).await?.1.version),
        }
    }.await;
    c.audit(&app, &format!("version of {share}.{schema}.{table}"), &r, |v| format!("version {v}"));
    match r {
        Ok(v) => (headers(v, None), "").into_response(),
        Err(f) => f.into_response(),
    }
}

async fn metadata(State(app): State<App>, c: Caller, Path((share, schema, table)): Path<(String, String, String)>) -> Response {
    let r: Result<(Arc<Log>, bool), Fail> = async {
        let t = c.table(&share, &schema, &table)?;
        let (_, log) = log_of(&app, t, None).await?;
        let delta = delta_form(&c, t, &log)?;
        Ok((log, delta))
    }.await;
    c.audit(&app, &format!("metadata of {share}.{schema}.{table}"), &r, |(l, _)| format!("version {}", l.version));
    match r {
        Ok((log, delta)) => ndjson(headers(log.version, Some(delta)), &head(&log, delta)),
        Err(f) => f.into_response(),
    }
}

/// `POST …/query`: the table's files at its newest version (or `version`, `timestamp`: a table
/// shared WITH HISTORY), each with a link; `limitHint` stops once enough rows are listed. Hints
/// about rows (`predicateHints`) may be ignored, as the protocol allows: the client filters.
async fn query(State(app): State<App>, c: Caller, Path((share, schema, table)): Path<(String, String, String)>, body: axum::body::Bytes) -> Response {
    let ask: Value = serde_json::from_slice(&body).unwrap_or_default();
    let r: Result<(Arc<Log>, bool, Vec<Value>), Fail> = async {
        let t = c.table(&share, &schema, &table)?;
        let at = match (ask["version"].as_i64(), ask["timestamp"].as_str()) {
            (None, None) => None,
            _ if !t.history => return Err(invalid(format!("{schema}.{table} is shared without its history (ALTER SHARE … ADD TABLE … WITH HISTORY): only its newest version"))),
            (Some(v), _) => Some(v),
            (None, Some(ts)) => Some(version_at(&app, t, ts, false).await?),
        };
        ensure_not(ask["startingVersion"].is_number() || ask["endingVersion"].is_number(), "startingVersion/endingVersion asks for a change stream, which this share doesn't serve; request a single version or timestamp instead")?;
        let (folder, log) = log_of(&app, t, at).await?;
        let delta = delta_form(&c, t, &log)?;
        let files = files(&app, &c, t, &folder, &log, delta, ask["limitHint"].as_u64()).await?;
        Ok((log, delta, files))
    }.await;
    c.audit(&app, &format!("query {share}.{schema}.{table}"), &r, |(l, _, f)| format!("version {}: {} files", l.version, f.len()));
    match r {
        Ok((log, delta, files)) => ndjson(headers(log.version, Some(delta)), &[head(&log, delta).to_vec(), files].concat()),
        Err(f) => f.into_response(),
    }
}

fn ensure_not(bad: bool, why: &str) -> Result<(), Fail> { if bad { Err(invalid(why.into())) } else { Ok(()) } }

async fn changes(State(app): State<App>, c: Caller, Path((share, schema, table)): Path<(String, String, String)>) -> Response {
    let r: Result<String, Fail> = Err(Fail(StatusCode::BAD_REQUEST, "FEATURE_NOT_SUPPORTED", "this share serves table versions, not a change data feed; query a specific version (a table shared WITH HISTORY can be read at any past version)".into()));
    c.audit(&app, &format!("changes of {share}.{schema}.{table}"), &r, |_| String::new());
    r.into_response()
}

/// The version's files a recipient may have (a shared partition's only), each with its link.
async fn files(app: &App, c: &Caller, t: &Shared, folder: &str, log: &Log, delta: bool, limit: Option<u64>) -> Result<Vec<Value>, Fail> {
    let parts = if t.partitions.is_empty() { None } else { Some(partitions(app, t).await.map_err(internal)?) };
    let mut adds: Vec<&Value> = log.live().collect();
    adds.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str())); // (one order, every time)
    let (mut keys, mut kept, mut rows) = (vec![], vec![], 0u64);
    for add in adds {
        let rel = add["path"].as_str().unwrap_or_default();
        let key = format!("{folder}/{}", crate::read_delta::unescape(rel));
        // (a file whose partition isn't known here is left out: never one the share doesn't hold)
        if parts.as_ref().is_some_and(|p| p.get(&key).is_none_or(|v| !t.partitions.contains(v))) {
            continue;
        }
        if limit.is_some_and(|l| rows >= l) {
            break;
        }
        let stats: Value = add["stats"].as_str().and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default();
        rows += stats["numRecords"].as_u64().unwrap_or(0).saturating_sub(add["deletionVector"]["cardinality"].as_u64().unwrap_or(0));
        keys.push(key);
        kept.push(add);
    }
    quota(&c.name, keys.len())?;
    let (links, until) = crate::vend::links(&app.lake, &endpoint(), &keys, &c.name).await.map_err(internal)?;
    Ok(kept.into_iter().zip(links).map(|(add, url)| {
        let id = crate::users::sha256(add["path"].as_str().unwrap_or_default())[..32].to_string();
        match delta {
            true => {
                let mut a = add.clone();
                a["path"] = j!(url);
                j!({"file": {"id": id, "expirationTimestamp": until, "deltaSingleAction": {"add": a}}})
            }
            false => j!({"file": {"url": url, "id": id, "partitionValues": add["partitionValues"].as_object().cloned().unwrap_or_default(), "size": add["size"], "stats": add["stats"], "expirationTimestamp": until}}),
        }
    }).collect())
}

/// Every file of the table (current, and replaced but kept), by key, with its partition value.
async fn partitions(app: &App, t: &Shared) -> anyhow::Result<HashMap<String, String>> {
    let meta = app.lake.cat.meta(&crate::store::table_key(&t.table)).await?.unwrap_or_default();
    let mut all: Vec<crate::store::DataFile> = meta.files.iter().chain(&meta.replaced).cloned().collect();
    for m in crate::manifest::list(&app.lake, &meta).await? {
        all.extend(crate::manifest::files(&app.lake, &m).await?);
    }
    Ok(all.into_iter().map(|f| (f.path, f.part)).collect())
}

/// At most `PONDRA_SHARE_FILES_PER_MINUTE` (10,000) links a minute for each recipient, on each
/// node: their reads are GETs on the lake's bucket that no node's request budget sees (principle 7).
fn quota(who: &str, n: usize) -> Result<(), Fail> {
    crate::per_node!(static USED: Mutex<Option<HashMap<String, (u64, usize)>>> = Mutex::new(None));
    let most: usize = crate::env::or("PONDRA_SHARE_FILES_PER_MINUTE", 10_000);
    let minute = crate::log::now_ms() / 60_000;
    let mut used = USED.lock().unwrap();
    let e = used.get_or_insert_default().entry(who.to_string()).or_insert((minute, 0));
    if e.0 != minute {
        *e = (minute, 0);
    }
    if e.1 + n > most && e.1 > 0 {
        return Err(Fail(StatusCode::TOO_MANY_REQUESTS, "RESOURCE_EXHAUSTED", format!("more than {most} files a minute: ask again in a minute")));
    }
    e.1 += n;
    Ok(())
}

// ---------------------------------------------------------------- a lake on a disk: its files

/// `GET /delta-sharing/files/<link>`: a file a node's link names (`vend.rs`), a range if asked.
async fn file(State(app): State<App>, method: axum::http::Method, Path(link): Path<String>, Query(q): Query<HashMap<String, String>>, h: HeaderMap) -> Response {
    let (key, _) = match crate::vend::opened(&app.lake, &link, q.get("sig").map_or("", String::as_str)).await {
        Ok(k) => k,
        Err(e) => return Fail(StatusCode::FORBIDDEN, "PERMISSION_DENIED", crate::ext::said(&e)).into_response(),
    };
    let path = object_store::path::Path::from(key.as_str());
    let size = match object_store::ObjectStoreExt::head(&app.lake.store, &path).await {
        Ok(m) => m.size,
        Err(_) => return missing("the file is gone: ask for the table's files again".into()).into_response(),
    };
    let range = h.get("range").and_then(|r| r.to_str().ok()).and_then(|r| byte_range::resolve(r, size));
    let (from, to) = range.unwrap_or((0, size));
    let mut out = HeaderMap::new();
    out.insert("accept-ranges", "bytes".parse().expect("a header"));
    out.insert("content-length", (to - from).into());
    out.insert("content-type", "application/octet-stream".parse().expect("a header"));
    let status = match range {
        Some(_) => {
            out.insert("content-range", byte_range::content_range(&(from..to), size).parse().expect("a header"));
            StatusCode::PARTIAL_CONTENT
        }
        None => StatusCode::OK,
    };
    if method == axum::http::Method::HEAD || from == to {
        return (status, out).into_response();
    }
    let opts = object_store::GetOptions { range: Some(object_store::GetRange::Bounded(from..to)), ..Default::default() };
    match app.lake.store.get_opts(&path, opts).await {
        Ok(got) => (status, out, axum::body::Body::from_stream(got.into_stream())).into_response(),
        Err(e) => internal(e.into()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pages() {
        let p = super::page((0..5).map(|i| serde_json::json!(i)).collect(), &[("maxResults".to_string(), "2".to_string())].into());
        assert_eq!((p["items"].as_array().unwrap().len(), p["nextPageToken"].as_str()), (2, Some("2")));
    }
}
