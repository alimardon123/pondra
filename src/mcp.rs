//! MCP (the Model Context Protocol) at `POST /mcp`: AI agents — Claude, Cursor, anything that
//! speaks MCP — list the tables, query them, write, and follow what changed, with the same tokens
//! as everyone else (`Authorization: Bearer …`). JSON-RPC 2.0 over MCP's "streamable HTTP"
//! transport, each answer plain JSON (no event stream: every tool answers at once).
//!
//! A session: `initialize` gives the client an `Mcp-Session-Id`, and the tool calls that send it
//! back share one session, so a `write` of `BEGIN` is a transaction the next calls are in, until
//! `COMMIT`, `ROLLBACK`, or `DELETE /mcp` with the id (`txn.rs`, `temp.rs`).
use crate::app::App;
use crate::store::{Lake, TableMeta};
use crate::views::View;
use anyhow::Result;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

const ROWS: usize = 1000; // at most this many rows per answer: an agent reads them all

pub async fn handle(State(app): State<App>, Extension(who): Extension<crate::auth::Principal>, headers: HeaderMap, body: Bytes) -> Response {
    let Ok(req) = serde_json::from_slice::<Value>(&body) else {
        return Json(json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "not JSON"}})).into_response();
    };
    let Some(id) = req.get("id").cloned() else { return StatusCode::ACCEPTED.into_response() }; // a notification
    let session = match named(&headers, &who.name) {
        Ok(session) => session,
        Err(refused) => return refused,
    };
    let (method, params) = (req["method"].as_str().unwrap_or_default(), &req["params"]);
    // A request that names a session must name one this node holds (made by initialize, used within
    // the idle time, not ended): otherwise 404, and the client initializes again (the Streamable HTTP
    // spec). A request with no session header is stateless, as before.
    if let (Some(s), false) = (&session, method == "initialize") {
        if !crate::temp::sessions(&app.lake).is_some_and(|at| at.live(s)) {
            return gone(&id);
        }
        if let Some(at) = crate::temp::sessions(&app.lake) {
            at.touch(s);
        }
    }
    let mut minted = None; // (`initialize` gives the client its session)
    let result = match method {
        "initialize" => {
            let raw = format!("mcp-{}", uuid::Uuid::new_v4().simple());
            if let Some(at) = crate::temp::sessions(&app.lake) {
                let _ = at.with(&crate::temp::id(&who.name, &raw), false, |_| Ok(())); // (held from now: idle ends it, `reap`)
            }
            minted = Some(raw);
            json!({
                "protocolVersion": params["protocolVersion"].as_str().unwrap_or("2025-06-18"),
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "pondra", "version": env!("CARGO_PKG_VERSION")},
                "instructions": "Pondra is a streaming lakehouse: tables that take appends, upserts and deletes and \
                    are queryable within milliseconds, in SQL: PostgreSQL's, with DuckDB's forms and Pondra's own. \
                    Start with list_tables."})
        }
        "ping" => json!({}),
        "tools/list" => {
            let all = [tools().as_array().cloned().unwrap_or_default(), crate::find::mcp_tools(), procedures(&app, &who.scope).await].concat();
            json!({"tools": all})
        }
        "tools/call" => match crate::temp::SESSION.scope(crate::temp::named(&app.lake, session), call(&app, &who, params)).await {
            Ok(result) => result,
            Err(error) => return Json(json!({"jsonrpc": "2.0", "id": id, "error": error})).into_response(), // (no such tool: a protocol error)
        },
        _ => return Json(json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": format!("no method {method}")}})).into_response(),
    };
    let mut out = Json(json!({"jsonrpc": "2.0", "id": id, "result": result})).into_response();
    if let Some(v) = minted.and_then(|s| HeaderValue::from_str(&s).ok()) {
        out.headers_mut().insert("mcp-session-id", v);
    }
    out
}

/// The session an `Mcp-Session-Id` names: a well-formed one (`mcp-` and letters and digits, at most
/// 64 characters) that `initialize` gave. Any other value is a 400. Whether this node still holds
/// it is `handle`'s check (404). Keyed by the user, as `/sessions/{id}` is, so another user's id is
/// another session.
fn named(headers: &HeaderMap, user: &str) -> Result<Option<String>, Response> {
    let Some(h) = headers.get("mcp-session-id") else { return Ok(None) };
    match h.to_str().ok().filter(|id| valid(id)) {
        Some(id) => Ok(Some(crate::temp::id(user, id))),
        None => Err(refused("Mcp-Session-Id: the id initialize gave (mcp- and letters and digits)")),
    }
}

/// A request MCP refuses before any tool runs: HTTP 400 with JSON-RPC's Invalid Request (-32600),
/// the protocol's own code, as the other protocol errors have.
fn refused(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32600, "message": message}}))).into_response()
}

fn valid(id: &str) -> bool {
    id.len() <= 64 && id.strip_prefix("mcp-").is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_alphanumeric()))
}

/// `DELETE /mcp` with its `Mcp-Session-Id`: the session ends, its transaction rolled back and its
/// temporary tables gone, as an MCP client does when it closes. One this node doesn't hold is 404.
pub async fn end(State(app): State<App>, Extension(who): Extension<crate::auth::Principal>, headers: HeaderMap) -> Response {
    match named(&headers, &who.name) {
        Ok(Some(session)) if crate::temp::sessions(&app.lake).is_some_and(|at| at.live(&session)) => {
            if let Some(at) = crate::temp::sessions(&app.lake) {
                at.end(&session);
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(Some(_)) => gone(&Value::Null),
        Ok(None) => refused("DELETE /mcp: send the Mcp-Session-Id that initialize gave"),
        Err(refused) => refused,
    }
}

/// A session this node doesn't hold (never made here, ended, or idle too long): HTTP 404, and the
/// JSON-RPC error that the MCP SDKs send for it (-32001), so the client initializes again.
fn gone(id: &Value) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32001,
        "message": "Mcp-Session-Id: no such session (it ended, went idle, or was never made here): send initialize again"}}))).into_response()
}

fn tools() -> Value {
    let args = |props: Value, required: &[&str]| json!({"type": "object", "properties": props, "required": required});
    let sql = json!({"sql": {"type": "string"}});
    json!([
        {"name": "list_tables", "annotations": {"readOnlyHint": true}, "inputSchema": args(json!({}), &[]),
         "description": "Every table and view, as SQL names it (schema.table in a schema other than public; attached lakes' as lake.table or lake.schema.table): \
             its columns; its kind — append, upsert (a primary key: the newest row per key wins), merge (rows per key combine, e.g. sum) \
             or view (a stored query), or null when you may not read its key; for views, their SQL; and `about`, what its owner \
             wrote of it (COMMENT ON TABLE), such as that each row is one change to a key rather than the key's current row."},
        {"name": "query", "annotations": {"readOnlyHint": true}, "inputSchema": args(sql.clone(), &["sql"]),
         "description": format!("Run one SQL query and get its rows as JSON (the first {ROWS}, and the total). \
             `WHERE key = …` on an upsert table is a fast point lookup. Vector search: ORDER BY cosine_distance(embedding, [0.1, …]) LIMIT k. \
             In a session (see write) it reads the transaction's snapshot and its own writes, and the session's variables.")},
        {"name": "write", "annotations": {"readOnlyHint": false, "destructiveHint": true, "idempotentHint": true},
         "inputSchema": args(json!({"sql": {"type": "string"}, "job": {"type": "string", "description": "an id for this write: retried with the same id, it is applied once"}}), &["sql"]),
         "description": "Runs any script: one statement or several, a SELECT or a CALL among them, and BEGIN … COMMIT or ROLLBACK (see below). \
             INSERT, UPDATE or DELETE; CREATE/DROP of a TABLE, VIEW, \
             MATERIALIZED VIEW or SCHEMA. Outside a transaction, committed and durable when this returns. Needs a write token; CREATE and DROP an admin token. \
             In a session (the client keeps the Mcp-Session-Id that initialize gave, as MCP's SDKs do), BEGIN … COMMIT and ROLLBACK \
             make a transaction, committed as one; without a session each call is on its own. A session idle for PONDRA_SESSION_IDLE_SECS \
             (3600) ends, its transaction rolled back, and its next call is HTTP 404: send initialize again."},
        {"name": "changes", "annotations": {"readOnlyHint": true},
         "inputSchema": args(json!({"table": {"type": "string"}, "after": {"type": "integer", "description": "a position from an earlier call; 0 for as far back as kept; none for from now"}}), &["table"]),
         "description": format!("What was committed to a table after a position — every insert, update and delete, each row with its \
             `_change_type` (insert, update_preimage, update_postimage, delete; upsert on a keyed table), `_row_id` and `_version` — \
             about {ROWS} rows at a time, and the position to ask from next.")},
    ])
}

/// A tool's answer (its JSON, as text) as its result, or the JSON-RPC error for a tool the server doesn't have
/// (-32602, Invalid params, with its SQLSTATE 42883 in `data`). A tool that fails is a result with
/// `isError`, its message carrying the SQLSTATE: the agent sees it and can act on it.
async fn call(app: &App, who: &crate::auth::Principal, params: &Value) -> Result<Value, Value> {
    let arg = |k: &str| params["arguments"][k].as_str().unwrap_or_default();
    let out = match params["name"].as_str().unwrap_or_default() {
        "list_tables" => list(app, &who.scope).await.map(|v| v.to_string()),
        "query" => query(app, who, arg("sql")).await,
        "write" => write(app, who, arg("sql"), params["arguments"]["job"].as_str()).await,
        "changes" => changes(app, who, arg("table"), params["arguments"]["after"].as_u64()).await,
        name if crate::find::is_tool(name) => match crate::find::mcp_sql(name, &params["arguments"]) {
            Ok(sql) => query(app, who, &sql).await,
            Err(e) => Err(e),
        },
        other => {
            let Some(name) = procedure(app, other).await else {
                return Err(json!({"code": -32602, "message": format!("no tool {other}"), "data": {"sqlstate": "42883"}}));
            };
            let given = params["arguments"].as_object().into_iter().flatten().map(|(k, v)| Ok(format!("{k} => {}", crate::routines::literal(v)?))).collect::<Result<Vec<_>>>();
            match given {
                Ok(given) => called(app, who, &format!("CALL {name}({})", given.join(", ")), None).await,
                Err(e) => Err(e),
            }
        }
    };
    let (text, error) = match out {
        Ok(text) => (text, false),
        Err(e) => {
            // (a tool error, which the agent sees and can act on: Postgres's code and the words every door gives, a
            // routine's code cut for a caller who may not see it)
            let (_, code, words) = crate::codes::answer(&e, who.scope.shows_code());
            (format!("SQLSTATE {code}: {words}"), true)
        }
    };
    Ok(json!({"content": [{"type": "text", "text": text}], "isError": error}))
}

/// Each stored procedure is a tool too, its parameters the tool's (ADR-023): what a lake's owner
/// wrote for a job becomes something an agent can do, with the agent's token's rights. Described as `scope` is shown them.
async fn procedures(app: &App, scope: &crate::rights::Scope) -> Vec<Value> {
    let Ok(all) = crate::routines::listed(&app.lake).await else { return vec![] };
    // (what COMMENT ON PROCEDURE says of it; else its body's first line, if a comment, to a caller who may see its code:
    // a user with grants isn't shown a routine's body, so not its first line either)
    let notes: std::collections::HashMap<String, String> =
        app.lake.cat.scan::<String>("cm/routine/", "cm/routine0").await.unwrap_or_default().into_iter().map(|(k, v)| (k["cm/routine/".len()..].to_string(), v)).collect();
    let code = scope.shows_code();
    let kind = |t: Option<&str>| match t.unwrap_or_default().split('(').next().unwrap_or_default().to_uppercase().as_str() {
        "BIGINT" | "INT" | "INTEGER" | "SMALLINT" | "TINYINT" => "integer",
        "DOUBLE" | "REAL" | "FLOAT" | "DECIMAL" | "NUMERIC" => "number",
        "BOOLEAN" | "BOOL" => "boolean",
        _ => "string",
    };
    let mut out: Vec<Value> = all.iter().filter(|(_, r)| r.kind == crate::expander::Kind::Procedure).map(|(name, r)| {
        let props: serde_json::Map<String, Value> = r.params.iter().map(|p| (p.name.clone(), json!({"type": kind(p.ty.as_deref()), "description": p.ty}))).collect();
        let required: Vec<&str> = r.params.iter().filter(|p| p.default.is_none()).map(|p| p.name.as_str()).collect();
        let first = || r.body.lines().map(str::trim).find(|l| !l.is_empty()).filter(|l| l.starts_with("--") || l.starts_with('#')).map(|l| l.trim_start_matches(['-', '#', ' ']).to_string());
        let about = notes.get(name.as_str()).cloned().or_else(|| code.then(first).flatten());
        json!({"name": tool(name), "annotations": {"readOnlyHint": false}, "inputSchema": {"type": "object", "properties": props, "required": required},
               "description": format!("{} (the stored procedure {name}, LANGUAGE {}: CALL {name}(…))", about.unwrap_or_else(|| "A job this lake's owner wrote".into()), r.language)})
    }).collect();
    out.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    out
}

/// A procedure's name as a tool's (letters, digits, `_`, `-`; not one of the four).
fn tool(name: &str) -> String {
    let t = name.replace('.', "__");
    if ["list_tables", "query", "write", "changes"].contains(&t.as_str()) || crate::find::is_tool(&t) { format!("call_{t}") } else { t }
}

/// The procedure a tool stands for.
async fn procedure(app: &App, t: &str) -> Option<String> {
    let all = crate::routines::listed(&app.lake).await.ok()?;
    all.iter().find(|(n, r)| r.kind == crate::expander::Kind::Procedure && tool(n) == t).map(|(n, _)| n.clone())
}

/// This lake and the attached ones, with the prefix their tables go by.
fn lakes(app: &App) -> Vec<(String, Arc<Lake>)> {
    let attached = app.lake.attached.read().unwrap().iter().map(|(n, l)| (format!("{n}."), l.clone())).collect::<Vec<_>>();
    [(String::new(), app.lake.clone())].into_iter().chain(attached).collect()
}

/// May `scope` read `name`, a table or view of the lake `prefix` names? A user with grants sees only what it may
/// SELECT, as `pondra.tables` lists it.
fn may_see(scope: &crate::rights::Scope, prefix: &str, name: &str) -> bool {
    match prefix.is_empty() {
        true => scope.sees(crate::rights::Object::Table(name)),
        false => scope.sees_named(&format!("{prefix}{name}")), // (another lake's: the grants naming it)
    }
}

/// A column's type as SQL names it, for an agent that writes SQL: as `SHOW CREATE` writes it (`types::shown`), so it
/// reads back in a `CREATE TABLE` or a `CAST`: `BIGINT`, `VARCHAR(3)`, `DECIMAL(10, 2)`, `UUID`, an enum's type.
fn sql_type(m: &TableMeta, column: &str, t: &str) -> String { m.col(column).map_or_else(|| crate::objects::sql_type(t), crate::types::shown) }

/// The tables and views `scope` may read, here and in the attached lakes (`list_tables`).
async fn list(app: &App, scope: &crate::rights::Scope) -> Result<Value> {
    let mut tables = vec![];
    for (prefix, lake) in lakes(app) {
        if !prefix.is_empty() && crate::users::across(&lake, prefix.trim_end_matches('.')).await.is_err() {
            continue; // (another database that signs in on its own: not even its names, as `pondra.tables`)
        }
        let views = lake.cat.scan::<View>("v/", "v0").await?;
        // (what COMMENT ON TABLE or VIEW says of each: an agent reads a table's meaning there, a change log's for one)
        let about: std::collections::BTreeMap<String, String> =
            lake.cat.scan::<String>("cm/relation/", "cm/relation0").await?.into_iter().map(|(k, v)| (k["cm/relation/".len()..].to_string(), v)).collect();
        for (key, m) in lake.cat.scan::<TableMeta>("t/", "t0").await?.into_iter().filter(|(k, _)| !crate::sys::hidden(k)) {
            let name = &key[2..];
            if !may_see(scope, &prefix, name) {
                continue;
            }
            let kind = if !m.merge.is_empty() { "merge" } else if !m.key.is_empty() { "upsert" } else { "append" };
            let m = m.described(); // (SQL's names: ADR-022; a finished view's answers)
            // (a table granted in part: the columns granted, as every listing shows them: `rights::Scope::columns`)
            let shown = match prefix.is_empty() {
                true => scope.columns(name),
                false => scope.named("select", &format!("{prefix}{name}")),
            }
            .flatten();
            let reads = |c: &str| shown.as_ref().is_none_or(|s| s.contains(c));
            let columns: Vec<Value> = m.columns().filter(|(c, _)| *c != "_deleted" && reads(c)).map(|(c, t)| json!({"name": c, "type": sql_type(&m, c, t)})).collect();
            // (a key it may not read all of: no kind, key or merge either, which would say there is one)
            let keyed = m.key.iter().all(|c| reads(c));
            let kind = keyed.then_some(kind);
            let key: Vec<&String> = m.key.iter().filter(|_| keyed).collect();
            let merge: std::collections::BTreeMap<&String, &String> = m.merge.iter().filter(|(c, _)| keyed && reads(c)).collect();
            let view = scope.shows_code().then(|| views.iter().find(|(k, _)| &k[2..] == name).map(|(_, v)| v.sql.clone())).flatten(); // (a view's code: `views::code_for_caller`)
            tables.push(json!({"table": format!("{prefix}{name}"), "kind": kind, "key": key, "merge": merge, "columns": columns, "view": view, "about": about.get(name)}));
        }
        for (key, v) in lake.cat.scan::<crate::ddl::StoredView>("q/", "q0").await? {
            if !may_see(scope, &prefix, &key[2..]) {
                continue;
            }
            tables.push(json!({"table": format!("{prefix}{}", &key[2..]), "kind": "view", "view": scope.shows_code().then(|| crate::views::shown(&v.sql)), "about": about.get(&key[2..])})); // (CREATE VIEW: run when read)
        }
    }
    Ok(json!({"tables": tables}))
}

/// A query's rows as JSON. Never a write (the write tool's, and BEGIN, COMMIT and ROLLBACK too). Run as every door runs a
/// statement (`statement::run`): in a session it reads the transaction's snapshot and its own writes, and `$name`s are
/// the session's variables; several statements run as a script.
async fn query(app: &App, who: &crate::auth::Principal, sql: &str) -> Result<String> {
    if crate::statement::is_write(sql) {
        return Err(crate::codes::coded("25006", "this changes data or a transaction: use the write tool")); // (a read-only tool: Postgres's read_only_sql_transaction)
    }
    let req = crate::routines::Request { sql: sql.to_string(), ..Default::default() };
    let how = crate::statement::How { job: None, spread: None, after: None, stale: None };
    let who = crate::routines::Who::signed(who.clone(), who.role, false);
    match crate::auth::reading(crate::statement::run(app, req, how, who, &McpWire)).await? { // (nothing it runs moves a sequence: a view's nextval neither)
        crate::statement::Answer::Own(text) => Ok(text),
        crate::statement::Answer::Done(v) => Ok(v.to_string()),
        crate::statement::Answer::Rows(batches) => rows_json(&batches),
        _ => anyhow::bail!("an answer no tool asks for"), // (MCP keeps no answer and streams none)
    }
}

/// MCP's wire: a query's rows as JSON, kept nowhere (`kept_as` None: no results cache for a tool's answer).
struct McpWire;

impl crate::statement::Wire for McpWire {
    type Own = String;
    fn kept_as(&self) -> Option<std::borrow::Cow<'_, str>> { None }
    fn encode(&self, rows: Vec<RecordBatch>) -> Result<(Option<Bytes>, String)> { Ok((None, rows_json(&rows)?)) }
}

/// A query's answer as a tool's JSON: its first rows, and the total.
fn rows_json(batches: &[RecordBatch]) -> Result<String> { Rows { total_rows: Some(total(batches)), ..Rows::of(batches, ROWS)? }.text() }

/// A tool's answer that carries rows: the rows as every door writes them (`answer::json_first`, put in as written:
/// parsed back, a DECIMAL would keep only what a double holds), and what goes with them.
#[derive(serde::Serialize)]
struct Rows {
    rows: Box<serde_json::value::RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    notices: Vec<String>,
}

impl Rows {
    /// The first `n` rows, and nothing with them yet.
    fn of(batches: &[RecordBatch], n: usize) -> Result<Rows> {
        Ok(Rows { rows: crate::answer::json_first(batches, n, true)?, total_rows: None, position: None, notices: vec![] })
    }

    fn text(&self) -> Result<String> { Ok(serde_json::to_string(self)?) }
}

/// The write tool's SQL, run as POST /sql runs it (`run`): one statement or several, a
/// transaction's BEGIN … COMMIT in its session, and `job`, so a retry is applied once.
async fn write(app: &App, who: &crate::auth::Principal, sql: &str, job: Option<&str>) -> Result<String> {
    let (out, heard) = crate::routines::with_notices(run(app, who, sql, job.map(String::from))).await;
    answer(out, heard)
}

/// `sql` as POST /sql runs it (`routines::script`), as `role`: each statement audited once, by the
/// runner. Sent with no session, a script of several statements is one of its own, which ends
/// with it (a transaction left open too), as POST /sql makes it.
async fn run(app: &App, who: &crate::auth::Principal, sql: &str, job: Option<String>) -> Result<crate::routines::Outcome> {
    // A transaction word alone, with no session, has nothing to hold it: refused by name. Several statements in one call
    // (BEGIN; …; COMMIT) run as one script, in a session of their own, so they are a transaction.
    let parts = crate::routines::split(sql).len();
    if crate::temp::current().is_none() && parts == 1 && crate::text::control(sql).is_some() {
        return Err(crate::codes::coded("0A000", "a transaction needs a session: send initialize and keep the Mcp-Session-Id it gives"));
    }
    let who = crate::routines::Who::signed(who.clone(), who.role, false);
    let own = (crate::temp::current().is_none() && parts > 1).then(|| crate::temp::of_script(who.name()));
    // (named, so the future borrows values that live as long as it does)
    let (params, views) = (HashMap::new(), HashMap::new());
    let go = crate::routines::script(app, sql, &params, &views, who, job);
    let out = match &own {
        Some(s) => crate::temp::SESSION.scope(crate::temp::named(&app.lake, Some(s.clone())), go).await,
        None => go.await,
    };
    own.inspect(|s| {
        if let Some(at) = crate::temp::sessions(&app.lake) {
            at.end(s);
        }
    });
    out
}

/// A procedure's answer, as a tool's: its rows or outcome, and what it printed (`notices`).
async fn called(app: &App, who: &crate::auth::Principal, sql: &str, job: Option<&str>) -> Result<String> {
    let who = crate::routines::Who::signed(who.clone(), who.role, false);
    let (out, heard) = crate::routines::with_notices(crate::routines::one(app, sql, who, job.map(String::from))).await;
    answer(out, heard)
}

/// A statement's answer as a tool's: its rows or outcome, and what it printed (`notices`).
fn answer(out: Result<crate::routines::Outcome>, heard: Vec<String>) -> Result<String> {
    match out {
        Ok(crate::routines::Outcome::Rows(batches)) => Rows { notices: heard, ..Rows::of(&batches, ROWS)? }.text(),
        Ok(crate::routines::Outcome::Done(mut v)) => {
            if !heard.is_empty() {
                v["notices"] = json!(heard);
            }
            Ok(v.to_string())
        }
        Err(e) if !heard.is_empty() => Err(e.context(format!("it printed:\n{}", heard.join("\n")))),
        Err(e) => Err(e),
    }
}

fn total(batches: &[RecordBatch]) -> usize { batches.iter().map(RecordBatch::num_rows).sum() }

/// Rows committed after `after`, in steps of doubling size until about `ROWS` are in hand.
async fn changes(app: &App, who: &crate::auth::Principal, table: &str, after: Option<u64>) -> Result<String> {
    let found = lakes(app).into_iter().rev().find_map(|(p, l)| Some((table.strip_prefix(p.as_str())?.to_string(), p, l)));
    let (name, prefix, lake) = found.ok_or_else(|| crate::codes::coded("42P01", format!("no table {table}")))?;
    crate::rights::may(who, crate::rights::Action::SelectAll, crate::rights::Object::Table(table))?; // (a user's: every column of the rows, as `/watch` and Kafka need, before anything is read)
    if !prefix.is_empty() {
        crate::users::across(&lake, prefix.trim_end_matches('.')).await?; // (an attached lake that signs in on its own: its own sign-in, as queries use)
    }
    if lake.cat.meta(&crate::store::table_key(&name)).await?.is_none() {
        return Err(crate::codes::coded("42P01", format!("no table {table}")));
    }
    let now = lake.visible();
    let (mut at, mut step, mut got) = (after.unwrap_or(now).min(now), 16, vec![]);
    while at < now && got.iter().map(RecordBatch::num_rows).sum::<usize>() < ROWS {
        let upto = now.min(at + step);
        got.extend(crate::change::feed(&lake, &name, at, Some(upto)).await?);
        (at, step) = (upto, step * 2);
    }
    Rows { position: Some(at), ..Rows::of(&got, usize::MAX)? }.text() // (all of them: `position` is past every one)
}
