//! MCP (the Model Context Protocol) at `POST /mcp`: AI agents — Claude, Cursor, anything that
//! speaks MCP — list the tables, query them, write, and follow what changed, with the same tokens
//! as everyone else (`Authorization: Bearer …`). JSON-RPC 2.0 over MCP's "streamable HTTP"
//! transport, each answer plain JSON (no event stream: every tool answers at once).
//!
//! A session: `initialize` gives the client an `Mcp-Session-Id`, and the tool calls that send it
//! back share one session, so a `write` of `BEGIN` is a transaction the next calls are in, until
//! `COMMIT`, `ROLLBACK`, or `DELETE /mcp` with the id (`txn.rs`, `temp.rs`).
use crate::auth::Role;
use crate::app::App;
use crate::store::{Lake, TableMeta};
use crate::views::View;
use anyhow::Result;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use datafusion::arrow::{json::ArrayWriter, record_batch::RecordBatch};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

const ROWS: usize = 1000; // at most this many rows per answer: an agent reads them all

pub async fn handle(State(app): State<App>, Extension(role): Extension<Role>, headers: HeaderMap, body: Bytes) -> Response {
    let Ok(req) = serde_json::from_slice::<Value>(&body) else {
        return Json(json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "not JSON"}})).into_response();
    };
    let Some(id) = req.get("id").cloned() else { return StatusCode::ACCEPTED.into_response() }; // a notification
    let session = match named(&headers) {
        Ok(session) => session,
        Err(refused) => return refused,
    };
    let (method, params) = (req["method"].as_str().unwrap_or_default(), &req["params"]);
    // A request that names a session must name one this node holds (made by initialize, used within
    // the idle time, not ended): otherwise 404, and the client initializes again (the Streamable HTTP
    // spec). A request with no session header is stateless, as before.
    if let (Some(s), false) = (&session, method == "initialize") {
        if !crate::temp::live(s) {
            return gone(&id);
        }
        crate::temp::touch(s);
    }
    let mut minted = None; // (`initialize` gives the client its session)
    let result = match method {
        "initialize" => {
            let raw = format!("mcp-{}", uuid::Uuid::new_v4().simple());
            let _ = crate::temp::with(&crate::temp::id(&raw), false, |_| Ok(())); // (held from now: idle ends it, `reap`)
            minted = Some(raw);
            json!({
                "protocolVersion": params["protocolVersion"].as_str().unwrap_or("2025-06-18"),
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "pondra", "version": env!("CARGO_PKG_VERSION")},
                "instructions": "Pondra is a streaming lakehouse: tables that take appends, upserts and deletes and \
                    are queryable within milliseconds, in SQL (Apache DataFusion's dialect, close to PostgreSQL). \
                    Start with list_tables."})
        }
        "ping" => json!({}),
        "tools/list" => {
            let all = [tools().as_array().cloned().unwrap_or_default(), crate::find::mcp_tools(), procedures(&app).await].concat();
            json!({"tools": all})
        }
        "tools/call" => match crate::temp::SESSION.scope(session, call(&app, role, params)).await {
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
fn named(headers: &HeaderMap) -> Result<Option<String>, Response> {
    let Some(h) = headers.get("mcp-session-id") else { return Ok(None) };
    match h.to_str().ok().filter(|id| valid(id)) {
        Some(id) => Ok(Some(crate::temp::id(id))),
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
pub async fn end(headers: HeaderMap) -> Response {
    match named(&headers) {
        Ok(Some(session)) if crate::temp::live(&session) => {
            crate::temp::end(&session);
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
             or view (a stored query); and for views, their SQL."},
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

/// A tool's answer as its result, or the JSON-RPC error for a tool the server doesn't have
/// (-32602, Invalid params, with its SQLSTATE 42883 in `data`). A tool that fails is a result with
/// `isError`, its message carrying the SQLSTATE: the agent sees it and can act on it.
async fn call(app: &App, role: Role, params: &Value) -> Result<Value, Value> {
    let arg = |k: &str| params["arguments"][k].as_str().unwrap_or_default();
    let out = match params["name"].as_str().unwrap_or_default() {
        "list_tables" => list(app).await,
        "query" => query(app, role, arg("sql")).await,
        "write" => write(app, role, arg("sql"), params["arguments"]["job"].as_str()).await,
        "changes" => changes(app, arg("table"), params["arguments"]["after"].as_u64()).await,
        name if crate::find::is_tool(name) => match crate::find::mcp_sql(name, &params["arguments"]) {
            Ok(sql) => query(app, role, &sql).await,
            Err(e) => Err(e),
        },
        other => {
            let Some(name) = procedure(app, other).await else {
                return Err(json!({"code": -32602, "message": format!("no tool {other}"), "data": {"sqlstate": "42883"}}));
            };
            let given = params["arguments"].as_object().into_iter().flatten().map(|(k, v)| Ok(format!("{k} => {}", crate::routines::literal(v)?))).collect::<Result<Vec<_>>>();
            match given {
                Ok(given) => called(app, role, &format!("CALL {name}({})", given.join(", ")), None).await,
                Err(e) => Err(e),
            }
        }
    };
    let (text, error) = match out {
        Ok(v) => (v.to_string(), false),
        Err(e) => {
            // (a tool error, which the agent sees and can act on: Postgres's code and the words every door gives, a
            // routine's code cut for a user who may not see it)
            let (_, code, words) = crate::codes::answer(&e, crate::auth::shows_code());
            (format!("SQLSTATE {code}: {words}"), true)
        }
    };
    Ok(json!({"content": [{"type": "text", "text": text}], "isError": error}))
}

/// Each stored procedure is a tool too, its parameters the tool's (ADR-023): what a lake's owner
/// wrote for a job becomes something an agent can do, with the agent's token's rights.
async fn procedures(app: &App) -> Vec<Value> {
    let Ok(all) = crate::routines::listed(&app.lake).await else { return vec![] };
    let kind = |t: Option<&str>| match t.unwrap_or_default().split('(').next().unwrap_or_default().to_uppercase().as_str() {
        "BIGINT" | "INT" | "INTEGER" | "SMALLINT" | "TINYINT" => "integer",
        "DOUBLE" | "REAL" | "FLOAT" | "DECIMAL" | "NUMERIC" => "number",
        "BOOLEAN" | "BOOL" => "boolean",
        _ => "string",
    };
    let mut out: Vec<Value> = all.iter().filter(|(_, r)| r.kind == crate::expander::Kind::Procedure).map(|(name, r)| {
        let props: serde_json::Map<String, Value> = r.params.iter().map(|p| (p.name.clone(), json!({"type": kind(p.ty.as_deref()), "description": p.ty}))).collect();
        let required: Vec<&str> = r.params.iter().filter(|p| p.default.is_none()).map(|p| p.name.as_str()).collect();
        let about = r.body.lines().map(str::trim).find(|l| !l.is_empty()).filter(|l| l.starts_with("--") || l.starts_with('#')).map(|l| l.trim_start_matches(['-', '#', ' ']).to_string());
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

/// May the request being served read `name`, a table or view of the lake `prefix` names? A user
/// with grants sees only what it may SELECT, as `pondra.tables` lists it.
fn may_see(prefix: &str, name: &str) -> bool {
    crate::auth::limited().is_none_or(|a| a.may("select", &format!("{prefix}{name}")))
}

async fn list(app: &App) -> Result<Value> {
    let mut tables = vec![];
    for (prefix, lake) in lakes(app) {
        if !prefix.is_empty() && crate::users::across(&lake, prefix.trim_end_matches('.')).await.is_err() {
            continue; // (another database that signs in on its own: not even its names, as `pondra.tables`)
        }
        let views = lake.cat.scan::<View>("v/", "v0").await?;
        for (key, m) in lake.cat.scan::<TableMeta>("t/", "t0").await?.into_iter().filter(|(k, _)| !crate::sys::hidden(k)) {
            let name = &key[2..];
            if !may_see(&prefix, name) {
                continue;
            }
            let kind = if !m.merge.is_empty() { "merge" } else if !m.key.is_empty() { "upsert" } else { "append" };
            let m = m.described(); // (SQL's names: ADR-022; a finished view's answers)
            let columns: Vec<Value> = m.columns.iter().filter(|(c, _)| c != "_deleted").map(|(c, t)| json!({"name": c, "type": t})).collect();
            let view = views.iter().find(|(k, _)| &k[2..] == name).map(|(_, v)| v.sql.clone());
            tables.push(json!({"table": format!("{prefix}{name}"), "kind": kind, "key": m.key, "merge": m.merge, "columns": columns, "view": view}));
        }
        for (key, v) in lake.cat.scan::<crate::ddl::StoredView>("q/", "q0").await? {
            if !may_see(&prefix, &key[2..]) {
                continue;
            }
            tables.push(json!({"table": format!("{prefix}{}", &key[2..]), "kind": "view", "view": v.sql})); // (CREATE VIEW: run when read)
        }
    }
    Ok(json!({"tables": tables}))
}

/// A query's rows as JSON. Never a write (the write tool's, and BEGIN, COMMIT and ROLLBACK too).
/// In a session it runs as POST /sql runs a statement sent with its session: in a transaction it
/// reads the transaction's snapshot and its own writes, and `$name`s are the session's variables.
async fn query(app: &App, role: Role, sql: &str) -> Result<Value> {
    if is_write(sql) {
        return Err(crate::codes::coded("25006", "this changes data or a transaction: use the write tool")); // (a read-only tool: Postgres's read_only_sql_transaction)
    }
    if crate::temp::current().is_some() {
        return Ok(match run(app, role, sql, None).await? {
            crate::routines::Outcome::Rows(batches) => json!({"rows": rows(&batches)?, "total_rows": total(&batches)}),
            crate::routines::Outcome::Done(v) => v,
        });
    }
    let sql = &crate::routines::expand(&app.lake, sql).await?; // (macros: ADR-023)
    let batches = crate::audit::statement(app, sql, app.query(sql, None)).await?;
    Ok(json!({"rows": rows(&batches)?, "total_rows": total(&batches)}))
}

/// The write tool's SQL, run as POST /sql runs it (`run`): one statement or several, a
/// transaction's BEGIN … COMMIT in its session, and `job`, so a retry is applied once.
async fn write(app: &App, role: Role, sql: &str, job: Option<&str>) -> Result<Value> {
    let (out, heard) = crate::routines::with_notices(run(app, role, sql, job.map(String::from))).await;
    answer(out, heard)
}

/// `sql` as POST /sql runs it (`routines::script`), as `role`: each statement audited once, by the
/// runner. Sent with no session, a script of several statements is one of its own, which ends
/// with it (a transaction left open too), as POST /sql makes it.
async fn run(app: &App, role: Role, sql: &str, job: Option<String>) -> Result<crate::routines::Outcome> {
    // A transaction word alone, with no session, has nothing to hold it: refused by name. Several statements in one call
    // (BEGIN; …; COMMIT) run as one script, in a session of their own, so they are a transaction.
    let parts = crate::routines::split(sql).len();
    if crate::temp::current().is_none() && parts == 1 && crate::text::control(sql).is_some() {
        return Err(crate::codes::coded("0A000", "a transaction needs a session: send initialize and keep the Mcp-Session-Id it gives"));
    }
    let who = crate::routines::Who { role, files: false, depth: 0 };
    let own = (crate::temp::current().is_none() && parts > 1).then(crate::temp::of_script);
    // (named, so the future borrows values that live as long as it does)
    let (params, views) = (HashMap::new(), HashMap::new());
    let go = crate::routines::script(app, sql, &params, &views, who, job);
    let out = match &own {
        Some(s) => crate::temp::SESSION.scope(Some(s.clone()), go).await,
        None => go.await,
    };
    own.inspect(|s| _ = crate::temp::end(s));
    out
}

/// A procedure's answer, as a tool's: its rows or outcome, and what it printed (`notices`).
async fn called(app: &App, role: Role, sql: &str, job: Option<&str>) -> Result<Value> {
    let who = crate::routines::Who { role, files: false, depth: 0 };
    let (out, heard) = crate::routines::with_notices(crate::routines::one(app, sql, who, job.map(String::from))).await;
    answer(out, heard)
}

/// A statement's answer as a tool's: its rows or outcome, and what it printed (`notices`).
fn answer(out: Result<crate::routines::Outcome>, heard: Vec<String>) -> Result<Value> {
    let mut v = match out {
        Ok(crate::routines::Outcome::Rows(batches)) => json!({"rows": rows(&batches)?}),
        Ok(crate::routines::Outcome::Done(v)) => v,
        Err(e) if !heard.is_empty() => return Err(e.context(format!("it printed:\n{}", heard.join("\n")))),
        Err(e) => return Err(e),
    };
    if !heard.is_empty() {
        v["notices"] = json!(heard);
    }
    Ok(v)
}

/// Changes data or a transaction (INSERT, CALL, BEGIN…): not for the query tool.
fn is_write(sql: &str) -> bool {
    crate::write::parse(sql).is_some() || crate::routines::runs_procedure(sql) || crate::text::control(sql).is_some()
}

fn total(batches: &[RecordBatch]) -> usize { batches.iter().map(RecordBatch::num_rows).sum() }

/// Rows committed after `after`, in steps of doubling size until about `ROWS` are in hand.
async fn changes(app: &App, table: &str, after: Option<u64>) -> Result<Value> {
    let found = lakes(app).into_iter().rev().find_map(|(p, l)| Some((table.strip_prefix(p.as_str())?.to_string(), p, l)));
    let (name, prefix, lake) = found.ok_or_else(|| crate::codes::coded("42P01", format!("no table {table}")))?;
    crate::auth::check_all(table)?; // (a user's: every column of the rows, as `/watch` and Kafka need, before anything is read)
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
    Ok(json!({"rows": all_rows(&got)?, "position": at})) // (all of them: `position` is past every one)
}

/// Every row as a JSON object.
fn all_rows(batches: &[RecordBatch]) -> Result<Value> {
    let mut w = ArrayWriter::new(Vec::new());
    batches.iter().try_for_each(|b| w.write(b))?;
    w.finish()?;
    let out = w.into_inner();
    Ok(if out.is_empty() { json!([]) } else { serde_json::from_slice(&out)? })
}

/// The first `ROWS` rows as JSON objects.
fn rows(batches: &[RecordBatch]) -> Result<Value> {
    let (mut left, mut w) = (ROWS, ArrayWriter::new(Vec::new()));
    for b in batches {
        let n = left.min(b.num_rows());
        w.write(&b.slice(0, n))?;
        left -= n;
    }
    w.finish()?;
    let out = w.into_inner();
    Ok(if out.is_empty() { json!([]) } else { serde_json::from_slice(&out)? })
}
