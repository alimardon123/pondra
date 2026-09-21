//! MCP (the Model Context Protocol) at `POST /mcp`: AI agents — Claude, Cursor, anything that
//! speaks MCP — list the tables, query them, write, and follow what changed, with the same tokens
//! as everyone else (`Authorization: Bearer …`). JSON-RPC 2.0 over MCP's "streamable HTTP"
//! transport, each answer plain JSON (no event stream: every tool answers at once).
use crate::auth::Role;
use crate::query::tail;
use crate::server::App;
use crate::store::{Lake, TableMeta};
use crate::views::View;
use anyhow::{anyhow, ensure, Result};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use datafusion::arrow::{json::ArrayWriter, record_batch::RecordBatch};
use serde_json::{json, Value};
use std::sync::Arc;

const ROWS: usize = 1000; // at most this many rows per answer: an agent reads them all

pub async fn handle(State(app): State<App>, Extension(role): Extension<Role>, body: Bytes) -> Response {
    let Ok(req) = serde_json::from_slice::<Value>(&body) else {
        return Json(json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "not JSON"}})).into_response();
    };
    let Some(id) = req.get("id").cloned() else { return StatusCode::ACCEPTED.into_response() }; // a notification
    let (method, params) = (req["method"].as_str().unwrap_or_default(), &req["params"]);
    let result = match method {
        "initialize" => json!({
            "protocolVersion": params["protocolVersion"].as_str().unwrap_or("2025-06-18"),
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "pondra", "version": env!("CARGO_PKG_VERSION")},
            "instructions": "Pondra is a streaming lakehouse: tables that take appends, upserts and deletes and \
                are queryable within milliseconds, in SQL (Apache DataFusion's dialect, close to PostgreSQL). \
                Start with list_tables."}),
        "ping" => json!({}),
        "tools/list" => json!({"tools": tools()}),
        "tools/call" => call(&app, role, params).await,
        _ => return Json(json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": format!("no method {method}")}})).into_response(),
    };
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result})).into_response()
}

fn tools() -> Value {
    let args = |props: Value, required: &[&str]| json!({"type": "object", "properties": props, "required": required});
    let sql = json!({"sql": {"type": "string"}});
    json!([
        {"name": "list_tables", "annotations": {"readOnlyHint": true}, "inputSchema": args(json!({}), &[]),
         "description": "Every table (attached lakes' as name.table): its columns; its kind — append, upsert (a primary key: \
             the newest row per key wins) or merge (rows per key combine, e.g. sum); and for views, the SQL they follow."},
        {"name": "query", "annotations": {"readOnlyHint": true}, "inputSchema": args(sql.clone(), &["sql"]),
         "description": format!("Run one SQL query and get its rows as JSON (the first {ROWS}, and the total). \
             `WHERE key = …` on an upsert table is a fast point lookup. Vector search: ORDER BY cosine_distance(embedding, [0.1, …]) LIMIT k.")},
        {"name": "write", "annotations": {"readOnlyHint": false, "destructiveHint": true, "idempotentHint": true},
         "inputSchema": args(json!({"sql": {"type": "string"}, "job": {"type": "string", "description": "an id for this write: retried with the same id, it is applied once"}}), &["sql"]),
         "description": "CREATE TABLE, INSERT, UPDATE or DELETE (UPDATE and DELETE on tables with a primary key). \
             Committed and durable when this returns. Needs a write token; CREATE TABLE an admin token."},
        {"name": "changes", "annotations": {"readOnlyHint": true},
         "inputSchema": args(json!({"table": {"type": "string"}, "after": {"type": "integer", "description": "a position from an earlier call; 0 for as far back as kept; none for from now"}}), &["table"]),
         "description": format!("What was committed to a table after a position — every append, upsert and delete (`_deleted`) — \
             about {ROWS} rows at a time, and the position to ask from next.")},
    ])
}

async fn call(app: &App, role: Role, params: &Value) -> Value {
    let arg = |k: &str| params["arguments"][k].as_str().unwrap_or_default();
    let out = match params["name"].as_str().unwrap_or_default() {
        "list_tables" => list(app).await,
        "query" => query(app, arg("sql")).await,
        "write" => write(app, role, arg("sql"), params["arguments"]["job"].as_str()).await,
        "changes" => changes(app, arg("table"), params["arguments"]["after"].as_u64()).await,
        other => Err(anyhow!("no tool {other}")),
    };
    let (text, error) = match out {
        Ok(v) => (v.to_string(), false),
        Err(e) => (format!("{e:#}"), true), // (a tool error, which the agent sees and can act on)
    };
    json!({"content": [{"type": "text", "text": text}], "isError": error})
}

/// This lake and the attached ones, with the prefix their tables go by.
fn lakes(app: &App) -> Vec<(String, Arc<Lake>)> {
    let attached = app.lake.attached.read().unwrap().iter().map(|(n, l)| (format!("{n}."), l.clone())).collect::<Vec<_>>();
    [(String::new(), app.lake.clone())].into_iter().chain(attached).collect()
}

async fn list(app: &App) -> Result<Value> {
    let mut tables = vec![];
    for (prefix, lake) in lakes(app) {
        let views = lake.cat.scan::<View>("v/", "v0").await?;
        for (key, m) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
            let name = &key[2..];
            let kind = if !m.merge.is_empty() { "merge" } else if !m.key.is_empty() { "upsert" } else { "append" };
            let columns: Vec<Value> = m.columns.iter().filter(|(c, _)| c != "_deleted").map(|(c, t)| json!({"name": c, "type": t})).collect();
            let view = views.iter().find(|(k, _)| &k[2..] == name).map(|(_, v)| v.sql.clone());
            tables.push(json!({"table": format!("{prefix}{name}"), "kind": kind, "key": m.key, "merge": m.merge, "columns": columns, "view": view}));
        }
    }
    Ok(json!({"tables": tables}))
}

async fn query(app: &App, sql: &str) -> Result<Value> {
    ensure!(crate::write::parse(sql).is_none(), "this is a write: use the write tool");
    let batches = app.query(sql, None).await?;
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    Ok(json!({"rows": rows(&batches)?, "total_rows": total}))
}

async fn write(app: &App, role: Role, sql: &str, job: Option<&str>) -> Result<Value> {
    let stmt = crate::write::parse(sql).ok_or_else(|| anyhow!("not a write (CREATE TABLE, INSERT, UPDATE, DELETE): use the query tool"))?;
    app.auth.allows(role, &stmt)?;
    crate::write::on_node(app, stmt, job.map(String::from)).await
}

/// Rows committed after `after`, in steps of doubling size until about `ROWS` are in hand.
async fn changes(app: &App, table: &str, after: Option<u64>) -> Result<Value> {
    let found = lakes(app).into_iter().rev().find_map(|(p, l)| Some((table.strip_prefix(p.as_str())?.to_string(), l)));
    let (name, lake) = found.ok_or_else(|| anyhow!("no table {table}"))?;
    ensure!(lake.cat.get::<TableMeta>(&crate::store::table_key(&name)).await?.is_some(), "no table {table}");
    let now = lake.visible();
    let (mut at, mut step, mut got) = (after.unwrap_or(now).min(now), 16, vec![]);
    while at < now && got.iter().map(RecordBatch::num_rows).sum::<usize>() < ROWS {
        let upto = now.min(at + step);
        got.extend(tail(&lake, &name, at, Some(upto), false).await?);
        (at, step) = (upto, step * 2);
    }
    Ok(json!({"rows": rows(&got)?, "position": at}))
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
