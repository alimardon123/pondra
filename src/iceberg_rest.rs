//! The Iceberg REST catalog's HTTP door over what `iceberg.rs` publishes and records: the routes,
//! their handlers and the answers they give. What a commit means and how the leader records it is
//! `iceberg.rs`'s, a lake module; this one is axum's, so it is a doors module.

use crate::iceberg::{loaded, prepare, requirements, Commit, Published, CONFLICT, REFUSED};
use crate::store::Lake;
use anyhow::{Context, Result};
use serde_json::{json, Value};

/// A refusal as this door answers it: `iceberg.rs`'s status and body, the body as an axum `Json`.
type Refusal = (axum::http::StatusCode, axum::Json<Value>);

fn door((code, body): crate::iceberg::Refusal) -> Refusal { (code, axum::Json(body)) }

fn refused(code: u16, kind: &str, message: String) -> Refusal { door(crate::iceberg::refused(code, kind, message)) }

fn bad(message: String) -> Refusal { door(crate::iceberg::bad(message)) }

fn conflict(message: String) -> Refusal { door(crate::iceberg::conflict(message)) }

// ---------------------------------------------------------------- the REST catalog

/// The Iceberg REST catalog API over what Pondra publishes: engines attach a node by URL —
/// PyIceberg, DuckDB, Spark, Trino, Snowflake — instead of pointing at metadata files, and append
/// to its tables (`update`, ADR-028). Namespace `default` is this lake's `public` schema, and each
/// other schema is a namespace of its own; an attached lake `l` is namespace `l` (its `public`),
/// and `l.s` (`["l", "s"]`) for its others. Tables appear once they publish Iceberg. Tokens work
/// as elsewhere (`Authorization: Bearer …`; a commit needs a writer's).
pub fn rest() -> axum::Router<crate::server::App> {
    use axum::routing::{get, post};
    let endpoints = ["GET /v1/{prefix}/namespaces", "POST /v1/{prefix}/namespaces", "GET /v1/{prefix}/namespaces/{namespace}", "HEAD /v1/{prefix}/namespaces/{namespace}", "DELETE /v1/{prefix}/namespaces/{namespace}",
        "GET /v1/{prefix}/namespaces/{namespace}/tables", "POST /v1/{prefix}/namespaces/{namespace}/tables", "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}", "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}", "POST /v1/{prefix}/tables/rename", "POST /v1/{prefix}/transactions/commit"];
    axum::Router::new()
        .route("/v1/config", get(move || async move { axum::Json(json!({"defaults": {}, "overrides": {}, "endpoints": endpoints})) }))
        .route("/v1/namespaces", get(namespaces).post(create_namespace))
        .route("/v1/namespaces/{ns}", get(namespace).head(namespace).delete(drop_namespace))
        .route("/v1/namespaces/{ns}/tables", get(tables).post(create))
        .route("/v1/namespaces/{ns}/tables/{table}", get(load).head(load).post(update).delete(drop_table))
        .route("/v1/tables/rename", post(rename))
        .route("/v1/transactions/commit", post(transaction))
}

type Reply = Result<axum::Json<Value>, (axum::http::StatusCode, axum::Json<Value>)>;

fn missing(what: &str, kind: &str) -> (axum::http::StatusCode, axum::Json<Value>) {
    (axum::http::StatusCode::NOT_FOUND, axum::Json(json!({"error": {"message": format!("no {what}"), "type": kind, "code": 404}})))
}

/// Every namespace: its parts, its lake, and the schema in it.
async fn spaces(app: &crate::server::App) -> Vec<(Vec<String>, std::sync::Arc<Lake>, String)> {
    use crate::ddl::schemas;
    use crate::text::PUBLIC;
    let attached = app.lake.attached.read().unwrap().clone();
    let mut out = vec![];
    for (name, lake) in [(None, app.lake.clone())].into_iter().chain(attached.into_iter().map(|(n, l)| (Some(n), l))) {
        for s in schemas(&lake).await.unwrap_or_default() {
            let parts = match (&name, s == PUBLIC) {
                (None, true) => vec!["default".to_string()],
                (None, false) => vec![s.clone()],
                (Some(n), true) => vec![n.clone()],
                (Some(n), false) => vec![n.clone(), s.clone()],
            };
            out.push((parts, lake.clone(), s));
        }
    }
    out
}

/// A namespace from the URL (its parts joined by 0x1F, as the REST spec has it). An attached lake's
/// only under its own sign-in (`users::across`), and not for a user granted some tables: an engine
/// reads whole files, and only grants naming another database's tables count.
async fn space(app: &crate::server::App, ns: &str) -> Result<(Vec<String>, std::sync::Arc<Lake>, String), (axum::http::StatusCode, axum::Json<Value>)> {
    let parts: Vec<&str> = ns.split('\u{1f}').collect();
    let found = spaces(app).await.into_iter().find(|(p, _, _)| *p == parts).ok_or_else(|| missing(&format!("namespace {}", parts.join(".")), "NoSuchNamespaceException"))?;
    if !std::sync::Arc::ptr_eq(&found.1, &app.lake) {
        crate::users::across(&found.1, &found.0[0]).await.map_err(|e| refused(403, "ForbiddenException", crate::ext::said(&e)))?;
        if crate::auth::limited().is_some() {
            return Err(refused(403, "ForbiddenException", format!("permission denied: {} is another database: its tables are read through it (the database {})", found.0[0], found.0[0])));
        }
    }
    Ok(found)
}

async fn namespaces(axum::extract::State(app): axum::extract::State<crate::server::App>) -> axum::Json<Value> {
    axum::Json(json!({"namespaces": spaces(&app).await.into_iter().map(|(p, _, _)| p).collect::<Vec<_>>()}))
}

async fn namespace(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::extract::Path(ns): axum::extract::Path<String>) -> Reply {
    let (parts, _, _) = space(&app, &ns).await?;
    Ok(axum::Json(json!({"namespace": parts, "properties": {}})))
}

/// `POST /v1/namespaces`: `CREATE SCHEMA`, run as the caller (DDL: an admin's), in this lake: an
/// attached lake's schemas are made by its own nodes, as in SQL. Its properties are none but
/// `owner` (Spark's, the caller as in SQL); another is refused by name, since a schema keeps none.
async fn create_namespace(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, body: bytes::Bytes) -> Reply {
    let asked: Value = serde_json::from_slice(&body).map_err(|e| bad(format!("the namespace: {e}")))?;
    let parts: Vec<String> = asked["namespace"].as_array().into_iter().flatten().filter_map(Value::as_str).map(String::from).collect();
    if let Some(p) = asked["properties"].as_object().and_then(|p| p.keys().find(|k| *k != "owner")) {
        return Err(bad(format!("the namespace property {p}: Pondra's schemas keep no properties")));
    }
    if spaces(&app).await.iter().any(|(p, _, _)| *p == parts) {
        return Err(refused(409, "AlreadyExistsException", format!("namespace {} already exists", parts.join("."))));
    }
    let [s] = &parts[..] else {
        return Err(bad(format!("namespace {}: a schema of this database is one part; an attached database's schemas are made by its own nodes", parts.join("."))));
    };
    as_caller(&app, role, &format!("CREATE SCHEMA {}", quoted(s))).await?;
    Ok(axum::Json(json!({"namespace": parts, "properties": {}})))
}

/// `DELETE /v1/namespaces/{ns}`: `DROP SCHEMA`, as the caller. One that holds anything is `409
/// NamespaceNotEmptyException`, as the REST specification has it (`as_caller`).
async fn drop_namespace(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, axum::extract::Path(ns): axum::extract::Path<String>) -> Result<axum::http::StatusCode, Refusal> {
    let (parts, lake, schema) = space(&app, &ns).await?;
    if !std::sync::Arc::ptr_eq(&lake, &app.lake) {
        return Err(bad(format!("namespace {} is {}'s, an attached database: its schemas are dropped by its own nodes", parts.join("."), parts[0])));
    }
    as_caller(&app, role, &format!("DROP SCHEMA {}", quoted(&schema))).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

fn quoted(n: &str) -> String {
    format!("\"{}\"", n.replace('"', "\"\""))
}

/// The tables with Iceberg metadata published.
async fn tables(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::extract::Path(ns): axum::extract::Path<String>) -> Reply {
    let (parts, lake, schema) = space(&app, &ns).await?;
    let published = lake.cat.scan::<Published>("i/", "i0").await.unwrap_or_default();
    let ids: Vec<Value> = published.iter().filter(|(k, p)| p.version > 0 && crate::text::split(&k[2..]).0 == schema)
        .map(|(k, _)| json!({"namespace": parts, "name": crate::text::split(&k[2..]).1})).collect();
    Ok(axum::Json(json!({"identifiers": ids})))
}

/// A table's name as SQL says it, from its namespace (`spaces`).
fn sql_name(app: &crate::server::App, parts: &[String], lake: &std::sync::Arc<Lake>, schema: &str, table: &str) -> String {
    let q = quoted;
    match (std::sync::Arc::ptr_eq(lake, &app.lake), schema == crate::text::PUBLIC) {
        (true, true) => q(table),
        (true, false) => format!("{}.{}", q(schema), q(table)),
        (false, _) => format!("{}.{}.{}", q(&parts[0]), q(schema), q(table)),
    }
}

/// A statement for the catalog's table changes, run as the caller (DDL: an admin's).
async fn as_caller(app: &crate::server::App, role: crate::auth::Role, sql: &str) -> Result<(), Refusal> {
    let who = crate::routines::Who { role, files: false, depth: 0 };
    Box::pin(crate::routines::one(app, sql, who, None)).await.map(|_| ()).map_err(|e| {
        let (raw, words) = (format!("{e:#}"), crate::ext::said(&e)); // (the kind by the engine's words, the message as said)
        match () {
            _ if raw.contains("may not") => refused(403, "ForbiddenException", words),
            _ if raw.contains("already exists") => refused(409, "AlreadyExistsException", words),
            _ if raw.contains("isn't empty") => refused(409, "NamespaceNotEmptyException", words),
            _ => bad(words),
        }
    })
}

/// `POST /v1/namespaces/{ns}/tables`: a table made through the catalog (ADR-029 §8), as `CREATE
/// TABLE` makes it: its schema's types as Pondra's (a required field NOT NULL, the identifier
/// fields its key), its partition spec as `partition_by` (one field: identity, or year, month,
/// day or hour), its write order as `cluster_by` (columns sorted ascending), published as Iceberg
/// (and anything else its `publish` property names). The metadata answered is its first
/// version's. What Pondra's tables can't be is refused by name.
async fn create(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, axum::extract::Path(ns): axum::extract::Path<String>, body: bytes::Bytes) -> Reply {
    let (parts, lake, schema) = space(&app, &ns).await?;
    let asked: Value = serde_json::from_slice(&body).map_err(|e| bad(format!("the table: {e}")))?;
    let table = asked["name"].as_str().filter(|n| !n.is_empty()).ok_or_else(|| bad("a table without its name".into()))?;
    let sql = create_sql(&sql_name(&app, &parts, &lake, &schema, table), &asked).map_err(|e| bad(crate::ext::said(&e)))?;
    as_caller(&app, role, &sql).await?;
    let name = crate::text::join(&schema, table);
    for _ in 0..600 {
        if let Ok(v) = loaded(&lake, &name).await {
            return Ok(axum::Json(v)); // (its first version: published by the leader's next round)
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Err(refused(500, "ServiceUnavailableException", format!("{name} was made, and isn't published yet: load it again")))
}

/// An Iceberg field's type as Pondra's SQL says it.
fn sql_type(t: &Value) -> Result<String> {
    use anyhow::bail;
    let prim = |t: &str| -> Result<String> {
        Ok(match t {
            "boolean" => "BOOLEAN".into(),
            "int" => "INT".into(),
            "long" => "BIGINT".into(),
            "float" => "REAL".into(),
            "double" => "DOUBLE".into(),
            "date" => "DATE".into(),
            "timestamp" => "TIMESTAMP".into(),
            "timestamptz" => "TIMESTAMPTZ".into(),
            "string" => "VARCHAR".into(),
            "binary" => "BYTEA".into(),
            t if t.starts_with("decimal(") => t.to_uppercase(),
            t => bail!("an Iceberg {t} column: not one Pondra has (boolean, int, long, float, double, date, timestamp, timestamptz, string, binary, decimal, and lists of them)"),
        })
    };
    match t {
        Value::String(t) => prim(t),
        t if t["type"] == "list" && t["element"].is_string() => Ok(format!("{}[]", prim(t["element"].as_str().unwrap_or_default())?)),
        t => bail!("an Iceberg {} column: not one Pondra has (lists of plain values are)", t["type"]),
    }
}

/// A catalog's table request as `CREATE TABLE` (`create`).
fn create_sql(name: &str, asked: &Value) -> Result<String> {
    use anyhow::bail;
    anyhow::ensure!(!asked["stage-create"].as_bool().unwrap_or(false), "a staged create (a table made with its first commit): not yet; make it, then append");
    let fields = asked["schema"]["fields"].as_array().context("a table without its schema's fields")?;
    let q = |n: &str| format!("\"{}\"", n.replace('"', "\"\""));
    let by_id = |id: &Value| fields.iter().find(|f| f["id"] == *id).and_then(|f| f["name"].as_str()).with_context(|| format!("no field {id} in the schema"));
    let mut columns = vec![];
    for f in fields {
        let n = f["name"].as_str().context("a field without its name")?;
        columns.push(format!("{} {}{}", q(n), sql_type(&f["type"])?, if f["required"].as_bool().unwrap_or(false) { " NOT NULL" } else { "" }));
    }
    let keys: Vec<String> = asked["schema"]["identifier-field-ids"].as_array().into_iter().flatten().map(|id| by_id(id).map(q)).collect::<Result<_>>()?;
    if !keys.is_empty() {
        columns.push(format!("PRIMARY KEY ({})", keys.join(", ")));
    }
    let mut options = vec![];
    let publish = asked["properties"]["publish"].as_str().unwrap_or("iceberg");
    anyhow::ensure!(publish.split(',').any(|f| f.trim() == "iceberg"), "publish = '{publish}': a table made through the Iceberg catalog publishes Iceberg");
    options.push(format!("publish = '{}'", publish.replace('\'', "")));
    let spec = asked["partition-spec"]["fields"].as_array().cloned().unwrap_or_default();
    match spec.as_slice() {
        [] => {}
        [f] => {
            let col = by_id(&f["source-id"])?;
            let by = match f["transform"].as_str().unwrap_or("identity") {
                "identity" => col.to_string(),
                t @ ("year" | "month" | "day" | "hour") => format!("{t}({col})"),
                t => bail!("a partition by {t}: Pondra's tables partition by a column, or the year, month, day or hour of one"),
            };
            options.push(format!("partition_by = '{}'", by.replace('\'', "")));
        }
        _ => bail!("a partition spec of {} fields: Pondra's tables partition by one", spec.len()),
    }
    let order = asked["write-order"]["fields"].as_array().cloned().unwrap_or_default();
    if !order.is_empty() {
        let mut cols = vec![];
        for f in &order {
            anyhow::ensure!(f["transform"].as_str().unwrap_or("identity") == "identity" && f["direction"].as_str().unwrap_or("asc") == "asc", "a write order by {} {}: Pondra's tables sort by columns, ascending (cluster_by)", f["transform"], f["direction"]);
            cols.push(by_id(&f["source-id"])?.replace('\'', ""));
        }
        options.push(format!("cluster_by = '{}'", cols.join(",")));
    }
    Ok(format!("CREATE TABLE {name} ({}) WITH ({})", columns.join(", "), options.join(", ")))
}

/// A commit's table properties (`set-properties`, `remove-properties`: Spark's `ALTER TABLE … SET
/// TBLPROPERTIES`, say) as the `ALTER TABLE … SET` that keeps them for other engines, '' taking one
/// out; the ones Pondra writes itself (its name mapping) are its own.
fn properties_sql(name: &str, updates: &[Value]) -> Option<String> {
    let own = |k: &str| ["schema.name-mapping.default", "written-by"].contains(&k);
    let q = |v: &str| format!("'{}'", v.replace('\'', "''"));
    let mut set = vec![];
    for u in updates {
        match u["action"].as_str().unwrap_or_default() {
            "set-properties" => set.extend(u["updates"].as_object().into_iter().flatten().filter(|(k, _)| !own(k)).map(|(k, v)| format!("{} = {}", q(k), q(v.as_str().unwrap_or_default())))),
            "remove-properties" => set.extend(u["removals"].as_array().into_iter().flatten().filter_map(Value::as_str).filter(|k| !own(k)).map(|k| format!("{} = ''", q(k)))),
            _ => {}
        }
    }
    (!set.is_empty()).then(|| format!("ALTER TABLE {name} SET TBLPROPERTIES ({})", set.join(", ")))
}

/// A commit that changes the schema (`add-schema` and `set-current-schema`, no snapshot: PyIceberg's
/// `update_schema`, Spark's `ALTER TABLE`) as the `ALTER TABLE` statements it comes to (ADR-029
/// §8), by field id against the current schema: a field gone is dropped, one renamed renamed, one
/// of a wider type widened, a new one (optional, at the end) added. What Pondra's tables can't
/// take (a new required column, a key changed, columns reordered, a type narrowed) is refused by
/// name before anything is changed.
fn schema_sql(name: &str, meta: &Value, asked: &Value) -> Result<Vec<String>> {
    use anyhow::ensure;
    let updates = asked["updates"].as_array().context("a commit without its updates")?;
    let mut new = None;
    for u in updates {
        match u["action"].as_str().unwrap_or_default() {
            "add-schema" if new.is_none() => new = Some(&u["schema"]),
            "set-current-schema" => {}
            "set-properties" | "remove-properties" => {} // (`properties_sql`)
            a => anyhow::bail!("{a} with a schema change: a commit changes the schema alone"),
        }
    }
    let new = new.context("a schema change without its schema")?;
    let id = meta["current-schema-id"].clone();
    let old = meta["schemas"].as_array().into_iter().flatten().find(|s| s["schema-id"] == id).context("the table's current schema")?;
    let by_id = |s: &Value| s["fields"].as_array().into_iter().flatten().map(|f| (f["id"].as_i64().unwrap_or(-1), f.clone())).collect::<Vec<_>>();
    let (was, now) = (by_id(old), by_id(new));
    let keys = |s: &Value| s["identifier-field-ids"].as_array().cloned().unwrap_or_default();
    ensure!(keys(old) == keys(new), "a key changed: a Pondra table keeps its key");
    let kept: Vec<i64> = now.iter().map(|(i, _)| *i).filter(|i| was.iter().any(|(w, _)| w == i)).collect();
    let before: Vec<i64> = was.iter().map(|(i, _)| *i).filter(|i| kept.contains(i)).collect();
    let last_old = now.iter().rposition(|(i, _)| kept.contains(i)).map_or(0, |p| p + 1);
    ensure!(kept == before && now[last_old..].iter().all(|(i, _)| !kept.contains(i)), "columns reordered: a Pondra table's columns keep their order, and a new one goes last");
    let q = |n: &Value| format!("\"{}\"", n.as_str().unwrap_or_default().replace('"', "\"\""));
    let mut out = vec![];
    for (i, f) in &was {
        if !now.iter().any(|(n, _)| n == i) {
            out.push(format!("ALTER TABLE {name} DROP COLUMN {}", q(&f["name"])));
        }
    }
    for (i, f) in &now {
        match was.iter().find(|(w, _)| w == i).map(|(_, w)| w) {
            Some(w) => {
                ensure!(w["required"] == f["required"], "{}: required or optional, a column stays as it was", f["name"]);
                if w["name"] != f["name"] {
                    out.push(format!("ALTER TABLE {name} RENAME COLUMN {} TO {}", q(&w["name"]), q(&f["name"])));
                }
                if w["type"] != f["type"] {
                    out.push(format!("ALTER TABLE {name} ALTER COLUMN {} TYPE {}", q(&f["name"]), sql_type(&f["type"])?));
                }
            }
            None => {
                ensure!(!f["required"].as_bool().unwrap_or(false), "{}: a column added can't be required (NOT NULL): the rows already there have no value for it", f["name"]);
                out.push(format!("ALTER TABLE {name} ADD COLUMN {} {}", q(&f["name"]), sql_type(&f["type"])?));
            }
        }
    }
    Ok(out)
}

/// `DELETE /v1/namespaces/{ns}/tables/{table}`: `DROP TABLE`, its files with it (the catalog's
/// `purgeRequested` or not: a Pondra table's files are its own).
async fn drop_table(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, axum::extract::Path((ns, table)): axum::extract::Path<(String, String)>) -> Result<axum::http::StatusCode, Refusal> {
    let (parts, lake, schema) = space(&app, &ns).await?;
    loaded(&lake, &crate::text::join(&schema, &table)).await.map_err(|_| missing(&format!("table {}.{table}", parts.join(".")), "NoSuchTableException"))?;
    as_caller(&app, role, &format!("DROP TABLE {}", sql_name(&app, &parts, &lake, &schema, &table))).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// `POST /v1/tables/rename`: `ALTER TABLE … RENAME TO`, within its lake and schema.
async fn rename(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, body: bytes::Bytes) -> Result<axum::http::StatusCode, Refusal> {
    let asked: Value = serde_json::from_slice(&body).map_err(|e| bad(format!("the rename: {e}")))?;
    let ns = |v: &Value| v["namespace"].as_array().into_iter().flatten().filter_map(Value::as_str).collect::<Vec<_>>().join("\u{1f}");
    let (from, to) = (&asked["source"], &asked["destination"]);
    let (parts, lake, schema) = space(&app, &ns(from)).await?;
    let (name, new) = (from["name"].as_str().unwrap_or_default(), to["name"].as_str().unwrap_or_default());
    if ns(from) != ns(to) {
        return Err(bad(format!("{name} to another namespace: a Pondra table is renamed within its schema")));
    }
    loaded(&lake, &crate::text::join(&schema, name)).await.map_err(|_| missing(&format!("table {}.{name}", parts.join(".")), "NoSuchTableException"))?;
    let sql = format!("ALTER TABLE {} RENAME TO \"{}\"", sql_name(&app, &parts, &lake, &schema, name), new.replace('"', "\"\""));
    as_caller(&app, role, &sql).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// A table: its current metadata file, and what it says.
async fn load(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::extract::Path((ns, table)): axum::extract::Path<(String, String)>) -> Reply {
    // (no such table, when its namespace isn't one either: an engine then reads `t.files` as the
    // metadata table `files` of `t`, as Spark does)
    let no_table = || missing(&format!("table {}.{table}", ns.replace('\u{1f}', ".")), "NoSuchTableException");
    let (_, lake, schema) = space(&app, &ns).await.map_err(|_| no_table())?;
    crate::auth::check_all(&crate::text::join(&schema, &table)).map_err(|e| refused(403, "ForbiddenException", crate::ext::said(&e)))?; // (a user's: an engine reads the whole files)
    Ok(axum::Json(loaded(&lake, &crate::text::join(&schema, &table)).await.map_err(|_| no_table())?))
}

/// `POST /v1/namespaces/{ns}/tables/{table}`: another engine's commit. Its files become the
/// table's where the writer put them (ADR-029), their footers read here and checked; a table whose
/// files the writer can't lay out as its own (renamed columns, a partition spec it doesn't
/// publish: `adopt::fits_as_written`) has them rewritten here, as a bulk INSERT's (round 25's
/// copy). A copy-on-write change (`DELETE`, `UPDATE`, `MERGE`, an overwrite) takes files out as
/// it adds others, on a table not renamed, and only when the writer read the table as Pondra has
/// it (`adopt::stale`: else 409, once its rows are in its files). The leader checks what the
/// commit asserts and commits it through the log, the views and tasks that follow the table taking
/// it as they take the log's rows, and the table's next version is published under the writer's
/// snapshot id. Anything else is refused by name.
async fn update(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, axum::extract::Path((ns, table)): axum::extract::Path<(String, String)>, body: bytes::Bytes) -> Reply {
    let (parts, lake, schema) = space(&app, &ns).await?;
    let name = crate::text::join(&schema, &table);
    crate::auth::check("insert", &name).map_err(|e| refused(403, "ForbiddenException", crate::ext::said(&e)))?;
    let current = loaded(&lake, &name).await.map_err(|_| missing(&format!("table {}.{table}", ns.replace('\u{1f}', ".")), "NoSuchTableException"))?;
    let asked: Value = serde_json::from_slice(&body).map_err(|e| bad(format!("the commit: {e}")))?;
    let updates = asked["updates"].as_array().cloned().unwrap_or_default();
    let alters = |a: &str| ["add-schema", "set-properties", "remove-properties"].contains(&a);
    if updates.iter().any(|u| alters(u["action"].as_str().unwrap_or_default())) && !updates.iter().any(|u| u["action"] == "add-snapshot") {
        requirements(&current["metadata"], &asked).map_err(door)?;
        let target = sql_name(&app, &parts, &lake, &schema, &table);
        let mut sql = match updates.iter().any(|u| u["action"] == "add-schema") {
            true => schema_sql(&target, &current["metadata"], &asked).map_err(|e| bad(crate::ext::said(&e)))?,
            false => vec![],
        };
        sql.extend(properties_sql(&target, &updates));
        for s in &sql {
            as_caller(&app, role, s).await?;
        }
        let version = current["metadata"]["last-sequence-number"].as_u64().unwrap_or(0);
        for _ in 0..200 {
            match loaded(&lake, &name).await {
                Ok(v) if sql.is_empty() || v["metadata"]["last-sequence-number"].as_u64().unwrap_or(0) > version => return Ok(axum::Json(v)),
                _ => tokio::time::sleep(std::time::Duration::from_millis(50)).await, // (published on the leader: this node sees it a moment later)
            }
        }
        return loaded(&lake, &name).await.map(axum::Json).map_err(|e| bad(crate::ext::said(&e)));
    }
    match prepare(&lake, &name, &current, &asked).await.map_err(door)? {
        Some(c) => commit(&app, &lake, vec![c]).await.map(axum::Json),
        None => Ok(axum::Json(current)), // (this commit is in already: a retry, answered as it was)
    }
}

/// `POST /v1/transactions/commit`: another engine's changes to several tables of a database, as
/// one commit (ADR-029 §7): all of them, or none. Each is taken as a table's own commit is
/// (`update`); a schema change goes on its own.
async fn transaction(axum::extract::State(app): axum::extract::State<crate::server::App>, body: bytes::Bytes) -> Result<axum::http::StatusCode, Refusal> {
    let asked: Value = serde_json::from_slice(&body).map_err(|e| bad(format!("the transaction: {e}")))?;
    let (mut commits, mut at) = (vec![], None::<std::sync::Arc<Lake>>);
    for change in asked["table-changes"].as_array().into_iter().flatten() {
        let id = &change["identifier"];
        let ns: Vec<&str> = id["namespace"].as_array().into_iter().flatten().filter_map(|p| p.as_str()).collect();
        let (_, lake, schema) = space(&app, &ns.join("\u{1f}")).await?;
        if at.as_ref().is_some_and(|l| !std::sync::Arc::ptr_eq(l, &lake)) {
            return Err(bad("a transaction over tables of two databases: each database commits its own".into()));
        }
        let table = id["name"].as_str().unwrap_or_default();
        let name = crate::text::join(&schema, table);
        crate::auth::check("insert", &name).map_err(|e| refused(403, "ForbiddenException", crate::ext::said(&e)))?;
        let current = loaded(&lake, &name).await.map_err(|_| missing(&format!("table {}.{table}", ns.join(".")), "NoSuchTableException"))?;
        if let Some(c) = prepare(&lake, &name, &current, change).await.map_err(door)? {
            commits.push(c);
        }
        at = Some(lake);
    }
    if let (Some(lake), false) = (at, commits.is_empty()) {
        commit(&app, &lake, commits).await?;
    }
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// Commits the leader records as one (`record`), sent to it; the rows written for them deleted if
/// they are refused. The last table, as the catalog answers it.
async fn commit(app: &crate::server::App, lake: &std::sync::Arc<Lake>, commits: Vec<Commit>) -> Result<Value, Refusal> {
    let files: Vec<String> = commits.iter().flat_map(|c| c.written.iter().flat_map(|f| f.paths())).collect();
    let out = match std::sync::Arc::ptr_eq(lake, &app.lake) {
        true => app.record_iceberg(commits).await,
        false => crate::write::send(&lake.url, crate::write::Request::Iceberg(commits)).await,
    };
    out.map_err(|e| {
        let (raw, e) = (format!("{e:#}"), crate::ext::said(&e)); // (the words are matched raw, sent as said)
        match (raw.contains(CONFLICT), raw.contains(REFUSED)) {
            (true, _) => {
                let lake = lake.clone();
                crate::panics::task(async move { futures::future::join_all(files.iter().map(|f| lake.delete(f))).await }); // (never recorded)
                conflict(e)
            }
            (_, true) => bad(e),
            _ => refused(500, "CommitStateUnknownException", e),
        }
    })
}
