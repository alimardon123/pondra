//! Outside the lake (ADR-026): files anywhere — S3 (R2, MinIO), Google Cloud Storage, Azure,
//! HTTPS, and this machine's for the program that started the node — read as tables wherever SQL
//! takes one, with DuckDB's names (`FROM 's3://b/x/*.parquet'`, `read_csv('gs://…', header =>
//! true)`), and spread over the nodes as a table's files are; and the secrets they are read with
//! (`CREATE SECRET`), kept encrypted in the catalog.
//!
//! Where SQL comes in (`routines::expand`), each reference becomes a table named for what it
//! reads (`"ext:<its spec>"`), so every node and every later step sees the same name; `meta`
//! makes that name a table of files — listed, their schema inferred, Parquet footers giving rows
//! and each column's range — which the query engine, the pruning and the spreading treat as any
//! append table's files (with no log).
use crate::store::{json, DataFile, Lake, TableMeta};
use anyhow::{anyhow, bail, ensure, Context, Result};
use aws_lc_rs::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use datafusion::datasource::file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::sql::sqlparser::ast::Value;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

pub use crate::ext_names::{attached_table, is, names, spec, Attached, Spec};
pub(crate) use crate::ext_names::{format_of, quoted, scheme};

tokio::task_local! {
    /// The caller is the program that started this node (the shell, `connect()` of a lake: `server::owner`),
    /// or another node running its share of a query its coordinator checked: this machine's files,
    /// and URLs no secret covers, read with the node's own credentials.
    static OWNER: bool;
    /// The files each table of files in this statement reads, listed once for it (planning asks
    /// several times; each node of a spread query has its coordinator's list). The next statement
    /// lists again: a file changed under its name is read as it is now, never by an old size.
    static LISTED: std::cell::RefCell<HashMap<String, TableMeta>>;
}

/// Run a statement (or a node's share of one) as its caller: `owner` or not, listing each table
/// of files once.
pub async fn scope<F: std::future::Future>(owner: bool, f: F) -> F::Output {
    OWNER.scope(owner, listing(f)).await
}

/// Run `f` listing each table of files once (within a statement already doing so: as it is).
/// (not `async`: its future would hold `f` twice, once per arm, and a query's is over 400 KB)
pub fn listing<F: std::future::Future>(f: F) -> impl std::future::Future<Output = F::Output> {
    use futures::future::Either;
    match LISTED.try_with(|_| ()) {
        Ok(()) => Either::Left(f),
        Err(_) => Either::Right(LISTED.scope(Default::default(), f)),
    }
}

pub(crate) fn owner() -> bool { OWNER.try_with(|o| *o).unwrap_or(false) }

// ---------------------------------------------------------------- what a query reads

/// DataFusion's (and Hive's) `CREATE EXTERNAL TABLE t [(columns)] STORED AS PARQUET | CSV | JSON
/// LOCATION '…' [PARTITIONED BY (…)] [OPTIONS (…)]`: a stored view of the files by that name,
/// `SELECT * FROM read_csv('…', …)` (ADR-032). The columns it declares are the files' — CSV's by
/// position, Parquet's and JSON's by name — each read as the type declared; without them, as the
/// files say. A path on this machine is made whole here, as ATTACH makes it, so each node reads
/// the same files; a folder is its files of that format.
pub fn external(c: &datafusion::sql::parser::CreateExternalTable) -> Result<crate::ddl::Ddl> {
    use datafusion::sql::sqlparser::ast::Value as V;
    let name = crate::write::object(&c.name);
    ensure!(!c.temporary, "CREATE TEMPORARY EXTERNAL TABLE: make a temporary view of the files (CREATE TEMP VIEW {name} AS SELECT * FROM read_parquet('…'))");
    let (format, function) = match c.file_type.to_lowercase().as_str() {
        "parquet" => ("parquet", "read_parquet"),
        "csv" => ("csv", "read_csv"),
        "json" | "ndjson" => ("json", "read_json"),
        "arrow" => ("arrow", "read_arrow"),
        f => bail!("files stored as {f}: Pondra reads Parquet, CSV, JSON (a value a line) and Arrow"),
    };
    let text = |s: &str| format!("'{}'", s.replace('\'', "''"));
    let mut args: Vec<String> = vec![];
    for (k, v) in &c.options {
        let v = match v {
            V::SingleQuotedString(s) | V::DoubleQuotedString(s) => s.clone(),
            V::Number(n, _) => n.to_string(),
            V::Boolean(b) => b.to_string(),
            v => v.to_string(),
        };
        let lower = k.to_lowercase();
        let key = lower.strip_prefix("format.").unwrap_or(&lower);
        let yes = v.eq_ignore_ascii_case("true");
        match (format, key.split("::").next().unwrap_or(key)) {
            ("csv", "has_header") => args.push(format!("header => {yes}")),
            ("csv", "delimiter") => args.push(format!("delim => {}", text(&v))),
            ("csv", "quote" | "escape" | "comment") => args.push(format!("{key} => {}", text(&v))),
            ("csv", "terminator") => args.push(format!("new_line => {}", text(&v))),
            ("csv", "double_quote") => {} // (how quotes inside quotes are written: read either way)
            ("json", "newline_delimited") if yes => {} // (as Pondra reads them)
            ("csv" | "json", "compression" | "file_compression_type") if !["", "uncompressed"].contains(&v.to_lowercase().as_str()) => {
                bail!("{v} files aren't read yet: Parquet, CSV, JSON and Arrow as they are")
            }
            (_, k) if WRITING.contains(&k) || (format == "parquet" && (PARQUET_WRITING.contains(&k) || k.starts_with("content_defined_chunking") || PARQUET_TUNING.contains(&k))) => {} // (how files are written, or read faster: not what is read)
            _ => bail!("CREATE EXTERNAL TABLE … OPTIONS ('{k}' …): Pondra doesn't read files with it{}", if format == "csv" { " (CSV takes format.has_header, format.delimiter, format.quote, format.escape, format.comment and format.terminator)" } else { "" }),
        }
    }
    ensure!(!(c.or_replace && c.if_not_exists), "CREATE OR REPLACE EXTERNAL TABLE … IF NOT EXISTS: one or the other");
    let urls = c.locations.iter().map(|l| located(l)).collect::<Result<Vec<_>>>()?;
    ensure!(!urls.is_empty(), "CREATE EXTERNAL TABLE {name}: where are its files? (LOCATION '…')");
    let parts: Vec<String> = c.table_partition_cols.iter().map(|p| p.trim_matches('"').to_string()).collect();
    let own = |col: &&datafusion::sql::sqlparser::ast::ColumnDef| !parts.contains(&crate::write::ident(&col.name));
    let typed = |cols: Vec<&datafusion::sql::sqlparser::ast::ColumnDef>| cols.iter().map(|col| format!("{}: {}", text(&crate::write::ident(&col.name)), text(&crate::write::sql_type(&col.data_type)))).collect::<Vec<_>>().join(", ");
    let (in_files, keys): (Vec<_>, Vec<_>) = c.columns.iter().partition(own);
    if !in_files.is_empty() {
        args.push(format!("columns => {{{}}}", typed(in_files)));
    }
    match keys.is_empty() {
        false => args.push(format!("hive_types => {{{}}}", typed(keys))), // (its folders' keys, `day=…/`, as declared)
        true if !parts.is_empty() => args.push("hive_partitioning => true".into()),
        true => {}
    }
    let select = match c.columns.is_empty() {
        true => "*".to_string(),
        false => c.columns.iter().map(|col| quoted(&crate::write::ident(&col.name))).collect::<Vec<_>>().join(", "),
    };
    let files = match &urls[..] {
        [u] => text(u),
        us => format!("[{}]", us.iter().map(|u| text(u)).collect::<Vec<_>>().join(", ")),
    };
    let args = std::iter::once(files).chain(args).collect::<Vec<_>>().join(", ");
    Ok(crate::ddl::Ddl::CreateExternal { name, sql: format!("SELECT {select} FROM {function}({args})"), replace: c.or_replace, if_not_exists: c.if_not_exists })
}

/// A write to a stored view: INSERT into CREATE EXTERNAL TABLE's view of a folder is a new file in
/// the folder (`COPY … TO '…/' (APPEND)`, in its format, its folders' keys as folders), as
/// DataFusion's external tables take it: the query, where, and COPY's options. Any other write to
/// a view is refused (it would make a table by that name). None: not a view.
pub async fn view_write(lake: &Lake, stmt: &crate::write::Stmt) -> Result<Option<(String, String, BTreeMap<String, String>)>> {
    use crate::write::Stmt;
    let (Stmt::Insert(t, _) | Stmt::InsertInto(t, ..) | Stmt::Update(t, ..) | Stmt::Delete(t, _) | Stmt::AddColumn(t, ..)) = stmt else { return Ok(None) };
    let Ok((None, name)) = crate::ddl::resolve(lake, t).await else { return Ok(None) };
    let Some(view) = lake.cat.get::<crate::ddl::StoredView>(&crate::ddl::query_key(&name)).await? else { return Ok(None) };
    ensure!(view.external, "{name} is a view: write to the tables it reads");
    let (query, names) = match stmt {
        Stmt::Insert(_, q) => (q, vec![]),
        Stmt::InsertInto(_, n, q) => (q, n.clone()),
        _ => bail!("{name} is a view of files (CREATE EXTERNAL TABLE): UPDATE, DELETE and ALTER change a lake's tables; INSERT adds a file to a view of a folder"),
    };
    let expanded = crate::routines::expand_stored(lake, &view.sql).await?;
    let spec = match &self::names(&expanded)[..] {
        [one] => spec(one).context("its files")?,
        _ => bail!("{name}: a view of several reads"),
    };
    let folder = match &spec.urls[..] {
        [u] if u.ends_with('/') => u.clone(),
        _ => bail!("INSERT INTO {name}: it reads {} — a view of a folder takes INSERTs, each a new file in it (LOCATION '…/')", spec.urls.join(", ")),
    };
    // Its columns, in order and by type: the rows go as the view reads them (the files' order is
    // CSV's, their names Parquet's and JSON's).
    let ctx = crate::query::session(lake, &format!("SELECT * FROM {}", crate::write::sql_name(&name)), "").await?;
    let fields = ctx.sql(&format!("SELECT * FROM {} LIMIT 0", crate::write::sql_name(&name))).await?.schema().fields().clone();
    let given: Vec<String> = if names.is_empty() { fields.iter().map(|f| f.name().clone()).collect() } else { names };
    if let Some(n) = given.iter().find(|n| !fields.iter().any(|f| f.name() == *n)) {
        bail!("{name} has no column {n}");
    }
    let select = fields.iter().map(|f| {
        let n = quoted(f.name());
        let t = f.data_type().to_string().replace('\'', "''");
        match given.contains(f.name()) {
            true => format!("arrow_cast(s.{n}, '{t}') AS {n}"),
            false => format!("arrow_cast(NULL, '{t}') AS {n}"),
        }
    }).collect::<Vec<_>>().join(", ");
    let aliases = given.iter().map(|n| quoted(n)).collect::<Vec<_>>().join(", ");
    let rows = format!("SELECT {select} FROM ({query}) AS s({aliases})");
    let mut options = BTreeMap::from([("format".to_string(), spec.format.clone()), ("append".to_string(), "true".to_string())]);
    if spec.format == "csv" {
        options.insert("header".into(), spec.options.get("header").cloned().unwrap_or_else(|| "true".into()));
        if let Some(d) = spec.options.get("delim").or(spec.options.get("sep")).or(spec.options.get("delimiter")) {
            options.insert("delimiter".into(), d.clone());
        }
    }
    match spec.options.get("hive_types") {
        Some(h) => _ = options.insert("partition_by".into(), crate::write::declared(h).await?.iter().map(|f| f.name().clone()).collect::<Vec<_>>().join(",")), // (its folders' keys: a folder a value)
        None => ensure!(spec.options.get("hive_partitioning").is_none_or(|v| v == "false"), "INSERT INTO {name}: its folders' keys aren't declared (CREATE EXTERNAL TABLE {name} (…, day DATE) … PARTITIONED BY (day))"),
    }
    Ok(Some((rows, folder, options)))
}

/// How files are written, not read: options CREATE EXTERNAL TABLE takes for DataFusion's INSERT
/// and COPY, which don't change what a query reads.
const WRITING: [&str; 12] = ["quote_style", "null_value", "date_format", "datetime_format", "timestamp_format", "timestamp_tz_format", "time_format",
    "ignore_leading_whitespace", "ignore_trailing_whitespace", "compression", "compression_level", "write_batch_size"];
const PARQUET_WRITING: [&str; 19] = ["max_row_group_size", "data_page_row_count_limit", "data_pagesize_limit", "dictionary_enabled", "dictionary_page_size_limit",
    "statistics_enabled", "max_statistics_size", "created_by", "column_index_truncate_length", "statistics_truncate_length", "bloom_filter_on_write",
    "bloom_filter_fpp", "bloom_filter_ndv", "writer_version", "encoding", "allow_single_file_parallelism", "maximum_parallel_row_group_writers",
    "maximum_buffered_record_batches_per_stream", "skip_arrow_metadata"];
/// How DataFusion reads Parquet faster: Pondra decides these itself.
const PARQUET_TUNING: [&str; 8] = ["pushdown_filters", "reorder_filters", "enable_page_index", "pruning", "skip_metadata", "metadata_size_hint", "bloom_filter_on_read", "schema_force_view_types"];

/// A LOCATION as a read takes it: a URL as written; a path on this machine made whole, and a
/// folder there ending in `/` (its files).
fn located(l: &str) -> Result<String> {
    if scheme(l).is_some() {
        return Ok(l.to_string());
    }
    let whole = crate::ddl::full(l)?;
    Ok(match l.ends_with('/') || std::path::Path::new(&whole).is_dir() {
        true => format!("{whole}/"),
        false => whole,
    })
}

// ---------------------------------------------------------------- the table they make

/// Text (an error, a plan) with each table of files named as SQL named it: `'s3://…'`, or
/// `read_csv('…', delim => ';')`, not its internal name.
/// An error in words for whoever sent the statement: its causes after it, each said once
/// (DataFusion repeats a cause's words in its own), files named as SQL named them.
pub fn said(e: &anyhow::Error) -> String {
    let mut out = String::new();
    for c in e.chain().map(|c| c.to_string()) {
        // Unwrap DataFusion's and Arrow's wrappers, repeatedly, so the inner message — the part a
        // user can act on — leads, not an engine-internal label.
        let mut c = c;
        while let Some(inner) = ["External error: ", "Error during planning: ", "Arrow error: ", "Execution error: "]
            .iter()
            .find_map(|p| c.strip_prefix(p))
        {
            c = inner.to_string();
        }
        if !out.contains(c.trim()) {
            out += if out.is_empty() { "" } else { ": " };
            out += &c;
        }
    }
    match crate::auth::shows_code() {
        true => readable(&out),
        false => crate::python::unquoted(&readable(&out)), // (a Python routine's lines, quoted wherever its batch ran: S27)
    }
}

pub fn readable(text: &str) -> String {
    static NAMES: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#""?(ext:[A-Za-z0-9_-]+)"?"#).expect("a regex"));
    NAMES.replace_all(text, |c: &regex::Captures| {
        let Some(s) = spec(&c[1]) else { return c[0].to_string() };
        let urls = s.urls.iter().map(|u| format!("'{u}'")).collect::<Vec<_>>();
        let urls = if urls.len() == 1 { urls[0].clone() } else { format!("[{}]", urls.join(", ")) };
        match s.options.is_empty() {
            true if s.urls.len() == 1 => urls,
            _ => format!("read_{}({urls}{})", s.format, s.options.iter().map(|(k, v)| format!(", {k} => '{v}'")).collect::<String>()),
        }
    }).into_owned()
}

/// An EXPLAIN's rows, its tables of files named as SQL named them (`readable`).
pub fn readable_rows(batches: Vec<datafusion::arrow::record_batch::RecordBatch>) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
    use datafusion::arrow::array::{ArrayRef, AsArray, StringArray};
    batches.into_iter().map(|b| {
        let columns = b.columns().iter().map(|c| match c.data_type() {
            datafusion::arrow::datatypes::DataType::Utf8 => Arc::new(c.as_string::<i32>().iter().map(|v| v.map(readable)).collect::<StringArray>()) as ArrayRef,
            _ => c.clone(),
        });
        Ok(datafusion::arrow::record_batch::RecordBatch::try_new(b.schema(), columns.collect())?)
    }).collect()
}

/// Does this table read files on the node's machine? (Such a query stays on it.)
pub fn local(table: &str) -> bool { spec(table).is_some_and(|s| s.format == "share" || s.urls.iter().any(|u| scheme(u).is_none_or(|s| s == "file"))) } // (a share's links: as its node got them)

/// A table by name: this lake's, or the files an `ext:` name reads (checked: see `check`).
pub async fn meta(lake: &Lake, table: &str) -> Result<Option<TableMeta>> {
    if !is(table) {
        return lake.cat.get::<TableMeta>(&crate::store::table_key(table)).await;
    }
    let spec = spec(table).context("not a table of files")?;
    check(lake, &spec).await?;
    if let Ok(Some(m)) = LISTED.try_with(|l| l.borrow().get(table).cloned()) {
        return Ok(Some(m));
    }
    let m = resolve(lake, &spec).await?;
    keep(table, &m);
    Ok(Some(m))
}

fn keep(table: &str, m: &TableMeta) { _ = LISTED.try_with(|l| l.borrow_mut().insert(table.to_string(), m.clone())); }

/// The coordinator's list of the tables of files among `tables`, for its slices.
pub async fn of(lake: &Lake, tables: &[String]) -> Result<Vec<(String, TableMeta)>> {
    let (mut out, files) = (vec![], tables.iter().filter(|t| is(t)).cloned().collect::<Vec<_>>());
    for t in files {
        let m = meta(lake, &t).await?.expect("files");
        out.push((t, m));
    }
    Ok(out)
}

/// A node's share of a spread query: its coordinator's list, and the stores to read them through.
pub async fn prime(lake: &Lake, tables: &[(String, TableMeta)]) -> Result<()> {
    for (t, m) in tables {
        for u in m.ext.as_ref().map(|s| s.urls.clone()).unwrap_or_default() {
            OWNER.scope(true, register(lake, &u)).await?;
        }
        if let Some(s) = m.ext.as_ref().filter(|s| s.format == "iceberg" && s.options.contains_key("namespace")) {
            OWNER.scope(true, crate::catalogs::prime(lake, s, m)).await?;
        }
        keep(t, m);
    }
    Ok(())
}

/// May this caller read these files? A path on this machine: only the program that started the
/// node. A URL: with the secret whose scope covers it (an admin made it: the grant), or, if none
/// does, only that program again, with the node's own credentials.
pub async fn check(lake: &Lake, spec: &Spec) -> Result<()> {
    if owner() || spec.urls.iter().all(|u| own_file(lake, u)) {
        return Ok(()); // (the lake's own files: whoever reads the lake reads them, as GET /files/… does)
    }
    if let (Some(name), [url]) = (spec.options.get("catalog"), &spec.urls[..]) {
        if crate::catalogs::signed(lake, name, url).await? {
            return Ok(()); // (an AWS catalog: whoever may use its attachment's secret)
        }
    }
    let secrets = list(lake).await?;
    spec.urls.iter().try_for_each(|u| allowed(&secrets, u))
}

/// Is `u` in the lake's own files (`PUT /files/…`, `files()`)? Written out whole, with no `..`.
fn own_file(lake: &Lake, u: &str) -> bool {
    let area = format!("{}/files/", lake.url.trim_end_matches('/'));
    u.starts_with(&area) && !u[area.len()..].split('/').any(|p| p == ".." || p == ".")
}

/// May the caller read `url`, a file a catalog named (a REST table's metadata), as `check` asks of a
/// spec's own URLs? A catalog that vouches for its files' credentials is asked nothing here.
pub async fn check_url(lake: &Lake, url: &str) -> Result<()> {
    if owner() || own_file(lake, url) {
        return Ok(());
    }
    allowed(&list(lake).await?, url)
}

fn allowed(secrets: &[(String, Secret)], u: &str) -> Result<()> {
    match scheme(u).filter(|s| *s != "file") {
        None => bail!("{u} is a path on the server's own disk, which a client reads only through a node it started itself (the `pondra` shell, `pondra sql`, `connect()` of a lake): read it there, or from a bucket (s3://, gs://, az://, https://)"),
        Some(_) => match covering(secrets, u) {
            None => bail!("{}", uncovered(u)),
            Some((_, s)) if s.temporary && !s.trusted => crate::public::check(&url::Url::parse(u)?)?, // (`register` holds its store to the public internet)
            Some(_) => {}
        },
    }
    Ok(())
}

/// Another engine's table names its files wherever it likes (Iceberg's by their whole URL):
/// each must be one this caller may read, as the table is. A folder under `vouched` (a catalog's
/// table's location, whose credentials its catalog vouched for) needs no secret.
async fn check_files(lake: &Lake, m: &TableMeta, vouched: Option<&str>) -> Result<()> {
    if owner() {
        return Ok(());
    }
    let secrets = list(lake).await?;
    let mut folders: Vec<&str> = m.files.iter().flat_map(|f| {
        let deletes = f.outside.iter().flat_map(|o| o.deletes.iter()).filter_map(|d| match d {
            crate::scan::Delete::Vector { path, .. } | crate::scan::Delete::Blob { path, .. } | crate::scan::Delete::Positions { path, .. } | crate::scan::Delete::Equality { path, .. } => Some(path.as_str()),
            crate::scan::Delete::Inline { .. } => None,
        });
        std::iter::once(f.path.as_str()).chain(deletes)
    }).map(|p| &p[..p.rfind('/').unwrap_or(p.len())])
    .filter(|d| !vouched.is_some_and(|v| format!("{}/", d.replacen("s3a://", "s3://", 1)).starts_with(v))) // (`v` ends in `/`: a folder beside the location isn't in it)
    .collect();
    folders.sort_unstable();
    folders.dedup();
    folders.into_iter().try_for_each(|d| allowed(&secrets, &format!("{d}/")))
}

/// What to do about a URL no secret covers: the statement that would, for its store.
pub(crate) fn uncovered(u: &str) -> String {
    let (kind, keys) = match scheme(u) {
        Some("gs" | "gcs") => ("gcs", "SERVICE_ACCOUNT_KEY '…'"),
        Some("az" | "azure" | "abfs" | "abfss") => ("azure", "CONNECTION_STRING '…'"),
        Some("http" | "https") => ("http", "BEARER_TOKEN '…'"),
        Some("kafka") => ("kafka", "SECURITY_PROTOCOL 'SASL_SSL', USERNAME '…', PASSWORD '…'"),
        _ => ("s3", "KEY_ID '…', SECRET '…'"),
    };
    format!("no secret covers {u}: an admin makes one (CREATE SECRET name (TYPE {kind}, {keys}, SCOPE '{}'))", root(u).unwrap_or_default())
}

/// The files, their schema, and (Parquet) each file's rows and column ranges from its footer.
async fn resolve(lake: &Lake, spec: &Spec) -> Result<TableMeta> {
    let version = |k: &str| spec.options.get(k).map(|v| v.parse::<i64>().with_context(|| format!("{k} is a number, not {v}"))).transpose();
    match spec.format.as_str() {
        "kafka" => return Ok(TableMeta { ext: Some(spec.clone()), ..crate::kafka_client::resolve(lake, &spec.urls[0]).await? }),
        "share" => return Ok(TableMeta { ext: Some(spec.clone()), ..crate::sharing::read(lake, spec).await? }), // (its files' links are the provider's: ADR-046)
        "delta" | "iceberg" => {
            let (mut m, d) = match spec.format.as_str() {
                "delta" => (crate::read_delta::resolve(lake, &spec.urls[0], version("version")?).await?, None),
                _ => crate::read_iceberg::resolve(lake, &spec.urls[0], &spec.options).await?,
            };
            check_files(lake, &m, d.as_ref().map(|d| d.prefix.as_str())).await?;
            if let Some(d) = &d {
                crate::catalogs::rehome(d, &mut m); // (its files are read through the table's own delegated store)
            }
            return Ok(TableMeta { ext: Some(spec.clone()), ..m });
        }
        _ => {}
    }
    let ctx = lake.session();
    let format = file_format(spec)?;
    // Columns declared (`columns => {'id': 'BIGINT', …}`, CREATE EXTERNAL TABLE's): the table's.
    let declared = match spec.options.get("columns") {
        Some(c) => Some(crate::write::declared(c).await?),
        None => None,
    };
    // The folders' keys' types (`hive_types => {'day': 'DATE'}`, DuckDB's): typed as declared, and
    // columns even before a folder has files.
    let hive_types = match spec.options.get("hive_types") {
        Some(c) => crate::write::declared(c).await?,
        None => vec![],
    };
    let (mut objects, mut urls) = (vec![], vec![]);
    let deep = spec.urls.iter().any(|u| u.contains("**") || glob_at(u).is_some_and(|i| u[i..].contains('/')));
    ctx.state_ref().write().config_mut().options_mut().execution.listing_table_ignore_subdirectory = !deep; // (`**`, `*/x.parquet`: into folders)
    let state = ctx.state();
    for u in &spec.urls {
        let url = listing_url(u)?;
        register(lake, u).await?;
        let store = lake.rt.object_store(&url)?;
        let wanted = if u.ends_with('/') { format!(".{}", spec.format) } else { String::new() }; // (a folder: its files of that format)
        let found: Vec<_> = url.list_all_files(&state, store.as_ref(), &wanted).await?.try_collect().await?;
        ensure!(!found.is_empty() || declared.is_some(), "no files at {u}"); // (declared: none yet is an empty table)
        objects.extend(found.into_iter().map(|o| (store.clone(), url.clone(), o)));
        urls.push(url);
    }
    objects.sort_by(|a, b| a.2.location.cmp(&b.2.location)); // (every node lists them in one order)
    let named = |fields: &[datafusion::arrow::datatypes::FieldRef]| -> Vec<(String, String)> { fields.iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect() };
    if objects.is_empty() {
        let columns = [named(declared.as_deref().unwrap_or_default()), named(&hive_types)].concat();
        return Ok(TableMeta { columns, ext: Some(spec.clone()), ..Default::default() });
    }
    let (store, first) = (objects[0].0.clone(), objects.iter().take(if spec.format == "parquet" { usize::MAX } else { 16 }).map(|o| o.2.clone()).collect::<Vec<_>>());
    // CSV's declared columns are its columns by position, JSON's by name: the readers take them as
    // they are. Parquet's files say their own (for their statistics), read by name as declared.
    let schema = match (&declared, spec.format.as_str()) {
        (Some(d), "csv" | "json") => Arc::new(datafusion::arrow::datatypes::Schema::new(d.clone())),
        _ => inferred(format.as_ref(), &state, &store, &first).await.with_context(|| format!("reading {}", spec.urls.join(", ")))?,
    };
    let mut columns: Vec<(String, String)> = named(declared.as_deref().unwrap_or(&schema.fields()[..]));
    // (a column read as another type than the file's: its ranges are the file's type's, so none is kept)
    let as_stored: Vec<String> = match &declared {
        Some(d) => d.iter().filter(|f| schema.field_with_name(f.name()).is_ok_and(|g| g.data_type() == f.data_type())).map(|f| f.name().clone()).collect(),
        None => schema.fields().iter().map(|f| f.name().clone()).collect(),
    };
    let missing = declared.is_some() && spec.format == "parquet" && columns.iter().any(|(c, _)| schema.field_with_name(c).is_err());
    // Hive-style folders (`day=2026-09-28/`): each a column, the same in every file's path, typed
    // by what all its values are (as Spark, DuckDB and Polars do).
    let hive = |url: &ListingTableUrl, o: &object_store_df::ObjectMeta| -> Vec<(String, String)> {
        let rest = o.location.as_ref().strip_prefix(url.prefix().as_ref()).unwrap_or_default();
        rest.split('/').filter_map(|s| s.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect()
    };
    // Found unless turned off (DuckDB's and Spark's default); when asked for, every file has them.
    let asked = spec.options.get("hive_partitioning").map(|v| v != "false" && v != "0").or((!hive_types.is_empty()).then_some(true));
    let found: Vec<Vec<String>> = if asked == Some(false) { vec![] } else { objects.iter().map(|(_, u, o)| hive(u, o).into_iter().map(|(k, _)| k).collect()).collect() };
    let same = found.windows(2).all(|w| w[0] == w[1]);
    ensure!(same || asked != Some(true), "{}: files in other folders than {} (hive_partitioning)", spec.urls.join(", "), found[0].join("/"));
    let keys = if same { found.first().cloned().unwrap_or_default() } else { vec![] };
    let hived = !keys.is_empty();
    for (i, k) in keys.iter().enumerate() {
        ensure!(!columns.iter().any(|(c, _)| c == k), "{k} is a folder's key and a column in the files");
        let values: Vec<String> = objects.iter().filter_map(|(_, url, o)| hive_value(hive(url, o).get(i)?.1.as_str())).collect();
        let typed = hive_types.iter().find(|f| f.name() == k).map(|f| crate::query::type_name(f.data_type()));
        columns.push((k.clone(), typed.unwrap_or_else(|| hive_type(&values).into())));
    }
    let parquet = spec.format == "parquet";
    let stats: Vec<_> = objects.into_iter().map(|(store, url, o)| {
        let (format, schema, state, as_stored) = (format.clone(), schema.clone(), state.clone(), as_stored.clone());
        let part = if hived { hive(&url, &o) } else { vec![] };
        async move {
            let root = url.object_store();
            let full = format!("{}{}", root.as_str().strip_suffix('/').unwrap_or(root.as_str()), url_path(&o)); // (`file:///x`, `s3://b/x`)
            let s = if parquet { Some(format.infer_stats(&state, &store, schema.clone(), &o).await?) } else { None };
            let mut f = file(full, o.size, s.as_ref(), &schema);
            f.stats.retain(|c, _| as_stored.contains(c));
            if missing {
                f.nulls = None; // (a column declared that the files don't hold is all NULL)
            }
            f.part = part.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("/");
            if parquet && hived {
                let values = part.iter().map(|(k, v)| (k.clone(), hive_value(v))).collect();
                f.outside = Some(Box::new(crate::scan::Outside { values, ..Default::default() })); // (its folders' values, as `scan::read` takes them)
            }
            for (k, v) in part {
                match hive_value(&v) {
                    Some(v) => _ = f.stats.insert(k, (v.clone(), v)),
                    None => f.nulls.iter_mut().for_each(|n| n.push(k.clone())), // (a NULL's folder: `IS NULL` finds it)
                }
            }
            anyhow::Ok(f)
        }
    }).collect();
    let files = futures::stream::iter(stats).buffered(32).try_collect().await?;
    Ok(TableMeta { columns, files, ext: Some(spec.clone()), ..Default::default() })
}

/// The files a spec's path names, as `resolve` lists them (a glob, or a folder's files of the
/// format): each one's full URL and its listing (its e-tag, size and time), sorted by name. Checked
/// as a table's files are: a path of this machine only for the program that started the node.
pub(crate) async fn objects(lake: &Lake, spec: &Spec) -> Result<Vec<(String, object_store_df::ObjectMeta)>> {
    check(lake, spec).await?;
    let u = &spec.urls[0];
    let url = listing_url(u)?;
    register(lake, u).await?;
    let store = lake.rt.object_store(&url)?;
    let ctx = lake.session();
    let deep = u.contains("**") || glob_at(u).is_some_and(|i| u[i..].contains('/'));
    ctx.state_ref().write().config_mut().options_mut().execution.listing_table_ignore_subdirectory = !deep;
    let state = ctx.state();
    let wanted = if u.ends_with('/') { format!(".{}", spec.format) } else { String::new() };
    let found: Vec<_> = url.list_all_files(&state, store.as_ref(), &wanted).await?.try_collect().await?;
    let root = url.object_store();
    let root = root.as_str().strip_suffix('/').unwrap_or(root.as_str()).to_string();
    let mut out: Vec<_> = found.into_iter().map(|o| (format!("{root}{}", url_path(&o)), o)).collect();
    out.sort_by(|a, b| a.1.location.cmp(&b.1.location));
    Ok(out)
}

/// A NULL's folder, as Hive, Spark, pyarrow and Polars name it.
pub const NULL_FOLDER: &str = "__HIVE_DEFAULT_PARTITION__";

/// A folder's value as written (percent-escaped where a path can't hold a character), or None
/// for a NULL's folder.
fn hive_value(v: &str) -> Option<String> {
    if v == NULL_FOLDER {
        return None;
    }
    let (b, mut out, mut i) = (v.as_bytes(), Vec::with_capacity(v.len()), 0);
    while i < b.len() {
        match (b[i], v.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok())) {
            (b'%', Some(c)) => (out.push(c), i += 3),
            (c, _) => (out.push(c), i += 1),
        };
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

/// A folder key's type: whole numbers, numbers, dates, or text.
fn hive_type(values: &[String]) -> &'static str {
    let all = |ok: fn(&str) -> bool| !values.is_empty() && values.iter().all(|v| ok(v));
    if all(|v| v.parse::<i64>().is_ok()) {
        "Int64"
    } else if all(|v| v.parse::<f64>().is_ok_and(f64::is_finite)) {
        "Float64"
    } else if all(|v| v.len() == 10 && chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d").is_ok()) {
        "Date32"
    } else {
        "Utf8"
    }
}

/// Where a URL's glob starts (`*`, `[`; `?` too, but not in a web address: its query).
fn glob_at(u: &str) -> Option<usize> {
    let web = u.starts_with("http");
    u.char_indices().find(|(_, c)| *c == '*' || *c == '[' || (*c == '?' && !web)).map(|(i, _)| i)
}

/// A URL as DataFusion lists it, its glob split off (DataFusion splits only paths' globs).
fn listing_url(u: &str) -> Result<ListingTableUrl> {
    let Some(at) = glob_at(u).filter(|_| scheme(u).is_some_and(|s| s != "file")) else { return Ok(ListingTableUrl::parse(u)?) };
    let cut = u[..at].rfind('/').map_or(0, |j| j + 1);
    let (prefix, glob) = u.split_at(cut);
    Ok(ListingTableUrl::try_new(url::Url::parse(prefix)?, Some(glob::Pattern::new(glob)?))?)
}

fn url_path(o: &object_store_df::ObjectMeta) -> String { format!("/{}", o.location) }

/// A file as a table's `DataFile`: its rows and exact column ranges when its footer says.
fn file(path: String, bytes: u64, s: Option<&datafusion::common::Statistics>, schema: &datafusion::arrow::datatypes::Schema) -> DataFile {
    use datafusion::common::stats::Precision::Exact;
    let Some(s) = s else { return DataFile { path, bytes, rows: bytes / 100, ..Default::default() } }; // (CSV, JSON: rows unknown; about 100 bytes each)
    let (mut stats, mut nulls) = (BTreeMap::new(), Some(vec![]));
    for (f, c) in schema.fields().iter().zip(&s.column_statistics) {
        if let (Exact(lo), Exact(hi), false) = (&c.min_value, &c.max_value, f.data_type().is_floating()) {
            if let (Some(lo), Some(hi)) = (crate::manifest::text(lo), crate::manifest::text(hi)) {
                stats.insert(f.name().clone(), (lo, hi));
            }
        }
        match (&c.null_count, nulls.as_mut()) {
            (Exact(0), _) => {}
            (Exact(_), Some(n)) => n.push(f.name().clone()),
            _ => nulls = None,
        }
    }
    let rows = match s.num_rows { Exact(n) => n as u64, _ => bytes / 100 };
    DataFile { path, bytes, rows, stats, nulls, ..Default::default() }
}

/// The files' columns; files whose notes differ (pandas' metadata, each file's own) merged without them.
async fn inferred(format: &dyn FileFormat, state: &dyn datafusion::catalog::Session, store: &Arc<dyn object_store_df::ObjectStore>, files: &[object_store_df::ObjectMeta]) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    match format.infer_schema(state, store, files).await {
        Err(e) if e.to_string().contains("conflicting metadata") => {
            let mut each = vec![];
            for f in files {
                each.push(format.infer_schema(state, store, std::slice::from_ref(f)).await?.as_ref().clone().with_metadata(Default::default()));
            }
            Ok(Arc::new(datafusion::arrow::datatypes::Schema::try_merge(each)?))
        }
        r => Ok(r?),
    }
}

fn file_format(spec: &Spec) -> Result<Arc<dyn FileFormat>> {
    let o = |k: &str| spec.options.get(k).map(String::as_str);
    let byte = |k: &str, v: &str| -> Result<u8> { v.bytes().next().filter(|_| v.len() == 1).with_context(|| format!("{k} is one character, not {v:?}")) };
    Ok(match spec.format.as_str() {
        "parquet" => Arc::new(ParquetFormat::default()),
        "json" => Arc::new(JsonFormat::default()),
        "arrow" => Arc::new(datafusion::datasource::file_format::arrow::ArrowFormat),
        "csv" => {
            let mut f = CsvFormat::default().with_has_header(o("header").is_none_or(|h| h != "false"));
            if let Some(d) = o("delim").or(o("sep")).or(o("delimiter")) {
                f = f.with_delimiter(byte("delim", d)?);
            }
            if let Some(q) = o("quote") {
                f = f.with_quote(byte("quote", q)?);
            }
            if let Some(e) = o("escape") {
                f = f.with_escape(Some(byte("escape", e)?));
            }
            if let Some(c) = o("comment") {
                f = f.with_comment(Some(byte("comment", c)?)); // (lines starting with it are skipped)
            }
            match o("new_line") {
                None | Some("\n" | "\r\n" | "\\n" | "\\r\\n") => {} // (either, as read)
                Some("\r" | "\\r") => f = f.with_terminator(Some(b'\r')),
                Some(t) => bail!("new_line is \\n, \\r\\n or \\r, not {t:?}"),
            }
            Arc::new(f)
        }
        f => bail!("files as {f}: parquet, csv, json or arrow"),
    })
}

/// The rows of `files` of an `ext:` table, as `schema`: DataFusion's own readers, without the hot
/// columns (outside the lake, a file may change under the same name).
pub async fn read(lake: &Lake, ctx: &datafusion::prelude::SessionContext, files: &[&DataFile], schema: &datafusion::arrow::datatypes::SchemaRef, spec: &Spec, table: Option<&crate::scan::Table>) -> Result<datafusion::prelude::DataFrame> {
    if ["delta", "iceberg", "share"].contains(&spec.format.as_str()) {
        return crate::scan::read(ctx, files, schema, table).await; // (another engine's table, or a share: its partition values, its deletes)
    }
    if spec.format == "kafka" {
        return crate::kafka_client::read(lake, ctx, files, schema).await; // (a topic)
    }
    if spec.format == "parquet" {
        return crate::scan::read(ctx, files, schema, None).await; // (as listed: no second look at each file before it is read)
    }
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::prelude::{cast, ident, lit};
    let folders = |f: &DataFile| f.part.split('/').filter_map(|s| s.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect::<Vec<_>>();
    let keys: Vec<String> = files.first().map(|f| folders(f).into_iter().map(|(k, _)| k).collect()).unwrap_or_default();
    let fields: Vec<_> = schema.fields().iter().filter(|f| !keys.contains(f.name())).cloned().collect();
    let stored = crate::query::schema(&fields.iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect::<Vec<_>>())?; // (as the files hold it)
    let in_files = if spec.format == "parquet" { Arc::new(datafusion::arrow::datatypes::Schema::new(fields)) } else { stored };
    let mut by_folder: BTreeMap<&str, Vec<&DataFile>> = BTreeMap::new();
    files.iter().for_each(|f| by_folder.entry(f.part.as_str()).or_default().push(f));
    let mut out: Option<datafusion::prelude::DataFrame> = None;
    for group in by_folder.into_values() {
        let urls = group.iter().map(|f| ListingTableUrl::parse(&f.path)).collect::<datafusion::error::Result<Vec<_>>>()?;
        let options = ListingOptions::new(file_format(spec)?).with_file_extension("");
        let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options);
        let config = match spec.format.as_str() {
            "arrow" => {
                // (as each file holds its columns: Arrow's reader doesn't cast them, the select below
                // does; the files' notes left out, as the table's columns have none)
                let mut c = config.infer_schema(&ctx.state()).await?;
                c.file_schema = c.file_schema.map(|s| Arc::new(s.as_ref().clone().with_metadata(Default::default())));
                c
            }
            _ => config.with_schema(in_files.clone()),
        };
        let table = ListingTable::try_new(config)?;
        let mut df = ctx.read_table(Arc::new(table))?;
        for (k, v) in folders(group[0]) {
            df = df.with_column(&k, lit(datafusion::common::ScalarValue::Utf8(hive_value(&v))))?; // (a folder's value, for each of its rows)
        }
        let typed = schema.fields().iter().map(|f| cast(ident(f.name()), f.data_type().clone()).alias(f.name())).collect::<Vec<_>>();
        let df = df.select(typed)?;
        out = Some(match out {
            Some(o) => o.union(df)?,
            None => df,
        });
    }
    out.context("no files")
}

// ---------------------------------------------------------------- where they are

/// A URL's store root: scheme and bucket (or container, or host).
fn root(url: &str) -> Option<String> {
    let u = url::Url::parse(url).ok()?;
    Some(format!("{}://{}", u.scheme(), &u[url::Position::BeforeUsername..url::Position::AfterPort]))
}

/// DataFusion reads `url` through a store for its bucket, made with the secret covering it (or,
/// for the node's owner, the node's own credentials). The lake's own bucket keeps its store.
pub(crate) async fn register(lake: &Lake, url: &str) -> Result<()> {
    let Some(root) = root(url).filter(|_| scheme(url).is_some_and(|s| s != "file" && s != "kafka")) else { return Ok(()) }; // (this machine's: DataFusion's own; a topic: a Kafka client's)
    let lake_root = self::root(&lake.url);
    let attached: Vec<Option<String>> = lake.attached.read().unwrap().iter().map(|(_, o)| self::root(&o.url)).collect();
    if lake_root.as_deref() == Some(root.as_str()) || attached.iter().any(|a| a.as_deref() == Some(root.as_str())) {
        return Ok(()); // (a lake's bucket: its store, the SSD tier's)
    }
    let secrets = list(lake).await?;
    let secret = covering(&secrets, url);
    let made_with = secret.as_ref().map_or("the node's own".to_string(), |(n, s)| format!("{n}:{}", s.sealed));
    crate::per_node!(static MADE: Mutex<Option<HashMap<String, String>>> = Mutex::new(None));
    if MADE.lock().unwrap().get_or_insert_default().get(&root) == Some(&made_with) {
        return Ok(());
    }
    let params = secret.as_ref().map(|(n, s)| open(n, s)).transpose()?;
    let to = if secret.as_ref().is_some_and(|(_, s)| s.temporary && !s.trusted) { For::Public } else { For::Files };
    let store = build(&url::Url::parse(&root)?, params.as_ref(), to)?;
    lake.rt.register_object_store(&url::Url::parse(&root)?, Arc::new(crate::cache::CachedStore::files(store))); // (a file's ranges kept by its version)
    MADE.lock().unwrap().get_or_insert_default().insert(root, made_with);
    Ok(())
}

/// The store `url` is read through (made as `register` makes it), and its path there.
pub async fn store(lake: &Lake, url: &str) -> Result<(Arc<dyn object_store_df::ObjectStore>, object_store_df::path::Path)> {
    if let Some(found) = crate::catalogs::scoped(url) { return Ok(found); } // (a catalog's table's location, while it is read or written: its delegated store)
    register(lake, url).await?;
    let u = ListingTableUrl::parse(url)?;
    Ok((lake.rt.object_store(&u)?, u.prefix().clone()))
}

/// An object's bytes (a log's commit, a manifest), through its store.
pub async fn get(lake: &Lake, url: &str) -> Result<bytes::Bytes> {
    let (store, path) = store(lake, url).await?;
    Ok(object_store_df::ObjectStoreExt::get(&store, &path).await.with_context(|| format!("reading {url}"))?.bytes().await?)
}

/// Whom a store `build` makes is for.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum For {
    /// A lake's own bucket, read as `store::open_store` reads it: idle connections dropped, and every
    /// request a turn of the bucket's budget (invariant 182).
    Lake,
    /// Files outside the lakes, with an admin's secret, a catalog's, or the node's own credentials.
    Files,
    /// Files read with a temporary secret its maker brought: the public internet only (`public.rs`).
    Public,
}

/// A store for a bucket, container or host, with a secret's settings or the environment's.
pub(crate) fn build(url: &url::Url, p: Option<&BTreeMap<String, String>>, to: For) -> Result<Arc<dyn object_store::ObjectStore>> {
    use object_store::{aws::{AmazonS3Builder, AmazonS3ConfigKey}, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder, http::HttpBuilder};
    let lake = to == For::Lake;
    let public = to == For::Public;
    let get = |k: &str| p.and_then(|p| p.get(k)).cloned();
    // (an endpoint or host written as an address is refused here; a name, as it resolves)
    let reach = |u: &str| if public { crate::public::check(&url::Url::parse(u).with_context(|| format!("endpoint {u}"))?) } else { Ok(()) };
    reach(url.as_str())?;
    let chain = p.is_none() || get("provider").as_deref() == Some("credential_chain");
    let bucket = url.host_str().context("no bucket in the URL")?.to_string();
    let home = format!("{}://{bucket}", url.scheme());
    let s3 = |endpoint: Option<String>| -> Result<Arc<dyn object_store::ObjectStore>> {
        let mut b = if chain { AmazonS3Builder::from_env() } else { AmazonS3Builder::new().with_region(get("region").unwrap_or_else(|| "us-east-1".into())) };
        b = b.with_bucket_name(&bucket);
        if lake {
            b = b.with_config(AmazonS3ConfigKey::Client(object_store::ClientConfigKey::PoolIdleTimeout), "15s").with_http_connector(crate::budget::Budget::of(&home));
        }
        if let Some(e) = endpoint.or_else(|| get("endpoint")) {
            let e = if e.contains("://") { e } else if get("use_ssl").as_deref() == Some("false") { format!("http://{e}") } else { format!("https://{e}") };
            reach(&e)?;
            b = b.with_allow_http(e.starts_with("http://")).with_endpoint(e);
        }
        if public {
            b = b.with_http_connector(crate::public::Connector::default());
        }
        if let Some(r) = get("region") {
            b = b.with_region(r);
        }
        match (get("key_id"), get("secret")) {
            (Some(k), Some(s)) => b = b.with_access_key_id(k).with_secret_access_key(s),
            _ if !chain => b = b.with_skip_signature(true), // (a secret without keys: a public bucket)
            _ => {}
        }
        if let Some(t) = get("session_token") {
            b = b.with_token(t);
        }
        Ok(Arc::new(b.with_virtual_hosted_style_request(get("url_style").as_deref() == Some("vhost")).build()?))
    };
    Ok(match url.scheme() {
        "s3" => s3(None)?,
        "r2" => s3(get("account_id").map(|a| format!("https://{a}.r2.cloudflarestorage.com")))?,
        "gs" | "gcs" if get("key_id").is_some() => s3(Some("https://storage.googleapis.com".into()))?, // (HMAC keys: GCS's S3 interface)
        "gs" | "gcs" => {
            use object_store::gcp::GoogleConfigKey as K;
            let mut b = if chain { GoogleCloudStorageBuilder::from_env() } else { GoogleCloudStorageBuilder::new() }.with_bucket_name(&bucket);
            match get("service_account_key") {
                Some(k) => b = b.with_service_account_key(k),
                None if !chain => b = b.with_skip_signature(true), // (a secret without a key: a public bucket)
                None => {}
            }
            if let Some(e) = get("endpoint") {
                reach(&e)?;
                b = b.with_config(K::Client(object_store::ClientConfigKey::AllowHttp), e.starts_with("http://").to_string()).with_base_url(&e);
            }
            if public {
                b = b.with_http_connector(crate::public::Connector::default());
            }
            if lake {
                b = b.with_config(K::Client(object_store::ClientConfigKey::PoolIdleTimeout), "15s").with_http_connector(crate::budget::Budget::of(&home));
            }
            Arc::new(b.build()?)
        }
        "az" | "azure" | "abfs" | "abfss" => {
            use object_store::azure::AzureConfigKey as K;
            let mut b = if chain { MicrosoftAzureBuilder::from_env() } else { MicrosoftAzureBuilder::new() }.with_url(url.as_str());
            let conn: BTreeMap<String, String> = get("connection_string").unwrap_or_default().split(';').filter_map(|kv| kv.split_once('=')).map(|(k, v)| (k.to_lowercase(), v.to_string())).collect();
            let endpoint = get("endpoint").or(conn.get("blobendpoint").cloned());
            endpoint.as_deref().map(reach).transpose()?;
            let http = endpoint.as_ref().map(|e| e.starts_with("http://").to_string());
            for (key, found) in [(K::AccountName, get("account_name").or(conn.get("accountname").cloned())), (K::AccessKey, get("account_key").or(conn.get("accountkey").cloned())),
                                 (K::SasKey, get("sas_token")), (K::AuthorityId, get("tenant_id")), (K::ClientId, get("client_id")), (K::ClientSecret, get("client_secret")),
                                 (K::Endpoint, endpoint), (K::Client(object_store::ClientConfigKey::AllowHttp), http), (K::UseEmulator, get("use_emulator"))] {
                if let Some(v) = found {
                    b = b.with_config(key, v);
                }
            }
            if lake {
                b = b.with_config(K::Client(object_store::ClientConfigKey::PoolIdleTimeout), "15s").with_http_connector(crate::budget::Budget::of(&home));
            }
            if public {
                b = b.with_http_connector(crate::public::Connector::default());
            }
            Arc::new(b.build()?)
        }
        "http" | "https" => {
            let mut options = object_store::ClientOptions::new().with_allow_http(url.scheme() == "http");
            let mut headers = object_store::HeaderMap::new();
            if let Some(t) = get("bearer_token") {
                headers.insert("authorization", format!("Bearer {t}").parse()?);
                options = options.with_default_headers(headers.clone());
            }
            let b = HttpBuilder::new().with_url(url.as_str()).with_client_options(options);
            Arc::new(if public { b.with_http_connector(crate::public::Connector { headers }) } else { b }.build()?)
        }
        s => bail!("{s}:// isn't read (s3, r2, gs, az, abfss, https)"),
    })
}

// ---------------------------------------------------------------- other engines' catalogs

fn attached_key(n: &str) -> String { format!("o/{n}") }

pub async fn attached(lake: &Lake) -> Result<Vec<(String, Attached)>> {
    Ok(lake.cat.scan::<Attached>("o/", "o0").await?.into_iter().map(|(k, a)| (k[2..].to_string(), a)).collect())
}

/// Is this attached as another engine's tables?
pub async fn is_attached(lake: &Lake, name: &str) -> Result<bool> { Ok(lake.cat.get::<Attached>(&attached_key(name)).await?.is_some()) }

/// The table of files a statement's target is, if it names an attached catalog's table.
pub async fn outside_target(lake: &Lake, name: &str) -> Result<Option<String>> {
    let all = attached(lake).await?;
    if all.is_empty() {
        return Ok(None);
    }
    attached_table(&all, &name.split('.').map(str::to_string).collect::<Vec<_>>())
}

/// Leader: keep an attachment of another engine's tables.
pub async fn attach(lake: &Lake, name: &str, url: &str, kind: &str, options: BTreeMap<String, String>) -> Result<serde_json::Value> {
    crate::ddl::check(name)?;
    ensure!(["delta", "iceberg", "kafka", "share"].contains(&kind), "ATTACH … (TYPE {kind}): delta, iceberg, kafka or share (or a Pondra lake, with no TYPE)");
    ensure!((kind == "kafka") == url.starts_with("kafka://"), "ATTACH … (TYPE kafka) takes a kafka://brokers URL, and only it does");
    let mut options = options;
    if kind == "share" {
        options = crate::sharing::accept(lake, name, url, options).await?; // (its token kept as a secret)
    }
    let iceberg_only = ["endpoint_type", "access_delegation_mode"];
    ensure!(kind == "iceberg" || !options.keys().any(|k| iceberg_only.contains(&k.as_str())), "ENDPOINT_TYPE and ACCESS_DELEGATION_MODE are an Iceberg REST catalog's");
    let known: &[&str] = match kind {
        "share" => &["share"],
        "iceberg" => &["endpoint", "secret", "read_only", "endpoint_type", "access_delegation_mode"],
        _ => &["endpoint", "secret", "read_only"],
    };
    if let Some(k) = options.keys().find(|k| !known.contains(&k.as_str())) {
        bail!("ATTACH … (TYPE {kind}) has no option {k}: {}", known.join(", "));
    }
    ensure!(kind == "iceberg" || !options.contains_key("endpoint"), "ENDPOINT is an Iceberg REST catalog's");
    if let Some(m) = options.get("access_delegation_mode") {
        ensure!(m == "vended_credentials" || m == "none", "ACCESS_DELEGATION_MODE is vended_credentials or none, not {m}");
    }
    // An AWS catalog (Glue, S3 Tables) is signed with a TYPE s3 secret's keys, and its endpoint is AWS's
    // for its region unless one is given (which also checks the type).
    if let Some(t) = options.get("endpoint_type").cloned() {
        crate::catalogs::service(&t)?; // (one it doesn't know: refused by name, before its secret is asked for)
        let secret = options.get("secret").cloned().with_context(|| format!("ATTACH … (TYPE iceberg, ENDPOINT_TYPE {t}) needs SECRET name: a TYPE s3 secret with the AWS keys"))?;
        let keys = crate::catalogs::aws_keys(lake, &secret).await?;
        let at = crate::catalogs::aws_endpoint(&t, &crate::catalogs::region(url, &keys)?)?;
        options.entry("endpoint".to_string()).or_insert(at);
    }
    ensure!(!lake.attached.read().unwrap().iter().any(|(n, _)| n == name) && name != crate::ddl::lake_name(lake), "{name} is a lake's name here: attach under another name");
    // A REST catalog is reached with the secret whose scope covers it (never one named anywhere else).
    // An AWS catalog's secret covers its buckets, not its endpoint, so it isn't checked against it.
    if let Some(secret) = options.get("secret").filter(|_| !options.contains_key("endpoint_type")) {
        let at = options.get("endpoint").unwrap_or(&url.to_string()).clone();
        let found = list(lake).await?.into_iter().find(|(n, _)| n == secret).with_context(|| format!("no secret {secret}"))?;
        ensure!(covering(&[found.clone()], &at).is_some(), "secret {secret}'s SCOPE doesn't cover {at} (CREATE OR REPLACE SECRET {secret} (…, SCOPE '{at}'))");
    }
    let a = Attached { kind: kind.into(), url: url.trim_end_matches('/').into(), options };
    if let Some(had) = lake.cat.get::<Attached>(&attached_key(name)).await? {
        ensure!(had.url == a.url && had.kind == a.kind, "{name} is attached already, to {}", had.url);
        return Ok(serde_json::json!({"attached": name, "unchanged": true}));
    }
    lake.cat.commit(vec![(attached_key(name), json(&a))], &[]).await?;
    Ok(serde_json::json!({"attached": name, "type": kind, "url": a.url}))
}

/// Leader: DETACH one (false: not attached this way).
pub async fn detach(lake: &Lake, name: &str) -> Result<bool> {
    let Some(a) = lake.cat.get::<Attached>(&attached_key(name)).await? else { return Ok(false) };
    let mut gone = vec![attached_key(name)];
    if a.kind == "share" {
        gone.push(secret_key(&format!("share_{name}"))); // (its token, kept when it was attached)
    }
    lake.cat.commit(vec![], &gone).await?;
    Ok(true)
}

/// The settings of the secret covering `url`, for a client of the service it names (Kafka).
pub async fn secret_for(lake: &Lake, url: &str) -> Result<Option<BTreeMap<String, String>>> {
    covering(&list(lake).await?, url).map(|(n, s)| open(&n, &s)).transpose()
}

/// A client for other engines' services (REST catalogs): Pondra's own tokens never go there.
pub fn web() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| reqwest::Client::builder().timeout(std::time::Duration::from_secs(60)).build().expect("an HTTP client"))
}

/// A recipient's token for a Delta Sharing server, from the secret of TYPE share covering it.
pub async fn share_token(lake: &Lake, endpoint: &str) -> Result<Option<String>> {
    let all: Vec<(String, Secret)> = list(lake).await?.into_iter().filter(|(_, s)| s.kind == "share").collect();
    let Some((name, secret)) = covering(&all, endpoint) else { return Ok(None) };
    Ok(open(&name, &secret)?.get("token").cloned())
}

/// The token a Pondra server's leader is asked with (ADR-058), from the TYPE pondra secret covering it.
fn pondra_token(secrets: &[(String, Secret)], endpoint: &str) -> Result<Option<String>> {
    let found = secrets.iter().filter(|(n, x)| x.kind == "pondra" && usable(n, x))
        .filter(|(_, x)| x.scope.as_deref().is_none_or(|p| within(endpoint, p)))
        .max_by_key(|(_, x)| x.scope.as_ref().map_or(0, |p| p.len() + 1));
    let Some((name, secret)) = found else { return Ok(None) };
    Ok(open(name, secret)?.get("token").cloned())
}

/// Say how this process reaches the lake at `dir` on another server (`store::Reach`): with the
/// bucket secret among `secrets` covering it, and its leader at `endpoint` with the pondra secret
/// covering that.
pub fn reach_with(secrets: &[(String, Secret)], dir: &str, read_only: bool, endpoint: Option<&str>) -> Result<()> {
    reach_by(covering(secrets, dir), secrets, dir, read_only, endpoint)
}

/// A branch's base on another server (`branch::load`, `make`), read with the key lent to the branch
/// for it (`lent`): the one use of a lent key, and only for the base's own folder.
pub fn reach_base(secrets: &[(String, Secret)], dir: &str, endpoint: Option<&str>) -> Result<()> {
    reach_by(scoped(secrets.iter().filter(|(_, s)| s.lent), dir), secrets, dir, true, endpoint)
}

fn reach_by(hit: Option<(String, Secret)>, secrets: &[(String, Secret)], dir: &str, read_only: bool, endpoint: Option<&str>) -> Result<()> {
    let params = hit.as_ref().map(|(n, s)| open(n, s)).transpose()?;
    let token = endpoint.map(|e| pondra_token(secrets, e)).transpose()?.flatten();
    let secret = hit.map(|(n, _)| n);
    crate::store::reach_by(dir, crate::store::Reach { read_only, params, endpoint: endpoint.map(str::to_string), token, secret });
    Ok(())
}

/// The same, with this lake's secrets.
pub async fn reach(lake: &Lake, dir: &str, read_only: bool, endpoint: Option<&str>) -> Result<()> {
    reach_with(&list(lake).await?, dir, read_only, endpoint)
}

/// A place this lake copies itself to or from (a backup's, a restore's: ADR-070), reached with the
/// bucket secret whose SCOPE covers it when one does, else with the environment's credentials. (Only
/// on a hit: a place registered with none would hide a key a broader place was registered with.)
pub async fn reach_place(lake: &Lake, dir: &str) -> Result<()> {
    let secrets = list(lake).await?;
    if let Some(hit) = covering(&secrets, dir) {
        reach_by(Some(hit), &secrets, dir, false, None)?;
    }
    Ok(())
}

/// The bucket secrets covering `urls` (no temporary ones), for a branch to keep (`branch::make`). Whoever
/// may clone a database attached here lends its key without USAGE on it: the branch can't read its base
/// without it, and CLONE is the grant that says who may (ADR-058). Lent, it reads the base and nothing
/// else (`reach_base`): no statement in the branch uses it, whoever its owner grants it to (`usable`).
pub async fn lent(lake: &Lake, urls: &[String]) -> Result<Vec<(String, Secret)>> {
    let all: Vec<(String, Secret)> = list(lake).await?.into_iter().filter(|(_, s)| !s.temporary).collect();
    let mut found: Vec<(String, Secret)> = vec![];
    for (name, mut secret) in urls.iter().filter_map(|u| scoped(all.iter(), u)) {
        if !found.iter().any(|(n, _)| *n == name) {
            secret.lent = true;
            found.push((name, secret));
        }
    }
    Ok(found)
}

/// The bearer token for a REST catalog, from the secret covering it: its TOKEN, or one its
/// CLIENT_ID and CLIENT_SECRET get (OAuth's client credentials), kept until it expires.
pub async fn rest_token(lake: &Lake, url: &str) -> Result<Option<String>> {
    crate::per_node!(static TOKENS: Mutex<Option<HashMap<String, (String, std::time::Instant)>>> = Mutex::new(None));
    let Some((name, secret)) = covering(&list(lake).await?, url).filter(|(_, s)| s.kind == "iceberg") else { return Ok(None) };
    let p = open(&name, &secret)?;
    if let Some(t) = p.get("token") {
        return Ok(Some(t.clone()));
    }
    let (Some(id), Some(pass)) = (p.get("client_id"), p.get("client_secret")) else { return Ok(None) };
    let key = format!("{name}:{}", secret.sealed);
    if let Some((t, until)) = TOKENS.lock().unwrap().get_or_insert_default().get(&key) {
        if std::time::Instant::now() < *until {
            return Ok(Some(t.clone()));
        }
    }
    let at = p.get("oauth2_server_uri").cloned().unwrap_or_else(|| format!("{url}/v1/oauth/tokens"));
    let form = [("grant_type", "client_credentials"), ("client_id", id), ("client_secret", pass), ("scope", p.get("oauth2_scope").map_or("catalog", String::as_str))];
    let body = url::form_urlencoded::Serializer::new(String::new()).extend_pairs(form).finish();
    let r = web().post(&at).header("content-type", "application/x-www-form-urlencoded").body(body).send().await.with_context(|| format!("asking {at} for a token"))?;
    let status = r.status();
    let body: serde_json::Value = r.json().await.unwrap_or_default();
    ensure!(status.is_success(), "{at} gave no token ({status}): {}", body["error_description"].as_str().or(body["error"].as_str()).unwrap_or(""));
    let t = body["access_token"].as_str().context("a token answer without access_token")?.to_string();
    let life = body["expires_in"].as_u64().unwrap_or(3600).saturating_sub(60);
    TOKENS.lock().unwrap().get_or_insert_default().insert(key, (t.clone(), std::time::Instant::now() + std::time::Duration::from_secs(life)));
    Ok(Some(t))
}

// ---------------------------------------------------------------- secrets

/// A secret as the catalog keeps it: its type and scope in the clear, its values sealed with a key
/// of its own (`key`), which the lake's master key wraps (envelope encryption, ADR-035 §3): the
/// master key is never in the lake, and changing it rewraps the keys, never the values. (A secret
/// sealed before round 29 has no key of its own: the master key sealed it.)
#[derive(Serialize, Deserialize, Clone)]
pub struct Secret {
    pub kind: String,
    pub scope: Option<String>,
    pub sealed: String, // base64 (nonce, AES-256-GCM ciphertext) of its values, as JSON
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>, // its data key, wrapped by the master key (`wrap`)
    #[serde(skip)]
    pub temporary: bool, // (CREATE TEMPORARY SECRET: the session's, in memory: `temp.rs`)
    #[serde(skip)]
    pub trusted: bool, // (a temporary one an admin or the node's owner made: it reaches what they may; anyone else's, the public internet: `public.rs`)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lent: bool, // (a key a branch was lent to read its base with: `reach_base` alone, never a statement: ADR-058)
}

pub(crate) fn secret_key(name: &str) -> String { format!("e/{name}") }

/// Each type's settings (DuckDB's names), and the URL schemes it serves.
fn kind(t: &str) -> Result<(&'static [&'static str], &'static [&'static str])> {
    Ok(match t {
        "s3" | "r2" => (&["key_id", "secret", "region", "session_token", "endpoint", "url_style", "use_ssl", "account_id", "provider", "scope"], &["s3", "r2"]),
        "gcs" => (&["key_id", "secret", "service_account_key", "endpoint", "provider", "scope"], &["gs", "gcs"]),
        "azure" => (&["connection_string", "account_name", "account_key", "sas_token", "tenant_id", "client_id", "client_secret", "endpoint", "use_emulator", "provider", "scope"], &["az", "azure", "abfs", "abfss"]),
        "http" => (&["bearer_token", "scope"], &["http", "https"]),
        "iceberg" => (&["token", "client_id", "client_secret", "oauth2_server_uri", "oauth2_scope", "scope"], &["http", "https"]), // (a REST catalog)
        "share" => (&["token", "scope"], &["http", "https"]), // (a Delta Sharing server: a recipient's token)
        "pondra" => (&["token", "scope"], &["http", "https"]), // (a Pondra server's leader, asked with a user's token: ADR-058)
        "git" => (&["token", "provider", "scope"], &["http", "https"]), // (a person's git host token: `git.rs`; never read as a file's secret)
        "kafka" => (&["security_protocol", "sasl_mechanism", "username", "password", "scope"], &["kafka"]),
        "generic" => (&[], &[]), // (any settings: for procedures, `pondra.secret(name)`)
        t => bail!("secrets of TYPE {t}: s3, r2, gcs, azure, http, iceberg, share, kafka, pondra, git or generic"),
    })
}

/// `CREATE [OR REPLACE] [PERSISTENT] SECRET [IF NOT EXISTS] name (TYPE t, KEY value, …)` and
/// `DROP SECRET [IF EXISTS] name`, parsed here: a value is a string, a word or a number.
pub fn statement(sql: &str) -> Option<crate::write::Stmt> {
    use crate::ddl::Ddl;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::Token};
    if let Some(load) = copy_into_statement(&mut Parser::new(&GenericDialect {}).try_with_sql(sql).ok()?) {
        return Some(load);
    }
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(sql).ok()?;
    if let Some(copy) = copy_statement(&mut p) {
        return Some(copy);
    }
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(sql).ok()?;
    let invalid = |e: String| Some(crate::write::Stmt::Invalid(e));
    if p.parse_keyword(Keyword::ATTACH) {
        return attach_statement(&mut p);
    }
    if p.parse_keyword(Keyword::DROP) {
        let _ = p.parse_one_of_keywords(&[Keyword::PERSISTENT, Keyword::TEMPORARY]);
        if !p.parse_keyword(Keyword::SECRET) {
            return None;
        }
        let if_exists = p.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        return match p.parse_identifier() {
            Ok(n) => Some(crate::write::Stmt::Ddl(vec![Ddl::DropSecret { name: n.value.to_lowercase(), if_exists }])),
            Err(e) => invalid(format!("DROP SECRET name: {e}")),
        };
    }
    if !p.parse_keyword(Keyword::CREATE) {
        return None;
    }
    let replace = p.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    let temporary = p.parse_one_of_keywords(&[Keyword::TEMPORARY, Keyword::TEMP]).is_some(); // (the session's, in memory: `temp.rs`)
    let _ = p.parse_keyword(Keyword::PERSISTENT);
    if !p.parse_keyword(Keyword::SECRET) {
        return None;
    }
    let usage = "CREATE SECRET name (TYPE s3, KEY_ID '…', SECRET '…', SCOPE 's3://bucket')";
    let if_not_exists = p.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    if replace && if_not_exists {
        return invalid("CREATE OR REPLACE SECRET … IF NOT EXISTS: one or the other".into());
    }
    let Ok(name) = p.parse_identifier() else { return invalid(format!("which name? {usage}")) };
    if !p.consume_token(&Token::LParen) {
        return invalid(usage.into());
    }
    let mut params = BTreeMap::new();
    loop {
        let (Ok(k), v) = (p.parse_identifier(), p.next_token().token) else { return invalid(usage.into()) };
        let v = match v {
            Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) | Token::Number(s, _) => s,
            Token::Word(w) => w.value,
            t => return invalid(format!("{}: a value, not {t} ({usage})", k.value)),
        };
        params.insert(k.value.to_lowercase(), v);
        if p.consume_token(&Token::RParen) {
            break;
        }
        if !p.consume_token(&Token::Comma) {
            return invalid(usage.into());
        }
    }
    if temporary {
        return Some(crate::write::Stmt::TempSecret(name.value.to_lowercase(), params, replace, if_not_exists));
    }
    Some(crate::write::Stmt::Ddl(vec![Ddl::CreateSecret { name: name.value.to_lowercase(), params, replace, if_not_exists }]))
}

/// `ATTACH [DATABASE] [IF NOT EXISTS] 'url' [AS] name (TYPE delta | iceberg, ENDPOINT '…', …)`:
/// another engine's tables. Another lake (no TYPE, or TYPE pondra) is `ddl::Attach`, with its
/// READ_ONLY and ENDPOINT options (ADR-058). None: an ATTACH with no options, as before.
fn attach_statement(p: &mut datafusion::sql::sqlparser::parser::Parser) -> Option<crate::write::Stmt> {
    use datafusion::sql::sqlparser::{keywords::Keyword, tokenizer::Token};
    let usage = "ATTACH 's3://bucket/tables' AS name (TYPE delta), or ATTACH 'https://catalog' AS name (TYPE iceberg)";
    let _ = p.parse_keyword(Keyword::DATABASE);
    let _ = p.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let url = p.parse_literal_string().ok()?;
    let _ = p.parse_keyword(Keyword::AS);
    let name = p.parse_identifier().ok()?.value.to_lowercase();
    if !p.consume_token(&Token::LParen) {
        return None;
    }
    let mut options = BTreeMap::new();
    loop {
        let Ok(k) = p.parse_identifier() else { return Some(crate::write::Stmt::Invalid(usage.into())) };
        let v = match p.peek_token().token {
            Token::Comma | Token::RParen => "true".to_string(),
            _ => match p.next_token().token {
                Token::SingleQuotedString(s) | Token::Number(s, _) => s,
                Token::Word(w) => w.value,
                t => return Some(crate::write::Stmt::Invalid(format!("{}: a value, not {t} ({usage})", k.value))),
            },
        };
        options.insert(k.value.to_lowercase(), v);
        if p.consume_token(&Token::RParen) {
            break;
        }
        if !p.consume_token(&Token::Comma) {
            return Some(crate::write::Stmt::Invalid(usage.into()));
        }
    }
    let kind = options.remove("type").unwrap_or_default().to_lowercase();
    if kind.is_empty() || kind == "pondra" {
        let read_only = options.remove("read_only").is_some_and(|v| !v.eq_ignore_ascii_case("false"));
        let endpoint = options.remove("endpoint");
        if let Some(k) = options.keys().next() {
            return Some(crate::write::Stmt::Invalid(format!("ATTACH 'lake' AS name (READ_ONLY, ENDPOINT 'https://…'): {k} isn't an option of a Pondra lake")));
        }
        return Some(crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::Attach { name, dir: url, read_only, endpoint }]));
    }
    let mut url = url;
    if kind == "share" && url.trim_start().starts_with('{') {
        // (a profile, as its provider gave it: where the door is, and the token)
        let p: serde_json::Value = match serde_json::from_str(&url) {
            Ok(p) => p,
            Err(e) => return Some(crate::write::Stmt::Invalid(format!("ATTACH '<a share\'s profile>' AS name (TYPE share): its JSON doesn't read ({e})"))),
        };
        let (Some(endpoint), Some(token)) = (p["endpoint"].as_str(), p["bearerToken"].as_str()) else {
            return Some(crate::write::Stmt::Invalid("a share's profile has its endpoint and bearerToken".into()));
        };
        options.insert("token".into(), token.to_string());
        url = endpoint.to_string();
    }
    Some(crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::AttachOutside { name, url, kind, options }]))
}

/// `COPY (query) TO 'url' (FORMAT parquet, PARTITION_BY (a, b), HEADER true, DELIMITER ';')` or
/// `COPY table TO …`, DuckDB's: a value is a word, a string, a number or a list; an option alone
/// is true. None: not a COPY to a file (`COPY … TO STDOUT` is the Postgres port's).
fn copy_statement(p: &mut datafusion::sql::sqlparser::parser::Parser) -> Option<crate::write::Stmt> {
    use datafusion::sql::sqlparser::{keywords::Keyword, tokenizer::Token};
    if !p.parse_keyword(Keyword::COPY) {
        return None;
    }
    let invalid = |e: String| Some(crate::write::Stmt::Invalid(format!("COPY … TO 'url' (FORMAT parquet, …): {e}")));
    let query = match p.consume_token(&Token::LParen) {
        true => match p.parse_query().and_then(|q| p.expect_token(&Token::RParen).map(|_| q)) {
            Ok(q) => q.to_string(),
            Err(e) => return invalid(e.to_string()),
        },
        false => match p.parse_object_name(false) {
            Ok(t) => format!("SELECT * FROM {t}"),
            Err(e) => return invalid(e.to_string()),
        },
    };
    if !p.parse_keyword(Keyword::TO) {
        return None; // (COPY … FROM: the Postgres port's)
    }
    let Ok(to) = p.parse_literal_string() else { return None }; // (TO STDOUT)
    let mut options = BTreeMap::new();
    // DataFusion's words for the same: STORED AS csv, PARTITIONED BY (…), OPTIONS ('format.has_header' 'false', …).
    loop {
        if p.parse_keywords(&[Keyword::STORED, Keyword::AS]) {
            let Ok(f) = p.parse_identifier() else { return invalid("STORED AS parquet, csv, json or arrow".into()) };
            options.insert("format".to_string(), f.value.to_lowercase());
        } else if p.parse_keywords(&[Keyword::PARTITIONED, Keyword::BY]) {
            match p.expect_token(&Token::LParen).and_then(|_| p.parse_comma_separated(|p| p.parse_identifier())).and_then(|l| p.expect_token(&Token::RParen).map(|_| l)) {
                Ok(l) => _ = options.insert("partition_by".to_string(), l.iter().map(crate::write::ident).collect::<Vec<_>>().join(",")),
                Err(e) => return invalid(e.to_string()),
            }
        } else if p.parse_keyword(Keyword::OPTIONS) {
            if p.expect_token(&Token::LParen).is_err() {
                return invalid("OPTIONS ('format.has_header' 'true', …)".into());
            }
            while !p.consume_token(&Token::RParen) {
                let (Ok(k), Ok(v)) = (p.parse_literal_string(), p.parse_value()) else { return invalid("OPTIONS ('key' 'value', …)".into()) };
                let v = match v.value { Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s, v => v.to_string() };
                let k = k.to_lowercase();
                let k = match k.strip_prefix("format.").unwrap_or(&k) {
                    "has_header" => "header",
                    "max_row_group_size" => "row_group_size",
                    k => k,
                };
                options.insert(k.to_string(), v);
                _ = p.consume_token(&Token::Comma);
            }
        } else {
            break;
        }
    }
    if p.consume_token(&Token::LParen) && !p.consume_token(&Token::RParen) {
        loop {
            let Ok(k) = p.parse_identifier() else { return invalid("an option's name".into()) };
            let v = match p.peek_token().token {
                Token::Comma | Token::RParen => "true".to_string(),
                Token::LParen => match p.expect_token(&Token::LParen).and_then(|_| p.parse_comma_separated(|p| p.parse_identifier())).and_then(|l| p.expect_token(&Token::RParen).map(|_| l)) {
                    Ok(l) => l.iter().map(|i| if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() }).collect::<Vec<_>>().join(","), // (as SQL reads names)
                    Err(e) => return invalid(e.to_string()),
                },
                _ => match p.next_token().token {
                    Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) | Token::Number(s, _) => s,
                    Token::Word(w) => w.value,
                    t => return invalid(format!("{}: a value, not {t}", k.value)),
                },
            };
            options.insert(k.value.to_lowercase(), v);
            if p.consume_token(&Token::RParen) {
                break;
            }
            if !p.consume_token(&Token::Comma) {
                return invalid("options are separated by commas".into());
            }
        }
    }
    Some(crate::write::Stmt::CopyTo(query, to, options))
}

/// Clauses other engines spell for a load, refused by name with Pondra's own spelling (Databricks'
/// FILEFORMAT, FORMAT_OPTIONS and COPY_OPTIONS, Snowflake's FILE_FORMAT and PATTERN).
const LOAD_REFUSED: [(&str, &str); 5] = [
    ("FILEFORMAT", "(FORMAT csv)"),
    ("FORMAT_OPTIONS", "the reader's options by their names: (HEADER true, DELIM ';')"),
    ("COPY_OPTIONS", "(FORCE true), to load every file again"),
    ("FILE_FORMAT", "(FORMAT csv)"),
    ("PATTERN", "a glob in the path: 's3://b/x/*.csv'"),
];

/// `COPY INTO t FROM 's3://b/x/*.csv' (FORMAT csv, HEADER true)` or `COPY INTO t FROM (SELECT … FROM
/// 's3://b/x/*.csv')`: each file loaded once a version (`copy.rs`). None: not a COPY INTO.
fn copy_into_statement(p: &mut datafusion::sql::sqlparser::parser::Parser) -> Option<crate::write::Stmt> {
    use crate::copy::Source;
    use datafusion::sql::sqlparser::{keywords::Keyword, tokenizer::Token};
    if !p.parse_keywords(&[Keyword::COPY, Keyword::INTO]) {
        return None;
    }
    let usage = "COPY INTO t FROM 's3://b/x/*.csv' (FORMAT csv, HEADER true), or FROM (SELECT … FROM 's3://b/x/*.csv')";
    let invalid = |e: String| Some(crate::write::Stmt::Invalid(format!("{usage}: {e}")));
    let table = match p.parse_object_name(false) {
        Ok(t) => crate::write::object(&t),
        Err(e) => return invalid(e.to_string()),
    };
    if !p.parse_keyword(Keyword::FROM) {
        return invalid("FROM is missing".into());
    }
    let from = if p.consume_token(&Token::LParen) {
        match p.parse_query().and_then(|q| p.expect_token(&Token::RParen).map(|_| q)) {
            Ok(q) => Source::Query(q.to_string()),
            Err(e) => return invalid(e.to_string()),
        }
    } else {
        match p.parse_literal_string() {
            Ok(path) if path.starts_with('@') => {
                // (Snowflake's stage: Pondra has none, it reads the files where they are)
                let stage = path.split('/').next().unwrap_or(&path);
                return Some(crate::write::Stmt::Invalid(format!(
                    "COPY INTO {table} FROM {stage}: a stage isn't supported; name the files where they are: COPY INTO {table} FROM 's3://bucket/folder/*.csv' (FORMAT csv, HEADER true)"
                )));
            }
            Ok(path) => Source::Path(path),
            Err(_) => return invalid("the path is a string".into()),
        }
    };
    let mut options = BTreeMap::new();
    if let Token::Word(w) = p.peek_token().token {
        // (a clause outside the parentheses: another engine's spelling, or Pondra's options left out)
        let name = w.value.to_uppercase();
        return invalid(match LOAD_REFUSED.iter().find(|(n, _)| *n == name) {
            _ if name == "FORCE" => "FORCE: Pondra writes (FORCE true)".into(),
            Some((n, hint)) => format!("{n}: Pondra writes {hint}"),
            None => format!("{}: the options go in parentheses after the path: (FORMAT csv, HEADER true)", w.value),
        });
    }
    if p.consume_token(&Token::LParen) && !p.consume_token(&Token::RParen) {
        loop {
            let Ok(k) = p.parse_identifier() else { return invalid("an option's name".into()) };
            let name = k.value.to_uppercase();
            if let Some((_, hint)) = LOAD_REFUSED.iter().find(|(n, _)| *n == name) {
                return invalid(format!("{name}: Pondra writes {hint}"));
            }
            let eq = p.consume_token(&Token::Eq); // (Snowflake's FORCE = TRUE: Pondra's is (FORCE true))
            if eq && name == "FORCE" {
                return invalid("FORCE: Pondra writes (FORCE true)".into());
            }
            let v = match p.peek_token().token {
                Token::Comma | Token::RParen => "true".to_string(),
                _ => match p.next_token().token {
                    Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) | Token::Number(s, _) => s,
                    Token::Word(w) => w.value,
                    t => return invalid(format!("{}: a value, not {t}", k.value)),
                },
            };
            options.insert(k.value.to_lowercase(), v);
            if p.consume_token(&Token::RParen) {
                break;
            }
            if !p.consume_token(&Token::Comma) {
                return invalid("options are separated by commas".into());
            }
        }
    }
    match p.peek_token().token {
        Token::EOF | Token::SemiColon => {}
        t => return invalid(format!("{t} after the options")),
    }
    Some(crate::write::Stmt::CopyInto(table, from, options))
}

/// Leader: keep a secret, sealed. Its scope's bucket may have no other secret's scope in it:
/// a bucket is read with one secret.
/// A secret's type, scope and values, checked, and sealed (`made`).
fn made(name: &str, mut params: BTreeMap<String, String>) -> Result<Secret> {
    crate::ddl::check(name)?;
    let t = params.remove("type").context("which TYPE? (s3, r2, gcs, azure, http or generic)")?.to_lowercase();
    let (known, schemes) = kind(&t)?;
    if let Some(k) = params.keys().find(|k| !known.is_empty() && !known.contains(&k.as_str())) {
        bail!("a {t} secret has no {k}: {}", known.join(", "));
    }
    let scope = params.remove("scope");
    ensure!(t != "git" || scope.is_some(), "a git secret says where its token may go: SCOPE 'https://github.com/acme'");
    if let Some(s) = &scope {
        ensure!(scheme(s).is_some_and(|x| schemes.contains(&x)), "a {t} secret's SCOPE is a URL of {}", schemes.iter().map(|s| format!("{s}://")).collect::<Vec<_>>().join(" or "));
    }
    let (sealed, key) = seal_new(&serde_json::to_vec(&params)?)?;
    Ok(Secret { kind: t, scope, sealed, key: Some(key), temporary: false, trusted: false, lent: false })
}

/// `CREATE TEMPORARY SECRET`: a session's own, in this node's memory only (`temp.rs`), used by its
/// queries before the lake's of the same scope, never written anywhere. One made by anyone but an
/// admin or the node's owner reads with its own keys, on the public internet: not with the node's
/// credentials (S37), not a topic (its brokers name others to connect to), and not an address of
/// the node's own network (`public.rs`, S6).
pub fn temporary(name: &str, params: BTreeMap<String, String>) -> Result<Secret> {
    let trusted = owner() || crate::auth::current().is_none_or(|p| p.role >= crate::auth::Role::Admin);
    if !trusted {
        let chain = params.get("provider").is_some_and(|p| p.eq_ignore_ascii_case("credential_chain"));
        ensure!(!chain, "PROVIDER credential_chain reads with the node's own credentials, which only an admin lends: CREATE SECRET {name} (…, PROVIDER credential_chain, SCOPE '…') and GRANT USAGE ON SECRET {name}");
        ensure!(!params.get("type").is_some_and(|t| t.eq_ignore_ascii_case("kafka")), "a temporary secret of TYPE kafka is an admin's: a topic's brokers name others to connect to, anywhere");
    }
    Ok(Secret { temporary: true, trusted, ..made(name, params)? })
}

pub async fn create(lake: &Lake, name: &str, params: BTreeMap<String, String>, replace: bool, if_not_exists: bool) -> Result<serde_json::Value> {
    let secret = made(name, params)?;
    let scope = secret.scope.clone();
    if lake.cat.get::<Secret>(&secret_key(name)).await?.is_some() {
        ensure!(replace || if_not_exists, "secret {name} already exists (CREATE OR REPLACE SECRET)");
        if !replace {
            return Ok(serde_json::json!({"secret": name, "unchanged": true}));
        }
    }
    let bucket = |s: &str| root(s);
    if let Some(b) = scope.as_deref().and_then(bucket).filter(|_| secret.kind != "git") { // (git tokens: each person has their own for the same host)
        for (other, s) in list(lake).await? {
            let same = s.kind != "git" && s.scope.as_deref().is_some_and(|o| bucket(o).as_deref() == Some(b.as_str()));
            ensure!(other == name || !same, "{b} already has secret {other} for {}: a bucket is read with one secret (one SCOPE in it)", s.scope.unwrap_or_default());
        }
    }
    lake.cat.commit(vec![(secret_key(name), json(&secret))], &[]).await?;
    Ok(serde_json::json!({"secret": name}))
}

pub async fn drop(lake: &Lake, name: &str, if_exists: bool) -> Result<serde_json::Value> {
    if lake.cat.get::<Secret>(&secret_key(name)).await?.is_none() {
        ensure!(if_exists, "no secret {name}");
        return Ok(serde_json::json!({"secret": name, "dropped": false}));
    }
    lake.cat.commit(vec![], &[secret_key(name)]).await?;
    Ok(serde_json::json!({"secret": name, "dropped": true}))
}

/// The lake's secrets, and the session's temporary ones (which win, by name).
pub async fn list(lake: &Lake) -> Result<Vec<(String, Secret)>> {
    let mut all: Vec<(String, Secret)> = crate::temp::secrets();
    for (k, s) in lake.cat.scan::<Secret>("e/", "e0").await? {
        if !all.iter().any(|(n, _)| *n == k[2..]) {
            all.push((k[2..].to_string(), s));
        }
    }
    Ok(all)
}

/// May the request being served use this secret? Its own temporary one, or one a user is granted
/// USAGE on (`GRANT USAGE ON SECRET`); a token's or a superuser's, any but a key lent to read a base.
pub(crate) fn usable(name: &str, s: &Secret) -> bool { !s.lent && (s.temporary || crate::auth::limited().is_none_or(|a| a.secret(name))) }

/// The secret for a URL: the longest scope that is a prefix of it (no scope: every URL of its
/// type's schemes).
pub(crate) fn covering(secrets: &[(String, Secret)], url: &str) -> Option<(String, Secret)> {
    scoped(secrets.iter().filter(|(n, x)| usable(n, x)), url)
}

/// The same among `secrets`, whoever asks.
fn scoped<'a>(secrets: impl Iterator<Item = &'a (String, Secret)>, url: &str) -> Option<(String, Secret)> {
    let s = scheme(url)?;
    secrets.filter(|(_, x)| !matches!(x.kind.as_str(), "pondra" | "git") && kind(&x.kind).is_ok_and(|(_, schemes)| schemes.contains(&s)))
        .filter(|(_, x)| x.scope.as_deref().is_none_or(|p| within(url, p)))
        .max_by_key(|(_, x)| x.scope.as_ref().map_or(0, |p| p.len() + 1)).cloned()
}

/// Is `url` inside `scope`? The scope whole, or followed by a path, a query or a fragment: so
/// `http://127.0.0.1:979` doesn't cover port 9790, nor `s3://data` the bucket `data-other` (S7).
fn within(url: &str, scope: &str) -> bool {
    url.strip_prefix(scope).is_some_and(|rest| scope.ends_with('/') || rest.is_empty() || rest.starts_with(['/', '?', '#']))
}

/// A secret's values by its name, for a procedure's code (`server::secret`): its settings, and
/// `type` and `scope`.
pub async fn reveal(lake: &Lake, name: &str) -> Result<BTreeMap<String, String>> {
    let (name, s) = list(lake).await?.into_iter().find(|(n, _)| n == name).with_context(|| format!("no secret {name} (CREATE SECRET {name} (TYPE generic, …))"))?;
    ensure!(usable(&name, &s), "permission denied: USAGE on secret {name} (GRANT USAGE ON SECRET {name} TO …)");
    let mut values = open(&name, &s)?;
    values.entry("type".into()).or_insert(s.kind.clone());
    Ok(values)
}

/// A secret's values, for the catalog that was attached with it (`catalogs.rs`).
pub(crate) fn reveal_secret(name: &str, s: &Secret) -> Result<BTreeMap<String, String>> { open(name, s) }

/// A secret's values: its data key unwrapped by the master key, then its values opened with it.
fn open(name: &str, s: &Secret) -> Result<BTreeMap<String, String>> {
    let damaged = || anyhow!("secret {name} was sealed with another master key: every node needs the same PONDRA_SECRET_KEY or PONDRA_ADMIN_TOKEN (or PONDRA_KMS_COMMAND)");
    let plain = match &s.key {
        Some(wrapped) => {
            let key = unwrap_key(wrapped).map_err(|e| anyhow!("{}: {e:#}", damaged()))?;
            open_with(&key, &s.sealed).ok_or_else(damaged)?
        }
        None => masters()?.iter().find_map(|m| open_with(m, &s.sealed)).ok_or_else(damaged)?, // (sealed before round 29: by a master key itself)
    };
    Ok(serde_json::from_slice(&plain)?)
}

/// `plain` sealed with a new data key: (the sealed values, the data key wrapped by the master key).
fn seal_new(plain: &[u8]) -> Result<(String, String)> {
    let mut key = [0u8; 32];
    aws_lc_rs::rand::fill(&mut key).map_err(|_| anyhow!("no randomness"))?;
    Ok((seal_with(&key, plain)?, wrap_key(&key)?))
}

fn seal_with(key: &[u8], plain: &[u8]) -> Result<String> {
    let mut nonce = [0u8; 12];
    aws_lc_rs::rand::fill(&mut nonce).map_err(|_| anyhow!("no randomness"))?;
    let mut data = plain.to_vec();
    LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).map_err(|_| anyhow!("key"))?).seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::empty(), &mut data).map_err(|_| anyhow!("sealing failed"))?;
    Ok(B64.encode([nonce.as_slice(), &data].concat()))
}

fn open_with(key: &[u8], sealed: &str) -> Option<Vec<u8>> {
    let sealed = B64.decode(sealed).ok()?;
    (sealed.len() > 12).then_some(())?;
    let (nonce, data) = sealed.split_at(12);
    let mut data = data.to_vec();
    let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).ok()?);
    key.open_in_place(Nonce::try_assume_unique_for_key(nonce).ok()?, Aad::empty(), &mut data).ok().map(|p| p.to_vec())
}

crate::per_node!(
    /// Data keys unwrapped so far (by their wrapped form): a query using a secret again doesn't ask the
    /// key service again.
    static UNWRAPPED: std::sync::Mutex<HashMap<String, Vec<u8>>> = Default::default());

/// A data key wrapped by the master key: by `PONDRA_KMS_COMMAND wrap` (a key service: its key never
/// leaves it), or sealed with the local master key (`PONDRA_SECRET_KEY`, else the admin token's, else
/// this machine's).
fn wrap_key(key: &[u8]) -> Result<String> {
    match kms() {
        Some(cmd) => Ok(format!("kms:{}", kms_run(&cmd, "wrap", &B64.encode(key))?)),
        None => seal_with(&master()?, key),
    }
}

/// A data key, unwrapped: by the key service for `kms:…`; else by the master key, or one of the others
/// that may have wrapped it (`masters`) while keys move to a new one (`rewrap`).
fn unwrap_key(wrapped: &str) -> Result<Vec<u8>> {
    if let Some(k) = UNWRAPPED.lock().unwrap().get(wrapped) {
        return Ok(k.clone());
    }
    let key = match wrapped.strip_prefix("kms:") {
        Some(w) => B64.decode(kms_run(&kms().context("this secret's key was wrapped by a key service: set PONDRA_KMS_COMMAND")?, "unwrap", w)?)?,
        None => masters()?.iter().find_map(|m| open_with(m, wrapped)).context("its key was wrapped by another master key: set the PONDRA_SECRET_KEY or PONDRA_ADMIN_TOKEN it was sealed with (or the old one as PONDRA_SECRET_KEY_PREVIOUS / PONDRA_ADMIN_TOKEN_PREVIOUS)")?,
    };
    UNWRAPPED.lock().unwrap().insert(wrapped.to_string(), key.clone());
    Ok(key)
}

fn kms() -> Option<String> { crate::env::get("PONDRA_KMS_COMMAND").filter(|c| !c.trim().is_empty()) }

/// Run the key service's command (`<command> wrap|unwrap`, the input on stdin, the answer on stdout):
/// `aws kms`, `gcloud kms`, `az keyvault key`, `vault write transit/…`, through a short script.
fn kms_run(cmd: &str, what: &str, input: &str) -> Result<String> {
    use std::io::Write;
    let shell = if cfg!(windows) { ("cmd", "/C") } else { ("sh", "-c") };
    let mut child = std::process::Command::new(shell.0).args([shell.1, &format!("{cmd} {what}")]).stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().context("PONDRA_KMS_COMMAND")?;
    child.stdin.take().context("stdin")?.write_all(input.as_bytes())?;
    let out = child.wait_with_output()?;
    ensure!(out.status.success(), "PONDRA_KMS_COMMAND {what}: {}", String::from_utf8_lossy(&out.stderr).trim());
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

/// Is the master key one every node can have: `PONDRA_SECRET_KEY`, the admin token (every node of a
/// cluster with one has the same), or a key service? Never only this machine's own
/// (`~/.pondra/secret.key`), which a node on another machine couldn't open with.
pub fn shared_master() -> bool {
    kms().is_some() || crate::env::is_set("PONDRA_SECRET_KEY") || crate::env::is_set("PONDRA_ADMIN_TOKEN")
}

/// `plain` sealed with a data key of its own, which the master key wraps: (the sealed bytes, the
/// wrapped key). The lake's own keys are kept this way (`users::Kept`), as a secret's values are.
pub fn seal(plain: &[u8]) -> Result<(String, String)> { seal_new(plain) }

/// What `seal` made, opened: its data key unwrapped by the master key (or the previous one).
pub fn unseal(sealed: &str, wrapped: &str) -> Result<Vec<u8>> { open_with(&unwrap_key(wrapped)?, sealed).context("sealed with another data key") }

/// A data key wrapped again by the master key in use now, when another wrapped it (the previous
/// master key, or the local key before a key service was set); `None` when it needs nothing.
pub fn rewrapped(wrapped: &str) -> Result<Option<String>> {
    if wrapped.starts_with("kms:") == kms().is_some() && (wrapped.starts_with("kms:") || open_with(&master()?, wrapped).is_some()) {
        return Ok(None);
    }
    Ok(Some(wrap_key(&unwrap_key(wrapped)?)?))
}

/// Leader: every secret's data key wrapped by the master key in use now (after it changed: the
/// previous one still set), its values untouched. How many were.
pub async fn rewrap(lake: &Lake) -> Result<usize> {
    let mut puts = vec![];
    for (k, mut s) in lake.cat.scan::<Secret>("e/", "e0").await? {
        match s.key.clone() {
            Some(w) => match rewrapped(&w)? {
                Some(again) => s.key = Some(again),
                None => continue,
            },
            None => {
                // (sealed by the master key itself, before round 29: sealed again, with a key of its own)
                let plain = masters()?.iter().find_map(|m| open_with(m, &s.sealed)).with_context(|| format!("secret {}", &k[2..]))?;
                (s.sealed, s.key) = { let (a, b) = seal_new(&plain)?; (a, Some(b)) };
            }
        }
        puts.push((k, json(&s)));
    }
    let n = puts.len();
    if n > 0 {
        lake.cat.commit(puts, &[]).await?;
    }
    Ok(n)
}

/// The master key, when no key service is set: `PONDRA_SECRET_KEY` (the same on every node of a
/// cluster), else the admin token's (`admin_key`: every node of a cluster with one has it), else one
/// made for this machine (`~/.pondra/secret.key`), enough for nodes that share it.
fn master() -> Result<[u8; 32]> {
    if let Some(k) = crate::env::get("PONDRA_SECRET_KEY") {
        return Ok(sha256_of(&k));
    }
    if let Some(t) = crate::env::get("PONDRA_ADMIN_TOKEN") {
        return Ok(admin_key(&t));
    }
    Ok(sha256_of(&machine_key()?))
}

/// The master keys before the one in use, which may have wrapped a data key: keys they wrapped still
/// open, and the leader rewraps them with the new one when it starts (`rewrap`). The admin token is
/// one of them when a lake moved from it to `PONDRA_SECRET_KEY`; this machine's key only if an older
/// release made it (`machine_key_if_made`).
fn previous() -> Vec<[u8; 32]> {
    let set = crate::env::get;
    [
        set("PONDRA_SECRET_KEY_PREVIOUS").map(|k| sha256_of(&k)),
        set("PONDRA_ADMIN_TOKEN").map(|t| admin_key(&t)),
        set("PONDRA_ADMIN_TOKEN_PREVIOUS").map(|t| admin_key(&t)),
        machine_key_if_made().map(|k| sha256_of(&k)),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// The master key in use first, then the others (`previous`): what may open a data key.
fn masters() -> Result<Vec<[u8; 32]>> {
    let mut all = vec![master()?];
    all.extend(previous());
    Ok(all)
}

/// The master key an admin token makes: PBKDF2 over the token, so a short token can't be guessed
/// offline from what the bucket holds (the keys sealed by it). Worked out once a node per token.
fn admin_key(token: &str) -> [u8; 32] {
    crate::per_node!(static KEYS: Mutex<HashMap<String, [u8; 32]>> = Mutex::new(HashMap::new()));
    if let Some(k) = KEYS.lock().unwrap().get(token) {
        return *k;
    }
    if token.chars().count() < 16 {
        eprintln!("the admin token is short: it guards the lake's keys and secrets too (use 32 random characters or more)");
    }
    let mut key = [0u8; 32];
    let rounds = std::num::NonZeroU32::new(100_000).expect("non-zero");
    aws_lc_rs::pbkdf2::derive(aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA256, rounds, b"pondra: the master key from the admin token", token.as_bytes(), &mut key);
    KEYS.lock().unwrap().insert(token.to_string(), key);
    key
}

/// A key's SHA-256: the 32 bytes AES-256 takes.
fn sha256_of(key: &str) -> [u8; 32] { aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, key.as_bytes()).as_ref().try_into().expect("32 bytes") }

/// This machine's key if an older release made one (`machine_key`'s file): never made here.
fn machine_key_if_made() -> Option<String> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))?;
    let key = std::fs::read_to_string(std::path::PathBuf::from(home).join(".pondra").join("secret.key")).ok()?;
    Some(key.trim().to_string()).filter(|k| is_key(k))
}

/// A machine key is 32 random bytes in hex: anything else in its file is refused, never used.
fn is_key(k: &str) -> bool { k.len() == 64 && k.bytes().all(|b| b.is_ascii_hexdigit()) }

fn machine_key() -> Result<String> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).context("secrets need a key: set PONDRA_SECRET_KEY")?;
    let path = std::path::PathBuf::from(home).join(".pondra").join("secret.key");
    if let Ok(k) = std::fs::read_to_string(&path) {
        return checked_key(&path, &k);
    }
    std::fs::create_dir_all(path.parent().expect("a folder"))?;
    let mut bytes = [0u8; 32];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| anyhow!("no randomness"))?;
    let key: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    match link_key(&key, &path) {
        Ok(()) => Ok(key),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => checked_key(&path, &std::fs::read_to_string(&path)?), // (another node, at once: a file is linked only once whole)
        Err(e) => Err(e.into()),
    }
}

/// The key in a machine key file, trimmed: a file that holds less than a key is an error (a crash
/// once left an empty one, and it would have sealed secrets with nothing).
fn checked_key(path: &std::path::Path, text: &str) -> Result<String> {
    let k = text.trim();
    if !is_key(k) {
        bail!("{} is empty or cut short ({} characters, not 64): if no secret was sealed with it, delete it and a new one is made; otherwise restore it from a backup", path.display(), k.len());
    }
    Ok(k.to_string())
}

/// Writes `key` as `path`, whole or not at all: it goes to a file of its own beside `path`, is synced,
/// then hard-linked to `path`, which fails if `path` exists (another node made it first). A crash
/// leaves at most a stray file beside the key, never half a key.
fn link_key(key: &str, path: &std::path::Path) -> std::io::Result<()> {
    let mut salt = [0u8; 8];
    aws_lc_rs::rand::fill(&mut salt).map_err(|_| std::io::Error::other("no randomness"))?;
    let tmp = path.with_file_name(format!("secret.key.{}.{}", std::process::id(), salt.iter().map(|b| format!("{b:02x}")).collect::<String>()));
    let mut f = std::fs::OpenOptions::new();
    f.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut f, 0o600);
    let linked = f
        .open(&tmp)
        .and_then(|mut file| {
            std::io::Write::write_all(&mut file, key.as_bytes())?;
            file.sync_all()
        })
        .and_then(|()| std::fs::hard_link(&tmp, path));
    let _ = std::fs::remove_file(&tmp); // (the key's name holds the key now, or nothing did)
    linked
}

/// `secrets()`: each secret's name, type and scope — never its values.
pub fn register_secrets(ctx: &datafusion::prelude::SessionContext, lake: Arc<Lake>) {
    ctx.register_udtf("secrets", Arc::new(Listing(lake)));
}

#[derive(Debug)]
struct Listing(Arc<Lake>);

impl datafusion::catalog::TableFunctionImpl for Listing {
    fn call(&self, _: &[datafusion::prelude::Expr]) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        Ok(Arc::new(Secrets(self.0.clone())))
    }
}

struct Secrets(Arc<Lake>);

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "secrets()") }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for Secrets {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        use datafusion::arrow::datatypes::{DataType::Utf8, Field, Schema};
        Arc::new(Schema::new(vec![Field::new("name", Utf8, false), Field::new("type", Utf8, false), Field::new("scope", Utf8, true)]))
    }
    fn table_type(&self) -> datafusion::datasource::TableType { datafusion::datasource::TableType::View }

    async fn scan(&self, state: &dyn datafusion::catalog::Session, projection: Option<&Vec<usize>>, _: &[datafusion::prelude::Expr], _: Option<usize>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::arrow::array::StringArray;
        let e = |e: anyhow::Error| datafusion::error::DataFusionError::External(e.into());
        let all = list(&self.0).await.map_err(e)?;
        let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = vec![
            Arc::new(all.iter().map(|(n, _)| Some(n.as_str())).collect::<StringArray>()),
            Arc::new(all.iter().map(|(_, s)| Some(s.kind.as_str())).collect::<StringArray>()),
            Arc::new(all.iter().map(|(_, s)| s.scope.as_deref()).collect::<StringArray>()),
        ];
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(self.schema(), columns)?;
        datafusion::catalog::MemTable::try_new(self.schema(), vec![vec![batch]])?.scan(state, projection, &[], None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_machine_key_file_is_whole_or_refused() {
        let key = "ab".repeat(32);
        let p = std::path::Path::new("secret.key");
        assert_eq!(checked_key(p, &format!("{key}\n")).unwrap(), key);
        assert!(checked_key(p, "ab").is_err());
        assert!(checked_key(p, "").is_err());
        let dir = std::env::temp_dir().join(format!("pondra-key-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("secret.key");
        let _ = std::fs::remove_file(&path);
        link_key(&key, &path).unwrap();
        assert!(link_key(&"cd".repeat(32), &path).is_err_and(|e| e.kind() == std::io::ErrorKind::AlreadyExists));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), key);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_scope_covers_its_url_whole_or_followed_by_a_path() {
        assert!(within("http://127.0.0.1:979/x.csv", "http://127.0.0.1:979") && within("http://127.0.0.1:979", "http://127.0.0.1:979"));
        assert!(within("s3://data/a/b.parquet", "s3://data/a/") && within("https://h/x?y=1", "https://h/x"));
        assert!(!within("http://127.0.0.1:9790/x.csv", "http://127.0.0.1:979"), "another port");
        assert!(!within("s3://data-other/x", "s3://data") && !within("https://prod.example.com.evil.net/", "https://prod.example.com"), "another bucket, another host");
        assert!(!within("s3://data/ab/x", "s3://data/a"), "another folder");
    }
}
