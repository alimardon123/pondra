//! The Postgres wire protocol (`--pg 0.0.0.0:5432`): psql, drivers (psycopg, JDBC, Go, Node,
//! SQLAlchemy and pandas) and BI tools talk to any node as they would to Postgres. Queries and
//! writes (CREATE TABLE, INSERT, UPDATE, DELETE) run exactly as `POST /sql` runs them. Results go
//! out as text or binary, whichever the client asks for, a batch at a time; `$1` parameters are
//! bound as literals. `COPY … TO STDOUT` sends a table or a query in text, CSV or binary (what the
//! ADBC Postgres driver reads results with), `COPY … FROM STDIN` loads text or CSV (psql's
//! `\copy`, psycopg's `cursor.copy`).
//! With tokens set, the user name picks the role (`reader`, `writer`, `admin`) and the password
//! is that role's token.
use crate::query::read_only;
use crate::server::App;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Schema, TimeUnit, TimestampMicrosecondType};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{stream, Sink, SinkExt};
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::copy::CopyHandler;
use pgwire::api::results::{CopyCsvOptions, CopyEncoder, CopyResponse, CopyTextOptions, DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use std::fmt::Debug;
use std::sync::Arc;

/// Accept Postgres clients on `addr`.
pub async fn serve(app: App, addr: String) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let pg = Arc::new(Pg(Arc::new(Backend { app, parser: Arc::new(NoopQueryParser::new()) })));
    loop {
        let (socket, _) = listener.accept().await?;
        let pg = pg.clone();
        tokio::spawn(async move { pgwire::tokio::process_socket(socket, None, pg).await });
    }
}

struct Pg(Arc<Backend>);

struct Backend {
    app: App,
    parser: Arc<NoopQueryParser>,
}

impl PgWireServerHandlers for Pg {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> { self.0.clone() }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> { self.0.clone() }
    fn copy_handler(&self) -> Arc<impl CopyHandler> { self.0.clone() }
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let mut params = DefaultServerParameterProvider::default();
        params.server_version = "16.0 (Pondra)".into();
        let password = CleartextPasswordAuthStartupHandler::new(Tokens(self.0.app.clone()), params);
        Arc::new(Startup { password, open: Arc::new(Open), on: self.0.app.auth.on() })
    }
}

/// With tokens: a password check; without: straight in.
struct Startup {
    password: CleartextPasswordAuthStartupHandler<Tokens, DefaultServerParameterProvider>,
    open: Arc<Open>,
    on: bool,
}

struct Open;

impl NoopStartupHandler for Open {}

#[async_trait]
impl StartupHandler for Startup {
    async fn on_startup<C>(&self, client: &mut C, message: PgWireFrontendMessage) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self.on {
            true => self.password.on_startup(client, message).await,
            false => self.open.on_startup(client, message).await,
        }
    }
}

/// Passwords: the user name picks the role, whose token is the password (anything goes when no
/// tokens are set).
#[derive(Debug)]
struct Tokens(App);

impl Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str("App") }
}

#[async_trait]
impl AuthSource for Tokens {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let token = self.0.auth.token_for(login.user().unwrap_or_default()).unwrap_or_default();
        Ok(Password::new(None, token.into_bytes()))
    }
}

fn user_error(e: anyhow::Error) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), "XX000".into(), format!("{e:#}"))))
}

impl Backend {
    /// Run one statement the way `POST /sql` does, for a client whose role comes from its user name.
    async fn run(&self, user: &str, sql: &str, format: &Format) -> PgWireResult<Response> {
        let sql = crate::routines::expand(&self.app.lake, sql).await.map_err(user_error)?; // (macros: ADR-023)
        let sql = crate::asof::rewrite(&pg_dialect(&self.app.lake, &sql)).map_err(user_error)?.into_owned();
        if let Some(r) = session_command(&sql) {
            return Ok(r);
        }
        if let Some(copy) = Copy::of(&sql) {
            return self.copy_out(copy?).await;
        }
        let role = self.app.auth.role_of_user(user);
        if crate::routines::call_of(&sql).is_some() {
            let who = crate::routines::Who { role, files: false, depth: 0 };
            return match crate::routines::one(&self.app, &sql, who, None).await.map_err(user_error)? {
                crate::routines::Outcome::Rows(batches) => {
                    let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
                    Ok(Response::Query(rows(&schema, batches, format)?))
                }
                crate::routines::Outcome::Done(_) => Ok(Response::Execution(Tag::new("CALL"))),
            };
        }
        if let Some(stmt) = crate::write::parse(&sql) {
            self.app.auth.allows(role, &stmt).map_err(user_error)?;
            let tag = sql.split_whitespace().next().unwrap_or("OK").to_uppercase();
            let v = crate::write::on_node(&self.app, stmt, None).await.map_err(user_error)?;
            let rows = v["rows"].as_u64().unwrap_or(0) as usize;
            return Ok(Response::Execution(if tag == "INSERT" { Tag::new("INSERT").with_oid(0).with_rows(rows) } else { Tag::new(&tag).with_rows(rows) }));
        }
        let batches = match sql.contains("pg_") {
            true => self.session(&sql).await?.sql_with_options(&sql, read_only()).await.map_err(|e| user_error(e.into()))?.collect().await.map_err(|e| user_error(e.into()))?,
            false => self.app.query(&sql, None).await.map_err(user_error)?,
        };
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => self.schema(&sql).await?,
        };
        Ok(Response::Query(rows(&schema, batches, format)?))
    }

    /// `COPY … TO STDOUT`: the rows, one COPY message each.
    async fn copy_out(&self, c: Copy) -> PgWireResult<Response> {
        let sql = match &c.query {
            Some(q) => q.clone(),
            None if c.columns.is_empty() => format!("SELECT * FROM {}", c.table),
            None => format!("SELECT {} FROM {}", c.columns.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", "), c.table),
        };
        let batches = self.app.query(&sql, None).await.map_err(user_error)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => self.schema(&sql).await?,
        };
        let format = if c.format == "binary" { FieldFormat::Binary } else { FieldFormat::Text };
        let (info, types) = fields(&schema, &|_| format);
        let mut enc = match c.format.as_str() {
            "binary" => CopyEncoder::new_binary(info.clone()),
            "csv" => CopyEncoder::new_csv(info.clone(), CopyCsvOptions { delimiter: c.delimiter.unwrap_or(',').to_string().as_str().into(), null_string: c.null.clone().unwrap_or_default().as_str().into(), ..Default::default() }),
            _ => CopyEncoder::new_text(info.clone(), CopyTextOptions { delimiter: c.delimiter.unwrap_or('\t').to_string().as_str().into(), null_string: c.null.clone().unwrap_or("\\N".into()).as_str().into() }),
        };
        let header = (c.header && c.format == "csv").then(|| {
            let names = schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(&c.delimiter.unwrap_or(',').to_string());
            Ok(CopyData::new(bytes::Bytes::from(names + "\n")))
        });
        let rows = batches.into_iter().flat_map(move |b| each_row(&b, &types, |cols, i| {
            for col in cols {
                encode!(&mut enc, col, i)?;
            }
            Ok(enc.take_copy())
        }));
        let data = stream::iter(header.into_iter().chain(rows));
        Ok(Response::CopyOut(CopyResponse::new(if c.format == "binary" { 1 } else { 0 }, schema.fields().len(), data)))
    }

    /// `COPY t FROM STDIN`: what is to come, noted on the connection (`on_copy_data` gathers it).
    async fn copy_in<C: ClientInfo>(&self, client: &mut C, user: &str, c: Copy) -> PgWireResult<Response> {
        let refuse = |why: &str| Err(user_error(anyhow::anyhow!("{why}")));
        if c.query.is_some() || c.format == "binary" {
            return refuse("COPY FROM STDIN takes a table, in text or CSV");
        }
        let stmt = crate::write::parse(&format!("INSERT INTO {} SELECT 1", c.table)).ok_or_else(|| user_error(anyhow::anyhow!("COPY: no table {}", c.table)))?;
        self.app.auth.allows(self.app.auth.role_of_user(user), &stmt).map_err(user_error)?;
        let (other, table) = crate::ddl::resolve(&self.app.lake, &c.table).await.map_err(user_error)?;
        if other.is_some() {
            return refuse("COPY into an attached lake: INSERT INTO it instead");
        }
        let meta = self.app.lake.cat.get::<crate::store::TableMeta>(&crate::store::table_key(&table)).await.map_err(user_error)?.ok_or_else(|| user_error(anyhow::anyhow!("no table {table}")))?.logical();
        let n = if c.columns.is_empty() { meta.columns.iter().filter(|(c, _)| c != "_deleted").count() } else { c.columns.len() };
        COPIES.lock().unwrap().insert(client.socket_addr(), Pending { copy: Copy { table, ..c }, data: vec![], rows: 0, seq: 0, job: uuid::Uuid::new_v4().to_string() });
        Ok(Response::CopyIn(CopyResponse::new(0, n, stream::empty())))
    }

    /// Rows a COPY FROM sent, up to the last whole line (`all`: everything), into the table's log.
    async fn load(&self, p: &mut Pending, all: bool) -> PgWireResult<()> {
        let cut = match all {
            true => p.data.len(),
            false => match p.copy.format.as_str() {
                // (a line ends a row unless it's inside quotes: an even number of them before it)
                "csv" => (0..p.data.len()).rev().filter(|&i| p.data[i] == b'\n').find(|&i| p.data[..i].iter().filter(|&&b| b == b'"').count() % 2 == 0).map_or(0, |i| i + 1),
                _ => p.data.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1),
            },
        };
        let chunk: Vec<u8> = p.data.drain(..cut).collect();
        let (header, csv) = (p.copy.header && p.seq == 0, p.copy.format == "csv");
        if chunk.iter().all(|b| b.is_ascii_whitespace() || *b == b'\\' || *b == b'.') {
            return Ok(()); // (nothing, or text format's end marker `\.`)
        }
        let err = |e: anyhow::Error| user_error(e);
        let meta = self.app.lake.cat.get::<crate::store::TableMeta>(&crate::store::table_key(&p.copy.table)).await.map_err(err)?.ok_or_else(|| err(anyhow::anyhow!("no table {}", p.copy.table)))?;
        let table = crate::query::schema(&meta.logical().columns).map_err(err)?; // (SQL's names: ADR-022)
        let given: Vec<String> = if p.copy.columns.is_empty() { table.fields().iter().map(|f| f.name().clone()).filter(|c| c != "_deleted").collect() } else { p.copy.columns.clone() }; // (as `SELECT *` shows it)
        let schema = Arc::new(Schema::new(given.iter().map(|c| table.field_with_name(c).cloned()).collect::<Result<Vec<_>, _>>().map_err(|e| err(e.into()))?));
        let null = regex::Regex::new(&format!("^{}$", regex::escape(&p.copy.null.clone().unwrap_or(if csv { String::new() } else { "\\N".into() })))).map_err(|e| err(e.into()))?;
        let mut reader = datafusion::arrow::csv::ReaderBuilder::new(schema.clone()).with_header(header).with_delimiter(p.copy.delimiter.unwrap_or(if csv { ',' } else { '\t' }) as u8).with_null_regex(null);
        if !csv {
            reader = reader.with_quote(0); // (text format quotes nothing)
        }
        let batches = reader.build(std::io::Cursor::new(chunk)).map_err(|e| err(e.into()))?.collect::<Result<Vec<_>, _>>().map_err(|e| err(e.into()))?;
        for b in batches {
            // the table's columns in its order, those not given null
            let cols = table.fields().iter().map(|f| b.column_by_name(f.name()).cloned().unwrap_or_else(|| datafusion::arrow::array::new_null_array(f.data_type(), b.num_rows()))).collect();
            let b = RecordBatch::try_new(table.clone(), cols).map_err(|e| err(e.into()))?;
            p.rows += b.num_rows() as u64;
            p.seq += 1;
            let src = crate::log::Src { producer: format!("copy:{}", p.job), seq: p.seq, prev: None };
            self.app.log().map_err(err)?.append(p.copy.table.clone(), src, b).await.map_err(err)?;
        }
        Ok(())
    }

    /// A query's result columns, without running it.
    async fn schema(&self, sql: &str) -> PgWireResult<Arc<Schema>> {
        let sql = crate::asof::rewrite(sql).map_err(user_error)?;
        let df = self.session(&sql).await?.sql_with_options(&sql, read_only()).await.map_err(|e| user_error(e.into()))?;
        Ok(Arc::new(df.schema().as_arrow().clone()))
    }

    /// A session for `sql`, with the little of Postgres's catalog that drivers look at on connect
    /// (types, namespaces, the tables) when it asks for it.
    async fn session(&self, sql: &str) -> PgWireResult<datafusion::prelude::SessionContext> {
        let ctx = crate::query::session(&self.app.lake, sql, "").await.map_err(user_error)?;
        if sql.contains("pg_") {
            let lake = &self.app.lake;
            let schemas = crate::ddl::schemas(lake).await.map_err(user_error)?; // (public first: oid 2200, as in Postgres)
            let oid = |schema: &str| schemas.iter().position(|s| s == schema).map_or(2200, |i| if i == 0 { 2200 } else { 30000 + i });
            let tables = lake.cat.scan::<crate::store::TableMeta>("t/", "t0").await.map_err(user_error)?.into_iter().filter(|(k, _)| !crate::sys::hidden(k)).map(|(k, _)| (k, 'r'));
            let views = lake.cat.scan::<crate::ddl::StoredView>("q/", "q0").await.map_err(user_error)?.into_iter().map(|(k, _)| (k, 'v'));
            let classes = tables.chain(views).enumerate().map(|(i, (k, kind))| format!("({}, '{}', {}, '{kind}')", 16384 + i, crate::ddl::split(&k[2..]).1, oid(crate::ddl::split(&k[2..]).0))).collect::<Vec<_>>();
            let namespaces = schemas.iter().map(|s| format!(", ({}, '{s}')", oid(s))).collect::<String>();
            // (with each type's binary receive function: what the ADBC driver knows a type by)
            let types = [(16, "bool", "boolrecv"), (17, "bytea", "bytearecv"), (20, "int8", "int8recv"), (21, "int2", "int2recv"), (23, "int4", "int4recv"), (25, "text", "textrecv"), (700, "float4", "float4recv"),
                         (701, "float8", "float8recv"), (1043, "varchar", "varcharrecv"), (1082, "date", "date_recv"), (1114, "timestamp", "timestamp_recv"), (1700, "numeric", "numeric_recv")];
            let types = types.iter().map(|(o, n, r)| format!("({o}, '{n}', 0, 11, 'b', 0, '{r}', '{}', 0, 0)", r.replace("recv", "send"))).collect::<Vec<_>>();
            for view in [
                format!("pg_type AS SELECT * FROM (VALUES {}) AS t(oid, typname, typarray, typnamespace, typtype, typrelid, typreceive, typsend, typbasetype, typelem)", types.join(", ")),
                "pg_attribute AS SELECT * FROM (VALUES (0, '', 0, 0, false)) AS t(attrelid, attname, atttypid, attnum, attisdropped) WHERE attrelid > 0".to_string(),
                format!("pg_namespace AS SELECT * FROM (VALUES (11, 'pg_catalog'){namespaces}) AS t(oid, nspname)"),
                format!("pg_class AS SELECT * FROM (VALUES (0, '', 0, ''){}) AS t(oid, relname, relnamespace, relkind) WHERE oid > 0", classes.iter().map(|c| format!(", {c}")).collect::<String>()),
                format!("pg_database AS SELECT * FROM (VALUES (1, '{}')) AS t(oid, datname)", crate::ddl::lake_name(lake)),
            ] {
                ctx.sql(&format!("CREATE VIEW {view}")).await.map_err(|e| user_error(e.into()))?;
            }
        }
        Ok(ctx)
    }
}

/// psql, JDBC and SQLAlchemy ask for a few Postgres-only things on connect.
fn pg_dialect(lake: &crate::store::Lake, sql: &str) -> String {
    let sql = sql.trim().trim_end_matches(';').replace("pg_catalog.", "");
    let sql = if sql.eq_ignore_ascii_case("select version()") { "SELECT version() AS version".into() } else { sql }; // (the column's name in Postgres)
    // (the ADBC driver's list of types: receive functions are names here, not function ids)
    let sql = sql.replace("(typreceive != 0 OR typsend != 0)", "true").replace("typreceive::TEXT", "typreceive");
    sql.replace("current_schema()", "'public'").replace("CURRENT_SCHEMA()", "'public'").replace("current_database()", &format!("'{}'", crate::ddl::lake_name(lake))).replace("version()", "'PostgreSQL 16.0 (Pondra on Apache DataFusion)'")
}

/// Session settings and transactions: accepted (every statement commits on its own).
fn session_command(sql: &str) -> Option<Response> {
    let first = sql.split_whitespace().next()?.to_uppercase();
    let setting = |v: &str| {
        let field = Arc::new(vec![FieldInfo::new("setting".into(), None, None, Type::VARCHAR, FieldFormat::Text)]);
        let mut row = DataRowEncoder::new(field.clone());
        row.encode_field(&v).ok()?;
        Some(Response::Query(QueryResponse::new(field, stream::iter([Ok(row.take_row())]))))
    };
    match first.as_str() {
        "SET" | "RESET" | "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "DISCARD" | "DEALLOCATE" | "CLOSE" => Some(Response::Execution(Tag::new(&first))),
        "SHOW" if !sql.to_lowercase().contains("tables") => setting(match sql.to_lowercase() {
            s if s.contains("standard_conforming_strings") => "on",
            s if s.contains("transaction") => "read committed",
            s if s.contains("server_version") => "16.0",
            s if s.contains("encoding") => "UTF8",
            _ => "",
        }),
        _ => None,
    }
}

/// Result rows, each column as the Postgres type closest to its Arrow type — encoded a batch at a
/// time, as the client takes them.
fn rows(schema: &Schema, batches: Vec<RecordBatch>, format: &Format) -> PgWireResult<QueryResponse> {
    let (info, types) = fields(schema, &|i| format.format_for(i));
    let each = info.clone();
    let rows = batches.into_iter().flat_map(move |b| each_row(&b, &types, |cols, i| {
        let mut row = DataRowEncoder::new(each.clone());
        for c in cols {
            encode!(&mut row, c, i)?;
        }
        Ok(row.take_row())
    }));
    Ok(QueryResponse::new(info, stream::iter(rows)))
}

/// Columns as Postgres sees them, and the Arrow types their values are sent as.
fn fields(schema: &Schema, format: &dyn Fn(usize) -> FieldFormat) -> (Arc<Vec<FieldInfo>>, Vec<DataType>) {
    let types: Vec<(Type, DataType)> = schema.fields().iter().enumerate().map(|(i, f)| pg_type(f.data_type(), format(i))).collect();
    let info = schema.fields().iter().zip(&types).enumerate().map(|(i, (f, (t, _)))| FieldInfo::new(f.name().clone(), None, None, t.clone(), format(i))).collect();
    (Arc::new(info), types.into_iter().map(|(_, a)| a).collect())
}

/// `row(columns, i)` for each row of `b`, its columns cast to `types` first.
fn each_row<T>(b: &RecordBatch, types: &[DataType], mut row: impl FnMut(&[ArrayRef], usize) -> PgWireResult<T>) -> Vec<PgWireResult<T>> {
    match b.columns().iter().zip(types).map(|(c, t)| cast(c, t)).collect::<Result<Vec<_>, _>>() {
        Ok(cols) => (0..b.num_rows()).map(|i| row(&cols, i)).collect(),
        Err(e) => vec![Err(user_error(e.into()))],
    }
}

/// The Postgres type for an Arrow type, and the Arrow type its values are sent as.
fn pg_type(t: &DataType, format: FieldFormat) -> (Type, DataType) {
    match t {
        DataType::Boolean => (Type::BOOL, DataType::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => (Type::INT2, DataType::Int16),
        DataType::Int32 | DataType::UInt16 => (Type::INT4, DataType::Int32),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => (Type::INT8, DataType::Int64),
        DataType::Float32 => (Type::FLOAT4, DataType::Float32),
        DataType::Float64 => (Type::FLOAT8, DataType::Float64),
        DataType::Decimal128(..) => (Type::NUMERIC, t.clone()), // (`Numeric`: text or binary)
        DataType::Decimal256(..) if format == FieldFormat::Text => (Type::NUMERIC, DataType::Utf8),
        DataType::Decimal256(..) => (Type::FLOAT8, DataType::Float64),
        DataType::Date32 | DataType::Date64 => (Type::DATE, DataType::Date32),
        DataType::Timestamp(..) => (Type::TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None)),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => (Type::BYTEA, DataType::Binary),
        _ => (Type::VARCHAR, DataType::Utf8),
    }
}

/// Value `i` of column `c` into a row being encoded (a result row, or a COPY row).
macro_rules! encode {
    ($row:expr, $c:expr, $i:expr) => {{
        let (row, c, i) = ($row, $c, $i);
        match c.data_type() {
            _ if c.is_null(i) => row.encode_field(&None::<i32>),
            DataType::Boolean => row.encode_field(&c.as_boolean().value(i)),
            DataType::Int16 => row.encode_field(&c.as_primitive::<Int16Type>().value(i)),
            DataType::Int32 => row.encode_field(&c.as_primitive::<Int32Type>().value(i)),
            DataType::Int64 => row.encode_field(&c.as_primitive::<Int64Type>().value(i)),
            DataType::Float32 => row.encode_field(&c.as_primitive::<Float32Type>().value(i)),
            DataType::Float64 => row.encode_field(&c.as_primitive::<Float64Type>().value(i)),
            DataType::Date32 => row.encode_field(&c.as_primitive::<Date32Type>().value_as_date(i)),
            DataType::Timestamp(..) => row.encode_field(&c.as_primitive::<TimestampMicrosecondType>().value_as_datetime(i)),
            DataType::Binary => row.encode_field(&c.as_binary::<i32>().value(i)),
            DataType::Decimal128(..) => row.encode_field(&Numeric(c.as_primitive::<datafusion::arrow::datatypes::Decimal128Type>().value_as_string(i))),
            _ => row.encode_field(&c.as_string::<i32>().value(i)),
        }
    }};
}
use encode;

/// A DECIMAL as Postgres's NUMERIC: its text as it is, or the binary form (base-10000 digits
/// around the point) that binary results and `COPY … (FORMAT binary)` carry.
#[derive(Debug)]
struct Numeric(String);

impl postgres_types::ToSql for Numeric {
    fn to_sql(&self, _: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut;
        let (negative, digits) = match self.0.strip_prefix('-') {
            Some(d) => (true, d),
            None => (false, self.0.as_str()),
        };
        let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
        let int = int.trim_start_matches('0');
        let int = format!("{}{int}", "0".repeat((4 - int.len() % 4) % 4));
        let groups = |s: &str| s.as_bytes().chunks(4).map(|c| std::str::from_utf8(c).unwrap_or("0").parse::<i16>().unwrap_or(0)).collect::<Vec<_>>();
        let mut all = groups(&int);
        all.extend(groups(&format!("{frac}{}", "0".repeat((4 - frac.len() % 4) % 4))));
        let mut weight = (int.len() / 4) as i16 - 1;
        while all.first() == Some(&0) {
            all.remove(0);
            weight -= 1;
        }
        while all.last() == Some(&0) {
            all.pop();
        }
        out.put_i16(all.len() as i16);
        out.put_i16(if all.is_empty() { 0 } else { weight });
        out.put_u16(if negative && !all.is_empty() { 0x4000 } else { 0 });
        out.put_i16(frac.len() as i16);
        all.iter().for_each(|d| out.put_i16(*d));
        Ok(postgres_types::IsNull::No)
    }
    fn accepts(ty: &Type) -> bool { *ty == Type::NUMERIC }
    postgres_types::to_sql_checked!();
}

impl pgwire::types::ToSqlText for Numeric {
    fn to_sql_text(&self, _: &Type, out: &mut bytes::BytesMut, _: &pgwire::types::format::FormatOptions) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(postgres_types::IsNull::No)
    }
}

/// A `COPY` to or from the client, as it asked for it.
struct Copy {
    table: String,
    columns: Vec<String>,
    query: Option<String>, // COPY (query) TO STDOUT
    to: bool,
    format: String, // text, csv or binary
    header: bool,
    delimiter: Option<char>,
    null: Option<String>,
}

impl Copy {
    /// `sql` as a COPY to STDOUT or from STDIN (None: not a COPY).
    fn of(sql: &str) -> Option<PgWireResult<Copy>> {
        use datafusion::sql::sqlparser::ast::{CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, Statement};
        if !sql.trim_start().get(..4).is_some_and(|w| w.eq_ignore_ascii_case("copy")) {
            return None;
        }
        if matches!(crate::ext::statement(sql), Some(crate::write::Stmt::CopyTo(..))) {
            return None; // (`COPY … TO 's3://…'`: files anywhere, the statement every door runs: ADR-026)
        }
        let parsed = datafusion::sql::sqlparser::parser::Parser::parse_sql(&datafusion::sql::sqlparser::dialect::PostgreSqlDialect {}, sql);
        let Ok(Some(Statement::Copy { source, to, target, options, legacy_options, .. })) = parsed.map(|mut s| s.pop()) else { return Some(Err(user_error(anyhow::anyhow!("COPY: can't read {sql}")))) };
        if !matches!((to, &target), (true, CopyTarget::Stdout) | (false, CopyTarget::Stdin)) {
            return Some(Err(user_error(anyhow::anyhow!("COPY reads and writes the client's files: psql's \\copy, or COPY … TO STDOUT / FROM STDIN"))));
        }
        let mut c = Copy { table: String::new(), columns: vec![], query: None, to, format: "text".into(), header: false, delimiter: None, null: None };
        match source {
            CopySource::Table { table_name, columns } => (c.table, c.columns) = (table_name.to_string(), columns.into_iter().map(|i| i.value).collect()),
            CopySource::Query(q) => c.query = Some(q.to_string()),
        }
        for o in options {
            match o {
                CopyOption::Format(f) => c.format = f.value.to_lowercase(),
                CopyOption::Header(h) => c.header = h,
                CopyOption::Delimiter(d) => c.delimiter = Some(d),
                CopyOption::Null(n) => c.null = Some(n),
                _ => {}
            }
        }
        for o in legacy_options {
            match o {
                CopyLegacyOption::Binary => c.format = "binary".into(),
                CopyLegacyOption::Csv(opts) => (c.format, c.header) = ("csv".into(), c.header || opts.iter().any(|o| matches!(o, CopyLegacyCsvOption::Header))),
                CopyLegacyOption::Delimiter(d) => c.delimiter = Some(d),
                CopyLegacyOption::Null(n) => c.null = Some(n),
                _ => {}
            }
        }
        Some(Ok(c))
    }
}

/// A COPY FROM STDIN under way, per connection: what it loads, and what came that isn't loaded yet.
struct Pending {
    copy: Copy,
    data: Vec<u8>,
    rows: u64,
    seq: u64,
    job: String,
}

static COPIES: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<std::net::SocketAddr, Pending>>> = std::sync::LazyLock::new(Default::default);

#[async_trait]
impl CopyHandler for Backend {
    /// Gathered, and loaded 32 MB at a time.
    async fn on_copy_data<C>(&self, client: &mut C, data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut p = {
            let mut copies = COPIES.lock().unwrap();
            let Some(p) = copies.get_mut(&client.socket_addr()) else { return Err(user_error(anyhow::anyhow!("COPY data without a COPY"))) };
            p.data.extend_from_slice(&data.data);
            if p.data.len() < 32 << 20 {
                return Ok(());
            }
            copies.remove(&client.socket_addr()).expect("there")
        };
        let r = self.load(&mut p, false).await;
        COPIES.lock().unwrap().insert(client.socket_addr(), p);
        r
    }

    async fn on_copy_done<C>(&self, client: &mut C, _: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let Some(mut p) = COPIES.lock().unwrap().remove(&client.socket_addr()) else { return Err(user_error(anyhow::anyhow!("COPY done without a COPY"))) };
        self.load(&mut p, true).await?;
        client.send(PgWireBackendMessage::CommandComplete(Tag::new("COPY").with_rows(p.rows as usize).into())).await?;
        Ok(())
    }
}

/// `$1`, `$2`… replaced by the portal's values: numbers as they are, anything else quoted. A
/// parameter's type is the client's, or else the one the query implies (`inferred`).
fn bind(portal: &Portal<String>, inferred: &[Type]) -> PgWireResult<String> {
    let mut sql = portal.statement.statement.clone();
    for i in (0..portal.parameter_len()).rev() {
        let t = portal.statement.parameter_types.get(i).cloned().flatten().filter(|t| *t != Type::UNKNOWN);
        let t = t.or_else(|| inferred.get(i).cloned()).unwrap_or(Type::UNKNOWN);
        let v = match t {
            Type::BOOL => portal.parameter::<bool>(i, &t)?.map(|b| b.to_string()),
            Type::INT2 => portal.parameter::<i16>(i, &t)?.map(|v| v.to_string()),
            Type::INT4 => portal.parameter::<i32>(i, &t)?.map(|v| v.to_string()),
            Type::INT8 => portal.parameter::<i64>(i, &t)?.map(|v| v.to_string()),
            Type::FLOAT4 => portal.parameter::<f32>(i, &t)?.map(|v| v.to_string()),
            Type::FLOAT8 => portal.parameter::<f64>(i, &t)?.map(|v| v.to_string()),
            // arrays (embeddings for vector search): DataFusion's `[a, b, …]`
            Type::FLOAT4_ARRAY => list(portal.parameter::<Vec<Option<f32>>>(i, &t)?),
            Type::FLOAT8_ARRAY => list(portal.parameter::<Vec<Option<f64>>>(i, &t)?),
            Type::INT4_ARRAY => list(portal.parameter::<Vec<Option<i32>>>(i, &t)?),
            Type::INT8_ARRAY => list(portal.parameter::<Vec<Option<i64>>>(i, &t)?),
            _ => portal.parameter::<String>(i, &Type::VARCHAR)?.map(|s| match s.parse::<f64>() {
                Ok(_) if t == Type::UNKNOWN => s,
                _ => format!("'{}'", s.replace('\'', "''")),
            }),
        };
        sql = sql.replace(&format!("${}", i + 1), &v.unwrap_or_else(|| "NULL".into()));
    }
    Ok(sql)
}

fn list<T: ToString>(v: Option<Vec<Option<T>>>) -> Option<String> {
    let item = |x: &Option<T>| x.as_ref().map_or("NULL".to_string(), T::to_string);
    v.map(|v| format!("[{}]", v.iter().map(item).collect::<Vec<_>>().join(", ")))
}

#[async_trait]
impl SimpleQueryHandler for Backend {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let user = client.metadata().get("user").cloned().unwrap_or_default();
        let mut out = vec![];
        for q in crate::routines::split(query).iter().map(|q| q.trim()) {
            out.push(match Copy::of(q) {
                Some(Ok(c)) if !c.to => self.copy_in(client, &user, c).await?,
                _ => self.run(&user, q, &Format::UnifiedText).await?,
            });
        }
        Ok(out)
    }
}

#[async_trait]
impl ExtendedQueryHandler for Backend {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> { self.parser.clone() }

    async fn do_query<C>(&self, client: &mut C, portal: &Portal<Self::Statement>, _max_rows: usize) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let user = client.metadata().get("user").cloned().unwrap_or_default();
        if let Some(Ok(c)) = Copy::of(&portal.statement.statement).filter(|c| c.as_ref().is_ok_and(|c| !c.to)) {
            return self.copy_in(client, &user, c).await;
        }
        let inferred = self.param_types(&pg_dialect(&self.app.lake, &portal.statement.statement)).await;
        self.run(&user, &bind(portal, &inferred)?, &portal.result_column_format).await
    }

    async fn do_describe_statement<C>(&self, _client: &mut C, stmt: &StoredStatement<Self::Statement>) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let sql = pg_dialect(&self.app.lake, &stmt.statement);
        Ok(DescribeStatementResponse::new(self.param_types(&sql).await, self.describe(&sql, &Format::UnifiedText).await?))
    }

    async fn do_describe_portal<C>(&self, _client: &mut C, portal: &Portal<Self::Statement>) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let inferred = self.param_types(&pg_dialect(&self.app.lake, &portal.statement.statement)).await;
        Ok(DescribePortalResponse::new(self.describe(&pg_dialect(&self.app.lake, &bind(portal, &inferred)?), &portal.result_column_format).await?))
    }
}

impl Backend {
    /// The types of `$1`, `$2`… as the query implies them (`WHERE id = $1`: id's type); text when
    /// it doesn't say.
    async fn param_types(&self, sql: &str) -> Vec<Type> {
        let n = (1..).take_while(|i| sql.contains(&format!("${i}"))).count();
        let mut types = vec![Type::VARCHAR; n];
        if n > 0 && session_command(sql).is_none() && crate::write::parse(sql).is_none() && Copy::of(sql).is_none() {
            let plan = async { self.session(sql).await.ok()?.sql_with_options(&crate::asof::rewrite(sql).ok()?, read_only()).await.ok() }.await;
            for (name, t) in plan.and_then(|df| df.logical_plan().get_parameter_types().ok()).unwrap_or_default() {
                if let (Some(i @ 1..), Some(t)) = (name.trim_start_matches('$').parse::<usize>().ok(), t) {
                    if i <= n {
                        types[i - 1] = pg_type(&t, FieldFormat::Text).0;
                    }
                }
            }
        }
        types
    }

    /// The columns a statement returns (none for writes and session commands).
    async fn describe(&self, sql: &str, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
        if session_command(sql).is_some() || crate::write::parse(sql).is_some() || Copy::of(sql).is_some() {
            return Ok(vec![]); // (a COPY's columns come with its data)
        }
        let probe = (1..).take_while(|i| sql.contains(&format!("${i}"))).fold(sql.to_string(), |q, i| q.replace(&format!("${i}"), "NULL"));
        let schema = self.schema(&probe).await?;
        Ok(schema.fields().iter().enumerate().map(|(i, f)| FieldInfo::new(f.name().clone(), None, None, pg_type(f.data_type(), format.format_for(i)).0, format.format_for(i))).collect())
    }
}
