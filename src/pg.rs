//! The Postgres wire protocol (`--pg 0.0.0.0:5432`): psql, drivers (psycopg, JDBC, Go, Node,
//! SQLAlchemy and pandas) and BI tools talk to any node as they would to Postgres. Queries and
//! writes (CREATE TABLE, INSERT, UPDATE, DELETE) run exactly as `POST /sql` runs them. Results go
//! out as text or binary, whichever the client asks for; `$1` parameters are bound as literals.
//! With tokens set, the user name picks the role (`reader`, `writer`, `admin`) and the password
//! is that role's token.
use crate::query::read_only;
use crate::server::App;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Schema, TimeUnit, TimestampMicrosecondType};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{stream, Sink};
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
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
        let sql = crate::asof::rewrite(&pg_dialect(&self.app.lake, sql)).map_err(user_error)?.into_owned();
        if let Some(r) = session_command(&sql) {
            return Ok(r);
        }
        let role = self.app.auth.role_of_user(user);
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
        Ok(Response::Query(rows(&schema, &batches, format)?))
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
            let tables = lake.cat.scan::<crate::store::TableMeta>("t/", "t0").await.map_err(user_error)?.into_iter().map(|(k, _)| (k, 'r'));
            let views = lake.cat.scan::<crate::ddl::StoredView>("q/", "q0").await.map_err(user_error)?.into_iter().map(|(k, _)| (k, 'v'));
            let classes = tables.chain(views).enumerate().map(|(i, (k, kind))| format!("({}, '{}', {}, '{kind}')", 16384 + i, crate::ddl::split(&k[2..]).1, oid(crate::ddl::split(&k[2..]).0))).collect::<Vec<_>>();
            let namespaces = schemas.iter().map(|s| format!(", ({}, '{s}')", oid(s))).collect::<String>();
            let types = [(16, "bool"), (17, "bytea"), (20, "int8"), (21, "int2"), (23, "int4"), (25, "text"), (700, "float4"), (701, "float8"), (1043, "varchar"), (1082, "date"), (1114, "timestamp"), (1700, "numeric")];
            let types = types.iter().map(|(o, n)| format!("({o}, '{n}', 0, 11, 'b', 0)")).collect::<Vec<_>>();
            for view in [
                format!("pg_type AS SELECT * FROM (VALUES {}) AS t(oid, typname, typarray, typnamespace, typtype, typrelid)", types.join(", ")),
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
    sql.replace("current_schema()", "'public'").replace("current_database()", &format!("'{}'", crate::ddl::lake_name(lake))).replace("version()", "'PostgreSQL 16.0 (Pondra on Apache DataFusion)'")
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

/// Result rows, each column as the Postgres type closest to its Arrow type.
fn rows(schema: &Schema, batches: &[RecordBatch], format: &Format) -> PgWireResult<QueryResponse> {
    let err = |e: datafusion::arrow::error::ArrowError| user_error(e.into());
    let fields: Vec<(Type, DataType)> = schema.fields().iter().enumerate().map(|(i, f)| pg_type(f.data_type(), format.format_for(i))).collect();
    let info = Arc::new(schema.fields().iter().zip(&fields).enumerate().map(|(i, (f, (t, _)))| FieldInfo::new(f.name().clone(), None, None, t.clone(), format.format_for(i))).collect::<Vec<_>>());
    let mut out = vec![];
    for b in batches {
        let cols: Vec<ArrayRef> = b.columns().iter().zip(&fields).map(|(c, (_, as_))| cast(c, as_)).collect::<Result<_, _>>().map_err(err)?;
        for i in 0..b.num_rows() {
            let mut row = DataRowEncoder::new(info.clone());
            for c in &cols {
                encode(&mut row, c, i)?;
            }
            out.push(Ok(row.take_row()));
        }
    }
    Ok(QueryResponse::new(info, stream::iter(out)))
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
        DataType::Decimal128(..) | DataType::Decimal256(..) if format == FieldFormat::Text => (Type::NUMERIC, DataType::Utf8),
        DataType::Decimal128(..) | DataType::Decimal256(..) => (Type::FLOAT8, DataType::Float64),
        DataType::Date32 | DataType::Date64 => (Type::DATE, DataType::Date32),
        DataType::Timestamp(..) => (Type::TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None)),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => (Type::BYTEA, DataType::Binary),
        _ => (Type::VARCHAR, DataType::Utf8),
    }
}

fn encode(row: &mut DataRowEncoder, c: &ArrayRef, i: usize) -> PgWireResult<()> {
    if c.is_null(i) {
        return row.encode_field(&None::<i32>);
    }
    match c.data_type() {
        DataType::Boolean => row.encode_field(&c.as_boolean().value(i)),
        DataType::Int16 => row.encode_field(&c.as_primitive::<Int16Type>().value(i)),
        DataType::Int32 => row.encode_field(&c.as_primitive::<Int32Type>().value(i)),
        DataType::Int64 => row.encode_field(&c.as_primitive::<Int64Type>().value(i)),
        DataType::Float32 => row.encode_field(&c.as_primitive::<Float32Type>().value(i)),
        DataType::Float64 => row.encode_field(&c.as_primitive::<Float64Type>().value(i)),
        DataType::Date32 => row.encode_field(&c.as_primitive::<Date32Type>().value_as_date(i)),
        DataType::Timestamp(..) => row.encode_field(&c.as_primitive::<TimestampMicrosecondType>().value_as_datetime(i)),
        DataType::Binary => row.encode_field(&c.as_binary::<i32>().value(i)),
        _ => row.encode_field(&c.as_string::<i32>().value(i)),
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
        for q in query.split(';').map(str::trim).filter(|q| !q.is_empty()) {
            out.push(self.run(&user, q, &Format::UnifiedText).await?);
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
        if n > 0 && session_command(sql).is_none() && crate::write::parse(sql).is_none() {
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
        if session_command(sql).is_some() || crate::write::parse(sql).is_some() {
            return Ok(vec![]);
        }
        let probe = (1..).take_while(|i| sql.contains(&format!("${i}"))).fold(sql.to_string(), |q, i| q.replace(&format!("${i}"), "NULL"));
        let schema = self.schema(&probe).await?;
        Ok(schema.fields().iter().enumerate().map(|(i, f)| FieldInfo::new(f.name().clone(), None, None, pg_type(f.data_type(), format.format_for(i)).0, format.format_for(i))).collect())
    }
}
