//! The Postgres wire protocol (`--pg 0.0.0.0:5432`): psql, drivers (psycopg, JDBC, Go, Node,
//! SQLAlchemy and pandas) and BI tools talk to any node as they would to Postgres. Queries and
//! writes (CREATE TABLE, INSERT, UPDATE, DELETE) run exactly as `POST /sql` runs them. Results go
//! out as text or binary, whichever the client asks for, a batch at a time; `$1` parameters are
//! bound as literals. `COPY … TO STDOUT` sends a table or a query in text, CSV or binary (what the
//! ADBC Postgres driver reads results with), `COPY … FROM STDIN` loads text or CSV (psql's
//! `\copy`, psycopg's `cursor.copy`).
//! With tokens set or users made (`users.rs`), a client signs in with SCRAM-SHA-256, as Postgres's
//! own: a user with its password (or one of its tokens), or `reader`, `writer`, `admin` with that
//! role's token. Each statement runs as whoever signed in (`auth::WHO`).
use crate::query::read_only;
use crate::server::App;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Schema, TimeUnit, TimestampMicrosecondType};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{stream, Sink, SinkExt};
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
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
    let tls = crate::tls::pg(); // (sslmode=require: `tls.rs`)
    let parser = Arc::new(NoopQueryParser::new());
    loop {
        let (socket, _) = listener.accept().await?;
        let tls = tls.clone();
        // A connection is a session: its temporary tables end with it (`temp.rs`).
        let session = format!("pg-{}", uuid::Uuid::new_v4().simple());
        let backend = Arc::new(Backend { app: app.clone(), parser: parser.clone(), session: session.clone(), who: Default::default() });
        let pg = Arc::new(Pg(backend.clone(), Arc::new(Startup { backend, scram: Default::default(), plain: Default::default() })));
        tokio::spawn(async move {
            let _ = socket.set_nodelay(true);
            let _ = pgwire::tokio::process_socket(socket, tls, pg).await;
            crate::temp::end(&session);
        });
    }
}

struct Pg(Arc<Backend>, Arc<Startup>);

struct Backend {
    app: App,
    parser: Arc<NoopQueryParser>,
    session: String,
    who: std::sync::Mutex<Option<crate::auth::Principal>>, // (whoever signed in)
}

impl PgWireServerHandlers for Pg {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> { self.0.clone() }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> { self.0.clone() }
    fn copy_handler(&self) -> Arc<impl CopyHandler> { self.0.clone() }
    fn startup_handler(&self) -> Arc<impl StartupHandler> { self.1.clone() }
}

/// Signing in: SCRAM-SHA-256 against the user's verifier (`users::Scram`), or a role's token; with
/// nothing that needs a sign-in, straight in as an admin.
struct Startup {
    backend: Arc<Backend>,
    scram: tokio::sync::Mutex<Option<(crate::users::Scram, String)>>, // (the exchange so far, and whose)
    plain: tokio::sync::Mutex<bool>, // (a token asked for as a plain password)
}

fn params() -> DefaultServerParameterProvider {
    let mut params = DefaultServerParameterProvider::default();
    params.server_version = "16.0 (Pondra)".into();
    params
}

#[async_trait]
impl StartupHandler for Startup {
    async fn on_startup<C>(&self, client: &mut C, message: PgWireFrontendMessage) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        use pgwire::messages::startup::Authentication;
        let app = &self.backend.app;
        let from = Some(client.socket_addr());
        let wrong = |user: &str| {
            crate::audit::refused(app, user, "postgres", from, "sign in", "wrong user name or password");
            PgWireError::InvalidPassword(user.to_string())
        };
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                if !crate::tls::pg_allowed(client.is_secure(), client.socket_addr()) {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new("FATAL".into(), "28000".into(), crate::tls::PLAIN.into()))));
                }
                pgwire::api::auth::protocol_negotiation(client, startup).await?;
                pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                if app.open().await {
                    *self.backend.who.lock().unwrap() = Some(crate::auth::Principal::of(crate::auth::Role::Admin).at("postgres", Some(client.socket_addr())));
                    return finish(client).await;
                }
                client.set_state(pgwire::api::PgWireConnectionState::AuthenticationInProgress);
                // (a user with only tokens, a service's: its token as a password, asked for plainly)
                let user = client.metadata().get("user").cloned().unwrap_or_default();
                let tokens_only = !crate::users::BUILT_IN.contains(&user.as_str()) && crate::users::verifier(&app.lake, &user).await.is_none() && crate::users::principal(&app.lake, &user).await.is_ok();
                *self.plain.lock().await = tokens_only;
                client.send(PgWireBackendMessage::Authentication(if tokens_only { Authentication::CleartextPassword } else { Authentication::SASL(vec!["SCRAM-SHA-256".into()]) })).await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(m) if *self.plain.lock().await => {
                let user = client.metadata().get("user").cloned().unwrap_or_default();
                let password = m.into_password()?.password;
                let Some(who) = crate::users::sign_in(&app.lake, &app.auth, &user, &password).await else {
                    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                    return Err(wrong(&user));
                };
                *self.backend.who.lock().unwrap() = Some(who.at("postgres", Some(client.socket_addr())));
                return finish(client).await;
            }
            PgWireFrontendMessage::PasswordMessageFamily(m) => {
                let user = client.metadata().get("user").cloned().unwrap_or_default();
                let mut scram = self.scram.lock().await;
                match scram.take() {
                    None => {
                        let first = m.into_sasl_initial_response()?;
                        let text = String::from_utf8_lossy(first.data.as_deref().unwrap_or_default()).to_string();
                        // (a token's role: its token as the password; a user: its verifier; nobody: one no password fits)
                        let verifier = match crate::users::BUILT_IN.contains(&user.as_str()) {
                            true => app.auth.token_for(&user).filter(|t| !t.is_empty()).map(|t| crate::users::Verifier::of(&t)),
                            false => crate::users::verifier(&app.lake, &user).await,
                        };
                        let (exchange, reply) = crate::users::Scram::first(verifier.unwrap_or_else(|| crate::users::Verifier::of(&uuid::Uuid::new_v4().to_string())), &text).map_err(|_| wrong(&user))?;
                        *scram = Some((exchange, user));
                        client.send(PgWireBackendMessage::Authentication(Authentication::SASLContinue(reply.into_bytes().into()))).await?;
                    }
                    Some((exchange, user)) => {
                        let last = m.into_sasl_response()?;
                        let text = String::from_utf8_lossy(&last.data).to_string();
                        let Ok(reply) = exchange.last(&text) else {
                            tokio::time::sleep(std::time::Duration::from_millis(400)).await; // (a guess costs time)
                            return Err(wrong(&user));
                        };
                        let who = match crate::users::BUILT_IN.contains(&user.as_str()) {
                            true => Some(crate::auth::Principal::of(app.auth.role_of_user(&user))),
                            false => crate::users::principal(&app.lake, &user).await.ok(),
                        };
                        let Some(who) = who else { return Err(wrong(&user)) };
                        *self.backend.who.lock().unwrap() = Some(who.at("postgres", Some(client.socket_addr())));
                        client.send(PgWireBackendMessage::Authentication(Authentication::SASLFinal(reply.into_bytes().into()))).await?;
                        return finish(client).await;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Signed in: Postgres's parameters, a key to cancel with, ready for queries.
async fn finish<C>(client: &mut C) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    use pgwire::api::PidSecretKeyGenerator;
    let (pid, key) = pgwire::api::RandomPidSecretKeyGenerator::default().generate(client);
    client.set_pid_and_secret_key(pid, key);
    pgwire::api::auth::finish_authentication(client, &params()).await
}

impl Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str("App") }
}

fn user_error(e: anyhow::Error) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), crate::codes::of(&e).into(), crate::ext::said(&e))))
}

impl Backend {
    /// Run one statement the way `POST /sql` does, for a client whose role comes from its user name.
    async fn run(&self, user: &str, sql: &str, format: &Format) -> PgWireResult<Response> {
        let reader = crate::auth::current().is_some_and(|p| p.role >= crate::auth::Role::Read);
        if reader && crate::txn::open() && crate::txn::refuse().is_ok() {
            if let Some(b) = crate::txn::point_read(&self.app.lake, sql).await.map_err(user_error)? {
                let schema = b.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(Schema::empty()));
                return Ok(Response::Query(rows(&schema, b, format)?)); // (in a transaction: its own version, or its snapshot's)
            }
        }
        if reader && crate::auth::limited().is_none() && !crate::txn::open() && !crate::temp::mentioned(sql) {
            // A key lookup: the serving path, before anything else (ADR-036 §6), as `/lookup` answers it.
            let t0 = std::time::Instant::now();
            if let Some(p) = crate::serve::point(&self.app.lake, sql.trim().trim_end_matches(';')).await.map_err(user_error)? {
                let t1 = t0.elapsed();
                if let Some(b) = p.rows(&self.app.lake).await.map_err(user_error)? {
                    if trace() {
                        eprintln!("pg point: parse {} µs, row {} µs", t1.as_micros(), (t0.elapsed() - t1).as_micros());
                    }
                    return Ok(Response::Query(rows(&b.schema(), vec![b], format)?));
                }
            }
        }
        let sql = crate::routines::expand(&self.app.lake, sql).await.map_err(user_error)?; // (macros: ADR-023)
        let sql = crate::asof::rewrite(&pg_dialect(&self.app.lake, &sql, user)).map_err(user_error)?.into_owned();
        if std::env::var_os("PONDRA_DEBUG_PG").is_some() {
            eprintln!("pg {user}: {sql}");
        }
        if let Some(word) = crate::txn::control(&sql) {
            // BEGIN, COMMIT, ROLLBACK: this connection's transaction (ADR-036 §5)
            let (tag, warning) = crate::txn::command(&self.app, word).await.map_err(user_error)?;
            warning.iter().for_each(|w| crate::routines::heard(&format!("WARNING: {w}")));
            return Ok(match tag {
                "BEGIN" => Response::TransactionStart(Tag::new(tag)), // (the client sees it: ReadyForQuery's status)
                _ => Response::TransactionEnd(Tag::new(tag)),
            });
        }
        crate::txn::refuse().map_err(user_error)?; // (a failed transaction takes nothing but its end)
        if let Some(r) = session_command(&sql) {
            return Ok(r);
        }
        if let Some(copy) = Copy::of(&sql) {
            return self.copy_out(copy?).await;
        }
        let role = crate::auth::current().map_or(crate::auth::Role::None, |p| p.role);
        if role < crate::auth::Role::Read {
            return Err(user_error(anyhow::anyhow!("sign in first")));
        }
        if crate::routines::runs_procedure(&sql) {
            let who = crate::routines::Who { role, files: false, depth: 0 };
            return match crate::routines::one(&self.app, &sql, who, None).await.map_err(user_error)? {
                crate::routines::Outcome::Rows(batches) => {
                    let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
                    Ok(Response::Query(rows(&schema, batches, format)?))
                }
                crate::routines::Outcome::Done(_) => Ok(Response::Execution(Tag::new(if crate::routines::do_of(&sql).is_some() { "DO" } else { "CALL" }))),
            };
        }
        if let Some(stmt) = crate::write::parse(&sql) {
            self.app.auth.allows(role, &stmt).map_err(user_error)?;
            let tag = command_tag(&sql, &stmt);
            let v = crate::write::on_node(&self.app, stmt, None).await.map_err(user_error)?;
            let rows = v["rows"].as_u64().unwrap_or(0) as usize;
            return Ok(Response::Execution(match tag.as_str() {
                "INSERT" => Tag::new("INSERT").with_oid(0).with_rows(rows),
                "SELECT" | "UPDATE" | "DELETE" | "MERGE" | "COPY" => Tag::new(&tag).with_rows(rows),
                _ => Tag::new(&tag),
            }));
        }
        let catalog = crate::pg_catalog::wanted(&sql);
        let batches = match catalog {
            true => match async { self.session(&sql, user).await?.sql_with_options(&sql, read_only()).await.map_err(|e| user_error(e.into()))?.collect().await.map_err(|e| user_error(e.into())) }.await {
                Ok(b) => b,
                // (over catalog tables that are always empty: no rows, whatever DataFusion made of it)
                Err(e) => match crate::pg_catalog::empty_answer(&sql) {
                    Some(names) => {
                        let schema = Schema::new(names.into_iter().map(|n| datafusion::arrow::datatypes::Field::new(n, DataType::Utf8, true)).collect::<Vec<_>>());
                        return Ok(Response::Query(rows(&schema, vec![], format)?));
                    }
                    None => return Err(e),
                },
            },
            false => self.app.query(&sql, None).await.map_err(user_error)?,
        };
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => self.schema(&sql, user).await?,
        };
        Ok(Response::Query(rows(&schema, batches, format)?))
    }

    /// `run`, the notices its procedures send (what they print) sent first: psql shows NOTICE.
    async fn told<C: Sink<PgWireBackendMessage> + Unpin + Send>(&self, client: &mut C, user: &str, sql: &str, format: &Format) -> PgWireResult<Response> {
        let t0 = std::time::Instant::now();
        let (out, heard) = crate::routines::with_notices(crate::temp::SESSION.scope(Some(self.session.clone()), crate::auth::WHO.scope(self.who(), self.caught(user, sql, format)))).await;
        for n in heard {
            let _ = client.send(PgWireBackendMessage::NoticeResponse(ErrorInfo::new("NOTICE".into(), "00000".into(), n).into())).await; // (a client gone: the answer fails too)
        }
        if trace() {
            eprintln!("pg told: {} µs: {}", t0.elapsed().as_micros(), sql.chars().take(60).collect::<String>());
        }
        out
    }

    /// `run`, a panic in it answered as an error (`panics.rs`); in a transaction, an error fails it.
    async fn caught(&self, user: &str, sql: &str, format: &Format) -> PgWireResult<Response> {
        let run = async { crate::panics::door(self.run(user, sql, format)).await.unwrap_or_else(|m| Err(user_error(anyhow::anyhow!(m)))) };
        let out = crate::audit::statement(&self.app, sql, run).await;
        if let Err(e) = &out {
            if crate::txn::control(sql).is_none() {
                crate::txn::failed(&e.to_string());
            }
        }
        out
    }

    /// Whoever signed in on this connection (nobody, before).
    fn who(&self) -> crate::auth::Principal { self.who.lock().unwrap().clone().unwrap_or_else(|| crate::auth::Principal::of(crate::auth::Role::None)) }

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
            None => self.schema(&sql, "").await?,
        };
        let format = if c.format == "binary" { FieldFormat::Binary } else { FieldFormat::Text };
        let (info, types) = fields(&schema, &|_| format);
        let mut enc = match c.format.as_str() {
            "binary" => CopyEncoder::new_binary(info.clone()),
            "csv" => CopyEncoder::new_csv(info.clone(), CopyCsvOptions { delimiter: c.delimiter.unwrap_or(',').to_string().as_str().into(), null_string: c.null.clone().unwrap_or_default().as_str().into(), ..Default::default() }),
            _ => CopyEncoder::new_text(info.clone(), CopyTextOptions { delimiter: c.delimiter.unwrap_or('\t').to_string().as_str().into(), null_string: c.null.clone().unwrap_or("\\N".into()).as_str().into() }),
        };
        let mut header = (c.header && c.format == "csv").then(|| {
            let names = schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(&c.delimiter.unwrap_or(',').to_string());
            bytes::Bytes::from(names + "\n")
        });
        let mut rows = batches.into_iter().flat_map(move |b| each_row(&b, &types, |cols, i| {
            for col in cols {
                encode!(&mut enc, col, i)?;
            }
            Ok(enc.take_copy())
        }));
        // The header goes in with the first row: COPY's count is of messages, and it isn't a row.
        let first = match (header.take(), rows.next()) {
            (Some(h), Some(Ok(row))) => Some(Ok(CopyData::new([h, row.data].concat().into()))),
            (Some(h), None) => Some(Ok(CopyData::new(h))),
            (_, row) => row,
        };
        let data = stream::iter(first.into_iter().chain(rows));
        Ok(Response::CopyOut(CopyResponse::new(if c.format == "binary" { 1 } else { 0 }, schema.fields().len(), data)))
    }

    /// `COPY t FROM STDIN`: what is to come, noted on the connection (`on_copy_data` gathers it).
    async fn copy_in<C: ClientInfo>(&self, client: &mut C, _user: &str, c: Copy) -> PgWireResult<Response> {
        let refuse = |why: &str| Err(user_error(anyhow::anyhow!("{why}")));
        if c.query.is_some() || c.format == "binary" {
            return refuse("COPY FROM STDIN takes a table, in text or CSV");
        }
        let stmt = crate::write::parse(&format!("INSERT INTO {} SELECT 1", c.table)).ok_or_else(|| user_error(anyhow::anyhow!("COPY: no table {}", c.table)))?;
        self.app.auth.allows(crate::auth::current().map_or(crate::auth::Role::None, |p| p.role), &stmt).map_err(user_error)?;
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
            // the table's columns in its order, those not given their DEFAULT, or null
            let cols = table.fields().iter().map(|f| b.column_by_name(f.name()).cloned().unwrap_or_else(|| datafusion::arrow::array::new_null_array(f.data_type(), b.num_rows()))).collect();
            let b = RecordBatch::try_new(table.clone(), cols).map_err(|e| err(e.into()))?;
            let b = crate::defaults::fill(&meta, b.clone(), |c| (!given.iter().any(|g| g == c)).then(|| crate::defaults::all(b.num_rows()))).await.map_err(err)?;
            p.rows += b.num_rows() as u64;
            p.seq += 1;
            let src = crate::log::Src { producer: format!("copy:{}", p.job), seq: p.seq, prev: None };
            self.app.log().map_err(err)?.append(p.copy.table.clone(), src, b).await.map_err(err)?;
        }
        Ok(())
    }

    /// A query's result columns, without running it.
    async fn schema(&self, sql: &str, user: &str) -> PgWireResult<Arc<Schema>> {
        let sql = crate::asof::rewrite(sql).map_err(user_error)?;
        let df = self.session(&sql, user).await?.sql_with_options(&sql, read_only()).await.map_err(|e| user_error(e.into()))?;
        Ok(Arc::new(df.schema().as_arrow().clone()))
    }

    /// A session for `sql`; with Postgres's catalog, its functions and its `information_schema`
    /// when it reads them (`pg_catalog.rs`).
    async fn session(&self, sql: &str, user: &str) -> PgWireResult<datafusion::prelude::SessionContext> {
        let ctx = crate::query::session(&self.app.lake, sql, "").await.map_err(user_error)?;
        if crate::pg_catalog::wanted(sql) {
            crate::pg_catalog::register(&ctx, &self.app.lake, if user.is_empty() { "pondra" } else { user }, sql).await.map_err(user_error)?;
        }
        Ok(ctx)
    }
}

/// psql, JDBC and SQLAlchemy ask for a few Postgres-only things on connect.
fn pg_dialect(lake: &crate::store::Lake, sql: &str, user: &str) -> String {
    let sql = sql.trim().trim_end_matches(';');
    let sql = match crate::pg_catalog::wanted(sql) {
        true => crate::pg_catalog::rewrite(sql, user),
        false => sql.to_string(),
    };
    let sql = if sql.eq_ignore_ascii_case("select version()") { "SELECT version() AS version".into() } else { sql }; // (the column's name in Postgres)
    // (the ADBC driver's list of types: receive functions are names here, not function ids)
    let sql = sql.replace("(typreceive != 0 OR typsend != 0)", "true").replace("typreceive::TEXT", "typreceive");
    sql.replace("current_schema()", "'public'").replace("CURRENT_SCHEMA()", "'public'").replace("current_database()", &format!("'{}'", crate::ddl::lake_name(lake))).replace("version()", "'PostgreSQL 16.0 (Pondra on Apache DataFusion)'")
}

/// The command tag Postgres answers a write with: `INSERT 0 3`, `UPDATE 2`, `SELECT 5` for a
/// CREATE TABLE … AS, `CREATE VIEW`, `DROP TABLE`, `ALTER TABLE` (what clients show, and dbt logs).
fn command_tag(sql: &str, stmt: &crate::write::Stmt) -> String {
    use crate::write::Stmt;
    let words: Vec<String> = crate::write::first_word(sql).split_whitespace().take(6).map(|w| w.to_uppercase()).collect();
    match stmt {
        Stmt::Insert(..) | Stmt::InsertInto(..) => return "INSERT".into(),
        Stmt::Update(..) => return "UPDATE".into(),
        Stmt::Delete(..) if words.first().is_some_and(|w| w == "TRUNCATE") => return "TRUNCATE TABLE".into(),
        Stmt::Delete(..) => return "DELETE".into(),
        Stmt::Merge(_) if words.first().is_some_and(|w| w == "MERGE") => return "MERGE".into(),
        Stmt::Merge(_) if words.first().is_some_and(|w| w == "INSERT") => return "INSERT".into(),
        Stmt::Merge(_) => return words.first().cloned().unwrap_or_default(),
        Stmt::Create(c) if c.query.is_some() => return "SELECT".into(),
        _ => {}
    }
    let skip = ["OR", "REPLACE", "TEMP", "TEMPORARY", "UNLOGGED", "GLOBAL", "LOCAL", "IF", "NOT", "EXISTS"];
    match words.first().map(String::as_str) {
        Some(verb @ ("CREATE" | "DROP" | "ALTER")) => {
            let mut rest = words[1..].iter().filter(|w| !skip.contains(&w.as_str()));
            match rest.next().map(String::as_str) {
                Some("MATERIALIZED") => format!("{verb} MATERIALIZED VIEW"),
                Some(obj) => format!("{verb} {obj}"),
                None => verb.to_string(),
            }
        }
        Some(w) => w.to_string(),
        None => "OK".into(),
    }
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
        DataType::UInt32 => (Type::OID, DataType::UInt32), // (the catalog's oids: `pg_catalog.rs`)
        DataType::Int64 | DataType::UInt64 => (Type::INT8, DataType::Int64),
        DataType::Float32 => (Type::FLOAT4, DataType::Float32),
        DataType::Float64 => (Type::FLOAT8, DataType::Float64),
        DataType::Decimal128(..) => (Type::NUMERIC, t.clone()), // (`Numeric`: text or binary)
        DataType::Decimal256(..) if format == FieldFormat::Text => (Type::NUMERIC, DataType::Utf8),
        DataType::Decimal256(..) => (Type::FLOAT8, DataType::Float64),
        DataType::Date32 | DataType::Date64 => (Type::DATE, DataType::Date32),
        DataType::Timestamp(_, Some(_)) => (Type::TIMESTAMPTZ, DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))), // (an instant: sent in UTC)
        DataType::Timestamp(..) => (Type::TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None)),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => (Type::BYTEA, DataType::Binary),
        // Lists as Postgres's arrays (drivers give them as lists); of other things, as text.
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            let of = |t: DataType| DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new("item", t, true)));
            match f.data_type() {
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => (Type::VARCHAR_ARRAY, of(DataType::Utf8)),
                DataType::Int8 | DataType::Int16 | DataType::UInt8 => (Type::INT2_ARRAY, of(DataType::Int16)),
                DataType::Int32 | DataType::UInt16 => (Type::INT4_ARRAY, of(DataType::Int32)),
                DataType::Int64 | DataType::UInt64 => (Type::INT8_ARRAY, of(DataType::Int64)),
                DataType::UInt32 => (Type::OID_ARRAY, of(DataType::UInt32)),
                DataType::Float16 | DataType::Float32 => (Type::FLOAT4_ARRAY, of(DataType::Float32)),
                DataType::Float64 => (Type::FLOAT8_ARRAY, of(DataType::Float64)),
                DataType::Boolean => (Type::BOOL_ARRAY, of(DataType::Boolean)),
                _ => (Type::VARCHAR, DataType::Utf8),
            }
        }
        _ => (Type::VARCHAR, DataType::Utf8),
    }
}

/// Item `i` of a list column, as a Vec of its element type (an array's value).
fn items<T: datafusion::arrow::datatypes::ArrowPrimitiveType>(c: &ArrayRef, i: usize) -> Vec<Option<T::Native>> {
    let v = c.as_list::<i32>().value(i);
    v.as_primitive::<T>().iter().collect()
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
            DataType::UInt32 => row.encode_field(&c.as_primitive::<datafusion::arrow::datatypes::UInt32Type>().value(i)),
            DataType::Float32 => row.encode_field(&c.as_primitive::<Float32Type>().value(i)),
            DataType::Float64 => row.encode_field(&c.as_primitive::<Float64Type>().value(i)),
            DataType::Date32 => row.encode_field(&c.as_primitive::<Date32Type>().value_as_date(i)),
            DataType::Timestamp(_, Some(_)) => row.encode_field(&c.as_primitive::<TimestampMicrosecondType>().value_as_datetime(i).map(|t| t.and_utc())),
            DataType::Timestamp(..) => row.encode_field(&c.as_primitive::<TimestampMicrosecondType>().value_as_datetime(i)),
            DataType::Binary => row.encode_field(&c.as_binary::<i32>().value(i)),
            DataType::List(f) => match f.data_type() {
                DataType::Utf8 => row.encode_field(&c.as_list::<i32>().value(i).as_string::<i32>().iter().map(|v| v.map(str::to_string)).collect::<Vec<_>>()),
                DataType::Int16 => row.encode_field(&items::<Int16Type>(c, i)),
                DataType::Int32 => row.encode_field(&items::<Int32Type>(c, i)),
                DataType::Int64 => row.encode_field(&items::<Int64Type>(c, i)),
                DataType::UInt32 => row.encode_field(&items::<datafusion::arrow::datatypes::UInt32Type>(c, i)),
                DataType::Float32 => row.encode_field(&items::<Float32Type>(c, i)),
                DataType::Float64 => row.encode_field(&items::<Float64Type>(c, i)),
                DataType::Boolean => row.encode_field(&c.as_list::<i32>().value(i).as_boolean().iter().collect::<Vec<_>>()),
                _ => row.encode_field(&None::<i32>),
            },
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
        if !crate::write::first_word(sql).get(..4).is_some_and(|w| w.eq_ignore_ascii_case("copy")) {
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
                Some(Ok(c)) if !c.to => crate::auth::WHO.scope(self.who(), self.copy_in(client, &user, c)).await?,
                _ => self.told(client, &user, q, &Format::UnifiedText).await?,
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
            return crate::auth::WHO.scope(self.who(), self.copy_in(client, &user, c)).await;
        }
        let inferred = crate::temp::SESSION.scope(Some(self.session.clone()), self.param_types(&pg_dialect(&self.app.lake, &portal.statement.statement, ""))).await;
        self.told(client, &user, &bind(portal, &inferred)?, &portal.result_column_format).await
    }

    async fn do_describe_statement<C>(&self, _client: &mut C, stmt: &StoredStatement<Self::Statement>) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let sql = pg_dialect(&self.app.lake, &stmt.statement, "");
        crate::temp::SESSION.scope(Some(self.session.clone()), async { Ok(DescribeStatementResponse::new(self.param_types(&sql).await, self.describe(&sql, &Format::UnifiedText).await?)) }).await // (its temporary tables too)
    }

    async fn do_describe_portal<C>(&self, _client: &mut C, portal: &Portal<Self::Statement>) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        crate::temp::SESSION.scope(Some(self.session.clone()), async {
            let inferred = self.param_types(&pg_dialect(&self.app.lake, &portal.statement.statement, "")).await;
            Ok(DescribePortalResponse::new(self.describe(&pg_dialect(&self.app.lake, &bind(portal, &inferred)?, ""), &portal.result_column_format).await?))
        })
        .await
    }
}

impl Backend {
    /// The types of `$1`, `$2`… as the query implies them (`WHERE id = $1`: id's type); text when
    /// it doesn't say.
    async fn param_types(&self, sql: &str) -> Vec<Type> {
        if let (None, Ok(Some(p))) = (crate::auth::limited(), crate::serve::point(&self.app.lake, sql).await) {
            return p.parameters(sql).iter().map(|t| pg_type(t, FieldFormat::Text).0).collect(); // (a key lookup's: its key's types, unplanned)
        }
        let n = (1..).take_while(|i| sql.contains(&format!("${i}"))).count();
        let mut types = vec![Type::VARCHAR; n];
        if n > 0 && session_command(sql).is_none() && crate::write::parse(sql).is_none() && Copy::of(sql).is_none() {
            let plan = async { self.session(sql, "").await.ok()?.sql_with_options(&crate::asof::rewrite(sql).ok()?, read_only()).await.ok() }.await;
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
        let point = match crate::auth::limited() {
            None => crate::serve::point(&self.app.lake, sql).await.ok().flatten().and_then(|p| p.schema().ok()), // (a key lookup's columns, unplanned, first)
            Some(_) => None,
        };
        if point.is_none() && (session_command(sql).is_some() || crate::write::parse(sql).is_some() || Copy::of(sql).is_some()) {
            return Ok(vec![]); // (a COPY's columns come with its data)
        }
        let schema = match point {
            Some(s) => s,
            None => {
                let probe = (1..).take_while(|i| sql.contains(&format!("${i}"))).fold(sql.to_string(), |q, i| q.replace(&format!("${i}"), "NULL"));
                self.schema(&probe, "").await?
            }
        };
        Ok(schema.fields().iter().enumerate().map(|(i, f)| FieldInfo::new(f.name().clone(), None, None, pg_type(f.data_type(), format.format_for(i)).0, format.format_for(i))).collect())
    }
}

/// `PONDRA_TRACE_PG=1`: where a Postgres statement's time goes, on stderr.
fn trace() -> bool {
    static ON: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| std::env::var_os("PONDRA_TRACE_PG").is_some());
    *ON
}

