//! The Postgres wire protocol (`--pg 0.0.0.0:5432`): psql, drivers (psycopg, JDBC, Go, Node,
//! SQLAlchemy and pandas) and BI tools talk to any node as they would to Postgres. Queries and
//! writes (CREATE TABLE, INSERT, UPDATE, DELETE) run exactly as `POST /sql` runs them. Results go
//! out as text or binary, whichever the client asks for, a batch at a time; `$1` parameters are
//! bound as literals. `COPY … TO STDOUT` sends a table or a query in text, CSV or binary (what the
//! ADBC Postgres driver reads results with), `COPY … FROM STDIN` loads text or CSV (psql's
//! `\copy`, psycopg's `cursor.copy`).
//! With tokens set or users made (`users.rs`), a client signs in with SCRAM-SHA-256, as Postgres's
//! own: a user with its password (or one of its tokens), or `reader`, `writer`, `admin` with that
//! role's token. A user with no password sends an identity provider's token as its password, or, when
//! the provider says `POSTGRES 'oauth'`, through OAUTHBEARER, which psql 18 fetches itself (ADR-074).
//! Each statement runs as whoever signed in (`auth::WHO`).
use crate::pg_types::Ty;
use crate::server::App;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, IntervalMonthDayNanoType, IntervalUnit, Schema, Time64MicrosecondType, TimeUnit, TimestampMicrosecondType};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{stream, Sink, SinkExt};
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::copy::CopyHandler;
use pgwire::api::results::{CopyCsvOptions, CopyEncoder, CopyResponse, CopyTextOptions, DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, ConnectionManager, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone};
use pgwire::messages::extendedquery::{Describe, Execute, TARGET_TYPE_BYTE_PORTAL};
use pgwire::messages::simplequery::Query;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use std::fmt::Debug;
use std::sync::Arc;

/// Accept Postgres clients on `addr`.
pub async fn serve(app: App, addr: String) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let tls = crate::tls::pg(); // (sslmode=require: `tls.rs`)
    let parser = Arc::new(Parser);
    // One registry of connections for the server: a cancel request finds its connection's key here.
    let cancels = Arc::new(ConnectionManager::new());
    let cancel = Arc::new(DefaultCancelHandler::new(cancels.clone()));
    loop {
        let (socket, _) = listener.accept().await?;
        if crate::drain::draining() {
            continue; // (stopping: a new connection goes to another node, `drain.rs`)
        }
        let tls = tls.clone();
        // A connection is a session: its temporary tables end with it (`temp.rs`).
        let session = format!("pg-{}", uuid::Uuid::new_v4().simple());
        let backend = Arc::new_cyclic(|me| Backend { app: app.clone(), parser: parser.clone(), session: session.clone(), who: Default::default(), me: me.clone() });
        let startup = Startup { backend: backend.clone(), scram: Default::default(), plain: Default::default(), oauth: Default::default(), cancels: cancels.clone() };
        let pg = Arc::new(Pg(backend, Arc::new(startup), cancel.clone()));
        crate::panics::task(async move {
            let _ = socket.set_nodelay(true);
            let _ = pgwire::tokio::process_socket(socket, tls, pg).await;
            crate::temp::end(&session);
        });
    }
}

/// A connection's handlers: the statements' (`Backend`), the sign-in (`Startup`), and the cancel
/// requests' (one for the server: `serve`).
struct Pg(Arc<Backend>, Arc<Startup>, Arc<DefaultCancelHandler>);

struct Backend {
    app: App,
    parser: Arc<Parser>,
    session: String,
    who: std::sync::Mutex<Option<crate::auth::Principal>>, // (whoever signed in)
    me: std::sync::Weak<Backend>,                          // (a statement runs on the queries' runtime: `told`)
}

impl PgWireServerHandlers for Pg {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> { self.0.clone() }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> { self.0.clone() }
    fn copy_handler(&self) -> Arc<impl CopyHandler> { self.0.clone() }
    fn startup_handler(&self) -> Arc<impl StartupHandler> { self.1.clone() }
    fn cancel_handler(&self) -> Arc<impl CancelHandler> { self.2.clone() }
}

/// Signing in: SCRAM-SHA-256 against the user's verifier (`users::Scram`), or a role's token; with
/// nothing that needs a sign-in, straight in as an admin.
struct Startup {
    backend: Arc<Backend>,
    scram: tokio::sync::Mutex<Option<(crate::users::Scram, String)>>, // (the exchange so far, and whose)
    plain: tokio::sync::Mutex<bool>, // (a token asked for as a plain password)
    oauth: tokio::sync::Mutex<Option<Bearer>>, // (a token asked for by OAUTHBEARER, psql 18's)
    cancels: Arc<ConnectionManager>, // (the server's connections, by the key a cancel request names)
}

/// OAUTHBEARER's exchange (RFC 7628, as psql 18 speaks it; ADR-074): a client with no token yet, or a
/// token refused, is told where the provider is, answers `\x01`, and is refused; psql then gets a token
/// through the device flow and connects again with it.
struct Bearer {
    issuer: String, // (the provider's issuer and scopes: what psql is told)
    scope: String,
    told: Option<String>, // (told, and why it is refused once it answers)
}

fn params() -> DefaultServerParameterProvider {
    let mut params = DefaultServerParameterProvider::default();
    params.server_version = "16.0 (Pondra)".into();
    params
}

/// A sign-in refused outright: FATAL, SQLSTATE 28000, with why (the connection ends with it).
fn fatal(why: String) -> PgWireError { PgWireError::UserError(Box::new(ErrorInfo::new("FATAL".into(), "28000".into(), why))) }

/// A sign-in refused for its failures from this address (`auth::guessing`), the right password too: it is
/// audited as HTTP's is, and waits 400 ms first, as a wrong guess does.
async fn guessing_refused(app: &App, user: &str, from: Option<std::net::SocketAddr>, wait: u64) -> PgWireError {
    let why = crate::auth::guessing_says(user, wait);
    crate::audit::refused(app, user, "postgres", from, "sign in", &why);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    fatal(why)
}

impl Startup {
    /// OAUTHBEARER asked for (true), when a provider that says `POSTGRES 'oauth'` would sign this user in: one
    /// way a connection, as pg_hba has it, since libpq 18 takes the first mechanism it knows and never falls
    /// back to a password (ADR-074 §4a). SCRAM-SHA-256 is offered after it only so that a client before libpq
    /// 18 says why it can't sign in (`oauth_step`): such a user has no password, so it never signs anyone in.
    async fn ask_oauth<C>(&self, client: &mut C, listed: bool) -> PgWireResult<bool>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let Some((issuer, scope)) = crate::sso::oauth_for(&self.backend.app.lake, listed).await else { return Ok(false) };
        *self.oauth.lock().await = Some(Bearer { issuer, scope, told: None });
        let mechanisms = vec!["OAUTHBEARER".into(), "SCRAM-SHA-256".into()];
        client.send(PgWireBackendMessage::Authentication(pgwire::messages::startup::Authentication::SASL(mechanisms))).await?;
        Ok(true)
    }

    /// A step of OAUTHBEARER's exchange: a token signs in (no SASLFinal: OAUTHBEARER has none, and libpq
    /// refuses one); none, or one refused, is told where the provider is and refused once it answers; a
    /// client that chose SCRAM is told the way out.
    async fn oauth_step<C>(&self, client: &mut C, m: pgwire::messages::startup::PasswordMessageFamily) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let app = &self.backend.app;
        let from = Some(client.socket_addr());
        let user = client.metadata().get("user").cloned().unwrap_or_default();
        let mut oauth = self.oauth.lock().await;
        let Some(bearer) = oauth.as_mut() else { return Ok(()) };
        if let Some(why) = bearer.told.take() {
            return Err(fatal(why)); // (its `\x01` after it was told where the provider is)
        }
        let first = m.into_sasl_initial_response()?;
        if first.auth_method != "OAUTHBEARER" {
            let why = format!("user \"{user}\" signs in through an identity provider (OAuth), which this client doesn't speak: use psql 18 or another libpq 18 client, with oauth_issuer={} and oauth_client_id, or ask for the provider's POSTGRES 'password' to send a token as the password", bearer.issuer);
            crate::audit::refused(app, &user, "postgres", from, "sign in", "the client doesn't speak OAuth");
            return Err(fatal(why));
        }
        let discovery = format!("{}/.well-known/openid-configuration", bearer.issuer);
        let why = match crate::sso::bearer(first.data.as_deref().unwrap_or_default()).filter(|t| !t.is_empty()) {
            None => format!("{user} signs in with a token from the identity provider ({discovery})"),
            Some(token) => match crate::users::sign_in(&app.lake, &app.auth, &user, &token).await {
                Some(who) => {
                    *self.backend.who.lock().unwrap() = Some(who.at("postgres", from));
                    return finish(client, &self.cancels).await;
                }
                None => {
                    let why = crate::sso::why(&app.lake, &token).await.unwrap_or_else(|| format!("the token doesn't sign in {user}"));
                    crate::audit::refused(app, &user, "postgres", from, "sign in", &why);
                    // (the cost of a wrong guess, but not counted by `auth::Guesses`: a provider's token isn't
                    // guessed, and counting it would lock the user out of the right one, invariant 381)
                    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                    format!("OAuth bearer authentication failed for user \"{user}\": {why}")
                }
            },
        };
        let error = serde_json::json!({"status": "invalid_token", "openid-configuration": discovery, "scope": bearer.scope});
        bearer.told = Some(why);
        client.send(PgWireBackendMessage::Authentication(pgwire::messages::startup::Authentication::SASLContinue(error.to_string().into_bytes().into()))).await?;
        Ok(())
    }
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
        let ip = from.map(|a| a.ip()); // (failed sign-ins are counted per address and user name: `auth::guessing`)
        let wrong = |user: &str| {
            crate::audit::refused(app, user, "postgres", from, "sign in", "wrong user name or password");
            PgWireError::InvalidPassword(user.to_string())
        };
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                if !crate::tls::pg_allowed(client.is_secure(), client.socket_addr()) {
                    return Err(fatal(crate::tls::PLAIN.into()));
                }
                pgwire::api::auth::protocol_negotiation(client, startup).await?;
                pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                if app.open().await {
                    *self.backend.who.lock().unwrap() = Some(crate::auth::Principal::of(crate::auth::Role::Admin).at("postgres", Some(client.socket_addr())));
                    return finish(client, &self.cancels).await;
                }
                client.set_state(pgwire::api::PgWireConnectionState::AuthenticationInProgress);
                // (a user with only tokens, a service's, or one an identity provider signs in: its token
                // as a password, asked for plainly; ADR-074. A name no user has is asked so only where a
                // password may cross: it may be a mistyped name, and its password would come in the clear.
                // A provider that says POSTGRES 'oauth' asks by OAUTHBEARER instead: `ask_oauth`)
                let user = client.metadata().get("user").cloned().unwrap_or_default();
                let no_password = !crate::users::BUILT_IN.contains(&user.as_str()) && crate::users::verifier(&app.lake, &user).await.is_none();
                let listed = no_password && crate::users::principal(&app.lake, &user).await.is_ok();
                if no_password && self.ask_oauth(client, listed).await? {
                    return Ok(());
                }
                let tokens_only = listed || (no_password && crate::tls::may_send_password(client.is_secure(), client.socket_addr()) && crate::sso::anyone(&app.lake).await);
                *self.plain.lock().await = tokens_only;
                client.send(PgWireBackendMessage::Authentication(if tokens_only { Authentication::CleartextPassword } else { Authentication::SASL(vec!["SCRAM-SHA-256".into()]) })).await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(m) if self.oauth.lock().await.is_some() => return self.oauth_step(client, m).await,
            PgWireFrontendMessage::PasswordMessageFamily(m) if *self.plain.lock().await => {
                let user = client.metadata().get("user").cloned().unwrap_or_default();
                let password = m.into_password()?.password;
                let token = crate::users::is_token(&user, &password);
                // (a user's password crosses only TLS from another machine; a token may, as `tls.rs` says)
                if !token && !crate::tls::may_send_password(client.is_secure(), client.socket_addr()) {
                    return Err(fatal(crate::tls::PASSWORD_PLAIN.into()));
                }
                if let Some(wait) = crate::auth::guessing(ip, &user) {
                    return Err(guessing_refused(app, &user, from, wait).await);
                }
                let Some(who) = crate::users::sign_in(&app.lake, &app.auth, &user, &password).await else {
                    if !token {
                        crate::auth::guessed(ip, &user); // (a token isn't guessed: it is random)
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                    if let Some(why) = crate::sso::why(&app.lake, &password).await {
                        // (an identity provider's token says why it was refused, as HTTP's 401 does)
                        crate::audit::refused(app, &user, "postgres", from, "sign in", &why);
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new("FATAL".into(), "28P01".into(), format!("password authentication failed for user \"{user}\": {why}")))));
                    }
                    return Err(wrong(&user));
                };
                *self.backend.who.lock().unwrap() = Some(who.at("postgres", Some(client.socket_addr())));
                return finish(client, &self.cancels).await;
            }
            PgWireFrontendMessage::PasswordMessageFamily(m) => {
                let user = client.metadata().get("user").cloned().unwrap_or_default();
                let mut scram = self.scram.lock().await;
                match scram.take() {
                    None => {
                        // (refused before SCRAM answers: a user who failed ten times in a minute from this address)
                        if let Some(wait) = crate::auth::guessing(ip, &user) {
                            return Err(guessing_refused(app, &user, from, wait).await);
                        }
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
                            // (a built-in name's password is its token, random and never guessed)
                            if !crate::users::BUILT_IN.contains(&user.as_str()) {
                                crate::auth::guessed(ip, &user);
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(400)).await; // (a guess costs time)
                            return Err(wrong(&user));
                        };
                        let who = match crate::users::BUILT_IN.contains(&user.as_str()) {
                            true => Some(crate::auth::Principal::token(app.auth.role_of_user(&user))),
                            false => crate::users::principal(&app.lake, &user).await.ok(),
                        };
                        let Some(who) = who else { return Err(wrong(&user)) };
                        *self.backend.who.lock().unwrap() = Some(who.at("postgres", Some(client.socket_addr())));
                        client.send(PgWireBackendMessage::Authentication(Authentication::SASLFinal(reply.into_bytes().into()))).await?;
                        return finish(client, &self.cancels).await;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Signed in: Postgres's parameters, a key to cancel with, ready for queries.
async fn finish<C>(client: &mut C, cancels: &Arc<ConnectionManager>) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    use pgwire::api::PidSecretKeyGenerator;
    let (pid, key) = pgwire::api::RandomPidSecretKeyGenerator::default().generate(client);
    client.set_pid_and_secret_key(pid, key.clone());
    // The connection's cancel handle, under its key: a cancel request fires the statement running now
    // (pgwire races each query with it). The guard unregisters the key when the connection ends.
    let (handle, guard) = cancels.register(pid, key);
    client.session_extensions().insert(handle);
    client.session_extensions().insert(guard);
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
        if let Some(c) = crate::vars::change(sql) {
            // DECLARE $day …, $day = …: this connection's variable (`vars.rs`)
            let tag = c.tag();
            Box::pin(crate::vars::apply(&self.app, c)).await.map_err(user_error)?;
            return Ok(Response::Execution(Tag::new(tag)));
        }
        let bound = crate::vars::bound(sql).map_err(user_error)?; // (`$day`: its value; `$1` is the protocol's)
        let bound = self.placed(bound.into_owned()).await?; // (the session's search path: full names, ahead of the key lookup)
        let sql: &str = &bound;
        let reader = crate::auth::current().is_some_and(|p| p.role >= crate::auth::Role::Read);
        if reader && crate::txn::open() && crate::txn::gate(&self.app.lake, sql).is_ok() {
            if let Some(b) = crate::txn::point_read(&self.app.lake, sql).await.map_err(user_error)? {
                let schema = b.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(Schema::empty()));
                let schema = crate::pg_types::typed(&schema, &crate::pg_types::columns(&self.app.lake, sql).await);
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
                    return Ok(Response::Query(rows(&crate::pg_types::typed(&b.schema(), &p.types().into()), vec![b], format)?));
                }
            }
        }
        let sql = crate::routines::expand(&self.app.lake, sql).await.map_err(user_error)?; // (macros: ADR-023)
        let sql = crate::asof::rewrite(&pg_dialect(&self.app.lake, &sql, user)).map_err(user_error)?.into_owned();
        if crate::env::is_set("PONDRA_DEBUG_PG") {
            eprintln!("pg {user}: {sql}");
        }
        let statement = crate::write::first_word(&sql); // (dbt's leading comment is no part of the statement)
        if let Some(word) = crate::txn::control(statement) {
            // BEGIN, COMMIT, ROLLBACK: this connection's transaction (ADR-036 §5)
            let answer = crate::txn::command(&self.app, word, statement).await;
            crate::cursors::sweep(); // (a COMMIT or ROLLBACK ends the cursors without HOLD: their answers go now, refused or not)
            let (tag, warning) = answer.map_err(user_error)?;
            warning.iter().for_each(|w| crate::routines::heard(&format!("WARNING: {w}")));
            // (the client sees each one in ReadyForQuery's status: a savepoint leaves the transaction open, ROLLBACK TO
            // reopens it after a failure, and only BEGIN and COMMIT or ROLLBACK start and end it)
            return Ok(match word {
                "BEGIN" | "ROLLBACK TO" => Response::TransactionStart(Tag::new(tag)),
                "SAVEPOINT" | "RELEASE" => Response::Execution(Tag::new(tag)),
                _ => Response::TransactionEnd(Tag::new(tag)),
            });
        }
        let _busy = crate::txn::next(&self.app, &sql).await.map_err(user_error)?; // (a failed transaction takes nothing but its end; READ COMMITTED reads the newest commit; the guard keeps the transaction busy while this runs)
        if let Some(b) = crate::txn::locked_read(&self.app, &sql).await.map_err(user_error)? {
            let schema = b.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
            let schema = crate::pg_types::typed(&schema, &crate::pg_types::columns(&self.app.lake, &bound).await);
            return Ok(Response::Query(rows(&schema, b, format)?)); // (a locking read in a transaction: the rows it locked)
        }
        if let Some(cmd) = crate::cursors::of(&sql) {
            // DECLARE, FETCH, MOVE, CLOSE: this connection's cursors (cursors.rs). Before the settings and
            // session commands, which read CLOSE as their own.
            let role = crate::auth::current().map_or(crate::auth::Role::None, |p| p.role);
            return Ok(match crate::cursors::run(&self.app, cmd.map_err(user_error)?, role).await.map_err(user_error)? {
                crate::cursors::Answer::Rows(batches) => {
                    let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(Schema::empty()));
                    let mut answer = rows(&schema, batches, format)?;
                    answer.command_tag = "FETCH".into(); // (its command tag: "FETCH n", the rows counted as they go out)
                    Response::Query(answer)
                }
                crate::cursors::Answer::Tag(tag) => Response::Execution(Tag::new(&tag)),
            });
        }
        if crate::settings::is(&sql) {
            // SET, RESET, PREPARE, EXECUTE, DEALLOCATE: this connection's (round 31)
            return match crate::settings::statement(&self.app.lake, &sql).await {
                Ok(crate::settings::Done::Said(tag)) => Ok(Response::Execution(Tag::new(tag))),
                Ok(crate::settings::Done::Run(sql)) => Box::pin(self.run(user, &sql, format)).await,
                Err(_) if crate::write::first_word(&sql).to_uppercase().starts_with("DEALLOCATE") => Ok(Response::Execution(Tag::new("DEALLOCATE"))), // (a statement the protocol prepared: pgwire's)
                Err(e) => Err(user_error(e)),
            };
        }
        if let Some(r) = session_command(&sql) {
            return Ok(r);
        }
        if let Some(copy) = Copy::of(&sql) {
            return self.copy_out(copy?).await;
        }
        let role = crate::auth::current().map_or(crate::auth::Role::None, |p| p.role);
        if role < crate::auth::Role::Read {
            return Err(user_error(crate::codes::coded("28000", "sign in first")));
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
        if let Some(r) = crate::returning::of(&sql) {
            // INSERT, UPDATE or DELETE … RETURNING: the rows it wrote, under its command's tag (`INSERT 0 3`)
            let b = Box::pin(crate::returning::run(&self.app, &r, role, false, None)).await.map_err(user_error)?;
            let schema = crate::pg_types::typed(&b.schema(), &crate::pg_types::columns(&self.app.lake, &bound).await);
            let mut answer = rows(&schema, vec![b], format)?;
            answer.command_tag = r.tag().into();
            return Ok(Response::Query(answer));
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
            true => crate::pg_catalog::answer(&self.app.lake, &sql, if user.is_empty() { "pondra" } else { user }).await.map_err(user_error)?,
            false => self.answer(&sql).await.map_err(user_error)?,
        };
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => self.schema(&sql, user).await?,
        };
        // (its uuids and Postgres names, from the statement as written; a catalog query has names only)
        let schema = crate::pg_types::typed(&schema, &crate::pg_types::columns(&self.app.lake, &bound).await);
        Ok(Response::Query(rows(&schema, batches, format)?))
    }

    /// `run`, the notices its procedures send (what they print) sent first: psql shows NOTICE.
    async fn told<C: Sink<PgWireBackendMessage> + Unpin + Send>(&self, client: &mut C, user: &str, sql: &str, format: &Format) -> PgWireResult<Response> {
        let t0 = std::time::Instant::now();
        let me = self.me.upgrade().expect("its connection holds it");
        let (u, q, f) = (user.to_string(), sql.to_string(), format.clone());
        let run = async move {
            let (session, who) = (Some(me.session.clone()), me.who());
            crate::routines::with_notices(crate::temp::SESSION.scope(session, crate::auth::WHO.scope(who, me.caught(&u, &q, &f)))).await
        };
        let (out, heard) = crate::panics::work(true, Box::pin(run)).await.unwrap_or_else(|m| (Err(user_error(anyhow::anyhow!(m))), vec![])); // (stoppable for memory: `oom.rs`)
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
        if crate::drain::draining() {
            // (stopping: the statement goes to another node, as a new connection does; `drain.rs`)
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), "57P01".into(), "this node is stopping: connect to another node".into()))));
        }
        let run = async { crate::panics::door(Box::pin(self.run(user, sql, format))).await.unwrap_or_else(|m| Err(user_error(anyhow::anyhow!(m)))) };
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

    /// A statement pgwire cancelled (a cancel request, psql's Ctrl-C), or one the node stopped for memory
    /// (`oom.rs`), is dropped before it reaches `caught`, which would fail its transaction: here it does,
    /// as the error would have.
    fn cancelled<T>(&self, out: &PgWireResult<T>) {
        let why = match out {
            Err(PgWireError::QueryCanceled) => "canceling statement due to user request",
            Err(PgWireError::UserError(e)) if e.message == crate::oom::OOM => crate::oom::OOM,
            _ => return,
        };
        crate::temp::SESSION.sync_scope(Some(self.session.clone()), || crate::txn::failed(why));
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
            None => self.schema(&sql, "").await?,
        };
        let format = if c.format == "binary" { FieldFormat::Binary } else { FieldFormat::Text };
        let (info, types) = fields(&schema, &|_| format);
        let pg_types: Vec<Type> = info.iter().map(|f| f.datatype().clone()).collect();
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
            for (col, t) in cols.iter().zip(&pg_types) {
                field!(&mut enc, col, i, t)?;
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
    async fn copy_in<C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send>(&self, client: &mut C, _user: &str, c: Copy) -> PgWireResult<Response> {
        let refuse = |why: &str| Err(user_error(anyhow::anyhow!("{why}")));
        if c.query.is_some() || c.format == "binary" {
            return refuse("COPY FROM STDIN takes a table, in text or CSV");
        }
        let stmt = crate::write::parse(&format!("INSERT INTO {} SELECT 1", c.table)).ok_or_else(|| user_error(anyhow::anyhow!("COPY: no table {}", c.table)))?;
        self.app.auth.allows(crate::auth::current().map_or(crate::auth::Role::None, |p| p.role), &stmt).map_err(user_error)?;
        // In a transaction the rows go to the log as they come, not into the transaction's kept writes (as DDL does,
        // ADR-067 §7): READ ONLY and a failed transaction refuse the COPY (25006, 25P02), and a notice says it ran at once.
        crate::txn::gate(&self.app.lake, &format!("COPY {} FROM STDIN", c.table)).map_err(user_error)?;
        if crate::txn::open() {
            let note = "COPY ran at once: it isn't part of a transaction here, and ROLLBACK won't undo it";
            let _ = client.send(PgWireBackendMessage::NoticeResponse(ErrorInfo::new("NOTICE".into(), "00000".into(), note.into()).into())).await;
        }
        let (other, table) = crate::ddl::resolve(&self.app.lake, &c.table).await.map_err(user_error)?;
        if other.is_some() {
            return refuse("COPY into an attached lake: INSERT INTO it instead");
        }
        let meta = self.app.lake.cat.get::<crate::store::TableMeta>(&crate::store::table_key(&table)).await.map_err(user_error)?.ok_or_else(|| user_error(anyhow::anyhow!("no table {table}")))?.logical();
        // (a keyed table's rows replace their keys': UPDATE too; a `_deleted` column named may delete them: S11)
        crate::auth::check_changes(&table, !meta.key.is_empty(), c.columns.iter().any(|c| c == "_deleted")).map_err(user_error)?;
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
        let table = crate::query::schema(&meta.logical_columns()).map_err(err)?; // (SQL's names: ADR-022)
        let uuids = meta.logical().sql_types;
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
            let b = crate::types::spelled(b, &uuids).map_err(err)?; // (`'A0EE…'` in a uuid column as Postgres reads it)
            let b = crate::defaults::fill(&self.app.lake, &meta, b.clone(), |c| (!given.iter().any(|g| g == c)).then(|| crate::defaults::all(b.num_rows()))).await.map_err(err)?;
            p.rows += b.num_rows() as u64;
            p.seq += 1;
            let src = crate::log::Src { producer: format!("copy:{}", p.job), seq: p.seq, prev: None };
            self.app.log().map_err(err)?.append(p.copy.table.clone(), src, b).await.map_err(err)?;
        }
        Ok(())
    }

    /// A query's answer, remembered as HTTP's are (`App::remembers`): a dashboard's repeated query is a
    /// lookup until the next commit, however many connections ask. Never in a transaction (its own
    /// writes and snapshot). Kept as Arrow, so each connection sends it in its own formats.
    async fn answer(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        use crate::server::Kept;
        let version = match crate::txn::open() {
            true => None,
            false => self.app.remembers(sql).await,
        };
        let Some(version) = version else { return self.app.query(sql, None).await };
        let run = || async {
            let batches = self.app.query(sql, None).await?;
            let keep = match batches.iter().map(|b| b.get_array_memory_size()).sum::<usize>() <= 1 << 20 {
                true => Some(crate::query::ipc(&batches)?.into()),
                false => None, // (a big answer: `Results` keeps none past 1 MB, nor encodes it to find out)
            };
            Ok((keep, batches)) // (its own caller takes the batches as they are: never decoded again)
        };
        match self.app.remembered(format!("pg|{sql}"), sql, version, None, run).await? {
            Kept::Bytes(b) => crate::query::read_ipc(&b),
            Kept::Own(batches) => Ok(batches),
        }
    }

    /// A query's result columns, without running it.
    async fn schema(&self, sql: &str, user: &str) -> PgWireResult<Arc<Schema>> {
        let sql = crate::asof::rewrite(sql).map_err(user_error)?;
        let ctx = self.session(&sql, user).await?;
        let df = crate::query::sql(&ctx, &sql).await.map_err(user_error)?;
        Ok(Arc::new(crate::query::run_schema(&ctx, df.logical_plan()).map_err(user_error)?.as_arrow().clone()))
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
    let schema = crate::path::current(lake).map_or_else(|| "public".to_string(), |p| p.schema().to_string()); // (the session's current schema)
    let sql = match crate::pg_catalog::wanted(sql) {
        true => crate::pg_catalog::rewrite(sql, user, &schema),
        false => sql.to_string(),
    };
    let sql = if sql.eq_ignore_ascii_case("select version()") { "SELECT version() AS version".into() } else { sql }; // (the column's name in Postgres)
    // (the ADBC driver's list of types: receive functions are names here, not function ids)
    let sql = sql.replace("(typreceive != 0 OR typsend != 0)", "true").replace("typreceive::TEXT", "typreceive");
    sql.replace("version()", "'PostgreSQL 16.0 (Pondra on Apache DataFusion)'") // (current_schema() and current_database() are `path::door`'s, at every door)
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

/// Postgres's session commands: `SHOW` of a Postgres setting (what the session set, or what
/// clients expect), and `DISCARD` and `CLOSE` accepted. (`SET` and the rest: `settings.rs`.)
fn session_command(sql: &str) -> Option<Response> {
    if crate::script::is(sql) {
        return None; // (`BEGIN … END` that opens a block is a script's)
    }
    // (past a comment: dbt's. A word that is only a no-op here is refused when a comment came before it,
    // as it was before this: a `BEGIN` answered as if it began a transaction would autocommit what follows.)
    let commented = crate::write::first_word(sql) != sql.trim_start();
    let first = crate::write::first_word(sql).split_whitespace().next()?.to_uppercase();
    let setting = |column: &str, v: &str| {
        let field = Arc::new(vec![FieldInfo::new(column.into(), None, None, Type::VARCHAR, FieldFormat::Text)]);
        let mut row = DataRowEncoder::new(field.clone());
        row.encode_field(&v).ok()?;
        Some(Response::Query(QueryResponse::new(field, stream::iter([Ok(row.take_row())]))))
    };
    match first.as_str() {
        "START" if crate::routines::start_of(sql).is_some() => None, // (`START CALL p(…)`: a run started)
        "SET" | "RESET" | "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "DISCARD" | "DEALLOCATE" | "CLOSE" if !commented => {
            Some(Response::Execution(Tag::new(&first)))
        }
        "SHOW" if show_column(sql).is_some() => {
            let (name, column) = show_column(sql)?;
            setting(&column, &crate::txn::shown(&name).or_else(|| crate::settings::shown(&name)).unwrap_or_else(|| match name.as_str() {
                "search_path" => "public".into(), // (no session path: this lake's public, as Postgres's `"$user", public` in its usual setup)
                s if s.contains("standard_conforming_strings") => "on".into(),
                s if s.contains("transaction") => "read committed".into(),
                s if s.contains("server_version") => "16.0".into(),
                s if s.contains("encoding") => "UTF8".into(),
                s if s.contains("zone") => "UTC".into(),
                _ => String::new(),
            }))
        }
        _ => None,
    }
}

/// A `SHOW` that `session_command` answers: the setting's name, and the column its one row comes back in.
/// Postgres names that column after the parameter (`SHOW TRANSACTION ISOLATION LEVEL` is
/// `transaction_isolation`), and pgjdbc and psycopg read the row by that name, so Describe must say it too.
fn show_column(sql: &str) -> Option<(String, String)> {
    let sql = crate::write::first_word(sql); // (from its first word: a comment before it is no part of the setting's name)
    let first = sql.split_whitespace().next()?.to_uppercase();
    let lower = sql.to_lowercase();
    if first != "SHOW" || lower.contains("tables") || lower.contains("datafusion.") || lower.trim_end_matches(';').trim().ends_with(" all") {
        return None;
    }
    let name = sql.split_whitespace().skip(1).collect::<Vec<_>>().join(" ").trim_end_matches(';').to_lowercase();
    let column = match name.as_str() {
        "transaction isolation level" => "transaction_isolation".to_string(),
        "time zone" => "timezone".to_string(),
        _ => name.replace(' ', "_"),
    };
    Some((name, column))
}

/// Result rows, each column as the Postgres type closest to its Arrow type — encoded a batch at a
/// time, as the client takes them.
fn rows(schema: &Schema, batches: Vec<RecordBatch>, format: &Format) -> PgWireResult<QueryResponse> {
    let (info, types) = fields(schema, &|i| format.format_for(i));
    let each = info.clone();
    let rows = batches.into_iter().flat_map(move |b| each_row(&b, &types, |cols, i| {
        let mut row = DataRowEncoder::new(each.clone());
        for (c, f) in cols.iter().zip(each.iter()) {
            field!(&mut row, c, i, f.datatype())?;
        }
        Ok(row.take_row())
    }));
    Ok(QueryResponse::new(info, stream::iter(rows)))
}

/// Columns as Postgres sees them, and the Arrow types their values are sent as.
fn fields(schema: &Schema, format: &dyn Fn(usize) -> FieldFormat) -> (Arc<Vec<FieldInfo>>, Vec<DataType>) {
    let types: Vec<(Type, DataType)> = schema.fields().iter().enumerate().map(|(i, f)| field_type(f, format(i))).collect();
    let info = schema.fields().iter().zip(&types).enumerate().map(|(i, (f, (t, _)))| FieldInfo::new(crate::pg_names::shown(f).to_string(), None, None, t.clone(), format(i))).collect();
    (Arc::new(info), types.into_iter().map(|(_, a)| a).collect())
}

/// `row(columns, i)` for each row of `b`, its columns cast to `types` first.
fn each_row<T>(b: &RecordBatch, types: &[DataType], mut row: impl FnMut(&[ArrayRef], usize) -> PgWireResult<T>) -> Vec<PgWireResult<T>> {
    match b.columns().iter().zip(types).map(|(c, t)| cast(c, t)).collect::<Result<Vec<_>, _>>() {
        Ok(cols) => (0..b.num_rows()).map(|i| row(&cols, i)).collect(),
        Err(e) => vec![Err(user_error(e.into()))],
    }
}

/// A column's Postgres type and the Arrow type its values are sent as: a uuid's, json's or jsonb's
/// (`pg_types::typed` marks one), else its Arrow type's.
fn field_type(f: &datafusion::arrow::datatypes::Field, format: FieldFormat) -> (Type, DataType) {
    match crate::pg_types::marked_type(f) {
        Some(Ty::Uuid) => (Type::UUID, DataType::Utf8),
        Some(Ty::Json) => (Type::JSON, DataType::Utf8),
        Some(Ty::Jsonb) => (Type::JSONB, DataType::Utf8),
        None => pg_type(f.data_type(), format),
    }
}

/// The Postgres type for an Arrow type, and the Arrow type its values are sent as.
pub(crate) fn pg_type(t: &DataType, format: FieldFormat) -> (Type, DataType) {
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
        DataType::Time32(_) | DataType::Time64(_) => (Type::TIME, DataType::Time64(TimeUnit::Microsecond)), // (`time`, as `micros_time` gives it)
        DataType::Interval(_) => (Type::INTERVAL, DataType::Interval(IntervalUnit::MonthDayNano)), // (`PgInterval`: Postgres's text and binary)
        DataType::Timestamp(_, Some(_)) => (Type::TIMESTAMPTZ, DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))), // (an instant: sent in UTC)
        DataType::Timestamp(..) => (Type::TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None)),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => (Type::BYTEA, DataType::Binary),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => (Type::TEXT, DataType::Utf8), // (text, as Postgres names a string expression's type)
        // Lists as Postgres's arrays (drivers give them as lists); of other things, as text.
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            let of = |t: DataType| DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new("item", t, true)));
            match f.data_type() {
                // (text[], as the catalog's pg_attribute and pg_type give a list of strings: pg_catalog.rs)
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => (Type::TEXT_ARRAY, of(DataType::Utf8)),
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
            DataType::Time64(_) => row.encode_field(&PgTime(c.as_primitive::<Time64MicrosecondType>().value(i))),
            DataType::Interval(_) => row.encode_field(&PgInterval::of(c.as_primitive::<IntervalMonthDayNanoType>().value(i))),
            DataType::Timestamp(_, zone) => row.encode_field(&c.as_primitive::<TimestampMicrosecondType>().value_as_datetime(i).map(|at| PgStamp { at, zoned: zone.is_some() })),
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

/// One column's value in a row or a COPY's line, as its Postgres type sends it: a uuid, json or jsonb as its
/// own type (`PgUuid`, `PgJson`), anything else as its Arrow type says (`encode!`). `t` is the column's type.
macro_rules! field {
    ($row:expr, $c:expr, $i:expr, $t:expr) => {{
        let (c, i, t): (&datafusion::arrow::array::ArrayRef, usize, &Type) = ($c, $i, $t);
        if c.is_null(i) {
            encode!($row, c, i)
        } else if *t == Type::UUID {
            $row.encode_field(&PgUuid(c.as_string::<i32>().value(i).to_string()))
        } else if *t == Type::JSON || *t == Type::JSONB {
            $row.encode_field(&PgJson(c.as_string::<i32>().value(i).to_string()))
        } else {
            encode!($row, c, i)
        }
    }};
}
use field;

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

/// An INTERVAL as Postgres sends it: its text (`interval_text`) or its binary (microseconds, days,
/// months, in that order).
#[derive(Debug)]
struct PgInterval {
    months: i64,
    days: i64,
    micros: i64,
}

impl PgInterval {
    fn of(v: datafusion::arrow::datatypes::IntervalMonthDayNano) -> Self {
        Self { months: v.months.into(), days: v.days.into(), micros: v.nanoseconds / 1000 }
    }
}

impl postgres_types::ToSql for PgInterval {
    fn to_sql(&self, _: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut;
        out.put_i64(self.micros);
        out.put_i32(self.days as i32);
        out.put_i32(self.months as i32);
        Ok(postgres_types::IsNull::No)
    }
    fn accepts(ty: &Type) -> bool { *ty == Type::INTERVAL }
    postgres_types::to_sql_checked!();
}

impl pgwire::types::ToSqlText for PgInterval {
    fn to_sql_text(&self, _: &Type, out: &mut bytes::BytesMut, _: &pgwire::types::format::FormatOptions) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(crate::pg_values::interval_text(self.months, self.days, self.micros).as_bytes());
        Ok(postgres_types::IsNull::No)
    }
}

/// A TIME as Postgres sends it: its text (`time_text`, as few fraction digits as the value needs)
/// or its binary (microseconds since midnight, which is what `NaiveTime` sends too).
#[derive(Debug)]
struct PgTime(i64);

impl postgres_types::ToSql for PgTime {
    fn to_sql(&self, _: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut;
        out.put_i64(self.0);
        Ok(postgres_types::IsNull::No)
    }
    fn accepts(ty: &Type) -> bool { *ty == Type::TIME }
    postgres_types::to_sql_checked!();
}

impl pgwire::types::ToSqlText for PgTime {
    fn to_sql_text(&self, _: &Type, out: &mut bytes::BytesMut, _: &pgwire::types::format::FormatOptions) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(crate::pg_values::time_text(self.0).as_bytes());
        Ok(postgres_types::IsNull::No)
    }
}

/// A TIMESTAMP or TIMESTAMPTZ as Postgres sends it: in ISO text with as few fraction digits as the value
/// needs (`2026-01-01 00:00:00`, where pgwire writes `…00.000000`), an instant with `+00`; in binary as
/// chrono's. Another DateStyle, or a year before 1, is pgwire's.
#[derive(Debug)]
struct PgStamp {
    at: chrono::NaiveDateTime,
    zoned: bool,
}

impl postgres_types::ToSql for PgStamp {
    fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if self.zoned { self.at.and_utc().to_sql(ty, out) } else { self.at.to_sql(ty, out) }
    }
    fn accepts(ty: &Type) -> bool { matches!(*ty, Type::TIMESTAMP | Type::TIMESTAMPTZ) }
    postgres_types::to_sql_checked!();
}

impl pgwire::types::ToSqlText for PgStamp {
    fn to_sql_text(&self, ty: &Type, out: &mut bytes::BytesMut, o: &pgwire::types::format::FormatOptions) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use chrono::Datelike;
        if !o.date_style.trim_start().to_ascii_uppercase().starts_with("ISO") || self.at.year() < 1 {
            return if self.zoned { self.at.and_utc().to_sql_text(ty, out, o) } else { self.at.to_sql_text(ty, out, o) };
        }
        out.extend_from_slice(crate::pg_values::stamp_text(self.at, self.zoned).as_bytes());
        Ok(postgres_types::IsNull::No)
    }
}

/// A uuid as Postgres sends it: its text, or its 16 bytes (a column holds the text: `types.rs`).
#[derive(Debug)]
struct PgUuid(String);

/// A json or jsonb value as Postgres sends it: its text, and for jsonb the version byte 1 before it, as
/// Postgres's binary jsonb has it (a column holds the text: `types.rs`).
#[derive(Debug)]
struct PgJson(String);

impl postgres_types::ToSql for PgJson {
    fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if *ty == Type::JSONB {
            out.extend_from_slice(&[1]);
        }
        out.extend_from_slice(self.0.as_bytes());
        Ok(postgres_types::IsNull::No)
    }
    fn accepts(ty: &Type) -> bool { *ty == Type::JSON || *ty == Type::JSONB }
    postgres_types::to_sql_checked!();
}

impl pgwire::types::ToSqlText for PgJson {
    fn to_sql_text(&self, _: &Type, out: &mut bytes::BytesMut, _: &pgwire::types::format::FormatOptions) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(postgres_types::IsNull::No)
    }
}

impl postgres_types::ToSql for PgUuid {
    fn to_sql(&self, _: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(uuid::Uuid::try_parse(&self.0)?.as_bytes());
        Ok(postgres_types::IsNull::No)
    }
    fn accepts(ty: &Type) -> bool { *ty == Type::UUID }
    postgres_types::to_sql_checked!();
}

impl pgwire::types::ToSqlText for PgUuid {
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

crate::per_node!(static COPIES: std::sync::Mutex<std::collections::HashMap<std::net::SocketAddr, Pending>> = Default::default());

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

/// A statement as Parse gave it, and what was found of it once. Postgres settles a prepared statement's
/// parameter types, and its columns, when it is prepared. Finding them here plans the query, and libpq's
/// drivers (psycopg) bind, describe and execute a prepared statement every time it runs: each run
/// planned it three times. Clones share what was found.
#[derive(Clone)]
pub struct Prepared {
    sql: String,
    found: Arc<Found>,
}

#[derive(Default)]
struct Found {
    types: tokio::sync::OnceCell<Vec<Type>>, // ($1, $2…: `param_types`, the first time they are asked for)
    columns: std::sync::Mutex<Columns>,
}

/// What a statement's Describe found, for the next one.
#[derive(Default)]
enum Columns {
    #[default]
    Unknown,
    /// No rows: a write or a session's command, as its text says.
    None,
    /// A query's columns. `given` once a Describe was answered from them, not found afresh: the client
    /// reads the rows by them, so an answer whose columns differ is refused (`answered`).
    Kept { schema: Arc<Schema>, given: bool },
    /// Described each time: a cursor's FETCH (its cursor may be declared again), a SHOW, or a statement
    /// whose columns changed once.
    Each,
}

impl Prepared {
    fn new(sql: String) -> Self { Prepared { sql, found: Default::default() } }

    /// The columns a Describe of the portal answers without planning, if kept.
    fn kept(&self, format: &Format) -> Option<Vec<FieldInfo>> {
        match &mut *self.found.columns.lock().unwrap() {
            Columns::None => Some(vec![]),
            Columns::Kept { schema, given } => {
                *given = true;
                Some(described(schema, format))
            }
            Columns::Unknown | Columns::Each => None,
        }
    }

    /// What its first Describe found, kept (a later one's never replaces it).
    fn keep(&self, columns: Columns) {
        let mut kept = self.found.columns.lock().unwrap();
        if matches!(*kept, Columns::Unknown) {
            *kept = columns;
        }
    }

    /// An answer checked against the columns its Describe gave from what was kept: a query whose columns
    /// changed since (a table altered, another search path) is refused, as Postgres refuses it (0A000),
    /// and described afresh from then on. One that differs from a Describe found afresh is answered as
    /// it was before anything was kept, and not kept.
    fn answered(&self, out: &Response, format: &Format) -> PgWireResult<()> {
        let mut kept = self.found.columns.lock().unwrap();
        let (Columns::Kept { schema, given }, Response::Query(q)) = (&*kept, out) else { return Ok(()) };
        if described(schema, format).iter().map(FieldInfo::datatype).eq(q.row_schema.iter().map(FieldInfo::datatype)) {
            return Ok(());
        }
        let given = *given;
        *kept = Columns::Each;
        match given {
            true => Err(PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), "0A000".into(), "cached plan must not change result type".into())))),
            false => Ok(()),
        }
    }
}

/// Parse keeps the statement's text; what it takes and answers is found when first asked (`Prepared`).
struct Parser;

#[async_trait]
impl QueryParser for Parser {
    type Statement = Prepared;

    async fn parse_sql<C>(&self, _client: &C, sql: &str, _types: &[Option<Type>]) -> PgWireResult<Option<Prepared>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(Some(Prepared::new(sql.to_string())))
    }

    // (Describe is answered by `do_describe_statement` and `do_describe_portal`, never from these.)
    fn get_parameter_types(&self, _: &Prepared) -> PgWireResult<Vec<Type>> { Ok(vec![]) }

    fn get_result_schema(&self, _: &Prepared, _: Option<&Format>) -> PgWireResult<Vec<FieldInfo>> { Ok(vec![]) }
}

/// `$1`, `$2`… replaced by the portal's values, each as a SQL literal of its type (`value`). A
/// parameter's type is the client's, or else the one the query implies (`inferred`).
fn bind(portal: &Portal<Prepared>, inferred: &[Type]) -> PgWireResult<String> {
    let mut values = Vec::with_capacity(portal.parameter_len());
    for i in 0..portal.parameter_len() {
        let t = portal.statement.parameter_types.get(i).cloned().flatten().filter(|t| *t != Type::UNKNOWN);
        let t = t.or_else(|| inferred.get(i).cloned()).unwrap_or(Type::UNKNOWN);
        let v = match t {
            Type::BOOL => portal.parameter::<bool>(i, &t)?.map(|b| b.to_string()),
            Type::INT2 => portal.parameter::<i16>(i, &t)?.map(|v| v.to_string()),
            Type::INT4 => portal.parameter::<i32>(i, &t)?.map(|v| v.to_string()),
            Type::INT8 => portal.parameter::<i64>(i, &t)?.map(|v| v.to_string()),
            // arrays (embeddings for vector search): DataFusion's `[a, b, …]`
            Type::FLOAT4_ARRAY => list(portal.parameter::<Vec<Option<f32>>>(i, &t)?),
            Type::FLOAT8_ARRAY => list(portal.parameter::<Vec<Option<f64>>>(i, &t)?),
            Type::INT4_ARRAY => list(portal.parameter::<Vec<Option<i32>>>(i, &t)?),
            Type::INT8_ARRAY => list(portal.parameter::<Vec<Option<i64>>>(i, &t)?),
            _ => crate::pg_values::value(&t, portal.parameters[i].as_deref(), portal.parameter_format.is_binary(i)).map_err(user_error)?,
        };
        values.push(v.unwrap_or_else(|| "NULL".into()));
    }
    Ok(crate::pg_values::substitute(&portal.statement.statement.sql, &values))
}

fn list<T: ToString>(v: Option<Vec<Option<T>>>) -> Option<String> {
    let item = |x: &Option<T>| x.as_ref().map_or("NULL".to_string(), T::to_string);
    v.map(|v| format!("[{}]", v.iter().map(item).collect::<Vec<_>>().join(", ")))
}

#[async_trait]
impl SimpleQueryHandler for Backend {
    // pgwire races each statement with a cancel request (psql's Ctrl-C): see `cancelled`.
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let out = self._on_query(client, query).await;
        self.cancelled(&out);
        out
    }

    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let user = client.metadata().get("user").cloned().unwrap_or_default();
        let mut out = vec![];
        for q in crate::routines::split(query).iter().map(|q| q.trim()) {
            out.push(match Copy::of(q) {
                Some(Ok(c)) if !c.to => crate::temp::SESSION.scope(Some(self.session.clone()), crate::auth::WHO.scope(self.who(), self.copy_in(client, &user, c))).await?,
                _ => self.told(client, &user, q, &Format::UnifiedText).await?,
            });
        }
        Ok(out)
    }
}

#[async_trait]
impl ExtendedQueryHandler for Backend {
    type Statement = Prepared;
    type QueryParser = Parser;

    fn query_parser(&self) -> Arc<Self::QueryParser> { self.parser.clone() }

    // (pgwire races the portal's statement with a cancel request, as it does a simple query: `cancelled`.)
    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let out = self._on_execute(client, message).await;
        self.cancelled(&out);
        out
    }

    // (psycopg describes the portal named after a DECLARE'd cursor right after the DECLARE: `cursor_portal`
    // makes it when asked, as Postgres has it.)
    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.clone().unwrap_or_default();
        if message.target_type == TARGET_TYPE_BYTE_PORTAL && client.portal_store().get_portal(&name).is_none() {
            crate::temp::SESSION.sync_scope(Some(self.session.clone()), || cursor_portal(&*client, name));
        }
        self._on_describe(client, message).await
    }

    async fn do_query<C>(&self, client: &mut C, portal: &Portal<Self::Statement>, _max_rows: usize) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let user = client.metadata().get("user").cloned().unwrap_or_default();
        let statement = &portal.statement.statement;
        if let Some(Ok(c)) = Copy::of(&statement.sql).filter(|c| c.as_ref().is_ok_and(|c| !c.to)) {
            return crate::temp::SESSION.scope(Some(self.session.clone()), crate::auth::WHO.scope(self.who(), self.copy_in(client, &user, c))).await;
        }
        let inferred = crate::temp::SESSION.scope(Some(self.session.clone()), self.types(statement)).await;
        let out = self.told(client, &user, &bind(portal, inferred)?, &portal.result_column_format).await?;
        match statement.answered(&out, &portal.result_column_format) {
            Ok(()) => Ok(out),
            Err(e) => {
                crate::temp::SESSION.sync_scope(Some(self.session.clone()), || crate::txn::failed(&e.to_string())); // (as any error fails a transaction: `caught`)
                Err(e)
            }
        }
    }

    async fn do_describe_statement<C>(&self, _client: &mut C, stmt: &StoredStatement<Self::Statement>) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let sql = pg_dialect(&self.app.lake, &stmt.statement.sql, "");
        let out = crate::temp::SESSION.scope(Some(self.session.clone()), async { Ok(DescribeStatementResponse::new(self.types(&stmt.statement).await.to_vec(), self.describe(&sql, &stmt.statement.sql, &Format::UnifiedText).await?.0)) }).await; // (its temporary tables too)
        self.noted(&stmt.statement.sql, out).await
    }

    async fn do_describe_portal<C>(&self, _client: &mut C, portal: &Portal<Self::Statement>) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let statement = &portal.statement.statement;
        if let Some(fields) = statement.kept(&portal.result_column_format) {
            return Ok(DescribePortalResponse::new(fields));
        }
        let mut sent = statement.sql.clone(); // (what the history says it ran: the statement with its values, once bound)
        let out = crate::temp::SESSION.scope(Some(self.session.clone()), async {
            let inferred = self.types(statement).await;
            sent = bind(portal, inferred)?;
            let (fields, columns) = self.describe(&pg_dialect(&self.app.lake, &sent, ""), &sent, &portal.result_column_format).await?;
            statement.keep(columns);
            Ok(DescribePortalResponse::new(fields))
        })
        .await;
        self.noted(&sent, out).await
    }
}

impl Backend {
    /// A Describe that failed: the client gives up on the statement at that error, so it never runs, and it
    /// is recorded here as failed, as `caught` records every statement that runs (`audit::statement`: the
    /// history and the audit log). Metabase's and DBeaver's catalog queries failed this way, unrecorded.
    async fn noted<T>(&self, sql: &str, out: PgWireResult<T>) -> PgWireResult<T> {
        let Err(e) = out else { return out };
        let failed = crate::audit::statement(&self.app, sql, async { Err::<T, _>(e) });
        crate::temp::SESSION.scope(Some(self.session.clone()), crate::auth::WHO.scope(self.who(), failed)).await
    }

    /// A statement's `param_types`, worked out once (`Prepared`).
    async fn types<'a>(&self, statement: &'a Prepared) -> &'a [Type] {
        statement.found.types.get_or_init(|| async { self.param_types(&pg_dialect(&self.app.lake, &statement.sql, "")).await }).await
    }

    /// The types of `$1`, `$2`… as the query implies them (`WHERE id = $1`: id's type); text when
    /// it doesn't say.
    async fn param_types(&self, sql: &str) -> Vec<Type> {
        let mut types = self.inferred(sql).await;
        for i in crate::pg_types::params(&self.app.lake, sql).await {
            if let Some(t) = types.get_mut(i).filter(|t| **t == Type::TEXT) {
                *t = Type::UUID; // (a uuid's: asyncpg sends it as one, and `pg_values` takes it as Postgres's text or binary)
            }
        }
        types
    }

    /// `param_types`, but for uuids (`pg_types`), which are text to the planner.
    async fn inferred(&self, sql: &str) -> Vec<Type> {
        if let (None, Ok(Some(p))) = (crate::auth::limited(), crate::serve::point(&self.app.lake, sql).await) {
            return p.parameters(sql).iter().map(|t| pg_type(t, FieldFormat::Text).0).collect(); // (a key lookup's: its key's types, unplanned)
        }
        let n = (1..).take_while(|i| sql.contains(&format!("${i}"))).count();
        let mut types = vec![Type::TEXT; n]; // (text when the query doesn't say, as the doc above has it)
        if n > 0 && session_command(sql).is_none() && !crate::settings::is(sql) && crate::write::parse(sql).is_none() && Copy::of(sql).is_none() && !crate::script::is(sql) {
            let plan = async {
                let sql = crate::routines::expand(&self.app.lake, sql).await.ok()?;
                crate::query::sql(&self.session(&sql, "").await.ok()?, &crate::asof::rewrite(&sql).ok()?).await.ok()
            }.await;
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

    /// `sql` with the session's search path written in (`path.rs`). Only a session that set one:
    /// without, `routines::expand` still answers `current_schema()` (as `door` does for every door),
    /// and drivers' catalog queries go on as sent.
    async fn placed(&self, sql: String) -> PgWireResult<String> {
        match crate::path::current(&self.app.lake) {
            Some(_) => crate::path::door(&self.app.lake, &sql, &Default::default()).await.map_err(user_error),
            None => Ok(sql),
        }
    }

    /// The columns a statement returns (none for writes and session commands), and what of them a
    /// prepared statement may keep (`Columns`). `sql` is what is planned (rewritten for Postgres's catalog);
    /// `written` is the statement as the client sent it, whose uuids and column names are the answer's, as
    /// `run` has them (a rewrite turns `current_user` into a literal, and the literal has no name of its own).
    async fn describe(&self, sql: &str, written: &str, format: &Format) -> PgWireResult<(Vec<FieldInfo>, Columns)> {
        let sql = &self.placed(sql.to_string()).await?; // (the session's search path, as `run` has it)
        let written = self.placed(written.to_string()).await?;
        if let Some(cmd) = crate::cursors::of(sql) {
            // FETCH's columns are its cursor's (which a DECLARE may change); DECLARE, MOVE and CLOSE send no rows.
            let columns = crate::cursors::columns(&cmd.map_err(user_error)?).map_err(user_error)?;
            return Ok((columns.map_or(vec![], |s| described(&s, format)), Columns::Each));
        }
        if let Some((_, column)) = show_column(sql) {
            // A SHOW sends one row of one text column (`session_command`): Describe must say so, or pgjdbc and
            // prepared psycopg statements fail on a row with no column description.
            return Ok((vec![FieldInfo::new(column, None, None, Type::VARCHAR, FieldFormat::Text)], Columns::Each));
        }
        if crate::returning::of(sql).is_some() {
            // (its columns as `run` answers them: the same text, its macros and Postgres's forms written out)
            let marks = crate::pg_types::columns(&self.app.lake, &written).await; // (its uuids, from the statement as written)
            let sql = crate::vars::bound(sql).map_err(user_error)?;
            let sql = crate::routines::expand(&self.app.lake, &sql).await.map_err(user_error)?;
            if let Some(r) = crate::returning::of(&pg_dialect(&self.app.lake, &sql, "")) {
                let schema = crate::returning::columns(&self.app, &r).await.map_err(user_error)?;
                let schema = crate::pg_types::typed(&schema, &marks);
                return Ok((described(&schema, format), Columns::Kept { schema, given: false }));
            }
        }
        let point = match crate::auth::limited() {
            None => crate::serve::point(&self.app.lake, sql).await.ok().flatten().and_then(|p| Some(crate::pg_types::typed(&p.schema().ok()?, &p.types().into()))), // (a key lookup's columns, unplanned, first)
            Some(_) => None,
        };
        if point.is_none() && (session_command(sql).is_some() || crate::settings::is(sql) || crate::vars::change(sql).is_some() || crate::write::parse(sql).is_some() || Copy::of(sql).is_some() || crate::script::is(sql)) {
            return Ok((vec![], Columns::None)); // (a COPY's columns come with its data)
        }
        let marks = match point {
            Some(_) => Default::default(),
            None => crate::pg_types::columns(&self.app.lake, &written).await, // (its uuids, from the statement as written, as `run` has them)
        };
        let bound = crate::vars::bound(sql).map_err(user_error)?; // (`$day`: the connection's variable)
        let bound = crate::routines::expand(&self.app.lake, &bound).await.map_err(user_error)?; // (macros, DuckDB's forms: as `run` has them)
        let sql: &str = &bound;
        let schema = match point {
            Some(s) => s,
            None => {
                let probe = (1..).take_while(|i| sql.contains(&format!("${i}"))).fold(sql.to_string(), |q, i| q.replace(&format!("${i}"), "NULL"));
                crate::pg_types::typed(&self.schema(&probe, "").await?, &marks)
            }
        };
        Ok((described(&schema, format), Columns::Kept { schema, given: false }))
    }
}

/// A DECLARE that ran makes a portal of the cursor's name, as Postgres does: psycopg describes it right
/// after DECLARE to learn a named cursor's columns, and pgwire answers only for a portal it holds. The
/// portal's statement is the cursor's own FETCH, which Describe answers with the cursor's columns, so it
/// is only as good as the cursor: a cursor that is gone describes as an error (34000).
fn cursor_portal<C>(client: &C, cursor: String)
where
    C: ClientPortalStore,
    C::PortalStore: PortalStore<Statement = Prepared>,
{
    if crate::cursors::columns(&crate::cursors::Cmd::Fetch { name: cursor.clone(), rows: None }).is_err() {
        return; // (a DECLARE that failed: no cursor of that name)
    }
    let fetch = format!("FETCH ALL FROM \"{cursor}\"");
    let statement = Arc::new(StoredStatement::new(cursor.clone(), Prepared::new(fetch), vec![])); // (pgwire's struct is non-exhaustive: its constructor)
    client.portal_store().put_portal(Arc::new(Portal::new_cursor(cursor, statement)));
}

/// A result's columns as Postgres describes them: each one's type, in the format the client asked for.
fn described(schema: &Schema, format: &Format) -> Vec<FieldInfo> {
    schema.fields().iter().enumerate().map(|(i, f)| FieldInfo::new(crate::pg_names::shown(f).to_string(), None, None, field_type(f, format.format_for(i)).0, format.format_for(i))).collect()
}

/// `PONDRA_TRACE_PG=1`: where a Postgres statement's time goes, on stderr.
fn trace() -> bool {
    static ON: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| crate::env::is_set("PONDRA_TRACE_PG"));
    *ON
}
