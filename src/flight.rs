//! Arrow Flight and Flight SQL (`--flight 0.0.0.0:8815`): Arrow in and out over gRPC, nothing
//! parsed or converted on the way.
//!
//! - **Flight SQL** for ADBC, JDBC and other Flight SQL drivers: queries (`CommandStatementQuery`),
//!   writes (`CommandStatementUpdate`, or a write sent as a query), bulk ingest
//!   (`CommandStatementIngest`: ADBC's `adbc_ingest`), catalogs, schemas and tables.
//! - **Plain Flight** for pyarrow and any Flight client:
//!   - `DoPut` to a path descriptor `[table]`, or `[table, producer, first seq]` for exactly-once:
//!     batch i gets seq `first + i`, and a retried stream is applied once. Batches are appended
//!     through the log, pipelined, each answered with its ack as JSON in `app_metadata`.
//!   - `DoGet` with a JSON ticket: `{"sql": "SELECT …"}`, or a table's log as a columnar stream
//!     (as Fluss serves its log): `{"table": "events", "after": 0, "columns": ["user", "amount"],
//!     "follow": true}` — the rows committed after segment `after`, only those columns, and with
//!     `follow` every new commit as it lands. After each commit's rows comes a message with only
//!     `app_metadata` `{"after": N}`: where to resume.
//!   - `GetFlightInfo` for the same JSON, and `ListFlights` (the tables).
//! - **Transactions** (Flight SQL's own): `BeginTransaction` gives a `transaction_id`; statements
//!   that carry it (queries, updates) run in that transaction's session, and `EndTransaction` commits
//!   or rolls it back. Bulk ingest inside one is refused by name for now.
//! - **Tokens:** `authorization: Bearer <token>`, or a handshake with basic auth whose password is
//!   the token (what ADBC and pyarrow's `authenticate_basic_token` send).
use crate::auth::Role;
use crate::server::App;
use crate::store::{table_key, TableMeta};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{Any, ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest, CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate, EndTransaction, ProstMessageExt, SqlInfo, SqlSupportedTransaction, TicketStatementQuery};
use arrow_flight::{Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use serde::Deserialize;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use tonic::{Request, Response, Status, Streaming};

type Out<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

pub async fn serve(app: App, addr: String) -> anyhow::Result<()> {
    // (a message up to 1 GiB, as HTTP takes a batch, but only from a caller `Gate` has signed in)
    let service = FlightServiceServer::new(Door(Sql(app.clone()))).max_decoding_message_size(1 << 30).max_encoding_message_size(1 << 30);
    let doors = crate::tls::Doors::bind(&addr, crate::tls::Door::Flight).await?; // (TLS too: `tls.rs`)
    tonic::transport::Server::builder().add_service(Gate { app, inner: service }).serve_with_incoming(doors.stream()).await?;
    Ok(())
}

/// The calls a caller not signed in may make: the handshake that signs it in, and the list of actions.
const UNSIGNED: [&str; 2] = ["/Handshake", "/ListActions"];

/// Signs every call in from its headers before its body is read, as HTTP's guard does. Without it,
/// anyone who reached the port could make the node take in a message of 1 GiB before saying who they
/// were. A caller not signed in is refused at once, but for `UNSIGNED`, whose body may be 64 KB at
/// most (as HTTP's `/login`); the principal rides with the call to `caller`.
#[derive(Clone)]
struct Gate {
    app: App,
    inner: FlightServiceServer<Door>,
}

impl tonic::server::NamedService for Gate {
    const NAME: &'static str = <FlightServiceServer<Door> as tonic::server::NamedService>::NAME;
}

impl tower::Service<http::Request<tonic::body::Body>> for Gate {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> { std::task::Poll::Ready(Ok(())) }

    fn call(&mut self, mut req: http::Request<tonic::body::Body>) -> Self::Future {
        let (app, mut inner) = (self.app.clone(), self.inner.clone());
        Box::pin(async move {
            let header = req.headers().get("authorization").and_then(|v| v.to_str().ok()).map(str::to_string);
            let peer = req.extensions().get::<crate::tls::Peer>().copied();
            let who = match sign_in(&app, header.as_deref(), peer).await {
                Ok(who) => who,
                Err(refused) => return Ok(refused.into_http()),
            };
            if who.role == Role::None {
                if !UNSIGNED.iter().any(|call| req.uri().path().ends_with(call)) {
                    return Ok(refused("28000", SIGN_IN).into_http());
                }
                req = req.map(|body| tonic::body::Body::new(http_body_util::Limited::new(body, 64 << 10)));
            }
            req.extensions_mut().insert(who);
            tower::Service::call(&mut inner, req).await
        })
    }
}

const SIGN_IN: &str = "sign in: a token, or a user's name and password (handshake)";

/// The gRPC status nearest a SQLSTATE (Flight SQL's errors).
fn grpc(code: &str) -> tonic::Code {
    match code {
        "40001" => tonic::Code::Aborted,
        "42501" => tonic::Code::PermissionDenied,
        "28P01" | "28000" => tonic::Code::Unauthenticated,
        "42P01" | "42703" | "42883" | "3F000" | "34000" => tonic::Code::NotFound,
        "42P07" | "23505" => tonic::Code::AlreadyExists,
        "57014" => tonic::Code::DeadlineExceeded,
        "53100" | "53200" | "53300" => tonic::Code::ResourceExhausted,
        "0A000" => tonic::Code::Unimplemented,
        "23514" | "23502" | "25006" | "25P01" | "25P02" | "25P03" => tonic::Code::FailedPrecondition, // (a transaction that isn't open, failed, or idle-ended)
        c if c.starts_with("42") || c.starts_with("22") || c.starts_with("21") => tonic::Code::InvalidArgument,
        _ => tonic::Code::Internal,
    }
}

/// An error as a gRPC status: the nearest code to its SQLSTATE, which rides along in `x-pondra-sqlstate`.
fn status(e: impl Into<anyhow::Error>) -> Status {
    let e = e.into();
    let code = crate::codes::of(&e);
    let mut s = Status::new(grpc(code), crate::ext::said(&e));
    s.metadata_mut().insert("x-pondra-sqlstate", tonic::metadata::MetadataValue::from_static(code));
    s
}

/// A refusal with its SQLSTATE, as a gRPC status (`status`).
fn refused(code: &'static str, message: impl Into<String>) -> Status { status(crate::codes::coded(code, message)) }

/// A permission check's refusal (`status`): 42501, unless it carries a code of its own.
fn denied(e: impl Into<anyhow::Error>) -> Status {
    let e = e.into();
    status(match crate::codes::of(&e) { "XX000" => crate::codes::coded("42501", crate::ext::said(&e)), _ => e })
}

/// The caller's role (who it is: `Door` works it out); refused below `need`.
fn allowed<T>(_: &App, _: &Request<T>, need: Role) -> Result<Role, Status> {
    let role = crate::auth::current().map_or(Role::None, |p| p.role);
    match role {
        _ if role >= need => Ok(role),
        Role::None => Err(refused("28000", SIGN_IN)),
        _ => Err(refused("42501", "this needs more rights than this token's or user's")),
    }
}

/// Who a call is, from its `authorization`: a token, or a user's token or session (`Bearer`:
/// `users::who`); or a name and password (`Basic`: what ADBC's and pyarrow's handshakes send). With
/// nothing that needs a sign-in, anyone is an admin. A wrong password is an error, said at once.
async fn sign_in(app: &App, header: Option<&str>, peer: Option<crate::tls::Peer>) -> Result<crate::auth::Principal, Status> {
    let from = peer.map(|p| p.addr);
    let who = match header.and_then(basic) {
        Some((user, password)) => password_sign_in(app, &user, &password, peer).await?,
        None => crate::users::who(&app.lake, &app.auth, header).await,
    };
    let who = match who {
        Some(p) => p,
        None if app.open().await => crate::auth::Principal::of(Role::Admin),
        None => {
            // (an identity provider's token says why it was refused, as HTTP's 401 does)
            let why = match header.and_then(|h| h.strip_prefix("Bearer ")) {
                Some(t) => crate::sso::why(&app.lake, t.trim()).await,
                None => None,
            };
            if header.is_some() {
                crate::audit::refused(app, "", "flight", from, "sign in", why.as_deref().unwrap_or("wrong token"));
            }
            if let Some(why) = why {
                return Err(refused("28000", why));
            }
            crate::auth::Principal::of(Role::None)
        }
    };
    Ok(who.at("flight", from))
}

/// A `Basic` header's name and password (padding optional: ADBC's driver sends none).
fn basic(header: &str) -> Option<(String, String)> {
    use base64::Engine;
    let lenient = base64::engine::GeneralPurpose::new(&base64::alphabet::STANDARD, base64::engine::GeneralPurposeConfig::new().with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent));
    let up = String::from_utf8(lenient.decode(header.strip_prefix("Basic ")?).ok()?).ok()?;
    up.split_once(':').map(|(u, p)| (u.to_string(), p.to_string()))
}

/// A name and password: a token as the password whatever the name, else a user's password (or
/// `pt_` token), checked as every door checks one. A password crosses only TLS from another machine
/// (`tls.rs`; no peer at all refuses one), and failed sign-ins are counted per address and name
/// (`auth::guessing`), each costing 400 ms. None on a node nothing locks.
async fn password_sign_in(app: &App, user: &str, password: &str, peer: Option<crate::tls::Peer>) -> Result<Option<crate::auth::Principal>, Status> {
    let role = app.auth.token_role(password);
    if role > Role::None {
        return Ok(Some(crate::auth::Principal::token(role)));
    }
    if app.open().await {
        return Ok(None);
    }
    let from = peer.map(|p| p.addr);
    let ip = from.map(|a| a.ip());
    let by_token = crate::users::is_token(user, password);
    if !by_token && !peer.is_some_and(|p| p.may_send_password()) {
        return Err(refused("28000", crate::tls::PASSWORD_PLAIN));
    }
    if let Some(wait) = crate::auth::guessing(ip, user) {
        let why = crate::auth::guessing_says(user, wait);
        crate::audit::refused(app, user, "flight", from, "sign in", &why);
        tokio::time::sleep(std::time::Duration::from_millis(400)).await; // (as a wrong guess does)
        return Err(refused("28000", why));
    }
    match crate::users::sign_in(&app.lake, &app.auth, user, password).await {
        Some(p) if p.role > Role::None => Ok(Some(p)),
        _ => {
            if !by_token {
                crate::auth::guessed(ip, user); // (a token isn't guessed: it is random)
            }
            // (a provider's refusal says why, 28000; a wrong password is 28P01, as Postgres says it)
            let (code, why) = match if by_token { crate::sso::why(&app.lake, password).await } else { None } {
                Some(why) => ("28000", why),
                None => ("28P01", "wrong user name or password".to_string()),
            };
            crate::audit::refused(app, user, "flight", from, "sign in", &why);
            tokio::time::sleep(std::time::Duration::from_millis(400)).await; // (a guess costs time, as HTTP's and Postgres's do)
            Err(refused(code, why))
        }
    }
}

/// Who a call is: signed in by `Gate` before its body was read.
fn caller<T>(req: &Request<T>) -> crate::auth::Principal {
    req.extensions().get::<crate::auth::Principal>().cloned().unwrap_or_else(|| crate::auth::Principal::of(Role::None))
}

/// Batches as a Flight stream (the schema first, so an empty result still has columns).
fn send(schema: SchemaRef, batches: Vec<RecordBatch>) -> Out<FlightData> {
    let stream = futures::stream::iter(batches.into_iter().map(|b| Ok(crate::answer::compact(b))));
    Box::pin(FlightDataEncoderBuilder::new().with_schema(schema).build(stream).map_err(Status::from))
}

/// The schema Flight announces for a statement and streams its batches in: each field as the encoder sends
/// it (`sent_field`), and every column nullable. The plan's nullability and the run's differ where the
/// optimizer folds an expression (`upper('x')` becomes a literal, never NULL), and where the plan says NOT
/// NULL the run gives NULL (a scalar subquery takes its inner field's nullability: `SELECT (SELECT 1 WHERE
/// false)`); table scans are nullable anyway (`answer::schema`). Clients compare the two, so both say this.
fn announced(schema: &Schema) -> Schema {
    let fields = schema.fields().iter().map(|f| sent_field(f).with_nullable(true)).collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// A field as the encoder sends it (arrow-flight's `prepare_field_for_flight`, in its default, hydrated form):
/// a dictionary is its values' type, at any depth. Its name, nullability and metadata stay as they are.
fn sent_field(f: &Field) -> Field { Field::new(f.name(), sent_type(f.data_type()), f.is_nullable()).with_metadata(f.metadata().clone()) }

fn sent_type(t: &DataType) -> DataType {
    match t {
        DataType::Dictionary(_, values) => sent_type(values),
        DataType::List(f) => DataType::List(Arc::new(sent_field(f))),
        DataType::LargeList(f) => DataType::LargeList(Arc::new(sent_field(f))),
        DataType::ListView(f) => DataType::ListView(Arc::new(sent_field(f))),
        DataType::LargeListView(f) => DataType::LargeListView(Arc::new(sent_field(f))),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(Arc::new(sent_field(f)), *n),
        DataType::Map(f, sorted) => DataType::Map(Arc::new(sent_field(f)), *sorted),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(|f| sent_field(f)).collect()),
        DataType::Union(fields, mode) => DataType::Union(fields.iter().map(|(id, f)| (id, Arc::new(sent_field(f)))).collect(), *mode),
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(run_ends.clone(), Arc::new(sent_field(values))),
        other => other.clone(),
    }
}

/// The schema a statement's batches are announced with: `announced` of the first batch's, or no columns.
fn first_announced(batches: &[RecordBatch]) -> Schema { announced(&batches.first().map_or_else(|| Arc::new(Schema::empty()), |b| b.schema())) }

/// Each batch taken to the announced schema: a column cast to its field's type (a dictionary to its values,
/// as the encoder sends it), so `RecordBatch::try_new` takes it and the stream says what the info said.
fn conform(schema: &SchemaRef, batches: Vec<RecordBatch>) -> anyhow::Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|b| -> anyhow::Result<RecordBatch> {
            let columns = b
                .columns()
                .iter()
                .zip(schema.fields())
                .map(|(c, f)| match c.data_type() == f.data_type() {
                    true => Ok(c.clone()),
                    false => datafusion::arrow::compute::cast(c, f.data_type()),
                })
                .collect::<Result<Vec<datafusion::arrow::array::ArrayRef>, datafusion::arrow::error::ArrowError>>()?;
            Ok(RecordBatch::try_new(schema.clone(), columns)?)
        })
        .collect()
}

/// Run a query (spread over the cluster when it pays), with its schema. The batches take that schema,
/// so the stream says what its header and the flight's info say.
async fn query(app: &App, sql: &str) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)> {
    let sql = &crate::routines::expand(&app.lake, sql).await?; // (macros: ADR-023)
    let batches = crate::audit::statement(app, sql, app.query(sql, None)).await?;
    let schema = Arc::new(if batches.is_empty() { planned(app, sql).await? } else { first_announced(&batches) });
    let batches = conform(&schema, batches)?;
    Ok((schema, batches))
}

/// The columns a query will answer with, for its flight's info: expanded as every door expands SQL
/// (macros, `FROM t`, `pondra.git_status`), then planned, not run.
async fn plan_schema(app: &App, sql: &str) -> anyhow::Result<Schema> { planned(app, &crate::routines::expand(&app.lake, sql).await?).await }

async fn planned(app: &App, sql: &str) -> anyhow::Result<Schema> {
    let ctx = crate::query::session(&app.lake, sql, "").await?;
    let df = crate::engine::sql(&ctx, &crate::sys::planned(sql)?).await?;
    Ok(announced(crate::query::run_schema(&ctx, df.logical_plan())?.as_arrow()))
}

/// A write's answer, as its ticket and its flight's info give it: one row, `rows`, the count written.
fn rows_schema() -> Schema { Schema::new(vec![Field::new("rows", DataType::Int64, false)]) }

/// A statement's answer, as its ticket names it: a CALL's kept answer, a write's row count, or a
/// query run now.
async fn answer(app: &App, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
    if let Some(id) = sql.strip_prefix("kept:") {
        let Some((batches, _)) = KEPT.lock().unwrap().remove(id) else { return Err(refused("34000", "that answer was read already, or is gone")) };
        let schema = Arc::new(first_announced(&batches));
        return Ok((schema.clone(), conform(&schema, batches).map_err(status)?));
    }
    if let Some(n) = sql.strip_prefix("rows:").and_then(|n| n.parse::<i64>().ok()) {
        let schema = Arc::new(announced(&rows_schema()));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![n]))]).map_err(status)?;
        return Ok((schema, vec![batch]));
    }
    query(app, sql).await.map_err(status)
}

/// A statement's work, as a request's: in the transaction the call names (`txn`, checked first), as its
/// caller (`auth::carried`, which a spawned task would lose), and through `panics::work`, where the memory
/// watch (`oom.rs`) may stop a query that grew the node, and a long one moves to the queries' runtime
/// (invariant 231). A statement the node stopped for memory fails its transaction, as on HTTP and Postgres.
async fn worked<T: Send + 'static>(txn: Option<&[u8]>, f: impl std::future::Future<Output = Result<T, Status>> + Send + 'static) -> Result<T, Status> {
    let session = txn.map(in_txn).transpose()?;
    let failing = session.clone();
    let f = crate::auth::carried(in_session(session, f));
    match crate::panics::work(true, f).await {
        Ok(out) => out,
        Err(m) => {
            if m == crate::oom::OOM {
                if let Some(s) = failing {
                    crate::temp::SESSION.sync_scope(Some(s), || crate::txn::failed(&m));
                }
            }
            Err(status(anyhow::anyhow!(m)))
        }
    }
}

crate::per_node!(
    /// A CALL's answer, run when its flight was asked for, until its ticket reads it (10 minutes).
    static KEPT: std::sync::Mutex<std::collections::HashMap<String, (Vec<RecordBatch>, std::time::Instant)>> = Default::default());

/// A write sent as SQL: a CREATE, INSERT, UPDATE, DELETE or ALTER (or a CALL). Returns the rows written.
async fn write(app: &App, role: Role, sql: &str) -> Result<i64, Status> {
    no_sql_transaction(sql)?;
    if crate::returning::of(sql).is_some() {
        // (sent as an update, a write with RETURNING answers how many rows it wrote)
        let who = crate::routines::Who { role, files: false, depth: 0 };
        return match crate::routines::one(app, sql, who, None).await.map_err(status)? {
            crate::routines::Outcome::Rows(b) => Ok(b.iter().map(|b| b.num_rows() as i64).sum()),
            crate::routines::Outcome::Done(_) => Ok(0),
        };
    }
    if crate::routines::runs_procedure(sql) {
        let who = crate::routines::Who { role, files: false, depth: 0 };
        crate::routines::one(app, sql, who, None).await.map_err(status)?;
        return Ok(0);
    }
    let sql = crate::routines::expand(&app.lake, sql).await.map_err(status)?;
    let stmt = crate::write::parse(&sql).ok_or_else(|| refused("22023", "not a write statement"))?;
    let done = crate::audit::statement(app, &sql, async {
        app.auth.allows(role, &stmt).map_err(denied)?;
        crate::write::on_node(app, stmt, None).await.map_err(status)
    });
    Ok(done.await?["rows"].as_i64().unwrap_or(0))
}

/// BEGIN, COMMIT, ROLLBACK and SAVEPOINT in SQL are refused, with or without a transaction id: a
/// Flight SQL transaction is begun and ended by its own actions, as the protocol has it.
fn no_sql_transaction(sql: &str) -> Result<(), Status> {
    match crate::txn::control(sql) {
        Some(_) => Err(status(crate::codes::coded("0A000", "Flight SQL's transactions are the protocol's own: BeginTransaction (ADBC: autocommit off), then EndTransaction (commit or rollback); BEGIN, COMMIT and ROLLBACK aren't run as SQL"))),
        None => Ok(()),
    }
}

/// The session of a transaction a Flight SQL call names (`transaction_id`, from BeginTransaction).
/// One with no transaction open (ended, timed out, or never begun) is refused by name, never run
/// outside a transaction.
fn in_txn(id: &[u8]) -> Result<String, Status> {
    let session = crate::temp::id(&String::from_utf8_lossy(id));
    match crate::txn::held(&session) {
        true => Ok(session),
        false => Err(status(crate::codes::coded("25P01", "no transaction is open under that transaction_id: it was ended, or never begun"))),
    }
}

/// `f`, in the session of the transaction the call names, if it names one: its reads see the
/// transaction's snapshot and its own writes, as a POST /sql statement sent with its session does.
/// Without a transaction id, `f` runs as it always has.
async fn within<T>(txn: Option<&[u8]>, f: impl std::future::Future<Output = Result<T, Status>>) -> Result<T, Status> {
    in_session(txn.map(in_txn).transpose()?, f).await
}

/// `f` in a transaction's session, when there is one (`within` and `worked` both).
async fn in_session<F: std::future::Future>(session: Option<String>, f: F) -> F::Output {
    match session {
        Some(s) => crate::temp::SESSION.scope(Some(s), f).await,
        None => f.await,
    }
}

/// A statement's ticket: its SQL, and the transaction it runs in first (`txn:<id>:`), if it has
/// one. A transaction id never has a colon (`fl-` and hex), so the first colon ends it.
fn handle(sql: &str, txn: Option<&[u8]>) -> Vec<u8> {
    match txn {
        Some(id) => format!("txn:{}:{sql}", String::from_utf8_lossy(id)).into_bytes(),
        None => sql.as_bytes().to_vec(),
    }
}

/// A ticket's transaction and SQL (`handle`'s form; the rest is as it was).
fn unhandle(handle: &[u8]) -> (Option<&[u8]>, &[u8]) {
    let split = handle.strip_prefix(b"txn:").and_then(|rest| rest.iter().position(|b| *b == b':').map(|i| (&rest[..i], &rest[i + 1..])));
    match split {
        Some((id, sql)) => (Some(id), sql),
        None => (None, handle),
    }
}

/// Appends batches to `table` through the log, pipelined: each is queued as it arrives and
/// answered with its ack when committed. `producer` + `first`: batch i is seq `first + i`.
fn append(app: &App, table: String, producer: String, first: u64, batches: impl Stream<Item = Result<RecordBatch, Status>> + Send + 'static) -> Result<Out<PutResult>, Status> {
    let log = app.log.clone().ok_or_else(|| refused("25006", "read-only node"))?;
    if !producer.is_empty() && first == 0 {
        return Err(refused("22023", crate::log::SEQ_FROM_1)); // (0 is "nothing yet": its batch would be taken for a retry)
    }
    let (lake, again, table2) = (app.lake.clone(), log.clone(), table.clone());
    let who = crate::auth::current(); // (the caller, for its grants in the task below: a task keeps no task-local)
    type Queued = Pin<Box<dyn std::future::Future<Output = anyhow::Result<crate::log::Ack>> + Send>>;
    let (acks_tx, mut acks_rx) = tokio::sync::mpsc::channel::<Result<(Queued, RecordBatch, crate::log::Src), Status>>(256);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel(); // (acks are small; a client may read them only at the end)
    crate::panics::task(async move {
        let mut batches = std::pin::pin!(batches);
        let meta: Option<TableMeta> = lake.cat.get(&table_key(&table)).await.ok().flatten();
        let Some((meta, schema)) = meta.and_then(|m| Some((m.clone(), crate::answer::schema(&m.logical_columns()).ok()?))) else { // (SQL's names: ADR-022)
            let _ = acks_tx.send(Err(refused("42P01", format!("no table {table}")))).await;
            return;
        };
        // (a column the batch leaves out: its DEFAULT)
        let (m, sch, lk) = (&meta, &schema, &lake);
        let conformed = |b: RecordBatch| async move {
            let given: Vec<String> = b.schema().fields().iter().map(|f| f.name().clone()).collect();
            let rows = crate::query::conform(&b, sch)?;
            crate::defaults::fill(lk, m, rows, |c| (!given.iter().any(|g| g == c)).then(|| crate::defaults::all(b.num_rows()))).await
        };
        // (a keyed table's rows replace their keys', and rows marked _deleted delete them: S11)
        let named = crate::auth::catalog_name(&table);
        let load = |b: &RecordBatch| match &who {
            Some(w) => crate::auth::WHO.sync_scope(w.clone(), || crate::auth::check_load(m, &named, b).map_err(denied)), // (said as the writer's scope says it: `shows_code`)
            None => Ok(()),
        };
        let mut i = 0;
        while let Some(b) = batches.next().await {
            let b = match b {
                Ok(b) => conformed(b).await.map_err(status).and_then(|b| load(&b).map(|_| b)),
                Err(e) => Err(e),
            };
            let queued = match b {
                Ok(b) => {
                    let (seq, prev) = if producer.is_empty() { (0, None) } else { (first + i, (i > 0).then(|| first + i - 1)) };
                    let src = crate::log::Src { producer: producer.clone(), seq, prev };
                    log.queue(table.clone(), src.clone(), b.clone()).await.map(|f| (Box::pin(f) as Queued, b, src)).map_err(status)
                }
                Err(e) => Err(e),
            };
            let stop = queued.is_err();
            if acks_tx.send(queued).await.is_err() || stop {
                break;
            }
            i += 1;
        }
    });
    let (log, table) = (again, table2);
    crate::panics::task(async move {
        while let Some(queued) = acks_rx.recv().await {
            let acked = match queued {
                Ok((ack, batch, src)) => {
                    // (overtaken by the batch after it on the way to the leader, it goes again: `Log::sent_again`)
                    let ack = log.sent_again(ack.await, &table, &src, &batch).await.map_err(status);
                    ack.map(|a| {
                        let mut a = serde_json::to_value(a).unwrap_or_default();
                        a["rows"] = batch.num_rows().into();
                        PutResult { app_metadata: a.to_string().into_bytes().into() }
                    })
                }
                Err(e) => Err(e),
            };
            let stop = acked.is_err();
            if tx.send(acked).is_err() || stop {
                break;
            }
        }
    });
    Ok(Box::pin(futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|r| (r, rx)) })))
}

/// A plain Flight ticket or command.
#[derive(Deserialize)]
struct Json {
    sql: Option<String>,
    table: Option<String>,
    after: Option<u64>,
    columns: Option<Vec<String>>,
    #[serde(default = "yes")]
    follow: bool,
}

fn yes() -> bool { true }

impl Json {
    fn parse(b: &[u8]) -> Option<Json> { b.first().filter(|c| **c == b'{').and_then(|_| serde_json::from_slice(b).ok()) }
}

/// A table's log as Flight data, from segment `after`: each commit's rows (just `columns`), then
/// a message with `{"after": N}`.
async fn log_stream(app: App, table: String, after: Option<u64>, columns: Option<Vec<String>>, follow: bool) -> Result<Out<FlightData>, Status> {
    let meta: TableMeta = app.lake.cat.get(&table_key(&table)).await.map_err(status)?.ok_or_else(|| refused("42P01", format!("no table {table}")))?;
    let full = crate::answer::schema(&meta.logical_columns()).map_err(status)?;
    let pick: Vec<usize> = match &columns {
        Some(cols) => cols.iter().map(|c| full.index_of(c).map_err(|_| refused("42703", format!("no column {c}")))).collect::<Result<_, _>>()?,
        None => (0..full.fields().len()).collect(),
    };
    let schema = Arc::new(full.project(&pick).map_err(status)?);
    let after = after.unwrap_or_else(|| app.lake.visible());
    let hwm = app.lake.hwm.subscribe();
    let first = FlightDataEncoderBuilder::new().with_schema(schema.clone()).build(futures::stream::empty()).map_err(Status::from);
    let chunks = futures::stream::unfold((app, hwm, after, false), move |(app, mut hwm, mut after, done)| {
        let (table, schema, pick) = (table.clone(), schema.clone(), pick.clone());
        async move {
            loop {
                if done {
                    return None;
                }
                hwm.borrow_and_update();
                let now = app.lake.visible();
                if now > after || !follow {
                    let rows = crate::query::tail(&app.lake, &table, after, Some(now), false).await.map_err(status);
                    if follow && rows.as_ref().is_ok_and(|r| r.iter().all(|b| b.num_rows() == 0)) {
                        after = now; // (commits to other tables: nothing to send while following)
                        continue;
                    }
                    let chunk = rows.and_then(|rows| {
                        let rows = rows.iter().map(|b| b.project(&pick).map(crate::answer::compact)).collect::<Result<Vec<_>, _>>().map_err(status)?;
                        let mut data: Vec<Result<FlightData, Status>> = arrow_flight::utils::batches_to_flight_data(&schema, rows).map_err(status)?.into_iter().skip(1).map(Ok).collect(); // (the schema went first)
                        data.push(Ok(FlightData { app_metadata: format!("{{\"after\":{now}}}").into_bytes().into(), ..Default::default() }));
                        Ok(data)
                    });
                    let chunk = chunk.unwrap_or_else(|e| vec![Err(e)]);
                    return Some((futures::stream::iter(chunk), (app, hwm, now, !follow)));
                }
                hwm.changed().await.ok()?;
            }
        }
    });
    Ok(Box::pin(first.chain(chunks.flatten())))
}

/// The server's answers to `GetSqlInfo`.
static INFO: LazyLock<SqlInfoData> = LazyLock::new(|| {
    let mut b = SqlInfoDataBuilder::new();
    b.append(SqlInfo::FlightSqlServerName, "Pondra");
    b.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    b.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    b.append(SqlInfo::FlightSqlServerReadOnly, false);
    b.append(SqlInfo::FlightSqlServerSql, true);
    b.append(SqlInfo::FlightSqlServerTransaction, SqlSupportedTransaction::Transaction as i32); // (transactions: BeginTransaction, EndTransaction; ADBC's autocommit off)
    b.build().expect("static SQL info")
});

/// A statement's FlightInfo: its schema, and a ticket to fetch it.
/// The kinds of table `CommandGetTables` lists.
fn table_types() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![datafusion::arrow::datatypes::Field::new("table_type", datafusion::arrow::datatypes::DataType::Utf8, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(datafusion::arrow::array::StringArray::from(vec!["TABLE", "VIEW"]))]).expect("a batch")
}

fn info(schema: &Schema, ticket: Vec<u8>, descriptor: FlightDescriptor) -> Result<Response<FlightInfo>, Status> {
    let info = FlightInfo::new().try_with_schema(schema).map_err(status)?.with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket))).with_descriptor(descriptor);
    Ok(Response::new(info))
}

/// The Flight SQL service.
struct Sql(App);

#[tonic::async_trait]
impl FlightSqlService for Sql {
    type FlightService = Sql;

    async fn do_handshake(&self, req: Request<Streaming<HandshakeRequest>>) -> Result<Response<Out<HandshakeResponse>>, Status> {
        // (signed in by `Gate` already; here a token is handed back: the password when it is one, or
        // anything on a node nothing locks; a session when a user signed in)
        let password = req.metadata().get("authorization").and_then(|v| v.to_str().ok()).and_then(basic).map(|(_, p)| p);
        let Some(password) = password else { return Err(refused("28P01", "wrong user name or password")) };
        let open = self.0.open().await;
        let token = match self.0.auth.token_role(&password) {
            Role::None if !open => match caller(&req) {
                who if who.role > Role::None => match crate::sso::check(&self.0.lake, &password).await {
                    Some(Ok(c)) => crate::users::session_of(&self.0.lake, &c).await, // (a provider's: its session names it, ADR-074 §5)
                    _ => crate::users::session(&self.0.lake, &who.name).await,
                }
                .map_err(status)?
                .0,
                _ => return Err(refused("28P01", "wrong user name or password")),
            },
            _ => password,
        };
        let mut res: Response<Out<HandshakeResponse>> = Response::new(Box::pin(futures::stream::iter([Ok(HandshakeResponse { protocol_version: 0, payload: token.clone().into_bytes().into() })])));
        if let Ok(v) = format!("Bearer {token}").parse() {
            res.metadata_mut().insert("authorization", v);
        }
        Ok(res)
    }

    async fn get_flight_info_statement(&self, q: CommandStatementQuery, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        let role = allowed(&self.0, &req, Role::Read)?;
        no_sql_transaction(&q.query)?;
        let txn = q.transaction_id.as_deref(); // (the transaction the statement runs in, if it names one)
        if crate::routines::runs_procedure(&q.query) || crate::returning::of(&q.query).is_some() {
            // A CALL (or DO), or a write with RETURNING: run now, as every door runs one; the ticket reads back what it answered.
            let who = crate::routines::Who { role, files: false, depth: 0 };
            let (app, sql) = (self.0.clone(), q.query.clone());
            let out = worked(txn, async move { crate::routines::one(&app, &sql, who, None).await.map_err(status) }).await?;
            let batches = match out {
                crate::routines::Outcome::Rows(b) => b,
                crate::routines::Outcome::Done(_) => vec![RecordBatch::try_from_iter([("rows", Arc::new(datafusion::arrow::array::Int64Array::from(vec![0])) as _)]).map_err(status)?],
            };
            let schema = first_announced(&batches);
            let id = uuid::Uuid::new_v4().simple().to_string();
            let mut kept = KEPT.lock().unwrap();
            kept.retain(|_, (_, at)| at.elapsed() < std::time::Duration::from_secs(600));
            kept.insert(id.clone(), (batches, std::time::Instant::now()));
            let ticket = TicketStatementQuery { statement_handle: format!("kept:{id}").into_bytes().into() };
            return info(&schema, ticket.as_any().encode_to_vec(), req.into_inner());
        }
        if crate::write::parse(&q.query).is_some() {
            // A write sent as a query (DB-API's `execute`): done now; the ticket reads back the count.
            let (app, sql) = (self.0.clone(), q.query.clone());
            let rows = worked(txn, async move { write(&app, role, &sql).await }).await?;
            let ticket = TicketStatementQuery { statement_handle: format!("rows:{rows}").into_bytes().into() };
            return info(&announced(&rows_schema()), ticket.as_any().encode_to_vec(), req.into_inner());
        }
        let schema = within(txn, async { plan_schema(&self.0, &q.query).await.map_err(status) }).await?;
        let ticket = TicketStatementQuery { statement_handle: handle(&q.query, txn).into() };
        info(&schema, ticket.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_statement(&self, t: TicketStatementQuery, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let (txn, sql) = unhandle(&t.statement_handle);
        let sql = String::from_utf8(sql.to_vec()).map_err(status)?;
        let app = self.0.clone();
        let (schema, batches) = worked(txn, async move { answer(&app, &sql).await }).await?;
        Ok(Response::new(send(schema, batches)))
    }

    async fn do_action_begin_transaction(&self, _: ActionBeginTransactionRequest, req: Request<Action>) -> Result<ActionBeginTransactionResult, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let id = format!("fl-{}", uuid::Uuid::new_v4().simple()); // (the client's handle; its session is the caller's own, named by it)
        crate::temp::with(&crate::temp::id(&id), false, |_| Ok(())).map_err(status)?; // (held as a session: an idle one ends, its transaction rolled back, as `temp::reap` does)
        crate::temp::SESSION.scope(Some(crate::temp::id(&id)), crate::txn::command(&self.0, "BEGIN", "BEGIN")).await.map_err(status)?;
        Ok(ActionBeginTransactionResult { transaction_id: id.into_bytes().into() })
    }

    async fn do_action_end_transaction(&self, q: ActionEndTransactionRequest, req: Request<Action>) -> Result<(), Status> {
        allowed(&self.0, &req, Role::Read)?;
        let word = match EndTransaction::try_from(q.action) {
            Ok(EndTransaction::Commit) => "COMMIT",
            Ok(EndTransaction::Rollback) => "ROLLBACK",
            _ => return Err(refused("22023", "EndTransaction: commit or rollback")),
        };
        let session = in_txn(&q.transaction_id)?;
        // (no statement text: COMMIT and ROLLBACK take none, only BEGIN's modes do)
        let app = self.0.clone();
        let out = worked(Some(&q.transaction_id[..]), async move { crate::txn::command(&app, word, "").await.map_err(status) }).await;
        crate::temp::end(&session); // (the session ends with its transaction, committed or not)
        let (tag, _) = out?;
        if word == "COMMIT" && tag == "ROLLBACK" {
            // (a failed transaction's COMMIT rolls back, as in Postgres: a client must learn it did not commit)
            return Err(status(crate::codes::coded("25P02", "the transaction had failed, so it was rolled back, not committed")));
        }
        Ok(())
    }

    async fn do_put_statement_update(&self, cmd: CommandStatementUpdate, req: Request<PeekableFlightDataStream>) -> Result<i64, Status> {
        let role = allowed(&self.0, &req, Role::Write)?;
        let (app, sql) = (self.0.clone(), cmd.query.clone());
        worked(cmd.transaction_id.as_deref(), async move { write(&app, role, &sql).await }).await
    }

    async fn do_put_statement_ingest(&self, cmd: CommandStatementIngest, req: Request<PeekableFlightDataStream>) -> Result<i64, Status> {
        let role = allowed(&self.0, &req, Role::Write)?;
        if cmd.transaction_id.is_some() {
            // (an ingest appends through the log: its rows would commit at once, not with the transaction)
            return Err(status(crate::codes::coded("0A000", "an ingest inside a transaction isn't taken yet: ingest outside it, or INSERT the rows in it")));
        }
        let table = match cmd.schema.as_deref() {
            Some(schema) if !schema.is_empty() => crate::text::join(schema, &cmd.table), // (ADBC's db_schema_name)
            _ => cmd.table.clone(),
        };
        let mut batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(req.into_inner().map_err(Into::into)).map_err(Status::from).peekable();
        let mut exists = self.0.lake.cat.get::<TableMeta>(&table_key(&table)).await.map_err(status)?.is_some();
        crate::auth::check_rows(&crate::auth::catalog_name(&table)).map_err(denied)?; // (the row doors' one check)
        // ADBC's modes: create (fail if there), append (fail if not), replace, create_append.
        use arrow_flight::sql::{TableExistsOption as IfThere, TableNotExistOption as IfNot};
        let options = cmd.table_definition_options.unwrap_or_default();
        match (exists, IfThere::try_from(options.if_exists), IfNot::try_from(options.if_not_exist)) {
            (true, Ok(IfThere::Fail), _) => return Err(refused("42P07", format!("{table} exists (mode create): append to it, or replace it"))),
            (true, Ok(IfThere::Replace), _) => {
                if role < Role::Admin {
                    return Err(refused("42501", "replacing a table needs the admin token"));
                }
                let drop = crate::write::parse(&format!("DROP TABLE {}", crate::write::sql_name(&table))).ok_or_else(|| refused("XX000", "DROP TABLE"))?;
                crate::write::on_node(&self.0, drop, None).await.map_err(status)?;
                exists = false;
            }
            (false, _, Ok(IfNot::Fail)) => return Err(refused("42P01", format!("no table {table} (mode append): create it first, or use create_append"))),
            _ => {}
        }
        if !exists {
            // ADBC's create modes: a table from the stream's schema (strings as Utf8, times in µs).
            if role < Role::Admin {
                return Err(refused("42501", "creating a table needs the admin token"));
            }
            let schema = match std::pin::Pin::new(&mut batches).peek().await {
                Some(Ok(b)) => b.schema(),
                _ => return Err(refused("22023", "no rows to take the table's columns from")),
            };
            let columns: Vec<(String, String)> = schema.fields().iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()))).collect();
            crate::write::define(&self.0, &table, &serde_json::json!(columns).to_string()).await.map_err(status)?;
        }
        let mut acks = append(&self.0, table, String::new(), 0, batches)?;
        let mut rows = 0i64;
        while let Some(ack) = acks.next().await {
            let ack: serde_json::Value = serde_json::from_slice(&ack?.app_metadata).unwrap_or_default();
            rows += ack["rows"].as_i64().unwrap_or(0);
        }
        Ok(rows)
    }

    async fn do_put_fallback(&self, req: Request<PeekableFlightDataStream>, _: Any) -> Result<Response<Out<PutResult>>, Status> {
        allowed(&self.0, &req, Role::Write)?;
        // A path descriptor: [table] or [table, producer, first seq].
        let mut input = req.into_inner();
        let first = std::pin::Pin::new(&mut input).peek().await.cloned().transpose()?.and_then(|d| d.flight_descriptor);
        let path = first.map(|d| d.path).unwrap_or_default();
        let (table, producer, seq) = match &path[..] {
            [t] => (t.clone(), String::new(), 0),
            [t, p, s] => (t.clone(), p.clone(), s.parse().map_err(|_| refused("22023", "the first seq is a number"))?),
            _ => return Err(refused("22023", "DoPut to a path: [table] or [table, producer, first seq]")),
        };
        crate::auth::check_rows(&crate::auth::catalog_name(&table)).map_err(denied)?; // (the row doors' one check)
        let batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(input.map_err(Into::into)).map_err(Status::from);
        Ok(Response::new(append(&self.0, table, producer, seq, batches)?))
    }

    async fn get_flight_info_sql_info(&self, q: CommandGetSqlInfo, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(q.clone().into_builder(&INFO).schema().as_ref(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_sql_info(&self, q: CommandGetSqlInfo, _: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        let b = q.into_builder(&INFO);
        let (schema, batch) = (b.schema(), b.build().map_err(status)?);
        Ok(Response::new(send(schema, vec![batch])))
    }

    async fn get_flight_info_catalogs(&self, q: CommandGetCatalogs, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(&q.clone().into_builder().schema(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_catalogs(&self, q: CommandGetCatalogs, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let mut b = q.into_builder();
        b.append(crate::ddl::lake_name(&self.0.lake));
        Ok(Response::new(send(b.schema(), vec![b.build().map_err(status)?])))
    }

    async fn get_flight_info_schemas(&self, q: CommandGetDbSchemas, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(&q.clone().into_builder().schema(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_schemas(&self, q: CommandGetDbSchemas, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let mut b = q.into_builder();
        for schema in crate::ddl::schemas(&self.0.lake).await.map_err(status)? {
            b.append(crate::ddl::lake_name(&self.0.lake), schema);
        }
        Ok(Response::new(send(b.schema(), vec![b.build().map_err(status)?])))
    }

    async fn get_flight_info_table_types(&self, q: CommandGetTableTypes, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(&table_types().schema(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_table_types(&self, _: CommandGetTableTypes, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let types = table_types();
        Ok(Response::new(send(types.schema(), vec![types])))
    }

    async fn get_flight_info_tables(&self, q: CommandGetTables, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(&q.clone().into_builder().schema(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_tables(&self, q: CommandGetTables, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let (mut b, lake) = (q.into_builder(), &self.0.lake);
        for (key, meta) in lake.cat.scan::<TableMeta>("t/", "t0").await.map_err(status)?.into_iter().filter(|(k, _)| !crate::sys::hidden(k)) {
            let (schema, table) = crate::text::split(&key[2..]);
            let columns = crate::answer::schema(&meta.logical_columns()).map_err(status)?;
            b.append(crate::ddl::lake_name(lake), schema, table, "TABLE", &columns).map_err(status)?;
        }
        for (key, _) in lake.cat.scan::<crate::ddl::StoredView>("q/", "q0").await.map_err(status)? {
            let (schema, view) = crate::text::split(&key[2..]);
            if let Ok(columns) = plan_schema(&self.0, &format!("SELECT * FROM {}", crate::write::sql_name(&key[2..]))).await {
                b.append(crate::ddl::lake_name(lake), schema, view, "VIEW", &columns).map_err(status)?;
            }
        }
        Ok(Response::new(send(b.schema(), vec![b.build().map_err(status)?])))
    }

    async fn register_sql_info(&self, _: i32, _: &SqlInfo) {}
}

/// The actions Pondra takes (`ListActions` lists exactly these). Any other is refused by name.
const ACTIONS: [(&str, &str); 2] = [
    ("BeginTransaction", "Begins a transaction: Request Message: ActionBeginTransactionRequest; Response Message: ActionBeginTransactionResult"),
    ("EndTransaction", "Commits or rolls back a transaction: Request Message: ActionEndTransactionRequest; Response Message: N/A"),
];

/// Plain Flight in front of Flight SQL: JSON tickets and commands are ours, the rest is Flight
/// SQL's (its messages are protobuf, never `{`).
struct Door(Sql);

#[tonic::async_trait]
impl FlightService for Door {
    type HandshakeStream = Out<HandshakeResponse>;
    type ListFlightsStream = Out<FlightInfo>;
    type DoGetStream = Out<FlightData>;
    type DoPutStream = Out<PutResult>;
    type DoActionStream = Out<arrow_flight::Result>;
    type ListActionsStream = Out<ActionType>;
    type DoExchangeStream = Out<FlightData>;

    async fn handshake(&self, req: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> { self.0.handshake(req).await }

    async fn list_flights(&self, req: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        let who = caller(&req);
        crate::auth::WHO.scope(who, async move {
            let app = &self.0 .0;
            allowed(app, &req, Role::Read)?;
            let mut infos = vec![];
            for (key, _) in app.lake.cat.scan::<TableMeta>("t/", "t0").await.map_err(status)?.into_iter().filter(|(k, _)| !crate::sys::hidden(k) && crate::auth::check("select", &k[2..]).is_ok()) {
                let table = &key[2..];
                // (the schema its ticket streams, planned as DoGet runs it: a table's strings are Utf8View, not the table's Utf8)
                let sql = format!("SELECT * FROM {}", crate::write::sql_name(table));
                let ticket = serde_json::json!({"sql": sql}).to_string();
                let schema = plan_schema(app, &sql).await.map_err(status)?;
                infos.push(Ok(FlightInfo::new().try_with_schema(&schema).map_err(status)?.with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket))).with_descriptor(FlightDescriptor::new_path(vec![table.to_string()]))));
            }
            Ok(Response::new(Box::pin(futures::stream::iter(infos)) as Self::ListFlightsStream))
        }).await
    }

    async fn get_flight_info(&self, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        let who = caller(&req);
        crate::auth::WHO.scope(who, async move {
            let Some(json) = Json::parse(&req.get_ref().cmd) else { return self.0.get_flight_info(req).await };
            let app = &self.0 .0;
            allowed(app, &req, Role::Read)?;
            let schema = match (&json.sql, &json.table) {
                (Some(sql), _) => plan_schema(app, sql).await.map_err(status)?,
                (None, Some(table)) => {
                    // Its schema names every column: a user needs SELECT on all of them, as its log stream does (checked before the table is looked up, so an unreadable one isn't told apart).
                    crate::auth::check_all(&crate::auth::catalog_name(table)).map_err(denied)?;
                    let meta: TableMeta = app.lake.cat.get(&table_key(table)).await.map_err(status)?.ok_or_else(|| refused("42P01", format!("no table {table}")))?;
                    let full = crate::answer::schema(&meta.logical_columns()).map_err(status)?;
                    let pick = json.columns.iter().flatten().filter_map(|c| full.index_of(c).ok()).collect::<Vec<_>>();
                    if json.columns.is_some() { full.project(&pick).map_err(status)? } else { full.as_ref().clone() }
                }
                _ => return Err(refused("22023", "{\"sql\": …} or {\"table\": …}")),
            };
            let cmd = req.get_ref().cmd.to_vec();
            info(&schema, cmd, req.into_inner())
        }).await
    }

    // (Not taken, refused by name: the protocol's default says only "Not yet implemented")
    async fn poll_flight_info(&self, _req: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(refused("0A000", "PollFlightInfo: Pondra doesn't take it; GetFlightInfo answers at once"))
    }

    async fn get_schema(&self, _req: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(refused("0A000", "GetSchema: Pondra doesn't take it; GetFlightInfo gives a statement's or a table's schema"))
    }

    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let who = caller(&req);
        crate::auth::WHO.scope(who, async move {
            let Some(json) = Json::parse(&req.get_ref().ticket) else { return self.0.do_get(req).await };
            let app = &self.0 .0;
            allowed(app, &req, Role::Read)?;
            match (json.sql, json.table) {
                (Some(sql), _) => {
                    let app = app.clone();
                    let (schema, batches) = worked(None, async move { query(&app, &sql).await.map_err(status) }).await?;
                    Ok(Response::new(send(schema, batches)))
                }
                (None, Some(table)) => {
                    crate::auth::check_all(&crate::auth::catalog_name(&table)).map_err(denied)?; // (a stream of rows: every column, as `/watch` needs)
                    Ok(Response::new(log_stream(app.clone(), table, json.after, json.columns, json.follow).await?))
                }
                _ => Err(refused("22023", "{\"sql\": …} or {\"table\": …}")),
            }
        }).await
    }

    async fn do_put(&self, req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        let who = caller(&req);
        crate::auth::WHO.scope(who, async move { self.0.do_put(req).await }).await
    }

    async fn do_action(&self, req: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        // (refused here, before the protocol decodes the body: its own decoding answers a body it can't unpack INVALID_ARGUMENT)
        let action = req.get_ref().r#type.clone();
        if !ACTIONS.iter().any(|(name, _)| *name == action) {
            return Err(refused("0A000", format!("DoAction {action}: Pondra doesn't take it (its actions: BeginTransaction, EndTransaction)")));
        }
        let who = caller(&req);
        crate::auth::WHO.scope(who, async move { self.0.do_action(req).await }).await
    }

    async fn list_actions(&self, _req: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = ACTIONS.map(|(name, about)| Ok::<_, Status>(ActionType { r#type: name.into(), description: about.into() }));
        Ok(Response::new(Box::pin(futures::stream::iter(actions)) as Self::ListActionsStream))
    }

    async fn do_exchange(&self, _req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(refused("0A000", "DoExchange: Pondra doesn't take it; DoGet and DoPut are its streams"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, StringArray, StringViewArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::Fields;
    use futures::TryStreamExt;

    fn dictionary() -> DataType { DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)) }

    #[test]
    fn announced_maps_dictionaries_to_their_values_at_any_depth() {
        let dict = dictionary();
        let schema = Schema::new(vec![
            Field::new("d", dict.clone(), false),
            Field::new("l", DataType::List(Arc::new(Field::new("item", dict.clone(), false))), false),
            Field::new("s", DataType::Struct(Fields::from(vec![Field::new("k", dict.clone(), true)])), false),
            Field::new("v", DataType::Utf8View, true),
        ]);
        let out = announced(&schema);
        assert_eq!(out.field(0), &Field::new("d", DataType::Utf8, true));
        assert_eq!(out.field(1), &Field::new("l", DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))), true));
        assert_eq!(out.field(2), &Field::new("s", DataType::Struct(Fields::from(vec![Field::new("k", DataType::Utf8, true)])), true));
        assert_eq!(out.field(3), &Field::new("v", DataType::Utf8View, true));
    }

    #[test]
    fn announced_keeps_names_metadata_and_the_schema_metadata() {
        let field = Field::new("a", DataType::Int64, false).with_metadata([("k".to_string(), "v".to_string())].into());
        let schema = Schema::new_with_metadata(vec![field], [("s".to_string(), "t".to_string())].into());
        let out = announced(&schema);
        assert_eq!(out.field(0), &Field::new("a", DataType::Int64, true).with_metadata([("k".to_string(), "v".to_string())].into()));
        assert_eq!(out.metadata(), schema.metadata());
    }

    #[test]
    fn conform_takes_a_dictionary_batch_to_its_announced_schema() -> anyhow::Result<()> {
        let dict = dictionary();
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("d", dict.clone(), false)])), vec![cast(&strings, &dict)?])?;
        let schema = Arc::new(announced(&batch.schema()));
        let out = conform(&schema, vec![batch])?;
        assert_eq!(out[0].schema().as_ref(), schema.as_ref());
        assert_eq!(out[0].column(0).data_type(), &DataType::Utf8);
        Ok(())
    }

    #[tokio::test]
    async fn the_encoder_streams_the_types_announced_says() -> anyhow::Result<()> {
        use arrow_flight::encode::FlightDataEncoderBuilder;
        let dict = dictionary();
        let schema = Arc::new(Schema::new(vec![Field::new("d", dict.clone(), false), Field::new("s", DataType::Utf8View, false)]));
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let view: ArrayRef = Arc::new(StringViewArray::from(vec!["x", "y"]));
        let batch = RecordBatch::try_new(schema.clone(), vec![cast(&strings, &dict)?, view])?;
        let sent: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .build(futures::stream::iter(vec![Ok::<_, arrow_flight::error::FlightError>(batch)]))
            .try_collect()
            .await?;
        // (the names and types the encoder sends, as a client decodes them, against `announced`'s; nullability is
        // `announced`'s own rule, tested above)
        let streamed = arrow_flight::utils::flight_data_to_batches(&sent)?;
        let names_and_types = |s: &Schema| s.fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect::<Vec<_>>();
        assert_eq!(names_and_types(streamed[0].schema().as_ref()), names_and_types(&announced(&schema)));
        Ok(())
    }
}
