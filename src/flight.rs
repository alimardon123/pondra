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
use crate::app::App;
use crate::statement::{self, Answer, How, Wire};
use crate::store::{table_key, TableMeta};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{Any, ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest, CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate, EndTransaction, ProstMessageExt, SqlInfo, SqlSupportedTransaction, TicketStatementQuery};
use arrow_flight::{Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use serde::Deserialize;
use std::borrow::Cow;
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

/// The SQLSTATE a gRPC code is answered with when its status has none (`grpc`, the other way round).
fn sqlstate(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Aborted => "40001",
        tonic::Code::PermissionDenied => "42501",
        tonic::Code::Unauthenticated => "28000",
        tonic::Code::NotFound => "42P01",
        tonic::Code::AlreadyExists => "42P07",
        tonic::Code::DeadlineExceeded => "57014",
        tonic::Code::ResourceExhausted => "53000",
        tonic::Code::Unimplemented => "0A000",
        tonic::Code::FailedPrecondition => "55000",
        tonic::Code::InvalidArgument => "22023",
        _ => "XX000",
    }
}

/// An error as a gRPC status: the nearest code to its SQLSTATE, which rides along in `x-pondra-sqlstate`, and its words as
/// every door tells them (`codes::answer`).
fn status(e: impl Into<anyhow::Error>) -> Status {
    let e = e.into();
    let (_, code, words) = crate::codes::answer(&e, crate::auth::shows_code()); // (the code and words every door gives)
    let mut s = Status::new(grpc(code), words);
    s.metadata_mut().insert("x-pondra-sqlstate", tonic::metadata::MetadataValue::from_static(code));
    s
}

/// A status with no SQLSTATE of its own gets the one its gRPC code is answered with (`sqlstate`); one that has it is returned as it is.
fn coded(mut s: Status) -> Status {
    if !s.metadata().contains_key("x-pondra-sqlstate") {
        let code = sqlstate(s.code());
        s.metadata_mut().insert("x-pondra-sqlstate", tonic::metadata::MetadataValue::from_static(code));
    }
    s
}

/// A stream's error as a gRPC status with its SQLSTATE: a status is `coded`, an Arrow error is `status` (its code from its words), and any other is `status` of its message.
fn stream_status(e: FlightError) -> Status {
    match e {
        FlightError::Tonic(s) => coded(*s),
        FlightError::Arrow(e) => status(e),
        e => status(anyhow::anyhow!("{e}")),
    }
}

/// A protocol default's stream with each of its errors coded (`coded`), and its refusal too.
fn coded_stream<T: Send + 'static>(answer: Result<Response<Out<T>>, Status>) -> Result<Response<Out<T>>, Status> {
    answer.map(|r| r.map(|s| Box::pin(s.map_err(coded)) as Out<T>)).map_err(coded)
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
    Box::pin(FlightDataEncoderBuilder::new().with_schema(schema).build(stream).map_err(stream_status))
}

/// The messages of Arrow IPC stream bytes, as FlightData, without decoding a batch: each message is `0xFFFFFFFF`, an i32 LE
/// metadata length, the flatbuffer `Message` (that long), then a body of the `bodyLength` the message names. The metadata is
/// a message's `data_header` and the body its `data_body`, both slices of `ipc` (no copy). A metadata length of 0 is the
/// stream's end. Bytes that end inside a message are a piece cut short: an error, never a short answer.
fn messages(ipc: Bytes) -> anyhow::Result<Vec<FlightData>> {
    let cut = || crate::codes::coded("XX000", "an answer's Arrow stream ends inside a message");
    let mut out = vec![];
    let mut at = 0;
    while at < ipc.len() {
        let head = ipc.get(at..at + 8).ok_or_else(cut)?;
        if head[..4] != [0xFF; 4] {
            return Err(crate::codes::coded("XX000", "an answer's Arrow stream has no message where one starts"));
        }
        let meta_len = usize::try_from(i32::from_le_bytes(head[4..].try_into()?))?;
        at += 8;
        if meta_len == 0 {
            break;
        }
        let meta_end = at.checked_add(meta_len).ok_or_else(cut)?;
        let header = ipc.get(at..meta_end).ok_or_else(cut)?;
        let body_len = usize::try_from(arrow_ipc::root_as_message(header)?.bodyLength())?;
        let body_end = meta_end.checked_add(body_len).ok_or_else(cut)?;
        if body_end > ipc.len() {
            return Err(cut());
        }
        out.push(FlightData { data_header: ipc.slice(at..meta_end), data_body: ipc.slice(meta_end..body_end), ..Default::default() });
        at = body_end;
    }
    Ok(out)
}

/// The schema a statement's batches are announced with: `answer::arrow_schema` of the first batch's, or no columns.
fn first_announced(batches: &[RecordBatch]) -> Schema { crate::answer::arrow_schema(&batches.first().map_or_else(|| Arc::new(Schema::empty()), |b| b.schema())) }

/// Run a query as every door runs a statement (`statement::run`, with `FlightWire`): its `$name`s, its macros, a script,
/// and its answer kept for the next asker of the same query. It is armed as HTTP arms one (`FlightWire::pieces`), so an
/// answer big enough goes to the stream in pieces as its plan makes them: then no rows come back here, and `answered`
/// takes the pieces. The schema is the planned one when it has no rows, else the first batch's.
async fn query(app: &App, sql: &str, who: crate::routines::Who) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
    if statement::is_write(sql) {
        return Err(refused("25006", "this changes data or a transaction: a query's ticket reads only (CommandStatementUpdate writes)"));
    }
    let req = crate::routines::Request { sql: sql.to_string(), ..Default::default() };
    let how = How { job: None, spread: None, after: None, stale: None };
    let got = statement::run(app, req, how, who, &FlightWire).await.map_err(status)?;
    if matches!(got, Answer::Sent) {
        return Ok((Arc::new(Schema::empty()), vec![]));
    }
    let batches = rows_of(got)?;
    let schema = match batches.is_empty() {
        false => first_announced(&batches),
        true => match statement::plain_of(app, sql).await.map_err(status)? {
            Some((_, one)) => planned(app, &one).await.map_err(status)?,
            None => Schema::empty(),
        },
    };
    let schema = Arc::new(schema);
    Ok((schema, batches))
}

/// Flight's wire. A query's answer is kept under `arrow`, HTTP's `?format=arrow` key, as the bytes HTTP's render makes for
/// it (`answer::render`), so either door reads back what its own render writes. Kept only when it has rows and is up to
/// `Results::BIGGEST`. It is armed as HTTP's arrow is: an answer big enough goes in pieces, except an EXPLAIN's.
struct FlightWire;

impl Wire for FlightWire {
    type Own = Vec<RecordBatch>;
    fn kept_as(&self) -> Option<Cow<'_, str>> { Some(Cow::Borrowed("arrow")) }
    fn encode(&self, rows: Vec<RecordBatch>) -> anyhow::Result<(Option<bytes::Bytes>, Vec<RecordBatch>)> {
        let small = !rows.is_empty() && rows.iter().map(|b| b.get_array_memory_size()).sum::<usize>() <= crate::app::Results::BIGGEST;
        let keep = small.then(|| crate::answer::render(&rows, Some("arrow"), crate::pages::keep)).transpose()?;
        Ok((keep, rows))
    }
    fn pieces(&self, query: &str) -> Option<Option<&str>> { (!crate::explain::asked(query)).then_some(Some("arrow")) }
}

/// A statement's answer as the batches Flight streams: a query's rows, a kept answer read back, or, for a statement
/// with no rows (`Done`), one row of `rows` 0, as a CALL's answer is.
fn rows_of(got: Answer<Vec<RecordBatch>>) -> Result<Vec<RecordBatch>, Status> {
    match got {
        Answer::Own(batches) | Answer::Rows(batches) => Ok(batches),
        Answer::Kept(bytes) => crate::query::read_ipc(&bytes).map_err(status),
        Answer::Done(_) => Ok(vec![RecordBatch::try_from_iter([("rows", Arc::new(datafusion::arrow::array::Int64Array::from(vec![0])) as _)]).map_err(status)?]),
        Answer::Sent => Err(refused("XX000", "an answer handed over in pieces where Flight keeps it whole (a script's)")),
    }
}

/// A script, a CALL or a write with RETURNING, as a statement's info: run now, as every door runs a statement, and its
/// answer kept for the ticket to read back (`kept:`, ten minutes: `KEPT`). The info's schema is the answer's.
async fn kept(app: &App, sql: &str, who: crate::routines::Who, txn: Option<&[u8]>, descriptor: FlightDescriptor) -> Result<Response<FlightInfo>, Status> {
    let (app, sql) = (app.clone(), sql.to_string());
    let batches = worked(txn, async move {
        let req = crate::routines::Request { sql, ..Default::default() };
        let how = How { job: None, spread: None, after: None, stale: None };
        rows_of(statement::run(&app, req, how, who, &FlightWire).await.map_err(status)?)
    })
    .await?;
    let schema = first_announced(&batches);
    let id = uuid::Uuid::new_v4().simple().to_string();
    let mut held = KEPT.lock().unwrap();
    held.retain(|_, (_, at)| at.elapsed() < std::time::Duration::from_secs(600));
    held.insert(id.clone(), (batches, std::time::Instant::now()));
    let ticket = TicketStatementQuery { statement_handle: format!("kept:{id}").into_bytes().into() };
    info(&schema, ticket.as_any().encode_to_vec(), descriptor)
}

/// The columns a query will answer with, for its flight's info: bound and expanded as every door does (`$name`s from
/// the session, macros, `FROM t`, `pondra.git_status`), then planned, not run.
async fn plan_schema(app: &App, sql: &str) -> anyhow::Result<Schema> { planned(app, &crate::routines::expand(&app.lake, &crate::vars::bound(sql)?).await?).await }

async fn planned(app: &App, sql: &str) -> anyhow::Result<Schema> {
    let ctx = crate::query::session(&app.lake, sql, "").await?;
    let df = crate::engine::sql(&ctx, &crate::sys::planned(sql)?).await?;
    Ok(crate::answer::arrow_schema(crate::query::run_schema(&ctx, df.logical_plan())?.as_arrow()))
}

/// A write's answer, as its ticket and its flight's info give it: one row, `rows`, the count written.
fn rows_schema() -> Schema { Schema::new(vec![Field::new("rows", DataType::Int64, false)]) }

/// A statement's answer, as its ticket names it: a script's or CALL's kept answer, a write's row count, or a
/// query run now, as `who`.
async fn answer(app: &App, sql: &str, who: crate::routines::Who) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
    if let Some(id) = sql.strip_prefix("kept:") {
        let Some((batches, _)) = KEPT.lock().unwrap().remove(id) else { return Err(refused("34000", "that answer was read already, or is gone")) };
        return Ok((Arc::new(first_announced(&batches)), batches));
    }
    if let Some(n) = sql.strip_prefix("rows:").and_then(|n| n.parse::<i64>().ok()) {
        let schema = Arc::new(crate::answer::arrow_schema(&rows_schema()));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![n]))]).map_err(status)?;
        return Ok((schema, vec![batch]));
    }
    query(app, sql, who).await
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

/// What a statement's work hands its Flight stream: its whole answer, or the pieces its plan made (`query::Handed`), with the
/// count its body holds until the last piece is read (`drain::Busy`).
enum Sent {
    Whole(SchemaRef, Vec<RecordBatch>),
    Pieces(crate::query::Handed, crate::drain::Busy),
}

/// A statement's answer as Flight data. Its work (`f`, which runs in `query::PIECES`, so a query may hand its answer to the
/// stream in pieces as its plan makes them) is `worked` as every statement is, and the answer is then its whole form
/// (`whole`) or its pieces (`piece_stream`).
async fn answered<F>(txn: Option<&[u8]>, f: F) -> Result<Out<FlightData>, Status>
where
    F: std::future::Future<Output = Result<(SchemaRef, Vec<RecordBatch>), Status>> + Send + 'static,
{
    let sent = worked(txn, async move {
        let (hand, mut handed) = tokio::sync::oneshot::channel();
        // (`worked` gives this future its transaction's session and who asked, once for the statement. When its answer is handed
        // over, the rest runs aside: then those two go on it, and only then, as an HTTP request's do)
        let session = crate::temp::SESSION.try_with(Clone::clone).ok().flatten();
        let mut go = Box::pin(crate::query::PIECES.scope(crate::query::Pieces::new(hand), f));
        tokio::select! {
            biased;
            done = &mut go => match handed.try_recv() {
                Ok(h) => Ok(Sent::Pieces(h, crate::drain::Busy::carried())), // (handed as the statement ended: its rows still come in pieces)
                Err(_) => done.map(|(schema, batches)| Sent::Whole(schema, batches)),
            },
            Ok(h) = &mut handed => {
                // (the rest of the statement runs on the queries' runtime while the client reads its pieces, as the request that asked)
                crate::panics::aside(crate::panics::work(true, crate::auth::carried(in_session(session, go))));
                Ok(Sent::Pieces(h, crate::drain::Busy::carried()))
            }
        }
    })
    .await?;
    Ok(match sent {
        Sent::Whole(schema, batches) => whole(schema, batches),
        Sent::Pieces(h, busy) => piece_stream(h, busy),
    })
}

/// A statement's whole answer as Flight data, encoded as the client reads it: each batch's pieces (`piece_rows`) through one
/// Arrow encoder on `schema` (`answer::Encoding::with_schema`), each step's messages as the step is written. With no batches,
/// the schema and the end. An encoding error ends the stream with its status.
fn whole(schema: SchemaRef, batches: Vec<RecordBatch>) -> Out<FlightData> {
    // (at most one piece: the answer is encoded at once, its piece then its end, as the stream below writes them, and is
    // sent as those messages; an encoding error ends it after the messages before it, as the stream does)
    let mut few: Vec<RecordBatch> = batches.iter().flat_map(crate::answer::piece_rows).take(2).collect();
    if few.len() <= 1 {
        let mut enc = crate::answer::Encoding::with_schema(schema);
        let mut steps = vec![];
        if let Some(piece) = few.pop() {
            steps.push(enc.step(Some(&piece)));
        }
        steps.push(enc.step(None));
        let mut out: Vec<Result<FlightData, Status>> = vec![];
        for step in steps {
            match step.and_then(|bytes| messages(Bytes::from(bytes))).map_err(status) {
                Ok(data) => out.extend(data.into_iter().map(Ok)),
                Err(e) => {
                    out.push(Err(e));
                    break;
                }
            }
        }
        return Box::pin(futures::stream::iter(out));
    }
    let pieces = batches.into_iter().flat_map(|b| crate::answer::piece_rows(&b));
    let steps = futures::stream::try_unfold(Some((crate::answer::Encoding::with_schema(schema), pieces)), |state| async move {
        let Some((mut enc, mut pieces)) = state else { return Ok::<_, Status>(None) };
        let (step, next) = match pieces.next() {
            Some(piece) => (enc.step(Some(&piece)), Some((enc, pieces))),
            None => (enc.step(None), None),
        };
        let data = step.and_then(|bytes| messages(Bytes::from(bytes))).map_err(status)?;
        Ok(Some((data, next)))
    });
    Box::pin(steps.map_ok(|data| futures::stream::iter(data.into_iter().map(Ok))).try_flatten())
}

/// A statement's answer in pieces, as its plan made them (`query::Handed`): the first piece's messages, then each piece as the
/// door's body takes it. `busy` is held until the stream ends. A piece that fails, or a channel that closes before the answer's
/// end, ends the stream with its status: a cut answer is never taken for a whole one.
fn piece_stream(h: crate::query::Handed, busy: crate::drain::Busy) -> Out<FlightData> {
    let crate::query::Handed { first, rest, ended } = h;
    let pieces = futures::stream::try_unfold((Some(first), rest, ended, busy), |(first, mut rest, ended, busy)| async move {
        let bytes = match first {
            Some(first) => Bytes::from(first),
            None => match rest.recv().await {
                Some(Ok(piece)) => piece,
                Some(Err(e)) => return Err(status(anyhow::anyhow!("{e}"))),
                None if ended.load(std::sync::atomic::Ordering::Acquire) => return Ok(None),
                None => return Err(status(crate::codes::coded("XX000", "the answer stopped before its end"))),
            },
        };
        let data = messages(bytes).map_err(status)?;
        Ok(Some((data, (None, rest, ended, busy))))
    });
    Box::pin(pieces.map_ok(|data| futures::stream::iter(data.into_iter().map(Ok))).try_flatten())
}

crate::per_node!(
    /// A script's or CALL's answer, run when its flight was asked for, until its ticket reads it (10 minutes).
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
    match crate::text::control(sql) {
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
    let (acks_tx, mut acks_rx) = tokio::sync::mpsc::channel::<Result<(Queued, crate::rules::Shaped, crate::log::Src), Status>>(256);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel(); // (acks are small; a client may read them only at the end)
    crate::panics::task(async move {
        let mut batches = std::pin::pin!(batches);
        let meta = lake.cat.meta(&table_key(&table)).await.ok().flatten();
        let Some((meta, rules)) = meta.and_then(|m| {
            let rules = m.rules().ok()?;
            Some((m, rules))
        }) else {
            // (SQL's names: ADR-022)
            let _ = acks_tx.send(Err(refused("42P01", format!("no table {table}")))).await;
            return;
        };
        // (the batch's rows, shaped for the table: a column it leaves out takes its DEFAULT)
        let (m, r, lk, name) = (&meta, &rules, &lake, table.as_str());
        let shaped = |b: RecordBatch| async move {
            let given: Vec<String> = b.schema().fields().iter().map(|f| f.name().clone()).collect();
            let left_out = r.left_out(&given);
            crate::rules::shape(r, name, crate::rules::Rows::Named(b, left_out), crate::rules::How::Load, &crate::defaults::Node(lk)).await
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
                Ok(b) => shaped(b).await.map_err(status).and_then(|s| load(s.rows()).map(|_| s)),
                Err(e) => Err(e),
            };
            let queued = match b {
                Ok(s) => {
                    let (seq, prev) = if producer.is_empty() { (0, None) } else { (first + i, (i > 0).then(|| first + i - 1)) };
                    let src = crate::log::Src { producer: producer.clone(), seq, prev };
                    log.queue_shaped(table.clone(), src.clone(), s.clone()).await.map(|f| (Box::pin(f) as Queued, s, src)).map_err(status)
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
                        a["rows"] = batch.rows().num_rows().into();
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
    let meta = app.lake.cat.meta(&table_key(&table)).await.map_err(status)?.ok_or_else(|| refused("42P01", format!("no table {table}")))?;
    let full = crate::answer::schema(&meta.logical_columns()).map_err(status)?;
    let pick: Vec<usize> = match &columns {
        Some(cols) => cols.iter().map(|c| full.index_of(c).map_err(|_| refused("42703", format!("no column {c}")))).collect::<Result<_, _>>()?,
        None => (0..full.fields().len()).collect(),
    };
    let schema = Arc::new(full.project(&pick).map_err(status)?);
    let after = after.unwrap_or_else(|| app.lake.visible());
    let hwm = app.lake.hwm.subscribe();
    let first = FlightDataEncoderBuilder::new().with_schema(schema.clone()).build(futures::stream::empty()).map_err(stream_status);
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
        let who = crate::routines::Who { role, files: false, depth: 0 };
        if crate::routines::runs_procedure(&q.query) || crate::returning::of(&q.query).is_some() {
            // A CALL (or DO), or a write with RETURNING: run now, as every door runs one; the ticket reads back what it answered.
            return kept(&self.0, &q.query, who, txn, req.into_inner()).await;
        }
        if crate::write::parse(&q.query).is_some() {
            // A write sent as a query (DB-API's `execute`): done now; the ticket reads back the count.
            let (app, sql) = (self.0.clone(), q.query.clone());
            let rows = worked(txn, async move { write(&app, role, &sql).await }).await?;
            let ticket = TicketStatementQuery { statement_handle: format!("rows:{rows}").into_bytes().into() };
            return info(&crate::answer::arrow_schema(&rows_schema()), ticket.as_any().encode_to_vec(), req.into_inner());
        }
        // A query is planned here and run by its ticket; a script (several statements) can't be planned, so it runs now, as a CALL does.
        let Some((_, one)) = within(txn, async { statement::plain_of(&self.0, &q.query).await.map_err(status) }).await? else {
            return kept(&self.0, &q.query, who, txn, req.into_inner()).await;
        };
        let schema = within(txn, async { planned(&self.0, &one).await.map_err(status) }).await?;
        let ticket = TicketStatementQuery { statement_handle: handle(&q.query, txn).into() };
        info(&schema, ticket.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_statement(&self, t: TicketStatementQuery, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        let role = allowed(&self.0, &req, Role::Read)?;
        let (txn, sql) = unhandle(&t.statement_handle);
        let sql = String::from_utf8(sql.to_vec()).map_err(status)?;
        let app = self.0.clone();
        let who = crate::routines::Who { role, files: false, depth: 0 };
        // (boxed: a statement's future is large, and each layer of `answered`'s and `worked`'s would copy it by value)
        Ok(Response::new(answered(txn, Box::pin(async move { answer(&app, &sql, who).await })).await?))
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
        let mut batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(req.into_inner().map_err(Into::into)).map_err(stream_status).peekable();
        let mut exists = self.0.lake.cat.meta(&table_key(&table)).await.map_err(status)?.is_some();
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
        let batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(input.map_err(Into::into)).map_err(stream_status);
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
            let Some(json) = Json::parse(&req.get_ref().cmd) else { return self.0.get_flight_info(req).await.map_err(coded) };
            let app = &self.0 .0;
            allowed(app, &req, Role::Read)?;
            let schema = match (&json.sql, &json.table) {
                (Some(sql), _) => plan_schema(app, sql).await.map_err(status)?,
                (None, Some(table)) => {
                    // Its schema names every column: a user needs SELECT on all of them, as its log stream does (checked before the table is looked up, so an unreadable one isn't told apart).
                    crate::auth::check_all(&crate::auth::catalog_name(table)).map_err(denied)?;
                    let meta = app.lake.cat.meta(&table_key(table)).await.map_err(status)?.ok_or_else(|| refused("42P01", format!("no table {table}")))?;
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
            let Some(json) = Json::parse(&req.get_ref().ticket) else { return coded_stream(self.0.do_get(req).await) };
            let app = &self.0 .0;
            let role = allowed(app, &req, Role::Read)?;
            match (json.sql, json.table) {
                (Some(sql), _) => {
                    let app = app.clone();
                    let who = crate::routines::Who { role, files: false, depth: 0 };
                    Ok(Response::new(answered(None, Box::pin(async move { query(&app, &sql, who).await })).await?))
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
        crate::auth::WHO.scope(who, async move { coded_stream(self.0.do_put(req).await) }).await
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
    use datafusion::arrow::compute::{cast, concat_batches};
    use futures::TryStreamExt;

    fn dictionary() -> DataType { DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)) }

    /// `rows` rows: an integer, and a text of about 40 bytes, so a batch of a few MB of Arrow.
    fn wide(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false), Field::new("s", DataType::Utf8, true)]));
        let ints: ArrayRef = Arc::new(datafusion::arrow::array::Int64Array::from_iter_values(0..rows as i64));
        let texts: ArrayRef = Arc::new(StringArray::from_iter_values((0..rows).map(|i| format!("row {i} with a little padding text"))));
        RecordBatch::try_new(schema, vec![ints, texts]).expect("a batch")
    }

    #[test]
    fn messages_of_the_pieces_are_the_answer() -> anyhow::Result<()> {
        let batch = wide(100_000);
        let pieces = crate::answer::piece_rows(&batch);
        assert!(pieces.len() > 1, "a few MB of Arrow: {} pieces", pieces.len());
        let mut enc = crate::answer::Encoding::new(Some("arrow"));
        let mut ipc = vec![];
        for piece in &pieces {
            ipc.extend(enc.step(Some(piece))?);
        }
        ipc.extend(enc.step(None)?);
        let back = arrow_flight::utils::flight_data_to_batches(&messages(Bytes::from(ipc))?)?;
        let rows = concat_batches(&back[0].schema(), &back)?;
        assert_eq!(rows.num_rows(), batch.num_rows());
        for (a, b) in rows.columns().iter().zip(batch.columns()) {
            assert_eq!(a.to_data(), b.to_data());
        }
        Ok(())
    }

    #[test]
    fn a_piece_cut_mid_message_is_an_error() -> anyhow::Result<()> {
        let mut enc = crate::answer::Encoding::new(Some("arrow"));
        let piece = enc.step(Some(&wide(1000)))?;
        assert_eq!(messages(Bytes::from(piece.clone()))?.len(), 2, "the schema and the batch");
        for cut in [5, piece.len() / 2, piece.len() - 1] {
            assert!(messages(Bytes::from(piece[..cut].to_vec())).is_err(), "cut at {cut} of {}", piece.len());
        }
        Ok(())
    }

    #[tokio::test]
    async fn whole_sends_the_announced_types_and_an_empty_answer_its_columns() -> anyhow::Result<()> {
        let dict = dictionary();
        let plan = Schema::new(vec![Field::new("d", dict.clone(), false), Field::new("s", DataType::Utf8View, false)]);
        let schema = Arc::new(crate::answer::arrow_schema(&plan));
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let view: ArrayRef = Arc::new(StringViewArray::from(vec!["x", "y"]));
        let batch = RecordBatch::try_new(Arc::new(plan), vec![cast(&strings, &dict)?, view])?;
        let names_and_types = |s: &Schema| s.fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect::<Vec<_>>();

        let sent: Vec<FlightData> = whole(schema.clone(), vec![batch]).try_collect().await?;
        let streamed = arrow_flight::utils::flight_data_to_batches(&sent)?;
        assert_eq!(names_and_types(streamed[0].schema().as_ref()), names_and_types(&schema));
        assert_eq!(streamed[0].column(0).data_type(), &DataType::Utf8, "a dictionary arrives as its values");

        let empty: Vec<FlightData> = whole(schema.clone(), vec![]).try_collect().await?;
        assert_eq!(empty.len(), 1, "the schema alone");
        assert_eq!(names_and_types(&Schema::try_from(&empty[0])?), names_and_types(&schema));
        Ok(())
    }

    #[test]
    fn coded_gives_a_status_its_sqlstate_by_its_code_and_keeps_its_own() {
        let header = |s: Status| s.metadata().get("x-pondra-sqlstate").unwrap().to_str().unwrap().to_string();
        assert_eq!(header(coded(Status::unimplemented("no"))), "0A000");
        assert_eq!(header(coded(Status::permission_denied("no"))), "42501");
        let mut own = Status::unimplemented("no");
        own.metadata_mut().insert("x-pondra-sqlstate", tonic::metadata::MetadataValue::from_static("42P01"));
        assert_eq!(header(coded(own)), "42P01");
    }

    #[test]
    fn stream_status_of_an_arrow_error_carries_a_sqlstate() {
        let e = FlightError::Arrow(datafusion::arrow::error::ArrowError::ComputeError("no".into()));
        assert!(stream_status(e).metadata().contains_key("x-pondra-sqlstate"));
    }
}
