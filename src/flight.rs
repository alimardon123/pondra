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
use crate::tls::Peer;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{Any, ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest, CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate, EndTransaction, ProstMessageExt, SqlInfo, SqlSupportedTransaction, TicketStatementQuery};
use arrow_flight::{Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::MemoryReservation;
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
    // (it closes with the HTTP door, gracefully: the answers being written go out whole, and the node's stop waits for its
    // connections to close, `Drain::serving`, as an answer's last messages may still be in one when its stream has ended)
    let (_serving, closing) = (app.lake.node.drain.serving(), app.lake.node.drain.closing());
    tonic::transport::Server::builder().add_service(Gate { app, inner: service }).serve_with_incoming_shutdown(doors.stream(), closing).await?;
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
        let (app, mut inner, used) = (self.app.clone(), self.inner.clone(), self.app.lake.node.idle.begin()); // (a call is a use of the node until its answer is sent: `idle.rs`)
        Box::pin(async move {
            // (a header that isn't text can't be read, and is refused as HTTP refuses it: not taken for no credential)
            let (credential, sent) = (crate::auth::Credential::of_headers(req.headers()), req.headers().contains_key("authorization"));
            let peer = req.extensions().get::<Peer>().copied();
            let again = Again { credential: credential.clone(), sent, peer }; // (for a call that outlives a change of who may do what: `Again`)
            let who = match sign_in(&app, credential, sent, peer).await {
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
            req.extensions_mut().insert(again);
            let answer = tower::Service::call(&mut inner, req).await;
            Ok(answer?.map(|body| tonic::body::Body::new(crate::node_state::kept(body, used))))
        })
    }
}

const SIGN_IN: &str = "sign in: a token, or a user's name and password (handshake)";

/// What a call signed in with, so a stream that outlives a change of who may do what signs in again (`Principal::stale`:
/// a REVOKE, a DROP USER, a token revoked, a sign-in that ended), as Postgres's connections and Kafka's do: a DoPut goes on
/// as whoever that signs in now, or stops (`append`).
#[derive(Clone)]
struct Again {
    credential: crate::auth::Credential,
    sent: bool,
    peer: Option<Peer>,
}

/// `again`'s caller now, if `who` is stale: signed in again, with the rights version read before (so a change meanwhile
/// is seen at the next batch), or refused, 28000, once it no longer signs in. None while `who` is current, or a token's.
async fn signed_again(app: &App, who: &crate::auth::Principal, again: &Again) -> Option<Result<crate::auth::Principal, Status>> {
    if who.operator || !who.stale(&app.lake) {
        return None;
    }
    let epoch = app.lake.cat.rights_version();
    Some(match sign_in(app, again.credential.clone(), again.sent, again.peer).await {
        Ok(p) if p.role > Role::None => Ok(crate::auth::Principal { epoch: p.epoch.max(epoch), ..p }),
        // (signed in before, so whatever stops it now ends its session, as Postgres says it: `pg::signed_again`)
        _ => {
            let why = "it can't sign in any more (dropped, NOLOGIN, its password changed, or its token revoked or ended)";
            crate::audit::refused(app, &who.name, "flight", who.from, "sign in again", why).await;
            Err(refused("28000", format!("user \"{}\" is signed out: {why}", who.name)))
        }
    })
}

/// The gRPC status of a SQLSTATE (Flight SQL's errors), by its kind (`codes::kind`).
fn grpc(code: &str) -> tonic::Code {
    use crate::codes::Kind;
    match crate::codes::kind(code) {
        Kind::Invalid => tonic::Code::InvalidArgument,
        Kind::Refused | Kind::State => tonic::Code::FailedPrecondition, // (a constraint, a transaction that isn't open, failed or idle-ended, a lock)
        Kind::SignIn => tonic::Code::Unauthenticated,
        Kind::Denied => tonic::Code::PermissionDenied,
        Kind::Missing => tonic::Code::NotFound,
        Kind::Exists => tonic::Code::AlreadyExists,
        Kind::Retry => tonic::Code::Aborted,
        Kind::TooBig | Kind::Busy | Kind::Exhausted => tonic::Code::ResourceExhausted,
        Kind::Timeout => tonic::Code::DeadlineExceeded,
        Kind::Unavailable => tonic::Code::Unavailable,
        Kind::Unsupported => tonic::Code::Unimplemented,
        Kind::Internal => tonic::Code::Internal,
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
        tonic::Code::Unavailable => "57P03",
        _ => "XX000",
    }
}

/// How a call's errors are worded: with a routine's code to a caller shown it, without to any other (`codes::answer`). Its
/// handler makes it from the call's caller (`worded`), and a stream built there moves it in, so no answer is worded from
/// the task's caller. `PLAIN` is for words made away from any request.
#[derive(Clone, Copy)]
struct Worded(bool);

/// As a caller who isn't shown code is told.
const PLAIN: Worded = Worded(false);

/// The call's caller's words (`Worded`).
fn worded<T>(req: &Request<T>) -> Worded { Worded::of(&caller(req).scope) }

impl Worded {
    fn of(scope: &crate::rights::Scope) -> Self { Worded(scope.shows_code()) }

    /// An error as a gRPC status: the nearest code to its SQLSTATE, which rides along in `x-pondra-sqlstate`, and its words
    /// as every door tells them (`codes::answer`).
    fn status(self, e: impl Into<anyhow::Error>) -> Status {
        let e = e.into();
        let (_, code, words) = crate::codes::answer(&e, self.0); // (the code and words every door gives)
        let mut s = Status::new(grpc(code), words);
        s.metadata_mut().insert("x-pondra-sqlstate", tonic::metadata::MetadataValue::from_static(code));
        s
    }

    /// `status`, to map an error with.
    fn err<E: Into<anyhow::Error>>(self) -> impl Fn(E) -> Status + Copy { move |e| self.status(e) }

    /// A stream's error as a gRPC status with its SQLSTATE: a status is `coded`, an Arrow error is `status` (its code from
    /// its words), and any other is `status` of its message.
    fn stream(self, e: FlightError) -> Status {
        match e {
            FlightError::Tonic(s) => coded(*s),
            FlightError::Arrow(e) => self.status(e),
            e => self.status(anyhow::anyhow!("{e}")),
        }
    }

    /// A permission check's refusal (`status`): 42501, unless it carries a code of its own.
    fn denied(self, e: impl Into<anyhow::Error>) -> Status {
        let e = e.into();
        self.status(match crate::codes::of(&e) { "XX000" => crate::codes::coded("42501", crate::codes::told(&e, self.0)), _ => e })
    }
}

/// A status with no SQLSTATE of its own gets the one its gRPC code is answered with (`sqlstate`); one that has it is returned as it is.
fn coded(mut s: Status) -> Status {
    if !s.metadata().contains_key("x-pondra-sqlstate") {
        let code = sqlstate(s.code());
        s.metadata_mut().insert("x-pondra-sqlstate", tonic::metadata::MetadataValue::from_static(code));
    }
    s
}

/// A protocol default's stream with each of its errors coded (`coded`), and its refusal too.
fn coded_stream<T: Send + 'static>(answer: Result<Response<Out<T>>, Status>) -> Result<Response<Out<T>>, Status> {
    answer.map(|r| r.map(|s| Box::pin(s.map_err(coded)) as Out<T>)).map_err(coded)
}

/// A refusal with its SQLSTATE, as a gRPC status (`Worded::status`). Its words are Pondra's own, never an error's, so they
/// are told alike to every caller (`PLAIN`).
fn refused(code: &'static str, message: impl Into<String>) -> Status { PLAIN.status(crate::codes::coded(code, message)) }

/// The caller's role (who it is: `Gate` signs it in); refused below `need`.
fn allowed<T>(_: &App, req: &Request<T>, need: Role) -> Result<Role, Status> {
    let role = caller(req).role;
    match role {
        _ if role >= need => Ok(role),
        Role::None => Err(refused("28000", SIGN_IN)),
        _ => Err(refused("42501", "this needs more rights than this token's or user's")),
    }
}

/// Who a call is, from its `authorization`: a token, or a user's token or session (`Bearer`:
/// `users::who`); or a name and password (`Basic`: what ADBC's and pyarrow's handshakes send). With
/// nothing that needs a sign-in, anyone is an admin. A wrong password is an error, said at once.
async fn sign_in(app: &App, credential: crate::auth::Credential, sent: bool, peer: Option<Peer>) -> Result<crate::auth::Principal, Status> {
    let from = peer.map(|p| p.addr);
    if let Some(why) = credential.malformed() {
        crate::audit::refused(app, "", "flight", from, "sign in", &why).await;
        return Err(refused("28000", why));
    }
    let who = match &credential {
        crate::auth::Credential::Password { user, password } => password_sign_in(app, user, password, peer).await?,
        c => crate::users::who(&app.lake, &app.auth, c).await,
    };
    let who = match who {
        Some(p) => p,
        None if app.open().await => crate::auth::Principal::of(Role::Admin),
        None => {
            // (an identity provider's token says why it was refused, as HTTP's 401 does)
            let why = match credential.token() {
                Some(t) => crate::sso::why(&app.lake, t).await,
                None => None,
            };
            if sent {
                crate::audit::refused(app, "", "flight", from, "sign in", why.as_deref().unwrap_or("wrong token")).await;
            }
            if let Some(why) = why {
                return Err(refused("28000", why));
            }
            crate::auth::Principal::of(Role::None)
        }
    };
    Ok(who.at("flight", from))
}

/// A name and password: a token as the password whatever the name, else a user's password (or
/// `pt_` token), checked as every door checks one. A password crosses only TLS from another machine
/// (`tls.rs`; no peer at all refuses one), and failed sign-ins are counted per address and name
/// (`auth::guessing`), each costing 400 ms. None on a node nothing locks.
async fn password_sign_in(app: &App, user: &str, password: &str, peer: Option<Peer>) -> Result<Option<crate::auth::Principal>, Status> {
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
        crate::audit::refused(app, user, "flight", from, "sign in", &why).await;
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
            crate::audit::refused(app, user, "flight", from, "sign in", &why).await;
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
fn send(w: Worded, schema: SchemaRef, batches: Vec<RecordBatch>) -> Out<FlightData> {
    let stream = futures::stream::iter(batches.into_iter().map(|b| Ok(crate::answer::compact(b))));
    Box::pin(FlightDataEncoderBuilder::new().with_schema(schema).build(stream).map_err(move |e| w.stream(e)))
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
/// takes the pieces. The schema is the planned one when it has no rows, else the first batch's. A kept answer is not decoded:
/// it comes back as its bytes (`Flown::Ipc`), which carry their own schema.
async fn query(app: &App, sql: &str, who: crate::routines::Who) -> Result<Flown, Status> {
    if statement::is_write(sql) {
        return Err(refused("25006", "this changes data or a transaction: a query's ticket reads only (CommandStatementUpdate writes)"));
    }
    let req = crate::routines::Request { sql: sql.to_string(), ..Default::default() };
    let how = How { job: None, spread: None, after: None, stale: None };
    let w = Worded::of(&who.scope);
    let got = statement::run(app, req, how, who.clone(), &FlightWire).await.map_err(w.err())?;
    if matches!(got, Answer::Sent) {
        return Ok(Flown::Batches(Arc::new(Schema::empty()), vec![]));
    }
    if let Answer::Kept(bytes) | Answer::Own(Encoded::Kept(bytes)) = got {
        return Ok(Flown::Ipc(bytes));
    }
    let batches = rows_of(w, got)?;
    let schema = match batches.is_empty() {
        false => first_announced(&batches),
        true => match statement::plain_of(app, sql, &who.scope).await.map_err(w.err())? {
            Some((_, one)) => planned(app, &one, &who.scope).await.map_err(w.err())?,
            None => Schema::empty(),
        },
    };
    let schema = Arc::new(schema);
    Ok(Flown::Batches(schema, batches))
}

/// A query's answer as Flight sends it: its batches and the schema they are announced with, or the Arrow stream the
/// results cache keeps for it (this run's `FlightWire::encode`, or another run's), sent as its messages are (`messages`),
/// never decoded and encoded again.
enum Flown {
    Batches(SchemaRef, Vec<RecordBatch>),
    Ipc(Bytes),
}

/// What `FlightWire::encode` gives its door: the bytes it kept (an answer up to `Results::BIGGEST`), or the rows.
enum Encoded {
    Kept(Bytes),
    Rows(Vec<RecordBatch>),
}

/// Flight's wire. A query's answer is kept under `arrow`, HTTP's `?format=arrow` key, as the bytes HTTP's render makes for
/// it (`answer::render`), so either door reads back what its own render writes. Kept only when it has rows and is up to
/// `Results::BIGGEST`, and the kept bytes are also what Flight streams for it, as they are (`sent_as_kept`). It is armed as
/// HTTP's arrow is: an answer big enough goes in pieces, except an EXPLAIN's.
struct FlightWire;

impl Wire for FlightWire {
    type Own = Encoded;
    fn kept_as(&self) -> Option<Cow<'_, str>> { Some(Cow::Borrowed("arrow")) }
    fn encode(&self, rows: Vec<RecordBatch>) -> anyhow::Result<(Option<bytes::Bytes>, Encoded)> {
        let small = !rows.is_empty() && rows.iter().map(|b| b.get_array_memory_size()).sum::<usize>() <= crate::app::Results::BIGGEST;
        let keep = small.then(|| crate::answer::render(&rows, Some("arrow"), &|_| None)).transpose()?; // (Arrow keeps nothing for pages: only a typed answer does)
        Ok(match keep {
            Some(bytes) => (Some(bytes.clone()), Encoded::Kept(bytes)),
            None => (None, Encoded::Rows(rows)),
        })
    }
    fn pieces(&self, query: &str) -> Option<Option<&str>> { (!crate::explain::asked(query)).then_some(Some("arrow")) }
}

/// A statement's answer as the batches Flight streams: a query's rows, a kept answer read back, or, for a statement
/// with no rows (`Done`), one row of `rows` 0, as a CALL's answer is.
fn rows_of(w: Worded, got: Answer<Encoded>) -> Result<Vec<RecordBatch>, Status> {
    match got {
        Answer::Own(Encoded::Rows(batches)) | Answer::Rows(batches) => Ok(batches),
        Answer::Kept(bytes) | Answer::Own(Encoded::Kept(bytes)) => crate::query::read_ipc(&bytes).map_err(w.err()),
        Answer::Done(_) => Ok(vec![RecordBatch::try_from_iter([("rows", Arc::new(datafusion::arrow::array::Int64Array::from(vec![0])) as _)]).map_err(w.err())?]),
        Answer::Sent => Err(refused("XX000", "an answer handed over in pieces where Flight keeps it whole (a script's)")),
    }
}

/// A script, a CALL or a write with RETURNING, as a statement's info: run now, as every door runs a statement, and its
/// answer kept for the ticket to read back (`kept:`, `KEPT`, until `KEPT_FOR`). The answer counts in the query memory while
/// it is kept. The info's schema is the answer's.
async fn kept(app: &App, sql: &str, who: crate::routines::Who, txn: Option<String>, descriptor: FlightDescriptor) -> Result<Response<FlightInfo>, Status> {
    let pool = app.lake.rt.memory_pool.clone();
    let (app, sql, w) = (app.clone(), sql.to_string(), Worded::of(&who.scope));
    let batches = worked(w, app.lake.node.clone(), txn, async move {
        let req = crate::routines::Request { sql, ..Default::default() };
        let how = How { job: None, spread: None, after: None, stale: None };
        rows_of(w, statement::run(&app, req, how, who, &FlightWire).await.map_err(w.err())?)
    })
    .await?;
    let schema = first_announced(&batches);
    // (a read token's script could keep answers that no ticket reads: counted here, they are bounded by the query memory)
    let reserved = crate::query::kept_memory(&pool, &batches).ok_or_else(|| w.status(crate::query::too_big(crate::query::room_of(&pool))))?;
    let id = uuid::Uuid::new_v4().simple().to_string();
    KEPT.lock().unwrap().insert(id.clone(), (batches, std::time::Instant::now(), reserved));
    let ticket = TicketStatementQuery { statement_handle: format!("kept:{id}").into_bytes().into() };
    info(w, &schema, ticket.as_any().encode_to_vec(), descriptor)
}

/// The columns a query will answer with, for its flight's info: bound and expanded as every door does (`$name`s from
/// the session, macros, `FROM t`, `pondra.git_status`), then planned, not run.
async fn plan_schema(app: &App, sql: &str, scope: &crate::rights::Scope) -> anyhow::Result<Schema> { planned(app, &crate::routines::expand(&app.lake, &crate::vars::bound(sql)?, scope).await?, scope).await }

async fn planned(app: &App, sql: &str, scope: &crate::rights::Scope) -> anyhow::Result<Schema> { Ok(crate::with_writes::planned(app, sql, scope).await?.as_ref().clone()) }

/// A write's answer, as its ticket and its flight's info give it: one row, `rows`, the count written.
fn rows_schema() -> Schema { Schema::new(vec![Field::new("rows", DataType::Int64, false)]) }

/// A statement's answer, as its ticket names it: a script's or CALL's kept answer, a write's row count, or a
/// query run now, as `who`.
async fn answer(app: &App, sql: &str, who: crate::routines::Who) -> Result<Flown, Status> {
    if let Some(id) = sql.strip_prefix("kept:") {
        // (the entry's memory goes with it: the batches stream out from here, as a query's answer does once it is returned)
        let Some((batches, _, _)) = KEPT.lock().unwrap().remove(id) else { return Err(refused("34000", "that answer was read already, or is gone")) };
        return Ok(Flown::Batches(Arc::new(first_announced(&batches)), batches));
    }
    if let Some(n) = sql.strip_prefix("rows:").and_then(|n| n.parse::<i64>().ok()) {
        let schema = Arc::new(crate::answer::arrow_schema(&rows_schema()));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![n]))]).map_err(Worded::of(&who.scope).err())?;
        return Ok(Flown::Batches(schema, vec![batch]));
    }
    query(app, sql, who).await
}

/// A statement's work, as a request's: in the transaction the call names (`session`, its session: `in_txn`), as its
/// caller (`auth::carried`, which a spawned task would lose), and through `panics::work`, where the memory
/// watch (`oom.rs`) may stop a query that grew the node, and a long one moves to the queries' runtime
/// (invariant 231). A statement the node stopped for memory fails its transaction, as on HTTP and Postgres.
async fn worked<T: Send + 'static>(w: Worded, node: Arc<crate::node_state::NodeState>, session: Option<String>, f: impl std::future::Future<Output = Result<T, Status>> + Send + 'static) -> Result<T, Status> {
    prune_kept(); // (every Flight call that runs work lets go of the kept answers that expired, not only a script's)
    let failing = session.clone();
    let f = crate::auth::carried(in_session(session, f));
    match crate::panics::work(&node, true, f).await {
        Ok(out) => out,
        Err(m) => {
            if m == crate::oom::OOM {
                if let Some(s) = failing {
                    crate::temp::SESSION.sync_scope(Some(s), || crate::txn::failed(&m));
                }
            }
            Err(w.status(anyhow::anyhow!(m)))
        }
    }
}

/// What a statement's work hands its Flight stream: its whole answer, or the pieces its plan made (`query::Handed`), with the
/// count its body holds until the last piece is read (`Drain::carried`).
enum Sent {
    Whole(Flown, crate::node_state::Busy),
    Pieces(crate::query::Handed, crate::node_state::Busy),
}

/// A statement's answer as Flight data. Its work (`f`, which runs in `query::PIECES`, so a query may hand its answer to the
/// stream in pieces as its plan makes them) is `worked` as every statement is, and the answer is then its whole form
/// (`whole`, or `sent_as_kept` for the bytes the results cache keeps) or its pieces (`piece_stream`).
async fn answered<F>(w: Worded, node: Arc<crate::node_state::NodeState>, txn: Option<String>, f: F) -> Result<Out<FlightData>, Status>
where
    F: std::future::Future<Output = Result<Flown, Status>> + Send + 'static,
{
    let held = node.clone(); // (the body's count, and the rest of the statement's, are this node's)
    let sent = worked(w, node, txn, async move {
        let (hand, mut handed) = tokio::sync::oneshot::channel();
        // (`worked` gives this future its transaction's session and who asked, once for the statement. When its answer is handed
        // over, the rest runs aside: then those two go on it, and only then, as an HTTP request's do)
        let session = crate::temp::SESSION.try_with(Clone::clone).ok().flatten();
        let mut go = Box::pin(crate::query::PIECES.scope(crate::query::Pieces::new(hand), f));
        tokio::select! {
            biased;
            done = &mut go => match handed.try_recv() {
                Ok(h) => Ok(Sent::Pieces(h, held.drain.carried())), // (handed as the statement ended: its rows still come in pieces)
                Err(_) => done.map(|flown| Sent::Whole(flown, held.drain.carried())), // (its messages are written as the client reads them, as the pieces' are)
            },
            Ok(h) = &mut handed => {
                // (the rest of the statement runs on the queries' runtime while the client reads its pieces, as the request that asked)
                held.tasks.aside(crate::auth::caller(), crate::panics::work(&held, true, in_session(session, go)));
                Ok(Sent::Pieces(h, held.drain.carried()))
            }
        }
    })
    .await?;
    Ok(match sent {
        Sent::Whole(Flown::Batches(schema, batches), busy) => whole(w, schema, batches, busy),
        Sent::Whole(Flown::Ipc(bytes), busy) => sent_as_kept(w, bytes, busy),
        Sent::Pieces(h, busy) => piece_stream(w, h, busy),
    })
}

/// A statement's whole answer as Flight data, encoded as the client reads it: its pieces (`answer::pieces`) through one
/// Arrow encoder on `schema` (`answer::Encoding::with_schema`), each step's messages as the step is written. With no batches,
/// the schema and the end. An encoding error ends the stream with its status. `busy` is held until the stream ends or is
/// dropped, as `piece_stream`'s is.
fn whole(w: Worded, schema: SchemaRef, batches: Vec<RecordBatch>, busy: crate::node_state::Busy) -> Out<FlightData> {
    // (at most one piece: the answer is encoded at once, its piece then its end, as the stream below writes them, and is
    // sent as those messages; an encoding error ends it after the messages before it, as the stream does)
    let mut pieces = crate::answer::pieces(batches.into_iter().map(|b| crate::answer::as_sent(b, Some("arrow"))));
    if pieces.len() <= 1 {
        let mut enc = crate::answer::Encoding::with_schema(schema);
        let mut steps = vec![];
        if let Some(piece) = pieces.pop() {
            steps.push(enc.step(Some(&piece)));
        }
        steps.push(enc.step(None));
        let mut out: Vec<Result<FlightData, Status>> = vec![];
        for step in steps {
            match step.and_then(|bytes| messages(Bytes::from(bytes))).map_err(w.err()) {
                Ok(data) => out.extend(data.into_iter().map(Ok)),
                Err(e) => {
                    out.push(Err(e));
                    break;
                }
            }
        }
        return held(out, busy);
    }
    let pieces = pieces.into_iter();
    let steps = futures::stream::try_unfold((Some((crate::answer::Encoding::with_schema(schema), pieces)), busy), move |(state, busy)| async move {
        let Some((mut enc, mut pieces)) = state else { return Ok::<_, Status>(None) };
        let (step, next) = match pieces.next() {
            Some(piece) => (enc.step(Some(&piece)), Some((enc, pieces))),
            None => (enc.step(None), None),
        };
        let data = step.and_then(|bytes| messages(Bytes::from(bytes))).map_err(w.err())?;
        Ok(Some((data, (next, busy))))
    });
    Box::pin(steps.map_ok(|data| futures::stream::iter(data.into_iter().map(Ok))).try_flatten())
}

/// Messages already made, as a Flight stream that holds `busy` until it ends or is dropped.
fn held(out: Vec<Result<FlightData, Status>>, busy: crate::node_state::Busy) -> Out<FlightData> {
    Box::pin(futures::stream::unfold((futures::stream::iter(out), busy), |(mut out, busy)| async move {
        match out.next().await {
            Some(item) => Some((item, (out, busy))),
            None => None,
        }
    }))
}

/// A kept answer's Arrow stream as Flight data: its messages as they are (`messages`), holding `busy` as `whole` does.
fn sent_as_kept(w: Worded, bytes: Bytes, busy: crate::node_state::Busy) -> Out<FlightData> {
    held(messages(bytes).map_or_else(|e| vec![Err(w.status(e))], |data| data.into_iter().map(Ok).collect()), busy)
}

/// A statement's answer in pieces, as its plan made them (`query::Handed`): the first piece's messages, then each piece as the
/// door's body takes it. `busy` is held until the stream ends. A piece that fails, or a channel that closes before the answer's
/// end, ends the stream with its status: a cut answer is never taken for a whole one.
fn piece_stream(w: Worded, h: crate::query::Handed, busy: crate::node_state::Busy) -> Out<FlightData> {
    let crate::query::Handed { first, rest, ended, stop } = h;
    let pieces = futures::stream::try_unfold((Some(first), rest, ended, busy), move |(first, mut rest, ended, busy)| async move {
        let bytes = match first {
            Some(first) => Bytes::from(first),
            None => match rest.recv().await {
                Some(Ok(piece)) => piece,
                Some(Err(e)) => return Err(w.status(anyhow::anyhow!("{e}"))),
                None if ended.load(std::sync::atomic::Ordering::Acquire) => return Ok(None),
                None => return Err(refused("XX000", "the answer stopped before its end")),
            },
        };
        let data = messages(bytes).map_err(w.err())?;
        Ok(Some((data, (None, rest, ended, busy))))
    });
    let pieces = pieces.map_ok(|data| futures::stream::iter(data.into_iter().map(Ok))).try_flatten();
    // (a client that goes mid-answer stops the plan's search still running: the statement's flag, if it has one)
    match stop {
        Some(flag) => Box::pin(crate::audit::guarded(flag, pieces)),
        None => Box::pin(pieces),
    }
}

crate::per_node!(
    /// A script's or CALL's answer, run when its flight was asked for, until its ticket reads it (`KEPT_FOR`), with the
    /// query memory its batches hold while it is kept.
    static KEPT: std::sync::Mutex<std::collections::HashMap<String, (Vec<RecordBatch>, std::time::Instant, MemoryReservation)>> = Default::default());

/// How long a kept answer waits for its ticket to read it.
const KEPT_FOR: std::time::Duration = std::time::Duration::from_secs(600);

/// Lets go of the kept answers older than `KEPT_FOR`, and with them their memory. `worked` calls it, so every Flight call that
/// runs work does.
fn prune_kept() { KEPT.lock().unwrap().retain(|_, (_, at, _)| at.elapsed() < KEPT_FOR); }

/// A write sent as SQL: a CREATE, INSERT, UPDATE, DELETE or ALTER (or a CALL). Returns the rows written. It runs as every
/// door runs a statement (`statement::run`): its `$name`s bound from the session (a Flight transaction's `DECLARE $x` is
/// one: audit 10-31 L9), its macros expanded, audited, checked against the caller's role.
async fn write(app: &App, who: crate::routines::Who, sql: &str) -> Result<i64, Status> {
    no_sql_transaction(sql)?;
    if !statement::is_write(sql) {
        return Err(refused("22023", "not a write statement"));
    }
    let req = crate::routines::Request { sql: sql.to_string(), ..Default::default() };
    let how = How { job: None, spread: None, after: None, stale: None };
    let (returning, w) = (crate::returning::of(sql).is_some(), Worded::of(&who.scope));
    Ok(match statement::run(app, req, how, who, &FlightWire).await.map_err(w.err())? {
        Answer::Done(v) => v["rows"].as_i64().unwrap_or(0),
        // (sent as an update, a write with RETURNING answers how many rows it wrote; a CALL's rows are its own)
        got if returning => rows_of(w, got)?.iter().map(|b| b.num_rows() as i64).sum(),
        _ => 0,
    })
}

/// BEGIN, COMMIT, ROLLBACK and SAVEPOINT in SQL are refused, with or without a transaction id: a
/// Flight SQL transaction is begun and ended by its own actions, as the protocol has it. Every statement of
/// the text is checked (`SELECT 1; COMMIT` is a script, and running it would commit).
fn no_sql_transaction(sql: &str) -> Result<(), Status> {
    match crate::routines::split(sql).iter().any(|one| crate::text::control(one).is_some()) {
        true => Err(refused("0A000", crate::txn::PROTOCOL_OWN)),
        false => Ok(()),
    }
}

/// The session of a transaction a Flight SQL call names (`transaction_id`, from BeginTransaction), its caller's
/// (`user`, as BeginTransaction named it). One with no transaction open (ended, timed out, or never begun) is refused by
/// name, never run outside a transaction.
fn in_txn(user: &str, id: Option<&[u8]>) -> Result<Option<String>, Status> {
    let Some(id) = id else { return Ok(None) };
    let session = crate::temp::id(user, &String::from_utf8_lossy(id));
    match crate::txn::held(&session) {
        true => Ok(Some(session)),
        false => Err(refused("25P01", "no transaction is open under that transaction_id: it was ended, or never begun")),
    }
}

/// `f` in the session of the transaction the call names, when it names one (`in_txn`): its reads see the transaction's
/// snapshot and its own writes, as a POST /sql statement sent with its session does. Without one, `f` runs as it always has.
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
/// Before each batch, a caller whose rights changed signs in again (`again`, `signed_again`) and the
/// writer is opened again as it is now, so a REVOKE, a DROP USER or a token's end stops a load at its
/// next batch (`writer.rs`), as it stops a COPY's next chunk and a Kafka client's next request. `who` is the call's caller.
fn append(app: &App, who: crate::auth::Principal, table: String, producer: String, first: u64, batches: impl Stream<Item = Result<RecordBatch, Status>> + Send + 'static, again: Option<Again>) -> Result<Out<PutResult>, Status> {
    let log = app.log.clone().ok_or_else(|| refused("25006", "read-only node"))?;
    if !producer.is_empty() && first == 0 {
        return Err(refused("22023", crate::log::SEQ_FROM_1)); // (0 is "nothing yet": its batch would be taken for a retry)
    }
    let (app, log2, table2) = (app.clone(), log.clone(), table.clone());
    let mut load = crate::writer::Load::new(&app, who.clone(), "Flight DoPut", &table); // (its line in the audit log, as its acks end)
    let mut who = Some(who); // (the caller, for its grants in the task below)
    let (acks_tx, mut acks_rx) = tokio::sync::mpsc::channel::<Result<crate::writer::Queued, Status>>(256);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel(); // (acks are small; a client may read them only at the end)
    let node = app.lake.node.clone();
    node.tasks.spawn_as(crate::auth::caller(), async move {
        let mut batches = std::pin::pin!(batches);
        // (a refusal is said as the writer's caller is told: `Worded`)
        let worded = |who: &Option<crate::auth::Principal>| who.as_ref().map_or(PLAIN, |w| Worded::of(&w.scope));
        let denied_as = |who: &Option<crate::auth::Principal>, e: anyhow::Error| worded(who).denied(e);
        // (a keyed table's rows replace their keys', and rows marked _deleted delete them: S11)
        let opened = |who: Option<crate::auth::Principal>| crate::writer::Writer::open(&app, who, &table, crate::rules::How::Load, false);
        let opened_or = |who: &Option<crate::auth::Principal>, w: Result<crate::writer::Writer, crate::writer::Refused>| match w {
            Ok(w) => Ok(w),
            Err(crate::writer::Refused::Rights(e)) => Err(denied_as(who, e)),
            Err(_) => Err(refused("42P01", format!("no table {table}"))), // (SQL's names: ADR-022)
        };
        let mut writer = match opened_or(&who, opened(who.clone()).await) {
            Ok(w) => w,
            Err(e) => {
                let _ = acks_tx.send(Err(e)).await;
                return;
            }
        };
        let mut i = 0;
        while let Some(b) = batches.next().await {
            // (who may do what changed since the last batch: signed in again, and the writer opened again as it is now)
            if let (Some(w), Some(a)) = (&who, &again) {
                let now = match signed_again(&app, w, a).await {
                    None => Ok(None),
                    Some(Ok(p)) => opened_or(&Some(p.clone()), opened(Some(p.clone())).await).map(|w| Some((p, w))),
                    Some(Err(e)) => Err(e),
                };
                match now {
                    Ok(None) => {}
                    Ok(Some((p, w))) => (who, writer) = (Some(p), w),
                    Err(e) => {
                        let _ = acks_tx.send(Err(e)).await;
                        break;
                    }
                }
            }
            let queued = match b {
                Ok(b) => {
                    // (the batch's rows, shaped for the table: a column it leaves out takes its DEFAULT)
                    let given: Vec<String> = b.schema().fields().iter().map(|f| f.name().clone()).collect();
                    let left_out = writer.rules().left_out(&given);
                    let (seq, prev) = if producer.is_empty() { (0, None) } else { (first + i, (i > 0).then(|| first + i - 1)) };
                    let src = crate::log::Src { producer: producer.clone(), seq, prev };
                    writer.put(crate::rules::Rows::Named(b, left_out), src).await.map_err(|r| match r {
                        crate::writer::Refused::Rights(e) => denied_as(&who, e),
                        r => worded(&who).status(anyhow::Error::from(r)),
                    })
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
    let (log, table) = (log2, table2);
    node.tasks.spawn_as(crate::auth::caller(), async move {
        while let Some(queued) = acks_rx.recv().await {
            let acked = match queued {
                Ok(crate::writer::Queued { ack, src, rows: batch }) => {
                    // (overtaken by the batch after it on the way to the leader, it goes again: `Log::sent_again`)
                    // (an ack's error is the log's, never a routine's: told as to anyone, as this task outlives the call)
                    let ack = log.sent_again(ack.await, &table, &src, &batch).await.map_err(PLAIN.err());
                    ack.map(|a| {
                        let mut a = serde_json::to_value(a).unwrap_or_default();
                        a["rows"] = batch.rows().num_rows().into();
                        PutResult { app_metadata: a.to_string().into_bytes().into() }
                    })
                }
                Err(e) => Err(e),
            };
            if let Err(e) = &acked {
                load.end(Some(e.message().to_string()));
            }
            let stop = acked.is_err();
            if tx.send(acked).is_err() || stop {
                break;
            }
        }
        if acks_rx.is_closed() && acks_rx.is_empty() {
            load.end(None); // (every batch taken; a client gone before the last ack leaves the load unended: failed)
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
/// a message with `{"after": N}`. Once `revoked` resolves (`server::until_revoked`), the stream ends with 28000.
async fn log_stream(w: Worded, app: App, table: String, after: Option<u64>, columns: Option<Vec<String>>, follow: bool, revoked: impl std::future::Future<Output = ()> + Send + 'static) -> Result<Out<FlightData>, Status> {
    let meta = app.lake.cat.meta(&table_key(&table)).await.map_err(w.err())?.ok_or_else(|| refused("42P01", format!("no table {table}")))?;
    let full = crate::answer::schema(&meta.logical_columns()).map_err(w.err())?;
    let pick: Vec<usize> = match &columns {
        Some(cols) => cols.iter().map(|c| full.index_of(c).map_err(|_| refused("42703", format!("no column {c}")))).collect::<Result<_, _>>()?,
        None => meta.sql_order().iter().filter_map(|c| full.index_of(c).ok()).collect(), // (SQL's order, as `SELECT *` reads them)
    };
    let schema = Arc::new(full.project(&pick).map_err(w.err())?);
    let after = after.unwrap_or_else(|| app.lake.visible());
    let hwm = app.lake.hwm.subscribe();
    let first = FlightDataEncoderBuilder::new().with_schema(schema.clone()).build(futures::stream::empty()).map_err(move |e| w.stream(e));
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
                    let rows = crate::query::tail(&app.lake, &table, after, Some(now), false).await.map_err(w.err());
                    if follow && rows.as_ref().is_ok_and(|r| r.iter().all(|b| b.num_rows() == 0)) {
                        after = now; // (commits to other tables: nothing to send while following)
                        continue;
                    }
                    let chunk = rows.and_then(|rows| {
                        let rows = rows.iter().map(|b| b.project(&pick).map(crate::answer::compact)).collect::<Result<Vec<_>, _>>().map_err(w.err())?;
                        let mut data: Vec<Result<FlightData, Status>> = arrow_flight::utils::batches_to_flight_data(&schema, rows).map_err(w.err())?.into_iter().skip(1).map(Ok).collect(); // (the schema went first)
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
    // (ended for its sign-in, the client is told why: a stream that just stops reads as the node's going away)
    let (ended, why) = tokio::sync::oneshot::channel();
    let revoked = async move {
        revoked.await;
        let _ = ended.send(());
    };
    let told = futures::stream::once(why).filter_map(|r| async move { r.ok().map(|()| Err(refused("28000", REVOKED))) }); // (the stream done first: nothing)
    Ok(Box::pin(first.chain(chunks.flatten()).take_until(revoked).chain(told)))
}

const REVOKED: &str = "this stream's sign-in no longer holds, or may no longer read the table's rows (a grant taken back, its user dropped, its session expired): sign in again";

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

fn info(w: Worded, schema: &Schema, ticket: Vec<u8>, descriptor: FlightDescriptor) -> Result<Response<FlightInfo>, Status> {
    let info = FlightInfo::new().try_with_schema(schema).map_err(w.err())?.with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket))).with_descriptor(descriptor);
    Ok(Response::new(info))
}

/// The Flight SQL service.
struct Sql(App);

#[tonic::async_trait]
impl FlightSqlService for Sql {
    type FlightService = Sql;

    async fn do_handshake(&self, req: Request<Streaming<HandshakeRequest>>) -> Result<Response<Out<HandshakeResponse>>, Status> {
        // (signed in by `Gate` already; here a token is handed back: the password when it is one, a user's
        // `pt_` token included, or anything on a node nothing locks; a session when a user signed in)
        let credential = crate::auth::Credential::of_header(req.metadata().get("authorization").and_then(|v| v.to_str().ok()));
        let crate::auth::Credential::Password { password, .. } = credential else { return Err(refused("28P01", "wrong user name or password")) };
        let open = self.0.open().await;
        let token = match self.0.auth.token_role(&password) {
            // (a user's token is handed back as itself, which every call checks again: a session made from it
            // would outlive a DROP TOKEN and the token's own end by the session's hours, 11-06 audit H2)
            Role::None if !open && password.starts_with("pt_") => match caller(&req) {
                who if who.role > Role::None => password,
                _ => return Err(refused("28P01", "wrong user name or password")),
            },
            Role::None if !open => match caller(&req) {
                who if who.role > Role::None => match crate::sso::check(&self.0.lake, &password).await {
                    Some(Ok(c)) => crate::users::session_of(&self.0.lake, &c).await, // (a provider's: its session names it, ADR-074 §5)
                    _ => crate::users::session(&self.0.lake, &who.name).await,
                }
                .map_err(Worded::of(&who.scope).err())?
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
        let who = crate::routines::Who::signed(caller(&req), role, false); // (planned for its sign-in)
        let session = in_txn(who.name(), txn)?;
        let w = Worded::of(&who.scope);
        if crate::routines::runs_procedure(&q.query) || crate::returning::of(&q.query).is_some() {
            // A CALL (or DO), or a write with RETURNING: run now, as every door runs one; the ticket reads back what it answered.
            return kept(&self.0, &q.query, who, session, req.into_inner()).await;
        }
        if crate::write::parse(&q.query).is_some() {
            // A write sent as a query (DB-API's `execute`): done now; the ticket reads back the count.
            let (app, sql) = (self.0.clone(), q.query.clone());
            let rows = worked(w, app.lake.node.clone(), session, async move { write(&app, who, &sql).await }).await?;
            let ticket = TicketStatementQuery { statement_handle: format!("rows:{rows}").into_bytes().into() };
            return info(w, &crate::answer::arrow_schema(&rows_schema()), ticket.as_any().encode_to_vec(), req.into_inner());
        }
        // A query is planned here and run by its ticket; a script (several statements) can't be planned, so it runs now, as a CALL does.
        let Some((_, one)) = in_session(session.clone(), async { statement::plain_of(&self.0, &q.query, &who.scope).await.map_err(w.err()) }).await? else {
            return kept(&self.0, &q.query, who, session, req.into_inner()).await;
        };
        let schema = in_session(session, async { planned(&self.0, &one, &who.scope).await.map_err(w.err()) }).await?;
        let ticket = TicketStatementQuery { statement_handle: handle(&q.query, txn).into() };
        info(w, &schema, ticket.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_statement(&self, t: TicketStatementQuery, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        let role = allowed(&self.0, &req, Role::Read)?;
        let ((txn, sql), w) = (unhandle(&t.statement_handle), worded(&req));
        let sql = String::from_utf8(sql.to_vec()).map_err(w.err())?;
        let app = self.0.clone();
        let who = crate::routines::Who::signed(caller(&req), role, false);
        let session = in_txn(who.name(), txn)?;
        // (boxed: a statement's future is large, and each layer of `answered`'s and `worked`'s would copy it by value)
        Ok(Response::new(answered(w, app.lake.node.clone(), session, Box::pin(async move { answer(&app, &sql, who).await })).await?))
    }

    async fn do_action_begin_transaction(&self, _: ActionBeginTransactionRequest, req: Request<Action>) -> Result<ActionBeginTransactionResult, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let w = worded(&req);
        let id = format!("fl-{}", uuid::Uuid::new_v4().simple()); // (the client's handle; its session is the caller's own, named by it)
        crate::temp::with(&crate::temp::id(&caller(&req).name, &id), false, |_| Ok(())).map_err(w.err())?; // (held as a session: an idle one ends, its transaction rolled back, as `temp::reap` does)
        crate::temp::SESSION.scope(Some(crate::temp::id(&caller(&req).name, &id)), crate::txn::protocol(&self.0, "BEGIN")).await.map_err(w.err())?;
        Ok(ActionBeginTransactionResult { transaction_id: id.into_bytes().into() })
    }

    async fn do_action_end_transaction(&self, q: ActionEndTransactionRequest, req: Request<Action>) -> Result<(), Status> {
        allowed(&self.0, &req, Role::Read)?;
        let word = match EndTransaction::try_from(q.action) {
            Ok(EndTransaction::Commit) => "COMMIT",
            Ok(EndTransaction::Rollback) => "ROLLBACK",
            _ => return Err(refused("22023", "EndTransaction: commit or rollback")),
        };
        let session = in_txn(&caller(&req).name, Some(&q.transaction_id))?.unwrap_or_default();
        // (no statement text: COMMIT and ROLLBACK take none, only BEGIN's modes do)
        let (app, w) = (self.0.clone(), worded(&req));
        let out = worked(w, app.lake.node.clone(), Some(session.clone()), async move { crate::txn::protocol(&app, word).await.map_err(w.err()) }).await;
        crate::temp::end(&session); // (the session ends with its transaction, committed or not)
        let (tag, _) = out?;
        if word == "COMMIT" && tag == "ROLLBACK" {
            // (a failed transaction's COMMIT rolls back, as in Postgres: a client must learn it did not commit)
            return Err(refused("25P02", "the transaction had failed, so it was rolled back, not committed"));
        }
        Ok(())
    }

    async fn do_put_statement_update(&self, cmd: CommandStatementUpdate, req: Request<PeekableFlightDataStream>) -> Result<i64, Status> {
        let role = allowed(&self.0, &req, Role::Write)?;
        let (app, sql) = (self.0.clone(), cmd.query.clone());
        let who = crate::routines::Who::signed(caller(&req), role, false);
        let session = in_txn(who.name(), cmd.transaction_id.as_deref())?;
        worked(Worded::of(&who.scope), app.lake.node.clone(), session, async move { write(&app, who, &sql).await }).await
    }

    async fn do_put_statement_ingest(&self, cmd: CommandStatementIngest, req: Request<PeekableFlightDataStream>) -> Result<i64, Status> {
        allowed(&self.0, &req, Role::Write)?;
        if cmd.transaction_id.is_some() {
            // (an ingest appends through the log: its rows would commit at once, not with the transaction)
            return Err(refused("0A000", "an ingest inside a transaction isn't taken yet: ingest outside it, or INSERT the rows in it"));
        }
        let table = match cmd.schema.as_deref() {
            Some(schema) if !schema.is_empty() => crate::text::join(schema, &cmd.table), // (ADBC's db_schema_name)
            _ => cmd.table.clone(),
        };
        let again = req.extensions().get::<Again>().cloned(); // (signed in again as its rights change: `append`)
        let who = caller(&req); // (its sign-in)
        let w = Worded::of(&who.scope);
        let mut batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(req.into_inner().map_err(Into::into)).map_err(move |e| w.stream(e)).peekable();
        let mut exists = self.0.lake.cat.meta(&table_key(&table)).await.map_err(w.err())?.is_some();
        crate::writer::may(Some(&who), &crate::auth::catalog_name(&table), None, false).map_err(|e| {
            crate::writer::Load::refused(&self.0, who.clone(), "Flight DoPut", &table, &e); // (in the audit log, as every refusal at a door is)
            w.denied(e)
        })?; // (the row doors' one check)
        // ADBC's modes: create (fail if there), append (fail if not), replace, create_append.
        use arrow_flight::sql::{TableExistsOption as IfThere, TableNotExistOption as IfNot};
        let options = cmd.table_definition_options.unwrap_or_default();
        match (exists, IfThere::try_from(options.if_exists), IfNot::try_from(options.if_not_exist)) {
            (true, Ok(IfThere::Fail), _) => return Err(refused("42P07", format!("{table} exists (mode create): append to it, or replace it"))),
            (true, Ok(IfThere::Replace), _) => {
                crate::rights::may(&who, crate::rights::Action::Define, crate::rights::Object::Table(&table)).map_err(|e| w.denied(e))?; // (replacing it)
                let drop = crate::write::parse(&format!("DROP TABLE {}", crate::write::sql_name(&table))).ok_or_else(|| refused("XX000", "DROP TABLE"))?;
                crate::write::on_node(&self.0, drop, None, &who.scope).await.map_err(w.err())?;
                exists = false;
            }
            (false, _, Ok(IfNot::Fail)) => return Err(refused("42P01", format!("no table {table} (mode append): create it first, or use create_append"))),
            _ => {}
        }
        if !exists {
            // ADBC's create modes: a table from the stream's schema (strings as Utf8, times in µs).
            crate::rights::may(&who, crate::rights::Action::Define, crate::rights::Object::Table(&table)).map_err(|e| w.denied(e))?; // (creating it)
            let schema = match std::pin::Pin::new(&mut batches).peek().await {
                Some(Ok(b)) => b.schema(),
                _ => return Err(refused("22023", "no rows to take the table's columns from")),
            };
            let columns: Vec<(String, String)> = schema.fields().iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()))).collect();
            crate::write::define(&self.0, &table, &serde_json::json!(columns).to_string()).await.map_err(w.err())?;
        }
        let mut acks = append(&self.0, who, table, String::new(), 0, batches, again)?;
        let mut rows = 0i64;
        while let Some(ack) = acks.next().await {
            let ack: serde_json::Value = serde_json::from_slice(&ack?.app_metadata).unwrap_or_default();
            rows += ack["rows"].as_i64().unwrap_or(0);
        }
        Ok(rows)
    }

    async fn do_put_fallback(&self, req: Request<PeekableFlightDataStream>, _: Any) -> Result<Response<Out<PutResult>>, Status> {
        allowed(&self.0, &req, Role::Write)?;
        let again = req.extensions().get::<Again>().cloned(); // (signed in again as its rights change: `append`)
        let who = caller(&req); // (its sign-in)
        let w = Worded::of(&who.scope);
        // A path descriptor: [table] or [table, producer, first seq].
        let mut input = req.into_inner();
        let first = std::pin::Pin::new(&mut input).peek().await.cloned().transpose()?.and_then(|d| d.flight_descriptor);
        let path = first.map(|d| d.path).unwrap_or_default();
        let (table, producer, seq) = match &path[..] {
            [t] => (t.clone(), String::new(), 0),
            [t, p, s] => (t.clone(), p.clone(), s.parse().map_err(|_| refused("22023", "the first seq is a number"))?),
            _ => return Err(refused("22023", "DoPut to a path: [table] or [table, producer, first seq]")),
        };
        crate::writer::may(Some(&who), &crate::auth::catalog_name(&table), None, false).map_err(|e| {
            crate::writer::Load::refused(&self.0, who.clone(), "Flight DoPut", &table, &e); // (in the audit log, as every refusal at a door is)
            w.denied(e)
        })?; // (the row doors' one check, before a batch is read)
        let batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(input.map_err(Into::into)).map_err(move |e| w.stream(e));
        Ok(Response::new(append(&self.0, who, table, producer, seq, batches, again)?))
    }

    async fn get_flight_info_sql_info(&self, q: CommandGetSqlInfo, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(worded(&req), q.clone().into_builder(&INFO).schema().as_ref(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_sql_info(&self, q: CommandGetSqlInfo, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        let (b, w) = (q.into_builder(&INFO), worded(&req));
        let (schema, batch) = (b.schema(), b.build().map_err(w.err())?);
        Ok(Response::new(send(w, schema, vec![batch])))
    }

    async fn get_flight_info_catalogs(&self, q: CommandGetCatalogs, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(worded(&req), &q.clone().into_builder().schema(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_catalogs(&self, q: CommandGetCatalogs, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let (mut b, w) = (q.into_builder(), worded(&req));
        b.append(crate::ddl::lake_name(&self.0.lake));
        Ok(Response::new(send(w, b.schema(), vec![b.build().map_err(w.err())?])))
    }

    async fn get_flight_info_schemas(&self, q: CommandGetDbSchemas, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(worded(&req), &q.clone().into_builder().schema(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_schemas(&self, q: CommandGetDbSchemas, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let (mut b, w) = (q.into_builder(), worded(&req));
        for schema in crate::ddl::schemas(&self.0.lake).await.map_err(w.err())? {
            b.append(crate::ddl::lake_name(&self.0.lake), schema);
        }
        Ok(Response::new(send(w, b.schema(), vec![b.build().map_err(w.err())?])))
    }

    async fn get_flight_info_table_types(&self, q: CommandGetTableTypes, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(worded(&req), &table_types().schema(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_table_types(&self, _: CommandGetTableTypes, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let types = table_types();
        Ok(Response::new(send(worded(&req), types.schema(), vec![types])))
    }

    async fn get_flight_info_tables(&self, q: CommandGetTables, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(worded(&req), &q.clone().into_builder().schema(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_tables(&self, q: CommandGetTables, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let (mut b, lake, scope) = (q.into_builder(), &self.0.lake, caller(&req).scope);
        let w = Worded::of(&scope);
        // (a user's: the tables and views it may read, and of one granted in part those columns, as every listing: S27)
        let reads = |shown: &Option<std::collections::HashSet<String>>, c: &str| shown.as_ref().is_none_or(|s| s.contains(c));
        for (key, meta) in lake.cat.scan::<TableMeta>("t/", "t0").await.map_err(w.err())?.into_iter().filter(|(k, _)| !crate::sys::hidden(k)) {
            let Some(shown) = scope.columns(&key[2..]) else { continue };
            let (schema, table) = crate::text::split(&key[2..]);
            let columns: Vec<_> = meta.sql_columns().into_iter().filter(|(c, _)| reads(&shown, c)).collect();
            let columns = crate::answer::schema(&columns).map_err(w.err())?; // (SQL's order)
            b.append(crate::ddl::lake_name(lake), schema, table, "TABLE", &columns).map_err(w.err())?;
        }
        for (key, _) in lake.cat.scan::<crate::ddl::StoredView>("q/", "q0").await.map_err(w.err())? {
            let Some(shown) = scope.columns(&key[2..]) else { continue };
            let (schema, view) = crate::text::split(&key[2..]);
            if let Ok(columns) = plan_schema(&self.0, &format!("SELECT * FROM {}", crate::write::sql_name(&key[2..])), &scope).await {
                let columns = Schema::new(columns.fields().iter().filter(|f| reads(&shown, f.name())).cloned().collect::<Vec<_>>());
                b.append(crate::ddl::lake_name(lake), schema, view, "VIEW", &columns).map_err(w.err())?;
            }
        }
        Ok(Response::new(send(w, b.schema(), vec![b.build().map_err(w.err())?])))
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
            let (app, who) = (&self.0 .0, caller(&req));
            allowed(app, &req, Role::Read)?;
            let (w, mut infos) = (Worded::of(&who.scope), vec![]);
            // (each a flight of `SELECT *`: listed for a table it may read every column of, as its ticket would be refused
            // naming none for one granted in part, and its info would be: 11-08 audit M6)
            for (key, _) in app.lake.cat.scan::<TableMeta>("t/", "t0").await.map_err(w.err())?.into_iter().filter(|(k, _)| !crate::sys::hidden(k) && crate::rights::may(&who, crate::rights::Action::SelectAll, crate::rights::Object::Table(&k[2..])).is_ok()) {
                let table = &key[2..];
                // (the schema its ticket streams, planned as DoGet runs it: a table's strings are Utf8View, not the table's Utf8)
                let sql = format!("SELECT * FROM {}", crate::write::sql_name(table));
                let ticket = serde_json::json!({"sql": sql}).to_string();
                let schema = plan_schema(app, &sql, &who.scope).await.map_err(w.err())?;
                infos.push(Ok(FlightInfo::new().try_with_schema(&schema).map_err(w.err())?.with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket))).with_descriptor(FlightDescriptor::new_path(vec![table.to_string()]))));
            }
            Ok(Response::new(Box::pin(futures::stream::iter(infos)) as Self::ListFlightsStream))
        }).await
    }

    async fn get_flight_info(&self, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        let who = caller(&req);
        crate::auth::WHO.scope(who, async move {
            let Some(json) = Json::parse(&req.get_ref().cmd) else { return self.0.get_flight_info(req).await.map_err(coded) };
            let (app, who) = (&self.0 .0, caller(&req));
            allowed(app, &req, Role::Read)?;
            let w = Worded::of(&who.scope);
            let schema = match (&json.sql, &json.table) {
                (Some(sql), _) => plan_schema(app, sql, &who.scope).await.map_err(w.err())?,
                (None, Some(table)) => {
                    // Its schema names every column: a user needs SELECT on all of them, as its log stream does (checked before the table is looked up, so an unreadable one isn't told apart).
                    crate::rights::may(&who, crate::rights::Action::SelectAll, crate::rights::Object::Table(table)).map_err(|e| w.denied(e))?;
                    let meta = app.lake.cat.meta(&table_key(table)).await.map_err(w.err())?.ok_or_else(|| refused("42P01", format!("no table {table}")))?;
                    let full = crate::answer::schema(&meta.logical_columns()).map_err(w.err())?;
                    let order = meta.sql_order(); // (no columns asked: SQL's order, as the log stream sends them)
                    let pick = json.columns.as_ref().unwrap_or(&order).iter().filter_map(|c| full.index_of(c).ok()).collect::<Vec<_>>();
                    full.project(&pick).map_err(w.err())?
                }
                _ => return Err(refused("22023", "{\"sql\": …} or {\"table\": …}")),
            };
            let cmd = req.get_ref().cmd.to_vec();
            info(w, &schema, cmd, req.into_inner())
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
            let (app, signed) = (&self.0 .0, caller(&req));
            let role = allowed(app, &req, Role::Read)?;
            let w = Worded::of(&signed.scope);
            match (json.sql, json.table) {
                (Some(sql), _) => {
                    let app = app.clone();
                    let who = crate::routines::Who::signed(signed, role, false);
                    Ok(Response::new(answered(w, app.lake.node.clone(), None, Box::pin(async move { query(&app, &sql, who).await })).await?))
                }
                (None, Some(table)) => {
                    let name = crate::auth::catalog_name(&table);
                    crate::rights::may(&signed, crate::rights::Action::SelectAll, crate::rights::Object::Table(&name)).map_err(|e| w.denied(e))?; // (a stream of rows: every column, as `/watch` needs)
                    // (the rows come after this call's scope has ended: its sign-in is checked again, and the table's whole rows
                    // with it, so a REVOKE, a DROP USER or a session's end stops them within a second, as at `/watch`: 11-03 audit H1)
                    let header = req.metadata().get("authorization").and_then(|v| v.to_str().ok()).map(String::from);
                    let revoked = crate::server::until_revoked(app.clone(), Some(signed), crate::auth::operator(), header, move |p| {
                        crate::rights::may(p, crate::rights::Action::SelectAll, crate::rights::Object::Table(&name)).is_ok()
                    });
                    Ok(Response::new(log_stream(w, app.clone(), table, json.after, json.columns, json.follow, revoked).await?))
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

    /// `rows` rows: an integer, and a text of about 40 bytes, so a batch of several pieces of Arrow.
    fn wide(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false), Field::new("s", DataType::Utf8, true)]));
        let ints: ArrayRef = Arc::new(datafusion::arrow::array::Int64Array::from_iter_values(0..rows as i64));
        let texts: ArrayRef = Arc::new(StringArray::from_iter_values((0..rows).map(|i| format!("row {i} with a little padding text"))));
        RecordBatch::try_new(schema, vec![ints, texts]).expect("a batch")
    }

    #[test]
    fn messages_of_the_pieces_are_the_answer() -> anyhow::Result<()> {
        let batch = wide(300_000);
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

        let sent: Vec<FlightData> = whole(PLAIN, schema.clone(), vec![batch], crate::node_state::NodeState::unhosted().drain.busy()).try_collect().await?;
        let streamed = arrow_flight::utils::flight_data_to_batches(&sent)?;
        assert_eq!(names_and_types(streamed[0].schema().as_ref()), names_and_types(&schema));
        assert_eq!(streamed[0].column(0).data_type(), &DataType::Utf8, "a dictionary arrives as its values");

        let empty: Vec<FlightData> = whole(PLAIN, schema.clone(), vec![], crate::node_state::NodeState::unhosted().drain.busy()).try_collect().await?;
        assert_eq!(empty.len(), 1, "the schema alone");
        assert_eq!(names_and_types(&Schema::try_from(&empty[0])?), names_and_types(&schema));
        Ok(())
    }

    #[tokio::test]
    async fn a_kept_answer_streams_the_messages_whole_would_make() -> anyhow::Result<()> {
        let dict = dictionary();
        let plan = Schema::new(vec![Field::new("d", dict.clone(), false), Field::new("s", DataType::Utf8View, false)]);
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let view: ArrayRef = Arc::new(StringViewArray::from(vec!["x", "y"]));
        let batch = RecordBatch::try_new(Arc::new(plan), vec![cast(&strings, &dict)?, view])?;
        let kept = crate::answer::render(&[batch.clone()], Some("arrow"), &|_| None)?;
        let a: Vec<FlightData> = sent_as_kept(PLAIN, kept, crate::node_state::NodeState::unhosted().drain.busy()).try_collect().await?;
        let b: Vec<FlightData> = whole(PLAIN, Arc::new(first_announced(&[batch.clone()])), vec![batch], crate::node_state::NodeState::unhosted().drain.busy()).try_collect().await?;
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.data_header, y.data_header);
            assert_eq!(x.data_body, y.data_body);
        }
        let back = arrow_flight::utils::flight_data_to_batches(&a)?;
        assert_eq!(back.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
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
    fn no_sql_transaction_refuses_a_transaction_word_in_any_statement() {
        for sql in ["SELECT 1; COMMIT", "select 1;\nrollback", "BEGIN; SELECT 1"] {
            assert!(no_sql_transaction(sql).is_err(), "{sql:?} runs a transaction word");
        }
        for sql in ["SELECT 1", "SELECT 'COMMIT'", "SELECT 1; SELECT 2"] {
            assert!(no_sql_transaction(sql).is_ok(), "{sql:?} runs none");
        }
    }

    /// A call's errors are worded for its caller (`Worded`): a routine's lines in full to a caller shown code, without their
    /// text to any other, and as to the latter where no request is there to ask (`PLAIN`).
    #[test]
    fn a_calls_errors_are_worded_for_its_caller() {
        let e = || anyhow::anyhow!("ValueError: bad row\n  line 3: rate = 42");
        assert_eq!(Worded::of(&crate::rights::Scope::everything()).status(e()).message(), "ValueError: bad row\n  line 3: rate = 42");
        assert_eq!(Worded::of(&crate::rights::Scope::nothing()).status(e()).message(), "ValueError: bad row\n  line 3");
        assert_eq!(PLAIN.status(e()).message(), "ValueError: bad row\n  line 3");
        assert_eq!(PLAIN.status(e()).metadata().get("x-pondra-sqlstate").unwrap(), crate::codes::of(&e()));
    }

    #[test]
    fn stream_status_of_an_arrow_error_carries_a_sqlstate() {
        let e = FlightError::Arrow(datafusion::arrow::error::ArrowError::ComputeError("no".into()));
        assert!(PLAIN.stream(e).metadata().contains_key("x-pondra-sqlstate"));
    }
}
