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
//! - **Tokens:** `authorization: Bearer <token>`, or a handshake with basic auth whose password is
//!   the token (what ADBC and pyarrow's `authenticate_basic_token` send).
use crate::auth::Role;
use crate::server::App;
use crate::store::{table_key, TableMeta};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTables, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use serde::Deserialize;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use tonic::{Request, Response, Status, Streaming};

type Out<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

pub async fn serve(app: App, addr: String) -> anyhow::Result<()> {
    let door = Door(Sql(app));
    let service = FlightServiceServer::new(door).max_decoding_message_size(1 << 30).max_encoding_message_size(1 << 30);
    tonic::transport::Server::builder().add_service(service).serve(addr.parse()?).await?;
    Ok(())
}

fn status(e: impl std::fmt::Display) -> Status { Status::internal(format!("{e:#}")) }

/// The caller's role, from its bearer token; refused below `need`.
fn allowed<T>(app: &App, req: &Request<T>, need: Role) -> Result<Role, Status> {
    let token = req.metadata().get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    let role = app.auth.role(token);
    if role < need {
        return Err(Status::unauthenticated("this needs a token with more rights"));
    }
    Ok(role)
}

/// Batches as a Flight stream (the schema first, so an empty result still has columns).
fn send(schema: SchemaRef, batches: Vec<RecordBatch>) -> Out<FlightData> {
    let stream = futures::stream::iter(batches.into_iter().map(Ok));
    Box::pin(FlightDataEncoderBuilder::new().with_schema(schema).build(stream).map_err(Status::from))
}

/// Run a query (spread over the cluster when it pays), with its schema.
async fn query(app: &App, sql: &str) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)> {
    let batches = app.query(sql, None).await?;
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => Arc::new(plan_schema(app, sql).await?),
    };
    Ok((schema, batches))
}

async fn plan_schema(app: &App, sql: &str) -> anyhow::Result<Schema> {
    let ctx = crate::query::session(&app.lake, sql, "").await?;
    Ok(ctx.sql_with_options(&crate::asof::rewrite(sql)?, crate::query::read_only()).await?.schema().as_arrow().clone())
}

/// A write sent as SQL: a CREATE, INSERT, UPDATE, DELETE or ALTER. Returns the rows written.
async fn write(app: &App, role: Role, sql: &str) -> Result<i64, Status> {
    let stmt = crate::write::parse(sql).ok_or_else(|| Status::invalid_argument("not a write statement"))?;
    app.auth.allows(role, &stmt).map_err(|e| Status::permission_denied(e.to_string()))?;
    let done = crate::write::on_node(app, stmt, None).await.map_err(status)?;
    Ok(done["rows"].as_i64().unwrap_or(0))
}

/// Appends batches to `table` through the log, pipelined: each is queued as it arrives and
/// answered with its ack when committed. `producer` + `first`: batch i is seq `first + i`.
fn append(app: &App, table: String, producer: String, first: u64, batches: impl Stream<Item = Result<RecordBatch, Status>> + Send + 'static) -> Result<Out<PutResult>, Status> {
    let log = app.log.clone().ok_or_else(|| Status::failed_precondition("read-only node"))?;
    let (lake, again, table2) = (app.lake.clone(), log.clone(), table.clone());
    type Queued = Pin<Box<dyn std::future::Future<Output = anyhow::Result<crate::log::Ack>> + Send>>;
    let (acks_tx, mut acks_rx) = tokio::sync::mpsc::channel::<Result<(Queued, RecordBatch, crate::log::Src), Status>>(256);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel(); // (acks are small; a client may read them only at the end)
    tokio::spawn(async move {
        let mut batches = std::pin::pin!(batches);
        let meta: Option<TableMeta> = lake.cat.get(&table_key(&table)).await.ok().flatten();
        let Some(schema) = meta.and_then(|m| crate::query::schema(&m.columns).ok()) else {
            let _ = acks_tx.send(Err(Status::not_found(format!("no table {table}")))).await;
            return;
        };
        let mut i = 0;
        while let Some(b) = batches.next().await {
            let queued = match b.and_then(|b| crate::query::conform(&b, &schema).map_err(status)) {
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
    tokio::spawn(async move {
        while let Some(queued) = acks_rx.recv().await {
            let acked = match queued {
                Ok((ack, batch, src)) => {
                    let mut ack = ack.await.map_err(status);
                    // Overtaken by the batch after it (flushes can race on the way to the leader):
                    // its predecessor is committed by now, so it goes again.
                    for _ in 0..100 {
                        match &ack {
                            Ok(a) if a.conflict => ack = async { log.queue(table.clone(), src.clone(), batch.clone()).await?.await }.await.map_err(status),
                            _ => break,
                        }
                    }
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
    let meta: TableMeta = app.lake.cat.get(&table_key(&table)).await.map_err(status)?.ok_or_else(|| Status::not_found(format!("no table {table}")))?;
    let full = crate::query::schema(&meta.columns).map_err(status)?;
    let pick: Vec<usize> = match &columns {
        Some(cols) => cols.iter().map(|c| full.index_of(c).map_err(|_| Status::invalid_argument(format!("no column {c}")))).collect::<Result<_, _>>()?,
        None => (0..full.fields().len()).collect(),
    };
    let schema = Arc::new(full.project(&pick).map_err(status)?);
    let after = after.unwrap_or_else(|| app.lake.visible());
    let hwm = app.lake.hwm.subscribe();
    let first = FlightDataEncoderBuilder::new().with_schema(schema.clone()).build(futures::stream::empty()).map_err(Status::from);
    let chunks = futures::stream::unfold((app, hwm, after, false), move |(app, mut hwm, after, done)| {
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
                    let chunk = rows.and_then(|rows| {
                        let rows = rows.iter().map(|b| b.project(&pick)).collect::<Result<Vec<_>, _>>().map_err(status)?;
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
    b.append(SqlInfo::FlightSqlServerTransaction, 0i32); // (none)
    b.build().expect("static SQL info")
});

/// A statement's FlightInfo: its schema, and a ticket to fetch it.
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
        use base64::Engine;
        let basic = req.metadata().get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Basic "));
        let token = basic.and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok()).and_then(|b| String::from_utf8(b).ok()).and_then(|up| up.split_once(':').map(|(_, p)| p.to_string()));
        if self.0.auth.role(token.as_deref()) == Role::None {
            return Err(Status::unauthenticated("wrong token"));
        }
        let token = token.unwrap_or_default();
        let mut res: Response<Out<HandshakeResponse>> = Response::new(Box::pin(futures::stream::iter([Ok(HandshakeResponse { protocol_version: 0, payload: token.clone().into_bytes().into() })])));
        if let Ok(v) = format!("Bearer {token}").parse() {
            res.metadata_mut().insert("authorization", v);
        }
        Ok(res)
    }

    async fn get_flight_info_statement(&self, q: CommandStatementQuery, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        let role = allowed(&self.0, &req, Role::Read)?;
        if crate::write::parse(&q.query).is_some() {
            // A write sent as a query (DB-API's `execute`): done now; the ticket reads back the count.
            let rows = write(&self.0, role, &q.query).await?;
            let schema = Schema::new(vec![datafusion::arrow::datatypes::Field::new("rows", datafusion::arrow::datatypes::DataType::Int64, false)]);
            let ticket = TicketStatementQuery { statement_handle: format!("rows:{rows}").into_bytes().into() };
            return info(&schema, ticket.as_any().encode_to_vec(), req.into_inner());
        }
        let schema = plan_schema(&self.0, &q.query).await.map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        let ticket = TicketStatementQuery { statement_handle: q.query.into_bytes().into() };
        info(&schema, ticket.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_statement(&self, t: TicketStatementQuery, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let sql = String::from_utf8(t.statement_handle.to_vec()).map_err(status)?;
        if let Some(n) = sql.strip_prefix("rows:").and_then(|n| n.parse::<i64>().ok()) {
            let batch = RecordBatch::try_from_iter([("rows", Arc::new(datafusion::arrow::array::Int64Array::from(vec![n])) as _)]).map_err(status)?;
            return Ok(Response::new(send(batch.schema(), vec![batch])));
        }
        let (schema, batches) = query(&self.0, &sql).await.map_err(status)?;
        Ok(Response::new(send(schema, batches)))
    }

    async fn do_put_statement_update(&self, cmd: CommandStatementUpdate, req: Request<PeekableFlightDataStream>) -> Result<i64, Status> {
        let role = allowed(&self.0, &req, Role::Write)?;
        write(&self.0, role, &cmd.query).await
    }

    async fn do_put_statement_ingest(&self, cmd: CommandStatementIngest, req: Request<PeekableFlightDataStream>) -> Result<i64, Status> {
        let role = allowed(&self.0, &req, Role::Write)?;
        let table = match cmd.schema.as_deref() {
            Some(schema) if !schema.is_empty() => crate::ddl::join(schema, &cmd.table), // (ADBC's db_schema_name)
            _ => cmd.table.clone(),
        };
        let mut batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(req.into_inner().map_err(Into::into)).map_err(Status::from).peekable();
        let exists = self.0.lake.cat.get::<TableMeta>(&table_key(&table)).await.map_err(status)?.is_some();
        if !exists {
            // ADBC's create modes: a table from the stream's schema (strings as Utf8, times in µs).
            if role < Role::Admin {
                return Err(Status::permission_denied("creating a table needs the admin token"));
            }
            let schema = match std::pin::Pin::new(&mut batches).peek().await {
                Some(Ok(b)) => b.schema(),
                _ => return Err(Status::invalid_argument("no rows to take the table's columns from")),
            };
            let columns: Vec<(String, String)> = schema.fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
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
            [t, p, s] => (t.clone(), p.clone(), s.parse().map_err(|_| Status::invalid_argument("the first seq is a number"))?),
            _ => return Err(Status::invalid_argument("DoPut to a path: [table] or [table, producer, first seq]")),
        };
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

    async fn get_flight_info_tables(&self, q: CommandGetTables, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info(&q.clone().into_builder().schema(), q.as_any().encode_to_vec(), req.into_inner())
    }

    async fn do_get_tables(&self, q: CommandGetTables, req: Request<Ticket>) -> Result<Response<Out<FlightData>>, Status> {
        allowed(&self.0, &req, Role::Read)?;
        let (mut b, lake) = (q.into_builder(), &self.0.lake);
        for (key, meta) in lake.cat.scan::<TableMeta>("t/", "t0").await.map_err(status)?.into_iter().filter(|(k, _)| !crate::sys::hidden(k)) {
            let (schema, table) = crate::ddl::split(&key[2..]);
            let columns = crate::query::schema(&meta.columns).map_err(status)?;
            b.append(crate::ddl::lake_name(lake), schema, table, "TABLE", &columns).map_err(status)?;
        }
        for (key, _) in lake.cat.scan::<crate::ddl::StoredView>("q/", "q0").await.map_err(status)? {
            let (schema, view) = crate::ddl::split(&key[2..]);
            if let Ok(columns) = plan_schema(&self.0, &format!("SELECT * FROM {}", crate::write::sql_name(&key[2..]))).await {
                b.append(crate::ddl::lake_name(lake), schema, view, "VIEW", &columns).map_err(status)?;
            }
        }
        Ok(Response::new(send(b.schema(), vec![b.build().map_err(status)?])))
    }

    async fn register_sql_info(&self, _: i32, _: &SqlInfo) {}
}

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
        let app = &self.0 .0;
        allowed(app, &req, Role::Read)?;
        let mut infos = vec![];
        for (key, meta) in app.lake.cat.scan::<TableMeta>("t/", "t0").await.map_err(status)?.into_iter().filter(|(k, _)| !crate::sys::hidden(k)) {
            let table = &key[2..];
            let ticket = serde_json::json!({"sql": format!("SELECT * FROM {}", crate::write::sql_name(table))}).to_string();
            let schema = crate::query::schema(&meta.columns).map_err(status)?;
            infos.push(Ok(FlightInfo::new().try_with_schema(&schema).map_err(status)?.with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket))).with_descriptor(FlightDescriptor::new_path(vec![table.to_string()]))));
        }
        Ok(Response::new(Box::pin(futures::stream::iter(infos))))
    }

    async fn get_flight_info(&self, req: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        let Some(json) = Json::parse(&req.get_ref().cmd) else { return self.0.get_flight_info(req).await };
        let app = &self.0 .0;
        allowed(app, &req, Role::Read)?;
        let schema = match (&json.sql, &json.table) {
            (Some(sql), _) => plan_schema(app, sql).await.map_err(|e| Status::invalid_argument(format!("{e:#}")))?,
            (None, Some(table)) => {
                let meta: TableMeta = app.lake.cat.get(&table_key(table)).await.map_err(status)?.ok_or_else(|| Status::not_found(format!("no table {table}")))?;
                let full = crate::query::schema(&meta.columns).map_err(status)?;
                let pick = json.columns.iter().flatten().filter_map(|c| full.index_of(c).ok()).collect::<Vec<_>>();
                if json.columns.is_some() { full.project(&pick).map_err(status)? } else { full.as_ref().clone() }
            }
            _ => return Err(Status::invalid_argument("{\"sql\": …} or {\"table\": …}")),
        };
        let cmd = req.get_ref().cmd.to_vec();
        info(&schema, cmd, req.into_inner())
    }

    async fn poll_flight_info(&self, req: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> { self.0.poll_flight_info(req).await }

    async fn get_schema(&self, req: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> { self.0.get_schema(req).await }

    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let Some(json) = Json::parse(&req.get_ref().ticket) else { return self.0.do_get(req).await };
        let app = &self.0 .0;
        allowed(app, &req, Role::Read)?;
        match (json.sql, json.table) {
            (Some(sql), _) => {
                let (schema, batches) = query(app, &sql).await.map_err(status)?;
                Ok(Response::new(send(schema, batches)))
            }
            (None, Some(table)) => Ok(Response::new(log_stream(app.clone(), table, json.after, json.columns, json.follow).await?)),
            _ => Err(Status::invalid_argument("{\"sql\": …} or {\"table\": …}")),
        }
    }

    async fn do_put(&self, req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> { self.0.do_put(req).await }

    async fn do_action(&self, req: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> { self.0.do_action(req).await }

    async fn list_actions(&self, req: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> { self.0.list_actions(req).await }

    async fn do_exchange(&self, req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> { self.0.do_exchange(req).await }
}
