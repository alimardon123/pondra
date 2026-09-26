//! The Kafka protocol (`--kafka 0.0.0.0:9092`): Kafka producers and consumers — the Java client,
//! librdkafka (Python, Go, .NET, C/C++), kafka-python, Kafka Connect, Debezium — talk to any node
//! as they would to a broker. A topic is a table, with one partition.
//!
//! - **Producing:** each record's value is a JSON row. Tables with `_key` / `_timestamp` columns
//!   also get the record's key and timestamp; a table with a `_value` column takes values raw
//!   (any format). Debezium change events are unwrapped (`after`; a delete removes `before`), and
//!   a null value (a tombstone) deletes its key from a keyed table. Idempotent producers are
//!   exactly-once: their producer id and sequence become a Pondra producer and seq.
//! - **Consuming:** a topic reads the table's log, as far back as it is kept (`--changelog-secs`).
//!   A record's offset is its `_ord` — (segment << 32) + row: increasing, but not dense. Keyed
//!   tables' deletes arrive as tombstones.
//! - **Tokens:** with tokens set, clients log in with SASL/PLAIN: the user name picks the role
//!   (`reader`, `writer`, `admin`), the password is its token.
//!
//! - **Consumer groups:** the leader coordinates them (members, generations, assignments);
//!   committed offsets live in the catalog.
//!
//! Every node answers for every topic (it lists itself as the leader), so clients spread over
//! nodes by the address they start from. Not supported: transactions.
use crate::auth::Role;
use crate::log::{Ack, Src};
use crate::query::schema;
use crate::server::App;
use crate::store::{seg_key, table_key, Segment, TableMeta};
use anyhow::{anyhow, bail, ensure, Result};
use bytes::{BufMut, Bytes};
use datafusion::arrow::array::{new_null_array, Array, ArrayRef, AsArray, BinaryArray, BooleanArray, StringArray, TimestampMillisecondArray};
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::{json as arrow_json, record_batch::RecordBatch};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// The requests we answer and their version ranges (none needs the "flexible" encoding, except
/// ApiVersions v3's request header).
const APIS: &[(i16, i16, i16)] = &[
    (PRODUCE, 3, 8),
    (FETCH, 4, 11),
    (LIST_OFFSETS, 1, 5),
    (METADATA, 1, 8),
    (SASL_HANDSHAKE, 1, 1),
    (API_VERSIONS, 0, 3),
    (INIT_PRODUCER_ID, 0, 1),
    (SASL_AUTHENTICATE, 0, 1),
    (OFFSET_COMMIT, 2, 7),
    (OFFSET_FETCH, 1, 5),
    (FIND_COORDINATOR, 0, 2),
    (JOIN_GROUP, 0, 5),
    (HEARTBEAT, 0, 3),
    (LEAVE_GROUP, 0, 3),
    (SYNC_GROUP, 0, 3),
];
const PRODUCE: i16 = 0;
const FETCH: i16 = 1;
const LIST_OFFSETS: i16 = 2;
const METADATA: i16 = 3;
const OFFSET_COMMIT: i16 = 8;
const OFFSET_FETCH: i16 = 9;
const FIND_COORDINATOR: i16 = 10;
const JOIN_GROUP: i16 = 11;
const HEARTBEAT: i16 = 12;
const LEAVE_GROUP: i16 = 13;
const SYNC_GROUP: i16 = 14;
const SASL_HANDSHAKE: i16 = 17;
const API_VERSIONS: i16 = 18;
const INIT_PRODUCER_ID: i16 = 22;
const SASL_AUTHENTICATE: i16 = 36;

// Kafka error codes
const OFFSET_OUT_OF_RANGE: i16 = 1;
const UNKNOWN_TOPIC: i16 = 3;
const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const UNSUPPORTED_VERSION: i16 = 35;
const OUT_OF_ORDER_SEQUENCE: i16 = 45;
const TRANSACTIONS_UNSUPPORTED: i16 = 53; // (TRANSACTIONAL_ID_AUTHORIZATION_FAILED: not retried)
const SASL_FAILED: i16 = 58;
const INVALID_RECORD: i16 = 87;
const POLICY_VIOLATION: i16 = 44;

/// How this node appears to clients.
struct Broker {
    id: i32,
    host: String,
    port: i32,
}

/// Answer Kafka clients on `addr`; tell them to come back to `advertise` (host:port).
pub async fn serve(app: App, addr: String, advertise: String) -> Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    let (host, port) = advertise.rsplit_once(':').ok_or_else(|| anyhow!("--kafka-advertise is host:port"))?;
    let id = (advertise.bytes().fold(0u32, |h, b| h.wrapping_mul(31).wrapping_add(b as u32)) & 0x7fff_ffff) as i32; // stable per address
    let me = Arc::new(Broker { id, host: host.into(), port: port.parse()? });
    let _ = ME.set(serde_json::json!({"id": me.id, "host": me.host, "port": me.port}));
    loop {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let (app, me) = (app.clone(), me.clone());
        tokio::spawn(async move { conn(app, me, socket).await.map_err(|e| eprintln!("kafka client: {e:#}")) });
    }
}

type Reply = BoxFuture<'static, Option<Vec<u8>>>; // a response frame, or None (acks=0 produce)

/// One client connection. Requests are read and started in order (produced rows are queued at
/// once), and their responses are written in that order as each completes: many in flight.
async fn conn(app: App, me: Arc<Broker>, socket: TcpStream) -> Result<()> {
    let (mut rd, mut wr) = socket.into_split();
    let (tx, mut rx) = mpsc::channel::<Reply>(64);
    let writer = tokio::spawn(async move {
        while let Some(reply) = rx.recv().await {
            if let Some(frame) = reply.await {
                wr.write_all(&frame).await?;
            }
        }
        anyhow::Ok(())
    });
    let mut role = if app.auth.on() { Role::None } else { Role::Admin };
    while let Ok(size) = rd.read_i32().await {
        ensure!((0..=128 << 20).contains(&size), "request of {size} bytes");
        let mut buf = vec![0; size as usize];
        rd.read_exact(&mut buf).await?;
        let reply = request(&app, &me, &mut role, Bytes::from(buf)).await?;
        if tx.send(reply).await.is_err() {
            break;
        }
    }
    drop(tx);
    writer.await?
}

async fn request(app: &App, me: &Broker, role: &mut Role, buf: Bytes) -> Result<Reply> {
    let mut r = Rd { b: buf, at: 0 };
    let (key, ver, corr) = (r.i16()?, r.i16()?, r.i32()?);
    r.nstr()?; // client id
    if key == API_VERSIONS && ver >= 3 {
        r.tags()?; // (request header v2)
    }
    let login = matches!(key, API_VERSIONS | SASL_HANDSHAKE | SASL_AUTHENTICATE);
    ensure!(login || *role >= Role::Read, "not logged in (api {key})"); // (a read token can't produce: see `produce`)
    let supported = APIS.iter().any(|&(k, lo, hi)| k == key && (lo..=hi).contains(&ver));
    ensure!(supported || key == API_VERSIONS, "unsupported request: api {key} v{ver}");
    let done = |body: Vec<u8>| -> Reply { async move { Some(frame(corr, body)) }.boxed() };
    Ok(match key {
        API_VERSIONS => done(api_versions(if supported { ver } else { 0 }, supported)),
        METADATA => done(metadata(app, me, ver, &mut r).await?),
        SASL_HANDSHAKE => done(sasl_handshake(&mut r)?),
        SASL_AUTHENTICATE => done(sasl_authenticate(app, role, ver, &mut r)?),
        INIT_PRODUCER_ID => done(init_producer_id(&mut r)?),
        PRODUCE => produce(app, *role >= Role::Write, ver, &mut r).await?.map(move |body| body.map(|b| frame(corr, b))).boxed(),
        LIST_OFFSETS => done(list_offsets(app, ver, &mut r).await?),
        FIND_COORDINATOR => done(find_coordinator(app, me, ver, &mut r).await?),
        JOIN_GROUP => join_group(app, ver, &mut r).await?.map(move |b| Some(frame(corr, b))).boxed(),
        SYNC_GROUP => sync_group(app, ver, &mut r).await?.map(move |b| Some(frame(corr, b))).boxed(),
        HEARTBEAT => done(heartbeat(app, ver, &mut r)?),
        LEAVE_GROUP => done(leave_group(app, ver, &mut r)?),
        OFFSET_COMMIT => done(offset_commit(app, ver, &mut r).await?),
        OFFSET_FETCH => done(offset_fetch(app, ver, &mut r).await?),
        FETCH => {
            let app = app.clone();
            let req = FetchReq::read(ver, &mut r)?;
            async move { Some(frame(corr, fetch(&app, ver, req).await.unwrap_or_else(|e| fetch_error(ver, &e)))) }.boxed()
        }
        _ => bail!("api {key}"),
    })
}

fn frame(corr: i32, body: Vec<u8>) -> Vec<u8> {
    let mut f = Vec::with_capacity(body.len() + 8);
    f.put_i32(body.len() as i32 + 4);
    f.put_i32(corr);
    f.extend(body);
    f
}

// ---------------------------------------------------------------- handshake, metadata, logins

/// The versions we speak. An ApiVersions request newer than ours gets a v0 answer with
/// UNSUPPORTED_VERSION and our list; the client then asks again in a version we know.
fn api_versions(ver: i16, supported: bool) -> Vec<u8> {
    let mut w = vec![];
    w.put_i16(if supported { 0 } else { UNSUPPORTED_VERSION });
    put_len(&mut w, APIS.len(), ver >= 3);
    for &(k, lo, hi) in APIS {
        w.put_i16(k);
        w.put_i16(lo);
        w.put_i16(hi);
        if ver >= 3 {
            w.put_u8(0); // tagged fields
        }
    }
    if ver >= 1 {
        w.put_i32(0); // throttle
    }
    if ver >= 3 {
        w.put_u8(0);
    }
    w
}

async fn metadata(app: &App, me: &Broker, ver: i16, r: &mut Rd) -> Result<Vec<u8>> {
    let asked: Option<Vec<String>> = match r.i32()? {
        -1 => None, // all topics
        n => Some((0..n).map(|_| r.str()).collect::<Result<_>>()?),
    };
    let tables: Vec<String> = app.lake.cat.scan::<TableMeta>("t/", "t0").await?.into_iter().map(|(k, _)| k[2..].to_string()).filter(|t| !crate::sys::hidden(t)).collect();
    let topics: Vec<(String, bool)> = match asked {
        None => tables.into_iter().map(|t| (t, true)).collect(),
        Some(names) => names.into_iter().map(|n| (n.clone(), tables.contains(&n))).collect(),
    };
    let mut w = vec![];
    if ver >= 3 {
        w.put_i32(0);
    }
    w.put_i32(1); // brokers: this node
    w.put_i32(me.id);
    put_str(&mut w, &me.host);
    w.put_i32(me.port);
    w.put_i16(-1); // rack
    if ver >= 2 {
        put_str(&mut w, "pondra"); // cluster id
    }
    w.put_i32(me.id); // controller
    w.put_i32(topics.len() as i32);
    for (name, exists) in topics {
        w.put_i16(if exists { 0 } else { UNKNOWN_TOPIC });
        put_str(&mut w, &name);
        w.put_i8(0); // internal
        w.put_i32(exists as i32); // one partition, led by this node
        if exists {
            w.put_i16(0);
            w.put_i32(0);
            w.put_i32(me.id);
            if ver >= 7 {
                w.put_i32(0); // leader epoch
            }
            for _ in 0..2 {
                w.put_i32(1); // replicas, in-sync replicas: this node
                w.put_i32(me.id);
            }
            if ver >= 5 {
                w.put_i32(0); // offline replicas
            }
        }
        if ver >= 8 {
            w.put_i32(i32::MIN); // authorized operations: not computed
        }
    }
    if ver >= 8 {
        w.put_i32(i32::MIN);
    }
    Ok(w)
}

fn sasl_handshake(r: &mut Rd) -> Result<Vec<u8>> {
    let mechanism = r.str()?;
    let mut w = vec![];
    w.put_i16(if mechanism == "PLAIN" { 0 } else { 33 }); // UNSUPPORTED_SASL_MECHANISM
    w.put_i32(1);
    put_str(&mut w, "PLAIN");
    Ok(w)
}

/// SASL/PLAIN: "\0user\0password". The user name picks the role; the password is its token.
fn sasl_authenticate(app: &App, role: &mut Role, ver: i16, r: &mut Rd) -> Result<Vec<u8>> {
    let auth = r.nbytes()?.unwrap_or_default();
    let parts: Vec<&[u8]> = auth.split(|&b| b == 0).collect();
    let (user, password) = match parts[..] {
        [_, user, password] => (String::from_utf8_lossy(user).to_string(), password),
        _ => (String::new(), &b""[..]),
    };
    let ok = !app.auth.on() || app.auth.token_for(&user).is_some_and(|t| t.as_bytes() == password);
    if ok {
        *role = app.auth.role_of_user(&user);
    }
    let mut w = vec![];
    w.put_i16(if ok { 0 } else { SASL_FAILED });
    put_nstr(&mut w, (!ok).then_some("wrong user name or password"));
    w.put_i32(0); // auth bytes
    if ver >= 1 {
        w.put_i64(0); // session lifetime: unlimited
    }
    Ok(w)
}

/// Idempotent producers get a fresh id; transactions aren't supported.
fn init_producer_id(r: &mut Rd) -> Result<Vec<u8>> {
    let transactional = r.nstr()?.is_some();
    let mut w = vec![];
    w.put_i32(0);
    w.put_i16(if transactional { TRANSACTIONS_UNSUPPORTED } else { 0 });
    w.put_i64(if transactional { -1 } else { (uuid::Uuid::new_v4().as_u64_pair().0 >> 1) as i64 });
    w.put_i16(0);
    Ok(w)
}

// ---------------------------------------------------------------- producing

/// A partition's outcome: error code, base offset, message.
type Outcome = (i16, i64, Option<String>);

/// Decode every batch now and queue its rows in the log; the response waits for their acks.
async fn produce(app: &App, allowed: bool, ver: i16, r: &mut Rd) -> Result<BoxFuture<'static, Option<Vec<u8>>>> {
    r.nstr()?; // transactional id
    let acks = r.i16()?;
    r.i32()?; // timeout
    let mut topics: Vec<(String, Vec<(i32, BoxFuture<'static, Outcome>)>)> = vec![];
    for _ in 0..r.len()? {
        let name = r.str()?;
        let meta: Option<TableMeta> = app.lake.cat.get(&table_key(&name)).await?;
        let mut parts = vec![];
        for _ in 0..r.len()? {
            let (index, records) = (r.i32()?, r.nbytes()?.unwrap_or_default());
            let outcome = match (&meta, index) {
                _ if !allowed => ready((TOPIC_AUTHORIZATION_FAILED, -1, Some("this token may not write".into()))),
                (Some(meta), 0) => queue(app, &name, meta, &records).await,
                _ => ready((UNKNOWN_TOPIC, -1, None)),
            };
            parts.push((index, outcome));
        }
        topics.push((name, parts));
    }
    Ok(async move {
        let mut w = vec![];
        w.put_i32(topics.len() as i32);
        for (name, parts) in topics {
            put_str(&mut w, &name);
            w.put_i32(parts.len() as i32);
            for (index, outcome) in parts {
                let (error, offset, message) = outcome.await;
                w.put_i32(index);
                w.put_i16(error);
                w.put_i64(offset);
                w.put_i64(-1); // log append time: not used
                if ver >= 5 {
                    w.put_i64(0); // log start offset
                }
                if ver >= 8 {
                    w.put_i32(0); // record errors
                    put_nstr(&mut w, message.as_deref());
                }
            }
        }
        w.put_i32(0); // throttle
        (acks != 0).then_some(w)
    }
    .boxed())
}

fn ready(o: Outcome) -> BoxFuture<'static, Outcome> { async move { o }.boxed() }

/// Queue one partition's record batches in the log, in order. An idempotent producer's batch is
/// exactly-once: its (producer id, sequence) become a Pondra producer and seq, and `prev` makes
/// a batch that overtook its predecessor fail (OUT_OF_ORDER_SEQUENCE; the client resends it).
async fn queue(app: &App, table: &str, meta: &TableMeta, records: &[u8]) -> BoxFuture<'static, Outcome> {
    let invalid = |e: anyhow::Error| ready((INVALID_RECORD, -1, Some(format!("{e:#}"))));
    let log = match app.log() {
        Ok(log) => log,
        Err(e) => return ready((POLICY_VIOLATION, -1, Some(e.to_string()))),
    };
    let batches = match decode_batches(records) {
        Ok(b) => b,
        Err(e) => return invalid(e),
    };
    let mut acks = vec![];
    for b in batches {
        let rows = match to_rows(meta, &b.recs) {
            Ok(rows) => rows,
            Err(e) => return invalid(e),
        };
        let src = match b.producer_id >= 0 && b.base_seq >= 0 {
            true => {
                let (base, count) = (b.base_seq as u64, b.recs.len() as u64);
                Src { producer: format!("kafka:{}:{table}", b.producer_id), seq: base + count, prev: Some(base) }
            }
            false => Src { producer: String::new(), seq: 0, prev: None }, // at-least-once, as in Kafka
        };
        match log.queue(table.to_string(), src, rows).await {
            Ok(ack) => acks.push(ack),
            Err(e) => return ready((POLICY_VIOLATION, -1, Some(format!("{e:#}")))),
        }
    }
    async move {
        let mut first = None;
        for ack in acks {
            match ack.await {
                Ok(Ack { conflict: true, .. }) => return (OUT_OF_ORDER_SEQUENCE, -1, None),
                Ok(Ack { duplicate: true, .. }) => {} // a retry of a committed batch: success
                Ok(a) => first = first.or(Some(((a.seg << 32) + a.row) as i64)),
                Err(e) => return (POLICY_VIOLATION, -1, Some(format!("{e:#}"))),
            }
        }
        (0, first.unwrap_or(-1), None)
    }
    .boxed()
}

struct Rec {
    key: Option<Bytes>,
    value: Option<Bytes>,
    ts: i64,
}

struct Batch {
    producer_id: i64,
    base_seq: i32,
    recs: Vec<Rec>,
}

/// Record batches (format v2, "magic 2"), checked against their CRC, decompressed.
fn decode_batches(mut b: &[u8]) -> Result<Vec<Batch>> {
    let mut out = vec![];
    while b.len() >= 61 {
        let len = i32::from_be_bytes(b[8..12].try_into()?) as usize + 12;
        ensure!(len >= 61 && len <= b.len(), "truncated record batch");
        let (batch, rest) = b.split_at(len);
        b = rest;
        ensure!(batch[16] == 2, "record batch format v{} (only v2)", batch[16]);
        ensure!(u32::from_be_bytes(batch[17..21].try_into()?) == crc32c(&batch[21..]), "record batch CRC mismatch");
        let mut r = Rd { b: Bytes::copy_from_slice(&batch[21..]), at: 0 };
        let attributes = r.i16()?;
        r.i32()?; // last offset delta
        let base_ts = r.i64()?;
        r.i64()?; // max timestamp
        let (producer_id, _epoch, base_seq, count) = (r.i64()?, r.i16()?, r.i32()?, r.i32()?);
        let body = decompress(attributes & 7, &r.b[r.at..])?;
        let log_append = attributes & 8 != 0;
        let mut r = Rd { b: body.into(), at: 0 };
        let mut recs = Vec::with_capacity(count.max(0) as usize);
        for _ in 0..count {
            r.varint()?; // length
            r.i8()?; // attributes
            let ts = base_ts + r.varint()?;
            r.varint()?; // offset delta
            let (key, value) = (r.vbytes()?, r.vbytes()?);
            for _ in 0..r.varint()? {
                r.vbytes()?; // header key and value: not kept
                r.vbytes()?;
            }
            recs.push(Rec { key, value, ts: if log_append { now_ms() } else { ts } });
        }
        out.push(Batch { producer_id, base_seq, recs });
    }
    Ok(out)
}

fn decompress(codec: i16, data: &[u8]) -> Result<Vec<u8>> {
    let mut out = vec![];
    match codec {
        0 => out.extend_from_slice(data),
        1 => drop(flate2::read::GzDecoder::new(data).read_to_end(&mut out)?),
        2 => out = unsnappy(data)?,
        3 => drop(lz4_flex::frame::FrameDecoder::new(data).read_to_end(&mut out)?),
        4 => out = zstd::decode_all(data)?,
        c => bail!("compression codec {c}"),
    }
    Ok(out)
}

/// Snappy, raw or in the Java client's framing (a header, then length-prefixed blocks).
fn unsnappy(data: &[u8]) -> Result<Vec<u8>> {
    let mut snappy = snap::raw::Decoder::new();
    if !data.starts_with(b"\x82SNAPPY\x00") {
        return Ok(snappy.decompress_vec(data)?);
    }
    let (mut out, mut at) = (vec![], 16);
    while at + 4 <= data.len() {
        let n = u32::from_be_bytes(data[at..at + 4].try_into()?) as usize;
        ensure!(at + 4 + n <= data.len(), "truncated snappy block");
        out.extend(snappy.decompress_vec(&data[at + 4..at + 4 + n])?);
        at += 4 + n;
    }
    Ok(out)
}

/// Records as rows of the table: each value a JSON row (see the module comment).
fn to_rows(meta: &TableMeta, recs: &[Rec]) -> Result<RecordBatch> {
    let schema = schema(&meta.columns)?;
    let raw = schema.index_of("_value").is_ok();
    // Tombstones and change events are rewritten one by one; plain JSON rows go straight through.
    let has = |v: &[u8], word: &[u8]| v.windows(word.len()).any(|w| w == word);
    let special = |r: &Rec| r.value.as_deref().is_none_or(|v| v.is_empty() || ((has(v, b"\"op\"") || has(v, b"\"payload\"")) && envelope(Some(v))));
    let slow = !raw && recs.iter().any(special);
    // The JSON part: all values, one per line (the envelope path rewrites them first).
    let (mut lines, mut kept) = (Vec::new(), vec![]);
    for r in recs {
        match (raw, slow) {
            (true, _) => {}
            (false, false) => lines.extend_from_slice(r.value.as_deref().unwrap_or_default()),
            (false, true) => match row(meta, r.key.as_deref(), r.value.as_deref())? {
                Some(v) => serde_json::to_writer(&mut lines, &v)?,
                None => continue,
            },
        }
        lines.push(b'\n');
        kept.push(r);
    }
    let meta_cols = ["_key", "_timestamp", "_value"];
    let json_schema = Arc::new(Schema::new(schema.fields().iter().filter(|f| !meta_cols.contains(&f.name().as_str())).cloned().collect::<Vec<_>>()));
    let decoded = match raw {
        true => RecordBatch::try_new_with_options(json_schema.clone(), json_schema.fields().iter().map(|f| new_null_array(f.data_type(), kept.len())).collect(), &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(kept.len())))?,
        false => {
            let batches = arrow_json::ReaderBuilder::new(json_schema.clone()).build(&lines[..])?.collect::<Result<Vec<_>, _>>()?;
            concat_batches(&json_schema, &batches)?
        }
    };
    ensure!(decoded.num_rows() == kept.len(), "a value isn't a JSON object");
    let text = |f: fn(&Rec) -> Option<&Bytes>| -> ArrayRef { Arc::new(BinaryArray::from_iter(kept.iter().map(|r| f(*r).map(|b| b.as_ref())))) };
    let columns = schema.fields().iter().map(|f| -> Result<ArrayRef> {
        let c: ArrayRef = match f.name().as_str() {
            "_key" => text(|r| r.key.as_ref()),
            "_value" => text(|r| r.value.as_ref()),
            "_timestamp" => Arc::new(TimestampMillisecondArray::from_iter_values(kept.iter().map(|r| r.ts))),
            n => return Ok(decoded.column_by_name(n).expect("decoded").clone()),
        };
        Ok(match (c.data_type(), f.data_type()) {
            (DataType::Binary, DataType::Utf8) => Arc::new(StringArray::from_iter(c.as_binary::<i32>().iter().map(|b| b.map(|b| String::from_utf8_lossy(b).into_owned())))),
            _ => cast(&c, f.data_type())?,
        })
    });
    Ok(RecordBatch::try_new(schema.clone(), columns.collect::<Result<Vec<ArrayRef>>>()?)?)
}

/// A Debezium change event, or Kafka Connect's JSON with its schema alongside?
fn envelope(value: Option<&[u8]>) -> bool {
    let Some(Value::Object(v)) = value.and_then(|v| serde_json::from_slice(v).ok()) else { return false };
    (v.contains_key("schema") && v.contains_key("payload")) || (v.contains_key("op") && (v.contains_key("after") || v.contains_key("before")))
}

/// A record as the JSON row to write, None to skip it. Debezium's `after` is the new row, a
/// delete marks `before` deleted; a null value (a tombstone) deletes the key from a keyed table.
fn row(meta: &TableMeta, key: Option<&[u8]>, value: Option<&[u8]>) -> Result<Option<Value>> {
    let unwrap = |mut v: Value| if v.get("schema").is_some() && v.get("payload").is_some() { v["payload"].take() } else { v };
    let deleted = |mut v: Value| {
        if let Some(o) = v.as_object_mut() {
            o.insert("_deleted".into(), true.into());
        }
        v
    };
    let mut v = unwrap(match value.filter(|v| !v.is_empty()) {
        Some(v) => serde_json::from_slice(v)?,
        None => Value::Null,
    });
    if let Some(op) = v.get("op").and_then(Value::as_str).map(String::from).filter(|_| v.get("after").is_some() || v.get("before").is_some()) {
        v = match op.as_str() {
            "c" | "u" | "r" => v["after"].take(),
            "d" => deleted(v["before"].take()),
            _ => return Ok(None), // (a truncate)
        };
    }
    if !v.is_null() {
        return Ok(Some(v));
    }
    let keyed = !meta.key.is_empty() && meta.columns.iter().any(|(c, _)| c == "_deleted");
    let Some(key) = key.filter(|_| keyed) else { return Ok(None) }; // (nothing to delete)
    let k = unwrap(serde_json::from_slice(key).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(key).into())));
    Ok(Some(deleted(match k {
        Value::Object(_) => k,
        k if meta.key.len() == 1 => Value::Object([(meta.key[0].clone(), k)].into_iter().collect()),
        _ => bail!("a tombstone's key must name every key column"),
    })))
}

// ---------------------------------------------------------------- consuming

/// Offsets: the earliest one still in the log, the next one to be written (-1), or the first at
/// or after a time.
async fn list_offsets(app: &App, ver: i16, r: &mut Rd) -> Result<Vec<u8>> {
    r.i32()?; // replica
    if ver >= 2 {
        r.i8()?; // isolation level
    }
    let mut w = vec![];
    if ver >= 2 {
        w.put_i32(0);
    }
    let n = r.len()?;
    w.put_i32(n as i32);
    for _ in 0..n {
        let name = r.str()?;
        let exists = app.lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_some();
        put_str(&mut w, &name);
        let parts = r.len()?;
        w.put_i32(parts as i32);
        for _ in 0..parts {
            let index = r.i32()?;
            if ver >= 4 {
                r.i32()?; // leader epoch
            }
            let at = r.i64()?;
            let offset = match at {
                -1 => (app.lake.visible() + 1) << 32,
                -2 => first_segment(app).await? << 32,
                t => after_time(app, t).await? << 32,
            };
            w.put_i32(index);
            w.put_i16(if exists && index == 0 { 0 } else { UNKNOWN_TOPIC });
            w.put_i64(-1); // timestamp
            w.put_i64(offset as i64);
            if ver >= 4 {
                w.put_i32(0);
            }
        }
    }
    Ok(w)
}

/// The oldest segment still in the log (segments expire oldest first).
async fn first_segment(app: &App) -> Result<u64> {
    let (mut lo, mut hi) = (0, app.lake.visible() + 1); // the answer is in [lo, hi]
    while lo < hi {
        let mid = (lo + hi) / 2;
        match app.lake.cat.get_raw(&seg_key(mid)).await?.is_some() {
            true => hi = mid,
            false => lo = mid + 1,
        }
    }
    Ok(lo)
}

/// The first segment committed at or after `t_ms`.
async fn after_time(app: &App, t_ms: i64) -> Result<u64> {
    let (mut lo, mut hi) = (first_segment(app).await?, app.lake.visible() + 1);
    while lo < hi {
        let mid = (lo + hi) / 2;
        match app.lake.cat.get::<Segment>(&seg_key(mid)).await? {
            Some(s) if (s.ts_ms as i64) < t_ms => lo = mid + 1,
            _ => hi = mid,
        }
    }
    Ok(lo)
}

struct FetchReq {
    max_wait: Duration,
    max_bytes: usize,
    topics: Vec<(String, Vec<(i32, u64, usize)>)>, // topic -> (partition, offset, max bytes)
}

impl FetchReq {
    fn read(ver: i16, r: &mut Rd) -> Result<FetchReq> {
        r.i32()?; // replica
        let max_wait = Duration::from_millis(r.i32()?.clamp(0, 30_000) as u64);
        r.i32()?; // min bytes: any data answers at once
        let max_bytes = r.i32()?.max(1) as usize;
        r.i8()?; // isolation level
        if ver >= 7 {
            r.i32()?; // fetch session: none (every fetch is a full one)
            r.i32()?;
        }
        let mut topics = vec![];
        for _ in 0..r.len()? {
            let name = r.str()?;
            let mut parts = vec![];
            for _ in 0..r.len()? {
                let index = r.i32()?;
                if ver >= 9 {
                    r.i32()?; // leader epoch
                }
                let offset = r.i64()?.max(0) as u64;
                if ver >= 5 {
                    r.i64()?; // log start offset
                }
                parts.push((index, offset, r.i32()?.max(1) as usize));
            }
            topics.push((name, parts));
        }
        Ok(FetchReq { max_wait, max_bytes, topics })
    }
}

/// Records from each asked offset on, one batch per log segment. With nothing new yet, it waits
/// up to the client's max wait for the next commit.
async fn fetch(app: &App, ver: i16, req: FetchReq) -> Result<Vec<u8>> {
    let mut hwm = app.lake.hwm.subscribe();
    let deadline = tokio::time::Instant::now() + req.max_wait;
    let answers = loop {
        hwm.borrow_and_update();
        let (visible, first) = (app.lake.visible(), first_segment(app).await?);
        let (mut answers, mut budget, mut any) = (vec![], req.max_bytes, false);
        for (name, parts) in &req.topics {
            let meta: Option<TableMeta> = app.lake.cat.get(&table_key(name)).await?;
            let mut out = vec![];
            for &(index, offset, limit) in parts {
                // An offset before the oldest segment still kept reads from that one: its rows went
                // with retention, as Kafka's would. ("Out of range" made librdkafka retry an
                // earliest offset it had cached from before those segments expired, in a loop.)
                let (offset, end) = (offset.max(first << 32), (visible + 1) << 32);
                let (error, records) = match &meta {
                    Some(meta) if index == 0 && offset <= end => (0, read(app, name, meta, offset, visible, limit.min(budget)).await?),
                    Some(_) if index == 0 => (OFFSET_OUT_OF_RANGE, vec![]),
                    _ => (UNKNOWN_TOPIC, vec![]),
                };
                budget = budget.saturating_sub(records.len());
                any |= !records.is_empty();
                out.push((index, error, end, records));
            }
            answers.push((name.clone(), out));
        }
        if any || tokio::time::timeout_at(deadline, hwm.changed()).await.is_err() {
            break answers;
        }
    };
    let mut w = vec![];
    w.put_i32(0); // throttle
    if ver >= 7 {
        w.put_i16(0);
        w.put_i32(0); // session: none
    }
    w.put_i32(answers.len() as i32);
    for (name, parts) in answers {
        put_str(&mut w, &name);
        w.put_i32(parts.len() as i32);
        for (index, error, end, records) in parts {
            w.put_i32(index);
            w.put_i16(error);
            w.put_i64(end as i64); // high watermark
            w.put_i64(end as i64); // last stable offset
            if ver >= 5 {
                w.put_i64(0); // log start offset
            }
            w.put_i32(-1); // aborted transactions: none
            if ver >= 11 {
                w.put_i32(-1); // preferred read replica
            }
            w.put_i32(records.len() as i32);
            w.extend(records);
        }
    }
    Ok(w)
}

fn fetch_error(ver: i16, e: &anyhow::Error) -> Vec<u8> {
    eprintln!("kafka fetch: {e:#}");
    let mut w = vec![];
    w.put_i32(0);
    if ver >= 7 {
        w.put_i16(-1); // UNKNOWN_SERVER_ERROR
        w.put_i32(0);
    }
    w.put_i32(0);
    w
}

/// Record batches of `table` from `offset` on, up to about `limit` bytes (at least one batch).
async fn read(app: &App, table: &str, meta: &TableMeta, offset: u64, visible: u64, limit: usize) -> Result<Vec<u8>> {
    let (from, skip) = (offset >> 32, offset & 0xffff_ffff);
    let (mut out, mut at) = (vec![], from);
    while out.is_empty() && at <= visible {
        let upto = visible.min(at + 4095); // (segments without this table are skipped, 4,096 at a time)
        for (key, seg) in app.lake.cat.scan::<Segment>(&seg_key(at), &seg_key(upto + 1)).await? {
            if !seg.parts.contains_key(table) || out.len() >= limit {
                continue;
            }
            let n: u64 = key[2..].parse()?;
            let rows = app.lake.segment_rows(n, &seg, table).await?;
            let s = schema(&meta.columns)?;
            let all = concat_batches(&s, &rows.iter().map(|b| crate::query::conform(b, &s)).collect::<Result<Vec<_>>>()?)?;
            if all.num_rows() == 0 {
                continue;
            }
            let start = if n == from { skip.min(all.num_rows() as u64) as usize } else { 0 };
            let rows = all.slice(start, all.num_rows() - start);
            if rows.num_rows() > 0 {
                out.extend(encode_batch(((n << 32) + start as u64) as i64, seg.ts_ms as i64, &records(meta, &rows)?));
            }
        }
        at = upto + 1;
    }
    Ok(out)
}

/// Rows as (key, value): the value a JSON row (null for a delete), the key the key columns.
fn records(meta: &TableMeta, rows: &RecordBatch) -> Result<Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>> {
    let lines = |b: &RecordBatch| -> Result<Vec<Vec<u8>>> {
        let mut w = arrow_json::LineDelimitedWriter::new(Vec::new());
        w.write(b)?;
        w.finish()?;
        Ok(w.into_inner().split(|&c| c == b'\n').take(b.num_rows()).map(<[u8]>::to_vec).collect())
    };
    let pick = |cols: &[&str]| -> Result<RecordBatch> { Ok(rows.project(&cols.iter().map(|c| rows.schema().index_of(c)).collect::<Result<Vec<_>, _>>()?)?) };
    let shown: Vec<&str> = meta.columns.iter().map(|(c, _)| c.as_str()).filter(|c| *c != "_deleted").collect();
    let values = lines(&pick(&shown)?)?;
    let keys = match meta.key.is_empty() {
        true => vec![None; rows.num_rows()],
        false => lines(&pick(&meta.key.iter().map(String::as_str).collect::<Vec<_>>())?)?.into_iter().map(Some).collect(),
    };
    let deleted = rows.column_by_name("_deleted").map(|c| c.as_boolean().clone()).unwrap_or_else(|| BooleanArray::from(vec![false; rows.num_rows()]));
    Ok(keys.into_iter().zip(values).enumerate().map(|(i, (k, v))| (k, (!deleted.value(i) || deleted.is_null(i)).then_some(v))).collect())
}

/// A record batch (v2, uncompressed) of consecutive offsets from `base`.
fn encode_batch(base: i64, ts: i64, recs: &[(Option<Vec<u8>>, Option<Vec<u8>>)]) -> Vec<u8> {
    let mut body = vec![];
    for (i, (key, value)) in recs.iter().enumerate() {
        let mut rec = vec![0u8]; // attributes
        put_varint(&mut rec, 0); // timestamp delta
        put_varint(&mut rec, i as i64); // offset delta
        for field in [key, value] {
            put_varint(&mut rec, field.as_ref().map_or(-1, |f| f.len() as i64));
            rec.extend(field.iter().flatten());
        }
        put_varint(&mut rec, 0); // headers
        put_varint(&mut body, rec.len() as i64);
        body.extend(rec);
    }
    let mut tail = vec![]; // from attributes on: what the CRC covers
    tail.put_i16(0);
    tail.put_i32(recs.len() as i32 - 1);
    tail.put_i64(ts);
    tail.put_i64(ts);
    tail.put_i64(-1); // producer id
    tail.put_i16(-1);
    tail.put_i32(-1);
    tail.put_i32(recs.len() as i32);
    tail.extend(body);
    let mut b = vec![];
    b.put_i64(base);
    b.put_i32(tail.len() as i32 + 9); // partition leader epoch, magic, crc
    b.put_i32(0);
    b.put_i8(2);
    b.put_u32(crc32c(&tail));
    b.extend(tail);
    b
}

// ---------------------------------------------------------------- encoding

struct Rd {
    b: Bytes,
    at: usize,
}

impl Rd {
    fn take(&mut self, n: usize) -> Result<&[u8]> {
        ensure!(self.at + n <= self.b.len(), "request ends early");
        self.at += n;
        Ok(&self.b[self.at - n..self.at])
    }
    fn i8(&mut self) -> Result<i8> { Ok(self.take(1)?[0] as i8) }
    fn i16(&mut self) -> Result<i16> { Ok(i16::from_be_bytes(self.take(2)?.try_into()?)) }
    fn i32(&mut self) -> Result<i32> { Ok(i32::from_be_bytes(self.take(4)?.try_into()?)) }
    fn i64(&mut self) -> Result<i64> { Ok(i64::from_be_bytes(self.take(8)?.try_into()?)) }
    /// An array's length (a null array reads as empty).
    fn len(&mut self) -> Result<usize> { Ok(self.i32()?.max(0) as usize) }
    fn nstr(&mut self) -> Result<Option<String>> {
        let n = self.i16()?;
        Ok(if n < 0 { None } else { Some(String::from_utf8(self.take(n as usize)?.to_vec())?) })
    }
    fn str(&mut self) -> Result<String> { Ok(self.nstr()?.unwrap_or_default()) }
    fn nbytes(&mut self) -> Result<Option<Bytes>> {
        let n = self.i32()?;
        Ok(if n < 0 { None } else { Some(self.slice(n as usize)?) })
    }
    fn slice(&mut self, n: usize) -> Result<Bytes> {
        self.take(n)?;
        Ok(self.b.slice(self.at - n..self.at))
    }
    fn uvarint(&mut self) -> Result<u64> {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let b = self.take(1)?[0];
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                return Ok(v);
            }
            shift += 7;
            ensure!(shift < 64, "bad varint");
        }
    }
    fn varint(&mut self) -> Result<i64> {
        let v = self.uvarint()?;
        Ok((v >> 1) as i64 ^ -((v & 1) as i64))
    }
    /// Varint-length bytes inside a record (-1 = null).
    fn vbytes(&mut self) -> Result<Option<Bytes>> {
        let n = self.varint()?;
        Ok(if n < 0 { None } else { Some(self.slice(n as usize)?) })
    }
    /// Tagged fields (flexible versions): skipped.
    fn tags(&mut self) -> Result<()> {
        for _ in 0..self.uvarint()? {
            self.uvarint()?;
            let n = self.uvarint()? as usize;
            self.take(n)?;
        }
        Ok(())
    }
}

fn put_str(w: &mut Vec<u8>, s: &str) {
    w.put_i16(s.len() as i16);
    w.put_slice(s.as_bytes());
}

fn put_nstr(w: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(s) => put_str(w, s),
        None => w.put_i16(-1),
    }
}

/// An array length: i32, or in flexible versions a varint of length + 1.
fn put_len(w: &mut Vec<u8>, n: usize, compact: bool) {
    match compact {
        true => put_uvarint(w, n as u64 + 1),
        false => w.put_i32(n as i32),
    }
}

fn put_uvarint(w: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        w.put_u8(v as u8 | 0x80);
        v >>= 7;
    }
    w.put_u8(v as u8);
}

fn put_varint(w: &mut Vec<u8>, v: i64) { put_uvarint(w, ((v << 1) ^ (v >> 63)) as u64) }

fn crc32c(data: &[u8]) -> u32 { crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, data) as u32 }

fn now_ms() -> i64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64) }

// ---------------------------------------------------------------- consumer groups
//
// The leader coordinates every group, in memory: members join, the group's leader member assigns
// the partitions, members heartbeat; a member that joins, leaves or goes silent starts a new
// generation. Committed offsets are kept as a producer's progress in the catalog
// (`kafka-group:{group}:{topic}` = offset + 1), written through the log like any append, so they
// survive a change of leader (members then just join again).

static GROUPS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, Group>>> = std::sync::LazyLock::new(Default::default);
static ME: std::sync::OnceLock<Value> = std::sync::OnceLock::new(); // this node's broker, for `/cluster/kafka`
static COORDINATOR: tokio::sync::OnceCell<(i32, String, i32)> = tokio::sync::OnceCell::const_new();

const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const ILLEGAL_GENERATION: i16 = 22;
const UNKNOWN_MEMBER_ID: i16 = 25;
const REBALANCE_IN_PROGRESS: i16 = 27;

/// This node as a Kafka broker (the leader's is where followers send group requests).
pub fn me() -> Value { ME.get().cloned().unwrap_or(Value::Null) }

#[derive(Default)]
struct Group {
    generation: i32,
    protocol: String,
    leader: String,
    members: std::collections::BTreeMap<String, Member>,
    round: u64,       // counts rebalances (a rebalance's timer ends with it)
    joining: bool,    // a rebalance is waiting for members to join
    synced: bool,     // this generation's assignments are in
    joins: Vec<(String, tokio::sync::oneshot::Sender<Joined>)>,
    syncs: Vec<(String, tokio::sync::oneshot::Sender<Bytes>)>,
}

struct Member {
    protocols: Vec<(String, Bytes)>,
    session: Duration,
    seen: std::time::Instant,
    joined: bool,
    assignment: Bytes,
}

#[derive(Clone, Default)]
struct Joined {
    error: i16,
    generation: i32,
    protocol: String,
    leader: String,
    member: String,
    members: Vec<(String, Bytes)>, // for the group's leader member only
}

impl Group {
    /// Start a rebalance: everyone must join again (they learn it from their heartbeats).
    fn rebalance(&mut self, group: &str, timeout: Duration) {
        if self.joining {
            return;
        }
        (self.joining, self.synced, self.round) = (true, false, self.round + 1);
        self.members.values_mut().for_each(|m| m.joined = false);
        let (group, round, start) = (group.to_string(), self.round, std::time::Instant::now());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let mut groups = GROUPS.lock().unwrap();
                let Some(g) = groups.get_mut(&group).filter(|g| g.round == round && g.joining) else { return };
                g.members.retain(|_, m| m.joined || m.seen.elapsed() < m.session); // (silent ones drop out)
                g.complete(start.elapsed() > timeout);
            }
        });
    }

    /// End the rebalance once every member has joined (or `force`: without those who haven't).
    fn complete(&mut self, force: bool) {
        if !self.joining || !(force || self.members.values().all(|m| m.joined)) {
            return;
        }
        self.members.retain(|_, m| m.joined);
        self.joining = false;
        let Some(first) = self.members.keys().next().cloned() else { return };
        if !self.members.contains_key(&self.leader) {
            self.leader = first;
        }
        self.generation += 1;
        let common = |p: &String| self.members.values().all(|m| m.protocols.iter().any(|(q, _)| q == p));
        self.protocol = self.members[&self.leader].protocols.iter().map(|(p, _)| p.clone()).find(common).unwrap_or_default();
        let all: Vec<(String, Bytes)> = self.members.iter().map(|(id, m)| (id.clone(), m.protocols.iter().find(|(p, _)| *p == self.protocol).map(|(_, b)| b.clone()).unwrap_or_default())).collect();
        for (member, tx) in self.joins.drain(..) {
            let members = if member == self.leader { all.clone() } else { vec![] };
            let _ = tx.send(Joined { error: 0, generation: self.generation, protocol: self.protocol.clone(), leader: self.leader.clone(), member, members });
        }
    }

    /// Drop members silent past their session timeout; someone leaving means a new generation.
    fn expire(&mut self, group: &str) {
        let before = self.members.len();
        self.members.retain(|_, m| m.seen.elapsed() < m.session);
        if self.members.len() < before && !self.members.is_empty() {
            self.rebalance(group, Duration::from_secs(30));
        }
    }

    /// Is `member` in this generation? (A Kafka error code if not.)
    fn check(&self, member: &str, generation: i32) -> i16 {
        match self.members.get(member) {
            None => UNKNOWN_MEMBER_ID,
            Some(_) if generation != self.generation => ILLEGAL_GENERATION,
            Some(_) if self.joining => REBALANCE_IN_PROGRESS,
            Some(_) => 0,
        }
    }
}

/// The coordinator: this node if it leads, else the leader's Kafka address (asked once).
async fn find_coordinator(app: &App, me: &Broker, ver: i16, r: &mut Rd) -> Result<Vec<u8>> {
    r.str()?; // group
    let transactions = ver >= 1 && r.i8()? == 1;
    let coordinator = match app.seq.is_some() {
        true => Some((me.id, me.host.clone(), me.port)),
        false => COORDINATOR.get_or_try_init(|| leader_broker(app)).await.ok().cloned(),
    };
    let mut w = vec![];
    if ver >= 1 {
        w.put_i32(0);
    }
    let (id, host, port) = coordinator.clone().filter(|_| !transactions).unwrap_or((-1, String::new(), -1));
    w.put_i16(if id < 0 { COORDINATOR_NOT_AVAILABLE } else { 0 });
    if ver >= 1 {
        put_nstr(&mut w, None);
    }
    w.put_i32(id);
    put_str(&mut w, &host);
    w.put_i32(port);
    Ok(w)
}

async fn leader_broker(app: &App) -> Result<(i32, String, i32)> {
    let b: Value = crate::cluster::http().get(format!("http://{}/cluster/kafka", app.cluster.leader.addr)).send().await?.error_for_status()?.json().await?;
    Ok((b["id"].as_i64().ok_or_else(|| anyhow!("the leader doesn't speak Kafka"))? as i32, b["host"].as_str().unwrap_or_default().into(), b["port"].as_i64().unwrap_or(-1) as i32))
}

async fn join_group(app: &App, ver: i16, r: &mut Rd) -> Result<BoxFuture<'static, Vec<u8>>> {
    let (group, session) = (r.str()?, Duration::from_millis(r.i32()?.max(1000) as u64));
    let rebalance = if ver >= 1 { Duration::from_millis(r.i32()?.max(1000) as u64) } else { session };
    let member = r.str()?;
    if ver >= 5 {
        r.nstr()?; // group instance id (static membership: treated as dynamic)
    }
    r.str()?; // protocol type
    let protocols = (0..r.len()?).map(|_| Ok((r.str()?, r.nbytes()?.unwrap_or_default()))).collect::<Result<Vec<_>>>()?;
    let answer = move |j: Joined| {
        let mut w = vec![];
        if ver >= 2 {
            w.put_i32(0);
        }
        w.put_i16(j.error);
        w.put_i32(j.generation);
        put_str(&mut w, &j.protocol);
        put_str(&mut w, &j.leader);
        put_str(&mut w, &j.member);
        w.put_i32(j.members.len() as i32);
        for (id, meta) in &j.members {
            put_str(&mut w, id);
            if ver >= 5 {
                put_nstr(&mut w, None);
            }
            w.put_i32(meta.len() as i32);
            w.extend(meta.iter());
        }
        w
    };
    if app.seq.is_none() {
        return Ok(async move { answer(Joined { error: NOT_COORDINATOR, ..Default::default() }) }.boxed());
    }
    let rx = {
        let mut groups = GROUPS.lock().unwrap();
        let g = groups.entry(group.clone()).or_default();
        g.expire(&group);
        if !member.is_empty() && !g.members.contains_key(&member) {
            return Ok(async move { answer(Joined { error: UNKNOWN_MEMBER_ID, member, ..Default::default() }) }.boxed());
        }
        let id = if member.is_empty() { format!("pondra-{}", uuid::Uuid::new_v4()) } else { member };
        let m = g.members.entry(id.clone()).or_insert(Member { protocols: vec![], session, seen: std::time::Instant::now(), joined: false, assignment: Bytes::new() });
        (m.protocols, m.session, m.seen) = (protocols, session, std::time::Instant::now());
        g.rebalance(&group, rebalance);
        g.members.get_mut(&id).expect("just added").joined = true;
        let (tx, rx) = tokio::sync::oneshot::channel();
        g.joins.push((id, tx));
        g.complete(false);
        rx
    };
    Ok(async move { answer(rx.await.unwrap_or(Joined { error: REBALANCE_IN_PROGRESS, ..Default::default() })) }.boxed())
}

async fn sync_group(app: &App, ver: i16, r: &mut Rd) -> Result<BoxFuture<'static, Vec<u8>>> {
    let (group, generation, member) = (r.str()?, r.i32()?, r.str()?);
    if ver >= 3 {
        r.nstr()?;
    }
    let assignments = (0..r.len()?).map(|_| Ok((r.str()?, r.nbytes()?.unwrap_or_default()))).collect::<Result<Vec<_>>>()?;
    let answer = move |error: i16, assignment: Bytes| {
        let mut w = vec![];
        if ver >= 1 {
            w.put_i32(0);
        }
        w.put_i16(error);
        w.put_i32(assignment.len() as i32);
        w.extend(assignment.iter());
        w
    };
    if app.seq.is_none() {
        return Ok(async move { answer(NOT_COORDINATOR, Bytes::new()) }.boxed());
    }
    let mut groups = GROUPS.lock().unwrap();
    let g = groups.entry(group).or_default();
    let error = g.check(&member, generation);
    if error != 0 {
        return Ok(async move { answer(error, Bytes::new()) }.boxed());
    }
    g.members.get_mut(&member).expect("checked").seen = std::time::Instant::now();
    if member == g.leader {
        for (id, a) in assignments {
            if let Some(m) = g.members.get_mut(&id) {
                m.assignment = a;
            }
        }
        g.synced = true;
        for (id, tx) in g.syncs.drain(..) {
            let _ = tx.send(g.members.get(&id).map(|m| m.assignment.clone()).unwrap_or_default());
        }
    }
    if g.synced {
        let a = g.members[&member].assignment.clone();
        return Ok(async move { answer(0, a) }.boxed());
    }
    let (tx, rx) = tokio::sync::oneshot::channel();
    g.syncs.push((member, tx));
    Ok(async move {
        match rx.await {
            Ok(a) => answer(0, a),
            Err(_) => answer(REBALANCE_IN_PROGRESS, Bytes::new()),
        }
    }
    .boxed())
}

fn heartbeat(app: &App, ver: i16, r: &mut Rd) -> Result<Vec<u8>> {
    let (group, generation, member) = (r.str()?, r.i32()?, r.str()?);
    let error = match app.seq.is_some() {
        false => NOT_COORDINATOR,
        true => {
            let mut groups = GROUPS.lock().unwrap();
            let g = groups.entry(group.clone()).or_default();
            g.expire(&group);
            if let Some(m) = g.members.get_mut(&member) {
                m.seen = std::time::Instant::now();
            }
            g.check(&member, generation)
        }
    };
    let mut w = vec![];
    if ver >= 1 {
        w.put_i32(0);
    }
    w.put_i16(error);
    Ok(w)
}

fn leave_group(app: &App, ver: i16, r: &mut Rd) -> Result<Vec<u8>> {
    let group = r.str()?;
    let leaving: Vec<String> = match ver >= 3 {
        true => (0..r.len()?).map(|_| {
            let id = r.str()?;
            r.nstr()?;
            Ok(id)
        }).collect::<Result<_>>()?,
        false => vec![r.str()?],
    };
    if app.seq.is_some() {
        let mut groups = GROUPS.lock().unwrap();
        let g = groups.entry(group.clone()).or_default();
        let before = g.members.len();
        g.members.retain(|id, _| !leaving.contains(id));
        if g.members.len() < before && !g.members.is_empty() {
            g.rebalance(&group, Duration::from_secs(30));
        }
    }
    let mut w = vec![];
    if ver >= 1 {
        w.put_i32(0);
    }
    w.put_i16(if app.seq.is_some() { 0 } else { NOT_COORDINATOR });
    if ver >= 3 {
        w.put_i32(leaving.len() as i32);
        for id in &leaving {
            put_str(&mut w, id);
            put_nstr(&mut w, None);
            w.put_i16(0);
        }
    }
    Ok(w)
}

fn offsets_key(group: &str, topic: &str) -> String { format!("kafka-group:{group}:{topic}") }

/// Committed offsets become a producer's progress: an empty append that only moves its seq
/// (so a commit never moves an offset back).
async fn offset_commit(app: &App, ver: i16, r: &mut Rd) -> Result<Vec<u8>> {
    let (group, generation, member) = (r.str()?, r.i32()?, r.str()?);
    if ver >= 7 {
        r.nstr()?;
    }
    if (2..=4).contains(&ver) {
        r.i64()?; // retention
    }
    let error = match (app.seq.is_some(), generation) {
        (false, _) => NOT_COORDINATOR,
        (true, g) if g < 0 => 0, // a consumer outside any generation (it assigns partitions itself)
        (true, g) => GROUPS.lock().unwrap().get(&group).map_or(UNKNOWN_MEMBER_ID, |grp| grp.check(&member, g)),
    };
    let mut w = vec![];
    if ver >= 3 {
        w.put_i32(0);
    }
    let topics = r.len()?;
    w.put_i32(topics as i32);
    for _ in 0..topics {
        let name = r.str()?;
        let meta: Option<TableMeta> = app.lake.cat.get(&table_key(&name)).await?;
        put_str(&mut w, &name);
        let parts = r.len()?;
        w.put_i32(parts as i32);
        for _ in 0..parts {
            let (index, offset) = (r.i32()?, r.i64()?);
            if ver >= 6 {
                r.i32()?;
            }
            r.nstr()?; // metadata
            let code = match (&meta, error) {
                (_, e) if e != 0 => e,
                (Some(meta), _) if index == 0 && offset >= 0 => {
                    let src = Src { producer: offsets_key(&group, &name), seq: offset as u64 + 1, prev: None };
                    match app.log()?.append(name.clone(), src, RecordBatch::new_empty(schema(&meta.columns)?)).await {
                        Ok(_) => 0,
                        Err(e) => {
                            eprintln!("kafka offset commit: {e:#}");
                            -1
                        }
                    }
                }
                _ => UNKNOWN_TOPIC,
            };
            w.put_i32(index);
            w.put_i16(code);
        }
    }
    Ok(w)
}

async fn offset_fetch(app: &App, ver: i16, r: &mut Rd) -> Result<Vec<u8>> {
    let group = r.str()?;
    let asked: Option<Vec<(String, Vec<i32>)>> = match r.i32()? {
        -1 => None,
        n => Some((0..n).map(|_| Ok((r.str()?, (0..r.len()?).map(|_| r.i32()).collect::<Result<Vec<_>>>()?))).collect::<Result<_>>()?),
    };
    let prefix = format!("p/{}", offsets_key(&group, ""));
    let topics = match asked {
        Some(t) => t,
        None => app.lake.cat.scan::<u64>(&prefix, &format!("{prefix}\u{10ffff}")).await?.into_iter().map(|(k, _)| (k[prefix.len()..].to_string(), vec![0])).collect(),
    };
    let coordinator = app.seq.is_some();
    let mut w = vec![];
    if ver >= 3 {
        w.put_i32(0);
    }
    w.put_i32(topics.len() as i32);
    for (name, parts) in topics {
        let committed: Option<u64> = app.lake.cat.get(&crate::store::producer_key(&offsets_key(&group, &name))).await?;
        put_str(&mut w, &name);
        w.put_i32(parts.len() as i32);
        for index in parts {
            w.put_i32(index);
            w.put_i64(committed.filter(|_| index == 0).map_or(-1, |s| s as i64 - 1));
            if ver >= 5 {
                w.put_i32(-1);
            }
            put_nstr(&mut w, Some(""));
            w.put_i16(if coordinator { 0 } else { NOT_COORDINATOR });
        }
    }
    if ver >= 2 {
        w.put_i16(if coordinator { 0 } else { NOT_COORDINATOR });
    }
    Ok(w)
}
