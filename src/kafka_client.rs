//! A Kafka client (ADR-026): other Kafka clusters' topics read as tables (`kafka://brokers/topic`,
//! or a cluster attached `(TYPE kafka)`), fed into views, and written to by `COPY … TO`. The same
//! wire protocol Pondra's own Kafka port speaks (`kafka.rs`), the other way round, in the
//! versions every broker since Kafka 2.1 answers and Kafka 4 still does; plain TCP or TLS, SASL
//! PLAIN or SCRAM, from the secret covering the cluster's URL.
use crate::kafka::{crc32c, decompress, put_nstr, put_str, put_varint, Rd};
use anyhow::{bail, ensure, Context, Result};
use bytes::{BufMut, Bytes};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

const PRODUCE: i16 = 0;
const FETCH: i16 = 1;
const LIST_OFFSETS: i16 = 2;
const METADATA: i16 = 3;
const SASL_HANDSHAKE: i16 = 17;
const INIT_PRODUCER_ID: i16 = 22;
const SASL_AUTHENTICATE: i16 = 36;

/// How to reach a cluster: its bootstrap brokers, and a secret's settings (`CREATE SECRET … (TYPE
/// kafka, …)`): SECURITY_PROTOCOL (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL), SASL_MECHANISM
/// (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), USERNAME, PASSWORD.
#[derive(Clone, Default)]
pub struct Settings {
    pub brokers: Vec<String>,
    pub tls: bool,
    pub sasl: Option<(String, String, String)>, // mechanism, user, password
}

impl Settings {
    pub fn of(brokers: &str, secret: Option<&BTreeMap<String, String>>) -> Result<Settings> {
        let get = |k: &str| secret.and_then(|s| s.get(k)).cloned();
        let protocol = get("security_protocol").unwrap_or_else(|| if get("username").is_some() { "SASL_SSL".into() } else { "PLAINTEXT".into() }).to_uppercase();
        ensure!(["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"].contains(&protocol.as_str()), "SECURITY_PROTOCOL {protocol}: PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL");
        let sasl = match protocol.starts_with("SASL") {
            true => {
                let mech = get("sasl_mechanism").unwrap_or_else(|| "PLAIN".into()).to_uppercase();
                ensure!(["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"].contains(&mech.as_str()), "SASL_MECHANISM {mech}: PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512");
                Some((mech, get("username").context("SASL needs USERNAME")?, get("password").context("SASL needs PASSWORD")?))
            }
            false => None,
        };
        let brokers: Vec<String> = brokers.split(',').map(|b| b.trim().to_string()).filter(|b| !b.is_empty()).collect();
        ensure!(!brokers.is_empty(), "which brokers? kafka://host:9092,host2:9092/topic");
        Ok(Settings { brokers, tls: protocol.ends_with("SSL"), sasl })
    }
}

trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}

/// One broker connection: a request at a time. A call dropped halfway (a query cancelled, a
/// LIMIT reached) leaves an answer unread: the next call opens the connection again.
struct Conn {
    io: Mutex<(Box<dyn Stream>, i32, bool)>, // the stream, the last correlation id, whether a call is halfway
    addr: String,
    settings: Settings,
}

impl Conn {
    async fn open(addr: &str, s: &Settings) -> Result<Conn> {
        Ok(Conn { io: Mutex::new((Self::stream(addr, s).await?, 0, false)), addr: addr.to_string(), settings: s.clone() })
    }

    async fn stream(addr: &str, s: &Settings) -> Result<Box<dyn Stream>> {
        let tcp = tokio::time::timeout(std::time::Duration::from_secs(10), tokio::net::TcpStream::connect(addr)).await.with_context(|| format!("reaching {addr}"))??;
        tcp.set_nodelay(true)?;
        let io: Box<dyn Stream> = match s.tls {
            true => {
                use rustls_platform_verifier::ConfigVerifierExt;
                let config = tokio_rustls::rustls::ClientConfig::with_platform_verifier()?;
                let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h).to_string();
                let name = tokio_rustls::rustls::pki_types::ServerName::try_from(host)?;
                Box::new(tokio_rustls::TlsConnector::from(Arc::new(config)).connect(name, tcp).await.with_context(|| format!("TLS with {addr}"))?)
            }
            false => Box::new(tcp),
        };
        let mut io = (io, 0, false);
        if let Some((mech, user, pass)) = &s.sasl {
            login(&mut io, mech, user, pass).await.with_context(|| format!("logging in to {addr} as {user}"))?;
        }
        Ok(io.0)
    }

    /// One request, its answer (after the correlation id).
    async fn call(&self, api: i16, version: i16, body: &[u8]) -> Result<Rd> {
        let mut g = self.io.lock().await;
        if g.2 {
            *g = (Self::stream(&self.addr, &self.settings).await?, 0, false); // (a call left halfway: its answer would come next)
        }
        exchange(&mut g, api, version, body).await
    }
}

/// A request and its answer on a connection (marked halfway until the answer is read).
async fn exchange(g: &mut (Box<dyn Stream>, i32, bool), api: i16, version: i16, body: &[u8]) -> Result<Rd> {
        g.2 = true;
        g.1 += 1;
        let corr = g.1;
        let mut req = vec![];
        req.put_i16(api);
        req.put_i16(version);
        req.put_i32(corr);
        put_str(&mut req, "pondra");
        req.extend_from_slice(body);
        let mut framed = (req.len() as u32).to_be_bytes().to_vec();
        framed.extend(req);
        g.0.write_all(&framed).await?;
        let n = g.0.read_u32().await? as usize;
        ensure!(n <= 256 << 20, "a Kafka answer of {n} bytes");
        let mut buf = vec![0; n];
        g.0.read_exact(&mut buf).await?;
        let mut r = Rd { b: Bytes::from(buf), at: 0 };
        ensure!(r.i32()? == corr, "a Kafka answer out of order");
        g.2 = false;
        Ok(r)
}

async fn login(io: &mut (Box<dyn Stream>, i32, bool), mech: &str, user: &str, pass: &str) -> Result<()> {
        let mut b = vec![];
        put_str(&mut b, mech);
        let mut r = exchange(io, SASL_HANDSHAKE, 1, &b).await?;
        let err = r.i16()?;
        ensure!(err == 0, "the broker doesn't take SASL {mech} (error {err})");
        match mech {
            "PLAIN" => {
                authenticate(io, format!("\0{user}\0{pass}").as_bytes()).await?;
            }
            _ => {
                let (alg, len) = if mech == "SCRAM-SHA-512" { (aws_lc_rs::hmac::HMAC_SHA512, 64) } else { (aws_lc_rs::hmac::HMAC_SHA256, 32) };
                let nonce = uuid::Uuid::new_v4().simple().to_string();
                let first_bare = format!("n={},r={nonce}", user.replace('=', "=3D").replace(',', "=2C"));
                let server_first = String::from_utf8(authenticate(io, format!("n,,{first_bare}").as_bytes()).await?)?;
                let field = |k: &str| server_first.split(',').find_map(|p| p.strip_prefix(k)).map(str::to_string);
                let (r, salt, iterations) = (field("r=").context("SCRAM: no nonce")?, field("s=").context("SCRAM: no salt")?, field("i=").context("SCRAM: no iterations")?.parse::<u32>()?);
                ensure!(r.starts_with(&nonce), "SCRAM: the server's nonce isn't ours");
                use base64::Engine;
                let b64 = base64::engine::general_purpose::STANDARD;
                let mut salted = vec![0u8; len];
                let pbkdf = if len == 64 { aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA512 } else { aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA256 };
                aws_lc_rs::pbkdf2::derive(pbkdf, std::num::NonZeroU32::new(iterations).context("SCRAM: 0 iterations")?, &b64.decode(salt)?, pass.as_bytes(), &mut salted);
                let key = aws_lc_rs::hmac::Key::new(alg, &salted);
                let client_key = aws_lc_rs::hmac::sign(&key, b"Client Key");
                let stored = aws_lc_rs::digest::digest(if len == 64 { &aws_lc_rs::digest::SHA512 } else { &aws_lc_rs::digest::SHA256 }, client_key.as_ref());
                let without_proof = format!("c=biws,r={r}");
                let auth = format!("{first_bare},{server_first},{without_proof}");
                let signature = aws_lc_rs::hmac::sign(&aws_lc_rs::hmac::Key::new(alg, stored.as_ref()), auth.as_bytes());
                let proof: Vec<u8> = client_key.as_ref().iter().zip(signature.as_ref()).map(|(a, b)| a ^ b).collect();
                authenticate(io, format!("{without_proof},p={}", b64.encode(proof)).as_bytes()).await?;
            }
        }
        Ok(())
    }

async fn authenticate(io: &mut (Box<dyn Stream>, i32, bool), bytes: &[u8]) -> Result<Vec<u8>> {
        let mut b = vec![];
        b.put_i32(bytes.len() as i32);
        b.extend_from_slice(bytes);
        let mut r = exchange(io, SASL_AUTHENTICATE, 1, &b).await?;
        let (err, msg) = (r.i16()?, r.nstr()?);
        ensure!(err == 0, "refused: {}", msg.unwrap_or_else(|| format!("error {err}")));
        Ok(r.nbytes()?.map(|b| b.to_vec()).unwrap_or_default())
}

/// A record as read.
pub struct Record {
    pub offset: i64,
    pub ts: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

/// A cluster: its brokers, reached as needed, and where each topic's partitions lead.
pub struct Client {
    s: Settings,
    conns: Mutex<HashMap<String, Arc<Conn>>>,
    brokers: Mutex<HashMap<i32, String>>,
}

impl Client {
    pub fn new(s: Settings) -> Client { Client { s, conns: Default::default(), brokers: Default::default() } }

    async fn conn(&self, addr: &str) -> Result<Arc<Conn>> {
        if let Some(c) = self.conns.lock().await.get(addr) {
            return Ok(c.clone());
        }
        let c = Arc::new(Conn::open(addr, &self.s).await?);
        self.conns.lock().await.insert(addr.to_string(), c.clone());
        Ok(c)
    }

    /// Forget a connection that failed (the next call opens another).
    async fn drop_conn(&self, addr: &str) { self.conns.lock().await.remove(addr); }

    /// A topic's partitions and their leaders' addresses.
    pub async fn partitions(&self, topic: &str) -> Result<Vec<(i32, String)>> { self.partitions_or_new(topic, false).await }

    /// …made first if the cluster makes topics when asked for them (`create`: for writing).
    pub async fn partitions_or_new(&self, topic: &str, create: bool) -> Result<Vec<(i32, String)>> {
        let mut last = None;
        for _ in 0..if create { 10 } else { 1 } {
            for b in &self.s.brokers {
                match self.metadata(b, topic, create).await {
                    Ok(p) => return Ok(p),
                    Err(e) => {
                        self.drop_conn(b).await;
                        last = Some(e);
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await; // (a topic just made: its leaders come a moment later)
        }
        Err(last.context("no brokers")?)
    }

    async fn metadata(&self, broker: &str, topic: &str, create: bool) -> Result<Vec<(i32, String)>> {
        let mut b = vec![];
        b.put_i32(1);
        put_str(&mut b, topic);
        b.put_i8(create as i8);
        b.put_i8(0);
        b.put_i8(0);
        let mut r = self.conn(broker).await?.call(METADATA, 8, &b).await?;
        r.i32()?; // throttle
        let mut brokers = HashMap::new();
        for _ in 0..r.len()? {
            let (id, host, port) = (r.i32()?, r.str()?, r.i32()?);
            r.nstr()?; // rack
            brokers.insert(id, format!("{host}:{port}"));
        }
        r.nstr()?; // cluster id
        r.i32()?; // controller
        let mut out = vec![];
        for _ in 0..r.len()? {
            let (err, name) = (r.i16()?, r.str()?);
            r.i8()?; // internal
            ensure!(err != 3, "no topic {name} in the cluster");
            ensure!(err == 0, "the cluster's metadata for {name}: error {err}");
            for _ in 0..r.len()? {
                let (_, index, leader) = (r.i16()?, r.i32()?, r.i32()?);
                r.i32()?; // leader epoch
                for _ in 0..3 {
                    for _ in 0..r.len()? {
                        r.i32()?;
                    }
                }
                out.push((index, brokers.get(&leader).cloned().with_context(|| format!("{name}[{index}] has no leader now"))?));
            }
            r.i32()?; // authorized operations
        }
        ensure!(!out.is_empty(), "no topic {topic} in the cluster");
        self.brokers.lock().await.extend(brokers);
        out.sort();
        Ok(out)
    }

    /// A partition's offset at a time: -2 the earliest there is, -1 the next one written.
    pub async fn offset(&self, leader: &str, topic: &str, partition: i32, at: i64) -> Result<i64> {
        let mut b = vec![];
        b.put_i32(-1);
        b.put_i8(1); // (read committed: up to the last stable offset)
        b.put_i32(1);
        put_str(&mut b, topic);
        b.put_i32(1);
        b.put_i32(partition);
        b.put_i32(-1);
        b.put_i64(at);
        let mut r = self.conn(leader).await?.call(LIST_OFFSETS, 5, &b).await?;
        r.i32()?;
        for _ in 0..r.len()? {
            r.str()?;
            for _ in 0..r.len()? {
                let (_, err, _, offset) = (r.i32()?, r.i16()?, r.i64()?, r.i64()?);
                r.i32()?;
                ensure!(err == 0, "{topic}[{partition}]: its offsets (error {err})");
                return Ok(offset);
            }
        }
        bail!("{topic}[{partition}]: no offsets in the answer")
    }

    /// Records of a partition from `offset` (waiting up to `wait_ms` for some), the offset to
    /// fetch from next, and the partition's last stable offset (the next one a reader may see).
    pub async fn fetch(&self, leader: &str, topic: &str, partition: i32, offset: i64, wait_ms: i32) -> Result<(Vec<Record>, i64, i64)> {
        let mut b = vec![];
        b.put_i32(-1);
        b.put_i32(wait_ms);
        b.put_i32(1); // min bytes
        b.put_i32(32 << 20); // max bytes
        b.put_i8(1); // read committed
        b.put_i32(0); // session
        b.put_i32(-1);
        b.put_i32(1);
        put_str(&mut b, topic);
        b.put_i32(1);
        b.put_i32(partition);
        b.put_i32(-1);
        b.put_i64(offset);
        b.put_i64(-1);
        b.put_i32(16 << 20);
        b.put_i32(0); // forgotten topics
        put_str(&mut b, ""); // rack
        let conn = self.conn(leader).await?;
        let mut r = match conn.call(FETCH, 11, &b).await {
            Ok(r) => r,
            Err(e) => {
                self.drop_conn(leader).await;
                return Err(e);
            }
        };
        r.i32()?;
        let err = r.i16()?;
        ensure!(err == 0, "{topic}[{partition}]: fetching (error {err})");
        r.i32()?; // session
        for _ in 0..r.len()? {
            r.str()?;
            for _ in 0..r.len()? {
                let (_, err, _high, stable, _start) = (r.i32()?, r.i16()?, r.i64()?, r.i64()?, r.i64()?);
                ensure!(err == 0, "{topic}[{partition}]: fetching from offset {offset} (error {err}{})", if err == 1 { ": out of range, no longer kept" } else { "" });
                let mut aborted = vec![];
                for _ in 0..r.len()? {
                    aborted.push((r.i64()?, r.i64()?));
                }
                r.i32()?; // preferred read replica
                let records = r.nbytes()?.unwrap_or_default();
                let (recs, next) = decode(&records, offset, &aborted)?;
                return Ok((recs, next, stable));
            }
        }
        bail!("{topic}[{partition}]: no records in the answer")
    }

    /// A producer id for idempotent writes (retries within this producer are written once).
    pub async fn producer(&self) -> Result<(i64, i16)> {
        let broker = self.s.brokers[0].clone();
        let mut b = vec![];
        put_nstr(&mut b, None);
        b.put_i32(-1);
        let mut r = self.conn(&broker).await?.call(INIT_PRODUCER_ID, 1, &b).await?;
        r.i32()?;
        let err = r.i16()?;
        ensure!(err == 0, "a producer id (error {err})");
        Ok((r.i64()?, r.i16()?))
    }

    /// Records into a partition, acknowledged by all its in-sync replicas: the first one's offset.
    pub async fn produce(&self, leader: &str, topic: &str, partition: i32, producer: (i64, i16, i32), recs: &[(Option<Vec<u8>>, Option<Vec<u8>>)]) -> Result<i64> {
        let batch = encode(producer, recs);
        let mut b = vec![];
        put_nstr(&mut b, None);
        b.put_i16(-1); // acks: all
        b.put_i32(30_000);
        b.put_i32(1);
        put_str(&mut b, topic);
        b.put_i32(1);
        b.put_i32(partition);
        b.put_i32(batch.len() as i32);
        b.extend(batch);
        let mut r = self.conn(leader).await?.call(PRODUCE, 8, &b).await?;
        for _ in 0..r.len()? {
            r.str()?;
            for _ in 0..r.len()? {
                let (_, err, base) = (r.i32()?, r.i16()?, r.i64()?);
                r.i64()?;
                r.i64()?;
                for _ in 0..r.len()? {
                    r.i32()?;
                    r.nstr()?;
                }
                let msg = r.nstr()?;
                ensure!(err == 0 || err == 46, "{topic}[{partition}]: writing (error {err}{})", msg.map(|m| format!(": {m}")).unwrap_or_default()); // (46: a duplicate: written already)
                return Ok(base);
            }
        }
        bail!("{topic}[{partition}]: no answer to the write")
    }
}

/// A fetch's record batches as records from `from` on: control batches and aborted
/// transactions' records left out, a batch cut short at the end ignored. And the offset after
/// the last whole batch (markers and aborted records count: the next fetch starts there).
fn decode(mut b: &[u8], from: i64, aborted: &[(i64, i64)]) -> Result<(Vec<Record>, i64)> {
    let (mut out, mut next) = (vec![], from);
    let mut aborting: std::collections::HashSet<i64> = Default::default();
    let mut starts: Vec<(i64, i64)> = aborted.to_vec(); // (producer, first offset)
    starts.sort_by_key(|a| a.1);
    while b.len() >= 61 {
        let len = i32::from_be_bytes(b[8..12].try_into()?) as usize + 12;
        if len > b.len() {
            break; // (the rest comes with the next fetch)
        }
        let (batch, rest) = b.split_at(len);
        b = rest;
        ensure!(batch[16] == 2, "a record batch of format v{} (v2 only)", batch[16]);
        ensure!(u32::from_be_bytes(batch[17..21].try_into()?) == crc32c(&batch[21..]), "a record batch's CRC doesn't match");
        let base = i64::from_be_bytes(batch[..8].try_into()?);
        let mut r = Rd { b: Bytes::copy_from_slice(&batch[21..]), at: 0 };
        let attributes = r.i16()?;
        next = next.max(base + r.i32()? as i64 + 1);
        let base_ts = r.i64()?;
        r.i64()?;
        let (producer, _, _, count) = (r.i64()?, r.i16()?, r.i32()?, r.i32()?);
        while starts.first().is_some_and(|s| s.1 <= base) {
            aborting.insert(starts.remove(0).0);
        }
        if attributes & 0x20 != 0 {
            aborting.remove(&producer); // (a control batch: its transaction ends here)
            continue;
        }
        if attributes & 0x10 != 0 && aborting.contains(&producer) {
            continue; // (a transaction that was aborted)
        }
        let body = decompress(attributes & 7, &r.b[r.at..])?;
        let log_append = attributes & 8 != 0;
        let mut r = Rd { b: body.into(), at: 0 };
        for _ in 0..count {
            r.varint()?;
            r.i8()?;
            let ts = base_ts + r.varint()?;
            let offset = base + r.varint()?;
            let (key, value) = (r.vbytes()?, r.vbytes()?);
            for _ in 0..r.varint()? {
                r.vbytes()?;
                r.vbytes()?;
            }
            if offset >= from {
                out.push(Record { offset, ts: if log_append { base_ts } else { ts }, key, value });
            }
        }
    }
    Ok((out, next))
}

/// A record batch (v2, uncompressed) of an idempotent producer.
fn encode((producer, epoch, seq): (i64, i16, i32), recs: &[(Option<Vec<u8>>, Option<Vec<u8>>)]) -> Vec<u8> {
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64);
    let mut body = vec![];
    for (i, (key, value)) in recs.iter().enumerate() {
        let mut rec = vec![0u8];
        put_varint(&mut rec, 0);
        put_varint(&mut rec, i as i64);
        for field in [key, value] {
            put_varint(&mut rec, field.as_ref().map_or(-1, |f| f.len() as i64));
            rec.extend(field.iter().flatten());
        }
        put_varint(&mut rec, 0);
        put_varint(&mut body, rec.len() as i64);
        body.extend(rec);
    }
    let mut tail = vec![];
    tail.put_i16(0);
    tail.put_i32(recs.len() as i32 - 1);
    tail.put_i64(ts);
    tail.put_i64(ts);
    tail.put_i64(producer);
    tail.put_i16(epoch);
    tail.put_i32(seq);
    tail.put_i32(recs.len() as i32);
    tail.extend(body);
    let mut b = vec![];
    b.put_i64(0);
    b.put_i32(tail.len() as i32 + 9);
    b.put_i32(-1);
    b.put_i8(2);
    b.put_u32(crc32c(&tail));
    b.extend(tail);
    b
}

// ---------------------------------------------------------------- topics as tables

/// A topic's URL, split: its brokers and its name (`kafka://host:9092,host2:9092/topic`).
pub fn parse(url: &str) -> Result<(String, String)> {
    let rest = url.strip_prefix("kafka://").context("a Kafka URL is kafka://brokers/topic")?;
    let (brokers, topic) = rest.split_once('/').context("which topic? kafka://brokers/topic")?;
    ensure!(!topic.is_empty() && !topic.contains('/'), "{url}: one topic, as kafka://brokers/topic");
    Ok((brokers.to_string(), topic.to_string()))
}

/// A client of the cluster at `url`, with the secret covering it (kept: its connections too).
pub async fn client(lake: &crate::store::Lake, url: &str) -> Result<Arc<Client>> {
    static CLIENTS: std::sync::LazyLock<std::sync::Mutex<HashMap<String, Arc<Client>>>> = std::sync::LazyLock::new(Default::default);
    let (brokers, _) = parse(url)?;
    let secret = crate::ext::secret_for(lake, url).await?;
    let key = format!("{brokers}|{}", secret.as_ref().map(|s| serde_json::to_string(s).unwrap_or_default()).unwrap_or_default());
    if let Some(c) = CLIENTS.lock().unwrap().get(&key) {
        return Ok(c.clone());
    }
    let c = Arc::new(Client::new(Settings::of(&brokers, secret.as_ref())?));
    CLIENTS.lock().unwrap().insert(key, c.clone());
    Ok(c)
}

/// A topic's columns: its records' partition, offset and time (named as system columns are), key
/// and value (as text).
pub fn columns() -> Vec<(String, String)> {
    ["_partition:Int32", "_offset:Int64", "_timestamp:Timestamp(Millisecond, Some(\"UTC\"))", "key:Utf8", "value:Utf8"].iter()
        .map(|c| c.split_once(':').map(|(n, t)| (n.to_string(), t.to_string())).expect("a column")).collect()
}

/// A topic as a table of what it holds now: a "file" per partition with records, its offsets
/// from the earliest kept to the last stable one (`#partition:from-to`), statistics by
/// partition and offset (so a filter on them skips partitions).
pub async fn resolve(lake: &crate::store::Lake, url: &str) -> Result<crate::store::TableMeta> {
    let (_, topic) = parse(url)?;
    let c = client(lake, url).await?;
    let mut files = vec![];
    for (p, leader) in c.partitions(&topic).await? {
        let (from, to) = (c.offset(&leader, &topic, p, -2).await?, c.offset(&leader, &topic, p, -1).await?);
        if to > from {
            let mut f = crate::store::DataFile { path: format!("{url}#{p}:{from}-{to}"), rows: (to - from) as u64, bytes: (to - from) as u64 * 100, nulls: None, ..Default::default() };
            f.stats.insert("_partition".into(), (p.to_string(), p.to_string()));
            f.stats.insert("_offset".into(), (from.to_string(), (to - 1).to_string()));
            files.push(f);
        }
    }
    Ok(crate::store::TableMeta { columns: columns(), files, ..Default::default() })
}

/// The records of these partitions' ranges, read as the query asks for them (a LIMIT stops
/// fetching).
pub async fn read(lake: &crate::store::Lake, ctx: &datafusion::prelude::SessionContext, files: &[&crate::store::DataFile], schema: &datafusion::arrow::datatypes::SchemaRef) -> Result<datafusion::prelude::DataFrame> {
    let mut parts: Vec<Arc<dyn datafusion::physical_plan::streaming::PartitionStream>> = vec![];
    for f in files {
        let (url, range) = f.path.split_once('#').context("a topic's partition")?;
        let (p, offsets) = range.split_once(':').context("a partition's offsets")?;
        let (from, to) = offsets.split_once('-').context("a partition's offsets")?;
        let (_, topic) = parse(url)?;
        let c = client(lake, url).await?;
        let p: i32 = p.parse()?;
        let leader = c.partitions(&topic).await?.into_iter().find(|(i, _)| *i == p).map(|(_, l)| l).with_context(|| format!("{topic}[{p}] is gone"))?;
        parts.push(Arc::new(Range { client: c, leader, topic, partition: p, from: from.parse()?, to: to.parse()?, schema: schema.clone() }));
    }
    let table = datafusion::catalog::streaming::StreamingTable::try_new(schema.clone(), parts)?;
    Ok(ctx.read_table(Arc::new(table))?)
}

#[derive(Debug)]
struct Range {
    client: Arc<Client>,
    leader: String,
    topic: String,
    partition: i32,
    from: i64,
    to: i64,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Kafka({})", self.s.brokers.join(",")) }
}

impl datafusion::physical_plan::streaming::PartitionStream for Range {
    fn schema(&self) -> &datafusion::arrow::datatypes::SchemaRef { &self.schema }

    fn execute(&self, _: Arc<datafusion::execution::TaskContext>) -> datafusion::execution::SendableRecordBatchStream {
        use futures::TryStreamExt;
        let (c, leader, topic, p, to, schema) = (self.client.clone(), self.leader.clone(), self.topic.clone(), self.partition, self.to, self.schema.clone());
        let s2 = schema.clone();
        let stream = futures::stream::try_unfold(self.from, move |at| {
            let (c, leader, topic, schema) = (c.clone(), leader.clone(), topic.clone(), s2.clone());
            async move {
                if at >= to {
                    return anyhow::Ok(None);
                }
                let (recs, next, _) = c.fetch(&leader, &topic, p, at, 0).await?;
                if next <= at && recs.is_empty() {
                    return Ok(None); // (nothing left below the end: offsets needn't be dense — compacted topics, Pondra's own)
                }
                let recs: Vec<Record> = recs.into_iter().filter(|r| r.offset < to).collect();
                Ok(Some((batch(&schema, p, &recs)?, next.max(recs.last().map_or(at, |r| r.offset + 1)))))
            }
        }).map_err(|e| datafusion::error::DataFusionError::External(e.into()));
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream))
    }
}

/// Records as rows of a topic's columns (keys and values as UTF-8 text).
pub fn batch(schema: &datafusion::arrow::datatypes::SchemaRef, p: i32, recs: &[Record]) -> Result<datafusion::arrow::record_batch::RecordBatch> {
    use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray, TimestampMillisecondArray};
    let text = |b: &Option<Bytes>| b.as_ref().map(|b| String::from_utf8_lossy(b).into_owned());
    let all: Vec<(&str, ArrayRef)> = vec![
        ("_partition", Arc::new(Int32Array::from(vec![p; recs.len()]))),
        ("_offset", Arc::new(Int64Array::from_iter_values(recs.iter().map(|r| r.offset)))),
        ("_timestamp", Arc::new(TimestampMillisecondArray::from_iter_values(recs.iter().map(|r| r.ts)).with_timezone("UTC"))),
        ("key", Arc::new(StringArray::from(recs.iter().map(|r| text(&r.key)).collect::<Vec<_>>()))),
        ("value", Arc::new(StringArray::from(recs.iter().map(|r| text(&r.value)).collect::<Vec<_>>()))),
    ];
    let columns = schema.fields().iter().map(|f| {
        let a = all.iter().find(|(n, _)| n == f.name()).map(|(_, a)| a.clone()).with_context(|| format!("a topic has no column {}", f.name()))?;
        Ok(datafusion::arrow::compute::cast(&a, f.data_type())?)
    }).collect::<Result<Vec<_>>>()?;
    Ok(datafusion::arrow::record_batch::RecordBatch::try_new(schema.clone(), columns)?)
}

// ---------------------------------------------------------------- rows out

/// `COPY (query) TO 'kafka://brokers/topic' (FORMAT json, KEY col)`: each row a record, its value
/// the row as JSON (FORMAT raw: the one column's text), its key a column's text (by its hash,
/// the partition, as Kafka's own producers pick it; without a key, spread over the partitions).
/// Written by an idempotent producer, every in-sync replica acknowledging: a retry inside the
/// statement never writes a row twice.
pub async fn copy_to(lake: &crate::store::Lake, df: datafusion::prelude::DataFrame, to: &str, options: &BTreeMap<String, String>) -> Result<serde_json::Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use futures::StreamExt;
    let (_, topic) = parse(to)?;
    let format = options.get("format").map(|f| f.to_lowercase()).unwrap_or_else(|| "json".into());
    ensure!(["json", "raw"].contains(&format.as_str()), "COPY … TO a topic as {format}: json or raw");
    let c = client(lake, to).await?;
    let parts = c.partitions_or_new(&topic, true).await?;
    let names: Vec<String> = df.schema().fields().iter().map(|f| f.name().clone()).collect();
    let key_at = match options.get("key") {
        Some(k) => Some(names.iter().position(|n| n == k).with_context(|| format!("KEY {k}: no such column ({})", names.join(", ")))?),
        None => None,
    };
    ensure!(format == "json" || names.len() == 1, "FORMAT raw writes one column (the query gives {})", names.len());
    let (id, epoch) = c.producer().await?;
    let mut seqs = vec![0i32; parts.len()];
    let (mut rows, mut spread) = (0u64, 0usize);
    let mut stream = df.execute_stream().await?;
    while let Some(b) = stream.next().await.transpose()? {
        let values: Vec<Option<Vec<u8>>> = match format.as_str() {
            "raw" => {
                let text = datafusion::arrow::compute::cast(b.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
                let text = text.as_string::<i32>();
                (0..b.num_rows()).map(|i| (!text.is_null(i)).then(|| text.value(i).as_bytes().to_vec())).collect()
            }
            _ => {
                let mut w = datafusion::arrow::json::LineDelimitedWriter::new(vec![]);
                w.write(&b)?;
                w.finish()?;
                w.into_inner().split(|c| *c == b'\n').filter(|l| !l.is_empty()).map(|l| Some(l.to_vec())).collect()
            }
        };
        let keys: Vec<Option<Vec<u8>>> = match key_at {
            Some(k) => {
                let text = datafusion::arrow::compute::cast(b.column(k), &datafusion::arrow::datatypes::DataType::Utf8)?;
                let text = text.as_string::<i32>();
                (0..b.num_rows()).map(|i| (!text.is_null(i)).then(|| text.value(i).as_bytes().to_vec())).collect()
            }
            None => vec![None; b.num_rows()],
        };
        let mut by_partition: Vec<Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>> = vec![vec![]; parts.len()];
        spread = (spread + 1) % parts.len();
        for (k, v) in keys.into_iter().zip(values) {
            let p = k.as_deref().map_or(spread, |k| (murmur2(k) & 0x7fffffff) as usize % parts.len()); // (Kafka's own choice for a key)
            by_partition[p].push((k, v));
        }
        for (i, recs) in by_partition.iter().enumerate() {
            for chunk in recs.chunks(5_000) {
                let (p, leader) = &parts[i];
                c.produce(leader, &topic, *p, (id, epoch, seqs[i]), chunk).await?;
                seqs[i] += chunk.len() as i32;
                rows += chunk.len() as u64;
            }
        }
    }
    Ok(serde_json::json!({"copied": rows, "to": to}))
}

/// Kafka's murmur2 (the Java client's `Utils.murmur2`): how its producers pick a key's partition.
fn murmur2(data: &[u8]) -> u32 {
    let (m, r) = (0x5bd1e995u32, 24);
    let mut h = 0x9747b28cu32 ^ data.len() as u32;
    let chunks = data.chunks_exact(4);
    let tail = chunks.remainder();
    for c in chunks {
        let mut k = u32::from_le_bytes(c.try_into().expect("four bytes")).wrapping_mul(m);
        k ^= k >> r;
        h = h.wrapping_mul(m) ^ k.wrapping_mul(m);
    }
    match tail.len() {
        3 => h = (h ^ ((tail[2] as u32) << 16) ^ ((tail[1] as u32) << 8) ^ tail[0] as u32).wrapping_mul(m),
        2 => h = (h ^ ((tail[1] as u32) << 8) ^ tail[0] as u32).wrapping_mul(m),
        1 => h = (h ^ tail[0] as u32).wrapping_mul(m),
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(m);
    h ^ (h >> 15)
}
