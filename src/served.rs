//! A lake's store over HTTP (ADR-072 §8). A node serves any store it holds, at `/cluster/store`
//! (`door`), and another node reads and writes that store through `Served`, an `ObjectStore` that
//! talks to the door. So a lake held in one node's memory or folder can have more nodes with no
//! bucket: they reach the store through the node that holds it, and everything else (the log, the
//! catalog, terms, tiering, spreading) goes through `ObjectStore` as it does for a bucket. The node
//! that serves a store leads its lake, and the others never claim a term (`checked`, invariant 299).

use crate::byte_range;
use crate::store::Store;
use axum::body::Body;
use axum::extract::{self, Query, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, Stream};
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::client::{HttpClient, HttpError, HttpErrorKind, HttpRequest, HttpRequestBody, HttpResponse};
use object_store::path::Path;
use object_store::{
    Attributes, CopyMode, CopyOptions, Error as E, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, TagSet, UpdateVersion,
    UploadPart,
};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::ops::Range;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;
use url::Url;

const MIB: u64 = 1 << 20;
/// A PUT's body past this is refused: a merge's file is 256 MB at most.
const MAX_BODY: u64 = 1 << 30;
/// An answer at least this long is streamed, not read into one buffer.
const STREAM_FROM: u64 = 8 * MIB;
/// How long a body may go without a chunk before its read fails (a stalled bucket fails the same way).
const IDLE: Duration = Duration::from_secs(30);

/// One object as the listings and the object headers say it: the same fields on both sides.
#[derive(Serialize, Deserialize)]
struct Meta { path: String, size: u64, modified_ns: i64, e_tag: Option<String>, version: Option<String> }

impl From<&ObjectMeta> for Meta {
    fn from(m: &ObjectMeta) -> Meta {
        Meta { path: m.location.to_string(), size: m.size, modified_ns: nanos(m.last_modified), e_tag: m.e_tag.clone(), version: m.version.clone() }
    }
}

impl Meta {
    fn meta(self) -> object_store::Result<ObjectMeta> {
        Ok(ObjectMeta { location: Path::parse(&self.path)?, last_modified: DateTime::from_timestamp_nanos(self.modified_ns), size: self.size, e_tag: self.e_tag, version: self.version })
    }
}

/// A listing's last line when the store fails midway: the error's text and the status it answers with.
#[derive(Serialize, Deserialize)]
struct Failed { error: String, status: u16 }

/// One line of a listing: an object, or the failure that ends it.
#[derive(Deserialize)]
#[serde(untagged)]
enum Line { Failed(Failed), Object(Meta) }

/// A delimited listing: the objects under the prefix and the prefixes under it.
#[derive(Serialize, Deserialize)]
struct Delimited { objects: Vec<Meta>, prefixes: Vec<String> }

/// A listing's query: `prefix`, `offset` (the objects after it) or `delimiter` (any value).
#[derive(Deserialize)]
struct Listing { prefix: Option<String>, offset: Option<String>, delimiter: Option<String> }

/// A copy's query: the path it copies from.
#[derive(Deserialize)]
struct CopyFrom { from: Option<String> }

/// A time as nanoseconds since the epoch, which the headers and listings carry so it comes back
/// exactly (no store reports a time past 2262, the last one that has none).
fn nanos(t: DateTime<Utc>) -> i64 { t.timestamp_nanos_opt().unwrap_or_default() }

// ---- the door (the server side) ----

/// The store, served: its listings at `/`, its objects at `/{path}`. The node mounts it at
/// `/cluster/store`, through `door_for`, which decides who may call it. It serves whatever store it is given.
pub fn door(store: Store) -> Router {
    Router::new()
        .route("/", get(list))
        .route("/{*path}", get(object).put(put_object).delete(delete_object).post(copy_object))
        .layer(middleware::map_response(stamped))
        .with_state(store)
}

/// The store door as a node mounts it: its lake's store, to whoever runs the nodes (their admin token;
/// a user's, even a superuser's, is not), when the node serves its store (`serving`, read once at its
/// start: `Cluster::serves_store`); otherwise a door that says how to open it.
pub fn door_for(store: Store, serving: bool) -> Router {
    if serving {
        return door(store).layer(middleware::from_fn(operator_only));
    }
    Router::new().fallback(|| async { refused("this node doesn't serve its lake's store: start it with --serve-store") })
}

/// Passes a request only from whoever runs the nodes (`auth::operator`: their admin token, or the
/// program that started the node). The guard has let the request in as a sign-in, and says who it was.
async fn operator_only(req: extract::Request, next: middleware::Next) -> Response {
    if crate::auth::operator() {
        return next.run(req).await;
    }
    refused("the store of a lake is opened by the nodes that run it, with the admin token they share; a user's token (a superuser's too) can't open it")
}

/// A refusal of the door: 403, with the header that makes the client say it as `PermissionDenied`.
fn refused(text: &'static str) -> Response {
    let mut res = (StatusCode::FORBIDDEN, text).into_response();
    res.headers_mut().insert("x-pondra-store", HeaderValue::from_static("1"));
    res
}

/// Whether a node may start on `dir` as it is asked: one that serves its store (`serving`) leads it, so it is
/// never a reader, holds its lake on this machine, and has the admin token its nodes open the store with; a
/// node of a `pondra://` lake opens it with that token too.
pub fn checked(dir: &str, reader: bool, admin: bool, serving: bool) -> anyhow::Result<()> {
    anyhow::ensure!(!(serving && reader), "--serve-store: the node that serves a lake's store leads it, and a --reader never leads");
    anyhow::ensure!(!(serving && dir.contains("://") && !crate::memory::in_memory(dir)), "--serve-store serves a lake on this machine (a folder, or :memory:): {dir} is reached by its own URL");
    anyhow::ensure!(!(serving && !admin), "--serve-store needs --admin-token (or PONDRA_ADMIN_TOKEN), the same on every node: the nodes open the store with it");
    anyhow::ensure!(!(crate::store::served(dir) && !admin), "a node of {dir} opens its store with the admin token its holder has: set --admin-token (or PONDRA_ADMIN_TOKEN)");
    Ok(())
}

/// Every answer the door makes says it is a door's, errors and axum's own rejections too.
async fn stamped(mut res: Response) -> Response {
    res.headers_mut().insert("x-pondra-store", HeaderValue::from_static("1"));
    res
}

/// A path off the wire, parsed as it is: `Path::from` would percent-encode the text again.
fn path_of(raw: &str) -> Result<Path, Response> { Path::parse(raw).map_err(|e| bad(e.to_string())) }

fn bad(msg: impl Into<String>) -> Response { (StatusCode::BAD_REQUEST, msg.into()).into_response() }

/// The status an error answers with; the client maps it back to the same variant (`error`).
fn status(e: &E) -> StatusCode {
    match e {
        E::NotFound { .. } => StatusCode::NOT_FOUND,
        E::AlreadyExists { .. } | E::Precondition { .. } => StatusCode::PRECONDITION_FAILED,
        E::NotModified { .. } => StatusCode::NOT_MODIFIED,
        E::NotImplemented { .. } | E::NotSupported { .. } => StatusCode::NOT_IMPLEMENTED,
        E::InvalidPath { .. } => StatusCode::BAD_REQUEST,
        E::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        E::Unauthenticated { .. } => StatusCode::UNAUTHORIZED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// A store's error as an answer. The body is the error's text, except an unimplemented operation's
/// body is its name (the client's `operation`); `x-pondra-error` says which 501 it was.
fn answer_error(e: E) -> Response {
    let (body, kind) = match &e {
        E::NotImplemented { operation, .. } => (operation.clone(), Some("not-implemented")),
        E::NotSupported { .. } => (e.to_string(), Some("not-supported")),
        _ => (e.to_string(), None),
    };
    let mut res = (status(&e), body).into_response();
    if let Some(kind) = kind {
        res.headers_mut().insert("x-pondra-error", HeaderValue::from_static(kind));
    }
    res
}

/// A header value for text the store gave (an e-tag, a version): one no header can carry is the
/// store's fault, a 500.
fn header_of(text: &str) -> Result<HeaderValue, Response> {
    HeaderValue::from_str(text).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the store gave {text:?}, which a header can't carry: {e}")).into_response())
}

fn text_of(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

/// The listings: `?prefix=` (optional), `&offset=` (the objects after it) or `&delimiter=1` (the
/// objects and the prefixes under the prefix). Objects stream out one JSON line each.
async fn list(State(store): State<Store>, Query(q): Query<Listing>) -> Response {
    list_at(&store, q).await.unwrap_or_else(|r| r)
}

async fn list_at(store: &Store, q: Listing) -> Result<Response, Response> {
    let prefix = q.prefix.as_deref().map(|p| Path::parse(p).map_err(|e| bad(e.to_string()))).transpose()?;
    if q.delimiter.is_some() {
        let got = store.list_with_delimiter(prefix.as_ref()).await.map_err(answer_error)?;
        let body = Delimited { objects: got.objects.iter().map(Meta::from).collect(), prefixes: got.common_prefixes.iter().map(Path::to_string).collect() };
        return Ok(Json(body).into_response());
    }
    let objects = match &q.offset {
        Some(offset) => store.list_with_offset(prefix.as_ref(), &path_of(offset)?),
        None => store.list(prefix.as_ref()),
    };
    // (after a store's failure its line ends the listing: nothing after it is sent)
    let lines = objects.scan(false, |failed, item| {
        let line = match item {
            _ if *failed => None,
            Ok(meta) => Some(ndjson(&Meta::from(&meta))),
            Err(e) => {
                *failed = true;
                Some(ndjson(&Failed { error: e.to_string(), status: status(&e).as_u16() }))
            }
        };
        futures::future::ready(line)
    });
    let body = Body::from_stream(lines.map(Ok::<Bytes, Infallible>));
    Ok(([("content-type", "application/x-ndjson")], body).into_response())
}

/// One JSON value and its newline, as a listing's line.
fn ndjson<T: Serialize>(value: &T) -> Bytes {
    let mut line = serde_json::to_vec(value).unwrap_or_default(); // (our own values: always serializable)
    line.push(b'\n');
    Bytes::from(line)
}

/// A GET or HEAD of an object: its bytes (or none, for HEAD) with the store's headers for it.
async fn object(State(store): State<Store>, method: Method, extract::Path(raw): extract::Path<String>, headers: HeaderMap) -> Response {
    get_at(&store, method == Method::HEAD, &raw, &headers).await.unwrap_or_else(|r| r)
}

async fn get_at(store: &Store, head: bool, raw: &str, headers: &HeaderMap) -> Result<Response, Response> {
    let at = path_of(raw)?;
    let range = requested_range(raw, headers)?;
    let asked = range.is_some();
    let opts = GetOptions {
        range,
        if_match: text_of(headers, "if-match"),
        if_none_match: text_of(headers, "if-none-match"),
        head,
        ..Default::default()
    };
    let got = store.get_opts(&at, opts).await.map_err(answer_error)?;
    let (meta, span) = (got.meta.clone(), got.range.clone());
    let body = if head {
        Body::empty()
    } else if span.end.saturating_sub(span.start) < STREAM_FROM {
        Body::from(got.bytes().await.map_err(answer_error)?)
    } else {
        Body::from_stream(got.into_stream())
    };
    let partial = asked.then(|| byte_range::content_range(&span, meta.size));
    let mut res = Response::new(body);
    *res.status_mut() = if asked { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
    object_headers(&mut res, &meta, partial)?;
    Ok(res)
}

/// The Range header as a range the store can serve; any other is a 400.
fn requested_range(raw: &str, headers: &HeaderMap) -> Result<Option<GetRange>, Response> {
    let Some(value) = headers.get("range") else { return Ok(None) };
    value.to_str().ok().and_then(byte_range::parse).filter(|r| r.is_valid().is_ok())
        .ok_or_else(|| bad(format!("{raw}: a Range this store can't serve")))
        .map(Some)
}

/// The headers every answer about an object carries, as the store gave the object.
fn object_headers(res: &mut Response, meta: &ObjectMeta, partial: Option<String>) -> Result<(), Response> {
    let h = res.headers_mut();
    if let Some(e_tag) = &meta.e_tag {
        h.insert("etag", header_of(e_tag)?);
    }
    if let Some(version) = &meta.version {
        h.insert("x-pondra-version", header_of(version)?);
    }
    h.insert("x-pondra-size", header_of(&meta.size.to_string())?);
    h.insert("x-pondra-modified-ns", header_of(&nanos(meta.last_modified).to_string())?);
    if let Some(range) = partial {
        h.insert("content-range", header_of(&range)?);
    }
    Ok(())
}

/// A PUT: the body's bytes, written as the headers ask (`If-None-Match: *` creates, `If-Match` updates
/// the version it names, neither overwrites).
async fn put_object(State(store): State<Store>, extract::Path(raw): extract::Path<String>, headers: HeaderMap, body: Body) -> Response {
    put_at(&store, &raw, &headers, body).await.unwrap_or_else(|r| r)
}

async fn put_at(store: &Store, raw: &str, headers: &HeaderMap, body: Body) -> Result<Response, Response> {
    let at = path_of(raw)?;
    let payload = body_of(headers, body).await?;
    let put = store.put_opts(&at, payload, PutOptions { mode: write_mode(headers), ..Default::default() }).await.map_err(answer_error)?;
    let mut res = StatusCode::OK.into_response();
    if let Some(e_tag) = &put.e_tag {
        res.headers_mut().insert("etag", header_of(e_tag)?);
    }
    if let Some(version) = &put.version {
        res.headers_mut().insert("x-pondra-version", header_of(version)?);
    }
    Ok(res)
}

fn write_mode(headers: &HeaderMap) -> PutMode {
    if text_of(headers, "if-none-match").as_deref() == Some("*") {
        return PutMode::Create;
    }
    match text_of(headers, "if-match") {
        Some(e_tag) => PutMode::Update(UpdateVersion { e_tag: Some(e_tag), version: text_of(headers, "x-pondra-version") }),
        None => PutMode::Overwrite,
    }
}

/// A PUT's body in the frames it arrived in (held once, never copied into one buffer); past
/// `MAX_BODY`, 413 as soon as it is known.
async fn body_of(headers: &HeaderMap, body: Body) -> Result<PutPayload, Response> {
    let too_big = || (StatusCode::PAYLOAD_TOO_LARGE, format!("a body past {MAX_BODY} bytes is refused")).into_response();
    if text_of(headers, "content-length").and_then(|n| n.parse::<u64>().ok()).is_some_and(|n| n > MAX_BODY) {
        return Err(too_big());
    }
    let mut frames = Vec::new();
    let mut total = 0u64;
    let mut data = body.into_data_stream();
    while let Some(frame) = data.next().await {
        let frame = frame.map_err(|e| bad(format!("the body couldn't be read: {e}")))?;
        total += frame.len() as u64;
        if total > MAX_BODY {
            return Err(too_big());
        }
        frames.push(frame);
    }
    Ok(PutPayload::from_iter(frames))
}

/// A DELETE: 204 whether or not the object was there (a local disk says NotFound, a memory store
/// doesn't; the client sees the same either way).
async fn delete_object(State(store): State<Store>, extract::Path(raw): extract::Path<String>) -> Response {
    let at = match path_of(&raw) {
        Ok(at) => at,
        Err(r) => return r,
    };
    match store.delete(&at).await {
        Ok(()) | Err(E::NotFound { .. }) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => answer_error(e),
    }
}

/// A POST to `/{to}?from=<path>`: a copy inside the store (`If-None-Match: *` makes it create).
async fn copy_object(State(store): State<Store>, extract::Path(raw): extract::Path<String>, Query(q): Query<CopyFrom>, headers: HeaderMap) -> Response {
    copy_at(&store, &raw, q, &headers).await.unwrap_or_else(|r| r)
}

async fn copy_at(store: &Store, raw: &str, q: CopyFrom, headers: &HeaderMap) -> Result<Response, Response> {
    let to = path_of(raw)?;
    let from = path_of(&q.from.ok_or_else(|| bad("a copy names its source: ?from=<path>"))?)?;
    let mode = if text_of(headers, "if-none-match").as_deref() == Some("*") { CopyMode::Create } else { CopyMode::Overwrite };
    store.copy_opts(&from, &to, CopyOptions { mode, ..Default::default() }).await.map_err(answer_error)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

// ---- the client side ----

/// What a `Served` store reaches: the door's base URL, and the client that takes the bucket's turns
/// and carries the node's token and TLS.
#[derive(Clone, Debug)]
struct Link { at: String, base: Url, client: HttpClient }

/// A lake's store on the node at `host:port`, read and written through that node's door. Nothing is
/// connected until the first request: a node's TLS and token are set after `open_store` runs.
#[derive(Debug)]
pub struct Served { at: String, made: OnceLock<Result<Link, String>> }

impl Served {
    pub fn new(host_port: &str) -> Served { Served { at: host_port.to_string(), made: OnceLock::new() } }

    /// The door's link, made at the first request.
    fn link(&self) -> Result<Link, String> {
        self.made
            .get_or_init(|| {
                let base = Url::parse(&crate::tls::url(&format!("{}/cluster/store", self.at))).map_err(|e| e.to_string())?;
                Ok(Link { at: self.at.clone(), base, client: crate::budget::client(&format!("pondra://{}", self.at), crate::cluster::http()) })
            })
            .clone()
    }

    fn link_of(&self) -> object_store::Result<Link> { self.link().map_err(|e| generic(&self.at, e)) }

    fn list_from(&self, prefix: Option<&Path>, offset: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let made = self.link();
        let served = self.at.clone();
        let query = listing_query(prefix, offset);
        let prefix_text = prefix.map(Path::to_string).unwrap_or_default();
        futures::stream::once(async move {
            let link = made.map_err(|e| generic(&served, e))?;
            let answer = link.listing(&query, &prefix_text).await?;
            Ok::<_, E>(lines(chunks(answer.into_body().bytes_stream(), served.clone()), &served))
        })
        .try_flatten()
        .boxed()
    }
}

impl std::fmt::Display for Served {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "pondra://{}", self.at) }
}

/// Until the holder at `at` answers (any HTTP answer, whatever its status), once a second. A node of its
/// lake that starts while the holder is away waits here, and says so once.
pub async fn wait_up(at: &str) {
    let url = crate::tls::url(&format!("{at}/healthz"));
    let mut said = false;
    loop {
        if crate::cluster::http().get(url.as_str()).timeout(Duration::from_secs(2)).send().await.is_ok() {
            return;
        }
        if !std::mem::replace(&mut said, true) {
            eprintln!("waiting for pondra://{at} to answer");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// What one request sends: its method, URL and headers, and its body (none, or a payload a try sends
/// again: a `PutPayload` is cheap to copy). `conditional`: a write a second try could find its own
/// first write of.
struct Ask { method: Method, url: Url, headers: HeaderMap, body: Option<PutPayload>, conditional: bool }

impl Link {
    /// The URL of the object at `at`: the base, then each part of the path, percent-encoded on its own.
    fn object(&self, at: &Path) -> object_store::Result<Url> {
        if at.parts_count() == 0 {
            return Err(generic(&self.at, "the root of a store is not an object"));
        }
        let mut url = self.base.clone();
        url.path_segments_mut().map_err(|()| generic(&self.at, "has no path to add an object to"))?.extend(at.parts());
        Ok(url)
    }

    /// Send `ask` and return the answer (its body is the caller's to read). A try waits for the
    /// headers 30 s at most, and a second more for each MiB sent. A try that fails is sent again after
    /// 100 ms and then 400 ms: 3 tries in all. A conditional write is sent again only when it never
    /// left this node, since a second try could find its own first write: a timeout or a 503 is its error.
    async fn send(&self, ask: Ask) -> object_store::Result<HttpResponse> {
        let mib = ask.body.as_ref().map_or(0, |b| b.content_length() as u64 / MIB);
        let wait = Duration::from_secs(30 + mib);
        let mut gaps = [100, 400].map(Duration::from_millis).into_iter();
        loop {
            let sent = tokio::time::timeout(wait, self.client.execute(self.request(&ask)?)).await;
            let answer = match sent {
                Ok(answer) => answer,
                Err(_) => Err(HttpError::new(HttpErrorKind::Timeout, std::io::Error::new(std::io::ErrorKind::TimedOut, "no answer in time"))),
            };
            let again = match &answer {
                Ok(r) => !ask.conditional && matches!(r.status().as_u16(), 502..=504),
                Err(e) => !ask.conditional || e.kind() == HttpErrorKind::Connect,
            };
            match gaps.next() {
                Some(gap) if again => tokio::time::sleep(gap).await,
                _ => return answer.map_err(|e| generic(&self.at, format!("{} {} failed: {e}", ask.method, ask.url.path()))),
            }
        }
    }

    /// The request one try sends, built afresh.
    fn request(&self, ask: &Ask) -> object_store::Result<HttpRequest> {
        let body = ask.body.clone().map_or_else(HttpRequestBody::empty, HttpRequestBody::from);
        let mut req = axum::http::Request::builder()
            .method(ask.method.clone())
            .uri(ask.url.as_str())
            .body(body)
            .map_err(|e| generic(&self.at, e))?;
        *req.headers_mut() = ask.headers.clone();
        Ok(req)
    }

    /// The error for an answer that isn't a success, its body read for the text.
    async fn failed(&self, answer: HttpResponse, path: &str, create: bool) -> E {
        let status = answer.status();
        let headers = answer.headers().clone();
        let body = answer.into_body().bytes().await.map(|b| String::from_utf8_lossy(&b).into_owned()).unwrap_or_default();
        error(&self.at, status, &headers, &body, path, create)
    }

    /// The answer to a listing: the door's root with the query, read as a stream by the caller.
    async fn listing(&self, query: &[(&str, String)], prefix: &str) -> object_store::Result<HttpResponse> {
        let mut url = self.base.clone();
        for (key, value) in query {
            url.query_pairs_mut().append_pair(key, value);
        }
        let answer = self.send(Ask { method: Method::GET, url, headers: HeaderMap::new(), body: None, conditional: false }).await?;
        if answer.status() == StatusCode::OK { Ok(answer) } else { Err(self.failed(answer, prefix, false).await) }
    }

    async fn put(&self, at: &Path, payload: PutPayload, mode: PutMode) -> object_store::Result<PutResult> {
        let mut headers = HeaderMap::new();
        let create = matches!(mode, PutMode::Create);
        let conditional = !matches!(mode, PutMode::Overwrite);
        match mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                headers.insert("if-none-match", HeaderValue::from_static("*"));
            }
            PutMode::Update(version) => {
                let Some(e_tag) = version.e_tag else { return Err(refuse(&self.at, "an update that names no e-tag")) };
                headers.insert("if-match", header_text(&self.at, &e_tag)?);
                if let Some(v) = version.version {
                    headers.insert("x-pondra-version", header_text(&self.at, &v)?);
                }
            }
        }
        let url = self.object(at)?;
        let answer = self.send(Ask { method: Method::PUT, url, headers, body: Some(payload), conditional }).await?;
        if answer.status() != StatusCode::OK {
            return Err(self.failed(answer, at.as_ref(), create).await);
        }
        Ok(PutResult {
            e_tag: text_of(answer.headers(), "etag"),
            version: text_of(answer.headers(), "x-pondra-version"),
            extensions: Default::default(),
        })
    }

    async fn delete(&self, at: &Path) -> object_store::Result<()> {
        let url = self.object(at)?;
        let answer = self.send(Ask { method: Method::DELETE, url, headers: HeaderMap::new(), body: None, conditional: false }).await?;
        if answer.status() == StatusCode::NO_CONTENT { Ok(()) } else { Err(self.failed(answer, at.as_ref(), false).await) }
    }

    async fn copy(&self, from: &Path, to: &Path, create: bool) -> object_store::Result<()> {
        let mut url = self.object(to)?;
        url.query_pairs_mut().append_pair("from", from.as_ref());
        let mut headers = HeaderMap::new();
        if create {
            headers.insert("if-none-match", HeaderValue::from_static("*"));
        }
        let answer = self.send(Ask { method: Method::POST, url, headers, body: None, conditional: create }).await?;
        if answer.status() == StatusCode::NO_CONTENT { Ok(()) } else { Err(self.failed(answer, to.as_ref(), create).await) }
    }
}

/// The error for an answer the door gave. Without the door's header it is another server's answer
/// (an older Pondra, a proxy, or the node's sign-in check, which says why in its body).
fn error(at: &str, status: StatusCode, headers: &HeaderMap, body: &str, path: &str, create: bool) -> E {
    if !headers.contains_key("x-pondra-store") {
        return generic(at, format!("answered {status} without a store door: an older Pondra, or not a node ({body})"));
    }
    let path = path.to_string();
    let source = |text: &str| -> Box<dyn std::error::Error + Send + Sync> { text.into() };
    match status.as_u16() {
        404 => E::NotFound { path, source: source(body) },
        412 if create => E::AlreadyExists { path, source: source(body) },
        412 => E::Precondition { path, source: source(body) },
        304 => E::NotModified { path, source: source(body) },
        403 => E::PermissionDenied { path, source: source(body) },
        401 => E::Unauthenticated { path, source: source(body) },
        501 => match headers.get("x-pondra-error").and_then(|v| v.to_str().ok()) {
            Some("not-supported") => E::NotSupported { source: source(body) },
            Some("not-implemented") => E::NotImplemented { operation: body.to_string(), implementer: format!("pondra://{at}") },
            _ => generic(at, format!("answered 501: {body}")),
        },
        _ => generic(at, format!("answered {status}: {body}")),
    }
}

/// A failure of this store that no variant of its own says: `pondra://host:port`, then what happened.
fn generic(at: &str, what: impl std::fmt::Display) -> E {
    E::Generic { store: "pondra", source: format!("pondra://{at} {what}").into() }
}

/// An operation the door doesn't carry: refused by name, never done some other way.
fn refuse(at: &str, operation: impl Into<String>) -> E {
    E::NotImplemented { operation: operation.into(), implementer: format!("pondra://{at}") }
}

/// Tags and attributes are refused before anything is sent: the door keeps neither (SlateDB tries
/// again without them when a store refuses them, so a refusal is the answer it expects).
fn refuse_tags(at: &str, tags: &TagSet, attributes: &Attributes) -> object_store::Result<()> {
    if tags.is_empty() && attributes.is_empty() { Ok(()) } else { Err(refuse(at, "a write with tags or attributes")) }
}

/// A header value for text this side was given: one no header can carry is refused.
fn header_text(at: &str, text: &str) -> object_store::Result<HeaderValue> {
    HeaderValue::from_str(text).map_err(|e| generic(at, format!("can't send {text:?} in a header: {e}")))
}

/// The query of a listing: its prefix and where it starts after.
fn listing_query(prefix: Option<&Path>, offset: Option<&Path>) -> Vec<(&'static str, String)> {
    prefix.map(|p| ("prefix", p.to_string())).into_iter().chain(offset.map(|o| ("offset", o.to_string()))).collect()
}

/// An object's metadata as the object headers carry it.
fn object_meta(headers: &HeaderMap, at: &Path, served: &str) -> object_store::Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: at.clone(),
        last_modified: DateTime::from_timestamp_nanos(number(headers, "x-pondra-modified-ns", served)?),
        size: number(headers, "x-pondra-size", served)?,
        e_tag: text_of(headers, "etag"),
        version: text_of(headers, "x-pondra-version"),
    })
}

/// A header's number, which the door always sends.
fn number<T: std::str::FromStr>(headers: &HeaderMap, name: &str, served: &str) -> object_store::Result<T> {
    text_of(headers, name).and_then(|t| t.parse().ok()).ok_or_else(|| generic(served, format!("answered without a usable {name}")))
}

/// The range of a partial answer, from its `content-range`.
fn content_span(headers: &HeaderMap, served: &str) -> object_store::Result<Range<u64>> {
    let text = text_of(headers, "content-range").ok_or_else(|| generic(served, "answered a partial read without its content-range"))?;
    byte_range::answered(&text).map(|(span, _)| span).ok_or_else(|| generic(served, format!("answered the content-range {text:?}, which isn't one")))
}

/// A body's chunks, each waited for `IDLE` at most. After a failure the stream is over.
fn chunks(body: BoxStream<'static, Result<Bytes, HttpError>>, at: String) -> impl Stream<Item = object_store::Result<Bytes>> + Send + 'static {
    futures::stream::unfold(Some(body), move |state| {
        let at = at.clone();
        async move {
            let mut body = state?;
            match tokio::time::timeout(IDLE, body.next()).await {
                Ok(Some(Ok(chunk))) => Some((Ok(chunk), Some(body))),
                Ok(Some(Err(e))) => Some((Err(generic(&at, format!("failed mid-body: {e}"))), None)),
                Ok(None) => None,
                Err(_) => Some((Err(generic(&at, format!("sent no bytes for {} s", IDLE.as_secs()))), None)),
            }
        }
    })
    .fuse() // (object_store's `bytes` polls an empty body once more after its end)
}

/// A listing's lines as objects. A line cut across chunks is put together again; a line that says the
/// store failed is that error, and the listing is over after it.
pub fn lines(chunks: impl Stream<Item = object_store::Result<Bytes>>, at: &str) -> impl Stream<Item = object_store::Result<ObjectMeta>> {
    let state = Lines { chunks: Box::pin(chunks), held: Vec::new(), at: at.to_string(), ended: false, done: false };
    futures::stream::unfold(state, next_line).fuse()
}

struct Lines<S> { chunks: Pin<Box<S>>, held: Vec<u8>, at: String, ended: bool, done: bool }

async fn next_line<S: Stream<Item = object_store::Result<Bytes>>>(mut st: Lines<S>) -> Option<(object_store::Result<ObjectMeta>, Lines<S>)> {
    loop {
        if st.done {
            return None;
        }
        if let Some(end) = st.held.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = st.held.drain(..=end).collect();
            if line.len() == 1 {
                continue; // (a blank line)
            }
            let item = object_of_line(&line[..line.len() - 1], &st.at);
            st.done = item.is_err();
            return Some((item, st));
        }
        if st.ended {
            if st.held.is_empty() {
                return None;
            }
            st.done = true;
            let text = format!("the listing from pondra://{} ended midway", st.at);
            return Some((Err(E::Generic { store: "pondra", source: text.into() }), st));
        }
        match st.chunks.next().await {
            Some(Ok(chunk)) => st.held.extend_from_slice(&chunk),
            Some(Err(e)) => {
                st.done = true;
                return Some((Err(e), st));
            }
            None => st.ended = true,
        }
    }
}

fn object_of_line(text: &[u8], at: &str) -> object_store::Result<ObjectMeta> {
    match serde_json::from_slice::<Line>(text) {
        Ok(Line::Object(meta)) => meta.meta(),
        Ok(Line::Failed(f)) => {
            let mut headers = HeaderMap::new(); // (a failure line comes from a door)
            headers.insert("x-pondra-store", HeaderValue::from_static("1"));
            let status = StatusCode::from_u16(f.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Err(error(at, status, &headers, &f.error, "", false))
        }
        Err(e) => Err(generic(at, format!("sent a listing line that isn't one: {e}"))),
    }
}

/// A multipart upload: its parts are kept until `complete`, which sends them as one PUT (the door's
/// only write of an object).
#[derive(Debug)]
struct Upload { link: Link, at: Path, parts: Vec<PutPayload> }

#[async_trait::async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.parts.push(data);
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let whole: PutPayload = self.parts.iter().flat_map(|p| p.iter().cloned()).collect();
        self.link.put(&self.at, whole, PutMode::Overwrite).await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.clear();
        Ok(())
    }
}

/// One path of `delete_stream`: deleted, and given back as it was asked for.
async fn delete_one(made: Result<Link, String>, served: String, at: object_store::Result<Path>) -> object_store::Result<Path> {
    let at = at?;
    made.map_err(|e| generic(&served, e))?.delete(&at).await?;
    Ok(at)
}

#[async_trait::async_trait]
impl ObjectStore for Served {
    async fn put_opts(&self, at: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        refuse_tags(&self.at, &opts.tags, &opts.attributes)?;
        self.link_of()?.put(at, payload, opts.mode).await
    }

    async fn put_multipart_opts(&self, at: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        refuse_tags(&self.at, &opts.tags, &opts.attributes)?;
        Ok(Box::new(Upload { link: self.link_of()?, at: at.clone(), parts: Vec::new() }))
    }

    async fn get_opts(&self, at: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if options.if_modified_since.is_some() || options.if_unmodified_since.is_some() || options.version.is_some() {
            return Err(refuse(&self.at, "a get with if_modified_since, if_unmodified_since or a version"));
        }
        let link = self.link_of()?;
        let mut headers = HeaderMap::new();
        if let Some(range) = &options.range {
            range.is_valid().map_err(|e| generic(&self.at, e))?;
            headers.insert("range", header_text(&self.at, &byte_range::header(range))?);
        }
        if let Some(e_tag) = &options.if_match {
            headers.insert("if-match", header_text(&self.at, e_tag)?);
        }
        if let Some(e_tag) = &options.if_none_match {
            headers.insert("if-none-match", header_text(&self.at, e_tag)?);
        }
        let method = if options.head { Method::HEAD } else { Method::GET };
        let url = link.object(at)?;
        let answer = link.send(Ask { method, url, headers, body: None, conditional: false }).await?;
        if !answer.status().is_success() {
            return Err(link.failed(answer, &at.to_string(), false).await);
        }
        let meta = object_meta(answer.headers(), at, &self.at)?;
        let range = match answer.status() {
            StatusCode::PARTIAL_CONTENT => content_span(answer.headers(), &self.at)?,
            _ => 0..meta.size,
        };
        let body = chunks(answer.into_body().bytes_stream(), self.at.clone());
        Ok(GetResult {
            payload: GetResultPayload::Stream(body.boxed()),
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }

    fn delete_stream(&self, at: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        let (made, served) = (self.link(), self.at.clone());
        at.map(move |p| delete_one(made.clone(), served.clone(), p)).buffered(8).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> { self.list_from(prefix, None) }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_from(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let link = self.link_of()?;
        let mut query = listing_query(prefix, None);
        query.push(("delimiter", "1".to_string()));
        let answer = link.listing(&query, &prefix.map(Path::to_string).unwrap_or_default()).await?;
        let mut body = Box::pin(chunks(answer.into_body().bytes_stream(), self.at.clone()));
        let mut text = Vec::new();
        while let Some(chunk) = body.next().await {
            text.extend_from_slice(&chunk?);
        }
        let listed: Delimited = serde_json::from_slice(&text).map_err(|e| generic(&self.at, format!("sent a listing that isn't JSON: {e}")))?;
        Ok(ListResult {
            common_prefixes: listed.prefixes.iter().map(|p| Path::parse(p)).collect::<Result<_, _>>()?,
            objects: listed.objects.into_iter().map(Meta::meta).collect::<object_store::Result<_>>()?,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.link_of()?.copy(from, to, matches!(options.mode, CopyMode::Create)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::Attribute;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::sync::Arc;

    /// Each refusal of `checked` says its reason, and the cases it lets through pass.
    #[test]
    fn a_node_starts_on_a_store_only_as_its_flags_allow() {
        let why = |dir: &str, reader: bool, admin: bool, serving: bool| checked(dir, reader, admin, serving).err().map(|e| e.to_string()).unwrap_or_default();
        assert!(why("/lake", false, true, true).is_empty(), "serving a folder with the admin token");
        assert!(why("/lake", false, false, false).is_empty(), "a folder, served by no one");
        assert!(why("memory://m", false, true, true).is_empty(), "serving a lake in memory");
        assert!(why("pondra://127.0.0.1:1", false, true, false).is_empty(), "a node of a served lake, with the admin token");
        assert!(why("/lake", true, true, true).contains("a --reader never leads"));
        assert!(why("s3://b/x", false, true, true).contains("reached by its own URL"));
        assert!(why("/lake", false, false, true).contains("needs --admin-token"));
        assert!(why("pondra://127.0.0.1:1", false, false, false).contains("admin token its holder has"));
    }

    /// Serve `app` on a port of this machine.
    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("127.0.0.1:{port}")
    }

    /// The store's door, mounted as the node mounts it.
    async fn serve_store(store: Store) -> String { serve(Router::new().nest_service("/cluster/store", door(store))).await }

    /// The variant of an error: what the two sides must agree on.
    fn variant(e: &E) -> &'static str {
        match e {
            E::NotFound { .. } => "NotFound",
            E::AlreadyExists { .. } => "AlreadyExists",
            E::Precondition { .. } => "Precondition",
            E::NotModified { .. } => "NotModified",
            E::NotImplemented { .. } => "NotImplemented",
            E::NotSupported { .. } => "NotSupported",
            E::PermissionDenied { .. } => "PermissionDenied",
            E::Unauthenticated { .. } => "Unauthenticated",
            E::InvalidPath { .. } => "InvalidPath",
            E::Generic { .. } => "Generic",
            _ => "other",
        }
    }

    fn outcome<T>(r: object_store::Result<T>) -> Result<(), &'static str> { r.map(|_| ()).map_err(|e| variant(&e)) }

    /// A read as the test compares it: the metadata, the range and the bytes (none for a HEAD), or
    /// the failure's variant.
    type Read = Result<(ObjectMeta, Range<u64>, Vec<u8>), &'static str>;

    async fn read(r: object_store::Result<GetResult>, head: bool) -> Read {
        let got = r.map_err(|e| variant(&e))?;
        let (meta, span) = (got.meta.clone(), got.range.clone());
        let bytes = if head { Vec::new() } else { got.bytes().await.map_err(|e| variant(&e))?.to_vec() };
        Ok((meta, span, bytes))
    }

    /// Read `at` through the `Served` and on the store, and say they agree.
    async fn both_reads(served: &Served, store: &Store, at: &Path, opts: GetOptions, case: &str) {
        let head = opts.head;
        let a = read(served.get_opts(at, opts.clone()).await, head).await;
        let b = read(store.get_opts(at, opts).await, head).await;
        assert_eq!(a, b, "get {case}");
    }

    fn ranged(r: GetRange) -> GetOptions { GetOptions { range: Some(r), ..Default::default() } }

    /// A listing, sorted by path (a store's own order is its business).
    async fn listed(s: BoxStream<'static, object_store::Result<ObjectMeta>>) -> Result<Vec<ObjectMeta>, &'static str> {
        let mut all: Vec<ObjectMeta> = s.try_collect().await.map_err(|e| variant(&e))?;
        all.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
        Ok(all)
    }

    /// A delimited listing, both sorted.
    async fn delimited(r: object_store::Result<ListResult>) -> Result<(Vec<ObjectMeta>, Vec<String>), &'static str> {
        let r = r.map_err(|e| variant(&e))?;
        let mut objects = r.objects;
        objects.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
        let mut prefixes: Vec<String> = r.common_prefixes.iter().map(Path::to_string).collect();
        prefixes.sort();
        Ok((objects, prefixes))
    }

    /// The whole checklist against one store, once through the door and once on the store itself.
    /// `updates`: the store changes an object in place (a memory store does; a local disk refuses).
    async fn check(store: Store, updates: bool) {
        let at = serve_store(store.clone()).await;
        let served = Served::new(&at);
        let path = Path::from("k/one");

        // put, and what the put answers is what the store has
        let put = served.put(&path, PutPayload::from_static(b"hello")).await.expect("put through the Served");
        let head = store.head(&path).await.expect("the store has the object");
        assert_eq!((&put.e_tag, &put.version), (&head.e_tag, &head.version), "put: the PutResult is the store's");
        let e_tag = head.e_tag.clone().expect("an e-tag");

        // gets: the whole object, every kind of range, a range past the end, Suffix(0), HEAD
        for (case, opts) in [
            ("whole", GetOptions::default()),
            ("2..5", ranged(GetRange::Bounded(2..5))),
            ("0..5, ending on the last byte", ranged(GetRange::Bounded(0..5))),
            ("an offset", ranged(GetRange::Offset(2))),
            ("a suffix", ranged(GetRange::Suffix(3))),
            ("a range past the end", ranged(GetRange::Bounded(3..100))),
            ("suffix 0", ranged(GetRange::Suffix(0))),
            ("head", GetOptions { head: true, ..Default::default() }),
        ] {
            both_reads(&served, &store, &path, opts, case).await;
        }

        // an empty object, whole and with a suffix
        let empty = Path::from("k/empty");
        served.put(&empty, PutPayload::default()).await.expect("put an empty object");
        both_reads(&served, &store, &empty, GetOptions::default(), "an empty object, whole").await;
        both_reads(&served, &store, &empty, ranged(GetRange::Suffix(5)), "an empty object, suffix 5").await;

        // a 9 MiB object: streamed, whole and by a range
        let big = Path::from("k/big");
        let bytes: Vec<u8> = (0..9 * MIB).map(|i| (i % 251) as u8).collect();
        served.put(&big, PutPayload::from(bytes)).await.expect("put 9 MiB");
        both_reads(&served, &store, &big, GetOptions::default(), "9 MiB, whole").await;
        both_reads(&served, &store, &big, ranged(GetRange::Bounded(1..9_000_000)), "9 MiB, 1..9000000").await;

        // conditional gets, and a missing path
        both_reads(&served, &store, &path, GetOptions { if_none_match: Some(e_tag.clone()), ..Default::default() }, "if_none_match its e-tag").await;
        both_reads(&served, &store, &path, GetOptions { if_match: Some("wrong-etag".into()), ..Default::default() }, "if_match a wrong e-tag").await;
        both_reads(&served, &store, &path, GetOptions { if_match: Some(e_tag.clone()), ..Default::default() }, "if_match the e-tag").await;
        both_reads(&served, &store, &Path::from("k/missing"), GetOptions::default(), "a missing path").await;

        // creates: the second is AlreadyExists on both sides
        let fresh = Path::from("k/fresh");
        let create = || PutOptions { mode: PutMode::Create, ..Default::default() };
        served.put_opts(&fresh, PutPayload::from_static(b"a"), create()).await.expect("the first create");
        assert_eq!(
            outcome(served.put_opts(&fresh, PutPayload::from_static(b"b"), create()).await),
            outcome(store.put_opts(&fresh, PutPayload::from_static(b"b"), create()).await),
            "create twice"
        );

        // updates: a wrong e-tag is refused; the right one changes the bytes where the store allows it
        let update = |e_tag: &str| PutOptions { mode: PutMode::Update(UpdateVersion { e_tag: Some(e_tag.into()), version: None }), ..Default::default() };
        assert_eq!(
            outcome(served.put_opts(&path, PutPayload::from_static(b"nope"), update("wrong-etag")).await),
            outcome(store.put_opts(&path, PutPayload::from_static(b"nope"), update("wrong-etag")).await),
            "update with a wrong e-tag"
        );
        if updates {
            served.put_opts(&path, PutPayload::from_static(b"changed"), update(&e_tag)).await.expect("update with the right e-tag");
            both_reads(&served, &store, &path, GetOptions::default(), "after an update").await;
        } else {
            assert_eq!(
                outcome(served.put_opts(&path, PutPayload::from_static(b"changed"), update(&e_tag)).await),
                Err("NotImplemented"),
                "an update the store refuses by name"
            );
        }

        // a path with every awkward character: put, get, list, copy and delete under one Path
        let awkward = Path::from("files/a b#%é?.txt");
        served.put(&awkward, PutPayload::from_static(b"awkward")).await.expect("put an awkward path");
        both_reads(&served, &store, &awkward, GetOptions::default(), "an awkward path").await;
        let files = Path::from("files");
        assert_eq!(listed(served.list(Some(&files))).await, listed(store.list(Some(&files))).await, "list an awkward path");
        let copied = Path::from("files/copy b#%é?.txt");
        served.copy_opts(&awkward, &copied, CopyOptions::default()).await.expect("copy an awkward path");
        both_reads(&served, &store, &copied, GetOptions::default(), "a copy of an awkward path").await;
        served.delete(&awkward).await.expect("delete an awkward path");
        both_reads(&served, &store, &awkward, GetOptions::default(), "a deleted awkward path").await;

        // copies: Create onto an existing path is AlreadyExists; a copy reads back its bytes
        let k_copy = Path::from("k/copy");
        assert_eq!(
            outcome(served.copy_opts(&path, &path, CopyOptions { mode: CopyMode::Create, ..Default::default() }).await),
            outcome(store.copy_opts(&path, &path, CopyOptions { mode: CopyMode::Create, ..Default::default() }).await),
            "copy Create onto an existing path"
        );
        served.copy_opts(&path, &k_copy, CopyOptions::default()).await.expect("copy");
        both_reads(&served, &store, &k_copy, GetOptions::default(), "a copy").await;

        // deletes: the path is gone, and a delete of a missing path is Ok
        served.delete(&k_copy).await.expect("delete");
        both_reads(&served, &store, &k_copy, GetOptions::default(), "after a delete").await;
        served.delete(&Path::from("k/never")).await.expect("a delete of a missing path is Ok");

        // a multipart upload of three parts reads back as their concatenation
        let multi = Path::from("k/multi");
        let mut upload = served.put_multipart_opts(&multi, PutMultipartOptions::default()).await.expect("start an upload");
        for part in ["one-", "two-", "three"] {
            upload.put_part(PutPayload::from_static(part.as_bytes())).await.expect("a part");
        }
        upload.complete().await.expect("complete");
        both_reads(&served, &store, &multi, GetOptions::default(), "multipart").await;

        // what the door doesn't carry is refused by name, before anything is sent
        let with_attribute = PutOptions { attributes: [(Attribute::ContentType, "text/plain")].into_iter().collect(), ..Default::default() };
        assert_eq!(outcome(served.put_opts(&fresh, PutPayload::from_static(b"x"), with_attribute).await), Err("NotImplemented"), "an attribute");
        let since = GetOptions { if_modified_since: Some(DateTime::from_timestamp_nanos(0)), ..Default::default() };
        assert_eq!(outcome(served.get_opts(&path, since).await), Err("NotImplemented"), "if_modified_since");

        // listings: 2,500 objects under p/ and one under p/q/
        for i in 0..2_500 {
            served.put(&Path::from(format!("p/obj-{i:05}")), PutPayload::from_static(b"x")).await.expect("put");
        }
        served.put(&Path::from("p/q/x"), PutPayload::from_static(b"x")).await.expect("put");
        let p = Path::from("p");
        let all = listed(store.list(Some(&p))).await;
        assert_eq!(all.as_ref().map(Vec::len), Ok(2501), "the store lists 2,501");
        assert_eq!(listed(served.list(Some(&p))).await, all, "list p");
        let offset = all.as_ref().expect("a listing")[999].location.clone();
        assert_eq!(
            listed(served.list_with_offset(Some(&p), &offset)).await,
            listed(store.list_with_offset(Some(&p), &offset)).await,
            "list_with_offset from the 1,000th"
        );
        let split = delimited(served.list_with_delimiter(Some(&p)).await).await;
        assert_eq!(split, delimited(store.list_with_delimiter(Some(&p)).await).await, "list_with_delimiter p");
        let (objects, prefixes) = split.expect("a delimited listing");
        assert_eq!((objects.len(), prefixes), (2500, vec!["p/q".to_string()]), "p/'s objects and prefixes");

        // the awkward answers: a copy's and a listing's errors and variants agree (above); a Range the
        // door can't read is a 400 with the door's header, from a client that never sends it
        let raw = reqwest::Client::new().get(format!("http://{at}/cluster/store/k/one")).header("range", "bytes=x").send().await.expect("a request");
        assert_eq!(
            (raw.status().as_u16(), raw.headers().get("x-pondra-store").and_then(|v| v.to_str().ok()).map(str::to_string)),
            (400, Some("1".to_string())),
            "a Range the door can't read"
        );

        // a node without a door answers Generic, naming the door
        let plain = serve(Router::new()).await;
        let err = Served::new(&plain).get(&path).await.expect_err("no door, no object");
        assert!(matches!(err, E::Generic { .. }) && err.to_string().contains("without a store door"), "no door: {err}");
    }

    /// Retries, in front of the door: 503 to the first requests it sees, then the door's answers.
    async fn check_retries(store: Store) {
        let fails = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(AtomicUsize::new(0));
        let (f, s) = (fails.clone(), seen.clone());
        let flaky = Router::new()
            .nest_service("/cluster/store", door(store.clone()))
            .layer(middleware::from_fn(move |req: axum::extract::Request, next: middleware::Next| {
                let (f, s) = (f.clone(), s.clone());
                async move {
                    s.fetch_add(1, SeqCst);
                    if f.fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1)).is_ok() {
                        return StatusCode::SERVICE_UNAVAILABLE.into_response();
                    }
                    next.run(req).await
                }
            }));
        let via = Served::new(&serve(flaky).await);
        store.put(&Path::from("k/one"), PutPayload::from_static(b"hello")).await.expect("put on the store");

        fails.store(2, SeqCst);
        seen.store(0, SeqCst);
        via.get(&Path::from("k/one")).await.expect("a get answered 503 twice is sent again");
        assert_eq!(seen.load(SeqCst), 3, "a get is tried 3 times");

        fails.store(1, SeqCst);
        seen.store(0, SeqCst);
        let refused = Path::from("k/refused");
        let err = via
            .put_opts(&refused, PutPayload::from_static(b"z"), PutOptions { mode: PutMode::Create, ..Default::default() })
            .await
            .expect_err("a create answered 503 is an error at once");
        assert_eq!((variant(&err), seen.load(SeqCst)), ("Generic", 1), "a conditional write is sent once");
        assert!(matches!(store.head(&refused).await, Err(E::NotFound { .. })), "the store holds nothing");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_served_store_answers_as_the_store_does() {
        let memory: Store = Arc::new(InMemory::new());
        check(memory.clone(), true).await;
        check_retries(memory).await;

        let dir = std::env::temp_dir().join(format!("pondra-served-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let disk: Store = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        check(disk, false).await;
        std::fs::remove_dir_all(&dir).ok();

        // (a lake's folder as `open_store` opens it: it takes the conditional overwrites)
        let dir = std::env::temp_dir().join(format!("pondra-served-folder-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let folder: Store = Arc::new(crate::store::Local(LocalFileSystem::new_with_prefix(&dir).unwrap()));
        check(folder, true).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_listing_comes_apart_and_together_across_chunks() {
        let one = serde_json::to_string(&Meta { path: "a/one".into(), size: 3, modified_ns: 1_700_000_000_000_000_001, e_tag: Some("e1".into()), version: None }).unwrap();
        let two = serde_json::to_string(&Meta { path: "a/two".into(), size: 4, modified_ns: 0, e_tag: None, version: Some("v2".into()) }).unwrap();
        let text = format!("{one}\n{two}\n");
        let chunks: Vec<object_store::Result<Bytes>> = text.as_bytes().chunks(7).map(|c| Ok(Bytes::copy_from_slice(c))).collect();
        let got: Vec<ObjectMeta> = lines(futures::stream::iter(chunks), "h:1").try_collect().await.unwrap();
        assert_eq!(got.iter().map(|m| m.location.to_string()).collect::<Vec<_>>(), ["a/one", "a/two"]);
        assert_eq!((got[0].size, got[0].e_tag.as_deref(), got[1].version.as_deref()), (3, Some("e1"), Some("v2")));
        assert_eq!(got[0].last_modified, DateTime::from_timestamp_nanos(1_700_000_000_000_000_001), "times come back exactly");

        let failed: Vec<object_store::Result<Bytes>> = vec![Ok(Bytes::from_static(b"{\"error\":\"gone\",\"status\":404}\n"))];
        let err = lines(futures::stream::iter(failed), "h:1").try_collect::<Vec<_>>().await.unwrap_err();
        assert!(matches!(err, E::NotFound { .. }), "a failure line is the error it carries: {err}");

        let midway: Vec<object_store::Result<Bytes>> = vec![Ok(Bytes::from_static(b"{\"path\":\"a\""))];
        let err = lines(futures::stream::iter(midway), "h:1").try_collect::<Vec<_>>().await.unwrap_err();
        assert!(matches!(err, E::Generic { .. }) && err.to_string().contains("ended midway"), "a body ending midway: {err}");
    }
}
