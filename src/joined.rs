//! A joined lake's store: the lake's files and log, read through the node a page joined (ADR-072 §8; the page joins a
//! cluster, J4). The node serves them at `/store` to an admin's sign-in (`served::files_door`, which passes only what
//! `store::lake_file` names), and this reads them there with the page's `Authorization`: no bucket credential reaches the
//! page, and nothing is written here (a write goes to the node's `/sql`). Its requests go through `fetch`, so it builds for
//! a page as well as a node.

use crate::byte_range;
use crate::fetch;
use crate::store::lake_file;
use crate::store_wire::{chunks, content_span, folder_of_lake, generic, listing_query, lines, object_meta, text_of, Delimited, Meta};
use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error as E, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use reqwest::header::{AUTHORIZATION, RANGE};
use std::fmt;
use std::future::Future;
use std::time::Duration;
use url::Url;

/// How long a request waits for its answer's headers. A body is read as it comes, each chunk within the time `store_wire`
/// allows it.
const WAIT: Duration = Duration::from_secs(30);

/// A create-only PUT waits `WAIT` plus a second for each megabyte of its body: a 128 MB file can take longer than `WAIT`.
const MB: usize = 1 << 20;

/// A lake's store on the node at `node` (its URL, such as `https://node:7000`), read with `auth`, the `Authorization` header
/// of every request (a page's sign-in). Nothing is connected until the first request.
#[derive(Clone)]
pub struct Joined {
    node: String,
    auth: String,
    http: reqwest::Client,
}

impl Joined {
    pub fn new(node: &str, auth: &str) -> Joined {
        // (`bare`: no token of its own and no time limit, so a whole file streams through it; the sign-in is this store's own)
        Joined { node: node.to_string(), auth: auth.to_string(), http: crate::ext::bare().clone() }
    }

    /// The node's address as a message names it, `host:port` with no scheme, as `Served` names its node.
    fn at(&self) -> &str {
        let rest = self.node.split_once("://").map_or(self.node.as_str(), |(_, rest)| rest);
        rest.trim_end_matches('/')
    }

    /// The node's `/store`: the door's root, under which the lake's objects and listings are asked for.
    fn store_url(&self) -> StoreResult<Url> {
        let mut url = Url::parse(&self.node).map_err(|e| generic(self.at(), e))?;
        url.path_segments_mut().map_err(|()| generic(self.at(), "has no path to add /store to"))?.push("store");
        Ok(url)
    }

    /// The URL of the object at `at`: the node's `/store`, then each part of its path, percent-encoded on its own (as `Served`).
    fn object_url(&self, at: &Path) -> StoreResult<Url> {
        let mut url = self.store_url()?;
        url.path_segments_mut().map_err(|()| generic(self.at(), "has no path to add an object to"))?.extend(at.parts());
        Ok(url)
    }

    fn request(&self, method: reqwest::Method, url: Url) -> reqwest::RequestBuilder {
        self.http.request(method, url).header(AUTHORIZATION, self.auth.as_str())
    }

    /// `req`'s answer, waited for `WAIT` at most. The send starts here, so the request is not held across an await.
    fn sent(&self, req: reqwest::RequestBuilder, what: String) -> impl Future<Output = StoreResult<fetch::Response>> + Send {
        self.sent_within(req, what, WAIT)
    }

    /// The error for an answer that isn't a success: a 404 is `NotFound` and a 403 `PermissionDenied`, as the door says them,
    /// and a 503 a generic `Busy` (sent again: `write::stage_put`); any other is generic.
    async fn failed(&self, answer: fetch::Response, path: &str) -> E {
        let status = answer.status();
        let body = answer.text().await.unwrap_or_default();
        match status.as_u16() {
            401 => E::Unauthenticated { path: path.to_string(), source: format!("{status}: {body}").into() },
            404 => E::NotFound { path: path.to_string(), source: format!("{status}: {body}").into() },
            403 => E::PermissionDenied { path: path.to_string(), source: format!("{status}: {body}").into() },
            503 => E::Generic { store: "pondra", source: Box::new(crate::store_wire::Busy(format!("pondra://{} answered {status}: {body}", self.at()))) },
            _ => generic(self.at(), format!("answered {status}: {body}")),
        }
    }

    /// `req`'s answer, waited for `wait` at most (`sent`, with a longer wait for a body that is long to send).
    fn sent_within(&self, req: reqwest::RequestBuilder, what: String, wait: Duration) -> impl Future<Output = StoreResult<fetch::Response>> + Send {
        let answer = fetch::send(req);
        let at = self.at().to_string();
        async move {
            match tokio::time::timeout(wait, answer).await {
                Ok(Ok(answer)) => Ok(answer),
                Ok(Err(e)) => Err(generic(&at, format!("{what} failed: {e}"))),
                Err(_) => Err(generic(&at, format!("{what}: no answer in {} s", wait.as_secs()))),
            }
        }
    }

    /// The answer to a listing of `prefix`: the node's `/store` with `query` (the prefix, where it starts after, or `delimiter`).
    async fn listing(&self, query: &[(&str, String)], prefix: &str) -> StoreResult<fetch::Response> {
        let mut url = self.store_url()?;
        for (key, value) in query {
            url.query_pairs_mut().append_pair(key, value);
        }
        let answer = self.sent(self.request(reqwest::Method::GET, url), format!("GET {prefix}")).await?;
        if answer.status() == reqwest::StatusCode::OK { Ok(answer) } else { Err(self.failed(answer, prefix).await) }
    }

    /// The objects under `prefix` from `offset` on, one listing line each. A prefix that is not a lake folder lists
    /// nothing, with no request.
    fn list_from(&self, prefix: Option<&Path>, offset: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        let prefix_text = prefix.map(Path::to_string).unwrap_or_default();
        if !lake_folder(prefix) {
            return futures::stream::empty().boxed(); // (nothing a page may read is there: the catalog's open lists `catalog/manifest` only to say why it failed)
        }
        let (me, query) = (self.clone(), listing_query(prefix, offset));
        futures::stream::once(async move {
            let answer = me.listing(&query, &prefix_text).await?;
            Ok::<_, E>(lines(chunks(answer.bytes_stream().boxed(), me.at().to_string()), me.at()))
        })
        .try_flatten()
        .boxed()
    }
}

/// Whether a listing's prefix is one of the lake's folders, as the door checks it. The whole store is none.
fn lake_folder(prefix: Option<&Path>) -> bool {
    prefix.is_some_and(|p| folder_of_lake(p.as_ref()))
}

/// What a page doesn't read: the lake's files and log only (`data/`, `log/`). It is `NotFound` with no request sent.
fn not_here(path: &str) -> E {
    E::NotFound { path: path.to_string(), source: "a joined page reads the lake's files and log (data/, log/) through its node, and no other path".into() }
}

/// A write other than a create: a joined page's files are new ones only (`PutMode::Create`), and its rows go through its node's
/// commit. Multipart, copy and delete are the node's to refuse.
fn writes() -> E {
    E::NotSupported { source: "a joined page writes only new files of the lake (data/, log/), each once, through its node".into() }
}

/// A payload's bytes as one body: a single part as it is, several put together (one copy).
fn body_of(payload: PutPayload) -> Bytes {
    let mut parts: Vec<Bytes> = payload.into_iter().collect();
    if parts.len() == 1 {
        return parts.remove(0);
    }
    let mut all = BytesMut::with_capacity(parts.iter().map(Bytes::len).sum());
    for part in parts {
        all.extend_from_slice(&part);
    }
    all.freeze()
}

impl fmt::Debug for Joined {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // (the sign-in is not shown: it is the page's session)
        f.debug_struct("Joined").field("node", &self.node).finish_non_exhaustive()
    }
}

impl fmt::Display for Joined {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "joined pondra://{}", self.at())
    }
}

#[async_trait::async_trait]
impl ObjectStore for Joined {
    async fn put_opts(&self, at: &Path, payload: PutPayload, opts: PutOptions) -> StoreResult<PutResult> {
        // (a new file only: the node's door takes `PUT /store/…` with `If-None-Match: *` for the page's own staged rows, and
        // refuses any other put, so nothing here overwrites a file)
        if !matches!(opts.mode, PutMode::Create) {
            return Err(writes());
        }
        if !lake_file(at.as_ref()) && !at.as_ref().starts_with("stage/") {
            return Err(not_here(at.as_ref())); // (a page's staged rows, `stage/`: the node takes them only from its own user, `auth::page_creates`)
        }
        let body = body_of(payload);
        let wait = WAIT + Duration::from_secs(body.len().div_ceil(MB) as u64);
        let req = self.request(reqwest::Method::PUT, self.object_url(at)?).header("if-none-match", "*").body(body);
        let answer = self.sent_within(req, format!("PUT {at}"), wait).await?;
        if answer.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            let body = answer.text().await.unwrap_or_default();
            return Err(E::AlreadyExists { path: at.to_string(), source: format!("412 Precondition Failed: {body}").into() });
        }
        if !answer.status().is_success() {
            return Err(self.failed(answer, at.as_ref()).await);
        }
        Ok(PutResult { e_tag: text_of(answer.headers(), "etag"), version: text_of(answer.headers(), "x-pondra-version"), extensions: Default::default() })
    }

    async fn put_multipart_opts(&self, _at: &Path, _opts: PutMultipartOptions) -> StoreResult<Box<dyn MultipartUpload>> {
        Err(writes())
    }

    async fn get_opts(&self, at: &Path, options: GetOptions) -> StoreResult<GetResult> {
        // (a file a page doesn't read is not there: the lake's open asks for its catalog and a move's mark, and gets no request)
        if !lake_file(at.as_ref()) {
            return Err(not_here(at.as_ref()));
        }
        // (a joined page's reads ask for no condition and no version: one that does is refused, not read without it)
        if options.if_match.is_some() || options.if_none_match.is_some() || options.if_modified_since.is_some() || options.if_unmodified_since.is_some() || options.version.is_some() {
            return Err(E::NotImplemented { operation: "a get with a condition or a version".into(), implementer: format!("pondra://{}", self.at()) });
        }
        let method = if options.head { reqwest::Method::HEAD } else { reqwest::Method::GET };
        let mut req = self.request(method.clone(), self.object_url(at)?);
        if let Some(range) = &options.range {
            range.is_valid().map_err(|e| generic(self.at(), e))?;
            req = req.header(RANGE, byte_range::header(range));
        }
        let answer = self.sent(req, format!("{method} {at}")).await?;
        if !answer.status().is_success() {
            return Err(self.failed(answer, at.as_ref()).await);
        }
        let meta = object_meta(answer.headers(), at, self.at())?;
        let range = match answer.status() {
            reqwest::StatusCode::PARTIAL_CONTENT => content_span(answer.headers(), self.at())?,
            _ => 0..meta.size,
        };
        let body = chunks(answer.bytes_stream().boxed(), self.at().to_string());
        Ok(GetResult {
            payload: GetResultPayload::Stream(body.boxed()),
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }

    fn delete_stream(&self, at: BoxStream<'static, StoreResult<Path>>) -> BoxStream<'static, StoreResult<Path>> {
        at.map(|_| Err::<Path, E>(writes())).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.list_from(prefix, None)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.list_from(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        let prefix_text = prefix.map(Path::to_string).unwrap_or_default();
        if !lake_folder(prefix) {
            return Ok(ListResult { common_prefixes: vec![], objects: vec![], extensions: Default::default() }); // (as `list_from`: nothing a page may read)
        }
        let mut query = listing_query(prefix, None);
        query.push(("delimiter", "1".to_string()));
        let answer = self.listing(&query, &prefix_text).await?;
        let mut body = Box::pin(chunks(answer.bytes_stream().boxed(), self.at().to_string()));
        let mut text = Vec::new();
        while let Some(chunk) = body.next().await {
            text.extend_from_slice(&chunk?);
        }
        let listed: Delimited = serde_json::from_slice(&text).map_err(|e| generic(self.at(), format!("sent a listing that isn't JSON: {e}")))?;
        Ok(ListResult {
            common_prefixes: listed.prefixes.iter().map(|p| Path::parse(p)).collect::<Result<_, _>>()?,
            objects: listed.objects.into_iter().map(Meta::meta).collect::<StoreResult<_>>()?,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> StoreResult<()> {
        Err(writes())
    }
}
