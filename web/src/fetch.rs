//! A web origin's objects, read with `fetch` (the page's store for one origin, designs/l2-b4-web.md section 4i): a `GET`
//! with a `Range`, and a `HEAD`. A web server lists and writes nothing, so those are refused by name. Options other than
//! `head` and `range` (`if_match`, `version`, …) are not sent: DataFusion asks for none of them.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::channel::oneshot;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use std::fmt;
use std::future::Future;
use std::ops::Range;
use url::Url;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{spawn_local, JsFuture};

/// A web origin's objects. The origin is kept as parsed, or as the error that says why it isn't a URL: the core makes
/// the store from a name in SQL, and a bad name must answer each read with its error, never trap the page.
#[derive(Debug)]
pub struct Fetch {
    origin: Result<Url, String>,
}

impl Fetch {
    pub fn new(origin: &str) -> Fetch {
        Fetch { origin: Url::parse(origin).map_err(|e| format!("{origin} is not an origin: {e}")) }
    }

    /// The object's URL: the origin, then the object's path segments (as object_store's own HTTP store makes it).
    fn url_of(&self, location: &Path) -> StoreResult<Url> {
        let origin = self.origin.as_ref().map_err(|e| generic(e.clone()))?;
        let mut url = origin.clone();
        url.path_segments_mut()
            .map_err(|()| generic(format!("{origin} can't hold a path")))?
            .extend(location.parts());
        Ok(url)
    }
}

impl fmt::Display for Fetch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.origin {
            Ok(origin) => write!(f, "Fetch({origin})"),
            Err(text) => write!(f, "Fetch({text})"),
        }
    }
}

#[async_trait]
impl ObjectStore for Fetch {
    async fn put_opts(&self, _location: &Path, _payload: PutPayload, _opts: PutOptions) -> StoreResult<PutResult> {
        Err(writes())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        Err(writes())
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let url = self.url_of(location)?;
        // An empty range has no bytes to ask for, so it is answered as a HEAD is: the object's meta, and no body.
        let empty = match &options.range {
            Some(GetRange::Bounded(r)) if r.start == r.end => Some(r.start),
            _ => None,
        };
        let head = options.head || empty.is_some();
        // A HEAD asks for no range: its meta is the whole object's.
        let wanted = if head { None } else { options.range };
        let method = if head { "HEAD" } else { "GET" };
        let answer = sent(fetched(url.to_string(), method, wanted.as_ref().map(range_header)))
            .await
            .map_err(|_| dropped(&url))?
            .map_err(|e| transport(&url, &e))?;
        let mut object = object_from(location, &url, answer, wanted.as_ref(), head)?;
        if let Some(start) = empty {
            object.range = start..start;
        }
        let Object { meta, range, body } = object;
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(body) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        locations.map(|_| Err(writes())).boxed()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        stream::once(async { Err(listing()) }).boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> StoreResult<ListResult> {
        Err(listing())
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> StoreResult<()> {
        Err(writes())
    }
}

fn writes() -> Error {
    Error::NotSupported { source: "a page reads the web: it writes nothing there".into() }
}

fn listing() -> Error {
    Error::NotSupported { source: "a web server lists nothing".into() }
}

fn generic(text: String) -> Error {
    Error::Generic { store: "web", source: text.into() }
}

/// A request that failed: the words the core's codes read (`codes.rs` maps "error sending request" to 58030).
fn transport(url: &Url, e: &str) -> Error {
    generic(format!("error sending request for url ({url}): {e}"))
}

fn dropped(url: &Url) -> Error {
    generic(format!("error sending request for url ({url}): the fetch was dropped"))
}

/// Runs `work` as a task on the page's thread and gives its result back as a `Send` future: `fetch`'s futures are not
/// `Send`, and an `ObjectStore`'s must be.
fn sent<T: Send + 'static>(
    work: impl Future<Output = T> + 'static,
) -> impl Future<Output = Result<T, oneshot::Canceled>> + Send {
    let (tx, rx) = oneshot::channel();
    spawn_local(async move {
        let _ = tx.send(work.await);
    });
    rx
}

/// A range as the `Range` header asks for it (a bounded range's last byte is `end - 1`).
fn range_header(range: &GetRange) -> String {
    match range {
        GetRange::Bounded(r) => format!("bytes={}-{}", r.start, r.end.saturating_sub(1)),
        GetRange::Offset(from) => format!("bytes={from}-"),
        GetRange::Suffix(last) => format!("bytes=-{last}"),
    }
}

/// What one fetch answered: the status, the headers the store reads, and the body (only a GET has one).
struct Fetched {
    status: u16,
    content_length: Option<String>,
    content_range: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    body: Vec<u8>,
}

/// An object as an answer describes it: its meta, the range its body covers, and the body.
struct Object {
    meta: ObjectMeta,
    range: Range<u64>,
    body: Bytes,
}

/// One request: `fetch(url, {method, headers: {Range}})`, its answer read to the end. Not `Send`: `sent` runs it.
async fn fetched(url: String, method: &'static str, range: Option<String>) -> Result<Fetched, String> {
    let response = JsFuture::from(send(&url, method, range)?).await.map_err(js_words)?;
    let headers = get(&response, "headers")?;
    Ok(Fetched {
        status: status(&response)?,
        content_length: header(&headers, "content-length")?,
        content_range: header(&headers, "content-range")?,
        etag: header(&headers, "etag")?,
        last_modified: header(&headers, "last-modified")?,
        body: if method == "GET" { body(&response).await? } else { Vec::new() },
    })
}

/// The request's promise, from the page's global `fetch` (called with the global as `this`, as a browser needs).
fn send(url: &str, method: &str, range: Option<String>) -> Result<js_sys::Promise, String> {
    let init = JsValue::from(js_sys::Object::new());
    set(&init, "method", method)?;
    if let Some(range) = range {
        let headers = JsValue::from(js_sys::Object::new());
        set(&headers, "Range", range)?;
        set(&init, "headers", headers)?;
    }
    let global = JsValue::from(js_sys::global());
    let promise = function(&global, "fetch")?.call2(&global, &url.into(), &init).map_err(js_words)?;
    promise.dyn_into::<js_sys::Promise>().map_err(js_words)
}

fn status(response: &JsValue) -> Result<u16, String> {
    let code = get(response, "status")?.as_f64().ok_or("the response has no status")?;
    Ok(code as u16)
}

/// One response header, or None when the response has none (`Headers.get` answers null then).
fn header(headers: &JsValue, name: &str) -> Result<Option<String>, String> {
    let value = function(headers, "get")?.call1(headers, &name.into()).map_err(js_words)?;
    Ok(value.as_string())
}

async fn body(response: &JsValue) -> Result<Vec<u8>, String> {
    let promise = call_method(response, "arrayBuffer")?.dyn_into::<js_sys::Promise>().map_err(js_words)?;
    let buffer = JsFuture::from(promise).await.map_err(js_words)?;
    Ok(js_sys::Uint8Array::new(&buffer).to_vec())
}

fn get(object: &JsValue, name: &str) -> Result<JsValue, String> {
    js_sys::Reflect::get(object, &name.into()).map_err(js_words)
}

fn set(object: &JsValue, name: &str, value: impl Into<JsValue>) -> Result<(), String> {
    js_sys::Reflect::set(object, &name.into(), &value.into()).map(|_| ()).map_err(js_words)
}

fn function(object: &JsValue, name: &str) -> Result<js_sys::Function, String> {
    get(object, name)?.dyn_into::<js_sys::Function>().map_err(js_words)
}

fn call_method(object: &JsValue, name: &str) -> Result<JsValue, String> {
    function(object, name)?.call0(object).map_err(js_words)
}

/// A JavaScript failure's words: an Error's message, a thrown string, or none of those. (`JsString::from` on an object
/// throws, so an object is never turned into a string that way.)
fn js_words(e: JsValue) -> String {
    e.dyn_ref::<js_sys::Error>()
        .map(|e| String::from(e.to_string()))
        .or_else(|| e.as_string())
        .unwrap_or_else(|| "fetch failed".into())
}

/// What an answer says of its object. A 404 or a 403 is a missing object: a public bucket that lets no one list
/// answers 403 for a key it doesn't have.
fn object_from(location: &Path, url: &Url, answer: Fetched, wanted: Option<&GetRange>, head: bool) -> StoreResult<Object> {
    let Fetched { status, content_length, content_range, etag, last_modified, body } = answer;
    let (range, size, body) = match status {
        200 => whole(url, head, content_length.as_deref(), body, wanted)?,
        206 => partial(url, content_range.as_deref(), body)?,
        404 | 403 => {
            return Err(Error::NotFound {
                path: location.to_string(),
                source: format!("{url} answered {status}").into(),
            })
        }
        other => return Err(generic(format!("{url} answered {other}"))),
    };
    let meta = ObjectMeta {
        location: location.clone(),
        last_modified: modified(last_modified.as_deref()),
        size,
        e_tag: etag,
        version: None,
    };
    Ok(Object { meta, range, body })
}

/// A whole object (a 200). A HEAD's size is its content-length. A GET's size is its body's length, since a server that
/// compresses sends a content-length of the compressed bytes and fetch hands the body back decoded. A range asked of a
/// whole object is cut from the body here, because the server sent all of it.
fn whole(
    url: &Url,
    head: bool,
    content_length: Option<&str>,
    body: Vec<u8>,
    wanted: Option<&GetRange>,
) -> StoreResult<(Range<u64>, u64, Bytes)> {
    if head {
        let size = content_length
            .and_then(|v| v.trim().parse::<u64>().ok())
            .ok_or_else(|| generic(format!("{url} sent no content-length to a HEAD")))?;
        return Ok((0..size, size, Bytes::new()));
    }
    let size = body.len() as u64;
    let Some(wanted) = wanted else {
        return Ok((0..size, size, Bytes::from(body)));
    };
    let range = wanted.as_range(size).map_err(|e| generic(format!("{url}: {e}")))?;
    let part = body
        .get(range.start as usize..range.end as usize)
        .map(Bytes::copy_from_slice)
        .ok_or_else(|| generic(format!("{url}: the range {range:?} is not in its body")))?;
    Ok((range, size, part))
}

/// A part of an object (a 206): its range and the object's size, both from its content-range.
fn partial(url: &Url, content_range: Option<&str>, body: Vec<u8>) -> StoreResult<(Range<u64>, u64, Bytes)> {
    let text = content_range.ok_or_else(|| generic(format!("{url} answered a part with no content-range")))?;
    let (range, size) =
        parse_content_range(text).ok_or_else(|| generic(format!("{url} sent an unreadable content-range: {text}")))?;
    Ok((range, size, Bytes::from(body)))
}

/// `bytes first-last/size` as the range and the size. A size of `*` is not known: the object ends at `last`.
fn parse_content_range(text: &str) -> Option<(Range<u64>, u64)> {
    let (span, size) = text.trim().strip_prefix("bytes ")?.split_once('/')?;
    let (first, last) = span.split_once('-')?;
    let first = first.trim().parse::<u64>().ok()?;
    let last = last.trim().parse::<u64>().ok()?;
    if first > last {
        return None;
    }
    let end = last.checked_add(1)?;
    let size = match size.trim() {
        "*" => end,
        digits => digits.parse::<u64>().ok()?,
    };
    Some((first..end, size))
}

/// The object's last change, from its last-modified; unreadable or missing, the epoch (a meta always has a time).
fn modified(text: Option<&str>) -> DateTime<Utc> {
    text.and_then(|t| DateTime::parse_from_rfc2822(t).ok())
        .map_or(DateTime::<Utc>::UNIX_EPOCH, |t| t.to_utc())
}
