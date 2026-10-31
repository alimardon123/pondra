//! A web origin's objects, read with `fetch` (the page's store for one origin, designs/l2-b4-web.md section 4i): a `GET`
//! with a `Range`, and a `HEAD`. A web server writes nothing, so a write is refused by name. Options other than `head` and
//! `range` (`if_match`, `version`, …) are not sent: DataFusion asks for none of them. A web server lists nothing, so a
//! listing is refused by name, except the listing of an object this store found missing, which is empty (`missing`).
//!
//! A web server on another origin must allow the page's origin (CORS). A page sees only the response headers CORS lets
//! it read, so the server exposes `Content-Range` only for a read from the end of an object (a suffix range); a part
//! whose content-range the page can't see is read from the range asked (`unranged`). An `ETag` the server hides is
//! only left out of the object's meta.

use crate::js::{body, get, header, js_words, read_of, send, sent, status};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use std::fmt;
use std::ops::Range;
use url::Url;
use wasm_bindgen_futures::JsFuture;

/// A web origin's objects. The origin is kept as parsed, or as the error that says why it isn't a URL: the core makes
/// the store from a name in SQL, and a bad name must answer each read with its error, never trap the page.
#[derive(Debug)]
pub struct Fetch {
    origin: Result<Url, String>,
    /// The objects a read found missing (a 404 or a 403). DataFusion reads a file it names by its path, and when that
    /// path is no object it lists the path, as a folder: a web server lists nothing, so the listing of a missing object
    /// is empty (what a store answers for a key it doesn't have), and a folder is still refused by name.
    missing: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl Fetch {
    pub fn new(origin: &str) -> Fetch {
        Fetch {
            origin: Url::parse(origin).map_err(|e| format!("{origin} is not an origin: {e}")),
            missing: Default::default(),
        }
    }

    /// Is `prefix` an object this store found missing (a read of it answered 404 or 403)?
    fn is_missing(&self, prefix: Option<&Path>) -> bool {
        let Some(prefix) = prefix else { return false };
        self.missing.lock().map_or(false, |m| m.contains(prefix.as_ref()))
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
        // (what is read, and whether a body is wanted, is `js::read_of`'s: a HEAD and an empty range want no body)
        let read = read_of(options);
        let method = if read.body { "GET" } else { "HEAD" };
        let answer = sent(fetched(url.to_string(), method, read.range.as_ref().map(range_header)))
            .await
            .map_err(|_| dropped(&url))?
            .map_err(|e| transport(&url, &e))?;
        let found = object_from(location, &url, answer, read.range.as_ref(), !read.body);
        if let Err(Error::NotFound { .. }) = &found {
            if let Ok(mut missing) = self.missing.lock() {
                missing.insert(location.as_ref().to_string());
            }
        }
        let mut object = found?;
        if let Some(start) = read.empty_at {
            object.range = start..start;
        }
        let Object { meta, range, body } = object;
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(body) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        locations.map(|_| Err(writes())).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        if self.is_missing(prefix) {
            return stream::empty().boxed();
        }
        stream::once(async { Err(listing()) }).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        if self.is_missing(prefix) {
            return Ok(ListResult { common_prefixes: Vec::new(), objects: Vec::new(), extensions: Default::default() });
        }
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
    let asked: Vec<(&str, String)> = range.into_iter().map(|range| ("Range", range)).collect();
    let response = JsFuture::from(send(&url, method, &asked, None)?).await.map_err(js_words)?;
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

/// What an answer says of its object. A 404 or a 403 is a missing object: a public bucket that lets no one list
/// answers 403 for a key it doesn't have.
fn object_from(location: &Path, url: &Url, answer: Fetched, wanted: Option<&GetRange>, head: bool) -> StoreResult<Object> {
    let Fetched { status, content_length, content_range, etag, last_modified, body } = answer;
    let (range, size, body) = match status {
        200 => whole(url, head, content_length.as_deref(), body, wanted)?,
        206 => partial(url, content_range.as_deref(), body, wanted)?,
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

/// A part of an object (a 206): its range and the object's size, both from its content-range when the page can see it.
/// When it can't (the server sent none, or CORS hides it), the range asked gives them (`unranged`).
fn partial(
    url: &Url,
    content_range: Option<&str>,
    body: Vec<u8>,
    wanted: Option<&GetRange>,
) -> StoreResult<(Range<u64>, u64, Bytes)> {
    let Some(text) = content_range else {
        return unranged(url, body, wanted);
    };
    let (range, size) =
        parse_content_range(text).ok_or_else(|| generic(format!("{url} sent an unreadable content-range: {text}")))?;
    Ok((range, size, Bytes::from(body)))
}

/// A part whose content-range the page can't see. Its body starts at the range asked: a bounded range's part is
/// `start..start + len`, and an offset's runs to the object's end, so its size is `offset + len`. A suffix has no
/// start to read from, and a part for no range asked is no read the page makes: both are refused, with the cure.
fn unranged(url: &Url, body: Vec<u8>, wanted: Option<&GetRange>) -> StoreResult<(Range<u64>, u64, Bytes)> {
    let len = body.len() as u64;
    let (start, size) = match wanted {
        Some(GetRange::Bounded(r)) => bounded(url, r, len)?,
        Some(GetRange::Offset(from)) => (*from, from.saturating_add(len)),
        Some(GetRange::Suffix(_)) | None => {
            return Err(generic(format!(
                "{url} answered a part whose content-range isn't visible to this page: a read from the end of an \
                 object needs it, and a server on another origin must expose it \
                 (Access-Control-Expose-Headers: Content-Range)"
            )))
        }
    };
    if len == 0 {
        return Err(generic(format!("{url} answered a part with no bytes")));
    }
    Ok((start..start.saturating_add(len), size, Bytes::from(body)))
}

/// A bounded range's start, and the object's size, from a body of `len` bytes: a body longer than the range is an
/// error. A shorter body ends the object there; a full one says only that the object is at least the range's end (all
/// that is known of its size).
fn bounded(url: &Url, r: &Range<u64>, len: u64) -> StoreResult<(u64, u64)> {
    let asked = r.end.saturating_sub(r.start);
    if len > asked {
        return Err(generic(format!("{url} answered {len} bytes to a range of {asked}")));
    }
    let size = if len < asked { r.start.saturating_add(len) } else { r.end };
    Ok((r.start, size))
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
