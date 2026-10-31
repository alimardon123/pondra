//! The store's wire (ADR-072 §8): what a node's store door (`served.rs`) answers, and what a store client reads back. A
//! listing is one `Meta` JSON line per object, or one `Delimited` with `delimiter`; a failure ends it as a `Failed` line.
//! An object's size and time are headers the door sends (`object_meta`, `content_span`), the time in nanoseconds so it
//! comes back exactly, and an answer that isn't a success is `error`. `Served` (a node's client) and `joined.rs` (a
//! page's, over `fetch`) read through here, so the two sides can't drift apart.

use crate::byte_range;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use object_store::path::Path;
use object_store::{Error as E, ObjectMeta};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::pin::Pin;
use std::time::Duration;

/// How long a body may go without a chunk before its read fails (a stalled bucket fails the same way).
const IDLE: Duration = Duration::from_secs(30);

/// One object as the listings and the object headers say it: the same fields on both sides.
#[derive(Serialize, Deserialize)]
pub struct Meta { pub path: String, pub size: u64, pub modified_ns: i64, pub e_tag: Option<String>, pub version: Option<String> }

impl From<&ObjectMeta> for Meta {
    fn from(m: &ObjectMeta) -> Meta {
        Meta { path: m.location.to_string(), size: m.size, modified_ns: nanos(m.last_modified), e_tag: m.e_tag.clone(), version: m.version.clone() }
    }
}

impl Meta {
    pub fn meta(self) -> object_store::Result<ObjectMeta> {
        Ok(ObjectMeta { location: Path::parse(&self.path)?, last_modified: DateTime::from_timestamp_nanos(self.modified_ns), size: self.size, e_tag: self.e_tag, version: self.version })
    }
}

/// A listing's last line when the store fails midway: the error's text and the status it answers with.
#[derive(Serialize, Deserialize)]
pub struct Failed { pub error: String, pub status: u16 }

/// One line of a listing: an object, or the failure that ends it.
#[derive(Deserialize)]
#[serde(untagged)]
enum Line { Failed(Failed), Object(Meta) }

/// A delimited listing: the objects under the prefix and the prefixes under it.
#[derive(Serialize, Deserialize)]
pub struct Delimited { pub objects: Vec<Meta>, pub prefixes: Vec<String> }

/// A time as nanoseconds since the epoch, which the headers and listings carry so it comes back
/// exactly (no store reports a time past 2262, the last one that has none).
pub fn nanos(t: DateTime<Utc>) -> i64 { t.timestamp_nanos_opt().unwrap_or_default() }

/// Whether a listing's prefix is one of the lake's folders. `Path::parse` takes the trailing slash off a prefix, so
/// the folder is checked with it back on, the way the lake's files are named. No prefix is the whole store: refused.
pub fn folder_of_lake(prefix: &str) -> bool {
    Path::parse(prefix).is_ok_and(|at| crate::store::lake_file(&format!("{at}/")))
}

pub fn text_of(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

/// The error for an answer the door gave. Without the door's header it is another server's answer
/// (an older Pondra, a proxy, or the node's sign-in check, which says why in its body).
pub fn error(at: &str, status: StatusCode, headers: &HeaderMap, body: &str, path: &str, create: bool) -> E {
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
pub fn generic(at: &str, what: impl std::fmt::Display) -> E {
    E::Generic { store: "pondra", source: format!("pondra://{at} {what}").into() }
}

/// The query of a listing: its prefix and where it starts after.
pub fn listing_query(prefix: Option<&Path>, offset: Option<&Path>) -> Vec<(&'static str, String)> {
    prefix.map(|p| ("prefix", p.to_string())).into_iter().chain(offset.map(|o| ("offset", o.to_string()))).collect()
}

/// An object's metadata as the object headers carry it.
pub fn object_meta(headers: &HeaderMap, at: &Path, served: &str) -> object_store::Result<ObjectMeta> {
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
pub fn content_span(headers: &HeaderMap, served: &str) -> object_store::Result<Range<u64>> {
    let text = text_of(headers, "content-range").ok_or_else(|| generic(served, "answered a partial read without its content-range"))?;
    byte_range::answered(&text).map(|(span, _)| span).ok_or_else(|| generic(served, format!("answered the content-range {text:?}, which isn't one")))
}

/// A body's chunks, each waited for `IDLE` at most. After a failure the stream is over.
pub fn chunks<F: std::fmt::Display + 'static>(body: BoxStream<'static, Result<Bytes, F>>, at: String) -> impl Stream<Item = object_store::Result<Bytes>> + Send + 'static {
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
