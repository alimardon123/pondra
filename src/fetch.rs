//! The lake's calls to services outside it (a catalog's REST API, a share's provider, an identity provider, GitHub, a
//! model's endpoint), through reqwest: on a node over its own sockets, on a page over the browser's fetch. A page's fetch
//! futures and answers aren't `Send`, while the lake's tasks and DataFusion's traits want `Send`, so a caller sends here
//! and reads the answer here, the same on both. (The cluster's own calls are the cluster's.)
use bytes::Bytes;
use std::future::Future;
use std::time::Duration;

/// An answer, its body not read yet.
pub struct Response(reqwest::Response);

/// `req` sent.
pub fn send(req: reqwest::RequestBuilder) -> impl Future<Output = reqwest::Result<Response>> + Send {
    sent(async move { req.send().await.map(Response) })
}

/// `req` with a time limit. A page's fetch keeps the browser's own limits.
pub fn timeout(req: reqwest::RequestBuilder, limit: Duration) -> reqwest::RequestBuilder {
    #[cfg(not(target_arch = "wasm32"))]
    let req = req.timeout(limit);
    #[cfg(target_arch = "wasm32")]
    let _ = limit;
    req
}

/// Did `e` fail before an answer came, other than by its time limit (worth trying again)?
#[cfg(not(target_arch = "wasm32"))]
pub fn unreached(e: &reqwest::Error) -> bool {
    (e.is_connect() || e.is_request()) && !e.is_timeout()
}

#[cfg(target_arch = "wasm32")]
pub fn unreached(e: &reqwest::Error) -> bool {
    e.is_request()
}

impl Response {
    pub fn status(&self) -> reqwest::StatusCode {
        self.0.status()
    }

    pub fn headers(&self) -> &reqwest::header::HeaderMap {
        self.0.headers()
    }

    /// This answer, or reqwest's error for a 4xx or 5xx status.
    pub fn error_for_status(self) -> reqwest::Result<Self> {
        self.0.error_for_status().map(Response)
    }

    pub fn bytes(self) -> impl Future<Output = reqwest::Result<Bytes>> + Send {
        sent(self.0.bytes())
    }

    pub fn text(self) -> impl Future<Output = reqwest::Result<String>> + Send {
        sent(self.0.text())
    }

    pub fn json<T: serde::de::DeserializeOwned>(self) -> impl Future<Output = reqwest::Result<T>> + Send {
        sent(self.0.json())
    }

    /// The body, or None once it is longer than `most` bytes: a node stops reading there; a page's fetch reads it whole.
    pub fn body_within(self, most: usize) -> impl Future<Output = reqwest::Result<Option<Vec<u8>>>> + Send {
        #[cfg(not(target_arch = "wasm32"))]
        let read = async move {
            let (mut r, mut body) = (self.0, vec![]);
            while let Some(piece) = r.chunk().await? {
                body.extend_from_slice(&piece);
                if body.len() > most {
                    return Ok(None);
                }
            }
            Ok(Some(body))
        };
        #[cfg(target_arch = "wasm32")]
        let read = async move { self.0.bytes().await.map(|b| (b.len() <= most).then(|| b.to_vec())) };
        sent(read)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn sent<F: Future + Send>(f: F) -> F {
    f
}

#[cfg(target_arch = "wasm32")]
fn sent<F: Future>(f: F) -> OneThread<F> {
    OneThread(f)
}

#[cfg(all(target_arch = "wasm32", target_feature = "atomics"))]
compile_error!("fetch.rs counts on a page having one thread: with wasm threads, its answers need another way to be Send");

/// A page's fetch future, `Send` because a page's wasm has one thread: built without `atomics`, it can make no other,
/// so nothing here is ever sent to or shared with one.
#[cfg(target_arch = "wasm32")]
struct OneThread<F>(F);

#[cfg(target_arch = "wasm32")]
unsafe impl<F> Send for OneThread<F> {}

#[cfg(target_arch = "wasm32")]
impl<F: Future> Future for OneThread<F> {
    type Output = F::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<F::Output> {
        // (the future is never moved out of its pin: this is a field's projection)
        unsafe { self.map_unchecked_mut(|s| &mut s.0) }.poll(cx)
    }
}

// (an answer is held across awaits by its caller, as a node's is: on a page's one thread that is safe, as above)
#[cfg(target_arch = "wasm32")]
unsafe impl Send for Response {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for Response {}
