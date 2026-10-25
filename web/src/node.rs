//! The page's node (`pondra::page::Node`, B5): a page's request to its node's `/sql`, made with the browser's `fetch`. The
//! node's answer is the page's, as it is: its status, its `content-type` and `x-pondra-sqlstate`, and its body. In a
//! browser the node must allow the page's origin (its `web_origins` setting): the browser refuses the request before the
//! node answers, so the page sees a failed `fetch`, as it would see a node that is down.

use crate::js;
use bytes::Bytes;
use futures::future::FutureExt;
use pondra::page::{past_room, Answer, Asked, Node, Sent};
use std::sync::Arc;
use wasm_bindgen_futures::JsFuture;

/// The node at `url` (its address, with or without a trailing `/`), asked with `token` as a bearer token when there is one.
/// Each request is a POST to `{url}/sql?{query}` (no `?` for an empty query) with the body's content type.
pub(crate) fn node(url: String, token: Option<String>) -> Node {
    let base = url.trim_end_matches('/').to_string();
    Arc::new(move |sent: Sent| -> Asked {
        let query = if sent.query.is_empty() { String::new() } else { format!("?{}", sent.query) };
        let target = format!("{base}/sql{query}");
        let mut headers = vec![("content-type", sent.content_type.to_string())];
        if let Some(token) = &token {
            headers.push(("authorization", format!("Bearer {token}")));
        }
        // (the exchange holds JavaScript values, which are not `Send`: `js::sent` runs it on the page's thread)
        let answered = js::sent(exchange(target.clone(), headers, sent.body, sent.room));
        async move {
            answered.await.map_err(|_| format!("the request to {target} was dropped")).and_then(|answer| answer)
        }
        .boxed()
    })
}

/// One POST to the node, its answer read to the end: its status, the headers a page reads, and its body. A `fetch` that
/// rejects is no answer, and its words name the node and the browser's rule that can refuse it. A body past the page's
/// `room` is not read (`js::body_within`), and the page's answer is `past_room` in its place.
async fn exchange(url: String, headers: Vec<(&'static str, String)>, body: Bytes, room: Option<usize>) -> Result<Answer, String> {
    let promise = js::send(&url, "POST", &headers, Some(&body[..]))?;
    let response = JsFuture::from(promise).await.map_err(|e| {
        format!("no answer from {url} ({}): in a browser, the node must allow this page's origin (web_origins)", js::js_words(e))
    })?;
    let answered = js::get(&response, "headers")?;
    let status = js::status(&response)?;
    let Some(body) = js::body_within(&response, room).await? else { return Ok(past_room(room.unwrap_or_default())) };
    let mut answer = Answer { status, headers: vec![], body: Bytes::from(body) };
    for name in ["content-type", "x-pondra-sqlstate"] {
        if let Some(value) = js::header(&answered, name)? {
            answer.headers.push((name, value));
        }
    }
    Ok(answer)
}
