//! A joined page's catalog: the node's own, read over its `GET /cluster/kv` (designs/l2-page-joins-cluster.md, J3). The node
//! answers from its in-memory catalog, so the page sees what the node has committed. The page writes nothing here: a joined
//! page's writes go to the node's `/sql` under its sign-in. `node::open_joined` opens a lake on this.
use super::{Error, Kv, Written};
use crate::fetch;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64U, Engine};
use bytes::Bytes;
use reqwest::header::AUTHORIZATION;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

/// A node's catalog as a `Kv`: its address, and the sign-in's Authorization header value (a `ps_` session, as `Bearer`).
pub(crate) struct Remote {
    node: String,
    auth: String,
    http: reqwest::Client,
}

#[derive(Deserialize)]
struct One {
    value: Option<String>,
}

/// A node's answer for a range: its entries, whether keys follow them, the commit it read them as of, and whether a key in
/// the range moved since the `since` asked (`read_pages`). An old node's answer has none of the last three (the whole
/// range, as it was).
#[derive(Deserialize)]
struct Many {
    entries: Vec<(String, String)>,
    #[serde(default)]
    more: bool,
    #[serde(default)]
    at: Option<u64>,
    #[serde(default)]
    moved: bool,
}

/// The keys a range is asked for in, at most, per request (the node's bound is 5,000, and 8 MB of values).
const PAGE: usize = 5_000;
/// How many times a range starts over, when the node's commit moved under one of its pages, before it fails.
const RESTARTS: usize = 3;

impl Remote {
    /// The catalog of the node at `node`, read with the sign-in `auth`.
    pub(crate) fn new(node: &str, auth: &str) -> Remote {
        Remote { node: node.trim_end_matches('/').to_string(), auth: auth.to_string(), http: reqwest::Client::new() }
    }

    /// One `GET` of the node's `/cluster/kv` at `url`, its answer as `T`. A refusal is its status and the node's words.
    async fn ask<T: DeserializeOwned>(&self, url: String) -> Result<T, Error> {
        let r = fetch::send(self.http.get(url).header(AUTHORIZATION, &self.auth)).await.map_err(|e| Error::new(false, e))?;
        let status = r.status();
        if status.is_success() {
            return r.json::<T>().await.map_err(|e| Error::new(false, e));
        }
        let words = r.text().await.unwrap_or_default();
        Err(refused(status, words))
    }
}

/// The error for a node's refusal. A 401 or 403 means the sign-in ended (closed), and the node's words are the message.
fn refused(status: StatusCode, words: String) -> Error {
    let ended = matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN);
    Error::new(ended, if words.is_empty() { status.to_string() } else { format!("{status}: {words}") }) // (the status names a sign-in the node refused)
}

/// `{node}/cluster/kv?key=K`: the key as text, percent-encoded.
fn key_url(node: &str, key: &str) -> String {
    let mut q = url::form_urlencoded::Serializer::new(String::new());
    q.append_pair("key", key);
    format!("{node}/cluster/kv?{}", q.finish())
}

/// `{node}/cluster/kv?from=F[&to=T]&limit=N[&since=C]`: the bounds' raw bytes, base64url without padding, a page's key count,
/// and the commit the range's first page read as of (a later page's). `to` is left out when the range ends at `[0xff]` or
/// beyond: the node then reads to the end of its keys, as the range does.
fn scan_url(node: &str, from: &[u8], end: &[u8], limit: usize, since: Option<(u64, &[u8])>) -> String {
    let mut q = url::form_urlencoded::Serializer::new(String::new());
    q.append_pair("from", &B64U.encode(from));
    if end < [0xff].as_slice() {
        q.append_pair("to", &B64U.encode(end));
    }
    q.append_pair("limit", &limit.to_string());
    if let Some((since, start)) = since {
        q.append_pair("since", &since.to_string()); // (and where the range starts: a key its first pages held may have moved)
        q.append_pair("start", &B64U.encode(start));
    }
    format!("{node}/cluster/kv?{}", q.finish())
}

/// The bound a range's next page starts from: the first key past `last` (the key with a NUL added, as the keys are UTF-8).
fn next_from(last: &str) -> Vec<u8> {
    let mut next = last.as_bytes().to_vec();
    next.push(0);
    next
}

fn unb64(s: &str) -> Result<Bytes, Error> {
    B64U.decode(s).map(Bytes::from).map_err(|e| Error::new(false, format!("the node's answer is not base64url: {e}")))
}

fn value_of(a: One) -> Result<Option<Bytes>, Error> {
    a.value.as_deref().map(unb64).transpose()
}

fn entries_of(a: Many) -> Result<BTreeMap<String, Bytes>, Error> {
    a.entries.into_iter().map(|(k, v)| Ok((k, unb64(&v)?))).collect()
}

/// A range read page by page: `page(from, since)` answers the keys from `from` on, a page's worth (`Many`). The first page
/// asks no `since`, and answers the commit it read as of (`at`); each later page asks `since` that one, and the node says
/// `moved` when a key in the range changed after it. A moved range starts over, `RESTARTS` times at most, then fails
/// (retryable). A page that says more and holds no key fails too (no endless asking).
async fn read_pages<F, Fut>(range: &Range<Vec<u8>>, page: F) -> Result<BTreeMap<String, Bytes>, Error>
where
    F: Fn(Vec<u8>, Option<u64>) -> Fut,
    Fut: std::future::Future<Output = Result<Many, Error>>,
{
    let mut restarts = 0;
    'range: loop {
        let mut all = BTreeMap::new();
        let mut from = range.start.clone();
        let mut since = None;
        loop {
            let got: Many = page(from, since).await?;
            if got.more && got.entries.is_empty() {
                return Err(Error::new(false, "the node says keys follow in this range, and sends none"));
            }
            if got.moved {
                restarts += 1;
                if restarts > RESTARTS {
                    return Err(Error::new(false, "keys in this range kept moving while it was read: try again"));
                }
                continue 'range;
            }
            if since.is_none() {
                since = got.at; // (the first page's commit: the later pages are asked since it)
            }
            let (more, last) = (got.more, got.entries.last().map(|(k, _)| k.clone()));
            all.extend(entries_of(got)?);
            match last {
                Some(last) if more => from = next_from(&last),
                _ => return Ok(all),
            }
        }
    }
}

#[async_trait::async_trait]
impl Kv for Remote {
    async fn get(&self, key: &str, _remote: bool) -> Result<Option<Bytes>, Error> {
        value_of(self.ask(key_url(&self.node, key)).await?)
    }

    /// The range, page after page of `PAGE` keys (`read_pages`).
    async fn scan(&self, range: Range<Vec<u8>>, _remote: bool) -> Result<BTreeMap<String, Bytes>, Error> {
        let end = &range.end;
        let start = &range.start;
        read_pages(&range, |from, since| async move { self.ask(scan_url(&self.node, &from, end, PAGE, since.map(|s| (s, start.as_slice())))).await }).await
    }

    fn is_writer(&self) -> bool {
        false
    }

    async fn write(&self, _: &[(String, Bytes)], _: &[String]) -> Result<Arc<dyn Written>, Error> {
        Err(Error::new(false, "a joined page writes through its node's /sql"))
    }

    async fn checkpoint(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::ledger::Charged for Remote {
    fn held(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_key_url_percent_encodes_the_key() {
        assert_eq!(key_url("https://n:7000", "cluster/term/3"), "https://n:7000/cluster/kv?key=cluster%2Fterm%2F3");
    }

    #[test]
    fn remote_scan_url_bounds_are_base64url_and_to_is_left_out_at_the_end() {
        assert_eq!(scan_url("https://n", &[1, 2], &[2], 5, None), "https://n/cluster/kv?from=AQI&to=Ag&limit=5");
        assert_eq!(scan_url("https://n", &[1], &[0xfe, 0xff], 5, None), "https://n/cluster/kv?from=AQ&to=_v8&limit=5");
        assert_eq!(scan_url("https://n", &[1], &[0xff], 5, None), "https://n/cluster/kv?from=AQ&limit=5", "a range to [0xff] reads to the end");
        assert_eq!(scan_url("https://n", &[1], &[0xff, 0], 5, None), "https://n/cluster/kv?from=AQ&limit=5", "and so does one beyond it");
    }

    #[test]
    fn remote_the_next_page_starts_past_its_last_key_and_asks_since_the_first_pages_commit() {
        let next = next_from("a");
        assert_eq!(next, [b'a', 0], "the first key past `a`: `a` and a NUL");
        assert_eq!(scan_url("https://n", &next, &[0x62], PAGE, Some((7, b"a"))), "https://n/cluster/kv?from=YQA&to=Yg&limit=5000&since=7&start=YQ");
    }

    #[test]
    fn remote_answers_decode_to_values_and_entries() {
        let one: One = serde_json::from_str(r#"{"value": "AQI"}"#).unwrap();
        assert_eq!(value_of(one).unwrap().unwrap().as_ref(), &[1, 2]);
        let none: One = serde_json::from_str(r#"{"value": null}"#).unwrap();
        assert!(value_of(none).unwrap().is_none());
        let many: Many = serde_json::from_str(r#"{"entries": [["a", "AQI"], ["b", "Ag"]]}"#).unwrap();
        assert!(!many.more && many.at.is_none(), "an old node's answer: the whole range, with neither");
        let got = entries_of(many).unwrap();
        assert_eq!(got["a"].as_ref(), &[1, 2]);
        assert_eq!(got["b"].as_ref(), &[2]);
        let paged: Many = serde_json::from_str(r#"{"entries": [["a", "AQI"]], "more": true, "at": 7}"#).unwrap();
        assert!(paged.more && paged.at == Some(7));
        let bad: One = serde_json::from_str(r#"{"value": "not base64!"}"#).unwrap();
        assert!(value_of(bad).is_err());
    }

    /// `read_pages` reads a range page by page, each key once and in order, each later page asked `since` the first's commit.
    /// A moved range starts over (once, here); one that keeps moving fails after `RESTARTS` tries; an old node's whole answer
    /// is one page.
    #[tokio::test]
    async fn remote_reads_a_range_in_pages_and_starts_over_when_keys_move() {
        let keys: Vec<String> = (0..10).map(|i| format!("k{i:02}")).collect();
        // A node's page from `from`: `size` keys at most, and the keys past them, as of commit `at`; `moved` as asked.
        let answer = |from: &[u8], size: usize, at: Option<u64>, moved: bool| {
            let rest: Vec<&String> = keys.iter().filter(|k| k.as_bytes() >= from).collect();
            Many { entries: rest.iter().take(size).map(|k| (k.to_string(), B64U.encode(k.as_bytes()))).collect(), more: rest.len() > size, at, moved }
        };
        let range = b"k".to_vec()..b"l".to_vec();

        let asked = std::cell::RefCell::new(Vec::new());
        let got = read_pages(&range, |from, since| {
            asked.borrow_mut().push(since);
            let page = answer(&from[..], 3, Some(7), false);
            async move { Ok(page) }
        })
        .await
        .unwrap();
        assert_eq!(got.keys().cloned().collect::<Vec<_>>(), keys, "every key, once, in order");
        assert_eq!(asked.into_inner(), [None, Some(7), Some(7), Some(7)], "the first page asks no commit; the others ask the first's");

        let calls = std::cell::Cell::new(0);
        let got = read_pages(&range, |from, since| {
            let n = calls.get();
            calls.set(n + 1);
            let page = answer(&from[..], 3, Some(7), n == 1 && since.is_some());
            async move { Ok(page) }
        })
        .await
        .unwrap();
        assert_eq!(got.keys().cloned().collect::<Vec<_>>(), keys, "a key moved once: the range is read again, whole");
        assert_eq!(calls.get(), 6, "two pages before the move, four after");

        let calls = std::cell::Cell::new(0);
        let failed = read_pages(&range, |from, since| {
            calls.set(calls.get() + 1);
            let page = answer(&from[..], 3, Some(7), since.is_some());
            async move { Ok(page) }
        })
        .await;
        assert!(failed.is_err() && failed.as_ref().err().is_some_and(|e| !e.closed()), "a retryable failure, not a fence");
        assert_eq!(calls.get(), 8, "the first try and three restarts, two pages each, then the fourth move fails it");

        let empty = read_pages(&range, |_, _| async { Ok(Many { entries: vec![], more: true, at: None, moved: false }) }).await;
        assert!(empty.is_err(), "more keys and none sent: never asked again");

        let old = read_pages(&range, |from, _| {
            let page = answer(&from[..], 100, None, false);
            async move { Ok(page) }
        })
        .await
        .unwrap();
        assert_eq!(old.len(), 10, "an old node's answer is the whole range, in one");
    }

    #[test]
    fn remote_a_refusal_ends_the_sign_in_with_its_status_and_the_nodes_words() {
        let ended = refused(StatusCode::FORBIDDEN, "the sign-in ended".into());
        assert!(ended.closed());
        assert_eq!(ended.to_string(), "403 Forbidden: the sign-in ended", "the status, then the node's words");
        assert!(refused(StatusCode::UNAUTHORIZED, String::new()).closed());
        assert!(!refused(StatusCode::INTERNAL_SERVER_ERROR, "boom".into()).closed());
    }

    #[tokio::test]
    async fn remote_is_read_only_and_refuses_writes_by_name() {
        let kv = Remote::new("https://n:7000/", "Bearer ps_test");
        assert!(!kv.is_writer());
        let e = kv.write(&[], &[]).await.err().expect("a refused write");
        assert!(e.to_string().contains("/sql"));
        assert_eq!(crate::ledger::Charged::held(&kv), 0);
    }
}
