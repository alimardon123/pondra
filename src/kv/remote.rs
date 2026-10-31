//! A joined page's catalog: the node's own, read over its `GET /cluster/kv` (designs/l2-page-joins-cluster.md, J3). The node
//! answers from its in-memory catalog, so the page sees what the node has committed. The page writes nothing here: a joined
//! page's writes go to the node's `/sql` under its sign-in. Nothing opens a lake on this yet (J5's `Page.join` does).
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

#[derive(Deserialize)]
struct Many {
    entries: Vec<(String, String)>,
}

impl Remote {
    /// The catalog of the node at `node`, read with the sign-in `auth`. (Nothing opens a lake on it yet: J5's `Page.join` does.)
    #[allow(dead_code)] // (J5's `Page.join` is the first to open a lake on it)
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
    Error::new(ended, if words.is_empty() { status.to_string() } else { words })
}

/// `{node}/cluster/kv?key=K`: the key as text, percent-encoded.
fn key_url(node: &str, key: &str) -> String {
    let mut q = url::form_urlencoded::Serializer::new(String::new());
    q.append_pair("key", key);
    format!("{node}/cluster/kv?{}", q.finish())
}

/// `{node}/cluster/kv?from=F[&to=T]`: the bounds' raw bytes, base64url without padding. `to` is left out when the range
/// ends at `[0xff]` or beyond: the node then reads to the end of its keys, as the range does.
fn scan_url(node: &str, range: &Range<Vec<u8>>) -> String {
    let mut q = url::form_urlencoded::Serializer::new(String::new());
    q.append_pair("from", &B64U.encode(&range.start));
    if range.end.as_slice() < [0xff].as_slice() {
        q.append_pair("to", &B64U.encode(&range.end));
    }
    format!("{node}/cluster/kv?{}", q.finish())
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

#[async_trait::async_trait]
impl Kv for Remote {
    async fn get(&self, key: &str, _remote: bool) -> Result<Option<Bytes>, Error> {
        value_of(self.ask(key_url(&self.node, key)).await?)
    }

    async fn scan(&self, range: Range<Vec<u8>>, _remote: bool) -> Result<BTreeMap<String, Bytes>, Error> {
        entries_of(self.ask(scan_url(&self.node, &range)).await?)
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

    fn cached(&self) -> usize {
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
        assert_eq!(scan_url("https://n", &(vec![1, 2]..vec![2])), "https://n/cluster/kv?from=AQI&to=Ag");
        assert_eq!(scan_url("https://n", &(vec![1]..vec![0xfe, 0xff])), "https://n/cluster/kv?from=AQ&to=_v8");
        assert_eq!(scan_url("https://n", &(vec![1]..vec![0xff])), "https://n/cluster/kv?from=AQ", "a range to [0xff] reads to the end");
        assert_eq!(scan_url("https://n", &(vec![1]..vec![0xff, 0])), "https://n/cluster/kv?from=AQ", "and so does one beyond it");
    }

    #[test]
    fn remote_answers_decode_to_values_and_entries() {
        let one: One = serde_json::from_str(r#"{"value": "AQI"}"#).unwrap();
        assert_eq!(value_of(one).unwrap().unwrap().as_ref(), &[1, 2]);
        let none: One = serde_json::from_str(r#"{"value": null}"#).unwrap();
        assert!(value_of(none).unwrap().is_none());
        let many: Many = serde_json::from_str(r#"{"entries": [["a", "AQI"], ["b", "Ag"]]}"#).unwrap();
        let got = entries_of(many).unwrap();
        assert_eq!(got["a"].as_ref(), &[1, 2]);
        assert_eq!(got["b"].as_ref(), &[2]);
        let bad: One = serde_json::from_str(r#"{"value": "not base64!"}"#).unwrap();
        assert!(value_of(bad).is_err());
    }

    #[test]
    fn remote_a_refusal_ends_the_sign_in_with_the_nodes_words() {
        let ended = refused(StatusCode::FORBIDDEN, "the sign-in ended".into());
        assert!(ended.closed());
        assert_eq!(ended.to_string(), "the sign-in ended");
        assert!(refused(StatusCode::UNAUTHORIZED, String::new()).closed());
        assert!(!refused(StatusCode::INTERNAL_SERVER_ERROR, "boom".into()).closed());
    }

    #[tokio::test]
    async fn remote_is_read_only_and_refuses_writes_by_name() {
        let kv = Remote::new("https://n:7000/", "Bearer ps_test");
        assert!(!kv.is_writer());
        let e = kv.write(&[], &[]).await.err().expect("a refused write");
        assert!(e.to_string().contains("/sql"));
        assert_eq!(kv.cached(), 0);
    }
}
