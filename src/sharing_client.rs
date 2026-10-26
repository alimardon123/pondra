//! Reading another's Delta Sharing shares (ADR-046 §3): `ATTACH '<profile>' AS name (TYPE share)` accepts a share, and
//! its tables read as tables, their files fetched by the provider's short-lived links. And where recipients reach this
//! lake's own door (`endpoint`), which a recipient's profile names. The door itself is `sharing.rs`.
use crate::byte_range;
use crate::reset::Once;
use serde_json::{json as j, Value};
use std::sync::Arc;

crate::per_node!(pub(crate) static ENDPOINT: std::sync::RwLock<Option<String>> = std::sync::RwLock::new(None));

/// Where recipients reach the door (a profile's `endpoint`): `PONDRA_SHARING_URL` (a cluster's
/// address that outlives its nodes), or this node's (`sharing::set_endpoint`).
pub fn endpoint() -> String {
    match crate::env::get("PONDRA_SHARING_URL") {
        Some(u) => u.trim_end_matches('/').to_string(),
        None => ENDPOINT.current().unwrap_or_else(|| "http://127.0.0.1:8080/delta-sharing".into()),
    }
}

// ---------------------------------------------------------------- reading another's shares

/// Leader, `ATTACH '<profile>' AS name (TYPE share)`: the token kept as a secret of TYPE share
/// (`share_<name>`, sealed like every secret), and the share it reads (the only one it may, unless
/// `SHARE 's'` says which). A profile sent is an invite; attaching it accepts it.
pub async fn accept(lake: &crate::store::Lake, name: &str, url: &str, mut options: std::collections::BTreeMap<String, String>) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
    use anyhow::{ensure, Context};
    ensure!(url.starts_with("http://") || url.starts_with("https://"), "ATTACH '<the share's profile, as JSON>' AS {name} (TYPE share), or ATTACH 'https://…/delta-sharing' AS {name} (TYPE share, TOKEN '…')");
    let endpoint = url.trim_end_matches('/');
    if let Some(token) = options.remove("token") {
        let params = [("type", "share"), ("token", token.as_str()), ("scope", endpoint)].into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        crate::ext::create(lake, &format!("share_{name}"), params, true, false).await?;
    }
    let token = crate::ext::share_token(lake, endpoint).await?.with_context(|| format!("no token for {endpoint}: ATTACH '<the share's profile>' AS {name} (TYPE share), or add TOKEN '…'"))?;
    let r = crate::fetch::send(crate::fetch::timeout(crate::ext::web().get(format!("{endpoint}/shares")).bearer_auth(&token), std::time::Duration::from_secs(15))).await.with_context(|| format!("reaching {endpoint}"))?;
    let (status, body) = (r.status(), r.json::<Value>().await.unwrap_or_default());
    ensure!(status.is_success(), "{endpoint} says {status}: {}", body["message"].as_str().unwrap_or_default());
    let shares: Vec<String> = body["items"].as_array().into_iter().flatten().filter_map(|s| s["name"].as_str().map(str::to_string)).collect();
    match options.get("share") {
        Some(s) => ensure!(shares.contains(s), "{endpoint} has no share {s} for this token (it has {})", shares.join(", ")),
        None => {
            ensure!(shares.len() == 1, "{endpoint} has {} shares for this token: say which, ATTACH … (TYPE share, SHARE 'one of {}')", shares.len(), shares.join(", "));
            options.insert("share".into(), shares[0].clone());
        }
    }
    Ok(options)
}

/// A shared table, as a table of files: the provider's answer for its newest version (or the
/// spec's `version`), each file read through its link (`share://`), deletion vectors and renamed
/// columns as Delta's readers take them (`read_delta::table`).
pub async fn read(lake: &crate::store::Lake, spec: &crate::ext_names::Spec) -> anyhow::Result<crate::store::TableMeta> {
    use anyhow::{ensure, Context};
    let endpoint = spec.urls[0].trim_end_matches('/');
    let o = |k: &str| spec.options.get(k).map(String::as_str).unwrap_or_default();
    let token = crate::ext::share_token(lake, endpoint).await?.with_context(|| format!("no token for {endpoint} (a secret of TYPE share whose SCOPE covers it)"))?;
    let mut ask = j!({});
    if let Some(v) = spec.options.get("version") {
        ask["version"] = j!(v.parse::<i64>().with_context(|| format!("version is a number, not {v}"))?);
    }
    let at = format!("{endpoint}/shares/{}/schemas/{}/tables/{}/query", o("share"), o("schema"), o("table"));
    let r = crate::fetch::send(crate::ext::web().post(&at).bearer_auth(&token).header("delta-sharing-capabilities", "responseformat=delta,parquet;readerfeatures=deletionvectors,columnmapping,timestampntz").json(&ask)).await.with_context(|| format!("reaching {endpoint}"))?;
    let status = r.status();
    let delta = r.headers().get("delta-sharing-capabilities").and_then(|v| v.to_str().ok()).is_some_and(|c| c.contains("responseformat=delta"));
    let text = r.text().await?;
    ensure!(status.is_success(), "{}.{}.{} at {endpoint}: {status} {}", o("share"), o("schema"), o("table"), serde_json::from_str::<Value>(&text).ok().and_then(|e| e["message"].as_str().map(str::to_string)).unwrap_or(text));
    let lines: Vec<Value> = text.lines().filter(|l| !l.trim().is_empty()).map(serde_json::from_str).collect::<Result<_, _>>()?;
    ensure!(lines.len() >= 2, "{at}: an answer without its protocol and metadata");
    // (a link on the provider's own host is reached as the provider is, its admin having named it; any other only on the
    // public internet, every address and redirect checked as it is reached: `public::web`)
    let host = url::Url::parse(endpoint).ok().and_then(|u| u.host_str().map(str::to_string));
    let link = |u: &str| -> anyhow::Result<String> {
        let at = url::Url::parse(u).with_context(|| format!("{endpoint}: a link that isn't a URL"))?;
        let own = at.host_str().is_some_and(|h| Some(h) == host.as_deref());
        if !own {
            crate::public::check(&at).with_context(|| format!("{endpoint} links to {}", at.host_str().unwrap_or_default()))?;
        }
        Ok(format!("share://{}/{}", if own { "files" } else { "public" }, base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, u)))
    };
    let (protocol, metadata) = match delta {
        true => (lines[0]["protocol"]["deltaProtocol"].clone(), lines[1]["metaData"]["deltaMetadata"].clone()),
        false => (j!({"minReaderVersion": 1, "minWriterVersion": 1}), lines[1]["metaData"].clone()),
    };
    let adds: Vec<Value> = lines[2..].iter().map(|l| {
        let f = &l["file"];
        let mut add = match delta {
            true => f["deltaSingleAction"]["add"].clone(),
            false => j!({"path": f["url"], "partitionValues": f["partitionValues"], "size": f["size"], "stats": f["stats"]}),
        };
        add["path"] = j!(link(add["path"].as_str().unwrap_or_default())?);
        if add["deletionVector"]["storageType"] == "p" {
            add["deletionVector"]["pathOrInlineDv"] = j!(link(add["deletionVector"]["pathOrInlineDv"].as_str().unwrap_or_default())?); // (a vector of its own: a link too)
        }
        Ok(add)
    }).collect::<anyhow::Result<_>>()?;
    lake.rt.register_object_store(&url::Url::parse("share://files")?, Arc::new(Links { public: false }));
    lake.rt.register_object_store(&url::Url::parse("share://public")?, Arc::new(Links { public: true }));
    crate::read_delta::table(endpoint, &protocol, &metadata, adds.iter()).map(crate::ext::table_meta)
}

/// The files a provider links to (`share://files/<link, base64url>`, or `share://public/…` for a link off its own host):
/// read with a GET of the link and the range asked, as a bucket would answer it. Links end, so nothing of them is kept.
#[derive(Debug)]
struct Links {
    public: bool, // (reached only on the public internet: `public::web`)
}

impl std::fmt::Display for Links {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("a share's links") }
}

fn links_error(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> object_store_df::Error { object_store_df::Error::Generic { store: "share", source: e.into() } }

/// DataFusion's object_store is another crate's copy of this one's, so its ranges are written out as ours.
fn ours(r: &object_store_df::GetRange) -> object_store::GetRange {
    match r {
        object_store_df::GetRange::Bounded(b) => object_store::GetRange::Bounded(b.clone()),
        object_store_df::GetRange::Offset(o) => object_store::GetRange::Offset(*o),
        object_store_df::GetRange::Suffix(n) => object_store::GetRange::Suffix(*n),
    }
}

#[async_trait::async_trait]
impl object_store_df::ObjectStore for Links {
    async fn put_opts(&self, _: &object_store_df::path::Path, _: object_store_df::PutPayload, _: object_store_df::PutOptions) -> object_store_df::Result<object_store_df::PutResult> {
        Err(object_store_df::Error::NotSupported { source: "a share is read only".into() })
    }
    async fn put_multipart_opts(&self, _: &object_store_df::path::Path, _: object_store_df::PutMultipartOptions) -> object_store_df::Result<Box<dyn object_store_df::MultipartUpload>> {
        Err(object_store_df::Error::NotSupported { source: "a share is read only".into() })
    }
    async fn get_opts(&self, location: &object_store_df::path::Path, options: object_store_df::GetOptions) -> object_store_df::Result<object_store_df::GetResult> {
        let encoded = location.as_ref().rsplit('/').next().unwrap_or_default();
        let url = String::from_utf8(base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, encoded).map_err(links_error)?).map_err(links_error)?;
        // (a link is signed for GET alone: a HEAD is a GET of the first byte; no range asked is the whole file)
        let want = match (&options.range, options.head) {
            (_, true) => object_store::GetRange::Bounded(0..1),
            (Some(r), _) => ours(r),
            (None, _) => object_store::GetRange::Offset(0),
        };
        let web = if self.public { crate::public::web() } else { crate::ext::web() };
        let r = crate::fetch::send(web.get(&url).header("range", byte_range::header(&want))).await.map_err(links_error)?;
        match r.status().as_u16() {
            200 | 206 => {}
            404 => return Err(object_store_df::Error::NotFound { path: location.to_string(), source: "the provider has no such file now".into() }),
            s => return Err(links_error(format!("the provider's link answered {s} (links end after a while: run the query again)"))),
        }
        let answer = r.headers().get("content-range").and_then(|v| v.to_str().ok()).and_then(byte_range::answered);
        let start = answer.as_ref().map_or(0, |(span, _)| span.start);
        let total = answer.and_then(|(_, size)| size);
        let bytes = if options.head { bytes::Bytes::new() } else { r.bytes().await.map_err(links_error)? };
        let size = total.unwrap_or(start + bytes.len() as u64);
        let meta = object_store_df::ObjectMeta { location: location.clone(), last_modified: chrono::DateTime::UNIX_EPOCH, size, e_tag: None, version: None };
        let range = if options.head { 0..size } else { start..start + bytes.len() as u64 };
        let payload = object_store_df::GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(bytes) })));
        Ok(object_store_df::GetResult { payload, meta, range, attributes: Default::default() })
    }
    fn delete_stream(&self, locations: futures::stream::BoxStream<'static, object_store_df::Result<object_store_df::path::Path>>) -> futures::stream::BoxStream<'static, object_store_df::Result<object_store_df::path::Path>> {
        use futures::StreamExt;
        locations.map(|_| Err(object_store_df::Error::NotSupported { source: "a share is read only".into() })).boxed()
    }
    fn list(&self, _: Option<&object_store_df::path::Path>) -> futures::stream::BoxStream<'static, object_store_df::Result<object_store_df::ObjectMeta>> { Box::pin(futures::stream::empty()) }
    async fn list_with_delimiter(&self, _: Option<&object_store_df::path::Path>) -> object_store_df::Result<object_store_df::ListResult> { Ok(object_store_df::ListResult { common_prefixes: vec![], objects: vec![] }) }
    async fn copy_opts(&self, _: &object_store_df::path::Path, _: &object_store_df::path::Path, _: object_store_df::CopyOptions) -> object_store_df::Result<()> {
        Err(object_store_df::Error::NotSupported { source: "a share is read only".into() })
    }
}
