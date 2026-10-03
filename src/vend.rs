//! A file of the lake for someone outside it (ADR-046 §2): a link that reads that file for a while
//! and nothing else, never a credential. A lake on S3, R2, GCS or Azure signs links with its own
//! credentials (object_store's `Signer`): the bucket checks them, and the bytes never pass through
//! a node. A lake on a disk links to a node, which checks the link's signature (the lake's key)
//! and time and serves the bytes, a range at a time if asked. The one place a grant becomes bytes
//! outside the nodes: the sharing door uses it now, other doors later.
use crate::store::Lake;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64U, Engine};
use object_store::signer::Signer;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

/// How long a link lasts: `PONDRA_SHARE_URL_SECS` (900, at most a week: S3's limit).
pub fn secs() -> u64 { std::env::var("PONDRA_SHARE_URL_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(900).clamp(1, 7 * 86400) }

/// Links to these objects of the lake (`data/…`, as the lake names them) for `who`, and when they
/// end (ms). `base` is where a node's links point (a lake on a disk): the sharing door's address.
pub async fn links(lake: &Lake, base: &str, keys: &[String], who: &str) -> Result<(Vec<String>, u64)> {
    let until = crate::log::now_ms() + secs() * 1000;
    let Some((scheme, rest)) = lake.url.split_once("://") else {
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            let payload = B64U.encode(serde_json::to_vec(&serde_json::json!({"k": k, "x": until, "r": who}))?);
            // (`sp`, a signed permission as Azure's links carry it: Delta's kernel reads an http link
            // as a link only when it has one of the clouds' signature parameters)
            out.push(format!("{base}/files/{payload}?sp=r&sig={}", crate::users::sign(lake, payload.as_bytes()).await?));
        }
        return Ok((out, until));
    };
    let (bucket, prefix) = rest.trim_end_matches('/').split_once('/').unwrap_or((rest, ""));
    let paths: Vec<object_store::path::Path> = keys.iter().map(|k| object_store::path::Path::from(if prefix.is_empty() { k.clone() } else { format!("{prefix}/{k}") })).collect();
    let signed = signer(scheme, bucket)?.signed_urls(object_store::signer::Method::GET, &paths, Duration::from_secs(secs())).await
        .with_context(|| format!("signing links to {}'s files (its credentials must be able to sign: an access key, a service account's key, an account key)", lake.url))?;
    Ok((signed.into_iter().map(|u| u.to_string()).collect(), until))
}

/// The bucket's signer, made once from the environment's credentials (as the lake's store is).
fn signer(scheme: &str, bucket: &str) -> Result<Arc<dyn Signer>> {
    static MADE: LazyLock<Mutex<HashMap<String, Arc<dyn Signer>>>> = LazyLock::new(Default::default);
    let at = format!("{scheme}://{bucket}");
    if let Some(s) = MADE.lock().unwrap().get(&at) {
        return Ok(s.clone());
    }
    use object_store::{aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder};
    let s: Arc<dyn Signer> = match scheme {
        "s3" => Arc::new(AmazonS3Builder::from_env().with_bucket_name(bucket).with_allow_http(true).build()?),
        "gs" => Arc::new(GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket).build()?),
        _ => Arc::new(MicrosoftAzureBuilder::from_env().with_url(&at).build()?),
    };
    MADE.lock().unwrap().insert(at, s.clone());
    Ok(s)
}

/// What a node's link names (the object, and who it was made for), if it was signed here and
/// hasn't ended.
pub async fn opened(lake: &Lake, payload: &str, sig: &str) -> Result<(String, String)> {
    let want = crate::users::sign(lake, payload.as_bytes()).await?;
    ensure!(aws_lc_rs::constant_time::verify_slices_are_equal(want.as_bytes(), sig.as_bytes()).is_ok(), "a link that wasn't made here");
    let v: serde_json::Value = serde_json::from_slice(&B64U.decode(payload)?)?;
    ensure!(v["x"].as_u64().unwrap_or(0) > crate::log::now_ms(), "this link has ended: ask the sharing server for the table's files again");
    let key = v["k"].as_str().context("a link without its file")?.to_string();
    ensure!(key.starts_with("data/") && !key.split('/').any(|p| p == ".." || p == "." || p.is_empty()), "a link to something that isn't a table's file");
    Ok((key, v["r"].as_str().unwrap_or_default().to_string()))
}
