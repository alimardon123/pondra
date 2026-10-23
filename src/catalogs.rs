//! Other engines' catalogs over Iceberg's REST protocol (ADR-026, ADR-028): how a request reaches one (a
//! bearer token, or AWS's signature for Glue and S3 Tables), and the credentials a table's files
//! are reached with when the catalog vouches for them: the ones it hands out with the table
//! (vended: Polaris, Unity, Snowflake's Open Catalog, Lakekeeper, Glue through Lake Formation), or
//! the AWS keys an AWS catalog was attached with.
//!
//! Those credentials reach that table's files and nothing else. While the table is read or
//! written they serve its location only (`within`); its files are then read through a store of
//! their own, `delegated://<id>/…` (`rehome`), never through the bucket's store, so no other
//! statement, and no file another table names, ever reads with them (invariant 339).
use crate::ext_names::{Attached, Spec};
use crate::ext_names::endpoint;
use crate::store::{DataFile, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use object_store::aws::{AwsAuthorizer, AwsCredential};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/// A REST catalog as one statement reaches it.
pub struct Rest {
    base: String, // `{endpoint}/v1/{prefix}`
    auth: Auth,
    vended: bool,                                   // ask for the table's files' credentials (ACCESS_DELEGATION_MODE)
    keys: Option<BTreeMap<String, String>>,         // an AWS catalog's secret: its tables' files are read with it too
}

enum Auth {
    Token(Option<String>),
    Aws { service: &'static str, region: String, keys: BTreeMap<String, String> },
}

/// The catalog `spec` names: an attachment's (`catalog` in its options: its endpoint, how it signs,
/// whether it vends), else the one at its URL, with the token its covering secret gets.
pub async fn open(lake: &Lake, url: &str, options: &BTreeMap<String, String>) -> Result<Rest> {
    let attached = match options.get("catalog") {
        Some(name) => Some(attachment(lake, name, url).await?),
        None => None,
    };
    let o = |k: &str| attached.as_ref().and_then(|a| a.options.get(k)).cloned();
    let auth = match o("endpoint_type") {
        Some(t) => {
            let name = o("secret").with_context(|| format!("{t}: attached without its SECRET"))?;
            let keys = aws_keys(lake, &name).await?;
            let region = region(attached.as_ref().map_or("", |a| a.url.as_str()), &keys)?;
            Auth::Aws { service: service(&t)?, region, keys }
        }
        None => Auth::Token(crate::ext::rest_token(lake, url).await?),
    };
    let keys = match &auth {
        Auth::Aws { keys, .. } => Some(keys.clone()),
        Auth::Token(_) => None,
    };
    let vended = o("access_delegation_mode").is_none_or(|m| m != "none");
    let mut rest = Rest { base: format!("{url}/v1/"), auth, vended, keys };
    // The catalog's own prefix (Glue's `catalogs/{account}`, S3 Tables' bucket ARN): from its config.
    let enc = |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>().replace('+', "%20");
    let mut config = format!("{url}/v1/config");
    if let Some(w) = options.get("warehouse") {
        config += &format!("?warehouse={}", enc(w));
    }
    let (status, conf) = rest.send(reqwest::Method::GET, &config, None).await?;
    let conf = if status.is_success() { conf } else { Value::Null }; // (a catalog with no config: no prefix)
    if let Some(p) = conf["overrides"]["prefix"].as_str().or(conf["defaults"]["prefix"].as_str()) {
        rest.base += &format!("{p}/");
    }
    Ok(rest)
}

/// The attachment a spec names: an Iceberg catalog's, and the endpoint it was attached with
/// (a name written by hand can't send its signed requests anywhere else).
async fn attachment(lake: &Lake, name: &str, url: &str) -> Result<Attached> {
    let a = crate::ext::attached(lake).await?.into_iter().find(|(n, _)| n == name).map(|(_, a)| a).with_context(|| format!("no catalog {name} is attached"))?;
    ensure!(a.kind == "iceberg" && endpoint(&a).as_deref() == Some(url), "{url} isn't the endpoint {name} is attached with");
    Ok(a)
}

/// Is `url` the endpoint of the AWS catalog attached as `name`, signed with a secret this caller may
/// use? Its secret covers its buckets, not its endpoint, so using that secret is the right to read
/// through it (`ext::check`); one the caller may not use is refused by name.
pub async fn signed(lake: &Lake, name: &str, url: &str) -> Result<bool> {
    let a = attachment(lake, name, url).await?;
    let Some(secret) = a.options.get("secret").filter(|_| a.options.contains_key("endpoint_type")) else { return Ok(false) };
    aws_keys(lake, secret).await?;
    Ok(true)
}

impl Rest {
    /// A table's place in the catalog.
    pub fn table(&self, options: &BTreeMap<String, String>) -> String {
        let enc = |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>().replace('+', "%20");
        format!("{}namespaces/{}/tables/{}", self.base, enc(&options["namespace"].replace('.', "\u{1f}")), enc(&options["table"]))
    }

    /// A request to the catalog, signed as it wants, and its answer.
    pub async fn send(&self, method: reqwest::Method, url: &str, body: Option<&Value>) -> Result<(reqwest::StatusCode, Value)> {
        let bytes = body.map(serde_json::to_vec).transpose()?.unwrap_or_default();
        let mut r = crate::ext::web().request(method.clone(), url);
        if body.is_some() {
            r = r.header("content-type", "application/json");
        }
        if self.vended && method == reqwest::Method::GET && url.contains("/tables/") {
            r = r.header("x-iceberg-access-delegation", "vended-credentials");
        }
        match &self.auth {
            Auth::Token(Some(t)) => r = r.bearer_auth(t),
            Auth::Token(None) => {}
            Auth::Aws { service, region, keys } => {
                let mut signed: object_store::client::HttpRequest = http::Request::builder().method(method).uri(url).body(bytes.clone().into())?;
                if body.is_some() {
                    signed.headers_mut().insert("content-type", http::HeaderValue::from_static("application/json"));
                }
                AwsAuthorizer::new(&credential(keys).await?, service, region).try_authorize(&mut signed, None).context("signing the catalog's request")?;
                for (k, v) in signed.headers() {
                    r = r.header(k.as_str(), v.as_bytes());
                }
            }
        }
        let r = r.body(bytes).send().await.with_context(|| format!("reaching {url}"))?;
        let status = r.status();
        Ok((status, r.json().await.unwrap_or_default()))
    }

    /// A table as the catalog gives it (`metadata-location`, `metadata`, `config`), and the
    /// credentials its files are reached with, if the catalog vouches for them.
    pub async fn load(&self, lake: &Lake, url: &str, options: &BTreeMap<String, String>) -> Result<(Value, Option<Delegated>)> {
        let at = self.table(options);
        let (status, body) = self.send(reqwest::Method::GET, &at, None).await?;
        ensure!(status.is_success(), "{url}: no table {}.{} ({status}: {})", options["namespace"], options["table"], body["error"]["message"].as_str().unwrap_or(""));
        let delegated = self.delegated(lake, url, options, &body)?;
        Ok((body, delegated))
    }

    /// The store a table's files are reached with: the credentials vended for its location (the
    /// longest `storage-credentials` prefix, else the answer's `config`), or an AWS catalog's keys.
    fn delegated(&self, lake: &Lake, url: &str, options: &BTreeMap<String, String>, body: &Value) -> Result<Option<Delegated>> {
        let Some(location) = body["metadata"]["location"].as_str().map(|l| format!("{}/", l.trim_end_matches('/'))) else { return Ok(None) };
        let given = body["storage-credentials"].as_array().into_iter().flatten()
            .filter(|c| c["prefix"].as_str().is_some_and(|p| location.starts_with(&format!("{}/", p.trim_end_matches('/')))))
            .max_by_key(|c| c["prefix"].as_str().map_or(0, str::len)).map(|c| &c["config"]).unwrap_or(&body["config"]);
        let params = match (vended(given), &self.keys) {
            (Some(p), _) => p,
            (None, Some(keys)) => keys.clone(),
            (None, None) => return Ok(None), // (its files need a secret covering them, as a folder's do)
        };
        let u = url::Url::parse(&location).with_context(|| format!("{url}: its location {location}"))?;
        ensure!(matches!(u.scheme(), "s3" | "s3a"), "{url}: {} vends credentials for {location}, which Pondra takes only for s3:// yet: CREATE SECRET … with SCOPE '{location}'", options["table"]);
        let bucket = format!("s3://{}", u.host_str().context("no bucket in the table's location")?);
        let id = hex(&aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, format!("{url}\n{}\n{}\n{location}", options["namespace"], options["table"]).as_bytes()).as_ref()[..12]);
        let made = hex(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, serde_json::to_vec(&params)?.as_slice()).as_ref());
        let root = format!("delegated://{id}");
        let store = stored(&root, &made, || Ok(Arc::new(crate::cache::CachedStore::files(crate::ext::build(&url::Url::parse(&bucket)?, Some(&params), crate::ext::For::Files)?))))?;
        lake.rt.register_object_store(&url::Url::parse(&root)?, store.clone());
        Ok(Some(Delegated { prefix: format!("s3://{}{}", u.host_str().unwrap_or_default(), u.path()), bucket, root, store }))
    }
}

/// A table's location and the store its files are reached through.
#[derive(Clone)]
pub struct Delegated {
    pub prefix: String, // `s3://bucket/path/` (an `s3a://` location named as `s3://`)
    bucket: String,     // `s3://bucket`
    root: String,       // `delegated://<id>`
    store: Arc<dyn object_store_df::ObjectStore>,
}

/// The stores made for delegated roots, by root, with what they were made from: made again only
/// when the credentials change (a store keeps what it read of each file's version).
fn stored(root: &str, made: &str, make: impl FnOnce() -> Result<Arc<dyn object_store_df::ObjectStore>>) -> Result<Arc<dyn object_store_df::ObjectStore>> {
    type Made = HashMap<String, (String, Arc<dyn object_store_df::ObjectStore>)>;
    crate::per_node!(static MADE: Mutex<Option<Made>> = Mutex::new(None));
    if let Some((m, s)) = MADE.lock().unwrap().get_or_insert_default().get(root) {
        if m == made {
            return Ok(s.clone());
        }
    }
    let s = make()?;
    MADE.lock().unwrap().get_or_insert_default().insert(root.to_string(), (made.to_string(), s.clone()));
    Ok(s)
}

/// The settings a store takes, from a catalog's vended S3 credentials (Iceberg's names).
fn vended(c: &Value) -> Option<BTreeMap<String, String>> {
    let s = |k: &str| c[k].as_str().map(str::to_string);
    let mut p = BTreeMap::from([("key_id".to_string(), s("s3.access-key-id")?), ("secret".to_string(), s("s3.secret-access-key")?)]);
    for (from, to) in [("s3.session-token", "session_token"), ("s3.endpoint", "endpoint"), ("s3.region", "region"), ("client.region", "region")] {
        if let Some(v) = s(from) {
            p.entry(to.to_string()).or_insert(v);
        }
    }
    if s("s3.path-style-access").as_deref() != Some("true") && p.contains_key("endpoint") {
        p.insert("url_style".into(), "vhost".into());
    }
    Some(p)
}

tokio::task_local! {
    /// The table being read or written now, and the store its location is reached through.
    static WITHIN: Arc<Delegated>;
}

/// Run `f` (a table's metadata read, or a write to it) with its location reached through `d`.
pub async fn within<F: std::future::Future>(d: Option<Delegated>, f: F) -> F::Output {
    match d {
        Some(d) => WITHIN.scope(Arc::new(d), f).await,
        None => f.await,
    }
}

/// The delegated store for `url`, while `within` its table and under its location.
pub fn scoped(url: &str) -> Option<(Arc<dyn object_store_df::ObjectStore>, object_store_df::path::Path)> {
    let url = url.replacen("s3a://", "s3://", 1);
    WITHIN.try_with(|d| {
        let key = url.strip_prefix(&d.prefix).map(|rest| format!("{}{rest}", &d.prefix[d.bucket.len() + 1..]))?;
        Some((d.store.clone(), object_store_df::path::Path::parse(&key).ok()?))
    }).ok().flatten()
}

/// A table's files under its delegated location, named by its own root (as its deletes are):
/// only these are read through it.
pub fn rehome(d: &Delegated, m: &mut TableMeta) {
    let moved = |p: &mut String| {
        let s = p.replacen("s3a://", "s3://", 1);
        if s.starts_with(&d.prefix) {
            *p = format!("{}{}", d.root, &s[d.bucket.len()..]);
        }
    };
    for f in m.files.iter_mut() {
        moved(&mut f.path);
        for del in f.outside.iter_mut().flat_map(|o| o.deletes.iter_mut()) {
            match del {
                crate::scan::Delete::Vector { path, .. } | crate::scan::Delete::Blob { path, .. } | crate::scan::Delete::Positions { path, .. } | crate::scan::Delete::Equality { path, .. } => moved(path),
                crate::scan::Delete::Inline { .. } => {}
            }
        }
    }
}

/// Is `path` one `rehome` named? (Checked as its catalog's table's, never as a file of its own.)
pub fn is_delegated(path: &str) -> bool { path.starts_with("delegated://") }

/// A node's share of a spread query over a catalog's table: its files' store, from the catalog.
pub async fn prime(lake: &Lake, spec: &Spec, m: &TableMeta) -> Result<()> {
    if !m.files.iter().any(|f: &DataFile| is_delegated(&f.path)) {
        return Ok(());
    }
    let url = spec.urls[0].trim_end_matches('/');
    open(lake, url, &spec.options).await?.load(lake, url, &spec.options).await.map(|_| ())
}

// ---------------------------------------------------------------- AWS

/// The service an AWS catalog signs as.
pub fn service(endpoint_type: &str) -> Result<&'static str> {
    Ok(match endpoint_type {
        "glue" => "glue",
        "s3_tables" => "s3tables",
        t => bail!("ENDPOINT_TYPE {t}: glue or s3_tables"),
    })
}

/// An AWS catalog's endpoint, from its region: the one given, else AWS's.
pub fn aws_endpoint(endpoint_type: &str, region: &str) -> Result<String> {
    Ok(format!("https://{}.{region}.amazonaws.com/iceberg", service(endpoint_type)?))
}

/// An AWS catalog's region: an S3 Tables bucket's ARN says it; else its secret's, else the
/// environment's.
pub fn region(warehouse: &str, keys: &BTreeMap<String, String>) -> Result<String> {
    if let Some(r) = warehouse.strip_prefix("arn:").and_then(|a| a.split(':').nth(2)).filter(|r| !r.is_empty()) {
        return Ok(r.to_string());
    }
    keys.get("region").cloned().or_else(|| std::env::var("AWS_REGION").ok()).or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .context("which AWS region? give the secret one (CREATE SECRET … (TYPE s3, …, REGION 'us-east-1'))")
}

/// The settings of the TYPE s3 secret an AWS catalog was attached with, if this caller may use it.
pub async fn aws_keys(lake: &Lake, name: &str) -> Result<BTreeMap<String, String>> {
    let (n, s) = crate::ext::list(lake).await?.into_iter().find(|(n, _)| n == name).with_context(|| format!("no secret {name}"))?;
    ensure!(crate::ext::usable(&n, &s), "secret {name} isn't yours to use (an admin grants it: GRANT USAGE ON SECRET {name} TO …)");
    ensure!(matches!(s.kind.as_str(), "s3" | "r2"), "secret {name} is TYPE {}: an AWS catalog is reached with a TYPE s3 secret", s.kind);
    crate::ext::reveal_secret(&n, &s)
}

/// The keys to sign with: the secret's, or (PROVIDER credential_chain) the environment's or the
/// machine's role, as the bucket's store finds them.
async fn credential(keys: &BTreeMap<String, String>) -> Result<AwsCredential> {
    if let (Some(k), Some(s)) = (keys.get("key_id"), keys.get("secret")) {
        return Ok(AwsCredential { key_id: k.clone(), secret_key: s.clone(), token: keys.get("session_token").cloned() });
    }
    ensure!(keys.get("provider").map(String::as_str) == Some("credential_chain"), "the AWS catalog's secret has no KEY_ID and SECRET (or PROVIDER credential_chain)");
    let s3 = object_store::aws::AmazonS3Builder::from_env().with_bucket_name("pondra").with_region(keys.get("region").cloned().unwrap_or_else(|| "us-east-1".into())).build()?;
    let c = s3.credentials().get_credential().await?; // (AwsCredential isn't Clone: its three fields are copied)
    Ok(AwsCredential { key_id: c.key_id.clone(), secret_key: c.secret_key.clone(), token: c.token.clone() })
}

fn hex(b: &[u8]) -> String { b.iter().map(|x| format!("{x:02x}")).collect() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regions() {
        let none = BTreeMap::new();
        assert_eq!(region("arn:aws:s3tables:eu-west-1:111122223333:bucket/sales", &none).unwrap(), "eu-west-1");
        let keys = BTreeMap::from([("region".to_string(), "us-east-2".to_string())]);
        assert_eq!(region("123456789012", &keys).unwrap(), "us-east-2");
        assert_eq!(aws_endpoint("s3_tables", "eu-west-1").unwrap(), "https://s3tables.eu-west-1.amazonaws.com/iceberg");
        assert!(aws_endpoint("bigquery", "x").is_err());
    }

    #[test]
    fn vended_settings() {
        let c = serde_json::json!({"s3.access-key-id": "k", "s3.secret-access-key": "s", "s3.session-token": "t", "s3.endpoint": "http://h:1", "s3.path-style-access": "true", "client.region": "eu-west-1"});
        let p = vended(&c).unwrap();
        assert_eq!((p["key_id"].as_str(), p["session_token"].as_str(), p["region"].as_str()), ("k", "t", "eu-west-1"));
        assert!(!p.contains_key("url_style"));
        assert!(vended(&serde_json::json!({"s3.endpoint": "x"})).is_none());
    }

    #[test]
    fn rehomed_only_under_the_location() {
        let store: Arc<dyn object_store_df::ObjectStore> = Arc::new(object_store_df::memory::InMemory::new());
        let d = Delegated { prefix: "s3://wh/sales/orders/".into(), bucket: "s3://wh".into(), root: "delegated://ab12".into(), store };
        let mut m = TableMeta::default();
        m.files = ["s3://wh/sales/orders/data/a.parquet", "s3a://wh/sales/orders/data/b.parquet", "s3://wh/sales/other/data/c.parquet", "s3://wh/sales/orders2/x.parquet"]
            .iter().map(|p| DataFile { path: p.to_string(), ..Default::default() }).collect();
        rehome(&d, &mut m);
        let paths: Vec<&str> = m.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, ["delegated://ab12/sales/orders/data/a.parquet", "delegated://ab12/sales/orders/data/b.parquet", "s3://wh/sales/other/data/c.parquet", "s3://wh/sales/orders2/x.parquet"]);
    }
}
