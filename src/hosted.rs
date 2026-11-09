//! What the hosts give the floors below (redesign step 5, piece 8g): the platform's implementations, which only a host may
//! name. Every form is a host of this crate's (the program, the library, the page), so each registers them alike here.
//! - The buckets a lake's store opens (`buckets::Buckets`): the clouds' clients, each request a turn of its bucket's
//!   budget (`budget.rs`, C5), with a secret's settings when its reach names one (`ext::build`); a node's door
//!   (`pondra://`, `served.rs`); a branch's base's share of its bucket's turns; DataFusion's reader of a lake in memory
//!   (`bridge.rs`).
use crate::buckets::{At, Store};
use anyhow::Result;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::ClientConfigKey;
use std::collections::BTreeMap;
use std::sync::Arc;

pub static HOSTED: crate::registry::BuiltIn = crate::registry::BuiltIn {
    name: "hosted",
    requires: &[],
    register: |r| {
        let b = r.section_mut::<crate::buckets::Buckets>();
        b.open = Some(bucket);
        #[cfg(feature = "doors")] // (a page reads no lake through a node's door: it joins the node instead)
        {
            b.served = Some(|at| Arc::new(crate::served::Served::new(at)) as Store);
        }
        b.share = Some(crate::budget::share);
        b.reader = Some(|s| Arc::new(crate::bridge::Bridge(s)) as Arc<dyn object_store_df::ObjectStore>);
    },
};

/// A lake's whole bucket: with a secret's settings when its reach names one, else as the environment says (`AWS_*`, with
/// `AWS_ENDPOINT` for R2 and MinIO; `GOOGLE_*`; `AZURE_*`).
fn bucket(at: &At, params: Option<&BTreeMap<String, String>>) -> Result<Store> {
    // Idle connections are dropped after 15 s rather than reused: through proxies and NATs that
    // silently forget idle connections, a reused one hung a PUT for the full 30 s timeout on R2.
    let (idle, after) = (ClientConfigKey::PoolIdleTimeout, "15s");
    // Every request to the bucket takes a turn of its budget, retries too (`budget.rs`, C5).
    let turns = || crate::budget::Budget::of(at.root);
    Ok(match (params, at.scheme) {
        (Some(_), _) => crate::ext::build(&url::Url::parse(at.root)?, params, crate::ext::For::Lake)?,
        (None, "s3") => Arc::new(crate::store::with_kms(AmazonS3Builder::from_env(), crate::store::sse_kms_from_env()).with_bucket_name(at.bucket).with_allow_http(true).with_config(AmazonS3ConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
        (None, "gs") => Arc::new(GoogleCloudStorageBuilder::from_env().with_bucket_name(at.bucket).with_config(GoogleConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
        (None, _) => Arc::new(MicrosoftAzureBuilder::from_env().with_url(at.root).with_config(AzureConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
    })
}

#[cfg(test)]
mod tests {
    /// Every form registers the platform's stores: a bucket, a branch base's share and DataFusion's reader, and (a node's
    /// forms) a node's door.
    #[test]
    fn the_hosts_give_the_lake_its_buckets() {
        let b = crate::buckets::get();
        assert!(b.open.is_some() && b.share.is_some() && b.reader.is_some());
        #[cfg(feature = "doors")]
        assert!(b.served.is_some());
    }
}
