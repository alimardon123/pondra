//! Where another engine's files are reached from (ADR-026's readers, in the core): the store a URL is in, and a
//! session to read Parquet with. A node's are its lake's (its secrets, a catalog's delegated store: `ext::store`);
//! a page's are what its program gave it and the web.
use anyhow::{Context, Result};
use bytes::Bytes;
use datafusion::object_store::{path::Path, Error as StoreError, ObjectStore, ObjectStoreExt};
use datafusion::prelude::SessionContext;
use std::future::Future;
use std::sync::Arc;

pub trait Reach: Sync {
    /// The store `url` is in, and the object's path there.
    fn store(&self, url: &str) -> impl Future<Output = Result<(Arc<dyn ObjectStore>, Path)>> + Send;
    /// A session to read Parquet with (a checkpoint's actions).
    fn session(&self) -> SessionContext;
}

/// An object's bytes (a log's commit, a manifest), through its store. `Ok(None)`: no object there.
pub async fn found(reach: &impl Reach, url: &str) -> Result<Option<Bytes>> {
    let (store, path) = reach.store(url).await?;
    match store.get(&path).await {
        Ok(got) => Ok(Some(got.bytes().await.with_context(|| format!("reading {url}"))?)),
        // (a missing object is an answer, not a failure: the caller decides what its absence means)
        Err(StoreError::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {url}")),
    }
}

/// An object's bytes; none there is an error that names `url`.
pub async fn get(reach: &impl Reach, url: &str) -> Result<Bytes> {
    found(reach, url).await?.with_context(|| format!("reading {url}: not found"))
}
