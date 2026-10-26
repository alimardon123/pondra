//! The catalog's key-value store, behind one trait: the catalog (`store.rs`) keeps its keys here and names no
//! other store. On a node it is SlateDB (`kv::slate`); a page gives its own later (designs/page-same-core.md).
use bytes::Bytes;
use futures::future::BoxFuture;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) mod slate;

/// The catalog's ordered key-value store: SlateDB on a node (`kv::slate`); a page's own later (designs/page-same-core.md).
#[async_trait::async_trait]
pub(crate) trait Kv: Send + Sync {
    /// A key's value. `remote`: only what the bucket holds (a writer's memtable left out, as `DurabilityLevel::Remote`).
    async fn get(&self, key: &str, remote: bool) -> Result<Option<Bytes>, Error>;
    /// The keys in `range`, in order, with their values; `remote` as for `get`.
    async fn scan(&self, range: std::ops::Range<Vec<u8>>, remote: bool) -> Result<BTreeMap<String, Bytes>, Error>;
    /// Does this store take writes (a leader's), or only read (a follower's view of the bucket)?
    fn is_writer(&self) -> bool;
    /// One atomic batch: the puts, then the deletes (a key both put and deleted ends deleted). The returned handle
    /// says when it is durable.
    async fn write(&self, puts: &[(String, Bytes)], deletes: &[String]) -> Result<Arc<dyn Written>, Error>;
    /// Writes what the memtable holds into the bucket.
    async fn checkpoint(&self) -> Result<(), Error>;
    async fn close(&self) -> Result<(), Error>;
    /// The bytes its block caches hold (`/metrics`).
    fn cached(&self) -> usize;
}

/// One batch's handle: `durable` resolves once the batch is in the bucket.
pub(crate) trait Written: Send + Sync {
    fn durable(&self) -> BoxFuture<'_, Result<(), Error>>;
}

/// A store's error. Its words are the source's: `Display` and `source` are the store's own, so every log line and
/// every check that greps one stays the same. `closed` is the store saying it was closed (fenced out by a newer
/// writer, or closed under us): a writer that sees it restarts (`store::fatal`).
#[derive(Debug)]
pub(crate) struct Error {
    closed: bool,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl Error {
    pub(crate) fn new(closed: bool, source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Error { closed, source: source.into() }
    }

    /// Whether the store was closed: fenced out by a newer writer, or closed under us.
    pub(crate) fn closed(&self) -> bool {
        self.closed
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.source, f)
    }
}

// (transparent: an error chain reads as the store's own would, one link for its words and one per source)
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}
