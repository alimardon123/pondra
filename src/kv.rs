//! The catalog's key-value store, behind one trait: the catalog (`store.rs`) keeps its keys here and names no
//! other store. On a node it is SlateDB (`kv::slate`); on a page it is a log in the lake's own store (`kv::page`,
//! designs/page-same-core.md).
use bytes::Bytes;
use futures::future::BoxFuture;
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod slate;

#[cfg(any(target_arch = "wasm32", test))]
pub(crate) mod page;

pub(crate) mod remote;

/// The catalog's store as its leader opens it: SlateDB's writer on a node.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn writer(store: crate::store::Store, cache_dir: Option<std::path::PathBuf>, local: bool, node: Arc<crate::node_state::NodeState>) -> Result<Arc<dyn Kv>, Error> {
    Ok(Arc::new(slate::Writer::open(store, cache_dir, local, node).await?))
}

/// The catalog's store as a follower or a reader opens it: SlateDB's reader on a node.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn reader(store: crate::store::Store, streamed: bool, cache_dir: Option<std::path::PathBuf>, poll: std::time::Duration) -> Result<Arc<dyn Kv>, Error> {
    Ok(Arc::new(slate::Reader::open(store, streamed, cache_dir, poll).await?))
}

/// The catalog's store as its leader opens it on a page: a `page::Log` in the lake's own store (designs/page-same-core.md P2).
#[cfg(target_arch = "wasm32")]
pub(crate) async fn writer(store: crate::store::Store, _: Option<std::path::PathBuf>, _: bool, _: Arc<crate::node_state::NodeState>) -> Result<Arc<dyn Kv>, Error> {
    Ok(Arc::new(page::Log::writer(store).await?))
}

/// The catalog's store as a follower or a reader opens it on a page: the same log, read only.
#[cfg(target_arch = "wasm32")]
pub(crate) async fn reader(store: crate::store::Store, _: bool, _: Option<std::path::PathBuf>, poll: std::time::Duration) -> Result<Arc<dyn Kv>, Error> {
    Ok(Arc::new(page::Log::reader(store, poll).await?))
}

/// The catalog's ordered key-value store: SlateDB on a node (`kv::slate`), a page's log in the lake's store (`kv::page`).
/// What its block caches hold is what it says it holds (`ledger::Charged`: `/metrics`' `catalog_blocks`).
#[async_trait::async_trait]
pub(crate) trait Kv: crate::ledger::Charged {
    /// A key's value. `remote`: only what the bucket holds (a writer's memtable left out, as `DurabilityLevel::Remote`).
    async fn get(&self, key: &str, remote: bool) -> Result<Option<Bytes>, Error>;
    /// The keys in `range`, in order, with their values; `remote` as for `get`.
    async fn scan(&self, range: std::ops::Range<Vec<u8>>, remote: bool) -> Result<BTreeMap<String, Bytes>, Error>;
    /// The keys in `range` as `scan` reads them, for a read of most of the store at once (a node's catalog in memory as
    /// it starts, `Catalog::seed`, and the records kept with its entries, `Catalog::keep_records`): read in large pieces,
    /// several at a time, where `scan` reads a block a request.
    async fn scan_whole(&self, range: std::ops::Range<Vec<u8>>) -> Result<BTreeMap<String, Bytes>, Error> { self.scan(range, false).await }
    /// Does this store take writes (a leader's), or only read (a follower's view of the bucket)?
    fn is_writer(&self) -> bool;
    /// One atomic batch: the puts, then the deletes (a key both put and deleted ends deleted). The returned handle
    /// says when it is durable.
    async fn write(&self, puts: &[(String, Bytes)], deletes: &[String]) -> Result<Arc<dyn Written>, Error>;
    /// Writes what the memtable holds into the bucket.
    async fn checkpoint(&self) -> Result<(), Error>;
    /// A writer's level-0 files waiting for compaction (0: none, or a store without levels).
    fn waiting(&self) -> usize { 0 }
    /// A writer that lives for one statement (`pondra sql`): its compactor starts now, and this waits, `cap` at
    /// most, until fewer than `below` level-0 files wait (none of its own runs long enough to start it alone).
    async fn compact(&self, _below: usize, _cap: std::time::Duration) -> Result<(), Error> { Ok(()) }
    /// Its garbage collector once, now: old WAL, manifests and compacted files that no reader needs.
    async fn collect(&self) {}
    async fn close(&self) -> Result<(), Error>;
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
