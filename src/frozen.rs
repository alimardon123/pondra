//! A frozen lake (ADR-070): a backup is a lake that takes writes from its next run alone (`backup.rs`). The catalog keeps
//! the rule, since every write to the lake comes through it (`Catalog::write`, `Lake::put`, the log's rows), whatever
//! door it came in by. The backup decides when a lake is one (it writes `KEY`) and runs its writes as `run`.
use crate::store::Lake;

/// The catalog entry that makes a lake a backup.
pub const KEY: &str = "bk";

tokio::task_local! {
    /// Set while a run commits: the one writer a backup takes everything from.
    static RUN: ();
}

/// Runs `f` as a backup's run: the one writer its catalog takes everything from (`may_write`).
pub async fn run<F: std::future::Future>(f: F) -> F::Output {
    RUN.scope((), f).await
}

/// Is this lake a backup? (Read as it opens, and set by its first run.)
pub fn frozen(lake: &Lake) -> bool { lake.cat.frozen.load(std::sync::atomic::Ordering::Relaxed) }

/// Whether a backup's catalog takes a commit of these keys (`Catalog::write`): a run's, anything;
/// otherwise only what a backup keeps of its own: the pins a clone of it holds, its own keys, its
/// format and its members.
pub fn may_write<'a>(mut keys: impl Iterator<Item = &'a String>) -> bool {
    RUN.try_with(|_| ()).is_ok() || keys.all(|k| k.starts_with("pn/") || matches!(k.as_str(), crate::records::LAKE_KEYS | "format" | "m"))
}

/// What every door says to a write to a backup (25006: read-only).
pub fn refused(url: Option<&str>) -> anyhow::Error {
    let (it, from) = match url {
        Some(u) => (u.to_string(), format!("'{u}'")),
        None => ("this database".to_string(), "'<its place>'".to_string()),
    };
    crate::codes::coded("25006", format!("{it} is a backup, so it is read-only: read it, CLONE it where it is attached, or restore it as a database of its own (RESTORE DATABASE name FROM {from}). Only its next BACKUP DATABASE … TO writes it"))
}
