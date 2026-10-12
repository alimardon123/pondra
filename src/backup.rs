//! Backups (ADR-070): a backup is a lake. `BACKUP DATABASE [name] TO 'url'` pins the database,
//! then the backup's own leader (a `pondra lead` of its own when nobody leads it, as a branch's
//! first commit is made) reads the database's catalog at a version that holds the pin, copies every
//! object that version names and the backup doesn't have yet, and writes the whole catalog in one
//! commit. Run again, it copies only what is new; what a run no longer names stays for the
//! backup's retention, so a restore reads any run it keeps. A backup is read-only to everything
//! but its next run (`frozen`); `RESTORE DATABASE` (or `pondra restore`) copies a run into a lake
//! of its own.

use crate::ddl::Ddl;
use crate::store::{json, DataFile, Lake, Segment, Store, TableMeta};
use crate::write::Request;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::{BTreeMap, BTreeSet};

/// The catalog entry that makes a lake a backup.
pub const KEY: &str = "bk";
/// Where each run's catalog is kept (a restore of an earlier run reads it), and the mark of a run
/// under way.
const RUNS: &str = "_backups";
const DAY_MS: u64 = 86_400_000;

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Backup {
    pub source: String,
    pub retention_days: u64,
    pub runs: Vec<Run>,
    /// Objects the newest run no longer names, and since when: deleted once past the retention
    /// (and older than every pin in the backup: a clone of it may read them).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub garbage: Vec<(String, u64)>,
}

/// One run: when, the source's commit it read, its catalog's object, and what it copied.
#[derive(Serialize, Deserialize, Clone)]
pub struct Run {
    pub ms: u64,
    pub commit: u64,
    pub next: u64,
    pub block: u64,
    pub catalog: String,
    pub tables: u64,
    pub objects: u64,
    pub bytes: u64,
    pub copied: u64,
    pub copied_bytes: u64,
}

/// What a lake's leader is asked to do: take a run of `source` (pinned there as `pin`) into this
/// backup, or (`restore`) make this new lake the backup at `source` as of its newest run at or
/// before `at` (None: its newest).
#[derive(Serialize, Deserialize, Clone)]
pub struct Take {
    pub source: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_days: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub restore: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<u64>,
}

/// Keys that are each lake's own, never carried from one to another: its counters, format and
/// members, its own keys (a backup never holds the key its source's nodes sign each other in with),
/// a branch's bases and a backup's entry, an apply under way, the pins others hold in it, and what
/// it published to Delta and Iceberg (a restored lake publishes anew).
fn own(k: &str) -> bool {
    matches!(k, "c" | "n" | "b" | "m" | "format" | "dl" | KEY | crate::users::KEYS | crate::store::QUIET | crate::branch::BASES)
        || ["pn/", "x/", "i/"].iter().any(|p| k.starts_with(p))
}

tokio::task_local! {
    /// Set while a run commits: the one writer a backup takes everything from.
    static RUN: ();
}

/// Is this lake a backup? (Read as it opens, and set by its first run.)
pub fn frozen(lake: &Lake) -> bool { lake.cat.frozen.load(std::sync::atomic::Ordering::Relaxed) }

/// Whether a backup's catalog takes a commit of these keys (`Catalog::write`): a run's, anything;
/// otherwise only what a backup keeps of its own: the pins a clone of it holds, its own keys, its
/// format and its members.
pub fn may_write<'a>(mut keys: impl Iterator<Item = &'a String>) -> bool {
    RUN.try_with(|_| ()).is_ok() || keys.all(|k| k.starts_with("pn/") || matches!(k.as_str(), crate::users::KEYS | "format" | "m"))
}

/// What every door says to a write to a backup (25006: read-only).
pub fn refused(url: Option<&str>) -> anyhow::Error {
    let (it, from) = match url {
        Some(u) => (u.to_string(), format!("'{u}'")),
        None => ("this database".to_string(), "'<its place>'".to_string()),
    };
    crate::codes::coded("25006", format!("{it} is a backup, so it is read-only: read it, CLONE it where it is attached, or restore it as a database of its own (RESTORE DATABASE name FROM {from}). Only its next BACKUP DATABASE … TO writes it"))
}

/// What a backup's leader still takes: a run, pins (a clone of it, a backup of it), and a backup of
/// it somewhere else.
pub fn allowed(req: &Request) -> bool {
    matches!(req, Request::Ddl(Ddl::Backup(_) | Ddl::Pin { .. } | Ddl::Unpin { .. } | Ddl::BackupDatabase { .. }))
}

/// A statement a backup's nodes take: one that only reads it (COPY … TO files outside it, a backup
/// of it somewhere else).
pub fn reads_only(stmt: &crate::write::Stmt) -> bool {
    use crate::write::Stmt;
    matches!(stmt, Stmt::CopyTo(..)) || matches!(stmt, Stmt::Ddl(ds) if ds.iter().all(|d| matches!(d, Ddl::BackupDatabase { .. })))
}

/// A backup's leader, in place of a tiering round: its tables never change, so only its clones'
/// pins are kept as a lake keeps them, hourly: one on lease that wasn't renewed goes, and one whose
/// clone is gone (`branch::lapse`, `sweep`).
pub async fn tend(lake: &Lake) -> Result<()> {
    crate::per_node!(static LOOKED: std::sync::Mutex<u64> = std::sync::Mutex::new(0));
    let now = crate::log::now_ms();
    {
        let mut looked = LOOKED.lock().unwrap();
        if *looked + 3_600_000 > now {
            return Ok(());
        }
        *looked = now;
    }
    crate::branch::lapse(lake).await?;
    crate::branch::sweep(lake).await
}

// ---------------------------------------------------------------- the statements

/// BACKUP and RESTORE DATABASE, on the leader of the database they were sent to, beside its lock:
/// a run takes long, and only a pin and an attachment take the lock, for a moment.
pub async fn statement(lake: &Lake, lock: &tokio::sync::Mutex<()>, d: Ddl) -> Result<Value> {
    match d {
        Ddl::BackupDatabase { database, to, retention_days } => backup(lake, lock, database.as_deref(), &to, retention_days).await,
        Ddl::RestoreDatabase { name, from, dir, at } => {
            crate::ddl::attachable(lake, &name, true).await?; // (before anything is copied)
            let to = crate::ddl::full(&dir.unwrap_or_else(|| crate::ddl::beside(&lake.url, &name)))?;
            apart(lake, &to)?;
            for place in [&crate::ddl::full(&from)?, &to] {
                crate::ext::reach_place(lake, place).await?; // (a backup in another bucket: the secret that reaches it)
            }
            let mut out = restore(&from, &to, at).await?;
            let _guard = lock.lock().await;
            crate::ddl::apply(lake, Ddl::Attach { name: name.clone(), dir: to, read_only: false, endpoint: None }).await?;
            out["attached"] = name.into();
            Ok(out)
        }
        _ => anyhow::bail!("only BACKUP and RESTORE DATABASE are run beside the lock"),
    }
}

/// A backup or a restored lake goes in a place of its own: never inside this lake or one attached
/// here, nor around one (their own collection of what they don't name would take its objects). A
/// backup attached here, to be read, takes its next run.
fn apart(lake: &Lake, to: &str) -> Result<()> {
    let mut lakes = vec![lake.arc()];
    lakes.extend(lake.attached.read().unwrap().iter().map(|(_, l)| l.clone()));
    for l in lakes.iter().filter(|l| !(l.url == to && frozen(l))) {
        ensure!(to != l.url && !to.starts_with(&format!("{}/", l.url)), "{to} is inside the database at {}: put it in a place of its own", l.url);
        ensure!(!l.url.starts_with(&format!("{to}/")), "{to} holds the database at {}: put it in a place of its own", l.url);
    }
    Ok(())
}

/// `BACKUP DATABASE [name] TO 'url'`: the database pinned (by its leader: this one, or an attached
/// database's), the run asked of the backup's leader, the pin let go.
async fn backup(lake: &Lake, lock: &tokio::sync::Mutex<()>, database: Option<&str>, to: &str, retention_days: Option<u64>) -> Result<Value> {
    let source = match database.map(|n| n.trim_matches('"').to_lowercase()) {
        None => lake.arc(),
        Some(n) if n == crate::ddl::lake_name(lake) => lake.arc(),
        Some(n) => {
            let other = lake.attached.read().unwrap().iter().find(|(a, _)| *a == n).map(|(_, l)| l.clone());
            let other = other.with_context(|| format!("no database {n} here: a backup is of this database or one attached to it"))?;
            crate::users::across(&other, &n).await?; // (another database's own sign-in: invariant 240)
            other
        }
    };
    ensure!(crate::store::reach_of(&source.url).is_none(), "{} is on another server: back it up from there", source.url);
    ensure!(retention_days.is_none_or(|d| d > 0), "retention_days is at least 1");
    let to = crate::ddl::full(to)?;
    apart(lake, &to)?;
    crate::ext::reach_place(lake, &to).await?; // (another bucket or provider: the secret whose SCOPE covers it)
    ensure!(crate::ddl::has_catalog(&to).await? || empty(&to).await?, "{to} holds other things: back up into a new place (or an empty folder)");
    let pin = format!("{to}#backup");
    let take = Take { source: source.url.clone(), pin: pin.clone(), retention_days, restore: false, at: None };
    // (this lake pinned here, under its lock: its leader may be this process alone, a `pondra sql`
    // leading for a moment, which nothing else could reach)
    let here = source.url == lake.url;
    let ask = |d: Ddl| async {
        match here {
            true => {
                let _guard = lock.lock().await;
                crate::ddl::apply(lake, d).await
            }
            false => Box::pin(crate::write::send(&source.url, Request::Ddl(d))).await,
        }
    };
    let first = ask(Ddl::Pin { lake: pin.clone(), ms: None, lease: false }).await.with_context(|| format!("pinning {}", source.url))?;
    let ms = first["ms"].as_u64();
    let renew = async {
        // (a pin whose lake isn't there is let go after an hour: `branch::sweep`)
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(20 * 60)).await;
            if let Err(e) = ask(Ddl::Pin { lake: pin.clone(), ms, lease: false }).await {
                eprintln!("renewing the pin {pin} in {}: {e:#}", source.url);
            }
        }
    };
    let out = tokio::select! {
        out = Box::pin(crate::write::send(&to, Request::Ddl(Ddl::Backup(take)))) => out,
        _ = renew => unreachable!(),
    };
    if let Err(e) = ask(Ddl::Unpin { lake: pin.clone() }).await {
        eprintln!("letting go of the pin {pin} in {}: {e:#} (its hourly sweep lets it go)", source.url);
    }
    out.with_context(|| format!("backing up {} to {to}", source.url))
}

/// `RESTORE DATABASE name FROM 'url' [AT (TIMESTAMP => …)]` and `pondra restore`: a new lake at
/// `to`, made by its own leader from the backup's run. Only read from the backup: a backup on
/// storage that this machine may only read can be restored.
pub async fn restore(from: &str, to: &str, at: Option<u64>) -> Result<Value> {
    let (from, to) = (crate::ddl::full(from)?, crate::ddl::full(to)?);
    ensure!(crate::ddl::has_catalog(&from).await?, "{from} holds no lake");
    ensure!(!crate::ddl::has_catalog(&to).await?, "a lake is at {to} already: restore into a new place");
    ensure!(empty(&to).await?, "{to} holds other things: restore into a new place (or an empty folder)");
    let take = Take { source: from, pin: String::new(), retention_days: None, restore: true, at };
    let out = Box::pin(crate::write::send(&to, Request::Ddl(Ddl::Backup(take)))).await;
    if out.is_err() {
        let _ = crate::ddl::delete_lake(&to).await; // (nothing half made stays)
    }
    out
}

/// Nothing at `dir` yet (a restore that fails takes away what it made there, and only that).
async fn empty(dir: &str) -> Result<bool> {
    if !dir.contains("://") {
        let path = std::path::Path::new(dir);
        return Ok(!path.exists() || std::fs::read_dir(path)?.next().is_none());
    }
    let store = crate::store::open_store(dir)?.1;
    Ok(store.list(None).next().await.transpose()?.is_none())
}

/// A time a restore names: RFC 3339, `YYYY-MM-DD[ HH:MM:SS[.fff]]` (UTC), or milliseconds.
pub fn instant(s: &str) -> Result<u64> {
    let s = s.trim().trim_matches('\'');
    if let Ok(ms) = s.parse::<u64>() {
        return Ok(ms);
    }
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(t.timestamp_millis() as u64);
    }
    for f in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M"] {
        if let Ok(t) = chrono::NaiveDateTime::parse_from_str(s, f) {
            return Ok(t.and_utc().timestamp_millis() as u64);
        }
    }
    let day = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").with_context(|| format!("{s}: a time is '2026-10-12 03:00:00' (UTC), RFC 3339 or milliseconds"))?;
    Ok(day.and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp_millis() as u64)
}

// ---------------------------------------------------------------- the backup's leader

/// The backup's (or the restored lake's) leader: take the run.
pub async fn take(lake: &Lake, t: Take) -> Result<Value> {
    let mine = lake.cat.scan_raw("", "\x7f").await?;
    let tables = mine.keys().any(|k| k.starts_with("t/"));
    let kept: Option<Backup> = mine.get(KEY).map(|v| serde_json::from_slice(v)).transpose()?;
    if t.restore {
        ensure!(!tables && kept.is_none(), "a lake is at {} already: restore into a new place", lake.url);
        return restore_into(lake, &t).await;
    }
    let target = lake.store.clone();
    let running = Path::from(format!("{RUNS}/running"));
    let resuming = target.head(&running).await.is_ok();
    match &kept {
        Some(b) => ensure!(b.source == t.source, "{} is a backup of {}: back {} up somewhere else", lake.url, b.source, t.source),
        // (a lake made for this backup holds only what every lake makes of its own, and what a first
        // run stopped half way copied; a lake with anything of its own would lose it)
        None => ensure!(
            mine.keys().all(|k| own(k)) && (resuming || target.list(Some(&Path::from("files"))).next().await.is_none()),
            "a lake that isn't a backup is at {}: back up into a new place",
            lake.url
        ),
    }
    let source = Lake::open(&t.source, false, false).await.with_context(|| format!("opening {}", t.source))?;
    let snap = crate::branch::snapshot(&source, &crate::branch::pin_key(&t.pin), 0).await?;
    let number = |k: &str| snap.get(k).and_then(|v| serde_json::from_slice::<u64>(v).ok()).unwrap_or(0);
    let (commit, next, block) = (number("c"), number("n").max(1), number("b"));
    // The source's catalog as the backup keeps it (a branch's base's files under `data/_bases/`),
    // and the objects it names: where each goes, and where it is in the source.
    let (cat, wanted, written) = placed(&source, &snap, &target).await?;
    let mut b = kept.unwrap_or_default();
    let have = paths(lake, &mine).await?;
    let held: BTreeSet<&String> = have.keys().chain(b.garbage.iter().map(|(p, _)| p)).collect();
    let mut copying: Vec<(String, String, Wanted)> = wanted.iter().filter(|(to, _)| !held.contains(to)).map(|(to, (from, w))| (to.clone(), from.clone(), *w)).collect();
    target.put(&running, Bytes::new().into()).await?;
    // The log's objects first: tiering lets them go once their rows are in files, which a pin
    // doesn't hold back, so they are copied while they are surely there; files the pin keeps.
    copying.sort_by_key(|(_, _, w)| !w.log);
    let (copied, copied_bytes) = copy_all(&source, &target, copying, resuming).await?;
    let (files_copied, files_bytes) = workspace(&source, &target).await?;
    // Each run's catalog, as an object: a restore of an earlier run reads it.
    let ms = crate::log::now_ms();
    let catalog = format!("{RUNS}/{ms:013}-{commit}.json.zst");
    let packed: BTreeMap<&String, String> = cat.iter().map(|(k, v)| (k, B64.encode(v))).collect();
    put_new(&target, &catalog, zstd::encode_all(&json(&packed)[..], 3)?.into()).await?; // (a run's catalog: named by its time and commit)
    // One commit: the catalog as the source had it, what the backup named before and no longer
    // does (garbage, kept for the retention), the runs kept.
    b.source = t.source.clone();
    b.retention_days = t.retention_days.unwrap_or(if b.retention_days == 0 { 7 } else { b.retention_days });
    let names: BTreeSet<&String> = wanted.keys().chain(written.iter()).collect();
    b.garbage.retain(|(p, _)| !names.contains(p)); // (named again: a RESTORE TABLE of the source)
    let known: BTreeSet<String> = b.garbage.iter().map(|(p, _)| p.clone()).collect();
    b.garbage.extend(have.keys().filter(|p| !names.contains(p) && !known.contains(*p)).map(|p| (p.clone(), ms)));
    let keep_ms = b.retention_days * DAY_MS;
    let pins = crate::branch::pins(lake).await?;
    let (gone, garbage): (Vec<_>, Vec<_>) = b.garbage.drain(..).partition(|(_, at)| ms >= at + keep_ms && pins.is_none_or(|p| *at < p.0));
    b.garbage = garbage;
    let (old, runs): (Vec<Run>, Vec<Run>) = b.runs.drain(..).partition(|r| ms >= r.ms + keep_ms && pins.is_none_or(|p| r.ms < p.0));
    b.runs = runs;
    let objects = (wanted.len() + written.len()) as u64;
    let bytes = wanted.values().map(|(_, w)| w.bytes).sum::<u64>();
    let tables = cat.keys().filter(|k| k.starts_with("t/") && !k.contains('$')).count() as u64; // (not the hidden `{t}$deleted`)
    b.runs.push(Run { ms, commit, next, block, catalog, tables, objects, bytes, copied: copied + files_copied, copied_bytes: copied_bytes + files_bytes });
    let mut puts: Vec<(String, Vec<u8>)> = cat.iter().map(|(k, v)| (k.clone(), v.to_vec())).collect();
    let n = |k: &str| mine.get(k).and_then(|v| serde_json::from_slice::<u64>(v).ok()).unwrap_or(0);
    puts.push(("n".into(), json(&next.max(n("n")))));
    puts.push(("b".into(), json(&block.max(n("b")))));
    // (its format is the newest of its runs': a release that can't read what a run holds refuses it)
    let format = |c: &BTreeMap<String, Bytes>| c.get("format").and_then(|v| serde_json::from_slice::<crate::format::Stamp>(v).ok()).unwrap_or_default();
    if format(&snap).format > format(&mine).format {
        puts.push(("format".into(), json(&format(&snap))));
    }
    puts.push((KEY.into(), json(&b)));
    let deletes: Vec<String> = mine.keys().filter(|k| !own(k) && !cat.contains_key(*k)).cloned().collect();
    lake.cat.start_after(commit).await;
    RUN.scope((), lake.cat.commit(puts, &deletes)).await?;
    lake.cat.frozen.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = target.delete(&running).await;
    // What the retention let go, after the commit that stopped naming it. (A run stopped half way
    // leaves objects nothing names, which no later run knows of: they cost room, never a read.)
    let dead = gone.into_iter().map(|(p, _)| p).chain(old.into_iter().map(|r| r.catalog));
    futures::stream::iter(dead).for_each_concurrent(16, |p| { let target = target.clone(); async move { let _ = target.delete(&Path::from(p)).await; } }).await;
    let run = b.runs.last().expect("this run");
    Ok(j!({"backup": lake.url, "of": t.source, "run": when(run.ms), "tables": run.tables, "objects": run.objects, "bytes": run.bytes, "copied": run.copied, "copied_bytes": run.copied_bytes, "runs": b.runs.len()}))
}

/// A new lake's leader: the backup at `t.source` as of its newest run at or before `t.at`, copied
/// in. Its tasks are suspended (nothing a copy runs reaches the outside on its own: ALTER TASK …
/// RESUME), and it publishes to Delta and Iceberg anew.
async fn restore_into(lake: &Lake, t: &Take) -> Result<Value> {
    let backup = Lake::open(&t.source, false, false).await.with_context(|| format!("opening {}", t.source))?;
    let b: Backup = backup.cat.get(KEY).await?.with_context(|| format!("{} is not a backup (BACKUP DATABASE … TO makes one)", t.source))?;
    let run = b.runs.iter().rev().find(|r| t.at.is_none_or(|at| r.ms <= at)).with_context(|| {
        format!("{} has no run then: its first kept is {}", t.source, b.runs.first().map(|r| when(r.ms)).unwrap_or_default())
    })?;
    let object = backup.store.get(&Path::from(run.catalog.as_str())).await.with_context(|| format!("{} of {}", run.catalog, t.source))?;
    let packed: BTreeMap<String, String> = serde_json::from_slice(&zstd::decode_all(&object.bytes().await?[..])?)?;
    let mut cat: BTreeMap<String, Bytes> = packed.into_iter().map(|(k, v)| Ok((k, Bytes::from(B64.decode(v)?)))).collect::<Result<_>>()?;
    let mut suspended = vec![];
    for (k, v) in cat.iter_mut().filter(|(k, _)| k.starts_with("j/")) {
        let mut task: crate::runs::Task = serde_json::from_slice(v).with_context(|| k.clone())?;
        if !task.suspended {
            task.suspended = true;
            suspended.push(k[2..].to_string());
            *v = Bytes::from(json(&task));
        }
    }
    let wanted = paths(&backup, &cat).await?;
    let target = lake.store.clone();
    let mut copying: Vec<(String, String, Wanted)> = wanted.into_iter().map(|(p, w)| (p.clone(), p, w)).collect();
    copying.sort_by_key(|(_, _, w)| !w.log);
    let (copied, copied_bytes) = copy_all(&backup, &target, copying, false).await.with_context(|| format!("restoring {} (a run past the backup's retention lets its files go)", when(run.ms)))?;
    let (files, files_bytes) = workspace(&backup, &target).await?;
    let mut puts: Vec<(String, Vec<u8>)> = cat.iter().map(|(k, v)| (k.clone(), v.to_vec())).collect();
    puts.push(("n".into(), json(&run.next)));
    puts.push(("b".into(), json(&run.block)));
    if let Some(f) = backup.cat.get::<crate::format::Stamp>("format").await? {
        puts.push(("format".into(), json(&f)));
    }
    lake.cat.start_after(run.commit).await;
    lake.cat.commit(puts, &[]).await?;
    let mut out = j!({"restored": lake.url, "from": t.source, "run": when(run.ms), "tables": run.tables, "copied": copied + files, "copied_bytes": copied_bytes + files_bytes});
    if !suspended.is_empty() {
        out["notice"] = format!("tasks suspended, as in a backup: {} (ALTER TASK name RESUME, once this copy should run them)", suspended.join(", ")).into();
        out["suspended"] = j!(suspended);
    }
    Ok(out)
}

fn when(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64).map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)).unwrap_or_default()
}

// ---------------------------------------------------------------- what a catalog names

/// An object a catalog names: its size (0 when not known); whether a read needs it (a file only a
/// log segment kept for the change feed names may be gone already); and whether it is the log's.
#[derive(Clone, Copy)]
struct Wanted {
    bytes: u64,
    needed: bool,
    log: bool,
}

const FILE: Wanted = Wanted { bytes: 0, needed: true, log: false };

/// Every object `cat` names, by its path in `lake` (its manifests read through it).
async fn paths(lake: &Lake, cat: &BTreeMap<String, Bytes>) -> Result<BTreeMap<String, Wanted>> {
    let mut out = BTreeMap::new();
    let all = tables(cat)?;
    for meta in &all {
        for (p, w) in table_objects(lake, meta).await? {
            out.insert(p, w);
        }
    }
    let floor = all.iter().map(|m| m.tiered).min().unwrap_or(0);
    for (k, v) in cat.range("s/".to_string().."s0".to_string()) {
        let seg: Segment = serde_json::from_slice(v).with_context(|| k.clone())?;
        segment_objects(&seg, k[2..].parse::<u64>().unwrap_or(0) > floor, &mut out);
    }
    Ok(out)
}

/// Every table entry of a catalog: the live ones, and those kept to be undropped.
fn tables(cat: &BTreeMap<String, Bytes>) -> Result<Vec<TableMeta>> {
    let mut out = vec![];
    for (k, v) in cat.range("dt/".to_string().."dt0".to_string()) {
        let d: crate::ddl::Dropped = serde_json::from_slice(v).with_context(|| k.clone())?;
        out.push(d.meta);
        out.extend(d.deleted);
    }
    for (k, v) in cat.range("t/".to_string().."t0".to_string()) {
        out.push(serde_json::from_slice(v).with_context(|| k.clone())?);
    }
    Ok(out)
}

async fn table_objects(lake: &Lake, meta: &TableMeta) -> Result<Vec<(String, Wanted)>> {
    let mut out = vec![];
    if let Some(s) = &meta.sealed {
        out.push((s.list.clone(), FILE));
    }
    let mut files = meta.files.clone();
    for m in crate::manifest::list(lake, meta).await? {
        files.extend(crate::manifest::files(lake, &m).await?);
        out.push((m.path, FILE));
    }
    for f in &files {
        file_objects(f, FILE, &mut out);
    }
    Ok(out)
}

fn file_objects(f: &DataFile, w: Wanted, out: &mut impl Extend<(String, Wanted)>) {
    use crate::scan::Delete;
    out.extend([(f.path.clone(), Wanted { bytes: f.bytes, ..w })]);
    out.extend(f.deletes.iter().filter_map(|d| match d {
        Delete::Positions { path, bytes, .. } => Some((path.clone(), Wanted { bytes: *bytes, ..w })),
        Delete::Vector { path, .. } | Delete::Blob { path, .. } | Delete::Equality { path, .. } => Some((path.clone(), w)),
        Delete::Inline { .. } => None,
    }));
}

/// A segment's objects: its rows (when not inline: needed while a table hasn't tiered past it), and
/// the files a file commit named, which only the log's readers read.
fn segment_objects(seg: &Segment, needed: bool, out: &mut BTreeMap<String, Wanted>) {
    if !seg.path.is_empty() {
        out.insert(seg.path.clone(), Wanted { bytes: 0, needed, log: true });
    }
    for filed in seg.files.values() {
        let mut v = vec![];
        for f in filed.added.iter().chain(&filed.removed).chain(filed.deleted.iter().map(|(f, _)| f)) {
            file_objects(f, Wanted { bytes: 0, needed: false, log: false }, &mut v);
        }
        for (p, w) in v {
            out.entry(p).or_insert(w);
        }
    }
}

// ---------------------------------------------------------------- paths

/// Where a source's object goes in its backup: where it is, except a branch's base's file (under
/// `_base/<id>/`), which the backup keeps under `data/_bases/<id>/` (a backup has no bases).
fn place(path: &str) -> String {
    match crate::branch::split(path) {
        Some((id, rest)) => format!("data/_bases/{id}/{rest}"),
        None => path.to_string(),
    }
}

/// The source's catalog as a backup keeps it, what it names (by the backup's paths, each with
/// where it is in the source), and the manifests written here for it: a branch's base's sealed
/// files, whose manifests name the base's own paths, are sealed again under the backup's.
#[allow(clippy::type_complexity)]
async fn placed(source: &Lake, snap: &BTreeMap<String, Bytes>, target: &Store) -> Result<(BTreeMap<String, Bytes>, BTreeMap<String, (String, Wanted)>, Vec<String>)> {
    let (mut cat, mut wanted, mut written) = (BTreeMap::new(), BTreeMap::<String, (String, Wanted)>::new(), vec![]);
    let mut want = |objects: Vec<(String, Wanted)>| {
        for (from, w) in objects {
            let e = wanted.entry(place(&from)).or_insert((from, w));
            (e.1.needed, e.1.log, e.1.bytes) = (e.1.needed | w.needed, e.1.log | w.log, e.1.bytes.max(w.bytes)); // (a table's file a log segment names too)
        }
    };
    let floor = tables(snap)?.iter().map(|m| m.tiered).min().unwrap_or(0);
    for (k, v) in snap {
        if own(k) {
            continue;
        }
        let value = match k.split_once('/').map_or("", |(p, _)| p) {
            "t" => {
                let (meta, objects) = table_placed(source, &k[2..], serde_json::from_slice(v).with_context(|| k.clone())?, target, &mut written).await?;
                want(objects);
                json(&meta)
            }
            "dt" => {
                let mut d: crate::ddl::Dropped = serde_json::from_slice(v).with_context(|| k.clone())?;
                let name = k[3..].rsplit_once('/').map_or(&k[3..], |(n, _)| n).to_string();
                let (meta, objects) = table_placed(source, &name, d.meta, target, &mut written).await?;
                want(objects);
                d.meta = meta;
                if let Some(m) = d.deleted.take() {
                    let (m, objects) = table_placed(source, &crate::sys::deleted(&name), m, target, &mut written).await?;
                    want(objects);
                    d.deleted = Some(m);
                }
                json(&d)
            }
            "s" => {
                let mut seg: Segment = serde_json::from_slice(v).with_context(|| k.clone())?;
                let mut objects = BTreeMap::new();
                segment_objects(&seg, k[2..].parse::<u64>().unwrap_or(0) > floor, &mut objects);
                want(objects.into_iter().collect());
                for filed in seg.files.values_mut() {
                    filed.added.iter_mut().chain(filed.removed.iter_mut()).chain(filed.deleted.iter_mut().map(|(f, _)| f)).for_each(placed_file);
                }
                seg.path = place(&seg.path);
                json(&seg)
            }
            // (a key a branch was lent to read its base with: a backup has no base)
            "e" if serde_json::from_slice::<crate::ext::Secret>(v).is_ok_and(|s| s.lent) => continue,
            _ => {
                cat.insert(k.clone(), v.clone());
                continue;
            }
        };
        cat.insert(k.clone(), Bytes::from(value));
    }
    Ok((cat, wanted, written))
}

/// A table's entry as the backup keeps it (nothing the source has yet to delete), and what it names.
async fn table_placed(source: &Lake, name: &str, mut meta: TableMeta, target: &Store, written: &mut Vec<String>) -> Result<(TableMeta, Vec<(String, Wanted)>)> {
    let list = crate::manifest::list(source, &meta).await?;
    let mut sealed = vec![];
    for m in &list {
        sealed.extend(crate::manifest::files(source, m).await?);
    }
    let mut objects = vec![];
    for f in meta.files.iter().chain(&sealed) {
        file_objects(f, FILE, &mut objects);
    }
    let based = |p: &str| p.starts_with("_base/");
    match &meta.sealed {
        // (manifests that name a base's paths: sealed again here, naming the backup's)
        Some(s) if based(&s.list) || list.iter().any(|m| based(&m.path)) || sealed.iter().any(|f| based(&f.path) || f.delete_files().any(|p| based(p))) => {
            sealed.iter_mut().for_each(placed_file);
            let schema = crate::query::schema(&crate::sys::with_sys(&meta).columns)?;
            meta.sealed = Some(crate::manifest::sealed_into(target, &format!("data/{}/_manifests", meta.folder(name)), &sealed, &schema, written).await?);
        }
        Some(s) => {
            objects.push((s.list.clone(), FILE));
            objects.extend(list.iter().map(|m| (m.path.clone(), FILE)));
        }
        None => {}
    }
    meta.files.iter_mut().chain(meta.replaced.iter_mut()).for_each(placed_file);
    (meta.garbage, meta.garbage_deletes) = Default::default(); // (the source's to delete: the backup keeps its own)
    Ok((meta, objects))
}

fn placed_file(f: &mut DataFile) {
    use crate::scan::Delete;
    f.path = place(&f.path);
    for d in &mut f.deletes {
        match d {
            Delete::Positions { path, .. } | Delete::Vector { path, .. } | Delete::Blob { path, .. } | Delete::Equality { path, .. } => *path = place(path),
            Delete::Inline { .. } => {}
        }
    }
}

// ---------------------------------------------------------------- copies

/// An object up to this size is copied whole; a bigger one in parts of it, never whole in memory.
const PIECE: usize = 8 << 20;

/// Copy `objects` (where each goes, where it is in `from`) to `to`, eight at a time (at most about
/// 128 MB in memory), each written only if it isn't there: an object never changes under its name
/// (invariant 6). Resuming a run that stopped, one that is there isn't read again.
async fn copy_all(from: &Lake, to: &Store, objects: Vec<(String, String, Wanted)>, resuming: bool) -> Result<(u64, u64)> {
    futures::stream::iter(objects)
        .map(|(dest, src, w)| async move {
            let dest = Path::from(dest.as_str());
            if resuming && to.head(&dest).await.is_ok() {
                return Ok((0, 0));
            }
            let (store, path) = from.reader(&src)?;
            let got = match store.get(&path).await {
                Ok(g) => g,
                Err(object_store::Error::NotFound { .. }) if !w.needed => return Ok((0, 0)), // (a log reader's file the source let go)
                Err(e) => return Err(anyhow::Error::new(e).context(format!("reading {src} of {}", from.url))),
            };
            let size = got.meta.size;
            if size <= PIECE as u64 {
                if !put_new(to, dest.as_ref(), got.bytes().await?).await? {
                    return Ok((0, 0)); // (a run stopped half way copied it)
                }
            } else if to.head(&dest).await.is_ok() {
                return Ok((0, 0));
            } else {
                let mut out = object_store::WriteMultipart::new_with_chunk_size(to.put_multipart(&dest).await?, PIECE);
                let mut pieces = got.into_stream();
                while let Some(piece) = pieces.next().await {
                    let piece: Bytes = piece?;
                    out.wait_for_capacity(1).await?;
                    out.write(&piece);
                }
                out.finish().await.with_context(|| format!("writing {dest}"))?;
            }
            Ok::<_, anyhow::Error>((1, size))
        })
        .buffer_unordered(8)
        .try_fold((0, 0), |(n, b), (n2, b2)| async move { Ok((n + n2, b + b2)) })
        .await
}

/// The workspace's files (`files/`, their kept versions too): each one that is new or changed since
/// the last run, and none that the source deleted. (Not in the catalog: a run copies them as they
/// are when it gets to them.)
async fn workspace(from: &Lake, to: &Store) -> Result<(u64, u64)> {
    let folder = Path::from("files");
    let theirs: Vec<object_store::ObjectMeta> = from.store.list(Some(&folder)).try_collect().await?;
    let mine: std::collections::HashMap<String, object_store::ObjectMeta> = to.list(Some(&folder)).map_ok(|o| (o.location.to_string(), o)).try_collect().await?;
    let changed: Vec<_> = theirs.iter().filter(|o| mine.get(&o.location.to_string()).is_none_or(|m| m.size != o.size || m.last_modified < o.last_modified)).collect();
    let (mut copied, mut bytes) = (0, 0);
    for o in changed {
        let body = from.store.get(&o.location).await?.bytes().await?;
        to.put(&o.location, body.into()).await.with_context(|| format!("writing {}", o.location))?;
        (copied, bytes) = (copied + 1, bytes + o.size);
    }
    let theirs: BTreeSet<String> = theirs.iter().map(|o| o.location.to_string()).collect();
    for gone in mine.keys().filter(|p| !theirs.contains(*p)) {
        let _ = to.delete(&Path::from(gone.as_str())).await;
    }
    Ok((copied, bytes))
}

/// Write an object that never changes under its name: false if it is there already.
async fn put_new(store: &Store, path: &str, bytes: Bytes) -> Result<bool> {
    match store.put_opts(&Path::from(path), bytes.into(), PutOptions { mode: PutMode::Create, ..Default::default() }).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(e) => Err(anyhow::Error::new(e).context(format!("writing {path}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lakes_own_keys_never_go() {
        for k in ["c", "n", "b", "m", "format", "dl", "bk", "z/auth", "quiet", "bs", "pn/x", "x/t", "i/t"] {
            assert!(own(k), "{k} is the lake's own");
        }
        for k in ["t/t", "dt/t/1", "s/00000000000000000001", "e/k", "u/ali", "j/nightly", "sh/s", "sr/r", "v/v", "f/f", "cm/table/t"] {
            assert!(!own(k), "{k} goes with a backup");
        }
    }

    #[test]
    fn a_base_file_goes_under_the_backups_bases() {
        assert_eq!(place("data/t/a.parquet"), "data/t/a.parquet");
        assert_eq!(place("_base/abc/data/t/a.parquet"), "data/_bases/abc/data/t/a.parquet");
    }

    #[test]
    fn times_a_restore_takes() {
        assert_eq!(instant("1760238000000").unwrap(), 1_760_238_000_000);
        assert_eq!(instant("2026-10-12 03:00:00").unwrap(), instant("2026-10-12T03:00:00Z").unwrap());
        assert_eq!(instant("'2026-10-12'").unwrap(), instant("2026-10-12 00:00").unwrap());
        assert!(instant("yesterday").is_err());
    }
}
