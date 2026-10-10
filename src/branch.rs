//! Branches (ADR-047): `CREATE DATABASE dev CLONE prod` makes a database that is prod as it was at
//! that moment, copying none of its files. The branch lists prod's files where they are, under
//! `_base/<id>/` (a prefix naming prod, so a file is named with its lake wherever it is read or
//! cached: `Lake::full`, `Lake::object`), and copies only prod's log tail into its first commit.
//! Its own writes go under its own prefix, and it never deletes outside it (`Lake::delete`).
//! prod keeps every file that was live when the branch was made, for as long as its pin is there
//! (`pn/`: `tier::expire`, the orphan sweep); a branch of a branch pins every lake it reads.
use crate::ddl::Ddl;
use crate::store::{json, table_key, Lake, TableMeta};
use crate::views::View;
use crate::write::Request;
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use object_store::ObjectStoreExt;
use std::collections::BTreeMap;

/// The catalog key of a branch's bases (`Bases`).
pub const BASES: &str = "bs";
/// A pin is taken this long before its base's leader commits it: garbage is stamped by whichever
/// leader made it, and a leader since may run a little behind on its clock.
const MARGIN_MS: u64 = 60_000;

/// What a branch reads besides itself: its base, and its base's bases (each by its id).
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Bases {
    pub base: String,  // the lake it was made from
    pub at_ms: u64,    // when
    pub me: String,    // its own place, as its bases' pins name it
    pub lakes: BTreeMap<String, Base>,
    // (what it was made with: `SHOW CREATE DATABASE` gives it, and REFRESH brings no schema it
    // left out; none: all)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<String>,
    #[serde(default = "yes")]
    pub data: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Base {
    pub url: String,
    pub ms: u64, // its pin: every file live then is kept
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>, // its key there: its pin renewed and let go with it (`keyed`, ADR-058)
}

/// A base's promise to a branch: every file live at `ms` stays until this goes.
#[derive(Serialize, Deserialize, Clone)]
pub struct Pin {
    pub lake: String,
    pub ms: u64,
    pub at_ms: u64,
}

/// `CLONE a [WITH (schemas = (x, y))] [WITH NO DATA]`.
#[derive(Serialize, Deserialize, Clone)]
pub struct CloneOf {
    pub from: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<String>,
    #[serde(default = "yes")]
    pub data: bool,
}

/// What the new lake's leader is asked to make of itself.
#[derive(Serialize, Deserialize, Clone)]
pub struct Make {
    pub base: String,
    pub me: String,
    pub lakes: BTreeMap<String, Base>,
    #[serde(default)]
    pub schemas: Vec<String>,
    pub data: bool,
}

fn yes() -> bool { true }

/// A lake's id in its branches' paths: stable across processes and builds (FNV-1a).
pub fn id_of(url: &str) -> String {
    let h = url.bytes().fold(0xcbf29ce484222325u64, |h, b| (h ^ b as u64).wrapping_mul(0x100000001b3));
    format!("{:012x}", h & 0xffff_ffff_ffff)
}

fn pin_key(branch: &str) -> String { format!("pn/{}", id_of(branch)) }

/// Is `name` (a table or view, `schema.t` or `t`) in the schemas a clone took (none: all)?
fn took(schemas: &[String], name: &str) -> bool {
    schemas.is_empty() || schemas.iter().any(|s| s == name.split_once('.').map_or(crate::ddl::PUBLIC, |(s, _)| s))
}

/// `_base/<id>/rest` as (id, rest).
pub fn split(path: &str) -> Option<(&str, &str)> { path.strip_prefix("_base/")?.split_once('/') }

/// A base's path as the branch names it: its own files under the base's prefix; a path that
/// names a lake already (`_base/…`, a URL) stays as it is.
pub fn rebase(path: &str, id: &str) -> String {
    match path.is_empty() || path.contains("://") || path.starts_with("_base/") {
        true => path.to_string(),
        false => format!("_base/{id}/{path}"),
    }
}

/// The id of the base an object a branch read belongs to (its paths are that base's).
pub fn base_of(path: &str) -> Option<&str> { split(path).map(|(id, _)| id) }

/// A base's file record as the branch keeps it.
pub fn rebase_file(f: &mut crate::store::DataFile, id: &str) {
    use crate::scan::Delete;
    f.path = rebase(&f.path, id);
    for d in &mut f.deletes {
        match d {
            Delete::Positions { path, .. } | Delete::Vector { path, .. } | Delete::Blob { path, .. } | Delete::Equality { path, .. } => *path = rebase(path, id),
            Delete::Inline { .. } => {}
        }
    }
}

/// Lake::open: the bases a branch reads, so its paths resolve (`Lake::full`, `Lake::object`).
pub async fn load(lake: &Lake) -> Result<()> {
    if let Some(b) = lake.cat.get::<Bases>(BASES).await? {
        for (id, base) in b.lakes {
            lake.add_base(&id, &base.url)?;
        }
    }
    Ok(())
}

/// `CREATE DATABASE name CLONE from` (this lake's leader, under its lock).
pub async fn create(lake: &Lake, name: &str, if_not_exists: bool, dir: Option<String>, of: CloneOf) -> Result<Value> {
    crate::ddl::check(name)?;
    let (base, inherited) = base(lake, &of.from).await?;
    let dir = crate::ddl::full(&dir.unwrap_or_else(|| crate::ddl::beside(&lake.url, name)))?;
    ensure!(dir.contains("://") == base.contains("://"), "a branch lives where its base does: {base} is {}", if base.contains("://") { "in a bucket" } else { "a folder on this machine" });
    if crate::ddl::has_catalog(&dir).await? {
        ensure!(if_not_exists, "a lake is at {dir} already: ATTACH '{dir}' AS {name} (or CREATE DATABASE IF NOT EXISTS {name} CLONE {})", of.from);
        return Box::pin(crate::ddl::apply(lake, Ddl::Attach { name: name.into(), dir })).await;
    }
    // Pins first, then the branch's first commit, which reads its base at a version that holds
    // its pin: every file it lists is then kept.
    let mut lakes = inherited.lakes.clone();
    let mut pinned = vec![];
    let made = async {
        let pinning = pin_in(lake, &base, &dir, None, None).await;
        let (ms, _, key) = match base == lake.url {
            true => pinning?,
            false => pinning.with_context(|| format!("pinning it in {}: a database that signs in on its own is cloned on its own node (CREATE DATABASE … CLONE … there), or by nodes that share its admin token", of.from))?,
        };
        pinned.push((base.clone(), key.clone()));
        for (id, b) in inherited.lakes.iter() {
            // (the key their pins give this branch, not the one its base gave it)
            let (_, _, key) = pin_in(lake, &b.url, &dir, Some(b.ms), None).await?;
            if let Some(e) = lakes.get_mut(id) {
                e.key = key.clone();
            }
            pinned.push((b.url.clone(), key));
        }
        lakes.insert(id_of(&base), Base { url: base.clone(), ms, key });
        let make = Make { base: base.clone(), me: dir.clone(), lakes, schemas: of.schemas.clone(), data: of.data };
        Box::pin(crate::write::send(&dir, Request::Ddl(Ddl::Branch(make)))).await
    }.await;
    let made = match made {
        Ok(v) => v,
        Err(e) => {
            // (nothing half made stays: the lake made for it, and the pins)
            let _ = crate::ddl::delete_lake(&dir).await;
            for (b, key) in pinned {
                let _ = unpin_in(lake, &b, &dir, key.as_deref()).await;
            }
            return Err(e.context(format!("cloning {}", of.from)));
        }
    };
    let attached = Box::pin(crate::ddl::apply(lake, Ddl::Attach { name: name.into(), dir })).await?;
    Ok(j!({"database": name, "cloned": of.from, "tables": made["tables"], "dir": attached["dir"]}))
}

/// The lake a clone is of (this one, or one attached here), and its own bases.
async fn base(lake: &Lake, from: &str) -> Result<(String, Bases)> {
    let from = from.trim_matches('"').to_lowercase();
    if from == crate::ddl::lake_name(lake) {
        return Ok((lake.url.clone(), lake.cat.get(BASES).await?.unwrap_or_default()));
    }
    let other = lake.attached.read().unwrap().iter().find(|(n, _)| *n == from).map(|(_, l)| l.clone());
    let Some(other) = other else { bail!("no database {from} here: a clone is of this database or one attached to it") };
    let bases = other.cat.get(BASES).await?.unwrap_or_default();
    Ok((other.url.clone(), bases))
}

/// Pin `branch` in the lake at `at`: here, or through its own leader (invariant 20), with `key` when
/// it has one (a branch's renewal, `keyed`). Its time, when it was written (by that leader's clock),
/// and the key its pin gives the branch.
async fn pin_in(lake: &Lake, at: &str, branch: &str, ms: Option<u64>, key: Option<&str>) -> Result<(u64, u64, Option<String>)> {
    let v = match at == lake.url {
        true => pin(lake, branch, ms).await?,
        false => Box::pin(crate::write::send_as(at, Request::Ddl(Ddl::Pin { lake: branch.into(), ms }), key)).await?,
    };
    Ok((v["ms"].as_u64().context("a pin's time")?, v["at_ms"].as_u64().unwrap_or(0), v["key"].as_str().map(String::from)))
}

async fn unpin_in(lake: &Lake, at: &str, branch: &str, key: Option<&str>) -> Result<()> {
    match at == lake.url {
        true => drop(unpin(lake, branch).await?),
        false => drop(Box::pin(crate::write::send_as(at, Request::Ddl(Ddl::Unpin { lake: branch.into() }), key)).await?),
    }
    Ok(())
}

/// A base's leader: keep every file live at `ms` (now, less a margin, unless a branch of a branch
/// carries its own base's on) while `branch` is there.
pub async fn pin(lake: &Lake, branch: &str, ms: Option<u64>) -> Result<Value> {
    let (key, now) = (pin_key(branch), crate::log::now_ms());
    let mut ms = ms.unwrap_or(now.saturating_sub(MARGIN_MS));
    if let Some(p) = lake.cat.get::<Pin>(&key).await? {
        ms = ms.min(p.ms);
    }
    lake.cat.commit(vec![(key, json(&Pin { lake: branch.into(), ms, at_ms: now }))], &[]).await?;
    Ok(j!({"pinned": branch, "ms": ms, "at_ms": now, "key": key_for(lake, branch).await?}))
}

pub async fn unpin(lake: &Lake, branch: &str) -> Result<Value> {
    lake.cat.commit(vec![], &[pin_key(branch)]).await?;
    Ok(j!({"unpinned": branch}))
}

/// A branch's key in this lake: its pin renewed and let go with it, nothing else (`keyed`).
/// Derived from the lake's own key, so asked again (a retried CLONE) it is the same.
pub async fn key_for(lake: &Lake, branch: &str) -> Result<String> {
    Ok(format!("pb_{}", crate::users::sign(lake, format!("branch-key\n{branch}").as_bytes()).await?))
}

/// `/cluster/ddl` with a branch's key: a renewal or an unpin of that branch's pin, while there is
/// one (a key makes no pin, nor an older one: a branch keeps no more of its base than it was given).
pub async fn keyed(lake: &Lake, d: &Ddl, key: &str) -> Result<()> {
    let branch = match d {
        Ddl::Pin { lake: b, ms: None } | Ddl::Unpin { lake: b } => b,
        _ => bail!("permission denied: a branch's key renews or lets go of its own pin, and does nothing else"),
    };
    let want = key_for(lake, branch).await?;
    ensure!(aws_lc_rs::constant_time::verify_slices_are_equal(want.as_bytes(), key.as_bytes()).is_ok(), "permission denied: not {branch}'s key");
    ensure!(matches!(d, Ddl::Unpin { .. }) || lake.cat.get::<Pin>(&pin_key(branch)).await?.is_some(), "permission denied: {branch} has no pin in {}: a branch's key renews its pin, it never makes one", lake.url);
    Ok(())
}

/// May the request being served clone from, or refresh, another database attached here?
/// (`users::across`: one that signs in on its own is cloned through its own sign-in, on its own
/// node, or with the nodes' tokens.) Checked where the statement comes in: the leader it goes
/// to runs it as the node. A branch's own REFRESH goes by its key (`keyed`).
pub async fn may(lake: &Lake, d: &Ddl) -> Result<()> {
    let name = match d {
        Ddl::CreateDatabase { clone: Some(of), .. } => of.from.trim_matches('"').to_lowercase(),
        Ddl::Refresh { database: Some(db), .. } => db.clone(),
        _ => return Ok(()),
    };
    let other = lake.attached.read().unwrap().iter().find(|(n, _)| *n == name).map(|(_, l)| l.clone());
    match other {
        Some(o) if name != crate::ddl::lake_name(lake) => crate::users::across(&o, &name).await,
        _ => Ok(()),
    }
}

/// The pins clean-up keeps files for: the oldest and the newest (None: no branch reads this lake).
pub async fn pins(lake: &Lake) -> Result<Option<(u64, u64)>> {
    let all = lake.cat.scan::<Pin>("pn/", "pn0").await?;
    Ok(all.iter().map(|(_, p)| p.ms).min().zip(all.iter().map(|(_, p)| p.ms).max()))
}

/// Hourly, on a base's leader: a pin whose branch is gone (its lake deleted without a DROP
/// DATABASE here) goes too. An hour's grace: a pin is taken just before its branch is made.
pub async fn sweep(lake: &Lake) -> Result<()> {
    let now = crate::log::now_ms();
    for (key, p) in lake.cat.scan::<Pin>("pn/", "pn0").await? {
        if p.at_ms + 3_600_000 < now && !crate::ddl::has_catalog(&p.lake).await.unwrap_or(true) {
            lake.cat.commit(vec![], &[key]).await?;
        }
    }
    Ok(())
}

/// The new lake's leader: make this lake its base as it is now. One commit holds it all: every
/// entry but what is the base's alone (secrets, its keys, its publishing and members, the pins of
/// its own branches), the base's files listed where they are, its log tail copied, and the
/// counters (commits, segments, row ids) carried on, so rows keep their `_row_id` and `_version`
/// and what the branch writes numbers after them.
pub async fn make(lake: &Lake, m: Make) -> Result<Value> {
    if let Some(b) = lake.cat.get::<Bases>(BASES).await? {
        ensure!(b.base == m.base, "{} is a branch of {} already", lake.url, b.base);
        return Ok(j!({"branch": lake.url, "of": m.base, "unchanged": true})); // (asked again: done)
    }
    ensure!(lake.cat.scan_raw("t/", "t0").await?.is_empty(), "{} holds tables: a branch is made into a new database", lake.url);
    let base = Lake::open(&m.base, false, false).await.with_context(|| format!("opening {}", m.base))?;
    let id = id_of(&m.base);
    let pin = pin_key(&m.me);
    let snap = snapshot(&base, &pin, 0).await?;
    let number = |k: &str| snap.get(k).and_then(|v| serde_json::from_slice::<u64>(v).ok()).unwrap_or(0);
    let (commit, next, block) = (number("c"), number("n").max(1), number("b"));
    let end = next - 1;
    let wanted = |name: &str| took(&m.schemas, name);
    // (a share and its recipients are the base's: a branch hands nothing to another company)
    let deny = ["e/", "z/", "x/", "i/", "dt/", "jt/", "pn/", "fd/", "s/", "d/", "sh/", "sr/"];
    let mut puts = vec![];
    let (mut tables, mut tiered) = (0, end);
    for (k, v) in &snap {
        let (prefix, name) = k.split_once('/').map_or((k.as_str(), ""), |(p, n)| (p, n));
        match prefix {
            _ if deny.iter().any(|d| k.starts_with(d)) => {}
            "c" | "n" | "b" | "m" | BASES | "format" | crate::store::QUIET => {}
            "t" if name.starts_with("pondra$") || !wanted(name) => {} // (the run log, audit, history: the branch's own)
            "t" => {
                let mut meta: TableMeta = serde_json::from_slice(v).with_context(|| k.clone())?;
                match m.data {
                    true => listed(&mut meta, &id),
                    false => (meta.files, meta.sealed, meta.tiered, meta.purges, meta.changed, meta.sketch, meta.rows_at) = (vec![], None, end, vec![], false, Default::default(), 0),
                }
                // (what the base deletes, publishes and shares is the base's: never the branch's)
                (meta.garbage, meta.garbage_deletes, meta.replaced, meta.shares, meta.publish) = Default::default();
                tiered = tiered.min(meta.tiered);
                tables += 1;
                puts.push((k.clone(), json(&meta)));
            }
            "v" | "q" | "w" | "k" if !wanted(name) => {}
            "ns" if !m.schemas.is_empty() && !m.schemas.iter().any(|s| s == name) => {}
            "p" | "w" if !m.data => {}
            "j" => {
                // (nothing in a branch reaches the outside on its own: ALTER TASK … RESUME)
                let mut task: crate::runs::Task = serde_json::from_slice(v).with_context(|| k.clone())?;
                task.suspended = true;
                puts.push((k.clone(), json(&task)));
            }
            _ => puts.push((k.clone(), v.to_vec())),
        }
    }
    // The base's log after the oldest table's files, copied with its segment numbers: rows not
    // yet in any file (seconds of them), with their ids and versions.
    let mut objects = vec![];
    if m.data {
        for (k, v) in snap.range(crate::store::seg_key(tiered + 1)..crate::store::seg_key(next)) {
            let mut seg: crate::store::Segment = serde_json::from_slice(v).with_context(|| k.clone())?;
            seg.files.values_mut().for_each(|f| f.added.iter_mut().chain(f.removed.iter_mut()).chain(f.deleted.iter_mut().map(|(d, _)| d)).for_each(|d| rebase_file(d, &id)));
            match seg.path.is_empty() {
                true => {
                    // (inline rows in the catalog; a file commit has none)
                    let data = crate::store::data_key(k[2..].parse()?);
                    match snap.get(&data) {
                        Some(v) => puts.push((data, v.to_vec())),
                        None => ensure!(seg.parts.is_empty(), "{data} of {} is missing", m.base),
                    }
                }
                false => objects.push(seg.path.clone()),
            }
            puts.push((k.clone(), json(&seg)));
        }
    }
    // The workspace's files (their current versions): a project's code, as the base has it.
    objects.extend(workspace(&base).await?);
    use futures::{StreamExt, TryStreamExt};
    futures::stream::iter(objects).map(|p| copy(&base, lake, p)).buffer_unordered(16).try_collect::<Vec<_>>().await?;
    puts.push(("n".into(), json(&next.max(lake.cat.get::<u64>("n").await?.unwrap_or(0)))));
    puts.push(("b".into(), json(&block.max(lake.cat.get::<u64>("b").await?.unwrap_or(0)))));
    let me = Bases { base: m.base.clone(), at_ms: crate::log::now_ms(), me: m.me.clone(), lakes: m.lakes.clone(), schemas: m.schemas.clone(), data: m.data };
    puts.push((BASES.into(), json(&me)));
    lake.cat.start_after(commit).await;
    lake.cat.commit(puts, &[]).await?;
    load(lake).await?;
    Ok(j!({"branch": lake.url, "of": m.base, "tables": tables}))
}

/// The base's catalog at a version that holds `pin`, written at `at_ms` or later.
async fn snapshot(base: &Lake, pin: &str, at_ms: u64) -> Result<BTreeMap<String, bytes::Bytes>> {
    let started = std::time::Instant::now();
    loop {
        let all = base.cat.scan_raw("", "\x7f").await?;
        if all.get(pin).and_then(|v| serde_json::from_slice::<Pin>(v).ok()).is_some_and(|p| p.at_ms >= at_ms) {
            return Ok(all);
        }
        ensure!(started.elapsed().as_secs() < 60, "{} didn't show its pin for this branch within a minute", base.url);
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

/// A base's table as a branch lists it: its files where they are, and nothing the base alone does
/// with them (deletes, publishes, shares).
fn listed(meta: &mut TableMeta, id: &str) {
    meta.files.iter_mut().for_each(|f| rebase_file(f, id));
    if let Some(s) = &mut meta.sealed {
        s.list = rebase(&s.list, id);
    }
    (meta.garbage, meta.garbage_deletes, meta.replaced, meta.shares, meta.publish) = Default::default();
}

/// The base's workspace files (not their kept versions), to copy.
async fn workspace(base: &Lake) -> Result<Vec<String>> {
    use futures::TryStreamExt;
    let all: Vec<_> = base.store.list(Some(&object_store::path::Path::from("files"))).try_collect().await?;
    Ok(all.into_iter().map(|o| o.location.to_string()).filter(|p| !p.starts_with("files/.versions/")).collect())
}

async fn copy(from: &Lake, to: &Lake, path: String) -> Result<()> {
    let bytes = from.store.get(&object_store::path::Path::from(path.as_str())).await?.bytes().await?;
    match to.store.put(&object_store::path::Path::from(path.as_str()), bytes.into()).await {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow::Error::new(e).context(format!("copying {path}"))),
    }
}

/// `DROP DATABASE` of a branch: its pins in its bases go once its lake has.
pub async fn release(lake: &Lake, bases: &Bases) {
    for b in bases.lakes.values() {
        if let Err(e) = unpin_in(lake, &b.url, &bases.me, b.key.as_deref()).await {
            eprintln!("releasing {}'s pin in {}: {e:#} (the base's hourly sweep lets it go)", bases.me, b.url);
        }
    }
}

/// `pondra.databases`: this database and the ones attached here, with what each was branched from.
pub async fn databases(lake: &Lake) -> Result<datafusion::arrow::record_batch::RecordBatch> {
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
    use std::sync::Arc;
    let mut all = vec![(crate::ddl::lake_name(lake), lake.arc())];
    all.extend(lake.attached.read().unwrap().iter().cloned());
    let named = |url: &str| all.iter().find(|(_, l)| l.url == url).map_or(url.to_string(), |(n, _)| n.clone());
    let mut rows = vec![];
    for (name, l) in &all {
        let b = l.cat.get::<Bases>(BASES).await.ok().flatten();
        let branches = l.cat.scan::<Pin>("pn/", "pn0").await.map(|p| p.len()).unwrap_or(0);
        rows.push((name.clone(), l.url.clone(), b.as_ref().map(|b| named(&b.base)), b.map(|b| b.at_ms as i64 * 1000), branches as i64));
    }
    let s = |f: &dyn Fn(&(String, String, Option<String>, Option<i64>, i64)) -> Option<String>| Arc::new(rows.iter().map(f).collect::<StringArray>()) as ArrayRef;
    Ok(datafusion::arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("name", s(&|r| Some(r.0.clone()))),
        ("location", s(&|r| Some(r.1.clone()))),
        ("base", s(&|r| r.2.clone())),
        ("branched_at", Arc::new(rows.iter().map(|r| r.3).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef),
        ("branches", Arc::new(rows.iter().map(|r| Some(r.4)).collect::<Int64Array>()) as ArrayRef),
    ])?)
}

/// `ALTER DATABASE b REFRESH [t, …]` (b's leader): each table named (a view stands for the tables
/// it reads; none named, every table b took from its base that the base still has), and every view
/// that follows them, as the base has them now. Their files are listed where they are and their rows still in the base's
/// log are written into b's own files, with their ids and versions; b's own rows of them go. b's
/// counters first move past the base's, so what b writes next numbers after what it took (a newer
/// keyed file's `ord` is greater: invariant 5). A view of them that b defines otherwise than its
/// base, or one keeping state of its own (a window, sessions, a stream join), is refused by name.
pub async fn refresh(lake: &Lake, seq: &crate::log::Sequencer, lock: &tokio::sync::Mutex<()>, database: Option<String>, tables: Vec<String>) -> Result<Value> {
    if let Some(db) = database.filter(|d| *d != crate::ddl::lake_name(lake)) {
        // (another database's: its own leader does it, and this lake's lock isn't held meanwhile)
        let url = lake.attached.read().unwrap().iter().find(|(n, _)| *n == db).map(|(_, l)| l.url.clone());
        let url = url.with_context(|| format!("no database {db} here"))?;
        return Box::pin(crate::write::send(&url, Request::Ddl(Ddl::Refresh { database: None, tables }))).await;
    }
    let me: Bases = lake.cat.get(BASES).await?.with_context(|| format!("{} isn't a branch: REFRESH brings a branch's tables up to its base's", crate::ddl::lake_name(lake)))?;
    // A version of the base from after this statement: the pin written again (its time kept: the
    // files it lists now are newer) shows in it.
    let key = me.lakes.get(&id_of(&me.base)).and_then(|b| b.key.clone());
    let (_, at, _) = pin_in(lake, &me.base, &me.me, None, key.as_deref()).await?;
    let base = Lake::open(&me.base, false, false).await.with_context(|| format!("opening {}", me.base))?;
    let snap = snapshot(&base, &pin_key(&me.me), at).await?;
    let number = |k: &str| snap.get(k).and_then(|v| serde_json::from_slice::<u64>(v).ok()).unwrap_or(0);
    let (next, block) = (number("n").max(1), number("b"));
    let end = next - 1;
    // The tables named, then whatever follows them, there or here: views, views of those, …
    let mut theirs: BTreeMap<String, View> = snap.range("v/".to_string().."v0".to_string()).map(|(k, v)| Ok((k[2..].to_string(), serde_json::from_slice(v)?))).collect::<Result<_>>()?;
    theirs.retain(|n, _| took(&me.schemas, n)); // (the base's views outside the schemas taken are never brought)
    let ours: BTreeMap<String, View> = lake.cat.scan::<View>("v/", "v0").await?.into_iter().map(|(k, v)| (k[2..].to_string(), v)).collect();
    let view = |n: &str| theirs.get(n).or_else(|| ours.get(n));
    let mut set: Vec<String> = vec![];
    if tables.is_empty() {
        // Every table this branch took from its base that the base still has; views (and a
        // window's `_final`) come with the tables they read, hidden tables with theirs.
        let here = lake.cat.scan::<serde_json::Value>("t/", "t0").await?;
        let made = |n: &str| view(n).is_some() || n.strip_suffix("_final").is_some_and(|v| view(v).is_some());
        set = here.into_iter().map(|(k, _)| k[2..].to_string()).filter(|n| !n.contains('$') && !made(n) && snap.contains_key(&table_key(n)) && took(&me.schemas, n)).collect();
    }
    for t in &tables {
        let name = crate::ddl::local(lake, t).with_context(|| format!("{t}: a table of this database"))?;
        // A view stands for the tables it reads (its rows are theirs, worked out), back to the first.
        let mut todo = vec![name];
        while let Some(n) = todo.pop() {
            match view(&n) {
                Some(v) => todo.extend(std::iter::once(v.source.clone()).chain(v.join.iter().flat_map(|j| j.tables.clone()))),
                None if set.contains(&n) => {}
                None => {
                    ensure!(took(&me.schemas, &n), "{n}: {} took only {} of {}", crate::ddl::lake_name(lake), me.schemas.join(", "), me.base);
                    ensure!(snap.contains_key(&table_key(&n)), "{n} isn't a table of {}", me.base);
                    set.push(n);
                }
            }
        }
    }
    let follows = |v: &View, set: &[String]| set.contains(&v.source) || v.join.as_ref().is_some_and(|j| j.tables.iter().any(|t| set.contains(t)));
    loop {
        let more: std::collections::BTreeSet<String> = theirs.iter().chain(&ours).filter(|(n, v)| !set.contains(n) && follows(v, &set)).map(|(n, _)| n.clone()).collect();
        if more.is_empty() {
            break;
        }
        set.extend(more);
    }
    let views: Vec<&String> = set.iter().filter(|n| theirs.contains_key(*n) || ours.contains_key(*n)).collect();
    for v in &views {
        let Some(t) = theirs.get(*v) else { bail!("{v} follows what REFRESH brings, and {} has no such view: drop it, REFRESH, then make it again", me.base) };
        ensure!(ours.get(*v).is_none_or(|o| serde_json::to_value(o).ok() == serde_json::to_value(t).ok()), "{v} is defined here otherwise than in {}: drop it, REFRESH, then make it again", me.base);
        ensure!(t.emit.is_none() && t.sessions.is_none() && t.join.is_none(), "{v} keeps state of its own (a window, sessions or a stream join): drop it, REFRESH, then make it again");
    }
    let all: Vec<String> = set.iter().flat_map(|t| [t.clone(), crate::sys::deleted(t)]).filter(|t| snap.contains_key(&table_key(t))).collect();
    // Each table as the base has it, its rows not yet in files written into files of this lake.
    let id = id_of(&me.base);
    let mut metas = vec![];
    for t in &all {
        let mut meta: TableMeta = serde_json::from_slice(&snap[&table_key(t)]).with_context(|| t.clone())?;
        let tail = match meta.tiered < end {
            true => crate::tier::fold_into(&base, lake, t, &meta, meta.tiered, end).await?,
            false => vec![],
        };
        listed(&mut meta, &id);
        meta.files.extend(tail);
        metas.push((t.clone(), meta));
    }
    let _guard = lock.lock().await;
    // This lake's counters past the base's, then a number of its own: every table here starts
    // after it (invariant 50), and its own rows of these tables before it are left behind.
    let now = seq.number().await?;
    let skip = crate::log::Flush { reserve: next.saturating_sub(now.version + 1), blocks: block.saturating_sub(now.block), ..Default::default() };
    if skip.reserve > 0 || skip.blocks > 0 {
        seq.submit(skip).await?;
    }
    let mark = seq.number().await?.version;
    let (mut puts, now_ms) = (vec![], crate::log::now_ms());
    for (t, mut meta) in metas {
        meta.tiered = mark;
        if let Some(old) = lake.cat.get::<TableMeta>(&table_key(&t)).await? {
            // (its own files go after the retention period; its bases' are theirs)
            meta.garbage = old.garbage.into_iter().chain(old.files.iter().filter(|f| split(&f.path).is_none()).map(|f| (f.path.clone(), now_ms))).collect();
            let left = crate::tier::backlog(lake, old.tiered, Some(mark)).await?.get(&t).copied().unwrap_or(0);
            lake.backlog.fetch_sub(left.min(lake.backlog.load(std::sync::atomic::Ordering::Relaxed)), std::sync::atomic::Ordering::Relaxed);
        }
        puts.push((table_key(&t), json(&meta)));
    }
    let mut fills = vec![];
    for v in views {
        puts.push((crate::views::view_key(v), snap[&crate::views::view_key(v)].to_vec()));
        let fill = crate::store::producer_key(&format!("fill:{v}"));
        if let Some(p) = snap.get(&fill) {
            puts.push((fill, p.to_vec()));
            fills.push(format!("fill:{v}"));
        }
    }
    lake.cat.commit(puts, &[]).await?;
    crate::views::forget(lake); // (the sequencer's views as they are now: invariant 77)
    crate::log::forget_producers(fills);
    Ok(j!({"refreshed": all, "from": me.base}))
}

/// `pondra.diff('a', 'b')` in a FROM: the rows of table b that differ from a's, in the change
/// feed's words (`_change_type`: insert, delete, update_preimage, update_postimage). A branch's
/// rows keep their `_row_id` and `_version` (ADR-020), so the rows both hold alike match.
pub fn diffs(sql: &str) -> Result<String> {
    static CALL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?i)\bpondra\.diff\s*\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)"#).expect("a regex"));
    if !sql.to_ascii_lowercase().contains("pondra.diff") {
        return Ok(sql.to_string());
    }
    static NAME: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"^[\w"$]+(\.[\w"$]+){0,2}$"#).expect("a regex"));
    let mut bad = None;
    let out = CALL.replace_all(sql, |c: &regex::Captures| {
        let (a, b) = (c[1].trim(), c[2].trim());
        if !NAME.is_match(a) || !NAME.is_match(b) {
            bad = Some(format!("pondra.diff takes two tables' names: pondra.diff('prod.sales.orders', 'dev.sales.orders'), not {a:?}, {b:?}"));
        }
        let arm = |kind: &str, t: &str, other: &str, test: &str, changed: bool| {
            let changed = if changed { " AND y._version <> x._version" } else { "" };
            format!("SELECT * FROM (SELECT '{kind}' AS _change_type, x.*, x._row_id, x._version FROM {t} x WHERE {test} (SELECT 1 FROM {other} y WHERE y._row_id = x._row_id{changed}))")
        };
        format!("({} UNION ALL {} UNION ALL {} UNION ALL {})", arm("insert", b, a, "NOT EXISTS", false), arm("update_preimage", a, b, "EXISTS", true),
            arm("update_postimage", b, a, "EXISTS", true), arm("delete", a, b, "NOT EXISTS", false))
    });
    match bad {
        Some(e) => bail!(e),
        None => Ok(out.into_owned()),
    }
}
