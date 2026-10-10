//! Branches (ADR-047): `CREATE DATABASE dev CLONE prod` makes a database that is prod as it was at
//! that moment, copying none of its files. The branch lists prod's files where they are, under
//! `_base/<id>/` (a prefix naming prod, so a file is named with its lake wherever it is read or
//! cached: `Lake::full`, `Lake::object`), and copies only prod's log tail into its first commit.
//! Its own writes go under its own prefix, and it never deletes outside it (`Lake::delete`).
//! prod keeps every file that was live when the branch was made, for as long as its pin is there
//! (`pn/`: `tier::expire`, the orphan sweep); a branch of a branch pins every lake it reads.
use crate::ddl::Ddl;
use crate::objects::{OnClone, KINDS};
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>, // (who made it: it or an admin drops it, `door`)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Base {
    pub url: String,
    pub ms: u64, // its pin: every file live then is kept
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>, // its key there, in the clear (no master key every node shares, or kept before): its pin renewed and let go with it (`keyed`, ADR-058)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed: Option<Sealed>, // (its key there, sealed by the master key: `Base::key`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>, // (a base on another server: its URL, where its leader is asked for pins)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool, // (a base on another server, read with a read-only key: ADR-058)
}

/// A branch's key sealed as the lake's own keys are (`users::Kept`): a data key of its own, wrapped.
#[derive(Serialize, Deserialize, Clone)]
pub struct Sealed {
    sealed: String,
    key: String,
}

impl Base {
    /// Its key there, opened when it is kept sealed.
    pub fn key(&self) -> Result<Option<String>> {
        let Some(s) = &self.sealed else { return Ok(self.key.clone()) };
        let plain = crate::ext::unseal(&s.sealed, &s.key).map_err(|e| anyhow::anyhow!("this branch's key in {} is sealed by a master key this node doesn't have ({e:#}): every node needs the same PONDRA_SECRET_KEY (or PONDRA_KMS_COMMAND)", self.url))?;
        Ok(Some(String::from_utf8(plain)?))
    }

    /// Its key sealed by the master key when every node shares one (`ext::shared_master`; never this
    /// machine's own, which another node couldn't open), or wrapped again after the master key
    /// changed. Whether it changed.
    fn seal(&mut self) -> Result<bool> {
        if let Some(s) = &mut self.sealed {
            let Some(key) = crate::ext::rewrapped(&s.key)? else { return Ok(false) };
            s.key = key;
            return Ok(true);
        }
        match self.key.take() {
            Some(k) if crate::ext::shared_master() => {
                let (sealed, key) = crate::ext::seal(k.as_bytes())?;
                self.sealed = Some(Sealed { sealed, key });
                Ok(true)
            }
            k => {
                self.key = k;
                Ok(false)
            }
        }
    }

    /// On another server (ADR-058): read with a read-only key, its leader at an endpoint.
    pub fn away(&self) -> bool { self.read_only || self.endpoint.is_some() }
}

/// A base's promise to a branch: every file live at `ms` stays until this goes.
#[derive(Serialize, Deserialize, Clone)]
pub struct Pin {
    pub lake: String,
    pub ms: u64,
    pub at_ms: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lease: bool, // (a branch on another server's: it renews it, and it goes when it isn't renewed: `lapse`)
}

/// `CLONE a [WITH (schemas = (x, y))] [WITH NO DATA]`.
#[derive(Serialize, Deserialize, Clone)]
pub struct CloneOf {
    pub from: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<String>,
    #[serde(default = "yes")]
    pub data: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>, // (who makes it, stamped where the statement comes in: `door`)
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<(String, crate::ext::Secret)>, // (the bucket keys its bases on other servers are read with: kept in its own catalog)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<crate::users::User>, // (its maker, as the database making it has it: a superuser in the branch)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_name: Option<String>,
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

/// Lake::open: the bases a branch reads, so its paths resolve (`Lake::full`, `Lake::object`). A base
/// on another server is read with the bucket key its branch keeps (ADR-058).
pub async fn load(lake: &Lake) -> Result<()> {
    if let Some(b) = lake.cat.get::<Bases>(BASES).await? {
        let secrets = match b.lakes.values().any(|b| b.read_only) {
            true => crate::ext::list(lake).await?,
            false => vec![],
        };
        for (id, base) in b.lakes {
            if base.read_only {
                crate::ext::reach_with(&secrets, &base.url, true, base.endpoint.as_deref())?;
            }
            lake.add_base(&id, &base.url)?;
        }
    }
    Ok(())
}

/// `CREATE DATABASE name CLONE from` (this lake's leader, under its lock).
pub async fn create(lake: &Lake, name: &str, if_not_exists: bool, dir: Option<String>, of: CloneOf) -> Result<Value> {
    crate::ddl::check(name)?;
    let (base, inherited) = base(lake, &of.from).await?;
    let away = crate::store::reach_of(&base); // (a base on another server: its key and its leader's URL, ADR-058)
    let dir = crate::ddl::full(&dir.unwrap_or_else(|| crate::ddl::beside(&lake.url, name)))?;
    ensure!(dir.contains("://") == base.contains("://"), "a branch lives where its base does: {base} is {}", if base.contains("://") { "in a bucket" } else { "a folder on this machine" });
    if crate::ddl::has_catalog(&dir).await? {
        ensure!(if_not_exists, "a lake is at {dir} already: ATTACH '{dir}' AS {name} (or CREATE DATABASE IF NOT EXISTS {name} CLONE {})", of.from);
        return Box::pin(crate::ddl::apply(lake, Ddl::Attach { name: name.into(), dir, read_only: false, endpoint: None })).await;
    }
    // The maker's record, as this lake has it: the branch makes it a superuser (`make`). Only a
    // user of this database has one; a token's name has none, and so no owner.
    let owner = match &of.owner {
        Some(n) => lake.cat.get::<crate::users::User>(&crate::users::user_key(n)).await?,
        None => None,
    };
    let owner_name = owner.as_ref().and(of.owner.clone());
    // Pins first, then the branch's first commit, which reads its base at a version that holds
    // its pin: every file it lists is then kept.
    let mut lakes = inherited.lakes.clone();
    let mut pinned = vec![];
    let made = async {
        let pinning = pin_in(lake, &base, &dir, None, None, away.is_some()).await;
        let p = match base == lake.url {
            true => pinning?,
            false => pinning.with_context(|| match &away {
                Some(r) => format!("pinning it in {}: its leader{} is asked with the TYPE pondra secret's token, whose user needs GRANT CLONE there", of.from, r.endpoint.as_ref().map(|e| format!(" at {e}")).unwrap_or_default()),
                None => format!("pinning it in {}: a database that signs in on its own is cloned on its own node (CREATE DATABASE … CLONE … there), or by nodes that share its admin token", of.from),
            })?,
        };
        pinned.push((base.clone(), p.key.clone()));
        let schemas = schemas_taken(&of, p.schemas)?; // (refused here, after the pin: the error path lets it go)
        for (id, b) in inherited.lakes.iter() {
            // (the key their pins give this branch, not the one its base gave it)
            let key = pin_in(lake, &b.url, &dir, Some(b.ms), None, b.away()).await?.key;
            if let Some(e) = lakes.get_mut(id) {
                e.key = key.clone();
                e.sealed = None; // (the inherited sealed key is the base branch's, never ours)
            }
            pinned.push((b.url.clone(), key));
        }
        let (endpoint, read_only) = (away.as_ref().and_then(|r| r.endpoint.clone()), away.as_ref().is_some_and(|r| r.read_only));
        lakes.insert(id_of(&base), Base { url: base.clone(), ms: p.ms, key: p.key, sealed: None, endpoint, read_only });
        for b in lakes.values_mut() {
            b.seal()?; // (sealed before they leave this node: the request to the branch's leader carries them so)
        }
        // (the bucket keys of the bases on other servers, which the branch reads with)
        let urls: Vec<String> = lakes.values().filter(|b| b.read_only).map(|b| b.url.clone()).collect();
        let secrets = crate::ext::lent(lake, &urls).await?;
        let make = Make { base: base.clone(), me: dir.clone(), lakes, schemas, data: of.data, secrets, owner, owner_name };
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
    let attached = Box::pin(crate::ddl::apply(lake, Ddl::Attach { name: name.into(), dir, read_only: false, endpoint: None })).await?;
    Ok(j!({"database": name, "cloned": of.from, "tables": made["tables"], "dir": attached["dir"]}))
}

/// The schemas a clone takes: the ones it names, within what its base's pin lets this caller clone (a
/// user granted CLONE on some schemas only; none: all of them, and none named: the ones allowed).
fn schemas_taken(of: &CloneOf, allowed: Option<Vec<String>>) -> Result<Vec<String>> {
    let Some(allowed) = allowed else { return Ok(of.schemas.clone()) };
    ensure!(!allowed.is_empty(), "{} lets this server clone no schema (GRANT CLONE ON SCHEMA … there)", of.from);
    if of.schemas.is_empty() {
        return Ok(allowed);
    }
    ensure!(of.schemas.iter().all(|s| allowed.contains(s)), "{} lets this server clone {} only (GRANT CLONE ON SCHEMA … there)", of.from, allowed.join(", "));
    Ok(of.schemas.clone())
}

/// What a user granted CLONE may ask over `/cluster/ddl`: a pin of a branch, and nothing else (ADR-058).
pub fn cloning(d: &Ddl) -> Result<()> {
    ensure!(matches!(d, Ddl::Pin { .. }), "permission denied: CLONE pins a branch, and does nothing else");
    Ok(())
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

/// What a pin answers: its time, and when it was written (by that leader's clock), the key its pin
/// gives the branch, and the schemas its user may clone (None: all of them).
struct Pinned {
    ms: u64,
    at_ms: u64,
    key: Option<String>,
    schemas: Option<Vec<String>>,
}

/// Pin `branch` in the lake at `at`: here, or through its own leader (invariant 20), with `key` when
/// it has one (a branch's renewal, `keyed`). `lease`: the branch is on another server (`pin`).
async fn pin_in(lake: &Lake, at: &str, branch: &str, ms: Option<u64>, key: Option<&str>, lease: bool) -> Result<Pinned> {
    let v = match at == lake.url {
        true => pin(lake, branch, ms, lease).await?,
        false => Box::pin(crate::write::send_as(at, Request::Ddl(Ddl::Pin { lake: branch.into(), ms, lease }), key)).await?,
    };
    Ok(Pinned {
        ms: v["ms"].as_u64().context("a pin's time")?,
        at_ms: v["at_ms"].as_u64().unwrap_or(0),
        key: v["key"].as_str().map(String::from),
        schemas: v["schemas"].as_array().map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect()),
    })
}

async fn unpin_in(lake: &Lake, at: &str, branch: &str, key: Option<&str>) -> Result<()> {
    match at == lake.url {
        true => drop(unpin(lake, branch).await?),
        false => drop(Box::pin(crate::write::send_as(at, Request::Ddl(Ddl::Unpin { lake: branch.into() }), key)).await?),
    }
    Ok(())
}

/// A base's leader: keep every file live at `ms` (now, less a margin, unless a branch of a branch
/// carries its own base's on) while `branch` is there. A lease (a branch on another server) goes when
/// it isn't renewed (`lapse`).
pub async fn pin(lake: &Lake, branch: &str, ms: Option<u64>, lease: bool) -> Result<Value> {
    let (key, now) = (pin_key(branch), crate::log::now_ms());
    let mut ms = ms.unwrap_or(now.saturating_sub(MARGIN_MS));
    // (a user who may only clone is a server elsewhere's: its pin is a lease, and once one, always)
    let mut lease = lease || crate::auth::limited().and_then(|a| a.clones()).is_some();
    if let Some(p) = lake.cat.get::<Pin>(&key).await? {
        ms = ms.min(p.ms);
        lease |= p.lease;
    }
    lake.cat.commit(vec![(key, json(&Pin { lake: branch.into(), ms, at_ms: now, lease }))], &[]).await?;
    let mut out = j!({"pinned": branch, "ms": ms, "at_ms": now, "key": key_for(lake, branch).await?});
    // (a user granted CLONE on some schemas only: the branch takes only those, and says so here)
    if let Some(s) = crate::auth::limited().and_then(|a| a.clones()).filter(|s| !s.is_empty()) {
        out["schemas"] = j!(s);
    }
    Ok(out)
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
        Ddl::Pin { lake: b, ms: None, .. } | Ddl::Unpin { lake: b } => b,
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

/// Where a statement comes in (`write::on_node_listed`, instead of `may`): what a user who isn't a
/// superuser may branch or drop, and who makes a branch, its owner (ADR-058). CLONE is enough to make
/// one; a branch is dropped by its owner and by nobody else. Admins and superusers are checked as
/// before (`may`).
pub async fn door(lake: &Lake, d: &mut Ddl) -> Result<()> {
    match crate::auth::limited() {
        Some(a) => user_door(lake, d, &a).await?,
        None => may(lake, d).await?,
    }
    stamp(lake, d).await
}

/// A user who isn't a superuser: its grants decide a branch or a drop (`Auth::allows` lets nothing
/// else of its through).
async fn user_door(lake: &Lake, d: &mut Ddl, a: &crate::users::Access) -> Result<()> {
    if let Ddl::CreateDatabase { clone: Some(of), .. } = d {
        return clone_as(lake, of, a);
    }
    if let Ddl::DropDatabase { name, if_exists } = d {
        return drop_as(lake, name.as_str(), *if_exists).await;
    }
    may(lake, d).await
}

/// A branch of a database, made by a user with CLONE: on this database, as its grants let it (every
/// schema, or those granted, and only those it names); on a database attached here, by its own grant.
/// Prod still decides what the attachment's token may pin (`pin`), so `may` isn't asked there.
fn clone_as(lake: &Lake, of: &mut CloneOf, a: &crate::users::Access) -> Result<()> {
    let from = of.from.trim_matches('"').to_lowercase();
    let here = crate::ddl::lake_name(lake);
    let me = crate::auth::current().map(|p| p.name).unwrap_or_default();
    if from != here {
        ensure!(a.clones_of(&from), "permission denied: a branch of {from} here needs CLONE on it (GRANT CLONE ON DATABASE {from} TO {me}, run on {here})");
        return Ok(());
    }
    let Some(schemas) = a.clones() else { bail!("permission denied: a branch of {here} needs CLONE (GRANT CLONE ON DATABASE {here} TO {me})") };
    if schemas.is_empty() {
        return Ok(()); // (CLONE ON DATABASE: every schema)
    }
    if of.schemas.is_empty() {
        of.schemas = schemas;
    } else {
        ensure!(of.schemas.iter().all(|s| schemas.contains(s)), "permission denied: {me} may clone {} of {here} only (GRANT CLONE ON SCHEMA … TO {me})", schemas.join(", "));
    }
    Ok(())
}

/// A branch dropped by a user who isn't a superuser: by its owner alone (an admin drops any). A
/// shared one (test, a pull request's) is CI's, so a developer who may clone prod can't drop it. A
/// database that isn't there is nothing to drop with IF EXISTS.
async fn drop_as(lake: &Lake, name: &str, if_exists: bool) -> Result<()> {
    let me = crate::auth::current().map(|p| p.name).unwrap_or_default();
    // (attached here, or beside this lake on its server, not yet attached to this node)
    let attached = lake.attached.read().unwrap().iter().find(|(n, _)| *n == name).map(|(_, l)| l.url.clone());
    let url = attached.unwrap_or_else(|| crate::ddl::beside(&lake.url, name));
    if !crate::ddl::has_catalog(&url).await? {
        ensure!(if_exists, "no database {name}");
        return Ok(());
    }
    let bases = Lake::open(&url, false, false).await?.cat.get::<Bases>(BASES).await?;
    match bases.and_then(|b| b.owner) {
        Some(owner) if owner == me => Ok(()),
        Some(owner) => bail!("permission denied: {name} is {owner}'s branch: {owner} or an admin drops it"),
        None => bail!("permission denied: DROP DATABASE {name} needs an admin (a user drops only the branches it made)"),
    }
}

/// Who makes a branch: a user of this database (not a token's name) is stamped on the statement where
/// it comes in. `create` reads that user's record, and `make` makes it the branch's superuser.
async fn stamp(lake: &Lake, d: &mut Ddl) -> Result<()> {
    let Ddl::CreateDatabase { clone: Some(of), .. } = d else { return Ok(()) };
    let me = crate::auth::current().map(|p| p.name).unwrap_or_default();
    if !me.is_empty() && lake.cat.get::<crate::users::User>(&crate::users::user_key(&me)).await?.is_some() {
        of.owner = Some(me);
    }
    Ok(())
}

/// The pins clean-up keeps files for: the oldest and the newest (None: no branch reads this lake).
pub async fn pins(lake: &Lake) -> Result<Option<(u64, u64)>> {
    let all = lake.cat.scan::<Pin>("pn/", "pn0").await?;
    Ok(all.iter().map(|(_, p)| p.ms).min().zip(all.iter().map(|(_, p)| p.ms).max()))
}

/// Hourly, on a base's leader: a pin whose branch is gone (its lake deleted without a DROP
/// DATABASE here) goes too. An hour's grace: a pin is taken just before its branch is made. These are
/// the pins of branches on this lake's own storage, the only ones it can look at; a pin on lease (a
/// branch on another server) goes by `lapse`.
pub async fn sweep(lake: &Lake) -> Result<()> {
    let now = crate::log::now_ms();
    for (key, p) in lake.cat.scan::<Pin>("pn/", "pn0").await? {
        if p.at_ms + 3_600_000 < now && !crate::ddl::has_catalog(&p.lake).await.unwrap_or(true) {
            lake.cat.commit(vec![], &[key]).await?;
        }
    }
    Ok(())
}

/// How long a base keeps a pin on lease that isn't renewed (`PONDRA_PIN_LEASE_SECS`, 14 days).
fn lease_ms() -> u64 { secs("PONDRA_PIN_LEASE_SECS", 14 * 86_400) * 1000 }

/// How often a branch renews its pins on lease (`PONDRA_PIN_RENEW_SECS`, an hour).
pub fn renew_every() -> std::time::Duration { std::time::Duration::from_secs(secs("PONDRA_PIN_RENEW_SECS", 3600)) }

fn secs(name: &str, default: u64) -> u64 { std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default) }

/// A base's leader, each retention round: a pin on lease that wasn't renewed within
/// `PONDRA_PIN_LEASE_SECS` goes. Its branch is on another server, which this lake can't look at
/// (`sweep`), so a branch nobody uses there any more stops holding this lake's files.
pub async fn lapse(lake: &Lake) -> Result<()> {
    let now = crate::log::now_ms();
    let lapsed: Vec<(String, Pin)> = lake.cat.scan::<Pin>("pn/", "pn0").await?.into_iter().filter(|(_, p)| p.lease && p.at_ms + lease_ms() < now).collect();
    if lapsed.is_empty() {
        return Ok(());
    }
    for (_, p) in &lapsed {
        eprintln!("{}'s pin let go: not renewed for {} s (PONDRA_PIN_LEASE_SECS)", p.lake, lease_ms() / 1000);
    }
    lake.cat.commit(vec![], &lapsed.into_iter().map(|(k, _)| k).collect::<Vec<_>>()).await?;
    Ok(())
}

/// Leader, every `renew_every()`: the pins on lease this lake holds as a branch, and those of the
/// branches attached here (a branch nobody leads is renewed by whoever uses it), renewed with each
/// branch's own key, so their bases keep their files (`lapse`). Only bases on other servers, and
/// only with the key: a renewal never makes a pin that was let go.
pub async fn renew(lake: &Lake) {
    let mut lakes = vec![lake.arc()];
    lakes.extend(lake.attached.read().unwrap().iter().map(|(_, l)| l.clone()));
    for l in lakes {
        let Ok(Some(b)) = l.cat.get::<Bases>(BASES).await else { continue };
        for base in b.lakes.values().filter(|b| b.away()) {
            let renewed = async {
                let Some(key) = base.key()? else { return Ok(()) };
                pin_in(&l, &base.url, &b.me, None, Some(&key), true).await.map(|_| ())
            };
            if let Err(e) = renewed.await {
                eprintln!("renewing {}'s pin in {}: {e:#} (a pin let go isn't made again: what the branch reads of it may be gone; DROP DATABASE it and CLONE again)", b.me, base.url);
            }
        }
    }
}

/// Leader: this branch's keys in its bases sealed by the master key once every node shares one, and
/// wrapped again after it changed, as the lake's own keys are (`users::seal_keys`).
pub async fn seal_keys(lake: &Lake) -> Result<()> {
    let Some(mut b) = lake.cat.get::<Bases>(BASES).await? else { return Ok(()) };
    let mut changed = false;
    for base in b.lakes.values_mut() {
        changed |= base.seal()?;
    }
    if changed {
        crate::format::require(lake, crate::users::SEALED, "a branch's keys, sealed").await?;
        lake.cat.commit(vec![(BASES.into(), json(&b))], &[]).await?;
        eprintln!("this branch's keys in its bases are sealed by its master key now");
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
    for b in m.lakes.values().filter(|b| b.read_only) {
        crate::ext::reach_with(&m.secrets, &b.url, true, b.endpoint.as_deref())?; // (its key and its leader, before it is read)
    }
    let base = Lake::open(&m.base, false, false).await.with_context(|| format!("opening {}", m.base))?;
    let id = id_of(&m.base);
    let pin = pin_key(&m.me);
    let snap = snapshot(&base, &pin, 0).await?;
    let number = |k: &str| snap.get(k).and_then(|v| serde_json::from_slice::<u64>(v).ok()).unwrap_or(0);
    let (commit, next, block) = (number("c"), number("n").max(1), number("b"));
    let end = next - 1;
    let wanted = |name: &str| took(&m.schemas, name);
    // (what a branch never takes: the kinds the registry marks `Leave` (a secret, a share and a
    // recipient, with their comments: a branch hands nothing to another company), and the catalog's
    // own keys, which no kind has)
    let mut deny: Vec<String> = ["z/", "x/", "i/", "dt/", "jt/", "pn/", "fd/", "s/", "d/"].iter().map(|p| p.to_string()).collect();
    for k in KINDS.iter().filter(|k| k.on_clone == OnClone::Leave) {
        deny.push(k.prefix.to_string());
        deny.push(format!("cm/{}/", k.family));
    }
    let mut puts: Vec<(String, Vec<u8>)> = m.secrets.iter().map(|(name, secret)| (crate::ext::secret_key(name), json(secret))).collect(); // (the base's own `e/` stays denied)
    // (a base on another server: its users and grants sign in there; the branch is this server's,
    // signed in to as this server's databases are: ADR-058)
    let away = m.lakes.get(&id).is_some_and(|b| b.read_only);
    let (mut tables, mut tiered) = (0, end);
    for (k, v) in &snap {
        let (prefix, name) = k.split_once('/').map_or((k.as_str(), ""), |(p, n)| (p, n));
        match prefix {
            _ if deny.iter().any(|d| k.starts_with(d)) => {}
            "u" if away => {}
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
    // The maker owns what it made: a superuser there, whatever it may do in the database it was made
    // from. (Whether the base's users were copied above or not, `"u" if away`, its record is this one.)
    if let (Some(record), Some(name)) = (&m.owner, &m.owner_name) {
        let key = crate::users::user_key(name);
        puts.retain(|(k, _)| *k != key);
        puts.push((key, json(&crate::users::User { superuser: true, ..record.clone() })));
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
    let me = Bases { base: m.base.clone(), at_ms: crate::log::now_ms(), me: m.me.clone(), lakes: m.lakes.clone(), schemas: m.schemas.clone(), data: m.data, owner: m.owner_name.clone() };
    puts.push((BASES.into(), json(&me)));
    if m.lakes.values().any(|b| b.sealed.is_some()) {
        crate::format::require(lake, crate::users::SEALED, "a branch's keys, sealed").await?; // (a release before it finds no key in a base, and can't renew its pin)
    }
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
        let key = match b.url == lake.url {
            true => None, // (a base on this node lets go without a key)
            false => b.key().unwrap_or_else(|e| {
                eprintln!("{e:#}");
                None
            }),
        };
        if let Err(e) = unpin_in(lake, &b.url, &bases.me, key.as_deref()).await {
            eprintln!("releasing {}'s pin in {}: {e:#} (the base lets it go by itself: its hourly sweep, or the lease running out)", bases.me, b.url);
        }
    }
}

/// `pondra.databases`: this database and the ones attached here, with what each was branched from.
pub async fn databases(lake: &Lake) -> Result<datafusion::arrow::record_batch::RecordBatch> {
    use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray};
    use std::sync::Arc;
    let mut all = vec![(crate::ddl::lake_name(lake), lake.arc())];
    all.extend(lake.attached.read().unwrap().iter().cloned());
    let named = |url: &str| all.iter().find(|(_, l)| l.url == url).map_or(url.to_string(), |(n, _)| n.clone());
    let mut rows = vec![];
    let mut lifts = vec![]; // (protected, lifted_at in µs, lifted_by), per row: a lifted protection shows until it is set again
    for (name, l) in &all {
        let b = l.cat.get::<Bases>(BASES).await.ok().flatten();
        let branches = l.cat.scan::<Pin>("pn/", "pn0").await.map(|p| p.len()).unwrap_or(0);
        let owner = b.as_ref().and_then(|b| b.owner.clone());
        rows.push((name.clone(), l.url.clone(), b.as_ref().map(|b| named(&b.base)), b.map(|b| b.at_ms as i64 * 1000), branches as i64, owner));
        lifts.push(match crate::protect::of(l).await.ok().flatten() {
            Some(p) if p.protected => (true, None, None),
            Some(p) => (false, Some(p.at_ms as i64 * 1000), Some(p.by)),
            None => (false, None, None),
        });
    }
    let s = |f: &dyn Fn(&(String, String, Option<String>, Option<i64>, i64, Option<String>)) -> Option<String>| Arc::new(rows.iter().map(f).collect::<StringArray>()) as ArrayRef;
    Ok(datafusion::arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("name", s(&|r| Some(r.0.clone()))),
        ("location", s(&|r| Some(r.1.clone()))),
        ("base", s(&|r| r.2.clone())),
        ("owner", s(&|r| r.5.clone())), // (the user who made a branch; none for an admin's branch and for a database that isn't one)
        ("branched_at", Arc::new(rows.iter().map(|r| r.3).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef),
        ("branches", Arc::new(rows.iter().map(|r| Some(r.4)).collect::<Int64Array>()) as ArrayRef),
        ("protected", Arc::new(lifts.iter().map(|l| Some(l.0)).collect::<BooleanArray>()) as ArrayRef),
        ("lifted_at", Arc::new(lifts.iter().map(|l| l.1).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef),
        ("lifted_by", Arc::new(lifts.iter().map(|l| l.2.clone()).collect::<StringArray>()) as ArrayRef),
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
    let key = me.lakes.get(&id_of(&me.base)).map(Base::key).transpose()?.flatten();
    let away = me.lakes.get(&id_of(&me.base)).is_some_and(Base::away);
    let at = pin_in(lake, &me.base, &me.me, None, key.as_deref(), away).await?.at_ms;
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
