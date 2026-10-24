//! CREATE OR REPLACE TABLE: the new table is made under a staged name, filled, and the leader puts it in the old
//! one's place in one commit (invariant 425). In steps:
//! - the statement makes `t$new-<job>` (`sys::staged`: hidden, its `made_by` the job) and fills it, as any table's rows
//!   go in; `t` is as it was until then, so a failed or stopped statement leaves it so (`replacing`);
//! - the leader, in one commit (`Ddl::ReplaceTable` into `replace`), moves the staged table's entries to `t`, keeping the
//!   folder they have so the published copies stay where other engines read them, and sets the old `t` aside as a drop
//!   does, so UNDROP brings it back;
//! - the formats the new table publishes go on from the old state (Iceberg's snapshots are let go: `iceberg::reset`) and
//!   are published after the commit; a format the new table can't publish is taken out of `t`'s copies, so no engine
//!   reads the old rows as `t`;
//! - a staged table a node left behind is let go once older than `PONDRA_STAGED_SECS` (`drop_stale_staged`).

use crate::ddl::{drop_table, folder_users, keeps, left_shares, mark, move_entry, no_followers, publishes_alone, query_key, set_aside, still, table_only, StoredView};
use crate::store::{table_key, Lake, TableMeta};
use crate::write::{create_spec, made_by_job, undo_made, Stmt};
use anyhow::{ensure, Result};
use datafusion::sql::sqlparser::ast;
use serde_json::{json as j, Value};

/// CREATE OR REPLACE TABLE: the new table is made under a staged name (`sys::staged`), its rows go in, and the leader puts it
/// in `name`'s place in one commit (`Ddl::ReplaceTable`). Until then `name` is as it was, so a failed or stopped statement
/// leaves it so. A retry whose replace went through answers as a duplicate; a retry whose rows went into a table that was let
/// go is refused by name (its job's rows are recorded, so they would not be written again).
pub(crate) async fn replacing<F>(run: &F, lake: &Lake, c: &ast::CreateTable, name: &str, job: &str, files: bool, armed: &mut (dyn FnMut(&str) + Send)) -> Result<Value>
where
    F: Fn(Stmt) -> futures::future::BoxFuture<'static, Result<Value>>,
{
    if crate::ddl::resolve(lake, name).await?.0.is_some() {
        return Err(crate::codes::coded("0A000", format!("CREATE OR REPLACE TABLE {name}: another database's table is replaced from a node of that database")));
    }
    if lake.cat.get::<TableMeta>(&table_key(name)).await?.is_some_and(|m| m.made_by.as_deref() == Some(job)) {
        return Ok(j!({"table": name, "duplicate": true})); // (a retry: the replace this job sent went through)
    }
    replaceable(lake, name).await?; // (refused before any query runs)
    let staged = crate::sys::staged(name, job);
    let existed = lake.cat.get::<TableMeta>(&table_key(&staged)).await?.is_some(); // (a node left this job's staged table: its rows are in it)
    let spec = made_by_job(create_spec(c, lake, files).await?, job)?;
    let mut out = run(Stmt::Define(staged.clone(), spec)).await.map_err(|e| named(e, &staged, name))?;
    armed(&staged);
    if let Some(q) = &c.query {
        let inserted = run(Stmt::Insert(staged.clone(), q.to_string())).await.map_err(|e| named(e, &staged, name));
        let rows = match inserted {
            Ok(v) if v["duplicate"] == true && !existed => {
                Err(anyhow::anyhow!("{name}: this statement's rows were written before, into a table that was let go, so they can't be written again: run the statement again with a new job"))
            }
            other => other,
        };
        out = match rows {
            Ok(v) => v,
            Err(e) => {
                undo_made(run, &staged, job).await; // (nothing replaced: `name` is as it was)
                return Err(e);
            }
        };
    }
    let replace = crate::ddl::Ddl::ReplaceTable { name: name.to_string(), staged: staged.clone() };
    if let Err(e) = run(Stmt::Ddl(vec![replace])).await {
        undo_made(run, &staged, job).await;
        return Err(named(e, &staged, name));
    }
    if let Some(t) = out.get_mut("table") {
        *t = Value::from(name); // (the staged table's answer names the table it replaced)
    }
    Ok(out)
}

/// An error of a replace's steps, said of the table the statement named: the staged name (`t$new-…`) becomes `t`, and its code
/// stays. An error that doesn't name the staged table goes on as it is.
fn named(e: anyhow::Error, staged: &str, name: &str) -> anyhow::Error {
    if !format!("{e:#}").contains(staged) {
        return e;
    }
    let text = format!("{e:#}").replace(staged, name);
    match crate::codes::of(&e) {
        "XX000" => anyhow::anyhow!(text),
        code => crate::codes::coded(code, text),
    }
}

/// What CREATE OR REPLACE TABLE refuses to replace (the leader, and a node before it runs the statement's query): a view, a
/// table `table_only` leaves alone, a hidden table, or a table that a materialized view or a task keeps rows derived from (drop
/// those first). Stored views read a table by name, so they keep working, as after a rename.
pub(crate) async fn replaceable(lake: &Lake, name: &str) -> Result<()> {
    ensure!(lake.cat.get::<StoredView>(&query_key(name)).await?.is_none(), "{name} is a view: CREATE OR REPLACE VIEW changes a view");
    table_only(lake, name, "replaced").await?;
    ensure!(!crate::sys::hidden(name), "{name} is written by Pondra alone");
    no_followers(lake, name, "they keep rows derived from what it holds").await
}

/// `CREATE OR REPLACE TABLE`'s last step (leader, under the lock): the staged table, filled, takes `name`'s place in one commit.
/// `name`'s entries are set aside as a drop sets them, so UNDROP brings them back; the kept copy is unpublished, since its
/// published copies are the new table's now. The staged table's entries move under its name with the folder they have (the old
/// one's, `create_table`), so the published copies stay where other engines read them. A format the new table publishes too
/// continues its state: the same log and metadata, as Delta's own CREATE OR REPLACE does (Iceberg's snapshots are let go, as
/// its schema is the old one's: `iceberg::reset`). The other formats' states go in the commit, their objects after it. All of
/// them take the largest `tiered` mark: the log between two marks holds rows of the other table, already in files (as
/// `exchange` does). The new table was made now, so `name AT (…)` before this is refused rather than answered with the staged
/// rows (`past_from`). A staged table is never published: the new table publishes after the commit, and a format it can't
/// publish (or whose round fails) is taken out, so no engine reads the old rows as `name`.
pub(crate) async fn replace(lake: &Lake, name: &str, staged: &str) -> Result<Value> {
    if lake.cat.get::<TableMeta>(&table_key(staged)).await?.is_none() {
        // (a repeat of a replace that committed, or the staged table was let go: the job that made `name` says which)
        let job = lake.cat.get::<TableMeta>(&table_key(name)).await?.and_then(|m| m.made_by);
        ensure!(job.is_some_and(|j| crate::sys::staged(name, &j) == staged), "{name}: its new table is gone (its statement failed or expired): run the statement again");
        return Ok(j!({"table": name, "replaced": true}));
    }
    replaceable(lake, name).await?;
    let (old_deleted, staged_deleted) = (crate::sys::deleted(name), crate::sys::deleted(staged));
    let _held = still(lake, &[name.to_string(), old_deleted.clone(), staged.to_string(), staged_deleted.clone()]).await?;
    let old = lake.cat.get::<TableMeta>(&table_key(name)).await?;
    let old_del = lake.cat.get::<TableMeta>(&table_key(&old_deleted)).await?;
    let new_del = lake.cat.get::<TableMeta>(&table_key(&staged_deleted)).await?;
    let new = lake.cat.get::<TableMeta>(&table_key(staged)).await?.ok_or_else(|| anyhow::anyhow!("no table {staged}"))?;
    // The new table's files are in the folder it was given when the statement started: the old table's, or a free one. A
    // rename or drop of `name` since then would leave them elsewhere: refused, the statement runs again.
    let folder = new.folder(staged).to_string();
    let same_place = match &old {
        Some(m) => folder == m.folder(name),
        None => !folder_users(lake).await?.get(&folder).is_some_and(|users| users.iter().any(|(user, _)| user != staged)), // (live or kept)
    };
    ensure!(same_place, "{name} changed while its statement ran: run it again");
    if !new.publish.is_empty() {
        publishes_alone(lake, name, &folder, &[name, staged]).await?;
    }
    let mark = mark([old.as_ref(), old_del.as_ref(), new_del.as_ref(), Some(&new)]);
    let (at, now) = (lake.visible(), crate::log::now_ms());
    let kept = old.as_ref().is_some_and(|m| keeps(name, m, false));
    let (mut puts, shares) = match &old {
        Some(m) => set_aside(lake, name, &TableMeta { publish: vec![], ..m.clone() }, kept).await?,
        None => (vec![], vec![]),
    };
    let stamp = |m: &mut TableMeta| {
        m.tiered = mark;
        m.made = Some((at, now));
        m.past_from = Some((at, now));
    };
    let (moved, mut gone) = move_entry(lake, staged, name, stamp).await?;
    puts.extend(moved);
    gone.extend([table_key(name), table_key(&old_deleted)]); // (the old's, where the moved ones don't go over them)
    let dropped: Vec<String> = ["delta", "iceberg"].into_iter().map(String::from).filter(|f| !new.publish.contains(f)).collect();
    let dropped = crate::delta::Copies::of(lake, name, &folder, &dropped).await?; // (the formats the new table doesn't publish)
    gone.extend(dropped.keys());
    let mut stale = vec![]; // (the snapshots Iceberg let go: deleted once the new version is written)
    if new.publish.iter().any(|f| f == "iceberg") {
        if let Some(state) = lake.cat.get_raw(&crate::delta::state_key(name, "iceberg")).await? {
            let (reset, objects) = crate::iceberg::reset(&state, &folder)?;
            puts.push((crate::delta::state_key(name, "iceberg"), reset));
            stale = objects;
        }
    }
    gone.retain(|k| !puts.iter().any(|(p, _)| p == k));
    lake.cat.commit(puts, &gone).await?;
    if let (false, Some(m)) = (kept, &old) {
        crate::seq::drop_owned(lake, m).await?; // (after the commit: the new table's identity sequences are its own now)
    }
    // After the commit each step is logged, not returned: the replace has happened.
    dropped.delete(lake).await;
    if let Some(m) = &old {
        lake.hot.forget(&m.files.iter().map(|f| (f.path.clone(), f.bytes)).chain(m.garbage.iter().cloned()).collect::<Vec<_>>()); // (the old's columns held as its types: the new files share the folder)
    }
    // The formats the new table publishes: their copies go out now. One left as it was (the round failed, or the new table can't be
    // published yet: a keyed table with several generations) still shows the old table's rows as `name`, and is taken out.
    let mut before = vec![];
    for format in &new.publish {
        before.push((format.clone(), state_bytes(lake, name, format).await));
    }
    let round = crate::delta::publish_all(lake).await;
    if let Err(e) = &round {
        eprintln!("publishing {name}, which was replaced: {e:#}"); // (the replace has happened: the next round publishes it)
    }
    let unpublishable = crate::delta::publishable(lake, &new).await.map_or(true, |p| p.is_none());
    for (format, was) in before {
        if (round.is_err() || unpublishable) && state_bytes(lake, name, &format).await == was {
            if let Err(e) = crate::delta::unpublish(lake, name, &format).await {
                eprintln!("replacing {name}: taking its {format} copy out: {e:#}");
            }
        }
    }
    for object in &stale {
        lake.delete(object).await; // (after the round: until its version is written, readers find the old one's)
    }
    Ok(left_shares(j!({"table": name, "replaced": true}), shares))
}

/// A table's published state in `format` as the catalog holds it now (None: none).
async fn state_bytes(lake: &Lake, table: &str, format: &str) -> Option<Vec<u8>> {
    lake.cat.get_raw(&crate::delta::state_key(table, format)).await.ok().flatten().map(|b| b.to_vec())
}

/// Staged tables a node left behind (killed between its definition and its replace) are let go once older than
/// `PONDRA_STAGED_SECS` (`sys::stale_staged`), purged as a failed replace's are. `tables` is the table entries the caller
/// has just read (`tier::expire`'s scan, so no second one); the tables let go are taken out of it.
pub async fn drop_stale_staged(lake: &Lake, tables: &mut Vec<(String, TableMeta)>) -> Result<()> {
    let (now, secs) = (crate::log::now_ms(), crate::env::or("PONDRA_STAGED_SECS", 86_400u64));
    let stale: Vec<String> = tables
        .iter()
        .filter(|(key, m)| m.made.is_some_and(|(_, made_ms)| crate::sys::stale_staged(&key[2..], made_ms, now, secs)))
        .map(|(key, _)| key[2..].to_string())
        .collect();
    for name in &stale {
        drop_table(lake, name, true, true, None).await?;
    }
    tables.retain(|(key, _)| !stale.iter().any(|name| name.as_str() == &key[2..]));
    Ok(())
}
