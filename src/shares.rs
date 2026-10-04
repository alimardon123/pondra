//! Shares and recipients (ADR-046): what another company may read of the lake, and who. A share
//! names tables (or an append table's partitions) under the names a recipient sees them by,
//! `schema.table`; a recipient is a token another company holds, kept as its SHA-256 as users'
//! tokens are, which signs in only at the sharing door (`sharing.rs`). `GRANT SELECT ON SHARE s TO
//! RECIPIENT r` lets it read the share. Kept in the catalog: `sh/<share>`, `sr/<recipient>`
//! (invariant 104); their comments where every object's are (`cm/share/…`, `cm/recipient/…`:
//! `objects.rs`). Databricks' words; Snowflake's `GRANT SELECT ON TABLE t TO SHARE s` too. As in
//! Snowflake, a share follows its table through a rename and loses it with a drop (`tables_moved`),
//! so a new table under a dropped one's name is shared only once it is granted.
//!
//! A shared table publishes Delta (`delta.rs`): the door hands out the files of a version of its
//! log, so a recipient sees what Delta's readers see — deletion vectors, renamed columns, purged
//! changes — and nothing that isn't durable yet (invariant 16).
use crate::store::{json, table_key, Lake, TableMeta};
use crate::users::{Words, W};
use anyhow::{anyhow, bail, ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64U, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::sync::Arc;

pub fn share_key(n: &str) -> String { format!("sh/{n}") }
pub fn recipient_key(n: &str) -> String { format!("sr/{n}") }

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Share {
    pub id: String,
    #[serde(default)]
    pub tables: Vec<Shared>,
    #[serde(default)]
    pub recipients: Vec<String>,
    #[serde(default)]
    pub created_ms: u64,
}

/// A table in a share: the lake's table, the name the recipient sees, and what of it.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Shared {
    pub id: String,
    pub table: String,  // as the catalog names it
    pub schema: String, // as the recipient sees it
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<String>, // only the files of these values of its `partition_by` column (none: all)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub history: bool, // older versions too (`WITH HISTORY`)
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Recipient {
    pub id: String,
    pub hash: String, // its token's SHA-256 (hex): the token is shown once, in its profile
    pub created_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<u64>,
}

/// What a statement about shares changes: the leader carries it out (`ddl::Ddl::Shares`).
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "do", rename_all = "snake_case")]
pub enum Change {
    CreateShare { name: String, comment: Option<String>, if_not_exists: bool },
    DropShare { name: String, if_exists: bool },
    AddTable { share: String, table: String, as_name: Option<String>, partitions: Vec<(String, String)>, history: bool },
    RemoveTable { share: String, table: String },
    CreateRecipient { name: String, comment: Option<String>, expires_secs: Option<u64>, if_not_exists: bool, endpoint: String },
    RotateToken { name: String, expires_secs: Option<u64>, endpoint: String },
    DropRecipient { name: String, if_exists: bool },
    Grant { share: String, recipients: Vec<String> },
    Revoke { share: String, recipients: Vec<String> },
}

// ---------------------------------------------------------------- statements

/// `CREATE | ALTER | DROP SHARE`, `CREATE | ALTER | DROP RECIPIENT`, `GRANT | REVOKE SELECT ON
/// SHARE s TO | FROM RECIPIENT r`, `GRANT | REVOKE SELECT ON TABLE t TO | FROM SHARE s`; None for
/// anything else (before `users.rs`, which takes every other GRANT).
pub fn statement(sql: &str) -> Option<crate::write::Stmt> {
    let mut w = Words::of(sql)?;
    let words: Vec<String> = w.0.iter().rev().map(W::word).collect();
    let ours = match words.first()?.as_str() {
        "create" | "alter" | "drop" => matches!(words.get(1).map(String::as_str), Some("share" | "recipient")),
        "grant" | "revoke" => words.windows(2).any(|p| matches!(p[0].as_str(), "on" | "to" | "from") && p[1] == "share"),
        _ => false,
    };
    if !ours {
        return None;
    }
    Some(match parse(&mut w) {
        Ok(change) => crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::Shares(change)]),
        Err(e) => crate::write::Stmt::Invalid(e.to_string()),
    })
}

fn parse(w: &mut Words) -> Result<Change> {
    let verb = w.next().map(|x| x.word()).unwrap_or_default();
    if verb == "grant" || verb == "revoke" {
        return grant(w, verb == "grant");
    }
    let kind = w.next().map(|x| x.word()).unwrap_or_default();
    Ok(match (verb.as_str(), kind.as_str()) {
        ("create", _) => {
            let if_not_exists = w.is("if") && { w.expect("not")?; w.expect("exists")?; true };
            let name = plain(&w.name()?)?;
            let (mut comment, mut expires_secs) = (None, None);
            while !w.0.is_empty() {
                if w.is("comment") {
                    w.sym('=');
                    comment = Some(w.string()?);
                } else if kind == "recipient" && w.is("expires") {
                    w.expect("in")?;
                    expires_secs = Some(span(&w.string()?)?);
                } else {
                    bail!("expected COMMENT '…'{} {}", if kind == "recipient" { " or EXPIRES IN '90 days'" } else { "" }, w.near());
                }
            }
            match kind.as_str() {
                "share" => Change::CreateShare { name, comment, if_not_exists },
                _ => Change::CreateRecipient { name, comment, expires_secs, if_not_exists, endpoint: crate::sharing::endpoint() },
            }
        }
        ("drop", _) => {
            let if_exists = w.is("if") && { w.expect("exists")?; true };
            let name = plain(&w.name()?)?;
            w.done()?;
            match kind.as_str() {
                "share" => Change::DropShare { name, if_exists },
                _ => Change::DropRecipient { name, if_exists },
            }
        }
        ("alter", "share") => {
            let share = plain(&w.name()?)?;
            if w.is("remove") {
                w.is("table");
                let table = w.name()?;
                w.done()?;
                return Ok(Change::RemoveTable { share, table });
            }
            ensure!(w.is("add"), "ALTER SHARE {share} ADD TABLE t [PARTITION (col = 'value'), …] [AS schema.table] [WITH HISTORY], or REMOVE TABLE t");
            w.is("table");
            let table = w.name()?;
            let (mut partitions, mut as_name, mut history) = (vec![], None, false);
            while !w.0.is_empty() {
                if w.is("partition") {
                    loop {
                        ensure!(w.sym('('), "PARTITION (col = 'value'), (col = 'other') {}", w.near());
                        let column = w.name()?;
                        ensure!(w.sym('='), "PARTITION (col = 'value'): a value of the table's partition_by column {}", w.near());
                        let value = match w.next() {
                            Some(W::Str(s) | W::Num(s)) => s,
                            _ => bail!("PARTITION ({column} = 'value'): the value is a string or a number"),
                        };
                        ensure!(w.sym(')'), "PARTITION ({column} = 'value'): one column a partition {}", w.near());
                        partitions.push((column, value));
                        if !w.sym(',') {
                            break;
                        }
                    }
                } else if w.is("as") {
                    as_name = Some(w.name()?);
                } else if w.is("with") {
                    w.expect("history")?;
                    history = true;
                } else if w.is("without") {
                    w.expect("history")?;
                    history = false;
                } else {
                    bail!("expected PARTITION (…), AS schema.table or WITH HISTORY {}", w.near());
                }
            }
            Change::AddTable { share, table, as_name, partitions, history }
        }
        ("alter", "recipient") => {
            let name = plain(&w.name()?)?;
            w.expect("rotate")?;
            w.expect("token")?;
            let expires_secs = if w.is("expires") { w.expect("in")?; Some(span(&w.string()?)?) } else { None };
            w.done()?;
            Change::RotateToken { name, expires_secs, endpoint: crate::sharing::endpoint() }
        }
        _ => bail!("not a statement about shares"),
    })
}

/// `GRANT SELECT ON SHARE s TO RECIPIENT r, …` (and REVOKE … FROM), or Snowflake's `GRANT SELECT
/// ON TABLE t TO SHARE s`, which adds the table (and REVOKE … FROM SHARE, which removes it).
fn grant(w: &mut Words, grant: bool) -> Result<Change> {
    let (to, usage) = (if grant { "to" } else { "from" }, "GRANT SELECT ON SHARE s TO RECIPIENT r, or GRANT SELECT ON TABLE t TO SHARE s");
    ensure!(w.is("select"), "a share is read: {usage}");
    w.expect("on")?;
    if w.is("share") {
        let share = plain(&w.name()?)?;
        w.expect(to)?;
        ensure!(w.is("recipient"), "a share is granted to recipients: {usage}");
        let recipients = w.names()?.iter().map(|n| plain(n)).collect::<Result<Vec<_>>>()?;
        w.done()?;
        return Ok(if grant { Change::Grant { share, recipients } } else { Change::Revoke { share, recipients } });
    }
    w.is("table");
    let table = w.name()?;
    w.expect(to)?;
    w.expect("share")?;
    let share = plain(&w.name()?)?;
    w.done()?;
    Ok(match grant {
        true => Change::AddTable { share, table, as_name: None, partitions: vec![], history: false },
        false => Change::RemoveTable { share, table },
    })
}

/// A share's or recipient's name: letters, digits and _, as Delta Sharing's paths carry it.
fn plain(name: &str) -> Result<String> {
    ensure!(!name.is_empty() && name.len() <= 63 && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !name.starts_with(|c: char| c.is_ascii_digit()),
        "{name}: a share's or recipient's name is letters, digits and _, at most 63");
    Ok(name.to_string())
}

fn span(text: &str) -> Result<u64> {
    match crate::runs::every(text) {
        Ok(crate::runs::Every::Seconds(s)) if s > 0 => Ok(s),
        _ => bail!("EXPIRES IN '90 days' (or hours, minutes)"),
    }
}

// ---------------------------------------------------------------- the leader carries them out

pub async fn share(lake: &Lake, name: &str) -> Result<Option<Share>> { lake.cat.get::<Share>(&share_key(name)).await }

pub async fn shares(lake: &Lake) -> Result<Vec<(String, Share)>> {
    Ok(lake.cat.scan::<Share>("sh/", "sh0").await?.into_iter().map(|(k, s)| (k[3..].to_string(), s)).collect())
}

pub async fn recipients(lake: &Lake) -> Result<Vec<(String, Recipient)>> {
    Ok(lake.cat.scan::<Recipient>("sr/", "sr0").await?.into_iter().map(|(k, r)| (k[3..].to_string(), r)).collect())
}

fn id() -> String { uuid::Uuid::new_v4().to_string() }

/// A new share's or recipient's comment, kept where `COMMENT ON` keeps it (`cm/{kind}/{name}`); none
/// takes away one a dropped namesake may have left.
fn noted(kind: &str, name: &str, comment: Option<String>) -> (Vec<(String, Vec<u8>)>, Vec<String>) {
    let key = format!("cm/{kind}/{name}");
    match comment {
        Some(c) => (vec![(key, json(&c))], vec![]),
        None => (vec![], vec![key]),
    }
}

/// Each share's or recipient's comment (`cm/{kind}/…`), by its name.
async fn notes(lake: &Lake, kind: &str) -> Result<std::collections::HashMap<String, String>> {
    let from = format!("cm/{kind}/");
    Ok(lake.cat.scan::<String>(&from, &format!("cm/{kind}0")).await?.into_iter().map(|(k, v)| (k[from.len()..].to_string(), v)).collect())
}

/// Tables renamed (to a new name) or dropped (None): the shares that hand them out, changed to
/// follow them or to leave them out, for the statement's own commit, and those shares' names.
pub async fn tables_moved(lake: &Lake, moved: &[(&str, Option<&str>)]) -> Result<(Vec<(String, Vec<u8>)>, Vec<String>)> {
    let mut puts = vec![];
    let mut names = vec![];
    for (n, mut s) in shares(lake).await? {
        let before = s.tables.len();
        let mut changed = false;
        s.tables.retain_mut(|x| match moved.iter().find(|(t, _)| *t == x.table) {
            Some((_, Some(to))) => {
                x.table = to.to_string(); // (the name it is shared as stays)
                changed = true;
                true
            }
            Some((_, None)) => false,
            None => true,
        });
        if changed || s.tables.len() < before {
            puts.push((share_key(&n), json(&s)));
            names.push(n);
        }
    }
    Ok((puts, names))
}

/// Leader: carry out a change (under the lake's lock).
pub async fn apply(lake: &Lake, c: Change) -> Result<Value> {
    let must = |name: &str, s: Option<Share>| s.ok_or_else(|| anyhow!("no share {name} (CREATE SHARE {name})"));
    let put = |name: &str, s: &Share| (share_key(name), json(s));
    Ok(match c {
        Change::CreateShare { name, comment, if_not_exists } => {
            if share(lake, &name).await?.is_some() {
                ensure!(if_not_exists, "share {name} already exists");
                return Ok(j!({"share": name, "unchanged": true}));
            }
            let (note, gone) = noted("share", &name, comment);
            lake.cat.commit([vec![put(&name, &Share { id: id(), created_ms: crate::log::now_ms(), ..Default::default() })], note].concat(), &gone).await?;
            j!({"share": name})
        }
        Change::DropShare { name, if_exists } => {
            if share(lake, &name).await?.is_none() {
                ensure!(if_exists, "no share {name}");
                return Ok(j!({"share": name, "dropped": false}));
            }
            lake.cat.commit(vec![], &[share_key(&name)]).await?;
            j!({"share": name, "dropped": true})
        }
        Change::AddTable { share: name, table, as_name, partitions, history } => {
            let mut s = must(&name, share(lake, &name).await?)?;
            let (t, mut meta) = shareable(lake, &table).await?;
            let (schema, as_table) = match &as_name {
                Some(a) => a.split_once('.').map(|(s, t)| (s.to_string(), t.to_string())).filter(|(_, t)| !t.contains('.')).with_context(|| format!("AS {a}: the name a recipient sees is schema.table"))?,
                None => { let (s, n) = crate::ddl::split(&t); (s.to_string(), n.to_string()) }
            };
            crate::ddl::check(&schema)?;
            crate::ddl::check(&as_table)?;
            ensure!(!s.tables.iter().any(|x| x.schema == schema && x.name == as_table), "share {name} already has a table {schema}.{as_table} (ALTER SHARE {name} REMOVE TABLE {schema}.{as_table})");
            let partitions = partitions_of(&t, &meta, partitions)?;
            let mut puts = vec![];
            let publish = !meta.publish.iter().any(|f| f == "delta");
            if publish {
                meta.publish.push("delta".into()); // (the door hands out a version of its Delta log)
                puts.push((table_key(&t), json(&meta)));
            }
            s.tables.push(Shared { id: id(), table: t.clone(), schema: schema.clone(), name: as_table.clone(), partitions, history });
            puts.push(put(&name, &s));
            lake.cat.commit(puts, &[]).await?;
            if publish {
                crate::delta::publish_all(lake).await?; // (there for the recipient at once)
            }
            j!({"share": name, "table": format!("{schema}.{as_table}"), "of": t})
        }
        Change::RemoveTable { share: name, table } => {
            let mut s = must(&name, share(lake, &name).await?)?;
            // (by the name it is shared as; else, as Snowflake's REVOKE … FROM SHARE names it, every
            // entry of that table)
            let shown = if table.contains('.') { table.clone() } else { format!("public.{table}") };
            let named = |x: &Shared| format!("{}.{}", x.schema, x.name) == shown;
            let before = s.tables.len();
            match s.tables.iter().any(named) {
                true => s.tables.retain(|x| !named(x)),
                false => {
                    let local = crate::ddl::local(lake, &table);
                    s.tables.retain(|x| Some(&x.table) != local.as_ref());
                }
            }
            ensure!(s.tables.len() < before, "share {name} has no table {table}");
            lake.cat.commit(vec![put(&name, &s)], &[]).await?;
            j!({"share": name, "removed": table})
        }
        Change::CreateRecipient { name, comment, expires_secs, if_not_exists, endpoint } => {
            if lake.cat.get::<Recipient>(&recipient_key(&name)).await?.is_some() {
                ensure!(if_not_exists, "recipient {name} already exists (ALTER RECIPIENT {name} ROTATE TOKEN gives it a new profile)");
                return Ok(j!({"recipient": name, "unchanged": true}));
            }
            let now = crate::log::now_ms();
            let (token, hash) = token()?;
            let r = Recipient { id: id(), hash, created_ms: now, expires_ms: expires_secs.map(|s| now + s * 1000) };
            let (note, gone) = noted("recipient", &name, comment);
            lake.cat.commit([vec![(recipient_key(&name), json(&r))], note].concat(), &gone).await?;
            j!({"recipient": name, "profile": profile(&endpoint, &token, r.expires_ms)}) // (shown this once: only its hash is kept)
        }
        Change::RotateToken { name, expires_secs, endpoint } => {
            let mut r = lake.cat.get::<Recipient>(&recipient_key(&name)).await?.with_context(|| format!("no recipient {name}"))?;
            let now = crate::log::now_ms();
            let life = expires_secs.map(|s| s * 1000).or(r.expires_ms.map(|e| e.saturating_sub(r.created_ms))); // (as long as the last, unless said)
            let (token, hash) = token()?;
            (r.hash, r.created_ms, r.expires_ms) = (hash, now, life.map(|l| now + l));
            lake.cat.commit(vec![(recipient_key(&name), json(&r))], &[]).await?;
            j!({"recipient": name, "profile": profile(&endpoint, &token, r.expires_ms)})
        }
        Change::DropRecipient { name, if_exists } => {
            if lake.cat.get::<Recipient>(&recipient_key(&name)).await?.is_none() {
                ensure!(if_exists, "no recipient {name}");
                return Ok(j!({"recipient": name, "dropped": false}));
            }
            let mut puts = vec![];
            for (n, mut s) in shares(lake).await? {
                if s.recipients.contains(&name) {
                    s.recipients.retain(|r| *r != name);
                    puts.push(put(&n, &s));
                }
            }
            lake.cat.commit(puts, &[recipient_key(&name)]).await?;
            j!({"recipient": name, "dropped": true})
        }
        Change::Grant { share: name, recipients } => {
            let mut s = must(&name, share(lake, &name).await?)?;
            for r in &recipients {
                ensure!(lake.cat.get::<Recipient>(&recipient_key(r)).await?.is_some(), "no recipient {r} (CREATE RECIPIENT {r})");
                if !s.recipients.contains(r) {
                    s.recipients.push(r.clone());
                }
            }
            lake.cat.commit(vec![put(&name, &s)], &[]).await?;
            j!({"granted": name, "to": recipients})
        }
        Change::Revoke { share: name, recipients } => {
            let mut s = must(&name, share(lake, &name).await?)?;
            s.recipients.retain(|r| !recipients.contains(r));
            lake.cat.commit(vec![put(&name, &s)], &[]).await?;
            j!({"revoked": name, "from": recipients})
        }
    })
}

/// A table a share can hand out: one of this lake's, publishable as Delta whole files.
async fn shareable(lake: &Lake, table: &str) -> Result<(String, TableMeta)> {
    let t = crate::ddl::local(lake, table).with_context(|| format!("{table}: a table of this lake (an attached lake's tables are shared by that lake)"))?;
    ensure!(!crate::sys::hidden(&t), "{table} is Pondra's own");
    let Some(meta) = lake.cat.get::<TableMeta>(&table_key(&t)).await? else {
        bail!("no table {table} (a share hands out files: a view is shared as a materialized view of it, CREATE MATERIALIZED VIEW … AS …)");
    };
    ensure!(meta.ext.is_none() && meta.shares.is_empty(), "{table} is a clone, whose files are partly another table's: share the table it was cloned from, or a copy (CREATE TABLE … AS SELECT * FROM {table})");
    ensure!(meta.merge.is_empty() && meta.order.is_none(), "{table}'s rows combine as they are read (merge, order_by), which files can't carry: share a materialized view of it");
    Ok((t, meta))
}

/// `PARTITION (col = 'v'), …`: values of the table's own `partition_by` column, whose files each
/// hold one (invariant 26), so a partition is whole files.
fn partitions_of(table: &str, meta: &TableMeta, asked: Vec<(String, String)>) -> Result<Vec<String>> {
    if asked.is_empty() {
        return Ok(vec![]);
    }
    let l = meta.logical();
    let spec = l.partition.as_deref().with_context(|| format!("{table} has no partition_by: share it whole, or a materialized view of the rows"))?;
    ensure!(!spec.contains('('), "{table} is partitioned by {spec}: a share takes partitions of a table partitioned by a column; a materialized view takes any rows");
    ensure!(meta.key.is_empty(), "{table} is keyed, and its rows move between partitions: share it whole, or a materialized view of the rows");
    asked.into_iter().map(|(c, v)| { ensure!(c == spec, "{table} is partitioned by {spec}, not {c}"); Ok(v) }).collect()
}

/// A recipient's token (`pds_…`), and its hash, which is all that is kept.
fn token() -> Result<(String, String)> {
    let mut secret = [0u8; 32];
    aws_lc_rs::rand::fill(&mut secret).map_err(|_| anyhow!("no randomness"))?;
    let token = format!("pds_{}", B64U.encode(secret));
    let hash = crate::users::sha256(&token);
    Ok((token, hash))
}

/// Delta Sharing's profile: what a recipient's client reads (a `.share` file).
fn profile(endpoint: &str, token: &str, expires_ms: Option<u64>) -> Value {
    let mut p = j!({"shareCredentialsVersion": 1, "endpoint": endpoint, "bearerToken": token});
    if let Some(d) = expires_ms.and_then(|ms| chrono::DateTime::from_timestamp_millis(ms as i64)) {
        p["expirationTime"] = j!(d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    }
    p
}

/// The recipient a token is, if it is one and hasn't expired.
pub async fn recipient_of(lake: &Lake, token: &str) -> Result<Option<(String, Recipient)>> {
    let hash = crate::users::sha256(token);
    let now = crate::log::now_ms();
    Ok(recipients(lake).await?.into_iter().find(|(_, r)| r.hash == hash && r.expires_ms.is_none_or(|e| e > now)))
}

// ---------------------------------------------------------------- as tables

/// `pondra.shares` (a row a table shared; a share with none, one with no table) and
/// `pondra.recipients`: an admin's (a user's grants show none).
pub async fn tables(lake: &Lake) -> Result<Vec<(&'static str, Arc<dyn datafusion::catalog::TableProvider>)>> {
    use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMicrosecondArray};
    let mine = crate::auth::limited().is_none();
    let all = if mine { shares(lake).await? } else { vec![] };
    let (said, told) = if mine { (notes(lake, "share").await?, notes(lake, "recipient").await?) } else { Default::default() };
    let rows: Vec<(&String, &Share, Option<&Shared>)> = all.iter().flat_map(|(n, s)| {
        let t: Vec<Option<&Shared>> = if s.tables.is_empty() { vec![None] } else { s.tables.iter().map(Some).collect() };
        t.into_iter().map(move |t| (n, s, t))
    }).collect();
    let s = |f: &dyn Fn(&String, &Share, Option<&Shared>) -> Option<String>| Arc::new(rows.iter().map(|(n, s, t)| f(n, s, *t)).collect::<StringArray>()) as ArrayRef;
    let at = |ms: &dyn Fn(usize) -> Option<u64>, n: usize| Arc::new((0..n).map(|i| ms(i).map(|m| m as i64 * 1000)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef;
    let shares = RecordBatch::try_from_iter(vec![
        ("share", s(&|n, _, _| Some(n.clone()))),
        ("comment", s(&|n, _, _| said.get(n).cloned())),
        ("shared_as", s(&|_, _, t| t.map(|t| format!("{}.{}", t.schema, t.name)))),
        ("table", s(&|_, _, t| t.map(|t| t.table.clone()))),
        ("partitions", s(&|_, _, t| t.map(|t| t.partitions.join(", ")).filter(|p| !p.is_empty()))),
        ("history", Arc::new(rows.iter().map(|(_, _, t)| t.map(|t| t.history)).collect::<BooleanArray>()) as ArrayRef),
        ("recipients", s(&|_, s, _| Some(s.recipients.join(", ")).filter(|r| !r.is_empty()))),
        ("created", at(&|i| Some(rows[i].1.created_ms), rows.len())),
    ])?;
    let people = if mine { recipients(lake).await? } else { vec![] };
    let of = |name: &str| all.iter().filter(|(_, s)| s.recipients.iter().any(|r| r == name)).map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(", ");
    let r = |f: &dyn Fn(&String, &Recipient) -> Option<String>| Arc::new(people.iter().map(|(n, r)| f(n, r)).collect::<StringArray>()) as ArrayRef;
    let recipients = RecordBatch::try_from_iter(vec![
        ("name", r(&|n, _| Some(n.clone()))),
        ("comment", r(&|n, _| told.get(n).cloned())),
        ("shares", r(&|n, _| Some(of(n)).filter(|s| !s.is_empty()))),
        ("created", at(&|i| Some(people[i].1.created_ms), people.len())),
        ("expires", at(&|i| people[i].1.expires_ms, people.len())),
    ])?;
    let mem = |b: RecordBatch| -> Result<Arc<dyn datafusion::catalog::TableProvider>> { Ok(Arc::new(datafusion::datasource::MemTable::try_new(b.schema(), vec![vec![b]])?)) };
    Ok(vec![("shares", mem(shares)?), ("recipients", mem(recipients)?)])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(sql: &str) -> Change {
        match statement(sql) {
            Some(crate::write::Stmt::Ddl(mut d)) => match d.remove(0) {
                crate::ddl::Ddl::Shares(c) => c,
                _ => panic!("not a share's"),
            },
            Some(crate::write::Stmt::Invalid(e)) => panic!("{sql}: {e}"),
            _ => panic!("{sql}: not taken"),
        }
    }

    #[test]
    fn statements() {
        assert!(matches!(change("CREATE SHARE acme COMMENT 'Orders for Acme'"), Change::CreateShare { name, comment: Some(_), if_not_exists: false } if name == "acme"));
        match change("ALTER SHARE acme ADD TABLE sales.orders PARTITION (region = 'EU'), (region = 'UK') AS sales.orders_eu WITH HISTORY") {
            Change::AddTable { share, table, as_name, partitions, history } => {
                assert_eq!((share.as_str(), table.as_str(), as_name.as_deref(), history), ("acme", "sales.orders", Some("sales.orders_eu"), true));
                assert_eq!(partitions, vec![("region".to_string(), "EU".to_string()), ("region".to_string(), "UK".to_string())]);
            }
            _ => panic!(),
        }
        assert!(matches!(change("alter share acme remove table sales.orders_eu"), Change::RemoveTable { .. }));
        assert!(matches!(change("CREATE RECIPIENT acme_corp EXPIRES IN '90 days'"), Change::CreateRecipient { expires_secs: Some(7_776_000), .. }));
        assert!(matches!(change("GRANT SELECT ON SHARE acme TO RECIPIENT acme_corp, other"), Change::Grant { recipients, .. } if recipients.len() == 2));
        assert!(matches!(change("REVOKE SELECT ON SHARE acme FROM RECIPIENT acme_corp"), Change::Revoke { .. }));
        assert!(matches!(change("GRANT SELECT ON TABLE t TO SHARE acme"), Change::AddTable { table, .. } if table == "t"));
        assert!(matches!(change("ALTER RECIPIENT acme_corp ROTATE TOKEN"), Change::RotateToken { .. }));
        assert!(matches!(change("DROP RECIPIENT IF EXISTS acme_corp"), Change::DropRecipient { if_exists: true, .. }));
        // (users' grants stay users')
        assert!(statement("GRANT SELECT ON sales.orders TO bob").is_none());
        assert!(statement("GRANT SELECT ON share_log TO bob").is_none());
        assert!(matches!(statement("GRANT INSERT ON SHARE acme TO RECIPIENT r"), Some(crate::write::Stmt::Invalid(_))));
        // (OR REPLACE would drop a share's tables and grants, or end a recipient's token: refused, invariant 192)
        for sql in ["CREATE OR REPLACE SHARE acme", "create or replace recipient acme_corp"] {
            assert!(matches!(crate::write::parse(sql), Some(crate::write::Stmt::Invalid(e)) if e.contains("replacing one would drop")), "{sql}");
        }
    }
}
