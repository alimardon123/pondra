//! A table's past (ADR-043): `FROM t AT (VERSION => n)`, `AT (TIMESTAMP => t)` and
//! `AT (OFFSET => -60)` (seconds before now) read an append table as it was then. Nothing is kept
//! for it but the rows: every row carries the commit that made it (`_version`) and when
//! (`_updated_at`), and a change keeps the versions it replaced in `{t}$deleted` (invariant 55),
//! for the table's retention (`tier::purge`).
//!
//! Where SQL comes in, `t AT (…)` becomes `pondra_at(t, version => …)` (`past_names::syntax`, so the parser
//! takes it), then a table of its own, `"at:<base64url JSON>"` (`past_names::table_factor`), which every
//! session reading it registers (`table`), as files anywhere are (`ext.rs`).

use anyhow::{anyhow, ensure, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use crate::past_names::Spec;
use crate::store::{table_key, Lake, TableMeta};
use anyhow::Context;
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit};
use datafusion::catalog::TableProvider;
use datafusion::common::{JoinType, ScalarValue};
use datafusion::prelude::*;
use std::sync::Arc;

enum At {
    Version(i64),
    Time(i64), // microseconds since 1970, UTC
}

/// Does this (expanded) SQL read a table as it was? Such a query runs on its node alone.
pub fn mentioned(sql: &str) -> bool { sql.contains("\"at:") }

/// The `t AT (… => …)` that each table as it was in this (expanded) SQL stands for, as written: its
/// moment is worked out as the query runs, so `server::query` looks there for one that moves.
pub fn spelled(sql: &str) -> String {
    let spec = |n: &String| serde_json::from_slice::<Spec>(&B64.decode(n.trim_start_matches("at:")).ok()?).ok();
    names(sql).iter().filter_map(spec).map(|s| format!("{} AT ({} => {})", s.table, s.at.to_uppercase(), s.expr)).collect::<Vec<_>>().join(" ")
}

/// The tables as they were that `sql` reads, by their names in it.
pub fn names(sql: &str) -> Vec<String> {
    static NAMES: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#""(at:[A-Za-z0-9_-]+)""#).expect("a regex"));
    let mut out: Vec<String> = NAMES.captures_iter(sql).map(|c| c[1].to_string()).collect();
    out.sort();
    out.dedup();
    out
}

/// The table `name` (`at:…`) stands for, read as of `upto` as any is: (the table's name, its rows
/// as they were). `sys`: with the system columns, which the query names.
///
/// As of commit `n`, an append table is its rows made by `n` (`_version <= n`), and the old
/// versions of rows changed since that were there at `n` (`{t}$deleted`: `_old_version <= n <
/// _version`). As of a time, the rows made by then, and, of a row changed since, the version its
/// first change after then replaced, if the row was made by then.
pub async fn table(lake: &Lake, ctx: &SessionContext, name: &str, upto: Option<u64>, sys: bool) -> Result<(String, Arc<dyn TableProvider>)> {
    use crate::sys::with_sys;
    use crate::text::{CREATED, ROW_ID, UPDATED, VERSION};
    let spec: Spec = serde_json::from_slice(&B64.decode(name.trim_start_matches("at:"))?)?;
    let t = &spec.table;
    let meta = lake.cat.meta(&table_key(t)).await?.ok_or_else(|| anyhow!("no table {t}"))?;
    ensure!(meta.key.is_empty(), "{t} AT (…): a keyed table keeps only each key's newest version, so only append tables are read as they were");
    ensure!(meta.history.is_none(), "{t} AT (…): a history view keeps every version already (__start_at, __end_at)");
    let at = when(lake, &spec).await?;
    if let Some((v, ms)) = kept_since(&meta) {
        let refused = match at {
            At::Version(n) => n < v as i64,
            At::Time(us) => us < ms as i64 * 1000,
        };
        let since = chrono::DateTime::from_timestamp_millis(ms as i64).map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)).unwrap_or_default();
        ensure!(!refused, "{t} AT (…): its past is kept from version {v} ({since}) on (its retention: ALTER TABLE {t} SET (retention = '7 days') keeps more from now on)");
    }
    if let Some((v, ms)) = meta.made {
        // (strictly before `v`: at `v` itself the table may already be there, as after CREATE TABLE)
        let before = match at {
            At::Version(n) => n < v as i64,
            At::Time(us) => us < ms as i64 * 1000,
        };
        let made = chrono::DateTime::from_timestamp_millis(ms as i64).map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)).unwrap_or_default();
        ensure!(!before, "{t} AT (…): {t} was made at version {v} ({made}); it has no past before that");
    }
    let ts = |us: i64| lit(ScalarValue::TimestampMicrosecond(Some(us), Some("UTC".into())));
    let user: Vec<Expr> = meta.columns.iter().map(|(c, _)| ident(c)).collect();
    let shown = |more: Vec<Expr>| user.iter().cloned().chain(more).collect::<Vec<_>>();
    let system = || if sys { [ROW_ID, VERSION, CREATED, UPDATED].map(col).to_vec() } else { vec![] };
    let rows = ctx.read_table(crate::query::table_view(lake, ctx, t, &with_sys(&meta), upto).await?)?;
    let made = match at {
        At::Version(n) => col(VERSION).lt_eq(lit(n)),
        At::Time(us) => col(UPDATED).lt_eq(ts(us)),
    };
    let mut out = rows.filter(made)?.select(shown(system()))?;
    let deleted = crate::sys::deleted(t);
    let dmeta = match meta.changed {
        true => lake.cat.meta(&table_key(&deleted)).await?,
        false => None,
    };
    if let Some(dmeta) = dmeta {
        let d = ctx.read_table(crate::query::table_view(lake, ctx, &deleted, &with_sys(&dmeta), upto).await?)?;
        let old = match at {
            At::Version(n) => d.clone().filter(col("_old_version").lt_eq(lit(n)).and(col(VERSION).gt(lit(n))))?,
            At::Time(us) => {
                let after = d.clone().filter(col(UPDATED).gt(ts(us)).and(col(CREATED).lt_eq(ts(us))))?;
                let first = after.clone().aggregate(vec![col(ROW_ID).alias("__r")], vec![datafusion::functions_aggregate::expr_fn::min(col(VERSION)).alias("__v")])?;
                after.join(first, JoinType::Inner, &[ROW_ID, VERSION], &["__r", "__v"], None)?
            }
        };
        let old = match sys {
            // (when its version was made: at the change before it, or when the row was)
            true => {
                let before = d.select(vec![col(ROW_ID).alias("__r2"), col(VERSION).alias("__v2"), col(UPDATED).alias("__u2")])?;
                let old = old.join(before, JoinType::Left, &[ROW_ID, "_old_version"], &["__r2", "__v2"], None)?;
                let made_at = datafusion::functions::expr_fn::coalesce(vec![col("__u2"), col(CREATED)]).alias(UPDATED);
                old.select(shown(vec![col(ROW_ID), col("_old_version").alias(VERSION), col(CREATED), made_at]))?
            }
            false => old.select(shown(vec![]))?,
        };
        out = out.union(old)?;
    }
    Ok((t.clone(), crate::query::named(ctx, out.into_view(), &meta, false)?))
}

/// The version (and time, in ms) before which `meta`'s past is no longer whole: a purge has let
/// old versions go (`TableMeta::past_from`). A table changed before this was kept (legacy) is whole
/// from its oldest purge kept on.
pub(crate) fn kept_since(meta: &TableMeta) -> Option<(u64, u64)> {
    match meta.past_from {
        Some(b) => Some(b).filter(|b| *b != (0, 0)),
        None => meta.purges.first().copied().filter(|_| meta.changed),
    }
}

/// When the query asked for: a version, or a time (TIMESTAMP, or OFFSET seconds from now).
async fn when(lake: &Lake, spec: &Spec) -> Result<At> {
    use datafusion::arrow::array::AsArray;
    let (sql, ty) = match spec.at.as_str() {
        "version" => (format!("SELECT CAST(({}) AS BIGINT)", spec.expr), DataType::Int64),
        "offset" => (format!("SELECT CAST(({}) AS DOUBLE)", spec.expr), DataType::Float64),
        _ => (format!("SELECT CAST(({}) AS TIMESTAMP)", spec.expr), DataType::Timestamp(TimeUnit::Microsecond, None)),
    };
    let what = || format!("{} AT ({} => {})", spec.table, spec.at.to_uppercase(), spec.expr);
    let b = lake.session().sql(&sql).await.with_context(what)?.collect().await.with_context(what)?;
    let v = b.first().filter(|b| b.num_rows() == 1).map(|b| datafusion::arrow::compute::cast(b.column(0), &ty)).transpose()?.filter(|c| c.is_valid(0));
    let v = v.ok_or_else(|| anyhow!("{}: one value, not null", what()))?;
    Ok(match spec.at.as_str() {
        "version" => At::Version(v.as_primitive::<Int64Type>().value(0)),
        "offset" => At::Time(crate::log::now_ms() as i64 * 1000 + (v.as_primitive::<datafusion::arrow::datatypes::Float64Type>().value(0) * 1e6) as i64),
        _ => At::Time(v.as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>().value(0)),
    })
}

static RESTORE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r#"(?is)^\s*RESTORE\s+(?:TABLE\s+)?([\w."-]+)\s+(?:TO\s+)?(VERSION|TIMESTAMP)\s+AS\s+OF\s+(.+?)\s*;?\s*$"#).expect("a regex")
});

/// A RESTORE's text before its moment, and the moment (`$day` in it is bound by `routines::bind`:
/// the parser doesn't read a RESTORE). None: not a RESTORE.
pub fn restore_moment(sql: &str) -> Option<(&str, &str)> {
    if crate::text::head(sql) != "restore" {
        return None;
    }
    let moment = RESTORE.captures(sql)?.get(3)?;
    Some((&sql[..moment.start()], moment.as_str()))
}

/// `RESTORE TABLE t TO VERSION AS OF n` (or `TIMESTAMP AS OF …`, Delta's words): t's rows as they
/// were then, by one MERGE from `t AT (…)`. A row changed since gets its old values back (its
/// `_row_id` kept), a row made since goes, a row deleted since comes back as a new row. It is a
/// change like any, so it can be undone the same way. None: not a RESTORE.
pub async fn restore(lake: &Lake, sql: &str) -> Result<Option<String>> {
    if crate::text::head(sql) != "restore" {
        return Ok(None);
    }
    let Some(c) = RESTORE.captures(sql) else { return Ok(None) };
    let name = c[1].split('.').map(|p| if p.starts_with('"') { p.trim_matches('"').to_string() } else { p.to_lowercase() }).collect::<Vec<_>>().join(".");
    let t = crate::ddl::local(lake, &name).ok_or_else(|| anyhow!("RESTORE {name}: a table of this lake (run it on a node of {name}'s)"))?;
    let meta: TableMeta = lake.cat.get(&table_key(&t)).await?.ok_or_else(|| anyhow!("no table {name}"))?;
    let cols: Vec<String> = meta.live().map(|(_, n, _)| format!("\"{}\"", n.replace('"', "\"\""))).collect();
    let all = |p: &str| cols.iter().map(|c| format!("{p}{c}")).collect::<Vec<_>>();
    let changed = cols.iter().map(|c| format!("(__now.{c} IS DISTINCT FROM __was.{c})")).collect::<Vec<_>>().join(" OR ");
    let set = cols.iter().map(|c| format!("{c} = __was.{c}")).collect::<Vec<_>>().join(", ");
    let target = &c[1];
    Ok(Some(format!(
        "MERGE INTO {target} AS __now USING (SELECT _row_id AS __row, {} FROM {target} AT ({} => {})) AS __was ON __now._row_id = __was.__row \
         WHEN MATCHED AND ({changed}) THEN UPDATE SET {set} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({}) WHEN NOT MATCHED BY SOURCE THEN DELETE",
        cols.join(", "), &c[2], &c[3], cols.join(", "), all("__was.").join(", ")
    )))
}
