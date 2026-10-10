//! A table's past (ADR-043): `FROM t AT (VERSION => n)`, `AT (TIMESTAMP => t)` and
//! `AT (OFFSET => -60)` (seconds before now) read an append table as it was then. Nothing is kept
//! for it but the rows: every row carries the commit that made it (`_version`) and when
//! (`_updated_at`), and a change keeps the versions it replaced in `{t}$deleted` (invariant 55),
//! for the table's retention (`tier::purge`).
//!
//! Where SQL comes in, `t AT (…)` becomes `pondra_at(t, version => …)` (`syntax`, so the parser
//! takes it), then a table of its own, `"at:<base64url JSON>"` (`table_factor`), which every
//! session reading it registers (`table`), as files anywhere are (`ext.rs`).

use crate::store::{table_key, Lake, TableMeta};
use anyhow::{anyhow, bail, ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit};
use datafusion::catalog::TableProvider;
use datafusion::common::{JoinType, ScalarValue};
use datafusion::prelude::*;
use datafusion::sql::sqlparser::{ast, dialect::GenericDialect, keywords::{Keyword, RESERVED_FOR_TABLE_ALIAS}, tokenizer::{Location, Token, Tokenizer}};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

/// What a query asked for: a table, and when.
#[derive(Serialize, Deserialize)]
struct Spec {
    table: String,
    at: String,   // version, timestamp or offset
    expr: String, // as written: worked out when the query runs (`now()` is then)
}

enum At {
    Version(i64),
    Time(i64), // microseconds since 1970, UTC
}

/// `t AT (VERSION => …)` → `pondra_at(t, version => …) AS t`, outside strings and comments: the
/// parser takes no `AT` after a table (only Snowflake's and Databricks' dialects do).
pub fn syntax(sql: &str) -> Cow<'_, str> {
    if let Cow::Owned(at) = as_of(sql) {
        return Cow::Owned(syntax(&at).into_owned());
    }
    static AT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\bAT\s*\(\s*(VERSION|TIMESTAMP|OFFSET)\s*=>").expect("a regex"));
    if !AT.is_match(sql) {
        return Cow::Borrowed(sql);
    }
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize_with_location() else { return Cow::Borrowed(sql) };
    let lines: Vec<usize> = std::iter::once(0).chain(sql.match_indices('\n').map(|(i, _)| i + 1)).collect();
    let byte = |l: &Location| -> usize {
        let start = lines.get((l.line.max(1) - 1) as usize).copied().unwrap_or(sql.len());
        sql[start..].char_indices().nth((l.column.max(1) - 1) as usize).map_or(sql.len(), |(i, _)| start + i)
    };
    let solid: Vec<&datafusion::sql::sqlparser::tokenizer::TokenWithSpan> = tokens.iter().filter(|t| !matches!(t.token, Token::Whitespace(_))).collect();
    let word = |k: usize| match solid.get(k).map(|t| &t.token) {
        Some(Token::Word(w)) => Some(w),
        _ => None,
    };
    let is = |k: usize, kw: Keyword| word(k).is_some_and(|w| w.quote_style.is_none() && w.keyword == kw);
    let (mut out, mut copied, mut i) = (String::new(), 0, 1);
    while i < solid.len() {
        let when = [Keyword::VERSION, Keyword::TIMESTAMP, Keyword::OFFSET].into_iter().find(|k| is(i + 2, *k));
        let shaped = is(i, Keyword::AT) && matches!(solid.get(i + 1).map(|t| &t.token), Some(Token::LParen)) && matches!(solid.get(i + 3).map(|t| &t.token), Some(Token::RArrow));
        let (Some(when), true, Some(_)) = (when, shaped, word(i - 1)) else {
            i += 1;
            continue;
        };
        let mut first = i - 1; // (the table's name: `t`, `s.t`, `lake.s.t`, quoted or not)
        while first >= 2 && matches!(solid[first - 1].token, Token::Period) && word(first - 2).is_some() {
            first -= 2;
        }
        let mut depth = 0;
        let Some(close) = (i + 1..solid.len()).find(|&k| {
            depth += match solid[k].token {
                Token::LParen => 1,
                Token::RParen => -1,
                _ => 0,
            };
            depth == 0
        }) else { break };
        let (start, end) = (byte(&solid[first].span.start), byte(&solid[close].span.end));
        let name = &sql[start..byte(&solid[i - 1].span.end)];
        let expr = &sql[byte(&solid[i + 3].span.end)..byte(&solid[close].span.start)];
        let aliased = word(close + 1).is_some_and(|w| w.keyword == Keyword::AS || w.quote_style.is_some() || !RESERVED_FOR_TABLE_ALIAS.contains(&w.keyword));
        out.push_str(&sql[copied..start]);
        out.push_str(&format!("pondra_at({name}, {} => {expr})", format!("{when:?}").to_lowercase()));
        if !aliased {
            out.push_str(&format!(" AS {}", word(i - 1).expect("a name")));
        }
        (copied, i) = (end, close + 1);
    }
    if copied == 0 {
        return Cow::Borrowed(sql);
    }
    out.push_str(&sql[copied..]);
    Cow::Owned(out)
}

/// Other engines' words for `AT (…)`: Delta's and Spark's `t VERSION AS OF n` and `t TIMESTAMP AS
/// OF '…'`, and SQL:2011's and BigQuery's `t FOR SYSTEM_TIME AS OF '…'`, after a table in a FROM or
/// a JOIN. What follows is a number, a string, `TIMESTAMP '…'`, a `$variable` or `(an expression)`.
fn as_of(sql: &str) -> Cow<'_, str> {
    static AS_OF: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\b(VERSION|TIMESTAMP|SYSTEM_TIME)\s+AS\s+OF\b").expect("a regex"));
    if !AS_OF.is_match(sql) {
        return Cow::Borrowed(sql);
    }
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize_with_location() else { return Cow::Borrowed(sql) };
    let lines: Vec<usize> = std::iter::once(0).chain(sql.match_indices('\n').map(|(i, _)| i + 1)).collect();
    let byte = |l: &Location| -> usize {
        let start = lines.get((l.line.max(1) - 1) as usize).copied().unwrap_or(sql.len());
        sql[start..].char_indices().nth((l.column.max(1) - 1) as usize).map_or(sql.len(), |(i, _)| start + i)
    };
    let solid: Vec<_> = tokens.iter().filter(|t| !matches!(t.token, Token::Whitespace(_))).collect();
    let is = |k: usize, kw: Keyword| matches!(solid.get(k).map(|t| &t.token), Some(Token::Word(w)) if w.quote_style.is_none() && w.keyword == kw);
    let named = |k: usize| matches!(solid.get(k).map(|t| &t.token), Some(Token::Word(_)));
    let (mut out, mut copied, mut next) = (String::new(), 0, 1);
    for i in 1..solid.len() {
        if i < next {
            continue;
        }
        let (when, at) = match () {
            _ if is(i, Keyword::VERSION) && is(i + 1, Keyword::AS) && is(i + 2, Keyword::OF) => ("VERSION", i + 3),
            _ if is(i, Keyword::TIMESTAMP) && is(i + 1, Keyword::AS) && is(i + 2, Keyword::OF) => ("TIMESTAMP", i + 3),
            _ if is(i, Keyword::FOR) && is(i + 1, Keyword::SYSTEM_TIME) && is(i + 2, Keyword::AS) && is(i + 3, Keyword::OF) => ("TIMESTAMP", i + 4),
            _ => continue,
        };
        let mut first = i - 1; // (the table's name, after FROM, JOIN or a comma)
        while first >= 2 && matches!(solid[first - 1].token, Token::Period) && named(first - 2) {
            first -= 2;
        }
        let after = first.checked_sub(1).is_some_and(|k| is(k, Keyword::FROM) || is(k, Keyword::JOIN) || matches!(solid[k].token, Token::Comma));
        if !named(i - 1) || !after {
            continue;
        }
        let end = match solid.get(at).map(|t| &t.token) {
            Some(Token::Number(..) | Token::SingleQuotedString(_) | Token::Placeholder(_)) => at,
            Some(Token::Word(_)) if matches!(solid.get(at + 1).map(|t| &t.token), Some(Token::SingleQuotedString(_))) => at + 1, // (TIMESTAMP '…')
            Some(Token::LParen) => {
                let mut depth = 0;
                match (at..solid.len()).find(|&k| {
                    depth += match solid[k].token {
                        Token::LParen => 1,
                        Token::RParen => -1,
                        _ => 0,
                    };
                    depth == 0
                }) {
                    Some(k) => k,
                    None => continue,
                }
            }
            _ => continue,
        };
        let (start, stop) = (byte(&solid[i].span.start), byte(&solid[end].span.end));
        let expr = &sql[byte(&solid[at].span.start)..stop];
        out.push_str(&sql[copied..start]);
        out.push_str(&format!("AT ({when} => {expr})"));
        (copied, next) = (stop, end + 1);
    }
    match copied {
        0 => Cow::Borrowed(sql),
        _ => Cow::Owned(out + &sql[copied..]),
    }
}

/// `pondra_at(t, version => …)` in a FROM: the table of `t` as it was, by its own name. Ok(false):
/// not one.
pub fn table_factor(lake: &Lake, t: &mut ast::TableFactor) -> Result<bool> {
    let ast::TableFactor::Table { name, args: Some(args), .. } = t else { return Ok(false) };
    if crate::write::object(name) != "pondra_at" {
        return Ok(false);
    }
    let shape = "t AT (VERSION => n), AT (TIMESTAMP => t) or AT (OFFSET => -seconds)";
    let table = match args.args.first() {
        Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Identifier(i)))) => crate::write::ident(i),
        Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::CompoundIdentifier(parts)))) => parts.iter().map(crate::write::ident).collect::<Vec<_>>().join("."),
        _ => bail!("{shape}: after a table's name"),
    };
    let table = crate::ddl::local(lake, &table).ok_or_else(|| anyhow!("{table} AT (…): only this lake's tables are read as they were (ask a node of that lake)"))?;
    let (at, expr) = match args.args.get(1) {
        Some(ast::FunctionArg::Named { name, arg: ast::FunctionArgExpr::Expr(e), .. }) => (name.value.to_lowercase(), e.to_string()),
        _ => bail!("{shape}"),
    };
    ensure!(args.args.len() == 2 && ["version", "timestamp", "offset"].contains(&at.as_str()), "{shape}");
    let spec = B64.encode(serde_json::to_vec(&Spec { table, at, expr })?);
    let ast::TableFactor::Table { name, args, .. } = t else { unreachable!() };
    (*name, *args) = (ast::ObjectName::from(vec![ast::Ident::with_quote('"', format!("at:{spec}"))]), None);
    Ok(true)
}

/// Does this (expanded) SQL read a table as it was? Such a query runs on its node alone.
pub fn mentioned(sql: &str) -> bool { sql.contains("\"at:") }

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
    use crate::sys::{with_sys, CREATED, ROW_ID, UPDATED, VERSION};
    let spec: Spec = serde_json::from_slice(&B64.decode(name.trim_start_matches("at:"))?)?;
    let t = &spec.table;
    let meta: TableMeta = lake.cat.get(&table_key(t)).await?.ok_or_else(|| anyhow!("no table {t}"))?;
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
        true => lake.cat.get::<TableMeta>(&table_key(&deleted)).await?,
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

/// `RESTORE TABLE t TO VERSION AS OF n` (or `TIMESTAMP AS OF …`, Delta's words): t's rows as they
/// were then, by one MERGE from `t AT (…)`. A row changed since gets its old values back (its
/// `_row_id` kept), a row made since goes, a row deleted since comes back as a new row. It is a
/// change like any, so it can be undone the same way. None: not a RESTORE.
pub async fn restore(lake: &Lake, sql: &str) -> Result<Option<String>> {
    static RESTORE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"(?is)^\s*RESTORE\s+(?:TABLE\s+)?([\w."-]+)\s+(?:TO\s+)?(VERSION|TIMESTAMP)\s+AS\s+OF\s+(.+?)\s*;?\s*$"#).expect("a regex")
    });
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
