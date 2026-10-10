//! Views that keep each group once, when it's over: `CREATE MATERIALIZED VIEW v AS SELECT …
//! GROUP BY <a time bucket>, … EMIT FINAL` (ADR-052). The query is the one you would run by
//! hand; its GROUP BY says when a group is over. Any expression that only grows with a timestamp
//! column of the source is a window (`date_trunc('minute', ts)`, `date_bin(…)`, `CAST(ts AS
//! DATE)`, `floor(date_part('epoch', ts) / 60)`), and a group is over once the watermark's bucket is past it.
//! `GROUP BY user, SESSION(ts, INTERVAL '30 minutes')` is a session view (`views::Sessions`).
//!
//! The view's partial rows are kept as any GROUP BY view's are, in `{v}$open`, which no one else
//! reads; the groups that are over go to `v`, once, with the watermark they were cut at as the
//! producer's seq (exactly once, as a window view's `_final` is). `WITH (lateness = '10
//! seconds')` waits for rows that much out of order; `WITH (idle = '1 minute')` moves time on
//! with the clock once no row has come for that long, so a quiet source's last group is kept too.
//! Rows later than that are counted (`pondra.flows`' `late_rows`) and stay in their table.
use crate::ddl::Ddl;
use crate::store::*;
use crate::views::{Once, Options, Sessions, View};
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::sql::sqlparser::{ast, dialect::GenericDialect, parser::Parser};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock, Mutex};

/// The table a view's partial rows are kept in, and its entry's name.
pub fn open(name: &str) -> String { format!("{name}$open") }

/// `CREATE MATERIALIZED VIEW … EMIT FINAL`: the statement without its last two words.
pub fn emit_final(sql: &str) -> Option<String> {
    static END: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^(\s*CREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b.*?)\s+EMIT\s+FINAL\s*;?\s*$").expect("a regex"));
    END.captures(sql).map(|c| c[1].to_string())
}

/// The view `d` makes keeps each group once (`emit = 'final'`); false if `d` makes none.
pub fn mark(d: &mut Ddl) -> bool {
    match d {
        Ddl::CreateMaterialized { options, .. } => options.insert("emit".into(), "final".into()).is_none(),
        Ddl::Replacing { then, .. } | Ddl::Unless { then, .. } => mark(then),
        _ => false,
    }
}

/// `EMIT FINAL`: a session view if the GROUP BY has `SESSION(ts, gap)`, else a view whose
/// window is its time bucket (`views::create` under `{name}$open`, with the table `name`).
pub async fn create(lake: &Lake, name: &str, sql: &str, o: Options) -> Result<()> {
    ensure!(o.emit.is_none() && o.join.is_none() && o.history.is_none() && o.sessions.is_none(), "EMIT FINAL finds its window in the GROUP BY: no window, session, join or history options with it");
    ensure!(o.expect.is_empty(), "EMIT FINAL keeps a group once it's over: put expectations on a view of the rows before it");
    ensure!(matches!(o.refresh, None | Some(crate::views::Refresh::Incremental)) && o.lag_secs.is_none(), "EMIT FINAL keeps each group once, when it's over, from the rows as they come: not by key, run whole or with a lag");
    let (select, mut stmt) = select(sql)?;
    if let Some((time, gap_secs, rest)) = session(&select, &mut stmt)? {
        let s = Sessions { time, gap_secs, lateness_secs: o.lateness_secs, keys: vec![], idle_secs: o.idle_secs };
        return Box::pin(crate::views::create(lake, name, &rest, Options { sessions: Some(s), ..Default::default() })).await;
    }
    if let Some(v) = lake.cat.get::<View>(&crate::views::view_key(&open(name))).await? {
        ensure!(v.once.is_some(), "view {name} already exists");
    } else {
        ensure!(lake.cat.get::<TableMeta>(&table_key(name)).await?.is_none() && lake.cat.get::<View>(&crate::views::view_key(name)).await?.is_none(), "table {name} already exists");
    }
    let (other, source) = crate::ddl::resolve(lake, &crate::query::first_table(sql)?).await?;
    ensure!(other.is_none(), "a view follows a table of this lake");
    let src: TableMeta = lake.cat.get::<TableMeta>(&table_key(&source)).await?.with_context(|| format!("no table {source}"))?.logical();
    let (column, time, bucket) = window(&select, &src)?;
    let once = Once { into: name.to_string(), column, time, bucket, lateness_secs: o.lateness_secs, idle_secs: o.idle_secs };
    Box::pin(crate::views::create(lake, &open(name), sql, Options { keep: Some(once), ..Default::default() })).await
}

/// The query's one SELECT, parsed.
fn select(sql: &str) -> Result<(ast::Select, Vec<ast::Statement>)> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let plain = "EMIT FINAL ends one SELECT … GROUP BY …";
    let [ast::Statement::Query(q)] = &stmts[..] else { bail!(plain) };
    let ast::SetExpr::Select(s) = q.body.as_ref() else { bail!(plain) };
    ensure!(matches!(&s.group_by, ast::GroupByExpr::Expressions(by, _) if !by.is_empty()), "EMIT FINAL keeps each group once it's over: GROUP BY a time bucket, such as date_trunc('minute', ts) AS minute");
    Ok((s.as_ref().clone(), stmts))
}

/// `SESSION(ts, INTERVAL '30 minutes')` (Spark's `session_window` too) in the GROUP BY: its
/// column, its gap, and the query without it (a session view groups by its keys).
fn session(s: &ast::Select, stmts: &mut [ast::Statement]) -> Result<Option<(String, u64, String)>> {
    let ast::GroupByExpr::Expressions(by, _) = &s.group_by else { return Ok(None) };
    let is = |e: &ast::Expr| matches!(e, ast::Expr::Function(f) if matches!(f.name.to_string().to_lowercase().as_str(), "session" | "session_window"));
    let Some(found) = by.iter().find(|e| is(e)) else { return Ok(None) };
    let args = arguments(found);
    let time = match &args[..] {
        [ast::Expr::Identifier(t), _] => name(t),
        [ast::Expr::CompoundIdentifier(p), _] => name(p.last().expect("a part")),
        _ => bail!("SESSION(ts, INTERVAL '30 minutes'): its time column and the gap that ends a session"),
    };
    let gap = &args[1];
    let gap_secs = interval(gap).context("SESSION(ts, INTERVAL '30 minutes'): the gap is an INTERVAL")?;
    let ast::Statement::Query(q) = &mut stmts[0] else { unreachable!() };
    let ast::SetExpr::Select(sel) = q.body.as_mut() else { unreachable!() };
    if let ast::GroupByExpr::Expressions(by, _) = &mut sel.group_by {
        by.retain(|e| !is(e));
    }
    Ok(Some((time, gap_secs, stmts[0].to_string())))
}

/// A function's arguments, as expressions.
fn arguments(e: &ast::Expr) -> Vec<ast::Expr> {
    let ast::Expr::Function(f) = e else { return vec![] };
    let ast::FunctionArguments::List(l) = &f.args else { return vec![] };
    l.args.iter().filter_map(|a| match a {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
        _ => None,
    }).collect()
}

/// The columns an expression names.
fn named(e: &ast::Expr) -> Vec<String> {
    let mut out = vec![];
    let _ = ast::visit_expressions(e, |x| {
        match x {
            ast::Expr::Identifier(i) => out.push(name(i)),
            ast::Expr::CompoundIdentifier(p) => out.extend(p.last().map(name)),
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    out
}

/// An unquoted name as SQL means it (lower case); a quoted one as written.
fn name(i: &ast::Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } }

/// `INTERVAL '30 minutes'`, in seconds.
fn interval(e: &ast::Expr) -> Result<u64> {
    let ast::Expr::Interval(i) = e else { bail!("an INTERVAL, such as INTERVAL '30 minutes'") };
    let amount = match i.value.as_ref() {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) => s.clone(),
            other => other.to_string(),
        },
        other => other.to_string(),
    };
    crate::layout::seconds(&match &i.leading_field {
        Some(f) => format!("{amount} {f}"),
        None => amount,
    })
}

/// How an expression moves with time: not at all, only up with one column, or otherwise.
enum Grows {
    Constant,
    With(String),
    No,
}

/// Does `e` only grow (never go down) as one column grows? Built from what does: the column, a
/// constant, `date_trunc`, `date_bin`, `time_bucket`, casts to a date, time or number, `floor`
/// and friends, the year or epoch of a time, adding or taking away a constant, and multiplying or
/// dividing by a positive one. `hour(ts)` comes back round every day, so it doesn't.
fn grows(e: &ast::Expr) -> Grows {
    use ast::{BinaryOperator as B, Expr as E};
    let constant = |e: &ast::Expr| named(e).is_empty();
    let positive = |e: &ast::Expr| match e {
        E::Value(v) => matches!(&v.value, ast::Value::Number(n, _) if n.parse::<f64>().is_ok_and(|n| n > 0.0)),
        _ => false,
    };
    // (a function of one growing argument, the rest constant)
    let one = |args: &[ast::Expr], at: usize| match args.get(at).map(grows) {
        Some(g @ Grows::With(_)) if args.iter().enumerate().all(|(i, a)| i == at || constant(a)) => g,
        _ => Grows::No,
    };
    match e {
        _ if constant(e) => Grows::Constant,
        E::Identifier(i) => Grows::With(name(i)),
        E::CompoundIdentifier(p) => Grows::With(name(p.last().expect("a part"))),
        E::Nested(e) | E::UnaryOp { op: ast::UnaryOperator::Plus, expr: e } | E::Floor { expr: e, .. } | E::Ceil { expr: e, .. } => grows(e),
        E::Cast { expr, data_type, .. } => {
            let t = data_type.to_string().to_uppercase();
            match ["DATE", "TIMESTAMP", "DATETIME", "BIGINT", "INT", "INTEGER", "DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"].iter().any(|k| t.starts_with(k)) {
                true => grows(expr),
                false => Grows::No,
            }
        }
        E::Extract { field, expr, .. } => match field.to_string().to_uppercase().as_str() {
            "YEAR" | "EPOCH" | "ISOYEAR" | "DECADE" | "CENTURY" | "MILLENNIUM" => grows(expr),
            _ => Grows::No,
        },
        E::Function(f) => {
            let args = arguments(e);
            match f.name.to_string().to_lowercase().as_str() {
                "date_trunc" | "datetrunc" | "date_bin" | "time_bucket" => one(&args, 1),
                "date_part" | "datepart" => match &args[..] {
                    [E::Value(v), _] if matches!(&v.value, ast::Value::SingleQuotedString(u) if matches!(u.to_lowercase().as_str(), "year" | "epoch" | "isoyear" | "decade" | "century" | "millennium")) => one(&args, 1),
                    _ => Grows::No,
                },
                "date" | "to_date" | "year" | "isoyear" | "epoch" | "epoch_ms" | "epoch_us" | "epoch_ns" | "unix_timestamp" | "to_unixtime" | "floor" | "ceil" | "ceiling" | "trunc" | "round"
                | "to_timestamp" | "to_timestamp_seconds" | "to_timestamp_millis" | "to_timestamp_micros" | "to_timestamp_nanos" => one(&args, 0),
                _ => Grows::No,
            }
        }
        E::BinaryOp { left, op, right } => match (op, grows(left), grows(right)) {
            (B::Plus, g @ Grows::With(_), Grows::Constant) | (B::Plus, Grows::Constant, g @ Grows::With(_)) | (B::Minus, g @ Grows::With(_), Grows::Constant) => g,
            (B::Multiply, g @ Grows::With(_), _) if positive(right) => g,
            (B::Multiply, _, g @ Grows::With(_)) if positive(left) => g,
            (B::Divide | B::DuckIntegerDivide | B::MyIntegerDivide, g @ Grows::With(_), _) if positive(right) => g,
            _ => Grows::No,
        },
        _ => Grows::No,
    }
}

/// The GROUP BY's time bucket: the view's column it is, the source's timestamp column it grows
/// with, and the expression over that column alone (written `"ts"`).
fn window(s: &ast::Select, src: &TableMeta) -> Result<(String, String, String)> {
    let ast::GroupByExpr::Expressions(by, _) = &s.group_by else { bail!("EMIT FINAL: GROUP BY a time bucket") };
    let timestamp = |c: &str| src.columns.iter().any(|(n, t)| n == c && (t.starts_with("Timestamp") || t.starts_with("Date")));
    // (each GROUP BY item as the SELECT names it: an alias, a position, or the same expression)
    let projected: Vec<(Option<String>, ast::Expr)> = s.projection.iter().filter_map(|p| match p {
        ast::SelectItem::ExprWithAlias { expr, alias } => Some((Some(name(alias)), expr.clone())),
        ast::SelectItem::UnnamedExpr(e @ ast::Expr::Identifier(i)) => Some((Some(name(i)), e.clone())),
        ast::SelectItem::UnnamedExpr(e) => Some((None, e.clone())),
        _ => None,
    }).collect();
    let (mut found, mut repeats) = (vec![], vec![]);
    for g in by {
        let item = match g {
            ast::Expr::Identifier(i) => projected.iter().find(|(n, _)| n.as_deref() == Some(&name(i))).cloned().or(Some((Some(name(i)), g.clone()))),
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n.parse::<usize>().ok().and_then(|n| projected.get(n.wrapping_sub(1))).cloned(),
                _ => None,
            },
            _ => Some(projected.iter().find(|(_, e)| e.to_string() == g.to_string()).cloned().unwrap_or((None, g.clone()))),
        };
        let Some((column, e)) = item else { continue };
        match grows(&e) {
            Grows::With(t) if timestamp(&t) => found.push((column, t, e)),
            Grows::No if named(&e).iter().any(|c| timestamp(c)) => repeats.push(e.to_string()),
            _ => {}
        }
    }
    let (column, time, mut e) = match found.len() {
        1 => found.remove(0),
        0 if !repeats.is_empty() => bail!("EMIT FINAL: {} comes back round, so its groups are never over. Group by a time that only grows: date_trunc('hour', ts), date_bin(INTERVAL '5 minutes', ts), CAST(ts AS DATE)", repeats[0]),
        0 => bail!("EMIT FINAL keeps each group once it's over: GROUP BY a time bucket of a timestamp column, such as date_trunc('minute', ts) AS minute"),
        _ => bail!("EMIT FINAL: one time bucket in the GROUP BY, not {} and {}", found[0].2, found[1].2),
    };
    let column = column.with_context(|| format!("EMIT FINAL: name the window in the SELECT, as {e} AS window_start"))?;
    // (over the time column alone, as `"ts"`: worked out for the watermark and each new row)
    let _ = ast::visit_expressions_mut(&mut e, |x| {
        if matches!(x, ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)) {
            *x = ast::Expr::Identifier(ast::Ident::with_quote('"', time.clone()));
        }
        ControlFlow::<()>::Continue(())
    });
    Ok((column, time, e.to_string()))
}

/// A view's bucket, planned once over its time column (per lake and expression).
type Planned = (Arc<dyn PhysicalExpr>, SchemaRef);

async fn planned(lake: &Lake, source: &str, o: &Once) -> Result<Planned> {
    static PLANNED: LazyLock<Mutex<HashMap<(String, String, String), Planned>>> = LazyLock::new(Default::default);
    let key = (lake.url.clone(), o.time.clone(), o.bucket.clone());
    if let Some(p) = PLANNED.lock().unwrap().get(&key) {
        return Ok(p.clone());
    }
    let src: TableMeta = lake.cat.get::<TableMeta>(&table_key(source)).await?.with_context(|| format!("no table {source}"))?.logical();
    let t = crate::query::schema(&src.columns)?.field_with_name(&o.time)?.data_type().clone();
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(&o.time, t, true)]));
    let df = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;
    let ctx = crate::query::session(lake, "", "").await?;
    let e = ctx.parse_sql_expr(&o.bucket, &df)?;
    let p = (ctx.create_physical_expr(e, &df)?, schema);
    PLANNED.lock().unwrap().insert(key, p.clone());
    Ok(p)
}

/// The buckets of `times`, any timestamp type.
fn buckets(p: &Planned, times: &ArrayRef) -> Result<ArrayRef> {
    let column = datafusion::arrow::compute::cast(times, p.1.field(0).data_type())?;
    let b = RecordBatch::try_new(p.1.clone(), vec![column])?;
    Ok(p.0.evaluate(&b)?.into_array(b.num_rows())?)
}

/// The buckets of these watermarks (µs).
fn marks(p: &Planned, us: &[i64]) -> Result<Vec<ScalarValue>> {
    let a: ArrayRef = Arc::new(datafusion::arrow::array::TimestampMicrosecondArray::from(us.to_vec()));
    let b = buckets(p, &a)?;
    (0..b.len()).map(|i| Ok(ScalarValue::try_from_array(&b, i)?)).collect()
}

/// Leader: the groups of `view` (`{into}$open`) the watermark is now past, appended to `into`
/// with the watermark as the producer's seq (`prev`: the last one), so each is kept once. A
/// round whose watermark moved within the same bucket commits nothing.
pub async fn emit(lake: &Lake, log: &crate::log::Log, view: &str, v: &View, o: &Once) -> Result<()> {
    if v.fill.is_some() && lake.cat.get::<u64>(&producer_key(&format!("fill:{view}"))).await?.is_none() {
        return Ok(()); // (a view made over rows has its groups only once its fill commits: kept before, they'd be empty, and never again)
    }
    let Some(wm) = crate::views::watermark(lake, &v.source, &o.time, o.lateness_secs, o.idle_secs).await? else { return Ok(()) };
    let producer = format!("emit:{view}");
    let done: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
    if wm <= done as i64 {
        return Ok(());
    }
    let p = planned(lake, &v.source, o).await?;
    let [lo, hi] = <[ScalarValue; 2]>::try_from(marks(&p, &[done as i64, wm])?).map_err(|_| anyhow::anyhow!("two marks"))?;
    if hi.is_null() || (done > 0 && lo == hi) {
        return Ok(());
    }
    let ctx = crate::query::session(lake, "", "").await?;
    let meta: TableMeta = lake.cat.get(&table_key(view)).await?.with_context(|| format!("no table {view}"))?;
    let open = crate::query::table_view(lake, &ctx, view, &meta, None).await?;
    ctx.register_table("__open", crate::query::named(&ctx, open, &meta, false)?)?;
    use datafusion::prelude::{ident, lit};
    let g = ident(&o.column);
    let mut rows = ctx.table("__open").await?.filter(g.clone().lt(lit(hi)))?;
    if done > 0 {
        rows = rows.filter(g.clone().gt_eq(lit(lo)))?;
    }
    let rows = rows.sort(vec![g.sort(true, true)])?.collect().await?;
    crate::views::append(lake, log, &o.into, crate::log::Src { producer, seq: wm as u64, prev: Some(done) }, rows).await
}

/// A flush's rows of `v`'s source that came for groups already kept (their bucket is before the
/// one of the watermark they were cut at): counted, as `pondra$expectations`' `$late` of the
/// view. Only a row older than that watermark can be one, so most flushes cost a min().
pub async fn late(lake: &Lake, view: &str, v: &View, o: &Once, rows: &[RecordBatch]) -> Result<Option<RecordBatch>> {
    use datafusion::arrow::array::{BooleanArray, Int64Array, StringArray};
    let done: u64 = lake.cat.get(&producer_key(&format!("emit:{view}"))).await?.unwrap_or(0);
    if done == 0 {
        return Ok(None);
    }
    let src: TableMeta = lake.cat.get::<TableMeta>(&table_key(&v.source)).await?.with_context(|| format!("no table {}", v.source))?;
    let stored = src.stored(&o.time).unwrap_or(&o.time).to_string();
    let us = datafusion::arrow::datatypes::DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None);
    let (mut n, mut p) = (0i64, None);
    for b in rows {
        let Some(c) = b.column_by_name(&o.time).or_else(|| b.column_by_name(&stored)) else { continue };
        let t = datafusion::arrow::compute::cast(c, &us)?;
        let t = t.as_any().downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>().context("µs")?;
        if datafusion::arrow::compute::min(t).is_none_or(|m| m >= done as i64) {
            continue; // (nothing older than the watermark: no group of it is kept yet)
        }
        if p.is_none() {
            let planned = planned(lake, &v.source, o).await?;
            let lo = marks(&planned, &[done as i64])?.remove(0).to_scalar()?;
            p = Some((planned, lo));
        }
        let (planned, lo) = p.as_ref().expect("planned");
        let kept: BooleanArray = datafusion::arrow::compute::kernels::cmp::lt(&buckets(planned, c)?, lo)?;
        n += kept.true_count() as i64;
    }
    if n == 0 {
        return Ok(None);
    }
    let text = |s: &str| Arc::new(StringArray::from(vec![s.to_string()])) as ArrayRef;
    Ok(Some(RecordBatch::try_from_iter(vec![
        ("view", text(&o.into)),
        ("id", text(v.fill.as_ref().map_or("", |f| f.id.as_str()))),
        ("expectation", text(LATE)),
        ("action", text("keep")),
        ("failed", Arc::new(Int64Array::from(vec![n])) as ArrayRef),
    ])?))
}

/// The count of a view's late rows, among its expectations' (`pondra.flows`).
pub const LATE: &str = "$late";

/// The view as it was made: its query, its bucket in the GROUP BY, then EMIT FINAL (and a session
/// view's `SESSION(ts, INTERVAL '…')` back in its GROUP BY).
pub fn written(v: &View) -> Option<(String, Vec<String>)> {
    let time = |s: u64| crate::objects::span(s);
    if let Some(o) = &v.once {
        let mut with = vec![];
        if o.lateness_secs > 0 {
            with.push(format!("lateness = '{}'", time(o.lateness_secs)));
        }
        if let Some(i) = o.idle_secs {
            with.push(format!("idle = '{}'", time(i)));
        }
        return Some((format!("{}\nEMIT FINAL", v.query()), with));
    }
    let s = v.sessions.as_ref()?;
    let mut stmts = Parser::parse_sql(&GenericDialect {}, &v.sql).ok()?;
    let ast::Statement::Query(q) = stmts.first_mut()? else { return None };
    let ast::SetExpr::Select(sel) = q.body.as_mut() else { return None };
    let ast::GroupByExpr::Expressions(by, _) = &mut sel.group_by else { return None };
    let session = format!("SESSION({}, INTERVAL '{}')", crate::objects::ident(&s.time), time(s.gap_secs));
    by.push(Parser::new(&GenericDialect {}).try_with_sql(&session).ok()?.parse_expr().ok()?);
    let mut with = vec![];
    if s.lateness_secs > 0 {
        with.push(format!("lateness = '{}'", time(s.lateness_secs)));
    }
    if let Some(i) = s.idle_secs {
        with.push(format!("idle = '{}'", time(i)));
    }
    Some((format!("{}\nEMIT FINAL", stmts[0]), with))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(sql: &str) -> Result<(String, String, String)> {
        let src = TableMeta { columns: vec![("ts".into(), "Timestamp(Microsecond, None)".into()), ("user".into(), "Utf8".into()), ("n".into(), "Int64".into())], ..Default::default() };
        window(&select(sql)?.0, &src)
    }

    #[test]
    fn windows_from_the_group_by() {
        let w = |sql: &str| bucket(sql).map(|(c, t, e)| format!("{c}|{t}|{e}")).map_err(|e| e.to_string());
        assert_eq!(w("SELECT date_trunc('minute', ts) AS m, user, count(*) AS n FROM c GROUP BY m, user").unwrap(), "m|ts|date_trunc('minute', \"ts\")");
        assert_eq!(w("SELECT date_bin(INTERVAL '5 minutes', c.ts) AS w, count(*) FROM c GROUP BY 1").unwrap(), "w|ts|date_bin(INTERVAL '5 minutes', \"ts\")");
        assert_eq!(w("SELECT floor(date_part('epoch', ts) / 60) AS m, count(*) FROM c GROUP BY 1").unwrap(), "m|ts|FLOOR(date_part('epoch', \"ts\") / 60)");
        assert_eq!(w("SELECT CAST(ts AS DATE) AS day, sum(n) FROM c GROUP BY day").unwrap(), "day|ts|CAST(\"ts\" AS DATE)");
        assert!(w("SELECT hour(ts) AS h, count(*) FROM c GROUP BY h").unwrap_err().contains("comes back round"));
        assert!(w("SELECT user, count(*) FROM c GROUP BY user").unwrap_err().contains("GROUP BY a time bucket"));
        assert!(w("SELECT date_trunc('hour', ts) AS h, date_trunc('day', ts) AS d, count(*) FROM c GROUP BY h, d").unwrap_err().contains("one time bucket"));
        assert!(w("SELECT ts - INTERVAL '1 hour' AS t, count(*) FROM c GROUP BY t").is_ok());
        assert!(w("SELECT 0 - floor(epoch(ts)) AS t, count(*) FROM c GROUP BY t").is_err());
    }

    #[test]
    fn emit_final_ends_the_statement() {
        assert_eq!(emit_final("CREATE MATERIALIZED VIEW v AS SELECT 1 GROUP BY 1 emit final;").as_deref(), Some("CREATE MATERIALIZED VIEW v AS SELECT 1 GROUP BY 1"));
        assert!(emit_final("SELECT 1 EMIT FINAL").is_none());
        let (_, mut stmts) = select("SELECT user, count(*) AS n FROM v GROUP BY user, SESSION(ts, INTERVAL '30 minutes')").unwrap();
        let (s, _) = select("SELECT user, count(*) AS n FROM v GROUP BY user, SESSION(ts, INTERVAL '30 minutes')").unwrap();
        let (t, gap, rest) = session(&s, &mut stmts).unwrap().unwrap();
        assert_eq!((t.as_str(), gap, rest.as_str()), ("ts", 1800, "SELECT user, count(*) AS n FROM v GROUP BY user"));
    }
}
