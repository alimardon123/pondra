//! Materialized views whose answers need a last step (ADR-055): `avg`, `stddev` and `variance`,
//! `bool_and` and `bool_or`, expressions over aggregates (`sum(a) / count(*)`, `round(avg(x), 2)`),
//! a window function over the groups, `HAVING`, `ORDER BY` and `LIMIT`.
//!
//! The view's table keeps what adds up as rows arrive, one column each: counts, sums, mins, maxes
//! and moments (`pondra_moments`: a count, a mean and a sum of squared differences, combined as
//! Chan's formula does, so a variance stays exact where a sum of squares would cancel). The GROUP
//! BY view machinery already keeps such a table, from every node, in the same commit as the rows,
//! and takes back a changed row's part. Its answers are a SELECT over those columns
//! (`TableMeta::finish`), worked out as it is read (`query::table_view`): one projection more.
use anyhow::{bail, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, Int64Type};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};
use datafusion::sql::sqlparser::{ast, dialect::GenericDialect, parser::Parser};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

/// How a view's answers come from its table's merged rows: `sql` reads them as `__partial`, and
/// answers `columns` (what listings, `pg_catalog` and MCP show of the view: `TableMeta::described`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Finish {
    pub sql: String,
    #[serde(default)]
    pub columns: Vec<(String, String)>,
}

/// The table every finished view's answers are read from.
pub const PARTIAL: &str = "__partial";

/// The aggregates a view keeps as rows arrive, as the error names them.
const KEPT: &str = "count, sum, avg, min, max, stddev, variance, bool_and and bool_or";

/// A GROUP BY query, split in two: `partial`, a GROUP BY of counts, sums, mins, maxes and moments,
/// which the view's table keeps; and its finish, a SELECT over that table.
pub struct Split {
    pub partial: String,
    /// Its answers are ordered or cut (ORDER BY, LIMIT).
    pub ordered: bool,
    finish: ast::Query,
}

/// `sql` split, when it needs a last step: an aggregate kept as more than itself (avg, …), an
/// expression over aggregates, a GROUP BY expression it doesn't SELECT, HAVING, ORDER BY or LIMIT.
/// `names` are its columns as planned, `aggregates` the session's aggregate functions. `None`: the
/// query is kept as it is (or isn't one this can split, and `views::merges` says why).
pub fn split(sql: &str, names: &[String], aggregates: &HashSet<String>) -> Result<Option<Split>> {
    let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return Ok(None) };
    let (Some(ast::Statement::Query(q)), true) = (stmts.pop(), stmts.is_empty()) else { return Ok(None) };
    let ast::SetExpr::Select(s) = q.body.as_ref() else { return Ok(None) };
    let ast::GroupByExpr::Expressions(by, modifiers) = &s.group_by else { return Ok(None) };
    let items: Vec<ast::Expr> = s.projection.iter().map_while(|p| match p {
        ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => Some(e.clone()),
        _ => None,
    }).collect();
    if by.is_empty() || !modifiers.is_empty() || items.len() != s.projection.len() || items.len() != names.len() {
        return Ok(None);
    }
    // Each GROUP BY item: one the SELECT names (by alias, position or the same text), or a key of
    // its own, named as its column if it is one.
    let mut key = vec![false; items.len()];
    let mut hidden: Vec<(ast::Expr, String)> = vec![];
    for g in by {
        let at = match g {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n.parse::<usize>().ok().and_then(|n| n.checked_sub(1)),
                _ => None,
            },
            ast::Expr::Identifier(i) => names.iter().position(|n| *n == name(i)),
            _ => items.iter().position(|e| e.to_string() == g.to_string()),
        };
        match at.filter(|&i| i < items.len()) {
            Some(i) => key[i] = true,
            None => {
                let column = match g {
                    ast::Expr::Identifier(i) => name(i),
                    ast::Expr::CompoundIdentifier(p) => p.last().map(name).unwrap_or_default(),
                    _ => format!("__key_{}", hidden.len() + 1),
                };
                hidden.push((g.clone(), column));
            }
        }
    }
    let plain = |e: &ast::Expr| match e {
        ast::Expr::Function(f) => matches!(lower(&f.name).as_str(), "count" | "sum" | "min" | "max") && f.over.is_none() && f.within_group.is_empty() && !distinct(f),
        _ => false,
    };
    let needed = s.having.is_some() || s.qualify.is_some() || q.order_by.is_some() || q.limit_clause.is_some() || q.fetch.is_some() || !hidden.is_empty()
        || items.iter().zip(&key).any(|(e, k)| !k && !plain(e));
    if !needed {
        return Ok(None);
    }
    if s.distinct.is_some() {
        bail!("SELECT DISTINCT … GROUP BY: the GROUP BY gives each group once already");
    }
    // The finish: every aggregate a column (or columns) of the partial table, everything else as written.
    let mut parts = Parts::default();
    let mut finish_of = |e: &ast::Expr| -> Result<ast::Expr> {
        let mut e = e.clone();
        let mut failed = None;
        let _ = ast::visit_expressions_mut(&mut e, |x| {
            if let ast::Expr::Function(f) = x {
                if f.over.is_none() && aggregates.contains(&lower(&f.name)) {
                    match parts.of(f) {
                        Ok(r) => *x = r,
                        Err(err) => {
                            failed = Some(err);
                            return ControlFlow::Break(());
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        });
        // (what's left names keys, as the partial table does: without the table they came from)
        let _ = ast::visit_expressions_mut(&mut e, |x| {
            if let ast::Expr::CompoundIdentifier(p) = x {
                *x = ast::Expr::Identifier(p.last().expect("a part").clone());
            }
            ControlFlow::<()>::Continue(())
        });
        failed.map_or(Ok(e), Err)
    };
    let mut out = vec![];
    for (i, e) in items.iter().enumerate() {
        let shown = ast::Ident::with_quote('"', names[i].clone());
        let expr = if key[i] { ast::Expr::Identifier(shown.clone()) } else { finish_of(e)? };
        out.push(ast::SelectItem::ExprWithAlias { expr, alias: shown });
    }
    let having = s.having.as_ref().map(&mut finish_of).transpose()?;
    let qualify = s.qualify.as_ref().map(&mut finish_of).transpose()?;
    let mut order_by = q.order_by.clone();
    if let Some(ast::OrderBy { kind: ast::OrderByKind::Expressions(es), .. }) = &mut order_by {
        for o in es.iter_mut() {
            o.expr = finish_of(&o.expr)?;
        }
    }
    parts.count_rows();
    // The partial query: the keys first (grouped by position), then what the table adds up.
    let mut partial = (*q).clone();
    let ast::SetExpr::Select(ps) = partial.body.as_mut() else { unreachable!("a SELECT") };
    let mut projection: Vec<ast::SelectItem> = items.iter().zip(&key).zip(names).filter(|((_, k), _)| **k).map(|((e, _), n)| alias(e.clone(), n)).collect();
    projection.extend(hidden.iter().map(|(e, n)| alias(e.clone(), n)));
    let keys = projection.len();
    projection.extend(parts.columns.iter().map(|(call, n, _)| alias(call.clone(), n)));
    ps.projection = projection;
    ps.group_by = ast::GroupByExpr::Expressions((1..=keys).map(|i| number(&i.to_string())).collect(), vec![]);
    (ps.having, ps.qualify, ps.named_window, partial.order_by, partial.limit_clause, partial.fetch) = (None, None, vec![], None, None, None);
    // …and the finish over it.
    let mut finish = parse_query(&format!("SELECT 1 FROM {PARTIAL}"))?;
    let ast::SetExpr::Select(fs) = finish.body.as_mut() else { unreachable!("a SELECT") };
    (fs.projection, fs.selection, fs.qualify, fs.named_window) = (out, having, qualify, s.named_window.clone());
    (finish.order_by, finish.limit_clause, finish.fetch) = (order_by, q.limit_clause.clone(), q.fetch.clone());
    let ordered = q.order_by.is_some() || q.limit_clause.is_some() || q.fetch.is_some();
    Ok(Some(Split { partial: partial.to_string(), ordered, finish }))
}

impl Split {
    /// The finish's SQL, once the partial query's types are known: `decimal` says whether a column of
    /// it is a decimal, and `casts` the type each answer column is cast to, if any (Arrow's name).
    pub fn finish(&self, decimal: impl Fn(&str) -> bool, casts: &[Option<String>]) -> String {
        // (an average divides a sum by a count: as decimals for a decimal sum, else as doubles)
        let mut q = self.finish.clone();
        if let ast::SetExpr::Select(s) = q.body.as_mut() {
            for (item, to) in s.projection.iter_mut().zip(casts) {
                if let (ast::SelectItem::ExprWithAlias { expr: e, .. }, Some(t)) = (item, to) {
                    *e = expr(&format!("arrow_cast({e}, '{}')", t.replace('\'', "''"))).expect("a cast");
                }
            }
        }
        let _ = ast::visit_expressions_mut(&mut q, |x| {
            if let ast::Expr::Function(f) = x {
                if lower(&f.name) == AVG {
                    let a = args(f);
                    let (s, n) = (a[0].to_string(), a[1].to_string());
                    let sum = if decimal(s.trim_matches('"')) { s } else { format!("CAST({s} AS DOUBLE)") };
                    *x = expr(&format!("{sum} / NULLIF({n}, 0)")).expect("an average");
                }
            }
            ControlFlow::<()>::Continue(())
        });
        q.to_string()
    }
}

/// A finished view's table as it is read: its merged partial rows (`current`), finished.
pub async fn finished(aux: &datafusion::prelude::SessionContext, current: datafusion::prelude::DataFrame, f: &Finish) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    aux.register_table(PARTIAL, current.into_view())?;
    Ok(aux.sql(&f.sql).await?.into_view())
}

/// The finish's stand-in for `sum / count` until the sum's type is known (`Split::finish`).
const AVG: &str = "pondra_avg";

/// The partial table's columns: what each adds up (`count(x)`, …), its name and its merge kind.
#[derive(Default)]
struct Parts {
    columns: Vec<(ast::Expr, String, &'static str)>,
}

impl Parts {
    /// The column for an aggregate call (one per distinct call), named by its kind.
    fn column(&mut self, call: ast::Expr, kind: &'static str) -> String {
        let text = call.to_string();
        if let Some((_, n, _)) = self.columns.iter().find(|(c, _, _)| c.to_string() == text) {
            return n.clone();
        }
        let rows = text.eq_ignore_ascii_case("count(*)");
        let n = if rows { "__count".to_string() } else { format!("__{}_{}", kind.trim_start_matches("pondra_"), self.columns.len() + 1) };
        self.columns.push((call, n.clone(), kind));
        n
    }

    /// Every finished view counts its rows (`__count`): a group a change empties is gone (`query::live`).
    fn count_rows(&mut self) {
        self.column(expr("count(*)").expect("count(*)"), "count");
    }

    /// What stands for the aggregate `f` in the finish.
    fn of(&mut self, f: &ast::Function) -> Result<ast::Expr> {
        let fname = lower(&f.name);
        if distinct(f) || !f.within_group.is_empty() {
            bail!("{fname}(DISTINCT …) needs every row, not a running total: keep {KEPT} in a view, and work it out when you read (a stored view)");
        }
        let renamed = |to: &str| {
            let mut g = f.clone();
            g.name = ast::ObjectName::from(vec![ast::Ident::new(to)]);
            ast::Expr::Function(g)
        };
        let col = |n: String| ast::Expr::Identifier(ast::Ident::with_quote('"', n));
        Ok(match fname.as_str() {
            "count" | "sum" | "min" | "max" => col(self.column(ast::Expr::Function(f.clone()), kind(&fname))),
            "bool_and" => col(self.column(renamed("min"), "min")),
            "bool_or" => col(self.column(renamed("max"), "max")),
            "avg" | "mean" => {
                let (s, n) = (self.column(renamed("sum"), "sum"), self.column(renamed("count"), "count"));
                expr(&format!("{AVG}(\"{s}\", \"{n}\")"))?
            }
            "var" | "var_samp" | "var_sample" | "variance" | "var_pop" | "var_population" | "stddev" | "stddev_samp" | "std" | "stddev_pop" => {
                let m = self.column(renamed(MOMENTS), MOMENTS);
                let (n, m2) = (format!("get_field(\"{m}\", 'n')"), format!("get_field(\"{m}\", 'm2')"));
                let pop = fname.ends_with("pop") || fname.ends_with("population");
                let var = match pop {
                    true => format!("CASE WHEN {n} > 0 THEN greatest({m2} / {n}, 0) END"),
                    false => format!("CASE WHEN {n} > 1 THEN greatest({m2} / ({n} - 1), 0) END"),
                };
                expr(&if fname.starts_with("std") { format!("sqrt({var})") } else { var })?
            }
            other => bail!("{other}() needs every row, not a running total: a view keeps {KEPT} as rows arrive. Work {other}() out when you read (a stored view), or keep what it needs here"),
        })
    }
}

fn kind(f: &str) -> &'static str {
    match f {
        "count" => "count",
        "sum" => "sum",
        "min" => "min",
        _ => "max",
    }
}

/// A function's name as SQL means it.
fn lower(n: &ast::ObjectName) -> String { n.to_string().to_lowercase() }

fn distinct(f: &ast::Function) -> bool { matches!(&f.args, ast::FunctionArguments::List(l) if l.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)) }

fn args(f: &ast::Function) -> Vec<ast::Expr> {
    let ast::FunctionArguments::List(l) = &f.args else { return vec![] };
    l.args.iter().filter_map(|a| match a {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
        _ => None,
    }).collect()
}

/// An unquoted name as SQL means it (lower case); a quoted one as written.
fn name(i: &ast::Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } }

fn alias(e: ast::Expr, n: &str) -> ast::SelectItem { ast::SelectItem::ExprWithAlias { expr: e, alias: ast::Ident::with_quote('"', n) } }

fn number(n: &str) -> ast::Expr { ast::Expr::value(ast::Value::Number(n.into(), false)) }

fn expr(sql: &str) -> Result<ast::Expr> { Ok(Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_expr()?) }

fn parse_query(sql: &str) -> Result<ast::Query> {
    match Parser::parse_sql(&GenericDialect {}, sql)?.pop() {
        Some(ast::Statement::Query(q)) => Ok(*q),
        _ => bail!("a query"),
    }
}

/// `pondra_moments(x)`: a column's count, mean and sum of squared differences from it (`m2`),
/// for a variance. Over numbers it adds each in (Welford's way); over its own results it combines
/// them (Chan's), so it is both what a view's partial rows hold and how they merge. A part with a
/// negative count takes rows back. A NULL is passed over, as `var` does.
pub const MOMENTS: &str = "pondra_moments";

/// Adds `pondra_moments` to a session.
pub fn register(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udaf(AggregateUDF::from(Moments::default()));
    // (Postgres's and DuckDB's name for the sample variance, which DataFusion calls `var`)
    let var = datafusion::functions_aggregate::variance::var_samp_udaf();
    ctx.register_udaf(var.as_ref().clone().with_aliases(["variance"]));
}

fn moment_fields() -> Fields {
    Fields::from(vec![Field::new("n", DataType::Int64, false), Field::new("mean", DataType::Float64, false), Field::new("m2", DataType::Float64, false)])
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Moments(Signature);

impl Default for Moments {
    fn default() -> Self { Moments(Signature::any(1, Volatility::Immutable)) }
}

impl AggregateUDFImpl for Moments {
    fn name(&self) -> &str { MOMENTS }
    fn signature(&self) -> &Signature { &self.0 }
    fn return_type(&self, types: &[DataType]) -> datafusion::common::Result<DataType> {
        match &types[0] {
            t if t.is_numeric() || matches!(t, DataType::Struct(_) | DataType::Null) => Ok(DataType::Struct(moment_fields())),
            t => datafusion::common::plan_err!("{MOMENTS} takes numbers, not {t}"),
        }
    }
    fn accumulator(&self, _: AccumulatorArgs) -> datafusion::common::Result<Box<dyn Accumulator>> { Ok(Box::<Moment>::default()) }
    fn state_fields(&self, args: StateFieldsArgs) -> datafusion::common::Result<Vec<FieldRef>> {
        Ok(moment_fields().iter().map(|f| Arc::new(Field::new(format!("{}[{}]", args.name, f.name()), f.data_type().clone(), true))).collect())
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Moment {
    n: i64,
    mean: f64,
    m2: f64,
}

impl Moment {
    /// Chan's combination of two parts (either may have taken rows back: a negative count).
    fn add(&mut self, n: i64, mean: f64, m2: f64) {
        let total = self.n + n;
        if total == 0 {
            *self = Moment::default();
            return;
        }
        let delta = mean - self.mean;
        self.mean += delta * n as f64 / total as f64;
        self.m2 += m2 + delta * delta * self.n as f64 * n as f64 / total as f64;
        self.n = total;
    }

    fn parts(&mut self, n: &Int64Array, mean: &Float64Array, m2: &Float64Array, valid: Option<&datafusion::arrow::buffer::NullBuffer>) {
        for i in (0..n.len()).filter(|&i| valid.is_none_or(|v| v.is_valid(i)) && n.is_valid(i)) {
            self.add(n.value(i), mean.value(i), m2.value(i));
        }
    }
}

impl Accumulator for Moment {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let v = &values[0];
        if let DataType::Struct(_) = v.data_type() {
            let s = v.as_struct();
            let (n, mean, m2) = (s.column(0).as_primitive::<Int64Type>(), s.column(1).as_primitive::<Float64Type>(), s.column(2).as_primitive::<Float64Type>());
            self.parts(n, mean, m2, s.nulls());
            return Ok(());
        }
        let x = datafusion::arrow::compute::cast(v, &DataType::Float64)?;
        for x in x.as_primitive::<Float64Type>().iter().flatten() {
            self.add(1, x, 0.0);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.parts(states[0].as_primitive::<Int64Type>(), states[1].as_primitive::<Float64Type>(), states[2].as_primitive::<Float64Type>(), None);
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.n)), ScalarValue::Float64(Some(self.mean)), ScalarValue::Float64(Some(self.m2))])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![self.n])), Arc::new(Float64Array::from(vec![self.mean])), Arc::new(Float64Array::from(vec![self.m2]))];
        Ok(ScalarValue::Struct(Arc::new(StructArray::new(moment_fields(), columns, None))))
    }

    fn size(&self) -> usize { std::mem::size_of_val(self) }
}

/// Moments taken back (a change's old rows): the count and the squared differences negated.
pub fn negated(a: &ArrayRef) -> Result<ArrayRef> {
    let s = a.as_struct();
    let neg = |c: &ArrayRef| datafusion::arrow::compute::kernels::numeric::neg(c);
    let columns = vec![neg(s.column(0))?, s.column(1).clone(), neg(s.column(2))?];
    Ok(Arc::new(StructArray::new(moment_fields(), columns, s.nulls().cloned())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split_of(sql: &str, names: &[&str]) -> Option<(String, String)> {
        let aggregates: HashSet<String> = ["count", "sum", "min", "max", "avg", "stddev", "var_pop", "bool_and", "median"].map(String::from).into();
        let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        split(sql, &names, &aggregates).unwrap().map(|s| (s.partial.clone(), s.finish(|c| c == "__sum_1", &[])))
    }

    #[test]
    fn plain_views_are_kept_as_they_are() {
        assert!(split_of("SELECT k, count(*) AS n, sum(x) AS s FROM t GROUP BY k", &["k", "n", "s"]).is_none());
        assert!(split_of("SELECT date_bin(INTERVAL '1 minute', ts) AS m, min(x) FROM t GROUP BY 1", &["m", "min(t.x)"]).is_none());
    }

    #[test]
    fn averages_and_expressions_are_finished_when_read() {
        let (partial, finish) = split_of("SELECT t.k, avg(x) AS a, round(sum(x) / count(*), 2) AS r FROM t GROUP BY t.k HAVING count(*) > 1 ORDER BY a DESC LIMIT 3", &["k", "a", "r"]).unwrap();
        assert_eq!(partial, "SELECT t.k AS \"k\", sum(x) AS \"__sum_1\", count(x) AS \"__count_2\", count(*) AS \"__count\" FROM t GROUP BY 1");
        assert_eq!(finish, "SELECT \"k\" AS \"k\", \"__sum_1\" / NULLIF(\"__count_2\", 0) AS \"a\", round(\"__sum_1\" / \"__count\", 2) AS \"r\" FROM __partial WHERE \"__count\" > 1 ORDER BY a DESC LIMIT 3");
    }

    #[test]
    fn variances_keep_moments() {
        let (partial, finish) = split_of("SELECT k, stddev(x) FILTER (WHERE x > 0) AS s, var_pop(x) AS v, bool_and(ok) AS all_ok FROM t GROUP BY k", &["k", "s", "v", "all_ok"]).unwrap();
        assert!(partial.contains("pondra_moments(x) FILTER (WHERE x > 0) AS \"__moments_1\", pondra_moments(x) AS \"__moments_2\", min(ok) AS \"__min_3\""), "{partial}");
        assert!(finish.contains("sqrt(CASE WHEN get_field(\"__moments_1\", 'n') > 1"), "{finish}");
    }

    #[test]
    fn keys_not_selected_are_kept_and_others_refused() {
        let (partial, finish) = split_of("SELECT upper(region) AS r, count(*) AS n FROM t GROUP BY region", &["r", "n"]).unwrap();
        assert_eq!(partial, "SELECT region AS \"region\", count(*) AS \"__count\" FROM t GROUP BY 1");
        assert_eq!(finish, "SELECT upper(region) AS \"r\", \"__count\" AS \"n\" FROM __partial");
        let aggregates: HashSet<String> = ["count", "median"].map(String::from).into();
        let e = split("SELECT k, median(x) AS m FROM t GROUP BY k", &["k".into(), "m".into()], &aggregates).err().unwrap().to_string();
        assert!(e.contains("median() needs every row"), "{e}");
        assert!(split("SELECT k, count(DISTINCT x) AS m, avg(x) FROM t GROUP BY k", &["k".into(), "m".into(), "a".into()], &aggregates).is_err());
    }

    #[test]
    fn moments_combine_as_one_pass_would() {
        let xs = [1.0e9 + 1.0, 1.0e9 + 2.0, 1.0e9 + 4.0, 1.0e9 + 8.0];
        let (mut a, mut b) = (Moment::default(), Moment::default());
        xs[..2].iter().for_each(|x| a.add(1, *x, 0.0));
        xs[2..].iter().for_each(|x| b.add(1, *x, 0.0));
        a.add(b.n, b.mean, b.m2);
        assert_eq!(a.n, 4);
        assert!((a.m2 / 3.0 - 9.583333333333334).abs() < 1e-6, "{}", a.m2 / 3.0); // (a sum of squares loses every digit here)
        a.add(-b.n, b.mean, -b.m2); // (taken back)
        assert!((a.m2 - 0.5).abs() < 1e-6 && a.n == 2, "{} {}", a.n, a.m2);
    }
}
