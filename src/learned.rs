//! What the lake learns from its runs (ADR-050 §3): a filter on a table that kept far more or far
//! fewer of its rows than the planner expected. A query of `PONDRA_LEARN_MS` (100) or more that ran
//! here notes each such filter, off by 2× or more over 1,000 rows or more, in its history row
//! (`pondra.history`'s `learned`); `pondra.learned` is each one as last seen, and how many runs saw it.
//!
//! The facts are the history's own, so they cost no catalog entries and no requests of their own:
//! written by its writer, off the statement's path, in its quiet commits (invariant 224), and gone
//! with its rows after `PONDRA_HISTORY_DAYS`.
//!
//! A filter is known by its table and its conditions as one text (`about`): each `column op value`,
//! `IN`, `LIKE` and `IS [NOT] NULL`, sorted, joined by AND, the same whether read from the plan the
//! planner made (a logical `Expr`) or the one that ran (a `PhysicalExpr`), so the planner can ask
//! for what it is about to estimate in the words it was learned in.
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{ScalarValue, TableReference};
use datafusion::logical_expr::{utils::split_conjunction, Expr, LogicalPlan};
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, LikeExpr, Literal, TryCastExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// A filter that came out far from what the planner expected: of the rows it read, the share it was
/// expected to keep and the share it kept.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Fact {
    pub object: String,
    pub kind: String,
    pub about: String,
    pub expected: f64,
    pub actual: f64,
}

/// The filters on one table each that a plan which ran found 2× or more off what was expected.
/// A filter whose rows a join, a top-N, a min/max or a limit may have cut as they ran (dynamic
/// filters, an early stop) is passed over: its rows say nothing of the filter.
pub fn found(logical: &LogicalPlan, plan: &Arc<dyn ExecutionPlan>) -> Vec<Fact> {
    let mut tables: HashMap<String, BTreeSet<String>> = HashMap::new();
    let _ = logical.apply_with_subqueries(|n| {
        if let LogicalPlan::Filter(f) = n {
            if let (LogicalPlan::TableScan(s), Some(about)) = (f.input.as_ref(), about(&f.predicate)) {
                tables.entry(about).or_default().insert(name(&s.table_name));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if tables.is_empty() {
        return vec![];
    }
    let mut seen = HashMap::new();
    running(plan, false, &mut seen);
    let mut facts: Vec<Fact> = seen.into_iter().filter_map(|(about, [expected, actual, of])| {
        let [object] = &tables.get(&about)?.iter().collect::<Vec<_>>()[..] else { return None }; // (one table filtered so: the fact is that table's)
        let off = expected.max(actual) / expected.min(actual).max(1.0);
        (of >= 1000.0 && off >= 2.0).then(|| Fact { object: object.to_string(), kind: "filter".into(), about, expected: expected / of, actual: actual / of })
    }).collect();
    facts.sort_by(|a, b| (&a.object, &a.about).cmp(&(&b.object, &b.about)));
    facts
}

/// Each filter that ran and wasn't cut: its conditions, and its expected rows, its rows and the
/// rows it read, added up over the plan (a table's files and its log tail are filtered apart).
fn running(p: &Arc<dyn ExecutionPlan>, cut: bool, seen: &mut HashMap<String, [f64; 3]>) {
    if let (false, Some(f)) = (cut, p.downcast_ref::<datafusion::physical_plan::filter::FilterExec>()) {
        if let (Some(about), Some(actual), Some(expected), Some(of)) = (running_about(f.predicate()), p.metrics().and_then(|m| m.output_rows()), rows(p.as_ref()), rows(f.input().as_ref())) {
            let s = seen.entry(about).or_default();
            *s = [s[0] + expected, s[1] + actual as f64, s[2] + of];
        }
    }
    let minmax = || {
        let text = datafusion::physical_plan::displayable(p.as_ref()).one_line().to_string();
        text.contains("min(") || text.contains("max(")
    };
    let cuts = cut || p.fetch().is_some() || (p.name() == "AggregateExec" && minmax());
    for (i, c) in p.children().into_iter().enumerate() {
        running(c, cuts || (p.name() == "HashJoinExec" && i == 1), seen); // (a hash join's probe side gets its dynamic filter)
    }
}

/// The rows the planner expects of `p`.
fn rows(p: &dyn ExecutionPlan) -> Option<f64> {
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    let s = StatisticsContext::new().compute(p, &StatisticsArgs::new()).ok()?;
    s.num_rows.get_value().map(|&n| n as f64)
}

/// A table's name as the planner read it, its parts joined (`history` makes it the lake's own name).
fn name(t: &TableReference) -> String {
    [t.catalog(), t.schema(), Some(t.table())].into_iter().flatten().collect::<Vec<_>>().join(".")
}

/// A filter's conditions as one text (see the top), from the plan the planner made; `None` when
/// any of them is something else.
pub fn about(e: &Expr) -> Option<String> {
    fn column(e: &Expr) -> Option<&str> {
        match e {
            Expr::Column(c) => Some(&c.name),
            Expr::Cast(c) => column(&c.expr),
            Expr::TryCast(c) => column(&c.expr),
            _ => None,
        }
    }
    let value = |e: &Expr| match e {
        Expr::Literal(v, _) => Some(sql(v)),
        _ => None,
    };
    let each = split_conjunction(e).into_iter().map(|c| match c {
        Expr::BinaryExpr(b) => match (column(&b.left), value(&b.right), column(&b.right), value(&b.left)) {
            (Some(c), Some(v), ..) => Some(format!("{c} {} {v}", b.op)),
            (.., Some(c), Some(v)) => Some(format!("{c} {} {v}", b.op.swap()?)),
            _ => None,
        },
        Expr::InList(l) => Some(within(column(&l.expr)?, l.negated, l.list.iter().map(value).collect::<Option<_>>()?)),
        Expr::Like(l) if l.escape_char.is_none() => Some(like(column(&l.expr)?, l.negated, l.case_insensitive, value(&l.pattern)?)),
        Expr::IsNull(c) => Some(format!("{} IS NULL", column(c)?)),
        Expr::IsNotNull(c) => Some(format!("{} IS NOT NULL", column(c)?)),
        _ => None,
    });
    joined(each.collect::<Option<_>>()?)
}

/// The same text from the plan that ran.
fn running_about(e: &Arc<dyn PhysicalExpr>) -> Option<String> {
    fn column(e: &Arc<dyn PhysicalExpr>) -> Option<&str> {
        if let Some(c) = e.downcast_ref::<Column>() {
            return Some(c.name());
        }
        column(e.downcast_ref::<CastExpr>().map(|c| c.expr()).or_else(|| e.downcast_ref::<TryCastExpr>().map(|c| c.expr()))?)
    }
    let value = |e: &Arc<dyn PhysicalExpr>| e.downcast_ref::<Literal>().map(|l| sql(l.value()));
    let each = datafusion::physical_expr::split_conjunction(e).into_iter().map(|c| {
        if let Some(b) = c.downcast_ref::<BinaryExpr>() {
            return match (column(b.left()), value(b.right()), column(b.right()), value(b.left())) {
                (Some(c), Some(v), ..) => Some(format!("{c} {} {v}", b.op())),
                (.., Some(c), Some(v)) => Some(format!("{c} {} {v}", b.op().swap()?)),
                _ => None,
            };
        }
        if let Some(l) = c.downcast_ref::<InListExpr>() {
            return Some(within(column(l.expr())?, l.negated(), l.list().iter().map(value).collect::<Option<_>>()?));
        }
        if let Some(l) = c.downcast_ref::<LikeExpr>() {
            return Some(like(column(l.expr())?, l.negated(), l.case_insensitive(), value(l.pattern())?));
        }
        if let Some(n) = c.downcast_ref::<IsNullExpr>() {
            return Some(format!("{} IS NULL", column(n.arg())?));
        }
        c.downcast_ref::<IsNotNullExpr>().and_then(|n| Some(format!("{} IS NOT NULL", column(n.arg())?)))
    });
    joined(each.collect::<Option<_>>()?)
}

fn within(column: &str, negated: bool, values: Vec<String>) -> String {
    format!("{column} {}IN ({})", if negated { "NOT " } else { "" }, values.join(", "))
}

fn like(column: &str, negated: bool, case_insensitive: bool, pattern: String) -> String {
    format!("{column} {}{} {pattern}", if negated { "NOT " } else { "" }, if case_insensitive { "ILIKE" } else { "LIKE" })
}

fn joined(mut each: Vec<String>) -> Option<String> {
    each.sort();
    each.dedup();
    (!each.is_empty()).then(|| each.join(" AND "))
}

/// A value as SQL writes it: text quoted, numbers bare, anything else quoted as it prints.
fn sql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => format!("'{}'", s.replace('\'', "''")),
        v if v.is_null() => "NULL".into(),
        v if v.data_type().is_numeric() => v.to_string(),
        v => format!("'{v}'"),
    }
}

/// `pondra.learned`: each fact as last seen in the history the caller may read, how many runs saw
/// it and when the last did.
pub async fn table(ctx: &datafusion::prelude::SessionContext, history: Arc<dyn datafusion::catalog::TableProvider>) -> anyhow::Result<Arc<dyn datafusion::catalog::TableProvider>> {
    use datafusion::arrow::array::{Array, AsArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType};
    use datafusion::prelude::col;
    let rows = ctx.read_table(history)?.filter(col("learned").is_not_null())?.select_columns(&["at", "learned"])?.collect().await?;
    let mut last: BTreeMap<(String, String, String), (i64, f64, f64, i64)> = BTreeMap::new(); // (at, expected, actual, runs)
    for b in &rows {
        let (at, text) = (b.column(0).as_primitive::<TimestampMicrosecondType>(), datafusion::arrow::compute::cast(b.column(1), &DataType::Utf8)?);
        let text = text.as_string::<i32>();
        for i in (0..b.num_rows()).filter(|&i| text.is_valid(i)) {
            for f in serde_json::from_str::<Vec<Fact>>(text.value(i)).unwrap_or_default() {
                let e = last.entry((f.object, f.kind, f.about)).or_insert((i64::MIN, 0.0, 0.0, 0));
                e.3 += 1;
                if at.value(i) >= e.0 {
                    (e.0, e.1, e.2) = (at.value(i), f.expected, f.actual);
                }
            }
        }
    }
    let text = |f: fn(&(String, String, String)) -> &String| Arc::new(last.keys().map(|k| Some(f(k).as_str())).collect::<StringArray>()) as _;
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("object", DataType::Utf8, false), Field::new("kind", DataType::Utf8, false), Field::new("about", DataType::Utf8, false),
        Field::new("expected", DataType::Float64, false), Field::new("actual", DataType::Float64, false), Field::new("runs", DataType::Int64, false), Field::new("updated_at", ts, false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        text(|k| &k.0), text(|k| &k.1), text(|k| &k.2),
        Arc::new(last.values().map(|v| v.1).collect::<Float64Array>()), Arc::new(last.values().map(|v| v.2).collect::<Float64Array>()),
        Arc::new(last.values().map(|v| v.3).collect::<Int64Array>()), Arc::new(last.values().map(|v| Some(v.0)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")),
    ])?;
    Ok(Arc::new(datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    /// A filter reads the same from the planner's side, whichever way round and in whatever order its
    /// conditions are written; anything else has no text.
    #[test]
    fn a_filter_reads_the_same_either_way() {
        let a = about(&col("country").eq(lit("FR")).and(col("city").eq(lit("Paris"))));
        let b = about(&lit("Paris").eq(col("city")).and(col("country").eq(lit("FR"))));
        assert_eq!(a.as_deref(), Some("city = 'Paris' AND country = 'FR'"));
        assert_eq!(a, b);
        assert_eq!(about(&col("n").gt(lit(5)).and(col("s").in_list(vec![lit("a"), lit("b")], false))).as_deref(), Some("n > 5 AND s IN ('a', 'b')"));
        assert_eq!(about(&lit(5).lt(col("n"))).as_deref(), Some("n > 5"));
        assert_eq!(about(&(col("k") % lit(7)).eq(lit(0))), None);
    }
}
