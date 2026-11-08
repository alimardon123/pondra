//! Rows dropped, kept for review (`pondra.rejects`): each row a view's expectation leaves out
//! (`ON VIOLATION DROP ROW`, and a `FAIL` one's rows when a view is filled from rows already there,
//! which can't be refused any more) is kept here, in the commit that writes the view's other rows.
//! A row says its object, the rule it broke, why (the rule's condition), the row as JSON, the run that
//! brought it (the job that wrote its source rows: a task's is `pondra.runs.job`) and when. "Drop"
//! still drops the row from the view: nothing new to choose.
//!
//! One table for the lake (`pondra$rejects`, hidden, keyed by a row's `id`). Each row is held for its
//! object's retention (`retention`, ADR-043, 7 days unless set): a read shows a row while that lasts,
//! and the table's TTL, which drops rows for good, is the longest any object asks for (`retain`). A
//! user with grants sees the rows of the objects it may read in whole, as a row's JSON holds every
//! column.
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub const TABLE: &str = "pondra$rejects";

fn columns() -> Vec<(String, String)> {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    let ts = crate::answer::type_name(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
    let text = |n: &str| (n.to_string(), "Utf8".to_string());
    vec![text("id"), text("object"), text("rule"), text("reason"), text("row"), text("run"), ("time".into(), ts)]
}

/// How long an object's rejects are held: its retention.
fn held(meta: Option<&TableMeta>) -> u64 { meta.and_then(|m| m.retention_secs).unwrap_or(crate::ddl::KEEP_MS / 1000) }

/// The table, for a commit that makes an object whose rows may be dropped: made if it isn't there, its
/// TTL raised to `secs` if it is shorter.
pub async fn made(lake: &Lake, secs: Option<u64>) -> Result<Option<(String, Vec<u8>)>> {
    let secs = held(None).max(secs.unwrap_or(0));
    Ok(match lake.cat.get::<TableMeta>(&table_key(TABLE)).await? {
        None => Some((table_key(TABLE), json(&{ let mut entry = TableMeta::with_columns(columns()); entry.key = vec!["id".into()]; entry.ttl = Some(("time".into(), secs)); entry.tiered = lake.visible(); entry }))),
        Some(mut m) if m.ttl.as_ref().is_some_and(|(_, s)| *s < secs) => {
            m.ttl = Some(("time".into(), secs));
            Some((table_key(TABLE), json(&m)))
        }
        Some(_) => None,
    })
}

/// An object's retention is now `secs`: the table holds its rows that long too.
pub async fn retain(lake: &Lake, secs: Option<u64>) -> Result<()> {
    if lake.cat.get::<TableMeta>(&table_key(TABLE)).await?.is_none() {
        return Ok(()); // (made with the first object that drops rows, long enough for every one there is then)
    }
    if let Some(put) = made(lake, secs).await? {
        lake.cat.commit(vec![put], &[]).await?;
    }
    Ok(())
}

/// The run a write's producer names: a SQL statement's job (a task's `task:{name}:{tick}`), or the
/// producer as the log has it (`copy:…`, `kafka:…`).
pub fn run(producer: &str) -> &str { producer.strip_prefix("sql:").or_else(|| producer.strip_prefix("job:")).unwrap_or(producer) }

/// The rows of `object` left out (`rows`), each with the rule that left it out and why, as rows of the table.
pub fn keep(object: &str, run: &str, why: &[(&str, &str)], rows: &RecordBatch) -> Result<RecordBatch> {
    let shown: Vec<usize> = (0..rows.num_columns()).filter(|&i| !crate::text::NAMES.contains(&rows.schema().field(i).name().as_str()) && rows.schema().field(i).name() != "_deleted").collect();
    let row = crate::answer::json_each(&rows.project(&shown)?)?;
    let n = rows.num_rows();
    let text = |f: &dyn Fn(usize) -> String| Arc::new((0..n).map(|i| Some(f(i))).collect::<StringArray>()) as ArrayRef;
    let at = crate::log::now_us() as i64;
    Ok(RecordBatch::try_from_iter(vec![
        ("id", text(&|_| crate::runs::new_id())),
        ("object", text(&|_| object.to_string())),
        ("rule", text(&|i| why[i].0.to_string())),
        ("reason", text(&|i| why[i].1.to_string())),
        ("row", text(&|i| String::from_utf8_lossy(&row[i]).into_owned())),
        ("run", text(&|_| run.to_string())),
        ("time", Arc::new(TimestampMicrosecondArray::from(vec![at; n]).with_timezone("UTC")) as ArrayRef),
    ])?)
}

/// `pondra.rejects`: the rows kept, each while its object's retention lasts, of the objects the caller
/// may read in whole.
pub async fn shown(lake: &Lake, ctx: &SessionContext, upto: Option<u64>, scope: &crate::rights::Scope) -> Result<Arc<dyn TableProvider>> {
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    let Some(meta) = lake.cat.meta(&table_key(TABLE)).await? else { return empty() };
    let rows = crate::query::named(ctx, crate::query::table_view(lake, ctx, TABLE, &meta, false, upto).await?, &meta, false)?;
    let now = crate::log::now_us() as i64;
    let (mut objects, mut since) = (vec![], vec![]);
    for (key, v) in lake.cat.scan::<crate::views::View>("v/", "v0").await? {
        let name = &key[2..];
        let object = v.shown(name);
        if !(scope.whole() || matches!(scope.columns(object), Some(None))) {
            continue;
        }
        let secs = held(lake.cat.meta(&table_key(name)).await?.as_deref());
        objects.push(object.to_string());
        since.push(now.saturating_sub(i64::try_from(secs).unwrap_or(i64::MAX).saturating_mul(1_000_000)));
    }
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![Field::new("object", DataType::Utf8, false), Field::new("since", ts, false)]));
    let kept = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(objects)), Arc::new(TimestampMicrosecondArray::from(since).with_timezone("UTC"))])?;
    let aux = SessionContext::new_with_state(ctx.state());
    aux.register_table("__rejects", rows)?;
    aux.register_table("__kept", Arc::new(datafusion::datasource::MemTable::try_new(schema, vec![vec![kept]])?))?;
    let sql = "SELECT r.id, r.object, r.rule, r.reason, r.row, r.run, r.time FROM __rejects r JOIN __kept k ON r.object = k.object WHERE r.time >= k.since";
    Ok(crate::engine::sql(&aux, sql).await?.into_view())
}

/// `pondra.rejects` before its first row: the columns, no rows.
pub fn empty() -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(datafusion::datasource::MemTable::try_new(crate::query::read_schema(&columns())?, vec![vec![]])?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dropped_row_is_kept_whole_with_its_rule_and_run() {
        use datafusion::arrow::array::Int64Array;
        let rows = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![2, 3])) as ArrayRef),
            ("amount", Arc::new(Int64Array::from(vec![Some(-3), None])) as ArrayRef),
            (crate::text::ROW_ID, Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef),
        ])
        .unwrap();
        let why = [("positive", "CHECK (amount > 0)"), ("has_amount", "CHECK (amount IS NOT NULL)")];
        let kept = keep("silver", run("sql:task:nightly:5"), &why, &rows).unwrap();
        let col = |n: &str| kept.column_by_name(n).unwrap().as_any().downcast_ref::<StringArray>().unwrap().iter().map(|v| v.unwrap().to_string()).collect::<Vec<_>>();
        assert_eq!(col("object"), ["silver", "silver"]);
        assert_eq!(col("rule"), ["positive", "has_amount"]);
        assert_eq!(col("reason"), ["CHECK (amount > 0)", "CHECK (amount IS NOT NULL)"]);
        assert_eq!(col("row"), [r#"{"id":2,"amount":-3}"#, r#"{"id":3}"#], "the row's own columns, none of the system's");
        assert_eq!(col("run"), ["task:nightly:5", "task:nightly:5"], "a task's run is its job, as pondra.runs has it");
        let ids = col("id");
        assert_ne!(ids[0], ids[1], "each kept row is its own");
        assert_eq!(run("copy:j1"), "copy:j1");
        assert_eq!(run("job:j2"), "j2");
    }
}
