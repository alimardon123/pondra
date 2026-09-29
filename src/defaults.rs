//! `NOT NULL` and `DEFAULT` (round 26). A column a write leaves out takes its default, worked out
//! for each row as the row is written (`now()`, `uuid()`): an INSERT that doesn't name it (or says
//! `DEFAULT`), a JSON row without the key, an Arrow batch or a `COPY` without the column. A NULL
//! in a `NOT NULL` column (a key's columns are) is refused, naming the column, whichever door the
//! write came in by: SQL, append, Kafka, Flight, `COPY`, `UPDATE`, `MERGE`.
use crate::store::TableMeta;
use anyhow::{bail, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch};
use datafusion::prelude::SessionContext;

/// Refuse rows that leave a NOT NULL column empty. `rows` are under SQL's names; `meta` is as
/// stored. A delete marker (`_deleted`) carries only its key, and passes.
pub fn check(meta: &TableMeta, table: &str, rows: &RecordBatch) -> Result<()> { check_named(meta, table, rows, true) }

fn check_named(meta: &TableMeta, table: &str, rows: &RecordBatch, sql_names: bool) -> Result<()> {
    if meta.not_null.is_empty() || rows.num_rows() == 0 {
        return Ok(());
    }
    let deleted = rows.column_by_name("_deleted").and_then(|d| d.as_boolean_opt().cloned());
    for stored in &meta.not_null {
        let name = meta.name_of(stored);
        let Some(c) = rows.column_by_name(if sql_names { name } else { stored }).filter(|c| c.null_count() > 0) else { continue };
        if (0..c.len()).any(|i| c.is_null(i) && !deleted.as_ref().is_some_and(|d| d.is_valid(i) && d.value(i))) {
            bail!("{table}.{name} is NOT NULL, and a row gives it no value");
        }
    }
    Ok(())
}

/// A bulk INSERT's rows (under their stored names), checked as they stream into files.
pub fn checked(rows: datafusion::execution::SendableRecordBatchStream, meta: &Option<TableMeta>, table: &str) -> datafusion::execution::SendableRecordBatchStream {
    use futures::StreamExt;
    let Some(meta) = meta.clone().filter(|m| !m.not_null.is_empty()) else { return rows };
    let (schema, table) = (rows.schema(), table.to_string());
    let checked = rows.map(move |b| {
        let b = b?;
        check_named(&meta, &table, &b, false).map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
        Ok(b)
    });
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, checked))
}

/// Does the table have defaults (under SQL's names)?
pub fn any(meta: &TableMeta) -> bool { !meta.defaults.is_empty() }

/// The value of `column`'s default for each of `n` new rows (as the column's type), or None if it
/// has none. Each row gets its own (`uuid()` differs row to row).
pub async fn values(meta: &TableMeta, column: &str, n: usize) -> Result<Option<ArrayRef>> {
    let Some(stored) = meta.stored(column) else { return Ok(None) };
    let (Some(expr), Some((_, _, t))) = (meta.defaults.get(stored), meta.live().find(|(s, _, _)| *s == stored)) else { return Ok(None) };
    let batches = SessionContext::new().sql(&format!("SELECT {expr} AS v FROM range({n})")).await?.collect().await?;
    let rows: Vec<ArrayRef> = batches.iter().map(|b| b.column(0).clone()).collect();
    let all = datafusion::arrow::compute::concat(&rows.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
    Ok(Some(crate::query::strict(&all, &crate::query::dtype(t)?)?))
}

/// A default's expression checked when the table is made: it gives one value of the column's type.
pub async fn validate(column: &str, sql_type: &str, expr: &str) -> Result<()> {
    let got = SessionContext::new().sql(&format!("SELECT {expr} AS v")).await;
    let batches = match got {
        Ok(df) => df.collect().await,
        Err(e) => Err(e),
    };
    match batches {
        Ok(b) if b.iter().map(|b| b.num_rows()).sum::<usize>() == 1 => {
            crate::query::strict(b[0].column(0), &crate::query::dtype(sql_type)?).map(|_| ()).map_err(|e| anyhow::anyhow!("{column} DEFAULT {expr}: {e}"))
        }
        Ok(_) => bail!("{column} DEFAULT {expr}: one value, please"),
        Err(e) => bail!("{column} DEFAULT {expr}: {e} (a default is an expression of constants and built-in functions, such as now() or uuid())"),
    }
}

/// `rows` with defaults put where the write left a column out: `left_out(column)` says in which
/// rows (None: in none).
pub async fn fill(meta: &TableMeta, rows: RecordBatch, left_out: impl Fn(&str) -> Option<BooleanArray>) -> Result<RecordBatch> {
    if !any(meta) || rows.num_rows() == 0 {
        return Ok(rows);
    }
    let mut columns = rows.columns().to_vec();
    for (i, f) in rows.schema().fields().iter().enumerate() {
        let Some(mask) = left_out(f.name()).filter(|m| m.true_count() > 0 && m.len() == rows.num_rows()) else { continue };
        let Some(v) = values(meta, f.name(), rows.num_rows()).await? else { continue };
        columns[i] = datafusion::arrow::compute::kernels::zip::zip(&mask, &v, &columns[i])?;
    }
    Ok(RecordBatch::try_new(rows.schema(), columns)?)
}

/// Every row left `column` out (a batch without it).
pub fn all(n: usize) -> BooleanArray { BooleanArray::from(vec![true; n]) }

/// JSON lines: for each default's column, the rows whose object hasn't the key.
pub fn absent_keys(meta: &TableMeta, lines: &[u8]) -> Result<std::collections::HashMap<String, BooleanArray>> {
    let names: Vec<String> = meta.defaults.keys().map(|s| meta.name_of(s).to_string()).collect();
    let mut out: std::collections::HashMap<String, Vec<bool>> = names.iter().map(|n| (n.clone(), vec![])).collect();
    for v in serde_json::Deserializer::from_slice(lines).into_iter::<serde_json::Value>() {
        let v = v?;
        for n in &names {
            out.get_mut(n).expect("a name").push(v.get(n).is_none());
        }
    }
    Ok(out.into_iter().map(|(k, v)| (k, BooleanArray::from(v))).collect())
}

/// A column's default as SQL, for an INSERT that leaves the column out (its rows are cast to
/// the table's types after).
pub fn sql_of(meta: &TableMeta, column: &str) -> Option<String> {
    Some(format!("({})", meta.defaults.get(meta.stored(column)?)?))
}
