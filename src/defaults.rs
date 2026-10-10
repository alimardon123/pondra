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
    if (meta.not_null.is_empty() && meta.checks.is_empty() && meta.enums.is_empty()) || rows.num_rows() == 0 {
        return Ok(());
    }
    let deleted = rows.column_by_name("_deleted").and_then(|d| d.as_boolean_opt().cloned());
    let marker = |i: usize| deleted.as_ref().is_some_and(|d| d.is_valid(i) && d.value(i));
    crate::types::check(meta, rows, sql_names, &marker)?; // (an enum's labels)
    if !meta.checks.is_empty() {
        // (under SQL's names, every column there: one a write leaves out is NULL, which passes)
        let named = match sql_names || !meta.mapped() {
            true => rows.clone(),
            false => {
                let fields: Vec<_> = rows.schema().fields().iter().map(|f| std::sync::Arc::new(f.as_ref().clone().with_name(meta.name_of(f.name())))).collect();
                RecordBatch::try_new(std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(fields)), rows.columns().to_vec())?
            }
        };
        let all = crate::query::conform(&named, &crate::query::schema(&meta.logical_columns())?)?;
        for (name, c) in &meta.checks {
            let bad = breaking(&all, c)?;
            if (0..bad.len()).any(|i| bad.value(i) && !marker(i)) {
                return Err(anyhow::Error::new(crate::views::Violation(format!("new row for relation \"{table}\" violates check constraint \"{name}\": CHECK ({c})"), "23514")));
            }
        }
    }
    for stored in &meta.not_null {
        let name = meta.name_of(stored);
        let Some(c) = rows.column_by_name(if sql_names { name } else { stored }).filter(|c| c.null_count() > 0) else { continue };
        if (0..c.len()).any(|i| c.is_null(i) && !marker(i)) {
            bail!("{table}.{name} is NOT NULL, and a row gives it no value");
        }
    }
    Ok(())
}

/// `expr` over `rows` (their columns by name), a value for each row; None if it doesn't plan
/// over them alone (a qualified name, a subquery: SQL works it out instead).
pub fn evaluate(rows: &RecordBatch, expr: &str) -> Result<Option<ArrayRef>> {
    use datafusion::common::DFSchema;
    static CTX: std::sync::LazyLock<SessionContext> = std::sync::LazyLock::new(SessionContext::new);
    let schema = DFSchema::try_from(rows.schema().as_ref().clone())?;
    let Ok(e) = CTX.parse_sql_expr(expr, &schema) else { return Ok(None) };
    let Ok(phys) = CTX.create_physical_expr(e, &schema) else { return Ok(None) };
    Ok(Some(phys.evaluate(rows)?.into_array(rows.num_rows())?))
}

/// Which of `rows` break `check`, a condition over their columns by name: those it is false for
/// (NULL passes, as with Postgres's CHECK). Expectations (`views::Expect`) and tables' CHECKs.
pub fn breaking(rows: &RecordBatch, check: &str) -> Result<BooleanArray> {
    use datafusion::common::DFSchema;
    static CTX: std::sync::LazyLock<SessionContext> = std::sync::LazyLock::new(SessionContext::new);
    let schema = DFSchema::try_from(rows.schema().as_ref().clone())?;
    let expr = CTX.parse_sql_expr(check, &schema)?;
    let value = CTX.create_physical_expr(expr, &schema)?.evaluate(rows)?.into_array(rows.num_rows())?;
    let Some(b) = value.as_boolean_opt() else { bail!("CHECK ({check}) is not a condition: it gives {}, not true or false", value.data_type()) };
    Ok(b.iter().map(|v| Some(v == Some(false))).collect())
}

/// A bulk INSERT's rows (under their stored names), checked as they stream into files.
pub fn checked(rows: datafusion::execution::SendableRecordBatchStream, meta: &Option<TableMeta>, table: &str) -> datafusion::execution::SendableRecordBatchStream {
    use futures::StreamExt;
    let Some(meta) = meta.clone().filter(|m| !m.not_null.is_empty() || !m.checks.is_empty() || !m.enums.is_empty()) else { return rows };
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
pub async fn values(lake: &crate::store::Lake, meta: &TableMeta, column: &str, n: usize) -> Result<Option<ArrayRef>> {
    let Some(stored) = meta.stored(column) else { return Ok(None) };
    let (Some(expr), Some((_, _, t))) = (meta.defaults.get(stored), meta.live().find(|(s, _, _)| *s == stored)) else { return Ok(None) };
    let ctx = SessionContext::new();
    if crate::seq::calls(expr) {
        crate::seq::register(&ctx, lake.arc()); // (an identity's `nextval('t_id_seq')`)
    }
    let batches = ctx.sql(&format!("SELECT {expr} AS v FROM range({n})")).await?.collect().await?;
    let rows: Vec<ArrayRef> = batches.iter().map(|b| b.column(0).clone()).collect();
    let all = datafusion::arrow::compute::concat(&rows.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
    Ok(Some(crate::query::strict(&all, &crate::query::dtype(t)?)?))
}

/// A default's expression checked when the table is made: it gives one value of the column's type.
pub async fn validate(column: &str, sql_type: &str, expr: &str) -> Result<()> {
    let ctx = SessionContext::new();
    let one = |_: &[datafusion::logical_expr::ColumnarValue]| Ok(datafusion::logical_expr::ColumnarValue::Scalar(datafusion::common::ScalarValue::Int64(Some(1))));
    let int = datafusion::arrow::datatypes::DataType::Int64;
    ctx.register_udf(datafusion::prelude::create_udf("nextval", vec![datafusion::arrow::datatypes::DataType::Utf8], int, datafusion::logical_expr::Volatility::Volatile, std::sync::Arc::new(one))); // (its type: the sequence itself is looked for by `seq::named_exist`)
    let got = ctx.sql(&format!("SELECT {expr} AS v")).await;
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
pub async fn fill(lake: &crate::store::Lake, meta: &TableMeta, rows: RecordBatch, left_out: impl Fn(&str) -> Option<BooleanArray>) -> Result<RecordBatch> {
    if !any(meta) || rows.num_rows() == 0 {
        return Ok(rows);
    }
    let mut columns = rows.columns().to_vec();
    for (i, f) in rows.schema().fields().iter().enumerate() {
        let Some(mask) = left_out(f.name()).filter(|m| m.true_count() > 0 && m.len() == rows.num_rows()) else { continue };
        let Some(v) = values(lake, meta, f.name(), rows.num_rows()).await? else { continue };
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
