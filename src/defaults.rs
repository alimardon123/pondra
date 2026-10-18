//! `NOT NULL` and `DEFAULT` (round 26). A column a write leaves out takes its default, worked out
//! for each row as the row is written (`now()`, `uuid()`): an INSERT that doesn't name it (or says
//! `DEFAULT`), a JSON row without the key, an Arrow batch or a `COPY` without the column. A NULL
//! in a `NOT NULL` column (a key's columns are) is refused, naming the column, whichever door the
//! write came in by: SQL, append, Kafka, Flight, `COPY`, `UPDATE`, `MERGE`.
use crate::ddl::Change;
use crate::store::{Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast;
use serde_json::{json, Value};

/// Refuse rows that leave a NOT NULL column empty. `rows` are under SQL's names; `meta` is as
/// stored. A delete marker (`_deleted`) carries only its key, and passes.
pub fn check(meta: &TableMeta, table: &str, rows: &RecordBatch) -> Result<()> { check_named(meta, table, rows, true) }

fn check_named(meta: &TableMeta, table: &str, rows: &RecordBatch, sql_names: bool) -> Result<()> {
    if (meta.not_null.is_empty() && meta.checks.is_empty() && meta.enums.is_empty() && meta.sql_types.is_empty()) || rows.num_rows() == 0 {
        return Ok(());
    }
    let deleted = rows.column_by_name("_deleted").and_then(|d| d.as_boolean_opt().cloned());
    let marker = |i: usize| deleted.as_ref().is_some_and(|d| d.is_valid(i) && d.value(i));
    crate::types::check(meta, rows, sql_names, &marker)?; // (an enum's labels, a uuid's form)
    if !meta.checks.is_empty() {
        // (under SQL's names, every column there: one a write leaves out is NULL, which passes)
        let named = match sql_names || !meta.mapped() {
            true => rows.clone(),
            false => {
                let fields: Vec<_> = rows.schema().fields().iter().map(|f| std::sync::Arc::new(f.as_ref().clone().with_name(meta.name_of(f.name())))).collect();
                RecordBatch::try_new(std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(fields)), rows.columns().to_vec())?
            }
        };
        let all = crate::query::conform(&named, &crate::answer::schema(&meta.logical_columns())?)?;
        for (name, c) in &meta.checks {
            let bad = breaking(&all, c)?;
            if (0..bad.len()).any(|i| bad.value(i) && !marker(i)) {
                return Err(anyhow::Error::new(crate::codes::Violation(format!("new row for relation \"{table}\" violates check constraint \"{name}\": CHECK ({c})"), "23514")));
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

/// The session CHECKs, DEFAULTs and expectations are planned in: every function a query's session has but the lake's
/// (`store::functions`, Postgres's JSON operators among them), so a condition that works in a query works here, and one
/// that doesn't is refused at CREATE, as Postgres refuses it. Built once: `evaluate` and `breaking` run it as it is;
/// `values` and `validate` take a copy (`copy`), so what they register is theirs alone.
static BASE: std::sync::LazyLock<SessionContext> =
    std::sync::LazyLock::new(|| crate::store::functions(datafusion::prelude::SessionConfig::new(), std::sync::Arc::new(Default::default())));

/// A session with BASE's functions, for what registers functions of its own (`nextval`): what holds the lake must never
/// go into BASE, which every check shares (invariants 199, 301).
fn copy() -> SessionContext { SessionContext::new_with_state(BASE.state()) }

/// `expr` over `rows` (their columns by name), a value for each row; None if it doesn't plan
/// over them alone (a qualified name, a subquery: SQL works it out instead).
pub fn evaluate(rows: &RecordBatch, expr: &str) -> Result<Option<ArrayRef>> {
    use datafusion::common::DFSchema;
    let schema = DFSchema::try_from(rows.schema().as_ref().clone())?;
    let Ok(e) = BASE.parse_sql_expr(expr, &schema).and_then(crate::optimize::whole_literals) else { return Ok(None) };
    let Ok(phys) = BASE.create_physical_expr(e, &schema) else { return Ok(None) };
    Ok(Some(phys.evaluate(rows)?.into_array(rows.num_rows())?))
}

/// Which of `rows` break `check`, a condition over their columns by name: those it is false for
/// (NULL passes, as with Postgres's CHECK). Expectations (`views::Expect`) and tables' CHECKs.
pub fn breaking(rows: &RecordBatch, check: &str) -> Result<BooleanArray> {
    use datafusion::common::DFSchema;
    let schema = DFSchema::try_from(rows.schema().as_ref().clone())?;
    let expr = BASE.parse_sql_expr(check, &schema).and_then(crate::optimize::whole_literals)?;
    let value = BASE.create_physical_expr(expr, &schema)?.evaluate(rows)?.into_array(rows.num_rows())?;
    let Some(b) = value.as_boolean_opt() else { bail!("CHECK ({check}) is not a condition: it gives {}, not true or false (a condition, such as CHECK (amount > 0))", value.data_type()) };
    Ok(b.iter().map(|v| Some(v == Some(false))).collect())
}

/// A table's DEFAULTs and CHECKs, checked as its CREATE checks them: a default gives one value of its column's type,
/// and a CHECK is a condition over the table's columns. A lake table's CREATE (`write::create_table`) and a temporary
/// table's (`temp::create`) make the same calls, so both refuse the same definitions.
pub async fn definition(columns: &[(String, String)], defaults: &std::collections::BTreeMap<String, String>, checks: &[(String, String)]) -> Result<()> {
    for (c, expr) in defaults {
        let t = &columns.iter().find(|(n, _)| n == c).with_context(|| format!("column \"{c}\" does not exist"))?.1;
        validate(c, t, expr).await?;
    }
    let empty = RecordBatch::new_empty(crate::answer::schema(columns)?);
    for (n, c) in checks {
        breaking(&empty, c).with_context(|| format!("CONSTRAINT {n} CHECK ({c})"))?;
    }
    Ok(())
}

/// A bulk INSERT's rows (under their stored names), checked as they stream into files.
pub fn checked(rows: datafusion::execution::SendableRecordBatchStream, meta: &Option<TableMeta>, table: &str) -> datafusion::execution::SendableRecordBatchStream {
    use futures::StreamExt;
    let Some(meta) = meta.clone().filter(|m| !m.not_null.is_empty() || !m.checks.is_empty() || !m.enums.is_empty() || !m.sql_types.is_empty()) else { return rows };
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
    let ctx = copy();
    if crate::seq::calls(expr) {
        crate::seq::register(&ctx, lake.arc()); // (an identity's `nextval('t_id_seq')`)
    }
    let batches = ctx.sql(&format!("SELECT {expr} AS v FROM range({n})")).await?.collect().await?;
    let rows: Vec<ArrayRef> = batches.iter().map(|b| b.column(0).clone()).collect();
    let all = datafusion::arrow::compute::concat(&rows.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
    Ok(Some(crate::query::strict(&all, &crate::answer::dtype(t)?)?))
}

/// A default's expression checked when the table is made: it gives one value of the column's type.
pub async fn validate(column: &str, sql_type: &str, expr: &str) -> Result<()> {
    let ctx = copy();
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
            crate::query::strict(b[0].column(0), &crate::answer::dtype(sql_type)?).map(|_| ()).map_err(|e| anyhow::anyhow!("{column} DEFAULT {expr}: {e}"))
        }
        Ok(_) => bail!("{column} DEFAULT {expr}: a default is one value, not a query's rows (DEFAULT 0, DEFAULT now(), DEFAULT nextval('s'))"),
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

/// Is this text the word DEFAULT (any case, spaces aside)? A quoted `"default"` is a column's name.
pub fn is_word(text: &str) -> bool { text.trim().eq_ignore_ascii_case("default") }

/// The SQL for a value written for `column`: the word DEFAULT is the column's default (NULL with none), as Postgres
/// reads it in an UPDATE's SET, a MERGE's INSERT and an upsert; anything else is the value as written.
pub fn value(meta: &TableMeta, column: &str, text: &str) -> String {
    match is_word(text) {
        true => sql_of(meta, column).unwrap_or_else(|| "NULL".into()),
        false => text.to_string(),
    }
}

/// Is a DEFAULT the NULL literal (`DEFAULT NULL`, `NULL::text`, `CAST(NULL AS text)`)? Postgres keeps
/// no default for it: a column added with one is NULL in the rows there, as one with none is.
pub fn is_null(e: &ast::Expr) -> bool {
    match e {
        ast::Expr::Value(v) => matches!(v.value, ast::Value::Null),
        ast::Expr::Nested(e) | ast::Expr::Cast { expr: e, .. } => is_null(e),
        _ => false,
    }
}

/// Does an ADD COLUMN owe the rows already there a value: NOT NULL, or a DEFAULT that isn't NULL?
pub fn fills_existing(c: &ast::ColumnDef) -> bool {
    c.options.iter().any(|o| match &o.option {
        ast::ColumnOption::NotNull => true,
        ast::ColumnOption::Default(e) => !is_null(e),
        _ => false,
    })
}

/// `ALTER TABLE t ALTER COLUMN c SET DEFAULT e | DROP DEFAULT | SET NOT NULL | DROP NOT NULL`
/// (leader, under the lake's lock), with Postgres's meanings: a default is worked out for the rows
/// written from now on, not for the rows there; SET NOT NULL is refused while a row there is NULL.
/// Neither changes a name or a type, so a view that reads the table doesn't stop them.
pub async fn alter(lake: &Lake, table: &str, column: &str, change: Change) -> Result<Value> {
    let key = crate::store::table_key(table);
    let mut m: TableMeta = lake.cat.get(&key).await?.with_context(|| format!("no table {table}"))?;
    let stored = m.stored(column).map(str::to_string).with_context(|| format!("column \"{column}\" of relation \"{table}\" does not exist"))?;
    ensure!(!m.identity.contains_key(&stored), "{column} is an identity column: its values come from its sequence, not a DEFAULT or NULL");
    match change {
        Change::Default(None) => {
            m.defaults.remove(&stored); // (none there is no error)
        }
        Change::Default(Some(expr)) => {
            let sql_type = m.columns.iter().find(|(c, _)| *c == stored).map(|(_, t)| t.clone()).unwrap_or_default();
            validate(column, &sql_type, &expr).await?;
            crate::seq::named_exist(lake, &expr).await?; // (DEFAULT nextval('s'): s is there)
            m.defaults.insert(stored.clone(), expr);
        }
        Change::NotNull(false) => {
            ensure!(!m.key.contains(&stored), "{column} is {table}'s key: a key is never NULL");
            m.not_null.retain(|c| *c != stored);
        }
        Change::NotNull(true) => return set_not_null(lake, table, column, &stored, m).await,
        _ => unreachable!("alter_column sends only defaults and NOT NULL here (ddl.rs)"),
    }
    lake.cat.commit(vec![(key, crate::store::json(&m))], &[]).await?;
    Ok(json!({"table": table, "column": column}))
}

/// SET NOT NULL, as `constraints.rs` adds an enforced UNIQUE: the column is NOT NULL from now on,
/// committed first, then one flush through the sequencer so the writes before it are in; then the
/// rows there are counted, and a NULL among them puts the entry back and refuses.
async fn set_not_null(lake: &Lake, table: &str, column: &str, stored: &str, mut m: TableMeta) -> Result<Value> {
    let (key, before) = (crate::store::table_key(table), m.clone());
    m.not_null.push(stored.to_string());
    lake.cat.commit(vec![(key.clone(), crate::store::json(&m))], &[]).await?;
    if let Some(to) = lake.to.get() {
        to.block().await?; // (a flush through the sequencer: those before it are in)
    }
    let nulls = match count_nulls(lake, table, column).await {
        Ok(n) => n,
        Err(e) => {
            lake.cat.commit(vec![(key, crate::store::json(&before))], &[]).await?;
            return Err(e);
        }
    };
    if nulls > 0 {
        lake.cat.commit(vec![(key, crate::store::json(&before))], &[]).await?;
        return Err(crate::codes::coded("23502", format!("column \"{column}\" of relation \"{table}\" contains null values ({nulls} rows): UPDATE {table} SET {column} = … WHERE {column} IS NULL first")));
    }
    Ok(json!({"table": table, "column": column, "not_null": true}))
}

/// The rows of `table` whose `column` is NULL, at the lake's visible commit.
async fn count_nulls(lake: &Lake, table: &str, column: &str) -> Result<i64> {
    let sql = format!("SELECT count(*) AS n FROM {} WHERE \"{}\" IS NULL", crate::write::sql_name(table), column.replace('"', "\"\""));
    let ctx = crate::query::session_at(lake, &sql, "", Some(lake.visible())).await?;
    let found = crate::engine::sql(&ctx, &sql).await?.collect().await?;
    Ok(found.first().and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>()).map_or(0, |a| a.value(0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    fn expr(sql: &str) -> ast::Expr { Parser::new(&GenericDialect {}).try_with_sql(sql).unwrap().parse_expr().unwrap() }

    fn column(def: &str) -> ast::ColumnDef {
        match &Parser::parse_sql(&GenericDialect {}, &format!("CREATE TABLE t ({def})")).unwrap()[0] {
            ast::Statement::CreateTable(c) => c.columns[0].clone(),
            _ => unreachable!("a CREATE TABLE"),
        }
    }

    #[test]
    fn null_defaults() {
        // (Postgres's NULL spellings: no default at all, so ADD COLUMN owes the rows nothing)
        for e in ["NULL", "NULL::text", "CAST(NULL AS varchar(200))", "(NULL)"] {
            assert!(is_null(&expr(e)), "{e}");
        }
        for e in ["'z'", "0", "NULL + 1", "now()"] {
            assert!(!is_null(&expr(e)), "{e}");
        }
        assert!(!fills_existing(&column("c varchar(200) DEFAULT NULL")));
        assert!(!fills_existing(&column("c INT DEFAULT CAST(NULL AS INT)")));
        assert!(fills_existing(&column("c INT DEFAULT 0")));
        assert!(fills_existing(&column("c INT NOT NULL")));
        assert!(fills_existing(&column("c INT NOT NULL DEFAULT NULL")));
    }

    #[test]
    fn the_word_default_is_the_column_default() {
        // (one test of the word: a SET, a VALUES item, an upsert and a MERGE all take it from `value`)
        let meta = TableMeta { columns: vec![("n".into(), "Int32".into()), ("t".into(), "Utf8".into())], defaults: [("n".to_string(), "3".to_string())].into(), ..Default::default() };
        for w in ["DEFAULT", "default", " DEFAULT "] {
            assert!(is_word(w), "{w}");
            assert_eq!(value(&meta, "n", w), "(3)", "{w}");
            assert_eq!(value(&meta, "t", w), "NULL", "{w}");
        }
        assert!(!is_word("\"default\""));
        assert_eq!(value(&meta, "n", "\"default\""), "\"default\"");
        assert_eq!(value(&meta, "n", "n + 1"), "n + 1");
    }

    #[test]
    fn the_shared_base_plans_postgres_json_operators() {
        // (CHECK, DEFAULT and expectations are planned in BASE: its `->>` comes from the JSON functions, as a session's does)
        let doc: ArrayRef = std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec!["{\"a\": 1}"]));
        let rows = RecordBatch::try_from_iter(vec![("doc", doc)]).unwrap();
        assert!(evaluate(&rows, "doc ->> 'a'").unwrap().is_some());
    }
}
