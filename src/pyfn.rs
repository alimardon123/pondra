//! Python functions in queries (ADR-027): `CREATE FUNCTION … LANGUAGE python`.
//!
//! - **A scalar function** is a DataFusion function whose batches go to this node's Python
//!   workers (`python.rs`), with its body, one column per argument: the worker calls it per row
//!   (or once, `vectorized`) and answers one column. A spread query runs it on every node, each on
//!   its own rows. Each batch may take the function's `timeout` (60 s unless it says).
//! - **A table function** (`RETURNS TABLE (…)`) is called once, when its query reads it, with its
//!   arguments' values; it answers rows. A query reading one runs on one node
//!   (`routines::pinned`): every node would call it.
//!
//! Functions have no connection back to the lake: a query may run one over millions of rows on
//! every node. They may import anything and call out.
use crate::routines::{Kind, Routine};
use crate::store::Lake;
use anyhow::{Context, Result};
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{exec_datafusion_err, plan_err, ScalarValue};
use datafusion::datasource::{MemTable, TableType};
use datafusion::logical_expr::{async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl}, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

/// A SQL type as DataFusion reads it (`VARCHAR` → Utf8, `DOUBLE` → Float64, `INT[]` → a list),
/// strings as plain Utf8; `VARIANT` and `JSON` are JSON text. Worked out once per type.
pub async fn arrow_of(sql_type: &str) -> Result<DataType> {
    static SEEN: LazyLock<Mutex<HashMap<String, DataType>>> = LazyLock::new(Default::default);
    if let Some(t) = SEEN.lock().unwrap().get(sql_type) {
        return Ok(t.clone());
    }
    let t = match is_json(Some(sql_type)) {
        true => DataType::Utf8,
        false => {
            let df = SessionContext::new().sql(&format!("SELECT CAST(NULL AS {sql_type}) AS v")).await?;
            plain(df.schema().field(0).data_type())
        }
    };
    SEEN.lock().unwrap().insert(sql_type.to_string(), t.clone());
    Ok(t)
}

/// VARIANT (JSON) values go to Python as what they hold, and come back as JSON.
pub fn is_json(sql_type: Option<&str>) -> bool { sql_type.is_some_and(|t| ["VARIANT", "JSON"].contains(&t.trim().to_uppercase().as_str())) }

/// A parameter that takes any value as it comes, not cast: none given, `ANY` (Postgres's
/// `anyelement`), or VARIANT / JSON (a JSON document's text, or any other value).
pub fn loose(sql_type: Option<&str>) -> bool { sql_type.is_none_or(|t| is_json(Some(t)) || ["ANY", "ANYELEMENT"].contains(&t.trim().to_uppercase().as_str())) }

fn plain(t: &DataType) -> DataType {
    match t {
        DataType::List(f) | DataType::LargeList(f) => DataType::List(Arc::new(Field::new("item", plain(f.data_type()), true))),
        t => crate::write::stored(t),
    }
}

/// Register the lake's Python functions in a session (on every node: they are in the catalog).
pub async fn register(lake: &Lake, ctx: &SessionContext) -> Result<()> {
    let all = crate::routines::listed(lake).await?;
    for (name, r) in all.iter().filter(|(_, r)| r.python() && r.kind != Kind::Procedure) {
        let mut args = vec![];
        for p in &r.params {
            args.push(match &p.ty {
                Some(t) if !loose(Some(t)) => Some(arrow_of(t).await.with_context(|| format!("{name}: {t}"))?),
                _ => None, // (any value)
            });
        }
        let routine = Arc::new(r.clone());
        match r.kind {
            Kind::Table => {
                let mut fields = vec![];
                for (c, t) in crate::routines::columns_of(r.returns.as_deref().unwrap_or_default())? {
                    fields.push(Field::new(c, arrow_of(&t).await?, true));
                }
                ctx.register_udtf(name, Arc::new(TableFn { name: name.clone(), routine, args, schema: Arc::new(Schema::new(fields)) }));
            }
            _ => {
                let returns = arrow_of(r.returns.as_deref().context("RETURNS")?).await?;
                let signature = match args.iter().all(Option::is_some) {
                    true => Signature::exact(args.iter().flatten().cloned().collect(), Volatility::Volatile),
                    false => Signature::new(TypeSignature::Any(args.len()), Volatility::Volatile),
                };
                let signature = if args.is_empty() { Signature::nullary(Volatility::Volatile) } else { signature };
                ctx.register_udf(AsyncScalarUDF::new(Arc::new(Function { name: name.clone(), routine, returns, signature })).into_scalar_udf());
            }
        }
    }
    Ok(())
}

/// What a worker needs to run it: its body, parameters and options.
fn head(name: &str, r: &Routine, table: bool) -> serde_json::Value {
    let json: Vec<bool> = r.params.iter().map(|p| is_json(p.ty.as_deref())).collect();
    json!({"op": "apply", "name": name, "body": r.body, "entry": r.with.entry, "params": crate::routines::names(r), "json": json,
           "json_returns": is_json(r.returns.as_deref()), "vectorized": r.with.vectorized, "strict": r.with.strict, "table": table})
}

/// Rows through a worker: the arguments' batch out, the answer back (as `out`'s columns).
async fn ask(name: &str, r: &Routine, table: bool, args: RecordBatch, out: SchemaRef) -> Result<Vec<RecordBatch>> {
    anyhow::ensure!(crate::python::runs(), "{name} is a Python function, and this node runs no Python: start it with --python <python>");
    let limit = std::time::Duration::from_secs_f64(r.with.timeout.unwrap_or(60.0));
    let parts = vec![crate::query::ipc(&[args])?, crate::query::ipc(&[RecordBatch::new_empty(out)])?];
    let mut log = |n: String| eprintln!("function {name}: {n}");
    let (_, parts) = crate::python::ask(&r.with.packages, crate::python::Use::Function, head(name, r, table), parts, Some(limit), &mut log).await.with_context(|| name.to_string())?;
    crate::query::read_ipc(parts.first().context("no rows back")?)
}

/// The arguments as a batch, a column per parameter (its name `_1`, `_2`… where it has none).
fn batch(r: &Routine, arrays: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
    let fields: Vec<Field> = r.params.iter().zip(&arrays).map(|(p, a)| Field::new(&p.name, a.data_type().clone(), true)).collect();
    Ok(RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)))?)
}

#[derive(Debug)]
struct Function {
    name: String,
    routine: Arc<Routine>,
    returns: DataType,
    signature: Signature,
}

impl PartialEq for Function {
    fn eq(&self, o: &Self) -> bool { self.name == o.name && self.routine == o.routine }
}
impl Eq for Function {}
impl std::hash::Hash for Function {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { (&self.name, &self.routine.body).hash(h) }
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> { Ok(self.returns.clone()) }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> { datafusion::common::exec_err!("{} runs on a Python worker", self.name) }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for Function {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> {
        let rows = args.number_rows;
        let arrays = args.args.iter().map(|a| a.to_array(rows)).collect::<datafusion::common::Result<Vec<ArrayRef>>>()?;
        let out = Arc::new(Schema::new(vec![Field::new("result", self.returns.clone(), true)]));
        // An IMMUTABLE or STABLE function called per row gets each distinct argument once
        // (`geocode(city)` over a million rows of a thousand cities: a thousand calls), and its
        // answers go back to every row that had them.
        let once = self.routine.cacheable() && !self.routine.with.vectorized && rows > 1;
        let (arrays, back) = if once { distinct(arrays, rows)? } else { (arrays, None) };
        let asked = back.as_ref().map_or(rows, |(n, _)| *n);
        let go = async {
            let answers = ask(&self.name, &self.routine, false, batch(&self.routine, arrays, asked)?, out.clone()).await?;
            let column = datafusion::arrow::compute::concat(&answers.iter().map(|b| b.column(0).as_ref()).collect::<Vec<_>>())?;
            anyhow::ensure!(column.len() == asked, "{} values back for {asked} rows", column.len());
            anyhow::Ok(match &back {
                Some((_, rows)) => datafusion::arrow::compute::take(&column, rows, None)?,
                None => column,
            })
        };
        Ok(ColumnarValue::Array(go.await.map_err(|e| exec_datafusion_err!("{e:#}"))?))
    }
}

/// The distinct rows of `arrays`, and for each row of theirs, which of those it is.
fn distinct(arrays: Vec<ArrayRef>, rows: usize) -> datafusion::common::Result<(Vec<ArrayRef>, Option<(usize, datafusion::arrow::array::UInt32Array)>)> {
    use datafusion::arrow::row::{RowConverter, SortField};
    if arrays.is_empty() {
        return Ok((arrays, Some((1, vec![0u32; rows].into())))); // (no arguments: one call)
    }
    let converted = RowConverter::new(arrays.iter().map(|a| SortField::new(a.data_type().clone())).collect())?.convert_columns(&arrays)?;
    let (mut seen, mut firsts, mut which) = (HashMap::<&[u8], u32>::new(), vec![], Vec::with_capacity(rows));
    for i in 0..rows {
        let at = *seen.entry(converted.row(i).data()).or_insert_with(|| {
            firsts.push(i as u32);
            firsts.len() as u32 - 1
        });
        which.push(at);
    }
    if firsts.len() == rows {
        return Ok((arrays, None)); // (all different: nothing to save)
    }
    let firsts = datafusion::arrow::array::UInt32Array::from(firsts);
    let arrays = arrays.iter().map(|a| datafusion::arrow::compute::take(a, &firsts, None)).collect::<Result<Vec<_>, _>>()?;
    Ok((arrays, Some((firsts.len(), which.into()))))
}

/// A Python table function, as DataFusion calls it: its arguments' values, then its rows.
#[derive(Debug)]
struct TableFn {
    name: String,
    routine: Arc<Routine>,
    args: Vec<Option<DataType>>,
    schema: SchemaRef,
}

impl TableFunctionImpl for TableFn {
    fn call(&self, args: &[Expr]) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        use datafusion::optimizer::simplify_expressions::ExprSimplifier;
        let simplifier = ExprSimplifier::new(datafusion::logical_expr::simplify::SimplifyContext::builder().with_current_time().build());
        let mut values = vec![];
        for (i, a) in args.iter().enumerate() {
            let v = match simplifier.simplify(a.clone())? {
                Expr::Literal(v, _) => v,
                e => return plan_err!("{}: its arguments are values (worked out once), not {e}", self.name),
            };
            values.push(match self.args.get(i).cloned().flatten() {
                Some(t) if v.data_type() != t => v.cast_to(&t)?,
                _ => v,
            });
        }
        Ok(Arc::new(Called { name: self.name.clone(), routine: self.routine.clone(), values, schema: self.schema.clone() }))
    }
}

#[derive(Debug)]
struct Called {
    name: String,
    routine: Arc<Routine>,
    values: Vec<ScalarValue>,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl TableProvider for Called {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Temporary }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, _: &[Expr], _: Option<usize>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let go = async {
            let arrays = self.values.iter().map(|v| v.to_array()).collect::<datafusion::common::Result<Vec<_>>>()?;
            let rows = ask(&self.name, &self.routine, true, batch(&self.routine, arrays, 1)?, self.schema.clone()).await?;
            anyhow::Ok(rows)
        };
        let rows = go.await.map_err(|e| exec_datafusion_err!("{e:#}"))?;
        MemTable::try_new(self.schema.clone(), vec![rows])?.scan(state, projection, &[], None).await
    }
}
