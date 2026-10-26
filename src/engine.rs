//! The session every query starts from, in every build: DataFusion's, with Pondra's rules, planners and the
//! functions that need no lake (Spark's, JSON's, `sum` over DOUBLE, …). A node adds its own (`Lake::made`); a page
//! uses it as it is, so both plan a query alike (designs/l2-b4-front-and-web.md).
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, StringArray, StringBuilder, UnionArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::format::{ExplainFormat, MetricCategory, MetricType};
use datafusion::common::instant::Instant;
use datafusion::common::{internal_err, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;

/// A statement's session: `state`'s functions, settings and runtime, with catalogs of its own whose default catalog is
/// `catalog`, so what the statement registers is its alone. Both hosts copy their kept session this way
/// (`Lake::session_with`, the page's `statement`).
pub fn with_catalog(mut state: datafusion::execution::SessionState, catalog: Arc<dyn datafusion::catalog::CatalogProvider>) -> SessionContext {
    let catalogs = datafusion::catalog::MemoryCatalogProviderList::new();
    datafusion::catalog::CatalogProviderList::register_catalog(&catalogs, state.config().options().catalog.default_catalog.clone(), catalog);
    state.register_catalog_list(Arc::new(catalogs));
    SessionContext::new_with_state(state)
}

/// The session every query starts from: DataFusion's, with Pondra's rules and planners, and the functions
/// that need no lake. `Lake::made` adds the node's own.
pub fn session(config: SessionConfig, rt: Arc<RuntimeEnv>) -> SessionContext {
    let config = crate::optimize::config(config.with_information_schema(true));
    let state = SessionStateBuilder::new().with_config(config).with_runtime_env(rt).with_default_features();
    let mut state = state.with_analyzer_rules(crate::optimize::analyzer_rules()).with_optimizer_rules(crate::optimize::rules()).with_physical_optimizer_rules(crate::optimize::physical_rules());
    state.expr_planners().get_or_insert_with(Vec::new).insert(0, substrings()); // (SUBSTRING of bytes too)
    state.expr_planners().get_or_insert_with(Vec::new).insert(0, crate::intervals::planner()); // (n * INTERVAL '37 seconds')
    let mut ctx = SessionContext::new_with_state(state.build());
    // format_string, pmod, parse_url, …
    spark(&ctx); // (first: Spark's set is worked out against DataFusion's names alone)
    datafusion_functions_json::register_all(&mut ctx).expect("JSON functions register"); // json_get(…), ->, ->>
    ctx.register_udf(JSON_TEXT.as_ref().clone()); // pondra_json_text(x): a union (json_get's answer) as its text
    crate::asof::register(&ctx); // (ASOF JOIN's marker)
    crate::fsum::register(&ctx); // sum(DOUBLE): the same answer in any order
    crate::avg::register(&ctx); // avg(DECIMAL): rounded at its last place
    crate::casts::register(&ctx); // pondra_whole(x): what a cast to an integer rounds by
    crate::finish::register(&ctx); // pondra_moments(x): what a view keeps for a variance
    crate::optimize::register_zoned(&ctx); // to_timestamp(column): in the zone its type says
    // `pg_bytea` gives Postgres's bytea functions, `byte_length` among them (DataFusion's other byte functions, sha256
    // and md5, take BINARY as they are); substr is below.
    ctx.register_udf(substr().as_ref().clone());
    let ctx = crate::pg_json::planned(ctx); // (Postgres's JSON: its operators ahead of the planners above, its functions)
    crate::pg_functions::register(&ctx); // format(), array_upper(), to_char(ts, 'YYYY-MM'): what Postgres's clients send
    crate::pg_casts::register(&ctx); // gen_random_uuid(), uuidv7(), pondra_uuid(x), pg_typeof(x): the casts' functions, every session's
    crate::matcher::register(&ctx); // pondra_match(…) OVER (…): MATCH_RECOGNIZE's matcher
    ctx
}

/// Spark's functions whose names DataFusion doesn't have (`format_string`, `pmod`, `parse_url`,
/// `sha2`, `collect_list`, …: PySpark's vocabulary in SQL). Where both have a name, DataFusion's
/// stays, so no answer changes, and Spark's is there under a name of its own (`spark_floor`):
/// `spark_sql('…')` calls it where Spark SQL names `floor` (`sparksql.rs`).
fn spark(ctx: &SessionContext) {
    let more = SPARK.get_or_init(|| spark_of(ctx));
    more.scalar.iter().chain(&more.shared).for_each(|f| { ctx.register_udf(f.clone()); });
    more.aggregate.iter().for_each(|f| { ctx.register_udaf(f.clone()); });
}

pub struct Spark {
    scalar: Vec<datafusion::logical_expr::ScalarUDF>,
    aggregate: Vec<datafusion::logical_expr::AggregateUDF>,
    shared: Vec<datafusion::logical_expr::ScalarUDF>, // (Spark's of the names both have, renamed)
    pub names: std::collections::HashMap<String, String>, // a name both have (or its alias) → Spark's here
}

static SPARK: std::sync::OnceLock<Spark> = std::sync::OnceLock::new();

/// Spark's functions as every session has them (worked out once, against the first session's
/// names: DataFusion's own, nothing else yet).
pub fn spark_functions() -> &'static Spark {
    SPARK.get_or_init(|| spark_of(&SessionContext::new()))
}

fn spark_of(ctx: &SessionContext) -> Spark {
    use datafusion::execution::FunctionRegistry;
    let new = |name: &str, aliases: &[String]| std::iter::once(name).chain(aliases.iter().map(String::as_str)).all(|n| ctx.udf(n).is_err() && ctx.udaf(n).is_err() && ctx.udwf(n).is_err());
    let (scalar, shared): (Vec<_>, Vec<_>) = datafusion_spark::all_default_scalar_functions().into_iter().partition(|f| new(f.name(), f.aliases()));
    let aggregate = datafusion_spark::all_default_aggregate_functions().into_iter().filter(|f| new(f.name(), f.aliases()));
    let names = shared.iter().flat_map(|f| std::iter::once(f.name().to_string()).chain(f.aliases().iter().cloned()).map(|n| (n, format!("spark_{}", f.name())))).collect();
    Spark {
        scalar: scalar.iter().map(|f| f.as_ref().clone()).collect(),
        aggregate: aggregate.map(|f| f.as_ref().clone()).collect(),
        shared: shared.iter().map(renamed).collect(),
        names,
    }
}

/// Spark's version of a function DataFusion has too, under a name of its own (`spark_floor`):
/// everything as Spark's, but the name.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Renamed {
    name: String,
    inner: Arc<ScalarUDF>,
}

fn renamed(f: &Arc<ScalarUDF>) -> ScalarUDF {
    ScalarUDF::new_from_impl(Renamed { name: format!("spark_{}", f.name()), inner: f.clone() })
}

impl ScalarUDFImpl for Renamed {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, types: &[DataType]) -> datafusion::common::Result<DataType> {
        self.inner.return_type(types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::common::Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }
    fn coerce_types(&self, types: &[DataType]) -> datafusion::common::Result<Vec<DataType>> {
        self.inner.coerce_types(types)
    }
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> datafusion::common::Result<ExprSimplifyResult> {
        self.inner.simplify(args, info)
    }
    fn short_circuits(&self) -> bool {
        self.inner.short_circuits()
    }
    fn output_ordering(&self, inputs: &[ExprProperties]) -> datafusion::common::Result<SortProperties> {
        self.inner.output_ordering(inputs)
    }
    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        let f = Arc::new(self.inner.inner().with_updated_config(config)?);
        Some(ScalarUDF::new_from_impl(Renamed { name: self.name.clone(), inner: f }))
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// `pondra_json_text(x)`: a union (what `json_get` answers: a number, a string, a list…) as the text
/// of its value, which `->>` gives too. JSON, CSV, Parquet and the Postgres port can't write a union.
pub static JSON_TEXT: std::sync::LazyLock<Arc<ScalarUDF>> = std::sync::LazyLock::new(|| Arc::new(ScalarUDF::new_from_impl(JsonText(Signature::any(1, Volatility::Immutable)))));

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonText(Signature);

impl ScalarUDFImpl for JsonText {
    fn name(&self) -> &str { "pondra_json_text" }
    fn signature(&self) -> &Signature { &self.0 }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::Array;
        let array = args.args[0].to_array(args.number_rows)?;
        let Some(union) = array.as_any().downcast_ref::<UnionArray>() else {
            return Err(datafusion::error::DataFusionError::Execution("pondra_json_text takes a union".into()));
        };
        Ok(ColumnarValue::Array(Arc::new(union_text(union)?)))
    }
}

/// One union column's text: each row's value in the child its type id selects, NULL where that
/// value is NULL or the child is of type Null. Sparse and dense unions alike (`value_offset`).
fn union_text(u: &UnionArray) -> datafusion::arrow::error::Result<StringArray> {
    use datafusion::arrow::array::Array;
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    use std::collections::{hash_map::Entry, HashMap};
    let options = FormatOptions::default();
    let mut formats: HashMap<i8, ArrayFormatter<'_>> = HashMap::new(); // (one per child, made when first asked)
    let mut out: Vec<Option<String>> = Vec::with_capacity(u.len());
    for i in 0..u.len() {
        let id = u.type_id(i);
        let child = u.child(id);
        let at = u.value_offset(i);
        if child.data_type() == &DataType::Null || child.is_null(at) {
            out.push(None);
            continue;
        }
        let format = match formats.entry(id) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(v) => v.insert(ArrayFormatter::try_new(child.as_ref(), &options)?),
        };
        out.push(Some(format.value(at).to_string()));
    }
    Ok(StringArray::from(out))
}

/// `substr` and `SUBSTRING(… FROM … FOR …)`, and `POSITION(… IN …)`, for bytes and text: planned here, ahead of
/// DataFusion's own planner, which knows only the text ones.
fn substrings() -> Arc<dyn ExprPlanner> {
    Arc::new(Substrings)
}

#[derive(Debug)]
struct Substrings;

impl ExprPlanner for Substrings {
    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(substr().call(args)))
    }
    // (DataFusion passes `(string, substring)`, which is strpos's order)
    fn plan_position(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(crate::pg_bytea::strpos().call(args)))
    }
}

fn substr() -> Arc<ScalarUDF> {
    static SUBSTR: std::sync::LazyLock<Arc<ScalarUDF>> = std::sync::LazyLock::new(|| {
        let signature = Signature::user_defined(Volatility::Immutable).with_parameter_names(vec!["str", "start_pos", "length"]).expect("names"); // (DataFusion's: `substr(str => …, start_pos => …)`)
        Arc::new(ScalarUDF::new_from_impl(Substr { text: datafusion::functions::unicode::substr(), signature }))
    });
    SUBSTR.clone()
}

/// `substr(bytes, from [, count])` on BINARY, 1-based like the text one; text goes to DataFusion's.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Substr {
    text: Arc<ScalarUDF>,
    signature: Signature,
}

pub(crate) fn bytes(t: &DataType) -> bool {
    matches!(t, DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_))
}

impl ScalarUDFImpl for Substr {
    fn name(&self) -> &str {
        "substr"
    }
    fn aliases(&self) -> &[String] {
        self.text.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        match types {
            [b, rest @ ..] if bytes(b) && (1..=2).contains(&rest.len()) => Ok([DataType::Binary].into_iter().chain(rest.iter().map(|_| DataType::Int64)).collect()),
            _ => {
                let fields: Vec<_> = types.iter().map(|t| Arc::new(Field::new("a", t.clone(), true))).collect();
                Ok(datafusion::logical_expr::type_coercion::functions::fields_with_udf(&fields, self.text.as_ref())?.iter().map(|f| f.data_type().clone()).collect())
            }
        }
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.first().is_some_and(bytes) { Ok(DataType::Binary) } else { self.text.return_type(types) }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.first().is_some_and(|a| bytes(&a.data_type())) {
            return self.text.invoke_with_args(args);
        }
        let n = args.number_rows;
        let arrays: Vec<ArrayRef> = args.args.iter().map(|a| a.to_array(n)).collect::<Result<_>>()?;
        let (b, from) = (arrays[0].as_binary::<i32>(), arrays[1].as_primitive::<datafusion::arrow::datatypes::Int64Type>());
        let count = arrays.get(2).map(|c| c.as_primitive::<datafusion::arrow::datatypes::Int64Type>());
        let mut out = BinaryBuilder::new();
        for i in 0..n {
            if b.is_null(i) || from.is_null(i) || count.is_some_and(|c| c.is_null(i)) {
                out.append_null();
                continue;
            }
            let (v, start) = (b.value(i), from.value(i));
            // As SQL's: characters before position 1 count against the length.
            let end = match count {
                Some(c) if c.value(i) < 0 => return Err(crate::pg_json_make::refuse("22011", "negative substring length not allowed")),
                Some(c) => start.saturating_add(c.value(i)),
                None => i64::MAX,
            };
            let (lo, hi) = (start.max(1) - 1, (end - 1).clamp(0, v.len() as i64));
            out.append_value(if lo < hi { &v[lo as usize..hi as usize] } else { &[][..] });
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// A query's union columns planned as their text: every door plans through `sql`, so its schema,
/// its rows and a table made of it all say Utf8. A query with no union column is returned as it is.
fn text_unions(df: datafusion::dataframe::DataFrame) -> datafusion::error::Result<datafusion::dataframe::DataFrame> {
    if !df.schema().fields().iter().any(|f| matches!(f.data_type(), DataType::Union(..))) {
        return Ok(df);
    }
    let exprs: Vec<Expr> = df
        .schema()
        .iter()
        .map(|(qualifier, f)| {
            let column = Expr::Column(datafusion::common::Column::from((qualifier, f)));
            match f.data_type() {
                DataType::Union(..) => crate::engine::JSON_TEXT.call(vec![column]).alias_qualified(qualifier.cloned(), f.name()), // (a join's two of one name stay two)
                _ => column,
            }
        })
        .collect();
    df.select(exprs)
}

/// What SQL from users may do on a node: queries only. No `COPY … TO` files, no `CREATE EXTERNAL
/// TABLE` over the node's disk, no session DDL; writes go through `write.rs`, `SET` through pg.rs.
pub fn read_only() -> datafusion::execution::context::SQLOptions {
    datafusion::execution::context::SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false)
}

/// A user's query planned (`read_only`), with a higher-order function's `x -> …` as a lambda
/// (`friendly::lambdas`: the generic dialect's text keeps it as JSON's arrow, so it is planned from
/// the tree). A session in another dialect reads its own lambdas, and its own strings: read as the
/// generic dialect, Databricks' `"a"` became a column.
pub async fn sql(ctx: &SessionContext, sql: &str) -> anyhow::Result<datafusion::dataframe::DataFrame> {
    sql_checked(ctx, sql, |plan| Ok(read_only().verify_plan(plan)?)).await
}

/// A user's statement planned as `sql` plans it, refused unless `may` allows its plan. `may` runs before
/// anything does: a `CREATE TABLE … AS` runs as it is planned.
pub async fn sql_checked(ctx: &SessionContext, sql: &str, may: impl Fn(&LogicalPlan) -> anyhow::Result<()>) -> anyhow::Result<datafusion::dataframe::DataFrame> {
    let plan = planned(ctx, sql).await?;
    may(&plan)?;
    run(ctx, plan).await
}

/// A user's statement planned, and nothing run: `run` runs the plan. A caller that must judge the plan first, or hold a
/// lock across the run, takes the two apart. Planning makes no change; a DDL statement takes effect when it runs.
pub async fn planned(ctx: &SessionContext, sql: &str) -> anyhow::Result<LogicalPlan> {
    use datafusion::config::Dialect;
    let generic = matches!(ctx.state().config().options().sql_parser.dialect, Dialect::Generic);
    // (a MATCH_RECOGNIZE's measure named like an input's column: DataFusion's duplicate field, said as the refusal: `pattern::reworded`)
    Ok(match crate::friendly::lambdas(sql).filter(|_| generic) {
        Some(stmt) => ctx.state().statement_to_plan(stmt).await.map_err(|e| crate::pattern::reworded(e, sql))?,
        None => ctx.state().create_logical_plan(sql).await.map_err(|e| crate::pattern::reworded(e, sql))?,
    })
}

/// A planned statement run, as `sql_checked` runs it: the frame of its rows. A DDL statement takes effect here.
pub async fn run(ctx: &SessionContext, plan: LogicalPlan) -> anyhow::Result<datafusion::dataframe::DataFrame> {
    Ok(text_unions(ctx.execute_logical_plan(plan).await?)?)
}

/// A plan's rows; an answer with none still has its columns, typed as the plan runs them (the physical plan's
/// schema, not the planner's, which types `1e15` as a negative-scale decimal: invariant 88).
pub fn kept(rows: Vec<RecordBatch>, plan: &Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    if rows.is_empty() {
        return vec![RecordBatch::new_empty(plan.schema())];
    }
    rows
}

/// A frame's rows, as `kept` gives them. An `EXPLAIN ANALYZE` runs its plan in this task (`analyzed`).
pub async fn collect(df: datafusion::dataframe::DataFrame) -> anyhow::Result<Vec<RecordBatch>> {
    if let LogicalPlan::Analyze(a) = df.logical_plan() {
        let level = a.analyze_level;
        return analyzed(df, level).await;
    }
    let task = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;
    Ok(kept(datafusion::physical_plan::collect(plan.clone(), task).await?, &plan))
}

/// `EXPLAIN ANALYZE` run in this task. DataFusion's `AnalyzeExec` runs each input partition on a tokio task, and a
/// page has no tokio runtime to spawn one on: it panics there and takes every table of the page with it. So the
/// input's partitions run here, all at once (`in_task::stream`: a hash join's probe partitions wait for each other),
/// and the answer is built as DataFusion builds it. Its names and estimates are made readable by `explain::answered`,
/// on a node and a page alike.
async fn analyzed(df: datafusion::dataframe::DataFrame, level: Option<MetricType>) -> anyhow::Result<Vec<RecordBatch>> {
    let task = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;
    let Some(exec) = plan.downcast_ref::<AnalyzeExec>() else {
        return Ok(kept(datafusion::physical_plan::collect(plan.clone(), task).await?, &plan));
    };
    let input = exec.input().clone();
    let start = Instant::now();
    let mut rows = 0;
    let mut stream = crate::in_task::stream(input, task.clone())?;
    while let Some(batch) = stream.next().await.transpose()? {
        rows += batch.num_rows();
    }
    let duration = Instant::now() - start;
    // (The planner works the metric types out from the statement's level or the session's; `AnalyzeExec` keeps
    // the result but doesn't expose it, so it is worked out here the same way.)
    let level = level.unwrap_or(task.session_config().options().explain.analyze_level);
    Ok(vec![analyze_output(exec, &plan.schema(), rows, duration, &level.included_types())?])
}

/// The answer of an `EXPLAIN ANALYZE` as one batch of (plan type, plan) rows. This is DataFusion's
/// `create_output_batch` (`physical-plan/src/analyze.rs`), which is private, so its text is kept the same: a page's
/// answer is a node's.
fn analyze_output(exec: &AnalyzeExec, schema: &SchemaRef, rows: usize, duration: Duration, types: &[MetricType]) -> Result<RecordBatch> {
    let (input, verbose) = (exec.input().as_ref(), exec.verbose());
    let categories = exec.metric_categories().map(<[MetricCategory]>::to_vec);
    let mut kinds = StringBuilder::new();
    let mut plans = StringBuilder::new();
    match exec.format() {
        ExplainFormat::Indent => {
            kinds.append_value("Plan with Metrics");
            let plan = DisplayableExecutionPlan::with_metrics(input).set_metric_types(types.to_vec()).set_metric_categories(categories.clone()).set_show_statistics(exec.show_statistics());
            plans.append_value(plan.indent(verbose).to_string());
            if verbose {
                kinds.append_value("Plan with Full Metrics");
                let full = DisplayableExecutionPlan::with_full_metrics(input).set_metric_types(types.to_vec()).set_metric_categories(categories).set_show_statistics(exec.show_statistics());
                plans.append_value(full.indent(verbose).to_string());
                kinds.append_value("Output Rows");
                plans.append_value(rows.to_string());
                kinds.append_value("Duration");
                plans.append_value(format!("{duration:?}"));
            }
        }
        ExplainFormat::PostgresJSON => {
            // (As DataFusion has it: the JSON renderer prints no statistics, so none are asked for here.)
            kinds.append_value("Plan with Metrics");
            let plan = if verbose { DisplayableExecutionPlan::with_full_metrics(input) } else { DisplayableExecutionPlan::with_metrics(input) };
            let mut plan = plan.set_metric_types(types.to_vec()).set_metric_categories(categories);
            if verbose {
                plan = plan.set_summary(Some(rows), Some(duration));
            }
            plans.append_value(plan.pgjson(verbose).to_string());
        }
        other => return internal_err!("AnalyzeExec does not support {other} output format"),
    }
    Ok(RecordBatch::try_new(schema.clone(), vec![Arc::new(kinds.finish()), Arc::new(plans.finish())])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::util::display::array_value_to_string;

    /// A session answers as a node's does: one value per query, from the functions the session has.
    #[tokio::test]
    async fn a_session_answers_as_a_node() {
        let ctx = session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let cases = [
            ("SELECT pmod(-7, 3)", "2"),
            ("SELECT sum(x) FROM (VALUES (CAST('1e16' AS DOUBLE)), (CAST('1' AS DOUBLE)), (CAST('-1e16' AS DOUBLE))) t(x)", "1.0"), // (a plain sum gives 0)
            ("SELECT byte_length(X'010203')", "3"),
            ("SELECT avg(x) FROM (VALUES (CAST(1 AS DECIMAL(10,2))), (2), (2)) t(x)", "1.666667"),
            ("SELECT '{\"a\": 7}' ->> 'a'", "7"),
            ("SELECT encode(substring(X'0102030405' FROM 2 FOR 2), 'hex')", "0203"),
        ];
        for (sql, want) in cases {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let got = array_value_to_string(batches[0].column(0), 0).unwrap();
            assert_eq!(got, want, "{sql}");
        }
    }

    /// `EXPLAIN ANALYZE` is answered by `collect` (`analyzed`, this file's copy of DataFusion's `create_output_batch`) and
    /// by DataFusion itself (its `AnalyzeExec`): the same columns, the same plan types, and the same plan text once every
    /// number is `N` (times and counts differ run to run). If DataFusion changes its answer, this fails, and the copy follows.
    #[tokio::test]
    async fn explain_analyze_is_datafusions_answer() {
        let ctx = session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let batch = RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)]).unwrap();
        ctx.register_batch("t", batch).unwrap();
        let numbers = regex::Regex::new(r"\d+(\.\d+)?(ns|µs|ms|s|K|M|B)?").expect("a regex");
        let texts = |batches: &[RecordBatch], col: usize| -> Vec<String> {
            batches.iter().flat_map(|b| b.column(col).as_string::<i32>().iter().map(|v| v.unwrap_or_default().to_string()).collect::<Vec<_>>()).collect()
        };
        let names = |batches: &[RecordBatch]| batches[0].schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>();
        for sql in ["EXPLAIN ANALYZE SELECT count(*) FROM t", "EXPLAIN ANALYZE VERBOSE SELECT count(*) FROM t"] {
            let ours = collect(ctx.sql(sql).await.unwrap()).await.unwrap();
            let theirs = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let drift = "the copy has drifted from DataFusion's create_output_batch (physical-plan/src/analyze.rs)";
            assert_eq!(names(&ours), names(&theirs), "{sql}: its columns; {drift}");
            assert_eq!(texts(&ours, 0), texts(&theirs, 0), "{sql}: its plan types; {drift}");
            let plans = |b: &[RecordBatch]| texts(b, 1).iter().map(|t| numbers.replace_all(t, "N").into_owned()).collect::<Vec<_>>();
            assert_eq!(plans(&ours), plans(&theirs), "{sql}: its plan text with numbers as N; {drift}");
        }
    }
}
