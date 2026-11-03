//! The session every query starts from, in every build: DataFusion's, with Pondra's rules, planners and the
//! functions that need no lake (Spark's, JSON's, `sum` over DOUBLE, …). A node adds its own (`Lake::made`); a page
//! uses it as it is, so both plan a query alike (designs/l2-b4-front-and-web.md).
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, StringArray, StringBuilder, UnionArray};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::format::{ExplainFormat, MetricCategory, MetricType};
use datafusion::common::instant::Instant;
use datafusion::common::{internal_err, DFSchema, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::{ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::sqlparser::ast::BinaryOperator;
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

/// `ctx`, which reads files by their path or URL as tables (`FROM 'jan.parquet'`): DataFusion's `enable_url_table`,
/// with the session's own state changed in place, where DataFusion builds the whole state again (7% of a short
/// query). Every caller's session is its statement's own (`query::session`), so no other session sees the change.
pub fn with_files(ctx: SessionContext) -> SessionContext {
    use datafusion::catalog::{DynamicFileCatalog, UrlTableFactory};
    let factory = Arc::new(datafusion::datasource::dynamic_file::DynamicListTableFactory::new(Default::default()));
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        let current = Arc::clone(state.catalog_list());
        state.register_catalog_list(Arc::new(DynamicFileCatalog::new(current, Arc::clone(&factory) as Arc<dyn UrlTableFactory>)));
    }
    factory.session_store().with_state(ctx.state_weak_ref()); // (as DataFusion's: the factory plans its tables in this session)
    ctx
}

/// The session every query starts from: DataFusion's, with Pondra's rules and planners, and every function set in the
/// registry (`Functions`): the engine's own, Spark's, Postgres's, MATCH_RECOGNIZE's, the AI functions. A node's
/// statements add the lake's own (`Lake::made`, `query::session_at`).
pub fn session(config: SessionConfig, rt: Arc<RuntimeEnv>) -> SessionContext {
    let config = crate::optimize::config(config.with_information_schema(true));
    let state = SessionStateBuilder::new().with_config(config).with_runtime_env(rt).with_default_features();
    let mut state = state.with_analyzer_rules(crate::optimize::analyzer_rules()).with_optimizer_rules(crate::optimize::rules()).with_physical_optimizer_rules(crate::optimize::physical_rules());
    state.expr_planners().get_or_insert_with(Vec::new).insert(0, substrings()); // (SUBSTRING of bytes too)
    state.expr_planners().get_or_insert_with(Vec::new).insert(0, crate::intervals::planner()); // (n * INTERVAL '37 seconds')
    state.expr_planners().get_or_insert_with(Vec::new).insert(0, Arc::new(NullConcat)); // (NULL || NULL)
    let mut ctx = SessionContext::new_with_state(state.build());
    for set in &crate::registry::get().section::<Functions>().0 {
        ctx = (set.add)(ctx);
    }
    ctx
}

/// The registry's functions section (designs/redesign/redesign.md §6): the sets of functions every session has, each
/// added by the extension that owns it, in the order the build lists them (`crate::built_in`). Each set is added to the
/// session the sets before it made, so it sees their functions, and where two name one function the later one's stays.
#[derive(Default)]
pub struct Functions(Vec<FunctionSet>);

/// One extension's functions: `add` registers them with a session, or builds the session again with them (Postgres's
/// JSON puts its operators' planner ahead of the others).
pub struct FunctionSet {
    pub name: &'static str,
    pub add: fn(SessionContext) -> SessionContext,
}

impl crate::registry::Section for Functions {
    const NAME: &'static str = "functions";
}

impl Functions {
    pub fn add(&mut self, name: &'static str, add: fn(SessionContext) -> SessionContext) {
        assert!(self.0.iter().all(|s| s.name != name), "two function sets are named {name}");
        self.0.push(FunctionSet { name, add });
    }

    /// The sets' names, in the order a session gets them.
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.0.iter().map(|s| s.name)
    }
}

/// The engine's own functions, as an extension.
pub static ENGINE: crate::registry::BuiltIn = crate::registry::BuiltIn {
    name: "engine",
    requires: &[],
    register: |r| r.section_mut::<Functions>().add("engine", own),
};

fn own(mut ctx: SessionContext) -> SessionContext {
    datafusion_functions_json::register_all(&mut ctx).expect("JSON functions register"); // json_get(…), ->, ->>
    ctx.register_udf(JSON_TEXT.as_ref().clone()); // pondra_json_text(x): a union (json_get's answer) as its text
    crate::asof::register(&ctx); // (ASOF JOIN's marker)
    crate::fsum::register(&ctx); // sum(DOUBLE): the same answer in any order
    crate::avg::register(&ctx); // avg(DECIMAL): rounded at its last place
    crate::string_agg::register(&ctx); // string_agg: one group's string held to a share of the query memory
    crate::casts::register(&ctx); // pondra_whole(x): what a cast to an integer rounds by
    crate::dialect_fns::register(&ctx); // safe_divide(a, b): BigQuery's, typed by its arguments
    crate::finish::register(&ctx); // pondra_moments(x): what a view keeps for a variance
    crate::optimize::register_zoned(&ctx); // to_timestamp(column): in the zone its type says
    // `pg_bytea` gives Postgres's bytea functions, `byte_length` among them (DataFusion's other byte functions, sha256
    // and md5, take BINARY as they are); substr is below.
    ctx.register_udf(substr().as_ref().clone());
    #[cfg(feature = "lake")]
    crate::panics::test_function(&ctx); // pondra_panic(), for the tests only
    ctx
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

/// `NULL || NULL`, both sides of no type (a NULL, a CASE of NULLs): NULL text, as Postgres reads `unknown || unknown`,
/// where DataFusion found no string type for the two and refused the query. DuckDB answers NULL too. A NULL beside text or
/// a list keeps the planning it has.
#[derive(Debug)]
struct NullConcat;

impl ExprPlanner for NullConcat {
    fn plan_binary_op(&self, mut e: RawBinaryExpr, schema: &DFSchema) -> Result<PlannerResult<RawBinaryExpr>> {
        let null = |x: &Expr| x.get_type(schema).is_ok_and(|t| t == DataType::Null);
        if matches!(e.op, BinaryOperator::StringConcat) && null(&e.left) && null(&e.right) {
            e.left = datafusion::logical_expr::cast(e.left, DataType::Utf8);
        }
        Ok(PlannerResult::Original(e))
    }
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
    let state = ctx.state(); // (one copy of the session's state: each is a clone of all of it, ~80K instructions a query)
    let generic = matches!(state.config().options().sql_parser.dialect, Dialect::Generic);
    // (a MATCH_RECOGNIZE's measure named like an input's column: DataFusion's duplicate field, said as the refusal: `pattern::reworded`)
    Ok(match crate::friendly::lambdas(sql).filter(|_| generic) {
        Some(stmt) => state.statement_to_plan(stmt).await.map_err(|e| crate::pattern::reworded(e, sql))?,
        None => state.create_logical_plan(sql).await.map_err(|e| crate::pattern::reworded(e, sql))?,
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

    /// Every session gets the build's function sets, in the order `crate::built_in` lists their extensions, and has
    /// each set's functions (one of each, by a name only it registers).
    #[test]
    fn a_session_has_every_function_set_in_order() {
        let sets: Vec<_> = crate::registry::get().section::<Functions>().names().collect();
        assert_eq!(sets, ["spark", "engine", "postgres", "match_recognize", "ai"]);
        let ctx = session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        use datafusion::execution::FunctionRegistry;
        for f in ["pmod", "pondra_json_text", "quote_ident", "gen_random_uuid", "ai_embed"] {
            assert!(ctx.udf(f).is_ok(), "{f}");
        }
        assert!(ctx.udwf("pondra_match").is_ok());
    }

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
            ("SELECT arrow_typeof(NULL || NULL)", "Utf8"), // (text, as Postgres reads `unknown || unknown`: DataFusion refused it)
            ("SELECT (CASE WHEN true THEN NULL ELSE NULL END || NULL) IS NULL", "true"),
            ("SELECT upper(NULL || NULL) IS NULL", "true"),
            ("SELECT coalesce(NULL || NULL, 'a')", "a"),
            ("SELECT count(*) FROM (VALUES (1)) t(x) WHERE (NULL || NULL) IS NULL", "1"),
            ("SELECT 'a' || NULL IS NULL", "true"),
        ];
        for (sql, want) in cases {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let got = array_value_to_string(batches[0].column(0), 0).unwrap();
            assert_eq!(got, want, "{sql}");
        }
    }

    /// `with_files` lets a session read a file by its path as a table, which a plain session can't, and keeps the session's id.
    #[tokio::test]
    async fn with_files_reads_a_file_by_its_path() {
        async fn rows_of(ctx: &SessionContext, sql: &str) -> datafusion::common::Result<Vec<RecordBatch>> {
            ctx.sql(sql).await?.collect().await
        }
        let dir = std::env::temp_dir().join(format!("pondra-with-files-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.csv");
        std::fs::write(&path, "a,b\n1,x\n2,y\n").unwrap();
        let sql = format!("SELECT count(*) FROM '{}'", path.display());
        assert!(rows_of(&SessionContext::new(), &sql).await.is_err(), "a plain session reads no file by its path");
        let ctx = SessionContext::new();
        let id = ctx.session_id();
        let ctx = with_files(ctx);
        assert_eq!(ctx.session_id(), id, "the session keeps its id");
        let batches = rows_of(&ctx, &sql).await.unwrap();
        assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), "2");
        let _ = std::fs::remove_dir_all(&dir);
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
