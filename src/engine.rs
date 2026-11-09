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
/// query). Every caller's session is its statement's own (`query::session_for`), so no other session sees the change.
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
/// statements add the lake's own (`Lake::made`, `query::session_for`).
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

/// The engine's own functions, as an extension. It lists every set's as `pondra.functions`.
pub static ENGINE: crate::registry::BuiltIn = crate::registry::BuiltIn {
    name: "engine",
    requires: &[],
    register: |r| {
        r.section_mut::<Functions>().add("engine", own);
        let passes = r.section_mut::<crate::passes::Passes>();
        for p in [crate::kept::pass(), crate::dialect_stmts::pass(), crate::diff::pass(), crate::past_names::pass(), crate::text::types_pass()].into_iter().chain(crate::friendly::passes()) {
            passes.add(p);
        }
        r.section_mut::<crate::lists::Lists>().add(crate::lists::List { name: "functions", columns: FUNCTION_COLUMNS, rows: functions });
        #[cfg(feature = "lake")] // (what a lake keeps for the engine's modules, `lake_state.rs`: the answers kept for their doors' pages, a joined page's unsent statements and a node's count of them, its file stores and catalogs' tokens, and the shuffles its node takes part in)
        {
            let states = r.section_mut::<crate::lake_state::Registered>();
            states.add::<crate::pages::Pages>();
            states.add::<crate::page_records::Unsent>();
            states.add::<crate::page_records::Counted>();
            states.add::<crate::ext::Stores>();
            states.add::<crate::ext::Tokens>();
            states.add::<crate::spmd::Shuffles>();
            states.add::<crate::users::SignIns>();
            states.add::<crate::users::Turns>();
            states.add::<crate::temp::Sessions>();
            states.add::<crate::tier::Rounds>();
            states.add::<crate::guard::Seen>();
            states.add::<crate::auth::Guesses>();
        }
        #[cfg(feature = "lake")] // (the engine floor's catalog records: `records`)
        for p in [crate::backup::PREFIXES, crate::branch::PREFIXES, crate::copy::PREFIXES, crate::ddl::PREFIXES, crate::ext::PREFIXES, crate::protect::PREFIXES,
                  crate::runs::PREFIXES, crate::tasks::PREFIXES, crate::users::PREFIXES, crate::views::PREFIXES] {
            r.section_mut::<crate::records::Prefixes>().add(p);
        }
        #[cfg(feature = "lake")]
        r.section_mut::<crate::records::Rights>().add(crate::users::RIGHTS);
    },
};

/// `pondra.functions`' columns: a function's name, whether it is a `scalar`, `aggregate`, `window` or `table` function,
/// and the set that gives it, its `source` (`datafusion` for DataFusion's own, else its extension's: `engine`, `spark`, `postgres`…).
const FUNCTION_COLUMNS: &[(&str, crate::lists::Cell)] = {
    use crate::lists::Cell::Text;
    &[("name", Text), ("kind", Text), ("source", Text)]
};

/// One row per function a session has, made as `session` makes one: DataFusion's, then each set in turn, a name the
/// set added or took over being its. A lake's own routines are `pondra.routines`'.
fn functions(r: &crate::registry::Registry) -> Vec<serde_json::Value> {
    use std::collections::BTreeMap;
    fn names<T>(m: &std::collections::HashMap<String, Arc<T>>) -> impl Iterator<Item = (&String, usize)> {
        m.iter().map(|(n, f)| (n, Arc::as_ptr(f) as *const () as usize))
    }
    let now = |ctx: &SessionContext| {
        let s = ctx.state();
        let mut all: BTreeMap<(String, &'static str), usize> = BTreeMap::new();
        for (kind, them) in [("scalar", names(s.scalar_functions()).collect::<Vec<_>>()), ("aggregate", names(s.aggregate_functions()).collect()), ("window", names(s.window_functions()).collect())] {
            all.extend(them.into_iter().map(|(n, p)| ((n.clone(), kind), p)));
        }
        all.extend(names(s.table_functions()).map(|(n, p)| ((n.clone(), "table"), p)));
        all
    };
    let mut ctx = SessionContext::new();
    let mut set: BTreeMap<(String, &'static str), (&'static str, usize)> = now(&ctx).into_iter().map(|(k, p)| (k, ("datafusion", p))).collect();
    for s in &r.section::<Functions>().0 {
        ctx = (s.add)(ctx);
        let has = now(&ctx);
        set.retain(|k, _| has.contains_key(k)); // (a set may take a name away)
        for (k, p) in has {
            if set.get(&k).is_none_or(|&(_, was)| was != p) {
                set.insert(k, (s.name, p));
            }
        }
    }
    set.into_iter().map(|((name, kind), (set, _))| serde_json::json!({"name": name, "kind": kind, "source": set})).collect()
}

fn own(mut ctx: SessionContext) -> SessionContext {
    datafusion_functions_json::register_all(&mut ctx).expect("JSON functions register"); // json_get(…), ->, ->>
    ctx.register_udf(JSON_TEXT.as_ref().clone()); // pondra_json_text(x): a union (json_get's answer) as its text
    crate::asof::register(&ctx); // (ASOF JOIN's marker)
    crate::fsum::register(&ctx); // sum(DOUBLE): the same answer in any order
    crate::fvar::register(&ctx); // var, stddev (and avg, below) over DOUBLE: the same answer in any order
    crate::avg::register(&ctx); // avg(DECIMAL): rounded at its last place
    crate::string_agg::register(&ctx); // string_agg: one group's string held to a share of the query memory
    crate::casts::register(&ctx); // pondra_whole(x): what a cast to an integer rounds by
    crate::one_row::register(&ctx); // pondra_one_row(s): a scalar subquery's one row, or 21000
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
    // (and a query the front's looks parsed, planned from that parse, when the session parses as they did: `parsed_query`)
    let as_looks = generic && state.config().options().sql_parser.recursion_limit.get() == LOOKS_DEPTH;
    let (stmt, kept) = match crate::friendly::lambdas(sql).filter(|_| generic) {
        Some(stmt) => (Some(stmt), false),
        None => match as_looks.then(|| parsed_query(sql)).flatten() {
            Some(stmt) => (Some(stmt), true),
            None => (None, false),
        },
    };
    if let (Some(stmt), true) = (&stmt, kept && crate::parsed::checked()) {
        datafusions_parse(&state, sql, stmt)?;
    }
    match stmt {
        Some(stmt) => state.statement_to_plan(stmt).await,
        None => state.create_logical_plan(sql).await,
    }
    .map_err(|e| crate::pattern::reworded(crate::rights::said(e), sql))
}

/// `kept`, the front's parse of `sql`, is the statement DataFusion parses `sql` to with the session's settings: an error
/// that names the text and both statements when it isn't (`parsed_query` would then be taking a text it must not). The
/// statements are compared, not their plans: DataFusion's planner gives one statement plans that differ run to run where
/// no answer does (the columns an ORDER BY adds below its projection come out of a hash set, in either order).
fn datafusions_parse(state: &datafusion::execution::SessionState, sql: &str, kept: &datafusion::sql::parser::Statement) -> anyhow::Result<()> {
    let theirs = state.sql_to_statement(sql, &state.config().options().sql_parser.dialect);
    anyhow::ensure!(theirs.as_ref().is_ok_and(|t| t == kept), "one parse: the front's parse of the text is not DataFusion's, for {sql}\nthe front's: {kept}\nDataFusion's: {theirs:?}");
    Ok(())
}

/// The depth the front's looks parse to: sqlparser's own limit, which is also DataFusion's default
/// (`sql_parser.recursion_limit`). A session set to another depth parses each text itself.
const LOOKS_DEPTH: usize = 50;

/// A query the front's looks parsed already (`parsed::already`), to plan from that parse rather than parse the text a
/// second time (step 8's one parse, while the front still hands on text). In the generic dialect at the same depth limit
/// DataFusion parses a query as this parse did: sqlparser's tokenizer and parser, for any statement but its own CREATE,
/// COPY, EXPLAIN and RESET. So only one statement that is a query is taken, and a text that didn't parse is parsed by
/// DataFusion, for its own words.
fn parsed_query(sql: &str) -> Option<datafusion::sql::parser::Statement> {
    use datafusion::sql::sqlparser::ast::Statement;
    match crate::parsed::already(sql)?.as_ref() {
        Ok(stmts) => match stmts.as_slice() {
            [q @ Statement::Query(_)] => Some(datafusion::sql::parser::Statement::Statement(Box::new(q.clone()))),
            _ => None,
        },
        Err(_) => None,
    }
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

    /// `pondra.functions` lists what a session has, each function under the set that gave it.
    #[test]
    fn pondra_functions_lists_each_function_by_its_set() {
        let list = crate::lists::all().iter().find(|l| l.name == "functions").expect("the engine lists its functions");
        let rows = crate::lists::rows(list);
        let set = |name: &str, kind: &str| rows.iter().find(|r| r["name"] == name && r["kind"] == kind).map(|r| r["source"].as_str().unwrap_or_default().to_string());
        assert_eq!(
            [set("abs", "scalar"), set("pmod", "scalar"), set("pondra_json_text", "scalar"), set("quote_ident", "scalar"), set("ai_embed", "scalar"), set("pondra_match", "window"), set("sum", "aggregate"), set("generate_series", "table")],
            [Some("datafusion"), Some("spark"), Some("engine"), Some("postgres"), Some("ai"), Some("match_recognize"), Some("engine"), Some("postgres")].map(|s| s.map(String::from))
        );
        let ctx = session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let state = ctx.state();
        assert_eq!(rows.iter().filter(|r| r["kind"] == "scalar").count(), state.scalar_functions().len(), "every scalar function a session has, once");
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

    /// A query planned from the front's parse (`parsed_query`) is the plan DataFusion makes of its text: the same tree, the
    /// same names and types, and the same refusal. If DataFusion's parser drifts from sqlparser's (its options: unescaping,
    /// the dialect's trailing commas; its depth; a statement it parses itself), this fails, and `parsed_query` takes fewer
    /// texts. The parse is kept under the very text it was made of, so it is planned only for that text (`parsed::already`).
    #[tokio::test]
    async fn a_query_planned_from_the_fronts_parse_is_datafusions_plan() {
        use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
        let ctx = session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let batch = RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)]).unwrap();
        ctx.register_batch("t", batch).unwrap();
        let state = ctx.state();
        assert_eq!(state.config().options().sql_parser.recursion_limit.get(), LOOKS_DEPTH, "DataFusion's default depth");
        let deep = |n: usize| format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
        let texts = [
            "SELECT x, count(*) AS n FROM t WHERE x > 1 GROUP BY x ORDER BY x DESC NULLS FIRST LIMIT 2 OFFSET 0".to_string(),
            "SELECT 'it''s', 'a\\nb', E'a\\nb', U&'\\0041', X'0102', 1e3, 0.1, -7, 2::bigint, CAST(x AS DOUBLE) FROM t".to_string(),
            r#"SELECT "x" AS "Mixed Case", "x" AS "a""b", t.x AS ünï FROM t AS t"#.to_string(),
            "SELECT E'\\n', E'it\\'s', 'a\nb', 'x''y''' FROM t".to_string(), // (escapes as the tokenizer unescapes them; a newline kept)
            "SELECT x, sum(x), FROM t GROUP BY x".to_string(), // (a trailing comma, as the dialect's own setting reads it)
            "SELECT x FROM t WHERE x IN (1, 2,)".to_string(),
            "WITH a AS (SELECT x FROM t), b AS (SELECT x FROM a) SELECT * FROM b UNION ALL SELECT 1".to_string(),
            "SELECT x, sum(x) OVER (PARTITION BY x % 2 ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t".to_string(),
            "SELECT [1, 2], {'a': 1}, INTERVAL '1 day', x IN (1, 2), x BETWEEN 1 AND 2, x IS DISTINCT FROM 1 FROM t -- a comment".to_string(),
            "/* a comment */ VALUES (1, 'a'), (2, 'b')".to_string(),
            "SELECT * FROM t WHERE x = $1".to_string(),
            "SELECT x FROM nope".to_string(),
            "SELECT nope FROM t".to_string(),
            deep(LOOKS_DEPTH / 2),
        ];
        let mut refused = vec![]; // (the texts neither parse takes)
        for sql in &texts {
            let theirs = state.create_logical_plan(sql).await.map(|p| p.display_indent_schema().to_string()).map_err(|e| e.to_string());
            let Ok(parsed) = Parser::parse_sql(&GenericDialect {}, sql) else {
                assert!(theirs.is_err(), "{sql}: DataFusion parsed a text sqlparser refused");
                refused.push(sql.as_str());
                continue;
            };
            assert!(matches!(parsed.as_slice(), [datafusion::sql::sqlparser::ast::Statement::Query(_)]), "{sql}: one query");
            let stmt = datafusion::sql::parser::Statement::Statement(Box::new(parsed[0].clone()));
            datafusions_parse(&state, sql, &stmt).unwrap_or_else(|e| panic!("{e}"));
            let ours = state.statement_to_plan(stmt).await.map(|p| p.display_indent_schema().to_string()).map_err(|e| e.to_string());
            assert_eq!(ours, theirs, "{sql}");
        }
        assert_eq!(refused, ["SELECT x FROM t WHERE x IN (1, 2,)"], "the texts neither parse takes");
        // (the check itself: a statement unlike DataFusion's parse of the text is refused, naming the text; its own passes)
        let sql = "SELECT x FROM t WHERE x > 1";
        let of = |sql: &str| datafusion::sql::parser::Statement::Statement(Box::new(Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0)));
        let err = datafusions_parse(&state, sql, &of("SELECT x FROM t WHERE x > 2")).expect_err("another statement").to_string();
        assert!(err.starts_with("one parse:") && err.contains(sql), "{err}");
        datafusions_parse(&state, sql, &of(sql)).unwrap();
        // (too deep for either parse: DataFusion parses it, for its own words)
        let sql = deep(LOOKS_DEPTH + 5);
        assert!(Parser::parse_sql(&GenericDialect {}, &sql).is_err() && state.create_logical_plan(&sql).await.is_err());
        // (the kept parse is the text's parse, or none: never another text's)
        assert!(crate::parsed::already("SELECT 'never parsed by a look'").is_none());
        let sql = "SELECT x + 1 AS y FROM t";
        let _ = crate::parsed::generic(sql);
        if let Some(kept) = crate::parsed::already(sql) {
            assert_eq!(*kept, Parser::parse_sql(&GenericDialect {}, sql));
        }
        assert_eq!(
            planned(&ctx, sql).await.unwrap().display_indent_schema().to_string(),
            state.create_logical_plan(sql).await.unwrap().display_indent_schema().to_string()
        );
    }

    /// A session that parses otherwise than the front's looks (another dialect, another depth) plans each text from its
    /// own parse, even with the front's parse of that text at hand: `planned` takes the kept parse only as the looks parse.
    #[tokio::test]
    async fn a_session_that_parses_otherwise_plans_from_the_text() {
        use datafusion::config::Dialect;
        let with = |set: &dyn Fn(&mut SessionConfig)| {
            let mut config = SessionConfig::new();
            set(&mut config);
            let ctx = session(config, Arc::new(RuntimeEnv::default()));
            let batch = RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)]).unwrap();
            ctx.register_batch("t", batch).unwrap();
            ctx
        };
        // (MySQL reads "x" as a string, the generic dialect as a column)
        let mysql = with(&|c| c.options_mut().sql_parser.dialect = Dialect::MySQL);
        let sql = r#"SELECT "x" AS v FROM t WHERE 'mysql' = 'mysql'"#;
        assert!(crate::parsed::generic(sql).is_ok());
        let plan = planned(&mysql, sql).await.unwrap().display_indent().to_string();
        assert!(plan.contains(r#"Utf8("x")"#), "{plan}");
        // (a depth the looks take but a session set shallower refuses)
        let shallow = with(&|c| c.options_mut().sql_parser.recursion_limit = datafusion::config::ConfigNonZeroUsize::try_new(10).unwrap());
        let sql = format!("SELECT {}x{} FROM t WHERE 'shallow' = 'shallow'", "(".repeat(20), ")".repeat(20));
        assert!(crate::parsed::generic(&sql).is_ok());
        let err = planned(&shallow, &sql).await.expect_err("too deep for the session's parser").to_string();
        assert!(err.to_lowercase().contains("recursion"), "{err}");
        // (and a session that parses as the looks do plans the same text from their parse, to the same plan)
        let generic = with(&|_| {});
        assert!(crate::parsed::generic(&sql).is_ok());
        let plan = planned(&generic, &sql).await.unwrap();
        assert_eq!(plan, generic.state().create_logical_plan(&sql).await.unwrap());
    }

    /// A tree the front rewrote is kept as the parse of the text it prints to (`text::text_kept`), so the engine plans it
    /// rather than parse that text: the same statements DataFusion's parse of the text gives (the one-parse check), and
    /// the same plan. A tree printed with a fix for the reader (a doubled minus) parses back to another tree: not kept.
    #[tokio::test]
    async fn a_tree_the_front_rewrote_is_planned_from_itself() {
        use datafusion::sql::sqlparser::{ast, dialect::GenericDialect, parser::Parser};
        let ctx = session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let batch = RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)]).unwrap();
        ctx.register_batch("t", batch).unwrap();
        let state = ctx.state();
        // (a rewrite as the front makes one: a FROM-first query given its `SELECT *`, a projection aliased)
        let mut stmts = Parser::parse_sql(&GenericDialect {}, "FROM t WHERE x > 1 -- kept").unwrap_or_else(|_| Parser::parse_sql(&GenericDialect {}, "SELECT * FROM t WHERE x > 1 -- kept").unwrap());
        let ast::Statement::Query(q) = &mut stmts[0] else { panic!("a query") };
        let ast::SetExpr::Select(s) = q.body.as_mut() else { panic!("a select") };
        s.projection = vec![ast::SelectItem::ExprWithAlias { expr: ast::Expr::Nested(Box::new(ast::Expr::BinaryOp { left: Box::new(ast::Expr::Identifier("x".into())), op: ast::BinaryOperator::Plus, right: Box::new(ast::Expr::Value(ast::Value::Number("1".into(), false).into())) })), alias: ast::Ident::with_quote('"', "x + 1") }];
        // (the kept texts are the last few of the process's looks, which other tests run beside this one: kept again until
        // it is the tree that is found)
        let (text, kept) = (0..20)
            .find_map(|_| {
                let text = crate::text::text_kept(stmts.clone());
                parsed_query(&text).map(|kept| (text, kept))
            })
            .expect("kept as its text's parse");
        assert_eq!(text, r#"SELECT (x + 1) AS "x + 1" FROM t WHERE x > 1"#);
        assert_eq!(kept, datafusion::sql::parser::Statement::Statement(Box::new(stmts[0].clone())));
        assert_eq!(Parser::parse_sql(&GenericDialect {}, &text).unwrap(), stmts, "it parses back to itself");
        datafusions_parse(&state, &text, &kept).unwrap();
        assert_eq!(planned(&ctx, &text).await.unwrap(), state.create_logical_plan(&text).await.unwrap());
        // (`- -3` is written `-(-3)`, which parses to another tree: the text is parsed again)
        let minus = ast::Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr: Box::new(ast::Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr: Box::new(ast::Expr::Value(ast::Value::Number("3".into(), false).into())) }) };
        let mut stmts = Parser::parse_sql(&GenericDialect {}, "SELECT 0 AS v FROM t -- minus").unwrap();
        let ast::Statement::Query(q) = &mut stmts[0] else { panic!("a query") };
        let ast::SetExpr::Select(s) = q.body.as_mut() else { panic!("a select") };
        s.projection = vec![ast::SelectItem::ExprWithAlias { expr: minus, alias: "v".into() }];
        let text = crate::text::text_kept(stmts);
        assert_eq!(text, "SELECT -(-3) AS v FROM t");
        assert!(crate::parsed::already(&text).is_none(), "not kept: {text}");
        assert_eq!(planned(&ctx, &text).await.unwrap(), state.create_logical_plan(&text).await.unwrap());
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
