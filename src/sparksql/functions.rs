//! Spark's functions in every session (the `spark` extension's function set), beside the Spark SQL it reads
//! (`sparksql.rs`).
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Spark's functions whose names DataFusion doesn't have (`format_string`, `pmod`, `parse_url`,
/// `sha2`, `collect_list`, …: PySpark's vocabulary in SQL), in every session, as an extension. Where both have a name,
/// DataFusion's stays, so no answer changes, and Spark's is there under a name of its own (`spark_floor`):
/// `spark_sql('…')` calls it where Spark SQL names `floor`. First in the build's list (`crate::built_in`), so where a
/// function of Pondra's has a name only Spark's has too, Pondra's is added after it and stays.
pub static SPARK: crate::registry::BuiltIn = crate::registry::BuiltIn {
    name: "spark",
    requires: &[],
    register: |r| {
        r.section_mut::<crate::engine::Functions>().add("spark", add);
        r.section_mut::<crate::passes::Passes>().add(crate::sparksql::pass());
    },
};

fn add(ctx: SessionContext) -> SessionContext {
    let more = spark_functions();
    more.scalar.iter().chain(&more.shared).for_each(|f| { ctx.register_udf(f.clone()); });
    more.aggregate.iter().for_each(|f| { ctx.register_udaf(f.clone()); });
    ctx
}

pub struct Spark {
    scalar: Vec<datafusion::logical_expr::ScalarUDF>,
    aggregate: Vec<datafusion::logical_expr::AggregateUDF>,
    shared: Vec<datafusion::logical_expr::ScalarUDF>, // (Spark's of the names both have, renamed)
    pub names: std::collections::HashMap<String, String>, // a name both have (or its alias) → Spark's here
}

static FUNCTIONS: std::sync::OnceLock<Spark> = std::sync::OnceLock::new();

/// Spark's functions as every session has them, worked out once against DataFusion's names alone (a session with
/// nothing added), wherever Spark's set is in the build's list.
pub fn spark_functions() -> &'static Spark {
    FUNCTIONS.get_or_init(|| spark_of(&SessionContext::new()))
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
