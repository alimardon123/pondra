//! How SQL reaches a column type whose storage isn't its text (INET, FILE): its name as a cast's type (`Named`, a type planner:
//! `'10.0.0.1'::inet`, `CAST(x AS INET)`, `INET '10.0.0.1'`), a string compared with a value of one (`Compared`, an expression
//! planner, for `ip = '10.0.0.1'` wherever it is written), and the casts and the lists and ranges of strings that DataFusion
//! can't plan for its storage (`Casts`, an analyzer rule before type coercion). A string literal is read once, as the
//! statement is planned, and refused there in the type's words (22P02); any other text is read as it runs, by
//! `pondra_<type>(x)` (TRY_CAST: `pondra_<type>(x, true)`, NULL for what isn't one), and `x::text` is
//! `pondra_<type>_text(x)`. Every session has both functions for each such type (`register`), so a plan spread over the
//! nodes names them.
use super::{mark, named, of, ColumnType};
use datafusion::arrow::array::{new_null_array, Array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion::logical_expr::expr::{Cast, ScalarFunction, TryCast};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr, TypePlanner};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast;
use std::sync::Arc;

/// Every column type whose storage isn't text.
fn typed() -> impl Iterator<Item = &'static ColumnType> {
    crate::registry::get().find::<super::ColumnTypes>().into_iter().flat_map(|s| s.0.iter().copied()).filter(|t| t.parse.is_some())
}

/// The type a field is marked with, if its storage isn't text.
fn typed_field(f: &Field) -> Option<&'static ColumnType> { of(f).filter(|t| t.parse.is_some()) }

/// The type `e`'s values are over `schema`, if its storage isn't text.
fn typed_expr(e: &Expr, schema: &DFSchema) -> Option<&'static ColumnType> { e.to_field(schema).ok().and_then(|(_, f)| typed_field(&f)) }

fn is_text(t: &DataType) -> bool { matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) }

/// Each type's two functions, in a session: `pondra_<type>(text[, trying])` and `pondra_<type>_text(value)`.
pub fn register(ctx: &SessionContext) {
    for t in typed() {
        ctx.register_udf(read(t));
        ctx.register_udf(text(t));
    }
}

/// `pondra_<type>(text[, trying])`: text read as the type, its answer marked as one.
fn read(t: &'static ColumnType) -> ScalarUDF {
    let signature = Signature::one_of(vec![TypeSignature::Any(1), TypeSignature::Any(2)], Volatility::Immutable);
    ScalarUDF::new_from_impl(Of { name: format!("pondra_{}", t.name), of: t.name, text: false, signature })
}

/// `pondra_<type>_text(value)`: the type's values as their text.
fn text(t: &'static ColumnType) -> ScalarUDF {
    ScalarUDF::new_from_impl(Of { name: format!("pondra_{}_text", t.name), of: t.name, text: true, signature: Signature::any(1, Volatility::Immutable) })
}

/// One of a type's functions: into the type from text, or out of it (`text`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Of {
    name: String,
    of: &'static str,
    text: bool,
    signature: Signature,
}

impl Of {
    fn entry(&self) -> Result<&'static ColumnType> {
        named(self.of).ok_or_else(|| datafusion::error::DataFusionError::Internal(format!("{}: no column type {}", self.name, self.of)))
    }
}

impl ScalarUDFImpl for Of {
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(if self.text { DataType::Utf8 } else { (self.entry()?.storage)(&[]) }) }
    // (the mark rides on the field: a projection of the answer keeps it, and every door shows it as the type)
    fn return_field_from_args(&self, _: ReturnFieldArgs) -> Result<FieldRef> {
        let t = self.entry()?;
        Ok(Arc::new(match self.text {
            true => Field::new(&self.name, DataType::Utf8, true),
            false => mark(Field::new(&self.name, (t.storage)(&[]), true), t),
        }))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (t, n) = (self.entry()?, args.number_rows);
        let given = args.args[0].to_array(n)?;
        let out = match self.text {
            true => t.show.map(|show| show(&given)),
            false => t.parse.map(|_| into(t, &given, trying(&args.args))),
        };
        let out = out.ok_or_else(|| datafusion::error::DataFusionError::Internal(format!("{}: {} isn't read from text", self.name, t.name)))?;
        out.map(ColumnarValue::Array).map_err(crate::codes::for_datafusion)
    }
}

/// The second argument of `pondra_<type>(x, true)`: a TRY_CAST's, NULL for a value it refuses.
fn trying(args: &[ColumnarValue]) -> bool { matches!(args.get(1), Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))) }

/// `given` (text, or NULLs) read as `t`, or refused (42846 for a type that isn't text); trying, each value it refuses is NULL.
fn into(t: &ColumnType, given: &ArrayRef, trying: bool) -> anyhow::Result<ArrayRef> {
    let parse = t.parse.expect("a type read from text");
    if !is_text(given.data_type()) && !given.data_type().is_null() {
        return Err(crate::codes::coded("42846", format!("cannot cast type {} to {}", crate::objects::sql_type(&given.data_type().to_string()).to_lowercase(), t.name)));
    }
    let text = datafusion::arrow::compute::cast(given, &DataType::Utf8)?;
    match parse(&text) {
        Err(_) if trying => {
            let each = (0..text.len()).map(|i| parse(&text.slice(i, 1)).or_else(|_| Ok(new_null_array(&(t.storage)(&[]), 1)))).collect::<anyhow::Result<Vec<_>>>()?;
            Ok(datafusion::arrow::compute::concat(&each.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?)
        }
        read => read,
    }
}

/// A string literal read as `t` now, as a literal marked as one; Postgres refuses a literal that isn't one where it is
/// written, and so does this (22P02).
fn literal(t: &'static ColumnType, s: &str) -> Result<Expr> {
    let parse = t.parse.expect("a type read from text");
    let read = parse(&(Arc::new(datafusion::arrow::array::StringArray::from(vec![s])) as ArrayRef)).map_err(crate::codes::for_datafusion)?;
    let value = ScalarValue::try_from_array(&read, 0)?;
    let field = mark(Field::new("", value.data_type(), true), t);
    Ok(Expr::Literal(value, Some(FieldMetadata::from(&field))))
}

/// A string literal (or NULL) meeting a value of `t`, read as one; any other expression is left as it is.
fn as_typed(t: &'static ColumnType, e: &mut Expr) -> Result<bool> {
    match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => {
            *e = literal(t, s)?;
            Ok(true)
        }
        Expr::Literal(v, _) if v.is_null() && v.data_type() != (t.storage)(&[]) => {
            *e = Expr::Literal(ScalarValue::try_from(&(t.storage)(&[]))?, None);
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// A type's name as a cast writes it, `INET`, `FILE`: its storage, marked as the type, which `Casts` makes a read of the text.
#[derive(Debug)]
pub struct Named;

impl TypePlanner for Named {
    fn plan_type_field(&self, t: &ast::DataType) -> Result<Option<FieldRef>> {
        let ast::DataType::Custom(name, args) = t else { return Ok(None) };
        if !args.is_empty() || name.0.len() != 1 {
            return Ok(None);
        }
        let Some(t) = named(&name.to_string().to_lowercase()).filter(|t| t.parse.is_some()) else { return Ok(None) };
        Ok(Some(Arc::new(mark(Field::new("", (t.storage)(&[]), true), t))))
    }
}

/// A string compared with a value of a type whose storage isn't text, read as one as the statement is planned: a projection's
/// comparison is typed as it is planned, so the analyzer would come too late for it.
#[derive(Debug)]
pub struct Compared;

impl ExprPlanner for Compared {
    fn plan_binary_op(&self, mut e: RawBinaryExpr, schema: &DFSchema) -> Result<PlannerResult<RawBinaryExpr>> {
        use ast::BinaryOperator as B;
        if matches!(e.op, B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq | B::Spaceship) {
            compared(&mut e.left, &mut e.right, schema)?;
        }
        Ok(PlannerResult::Original(e))
    }
}

/// Of two sides compared, a string meeting a value of a type whose storage isn't text, read as one.
fn compared(left: &mut Expr, right: &mut Expr, schema: &DFSchema) -> Result<bool> {
    let literal = |e: &Expr| matches!(e, Expr::Literal(..));
    match (literal(left), literal(right)) {
        (false, true) => match typed_expr(left, schema) {
            Some(t) => as_typed(t, right),
            None => Ok(false),
        },
        (true, false) => match typed_expr(right, schema) {
            Some(t) => as_typed(t, left),
            None => Ok(false),
        },
        _ => Ok(false),
    }
}

/// What DataFusion can't plan for a type whose storage isn't text, made what it can, before type coercion: a cast to the type
/// (a read of its text: `pondra_<type>`), a cast of a value of one to text (`pondra_<type>_text`), and the strings an IN list,
/// a BETWEEN or a comparison sets beside a value of one (read as the type). A plan with no such type in it is handed back as
/// it came.
#[derive(Debug)]
pub struct Casts;

impl AnalyzerRule for Casts {
    fn name(&self) -> &str { "pondra_column_type_casts" }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        if !touches(&plan)? {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(|plan| {
            let schema = input_schema(&plan);
            let names = NamePreserver::new(&plan);
            plan.map_expressions(|e| {
                let name = names.save(&e);
                let e = e.transform_up(|e| rewrite(e, &schema))?;
                Ok(e.update_data(|e| name.restore(e)))
            })
        })
        .data()
    }
}

/// Whether a plan has a value of a type whose storage isn't text in it: a column of one in a node's schema, or a cast to one.
fn touches(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|p| {
        found = p.schema().fields().iter().any(|f| typed_field(f).is_some());
        if !found {
            p.apply_expressions(|e| {
                found = e.exists(|x| Ok(matches!(x, Expr::Cast(Cast { field, .. }) | Expr::TryCast(TryCast { field, .. }) if typed_field(field).is_some())))?;
                Ok(if found { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
            })?;
        }
        Ok(if found { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
    })?;
    Ok(found)
}

/// The schema a plan node's expressions are worked out over: its inputs' (a join's both sides), or its own.
fn input_schema(plan: &LogicalPlan) -> DFSchemaRef {
    match plan.inputs().as_slice() {
        [] => plan.schema().clone(),
        [one] => one.schema().clone(),
        [first, rest @ ..] => {
            let mut schema = first.schema().as_ref().clone();
            for other in rest {
                schema.merge(other.schema());
            }
            Arc::new(schema)
        }
    }
}

fn rewrite(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    Ok(match e {
        Expr::Cast(Cast { expr, field }) => cast(*expr, field, false, schema)?,
        Expr::TryCast(TryCast { expr, field }) => cast(*expr, field, true, schema)?,
        Expr::InList(mut l) => match typed_expr(&l.expr, schema) {
            Some(t) => {
                let mut any = false;
                for x in l.list.iter_mut() {
                    any |= as_typed(t, x)?;
                }
                Transformed::new_transformed(Expr::InList(l), any)
            }
            None => Transformed::no(Expr::InList(l)),
        },
        Expr::Between(mut b) => match typed_expr(&b.expr, schema) {
            Some(t) => {
                let any = as_typed(t, &mut b.low)? | as_typed(t, &mut b.high)?;
                Transformed::new_transformed(Expr::Between(b), any)
            }
            None => Transformed::no(Expr::Between(b)),
        },
        Expr::BinaryExpr(mut b) if matches!(b.op, Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq | Operator::IsDistinctFrom | Operator::IsNotDistinctFrom) => {
            let any = compared(&mut b.left, &mut b.right, schema)?;
            Transformed::new_transformed(Expr::BinaryExpr(b), any)
        }
        e => Transformed::no(e),
    })
}

/// A cast to a type whose storage isn't text, or of a value of one to text, as the functions that do it; any other cast as it was.
fn cast(expr: Expr, field: FieldRef, trying: bool, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let given = typed_expr(&expr, schema);
    let call = |f: ScalarUDF, args: Vec<Expr>| Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(f), args));
    if let Some(t) = typed_field(&field) {
        return Ok(Transformed::yes(match (given, expr) {
            (Some(g), expr) if g.name == t.name => expr,
            (_, Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _)) if !trying => literal(t, &s)?,
            (_, expr) if trying => call(read(t), vec![expr, Expr::Literal(ScalarValue::Boolean(Some(true)), None)]),
            (_, expr) => call(read(t), vec![expr]),
        }));
    }
    let undone = |expr: Expr| match trying {
        true => Expr::TryCast(TryCast::new_from_field(Box::new(expr), field.clone())),
        false => Expr::Cast(Cast::new_from_field(Box::new(expr), field.clone())),
    };
    Ok(match given {
        Some(t) if is_text(field.data_type()) && t.show.is_some() => {
            let shown = call(text(t), vec![expr]);
            Transformed::yes(if field.data_type() == &DataType::Utf8 { shown } else { undone(shown) })
        }
        _ => Transformed::no(undone(expr)),
    })
}
