//! The casts a statement's text gets for uuid, json, jsonb and enums, and the functions they call. Every form builds
//! it, so `engine::session` can register those functions in every session (a page's too).

use crate::pg_json::json_of;
use crate::text::function;
use anyhow::Result;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::sql::sqlparser::ast::{self, Statement, VisitorMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use std::collections::HashMap;
use std::ops::ControlFlow;

pub(crate) fn quoted(labels: &[String]) -> String { labels.iter().map(|l| format!("'{}'", l.replace('\'', "''"))).collect::<Vec<_>>().join(", ") }

/// Any spelling of a uuid Postgres takes (upper case, braces, no hyphens), written the one way;
/// refused as Postgres refuses it (22P02).
pub fn uuid_of(s: &str) -> Result<String> {
    match uuid::Uuid::try_parse(s) {
        Ok(u) => Ok(u.hyphenated().to_string()),
        Err(_) => Err(crate::codes::coded("22P02", format!("invalid input syntax for type uuid: \"{s}\""))),
    }
}

pub(crate) struct Casts {
    pub(crate) types: HashMap<String, Vec<String>>, // the names found and their enums' labels, by their name as written (`text::object`)
    pub(crate) named: Vec<String>,
    pub(crate) labelled: Vec<String>, // the types `enum_range` and its kin name, which must be there
    pub(crate) typed: bool,         // a cast to uuid, json or jsonb was found
    pub(crate) collecting: bool,    // the first pass, which only finds what is named
    pub(crate) failed: Option<anyhow::Error>,
}

impl Casts {
    fn custom(t: &ast::DataType) -> Option<String> {
        match t {
            ast::DataType::Custom(name, args) if args.is_empty() => Some(crate::text::object(name)),
            _ => None,
        }
    }

    /// `NULL::t` or `CAST(NULL AS t)`: the type it names.
    fn null_of(e: &ast::Expr) -> Option<String> {
        match e {
            ast::Expr::Cast { data_type, .. } => Self::custom(data_type),
            _ => None,
        }
    }
}

impl VisitorMut for Casts {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &mut ast::Expr) -> ControlFlow<()> {
        if self.failed.is_some() {
            return ControlFlow::Break(());
        }
        let collecting = self.collecting;
        if let ast::Expr::Cast { data_type, expr, kind, .. } = e {
            if let Some(to) = held_cast(data_type) {
                if collecting {
                    self.typed = true;
                    return ControlFlow::Continue(());
                }
                let trying = matches!(kind, ast::CastKind::TryCast | ast::CastKind::SafeCast);
                let literal = match expr.as_ref() {
                    ast::Expr::Value(ast::ValueWithSpan { value: ast::Value::SingleQuotedString(text), .. }) => Some(text.clone()),
                    _ => None,
                };
                let written = match literal.map(|text| one_way(to, &text)) {
                    // (a json literal stays a call of its type's function: a projection names `'{}'::json` and
                    // `'{}'::jsonb` apart, as Postgres does, where two equal literals would clash)
                    Some(Ok(v)) if to == "uuid" => ast::Expr::value(ast::Value::SingleQuotedString(v)),
                    Some(Ok(v)) => function(&format!("pondra_{to}"), vec![ast::Expr::value(ast::Value::SingleQuotedString(v))]),
                    Some(Err(_)) if trying => ast::Expr::value(ast::Value::Null),
                    Some(Err(err)) => {
                        self.failed = Some(err);
                        return ControlFlow::Break(());
                    }
                    None => {
                        let mut args = vec![std::mem::replace(expr.as_mut(), ast::Expr::value(ast::Value::Null))];
                        if trying {
                            args.push(ast::Expr::value(ast::Value::Boolean(true)));
                        }
                        function(&format!("pondra_{to}"), args)
                    }
                };
                *e = written;
                return ControlFlow::Continue(());
            }
        }
        if let ast::Expr::Function(f) = e {
            let word = f.name.to_string().to_lowercase();
            if matches!(word.as_str(), "enum_range" | "enum_first" | "enum_last") {
                let ast::FunctionArguments::List(args) = &f.args else { return ControlFlow::Continue(()) };
                let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))] = &args.args[..] else { return ControlFlow::Continue(()) };
                let Some(name) = Self::null_of(arg) else { return ControlFlow::Continue(()) };
                if collecting {
                    self.labelled.push(name.clone());
                    self.named.push(name);
                    return ControlFlow::Continue(());
                }
                let Some(labels) = self.types.get(&name) else { return ControlFlow::Continue(()) };
                let text = match word.as_str() {
                    "enum_range" => format!("make_array({})", quoted(labels)),
                    "enum_first" => quoted(&labels[..1]),
                    _ => quoted(&labels[labels.len() - 1..]),
                };
                *e = Parser::new(&GenericDialect {}).try_with_sql(&text).and_then(|mut p| p.parse_expr()).expect("labels as SQL");
                return ControlFlow::Continue(());
            }
        }
        if let ast::Expr::Cast { data_type, expr, .. } = e {
            let Some(name) = Self::custom(data_type) else { return ControlFlow::Continue(()) };
            if collecting {
                self.named.push(name);
                return ControlFlow::Continue(());
            }
            let Some(labels) = self.types.get(&name) else { return ControlFlow::Continue(()) };
            if let ast::Expr::Value(v) = expr.as_ref() {
                if let ast::Value::SingleQuotedString(s) = &v.value {
                    if !labels.contains(s) {
                        self.failed = Some(crate::codes::coded("22P02", format!("invalid input value for enum {name}: \"{s}\" (its labels: {})", quoted(labels))));
                        return ControlFlow::Break(());
                    }
                }
            }
            *data_type = ast::DataType::Varchar(None);
        }
        ControlFlow::Continue(())
    }
}

/// Where SQL comes in: a cast to an enum type is a cast to text (a literal one of its labels, or
/// refused as Postgres refuses it), and `enum_range(NULL::t)`, `enum_first`, `enum_last` its
/// labels. A cast to `uuid` is the uuid written the one way a column holds it: a literal at once
/// (`'A0EE…'::uuid` is `'a0ee…'`), anything else by `pondra_uuid(x)` (`TRY_CAST`: `pondra_uuid(x,
/// true)`, NULL for what isn't one). A string compared with a uuid or written into one is read as one, and
/// `pg_typeof(x)` of one is 'uuid' (`pg_trace::rewrite`), before the casts it may read are written out.
/// Whether anything changed.
pub(crate) async fn rewrite<H: crate::front::Host>(host: &H, stmts: &mut [Statement]) -> Result<bool> {
    let mut read = false;
    for s in stmts.iter_mut() {
        // (a bare name of a JSON call's rows is its value unless a relation has a column of that name: the catalog is asked
        // only for a statement with such a name)
        let columns = match crate::pg_json_sets::bare_aliases(s) {
            true => crate::pg_trace::columns_named(host, s).await,
            false => None,
        };
        read |= crate::pg_json_sets::sets(s, columns.as_ref())?; // (set-returning JSON functions: rows of a document, before the casts they may read)
        read |= crate::pg_trace::rewrite(host, s).await?; // (strings read as uuids, pg_typeof, JSON makers' arguments)
    }
    for s in stmts.iter_mut() {
        read |= crate::lateral::functions(s)?; // (`generate_series(1, t.n)` in a FROM: a LATERAL subquery it can plan)
    }
    for s in stmts.iter_mut() {
        read |= crate::pg_text::array_literals(s)?; // (a string cast to an array type reads as Postgres reads an array literal)
    }
    let mut f = Casts { types: HashMap::new(), named: vec![], labelled: vec![], typed: false, collecting: true, failed: None };
    for s in stmts.iter_mut() {
        let _ = crate::walk::visit_mut(s, &mut f); // (the first pass only collects the names)
    }
    if f.named.is_empty() && !f.typed {
        return Ok(read);
    }
    for n in std::mem::take(&mut f.named) {
        if let Some(labels) = host.enum_labels(&n).await {
            f.types.insert(n, labels);
        }
    }
    if let Some(n) = f.labelled.iter().find(|n| !f.types.contains_key(*n)) {
        return Err(crate::codes::coded("42704", format!("type \"{n}\" does not exist"))); // (not DataFusion's "Invalid function 'enum_range'")
    }
    if f.types.is_empty() && !f.typed {
        return Ok(read);
    }
    f.collecting = false;
    for s in stmts.iter_mut() {
        let _ = crate::walk::visit_mut(s, &mut f);
    }
    match f.failed {
        Some(e) => Err(e),
        None => Ok(true),
    }
}

/// The type a cast writes its value as, when the lake holds the value as text that is checked: "uuid",
/// "json" or "jsonb" (the function that writes it is `pondra_{type}`).
fn held_cast(t: &ast::DataType) -> Option<&'static str> {
    match t {
        ast::DataType::Uuid => Some("uuid"),
        ast::DataType::JSON => Some("json"),
        ast::DataType::JSONB => Some("jsonb"),
        _ => None,
    }
}

/// `text` written as a value of `to` ("uuid", "json" or "jsonb"): the one way the lake holds it, or Postgres's refusal.
fn one_way(to: &str, text: &str) -> Result<String> {
    match to {
        "uuid" => uuid_of(text),
        _ => json_of(text),
    }
}

/// The functions every session has for these types: `gen_random_uuid()` and `uuidv4()` (DataFusion's
/// `uuid()` by Postgres's names), `uuidv7()`, `pondra_uuid(text [, try])`, `pondra_json(text [, try])` and
/// `pondra_jsonb(text [, try])` (a cast to uuid, json or jsonb: `rewrite`) and `pg_typeof(x)`.
pub fn register(ctx: &datafusion::prelude::SessionContext) {
    use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
    let uuid = datafusion::functions::string::uuid();
    ctx.register_udf(uuid.as_ref().clone().with_aliases(["gen_random_uuid", "uuidv4"]));
    ctx.register_udf(made("uuidv7", Signature::nullary(Volatility::Volatile), |_, n| {
        Ok(std::sync::Arc::new(datafusion::arrow::array::StringArray::from_iter_values((0..n).map(|_| uuid::Uuid::now_v7().hyphenated().to_string()))))
    }));
    let text = || TypeSignature::String(1);
    let trying = || TypeSignature::Exact(vec![DataType::Utf8, DataType::Boolean]);
    // (its answers are marked uuid, so the Postgres port sends a uuid result as Postgres's uuid, whatever query it is in)
    ctx.register_udf(made_marked("pondra_uuid", Signature::one_of(vec![text(), trying()], Volatility::Immutable), "uuid", |a, _| {
        let text = datafusion::arrow::compute::cast(&a[0], &DataType::Utf8)?;
        let out = text.as_string::<i32>().iter().map(|v| match v.map(uuid_of) {
            Some(Ok(u)) => Ok(Some(u)),
            Some(Err(_)) if is_trying(a) => Ok(None),
            Some(Err(e)) => Err(datafusion::error::DataFusionError::Execution(e.to_string())),
            None => Ok(None),
        }).collect::<datafusion::error::Result<datafusion::arrow::array::StringArray>>()?;
        Ok(std::sync::Arc::new(out))
    }));
    // (`pondra_jsonb` is its own function, so a projection names a cast to each apart; its text is jsonb as Postgres prints it)
    let cast_sig = || Signature::one_of(vec![text(), trying()], Volatility::Immutable);
    ctx.register_udf(made_marked("pondra_json", cast_sig(), "json", |a, _| cast_each(a, json_text)));
    ctx.register_udf(made_marked("pondra_jsonb", cast_sig(), "jsonb", |a, _| cast_each(a, crate::pg_json_make::jsonb_text)));
    ctx.register_udf(made("pg_typeof", Signature::any(1, Volatility::Immutable), |a, n| {
        let name = crate::pg_oids::type_of(a[0].data_type());
        Ok(std::sync::Arc::new(datafusion::arrow::array::StringArray::from_iter_values((0..n).map(|_| name.clone()))))
    }));
}

type Body = fn(&[datafusion::arrow::array::ArrayRef], usize) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef>;

/// A function of `register`'s: text out, its rows' count known (a function of no arguments needs it). `mark` is the
/// type its answers are ("json", "jsonb" or "uuid"), if the column they make should say so.
#[derive(Debug)]
struct Made {
    name: &'static str,
    signature: datafusion::logical_expr::Signature,
    mark: Option<&'static str>,
    body: Body,
}

// (one of each name: its name says which, never its code's address)
impl PartialEq for Made {
    fn eq(&self, other: &Self) -> bool { self.name == other.name }
}

impl Eq for Made {}

impl std::hash::Hash for Made {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.name.hash(h) }
}

impl datafusion::logical_expr::ScalarUDFImpl for Made {
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &datafusion::logical_expr::Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> { Ok(DataType::Utf8) }
    // (the mark rides on the field, so a projection of the answer keeps it and the port sends it as that type)
    fn return_field_from_args(&self, _: datafusion::logical_expr::ReturnFieldArgs) -> datafusion::error::Result<datafusion::arrow::datatypes::FieldRef> {
        let field = datafusion::arrow::datatypes::Field::new(self.name, DataType::Utf8, true);
        Ok(std::sync::Arc::new(match self.mark {
            Some(mark) => field.with_metadata([(crate::pg_functions::MARK.to_string(), mark.to_string())].into()),
            None => field,
        }))
    }
    fn invoke_with_args(&self, args: datafusion::logical_expr::ScalarFunctionArgs) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        let n = args.number_rows;
        let arrays = args.args.iter().map(|a| a.to_array(n)).collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(datafusion::logical_expr::ColumnarValue::Array((self.body)(&arrays, n)?))
    }
}

fn made(name: &'static str, signature: datafusion::logical_expr::Signature, body: Body) -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(Made { name, signature, mark: None, body })
}

/// `made`, whose answers are text of the type `mark` names ("json", "jsonb" or "uuid").
fn made_marked(name: &'static str, signature: datafusion::logical_expr::Signature, mark: &'static str, body: Body) -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(Made { name, signature, mark: Some(mark), body })
}

/// The second argument of `pondra_uuid(x, true)` and `pondra_json(x, true)`: a TRY_CAST, which answers
/// NULL for a value it refuses instead of failing.
fn is_trying(a: &[datafusion::arrow::array::ArrayRef]) -> bool { a.get(1).is_some_and(|t| t.as_boolean_opt().is_some_and(|t| t.is_valid(0) && t.value(0))) }

/// `s` as the json text the lake keeps (`json_of`), its refusal a query error with its SQLSTATE (22P02).
fn json_text(s: &str) -> datafusion::error::Result<String> {
    json_of(s).map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
}

/// A cast of each text to json or jsonb: `of` answers each value, or refuses it with its own error. A `TRY_CAST`
/// answers NULL for a refusal instead.
fn cast_each(a: &[datafusion::arrow::array::ArrayRef], of: fn(&str) -> datafusion::error::Result<String>) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
    let text = datafusion::arrow::compute::cast(&a[0], &DataType::Utf8)?;
    let out = text.as_string::<i32>().iter().map(|v| match v.map(of) {
        Some(Ok(j)) => Ok(Some(j)),
        Some(Err(_)) if is_trying(a) => Ok(None),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }).collect::<datafusion::error::Result<datafusion::arrow::array::StringArray>>()?;
    Ok(std::sync::Arc::new(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one text `sql` answers, through the functions `register` makes.
    async fn one(sql: &str) -> String {
        let ctx = datafusion::prelude::SessionContext::new();
        register(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0].column(0).as_string::<i32>().value(0).to_string()
    }

    #[tokio::test]
    async fn jsonb_is_written_as_postgres_prints_it_and_json_as_it_was_written() {
        assert_eq!(one(r#"SELECT pondra_jsonb('{"b":1,"a":2}')"#).await, r#"{"a": 2, "b": 1}"#);
        assert_eq!(one(r#"SELECT pondra_json('{"b":1,"a":2}')"#).await, r#"{"b":1,"a":2}"#);
    }
}
