//! Spark SQL where PySpark code sends it (H8, round 31): `spark_sql('…')` in a FROM is that query
//! read with Spark's grammar and turned into Pondra's SQL where SQL comes in (`inline`, from
//! `routines::expand` and `routines::bind`), so what is built on it (a frame's steps, a join, a
//! view) is Pondra's SQL as everywhere else. What it turns: `"text"` (a string in Spark),
//! `a DIV b`, `<=>`, `!`, `RLIKE`, `LATERAL VIEW [OUTER] explode(…)` and `explode` itself, and the
//! functions both have with Spark's answers (`floor` a BIGINT, `substring`, `round`, …): Spark's
//! own, registered in every session as `spark_floor`, … (`store::spark`), so DataFusion's answers
//! never change outside it (invariant 190). Backticks are names in Pondra's SQL too.
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{ColumnarValue, Documentation, Expr as DfExpr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature};
use datafusion::sql::sqlparser::ast::{self, BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, TableFactor, UnaryOperator, VisitMut, VisitorMut};
use datafusion::sql::sqlparser::dialect::{GenericDialect, SparkSqlDialect};
use datafusion::sql::sqlparser::parser::Parser;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

/// Does this SQL name `spark_sql`? (A word match: `inline` parses only what does.)
pub fn mentions(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("spark_sql")
}

/// Every `spark_sql('…')` in a FROM, replaced by its query turned into Pondra's SQL.
pub fn inline(sql: &str) -> Result<String> {
    if !mentions(sql) {
        return Ok(sql.to_string());
    }
    let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return Ok(sql.to_string()) }; // (its own error later, as written)
    let mut found = Found(false);
    if let ControlFlow::Break(e) = stmts.visit(&mut found) {
        return Err(e);
    }
    Ok(match found.0 {
        true => stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(";\n"),
        false => sql.to_string(),
    })
}

struct Found(bool);

impl VisitorMut for Found {
    type Break = anyhow::Error;

    /// `SELECT * FROM spark_sql('…')` alone is that query itself, so its ORDER BY holds (a
    /// subquery's sort is dropped, as SQL does, and Spark may sort by a column it leaves out).
    fn pre_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<Self::Break> {
        let SetExpr::Select(s) = q.body.as_ref() else { return ControlFlow::Continue(()) };
        let [from] = &s.from[..] else { return ControlFlow::Continue(()) };
        if !from.joins.is_empty() || q.to_string() != format!("SELECT * FROM {}", from.relation) {
            return ControlFlow::Continue(());
        }
        match spark_text(&from.relation).map(|t| t.and_then(|t| query(&t))) {
            Some(Ok(to)) => {
                *q = to;
                self.0 = true;
                ControlFlow::Continue(())
            }
            Some(Err(e)) => ControlFlow::Break(e),
            None => ControlFlow::Continue(()),
        }
    }

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
        match spark_text(t).map(|t| t.and_then(|t| query(&t))) {
            Some(Ok(q)) => {
                let TableFactor::Table { alias, .. } = t else { unreachable!() };
                let alias = alias.clone().or_else(|| Some(ast::TableAlias { explicit: true, name: ast::Ident::new("spark_sql"), columns: vec![], at: None }));
                *t = TableFactor::Derived { lateral: false, subquery: Box::new(q), alias, sample: None };
                self.0 = true;
                ControlFlow::Continue(())
            }
            Some(Err(e)) => ControlFlow::Break(e),
            None => ControlFlow::Continue(()),
        }
    }
}

/// The query of a `spark_sql('…')` in a FROM (None: another table).
fn spark_text(t: &TableFactor) -> Option<Result<String>> {
    let TableFactor::Table { name, args: Some(args), .. } = t else { return None };
    if crate::write::object(name) != "spark_sql" {
        return None;
    }
    Some(match &args.args[..] {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))] => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DollarQuotedString(ast::DollarQuotedString { value: s, .. }) => Ok(s.clone()),
            _ => Err(anyhow::anyhow!("spark_sql takes the query as a string: spark_sql('SELECT …')")),
        },
        _ => Err(anyhow::anyhow!("spark_sql takes the query as a string: spark_sql('SELECT …')")),
    })
}

/// A Spark SQL query as Pondra's.
pub fn query(text: &str) -> Result<ast::Query> {
    let mut stmts = Parser::parse_sql(&SparkSqlDialect {}, text).context("Spark SQL")?;
    ensure!(stmts.len() == 1, "spark_sql takes one query, not {}", stmts.len());
    let ast::Statement::Query(mut q) = stmts.remove(0) else { bail!("spark_sql takes a query (SELECT, WITH, VALUES): other statements are Pondra's SQL") };
    let mut t = Turn { names: &crate::store::spark_functions().names };
    if let ControlFlow::Break(e) = q.visit(&mut t) {
        return Err(e);
    }
    Ok(*q)
}

struct Turn<'a> {
    names: &'a HashMap<String, String>, // Spark's name (or alias) → the name its own version has here
}

impl VisitorMut for Turn<'_> {
    type Break = anyhow::Error;

    /// Before what is in it: its lateral views become what DataFusion runs (`unnest` in a select
    /// over the FROM), and a column a translation renames keeps the name Spark gives it.
    fn pre_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<Self::Break> {
        let mut views = HashMap::new();
        if let Err(e) = selects(&mut q.body, &mut |s| lateral(s, &mut views)) {
            return ControlFlow::Break(e);
        }
        if !views.is_empty() {
            let _ = ast::visit_expressions_mut(q, |e| {
                if let Expr::CompoundIdentifier(parts) = e {
                    if let [view, _] = &mut parts[..] {
                        if let Some(to) = views.get(&view.value.to_lowercase()) {
                            *view = ast::Ident::new(to); // (`v.x`, the lateral view's column: the FROM's now)
                        }
                    }
                }
                ControlFlow::<()>::Continue(())
            });
        }
        let names = self.names;
        let _ = selects(&mut q.body, &mut |s| {
            for item in s.projection.iter_mut() {
                let SelectItem::UnnamedExpr(e) = item else { continue };
                let was = e.to_string();
                let mut now = e.clone();
                if let ControlFlow::Break(_) = now.visit(&mut Turn { names }) {
                    continue; // (said when the walk reaches it)
                }
                if now.to_string() == was || matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                    continue;
                }
                let name = match &*e {
                    Expr::Value(v) => v.value.clone().into_string().unwrap_or(was), // (a literal: named its value, as Spark names it)
                    Expr::Function(f) if ["explode", "explode_outer"].contains(&crate::write::object(&f.name).as_str()) => "col".into(),
                    _ => was,
                };
                *item = SelectItem::ExprWithAlias { expr: now, alias: ast::Ident::with_quote('"', name) };
            }
            Ok(())
        });
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<Self::Break> {
        match turn(e, self.names) {
            Ok(Some(to)) => *e = to,
            Ok(None) => {}
            Err(err) => return ControlFlow::Break(err),
        }
        ControlFlow::Continue(())
    }
}

/// One expression, its parts turned already: Spark's meaning as Pondra's SQL (None: as it is).
fn turn(e: &mut Expr, names: &HashMap<String, String>) -> Result<Option<Expr>> {
    let spark = |name: &str| names.get(name).cloned();
    Ok(match e {
        Expr::Value(v) => {
            if let ast::Value::DoubleQuotedString(s) = &v.value {
                v.value = ast::Value::SingleQuotedString(s.clone()); // ("text" is a string in Spark)
            }
            None
        }
        Expr::UnaryOp { op, .. } if *op == UnaryOperator::BangNot => {
            *op = UnaryOperator::Not;
            None
        }
        Expr::BinaryOp { left, op: BinaryOperator::MyIntegerDivide, right } => Some(expr(&format!("CAST(({left}) / ({right}) AS BIGINT)"))?), // (DIV: a whole number, toward zero)
        Expr::BinaryOp { left, op: BinaryOperator::Spaceship, right } => Some(expr(&format!("({left}) IS NOT DISTINCT FROM ({right})"))?),
        Expr::RLike { negated, expr: x, pattern, .. } => Some(expr(&format!("{}regexp_like({x}, {pattern})", if *negated { "NOT " } else { "" }))?),
        Expr::Substring { expr: x, substring_from, substring_for, .. } => match spark("substring") {
            Some(f) => {
                let from = substring_from.as_ref().map_or("1".to_string(), |f| f.to_string());
                Some(expr(&format!("{f}({x}, {from}{})", substring_for.as_ref().map_or(String::new(), |n| format!(", {n}"))))?)
            }
            None => None,
        },
        Expr::Extract { field, expr: x, .. } => spark("date_part").map(|f| expr(&format!("{f}('{field}', {x})"))).transpose()?,
        // (the parser reads `floor(x)` and `ceil(x)` as expressions of their own, not calls)
        Expr::Floor { expr: x, field: ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime) } => spark("floor").map(|f| expr(&format!("{f}({x})"))).transpose()?,
        Expr::Ceil { expr: x, field: ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime) } => spark("ceil").map(|f| expr(&format!("{f}({x})"))).transpose()?,
        Expr::Function(f) if f.name.0.len() == 1 => {
            let name = crate::write::object(&f.name);
            let args = match &f.args {
                FunctionArguments::List(l) => l.args.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                _ => vec![],
            };
            match (name.as_str(), &args[..]) {
                ("explode", [a]) => Some(expr(&format!("unnest({a})"))?), // (a row for each of an array's values; none for an empty or NULL one)
                ("explode_outer", [a]) => Some(expr(&format!("unnest(CASE WHEN cardinality({a}) > 0 THEN {a} ELSE make_array(NULL) END)"))?), // (…and a NULL for those)
                ("explode" | "explode_outer" | "posexplode" | "posexplode_outer" | "inline" | "stack", _) => bail!("{name}({}): Pondra takes explode and explode_outer of an array", args.join(", ")),
                _ => {
                    if let Some(to) = spark(&name) {
                        f.name = ast::ObjectName::from(vec![ast::Ident::new(to)]);
                    }
                    None
                }
            }
        }
        _ => None,
    })
}

fn expr(sql: &str) -> Result<Expr> {
    Ok(Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_expr()?)
}

/// Each SELECT of a query's body (through UNION and the like, not into subqueries).
fn selects(body: &mut SetExpr, f: &mut dyn FnMut(&mut ast::Select) -> Result<()>) -> Result<()> {
    match body {
        SetExpr::Select(s) => f(s),
        SetExpr::SetOperation { left, right, .. } => {
            selects(left, f)?;
            selects(right, f)
        }
        SetExpr::Query(q) => selects(&mut q.body, f),
        _ => Ok(()),
    }
}

/// `FROM t LATERAL VIEW [OUTER] explode(e) v AS x`: `FROM (SELECT t.*, explode(e) AS x FROM t) t`,
/// a view after another nested in it (two `unnest`s in one select would pair their values, not
/// cross them). `views` gets each view's name → the FROM's, for its columns' qualifiers.
fn lateral(s: &mut ast::Select, views: &mut HashMap<String, String>) -> Result<()> {
    if s.lateral_views.is_empty() {
        return Ok(());
    }
    ensure!(s.from.len() == 1 && s.from[0].joins.is_empty(), "LATERAL VIEW over a join: join in a subquery, then LATERAL VIEW over it");
    let rel = &mut s.from[0].relation;
    let alias = match rel {
        TableFactor::Table { alias: Some(a), .. } | TableFactor::Derived { alias: Some(a), .. } => a.name.clone(),
        TableFactor::Table { name, alias: None, .. } => name.0.last().and_then(|p| p.as_ident()).cloned().context("a table's name")?,
        _ => bail!("LATERAL VIEW over this FROM: name it (… AS t)"),
    };
    for v in std::mem::take(&mut s.lateral_views) {
        let Expr::Function(f) = &v.lateral_view else { bail!("LATERAL VIEW {}: a generator function, explode(…)", v.lateral_view) };
        let name = crate::write::object(&f.name);
        ensure!(name == "explode" || name == "explode_outer", "LATERAL VIEW {name}: Pondra takes explode and explode_outer of an array");
        let [col] = &v.lateral_col_alias[..] else { bail!("LATERAL VIEW {} {} AS …: one column (a map's key and value: not yet)", v.lateral_view, v.lateral_view_name) };
        views.insert(v.lateral_view_name.to_string().to_lowercase(), alias.value.clone());
        let mut call = v.lateral_view.clone();
        if v.outer {
            if let Expr::Function(f) = &mut call {
                f.name = ast::ObjectName::from(vec![ast::Ident::new("explode_outer")]);
            }
        }
        let _ = ast::visit_expressions_mut(&mut call, |e| {
            if let Expr::CompoundIdentifier(parts) = e {
                if let [view, _] = &mut parts[..] {
                    if let Some(to) = views.get(&view.value.to_lowercase()) {
                        *view = ast::Ident::new(to); // (an earlier view's column)
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
        let mut inner = *Parser::new(&SparkSqlDialect {}).try_with_sql(&format!("SELECT {alias}.*, {call} AS {col} FROM t"))?.parse_query()?;
        if let SetExpr::Select(sel) = inner.body.as_mut() {
            sel.from[0].relation = rel.clone();
        }
        *rel = TableFactor::Derived { lateral: false, subquery: Box::new(inner), alias: Some(ast::TableAlias { explicit: true, name: alias.clone(), columns: vec![], at: None }), sample: None };
    }
    Ok(())
}

/// Spark's version of a function DataFusion has too, under a name of its own (`spark_floor`):
/// everything as Spark's, but the name.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Renamed {
    name: String,
    inner: Arc<ScalarUDF>,
}

pub fn renamed(f: &Arc<ScalarUDF>) -> ScalarUDF {
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
    fn simplify(&self, args: Vec<DfExpr>, info: &SimplifyContext) -> datafusion::common::Result<ExprSimplifyResult> {
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

#[cfg(test)]
mod tests {
    #[test]
    fn spark_becomes_pondra() {
        let q = |s: &str| super::query(s).unwrap().to_string();
        assert_eq!(q(r#"SELECT "a", `b c` FROM t WHERE !x AND y <=> 1"#), r#"SELECT 'a' AS "a", `b c` FROM t WHERE NOT x AND (y) IS NOT DISTINCT FROM (1)"#);
        assert_eq!(q("SELECT explode(xs) FROM t"), r#"SELECT unnest(xs) AS "col" FROM t"#);
        assert_eq!(q("SELECT a DIV 2 AS h FROM t"), "SELECT CAST((a) / (2) AS BIGINT) AS h FROM t");
        assert_eq!(q("SELECT floor(d) AS f, CEIL(d) FROM t"), r#"SELECT spark_floor(d) AS f, spark_ceil(d) AS "CEIL(d)" FROM t"#);
        assert_eq!(q("SELECT id, x FROM t LATERAL VIEW explode(xs) v AS x"), "SELECT id, x FROM (SELECT t.*, unnest(xs) AS x FROM t) AS t");
        assert_eq!(q("SELECT a.id, v.x FROM t a LATERAL VIEW OUTER explode(a.xs) v AS x"),
            "SELECT a.id, a.x FROM (SELECT a.*, unnest(CASE WHEN cardinality(a.xs) > 0 THEN a.xs ELSE make_array(NULL) END) AS x FROM t a) AS a");
        assert!(super::query("SELECT * FROM a JOIN b LATERAL VIEW explode(xs) v AS x").is_err());
        assert!(super::query("INSERT INTO t SELECT 1").is_err());
        assert_eq!(super::inline("SELECT * FROM spark_sql('SELECT a FROM t ORDER BY b')").unwrap(), "SELECT a FROM t ORDER BY b"); // (its sort holds)
        assert_eq!(super::inline("SELECT a FROM spark_sql('SELECT a FROM t') s WHERE a > 1").unwrap(), "SELECT a FROM (SELECT a FROM t) s WHERE a > 1");
    }
}
