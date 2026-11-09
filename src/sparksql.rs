//! Spark SQL where PySpark code sends it (H8, round 31), a language of the registry's (`LANGUAGE`). A session's statements
//! in it (`SET language = 'spark'`), and `spark_sql('…')` in a FROM, are read with Spark's grammar and turned into Pondra's
//! SQL as they come in (`statement::in_language`, `languages::inline`), so what is built on them (a frame's steps, a join, a view)
//! is Pondra's SQL as everywhere else, and one text answers the same either way. What it turns: `"text"` (a string in
//! Spark), `a / b` (always fractional, `spark_divide`), `a DIV b`, `<=>`, `!`, `RLIKE`, `LATERAL VIEW [OUTER] explode(…)` and
//! `explode` itself, and the functions both have with Spark's answers (`floor` a BIGINT, `substring`, `round`, …): Spark's
//! own, registered in every session as `spark_floor`, … (`SPARK`), so DataFusion's answers never change outside it
//! (invariant 190). A cast to an integer cuts its fraction toward zero, as Spark does (`pondra_truncate`: Pondra's own
//! casts round it). Backticks are names in Pondra's SQL too.
use anyhow::{bail, ensure, Context, Result};
use datafusion::sql::sqlparser::ast::{self, BinaryOperator, CastKind, DataType, Expr, FunctionArguments, SelectItem, SetExpr, TableFactor, UnaryOperator, VisitorMut};
use datafusion::sql::sqlparser::dialect::{GenericDialect, SparkSqlDialect};
use datafusion::sql::sqlparser::parser::Parser;
use std::collections::HashMap;
use std::ops::ControlFlow;

mod functions;
pub use functions::{spark_functions, SPARK};

/// Spark SQL, as a language of the registry's (`languages.rs`): its text read with Spark's grammar and turned into
/// Pondra's statements, and `spark_sql('…')` its table function in a FROM. `databricks` names it too, as DataFusion's
/// dialect setting does.
pub static LANGUAGE: crate::languages::Language = crate::languages::Language { name: "spark", names: &["databricks"], parse: statements, inline: Some("spark_sql"), about: "Spark SQL" };

/// The front's pass of this module (`passes.rs`): Spark's queries in a FROM (`languages::inline`), triggered by `spark_sql`.
pub(crate) fn pass() -> crate::passes::Pass {
    fn run(sql: &str) -> Result<std::borrow::Cow<'_, str>> {
        let out = crate::languages::inline(sql)?;
        Ok(if out == sql { std::borrow::Cow::Borrowed(sql) } else { std::borrow::Cow::Owned(out) })
    }
    crate::passes::Pass { name: "sparksql.inline", order: 10, words: vec![], marks: vec![&["spark_sql"]], run: crate::passes::Run::Text(run) }
}

/// Spark SQL's statements as Pondra's: each read with Spark's grammar, and what Spark means by it turned into Pondra's SQL.
pub fn statements(text: &str) -> Result<Vec<ast::Statement>> {
    let mut stmts = crate::parsed::signed_all(Parser::parse_sql(&SparkSqlDialect {}, text).context("Spark SQL")?);
    let mut t = Turn { names: &spark_functions().names };
    if let ControlFlow::Break(e) = crate::walk::visit_mut(&mut stmts, &mut t) {
        return Err(e);
    }
    Ok(stmts)
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
            let _ = crate::walk::visit_expressions_mut(q, |e| {
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
                if let ControlFlow::Break(_) = crate::walk::visit_mut(&mut now, &mut Turn { names }) {
                    continue; // (said when the walk reaches it)
                }
                if now.to_string() == was || matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                    continue;
                }
                let name = match &*e {
                    Expr::Value(v) => v.value.clone().into_string().unwrap_or(was), // (a literal: named its value, as Spark names it)
                    Expr::Function(f) if ["explode", "explode_outer"].contains(&crate::text::object(&f.name).as_str()) => "col".into(),
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
            match &v.value {
                ast::Value::DoubleQuotedString(s) => v.value = ast::Value::SingleQuotedString(s.clone()), // ("text" is a string in Spark)
                // (Spark's strings take a \' that its X'…' reads too, and a quote in one would print past its end)
                ast::Value::HexStringLiteral(s) if !s.chars().all(|c| c.is_ascii_hexdigit()) => {
                    bail!(crate::codes::coded("42601", format!("Spark SQL: X'{s}' is not hex: X'…' takes the digits 0-9 and a-f, as Spark's does")))
                }
                _ => {}
            }
            None
        }
        Expr::UnaryOp { op, .. } if *op == UnaryOperator::BangNot => {
            *op = UnaryOperator::Not;
            None
        }
        Expr::BinaryOp { left, op: BinaryOperator::MyIntegerDivide, right } => Some(expr(&format!("CAST(pondra_truncate(({left}) / ({right})) AS BIGINT)"))?), // (DIV: a whole number, toward zero: the cut first, as Pondra's cast would round)
        // (Spark's cast to an integer cuts a fraction toward zero; Pondra's would round it, so the cut goes in first)
        Expr::Cast { kind, expr: x, data_type, array: false, format: None } if whole_number(data_type) => {
            let cast = match kind {
                CastKind::TryCast | CastKind::SafeCast => "TRY_CAST",
                _ => "CAST",
            };
            Some(expr(&format!("{cast}(pondra_truncate({x}) AS {data_type})"))?)
        }
        // (Spark's `/` is always fractional, and two DECIMALs have Spark's places: `spark_divide`, `divide.rs`)
        Expr::BinaryOp { left, op: BinaryOperator::Divide, right } => Some(expr(&format!("{}({left}, {right})", crate::divide::SPARK_NAME))?),
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
            let name = crate::text::object(&f.name);
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

/// Spark's integer types, the unsigned forms too (sqlparser reads them): what a cast to an integer names.
fn whole_number(t: &DataType) -> bool {
    matches!(
        t,
        DataType::TinyInt(_) | DataType::TinyIntUnsigned(_) | DataType::UTinyInt | DataType::SmallInt(_) | DataType::SmallIntUnsigned(_) | DataType::USmallInt
            | DataType::Int(_) | DataType::IntUnsigned(_) | DataType::Integer(_) | DataType::IntegerUnsigned(_) | DataType::BigInt(_) | DataType::BigIntUnsigned(_) | DataType::UBigInt
    )
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
        let name = crate::text::object(&f.name);
        ensure!(name == "explode" || name == "explode_outer", "LATERAL VIEW {name}: Pondra takes explode and explode_outer of an array");
        let [col] = &v.lateral_col_alias[..] else { bail!("LATERAL VIEW {} {} AS …: one column (a map's key and value: not yet)", v.lateral_view, v.lateral_view_name) };
        views.insert(v.lateral_view_name.to_string().to_lowercase(), alias.value.clone());
        let mut call = v.lateral_view.clone();
        if v.outer {
            if let Expr::Function(f) = &mut call {
                f.name = ast::ObjectName::from(vec![ast::Ident::new("explode_outer")]);
            }
        }
        let _ = crate::walk::visit_expressions_mut(&mut call, |e| {
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

#[cfg(test)]
mod tests {
    #[test]
    fn spark_becomes_pondra() {
        let q = |s: &str| super::statements(s).unwrap().iter().map(|s| s.to_string()).collect::<String>();
        assert_eq!(q(r#"SELECT "a", `b c` FROM t WHERE !x AND y <=> 1"#), r#"SELECT 'a' AS "a", `b c` FROM t WHERE NOT x AND (y) IS NOT DISTINCT FROM (1)"#);
        assert_eq!(q("SELECT explode(xs) FROM t"), r#"SELECT unnest(xs) AS "col" FROM t"#);
        assert_eq!(q("SELECT a DIV 2 AS h FROM t"), "SELECT CAST(pondra_truncate((a) / (2)) AS BIGINT) AS h FROM t");
        assert_eq!(q("SELECT a / 2, CAST(a / b AS INT) AS i FROM t"), r#"SELECT spark_divide(a, 2) AS "a / 2", CAST(pondra_truncate(spark_divide(a, b)) AS INT) AS i FROM t"#); // (its column keeps the name Pondra's gave it)
        assert_eq!(q("SELECT CAST(a AS INT) AS i, a::BIGINT AS j, TRY_CAST(a AS SMALLINT) AS k, CAST(a AS DOUBLE) AS d, CAST(a AS STRING) AS s FROM t"),
            "SELECT CAST(pondra_truncate(a) AS INT) AS i, CAST(pondra_truncate(a) AS BIGINT) AS j, TRY_CAST(pondra_truncate(a) AS SMALLINT) AS k, CAST(a AS DOUBLE) AS d, CAST(a AS STRING) AS s FROM t");
        assert_eq!(q("SELECT floor(d) AS f, CEIL(d) FROM t"), r#"SELECT spark_floor(d) AS f, spark_ceil(d) AS "CEIL(d)" FROM t"#);
        assert_eq!(q("SELECT id, x FROM t LATERAL VIEW explode(xs) v AS x"), "SELECT id, x FROM (SELECT t.*, unnest(xs) AS x FROM t) AS t");
        assert_eq!(q("SELECT a.id, v.x FROM t a LATERAL VIEW OUTER explode(a.xs) v AS x"),
            "SELECT a.id, a.x FROM (SELECT a.*, unnest(CASE WHEN cardinality(a.xs) > 0 THEN a.xs ELSE make_array(NULL) END) AS x FROM t a) AS a");
        assert!(super::statements("SELECT * FROM a JOIN b LATERAL VIEW explode(xs) v AS x").is_err());
        assert_eq!(q("SELECT - -k AS m, - - -1 AS n, 2 - -k AS o FROM t"), "SELECT -(-k) AS m, -(-(-1)) AS n, 2 - -k AS o FROM t"); // (a sign's sign: `--` would be a comment)
        let hex = super::statements(r"SELECT X'\'' AS b").unwrap_err(); // (Spark's \' read into a hex literal: its quote would print past its end)
        assert_eq!((crate::codes::of(&hex), q("SELECT X'0aFF' AS b")), ("42601", "SELECT X'0aFF' AS b".to_string()));
        assert_eq!(q("INSERT INTO t SELECT a DIV 2 AS h FROM s WHERE b = \"x\""), "INSERT INTO t SELECT CAST(pondra_truncate((a) / (2)) AS BIGINT) AS h FROM s WHERE b = 'x'"); // (a statement's queries are turned too)
    }

    /// A statement in Spark SQL is planned from its tree printed (`languages::inline` prints the statement it is in, and
    /// from step 8's PR 7b a Spark session's statements go on so until PR 9 hands the tree over), so each tree a session or
    /// `spark_sql('…')` prints, prints as a text that means it: one that reads back as that tree, or reads as itself again
    /// (Pondra reads what it prints: `unnest` as its UNNEST, `user` as its USER), or is the statement as written, token for
    /// token (a form Pondra's other readers take). Of every Spark text the repository sends (harness.py's, spark_check.py's,
    /// the docs' and the client's `spark.sql(…)` and `spark_sql('…')`), TPC-H's queries, and every other text of its SQL that
    /// Spark's grammar reads (`passes::tests::texts`), each statement as a request's are split (`routines::split`). A sign's
    /// sign printed `--` once, a comment that ate the rest of its line.
    #[test]
    fn a_spark_tree_prints_as_its_text() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let called = regex::Regex::new(r#"(?s)spark\.sql\(\s*(?:"""(.*?)"""|"((?:[^"\\\n]|\\.)*)"|'((?:[^'\\\n]|\\.)*)')"#).unwrap();
        let inline = regex::Regex::new(r"spark_sql\('((?:[^']|'')*)'\)").unwrap();
        let unescape = |s: &str| s.replace("\\n", "\n").replace("\\\"", "\"").replace("\\'", "'").replace("\\\\", "\\");
        let mut spark = std::collections::BTreeSet::new();
        for f in ["tools/harness.py", "tools/spark_check.py", "docs/dataframe-api.md", "python/pondra/spark/__init__.py", "README.md"] {
            let text = std::fs::read_to_string(root.join(f)).unwrap_or_default();
            for c in called.captures_iter(&text) {
                spark.insert(unescape(c.get(1).or(c.get(2)).or(c.get(3)).map_or("", |m| m.as_str())));
            }
            for c in inline.captures_iter(&text) {
                spark.insert(c[1].replace("''", "'"));
            }
        }
        let read = spark.iter().filter(|t| super::statements(t).is_ok()).count();
        // (21 of the 28 found read when this was written: the floor catches a file or a pattern that stops finding them)
        assert!(read >= 20, "the Spark texts found: {read} of {} read", spark.len());
        let tokens = |sql: &str| -> Option<Vec<String>> {
            use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};
            let all = Tokenizer::new(&GenericDialect {}, sql).tokenize().ok()?;
            let mut out: Vec<String> = all.into_iter().filter(|t| !matches!(t, Token::Whitespace(_))).map(|t| match t {
                Token::Word(w) if w.quote_style.is_none() => w.value.to_lowercase(),
                t => t.to_string(),
            }).collect();
            while out.last().is_some_and(|t| t == ";") {
                out.pop();
            }
            Some(out)
        };
        let (mut trees, mut differ) = (0, vec![]);
        for t in spark.iter().cloned().chain(crate::passes::tests::texts("")) {
            for one in crate::routines::split(&t) {
                let Ok(stmts) = super::statements(&one) else { continue }; // (refused, by Spark's grammar or the turn)
                // (a query is what `spark_sql('…')` takes; a session prints each statement but Pondra's own, read as written)
                let printed_here = |s: &super::ast::Statement| matches!(s, super::ast::Statement::Query(_)) || !crate::statement::pondra_s_own(one.trim());
                for s in stmts.into_iter().filter(printed_here) {
                    trees += 1;
                    let printed = s.to_string();
                    let reads = crate::parsed::generic(&printed).is_ok_and(|back| back == [s.clone()] || back.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(";\n") == printed);
                    let as_written = tokens(&printed).is_some_and(|p| tokens(&one) == Some(p));
                    if !reads && !as_written {
                        differ.push((one.clone(), printed));
                    }
                }
            }
        }
        assert!(differ.is_empty(), "{} of {trees} trees print as a text that means otherwise, the first: {:#?}", differ.len(), &differ[..differ.len().min(8)]);
    }
}
