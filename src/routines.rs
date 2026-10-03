//! Functions, procedures and scripts: SQL — and Python — kept in the catalog under a name (`r/`),
//! as stored views are (ADR-023, ADR-027).
//!
//! - **A SQL function** (Postgres's `CREATE FUNCTION … RETURN expr` or `LANGUAGE sql AS $$ SELECT
//!   … $$`, DuckDB's `CREATE MACRO`) is an expression or a query with parameters. Where SQL comes
//!   in (every door, and stored views as they are read) a call is replaced by the body, the
//!   arguments — cast to the parameters' types — in the parameters' places (`expand`). After that
//!   it is plain SQL: it plans, spreads and is remembered like any other.
//! - **A Python function** (`LANGUAGE python`) is a DataFusion function whose batches go to this
//!   node's Python workers (`pyfn.rs`, `python.rs`): per row, a batch at once (`vectorized`), or,
//!   returning a table, once per call.
//! - **A procedure** is statements run in order (`LANGUAGE sql`) or a Python program (`LANGUAGE
//!   python`), with typed parameters: `CALL load_day(DATE '2026-09-27')`. Its arguments are worked
//!   out once; then each statement runs as if the caller had sent it, with the caller's rights. A
//!   Python procedure runs on a warm worker beside the node with a connection back to it that has
//!   the caller's rights and no more (`auth::lend`); what it prints goes to its caller as notices,
//!   and the value it returns (or its last line's) is the answer: a frame (whose SQL then runs
//!   here), a table, a value, or nothing. Every call is in the run log (`runs.rs`).
//! - **A script** is several statements with `$name` parameters: a request to `POST /sql`, a file
//!   `pondra run` sends, a procedure's body (`split`, `prepare`, then `one` each in turn).
use crate::auth::Role;
use crate::ddl::Ddl;
use crate::server::App;
use crate::store::{json, Lake};
use crate::write::{ident, object, Stmt};
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::RecordBatch;
use datafusion::sql::sqlparser::ast::{self, visit_expressions_mut, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Statement, TableFactor, VisitMut, VisitorMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::Token};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock, Mutex};

pub fn key(name: &str) -> String { format!("r/{name}") }

/// A function or a procedure, as the catalog keeps it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Routine {
    pub kind: Kind,
    pub params: Vec<Param>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub language: String, // sql or python; none: a macro (DuckDB's CREATE MACRO)
    pub body: String,
    /// A function's result: a SQL type, or `TABLE (name type, …)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returns: Option<String>,
    #[serde(default, skip_serializing_if = "Options::is_empty")]
    pub with: Options,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Macro,     // a function whose value is an expression's (a scalar function)
    Table,     // a function whose value is a query's rows (a table function)
    Procedure, // statements, or a Python program
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Param {
    pub name: String, // (Postgres's unnamed parameters: 1, 2…, which the body calls $1, $2…)
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub ty: Option<String>, // (its arguments are cast to it)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>, // a SQL expression
}

/// Postgres's words for how a function behaves, and Pondra's options (`WITH (…)`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Options {
    /// NULL in, NULL out, without running it (`STRICT`, `RETURNS NULL ON NULL INPUT`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub strict: bool,
    /// immutable, stable or volatile. A query calling a volatile Python function isn't answered
    /// from the result cache (a Python function is volatile unless it says otherwise).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volatility: Option<String>,
    /// A Python function called once per batch with pyarrow arrays, not once per row.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub vectorized: bool,
    /// Packages its Python needs (`requests, jinja2`): each node installs them once.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub packages: String,
    /// The body is a module, and this function of it is what runs (the decorators' form).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub entry: String,
    /// Seconds a call may take (a function's: each batch, 60 by default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<f64>,
    /// Seconds an answer is reused for the same arguments (`cache = '5 minutes'`: `pyfn::Answers`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<u64>,
}

impl Options {
    fn is_empty(&self) -> bool { *self == Options::default() }
}

impl Routine {
    pub fn python(&self) -> bool { self.language == "python" }

    /// What SQL calls it: a function (a macro, if made as one) or a procedure.
    pub fn what(&self) -> &'static str {
        match (self.kind, self.language.is_empty()) {
            (Kind::Procedure, _) => "procedure",
            (_, true) => "macro",
            _ => "function",
        }
    }

    /// Does a query calling it give the same answer every time (so it may be remembered)?
    pub fn cacheable(&self) -> bool { !self.python() || matches!(self.with.volatility.as_deref(), Some("immutable" | "stable")) }
}

// ---------------------------------------------------------------- statements

/// `CREATE [OR REPLACE] MACRO …`, as sqlparser reads it.
pub fn of_macro(args: &Option<Vec<ast::MacroArg>>, def: &ast::MacroDefinition) -> Routine {
    let params = args.iter().flatten().map(|a| Param { name: ident(&a.name), ty: None, default: a.default_expr.as_ref().map(|e| e.to_string()) }).collect();
    let (kind, body) = match def {
        ast::MacroDefinition::Expr(e) => (Kind::Macro, e.to_string()),
        ast::MacroDefinition::Table(q) => (Kind::Table, q.to_string()),
    };
    Routine { kind, params, language: String::new(), body, returns: None, with: Options::default() }
}

/// The statements sqlparser doesn't read as Pondra means them: `CREATE [OR REPLACE] FUNCTION`
/// and `CREATE [OR REPLACE] PROCEDURE` (Postgres's forms; DuckDB's `CREATE FUNCTION f(x) AS
/// expr` too), `CREATE [OR REPLACE] TASK name SCHEDULE '…' AS statement`, `DROP TASK` and `DROP
/// MACRO [TABLE] [IF EXISTS] name` (DuckDB's). None: none of them; `Stmt::Invalid`: one, written
/// wrong, and why.
pub fn statement(sql: &str) -> Option<Stmt> {
    let head = crate::write::first_word(sql).get(..6)?.to_lowercase();
    if head != "create" && !head.starts_with("drop") && !head.starts_with("alter") {
        return None;
    }
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(sql).ok()?;
    if p.parse_keywords(&[Keyword::ALTER, Keyword::TASK]) {
        // `ALTER TASK name SUSPEND | RESUME` (ADR-045)
        let name = p.parse_object_name(false).ok()?;
        let suspended = match () {
            _ if word(&mut p, "suspend") => true,
            _ if word(&mut p, "resume") => false,
            _ => return Some(Stmt::Invalid("ALTER TASK name SUSPEND | RESUME".into())),
        };
        return Some(Stmt::Ddl(vec![Ddl::AlterTask { name: object(&name), suspended }]));
    }
    if head.starts_with("alter") {
        return None;
    }
    if p.parse_keyword(Keyword::DROP) {
        let task = p.parse_keyword(Keyword::TASK);
        if !task && !p.parse_keyword(Keyword::MACRO) {
            return None;
        }
        let _ = !task && p.parse_keyword(Keyword::TABLE);
        let if_exists = p.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        return Some(match p.parse_object_name(false) {
            Ok(n) if task => Stmt::Ddl(vec![Ddl::DropTask { name: object(&n), if_exists }]),
            Ok(n) => Stmt::Ddl(vec![Ddl::DropRoutine { name: object(&n), if_exists }]),
            Err(e) => Stmt::Invalid(format!("DROP {}: {e}", if task { "TASK" } else { "MACRO" })),
        });
    }
    let create = p.parse_keyword(Keyword::CREATE);
    let replace = p.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    let what = p.parse_one_of_keywords(&[Keyword::FUNCTION, Keyword::PROCEDURE, Keyword::TASK]).filter(|_| create)?;
    let quiet = p.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]); // (nothing if one of the name is there)
    if quiet && replace {
        return Some(Stmt::Invalid("CREATE OR REPLACE … IF NOT EXISTS: one or the other".into()));
    }
    Some(match what {
        Keyword::TASK => crate::runs::task(&mut p, sql).map_or_else(|e| Stmt::Invalid(format!("CREATE TASK: {e:#} ({})", crate::runs::USAGE)), |(name, task)| Stmt::Ddl(vec![crate::write::unless(quiet, &name, "task", Ddl::CreateTask { name: name.clone(), task, replace })])),
        k => {
            let procedure = k == Keyword::PROCEDURE;
            let usage = match procedure {
                true => "CREATE PROCEDURE name($p TYPE [= …], …) AS BEGIN … END, or LANGUAGE python AS $$ … $$",
                false => "CREATE FUNCTION name(p TYPE, …) RETURNS TYPE RETURN expression, or … RETURNS TYPE|TABLE (c TYPE, …) LANGUAGE sql|python AS $$ … $$",
            };
            routine(&mut p, sql, procedure).map_or_else(|e| Stmt::Invalid(format!("CREATE {}: {e:#} ({usage})", if procedure { "PROCEDURE" } else { "FUNCTION" })), |(name, routine)| Stmt::Ddl(vec![crate::write::unless(quiet, &name, "routine", Ddl::CreateRoutine { name: name.clone(), routine, replace })]))
        }
    })
}

/// A word sqlparser has no keyword for (`SCHEDULE`), if it is next.
pub fn word(p: &mut Parser, w: &str) -> bool {
    match p.peek_token().token {
        Token::Word(x) if x.value.eq_ignore_ascii_case(w) => {
            p.next_token();
            true
        }
        _ => false,
    }
}

/// A body: `$$ … $$` or `'…'`.
pub fn text_of(p: &mut Parser) -> Result<Option<String>> {
    Ok(match p.peek_token().token {
        Token::DollarQuotedString(s) => {
            p.next_token();
            Some(s.value)
        }
        Token::SingleQuotedString(s) => {
            p.next_token();
            Some(s)
        }
        _ => None,
    })
}

/// The rest of `CREATE FUNCTION` or `CREATE PROCEDURE`: its name, parameters, and the clauses
/// that follow in any order — Postgres's, and `WITH (…)`.
fn routine(p: &mut Parser, sql: &str, procedure: bool) -> Result<(String, Routine)> {
    let name = object(&p.parse_object_name(false)?);
    let params = params(p)?;
    let mut r = Routine { kind: if procedure { Kind::Procedure } else { Kind::Macro }, params, language: String::new(), body: String::new(), returns: None, with: Options::default() };
    let (mut body, mut expression, mut rest) = (None, false, false);
    while !rest {
        if p.parse_keyword(Keyword::LANGUAGE) {
            r.language = p.parse_identifier()?.value.to_lowercase();
        } else if !procedure && p.parse_keyword(Keyword::RETURNS) {
            if p.parse_keywords(&[Keyword::NULL, Keyword::ON, Keyword::NULL, Keyword::INPUT]) {
                r.with.strict = true;
            } else if p.parse_keyword(Keyword::TABLE) {
                p.expect_token(&Token::LParen)?;
                let cols = p.parse_comma_separated(|p| Ok(format!("{} {}", ast::Ident::with_quote('"', ident(&p.parse_identifier()?)), p.parse_data_type()?)))?;
                p.expect_token(&Token::RParen)?;
                (r.kind, r.returns) = (Kind::Table, Some(format!("TABLE ({})", cols.join(", "))));
            } else if p.parse_keyword(Keyword::SETOF) {
                let t = p.parse_data_type()?;
                (r.kind, r.returns) = (Kind::Table, Some(format!("TABLE (\"{}\" {t})", crate::ddl::split(&name).1)));
            } else {
                r.returns = Some(p.parse_data_type()?.to_string());
            }
        } else if !procedure && p.parse_keyword(Keyword::RETURN) {
            (body, expression) = (Some(p.parse_expr()?.to_string()), true);
        } else if p.parse_keyword(Keyword::AS) {
            body = match text_of(p)? {
                Some(t) => Some(t),
                None if procedure => {
                    rest = true; // (`AS BEGIN … END`, or any one statement: the rest of the text, as a task's)
                    let b = sql[crate::runs::offset(sql, p.peek_token().span.start)..].trim();
                    Some(b.strip_suffix(';').unwrap_or(b).trim_end().to_string()).filter(|b| !b.is_empty())
                }
                None if p.parse_keyword(Keyword::TABLE) => {
                    r.kind = Kind::Table; // (DuckDB's: CREATE FUNCTION f(x) AS TABLE SELECT …)
                    Some(p.parse_query()?.to_string())
                }
                None => {
                    expression = true; // (DuckDB's: CREATE FUNCTION f(x) AS x + 1)
                    Some(p.parse_expr()?.to_string())
                }
            };
        } else if let Some(k) = p.parse_one_of_keywords(&[Keyword::IMMUTABLE, Keyword::STABLE, Keyword::VOLATILE]) {
            r.with.volatility = Some(format!("{k:?}").to_lowercase());
        } else if p.parse_keyword(Keyword::STRICT) {
            r.with.strict = true;
        } else if p.parse_keywords(&[Keyword::CALLED, Keyword::ON, Keyword::NULL, Keyword::INPUT]) {
            r.with.strict = false;
        } else if p.parse_keyword(Keyword::LEAKPROOF) || p.parse_keywords(&[Keyword::NOT, Keyword::LEAKPROOF]) {
        } else if p.parse_keyword(Keyword::SECURITY) {
            ensure!(p.parse_keyword(Keyword::INVOKER), "SECURITY DEFINER: a routine runs with its caller's rights (SECURITY INVOKER)");
        } else if p.parse_keyword(Keyword::PARALLEL) {
            p.parse_one_of_keywords(&[Keyword::SAFE, Keyword::RESTRICTED, Keyword::UNSAFE]).context("PARALLEL SAFE, RESTRICTED or UNSAFE")?;
        } else if p.parse_one_of_keywords(&[Keyword::COST, Keyword::ROWS]).is_some() {
            p.parse_number_value()?;
        } else if p.parse_keyword(Keyword::WITH) {
            options(p, &mut r.with)?;
        } else {
            break;
        }
    }
    let t = if rest { Token::EOF } else { p.next_token().token };
    ensure!(matches!(t, Token::EOF | Token::SemiColon), "unexpected {t}");
    r.body = body.context("no body: AS $$ … $$, or RETURN expression")?;
    r.language = match r.language.as_str() {
        "" | "sql" => "sql".into(),
        "python" | "python3" | "plpython3u" | "plpythonu" | "plpython" => "python".into(),
        l => bail!("LANGUAGE sql or python, not {l}"),
    };
    ensure!(!expression || r.language == "sql", "an expression's language is SQL: LANGUAGE python AS $$ … $$ for Python");
    ensure!(!rest || r.language == "sql", "a Python procedure's body is a string: LANGUAGE python AS $$ … $$");
    ensure!(procedure || r.returns.is_some() || r.language == "sql", "a Python function says what it returns: RETURNS TYPE, or RETURNS TABLE (c TYPE, …)");
    ensure!(r.returns.as_deref().is_none_or(|t| !crate::pyfn::loose(Some(t)) || crate::pyfn::is_json(Some(t))), "RETURNS ANY: say the type it returns (VARIANT for any JSON value)");
    ensure!(!r.with.vectorized || (r.python() && r.kind == Kind::Macro), "vectorized: a Python function returning a value (not a table, or a procedure)");
    ensure!(r.python() || (r.with.packages.is_empty() && r.with.entry.is_empty()), "packages and entry: a Python routine's");
    ensure!(r.with.cache.is_none() || (r.python() && !procedure), "cache: a Python function's (a SQL function is part of its query, whose answers the result cache keeps; a procedure is called for what it does)");
    Ok((name, r))
}

/// Parameters: `( [IN] [name] type [DEFAULT expr | = expr], … )`; a parameter without a name is
/// called by its place (`$1`).
fn params(p: &mut Parser) -> Result<Vec<Param>> {
    if !p.consume_token(&Token::LParen) || p.consume_token(&Token::RParen) {
        return Ok(vec![]);
    }
    let mut i = 0;
    let params = p.parse_comma_separated(|p| {
        i += 1;
        if let Some(k) = p.parse_one_of_keywords(&[Keyword::IN, Keyword::OUT, Keyword::INOUT, Keyword::VARIADIC]) {
            if k != Keyword::IN {
                return Err(datafusion::sql::sqlparser::parser::ParserError::ParserError(format!("{k:?} parameters: return a table instead (RETURNS TABLE (…))")));
            }
        }
        let unnamed = p.maybe_parse(|p| {
            let t = p.parse_data_type()?;
            match p.peek_token().token {
                Token::Comma | Token::RParen | Token::Eq => Ok(t),
                Token::Word(w) if w.keyword == Keyword::DEFAULT => Ok(t),
                t => Err(datafusion::sql::sqlparser::parser::ParserError::ParserError(format!("{t}"))),
            }
        })?;
        let (name, ty) = match unnamed {
            Some(ast::DataType::Custom(n, m)) if m.is_empty() && n.0.len() == 1 && !crate::pyfn::loose(Some(&n.to_string())) => (object(&n), None), // (`f(x)`: a name, untyped, as DuckDB's)
            Some(t) => (i.to_string(), Some(t.to_string())),
            None => {
                let name = match p.peek_token().token {
                    Token::Placeholder(n) if n.len() > 1 && !n[1..].starts_with(|c: char| c.is_ascii_digit()) => {
                        p.next_token();
                        n[1..].to_lowercase() // (`$day DATE`, as a file's parameter is written)
                    }
                    _ => ident(&p.parse_identifier()?),
                };
                let typed = !matches!(p.peek_token().token, Token::Comma | Token::RParen | Token::Eq) && !matches!(p.peek_token().token, Token::Word(ref w) if w.keyword == Keyword::DEFAULT);
                (name, if typed { Some(p.parse_data_type()?.to_string()) } else { None })
            }
        };
        let default = match p.parse_keyword(Keyword::DEFAULT) || p.consume_token(&Token::Eq) {
            true => Some(p.parse_expr()?.to_string()),
            false => None,
        };
        Ok(Param { name, ty, default })
    })?;
    p.expect_token(&Token::RParen)?;
    Ok(params)
}

/// `WITH (vectorized = true, packages = 'requests, jinja2', entry = 'f', timeout = 5)`.
fn options(p: &mut Parser, o: &mut Options) -> Result<()> {
    p.expect_token(&Token::LParen)?;
    loop {
        let k = p.parse_identifier()?.value.to_lowercase();
        let v = match p.consume_token(&Token::Eq) {
            true => match p.next_token().token {
                Token::SingleQuotedString(s) | Token::Number(s, _) => s,
                Token::Word(w) => w.value.to_lowercase(),
                t => bail!("{k}: a value, not {t}"),
            },
            false => "true".into(),
        };
        let yes = || -> Result<bool> { Ok(v.parse::<bool>().with_context(|| format!("{k}: true or false"))?) };
        match k.as_str() {
            "vectorized" => o.vectorized = yes()?,
            "strict" => o.strict = yes()?,
            "packages" => o.packages = v.split(',').map(str::trim).filter(|s| !s.is_empty()).collect::<Vec<_>>().join(", "),
            "entry" => o.entry = v.clone(),
            "timeout" => o.timeout = Some(v.parse::<f64>().ok().filter(|s| *s > 0.0).context("timeout: seconds")?),
            "cache" => o.cache = Some(match crate::runs::every(&v).context("cache: how long an answer is reused ('10 minutes', '30 seconds')")? {
                crate::runs::Every::Seconds(s) => s,
                crate::runs::Every::Cron(..) => bail!("cache: how long ('10 minutes'), not a schedule"),
            }),
            "volatility" => {
                ensure!(["immutable", "stable", "volatile"].contains(&v.as_str()), "volatility: immutable, stable or volatile");
                o.volatility = Some(v.clone());
            }
            _ => bail!("WITH ({k} …): vectorized, packages, entry, timeout, cache, strict or volatility"),
        }
        if p.consume_token(&Token::RParen) {
            return Ok(());
        }
        p.expect_token(&Token::Comma)?;
    }
}

/// `TABLE (a BIGINT, "b" VARCHAR)` → its columns and their SQL types.
pub fn columns_of(returns: &str) -> Result<Vec<(String, String)>> {
    let inner = returns.trim().strip_prefix("TABLE").context("not a table")?.trim();
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(inner)?;
    p.expect_token(&Token::LParen)?;
    let cols = p.parse_comma_separated(|p| Ok((ident(&p.parse_identifier()?), p.parse_data_type()?.to_string())))?;
    p.expect_token(&Token::RParen)?;
    Ok(cols)
}

/// Leader: keep a function or procedure (`ddl::apply`).
pub async fn create(lake: &Lake, name: &str, r: Routine, replace: bool) -> Result<Value> {
    let name = crate::ddl::new_name(lake, name).await?;
    ensure!(r.kind != Kind::Procedure || !crate::workspace::is_run(crate::ddl::split(&name).1), "{name}: run is Pondra's own procedure (CALL run('etl/orders.sql') runs a file of the lake's)");
    if let Some(old) = lake.cat.get::<Routine>(&key(&name)).await? {
        ensure!(replace, "{} {name} already exists (CREATE OR REPLACE {})", old.what(), r.what().to_uppercase());
        ensure!((old.kind == Kind::Procedure) == (r.kind == Kind::Procedure), "{name} is a {}", old.what());
    }
    check(lake, &name, &r).await?;
    lake.cat.commit(vec![(key(&name), json(&r))], &[]).await?;
    Ok(j!({r.what(): name}))
}

/// A body that reads, parameters named once, and a function that hides none of SQL's own.
async fn check(lake: &Lake, name: &str, r: &Routine) -> Result<()> {
    for (i, p) in r.params.iter().enumerate() {
        ensure!(!r.params[..i].iter().any(|q| q.name == p.name), "{name}: two parameters called {}", p.name);
    }
    let short = crate::ddl::split(name).1;
    let state = lake.session().state();
    let types = r.params.iter().filter_map(|p| p.ty.as_deref()).chain(r.returns.as_deref().filter(|t| !t.starts_with("TABLE"))).filter(|t| !crate::pyfn::loose(Some(t)) || crate::pyfn::is_json(Some(t)));
    for t in types {
        crate::pyfn::arrow_of(t).await.with_context(|| format!("{name}: the type {t}"))?;
    }
    if let Some(t) = r.returns.as_deref().filter(|t| t.starts_with("TABLE")) {
        for (_, t) in columns_of(t)? {
            crate::pyfn::arrow_of(&t).await.with_context(|| format!("{name}: the type {t}"))?;
        }
    }
    match r.kind {
        Kind::Macro => {
            ensure!(!state.scalar_functions().contains_key(short) && !state.aggregate_functions().contains_key(short) && !state.window_functions().contains_key(short), "{short} is one of SQL's own functions: call yours something else");
            if !r.python() {
                places(r, &mut scalar_body(&r.body)?)?;
            }
        }
        Kind::Table => {
            ensure!(!state.table_functions().contains_key(short), "{short} is one of SQL's own table functions: call yours something else");
            if !r.python() {
                places(r, &mut parse_query(query_text(&r.body))?)?;
            }
        }
        Kind::Procedure if r.language == "sql" => {
            let mut known: HashMap<String, Value> = r.params.iter().map(|p| (p.name.clone(), Value::Null)).chain((1..=r.params.len()).map(|i| (i.to_string(), Value::Null))).collect();
            for s in split(&r.body) {
                use crate::vars::Change;
                if crate::script::is(&s) {
                    // (a block, a branch, a loop: what it uses is known, or set inside it)
                    let bound = crate::script::binds(&s);
                    if let Some(n) = crate::vars::names(&s).into_iter().find(|n| !known.contains_key(n) && !bound.contains(n)) {
                        bail!("{name}: no value for ${n} (give it one, or declare it: DECLARE ${n} = …)");
                    }
                    known.extend(bound.into_iter().map(|n| (n, Value::Null)));
                    continue;
                }
                let (check, sets) = match crate::vars::change(&s) {
                    Some(Change::Declare { name, default, .. }) => (default.map(|d| format!("SELECT {d}")), Some(name)), // (its variables: known after their DECLARE)
                    Some(Change::Set { name, value }) => (Some(format!("SELECT {value}")), Some(name)),
                    Some(Change::Reset(_)) => (None, None),
                    None => (statement(&s).is_none().then(|| s.clone()), None),
                };
                if let Some(check) = check {
                    bind(&check, &known).with_context(|| format!("{name}: {}", s.trim()))?;
                }
                known.extend(sets.map(|n| (n, Value::Null)));
            }
        }
        Kind::Procedure => {}
    }
    if r.python() && r.with.packages.is_empty() && crate::python::found().await {
        // (compiled by a worker now, so a mistake is found when it's made, with its line; one
        // with packages when it is first used: they are installed then, not under the DDL lock)
        crate::python::ask(&r.with.packages, crate::python::Use::Procedure { nested: true }, j!({"op": "check", "name": name, "body": r.body, "entry": r.with.entry, "params": names(r)}), vec![], Some(std::time::Duration::from_secs(600)), &mut |_| {}).await.with_context(|| name.to_string())?;
    }
    Ok(())
}

/// Every `$n` and `$name` in a SQL function's body is one of its parameters (Postgres: "there is no
/// parameter $2"): a function's answer depends on its arguments alone, never a session's variable.
fn places<T: VisitMut>(r: &Routine, body: &mut T) -> Result<()> {
    let mut wrong = None;
    let _ = visit_expressions_mut(body, |e| {
        if let Expr::Value(v) = e {
            if let ast::Value::Placeholder(p) = &v.value {
                let n = p.trim_start_matches('$');
                let known = match n.parse::<usize>() {
                    Ok(i) => i > 0 && i <= r.params.len(),
                    Err(_) => r.params.iter().any(|q| q.name.eq_ignore_ascii_case(n)),
                };
                if !known && wrong.is_none() {
                    wrong = Some(p.clone());
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    wrong.map_or(Ok(()), |p| bail!("there is no parameter {p} (a function sees only its own parameters: {})", if r.params.is_empty() { "it has none".into() } else { r.params.iter().map(|q| format!("${}", q.name)).collect::<Vec<_>>().join(", ") }))
}

/// Its parameters' names, in order.
pub fn names(r: &Routine) -> Vec<&str> { r.params.iter().map(|p| p.name.as_str()).collect() }

/// Leader: `DROP MACRO`, `DROP FUNCTION`, `DROP PROCEDURE`.
pub async fn drop(lake: &Lake, name: &str, if_exists: bool) -> Result<Value> {
    let name = crate::ddl::local(lake, name).with_context(|| format!("{name}: not this lake's"))?;
    let Some(r) = lake.cat.get::<Routine>(&key(&name)).await? else {
        ensure!(if_exists, "no function or procedure {name}");
        return Ok(j!({"dropped": false}));
    };
    lake.cat.commit(vec![], &[key(&name)]).await?;
    Ok(j!({r.what(): name, "dropped": true}))
}

/// This lake's functions and procedures, by name: read again only after a commit (every query asks).
pub async fn listed(lake: &Lake) -> Result<Arc<HashMap<String, Routine>>> {
    type Seen = Mutex<HashMap<String, (u64, Arc<HashMap<String, Routine>>)>>;
    static SEEN: std::sync::LazyLock<Seen> = std::sync::LazyLock::new(Default::default);
    let version = lake.cat.version();
    if let (Some(v), Some((at, all))) = (version, SEEN.lock().unwrap().get(&lake.url)) {
        if *at == v {
            return Ok(all.clone());
        }
    }
    let all: Arc<HashMap<String, Routine>> = Arc::new(lake.cat.scan::<Routine>("r/", "r0").await?.into_iter().map(|(k, r)| (k[2..].to_string(), r)).collect());
    if let Some(v) = version {
        SEEN.lock().unwrap().insert(lake.url.clone(), (v, all.clone()));
    }
    Ok(all)
}

// ---------------------------------------------------------------- SQL made ready

/// A statement as it arrives, made ready to run: `$name` parameters bound to `params` (JSON
/// values, or `{"sql": "…"}` for an expression), macro calls replaced by their bodies, and the
/// request's own `views` (a client's frames, by name) by theirs. Text that needs none of it is
/// left as it is (and so is text sqlparser can't read).
pub async fn prepare(lake: &Lake, sql: &str, params: &HashMap<String, Value>, views: &HashMap<String, String>) -> Result<String> {
    let values = crate::vars::values(params); // (the variables there are, the values given over them: `vars.rs`)
    let numbered = values.keys().any(|k| k.starts_with(|c: char| c.is_ascii_digit())); // ($1 only when given: else a PREPARE's own)
    let sql = match crate::vars::uses(sql) || !values.is_empty() && sql.contains('$') { // (a value given may be for spark_sql('…')'s text)
        false => sql.to_string(),
        true => match bind_as(sql, &values, numbered) {
            Err(_) if !crate::vars::uses(sql) => sql.to_string(), // (text sqlparser can't read, using no variable: as it is)
            out => out?,
        },
    };
    expand_with(lake, &sql, views).await
}

/// Is `sql` a `CREATE` of a task, procedure, function or macro? Its `$name`s are its own
/// parameters or its graph's values, bound as it runs, never as it is made.
pub fn makes_code(sql: &str) -> bool {
    static HEAD: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^create\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?(?:task|procedure|function|macro)\b").expect("a regex"));
    HEAD.is_match(crate::write::first_word(sql))
}

/// `$name` → the value given for it; every `$name` needs one. `getvariable('name')` (DuckDB's) is
/// its value too, NULL if there is none.
pub fn bind(sql: &str, params: &HashMap<String, Value>) -> Result<String> { bind_as(sql, params, true) }

/// `bind`, but Postgres's `$1`, `$2` left for whoever binds them (the Postgres port's protocol).
pub fn bind_named(sql: &str, params: &HashMap<String, Value>) -> Result<String> { bind_as(sql, params, false) }

fn bind_as(sql: &str, params: &HashMap<String, Value>, numbered: bool) -> Result<String> {
    let getvariable = crate::vars::calls_getvariable(sql);
    if !sql.contains('$') && !getvariable || makes_code(sql) {
        return Ok(sql.to_string()); // (a task's or a routine's `$day`: its own, each time it runs)
    }
    let sql = &crate::sparksql::inline(sql)?; // (a parameter of Spark SQL's is in its text: bound once it is Pondra's)
    let sql = &crate::past::syntax(sql); // (`t AT (VERSION => $v)`)
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let values = params.iter().map(|(k, v)| Ok((k.clone(), literal(v)?))).collect::<Result<HashMap<_, _>>>()?;
    let bound = |p: &str| numbered || p.trim_start_matches('$').starts_with(|c: char| c.is_alphabetic() || c == '_');
    named_as_written(&mut stmts, |e| {
        let mut uses = false;
        let _ = ast::visit_expressions(e, |x| {
            uses |= match x {
                Expr::Value(v) => matches!(&v.value, ast::Value::Placeholder(p) if bound(p)),
                Expr::Function(f) => getvariable && object(&f.name) == "getvariable",
                _ => false,
            };
            ControlFlow::<()>::Continue(())
        });
        uses
    });
    let mut missing: Vec<String> = vec![];
    let _ = visit_expressions_mut(&mut stmts, |e| {
        match e {
            Expr::Value(v) => {
                if let ast::Value::Placeholder(p) = &v.value {
                    let name = p.trim_start_matches('$');
                    match values.get(name) {
                        _ if !bound(p) => {}
                        Some(to) => *e = Expr::Nested(Box::new(to.clone())),
                        None if !missing.contains(p) => missing.push(p.clone()),
                        None => {}
                    }
                }
            }
            Expr::Function(f) if getvariable && object(&f.name) == "getvariable" => {
                if let FunctionArguments::List(l) = &f.args {
                    if let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))] = &l.args[..] {
                        if let ast::Value::SingleQuotedString(name) = &v.value {
                            let null = || Expr::Value(ast::Value::Null.into());
                            *e = Expr::Nested(Box::new(values.get(name.as_str()).cloned().unwrap_or_else(null)));
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    ensure!(missing.is_empty(), "no value for {} (give it one, or declare it: DECLARE {} = …)", missing.join(", "), missing[0]);
    Ok(text(&stmts))
}

/// A select's columns that use a parameter or a variable, and have no name, named as written
/// (`$day + 1`), not after the value put in their place (`arrow_cast('2026-09-29', 'Date32') + 1`).
fn named_as_written(stmts: &mut [Statement], uses: impl Fn(&Expr) -> bool) {
    struct Namer<F>(F);
    impl<F: Fn(&Expr) -> bool> VisitorMut for Namer<F> {
        type Break = ();
        fn pre_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<()> {
            let mut body = &mut *q.body;
            while let ast::SetExpr::SetOperation { left, .. } = body {
                body = left; // (a union's names are its first part's)
            }
            if let ast::SetExpr::Select(s) = body {
                for item in s.projection.iter_mut() {
                    if let ast::SelectItem::UnnamedExpr(e) = item {
                        if (self.0)(e) {
                            *item = ast::SelectItem::ExprWithAlias { alias: ast::Ident::with_quote('"', e.to_string()), expr: e.clone() };
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut namer = Namer(uses);
    for s in stmts {
        let _ = s.visit(&mut namer);
    }
}

/// A SQL type as written (`DATE`, `DECIMAL(10, 2)`), read.
pub fn data_type(t: &str) -> Result<ast::DataType> {
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(t)?;
    let ty = p.parse_data_type()?;
    ensure!(p.peek_token().token == Token::EOF, "{t}: not a type");
    Ok(ty)
}

/// A parameter's value as SQL: JSON's strings, numbers, booleans, null and lists as literals;
/// `{"sql": "DATE '2026-09-27'"}` as the expression it is.
pub fn literal(v: &Value) -> Result<Expr> {
    parse_expr(&match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => b.to_string().to_uppercase(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote(s),
        Value::Array(a) => format!("[{}]", a.iter().map(|v| literal(v).map(|e| e.to_string())).collect::<Result<Vec<_>>>()?.join(", ")),
        Value::Object(o) => o.get("sql").and_then(Value::as_str).context("a parameter is a JSON value, or {\"sql\": \"an expression\"}")?.to_string(),
    })
}

fn quote(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }

fn parse_expr(sql: &str) -> Result<Expr> { Ok(Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_expr()?) }

fn parse_query(sql: &str) -> Result<ast::Query> { Ok(*Parser::new(&GenericDialect {}).try_with_sql(sql)?.parse_query()?) }

fn text(stmts: &[Statement]) -> String { stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(";\n") }

/// Every SQL function call in `sql` replaced by its body (a function's own calls too, 16 deep at
/// most), and every Python function's call made whole: its arguments in their places, defaults
/// filled in, under the name it is registered by (`pyfn.rs`). A statement that makes a function,
/// procedure or stored view keeps its calls: they are read when used (`query::stored_views`), so
/// a function changed later changes them too. A materialized view keeps the functions as they were
/// when it was made: it has been adding up rows since.
pub async fn expand(lake: &Lake, sql: &str) -> Result<String> { expand_with(lake, sql, &HashMap::new()).await }

async fn expand_with(lake: &Lake, sql: &str, views: &HashMap<String, String>) -> Result<String> {
    let sql = &crate::vars::parameters_in(lake, sql).await?; // (`pondra.parameters('etl/orders.sql')`: a file's parameters, as rows)
    if let Some(q) = show(sql) {
        return Ok(q);
    }
    if let Some(q) = crate::friendly::summarize(lake, sql).await? {
        return Ok(q);
    }
    let sql = &crate::past::restore(lake, sql).await?.unwrap_or_else(|| sql.to_string()); // (`RESTORE TABLE t TO VERSION AS OF n`: a MERGE, ADR-043)
    let sql = &crate::sparksql::inline(sql)?; // (`spark_sql('…')`: Spark SQL as Pondra's, then expanded as any)
    let sql = &crate::branch::diffs(sql)?; // (`pondra.diff('prod.t', 'dev.t')`: rows apart, ADR-047)
    let sql = &crate::past::syntax(sql).into_owned(); // (`t AT (VERSION => n)`: a table as it was, ADR-043)
    let sql = &crate::friendly::text(sql)?.into_owned(); // (`PIVOT t ON g`, `[x FOR x IN l]`, DuckDB's ASOF … ON: as SQL that parses)
    let all = listed(lake).await?;
    let outside = crate::ext::attached(lake).await?;
    let named = |n: &String| crate::ddl::mentions(sql, n);
    let expands = all.iter().any(|(n, r)| r.kind != Kind::Procedure && named(n)) || views.keys().any(named) || FROM_FIRST.is_match(sql) || crate::ext::mentions(sql)
        || outside.iter().any(|(n, _)| named(n)) || sql.contains("pondra_at(");
    let as_written = || Ok(named_apart(sql).unwrap_or_else(|| sql.to_string()));
    if !expands && !crate::friendly::wanted(sql) {
        return as_written();
    }
    // DuckDB's `FROM t WHERE …` (FROM first, with clauses after it): `SELECT * FROM t WHERE …`.
    static LEADING_FROM: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*(?:(?:--[^\n]*\n|/\*.*?\*/)\s*)*from\b").expect("a regex"));
    let parsed = Parser::parse_sql(&GenericDialect {}, sql).or_else(|e| match LEADING_FROM.is_match(sql) {
        true => Parser::parse_sql(&GenericDialect {}, &format!("SELECT * {sql}")),
        false => Err(e),
    });
    let Ok(mut stmts) = parsed else { return if expands { Ok(sql.to_string()) } else { as_written() } };
    for s in stmts.iter_mut().filter(|_| expands) {
        if !matches!(s, Statement::CreateMacro { .. } | Statement::CreateView(ast::CreateView { materialized: false, .. })) {
            if let ControlFlow::Break(e) = s.visit(&mut Expander { lake, all: &all, views, outside: &outside, depth: 0, own: 0 }) {
                return Err(e);
            }
        }
    }
    // DuckDB's and Snowflake's forms (`friendly.rs`): a text sent on as it was when none is in it
    let friendly = crate::friendly::rewrite(lake, &mut stmts).await?;
    if !expands && !friendly {
        return as_written();
    }
    Ok(text(&stmts))
}

/// `SHOW USER FUNCTIONS`, `SHOW PROCEDURES`, `SHOW TASKS`, `SHOW VIEWS`, `SHOW MATERIALIZED VIEWS`,
/// `SHOW SCHEMAS`, `SHOW DATABASES`, `SHOW SECRETS`, `SHOW USERS`, `SHOW ROLES`, `SHOW GRANTS`
/// (`[LIKE 'pattern']`): this lake's own, from `pondra.routines`, `pondra.tasks`, `pondra.tables`,
/// `information_schema.schemata`, `secrets()`, `pondra.users` and `pondra.grants`, as Snowflake has them. (`SHOW FUNCTIONS` is every function a query may call,
/// as DataFusion lists them: its own, and the Python ones; `SHOW TABLES` is DataFusion's.)
fn show(sql: &str) -> Option<String> {
    static SHOW: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*show\s+(user\s+functions|procedures|tasks|materialized\s+views|views|schemas|databases|secrets|users|roles|grants)(?:\s+like\s+('(?:[^']|'')*'))?\s*;?\s*$").expect("a regex"));
    let m = SHOW.captures(sql)?;
    let what = m[1].split_whitespace().map(str::to_lowercase).collect::<Vec<_>>().join(" ");
    let named = match what.as_str() { "schemas" => "schema_name", "databases" => "catalog_name", "grants" => "grantee", _ => "name" };
    let like = m.get(2).map(|l| format!(" AND {named} LIKE {}", l.as_str())).unwrap_or_default();
    Some(match what.as_str() {
        "schemas" => format!("SELECT catalog_name AS lake, schema_name AS name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog') AND schema_name NOT LIKE 'pg_temp%'{like} ORDER BY 1, 2"),
        "databases" => format!("SELECT DISTINCT catalog_name AS name FROM information_schema.schemata WHERE true{like} ORDER BY 1"),
        "secrets" => format!("SELECT * FROM secrets() WHERE true{like} ORDER BY name"),
        "users" => format!("SELECT * FROM pondra.users WHERE kind <> 'role'{like} ORDER BY name"),
        "roles" => format!("SELECT * FROM pondra.users WHERE kind = 'role'{like} ORDER BY name"),
        "grants" => format!("SELECT * FROM pondra.grants WHERE true{like} ORDER BY 1, 2"),
        "user functions" => format!("SELECT name, kind, language, arguments, returns, volatility FROM pondra.routines WHERE kind <> 'procedure'{like} ORDER BY name"),
        "procedures" => format!("SELECT name, language, arguments FROM pondra.routines WHERE kind = 'procedure'{like} ORDER BY name"),
        "tasks" => format!("SELECT * FROM pondra.tasks WHERE true{like} ORDER BY name"),
        "views" => format!("SELECT lake, schema, name, kind, definition FROM pondra.tables WHERE kind <> 'table'{like} ORDER BY 1, 2, 3"),
        _ => format!("SELECT lake, schema, name, key, definition FROM pondra.tables WHERE kind = 'materialized view'{like} ORDER BY 1, 2, 3"),
    })
}

/// A query that may start with FROM (DuckDB's `FROM t`, alone or as a subquery, a CTE or a view's).
static FROM_FIRST: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)(?:^|[(;]|\bas)\s*(?:(?:--[^\n]*\n|/\*.*?\*/)\s*)*from\b").expect("a regex"));

/// DuckDB's `FROM t` with no SELECT is `SELECT * FROM t` (DataFusion took it as no columns at all).
fn select_star(body: &mut ast::SetExpr) {
    match body {
        ast::SetExpr::Select(s) if s.flavor == ast::SelectFlavor::FromFirstNoSelect => {
            s.flavor = ast::SelectFlavor::Standard;
            if s.projection.is_empty() {
                s.projection = vec![ast::SelectItem::Wildcard(Default::default())];
            }
        }
        ast::SetExpr::SetOperation { left, right, .. } => {
            select_star(left);
            select_star(right);
        }
        ast::SetExpr::Query(q) => select_star(&mut q.body),
        _ => {}
    }
}

/// `sql` with its answers' columns named apart (`output_names`), if any needed it; else None, and
/// the text is left as written.
fn named_apart(sql: &str) -> Option<String> {
    static MAY: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)\bselect\b.*(?:::|\bcast\s*\(|,)").expect("a regex"));
    if sql.len() > 64 << 10 || !MAY.is_match(sql) && !VALUES_SUB.is_match(sql) {
        return None; // (a SELECT with more than one column or a cast, or VALUES with a subquery; not a long INSERT's rows)
    }
    struct Names(bool, usize);
    impl VisitorMut for Names {
        type Break = ();
        fn pre_visit_statement(&mut self, s: &mut Statement) -> ControlFlow<()> {
            self.1 = own_values(s);
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<()> {
            self.0 |= output_names(q) | values_apart(q, self.1);
            ControlFlow::Continue(())
        }
    }
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let mut names = Names(false, 0);
    for s in stmts.iter_mut().filter(|s| !matches!(s, Statement::CreateMacro { .. })) {
        let _ = s.visit(&mut names);
    }
    names.0.then(|| text(&stmts))
}

/// VALUES that may hold a subquery (`values_apart`).
static VALUES_SUB: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)\bvalues\b.*\bselect\b").expect("a regex"));

/// A VALUES list with a subquery in a row as `SELECT … AS column1, … UNION ALL SELECT …`, its
/// ORDER BY and LIMIT kept: DataFusion works a VALUES list's rows out while it plans it, before
/// any subquery has run ("ScalarSubqueryExpr evaluated before the subquery was executed"). True if
/// it was one. `own` is an INSERT's own VALUES (`own_values`), left as they are: `write.rs` takes
/// them through the log as VALUES, and `write::rows` sets them apart.
fn values_apart(q: &mut ast::Query, own: usize) -> bool {
    let ast::SetExpr::Values(v) = q.body.as_ref() else { return false };
    let sub = |e: &Expr| match e {
        Expr::Subquery(_) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    };
    if &*q.body as *const ast::SetExpr as usize == own || ast::visit_expressions(&v.rows, sub).is_continue() {
        return false;
    }
    let select = |row: &[Expr]| format!("SELECT {}", row.iter().enumerate().map(|(i, e)| format!("{e} AS column{}", i + 1)).collect::<Vec<_>>().join(", "));
    let Ok(union) = parse_query(&v.rows.iter().map(|r| select(&r.content)).collect::<Vec<_>>().join(" UNION ALL ")) else { return false };
    q.body = union.body;
    true
}

/// Where an INSERT's own VALUES are (`values_apart`), or 0.
fn own_values(s: &Statement) -> usize {
    match s {
        Statement::Insert(ast::Insert { source: Some(q), .. }) if matches!(*q.body, ast::SetExpr::Values(_)) => &*q.body as *const ast::SetExpr as usize,
        _ => 0,
    }
}

/// A row query (`write::rows`) with its VALUES set apart if they hold a subquery.
pub fn rows_apart(sql: &str) -> std::borrow::Cow<'_, str> {
    if !VALUES_SUB.is_match(sql) {
        return sql.into();
    }
    match parse_query(sql) {
        Ok(mut q) => match values_apart(&mut q, 0) {
            true => q.to_string().into(),
            false => sql.into(),
        },
        Err(_) => sql.into(),
    }
}

/// A SELECT's columns named as other engines name them, where DataFusion would refuse the query or
/// name one badly; true if any was. DataFusion names an unnamed cast of a column (`ts::date`,
/// `CAST(ts AS DATE)`) as the column, qualified (`sales.orders.ts`), and refuses two columns of one
/// name, which Postgres, DuckDB and Snowflake all take (`SELECT ts::date, *`, `SELECT id, *`). So:
/// - a cast of a column is named as the column (`ts`), as Postgres names it; or, when the SELECT
///   has another column that may have that name (a `*`, or another of its items), as written
///   (`ts::DATE`), as Snowflake and DuckDB name every cast;
/// - a column the SELECT has again (`SELECT id, *`, `SELECT id, id`) is named `id_1`, as DuckDB
///   names the second in a frame (only the item named can be: a `*`'s columns are the table's).
///
/// (A column the ORDER BY names qualified, `ORDER BY o.ts`, is one more of the SELECT's: DataFusion
/// adds it, so a cast of `ts` is then named as written too.)
fn output_names(q: &mut ast::Query) -> bool {
    let sorted: Vec<String> = match q.order_by.as_ref().map(|o| &o.kind) {
        Some(ast::OrderByKind::Expressions(all)) => all.iter().filter_map(|o| match &o.expr {
            Expr::CompoundIdentifier(ids) => ids.last().map(|i| if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() }),
            _ => None,
        }).collect(),
        _ => vec![],
    };
    select_names(&mut q.body, &sorted)
}

fn select_names(body: &mut ast::SetExpr, sorted: &[String]) -> bool {
    fn plain(i: &ast::Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } }
    // (a column's qualifier, if written, and name)
    fn column(e: &Expr) -> Option<(Option<String>, String)> {
        match e {
            Expr::Identifier(i) => Some((None, plain(i))),
            Expr::CompoundIdentifier(ids) if ids.len() > 1 => Some((Some(plain(&ids[ids.len() - 2])), plain(&ids[ids.len() - 1]))),
            _ => None,
        }
    }
    fn cast(e: &Expr) -> Option<String> {
        match e {
            Expr::Cast { expr, .. } => cast(expr).or_else(|| column(expr).map(|c| c.1)),
            _ => None,
        }
    }
    match body {
        ast::SetExpr::Select(s) => {
            use ast::SelectItem::{ExprWithAlias, QualifiedWildcard, UnnamedExpr, Wildcard};
            let star = s.projection.iter().any(|i| matches!(i, Wildcard(_)));
            let stars: Vec<String> = s.projection.iter().filter_map(|i| match i {
                QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::ObjectName(n), _) => n.0.last().and_then(|p| p.as_ident()).map(plain),
                _ => None,
            }).collect();
            let named = |i: &ast::SelectItem| match i {
                UnnamedExpr(e) => column(e).map(|c| c.1).or_else(|| cast(e)),
                ExprWithAlias { alias, .. } => Some(plain(alias)),
                _ => None,
            };
            let mut names: Vec<Option<String>> = s.projection.iter().map(named).collect();
            let mut changed = false;
            for k in 0..s.projection.len() {
                let UnnamedExpr(e) = &s.projection[k] else { continue };
                let Some(n) = names[k].clone() else { continue };
                let again = |j: usize| j != k && names[j].as_deref() == Some(n.as_str());
                let name = match (cast(e).is_some(), column(e)) {
                    (true, _) if star || !stars.is_empty() || (0..names.len()).any(again) || sorted.contains(&n) => e.to_string(),
                    (true, _) => n.clone(),
                    // (a `*` leaves out the system columns, and a keyed table's `_deleted`: `sys::hide`)
                    (false, Some((q, _))) if ((star || q.as_ref().is_some_and(|q| stars.contains(q))) && !crate::sys::NAMES.contains(&n.as_str()) && n != "_deleted") || (0..k).any(again) => {
                        (1..).map(|i| format!("{n}_{i}")).find(|m| !names.iter().any(|x| x.as_deref() == Some(m.as_str()))).expect("a free name")
                    }
                    _ => continue,
                };
                names[k] = Some(name.clone());
                s.projection[k] = ExprWithAlias { expr: e.clone(), alias: ast::Ident::with_quote('"', name) };
                changed = true;
            }
            changed
        }
        ast::SetExpr::SetOperation { left, right, .. } => select_names(left, sorted) | select_names(right, sorted),
        ast::SetExpr::Query(q) => output_names(q),
        _ => false,
    }
}

/// A scalar SQL function's body as an expression: an expression (`RETURN x * 2`), a query's one
/// value (`AS $$ SELECT x * 2 $$`), or a scalar subquery (`AS $$ SELECT max(v) FROM t WHERE k =
/// x $$`).
fn scalar_body(body: &str) -> Result<Expr> {
    let text = query_text(body);
    let first = text.split(|c: char| !c.is_alphanumeric()).next().unwrap_or_default().to_lowercase();
    if !["select", "with", "values"].contains(&first.as_str()) {
        return parse_expr(text);
    }
    let q = parse_query(text)?;
    if let ast::SetExpr::Select(s) = &*q.body {
        if let [ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. }] = &s.projection[..] {
            if q.to_string() == format!("SELECT {}", s.projection[0]) {
                return Ok(e.clone()); // (nothing but the value)
            }
        }
    }
    Ok(Expr::Subquery(Box::new(q)))
}

/// A body's statement without the `;` that may end it.
fn query_text(body: &str) -> &str { body.trim().trim_end_matches(';').trim_end() }

struct Expander<'a> {
    lake: &'a Lake,
    all: &'a HashMap<String, Routine>,
    views: &'a HashMap<String, String>, // (a request's own: `FROM name` is its query)
    outside: &'a [(String, crate::ext::Attached)], // other engines' tables attached (`ext.rs`)
    depth: usize,
    own: usize, // (an INSERT's own VALUES: `values_apart`)
}

impl Expander<'_> {
    fn find(&self, name: &ast::ObjectName, kind: Kind) -> Option<(String, &Routine)> {
        let name = crate::ddl::local(self.lake, &object(name))?;
        self.all.get(&name).filter(|r| r.kind == kind).map(|r| (name, r))
    }

    /// The body with the arguments in its parameters' places (`p`, or `$1`), cast to their types,
    /// its own function calls expanded; and the arguments.
    fn call<T: VisitMut>(&self, name: &str, r: &Routine, args: &[FunctionArg], mut body: T) -> Result<(T, Vec<Expr>)> {
        ensure!(self.depth < 16, "{name}: functions calling functions 16 deep (a loop?)");
        let values = typed(r, arguments(name, r, args)?)?;
        let _ = visit_expressions_mut(&mut body, |e| {
            let v = match e {
                Expr::Identifier(i) => values.get(&ident(i)),
                Expr::Value(v) => match &v.value {
                    ast::Value::Placeholder(p) => match p.trim_start_matches('$') {
                        n if n.starts_with(|c: char| c.is_ascii_digit()) => n.parse::<usize>().ok().and_then(|n| r.params.get(n.wrapping_sub(1))).and_then(|p| values.get(&p.name)),
                        n => values.get(&n.to_lowercase()), // (`$x`: the parameter `x`, as everywhere else)
                    },
                    _ => None,
                },
                _ => None,
            };
            if let Some(v) = v {
                *e = Expr::Nested(Box::new(v.clone()));
            }
            ControlFlow::<()>::Continue(())
        });
        match body.visit(&mut Expander { depth: self.depth + 1, ..*self }) {
            ControlFlow::Break(e) => Err(e),
            ControlFlow::Continue(()) => Ok((body, r.params.iter().map(|p| values[&p.name].clone()).collect())),
        }
    }
}

/// Each argument cast to its parameter's type, if it has one.
fn typed(r: &Routine, values: HashMap<String, Expr>) -> Result<HashMap<String, Expr>> {
    let ty: HashMap<&str, &str> = r.params.iter().filter(|p| !crate::pyfn::loose(p.ty.as_deref())).filter_map(|p| Some((p.name.as_str(), p.ty.as_deref()?))).collect();
    values.into_iter().map(|(k, v)| Ok(match ty.get(k.as_str()) {
        Some(t) => (k.clone(), parse_expr(&format!("CAST(({v}) AS {t})"))?),
        None => (k, v),
    })).collect()
}

/// A Python function's call as its DataFusion function takes it: every argument, in order, cast
/// to its parameter's type, under the name it is registered by.
fn whole(name: &str, r: &Routine, args: &[FunctionArg]) -> Result<(ast::ObjectName, Vec<FunctionArg>)> {
    let values = typed(r, arguments(name, r, args)?)?;
    let args = r.params.iter().map(|p| FunctionArg::Unnamed(FunctionArgExpr::Expr(values[&p.name].clone()))).collect();
    let name = match r.kind {
        Kind::Table => ast::Ident::new(name), // (DataFusion looks a table function up by its name as written)
        _ => ast::Ident::with_quote('"', name),
    };
    Ok((ast::ObjectName::from(vec![name]), args))
}

impl VisitorMut for Expander<'_> {
    type Break = anyhow::Error;

    fn pre_visit_statement(&mut self, s: &mut Statement) -> ControlFlow<Self::Break> {
        self.own = own_values(s);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<Self::Break> {
        select_star(&mut q.body);
        output_names(q);
        values_apart(q, self.own);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(f) = expr else { return ControlFlow::Continue(()) };
        let Some((name, r)) = self.find(&f.name, Kind::Macro) else { return ControlFlow::Continue(()) };
        let args = match &f.args {
            FunctionArguments::List(l) => l.args.clone(),
            _ => vec![],
        };
        if r.python() {
            return match whole(&name, r, &args) {
                Ok((n, args)) => {
                    f.name = n;
                    f.args = FunctionArguments::List(ast::FunctionArgumentList { duplicate_treatment: None, args, clauses: vec![] });
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(e),
            };
        }
        let expanded = scalar_body(&r.body).and_then(|body| self.call(&name, r, &args, body)).and_then(|(e, args)| {
            let e = match &r.returns {
                Some(t) => format!("CAST(({e}) AS {t})"),
                None => e.to_string(),
            };
            let e = match (r.with.strict, args.is_empty()) {
                (true, false) => format!("CASE WHEN {} THEN NULL ELSE {e} END", args.iter().map(|a| format!("({a}) IS NULL")).collect::<Vec<_>>().join(" OR ")),
                _ => e,
            };
            parse_expr(&e)
        });
        match expanded {
            Ok(e) => *expr = Expr::Nested(Box::new(e)),
            Err(e) => return ControlFlow::Break(e),
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
        if let (TableFactor::Table { name, args: None, .. }, false) = (&mut *t, self.outside.is_empty()) {
            let parts: Vec<String> = object(name).split('.').map(str::to_string).collect();
            match crate::ext::attached_table(self.outside, &parts) {
                Ok(Some(files)) => {
                    *name = ast::ObjectName::from(vec![ast::Ident::with_quote('"', files)]); // (another engine's table: `ext.rs`)
                    return ControlFlow::Continue(());
                }
                Err(e) => return ControlFlow::Break(e),
                Ok(None) => {}
            }
        }
        match crate::past::table_factor(self.lake, t) {
            Ok(true) => return ControlFlow::Continue(()),
            Err(e) => return ControlFlow::Break(e),
            Ok(false) => {}
        }
        match crate::ext::table(t) {
            Ok(Some(files)) => {
                if let TableFactor::Table { name, args, .. } = t {
                    (*name, *args) = (ast::ObjectName::from(vec![ast::Ident::with_quote('"', files)]), None); // (files anywhere: `ext.rs`)
                }
                return ControlFlow::Continue(());
            }
            Err(e) => return ControlFlow::Break(e),
            Ok(None) => {}
        }
        if let TableFactor::Table { name, alias, args: None, .. } = t {
            let Some(sql) = self.views.get(&object(name)) else { return ControlFlow::Continue(()) };
            static NONE: std::sync::LazyLock<HashMap<String, String>> = std::sync::LazyLock::new(HashMap::new);
            let mut q = match parse_query(sql) {
                Ok(q) => q,
                Err(e) => return ControlFlow::Break(e.context(format!("{name}"))),
            };
            q.visit(&mut Expander { views: &NONE, depth: self.depth + 1, ..*self })?; // (its functions; its names are the lake's)
            let alias = alias.clone().or_else(|| Some(ast::TableAlias { explicit: true, name: ast::Ident::new(object(name)), columns: vec![], at: None }));
            *t = TableFactor::Derived { lateral: false, subquery: Box::new(q), alias, sample: None };
            return ControlFlow::Continue(());
        }
        let TableFactor::Table { name, alias, args: Some(args), .. } = t else { return ControlFlow::Continue(()) };
        let Some((found, r)) = self.find(name, Kind::Table) else { return ControlFlow::Continue(()) };
        if r.python() {
            return match whole(&found, r, &args.args) {
                Ok((n, a)) => {
                    (*name, args.args) = (n, a);
                    if alias.is_none() {
                        *alias = Some(ast::TableAlias { explicit: true, name: ast::Ident::new(crate::ddl::split(&found).1), columns: vec![], at: None });
                    }
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(e),
            };
        }
        let q = parse_query(query_text(&r.body)).and_then(|body| self.call(&found, r, &args.args, body)).and_then(|(q, _)| match &r.returns {
            // (RETURNS TABLE: its columns, by place, as their types)
            Some(ret) => {
                let cols = columns_of(ret)?;
                let quoted = |c: &str| ast::Ident::with_quote('"', c).to_string();
                let select = cols.iter().map(|(c, t)| format!("CAST({} AS {t}) AS {}", quoted(c), quoted(c))).collect::<Vec<_>>().join(", ");
                parse_query(&format!("SELECT {select} FROM ({q}) AS \"_f\"({})", cols.iter().map(|(c, _)| quoted(c)).collect::<Vec<_>>().join(", ")))
            }
            None => Ok(q),
        });
        let q = match q {
            Ok(q) => q,
            Err(e) => return ControlFlow::Break(e),
        };
        let alias = alias.clone().or_else(|| Some(ast::TableAlias { explicit: true, name: ast::Ident::new(crate::ddl::split(&found).1), columns: vec![], at: None }));
        *t = TableFactor::Derived { lateral: false, subquery: Box::new(q), alias, sample: None };
        ControlFlow::Continue(())
    }
}

/// Each parameter's argument: by position, then by name (`rate := 0.3`, `rate => 0.3`), then its
/// default.
fn arguments(name: &str, r: &Routine, args: &[FunctionArg]) -> Result<HashMap<String, Expr>> {
    let mut out = HashMap::new();
    for (i, a) in args.iter().enumerate() {
        let (p, e) = match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => (r.params.get(i).map(|p| p.name.clone()).with_context(|| format!("{name} takes {} arguments", r.params.len()))?, e),
            FunctionArg::Named { name: n, arg: FunctionArgExpr::Expr(e), .. } => (ident(n), e),
            _ => bail!("{name}: an argument is an expression, or name := expression"),
        };
        ensure!(r.params.iter().any(|q| q.name == p), "{name} has no parameter {p}");
        ensure!(out.insert(p.clone(), e.clone()).is_none(), "{name}: {p} given twice");
    }
    for p in &r.params {
        if !out.contains_key(&p.name) {
            let d = p.default.as_deref().with_context(|| format!("{name}: no value for {}", p.name))?;
            out.insert(p.name.clone(), parse_expr(d)?);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------- running them

/// A request to `POST /sql`: its statements, `$name` parameters, and tables sent along with it.
#[derive(Default)]
pub struct Request {
    pub sql: String,
    pub params: HashMap<String, Value>,
    pub views: HashMap<String, String>, // (a client's frames, by name: `FROM name` is the query)
    pub tables: Vec<(String, Vec<RecordBatch>)>,
}

impl Request {
    /// The body as it came: SQL; JSON (`{"sql": …, "params": {…}}`); or, with tables of its own,
    /// `application/vnd.pondra.request`: the JSON's length (4 bytes, little-endian), the JSON
    /// (`"tables"`: their names), then each table as its length (8 bytes) and an Arrow IPC stream.
    pub fn read(kind: &str, body: &[u8]) -> Result<Request> {
        #[derive(Deserialize)]
        struct Head {
            sql: String,
            #[serde(default)]
            params: HashMap<String, Value>,
            #[serde(default)]
            views: HashMap<String, String>,
            #[serde(default)]
            tables: Vec<String>,
        }
        let short = || anyhow::anyhow!("the request ends too soon");
        let (head, mut rest): (Head, &[u8]) = match kind.split(';').next().unwrap_or_default().trim() {
            "application/json" => (serde_json::from_slice(body)?, &[]),
            "application/vnd.pondra.request" => {
                let n = u32::from_le_bytes(body.get(..4).ok_or_else(short)?.try_into()?) as usize;
                (serde_json::from_slice(body.get(4..4 + n).ok_or_else(short)?)?, &body[4 + n..])
            }
            _ => return Ok(Request { sql: String::from_utf8(body.to_vec())?, ..Default::default() }),
        };
        let mut tables = vec![];
        for name in head.tables {
            let n = u64::from_le_bytes(rest.get(..8).ok_or_else(short)?.try_into()?) as usize;
            tables.push((name, crate::query::read_ipc(rest.get(8..8 + n).ok_or_else(short)?)?));
            rest = &rest[8 + n..];
        }
        Ok(Request { sql: head.sql, params: head.params, views: head.views.into_iter().map(|(k, v)| (k.to_lowercase(), v)).collect(), tables })
    }
}

/// Who a statement runs for: the caller's rights (`files`: it may read files on this machine,
/// `server::owner`), and how deep in procedures calling procedures it is.
#[derive(Clone, Copy)]
pub struct Who {
    pub role: Role,
    pub files: bool,
    pub depth: u32,
}

pub enum Outcome {
    Rows(Vec<RecordBatch>),
    Done(Value), // a write's or DDL's answer
}

/// The complete statements of `text` — each ends at a `;` outside strings (`'…'`, `$$…$$`,
/// `$tag$…$tag$`), quoted names and comments, a block's at its `END`'s (`script::joined`) — and what
/// follows the last one, if it holds more than whitespace and comments (the shell waits for its `;`).
pub fn statements(text: &str) -> (Vec<String>, String) {
    let (out, rest) = pieces(text);
    match crate::script::joined(out) {
        (out, None) => (out, rest),
        (out, Some(open)) => (out, format!("{open};{rest}")), // (a block not closed yet: what follows it)
    }
}

/// `statements`, each `;` a statement's end (a block's statements apart).
fn pieces(text: &str) -> (Vec<String>, String) {
    let (mut out, mut start, mut i, mut code) = (vec![], 0, 0, false);
    while i < text.len() {
        let rest = &text[i..];
        let quoted = |end: &str, from: usize| rest[from..].find(end).map_or(rest.len(), |e| from + e + end.len());
        let (skip, is_code) = match rest.as_bytes()[0] {
            b'-' if rest.starts_with("--") => (quoted("\n", 2), false),
            b'/' if rest.starts_with("/*") => (quoted("*/", 2), false),
            b'\'' => (quoted("'", 1), true),
            b'"' => (quoted("\"", 1), true),
            b'$' => match dollar_tag(rest) {
                Some(tag) => (quoted(tag, tag.len()), true),
                None => (1, true),
            },
            b';' => {
                if code {
                    out.push(text[start..i].to_string());
                }
                (start, code) = (i + 1, false);
                (1, false)
            }
            _ => {
                let c = rest.chars().next().expect("not empty");
                (c.len_utf8(), !c.is_whitespace())
            }
        };
        code |= is_code;
        i += skip.min(rest.len());
    }
    (out, if code { text[start..].to_string() } else { String::new() })
}

/// A script's statements, the last one's `;` optional.
pub fn split(text: &str) -> Vec<String> {
    let (mut out, rest) = statements(text);
    if !rest.is_empty() {
        out.push(rest);
    }
    out
}

/// `$$` or `$tag$` where a dollar-quoted string starts (not `$1` or `$name`: parameters).
pub(crate) fn dollar_tag(s: &str) -> Option<&str> {
    let end = s[1..].find('$')? + 2;
    let inner = &s[1..end - 1];
    (inner.chars().all(|c| c.is_alphanumeric() || c == '_') && !inner.starts_with(|c: char| c.is_ascii_digit())).then(|| &s[..end])
}

/// Run one statement here as the caller could have sent it (the audit log told: `audit.rs`).
pub async fn one(app: &App, sql: &str, who: Who, job: Option<String>) -> Result<Outcome> {
    if let Some(word) = crate::txn::control(sql) {
        // BEGIN, COMMIT, ROLLBACK: the session's transaction (ADR-036 §5)
        let (tag, warning) = crate::audit::statement(app, sql, Box::pin(crate::txn::command(app, word))).await?; // (on the heap: procedures call procedures deep)
        warning.iter().for_each(|w| heard(&format!("WARNING: {w}")));
        return Ok(Outcome::Done(j!({"transaction": tag.to_lowercase()})));
    }
    crate::txn::refuse()?; // (a failed transaction takes nothing but its end)
    let out = crate::audit::statement(app, sql, one_of(app, sql, who, job)).await;
    if let Err(e) = &out {
        crate::txn::failed(&crate::ext::said(e)); // (in a transaction: it fails with the statement)
    }
    out
}

async fn one_of(app: &App, sql: &str, who: Who, job: Option<String>) -> Result<Outcome> {
    if crate::script::is(sql) {
        return Box::pin(crate::script::run(app, sql, &HashMap::new(), who, job)).await; // (a block, a branch, a loop, PRINT…: ADR-045)
    }
    if let Some(c) = crate::vars::change(sql) {
        return Box::pin(crate::vars::apply(app, c)).await; // (DECLARE $day …, $day = …: the scope's variable)
    }
    if crate::write::checkpoint(sql) {
        ensure!(who.role >= Role::Write, "CHECKPOINT needs a write token");
        return Ok(Outcome::Done(app.checkpoint().await?));
    }
    if let Some((name, args)) = call_of(sql) {
        if crate::workspace::is_run(&name) {
            return Box::pin(crate::workspace::run(app, &args, who, job, None)).await; // (a file of the lake's: ADR-033)
        }
        let (local, r, row) = Box::pin(prepared(app, &name, &args, who)).await?;
        return Box::pin(run(app, local, r, row, who, job, None)).await;
    }
    if let Some((name, args, column)) = start_of(sql) {
        return start(app, &name, &args, &column, who, job).await;
    }
    if let Some((language, body)) = do_of(sql) {
        // A procedure made, called once and forgotten, as its caller: an admin's, as making one is.
        ensure!(who.role >= Role::Admin, "DO runs code on the node: it needs an admin token, as making a procedure does");
        let r = match language.as_str() {
            "python" => Routine { kind: Kind::Procedure, params: vec![], language, body, returns: None, with: Options::default() },
            "sql" => Routine { kind: Kind::Procedure, params: vec![], language, body, returns: None, with: Options::default() },
            l => bail!("DO LANGUAGE {l}: Pondra's code blocks are LANGUAGE python (a console's Python cell) or LANGUAGE sql"),
        };
        let none = RecordBatch::try_new_with_options(Arc::new(datafusion::arrow::datatypes::Schema::empty()), vec![], &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)))?;
        return Box::pin(run(app, "do".into(), r, none, who, job, None)).await;
    }
    if let Some((name, args)) = crate::runs::execute_of(sql) {
        return Box::pin(crate::runs::execute(app, name, args, who)).await; // (EXECUTE TASK: a tick now, ADR-045)
    }
    if crate::settings::is(sql) {
        // SET, RESET, PREPARE, EXECUTE, DEALLOCATE: the session's (round 31)
        return match Box::pin(crate::settings::statement(&app.lake, sql)).await? {
            crate::settings::Done::Said(tag) => Ok(Outcome::Done(j!({"command": tag}))),
            crate::settings::Done::Run(sql) => Box::pin(one_of(app, &sql, who, job)).await,
        };
    }
    if let Some(stmt) = crate::write::parse(sql) {
        app.auth.allows(who.role, &stmt)?;
        return Ok(Outcome::Done(crate::write::on_node_as(app, stmt, job, who.files).await?));
    }
    if let Some(rows) = Box::pin(crate::txn::point_read(&app.lake, sql)).await? {
        return Ok(Outcome::Rows(rows)); // (a key lookup in a transaction: no planning)
    }
    Ok(Outcome::Rows(match who.files {
        true => app.query_as(&crate::asof::rewrite(sql)?, Some("0"), true).await?, // (a file here: this node only)
        false => app.query(sql, None).await?,
    }))
}

/// A script's statements, each in turn; the last one's outcome. With a `job`, each gets its own
/// (`{job}:{i}`): the script run again with the same job applies each write once.
/// `params` are the values it was given (`vars.rs`): `$name` is one until something sets it, and a
/// `DECLARE` takes one in place of its default.
pub async fn script(app: &App, sql: &str, params: &HashMap<String, Value>, views: &HashMap<String, String>, who: Who, job: Option<String>) -> Result<Outcome> {
    let go = script_of(app, sql, views, who, job);
    match crate::vars::in_run() && params.is_empty() {
        true => go.await, // (a file run's cell, a lent connection's request: the run's values)
        false => crate::vars::run(params.clone(), go).await, // (with no session, a DECLARE says it needs one)
    }
}

async fn script_of(app: &App, sql: &str, views: &HashMap<String, String>, who: Who, job: Option<String>) -> Result<Outcome> {
    crate::script::run(app, sql, views, who, job).await // (each statement prepared and run by `one`, blocks as they say)
}

/// `CALL name(…)`: the procedure's name and arguments, if `sql` is one.
pub fn call_of(sql: &str) -> Option<(String, Vec<FunctionArg>)> {
    if !crate::write::first_word(sql).get(..4)?.eq_ignore_ascii_case("call") {
        return None;
    }
    match Parser::parse_sql(&GenericDialect {}, sql).ok()?.pop()? {
        Statement::Call(f) => Some((object(&f.name), match f.args {
            FunctionArguments::List(l) => l.args,
            _ => vec![],
        })),
        _ => None,
    }
}

/// `START CALL name(…)`, or `SELECT pondra.start('name', …) [AS column]`: a procedure to start
/// without waiting, its arguments, and the answer's column (`run`).
pub fn start_of(sql: &str) -> Option<(String, Vec<FunctionArg>, String)> {
    static START: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^start\s+(call\b.*)$").expect("a regex"));
    if let Some(call) = START.captures(crate::write::first_word(sql)) {
        let (name, args) = call_of(call.get(1)?.as_str())?;
        return Some((name, args, "run".into()));
    }
    if !sql.to_lowercase().contains("pondra.start") {
        return None;
    }
    let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, sql).ok()?.pop()? else { return None };
    let ast::SetExpr::Select(s) = &*q.body else { return None };
    let (f, column) = match &s.projection[..] {
        [ast::SelectItem::UnnamedExpr(Expr::Function(f))] => (f, "run".to_string()),
        [ast::SelectItem::ExprWithAlias { expr: Expr::Function(f), alias }] => (f, ident(alias)),
        _ => return None,
    };
    let FunctionArguments::List(l) = &f.args else { return None };
    let (Some((FunctionArg::Unnamed(FunctionArgExpr::Expr(first)), rest)), "pondra.start", true) = (l.args.split_first(), object(&f.name).as_str(), s.from.is_empty()) else { return None };
    let mut first = first;
    while let Expr::Nested(e) = first {
        first = e; // (a parameter: `pondra.start($name, …)`)
    }
    match first {
        Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(name) => Some((name.clone(), rest.to_vec(), column)),
            _ => None,
        },
        _ => None,
    }
}

/// Does `sql` run a procedure (`CALL`, `pondra.start`)? Then it goes to `one`, not to a query.
pub fn runs_procedure(sql: &str) -> bool { call_of(sql).is_some() || start_of(sql).is_some() || do_of(sql).is_some() || crate::script::is(sql) || crate::runs::execute_of(sql).is_some() }

/// `DO LANGUAGE python $$ … $$` (Postgres's anonymous code block; the language may come after
/// the code): (language, body). What a console's Python cell sends (ADR-030).
pub fn do_of(sql: &str) -> Option<(String, String)> {
    static DO: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?is)^DO\s+(?:LANGUAGE\s+(\w+)\s+)?\$(\w*)\$(.*)\$(\w*)\$\s*(?:LANGUAGE\s+(\w+))?\s*;?\s*$").expect("a regex")
    });
    let c = DO.captures(crate::write::first_word(sql))?;
    let body = c[3].strip_prefix("\r\n").or_else(|| c[3].strip_prefix('\n')).unwrap_or(&c[3]); // (line 1 is the code's first line)
    (c[2] == c[4]).then(|| (c.get(1).or(c.get(5)).map_or("plpgsql", |m| m.as_str()).to_lowercase(), body.to_string()))
}

/// A procedure and its arguments, worked out once, as the caller (`CALL p(now())`: one moment for
/// every statement), cast to their parameters' types.
async fn prepared(app: &App, name: &str, args: &[FunctionArg], who: Who) -> Result<(String, Routine, RecordBatch)> {
    ensure!(who.depth < 16, "{name}: procedures calling procedures 16 deep (a loop?)");
    let local = crate::ddl::local(&app.lake, name).with_context(|| format!("no procedure {name}"))?;
    let r = listed(&app.lake).await?.get(&local).filter(|r| r.kind == Kind::Procedure).cloned().with_context(|| format!("no procedure {name}"))?;
    let values = arguments(name, &r, args)?;
    let select = r.params.iter().map(|p| match &p.ty {
        Some(t) if !crate::pyfn::loose(Some(t)) => format!("CAST(({}) AS {t}) AS \"{}\"", values[&p.name], p.name),
        _ => format!("({}) AS \"{}\"", values[&p.name], p.name), // (any value, as it is)
    });
    let row = match r.params.is_empty() {
        true => RecordBatch::new_empty(Arc::new(datafusion::arrow::datatypes::Schema::empty())),
        false => {
            let rows = app.query(&format!("SELECT {}", select.collect::<Vec<_>>().join(", ")), Some("0")).await.with_context(|| format!("{name}'s arguments"))?;
            datafusion::arrow::compute::concat_batches(&rows[0].schema(), &rows)?
        }
    };
    Ok((local, r, row))
}

/// Run a procedure, logged (`pondra.runs`): its statements, or its Python on a worker.
async fn run(app: &App, name: String, r: Routine, row: RecordBatch, who: Who, job: Option<String>, id: Option<String>) -> Result<Outcome> {
    let log = match name.as_str() {
        // (a DO block's code is what says what it was: Runs shows its first line)
        "do" => crate::runs::Run::begin(app, &name, who.role, job.as_deref(), serde_json::json!({"language": r.language, "code": r.body}).to_string(), id),
        _ => crate::runs::Run::start(app, &name, who.role, job.as_deref(), &row, id),
    };
    let inner = Who { depth: who.depth + 1, ..who };
    let mut heard = vec![];
    let out = match (r.python(), name == "do") {
        (true, true) => Box::pin(python(app, &name, &r, row, inner, job, &mut heard)).await, // (a session's DO: the session's variables)
        (true, false) => Box::pin(crate::vars::own(HashMap::new(), python(app, &name, &r, row, inner, job, &mut heard))).await,
        (false, _) => {
            let mut values = values_of(&row)?;
            for (i, p) in r.params.iter().enumerate() {
                values.insert((i + 1).to_string(), values[&p.name].clone()); // ($1: the first)
            }
            Box::pin(crate::vars::own(values, script(app, &r.body, &HashMap::new(), &HashMap::new(), inner, job))).await // (a procedure's variables are its own)
        }
    };
    let _ = log.end(app, &out, heard); // (written a moment later: a call doesn't wait for its log)
    out
}

/// `SELECT pondra.start('p', …)`: the procedure started, not waited for; its run's id, to look
/// for in `pondra.runs`.
fn start<'a>(app: &'a App, name: &'a str, args: &'a [FunctionArg], column: &'a str, who: Who, job: Option<String>) -> futures::future::BoxFuture<'a, Result<Outcome>> {
    Box::pin(async move {
        let id = crate::runs::new_id();
        let (app2, id2) = (app.clone(), id.clone());
        if crate::workspace::is_run(name) {
            let args = args.to_vec();
            crate::workspace::arguments(app, &args).await?; // (its mistakes: said now, not only in the log)
            tokio::spawn(async move {
                let _ = Box::pin(crate::workspace::run(&app2, &args, who, job, Some(id2))).await;
            });
        } else {
            let (local, r, row) = Box::pin(prepared(app, name, args, who)).await?;
            tokio::spawn(async move {
                let _ = Box::pin(run(&app2, local, r, row, who, job, Some(id2))).await; // (its outcome: the run log's)
            });
        }
        let ids = datafusion::arrow::array::StringArray::from(vec![id]);
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![datafusion::arrow::datatypes::Field::new(column, datafusion::arrow::datatypes::DataType::Utf8, false)]));
        Ok(Outcome::Rows(vec![RecordBatch::try_new(schema, vec![Arc::new(ids)])?]))
    })
}

/// The arguments as parameters for SQL: each value exactly, at its own type.
pub fn values_of(row: &RecordBatch) -> Result<HashMap<String, Value>> {
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    let mut out = HashMap::new();
    for (f, col) in row.schema().fields().iter().zip(row.columns()) {
        let v = match col.is_null(0) {
            true => "NULL".to_string(),
            false => quote(&ArrayFormatter::try_new(col.as_ref(), &FormatOptions::default())?.value(0).to_string()),
        };
        out.insert(f.name().clone(), j!({"sql": format!("arrow_cast({v}, '{}')", f.data_type())}));
    }
    Ok(out)
}

tokio::task_local! {
    /// What the procedures a request calls print, for its caller (`with_notices`).
    static NOTICES: Arc<Mutex<Vec<String>>>;
}

/// A notice for the request being answered (what a procedure or a file run printed).
pub fn heard(n: &str) { let _ = NOTICES.try_with(|all| all.lock().unwrap().push(n.to_string())); }

/// Run `f`, and collect the notices its procedures send (what they print): the Postgres port sends
/// them as NOTICE, HTTP as the `x-pondra-notices` header, MCP with the tool's answer.
pub async fn with_notices<F: std::future::Future>(f: F) -> (F::Output, Vec<String>) {
    let heard = Arc::new(Mutex::new(vec![]));
    let out = NOTICES.scope(heard.clone(), f).await;
    let heard = std::mem::take(&mut *heard.lock().unwrap());
    (out, heard)
}

/// A Python procedure, on a worker of this node's (`python.rs`): handed its body, the arguments
/// (Arrow) and a connection back here with the caller's rights. Its notices come as it prints;
/// then the answer: rows, a frame's SQL (run here), or nothing. Secrets it read are blanked out of
/// what it says (notices, errors).
async fn python(app: &App, name: &str, r: &Routine, args: RecordBatch, who: Who, job: Option<String>, heard: &mut Vec<String>) -> Result<Outcome> {
    crate::python::ready(&match name {
        "do" => "DO LANGUAGE python (a console's Python cell) runs Python on the node".to_string(),
        _ => format!("{name} is a Python procedure"),
    })?;
    let lease = crate::auth::lend(who.role, who.files); // (ends when this does)
    let url = format!("http://{}", app.cluster.addr.replace("0.0.0.0", "127.0.0.1"));
    let json: Vec<bool> = r.params.iter().map(|p| crate::pyfn::is_json(p.ty.as_deref())).collect();
    let head = j!({"op": "call", "name": name, "body": r.body, "entry": r.with.entry, "params": names(r), "json": json, "url": url, "token": lease.0, "depth": who.depth, "job": job});
    let limit = r.with.timeout.map(std::time::Duration::from_secs_f64);
    let kind = crate::python::Use::Procedure { nested: who.depth > 1 }; // (called by a procedure: it holds a worker already)
    let mut notice = |n: String| {
        let n = lease.redact(&n);
        let _ = NOTICES.try_with(|all| all.lock().unwrap().push(n.clone()));
        heard.push(n);
    };
    let whose = if name == "do" { String::new() } else { format!("{name}: ") }; // (a DO block has no name)
    let parts = vec![crate::query::ipc(&[args])?];
    // A DO block its caller sends in a session (the console's cells): on the session's own worker,
    // in its namespace (`python::ask_session`). Not one a procedure sends: that would wait for the
    // cell that is running it.
    let asked = match (name, crate::temp::current()) {
        ("do", Some(session)) if who.depth == 1 && !session.contains("#script-") => { // (not a script's own: a session of the moment)
            let mut head = head;
            head["op"] = j!("cell");
            head["session"] = j!(session);
            crate::python::ask_session(&session, head, parts, limit, &mut notice).await
        }
        _ => crate::python::ask(&r.with.packages, kind, head, parts, limit, &mut notice).await,
    };
    let (answer, parts) = asked.map_err(|e| anyhow::anyhow!("{whose}{}", lease.redact(&format!("{e:#}"))))?;
    match answer["kind"].as_str() {
        Some("rows") => Ok(Outcome::Rows(crate::query::read_ipc(parts.first().context("no rows")?)?)),
        Some("images") => Ok(Outcome::Done(j!({"called": name, "images": answer["images"]}))), // (a cell's figures, as PNG: the console shows them)
        Some("sql") => Box::pin(one(app, answer["sql"].as_str().unwrap_or_default(), who, None)).await,
        _ => Ok(Outcome::Done(j!({"called": name}))),
    }
}

/// Does `sql` call a Python function that may answer differently each time (so its result mustn't
/// be remembered)?
pub async fn volatile(lake: &Lake, sql: &str) -> bool {
    let Ok(all) = listed(lake).await else { return true };
    all.iter().any(|(n, r)| r.kind != Kind::Procedure && !r.cacheable() && crate::ddl::mentions(sql, n))
}

/// Does `sql` read a Python table function? Then it runs on one node: each would call it.
pub async fn pinned(lake: &Lake, sql: &str) -> bool {
    let Ok(all) = listed(lake).await else { return false };
    all.iter().any(|(n, r)| r.kind == Kind::Table && r.python() && crate::ddl::mentions(sql, n))
}
