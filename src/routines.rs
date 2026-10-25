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
use crate::write::Stmt;
use crate::text::{ident, object};
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::RecordBatch;
use datafusion::sql::sqlparser::ast::{self, visit_expressions_mut, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Statement, VisitMut, VisitorMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::Token};
use serde::Deserialize;
use serde_json::{json as j, Value};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock, Mutex};
use crate::text::text;
use crate::expander::{columns_of, names, Kind, Routine};
use crate::expander::{arguments, is_query, params, parse_expr, parse_query, query_text, scalar_body, values_apart, Options, Param, VALUES_SUB};

pub fn key(name: &str) -> String { format!("r/{name}") }

// ---------------------------------------------------------------- statements

/// `CREATE [OR REPLACE] MACRO …`, as sqlparser reads it.
pub fn of_macro(args: &Option<Vec<ast::MacroArg>>, def: &ast::MacroDefinition) -> Routine {
    let params = args.iter().flatten().map(|a| Param { name: ident(&a.name), ty: None, default: a.default_expr.as_ref().map(|e| e.to_string()) }).collect();
    let (kind, body) = match def {
        ast::MacroDefinition::Expr(e) => (Kind::Macro, e.to_string()),
        ast::MacroDefinition::Table(q) => (Kind::Table, q.to_string()),
    };
    grouped(Routine { kind, params, language: String::new(), body, returns: None, with: Options::default() })
}

/// A SQL function's body with its JSON operators grouped as Postgres groups them: the rule a view's query gets from
/// `path::door` before it is stored (`pg_json::text`). It is done where the routine is read, so the routine a statement
/// carries (`ddl` waits for it to be kept) is the one kept. A query's body is grouped as text; an expression's
/// (`RETURN d ->> 'a' = '1'`, which the parser reads as `d ->> ('a' = '1')`) as one (`pg_json::expression`). A
/// procedure's body isn't: its statements go through the door as they run. Python's code is its own.
pub fn grouped(mut r: Routine) -> Routine {
    if r.kind == Kind::Procedure || r.python() {
        return r;
    }
    if is_query(&r.body) {
        r.body = crate::pg_json::text(&r.body).into_owned();
    } else {
        r.body = crate::pg_json::expression(&r.body).into_owned();
    }
    r
}

/// The statements sqlparser doesn't read as Pondra means them: `CREATE [OR REPLACE] FUNCTION`
/// and `CREATE [OR REPLACE] PROCEDURE` (Postgres's forms; DuckDB's `CREATE FUNCTION f(x) AS
/// expr` too), `CREATE [OR REPLACE] TASK name SCHEDULE '…' AS statement`, `DROP TASK` and `DROP
/// MACRO [TABLE] [IF EXISTS] name` (DuckDB's). None: none of them; `Stmt::Invalid`: one, written
/// wrong, and why.
pub fn statement(sql: &str) -> Option<Stmt> {
    let head = crate::text::first_word(sql).get(..6)?.to_lowercase();
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
                (r.kind, r.returns) = (Kind::Table, Some(format!("TABLE (\"{}\" {t})", crate::text::split(&name).1)));
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
    ensure!(r.returns.as_deref().is_none_or(|t| !crate::text::loose(Some(t)) || crate::text::is_json_type(Some(t))), "RETURNS ANY: say the type it returns (VARIANT for any JSON value)");
    ensure!(!r.with.vectorized || (r.python() && r.kind == Kind::Macro), "vectorized: a Python function returning a value (not a table, or a procedure)");
    ensure!(r.python() || (r.with.packages.is_empty() && r.with.entry.is_empty()), "packages and entry: a Python routine's");
    ensure!(r.with.cache.is_none() || (r.python() && !procedure), "cache: a Python function's (a SQL function is part of its query, whose answers the result cache keeps; a procedure is called for what it does)");
    Ok((name, grouped(r)))
}

/// The longest a routine's `timeout` may be: past it a duration would overflow (`1e20`, `inf`).
const WEEK: f64 = 7.0 * 86400.0;

/// `WITH (vectorized = true, packages = 'requests, jinja2', entry = 'f', timeout = 5)`.
fn options(p: &mut Parser, o: &mut Options) -> Result<()> {
    p.expect_token(&Token::LParen)?;
    loop {
        let k = p.parse_identifier()?.value.to_lowercase();
        let v = match p.consume_token(&Token::Eq) {
            true => match p.next_token().token {
                Token::SingleQuotedString(s) | Token::Number(s, _) => s,
                Token::Word(w) => w.value.to_lowercase(),
                t => bail!("{k}: a value, not {t} (a number, a word or 'text' in quotes, such as 3, immutable or '10 minutes')"),
            },
            false => "true".into(),
        };
        let yes = || -> Result<bool> { Ok(v.parse::<bool>().with_context(|| format!("{k}: true or false"))?) };
        match k.as_str() {
            "vectorized" => o.vectorized = yes()?,
            "strict" => o.strict = yes()?,
            "packages" => o.packages = v.split(',').map(str::trim).filter(|s| !s.is_empty()).collect::<Vec<_>>().join(", "),
            "entry" => o.entry = v.clone(),
            "timeout" => o.timeout = Some(v.parse::<f64>().ok().filter(|s| *s > 0.0 && *s <= WEEK).context("timeout: seconds, more than 0 and at most a week (604800)")?),
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

/// Leader: keep a function or procedure (`ddl::apply`).
pub async fn create(lake: &Lake, name: &str, r: Routine, replace: bool) -> Result<Value> {
    let name = crate::ddl::new_name(lake, name).await?;
    ensure!(r.kind != Kind::Procedure || !crate::workspace::is_run(crate::text::split(&name).1), "{name}: run is Pondra's own procedure (CALL run('etl/orders.sql') runs a file of the lake's)");
    ensure!(r.kind != Kind::Procedure || !crate::apply::is_own(crate::text::split(&name).1), "{name}: plan, apply, status and capture are Pondra's own procedures (CALL apply('files/sales', env => 'prod'))");
    ensure!(r.kind != Kind::Procedure || !crate::git::is_own(crate::text::split(&name).1), "{name}: git_checkout, git_status, git_commit, git_pull and git_pull_request are Pondra's own procedures (CALL git_commit('sales', 'message'))");
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
    let short = crate::text::split(name).1;
    let state = lake.session().state();
    let types = r.params.iter().filter_map(|p| p.ty.as_deref()).chain(r.returns.as_deref().filter(|t| !t.starts_with("TABLE"))).filter(|t| !crate::text::loose(Some(t)) || crate::text::is_json_type(Some(t)));
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
    crate::per_node!(static SEEN: Seen = Default::default());
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
    let sql = crate::path::door(lake, &sql, &views.keys().cloned().collect()).await?; // (the session's search path: `path.rs`; a client's frames are views, not tables)
    crate::front::expand(lake, &sql, views).await
}

/// Is `sql` a `CREATE` of a task, procedure, function or macro? Its `$name`s are its own
/// parameters or its graph's values, bound as it runs, never as it is made.
pub fn makes_code(sql: &str) -> bool {
    static HEAD: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^create\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?(?:task|procedure|function|macro)\b").expect("a regex"));
    HEAD.is_match(crate::text::first_word(sql))
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
    if let Some((head, moment)) = crate::past_names::restore_moment(sql) {
        // (`RESTORE TABLE t TO TIMESTAMP AS OF $before`: its moment bound as a query's column, the rest as written)
        let one = bind_as(&format!("SELECT {moment} AS __moment"), params, numbered)?;
        let moment = one.strip_prefix("SELECT ").and_then(|m| m.strip_suffix(" AS __moment")).context("a RESTORE's moment")?;
        return Ok(format!("{head}{moment}"));
    }
    let sql = &crate::sparksql::inline(sql)?; // (a parameter of Spark SQL's is in its text: bound once it is Pondra's)
    let sql = &crate::past_names::syntax(sql); // (`t AT (VERSION => $v)`)
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
    if let Some(first) = missing.first() {
        let n = first.trim_start_matches('$');
        let literal = |v: String| if v.parse::<f64>().is_ok() { v } else { format!("'{}'", v.replace('\'', "''")) };
        let setting = crate::settings::shown(n).map(|v| format!("; SET {n} = … made a setting, not a variable: ${n} = {} makes one", literal(v))).unwrap_or_default();
        return Err(crate::codes::coded("42P02", format!("no value for {} (give it one, or declare it: DECLARE {first} = …{setting})", missing.join(", ")))); // (undefined_parameter)
    }
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

/// Every SQL function call in `sql` replaced by its body (a function's own calls too, 16 deep at
/// most), and every Python function's call made whole: its arguments in their places, defaults
/// filled in, under the name it is registered by (`pyfn.rs`). A statement that makes a function,
/// procedure or stored view keeps its calls: they are read when used (`query::stored_views`), so
/// a function changed later changes them too. A materialized view keeps the functions as they were
/// when it was made: it has been adding up rows since.
///
/// The current session's search path is written in first (`path.rs`), so every door, a task's body
/// and a spread query see full names. Stored SQL is read with `expand_stored` instead.
pub async fn expand(lake: &Lake, sql: &str) -> Result<String> {
    let sql = crate::path::door(lake, sql, &std::collections::HashSet::new()).await?;
    crate::front::expand(lake, &sql, &HashMap::new()).await
}

/// `expand` for SQL that is already stored (a view's, a file's query): read as it was made, without
/// the session's path. A view made under `USE crm` kept `crm.t` when it was made; one made without a
/// path reads `t` as public, whoever reads it. Its `current_schema()` and the like are its reader's
/// (`path::currents`), as Postgres reads a view's.
pub async fn expand_stored(lake: &Lake, sql: &str) -> Result<String> { crate::front::expand(lake, &crate::path::currents(lake, sql), &HashMap::new()).await }

/// `SHOW USER FUNCTIONS`, `SHOW PROCEDURES`, `SHOW TASKS`, `SHOW VIEWS`, `SHOW MATERIALIZED VIEWS`,
/// `SHOW SCHEMAS`, `SHOW DATABASES`, `SHOW SECRETS`, `SHOW USERS`, `SHOW ROLES`, `SHOW GRANTS`
/// (`[LIKE 'pattern']`; shares and recipients: `show_shares`): this lake's own, from `pondra.routines`, `pondra.tasks`, `pondra.tables`,
/// `information_schema.schemata`, `secrets()`, `pondra.users` and `pondra.grants`, as Snowflake has them. (`SHOW FUNCTIONS` is every function a query may call,
/// as DataFusion lists them: its own, and the Python ones; `SHOW TABLES` is DataFusion's.)
pub(crate) fn show(sql: &str) -> Option<String> {
    // (SHOW, and DESCRIBE / DESC SHARE: the only words the regexes below can start with; any other statement skips them.)
    if !matches!(crate::text::head(sql).as_str(), "show" | "describe" | "desc") {
        return None;
    }
    if let Some(q) = crate::settings::show(sql) {
        return Some(q); // (`SHOW search_path`, `SHOW application_name`: the session's)
    }
    static SHOW: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*show\s+(user\s+functions|procedures|tasks|materialized\s+views|views|schemas|databases|secrets|users|roles|grants)(?:\s+like\s+('(?:[^']|'')*'))?\s*;?\s*$").expect("a regex"));
    if let Some(q) = show_shares(sql) {
        return Some(q);
    }
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

/// `SHOW SHARES`, `SHOW RECIPIENTS`, `DESCRIBE SHARE s` (Databricks' `SHOW ALL IN SHARE s` too):
/// what `pondra.shares` and `pondra.recipients` hold (`shares.rs`).
fn show_shares(sql: &str) -> Option<String> {
    static SHOW: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*(?:show\s+(shares|recipients)|(?:describe|desc|show\s+all\s+in)\s+share\s+([a-z_][a-z0-9_]*))\s*;?\s*$").expect("a regex"));
    let m = SHOW.captures(sql)?;
    Some(match (m.get(1).map(|w| w.as_str().to_lowercase()), m.get(2)) {
        (Some(w), _) if w == "shares" => "SELECT share AS name, first_value(comment) AS comment, count(shared_as) AS tables, first_value(recipients) AS recipients, min(created) AS created FROM pondra.shares GROUP BY share ORDER BY share".into(),
        (Some(_), _) => "SELECT * FROM pondra.recipients ORDER BY name".into(),
        (None, Some(s)) => format!("SELECT shared_as AS name, \"table\", partitions, history FROM pondra.shares WHERE share = '{}' AND shared_as IS NOT NULL ORDER BY 1", s.as_str().to_lowercase()),
        _ => return None,
    })
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
    /// `application/vnd.pondra.request` (`request_body`: the JSON, whose `"tables"` name the Arrow IPC
    /// streams after it, in order). A body that can't be read is the caller's mistake (22P02), not the node's.
    pub fn read(kind: &str, body: &[u8]) -> Result<Request> {
        Self::parse(kind, body).map_err(|e| crate::codes::coded("22P02", format!("the request's body can't be read: {e:#}")))
    }

    fn parse(kind: &str, body: &[u8]) -> Result<Request> {
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
        let (head, streams): (Head, Vec<&[u8]>) = match kind.split(';').next().unwrap_or_default().trim() {
            "application/json" => (serde_json::from_slice(body)?, vec![]),
            crate::request_body::CONTENT_TYPE => {
                let (json, streams) = crate::request_body::read(body)?;
                (serde_json::from_slice(json)?, streams)
            }
            _ => return Ok(Request { sql: String::from_utf8(body.to_vec())?, ..Default::default() }),
        };
        // (each name is one stream, in order: a name with no stream is a body that was cut short)
        if head.tables.len() > streams.len() {
            return Err(anyhow::anyhow!("the request ends too soon"));
        }
        let mut tables = vec![];
        for (name, stream) in head.tables.into_iter().zip(streams) {
            tables.push((name, crate::query::read_ipc(stream)?));
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
    if let Some(word) = crate::text::control(sql) {
        // BEGIN, COMMIT, ROLLBACK: the session's transaction (ADR-036 §5)
        let (tag, warning) = crate::audit::statement(app, sql, Box::pin(crate::txn::command(app, word, sql))).await?; // (on the heap: procedures call procedures deep)
        warning.iter().for_each(|w| heard(&format!("WARNING: {w}")));
        return Ok(Outcome::Done(j!({"transaction": tag.to_lowercase()})));
    }
    // (the gate runs inside the audited statement, so a refusal there is audited and fails the transaction too: a write
    // in a READ ONLY one (25006) leaves it failed, and the next statement gets 25P02)
    let out = crate::audit::statement(app, sql, Box::pin(async move {
        let busy = crate::txn::next(app, sql).await?; // (a failed transaction takes nothing but its end; READ COMMITTED reads the newest commit; the guard keeps the transaction busy while this runs)
        crate::txn::began(busy.id(), one_or_locked(app, sql, who, job)).await // (the transaction it began in: a write that outlives it is refused, 25000)
    }))
    .await;
    if let Err(e) = &out {
        crate::txn::failed(&crate::ext::said(e)); // (in a transaction: it fails with the statement)
    }
    out
}

/// One statement's outcome, a locking read in a transaction (`txn::locked_read`) first: its rows, locked.
async fn one_or_locked(app: &App, sql: &str, who: Who, job: Option<String>) -> Result<Outcome> {
    match crate::txn::locked_read(app, sql).await? {
        Some(rows) => Ok(Outcome::Rows(rows)),
        None => one_of(app, sql, who, job).await,
    }
}

async fn one_of(app: &App, sql: &str, who: Who, job: Option<String>) -> Result<Outcome> {
    if let Some(cmd) = crate::cursors::of(sql) {
        // DECLARE, FETCH, MOVE, CLOSE: the session's cursors (cursors.rs). Before the script's check: DECLARE reads as one.
        return Ok(match crate::cursors::run(app, cmd?, who.role).await? {
            crate::cursors::Answer::Rows(batches) => Outcome::Rows(batches),
            crate::cursors::Answer::Tag(tag) => Outcome::Done(j!({"command": tag})),
        });
    }
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
        if crate::apply::is_own(&name) {
            return Box::pin(crate::apply::call(app, &name, &args, who)).await; // (a project in the workspace, planned or applied: ADR-047 §4)
        }
        if crate::git::is_own(&name) {
            return Box::pin(crate::git::call(app, &name, &args, who)).await; // (a Git folder of the workspace: ADR-060 §8)
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
    if let Some(steps) = Box::pin(steps(app, sql, who)).await? {
        return Box::pin(run_steps(app, steps, who, job)).await; // (several statements: an ALTER's actions, a DROP's views)
    }
    if let Some(r) = crate::returning::of(sql) {
        return Ok(Outcome::Rows(vec![Box::pin(crate::returning::run(app, &r, who.role, who.files, job)).await?])); // (INSERT … RETURNING: the rows it wrote)
    }
    if let Some(stmt) = crate::write::parse(sql) {
        app.auth.allows(who.role, &stmt)?;
        return Ok(Outcome::Done(crate::write::on_node_as(app, stmt, job, who.files).await?));
    }
    if let Some(rows) = Box::pin(crate::txn::point_read(&app.lake, sql)).await? {
        return Ok(Outcome::Rows(rows)); // (a key lookup in a transaction: no planning)
    }
    Ok(Outcome::Rows(match who.files {
        true => app.query_as(&crate::sys::planned(sql)?, Some("0"), true).await?, // (a file here: this node only)
        false => app.query(sql, None).await?,
    }))
}

/// A statement Postgres does in one go that is taken one part at a time, each part a statement of its
/// own, with its notice: `ALTER TABLE t a1, a2` (Postgres applies the actions as one; here each is
/// its own commit) and `DROP TABLE t CASCADE` (the views that read it, deepest first, then it). Every
/// part is checked before the first runs, so a refusal changes nothing. None: `sql` is neither.
async fn steps(app: &App, sql: &str, who: Who) -> Result<Option<Vec<(String, Option<String>)>>> {
    let parts = match verb(sql) {
        v if v.eq_ignore_ascii_case("alter") => alter_steps(sql),
        v if v.eq_ignore_ascii_case("drop") => cascade_steps(&app.lake, sql).await?,
        _ => None,
    };
    let Some(parts) = parts else { return Ok(None) };
    for (part, _) in &parts {
        match crate::write::parse(part) {
            Some(Stmt::Invalid(why)) => bail!("{why}"),
            Some(stmt) => {
                app.auth.allows(who.role, &stmt)?;
                if let Stmt::Ddl(ds) = &stmt {
                    for d in ds {
                        crate::protect::check_ddl(&app.lake, d).await?;
                    }
                }
            }
            None => bail!("{part}: not taken as one statement: write it alone"),
        }
    }
    Ok(Some(parts))
}

/// The statement's first word, past leading comments (`text::first_word` gives the rest of the text).
fn verb(sql: &str) -> &str {
    crate::text::first_word(sql).split_whitespace().next().unwrap_or_default()
}

/// Whether `steps` takes `sql` apart: an ALTER TABLE with several actions, a DROP with CASCADE. The
/// fast path of `server::sql_as` (a statement `write::parse` doesn't know, sent to the query path)
/// leaves these to `one_of`; the first word is looked at first, so other statements pay nothing.
pub fn several(sql: &str) -> bool {
    let word = verb(sql);
    if !(word.eq_ignore_ascii_case("alter") || word.eq_ignore_ascii_case("drop")) {
        return false;
    }
    match Parser::parse_sql(&GenericDialect {}, sql).as_deref() {
        Ok([Statement::AlterTable(a)]) => a.operations.len() > 1,
        Ok([Statement::Drop { cascade, .. }]) => *cascade,
        _ => false,
    }
}

/// `ALTER TABLE t a1, a2, …` as one statement per action, when there is more than one.
fn alter_steps(sql: &str) -> Option<Vec<(String, Option<String>)>> {
    let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return None };
    let [Statement::AlterTable(a)] = &mut stmts[..] else { return None };
    if a.operations.len() < 2 {
        return None;
    }
    let ops = std::mem::take(&mut a.operations);
    Some(ops.into_iter().map(|op| {
        let mut one = a.clone();
        one.operations = vec![op];
        (crate::text::sql(&Statement::AlterTable(one)), None) // (`sql`: a default of `- -3` would otherwise read back as a comment)
    }).collect())
}

/// `DROP TABLE t CASCADE` / `DROP VIEW v CASCADE`: the views that read the dropped object, each after
/// the views that read it (Postgres's order), then the object itself. A task that reads it can't be
/// dropped by this statement, and says so before anything runs.
async fn cascade_steps(lake: &Lake, sql: &str) -> Result<Option<Vec<(String, Option<String>)>>> {
    let Ok(stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return Ok(None) };
    let [Statement::Drop { object_type, if_exists, names, cascade: true, .. }] = &stmts[..] else { return Ok(None) };
    let [name] = &names[..] else { return Ok(None) };
    let kind = match object_type {
        ast::ObjectType::Table => "TABLE",
        ast::ObjectType::View => "VIEW",
        _ => return Ok(None),
    };
    let Some(local) = crate::ddl::local(lake, &crate::text::object(name)) else { return Ok(None) }; // (another lake's: its own path)
    let mut out = vec![];
    views_reading(lake, &local, 0, &mut out).await?;
    let ifx = if *if_exists { "IF EXISTS " } else { "" };
    out.push((format!("DROP {kind} {ifx}{name}"), None));
    Ok(Some(out))
}

/// The views that read `name`, deepest first (each one's readers before it), with the notice a drop
/// of each gives: `drop cascades to view v1`.
async fn views_reading(lake: &Lake, name: &str, depth: usize, out: &mut Vec<(String, Option<String>)>) -> Result<()> {
    ensure!(depth < 16, "{name}: views that read views that read it, 16 deep: drop them by hand");
    for r in crate::ddl::readers(lake, name).await? {
        let (kind, view) = match (r.strip_prefix("materialized view "), r.strip_prefix("view ")) {
            (Some(v), _) => ("MATERIALIZED VIEW", v),
            (None, Some(v)) => ("VIEW", v),
            _ => bail!("{r} reads {name}: DROP … CASCADE drops views only: drop it first"),
        };
        let drop = format!("DROP {kind} {}", crate::write::sql_name(view)); // (quoted as stored: a view named "V" stays V)
        if out.iter().any(|(s, _)| *s == drop) {
            continue; // (read by two views: dropped once)
        }
        Box::pin(views_reading(lake, view, depth + 1, out)).await?; // (a view of a view: the future recurses)
        out.push((drop, Some(format!("drop cascades to {} {view}", kind.to_lowercase()))));
    }
    Ok(())
}

/// Runs the steps one after another, each with its notice and its own job part (`{job}:{i}`).
async fn run_steps(app: &App, steps: Vec<(String, Option<String>)>, who: Who, job: Option<String>) -> Result<Outcome> {
    let mut last = None;
    for (i, (step, notice)) in steps.iter().enumerate() {
        if let Some(n) = notice {
            heard(n);
        }
        last = Some(Box::pin(one_of(app, step, who, job.as_ref().map(|j| format!("{j}:{i}")))).await?);
    }
    last.context("no statement to run")
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
    if !crate::text::first_word(sql).get(..4)?.eq_ignore_ascii_case("call") {
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
    let word = crate::text::first_word(sql);
    if let Some(call) = (crate::text::head(sql) == "start").then(|| START.captures(word)).flatten() { // (the regex is for a statement that begins so)
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
    if crate::text::head(sql) != "do" {
        return None; // (the regex is for a statement that begins with `DO`)
    }
    let c = DO.captures(crate::text::first_word(sql))?;
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
        Some(t) if !crate::text::loose(Some(t)) => format!("CAST(({}) AS {t}) AS \"{}\"", values[&p.name], p.name),
        _ => format!("({}) AS \"{}\"", values[&p.name], p.name), // (any value, as it is)
    });
    let row = match r.params.is_empty() {
        true => RecordBatch::new_empty(Arc::new(datafusion::arrow::datatypes::Schema::empty())),
        false => {
            let about = || format!("{name}'s arguments");
            // (made ready as SQL that comes in, as a variable's value is: a cast to jsonb or uuid is the one the lake holds)
            let sql = prepare(&app.lake, &format!("SELECT {}", select.collect::<Vec<_>>().join(", ")), &HashMap::new(), &HashMap::new()).await.with_context(about)?;
            let rows = app.query(&sql, Some("0")).await.with_context(about)?;
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
            let (given, views) = (HashMap::new(), HashMap::new());
            let go = crate::vars::own(values, script(app, &r.body, &given, &views, inner, job)); // (a procedure's variables are its own)
            match name == "do" || crate::auth::shows_code() {
                true => Box::pin(go).await,
                false => Box::pin(crate::script::unshown(go)).await, // (its errors don't quote its code to this caller: S27)
            }
        }
    };
    let _ = log.end(app, &out, heard); // (written a moment later: a call doesn't wait for its log)
    out
}

/// `SELECT pondra.start('p', …)`: the procedure started, not waited for; its run's id, to look
/// for in `pondra.runs`.
fn start<'a>(app: &'a App, name: &'a str, args: &'a [FunctionArg], column: &'a str, who: Who, job: Option<String>) -> futures::future::BoxFuture<'a, Result<Outcome>> {
    Box::pin(async move {
        // (an apply's statement can't start one: it would run after the apply ended, with the apply's
        // admin rights and no record of it; `auth::carried` would keep both. `CALL` waits for it instead)
        ensure!(!crate::protect::applying(), "an apply's statements don't start a procedure (pondra.start, START CALL): it would run after the apply ends, outside its record. CALL it, which waits for it");
        let id = crate::runs::new_id();
        let (app2, id2) = (app.clone(), id.clone());
        if crate::workspace::is_run(name) {
            let args = args.to_vec();
            crate::workspace::arguments(app, &args).await?; // (its mistakes: said now, not only in the log)
            crate::panics::task(crate::auth::carried(async move {
                let _ = Box::pin(crate::workspace::run(&app2, &args, who, job, Some(id2))).await;
            }));
        } else {
            let (local, r, row) = Box::pin(prepared(app, name, args, who)).await?;
            crate::panics::task(crate::auth::carried(async move {
                let _ = Box::pin(run(&app2, local, r, row, who, job, Some(id2))).await; // (its outcome: the run log's)
            }));
        }
        let ids = datafusion::arrow::array::StringArray::from(vec![id]);
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![datafusion::arrow::datatypes::Field::new(column, datafusion::arrow::datatypes::DataType::Utf8, false)]));
        Ok(Outcome::Rows(vec![RecordBatch::try_new(schema, vec![Arc::new(ids)])?]))
    })
}

/// The arguments as parameters for SQL: each value exactly, at its own type.
pub fn values_of(row: &RecordBatch) -> Result<HashMap<String, Value>> {
    row.schema().fields().iter().zip(row.columns()).map(|(f, col)| Ok((f.name().clone(), j!({"sql": exact(col.as_ref(), 0)?})))).collect()
}

/// A value of an Arrow array as SQL that gives it back exactly, at its own type: its text cast
/// back (`arrow_cast('2026-09-27', 'Date32')`), bytes from hex (their text is hex, which cast as
/// it was gave the hex's own bytes), lists and structs item by item (their text doesn't cast).
pub fn exact(col: &dyn datafusion::arrow::array::Array, i: usize) -> Result<String> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::DataType as T;
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    let ty = col.data_type();
    let shown = || -> Result<String> { Ok(ArrayFormatter::try_new(col, &FormatOptions::default())?.value(i).to_string()) };
    let items = |items: datafusion::arrow::array::ArrayRef| -> Result<String> {
        Ok(format!("make_array({})", (0..items.len()).map(|j| exact(items.as_ref(), j)).collect::<Result<Vec<_>>>()?.join(", ")))
    };
    let value = match ty {
        _ if col.is_null(i) => "NULL".to_string(),
        T::Binary | T::LargeBinary | T::BinaryView | T::FixedSizeBinary(_) => format!("decode('{}', 'hex')", shown()?),
        T::List(_) => items(col.as_list::<i32>().value(i))?,
        T::LargeList(_) => items(col.as_list::<i64>().value(i))?,
        T::FixedSizeList(..) => items(col.as_fixed_size_list().value(i))?,
        T::Struct(fields) => {
            let parts = fields.iter().zip(col.as_struct().columns()).map(|(f, c)| Ok(format!("{}, {}", quote(f.name()), exact(c.as_ref(), i)?)));
            format!("named_struct({})", parts.collect::<Result<Vec<_>>>()?.join(", "))
        }
        T::Map(..) | T::Union(..) | T::ListView(_) | T::LargeListView(_) | T::RunEndEncoded(..) => bail!("a {ty} can't be passed as a value yet"),
        _ => quote(&shown()?),
    };
    Ok(format!("arrow_cast({value}, '{ty}')"))
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
    let json: Vec<bool> = r.params.iter().map(|p| crate::text::is_json_type(p.ty.as_deref())).collect();
    let head = j!({"op": "call", "name": name, "body": r.body, "entry": r.with.entry, "params": names(r), "json": json, "url": url, "token": lease.0, "depth": who.depth, "job": job});
    let limit = r.with.timeout.and_then(|s| std::time::Duration::try_from_secs_f64(s).ok());
    let kind = crate::python::Use::Procedure { nested: crate::python::holding() }; // (called by a procedure that holds a slot: it takes none)
    let mut notice = |n: String| {
        let n = lease.redact(&n);
        let _ = NOTICES.try_with(|all| all.lock().unwrap().push(n.clone()));
        heard.push(n);
    };
    let whose = if name == "do" { String::new() } else { format!("{name}: ") }; // (a DO block has no name)
    let parts = vec![crate::answer::ipc(&[args])?];
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
/// be remembered)? In a view it reads too.
pub async fn volatile(lake: &Lake, sql: &str) -> bool {
    let Ok(all) = listed(lake).await else { return true };
    reads_any(lake, sql, all.iter().filter(|(_, r)| r.kind != Kind::Procedure && !r.cacheable())).await
}

/// Does `sql` read a Python table function, itself or through a view? Then it runs on one node:
/// each would call it.
pub async fn pinned(lake: &Lake, sql: &str) -> bool {
    let Ok(all) = listed(lake).await else { return false };
    reads_any(lake, sql, all.iter().filter(|(_, r)| r.kind == Kind::Table && r.python())).await
}

/// Does `sql`, or a stored view it reads, name one of these routines? The views are looked at only
/// when there is one to find (most lakes have no Python function).
async fn reads_any<'a>(lake: &Lake, sql: &str, routines: impl Iterator<Item = (&'a String, &'a Routine)>) -> bool {
    let names: Vec<&String> = routines.map(|(n, _)| n).collect();
    if names.is_empty() || names.iter().any(|n| crate::text::mentions(sql, n)) {
        return !names.is_empty();
    }
    match with_views(lake, sql).await {
        Ok(text) => names.iter().any(|n| crate::text::mentions(&text, n)),
        Err(_) => true,
    }
}

/// A query's text with the SQL of every stored view it reads, one in another too (macros
/// expanded): what a query asks for (`now()`, files, a Python function) may be in a view of it.
pub async fn with_views(lake: &Lake, sql: &str) -> Result<String> {
    let mut text = sql.to_string();
    for (_, v) in crate::query::stored_views(lake, sql, false).await? {
        text.push(' ');
        text.push_str(&v);
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expander::named_apart;
    use crate::text::args;

    fn call(sql: &str) -> Vec<FunctionArg> {
        let Expr::Function(f) = parse_expr(sql).unwrap() else { panic!("{sql}: not a call") };
        let FunctionArguments::List(l) = f.args else { panic!("{sql}: no arguments") };
        l.args
    }

    #[test]
    fn a_sql_functions_json_operators_are_grouped_as_postgres_groups_them() {
        let body = |body: &str| grouped(Routine { kind: Kind::Macro, params: vec![], language: "sql".into(), body: body.into(), returns: None, with: Options::default() }).body;
        assert_eq!(body("d ->> 'a' = '1'"), "(d ->> 'a') = '1'");
        assert_eq!(body("d ? 'a'"), "jsonb_exists(d, 'a')", "an expression stays one, unnamed");
        assert_eq!(body("SELECT d ? 'a'"), r#"SELECT jsonb_exists(d, 'a') AS "?column?""#);
        assert_eq!(body("x + 1"), "x + 1", "nothing of JSON's: as written");
    }

    #[test]
    fn arguments_by_position_then_by_name() {
        let given = call("f(1, 'x', day => DATE '2026-09-29', \"Rate\" := 0.3)");
        let a = args("f", &given).unwrap();
        assert_eq!(a.given.iter().map(|e| e.to_string()).collect::<Vec<_>>(), ["1", "'x'"]);
        assert_eq!(a.named.iter().map(|(n, e)| format!("{n}={e}")).collect::<Vec<_>>(), ["day=DATE '2026-09-29'", "Rate=0.3"]);
        let refused = |sql: &str| args("f", &call(sql)).err().map(|e| e.to_string()).unwrap_or_default();
        assert!(refused("f(day => 1, 2)").contains("by position come before those by name"));
        assert!(refused("f(day => 1, DAY => 2)").contains("day given twice"));
    }

    #[test]
    fn a_restores_moment_is_bound() {
        let values = HashMap::from([("before".to_string(), serde_json::json!({"sql": "TIMESTAMP '2026-10-02 12:00:00'"}))]);
        let bound = bind("RESTORE TABLE sales.prices TO TIMESTAMP AS OF $before;", &values).unwrap();
        assert_eq!(bound, "RESTORE TABLE sales.prices TO TIMESTAMP AS OF (TIMESTAMP '2026-10-02 12:00:00')");
        assert!(bind("RESTORE TABLE t TO VERSION AS OF $v", &values).unwrap_err().to_string().contains("no value for $v"));
    }

    #[test]
    fn calls_that_differ_by_a_cast_are_told_apart() {
        assert_eq!(named_apart("SELECT sum(c), sum(c::DOUBLE), sum(CAST(c AS INT)) FROM t HAVING sum(c::DOUBLE) > 0").unwrap(),
                   "SELECT sum(c), sum(coalesce(c::DOUBLE)) AS \"sum(c::DOUBLE)\", sum(coalesce(coalesce(CAST(c AS INT)))) AS \"sum(CAST(c AS INT))\" FROM t HAVING sum(coalesce(c::DOUBLE)) > 0");
        // (a subquery's calls are its own; a cast of a value alone, and calls that differ otherwise, are left as written)
        assert_eq!(named_apart("SELECT sum(c), (SELECT sum(c::DOUBLE) FROM u) AS s FROM t"), None);
        assert_eq!(named_apart("SELECT max(c), max(d::INT) FROM t HAVING f(max(c), 0.5::DOUBLE) > f(max(c), 0.5)"), None);
    }

    #[test]
    fn an_expression_written_again_is_named_apart_from_the_first() {
        // (DataFusion names `id + 1` as `t.id + Int64(1)`, so the second can be named as written; it names
        // `NULL` and `count(*)` as written, so the second can't)
        assert_eq!(named_apart("SELECT id + 1, id + 1, id + 1 FROM t").unwrap(), "SELECT id + 1, id + 1 AS \"id + 1\", id + 1 AS \"id + 1_1\" FROM t");
        assert_eq!(named_apart("INSERT INTO t SELECT value, NULL, NULL FROM r").unwrap(), "INSERT INTO t SELECT value, NULL, NULL AS \"NULL_1\" FROM r");
        assert_eq!(named_apart("SELECT count(*), count(*) FROM t").unwrap(), "SELECT count(*), count(*) AS \"count(*)_1\" FROM t");
    }
}
