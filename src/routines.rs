//! Macros, procedures and scripts: SQL — and Python — kept in the catalog under a name (`r/`), as
//! stored views are (ADR-023).
//!
//! - **A macro** is an expression or a query with parameters, as DuckDB has them: `CREATE MACRO
//!   net(x, rate := 0.2) AS x * (1 - rate)`, `CREATE MACRO recent(days) AS TABLE SELECT … WHERE ts
//!   > now() - days * INTERVAL '1 day'`. Where SQL comes in (every door, and stored views as they
//!   are read) a call is replaced by the body, the arguments in place of the parameters
//!   (`expand`). After that it is plain SQL: it plans, spreads and is remembered like any other.
//! - **A procedure** is statements run in order (`LANGUAGE sql`) or a Python program (`LANGUAGE
//!   python`), with typed parameters: `CALL load_day(DATE '2026-09-27')`. Its arguments are worked
//!   out once; then each statement runs as if the caller had sent it, with the caller's rights. A
//!   Python procedure runs in a Python process beside the node (`--python`) with a connection back
//!   to it that has the caller's rights and no more (`auth::lend`); the value of its last line is
//!   the answer: a frame (whose SQL then runs here), a table, a value, or nothing.
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
use std::sync::{Arc, Mutex};

pub fn key(name: &str) -> String { format!("r/{name}") }

/// A macro or a procedure, as the catalog keeps it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Routine {
    pub kind: Kind,
    pub params: Vec<Param>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub language: String, // a procedure's: sql or python
    pub body: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Macro,     // an expression
    Table,     // a query (`AS TABLE`)
    Procedure, // statements, or a Python program
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Param {
    pub name: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub ty: Option<String>, // (a procedure's arguments are cast to it)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>, // a SQL expression
}

fn what(k: Kind) -> &'static str {
    match k {
        Kind::Procedure => "procedure",
        _ => "macro",
    }
}

// ---------------------------------------------------------------- statements

/// `CREATE [OR REPLACE] MACRO …`, as sqlparser reads it.
pub fn of_macro(args: &Option<Vec<ast::MacroArg>>, def: &ast::MacroDefinition) -> Routine {
    let params = args.iter().flatten().map(|a| Param { name: ident(&a.name), ty: None, default: a.default_expr.as_ref().map(|e| e.to_string()) }).collect();
    match def {
        ast::MacroDefinition::Expr(e) => Routine { kind: Kind::Macro, params, language: String::new(), body: e.to_string() },
        ast::MacroDefinition::Table(q) => Routine { kind: Kind::Table, params, language: String::new(), body: q.to_string() },
    }
}

/// The statements sqlparser doesn't read as Pondra means them: `CREATE [OR REPLACE] PROCEDURE
/// name(p type [DEFAULT e], …) LANGUAGE sql|python AS $$ … $$` (Postgres's form) and `DROP MACRO
/// [TABLE] [IF EXISTS] name` (DuckDB's). None: neither; `Stmt::Invalid`: one, written wrong.
pub fn statement(sql: &str) -> Option<Stmt> {
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(sql).ok()?;
    if p.parse_keywords(&[Keyword::DROP, Keyword::MACRO]) {
        let _ = p.parse_keyword(Keyword::TABLE);
        let if_exists = p.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        return Some(match p.parse_object_name(false) {
            Ok(n) => Stmt::Ddl(vec![Ddl::DropRoutine { name: object(&n), if_exists }]),
            Err(e) => Stmt::Invalid(format!("DROP MACRO: {e}")),
        });
    }
    let create = p.parse_keyword(Keyword::CREATE);
    let replace = p.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    if !(create && p.parse_keyword(Keyword::PROCEDURE)) {
        return None;
    }
    Some(procedure(&mut p).map_or_else(|e| Stmt::Invalid(format!("CREATE PROCEDURE: {e:#} (CREATE PROCEDURE name(p TYPE [DEFAULT …], …) LANGUAGE sql|python AS $$ … $$)")), |(name, routine)| Stmt::Ddl(vec![Ddl::CreateRoutine { name, routine, replace }])))
}

/// The rest of `CREATE PROCEDURE`: its name, parameters, language and body (either order).
fn procedure(p: &mut Parser) -> Result<(String, Routine)> {
    let name = object(&p.parse_object_name(false)?);
    let mut params = vec![];
    if p.consume_token(&Token::LParen) && !p.consume_token(&Token::RParen) {
        params = p.parse_comma_separated(|p| {
            let name = ident(&p.parse_identifier()?);
            let ty = p.parse_data_type()?.to_string();
            let default = match p.parse_keyword(Keyword::DEFAULT) || p.consume_token(&Token::Eq) {
                true => Some(p.parse_expr()?.to_string()),
                false => None,
            };
            Ok(Param { name, ty: Some(ty), default })
        })?;
        p.expect_token(&Token::RParen)?;
    }
    let (mut language, mut body) = (None, None);
    loop {
        if p.parse_keyword(Keyword::LANGUAGE) {
            language = Some(p.parse_identifier()?.value.to_lowercase());
        } else if p.parse_keyword(Keyword::AS) {
            body = Some(match p.next_token().token {
                Token::DollarQuotedString(s) => s.value,
                Token::SingleQuotedString(s) => s,
                t => bail!("the body is a string: AS $$ … $$, not {t}"),
            });
        } else {
            break;
        }
    }
    let t = p.next_token().token;
    ensure!(matches!(t, Token::EOF | Token::SemiColon), "unexpected {t}");
    let language = language.unwrap_or_else(|| "sql".into());
    ensure!(["sql", "python"].contains(&language.as_str()), "LANGUAGE sql or python, not {language}");
    Ok((name, Routine { kind: Kind::Procedure, params, language, body: body.context("no body: AS $$ … $$")? }))
}

/// Leader: keep a macro or procedure (`ddl::apply`).
pub async fn create(lake: &Lake, name: &str, r: Routine, replace: bool) -> Result<Value> {
    let name = crate::ddl::new_name(lake, name).await?;
    if let Some(old) = lake.cat.get::<Routine>(&key(&name)).await? {
        ensure!(replace, "{} {name} already exists (CREATE OR REPLACE {})", what(old.kind), what(r.kind).to_uppercase());
        ensure!((old.kind == Kind::Procedure) == (r.kind == Kind::Procedure), "{name} is a {}", what(old.kind));
    }
    check(lake, &name, &r)?;
    lake.cat.commit(vec![(key(&name), json(&r))], &[]).await?;
    Ok(j!({what(r.kind): name}))
}

/// A body that reads, parameters named once, and a macro that hides none of SQL's own functions.
fn check(lake: &Lake, name: &str, r: &Routine) -> Result<()> {
    for (i, p) in r.params.iter().enumerate() {
        ensure!(!r.params[..i].iter().any(|q| q.name == p.name), "{name}: two parameters called {}", p.name);
    }
    let short = crate::ddl::split(name).1;
    let state = lake.session().state();
    match r.kind {
        Kind::Macro => {
            ensure!(!state.scalar_functions().contains_key(short) && !state.aggregate_functions().contains_key(short) && !state.window_functions().contains_key(short), "{short} is one of SQL's own functions: call the macro something else");
            parse_expr(&r.body)?;
        }
        Kind::Table => {
            ensure!(!state.table_functions().contains_key(short), "{short} is one of SQL's own table functions: call the macro something else");
            parse_query(&r.body)?;
        }
        Kind::Procedure if r.language == "sql" => {
            let nulls = r.params.iter().map(|p| (p.name.clone(), Value::Null)).collect();
            for s in split(&r.body) {
                if statement(&s).is_none() {
                    bind(&s, &nulls).with_context(|| format!("{name}: {}", s.trim()))?;
                }
            }
        }
        Kind::Procedure => {}
    }
    Ok(())
}

/// Leader: `DROP MACRO`, `DROP FUNCTION`, `DROP PROCEDURE`.
pub async fn drop(lake: &Lake, name: &str, if_exists: bool) -> Result<Value> {
    let name = crate::ddl::local(lake, name).with_context(|| format!("{name}: not this lake's"))?;
    let Some(r) = lake.cat.get::<Routine>(&key(&name)).await? else {
        ensure!(if_exists, "no macro or procedure {name}");
        return Ok(j!({"dropped": false}));
    };
    lake.cat.commit(vec![], &[key(&name)]).await?;
    Ok(j!({what(r.kind): name, "dropped": true}))
}

/// This lake's macros and procedures, by name: read again only after a commit (every query asks).
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
    let sql = if params.is_empty() { sql.to_string() } else { bind(sql, params)? };
    expand_with(lake, &sql, views).await
}

/// `$name` → the value given for it; every `$name` needs one.
pub fn bind(sql: &str, params: &HashMap<String, Value>) -> Result<String> {
    if !sql.contains('$') {
        return Ok(sql.to_string());
    }
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let values = params.iter().map(|(k, v)| Ok((k.clone(), literal(v)?))).collect::<Result<HashMap<_, _>>>()?;
    let mut missing = None;
    let _ = visit_expressions_mut(&mut stmts, |e| {
        if let Expr::Value(v) = e {
            if let ast::Value::Placeholder(p) = &v.value {
                match values.get(p.trim_start_matches('$')) {
                    Some(to) => *e = Expr::Nested(Box::new(to.clone())),
                    None => missing = Some(p.clone()),
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    if let Some(p) = missing {
        bail!("no value for {p}");
    }
    Ok(text(&stmts))
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

/// Every macro call in `sql` replaced by its body (a macro's own calls too, 16 deep at most).
/// A statement that makes a macro, procedure or stored view keeps its calls: they are read when
/// used (`query::stored_views`), so a macro changed later changes them too. A materialized view
/// keeps the macros as they were when it was made: it has been adding up rows since.
pub async fn expand(lake: &Lake, sql: &str) -> Result<String> { expand_with(lake, sql, &HashMap::new()).await }

async fn expand_with(lake: &Lake, sql: &str, views: &HashMap<String, String>) -> Result<String> {
    let all = listed(lake).await?;
    let named = |n: &String| crate::ddl::mentions(sql, n);
    if !all.iter().any(|(n, r)| r.kind != Kind::Procedure && named(n)) && !views.keys().any(named) && !FROM_FIRST.is_match(sql) && !crate::ext::mentions(sql) {
        return Ok(sql.to_string());
    }
    let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return Ok(sql.to_string()) };
    for s in stmts.iter_mut() {
        if !matches!(s, Statement::CreateMacro { .. } | Statement::CreateView(ast::CreateView { materialized: false, .. })) {
            if let ControlFlow::Break(e) = s.visit(&mut Expander { lake, all: &all, views, depth: 0 }) {
                return Err(e);
            }
        }
    }
    Ok(text(&stmts))
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

struct Expander<'a> {
    lake: &'a Lake,
    all: &'a HashMap<String, Routine>,
    views: &'a HashMap<String, String>, // (a request's own: `FROM name` is its query)
    depth: usize,
}

impl Expander<'_> {
    fn find(&self, name: &ast::ObjectName, kind: Kind) -> Option<(String, &Routine)> {
        let name = crate::ddl::local(self.lake, &object(name))?;
        self.all.get(&name).filter(|r| r.kind == kind).map(|r| (name, r))
    }

    /// The body with the arguments in its parameters' places, its own macro calls expanded.
    fn call<T: VisitMut>(&self, name: &str, r: &Routine, args: &[FunctionArg], mut body: T) -> Result<T> {
        ensure!(self.depth < 16, "{name}: macros calling macros 16 deep (a loop?)");
        let values = arguments(name, r, args)?;
        let _ = visit_expressions_mut(&mut body, |e| {
            if let Expr::Identifier(i) = e {
                if let Some(v) = values.get(&ident(i)) {
                    *e = Expr::Nested(Box::new(v.clone()));
                }
            }
            ControlFlow::<()>::Continue(())
        });
        match body.visit(&mut Expander { depth: self.depth + 1, ..*self }) {
            ControlFlow::Break(e) => Err(e),
            ControlFlow::Continue(()) => Ok(body),
        }
    }
}

impl VisitorMut for Expander<'_> {
    type Break = anyhow::Error;

    fn post_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<Self::Break> {
        select_star(&mut q.body);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(f) = expr else { return ControlFlow::Continue(()) };
        let Some((name, r)) = self.find(&f.name, Kind::Macro) else { return ControlFlow::Continue(()) };
        let args = match &f.args {
            FunctionArguments::List(l) => l.args.clone(),
            _ => vec![],
        };
        match parse_expr(&r.body).and_then(|body| self.call(&name, r, &args, body)) {
            Ok(e) => *expr = Expr::Nested(Box::new(e)),
            Err(e) => return ControlFlow::Break(e),
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
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
            q.visit(&mut Expander { views: &NONE, depth: self.depth + 1, ..*self })?; // (its macros; its names are the lake's)
            let alias = alias.clone().or_else(|| Some(ast::TableAlias { explicit: true, name: ast::Ident::new(object(name)), columns: vec![], at: None }));
            *t = TableFactor::Derived { lateral: false, subquery: Box::new(q), alias, sample: None };
            return ControlFlow::Continue(());
        }
        let TableFactor::Table { name, alias, args: Some(args), .. } = t else { return ControlFlow::Continue(()) };
        let Some((found, r)) = self.find(name, Kind::Table) else { return ControlFlow::Continue(()) };
        let q = match parse_query(&r.body).and_then(|body| self.call(&found, r, &args.args, body)) {
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
/// `$tag$…$tag$`), quoted names and comments — and what follows the last one, if it holds more
/// than whitespace and comments (the shell waits for its `;`).
pub fn statements(text: &str) -> (Vec<String>, String) {
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
fn dollar_tag(s: &str) -> Option<&str> {
    let end = s[1..].find('$')? + 2;
    let inner = &s[1..end - 1];
    (inner.chars().all(|c| c.is_alphanumeric() || c == '_') && !inner.starts_with(|c: char| c.is_ascii_digit())).then(|| &s[..end])
}

/// Run one statement here as the caller could have sent it.
pub async fn one(app: &App, sql: &str, who: Who, job: Option<String>) -> Result<Outcome> {
    if crate::write::checkpoint(sql) {
        ensure!(who.role >= Role::Write, "CHECKPOINT needs a write token");
        return Ok(Outcome::Done(app.checkpoint().await?));
    }
    if let Some((name, args)) = call_of(sql) {
        return Box::pin(call(app, &name, &args, who, job)).await;
    }
    if let Some(stmt) = crate::write::parse(sql) {
        app.auth.allows(who.role, &stmt)?;
        return Ok(Outcome::Done(crate::write::on_node_as(app, stmt, job, who.files).await?));
    }
    Ok(Outcome::Rows(match who.files {
        true => app.query_as(&crate::asof::rewrite(sql)?, Some("0"), true).await?, // (a file here: this node only)
        false => app.query(sql, None).await?,
    }))
}

/// A script's statements, each in turn; the last one's outcome. With a `job`, each gets its own
/// (`{job}:{i}`): the script run again with the same job applies each write once.
pub async fn script(app: &App, sql: &str, params: &HashMap<String, Value>, views: &HashMap<String, String>, who: Who, job: Option<String>) -> Result<Outcome> {
    let (all, mut last) = (split(sql), Outcome::Done(j!({})));
    for (i, s) in all.iter().enumerate() {
        let s = prepare(&app.lake, s, params, views).await?;
        let job = job.as_ref().map(|j| if all.len() == 1 { j.clone() } else { format!("{j}:{i}") });
        last = match one(app, &s, who, job).await {
            Err(e) if all.len() > 1 => return Err(e.context(format!("statement {}: {}", i + 1, short(&s)))),
            r => r?,
        };
    }
    Ok(last)
}

fn short(s: &str) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.chars().count() > 80 { format!("{}…", s.chars().take(80).collect::<String>()) } else { s }
}

/// `CALL name(…)`: the procedure's name and arguments, if `sql` is one.
pub fn call_of(sql: &str) -> Option<(String, Vec<FunctionArg>)> {
    if !sql.trim_start().get(..4)?.eq_ignore_ascii_case("call") {
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

/// Run a procedure. Its arguments are worked out once, as the caller (`CALL p(now())`: one moment
/// for every statement), cast to their parameters' types.
async fn call(app: &App, name: &str, args: &[FunctionArg], who: Who, job: Option<String>) -> Result<Outcome> {
    ensure!(who.depth < 16, "{name}: procedures calling procedures 16 deep (a loop?)");
    let local = crate::ddl::local(&app.lake, name).with_context(|| format!("no procedure {name}"))?;
    let r = app.lake.cat.get::<Routine>(&key(&local)).await?.filter(|r| r.kind == Kind::Procedure).with_context(|| format!("no procedure {name}"))?;
    let values = arguments(name, &r, args)?;
    let select = r.params.iter().map(|p| match &p.ty {
        Some(t) => format!("CAST(({}) AS {t}) AS \"{}\"", values[&p.name], p.name),
        None => format!("({}) AS \"{}\"", values[&p.name], p.name),
    });
    let row = match r.params.is_empty() {
        true => RecordBatch::new_empty(Arc::new(datafusion::arrow::datatypes::Schema::empty())),
        false => {
            let rows = app.query(&format!("SELECT {}", select.collect::<Vec<_>>().join(", ")), Some("0")).await.with_context(|| format!("{name}'s arguments"))?;
            datafusion::arrow::compute::concat_batches(&rows[0].schema(), &rows)?
        }
    };
    let inner = Who { depth: who.depth + 1, ..who };
    match r.language.as_str() {
        "python" => python(app, &local, &r, row, inner, job).await,
        _ => Box::pin(script(app, &r.body, &values_of(&row)?, &HashMap::new(), inner, job)).await,
    }
}

/// The arguments as parameters for SQL: each value exactly, at its own type.
fn values_of(row: &RecordBatch) -> Result<HashMap<String, Value>> {
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

/// A Python procedure: `python -m pondra.procedure` beside this node, handed its body, the
/// arguments (Arrow) and a connection back here with the caller's rights; it answers with a JSON
/// line saying what follows: rows (Arrow), a frame's SQL (run here), or nothing. What it prints
/// goes to this node's log; if it fails, the end of it (the traceback) is the error.
async fn python(app: &App, name: &str, r: &Routine, args: RecordBatch, who: Who, job: Option<String>) -> Result<Outcome> {
    use tokio::io::AsyncWriteExt;
    let exe = app.python.as_deref().with_context(|| format!("{name} is a Python procedure, and this node runs no Python: start it with --python <python>"))?;
    let lease = crate::auth::lend(who.role, who.files); // (ends when this does)
    let url = format!("http://{}", app.cluster.addr.replace("0.0.0.0", "127.0.0.1"));
    let head = j!({"name": name, "body": r.body, "url": url, "token": lease.0, "depth": who.depth, "job": job});
    let mut child = tokio::process::Command::new(exe)
        .args(["-m", "pondra.procedure"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("{name}: couldn't start {exe}"))?;
    let mut input = serde_json::to_vec(&head)?;
    input.push(b'\n');
    input.extend(crate::query::ipc(&[args])?);
    let mut stdin = child.stdin.take().expect("piped");
    let writing = async move { stdin.write_all(&input).await }; // (while it reads: a pipe holds only so much)
    let (wrote, out) = tokio::join!(writing, child.wait_with_output());
    let out = out?;
    let said = String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        // (the error itself first, then the traceback's end)
        let last = said.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or_default();
        let tail: String = said.trim_end().chars().rev().take(3000).collect::<Vec<_>>().into_iter().rev().collect();
        bail!("{name} failed: {}", if tail.is_empty() { format!("{} ({wrote:?})", out.status) } else { format!("{last}\n{tail}") });
    }
    if !said.trim().is_empty() {
        eprintln!("procedure {name}: {}", said.trim_end());
    }
    let cut = out.stdout.iter().position(|b| *b == b'\n').context("no answer from the procedure")?;
    let answer: Value = serde_json::from_slice(&out.stdout[..cut])?;
    match answer["kind"].as_str() {
        Some("rows") => Ok(Outcome::Rows(crate::query::read_ipc(&out.stdout[cut + 1..])?)),
        Some("sql") => Box::pin(one(app, answer["sql"].as_str().unwrap_or_default(), who, None)).await,
        Some("error") => bail!("{name}: {}", answer["error"].as_str().unwrap_or_default()),
        _ => Ok(Outcome::Done(j!({"called": name}))),
    }
}
