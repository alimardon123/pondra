//! Variables, and a file's parameters (round 31, ADR-044). `DECLARE $mode = 'full';` declares a
//! variable of the script's own (its type and its default are both optional: `DECLARE $n BIGINT;` is
//! NULL), `DECLARE PARAMETER $day DATE = current_date - 1;` one its caller may give a value for
//! (`DECLARE PARAMETER $region VARCHAR;` has no value until one is given), and `$day = $day + 1;`
//! changes either. Every `$day` after
//! that is its value, bound as a parameter is, never pasted in as text. DuckDB's `SET VARIABLE day
//! = …`, `RESET VARIABLE day` and `getvariable('day')` are other names for the same; `SET` alone
//! stays the settings' (`settings.rs`), and the `$` keeps `DECLARE` apart from Postgres's cursors.
//!
//! - **Where they live.** A variable is its session's (a Postgres connection, a client's
//!   `x-pondra-session`, a console tab, a script sent in one go: `temp.rs`). A procedure and a file
//!   run (`CALL run(…)`) have their own (`own`), which the connection back of a Python procedure, a
//!   `.py` file or a notebook's Python cells shares (`auth::lend`): `db.vars` in Python is the same
//!   variables as `$name` in SQL.
//! - **Given values.** What a request, a procedure call or a file run is given (`params`, `CALL
//!   run('f.sql', day => …)`, `pondra run f.sql --day …`, the bar above a file) is its run's
//!   (`GIVEN`): `$day` is the value given until something sets it, and a `DECLARE PARAMETER` takes
//!   the value given in place of its default, cast to its type; a plain `DECLARE` given one is
//!   refused (it is the script's own). So in a file a `DECLARE PARAMETER` is a parameter with a
//!   default, and a `$name` used but never set is a required one; `parameters` lists them
//!   (`pondra.parameters('etl/orders.sql')`), the comment above each its description.
//! - **Typed.** A variable declared with a type casts every value it takes to it; one declared
//!   without casts a value given to its default's type. A variable holds one value (a number, a
//!   string, a date, …), worked out once, as its caller, when it is set.
use crate::routines::Outcome;
use crate::server::App;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{Array, RecordBatch};
use serde_json::{json as j, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};

/// A variable: its value as SQL (a typed literal, `arrow_cast('2026-09-30', 'Date32')`), as shown,
/// its Arrow type, and the SQL type it was declared with (every value it takes is cast to that).
#[derive(Clone, Debug, Default)]
pub struct Var {
    pub sql: String,
    pub shown: Option<String>,
    pub ty: String,
    pub declared: Option<String>,
}

pub type Vars = Arc<Mutex<BTreeMap<String, Var>>>;
type Given = Arc<Mutex<HashMap<String, Value>>>;

tokio::task_local! {
    /// A procedure's or a file run's own variables, while it runs (else the session's).
    static OWN: Vars;
    /// The values a run was given that nothing has set since.
    static GIVEN: Given;
}

const NO_SESSION: &str = "a variable is a session's: a Postgres connection's, the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>), or a script's (send it with the statements that use it)";

/// What a statement does to a variable.
pub enum Change {
    /// `parameter`: `DECLARE PARAMETER`, which takes a value given (a plain one is the script's own).
    Declare { name: String, ty: Option<String>, default: Option<String>, parameter: bool },
    Set { name: String, value: String },
    Reset(String),
}

impl Change {
    /// Its command tag (Postgres's port).
    pub fn tag(&self) -> &'static str {
        match self {
            Change::Declare { .. } => "DECLARE",
            Change::Set { .. } => "SET",
            Change::Reset(_) => "RESET",
        }
    }
}

/// `DECLARE [PARAMETER] $name [type] [= | DEFAULT value]`, `$name = value`, DuckDB's `SET VARIABLE name =
/// value` and `RESET VARIABLE name`: what `sql` does to a variable, if it is one of them.
pub fn change(sql: &str) -> Option<Change> {
    static DECLARE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^declare\s+(parameter\s+)?\$([a-z_]\w*)\b\s*(.*?)\s*;?\s*$").expect("a regex"));
    static TYPED: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^(.*?)\s*(?:=|\bdefault\b)\s*(.*)$").expect("a regex"));
    static SET: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^(?:\$|set\s+variable\s+)([a-z_]\w*)\s*(?:=|\bto\b)\s*(.*?)\s*;?\s*$").expect("a regex"));
    static RESET: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^reset\s+variable\s+([a-z_]\w*)\s*;?\s*$").expect("a regex"));
    let s = crate::write::first_word(sql);
    if !s.starts_with(['$', 'd', 'D', 's', 'S', 'r', 'R']) {
        return None;
    }
    let some = |t: &str| Some(t.trim().to_string()).filter(|t| !t.is_empty());
    if let Some(c) = DECLARE.captures(s) {
        let (ty, default) = match TYPED.captures(&c[3]) {
            Some(t) => (some(&t[1]), Some(t[2].trim().to_string())),
            None => (some(&c[3]), None),
        };
        return Some(Change::Declare { name: c[2].to_string(), ty, default, parameter: c.get(1).is_some() });
    }
    if let Some(c) = SET.captures(s) {
        return Some(Change::Set { name: c[1].to_string(), value: c[2].to_string() });
    }
    RESET.captures(s).map(|c| Change::Reset(c[1].to_string()))
}

/// The current scope's variables: a procedure's or a file run's own, else the session's.
fn vars<T>(f: impl FnOnce(&mut BTreeMap<String, Var>) -> T) -> Result<T> {
    if let Ok(own) = OWN.try_with(Arc::clone) {
        return Ok(f(&mut own.lock().unwrap()));
    }
    let s = crate::temp::current().context(NO_SESSION)?;
    crate::temp::with(&s, false, |x| Ok(f(&mut x.variables)))
}

/// Are there variables to keep (a procedure's or file run's own, or a session's)?
pub fn scoped() -> bool { OWN.try_with(|_| ()).is_ok() || crate::temp::current().is_some() }

/// Run `f` with variables of its own, keeping the values given (a script sent with no session).
pub async fn local<F: std::future::Future>(f: F) -> F::Output { OWN.scope(Vars::default(), f).await }

/// A copy of the variables in force, for work that runs beside its script (`PARALLEL`, `ASYNC`):
/// what it sets stays its own.
pub fn snapshot() -> Vars { Arc::new(Mutex::new(vars(|m| m.clone()).unwrap_or_default())) }

/// Run `f` with `own` as its variables (a `snapshot`).
pub async fn with_own<F: std::future::Future>(own: Vars, f: F) -> F::Output { OWN.scope(own, f).await }

/// A variable's value now (a block keeps it, to put back when it ends: `script.rs`).
pub fn get(name: &str) -> Option<Var> { vars(|m| m.get(name).cloned()).ok().flatten() }

/// A variable set as it was (None: not there).
pub fn put(name: &str, v: Option<Var>) {
    let _ = vars(|m| match v {
        Some(v) => m.insert(name.to_string(), v),
        None => m.remove(name),
    });
}

/// A one-row column's value as a variable's (a loop's row, `INTO`).
pub fn of_column(col: &dyn Array) -> Result<Var> {
    let one = datafusion::arrow::array::make_array(col.to_data());
    var_of(&RecordBatch::try_from_iter([("v", one)])?)
}

/// Text as a variable's value (a handler's `$error`).
pub fn text(t: &str) -> Var {
    let sql = format!("arrow_cast('{}', 'Utf8')", t.replace('\'', "''"));
    Var { sql, shown: Some(t.to_string()), ty: "Utf8".into(), declared: None }
}

/// Is a run's set of given values in force (a file run's cells share one)?
pub fn in_run() -> bool { GIVEN.try_with(|_| ()).is_ok() }

/// Run `f` with variables of its own, starting from `given` (a procedure call, a file run).
pub async fn own<F: std::future::Future>(given: HashMap<String, Value>, f: F) -> F::Output {
    OWN.scope(Vars::default(), GIVEN.scope(Arc::new(Mutex::new(given)), f)).await
}

/// Run `f` with `given` as its run's values, over the variables there are (a request's).
pub async fn run<F: std::future::Future>(given: HashMap<String, Value>, f: F) -> F::Output {
    let mut all = GIVEN.try_with(|g| g.lock().unwrap().clone()).unwrap_or_default(); // (a Python cell's request: its run's values too)
    all.extend(given);
    GIVEN.scope(Arc::new(Mutex::new(all)), f).await
}

/// What a connection lent to Python code carries (`auth::lend`): this run's variables, if they are
/// its own, and its given values, so its queries see them (`within`).
#[derive(Clone)]
pub struct Lent(Option<Vars>, Option<Given>);

pub fn lend() -> Lent { Lent(OWN.try_with(Arc::clone).ok(), GIVEN.try_with(Arc::clone).ok()) }

/// Run `f` as the Python code a `Lent` was lent to: with its run's variables and given values.
pub async fn within<F: std::future::Future>(lent: Option<Lent>, f: F) -> F::Output {
    match lent {
        Some(Lent(Some(own), Some(given))) => OWN.scope(own, GIVEN.scope(given, f)).await,
        Some(Lent(None, Some(given))) => GIVEN.scope(given, f).await,
        Some(Lent(Some(own), None)) => OWN.scope(own, f).await,
        _ => f.await,
    }
}

/// A value given and not yet taken, taken: a `DECLARE` uses it, an assignment replaces it.
fn take(name: &str) -> Option<Value> { GIVEN.try_with(|g| g.lock().unwrap().remove(name)).ok().flatten() }

/// The values `$name` stands for now: the scope's variables, the run's given values over them,
/// then `params` over both (a request that binds its own: a live query's).
pub fn values(params: &HashMap<String, Value>) -> HashMap<String, Value> {
    let mut out: HashMap<String, Value> = vars(|m| m.iter().map(|(k, v)| (k.clone(), j!({"sql": v.sql}))).collect()).unwrap_or_default();
    out.extend(GIVEN.try_with(|g| g.lock().unwrap().clone()).unwrap_or_default());
    out.extend(params.iter().map(|(k, v)| (k.clone(), v.clone())));
    out
}

/// Does `sql` use a variable (`$name`, `getvariable(…)`) outside its strings and comments?
pub fn uses(sql: &str) -> bool { sql.contains('$') && !names(sql).is_empty() || calls_getvariable(sql) }

/// Does `sql` say `getvariable` (any case)? Checked without making a copy: every statement asks.
pub fn calls_getvariable(sql: &str) -> bool { sql.as_bytes().windows(11).any(|w| w.eq_ignore_ascii_case(b"getvariable")) }

/// `$name` and `getvariable('name')` in `sql` → their values; Postgres's `$1`, `$2` left for
/// whoever binds them (`pg.rs`). Text that uses no variable is left as it is.
pub fn bound(sql: &str) -> Result<std::borrow::Cow<'_, str>> {
    Ok(match uses(sql) {
        true => crate::routines::bind_named(sql, &values(&HashMap::new()))?.into(),
        false => sql.into(),
    })
}

/// The `$name`s `sql` uses, outside strings, quoted names, comments and `$tag$` bodies, in order,
/// each once.
pub fn names(sql: &str) -> Vec<String> {
    let (mut out, mut i, b) = (Vec::<String>::new(), 0, sql.as_bytes());
    while i < b.len() {
        let rest = &sql[i..];
        let past = |end: &str, from: usize| rest[from..].find(end).map_or(rest.len(), |e| from + e + end.len());
        i += match b[i] {
            b'-' if rest.starts_with("--") => past("\n", 2),
            b'/' if rest.starts_with("/*") => past("*/", 2),
            b'\'' => past("'", 1),
            b'"' => past("\"", 1),
            b'$' => match crate::routines::dollar_tag(rest) {
                Some(tag) => past(tag, tag.len()),
                None => {
                    let n = rest[1..].find(|c: char| !(c.is_alphanumeric() || c == '_')).map_or(rest.len() - 1, |e| e);
                    let name = &rest[1..1 + n];
                    if name.starts_with(|c: char| c.is_alphabetic() || c == '_') && !out.iter().any(|o| o == name) {
                        out.push(name.to_string());
                    }
                    1 + n
                }
            },
            _ => rest.chars().next().map_or(1, char::len_utf8),
        };
    }
    out
}

/// Carry out a `DECLARE`, an assignment or a `RESET VARIABLE`: the value worked out once, as the
/// caller, and kept.
pub async fn apply(app: &App, c: Change) -> Result<Outcome> {
    let (name, var) = match c {
        Change::Reset(name) => {
            take(&name);
            vars(|m| m.remove(&name))?;
            return Ok(Outcome::Done(j!({"variable": name, "reset": true})));
        }
        Change::Set { name, value } => {
            ensure!(!value.is_empty(), "${name} = what? (`${name} = current_date - 1`)");
            let declared = vars(|m| m.get(&name).and_then(|v| v.declared.clone()))?;
            let v = evaluate(app, &value, declared.as_deref().map(Cast::Sql)).await.with_context(|| match &declared {
                Some(t) => format!("${name} is {t}"),
                None => format!("${name}"),
            })?;
            take(&name);
            (name, Var { declared, ..v })
        }
        Change::Declare { name, ty, default, parameter } => {
            if let Some(t) = &ty {
                let known = crate::routines::data_type(t).is_ok_and(|t| !matches!(t, datafusion::sql::sqlparser::ast::DataType::Custom(..)));
                ensure!(known, "DECLARE ${name} {t}: not a type (DATE, VARCHAR, BIGINT, DECIMAL(10, 2), …)");
            }
            ensure!(default.as_deref() != Some(""), "DECLARE ${name} = what? (`DECLARE ${name} DATE = current_date - 1`)");
            let given = take(&name);
            ensure!(parameter || given.is_none(), "a value was given for ${name}, which this script declares as its own variable: `DECLARE PARAMETER ${name} …` takes one");
            let v = match (given, &default) {
                (Some(given), _) => {
                    let arrow = match (&ty, &default) {
                        (None, Some(d)) => type_of(app, d).await, // (cast to its default's type)
                        _ => None,
                    };
                    let to = ty.as_deref().map(Cast::Sql).or(arrow.as_deref().map(Cast::Arrow));
                    evaluate(app, &crate::routines::literal(&given)?.to_string(), to).await.with_context(|| format!("the value given for ${name}"))?
                }
                (None, Some(d)) => evaluate(app, d, ty.as_deref().map(Cast::Sql)).await.with_context(|| format!("${name}"))?,
                (None, None) if !parameter => evaluate(app, "NULL", ty.as_deref().map(Cast::Sql)).await?, // (its own, no value yet: NULL, as SQL's DECLARE)
                (None, None) => match (vars(|m| m.get(&name).cloned())?, &ty) {
                    (Some(v), None) => v,
                    (Some(v), Some(t)) => evaluate(app, &v.sql, Some(Cast::Sql(t))).await.with_context(|| format!("${name}"))?,
                    (None, _) => bail!("no value for ${name}: DECLARE PARAMETER ${name} has no default, so it must be given (CALL run('…', {name} => …), pondra run … --{name} …, or the bar above the file)"),
                },
            };
            (name, Var { declared: ty, ..v })
        }
    };
    vars(|m| m.insert(name.clone(), var.clone()))?;
    Ok(Outcome::Done(j!({"variable": name, "value": var.shown, "type": var.ty})))
}

/// The type a value is cast to: SQL's (declared) or Arrow's (a default's).
enum Cast<'a> {
    Sql(&'a str),
    Arrow(&'a str),
}

/// `expr`, worked out once (its `$name`s bound), cast as asked: one value.
async fn evaluate(app: &App, expr: &str, cast: Option<Cast<'_>>) -> Result<Var> {
    let e = match cast {
        Some(Cast::Sql(t)) => format!("CAST(({expr}) AS {t})"),
        Some(Cast::Arrow(t)) => format!("arrow_cast(({expr}), '{t}')"),
        None => format!("({expr})"),
    };
    let sql = crate::routines::prepare(&app.lake, &format!("SELECT {e} AS v"), &HashMap::new(), &HashMap::new()).await?;
    let rows = answer(app, &sql).await?;
    let row = datafusion::arrow::compute::concat_batches(&rows[0].schema(), &rows)?;
    ensure!(row.num_rows() == 1, "a variable holds one value: {expr} gave {} rows", row.num_rows());
    var_of(&row)
}

/// What `sql` answers. One that reads nothing (a script's `IF $i < 10`, `$i = $i + 1`) is planned
/// and its values folded, as DataFusion folds constants, with no query run; anything else (a
/// table, an aggregate, a function that may answer otherwise next time) runs as a query.
pub async fn answer(app: &App, sql: &str) -> Result<Vec<RecordBatch>> {
    if let Some(rows) = folded(&app.lake, sql).await {
        return Ok(rows);
    }
    Box::pin(app.query(sql, Some("0"))).await
}

async fn folded(lake: &crate::store::Lake, sql: &str) -> Option<Vec<RecordBatch>> {
    use datafusion::logical_expr::{simplify::SimplifyContext, Expr, LogicalPlan};
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    if sql.as_bytes().windows(4).any(|w| w.eq_ignore_ascii_case(b"from")) {
        return None; // (a table or a subquery's)
    }
    let plan = lake.session_with(1).state().create_logical_plan(sql).await.ok()?;
    let simplifier = ExprSimplifier::new(SimplifyContext::builder().with_current_time().build());
    let literal = |e: &Expr| match simplifier.simplify(e.clone().unalias()).ok()? {
        Expr::Literal(v, _) => Some(v),
        _ => None,
    };
    let LogicalPlan::Projection(p) = &plan else { return None };
    let one = |input: &LogicalPlan| matches!(input, LogicalPlan::EmptyRelation(e) if e.produce_one_row);
    let rows = match p.input.as_ref() {
        input if one(input) => 1,
        LogicalPlan::Filter(f) if one(&f.input) => match literal(&f.predicate)? {
            datafusion::scalar::ScalarValue::Boolean(b) => b.unwrap_or(false) as usize,
            _ => return None,
        },
        _ => return None,
    };
    let schema = Arc::new(plan.schema().as_arrow().clone());
    let columns = p.expr.iter().zip(schema.fields()).map(|(e, f)| {
        let v = literal(e)?;
        let v = if &v.data_type() == f.data_type() { v } else { v.cast_to(f.data_type()).ok()? };
        v.to_array_of_size(rows).ok()
    });
    Some(vec![RecordBatch::try_new(schema.clone(), columns.collect::<Option<Vec<_>>>()?).ok()?])
}

/// The Arrow type of a default (planned, not run), for a value given in its place; None if it
/// can't be planned yet (a table it reads made later in the file).
async fn type_of(app: &App, default: &str) -> Option<String> {
    let sql = crate::routines::prepare(&app.lake, &format!("SELECT ({default}) AS v LIMIT 0"), &HashMap::new(), &HashMap::new()).await.ok()?;
    let rows = Box::pin(app.query(&sql, Some("0"))).await.ok()?;
    Some(rows.first()?.schema().field(0).data_type().to_string())
}

fn var_of(row: &RecordBatch) -> Result<Var> {
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    let col = row.column(0);
    let ty = col.data_type();
    ensure!(!ty.is_nested(), "a variable holds one value (a number, a string, a date, …), not a {ty}");
    let shown = match col.is_null(0) {
        true => None,
        false => Some(ArrayFormatter::try_new(col.as_ref(), &FormatOptions::default())?.value(0).to_string()),
    };
    let text = shown.as_ref().map_or("NULL".to_string(), |s| format!("'{}'", s.replace('\'', "''")));
    Ok(Var { sql: format!("arrow_cast({text}, '{ty}')"), shown, ty: ty.to_string(), declared: None })
}

/// The variables there are now, given values not yet set among them: (name, value, type, declared).
pub fn listed() -> Vec<(String, Option<String>, Option<String>, Option<String>)> {
    let mut out: BTreeMap<String, (Option<String>, Option<String>, Option<String>)> =
        vars(|m| m.iter().map(|(k, v)| (k.clone(), (v.shown.clone(), Some(v.ty.clone()), v.declared.clone()))).collect()).unwrap_or_default();
    for (k, v) in GIVEN.try_with(|g| g.lock().unwrap().clone()).unwrap_or_default().into_iter().filter(|(k, _)| !k.starts_with(|c: char| c.is_ascii_digit())) {
        let shown = match &v {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            Value::Object(o) => o.get("sql").and_then(Value::as_str).map(String::from),
            v => Some(v.to_string()),
        };
        out.insert(k, (shown, None, None));
    }
    out.into_iter().map(|(k, (v, t, d))| (k, v, t, d)).collect()
}

/// Does `sql` read `pondra.variables`? (The session's: run here, never a remembered answer.)
pub fn mentioned(sql: &str) -> bool { sql.to_ascii_lowercase().contains("pondra.variables") }

/// `pondra.variables`: the variables there are now (`listed`).
pub fn table() -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    let all = listed();
    let column = |f: fn(&(String, Option<String>, Option<String>, Option<String>)) -> Option<String>| Arc::new(all.iter().map(f).collect::<StringArray>()) as ArrayRef;
    let schema = Arc::new(Schema::new(["name", "value", "type", "declared"].map(|n| Field::new(n, DataType::Utf8, n != "name")).to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![column(|v| Some(v.0.clone())), column(|v| v.1.clone()), column(|v| v.2.clone()), column(|v| v.3.clone())])?;
    Ok(Arc::new(datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?))
}

// ---------------------------------------------------------------- a file's parameters

/// A parameter of a SQL file: a `DECLARE PARAMETER` (its type and default as written), or a `$name`
/// used before anything sets it (required, no type); a plain `DECLARE` is the file's own variable,
/// never one. Its description is the comment just above its `DECLARE`, else one after it on its line
/// (`DECLARE PARAMETER $d = 's1'; -- the sensor`), or a comment line `-- $name: …` anywhere.
#[derive(Debug, serde::Serialize)]
pub struct Param {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: Option<String>,
    pub default: Option<String>,
    pub required: bool,
    pub description: Option<String>,
    /// Declared (`DECLARE PARAMETER`), not only used.
    #[serde(skip)]
    pub declared: bool,
}

/// A SQL file's parameters, in the order it declares or first uses them.
pub fn parameters(text: &str) -> Vec<Param> {
    static NAMED: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?m)^\s*--\s*\$([A-Za-z_]\w*)\s*[:—–-]\s*(.+?)\s*$").expect("a regex"));
    let (mut out, mut set): (Vec<Param>, Vec<String>) = (vec![], vec![]);
    let all = crate::routines::split(text);
    for (i, s) in all.iter().enumerate() {
        let s = if i > 0 { its_own(s) } else { s.as_str() };
        match change(s) {
            Some(Change::Declare { name, ty, default, parameter }) if !set.contains(&name) => {
                if let Some(d) = &default {
                    unused(d, &set, &mut out);
                }
                out.retain(|p| p.name != name); // (used before its DECLARE: the DECLARE says what it is)
                if parameter {
                    let required = default.is_none();
                    out.push(Param { name: name.clone(), ty, default, required, description: above(s).or_else(|| after(all.get(i + 1))), declared: true });
                }
                set.push(name);
                continue;
            }
            Some(Change::Set { name, value }) => {
                unused(&value, &set, &mut out);
                set.push(name);
                continue;
            }
            _ if crate::script::is(s) => {
                let bound = crate::script::binds(s); // (a loop's row, INTO, a block's DECLARE: the script's own)
                unused(s, &[&set[..], &bound[..]].concat(), &mut out);
                set.extend(bound);
                continue;
            }
            _ => {}
        }
        unused(s, &set, &mut out);
    }
    for c in NAMED.captures_iter(text) {
        if let Some(p) = out.iter_mut().find(|p| p.name == c[1]) {
            p.description = Some(c[2].to_string());
        }
    }
    out
}

/// The `$name`s `sql` uses that nothing has set yet: required parameters.
fn unused(sql: &str, set: &[String], out: &mut Vec<Param>) {
    for n in names(sql) {
        if !set.contains(&n) && !out.iter().any(|p| p.name == n) {
            out.push(Param { name: n, ty: None, default: None, required: true, description: None, declared: false });
        }
    }
}

/// A statement without what follows the `;` before it on that line (a blank, or the comment
/// that line's statement ends with: `after`).
fn its_own(stmt: &str) -> &str {
    match stmt.split_once('\n') {
        Some((first, rest)) if first.trim().is_empty() || first.trim_start().starts_with("--") => rest,
        _ => stmt,
    }
}

/// The comment after a statement on its line (the start of the next one's text), as one line.
fn after(next: Option<&String>) -> Option<String> {
    let first = next?.split('\n').next()?.trim_start();
    first.strip_prefix("--").map(|t| t.trim_start_matches('-').trim().to_string()).filter(|t| !t.is_empty() && !t.starts_with('$'))
}

/// The comment lines just above a statement's code (`-- …`), as one line.
fn above(stmt: &str) -> Option<String> {
    let lead = &stmt[..stmt.len() - crate::write::first_word(stmt).len()];
    let lines: Vec<&str> = lead.lines().map(str::trim).rev().skip_while(|l| l.is_empty()).take_while(|l| l.starts_with("--")).collect();
    let text = lines.iter().rev().map(|l| l.trim_start_matches('-').trim()).collect::<Vec<_>>().join(" ");
    Some(text).filter(|t| !t.is_empty() && !t.starts_with('$'))
}

/// `pondra.parameters('etl/orders.sql')` in a query: the file's parameters, as rows (name, type,
/// default, required, description) put in its place where SQL comes in.
pub async fn parameters_in(lake: &crate::store::Lake, sql: &str) -> Result<String> {
    static CALL: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\bpondra\.parameters\s*\(\s*'((?:[^']|'')*)'\s*\)").expect("a regex"));
    if !sql.as_bytes().windows(10).any(|w| w.eq_ignore_ascii_case(b"parameters")) || !CALL.is_match(sql) {
        return Ok(sql.to_string());
    }
    let mut out = String::new();
    let mut last = 0;
    for c in CALL.captures_iter(sql) {
        let m = c.get(0).expect("a match");
        let path = crate::files::under_files(&c[1].replace("''", "'"));
        let (bytes, _) = lake.file(&path).await.map_err(|e| match e.downcast_ref::<object_store::Error>() {
            Some(object_store::Error::NotFound { .. }) => anyhow::anyhow!("pondra.parameters: no file {path}"),
            _ => e,
        })?;
        let text = String::from_utf8(bytes.to_vec()).with_context(|| format!("pondra.parameters: {path} is not text"))?;
        out.push_str(&sql[last..m.start()]);
        out.push_str(&rows(&crate::workspace::parameters(&path, &text)?.unwrap_or_default()));
        last = m.end();
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

/// Parameters as a subquery of rows, in the file's order (one VALUES list: one batch).
fn rows(all: &[Param]) -> String {
    let s = |v: &Option<String>| v.as_ref().map_or("CAST(NULL AS VARCHAR)".to_string(), |t| format!("CAST('{}' AS VARCHAR)", t.replace('\'', "''")));
    let none = [Param { name: String::new(), ty: None, default: None, required: false, description: None, declared: false }];
    let each = match all.is_empty() { true => &none[..], false => all };
    let values = each.iter().map(|p| format!("({}, {}, {}, {}, {})", s(&Some(p.name.clone())), s(&p.ty), s(&p.default), p.required, s(&p.description))).collect::<Vec<_>>().join(", ");
    let only = if all.is_empty() { " WHERE false" } else { "" };
    format!("(SELECT * FROM (VALUES {values}) AS parameters(name, type, \"default\", required, description){only})")
}
