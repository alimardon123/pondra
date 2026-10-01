//! The workspace (ADR-033): the lake's own files, run. `CALL run('etl/orders.sql', day => DATE
//! '2026-09-29')` runs a `.sql` file's statements (`$day` bound, never pasted in), a `.py` file
//! (`day` a variable), or a notebook (its `parameters` cell's values replaced, as papermill does),
//! as its caller, from every door: SQL over HTTP and Postgres, the clients' `run`, MCP, a task's
//! schedule, `pondra.start('run', …)`. A file may run another. Each run is a row of the run log
//! (`pondra.runs`), named `files/<path>@<version>`: the file and the version that ran.
use crate::auth::Role;
use crate::routines::{Outcome, Who};
use crate::server::App;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::sql::sqlparser::ast::{FunctionArg, FunctionArgExpr};
use serde_json::{json as j, Value};
use std::collections::HashMap;

/// Is a `CALL`'s procedure this one: `run`, or `pondra.run`?
pub fn is_run(name: &str) -> bool { matches!(name.to_ascii_lowercase().as_str(), "run" | "pondra.run") }

const USAGE: &str = "run('etl/orders.sql', day => DATE '2026-09-29'): the file, then its parameters by name";

/// The file's path and its parameters' values (one row), worked out once, as the caller.
pub async fn arguments(app: &App, args: &[FunctionArg]) -> Result<(String, RecordBatch)> {
    let (mut select, mut names) = (vec![], std::collections::HashSet::new());
    for (i, a) in args.iter().enumerate() {
        select.push(match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) if i == 0 => format!("CAST(({e}) AS VARCHAR) AS \"file to run\""),
            FunctionArg::Named { name, arg: FunctionArgExpr::Expr(e), .. } if i > 0 => {
                let n = crate::write::ident(name);
                ensure!(names.insert(n.clone()), "run: {n} given twice");
                format!("({e}) AS \"{}\"", n.replace('"', "\"\""))
            }
            _ => bail!(USAGE),
        });
    }
    ensure!(!select.is_empty(), USAGE);
    let rows = app.query(&format!("SELECT {}", select.join(", ")), Some("0")).await.context("run's arguments")?;
    let row = datafusion::arrow::compute::concat_batches(&rows[0].schema(), &rows)?;
    let path = datafusion::arrow::compute::cast(row.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?; // (VARCHAR is Utf8View)
    let path = path.as_any().downcast_ref::<StringArray>().filter(|p| !p.is_null(0)).map(|p| p.value(0).to_string()).context(USAGE)?;
    Ok((path, row.project(&(1..row.num_columns()).collect::<Vec<_>>())?))
}

/// Run a file of the lake's, logged: its outcome is its last statement's, expression's or cell's.
pub async fn run(app: &App, args: &[FunctionArg], who: Who, job: Option<String>, id: Option<String>) -> Result<Outcome> {
    ensure!(who.depth < 16, "run: files running files 16 deep (a loop?)");
    let (path, row) = arguments(app, args).await?;
    let path = match crate::files::under_files(&path) {
        p if p.starts_with("files/notebooks/") && p.matches('/').count() == 2 && !p.ends_with(".ipynb") => latest(app, &p).await?, // (a notebook by name: its newest version)
        p => p,
    };
    let kind = path.rsplit('.').next().unwrap_or_default().to_ascii_lowercase();
    ensure!(matches!(kind.as_str(), "sql" | "py" | "ipynb"), "run: {path}: a .sql, .py or .ipynb file runs");
    let (bytes, version) = app.lake.file(&path).await.map_err(|e| match e.downcast_ref::<object_store::Error>() {
        Some(object_store::Error::NotFound { .. }) => anyhow::anyhow!("run: no file {path}"),
        _ => e,
    })?;
    let text = String::from_utf8(bytes.to_vec()).with_context(|| format!("run: {path} is not text"))?;
    let runs_python = kind == "py" || kind == "ipynb" && notebook(&text)?.iter().any(|c| c.python);
    ensure!(!runs_python || who.role >= Role::Admin, "run: {path} runs Python on the node: it needs an admin token, as DO does");
    let log = crate::runs::Run::start(app, &format!("{path}@{version}"), who.role, job.as_deref(), &row, id);
    let inner = Who { depth: who.depth + 1, ..who };
    let mut heard = vec![];
    let values = crate::routines::values_of(&row)?;
    let session = format!("run-{}", crate::runs::new_id());
    let out = match kind.as_str() {
        "sql" => Box::pin(crate::routines::script(app, &text, &values, &HashMap::new(), inner, job)).await,
        "py" => python(app, &path, &text, Some(&row), &session, inner, job, &mut heard).await,
        _ => Box::pin(cells(app, &path, &text, &row, &values, &session, inner, job, &mut heard)).await,
    };
    crate::python::end_session(&session);
    let _ = log.end(app, &out, heard);
    out.map_err(|e| e.context(format!("run {path}")))
}

/// A notebook by name: `notebooks/<name>.ipynb` (its versions kept with it, ADR-035 §8), or, saved
/// before that, its newest `notebooks/<name>/<time>.ipynb` (the times sort).
async fn latest(app: &App, notebook: &str) -> Result<String> {
    use futures::TryStreamExt;
    if app.lake.version(&format!("{notebook}.ipynb")).await.is_ok() {
        return Ok(format!("{notebook}.ipynb"));
    }
    let found: Vec<object_store::ObjectMeta> = app.lake.store.list(Some(&object_store::path::Path::from(format!("{notebook}/")))).try_collect().await?;
    found.into_iter().map(|m| m.location.to_string()).filter(|p| p.ends_with(".ipynb")).max().with_context(|| format!("run: no notebook {notebook} (none saved)"))
}

/// A notebook's code cells, in order: each SQL (`%%sql`) or Python, which one takes the
/// parameters (tagged `parameters`, as papermill's), and the name a SQL cell's answer has in
/// Python (`%%sql df <<`, as Pondra's Jupyter magic writes it).
struct Cell {
    python: bool,
    code: String,
    parameters: bool,
    name: Option<String>,
}

fn notebook(text: &str) -> Result<Vec<Cell>> {
    let nb: Value = serde_json::from_str(text).context("the notebook is not JSON")?;
    let source = |c: &Value| match &c["source"] {
        Value::Array(lines) => lines.iter().filter_map(Value::as_str).collect::<String>(),
        s => s.as_str().unwrap_or_default().to_string(),
    };
    let mut out = vec![];
    for c in nb["cells"].as_array().context("a notebook has cells")? {
        if c["cell_type"] != "code" {
            continue;
        }
        let src = source(c);
        let tags = c["metadata"]["tags"].as_array().map_or(false, |t| t.iter().any(|t| t == "parameters"));
        let (python, code, name) = match src.trim_start().strip_prefix("%%sql") {
            Some(rest) => {
                let (head, body) = rest.split_once('\n').unwrap_or((rest, ""));
                let name = head.trim().strip_suffix("<<").map(str::trim).filter(|n| !n.is_empty() && n.chars().all(|c| c.is_alphanumeric() || c == '_')).map(String::from);
                (false, body.to_string(), name)
            }
            None => (true, src.lines().map(|l| match l.trim_start().starts_with(['%', '!']) {
                true => format!("{}pass", &l[..l.len() - l.trim_start().len()]), // (a magic or a shell line: Jupyter's, not Python)
                false => l.to_string(),
            }).collect::<Vec<_>>().join("\n"), None),
        };
        if !code.trim().is_empty() {
            out.push(Cell { python, code, parameters: tags, name });
        }
    }
    Ok(out)
}

/// A notebook's cells, in order: SQL ones as scripts (`$name` bound), Python ones in one
/// namespace of the run's own. The given parameters are set as variables after the cell tagged
/// `parameters` (its values are the defaults), or before the first cell if none is.
#[allow(clippy::too_many_arguments)]
async fn cells(app: &App, path: &str, text: &str, row: &RecordBatch, values: &HashMap<String, Value>, session: &str, who: Who, job: Option<String>, heard: &mut Vec<String>) -> Result<Outcome> {
    let all = notebook(text)?;
    let at = all.iter().position(|c| c.parameters);
    let (mut last, mut pending) = (Outcome::Done(j!({"ran": path})), row.num_columns() > 0);
    for (i, c) in all.iter().enumerate() {
        if pending && c.python && at.map_or(true, |a| i > a) {
            python(app, path, "", Some(row), session, who, None, heard).await?; // (the values given, over the parameters cell's defaults)
            pending = false;
        }
        let (name, job) = (format!("{path}, cell {}", i + 1), job.as_ref().map(|j| format!("{j}:c{i}")));
        let ran_python = all[..i].iter().any(|c| c.python);
        let query = crate::write::parse(&c.code).is_none() && crate::routines::split(&c.code).len() == 1;
        last = match c.python {
            true => python(app, &name, &c.code, None, session, who, job, heard).await,
            // (a query naming a table of the run's Python: through Python, which sends it along)
            false if query && ran_python && python_tables(session, &c.code).await => {
                let v = c.name.as_deref().unwrap_or("_sql");
                python(app, &name, &format!("{v} = db.sql({})\n{v}", j!(c.code.trim())), None, session, who, job, heard).await
            }
            false => {
                let out = Box::pin(crate::routines::script(app, &c.code, values, &HashMap::new(), who, job)).await;
                if let (Ok(_), Some(v), true) = (&out, &c.name, query && all[i + 1..].iter().any(|c| c.python)) {
                    python(app, &name, &format!("{v} = db.sql({})", j!(c.code.trim())), None, session, who, None, heard).await?; // (its answer, a frame, for the Python cells after it)
                }
                out
            }
        }
        .map_err(|e| e.context(format!("cell {}", i + 1)))?;
    }
    Ok(last)
}

/// Does `sql` name a table the session's Python holds (pandas, Polars, Arrow, a frame)?
async fn python_tables(session: &str, sql: &str) -> bool {
    let Ok(v) = crate::python::variables(session).await else { return false };
    v["variables"].as_array().into_iter().flatten().any(|v| {
        let (name, kind) = (v["name"].as_str().unwrap_or_default(), v["type"].as_str().unwrap_or_default());
        ["pandas.", "polars.", "pyarrow.", "pondra.frame."].iter().any(|p| kind.starts_with(p)) && crate::ddl::mentions(sql, name)
    })
}

/// Python code in the run's own namespace (a worker for the run, as a notebook's kernel): `vars`
/// set as variables first; its last expression is its answer. Named `name` in tracebacks.
#[allow(clippy::too_many_arguments)]
async fn python(app: &App, name: &str, code: &str, vars: Option<&RecordBatch>, session: &str, who: Who, job: Option<String>, heard: &mut Vec<String>) -> Result<Outcome> {
    crate::python::ready(&format!("{name} is Python"))?;
    let lease = crate::auth::lend(who.role, who.files);
    let url = format!("http://{}", app.cluster.addr.replace("0.0.0.0", "127.0.0.1"));
    let head = j!({"op": "cell", "session": session, "name": name, "body": code, "url": url, "token": lease.0, "depth": who.depth, "job": job, "vars": vars.is_some()});
    let parts = vec![crate::query::ipc(&[vars.cloned().unwrap_or_else(|| RecordBatch::new_empty(std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty())))])?];
    let mut notice = |n: String| {
        let n = lease.redact(&n);
        crate::routines::heard(&n);
        heard.push(n);
    };
    let (answer, parts) = crate::python::ask_session(session, head, parts, None, &mut notice).await.map_err(|e| anyhow::anyhow!("{}", lease.redact(&format!("{e:#}"))))?;
    match answer["kind"].as_str() {
        Some("rows") => Ok(Outcome::Rows(crate::query::read_ipc(parts.first().context("no rows")?)?)),
        Some("images") => Ok(Outcome::Done(j!({"ran": name, "images": answer["images"]}))),
        Some("sql") => Box::pin(crate::routines::one(app, answer["sql"].as_str().unwrap_or_default(), who, None)).await,
        _ => Ok(Outcome::Done(j!({"ran": name}))),
    }
}
