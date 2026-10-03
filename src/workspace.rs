//! The workspace (ADR-033): the lake's own files, run. `CALL run('etl/orders.sql', day => DATE
//! '2026-09-29')` runs a `.sql` file's statements (`$day` bound, never pasted in), a `.py` file
//! (`day` a variable; its `# %% tags=["parameters"]` cell's values replaced, as papermill does), or a
//! notebook (its `parameters` cell's values replaced), as its caller, from every door: SQL over HTTP and Postgres, the clients' `run`, MCP, a task's
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
use std::sync::LazyLock;

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
    if let Some(takes) = parameters(&path, &text)? {
        for f in row.schema().fields() {
            let names = takes.iter().map(|p| format!("{}{}", if kind == "py" { "" } else { "$" }, p.name)).collect::<Vec<_>>();
            let list = if names.is_empty() { "it takes none".to_string() } else { format!("it takes {}", names.join(", ")) };
            let why = if kind == "py" { "its # %% tags=[\"parameters\"] cell sets them" } else { "a plain DECLARE is the file's own variable, DECLARE PARAMETER one a run may give" };
            ensure!(takes.iter().any(|p| p.name == *f.name()), "run: {path} has no parameter {} ({list}); {why}", f.name());
        }
    }
    let runs_python = kind == "py" || kind == "ipynb" && notebook(&text)?.iter().any(|c| c.python);
    ensure!(!runs_python || who.role >= Role::Admin, "run: {path} runs Python on the node: it needs an admin token, as DO does");
    let log = crate::runs::Run::start(app, &format!("{path}@{version}"), who.role, job.as_deref(), &row, id);
    let inner = Who { depth: who.depth + 1, ..who };
    let mut heard = vec![];
    let values = crate::routines::values_of(&row)?;
    let session = format!("run-{}", crate::runs::new_id());
    let (none, no_views) = (HashMap::new(), HashMap::new());
    let out = crate::vars::own(values, async { // (the run's variables, from the values given: every cell's, and its Python's `db.vars`)
        match kind.as_str() {
            "sql" => Box::pin(crate::routines::script(app, &text, &none, &no_views, inner, job)).await,
            "py" => match cells_py(&text).into_iter().position(|c| c.0) {
                // (its parameters cell first, the values given over its defaults, then the rest: papermill's order)
                Some(at) => {
                    let all = cells_py(&text);
                    let (head, rest) = all.split_at(at + 1);
                    let lines = |c: &[(bool, String)]| c.iter().map(|c| c.1.as_str()).collect::<Vec<_>>().join("\n");
                    python(app, &path, &lines(head), None, &session, inner, None, &mut heard).await?;
                    python(app, &path, "", Some(&row), &session, inner, None, &mut heard).await?;
                    let padded = "\n".repeat(head.iter().map(|c| c.1.split('\n').count()).sum::<usize>()) + &lines(rest); // (its lines numbered as in the file)
                    python(app, &path, &padded, None, &session, inner, job, &mut heard).await
                }
                None => python(app, &path, &text, Some(&row), &session, inner, job, &mut heard).await,
            },
            _ => Box::pin(cells(app, &path, &text, &row, &session, inner, job, &mut heard)).await,
        }
    })
    .await;
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
async fn cells(app: &App, path: &str, text: &str, row: &RecordBatch, session: &str, who: Who, job: Option<String>, heard: &mut Vec<String>) -> Result<Outcome> {
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
                let out = Box::pin(crate::routines::script(app, &c.code, &HashMap::new(), &HashMap::new(), who, job)).await; // (`$name`: the run's variables and values)
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

// ---------------------------------------------------------------- a file's parameters

/// What a file a run may be given (ADR-043), or None when it doesn't say and takes any value: a
/// `.sql` file's `DECLARE PARAMETER`s and `$name`s used before anything sets them; a `.py` file's
/// `# %% tags=["parameters"]` cell (jupytext's and papermill's); a notebook's cell tagged
/// `parameters` (Python or `%%sql`) and the `DECLARE PARAMETER`s of its SQL cells.
pub fn parameters(path: &str, text: &str) -> Result<Option<Vec<crate::vars::Param>>> {
    Ok(match path.rsplit('.').next().unwrap_or_default().to_ascii_lowercase().as_str() {
        "py" => cells_py(text).into_iter().find(|c| c.0).map(|c| python_parameters(&c.1)),
        "ipynb" => {
            let cells = notebook(text)?;
            let sql = cells.iter().filter(|c| !c.python).map(|c| c.code.as_str()).collect::<Vec<_>>().join(";\n");
            let mut out: Vec<_> = cells.iter().filter(|c| c.python && c.parameters).flat_map(|c| python_parameters(&c.code)).collect();
            out.extend(crate::vars::parameters(&sql).into_iter().filter(|p| p.declared)); // (a `$name` its SQL uses may be its Python's `db.vars`)
            (!out.is_empty() || cells.iter().any(|c| c.parameters)).then_some(out)
        }
        _ => Some(crate::vars::parameters(text)),
    })
}

/// A `.py` file's cells, as jupytext's percent format cuts them (`# %%` lines, each its cell's
/// first), each with whether it is the parameters cell (`# %% tags=["parameters"]`); joined with
/// `\n` they are the file again.
fn cells_py(text: &str) -> Vec<(bool, String)> {
    static MARK: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"^#\s*%%").expect("a regex"));
    static TAGGED: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"tags\s*=\s*\[[^\]]*["']parameters["']"#).expect("a regex"));
    let mut out: Vec<(bool, Vec<&str>)> = vec![(false, vec![])];
    for line in text.split('\n') {
        if MARK.is_match(line) {
            out.push((TAGGED.is_match(line), vec![]));
        }
        out.last_mut().expect("a cell").1.push(line);
    }
    out.into_iter().filter(|c| !c.1.is_empty()).map(|(p, lines)| (p, lines.join("\n"))).collect()
}

/// A Python parameters cell's names: each `name = value` or `name: type = value` at its top level
/// (`name: type` alone is required), its type as SQL's (`date` DATE, `int` BIGINT, …, else what
/// its default is), what the comment after it or the lines just above it say.
fn python_parameters(cell: &str) -> Vec<crate::vars::Param> {
    static ONE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"^([A-Za-z_]\w*)\s*(?::\s*([^=]+?)\s*)?(?:=([^=].*?|))?\s*$").expect("a regex"));
    let (mut out, mut said): (Vec<crate::vars::Param>, Vec<String>) = (vec![], vec![]);
    for line in cell.lines() {
        let (code, comment) = split_comment(line);
        if line.trim_start().starts_with('#') {
            if !line.trim_start().starts_with("# %%") { said.push(line.trim_start().trim_start_matches('#').trim().to_string()); }
            continue;
        }
        let about = (!comment.is_empty()).then(|| comment.to_string()).or_else(|| Some(said.join(" ")).filter(|t| !t.is_empty()));
        said.clear();
        let Some(c) = ONE.captures(code.trim_end()).filter(|_| !code.starts_with(char::is_whitespace)) else { continue };
        let (hint, default) = (c.get(2).map(|t| t.as_str().trim().to_string()), c.get(3).map(|d| d.as_str().trim().to_string()).filter(|d| !d.is_empty()));
        if hint.is_none() && default.is_none() || out.iter().any(|p| p.name == c[1]) {
            continue;
        }
        let ty = hint.as_deref().and_then(sql_type).or_else(|| default.as_deref().and_then(literal_type));
        out.push(crate::vars::Param { name: c[1].to_string(), ty, required: default.is_none(), default, description: about, declared: true });
    }
    out
}

/// A line's code and its comment (`#` outside quotes).
fn split_comment(line: &str) -> (&str, &str) {
    let mut quote = None;
    for (i, ch) in line.char_indices() {
        match (quote, ch) {
            (None, '\'' | '"') => quote = Some(ch),
            (Some(q), c) if c == q => quote = None,
            (None, '#') => return (&line[..i], line[i + 1..].trim()),
            _ => {}
        }
    }
    (line, "")
}

/// A Python annotation as SQL's type.
fn sql_type(hint: &str) -> Option<String> {
    let t = match hint.rsplit('.').next().unwrap_or(hint) {
        "date" => "DATE",
        "datetime" => "TIMESTAMP",
        "int" => "BIGINT",
        "float" => "DOUBLE",
        "str" => "VARCHAR",
        "bool" => "BOOLEAN",
        _ => return None,
    };
    Some(t.to_string())
}

/// A Python literal's type, as SQL's.
fn literal_type(v: &str) -> Option<String> {
    let t = match v {
        "True" | "False" => "BOOLEAN",
        v if v.parse::<i64>().is_ok() => "BIGINT",
        v if v.parse::<f64>().is_ok() => "DOUBLE",
        v if v.starts_with(['"', '\'']) => "VARCHAR",
        _ => return None,
    };
    Some(t.to_string())
}
