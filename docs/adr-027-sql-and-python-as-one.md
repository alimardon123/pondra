# ADR-027: SQL and Python as one — functions and procedures (round 24)

**Date:** 2026-09-28 · **Status:** proposed (for the owner) · **Follows:** ADR-023 (macros and procedures), ADR-025, ADR-026

## Context

The owner, 2026-09-28:

- **Call macros functions**, as Postgres does, so that more people recognise them.
- **Functions and procedures may both be written in Python.** Functions may have limits.
  Procedures should be flexible: from a SQL cell, someone should be able to send an email, which
  Python's libraries do well and SQL can't.
- **A procedure shouldn't have to return anything.**
- **The Python decorator should be simpler, with fewer restrictions.**
- **The design:** SQL and Python should be interchangeable, and easy for someone who works only
  in SQL or only in Python. It should also leave room for a platform on top: notebooks in the
  catalog, and Python over the unstructured data Pondra already keeps.

Round 22 (ADR-023) built:

- DuckDB's `CREATE MACRO`, expanded in the syntax tree;
- procedures in SQL or Python, `CALL`ed with the caller's rights;
- a Python procedure run as a new process per call (0.1–0.2 s), its last expression the answer;
- `@con.procedure`, whose function must take the connection first.

Functions of your own were Arrow Flight servers the user runs (`POST /functions`).

Frictions:

- A notebook function's imports and helpers don't travel with it, so `smtplib` at the top of a
  notebook is a `NameError` on the node.
- There is no Python function to use inside a query.
- What a procedure prints goes to the node's log, not to whoever called it.
- Nothing runs on a schedule.
- Credentials have nowhere to live.

## Decision

### 1. Postgres's words: `CREATE FUNCTION` and `CREATE PROCEDURE`, in SQL or Python

```sql
-- SQL functions: what macros were (CREATE MACRO stays, as DuckDB's name for the same thing)
CREATE FUNCTION net(x DOUBLE, rate DOUBLE DEFAULT 0.2) RETURNS DOUBLE RETURN x * (1 - rate);
CREATE FUNCTION recent(days INT) RETURNS TABLE (id BIGINT, amount DOUBLE) LANGUAGE sql
  AS $$ SELECT id, amount FROM orders WHERE ts > now() - days * INTERVAL '1 day' $$;

-- Python functions: PL/Python's form (the body is a function's; parameters by name)
CREATE FUNCTION slug(title VARCHAR) RETURNS VARCHAR LANGUAGE python AS $$
    import re
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
$$;
SELECT slug(title), net(price) FROM posts, recent(7);
```

- **Postgres's clauses are accepted:** `RETURNS … RETURN expr`, `LANGUAGE sql AS $$ SELECT … $$`,
  `RETURNS TABLE (…)` / `SETOF`, `$1`-style parameters, `IMMUTABLE | STABLE | VOLATILE` (a query
  calling a volatile one isn't answered from the result cache), `STRICT` (NULL in, NULL out,
  without calling), `DROP FUNCTION`, `SHOW FUNCTIONS`, `information_schema.routines`.
  `LANGUAGE plpython3u` is `python`, and a `plpy` module (`plpy.execute`, `plpy.notice`) is there,
  so PL/Python code from Postgres runs as it is.
- **SQL functions are expanded in place**, as macros are (invariant 82). A Python function is a
  DataFusion function whose batches go to the node's Python workers (§4). A query using it
  spreads like any other: each node runs its own rows through its own workers.
- **Python functions come in three shapes:**
  - per row (the default);
  - vectorized (`WITH (vectorized = true)`: the arguments are pyarrow arrays, a whole batch, and
    it returns one);
  - a table (`RETURNS TABLE (…)`: it returns or yields rows, a DataFrame or an Arrow table, as in
    `SELECT * FROM fetch_rates('USD')`).
- **Functions have limits, since a query may run one over millions of rows on every node:**
  - no connection back to the lake;
  - a time limit per batch (60 s by default);
  - a worker that goes over its memory is replaced.

  They may import any library and call out, so a geocoding API or a PDF parser works.

### 2. Procedures: anything Python can do, as the caller

```sql
CREATE PROCEDURE send_report(day DATE, recipients VARCHAR[]) LANGUAGE python AS $$
    import smtplib
    from email.message import EmailMessage
    top = pondra.sql("SELECT item, sum(qty) AS sold FROM orders WHERE ts::DATE = $day "
                     "GROUP BY item ORDER BY sold DESC LIMIT 10", day=day).to_pandas()
    s = pondra.secret("smtp")
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = f"Top items {day}", s["user"], ", ".join(recipients)
    msg.set_content(top.to_string(index=False))
    with smtplib.SMTP_SSL(s["host"]) as smtp:
        smtp.login(s["user"], s["password"])
        smtp.send_message(msg)
    print(f"sent to {len(recipients)}")
$$;

CALL send_report(current_date - 1, ['ann@example.com', 'bo@example.com']);
-- NOTICE: sent to 2
```

- **The body is ordinary Python.** It can import anything, use files on the node's machine, and
  call the network: mail, HTTP APIs, Slack, cloud SDKs.
  - `return` is optional. With none, the call answers nothing.
  - 0.22's rule still holds: if the last line is an expression, that is the answer.
  - An answer can be a frame (the node runs its SQL, so it may spread), a table, a value, or
    nothing.
- **Inside a routine, `pondra.sql`, `pondra.table`, `pondra.call` and `pondra.secret` use the
  caller's connection**, lent as now: the caller's rights, for as long as the call runs. `con`
  is still a name there. The same `pondra.sql(…)` in a notebook uses the notebook's connection,
  as `duckdb.sql` does, so code moves between the two unchanged.
- **What a procedure prints goes back to its caller:**
  - the shell prints it;
  - the Postgres port sends it as `NOTICE` (psql shows it);
  - HTTP and the clients return it with the answer;
  - it is also kept in the run log (§3).
- **Credentials come from `CREATE SECRET`** (ADR-026), never from the body.
- **Exactly-once as now:** a `CALL` with a job id numbers the procedure's writes under that job.
  Mail and HTTP calls are outside the lake, so a retried call sends again unless the procedure
  checks the run log.

### 3. On a schedule, and a run log

```sql
CREATE TASK nightly_report SCHEDULE 'cron 0 2 * * * UTC'
  AS CALL send_report(current_date - 1, ['ann@example.com']);
SELECT * FROM pondra.runs WHERE routine = 'send_report' ORDER BY started DESC;
```

- **A schedule runs on the leader, once per tick.** The tick commits as a producer's seq, as a
  window's emission does (invariant 24), so a failover neither skips it nor runs it twice.
- **`pondra.runs`** is a system table: every call's routine, caller, start, end, outcome, notices
  and error.
- **A long job can be started without waiting:** `db.call(…, wait=False)` in Python, and
  `SELECT pondra.start('p', …)` in SQL, answer a run id.

### 4. Python beside every node: a pool of warm workers

A node started with `--python` (as now) keeps up to one Python worker per core,
`python -m pondra.worker`:

- **Started when first needed, stopped after a minute idle**, so nothing is held when unused
  (principle 6).
- **A worker imports `pondra` and pyarrow once**, then serves two kinds of request as Arrow over
  its standard input and output:
  - `apply`: a function's body and a batch, answered with an array or rows;
  - `call`: a procedure's body, arguments and lent token, answered with notices, then the answer.
- **Bodies are compiled once per worker.** Each call gets a fresh namespace; imported modules
  stay, which is why a `CALL` takes milliseconds, not 0.15 s.
- **A worker that dies, or goes over its time or memory, is replaced.** Its call fails with the
  reason.
- **Packages:** `WITH (packages = 'requests, jinja2')` on a routine has every node install them
  once, into an environment kept under the node's cache and keyed by that list (uv when it is
  there, else pip). Without it, a routine uses the `--python` environment.
- **Across nodes:** a query calling a Python function spreads only to nodes that run Python.
  The rest leave it to the others, and one node runs it when none of the others can.

Python still runs beside the node, never inside it: the binary stays one small file for glibc
2.17, and a crashing library can't take a node down.

### 5. The decorators: a notebook's function, as it is

```python
import re, smtplib, pondra
from datetime import date

@db.function
def slug(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")

@db.procedure
def send_report(day: date, recipients: list[str]):
    top = pondra.sql("SELECT item, sum(qty) AS sold FROM orders WHERE ts::DATE = $day GROUP BY item", day=day)
    ...                                   # as above: smtplib, pondra.secret("smtp")

db.sql("SELECT slug(title) FROM posts")              # the function, from SQL
posts.with_columns(s=pondra.fn.slug(pondra.col("title")))   # …and in a frame
db.call("send_report", date.today(), ["ann@example.com"])    # the procedure, on the node
send_report(date.today(), ["ann@example.com"])               # …or right here, the same code
```

- **No connection parameter.** `pondra.sql` is the caller's connection. A first parameter named
  `con` still gets it, as in 0.22.
- **What the function uses from the notebook comes with it:**
  - its imports (`import re`, `from x import y as z`);
  - helper functions defined in the notebook (their source);
  - simple constants (numbers, strings, lists and dicts of them).

  Anything else, such as a DataFrame or an open file, is refused when defined, with the fix in
  the message (pass it as an argument, or keep it in a table). The stored body is readable
  Python, so SQL users see exactly what runs (`SHOW FUNCTIONS`, `pondra.routines`).
- **Types come from annotations:**

  | Python | SQL |
  |---|---|
  | `str` | `VARCHAR` |
  | `int` | `BIGINT` |
  | `float` | `DOUBLE` |
  | `bool` | `BOOLEAN` |
  | `date` | `DATE` |
  | `datetime` | `TIMESTAMP` |
  | `bytes` | `BINARY` |
  | `list[T]` | `T[]` |
  | `dict` | `VARIANT` |

  An argument without one takes any value, as JSON. A function needs its return type; a
  procedure doesn't.
- **The decorator hands the function back unchanged,** so it still runs in the notebook.
- **`@db.function(vectorized=True)`, `@db.function(returns="TABLE(…)")`** and
  `db.create_function(name, file=…)` complete it, as `create_procedure` does now.

### 6. Every side, the same things

| | SQL | Python | JavaScript, HTTP, Postgres, MCP |
|---|---|---|---|
| a SQL function | `CREATE FUNCTION … RETURN …` | `db.sql(…)` | SQL |
| a Python function | `CREATE FUNCTION … LANGUAGE python` | `@db.function` | SQL |
| a procedure | `CREATE PROCEDURE …` | `@db.procedure`, `db.create_procedure(file=)` | SQL |
| use a function | `SELECT f(x)` | `pondra.fn.f(col("x"))`, or SQL in a frame | SQL |
| call a procedure | `CALL p(…)` | `db.call("p", …)` | `db.call`; MCP: every procedure is a tool |
| on a schedule | `CREATE TASK … SCHEDULE … AS CALL p(…)` | `db.sql(…)` | SQL |
| what ran | `pondra.runs` | `db.table("pondra.runs")` | SQL |

### 7. What it opens

- **Unstructured data in SQL**, on every node at once: `SELECT path, pdf_text(file_read(path))
  FROM files('contracts/')`, `image_labels(bytes)`. Here `pdf_text` and `image_labels` are Python
  functions; `ai_complete` and `ai_embed` are next to them.
- **Notebooks in the catalog, later.** A notebook is a procedure made of cells, kept in the lake
  as an `.ipynb` file:
  - SQL cells go to the node, Python cells to a worker;
  - `CALL run_notebook('reports/daily.ipynb', day => …)`, or on a schedule;
  - the console (round 25) opens and edits it.

## Kept from 0.22

- `CREATE MACRO` and `DROP MACRO`.
- `@con.procedure` with `con` first, and `con.create_procedure`.
- Bodies whose last line is the answer.
- `POST /functions` (Flight servers of your own).
- The rights model: only an admin makes a routine; a node runs Python only with `--python` (on
  127.0.0.1 without tokens); invariant 21 stands for SQL.

## Rejected

- **Python inside the binary (PyO3):** it would tie the binary to one Python version and break
  the glibc 2.17 build, and a crashing library would take the node down.
- **Pyodide / WebAssembly Python for functions:** safe, but many native libraries don't load
  there. Worth it later, for users who may not run code on the machine (with grants, round 26).
- **cloudpickle for the decorators:** bytecode tied to one Python version, unreadable in the
  catalog.
- **Separate words for SQL and Python routines:** Postgres has one `CREATE FUNCTION` with a
  `LANGUAGE`.

## Tests (the plan)

- **SQL functions:** Postgres's forms (`RETURN`, `$$ SELECT $$`, `RETURNS TABLE`, `$1`,
  `STRICT`) give Postgres 17's answers; 0.22's macros are unchanged.
- **Python functions:**
  - per row, vectorized and table functions, NULLs;
  - a spread query, where one node == three;
  - a worker killed mid-query (the query fails with the reason, the next one runs);
  - the time limit.
- **Procedures:**
  - one sends mail to a local SMTP server (`aiosmtpd`) from `CALL` in the shell, over psql (the
    NOTICE shown) and from JavaScript;
  - it answers nothing, a table, and a frame;
  - its writes are exactly-once with a job;
  - a secret never appears in the run log or in an error.
- **The decorator:** a notebook function using a module imported above it, a helper and a
  constant runs on the node and in the notebook alike; a DataFrame global is refused, with the
  fix in the message; PL/Python code with `plpy.execute` runs.
- **A schedule:** every tick once, through a leader failover.
- **Speed:** a `CALL` of a small procedure is under 10 ms warm (0.15 s in 0.22). A Python
  function over 1M rows runs at pyarrow's speed when vectorized; per row, the rate is measured and
  reported.
