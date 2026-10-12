<p align="center"><img src="https://alimardon123.github.io/pondra/favicon.svg" width="72" height="56" alt="Pondra"></p>

# Pondra

**A streamhouse in one binary.** Streaming ingest, a lakehouse in your folder or bucket, and SQL
that spreads over as many nodes as you start. No JVM, no Postgres, no ZooKeeper, no Kafka to run:
the bucket is the only state.

[Documentation](https://alimardon123.github.io/pondra/) ·
[GitHub](https://github.com/alimardon123/pondra) ·
[Releases](https://github.com/alimardon123/pondra/releases) · MIT OR Apache-2.0

This package installs the `pondra` binary for your platform and a small JavaScript client (no
dependencies; Node 18+). The client also runs in browsers, Deno and Bun, wherever `fetch` does; `local`
and `binary` need Node, `open` needs Node, Bun or Deno, and `ca` (the authority that signs an `https` node's
certificate) needs Node 22.19 or later.

```bash
npm install pondra        # in a project; or npm install -g pondra, or npx pondra with no install
```

```js
import { local } from "pondra";

const db = await local("lake");                          // a node on ./lake
await db.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)");
await db.append("events", [{ user: "ann", amount: 5 }]); // exactly once
console.log(await db.sql("SELECT user, sum(amount) AS total FROM events GROUP BY user"));

for await (const rows of db.live("SELECT count(*) AS n FROM events")) {
  console.log(rows);                                     // now, and again each time a commit changes it
  break;
}
await db.call("send_report", "2026-09-27");              // a stored procedure (SQL or Python)
await db.close();
```

`open()` runs the node in this process instead, so there is no second process to start. It works in Node, Bun
and Deno, and returns the same connection. `close()` stops the node and gives its term back; with no folder,
`open` holds the lake in this process's memory, as DuckDB's `:memory:` does, and `close()` lets it go. `local()` still starts a node beside your program.

```js
import { open } from "pondra";

const db = await open();                                 // a lake in memory (":memory:"), gone at close()
await db.sql("CREATE TABLE t (a INT)");
await db.close();
const kept = await open("lake");                         // a folder or s3://bucket/prefix, kept after close()
```

A frame is a query written in JavaScript: each step is a CTE of the SQL the node runs, so the plan
and the answer are the SQL's. Names are Polars' (`withColumns`, `groupBy`, `isNull`); nothing runs
until it is awaited, or written with `writeTable`. `await db.view(name, query)` makes a view and resolves
to the node's answer for it (a refusal rejects there, naming what it refused); read the view with
`db.table(name)`.

```js
import { col, local } from "pondra";

const db = await local("lake");
const big = db.table("events").filter(col("amount").gt(3)).groupBy("user").agg(col("amount").sum().alias("total"));
await big.writeTable("big_spenders");                     // a table made from the frame
console.log(await db.readParquet("sales/*.parquet").limit(5)); // files as a frame: awaiting runs it
```

Functions and procedures take their code as text or a file: `db.createFunction(…)` and `db.createProcedure(…)`
send it to the node, as SQL or as Python. Python's `@db.function` and `@db.procedure` decorators take a Python
function from Python's own source, so they can't ship code from JavaScript. Python's `secret()` answers only a
running procedure's own lent connection, which JavaScript code never runs as, so JavaScript has no way to read
a secret.

`npx pondra` opens a SQL shell on `./lake`, and `npx pondra serve lake` runs a node with its
console at `http://localhost:8080`.

## What it does

- **SQL** on Apache DataFusion: joins, windows, `MERGE`, `UPDATE`, `DELETE`, as-of joins, and views
  and materialized views that keep themselves current.
- **Streams:** the Kafka protocol in and out, windows over event time, change feeds, live queries.
- **Files and other engines' tables:** `read_parquet`, `read_csv`, `read_json`, `read_delta` and
  `read_iceberg` on S3, GCS, Azure and HTTPS. Tables are published as Delta and Iceberg for
  Spark, DuckDB and Polars to read.
- **SQL and Python as one:** functions and procedures in Python run on the node (`local()` gives
  it a Python with the `pondra` package if this machine has one), and frames in the Python client.
- **Every client:**
  - Postgres: psql, dbt, and BI tools;
  - Arrow Flight SQL;
  - HTTP;
  - Python;
  - MCP for AI agents.
- **A console** at every node's address: SQL, Python and text cells, notebooks kept in the lake.
- **Scale out** by starting more copies on the same bucket. Any node coordinates a query.

Read the [documentation](https://alimardon123.github.io/pondra/) for guides and the JavaScript
client's reference. Each example there is tested.
