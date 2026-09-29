<p align="center"><img src="https://alimardon123.github.io/pondra/favicon.svg" width="72" height="56" alt="Pondra"></p>

# Pondra

**A streamhouse in one binary.** Streaming ingest, a lakehouse in your folder or bucket, and SQL
that spreads over as many nodes as you start. No JVM, no Postgres, no ZooKeeper, no Kafka to run:
the bucket is the only state.

[Documentation](https://alimardon123.github.io/pondra/) ·
[GitHub](https://github.com/alimardon123/pondra) ·
[Releases](https://github.com/alimardon123/pondra/releases) · MIT OR Apache-2.0

This package installs the `pondra` binary for your platform and a small JavaScript client (no
dependencies; Node 18+).

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
