<p align="center"><img src="https://alimardon123.github.io/pondra/favicon.svg" width="72" height="56" alt="Pondra"></p>

# Pondra

**A streamhouse in one binary.** Streaming ingest, a lakehouse in your folder or bucket, and SQL
and Python that spread over as many nodes as you start. No JVM, no Postgres, no ZooKeeper, no
Kafka to run: the bucket is the only state.

[Documentation](https://alimardon123.github.io/pondra/) ·
[GitHub](https://github.com/alimardon123/pondra) ·
[Releases](https://github.com/alimardon123/pondra/releases) · MIT OR Apache-2.0

This package holds the `pondra` binary for your platform and the Python client.

```bash
pip install pondra            # the binary and the client
pip install "pondra[arrow]"   # with pyarrow: answers as pandas, Polars and Arrow tables
```

```python
import pondra
from pondra import col

db = pondra.local("lake")    # a node on ./lake, in the background; it stops when Python does
db.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)")
db.append("events", [{"user": "ann", "amount": 5}, {"user": "bo", "amount": 3}])  # exactly once
db.sql("SELECT user, sum(amount) AS total FROM events GROUP BY user").to_pandas()

# Frames with Polars' names (PySpark's: pondra.spark), one SQL statement underneath
db.table("events").group_by("user").agg(col("amount").sum()).sort("amount", descending=True)
```

`pondra` opens a SQL shell, and `pondra serve lake` runs a node with its console at
`http://localhost:8080`.

## What it does

- **SQL** on Apache DataFusion: joins, windows, `MERGE`, `UPDATE`, `DELETE`, as-of joins, and views
  and materialized views that keep themselves current.
- **Streams:** the Kafka protocol in and out, windows over event time, change feeds, live queries.
- **Files and other engines' tables:** `read_parquet`, `read_csv`, `read_json`, `read_delta` and
  `read_iceberg` on S3, GCS, Azure and HTTPS. Tables are published as Delta and Iceberg for
  Spark, DuckDB and Polars to read.
- **Python and SQL as one:**
  - frames;
  - functions and procedures in Python (`CREATE FUNCTION … LANGUAGE python`);
  - `%%sql` in Jupyter (`%load_ext pondra`).
- **Every client:**
  - Postgres: psql, dbt, and BI tools;
  - Arrow Flight SQL;
  - HTTP;
  - JavaScript;
  - MCP for AI agents.
- **A console** at every node's address: SQL, Python and text cells, notebooks kept in the lake.
- **Scale out** by starting more copies on the same bucket. Any node coordinates a query.

Read the [documentation](https://alimardon123.github.io/pondra/) for guides and every statement,
function and option. Each example there is tested.
