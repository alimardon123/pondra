# Pondra: a streamhouse in one binary

**Documentation: [alimardon123.github.io/pondra](https://alimardon123.github.io/pondra/)** — how to
install it, load and query data, stream, run a cluster, and every statement, function, flag and
endpoint, each example tested.

One Rust binary (~28,000 lines) that ingests streams, stores them as a lakehouse (Parquet files
plus a catalog, on object storage; Delta Lake and Iceberg metadata for other engines on request),
keeps SQL views and streaming state up to date, answers SQL, and scales out by starting more
copies of itself on the same bucket. It reads and writes what is outside it too: files on any
store, other engines' Delta and Iceberg tables, Kafka clusters. Object storage is the only state:
no Postgres, no ZooKeeper, no Kafka, no JVM. Runs on a local directory, any S3-compatible store
(S3, Cloudflare R2, MinIO), Google Cloud Storage or Azure.

## Install

| You have | Run | Then |
|---|---|---|
| Windows | `irm https://github.com/alimardon123/pondra/releases/latest/download/install.ps1 \| iex` | `pondra` |
| Linux, macOS | `curl -fsSL https://github.com/alimardon123/pondra/releases/latest/download/install.sh \| sh` | `pondra` |
| Python | `pip install pondra` (add `pyarrow` for pandas, Polars and Arrow) | `pondra`, `python -m pondra`, or `import pondra` |
| Node | `npm install -g pondra` (or `npm install pondra` in a project) | `pondra`, or `npx pondra` with no install |

The installers put the binary in your own folder (`~/.local/bin`, or `%LOCALAPPDATA%\Programs\pondra`)
and that folder on your PATH: no admin rights, nothing else to set up. pip puts `pondra` next to
Python; where that folder isn't on PATH (a user install into a Python installed for everyone, on
Windows or macOS), `python -m pondra` runs it anyway and `python -m pondra --add-to-path` puts it
there once. Without pyarrow the Python client's rows come as JSON (dates and times as text);
tables for pandas, Polars and Arrow need it.

The Linux binary asks for nothing newer than glibc 2.17, so it runs on any Linux from 2014 on —
tested on CentOS 7 and Ubuntu 22.04, the base of most cloud notebooks. macOS (Intel and Apple),
Windows and ARM Linux get their own builds; each release tries every package and the installer
on its own platform before publishing.

```bash
pondra                      # a SQL shell on ./lake (or: pondra my-lake, pondra s3://bucket/lake);
                            # the other lakes in this folder are its databases too (.databases)
```

```python
import pondra
from pondra import col
db = pondra.local("lake")   # a node on ./lake, in the background; it stops when Python does
db.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)")
db.append("events", [{"user": "ann", "amount": 5}])
db.sql("SELECT user, sum(amount) AS total FROM events GROUP BY user").to_pandas()

# Frames, with Polars' names (or PySpark's: pondra.spark): one SQL statement underneath
top = db.table("events").group_by("user").agg(col("amount").sum()).sort("amount", descending=True)
db.sql("SELECT * FROM top WHERE amount > $min", min=1).to_polars()   # SQL reads Python by name
```

```js
import { local } from "pondra";
const db = await local("lake");
await db.sql("SELECT 42 AS answer");
```

`examples/quickstart.ipynb` is the same in a notebook: tables, a view that keeps itself current,
new rows as they commit, a point-in-time join, frames and `%%sql` cells, functions and procedures
in SQL and Python, another engine appending, and a live answer. The shell and `local()` start a node with
`--stop-with-stdin`: it stops when the shell or program that started it exits — or is killed —
and hands the lake on at once, so the next one opens it straight away.

## Run it

```bash
cargo build --release

# Local directory; the console (notebooks, SQL, Python and data files in tabs) is at http://127.0.0.1:8080/
./target/release/pondra serve ./lake

# A folder of lakes as databases: psql -d sales, or http://host:8080/db/sales/…
./target/release/pondra serve ./data --pg 0.0.0.0:5432

# A cluster: the same command on each machine, same bucket. Every node takes writes and
# queries; one of them (elected through the bucket) orders the commits; any can take over.
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=auto
export AWS_ENDPOINT=https://<account>.r2.cloudflarestorage.com
./target/release/pondra serve s3://my-bucket/lake --addr 10.0.0.1:8080
./target/release/pondra serve s3://my-bucket/lake --addr 10.0.0.2:8080
./target/release/pondra serve s3://my-bucket/lake --addr 10.0.0.3:8080 --reader   # SQL only

# Serverless, from any machine with the binary and bucket credentials — no node needed:
./target/release/pondra sql --dir s3://my-bucket/lake "SELECT count(*) FROM events"
./target/release/pondra sql --dir s3://my-bucket/lake "INSERT INTO sales SELECT * FROM 'jan.parquet'"
./target/release/pondra sql --dir s3://my-bucket/lake "UPDATE users SET plan = 'pro' WHERE id = 7"

# SQL from anything that speaks Postgres, and from Python:
./target/release/pondra serve ./lake --pg 0.0.0.0:5432      # psql, psycopg, SQLAlchemy, dbt, DBeaver, Tableau, Excel
pip install ./python && python -c "import pondra; print(pondra.connect('http://127.0.0.1:8080').sql('SELECT 1').to_pandas())"

# Kafka producers and consumers (a topic is a table), and engines attaching the lake by URL:
./target/release/pondra serve ./lake --kafka 0.0.0.0:9092   # bootstrap.servers=host:9092
#   PyIceberg / DuckDB / Spark: an Iceberg REST catalog at http://host:8080 (namespace "default" = schema public; one per schema)

# Arrow Flight and Flight SQL: ADBC / JDBC drivers and pyarrow, Arrow in and out
./target/release/pondra serve ./lake --flight 0.0.0.0:8815  # adbc_driver_flightsql.dbapi.connect("grpc://host:8815")

# AI agents over MCP (Claude Code, Claude Desktop, Cursor, …): every node serves POST /mcp
claude mcp add --transport http pondra http://127.0.0.1:8080/mcp   # add --header "Authorization: Bearer $TOKEN" with tokens on
```

`--addr` must be reachable by the other nodes. Read-only nodes and `pondra sql` queries write
nothing, so read-only bucket credentials are enough for them.

**How machines share one lake.** The bucket is the platform; the binary is compute anyone
brings. Commits need one process to put them in order: whichever node leads, or — when nobody
runs — the `pondra sql` INSERT itself, for a moment.

- **Nobody running.** `pondra sql` reads the bucket directly. Its INSERT runs the query on that
  machine, writes the Parquet, and records the files itself.
- **A cluster running.** Nodes see each write milliseconds after it's acknowledged.
  - Laptops can join with `pondra serve` (or `--reader`) and leave again.
  - A laptop's `pondra sql` INSERT still does its own work; the leader only records the files.
- **After a full shutdown**, the first node started on the lake leads at once.
- **A machine that can't reach the leader** (another network, another company) still writes:
  its request goes through the bucket (`inbox/`), and the leader records it within a second or so.
- **Several clusters, one bucket:** each cluster leads its own lake and attaches the others
  (`--attach sales=s3://my-bucket/sales`): it reads `sales.orders` (or `sales.eu.orders`, a
  table of that lake's schema `eu`), and its writes to it are recorded by that lake's leader.

Useful `serve` flags (give every node the same ones: any of them may lead):

- `--ack durable|replicated`: when a write is acknowledged.
  - `durable` (default): once it is in the bucket. That is one object-store write: a millisecond
    on local disk, 0.25–0.7 s on S3/R2.
  - `replicated`: once `--replicas` nodes hold it (default 2: the leader in memory, a follower
    on local disk). That takes **~4 ms on any storage, no S3 Express needed**; the bucket gets
    the write a moment later. It survives any one node dying, but not the leader and every
    holder dying within that moment (see ADR-009).
- `--publish delta,iceberg`: new tables are also published as Delta Lake and/or Iceberg for other
  engines (default: none; per table: `"publish"`).
- `--tier-secs 10`: new rows become Parquet (and new Delta/Iceberg versions) as soon as they
  commit, at most this often (a table with a million rows waiting: within a second). Pondra's own
  reads see every commit at once; each run is a few object-store writes per busy table, so a
  bucket's bill and rate limits (S3: 3,500 writes a second per prefix) favour fewer. Fractions are
  fine (`0.25`). `GET /metrics` counts the writes, lists and deletes (`pondra_object_requests_total`).
- `--pg 0.0.0.0:5432`: also speak the Postgres protocol (with `COPY … TO STDOUT` and `COPY … FROM STDIN`).
- `--kafka 0.0.0.0:9092`: also speak the Kafka protocol (`--kafka-advertise host:port` if clients
  must reach this node at another address than `--addr`'s host).
- `--flight 0.0.0.0:8815`: also speak Arrow Flight and Flight SQL.
- `--memory-gb 24`: memory for queries (default: a third of the machine's); sorts, aggregations
  and joins that need more spill to the temp directory. The columns kept decoded in memory
  (`PONDRA_HOT_GB`, a quarter of it by default) come out of the same budget and give way to
  queries; `PONDRA_HOT_GB=0` turns them off.
- `PONDRA_CODEC=lz4|zstd|snappy|none`: how Parquet files are compressed. LZ4 by default — it
  decodes fastest, so scans are CPU-cheap; `zstd` where storage or bandwidth costs more than CPU.
- `--read-token`, `--write-token`, `--admin-token`: access control (none set = open). Over
  Postgres the user name picks the role (`reader`, `writer`, `admin`) and the password is its
  token. Whatever the token, SQL sent to a node never touches the node's own disk (no `COPY …
  TO`, no `CREATE EXTERNAL TABLE` over local files); only `pondra sql` reads local files, on its own machine, and
  so does the shell's (or `local()`'s) node for the program that started it, by a key only that
  program knows (`FROM 'D:\data\jan.csv'`).
- `--attach name=dir`: read (and write through its leader) another lake as a database of its
  own: `name.table`, `name.schema.table`. This lake's own name is its folder's. In SQL, `ATTACH
  'dir' AS name` does the same for every node of the cluster, kept in the lake (a new lake if
  nothing is there yet), and `CREATE DATABASE name` makes a new lake beside this one and attaches it.
- `--changelog-secs 86400`: keep the log as a replayable change feed (`/watch/{t}?after=…`;
  `&changes=true` for every UPDATE and DELETE too).
- `PONDRA_LINK=ms,MB/s`: the network between the nodes, if known (else measured): a query spreads
  only when what it would move costs less than the work it shares out. `PONDRA_PURGE_ROWS`
  (100,000): changed rows waiting before their files are rewritten without them.
- `--fsync` (with `--ack replicated`): followers flush each copy to disk before acknowledging.
- `--python /usr/bin/python3`: run Python functions and procedures (`LANGUAGE python`) with this
  Python, which has the `pondra` package and pyarrow, on warm workers beside the node (one per
  core at most, gone after `PONDRA_WORKER_IDLE_SECS`, 60, idle; `PONDRA_WORKER_MB` 2048 each;
  `PONDRA_PROCEDURES` at once, four per core). `--python auto` (the shell's, and `local()` in
  JavaScript) finds one. They run any code on the machine, so only an admin token makes one, and
  a node without tokens takes `--python` only on 127.0.0.1 (Python's `local()` passes its own).
- `--cache-dir`, `--cache-gb 20`: the local SSD tier for lakes on object storage. 0 turns it off.
- `--retain-secs 60`: how long replaced files and consumed log segments are kept.
- `--backlog 10000000`: rows allowed to wait for tiering before commits pause.

## Read the lake with other engines

Tables created with `"publish": ["delta", "iceberg"]` (or on a node started with `--publish`)
are also a **Delta Lake table** and an **Iceberg table** at `<lake>/data/<table>`. Spark,
Databricks, DuckDB, Polars, delta-rs, PyIceberg, Trino and Athena read them directly:

```sql
SELECT user, sum(amount) FROM delta_scan('s3://my-bucket/lake/data/events') GROUP BY user;  -- DuckDB
SELECT count(*) FROM iceberg_scan('s3://my-bucket/lake/data/events/metadata/v42.metadata.json');
```

Other engines read the table as of the last tiering round:

- ~30 ms after the ack on local disk;
- 3–10 s on R2 at the default `--tier-secs 2`, depending on how far away the bucket is.

**And they append to it** through the node's Iceberg REST catalog, as they would through Polaris
or Unity Catalog (Pondra's own catalog stays the one that counts):

```python
from pyiceberg.catalog import load_catalog
load_catalog("lake", type="rest", uri="http://node:8080").load_table("default.events").append(arrow_table)
# (a lake on R2: add "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO" and the s3.* properties; pip install s3fs)
# Spark: spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog, …lake.type=rest, …lake.uri=http://node:8080
#        spark.sql("INSERT INTO lake.default.events SELECT …"); df.writeTo("lake.default.events").append()
```

An append's files become the table's where the writer put them: the node reads each footer once,
checks it and records it, never copying the rows (a million rows cost it 0.01 s of CPU, against 0.24 s
to copy them), and the rows take their ids and versions from the commit. Each commit lands once: a
stale one gets 409 and the writer retries on top. Engines make, rename and drop tables through the
catalog too, and follow the layout it publishes (partition spec, sort order, key). They change rows
as well — Spark's `DELETE`, `UPDATE` and `MERGE`, copy-on-write or merge-on-read, PyIceberg's
`delete` and `overwrite` — against the table as Pondra has it (a change made before rows still in
the log were in its files gets 409 once they are), several tables in one transaction, and change
its schema as `ALTER TABLE` can. Views, tasks, the change feed and Kafka topics follow their commits
as they follow the log's. A keyed table takes their appends as upserts and their deletes as deletes
of keys, and publishes every round, like the rest.

Pondra's own readers (nodes, `pondra sql`) see every write sooner. The local folder and the bucket
use the same layout: see `docs/lake-format.md`.

## Read and write anything else

Files, other engines' tables and Kafka topics are tables wherever SQL takes one, and `COPY … TO`
writes files, Delta and Iceberg tables, or topics. Reading is `read_<format>` and writing
`write_<format>` in SQL and every client; DuckDB's, Polars' and PySpark's names work too
(`docs/dataframe-api.md` has the table). Credentials go in a secret, never in the SQL:

```sql
CREATE SECRET sales (TYPE s3, KEY_ID '…', SECRET '…', REGION 'eu-west-1', SCOPE 's3://sales');
SELECT region, sum(amount) FROM 's3://sales/2026/*.parquet' GROUP BY region;   -- spread over the nodes
SELECT * FROM read_csv('gs://drop/orders_*.csv', header => true, delim => ';');
SELECT day, count(*) FROM read_parquet('az://lake/events/', hive_partitioning => true) GROUP BY day;
SELECT * FROM read_delta('s3://other/warehouse/orders', version => 12);        -- Spark's, Databricks' (or delta_scan)
SELECT * FROM read_iceberg('s3://other/warehouse/db/orders');                   -- (or iceberg_scan)

ATTACH 's3://other/warehouse' AS spark_lake (TYPE delta);                      -- a folder of tables
ATTACH 'https://catalog.example.com/api/catalog' AS pol (TYPE iceberg, SECRET polaris);  -- REST
ATTACH 'kafka://broker1:9092,broker2:9092' AS k (TYPE kafka, SECRET kafka_prod);
INSERT INTO spark_lake.sales.orders SELECT * FROM orders WHERE day = '2026-09-27';   -- a Delta commit
CREATE MATERIALIZED VIEW orders_in AS SELECT … FROM k.orders;                  -- a feed, every record once

COPY (SELECT * FROM orders) TO 's3://exports/orders/' (FORMAT parquet, PARTITION_BY (region));
COPY (SELECT * FROM orders) TO 's3://exports/orders_delta/' (FORMAT delta, APPEND);  -- a Delta table there, made or added to
COPY alerts TO 'kafka://broker1:9092/alerts' (FORMAT json, KEY id);
```

The same from Python: `db.read_parquet(url)`, `read_csv`, `read_json`, `read_delta`,
`read_iceberg` give frames and `frame.write_parquet(url)`, `write_csv`, `write_json`,
`write_delta(url, mode=…)`, `write_iceberg` write them (Polars' `scan_*` and `sink_*` too);
`pondra.spark` has `spark.read.parquet(url)`, `spark.read.format("delta").load(url)` and
`df.write.mode("overwrite").format("delta").save(url)`.

- **Stores:** `s3://` (AWS, R2, MinIO), `gs://`, `az://` / `abfss://`, `http(s)://`, and the
  machine's own paths — those only for the program that started the node (the shell, `local()`).
  A URL no secret covers is refused, unless that same program asks.
- **Formats:** Parquet, CSV/TSV, JSON lines; Delta (checkpoints of every kind, deletion vectors,
  column mapping) and Iceberg v1–v3 (position and equality deletes, deletion vectors, field ids,
  REST catalogs with OAuth); a feature Pondra doesn't read is refused by name, never read wrongly.
  `INSERT` writes Delta and Iceberg v2 tables, exactly-once with a job id.
- **Spread:** a glob's or another engine's files are dealt to the nodes like a table's; a big
  `COPY … TO` a folder is written by every node, each its own share's files.
- **Fresh:** a file added to a folder, or changed under its name, is read as it is by the next
  statement; byte ranges are kept in memory only as the version that statement listed.

## Windows, macOS, Linux

The code is portable Rust; nothing in it is Linux-specific. `.github/workflows/release.yml`
builds Linux (x86-64 and ARM, glibc 2.17), macOS (Intel and Apple) and Windows binaries, packages
each for pip and npm, and tries each package on its own platform before publishing. On Windows:

- **The installer** (`irm …/install.ps1 | iex`, above), **`pip install pondra`** or **`npm
  install -g pondra`**. With pip, if `pondra` isn't found afterwards, `python -m pondra` works,
  and `python -m pondra --add-to-path` fixes it for good.
- **WSL2:** `wsl --install`, then the Linux package or binary as above. This is the combination
  the tests were run on.
- **The build workflow's binary:** every push builds `pondra-windows-x86_64.exe` (an artifact of
  the `build` run) and runs `tools/smoke.py` with it — the shell, SQL with a schema and a view,
  and the node's memory figures — as it does on macOS and Linux. The owner has run it on Windows:
  the shell, SQL and the HTTP API work.
- **Native build:** install [rustup](https://rustup.rs) and the Visual Studio Build Tools (C++),
  then `cargo build --release` → `target\release\pondra.exe`. The one Unix-only piece
  (restart-in-place after a leader change) has a Windows path that spawns the replacement process
  instead.

The Linux binary is portable because it is built with `cargo zigbuild --profile dist --target
x86_64-unknown-linux-gnu.2.17`: it asks for nothing newer than glibc 2.17, the floor Python's
own manylinux2014 wheels use.

Paths on Windows work either way, but a lake at `s3://bucket/lake` (R2, S3, MinIO) avoids local-path
differences entirely.

## What it does

| Need | How (HTTP API, on any node) | Replaces |
|---|---|---|
| Stream ingest, exactly-once | `POST /append/{t}?producer=&seq=` with NDJSON or an Arrow IPC stream | Kafka / Fluss |
| Tables | SQL `CREATE TABLE t (id BIGINT PRIMARY KEY, …) WITH (publish = 'delta,iceberg', cluster_by = 'user', partition_by = 'day(ts)', merge = 'total:sum', ttl = 'ts:86400', order_by = 'ts')`, or `POST /tables/{t}` with the same as JSON. A key = upsert table (`SELECT *` shows its own columns; `_deleted` only when named); `order_by` = the row with the latest event time wins, not the last to arrive (Flink's deduplication by event time); `merge` = merge table; `cluster_by` sorts files for fast filters (two or more columns: along a Hilbert curve, so a filter on any of them skips most row groups); `partition_by` (a column, or year/month/day/hour of a timestamp) keeps one partition per file; a key, `cluster_by` and `partition_by` go together; `ttl` expires a keyed table's rows. Every file's column ranges are kept, and past 128 files a table's file list goes into manifests: a table of a million files commits as fast as one of ten, and queries open only the files their filters can match | Delta/Iceberg MERGE, partitioning, liquid clustering, Fluss PK tables with TTL |
| Schemas and names | A lake is a database: `CREATE SCHEMA sales; CREATE TABLE sales.orders (…)`; a table is `t` (schema `public`), `schema.t` or `lake.schema.t`, and other lakes are databases too: `CREATE DATABASE l2` (a new lake beside this one), `ATTACH 's3://bucket/sales' AS sales` (or `--attach`), then `sales.eu.orders` joined with this lake's tables, and `INSERT INTO sales.t …` through its leader; `DETACH sales`. `DROP TABLE`, `DROP SCHEMA … [CASCADE]`, `CREATE TABLE … AS SELECT`; a drop is refused while a view or task reads the table. Postgres, Flight SQL, the Iceberg REST catalog and MCP list the schemas | Postgres / Snowflake `database.schema.table` |
| Views | `CREATE [OR REPLACE] VIEW v AS …`: a stored query, run over the tables as they are when read (spread over the nodes like any query); `CREATE MATERIALIZED VIEW v [WITH (window = 'w', size_secs = 60)] AS …`: the streaming view below, filled from the rows already there when it is made, then kept up to date with every flush of new rows — every row once, even with rows streaming in as it is made | SQL views, Databricks materialized views, Flink SQL jobs |
| SQL writes | `INSERT … SELECT/VALUES`, `UPDATE … SET … WHERE`, `DELETE … WHERE` and `MERGE INTO t USING s ON … WHEN [NOT] MATCHED [BY SOURCE] …` on every table (an attached lake's too), on any node, over Postgres, or with `pondra sql` on any machine; from a local file in the shell (`MERGE INTO t USING 'new.csv' …`). A change is one commit from one snapshot, exactly-once with a job id; views, the change feed and Delta/Iceberg readers follow it | Delta/Iceberg MERGE, Snowflake DML, Fluss 1.0's UPDATE/DELETE by condition |
| System columns | every row has `_row_id` (kept through an UPDATE or MERGE), `_version` (the commit that wrote it), `_created_at`, `_updated_at`: `SELECT _row_id, * FROM t`; `SELECT *` leaves them out | Postgres `ctid`/`xmin`, Iceberg v3 row lineage, Delta row tracking |
| Postgres protocol | `--pg`: psql, psycopg 2/3, asyncpg, SQLAlchemy + pandas (tested); JDBC/BI tools by the same protocol. `COPY t FROM STDIN` (text, CSV; psql's `\copy`, psycopg's `cursor.copy`) and `COPY (query) TO STDOUT` (text, CSV, binary); the ADBC Postgres driver reads results as Arrow that way. For speed, Arrow Flight SQL | a Postgres-compatible serving layer |
| Python and JavaScript | `pip install pondra` / `npm install pondra`: `local()` starts a node here, `connect()` reaches one; `sql()` → pandas / Polars / Arrow, `append()` exactly-once, `view(name, sql)` (`materialized=True`: kept up to date, as `CREATE MATERIALIZED VIEW`), `write_table()`, `watch()`, `live()`, `lookup()`, `$name` parameters, `run("model.sql", …)`, `call(procedure, …)`: the same names as SQL and frames (ADR-025, ADR-028) | PySpark / PyFlink clients for the common jobs |
| Live queries | `GET /live?sql=…` (`db.live(sql)` in Python, `for await (const rows of db.live(sql))` in JavaScript): the answer now, then again within milliseconds of each commit that changes it — not for commits to other tables, and not when the answer comes out the same. Nothing runs once the client goes | PGlite's live queries, a dashboard's polling |
| Temporary tables | `CREATE TEMP TABLE t AS …`, `CREATE TEMP VIEW v AS …`: a session's own (a Postgres connection, or a client's), in the node's memory; `INSERT`, `UPDATE`, `DELETE`, `MERGE` and joins with the lake's tables; gone when the session ends | Postgres / DuckDB temporary tables |
| DataFrames, and SQL mixed with them | `pondra.frame`: Polars' lazy API (`db.table("orders").filter(col("amount") > 100).group_by("user").agg(col("amount").sum())`), each step a CTE of one SQL statement (`frame.sql`) that runs, spreads and is remembered like any query. `pondra.spark`: PySpark's names over the same frames (`from pondra.spark import SparkSession, functions as F, Window`), PySpark's meanings where they differ (null order, `/`, column names). Either way round: `db.sql(…)` is a frame; SQL names Python frames and pandas / Polars / Arrow data by their variable names (or `{name}`); frame methods take SQL snippets; `to_view()` makes a frame a view every client reads; `%load_ext pondra` gives notebooks `%%sql` cells. All 22 TPC-H queries give the same answers as SQL, as frames and as PySpark code; 51 PySpark pipelines (files read and written too) give PySpark's own answers and column names | Polars / PySpark on a lake, SQLMesh / dbt's Python models |
| SQL functions | Postgres's `CREATE FUNCTION net(x DOUBLE, rate DOUBLE DEFAULT 0.2) RETURNS DOUBLE RETURN x * (1 - rate)`, `… RETURNS TABLE (id BIGINT, …) LANGUAGE sql AS $$ SELECT … $$`, `SETOF`, `$1`, `STRICT`, `IMMUTABLE`; DuckDB's `CREATE MACRO` too. Kept in the lake: every node and every client has them, replaced by their bodies where SQL comes in (so queries using them spread as any other). Stored views read them as they are now; materialized views keep them as made | Postgres / DuckDB SQL functions |
| Python functions | `CREATE FUNCTION slug(t VARCHAR) RETURNS VARCHAR LANGUAGE python AS $$ … $$`: per row, a batch at once (`WITH (vectorized = true)`: pyarrow in and out) or a table (`RETURNS TABLE`); `WITH (packages = 'requests')`; `WITH (cache = '10 minutes')`: an answer reused for the same arguments (an API or model called once, not once a query); PL/Python's `plpy`. Run on warm workers beside every node (each node its own rows), anywhere SQL's own functions go; no connection back, a time limit per batch; `@db.function` on a notebook's function (its imports, helpers and constants go along), PySpark's `udf` / `pandas_udf` | Snowflake / Databricks Python UDFs and UDTFs, PySpark UDFs |
| Procedures | `CREATE PROCEDURE p(day DATE, n BIGINT DEFAULT 10) LANGUAGE sql AS $$ …; …; $$` or `LANGUAGE python` (anything Python can do — mail, HTTP, files; `pondra.sql(…)` is the caller's connection, `pondra.secret(…)` a `CREATE SECRET`'s values, never shown; `return` optional); `CALL p(DATE '2026-09-27')` from SQL, Postgres, Python (`db.call`; `@db.procedure`, or `db.create_procedure(name, file="job.py")`), JavaScript, the shell, and as MCP tools. What it prints comes back as notices (psql's NOTICE). Arguments worked out once; the caller's rights; exactly-once with a job; `SELECT pondra.start('p', …)` / `db.call(…, wait=False)` without waiting; every call in `pondra.runs`; a warm call in about 2 ms | Snowflake / Postgres stored procedures, Databricks jobs |
| Tasks | `CREATE TASK nightly SCHEDULE 'cron 0 2 * * * UTC' AS CALL report(current_date - 1)` (or `'5 minutes'`): the leader runs each tick once, through a failover; `SHOW TASKS`, `DROP TASK` (and `SHOW USER FUNCTIONS`, `SHOW PROCEDURES`) | Snowflake tasks, Databricks jobs, cron |
| Files as jobs | `CALL run('etl/orders.sql', day => DATE '2026-09-29')` runs a `.sql`, `.py` or notebook file kept in the lake (`$day` bound; a `.py` file's `day` a variable; a notebook's `parameters` cell replaced, as papermill does), from every door (`db.run`, JavaScript, Postgres, MCP, `pondra.start('run', …)`, a task); files run files; each run in `pondra.runs` as `files/<path>@<version>`. The console's ⋯ runs a file as a job or on a schedule | Databricks jobs and notebooks, Snowflake notebooks and tasks, Airflow's simple DAGs |
| Scripts and parameters | `POST /sql` takes several statements and `{"sql": …, "params": {"day": "2026-09-27"}}` for `$day` (bound by the node, never pasted in); `pondra run load.sql lake --day 2026-09-27` runs a file | psql scripts, dbt's `var()` |
| A shell | `pondra` or `pondra <lake>`: SQL typed or piped in, answers as tables, `.tables`, `.databases`, DuckDB-style | the DuckDB / psql prompt |
| Kafka | `--kafka`: producers write to tables (a topic is a table; JSON values; `_key`/`_timestamp`/`_value` columns; idempotent producers exactly-once; gzip/snappy/lz4/zstd), Debezium change events and tombstones become upserts and deletes; consumers and consumer groups read the log (offsets = `_ord`); SASL/PLAIN with the tokens. Tested: librdkafka (confluent-kafka), kafka-python | Kafka / Fluss ingest, Debezium sinks |
| Files and other engines' tables | `SELECT … FROM 's3://b/*.parquet'`, `read_parquet/csv/json(url, …)` on S3, GCS, Azure and HTTPS (Hive folders, globs, lists); `delta_scan`, `iceberg_scan` (deletion vectors, column mapping, equality deletes, old versions); `ATTACH … (TYPE delta \| iceberg)` for a folder of tables or a REST catalog, and `INSERT` into them; `COPY … TO` Parquet, CSV or JSON files, `PARTITION_BY`; `CREATE SECRET` for the credentials. Spread over the nodes, both ways | DuckDB's `httpfs`, Spark's data sources, Snowflake's external tables and `COPY INTO`, Polars' `scan_*`/`sink_*` |
| Other Kafka clusters | `SELECT … FROM 'kafka://brokers/topic'` or `ATTACH … (TYPE kafka)`: a topic's records as rows, spread by partition; `COPY … TO 'kafka://…/topic'` with Kafka's own key partitioning; `CREATE MATERIALIZED VIEW … FROM k.topic` keeps a table fed from it, every record once through restarts | Kafka Connect, Flink's Kafka connector |
| Schema evolution | `ALTER TABLE t ADD COLUMN c TYPE`, `RENAME COLUMN a TO b`, `DROP COLUMN c`, `ALTER COLUMN c TYPE BIGINT` (widening) on any node, over Postgres or from `pondra sql`, while rows stream in: no file is rewritten (the catalog keeps each column's stored name), old rows read a new column as null, and Delta (column mapping) and Iceberg (field ids) readers follow. `ALTER TABLE t SET (publish = 'delta', cluster_by = 'user', ttl = 'ts:3600', order_by = 'ts')` | Delta/Iceberg schema evolution |
| Event-time windows | `POST /views/{v}?window=w&size_secs=60&lateness_secs=10` over `GROUP BY date_bin(…, ts) AS w`: the view updates live; `{v}_final` gets each window once, final, when the watermark — the newest `ts` in the stream less the lateness — passes its end. Sliding: `slide_secs=60` with `size_secs=300` (and `date_bin` of the slide) gives a 5-minute window every minute, each row added once | Flink tumbling and sliding windows with bounded out-of-orderness watermarks |
| Stream joins | `CREATE MATERIALIZED VIEW v WITH (join = 'streams', time = 'ts', within_secs = 600) AS SELECT … FROM orders o JOIN payments p ON …`: a row of either table pairs with the other's rows when it arrives and with those that arrive after, each pair once, exactly-once through restarts; `within_secs` bounds what is read to pair them | Flink regular and interval joins |
| Session windows | `POST /views/{v}?session=ts&gap_secs=30&lateness_secs=5` over `SELECT user, count(*) … GROUP BY user`: each user's rows with no 30 s gap between them are a session; `{v}` gets each once, whole, with `session_start` and `session_end`, when the watermark passes its last row plus the gap | Flink / Spark session windows |
| Point-in-time joins | `FROM trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym` (also `>`, `<=`, `<`): each row gets the other table's row as it was at that moment, NULL if none; in ad hoc queries, across the nodes, and in views over a stream, where each event gets the table as of its own time however late it arrives | Snowflake / DuckDB ASOF JOIN, Flink temporal joins |
| JSON | `json_get(col, 'a', 0)`, `json_get_str/int/float/bool`, `json_contains`, `json_length`, `->`, `->>` | VARIANT / JSON functions |
| Arrow Flight | `--flight`: Flight SQL for ADBC and JDBC drivers (queries, writes, `adbc_ingest`, catalog); pyarrow `DoPut` to `[table, producer, first seq]` (exactly-once, acks as batches commit), `DoGet` with `{"sql": …}`, or a table's log as a columnar stream with only the columns asked for (`{"table": t, "after": N, "columns": [...], "follow": true}`) | Arrow Flight SQL servers (Dremio, InfluxDB 3), Fluss's columnar log |
| AI agents | `POST /mcp` (the Model Context Protocol): tools `list_tables`, `query`, `write`, `changes`, and every stored procedure as a tool of its own (its parameters the tool's), under the same tokens | an MCP server in front of the warehouse |
| Vector search | `FLOAT[]` embedding columns (`Float32[]`, published as a Delta `array` and an Iceberg `list`); `ORDER BY cosine_similarity(emb, [...]) DESC LIMIT k` (also `l2_distance`, `dot_product`, `cosine_distance`, `inner_product`, `array_distance`), exact, over the log and the files; Postgres array parameters work | a vector database next to the lake; Flink `VECTOR_SEARCH` |
| Files (images, PDFs, audio) | `PUT /files/<path>` and `GET /files/<path>` put objects in the lake next to the tables; `SELECT * FROM files('photos/')` lists them (path, size, written); `file_read(path)` reads one where a query needs it. `BINARY` columns hold bytes, with `byte_length`, `sha256`, `md5`, `encode(…, 'base64')`, `decode`, `substr` | Databricks file types, Hudi blobs, a blob store beside the warehouse |
| Semi-structured | `VARIANT` columns (JSON text): `json_get(col, 'a', 0)`, `json_get_str/int/float/bool`, `json_contains`, `json_length`, `->`, `->>` | VARIANT / JSON functions |
| Models in SQL | `ai_complete(prompt [, model])` and `ai_embed(text [, model])` call an OpenAI-compatible endpoint (`PONDRA_AI_URL`: your own vLLM or Ollama, or a hosted one), eight rows in flight, a failed row null | Databricks `ai_query` / `ai_forecast`, Snowflake Cortex |
| Your own functions | `POST /functions/{name}` `{"flight": "http://host:port", "args": ["Binary"], "returns": "Utf8"}`: an Arrow Flight server of yours gets the rows as one Arrow batch and returns one column, so a model, a GPU or any Python library runs in that process and not in the node (`tools/udf_server.py` is one in forty lines) | Python/Pandas UDFs, Databricks model serving, Daft UDFs |
| Streaming SQL with no lag | `CREATE MATERIALIZED VIEW name AS …`, or `POST /views/{name}` with the SQL. Runs on every flush of new rows, commits with them. With GROUP BY it keeps per-key aggregates (sum/count/min/max) that any number of nodes update at once | Flink SQL jobs + keyed state |
| General stateful streaming | `POST /tasks/{name}` `{"source","target","sql"[, "key","shards","shard_by"]}`: runs as soon as rows commit, exactly-once, shards spread over nodes | Flink jobs |
| Push and change feeds | `GET /watch/{t}`: new rows as NDJSON the moment they commit (upserts and deletes of keyed tables included); `?changes=true`: every change, each row with `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`), `_row_id` and `_version`, as Delta's change data feed; `?after=N` replays from N, as far back as `--changelog-secs` keeps the log. MCP's `changes` tool gives the same | Kafka consumers, Fluss `$changelog`, Delta CDF |
| SQL | `POST /sql[?format=json\|table\|arrow][&after=<seg>][&stale_ms=N]`: files ∪ log tail, one snapshot. Large tables run SPMD across all nodes, with shuffles for many-group aggregations and big joins — when that pays: what the plan would move, at the measured speed of the network, against the time the query takes on one node (`&spread=1` forces, `0` disables). Queries beyond `--memory-gb` spill. Repeated queries are answered from a result cache until the next commit (`stale_ms`: accept one up to N ms old) | Trino / Spark SQL / Databricks SQL |
| Metrics | `GET /metrics` (Prometheus): rows in, queries and their time, spread and shuffled queries, files scanned and skipped, memory, commit latency, per-table files, rows and bytes | a metrics exporter |
| Serving reads | `GET /lookup/{t}/{key}` (or SQL `SELECT … WHERE key = …`): the current row of one key without SQL planning — log tail, then the files newest-first, each narrowed to one cached, key-sorted row group: ~0.2 ms, ~20k/s on two cores | Redis / Postgres / Lakehouse//RT in front of the lake |
| Batch ELT, exactly-once | `POST /insert/{t}?job=` with a `SELECT` (the receiving node does the work), or `pondra sql "INSERT INTO t SELECT …"` from any machine: straight to Parquet (`INSERT … VALUES` goes through the log, gathered into a file per tiering round); a retried job is a no-op | Spark batch jobs |
| Maintenance | automatic and spread over the nodes: tiering to Parquet, compaction, rewriting files without changed rows (every round for published tables), retention, orphan cleanup, backpressure; `CHECKPOINT` tiers everything now (and compacts a published keyed table, so Delta and Iceberg see it as it is) | Spark OPTIMIZE / VACUUM |
| Open formats | tables that ask are published as Delta Lake (`data/{t}/_delta_log`) and Iceberg (`data/{t}/metadata`) each tiering round, for engines that don't know Pondra; an Iceberg REST catalog (`/v1/…`) lets them attach by URL and append (Spark, PyIceberg, Trino, another Pondra) | a separate Delta/Iceberg writer and catalog (Polaris, Unity Catalog) |

## How it works

| File | Role |
|---|---|
| `log.rs` | Every node batches its writes (Arrow IPC + ZSTD) and runs the views on them; big flushes it writes to storage itself. The leader's sequencer only orders them: dedupes producer retries, sends back a flush packed with other views than its tables have, and commits every flush as a log segment in one catalog write, pipelined |
| `store.rs` | The lake: object store + catalog (SlateDB, inside the bucket). The leader commits in order and streams every change and commit to the other nodes. They keep the whole catalog in memory from it (seeded from their own view; after a gap they fall back to the view, checked before and after every read, so a read never goes back in time): every node sees a commit within milliseconds, without asking the bucket |
| `replica.rs` | `--ack replicated`: followers keep the changes the bucket doesn't have yet in local files and acknowledge them; a new leader collects and re-commits them before taking writes |
| `cluster.rs` | Leader election through the bucket (put-if-absent `cluster/term/{n}`), HTTP heartbeats, takeover after 5 s if no peer still hears the leader; a replaced leader is fenced by the catalog and rejoins. A liveness mark in the bucket lets a node on an idle lake lead at once |
| `views.rs` | Inline views, filled from the rows already there when made; GROUP BY views become merge tables. Window and session views emit what is final once, exactly-once, by a watermark taken from the data's own event time |
| `asof.rs` | `ASOF JOIN`: rewritten as a LEFT JOIN DataFusion can plan with a marker on its condition, then run by a join that looks each row's match up — per key, in time order, a binary search — in one table, one per partition, or, for a few rows (a stream's new ones), only their keys' rows |
| `tasks.rs` | Streaming tasks: output + progress commit together, only if progress is unchanged (compare-and-swap) |
| `spmd.rs` | Distributed queries: every node runs the same plan over its slice of the biggest table; small and keyed tables are read whole, at the coordinator's snapshot. The plan decides what splits (any join type, subqueries, CTEs, unions): up to the first gather, or through shuffles, each exchange a step in which every node splits its output into a bucket per node and partition and reads its own from every node, in node order, so answers are the same every time. Scalar subqueries are answered between steps. Work is dealt by bytes; a step that fails is retried, then run again without that node |
| `ranges.rs` | Slicing big tables by the ranges of a key they share (cut where the biggest one's bytes split evenly; each node keeps the rows in its range, NULLs in the first), so joins and aggregations on that key run where the rows are |
| `skew.rs` | Hot keys in a shuffled join: a partition much bigger than the rest stays split where it was hashed, and its other side goes to every node |
| `sketch.rs` | Distinct values per column: a HyperLogLog sketch per file, folded into its table's at commit, for the join order |
| `spill.rs` | What a shuffle moves, in pieces (`PONDRA_SPILL_MB`): held in memory while small, written to the node's scratch disk beyond, sent length-prefixed and read back a piece at a time, buckets chained in order — so a shuffle, and what the coordinator gathers, is bounded by disk rather than memory |
| `manifest.rs` | Table metadata that stays small: per-file column ranges, the oldest files sealed into immutable manifests behind one list object, pruning of manifests and files by a query's filters |
| `flight.rs` | Arrow Flight and Flight SQL: exactly-once `DoPut`, SQL and the log as columnar streams, ADBC's statements, ingest and catalog |
| `metrics.rs` | `GET /metrics` in Prometheus' format |
| `tier.rs` | Tiering, merging small files and compaction: the leader decides and commits, the data work is dealt to the nodes as jobs. Keyed tables are LSM-like — each round folds the log tail into a new file, and files are compacted once 8 pile up. Retention and orphan cleanup |
| `query.rs` | Hot+cold snapshot per query (DataFusion); strings are read as views |
| `hot.rs` | The columns queries read lately, decoded, in memory, per file: a scan that finds them all there skips reading and decoding Parquet. Files never change, so nothing goes stale. Filled in the background, only for a file a second scan came back to, and never at the expense of a running query |
| `optimize.rs` | The engine settings Pondra starts from, and five planning rules of its own: a semi join runs on the table it filters, a grouped subquery groups only the keys the join keeps, a filter's conditions run cheapest first, the few groups a HAVING keeps make the hash table, and inner joins are ordered by what the catalog knows (rows and column ranges) when that beats the order the query wrote |
| `files.rs`, `ai.rs`, `udf.rs` | Files in the lake (`files('…')`, `file_read`), models in SQL (`ai_complete`, `ai_embed`) and vector maths, and functions of your own on an Arrow Flight server |
| `routines.rs` | Functions (SQL, Python), procedures and scripts: `CREATE FUNCTION` / `PROCEDURE` in Postgres's forms, SQL functions expanded where SQL comes in, `CALL`, `pondra.start`, notices |
| `python.rs`, `pyfn.rs` | Python beside each node: a pool of warm workers (`python -m pondra.worker`) over Arrow IPC, and Python functions as DataFusion functions and table functions |
| `adopt.rs` | Other engines' files recorded as written (ADR-029): their footers read and checked, their lineage (a first row id, the commit, its time) giving their rows' system columns |
| `workspace.rs` | Files of the lake's run as jobs (ADR-033): `CALL run('etl/orders.sql', day => …)` for `.sql`, `.py` and notebooks, logged by path and version |
| `runs.rs` | The run log (`pondra.runs`), tasks on a schedule (each tick once), `pondra.routines` and `pondra.tasks` |
| `cache.rs` | For lakes on object storage: an in-memory read cache and a local SSD tier (write-through, read-through, prefetched from the commit stream, warmed at start) |
| `serve.rs` | Serving reads: key lookups without SQL (tail, then files newest-first, cached key-sorted row groups, binary search), and SQL point queries routed to them |
| `delta.rs`, `iceberg.rs` | Open formats, per table: a Delta JSON commit / an Iceberg v2 snapshot (hand-written Avro manifests) per change to a table's files; crash-safe (derived from durable catalog state, put-if-absent); the Iceberg REST catalog |
| `change.rs` | UPDATE, DELETE and MERGE: an append table's rows change by version (the old ones go to `{t}$deleted`, which reads leave out); the change feed |
| `sys.rs` | System columns: row ids reserved in blocks from the leader, stamped as rows enter the log or a bulk INSERT's files; versions and times from the commit |
| `guard.rs` | Spread a query only when it pays: links measured, what a query takes on one node, and which way was faster once it ran both |
| `hilbert.rs` | `cluster_by` over two or more columns: rows along a Hilbert curve through their ranks |
| `ddl.rs` | Schemas and names (`lake.schema.table`, attached lakes), and the statements that shape a lake: `CREATE`/`DROP SCHEMA`, `DROP TABLE`, `CREATE VIEW` (stored), `CREATE MATERIALIZED VIEW`, `DROP VIEW`, `ALTER TABLE … RENAME/DROP/ALTER COLUMN` — carried out by the leader |
| `write.rs` | Writes in SQL from anywhere (CREATE TABLE [AS], INSERT, UPDATE, DELETE, and the DDL of `ddl.rs`): the work runs where the statement runs; the leader records it — over HTTP, through the bucket inbox, or the statement leads for a moment when nobody does. Attached lakes' writes go to their own leaders |
| `inbox.rs` | The bucket inbox: writers that can't reach the leader leave requests in the bucket; the leader answers them |
| `pg.rs` | The Postgres wire protocol (queries and writes, text and binary results, typed `$1` parameters, a small `pg_catalog`) |
| `auth.rs` | Read / write / admin tokens, over HTTP, Postgres and MCP |
| `mcp.rs` | MCP for AI agents: JSON-RPC over HTTP, four tools |
| `kafka.rs` | The Kafka protocol: produce (record batches → rows, exactly-once), fetch, offsets, consumer groups, SASL/PLAIN |
| `fsum.rs` | `sum` over DOUBLE that gives the same answer in any order: each addition's rounding error is carried in a second double and added back at the end |
| `shell.rs` | `pondra [lake]`: a SQL shell, with a node on the lake in the background |
| `ext.rs` | Files and other engines' tables anywhere (ADR-026): `'s3://…'`, `read_parquet/csv/json`, `delta_scan`, `iceberg_scan` and attached catalogs become `ext:` tables, listed once a statement, with Hive folders; who may read them; `CREATE SECRET` (sealed with `PONDRA_SECRET_KEY`); the stores for S3, GCS, Azure and HTTP |
| `scan.rs`, `read_delta.rs`, `read_iceberg.rs`, `avro.rs` | Other engines' tables read natively: the Delta log (checkpoints, deletion vectors, column mapping) and Iceberg metadata (manifests in Avro, position and equality deletes, field ids), over DataFusion's Parquet reader |
| `write_outside.rs` | `INSERT` into an attached Delta table (a put-if-absent commit) or Iceberg v2 table (manifests and a metadata file, or a REST catalog's commit), exactly-once by job |
| `copy.rs` | `COPY … TO` files anywhere or a topic; a big folder written by every node (`spmd::copy`) |
| `kafka_client.rs`, `feeds.rs` | Other Kafka clusters: topics read as tables (spread by partition), rows written as records (SASL PLAIN/SCRAM, TLS), and materialized views fed by a topic, every record once |
| `server.rs`, `main.rs` | HTTP API (axum) and CLI |

**Producer contract:** each producer has its own name, sends batches in order with increasing
`seq`, one request in flight, to any node, retrying (on any node) until acknowledged. Retries of
committed batches come back as `"duplicate": true`.

## Tests

```bash
python3 tools/harness.py all [--s3]             # upsert, fence (split brain), insert, serverless, clients, reader, crash, load
python3 tools/harness.py clients                # SQL writes, Python client, Postgres drivers, tokens, inbox, attached lakes, vectors, MCP
python3 tools/mcp_client.py --url http://127.0.0.1:8080/mcp   # the official MCP SDK against a node (pip install mcp)
python3 tools/harness.py kafka | alter           # Kafka clients, ALTER TABLE under load
python3 tools/harness.py columns | fills | dedup # RENAME/DROP/widen under streaming; views filled from existing rows; dedup by event time
python3 tools/bench/nexmark.py                  # Nexmark q1, q2, q5, q7, q11: Pondra and Flink, the answers checked against DuckDB
python3 tools/harness.py procedures             # macros, procedures (SQL and Python), scripts, parameters: rights, depth, three nodes, MCP tools
python3 tools/harness.py functions              # functions (SQL, Python), workers, notices at every door, mail, secrets, run log, tasks, speed
python3 tools/harness.py names | answers         # read_*/write_* == the tools' names, the docs' table run; function answers kept for a while
python3 tools/harness.py writes | live           # PyIceberg (and Pondra) appending through the Iceberg catalog; live queries
python3 tools/harness.py temps | across          # temporary tables and views in sessions; UPDATE/DELETE/MERGE on another lake from any node
python3 tools/frames_check.py                   # pondra.frame == Polars; one question asked ten ways (SQL, frames, pandas, .sql, %%sql, procedures)
<venv with pyspark>/python tools/spark_check.py # pondra.spark == PySpark 4: 55 pipelines, values and column names
python3 tools/harness.py outside | clouds | kafkas   # files on S3/HTTP/this machine, GCS and Azure (emulated), other Kafka clusters
python3 tools/formats_check.py --spark <python with pyspark>   # Delta and Iceberg by Spark 4, delta-rs, PyIceberg == Pondra's reads; INSERT; Spark appending to Pondra's tables
python3 tools/bench/files_tpch.py --data <tpch>  # TPC-H from files outside the lake against the lake's own tables
python3 tools/slt_check.py --slt <datafusion>/datafusion/sqllogictest/test_files   # DataFusion's SQL tests: the pass rate
python3 tools/bench/tpch_frames.py              # the 22 TPC-H queries as SQL, as frames and as PySpark code: the same answers
python3 tools/harness.py windows | sessions | asof   # event-time windows and sessions emitted once; point-in-time joins over a stream
python3 tools/asof_check.py                     # ASOF JOIN == DuckDB's, every direction, on one node and three
python3 tools/stream_check.py                   # one stream, window + session + as-of views: every click once; clicks/s, emission delay
python3 tools/harness.py scale | flight         # partitions, manifests, shuffles, memory limits; Arrow Flight + ADBC
python3 tools/harness.py sums                   # sum(DOUBLE) == math.fsum, in any order, on every node
python3 tools/harness.py schemas                # lake.schema.table, attached lakes, CREATE/DROP SCHEMA/TABLE/VIEW, CTAS, views spread, clients list schemas
python3 tools/harness.py changes                # UPDATE/DELETE/MERGE vs a model on 3 nodes: row ids, views, the change feed, purges, Delta, spread
python3 tools/harness.py deal                   # a keyed table's and a view's first tiering round on 3 nodes: deleted keys stay deleted
python3 tools/harness.py guard                  # a query spreads only when it pays: a slow link keeps it on one node, a fast one spreads it
python3 tools/smoke.py <pondra>                 # a first run on any OS (stdlib only): the shell, SQL, memory figures
python3 tools/anywhere_check.py --bin <pondra> --dist dist [--docker]   # the shell, local(), the wheel, npm, the notebook; glibc 2.17 and Ubuntu 22.04
bash tools/try_packages.sh linux-x64            # dist/'s packages as users get them: pip without and with pyarrow, python -m pondra, npm, the installer
bash tools/npm_publish.sh --dry-run             # the release's npm publish, tried
python3 tools/bench/repeat.py --data <tpch> --query 15 --runs 20       # one TPC-H query many times, every answer against DuckDB's
python3 tools/shuffle_spill.py                 # a shuffle bigger than memory, and one that loses a node
python3 tools/spread_tpch.py --expect 22        # all 22 TPC-H queries on 3 nodes == one node (13 by key ranges)
python3 tools/skew_check.py                     # a hot join key: same answers, work shared out over the nodes
python3 tools/join_order.py                    # the same query written badly runs as fast
python3 tools/bench/singlenode.py run --suite clickbench --data <hits> --engines pondra,pondra-cold,duckdb,duckdb-native   # ClickBench against DuckDB
python3 tools/bench/clickbench_ties.py --data <hits>   # ClickBench's answers that differ from DuckDB's are ties a LIMIT cuts through
python3 tools/tpcds_check.py run --data <tpcds> [--hot] # TPC-DS's 99 queries == DuckDB's
python3 tools/metadata_bench.py [--files 1000000]   # a table with a million files: commits, pruning, 3 nodes
python3 tools/flight_bench.py                   # Arrow Flight in, out, and the log as a stream
python3 tools/cloud/bench.py --nodes …          # a cluster on several machines (tools/cloud/README.md)
python3 tools/kafka_bench.py                    # Kafka ingest throughput and latency on 3 nodes
python3 tools/keyed_bench.py                    # keyed-table compaction: bytes written, correctness
python3 tools/harness.py crash --runs 20        # kill -9 + injected crashes (PONDRA_CRASH=point:prob)
python3 tools/cluster.py users                  # 64 writers + 16 readers + serverless reads, 3 nodes
python3 tools/cluster.py failover               # views + sharded task state; leader killed twice
python3 tools/cluster.py latency [--load 4]     # event -> view row pushed to another node (add --flag ack=replicated)
python3 tools/cluster.py spread                 # distributed queries == single-node results
python3 tools/cluster.py race | isolate | split # elections (and a leader that dies before making the catalog), cut-off follower, where the CPU goes
python3 tools/bench/run.py batch 20000000       # vs Spark and Flink (ENGINES=pondra,spark,flink)
python3 tools/serve_bench.py --keys 2000000     # point lookups and dashboard queries, p50/p99/QPS (uses tools/loadgen.go if Go is installed)
python3 tools/bench/tpch.py --data sf1          # TPC-H (tpchgen-cli) on Pondra, DuckDB and Spark
python3 tools/sizes.py                          # storage bytes per event
python3 tools/open_check.py [--s3]              # 8 readers (Delta: delta-rs/Polars/DuckDB; Iceberg: PyIceberg/Polars/DuckDB, and both through the REST catalog) == Pondra
python3 tools/freshness.py [--s3] [--flag ack=replicated]  # head to head: nodes, pondra sql, Delta, Iceberg
python3 tools/clustering.py                     # what cluster_by buys
python3 tools/newuser_bench.py [--s3]           # a new client's first query, a new node's, write→visible
python3 tools/demo_lake.py --dir <folder|s3://…> # one of everything, then the folder tree
tools/r2_test.sh                                # the main tests against a real bucket
python3 tools/sim_r2.py --port 9000             # local S3 server with R2-like latency (moto)
```

`--s3` uses `s3://$PONDRA_BUCKET/$PONDRA_TEST_PREFIX` + `test-…` with the `AWS_*` variables. Add
`--flag tier-secs=10` to `cluster.py` to pass a serve flag to every node. Test runs delete their
lakes when they finish (`--keep` or `PONDRA_KEEP=1` keeps them); `tools/clean_bucket.py` trims a
bucket to its newest lakes.

## Not yet

- A `LIMIT` inside a subquery over sliced data and order-preserving shuffles run on one node; a
  join with a hot key on both sides shares out only one. Clusters have run on 3 and 6 GitHub
  runners over the internet (`.github/workflows/cluster-bench.yml`), not yet in one data centre.
- A query's own answer passes through the coordinator's memory once (an HTTP answer is one body,
  shared by identical queries); what the nodes send does not.
- Files written in key order split tables by ranges across nodes but aren't declared as sorted
  to DataFusion (it made TPC-H slower), so an aggregation on that key still hashes.
- Changes and what follows a table: a view that emits windows or sessions once, keeps a min or
  max, joins another table or joins two streams, and a streaming task, can't take a row back, so
  a change is refused while one follows the table. Kafka consumers see an UPDATE's new rows, not its deletes.
  Tables made before 0.19 (without row ids) change after a copy (`CREATE TABLE t2 AS SELECT …`).
  Clustering across files.
- A write to two lakes is two commits, not one transaction.
- Other engines' writes through the Iceberg REST catalog: another engine's compaction is refused
  (Pondra merges its tables itself); an append to a table with a renamed column is copied, and
  changes to it refused; a keyed table's changes are copied through the log (as upserts), not
  recorded as written. Iceberg v3 (row ids kept through other engines' updates), vended credentials
  and scan planning are ADR-029's phase 3. Delta writers need Delta's catalog-managed commits,
  which aren't out yet. delta-rs's `to_pyarrow_table` refuses a table with deletion vectors (its
  `QueryBuilder` reads it).
- A materialized view that is a session window or a stream join starts from its creation; others
  are filled from the rows already there, in one go on the leader. Narrowing a type,
  `search_path` and grants per schema.
- The owner has installed the Windows wheel from PyPI on a real machine; the macOS packages, and
  the installers on anything but CI's machines, haven't run elsewhere yet. No winget or Homebrew
  package. The node's JSON leaves out nulls (the Python client puts them back as None; the
  JavaScript client doesn't yet).
- Only `sum` over DOUBLE is order-independent; `avg`, `stddev` and friends over DOUBLE can still
  differ in their last bits from run to run.
- Per-table grants, quotas and TLS (tokens are per role; put a TLS proxy in front, and keep a
  cluster's nodes on a private network: they talk plain HTTP to each other). Power BI Desktop
  itself hasn't run against Pondra (its drivers, Npgsql and psqlODBC, are tested).
- The console: two people's changes to one file aren't merged (the second save is refused, and
  says so); a Parquet file, or a data file over 10 MB, opens read-only.
- Files run as jobs: a file's old versions aren't kept (the run log names the version that ran;
  keep the files in git), and a notebook's run doesn't write its outputs into a copy of it, as
  papermill does.
- A folder of lakes (`pondra serve data`) is on the node's own disk, not in a bucket.
- Kafka: one partition per topic, no transactions; offsets are positions in the log (increasing,
  not dense). Consumer groups live in the leader's memory (members rejoin after a failover).
- An approximate vector index (see the plan in `docs/comparison-spark-flink-fluss.md`).
- Frames: the JavaScript client has parameters, `run` and `call`, not the frame builder;
  a sort inside `db.sql(…)`'s own SQL by an expression (not a column) ends at the next frame step.
  A folder of `.sql` and `.py` models run in order of what reads what (`pondra run models/`) is
  next; files run one by one (`CALL run`), or one file runs the others in its order.
- Functions and procedures: a Python table function takes values, not another table's rows (no
  LATERAL); `plpy.subtransaction` is refused (each statement commits on its own); a cast to an
  integer truncates where Postgres rounds; a task's ticks missed while no node led run once.
- Streaming: a watermark per source, not per partition or node, and a source that goes quiet
  holds it (its last windows and sessions wait for more rows); Top-N by event time, timers,
  `MATCH_RECOGNIZE`; stream joins run on the leader alone; an as-of join
  looks up a table's rows, so a keyed table, which keeps only its latest row per key, gives the
  latest, not the one of that moment — keep a table's history as rows for that.
- On object storage a *durable* ack costs one PUT; `--ack replicated` trades a small window
  (the leader and every holder dying before that PUT) for milliseconds.
- A one-off `pondra sql` on far-away object storage spends 1–3 s opening the catalog; join
  (`pondra serve --reader`) for millisecond reads.

## License

Pondra is licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option — as Rust itself and much of its ecosystem are. The
Python and npm packages carry both.

Unless you explicitly state otherwise, any contribution you intentionally submit for inclusion in
Pondra, as defined in the Apache-2.0 license, is licensed as above, without any additional terms
or conditions. Third-party libraries Pondra depends on keep their own licenses.
