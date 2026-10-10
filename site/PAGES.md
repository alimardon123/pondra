# The site's pages

Paths are under `site/src/content/docs/`. Each page lists its main sources (besides `README.md`).
Pages marked ★ are written by the lead (they cover round 26's features, still being built):
leave them out.

## Start here (`start/`)

| Page | Covers | Sources |
|---|---|---|
| `install.mdx` | pip, npm, the one-line installers, the plain binaries, Windows/macOS/Linux, `python -m pondra`, what the wheel contains, pyarrow optional | README "Install", `docs/adr-018-install-anywhere.md`, `docs/adr-024-nothing-to-set-up.md` |
| `first-lake.mdx` | (written: the model for every page) | |
| `shell.mdx` | `pondra` and `pondra <lake>`: the SQL shell, `.tables`, `.databases`, lakes beside it as databases, piping a script, `pondra run file.sql --name value` | `src/shell.rs`, ADR-024 |
| `tour.mdx` | a ten-minute tour: a table, a keyed table, a materialized view kept current, a frame, a Python function, a live query, reading a Parquet file, an export | the notebook `examples/quickstart.ipynb` |
| `console.mdx` ★ | the web console | |

## Guides (`guides/`)

| Page | Covers | Sources |
|---|---|---|
| `tables.mdx` | `CREATE TABLE` and its options (publish, cluster_by, partition_by, PRIMARY KEY / key, merge, ttl, order_by), schemas, `lake.schema.table`, `CREATE DATABASE`, `ATTACH`/`DETACH`, `ALTER TABLE` (add, rename, drop, widen, `SET (…)`), `DROP` | README, ADR-019, ADR-021, ADR-022 |
| `load-data.mdx` | `INSERT … VALUES/SELECT`, `append` exactly-once (producers, seq), bulk insert with a job, `COPY FROM STDIN` over Postgres, CSV/Parquet/JSON files into tables, `write_table` from pandas/Polars/Arrow | README, ADR-009, ADR-020 |
| `query.mdx` | SQL basics in Pondra: snapshot reads, the log tail, joins, `FROM t`, parameters `$name`, scripts, the result cache and `stale_ms`, `format=arrow`, EXPLAIN, spreading over nodes | README "SQL", ADR-015, ADR-020 |
| `files-and-other-lakes.mdx` | `read_parquet/csv/json`, globs, Hive folders, S3/GCS/Azure/HTTPS, `CREATE SECRET`, `read_delta`, `read_iceberg`, `ATTACH … (TYPE delta \| iceberg \| kafka)`, `INSERT` into them, `COPY … TO` files, Delta and Iceberg folders | ADR-026, ADR-028, `docs/dataframe-api.md` (names) |
| `change-rows.mdx` | `UPDATE`, `DELETE`, `MERGE` (all its forms), jobs for exactly-once, system columns, the change feed (`/watch?changes=true`), temporary tables | ADR-020, ADR-028 |
| `streaming.mdx` | materialized views (filled from existing rows), GROUP BY views as merge tables, event-time windows (tumbling, sliding, `_final`), session windows, stream joins, dedup by event time (`order_by`), `ASOF JOIN`, tasks (`POST /tasks`), `/watch`, live queries | ADR-011, ADR-016, ADR-017, ADR-021, ADR-022, ADR-028 |
| `flows.mdx` | materialized views of materialized views moving in one commit (bronze → silver → gold), rollups of GROUP BY views, expectations (keep, drop, fail; `pondra.expectations`), history views (SCD type 2), `ALTER MATERIALIZED VIEW … DETACH`, the flow's graph (`pondra.flows`) | ADR-036 |
| `dataframes.mdx` | `pondra.frame` with Polars' names: `db.table`, `filter`, `group_by`, `agg`, joins, `with_columns`, windows, `frame.sql`, SQL naming frames and pandas/Polars/Arrow data, `to_view`, `write_*`, `%%sql` in notebooks | `docs/dataframe-api.md`, ADR-022, ADR-023, `python/pondra/frame.py` |
| `pyspark.mdx` | `pondra.spark`: SparkSession, DataFrame API, `functions as F`, Window, reading and writing, UDFs, what differs | `docs/dataframe-api.md`, `python/pondra/spark/` |
| `functions-and-procedures.mdx` | SQL functions (`CREATE FUNCTION`, macros), Python functions (per row, vectorized, table, packages, cache), procedures (SQL, Python, notices, secrets, `pondra.sql`), `@db.function`/`@db.procedure`, tasks on a schedule, `pondra.runs`, `pondra.start` | ADR-023, ADR-027, ADR-028 |
| `workspace.mdx` | the lake's files run as jobs: `CALL run('etl/orders.sql', day => …)`, `.sql` (`$name` bound), `.py` (variables), notebooks (the `parameters` cell), files running files, every client's `run`, `pondra.start('run', …)`, tasks, the run log (`files/<path>@<version>`), the console's parameters bar, Run as a job, Schedule…, Runs | ADR-033 |
| `environments.mdx` | branches: `CREATE DATABASE … CLONE` (schemas, `WITH NO DATA`), `pondra.databases`, writes apart, `pondra.diff`, `ALTER DATABASE … REFRESH`, `DROP DATABASE`, what a branch leaves behind | ADR-047, `src/branch.rs` |
| `other-engines.mdx` | publishing Delta and Iceberg, reading them with DuckDB, Polars, delta-rs, PyIceberg, Spark; the Iceberg REST catalog; other engines appending through it | `docs/lake-format.md`, ADR-007, ADR-011, ADR-028 |
| `kafka.mdx` | the Kafka port (producers, Debezium, consumers, groups, SASL), other Kafka clusters as tables, `COPY … TO` a topic, views fed from a topic | ADR-011, ADR-026 |
| `ai-and-files.mdx` | vector columns and search, `ai_complete`/`ai_embed`, files in the lake (`PUT /files`, `files()`, `file_read`), `BINARY` functions, JSON/VARIANT functions, functions on an Arrow Flight server | ADR-013, README |
| `clusters.mdx` | several nodes on one lake, leader election and failover, `--ack replicated`, readers, queries spread over the nodes (the guard), the bucket inbox, `pondra sql` serverless | ADR-004, ADR-005, ADR-010, ADR-015, ADR-020 |
| `upgrades.mdx` | stopping a node without dropping work (the drain, `PONDRA_DRAIN_SECS`, `PONDRA_DRAIN_GRACE_SECS`, a leader stepping down), `/healthz` and `/ready`, a rolling upgrade a node at a time, `/stats`' `version`, `format` and `releases`, which releases open a lake (its format, a newer one refused) | ADR-039 |
| `object-storage.mdx` | lakes on S3, R2, MinIO, GCS, Azure: credentials, the SSD cache, costs, freshness numbers | `docs/lake-format.md`, ADR-006, ADR-009, ADR-026 |
| `deploy.mdx` | running a node long-term: the container image (`ghcr.io/alimardon123/pondra`, `-python`, `/data`, ports, a bucket, memory from the cgroup), the three-node Compose cluster (`PONDRA_IMAGE`, `PONDRA_LAKE`), the Helm chart (`oci://ghcr.io/alimardon123/charts/pondra`: bucket or shared volume, tokens and key Secrets, TLS, doors, readers, `helm test`), `pondra service install \| status \| uninstall` (systemd, launchd, Windows service), Homebrew / winget / Scoop (prepared, not yet published), stopping a node cleanly | ADR-041, `deploy/`, `src/service.rs` |
| `notebooks.mdx` | Jupyter with `pondra.local()`, `%load_ext pondra` and `%%sql`, frames' display, pandas/Polars interop | ADR-023, the notebook |
| `security.mdx` | read/write/admin tokens on every door, `--python` rules, secrets (sealed with `PONDRA_SECRET_KEY`), what's next (grants, TLS: round 29) | ADR-010, ADR-026, ADR-027 |
| `sharing.mdx` | sharing a table with another company: `CREATE SHARE`, `ALTER SHARE … ADD TABLE` (partitions, `AS`, `WITH HISTORY`, a materialized view for less), `CREATE RECIPIENT` and its profile, `GRANT SELECT ON SHARE`, the delta-sharing client and Spark, `ATTACH … (TYPE share)`, `SHOW SHARES`, revoking and rotating, the audit log, where the bytes go | ADR-046 |
| `dbt-and-bi.mdx` ★ | dbt and BI tools | |
| `server.mdx` ★ | `pondra serve` on a folder of lakes (`--lakes`), local or in a bucket | ADR-030, ADR-032 |

## Reference (`reference/`)

| Page | Covers | Sources |
|---|---|---|
| `sql.mdx` | every statement Pondra takes, one short section each, with a line of syntax and an example; what is refused, by name | README tables, `src/write.rs` (`parse`), `src/ddl.rs`, `src/routines.rs` |
| `functions.mdx` | SQL functions beyond DataFusion's standard ones: JSON, vectors, files, binary, AI, `pondra.*`, `secrets()`, `files()`; a link to DataFusion's function list | `src/udf.rs`, `src/files.rs`, `src/ai.rs`, ADR-011, ADR-013 |
| `table-options.mdx` | every `WITH (…)` option of `CREATE TABLE`, `CREATE MATERIALIZED VIEW` and `CREATE FUNCTION` | README, the ADRs |
| `names.mdx` | the one vocabulary: `read_*`/`write_*` and their fallbacks in SQL, Python, PySpark, JavaScript | `docs/dataframe-api.md` (between the `<!-- vocabulary -->` markers) |
| `python.mdx` | the Python client: `local`, `connect`, every method of the connection, with signature and example | `python/pondra/client.py`, `__init__.py` |
| `frames.mdx` | every frame method and expression function, grouped | `docs/dataframe-api.md`, `python/pondra/frame.py` |
| `javascript.mdx` | the JavaScript client | `js/index.js`, `js/README.md` |
| `http.mdx` | every HTTP endpoint: method, path, parameters, body, answer | `src/server.rs` (`router`), README |
| `postgres.mdx` | the Postgres port: connecting, types, `COPY`, parameters, what drivers work | `src/pg.rs`, ADR-010 |
| `flight.mdx` | Arrow Flight and Flight SQL: ADBC, pyarrow `DoPut`/`DoGet`, the log as a stream | ADR-012, `src/flight.rs` |
| `kafka-protocol.mdx` | the Kafka port's details: topics, keys, offsets, compression, auth | ADR-011, `src/kafka.rs` |
| `iceberg-rest.mdx` | the Iceberg REST catalog: endpoints, namespaces, what writers may commit | ADR-011, ADR-028, `src/iceberg.rs` |
| `mcp.mdx` | MCP: the tools, connecting Claude Code / Claude Desktop / Cursor | `src/mcp.rs` |
| `cli.mdx` | `pondra`, `serve` (every flag), `sql`, `run`, `catalog`, `server` ★ (leave a placeholder line) | `src/main.rs` |
| `environment.mdx` | every `PONDRA_*` environment variable, with its default | `grep -rn 'PONDRA_' src python js` |
| `system-columns.mdx` | `_row_id`, `_version`, `_created_at`, `_updated_at`, `_deleted`, `_change_type`, `pondra.runs`, `pondra.routines`, `pondra.tasks` | ADR-020, ADR-027 |
| `glossary.mdx` | every kind of object, part and pattern, what each is and what other products call it; written from `GET /kinds` by `tools/docs_check.py --write-glossary` (the same text as `docs/glossary.md`) | `src/objects.rs` (`KINDS`, `PARTS`, `PATTERNS`), ADR-049 |

## Concepts (`concepts/`)

| Page | Covers | Sources |
|---|---|---|
| `how-it-works.mdx` | the one binary, the lake, nodes and the leader, the log, tiering, the catalog in the bucket, SPMD queries: a picture and a page | README "How it works", AGENTS.md "The model in one page", ADR-002, ADR-003, ADR-005 |
| `lake-format.mdx` | what's in the folder, and who can read it | `docs/lake-format.md` |
| `keyed-tables.mdx` | upserts, LSM files, compaction, merge tables, TTL, `order_by` | ADR-006, ADR-021 |
| `freshness-and-guarantees.mdx` | exactly-once, acks (durable, replicated), what each reader sees when, the numbers | `docs/comparison-spark-flink-fluss.md` (freshness), ADR-009 |
| `failures.mdx` | what a running lake survives (a node, the leader, the bucket erring, slow, down or cut off from the leader, a full disk, `pondra sql` killed), each door through a failover, giving clients every node, what isn't covered yet | `tools/resilience_check.py`, `python/pondra/client.py`, `js/index.js`, `src/cluster.rs` |
| `performance.mdx` | TPC-H against DuckDB, Polars, Daft, Spark; streaming against Flink; serving; how to measure yourself | `docs/prototype-status.md`, `docs/comparison-spark-flink-fluss.md` |
| `compared.mdx` | Pondra next to Spark, Flink, Fluss, DuckDB, Databricks and Snowflake: what each is, where Pondra fits, where they win | `docs/comparison-spark-flink-fluss.md`, ADR-002 |
| `design-notes.mdx` | the ADRs, one line each, linked on GitHub (`https://github.com/alimardon123/pondra/blob/main/docs/…`) | `docs/README.md` |
