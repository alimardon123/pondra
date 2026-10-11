# The site's pages

Paths are under `site/src/content/docs/`. The sidebar is `astro.config.mjs`: three tabs (Docs, SQL,
APIs), each a group of groups, and a page's place there is its order (no `sidebar.order` in pages).
`site/STYLE.md` says how a page is written; this file says what each page **owns**. A topic is
explained on its owner page only; every other page says one line and links there.

Pages marked *(planned)* come with the docs round's third pull request (`designs/docs-site-organization.md`
in the project files); until then their topics stay where the "now in" note says.

## Docs tab

### Get started (`start/`)

| Page | Owns | Sources |
|---|---|---|
| `install.mdx` | pip, npm, the one-line installers, the plain binaries, each OS, `python -m pondra`, pyarrow optional | ADR-018, ADR-024 |
| `first-lake.mdx` | the first five minutes from Python and JavaScript: `local()`, a table, a view, a query (the model for every page) | `python/pondra/client.py`, `js/index.js` |
| `features.mdx` | **Everything Pondra does**: every feature in one line and one example that runs, grouped as the home page's areas, each linking to its owner page | the whole site; checked against the code |
| `tour.mdx` | a ten-minute tour: a keyed table, a view kept current, a frame, a Python function, a live query, a file, an export | `examples/quickstart.ipynb` |
| `shell.mdx` | `pondra` and `pondra <lake>`: the shell, its dot commands, lakes beside it as databases, piped scripts, `pondra run file --name value` | `src/shell.rs`, ADR-024 |
| `console.mdx` | the console at `/`: its parts, keys, settings, extensions | `src/console/`, ADR-032, ADR-034 |

### Data (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `tables.mdx` | `CREATE TABLE` and its layouts (keys, `PARTITION BY`, `CLUSTER BY`, `SEQUENCE BY`, `TTL`, merge columns), schemas, `ALTER TABLE`, `DROP`, `UNDROP`, `SWAP`; sequences, indexes, types and constraints in short | ADR-019, ADR-021, ADR-022, ADR-049 |
| `load-data.mdx` | `INSERT`, exactly-once `append` (producer, seq), bulk loads with a job, `COPY FROM STDIN`, files into tables, `write_table` from pandas, Polars and Arrow | ADR-009, ADR-020 |
| `query.mdx` | how a read sees the lake (snapshot, log tail), DuckDB's spellings, `ASOF JOIN`, `$name` parameters, the result cache, `EXPLAIN`, spreading over nodes | ADR-015, ADR-020, `src/friendly.rs` |
| `change-rows.mdx` | `UPDATE`, `DELETE`, `MERGE`, jobs, system columns, the change feed, `BEGIN … COMMIT` | ADR-020, ADR-036 |
| `past.mdx` *(planned; now in `query.mdx` and `tables.mdx`)* | `AT (…)`, `RESTORE`, `UNDROP`, retention, `CLONE` | ADR-043 |
| `files-and-other-lakes.mdx` | files anywhere (`read_*`, globs, Hive folders, S3, GCS, Azure, HTTPS), `CREATE SECRET`, Delta and Iceberg read and written, `ATTACH`, `COPY … TO`, external tables | ADR-026, ADR-028 |

### Streams (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `streaming.mdx` | materialized views kept current (filled from the rows there, kept by key or run again), windows and sessions, `EMIT FINAL`, stream joins, dedup by event time, `/watch`, live queries. *Splits into views, windows and joins (planned).* | ADR-011, ADR-016, ADR-017, ADR-052, ADR-055, ADR-056, ADR-059 |
| `flows.mdx` | views of views in one commit, rollups, expectations, history views (SCD type 2), `DETACH`, `pondra.flows` | ADR-036 |
| `kafka.mdx` | the Kafka port (producers, Debezium, consumers, groups, SASL), other Kafka clusters as tables, `COPY … TO` a topic, views fed by a topic | ADR-011, ADR-026 |

### Python and code (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `sql-and-python.mdx` *(planned)* | SQL and Python together, both ways: `db.sql` naming frames and pandas data, `%%sql`, a notebook's SQL cell named into Python, `DO LANGUAGE python`, Python functions in SQL | ADR-022, ADR-023, ADR-025 |
| `dataframes.mdx` | `pondra.frame` with Polars' names: building, joining, windows, writing, views | `docs/dataframe-api.md`, `python/pondra/frame.py` |
| `pyspark.mdx` | `pondra.spark`: the session, DataFrames, `functions as F`, windows, readers and writers, UDFs, `spark_sql`, what differs | `python/pondra/spark/` |
| `functions-and-procedures.mdx` | `CREATE FUNCTION` (SQL, Python), macros, procedures, notices, secrets in procedures, `@db.function`, `@db.procedure`, `pondra.runs`, `pondra.start`. *Tasks move to their own page (planned).* | ADR-023, ADR-027 |
| `scripts.mdx` *(planned; now in `sql/statements.mdx`)* | variables (`DECLARE $x`), scripts' blocks, branches, loops, handlers, `ASYNC`/`AWAIT`, `EXECUTE IMMEDIATE`, `USE` | ADR-037, ADR-044, ADR-045 |
| `workspace.mdx` | the lake's files run as jobs: `CALL run(…)`, parameters, files running files, the run log | ADR-033 |
| `tasks.mdx` *(planned; now in `functions-and-procedures.mdx`)* | `CREATE TASK`: schedules, graphs (`AFTER`, `WHEN`), retries, timeouts, `SUSPEND` | ADR-045 |
| `notebooks.mdx` | Jupyter with `pondra.local()`, `%load_ext pondra`, `%%sql`, frames' display | ADR-023 |

### Teams (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `environments.mdx` | branches: `CREATE DATABASE … CLONE`, writes apart, `pondra.diff`, `REFRESH`, protected databases, separate servers | ADR-047, ADR-058 |
| `projects.mdx` *(planned; now in `environments.mdx`)* | `pondra init`, `plan`, `apply`, `status`, `capture`, `branch`, `switch`, migrations, seeds, tests, `pondra ci init` | ADR-060, `src/project.rs`, `src/apply.rs` |
| `git.mdx` *(planned; now in `environments.mdx`)* | Git folders: `git_checkout`, `git_status`, `git_commit`, `git_pull`, `git_pull_request` | ADR-060 §8, `src/git.rs` |
| `sharing.mdx` | sharing with another company: shares, recipients, profiles, the delta-sharing client, `ATTACH … (TYPE share)`, the audit | ADR-046 |
| `security.mdx` | tokens, users, roles, grants, TLS on every door, the audit log, quotas, secrets' sealing, `--python` | ADR-035, `src/users.rs`, `src/tls.rs` |

### Connect (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `dbt-and-bi.mdx` | dbt, psql, DBeaver, Tableau, Power BI, Excel through the Postgres port | `src/pg_catalog.rs` |
| `other-engines.mdx` | publishing Delta and Iceberg, other engines reading them, the Iceberg REST catalog, other engines writing through it | `docs/lake-format.md`, ADR-028, ADR-029 |
| `ai-and-files.mdx` | vectors and search, model calls in SQL, files in the lake (`files()`, `file_read`), JSON functions | ADR-013, ADR-051 |
| `agents.mdx` *(planned; now in `reference/mcp.mdx`)* | AI agents through MCP: connecting Claude, Cursor and others, what they may do | `src/mcp.rs` |

### Operate (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `clusters.mdx` | several nodes on one lake, the leader, failover, `--ack replicated`, readers, spread queries and the guard, `pondra sql` with no node | ADR-004, ADR-005, ADR-015, ADR-020 |
| `object-storage.mdx` | lakes on S3, R2, MinIO, GCS, Azure: credentials, the SSD cache, costs, freshness | ADR-006, ADR-009 |
| `server.mdx` | `pondra serve --lakes`: a folder of lakes as databases, local or in a bucket | ADR-030, ADR-032 |
| `deploy.mdx` | the container image, Compose, the Helm chart, `pondra service` | ADR-041, `deploy/` |
| `upgrades.mdx` | stopping without dropping work, `/healthz` and `/ready`, rolling upgrades, the lake's format | ADR-039 |
| `observability.mdx` *(planned; now spread over `query.mdx` and `clusters.mdx`)* | `pondra.history`, slow queries and their plans, `/metrics`, `/stats`, the audit log's place | ADR-048, ADR-050 |

### How it works (`concepts/`)

| Page | Owns | Sources |
|---|---|---|
| `how-it-works.mdx` | the binary, the lake, nodes and the leader, the log, tiering, the catalog in the bucket, SPMD queries | AGENTS.md "The model in one page" |
| `lake-format.mdx` | what's in the folder, and who can read it | `docs/lake-format.md` |
| `keyed-tables.mdx` | why and how keyed tables work: upserts, files as versions, compaction, merge tables, TTL | ADR-006, ADR-021 |
| `freshness-and-guarantees.mdx` | exactly-once, acks (durable, replicated), what each reader sees when | ADR-009 |
| `failures.mdx` | what a running lake survives, door by door | `tools/resilience_check.py` |
| `performance.mdx` | the measured numbers and how to measure them yourself | `docs/prototype-status.md` |
| `compared.mdx` | Pondra next to Spark, Flink, Fluss, DuckDB, Databricks, Snowflake | `docs/comparison-spark-flink-fluss.md` |
| `design-notes.mdx` | the ADRs, one line each | `docs/README.md` |

## SQL tab (`sql/`)

| Page | Owns | Sources |
|---|---|---|
| `index.mdx` | the dialect (DataFusion's, Postgres's forms, DuckDB's and Spark's spellings) and a map of the SQL pages | |
| `statements.mdx` | every statement, its syntax and an example; what is refused by name. *Splits by category (planned).* | `src/write.rs` (`parse`), `src/routines.rs`, the modules' `statement()` |
| `functions.mdx` | the functions Pondra adds to DataFusion's (JSON, vectors, files, AI, Spark's, `pondra.*`) | `src/udf.rs`, `src/files.rs`, `src/ai.rs` |
| `table-options.mdx` | every `WITH (…)` option of tables, materialized views and functions | the ADRs |
| `system-tables.mdx` | system columns and every `pondra.*` table | ADR-020, ADR-027, ADR-048 |
| `glossary.mdx` | every kind, part and pattern, written from `GET /kinds` (`tools/docs_check.py --write-glossary`) | `src/objects.rs` |

## APIs tab (`reference/`)

| Page | Owns | Sources |
|---|---|---|
| `index.mdx` | **Every door**: what each is for, where it listens, the same statement through several | `src/main.rs` |
| `python.mdx` | the Python client: `local`, `connect`, every method, with its signature and an example | `python/pondra/client.py` |
| `frames.mdx` | every frame method and expression | `python/pondra/frame.py` |
| `javascript.mdx` | the JavaScript client | `js/index.js` |
| `names.mdx` | the one vocabulary: `read_*`, `write_*` and their fallbacks everywhere | `docs/dataframe-api.md` |
| `cli.mdx` | every `pondra` command and flag | `src/main.rs`, `src/project.rs`, `src/service.rs` |
| `http.mdx` | every HTTP route: method, path, parameters, body, answer | `src/server.rs` (`router`) |
| `postgres.mdx` | the Postgres port: connecting, types, `COPY`, parameters, drivers | `src/pg.rs` |
| `flight.mdx` | Arrow Flight and Flight SQL | `src/flight.rs` |
| `kafka-protocol.mdx` | the Kafka port's details: topics, keys, offsets, compression, auth | `src/kafka.rs` |
| `iceberg-rest.mdx` | the Iceberg REST catalog: routes, namespaces, what writers may commit | `src/iceberg.rs` |
| `mcp.mdx` | MCP: the tools and connecting clients | `src/mcp.rs` |
| `environment.mdx` | every `PONDRA_*` environment variable, with its default | `grep -rn 'PONDRA_' src python js` |

## Who owns a topic that many pages touch

| Topic | Owner | Others say one line and link |
|---|---|---|
| materialized views kept current | `guides/streaming.mdx` | tables, flows, tour |
| keyed tables | how: `guides/tables.mdx`; why: `concepts/keyed-tables.mdx` | streaming, change-rows, kafka |
| `local()` and `connect()` | `reference/python.mdx`, `reference/javascript.mdx` | first-lake shows them once |
| tokens, users, grants, TLS | `guides/security.mdx` | every protocol page |
| `CREATE SECRET` | `guides/files-and-other-lakes.mdx` (its use); `guides/security.mdx` (its sealing) | kafka, other-engines, procedures, deploy |
| `CALL run` and parameters | `guides/workspace.mdx` | functions-and-procedures, notebooks |
| files on S3 and other clouds | `guides/files-and-other-lakes.mdx` | object-storage (lakes there, not files) |
