# The site's pages

Paths are under `site/src/content/docs/`. The sidebar is `astro.config.mjs`: three tabs (Docs, SQL,
APIs), each a group of groups, and a page's place there is its order (no `sidebar.order` in pages).
`site/STYLE.md` says how a page is written; this file says what each page **owns**. A topic is
explained on its owner page only; every other page says one line and links there.

Pages marked *(planned)* aren't written yet.

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
| `tables.mdx` | `CREATE TABLE` and its layouts (keys, `PARTITION BY`, `CLUSTER BY`, `SEQUENCE BY`, `TTL`, merge columns), schemas, `ALTER TABLE`, `DROP`, `SWAP`; sequences, indexes, types and constraints in short | ADR-019, ADR-021, ADR-022, ADR-049 |
| `load-data.mdx` | `INSERT`, exactly-once `append` (producer, seq), bulk loads with a job, `COPY FROM STDIN`, files into tables, `write_table` from pandas, Polars and Arrow | ADR-009, ADR-020 |
| `query.mdx` | how a read sees the lake (snapshot, log tail), DuckDB's spellings, `ASOF JOIN`, `$name` parameters, the result cache, `EXPLAIN`, what runs learned, spreading over nodes | ADR-015, ADR-020, `src/friendly.rs` |
| `measures.mdx` | `MEASURE` and measures in views, key facts (keys, uniques, references) declared on views | ADR-065 §2–3, `src/measures.rs`, `src/facts.rs` |
| `change-rows.mdx` | `UPDATE`, `DELETE`, `MERGE`, jobs, system columns, the change feed, `BEGIN … COMMIT` | ADR-020, ADR-036 |
| `past.mdx` | `AT (…)`, `RESTORE`, `UNDROP`, `CLONE`, how long the past is kept | ADR-043 |
| `files-and-other-lakes.mdx` | files anywhere (`read_*`, globs, Hive folders, S3, GCS, Azure, HTTPS), `CREATE SECRET`, Delta and Iceberg read and written, `ATTACH`, `COPY … TO`, external tables | ADR-026, ADR-028 |

### Streams (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `views.mdx` | materialized views kept current: filled from the rows there, kept by key or run whole, the newest row by event time, streaming tasks, `/watch`, live queries | ADR-020, ADR-055, ADR-056, ADR-059 |
| `windows.mdx` | windows and sessions: `EMIT FINAL`, lateness, the watermark, late rows, quiet streams, sliding windows, sessions | ADR-016, ADR-017, ADR-052 |
| `stream-joins.mdx` | `join = 'streams'` and `ASOF JOIN` over a stream | ADR-017, ADR-020 |
| `flows.mdx` | views of views in one commit, rollups, expectations, history views (SCD type 2), `DETACH`, `pondra.flows` | ADR-036 |
| `kafka.mdx` | the Kafka port (producers, Debezium, consumers, groups, SASL), other Kafka clusters as tables, `COPY … TO` a topic, views fed by a topic | ADR-011, ADR-026 |

### Python and code (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `sql-and-python.mdx` | SQL and Python together, both ways: `db.sql` naming frames and pandas data, `write_table`, `%%sql` and a cell named into Python, `DO LANGUAGE python`, Python functions in SQL | ADR-022, ADR-023, ADR-025 |
| `dataframes.mdx` | `pondra.frame` with Polars' names: building, joining, windows, writing, views | `docs/dataframe-api.md`, `python/pondra/frame.py` |
| `pyspark.mdx` | `pondra.spark`: the session, DataFrames, `functions as F`, windows, readers and writers, UDFs, `spark_sql`, what differs | `python/pondra/spark/` |
| `functions-and-procedures.mdx` | `CREATE FUNCTION` (SQL, Python), macros, procedures, notices, secrets in procedures, `@db.function`, `@db.procedure`, `pondra.runs`, `pondra.start` | ADR-023, ADR-027 |
| `scripts.mdx` | variables (`DECLARE $x`), scripts' blocks, branches, loops, handlers, `PARALLEL`/`ASYNC`/`AWAIT`, `EXECUTE IMMEDIATE`, `IDENTIFIER` | ADR-037, ADR-044, ADR-045 |
| `workspace.mdx` | the lake's files run as jobs: `CALL run(…)`, parameters, files running files, the run log | ADR-033 |
| `tasks.mdx` | `CREATE TASK`: schedules, graphs (`AFTER`, `WHEN`), retries, timeouts, `SUSPEND`/`RESUME`, `pondra.tasks` | ADR-045 |
| `notebooks.mdx` | Jupyter with `pondra.local()`, `%load_ext pondra`, frames' display (SQL cells are `sql-and-python.mdx`'s) | ADR-023 |

### Teams (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `environments.mdx` | branches: `CREATE DATABASE … CLONE`, writes apart, `pondra.diff`, `REFRESH`, `pondra branch` and `switch`, separate servers | ADR-047, ADR-058 |
| `projects.mdx` | project files, `pondra init`, `plan`, `apply`, `test`, `status`, `capture`, `export`, `diff`, `login`, migrations, seeds, protected databases, `pondra ci init` | ADR-060, `src/project.rs`, `src/apply.rs` |
| `git.mdx` | Git folders: git tokens and their `SCOPE`, `git_checkout`, `git_status`, `git_commit`, `git_pull`, `git_pull_request` | ADR-060 §8, `src/git.rs` |
| `sharing.mdx` | sharing with another company: shares, recipients, profiles, the delta-sharing client, `ATTACH … (TYPE share)`, the audit | ADR-046 |
| `security.mdx` | tokens, users, roles, grants, TLS on every door, the audit log, quotas, secrets' sealing, `--python` | ADR-035, `src/users.rs`, `src/tls.rs` |
| `single-sign-on.mdx` | identity providers: `CREATE IDENTITY PROVIDER`, `USERS`, `ROLES_CLAIM`, the console's buttons, `pondra login`, Postgres (token as password, psql 18's `POSTGRES 'oauth'`), Kafka, Flight, MCP; Okta, Entra ID, Google, Keycloak | ADR-074, `src/sso.rs`, `src/sso_flow.rs` |

### Connect (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `dbt-and-bi.mdx` | dbt, psql, DBeaver, Tableau, Power BI, Excel through the Postgres port | `src/pg_catalog.rs` |
| `other-engines.mdx` | publishing Delta and Iceberg, other engines reading them, the Iceberg REST catalog, other engines writing through it | `docs/lake-format.md`, ADR-028, ADR-029 |
| `ai-and-files.mdx` | vectors and search, model calls in SQL, files in the lake (`files()`, `file_read`), JSON functions | ADR-013, ADR-051 |
| `agents.mdx` | AI agents through MCP: connecting Claude, Cursor and others, their token, procedures as tools | `src/mcp.rs` |

### Operate (`guides/`)

| Page | Owns | Sources |
|---|---|---|
| `clusters.mdx` | several nodes on one lake, the leader, failover, `--ack replicated`, readers, spread queries and the guard, `pondra sql` with no node | ADR-004, ADR-005, ADR-015, ADR-020 |
| `object-storage.mdx` | lakes on S3, R2, MinIO, GCS, Azure: credentials, the SSD cache, costs, freshness | ADR-006, ADR-009 |
| `server.mdx` | `pondra serve --lakes`: a folder of lakes as databases, local or in a bucket | ADR-030, ADR-032 |
| `deploy.mdx` | the container image, Compose, the Helm chart, `pondra service` | ADR-041, `deploy/` |
| `upgrades.mdx` | stopping without dropping work, `/healthz` and `/ready`, rolling upgrades, the lake's format | ADR-039 |
| `backups.mdx` | `BACKUP DATABASE`, `RESTORE DATABASE`, `pondra backup` and `restore`, runs and their retention, reading and cloning a backup, what a backup holds | ADR-070, `src/backup.rs` |
| `observability.mdx` | `pondra.history`, slow statements and their plans, `/metrics`, `/stats` | ADR-048, ADR-050 |

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
| `mcp.mdx` | MCP's look-up: the tools, the wire, errors (connecting an agent is `guides/agents.mdx`'s) | `src/mcp.rs` |
| `environment.mdx` | every `PONDRA_*` environment variable, with its default | `grep -rn 'PONDRA_' src python js` |

## Who owns a topic that many pages touch

| Topic | Owner | Others say one line and link |
|---|---|---|
| materialized views kept current | `guides/views.mdx` | tables, flows, tour |
| keyed tables | how: `guides/tables.mdx`; why: `concepts/keyed-tables.mdx` | views, change-rows, kafka |
| `local()` and `connect()` | `reference/python.mdx`, `reference/javascript.mdx` | first-lake shows them once |
| tokens, users, grants, TLS | `guides/security.mdx` | every protocol page |
| identity providers, single sign-on | `guides/single-sign-on.mdx` | security, postgres, kafka-protocol, cli |
| `CREATE SECRET` | `guides/files-and-other-lakes.mdx` (its use); `guides/security.mdx` (its sealing) | kafka, other-engines, procedures, deploy |
| `CALL run` and parameters | `guides/workspace.mdx` | functions-and-procedures, notebooks |
| files on S3 and other clouds | `guides/files-and-other-lakes.mdx` | object-storage (lakes there, not files) |
