# ADR-035: Trust anywhere: users, keys and secrets for every form; plugins; files with versions; the platform around the lake

**Date:** 2026-10-01 · **Status:** proposed (the owner's questions; parts are round 29 part 2, the rest later rounds) · **Builds on:** ADR-011 (open doors), ADR-027 (secrets, procedures), ADR-029 (anyone's compute, one catalog), ADR-031 (extensions), ADR-033 (the workspace), ADR-034 (the console)

## Context

The owner, 2026-10-01, asked five things before round 29's security part:

1. How users' credentials and Pondra's secrets work when they live in the lake, which every node,
   and in time a laptop, a browser tab or another engine, reads from anywhere; what decrypts them,
   and how secure, fast and simple that is in every form Pondra runs in (one node, a cluster, in
   memory, in-process, peers, WebAssembly).
2. How extensions, and plugins that add more than SQL (ETL, dbt, reporting, BI, ML tools), are kept
   and found by every session.
3. Machine learning, AI and GPUs, as Spark, Databricks and Snowflake have them.
4. Whether the browser can have Pondra's full power, without the limits said so far.
5. Notebook versions outside `notebooks/`, and whether the workspace is ready for git and CI/CD.

Today: three tokens (read, write, admin) shared by every node; no TLS; `CREATE SECRET` sealed with
`PONDRA_SECRET_KEY` (AES-256-GCM) in the catalog, used by any query whose URL a secret's scope
covers, its values readable only by a procedure's code; a node's own disk closed to SQL; and, as
the security guide says, **tokens guard nodes, not the bucket**.

## Decision (proposed)

### 1. The root of trust, said plainly

Whoever holds the bucket's credentials and the lake's master key is the lake's superuser, as in
every lakehouse. So only **trusted compute** holds them: nodes (single, clustered, serverless) and a
program the owner runs in-process (`pondra.open`, `pondra sql`). Everything else, **untrusted
compute** (a browser tab, a laptop peer, Spark, DuckDB, an agent), holds neither, and reaches data
only through a node's check of its grants.

### 2. Credentials are verified, never decrypted

- A user's password is kept as an Argon2id hash, a token as the SHA-256 of 32 random bytes, in the
  catalog (`u/`). Any node verifies a sign-in by hashing what it is given: no key is needed, and a
  catalog read by the wrong person gives away no password.
- **SSO:** OIDC (Okta, Entra ID, Google, Keycloak). A node checks the identity provider's signature
  (its JWKS) and maps the claims to users and roles; nothing of the user's is stored.
- **Sessions:** a sign-in returns a short-lived session token signed with the lake's Ed25519 key:
  any node or peer verifies it with the public half, only nodes issue it. Service accounts take a
  token, a client certificate, or their cloud's workload identity.
- The three tokens stay, as the first admin and for scripts, and every door (HTTP, Postgres with
  SCRAM, Kafka SASL, Flight, MCP, Iceberg REST) calls one check.

### 3. Secrets: envelope encryption, opened only by trusted compute

- Each secret is sealed with its own data key; the data keys are wrapped by **the lake's master key,
  which is never in the lake**: a cloud KMS (AWS KMS, Google Cloud KMS, Azure Key Vault) or
  HashiCorp Vault, reached with the node's own cloud identity; on a laptop, the OS keychain or
  today's `~/.pondra/secret.key`; `PONDRA_SECRET_KEY` still works. Rotating the master key re-wraps
  the data keys, never the data.
- **Kept for every session:** secrets live in the catalog, as today; a lake reopened anywhere with
  its master key has them. `CREATE TEMPORARY SECRET` (the owner's approval, 2026-10-01) lives only in
  its session's memory, never in the lake.
- **Used only as granted:** `GRANT USAGE ON SECRET s3_sales TO analyst`. A query that reads a URL
  uses a secret whose scope covers it *and* that its user may use. Values are never readable in SQL;
  a procedure's code reads them (`pondra.secret`), blanked from what it says, as today.
- A browser, a laptop peer or another engine never opens a secret: the part of a query that needs
  one runs on a node.

### 4. Data for untrusted compute: the catalog door, scoped URLs, column keys

Untrusted compute asks a node's **catalog endpoint** (Iceberg REST's shape, which ADR-029 serves)
for a table at a snapshot, as a user. The answer holds only what that user may read:

- the files, each with a **short-lived URL signed for it** (or credentials scoped to the table's
  prefix where the store mints them: S3 session policies, R2's temporary credentials, GCS's
  downscoped tokens); the client reads Parquet straight from the bucket;
- **column grants by keys:** a table can be written with Parquet's modular encryption, a key per
  column (or per group of columns), wrapped by the master key. The endpoint hands out only the keys of
  the columns granted, so a column grant holds even for someone reading the files directly;
- **row filters and masks** can't be given as files (a file holds every row): such a table is read
  through a node, which streams back only the rows and values allowed. Databricks' credential
  vending has the same limit.

Writes go the same way: files to a staging prefix under a URL signed for it, then a commit through
the endpoint (ADR-029's path for Spark's writes). Grants are cached per catalog version, as the
lake's tasks are: a check is a hash lookup per statement. Masks and row filters are rewritten into
the plan, so they cost what the filter costs.

### 5. Every form, one model

| Form | Holds | Reaches data through |
|---|---|---|
| One node, a cluster, serverless | bucket credentials, master key (KMS) | itself; checks every door |
| In-process (`pondra.open`, the shell) | the owner's credentials | itself: its owner is superuser, as any library's |
| A laptop peer in a cluster | a short-lived certificate (a join token) | plan fragments over data its user may read; shuffles over mTLS |
| A browser tab (WebAssembly) | a session token | the catalog endpoint; scoped URLs; column keys |
| Spark, DuckDB, Trino, an agent | a user's token | the catalog endpoint (Iceberg REST), `/mcp` |

TLS on every door; mutual TLS between nodes, certificates from the cluster's CA (the leader, or an
external one); `pondra.audit` (who, what, when, allowed or refused) as a table; quotas per user and
role. The same code path whether one node or a hundred: a cluster is only more doors calling one check.

### 6. Extensions and plugins, kept in the lake

- **Extensions** (ADR-031): WebAssembly components adding functions, formats and connectors, kept
  under `ext/<name>/<version>.wasm` with a catalog entry, so every node and session has them;
  sandboxed, granted capabilities at `LOAD`, signed by their publisher. The same `.wasm` runs in the
  browser build.
- **Plugins:** a package that adds any of:
  - extensions (above);
  - console modules: views, kinds of file, kinds of job (`register.jobKind`), settings sections
    (`register.setting`), answer views;
  - Python packages for the node's workers (dbt-core, a reporting library, scikit-learn);
  - SQL: procedures, tasks, templates.

  One manifest (`pondra-plugin.toml`), kept in the lake as `plugins/<name>/<version>/`,
  `INSTALL PLUGIN dbt [FROM 'url']`, enabled per lake with grants, rolled back by version. Examples:
  dbt projects run as jobs with their lineage drawn; ingestion connectors; dashboards; ML tooling. A
  marketplace is an enterprise build's.

### 7. Machine learning, AI and GPUs

- **Now:** Python functions run vectorized over Arrow on warm workers beside every node and spread
  with their queries: batch inference, and a model trained per partition (Spark's
  `applyInPandas`), with scikit-learn, XGBoost, LightGBM or PyTorch.
- **Next:** `ai_complete`, `ai_embed`, `ai_classify` in SQL (a provider's key a secret, answers in
  the function cache), as Snowflake's Cortex and Databricks' `ai_query`; a vector index; models as
  versioned lake files (§8); keyed tables (0.17 ms lookups) as an online feature store; `/mcp` for
  agents, under grants.
- **Distributed training:** one Python worker per node, started together with the peers' addresses
  (the network SPMD already has), for PyTorch DDP or XGBoost's own.
- **GPUs:** a node started with `--gpu` is labelled, and the planner places GPU functions' stages
  there (RAPIDS, PyTorch). GPU SQL execution is not planned: DataFusion has none, and it would be
  taken on only if a benchmark shows vectorized CPU losing.

### 8. Every file keeps its versions

Today only `notebooks/` keeps a version per save; a plain `.ipynb` elsewhere is replaced in place
and can't be a job (ADR-033 chose git as the history for text files). The owner wants one behaviour
everywhere. Proposed:

- **every file under `files/` keeps its versions**: each save is recorded in the catalog (path,
  version, who, when); earlier versions are kept for a retention the lake sets (days, or a count);
- every file's ⋯ has **Versions…**, and a version opens, compares and **restores**;
- a run records the version it ran (as today), and any notebook, anywhere, can be a job or a
  schedule;
- `notebooks/` becomes an ordinary folder; its old `<name>/<time>.ipynb` layout still opens.

### 9. Git and CI/CD

The workspace already fits git: files are plain files, every object is SQL text in the catalog, and
a run records what it ran. To add:

- `pondra workspace pull | push <dir>`: the lake's files with a git folder (conflicts by version);
- `pondra export | plan | apply <dir>`: every object as a SQL file, and the difference applied, as
  Terraform and dbt do; secrets by name only, their values per environment;
- notebooks exported without their outputs;
- **environments** as lakes (or a server's databases), a staging copy of production by zero-copy
  `CLONE` (round 32); CI applies a branch to a clone, runs its tests (round 30's expectations), then
  promotes;
- later, a Git panel in the console (branch, commit, pull, push).

### 10. The browser at full power

- **Many cores:** several WebAssembly workers in the tab, each a peer running part of the plan (SPMD
  in the tab), sharing Arrow through `SharedArrayBuffer`; the console's page is served by the node,
  so it sends the headers that allow it (cross-origin isolation).
- **Past 4 GB:** Memory64 (Chrome and Firefox; Safari not yet), and spilling to the origin's private
  file system.
- **The catalog:** read through the catalog endpoint (§4), not SlateDB: the secure design anyway.
- **Data:** straight from the bucket, by the scoped URLs (the bucket allows the page's origin).
- **Python:** Pyodide runs Python cells in the tab.
- **What stays:** a tab has its machine's memory and cores and can be closed, and it opens no raw
  TCP (no Kafka or Postgres door in it). The guard sends what is heavy to nodes: MotherDuck's hybrid,
  with any number of peers.

## Rounds

- **29, part 2:** §2, §3 (KMS, `USAGE`, `TEMPORARY`), §5's TLS, mTLS, one check, audit, quotas.
- **Right after part 2** (the owner, 2026-10-01: "after it"): §8 (files with versions), about one round.
- **30–32:** §4 (the catalog door, scoped URLs, column keys) with ADR-029's phase 3; §9's export and
  apply with `CLONE`.
- **34:** §10 with the in-process library.
- **After 1.0:** §6's plugins with ADR-031; §7 beyond what Python functions do today.

## Rejected

- **Giving a browser or a peer bucket credentials**, even read-only: they would read every table.
- **Passwords or tokens encrypted** (so a node could decrypt them): a hash needs no key to keep.
- **One master key in an environment variable in production:** it stays for laptops and tests; a
  KMS keeps it out of every file, process list and backup.
- **A plugin format per layer** (functions one way, UI another, Python a third): one package, one
  version, one rollback.
