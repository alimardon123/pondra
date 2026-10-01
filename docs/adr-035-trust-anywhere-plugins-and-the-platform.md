# ADR-035: Trust anywhere: users, keys and secrets for every form; plugins; files with versions; the platform around the lake

**Date:** 2026-10-01 · **Status:** §2 and §3 built (round 29 part 2, 2026-10-01); the rest proposed, for later rounds · **Builds on:** ADR-011 (open doors), ADR-027 (secrets, procedures), ADR-029 (anyone's compute, one catalog), ADR-031 (extensions), ADR-033 (the workspace), ADR-034 (the console)

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

## Built (round 29, part 2, 2026-10-01)

§2 and §3, with these decisions taken while building them (*by Claude*):

- **Users and roles** are one record each under `u/<name>` (a role can't sign in); `public` is every
  user. `CREATE USER … PASSWORD … [SUPERUSER]`, `CREATE ROLE`, `ALTER USER`, `DROP USER|ROLE`,
  `GRANT`/`REVOKE` (SELECT on some columns, INSERT, UPDATE, DELETE, ALL; on a table, a schema — its
  later tables too, as Snowflake's FUTURE grants — or the lake; USAGE on a secret; a role to a user),
  `CREATE TOKEN … FOR USER … [EXPIRES IN …]`, `pondra.users`, `pondra.grants` (`users.rs`).
- **A password is kept as SCRAM-SHA-256's verifier**, not Argon2id: one hash then serves every door,
  Postgres's SCRAM included (as Postgres keeps it), and a cleartext password (HTTP Basic, Kafka's
  PLAIN, Flight's handshake) is checked by hashing it as a client would, 10,000 iterations, a checked
  one remembered a minute. A token is kept as its SHA-256.
- **Sessions are signed with an HMAC key the lake keeps** (`z/auth`), not Ed25519: only nodes check
  them today, and every node holds the catalog (§1). Ed25519 comes when a browser or a peer must
  check one. The same record holds the key nodes call each other with when no admin token is set.
- **One check**: every door works out a `Principal` and runs the request inside `auth::WHO`; a
  query's tables are registered as the user may read them (`query::Guarded`: a scan of a column it
  may not read is refused, the columns its filters use included, as Postgres refuses `SELECT *` on a
  table granted in part); writes are checked by privilege (`auth::allows`); a stream of a table's
  rows (Kafka, Flight's log, `/watch`, lookups, the Iceberg catalog's loads) needs every column; a
  user's queries run on the node that planned them (its grants checked there), and its answers are
  cached as its own; a live query re-reads its user's grants each time.
- **Not yet:** a view lending its owner's rights (a user reading a view needs SELECT on what it
  reads), row filters and masks, CREATE privileges on a schema (DDL is a superuser's).
- **Secrets**: each sealed with a data key of its own, wrapped by the master key. The master key is
  `PONDRA_SECRET_KEY` (or this machine's), or **a key service through a command**
  (`PONDRA_KMS_COMMAND wrap|unwrap`), rather than an SDK per cloud: one seam, any KMS (a short
  script over `aws kms`, `gcloud kms`, `az keyvault key`, Vault's transit), and no SDK in the binary.
  A new master key, with the old one as `PONDRA_SECRET_KEY_PREVIOUS`, has the leader rewrap every
  data key when it starts. `GRANT USAGE ON SECRET`; `CREATE TEMPORARY SECRET` in the session's
  memory, used before the lake's; sessions are their user's (another user naming the same session id
  has its own).
- Found and fixed on the way: a Postgres client could sign in as `reader` with an empty password
  when no read token was set, and Postgres reads weren't held to a role; the AI functions' calls
  carried the nodes' admin token to the AI endpoint.

### The rest of §5 (round 29, part 2, 2026-10-01)

- **TLS on every door, on the door's own port** (`tls.rs`): HTTP, Kafka and Flight tell a TLS
  client from a plain one by its first byte (a handshake starts with 22), so no second port and no
  proxy; Postgres takes it as Postgres does (`SSLRequest`, through pgwire's own TLS). rustls on
  aws-lc-rs, already in the binary for the bucket's HTTPS. *By Claude:* with a certificate, a plain
  connection is taken only from the node's own machine (loopback, or its own address), so the
  shell, `pondra.local()` and Python workers calling back need nothing, and a password or a token
  never crosses a network in the clear; `PONDRA_TLS=optional` takes plain from anywhere.
- **Nodes over HTTPS, mutual TLS with an authority** (`--tls-ca`): nodes trust the authority (or,
  without one, the shared certificate) and show their certificate when they call; a request with the
  nodes' own key (`pn_…`) is taken only over a connection whose certificate the authority signed.
  Rejected for now: certificates the leader issues (a join token, §5's laptop peer): an outside
  authority first, the leader's own with the laptop peer. A served folder of lakes (`dbserver.rs`)
  takes TLS at the server; each database's node listens on this machine only, plain.
- **The audit log** (`audit.rs`, `pondra.audit`): a hidden append table with a TTL, written by a
  writer on each node in batches (as the run log), exactly once. Classes as pgaudit names them
  (`role`, `ddl`, `function`, `write`, `read`, `misc`); the default is `role,ddl,function`, cheap
  enough to leave on; refusals always (a sign-in failed at any door, a request its rights or its
  quota didn't cover). A statement that makes a user, a token or a secret is kept with its quoted
  values as `'***'`. One wrapper (`audit::statement`) every door's statement goes through: the
  HTTP door's scripts and its single-query path, Postgres, Flight SQL, MCP; a procedure's own
  statements are its call's. Only a superuser reads it, and it is never answered from the result
  cache.
- **Quotas** (`users::Quota`): per user, on each node, `MAX_QUERIES` (a semaphore: the rest wait
  their turn, 30 s at most) and `STATEMENT_TIMEOUT` (the statement's future dropped, as
  `statement_timeout`), defaults for every user from `PONDRA_USER_QUERIES` and
  `PONDRA_USER_TIMEOUT`; tokens and superusers have none. Rejected for now: memory per user
  (DataFusion's pool is the node's; a pool per user means a runtime per query) and a cluster-wide
  count (a round trip per statement).
- **A panic in a request is an error** (`panics.rs`): panics unwind now (`panic = "abort"` gone);
  each HTTP request, each Postgres statement and each Kafka and Flight connection is a door whose
  panic is answered (HTTP 500, `XX000`) or ends that connection; DataFusion hands a panic in a
  query's own tasks back to the request. The node's own loops (committing, tiering, following,
  the run and audit logs, the doors' accept loops) are started with `panics::spawn`, which still
  stops the node on a panic: a broken invariant there is not survivable. The cost: unwinding tables
  make the binary about a fifth bigger (the release build here, stripped: 143 → 177 MB, pgwire's
  TLS included); no request stopping a node is worth it, and the dist build's LTO keeps it smaller.
- **Fuzzed** (`tools/fuzz_doors.py`): malformed and random input at all four doors, and generated
  SQL. Against the build before this one it stopped the node through `/cluster/commit` (a flush
  header's length past its body: fixed) and through pgwire's decoding of `Bind`, `Parse` and
  `CopyData` messages cut short (pgwire's, 0.41: now that connection's alone); Kafka's and Flight's
  parsers held.
