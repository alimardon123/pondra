# ADR-075: Extensions: one line to install, one build for every version and every form

**Date:** 2026-10-24 · **Status:** accepted (the owner's "Ok", 2026-10-24 02:39, to all eight
decisions in `reviews/extensions-design.md` as recommended); built from 1.1, its first row before
1.0 · **Merges and replaces:** ADR-031 (extensions as WebAssembly
components) and ADR-035 §6 (plugins) · **Builds on:** ADR-013 (functions over Flight), ADR-027
(Python on the node's workers), ADR-049 and `designs/rules-for-new-kinds.md` (one registry), ADR-060
§9 (every tool takes part in branch and apply), ADR-068 (what 1.0 keeps), ADR-072 (one core, one
request API, every form) · **Advised by:** two outside reviews of the key calls (§2, §3, §6, §8 took
the first's points; §4's deep hooks and §10's installed tools the second's)

## Context

The owner, 2026-10-24 01:28: an ecosystem like DuckDB's, easy to use, to build and to maintain.
Extensions can change whole parts of the platform and add to the console, so ETL tools, BI tools and
whole apps plug in. Our own ETL and BI are the default ones; open-source BI and ETL tools come in as
extensions too. And an extension must keep working across Pondra's versions, backward and forward,
without a rebuild for each release, which DuckDB gets wrong.

His two notes, 02:14: the system must be deep and flexible, able to change much of the product the
way DuckDB's extensions change DuckDB, even its syntax. And big tools (Superset, Metabase, Grafana,
dbt) should install through the same system, official ones by us or by their makers, with their
screens inside our console and everything kept in sync.

The two pull against each other once: DuckDB's depth comes from linking its C++ internals, which is
exactly why its extensions are rebuilt each release. §4 gets nearly the same reach through carriers
that don't change between releases (SQL text, Arrow batches and types, Substrait expressions), and
says plainly the little it still refuses. Snowflake's Native Apps, the richest marketplace of its
kind, make the same trade: SQL, functions and containers, never the planner.

### What exists

| Piece | State |
|---|---|
| ADR-031: extensions as WebAssembly components, `INSTALL`/`LOAD`, a WIT world `pondra:extension@1`, kept in the lake, sandboxed, grants at `LOAD` | proposed, not built |
| ADR-035 §6: a plugin is a package of extensions, console modules, Python packages and SQL | proposed, not built |
| The console registers everything through `window.pondra` (`register.view`, `.doc`, `.renderer`, `.action`, `.objectKind`, `.jobKind`, `.setting`, `.command`, …), its own parts as an extension's (invariant 145); `PONDRA_CONSOLE_EXTENSIONS` loads more scripts (`examples/console-extension.js`) | built |
| One registry of kinds (`objects::KINDS`, prefix, verbs, `on_clone`, `project`); AGENTS.md: a plugin's entries name their origin, live under `pl/{plugin}/`, take no word in use | built (origin not yet) |
| ADR-060 §9: a tool's objects are `KINDS` entries; its files claim a project folder | rule, built with the first tool |
| The doors behind one request API (ADR-072 §3), all kept by ADR-068; BI tools on the Postgres port: Metabase, Superset, Grafana, Tableau, DBeaver, Power BI (`harness.py bi`, L3) | built |
| Functions in SQL, in Python (warm workers, `packages =` installed with uv), over Flight (ADR-013) | built |
| A seam by command: `PONDRA_KMS_COMMAND wrap|unwrap` | built |
| MCP at `POST /mcp`: `list_tables`, `query`, `write`, `changes`, the `find` tools, every procedure as a tool, sessions with transactions, the same grants; ADR-068 keeps its tools and arguments | built |

### DuckDB's lesson

DuckDB's extensions are native libraries. Those on its C++ API, most of the big ones, are built per
DuckDB version and per platform, about eight builds each release, three of them for the browser.
DuckDB 2.0 added a stable C API: an extension built on it loads on later releases. That fixes
backward, not forward (an older DuckDB can't load an extension built for a newer API), keeps one
build per platform, and keeps no sandbox: a faulty extension can crash the process or read the
bucket's credentials.

### Measured (2026-10-24, Wasmtime 50.0.0, 4 cores; the program is `reviews/extensions-bench/`)

| What | Number |
|---|---|
| Wasmtime with its compiler (Cranelift, component model, pooling), stripped, thin LTO | **+10.3 MB** on a bare binary (likely less on Pondra's, which already links some of its crates; to be measured in the build) |
| Compile a small module once (at `INSTALL`, or after an upgrade) | 1.7 ms |
| Load it from the compiled cache | 0.06 ms |
| A fresh instance from the cache (per worker, not per call) | 5 µs |
| A call into the sandbox and back | 13 ns |
| `x*3+1` over 8,192 BIGINTs: copy in, call, copy out (the copies are 2.5 µs of the 14) | about 580 M rows/s |
| the same in native code, which the compiler vectorized (the wasm loop is hand-written and scalar) | about 4,000 M rows/s |
| ASCII upper-case over 64 KB of text, same way | about 460 MB/s (native 11,000 to 20,000) |
| The same 8,192 BIGINTs through a loopback socket and back, as to another process, no encoding at all | 75 to 88 µs per batch, about 100 M rows/s |

So crossing into the sandbox costs little (13 ns a call, 2.5 µs to copy a batch in and out), while
just moving a batch to another process and back costs about 6 times a whole sandboxed call, before any
gRPC or Arrow framing. The code inside runs slower than native, a seventh on a loop native code
vectorizes; a Rust guest built with WebAssembly's SIMD should narrow that (not yet measured). That
sets the tiers in §2 and §4.

## Decision

### 1. One word, one package

**Extension** is the one word (`INSTALL`'s, DuckDB's, the owner's). "Plugin" goes in `also`. An
extension is one package: a folder or a zip with `pondra-extension.toml`, holding any of these parts.
A bare `.wasm` is an extension with one part, so `INSTALL geo FROM 'geo.wasm'` still reads.

| Part | Adds | Runs | Speaks |
|---|---|---|---|
| `wasm/*.wasm` | scalar, table and aggregate functions; file formats; sources over HTTP; providers (§4) | sandboxed, in every form, the browser too | the WIT world (§2) |
| `sql/*.sql` | objects: views, macros, procedures, tasks, types, measures, an ontology, new kinds made of SQL | the engine | SQL |
| `console/*.js`, `*.css` | sections, tabs, views, kinds of file, kinds of job, answer views, settings, commands, a whole app's screens | the page | `window.pondra` (§5) |
| `python/` (`requirements.txt`, `*.py`) | procedures and functions in Python (dbt-core, dlt, a reporting library) | the node's Python workers; refused by name in the browser | ADR-027 |
| `[app]` in the manifest | a whole tool that is a service (Superset, Metabase, Grafana): its container image, its user and grants, its screens served through the node, its objects synced into project files (§10) | a container the machine's runtime starts, or a URL already running | the doors |

Everything a part declares lands in the registries the core already uses: `KINDS` (with `origin`),
functions, `register.*`, settings, the MCP tools. There is no second listing of anything.

### 2. The contract: what ADR-068 keeps, plus one small world

An extension never links against Pondra's Rust. That is the whole reason no rebuild is needed: the
contract is versioned, not the build.

- **What an extension says to Pondra** is SQL and the request API (`request(method, path, headers,
  body)`, ADR-072 §3), both kept by ADR-068 and checked by `docs/surface.json`. A wasm part reaches it
  through one import; a console part through `api.request`; an app through its door. So an extension
  can do anything the doors can, as its own user, under its grants.
- **How Pondra calls a wasm part** is a WIT world kept small on purpose:
  - `describe() → json`: name, version, functions with argument and result types, volatility,
    capabilities asked, examples, text for docs and for MCP. The JSON only grows; a host ignores
    fields it doesn't know.
  - `call(name, batch) → batch` for scalars; `open`, `next`, `close` for tables; `init`, `update`,
    `merge`, `finish` for aggregates. A batch is Arrow IPC bytes.
- **Capabilities are separate imports, each versioned on its own:** `request@1`, `log@1`, `clock@1`,
  `http@1` (fetch, to granted hosts only), `secret@1`, `files@1` (byte ranges of granted files). New
  powers come as new imports. An import's `@2`, if ever needed, comes beside its `@1`, which stays.
  The build tool reads the imports from the component, so `needs` is never a hand-kept list.
- **Network is `http@1`, not raw sockets.** A browser has only `fetch`; sockets would break "runs in
  every form". A connector that needs a wire protocol (MySQL, Oracle) is a process (§4).
- **Deterministic unless it says so.** A function that imports `clock`, `http` or `request` is
  volatile in `describe()`, so a spread query never gets two answers from two nodes.

### 3. Versions, backward and forward

- **The rule:** a Pondra loads an extension when it offers every import the extension needs, and
  calls only the exports it knows.
- **Backward** (a newer Pondra, an older extension): always. No 1.x removes an import or a field;
  `surface.json` fails a pull request that tries.
- **Forward** (an older Pondra, a newer extension): whenever the extension needs nothing newer.
  When it does, the refusal names it: `h3 2.1 needs http-stream@1 (Pondra 1.3); this is 1.2`. `INSTALL h3` picks the newest version whose needs this Pondra meets, as pip picks by
  `Requires-Python`. An extension that uses a newer power only when present asks at run time
  (`host.has("…")`) instead of importing it, so one build serves old and new.
- **A newer kind of export** (say a window function) on an older Pondra: the rest loads, and `SHOW
  EXTENSIONS` lists that function as needing the newer release.
- **The same for the other parts:** console modules ask `pondra.api.version` and `pondra.has(…)`;
  SQL parts declare the release whose functions they use, checked against `surface.json` at build.
- **Proof, not memory:** `pondra extension test` runs an extension on the oldest and the newest
  Pondra its needs admit. The core's CI runs the index's most used extensions against every main
  build, so a change that breaks one fails like a removed SQL function does.
- **Compiled code is a cache.** The lake keeps the `.wasm`; a node compiles it once per Wasmtime
  version (1.7 ms for a small module) and keeps the result on disk. An upgrade costs one
  compile, never a rebuild by the author.

### 4. What an extension may change

Two layers. **Providers** swap a whole part behind a seam. **Deep hooks** change how Pondra itself
reads, plans and stores, as DuckDB's extensions do, but through carriers that stay the same from one
release to the next, never DataFusion's Rust traits.

#### 4a. Providers

Each seam is a registry whose first entry is Pondra's own, registered the same way, and each already
has a statement. An extension adds a `TYPE` to it, so there are no new verbs:

| Seam | Pondra's own | An extension adds | Its statement |
|---|---|---|---|
| functions, table functions, aggregates | built-ins | H3, geometry, a URL parser | used in any query |
| file formats | Parquet, CSV, JSON, Avro, Excel | shapefiles, XML, Lance | `read_x(…)`, `FROM 'f.x'`, `COPY … (FORMAT x)` |
| sources and outside catalogs | Kafka, Postgres, Iceberg REST, Unity, Glue | an API as a table, Salesforce, a company's catalog | `ATTACH … (TYPE x)` |
| sign-in | OIDC | SAML, LDAP | `CREATE IDENTITY PROVIDER … TYPE x` |
| key service, secrets | `PONDRA_KMS_COMMAND`, the lake's own | Vault, an HSM | `CREATE SECRET … TYPE x` |
| AI models | OpenAI-style endpoints | a local model server | `CREATE MODEL … TYPE x` (ADR-051) |
| query languages | SQL | PRQL, Malloy, translated to SQL text | `LANGUAGE x` |
| alerts and notifications | email, webhook | Slack, PagerDuty | a task's `NOTIFY` target |
| object kinds | tables, views, tasks… | dashboard, pipeline, report | `CREATE x` (a `KINDS` entry) |
| the console | Data, Workspace, editors, grids, charts | an ETL canvas, a BI editor, a new kind of file | `register.*` |

Where a provider needs what a sandbox can't give (a wire protocol, a local library, a GPU), the same
manifest points at a process: a command (the KMS seam's shape) or a service (a Flight function,
ADR-013). Same `describe()`, same statement.

#### 4b. Deep hooks

Ranked by what they are worth. Every hook declares itself deterministic or volatile in `describe()`;
a volatile one runs on the planning node only, as Python table functions already do.

| Hook | What it lets an extension do | Carrier | Cost | Browser |
|---|---|---|---|---|
| **Table storage** | tables kept by the extension: `CREATE TABLE t (…) USING lance`, `ATTACH 'x' (TYPE mongo)` | `schema()`, `splits(filters, columns, limit)`, `scan(split)`, `write(batch)`, `commit(json)`; filters as Substrait expressions | one copy per batch (2.5 µs for 8,192 rows) | yes, over `http@1` |
| **Operators** | a step in a plan: sessionize, a model's scoring per partition, a custom deduplication | a table-in, table-out function, `FROM sessionize(TABLE clicks PARTITION BY user ORDER BY t)`: `open`, `push(batch)`, `finish`; the planner shuffles by its `PARTITION BY` and runs it on every node | per batch | yes |
| **Types and casts** | geometry, an H3 cell, money, an IP address, with their casts and operators | Arrow extension types (geoarrow's shape) over a storage type the files keep, as enums keep text | none per row beyond its functions | yes |
| **Syntax** | new statements: `CREATE PIPELINE …`, `MATCH (a)-[r]->(b)` (SQL/PGQ), `PIVOT` forms, whole languages | a pass in the SQL front (`front.rs`, which every form runs): the extension claims its leading words, gets the statement's text, and returns SQL text or request-API calls | one call per statement | yes |
| **Operators in expressions** | `a <-> b`, `geom && box` | declared in `describe()` as a function's other spelling, rewritten once by the front | none | yes |
| **Names that resolve** | `FROM 'gs://…/*.lance'`, a bare name that is a remote dataset | a name the core doesn't know becomes a table function's call text (the front's "files by name" pass is the precedent) | once per name | yes |
| **Settings** | `SET h3.resolution = 9` | declared in `describe()`, under the extension's name, read through `settings@1` | none | yes |
| **Indexes** | a vector index, full text, H3 cells | `CREATE INDEX … USING x`: `build(batches) → bytes`, `prune(filters) → files and row groups`; the core keeps the bytes per commit and drops them on the next | per query, before the scan | yes |
| **Expression rewrites** | a bounding box tried before `ST_Contains` | Substrait expression in, expression out | once per plan | yes |
| **Whole-plan rules** | later, behind the gate below | Substrait plan in, plan out | about 1 ms per plan it sees | yes |

**How depth stays correct.** The engine re-applies every filter above a provider's scan, so a pushdown
done wrong or in part can't change an answer. An index may only prune, never answer, so a false
positive costs time, not rows. A statement's word is refused at install if SQL, Pondra or another
extension already has it (the index's CI refuses clashes across every listed extension), and the
word lands in `pondra.kinds` and the glossary. Every hook kind joins the CI canary: one node against
three, the browser's `web_check`, and TPC-H and TPC-DS answers with the extension loaded. `EXPLAIN`
names every hook that touched a plan, and `SET pondra.hooks = off` runs a session without them.

**Substrait, measured** (`reviews/extensions-bench/substrait.rs`: DataFusion 55's own producer and
consumer, the 22 TPC-H queries on generated data at scale 0.1, each optimized plan encoded, decoded,
run, and its rows compared): **all 22 go through and back, and all 22 give the same rows**. 21 come
back with other column names and extra projections, which change nothing. A plan is 1.9 KB; encoding
takes 0.3 ms and decoding 0.7 ms, so about 1 ms per plan.

So expressions are the carrier now, and whole plans are close. What still holds the rule hook back:
TPC-DS and Pondra's own plan nodes (`ext:` tables, measures, streaming windows) are untried, and
Substrait itself is still 0.x. And 1 ms is more than a key lookup takes (0.17 ms), so a rule sees only
the plans that name something of its own (its tables, its functions), never every query. The steps:
`EXPLAIN (FORMAT substrait)` ships first as an export; the rule hook opens when 95% of the one test
corpus (ADR-072 §6) round-trips with the same rows. A node with no form passes through as an opaque
relation a rule must leave alone, and a rule whose output names something Pondra can't build is
refused by name, and the plan runs as it was.

**Still refused, and why:** replacing the core's own joins and operators, the log and commits, the
lake's file layout, the cluster's protocol, the doors' protocols and the one check of rights, and a
new clause inside `SELECT` (DuckDB's parser extensions get whole statements too, so nothing is lost
against it). These hold the promises: one answer bit for bit on every node (ADR-015 §6), every 1.x
opens every lake, every row kept, rights at every door. A faster join is a pull request to the core.

### 5. The console

- **Same-page modules** (`register.*`, today's API) for extensions signed by the index or by the
  lake's admin: full power, the look of the core. They run with the viewer's session, so they can do
  only what the viewer may, and the page's rule that every request goes to its node (invariant 138)
  holds for them.
- **An app tab** (`register.app({ path })`) for everything else: a tool's screens (§10, served through
  the node at `/apps/<name>/`) or an unsigned module run in a sandboxed iframe with a key narrowed to
  the extension's grants, and a `postMessage` bridge that offers the same `api.request` plus `toast`,
  `navigate` and the theme. One API shape on both sides.
- **`window.pondra` becomes a kept API** (version 1): its `register.*` names and arguments go into
  `surface.json` (a preview in 1.0, kept from 1.1). The console's look stays unkept.

### 6. Install, use, pin

```sql
INSTALL h3;                                    -- the newest version that fits this Pondra, from the index
INSTALL h3 VERSION '2.0';
INSTALL geo FROM 'https://example.com/geo-1.3.zip';
INSTALL maps WITH (network = 'api.maps.example', secrets = 'maps_key');   -- its grants, said once
SELECT h3_cell(lat, lon, 9) FROM trips;        -- on at once, on every node and in every session
SHOW EXTENSIONS;  UPDATE EXTENSIONS;  UNINSTALL h3 [CASCADE];
SHOW CREATE EXTENSION h3;   -- INSTALL h3 VERSION '2.0.3' SHA256 '…' WITH (…)
```

- **One statement turns it on.** `INSTALL` installs and loads. An extension that asks for grants is
  refused until they are given, and the refusal lists them as the `WITH` to write. DuckDB's `LOAD x`
  is accepted for what is installed (a notice: already on), so DuckDB scripts run unchanged.
- **An extension is an object**: a `KINDS` entry `extension` (prefix `xt/`, `on_clone: Copy`, a
  project may declare it), its bytes at `ext/<name>/<version>/` by content hash, the objects its
  kinds make under `xt/<name>/`. `UNINSTALL` removes exactly what it added; objects people made of
  its kinds hold it, as Postgres's `DROP EXTENSION` does, unless `CASCADE`. An admin's act, in
  `pondra.audit`.
- **Every door:** SQL is the door, so `db.sql("INSTALL h3")`, `psql`, Flight, the shell and MCP all
  have it; the console's Settings › Extensions lists the index with Install and the grants it asks.
- **A project pins them** (ADR-060): `pondra.toml` gets `[extensions] h3 = "2.0.3"`; `pondra apply`
  installs exactly that in each environment; `UPDATE EXTENSIONS` on a database a project applies is
  drift, which `pondra status` shows and `pondra capture` writes back. A branch copies them, so a new
  version is tried on a branch and merged.
- **The lake carries them** to every node through the commit stream, and to the browser: the page
  fetches the same `.wasm` and `.js` from the node or by snapshot from the bucket.

### 7. Trust

| Level | Loads | Its console part |
|---|---|---|
| signed by Pondra's index (built from source by the index's CI, ed25519) | anywhere | same page |
| signed by the lake's admin key | in that lake | same page |
| unsigned | with `allow_unsigned_extensions`, an admin's | app tab only |

Wasm parts are sandboxed whatever their level: no files, network or secrets unless granted, a memory
limit (`PONDRA_EXTENSION_MB`, 256) and a time limit per call (epoch interruption); a runaway call
fails its query, the node goes on. Python parts are not sandboxed (ADR-027), so they need a signed
extension or an admin. An app's user gets only the grants its manifest asks, shown at install.

### 8. Building one

- `pondra extension new NAME --sql | --rust | --js | --python` writes the manifest, a test file and a
  README. **SQL and console JavaScript need no toolchain at all**; most ETL and BI templates are that.
- **Rust:** a `pondra-extension` crate with `#[pondra::scalar] fn h3_cell(lat: f64, lon: f64, res: u8)
  -> u64`, Arrow in and out, types mapped from the signature. The fastest guest.
- **JavaScript** (jco) and **Python** (componentize-py) components work but are heavy: CPython inside
  makes a component tens of MB with a slow first call (to be measured before the templates ship).
  Python authors who need numpy or pyarrow write a `python/` part for the workers instead.
- `pondra extension dev` reloads it into a local lake on save, the console's modules too.
- `pondra extension test` runs the declared examples (SQL in, rows out) on a temporary lake, on the
  oldest and newest Pondra it fits, and in the browser (`web_check`).
- `pondra extension publish` opens a pull request to the index: one TOML per extension naming its
  source and tag, as DuckDB's community extensions do. The index's CI builds **one artifact for every
  platform and the browser** (DuckDB builds eight), tests it, signs it, and publishes one JSON with
  the docs site.

### 9. Our ETL and BI are extensions

They are built on the public API only, as the console's own parts already are (invariant 145): if
our BI needs something an extension can't reach, the API is incomplete and gets it. They ship inside
the binary (invariant 138: no CDN; they are JavaScript, SQL and kinds, so small), on by default, and
each can be turned off and replaced by another. Their objects (dashboards, pipelines) are `KINDS`
entries and their files claim project folders (`dashboards/`, `pipelines/`), so branch, apply,
status and capture treat them as tables (ADR-060 §9).

### 10. Whole tools as extensions: Superset, Metabase, Grafana, dbt

`INSTALL grafana` should feel like `INSTALL h3`: one line, the tool's screens open as a tab of the
console, its objects in the project's files. What a tool needs depends on what it is.

**A library** (dbt-core, dlt, SQLMesh, Great Expectations) is a `python/` part on the node's workers.
dbt's project is already files, so it takes its project folder (`dbt/`) and branch, apply, status
and capture treat it as tables with no adapter; `pondra apply` runs `dbt build` against the branch's
database, and the console draws its lineage from dbt's manifest. The real work is the `dbt-pondra`
adapter (L3). We ship dbt-core (Apache), not dbt's newer engine under its own licence.

**A service** (Grafana, Superset, Metabase, later Airbyte, Lightdash) is an `[app]`:

- **Its image, run by the machine's own runtime.** Pondra holds no container runtime or Kubernetes
  client: it calls a command (`PONDRA_APP_RUNTIME = docker | podman | kubectl | none`, the KMS seam's
  shape) with the app's wanted state, so the binary grows by nothing. On Kubernetes the GitOps way
  comes first: `pondra apply` writes the chart's `apps:` values and the node only checks health;
  letting a node make Deployments itself is opt-in, since the chart gives nodes no Kubernetes rights
  today. In the browser, in-process, or with no runtime: refused by name, or `WITH (url = '…')` to
  use one already running.
- **Its screens through the node** at `/apps/<name>/`: the node passes requests on (WebSockets too,
  for Grafana's live panels and Superset's async queries) with the signed-in user, so one sign-in
  covers Pondra and the tool, and the console's tab is the same origin (invariant 138 holds). The tool
  is reachable only through the node and takes the user's name only from it (a network policy and a
  shared secret), since a forged header is the classic hole of proxy sign-in.
- **Its data through a door** (the Postgres port, which L3's replays already pass for these tools),
  as a user of its own with only the grants its manifest asks, written as SQL so branch and apply
  carry it. Later, the viewer's own rights where a tool can pass them.
- **Its own state stays its own.** The tool's metadata database (SQLite in a volume, or a Postgres
  container the manifest names) is never Pondra's Postgres port: their migrations need sequences, row
  locks and isolation Pondra doesn't promise.
- **In sync, explicitly** (ADR-060 rejected automatic two-way sync): the tool's objects are a kind
  with a folder (`apps/grafana/`). `pondra capture` exports what was changed in the tool's screens
  into files, `pondra apply` imports the files into each environment's tool, and `pondra status`
  shows the drift. The adapter is a small `python/` or wasm part over the tool's own API.
- **Its cost said up front.** `INSTALL` and Settings › Extensions show its image size, memory and
  the databases it needs, and one screen shows every app's health and drift.

| Tool | Footprint (about) | One sign-in | Sync | Verdict |
|---|---|---|---|---|
| Grafana | a few hundred MB, SQLite | yes: `auth.proxy` and embedding are in the open-source edition | dashboards are JSON, loaded from files natively | first |
| Superset | 1.5 GB, a metadata Postgres, Redis for async | yes: `AUTH_REMOTE_USER` in a mounted `superset_config.py` | dashboards and charts as YAML (`export`/`import`) | second |
| dbt | a Python package | the node's own | files already | with the `python/` part |
| Metabase | a JVM of about 1 GB, a metadata database | no: header sign-in, SAML and JWT are paid editions; a second login | serialization is paid too; questions through its REST API, partial | offered, with these limits said |

**Who makes them.** The index lists three kinds: *official* (made and kept by us), *verified* (made
by the tool's own makers), *community*. Official images are the makers' own, pinned by digest,
unmodified, with their SBOMs; Grafana and Metabase are AGPL, so we distribute them as they are.

**What the core owns, and nothing more:** the runtime command, the passage at `/apps/<name>/` with the
user, the app's user and grants as SQL, health and the tab, and the sync contract (export to files,
import from files). Everything particular to a tool lives in its extension.

### 11. MCP and agents

MCP is a door, so it stays in the core (principle 10), and it is built. Extensions extend it with no
new contract: their procedures are MCP tools already, their functions are usable in `query`, and
`describe()`'s text becomes the tool's description through the same path as `find::mcp_tools`.
Later, as additions: an extension may declare MCP prompts and resources in its manifest, and a source
extension may call outside MCP servers (an agent's tool as a table function).

## Rejected

- **Native Rust libraries:** Rust has no stable ABI, so one build per release and per platform, no
  sandbox, nothing for the browser.
- **A stable C ABI** (DuckDB 2.0's way): fixes backward only, keeps five builds per extension and none
  for the browser, and no sandbox. Revisited only if a measured extension needs native speed that
  neither the sandbox nor a Flight function gives.
- **Out of process for everything:** 6 times the cost per batch before any framing, and a process
  to run. Kept for what needs it (§4, §10).
- **ADR-031's N and N-1 worlds:** a whole-world version moves every extension at once. Capabilities
  versioned one by one move only what changed.
- **Linking to the planner's Rust for depth** (DuckDB's way): §4b reaches nearly as deep through
  carriers that don't change between releases.
- **Two words and two packages** (ADR-035's extensions and plugins), and three prefixes (`ext/`,
  `plugins/`, `pl/`): one of each.
- **A container runtime or a Kubernetes client inside the binary:** a command does it, at no size.
- **A tool's metadata database on Pondra's Postgres port:** §10.
- **Automatic two-way sync with a tool:** ADR-060 chose capture and apply.
- **WASI sockets for connectors:** not in the browser.

## Phases

| When | What | Proof |
|---|---|---|
| **Before 1.0** (cheap) | DuckDB's `INSTALL x` / `LOAD x` for what Pondra has built in (httpfs, parquet, json, icu, iceberg, delta, excel…) succeed with a notice, others are refused by name (`0A000`, "extensions come in 1.1"); `window.pondra` listed as a preview in ADR-068 and `surface.json`; the `extension` kind, `xt/` and `ext/` reserved in the kinds rules (AGENTS.md's `pl/` becomes `xt/`) | a DuckDB script with `INSTALL httpfs; LOAD httpfs;` runs unchanged |
| **1.1** | The world with scalar and table functions; `INSTALL` from a file or URL; the lake keeps them; grants and limits; the Rust crate and `pondra extension new/dev/test/build`; the browser's host; the console's `register.*` as API v1 | one `.wasm` built outside the repo runs on all five platforms and in the page; a spread query over three nodes equals one node; a runaway call fails alone; an extension built for 1.1 loads on 1.2 and a 1.2 one that needs nothing new loads on 1.1; the binary's size before and after |
| **1.2** | The signed index, `UPDATE EXTENSIONS`, aggregates and formats; `pondra.toml` pins, apply, status, capture, branches; SQL, console and Python parts; app tabs and the bridge; the CI canary. Deep hooks, first set: table storage (`USING`, `ATTACH … TYPE`), table-in table-out operators, types and casts, statements, operators in expressions, names that resolve, settings; `EXPLAIN (FORMAT substrait)` | a tampered package refused; a format read by `read_x` and `FROM 'f.x'`; a project's extensions applied to test and prod alike; an extension's table and operator give one node's answer on three nodes and in the page; a clashing word refused at install |
| **1.3** | The seams of §4a (sign-in, secrets, AI models, catalogs, sources over `http`, languages, alerts); whole tools (§10): the runtime command, `/apps/<name>/` with one sign-in, sync by capture and apply; Grafana, then Superset and dbt, then Metabase with its limits; our ETL and BI moved onto the public API; expression rewrites and indexes that prune | our BI turned off and Superset's tab in its place, with no change to the core; a Grafana dashboard edited in its screen, captured, applied to prod |
| **Later** | Whole-plan rules, once 95% of the test corpus round-trips through Substrait with the same rows (TPC-H already does: 22 of 22) | the gate's own number, in the release notes |

## Consequences

- Authors build once. Users install in one line. Maintainers keep one small world, the console API
  and what ADR-068 already keeps, all checked by `surface.json`.
- The server build grows by about 10.3 MB (Wasmtime and Cranelift); the browser build grows by
  nothing, since the page runs extensions through the browser's own WebAssembly. A node that never
  installs one spends no memory on it. A whole tool costs its own container's memory, shown before it
  is installed, and the binary only the passage at `/apps/` and the runtime command.
- A hot built-in stays built in: a tight loop in the sandbox ran at a seventh of vectorized native
  code, right for functions that do real work per row (parsing, geometry, an API), not for replacing
  Parquet's reader.
