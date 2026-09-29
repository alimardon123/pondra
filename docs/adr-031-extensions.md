# ADR-031: Extensions: `INSTALL` and `LOAD`, as WebAssembly components

**Date:** 2026-09-29 · **Status:** proposed (the owner's request; the round is the owner's choice) · **Builds on:** ADR-013 (functions over Arrow Flight), ADR-026 (connectors in the binary), ADR-027 (functions and procedures in SQL and Python)

## Context

The owner, 2026-09-29: "What happened to our extension framework? Is it done or on way? How is
the architecture of it?" Asked what was meant, the owner chose DuckDB-style `INSTALL` and `LOAD`.
Third parties could then add functions, formats or connectors to Pondra without rebuilding it.

Nothing like it has been designed yet. What Pondra has today for extending it:

| Way | What it adds | Where it runs | Limits |
|---|---|---|---|
| `CREATE FUNCTION … LANGUAGE sql` (ADR-027) | functions and table functions | in the query | SQL only |
| `CREATE FUNCTION … LANGUAGE python`, procedures (ADR-027) | anything Python can do | warm Python workers beside each node | needs Python and its packages on every node; not sandboxed, so admins only |
| Functions over Arrow Flight (ADR-013) | a function served by your own server (a GPU, a model) | that server | a server to run |
| Connectors (ADR-026) | files, Delta, Iceberg, Kafka, S3, GCS, Azure | compiled into the binary | only the ones Pondra ships |

DuckDB's extensions are native libraries, one build per platform, signed and fetched from a
repository. They are loaded into the process with no sandbox, so a faulty extension can crash
DuckDB or read anything it can.

## Decision (proposed)

**An extension is a WebAssembly component**: one `.wasm` file for every platform, loaded at
runtime and run in a sandbox.

```sql
INSTALL h3;                                   -- from Pondra's extension index
INSTALL geo FROM 'https://example.com/geo.wasm';
INSTALL mine FROM 'extensions/mine.wasm';     -- a file (the node's owner, or an admin with a secret for its URL)
LOAD h3;                                      -- its functions exist for every query in this lake
SELECT h3_cell(lat, lon, 9) FROM trips;
SHOW EXTENSIONS;  UPDATE EXTENSIONS;  UNINSTALL h3;
```

### 1. What an extension can add

- **Phase 1:** scalar functions and table functions. They take and give Arrow batches, and
  spread with their queries like every function.
- **Phase 2:** aggregate functions, and file formats to read (`read_<format>(…)` and `FROM
  'x.<ext>'`).
- **Phase 3:** connectors that need the network (an API as a table, a database's wire protocol),
  through WASI's sockets, only to the hosts the admin allows.

### 2. The contract: one WIT world, versioned

`pondra:extension@1` is the interface every extension implements:

- `describe()`: its name, version, the functions it exports with their argument and result
  types, and the capabilities it asks for;
- `call(function, batch)`: an Arrow IPC batch in, an Arrow IPC batch out;
- `open-table(function, args)` and `next(handle)`: a table function's batches, one at a time.

The host offers only what the admin granted at `LOAD`:

- `log`;
- `now`;
- `secret(name)` (a `CREATE SECRET`, never shown);
- `http` to named hosts;
- read access to the lake's `files/`.

A node supports version N and N-1 of the world. An extension that asks for more is refused by
name.

### 3. Where extensions live and run

- **In the lake:** `INSTALL` stores the component under `ext/<name>/<version>.wasm`, with a
  catalog entry for it (`xt/`). Every node of the lake has it, as it has the lake's functions,
  and a lake copied elsewhere keeps its extensions.
- **`LOAD` turns it on for the lake** (an admin), with its grants:
  `LOAD h3 WITH (network = 'api.example.com', secrets = 'maps')`. Its functions then exist in
  every session, on every node, in SQL and in every client.
- **Warm instances:** each node compiles an extension once (Wasmtime, with the compiled code cached
  on disk) and keeps a small pool of warm instances, as it keeps Python workers. An idle pool is
  dropped after a minute.
- **Limits per call:** memory (`PONDRA_EXTENSION_MB`, 256) and time, through Wasmtime's epoch
  interruption. A runaway call fails its query with why. The node and the other queries go on.

### 4. Trust

- **Sandboxed by default:** no files, no network and no secrets unless granted at `LOAD`. A
  sandboxed extension can't read the lake or the machine, so installing one is safer than a
  native library, or a Python procedure.
- **Signatures:**
  - Pondra's index signs what it lists (ed25519).
  - An unsigned component, from a URL or a file, needs an admin and
    `allow_unsigned_extensions`, as DuckDB's does.
  - `SHOW EXTENSIONS` says which is which.

### 5. Writing one

- **Templates in the repo:**
  - Rust (`cargo component`): the fastest;
  - Python (componentize-py): CPython inside the component, with the standard library and pure
    Python packages.
  
  Anything else that targets the component model works too.
- **Testing it:** `pondra extension test ./my.wasm` runs its declared examples against a
  temporary lake.
- **The index:** a JSON file published with the docs site, starting with Pondra's own examples
  (for instance H3 cells, a URL parser, a spreadsheet reader). Others are added by pull request,
  as DuckDB's community extensions are.

### 6. Cost, and how it is kept honest

- **Speed:** data crosses the sandbox as Arrow IPC, a copy in and a copy out per batch, and
  compiled WebAssembly runs somewhat slower than native code. That suits functions that do real
  work per row (parsing, geometry, calling an API). A hot built-in stays built in, and heavy
  compute stays on Flight functions (ADR-013).
- **Size:** Wasmtime with its compiler adds to the binary.
  - It will be measured before it goes in. If it costs more than the owner accepts, it becomes a
    Cargo feature, in the full build but not the lite one (ADR-018's A5).
  - A node that never loads an extension pays no memory for it.
- **Precedent:** InfluxData's `datafusion-udf-wasm` runs DataFusion UDFs as components, with Rust
  and Python guests, on the same idea.

## Rejected

- **Native libraries** (DuckDB's way):
  - five builds per extension and per Pondra version, since Rust has no stable ABI;
  - no sandbox, so one bad extension takes the node down or reads the bucket's credentials.
- **Only Python functions:** they already exist (ADR-027), but they need Python on every node,
  aren't sandboxed, and can't add formats or connectors.
- **A plugin process per extension** (Spark's and Flink's connectors): rejected by ADR-026. It is
  one more thing to run and supervise.
- **Extensions per session, as DuckDB's `LOAD`:** a lake is shared by every node and client, and
  a function must mean the same on every node that runs a slice of a query.

## Phases and proof

| Phase | What | Proof |
|---|---|---|
| 1 | `INSTALL` from a file or URL, `LOAD`, scalar and table functions, grants, limits, Rust and Python templates | An extension built outside the repo loads on all five platforms (CI). A spread query over three nodes equals one node. A runaway call fails alone. A call without its grant is refused by name. The binary's size is measured before and after |
| 2 | The signed index, `UPDATE EXTENSIONS`, aggregates and file formats | A format read by `read_<x>` and `FROM 'f.<x>'`; a tampered component refused |
| 3 | Connectors through sockets, to granted hosts | An API as a table, spread; a host not granted refused |

## Open

- **Its round.** The owner decides. Anyone's compute (ADR-029) is set for rounds 27 and 28, and
  security for round 29.
- **Whether `LOAD` should be implicit** when a known function is first used (DuckDB's autoload),
  or always explicit. My inclination is explicit: the grants are part of `LOAD`.
