# ADR-032: Files by name, one serve command, a console to build on (round 26, continued)

**Date:** 2026-09-29 · **Status:** §1–§8 accepted and built; §9 (the server's catalog) proposed · **Follows:** ADR-030 (the console, the server and the docs), ADR-031 (extensions, proposed) · **Leads to:** ADR-033 (a workspace: files, runs and parameters, proposed)

## Context

Round 26 (ADR-030) was delivered as a bundle. Before it was tagged, the owner looked at it and asked
for more, in this order:

- **Three limits the round had left:** `CREATE EXTERNAL TABLE` refused (about 380 sqllogictest
  records), `to_timestamp`'s answer differing from DataFusion's (about 200), and the console's
  Python cells not sharing variables.
- **A folder of lakes in a bucket** for `pondra server`, which served local folders only.
- **The logo:** "make it reusable from one place now… later, if we can find a better logo, we can
  change it anywhere". Direction E (depth rings, teal) for now.
- **`pondra serve` and `pondra server` look too similar:** "it might confuse or make people
  mistake". And later: "does it not confront with security or specific setting setups later?"
- **The console:** folders shown, a table's icon that looks like a table, its columns, no
  misleading numbers, a details or diagram panel on the right, "learn from the DuckDB UI". Then:
  "much more polished… sleek modern UI… good features of Jupyter notebooks if it is not too heavy…
  performant, simple, efficient and lightweight". Then: "make sure the UI of console is extendable
  too… just like MotherDuck… an enterprise platform on top of this product… extend it a little bit
  here and there… users to be familiar with this UI".
- **Stability:** "everything backward compatible and stable from the version where we feel our
  product is production ready. But not now."
- **The priorities, for everything:** "performance, simplicity, ease of use, ergonomic, beautiful,
  scalability, powerful, functional and versatility".

## Decision

### 1. `CREATE EXTERNAL TABLE` is a view of files, by name

DataFusion's statement names files. In Pondra it makes a stored view: `SELECT … FROM
read_csv('…', …)` (or `read_parquet`, `read_json`) under that name, flagged `external` in the
catalog (`StoredView.external`, `ext.rs`). Nothing is copied, and nothing new is stored beside
what a view already stores.

- **Columns** as DataFusion reads them: CSV's declared columns by position, Parquet's and JSON's
  by name. A folder's partition keys (`PARTITIONED BY`) are declared (`hive_types`), so a folder
  with no files yet is an empty table with the right columns.
- **`INSERT` into a view of a folder** writes a new file there: `COPY … TO folder/ (FORMAT …,
  APPEND true, HEADER, DELIMITER, PARTITION_BY)`, from a node and from `pondra sql`. Into a view of
  one file, or any other view, it is refused (it used to make a table silently).
- **`DROP TABLE`** drops the name and keeps the files.
- **Options** are DataFusion's (`format.has_header`, `format.delimiter`, …); options only a
  writer reads are ignored; any other is refused by name. `TEMPORARY`, `ARROW`/`AVRO`,
  compressed CSV and JSON are refused with what to do instead.
- The internal tables a view of files reads (`ext:…`) aren't listed in `information_schema`.
- One statement at a time: a `CREATE EXTERNAL TABLE` followed by more statements is refused
  rather than the rest dropped.

### 2. `to_timestamp` answers a `TIMESTAMP`, as DataFusion does

It answers a wall-clock time with no zone, as DataFusion, Spark and DuckDB's `strptime` do; text that
names a zone is converted to UTC. Pondra re-makes DataFusion's function with a configuration without
a zone (`optimize::register_zoned`, `naive()`), so `to_timestamp(column)` works whatever the session's
zone.

### 3. A page's Python cells share their variables

A session (a console page, a psql connection, a client sending `x-pondra-session`) gets one Python
worker, kept for it (`python::KERNELS`), with one namespace, as a notebook's kernel:

- Only a `DO` block (depth 1) with a session goes to it; functions and procedures keep the pool.
- One persistent connection inside it, its token refreshed per cell. `_` is the last answer; a
  cell's single value comes back in a column `value`.
- **Figures:** a matplotlib or seaborn figure (the cell's value, or pyplot's open figures), or a
  Pillow image, comes back as PNG (`images`), drawn by the console.
- It ends with the session, after `PONDRA_SESSION_IDLE_SECS` (3600) idle, or past
  `PONDRA_WORKER_MB`. `GET /sessions/{id}/python` lists its variables (name, type, size, a look
  at it) and `DELETE` restarts it; both need an admin, as `DO` does.

### 4. One serve command, which says what it serves

`pondra server` is gone (it was never in a release). `pondra serve` serves a lake or a folder of lakes:

```bash
pondra serve lake              # a lake: this one (new or empty: made)
pondra serve data              # a folder whose subfolders are lakes: each a database
pondra serve --lake /srv/lake  # exactly this lake: nothing is guessed (services, scripts)
pondra serve --lakes /srv/data # exactly this folder of lakes, local or s3://bucket/prefix
```

- **Guessing is only for what is unambiguous.** A folder holding other things is never made a
  lake; the current folder is made one only when named (`pondra serve .`).
- **The explicit forms refuse the other:** `--lake` on a folder of lakes, or `--lakes` on a lake,
  stops with the command that was meant.
- **Options that speak for one lake** (`--flight`, `--kafka`, `--attach`, `--advertise`,
  `--attach-found`) are refused with `--lakes`, naming the flag. Every other option goes to each
  database's node (`dbserver::Options.node`).
- **The owner's security question.** The explicit forms are what services use, so what a machine
  serves never depends on a folder's contents. Tokens, and later users and grants, apply the
  same whichever form started it.
- **A folder of lakes in a bucket.** Listing (`ddl::lakes_in`: a delimiter listing, then a
  `catalog/` probe each), `CREATE DATABASE` (a new prefix) and `DROP DATABASE` (its objects
  deleted) work on a bucket prefix as on a folder.
- **A database's node isn't stopped while in use.** It stops only when no connection is open, no
  request is in flight (`dbserver::Busy`, held until the answer's last byte has gone),
  `PONDRA_DATABASE_IDLE_SECS` have passed since the last one ended, and it has nothing left to tier.
  Before this, a statement longer than the idle time, or a connection a BI tool kept open, lost its
  node (found by the test on simulated R2, where `CREATE DATABASE` takes 14 s).

The name of the plural is the owner's to change: `--lakes` is built; "LakeHub" and others were
discussed as names for the mode in the docs, not decided.

### 5. The brand, in one place

`brand/mark.svg` (draws in `currentColor`, its dark colour in one media query) and
`brand/colors.css` are the only copies. The console `include_str!`s them into its page, icon and
style sheet. The docs site makes its favicon and header marks from them at build time
(`astro.config.mjs`, generated files ignored by git). `tools/brand_check.py` fails on any other copy
of the mark, any other logo or favicon file, or colours out of step. It checks the repository, a
node's page (`--node`) and the built site (`--site`). Changing the logo is one file.

### 6. The console: a core to build on

`src/console/`: `index.html` (the shell), `console.css` and `console.js`, an ES module, about
115 KB together (35 KB compressed), no framework, nothing from any other host. Each file is served
at `/console/…` with a tag from its contents' hash, so the browser asks again and gets `304` until
the binary changes.

**What it does.**

- **The catalog on the left:**
  - schemas, then tables, views, materialized views and views of files, each kind its own icon;
  - columns with a glyph for their type and the SQL type, and a key marked;
  - no bare numbers;
  - the lake's own files by folder;
  - the notebook's outline, and saved notebooks with their versions.
- **A panel on the right, with tabs:**
  - **Details:** a table's rows, key, layout, formats and size; a view's definition; a file's
    size and how SQL reads it.
  - **Profile:** each column's nulls, distinct values, range, and a histogram or its commonest
    values, over the whole table.
  - **An answer's columns,** summarized from the rows it holds.
  - **Variables:** the page's Python names, with Restart.
- **Answers:**
  - a grid that draws only the rows in sight (10,000 kept);
  - sort by a header;
  - Explore;
  - CSV;
  - figures as pictures.
- **Jupyter's working set, and nothing heavier:**
  - keys (with O to hide an output, 0 0 to restart, ? to list them all);
  - run all, run above, run this and below;
  - hide or clear an output;
  - move, add or delete cells;
  - Tab and Ctrl+Space completion: tables, the named tables' columns first, functions, SQL
    words, and Python's variables;
  - a kernel badge;
  - `.ipynb` in and out.

**How it is built to be extended.** Everything the page shows is registered through its API,
`window.pondra` (also the module's export), the built-in parts as an extension's would be:

- `register.section` (the left), `panel` (a tab on the right), `cellKind`, `renderer` (a view of an
  answer: the first that matches draws it), `action` (a button, or an item of the ⋯ menu), `nav` (a
  rail at the far left, shown once something is added to it), `key`, `command`;
- `on('start' | 'run' | 'ran' | 'pick' | 'refresh' | 'changed')`;
- `configure({ fetch, token, headers })`, so a platform puts its own gateway and sign-in in front;
- `api` (`run`, `rows`, `call`) and `ui` (the page's own `h`, `icon`, `line`, `button`, `toast`,
  `menu`, `pick`, `add`, `notebook`, `open`), so an extension looks like the rest of the page.

A node given `PONDRA_CONSOLE_EXTENSIONS` (scripts, separated as `PATH` is) serves them at
`/console/ext/{n}.js` and the page loads them after its own. `examples/console-extension.js`
adds a section (History), a tab (Sample), a view (one number, large) and a menu action. This is
the way the owner described: a platform keeps this console and adds its own places to it, so its
users already know their way around.

### 7. The lake's own files, for every reader

A lake's own files (`files/…`, where notebooks are) can be read as tables by whoever may read the
lake (`ext::own_file`), from any door: `read_csv('<lake>/files/reports/q1.csv')`. `GET /objects`
gives the console, and anyone, each table's and view's kind, key, layout, formats, size and
definition from the catalog, and where the lake's files are.

### 8. Backward compatibility starts at the production-ready release

From the release the owner calls production-ready (1.0), the lake's format, SQL, the HTTP API,
the clients' APIs and the command line stay compatible: a change that breaks one needs a
deprecation release first. Until then, names and formats may still change when that makes the
product better, and each change is written in its ADR and in the release notes.

### 9. Proposed: the server's catalog

The owner, on serving many lakes: an upper-level catalog that "will just store the attached
databases anywhere around the world… maybe extensions", and "for any new databases we don't have
to search and list the directory each time".

- **What it is.** A small SlateDB catalog of the server's own, beside its databases
  (`<folder>/_pondra/`, or wherever `--catalog` names), holding what belongs to all of them:
  - the databases, each by name and location: a subfolder, another bucket or region, a Delta or
    Iceberg catalog;
  - attachments every database sees;
  - secrets;
  - users and roles (Postgres's roles are the server's, not a database's);
  - installed extensions (ADR-031);
  - default settings.
- **What it costs.** Nothing on the query path:
  - the server reads it at start and keeps it in memory;
  - `CREATE`/`DROP DATABASE`, `ATTACH`, `CREATE SECRET … GLOBAL` and the like write it;
  - each database's node reads what it needs when it starts, and picks up changes within a
    second, as attachments are picked up today;
  - listing the databases is a lookup, not a folder listing.
- **One lake behaves the same.** A single lake keeps these in its own catalog, as today, and
  behaves identically served alone or in a folder.
- **Lakes copied in by hand.** The first start adopts the lakes already in the folder, once. An
  explicit rescan (`pondra serve --lakes … --adopt`, or SQL) registers any added later. The normal
  path never scans.
- **Judged by the owner's priorities:**
  - performance: nothing per query, and no listing per connection;
  - simplicity: one more catalog of the same kind, and no new service;
  - scalability: databases anywhere, not only in one folder;
  - versatility: extensions and users in one place.
- **When.** Before round 29 (users, grants, TLS), since users live above the databases. Its own
  ADR before it is built.

## Rejected

- **Arrow to the browser for answers.** Promised earlier in the round, and not built. The
  console's answers stay typed JSON (exact: decimals and big integers as text), with the grid
  drawing only the rows in sight. Parsing 10,000 rows of JSON takes milliseconds, so an Arrow
  decoder in the page would add weight for no gain at this size. The cap on rows is what limits
  big answers, not the format. Worth revisiting if the console streams answers past the cap.
- **A framework, Monaco or CodeMirror.** React plus Monaco is several megabytes; the console loads
  in one request of 115 KB and draws in milliseconds. The editor is a textarea under a highlighted
  copy, with completion added; that covers what cells need.
- **`--dir` and `--databases`**, and `--server` for the plural: the owner found them unergonomic.
  `--lake` and `--lakes` read as what they serve. (`--dir` and `--databases` still work, hidden.)
- **Scanning the folder on each request, for good.** Kept for now (it works, and is fast on
  disk), replaced by §9's catalog.

## Tests

- `harness.py external` covers files by name. `harness.py found` covers `to_timestamp`. `harness.py
  procedures` covers shared cells: other sessions and blocks in none share nothing, and ending a
  session ends them.
- `harness.py server` passes locally, on simulated R2 and on R2. It covers:
  - both forms, and the mix-ups refused;
  - a folder with other things never made a lake;
  - options reaching each database's node;
  - the idle stop;
  - a connection held open past the idle time;
  - a follower joining through the server;
  - a restart.
- `console_check.py` passes 30 checks in Chromium:
  - the tree, details, profile, files and the grid;
  - cells, live answers, keys, notebooks and nbformat;
  - figures, the Variables tab and Restart;
  - completion, the outline, and folding;
  - the extension's four additions;
  - `304`s;
  - the Variables tab being an admin's;
  - no request leaving the node.
- `brand_check.py` checks the repository, `--node` in the build job, and `--site` in the pages job.
- **sqllogictest (D1)** went from 16,090 to 18,462 of 24,783 records (64.9% to 74.5%), and no
  file lost one. The gain has two sources: this round's two changes, and the test data
  DataFusion's files read (its `core/tests/data`, `testing` and `parquet-testing`), now in place.
  The most gained:
  - `timestamps` (561 to 764 of 831);
  - `sort_pushdown` (124 to 326);
  - `aggregate` (1,104 to 1,257);
  - `push_down_filter_parquet` (2 to 133);
  - `window` (292 to 401);
  - `parquet` (6 to 69);
  - `insert_to_external` (12 to 66).

  See `logs/round26/slt-1-node.json` and `local-slt.txt`.
