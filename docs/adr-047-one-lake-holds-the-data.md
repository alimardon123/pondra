# ADR-047: Environments: one lake holds the data, everyone else works on a branch of it

**Date:** 2026-10-03 · **Status:** accepted (Alimardon chose "As designed", 2026-10-03); phase 1's branches built in PR A (`src/branch.rs`) · **Builds on:** ADR-001 to ADR-003
(files in place, never copied; serverless: your compute, the leader's commit), ADR-032 (`pondra serve
--lakes`: a folder or bucket prefix of lakes as databases, each started on use and stopped when idle),
ADR-033 (the workspace and `CALL run`), ADR-035 §9 (Git and CI/CD, sketched; this ADR designs it),
ADR-036 (flows and expectations), ADR-043 (`CLONE`, `AT`, `UNDROP`, `RESTORE`), ADR-044 (`DECLARE
PARAMETER`), ADR-045 (tasks), ADR-046 (proposed: `vend`, files a principal may read)

## Context

Alimardon, 2026-10-03: how does a company work with Pondra day to day? Developers build and test new
tables, functions, procedures and flows, locally or in the cloud; business users work with the data;
there are environments (dev, test, prod) and five developers in dev. Do we branch data, how does work
go from dev to test to prod, how are the syncs from source systems kept, does each developer get a
workspace of their own, and how is work merged? It should be as easy as MotherDuck's laptop and one
command, simple to use and develop with, and powerful. And (06:56): a whole lake defined by files (SQL
or Python, maybe drawn as a graph), Git for the Workspace, and every object made in SQL usable by every
other tool (ETL, BI, plugins) as one ecosystem.

**What exists (0.32):** zero-copy `CREATE TABLE c CLONE t` (one table, inside one lake), time travel,
`UNDROP`, `RESTORE`; databases as lakes under one folder or bucket prefix, idle ones costing nothing;
users, roles and grants; secrets sealed per lake; tasks that suspend and resume; flows with
expectations; workspace files with every save kept, runs that record the version they ran; parameters
declared in files; the console's **Script as** for every object. Nothing branches a whole database,
nothing turns a folder of files into objects, and nothing records what was deployed where.

**What the others do:** Snowflake clones a whole database without copying (`CREATE DATABASE dev CLONE
prod`) and leaves deployment to outside tools (schemachange, its DCM projects). Databricks deploys
asset bundles (YAML, `databricks bundle deploy -t prod`) from Git folders; its data has no branches.
dbt and SQLMesh build each environment's tables from SELECT files; SQLMesh's virtual environments
reuse tables whose definition didn't change. lakeFS and Nessie branch and *merge* data like Git.
MotherDuck clones a database in one statement and runs the same SQL on a laptop or in its cloud.
Terraform shows a plan, then applies it.

## Decision (proposed)

Three words, each one thing:

- **A project** is a folder of files (in Git) that says what every object is.
- **An environment** is a database the project is deployed to: `prod`, `test`, whatever a team names.
- **A branch** is a database that starts as a zero-copy copy of another, at one moment, for one
  person, one pull request or one release. It costs only what it writes.

Only production holds data of its own. Code is merged in Git and deployed; data is never merged.

### 1. A project: the whole lake as files

```
sales/                          a Git repository, and the lake's workspace (files/) one to one
  pondra.toml                   where the environments are; their connections, values, secrets' names
  objects/                      what every object is, in any layout:
    sources/orders.sql            CREATE MATERIALIZED VIEW raw.orders AS SELECT * FROM events.orders;
    silver.sql                    CREATE TABLE …, CREATE MATERIALIZED VIEW … (a flow, ADR-036)
    scores.py                     db.view("gold.scores", frame, materialized=True)
    margin.sql                    CREATE FUNCTION sales.margin(…) …
    nightly.sql                   CREATE TASK nightly SCHEDULE '0 2 * * *' AS CALL run('jobs/load.sql');
    access.sql                    CREATE ROLE analyst; GRANT SELECT ON SCHEMA gold TO analyst;
  migrations/2026-10-03-backfill-channel.sql    runs once in each environment
  tests/revenue_positive.sql    a query: any row back is a failure
  tests/fixtures/orders.csv     rows for a branch made with no data
  jobs/load.sql                 everything else is a workspace file, run with CALL run(…)
  notebooks/explore.ipynb
```

- **The folder is the workspace.** `pondra workspace pull | push` (§7) keep it and the lake's `files/` alike,
  file for file; `objects/`, `migrations/` and `tests/` are what a deploy reads, and the rest are files
  runs use.
- **Files hold ordinary statements**, the ones a console runs: any layout, several objects to a file,
  objects known by name, not by path. A file copied into the console runs as it is.
- **A Python file is SQL too.** It runs in a worker with a `db` that records instead of doing:
  `db.view(…)`, `@db.function`, `@db.procedure`, `db.task(…)` each become the statement they stand
  for, and a frame is already one SQL statement (ADR-023). So the plan sees only SQL.
- **Every object is a file and every file an object.** `pondra export` writes a database's objects
  as such files (the console's Script as, on the node), so a lake built by hand in the console becomes
  a project in one command, and J5's export without data is this. A kind of file (`.sql`, `.py`, later
  YAML or an ETL canvas's file) is a registry entry that turns a file into statements (principle 9), so
  the three ways of the ETL tool, dashboards and connectors are new kinds, not new machinery.

```toml
# pondra.toml
[project]
name = "sales"
server = "https://pondra.acme.com"   # where the databases are; a branch is any other database there

[env.prod]
protected = true                     # its project's objects change only by a deploy (§4)
attach.events = { type = "kafka", url = "kafka://kafka.acme.internal:9092" }
values = { min_order = 10 }          # DECLARE PARAMETER values (ADR-044)

[env.test]
clone = "prod"                       # made again as a branch of prod before each deploy
attach.events = { type = "kafka", url = "kafka://kafka-test.acme.internal:9092" }

[secrets]                            # names only; each value from the deploying machine's environment
crm_token = "env:CRM_TOKEN"          # (GitHub's per-environment secrets give each env its own)
```

The SQL names `events.orders` in every environment; only where `events` points changes. Secrets'
values never reach Git or an export.

### 2. Environments are databases

- **Production** runs its own cluster: `pondra serve --lake s3://acme-lake/prod` on as many machines as
  it needs. **Everything else** (test, every branch) is served by one small machine: `pondra serve
  --lakes s3://acme-lake/`, each database started when used and stopped when idle (ADR-032). Five
  developers' branches cost nothing at night.
- **A protected database** (`ALTER DATABASE prod SET (protected = true)`) changes its project's objects
  only through a deploy, by a role with the new `DEPLOY` privilege (CI's token). Objects people make
  themselves (a business user's schema) are theirs as before; a deploy never touches what it didn't make.
- **The project says what each role may do; each environment says who is in it.** `GRANT … TO analyst`
  is in the project; `GRANT analyst TO ann` is prod's admin's.

### 3. Branches: a whole database, zero copy

```sql
CREATE DATABASE ali_orders CLONE prod;                    -- every schema, as prod is now
CREATE DATABASE ali_orders CLONE prod WITH (schemas = (raw, sales));
CREATE DATABASE unit CLONE prod WITH NO DATA;              -- definitions only, for fixtures
ALTER DATABASE ali_orders REFRESH raw.orders;              -- that table and what follows it, as prod has them now
DROP DATABASE ali_orders;                                  -- only what the branch wrote goes
SELECT * FROM pondra.databases;                            -- base, branched at, bytes written, bytes held (SHOW DATABASES reads it)
SELECT * FROM pondra.diff('prod.gold.revenue', 'ali_orders.gold.revenue');   -- rows added, removed, changed
```

- **One snapshot of the base, no work asked of it.** The branch reads the base's catalog at one
  version, takes its entries (tables, views, flows, functions, procedures, tasks, users, roles, grants,
  workspace files), lists the base's data files where they are, and copies only the rows still in the
  base's log (seconds of data) into its own first commit, with their row ids and versions.
- **Its writes are its own.** New files go under the branch's prefix; it never deletes a file outside
  it. Its views follow its writes as any lake's do, so a developer inserts test events into `raw.orders`
  and watches the flow answer.
- **The base keeps what branches read.** A branch leaves a pin in its base's catalog (through the base's
  leader, as any write to another lake goes: invariant 20); while it lives, the base's clean-up keeps
  every file that was live at a pin (replaced files, dropped tables, purged old rows). A branch of a
  branch carries its base's pins on. The base checks now and then that each pin's lake is still there.
  Branches made by `pondra branch` expire after 14 idle days unless renewed: their code is in Git, and
  their data is the base's plus test rows.
- **Nothing in a branch reaches the outside on its own.** Its tasks and feeds start suspended, and it
  gets no secret from its base: a branch takes its environment's (`[env.dev]`'s by default), so a
  developer's branch of prod can never mail customers or write a partner's bucket with prod's keys.
  `ALTER TASK … RESUME` starts one on purpose.
- **A clone shows nobody more than they could read.** It copies only tables its maker may read whole
  (a table they see only some columns or rows of is left out, and named), keeps the base's grants, and
  makes its maker its owner. Copying is `CLONE`, a new privilege on a database.
- **Comparing is cheap.** Rows a branch shares with its base have the same `_row_id` and `_version`
  (ADR-020), so `pondra.diff` reads only the rows either side changed since the branch was made.

### 4. Plan and deploy: the project made true in a database

```
$ pondra plan --env prod
prod, deploy 41 (git 3f2a9c1) → git 8e01d77
  + function   sales.margin
  ~ view       sales.daily              replaced
  ~ table      sales.orders             ADD COLUMN channel TEXT
  ↻ flow       gold.revenue             built beside the old one from 1.2 B rows, then swapped in
  ↻ follows    gold.revenue_by_region   built with it (reads gold.revenue)
  ▶ migration  2026-10-03-backfill-channel.sql   runs once
  ! table      sales.legacy             no longer in the project: kept (--prune drops it, UNDROP for a day)
  ✓ tests      12 queries, 3 expectations
```

- **Declared, then compared.** Every object a deploy made remembers its project, the commit and a
  fingerprint: its parsed statement (comments, spacing and case don't count) with the values bound
  into it. Same fingerprint: nothing. A view, function, macro, procedure, task or grant that changed is
  replaced. A table is compared column by column: a column added or widened becomes `ALTER TABLE`
  (as `iceberg::schema_sql` already does for other engines); a column gone, renamed or narrowed is
  refused with the migration to write, since a declaration can't tell a rename from a drop and an add,
  and nothing loses data unasked. A flow whose query changed is built
  beside the old one from the rows already there, with everything that follows it, and swapped in, in
  one commit (`ALTER … SWAP WITH …`, which the SQL review's statement registry gives every kind), so readers never see a half-built view.
- **Migrations are the one-off steps** a declaration can't say: a rename, a backfill, a fix to rows.
  Each runs once per environment, in name order, before the comparison, recorded with the deploy.
  A column renamed by a migration makes the table match its declaration again.
- **Tests** run after a deploy (and alone with `pondra test`): every query in `tests/` must return no
  rows, and the flows' expectations hold. A failed test in test stops the release before prod.
- **One deploy at a time, from the plan that was shown.** A deploy takes the lake's lock and refuses
  a plan made against an older version of the database (someone deployed or changed it since: plan
  again). Each statement runs under its part of the deploy's job, so a retried deploy writes once
  (ADR-033's jobs). It is resumable, not one transaction; flows swap whole.
- **Every deploy is a row** of `pondra.deploys` (commit, who, the plan, each step, tests, how long),
  and its files are kept under `files/.deploys/<n>/`, so prod always says which code it runs.
  `pondra plan` also shows **drift**: a project's object changed outside a deploy.
- **Going back:** deploying the previous commit puts the code back; data has `RESTORE` and `AT`
  (ADR-043). A deploy never rolls data back on its own: prod kept taking rows meanwhile.
- **Every door:** `CALL plan('files/sales', env => 'prod')` and `CALL deploy(…)` in SQL (a project kept
  in the workspace), `pondra plan | deploy` on the command line (the folder is sent to the node), the
  console's **Deploy…** showing the plan first, `db.deploy("./sales", env="prod")` in Python and
  JavaScript. (ADR-035 §9's `apply` is called `deploy` here.)

### 5. Sources and syncs: run once, in production

- A **source** is an object that fills a table from outside: a feed from a Kafka topic, a task that
  copies files or calls an API, an attached Iceberg or Delta catalog, Debezium through the Kafka port.
  It is declared once in the project, with its connection named per environment (§1).
- **Production runs the syncs.** Test and branches get the source tables as zero-copy copies, frozen at
  their moment, so tests repeat; `ALTER DATABASE … REFRESH raw.orders` brings a table and everything
  that follows it up to production's present, still copying nothing.
- **Testing a sync itself:** point its connection at a test topic or bucket (`[env.test]`) and resume
  its task or feed in that database. A feed keeps its own offsets in its own catalog (invariant 103),
  so a branch can even follow the real topic without disturbing production.
- **Where developers may not see production rows**, their branches' base is a `dev` database a task in
  prod fills with masked or sampled rows; everything else stays the same.

### 6. A day with Pondra

**A developer**, the first time and then every feature:

```bash
pip install pondra                          # or the one-line installer
pondra login https://pondra.acme.com        # once: a token kept in ~/.pondra
git clone git@github.com:acme/sales && cd sales
git switch -c orders-by-channel
pondra branch                               # a database orders_by_channel: prod as it is now, zero copy
pondra dev                                  # the console on localhost:8080 on that branch;
                                            #   each saved file planned and deployed, tests on each save
git commit -am "orders by channel" && git push
```

- The laptop path: `pondra dev --local` keeps a lake in `.pondra/` with the fixtures, or a sample; once
  ADR-046's `vend` exists, a laptop branch reads production's files straight from the bucket with the
  developer's token and the SSD cache, its own writes on the laptop: MotherDuck's model, no copy.
- **Five developers** never share a branch. Each has a Git branch and a database of the same name; they
  merge in Git, where code conflicts belong. Nobody deploys to a shared dev by hand.
- **A pull request:** CI makes `pr_123` from prod, deploys the branch, runs the tests, and writes the
  plan and `pondra diff --against prod` (objects and rows that change) on the pull request. Merged or
  closed: the branch is dropped.

```yaml
- run: pondra branch pr-${{ github.event.number }} --from prod
- run: pondra deploy --env pr-${{ github.event.number }} --test
- run: pondra diff --env pr-${{ github.event.number }} --against prod >> "$GITHUB_STEP_SUMMARY"
```

- **A release:** merging to `main` deploys to `test` (made again from prod, so staging is always
  production plus the new code), runs the tests; an approval in GitHub's `prod` environment deploys
  the same commit to prod.
- **A business user** reads prod in the console, a notebook, Excel or Power BI (the Postgres port);
  makes their own tables in a schema granted to them, which deploys never touch; tries a what-if on all
  of prod in a branch of their own, for free; and hands a useful query to the developers with **Add to
  project** in the console (its statement as a file on a Git branch).
- **An admin** runs the two `pondra serve`s, makes prod protected, gives CI a `DEPLOY` token, and sees
  every branch, its owner and what it holds in `pondra.databases`.

### 7. Git

Git is the project's history and its place for review; the node needs no Git of its own at first.
The command line and CI use the machine's `git`; `pondra workspace pull | push <dir>` (PR #20, `src/sync.rs`; its `.pondra/workspace.json` keeps each file's version and checksum, which a deploy reuses to send only what changed;
ADR-035 §9) syncs a lake's workspace with a folder. Later, the console's Git panel
(branch, commit, pull, push, with a token secret) pairs a Git branch with the database branch of the
same name, so switching one switches the other.

### 8. Many teams, petabytes

- **A branch costs its catalog, not its data.** A table of a million files is a 20 KB entry plus
  sealed manifests that never change (invariant 25), so a branch of a petabyte lake copies kilobytes
  a table. What grows is what the base keeps for it: the files production replaced since the branch
  was made (a petabyte lake that rewrites 1% a day holds 10 TB a day for each long-lived branch). So
  big lakes branch only the schemas a change needs, `REFRESH` moves the pin forward, idle branches
  expire, and `pondra.databases` shows what each holds.
- **A lake per domain.** A lake has one sequencer (known limit 6). It never touches data, but every
  commit of that lake goes through it, and one lake is one blast radius. An enterprise splits by
  department: `core`, `finance` and `marketing` are lakes, each with its own leader, nodes, project,
  Git repository and deploys, in one bucket or several. They read each other live with `ATTACH`, and
  a write into another domain's lake is recorded by that lake's leader (invariant 20): a data mesh
  with no central service. Not there: one transaction across two lakes.
- **One owner per object.** Every object a deploy made names its project, and a deploy refuses to
  change an object another project owns. Two teams can share one lake, each with its own schemas
  (`[project] schemas = ["finance"]`), repository and pipeline, and never undo each other's work.
- **Contracts between teams.** A lake that reads another registers, at each deploy, what it reads
  there (objects and columns), as a branch registers its pin. The owner's plan then shows the change
  that would break a consumer (`drops core.orders.channel, read by marketing.campaign_roi`) and
  refuses it until the consumer stops reading it, or the deploy names it with `--break`.
- **Compute apart, data shared.** All of a lake's nodes are one cluster, and a query spreads across
  all of them. Node groups (`pondra serve --group bi`) keep work apart: a query spreads only within
  the group it arrived at, and each group grows, shrinks and stops on its own. With quotas per user and
  role (ADR-035) that is Snowflake's warehouses, with no warehouse service. A heavy notebook in
  marketing's group never slows finance's dashboards, and ingest keeps nodes of its own.
- **Many teams on the same data at once:** a developer branches every lake their change touches
  (`core` and `marketing`, both zero copy), each repository deploys to its own lake, and the contract
  check catches the cross-team break before production.
- **Not proven yet:** petabyte-sized metadata is measured (a million files a table: a 20 KB entry, a
  restart, three nodes), not a petabyte end to end; the cluster bench has run SF10 on GitHub's runners,
  and SF100 on dedicated machines is round 34's.

## Invariants (proposed)

1. A branch never deletes a file outside its own prefix, and its base keeps every file live at any
   pin until that pin goes.
2. A file a branch shares is named with its lake wherever it is read or cached.
3. A branch's tasks and feeds start suspended, and its secrets never come from its base.
4. A clone copies only tables its maker may read whole.
5. A deploy applies the plan it was made from, against that version of the database, each step under
   its job's part; nothing but a deploy changes a protected database's project objects.
6. A fingerprint is the parsed statement plus the values bound into it; equal fingerprints change nothing.
7. A deploy changes only objects its own project owns.
8. A query spreads only within the node group it arrived at.

## What it costs

No new service. A branch: its catalog, the log rows it copied, what it writes, and the base's replaced
files kept while it lives (shown per branch). Production pays nothing for branches but that storage:
no compute, no reads (a branch reads the bucket with its own nodes). No bucket listing, no key written
more than once a second (principle 7). Queries, the cluster bench and the read path are untouched.

## Phases

1. **Round 33** (with observability, for 0.33.0): §3 (`CLONE` of a database, pins, `REFRESH`,
   `pondra.databases`, `pondra.diff`); §4 (plan, deploy, export, migrations, tests, `pondra.deploys`,
   protected, swap, one owner per object) for `.sql` and `.py` files; `pondra.toml`; the command line (`init`, `login`,
   `branch`, `plan`, `deploy`, `test`, `export`, `diff`); `tools/environments_check.py`. *Done when:* a
   branch of a lake with every kind of table answers as its base did while the base merges,
   compacts, purges and drops; a deploy run twice changes nothing the second time; an export deployed
   into an empty lake answers alike.
2. **Next:** the console (the database pill shows a branch and its base, **Branch…**, **Deploy…** with
   the plan, the project drawn as a graph of what reads what), `pondra dev`'s watcher, the Git panel
   with the console thread, laptop branches over `vend` (ADR-046), `CLONE … AT` from catalog
   checkpoints taken at each deploy, J5's export with data, contracts between lakes, node groups (in
   `spmd.rs` and `guard.rs`: the main thread's).
3. **Later:** YAML and the ETL canvas as kinds of file, dashboards as objects deployed like the rest.

## Rejected

- **Branches as schemas inside production's lake:** less to build, but every developer's writes, views
  and tasks would go through production's leader and catalog, and a branch's mistake would be prod's.
- **Merging data like Git (lakeFS, Nessie):** two branches' backfills of one table don't combine;
  code merges, and each environment builds its own rows. A real fix to rows is a migration.
- **One lake for the whole enterprise:** one sequencer for every team's commits, and one blast radius.
- **A copy of the data per environment:** slow and costly at terabytes, and stale the moment it's made.
- **Migrations only (Flyway):** no file says what an object *is*; reviewers read history.
- **dbt's SELECT-only files and Jinja templates:** a second way to write `CREATE`, and SQL that doesn't
  run as written; parameters and per-environment connections cover what templates do.
