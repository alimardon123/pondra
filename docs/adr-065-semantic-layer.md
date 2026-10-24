# ADR-065: The semantic layer: finding everything, key facts on views, measures in views

**Date:** 2026-10-11 · **Status:** accepted (Alimardon approved E13's three phases 2026-10-03, and
`finding-everything.md`'s names 2026-10-10; the SQL review reserved `AS MEASURE`, `MEASURE`, `AGG`,
`AGGREGATE`, `pondra.find`, `pondra.parts`) · **Builds on:** ADR-049 (one registry), ADR-057
(constraints), ADR-046 (shares), ADR-047 (branches) · **Thread:** Semantic layer and AI (lane 5)

## Context

A business keeps its numbers in one place only if every tool gets the same answer for "revenue" and
"active customers". Views can't do it: a view fixes its grain, so grouping a view of revenue per
customer by region averages ratios. Agents writing SQL over our tables guess the joins. And nothing
lists what is *inside* objects (columns, keys, links, parameters), so nothing can be searched.

Databricks puts metric views in YAML and refuses `SELECT *` and `sum(m)`, so BI tools need its own
connector. Snowflake's semantic views are a new kind of object with its own DDL. Measures in SQL
(Hyde and Fremlin, SIGMOD 2024) put them in a view's SELECT. We take the last: one word, no new kind.

## Decision

### 1. One listing of parts, one search (`pondra.parts`, `pondra.find`)

- `pondra.parts`: one row per part inside an object the caller may see: `kind, lake, schema, object,
  object_kind, name, type, comment, definition`. Kinds are `PARTS`' names: `column`, `key`, `link`,
  `check`, `parameter`, `label`, `expectation`, and `measure` (§3). Parts come from the objects
  `objects::list` gave the caller, so rights are checked once: a user granted some columns of a table
  sees only those columns, and none of its other parts (a key or a check names columns).
- `pondra.find('words' [, kind => 'k'])` searches objects, parts and concepts (`pondra.kinds`, by
  `about` and `also`: "foreign key" finds `link`, "metric" finds `measure`). Every word must be in
  one field. Exact names first, then by the first field that holds the words (name, comment,
  definition; a concept's name, also, about), then shorter names. Columns `rank, is, kind, lake,
  schema, object, name, about, matched`.
- Both are text where SQL comes in (`find::functions_in`, as `pondra.parameters` is), so they work
  at every door: HTTP, Postgres, Flight, the shell, frames. MCP's `find` tool is the same query, and
  `db.find(text, kind=…)` in Python and JavaScript. Neither is ever a remembered answer.
- A kind added to the registry is found as soon as its lister yields it. E13's `list_metrics` and
  E14's listings are not added: they are `find(…, kind => 'measure')` and filters of `pondra.parts`.

### 2. Key facts on views (L5-2)

A view's PRIMARY KEY, UNIQUE and FOREIGN KEY are facts about its rows, said NOT ENFORCED: nothing
checks them (a view holds no rows to check), as Oracle's view constraints (`RELY DISABLE NOVALIDATE`)
and a table's NOT ENFORCED keys (ADR-057). They are written where a view is made, in its list, as a
materialized view's expectations are, and added or dropped later:

```sql
CREATE VIEW sales.customers_v (
  PRIMARY KEY (id) NOT ENFORCED,
  CONSTRAINT customers_v_region_fkey FOREIGN KEY (region) REFERENCES sales.regions (code) NOT ENFORCED
) AS SELECT id, name, region FROM sales.customers;

ALTER VIEW sales.customers_v ADD CONSTRAINT customers_v_name_key UNIQUE (name) NOT ENFORCED;
ALTER VIEW sales.customers_v DROP CONSTRAINT [IF EXISTS] customers_v_name_key;
ALTER MATERIALIZED VIEW sales.daily ADD PRIMARY KEY (day) NOT ENFORCED;
```

- **Kept in the view's own entry**: a view's in its `q/` entry (`StoredView::facts`), a materialized
  view's among its table's constraints (`TableMeta::constraints`, never enforced). So a rename moves
  them, a drop takes them, a clone and a branch carry them, and no prefix or listing is added.
- **Named as Postgres names a table's**: `{view}_pkey`, `{view}_{columns}_key`, `{view}_{columns}_fkey`.
  Each names only the view's columns (a measure is no column: refused), a view has one PRIMARY KEY,
  and a name is used once.
- **Through `CREATE OR REPLACE VIEW`** (dbt makes views so on every run): a statement with a list
  sets them as written; one without keeps them, and is refused by name when one names a column the
  new query drops, with the `ALTER VIEW … DROP CONSTRAINT` to run first (the SQL review's rule, so
  a suggestion run again keeps what the user added).
- **`ADD CONSTRAINT IF NOT EXISTS name`** adds nothing when the name is there, so a file of them runs
  twice safely (`pondra.suggest_ontology`'s statements).
- **Shown everywhere a table's are**: `SHOW CREATE VIEW` and `pondra export` write the list (one
  statement, so a project's file and a drop-and-run-again make the same view), `pondra.parts` lists
  `key` and `link` parts, Postgres's `pg_constraint` and `information_schema.table_constraints`
  have them (BI tools read relationships there), and §3's join check takes a view's key as a
  table's.
- **Refused by name**, each with the statement to write: one without NOT ENFORCED, a CHECK on a
  stored view (a materialized view's checks are its expectations), a column it doesn't have (a
  measure is none), a second PRIMARY KEY, a name taken, `ALTER VIEW` of a table, and `ALTER TABLE …
  ADD CONSTRAINT` of a view (42809, pointing at `ALTER VIEW`). No `RELY`: the planner never uses a
  fact (ADR-057 §4).
- **Spelled as the SQL review settled** (`designs/ontology-as-a-schema.md`, Names), with two
  additions it was asked about: the list form, and `SHOW CREATE VIEW` as that one statement rather
  than `CREATE VIEW` and `ALTER VIEW` lines, because a project's file holds one statement per object.

### 3. Measures in views (E13 phase 1, L5-3)

```sql
CREATE VIEW sales AS
SELECT o.ordered_at::date AS day, c.region,                    -- dimensions
       sum(o.amount)                 AS MEASURE revenue,       -- measures
       count(DISTINCT o.customer_id) AS MEASURE customers,
       revenue / customers           AS MEASURE per_customer
FROM orders o LEFT JOIN customers c ON o.customer_id = c.id;

SELECT region, MEASURE(per_customer) FROM sales WHERE day >= DATE '2026-01-01' GROUP BY region;
```

- **A view with measures is a view.** Listed, granted, shared, branched, commented on and dropped as
  one. Its measures are `measure` parts; its other columns are its dimensions.
- **Stored as SQL that parses.** `expr AS MEASURE name` becomes `expr AS "pondra_measure:name"` where
  SQL comes in (`measures::text`, outside strings, quoted names and comments). Every place that shows
  a view's SQL to people (`SHOW CREATE`, `pondra.tables`, the console, Postgres's `pg_views`) shows it
  back as `AS MEASURE` (`measures::written`).
- **Read plainly, it is its rows at its own grain.** Read as stored (`SELECT * FROM sales`, a BI
  tool's preview, Postgres's catalog), a view with measures is its *default form*: its dimensions
  and each measure, grouped by every dimension. So `SELECT * FROM sales` and the catalog's columns
  work with no special client.
- **Used, a measure is computed at the query's own grain.** A SELECT that reads a view with measures
  and names a measure (`MEASURE(m)`, `AGG(m)`, `AGGREGATE(m)`, one name: invariant 126; `sum(m)`,
  `min`, `max`, `avg`, `any_value` of it alone, as BI tools send; or the bare column) reads the
  view's *base* instead: its FROM and WHERE, its dimensions by name, and each column a measure reads
  as a hidden column. Each measure becomes its expression over those columns, at the query's own
  GROUP BY (none given: the dimensions it selects). Per customer by region is that region's revenue
  over its distinct customers, never an average of ratios. The base is then written in place
  (`measures::flattened`): the query becomes the `GROUP BY` a person would write over the view's
  tables, and plans as one (on three nodes the subquery cost a query about 3 ms more). It stays a
  subquery only where writing it in could change a name's meaning: a name the base doesn't have
  (outside ORDER BY's names of the query's own columns), a subquery, a window, DISTINCT, or an
  unnamed item that isn't a plain column. A query that names no measure reads the default form, with
  SQL's meaning.
- **Expanded where SQL comes in, before anything plans it** (`measures::rewrite`, first in
  `front::expand`'s tree stage, so the macros, DuckDB's spellings and enum casts of the
  inserted SQL are expanded after it). After it the query is a plain `GROUP BY`: spread, the result
  cache, the guard and the join order work unchanged. A query without measures pays a look at the
  lake's views with measures, kept per catalog version (`measures::own_views`, as `routines::listed`
  keeps functions), and at an attached lake's only when the statement names that lake.
- **No double counting.** When a view with measures is made, each of its joins must be `JOIN` or
  `LEFT JOIN … ON` equalities that cover a key of the joined table (its `PRIMARY KEY`, a `UNIQUE`,
  enforced or not, or a view's key fact from §2). The first relation may be anything. A `LEFT JOIN`
  that no used dimension or measure reads is left out of the base.
- **Refused by name**, never answered wrong: a measure in WHERE (use HAVING); a measure inside a
  larger aggregate (`sum(m * 2)`); a SELECT with measures that joins the view to anything else (join
  inside the view, where it is checked); `MEASURE()` over a view without measures, or of a name that
  isn't a measure; in a definition: GROUP BY, HAVING, DISTINCT, ORDER BY, LIMIT, a set operation, `*`,
  a subquery or window in a measure, a RIGHT, FULL, CROSS, NATURAL or comma join, a join not on a key,
  a measure named as a dimension, measures that refer to each other in a loop, measures over another
  view's measures, a column list after the view's name (`CREATE VIEW v (a, b)`: name them in the
  SELECT), and `AS MEASURE` in a materialized view (phase 2's rollups). A view of a view with
  measures, and a materialized view of one, read it with `MEASURE(…)` as any query does.
- **What a BI tool's own subquery does:** a tool that reads the view in a subquery of its own and
  groups again (`SELECT region, sum(r) FROM (SELECT region, status, revenue AS r FROM sales) …`)
  gets the subquery's rows at the subquery's grain, as SQL says; additive measures add up right, a
  ratio does not. Tools that send `sum(m)` against the view itself (Tableau, Superset, Metabase) get
  the measure.
- **Other doors:** MCP's agents find measures with `find(kind => 'measure')` and query them as SQL;
  frames get `pondra.measure("m")`; the Postgres port lists measures as columns.

Phase 2 (rollups kept in the source's commit and read in place of the base) and phase 3 (rollups
proposed from `pondra.history`) follow E13 after 1.0's freeze unless asked sooner.

### 4. The ontology's reads (L5-4, L5-5)

`pondra.ontology('s')`, `pondra.related(…)` and `pondra.suggest_ontology(…)` read §1–§3 as E14
(`designs/ontology-as-a-schema.md`) describes; their own section is added when they are built.

## Invariants

345. **`pondra.parts` and `pondra.find` show only what the caller may see** (`find::parts`, `find::found`):
   parts come only from the objects `objects::list` gave the caller; a user granted some columns of a
   table sees those columns and no other part of it, and that table's definition is not shown. Neither
   is a remembered answer. `harness.py find`: "a user granted one table…", "a user granted some
   columns…", "never a remembered answer…".

346. **A measure is worked out at the query's own grain, from the view's base rows, never from its
   default form's** (`measures::rewrite`, first in `front::expand`'s tree stage): a query naming
   a measure reads the view's FROM and WHERE with the dimensions it uses and each column its measures
   read, and each measure is its expression over those, grouped by the query's GROUP BY; the base is
   then written in place (`measures::flattened`) unless that could change a name's meaning. Everything
   after it sees a plain `GROUP BY` (spread, the result cache, the join order). `harness.py measures`:
   "MEASURE() at the query's own grain…", "every way of naming a measure…", "spread over three nodes
   == one node".
347. **A view with measures joins only onto keys, so no row is counted twice** (`measures::check`, in
   `ddl::create_view`): each JOIN or LEFT JOIN's equalities cover a PRIMARY KEY or UNIQUE of the table
   or view it joins (NOT ENFORCED is enough); a query that joins the view to anything else is refused.
   `harness.py measures`: "refused by name, and nothing made" ('not on a key', 'has no key', 'inside
   the view').

348. **A view's key facts are its own entry's, name only its columns, and nothing checks them**
   (`facts.rs`): a stored view's in `StoredView::facts`, a materialized view's in its table's
   constraints, never enforced; set by the statement that makes the view; a `CREATE OR REPLACE VIEW`
   that drops a column one names is refused; one naming anything but the view's columns is refused.
   `harness.py facts`: "kept through CREATE OR REPLACE…", "refused by name, and nothing made",
   "SHOW CREATE VIEW run again after a drop makes the same view".

## Consequences

- One search covers every kind, present and future: a new kind is found when its lister yields it.
- BI tools and agents get right numbers through plain SQL; there is no semantic-layer server, YAML
  file or connector.
- A view with measures costs nothing when unused, and a query with measures runs as the `GROUP BY`
  a person would write by hand (`harness.py measures` compares both, answers and time).
