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

`ALTER VIEW v ADD CONSTRAINT name PRIMARY KEY | UNIQUE (cols) NOT ENFORCED`, and `FOREIGN KEY (cols)
REFERENCES t (cols) NOT ENFORCED`, are facts about a view, kept at `vk/{view}` (a prefix of its own,
invariant 104). They are listed as `key` and `link` parts, shown by `SHOW CREATE VIEW`, put in
Postgres's `pg_constraint` (BI tools read relationships there), kept through `CREATE OR REPLACE
VIEW` while their columns are still there, moved by a rename and gone with a drop. A view's key is
what §3's join check and the ontology (E14) read. Nothing checks it: it is the writer's promise, as
on Snowflake and Databricks.

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
  over its distinct customers, never an average of ratios. A query that names no measure reads the
  default form, with SQL's meaning.
- **Expanded where SQL comes in, before anything plans it** (`measures::rewrite`, first in
  `routines::expand_with`'s tree stage, so the macros, DuckDB's spellings and enum casts of the
  inserted SQL are expanded after it). After it the query is a plain `GROUP BY`: spread, the result
  cache, the guard and the join order work unchanged, and a query without measures pays one scan of
  `q/` names.
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
  view's measures, and `AS MEASURE` in a materialized view (phase 2's rollups).
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

(Invariants for §2 and §3 are added with their pull requests, from 346.)

## Consequences

- One search covers every kind, present and future: a new kind is found when its lister yields it.
- BI tools and agents get right numbers through plain SQL; there is no semantic-layer server, YAML
  file or connector.
- A view with measures costs nothing when unused, and a query with measures runs as the `GROUP BY`
  a person would write by hand (`harness.py measures` compares both, answers and time).
