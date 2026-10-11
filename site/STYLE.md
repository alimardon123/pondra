# Writing Pondra's docs

The site is for **users**: someone who wants to load data, query it, stream it, and use Pondra
from their tools. The ADRs in `docs/` are for builders. Say what a thing does and how to use it,
not why it was designed so. Link to a design note only when a reader would want the reasons.

## Voice

- Plain, short sentences. Say "you". Put the answer first, then the details.
- A page opens with one or two sentences: what this is, and when you'd use it.
- Show, then explain. Every feature gets a small example you can paste and run.
- Name things the way SQL does (`CREATE MATERIALIZED VIEW`, `MERGE`), then the clients' names.
- No marketing words: "blazing", "seamless" and "powerful" are out. Numbers are fine if the repo
  measured them (`docs/prototype-status.md`), with what was measured.
- Admit limits where a user would hit them, in an `<Aside type="caution">`.

## Where a page goes

The site has three tabs, each a top-level group of the sidebar in `astro.config.mjs` (`src/tabs.ts`
shows a page its own tab's sidebar):

- **Docs** teaches and does. *Get started* is tutorials, read in order. The groups after it (Data,
  Streams, Python and code, Teams, Connect, Operate) are guides: one task or area each, shown by
  example. *How it works* explains (`concepts/`).
- **SQL** is the language looked up: every statement, function, option and system table (`sql/`).
- **APIs** is every other door looked up: the clients, the command line, the protocols and the
  environment variables (`reference/`).

A page is one of these kinds, never two: a guide links to the reference for every option instead of
listing them, and a reference never teaches. `PAGES.md` says what each page covers, so each thing is
said once: on its owning page. Elsewhere, one line and a link.

**A new feature lands with its docs:** a line and an example on *Everything Pondra does*
(`start/features.mdx`, in its group), a section on the guide that owns its area, and its rows in the
reference (a statement, a function, a method, a route, a flag, a variable). A new page goes into the
sidebar group of its kind, in `astro.config.mjs`, and into `PAGES.md`.

## Pages

- Frontmatter: `title` (short), `description` (one sentence, for search results). `setup:` is SQL
  that `tools/docs_check.py` runs before the page's examples (hidden from readers). Use it only for
  data a page needs and doesn't show making. The sidebar's order is `astro.config.mjs`'s, not the
  page's.
- Use `.mdx` when you need components:
  `import { Tabs, TabItem, Aside, Card, CardGrid, Steps, LinkCard } from '@astrojs/starlight/components';`
- Headings start at `##`. Keep sections short, and make every heading a task or a noun a reader
  would search for.
- Link to other pages with site-relative paths: `/pondra/guides/views/`.

## Examples: every one runs

`tools/docs_check.py` runs each page's code blocks in order, against a fresh node. The node has
HTTP on 8080, Postgres on 5432, Kafka on 9092, Flight on 8815, and Python functions on. So:

- **`sql` blocks** go to `POST /sql`. Several statements in one block are fine.
- **`python` blocks** share one interpreter per page. Connect once, at the top of the page:

  ```python
  import pondra
  db = pondra.connect("http://localhost:8080")
  ```
- **`python cell` blocks** are the console's Python cells: they run on the node, as
  `DO LANGUAGE python`, with `db` and `pondra` already there (no connect).
- **`js` blocks** run on their own, as ES modules: `import { connect } from "pondra";` and top-level
  `await`.
- **`bash` blocks** run in a fresh folder, with `pondra` on `PATH`.
- A block that can't run here (a second machine, Windows, cloud credentials, a command that
  serves until stopped) gets `norun` in its info string:
  ````md
  ```bash norun
  pondra serve --dir s3://my-bucket/lake --addr 10.0.0.5:8080
  ```
  ````
  Use `norun` as little as you can. A reader trusts examples that run.
- Output shown to readers goes in a `text` block after the example (never run).
- Every page's examples start from an empty lake. Make what they need, or use `setup:`.

## The same thing in every language

Where a feature exists in SQL and in the clients, show it with synced tabs:

```mdx
<Tabs syncKey="lang">
  <TabItem label="SQL">
    ```sql
    SELECT user, sum(amount) AS total FROM orders GROUP BY user;
    ```
  </TabItem>
  <TabItem label="Python">
    ```python
    db.sql("SELECT user, sum(amount) AS total FROM orders GROUP BY user").to_pandas()
    ```
  </TabItem>
  <TabItem label="Frames">…</TabItem>
  <TabItem label="PySpark">…</TabItem>
  <TabItem label="JavaScript">…</TabItem>
</Tabs>
```

A statement that runs the same on every door is written once, as `<Ways sql={`…`} />` (import it with
`import Ways from '../../../components/Ways.astro';`, the path relative to the page). It shows the SQL,
Python, JavaScript, Shell and HTTP tabs from `site/src/ways.json`, and `docs_check.py` runs all five.
Write tabs by hand only where a door has its own call (frames, `append`, `write_table`, PySpark). Output
still goes in a `text` block after it.

Labels are always exactly `SQL`, `Python`, `Frames`, `PySpark`, `JavaScript`, `Shell`, `HTTP`, `psql`, in
that order, and only the ones that apply. The reader's choice is remembered across the site.

## Facts

Write only what the code does today. The sources are: `README.md`, `docs/dataframe-api.md` (the
name table), `docs/lake-format.md`, the ADRs, `python/pondra/`, `js/index.js`, `src/main.rs` (the
command line), `src/server.rs` (the HTTP routes), and `tools/harness.py` (tested usage). When in
doubt, run it. If a page's example fails, fix the example, not the check.
