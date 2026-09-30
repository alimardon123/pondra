# ADR-034: A console of tabs: files, results and data, as the canvas drew them

**Date:** 2026-09-29 · **Status:** accepted (the owner chose the canvas, "Pondra console layouts", comment by comment) · **Builds on:** ADR-030 (the console), ADR-032 (a core to build on), ADR-033 (a workspace, proposed: this round builds its console half)

## Context

ADR-032 made the console a core with an extension API: one notebook in the middle, the catalog,
files, outline and notebooks stacked on the left, details on the right. The owner's review
(2026-09-29) found it crowded and buggy, and wanted a lightweight, premium tool that mixes SSMS,
Databricks, DuckDB's UI, VS Code and Snowflake:

- one sidebar with two groups, simple for everyone, with room to grow into explorer + tabs for
  the enterprise build;
- a tab per open file with a close button, and a toolbar for the file in front;
- SQL files with their results pinned below, notebooks as they are, Python files with a console
  below;
- data files viewed and edited in tabs;
- the outline inside the notebook it belongs to, and icons that can't be confused.

The design was drawn as mock-ups first and settled comment by comment. Decided along the way:

- Data first in the sidebar (a setting puts Workspace first).
- Column types as coloured icons, with a card on hover.
- Tables that fit their columns.
- A charcoal dark theme.
- Geist, bundled.
- A Python console, not a shell.
- Views that move between the two sides, the Workspace on the left by default.
- The console round only: running files with parameters, and schedules for them, stay ADR-033's
  server half.

## Decision

### 1. The shell

From top to bottom:

- **The top bar:**
  - the mark;
  - where the page runs (the database, its role and nodes);
  - a search box, **Ctrl+K**: tables, files and commands;
  - three pane switches: left **Ctrl+B**, bottom **Ctrl+J**, right **Ctrl+Alt+B**;
  - Runs, Help, the menu, and **Sign in**.
- **The tab bar:** one tab per open document, each with ×. A dot marks one not saved, and the
  same dot marks it in the Workspace.
- **The document's toolbar:** its path, whether it is saved, and its own buttons.
- **The document.**
- **The status bar:**
  - left: the connection, and the document's place (line and column, rows);
  - right: its language and the version.

**Sign in.** In the open product it is the token dialog: the node's tokens are its accounts, for
now. The enterprise build puts who is signed in there (`pondra.configure` already takes its
sign-in). User accounts and single sign-on come with the server catalog (ADR-032 §9).

### 2. Views that can sit on either side

Every group of the left side and every tab of the right side is a view:
`register.view({ id, title, side, render(box, picked), tools, order })`.

- `register.section` is a view on the left, and `register.panel` one on the right; both stay.
- A view's ⋯ moves it to the other side, and the browser remembers where.
- A filter at the top of the left pane narrows its trees to the names that hold what is typed,
  with what they are in (Esc clears it).
- Each side pane's edge sets its width (dragged, or arrow keys when focused; a double-click resets
  it), kept in the browser.
- On the left, views stack as groups that fold, with a divider between them that sets their share.
  On the right they are tabs.
- The core's views:
  - **Data**: databases, schemas, tables, views, columns;
  - **Workspace**: the lake's files;
  - **Details**, **Variables** and **Runs**.

### 3. Documents, by kind

`register.doc({ id, match(path), open(path) })` opens what a path is. So an extension adds a kind (a
dashboard, a pipeline) as the core adds its own.

| Kind | Paths | The document | Below it |
|---|---|---|---|
| Notebook | `notebooks/<name>` (versions `<time>.ipynb`) | cells, as ADR-030 | — |
| SQL | `*.sql` | an editor with line numbers | Results, Messages, Chart, Plan (or at the right) |
| Python | `*.py` | the same editor | a console: the file's runs, and a `>>>` line in the page's Python |
| Data | `*.csv` `*.tsv` `*.json` `*.jsonl` `*.ndjson`, `*.parquet` | a table: CSV and JSON edited in place, Parquet read-only | — |
| Text | `*.md` `*.txt` | the editor | — |

- **Run.** A SQL file runs the selection, or else the whole file. A Python file runs as the page's
  `DO` block, so it shares the notebooks' variables. **Stop** stops waiting.
- **A SQL file's statements each get an answer.** They run one after another in the page's session,
  split as the node splits a script (`routines.rs`: a `;` in a string, a comment or a `$$` body
  doesn't end one), and stop at the first that fails. Above the results, a chip per statement (`1
  done`, `2 5 rows`, `3 failed`) picks the one shown; Messages lists them all. Settings can say
  **The last one's answer** instead: the text goes to the node at once, and the last statement's
  answer shows, as Snowflake's and Databricks' editors do (the owner, 2026-09-30). A notebook
  cell keeps one answer, its last statement's.
- **Opening a file** from the Workspace brings its tab forward, or opens one (a second click while it
  opens waits for the same tab). The folders of the file in front open in the Workspace.
- **The notebook's outline** (the headings of its text cells) sits under its row in the
  Workspace.
- **A table double-clicked** gives its first rows: in a cell when a notebook is in front,
  otherwise in a new SQL tab.
- **The open tabs** are remembered in the browser, per database.

### 4. Files are saved where they are: replaced, never overwritten blind

A file in the lake could not be replaced (`PUT /files` refused a path that exists). That keeps
notebooks' versions, but it makes a SQL file or a CSV uneditable. So the `files/` part of a lake,
the user's own files, may now be replaced:

- **`GET /files/<path>`** answers with an `etag`: the object's e-tag and size, or its time and
  size on a store without e-tags. It also gives a content type by the file's extension.
- **`PUT /files/<path>` with `If-Match: <etag>`** replaces the file if it is still that version.
  A newer version, saved meanwhile, is refused with `412`, so two people editing never overwrite
  each other blind. With no `If-Match`, a path that exists is still refused.
- **`DELETE /files/<path>`** removes it, for a writer, as `PUT` is. Rename is a copy, then this.
- **Notebooks keep versions** (a save is a new `<time>.ipynb`); other files are replaced in place.

What makes replacing safe:

- **Nothing caches `files/`.** Lake objects were never overwritten, so the caches never go stale
  (`cache.rs`). That still holds for everything but `files/`, which now bypasses the SSD tier on
  every node, for reads and writes alike.
- **SQL never cached those files.** It read `files/` through the store as it is, and DataFusion
  keeps no list of files (`with_list_files_cache_limit(0)`). So a replaced CSV reads its new rows on
  every node, at the next statement.

### 5. The result grid

One grid for query results, notebook outputs and data files.

**Its look:**

- A tinted header row and row-number column frame the cells.
- Rows are separated by lines, drawn as shadows so a selection's outline is one solid line.
- It sits in a box with 8 px corners and fits its columns.

**Types:**

- A coloured icon before each name:
  - blue T for text;
  - amber # for whole numbers;
  - amber .01 for decimals;
  - violet clock or calendar for times and dates;
  - teal switch for true/false;
  - orange braces for JSON-like values;
  - brackets for lists.
- Hovering a header opens a card with the full type and the column's least and greatest value,
  NULLs and distinct values, from the rows at hand.

**Clicks:**

- A cell: it becomes active, and its whole row is tinted, to follow it across a wide table.
- Drag or Shift+click: a range.
- A row number: that row. A header: that column, and its profile in Details. The corner:
  everything.
- A header's small arrow sorts by that column.
- The arrow keys move; Shift+arrow extends.

**Copying:**

- **Ctrl+C** copies tab-separated, so it pastes into a spreadsheet as cells. **Ctrl+Shift+C**
  adds the headers.
- Right-click: Copy as CSV, JSON or a SQL list; Filter to these values (in the page, with a chip
  to clear it); Sort; Profile the column.
- The footer, or the results bar, shows the selection's sum and average when there are numbers.

**Editing (a data file):** double-click, Enter or typing edits a cell. Enter keeps the value, Tab
moves right, Esc undoes. Rows and columns can be added and rows deleted. An edited cell is tinted
amber, and a new row green, until saved.

**Large answers:** the grid draws only the rows in sight, as before (10,000 rows at 60 frames a
second).

### 6. Look and feel

- **Tokens.** Colours, type and spacing are CSS variables (`brand/colors.css` first).
  - **Light:** surface `#fff`, chrome `#f4f4f6`, header band `#f1f1f4`, faint text `#6b6b76`
    (4.9:1).
  - **Dark:** charcoal, not black. Chrome `#1e1f23`, sidebar `#222328`, surfaces `#27282d`, text
    `#e4e4e9`, faint `#9596a1`. Every text colour passes WCAG AA (4.5:1) on every background.
- **Themes:** the system's, light or dark (Settings); a page can be pinned with
  `data-theme`.
- **Geist and Geist Mono** (SIL Open Font License, `brand/fonts/`) are built into the binary: a
  Latin subset of about 50 KB each, variable weight. `font-display: swap` means the page never
  waits for them. Settings can use the system's fonts instead.
- **The open tab:** rounded top corners, on the surface colour, joined to its toolbar, with a teal
  line over its top. The others stay flat.
- **Keyboard:**
  - the trees, tabs and menus use arrow keys, each one Tab stop (roving focus);
  - focus always shows;
  - targets are at least 24 px.
- **Figures keep their white paper** in the dark theme, on a rounded card, at most 460 px tall: a
  matplotlib figure is drawn with its own colours, and inverting it would change them.
- **Printed text is cut at 5,000 lines** (400 KB) in a cell or a console, the rest a click away
  (Show them, Download it all); the node itself sends back the first 32 KB a statement printed, and
  says so.
- **Narrow windows:** under 760 px wide (or at 200% zoom) the left pane is a drawer and the right
  a sheet, both opened from the pane switches; the page never scrolls sideways.

### 7. Light, fast, and measured

- **No framework, no editor library, nothing from anywhere else.** The console is plain
  JavaScript modules, served by the node with the fonts.
- **Its code is split by what it does:**
  - `core.js`: the API, the state, the node;
  - `editor.js`: highlighting, the editor, completion;
  - `grid.js`: the grid and charts;
  - `notebook.js`: cells and `.ipynb`;
  - `files.js`: the Workspace and the file documents;
  - `console.js`: the shell.

  The page preloads them all at once.
- **Gzip, and nothing but code.** The node compresses what it serves when the browser takes gzip,
  and serves its own scripts and style sheet without their whole-line comments and indentation (a
  tenth less, gzipped; the source keeps them). So their code holds no string over several lines.
  Every file is tagged by its contents' hash and answers `304` while it is the same.
- **The editor highlights a line at a time.** Each line is highlighted from the state the line
  before left (inside a block comment, a quote, a Python triple-quoted string), so a key
  re-highlights its line and those after it whose state it changed — not the file. The editor's
  width grows in steps, as a new width lays the whole file out again. Measured in a 1,000-line
  file: the whole-file highlighting took 9 ms a key, and a width change 20 ms.
- **A budget, checked in Chromium by `console_check.py` (`budget`):**
  - the page's scripts and styles, gzipped: at most 70 KB;
  - first paint: under 400 ms;
  - typing in a 1,000-line file: under 8 ms a key (median);
  - scrolling 10,000 rows: a frame under 20 ms (95th percentile).

### 8. What stays

The API of ADR-032 stays, extended:

- `register.section`, `panel`, `cellKind`, `renderer`, `action`, `nav`, `key` and `command`;
- `on` and `emit`;
- `configure`;
- `pondra.api` and `pondra.ui`.

New are `register.view`, `register.doc`, `pondra.ui.openFile(path)` and `pondra.docs()`.
`pondra.state.cells` is the notebook in front's cells.

- **Actions.** A menu action goes in the top bar's menu, and one with a label in the top bar.
  The notebook's own actions (Run all, Save) are in its toolbar.
- **Notebooks** are saved as before (`files/notebooks/<name>/<time>.ipynb`, valid for Jupyter).
- **Requests.** Every request still goes to the node that served the page (invariant 138).

## Rejected

- **Editing files by writing new versions only** (as notebooks): SQL reading a file by name would
  read the old one, or every version at once.
- **A pointer in the catalog from a name to its latest object:** every reader of files by name
  would have to resolve it. Replacing with `If-Match`, and not caching `files/`, is simpler.
- **A real shell under Python files:** a web page that runs commands on the node's machine. The
  owner chose a Python console on the page's session.
- **A code-editor library** (Monaco is 2–5 MB, CodeMirror about 150 KB): the core's editor is
  enough for SQL and Python with completion, and it is small.
- **Workspace in the right pane by default:** files and the details of what is picked are wanted
  at once, and trees sit on the left in every tool people know. It can be moved there.
- **The earlier CSS-only restyle** and **a left side mixing notebooks, files and the outline**
  (the owner, 2026-09-29).

## Tests

**`console_check.py`**, in parts:

- **node:** the Data tree, Details and Profile; notebook cells (SQL, Python, errors, shared
  variables, a figure, Variables and Restart, completion, a live cell, the keys); the outline under
  the notebook in the Workspace; a notebook saved twice, a version opened again, downloaded and
  uploaded (nbformat validating each).
- **files:** a SQL file opened in one tab (two clicks), run (Results, Messages, Plan), changed (the
  dot in its tab and in the Workspace) and saved in place; its statements' answers, and the
  setting; a new one asking for its path; a Python file's console and a `>>>` line; a CSV edited (a
  cell, a row) and saved, its untouched lines as they were, read by SQL with the new rows; a stale
  save refused (412), and Discard; a JSONL file edited; a Parquet file read-only.
- **grid:** a click tints the row; a range's outline (its edges), its sum; Ctrl+C and Ctrl+Shift+C
  (the clipboard read back); the keys; the menu's filter and its chip; the sort button; a header's
  card.
- **layout:** a view moved to the right and back (kept after a reload); Settings (Workspace first,
  the dark theme); the filter; Ctrl+B and Ctrl+Alt+B; a pane's edge (kept, reset); the tabs open
  again with the page; a narrow window (no sideways scroll, drawers); **axe** (WCAG 2.1 AA, contrast
  included, and its best practice) finds nothing, light and dark, on a notebook, a SQL file and a
  data file.
- **budget:** the numbers of §7, and the editor's line-at-a-time highlighting equal to the whole
  text's after edits that open and close comments and quotes.
- **tokens, server and extensions:** as before, the extension's section and tab now views.
- **Requests:** every one went to the node, and no page errors.

**`harness.py external`:**

- `GET /files` gives an `etag`;
- a `PUT` with it replaces the file, and a stale one is refused with 412;
- a `PUT` without one onto a file that exists is refused;
- `DELETE` removes it;
- a replaced CSV reads its new rows through SQL, on the node and on a second node of the lake;
- `files/` never lands in the SSD tier.

## Changed after 0.26 (round 28, 2026-09-30)

The owner tried 0.26 on Windows and asked for the Workspace to work like VS Code's explorer. Built
the same day; *decided by Claude where marked*:

- **New files are in the tree before they are saved**, in the folder they will be saved to (SQL in
  `queries/`, Python in `scripts/`), with the unsaved dot, which their tabs show too.
- **Folders.** New folder is in the + menus and each folder's menu. An empty folder is kept as a
  zero-byte `.folder` object, which nothing lists. *(Decided by Claude: object storage has no
  folders, and a marker is what keeps an empty one across reloads.)*
- **A ⋯ on every row** of the tree (on hover, on focus, and on the row that is picked) opens the menu
  a right-click does. A folder's menu: New notebook, SQL file, Python file and folder here, Upload a
  file here, Delete folder (it says how many files it deletes).
- **Notebooks in any folder.** In `notebooks/` they keep their versions, as before; anywhere else a
  notebook is one `.ipynb` file saved in place, as SQL and Python files are (`If-Match`). *(Decided by
  Claude: those can't run as jobs or on a schedule yet, which go by a notebook's name; the menu
  leaves them out.)*
- **One way to make things:** the Workspace's ⋯ no longer repeats the + (its item failed from
  there), and "Upload a file…" replaces "Open an .ipynb or put a file in the lake…".
- **Dialogs close on a click outside them** (search, Settings, Keys), as Esc closes them.
- **The Data tree lists what it can** when a view reads a file on the node's machine, which only
  the program that started the node may read: that view is left out, instead of the whole tree
  failing.
- **Tests:** `console_check.py`, 16 new checks (78 in all).

## Changed after 0.27 (round 29, 2026-09-30)

The owner went through 0.26 on Windows, part by part. Built in round 29; *decided by Claude where
marked*:

- **The grid.** A selection's outline is whole (its bottom edge was missing). A header's card is
  smaller, and hides as soon as the header is pressed. "Copy as a SQL list" takes every column
  selected, not just the first. A header has its own right-click menu: its name (or the selected
  columns' names), as SQL too, its values with the header, sort, filter, profile. A filter is typed:
  how to compare (contains, starts with, =, ≠, <, ≤, >, ≥, is NULL, …) and the value; its chip opens
  it again to change it.
- **Copy and Download are split buttons**: a click does the usual thing (tab-separated with the
  headers; CSV), the ▾ lists the other forms. A download is every row, not the 10,000 shown: the
  statement runs again on the node with `?format=csv|tsv|ndjson|parquet|xlsx`. *(Decided by Claude:
  the workbook is written by `xlsx.rs`, a zip of five XML parts, rather than a crate: a few KB of
  code, no dependency; Excel's own limit, 1,048,575 rows, is said in the error.)*
- **Messages** shows each statement: its SQL, its time, what it did or printed, its error.
- **The Plan tab draws the plan**: a graph of the steps, each above what it reads, the steps that
  move rows dashed. **Profile** runs `EXPLAIN ANALYZE` and puts each step's rows, time and share on
  it, the costliest in red. *(Decided by Claude: DataFusion's metrics as they are, no cost model of
  our own: they are measured, not estimated. The owner asked whether Runs should keep profiles, as
  Snowflake's and Databricks' query histories do: they will, from the query history table of round
  32, where every node's statements, plans and times are kept.)*
- **Charts:** bars, lines, areas, points, a pie; by any column, of any number columns; saved as PNG
  or SVG.
- **Runs:** each run a line (a statement shortened, a run's number, its file), a click shows it with
  what to do (open in a new file, put in the tab in front, copy, run again, see its plan), and a
  right-click the same. A node's `DO` block logs its code (`pondra.runs.args`: `{"language", "code"}`),
  so Runs names it by its first line, not "do". The clock in the top bar opens Runs, and closes it.
- **The right pane's tabs** reorder by dragging, and Details follows the file in front.
- **Editors** have a right-click menu: cut, copy, paste, select all, comment, run, and **Format**
  (Shift+Alt+F) of the selection or the whole file, in SQL and Python files and in notebook cells.
  SQL is formatted in the page; Python by the node's Python (`POST /python/format`: ruff, else
  black). Whatever can't be formatted, or changed while it was, stays as it was. *(Decided by
  Claude: no Python formatter in JavaScript is small enough for the budget, and ruff and black are
  the formatters Python's users already run.)*
- **Notebook cells:** the kind is a button with a menu (a text cell switches back); Python cells
  that never ended on Windows: see below.
- **Save** shows only when there is something to save, highlighted; no "not saved" text; a new file
  gets its dot when something is typed in it.
- **Settings** is a gear and a dialog (✕, Esc, a click outside). Light is the first theme; each
  theme takes a background and an accent colour of the user's (the panes' shades are mixed from
  them). **The node keeps the settings on its machine** (`GET/PUT /console/settings`,
  `PONDRA_CONFIG_DIR` or the system's place), so every lake and session opened there has them.
  *(Decided by Claude: a `PUT` from loopback only, so a page on another machine keeps its own in its
  browser rather than changing the machine's; an enterprise build replaces the store, not the page.)*
- **Python on Windows.** The likeliest cause of cells that never ended: `--python auto` waiting on
  a Python that never answered. Now each Python found (the PATH, the `py` launcher's, Anaconda's and
  Miniconda's and their environments) is tried at once, for `PONDRA_PYTHON_PROBE_SECS` (15) at most;
  a new worker must say hello within `PONDRA_WORKER_START_SECS` (60), and an error carries what it
  printed. **Choose the Python…** lists them (version, pondra, pyarrow, the pip command for one that
  lacks them) and keeps the choice beside the settings. Stop interrupts a running cell (SIGINT: its
  variables stay; Windows restarts Python); a cell the page stopped waiting for never holds up the
  next one.
- **Narrow windows:** a results panel at the right gives way (it may not take more than all but
  160 px of its file), and below 560 and 440 px it drops the time, then the row count and the tabs'
  words, so its buttons stay in reach.
- **The budget.** The page's first load had grown to 76.5 KB gzipped. *(Decided by Claude:)* Runs,
  Variables, Settings, search, choosing the Python and a table's profile moved to `more.js`, a data
  file's editor to `data.js`, and their style to `more.css`, all loaded when first used, as
  `chart.js` and `plan.js` are: 68.5 KB now, and each on-demand module under 8 KB, which
  `console_check.py` measures too.
