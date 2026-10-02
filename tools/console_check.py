#!/usr/bin/env python3
"""The console (ADR-030, ADR-032, ADR-034), in headless Chromium through Playwright.

  console_check.py [--port 8890] [--show DIR] [--only node,files,grid,layout,budget,tokens,server,extensions]

- node: a new notebook has no dot until something is typed (its tab, the Workspace); the Data tree lists the lake's schemas, tables and views (each kind its icon) and columns
  (each type its coloured mark, a key's marked), with no bare numbers; a table picked shows its
  details and its profile, a view its definition; double-clicking a table shows its first rows; the
  Workspace lists the lake's files by folder; a SQL cell's answer (its types as marks, a header's
  card), a long one drawn in part, sorted and summarized; a Python cell (what it printed, its last
  expression as rows); errors in plain words; Python cells share their variables, a figure shows,
  the Variables tab lists them, Restart empties them; Tab completes a name; a live cell; the
  notebook keys (Esc, B, D D, Z, M, O, Shift+Enter, ?); the outline under the notebook; a notebook
  saved (twice: two versions, the first opened again), downloaded and uploaded, Jupyter's
  nbformat validating each.
- files: a SQL file runs (Results, Messages, Plan) and is saved in place; a new one asks for its
  path, and shows it is not saved (its tab, the Workspace) until it is; a Python file runs in its
  console, and a line typed there too; a CSV file is edited in a grid (a cell, a row) and saved in
  place, its untouched lines as they were, and the node reads the new rows; saving over someone
  else's change is refused; a JSONL file is edited; a Parquet file opens read-only; two clicks open
  one tab. Folders (a zero-byte `.folder` marker nobody sees): made from the Workspace's +, the
  tab bar's + and a folder's menu (a name that exists or is not one is refused), deleted with
  their files; a ⋯ on every row of the Workspace (on hover, on focus, on the one picked) opens its
  menu; a notebook made in a folder is one plain file saved in place (refused over someone else's
  change), opens again after a reload, offers no jobs; a file renamed moves its tab; the +, Ctrl+K
  and the welcome page say "Upload a file…"; the top bar has a gear for Settings, no ⋯ of its own.
- grid: a click lights a cell and its row, Shift+click a range (one outline), the keys move it,
  Ctrl+C copies it (Shift: with the headers), the menu filters to its values, a header's sort
  button sorts, a header's card tells its type.
- layout: a view moves to the other pane and back (kept after a reload); Settings puts Workspace
  first and the dark theme on; the filter narrows the trees; Ctrl+B and Ctrl+Alt+B; a pane's edge
  sets its width (kept); the tabs open again after a reload; a narrow window: no sideways scroll,
  the panes drawers; axe finds nothing (contrast included), light and dark.
- budget (ADR-034 §7): the scripts and style sheet the page loads, gzipped as the node serves them,
  <= 70 KB, and those loaded when first used (chart, plan, more, details, data) <= 8 KB each; each answers
  304 when the browser has it; first paint < 400 ms; typing in a 1,000-line file < 8 ms a
  key (median); scrolling 10,000 rows: p95 frame < 20 ms.
- tokens: a node with tokens serves the page, which asks for one; given it, the tables show, and
  after a reload too (the browser keeps it).
- server: `pondra serve <folder of lakes>`'s console lists its databases, and a cell runs in the one picked.
- extensions: a node given PONDRA_CONSOLE_EXTENSIONS (examples/console-extension.js) serves it, and
  its section, panel tab, view of an answer and menu action show beside the console's own.
- every request the page made went to the node (or server) that served it; no page errors.

--show DIR keeps screenshots, light and dark.
Needs: playwright (Chromium at PLAYWRIGHT_BROWSERS_PATH), nbformat, pyarrow; axe-core (npm) for the
layout part's audit: AXE_JS, or node_modules/axe-core beside this file or in the current folder.
"""
import argparse, gzip, io, json, os, re, shutil, statistics, subprocess, sys, tempfile, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import harness
from harness import BIN, Node, call, sql

import nbformat
from playwright.sync_api import sync_playwright


def until(f, want=True, secs=20):
    """Poll `f` until it gives `want` (or `secs` pass); what it gave last."""
    end, got = time.time() + secs, None
    while time.time() < end:
        try:
            got = f()
        except Exception as e:  # noqa: BLE001 (not yet)
            got = e
        if got == want:
            return got
        time.sleep(0.1)
    return got


def _try(f):
    try:
        return f()
    except Exception as e:  # noqa: BLE001
        return e


def put(port, path, body, version=None):
    """PUT a file of the lake (with If-Match when `version`); its answer."""
    return call(port, "PUT", "/files/" + path, body, headers={"if-match": f'"{version}"'} if version else None)


def get(port, path):
    """A file of the lake, as its bytes."""
    return urllib.request.urlopen(f"http://127.0.0.1:{port}/files/{path}").read()


def version(port, path):
    c = urllib.request.urlopen(f"http://127.0.0.1:{port}/files/{path}")
    return c.headers["etag"].strip('"')


class Page:
    """A browser tab on `url`, with every request it made and every error it hit."""

    def __init__(self, browser, url, scheme="light", size=(1440, 900)):
        self.ctx = browser.new_context(viewport={"width": size[0], "height": size[1]}, color_scheme=scheme, accept_downloads=True)
        self.ctx.grant_permissions(["clipboard-read", "clipboard-write"], origin=url.split("/#")[0].rstrip("/"))
        self.p = self.ctx.new_page()
        self.url, self.seen, self.errors = url, [], []
        self.p.on("request", lambda r: self.seen.append(r.url))
        self.p.on("pageerror", lambda e: self.errors.append(str(e)))
        self.p.on("dialog", lambda d: d.accept())  # (confirm: close a tab with changes, replace a file)
        self.p.goto(url)

    def cells(self):
        return self.p.locator("#docs section.cell")

    def cell(self, i):
        return self.cells().nth(i)

    def run(self, i, code):
        """Put `code` in cell i and run it; the cell."""
        c = self.cell(i)
        c.locator("textarea").fill(code)
        c.locator("textarea").press("Control+Enter")
        c.locator(".out").wait_for(state="visible", timeout=30000)
        until(lambda: c.locator(".run.stop").count(), 0, 60)
        return c

    def grid(self, el):
        """An answer's column names and its rows, as text."""
        return el.evaluate("""el => { const g = el.matches('.gt') ? el : el.querySelector('.gt'); if (!g) return [[], []];
            return [[...g.querySelectorAll('thead th .hn')].map(x => x.textContent),
                    [...g.querySelectorAll('tbody tr:not(.gap)')].map(tr => [...tr.querySelectorAll('td:not(.i)')].map(td => td.textContent))] }""")

    def menu(self, item):
        """Settings (the top bar's gear: a dialog, loaded when first opened), or `item` in the Workspace's + menu."""
        if item == "Settings":
            self.p.click("#settingsBtn")
            self.p.locator("dialog.settings2[open] .s-row").first.wait_for(timeout=10000)
            return
        self.p.click("#newfile")
        self.p.locator("#menu button", has_text=item).click()

    def setting(self, section, row=None):
        """Settings' `section` (its row named `row`, when given): open it where it is."""
        self.p.locator("dialog.settings2 .s-i", has_text=section).click()
        self.p.locator("dialog.settings2 .s-body h4", has_text=section).wait_for(timeout=5000)
        return self.p.locator("dialog.settings2 .s-row", has=self.p.locator(".s-n", has_text=row)).first if row else None

    def runmenu(self, item):
        """Pick `item` in the ▾ beside the Run of the file in front (its other ways to run: a job, a schedule)."""
        self.p.locator("#docbar .split .caret").first.click()
        self.p.locator("#menu button", has_text=item).click()

    def docmenu(self, item):
        """Pick `item` in the ⋯ menu of the file in front."""
        self.p.locator("#docbar button[aria-label=More]").click()
        self.p.locator("#menu button", has_text=item).click()

    def workspace(self, *path):
        """Click through the Workspace's folders to a file; its row."""
        ws = self.p.locator("#workspace")
        for folder in path[:-1]:
            row = ws.locator(".row", has_text=folder).first
            row.wait_for(timeout=10000)
            if row.get_attribute("aria-expanded") == "false":
                row.click()
        row = ws.locator(".row", has_text=path[-1]).first
        row.wait_for(timeout=10000)
        return row

    def tab(self):
        """The tab in front: its title, and whether it has changes not saved."""
        t = self.p.locator("#tabbar .tab.on")
        return t.locator(".tn").inner_text(), t.locator(".dirty").count() == 1

    def toast(self):
        return self.p.locator("#toast").inner_text()

    def clipboard(self):
        return self.p.evaluate("navigator.clipboard.readText()")

    def left(self):
        """Requests that went anywhere but the node that served the page."""
        home = self.url.split("/", 3)[:3]
        return [u for u in self.seen if u.split("/", 3)[:3] != home and not u.startswith(("data:", "blob:", "about:"))]

    def shot(self, folder, name):
        if folder:
            self.p.screenshot(path=os.path.join(folder, name))


def node_checks(browser, port, show):
    checks = {}
    base = f"http://127.0.0.1:{port}"
    sql(port, "CREATE TABLE people (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL, born DATE, at TIMESTAMP, amt DECIMAL(10,2))")
    sql(port, "INSERT INTO people VALUES (1, 'Ann', '1990-01-02', '2024-01-01 10:00', 1.50), (2, 'Bo', NULL, NULL, NULL), (3, 'Cy', NULL, NULL, 2.25)")
    sql(port, "CREATE SCHEMA sales")
    sql(port, "CREATE TABLE sales.orders AS SELECT 1 AS id, 10.5 AS amount")
    sql(port, "CREATE VIEW grown AS SELECT name FROM people WHERE born IS NOT NULL")
    pg = Page(browser, base + "/")
    p = pg.p
    tree = p.locator("#data")
    tree.locator(".row", has_text="people").wait_for(timeout=20000)
    fresh = until(lambda: (pg.tab(), pg.workspace("notebooks", "untitled.ipynb").locator(".dirty").count()), (("untitled.ipynb", False), 0))
    tree.locator(".row", has_text="sales").locator(".tw").click()
    tree.locator(".row", has_text="orders").wait_for(timeout=10000)
    people = tree.locator(".row[data-kind]", has_text="people")
    people.locator(".tw").click()
    names = tree.locator(".row.col:visible .nm").all_inner_texts()
    types = tree.locator(".row.col:visible .ty").all_inner_texts()
    kinds = {r.locator(".nm").inner_text(): r.get_attribute("data-kind") for r in tree.locator(".row[data-kind]").all() if r.get_attribute("data-kind") not in ("database", "schema", "group")}
    checks["a new notebook, nothing typed, has nothing to save: no dot on its tab or its row in the Workspace's notebooks folder"] = fresh == (("untitled.ipynb", False), 0)
    checks["the Data tree lists schemas, tables and views (each kind its icon), columns with SQL types and coloured type marks, a key marked, no bare numbers"] = \
        kinds == {"people": "table", "grown": "view", "orders": "table"} and names == ["id", "name", "born", "at", "amt"] and types[4] == "DECIMAL(10,2)" and types[3] == "TIMESTAMP" \
        and tree.locator(".row.col:visible .ty-i").count() == 5 and tree.locator(".row.col:visible .kk").count() == 1 \
        and not [t for t in tree.locator(".row:visible .nm").all_inner_texts() if any(w.isdigit() for w in t.split())]
    people.click()
    detail = p.locator("#details")
    rows_shown = until(lambda: detail.locator("dd").first.inner_text(), "3")
    facts = dict(zip(detail.locator("dt").all_inner_texts(), detail.locator("dd").all_inner_texts()))
    checks["a table picked shows its details: rows, key, columns (a key's and NOT NULL marked)"] = rows_shown == "3" and facts.get("Key") == "id" \
        and detail.locator(".pc").count() == 5 and "not null" in detail.locator(".pc", has_text="name").inner_text()
    detail.locator("button", has_text="Profile").click()
    profiled = until(lambda: detail.locator(".pc", has_text="born").locator(".nums").inner_text().startswith("67% null"), True)
    charts = until(lambda: (detail.locator(".pc", has_text="amt").locator(".ps svg rect").count(), detail.locator(".pc", has_text="name").locator(".bars .v").count()), (20, 3))
    amt = detail.locator(".pc", has_text="amt").inner_text()
    checks["Profile: each column's nulls, distinct values, range, and a histogram or its commonest values"] = profiled is True and "1.50 … 2.25" in amt and charts == (20, 3)
    tree.locator(".row", has_text="grown").click()
    checks["a view picked shows its definition"] = until(lambda: "born IS NOT NULL" in detail.locator(".defn").inner_text(), True) is True

    c = pg.run(0, "SELECT id, name, born, at, amt FROM people ORDER BY id")
    heads, rows = pg.grid(c)
    c.locator("thead th", has_text="id").hover()
    card = until(lambda: p.locator(".hcard").is_visible() and "BIGINT" in p.locator(".hcard").inner_text(), True)
    p.mouse.move(5, 5)
    checks["a SQL cell shows the rows, each column's type a mark and its card on hover; timestamps and decimals as written"] = heads == ["id", "name", "born", "at", "amt"] \
        and rows[0] == ["1", "Ann", "1990-01-02", "2024-01-01 10:00:00", "1.50"] and rows[1][2] == "NULL" and c.locator(".n-rows").inner_text().startswith("3 rows · ") \
        and [m.get_attribute("class") for m in c.locator("thead .ty-i").all()] == ["ty-i k-num", "ty-i k-text", "ty-i k-date", "ty-i k-time", "ty-i k-dec"] and card is True
    tree.locator(".row", has_text="orders").dblclick()  # (a table's first rows, in a new cell)
    peek = until(lambda: pg.grid(pg.cell(1))[1], [["1", "10.5"]])
    checks["double-clicking a table shows its first rows"] = peek == [["1", "10.5"]] and "sales.orders" in pg.cell(1).locator("textarea").input_value()
    put(port, "reports/q1.csv", b"a,b\n1,x\n2,y\n")
    p.click("#refresh")
    row = pg.workspace("reports", "q1.csv")
    checks["the Workspace lists the lake's files by folder, with their sizes"] = "12 B" in row.inner_text()

    long = pg.run(1, "SELECT value AS n, value % 7 AS m FROM range(0, 5000)")
    drawn = long.locator("tbody tr:not(.gap)").count()
    long.locator(".grid").evaluate("g => g.scrollTop = g.scrollHeight")
    last = until(lambda: pg.grid(long)[1][-1][0], "4999")
    long.locator("thead th", has_text="m").locator(".srt").click()
    long.locator(".grid").evaluate("g => g.scrollTop = 0")
    top = until(lambda: pg.grid(long)[1][0][1], "0")
    long.locator("thead th", has_text="m").click()  # (a header: its column, summarized in the details)
    m = until(lambda: "7 distinct" in detail.locator(".pc.on").inner_text() and detail.locator(".pc.on").inner_text(), secs=10) or ""
    checks["a long answer draws only the rows in sight, sorts by a header's button, and a header clicked is summarized in the details"] = drawn < 120 and last == "4999" and top == "0" \
        and "7 distinct" in str(m) and "0 … 6" in str(m)

    p.click("[data-add=python]")
    p.keyboard.insert_text('for i in range(2):\n    print("hello", i)\ndb.sql("SELECT count(*) AS n FROM people")')
    p.keyboard.press("Control+Enter")
    py = pg.cell(2)
    py.locator("table").wait_for(timeout=60000)
    checks["a Python cell runs on the node: what it printed, then its last expression as rows"] = py.locator(".said").inner_text() == "hello 0\nhello 1" and pg.grid(py) == [["n"], [["3"]]]
    c = pg.run(2, "x = 1\n1 / 0")
    python_error = c.locator(".err").inner_text()
    c = pg.run(1, "SELECT nope FROM people")
    sql_error = c.locator(".err").inner_text()
    checks["errors in plain words: a SQL one, and a Python one at its line in the cell"] = "nope" in sql_error and "ZeroDivisionError" in python_error and "line 2" in python_error
    c = pg.run(2, "x * 10")  # (the cell before made x, then failed: x stays, as in a notebook)
    shared = until(lambda: pg.grid(c), [["value"], [["10"]]])
    checks["Python cells share their variables (one namespace per page, as a notebook's kernel)"] = shared == [["value"], [["10"]]]
    fig = pg.run(2, "import matplotlib.pyplot as plt\nfig, ax = plt.subplots(figsize=(4, 2))\nax.plot([1, 3, 2])\nfig")
    drawn = until(lambda: fig.locator("img.fig").count() == 1 and fig.locator("img.fig").evaluate("i => i.complete && i.naturalWidth") > 100, True)
    fig_said = fig.locator(".out").inner_text()[:300]  # (why, if it drew nothing: matplotlib missing says so)
    checks["a figure a Python cell returns (matplotlib) shows as a picture"] = drawn is True and fig.locator("img.fig").get_attribute("src").startswith("data:image/png;base64,")
    p.locator("#rtabs .rtab", has_text="Variables").click()
    want = ["ax", "fig", "i", "x"] if drawn is True else ["i", "x"]  # (the figure's failing is its own check's)
    names = until(lambda: p.locator("#variables .var .nm").all_inner_texts(), want)
    x_type = p.locator("#variables .var", has_text="x").last.locator(".ty").inner_text() if names == want else ""
    p.locator("#variables button", has_text="Restart").click()
    emptied = until(lambda: p.locator("#variables .var").count(), 0)
    gone = pg.run(2, "x").locator(".err").inner_text()
    p.locator("#rtabs .rtab", has_text="Details").click()
    checks["the Variables tab lists the page's Python names with their types; Restart empties them"] = names == want and x_type.startswith("int") \
        and emptied == 0 and "NameError" in gone
    named = pg.cell(1).locator(".bar .as input")  # (a SQL cell's → name: its answer a frame in the page's Python)
    named.fill("ppl")
    named.press("Tab")
    pg.run(1, "SELECT id FROM people")
    in_python = until(lambda: pg.grid(pg.run(2, "len(ppl.to_pandas())")), [["value"], [["3"]]], 10)
    pg.run(2, "import pandas as pd\ngoals = pd.DataFrame({'who': ['Ann'], 'goal': [5]})\nlen(goals)")
    until(lambda: "goals" in p.evaluate("(pondra.state.vars || []).map(v => v.name)"), True)
    named.fill("")
    named.press("Tab")
    from_python = until(lambda: pg.grid(pg.run(1, "SELECT goal FROM goals")), [["goal"], [["5"]]], 10)
    checks["a SQL cell's answer named (→ name) is a frame in the page's Python; a SQL cell reads the page's pandas table by its name"] = \
        in_python == [["value"], [["3"]]] and from_python == [["goal"], [["5"]]]
    ta = pg.cell(0).locator("textarea")
    ta.fill("SELECT * FROM peo")
    ta.press("End")
    ta.press("Tab")  # (one name fits: it is written)
    one = ta.input_value()
    ta.fill("SELECT na")
    ta.press("End")
    ta.press("Tab")  # (several fit: a list, the table's column first)
    listed = until(lambda: p.locator("#complete").is_visible() and p.locator("#complete [role=option]").count() > 1, True)
    ta.press("Enter")
    checks["Tab completes a table's name, and lists the choices when several fit (a column first)"] = one == "SELECT * FROM people" and listed is True \
        and ta.input_value() == "SELECT name" and p.locator("#complete").is_hidden()
    pg.run(0, "SELECT id, name, born, at, amt FROM people ORDER BY id")

    live = pg.run(1, "SELECT count(*) AS n, sum(amt) AS amt FROM people")
    live.locator("label.live").click()
    started = until(lambda: live.locator(".st .dot").count() == 1 and call(port, "GET", "/stats")["live_queries"] == 1, True)
    sql(port, "INSERT INTO people (id, name, amt) VALUES (4, 'Di', 0.25)")
    updated = until(lambda: pg.grid(live)[1], [["4", "4.00"]])
    live.locator("label.live").click()
    ended = until(lambda: call(port, "GET", "/stats")["live_queries"], 0)
    checks["a live cell shows a row INSERTed over HTTP (a decimal to its scale); off, its query ends"] = started is True and updated == [["4", "4.00"]] and ended == 0

    n = pg.cells().count()
    pg.cell(0).locator("textarea").click()
    p.keyboard.press("Escape")
    # (each key waits for the page to show what the one before did: a slow runner drew a step late)
    p.keyboard.press("b")
    added = until(lambda: pg.cells().count(), n + 1)
    p.keyboard.press("d")
    p.keyboard.press("d")
    deleted = until(lambda: pg.cells().count(), n)
    p.keyboard.press("z")
    back = until(lambda: pg.cells().count(), n + 1)
    pg.cell(pg.cells().count() - 1).locator("textarea").click()
    p.keyboard.press("Escape")
    p.keyboard.press("b")  # (a cell below the last,)
    until(lambda: pg.cells().count(), n + 2)
    p.keyboard.press("m")  # (for text)
    text = pg.cell(pg.cells().count() - 1)
    until(lambda: text.get_attribute("data-kind"), "markdown")
    p.keyboard.press("Enter")
    p.keyboard.insert_text("# Findings\nSome **bold** text.")
    p.keyboard.press("Shift+Enter")
    rendered = until(lambda: text.locator(".md h1").inner_text(timeout=1000), "Findings")
    keyed = [pg.cell(i).get_attribute("data-kind") for i in range(pg.cells().count())]
    typing = p.evaluate("document.activeElement.tagName")
    checks["keys: Esc, B adds a cell, D D deletes it, Z brings it back; M makes it text; Shift+Enter renders it and starts a new cell"] = \
        (added, deleted, back) == (n + 1, n, n + 1) and rendered == "Findings" and typing == "TEXTAREA" and pg.cells().count() == n + 3
    outline = p.locator("#workspace .outline:visible .row")
    headed = until(lambda: outline.all_inner_texts(), ["Findings"])
    pg.cell(0).locator("textarea").click()
    outline.first.click()
    checks["a text cell's headings make the outline under the notebook in the Workspace, and one clicked selects its cell"] = headed == ["Findings"] \
        and until(lambda: "sel" in pg.cell(pg.cells().count() - 2).get_attribute("class"), True) is True
    pg.cell(0).locator("textarea").click()
    p.keyboard.press("Escape")
    p.keyboard.press("o")
    folded = "folded" in pg.cell(0).get_attribute("class") and pg.cell(0).locator("table").is_hidden() and pg.cell(0).locator("textarea").is_visible()
    pg.cell(0).locator(".out").click()
    shown = "folded" not in pg.cell(0).get_attribute("class") and pg.cell(0).locator("table").is_visible()
    p.keyboard.press("Escape")
    p.keyboard.press("?")
    keys = until(lambda: p.locator("dialog.settings2[open] .s-body h4").inner_text() == "Keys" and p.locator("dialog.settings2 .keys-k kbd").count() > 20, True)
    p.keyboard.press("Escape")
    checks["O hides a cell's output (its code stays) and a click shows it; ? lists every key (Settings, Keys)"] = folded and shown and keys is True

    p.fill("#nbname", "report")
    p.press("#nbname", "Enter")
    p.keyboard.press("Control+s")
    saved_now = lambda: _try(lambda: json.loads(get(port, "notebooks/report.ipynb")))
    one = until(lambda: isinstance(saved_now(), dict), True)
    row_clean = until(lambda: pg.workspace("notebooks", "report.ipynb").locator(".dirty").count(), 0)
    saved = saved_now() if one is True else None
    nb = nbformat.reads(json.dumps(saved), as_version=4) if saved else None
    valid = _try(lambda: nbformat.validate(nb) is None)
    page_cells = p.evaluate("pondra.state.cells.map(c => [c.kind, c.src])")
    as_saved = [["markdown" if c.cell_type == "markdown" else "sql" if c.source.startswith("%%sql") else "python", c.source.removeprefix("%%sql\n")] for c in nb.cells] if nb else []
    checks["a notebook saved in the lake (one file, notebooks/report.ipynb) is valid for Jupyter (nbformat), SQL cells as %%sql; its tab has no changes then"] = one is True and valid is True and as_saved == page_cells \
        and any(c.source.startswith("%%sql\n") for c in nb.cells) and until(lambda: pg.tab(), ("report.ipynb", False)) == ("report.ipynb", False) and row_clean == 0 \
        and "notebooks%2Freport.ipynb" in p.url.replace("/", "%2F") and pg.workspace("notebooks", "report.ipynb").is_visible()
    kept = lambda: json.loads(get(port, "notebooks/report.ipynb?versions"))
    pg.cell(0).locator("textarea").fill("SELECT 'changed' AS v")
    p.keyboard.press("Control+s")
    two = until(lambda: len(kept()), 2)
    pg.cell(0).locator("textarea").fill("SELECT 'not saved' AS v")
    pg.docmenu("Versions")
    dlg = p.locator("dialog.pop.wide")
    until(lambda: dlg.locator(".vlist .row").count(), 2)
    diffed = until(lambda: dlg.locator(".dl.minus").count() > 0 and dlg.locator(".dl.plus").count() > 0, True)  # (the save before now, against now)
    as_cells = _try(lambda: dlg.locator(".vcell .vhead").first.inner_text())  # (a notebook's: by cell, as its kind shows it, not its JSON)
    lit = _try(lambda: dlg.locator(".vcell .dl .k").count())
    raw = _try(lambda: dlg.locator(".vdiff").inner_text().count('"cell_type"'))
    if show:
        pg.shot(show, "console-versions.png")
    # (its changes not saved: the page's confirm is accepted, as every one here is)
    dlg.locator("button", has_text="Restore this version").click()
    reopened = until(lambda: p.evaluate("pondra.state.cells.map(c => [c.kind, c.src])"), page_cells)
    three = until(lambda: len(kept()), 3)
    if show:
        pg.shot(show, "console-restored.png")
    checks["saved twice: two versions; Versions… shows what changed since (− and +), cell by cell, highlighted, and restores the first: its tab has it again, kept as the newest"] = \
        two == 2 and diffed is True and isinstance(as_cells, str) and "Cell 1 · SQL" in as_cells and "changed" in as_cells and (lit or 0) > 0 \
        and raw == 0 and reopened == page_cells and three == 3

    with p.expect_download() as d:
        pg.docmenu("Download as .ipynb")
    downloaded = open(d.value.path()).read()
    checks["a notebook downloaded is valid for Jupyter too"] = _try(lambda: nbformat.validate(nbformat.reads(downloaded, as_version=4)) is None) is True

    up = nbformat.v4.new_notebook(cells=[nbformat.v4.new_markdown_cell("# From Jupyter\nA `code` span."), nbformat.v4.new_code_cell("%%sql\nSELECT 42 AS answer"),
                                         nbformat.v4.new_code_cell("print('from python')")])
    path = os.path.join(tempfile.mkdtemp(prefix="pondra-console-"), "from-jupyter.ipynb")
    nbformat.write(up, path)
    with p.expect_file_chooser() as chooser:
        pg.menu("Upload a file")
    chooser.value.set_files(path)
    until(lambda: p.evaluate("pondra.state.cells.map(c => c.kind)"), ["markdown", "sql", "python"])
    kinds = p.evaluate("pondra.state.cells.map(c => c.kind)")
    c = pg.run(1, "SELECT 42 AS answer")
    checks["an .ipynb uploaded opens in a tab: its text rendered (its heading in the outline), its %%sql cell a SQL cell that runs"] = kinds == ["markdown", "sql", "python"] \
        and until(lambda: p.locator("#workspace .outline:visible").text_content(), "From Jupyter") == "From Jupyter" \
        and pg.cell(0).locator(".md h1").inner_text() == "From Jupyter" and pg.grid(c) == [["answer"], [["42"]]] and p.input_value("#nbname") == "from-jupyter" \
        and pg.tab()[0] == "from-jupyter.ipynb"
    shutil.rmtree(os.path.dirname(path), ignore_errors=True)

    if show:
        p.goto("about:blank")
        p.goto(base + "/#notebook=report")  # (a link of before: the one file now)
        pg.cells().first.wait_for()
        pg.run(0, "SELECT id, name, born, amt FROM people ORDER BY id")
        p.locator("#data .row", has_text="people").click()
        p.wait_for_timeout(800)
        pg.shot(show, "console-light.png")
        dark = Page(browser, base + "/#notebook=report", "dark")
        dark.cells().first.wait_for()
        dark.run(0, "SELECT id, name, born, amt FROM people ORDER BY id")
        dark.p.wait_for_timeout(800)
        dark.shot(show, "console-dark.png")
        dark.ctx.close()
    # A flow (ADR-036): a view's details draw what it follows and what follows it, and its expectations.
    sql(port, "CREATE TABLE pay (id BIGINT, amount BIGINT)")
    sql(port, "INSERT INTO pay VALUES (1, 5), (2, -1), (3, 7)")
    sql(port, "CREATE MATERIALIZED VIEW pay_ok (CONSTRAINT positive CHECK (amount > 0) ON VIOLATION DROP ROW) AS SELECT id, amount FROM pay")
    sql(port, "CREATE MATERIALIZED VIEW pay_sum AS SELECT id % 2 AS odd, sum(amount) AS total, count(*) AS n FROM pay_ok GROUP BY 1")
    p.goto("about:blank")
    p.goto(base + "/")
    p.locator("#data .row", has_text="pay_ok").first.wait_for(timeout=20000)
    p.locator("#data .row", has_text="pay_ok").first.click()
    flow = until(lambda: p.locator("#details .flow").inner_text().split() if p.locator("#details .flow").count() else [], ["pay", "→", "pay_ok", "→", "pay_sum"])
    broke = until(lambda: "1 row broke it" in (p.locator("#details .pc", has_text="positive").inner_text() if p.locator("#details .pc", has_text="positive").count() else ""), True)
    checks["a materialized view's details draw its flow (pay → pay_ok → pay_sum) and its expectations, with the rows that broke each"] = \
        flow == ["pay", "→", "pay_ok", "→", "pay_sum"] and broke is True
    pg.shot(show, "console-flow.png")
    checks["every request went to the node; no page errors"] = pg.left() == [] and pg.errors == [] and len(pg.seen) > 10
    info = {"named": [in_python, from_python], "keyed": keyed, "figure": fig_said, "left": pg.left(), "errors": pg.errors, "sql_error": sql_error, "python_error": python_error, "kinds": kinds, "names": names, "types": types, "facts": facts,
            "heads": heads, "rows": rows[:2], "m": m}
    pg.ctx.close()
    return checks, info


def files_checks(browser, port, show):
    """Files in tabs (ADR-034): SQL and Python files run and are saved in place, data files are
    edited in a grid and saved in place (If-Match: never over someone else's change)."""
    import pyarrow as pa, pyarrow.parquet as pq
    checks = {}
    base = f"http://127.0.0.1:{port}"
    sql(port, "CREATE TABLE fx AS SELECT value AS id, 'r' || (value % 3) AS region FROM range(0, 30)")
    top = b"SELECT region, count(*) AS n\nFROM fx\nGROUP BY region\nORDER BY region"
    put(port, "scripts/top.sql", top)
    put(port, "scripts/since.sql", b"-- The first day counted\nDECLARE $since DATE = DATE '2026-01-01';\nDECLARE $top BIGINT DEFAULT 3;\nSELECT $since AS since, $top + 1 AS more;\nSELECT 7 AS seven")
    put(port, "scripts/hello.py", b'import math\nprint("pi is", round(math.pi, 4))\ndb.sql("SELECT count(*) AS n FROM fx")')
    put(port, "data/q.csv", b'id,city,amount\r\n1,Oslo,10\r\n2,"Rome, IT",20\r\n')
    put(port, "data/e.jsonl", b'{"id":1,"tag":"a"}\n{"id":2,"tag":"b"}\n')
    out = io.BytesIO()
    pq.write_table(pa.table({"a": [1, 2], "b": ["x", "y"]}), out)
    put(port, "data/p.parquet", out.getvalue())
    lake_files = call(port, "GET", "/objects")["files"]
    pg = Page(browser, base + "/")
    p = pg.p

    pg.workspace("scripts", "top.sql").dblclick()  # (two clicks: one tab)
    until(lambda: pg.tab()[0], "top.sql")
    one_tab = until(lambda: p.locator("#tabbar .tab", has_text="top.sql").count(), 1, 3)
    p.click("#runBtn")
    body = p.locator(".filedoc .pbody")
    results = until(lambda: pg.grid(body), [["region", "n"], [["r0", "10"], ["r1", "10"], ["r2", "10"]]])
    p.locator(".ptab", has_text="Messages").click()
    said = until(lambda: "3 rows" in body.inner_text() and body.inner_text(), secs=5)
    p.locator(".ptab", has_text="Plan").click()
    drawn = until(lambda: body.locator(".pgraph .pn").count() > 1, True, 10)  # (a graph of its steps)
    body.locator(".seg", has_text="Text").click()
    plan = until(lambda: "Exec" in body.inner_text() and body.inner_text(), secs=10) if drawn is True else None
    p.locator(".ptab", has_text="Results").click()
    checks["a SQL file opens in one tab (two clicks), runs, and shows its Results, Messages and Plan (a graph of its steps, and as text)"] = one_tab == 1 and results[1] == [["r0", "10"], ["r1", "10"], ["r2", "10"]] \
        and bool(said) and bool(plan)
    ed = p.locator(".filedoc .editor textarea")
    ed.focus()
    ed.press("Control+End")
    p.keyboard.insert_text("\nLIMIT 2")
    dirty = until(lambda: (pg.tab(), pg.workspace("scripts", "top.sql").locator(".dirty").count()), (("top.sql", True), 1), 5)  # (the tree: drawn again a moment after)
    p.keyboard.press("Control+s")
    saved = until(lambda: get(port, "scripts/top.sql"), top + b"\nLIMIT 2")
    clean = until(lambda: pg.tab(), ("top.sql", False))
    checks["a SQL file changed shows it (its tab, the Workspace) and Ctrl+S saves it in place"] = dirty == (("top.sql", True), 1) and saved == top + b"\nLIMIT 2" and clean == ("top.sql", False)
    put(port, "scripts/multi.sql", b"CREATE TABLE fm AS SELECT 1 AS a;\n-- its rows; this ; is a comment's\nSELECT a, 'x;y' AS s FROM fm;\nSELECT nope;\nSELECT 5")
    pg.workspace("scripts", "multi.sql").click()
    until(lambda: pg.tab()[0], "multi.sql")
    p.click("#runBtn")
    strip = p.locator(".filedoc .stmts")
    listed = lambda: strip.evaluate("s => [...s.querySelectorAll('button.stmt')].map(b => b.querySelector('b').textContent + ' ' + b.lastChild.textContent)")
    each = until(listed, ["1 done", "2 1 row", "3 failed"])
    left = strip.locator(".stmts-left").inner_text()
    strip.locator("button.stmt").nth(1).click()
    second = until(lambda: pg.grid(body), [["a", "s"], [["1", "x;y"]]])
    pg.menu("Settings")
    pg.setting("Editor and results", "A SQL file's statements").locator(".seg", has_text="The last one's").click()
    p.keyboard.press("Escape")
    p.click("#runBtn")
    last = until(lambda: body.locator(".err").count() == 1 and strip.count() == 0, True)  # (all at once: it stops at the failure too, one answer)
    ed.focus()
    p.keyboard.press("Control+a")
    p.keyboard.insert_text("SELECT 1 AS a; SELECT 2 AS b")
    p.click("#runBtn")
    only = until(lambda: pg.grid(body), [["b"], [["2"]]])
    pg.menu("Settings")
    pg.setting("Editor and results", "A SQL file's statements").locator(".seg", has_text="An answer each").click()
    p.keyboard.press("Escape")
    checks["a SQL file's statements each get an answer (split as the node splits them), up to a failure; Settings can say the last one's only"] = \
        each == ["1 done", "2 1 row", "3 failed"] and left == "1 after it not run" and second == [["a", "s"], [["1", "x;y"]]] and last is True and only == [["b"], [["2"]]]
    p.click("#newfile")
    p.locator("#menu button", has_text="New SQL file").click()
    p.keyboard.insert_text("SELECT 1 AS one")
    unsaved = until(lambda: (pg.tab(), pg.workspace("queries", "untitled.sql").locator(".dirty").count()), (("untitled.sql", True), 1))  # (in the folder it will be saved to)
    p.keyboard.press("Control+s")
    p.locator("#askDlg[open]").wait_for(timeout=5000)
    p.fill("#askIn", "scripts/one.sql")
    p.press("#askIn", "Enter")
    made = until(lambda: get(port, "scripts/one.sql"), b"SELECT 1 AS one")
    checks["a new SQL file asks for its path when first saved"] = made == b"SELECT 1 AS one" and until(lambda: pg.tab(), ("one.sql", False)) == ("one.sql", False)
    checks["a new SQL file shows it is not saved (its tab, its row in the folder it will be saved to) until Ctrl+S saves it"] = unsaved == (("untitled.sql", True), 1) \
        and until(lambda: (pg.workspace("scripts", "one.sql").locator(".dirty").count(), p.locator("#workspace .row", has_text="untitled.sql").count()), (0, 0)) == (0, 0)

    put(port, "scripts/by_region.sql", b"SELECT count(*) AS n, $region AS region FROM fx WHERE region = $region")
    pg.workspace("scripts", "by_region.sql").click()
    until(lambda: pg.tab()[0], "by_region.sql")
    bar = p.locator(".filedoc .params")
    shown = until(lambda: bar.is_visible() and bar.locator(".param span").all_inner_texts(), ["$region"], 5)
    p.wait_for_function("document.activeElement?.tagName === 'TEXTAREA'")  # (the file open: its editor has the keys, so a value typed sooner could go there)
    bar.locator("input").fill("r1")
    bar.locator("input").press("Enter")
    bound = until(lambda: pg.grid(body), [["n", "region"], [["10", "r1"]]])
    pg.runmenu("Run as a job")
    jobs = p.locator("#runs .run-item", has_text="by_region.sql")
    ran = until(lambda: jobs.count() > 0 and "failed" not in jobs.first.inner_text() and "running" not in jobs.first.inner_text(), True, 30)
    pg.runmenu("Schedule")
    p.locator("#askDlg[open]").wait_for(timeout=5000)
    p.fill("#askIn", "1 hour")
    p.press("#askIn", "Enter")
    task = until(lambda: sql(port, "SELECT name, schedule, statement FROM pondra.tasks"), [{"name": "scripts_by_region", "schedule": "1 hour", "statement": "CALL run('scripts/by_region.sql', region => 'r1')"}], 10)
    job = p.locator("#jobs .job", has_text="scripts_by_region")
    listed = until(lambda: job.count(), 1, 10)
    cadence = job.locator(".cad").inner_text() if listed == 1 else None
    job.locator("button[aria-label$='more']").click()
    p.locator("#menu button", has_text="Drop it").click()
    dropped = until(lambda: sql(port, "SELECT count(*) AS n FROM pondra.tasks"), [{"n": 0}], 10)
    gone = until(lambda: p.locator("#jobs .job").count(), 0, 10)
    p.locator("#rtabs .rtab", has_text="Details").click()
    checks["a SQL file's $names each get an input, bound on the node; its Run ▾ runs it as a job (History shows it) and schedules it (Jobs shows it, apart from History, and drops it)"] = \
        shown == ["$region"] and bound == [["n", "region"], [["10", "r1"]]] and ran is True and isinstance(task, list) and len(task) == 1 and listed == 1 and cadence == "every 1 hour" and dropped == [{"n": 0}] and gone == 0
    pg.workspace("scripts", "since.sql").click()
    until(lambda: pg.tab()[0], "since.sql")
    params = lambda: bar.evaluate("b => [...b.querySelectorAll('.param')].map(l => [l.querySelector('span').textContent, l.querySelector('i')?.textContent, l.querySelector('input').type, l.querySelector('input').placeholder, l.title])")
    typed = until(params, [["$since", "date", "date", "DATE '2026-01-01'", "The first day counted\nDefault: DATE '2026-01-01'"], ["$top", "bigint", "text", "3", "Default: 3"]], 5)
    lit = p.locator(".filedoc pre.hl span.nu", has_text="$since").count() > 0
    p.wait_for_function("document.activeElement?.tagName === 'TEXTAREA'")
    ed.focus()
    p.keyboard.press("Control+Home")
    p.keyboard.press("Control+Enter")
    answers = lambda: strip.evaluate("s => [...s.querySelectorAll('button.stmt')].map(b => b.querySelector('b').textContent + ' ' + b.lastChild.textContent)")
    whole = until(lambda: strip.count() and answers(), ["1 done", "2 done", "3 1 row", "4 1 row"])
    strip.locator("button.stmt").nth(2).click()
    defaults = until(lambda: pg.grid(body), [["since", "more"], [["2026-01-01", "4"]]])
    bar.locator("input").nth(1).fill("10")
    bar.locator("input").nth(1).press("Enter")
    strip.locator("button.stmt").nth(2).click()
    given = until(lambda: pg.grid(body), [["since", "more"], [["2026-01-01", "11"]]])
    ed.focus()
    p.keyboard.press("Control+End")
    p.keyboard.press("Control+Shift+Enter")  # (the caret in the last statement: that one alone)
    alone = until(lambda: strip.count() == 0 and pg.grid(body), [["seven"], [["7"]]])
    ed.evaluate("t => t.setSelectionRange(t.value.indexOf('AS more;') + 8, t.value.indexOf('AS more;') + 8)")  # (just after its ;: the statement before)
    p.keyboard.press("Control+Shift+Enter")
    after = until(lambda: pg.grid(body), [["since", "more"], [["2026-01-01", "11"]]])
    box, cw = ed.bounding_box(), p.evaluate("(() => { const c = document.createElement('canvas').getContext('2d'); c.font = '13px ' + getComputedStyle(document.body).getPropertyValue('--mono'); return c.measureText('0').width; })()")
    p.mouse.move(box["x"] + 14 + cw * 9.5, box["y"] + 9 + 21 * 1.5)  # (over `$since`, line 2)
    hovered = until(lambda: ed.get_attribute("title") or "", "$since DATE = DATE '2026-01-01'\nThe first day counted\nNow: 2026-01-01 (date)", 5)
    p.locator("#rtabs .rtab", has_text="Variables").click()
    shown_vars = until(lambda: p.locator("#variables .vhead").count() == 1 and p.locator("#variables .var .nm").all_inner_texts()[-2:], ["$since", "$top"], 10)
    p.locator("#rtabs .rtab", has_text="Details").click()
    ed.focus()
    p.keyboard.press("Control+End")
    p.keyboard.insert_text(" + $t")
    p.keyboard.press("Control+Space")
    completes = until(lambda: p.locator("#complete:not([hidden]) div").all_inner_texts()[:1], ["$top\nvariable"], 5)
    p.keyboard.press("Escape")
    checks["a SQL file's DECLAREs are its parameters (type, default, what the comment above says; a date picks a date), $names highlighted; a value given replaces the default; Ctrl+Shift+Enter runs the statement at the caret (just after its ; too); hovering a $name says it; Variables lists SQL's; $ completes"] = \
        typed == [["$since", "date", "date", "DATE '2026-01-01'", "The first day counted\nDefault: DATE '2026-01-01'"], ["$top", "bigint", "text", "3", "Default: 3"]] and lit \
        and whole == ["1 done", "2 done", "3 1 row", "4 1 row"] and defaults == [["since", "more"], [["2026-01-01", "4"]]] and given == [["since", "more"], [["2026-01-01", "11"]]] \
        and alone == [["seven"], [["7"]]] and after == [["since", "more"], [["2026-01-01", "11"]]] and hovered.startswith("$since DATE") and shown_vars == ["$since", "$top"] and completes == ["$top\nvariable"]
    var_info = {"typed": typed, "whole": whole, "defaults": defaults, "given": given, "alone": alone, "after": after, "hovered": hovered, "vars": shown_vars, "completes": completes}
    pg.workspace("scripts", "hello.py").click()
    until(lambda: pg.tab()[0], "hello.py")
    p.locator("#docbar button", has_text="Run file").click()
    log = p.locator(".filedoc .log")
    printed = until(lambda: "pi is 3.1416" in log.inner_text(), True, 60)
    answered = until(lambda: pg.grid(log.locator(".entry").last), [["n"], [["30"]]])
    repl = p.locator(".filedoc .prompt input")
    repl.fill("y = 41")
    repl.press("Enter")
    until(lambda: log.locator(".entry").count(), 2)
    until(lambda: log.locator(".entry .wait").count(), 0)
    repl.fill("y + 1")
    repl.press("Enter")
    typed = until(lambda: pg.grid(log.locator(".entry").last), [["value"], [["42"]]])
    checks["a Python file runs in its console (what it printed, its answer), and a line typed there runs in the same Python"] = printed is True \
        and answered == [["n"], [["30"]]] and typed == [["value"], [["42"]]]

    pg.workspace("data", "q.csv").click()
    doc = p.locator(".datadoc")
    shown = until(lambda: pg.grid(doc), [["id", "city", "amount"], [["1", "Oslo", "10"], ["2", "Rome, IT", "20"]]])
    doc.locator("tbody tr:not(.gap)").nth(0).locator("td").nth(3).dblclick()
    p.keyboard.press("Control+a")
    p.keyboard.type("15")
    p.keyboard.press("Enter")
    doc.locator("button", has_text="Add row").click()
    p.keyboard.type("3")
    p.keyboard.press("Tab")
    p.keyboard.type("Paris")
    p.keyboard.press("Tab")
    p.keyboard.type("30")
    p.keyboard.press("Enter")
    marked = doc.locator("td.chg").count() >= 1 and doc.locator("tr.new").count() == 1 and pg.tab() == ("q.csv", True)
    p.keyboard.press("Control+s")
    want = b'id,city,amount\r\n1,Oslo,15\r\n2,"Rome, IT",20\r\n3,Paris,30\r\n'
    written = until(lambda: get(port, "data/q.csv"), want)
    read = until(lambda: sql(port, f"SELECT count(*) AS n, sum(amount) AS s FROM read_csv('{lake_files}data/q.csv')"), [{"n": 3, "s": 65}])
    checks["a CSV file is edited in a grid (a cell changed, a row added: both marked) and saved in place, its untouched lines as they were; the node reads the new rows"] = \
        shown[1] == [["1", "Oslo", "10"], ["2", "Rome, IT", "20"]] and marked and written == want and read == [{"n": 3, "s": 65}] and until(lambda: pg.tab(), ("q.csv", False)) == ("q.csv", False)
    theirs = b"id,city,amount\r\n9,Else,1\r\n"
    put(port, "data/q.csv", theirs, version(port, "data/q.csv"))
    doc.locator("tbody tr:not(.gap)").nth(0).locator("td").nth(2).dblclick()
    p.keyboard.press("Control+a")
    p.keyboard.type("Bergen")
    p.keyboard.press("Enter")
    p.keyboard.press("Control+s")
    refused = until(lambda: "saved by someone else" in pg.toast(), True)
    checks["saving over someone else's change is refused (If-Match: 412), and theirs is kept"] = refused is True and get(port, "data/q.csv") == theirs \
        and pg.tab() == ("q.csv", True)
    p.locator("#docbar button", has_text="Discard").click()  # (theirs, read again)
    checks["Discard reads the file again: theirs"] = until(lambda: pg.grid(doc)[1], [["9", "Else", "1"]]) == [["9", "Else", "1"]] and until(lambda: pg.tab(), ("q.csv", False)) == ("q.csv", False)

    pg.workspace("data", "e.jsonl").click()
    doc = p.locator(".datadoc")
    until(lambda: pg.grid(doc)[0], ["id", "tag"])
    doc.locator("tbody tr:not(.gap)").nth(0).locator("td").nth(2).dblclick()
    p.keyboard.press("Control+a")
    p.keyboard.type("z")
    p.keyboard.press("Enter")
    p.keyboard.press("Control+s")
    lines = until(lambda: [json.loads(l) for l in get(port, "data/e.jsonl").decode().splitlines()], [{"id": 1, "tag": "z"}, {"id": 2, "tag": "b"}])
    checks["a JSONL file is edited and saved, a JSON object a line"] = lines == [{"id": 1, "tag": "z"}, {"id": 2, "tag": "b"}]

    pg.workspace("data", "p.parquet").click()
    doc = p.locator(".datadoc")
    rows_ro = until(lambda: pg.grid(doc)[1], [["1", "x"], ["2", "y"]])
    doc.locator("tbody tr:not(.gap)").nth(0).locator("td").nth(1).dblclick()
    checks["a Parquet file opens read-only (it says so; a cell does not edit)"] = rows_ro == [["1", "x"], ["2", "y"]] and "read-only" in doc.locator(".note").inner_text() \
        and doc.locator("input.celled").count() == 0
    if show:
        pg.workspace("data", "q.csv").click()
        p.wait_for_timeout(500)
        pg.shot(show, "console-data-file.png")
    checks["files: every request went to the node; no page errors"] = pg.left() == [] and pg.errors == []
    more, folders = folders_checks(browser, port, show)
    checks.update(more)
    # The editor (round 29's review): an alias `c` is a name, not a comment; `alias.` + Tab lists that table's columns.
    lit = p.evaluate("""() => import('/console/editor.js').then(m => m.highlighted("SELECT c.x FROM fx c WHERE c.id = 1 /* c */", 'sql'))""")
    p.click("#tabbar .newtab")
    p.locator("#menu button", has_text="New SQL file").click()
    p.keyboard.insert_text("SELECT o. FROM fx o")
    for _ in range(len(" FROM fx o")):
        p.keyboard.press("ArrowLeft")
    p.keyboard.press("Tab")
    offered = until(lambda: p.locator("#complete").is_visible() and p.locator("#complete div span:first-child").all_inner_texts(), ["id", "region"], 5)
    p.keyboard.press("Escape")
    checks["the editor: an alias called c is a name, not a comment's start; after o. Tab lists the columns of the table o names"] = \
        '<span class="k">FROM</span>' in lit and '<span class="k">WHERE</span>' in lit and '<span class="c">/* c */</span>' in lit and offered == ["id", "region"]
    info_editor = {"highlighted": lit, "offered": offered}
    info = {"editor": info_editor, "variables": var_info, "task": task, "bound": bound, "results": results, "said": said, "each": each, "not run": left, "second": second, "last": last, "only": only, "plan": plan, "dirty": dirty, "saved": saved, "clean": clean, "shown": shown, "errors": pg.errors, "left": pg.left(), "toast": pg.toast(), "folders": folders}
    pg.ctx.close()
    return checks, info


def folders_checks(browser, port, show):
    """The Workspace's folders (a zero-byte `.folder` marker nobody sees), the ⋯ on each of its rows,
    notebooks in any folder (one plain file saved in place), Delete folder, Upload a file."""
    checks = {}
    base = f"http://127.0.0.1:{port}"
    put(port, "notebooks/seed/20240101T000000Z.ipynb", nbformat.writes(nbformat.v4.new_notebook()).encode())  # (so there is a notebooks folder)
    pg = Page(browser, base + "/")
    p = pg.p
    ws = p.locator("#workspace")
    asked = []  # (what the page asked to confirm)
    p.on("dialog", lambda d: asked.append(d.message))
    in_lake = lambda folder: [(r["path"], r["size"]) for r in sql(port, f"SELECT path, size FROM files('{folder}/') ORDER BY path")]
    folder = lambda name: ws.locator(".row[aria-expanded]", has_text=name).first
    kids = lambda name: folder(name).locator("xpath=../div[contains(@class,'kids')]")
    items = lambda: p.locator("#menu:not([hidden]) button .lb").all_inner_texts()
    palette = lambda q: (p.keyboard.press("Control+k"), p.fill("#palIn", q), p.locator("#palList .pi .nm").all_inner_texts(), p.keyboard.press("Escape"))[2]
    ws.locator(".row").first.wait_for(timeout=20000)

    def make(name, via):
        """Make a folder called `name`: `via` opens a menu that has New folder."""
        via()
        p.locator("#menu button", has_text="New folder").click()
        p.locator("#askDlg[open]").wait_for(timeout=5000)
        p.fill("#askIn", name)
        p.press("#askIn", "Enter")

    def more_of(row):
        """Open a row's ⋯ menu (on hover), and what it lists."""
        row.hover()
        row.locator("button.more").click()
        return items()

    make("projects", lambda: p.click("#newfile"))
    marker = until(lambda: in_lake("projects"), [("files/projects/.folder", 0)])
    shown = until(lambda: folder("projects").count(), 1)
    p.reload()
    again = until(lambda: folder("projects").count(), 1)
    folder("projects").click()
    empty = kids("projects").locator(".row").count() == 0 and ".folder" not in ws.text_content()
    checks["New folder in the Workspace's + makes a folder (a zero-byte .folder marker in the lake) that shows, empty and without the marker, and is there after a reload"] = \
        marker == [("files/projects/.folder", 0)] and shown == 1 and again == 1 and empty
    make("archive", lambda: p.click("#tabbar .newtab"))
    viaTab = until(lambda: in_lake("archive"), [("files/archive/.folder", 0)])
    make("2024", lambda: folder("projects").click(button="right"))
    inside = until(lambda: in_lake("projects"), [("files/projects/.folder", 0), ("files/projects/2024/.folder", 0)])
    make("projects", lambda: p.click("#newfile"))
    twice = until(lambda: "there already" in pg.toast(), True)
    make("a/../b", lambda: p.click("#newfile"))
    dots = until(lambda: "Not a folder name" in pg.toast(), True)
    checks["New folder from the tab bar's + and from a folder's menu (New folder here, inside it) makes one; a name that exists, or has a .. in it, is refused"] = \
        viaTab == [("files/archive/.folder", 0)] and inside == [("files/projects/.folder", 0), ("files/projects/2024/.folder", 0)] and twice is True and dots is True \
        and in_lake("a") == [] and in_lake("b") == [] and folder("2024").count() == 1

    p.mouse.move(700, 600)
    p.evaluate("document.activeElement?.blur()")
    row, bar = folder("archive"), folder("archive").locator("button.more")
    away = bar.is_visible()
    row.hover()
    hover = bar.is_visible()
    named = bar.get_attribute("aria-label")
    by_button = more_of(row)
    p.keyboard.press("Escape")
    row.click(button="right")
    by_click = items()
    p.keyboard.press("Escape")
    want = ["New notebook here", "New SQL file here", "New Python file here", "New folder here", "Upload a file here…", "Delete folder…"]
    checks["a folder's ⋯ (a button with a label) shows on hover, and its menu, the same as a right-click's, has New notebook, SQL file, Python file and folder here, Upload a file here, Delete folder"] = \
        away is False and hover is True and named == "archive: more" and by_button == want and by_click == want
    put(port, "misc/blob.bin", b"\x00\x01\x02")
    put(port, "misc/old.txt", b"old")
    p.click("#refresh")
    file_row = pg.workspace("misc", "blob.bin")
    file_row.click()  # (picked: its details show, its row stays marked)
    until(lambda: "on" in (file_row.get_attribute("class") or "").split(), True)
    p.evaluate("document.activeElement?.blur()")
    p.mouse.move(700, 600)
    picked = file_row.locator("button.more").is_visible()
    other = pg.workspace("misc", "old.txt")
    p.mouse.move(700, 600)
    hidden = other.locator("button.more").is_visible()
    other.focus()
    focused = other.locator("button.more").is_visible()
    file_menu = more_of(other)
    p.keyboard.press("Escape")
    other.click(button="right")
    checks["a file's ⋯ shows on hover, on focus and on the row picked, and opens the menu a right-click does"] = picked is True and hidden is False and focused is True \
        and file_menu == items() and "Rename…" in file_menu and "Delete…" in file_menu
    p.keyboard.press("Escape")

    folder("projects").click(button="right")
    p.locator("#menu button", has_text="New notebook here").click()
    mine = lambda name: kids("projects").locator(".row", has_text=name)
    fresh = until(lambda: (pg.tab(), mine("untitled.ipynb").locator(".dirty").count()), (("untitled.ipynb", False), 0))
    pg.cell(0).locator("textarea").fill("SELECT 7 AS seven")
    typed = until(lambda: (pg.tab(), mine("untitled.ipynb").locator(".dirty").count(), p.locator("#saveBtn").count()), (("untitled.ipynb", True), 1, 1))
    p.fill("#nbname", "analysis")
    p.press("#nbname", "Enter")
    p.keyboard.press("Control+s")
    want_files = sorted(["files/projects/.folder", "files/projects/2024/.folder", "files/projects/analysis.ipynb"])
    in_place = until(lambda: sorted(x[0] for x in in_lake("projects")), want_files)
    text = until(lambda: "seven" in get(port, "projects/analysis.ipynb").decode(), True) and get(port, "projects/analysis.ipynb").decode()
    valid = _try(lambda: nbformat.validate(nbformat.reads(text, as_version=4)) is None)
    clean = until(lambda: (pg.tab(), mine("analysis.ipynb").locator(".dirty").count()), (("analysis.ipynb", False), 0))
    checks["a new notebook in a folder: no dot until something is typed, then its tab and row have the dot and Save shows; Ctrl+S saves it in place, as projects/analysis.ipynb, valid for Jupyter"] = \
        fresh == (("untitled.ipynb", False), 0) and typed == (("untitled.ipynb", True), 1, 1) and in_place == want_files and valid is True and clean == (("analysis.ipynb", False), 0) and in_lake("notebooks/analysis") == [] \
        and "file=projects%2Fanalysis.ipynb" in p.url and p.locator("#docbar .crumb").first.inner_text() == "projects/"
    p.locator("#docbar button[aria-label=More]").click()
    nb_menu = items()
    p.keyboard.press("Escape")
    p.locator("#docbar .split .caret").first.click()
    nb_run = items()
    p.keyboard.press("Escape")
    checks["a notebook anywhere offers its versions, a job and a schedule (its ⋯ and its Run's ▾)"] = "Download as .ipynb" in nb_menu and "Run all" in nb_run \
        and {"Versions…", "Run as a job", "Schedule…"} <= set(nb_menu + nb_run)

    both = palette("projects/")
    every = palette("")
    only = palette(".folder")
    checks["the .folder marker is in no list: not the tree, not Ctrl+K (empty, or the folder's name, or .folder searched)"] = "projects/analysis.ipynb" in both \
        and not [x for x in both + every + only if x.endswith(".folder")] and ".folder" not in ws.text_content()
    p.reload()
    tab = until(lambda: p.locator("#tabbar .tab", has_text="analysis.ipynb").count(), 1)
    p.locator("#tabbar .tab", has_text="analysis.ipynb").click()
    back = until(lambda: pg.cell(0).locator("textarea").input_value(), "SELECT 7 AS seven")
    pg.cell(0).locator("textarea").fill("SELECT 8 AS eight")
    until(lambda: pg.tab(), ("analysis.ipynb", True))
    p.keyboard.press("Control+s")
    eight = until(lambda: "eight" in get(port, "projects/analysis.ipynb").decode(), True)
    checks["a notebook saved in a folder opens again after a reload, and Ctrl+S saves it over itself (still the one file)"] = tab == 1 and back == "SELECT 7 AS seven" and eight is True \
        and until(lambda: pg.tab(), ("analysis.ipynb", False)) == ("analysis.ipynb", False) and sorted(x[0] for x in in_lake("projects")) == want_files
    theirs = nbformat.writes(nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("%%sql\nSELECT 'theirs'")]))
    put(port, "projects/analysis.ipynb", theirs.encode(), version(port, "projects/analysis.ipynb"))
    pg.cell(0).locator("textarea").fill("SELECT 9 AS nine")
    p.keyboard.press("Control+s")
    refused = until(lambda: "saved by someone else" in pg.toast(), True)
    checks["a notebook saved in place is refused over someone else's change (If-Match: 412), and theirs is kept"] = refused is True and "theirs" in get(port, "projects/analysis.ipynb").decode() \
        and pg.tab() == ("analysis.ipynb", True)

    folder("notebooks").click(button="right")
    p.locator("#menu button", has_text="New notebook here").click()
    pg.cell(0).locator("textarea").fill("SELECT 1 AS one")
    p.fill("#nbname", "versioned")
    p.press("#nbname", "Enter")
    p.keyboard.press("Control+s")
    mine_v = until(lambda: _try(lambda: len(json.loads(get(port, "notebooks/versioned.ipynb?versions")))), 1)
    checks["New notebook here in notebooks: one file, notebooks/versioned.ipynb, its save kept as a version"] = mine_v == 1 and "SELECT 1 AS one" in get(port, "notebooks/versioned.ipynb").decode() \
        and in_lake("notebooks/versioned/") == [] and not [x for x in in_lake("projects") if "versioned" in x[0]]

    tmp = tempfile.mkdtemp(prefix="pondra-console-")
    csv, nb = os.path.join(tmp, "up.csv"), os.path.join(tmp, "up.ipynb")
    open(csv, "w").write("a,b\n1,2\n")
    nbformat.write(nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("%%sql\nSELECT 1 AS one")]), nb)
    row = folder("projects")
    row.hover()
    row.locator("button.more").click()
    with p.expect_file_chooser() as chooser:
        p.locator("#menu button", has_text="Upload a file here").click()
    chooser.value.set_files([csv, nb])
    put_in = until(lambda: sorted(x[0] for x in in_lake("projects") if "/up." in x[0]), ["files/projects/up.csv", "files/projects/up.ipynb"])
    pg.workspace("projects", "up.ipynb").click()
    opened = until(lambda: pg.tab(), ("up.ipynb", False))
    plain = p.locator("#docbar .crumb").first.inner_text() == "projects/" and p.input_value("#nbname") == "up"
    shutil.rmtree(tmp, ignore_errors=True)
    checks["Upload a file here puts a CSV and an .ipynb in that folder; the .ipynb, clicked, opens as a notebook saved in place"] = put_in == ["files/projects/up.csv", "files/projects/up.ipynb"] \
        and opened == ("up.ipynb", False) and plain

    put(port, "trash/a.txt", b"a")
    put(port, "trash/sub/b.csv", b"x\n1\n")
    put(port, "trash/.folder", b"")
    p.click("#refresh")
    folder("trash").wait_for(timeout=10000)
    asked.clear()
    more_of(folder("trash"))
    p.locator("#menu button", has_text="Delete folder").click()
    said = until(lambda: "Deleted trash" in pg.toast(), True)  # (the page asks first: its dialog is answered while this waits on the page)
    emptied = until(lambda: in_lake("trash"), [])
    gone = until(lambda: folder("trash").count(), 0)
    checks["Delete folder asks (with how many files), then deletes every file under it, the marker too, and the folder goes from the tree"] = said is True and emptied == [] and gone == 0 \
        and any("2 files" in m and "trash" in m for m in asked)

    rename = pg.workspace("misc", "old.txt")
    rename.click()
    until(lambda: pg.tab()[0], "old.txt")
    more_of(rename)
    p.locator("#menu button", has_text="Rename").click()
    p.locator("#askDlg[open]").wait_for(timeout=5000)
    p.fill("#askIn", "misc/new.txt")
    p.press("#askIn", "Enter")
    moved = until(lambda: sorted(x[0] for x in in_lake("misc")), ["files/misc/blob.bin", "files/misc/new.txt"])
    titles = until(lambda: "new.txt" in p.locator("#tabbar .tab .tn").all_inner_texts() and "old.txt" not in p.locator("#tabbar .tab .tn").all_inner_texts(), True)
    checks["a file renamed from its ⋯ moves its open tab to the new name"] = moved == ["files/misc/blob.bin", "files/misc/new.txt"] and titles is True and pg.tab()[0] == "new.txt"

    p.locator('#left button[aria-label="Workspace: more"]').click()
    group = items()
    p.keyboard.press("Escape")
    p.locator('#left button[aria-label="Data: more"]').click()
    data = items()
    p.keyboard.press("Escape")
    p.click("#newfile")
    plus = items()
    p.keyboard.press("Escape")
    top = p.locator("#moreBtn").is_hidden()  # (the top bar's ⋯: only an extension's actions)
    cmds = palette("upload")
    w = Page(browser, base + "/")
    w.p.locator("#tabbar .tab").first.click(button="middle")
    welcome = until(lambda: w.p.locator(".welcome .btn").all_inner_texts(), ["New notebook", "New SQL file", "New Python file", "New folder", "Upload a file…"])
    w.ctx.close()
    checks["the Workspace's ⋯ lists no New item (Data's keeps Refresh); the +, Ctrl+K and the welcome page say Upload a file…; the top bar has no ⋯ of its own"] = \
        not [x for x in group if x.startswith("New")] and "Move to the right pane" in group and "Refresh" in data \
        and plus == ["New notebook", "New SQL file", "New Python file", "New folder", "Upload a file…"] and top is True \
        and cmds.count("Upload a file…") == 1 and not [x for x in cmds + welcome if "Open an" in x] and welcome == ["New notebook", "New SQL file", "New Python file", "New folder", "Upload a file…"]
    if show:
        pg.menu("Settings")
        pg.setting("Layout", "The left pane").locator(".seg", has_text="Workspace first").click()
        p.keyboard.press("Escape")
        folder("projects").click(button="right")
        p.locator("#menu button", has_text="New SQL file here").click()
        p.keyboard.insert_text("SELECT 1")
        p.wait_for_function("!document.querySelector('#toast.on')", timeout=15000)  # (the last toast gone)
        folder("projects").hover()
        p.wait_for_timeout(300)
        pg.shot(show, "console-workspace.png")
    checks["folders: every request went to the node; no page errors"] = pg.left() == [] and pg.errors == []
    info = {"shown": shown, "made": [viaTab, inside, twice, dots], "menus": {"folder": by_button, "file": file_menu, "notebook": nb_menu, "notebook run": nb_run, "group": group, "data": data, "plus": plus, "top": top, "palette": cmds}, "asked": asked, "deleted": [said, emptied, gone],
            "toast": pg.toast(), "errors": pg.errors, "left": pg.left(), "welcome": welcome}
    pg.ctx.close()
    return checks, info


def grid_checks(browser, port, show):
    """The grid (ADR-034 §5): a spreadsheet's selection, keys, copy, filter and sort."""
    checks = {}
    pg = Page(browser, f"http://127.0.0.1:{port}/")
    p = pg.p
    pg.cells().first.wait_for(timeout=20000)
    c = pg.run(0, "SELECT value AS n, value % 3 AS m, 'r' || value AS s FROM range(0, 10)")
    cell = lambda r, col: c.locator("tbody tr:not(.gap)").nth(r).locator("td:not(.i)").nth(col)
    cell(1, 0).click()
    one = (c.locator("tr.cur").count(), c.locator("tr.cur td.i").inner_text(), c.locator("td.act").inner_text())
    cell(3, 1).click(modifiers=["Shift"])
    edges = tuple(c.locator(f"td.{k}").count() for k in ("in", "et", "eb", "el", "er"))
    total = c.locator(".sum").inner_text()
    p.keyboard.press("Control+c")
    copied = until(pg.clipboard, "1\t1\n2\t2\n3\t0", 5)
    p.keyboard.press("Control+Shift+c")
    headed = until(pg.clipboard, "n\tm\n1\t1\n2\t2\n3\t0", 5)
    checks["a click lights a cell and its row; Shift+click a range, one outline round it, summed; Ctrl+C copies it (Shift: with the headers)"] = \
        one == (1, "2", "1") and edges == (6, 2, 2, 3, 3) and "9" in total and copied == "1\t1\n2\t2\n3\t0" and headed == "n\tm\n1\t1\n2\t2\n3\t0"
    p.keyboard.press("ArrowDown")
    moved = (c.locator("td.in").count(), c.locator("tr.cur td.i").inner_text(), c.locator("td.act").inner_text())
    p.keyboard.press("Shift+ArrowRight")
    grown = c.locator("td.in").count()
    p.keyboard.press("Control+a")
    everything = c.locator("td.in").count()
    checks["the keys move the cell (Shift: grow the range; Ctrl+A: everything)"] = moved == (1, "5", "1") and grown == 2 and everything == 30
    cell(0, 1).click()
    cell(0, 1).click(button="right")  # (in the selection: the menu acts on it)
    p.locator("#menu button", has_text="Filter to these values").click()
    kept = until(lambda: [r[0] for r in pg.grid(c)[1]], ["0", "3", "6", "9"])
    chip = c.locator(".chip-f").is_visible()
    c.locator(".chip-f .x").click()
    back = until(lambda: len(pg.grid(c)[1]), 10)
    checks["the menu filters to a selection's values (a chip says so, and clears it)"] = kept == ["0", "3", "6", "9"] and chip and back == 10
    srt = c.locator("thead th", has_text="n").first.locator(".srt")
    srt.click()
    srt.click()  # (again: the other way)
    down = until(lambda: pg.grid(c)[1][0][0], "9")
    th = c.locator("thead th", has_text="s")
    th.hover()
    card = until(lambda: p.locator(".hcard").is_visible() and "VARCHAR" in p.locator(".hcard").inner_text(), True)
    hb, cb = th.bounding_box(), p.locator(".hcard").bounding_box()
    above = cb is not None and cb["y"] + cb["height"] <= hb["y"] + hb["height"] / 2  # (the pointer is at the header's middle)
    checks["a header's button sorts (again: the other way); its card tells its type, above the pointer (it hides no rows)"] = down == "9" and card is True and above
    # Pages (round 29): an answer of more rows than come at once turns its pages, kept on the node.
    p.keyboard.press("Escape")
    p.keyboard.press("b")
    big = pg.run(1, "SELECT value AS n FROM range(0, 25000)")
    rng = lambda: big.locator(".pg-r").inner_text()
    first = (rng(), [r[0] for r in pg.grid(big)[1][:1]], big.locator(".n-rows").inner_text().split(" · ")[0])
    big.locator(".pg-r").click()
    p.locator("#menu button", has_text="The last page").click()
    third = until(lambda: (rng(), pg.grid(big)[1][0][0]), ("20,001–25,000", "20000"))
    big.locator(".gt tbody td[data-c]").first.click()
    p.keyboard.press("Alt+PageUp")
    second = until(lambda: (rng(), pg.grid(big)[1][0][0]), ("10,001–20,000", "10000"))
    big.locator(".pages .pg.back").click()
    back = until(lambda: (rng(), big.locator(".pages .pg.back").is_disabled()), ("1–10,000", True))
    checks["an answer of more than 10,000 rows turns its pages (1–10,000 ▾ ‹ ›, the last page, Alt+Page Up): the node's rows, numbered on"] = \
        first == ("1–10,000", ["0"], "25,000 rows") and third == ("20,001–25,000", "20000") and second == ("10,001–20,000", "10000") and back == ("1–10,000", True)
    checks["grid: no page errors"] = pg.errors == []
    info = {"one": one, "edges": edges, "total": total, "copied": copied, "headed": headed, "moved": moved, "kept": kept, "pages": [first, third, second], "errors": pg.errors}
    pg.ctx.close()
    return checks, info


def work_checks(browser, port, show):
    """Round 29's second list (the owner's): tabs pinned and scrolled, Format of the selection or
    the file, Markdown drawn (cells and .md files), a SQL cell's Chart and Plan kept with its notebook."""
    checks = {}
    base = f"http://127.0.0.1:{port}"
    sql(port, "CREATE TABLE IF NOT EXISTS wk AS SELECT value AS id, 'r' || (value % 3) AS region, value * 1.5 AS amount FROM range(0, 30)")
    put(port, "wk/a.sql", b"select region,count(*) as n from wk group by region")
    md = b"# Title\n\nSome **bold**, *italic*, ~~gone~~ and `code`; [a query](a.sql), [the web](https://example.org).\n\n- [x] done\n- [ ] to do\n  1. nested\n\n| a | b |\n|---|--:|\n| 1 | 2 |\n\n![a picture](p.png)\n\n```sql\nSELECT 1\n```\n"
    put(port, "wk/notes.md", md)  # (its links and pictures read from its own folder, wk/)
    import struct, zlib
    chunk = lambda k, d: struct.pack(">I", len(d)) + k + d + struct.pack(">I", zlib.crc32(k + d))
    png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 2, 2, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(b"\x00" + b"\x0b\x7f\x6a" * 2 + b"\x00" + b"\x0b\x7f\x6a" * 2)) + chunk(b"IEND", b"")
    put(port, "wk/p.png", png)
    pg = Page(browser, base + "/")
    p = pg.p
    pg.cells().first.wait_for(timeout=20000)

    # Tabs: pinned at the left (kept after a reload), not closed with the others; many scroll, ⌄ lists them all
    for i in range(10):
        pg.menu("New SQL file")
    tabs = p.locator("#tabbar .tab")
    tabs.nth(3).click(button="right")
    p.locator("#menu button", has_text="Pin").click()
    pinned = until(lambda: p.locator("#tabbar .pins .tab").count(), 1)
    over = p.locator("#tabbar .tlist").evaluate("l => l.scrollWidth > l.clientWidth") and p.locator("#tabbar .tmore").is_visible()
    p.locator("#tabbar .tmore").click()
    listed = len([x for x in p.locator("#menu button").all_inner_texts() if x.strip()])
    p.keyboard.press("Escape")
    n = tabs.count()
    tabs.nth(n - 1).click(button="right")
    p.locator("#menu button", has_text="Close others").click()
    left = until(lambda: (p.locator("#tabbar .tab").count(), p.locator("#tabbar .tab.pinned").count()), (2, 1))
    checks["tabs: one pinned stays at the left and isn't closed with the others; many scroll, and ⌄ lists them all"] = pinned == 1 and over and listed == n and left == (2, 1)

    # Format: the file, or only what is selected
    pg.workspace("wk", "a.sql").click()
    until(lambda: pg.tab()[0], "a.sql")
    pg.p.locator("#docbar .split .caret").first.click()
    items = p.locator("#menu button").all_inner_texts()
    sel_off = p.locator("#menu button", has_text="Format selection").is_disabled()
    p.keyboard.press("Escape")
    ta = p.locator(".filedoc:visible .editor textarea")
    ta.evaluate("t => { t.value = 'select 1;\\nselect a from wk'; t.dispatchEvent(new Event('input')); t.focus(); t.setSelectionRange(0, 9); }")
    pg.runmenu("Format selection")
    part = until(lambda: ta.input_value(), "SELECT 1;\nselect a from wk")
    pg.runmenu("Format file")
    whole = until(lambda: "SELECT a\nFROM wk" in ta.input_value() and ta.input_value(), secs=5)
    checks["Format: the Run ▾ has Format file and Format selection (off with nothing selected); each formats what it says"] = \
        any(x.startswith("Format file") for x in items) and sel_off and part == "SELECT 1;\nselect a from wk" and bool(whole) and whole.startswith("SELECT 1;")

    # Markdown: a .md file's preview, and a notebook's Markdown cell
    pg.workspace("wk", "notes.md").click()
    until(lambda: pg.tab()[0], "notes.md")
    p.locator("#docbar button", has_text="Preview").click()
    view = p.locator(".mdfile")
    view.locator("h1").wait_for(timeout=10000)
    drawn = view.evaluate("""v => [v.querySelector('h1')?.textContent, v.querySelectorAll('p strong,p em,p del,p code').length, v.querySelectorAll('li.task input').length, v.querySelector('li.task input')?.checked,
        !!v.querySelector('ol li'), v.querySelectorAll('table td').length, v.querySelector('td[style]')?.style.textAlign, v.querySelector('a[target=_blank]')?.getAttribute('href'), v.querySelector('pre .k')?.textContent]""")
    pic = until(lambda: view.locator("img").evaluate("i => i.complete && i.naturalWidth"), 2)  # (the 2×2 picture, read from the lake)
    view.locator("a", has_text="a query").click()
    opened = until(lambda: pg.tab()[0], "a.sql")
    checks["Markdown drawn: headings, bold, italic, strikethrough, code, tasks, nested lists, tables (aligned), links (a lake file opens in a tab), pictures (read from the lake), SQL highlighted"] = \
        drawn == ["Title", 4, 2, True, True, 2, "right", "https://example.org", "SELECT"] and pic == 2 and opened == "a.sql"
    pg.menu("New notebook")
    kinds = pg.cell(0).locator(".kind")
    kinds.click()
    menu_kinds = p.locator("#menu button").all_inner_texts()
    p.locator("#menu button", has_text="Markdown").click()
    pg.cell(0).locator("textarea").fill("## Sales\n\n| a |\n|---|\n| 1 |")
    pg.cell(0).locator("textarea").press("Shift+Enter")
    cell_md = until(lambda: (pg.cell(0).locator(".md h2").inner_text(), pg.cell(0).locator(".md td").count()), ("Sales", 1))
    checks["a notebook's cell kinds are SQL, Python and Markdown; a Markdown cell draws its table"] = [x.strip() for x in menu_kinds] == ["SQL", "Python", "Markdown"] and cell_md == ("Sales", 1)

    # A SQL cell's Chart and Plan, as a SQL file's pane has them; the chart open is kept with the notebook
    c = pg.run(1, "SELECT region, sum(amount) AS total FROM wk GROUP BY region ORDER BY region")
    c.locator(".abar .ptab", has_text="Plan").click()
    plan = until(lambda: c.locator(".pgraph .pn").count() > 1, True, 10)
    c.locator(".abar .ptab", has_text="Chart").click()
    chart = until(lambda: c.locator(".chart svg").count() > 0 and c.locator(".abar .ptab.on").all_inner_texts() == ["Chart"], True, 10)
    p.fill("#nbname", "wkbook")
    p.press("#nbname", "Enter")
    p.keyboard.press("Control+s")
    saved = until(lambda: isinstance(_try(lambda: json.loads(get(port, "notebooks/wkbook.ipynb"))), dict), True)
    got = json.loads(get(port, "notebooks/wkbook.ipynb")) if saved is True else {}
    meta = (got.get("cells") or [{}, {}])[1].get("metadata", {})
    other = Page(browser, base + "/#file=notebooks%2Fwkbook.ipynb")
    oc = other.cell(1)
    again = until(lambda: oc.locator(".chart svg").count() > 0 and oc.locator(".abar .ptab.on").all_inner_texts() == ["Chart"], True, 15)
    other.ctx.close()
    checks["a SQL cell's answer has Chart and Plan (its graph), as a SQL file's pane; the chart open, and its settings, are kept with the notebook"] = \
        plan is True and chart is True and meta.get("pondra", {}).get("view") == "chart" and meta["pondra"].get("chart", {}).get("x") == "region" and again is True

    # The owner's third list: a cell's Data profile; SQL <-> Python; the editor's right-click; Create as; the tree's menus; rows a page
    c.locator(".abar .ptab", has_text="Data profile").click()
    profiled = until(lambda: c.locator(".dprof .dp-row").count(), 2, 10)
    ta = c.locator("textarea")
    ta.click(button="right")
    cell_items = [x.split("\n")[0] for x in p.locator("#menu button").all_inner_texts()]
    p.locator("#menu button", has_text="Make it Python").click()
    until(lambda: ta.input_value().startswith('db.sql("""'), True, 5)
    as_py = ta.input_value()
    ta.click(button="right")
    p.locator("#menu button", has_text="Make it SQL").click()
    as_sql = until(lambda: ta.input_value(), "SELECT region, sum(amount) AS total FROM wk GROUP BY region ORDER BY region", 5)
    checks["a SQL cell: Data profile (a row a column), its right-click runs, formats, creates as, and makes it Python (db.sql) and back"] = profiled == 2 \
        and {"Run cell", "Run the cells above", "Run this and the cells below", "Format cell", "Create as table or view…", "Make it Python"} <= set(cell_items) \
        and bool(as_py) and "GROUP BY region" in as_py and as_sql == "SELECT region, sum(amount) AS total FROM wk GROUP BY region ORDER BY region"
    before = pg.cells().count()
    c.hover(position={"x": 200, "y": 2})
    c.locator(".here button", has_text="Python").click()  # (the space above a cell: a cell added there)
    between = until(lambda: (pg.cells().count(), pg.cell(1).get_attribute("data-kind"), pg.cell(2).get_attribute("data-kind")), (before + 1, "python", "sql"), 5)
    checks["between two cells, + SQL, + Python, + Markdown add one there"] = between == (before + 1, "python", "sql")
    pg.menu("New SQL file")
    ta = p.locator(".filedoc:visible .editor textarea")
    ta.fill("SELECT value AS v FROM range(0, 250)")
    ta.click(button="right")
    ed_items = [x.split("\n")[0] for x in p.locator("#menu button").all_inner_texts()]
    p.locator("#menu button", has_text="Create as").click()
    d = p.locator("dialog.pop[open]")
    d.locator(".seg", has_text="View").first.click()
    d.locator("input").fill("wk_values")
    d.locator(".acts button", has_text="Create").click()
    made = until(lambda: sql(port, "SELECT kind FROM pondra.tables WHERE name = 'wk_values'"), [{"kind": "view"}])
    pg.menu("Settings")
    pg.setting("Editor and results", "Rows a page").locator("select").select_option("100")
    p.keyboard.press("Escape")
    ta.press("Control+Enter")
    bar = lambda: p.locator(".filedoc:visible .gfoot").inner_text().replace("\n", " ") if p.locator(".filedoc:visible .gfoot").count() else ""
    until(lambda: "250 rows" in bar() and "1–100" in bar(), True, 15)
    paged = bar()
    tabs_now = p.locator(".filedoc:visible .ptab").all_inner_texts()
    kept_rows = p.evaluate("JSON.parse(localStorage.getItem('pondra.prefs') || '{}').pageRows")
    p.locator(".filedoc:visible .pg-r").click()
    p.locator("#menu button", has_text="10,000").first.click()  # (back as it was: the parts after this one see 10,000 a page)
    checks["a SQL file's right-click has Run file, Format, Create as (a view made), Run as a job, Schedule, Save as; 100 rows a page (Settings; the pager's 1–100 ▾ changes it too); a Data profile tab"] = \
        {"Run file", "Format file", "Create as table or view…", "Run as a job", "Schedule…", "Save as…"} <= set(ed_items) and made == [{"kind": "view"}] \
        and "250 rows" in paged and "1–100" in paged and kept_rows == 100 and any("Data profile" in x for x in tabs_now)
    tree = p.locator("#data")
    pub = tree.locator(".row[data-kind=schema]", has_text="public").first
    if pub.get_attribute("aria-expanded") == "false":
        pub.click()
    wk = tree.locator(".row[data-kind]").filter(has=p.locator(".nm", has_text=re.compile("^wk$"))).first
    def opened(el):
        """Right-click `el` and read the menu it opens (its items come with objects.js: waited for)."""
        p.keyboard.press("Escape")
        until(lambda: p.locator("#menu").is_visible(), False, 5)
        el.click(button="right")
        until(lambda: p.locator("#menu").is_visible() and "Copy the name" in p.locator("#menu").inner_text(), True, 10)
        return [x.split("\n")[0] for x in p.locator("#menu button").all_inner_texts()]
    t_items = opened(wk)
    p.locator("#menu button", has_text="Script as").click()
    d = p.locator("dialog.pop[open]")
    scripts = d.locator(".sa-i").all_inner_texts()
    d.locator(".seg", has_text="Python").click()
    py = d.locator(".sa-c").inner_text()
    p.keyboard.press("Escape")
    vw = tree.locator(".row[data-kind]").filter(has=p.locator(".nm", has_text=re.compile("^wk_values$"))).first
    until(lambda: vw.count(), 1, 10)
    v_items = opened(vw)
    p.keyboard.press("Escape")
    checks["the Data tree's right-click: a table's Preview, Script as (SELECT … DROP, SQL or Python), Insert, Add a column, Rename, Truncate, Drop; a view's has no Insert"] = \
        {"Preview", "Preview in Python", "Script as…", "Insert rows…", "Add a column…", "Rename…", "Truncate…", "Drop…"} <= set(t_items) \
        and {"SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP"} <= set(scripts) and py.startswith("db.") and "Insert rows…" not in v_items and "Drop…" in v_items
    sql(port, "DROP VIEW wk_values")
    checks["the owner's second and third lists: no page errors"] = pg.errors == []
    info = {"left": left, "items": items, "formatted": [part, whole], "drawn": drawn, "picture": pic, "opened": opened, "kinds": menu_kinds, "meta": meta, "cell": cell_items, "editor": ed_items,
            "paged": paged, "table menu": t_items, "scripts": scripts, "python": py[:80], "view menu": v_items, "errors": pg.errors}
    pg.ctx.close()
    return checks, info


def axe_js():
    for path in [os.environ.get("AXE_JS", ""), os.path.join(HERE, "node_modules", "axe-core", "axe.min.js"), os.path.join("node_modules", "axe-core", "axe.min.js")]:
        if path and os.path.exists(path):
            return open(path).read()
    return None


def audit(page, axe):
    """What axe finds (WCAG 2.1 A and AA, and its best practice), as [rule, how many]."""
    page.add_script_tag(content=axe)
    return page.evaluate("async () => (await axe.run(document, { runOnly: ['wcag2a', 'wcag2aa', 'wcag21aa', 'best-practice'] })).violations.map(v => [v.id, v.nodes.length, v.nodes[0].target.join(' ')])")


def layout_checks(browser, port, show):
    """The shell (ADR-034 §1–2, §6): views on either side, settings, the filter, panes and their
    edges, tabs kept, a narrow window, and what axe finds."""
    checks = {}
    base = f"http://127.0.0.1:{port}"
    sql(port, "CREATE TABLE lx AS SELECT 1 AS id")
    sql(port, "CREATE TABLE ly AS SELECT 2 AS id")
    put(port, "notes/a.sql", b"SELECT count(*) AS n FROM lx;\nSELECT id FROM lx WHERE id >= $low")  # (for the audit: a parameter's input, a statement's answers)
    put(port, "notes/b.csv", b"k,v\n1,one\n")
    pg = Page(browser, base + "/")
    p = pg.p
    ready = lambda: p.locator("#data .row", has_text="lx").wait_for(timeout=20000)
    ready()
    p.locator('#left button[aria-label="Workspace: more"]').click()
    p.locator("#menu button", has_text="Move to the right pane").click()
    moved = (p.locator("#rtabs .rtab").all_inner_texts(), p.locator("#left .group h2").all_inner_texts())
    p.reload()
    ready()
    kept = "Workspace" in p.locator("#rtabs .rtab").all_inner_texts() and p.locator("#left .group h2").all_inner_texts() == ["Data"]
    p.locator("#rtabs .rtab", has_text="Workspace").click()
    p.locator('#rtabs button[aria-label="Workspace: more"]').click()
    p.locator("#menu button", has_text="Move to the left pane").click()
    back = p.locator("#left .group h2").all_inner_texts()
    checks["a view moves to the other pane by its ⋯ (kept after a reload), and back"] = "Workspace" in moved[0] and moved[1] == ["Data"] and kept and back == ["Data", "Workspace"]
    pg.menu("Settings")
    sections = p.locator("dialog.settings2 .s-i").all_inner_texts()
    pg.setting("Layout", "The left pane").locator(".seg", has_text="Workspace first").click()
    first = p.locator("#left .group h2").all_inner_texts()
    pg.setting("Appearance")
    p.locator("dialog.settings2 .theme", has_text="Dark").click()
    dark = p.evaluate("document.documentElement.dataset.theme") == "dark" and p.evaluate("getComputedStyle(document.body).backgroundColor") != "rgb(255, 255, 255)"
    pg.setting("Layout", "The left pane").locator(".seg", has_text="Data first").click()
    pg.setting("Appearance")
    p.locator("dialog.settings2 .theme", has_text="As the system").click()
    p.locator("dialog.settings2 .s-find").fill("statements")
    found = until(lambda: p.locator("dialog.settings2 .s-body .s-n").all_inner_texts(), ["A SQL file's statements"])
    p.locator("dialog.settings2 .s-find").fill("")
    p.keyboard.press("Escape")
    checks["Settings: sections (Appearance, Editor and results, Layout, Keys, Python, About) and a search across them; Workspace first (Data first by default), and the dark theme"] = \
        sections == ["Appearance", "Editor and results", "Layout", "Keys", "Python", "About"] and found == ["A SQL file's statements"] and first == ["Workspace", "Data"] and dark \
        and p.locator("#left .group h2").all_inner_texts() == ["Data", "Workspace"]
    # Kept on the machine (round 29): another browser (no storage of its own) opens with them.
    pg.menu("Settings")
    pg.setting("Appearance")
    p.locator("dialog.settings2 .theme", has_text="Dark").click()
    pg.setting("Appearance", "Background").locator("input[type=color]").evaluate("(el) => { el.value = '#203040'; el.dispatchEvent(new Event('input', { bubbles: true })); }")
    p.keyboard.press("Escape")
    kept_json = until(lambda: json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/console/settings").read()).get("theme"), "dark")
    other = Page(browser, f"http://127.0.0.1:{port}/")
    other.p.locator("#tabbar .tab").first.wait_for(timeout=20000)
    there = until(lambda: (other.p.evaluate("document.documentElement.dataset.theme"), other.p.evaluate("getComputedStyle(document.documentElement).getPropertyValue('--surface').trim()")), ("dark", "#203040"))
    other.ctx.close()
    pg.menu("Settings")
    pg.setting("Appearance")
    p.locator("dialog.settings2 .theme", has_text="Light").click()
    p.locator("dialog.settings2 .theme", has_text="Dark").click()  # (the dark theme's background, set above: back to its default)
    pg.setting("Appearance", "Background").locator("button", has_text="Default").click()
    p.locator("dialog.settings2 .theme", has_text="Light").click()
    p.keyboard.press("Escape")
    light = until(lambda: json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/console/settings").read()).get("theme"), "light")
    checks["Settings are kept on the machine: another browser opens with the dark theme and the background picked; light is the first theme"] = \
        kept_json == "dark" and there == ("dark", "#203040") and light == "light"
    p.fill("#filter", "lx")
    seen = lambda: p.locator("#data .row:visible .nm").all_inner_texts()
    only = until(lambda: "lx" in seen() and "ly" not in seen(), True)
    p.press("#filter", "Escape")
    checks["the filter narrows the trees to the names that hold it (and what they are in); Esc clears it"] = only is True and "ly" in seen()
    p.locator("#docs").click()
    p.keyboard.press("Control+b")
    hid = p.locator("#left").is_hidden()
    p.keyboard.press("Control+b")
    p.keyboard.press("Control+Alt+b")
    right = p.locator("#right").is_hidden()
    p.keyboard.press("Control+Alt+b")
    checks["Ctrl+B and Ctrl+Alt+B hide and show the side panes"] = hid and p.locator("#left").is_visible() and right and p.locator("#right").is_visible()
    w0 = p.evaluate("document.querySelector('#left').offsetWidth")
    b = p.locator("#leftEdge").bounding_box()
    p.mouse.move(b["x"] + b["width"] / 2, b["y"] + 200)
    p.mouse.down()
    p.mouse.move(b["x"] + b["width"] / 2 + 60, b["y"] + 200, steps=5)
    p.mouse.up()
    wide = p.evaluate("document.querySelector('#left').offsetWidth")
    p.reload()
    ready()
    again = p.evaluate("document.querySelector('#left').offsetWidth")
    p.locator("#leftEdge").dblclick()
    reset = p.evaluate("document.querySelector('#left').offsetWidth")
    checks["a pane's edge sets its width (kept after a reload); a double-click resets it"] = wide == w0 + 60 and again == wide and reset == w0
    pg.workspace("notes", "a.sql").click()
    until(lambda: pg.tab()[0], "a.sql")
    p.goto("about:blank")
    p.goto(base + "/")
    ready()
    tabs = until(lambda: "a.sql" in p.locator("#tabbar .tab").all_inner_texts(), True)
    checks["the files open in tabs open again with the page"] = tabs is True
    p.set_viewport_size({"width": 700, "height": 820})
    p.wait_for_timeout(300)
    narrow = p.evaluate("document.documentElement.scrollWidth") == 700 and p.locator("#left").is_hidden() and p.locator("#right").is_hidden()
    p.locator('#panes button[aria-label="Show or hide the left pane"]').click()
    drawer = p.locator("#left").is_visible() and p.locator("#left").bounding_box()["x"] == 0
    p.mouse.click(650, 500)
    closed = until(lambda: p.locator("#left").is_hidden(), True, 3)
    p.set_viewport_size({"width": 1440, "height": 900})
    checks["a narrow window: nothing scrolls sideways, the side panes are drawers, closed by using the page"] = narrow and drawer and closed is True
    axe, found = axe_js(), {}
    if axe:
        for scheme in ("light", "dark"):
            a = Page(browser, base + "/", scheme)
            a.p.locator("#data .row", has_text="lx").wait_for(timeout=20000)
            a.run(0, "SELECT id, 'one' AS s FROM lx")
            a.p.locator("#data .row", has_text="lx").click()
            a.p.wait_for_timeout(400)
            found[scheme + " notebook"] = audit(a.p, axe)
            a.workspace("notes", "a.sql").click()
            a.p.wait_for_function("document.activeElement?.tagName === 'TEXTAREA'")
            a.p.fill(".param input", "0")
            a.p.click("#runBtn")
            a.p.locator(".stmts .stmt").nth(1).wait_for(timeout=15000)
            a.p.locator(".filedoc .gt").wait_for(timeout=15000)
            found[scheme + " SQL file"] = audit(a.p, axe)
            a.workspace("notes", "b.csv").click()
            a.p.locator(".datadoc .gt").wait_for(timeout=15000)
            found[scheme + " data file"] = audit(a.p, axe)
            a.ctx.close()
    checks["axe finds nothing (WCAG 2.1 AA, contrast included, and its best practice), light and dark: a notebook, a SQL file, a data file"] = \
        bool(axe) and len(found) == 6 and not any(found.values())
    checks["layout: no page errors"] = pg.errors == []
    info = {"moved": moved, "back": back, "widths": [w0, wide, again, reset], "axe": found if axe else "axe-core not found: AXE_JS, or npm install --prefix tools axe-core", "errors": pg.errors}
    pg.ctx.close()
    return checks, info


def swallowed():
    """Lines whose code a `//` comment put in the middle of them hides (the node strips such comments):
    `x(); // (why) this.y = 1;` — what follows the comment never runs."""
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "console")
    pat = re.compile(r" // [^\n]*\)[ ;]+(this\.|const |let |if \(|return |[a-zA-Z_.]+\()")
    return [f"{f}:{i + 1}" for f in sorted(os.listdir(here)) if f.endswith(".js") for i, line in enumerate(open(os.path.join(here, f), encoding="utf-8")) if pat.search(line)]


def budget_checks(browser, port, show):
    """ADR-034 §7: what the page costs — bytes, first paint, typing, scrolling."""
    checks = {}
    base = f"http://127.0.0.1:{port}"
    sizes, fresh = {}, {}
    for name in ["core.js", "editor.js", "grid.js", "notebook.js", "files.js", "console.js", "console.css"]:
        r = urllib.request.urlopen(urllib.request.Request(f"{base}/console/{name}", headers={"accept-encoding": "gzip"}))
        body, tag = r.read(), r.headers["etag"]
        sizes[name] = len(body) if r.headers.get("content-encoding") == "gzip" and gzip.decompress(body) else None
        try:
            urllib.request.urlopen(urllib.request.Request(f"{base}/console/{name}", headers={"accept-encoding": "gzip", "if-none-match": tag}))
            fresh[name] = 200
        except urllib.error.HTTPError as e:
            fresh[name] = e.code
    total = sum(sizes.values()) if all(sizes.values()) else None
    later = {}
    for name in ["chart.js", "plan.js", "more.js", "details.js", "more.css", "data.js", "md.js", "jobs.js", "settings.js", "objects.js", "pyfile.js", "versions.js", "stmts.js", "params.js"]:  # (loaded when first used)
        r = urllib.request.urlopen(urllib.request.Request(f"{base}/console/{name}", headers={"accept-encoding": "gzip"}))
        later[name] = len(r.read()) if r.headers.get("content-encoding") == "gzip" else None
    checks["the scripts and style sheet the page loads, gzipped as the node serves them: <= 70 KB; each answers 304 when the browser has it"] = total is not None and total <= 70 * 1024 \
        and set(fresh.values()) == {304}
    checks["those loaded when first used (a chart, a plan, History, details, a data file, Markdown, Jobs, Settings, the tree's menus, a Python file): <= 8 KB each, gzipped"] = all(v is not None and v <= 8 * 1024 for v in later.values())
    paints = []
    for _ in range(3):
        pg = Page(browser, base + "/")
        pg.p.locator("#tabbar .tab").first.wait_for(timeout=20000)
        paints.append(pg.p.evaluate("performance.getEntriesByName('first-contentful-paint')[0]?.startTime ?? 1e9"))
        pg.ctx.close()
    checks["first paint < 400 ms (the median of three)"] = statistics.median(paints) < 400
    lines = "\n".join(f"SELECT {i} AS n, 'line {i}' AS s, {i} * 2 AS d FROM range(0, 1) WHERE {i} > 0  -- a comment on line {i}" for i in range(1000))
    put(port, "perf/big.sql", lines.encode())
    pg = Page(browser, base + "/#file=perf/big.sql")
    p = pg.p
    until(lambda: pg.tab()[0], "big.sql")
    ta = p.locator(".filedoc .editor textarea")
    ta.evaluate("""ta => { const at = ta.value.indexOf('\\n', ta.value.length / 2); ta.focus(); ta.setSelectionRange(at, at);
        const box = ta.closest('.editor'); window.keys = []; let t0 = 0;
        ta.addEventListener('keydown', () => { t0 = performance.now(); }, true);
        ta.addEventListener('input', () => { box.getBoundingClientRect(); document.body.offsetHeight; window.keys.push(performance.now() - t0); }); }""")
    for ch in " AND n < 1000 OR s = 'abc' -- more":
        p.keyboard.type(ch)
    keys = p.evaluate("window.keys")
    for text in ["/* a comment", "\n'a quote", "\nopened */ -- ", "\n'"]:  # (states that go on past a line, opened and closed)
        p.keyboard.insert_text(text)
    same = ta.evaluate("""async ta => { const { highlight } = await import('/console/editor.js'), d = document.createElement('div');
        const want = highlight(ta.value, 'sql').map(x => (d.innerHTML = x || ' ', d.innerHTML)), got = [...ta.closest('.editor').querySelectorAll('pre.hl > div')].map(x => x.innerHTML);
        return want.length === got.length && want.every((w, i) => w === got[i]); }""")
    checks["the editor highlights a line at a time (the state a comment or quote leaves carried on), as the whole text highlighted"] = same is True
    typing = statistics.median(keys) if keys else 1e9
    checks["typing in a 1,000-line file: < 8 ms a key (the median, the page's work to its layout)"] = len(keys) > 30 and typing < 8
    ta.evaluate("ta => { ta.value = 'SELECT value AS a, value * 2 AS b, \\'x\\' || value AS c, value % 7 AS d FROM range(0, 10000)'; ta.dispatchEvent(new Event('input')); }")
    p.click("#runBtn")
    p.locator(".filedoc .pbody .gt").wait_for(timeout=30000)
    frames = p.evaluate("""async () => { const g = document.querySelector('.filedoc .pbody .grid'), out = []; let last = performance.now();
        for (let i = 0; i < 150; i++) { await new Promise(r => requestAnimationFrame(r)); const t = performance.now(); out.push(t - last); last = t; g.scrollTop += 240; }
        return out.slice(10); }""")
    frames.sort()
    p95 = frames[int(len(frames) * 0.95)] if frames else 1e9
    checks["scrolling 10,000 rows: p95 frame < 20 ms"] = p95 < 20
    hidden = swallowed()
    checks["no code hidden after a // comment in the middle of a line (the node strips them)"] = hidden == []
    checks["budget: no page errors"] = pg.errors == []
    info = {"gzipped": sizes, "total": total, "later": later, "fresh": fresh, "first paint ms": paints, "typing ms": round(typing, 2), "keys": [round(k, 2) for k in keys[:40]], "scroll p95 ms": round(p95, 2),
            "errors": pg.errors}
    pg.ctx.close()
    return checks, info


def token_checks(browser, port):
    token = "console-check-admin-token"
    lake = tempfile.mkdtemp(prefix="pondra-")
    owner = "console-check-owner-key-1234"
    node = Node(lake, port, admin_token=token, read_token="console-check-reader", env={"PONDRA_OWNER_KEY": owner}).start()
    try:
        harness.call(port, "POST", "/sql", b"CREATE TABLE secret_things AS SELECT 1 AS id", headers={"authorization": f"Bearer {token}"})
        pg = Page(browser, f"http://127.0.0.1:{port}/")
        asked = until(lambda: pg.p.locator("#tokenDlg").get_attribute("open") is not None, True)
        hidden = pg.p.locator("#data .row", has_text="secret_things").count() == 0
        pg.p.fill("#tokenIn", token)
        pg.p.press("#tokenIn", "Enter")
        shown = until(lambda: pg.p.locator("#data .row", has_text="secret_things").count(), 1)
        signed = pg.p.locator("#signin").inner_text() == "Signed in"
        pg.p.reload()
        again = until(lambda: pg.p.locator("#data .row", has_text="secret_things").count(), 1)
        def python_of(who):
            try:
                return harness.call(port, "GET", "/sessions/some-page/python", headers={"authorization": f"Bearer {who}"})
            except Exception as e:  # noqa: BLE001 (refused: what it said)
                return str(e)
        # A user: its name and password (a session), and only what it may read
        adm = lambda q: harness.call(port, "POST", "/sql", q.encode(), headers={"authorization": f"Bearer {token}"})
        for q in ["CREATE TABLE other_things AS SELECT 2 AS id", "CREATE USER cc_user PASSWORD 'cc-password-1'", "GRANT SELECT ON secret_things TO cc_user"]:
            adm(q)
        user = Page(browser, f"http://127.0.0.1:{port}/")
        until(lambda: user.p.locator("#tokenDlg").get_attribute("open") is not None, True)
        user.p.fill("#userIn", "cc_user")
        user.p.fill("#tokenIn", "cc-password-1")
        user.p.press("#tokenIn", "Enter")
        as_user = until(lambda: (user.p.locator("#data .row", has_text="secret_things").count(), user.p.locator("#data .row", has_text="other_things").count(), user.p.locator("#signin").inner_text()), (1, 0, "cc_user"))
        user.ctx.close()
        # The shell's link (#key=…): the page works as the shell does, asking nothing
        shell = Page(browser, f"http://127.0.0.1:{port}/#key={owner}")
        as_shell = until(lambda: (shell.p.locator("#data .row", has_text="other_things").count(), shell.p.locator("#tokenDlg").get_attribute("open"), shell.p.evaluate("location.hash")), (1, None, ""))
        shell.ctx.close()
        checks = {"a user signs in with its name and password (a session) and sees only what it may read; the shell's link (#key=…) signs the page in as the shell, its key out of the address": as_user == (1, 0, "cc_user") and as_shell == (1, None, ""),
                  "with tokens, the page asks for one (Sign in), then shows the tables (and after a reload)": asked is True and hidden and shown == 1 and again == 1
                  and signed and pg.p.locator("#tokenDlg").get_attribute("open") is None,
                  "a session's Python variables are an admin's to read, as DO is": "admin" in str(python_of("console-check-reader"))
                  and python_of(token) == {"running": False, "variables": []}}
        pg.ctx.close()
        return checks, {}
    finally:
        node.kill()
        shutil.rmtree(lake, ignore_errors=True)


def server_checks(browser, port):
    folder = tempfile.mkdtemp(prefix="pondra-")
    for name, q in [("sales", "CREATE TABLE orders AS SELECT 1 AS id, 10.5 AS amount UNION ALL SELECT 2, 20.0"), ("lake", "CREATE TABLE notes AS SELECT 'hi' AS text")]:
        subprocess.run([BIN, "sql", "--dir", os.path.join(folder, name), q], check=True, capture_output=True)
    srv = subprocess.Popen([BIN, "serve", folder, "--addr", f"127.0.0.1:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        until(lambda: isinstance(call(port, "GET", "/databases"), list), True, 30)
        pg = Page(browser, f"http://127.0.0.1:{port}/")
        tree = pg.p.locator("#data")
        tree.locator(".row", has_text="notes").wait_for(timeout=30000)
        listed = tree.locator(".row[data-kind=database] .nm").all_inner_texts()
        title = pg.p.locator("#dataTitle").inner_text()
        tree.locator(".row[data-kind=database]", has_text="sales").click()
        tree.locator(".row", has_text="orders").wait_for(timeout=30000)
        c = pg.run(0, "SELECT sum(amount) AS total FROM orders")
        checks = {"the server's console lists its databases (Databases); a cell runs in the one picked (/db/sales)": listed == ["lake", "sales"] and title == "Databases"
                  and pg.grid(c) == [["total"], [["30.5"]]] and any("/db/sales/sql" in u for u in pg.seen) and "db=sales" in pg.p.url,
                  "the server's console: every request went to the server; no page errors": pg.left() == [] and pg.errors == []}
        info = {"listed": listed, "left": pg.left(), "errors": pg.errors}
        pg.ctx.close()
        return checks, info
    finally:
        srv.terminate()
        srv.wait(timeout=30)
        shutil.rmtree(folder, ignore_errors=True)


def ext_checks(browser, port):
    """An extension (examples/console-extension.js) given to a node: served, loaded, and what it
    registers shows beside the console's own."""
    lake = tempfile.mkdtemp(prefix="pondra-")
    ext = os.path.join(HERE, "..", "examples", "console-extension.js")
    node = Node(lake, port, env={"PONDRA_CONSOLE_EXTENSIONS": ext}).start()
    try:
        sql(port, "CREATE TABLE things AS SELECT * FROM (VALUES (1), (2), (3)) AS t(id)")
        served = call(port, "GET", "/console/ext/0.js").decode() == open(ext).read()
        pg = Page(browser, f"http://127.0.0.1:{port}/")
        p = pg.p
        p.locator("#data .row", has_text="things").wait_for(timeout=20000)
        section = p.locator("#historyTitle").text_content() == "History" and p.locator("#history .empty").count() == 1
        c = pg.run(0, "SELECT count(*) AS n FROM things")
        figure = c.locator(".ext-figure").inner_text().split("\n")
        history = until(lambda: p.locator("#history .row").all_inner_texts(), ["SELECT count(*) AS n FROM things"])
        p.locator("#data .row", has_text="things").click()
        p.locator("#rtabs .rtab", has_text="Sample").click()
        sample = until(lambda: p.locator("#sample pre.said").count(), 3)
        p.click("#moreBtn")
        action = p.locator("#menu button", has_text="Copy a link to this notebook").count() == 1
        p.keyboard.press("Escape")
        checks = {"an extension is served (/console/ext/0.js), and its section, panel tab, view of an answer and menu action show beside the console's own":
                  served and section and figure == ["3", "n"] and history == ["SELECT count(*) AS n FROM things"] and sample == 3 and action,
                  "with an extension: every request went to the node; no page errors": pg.left() == [] and pg.errors == []}
        info = {"served": served, "section": section, "action": action, "figure": figure, "history": history, "sample": sample, "errors": pg.errors}
        pg.ctx.close()
        return checks, info
    finally:
        node.kill()
        shutil.rmtree(lake, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8890)
    ap.add_argument("--show", help="keep screenshots here")
    ap.add_argument("--only", help="run only these parts (node, files, grid, work, layout, budget, tokens, server, extensions), separated by commas")
    A = harness.A = ap.parse_args()
    A.s3, A.keep = False, False
    if A.show:
        os.makedirs(A.show, exist_ok=True)
    lake = tempfile.mkdtemp(prefix="pondra-")
    # (the console's settings, kept by the node for its machine: this run's own, never the machine's)
    os.environ["PONDRA_CONFIG_DIR"] = tempfile.mkdtemp(prefix="pondra-config-")
    node = Node(lake, A.port, env={"PYTHONPATH": os.path.join(HERE, "..", "python")}, python=sys.executable).start()
    results, said = {}, {}
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            parts = [("node", lambda: node_checks(browser, A.port, A.show)), ("files", lambda: files_checks(browser, A.port, A.show)), ("grid", lambda: grid_checks(browser, A.port, A.show)), ("work", lambda: work_checks(browser, A.port, A.show)),
                     ("layout", lambda: layout_checks(browser, A.port, A.show)), ("budget", lambda: budget_checks(browser, A.port, A.show)),
                     ("tokens", lambda: token_checks(browser, A.port + 1)), ("server", lambda: server_checks(browser, A.port + 2)), ("extensions", lambda: ext_checks(browser, A.port + 3))]
            for part, f in parts:
                if A.only and part not in A.only.split(","):
                    continue
                try:
                    checks, info = f()
                except Exception as e:  # noqa: BLE001 (a part that couldn't run fails)
                    checks, info = {f"{part}: ran": False}, {"error": f"{type(e).__name__}: {str(e)[:1500]}"}
                results.update(checks)
                if info:
                    said[part] = info
                print(json.dumps({part: checks}, indent=1), flush=True)
            browser.close()
    finally:
        node.kill()
        shutil.rmtree(lake, ignore_errors=True)
    ok = all(results.values())
    if not ok or os.environ.get("CONSOLE_CHECK_SAY"):
        print(json.dumps(said, indent=1, default=str)[:12000])
    print(json.dumps({"checks": len(results), "passed": sum(results.values()), "ok": ok}))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
