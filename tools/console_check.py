#!/usr/bin/env python3
"""The console (ADR-030), in headless Chromium through Playwright.

  console_check.py [--port 8890] [--show DIR]

- node: the tree lists the lake's schemas, tables, views (each kind its icon) and columns (each
  type its glyph, a key's marked), with no bare numbers; a table picked shows its details (rows,
  key, columns, a view's definition) and its profile, and Query (or a double-click) its first
  rows; the lake's files are listed by folder, and one is read as a table; a long answer draws
  only the rows in sight, sorts by a header's click, and its columns are summarized; a
  SQL cell and a Python cell run (what the code printed, and its last expression as rows);
  errors come back in plain words, a Python error at its line in the cell; Python cells share
  their variables, a figure shows as a picture, the Variables tab lists them, and Restart empties
  them; a name completes with Tab; a live cell shows a row INSERTed over HTTP, and its query ends
  when the switch is off; the keys (Esc, B, D D, Z, O, Shift+Enter, ?); text cells' headings make
  the outline; a notebook saved in the lake (twice: two versions) and opened again is the one
  saved, and Jupyter's nbformat validates it; so does the one downloaded; an .ipynb uploaded
  opens, its text rendered and its %%sql cell run.
- tokens: a node with tokens serves the page, which asks for one; given it, the tables show,
  and after a reload too (the browser keeps it).
- server: `pondra serve <folder of lakes>`'s console lists its databases, and a cell runs in the one picked.
- extensions: a node given PONDRA_CONSOLE_EXTENSIONS (examples/console-extension.js) serves it, and
  its section, panel tab, view of an answer and menu action show beside the console's own; the
  console's files answer 304 to a browser that has them.
- every request the page made went to the node (or server) that served it.

--show DIR keeps screenshots, light and dark.
Needs: playwright (Chromium at PLAYWRIGHT_BROWSERS_PATH) and nbformat.
"""
import argparse, json, os, shutil, subprocess, sys, tempfile, time, urllib.error, urllib.request

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


class Page:
    """A browser tab on `url`, with every request it made and every error it hit."""

    def __init__(self, browser, url, scheme="light"):
        self.ctx = browser.new_context(viewport={"width": 1400, "height": 900}, color_scheme=scheme, accept_downloads=True)
        self.p = self.ctx.new_page()
        self.url, self.seen, self.errors = url, [], []
        self.p.on("request", lambda r: self.seen.append(r.url))
        self.p.on("pageerror", lambda e: self.errors.append(str(e)))
        self.p.on("dialog", lambda d: d.accept())  # (confirm: leave a notebook with changes)
        self.p.goto(url)

    def cells(self):
        return self.p.locator("section.cell")

    def cell(self, i):
        return self.cells().nth(i)

    def run(self, i, code):
        """Put `code` in cell i and run it; the cell."""
        c = self.cell(i)
        c.locator("textarea").fill(code)
        c.locator("textarea").press("Control+Enter")
        c.locator(".out").wait_for(state="visible", timeout=30000)
        until(lambda: "running" in (c.locator(".st").inner_text() or ""), False, 30)
        return c

    def grid(self, c):
        """A cell's answer: its column names and its rows, as text."""
        heads = [h.split("\n")[0] for h in c.locator("thead th").all_inner_texts()[1:]]
        rows = [[t.strip() for t in r.split("\t")[1:]] for r in c.locator("tbody tr:not(.gap)").all_inner_texts()]
        return heads, rows

    def menu(self, item):
        """Pick `item` in the top bar's ⋯ menu."""
        self.p.click("#moreBtn")
        self.p.locator("#menu button", has_text=item).click()

    def left(self):
        """Requests that went anywhere but the node that served the page."""
        home = self.url.split("/", 3)[:3]
        return [u for u in self.seen if u.split("/", 3)[:3] != home and not u.startswith(("data:", "blob:"))]

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
    tree = p.locator("#tree")
    tree.locator(".row", has_text="people").wait_for(timeout=20000)
    people = tree.locator(".line", has_text="people")
    tree.locator(".line", has_text="sales").locator(".tw").click()
    tree.locator(".row", has_text="orders").wait_for(timeout=10000)
    people.locator(".tw").click()
    columns = tree.locator(".col:visible").all_inner_texts()
    kinds = {r.inner_text().strip(): r.get_attribute("data-kind") for r in tree.locator(".row[data-kind]").all()}
    checks["the tree lists the lake's schemas, tables and views (each kind its icon), columns with SQL types and glyphs, a key marked, no bare numbers"] = \
        kinds == {"people": "table", "grown": "view", "orders": "table"} and [c.split("\n")[0] for c in columns] == ["id", "name", "born", "at", "amt"] \
        and "DECIMAL(10,2)" in columns[4] and "TIMESTAMP" in columns[3] and tree.locator(".col:visible .tg").count() == 5 and tree.locator(".col:visible .kk").count() == 1 \
        and not [t for t in tree.locator(".row:visible").all_inner_texts() if any(w.isdigit() for w in t.split())]
    people.locator(".row").click()  # (its details, in the panel on the right)
    detail = p.locator("#detail")
    rows_shown = until(lambda: detail.locator("dd").first.inner_text(), "3")
    facts = dict(zip(detail.locator("dt").all_inner_texts(), detail.locator("dd").all_inner_texts()))
    checks["a table picked shows its details: rows, key, columns (a key's and NOT NULL marked)"] = rows_shown == "3" and facts.get("Key") == "id" \
        and detail.locator(".pc").count() == 5 and "not null" in detail.locator(".pc", has_text="name").inner_text()
    detail.locator("button", has_text="Profile").click()
    profiled = until(lambda: detail.locator(".pc", has_text="born").locator(".nums").inner_text().startswith("67% null"), True)
    charts = until(lambda: (detail.locator(".pc", has_text="amt").locator("svg rect").count(), detail.locator(".pc", has_text="name").locator(".bars .v").count()), (20, 3))
    amt = detail.locator(".pc", has_text="amt").inner_text()
    checks["Profile: each column's nulls, distinct values, range, and a histogram or its commonest values"] = profiled is True and "1.50 … 2.25" in amt and charts == (20, 3)
    tree.locator(".row", has_text="grown").click()
    checks["a view picked shows its definition"] = until(lambda: "born IS NOT NULL" in detail.locator(".defn").inner_text(), True) is True

    c = pg.run(0, "SELECT id, name, born, at, amt FROM people ORDER BY id")
    heads, rows = pg.grid(c)
    checks["a SQL cell shows the rows with their types; timestamps and decimals as written"] = heads == ["id", "name", "born", "at", "amt"] \
        and rows[0] == ["1", "Ann", "1990-01-02", "2024-01-01 10:00:00", "1.50"] and rows[1][2] == "NULL" and "3 rows" in c.locator(".meta").inner_text() \
        and "BIGINT" in c.locator("thead").inner_text()
    tree.locator(".row", has_text="orders").dblclick()  # (a table's first rows, in a new cell)
    peek = until(lambda: pg.grid(pg.cell(1))[1], [["1", "10.5"]])
    checks["double-clicking a table shows its first rows"] = peek == [["1", "10.5"]] and "sales.orders" in pg.cell(1).locator("textarea").input_value()
    call(port, "PUT", "/files/reports/q1.csv", b"a,b\n1,x\n2,y\n")
    p.click("#refresh")
    files = p.locator("#files")
    files.locator(".row", has_text="reports").click()
    files.locator(".row", has_text="q1.csv").wait_for(timeout=10000)
    files.locator(".row", has_text="q1.csv").dblclick()  # (the lake's own file, read as a table: whoever reads the lake reads it)
    read = until(lambda: pg.grid(pg.cell(2))[1], [["1", "x"], ["2", "y"]])
    checks["the lake's files are listed by folder, and a data file is read as a table"] = read == [["1", "x"], ["2", "y"]]
    long = pg.run(2, "SELECT value AS n, value % 7 AS m FROM range(0, 5000)")
    drawn = long.locator("tbody tr:not(.gap)").count()
    long.locator(".grid").evaluate("g => g.scrollTop = g.scrollHeight")
    last = until(lambda: long.locator("tbody tr:not(.gap)").last.inner_text().split("\t")[1].strip(), "4999")
    long.locator("th", has_text="m").click()
    long.locator(".grid").evaluate("g => g.scrollTop = 0")
    top = until(lambda: long.locator("tbody tr:not(.gap)").first.inner_text().split("\t")[2].strip(), "0")
    m = detail.locator(".pc.on").inner_text()
    checks["a long answer draws only the rows in sight, sorts by a header, and its column is summarized in the panel"] = drawn < 120 and last == "4999" and top == "0" \
        and "7 distinct" in m and "0 … 6" in m

    p.click("[data-add=python]")
    p.keyboard.insert_text('for i in range(2):\n    print("hello", i)\ndb.sql("SELECT count(*) AS n FROM people")')
    p.keyboard.press("Control+Enter")
    py = pg.cell(3)
    py.locator("table").wait_for(timeout=60000)
    checks["a Python cell runs on the node: what it printed, then its last expression as rows"] = py.locator(".said").inner_text() == "hello 0\nhello 1" and pg.grid(py) == (["n"], [["3"]])
    c = pg.run(3, "x = 1\n1 / 0")
    python_error = c.locator(".err").inner_text()
    c = pg.run(1, "SELECT nope FROM people")
    sql_error = c.locator(".err").inner_text()
    checks["errors in plain words: a SQL one, and a Python one at its line in the cell"] = "nope" in sql_error and "ZeroDivisionError" in python_error and "line 2" in python_error
    c = pg.run(3, "x * 10")  # (the cell before made x, then failed: x stays, as in a notebook)
    shared = until(lambda: pg.grid(c), (["value"], [["10"]]))
    checks["Python cells share their variables (one namespace per page, as a notebook's kernel)"] = shared == (["value"], [["10"]])
    fig = pg.run(3, "import matplotlib.pyplot as plt\nfig, ax = plt.subplots(figsize=(4, 2))\nax.plot([1, 3, 2])\nfig")
    drawn = until(lambda: fig.locator("img.fig").count() == 1 and fig.locator("img.fig").evaluate("i => i.complete && i.naturalWidth") > 100, True)
    fig_said = fig.locator(".out").inner_text()[:300]  # (why, if it drew nothing: matplotlib missing says so)
    checks["a figure a Python cell returns (matplotlib) shows as a picture"] = drawn is True and fig.locator("img.fig").get_attribute("src").startswith("data:image/png;base64,")
    p.locator("#tabs button", has_text="Variables").click()
    want = ["ax", "fig", "i", "x"] if drawn is True else ["i", "x"]  # (the figure's failing is its own check's)
    names = until(lambda: p.locator("#detail .var .nm").all_inner_texts(), want)
    x_type = p.locator("#detail .var", has_text="x").last.locator(".ty").inner_text() if names == want else ""
    p.locator("#detail button", has_text="Restart").click()
    emptied = until(lambda: p.locator("#detail .var").count(), 0)
    gone = pg.run(3, "x").locator(".err").inner_text()
    p.locator("#tabs button", has_text="Details").click()
    checks["the Variables tab lists the page's Python names with their types; Restart empties them"] = names == want and x_type.startswith("int") \
        and emptied == 0 and "NameError" in gone
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
    c = pg.run(0, "SELECT id, name, born, at, amt FROM people ORDER BY id")

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
    p.keyboard.press("b")
    added = pg.cells().count()
    p.keyboard.press("d")
    p.keyboard.press("d")
    deleted = pg.cells().count()
    p.keyboard.press("z")
    back = pg.cells().count()
    pg.cell(pg.cells().count() - 1).locator("textarea").click()
    p.keyboard.press("Escape")
    p.keyboard.press("b")  # (a cell below the last,)
    p.keyboard.press("m")  # (for text)
    p.keyboard.press("Enter")
    p.keyboard.insert_text("# Findings\nSome **bold** text.")
    p.keyboard.press("Shift+Enter")
    rendered = pg.cell(pg.cells().count() - 2).locator(".md h1").inner_text()
    typing = p.evaluate("document.activeElement.tagName")
    checks["keys: Esc, B adds a cell, D D deletes it, Z brings it back; M makes it text; Shift+Enter renders it and starts a new cell"] = \
        (added, deleted, back) == (n + 1, n, n + 1) and rendered == "Findings" and typing == "TEXTAREA" and pg.cells().count() == n + 3
    outline = p.locator("#outline .row")
    headed = until(lambda: outline.all_inner_texts(), ["Findings"])
    pg.cell(0).locator("textarea").click()
    outline.first.click()
    checks["a text cell's headings make the outline, and one clicked selects its cell"] = headed == ["Findings"] \
        and until(lambda: "sel" in pg.cell(pg.cells().count() - 2).get_attribute("class"), True) is True
    pg.cell(0).locator("textarea").click()
    p.keyboard.press("Escape")
    p.keyboard.press("o")
    folded = "folded" in pg.cell(0).get_attribute("class") and pg.cell(0).locator("table").is_hidden() and pg.cell(0).locator("textarea").is_visible()
    pg.cell(0).locator(".out").click()
    shown = "folded" not in pg.cell(0).get_attribute("class") and pg.cell(0).locator("table").is_visible()
    p.keyboard.press("Escape")
    p.keyboard.press("?")
    keys = until(lambda: p.locator("#helpDlg").get_attribute("open") is not None and p.locator("#keys kbd").count() > 20, True)
    p.keyboard.press("Escape")
    checks["O hides a cell's output (its code stays) and a click shows it; ? lists every key"] = folded and shown and keys is True

    p.fill("#nbname", "report")
    p.press("#nbname", "Enter")
    p.keyboard.press("Control+s")
    listed = lambda: [r["path"] for r in sql(port, "SELECT path FROM files('notebooks/report/') ORDER BY path")]
    one = until(lambda: len(listed()), 1)
    saved = call(port, "GET", "/" + listed()[0]) if one == 1 else None  # (JSON: read as such)
    nb = nbformat.reads(json.dumps(saved), as_version=4) if saved else None
    valid = _try(lambda: nbformat.validate(nb) is None)
    page_cells = p.evaluate("pondra.state.cells.map(c => [c.kind, c.src])")
    as_saved = [["markdown" if c.cell_type == "markdown" else "sql" if c.source.startswith("%%sql") else "python", c.source.removeprefix("%%sql\n")] for c in nb.cells] if nb else []
    checks["a notebook saved in the lake is valid for Jupyter (nbformat), SQL cells as %%sql"] = one == 1 and valid is True and as_saved == page_cells \
        and any(c.source.startswith("%%sql\n") for c in nb.cells) and p.locator("#dirty").is_hidden()
    pg.cell(0).locator("textarea").fill("SELECT 'changed' AS v")
    p.keyboard.press("Control+s")
    two = until(lambda: len(listed()), 2)
    pg.cell(0).locator("textarea").fill("SELECT 'not saved' AS v")
    nbs = p.locator("#notebooks")
    until(lambda: nbs.locator(".tw").first.is_visible(), True)
    nbs.locator(".tw").first.click()  # (its versions)
    nbs.locator(".kids .row").last.click()  # (the first one saved)
    reopened = until(lambda: p.evaluate("pondra.state.cells.map(c => [c.kind, c.src])"), page_cells)
    checks["saved twice: two versions; the first, opened from the sidebar, is what was saved"] = two == 2 and reopened == page_cells

    with p.expect_download() as d:
        pg.menu("Download")
    downloaded = open(d.value.path()).read()
    checks["a notebook downloaded is valid for Jupyter too"] = _try(lambda: nbformat.validate(nbformat.reads(downloaded, as_version=4)) is None) is True

    up = nbformat.v4.new_notebook(cells=[nbformat.v4.new_markdown_cell("# From Jupyter\nA `code` span."), nbformat.v4.new_code_cell("%%sql\nSELECT 42 AS answer"),
                                         nbformat.v4.new_code_cell("print('from python')")])
    path = os.path.join(tempfile.mkdtemp(prefix="pondra-console-"), "from-jupyter.ipynb")
    nbformat.write(up, path)
    with p.expect_file_chooser() as chooser:
        pg.menu("Open an .ipynb")
    chooser.value.set_files(path)
    until(lambda: p.evaluate("pondra.state.cells.map(c => c.kind)"), ["markdown", "sql", "python"])
    kinds = p.evaluate("pondra.state.cells.map(c => c.kind)")
    c = pg.run(1, "SELECT 42 AS answer")
    checks["an .ipynb uploaded opens: its text rendered (its heading the outline), its %%sql cell a SQL cell that runs"] = kinds == ["markdown", "sql", "python"] \
        and until(lambda: p.locator("#outline").text_content(), "From Jupyter") == "From Jupyter" \
        and pg.cell(0).locator(".md h1").inner_text() == "From Jupyter" and pg.grid(c) == (["answer"], [["42"]]) and p.input_value("#nbname") == "from-jupyter"

    if show:
        pg.shot(show, "console-light.png")
        dark = Page(browser, base + "/#notebook=report", "dark")
        dark.p.locator("section.cell").first.wait_for()
        dark.p.wait_for_timeout(1500)
        dark.shot(show, "console-dark.png")
        dark.ctx.close()
    checks["every request went to the node; no page errors"] = pg.left() == [] and pg.errors == [] and len(pg.seen) > 10
    info = {"figure": fig_said, "outline": p.locator("#outline").text_content(), "left": pg.left(), "errors": pg.errors, "sql_error": sql_error, "python_error": python_error, "kinds": kinds, "columns": columns, "facts": facts}
    pg.ctx.close()
    return checks, info


def token_checks(browser, port):
    token = "console-check-admin-token"
    lake = tempfile.mkdtemp(prefix="pondra-")
    node = Node(lake, port, admin_token=token, read_token="console-check-reader").start()
    try:
        harness.call(port, "POST", "/sql", b"CREATE TABLE secret_things AS SELECT 1 AS id", headers={"authorization": f"Bearer {token}"})
        pg = Page(browser, f"http://127.0.0.1:{port}/")
        asked = until(lambda: pg.p.locator("#tokenDlg").get_attribute("open") is not None, True)
        hidden = pg.p.locator("#tree .row", has_text="secret_things").count() == 0
        pg.p.fill("#tokenIn", token)
        pg.p.press("#tokenIn", "Enter")
        shown = until(lambda: pg.p.locator("#tree .row", has_text="secret_things").count(), 1)
        pg.p.reload()
        again = until(lambda: pg.p.locator("#tree .row", has_text="secret_things").count(), 1)
        def python_of(who):
            try:
                return harness.call(port, "GET", "/sessions/some-page/python", headers={"authorization": f"Bearer {who}"})
            except Exception as e:  # noqa: BLE001 (refused: what it said)
                return str(e)
        checks = {"with tokens, the page asks for one, then shows the tables (and after a reload)": asked is True and hidden and shown == 1 and again == 1
                  and pg.p.locator("#tokenDlg").get_attribute("open") is None,
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
        tree = pg.p.locator("#tree")
        tree.locator(".row", has_text="notes").wait_for(timeout=30000)
        listed = [t.split("\n")[0] for t in tree.locator(":scope > div > .line .row").all_inner_texts()]
        tree.locator(".row", has_text="sales").first.click()
        tree.locator(".row", has_text="orders").wait_for(timeout=30000)
        c = pg.run(0, "SELECT sum(amount) AS total FROM orders")
        checks = {"the server's console lists its databases; a cell runs in the one picked (/db/sales)": listed == ["lake", "sales"] and pg.grid(c) == (["total"], [["30.5"]])
                  and any("/db/sales/sql" in u for u in pg.seen) and "#db=sales" in pg.p.url,
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
    node = Node(lake, port, env={"PONDRA_CONSOLE_EXTENSIONS": os.path.abspath(ext)}).start()
    try:
        sql(port, "CREATE TABLE things AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) v(id, name)")
        served = urllib.request.urlopen(f"http://127.0.0.1:{port}/console/ext/0.js").read().decode() == open(ext, encoding="utf-8").read()
        first = urllib.request.urlopen(f"http://127.0.0.1:{port}/console/console.js")
        tag = first.headers["etag"]
        try:
            again = urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}/console/console.js", headers={"if-none-match": tag})).status
        except urllib.error.HTTPError as e:
            again = e.code
        pg = Page(browser, f"http://127.0.0.1:{port}/")
        p = pg.p
        p.locator("#tree .row", has_text="things").wait_for(timeout=20000)
        section = p.locator("#historyTitle").text_content() == "History" and p.locator("#history .empty").count() == 1
        c = pg.run(0, "SELECT count(*) AS n FROM things")
        figure = c.locator(".ext-figure").inner_text().split("\n")
        history = until(lambda: p.locator("#history .row").all_inner_texts(), ["SELECT count(*) AS n FROM things"])
        p.locator("#tree .row", has_text="things").click()
        p.locator("#tabs button", has_text="Sample").click()
        sample = until(lambda: p.locator("#detail pre.said").count(), 3)
        p.click("#moreBtn")
        action = p.locator("#menu button", has_text="Copy a link to this notebook").count() == 1
        p.keyboard.press("Escape")
        checks = {"an extension is served (/console/ext/0.js), and its section, panel tab, view of an answer and menu action show beside the console's own":
                  served and section and figure == ["3", "n"] and history == ["SELECT count(*) AS n FROM things"] and sample == 3 and action,
                  "the console's files carry a tag, and a browser that has them gets 304": bool(tag) and again == 304,
                  "with an extension: every request went to the node; no page errors": pg.left() == [] and pg.errors == []}
        info = {"served": served, "section": section, "action": action, "figure": figure, "history": history, "sample": sample, "etag": tag, "again": again, "errors": pg.errors}
        pg.ctx.close()
        return checks, info
    finally:
        node.kill()
        shutil.rmtree(lake, ignore_errors=True)


def _try(f):
    try:
        return f()
    except Exception as e:  # noqa: BLE001
        return e


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8890)
    ap.add_argument("--show", help="keep screenshots here")
    ap.add_argument("--only", help="run only these parts (node, tokens, server, extensions), separated by commas")
    A = harness.A = ap.parse_args()
    A.s3, A.keep = False, False
    if A.show:
        os.makedirs(A.show, exist_ok=True)
    lake = tempfile.mkdtemp(prefix="pondra-")
    node = Node(lake, A.port, env={"PYTHONPATH": os.path.join(HERE, "..", "python")}, python=sys.executable).start()
    results, said = {}, {}
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            for part, f in [("node", lambda: node_checks(browser, A.port, A.show)), ("tokens", lambda: token_checks(browser, A.port + 1)), ("server", lambda: server_checks(browser, A.port + 2)),
                            ("extensions", lambda: ext_checks(browser, A.port + 3))]:
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
    if not ok:
        print(json.dumps(said, indent=1, default=str)[:8000])
    print(json.dumps({"checks": len(results), "passed": sum(results.values()), "ok": ok}))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
