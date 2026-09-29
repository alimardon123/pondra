#!/usr/bin/env python3
"""The console (ADR-030), in headless Chromium through Playwright.

  console_check.py [--port 8890] [--show DIR]

- node: the tree lists the lake's schemas, tables (with their row counts), views and columns; a
  SQL cell and a Python cell run (what the code printed, and its last expression as rows);
  errors come back in plain words, a Python error at its line in the cell; a live cell shows a
  row INSERTed over HTTP, and its query ends when the switch is off; the keys (Esc, B, D D, Z,
  Shift+Enter); a notebook saved in the lake (twice: two versions) and opened again is the one
  saved, and Jupyter's nbformat validates it; so does the one downloaded; an .ipynb uploaded
  opens, its text rendered and its %%sql cell run.
- tokens: a node with tokens serves the page, which asks for one; given it, the tables show,
  and after a reload too (the browser keeps it).
- server: `pondra server`'s console lists its databases, and a cell runs in the one picked.
- every request the page made went to the node (or server) that served it.

--show DIR keeps screenshots, light and dark.
Needs: playwright (Chromium at PLAYWRIGHT_BROWSERS_PATH) and nbformat.
"""
import argparse, json, os, shutil, subprocess, sys, tempfile, time

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
        rows = [[t.strip() for t in r.split("\t")[1:]] for r in c.locator("tbody tr").all_inner_texts()]
        return heads, rows

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
    counted = until(lambda: people.locator(".ct").inner_text(), "3")
    tree.locator(".line", has_text="sales").locator(".tw").click()
    tree.locator(".row", has_text="orders").wait_for(timeout=10000)
    people.locator(".tw").click()
    columns = tree.locator(".col:visible").all_inner_texts()
    checks["the tree lists the lake's schemas, tables with their row counts, views and columns (with SQL types)"] = counted == "3" \
        and tree.locator(".line", has_text="grown").locator(".ct").inner_text() == "view" \
        and [c.split("\n")[0] for c in columns] == ["id", "name", "born", "at", "amt"] and "DECIMAL(10,2)" in columns[4] and "TIMESTAMP" in columns[3]

    c = pg.run(0, "SELECT id, name, born, at, amt FROM people ORDER BY id")
    heads, rows = pg.grid(c)
    checks["a SQL cell shows the rows with their types; timestamps and decimals as written"] = heads == ["id", "name", "born", "at", "amt"] \
        and rows[0] == ["1", "Ann", "1990-01-02", "2024-01-01 10:00:00", "1.50"] and rows[1][2] == "NULL" and "3 rows" in c.locator(".meta").inner_text() \
        and "BIGINT" in c.locator("thead").inner_text()
    tree.locator(".row", has_text="orders").click()  # (a table's first rows, in a new cell)
    peek = until(lambda: pg.grid(pg.cell(1))[1], [["1", "10.5"]])
    checks["clicking a table shows its first rows"] = peek == [["1", "10.5"]] and "sales.orders" in pg.cell(1).locator("textarea").input_value()

    p.click("[data-add=python]")
    p.keyboard.insert_text('for i in range(2):\n    print("hello", i)\ndb.sql("SELECT count(*) AS n FROM people")')
    p.keyboard.press("Control+Enter")
    py = pg.cell(2)
    py.locator("table").wait_for(timeout=60000)
    checks["a Python cell runs on the node: what it printed, then its last expression as rows"] = py.locator(".said").inner_text() == "hello 0\nhello 1" and pg.grid(py) == (["n"], [["3"]])
    c = pg.run(2, "x = 1\n1 / 0")
    python_error = c.locator(".err").inner_text()
    c = pg.run(1, "SELECT nope FROM people")
    sql_error = c.locator(".err").inner_text()
    checks["errors in plain words: a SQL one, and a Python one at its line in the cell"] = "nope" in sql_error and "ZeroDivisionError" in python_error and "line 2" in python_error

    live = pg.run(1, "SELECT count(*) AS n FROM people")
    live.locator("label.live").click()
    started = until(lambda: "live" in live.locator(".st").inner_text() and call(port, "GET", "/stats")["live_queries"] == 1, True)
    sql(port, "INSERT INTO people (id, name) VALUES (4, 'Di')")
    updated = until(lambda: pg.grid(live)[1], [["4"]])
    live.locator("label.live").click()
    ended = until(lambda: call(port, "GET", "/stats")["live_queries"], 0)
    checks["a live cell shows a row INSERTed over HTTP; off, its query ends"] = started is True and updated == [["4"]] and ended == 0

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

    p.fill("#nbname", "report")
    p.press("#nbname", "Enter")
    p.keyboard.press("Control+s")
    listed = lambda: [r["path"] for r in sql(port, "SELECT path FROM files('notebooks/report/') ORDER BY path")]
    one = until(lambda: len(listed()), 1)
    saved = call(port, "GET", "/" + listed()[0]) if one == 1 else None  # (JSON: read as such)
    nb = nbformat.reads(json.dumps(saved), as_version=4) if saved else None
    valid = _try(lambda: nbformat.validate(nb) is None)
    page_cells = p.evaluate("S.cells.map(c => [c.kind, c.src])")
    as_saved = [["markdown" if c.cell_type == "markdown" else "sql" if c.source.startswith("%%sql") else "python", c.source.removeprefix("%%sql\n")] for c in nb.cells] if nb else []
    checks["a notebook saved in the lake is valid for Jupyter (nbformat), SQL cells as %%sql"] = one == 1 and valid is True and as_saved == page_cells \
        and any(c.source.startswith("%%sql\n") for c in nb.cells) and not p.locator("#dirty").inner_text()
    pg.cell(0).locator("textarea").fill("SELECT 'changed' AS v")
    p.keyboard.press("Control+s")
    two = until(lambda: len(listed()), 2)
    pg.cell(0).locator("textarea").fill("SELECT 'not saved' AS v")
    nbs = p.locator("#notebooks")
    until(lambda: nbs.locator(".tw").first.is_visible(), True)
    nbs.locator(".tw").first.click()  # (its versions)
    nbs.locator(".kids .row").last.click()  # (the first one saved)
    reopened = until(lambda: p.evaluate("S.cells.map(c => [c.kind, c.src])"), page_cells)
    checks["saved twice: two versions; the first, opened from the sidebar, is what was saved"] = two == 2 and reopened == page_cells

    with p.expect_download() as d:
        p.click("#download")
    downloaded = open(d.value.path()).read()
    checks["a notebook downloaded is valid for Jupyter too"] = _try(lambda: nbformat.validate(nbformat.reads(downloaded, as_version=4)) is None) is True

    up = nbformat.v4.new_notebook(cells=[nbformat.v4.new_markdown_cell("# From Jupyter\nA `code` span."), nbformat.v4.new_code_cell("%%sql\nSELECT 42 AS answer"),
                                         nbformat.v4.new_code_cell("print('from python')")])
    path = os.path.join(tempfile.mkdtemp(prefix="pondra-console-"), "from-jupyter.ipynb")
    nbformat.write(up, path)
    p.set_input_files("#upload", path)
    until(lambda: p.evaluate("S.cells.map(c => c.kind)"), ["markdown", "sql", "python"])
    kinds = p.evaluate("S.cells.map(c => c.kind)")
    c = pg.run(1, "SELECT 42 AS answer")
    checks["an .ipynb uploaded opens: its text rendered, its %%sql cell a SQL cell that runs"] = kinds == ["markdown", "sql", "python"] \
        and pg.cell(0).locator(".md h1").inner_text() == "From Jupyter" and pg.grid(c) == (["answer"], [["42"]]) and p.input_value("#nbname") == "from-jupyter"

    if show:
        pg.shot(show, "console-light.png")
        dark = Page(browser, base + "/#notebook=report", "dark")
        dark.p.locator("section.cell").first.wait_for()
        dark.p.wait_for_timeout(1500)
        dark.shot(show, "console-dark.png")
        dark.ctx.close()
    checks["every request went to the node; no page errors"] = pg.left() == [] and pg.errors == [] and len(pg.seen) > 10
    info = {"left": pg.left(), "errors": pg.errors, "sql_error": sql_error, "python_error": python_error, "counted": counted, "columns": columns}
    pg.ctx.close()
    return checks, info


def token_checks(browser, port):
    token = "console-check-admin-token"
    lake = tempfile.mkdtemp(prefix="pondra-")
    node = Node(lake, port, admin_token=token).start()
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
        checks = {"with tokens, the page asks for one, then shows the tables (and after a reload)": asked is True and hidden and shown == 1 and again == 1
                  and pg.p.locator("#tokenDlg").get_attribute("open") is None}
        pg.ctx.close()
        return checks, {}
    finally:
        node.kill()
        shutil.rmtree(lake, ignore_errors=True)


def server_checks(browser, port):
    folder = tempfile.mkdtemp(prefix="pondra-")
    for name, q in [("sales", "CREATE TABLE orders AS SELECT 1 AS id, 10.5 AS amount UNION ALL SELECT 2, 20.0"), ("lake", "CREATE TABLE notes AS SELECT 'hi' AS text")]:
        subprocess.run([BIN, "sql", "--dir", os.path.join(folder, name), q], check=True, capture_output=True)
    srv = subprocess.Popen([BIN, "server", folder, "--addr", f"127.0.0.1:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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


def _try(f):
    try:
        return f()
    except Exception as e:  # noqa: BLE001
        return e


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8890)
    ap.add_argument("--show", help="keep screenshots here")
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
            for part, f in [("node", lambda: node_checks(browser, A.port, A.show)), ("tokens", lambda: token_checks(browser, A.port + 1)), ("server", lambda: server_checks(browser, A.port + 2))]:
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
