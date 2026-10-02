#!/usr/bin/env python3
"""The website's pictures of the console (site/src/assets/console*.png), taken again: a lake named
shop, a notebook with text, SQL and Python, a SQL file with its results, a table picked.

  console_shots.py [--out site/src/assets] [--dark DIR]   (--dark: the dark theme's too, there)

Needs playwright (Chromium at PLAYWRIGHT_BROWSERS_PATH) and a release build.
"""
import argparse, os, shutil, subprocess, sys, tempfile, time, urllib.request

from playwright.sync_api import sync_playwright

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("PONDRA_BIN", os.path.join(ROOT, "target", "release", "pondra"))
PORT = 8898


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(ROOT, "site", "src", "assets"))
    ap.add_argument("--dark", help="also the dark theme's picture, in this folder")
    a = ap.parse_args()
    home = tempfile.mkdtemp(prefix="pondra-shots-")
    node = subprocess.Popen([BIN, "serve", "--dir", os.path.join(home, "shop"), "--addr", f"127.0.0.1:{PORT}", "--python", sys.executable],
                            env={**os.environ, "PYTHONPATH": os.path.join(ROOT, "python"), "PONDRA_CONFIG_DIR": os.path.join(home, "settings")}, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    base = f"http://127.0.0.1:{PORT}"
    send = lambda path, body, method="POST": urllib.request.urlopen(urllib.request.Request(base + path, data=body, method=method)).read()
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(base + "/stats")
                break
            except OSError:
                time.sleep(0.1)
        for q in ["CREATE TABLE sales (day DATE, item VARCHAR, region VARCHAR, amount DECIMAL(10,2), customer_id BIGINT)",
                  "INSERT INTO sales SELECT DATE '2026-03-01' + CAST(value % 30 AS INT), ['tea','cake','coffee','juice'][value % 4 + 1], ['west','south','north'][value % 3 + 1], "
                  "CAST(5 + (value * 37 % 400) / 10.0 AS DECIMAL(10,2)), value % 120 FROM range(0, 917)",
                  "CREATE TABLE customers AS SELECT value AS id, 'customer ' || value AS name FROM range(0, 120)",
                  "CREATE VIEW by_region AS SELECT region, sum(amount) AS revenue FROM sales GROUP BY region"]:
            send("/sql", q.encode())
        send("/files/etl/load_orders.sql", b"SELECT region, count(*) AS orders, sum(amount) AS revenue\nFROM sales\nGROUP BY region\nORDER BY revenue DESC;\n", "PUT")
        send("/files/etl/score_customers.py", b'scores = db.sql("SELECT customer_id, sum(amount) AS spent FROM sales GROUP BY customer_id")\nprint("customers:", len(scores.rows()))\n', "PUT")
        send("/files/data/q1_targets.csv", b"region,target\nwest,6000\nsouth,6000\nnorth,5500\n", "PUT")
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            for scheme in ["light"] + (["dark"] if a.dark else []):
                ctx = browser.new_context(viewport={"width": 1440, "height": 920}, color_scheme=scheme)
                p = ctx.new_page()
                p.goto(base + ("/" if scheme == "light" else "/#notebook=sales-q1"))
                p.locator("#data .row", has_text="sales").wait_for(timeout=20000)
                cells = p.locator("#docs section.cell")
                if scheme == "light":
                    p.fill("#nbname", "sales-q1")
                    p.press("#nbname", "Enter")
                    cells.first.locator(".kind").click()
                    p.locator("#menu button", has_text="Markdown").click()
                    cells.first.locator("textarea").fill("# Sales by region\nYesterday's orders from the lake, against the Q1 targets.")
                    cells.first.locator("textarea").press("Shift+Enter")
                    cells.nth(1).locator("textarea").fill("SELECT region, count(*) AS orders, sum(amount) AS revenue\nFROM sales GROUP BY region ORDER BY revenue DESC")
                    cells.nth(1).locator("textarea").press("Control+Enter")
                    cells.nth(1).locator(".gt").wait_for(timeout=20000)
                    p.click("[data-add=python]")
                    p.keyboard.insert_text('by_day = db.sql("SELECT day, sum(amount) AS total FROM sales GROUP BY day ORDER BY day")\nprint("days:", len(by_day.rows()))')
                    p.keyboard.press("Control+Enter")
                    cells.nth(2).locator(".said").wait_for(timeout=60000)
                    p.keyboard.press("Control+s")
                    p.wait_for_timeout(4500)  # (its toast gone)
                    grid = cells.nth(1).locator("tbody tr")
                    grid.nth(0).locator("td").nth(3).click()
                    grid.nth(2).locator("td").nth(3).click(modifiers=["Shift"])
                else:
                    cells.nth(1).locator(".run").click()
                    cells.nth(1).locator(".gt").wait_for(timeout=20000)
                p.locator("#workspace .row", has_text="etl").click()
                p.locator("#data .row[data-kind=table]", has_text="sales").click()
                p.wait_for_timeout(1200)
                p.mouse.move(700, 880)
                p.screenshot(path=os.path.join(a.out if scheme == "light" else a.dark, "console.png" if scheme == "light" else "console-dark.png"))
                if scheme == "light":
                    p.locator("#workspace .row", has_text="load_orders.sql").click()
                    p.click("#runBtn")
                    p.locator(".filedoc .gt").wait_for(timeout=20000)
                    p.wait_for_timeout(500)
                    p.mouse.move(700, 880)
                    p.screenshot(path=os.path.join(a.out, "console-sql-file.png"))
                ctx.close()
            browser.close()
    finally:
        node.kill()
        shutil.rmtree(home, ignore_errors=True)


if __name__ == "__main__":
    main()
