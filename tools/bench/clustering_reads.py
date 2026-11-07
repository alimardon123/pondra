#!/usr/bin/env python3
"""What clustering saves, counted in what a query reads (the Lindel review's pick C2, 2026-11-07; ADR-021 §4).

Lindel's setup: random points (x, y in [0, 1)), and a small box (x and y each within 0.01: 1/10,000 of the space). Plus
our own losing case, a filter on x alone. The same rows go in three tables, `cluster_by` none, 'x', and 'x, y' (a
Hilbert curve). Each table is laid out two ways: "one file", the first 4 M rows (the most one fold takes,
`tier::tier_table`'s MAX_ROWS, so they are sorted as one), and "many files", every row in `--files` folds of a file each
(a file of 4 M rows or more is never merged, `tier::maintain`, so the default 20 M rows in 5 keep their files). Each
fold sorts its own rows, so today every file of a clustered table spans the whole of its columns.

For each query on each table it counts what was read:
- files read and skipped by the catalog's min/max (`pondra_files_scanned_total`, `pondra_files_skipped_total`);
- row groups and pages the Parquet reader kept of those it had (EXPLAIN ANALYZE: `row_groups_pruned_statistics`,
  `page_index_pages_pruned`), and the rows it decoded (`page_index_rows_pruned`'s matched);
- the median time of `--queries` runs, each with its own box, with decoded columns kept in memory off
  (PONDRA_HOT_GB=0), so the files are what is read.
Every table's answer is checked against the plain table's. The fourth row, Hilbert with curve keys that hold across
files (pick C1), is listed with no numbers until C1 is built; C1 merges only if no row here gets slower.

  clustering_reads.py [--rows 20000000] [--files 5] [--queries 20] [--port 9480] [--out FILE]
"""
import argparse, io, json, os, random, re, statistics, sys, tempfile, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import harness
from harness import Node, call

TABLES = [("no order", None), ("one column (x)", "x"), ("Hilbert (x, y) today", "x, y")]
QUERIES = [
    ("a small box", "SELECT count(*) AS n FROM {t} WHERE x BETWEEN {a} AND {a} + 0.01 AND y BETWEEN {b} AND {b} + 0.01"),
    ("x alone", "SELECT count(*) AS n FROM {t} WHERE x BETWEEN {a} AND {a} + 0.0001"),
]
ONE_FOLD = 4_000_000  # (`tier::tier_table`'s MAX_ROWS: what one fold sorts together)
PRUNED = re.compile(r"(\w+)=([\d.]+ ?[KMG]?) total → ([\d.]+ ?[KMG]?) matched")


def number(text):
    text = text.replace(" ", "")
    scale = {"K": 1e3, "M": 1e6, "G": 1e9}.get(text[-1:], 1)
    return round(float(text.rstrip("KMG")) * scale)


def metric(port, name):
    for line in call(port, "GET", "/metrics").decode().splitlines():
        if line.startswith(f"pondra_{name} "):
            return float(line.split()[1])
    return 0.0


def reads(port, sql):
    """What one run of `sql` read: files from the node's counters, row groups, pages and rows from its plan's metrics."""
    before = (metric(port, "files_scanned_total"), metric(port, "files_skipped_total"))
    plan = "".join(r["plan"] for r in call(port, "POST", "/sql", ("EXPLAIN ANALYZE " + sql).encode(), timeout=600))
    read, skipped = metric(port, "files_scanned_total") - before[0], metric(port, "files_skipped_total") - before[1]
    got = {"files read": [int(read), int(read + skipped)], "row groups read": [0, 0], "pages read": [0, 0], "rows decoded": 0}  # (read of all)
    for name, total, kept in PRUNED.findall(plan):
        if name in ("row_groups_pruned_statistics", "page_index_pages_pruned"):
            k = "row groups read" if name.startswith("row") else "pages read"
            got[k] = [got[k][0] + number(kept), got[k][1] + number(total)]
        elif name == "page_index_rows_pruned":
            got["rows decoded"] += number(kept)
    return got


def batches(rows, per, seed=7):
    import pyarrow as pa
    rng = random.Random(seed)
    for start in range(0, rows, per):
        n = min(per, rows - start)
        rb = pa.record_batch([pa.array(range(start, start + n), pa.int64()), pa.array([rng.random() for _ in range(n)], pa.float64()),
                              pa.array([rng.random() for _ in range(n)], pa.float64())], names=["id", "x", "y"])
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, rb.schema) as w:
            w.write_batch(rb)
        yield buf.getvalue()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--rows", type=int, default=20_000_000)
    ap.add_argument("--files", type=int, default=5, help="the many-files layout's files (one fold per batch)")
    ap.add_argument("--queries", type=int, default=20)
    ap.add_argument("--port", type=int, default=9480)
    ap.add_argument("--out")
    a = ap.parse_args()
    lake = tempfile.mkdtemp(prefix="pondra-clustering-")
    node = Node(lake, a.port, tier_secs=0, env={"PONDRA_HOT_GB": "0"}).start()
    out = {"rows": a.rows, "files (many)": a.files, "queries": a.queries, "binary": harness.BIN, "layouts": {}}
    try:
        names = {}
        for layout in ("one file", "many files"):
            for i, (label, by) in enumerate(TABLES):
                t = f"{'one' if layout == 'one file' else 'many'}_{i}"
                names[(layout, label)] = t
                with_ = f" WITH (cluster_by = '{by}')" if by else ""
                call(a.port, "POST", "/sql", f"CREATE TABLE {t} (id BIGINT, x DOUBLE, y DOUBLE){with_}".encode())
        t0, per = time.time(), -(-a.rows // a.files)
        append = lambda t, seq, body: call(a.port, "POST", f"/append/{t}?producer=load-{t}&seq={seq}", body, headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=600)
        for seq, body in enumerate(batches(a.rows, per), 1):  # (the same rows in every table: one fold per batch, a file each)
            for (layout, _), t in names.items():
                if layout == "many files":
                    append(t, seq, body)
            call(a.port, "POST", "/tier", timeout=3600)
        for seq, body in enumerate(batches(min(a.rows, ONE_FOLD), min(per, ONE_FOLD)), 1):  # (then one fold's rows, the same first ones)
            for (layout, _), t in names.items():
                if layout == "one file":
                    append(t, seq, body)
        call(a.port, "POST", "/tier", timeout=3600)
        out["load and fold (s)"] = round(time.time() - t0, 1)
        rng = random.Random(11)
        boxes = [(round(rng.random() * 0.99, 6), round(rng.random() * 0.99, 6)) for _ in range(a.queries)]
        for layout in ("one file", "many files"):
            rows = {}
            for label, _ in TABLES:
                t = names[(layout, label)]
                files = harness.subprocess.run([harness.BIN, "catalog", "--dir", lake, f"t/{t}"], capture_output=True, text=True).stdout
                row = {"files": len(json.loads(files.split(" ", 1)[1])["files"])}
                for qname, q in QUERIES:
                    count = reads(a.port, q.format(t=t, a=boxes[0][0], b=boxes[0][1]))
                    lat, answers = [], []
                    for i, (x, y) in enumerate(boxes):
                        s = time.perf_counter()
                        answers.append(call(a.port, "POST", "/sql", (q.format(t=t, a=x, b=y) + f" -- {i}").encode(), timeout=600)[0]["n"])
                        lat.append(1000 * (time.perf_counter() - s))
                    row[qname] = {**count, "median ms": round(statistics.median(lat), 2), "answers": answers}
                rows[label] = row
            plain = {qname: rows[TABLES[0][0]][qname]["answers"] for qname, _ in QUERIES}
            for label, row in rows.items():
                for qname, _ in QUERIES:
                    row[qname]["same answers"] = row[qname].pop("answers") == plain[qname]
            rows["Hilbert (x, y) with C1"] = "not built yet (pick C1, steps 5 and 11)"
            out["layouts"][layout] = rows
    finally:
        node.kill()
    text = json.dumps(out, indent=1)
    print(text)
    if a.out:
        with open(a.out, "w") as f:
            f.write(text + "\n")


if __name__ == "__main__":
    main()
