#!/usr/bin/env python3
"""js/frame.js == python/pondra/frame.py, and the JavaScript client's other calls (ADR-023, ADR-025).

1. Every pipeline of tools/frames_check.py, built in Python and in JavaScript over the same tables:
   the same SQL text, the same column names, and equal rows (sorted unless the pipeline sorts; floats
   to a relative 1e-9). Python starts the node (pondra.local()); the JavaScript side connects to it.
2. `await db.sql("SELECT 1 AS x")` is [{x: 1}]; `CREATE TABLE t2 (a INT)`; `readParquet` of a Parquet
   file this check writes; `writeTable` with rows (create, overwrite, append) and with a frame, each
   read back. JavaScript starts this node itself (a node's reads of local files need the program that
   started it: invariant 61), after the first node is closed: one node at a time.
3. `unique`'s keep and maintainOrder are taken on a sorted frame (pipelines above) and refused by name, with
   the message, on one that isn't; `groupBy`'s maintainOrder is refused by name.

  python3 tools/js_frames_check.py [--port 8831] [--bin target/release/pondra]

Needs node (18+) and pyarrow; the binary is `--bin`, else `PONDRA_BIN`, else the release build.
Prints "ok …" or "FAIL …" for each check, and exits 1 on any failure.
"""
import argparse, datetime as dt, decimal, json, math, os, random, shutil, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
JS = os.path.join(HERE, "js_frames_pipelines.mjs")
sys.path.insert(0, os.path.join(HERE, "..", "python"))

import pyarrow as pa
import pyarrow.parquet as pq

import pondra
from pondra import col, lit, when

DEFAULT_BIN = os.environ.get("PONDRA_BIN", os.path.join(HERE, "..", "target", "release", "pondra"))


# ---------------------------------------------------------------- the tables (frames_check.py's data())

def data(n=3000, seed=11):
    """Orders, users and quotes, with nulls in places, as Arrow."""
    r = random.Random(seed)
    t0 = dt.datetime(2026, 9, 1)
    orders = pa.table({
        "id": list(range(n)),
        "user": [f"u{r.randrange(40)}" for _ in range(n)],
        "item": [r.choice(["tea", "cake", "coffee", "bun", "Tea Pot", "juice"]) for _ in range(n)],
        "qty": [None if r.random() < 0.05 else r.randrange(10) for _ in range(n)],
        "price": [None if r.random() < 0.05 else round(r.uniform(0.5, 40), 2) for _ in range(n)],
        "ts": [t0 + dt.timedelta(seconds=r.randrange(30 * 86400)) for _ in range(n)],
        "region": [r.choice(["north", "south", None]) for _ in range(n)],
    })
    users = pa.table({"user": [f"u{i}" for i in range(45)], "tier": [["gold", "plain", "new"][i % 3] for i in range(45)],
                      "joined": [dt.date(2026, 1, 1) + dt.timedelta(days=5 * i) for i in range(45)]})
    quotes = pa.table({"item": [r.choice(["tea", "cake", "coffee"]) for _ in range(400)],
                       "at": sorted(t0 + dt.timedelta(seconds=r.randrange(30 * 86400)) for _ in range(400)),
                       "px": [round(r.uniform(1, 9), 2) for _ in range(400)]})
    return {"orders": orders, "users": users, "quotes": quotes}


SQL_TYPES = {"int64": "BIGINT", "string": "VARCHAR", "double": "DOUBLE", "timestamp[us]": "TIMESTAMP", "date32[day]": "DATE"}


def load(con, tables):
    for name, t in tables.items():
        cols = ", ".join(f'"{f.name}" {SQL_TYPES[str(f.type)]}' for f in t.schema)
        con.sql(f"CREATE TABLE {name} ({cols})")
        con.append(name, t)


# ---------------------------------------------------------------- the pipelines (frames_check.py's, Python side)
# [name, build(con, orders, users, quotes), ordered]: the same list, in the same order, as
# js_frames_pipelines.mjs has it. The order matters: the CTE names count up through the process.

PIPELINES = [
    ("filter + select", lambda c, o, u, q: o.filter(col("qty") > 3).select("id", "user", "qty"), False),
    ("with_columns: new and replaced", lambda c, o, u, q: o.with_columns((col("qty") * col("price")).alias("total"), (col("price") * 2).alias("price")), False),
    ("group_by: sum mean min max count n_unique len", lambda c, o, u, q: o.group_by("user").agg(
        col("qty").sum().alias("s"), col("price").mean().alias("m"), col("price").min().alias("lo"), col("price").max().alias("hi"),
        col("qty").count().alias("c"), col("item").n_unique().alias("items"), pondra.len()), False),
    ("group_by two keys, std and var", lambda c, o, u, q: o.group_by("user", "item").agg(col("price").std().alias("sd"), col("price").var().alias("v")), False),
    ("sort desc (nulls first) + limit", lambda c, o, u, q: o.sort("price", "id", descending=[True, False]).limit(15), True),
    ("sort nulls_last, then filter and select (the sort kept)", lambda c, o, u, q: o.sort("qty", "id", nulls_last=True).filter(col("price") > 20).select("id", "qty").head(20), True),
    ("join inner", lambda c, o, u, q: o.join(u, on="user"), False),
    ("join left", lambda c, o, u, q: o.join(u, on="user", how="left"), False),
    ("join full", lambda c, o, u, q: u.join(o.select("user", "id"), on="user", how="full"), False),
    ("join semi / anti", lambda c, o, u, q: pondra.concat([u.join(o.filter(col("qty") == 9), on="user", how="semi"), u.join(o, on="user", how="anti")]), False),
    ("join left_on/right_on, a clashing name", lambda c, o, u, q: o.select("id", "user", "item").join(u.with_columns(col("tier").alias("item")), left_on="user", right_on="user"), False),
    ("join cross (small)", lambda c, o, u, q: u.filter(col("tier") == "gold").select("user").join(u.filter(col("tier") == "new").select("user"), how="cross"), False),
    ("join_asof backward by", lambda c, o, u, q: o.filter(col("item").is_in(["tea", "cake"])).sort("ts").join_asof(q.sort("at"), left_on="ts", right_on="at", by="item").select("id", "px"), False),
    ("unique", lambda c, o, u, q: o.select("user", "item").unique(), False),
    ("unique(keep='none') on a subset: the values that occur once", lambda c, o, u, q: o.filter(col("id") < 300).select("id", "user", "item").unique(["user", "item"], keep="none"), False),
    ("unique(keep='none') of every column", lambda c, o, u, q: o.filter(col("id") < 300).select("user", "item", "qty").unique(keep="none"), False),
    ("sort, then unique(keep='first') on a subset, the sort kept", lambda c, o, u, q: o.sort(["user", "id"]).select("id", "user", "item").unique("user", keep="first", maintain_order=True), True),
    ("sort, then unique(keep='last'), descending with nulls last", lambda c, o, u, q: o.sort(["user", "qty", "id"], descending=[False, True, False], nulls_last=[False, True, False])
        .select("id", "user", "qty").unique("user", keep="last", maintain_order=True), True),
    ("sort, then unique(keep='last') of every column", lambda c, o, u, q: o.sort(["user", "item"]).select("user", "item").unique(keep="last", maintain_order=True), True),
    ("drop, rename, cast", lambda c, o, u, q: o.drop("ts", "region").rename({"qty": "n"}).cast({"n": "Float64"}), False),
    ("when / then / otherwise", lambda c, o, u, q: o.select("id", when(col("qty") > 6).then(lit("big")).when(col("qty") > 2).then(lit("mid")).otherwise(lit("small")).alias("size")), False),
    ("is_in, is_between, is_null, fill_null", lambda c, o, u, q: o.select("id", col("item").is_in(["tea", "bun"]).alias("a"), col("price").is_between(5, 10).alias("b"),
        col("qty").is_null().alias("c"), col("qty").fill_null(-1).alias("d")), False),
    ("fill_null over the frame, drop_nulls", lambda c, o, u, q: o.fill_null(0).drop_nulls("region"), False),
    ("str: case, contains, starts_with, len, slice, replace_all", lambda c, o, u, q: o.select("id", col("item").str.to_uppercase().alias("up"), col("item").str.contains("ea").alias("ea"),
        col("item").str.starts_with("co").alias("co"), col("item").str.len_chars().alias("n"), col("item").str.slice(1, 2).alias("s"),
        col("item").str.replace_all("e", "E").alias("r")), False),
    ("dt: year month day weekday hour, truncate, date", lambda c, o, u, q: o.select("id", col("ts").dt.year().alias("y"), col("ts").dt.month().alias("m"), col("ts").dt.day().alias("d"),
        col("ts").dt.weekday().alias("wd"), col("ts").dt.hour().alias("h"), col("ts").dt.truncate("1d").alias("day"), col("ts").dt.truncate("6h").alias("q"),
        col("ts").dt.date().alias("date")), False),
    ("over: sum, rank dense, shift, cum_sum", lambda c, o, u, q: o.filter(col("qty").is_not_null()).select("id", col("qty").sum().over("user").alias("s"),
        col("qty").rank("dense").over("user").alias("r"), col("qty").shift(1).over("user", order_by="id").alias("prev"),
        col("qty").cum_sum().over("user", order_by="id").alias("run")), False),
    ("rank average", lambda c, o, u, q: o.filter(col("user") == "u3").select("id", col("qty").rank().alias("r")), False),
    ("concat vertical and diagonal", lambda c, o, u, q: pondra.concat([o.select("id", "user").limit(0),
        pondra.concat([u.select("user"), o.select("user", "id").filter(col("id") < 5)], how="diagonal")], how="diagonal"), False),
    ("division, floor division, modulo", lambda c, o, u, q: o.filter(col("qty") > 0).select("id", (col("id") / col("qty")).alias("d"),
        (col("id") % col("qty")).alias("m"), (col("price") // 3).alias("f")), False),
    ("floor division, negatives too (rounded down; integers stay integers)", lambda c, o, u, q: o.filter(col("qty") > 0).select("id",
        (col("id") // col("qty")).alias("a"), ((-col("id")) // col("qty")).alias("b"), (col("price") // -3).alias("c"), (100 // col("qty")).alias("d")), False),
    ("round, abs, sqrt, clip", lambda c, o, u, q: o.select("id", col("price").round(1).alias("r"), (-col("price")).abs().alias("a"),
        col("price").sqrt().alias("s"), col("price").clip(5, 30).alias("c")), False),
    ("SQL snippets inside frame methods", lambda c, o, u, q: o.filter("qty > 2 AND item LIKE 't%'").with_columns("qty * price AS total")
        .group_by("item").agg("sum(total) AS revenue", "count(*) AS n"), False),
]


# ---------------------------------------------------------------- comparing rows

def canon(v):
    """A value as the node's JSON writes it: a date or time as its text, a decimal as a number."""
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


def json_rows(rows):
    """Rows as the node's JSON gives them: a null (or a NaN) is left out."""
    return [{k: canon(v) for k, v in r.items() if v is not None and not (isinstance(v, float) and math.isnan(v))} for r in rows]


def same(x, y):
    if isinstance(x, float) or isinstance(y, float):
        return math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-12)
    return x == y


def compare_rows(ours, theirs, ordered):
    """None if the rows are equal (in order if `ordered`, else sorted), else why not."""
    a, b = json_rows(ours), theirs
    if not ordered:
        key = lambda r: json.dumps(r, sort_keys=True)
        a, b = sorted(a, key=key), sorted(b, key=key)
    if len(a) != len(b):
        return f"{len(a)} rows vs {len(b)}"
    for i, (x, y) in enumerate(zip(a, b)):
        if x.keys() != y.keys() or not all(same(x[k], y[k]) for k in x):
            return f"row {i}: {x} vs {y}"
    return None


# ---------------------------------------------------------------- running

def run_node(args, timeout):
    """tools/js_frames_pipelines.mjs's answer (its JSON on stdout)."""
    done = subprocess.run(["node", JS, *args], capture_output=True, text=True, timeout=timeout)
    if done.returncode != 0:
        raise RuntimeError(f"node {args[0]}: {done.stderr.strip()[-800:]}")
    return json.loads(done.stdout)


def first_difference(a, b):
    i = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
    return f"at character {i}: {a[i - 40:i + 60]!r} vs {b[i - 40:i + 60]!r}"


class Report:
    def __init__(self):
        self.failed = 0
        self.total = 0

    def check(self, ok, text):
        self.total += 1
        self.failed += 0 if ok else 1
        print(("ok " if ok else "FAIL ") + text, flush=True)


def pipelines_check(port, report, tmp):
    lake = os.path.join(tmp, "lake")
    tables = data()
    con = pondra.local(lake, port=port)
    try:
        load(con, tables)
        o, u, q = con.table("orders"), con.table("users"), con.table("quotes")
        ours = []
        for name, build, ordered in PIPELINES:  # (built in this order, as the JavaScript side builds them)
            row = {"name": name, "ordered": ordered}
            try:
                frame = build(con, o, u, q)
                row["sql"] = frame.sql
                row["rows"] = frame.rows()
                row["columns"] = frame.columns
            except Exception as e:  # noqa: BLE001 (reported as a failed check)
                row["error"] = f"{type(e).__name__}: {str(e)[:400]}"
            ours.append(row)
        theirs = run_node(["pipelines", f"http://127.0.0.1:{port}"], timeout=900)["pipelines"]
    finally:
        con.close()
    if len(theirs) != len(ours):
        report.check(False, f"pipelines: {len(ours)} in Python, {len(theirs)} in JavaScript")
        return
    for a, b in zip(ours, theirs):
        name = a["name"]
        if "error" in a or "error" in b:
            report.check(False, f"pipeline {name}: python {a.get('error', 'ok')}; javascript {b.get('error', 'ok')}")
            continue
        if a["name"] != b["name"]:
            report.check(False, f"pipeline {name}: javascript has {b['name']} here")
            continue
        if a["sql"] != b["sql"]:
            report.check(False, f"pipeline {name}: SQL differs, {first_difference(a['sql'], b['sql'])}")
            continue
        if a["columns"] != b["columns"]:
            report.check(False, f"pipeline {name}: columns {a['columns']} vs {b['columns']}")
            continue
        why = compare_rows(a["rows"], b["rows"], a["ordered"])
        report.check(why is None, f"pipeline {name}: SQL, columns and {len(b['rows'])} rows equal" if why is None else f"pipeline {name}: rows differ, {why}")


def misc_check(report, tmp):
    """The JavaScript client's own calls, on a node it starts: each result compared with what it should be."""
    d = os.path.join(tmp, "misc")
    os.makedirs(d, exist_ok=True)
    pq.write_table(pa.table({"id": [1, 2, 3, 4, 5], "v": [1, 3, 5, 2, 4], "name": ["a", "b", "c", "d", "e"]}), os.path.join(d, "readable.parquet"))
    got = run_node(["misc", d], timeout=600)
    report.check(got.get("select1") == [{"x": 1}], f"select 1: await db.sql('SELECT 1 AS x') is {got.get('select1')}")
    report.check(got.get("create_t2") == 0, f"create table: CREATE TABLE t2 (a INT), empty: {got.get('create_t2')}")
    everything = [{"id": i, "v": v, "name": n} for i, v, n in [(1, 1, "a"), (2, 3, "b"), (3, 5, "c"), (4, 2, "d"), (5, 4, "e")]]
    rows_back = [
        ("read_parquet", everything, "readParquet of the Parquet file: all 5 rows"),
        ("write_rows", [{"id": 1, "name": "ann", "score": 2.5, "ok": True, "at": "2026-09-01T12:00:00"}, {"id": 2, "score": 3.0, "ok": False}],
         "writeTable with rows (a fraction, text, a boolean, a date, a null)"),
        ("write_overwrite", [{"id": 7, "name": "bo", "score": 0.25, "ok": False, "at": "2026-09-02T00:00:00"}], "writeTable overwrite: the old rows gone"),
        ("write_append", [{"id": 7, "name": "bo", "score": 0.25, "ok": False, "at": "2026-09-02T00:00:00"}, {"id": 9, "name": "cy", "score": 1.0, "ok": True}],
         "writeTable append: one more row"),
        ("write_frame", [r for r in everything if r["v"] > 2], "writeTable of a frame (readParquet, filtered): its rows"),
    ]
    for key, want, text in rows_back:
        got_rows = got.get(key)
        if isinstance(got_rows, dict):
            report.check(False, f"{text}: {got_rows['error']}")
            continue
        why = compare_rows(want, got_rows, ordered=False)
        report.check(why is None, f"{text}" if why is None else f"{text}: {why}")
    # (the JavaScript spelling of each refusal: a JavaScript caller's own option names)
    unsorted = "a frame has no row order until it is sorted"
    refused = got.get("refusals") or {}
    for key, text in [
        ("keepFirst", f"Error: unique({{ keep: 'first' }}): {unsorted} (sort it first, or keep: 'any')"),
        ("keepLast", f"Error: unique({{ keep: 'last' }}): {unsorted} (sort it first, or keep: 'any')"),
        ("keepUnknown", "Error: unique({ keep: 'x' }): any, first, last or none"),
        ("maintainOrder", f"Error: unique({{ maintainOrder: true }}): {unsorted} (sort it first)"),
        ("groupMaintainOrder", "Error: groupBy({ maintainOrder: true }): groups come in no particular order here; sort the result by its keys (.sort(...))"),
    ]:
        report.check(refused.get(key) == text, f"refused by name, saying why: {text[len('Error: '):]}" if refused.get(key) == text
                     else f"{key}: {refused.get(key)!r}, not {text!r}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8831)
    ap.add_argument("--bin", default=DEFAULT_BIN)
    A = ap.parse_args()
    if not shutil.which("node"):
        print("FAIL node: not on PATH (node 18 or later runs js/frame.js)", flush=True)
        sys.exit(1)
    if not os.path.isfile(A.bin):
        print(f"FAIL the binary: {A.bin} isn't there (cargo build --release, or --bin)", flush=True)
        sys.exit(1)
    A.bin = os.path.abspath(A.bin)
    os.environ["PONDRA_BIN"] = A.bin  # (for pondra.local() here, and for the JavaScript side's local())
    tmp = tempfile.mkdtemp(prefix="pondra-js-frames-")
    report = Report()
    try:
        pipelines_check(A.port, report, tmp)
        misc_check(report, tmp)
    except Exception as e:  # noqa: BLE001 (reported, not a traceback)
        report.check(False, f"the check stopped: {type(e).__name__}: {str(e)[:600]}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"{report.total - report.failed} of {report.total} checks ok", flush=True)
    sys.exit(1 if report.failed else 0)


if __name__ == "__main__":
    main()
