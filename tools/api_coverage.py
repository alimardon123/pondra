#!/usr/bin/env python3
"""How much of Polars' and PySpark's DataFrame APIs Pondra's Python has, by name (roadmap round 31:
"Polars and PySpark coverage published"): each public method, property and function of theirs,
and whether `pondra.frame` (Polars' words) or `pondra.spark` (PySpark's) has one of that name.
What a method does is checked elsewhere (`tools/frames_check.py`); this says what is there.

PySpark's functions are counted twice: those `pondra.spark.functions` defines (with PySpark's
meaning where SQL's differs), and those a node's SQL has by the same name, which
`pondra.spark.functions` reaches through its fallback: Spark's own meaning where DataFusion has no
function of that name, DataFusion's where it has one.

  api_coverage.py [--bin target/release/pondra] [--out logs/round31/api-coverage.json] [--markdown]

Run with a Python that has polars and pyspark installed.
"""
import argparse, inspect, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "python"))


def public(obj):
    return sorted(n for n in dir(obj) if not n.startswith("_"))


def compare(theirs, ours, skip=()):
    names = [n for n in public(theirs) if n not in skip]
    have = [n for n in names if n in getattr(ours, "__dict__", {}) or n in dir(ours)]
    return {"theirs": len(names), "ours": len(have), "share": round(len(have) / max(len(names), 1), 3), "missing": [n for n in names if n not in have]}


def polars():
    import polars as pl
    from pondra import frame as f
    deprecated = {n for n in public(pl.DataFrame) if "deprecated" in (inspect.getdoc(getattr(pl.DataFrame, n, None)) or "").lower()[:300]}
    return {
        "DataFrame / LazyFrame → Frame": compare(type("Both", (), {n: 1 for n in set(public(pl.DataFrame)) | set(public(pl.LazyFrame))}), f.Frame, skip=deprecated),
        "Expr → Expr": compare(pl.Expr, f.Expr),
        "Expr.str → Expr.str": compare(pl.expr.string.ExprStringNameSpace, f._Str),
        "Expr.dt → Expr.dt": compare(pl.expr.datetime.ExprDateTimeNameSpace, f._Dt),
        "functions (pl.*) → pondra.*": compare(type("Fns", (), {n: 1 for n in public(pl.functions)}), f),
        "group_by → GroupBy": compare(pl.dataframe.group_by.GroupBy, f.GroupBy),
    }


def spark():
    import pyspark.sql as ps, pyspark.sql.functions as F
    from pondra import spark as s
    from pondra.spark import functions as sf
    from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
    from pyspark.sql.window import Window, WindowSpec
    fns = type("Fns", (), {n: 1 for n in public(F) if callable(getattr(F, n)) and not inspect.isclass(getattr(F, n)) and n not in ("PandasUDFType",)})
    sql = sql_functions()
    by_sql = [n for n in public(fns) if n not in vars(sf) and n.lower() in sql]
    return {
        "functions, in SQL by name": {"theirs": len(public(fns)), "ours": len(by_sql) + len([n for n in public(fns) if n in vars(sf)]), "share": round((len(by_sql) + len([n for n in public(fns) if n in vars(sf)])) / len(public(fns)), 3), "by_name": by_sql},
        "DataFrame": compare(ps.DataFrame, s.DataFrame),
        "Column": compare(ps.Column, s.Column),
        "functions": compare(fns, sf),
        "GroupedData": compare(ps.GroupedData, s.GroupedData),
        "SparkSession": compare(ps.SparkSession, s.SparkSession),
        "DataFrameReader": compare(DataFrameReader, s._Reader),
        "DataFrameWriter": compare(DataFrameWriter, s._Writer),
        "Window": compare(Window, s.Window),
        "WindowSpec": compare(WindowSpec, s.WindowSpec),
        "DataFrameNaFunctions": compare(ps.DataFrameNaFunctions, s._Na),
    }


def sql_functions():
    """The function names a node's SQL has (`SHOW FUNCTIONS`)."""
    import subprocess, tempfile, time, urllib.request
    lake = tempfile.mkdtemp(prefix="pondra-coverage-")
    p = subprocess.Popen([A.bin, "serve", "--dir", lake, "--addr", "127.0.0.1:9677"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            try:
                rows = json.loads(urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:9677/sql", data=b"SHOW FUNCTIONS"), timeout=30).read())
                return {r["function_name"].lower() for r in rows}
            except Exception:
                time.sleep(0.1)
        return set()
    finally:
        p.terminate()


def main():
    import polars as pl, pyspark as ps
    out = {"polars": {"version": pl.__version__, **polars()}, "pyspark": {"version": ps.__version__, **spark()}}
    for lib, parts in out.items():
        print(f"{lib} {parts['version']}")
        for k, v in parts.items():
            if k != "version":
                print(f"  {k:32} {v['ours']:4} of {v['theirs']:4} ({100 * v['share']:.0f}%)")
    if A.markdown:
        for lib, parts in out.items():
            print(f"\n| {lib} {parts['version']} | Pondra has | of |\n|---|---|---|")
            for k, v in parts.items():
                if k != "version":
                    print(f"| {k} | {v['ours']} ({100 * v['share']:.0f}%) | {v['theirs']} |")
    if A.out:
        json.dump(out, open(A.out, "w"), indent=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out")
    ap.add_argument("--markdown", action="store_true")
    ap.add_argument("--bin", default=os.path.join(HERE, "..", "target", "release", "pondra"))
    A = ap.parse_args()
    main()
