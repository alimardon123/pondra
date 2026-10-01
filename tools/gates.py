#!/usr/bin/env python3
"""The gates, as one command (roadmap, "The road to 1.0"): right answers and speed, measured the
same way every round, so a drop is seen before a release rather than after.

  gates.py [--only slt,tpch,postgres,nexmark] [--prepare] [--note "…"] [--bin target/release/pondra]

- **slt**: DataFusion's own SQL tests (sqllogictest) through Pondra on one node (`slt_check.py`).
- **tpch**: TPC-H SF1, Pondra from memory and from its lake's files, against DuckDB's own tables
  and DuckDB on the files; every answer checked against DuckDB's (`bench/singlenode.py`).
- **postgres**: key lookups, single-row inserts and TPC-H Q1 and Q6 against Postgres
  (`bench/vs_postgres.py`), on a Postgres this starts for the run.
- **nexmark**: five Nexmark queries over 2M bids, answers checked against DuckDB's
  (`bench/nexmark.py`).

Each run appends a row to `logs/gates/README.md` and its numbers to `logs/gates/history.jsonl`,
with the details beside them, and compares itself with the run before: fewer sqllogictest records
passed, an answer that stopped matching, or a total more than 15% slower is a **drop**, and the
command exits 1 (a drop explained, as an order SQL leaves open, is written in `--note`).

`--prepare` fetches what's missing: DataFusion's test files at the version Pondra builds on
(`~/datafusion`, git, sparse) and TPC-H SF1 (`~/tpch/sf1`, tpchgen-cli). A gate whose inputs
aren't there is skipped, and the row says so.
"""
import argparse, datetime, json, os, re, shutil, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LOGS = os.path.join(ROOT, "logs", "gates")
DF = os.path.expanduser("~/datafusion")
TPCH = os.path.expanduser("~/tpch/sf1")
DF_VERSION = "55.1.0"


def sh(*args, **kw):
    print("$", " ".join(args), flush=True)
    return subprocess.run(args, check=True, **kw)


# ---------------------------------------------------------------- inputs

def prepare():
    if not os.path.isdir(os.path.join(DF, "datafusion", "sqllogictest", "test_files")):
        sh("git", "clone", "--depth", "1", "--filter=blob:none", "--sparse", "--branch", DF_VERSION, "https://github.com/apache/datafusion", DF)
        sh("git", "-C", DF, "sparse-checkout", "set", "datafusion/sqllogictest/test_files", "datafusion/core/tests/data", "datafusion/core/tests/tpch-csv")
        sh("git", "-C", DF, "submodule", "update", "--init", "--depth", "1", "testing", "parquet-testing")
    if not os.path.exists(os.path.join(TPCH, "lineitem.parquet")):
        os.makedirs(TPCH, exist_ok=True)
        sh("tpchgen-cli", "-s", "1", "--format", "parquet", "--output-dir", TPCH)
    if not os.path.exists(os.path.join(TPCH + "-bench", "lineitem.parquet")):
        sh(sys.executable, os.path.join(HERE, "bench", "singlenode.py"), "prepare", "--data", TPCH)


# ---------------------------------------------------------------- the gates

def slt(a, day):
    files = os.path.join(DF, "datafusion", "sqllogictest", "test_files")
    if not os.path.isdir(files):
        return {"skipped": f"no {files} (--prepare)"}
    out = os.path.join(LOGS, f"{day}-slt.json")
    with open(os.path.join(LOGS, f"{day}-slt.txt"), "w") as log:
        sh(sys.executable, os.path.join(HERE, "slt_check.py"), "--slt", files, "--nodes", "1", "--out", out, stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "PONDRA_BIN": a.bin})
    r = json.load(open(out))
    return {"passed": r["passed"], "records": r["records"], "rate": r["pass_rate"]}


def tpch(a, day):
    data = TPCH + "-bench"
    if not os.path.exists(os.path.join(data, "lineitem.parquet")):
        return {"skipped": f"no {data} (--prepare)"}
    out = os.path.join(LOGS, f"{day}-tpch-sf1.json")
    with open(os.path.join(LOGS, f"{day}-tpch-sf1.txt"), "w") as log:
        sh(sys.executable, os.path.join(HERE, "bench", "singlenode.py"), "run", "--data", data, "--sf", "1", "--engines", "pondra,pondra-cold,duckdb,duckdb-native",
           "--queries", os.path.join(HERE, "bench", "tpch-queries"), "--repos", os.path.join(tempfile.gettempdir(), "pondra-gates-repos"), "--out", out,
           stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "PONDRA_BIN": a.bin})
    t = json.load(open(out))["results"]
    total = lambda e: t.get(e, {}).get("total_hot")
    return {"memory_s": total("pondra"), "files_s": total("pondra-cold"), "duckdb_own_s": total("duckdb-native"), "duckdb_files_s": total("duckdb"),
            "answers": min(t.get(e, {}).get("answers_match", 0) for e in ("pondra", "pondra-cold"))}


def postgres(a, day):
    bins = sorted(d for d in [f"/usr/lib/postgresql/{v}/bin" for v in range(20, 12, -1)] if os.path.exists(os.path.join(d, "initdb")))
    lineitem = os.path.join(TPCH, "lineitem.parquet")
    if not bins:
        return {"skipped": "no Postgres here (initdb)"}
    if not os.path.exists(lineitem):
        return {"skipped": f"no {lineitem} (--prepare)"}
    pg, data = bins[-1], tempfile.mkdtemp(prefix="pondra-gates-pg-")
    user = [] if os.geteuid() else ["runuser", "-u", "postgres", "--"]  # (Postgres won't run as root)
    if user:
        shutil.chown(data, "postgres")
    sh(*user, os.path.join(pg, "initdb"), "-D", data, "-U", "postgres", "--auth=trust", stdout=subprocess.DEVNULL)
    sh(*user, os.path.join(pg, "pg_ctl"), "-D", data, "-o", "-p 5499 -k /tmp", "-l", os.path.join(data, "log"), "-w", "start", stdout=subprocess.DEVNULL)
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "bench", "vs_postgres.py"), "--bin", a.bin, "--lineitem", lineitem], capture_output=True, text=True, timeout=3600)
        open(os.path.join(LOGS, f"{day}-postgres.txt"), "w").write(r.stdout + r.stderr)
        got = json.loads(r.stdout[r.stdout.index("{"):])
    finally:
        subprocess.run([*user, os.path.join(pg, "pg_ctl"), "-D", data, "-m", "fast", "stop"], capture_output=True)
        shutil.rmtree(data, ignore_errors=True)
    return {"details": got}


def nexmark(a, day):
    r = subprocess.run([sys.executable, os.path.join(HERE, "bench", "nexmark.py"), "--engines", "pondra"], capture_output=True, text=True, timeout=3600,
                       env={**os.environ, "PONDRA_BIN": a.bin})
    open(os.path.join(LOGS, f"{day}-nexmark.txt"), "w").write(r.stdout + r.stderr)
    got = json.loads(r.stdout.strip().splitlines()[-1])
    return {"secs": got["secs"], "bids_per_s": got["bids_per_s"], "answers": got["same_as_duckdb"]}


GATES = {"slt": slt, "tpch": tpch, "postgres": postgres, "nexmark": nexmark}


# ---------------------------------------------------------------- the record

def drops(now, before):
    """What got worse since the run before."""
    out = []
    g = lambda r, k, f: r.get(k, {}).get(f)
    if g(now, "slt", "passed") is not None and g(before, "slt", "passed") is not None and g(now, "slt", "passed") < g(before, "slt", "passed"):
        out.append(f"sqllogictest: {g(now, 'slt', 'passed')} passed, {g(before, 'slt', 'passed')} before")
    for f in ("memory_s", "files_s"):
        n, b = g(now, "tpch", f), g(before, "tpch", f)
        if n and b and n > b * 1.15:
            out.append(f"TPC-H {f}: {n} s, {b} s before")
    if g(now, "tpch", "answers") is not None and g(now, "tpch", "answers") < 22:
        out.append(f"TPC-H: {g(now, 'tpch', 'answers')} of 22 answers as DuckDB's")
    if g(now, "nexmark", "answers") is False:
        out.append("Nexmark: answers differ from DuckDB's")
    n, b = g(now, "nexmark", "secs"), g(before, "nexmark", "secs")
    if n and b and n > b * 1.15:
        out.append(f"Nexmark: {n} s, {b} s before")
    return out


def row(day, binary, r, note):
    s, t, n = r.get("slt", {}), r.get("tpch", {}), r.get("nexmark", {})
    cell = lambda g, text: g.get("skipped") and f"skipped: {g['skipped']}" or (text if g else "not run")
    slt_cell = cell(s, f"{s.get('passed', 0):,} of {s.get('records', 0):,} ({100 * s.get('rate', 0):.1f}%)")
    mem = cell(t, f"{t.get('memory_s')} s")
    files = cell(t, f"{t.get('files_s')} s")
    duck = cell(t, f"{t.get('duckdb_own_s')} s / {t.get('duckdb_files_s')} s")
    extra = [f"Nexmark 2M bids {n['secs']} s, answers {'right' if n['answers'] else 'WRONG'}"] if n.get("secs") else []
    if r.get("postgres", {}).get("details"):
        extra.append(f"vs Postgres: `{day}-postgres.txt`")
    return f"| {day} | {binary} | {slt_cell} | {mem} | {files} | {duck} | {'; '.join(extra + ([note] if note else [])) or '`tools/gates.py`'} |"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--only", default="slt,tpch,postgres,nexmark")
    ap.add_argument("--prepare", action="store_true")
    ap.add_argument("--note", default="")
    ap.add_argument("--bin", default=os.path.join(ROOT, "target", "release", "pondra"))
    a = ap.parse_args()
    a.bin = os.path.abspath(a.bin)
    os.makedirs(LOGS, exist_ok=True)
    if a.prepare:
        prepare()
    day = datetime.date.today().isoformat()
    version = subprocess.run([a.bin, "--version"], capture_output=True, text=True).stdout.split()[-1:] or ["?"]
    commit = subprocess.run(["git", "-C", ROOT, "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    binary = f"{version[0]} ({commit})"
    results, t0 = {"day": day, "binary": binary}, time.time()
    for name in a.only.split(","):
        print(f"== {name}", flush=True)
        try:
            results[name] = GATES[name](a, day)
        except Exception as e:
            results[name] = {"failed": f"{type(e).__name__}: {e}"[:500]}
        print(json.dumps(results[name]), flush=True)
    history = os.path.join(LOGS, "history.jsonl")
    before = [json.loads(l) for l in open(history)] if os.path.exists(history) else []
    last = {}
    for b in before:  # (each gate's latest measurement, whichever run made it)
        last.update({k: v for k, v in b.items() if isinstance(v, dict) and not v.get("skipped") and not v.get("failed")})
    dropped = drops(results, last)
    results["drops"], results["note"], results["secs"] = dropped, a.note, round(time.time() - t0)
    with open(history, "a") as f:
        f.write(json.dumps(results) + "\n")
    with open(os.path.join(LOGS, "README.md"), "a") as f:
        f.write(row(day, binary, results, a.note + ("; **dropped:** " + "; ".join(dropped) if dropped else "")) + "\n")
    print(json.dumps({"gates": results, "ok": not dropped}, indent=1))
    sys.exit(1 if dropped else 0)


if __name__ == "__main__":
    main()
