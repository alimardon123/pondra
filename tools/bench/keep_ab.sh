#!/usr/bin/env bash
# The A/B for "a loaded Parquet file kept as written" (design l2-load-keeps-parquet.md, check 5). One binary, two
# builds: A = PONDRA_KEEP_PARQUET=0 (the hook off: every table written as today), B = the variable unset (hook on).
#
#   tools/bench/keep_ab.sh --bin PONDRA --tpch DIR --tpcds DIR --clickbench DIR [--suites tpch,tpcds,clickbench]
#                          [--out DIR] [--rounds N] [--python PY]
#
#   --tpch        TPC-H SF1 as tpchgen wrote it: <table>.parquet each (Snappy, so the hook keeps them)
#   --tpcds       tpcds_check.py prepare's folder: <table>.parquet each, and queries/
#   --clickbench  singlenode.py prepare's -bench folder: hits.parquet
#   --suites      which suites to run (default all three); the table has only their rows
#   --out         where the JSON, the logs and the table go (default ./keep-ab-out)
#   --rounds      TPC-H rounds per ab_compare run (default 8)
#   --python      a Python with duckdb and pyarrow (default python3)
#
# TPC-H: ab_compare, A and B alike, each loaded by CTAS: folder lake, memory lake, hot tier off (--hot-gb 0).
# TPC-DS: tpcds_check run, A then B then A. ClickBench: singlenode pondra-cold, A then B then A.
# Before each B of TPC-H and TPC-DS: each table is loaded into a scratch lake by B's binary, and its files are listed
# by codec: SNAPPY is the source's own bytes (kept), anything else is pondra's (written). A table not kept is named.
# ClickBench has no codec check: its lake is singlenode.py's own, and a second one would not fit on the disk.
# A run that fails leaves its JSON out, and the table says so. Exit 0 whatever the answers: read the table.
set -u

usage() { awk 'NR > 1 && /^[^#]/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "$0"; exit 2; }
BIN= TPCH= TPCDS= CB= OUT=keep-ab-out ROUNDS=8 PY=python3 SUITES=tpch,tpcds,clickbench
while [ $# -gt 0 ]; do
  case "$1" in
    --bin) BIN=$2; shift 2 ;;
    --tpch) TPCH=$2; shift 2 ;;
    --tpcds) TPCDS=$2; shift 2 ;;
    --clickbench) CB=$2; shift 2 ;;
    --suites) SUITES=$2; shift 2 ;;
    --out) OUT=$2; shift 2 ;;
    --rounds) ROUNDS=$2; shift 2 ;;
    --python) PY=$2; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done
want() { case ",$SUITES," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }
for s in ${SUITES//,/ }; do
  case $s in tpch|tpcds|clickbench) ;; *) echo "unknown suite: $s" >&2; usage ;; esac
done
[ -n "$BIN" ] || { echo "--bin is required" >&2; usage; }
{ want tpch && [ -z "$TPCH" ]; } && { echo "--tpch is required" >&2; usage; }
{ want tpcds && [ -z "$TPCDS" ]; } && { echo "--tpcds is required" >&2; usage; }
{ want clickbench && [ -z "$CB" ]; } && { echo "--clickbench is required" >&2; usage; }

REPO=$(cd "$(dirname "$0")/../.." && pwd)
BIN=$(readlink -f "$BIN")
[ -n "$TPCH" ] && TPCH=$(readlink -f "$TPCH")
[ -n "$TPCDS" ] && TPCDS=$(readlink -f "$TPCDS")
[ -n "$CB" ] && CB=$(readlink -f "$CB")
mkdir -p "$OUT" && OUT=$(readlink -f "$OUT")
WORK=$(mktemp -d "${TMPDIR:-/tmp}/keep-ab.XXXXXX")   # scratch lakes and singlenode's repos folder; removed at the end
export PONDRA_BIN="$BIN"
unset PONDRA_KEEP_PARQUET   # (B is the unset variable: nothing from this shell reaches it)

# Before a B run. Loads each <table>.parquet of DATA into a scratch lake with B's binary (no A env), lists every
# table's files' column-chunk codec with pyarrow, and names the tables not kept. Removes the scratch lake.
codec_check() {
  local data=$1 lake f t
  lake=$(mktemp -d "$WORK/codec.XXXXXX")
  for f in "$data"/*.parquet; do
    t=$(basename "$f" .parquet)
    env -u PONDRA_KEEP_PARQUET "$BIN" sql --dir "$lake" "CREATE TABLE $t AS SELECT * FROM '$f'" > /dev/null 2>&1 \
      || echo "codec check: load of $t failed"
  done
  "$BIN" catalog --dir "$lake" "t/" > "$WORK/catalog.txt" 2>/dev/null
  "$PY" -I - "$lake" "$WORK/catalog.txt" <<'PY'
import collections, json, os, sys
import pyarrow.parquet as pq
lake = sys.argv[1]
kept, written, mixed = [], [], []
for line in open(sys.argv[2]).read().splitlines():
    name, js = line.split(" ", 1)  # "t/<table> {json}"
    table = name.split("/", 1)[1]
    if table.startswith("pondra$"):  # (the lake's own system tables)
        continue
    codecs = collections.Counter()
    for f in json.loads(js)["files"]:
        path = f["path"] if os.path.isabs(f["path"]) else os.path.join(lake, f["path"])
        md = pq.ParquetFile(path).metadata
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            for c in range(rg.num_columns):
                codecs[rg.column(c).compression] += 1
    label = ", ".join(f"{k} x{v}" for k, v in sorted(codecs.items())) or "no files"
    # Kept = the source's Snappy chunks; anything else is pondra's own encoding (written). The codec names are as
    # pyarrow reads them (main's own files read as LZ4, not LZ4_RAW).
    if codecs and set(codecs) == {"SNAPPY"}:
        kept.append(table); status = "Snappy (kept)"
    elif codecs and "SNAPPY" not in codecs:
        written.append(table); status = "written, not Snappy"
    else:
        mixed.append(table); status = "mixed or no files"
    print(f"  codec {table:24} {status}  [{label}]")
if written or mixed:
    print(f"  NOT KEPT: {', '.join(written + mixed)}")
else:
    print(f"  all {len(kept)} tables kept")
PY
  rm -rf "$lake"
}

# TPC-H: ab_compare, loaded by CTAS; the B's codec check before each of the three.
if want tpch; then
  for mode in folder memory cold; do
    extra=()
    [ "$mode" = memory ] && extra=(--lake memory)
    [ "$mode" = cold ] && extra=(--hot-gb 0)
    echo "== TPC-H $mode"
    codec_check "$TPCH"
    "$PY" -I "$REPO/tools/bench/ab_compare.py" --a "$BIN" --b "$BIN" --data "$TPCH" --load ctas --rounds "$ROUNDS" \
      --a-env PONDRA_KEEP_PARQUET=0 ${extra[@]+"${extra[@]}"} --out "$OUT/tpch-$mode.json" > "$OUT/tpch-$mode.log" 2>&1
    echo "   exit $? (log: $OUT/tpch-$mode.log)"
  done
fi

# TPC-DS: A, B, A. Each run's own environment: A has the hook off, B has nothing set.
tpcds_run() {  # TAG [VAR=VALUE ...]
  local tag=$1; shift
  (cd "$REPO" && env -u PONDRA_KEEP_PARQUET "$@" "$PY" -I tools/tpcds_check.py run --data "$TPCDS" --out "$OUT/tpcds-$tag.json") \
    > "$OUT/tpcds-$tag.log" 2>&1
  echo "   $tag exit $? (log: $OUT/tpcds-$tag.log)"
}
if want tpcds; then
  echo "== TPC-DS A"; tpcds_run a1 PONDRA_KEEP_PARQUET=0
  echo "== TPC-DS B"; codec_check "$TPCDS"; tpcds_run b
  echo "== TPC-DS A"; tpcds_run a2 PONDRA_KEEP_PARQUET=0
fi

# ClickBench from files, pondra-cold: A, B, A. No codec check (see the header).
cb_run() {  # TAG [VAR=VALUE ...]
  local tag=$1; shift
  env -u PONDRA_KEEP_PARQUET "$@" "$PY" -I "$REPO/tools/bench/singlenode.py" run --suite clickbench --data "$CB" \
    --engines pondra-cold,duckdb --runs 3 --repos "$WORK/repos" --out "$OUT/clickbench-$tag.json" > "$OUT/clickbench-$tag.log" 2>&1
  echo "   $tag exit $? (log: $OUT/clickbench-$tag.log)"
}
if want clickbench; then
  echo "== ClickBench A"; cb_run a1 PONDRA_KEEP_PARQUET=0
  echo "== ClickBench B (no codec check)"; cb_run b
  echo "== ClickBench A"; cb_run a2 PONDRA_KEEP_PARQUET=0
fi

# One table. Load seconds per build, the answers that differ, and the queries over 5% slower in B than in both A runs.
# TPC-DS and ClickBench also give A1 against A2: the noise, the queries over 5% apart between two runs of the same build.
"$PY" -I - "$OUT" "$SUITES" "$ROUNDS" <<'PY' | tee "$OUT/table.txt"
import json, os, sys
out, suites, rounds = sys.argv[1], sys.argv[2].split(","), sys.argv[3]
def load(name):
    p = os.path.join(out, name)
    return json.load(open(p)) if os.path.exists(p) else None
def pct(a, b):
    return abs(a - b) / min(a, b) > 0.05
rows = []
if "tpch" in suites:
    for mode in ("folder", "memory", "cold"):
        d = load(f"tpch-{mode}.json")
        if d is None:
            rows.append((f"TPC-H {mode}", "no JSON", "", "", "")); continue
        loads = "; ".join(f"A {r['load_s']['a']} s, B {r['load_s']['b']} s" for r in d["runs"])
        differ = sorted({i for r in d["runs"] for i in r["answers_differ"]}, key=int)
        slow = [i for i in d["runs"][0]["wall"]["per_query"]
                if all(r["wall"]["per_query"][i][1] > 1.05 * r["wall"]["per_query"][i][0] for r in d["runs"])]
        rows.append((f"TPC-H {mode} (B/A {d['b_over_a']['wall']})", loads, ",".join(differ) or "none", ",".join(slow) or "none",
                     f"not measured (no A/A); {rounds} rounds"))
def suite(name, files, answers, times, loads):
    runs = [load(f) for f in files]
    if any(r is None for r in runs):
        rows.append((name, "missing JSON", "", "", "")); return
    a1, b, a2 = runs
    la = [loads(r) for r in (a1, b, a2)]
    ok = [answers(r) for r in (a1, b, a2)]
    differ = sorted({q for o in ok for q, same in o.items() if not same})  # (not as DuckDB's, in any run)
    t = [times(r) for r in (a1, b, a2)]
    both = [q for q in t[1] if q in t[0] and q in t[2]]
    slow = [q for q in both if t[1][q] > 1.05 * t[0][q] and t[1][q] > 1.05 * t[2][q]]
    noise = [q for q in both if pct(t[0][q], t[2][q])]
    rows.append((name, f"A1 {la[0]} s, B {la[1]} s, A2 {la[2]} s", ",".join(differ) or "none", ",".join(slow) or "none",
                 f"A1 vs A2: {len(noise)} of {len(both)} over 5%"))
if "tpcds" in suites:
    suite("TPC-DS", ["tpcds-a1.json", "tpcds-b.json", "tpcds-a2.json"],
          lambda r: {q: v["verdict"] in ("same", "same rows") for q, v in r["each"].items()},
          lambda r: {q: v["pondra_s"] for q, v in r["each"].items() if "pondra_s" in v},
          lambda r: r["load_s"])
if "clickbench" in suites:
    suite("ClickBench", ["clickbench-a1.json", "clickbench-b.json", "clickbench-a2.json"],
          lambda r: {q: v["same_as_duckdb"] for q, v in r["results"]["pondra-cold"].items() if isinstance(v, dict) and "same_as_duckdb" in v},
          lambda r: {q: v["hot"] for q, v in r["results"]["pondra-cold"].items() if isinstance(v, dict) and "hot" in v},
          lambda r: r["info"]["pondra_load_s"])
w = max(len(r[0]) for r in rows)
print(f"{'suite':<{w}}  load seconds | answers differ | slower than A by 5%+ in both A runs | noise")
for name, loads, differ, slow, noise in rows:
    print(f"{name:<{w}}  {loads} | {differ} | {slow} | {noise}")
if "clickbench" in suites:
    print("codec check: before each B of TPC-H and TPC-DS; ClickBench skipped (its lake is singlenode's own)")
PY

rm -rf "$WORK"
echo "table: $OUT/table.txt"
exit 0
