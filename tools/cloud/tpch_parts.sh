#!/usr/bin/env bash
# TPC-H made in parts, a part on each machine, so SF100 is made and held by none of them alone
# (round 34), and loaded into the lake by every machine at once.
#
#   tpch_parts.sh hosts.txt SF [gen|load|all] [serve flags…]
#
# gen:  machine i of N makes its 1/N of every table (`tpchgen-cli --parts N --part i`) into
#       /mnt/ssd/tpch/<table>/, and copies them to $BUCKET/tpch-sf<SF>/<table>/, which is what
#       Spark reads. nation and region can't be split: machine 1 makes them whole, so they are
#       loaded once and not N times.
# load: starts the nodes (so each INSERT reaches the leader over HTTP, as in the cluster-bench
#       workflow), then every machine loads its own files, one INSERT a table:
#         pondra sql --lake $LAKE "INSERT INTO t SELECT * FROM read_parquet('/mnt/ssd/tpch/t/*.parquet')"
#       Machines inserting into one table at once is supported: each writes its own files and the
#       leader records them. At the end the row counts are checked against TPC-H's.
#
# hosts.txt as gcp.sh writes it (`user@host private_ip`); env.sh (LAKE, BUCKET) is found beside it,
# or named by $ENV. The binary is the one cluster.sh copies ($PONDRA_BIN, else target/dist/pondra).
# The serve flags for the nodes: the arguments after the phase, else $PONDRA_SERVE_FLAGS.
set -euo pipefail

hosts=${1:?usage: tpch_parts.sh hosts.txt SF [gen|load|all] [serve flags…]} sf=${2:?the scale factor} phase=${3:-all}
shift $(( $# < 3 ? $# : 3 ))
flags=${*:-${PONDRA_SERVE_FLAGS:---memory-gb 22 --cache-dir /mnt/ssd/pondra-cache}}
env=${ENV:-$(dirname "$hosts")/env.sh}
here=$(cd "$(dirname "$0")" && pwd)
n=$(grep -c . "$hosts")
read -r first ip < "$hosts"
. "$env"  # (LAKE, BUCKET: put into the scripts below, so the machines need no copy of it)
: "${LAKE:?$env sets no LAKE}" "${BUCKET:?$env sets no BUCKET (gs://…: gcp.sh up writes both)}"
data=$BUCKET/tpch-sf$sf

# What machine $1 (counting from 1) runs. Each is sent as one command: ssh -n, nothing on stdin.
gen_script() {
  cat <<EOF
set -euo pipefail
d=/mnt/ssd/tpch
if [ ! -x \$HOME/tpch-venv/bin/tpchgen-cli ]; then
  sudo apt-get update -qq && sudo apt-get install -y -qq python3-venv > /dev/null
  python3 -m venv \$HOME/tpch-venv && \$HOME/tpch-venv/bin/pip install -q tpchgen-cli
fi
export PATH=\$HOME/tpch-venv/bin:\$PATH
# (tpchgen-cli 3 names its format as a subcommand, \`tpchgen-cli parquet …\`; the versions before it by --format)
if tpchgen-cli parquet --help > /dev/null 2>&1; then sub=parquet; fmt=; else sub=; fmt=--format=parquet; fi
rm -rf \$d; mkdir -p \$d
tpchgen-cli \$sub -s $sf --tables supplier,customer,part,partsupp,orders,lineitem --parts $n --part $1 --output-dir \$d \$fmt
if [ $1 = 1 ]; then tpchgen-cli \$sub -s $sf --tables nation,region --output-dir \$d \$fmt; fi
# (a table made whole is a file, <table>.parquet: into a folder of its own, as the parts are)
for f in \$d/*.parquet; do [ -e "\$f" ] || continue; t=\$(basename "\$f" .parquet); mkdir -p \$d/\$t; mv "\$f" \$d/\$t/; done
if command -v gcloud > /dev/null; then gcloud storage cp --recursive \$d/* $data/; else gsutil -m cp -r \$d/* $data/; fi
du -sh \$d
EOF
}

load_script() {
  cat <<EOF
set -euo pipefail
for t in region nation supplier customer part partsupp orders lineitem; do
  [ -d /mnt/ssd/tpch/\$t ] || continue
  /tmp/pondra sql --lake '$LAKE' "INSERT INTO \$t SELECT * FROM read_parquet('/mnt/ssd/tpch/\$t/*.parquet')"
done
EOF
}

wait_script() {  # on machine 1: until all N nodes are in the cluster
  cat <<EOF
for _ in \$(seq 1 120); do
  if python3 -c "import json, urllib.request as u; s = json.load(u.urlopen('http://$ip:8080/stats', timeout=3)); raise SystemExit(len(s['nodes']) < $n)" 2>/dev/null; then exit 0; fi
  sleep 5
done
echo "not all $n nodes joined" >&2; exit 1
EOF
}

count_script() {
  cat <<EOF
for t in region nation supplier customer part partsupp orders lineitem; do
  echo "\$t \$(curl -s -X POST --data "SELECT count(*) AS n FROM \$t" http://$ip:8080/sql | sed 's/[^0-9]*\([0-9]*\).*/\1/')"
done
EOF
}

each() {  # each SCRIPT-FUNCTION: runs it on every machine at once (given its number), and waits for all
  local i=0 pids= fail=0 login _ip
  while read -r login _ip; do
    [ -z "$login" ] && continue
    i=$((i + 1))
    ssh -n "$login" "$("$1" "$i")" > >(sed "s/^/[$i] /") 2>&1 &
    pids="$pids $!"
  done < "$hosts"
  for p in $pids; do wait "$p" || fail=1; done
  return $fail
}

check() {  # the row counts are TPC-H's (nation and region don't grow with SF; lineitem's count is close to 6 M a SF)
  local t got want bad=0
  while read -r t got; do
    case $t in
      region) want=5 ;; nation) want=25 ;;
      supplier) want=10000 ;; customer) want=150000 ;; part) want=200000 ;; partsupp) want=800000 ;; orders) want=1500000 ;;
      *) want= ;;
    esac
    if [ -n "$want" ] && [ "$t" != region ] && [ "$t" != nation ]; then want=$(awk -v w=$want -v sf="$sf" 'BEGIN { printf "%d", w * sf + 0.5 }'); fi
    if [ -z "$want" ]; then
      ok=$(awk -v g="${got:-0}" -v sf="$sf" 'BEGIN { d = g - 6e6 * sf; print (d < 0 ? -d : d) <= 0.02 * 6e6 * sf }')
      want="about $(awk -v sf="$sf" 'BEGIN { printf "%d", 6e6 * sf }')"
    else
      ok=$([ "${got:-0}" = "$want" ] && echo 1 || echo 0)
    fi
    if [ "$ok" = 1 ]; then echo "  $t: $got rows"; else echo "  $t: $got rows, expected $want" >&2; bad=1; fi
  done
  return $bad
}

case $phase in gen | load | all) ;; *) echo "phase: gen, load or all" >&2; exit 2 ;; esac
if [ "$phase" != load ]; then each gen_script; fi
if [ "$phase" != gen ]; then
  # shellcheck disable=SC2086  # (flags are several words)
  bash "$here/cluster.sh" start "$hosts" "$env" $flags
  ssh -n "$first" "$(wait_script)"
  each load_script
  echo "rows in the lake:"
  ssh -n "$first" "$(count_script)" | check
fi
