#!/usr/bin/env bash
# Spark 4.0 on the same machines as the Pondra nodes (round 34: the same TPC-H, read from the same
# bucket, on the same VMs). Run it while Pondra's nodes are stopped: the two would share the CPUs.
#
#   spark.sh up hosts.txt [K]                   install (once), a master on the first host, a worker on each of the first K
#   spark.sh submit hosts.txt file.py [args…]   run a PySpark file against it; its driver runs on the first host
#   spark.sh down hosts.txt
#
# hosts.txt: `user@host private_ip` per line, as for cluster.sh. Debian or Ubuntu machines with
# the storage scope (gcp.sh's): the GCS connector reads the bucket as the VM, with no key.
set -euo pipefail

V=${SPARK_VERSION:-4.0.1}  # (what tools/spark_check.py compares with)
tgz=spark-$V-bin-hadoop3.tgz
gcs=https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
cmd=${1:?usage: spark.sh up|submit|down hosts.txt …} hosts=${2:?hosts.txt}
shift 2
read -r first master < "$hosts"

each_host() {  # each_host K FUNCTION: FUNCTION's script (given the host's private address) on the first K hosts at once
  local i=0 pids= fail=0 login ip
  while read -r login ip; do
    [ -z "$login" ] && continue
    i=$((i + 1)); [ "$i" -le "$1" ] || break
    ssh -n "$login" "$("$2" "$ip")" > >(sed "s/^/[$i] /") 2>&1 &
    pids="$pids $!"
  done < "$hosts"
  for p in $pids; do wait "$p" || fail=1; done
  return $fail
}

install_script() {  # Java 17, Spark and the GCS connector, once
  cat <<EOF
set -euo pipefail
if [ ! -f /opt/spark/jars/gcs-connector-hadoop3-latest.jar ]; then
  sudo apt-get update -qq && sudo apt-get install -y -qq openjdk-17-jre-headless > /dev/null
  # (dlcdn keeps only the newest releases; archive.apache.org keeps all)
  curl -fsSL https://dlcdn.apache.org/spark/spark-$V/$tgz -o /tmp/spark.tgz ||
    curl -fsSL https://archive.apache.org/dist/spark/spark-$V/$tgz -o /tmp/spark.tgz
  sudo tar -xzf /tmp/spark.tgz -C /opt && sudo ln -sfn /opt/spark-$V-bin-hadoop3 /opt/spark
  sudo chown -R \$(id -un) /opt/spark-$V-bin-hadoop3  # (its conf and logs are ours to write)
  sudo curl -fsSL $gcs -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar
fi
EOF
}

# One executor a machine, with its cores and 60% of its memory (the driver, on the first host, and
# the system have the rest). Every port Spark would pick at random is pinned into 7100-7399, which
# gcp.sh's firewall opens, and Spark's web pages move off 8080, which is Pondra's.
conf_script() {  # $1 = this host's private address
  cat <<EOF
set -euo pipefail
cores=\$(nproc)
mem=\$(awk '/MemTotal/ { print int(\$2 * 0.6 / 1048576) }' /proc/meminfo)
mkdir -p /mnt/ssd/spark
cat > /opt/spark/conf/spark-env.sh <<ENV
export SPARK_MASTER_HOST=$master
export SPARK_LOCAL_IP=$1
export SPARK_MASTER_WEBUI_PORT=8090
export SPARK_WORKER_WEBUI_PORT=8091
export SPARK_WORKER_PORT=7100
export SPARK_WORKER_CORES=\$cores
export SPARK_WORKER_MEMORY=\${mem}g
export SPARK_LOCAL_DIRS=/mnt/ssd/spark
export SPARK_WORKER_DIR=/mnt/ssd/spark-work
export SPARK_LOG_DIR=/tmp/spark-logs
ENV
cat > /opt/spark/conf/spark-defaults.conf <<CONF
spark.master spark://$master:7077
spark.driver.host $master
spark.driver.port 7200
spark.driver.blockManager.port 7250
spark.blockManager.port 7300
spark.port.maxRetries 64
spark.driver.memory 6g
spark.executor.cores \$cores
spark.executor.memory \${mem}g
spark.ui.enabled false
spark.hadoop.fs.gs.impl com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
spark.hadoop.fs.AbstractFileSystem.gs.impl com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS
spark.hadoop.google.cloud.auth.service.account.enable true
CONF
EOF
}

worker_script() { echo "/opt/spark/sbin/start-worker.sh spark://$master:7077"; }
stop_script() { echo "[ -x /opt/spark/sbin/stop-worker.sh ] && /opt/spark/sbin/stop-worker.sh || true"; }

down() {
  each_host 1000000 stop_script
  ssh -n "$first" "[ -x /opt/spark/sbin/stop-master.sh ] && /opt/spark/sbin/stop-master.sh || true"
}

case $cmd in
  up)
    k=${1:-$(grep -c . "$hosts")}
    down  # (so that exactly K workers are listed, whatever ran before)
    each_host "$k" install_script
    each_host "$k" conf_script
    ssh -n "$first" /opt/spark/sbin/start-master.sh
    each_host "$k" worker_script
    # (a worker is listed a moment after it starts)
    ssh -n "$first" "for _ in \$(seq 1 60); do
      if python3 -c \"import json, urllib.request as u; raise SystemExit(json.load(u.urlopen('http://$master:8090/json/'))['aliveworkers'] < $k)\" 2>/dev/null; then exit 0; fi
      sleep 2
    done; echo 'not all $k workers joined' >&2; exit 1"
    echo "spark: master $master:7077, $k workers" ;;
  submit)
    file=${1:?a PySpark file}; shift
    scp -q "$file" "$first:/tmp/$(basename "$file")"
    args=; if [ $# -gt 0 ]; then args=$(printf ' %q' "$@"); fi
    # (the driver runs on the first host, as a client of the cluster)
    ssh -n "$first" "/opt/spark/bin/spark-submit --master spark://$master:7077 /tmp/$(basename "$file")$args" ;;
  down) down ;;
  *) echo "usage: spark.sh up|submit|down hosts.txt …" >&2; exit 2 ;;
esac
