#!/usr/bin/env bash
# Start (or stop) a Pondra cluster on machines you can ssh into, all on one bucket.
#
#   cluster.sh start hosts.txt env.sh [extra serve flags…]
#   cluster.sh stop  hosts.txt
#
# hosts.txt: one "user@host private_ip" per line. env.sh: the bucket's credentials, sourced on
# each machine (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT, AWS_REGION, and LAKE,
# e.g. s3://my-bucket/bench). The binary is target/dist/pondra (or $PONDRA_BIN), copied over.
# Each node serves HTTP on 8080, Flight on 8815 and Kafka on 9092.
set -euo pipefail
cmd=$1 hosts=$2 env=${3:-}
shift $(( $# < 3 ? $# : 3 ))
bin=${PONDRA_BIN:-$(dirname "$0")/../../target/dist/pondra}
while read -r login ip; do
  [ -z "$login" ] && continue
  case $cmd in
    start)
      scp -q "$bin" "$env" "$login":/tmp/
      ssh -n "$login" "chmod +x /tmp/pondra; . /tmp/$(basename "$env"); nohup /tmp/pondra serve --dir \$LAKE --addr $ip:8080 \
        --flight 0.0.0.0:8815 --kafka 0.0.0.0:9092 --kafka-advertise $ip:9092 $* > /tmp/pondra.log 2>&1 &"
      echo "started $login ($ip)" ;;
    stop)
      ssh -n "$login" "pkill -x pondra || true"; echo "stopped $login" ;;
  esac
done < "$hosts"
