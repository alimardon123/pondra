#!/usr/bin/env bash
# Machines on Google Cloud for the cluster benchmark (round 34): N node VMs and one client VM in a
# zone, a firewall that lets them reach each other and nobody else, and a bucket for the lake.
#
#   gcp.sh up N [--zone us-central1-a] [--machine n2-standard-8] [--local-ssd] [--name pondra-bench]
#               [--project P] [--network default] [--ssh-pub-key FILE] [--ssh-from CIDR]
#               [--max-hours 12] [--dry-run]
#   gcp.sh sync [--name pondra-bench] [--dry-run]     this kit, the binary and the host lists to the client VM
#   gcp.sh down [--name pondra-bench] [--zone Z] [--bucket] [--dry-run]
#
# `up` writes, in this folder: hosts.txt (`user@external_ip private_ip` per node, what cluster.sh
# reads), hosts.internal.txt (the same over private addresses, for the client VM), client.txt
# (the client in the same form) and env.sh (`LAKE=gs://<project>-<name>/lake`). No secret is
# written anywhere: the VMs have the storage scope, and a node opens `gs://` with the VM's own
# account. `--dry-run` prints every command instead of running it.
set -euo pipefail

zone=us-central1-a machine=n2-standard-8 client_machine=e2-standard-4 name=pondra-bench
network=default ssh_from=0.0.0.0/0 pubkey= project= max_hours=12 disk_gb=200
ssd= dry=0 del_bucket=0 n=
cmd=${1:?usage: gcp.sh up N | sync | down [options]}; shift
[ "$cmd" = up ] && { n=${1:?usage: gcp.sh up N [options]}; shift; }
while [ $# -gt 0 ]; do
  case $1 in
    --zone) zone=$2; shift ;;
    --machine) machine=$2; shift ;;
    --client-machine) client_machine=$2; shift ;;
    --name) name=$2; shift ;;
    --project) project=$2; shift ;;
    --network) network=$2; shift ;;
    --ssh-pub-key) pubkey=$2; shift ;;
    --ssh-from) ssh_from=$2; shift ;;
    --max-hours) max_hours=$2; shift ;;
    --local-ssd) ssd=--local-ssd=interface=nvme ;;
    --bucket) del_bucket=1 ;;
    --dry-run) dry=1 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
  shift
done
case $n in *[!0-9]*) echo "up N: N is a number of nodes" >&2; exit 2 ;; esac

q() { case $1 in *[!A-Za-z0-9_./:=,@%+-]*) printf "'%s'" "$1" ;; *) printf '%s' "$1" ;; esac; }
run() {  # (a dry run shows the command, as it would be typed)
  if [ $dry = 1 ]; then printf '+'; for a in "$@"; do printf ' '; q "$a"; done; echo; else "$@"; fi
}
g() { run gcloud "$@" --project "$project"; }
there() { [ $dry = 0 ] && gcloud "$@" --project "$project" > /dev/null 2>&1; }  # (an `up` again, after a `down` that kept the bucket or an `up` that stopped half way)
say() { if [ $dry = 1 ]; then echo "  ($*)"; else echo "$*"; fi; }

# The project: asked of gcloud only on a real run (a dry run touches nothing, not even its config).
if [ -z "$project" ]; then
  if [ $dry = 1 ]; then project='<project>'; else project=$(gcloud config get-value project 2>/dev/null); fi
fi
[ -n "$project" ] || { echo "no project: gcloud config set project …, or --project" >&2; exit 2; }
bucket=$project-$name
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
user=pondra

vm_names() {  # the VMs of this benchmark, found by label, so `down` needs no N
  gcloud compute instances list --project "$project" --filter "labels.pondra-bench=$name" --format 'value(name)'
}

up() {
  [ -n "$pubkey" ] || pubkey=$(ls ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub 2>/dev/null | head -1 || true)
  if [ -z "$pubkey" ]; then
    [ $dry = 1 ] && pubkey='<~/.ssh/id_ed25519.pub>' || { echo "no ssh key: ssh-keygen -t ed25519, or --ssh-pub-key FILE" >&2; exit 2; }
  fi
  tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
  # /mnt/ssd is the local SSD when the machine has one, else a folder on the boot disk: the node's
  # cache, Spark's scratch and the generated data live there, and nothing else has to know which.
  cat > "$tmp/startup.sh" <<'EOF'
#!/bin/bash
set -eu
dev=/dev/disk/by-id/google-local-nvme-ssd-0
mkdir -p /mnt/ssd
if [ -e $dev ] && ! mountpoint -q /mnt/ssd; then mkfs.ext4 -q -F $dev; mount -o discard,noatime $dev /mnt/ssd; fi
chmod 777 /mnt/ssd
touch /var/lib/pondra-ready
EOF
  if [ -f "$pubkey" ]; then echo "$user:$(cat "$pubkey")" > "$tmp/ssh-keys"; else echo "$user:<your public key>" > "$tmp/ssh-keys"; fi
  [ $dry = 1 ] && { echo "# startup script:"; sed 's/^/#   /' "$tmp/startup.sh"; }

  # Between the benchmark's own VMs only (the source is their tag): Pondra's HTTP (8080), Flight (8815)
  # and Kafka (9092); Spark's master (7077), its web pages (8090, 8091: 8080 is Pondra's) and its
  # worker, driver and block manager ports, which spark.sh pins into 7100-7399; the ports an
  # executor picks for itself (Linux's 32768-60999); ssh from machine to machine.
  there compute firewall-rules describe "$name-internal" || g compute firewall-rules create "$name-internal" --network "$network" --direction INGRESS \
    --allow tcp:22,tcp:7077,tcp:7100-7399,tcp:8080,tcp:8090-8091,tcp:8815,tcp:9092,tcp:32768-60999,icmp \
    --source-tags "$name" --target-tags "$name"
  there compute firewall-rules describe "$name-ssh" || g compute firewall-rules create "$name-ssh" --network "$network" --direction INGRESS \
    --allow tcp:22 --source-ranges "$ssh_from" --target-tags "$name"
  there storage buckets describe "gs://$bucket" || g storage buckets create "gs://$bucket" --location "${zone%-*}" --uniform-bucket-level-access

  # A VM that nobody deletes costs money: it deletes itself after --max-hours (0: never).
  life=; [ "$max_hours" != 0 ] && life="--max-run-duration=${max_hours}h --instance-termination-action=DELETE"
  # shellcheck disable=SC2086  # ($ssd and $life are flags, or nothing)
  common="--zone $zone --image-family debian-12 --image-project debian-cloud
    --boot-disk-type pd-balanced --scopes storage-rw --tags $name --labels pondra-bench=$name $life
    --metadata enable-oslogin=FALSE --metadata-from-file startup-script=$tmp/startup.sh,ssh-keys=$tmp/ssh-keys"
  names=; for i in $(seq 1 "$n"); do names="$names $name-$i"; done
  # shellcheck disable=SC2086
  g compute instances create $names --machine-type "$machine" --boot-disk-size "${disk_gb}GB" $ssd $common
  # shellcheck disable=SC2086
  g compute instances create "$name-client" --machine-type "$client_machine" --boot-disk-size 30GB $common

  if [ $dry = 1 ]; then
    say "then: wait until each VM answers ssh, and write hosts.txt ($n lines), hosts.internal.txt, client.txt, env.sh"
    say "env.sh: LAKE=gs://$bucket/lake  BUCKET=gs://$bucket"
    return
  fi
  vms=$(gcloud compute instances list --project "$project" --filter "labels.pondra-bench=$name" \
    --format 'value(name,networkInterfaces[0].accessConfigs[0].natIP,networkInterfaces[0].networkIP)')
  : > hosts.txt; : > hosts.internal.txt
  for i in $(seq 1 "$n"); do
    read -r ext int <<< "$(echo "$vms" | awk -v v="$name-$i" '$1 == v { print $2, $3 }')"
    [ -n "$ext" ] || { echo "VM $name-$i not found" >&2; exit 1; }
    echo "$user@$ext $int" >> hosts.txt; echo "$user@$int $int" >> hosts.internal.txt
  done
  read -r ext int <<< "$(echo "$vms" | awk -v v="$name-client" '$1 == v { print $2, $3 }')"
  echo "$user@$ext $int" > client.txt
  printf 'LAKE=gs://%s/lake\nBUCKET=gs://%s\n' "$bucket" "$bucket" > env.sh
  # The first ssh to each VM also puts its key in known_hosts (cluster.sh's ssh can't ask).
  for login in $(cut -d' ' -f1 hosts.txt client.txt); do
    ready=0
    for _ in $(seq 1 60); do
      if ssh -n -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "$login" 'test -e /var/lib/pondra-ready' 2>/dev/null; then ready=1; break; fi
      sleep 5
    done
    [ $ready = 1 ] || { echo "$login is not ready after 5 minutes" >&2; exit 1; }
  done
  echo "up: $n nodes (hosts.txt), a client (client.txt), lake gs://$bucket/lake (env.sh)"
}

sync() {  # what the client VM needs to run scale.py: it runs inside the VPC, the nodes' ports are not open outside
  [ $dry = 1 ] || [ -f client.txt ] || { echo "no client.txt: gcp.sh up first" >&2; exit 2; }
  login=$(cut -d' ' -f1 client.txt 2>/dev/null || echo "$user@<client>")
  bin=${PONDRA_BIN:-$root/target/dist/pondra}
  run ssh "$login" 'mkdir -p pondra/tools/bench pondra/target/dist'
  run scp -rq "$here" "$login:pondra/tools/"
  run scp -rq "$root/tools/bench/tpch-queries" "$login:pondra/tools/bench/"
  run scp -q "$bin" "$login:pondra/target/dist/pondra"
  run scp -q hosts.txt hosts.internal.txt env.sh "$login:pondra/"
  # (the client's ssh to the nodes over private addresses: their host keys, asked for once)
  run ssh "$login" 'ssh-keyscan -H $(cut -d" " -f2 pondra/hosts.txt) >> ~/.ssh/known_hosts 2>/dev/null'
  say "then: ssh -A $login   (-A: the client's ssh to the nodes uses your key), cd pondra"
}

down() {
  if [ $dry = 1 ]; then vms="<VMs-labelled-pondra-bench=$name>"; else vms=$(vm_names); fi
  # shellcheck disable=SC2086
  if [ -n "$vms" ]; then g compute instances delete $vms --zone "$zone" --quiet; fi || true
  g compute firewall-rules delete "$name-internal" "$name-ssh" --quiet || true
  if [ $del_bucket = 1 ]; then
    g storage rm --recursive "gs://$bucket" || true
  else
    say "the bucket gs://$bucket and the lake in it are kept (--bucket deletes them)"
  fi
  if [ $dry = 0 ] && [ -f hosts.txt ]; then
    for login in $(cut -d' ' -f1 hosts.txt client.txt 2>/dev/null); do ssh-keygen -R "${login#*@}" > /dev/null 2>&1 || true; done
  fi
}

"$cmd"
