# Benchmarking a Pondra cluster in the cloud

Everything so far was measured on one 2-vCPU box. This kit runs the same binary on several
machines against one bucket, and measures what a cluster adds: ingest spread over nodes,
distributed queries and shuffles.

**No machines of your own?** `.github/workflows/cluster-bench.yml` runs the nodes on
GitHub-hosted runners joined by Tailscale, with the lake in your bucket, and times all 22 TPC-H
queries on one node and across the cluster (`actions/README.md`: the secrets it needs and what it
costs). The steps below are for VMs you run yourself (e.g. a Google Cloud trial).

1. **Machines.** N Linux VMs in one region with the bucket (e.g. 3 × 8 vCPU), plus one client
   VM with Python and `pyarrow`. Open ports 8080 (HTTP), 8815 (Flight) and 9092 (Kafka) between
   them.
2. **Binary.** `cargo build --profile dist` (target/dist/pondra, Linux x86-64 or arm64).
3. **Bucket.** An `env.sh` with `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`,
   `AWS_REGION` and `LAKE=s3://bucket/bench-1`. Keep it out of version control.
4. **Start:** `tools/cloud/cluster.sh start hosts.txt env.sh --memory-gb 24` — `hosts.txt` has one
   `user@host private_ip` per line. The first node to start leads.
5. **Run:** from the client VM,
   `python3 tools/cloud/bench.py --nodes 10.0.0.1:8080:8815,10.0.0.2:8080:8815,10.0.0.3:8080:8815 --rows 1000000000 --writers 24`.
   It loads through Flight (writers round-robin over the nodes, exactly-once), waits until every
   row is Parquet, then runs each query on one node (`?spread=0`) and on the cluster
   (`?spread=1`), and writes `results.json` with every node's `/metrics`.
6. **Stop:** `tools/cloud/cluster.sh stop hosts.txt`, and delete the lake's prefix from the bucket.

Compare 1, 3 and 6 nodes at a fixed data size (does a query get faster?) and at data growing
with the nodes (does it stay as fast?). `bench.py` also runs against local nodes
(`127.0.0.1:8401:8431,…`) to check the kit itself.

## Round 34 on Google Cloud

TPC-H SF100 on 1, 3 and 6 nodes in one zone, time falling, and Spark on the same machines to
compare (`docs/roadmap.md`, round 34). Three scripts and a driver: `gcp.sh` (the machines),
`tpch_parts.sh` (the data, made and loaded in parts by every node), `spark.sh` (Spark on the
same VMs) and `scale.py` (the runs, into `scale.json`). Nothing touches the cloud until you run
it, and `gcp.sh … --dry-run` prints every command instead of running it.

**What you do first**

- A Google Cloud project with billing; `gcloud auth login`, `gcloud config set project <id>`, and
  the two APIs on: `gcloud services enable compute.googleapis.com storage.googleapis.com`.
- Quota in the region for N × 8 vCPUs of N2 (six nodes: 48) and 4 of E2 for the client, N + 1
  external addresses, and the disks (200 GB a node, 30 GB the client; `--local-ssd` adds 375 GB a
  node). A free-trial project's quota is far below that; ask for more, which may mean upgrading
  the account (the trial's credit still pays).
- An ssh key, loaded in your agent (`ssh-keygen -t ed25519`, `ssh-add`): the VMs trust its public
  key, and the client VM uses it, forwarded, to reach the nodes.
- A Linux or macOS shell with `gcloud`, `ssh` and this repository: Google Cloud Shell has all
  three (WSL on Windows works too).
- The Linux binary: a release's (`curl -fsSL https://github.com/alimardon123/pondra/releases/latest/download/pondra-linux-x64.tar.gz | tar xz`,
  then `export PONDRA_BIN=$PWD/pondra`), or `cargo build --profile dist` on Linux (`target/dist/pondra`).

**The commands, in order**

```
# on your machine
tools/cloud/gcp.sh up 6 --local-ssd      # 6 nodes + a client, a firewall, gs://<project>-pondra-bench;
                                         # writes hosts.txt, hosts.internal.txt, client.txt, env.sh
tools/cloud/gcp.sh sync                  # this kit, the queries and the binary, to the client VM
ssh -A "$(cut -d' ' -f1 client.txt)"     # and, on the client VM (it reaches the nodes' private ports):
cd pondra

# data: SF100 made in 6 parts, one per node (tpchgen-cli --parts 6 --part i), copied to the
# bucket for Spark, and loaded by every node at once (one INSERT per table); counts checked
tools/cloud/tpch_parts.sh hosts.internal.txt 100

# Spark's install (Java 17, Spark 4.0, the GCS connector) on every node, once
tools/cloud/spark.sh up hosts.internal.txt 6 && tools/cloud/spark.sh down hosts.internal.txt

# the runs: k = 1, 3, 6 nodes, then Spark with 1, 3, 6 workers; scale.json when done
tools/cloud/scale.py --hosts hosts.internal.txt --env env.sh --sf 100 --spark

# on your machine again
scp "$(cut -d' ' -f1 client.txt):pondra/scale.json" .
tools/cloud/gcp.sh down --bucket         # the VMs, the firewall rules and the bucket (without
                                         # --bucket the lake stays in the bucket, and costs)
```

`scale.py` starts the first k hosts with `cluster.sh` (the one lake `gs://…/lake`; the nodes read
it with the VM's own account, no key anywhere), waits until all k are in the cluster and one
leads, waits until the lake is settled, and times the 22 queries with `actions/driver.py`'s own
measures: on one node at k = 1, else as the cluster decides and spread anyway, best of `--runs`
(3). Every answer is compared with k = 1's. Spark runs with Pondra's nodes stopped. In
`scale.json`: per k `total_s`, `forced_s`, `spread` (queries that ran across the nodes),
`all_same`, and per query its time and how it ran; `speedup` and `time_falls` over the sizes;
`spark_over_pondra` (above 1: Pondra is faster) and `spark_rows_same` (row counts, as
`tools/bench/tpch.py` compares them). `--runs 2`, `--no-forced` and `--only 1,3,6` shorten it.

**What it costs** (on-demand, us-central1; look up current prices and your zone's)

| | each | |
|---|---|---|
| n2-standard-8 node | about $0.39 an hour | + $0.03 for its 200 GB disk, $0.04 for a local SSD |
| e2-standard-4 client | about $0.13 an hour | |
| the bucket | about $0.02 per GB-month | the lake and the Parquet: tens of GB |

Six nodes and the client are about $2.9 an hour. My guess at the hours, before the first run:
VMs up and data made and loaded about 1 h (loading is the unknown), Spark's install 0.1 h,
Pondra at three sizes 1 to 1.5 h, Spark at three sizes 1 to 2 h: 3 to 4.5 hours, so 18 to 27
node-hours and about $12 to $15. A VM that is forgotten deletes itself after `--max-hours`
(12; 0 turns it off). Run it in one sitting and `down` right after.

`gcp.sh up N` takes `--zone`, `--machine`, `--name`, `--project`, `--network`, `--ssh-pub-key`
and `--ssh-from` too (`gcp.sh` has the list). The nodes' serve flags are `--memory-gb 22
--cache-dir /mnt/ssd/pondra-cache` unless `$PONDRA_SERVE_FLAGS` (or `scale.py --serve-flags`)
says otherwise. To check the kit without a cloud, `tools/cloud/scale.py --local --nodes 3` runs
the same steps on this machine (nodes on 127.0.0.1, a small stand-in for the data; the binary
at `target/release/pondra`): it checks the plumbing, it is not a benchmark.
