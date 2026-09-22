# Benchmarking a Pondra cluster in the cloud

Everything so far was measured on one 2-vCPU box. This kit runs the same binary on several
machines against one bucket, and measures what a cluster adds: ingest spread over nodes,
distributed queries and shuffles.

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
