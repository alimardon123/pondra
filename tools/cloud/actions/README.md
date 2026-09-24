# A cluster on GitHub Actions

`.github/workflows/cluster-bench.yml` starts a Pondra cluster on separate GitHub-hosted machines,
loads TPC-H into your bucket, and runs the 22 queries on one node and across all of them. It runs
only when started by hand, and deletes its lake when it's done.

## What it needs

- **Your R2 bucket** (or any S3-compatible one). Repository secrets `R2_ACCESS_KEY_ID`,
  `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` (`https://<account>.r2.cloudflarestorage.com`) and
  `R2_BUCKET`. An SF10 lake takes about 4 GB while it runs.
- **A Tailscale auth key** in `TS_AUTHKEY`: GitHub's machines can reach out but not each other,
  and Pondra's nodes talk over HTTP. Tailscale's free plan joins them into one private network.
  In the Tailscale admin console → Settings → Keys, make a **reusable, ephemeral** key (the
  machines disappear from your network when the run ends).

## Running it

Actions → cluster-bench → Run workflow. Inputs: `nodes` (3), `sf` (10), `binary` (`build`
compiles this repo's source; `r2` takes `bench-bin/pondra` from the bucket) and `runner`.

It runs a `binary` job, one `node` job per node, and a `driver` job that generates the data with
`tpchgen-cli`, loads it with `pondra sql` and runs `tools/cloud/actions/driver.py`. The results go
to `s3://<bucket>/bench-results/<run id>/results.json` (and every node's log next to it), and
are attached to the run as an artifact.

For a comparison that means something, run it three times — `nodes` = 1, 3 and 6 — at the same
`sf`: each `results.json` has every query's time on one node and across the cluster.

## What it costs

- **A private repository:** 2-vCPU / 8 GB runners, and the run counts against your monthly
  Actions minutes (a 3-node SF10 run is roughly 5 jobs × 30–40 minutes).
- **A public repository:** 4-vCPU / 16 GB runners, free and unlimited. To keep Pondra's source
  private, make a small public repo with only this workflow, `tools/cloud/actions/driver.py` and
  `tools/bench/tpch-queries/`, and run it with `binary: r2` (the binary is read from your bucket;
  its logs are public, your source isn't).
- Every job may run at most 6 hours; runners have 14 GB of disk (SF10 fits, SF100 doesn't), and
  they are shared machines: compare the shapes (1 → 3 → 6 nodes), not the last decimal.

## Reading the results

`results.json`: `load_s`, `one_node_s` and `cluster_s` (all 22 queries), `spread` (how many ran
across the nodes), `all_same` (every answer equal to one node's), and per query `one_node_s`,
`cluster_s`, `how` (shuffled, gathered, one node) and `same`, plus each node's `/metrics`.
