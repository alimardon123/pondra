# Round 7 test and benchmark logs

- `r7_*`: the full suite on the round-7 binary: local disk, simulated R2 (`sim_*`) and real R2
  (`r2_*`). The real-R2 lakes were kept in the bucket under `round7/`.
- `r7_serve_before.log`: the serving benchmark on the round-6 binary (the "before").
- `r7_serve_1.log`, `r7_serve_2.log`: the serving benchmark on the leader, and on a read-only
  node.
- `r7_tpch.log`, `r7_tpch_spark.log`: TPC-H SF1 on Pondra and DuckDB, then Spark on its own. The
  Spark step of the combined run failed once (no output) and was re-run by itself.
- `r7_batch`, `r7_stream`, `r7_etl`, `r7_live8`: `tools/bench/run.py` for Pondra. The second
  value of each batch query is the result cache answering.

Secrets, the R2 endpoint and the account id are redacted.
