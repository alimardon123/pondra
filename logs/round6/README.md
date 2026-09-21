# Round 6 test logs

- `r6c_*`: the full suite on the round-6 binary, run on local disk, on simulated R2 (`sim_*`) and
  on real R2 (`r2_*`). The real-R2 lakes were kept in the bucket under `round6/`.
- `r6d_*`: the last changes re-verified: the in-memory catalog is loaded once at startup,
  read-only nodes read the catalog's WAL when no leader is alive, and the catalog's files are
  kept on the SSD tier. It covers users, failover, isolate, latency and Delta on local disk,
  plus simulated and real R2.
- `before-fix_users_mirror_bug.log`: the bug this round's tests found first. The in-memory
  catalog skipped commits, which showed up as torn reads and lost batches.
- `r6c_demo_local.log`, `r6b_demo_r2.log`: `tools/demo_lake.py` on a local folder and on the R2
  bucket (`pondra-demo/`, kept), with their trees: the same layout.
- `r6c_ssd_demo.log`: a read-only node on the kept R2 demo lake: row counts, then its SSD tier
  folder.

Secrets, the R2 endpoint and the account id are redacted.
