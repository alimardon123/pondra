# Round-5 test logs

`r5e_*` is the final verification suite on the final binary: local disk first, then the same tests
against a **real Cloudflare R2 bucket** (`r5e_r2_*`). `r5f_*` holds the `--backlog` memory probes
and the size / idle-memory measurements of the size-optimised build. `r2_*` and `r2b_*` are the
two earlier full R2 runs (round-4 binary, then the first round-5 binary) — kept because they are
the first evidence the system runs on real object storage.

`r5g_*` fills the gaps the fact-check found: storage bytes per event (`r5g_sizes`), the 5-node
election and cut-off-follower tests on both disk and R2, the same serving benchmark on the
round-4 binary (the "before" number for the dedupe change), and a raw PUT/GET latency probe
against R2 (`r5g_r2_requests`), which is the round trip every R2 number here sits on top of.
