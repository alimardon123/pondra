# Round-4 test logs

Raw output of the runs the reports quote (`docs/prototype-status.md`,
`docs/comparison-spark-flink-fluss.md`). `r5_*` is the final verification suite: local disk first,
then the same tests against a simulated-R2 bucket (`r5_sim_*`). `r6_*` is the smoke run, the
`--backlog` memory probes and the size/idle-memory measurements of the size-optimised build.

Three logs here come from earlier round-4 runs, kept because the reports quote them:
`pre-change_split_leader70.log` (the same split test before tiering was dealt out to all nodes:
leader share 70 %), `probe_live_noview.log` and `probe_live_noprobe.log` (the same live benchmark
without the aggregating view / without the probing reader, to attribute memory).
