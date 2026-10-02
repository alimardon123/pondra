# Gates, measured

One line per measurement of the gates the roadmap names ("The road to 1.0": sqllogictest, TPC-H SF1
against DuckDB, `vs_postgres.py`; later a Nexmark subset). From round 29, `tools/gates.py` writes
these; until then, by hand.

| Date | Binary | sqllogictest (1 node) | TPC-H SF1, from memory | TPC-H SF1, from files | DuckDB own / files | Notes |
|---|---|---|---|---|---|---|
| 2026-09-29 | round 26 | 18,462 of 24,783 (74.5%) | 2.34 s | 3.62 s | 1.87 s / 4.18 s | `logs/round26/` |
| 2026-09-30 | round 28 (0.27.0, 443e607) | 18,460 (74.5%): three answers in an order SQL leaves open (`GROUP BY`, `LIMIT` without `ORDER BY`), one record newly passing | **3.48 s: regression** | 3.46 s | 1.81 s / 3.69 s | files with a lineage, and an append table's files with deleted rows, skip the hot columns (rounds 27, 28); fix first in round 29 |
| 2026-09-30 | round 28 with the fix (hot columns take files with a lineage or deleted rows) | (unchanged: reads only) | **2.03 s** | 3.60 s | 1.74 s / 3.70 s | `2026-09-30-tpch-sf1-fixed.*`; answers checked against DuckDB's |
| 2026-10-01 | 0.27.0 (22fd821) | 18,455 of 24,783 (74.5%) | 2.11 s | 3.31 s | 1.68 s / 3.42 s | Nexmark 2M bids 2.75 s, answers right; vs Postgres: `2026-10-01-postgres.txt`; round 29 part 2 (22fd821): unwinding panics, TLS, audit and quotas; **dropped:** sqllogictest: 18455 passed, 18460 before (explained: sqllogictest: 6 records by naming an answer's duplicate columns apart on purpose (wildcard.slt expects SELECT *, a refused; join_is_not_distinct_from's plans show AS val_1), and the order SQL leaves open (group_by.slt: 2 failing, 3 passing)) |
| 2026-10-01 | 0.28.0 + round 30 (on d094b55, before its commit) | not run | not run | not run | not run | pgbench 1c 134 tps, 4c 95 tps (Postgres 997 and 1,793), balances right; lookup through the Postgres port p50 0.29 ms (Postgres 0.09); vs Postgres: `2026-10-01-postgres.txt`; round 30: transactions, the point path |
| 2026-10-02 | 0.30.0 + round 32 (143b157) | not run | 1.26 s | 2.23 s | 1.03 s / 2.06 s | round 32 so far (join order from every input, planning that costs less, one session template) |
