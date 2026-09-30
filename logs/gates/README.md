# Gates, measured

One line per measurement of the gates the roadmap names ("The road to 1.0": sqllogictest, TPC-H SF1
against DuckDB, `vs_postgres.py`; later a Nexmark subset). From round 29, `tools/gates.py` writes
these; until then, by hand.

| Date | Binary | sqllogictest (1 node) | TPC-H SF1, from memory | TPC-H SF1, from files | DuckDB own / files | Notes |
|---|---|---|---|---|---|---|
| 2026-09-29 | round 26 | 18,462 of 24,783 (74.5%) | 2.34 s | 3.62 s | 1.87 s / 4.18 s | `logs/round26/` |
| 2026-09-30 | round 28 (0.27.0, 443e607) | 18,460 (74.5%): three answers in an order SQL leaves open (`GROUP BY`, `LIMIT` without `ORDER BY`), one record newly passing | **3.48 s: regression** | 3.46 s | 1.81 s / 3.69 s | files with a lineage, and an append table's files with deleted rows, skip the hot columns (rounds 27, 28); fix first in round 29 |
| 2026-09-30 | round 28 with the fix (hot columns take files with a lineage or deleted rows) | (unchanged: reads only) | **2.03 s** | 3.60 s | 1.74 s / 3.70 s | `2026-09-30-tpch-sf1-fixed.*`; answers checked against DuckDB's |
