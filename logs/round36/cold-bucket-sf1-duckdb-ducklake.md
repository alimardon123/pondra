# B49: cold reads from the bucket against DuckDB 2.0.0, and DuckLake 1.1 as a rival row

**Date:** 2026-10-31 · **Brief:** B49, review items 2 and 5 of `reviews/duckdb-polars-2-features.md` · **Pondra:** a task-branch build of 10-31 (it calls itself 0.34.0) · **Raw results:** the run's JSON lines were not kept; this report is the record.

This report states numbers only. Every time is on one simulated bucket (see Setup), so the seconds say how these engines behave against this simulator, not against R2 or S3.

## Setup and how it was run

- **Engines.** DuckDB 2.0.0 (`httpfs` 77392a7207, `ducklake` extension 417a9117f5; the catalog's metadata reads DuckLake format version 1.1). Pondra `pondra 0.34.0` (`scratchpad/pondra-task`).
- **Bucket.** `tools/sim_r2.py --port 19760`: moto 5.2.3 with boto3 1.43.106, in RAM, default latency. GET/HEAD/LIST lognormal p50 100 ms (sigma 0.426); PUT p50 197 ms; no write limits. Requests are counted per class by the simulator: read = GET and HEAD, list = LIST, write = PUT, POST and multipart, delete. The simulator is one Python process, so its throughput limits every number here.
- **Data.** TPC-H SF1 Parquet from `/root/tpch/sf1` (8 tables). DuckDB and Pondra files read the same files as uploaded to `s3://tpch/<table>/<table>.parquet`, unchanged. The Pondra lake and DuckLake load the same files from local disk with `CREATE TABLE … AS`.
- **Queries.** `origin/main:tools/bench/tpch-queries`, with q15's view written as a CTE (as `tools/bench/tpch.py` does). DuckDB and Pondra ran the same text with no other change. Query 0 is a baseline, `SELECT count(*) FROM nation`, used to show the fixed cost of a cold run.
- **Answer check.** Each cold answer was written as CSV by `COPY` in the same process. Row counts are equal across all four engines for all 22 queries and the baseline. Digests of numbers rounded to 5 significant digits are equal everywhere. Exact-text digests differ only for q1, q8, q14 and q17, where Pondra prints DECIMAL results at a fixed scale (averages at 6 places, ratios at 8 to 10) and DuckDB prints full doubles. q18's header differs only in the column name (`sum(lineitem.l_quantity)` in Pondra, `sum(l_quantity)` in DuckDB).
- **Timing.** Wall time from launch to exit of the process, with the process doing its own setup (extension load, secret, views or the attach) and then the query. Cold numbers are the median of 3 runs. Requests are the simulator's counts for that process, taken as the difference before and after it.
- **"Cold" for each engine.**
  - DuckDB 2.0.0, files: a new python process per run; `httpfs` loaded, secret and 8 views created in that process. Its external file cache is **on by default in 2.0.0** (`enable_external_file_cache` = true; the brief expected it off). I left it at the default. It starts empty in each new process, and it is in-process only. The HTTP metadata cache is off.
  - Pondra, files: a new `pondra-task sql --dir :memory:` process in a new, empty working folder, `PONDRA_CACHE_DIR` empty; secret and 8 views created in the process. Pondra's cache of byte ranges of files outside the lake is in memory and starts empty.
  - Pondra, lake: a new `pondra-task sql` process on `s3://lakes/pondra-s1`, `PONDRA_CACHE_DIR` empty, no node running.
  - DuckLake: a new python process: `LOAD`, secret, `ATTACH` to the local catalog file, `USE`, then the query. The OS page cache was not dropped, so the local catalog file may be warm. No external file cache survives the process.
- **Warm.** DuckLake: one python process running the 22 queries twice (pass 1 and pass 2). Pondra: one `serve` node with a fresh cache, `POST /tier`, then the 22 queries twice over HTTP. The Pondra answers came back over HTTP and were timed without writing a file; rows were counted after the timer and matched the cold row counts.

## Section 1: cold reads from the bucket (item 2)

Cold wall seconds, median of 3 runs. Requests in brackets for run 1 as read / list / write.

| query | DuckDB 2.0.0, files | Pondra, files | Pondra, lake |
|---|---|---|---|
| q0 (baseline) | 2.27 (17 / 0 / 0) | 2.11 (16 / 0 / 0) | 1.83 (33 / 2 / 0) |
| q1 | 10.05 (70 / 0 / 0) | 10.37 (73 / 0 / 0) | 2.53 (48 / 2 / 0) |
| q2 | 3.78 (59 / 0 / 0) | 4.23 (69 / 0 / 0) | 2.46 (60 / 2 / 0) |
| q3 | 22.79 (197 / 0 / 0) | 11.88 (104 / 0 / 0) | 2.73 (67 / 2 / 0) |
| q4 | 18.96 (156 / 0 / 0) | 18.66 (158 / 0 / 0) | 2.78 (63 / 2 / 0) |
| q5 | 22.05 (184 / 0 / 0) | 12.08 (109 / 0 / 0) | 2.73 (74 / 2 / 0) |
| q6 | 12.59 (86 / 0 / 0) | 10.39 (73 / 0 / 0) | 2.49 (46 / 2 / 0) |
| q7 | 23.64 (183 / 0 / 0) | 12.19 (109 / 0 / 0) | 2.81 (82 / 2 / 0) |
| q8 | 20.26 (173 / 0 / 0) | 12.33 (117 / 0 / 0) | 2.96 (84 / 2 / 0) |
| q9 | 19.95 (194 / 0 / 0) | 13.22 (126 / 0 / 0) | 2.92 (79 / 2 / 0) |
| q10 | 21.54 (182 / 0 / 0) | 11.64 (101 / 0 / 0) | 2.98 (80 / 2 / 0) |
| q11 | 3.37 (35 / 0 / 0) | 4.46 (56 / 0 / 0) | 1.84 (50 / 2 / 0) |
| q12 | 21.44 (172 / 0 / 0) | 19.31 (162 / 0 / 0) | 2.59 (56 / 2 / 0) |
| q13 | 3.98 (54 / 0 / 0) | 3.71 (36 / 0 / 0) | 2.27 (55 / 2 / 0) |
| q14 | 20.38 (144 / 0 / 0) | 10.53 (79 / 0 / 0) | 2.50 (53 / 2 / 0) |
| q15 | 14.69 (103 / 0 / 0) | 10.15 (74 / 0 / 0) | 2.57 (47 / 2 / 0) |
| q16 | 3.41 (39 / 0 / 0) | 3.57 (43 / 0 / 0) | 2.04 (49 / 2 / 0) |
| q17 | 12.25 (93 / 0 / 0) | 17.55 (128 / 0 / 0) | 2.21 (67 / 2 / 0) |
| q18 | 13.98 (122 / 0 / 0) | 11.40 (98 / 0 / 0) | 2.75 (77 / 2 / 0) |
| q19 | 20.47 (146 / 0 / 0) | 10.94 (79 / 0 / 0) | 2.22 (48 / 2 / 0) |
| q20 | 20.93 (161 / 0 / 0) | 10.43 (100 / 0 / 0) | 2.77 (75 / 2 / 0) |
| q21 | 21.61 (174 / 0 / 0) | 19.25 (150 / 0 / 0) | 2.84 (77 / 2 / 0) |
| q22 | 3.64 (42 / 0 / 0) | 3.69 (47 / 0 / 0) | 2.43 (49 / 2 / 0) |
| **total (22 medians)** | **335.75 s** | **241.98 s** | **56.41 s** |
| requests, run 1, all 22 | 2,769 reads | 2,091 reads | 1,386 reads, 44 lists |

Other facts from this section:
- The Pondra lake's run-to-run read count varied, 44 to 48 on q1. The lake's SSD tier held 213 MB on disk after the q1 run alone, because it fetches whole objects. Pondra's files path held nothing on disk.
- The Pondra lake load (8 tables, `CREATE TABLE … AS` from local Parquet) took 48.37 s in total, with 141 writes, 207 reads, 32 lists and 8 deletes.

**What this says.** Cold, the 22 queries total 335.75 s for DuckDB reading the files, 241.98 s for Pondra reading the files, and 56.41 s for the Pondra lake. The baseline costs 1.83 to 2.27 s per process, which is a fixed part of every cold number. The request counts are 2,769 reads (DuckDB files), 2,091 reads (Pondra files) and 1,386 reads plus 44 lists (Pondra lake). The Pondra lake's cold runs fetch whole objects into their SSD tier, while the two file paths read byte ranges.

## Section 2: DuckLake 1.1 against a Pondra lake, both in the bucket (item 5)

The Pondra lake is the one loaded in section 1 and reused here (same load, same runs). DuckLake's catalog is a local DuckDB file, `ducklake:…/cat.ducklake`, and its data is in `s3://lakes/dl-s2/`.

| measure | DuckLake 1.1 | Pondra lake |
|---|---|---|
| load, 8 tables (one process per table) | 14.53 s; 35 writes, 14 reads | 48.37 s; 141 writes, 207 reads, 32 lists, 8 deletes |
| first answer, `count(*) FROM lineitem`, new process, median of 3 | 0.34 s; 0 requests | 1.66 s; 33 reads, 2 lists |
| cold, 22 queries, total of medians | 239.85 s (2,113 reads) | 56.41 s (1,386 reads, 44 lists) |
| warm, pass 1, 22 queries | 60.44 s; 570 reads | 3.45 s; 62 reads, 3 lists, 9 writes, 18 deletes |
| warm, pass 2, 22 queries | 1.99 s; 0 reads | 0.071 s; 1 read |
| 500 one-row INSERTs, one process: p50 / p99 / max | 12.66 ms / 17.74 ms / 41.81 ms; 0 requests | 251.5 ms / 708.4 ms / 846.5 ms; 727 writes, 1,040 reads, 15 lists, 345 deletes over the 500 |
| bucket bytes after all of it | 323.6 MB, 7 objects (`lakes/dl-s2/`) | 347.8 MB, 557 objects (`lakes/pondra-s1/`, including the 500 inserts' commits) |

Per query, cold and warm (seconds):

| query | DuckLake cold | Pondra lake cold | DuckLake warm pass 1 | DuckLake warm pass 2 | Pondra lake warm pass 1 | Pondra lake warm pass 2 |
|---|---|---|---|---|---|---|
| q0 (baseline) | 0.33 | 1.83 | | | | |
| q1 | 10.33 | 2.53 | 9.591 | 0.066 | 0.692 | 0.0015 |
| q2 | 1.53 | 2.46 | 1.276 | 0.049 | 0.173 | 0.0013 |
| q3 | 17.40 | 2.73 | 7.960 | 0.084 | 0.461 | 0.0012 |
| q4 | 14.72 | 2.78 | 6.696 | 0.048 | 0.098 | 0.0037 |
| q5 | 12.88 | 2.73 | 2.311 | 0.091 | 0.148 | 0.0013 |
| q6 | 11.33 | 2.49 | 0.045 | 0.042 | 0.075 | 0.0010 |
| q7 | 18.10 | 2.81 | 0.114 | 0.100 | 0.181 | 0.0015 |
| q8 | 10.88 | 2.96 | 2.275 | 0.113 | 0.149 | 0.0016 |
| q9 | 10.45 | 2.92 | 0.315 | 0.203 | 0.225 | 0.0012 |
| q10 | 15.06 | 2.98 | 6.852 | 0.123 | 0.172 | 0.0011 |
| q11 | 1.15 | 1.84 | 0.306 | 0.034 | 0.029 | 0.0012 |
| q12 | 20.44 | 2.59 | 12.976 | 0.064 | 0.104 | 0.0016 |
| q13 | 2.03 | 2.27 | 1.043 | 0.142 | 0.114 | 0.0012 |
| q14 | 11.05 | 2.50 | 0.079 | 0.059 | 0.048 | 0.0013 |
| q15 | 10.96 | 2.57 | 0.051 | 0.050 | 0.078 | 0.0012 |
| q16 | 1.37 | 2.04 | 0.191 | 0.057 | 0.053 | 0.0396 |
| q17 | 9.70 | 2.21 | 0.250 | 0.126 | 0.084 | 0.0017 |
| q18 | 10.07 | 2.75 | 0.641 | 0.115 | 0.157 | 0.0010 |
| q19 | 20.27 | 2.22 | 6.238 | 0.116 | 0.151 | 0.0026 |
| q20 | 11.94 | 2.77 | 0.086 | 0.083 | 0.080 | 0.0012 |
| q21 | 16.84 | 2.84 | 1.086 | 0.165 | 0.162 | 0.0015 |
| q22 | 1.35 | 2.43 | 0.054 | 0.060 | 0.018 | 0.0017 |

Answers: DuckLake's cold answers match the other three engines (same row counts, same normalized digests, the same known printing differences). Its warm passes' row counts match its cold row counts.

**What this says.** The DuckLake load took 14.53 s and wrote 35 objects; the Pondra lake load took 48.37 s and wrote 141. A first answer costs 0.34 s with no requests on DuckLake, and 1.66 s with 33 reads on the Pondra lake. Cold, the 22 queries total 239.85 s on DuckLake and 56.41 s on the Pondra lake. Warm pass 1 is 60.44 s with 570 reads on DuckLake and 3.45 s with 62 reads on the Pondra lake. Warm pass 2 is 1.99 s with 0 reads on DuckLake, and 0.071 s with 1 read on the Pondra lake. The Pondra pass 2 is a result-cache hit (see notes), so it is not query execution. 500 one-row INSERTs cost a p50 of 12.66 ms on DuckLake with no requests, and 251.5 ms on the Pondra lake with about 3 requests each. The bucket holds 323.6 MB of DuckLake data and 347.8 MB of Pondra lake data after all of it.

**DuckLake 1.1 inlining.** Data inlining is on with the defaults used here (no option set on ATTACH). The 500 single-row inserts landed in the local catalog (an `ducklake_inlined_data_1_1` table, catalog 7.6 MB at the end) and made no bucket requests. I did not read the extension's default row limit.

## Notes

- **Not as the brief said.** The DuckDB external file cache is on by default in 2.0.0, not off; I left it at the default. In DuckLake pass 2 DuckDB answered from that in-process cache with 0 bucket reads, and its pass 2 timings are of executing the query over cached bytes.
- **Pondra warm pass 2 is a result-cache hit.** The node keeps a result cache of answers; a session that sets a `datafusion.*` option is not answered from it (`src/settings.rs`). I did not rerun with that setting, so the Pondra pass 2 figures are cache answers, not execution.
- **Reused, not repeated.** The Pondra lake load and cold runs in section 2 are the section 1 runs.
- **Interrupted and resumed.** The DuckDB files cold step was stopped by its background time limit after 43 of 69 runs. The completed runs are logged (rc 0) and the rest ran with `--resume` under the lock; all 69 runs are in `cold-duck-files.jsonl`.
- **Warm path differs by engine.** The first Pondra warm attempt failed: a node refuses `COPY … TO` a server path over HTTP (403), so it logged no records. The Pondra warm answers therefore come back over HTTP and are timed without a file write; DuckLake's warm passes write CSV with `COPY` inside the timed step.
- **The Pondra `:memory:` folder.** On this binary, `pondra sql --dir :memory:` does not hold a lake in memory. It writes a local catalog folder named `:memory:` in the working directory, so each files run's catalog writes (one secret and eight views) left a small local catalog. Each run had a new, empty working folder, removed after the run, so no state carried between runs and no `:memory:` folder was left behind. Measured in empty folders: one `CREATE VIEW` leaves about 25 to 26 KB (13 to 15 files with its SELECT; 51 to 64 ms wall, process start included); eight views leave about 29 KB in 20 to 22 files (65 to 96 ms). Against 10 to 23 s per files run, that is on the order of 0.1 s per run, not separated out in the timings. An earlier `SELECT 1` attempt in this check exited 1 ("holds no lake yet") and measured nothing.
- **Not run or not measured.** Real R2 or S3; the DuckDB external file cache off; scale factors above SF1; a warm Pondra pass with the result cache bypassed. Process-level CPU and memory were not recorded.
