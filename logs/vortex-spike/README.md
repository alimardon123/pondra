# Vortex spike: results (L2, 2026-11-28)

For L1 (11-21 21:33): Vortex as a table file format beside Parquet, measured against it. Branch `claude/l2-vortex` at
67a7dba5 (main cc5d8d82 merged in). Not to be merged: the bar is "faster, no query slower". Raw data:
`l2-wip/vortex/`.

## The answer

**Reads: Vortex is faster, and no query is slower.** TPC-H SF10 runs in 0.68× the wall time and 0.71× the CPU, and all
22 queries are faster (0.27× to 0.96×). The bigger-than-memory set answers 17 of 17 with both formats. It is as fast
or faster everywhere: scan-bound queries take 0.78-0.85×, and spill-bound ones tie (0.96-1.02× over four runs each).
Every answer is the same as Parquet's.

**What it costs:**

- Writes are slower: loads take 19-38% longer (lineitem at SF10: 23.7 s against 19.8 s).
- Scans hold more memory: a full COPY peaks at 1,376 MB against 760 MB, and other queries 5-15% more. All stay under
  the 2 GB cap.
- The binary grows by 34 MB (+15%), and 93 crates are added.
- Files are 11% smaller.

**What it can't do yet:** other engines don't read Vortex, so a table published as Delta or Iceberg has to stay
Parquet. That is a rule for 15c's per-table `file_format`. Several paths still read only Parquet (below).

## What was built

- **The `vortex` cargo feature** (off by default) adds `vortex-datafusion` 0.93.0 and `vortex` 0.93.0. These match
  DataFusion 55.1, Arrow 59.3 and object_store 0.13.2, so no version moved except `zstd-sys` 2.1.0 → 2.1.3. They
  are non-wasm dependencies only.
- **`src/file_formats/vortex.rs`** is the entry. It is registered by `engine.rs` when the feature is on.
  - **writer:** Vortex's writer is async, and the future it keeps isn't `Send`. So each file gets its own thread,
    which drives Vortex's blocking writer.
    - Batches reach the thread over a channel that holds one batch.
    - The thread enters the engine's tokio runtime, because Vortex spawns its compression work there.
    - `bytes()` is the bytes written plus what Vortex's layouts still hold.
    - The ranges and sizes a file's entry needs are kept from its rows as they pass (min and max accumulators). This
      is exact and doesn't depend on the format.
  - **reader:** `VortexFormat`, with a process-wide `VortexSession`. `PONDRA_VORTEX_SCAN_V2=1` switches to its v2 scan;
    that was not measured.
  - **select:** this is Vortex's own row selection (`VortexAccessPlan`: rows by place, included or excluded). So deletes
    and row ranges work: DELETE, UPDATE and a tier after them all answer as Parquet does.
  - **take and filtering:** None (#868's defaults). Late rows and wide-scan filtering stay Parquet-only, and a Vortex
    scan does its own predicate pushdown.
- **The temporary switch:** `PONDRA_FILE_FORMAT=vortex` makes `file_formats::default()` Vortex for a whole lake. It
  stays only on the spike branch, until 15c's table option.
- **Two spike-only guards**, needed because these paths read Parquet bytes directly:
  - A CTAS doesn't keep a Parquet source's blocks (`keep_load`).
  - A key lookup is planned as SQL (`serve::point`).

## Measured

All runs used one machine (4 cores, 15 GB) and **one binary**, a release build with the feature. Only the node's env
differed: `PONDRA_FILE_FORMAT=parquet` with `PONDRA_KEEP_PARQUET=0`, so both formats are written by our own writer
(Parquet with LZ4), against `PONDRA_FILE_FORMAT=vortex`. The hot tier was off (`PONDRA_HOT_GB=0`), because it holds
Arrow and would hide the format. The data is TPC-H SF10 from tpchgen-cli.

### Size and dependencies

| | without `vortex` | with it | change |
|---|---|---|---|
| release binary, stripped | 232.7 MB | 267.0 MB | +34.4 MB (+15%) |
| the same, gzipped | 81.8 MB | 92.8 MB | +11.0 MB |
| `cargo tree -e normal,build`, lines | 472 | 576 | +104 |
| crates new to the build | | 93 | 37 of them vortex-*, plus the smol/async-io stack, fsst-rs, fastlanes, pco, roaring, jiff, parquet-variant |

(The release profile, not `dist`, which has thin LTO and is stripped: the delta there would be somewhat smaller.)

### TPC-H SF10

`tools/bench/ab_compare.py --load ctas --hot-gb 0 --rounds 5`. Each format gets its own node and lake, the queries
alternate between them, and the whole run is repeated with the load order swapped. The ratios below are the geometric
means of the two runs. Answers were identical in both runs (`ab-sf10.json`).

| | Parquet | Vortex | Vortex / Parquet |
|---|---|---|---|
| 22 queries, wall (sum of medians) | 16.6 s | 11.3 s | **0.684** |
| 22 queries, node CPU | 58.7 s | 41.5 s | **0.706** |
| load, 8 tables by CTAS (second run's) | 29.0 s | 36.7 s | 1.27 |

| q | Parquet s | Vortex s | B/A | | q | Parquet s | Vortex s | B/A |
|---|---|---|---|---|---|---|---|---|
| 1 | 1.067 | 0.823 | 0.77 | | 12 | 0.559 | 0.292 | 0.52 |
| 2 | 0.151 | 0.091 | 0.61 | | 13 | 0.707 | 0.572 | 0.81 |
| 3 | 0.666 | 0.303 | 0.46 | | 14 | 0.403 | 0.143 | 0.35 |
| 4 | 0.466 | 0.336 | 0.72 | | 15 | 0.355 | 0.140 | 0.40 |
| 5 | 0.747 | 0.447 | 0.60 | | 16 | 0.344 | 0.328 | 0.96 |
| 6 | 0.339 | 0.116 | 0.34 | | 17 | 0.755 | 0.520 | 0.69 |
| 7 | 0.797 | 0.407 | 0.51 | | 18 | 1.541 | 1.325 | 0.86 |
| 8 | 0.896 | 0.561 | 0.63 | | 19 | 0.667 | 0.181 | 0.27 |
| 9 | 1.651 | 1.365 | 0.83 | | 20 | 0.621 | 0.447 | 0.72 |
| 10 | 0.753 | 0.337 | 0.45 | | 21 | 2.773 | 2.361 | 0.85 |
| 11 | 0.108 | 0.072 | 0.66 | | 22 | 0.182 | 0.153 | 0.84 |

The two runs agree to within 0.06 on every query. The biggest gains are on selective scans of lineitem: q19 0.27, q6
0.34, q14 0.35, q15 0.40. Those are Vortex's pushed-down filters and its cheaper decoding. The smallest gain is q16
(0.96), which is mostly partsupp and part joins.

### Bigger than memory (2 GB cgroup cap)

This is `logs/round36/bigger-than-memory/pbig.py` and its `queries.json`. It loads SF10's lineitem, orders and
customer by CTAS under the cap, then runs the set. Two queries were left out:

- the full sort to a file, which spills 15 GB, more than this sandbox's disk;
- `string_agg` over 3 groups, not run here: its group outgrows memory whichever format the rows come from.

A first pass ran every query once per format. The queries that looked slower in it were then run 3 more times,
alternating formats (`big-*.jsonl`).

| query | Parquet s | Vortex s | V/P | peak RSS MB P/V |
|---|---|---|---|---|
| load lineitem, 60 M rows | 19.2-19.8 | 22.8-23.7 | 1.19 | 1406/1420 |
| load orders | 4.3-4.5 | 6.0-6.1 | 1.38 | 1396/1456 |
| TPC-H q1 scan | 1.0 | 0.8 | 0.80 | 1019/1154 |
| COPY 59 M rows to Parquet | 11.9 (med of 4) | 12.1 | 1.01 | 760/1376 |
| top 10 by a string | 0.7 | 0.7 | 1.00 | 707/1075 |
| GROUP BY 60 M integer groups | 5.2 (med of 4) | 5.2 | 1.00 | 1291/1438 |
| GROUP BY strings | 10.6 (med of 4) | 10.8 | 1.02 | 1126/1294 |
| count(DISTINCT string) | 10.6 | 10.3 | 0.97 | 1106/1249 |
| DISTINCT over 4 columns | 12.8 (med of 4) | 12.8 | 1.00 | 1162/1308 |
| join building 15 M rows | 8.4 | 8.1 | 0.96 | 1276/1281 |
| self-join building 60 M rows | 10.1 (med of 4) | 9.6 | 0.96 | 1716/1739 |
| three-way join, 1 M groups | 3.5 (med of 4) | 3.5 | 0.99 | 1571/1657 |
| EXCEPT | 5.6 | 5.6 | 1.00 | 1609/1559 |
| TPC-H q18 | 2.0 | 1.7 | 0.85 | 1313/1390 |
| row_number() by order | 8.9 | 8.6 | 0.97 | 1440/1420 |
| rank() over 3 partitions | 31.9 | 32.0 | 1.00 | 1051/1289 |
| avg() OVER 3 partitions | 2.3 | 2.1 | 0.91 | 596/769 |
| count(*) OVER () | 0.8 | 0.7 | 0.87 | 610/739 |
| median over 3 groups | 0.9 | 0.7 | 0.78 | 838/966 |
| **lake on disk** | **2,824 MB** | **2,500 MB** | **0.89** | |

The first pass showed "GROUP BY 60 M integer groups" at 1.11 and "self-join building 60 M rows" at 1.16. Over four
runs each those came out 1.00 and 0.96: the first Vortex run was the outlier (5.9 s and 11.9 s), and the medians are
within each format's own spread. Peak spill is the same for both formats, except "rank() over 3 partitions" (2,046
against 2,387 MB) and "three-way join" (0 against 66 MB).

## What's left, if L1 wants Vortex beyond the spike

1. **15c: a table's `file_format`.**
   - Keep each file's format in its entry, so a lake can hold both and a table can change format without a rewrite.
     Today `Files.format` is one format per scan.
   - Refuse `publish = 'delta'/'iceberg'` on a Vortex table, by name. Other engines can't read it.
2. **Parquet-only paths to move behind the entry:**
   - key lookups and unique checks (`serve::lookup`, `lookup_versioned`: they read row groups and bloom filters);
   - the hot tier's loads (`hot.rs`);
   - CTAS keeping a source's blocks (`keep_load`, correct as it stands: a Parquet source kept into a Parquet table);
   - the struct-ids footer check (`tier::struct_ids`);
   - row-number scans (`lake_scan.rs` with `row: Some`).
   With the guards above none of these is wrong on a Vortex lake, but each loses its fast path.
3. **Write speed:** loads are 19-38% slower. The writer thread and the channel of one batch may cost part of that.
   Vortex's own compression (BtrBlocks) is most of it.
4. **Peak memory on wide scans** (COPY: +80%). Worth a look at `scan_concurrency` and its segment cache before Vortex
   could be anyone's default.
5. **Not measured:**
   - the hot tier on (the default when data fits in memory, where the format matters less);
   - Vortex's v2 scan (`PONDRA_VORTEX_SCAN_V2=1`);
   - S3 or R2 rather than a local disk;
   - the `dist` profile's binary.
