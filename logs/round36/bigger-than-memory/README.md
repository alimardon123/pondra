# Bigger than memory, rerun (B50): 2026-10-31, main at 55a774f

**For:** the freeze list and the scorecard. **From:** L1. Reruns `reviews/bigger-than-memory.md` (10-16, main at 3be8783)
on today's main, under the same hard 2 GB cgroup cap, with DuckDB 2.0.0 final (10-16 had a 2.0.0 preview). Polars was
not rerun (4/19 on 10-16; nothing in it changed for us).

## The answer

**Pondra answers 16 of 19 under 2 GB (10-16: 14 of 19).** All three windows that killed the node on 10-16 now answer,
and the median is refused by name instead of killing the node. DuckDB 2.0.0 answers 17 of 19 here (18 on 10-16: its
full sort ran out of this sandbox's disk, not memory). Every answer both gave is the same.

Still open:

1. **`string_agg` over 3 groups still kills the node** (0.6 s, the cgroup kills it before the memory watch sees it). DuckDB
   is killed too. A node killed by one query is the one outcome 1.0 shouldn't have: **fix** (L1), or refuse it by name
   as the median now is.
2. **The median over 3 groups is refused (53200) in 3 s**; DuckDB answers in 1.4 s using 1.1 GB. Refused by name, so
   the node lives, but it is an answer DuckDB gives: **state** for 1.0.
3. **The full sort to a file** spilled its 15.4 GB limit (`PONDRA_SPILL_GB`) after 62 s; DuckDB needed 10.9 GB on 10-16
   and ran out of this sandbox's 12 GB of free disk today. Pondra's spill is uncompressed (10-16 finding 7): **state**,
   compressing spill after 1.0.
4. **Windows over huge partitions answer but slowly:** a whole-partition `avg() OVER` 58.1 s vs DuckDB's 2.0 s, `rank()`
   over 3 partitions 43.7 s vs 36.1. **state**.

## Measured (2 GB cap; seconds / peak RSS MB / peak spill MB)

Pondra on a quiet machine (`pondra-55a774f-quiet.jsonl`); its lake loaded once under the cap. DuckDB 2.0.0 with
`memory_limit = '1.5GiB'`. The 10-16 columns are from another sandbox of the same size: compare within a column pair
with care.

| Query | Pondra today | Pondra 10-16 | DuckDB 2.0.0 today | DuckDB 10-16 |
|---|---|---|---|---|
| load lineitem, 60 M rows (CTAS from Parquet) | 21.3 / 1439 / 0 | 29.2 / 1428 / 0 | | |
| load orders, 15 M rows | 4.5 / 1212 / 0 | 6.9 / 1275 / 0 | | |
| TPC-H q1 (scan, 4 groups) | 1.1 / 293 / 0 | 2.6 / 879 / 0 | 2.9 / 549 / 0 | 1.2 / 572 / 0 |
| COPY 59 M rows to Parquet | 13.1 / 741 / 0 | 17.6 / 1304 / 0 | 12.4 / 482 / 0 | 18.3 / 441 / 0 |
| top 10 by a string | 0.9 / 1213 / 0 | 1.0 / 861 / 0 | 0.7 / 138 / 0 | 0.6 / 148 / 0 |
| GROUP BY 60 M integer groups | 10.7 / 869 / 1972 | 7.4 / 1036 / 1624 | 4.7 / 1755 / 948 | 6.7 / 1777 / 1003 |
| GROUP BY 34 M strings | 17.8 / 922 / 3153 | 16.6 / 877 / 2477 | 15.9 / 1496 / 4543 | 19.4 / 1522 / 4545 |
| count(DISTINCT string) | 17.1 / 925 / 2436 | 15.8 / 975 / 2079 | 11.7 / 1659 / 3155 | 14.6 / 1639 / 3076 |
| DISTINCT over 4 columns, 60 M rows | 24.2 / 997 / 4404 | 16.2 / 1057 / 3779 | 19.1 / 1482 / 5527 | 23.3 / 1547 / 5476 |
| join building 15 M rows | 7.5 / 1049 / 1708 | 6.2 / 1088 / 1506 | 3.1 / 1761 / 243 | 4.0 / 1782 / 246 |
| self-join building 60 M rows | 19.3 / 1206 / 3834 | 16.5 / 1130 / 3249 | 7.8 / 1953 / 1210 | 9.6 / 2029 / 1198 |
| three-way join, 1 M groups | 16.0 / 1217 / 2724 | 11.6 / 1264 / 2276 | 6.0 / 1764 / 640 | 6.6 / 1763 / 754 |
| EXCEPT | 12.1 / 1211 / 1614 | 14.7 / 1504 / 1432 | 4.8 / 1753 / 504 | 5.9 / 1766 / 487 |
| TPC-H q18 | 1.9 / 1041 / 377 | 2.7 / 1248 / 295 | 1.3 / 954 / 0 | 1.8 / 924 / 0 |
| full sort by a string, to a file | 53100 at 61.9 s, 15.4 GB spilled | 53200 at 19.6 s | out of disk at 59.8 s, 10.8 GB spilled | 78.2 / 1904 / 10866 |
| row_number() by order (15 M small partitions) | 10.4 / 1191 / 1046 | 14.1 / 1314 / 8688 | 2.2 / 1352 / 0 | 2.5 / 1378 / 0 |
| rank() over 3 partitions | **43.7 / 718 / 4380** | killed | 36.1 / 1856 / 4484 | 42.8 / 1850 / 4486 |
| avg() OVER (PARTITION BY 3 values) | **58.1 / 719 / 4671** | killed | 2.0 / 184 / 0 | 2.8 / 164 / 0 |
| count(*) OVER () | **1.0 / 602 / 0** | killed | 13.0 / 1754 / 633 | 16.6 / 1804 / 613 |
| median over 3 groups | **refused 53200** at 3.0 s | killed | 1.4 / 1097 / 0 | 1.6 / 1095 / 0 |
| string_agg over 3 groups | killed | killed | killed | killed |

## Slower than on 10-16?

**Yes: the queries that spill are 40-90% slower than on 10-16, and spill 15-30% more.** Same machine, same cap, both
lakes warm in the page cache, the two builds run in turn, twice each (`ab2-*.jsonl`):

| Query | 3be8783 (10-16) | 55a774f (today) | today, `PONDRA_HOT_GB=0` |
|---|---|---|---|
| GROUP BY 60 M integer groups | 10.7, 11.5 s; 1,624 MB spilled | 15.3, 14.9 s; 1,913-1,965 MB | 14.8, 14.6 s |
| GROUP BY 34 M strings | 25.7, 25.8 s; 2,476 MB | 37.7, 34.8 s; 3,215-3,273 MB | 35.1, 37.5 s |
| DISTINCT over 4 columns | 27.2, 28.9 s; 3,779 MB | 45.5, 47.0 s; 4,435-4,588 MB | 49.0, 44.2 s |
| join building 15 M rows | 7.9, 7.6 s; 1,506 MB | 13.1, 11.7 s; 1,701-1,831 MB | 10.2, 11.3 s |
| three-way join, 1 M groups | 17.0, 18.5 s; 2,248-2,277 MB | 35.1, 33.8 s; 2,538-2,584 MB | 36.3, 31.1 s |

The hot columns aren't the cause (off, nothing changes). The old build spills the same bytes every run and the new one
doesn't, so something since 10-16 changed what a query holds before it spills. Absolute times on this sandbox swing
with its disk (the same query ran 10.7 s in one session and 14.9 s in the next), so only the side-by-side pairs count.
**The cause: #150 (10-18), which caps how many spilled runs a sort merges at once.** Bisecting main's merges, each
step run beside the 10-16 build: good up to 0fa48fb (ratio 0.95-1.03), bad from #150 9d61562 on (1.42-1.6). In #150,
`store::spill_disk` sets DataFusion's merge fan-in to `memory_limit / partitions / 32 MB`, clamped to 4..64: under a
2 GB cap that is 5 runs, so a big sort merges in several passes and writes its rows to disk again each pass. Today's
main with `PONDRA_SPILL_FAN_IN=0` (DataFusion's own, no limit) spills exactly the 10-16 bytes and runs as fast:

| Query | 3be8783 (10-16) | main, fan-in 0 (two runs) | main, default fan-in (5) |
|---|---|---|---|
| GROUP BY 60 M integer groups | 11.6 s; 1,624 MB | 12.2, 13.5 s; 1,624 MB | 14.6 s; 1,942 MB |
| GROUP BY 34 M strings | 30.7 s; 2,471 MB | 29.2, 30.2 s; 2,471 MB | 40.5 s; 3,223 MB |
| three-way join, 1 M groups | 18.9 s; 2,340 MB | 21.4, 19.6 s; 2,373-2,439 MB | 31.1 s; 2,578 MB |

(DISTINCT over 4 columns ran out of this sandbox's disk in all four runs.) The cap was there so that a window beside a
spilling sort still gets memory (invariant 423). **L1 fix** before rc.1: a fan-in that merges in one pass whenever the
memory allows (the formula budgets 32 MB a run where a run holds one batch, about 4 MB), checked against 423's
windows-with-long-strings check and these queries.

## After the fix (fan-in per merge)

**Spilling GROUP BYs are back to one pass (10-16's spill bytes, to the MB), and every spilling query is faster than main: GROUP BYs 15-27%, joins 16-41%, a 3-partition
`rank()` 22%; most spill 5-30% less; answers identical.** The cause was #150's one global fan-in (5 runs under 2 GB). Now
a GROUP BY's or a join's merge reads up to 512 files over the partitions (4..64, `store::merge_files`): its memory is
bounded by its fair share of the pool first. Only the sorts feeding a window keep a memory-sized fan-in
(`store::merge_fan_in`: the query memory a partition has in 8 MB, 4..64; `store::window_sorts`), since their merge is
the one consumer the pool can't make spill, and the window beside it must still get memory (invariant 423).

Main at 1976feb (#318) against the same tree with the fix (and #322, which changes no query), both at their defaults,
run in turn on fresh copies of one lake, warm (`fix-main-1976feb.jsonl`, `fix-fanin.jsonl`; seconds / peak RSS MB / peak spill MB, the mean
where run twice):

| Query | main | the fix | |
|---|---|---|---|
| GROUP BY 60 M integer groups | 9.5 / 710 / 1911 | 8.1 / 708 / 1624 | −15% |
| GROUP BY 34 M strings | 23.0 / 777 / 3217 | 18.7 / 793 / 2477 | −19% |
| count(DISTINCT) of 34 M strings | 22.1 / 784 / 2514 | 16.1 / 823 / 2070 | −27% |
| three-table join, 1 M groups | 20.0 / 1172 / 2529 | 11.8 / 1222 / 2405 | −41% |
| join, 15 M-row build side | 10.2 / 1031 / 1837 | 6.2 / 1136 / 1506 | −39% |
| EXCEPT of 60 M rows | 17.2 / 1217 / 1544 | 14.5 / 1279 / 1439 | −16% |
| TPC-H q18 | 2.6 / 942 / 295 | 2.4 / 997 / 347 | |
| window by order | 11.6 / 1206 / 999 | 10.7 / 1220 / 702 | −8% |
| `rank()` over 3 partitions | 58.0 / 727 / 3883 | 45.1 / 836 / 3581 | −22% |
| top 10 by a string | 1.0 / 358 / 0 | 1.0 / 365 / 0 | |

Peak memory rises 0-15% (more runs open at once), all well under the cap. Invariant 423's window checks (`win423.py`,
the harness's two, at 256 MB; `win423-fan-in.jsonl`): the 3-partition window over long strings answers at fan-in 8
and 16 and at the fix's default (8 there), and fails at 0 (DataFusion's own: one merge of every run) and 32.

## string_agg and the memory watch, back to back

`string_agg(l_comment, ',')` over the 3 return flags (550 MB a group) killed the node in 0.9 s, right after the median
beside it was stopped (`pondra-55a774f.jsonl`). Two causes, both fixed with `src/string_agg.rs` and `src/oom.rs`:

- DataFusion's accumulator told the pool only its strings' lengths, while a string grown by doubling holds up to twice
  that and, as it doubles, three times. The pool is now told what the strings hold, and a group past an eighth of a
  partition's share of the query memory is refused by name (53200).
- A stop held the memory watch for ten seconds whatever happened next, so a heavy query started in those seconds was
  never stopped; and the stopped query's pages stayed resident for those seconds too. Now, while a stop holds, the
  freed pages go back to the system at each look, and once the node is under its line nothing holds.

`watch.py`, one node under a 2 GB cap, the queries one after another (seconds / peak RSS MB / answer):

| Query | string_agg capped, the old watch | Both fixes |
|---|---|---|
| string_agg, first 20 M rows | 4.8 / 1719 / 53200 (watch) | 2.7 / 1627 / 53200 by name |
| string_agg, after 3 s | 3.5 / 2162 / **killed** | 2.2 / 1755 / 53200 |
| string_agg, after 3 s | | 2.9 / 1745 / 53200 |
| median | | 4.5 / 1950 / 53200 |
| string_agg, at once | | 0.4 / 1792 / 53200 |
| median, at once | | 1.4 / 1962 / 53200 |
| string_agg, at once | | 3.0 / 2132 / 53200 |
| count(*) | | 0.01 / answered |

A group refused by its name needs it to pass its cap before its partial aggregate sends it on: over the whole table a
partial sends its strings on early, under the pool's pressure, so the watch's words answer instead. Either way it is
53200 and the node lives.

## How it was measured

Same scripts as 10-16 (`pbig.py`, `others.py`, `queries.json`), the cgroup path now read from `/proc/self/cgroup` (a
sandbox's cgroup is new each session). Machine: 4 cores, 15 GB, one disk with 12 GB free. Data: TPC-H SF10 Parquet
from tpchgen-cli (4 files per table). Pondra defaults (pool a third of the cap); `PONDRA_HOT_GB=0` changes nothing
under the cap (the load: 21.3 s either way).

A first Pondra run while a release build shared the machine took 163.5 s to load lineitem with 255,515 hits on the
cap; on the quiet machine it took 21.3 s. Those busy numbers (`pondra-55a774f.jsonl`) are kept but not quoted.

Results: this folder (JSON lines: Pondra busy and quiet, the load reruns, DuckDB 2.0.0).
