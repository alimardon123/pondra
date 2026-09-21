# The lake on disk: layout, formats, and reading it without Pondra

A Pondra lake is one folder: a local directory (`--dir /data/lake`) or a bucket prefix
(`--dir s3://pondbucket/lake`). **Both use exactly the same layout**. Object keys in the bucket
are the same relative paths as the files on disk, and the catalog only stores relative paths. So
a lake copied from disk to a bucket (or back) with any sync tool, with its nodes stopped, opens
there as it was.

## Layout

```
<lake root>/
├── catalog/                    Pondra's catalog: a SlateDB key-value store (Pondra-only)
│   ├── manifest/               which SSTs make up the store right now
│   ├── wal/                    recent catalog commits (write-ahead log)
│   ├── compacted/              the catalog's sorted tables (.sst)
│   └── compactions/            compaction bookkeeping
├── cluster/term/0000000001     leader election: one small object per term (put-if-absent)
├── log/<ms>-<uuid>.seg         the log tail: flushes over 64 KB (Arrow IPC stream, ZSTD);
│                               smaller flushes are stored inside the catalog commit instead
└── data/<table>/               one folder per table (views and task outputs are tables too)
    ├── <uuid>.parquet          the table's rows (Parquet, ZSTD; keyed tables sorted by key,
    │                           with bloom filters on the key columns)
    └── _delta_log/             the Delta Lake log of that folder: what other engines read
        ├── 00000000000000000000.json
        ├── …
        ├── 00000000000000000010.checkpoint.parquet
        └── _last_checkpoint
```

What each part is for:

| Path | Format | Who reads it | Changes? |
|---|---|---|---|
| `catalog/` | SlateDB (LSM of SSTs + WAL). Keys: `t/` tables, `s/` log segments, `d/` small segments' data, `p/` producer progress, `v/` views, `k/` tasks, `x/` Delta publish state, `n` next segment, `c` commit number | Pondra only | new objects only; old ones compacted away |
| `cluster/term/` | JSON: leader address and term | Pondra only | one new object per election |
| `log/` | Arrow IPC stream + ZSTD, one per large flush (rows of any tables) | Pondra only | immutable; deleted once tiered and older than `--retain-secs` |
| `data/<table>/*.parquet` | Parquet, ZSTD | anyone | immutable; replaced files deleted after `--retain-secs` |
| `data/<table>/_delta_log/` | Delta Lake protocol 1/2: JSON commits, Parquet checkpoints | anyone with a Delta reader | append-only; `_last_checkpoint` is rewritten; last 1,000 versions kept |

Not in the lake: each node's **SSD tier** (lakes on object storage only) lives outside it, in
`<temp dir>/pondra-cache/<bucket>_<prefix>/` (or `--cache-dir`). It mirrors the lake's relative
paths (`log/…`, `data/<table>/…`), holds copies of immutable objects only, and can be deleted at
any time.

## Where a row is, over time

1. **Acknowledged:** in the log. Either in `log/<ms>-<uuid>.seg`, or inside the catalog commit
   for small flushes. Pondra queries see it from here, on every node, within milliseconds.
2. **Tiered** (as soon as rows commit, at most every 2 s: `--tier-secs`): folded into a Parquet
   file in `data/<table>/`, and the table's Delta log gets a new version. Other engines see it
   from here: 17 ms after the ack on local disk, ~5 s on R2 from far away. That's three
   storage round trips: the Parquet file, the catalog commit and the Delta commit.
3. **Merged / compacted** later into bigger files. The Delta log records the swap: `remove` the
   old files, `add` the new one. The old files are deleted after `--retain-secs`.

So what another engine reads is **the table as of the last tiering round**. For append tables
that is at most `--tier-secs` plus one round behind Pondra under a steady stream of writes, and
one round after a quiet spell. Keyed tables (upsert, merge, GROUP BY views) are published when a
single file holds exactly one row per key, after each compaction, at most 8 tiering rounds apart.
The log tail itself is Pondra-only.

## Reading it without Pondra

Point any Delta Lake reader at `<lake root>/data/<table>`. These are the three readers the tests
compare against Pondra's own SQL (`tools/delta_check.py`, `tools/demo_lake.py`):

```python
# delta-rs
import deltalake
t = deltalake.DeltaTable("s3://pondbucket/pondra-demo/data/events", storage_options={
    "AWS_ENDPOINT_URL": "https://<account>.r2.cloudflarestorage.com", "AWS_REGION": "auto",
    "AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..."})
print(t.to_pyarrow_table().num_rows)

# Polars
import polars as pl
df = pl.read_delta("s3://pondbucket/pondra-demo/data/events", storage_options={...same...})
```

```sql
-- DuckDB (delta + httpfs extensions)
CREATE SECRET (TYPE s3, KEY_ID '...', SECRET '...', ENDPOINT '<account>.r2.cloudflarestorage.com',
               REGION 'auto', URL_STYLE 'path');
SELECT user, sum(amount) FROM delta_scan('s3://pondbucket/pondra-demo/data/events') GROUP BY user;
```

```python
# Spark / Databricks (delta-spark)
spark.read.format("delta").load("s3a://pondbucket/pondra-demo/data/events")
```

Trino, Athena and other engines with a Delta connector work the same way: register
`<lake root>/data/<table>` as an external Delta table.

Rules for outside readers:

- **Read-only.** Pondra owns the folder; an outside writer would be overwritten or ignored.
- **Latest version, plus a little history.** The last 1,000 Delta versions are listed, but files
  replaced by compaction are deleted after `--retain-secs` (60 s by default). Time travel further
  back than that fails.
- **Types:** Int8/16/32/64, Float32/64, Utf8, Boolean, Date32 and Binary map to Delta types. A
  table with any other column type is not published (yet).
- **No partitions** yet: engines prune by Parquet statistics and, for keyed tables, bloom filters
  and sort order.

Plain Parquet readers can also read `data/<table>/*.parquet` directly. They see every file,
including replaced ones in their grace period and, for keyed tables, older versions of a key, so
only the Delta log gives the right answer.
