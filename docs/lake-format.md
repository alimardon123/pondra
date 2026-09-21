# The lake on disk: the native format, the optional open formats, and who can read and write it

A Pondra lake is one folder: a local directory (`--dir /data/lake`) or a bucket prefix
(`--dir s3://pondbucket/lake`). **Both use exactly the same layout.** Object keys in the bucket
are the same relative paths as the files on disk, and the catalog only stores relative paths. So
you can copy a lake from disk to a bucket (or back) with any sync tool, with its nodes stopped,
and it opens there as it was.

## Native first

Pondra's own format is the lake itself: Parquet files, the log tail, and the catalog that lists
them. It works wherever the binary runs: Linux, macOS or Windows, on a local or network
directory, AWS S3, Cloudflare R2, MinIO — any S3-compatible store with conditional writes.
Anyone with the binary and credentials for the bucket can use it in one of three ways:

| | How | Sees | Writes | Costs |
|---|---|---|---|---|
| **Join**: `pondra serve --dir …` (add `--reader` to only read) | The catalog in memory, kept current by the leader's commit stream; hot objects on the local SSD | Every committed write, milliseconds after the ack | Yes (`--reader`: no) | A running process |
| **Serverless**: `pondra sql --dir … "…"` | Opens the catalog in the bucket, reads the log tail and Parquet directly | Every write already in the bucket (every acknowledged write, in the default `--ack durable` mode) | `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`: this machine does the work. The leader records it: over HTTP, through the bucket inbox if it can't be reached (`inbox/`), or this process leads for a moment if nobody does | Opening the catalog: ~20 sequential requests (30 ms on local disk, 3–6 s on R2 from this sandbox) |
| **Other engines**, through Delta Lake or Iceberg | The table's `_delta_log/` or `metadata/`, if the table publishes it | The table as of the last tiering round | No | Nothing extra for Pondra, beyond the publishing itself |

**Several lakes, one bucket.** Each lake is its own prefix with its own leader. A node or a
`pondra sql` given `--attach sales=s3://bucket/sales` reads that lake's tables as `sales.orders`
next to its own (joins, views, `SHOW TABLES`). A write to `sales.orders` is recorded by the sales
lake's leader, never by ours. Nothing in either lake's layout changes: attaching is read-only
metadata access plus that leader's usual doors (HTTP, inbox).

Delta and Iceberg are **opt-in per table**: `"publish": ["delta", "iceberg"]` when creating it
(send it again to change it), or `--publish delta,iceberg` for every new table on a node.
Turning a format off deletes its metadata, so nobody reads a stale copy.

## Layout

```
<lake root>/
├── catalog/                    Pondra's catalog: a SlateDB key-value store
│   ├── manifest/               which SSTs make up the store right now
│   ├── wal/                    recent catalog commits (write-ahead log)
│   ├── compacted/              the catalog's sorted tables (.sst)
│   └── compactions/            compaction bookkeeping
├── cluster/
│   ├── term/0000000001         leader election: one small object per term (put-if-absent)
│   └── alive/0000000001        "the leader of this term is still here" (rewritten every 10 s)
├── inbox/                      writes from machines that can't reach the leader
│   ├── bell                    touched by every writer; the leader HEADs it once a second
│   ├── <id>.files | .flush | .table   a request: a bulk INSERT's files, a log flush, a new table
│   └── <id>.out                the leader's answer (deleted by the writer, or after an hour)
├── log/<ms>-<uuid>.seg         the log tail: flushes over 64 KB (1 MB with --ack replicated),
│                               Arrow IPC stream + ZSTD; smaller ones ride inside the catalog commit
└── data/<table>/               one folder per table (views and task outputs are tables too)
    ├── <uuid>.parquet          the table's rows (Parquet, ZSTD; keyed tables sorted by key, with
    │                           bloom filters on the key; `cluster_by` tables sorted by those columns)
    ├── _delta_log/             only if the table publishes Delta
    │   ├── 00000000000000000000.json … 00000000000000000010.checkpoint.parquet
    │   └── _last_checkpoint
    └── metadata/               only if the table publishes Iceberg (format v2)
        ├── v1.metadata.json …  one per version
        ├── snap-<N>-<uuid>.avro   manifest lists
        ├── <uuid>-m0.avro      manifests
        └── version-hint.text   the newest version
```

| Path | Format | Who reads it | Changes? |
|---|---|---|---|
| `catalog/` | SlateDB (LSM of SSTs + WAL). Keys: `t/` tables, `s/` log segments, `d/` small segments' data, `p/` producer progress (and bulk-insert jobs), `v/` views, `k/` tasks, `x/` Delta state, `i/` Iceberg state, `m` the followers whose copies count (replicated acks), `n` next segment, `c` commit number | Pondra | new objects only; old ones compacted away |
| `cluster/term/` | JSON: leader address and term (empty address: a `pondra sql` INSERT recording its files) | Pondra | one new object per election |
| `cluster/alive/` | empty; its timestamp is what counts | Pondra | rewritten every 10 s by the leader; a one-off writer deletes its own when done |
| `inbox/` | JSON requests (a flush as its binary body), JSON answers | the leader | each request deleted once answered; answers deleted by the writer (unclaimed ones after an hour) |
| `log/` | Arrow IPC stream + ZSTD, one per large flush (rows of any tables) | Pondra | immutable; deleted once tiered and older than `--retain-secs`, or `--changelog-secs` if longer (the change feed) |
| `data/<table>/*.parquet` | Parquet, ZSTD | anyone | immutable; replaced files deleted after `--retain-secs` |
| `data/<table>/_delta_log/` | Delta Lake protocol 1/2: JSON commits, Parquet checkpoints | Delta readers | append-only; `_last_checkpoint` rewritten; last 1,000 versions kept |
| `data/<table>/metadata/` | Iceberg v2: metadata JSON, Avro manifest lists and manifests | Iceberg readers | append-only; `version-hint.text` rewritten; last 100 snapshots kept |

Not in the lake, on each node:

- **The SSD tier** (lakes on object storage only): `<temp dir>/pondra-cache/<bucket>_<prefix>/`,
  or `--cache-dir`. It holds copies of immutable objects under their lake paths, plus the
  catalog's SST cache (`….catalog`). Delete it any time.
- **Replica files** (`--ack replicated`): `….replica/<node address>/`. They hold the leader's
  changes that aren't in the bucket yet, and are deleted as soon as they are.

## Where a row is, over time

1. **Acknowledged.** The row is in the log: in `log/<ms>-<uuid>.seg`, or inside the catalog commit
   for small flushes.
   - **Default (`--ack durable`):** the ack comes once the catalog commit is in the bucket (one
     object-store write).
   - **`--ack replicated`:** the ack comes once the leader and a follower hold the write (a
     follower keeps it in a local file). The bucket gets it a moment later.
   - **Who sees it:** nodes see it within milliseconds either way. `pondra sql` sees it once it
     is in the bucket.
2. **Tiered.** Once rows commit (at most every `--tier-secs`, default 2), they are folded into a
   Parquet file in `data/<table>/`. Tables that publish get a new Delta version and a new
   Iceberg snapshot, written only from state already in the bucket. Other engines see the row
   from here:
   - ~30 ms after the ack on local disk;
   - 3–4 s on a nearby R2 bucket, 7–10 s on a far one (a few sequential bucket round trips);
   - the full head-to-head table is in `comparison-spark-flink-fluss.md`.
3. **Merged / compacted.** Later the row moves into bigger files (below). Delta records the swap
   as `remove` + `add`; Iceberg writes a new snapshot without the old files. Old files are
   deleted after `--retain-secs`.

## Small files, compaction and indexes

The same on local disk and on object storage:

- **Small writes don't make small objects.** A node batches everything it received into one
  flush per round. Flushes up to 64 KB (1 MB with replicated acks) ride inside the catalog
  commit.
- **Tiering** writes one Parquet file per busy table per round.
- **Append tables:** once 8 files are small, they are merged 8 → 1 (up to 4 M rows / 64 MB). A
  table has at most ~8 small files plus big ones.
- **Keyed tables** (upsert, merge, GROUP BY views) are an LSM. Each round writes a file with the
  newest version of each key it saw. Once 8 files pile up, the newest run of similar-sized files
  is merged (size-tiered, round 9):
  - going back while each older file is at most twice the size of what's newer;
  - delete markers are kept, so the merged file still hides older versions.

  Only when the run reaches the oldest file is the whole table rewritten. That drops deleted
  rows and rows past the table's TTL. On a 2 M-key table with 60 rounds of updates this writes
  3x less than rewriting every time.
- **TTL** (`ttl = 'ts:86400'` on a keyed table): reads hide rows whose timestamp column is older
  than that; full compactions delete them.
- **Retention:** replaced files and consumed log objects are deleted after `--retain-secs`.
  Objects no commit ever referenced are deleted after a day.
- **Skipping data:**
  - Parquet min/max statistics per row group and page;
  - keyed tables sorted by key, with bloom filters and small row groups;
  - `"cluster_by": ["col"]` on append tables: every file sorted by those columns. Measured with
    8 M rows: one-user queries 6.4x faster, ranges 11x; full scans 0.6x
    (`docs/adr-009-native-first.md`).

Not there yet: partitions, clustering across files (Z-order / Hilbert), deletion vectors,
copy-on-write DELETE for append tables.

## Reading it with other engines

Only for tables that publish (see above). Point a Delta reader at `<lake root>/data/<table>`, or
an Iceberg reader at `<lake root>/data/<table>/metadata/v<N>.metadata.json` (N is in
`version-hint.text`). `tools/open_check.py` and `tools/demo_lake.py` compare six readers against
Pondra's own SQL:

```python
# Delta: delta-rs and Polars
import deltalake, polars as pl
opts = {"AWS_ENDPOINT_URL": "https://<account>.r2.cloudflarestorage.com", "AWS_REGION": "auto",
        "AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..."}
deltalake.DeltaTable("s3://pondbucket/lake/data/events", storage_options=opts).to_pyarrow_table()
pl.read_delta("s3://pondbucket/lake/data/events", storage_options=opts)

# Iceberg: PyIceberg (on R2, use the fsspec file IO: pip install s3fs) and Polars
from pyiceberg.table import StaticTable
meta = "s3://pondbucket/lake/data/events/metadata/v42.metadata.json"
StaticTable.from_metadata(meta, properties={"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    "s3.endpoint": "https://<account>.r2.cloudflarestorage.com", "s3.region": "auto",
    "s3.access-key-id": "...", "s3.secret-access-key": "..."}).scan().to_arrow()
pl.scan_iceberg(meta, storage_options=opts).collect()
```

```sql
-- DuckDB (delta, iceberg, avro and httpfs extensions)
CREATE SECRET (TYPE s3, KEY_ID '...', SECRET '...', ENDPOINT '<account>.r2.cloudflarestorage.com',
               REGION 'auto', URL_STYLE 'path');
SELECT user, sum(amount) FROM delta_scan('s3://pondbucket/lake/data/events') GROUP BY user;
SELECT count(*) FROM iceberg_scan('s3://pondbucket/lake/data/events/metadata/v42.metadata.json');
```

Spark, Databricks, Trino and Athena read the same folders through their Delta or Iceberg
connectors (register the table's location or metadata file). They weren't tested here: their
connector jars can't be downloaded in this sandbox.

Rules for outside readers:

- **Read-only.** Pondra owns the folder; write through Pondra (any node, or `pondra sql`).
- **Latest version, plus a little history:** 1,000 Delta versions and 100 Iceberg snapshots are
  listed. Files replaced by compaction are deleted after `--retain-secs` (60 s by default), so
  time travel further back than that fails.
- **Types:** Int32/64, Float32/64, Utf8, Boolean, Date32, Binary and Decimal128 in both formats;
  Int8/16 in Delta only. A table with any other column type isn't published in that format (yet).
- **Keyed tables** are published when one file holds one row per key: after each compaction.
- **No partitions** yet: engines prune with Parquet statistics and, for keyed and clustered
  tables, bloom filters and sort order.

Plain Parquet readers can also read `data/<table>/*.parquet` directly. They see every file,
including replaced ones in their grace period and, for keyed tables, older versions of a key.
Only the catalog, the Delta log or the Iceberg metadata gives the right answer.
