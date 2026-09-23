# ADR-013: One node first, and anything in a column: files, vectors, models — and publishing that costs what changed

**Status:** Accepted, built and tested (round 12) · **Date:** 2026-09-23 · **Builds on:** ADR-009, ADR-011, ADR-012

## Context

Round 11 made table metadata small enough for petabytes and spread queries across nodes. Two
questions were left, and both are about being taken seriously:

1. **Is one node actually fast?** A distributed engine that is slow on one machine is a slow
   engine with extra machines. DuckDB, Polars, Daft and Bodo all run TPC-H on one box; until
   Pondra beats them there, "scales out" means little. At the end of round 11 Pondra ran TPC-H
   SF1 in 6.45 s against DuckDB's 4.18 s on the same files: 54% slower.
2. **Can a column hold something other than a number?** Every lakehouse is growing towards
   unstructured data: Hudi added blob and variant types, Databricks added file types and
   `ai_query`, Daft is built around multimodal columns. A warehouse that can only hold numbers
   and strings is now a partial warehouse.

And ADR-012 left a list of what still stood between Pondra and petabytes. The first item on it —
publishing a big table to Delta and Iceberg — turned out to be the expensive one: every publish
wrote every file's path.

## Decisions

### 1. One node first

Nothing here is a special case for a benchmark; each is a general rule that happens to show up
in TPC-H.

- **Strings are read as views** (`query::read_schema`). A table's columns are stored as `Utf8`
  but read as `Utf8View`: Parquet decodes into views without copying the bytes, and DataFusion's
  string filters, joins and grouping take their fast paths. Rows going the other way (the log,
  writes, Delta/Iceberg schemas) stay `Utf8`, so nothing outside the read path changes.
- **LZ4 is the default Parquet codec** (`PONDRA_CODEC`, `tier::codec`). ZSTD(1) files are a
  third smaller but decode much more slowly: TPC-H runs 13-20% faster on LZ4. `PONDRA_CODEC=zstd`
  where storage or bandwidth costs more than CPU; `snappy` and `none` are there too.
- **Engine settings Pondra starts from** (`optimize::config`): `0.06 + 0.01` is the exact decimal
  0.07 as the SQL standard (and DuckDB, and Postgres) has it, not a float a hair below — TPC-H q6
  gave the wrong answer before; and a join whose smaller side is under 32 MB builds one shared
  hash table instead of shuffling both sides by key.
- **Three planning rules of our own** (`optimize.rs`), each a page of code:
  - `semi_join_down`: `x IN (SELECT k … GROUP BY k HAVING …)` filters the one table `x` comes
    from, so it runs on that table, before the joins — as a WHERE filter would. TPC-H q18 joins
    57 orders instead of 6 million lineitems.
  - `group_only_joined`: a join with a grouped subquery keeps only the groups whose key the other
    side has; when that key comes from a filtered table (200 parts of one brand), the subquery
    groups only those keys' rows. TPC-H q17 groups 6 thousand lineitems, not 6 million.
  - `cheap_first`: a filter's conditions run cheapest first, because `AND` looks at its right side
    only for the rows its left side kept. Date and number comparisons go before string matching
    and functions, as DuckDB's expression heuristics do.
  - and one physical rule, `having_builds`: the few groups a `HAVING` keeps make the hash table,
    the table probes it. (Statistics can't tell how many groups a HAVING keeps, and DataFusion's
    guess — a fifth of the input — has it build on the table instead.)
- **Hot columns** (`hot.rs`). The columns queries read lately are kept decoded, in memory, per
  file: a scan takes a file's columns from there when they are all there, and skips reading and
  decoding Parquet entirely. Files never change, so nothing goes stale — no invalidation, ever.
  It fills in the background, one file at a time, within `PONDRA_HOT_GB` (a quarter of the query
  memory by default; 0 turns it off), and only for a file a second scan came back to: data read
  once — a backfill, a consumer reading the log through, a one-off report — passes through without
  costing the CPU that decoding it would. When full it makes room only by dropping columns nobody
  read for a minute, so a working set bigger than memory is read from Parquet as before rather
  than churned through the cache. `pondra_hot_bytes` says how much it holds.

This is DuckDB's own trick — its native tables live in its buffer pool — so the benchmark reports
both ways: from files, and from memory.

### 2. Anything in a column

- **Files in the lake** (`files.rs`). `PUT /files/<path>` puts an object next to the tables,
  `GET /files/<path>` reads it back, `SELECT * FROM files('photos/')` lists what's there (path,
  size, written), and `file_read(path)` reads one object's bytes where a query needs them, eight
  at a time. The bytes stay in the bucket; a column holds the path. This is what Databricks'
  file type and Hudi's blobs are for: the table row references the object, and a query pulls
  only the rows it reads.
- **Bytes are a column type.** `BINARY` columns work through the log, Parquet, Delta and Iceberg;
  `byte_length`, `sha256`, `md5`, `encode(…, 'base64')`, `decode`, `substr` work on them.
- **`VARIANT`** is a column type: semi-structured text, stored as a string, read with
  `json_get(…)`, `->` and `->>`. When Arrow and DataFusion carry a real variant type, the column
  type is already there to change underneath.
- **Vectors.** `Float32[]` (any `T[]`) is a column type, published as a Delta `array` and an
  Iceberg `list` — delta-rs, DuckDB, Polars and PyIceberg all read them back — with
  `cosine_similarity`, `l2_distance` and `dot_product` for the "most like this" queries.
- **Models, in SQL** (`ai.rs`). `ai_complete(prompt)` and `ai_embed(text)` call an
  OpenAI-compatible endpoint (`PONDRA_AI_URL`: your own vLLM or Ollama, or a hosted one), eight
  rows in flight, a failed row null. Pondra carries no model: the binary stays small and the GPU
  stays outside it.
- **Functions you bring yourself** (`udf.rs`). `POST /functions/caption {"flight":
  "http://host:port", "args": ["Binary"], "returns": "Utf8"}` registers an Arrow Flight server as
  a function: Pondra sends it the rows it has as one Arrow batch and reads back one column. The
  heavy part — a model, a tokenizer, a GPU, any Python library — runs in that process, so a slow
  model can't take a node down and the binary carries none of it. `tools/udf_server.py` is such a
  server in forty lines.

```sql
SELECT path, caption(file_read(path)) AS caption, ai_embed(caption) AS v
FROM files('photos/') WHERE size < 1000000;
```

### 3. Publishing that costs what changed (ADR-012's first item)

A published table's Delta and Iceberg metadata used to be derived from every file it holds: the
catalog kept a map of every path, and every Iceberg snapshot rewrote one manifest naming every
file. At a million files that is a 100 MB catalog entry rewritten every tiering round, and a
million-entry Avro manifest written every round.

Now both are derived from Pondra's own manifests, which are immutable:

- **Delta**: the state keeps which manifests are in the log (path → file count) and the ≤128
  inline files. A round reads only the manifests that came or went, and its commit carries their
  files. Checkpoints stream a manifest at a time through a blocking writer, so a million paths
  are never in memory at once.
- **Iceberg**: one Iceberg manifest per Pondra manifest, written once and named by every later
  snapshot; only the inline files' manifest is rewritten. A snapshot of a million-file table
  costs one manifest, not a million entries. Manifests no longer named are deleted once no kept
  snapshot names them.

Measured on a table of 200,002 files (195 TB of file entries): publish state 7.7 KB (Delta) and
22.7 KB (Iceberg), a publish round 12 ms. On 20,000 files: the first publish (20,001 adds) 0.33 s,
the next 1 ms, and a publish after one more file arrives 2 ms.

### 4. Memory that holds

Making a node fast at SF10 found the other end of the problem: a node could take itself down.
Three things were true and now aren't.

- **Maintenance ran every merge job at once.** A table of sixty 37 MB files produced seven merge
  jobs, each holding its rows and the Parquet file it was writing, and the leader started them
  all together. Now a node runs a couple at a time (`run_job` takes a slot), and a merge takes at
  most 256 MB of input per job.
- **The decoded columns were on top of the query budget.** They are now inside it: a load only
  takes what queries are not using, and a watcher gives memory back — dropping the least recently
  read columns — when the process passes three fifths of the machine's memory. The default budget
  is a quarter of the query memory (an eighth of RAM).
- **The query budget itself was half of RAM.** What DataFusion counts is the big hash tables and
  sort buffers, not the Parquet decoding or the batches in flight, so the node's own memory runs
  ahead of the number. A third of RAM leaves room for the rest.

A fourth, on slow storage: two `pondra sql` INSERTs that both found the lake idle could fence each
other out of the catalog, and the loser reported the error instead of starting over. It now
starts over with the same job (at most five times), so one of them records the write and the rest
go through it.

### 5. Two more for the petabyte road

- **Orphan collection in a few megabytes** (`tier::collect_orphans`). What a crashed writer left
  behind is found with a Bloom filter of the paths in use — ten bits each, so a million-file
  table costs about a megabyte — instead of holding every path in a set. A path the filter
  doesn't have is certainly unused; the one in a hundred it keeps by accident simply survives to
  the next round.
- **A node that is stopped hands over at once.** On Ctrl-C or SIGTERM (how a scheduler scales a
  deployment down) the leader releases its mark in the bucket, so the next node leads
  immediately instead of waiting out the lease. Acknowledged writes are already durable.

## What it measures

TPC-H on one 2-vCPU, 8 GB machine, best of three runs, every answer checked against DuckDB's
(`tools/bench/singlenode.py`; `logs/round12/`). Each engine runs its own published TPC-H code and
reads the same Parquet files; Pondra reads its own lake, loaded with one INSERT per table.

| From Parquet, every query | SF1 | SF10 | | From memory | SF1 | SF10 |
|---|---|---|---|---|---|---|
| **Pondra** (`PONDRA_HOT_GB=0`) | **3.19 s** | **38.0 s** | | **Pondra**, hot columns | **1.96 s** | **35.9 s** |
| DuckDB 1.5.5 | 3.36 s | 39.8 s | | DuckDB, native tables | 1.80 s | no room on this machine |
| Polars 1.44.2 | 3.78 s | q9 out of memory | | | | |
| Polars, streaming | 3.18 s | 42.8 s | | | | |
| Daft | 6.11 s | 89.0 s | | | | |
| Bodo | 8 of 21 answers differ, one crash, minutes a query | — | | | | |

Round 11 ran SF1 in 6.45 s (DuckDB 4.18 s on the same files and machine). Per rule, on SF1:
strings as views ≈ −8%, LZ4 instead of ZSTD ≈ −13%, the three planning rules ≈ −15% together
(q18 0.88 → 0.16 s, q17 0.40 → 0.09 s, q12 0.30 → 0.09 s), and the hot columns ≈ −40% on top.

The metadata numbers, on a table given a million files (1 PB on paper), published as Delta and
Iceberg: a 20 KB catalog entry, a 24 KB Delta state, a 70 KB Iceberg state, a 17 ms publish
round, 25 ms tiering commits, 5.6 ms INSERTs, and a query over today that skips the million
files in 10 ms (`tools/metadata_bench.py --files 1000000 --publish`).

The binary is 95.9 MB (32.4 MB gzipped), 39 MB idle with the Kafka, Flight and Postgres ports
open — 0.9 MB more than round 11 for hashing, base64, files, vectors and the AI and Flight
functions.

## What this costs

- The hot columns are memory: a quarter of the query budget by default, out of it rather than on
  top of it, and given back when the node's own memory runs high. `PONDRA_HOT_GB=0` turns them
  off, and the numbers above are reported both ways.
- LZ4 files are about a third bigger than ZSTD(1) ones. `PONDRA_CODEC=zstd` is one flag away.
- `VARIANT` is JSON text, not a shredded variant: `json_get` parses at read time. A shredded
  variant needs Arrow to have the type.
- The three planning rules are heuristics. Each fires only on a shape where the alternative is
  clearly worse, and every TPC-H answer is still checked against DuckDB's.

## What is still open

- **Cost-based join reordering.** DataFusion joins in the order the query names, with swaps by
  size at the physical level. TPC-H q7 is the one query where this still shows.
- **Sorted data.** Files written in key order aren't declared as sorted, so an aggregation on
  that key hashes instead of streaming.
- From ADR-012's list: shuffle buckets that spill, retrying a failed step elsewhere, Kafka
  partitions, and merging files inside sealed manifests.
