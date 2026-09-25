# ADR-018: Install anywhere — one line in a notebook, a shell, and sums that add up the same

**Status:** Accepted, built and tested (round 17) · **Date:** 2026-09-27 · **Builds on:** ADR-010 (runs anywhere), `docs/roadmap.md` (track A, item D3)

## Context

Round 16's binary did everything a streamhouse should, but only for someone who could build it:

- **It needed glibc 2.38.** It was built on this sandbox's Ubuntu 24.04, so it refused to start
  on Ubuntu 22.04, which most cloud notebooks and CI images still run, and on anything older.
- **There was nothing to install.** No pip or npm package; the Python client was a folder to
  `pip install ./python`, and it needed a node started by hand.
- **No shell.** DuckDB's first five minutes are `duckdb`, then SQL. Pondra's were `serve`, then
  curl or a Python client.
- **Two tests failed now and then.** A Kafka consumer group check (librdkafka looping on an
  offset from before the oldest segment), and TPC-H q15 on one node, whose answer differed from
  DuckDB's in some runs.

The roadmap (2026-09-25) made this round 17, learning from DuckDB and PGlite: install in one
line, start in one line, a lake in the current folder by default.

## Decisions

### 1. A Linux binary for glibc 2.17

`cargo zigbuild --profile dist --target x86_64-unknown-linux-gnu.2.17` links against glibc 2.17's
symbols, with zig as the linker. The binary asks for nothing newer (`objdump -T`: `GLIBC_2.17`
at most), so it runs on any Linux from 2014 on. That is also the floor of Python's own
manylinux2014 wheels, so pip accepts the wheel wherever it installs binary wheels at all.

- **Not musl.** A static musl binary would also run on Alpine. But glibc 2.17 already reaches
  every notebook, CI image and server we know of, and keeps the allocator and memory routines
  the benchmarks were measured with.
- **Same speed.** TPC-H SF1 with the portable binary: see "What it measures".

### 2. `pip install pondra`: the binary in the wheel

A wheel per platform holds the pure-Python client (`pondra/__init__.py`) and the binary as a
script (`pondra-<v>.data/scripts/pondra`). pip installs it next to Python as the `pondra`
command. That's how maturin's "bin" wheels are laid out; `tools/package.py` writes it directly
(zip, METADATA, WHEEL, RECORD, the executable bit), since the binary is built once per platform
anyway.

- `pondra.local("lake")` starts a node on a folder or bucket, on a free port, in the background,
  and returns a client. It stops when Python exits, or on `close()`.
- `pondra.binary()` finds the executable: `$PONDRA_BIN`, pip's scripts folders (user installs
  too), then `PATH`.
- `db.view(name, sql, **options)` creates a view. Asking again for the same view changes
  nothing, so a notebook cell can run twice; asking with other SQL or options is refused.
- `examples/quickstart.ipynb` covers tables, a view, appends, pandas, the change feed and an
  as-of join, starting from its own `%pip install` cell.

### 3. `npm install pondra`: esbuild's pattern

`pondra` holds the JavaScript client and lists `pondra-linux-x64`, `pondra-linux-arm64`,
`pondra-macos-x64`, `pondra-macos-arm64` and `pondra-windows-x64` as optional dependencies. Each
of those holds one binary and declares its `os` and `cpu`, so npm installs only the one that
fits. The client (`js/index.js`, 130 lines) has the Python client's calls: `local`, `connect`,
`sql`, `append`, `view`, `lookup`, `watch`, `close`.

### 4. A shell: `pondra` or `pondra <lake>`

With no command, the binary opens a SQL shell on `./lake` (or the folder or `s3://` URL given).
It starts a node on the lake — the same binary, in the background — and sends each statement to
it. So views, tasks and windows run while the shell is open, other nodes can join, and the lake
is left as any node leaves it.

- A statement ends at a `;` outside strings, quoted names and comments. At the end of piped
  input, the last one runs even without its `;`.
- `.tables`, `.quit`; answers as tables. On a terminal it also prints prompts and timings.
- An error is printed and the session goes on. If the node itself stops, the shell says why
  (its log) and ends.

### 5. A node stops when whoever started it does (`--stop-with-stdin`)

A node that the shell, Python or Node.js starts gets a pipe as its standard input. When the pipe
closes — the parent called `close()`, exited, or was killed outright — the node stops as it does
on Ctrl-C: a leader gives up its term at once (`cluster::release`), so the next process on the
lake leads immediately, instead of waiting out the lease a killed leader leaves.

- It works the same on Linux, macOS and Windows. The other ways of noticing a dead parent are
  platform-specific (Linux's `PR_SET_PDEATHSIG`, Windows job objects).
- A follower started this way just exits; acknowledged writes are already durable, or held by
  the other replicas.

### 6. Defaults for small machines

- **The SSD tier sizes itself to the disk.** A quarter of the free space, at most 20 GB, unless
  `--cache-gb` says otherwise. Notebooks often have 10–30 GB free, so the old fixed 20 GB could
  fill them.
- **No CA certificates, no crash.** Minimal images (Ubuntu's docker image among them) have no
  `ca-certificates`, and the HTTP client panicked at startup looking for them. Now the node
  prints that HTTPS calls out will fail and why, and runs; local lakes never needed TLS.
- **A proxy in the environment doesn't come between a client and its own node.** Many
  companies' notebooks set `HTTP_PROXY`, and Python's and Rust's HTTP clients then send even
  `http://127.0.0.1:…` through the proxy, which can't reach it. On Ubuntu 22.04 with such a
  proxy, `pondra.local()` failed and the shell waited for its node until it gave up. Now both
  reach a node on this machine directly. `anywhere_check.py` points the proxy variables at a
  dead port to check it. The node's own calls out (models, other nodes) still use the proxy.

### 7. The two flaky tests, fixed

- **Kafka.** A fetch from an offset before the oldest segment still kept now reads from that
  segment, as Kafka reads from its earliest after retention. Answering "out of range" made
  librdkafka retry the earliest offset it had cached from before those segments expired, in a
  loop. `harness.py kafka` has a check for it ("an offset before the oldest segment reads from
  there"), which fails on a binary without the fix.
- **TPC-H q15: `sum` over DOUBLE now gives the same answer in any order** (`fsum.rs`). q15
  compares each supplier's revenue with the max of the same revenues. DataFusion adds a group's
  values as they arrive, and each partition's partial sums in the order the partitions finish,
  so the two computations of one supplier's revenue could differ in the last bit and the top
  supplier didn't match itself.
  - **How.** Pondra's `sum` carries each addition's rounding error in a second double (the
    two-sum trick) and adds it back at the end, for rows and for partial sums alike. The result
    is the true sum rounded once, whatever the order and however many nodes, short of a sum
    within about n·2⁻¹⁰⁶ of a rounding tie.
  - **Everything else is as before.** Integers, decimals and `sum(DISTINCT …)` use DataFusion's
    own `sum`. So do the planner's rewrites and statistics shortcuts, because Pondra's `sum`
    passes them through. Sliding windows take values back out through the same pair.
  - **Tests.** `harness.py sums` checks sums equal to Python's `math.fsum` (whole, per group, in
    windows, on each of three nodes, three times each), `[1e16, 1, -1e16]` adding up to 1 (it
    was 0), and NULLs, overflow and types as before. Every check fails on the binary before.
    `tools/bench/repeat.py` runs one TPC-H query many times against DuckDB's answer.

### 8. No "lite" build (A5, measured and rejected)

Cargo features could leave out the Kafka, Flight, Postgres, MCP and AI front doors. We sized the
release binary's code by crate, from its symbols (about 103 MB of code, before stripping):

| Where the bytes are | MB |
|---|---|
| Generic code instantiated from the standard library (`core`, `alloc`: vectors, iterators, drops of every type) | 23.4 |
| `sqlparser` (DataFusion's SQL parser: the AST, its visitors, every dialect) | 16.8 |
| `arrow_array` | 8.3 |
| DataFusion's own crates | 16.6 |
| Pondra itself (all 13,200 lines) | 2.8 |
| The front doors' libraries: pgwire 0.09, arrow-flight 0.17, tonic 0.13, prost 0.04 | 0.4 |

A build without every front door would be about 1–2% smaller. The size is the SQL engine's, and
DuckDB's is its engine's too. The portable binary is 97 MB, and 33 MB compressed in the wheel.

### 9. A release workflow for every platform

`.github/workflows/release.yml`, on a `v*` tag or started by hand:

- **Builds** Linux x86-64 and ARM (zig, glibc 2.17), macOS Intel and Apple, and Windows.
- **Packages** each platform for pip and npm (`tools/package.py`) and tries the wheel and the npm
  package on that platform (`package_check.py`, `package_check.mjs`). The Linux wheel is also
  tried in CentOS 7 and Ubuntu 22.04 containers.
- **Publishes** to a GitHub release, PyPI (trusted publishing: no token) and npm (`NPM_TOKEN`).

It hasn't run: the sandbox can't push, and publishing needs the package names reserved and, for
free runners of any size, the owner's call on the repository.

## What it measures

One 2-vCPU box (`logs/round17/`).

| | |
|---|---|
| The portable binary | 97 MB; glibc 2.17 at most; the wheel 33 MB, the npm platform package 33 MB |
| Runs on | CentOS 7 (glibc 2.17, the manylinux2014 image) and Ubuntu 22.04 (its minimal image: no Python, no CA certificates): a shell session on a new lake. The wheel and its client on CentOS 7 (Python 3.11), and on Ubuntu 22.04 with Python 3.10 from apt, installed as root, behind a proxy named in `http_proxy` |
| A shell, first time on a new lake: start, create, insert, query, stop | 0.14–0.44 s |
| A second shell on the same lake, writing (the first handed the lake on) | 0.10–0.14 s |
| Python killed with `kill -9`: its node stops, and the lake reopens for writes | node gone at once; reopened and written in 0.21–0.24 s |
| `pip install` of the wheel in a fresh virtualenv; `npm install` in a fresh project; the quick-start notebook from its `%pip install` cell | all work; the notebook runs top to bottom in ~12 s |
| TPC-H q15, 20 runs on one node, against DuckDB's answer | before: 8 of 20 wrong from Parquet (2 of 20 in an earlier run); now 0 of 20 from Parquet and 0 of 20 from memory |
| TPC-H SF1, one node, from memory / from Parquet (best of 3) | native build 1.79–1.89 s / 2.79–2.98 s; the portable build 1.78 s / 2.83 s; this round's code without the exact sum 1.78–1.88 s / 2.95 s; DuckDB 2.99 s |
| The shell on a lake in a bucket: a session (create, insert, query), then a second shell (insert, query) | 12 s on the R2 simulator, 15 s on real R2, both answers right (mostly opening the catalog, twice) |
| Everything else (`harness.py all` with the new `sums`, asof and stream checks, 22 TPC-H queries on 3 nodes, users, failover, open formats, skew, spread) | all pass, with the native build and with the portable one; on the R2 simulator: sums, windows, sessions, Kafka, as-of, stream, scale, failover and users with replicated acks, crash, open formats; on real R2: sums, windows, Kafka, stream (windows out 1.2 s and sessions 1.8 s after the closing click), scale, open formats, failover |

## What this costs

- **Exact sums cost a few percent where sums dominate.** Each value is one addition and about
  five more floating-point operations, and a partial sum is two values. TPC-H SF1's totals
  didn't move beyond run-to-run noise. q18, a sum over 1.5 M groups whose partial sums arrive
  row by row, went from 0.13–0.14 s to 0.15–0.19 s. Skipping zero error terms when merging
  partial sums took the first version's cost (4–5% on the totals) down to that.
- **A node per notebook or shell.** `local()` and the shell run a separate process that uses
  memory of its own (tens of MB idle) and a port on 127.0.0.1. The in-process module (roadmap
  round 20) removes that for notebooks.
- **The shell has no line editing of its own.** On a terminal it gets the terminal's, which is
  basic; there is no history across sessions yet.

## What is still open

- **Publishing.** The package names on PyPI, npm and crates.io, and the repository (public, or a
  public bench repo), are the owner's decisions. Until then, `tools/package.py` builds the
  packages from any binary.
- **Windows and macOS haven't run on real machines.** The release workflow builds and tries them
  on GitHub's runners the first time it runs.
- **Other float aggregates.** `avg`, `stddev`, `var` and `corr` over DOUBLE still add in
  arrival order.
- **Alpine (musl) and 32-bit ARM** aren't built.
- **A write whose PUT fails in flight fails.** On real R2, one Parquet PUT of `harness.py
  scale`'s INSERTs failed with "error sending request", and the INSERT returned the error; the
  run passed again. Pondra's writes are create-only (put-if-absent), so the object store's
  client doesn't retry one that may already have landed. A client retrying the statement with
  its job id is safe; retrying inside, after checking what landed, would be kinder.
