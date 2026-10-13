# ADR-061: Pondra in your process (`pondra.open`)

**Date:** 2026-10-11 · **Status:** proposed (step 1 approved by the main thread, 2026-10-11) · **Builds on:** ADR-018 (installed anywhere), ADR-024
(`local()`, the shell), ADR-027 (Python beside the node, never inside it) · **Thread:** In-process and
browser, round 35 (roadmap B1, B2)

## Context

DuckDB and Polars run inside the program that uses them: `duckdb.connect()` returns at once and an
answer is Arrow in the same memory. Pondra's `local()` in Python and JavaScript starts `pondra serve`
as a child process and talks HTTP to it. That works, but:

- every notebook and test pays a process start, a port and a second copy of the engine's pages;
- the wheel carries a 118 MB binary;
- round 32 measured a cold first answer of 5.1 s (DuckDB 3.0 s) and 109 MB idle (DuckDB 50 MB):
  `logs/round32/footprint.txt`.

The code assumes one node per process. An audit of `src/` (2026-10-11) found what that means:

- `cluster::restart` execs the program again (16 call sites); `panics::spawn` aborts the process
  when a vital loop panics, as the catalog's compactor does when it fails; `format::watch` exits;
- the node handles SIGTERM and its standard input (invariant 46);
- about a dozen statics hold one node's state (the queries runtime, the owner key, the users' keys
  and grant caches, the history writer, the format mark, …), among 180 `OnceLock`/`LazyLock`s;
- `current_exe()` names the program a node starts (`pondra lead`, a database's node, the shell) and
  where Python workers are looked for; inside Python it is the Python interpreter;
- 100 `PONDRA_*` settings are read from the environment.

## Decision

1. **The crate is a library, and the program is a thin shell around it** (`src/lib.rs`, `src/cli.rs`,
   `src/main.rs`). Feature `server` (on by default) is the node: the catalog, the doors, the cluster
   and the command line. Without it the library is the parts a browser build shares (ADR-062). The
   release binary is built as before and is the same program. The global allocator (mimalloc) is set by
   the program (`main.rs`) and by the C ABI's library, never by the crate, so a Rust program built on
   it picks its own.
2. **A node is an object: `Node::start(options)` and `node.stop()`.** `pondra serve` is `Node::start`
   with the signals, the standard input and `exit` around it, and nothing else. `stop` is the steps a
   node takes when told to stop today (invariant 210): drain, wait for durability, checkpoint, step
   down (`cluster/left/{n}`). So the next `open` of the lake, in this process or another, leads at
   once (invariants 17 and 46).
3. **One node at a time in a process, started and stopped as often as wanted.** A test suite can open,
   close and open again. A second `open` while one is open is refused by name. Other lakes are read
   and written through `ATTACH`, as a node already does. It is a 1.0 limit, said in the docs; lifting
   it later changes no API.
   - The per-node statics stay where they are instead of threading a node through every module, but
     each is declared through one macro that registers how to reset it, and `stop` runs every reset.
     A rule that says "add your static to the reset" would be broken by the first one forgotten.
   - A check proves it: open lake A, sign a user in, write, close; open lake B, and nothing of A is
     there (its users, grants and keys, the format mark, the history writer, the hot columns).
4. **One engine runtime for the process, never dropped.** A runtime that HTTP clients, gRPC clients
   and timers were made on can't be replaced under them. A node's long-lived tasks are tracked
   (`panics::spawn` registers each); `stop` aborts what's left after the steps above.
5. **In a library, the node never takes the process down.** The program keeps restarting by exec,
   which `cluster.py race` and `failover` prove; restarting in place is the library's alone until
   those suites pass with it. In a library, a restart (`cluster::restart`) asks the
   node's supervisor, a thread of its own, to stop and start the node again in place, a few times
   with a pause, then gives up with the reason. A vital loop's panic stops the node with its reason
   instead of aborting. Callers get 57P03 with that reason, and `open` again starts over. The
   compactor's failure and the format watch (invariant 207) do the same. No signal handlers, no
   standard input.
6. **One C ABI for every language** (`pondra_lib`): `pondra_open(options_json)`,
   `pondra_request(handle, method, path, headers, body)`, `pondra_free`, `pondra_close`,
   `pondra_last_error`, and `pondra_main(argc, argv)` for the launcher. Every entry catches a panic
   and returns an error (unwinding across `extern "C"` is undefined). The library is never unloaded.
7. **A request goes straight to the node's router**, as its owner from 127.0.0.1 (invariant 61), so
   everything the HTTP API does works in process, with the same checks. Answers that stream (`/live`,
   the commit stream, big downloads) go to the node's own loopback port, which it opens anyway.
   That port takes what `local()`'s does today: 127.0.0.1 only, and the owner key for files on the
   machine (invariant 61). A node opened with no tokens is open to that machine's other users, as
   `local()`'s is; giving `open` a token closes it.
   - **Python** binds the ABI with `ctypes` (no compiled extension; the call releases the GIL).
     `pondra.open(path)` returns the same connection `connect()` and `local()` return, so frames,
     `db.sql`, `write_table` and the rest work unchanged. Arrow answers go to pyarrow, pandas and
     Polars from the answer's own buffer.
   - **Node** has no foreign-function interface of its own, so a small addon opens and closes the
     node (off the event loop) and the client speaks HTTP to its loopback port.
   - `pondra.open()` with no path, or `":memory:"` as DuckDB spells it, holds its lake in this
     process's memory: the same lake code over an object store in memory (`memory://<id>`), nothing
     written to disk but what a query spills when memory runs short, let go at `close`. `pondra
     :memory:` and `pondra serve :memory:` are the same from the command line. No other process
     reaches it, so none leads it (the hidden `pondra lead` refuses one, invariant 62).
8. **Every setting is read through one function, from one table of names** (`PONDRA_*`: the options
   given to `open`, then the environment). Setting the environment while a host already runs threads
   (Jupyter, any Python) is a data race, which is why Rust 2024 marks `set_var` unsafe, so a library
   never calls it: the node's own calls (the command line's TLS files and admin token, a one-off
   writer's `PONDRA_JOB` and `PONDRA_TRIES`) become options there. The browser has no environment
   and reads the same table (ADR-062), and the docs list the settings from it (principle 9).
   Settings are fixed by the first `open`; a later `open` with different ones is refused by name. The
   command line is unchanged.
9. **What the node starts, it starts with the launcher**, never `current_exe()` (`pondra::exe()`, the
   launcher's path from the options). A process that forks after `open` (Python's `multiprocessing`
   on Linux) finds the handle closed in the child and must open again; the docs say to use `spawn`.
10. **The packages carry the library and a small launcher.** The wheel and the npm packages hold
    `pondra_lib` and a `pondra` launcher that calls `pondra_main` (found beside it: `$ORIGIN`,
    `@loader_path`), so the engine is in them once. The installers and the release keep the single
    static binary. The library is built for glibc 2.17 as the binary is (invariant 48).

## Consequences

- `pondra.open()` needs no port, no child process and no second copy of the engine. What it costs to
  open, its idle memory and an answer of 10 M rows into pandas are measured against `local()` and
  DuckDB before it is called done (`tools/bench/footprint.py`, a row for each).
- A node in process is still a cluster node: it can lead a lake that other machines join, and it
  follows a leader elsewhere. Its idle memory is a node's; making that smaller is its own work.
- One node a process is a limit DuckDB doesn't have. `ATTACH` covers reading and writing several lakes
  from one connection.
- The per-node statics stay statics, declared through the macro that resets them, and the check
  above fails when one isn't.
- A node's paths that exit or abort go through one place that knows whether it runs as a program or
  a library.

## Plan

1. The library entry (`lib.rs`, `cli.rs`, the `server` feature) and this ADR.
2. `Node::start` and `stop` out of `cli.rs`; tracked tasks; the reset macro and its check; settings
   through one function.
3. Library mode: the supervisor, restarts in place, `exe()`, no signals; the owners' files as paste
   text.
4. The C ABI, `pondra.open` in Python, the fork guard.
5. The wheel and npm packages with the library and the launcher (`package.py`, `try_packages.sh`,
   `anywhere_check.py --docker`): the wheel's size against today's 118 MB binary, and the library
   checked for glibc 2.17 (invariant 48's `objdump` line).
6. The Node addon; the checks: open, close and open 50 times with no threads or files left over; a
   node in process following a leader that is killed, and leading in its place; a vital loop's
   panic answered as 57P03, then opened again; a fork.
