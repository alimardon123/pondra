#!/usr/bin/env python3
"""A node leaves nothing of itself in its process (ADR-061 §3, invariant 301).

  per_node_check.py [--list]

One process may start a node, stop it and start another on another lake (`pondra.open`). So every
static in `src/` (outside `#[cfg(test)]`) that can change once made must be one of:

- `per_node!`: the node's own, set back by `reset::reset()` when it stops (an atomic one starts at
  0 or false, which is what it is set back to);
- in PROCESS below, with the reason it outlives a node (a setting, fixed by the first open as
  ADR-061 §8 decides; the process's runtime; a lock that holds nothing).

A regex, a `task_local!` key and a value nothing can change once made (no lock, atomic, cell or
channel in its type: a table of constants, or a setting read once) need neither. Anything else fails
here by file, line and name, saying what to do. `--list` prints every static and how it passes.

Two more ways a node outlives its stop fail here too: a task started with `tokio::spawn` (or on a
runtime's handle) outside `panics.rs`, which `Node::stop` can't end (start it with `panics::spawn`
or `panics::task`), and `std::env::set_var` anywhere (the environment is the process's: the next
node and every child process would read it, and beside a library's host it is a data race).

And a page builds every module but the doors' (ADR-072): std's `Instant` and `SystemTime::now` panic
there, so those modules read the time through DataFusion's `Instant` (std's own on a node) and
`log::now_us` or `now_ms`, and a std clock in one fails (CLOCKS below names a file that may, and why).

And every setting goes through `env.rs` (ADR-061 §8): a `PONDRA_*` name read with `std::env`
anywhere else fails (by its text, or a constant that names it), so does a `"PONDRA_…"` name its NAMES table doesn't list, and a name the table
lists that nothing reads (env.rs's own rules, the memory line's, read outside the table).

Only the standard library; it reads the source, not a build.
"""
import argparse, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "src")

# What outlives a node, by `file:NAME`, each with its reason. One by one, never a pattern (a name
# twice in one file is one entry for both).
SETTING = "a setting read once: fixed by the first open, a later open with others refused (ADR-061 §8)"
LOCK = "a lock that holds nothing: it only takes turns"
GAUGE = "a gauge its holders' guards put back: 0 once they are gone, and set back by hand a guard dropped later would wrap it"
COUNTER = "a counter that only keeps names unique"
SIGNAL = "a wake-up holding no data: one left from a node only makes its next waiter look again"
PROCESS = {
    "ai.rs:TURNS": SETTING + " (PONDRA_AI_CALLS)",
    "audit.rs:C": SETTING + " (PONDRA_AUDIT)",
    "cache.rs:OUTSIDE": "files outside the lakes, kept by the version each was listed at (invariant 96): nothing of a lake's own",
    "cli.rs:ERROR_LINES": "the process's logger, set by the command (`log` keeps one a process): it holds nothing",
    "codes.rs:SEEN": "the SQLSTATE codes other nodes answered with, each kept once as a &'static: the same five letters for every node",
    "matcher.rs:LIVE_STEPS": "a count of the path steps alive on this thread, which a search makes and frees within one partition: a partition reads it against its own start, so no node's state is in it",
    "console.rs:FILES": "the console's files, built into the binary",
    "console.rs:PAGE": "the console's page, built into the binary",
    "console.rs:SERVER": "the server's page, built into the binary",
    "constraints.rs:TURN": LOCK,
    "copy.rs:MS": SETTING + " (PONDRA_LOAD_RETENTION_SECS)",
    "defaults.rs:BASE": "the functions every session has, with no tables, for CHECK, DEFAULT and expectation expressions: nothing of a lake (what registers one gets a copy)",
    "dbserver.rs:KEY": "the key a lakes server gives its databases' nodes (`client_key`): one a process, made once",
    "delta.rs:ONE": LOCK,
    "library.rs:R": "a library's runtime, kept for the next open: its threads are the process's, as the queries' runtime's are",
    "env.rs:GIVEN": "what the process was given (open's options, the command line's flags): fixed by its first open (ADR-061 §8)",
    "ext.rs:BARE": "a client with no token and no time limit (`bare`): only its keep-alive sockets",
    "ext.rs:CLIENT": "a client with no token, for other engines' REST catalogs: only its keep-alive sockets",
    "engine.rs:SUBSTR": "a function's definition",
    "fit.rs:ANALYZER": "the analyzer's rules, DataFusion's and Pondra's, for a point change's SET: no state, nothing of a lake",
    "flight.rs:INFO": "what Flight SQL says of the server: constants",
    "format.rs:ONE": LOCK,
    "history.rs:L": SETTING + " (PONDRA_LEARN_MS)",
    "history.rs:ON": SETTING + " (PONDRA_HISTORY)",
    "history.rs:R": SETTING + " (PONDRA_HISTORY_RATE)",
    "history.rs:S": SETTING + " (PONDRA_SLOW_MS)",
    "intervals.rs:SCALE": "a function's definition",
    "live.rs:OPEN": GAUGE,
    "log.rs:N": SETTING + " (PONDRA_INLINE_KB, PONDRA_REPLICAS)",
    "main.rs:ALLOC": "the allocator: one a process, the program's (ADR-061 §1)",
    "metrics.rs:RUNNING": "a gauge of the process's queries running now: a stopped node's last ones count themselves down (set back to 0, it would wrap)",
    "metrics.rs:WAITING": "a gauge of the process's queries waiting for a turn, as RUNNING",
    "node.rs:CLAIMED": "a live page holds the module's page (`node::claim_page`, `page::Page::claim`): plain, not per_node!, since a freed page's member closes in the background, and its reset() would give back the claim of the page made after it",
    "node.rs:HOSTED": "what the process is: a library's, from its first open (ADR-061 §5)",
    "node.rs:MOVER": "the host's end of a lake's moves (`node::mover`): taken by each host before its node starts, so it outlives the node it starts; a stopped host's receiver closes it",
    "node.rs:PAGED": "a page's member is open in this process (`node::open_page`): plain, not per_node!, since reset() would set it back before the other statics, and a page opened in that gap would have them cleared under it; end_page sets it back last",
    "node.rs:SUPERVISOR": "the supervisor of the node open now, which outlives each start of that node (a restart in place)",
    "optimize.rs:NEXT": COUNTER,
    "origins.rs:ORIGINS": SETTING + " (PONDRA_WEB_ORIGINS)",
    "panics.rs:NEXT": COUNTER,
    "panics.rs:R": "the queries' runtime and the memory watch's, one each a process (ADR-061 §4): the node's tasks on them end with the node, the runtimes don't",
    "path.rs:HEADS": "regexes and constants",
    "path.rs:LATE": "regexes and constants",
    "pg_bytea.rs:STRPOS": "a function's definition (strpos, position and instr over bytea and text), the same for every lake",
    "pg_json.rs:FUNCTIONS": "Postgres's JSON operators' and functions' definitions, made once: nothing of a lake",
    "pg_json_edit.rs:FUNCTIONS": "the JSON edits' definitions, made once: nothing of a lake",
    "pg_json_sets.rs:SETS": "the JSON set functions' names and columns, built into the binary",
    "pyfn.rs:SEEN": "SQL type names worked out by a session with nothing of a lake",
    "public.rs:CLIENT": "a client with no token, for links someone other than the admin named (a share's): only its keep-alive sockets",
    "python.rs:BUSY": LOCK,
    "python.rs:CHOSEN": "the Python chosen in the console: the machine's, kept in python.txt beside the console's settings",
    "python.rs:EXE": SETTING + " (--python)",
    "python.rs:LOOKING": LOCK,
    "python.rs:PYTHON": "what the process's Python (EXE's or CHOSEN's) said of itself",
    "engine.rs:JSON_TEXT": "a function's definition (pondra_json_text), the same for every lake",
    "casts.rs:WHOLE": "a function's definition (pondra_whole), the same for every lake",
    "multiply.rs:MULTIPLY": "a function's definition (pondra_multiply), the same for every lake",
    "reset.rs:USED": "what reset() sets back: every per-node static used since the process started",
    "runs.rs:WAKE": SIGNAL,
    "runs.rs:WRITTEN": SIGNAL,
    "app.rs:TURNS": SETTING + " (PONDRA_QUERIES, PONDRA_QUEUE_SECS; a stopped node's last queries give their turns back to it)",
    "service.rs:CONFIG": "the Windows service's file, set by its entry point once a process",
    "memory.rs:MADE": "the spill folders this process made and hasn't removed: a folder of a static (`store::SPILLS`) outlives a node until the program ends, when `memory::leave` removes what is left; `sweep` skips them",
    "memory.rs:MEMORY": "the lakes held in this process's memory: a node restarting in place finds its own; the library's close lets it go (`forget_memory`), and the program's end (`memory::leave`) empties the map",
    "memory.rs:PROCESS": "the spill disk is the machine's: every lake in memory in this process counts its bytes on disk in one count (a lake that goes gives its bytes back in its drop)",
    "oom.rs:POINTERS": "a thread's own allocation counters (jemalloc's), found once a thread: they belong to the thread, not to a node",
    "oom.rs:TEST_ALLOCATOR": "the unit tests' allocator, jemalloc as the node's (main.rs), so the meter has counters to read",
    "oom.rs:TRACER": "DataFusion's one join-set tracer for the process, set once: it reads the task-local of whichever request spawns",
    "store.rs:MS": SETTING + " (PONDRA_LOCK_LEASE_SECS: the retry window, `store::retry_ms`)",
    "store.rs:ONE": LOCK,
    "store.rs:UPDATES": LOCK,
    "store.rs:PARTS": SETTING + " (PONDRA_CORES, --memory-gb)",
    "store.rs:RAM": "the machine's memory, or its container's",
    "store.rs:READER": "a page's reading of its live bytes, the program's (`read_resident_with`): set once a process, as its allocator is (`main.rs:ALLOC`)",
    "store.rs:SPILLS": "the process's own spill folder, made by the first spill and held until the program ends: `memory::leave` removes it then, or where it can't (Windows, lock open) the next process's sweep does (memory::sweep)",
    "store.rs:SPILLERS": "the spill disk is the machine's: every lake's spills in this process share its room (held weakly, a lake's goes with it)",
    "store.rs:START": "the clock the spill disk's last look is measured from: the disk is the machine's",
    "store.rs:LOOKED": "when the spill disk was last looked at: the disk is the machine's, so one look serves every lake",
    "engine.rs:SPARK": "Spark's functions, made against a session with nothing of a lake",
    "temp.rs:CHANGES": "a counter live queries wait on: it only grows, so a waiter never misses a change",
    "tier.rs:SLOTS": SETTING + " (tiering's slots, sized by --memory-gb)",
    "tls.rs:KEY": SETTING + " (PONDRA_CLIENT_KEY, which the lakes server gives each database's node)",
    "tls.rs:PROXIES": SETTING + " (PONDRA_TRUSTED_PROXIES)",
    "tls.rs:TLS": SETTING + " (PONDRA_TLS_*)",
    "users.rs:MAKING": LOCK,
    "write.rs:JOB": "`pondra sql`'s job: the process's, carried through its restarts (`node::restart_with`)",
}

# What a value nothing can change is made of: anything else (a crate's own type, a session, a client,
# a lock behind a name) is a node's or needs its reason in PROCESS.
PLAIN = {"std", "sync", "collections", "time", "path", "num", "borrow", "regex", "serde_json", "bytes",
         "LazyLock", "Option", "Vec", "Box", "Arc", "HashMap", "HashSet", "BTreeMap", "BTreeSet", "String",
         "str", "bool", "char", "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64",
         "i128", "isize", "f32", "f64", "Duration", "PathBuf", "Path", "Regex", "Value", "Bytes", "Cow",
         "NonZeroUsize", "static"}
# Where a `"PONDRA_…"` name isn't a setting anything reads, by file, with the reason.
NOT_READ = {
    "ci.rs": "GitHub secrets' names, which `pondra ci init` sets for a repository's workflows",
}
READ = re.compile(r"""\benv::var(?:_os)?\(\s*"PONDRA_""")
READ_BY = re.compile(r"\benv::var(?:_os)?\(\s*(?:[a-z_]+::)*([A-Z_][A-Z0-9_]*)\s*\)")  # (through a constant's name)
CONST = re.compile(r'\bconst\s+([A-Z_][A-Z0-9_]*)\s*:\s*&(?:\'static\s+)?str\s*=\s*"(PONDRA_[A-Z0-9_]+)"')
NAME = re.compile(r'"(PONDRA_[A-Z0-9_]+)"')
SPAWN = re.compile(r"\btokio::(?:task::)?spawn\(|Handle::(?:current|try_current)\(\)[^;]*?\.spawn\(")
REGEX = re.compile(r"^(?:std::sync::)?LazyLock<(?:regex::)?Regex>$")
STATIC = re.compile(r"\bstatic\s+(?:mut\s+)?([A-Z_][A-Z0-9_]*)\s*:")


# Files a page builds that may read std's clock, each with why.
CLOCKS = {
    "log.rs": "now_us itself: the machine's clock on a node (a page's is the browser's, tokio::clock)",
}
STD_CLOCK = re.compile(r"\bstd::time::(?:Instant|SystemTime)\b|\bstd::time::\{[^}]*\b(?:Instant|SystemTime)\b|(?<![\w:])SystemTime::now\(")


def tests_cut(text):
    """The source with each `#[cfg(test)]` item blanked (lines kept, so line numbers hold)."""
    lines = text.split("\n")
    out, i = [], 0
    while i < len(lines):
        if lines[i].strip() == "#[cfg(test)]":
            depth, seen, j = 0, False, i
            while j < len(lines):
                depth += lines[j].count("{") - lines[j].count("}")
                seen = seen or "{" in lines[j]
                if (seen and depth <= 0) or (not seen and lines[j].rstrip().endswith(";")):
                    break
                j += 1
            out += [""] * (j - i + 1)
            i = j + 1
            continue
        out.append(lines[i])
        i += 1
    return "\n".join(out)


def aliases(text):
    """The file's `type X = …;` aliases (one level of them is enough: `Times`, `Seen`)."""
    return {m.group(1): " ".join(m.group(2).split()) for m in re.finditer(r"\btype\s+([A-Z]\w*)\s*=\s*([^;]+);", text)}


def resolved(ty, named):
    """`ty` with the file's aliases put in, so a lock behind a name is seen."""
    for _ in range(3):
        ty = re.sub(r"\b[A-Z]\w*\b", lambda m: named.get(m.group(0), m.group(0)), ty)
    return ty


def type_of(text, start):
    """A static's type: from after its `:` to the `=` or `;` at depth 0 of <>, () and []."""
    depth, i = 0, start
    while i < len(text):
        c = text[i]
        if c in "<([":
            depth += 1
        elif c in ">)]" and not (c == ">" and text[i - 1] == "-"):
            depth -= 1
        elif c in "=;" and depth == 0:
            return " ".join(text[start:i].split()), i
        i += 1
    return " ".join(text[start:].split()), i


def statics():
    """Every static outside tests: (file, line, name, type, how it is declared, its value's text)."""
    found = []
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if not f.endswith(".rs"):
                continue
            path = os.path.join(base, f)
            text = tests_cut(open(path, encoding="utf-8").read())
            rel = os.path.relpath(path, SRC).replace(os.sep, "/")
            locals_ = [(m.start(), m.end()) for m in re.finditer(r"task_local!\s*\{[^}]*\}", text)]
            named = aliases(text)
            for m in STATIC.finditer(text):
                line = text.count("\n", 0, m.start()) + 1
                ty, end = type_of(text, m.end())
                ty = resolved(ty, named)
                before = text[max(0, m.start() - 4000):m.start()]  # (a long doc comment inside per_node! included)
                if any(a <= m.start() < b for a, b in locals_):
                    how = "task_local"
                elif re.search(r"per_node!\s*\(\s*(?:(?:#\[[^\]]*\]|///[^\n]*)\s*)*(?:pub(?:\([^)]*\))?\s+)?$", before):
                    how = "per_node"
                elif "macro_rules!" in text[max(0, m.start() - 300):m.start()] or "$name" in text[m.start():m.start() + 40]:
                    continue  # (a macro's own text, as `per_node!` writes a static)
                else:
                    how = "static"
                value = text[end + 1:text.find(";", end) if text[end] == "=" else end].strip()
                found.append((rel, line, m.group(1), ty, how, value))
    return found


def verdict(rel, name, ty, how, value):
    """How a static passes, or None: what it must be."""
    key = f"{rel.rsplit('/', 1)[-1]}:{name}"
    if how == "per_node":
        if key in PROCESS:
            return None, "declared per_node!, so not the process's: take it out of PROCESS in tools/per_node_check.py"
        if re.match(r"(?:std::sync::atomic::)?Atomic", ty) and not re.search(r"::new\(\s*(0|false)\s*\)", value):
            return None, "a per_node! atomic starts at 0 or false (what reset() sets it back to)"
        return "per_node", None
    if how == "task_local":
        return "a task_local key", None
    if REGEX.match(ty):
        return "a regex", None
    if key in PROCESS:
        return f"PROCESS: {PROCESS[key]}", None
    if ty.startswith("&") or set(re.findall(r"[A-Za-z_]\w*", ty)) <= PLAIN:
        return "nothing changes it once made", None
    return None, "a node's state? declare it with per_node!; or add it to PROCESS in tools/per_node_check.py with why it outlives a node"


def leaks():
    """Tasks started where `Node::stop` can't end them, and the environment set by a node."""
    bad = []
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if not f.endswith(".rs") or f == "panics.rs":
                continue
            path = os.path.join(base, f)
            text = tests_cut(open(path, encoding="utf-8").read())
            rel = os.path.relpath(path, SRC).replace(os.sep, "/")
            for n, line in enumerate(text.split("\n"), 1):
                code = line.split("//", 1)[0]
                if SPAWN.search(code):
                    bad.append(f"src/{rel}:{n}: a task Node::stop can't end: start it with crate::panics::task (or panics::spawn for a loop the node can't do without)")
                if "set_var(" in code:
                    bad.append(f"src/{rel}:{n}: the environment is the process's, not the node's: give a setting with crate::env::give, keep a node's state in a per_node! static, or pass a child process its environment with Command::env")
    return bad


def settings():
    """Settings read past `env.rs`, names it doesn't list, and names it lists that nothing reads."""
    table = open(os.path.join(SRC, "env.rs"), encoding="utf-8").read()
    table = table[table.index("pub const NAMES"):]
    table = table[:table.index("];")]  # (the table alone: its tests name settings that aren't)
    names, read, bad = set(re.findall(r'\(\s*"(PONDRA_[A-Z0-9_]+)"\s*,', table)), set(), []
    sources = {}
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if f.endswith(".rs"):
                sources[os.path.join(base, f)] = tests_cut(open(os.path.join(base, f), encoding="utf-8").read())
    named = {c: v for text in sources.values() for c, v in CONST.findall(text)}  # (constants that are a setting's name)
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if not f.endswith(".rs") or f == "env.rs":
                continue
            path = os.path.join(base, f)
            rel = os.path.relpath(path, SRC).replace(os.sep, "/")
            for n, line in enumerate(sources[path].split("\n"), 1):
                code = line.split("//", 1)[0]
                if READ.search(code) or any(c in named for c in READ_BY.findall(code)):
                    bad.append(f"src/{rel}:{n}: a setting read past env.rs: read it with crate::env::get (or os, parse, or, secs, flag, is_set)")
                for name in NAME.findall(line):
                    read.add(name)
                    if name not in names and f not in NOT_READ:
                        bad.append(f"src/{rel}:{n}: {name} isn't in env.rs's NAMES: add it there with who it is for (or the file to NOT_READ in tools/per_node_check.py with why)")
    own = sources[os.path.join(SRC, "env.rs")]  # (a rule env.rs keeps itself, the same in every form, reads its setting too)
    start = own.index("pub const NAMES")
    read |= set(NAME.findall(own[:start] + own[own.index("];", start):]))
    return bad + [f"src/env.rs: NAMES lists {name}, which nothing reads: take it out" for name in sorted(names - read)]


def clocks():
    """std's clocks in a module a page builds (all but the `doors` ones, lib.rs's or a module's own): they panic on a page."""
    sources, doors = {}, {"main"}
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if f.endswith(".rs"):
                rel = os.path.relpath(os.path.join(base, f), SRC).replace(os.sep, "/")
                sources[rel] = open(os.path.join(base, f), encoding="utf-8").read()
                parent = "" if rel == "lib.rs" else rel.removesuffix("/mod.rs").removesuffix(".rs") + "/"
                doors |= {parent + m for m in re.findall(r'#\[cfg\([^\]]*"doors"[^\]]*\)\]\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;', sources[rel])}
    bad = []
    for rel, text in sorted(sources.items()):
        module = rel.removesuffix("/mod.rs").removesuffix(".rs")
        if rel in CLOCKS or any(module == d or module.startswith(d + "/") for d in doors):
            continue
        for n, line in enumerate(tests_cut(text).split("\n"), 1):
            if STD_CLOCK.search(line.split("//", 1)[0]):
                bad.append(f"src/{rel}:{n}: std's clock panics on a page: use datafusion::common::instant::Instant (std's own on a node) or crate::log::now_us / now_ms (or add the file to CLOCKS in tools/per_node_check.py with why)")
    return bad


def poisoned(found):
    """A process's lock read with `.unwrap()`: a library's node restarting in place unwinds through
    whatever lock it holds (`node::restart`), and the next start must still read it."""
    bad = []
    for rel, line, name, ty, how, value in found:
        if how != "static" or f"{rel.rsplit('/', 1)[-1]}:{name}" not in PROCESS or not re.search(r"(?<!tokio::sync::)\b(Mutex|RwLock)<", ty):
            continue
        text = tests_cut(open(os.path.join(SRC, rel), encoding="utf-8").read())
        for m in re.finditer(rf"\b{name}\s*\.\s*(?:lock|read|write)\(\)\s*\.\s*unwrap\(\)", text):
            n = text.count("\n", 0, m.start()) + 1
            bad.append(f"src/{rel}:{n}: {name} is the process's and may be poisoned by a restart in place: read it with .unwrap_or_else(PoisonError::into_inner)")
    return bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="every static and how it passes")
    args = ap.parse_args()
    found = statics()
    bad, keys = [], set()
    for rel, line, name, ty, how, value in found:
        keys.add(f"{rel.rsplit('/', 1)[-1]}:{name}")
        ok, why = verdict(rel, name, ty, how, value)
        if args.list:
            print(f"{'ok  ' if ok else 'FAIL'} src/{rel}:{line} {name}: {ty}  ({ok or why})")
        if not ok:
            bad.append(f"src/{rel}:{line} {name} ({ty}): {why}")
    stale = sorted(k for k in PROCESS if k not in keys)
    more = [f"PROCESS names {k}, which src/ no longer has: take it out" for k in stale] + leaks() + settings() + clocks() + poisoned(found)
    bad += more
    for b in bad if not args.list else more:
        print(f"FAIL {b}")
    print(f'{{"statics": {len(found)}, "failed": {len(bad)}, "ok": {str(not bad).lower()}}}')
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
