#!/usr/bin/env python3
"""A node leaves nothing of itself in its process (ADR-061 §3, invariant 301).

  per_node_check.py [--list]

One process may start a node, stop it and start another on another lake (`pondra.open`), or run several
at once. So a node's state is never a static: it is its lake's (a `LakeState`, `lake.state::<T>()`) or its
node's (`NodeState`), and goes with them (redesign step 2; `per_node!` and `reset.rs`, which set statics back
at a node's stop, are gone). Every static in `src/` (outside `#[cfg(test)]`) that can change once made must
be in PROCESS below, with the reason it outlives a node (a setting, fixed by the first open as ADR-061 §8
decides; the process's runtime; a lock that holds nothing; what the machine has one of).

A regex, a `task_local!` key and a value nothing can change once made (no lock, atomic, cell or
channel in its type: a table of constants, or a setting read once) need neither. Anything else fails
here by file, line and name, saying what to do. `--list` prints every static and how it passes.

Two more ways a node outlives its stop fail here too: a task started with `tokio::spawn` (or on a
runtime's handle) outside `spawns.rs`, which `Node::stop` can't end (start it on the node's list,
`Tasks::spawn_node` or `Tasks::spawn_as`, or on a state's `spawns::Loops` for a loop that serves that
state), and `std::env::set_var` anywhere (the environment is the process's: the next
node and every child process would read it, and beside a library's host it is a data race).

And a page builds every module but the doors' (ADR-072): std's `Instant` and `SystemTime::now` panic
there, so those modules read the time through `crate::clock` (its `Instant`, tokio's, which a test's paused
time moves too, and `wall_us` or `wall_ms`, which `log::now_us` and `now_ms` call), and a std clock in one
fails (CLOCKS below names a file that may, and why).

And every setting goes through `env.rs` (ADR-061 §8): a `PONDRA_*` name read with `std::env`
anywhere else fails (by its text, or a constant that names it), so does a `"PONDRA_…"` name the table (`settings.rs`) doesn't list, and a name the table
lists that nothing reads (env.rs's own rules, the memory line's, read outside the table).

And every walk of the SQL tree goes through `crate::walk` (`src/walk.rs`): sqlparser's `Visit` and `VisitMut` are
generic over the visitor, so each walk is compiled again for every visitor type. Outside walk.rs, a `use` of either
trait, or a call of `visit_expressions`, `visit_expressions_mut`, `visit_relations`, `visit_relations_mut`,
`visit_statements` or `visit_statements_mut` not through `walk::`, fails here by file and line.

And every call between members goes through the calling node's dial (`dial::call`, `NodeState::dial`), so the sim's
members reach each other in its process and none of their calls goes around it (redesign step 3, piece 3c): the nodes'
HTTP client (`cluster::http`) is sent with in one place, `cluster::send_call` (the `Http` dial), and named nowhere else.

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
    "ai.rs:CALLS": "requests to the AI endpoint that were answered: the endpoint and its turns are the process's environment's (`PONDRA_AI_URL`, `PONDRA_AI_CALLS`)",
    "ai.rs:FAILED": "requests to the AI endpoint that failed for good: the process's, as CALLS",
    "ai.rs:RETRIED": "requests to the AI endpoint that were tried again: the process's, as CALLS",
    "ai.rs:SAID": "when a failed AI request was last said on standard error: the process's, which says it",
    "budget.rs:GATES": "a bucket's turns (C5) are the machine's: the store limits requests per bucket, so every lake in this process that uses the bucket backs off together when it says slow down",
    "budget.rs:URGENT": "a bucket's few urgent turns (a claimer's mark, the inbox's bell) are the machine's, as its other turns are",
    "budget.rs:SHARES": "a branch's base's share of a bucket's turns, by the base's path: the machine's, as the bucket's turns are",
    "cache.rs:OUTSIDE": "files outside the lakes, kept by the version each was listed at (invariant 96): nothing of a lake's own",
    "cli.rs:ERROR_LINES": "the process's logger, set by the command (`log` keeps one a process): it holds nothing",
    "codes.rs:SEEN": "the SQLSTATE codes other nodes answered with, each kept once as a &'static: the same five letters for every node",
    "matcher.rs:LIVE_STEPS": "a count of the path steps alive on this thread, which a search makes and frees within one partition: a partition reads it against its own start, so no node's state is in it",
    "console.rs:FILES": "the console's files, built into the binary",
    "console.rs:PAGE": "the console's page, built into the binary",
    "console.rs:SERVER": "the server's page, built into the binary",
    "constraints.rs:TURN": LOCK,
    "defaults.rs:BASE": "the functions every session has, with no tables, for CHECK, DEFAULT and expectation expressions: nothing of a lake (what registers one gets a copy)",
    "dbserver.rs:KEY": "the key a lakes server gives its databases' nodes (`client_key`): one a process, made once",
    "ledger.rs:ROOTS": "the process's memory, every node's budget held on it (redesign step 2, review change 4): one root for the sizes its machine and settings gave, so one in any process but a test's",
    "library.rs:OPEN": "whether a library's node is open in the process: one at a time, since the machine and network its first start installs are the process's",
    "library.rs:R": "a library's runtime, kept for the next open: its threads are the process's, as the queries' runtime's are",
    "env.rs:GIVEN": "what the process was given (open's options, the command line's flags): fixed by its first open (ADR-061 §8)",
    "env.rs:HANDED": "what the Pondra process that started this one handed it (`settings::Line`), read once before anything: where its environment's would be",
    "ext.rs:BLOB": "a page's store of its tab's blob: URLs, the browser's fetch (`blob_with`): set once a module, as its allocator is",
    "ext.rs:UNWRAPPED": "data keys unwrapped by the process's master key or key service (PONDRA_SECRET_KEY, PONDRA_KMS_COMMAND: its environment's), by their wrapped form: the same for any lake here until step 7 gives a lake keys of its own",
    "ext.rs:SAID": "the short-key warning, said once a process: the key is its environment's",
    "ext.rs:KEYS": "PBKDF2 of an admin token, worked out once: a pure function of the token",
    "engine.rs:SUBSTR": "a function's definition",
    "fit.rs:ANALYZER": "the analyzer's rules, DataFusion's and Pondra's, for a point change's SET: no state, nothing of a lake",
    "flight.rs:INFO": "what Flight SQL says of the server: constants",
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
    "node.rs:CLAIMED": "a live page holds the module's page (`node::claim_page`, `page::Page::claim`): the module's one page, which unclaim_page alone gives back (a freed page's member closes in the background, after the next page may have claimed it)",
    "machine.rs:HOSTED": "what the process is: a library's, from its first open (ADR-061 §5)",
    "machine.rs:INSTALLED": "the process's machine, installed once by its host as its allocator is chosen once; a node holds it (`NodeState::machine`)",
    "net.rs:INSTALLED": "the process's clients by policy, installed once by its host as its machine is; a node holds them (`NodeState::net`)",
    "net.rs:UNINSTALLED": "the clients of a process whose host installed none (`net::process`): only their keep-alive sockets",
    "scratch.rs:INSTALLED": "the process's spill space, installed once by its host as its machine is; a node holds it (`NodeState::scratch`)",
    "scratch.rs:UNINSTALLED": "this machine's folders, for a process whose host installed no space (`scratch::process`): no state of its own",
    "node.rs:PAGED": "a page's member is open in this process (`node::open_page`): the module's one member, which end_page sets back last, once the one before has let go of everything",
    "oom.rs:WATCH": "the process's memory watch (redesign step 2): what it read last, whether it waits or runs, and its wake, for the memory of the whole process; started once at the first node's start, it lives as long as the process",
    "optimize.rs:NEXT": COUNTER,
    "origins.rs:ORIGINS": SETTING + " (PONDRA_WEB_ORIGINS)",
    "panics.rs:NEXT": COUNTER,
    "spawns.rs:R": "the queries' runtime and the memory watch's, one each a process (ADR-061 §4): the node's tasks on the queries' end with the node, the runtimes don't, and the watch is the process's (oom.rs:WATCH)",
    "path.rs:HEADS": "regexes and constants",
    "path.rs:LATE": "regexes and constants",
    "pg_bytea.rs:STRPOS": "a function's definition (strpos, position and instr over bytea and text), the same for every lake",
    "pg_json.rs:FUNCTIONS": "Postgres's JSON operators' and functions' definitions, made once: nothing of a lake",
    "pg_json_edit.rs:FUNCTIONS": "the JSON edits' definitions, made once: nothing of a lake",
    "pg_json_sets.rs:SETS": "the JSON set functions' names and columns, built into the binary",
    "pyfn.rs:SEEN": "SQL type names worked out by a session with nothing of a lake",
    "python.rs:BUSY": LOCK,
    "python.rs:CHOICE": "how many times this machine's Python was chosen (`choose`), which is the machine's: each lake's workers start again on the new one",
    "python.rs:CHOSEN": "the Python chosen in the console: the machine's, kept in python.txt beside the console's settings",
    "python.rs:EXE": SETTING + " (--python)",
    "python.rs:LOOKING": LOCK,
    "python.rs:PYTHON": "what the process's Python (EXE's or CHOSEN's) said of itself",
    "engine.rs:JSON_TEXT": "a function's definition (pondra_json_text), the same for every lake",
    "casts.rs:WHOLE": "a function's definition (pondra_whole), the same for every lake",
    "avg.rs:AVG_OF": "a function's definition (pondra_avg_of), the same for every lake",
    "multiply.rs:MULTIPLY": "a function's definition (pondra_multiply), the same for every lake",
    "divide.rs:DIVIDE": "a function's definitions (pondra_divide, Postgres's places and Spark's), the same for every lake",
    "runs.rs:WAKE": SIGNAL,
    "runs.rs:WRITTEN": SIGNAL,
    "app.rs:TURNS": SETTING + " (PONDRA_QUERIES, PONDRA_QUEUE_SECS; a stopped node's last queries give their turns back to it)",
    "service.rs:CONFIG": "the Windows service's file, set by its entry point once a process",
    "scratch.rs:MADE": "the spill folders this process made and hasn't removed: a folder of a static (`store::SPILLS`) outlives a node until the program ends, when `scratch::leave` removes what is left; `sweep` skips them",
    "memory_lake.rs:MEMORY": "the lakes held in this process's memory: a node restarting in place finds its own; the library's close lets it go (`forget_memory`), and the program's end (`memory::leave`) empties the map",
    "memory_lake.rs:COUNTED": "the counted opens of the lakes in this process's memory, which `held_in_memory` knows as their lake's store: weak both, as long as `MEMORY` holds the lake, and no node's (an open outlives no lake)",
    "terms.rs:RUNG": "what this process heard of claimers on other machines, keyed by the store, the term and its lock: the same for any node here (the CLI's `alive` reads it too); answers older than `RUNG_AGAIN` are let go as one is written",
    "parsed.rs:LAST": "the parses of the last few statements' SQL: a function of the text (a tree kept is its text's parse), the same for any lake here",
    "panics.rs:RUNNING": "the requests running in this process: its one memory watch stops the one that grew the process's memory, whatever its node (`oom.rs`), and a long one sends the ones after it to the process's queries' runtime",
    "vend.rs:MADE": "each bucket's URL signer, made from the process's environment's credentials (as `from_env` reads them): the same for any lake here",
    "text.rs:TOKENS": "the tokens of the last few statements' SQL: a pure function of the text, the same for any lake here",
    "memory_lake.rs:PROCESS": "the spill disk is the machine's: every lake in memory in this process counts its bytes on disk in one count (a lake that goes gives its bytes back in its drop)",
    "oom.rs:POINTERS": "a thread's own allocation counters (jemalloc's), found once a thread: they belong to the thread, not to a node",
    "oom.rs:TEST_ALLOCATOR": "the unit tests' allocator, jemalloc as the node's (main.rs), so the meter has counters to read",
    "oom.rs:TRACER": "DataFusion's one join-set tracer for the process, set once: it reads the task-local of whichever request spawns",
    "sso.rs:KEYS": "each identity provider's published keys, by its issuer's url: public, and the same for any node here",
    "sso.rs:FETCHING": "one fetch at a time of an issuer's keys, by its url: the same for any node here",
    "store.rs:MS": SETTING + " (PONDRA_LOCK_LEASE_SECS: the retry window, `store::retry_ms`)",
    "store.rs:NAMED": SETTING + " (PONDRA_LOAD_RETENTION_SECS: a named job's window, `store::retention_ms`)",
    "store.rs:ONE": LOCK,
    "store.rs:UPDATES": LOCK,
    "store.rs:CORES": SETTING + " (PONDRA_CORES, else the cgroup's cores): the machine's, which every node's partitions are of, by its own memory (`partitions_of`)",
    "store.rs:SAID": "a warning said once a process: the cache folder under the temp dir is the machine's, so it fails the same for every lake (`cache_base`)",
    "store.rs:SPILLS": "the process's own spill folder, made by the first spill and held until the program ends: `memory::leave` removes it then, or where it can't (Windows, lock open) the next process's sweep does (scratch::sweep)",
    "ledger.rs:ROOM": "the spill disk is the machine's: everything this process spills (shuffles, each lake's query spills, the lakes in memory) shares its room (held weakly, a lake's goes with it)",
    "spill.rs:SHUFFLES": "the process's shuffle folder, every node's under it, on the spill disk's room list once",
    "store.rs:START": "the clock the spill disk's last look is measured from: the disk is the machine's",
    "store.rs:LOOKED": "when the spill disk was last looked at: the disk is the machine's, so one look serves every lake",
    "extension.rs:PARTS": "extensions' parts loaded in this process, by their file's SHA-256: a part holds no lake's state and its file never changes under its hash, so one serves every node",
    "extension.rs:TURNS": "the extension calls running in this process, against one a core: the cores are the machine's, so every node in a process shares one count (each call gives its turn back as it ends)",
    "wasm.rs:ENGINE": "Wasmtime's engine, the process's compiler: compiled code is shared by every node in a process, and holds no node's state",
    "store.rs:DISKS": "the window sorts' spill disks (`window_sorts`, invariant 423), one for each merge fan-in (PONDRA_SPILL_FAN_IN, else of the node's memory for queries: 4 to 64, so at most 61), made once: the disk is the machine's, as ledger.rs's ROOM says, and holds no node's state",
    "functions.rs:FUNCTIONS": "Spark's functions, made against a session with nothing of a lake",
    "registry.rs:REGISTRY": "the build's extensions, registered once from `crate::built_in` and never changed after: the same for every node, and no section holds a node's state",
    "tier.rs:SLOTS": SETTING + " (tiering's slots, sized by --memory-gb)",
    "tls.rs:KEY": SETTING + " (PONDRA_CLIENT_KEY, which the lakes server gives each database's node)",
    "tls.rs:PROXIES": SETTING + " (PONDRA_TRUSTED_PROXIES)",
    "tls.rs:TLS": SETTING + " (PONDRA_TLS_*)",
    "write.rs:JOB": "`pondra sql`'s job: the process's, carried through its restarts (`Lifecycle::restart`)",
}

# What a value nothing can change is made of: anything else (a crate's own type, a session, a client,
# a lock behind a name) is a node's or needs its reason in PROCESS.
PLAIN = {"std", "sync", "collections", "time", "path", "num", "borrow", "regex", "serde_json", "bytes",
         "LazyLock", "Option", "Vec", "Box", "Arc", "HashMap", "HashSet", "BTreeMap", "BTreeSet", "String",
         "str", "bool", "char", "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64",
         "i128", "isize", "f32", "f64", "Duration", "PathBuf", "Path", "Regex", "Value", "Bytes", "Cow",
         "NonZeroUsize", "static",
         "crate", "registry", "BuiltIn"}  # (a built-in extension's `registry::BuiltIn`: its names and a function, nothing to change)
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
TABLE = re.compile(r"\bconst\s+[A-Z_]+\s*:\s*&\[(?:crate::settings::)?Setting\]\s*=[^;]*?&\[")
STATIC = re.compile(r"\bstatic\s+(?:mut\s+)?([A-Z_][A-Z0-9_]*)\s*:")


# Files a page builds that may read std's clock, each with why.
CLOCKS = {
    "clock.rs": "the one clock itself: the machine's on a node (a page's is the browser's, tokio::clock), ahead by what paused time passed in a test",
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
        return None, "per_node! is gone (redesign step 2): keep a lake's state in a LakeState (lake.state::<T>()) or a node's in NodeState"
    if how == "task_local":
        return "a task_local key", None
    if REGEX.match(ty):
        return "a regex", None
    if key in PROCESS:
        return f"PROCESS: {PROCESS[key]}", None
    if ty.startswith("&") or set(re.findall(r"[A-Za-z_]\w*", ty)) <= PLAIN:
        return "nothing changes it once made", None
    return None, "a node's state? keep it in its lake (a LakeState, lake.state::<T>()) or its node (NodeState); or add it to PROCESS in tools/per_node_check.py with why it outlives a node"


def leaks():
    """Tasks started where `Node::stop` can't end them, and the environment set by a node."""
    bad = []
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if not f.endswith(".rs") or f == "spawns.rs":
                continue
            path = os.path.join(base, f)
            text = tests_cut(open(path, encoding="utf-8").read())
            rel = os.path.relpath(path, SRC).replace(os.sep, "/")
            for n, line in enumerate(text.split("\n"), 1):
                code = line.split("//", 1)[0]
                if SPAWN.search(code):
                    bad.append(f"src/{rel}:{n}: a task Node::stop can't end: start it on the node's list (node.tasks.spawn_node, or spawn_as for a request's work), or on a state's crate::spawns::Loops for a loop that serves that state")
                if "set_var(" in code:
                    bad.append(f"src/{rel}:{n}: the environment is the process's, not the node's: give a setting with crate::env::give, keep a node's state in its lake or its NodeState, or pass a child process its environment with Command::env")
    return bad


def tables(text):
    """A file's settings tables (`settings::CORE`, an extension's `SETTINGS`): the spans of `&[Setting] = … &[ … ];`."""
    out = []
    for m in TABLE.finditer(text):
        start = text.index("&[", m.end() - 2)
        out.append((m.start(), text.index("];", start) + 2))
    return out


def untabled(text):
    """The text with its settings tables blanked (lines kept): what is left reads settings, or names them."""
    for a, b in reversed(tables(text)):
        text = text[:a] + re.sub(r"[^\n]", " ", text[a:b]) + text[b:]
    return text


def settings():
    """Settings read past `env.rs`, names no settings table lists, and names a table lists that nothing reads."""
    sources, names, read, bad = {}, {}, set(), []
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if f.endswith(".rs"):
                path = os.path.join(base, f)
                sources[path] = tests_cut(open(path, encoding="utf-8").read())
                for a, b in tables(sources[path]):
                    for name in re.findall(r'\(\s*"(PONDRA_[A-Z0-9_]+)"\s*,', sources[path][a:b]):
                        names[name] = os.path.relpath(path, SRC).replace(os.sep, "/")
    named = {c: v for text in sources.values() for c, v in CONST.findall(text)}  # (constants that are a setting's name)
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if not f.endswith(".rs") or (base == SRC and f in ("env.rs", "settings.rs")):
                continue
            path = os.path.join(base, f)
            rel = os.path.relpath(path, SRC).replace(os.sep, "/")
            for n, line in enumerate(untabled(sources[path]).split("\n"), 1):
                code = line.split("//", 1)[0]
                if READ.search(code) or any(c in named for c in READ_BY.findall(code)):
                    bad.append(f"src/{rel}:{n}: a setting read past env.rs: read it with crate::env::value (or text, get, os, parse, secs, flag, is_set)")
                for name in NAME.findall(line):
                    read.add(name)
                    if name not in names and f not in NOT_READ:
                        bad.append(f"src/{rel}:{n}: {name} isn't in a settings table: add it to settings.rs's CORE, or its extension's SETTINGS, with who it is for and its kind (or the file to NOT_READ in tools/per_node_check.py with why)")
    # (a rule env.rs keeps itself, the same in every form, reads its setting too, as does a default the table works out)
    read |= set(NAME.findall(untabled(sources[os.path.join(SRC, "env.rs")] + sources[os.path.join(SRC, "settings.rs")])))
    return bad + [f"src/{names[name]}: its settings table lists {name}, which nothing reads: take it out" for name in sorted(set(names) - read)]


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
                bad.append(f"src/{rel}:{n}: std's clock panics on a page: use crate::clock::Instant (tokio's: std's on a node, paused in a test) or crate::clock::wall_us / wall_ms (or add the file to CLOCKS in tools/per_node_check.py with why)")
    return bad


# A walk of the SQL tree: a `use` of sqlparser's Visit or VisitMut trait, and a call of one of its free functions
# that isn't through `walk::` (src/walk.rs makes every walk, so the one compiled walk serves all of them).
WALK_USE = re.compile(r"(?m)^[ \t]*(?:pub(?:\([^)]*\))?[ \t]+)?use\s[^;]*;")
WALK_TRAIT = re.compile(r"\bVisit(?:Mut)?\b")
WALK_CALL = re.compile(r"(?<![\w:])((?:\w+::)*)(visit_(?:expressions|relations|statements)(?:_mut)?)\s*\(")
WALK_WHY = "walk the SQL tree through crate::walk (one compiled walk; sqlparser's own compiles the whole tree again for each visitor)"


def walks():
    """Walks of the SQL tree outside src/walk.rs: a `use` of Visit or VisitMut, or a free walk not through `walk::`."""
    bad = []
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if not f.endswith(".rs"):
                continue
            path = os.path.join(base, f)
            rel = os.path.relpath(path, SRC).replace(os.sep, "/")
            if rel == "walk.rs":
                continue
            text = open(path, encoding="utf-8").read()
            for m in WALK_USE.finditer(text):
                trait = WALK_TRAIT.search(m.group(0))
                if trait:
                    line = text.count("\n", 0, m.start() + trait.start()) + 1
                    bad.append(f"src/{rel}:{line}: {WALK_WHY}")
            for m in WALK_CALL.finditer(text):
                if not m.group(1).endswith("walk::"):
                    line = text.count("\n", 0, m.start(2)) + 1
                    bad.append(f"src/{rel}:{line}: {WALK_WHY}")
    return bad


DIALED = re.compile(r"(?<!fn )(?<![\w:])http\((?:&|node\b)|\bcluster::http\(")


def dialed():
    """A call between members around the node's dial: the nodes' HTTP client used outside `cluster::send_call`."""
    bad = []
    for base, _, files in os.walk(SRC):
        for f in sorted(files):
            if not f.endswith(".rs"):
                continue
            path = os.path.join(base, f)
            rel = os.path.relpath(path, SRC).replace(os.sep, "/")
            text = tests_cut(open(path, encoding="utf-8").read())
            # (cluster.rs's own `send_call`, the `Http` dial, is the one place that sends with it)
            allowed = range(*span(text, "async fn send_call(")) if rel == "cluster.rs" else range(0)
            for n, line in enumerate(text.split("\n"), 1):
                if DIALED.search(line.split("//", 1)[0]) and n not in allowed:
                    bad.append(f"src/{rel}:{n}: a call between members goes through the node's dial (crate::dial::call), not the nodes' HTTP client: the sim's members reach each other only by their dial")
    return bad


def span(text, start):
    """The 1-based lines from `start`'s line through the end of its item (the next line that is a lone `}`)."""
    at = text.find(start)
    if at < 0:
        return (0, 0)
    first = text.count("\n", 0, at) + 1
    lines = text.split("\n")
    last = next((i for i in range(first, len(lines) + 1) if lines[i - 1] == "}"), len(lines))
    return (first, last + 1)


def poisoned(found):
    """A process's lock read with `.unwrap()`: a library's node restarting in place unwinds through
    whatever lock it holds (`Lifecycle::restart`), and the next start must still read it."""
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
    more = [f"PROCESS names {k}, which src/ no longer has: take it out" for k in stale] + leaks() + settings() + clocks() + poisoned(found) + walks() + dialed()
    bad += more
    for b in bad if not args.list else more:
        print(f"FAIL {b}")
    print(f'{{"statics": {len(found)}, "failed": {len(bad)}, "ok": {str(not bad).lower()}}}')
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
