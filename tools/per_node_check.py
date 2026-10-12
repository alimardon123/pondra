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
or `panics::task`), and `std::env::set_var` outside the program's own files (the environment is
the process's: the next node, and every child process, would read it).

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
    "cluster.rs:CLIENT": "a client with no token (`http_bare`): only its keep-alive sockets",
    "console.rs:FILES": "the console's files, built into the binary",
    "console.rs:PAGE": "the console's page, built into the binary",
    "console.rs:SERVER": "the server's page, built into the binary",
    "copy.rs:MS": SETTING + " (PONDRA_LOAD_RETENTION_SECS)",
    "defaults.rs:CTX": "a session with no tables, for DEFAULT and CHECK expressions: nothing of a lake",
    "delta.rs:ONE": LOCK,
    "drain.rs:IN_FLIGHT": GAUGE,
    "ext.rs:CLIENT": "a client with no token, for other engines' REST catalogs: only its keep-alive sockets",
    "files.rs:SUBSTR": "a function's definition",
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
    "optimize.rs:NEXT": COUNTER,
    "panics.rs:NEXT": COUNTER,
    "panics.rs:R": "the queries' runtime, one a process (ADR-061 §4): the node's tasks on it end with the node, the runtime doesn't",
    "path.rs:HEADS": "regexes and constants",
    "path.rs:LATE": "regexes and constants",
    "pyfn.rs:SEEN": "SQL type names worked out by a session with nothing of a lake",
    "python.rs:BUSY": LOCK,
    "python.rs:CHOSEN": "the Python chosen in the console: the machine's, kept in python.txt beside the console's settings",
    "python.rs:EXE": SETTING + " (--python)",
    "python.rs:LOOKING": LOCK,
    "python.rs:PYTHON": "what the process's Python (EXE's or CHOSEN's) said of itself",
    "query.rs:JSON_TEXT": "a function's definition (pondra_json_text), the same for every lake",
    "reset.rs:USED": "what reset() sets back: every per-node static used since the process started",
    "runs.rs:WAKE": SIGNAL,
    "runs.rs:WRITTEN": SIGNAL,
    "server.rs:TURNS": SETTING + " (PONDRA_QUERIES, PONDRA_QUEUE_SECS; a stopped node's last queries give their turns back to it)",
    "service.rs:CONFIG": "the Windows service's file, set by its entry point once a process",
    "store.rs:ONE": LOCK,
    "store.rs:PARTS": SETTING + " (PONDRA_CORES, --memory-gb)",
    "store.rs:RAM": "the machine's memory, or its container's",
    "store.rs:SPARK": "Spark's functions, made against a session with nothing of a lake",
    "temp.rs:CHANGES": "a counter live queries wait on: it only grows, so a waiter never misses a change",
    "tier.rs:SLOTS": SETTING + " (tiering's slots, sized by --memory-gb)",
    "tls.rs:TLS": SETTING + " (PONDRA_TLS_*)",
    "users.rs:MAKING": LOCK,
}

# What a value nothing can change is made of: anything else (a crate's own type, a session, a client,
# a lock behind a name) is a node's or needs its reason in PROCESS.
PLAIN = {"std", "sync", "collections", "time", "path", "num", "borrow", "regex", "serde_json", "bytes",
         "LazyLock", "Option", "Vec", "Box", "Arc", "HashMap", "HashSet", "BTreeMap", "BTreeSet", "String",
         "str", "bool", "char", "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64",
         "i128", "isize", "f32", "f64", "Duration", "PathBuf", "Path", "Regex", "Value", "Bytes", "Cow",
         "NonZeroUsize", "static"}
# Where a node may set the environment: the program itself, before any node, and `pondra sql`'s own
# process, which runs itself again with it (`write.rs`'s from_cli). By file, with the reason.
SET_VAR = {
    "cli.rs": "the program's options, set before its one node starts",
    "write.rs": "`pondra sql`'s own process: its job and tries, kept when it runs itself again",
}
SPAWN = re.compile(r"\btokio::(?:task::)?spawn\(|Handle::(?:current|try_current)\(\)[^;]*?\.spawn\(")
REGEX = re.compile(r"^(?:std::sync::)?LazyLock<(?:regex::)?Regex>$")
STATIC = re.compile(r"\bstatic\s+(?:mut\s+)?([A-Z_][A-Z0-9_]*)\s*:")


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
                before = text[max(0, text.rfind("\n", 0, m.start() - 1) - 200):m.start()]
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
                if "set_var(" in code and f not in SET_VAR:
                    bad.append(f"src/{rel}:{n}: the environment is the process's, not the node's: keep it in a per_node! static (or add the file to SET_VAR in tools/per_node_check.py with why)")
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
    more = [f"PROCESS names {k}, which src/ no longer has: take it out" for k in stale] + leaks()
    bad += more
    for b in bad if not args.list else more:
        print(f"FAIL {b}")
    print(f'{{"statics": {len(found)}, "failed": {len(bad)}, "ok": {str(not bad).lower()}}}')
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
