#!/usr/bin/env python3
"""What 1.0 keeps, read from a build and checked against a file (ADR-068 §3).

    python3 tools/surface.py --bin PATH [--write docs/surface.json]
    python3 tools/surface.py --bin PATH --check docs/surface.json [--base OLD.json]

--write reads the build's public surface (command line, HTTP routes, SQL functions and system
tables, SQLSTATEs, kinds, metrics, Python and JavaScript names) and writes it sorted, so diffs are
stable. --check reads it again and compares: a name the build has that the file lacks is "added", a
name the file has that the build lacks "gone". Until the file is promised (`--write --promise`, at
the freeze) they are notices, so the lanes adding names before then don't all edit one file; once it
is, either fails until the file says so. --base OLD also compares the file with main's copy: a name
OLD has and the file lacks was "removed since main", which fails once Cargo.toml is 1.x.

Standard library only. It starts the build's node on 127.0.0.1 (a free port in 9650-9699) with a
lake in a temporary folder, and stops it again.
"""
import argparse
import glob
import http.client
import json
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT = os.path.join(REPO, "docs", "surface.json")
SRC = sorted(glob.glob(os.path.join(REPO, "src", "*.rs")))
ROUTINES = "SELECT DISTINCT routine_name FROM information_schema.routines ORDER BY 1"
# information_schema.columns doesn't list pondra's own tables (system_tables says what is used instead).
SYSTEM_COLUMNS = ("SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
                  "WHERE table_schema = 'pondra' ORDER BY table_schema, table_name, ordinal_position")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # never send localhost through a proxy
METHODS = ("get", "post", "put", "delete", "patch", "head", "any")  # any = every method (ANY /db/{name})
# Runs inside `python3 -I`: the repo's python/ folder is put first, and nothing else from the cwd is read.
PY_CODE = r'''
import importlib, inspect, json, sys
sys.path.insert(0, sys.argv[1])
names, errors = set(), {}
for prefix, module in (("", "pondra"), ("spark.", "pondra.spark")):
    try:
        mod = importlib.import_module(module)
    except Exception as e:  # an optional package missing is recorded, not fatal
        errors[module] = f"{type(e).__name__}: {e}"[:300]
        continue
    for name in dir(mod):  # the package first, so its names don't depend on what spark imports
        if name.startswith("_"):
            continue
        names.add(prefix + name)
        obj = getattr(mod, name)
        if inspect.isclass(obj) and obj.__module__.startswith("pondra"):
            names.update(f"{prefix}{name}.{attr}" for attr in dir(obj) if not attr.startswith("_"))
print(json.dumps({"names": sorted(names), "errors": errors}))
'''
JS_CODE = r'''
const mod = await import(process.argv[1]);
const names = new Set();
for (const name of Object.keys(mod)) {
  names.add(name);
  const value = mod[name];
  if (typeof value === 'function' && value.prototype) {
    for (const m of Object.getOwnPropertyNames(value.prototype)) if (m !== 'constructor') names.add(`${name}.${m}`);
  }
}
console.log(JSON.stringify([...names].sort()));
'''


def fail(message):
    sys.exit(f"surface: {message}")


# ---- the command line: `pondra --help`, then each command's own --help, recursively ----

def help_of(bin_path, words):
    """The help text of `BIN words --help`. A help that won't print means the surface can't be checked."""
    cmd = [bin_path, *words, "--help"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env={**os.environ, "NO_COLOR": "1"})
    except subprocess.TimeoutExpired:
        fail(f"`{' '.join(cmd)}` took over 30 s")
    if r.returncode or not r.stdout.strip():
        fail(f"`{' '.join(cmd)}` exited {r.returncode}: {r.stderr.strip()[:300]}")
    return r.stdout


def clap_sections(text):
    """clap's help split at its headings ("Options:", "Commands:"...): {"options": [lines], ...}."""
    found, current = {}, None
    for line in text.splitlines():
        heading = re.match(r"^([A-Za-z][A-Za-z ]*):$", line)
        if heading:
            current = found.setdefault(heading.group(1).lower(), [])
        elif line and not line[0].isspace():
            current = None  # the usage line, or text after the sections
        elif current is not None:
            current.append(line)
    return found


def cli_entries(text):
    """(flags, positional names, subcommand names) of one command's help.

    Option lines start with two to eight spaces (descriptions are indented further); their names are
    the part before the first gap of two spaces, with `<VALUE>` placeholders left out.
    """
    sections = clap_sections(text)
    names, commands = [], []
    for line in sections.get("options", []):
        if re.match(r"^ {2,8}-", line):
            head = re.sub(r"<[^>]*>|\[[^\]]*\]", "", re.split(r"\s{2,}", line.strip())[0])
            names += [f for f in re.findall(r"--?[A-Za-z][\w-]*", head) if f not in ("-h", "--help", "-V", "--version")]
    for line in sections.get("arguments", []):
        m = re.match(r"^ {2}[\[<]([A-Za-z0-9_-]+)", line)
        if m:
            names.append(m.group(1))
    for line in sections.get("commands", sections.get("subcommands", [])):
        m = re.match(r"^ {2}([a-z][a-z0-9-]*)(\s|$)", line)
        if m and m.group(1) != "help":
            commands.append(m.group(1))
    return names, commands


def collect_cli(bin_path):
    cli = {}

    def walk(words):
        names, commands = cli_entries(help_of(bin_path, words))  # a leaf command has no subcommands to recurse into
        cli[" ".join(["pondra", *words])] = sorted(set(names))
        for command in commands:
            walk(words + [command])

    walk([])
    return cli


# ---- HTTP routes: read from the source, the router's `.route("/path", get(a).post(b))` calls ----

def closing(text, start):
    """The index of the bracket that closes the one before `start`; string literals are skipped."""
    depth, quoted, escaped = 1, False, False
    for i in range(start, len(text)):
        ch = text[i]
        if quoted:
            quoted = not (ch == '"' and not escaped)
            escaped = ch == "\\" and not escaped
        elif ch == '"':
            quoted = True
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                return i
    fail("a `.route(` without its closing parenthesis")


def top_methods(handlers):
    """The HTTP methods a route's handlers name: calls outside brackets (`get(a).post(b)`), not the calls
    a handler's own code makes. Brackets' insides are blanked, so only the top-level names remain."""
    flat, depth = [], 0
    for ch in handlers:
        if ch in "([{":
            flat.append(ch if depth == 0 else " ")
            depth += 1
        elif ch in ")]}":
            depth -= 1
            flat.append(" ")
        else:
            flat.append(ch if depth == 0 else " ")
    return sorted({m.upper() for m in re.findall(r"\b(%s)\(" % "|".join(METHODS), "".join(flat))})


def route_path(text, args):
    """The path of a route's arguments and what follows it. A literal, or `&format!("{t}/x")` where the same
    file says `let t = "…"` (Delta Sharing's routes); anything else gives None."""
    lit = re.match(r'\s*"([^"]+)"\s*,', args)
    if lit:
        return lit.group(1), args[lit.end():]
    fmt = re.match(r'\s*&format!\("\{(\w+)\}([^"]*)"\)\s*,', args)
    base = fmt and re.search(rf'let {fmt.group(1)} = "([^"]+)"', text)
    if base:
        return base.group(1) + fmt.group(2), args[fmt.end():]
    return None, args


def routes_in(text):
    """(path, methods) for each `.route(...)` in one source file; the path is None when it can't be read."""
    for m in re.finditer(r"\.route\(", text):
        path, rest = route_path(text, text[m.end():closing(text, m.end())])
        yield (path, top_methods(rest)) if path else (None, [])


def sources():
    return [open(path, encoding="utf-8", errors="replace").read() for path in SRC]


def collect_http():
    routes = set()
    for text in sources():
        for path, methods in routes_in(text):
            if path is None:
                fail("a `.route(` whose path isn't a string literal: the surface can't read it")
            if path.startswith("/cluster/"):  # between nodes only, not part of what users call (ADR-068 §2)
                continue
            if not methods:
                fail(f"route {path} names no method this reads ({', '.join(METHODS)})")
            routes.update(f"{m} {path}" for m in methods)
    return sorted(routes)


# ---- the running node: SQL functions, system tables, kinds, metrics ----

def free_port():
    for port in range(9650, 9700):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                pass
    fail("no free port between 9650 and 9699")


def call(base, method, path, body=None):
    """(status, text) of one request; an error status is an answer, not an exception."""
    req = urllib.request.Request(base + path, data=body, method=method)
    try:
        with OPENER.open(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def sql(base, text):
    """The rows of one statement (a list of objects). A statement the node refuses raises RuntimeError."""
    status, body = call(base, "POST", "/sql", text.encode())
    if status != 200:
        raise RuntimeError(f"{text[:60]!r} answered {status}: {body[:200]}")
    return json.loads(body)


def get(base, path):
    status, body = call(base, "GET", path)
    if status != 200:
        raise RuntimeError(f"GET {path} answered {status}: {body[:200]}")
    return body


def system_tables(base):
    """pondra's own tables and their columns as `col TYPE`, in column order (columns only grow at the end).

    information_schema.columns lists none of them and pondra.tables lists only the lake's own tables, so
    the names come from the source's `pondra.<name>` strings and each is asked with DESCRIBE: the build
    answers for the ones that are tables and refuses the rest (functions, file names, words).
    """
    found = {}
    for r in sql(base, SYSTEM_COLUMNS):
        found.setdefault(f"{r['table_schema']}.{r['table_name']}", []).append(f"{r['column_name']} {r['data_type']}")
    names = sorted(set(re.findall(r"\bpondra\.([a-z][a-z0-9_]*)", "\n".join(sources()))))
    for name in names:
        full = "pondra." + name
        if full in found:
            continue
        try:
            rows = sql(base, f"DESCRIBE {full}")
        except RuntimeError:
            continue
        if rows:
            found[full] = [f"{r['column_name']} {r['data_type']}" for r in rows]
    how = f"information_schema.columns, then DESCRIBE of the {len(names)} pondra.* names in src/*.rs: {len(found)} tables"
    return found, how


def wait_ready(proc, base):
    deadline = time.time() + 60
    while time.time() < deadline and proc.poll() is None:
        try:
            if call(base, "GET", "/ready")[0] == 200:
                return True
        except (OSError, http.client.HTTPException):
            pass  # not listening yet
        time.sleep(0.2)
    return False


def stop_node(proc):
    proc.terminate()
    try:
        proc.wait(30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def collect_node(bin_path):
    """Starts the node on a lake in a temporary folder, asks it, and stops it (always)."""
    how, out = {}, {}
    # PONDRA_* and AWS_* variables would make the node read a bucket or a token: a clean node is the same everywhere.
    env = {**{k: v for k, v in os.environ.items() if not k.startswith(("PONDRA_", "AWS_"))}, "NO_COLOR": "1"}
    with tempfile.TemporaryDirectory(prefix="surface-") as tmp:
        base = f"http://127.0.0.1:{free_port()}"
        log_path = os.path.join(tmp, "node.log")
        with open(log_path, "w") as log:
            proc = subprocess.Popen(
                [bin_path, "serve", "--lake", os.path.join(tmp, "lake"), "--cache-dir", os.path.join(tmp, "cache"),
                 "--addr", base.removeprefix("http://")],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=env, cwd=tmp)
        try:
            if not wait_ready(proc, base):
                with open(log_path) as f:
                    fail(f"the node didn't come up in 60 s; its log ends:\n{f.read()[-800:]}")
            try:
                rows = sql(base, ROUTINES)
            except RuntimeError:
                rows = []
            if rows:
                how["sql_functions"] = "information_schema.routines"
            else:
                rows = sql(base, "SHOW FUNCTIONS")
                how["sql_functions"] = "SHOW FUNCTIONS (information_schema.routines failed or was empty)"
            out["sql_functions"] = sorted({str(next(iter(r.values()))).lower() for r in rows})  # the name comes first
            out["system_tables"], how["system_tables"] = system_tables(base)
            out["kinds"] = sorted({k["kind"] for k in json.loads(get(base, "/kinds"))})
            out["metrics"] = sorted(set(re.findall(r"^# TYPE (\S+) ", get(base, "/metrics"), re.M)))
        finally:
            stop_node(proc)
    return out, how


# ---- SQLSTATEs, Python and JavaScript ----

def collect_sqlstates():
    with open(os.path.join(REPO, "src", "codes.rs"), encoding="utf-8") as f:
        return sorted(set(re.findall(r'"([0-9A-Z]{5})"', f.read())))


def collect_python():
    r = subprocess.run([sys.executable, "-I", "-c", PY_CODE, os.path.join(REPO, "python")],
                       capture_output=True, text=True, timeout=120, cwd=REPO)
    if r.returncode:
        return [], f"error: exited {r.returncode}: {r.stderr.strip()[-300:]}"
    data = json.loads(r.stdout)
    if data["errors"]:
        return data["names"], "error: " + "; ".join(f"{m}: {e}" for m, e in data["errors"].items())
    return data["names"], "ok"


def collect_javascript():
    if not shutil.which("node"):
        return [], "skipped: node is not installed"
    with open(os.path.join(REPO, "js", "package.json"), encoding="utf-8") as f:
        entry = pathlib.Path(REPO, "js", json.load(f)["main"]).as_uri()
    r = subprocess.run(["node", "--input-type=module", "-e", JS_CODE, entry],
                       capture_output=True, text=True, timeout=60)
    if r.returncode:
        return [], f"error: {r.stderr.strip()[-300:]}"
    return json.loads(r.stdout), "ok"


# ---- collect, write, compare ----

def collect(bin_path):
    data = {"cli": collect_cli(bin_path), "http": collect_http(), "sqlstates": collect_sqlstates()}
    node, how = collect_node(bin_path)
    data.update(node)
    data["python"], how["python"] = collect_python()
    data["javascript"], how["javascript"] = collect_javascript()
    data["_how"] = how
    return data


def dump(data):
    """The file's text: keys and lists sorted, except system tables' columns (kept in their order)."""
    out = {}
    for key, value in data.items():
        if key.startswith("_") or key == "system_tables":
            out[key] = value
        elif isinstance(value, dict):
            out[key] = {k: sorted(v) for k, v in value.items()}
        else:
            out[key] = sorted(set(value))
    return json.dumps(out, indent=1, sort_keys=True) + "\n"


def names_of(value):
    """What a section promises, one string each: a list's items, a dict's keys and `key: item` lines."""
    if isinstance(value, dict):
        return set(value) | {f"{key}: {item}" for key, items in value.items() for item in items}
    return set(value)


def unread(data, section):
    """True when the section was skipped on the machine that wrote the data (node missing): not compared."""
    return data.get("_how", {}).get(section, "").startswith("skipped")


def load(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        fail(f"{path} doesn't exist: write it with --write")


def differences(have, want):
    """(added, gone) as `section: name` strings; a section either side skipped is left out."""
    added, gone = [], []
    for section in sorted(k for k in set(have) | set(want) if not k.startswith("_")):
        if unread(have, section) or unread(want, section):
            print(f"note: {section} not compared: skipped on this or the file's machine")
            continue
        h, w = names_of(have.get(section, [])), names_of(want.get(section, []))
        added += [f"{section}: {n}" for n in sorted(h - w)]
        gone += [f"{section}: {n}" for n in sorted(w - h)]
    return added, gone


def major_version():
    with open(os.path.join(REPO, "Cargo.toml"), encoding="utf-8") as f:
        m = re.search(r'^version = "(\d+)\.\d+\.\d+"', f.read(), re.M)
    if not m:
        fail("no version in Cargo.toml")
    return int(m.group(1))


def check_base(want, old_path):
    if not os.path.isfile(old_path) or os.path.getsize(old_path) == 0:
        print(f"note: {old_path} is missing or empty, so nothing is compared with main")
        return 0
    old = load(old_path)
    removed = differences(want, old)[1]  # names old has and want lacks: the "gone" the other way round
    if not removed:
        print("no name removed since main")
        return 0
    if major_version() >= 1:
        for line in removed:
            print(f"removed since main: {line}")
        print("a 1.x release keeps what 1.0 kept: deprecate it first (ADR-068 §4)")
        return 1
    for line in removed:
        print(f"notice: removed since main (0.x, allowed): {line}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Write or check the public surface of a pondra build (ADR-068 §3).")
    ap.add_argument("--bin", required=True, help="the pondra binary to read")
    ap.add_argument("--write", nargs="?", const=DEFAULT, metavar="FILE", help="write the surface (default docs/surface.json)")
    ap.add_argument("--check", metavar="FILE", help="compare the build with FILE; exit 1 on any difference")
    ap.add_argument("--base", metavar="OLD", help="with --check: main's copy; names it has and FILE lacks were removed")
    ap.add_argument("--promise", action="store_true", help="with --write: from now on, a name added or gone fails --check (the freeze)")
    args = ap.parse_args()
    if args.write and args.check:
        ap.error("--write and --check are separate")
    if args.base and not args.check:
        ap.error("--base goes with --check")
    if args.promise and not args.write:
        ap.error("--promise goes with --write")
    bin_path = os.path.abspath(args.bin)
    if not os.path.isfile(bin_path):
        fail(f"no binary at {bin_path}")
    try:
        data = collect(bin_path)
    except RuntimeError as e:
        fail(str(e))
    if args.write:
        kept = os.path.isfile(args.write) and load(args.write).get("_promised", False)  # (recording a name keeps the promise)
        data["_promised"] = bool(args.promise or kept)
        os.makedirs(os.path.dirname(os.path.abspath(args.write)), exist_ok=True)
        with open(args.write, "w", encoding="utf-8") as f:
            f.write(dump(data))
        print(f"wrote {args.write}: {sum(len(names_of(v)) for k, v in data.items() if not k.startswith('_'))} names")
        return 0
    if not args.check:
        print(dump(data), end="")
        return 0
    want = load(args.check)
    added, gone = differences(data, want)
    promised = want.get("_promised", False)
    for line in added:
        print(f"{'' if promised else 'notice: '}added: {line}")
    for line in gone:
        print(f"{'' if promised else 'notice: '}gone: {line}")
    if not promised and (added or gone):
        print(f"surface: {args.check} isn't promised yet (`--write --promise` at the freeze): these are notices")
        added, gone = [], []
    elif not added and not gone:
        print(f"surface: {args.check} matches the build")
    status = 1 if added or gone else 0
    if added:
        print(f"run `python3 tools/surface.py --bin {args.bin} --write docs/surface.json` to record them")
    if gone:
        print("a name 1.0 keeps can't go: restore it, or deprecate it first (ADR-068 §4)")
    if args.base:
        status = max(status, check_base(want, args.base))
    return status


if __name__ == "__main__":
    sys.exit(main())
