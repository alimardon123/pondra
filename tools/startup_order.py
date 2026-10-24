#!/usr/bin/env python3
"""The code a node starts with, in one place (designs/l2-startup-layout.md).

A node's program is about 200 MB, and from its start to its first answer it runs about 10 MB of that, spread
through `.text`. The kernel maps code around every page a function touches, so an idle node kept 50–100 MB of
code resident and a cold start read about 70 MB from disk. A linker script that puts the functions a start runs
next to each other keeps that to what runs.

  trace   run the program (and with --lib, `pondra.connect()` over the library) under callgrind to its first
          answer and 5 s idle, and write the functions that ran to the list (tools/startup.txt)
  mark    mark the listed functions LLVM took for cold in a build made with `-C save-temps` (their sections are
          `.text.unlikely.<name>`)
  check   start the program, ask it its first question, and fail when its resident code passes --most-mb (a stale
          list, or a build that didn't link with the script); with --list, also say how much of the list landed in
          `.text.startup`

The list keeps working through dependency and toolchain bumps: crate hashes and LLVM's `.llvm.N` suffixes are
patterns. But it goes stale by a few MB a merge as functions are renamed and added, so CI traces it again
(.github/workflows/startup.yml, each night the code changed since the commit the list names): on a dist build left
unstripped, then marked from that build's objects (`-C save-temps`), and the new list rides the merge train. The
builds read it with `PONDRA_STARTUP_LAYOUT` set (tools/startup_layout.rs), and `check` on main's builds says when
it has gone stale anyway.
"""
import argparse, json, os, re, signal, socket, subprocess, sys, tempfile, time, urllib.request

FIRST = ["CREATE TABLE t (id INT, name TEXT)", "INSERT INTO t VALUES (1, 'a'), (2, 'b')", "SELECT count(*), max(name) FROM t"]


def pattern(name):
    """A traced name as the list keeps it: a crate's hash (`Cs<hash>_`, or legacy mangling's `17h<hash>E`) and
    LLVM's `.llvm.N` suffix as `*`, without callgrind's `'N` (a function's Nth turn in a recursion). Only those
    places get a `*`: lld matches every input section against every pattern, and a `*` after each name made a
    link of 200,000 functions 3.3x longer (206 s against 62)."""
    name = re.sub(r"\.llvm\.\d+$", ".llvm.*", re.sub(r"'\d+$", "", name))
    name = re.sub(r"17h[0-9a-f]{16}E", "17h*E", name)
    return re.sub(r"Cs[0-9A-Za-z]+_", "Cs*_", name)


def defined(path):
    """The function names an object defines (the list keeps only those: libc's and Python's ran too)."""
    out = subprocess.run(["nm", "--defined-only", path], capture_output=True, text=True, check=True).stdout
    return {f[2] for f in (l.split() for l in out.splitlines()) if len(f) == 3 and f[1] in "tTwW"}


def ran(callgrind_out):
    """Every function callgrind saw run: named where it first appears, as `fn=` or `cfn=` (name compression)."""
    names = set()
    for line in open(callgrind_out, errors="replace"):
        m = re.match(r"c?fn=\(\d+\) (.+)", line)
        if m:
            names.add(m.group(1).strip())
    return names


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def ask(port, q):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/sql", data=q.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=600) as r:
        return r.read()


def ready(port, proc, secs):
    end = time.time() + secs
    while time.time() < end:
        if proc.poll() is not None:
            sys.exit(f"the node ended before it was ready ({proc.returncode})")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/ready", timeout=5):
                return
        except OSError:
            time.sleep(0.2)
    sys.exit(f"the node wasn't ready in {secs} s")


def start(cmd, port, lake, secs):
    """The program serving a new lake, stopped by closing its standard input (invariant 46)."""
    p = subprocess.Popen(cmd + ["serve", "--dir", lake, "--addr", f"127.0.0.1:{port}", "--stop-with-stdin"],
                         stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ready(port, p, secs)
    return p


def through_library(lake, then):
    """A Python program that opens `lake` through the library (`pondra.connect()`), asks the first questions as a
    node's first client does, runs `then`, and closes it."""
    return (f"import sys, time, pondra\ndb = pondra.connect({lake!r})\n"
            + "".join(f"db.sql({q!r})" + (".rows()" if q.startswith("SELECT") else "") + "\n" for q in FIRST)
            + then + "db.close()\n")


def library_env(lib):
    """The environment that loads `lib` (`PONDRA_LIBRARY`) and this repository's Python client."""
    here = os.path.dirname(os.path.abspath(__file__))
    return dict(os.environ, PONDRA_LIBRARY=lib, PYTHONPATH=os.path.join(here, "..", "python"))


def trace(a):
    names = set()
    with tempfile.TemporaryDirectory(prefix="pondra-startup-") as tmp:
        grind = ["valgrind", "--tool=callgrind", "--demangle=no", "--dump-instr=no"]
        out, port = f"{tmp}/program.cg", free_port()
        p = start(grind + [f"--callgrind-out-file={out}", a.bin], port, f"{tmp}/lake", 600)
        for q in FIRST:
            ask(port, q)
        time.sleep(5)
        p.stdin.close()
        p.wait(600)
        names |= ran(out) & defined(a.bin)
        print(f"program: {len(names)} functions", file=sys.stderr)
        if a.lib:
            out = f"{tmp}/library.cg"
            code = through_library(tmp + "/lake2", "time.sleep(5)\n")
            subprocess.run(grind + [f"--callgrind-out-file={out}", sys.executable, "-c", code], env=library_env(a.lib), check=True, timeout=1200,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            lib = ran(out) & defined(a.lib)
            print(f"library: {len(lib)} functions", file=sys.stderr)
            names |= lib
    patterns = sorted({pattern(n) for n in names})
    with open(a.list, "w") as f:
        f.write("# The functions a node runs to its first answer and 5 s idle (tools/startup_order.py trace, then mark).\n")
        if at := commit():
            f.write(f"# Traced on {at}.\n") # (startup.yml traces it again once the code has changed since)
        f.writelines(p + "\n" for p in patterns)
    print(f"{len(patterns)} patterns in {a.list}", file=sys.stderr)


def commit():
    """The commit this checkout is at, or None outside a Git checkout."""
    here = os.path.dirname(os.path.abspath(__file__))
    r = subprocess.run(["git", "-C", here, "rev-parse", "HEAD"], capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else None


def lines(path):
    return [l.strip() for l in open(path) if l.strip() and not l.startswith("#")]


def patterns(path):
    """The list's patterns, without the mark of a cold one."""
    return [l.removeprefix("unlikely.") for l in lines(path)]


def mark(a):
    """Mark `unlikely.` the listed functions LLVM put in a cold section (`.text.unlikely.<name>`) in a build made
    with `-C save-temps`: the script places both sections for those alone. A pattern for every function's two
    sections made the link twice as long (86 s against 46), and without the cold ones 0.2 MB of them, spread
    through `.text`, kept 6–8 MB more resident."""
    cold = set()
    for root, _, files in os.walk(a.objects):
        for f in files:
            if f.endswith(".o"):
                out = subprocess.run(["readelf", "-SW", os.path.join(root, f)], capture_output=True, text=True).stdout
                cold |= {pattern(n) for n in re.findall(r"\.text\.unlikely\.(\S+)", out)}
    listed = patterns(a.list)
    head = [l for l in open(a.list) if l.startswith("#")]
    with open(a.list, "w") as f:
        f.writelines(head)
        f.writelines(("unlikely." if p in cold else "") + p + "\n" for p in listed)
    print(f"{sum(p in cold for p in listed)} of {len(listed)} listed functions are cold somewhere", file=sys.stderr)


def resident_code(pid, path):
    """MB of the program's own executable mappings resident in the process (what the kernel faulted in, read-ahead
    and fault-around included)."""
    path, kb, mine = os.path.realpath(path), 0, False
    for line in open(f"/proc/{pid}/smaps"):
        f = line.split()
        if re.match(r"[0-9a-f]+-[0-9a-f]+$", f[0]):
            mine = len(f) >= 6 and f[5] == path and "x" in f[1]
        elif mine and f[0] == "Rss:":
            kb += int(f[1])
    return kb / 1024


def placed(binary, names):
    """Of the listed functions in the binary, the MB inside `.text.startup` and the MB outside it."""
    sections = subprocess.run(["readelf", "-SW", binary], capture_output=True, text=True, check=True).stdout
    m = re.search(r"\.text\.startup\s+PROGBITS\s+([0-9a-f]+)\s+[0-9a-f]+\s+([0-9a-f]+)", sections)
    if not m:
        return 0.0, None
    lo, size = int(m.group(1), 16), int(m.group(2), 16)
    wanted = set(names)
    inside = outside = 0
    out = subprocess.run(["nm", "-S", "--defined-only", binary], capture_output=True, text=True, check=True).stdout
    for f in (l.split() for l in out.splitlines()):
        if len(f) != 4 or f[2] not in "tT":
            continue
        if pattern(f[3]) in wanted:
            n = int(f[1], 16)
            if lo <= int(f[0], 16) < lo + size:
                inside += n
            else:
                outside += n
    return inside / 2**20, outside / 2**20


def library_code(lib, lake):
    """MB of the library's code resident in a Python process that opened `lake` through it and asked the first
    questions, measured as the program's is (`resident_code`), while the process waits."""
    code = through_library(lake, "time.sleep(3)\nprint('ready', flush=True)\nsys.stdin.read()\n")
    p = subprocess.Popen([sys.executable, "-c", code], env=library_env(lib), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        if p.stdout.readline().strip() != "ready":
            sys.exit("the library didn't answer its first questions through pondra.connect()")
        return resident_code(p.pid, lib)
    finally:
        p.stdin.close()
        p.wait(60)


def check(a):
    with tempfile.TemporaryDirectory(prefix="pondra-startup-") as tmp:
        port = free_port()
        p = start([a.bin], port, f"{tmp}/lake", 60)
        for q in FIRST:
            ask(port, q)
        time.sleep(3)
        code = resident_code(p.pid, a.bin)
        rss = int(open(f"/proc/{p.pid}/status").read().split("VmRSS:")[1].split()[0]) / 1024
        p.stdin.close()
        p.wait(60)
        report = {"resident_code_mb": round(code, 1), "rss_mb": round(rss, 1)}
        if a.lib:
            report["library_resident_code_mb"] = round(library_code(a.lib, f"{tmp}/lake2"), 1)
    if a.list:
        inside, outside = placed(a.bin, patterns(a.list))
        report["listed_mb_in_startup"] = round(inside, 1)
        report["listed_mb_outside"] = None if outside is None else round(outside, 1)
    print(json.dumps(report))
    if a.most_mb and code > a.most_mb:
        sys.exit(f"resident code {code:.1f} MB at the first answer, over {a.most_mb} MB: "
                 "trace the list again (tools/startup_order.py trace), or the build didn't link with the script")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("trace")
    t.add_argument("--bin", required=True, help="an unstripped dist build of the program")
    t.add_argument("--lib", help="the library (pondra_lib) of the same build, traced through pondra.connect()")
    t.add_argument("--list", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "startup.txt"))
    m = sub.add_parser("mark")
    m.add_argument("--objects", required=True, help="a folder of the objects of a build made with -C save-temps")
    m.add_argument("--list", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "startup.txt"))
    c = sub.add_parser("check")
    c.add_argument("--bin", required=True)
    c.add_argument("--lib", help="also report the library's (pondra_lib) resident code, opened through pondra.connect()")
    c.add_argument("--most-mb", type=float)
    c.add_argument("--list", help="also say where the listed functions landed (needs an unstripped build)")
    a = ap.parse_args()
    {"trace": trace, "mark": mark, "check": check}[a.cmd](a)
