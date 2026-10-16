#!/usr/bin/env python3
"""The code a node starts with, in one place (designs/l2-startup-layout.md).

A node's program is about 200 MB, and from its start to its first answer it runs about 10 MB of that, spread
through `.text`. The kernel maps code around every page a function touches, so an idle node kept 50–100 MB of
code resident and a cold start read about 70 MB from disk. A linker script that puts the functions a start runs
next to each other keeps that to what runs.

  trace   run the program (and with --lib, `pondra.connect()` over the library) under callgrind to its first
          answer and 5 s idle, and write the functions that ran to the list (tools/startup.txt)
  script  write the linker script that puts the listed functions' sections in `.text.startup`, ahead of `.text`
  check   start the program, ask it its first question, and fail when its resident code passes --most-mb (a stale
          list, or a build that didn't link with the script); with --list, also say how much of the list landed in
          `.text.startup`

The list keeps working through dependency and toolchain bumps: crate hashes and LLVM's `.llvm.N` suffixes are
patterns. Trace it again once a round, on an unstripped dist build (`CARGO_PROFILE_DIST_STRIP=false`).
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
            here = os.path.dirname(os.path.abspath(__file__))
            code = (f"import time, pondra\ndb = pondra.connect({tmp + '/lake2'!r})\n"
                    + "".join(f"db.sql({q!r})" + (".rows()" if q.startswith("SELECT") else "") + "\n" for q in FIRST)
                    + "time.sleep(5)\ndb.close()\n")
            env = dict(os.environ, PONDRA_LIBRARY=a.lib, PYTHONPATH=os.path.join(here, "..", "python"))
            subprocess.run(grind + [f"--callgrind-out-file={out}", sys.executable, "-c", code], env=env, check=True, timeout=1200,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            lib = ran(out) & defined(a.lib)
            print(f"library: {len(lib)} functions", file=sys.stderr)
            names |= lib
    patterns = sorted({pattern(n) for n in names})
    with open(a.list, "w") as f:
        f.write("# The functions a node runs to its first answer and 5 s idle (tools/startup_order.py trace).\n")
        f.writelines(p + "\n" for p in patterns)
    print(f"{len(patterns)} patterns in {a.list}", file=sys.stderr)


def patterns(path):
    return [l.strip() for l in open(path) if l.strip() and not l.startswith("#")]


def script(a):
    """`.text.<name>` (a function's own section with -ffunction-sections, which rustc always uses) and LLVM's
    `.text.unlikely.<name>` for a cold one, into `.text.startup` before `.text`. zig's `cc` passes `-T` through to
    lld and refuses lld's ordering files, so the order has to be a script."""
    lines = ["/* Written by tools/startup_order.py script from tools/startup.txt. */", "SECTIONS {", "  .text.startup : {", "    *("]
    for p in patterns(a.list):
        lines += [f"      .text.{p}", f"      .text.unlikely.{p}"]
    lines += ["    )", "  }", "} INSERT BEFORE .text;"]
    sys.stdout.write("\n".join(lines) + "\n")


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
    wanted = [re.compile(re.escape(p).replace(r"\*", ".*") + "$") for p in names]
    exact = {p for p in names if "*" not in p}
    inside = outside = 0
    out = subprocess.run(["nm", "-S", "--defined-only", binary], capture_output=True, text=True, check=True).stdout
    for f in (l.split() for l in out.splitlines()):
        if len(f) != 4 or f[2] not in "tT":
            continue
        if f[3] in exact or any(w.match(f[3]) for w in wanted if "*" in w.pattern):
            n = int(f[1], 16)
            if lo <= int(f[0], 16) < lo + size:
                inside += n
            else:
                outside += n
    return inside / 2**20, outside / 2**20


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
    s = sub.add_parser("script")
    s.add_argument("--list", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "startup.txt"))
    c = sub.add_parser("check")
    c.add_argument("--bin", required=True)
    c.add_argument("--most-mb", type=float)
    c.add_argument("--list", help="also say where the listed functions landed (needs an unstripped build)")
    a = ap.parse_args()
    {"trace": trace, "script": script, "check": check}[a.cmd](a)
