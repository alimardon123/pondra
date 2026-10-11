#!/usr/bin/env python3
"""Checks for a node as an object (ADR-061).

  node_check.py [--bin target/release/pondra] [--port 9690]

a. "a node stopped once it serves gives its term back: the next leads at once": a node makes a lake
   and is stopped by closing its standard input; a second node serves it, is stopped the same way and
   must exit 0; a third is started on the folder and must be ready in under 5 s (a joining node waits
   out a 30 s lease otherwise).
b. "a node stopped while it starts gives its term back too": a node is stopped as soon as its trace
   says `start: the lease` (its term taken, its lake not yet open). It must exit 0 without ever
   printing `serving`, and the next node on the folder must be ready in under 5 s. A try that serves
   before the stop lands is made again on a new folder, up to five times.

Each node has a temporary folder of its own (TMPDIR), as on a machine of its own. Sharing one, a
term's lock file shows the next node that the stopped one is gone, and neither check could tell a
term given back from one kept.

Only the standard library. Every node is this script's child: it is stopped by its standard input,
and killed by its own pid if that doesn't end it. No other process is touched.
"""
import argparse, itertools, os, shutil, subprocess, sys, tempfile, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # (this machine's own node: never a proxy)
WITHIN = 5.0  # seconds: a node ready this soon after another stopped got its term back
children, logs, folders = [], [], []  # the nodes, their log files and the folders this run made


def fresh(name):
    """A new folder for one check's lake (its own folder, so no lake is shared between checks)."""
    work = tempfile.mkdtemp(prefix=f"node-check-{name}-")
    folders.append(work)
    return os.path.join(work, "lake")


def launch(bin_, lake, port, trace=False):
    """A node on `lake` at `port`, stopped by its standard input. With `trace`, its start steps go to
    its standard error (read by the caller); otherwise its log goes to a file beside the lake."""
    env = dict(os.environ)
    env.pop("PONDRA_TRACE_START", None)
    # A temporary folder of its own, as if each node ran on a machine of its own: a term's lock file
    # there would tell the next node at once that a stopped node is gone, whether or not it gave its
    # term back. Each node then sees only the bucket, as on separate machines (invariant 17).
    own = os.path.join(os.path.dirname(lake), f"tmp-{port}-{len(children)}")
    os.makedirs(own, exist_ok=True)
    env.update(TMPDIR=own, TMP=own, TEMP=own)
    err = subprocess.PIPE
    if trace:
        env["PONDRA_TRACE_START"] = "1"  # (a line per start step: `start: the lease 0.01 s`)
    else:
        log = open(os.path.join(os.path.dirname(lake), f"node-{port}.log"), "w")
        logs.append(log)
        err = log
    node = subprocess.Popen([bin_, "serve", "--lake", lake, "--addr", f"127.0.0.1:{port}", "--stop-with-stdin"],
                            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err, env=env)
    children.append(node)
    return node


def ready(port, secs):
    """Does GET /ready answer 200 within `secs`? (refused, or 503 while it starts, until then)"""
    t0 = time.time()
    while time.time() - t0 < secs:
        try:
            with direct.open(f"http://127.0.0.1:{port}/ready", timeout=2) as r:
                if r.status == 200:
                    return True
        except OSError:
            pass
        time.sleep(0.05)
    return False


def stop(node, secs=120):
    """Closes the node's standard input, as ending the program that started it does; returns its exit
    code. A node that doesn't end in `secs` is killed by its pid, and None is returned."""
    node.stdin.close()
    try:
        return node.wait(timeout=secs)
    except subprocess.TimeoutExpired:
        node.kill()
        node.wait()
        return None


def next_node_ready(bin_, lake, ports):
    """Starts the next node on `lake` and returns (ready in seconds, or None; its exit code)."""
    port = next(ports)
    t0 = time.time()
    node = launch(bin_, lake, port)
    up = ready(port, 60)
    secs = time.time() - t0
    code = stop(node)
    return (secs if up else None), code


def check_a(bin_, ports):
    lake = fresh("a")
    p1 = next(ports)
    first = launch(bin_, lake, p1)
    if not ready(p1, 60):
        return False, "the first node never answered /ready"
    if stop(first) != 0:
        return False, "the first node didn't exit 0 when stopped"
    port = next(ports)
    a = launch(bin_, lake, port)
    if not ready(port, 60):
        return False, "A never answered /ready"
    code = stop(a)
    if code != 0:
        return False, f"A exited with {code}, not 0"
    secs, code_b = next_node_ready(bin_, lake, ports)
    if secs is None:
        return False, "B never answered /ready in 60 s"
    if secs >= WITHIN:
        return False, f"B ready in {secs:.1f} s, not under {WITHIN:g} s: A didn't give its term back"
    return True, f"B ready in {secs:.1f} s"


def check_b(bin_, ports):
    for attempt in range(1, 6):
        lake = fresh(f"b{attempt}")
        port = next(ports)
        a = launch(bin_, lake, port, trace=True)
        closed = serving = False
        for raw in a.stderr:  # (read to the end: the node never waits on a full pipe)
            line = raw.decode(errors="replace")
            if not closed and line.startswith("start: the lease"):
                a.stdin.close()  # its term is taken, its lake not open yet: stop it now
                closed = True
            if line.startswith("pondra ") and " serving " in line:
                serving = True
        code = a.wait()
        if not closed:
            return False, f"A never printed `start: the lease` (exit {code})"
        if serving:
            continue  # the stop came after it served: try again, in a new folder
        if code != 0:
            return False, f"A stopped while it starts exited with {code}, not 0"
        secs, code_b = next_node_ready(bin_, lake, ports)
        if secs is None:
            return False, "B never answered /ready in 60 s"
        if secs >= WITHIN:
            return False, f"B ready in {secs:.1f} s, not under {WITHIN:g} s: A didn't give its term back"
        return True, f"A stopped at `start: the lease` exited 0 before serving (try {attempt}); B ready in {secs:.1f} s"
    return False, "A served before the stop landed, in five tries"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", default=os.environ.get("PONDRA_BIN") or os.path.join(HERE, "..", "target", "release", "pondra"))
    ap.add_argument("--port", type=int, default=9690, help="the first port; each node takes the next one")
    A = ap.parse_args()
    bin_ = os.path.abspath(A.bin)
    if not os.path.exists(bin_):
        print(f"FAIL no binary at {bin_} (--bin, PONDRA_BIN, or target/release/pondra)")
        sys.exit(1)
    ports = itertools.count(A.port)
    labels = [("a", "a node stopped once it serves gives its term back: the next leads at once", check_a),
              ("b", "a node stopped while it starts gives its term back too", check_b)]
    failed = False
    try:
        for _, label, fn in labels:
            ok, msg = fn(bin_, ports)
            print(f"{'ok' if ok else 'FAIL'} {label}: {msg}", flush=True)
            failed |= not ok
    finally:
        for node in children:
            if node.poll() is None:
                node.kill()  # (our own child, by its pid)
                node.wait()
        for log in logs:
            log.close()
        if not failed:
            for folder in folders:
                shutil.rmtree(folder, ignore_errors=True)
        else:
            print(f"(folders and node logs kept in {', '.join(folders)})", file=sys.stderr)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
