#!/usr/bin/env python3
"""A soak leg's heap dumps, turned into a report: what each node process's live heap grew by.

  heap_report.py BIN DUMPS OUT [--jeprof PATH] [--keep 12]

  Per node process, an early heap dump (a fifth of the way in, past the warm-up) is diffed against
  its last one with jeprof: what grew, by call stack. The top of each report is printed; OUT keeps
  the reports, the dumps chosen and a copy of BIN. It is a report, not a check: it exits 0 always.
"""
import argparse, glob, os, re, shutil, subprocess, sys
from pathlib import Path

DUMP = re.compile(r"^(?P<prefix>.+)\.(?P<pid>\d+)\.(?P<seq>\d+)\.(?P<kind>[a-z])(?P<n>\d*)\.heap$")
TIMEOUT = 900  # seconds per jeprof run


def newest_image(dumps):
    """The newest run of a node: its interval dumps as (seq, name, mtime), sorted by seq, up to the
    first one older than the one before it. A node restarted in place starts its numbers again."""
    for i in range(1, len(dumps)):
        if dumps[i][2] < dumps[i - 1][2]:
            return dumps[:i]
    return dumps


def dumps_by_pid(folder):
    """{pid: (interval dumps sorted by seq, final dump or None)}, each dump as (seq, name, mtime)."""
    intervals, finals = {}, {}
    for name in sorted(os.listdir(folder)):
        m, path = DUMP.match(name), os.path.join(folder, name)
        if not m or not os.path.isfile(path):
            continue
        pid, dump = int(m["pid"]), (int(m["seq"]), name, os.path.getmtime(path))
        if m["kind"] != "f":  # the other kinds (m, u) are interval dumps too
            intervals.setdefault(pid, []).append(dump)
        elif pid not in finals or dump[2] > finals[pid][2]:
            finals[pid] = dump
    return {pid: (sorted(intervals.get(pid, [])), finals.get(pid))
            for pid in sorted(set(intervals) | set(finals))}


def run_jeprof(jeprof, args, path):
    """jeprof's stdout into path (stdout then stderr if it failed); True if it worked."""
    try:
        r = subprocess.run(["perl", jeprof, *args], capture_output=True, timeout=TIMEOUT,
                           encoding="utf-8", errors="replace")
        out, err, ok, why = r.stdout, r.stderr, r.returncode == 0, f"exit {r.returncode}"
    except subprocess.TimeoutExpired as e:  # its output comes back as bytes, or None
        out, err = (e.stdout or b"").decode(errors="replace"), (e.stderr or b"").decode(errors="replace")
        ok, why = False, f"timed out after {TIMEOUT} s"
    except OSError as e:  # no perl
        out, err, ok, why = "", str(e), False, "did not start"
    Path(path).write_text(out if ok else out + err, encoding="utf-8", errors="replace")
    if not ok:  # shown, and the report goes on: jeprof never makes this script fail
        print(f"  jeprof failed ({why}); the last lines of its stderr:")
        for line in err.splitlines()[-5:]:
            print("    " + line)
    return ok


def make_jeprof(out):
    """jeprof from jemalloc's template in the cargo registry (the build keeps no script): OUT/jeprof."""
    home = os.environ.get("CARGO_HOME") or os.path.expanduser("~/.cargo")
    found = sorted(glob.glob(os.path.join(home, "registry/src/*/tikv-jemalloc-sys-*/jemalloc/bin/jeprof.in")))
    if not found:
        print(f"no jeprof template under {home}: no reports")
        return None
    # The crate's folder is tikv-jemalloc-sys-<crate>+<jemalloc>-<git>: jemalloc's version follows the +.
    version = re.search(r"\+([^-]+)", found[-1].split(os.sep)[-4])[1]
    text = Path(found[-1]).read_text().replace("@jemalloc_version@", version)
    # (and skips Rust's allocator entries too, as it does C's: a stack then starts at what allocated)
    text = text.replace('$skip_regexp = "TCMalloc|^tcmalloc::";',
                        '$skip_regexp = "TCMalloc|^tcmalloc::|^_rjem_|^__r(ust|g|dl)_(alloc|realloc|alloc_zeroed)";')
    # (and names Rust's functions by their type: C++'s rule drops every <…>, so `<Hot as Clone>::clone`
    # was `::clone`; generics go, and `<T as Trait>::f` and `<T>::f` become `T::f`)
    text = text.replace("  while ($function =~ s/<[^<>]*>//g)  { }    # Remove template arguments\n",
                        "  while ($function =~ s/(\\w)<[^<>]*>/$1/g)  { }\n"
                        "  $function =~ s/^<([^<>\\s]+)(?: as [^<>]+)?>::/$1::/;\n")
    jeprof = Path(out, "jeprof")
    jeprof.write_text(text.replace("@JEMALLOC_PREFIX@", "_rjem_"))
    jeprof.chmod(0o755)
    return str(jeprof)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("bin", help="the pondra binary the nodes ran (unstripped)")
    ap.add_argument("dumps", help="the folder of heap dumps")
    ap.add_argument("out", help="the output folder (made if missing)")
    ap.add_argument("--jeprof", help="a jeprof script (default: made from jemalloc's template)")
    ap.add_argument("--keep", type=int, default=12, help="interval dumps copied per node process")
    a = ap.parse_args()
    pids = dumps_by_pid(a.dumps) if os.path.isdir(a.dumps) else {}
    if not pids:
        print(f"no heap dumps in {a.dumps}")
        return 0
    Path(a.out, "dumps").mkdir(parents=True, exist_ok=True)
    shutil.copy2(a.bin, Path(a.out, "pondra"))
    jeprof = a.jeprof or make_jeprof(a.out)
    for pid, (intervals, final) in pids.items():
        kept = newest_image(intervals)
        left = len(intervals) - len(kept)
        note = f"; restarted in place: {left} older dumps left out" if left else ""
        # The copies: evenly spaced over the newest run, with its base (a fifth in) and its last.
        picks = ({round(i * (len(kept) - 1) / max(a.keep - 1, 1)) for i in range(a.keep)}
                 | {len(kept) // 5, len(kept) - 1}) if kept else set()
        for name in [kept[i][1] for i in sorted(picks)] + ([final[1]] if final else []):
            shutil.copy2(os.path.join(a.dumps, name), Path(a.out, "dumps"))
        if len(kept) < 2:
            print(f"pid {pid}: {len(kept)} interval dump(s), too few for a report{note}")
            continue
        base, last, t0 = kept[len(kept) // 5], kept[-1], kept[0][2]
        at = lambda d: round((d[2] - t0) / 60)
        print(f"pid {pid}: {len(kept)} dumps over {at(last)} min (base #{base[0]} at {at(base)} min, "
              f"last #{last[0]} at {at(last)} min){note}")
        if jeprof:
            base_path, last_path = (os.path.join(a.dumps, d[1]) for d in (base, last))
            growth = Path(a.out, f"growth-{pid}.txt")
            if run_jeprof(jeprof, ["--text", f"--base={base_path}", a.bin, last_path], growth):
                for line in growth.read_text(encoding="utf-8", errors="replace").splitlines()[:30]:
                    print("  " + line)
                run_jeprof(jeprof, ["--text", a.bin, last_path], Path(a.out, f"live-{pid}.txt"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
