#!/usr/bin/env python3
"""Who a scope is for, counted (11-08 audit M8; design v2.1 §5, step 6).

What a request may see is its principal's scope, read today from the task it runs in (`rights::scope()`,
`auth::current()`), and work no request asked for runs as the node's own (`auth::as_node`), whose scope is whole. Step 6
passes the asker's scope to planning as an argument, so each of these places is one to go. This counts them in src/
(outside `#[cfg(test)] mod`s, comments and strings), by file:

  reads  a read of the task's caller: `rights::scope()` or `auth::current()` outside auth.rs and rights.rs
  node   a place that runs work as the node's own: `as_node(` or `panics::own(` (a loop of the node's own), or that
         plans or lists as the node's own, `Principal::node()` (a view's run, the lake's own definitions a column's
         move rewrites, a count over every row), outside auth.rs and panics.rs, which define them. The baseline names
         each file's, so a new one shows in review.
  whole  a whole scope made by name: `Scope::everything()` outside rights.rs
  unscoped  a planning session made for the task's caller: a call of `query::session` or `query::session_at`, which
         read it, where `query::session_for` is given the scope (M8 piece 3b retires them)

With --baseline, a count above tools/scope-baseline.txt's, or a file the baseline doesn't name, fails: each may only fall.

  python3 tools/scope_check.py                  # report: every count
  python3 tools/scope_check.py --baseline       # CI: fail on a count that rose
  python3 tools/scope_check.py --write-baseline # after a change lowers one, record what is left
"""
import os, re, sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
BASELINE = os.path.join(ROOT, "tools", "scope-baseline.txt")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from layers import strip_comments  # noqa: E402

KINDS = {
    "reads": (re.compile(r"\b(?:rights::scope|auth::current)\(\)"), {"auth.rs", "rights.rs"}),
    "node": (re.compile(r"\b(?:as_node|panics::own)\(|\bPrincipal::node\(\)"), {"auth.rs", "panics.rs"}),
    "whole": (re.compile(r"\bScope::everything\(\)"), {"rights.rs"}),
}
WRAPPERS = ("session", "session_at")  # (query.rs's, which plan for the task's caller: `unscoped`)
QUALIFIED = re.compile(r"\bquery::(?:session|session_at)\(")
IMPORTED = re.compile(r"use crate::query::(?:\{([^}]*)\}|(\w+));")
DESCRIBED = dict({k: p.pattern for k, (p, _) in KINDS.items()}, unscoped="query::session( or query::session_at(")


def unscoped(rel, text):
    """The calls of query.rs's wrappers in `text`: by their path, or by the name a `use crate::query::…` brought in (in
    query.rs, by name), never a function of the same name of the file's own, nor the wrappers' own lines."""
    names = set(WRAPPERS) if rel == "query.rs" else set()
    for m in IMPORTED.finditer(text):
        names |= {w.strip() for w in (m.group(1) or m.group(2)).split(",")} & set(WRAPPERS)
    n = 0
    for line in text.split("\n"):
        if re.search(r"\bfn (?:session|session_at)\(", line):
            continue
        n += len(QUALIFIED.findall(line))
        n += sum(len(re.findall(rf"(?<![\w.:]){w}\(", line)) for w in names)
    return n


def code(path):
    text = open(path, encoding="utf-8").read()
    m = re.search(r"#\[cfg\((?:all\()?test\b[^\]]*\]\s*mod\s", text)  # (`#[cfg(test)] mod`, `#[cfg(all(test, …))] mod`)
    return strip_comments(text[: m.start()] if m else text)


def counts():
    out = Counter()
    for dirpath, _, names in os.walk(SRC):
        for name in names:
            if not name.endswith(".rs"):
                continue
            path = os.path.join(dirpath, name)
            rel = os.path.relpath(path, SRC).replace(os.sep, "/")
            text = code(path)
            for kind, (pattern, skip) in KINDS.items():
                if rel not in skip:
                    n = len(pattern.findall(text))
                    if n:
                        out[(kind, rel)] += n
            n = unscoped(rel, text)
            if n:
                out[("unscoped", rel)] += n
    return out


def load():
    base = Counter()
    for line in open(BASELINE):
        line = line.split("#", 1)[0].strip()
        if line:
            kind, rel, n = line.split()
            base[(kind, rel)] = int(n)
    return base


def main():
    now = counts()
    if "--write-baseline" in sys.argv:
        with open(BASELINE, "w") as f:
            f.write("# tools/scope_check.py's counts on main: each may only fall (`--write-baseline` after one does).\n")
            for (kind, rel), n in sorted(now.items()):
                f.write(f"{kind} {rel} {n}\n")
        print(f"wrote {len(now)} counts to {os.path.relpath(BASELINE, ROOT)}")
        return
    totals = Counter()
    for (kind, _), n in now.items():
        totals[kind] += n
    print(", ".join(f"{kind} {totals[kind]}" for kind in DESCRIBED))
    if "--baseline" not in sys.argv:
        for (kind, rel), n in sorted(now.items()):
            print(f"  {kind} {rel} {n}")
        return
    base = load()
    rose = [(k, r, n, base.get((k, r), 0)) for (k, r), n in sorted(now.items()) if n > base.get((k, r), 0)]
    for kind, rel, n, was in rose:
        print(f"src/{rel}: {n} {kind}, the baseline has {was}: {DESCRIBED[kind]} may only fall (tools/scope_check.py)")
    fell = sum(1 for key, n in base.items() if now.get(key, 0) < n)
    if fell:
        print(f"({fell} counts fell below the baseline: --write-baseline records them)")
    sys.exit(1 if rose else 0)


if __name__ == "__main__":
    main()
