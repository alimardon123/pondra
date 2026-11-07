#!/usr/bin/env python3
"""The redesign's floors, checked (designs/redesign/redesign.md §1, §11).

Reads every module of src/, finds the modules it uses (`crate::name`, `use crate::{a, b::c}`),
and checks each use against tools/layers.txt: a module may use its own floor and the floors below
(base < catalog < engine < extensions < hosts); platform modules are reached only from hosts; an
extension uses no other extension's modules. Uses that break a rule are listed. With --baseline,
an edge not in tools/layers-baseline.txt fails, and so does one with more uses than the baseline counts, so main can't
add one while the old ones go. Comments and the insides of string literals are not code (`strip_comments`).

  python3 tools/layers.py                  # report: counts by rule, every edge
  python3 tools/layers.py --baseline       # CI: fail on an edge the baseline doesn't have, or more uses along one
  python3 tools/layers.py --write-baseline # after a step removes edges, record what is left
"""
import os, re, sys
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
MAP = os.path.join(ROOT, "tools", "layers.txt")
BASELINE = os.path.join(ROOT, "tools", "layers-baseline.txt")
ORDER = {"base": 0, "catalog": 1, "engine": 2, "extensions": 3, "hosts": 4}


def load_map():
    floors = {}
    for line in open(MAP):
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        name, where = line.split()
        floor, _, ext = where.partition(":")
        assert floor in ORDER or floor == "platform", f"{name}: unknown floor {floor}"
        floors[name] = (floor, ext or None)
    return floors


def modules():
    """Each top-level module and the files it is made of."""
    out = {}
    for entry in sorted(os.listdir(SRC)):
        path = os.path.join(SRC, entry)
        if entry.endswith(".rs"):
            out.setdefault(entry[:-3], []).append(path)
        elif os.path.isdir(path):
            for dirpath, _, names in os.walk(path):
                out.setdefault(entry, []).extend(os.path.join(dirpath, n) for n in names if n.endswith(".rs"))
    return out


def braces(text, start):
    """The text inside the braces opening at `start`."""
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[start + 1 : i]
    return ""


def top_names(group):
    """The first segment of each path in a `{a, b::c, d::{e, f}}` group."""
    names, depth, cur = [], 0, ""
    for ch in group:
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        elif ch == "," and depth == 0:
            names.append(cur)
            cur = ""
            continue
        if depth == 0:
            cur += ch
    names.append(cur)
    return [n.strip().split("::")[0].strip() for n in names if n.strip()]


def strip_comments(text):
    """The code alone: comments (`//`, and `/* */` nested as Rust nests them) and the insides of string and char literals
    (`"…"`, `r#"…"#`, `b"…"`, `'x'`) taken out, newlines kept. A `crate::` in a doc line or a message isn't a use, and a
    `/*` or `//` inside a string doesn't start a comment (`"s3://…"`, `"*/*"`)."""
    out, i, n = [], 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith("//", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif text.startswith("/*", i):
            depth, i = 1, i + 2
            while i < n and depth:
                if text.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif text.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    if text[i] == "\n":
                        out.append("\n")
                    i += 1
        elif c in "rbc" and not (i and (text[i - 1].isalnum() or text[i - 1] == "_")) and (m := re.match(r'(?:br|cr|r)(#*)"', text[i:])):
            close = '"' + m.group(1)
            j = text.find(close, i + m.end())
            j = n if j < 0 else j + len(close)
            out.append('""' + "\n" * text.count("\n", i, j))
            i = j
        elif c == '"' or (c in "bc" and text.startswith('"', i + 1) and not (i and (text[i - 1].isalnum() or text[i - 1] == "_"))):
            i += 1 if c == '"' else 2
            start = i
            while i < n and text[i] != '"':
                i += 2 if text[i] == "\\" else 1
            out.append('""' + "\n" * text.count("\n", start, i))
            i += 1
        elif c == "'":
            # a char literal ('x', '\'', '\u{1F600}'), or a lifetime or label ('a, 'static), left as it is
            m = re.match(r"'(?:\\(?:u\{[0-9a-fA-F_]*\}|x[0-9a-fA-F]{2}|.)|[^\\'\n])'", text[i:])
            if m:
                out.append("' '")
                i += m.end()
            else:
                out.append(c)
                i += 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def uses(files, known):
    found = Counter()
    for f in files:
        text = strip_comments(open(f, encoding="utf-8").read())
        for m in re.finditer(r"crate::(\{|\w+)", text):
            if m.group(1) == "{":
                for n in top_names(braces(text, m.start(1))):
                    if n in known:
                        found[n] += 1
            elif m.group(1) in known:
                found[m.group(1)] += 1
    return found


def broken(src, dst, floors):
    (sf, sx), (df, dx) = floors[src], floors[dst]
    if df == "platform":
        return None if sf in ("platform", "hosts") else "reaches a platform module (use a base trait)"
    if sf == "platform":
        return None if ORDER[df] == 0 else "platform uses a floor above base"
    if ORDER[df] > ORDER[sf]:
        return f"{sf} uses {df} (upward)"
    if sf == df == "extensions" and sx != dx:
        return "an extension uses another extension"
    return None


# What `strip_comments` must keep and take out, checked on every run so a change to it can't quietly pass a broken floor.
LEXED = (
    'let a = "s3://x/*"; crate::one(); /* crate::two /* nested */ crate::three */ crate::four; let b = r#"crate::five "q" "#;\n'
    "let c = '\"'; let d = '\\''; fn f<'a>(x: &'a str) {} crate::six; // crate::seven\n"
    'let e = b"crate::eight"; let g = "esc \\" crate::nine"; crate::ten',
    ["one", "four", "six", "ten"],
)


def main():
    text, want = LEXED
    got = [m.group(1) for m in re.finditer(r"crate::(\w+)", strip_comments(text))]
    assert got == want, f"strip_comments keeps {got}, not {want}"
    floors = load_map()
    mods = modules()
    missing = sorted(set(mods) - set(floors))
    if missing:
        sys.exit(f"tools/layers.txt has no floor for: {', '.join(missing)}")
    edges = {}
    for name, files in mods.items():
        for dst, n in uses(files, set(mods)).items():
            if dst != name:
                why = broken(name, dst, floors)
                if why:
                    edges[f"{name} -> {dst}"] = (why, n)
    if "--write-baseline" in sys.argv:
        with open(BASELINE, "w") as out:
            out.write("# Uses that break the floors, as main has them (tools/layers.py --write-baseline): each edge and how many\n")
            out.write("# `crate::` uses it has. Each redesign step removes some; nothing may add an edge or a use along one.\n")
            for e in sorted(edges):
                out.write(f"{e}  {edges[e][1]}\n")
        print(f"{len(edges)} edges ({sum(n for _, n in edges.values())} uses) written to tools/layers-baseline.txt")
        return
    if "--baseline" in sys.argv:
        known = {}
        for l in open(BASELINE):
            if l.strip() and not l.startswith("#"):
                edge, _, count = l.strip().rpartition("  ")
                known[edge or count] = int(count) if edge else None  # (a line without a count: any number of uses)
        new = sorted(set(edges) - set(known))
        more = sorted(e for e in set(edges) & set(known) if known[e] is not None and edges[e][1] > known[e])
        fewer = sum(1 for e in known if e not in edges or (known[e] is not None and edges[e][1] < known[e]))
        for e in new:
            print(f"new: {e}  ({edges[e][0]})")
        for e in more:
            print(f"more: {e}  {known[e]} -> {edges[e][1]} uses ({edges[e][0]})")
        print(f"{len(edges)} edges break the floors ({len(new)} new, {len(more)} with more uses, {fewer} gone or with fewer since the baseline)")
        if fewer:
            print("run tools/layers.py --write-baseline to record the ones gone or fewer")
        sys.exit(1 if new or more else 0)
    by_rule = Counter(why for why, _ in edges.values())
    lines = defaultdict(int)
    for name, files in mods.items():
        lines[floors[name][0]] += sum(sum(1 for _ in open(f, encoding="utf-8")) for f in files)
    print("floor        modules   lines")
    for fl in ["base", "platform", "catalog", "engine", "extensions", "hosts"]:
        print(f"{fl:<12} {sum(1 for v in floors.values() if v[0] == fl):>7} {lines[fl]:>7}")
    print(f"\n{len(edges)} module uses break the floors ({sum(n for _, n in edges.values())} references):")
    for why, n in by_rule.most_common():
        print(f"  {n:>4}  {why}")
    if "-v" in sys.argv:
        for e, (why, n) in sorted(edges.items(), key=lambda kv: (kv[1][0], kv[0])):
            print(f"{e:<40} {n:>4}  {why}")


if __name__ == "__main__":
    main()
