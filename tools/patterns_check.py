#!/usr/bin/env python3
"""MATCH_RECOGNIZE (ADR-073): the row-pattern matcher against a reference of our own.

  patterns_check.py --self-test                          the reference alone (no node needed)
  patterns_check.py [--port 9340] [--seed 1] [--queries 600]   sections 2-4 against a running node

The reference (`find_matches`) is a backtracking matcher over one partition, written from the standard's rules
(ISO/IEC 19075-5 §4.11-4.16, as Trino's MATCH_RECOGNIZE page states them) and kept apart from the Rust matcher. It
parses the pattern text itself and tries every alternative in preference order: alternation left first, greedy
repetition more iterations first, reluctant fewer, PERMUTE's orders in the order a b c, a c b, b a c, …. The first
complete match from a start wins; a start is the leftmost row the search has not passed. After an empty match the
next start is one row on; otherwise the skip rule decides. A skip to a symbol with no row in the match, or to the
match's first row, is an error (SQLSTATE 22000).

`reference_output` gives the rows per match: ONE ROW PER MATCH (the partition columns, then the measures), or ALL ROWS
PER MATCH in its three forms (SHOW EMPTY MATCHES, OMIT EMPTY MATCHES, WITH UNMATCHED ROWS): each row of each match
except the {- -} ones, with the measures RUNNING (the default: over the match's rows up to the row, excluded rows
included) or FINAL (over the whole match), and the columns in the order the standard gives them: the PARTITION BY
columns, the ORDER BY column, the measures, then the other columns. Column names and rows are compared with the node's
answer read from Arrow (?format=arrow), NULLs kept; the rows of ALL ROWS are compared as multisets.

Sections: 2 random queries (`random_checks`), 3 fixed examples (`fixed_checks`), 4 refusals (`refusal_checks`).
Section 5, the doors (the Postgres port, three nodes, views), is harness.py's `patterns` section.
"""
import argparse, http.client, itertools, json, random, re, sys, time
from collections import Counter
from datetime import date, datetime, timedelta

SYMS = ("a", "b", "c", "d")  # d has no DEFINE: always true


# ---------------------------------------------------------------- the pattern language: text to tree

def tokens(text):
    out, i = [], 0
    while i < len(text):
        c = text[i]
        if c.isspace():
            i += 1
        elif text[i:i + 2] == "{-":  # (before "{": a quantifier never starts with "-")
            out.append(("EXO", None)); i += 2
        elif text[i:i + 2] == "-}":
            out.append(("EXC", None)); i += 2
        elif c in "(),|^$*+?{}":
            out.append((c, None)); i += 1
        elif c.isdigit():
            j = i
            while j < len(text) and text[j].isdigit():
                j += 1
            out.append(("NUM", int(text[i:j]))); i = j
        elif c.isalpha() or c == "_":
            j = i
            while j < len(text) and (text[j].isalnum() or text[j] == "_"):
                j += 1
            out.append(("ID", text[i:j])); i = j
        else:
            raise ValueError(f"pattern: unexpected {c!r} at {i}")
    return out + [("END", None)]


class PatternParser:
    def __init__(self, text):
        self.toks, self.i = tokens(text), 0

    def peek(self):
        return self.toks[self.i][0]

    def take(self, kind=None):
        tok = self.toks[self.i]
        if kind is not None and tok[0] != kind:
            raise ValueError(f"pattern: expected {kind!r}, got {tok[0]!r}")
        self.i += 1
        return tok

    def parse(self):
        tree = self.alt()
        self.take("END")
        return tree

    def alt(self):
        terms = [self.seq()]
        while self.peek() == "|":
            self.take("|")
            terms.append(self.seq())
        return terms[0] if len(terms) == 1 else ("alt", terms)

    def seq(self):
        items = []
        while self.peek() not in ("|", ")", ",", "EXC", "END"):
            items.append(self.factor())
        if not items:
            raise ValueError("pattern: an empty alternative")
        return items[0] if len(items) == 1 else ("seq", items)

    def factor(self):
        node = self.primary()
        if self.peek() in ("*", "+", "?", "{"):
            lo, hi = self.quantifier()
            greedy = self.peek() != "?"  # (a "?" right after a quantifier makes it reluctant)
            if not greedy:
                self.take("?")
            node = ("rep", node, lo, hi, greedy)
        return node

    def quantifier(self):
        k = self.take()[0]
        if k == "*":
            return 0, None
        if k == "+":
            return 1, None
        if k == "?":
            return 0, 1
        lo = self.take("NUM")[1] if self.peek() == "NUM" else None  # {n} {n,} {,m} {n,m}
        if self.peek() != ",":
            if lo is None:
                raise ValueError("pattern: {} needs a count")
            self.take("}")
            return lo, lo
        self.take(",")
        hi = self.take("NUM")[1] if self.peek() == "NUM" else None
        self.take("}")
        lo = 0 if lo is None else lo
        if lo is None or (hi is not None and hi < lo):
            raise ValueError("pattern: a quantifier's count is backwards")
        return lo, hi

    def primary(self):
        kind, _ = self.toks[self.i]
        if kind == "ID":
            _, name = self.take()
            if name.upper() == "PERMUTE" and self.peek() == "(":
                self.take("(")
                items = [self.alt()]
                while self.peek() == ",":
                    self.take(",")
                    items.append(self.alt())
                self.take(")")
                return ("perm", items)
            return ("sym", name.lower())
        if kind == "(":
            self.take("(")
            node = self.alt()
            self.take(")")
            return node  # (a group is its inside)
        if kind == "EXO":
            self.take()
            node = self.alt()
            self.take("EXC")
            return ("excl", node)
        if kind in ("^", "$"):
            self.take()
            return ("anchor", "start" if kind == "^" else "end")
        raise ValueError(f"pattern: expected a symbol, a group or an anchor, got {kind!r}")


def parse_pattern(text):
    return PatternParser(text).parse()


def parse_skip(text):
    m = re.fullmatch(r"\s*(PAST\s+LAST\s+ROW|TO\s+NEXT\s+ROW|TO\s+(FIRST|LAST)\s+([A-Za-z_]\w*))\s*", text, re.I)
    if not m:
        raise ValueError(f"skip: {text!r}")
    words = m.group(1).upper().split()
    if words[0] == "PAST":
        return ("past_last",)
    if words[1] == "NEXT":
        return ("next_row",)
    return (words[1].lower(), m.group(3).lower())


def quant_text(lo, hi):
    if hi is None:
        return {0: "*", 1: "+"}.get(lo, f"{{{lo},}}")
    if (lo, hi) == (0, 1):
        return "?"
    if lo == hi:
        return f"{{{lo}}}"
    if lo == 0:
        return f"{{,{hi}}}"
    return f"{{{lo},{hi}}}"


def render(t):
    """A tree as pattern text; parse_pattern(render(t)) == t for every tree the generator makes."""
    k = t[0]
    if k == "sym":
        return t[1]
    if k == "anchor":
        return "^" if t[1] == "start" else "$"
    if k == "excl":
        return "{- " + render(t[1]) + " -}"
    if k == "perm":
        return "PERMUTE(" + ", ".join(render(x) for x in t[1]) + ")"
    if k == "seq":
        return " ".join(f"({render(x)})" if x[0] in ("seq", "alt") else render(x) for x in t[1])
    if k == "alt":
        return " | ".join(f"({render(x)})" if x[0] == "alt" else render(x) for x in t[1])
    body = t[1]
    inner = f"({render(body)})" if body[0] in ("seq", "alt", "rep") else render(body)
    return inner + quant_text(t[2], t[3]) + ("" if t[4] else "?")


def nullable(t):
    k = t[0]
    if k == "sym":
        return False
    if k == "anchor":
        return True
    if k == "excl":
        return nullable(t[1])
    if k == "seq":
        return all(nullable(x) for x in t[1])
    if k == "alt":
        return any(nullable(x) for x in t[1])
    if k == "perm":
        return all(nullable(x) for x in t[1])
    return t[2] == 0 or nullable(t[1])


def symbols_of(t):
    if t[0] == "sym":
        return {t[1]}
    if t[0] == "anchor":
        return set()
    if t[0] == "excl":
        return symbols_of(t[1])
    if t[0] == "rep":
        return symbols_of(t[1])
    return set().union(*(symbols_of(x) for x in t[1]))


def has_excl(t):
    if t[0] == "excl":
        return True
    if t[0] in ("sym", "anchor"):
        return False
    if t[0] == "rep":
        return has_excl(t[1])
    return any(has_excl(x) for x in t[1])


def strip_excl(t):
    """The same pattern without its {- -} (WITH UNMATCHED ROWS refuses them)."""
    if t[0] == "excl":
        return strip_excl(t[1])
    if t[0] in ("sym", "anchor"):
        return t
    if t[0] == "rep":
        return ("rep", strip_excl(t[1]), t[2], t[3], t[4])
    return (t[0], [strip_excl(x) for x in t[1]])


# ---------------------------------------------------------------- the reference matcher

class RefError(Exception):
    def __init__(self, sqlstate, message):
        super().__init__(message)
        self.sqlstate, self.message = sqlstate, message


class OutOfBudget(Exception):
    """The backtracking search took more steps than its budget: the pattern is regenerated, never compared."""


class Match:
    def __init__(self, number, start, end, items):
        self.number, self.start, self.end, self.items = number, start, end, items  # end is exclusive


def find_matches(truth, tree, skip, budget=200_000, within=None):
    """The matches of one partition, in order. truth[pos] maps a symbol to whether row pos may be it; a symbol
    with no DEFINE is absent from the map and always true. skip is ('past_last',), ('next_row',), ('first', sym)
    or ('last', sym). Items are (pos, symbol, excluded) in row order. within is None, or (times, interval) for WITHIN:
    a thread takes a row only if in_time allows it (a row too late fails its step)."""
    left = [budget]
    matches, frm, n = [], 0, len(truth)
    while frm < n:
        hit = None
        for start in range(frm, n):
            found = attempt(truth, tree, start, left, within)
            if found is not None:
                hit = (start,) + found
                break
        if hit is None:
            break
        start, end, items = hit
        number = len(matches) + 1
        matches.append(Match(number, start, end, items))
        frm = start + 1 if end == start else skip_to(skip, number, start, end, items)
    return matches


def sym_true(truth, pos, sym, start):
    """Whether row pos may be sym. A DEFINE that reads the match so far is a callable of the match's first row."""
    v = truth[pos].get(sym, True)
    return v(start) if callable(v) else v


def in_time(within, start, pos):
    """WITHIN: row pos may be in a match that starts at start only if time[start] >= time[pos] - interval, that is,
    the row is at most the interval after the first row. A NULL time is no time: the row can't be in such a match."""
    times, interval = within
    if times[pos] is None or times[start] is None:
        return False
    return times[start] >= times[pos] - interval


def attempt(truth, tree, start, left, within=None):
    """The most preferred complete match that begins at start: (end, items), or None."""
    n, path, result = len(truth), [], {}

    def m(node, pos, excl, k):  # does node match from pos, with k(end) then succeeding?
        left[0] -= 1
        if left[0] < 0:
            raise OutOfBudget()
        kind = node[0]
        if kind == "sym":
            if pos < n and sym_true(truth, pos, node[1], start) and (within is None or in_time(within, start, pos)):
                path.append((pos, node[1], excl))
                if k(pos + 1):
                    return True
                path.pop()
            return False
        if kind == "anchor":  # ^ is the partition's first row, $ the point after its last
            return (pos == 0 if node[1] == "start" else pos == n) and k(pos)
        if kind == "excl":
            return m(node[1], pos, True, k)
        if kind == "seq":
            return seq(node[1], 0, pos, excl, k)
        if kind == "alt":
            return any(m(x, pos, excl, k) for x in node[1])
        if kind == "perm":  # the orders of the items, in lexicographic order of their positions
            return any(seq([node[1][i] for i in order], 0, pos, excl, k)
                       for order in itertools.permutations(range(len(node[1]))))
        return rep(node[1], node[2], node[3], node[4], pos, excl, k, 0)

    def seq(items, i, pos, excl, k):
        if i == len(items):
            return k(pos)
        return m(items[i], pos, excl, lambda p: seq(items, i + 1, p, excl, k))

    def rep(body, lo, hi, greedy, pos, excl, k, count):
        def more():
            if hi is not None and count >= hi:
                return False
            # an iteration must take a row (a body that can match empty is out of scope: none is generated)
            return m(body, pos, excl, lambda p: p > pos and rep(body, lo, hi, greedy, p, excl, k, count + 1))

        def stop():
            return count >= lo and k(pos)

        if count < lo:
            return more()
        return (more() or stop()) if greedy else (stop() or more())

    def done(pos):
        result["end"] = pos
        return True

    if m(tree, start, False, done):
        return result["end"], list(path)
    return None


def skip_to(skip, number, start, end, items):
    if skip[0] == "past_last":
        return end
    if skip[0] == "next_row":
        return start + 1
    kind, sym = skip
    hits = [pos for pos, s, _ in items if s == sym]
    if not hits:
        raise RefError("22000", f"MATCH_RECOGNIZE: AFTER MATCH SKIP TO {kind.upper()} {sym}: {sym} has no row in match {number}")
    target = hits[0] if kind == "first" else hits[-1]
    if target == start:
        raise RefError("22000", f"MATCH_RECOGNIZE: AFTER MATCH SKIP TO {kind.upper()} {sym}: {sym} goes back to where match {number} started: skip to a later symbol, or PAST LAST ROW")
    return target


def truth_of(rows, define):
    """DEFINE's conditions per row: each a Python function of (the partition's rows, position); NULL is false."""
    return [{s: bool(f(rows, i)) for s, f in define.items()} for i in range(len(rows))]


def truth_of_match(rows, define):
    """DEFINE's conditions that read the match so far (Flink's example: B.price > A.price - 10, A being the match's first
    row): each a function of (rows, position, the match's first row), so each is a callable of that first row."""
    return [{s: (lambda start, f=f, i=i: bool(f(rows, i, start))) for s, f in define.items()} for i in range(len(rows))]


def partitions_of(rows, partition, order):
    """The rows split by the PARTITION BY column (None: one partition), each sorted by ORDER BY; a tie keeps the
    order the rows were inserted in, which is the order the lake's _row_id gives them."""
    groups = {}
    for r in rows:
        groups.setdefault(r[partition] if partition else None, []).append(r)
    return [sorted(g, key=lambda r: r[order]) for g in groups.values()]


# ---------------------------------------------------------------- measures and the output (ONE ROW, ALL ROWS)

# A measure is (alias, op, column, variable, qualifier). op: "mn" (MATCH_NUMBER()), "class" (CLASSIFIER()), "first"
# or "last" (FIRST or LAST of the column, of the variable's rows when there is one), "count" (count(*), or count(v.*)
# with a variable). The qualifier is "" (the default), "RUNNING" or "FINAL". ONE ROW PER MATCH gives each measure the
# whole match; ALL ROWS PER MATCH gives it the match's items up to the output row (RUNNING, the default) or the whole
# match (FINAL). Items are the match's (pos, symbol, excluded): excluded rows are in every measure.

def measure_text(item):
    """A measure as the SQL it is written as: `FINAL LAST(v.col) AS alias`, `count(*) AS alias`."""
    alias, op, col, var, qual = item
    if op == "mn":
        expr = "MATCH_NUMBER()"
    elif op == "class":
        expr = "CLASSIFIER()"
    elif op == "count":
        expr = f"count({var}.*)" if var else "count(*)"
    else:
        prefix = f"{var}." if var else ""
        expr = f"{'FIRST' if op == 'first' else 'LAST'}({prefix}{col})"
    return f"{qual + ' ' if qual else ''}{expr} AS {alias}"


def measure_value(m, rows, seen, op, col, var):
    """A measure over `seen`, the match's items it may see; an empty match has none."""
    if op == "mn":
        return m.number
    if op == "class":  # the last row's variable
        return seen[-1][1] if seen else None
    picked = [pos for pos, sym, _ in seen if var is None or sym == var]
    if op == "count":
        return len(picked)
    if not picked:
        return None
    return rows[picked[0] if op == "first" else picked[-1]][col]


def measure_row(m, rows, spec, k):
    """The values of spec's measures on output row k of match m (k None: ONE ROW PER MATCH's row, or an empty match's
    row, where every measure is over the match's rows, which for an empty match are none)."""
    out = []
    for alias, op, col, var, qual in spec:
        seen = m.items if (k is None or qual == "FINAL") else m.items[:k + 1]
        out.append(measure_value(m, rows, seen, op, col, var))
    return out


def reference_output(parts, tree, skip, spec, mode="one", pcols=(), ocol=None, rcols=(), interval=None):
    """(column names, rows as tuples in those names' order) for the query's rows per match. parts is (rows, truth) per
    partition, the rows sorted by ORDER BY, truth each row's DEFINE conditions (truth_of). mode is "one", or for ALL
    ROWS "show" (SHOW EMPTY MATCHES), "omit" (OMIT EMPTY MATCHES) or "unmatched" (WITH UNMATCHED ROWS). ALL ROWS gives
    the partition columns, the ORDER BY column, the measures, then the other columns (rcols). interval is WITHIN's
    (the ORDER BY column is then the time). Raises RefError or OutOfBudget as find_matches does."""
    aliases = [s[0] for s in spec]
    if mode == "one":
        names = list(pcols) + aliases
    else:
        names = list(pcols) + [ocol] + aliases + list(rcols)
    out = []
    for rows, truth in parts:
        if not rows:
            continue
        within = None if interval is None else ([r[ocol] for r in rows], interval)
        ms = find_matches(truth, tree, skip, within=within)
        if mode == "one":
            out += [tuple([rows[0][c] for c in pcols] + measure_row(m, rows, spec, None)) for m in ms]
            continue

        def row_out(pos, measures):
            return tuple([rows[pos][c] for c in pcols] + [rows[pos][ocol]] + measures + [rows[pos][c] for c in rcols])

        covered = set()
        for m in ms:
            if not m.items:  # an empty match: its row is where it was found (none under OMIT EMPTY MATCHES)
                covered.add(m.start)
                if mode != "omit":
                    out.append(row_out(m.start, measure_row(m, rows, spec, None)))
                continue
            for k, (pos, _, excluded) in enumerate(m.items):
                covered.add(pos)
                if not excluded:  # a {- -} row is in its match for every measure, but it is not given
                    out.append(row_out(pos, measure_row(m, rows, spec, k)))
        if mode == "unmatched":  # a row in no match is given once more, with every measure NULL
            out += [row_out(pos, [None] * len(spec)) for pos in range(len(rows)) if pos not in covered]
    return names, out


# ---------------------------------------------------------------- the node: one POST /sql

def sql_value(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, datetime):
        return f"TIMESTAMP '{v:%Y-%m-%d %H:%M:%S}'"
    return "'" + str(v).replace("'", "''") + "'"


def norm(v):
    return round(v, 6) if isinstance(v, float) else v


def arrow_rows(raw):
    """An Arrow IPC stream as (column names, rows as tuples in that order), NULLs kept."""
    import pyarrow as pa
    table = pa.ipc.open_stream(raw).read_all()
    # (timestamps as microseconds, which to_pylist gives as datetime, whatever unit the engine answers in)
    cols = [(c.cast(pa.timestamp("us")) if pa.types.is_timestamp(c.type) else c).to_pylist() for c in table.columns]
    return table.schema.names, [tuple(norm(v) for v in row) for row in zip(*cols)] if cols else []


def post_sql(port, sql, arrow=False):
    """One statement: {ok, value, code (SQLSTATE), msg, took (seconds)}. The value is the JSON answer, or with arrow
    (?format=arrow) the pair (column names, rows as tuples)."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    t0 = time.time()
    try:
        conn.request("POST", "/sql?format=arrow" if arrow else "/sql", sql.encode())
        resp = conn.getresponse()
        raw = resp.read()
    except (OSError, http.client.HTTPException) as e:
        return {"ok": False, "value": None, "code": None, "msg": f"no answer ({type(e).__name__}): {e}", "took": time.time() - t0}
    finally:
        conn.close()
    took = time.time() - t0
    body = raw.decode("utf-8", "replace")
    if resp.status == 200:
        if arrow:
            return {"ok": True, "value": arrow_rows(raw), "code": None, "msg": "", "took": took}
        try:
            value = json.loads(body)
        except ValueError:
            value = body
        return {"ok": True, "value": value, "code": None, "msg": "", "took": took}
    state = resp.getheader("x-pondra-sqlstate")
    try:
        j = json.loads(body)
    except ValueError:
        j = None
    if isinstance(j, dict):
        return {"ok": False, "value": None, "code": state or j.get("code"), "msg": str(j.get("error", body)), "took": took}
    return {"ok": False, "value": None, "code": state, "msg": body, "took": took}


def sql_exec(port, sql):
    r = post_sql(port, sql)
    if not r["ok"]:
        raise RuntimeError(f"{' '.join(sql.split())[:120]}: [{r['code']}] {r['msg'][:300]}")
    return r["value"]


# ---------------------------------------------------------------- fixed examples (section 3)

def canon(d):
    """A row as a sorted tuple with its NULLs left out: the node's JSON answer leaves a NULL column out of the object
    (`SELECT 1 AS a, NULL AS b` is `[{"a": 1}]`), so a missing key and a NULL are the same answer here."""
    return tuple(sorted((k, norm(v)) for k, v in d.items() if v is not None))


def pos_of(m, sym):
    return [pos for pos, s, _ in m.items if s == sym]


def span_of(m, rows):
    if not m.items:  # an empty match: FIRST and LAST are NULL, count(*) is 0
        return {"s": None, "e": None, "n": 0}
    return {"s": rows[m.start]["day"], "e": rows[m.end - 1]["day"], "n": len(m.items)}


def is_sym(x):
    return lambda rows, i: rows[i]["sym"] == x


def down_(rows, i):  # price below the row before (PREV(price)): the first row has no PREV, so false
    return i > 0 and rows[i]["price"] < rows[i - 1]["price"]


def up_(rows, i):
    return i > 0 and rows[i]["price"] > rows[i - 1]["price"]


PRICES = [12, 17, 19, 21, 25, 12, 15, 20, 24, 25, 19, 15, 25, 25, 14, 12, 14, 24, 23, 22]  # Oracle's ACME, days 1-20

# name -> (columns, rows in insertion order)
TABLES = {
    "ticker": ("symbol TEXT, day INT, price INT", [{"symbol": "ACME", "day": d + 1, "price": p} for d, p in enumerate(PRICES)]),
    "tq": ("day INT, price INT", [{"day": d + 1, "price": p} for d, p in enumerate([10, 12, 11, 15, 14, 13, 20])]),
    "tie": ("day INT, price INT", [{"day": 1, "price": 10}, {"day": 2, "price": 12}, {"day": 2, "price": 11},
                                   {"day": 3, "price": 15}, {"day": 4, "price": 14}]),
    "flags": ("day INT, flag BOOLEAN", [{"day": d, "flag": f} for d, f in zip(range(1, 6), [False, True, True, False, True])]),
    "tnr": ("day INT, flag BOOLEAN", [{"day": d, "flag": f} for d, f in zip(range(1, 6), [True, True, False, True, True])]),
    "runs": ("day INT, flag BOOLEAN", [{"day": d, "flag": f} for d, f in zip(range(1, 5), [True, True, False, True])]),
    "pm": ("day INT, sym TEXT", [{"day": d, "sym": s} for d, s in zip(range(1, 6), ["c", "b", "a", "b", "a"])]),
    "rl": ("day INT, sym TEXT", [{"day": d, "sym": s} for d, s in zip(range(1, 4), ["a", "a", "b"])]),
    "bl": ("day INT, sym TEXT", [{"day": d, "sym": "a"} for d in range(1, 6)]),
    "excl_t": ("day INT, sym TEXT", [{"day": d, "sym": s} for d, s in zip(range(1, 4), ["a", "b", "c"])]),
    # the ticker with a timestamp: day d is at d:00 on 2 March 2026, so the hours are the days (WITHIN counts hours)
    "tickts": ("symbol TEXT, day INT, ts TIMESTAMP, price INT",
               [{"symbol": "ACME", "day": d + 1, "ts": datetime(2026, 3, 2) + timedelta(hours=d + 1), "price": p}
                for d, p in enumerate(PRICES)]),
    # Flink's WITHIN example (its SQL docs): ACME every 20 minutes from 10:00 on 1 April 2011
    "flink_ticker": ("symbol TEXT, rowtime TIMESTAMP, price INT, tax INT",
                      [{"symbol": "ACME", "rowtime": datetime(2011, 4, 1, 10) + timedelta(minutes=20 * k), "price": p, "tax": t}
                       for k, (p, t) in enumerate(zip([20, 17, 18, 11, 14, 9, 15, 14, 24, 1, 19], [1, 2, 1, 3, 2, 1, 1, 2, 2, 2, 1]))]),
    # a column d and a symbol d (see the fixed example of that name)
    "dclash": ("d INT, price INT", [{"d": d, "price": p} for d, p in zip(range(1, 9), [10, 9, 8, 12, 11, 13, 12, 5])]),
}


def mr(table, partition, order, measures, pattern, define, skip, rows_per="ONE ROW PER MATCH", tail="", within=None):
    """The query. order None leaves ORDER BY out; within is WITHIN's text (INTERVAL '4' HOUR), written after PATTERN."""
    part = f"PARTITION BY {partition} " if partition else ""
    by = f"ORDER BY {order} " if order else ""
    wi = f" WITHIN {within}" if within else ""
    return (f"SELECT * FROM {table} MATCH_RECOGNIZE ({part}{by}MEASURES {measures} {rows_per} "
            f"AFTER MATCH SKIP {skip} PATTERN ({pattern}){wi} DEFINE {define}){tail}")


TICK_DEF = "down AS down.price < PREV(down.price), up AS up.price > PREV(up.price)"
TICK_FN = {"down": down_, "up": up_}


def m_ticker(m, rows):  # Oracle's ticker: the measures of the brief
    return {"symbol": rows[0]["symbol"], "start_day": rows[m.start]["day"],
            "bottom_day": rows[pos_of(m, "down")[-1]]["day"], "end_day": rows[pos_of(m, "up")[-1]]["day"]}


def m_ticker_stats(m, rows):
    down, up = pos_of(m, "down"), pos_of(m, "up")
    return {"symbol": rows[0]["symbol"], "start_day": rows[m.start]["day"],
            "bottom": rows[down[-1]]["price"],
            "before_bottom": rows[down[-1] - 1]["price"] if down[-1] > 0 else None,  # PREV(LAST(down.price), 1)
            "avg_down": sum(rows[p]["price"] for p in down) / len(down),
            "low_up": min(rows[p]["price"] for p in up), "high_up": max(rows[p]["price"] for p in up),
            "n": len(m.items)}


def m_ticker_final(m, rows):
    end = rows[pos_of(m, "up")[-1]]["day"]
    return {"symbol": rows[0]["symbol"], "start_day": rows[m.start]["day"], "f": end, "r": end}


def m_tie(m, rows):
    return {"start_day": rows[m.start]["day"], "bottom_day": rows[pos_of(m, "down")[-1]]["day"],
            "end_day": rows[pos_of(m, "up")[-1]]["day"]}


def m_tq(m, rows):
    return {"s": rows[m.start]["day"], "e": rows[m.end - 1]["day"], "nlo": len(pos_of(m, "lo"))}


def m_flags(m, rows):
    return {"mn": m.number, **span_of(m, rows)}


def m_pm(m, rows):
    return {**span_of(m, rows), "last_sym": m.items[-1][1]}


def m_excl(m, rows):
    return {**span_of(m, rows), "nb": len(pos_of(m, "b")), "fb": rows[pos_of(m, "b")[0]]["day"]}


def m_flink(m, rows):  # dropTime: the C row's time; dropDiff: A's price less C's (A is the match's first row)
    c = pos_of(m, "c")[-1]
    return {"symbol": rows[0]["symbol"], "dropTime": rows[c]["rowtime"], "dropDiff": rows[m.start]["price"] - rows[c]["price"]}


def m_clash(m, rows):  # FIRST(d) and LAST(d) are the column d; count(d.*) is the symbol d's rows
    return {"s": rows[m.start]["d"], "e": rows[m.end - 1]["d"], "nd": len(pos_of(m, "d"))}


def lo_(rows, i):  # price below the price two rows on (NEXT(price, 2)); no such row is NULL, so false
    return i + 2 < len(rows) and rows[i]["price"] < rows[i + 2]["price"]


def hi_(rows, i):  # price above the price two rows back (PREV(price, 2))
    return i - 2 >= 0 and rows[i]["price"] > rows[i - 2]["price"]


def flag_(rows, i):
    return rows[i]["flag"]


TICK_SKIP = "TO LAST up"

# The ALL ROWS measures: the ticker's (RUNNING, and FINAL where written), and the flags' and tnr's.
TICK_SPEC = (("m", "mn", None, None, ""), ("c", "class", None, None, ""), ("f", "first", "price", None, ""),
             ("d", "last", "price", "down", ""), ("fl", "last", "price", None, "FINAL"),
             ("n", "count", None, None, ""), ("fn", "count", None, None, "FINAL"))
TICK_ALL = ", ".join(measure_text(x) for x in TICK_SPEC)
FLAG_SPEC = (("mn", "mn", None, None, ""), ("s", "first", "day", None, ""), ("e", "last", "day", None, ""),
             ("n", "count", None, None, ""), ("fn", "count", None, None, "FINAL"))
FLAG_TEXT = ", ".join(measure_text(x) for x in FLAG_SPEC)
TNR_SPEC = FLAG_SPEC[:4]  # (no FINAL: the table is not a whole match's)
TNR_TEXT = ", ".join(measure_text(x) for x in TNR_SPEC)

# Each: the table, the query, the reference's inputs (partition, order, pattern, skip, DEFINE functions and the
# measures), and the answer written here by hand, in the query's order. Ticker answers are worked out in the comments.
# ALL ROWS entries (with `spec`) give `expect` as dicts in column order, the rows in the query's ORDER BY.
FIXED = [
    # Oracle's ticker, SKIP TO LAST up. Days 5-10 (25, 12, 15, 20, 24, 25): strt 5, down 6, up 7-10. Next start
    # is day 10 (25): down 11, 12 (19, 15), up 13 (25); day 14 is not above 25. Next start is day 14 (25): down
    # 15, 16 (14, 12), up 17, 18 (14, 24); day 19 is not above 24. Next start is day 18: down 19, 20 but no up.
    dict(name="Oracle's ticker, AFTER MATCH SKIP TO LAST up (ONE ROW PER MATCH)", table="ticker", partition="symbol",
         order="day", pattern="strt down+ up+", skip=TICK_SKIP, define=TICK_FN, measures=m_ticker,
         query=mr("ticker", "symbol", "day", "strt.day AS start_day, LAST(down.day) AS bottom_day, LAST(up.day) AS end_day",
                  "strt down+ up+", TICK_DEF, TICK_SKIP, tail=" ORDER BY start_day"),
         expect=[{"symbol": "ACME", "start_day": 5, "bottom_day": 6, "end_day": 10},
                 {"symbol": "ACME", "start_day": 10, "bottom_day": 12, "end_day": 13},
                 {"symbol": "ACME", "start_day": 14, "bottom_day": 16, "end_day": 18}]),
    # The same with PAST LAST ROW (by hand, not in Oracle's docs): the match 5-10 ends before day 11, so the next
    # start is day 11 (19): down 12 (15), up 13 (25), giving 11-13; then day 14 gives 14-18 as above, and day 19
    # has no up after its down.
    dict(name="Oracle's ticker, AFTER MATCH SKIP PAST LAST ROW", table="ticker", partition="symbol", order="day",
         pattern="strt down+ up+", skip="PAST LAST ROW", define=TICK_FN, measures=m_ticker,
         query=mr("ticker", "symbol", "day", "strt.day AS start_day, LAST(down.day) AS bottom_day, LAST(up.day) AS end_day",
                  "strt down+ up+", TICK_DEF, "PAST LAST ROW", tail=" ORDER BY start_day"),
         expect=[{"symbol": "ACME", "start_day": 5, "bottom_day": 6, "end_day": 10},
                 {"symbol": "ACME", "start_day": 11, "bottom_day": 12, "end_day": 13},
                 {"symbol": "ACME", "start_day": 14, "bottom_day": 16, "end_day": 18}]),
    # Measures of the three matches above (down and up as before). Match 1: 25 starts, down 12, PREV of it 25, up
    # 15-25, 6 rows. Match 2: down 19, 15 (LAST 15, PREV 19, avg 17), up 25, 4 rows. Match 3: down 14, 12 (LAST 12,
    # PREV 14, avg 13), up 14, 24, 5 rows.
    dict(name="ticker: PREV(LAST(down.price), 1), avg, min, max, count(*) in MEASURES", table="ticker", partition="symbol",
         order="day", pattern="strt down+ up+", skip=TICK_SKIP, define=TICK_FN, measures=m_ticker_stats,
         query=mr("ticker", "symbol", "day",
                  "strt.day AS start_day, LAST(down.price) AS bottom, PREV(LAST(down.price), 1) AS before_bottom, "
                  "avg(down.price) AS avg_down, min(up.price) AS low_up, max(up.price) AS high_up, count(*) AS n",
                  "strt down+ up+", TICK_DEF, TICK_SKIP, tail=" ORDER BY start_day"),
         expect=[{"symbol": "ACME", "start_day": 5, "bottom": 12, "before_bottom": 25, "avg_down": 12.0, "low_up": 15, "high_up": 25, "n": 6},
                 {"symbol": "ACME", "start_day": 10, "bottom": 15, "before_bottom": 19, "avg_down": 17.0, "low_up": 25, "high_up": 25, "n": 4},
                 {"symbol": "ACME", "start_day": 14, "bottom": 12, "before_bottom": 14, "avg_down": 13.0, "low_up": 14, "high_up": 24, "n": 5}]),
    # FINAL and RUNNING of the same last row: the same value in ONE ROW PER MATCH.
    dict(name="ticker: FINAL LAST(up.day) and RUNNING LAST(up.day) agree", table="ticker", partition="symbol", order="day",
         pattern="strt down+ up+", skip=TICK_SKIP, define=TICK_FN, measures=m_ticker_final,
         query=mr("ticker", "symbol", "day", "strt.day AS start_day, FINAL LAST(up.day) AS f, RUNNING LAST(up.day) AS r",
                  "strt down+ up+", TICK_DEF, TICK_SKIP, tail=" ORDER BY start_day"),
         expect=[{"symbol": "ACME", "start_day": 5, "f": 10, "r": 10},
                 {"symbol": "ACME", "start_day": 10, "f": 13, "r": 13},
                 {"symbol": "ACME", "start_day": 14, "f": 18, "r": 18}]),
    # PREV and NEXT with offsets in DEFINE, no PARTITION BY. Prices 10 12 11 15 14 13 20 on days 1-7. lo: the price
    # is below the price two rows on (true on days 1, 2, 3, 5); hi: above the price two rows back (days 3, 4, 5, 7).
    # Days 1-3 are lo, day 4 is hi: the match 1-4 (3 lo rows). Then day 5 is lo but day 6 is neither, so no more.
    # (With NEXT(price, 1) instead, the match would be days 3-4: the offset matters.)
    dict(name="PREV(price, 2) and NEXT(price, 2) in DEFINE, no PARTITION BY", table="tq", partition=None, order="day",
         pattern="lo+ hi", skip="PAST LAST ROW",
         define={"lo": lo_, "hi": hi_}, measures=m_tq,
         query=mr("tq", None, "day", "FIRST(day) AS s, LAST(day) AS e, count(lo.*) AS nlo",
                  "lo+ hi", "lo AS price < NEXT(price, 2), hi AS price > PREV(price, 2)", "PAST LAST ROW", tail=" ORDER BY s"),
         expect=[{"s": 1, "e": 4, "nlo": 3}]),
    # Two rows of day 2 (12, then 11, in insertion order): strt 1 (day 1, 10) fails; from day 2 (12): down is the
    # 11 (day 2 again), up is day 3 (15): the match gives start 2, bottom 2, end 3. In the other order (11 then 12)
    # there is no match at all, so the answer depends on the tie order, which is the insertion order (_row_id).
    dict(name="ties on the ORDER BY, no PARTITION BY: the same answer in 5 runs", table="tie", partition=None, order="day",
         pattern="strt down+ up+", skip="PAST LAST ROW", define=TICK_FN, measures=m_tie, runs=5,
         query=mr("tie", None, "day", "strt.day AS start_day, LAST(down.day) AS bottom_day, LAST(up.day) AS end_day",
                  "strt down+ up+", TICK_DEF, "PAST LAST ROW", tail=" ORDER BY start_day"),
         expect=[{"start_day": 2, "bottom_day": 2, "end_day": 3}]),
    # An empty match. Flags F T T F T on days 1-5: day 1 is an empty match (a* takes nothing); from day 2, a* takes
    # 2-3; day 4 is empty again; day 5 takes one row. Empty matches are shown (ONE ROW PER MATCH, as Trino says).
    dict(name="an empty match: PATTERN (a*)", table="flags", partition=None, order="day",
         pattern="a*", skip="PAST LAST ROW", define={"a": flag_}, measures=m_flags,
         query=mr("flags", None, "day", "MATCH_NUMBER() AS mn, FIRST(day) AS s, LAST(day) AS e, count(*) AS n",
                  "a*", "a AS flag", "PAST LAST ROW", tail=" ORDER BY mn"),
         expect=[{"mn": 1, "s": None, "e": None, "n": 0}, {"mn": 2, "s": 2, "e": 3, "n": 2},
                 {"mn": 3, "s": None, "e": None, "n": 0}, {"mn": 4, "s": 5, "e": 5, "n": 1}]),
    # ^ is the partition's first row: flags T T F T, so only days 1-2 match.
    dict(name="anchor: PATTERN (^a+)", table="runs", partition=None, order="day",
         pattern="^a+", skip="PAST LAST ROW", define={"a": flag_}, measures=span_of,
         query=mr("runs", None, "day", "FIRST(day) AS s, LAST(day) AS e, count(*) AS n",
                  "^a+", "a AS flag", "PAST LAST ROW", tail=" ORDER BY s"),
         expect=[{"s": 1, "e": 2, "n": 2}]),
    # $ is the partition's end: the last run must reach the last row (day 4).
    dict(name="anchor: PATTERN (a+$)", table="runs", partition=None, order="day",
         pattern="a+$", skip="PAST LAST ROW", define={"a": flag_}, measures=span_of,
         query=mr("runs", None, "day", "FIRST(day) AS s, LAST(day) AS e, count(*) AS n",
                  "a+$", "a AS flag", "PAST LAST ROW", tail=" ORDER BY s"),
         expect=[{"s": 4, "e": 4, "n": 1}]),
    # Days c b a b a. From day 1: the orders a b c, a c b, b a c, b c a, c a b fail, c b a matches days 1-3.
    # From day 4 nothing completes. (CLASSIFIER() is the last row's symbol.)
    dict(name="PERMUTE(a, b, c)", table="pm", partition=None, order="day",
         pattern="PERMUTE(a, b, c)", skip="PAST LAST ROW", define={"a": is_sym("a"), "b": is_sym("b"), "c": is_sym("c")},
         measures=m_pm,
         query=mr("pm", None, "day", "FIRST(day) AS s, LAST(day) AS e, CLASSIFIER() AS last_sym, count(*) AS n",
                  "PERMUTE(a, b, c)", "a AS sym = 'a', b AS sym = 'b', c AS sym = 'c'", "PAST LAST ROW", tail=" ORDER BY s"),
         expect=[{"s": 1, "e": 3, "last_sym": "a", "n": 3}]),
    # Reluctant a+?: the first match takes one a (day 1) and then b? finds no b, so day 1 alone; from day 2, a+?
    # takes day 2 and b? takes day 3. (A greedy a+ would be days 1-3.)
    dict(name="reluctant a+? b?", table="rl", partition=None, order="day",
         pattern="a+? b?", skip="PAST LAST ROW", define={"a": is_sym("a"), "b": is_sym("b")}, measures=span_of,
         query=mr("rl", None, "day", "FIRST(day) AS s, LAST(day) AS e, count(*) AS n",
                  "a+? b?", "a AS sym = 'a', b AS sym = 'b'", "PAST LAST ROW", tail=" ORDER BY s"),
         expect=[{"s": 1, "e": 1, "n": 1}, {"s": 2, "e": 3, "n": 2}]),
    # Five a rows: a{2,3} takes days 1-3 (greedy), then days 4-5 (two left).
    dict(name="a{2,3}", table="bl", partition=None, order="day",
         pattern="a{2,3}", skip="PAST LAST ROW", define={"a": is_sym("a")}, measures=span_of,
         query=mr("bl", None, "day", "FIRST(day) AS s, LAST(day) AS e, count(*) AS n",
                  "a{2,3}", "a AS sym = 'a'", "PAST LAST ROW", tail=" ORDER BY s"),
         expect=[{"s": 1, "e": 3, "n": 3}, {"s": 4, "e": 5, "n": 2}]),
    # An exclusion: its row is in the match, so ONE ROW PER MATCH's count(*) is 3 and count(b.*) is 1.
    dict(name="an exclusion {- b -} in ONE ROW PER MATCH: the measures count it", table="excl_t", partition=None, order="day",
         pattern="a {- b -} c", skip="PAST LAST ROW", define={"a": is_sym("a"), "b": is_sym("b"), "c": is_sym("c")},
         measures=m_excl,
         query=mr("excl_t", None, "day", "FIRST(day) AS s, LAST(day) AS e, count(*) AS n, count(b.*) AS nb, FIRST(b.day) AS fb",
                  "a {- b -} c", "a AS sym = 'a', b AS sym = 'b', c AS sym = 'c'", "PAST LAST ROW", tail=" ORDER BY s"),
         expect=[{"s": 1, "e": 3, "n": 3, "nb": 1, "fb": 2}]),
]

# ---- ALL ROWS PER MATCH, worked out by hand. Each rows-per-match entry below has `spec` (the measures), `mode`, the
# ORDER BY column `order`, the partition column(s) `pcols` and the other columns `rcols`; its expect is in the
# query's order. The reference compares its rows as a multiset; the node's answer must be in the query's order.

# Oracle's ticker, SKIP TO LAST up, ALL ROWS PER MATCH: matches 1 (days 5-10), 2 (10-13), 3 (14-18), as above. Every
# row of every match is given, in match order, with the measures as the row sees them (RUNNING unless FINAL):
#   m    the match (1, 2, 3)
#   c    CLASSIFIER(), the row's own variable (strt, down, up)
#   f    FIRST(price): the match's first row's price, 25 for each of the three (days 5, 10, 14)
#   d    LAST(down.price) up to the row: NULL before the first down row; match 1: 12 from day 6; match 2: 19 at day 11,
#        15 from day 12 on; match 3: 14 at day 15, 12 from day 16 on
#   fl   FINAL LAST(price): the match's last row's price: 25 (day 10), 25 (day 13), 24 (day 18)
#   n    count(*) up to the row: match 1 1..6, match 2 1..4, match 3 1..5
#   fn   FINAL count(*): match 1 6, match 2 4, match 3 5
# Day 10 is in match 1 (as its last up row) and in match 2 (as its first row), so it is given twice.
TICK_SHOW = [
    {"symbol": "ACME", "day": 5, "m": 1, "c": "strt", "f": 25, "d": None, "fl": 25, "n": 1, "fn": 6, "price": 25},
    {"symbol": "ACME", "day": 6, "m": 1, "c": "down", "f": 25, "d": 12, "fl": 25, "n": 2, "fn": 6, "price": 12},
    {"symbol": "ACME", "day": 7, "m": 1, "c": "up", "f": 25, "d": 12, "fl": 25, "n": 3, "fn": 6, "price": 15},
    {"symbol": "ACME", "day": 8, "m": 1, "c": "up", "f": 25, "d": 12, "fl": 25, "n": 4, "fn": 6, "price": 20},
    {"symbol": "ACME", "day": 9, "m": 1, "c": "up", "f": 25, "d": 12, "fl": 25, "n": 5, "fn": 6, "price": 24},
    {"symbol": "ACME", "day": 10, "m": 1, "c": "up", "f": 25, "d": 12, "fl": 25, "n": 6, "fn": 6, "price": 25},
    {"symbol": "ACME", "day": 10, "m": 2, "c": "strt", "f": 25, "d": None, "fl": 25, "n": 1, "fn": 4, "price": 25},
    {"symbol": "ACME", "day": 11, "m": 2, "c": "down", "f": 25, "d": 19, "fl": 25, "n": 2, "fn": 4, "price": 19},
    {"symbol": "ACME", "day": 12, "m": 2, "c": "down", "f": 25, "d": 15, "fl": 25, "n": 3, "fn": 4, "price": 15},
    {"symbol": "ACME", "day": 13, "m": 2, "c": "up", "f": 25, "d": 15, "fl": 25, "n": 4, "fn": 4, "price": 25},
    {"symbol": "ACME", "day": 14, "m": 3, "c": "strt", "f": 25, "d": None, "fl": 24, "n": 1, "fn": 5, "price": 25},
    {"symbol": "ACME", "day": 15, "m": 3, "c": "down", "f": 25, "d": 14, "fl": 24, "n": 2, "fn": 5, "price": 14},
    {"symbol": "ACME", "day": 16, "m": 3, "c": "down", "f": 25, "d": 12, "fl": 24, "n": 3, "fn": 5, "price": 12},
    {"symbol": "ACME", "day": 17, "m": 3, "c": "up", "f": 25, "d": 12, "fl": 24, "n": 4, "fn": 5, "price": 14},
    {"symbol": "ACME", "day": 18, "m": 3, "c": "up", "f": 25, "d": 12, "fl": 24, "n": 5, "fn": 5, "price": 24},
]
# The same with the down rows excluded: {- down+ -}. The down rows (days 6, 11, 12, 15, 16) are in the matches for
# every measure, so d, n and fn are as above; they are not given.
TICK_EXCL = [r for r in TICK_SHOW if (r["m"], r["day"]) not in {(1, 6), (2, 11), (2, 12), (3, 15), (3, 16)}]
# flags a*: F T T F T on days 1-5. Matches: an empty one at day 1 (m 1), days 2-3 (m 2), an empty one at day 4 (m 3),
# day 5 (m 4). SHOW EMPTY MATCHES gives the empty matches' rows (s, e NULL, n 0) and the others' rows: n and e
# running, fn (FINAL) 2 for both rows of match 2 and 1 for day 5.
FLAG_SHOW = [
    {"day": 1, "mn": 1, "s": None, "e": None, "n": 0, "fn": 0, "flag": False},
    {"day": 2, "mn": 2, "s": 2, "e": 2, "n": 1, "fn": 2, "flag": True},
    {"day": 3, "mn": 2, "s": 2, "e": 3, "n": 2, "fn": 2, "flag": True},
    {"day": 4, "mn": 3, "s": None, "e": None, "n": 0, "fn": 0, "flag": False},
    {"day": 5, "mn": 4, "s": 5, "e": 5, "n": 1, "fn": 1, "flag": True},
]
# OMIT EMPTY MATCHES drops the two empty matches' rows (days 1 and 4).
FLAG_OMIT = [FLAG_SHOW[1], FLAG_SHOW[2], FLAG_SHOW[4]]
# WITH UNMATCHED ROWS: every row is in a match or is an empty match's row (the empty ones are not also unmatched), so
# no row is given twice or NULL: the same five rows as SHOW.
FLAG_UNMATCHED = FLAG_SHOW
# TO NEXT ROW with a+ on T T F T T (days 1-5, the table tnr). The matches overlap: m 1 is days 1-2, m 2 day 2, m 3
# days 4-5, m 4 day 5 (the next start is one row on each time). Day 3 is in no match, so WITH UNMATCHED ROWS gives it
# once more, with every measure NULL (MATCH_NUMBER() too). Seven rows, ordered by day and then m.
TNR_UNMATCHED = [
    {"day": 1, "mn": 1, "s": 1, "e": 1, "n": 1, "flag": True},
    {"day": 2, "mn": 1, "s": 1, "e": 2, "n": 2, "flag": True},
    {"day": 2, "mn": 2, "s": 2, "e": 2, "n": 1, "flag": True},
    {"day": 3, "mn": None, "s": None, "e": None, "n": None, "flag": False},
    {"day": 4, "mn": 3, "s": 4, "e": 4, "n": 1, "flag": True},
    {"day": 5, "mn": 3, "s": 4, "e": 5, "n": 2, "flag": True},
    {"day": 5, "mn": 4, "s": 5, "e": 5, "n": 1, "flag": True},
]

FIXED += [
    dict(name="ALL ROWS PER MATCH: the ticker's measures, RUNNING and FINAL (day 10 in two matches)", table="ticker",
         partition="symbol", order="day", pattern="strt down+ up+", skip=TICK_SKIP, define=TICK_FN,
         mode="show", spec=TICK_SPEC, pcols=("symbol",), rcols=("price",),
         query=mr("ticker", "symbol", "day", TICK_ALL, "strt down+ up+", TICK_DEF, TICK_SKIP,
                  rows_per="ALL ROWS PER MATCH", tail=" ORDER BY m, day"),
         expect=TICK_SHOW),
    dict(name="ALL ROWS PER MATCH: the ticker with its down rows excluded, {- down+ -}", table="ticker",
         partition="symbol", order="day", pattern="strt {- down+ -} up+", skip=TICK_SKIP, define=TICK_FN,
         mode="show", spec=TICK_SPEC, pcols=("symbol",), rcols=("price",),
         query=mr("ticker", "symbol", "day", TICK_ALL, "strt {- down+ -} up+", TICK_DEF, TICK_SKIP,
                  rows_per="ALL ROWS PER MATCH", tail=" ORDER BY m, day"),
         expect=TICK_EXCL),
    dict(name="ALL ROWS PER MATCH SHOW EMPTY MATCHES: flags a*, the empty matches shown", table="flags", partition=None,
         order="day", pattern="a*", skip="PAST LAST ROW", define={"a": flag_}, mode="show", spec=FLAG_SPEC,
         pcols=(), rcols=("flag",),
         query=mr("flags", None, "day", FLAG_TEXT, "a*", "a AS flag", "PAST LAST ROW",
                  rows_per="ALL ROWS PER MATCH SHOW EMPTY MATCHES", tail=" ORDER BY day"),
         expect=FLAG_SHOW),
    dict(name="ALL ROWS PER MATCH OMIT EMPTY MATCHES: flags a*, the empty matches gone", table="flags", partition=None,
         order="day", pattern="a*", skip="PAST LAST ROW", define={"a": flag_}, mode="omit", spec=FLAG_SPEC,
         pcols=(), rcols=("flag",),
         query=mr("flags", None, "day", FLAG_TEXT, "a*", "a AS flag", "PAST LAST ROW",
                  rows_per="ALL ROWS PER MATCH OMIT EMPTY MATCHES", tail=" ORDER BY day"),
         expect=FLAG_OMIT),
    dict(name="ALL ROWS PER MATCH WITH UNMATCHED ROWS: flags a* (no row in no match: as SHOW)", table="flags", partition=None,
         order="day", pattern="a*", skip="PAST LAST ROW", define={"a": flag_}, mode="unmatched", spec=FLAG_SPEC,
         pcols=(), rcols=("flag",),
         query=mr("flags", None, "day", FLAG_TEXT, "a*", "a AS flag", "PAST LAST ROW",
                  rows_per="ALL ROWS PER MATCH WITH UNMATCHED ROWS", tail=" ORDER BY day"),
         expect=FLAG_UNMATCHED),
    dict(name="ALL ROWS PER MATCH (bare: SHOW by default): flags a*", table="flags", partition=None,
         order="day", pattern="a*", skip="PAST LAST ROW", define={"a": flag_}, mode="show", spec=FLAG_SPEC,
         pcols=(), rcols=("flag",),
         query=mr("flags", None, "day", FLAG_TEXT, "a*", "a AS flag", "PAST LAST ROW",
                  rows_per="ALL ROWS PER MATCH", tail=" ORDER BY day"),
         expect=FLAG_SHOW),
    dict(name="ALL ROWS PER MATCH WITH UNMATCHED ROWS under TO NEXT ROW: a+ on T T F T T", table="tnr", partition=None,
         order="day", pattern="a+", skip="TO NEXT ROW", define={"a": flag_}, mode="unmatched", spec=TNR_SPEC,
         pcols=(), rcols=("flag",),
         query=mr("tnr", None, "day", TNR_TEXT, "a+", "a AS flag", "TO NEXT ROW",
                  rows_per="ALL ROWS PER MATCH WITH UNMATCHED ROWS", tail=" ORDER BY day, mn"),
         expect=TNR_UNMATCHED),
]

# WITHIN (after PATTERN): every row of a match is at most the interval after the match's first row, by the first ORDER
# BY key (a timestamp here: tickts's day d is at d:00, so an hour is a day). Worked out by hand, SKIP TO LAST up:
# within 4 hours (INTERVAL '4' HOUR): start 5 (25) takes down 6 (12) and up 7, 8, 9 (9 is 4 hours on: allowed); up 10 is
# 5 hours on, too late, so the match is 5-9 (bottom 6). From 9 (24) the price doesn't fall to 10 (25). Start 10 takes
# down 11, 12 (15) and up 13 (25, 3 hours on); 14 is no rise: match 10-13 (bottom 12). Start 13 has no fall to 14.
# Start 14 takes down 15, 16 (12, 2 hours on) and up 17, 18 (24, 4 hours on): match 14-18 (bottom 16). From 18, down 19
# and 20 leave no up: no match.
# within 2 hours: start 5 takes down 6 and up 7 (2 hours on), and up 8 is 3 hours on: match 5-7 (bottom 6). Days 7-9 start
# no match (no fall). Start 10 takes down 11, 12 (15, 2 hours on), but up 13 is 3 hours on: dead. Start 11 takes down 12
# and up 13 (2 hours on): match 11-13 (bottom 12). Start 14 takes down 15, 16 (2 hours on), but up 17 is 3 hours on:
# dead, so the later start 15 wins: down 16, up 17 (2 hours on), and 18 is 3 hours on: match 15-17 (bottom 16). From 17
# there is no fall to 18; from 18, down 19 and 20 leave no up.
FIXED += [
    dict(name="WITHIN INTERVAL '4' HOUR on the ticker's timestamps: the match from day 5 is cut short, to day 9", table="tickts",
         partition="symbol", order="ts", pattern="strt down+ up+", skip=TICK_SKIP, define=TICK_FN, measures=m_ticker,
         within=timedelta(hours=4),
         query=mr("tickts", "symbol", "ts", "strt.day AS start_day, LAST(down.day) AS bottom_day, LAST(up.day) AS end_day",
                  "strt down+ up+", TICK_DEF, TICK_SKIP, tail=" ORDER BY start_day", within="INTERVAL '4' HOUR"),
         expect=[{"symbol": "ACME", "start_day": 5, "bottom_day": 6, "end_day": 9},
                 {"symbol": "ACME", "start_day": 10, "bottom_day": 12, "end_day": 13},
                 {"symbol": "ACME", "start_day": 14, "bottom_day": 16, "end_day": 18}]),
    dict(name="WITHIN INTERVAL '2' HOUR: a later start wins when the first one runs out of time", table="tickts",
         partition="symbol", order="ts", pattern="strt down+ up+", skip=TICK_SKIP, define=TICK_FN, measures=m_ticker,
         within=timedelta(hours=2),
         query=mr("tickts", "symbol", "ts", "strt.day AS start_day, LAST(down.day) AS bottom_day, LAST(up.day) AS end_day",
                  "strt down+ up+", TICK_DEF, TICK_SKIP, tail=" ORDER BY start_day", within="INTERVAL '2' HOUR"),
         expect=[{"symbol": "ACME", "start_day": 5, "bottom_day": 6, "end_day": 7},
                 {"symbol": "ACME", "start_day": 11, "bottom_day": 12, "end_day": 13},
                 {"symbol": "ACME", "start_day": 15, "bottom_day": 16, "end_day": 17}]),
    # Flink's example, worked by hand. A is the match's first row; B (above A's price less 10) and C (below it) are read
    # from the match so far, which the engine refuses (DEFINE reads the match so far), so this one is the reference's
    # answer and the binary's refusal. Starts 10:00 (20) to 11:40 (9) have no C in their hour: from 10:00, A less 10 is
    # 10, and the rows to 11:00 (17, 18, 11) are not below it; from 11:00 (11), A less 10 is 1, and none is below that;
    # the rest likewise. Start 12:00 (15): A less 10 is 5; 12:20 (14) and 12:40 (24) are B; 13:00 (1) is C, exactly an
    # hour on, which the bound includes. One row: 13:00, 15 - 1 = 14. From 13:20 (19) there is no C.
    dict(name="Flink's WITHIN example: a drop of 10 in an hour, the bound inclusive (A B* C, DEFINE reads A), one row",
         table="flink_ticker", partition="symbol", order="rowtime", pattern="A B* C", skip="PAST LAST ROW",
         define_start={"b": lambda rows, i, a: rows[i]["price"] > rows[a]["price"] - 10,
                       "c": lambda rows, i, a: rows[i]["price"] < rows[a]["price"] - 10},
         measures=m_flink, within=timedelta(hours=1),
         query=mr("flink_ticker", "symbol", "rowtime", "C.rowtime AS dropTime, A.price - C.price AS dropDiff", "A B* C",
                  "B AS B.price > A.price - 10, C AS C.price < A.price - 10", "PAST LAST ROW", tail=" ORDER BY dropTime",
                  within="INTERVAL '1' HOUR"),
         expect=[{"symbol": "ACME", "dropTime": datetime(2011, 4, 1, 13, 0), "dropDiff": 14}]),
    # A column named d and a symbol named d: ORDER BY the column, DEFINE the symbol, which reads its own row (PREV is the
    # row before). Prices on d 1-8: 10 9 8 12 11 13 12 5. d1 has no PREV, so it is no fall; d2 and d3 fall, d4 rises, d5
    # falls, d6 rises, d7 and d8 fall: runs d2-3, d5 and d7-8. FIRST(d) and LAST(d) read the column, count(d.*) the symbol.
    dict(name="a column d and a symbol d: ORDER BY d, DEFINE d AS d.price < PREV(d.price), the column and the symbol apart",
         table="dclash", partition=None, order="d", pattern="d+", skip="PAST LAST ROW", define={"d": down_}, measures=m_clash,
         query=mr("dclash", None, "d", "FIRST(d) AS s, LAST(d) AS e, count(d.*) AS nd", "d+", "d AS d.price < PREV(d.price)",
                  "PAST LAST ROW", tail=" ORDER BY s"),
         expect=[{"s": 2, "e": 3, "nd": 2}, {"s": 5, "e": 5, "nd": 1}, {"s": 7, "e": 8, "nd": 2}]),
]

# The ticker's ALL ROWS query (SHOW), for harness.py's doors and spreads.
ALL_ROWS_Q = next(ex["query"] for ex in FIXED if ex["name"].startswith("ALL ROWS PER MATCH: the ticker's measures"))


def reference_rows(ex):
    """ONE ROW entries: the reference's dicts, the measures function of each match. A WITHIN entry (`within`, an
    interval) limits each match by its ORDER BY time."""
    out = []
    for rows in partitions_of(TABLES[ex["table"]][1], ex.get("partition"), ex["order"]):
        within = None if ex.get("within") is None else ([r[ex["order"]] for r in rows], ex["within"])
        truth = truth_of_match(rows, ex["define_start"]) if "define_start" in ex else truth_of(rows, ex["define"])
        ms = find_matches(truth, parse_pattern(ex["pattern"]), parse_skip(ex["skip"]), within=within)
        out += [ex["measures"](m, rows) for m in ms]
    return out


def load_tables(port):
    for name, (cols, rows) in TABLES.items():
        names = [c.split()[0] for c in cols.split(", ")]
        sql_exec(port, f"DROP TABLE IF EXISTS {name}")
        sql_exec(port, f"CREATE TABLE {name} ({cols})")
        vals = ", ".join("(" + ", ".join(sql_value(r[c]) for c in names) + ")" for r in rows)
        sql_exec(port, f"INSERT INTO {name} VALUES {vals}")


# The fixed examples whose DEFINE reads another symbol's rows (the match so far): the reference's answer, and the engine's
# refusal (0A000) until the next PR makes DEFINE read the match so far. That PR flips these to the answer.
NEXT_PR_REFUSED = ("Flink's WITHIN example: a drop of 10 in an hour, the bound inclusive (A B* C, DEFINE reads A), one row",)


def fixed_checks(port, show=True):
    """Section 3 on the node: each example's query, run `runs` times, answers the column names written for it and
    its rows in the written order (NULLs as NULL, read from Arrow)."""
    load_tables(port)
    out = {}
    for ex in FIXED:
        if ex["name"] in NEXT_PR_REFUSED:
            r = post_sql(port, ex["query"])
            ok = (not r["ok"]) and r["code"] == "0A000" and "reads the match so far" in r["msg"].lower()
            out[ex["name"]] = ok
            if show:
                print(f"fixed: {'REFUSED until the next PR' if ok else 'FAIL'} {ex['name']}"
                      + ("" if ok else f" (got ok={r['ok']} [{r['code']}] {r['msg'][:200]})"))
            continue
        names = list(ex["expect"][0])
        want = [tuple(norm(d.get(n)) for n in names) for d in ex["expect"]]
        runs = [post_sql(port, ex["query"], arrow=True) for _ in range(ex.get("runs", 1))]

        def same(r):
            return r["ok"] and r["value"][0] == names and r["value"][1] == want
        ok = all(same(r) for r in runs)
        out[ex["name"]] = ok
        if not show:
            continue
        if ok:
            print(f"fixed: OK   {ex['name']} ({len(runs)} run(s), {runs[0]['took'] * 1000:.0f} ms)")
            continue
        bad = next(r for r in runs if not same(r))
        got = f"columns {bad['value'][0]}, rows {bad['value'][1]}" if bad["ok"] else f"[{bad['code']}] {bad['msg'][:300]}"
        print(f"fixed: FAIL {ex['name']}\n  query: {ex['query']}\n  want:  columns {names}, rows {want}\n  got:   {got}")
    return out


# ---------------------------------------------------------------- refusals (section 4)

TICK_Q = lambda **kw: mr("ticker", kw.get("partition", "symbol"), "day", kw.get("measures", "strt.day AS s"),
                         kw.get("pattern", "strt down+ up+"), kw.get("define", TICK_DEF), kw.get("skip", "PAST LAST ROW"),
                         rows_per=kw.get("rows_per", "ONE ROW PER MATCH"))

# (what it is, the statement, SQLSTATE, a word the message must contain)
REFUSED = [
    ("a DEFINE reading another symbol", TICK_Q(pattern="a b", define="b AS b.price < a.price", measures="COUNT(*) AS n"),
     "0A000", "reads the match so far"),
    ("a DEFINE of a symbol the PATTERN lacks", TICK_Q(pattern="strt down+", define="up AS up.price > PREV(up.price), down AS down.price < PREV(down.price)"),
     "42601", "names no symbol of PATTERN"),
    ("PARTITION BY an expression", TICK_Q(partition="upper(symbol)"), "0A000", "PARTITION BY takes columns"),
    ("a qualifier that isn't a variable", TICK_Q(measures="t.day AS s"), "42601", "isn't a pattern variable"),
    ("two variables in one aggregate", TICK_Q(measures="max(up.price - down.price) AS m"), "42601", "at once"),
    # (the match of strt down+ has no up: the alternative up+ is where the symbol is, so skip TO LAST up finds none)
    ("SKIP TO a symbol absent from a match", TICK_Q(pattern="strt down+ | up+", skip="TO LAST up"), "22000", "has no row in match"),
    ("a pattern past 10,000 states", TICK_Q(pattern="a{5000} b{5000} c{5000}", define="a AS price > 0, b AS price > 0, c AS price > 0",
                                             measures="count(*) AS n"), "54000", "10,000"),
    ("WITH UNMATCHED ROWS with {- -}", mr("ticker", "symbol", "day", "strt.day AS s", "strt {- down+ -} up+", TICK_DEF,
                                          "PAST LAST ROW", rows_per="ALL ROWS PER MATCH WITH UNMATCHED ROWS"),
     "42601", "WITH UNMATCHED ROWS can't go with"),
    ("ALL ROWS ordered by an expression", mr("ticker", "symbol", "day + 1", "strt.day AS s", "strt down+ up+", TICK_DEF,
                                             "PAST LAST ROW", rows_per="ALL ROWS PER MATCH"),
     "0A000", "gives the ORDER BY columns"),
    # FINAL goes before a navigation or an aggregate, as the standard has it (FINAL LAST(b.price)), and a DEFINE refuses it
    ("FINAL in a DEFINE", mr("ticker", "symbol", "day", "strt.day AS s", "strt down+ up+",
                             "down AS FINAL LAST(down.price) < 10, up AS up.price > PREV(up.price)", "PAST LAST ROW"),
     "42601", "FINAL is for MEASURES"),
    ("an aggregate's own ORDER BY in ALL ROWS", mr("ticker", "symbol", "day", "strt.day AS s, array_agg(price ORDER BY day) AS ps",
                                                   "strt down+ up+", TICK_DEF, "PAST LAST ROW", rows_per="ALL ROWS PER MATCH"),
     "0A000", "own ORDER BY"),
    # WITHIN's refusals, before planning; a measure named like a column of a lake table is refused in ALL ROWS (42701).
    ("WITHIN without an ORDER BY", mr("tickts", "symbol", None, "strt.day AS s", "strt down+ up+", TICK_DEF, "PAST LAST ROW",
                                       within="INTERVAL '4' HOUR"),
     "42601", "WITHIN reads the first ORDER BY: add one, a date or a timestamp"),
    ("WITHIN with the first ORDER BY descending", mr("tickts", "symbol", "ts DESC", "strt.day AS s", "strt down+ up+", TICK_DEF,
                                                     "PAST LAST ROW", within="INTERVAL '4' HOUR"),
     "0A000", "WITHIN reads the first ORDER BY ascending"),
    ("WITHIN with an INT first ORDER BY", mr("tickts", "symbol", "day", "strt.day AS s", "strt down+ up+", TICK_DEF,
                                              "PAST LAST ROW", within="INTERVAL '4' HOUR"),
     "42804", "order by a date or a timestamp"),
    ("a measure named like a column of the table, in ALL ROWS PER MATCH", mr("ticker", "symbol", "day", "strt.day AS price",
                                                                            "strt down+ up+", TICK_DEF, "PAST LAST ROW",
                                                                            rows_per="ALL ROWS PER MATCH"),
     "42701", "has the name of a column of ticker"),
    ("WITHIN with a number, not an interval", mr("tickts", "symbol", "ts", "strt.day AS s", "strt down+ up+", TICK_DEF,
                                                 "PAST LAST ROW", within="5"),
     "42601", "WITHIN takes an interval, as INTERVAL '1' HOUR"),
    ("WITHIN with a subquery, not an interval", mr("tickts", "symbol", "ts", "strt.day AS s", "strt down+ up+", TICK_DEF,
                                                  "PAST LAST ROW", within="(SELECT 1)"),
     "42601", "WITHIN takes an interval, as INTERVAL '1' HOUR"),
]


def refusal_checks(port, show=True):
    out = {}
    for what, sql, code, word in REFUSED:
        r = post_sql(port, sql)
        ok = (not r["ok"]) and r["code"] == code and word.lower() in r["msg"].lower()
        out[what] = ok
        if show:
            print(f"refused: {'OK  ' if ok else 'FAIL'} {what}: {code} {word!r}" + ("" if ok else f" (got ok={r['ok']} [{r['code']}] {r['msg'][:200]})"))
    return out


# ---------------------------------------------------------------- random queries (section 2)

# The ONE ROW measures of the random queries (the same text as ever); ALL ROWS may qualify the navigations and aggregates.
RANDOM_BASE = (("mn", "mn", None, None), ("s", "first", "i", None), ("e", "last", "i", None), ("n", "count", None, None),
               ("na", "count", None, "a"), ("nb", "count", None, "b"), ("nc", "count", None, "c"),
               ("nd", "count", None, "d"), ("cl", "class", None, None), ("fa", "first", "i", "a"), ("lb", "last", "i", "b"))
MODES = ("one", "show", "omit", "unmatched")
MODE_WEIGHTS = (40, 20, 20, 20)
ROWS_PER = {"one": "ONE ROW PER MATCH", "show": "ALL ROWS PER MATCH SHOW EMPTY MATCHES",
            "omit": "ALL ROWS PER MATCH OMIT EMPTY MATCHES", "unmatched": "ALL ROWS PER MATCH WITH UNMATCHED ROWS"}


SEQ_COLS = ("p", "i", "ts", "dt", "ca", "cb", "cc")  # seqs's columns in order: the other columns of ALL ROWS follow this order
SEQ_BASE = datetime(2026, 3, 2, 9, 0)  # ts is i in minutes from here, with a jitter (gen_data)
WITHIN_MINUTES = (2, 3, 4, 6, 9, 12)  # the WITHIN intervals of the random queries, in minutes (i steps 3 a row)
WITHIN_DAYS = (1, 2, 3)  # and in days, for a WITHIN by the date column dt
SEQ_DATE = date(2026, 3, 2)  # dt is this day plus about half the position, in days (gen_data)
WITHIN_UNITS = {"MINUTE": lambda k: timedelta(minutes=k), "DAY": lambda k: timedelta(days=k)}


def pick_within(rng):
    """(order, within) of one query: ORDER BY i with no WITHIN, or WITHIN by ts in minutes, or by dt in days (within is
    (unit, amount))."""
    if rng.random() >= 0.2:
        return "i", None
    if rng.random() < 0.5:
        return "ts", ("MINUTE", rng.choice(WITHIN_MINUTES))
    return "dt", ("DAY", rng.choice(WITHIN_DAYS))


def random_spec(rng, mode):
    """The measures of a random query. In ALL ROWS PER MATCH each navigation and aggregate may carry RUNNING or FINAL
    (MATCH_NUMBER() and CLASSIFIER() never do); in ONE ROW PER MATCH none does."""
    out = []
    for alias, op, col, var in RANDOM_BASE:
        qual = ""
        if mode != "one" and op in ("first", "last", "count"):
            qual = rng.choice(["", "", "RUNNING", "FINAL"])
        out.append((alias, op, col, var, qual))
    return tuple(out)


def random_query(pattern, skip, mode, spec, order="i", within=None):
    """The query over seqs, ORDER BY order; within (unit, amount) is WITHIN's, written after PATTERN. The DEFINEs read
    their own rows only: a DEFINE that reads another symbol is refused by the engine until the next PR."""
    measures = ", ".join(measure_text(x) for x in spec)
    clause = "" if within is None else f" WITHIN INTERVAL '{within[1]}' {within[0]}"
    final = "ORDER BY p, mn" if mode == "one" else f"ORDER BY p, {order}, mn"
    return (f"SELECT * FROM seqs MATCH_RECOGNIZE (PARTITION BY p ORDER BY {order} MEASURES {measures} {ROWS_PER[mode]} "
            f"AFTER MATCH SKIP {skip} PATTERN ({pattern}){clause} DEFINE a AS ca, b AS cb, c AS cc) {final}")


def gen_count(rng):
    r = rng.random()
    if r < 0.2:
        return 0, None
    if r < 0.4:
        return 1, None
    if r < 0.5:
        return 0, 1
    if r < 0.6:
        n = rng.randint(1, 3)
        return n, n
    if r < 0.75:
        return rng.randint(0, 2), None
    if r < 0.85:
        return 0, rng.randint(1, 3)
    n = rng.randint(0, 2)
    return n, max(n, 1) + rng.randint(0, 2)


def gen_tree(rng, depth):
    if depth == 0 or rng.random() < 0.3:
        if rng.random() < 0.05:
            return ("anchor", rng.choice(["start", "end"]))
        return ("sym", rng.choice(SYMS))
    kind = rng.choices(["seq", "alt", "perm", "excl", "rep"], [35, 15, 8, 7, 35])[0]
    if kind in ("seq", "alt", "perm"):
        return (kind, [gen_tree(rng, depth - 1) for _ in range(rng.randint(2, 3))])
    if kind == "excl":
        return ("excl", gen_tree(rng, depth - 1))
    body = gen_tree(rng, depth - 1)
    if nullable(body):  # a quantifier over a sub-pattern that can match empty is out of scope: none is generated
        body = ("sym", rng.choice(SYMS))
    lo, hi = gen_count(rng)
    return ("rep", body, lo, hi, rng.random() < 0.7)


def gen_pattern(rng):
    tree = gen_tree(rng, 3)
    missing = [s for s in SYMS if s not in symbols_of(tree)]
    if missing:  # every symbol must be in PATTERN (DEFINE and the measures name a, b, c and d): add each as d? and the like
        tree = ("seq", [tree] + [("rep", ("sym", s), 0, 1, True) for s in missing])
    text = render(tree)
    if parse_pattern(text) != tree:
        raise AssertionError(f"render/parse disagree on {text!r}")
    return tree, text


def gen_skip(rng):
    r = rng.random()
    if r < 0.35:
        return "PAST LAST ROW"
    if r < 0.6:
        return "TO NEXT ROW"
    return f"TO {'FIRST' if r < 0.8 else 'LAST'} {rng.choice(SYMS)}"


def gen_data(rng):
    """Four partitions of 0-25 rows: i is 3 * position + 1 (not the position, so a mix-up shows), ts is i in minutes from
    SEQ_BASE with a jitter of up to four minutes (so ORDER BY ts reorders some rows, and some tie), each condition
    true with its own probability, and NULL one time in twelve (false in DEFINE)."""
    parts = []
    for p in range(1, 5):
        pr = {s: rng.uniform(0.2, 0.8) for s in "abc"}
        rows = []
        for pos in range(rng.randint(0, 25)):
            row = {"p": p, "i": 3 * pos + 1, "ts": SEQ_BASE + timedelta(minutes=3 * pos + 1 + rng.randint(-4, 4)),
                   "dt": SEQ_DATE + timedelta(days=max(0, pos // 2 + rng.randint(-1, 1)))}
            for s in "abc":
                row["c" + s] = None if rng.random() < 0.08 else (rng.random() < pr[s])
            rows.append(row)
        parts.append((p, rows))
    return parts


def random_parts(data, order="i"):
    """gen_data's partitions as (rows, truth) pairs for reference_output, each sorted by the ORDER BY column (a tie keeps
    the rows' order, which is the order the lake's _row_id gives them)."""
    out = []
    for _, rows in data:
        srt = sorted(rows, key=lambda r: r[order])
        out.append((srt, [{"a": r["ca"] is True, "b": r["cb"] is True, "c": r["cc"] is True} for r in srt]))
    return out


def reference_random(parts, tree, skip, mode, spec, order="i", within=None):
    """The random query's answer by the reference: ("rows", names, rows), or ("error", SQLSTATE, message). The other
    columns are seqs's, in its order, less the partition and ORDER BY columns."""
    rest = tuple(c for c in SEQ_COLS if c not in ("p", order))
    try:
        names, rows = reference_output(parts, tree, skip, spec, mode, ("p",), order, rest,
                                       None if within is None else WITHIN_UNITS[within[0]](within[1]))
    except RefError as e:
        return ("error", e.sqlstate, e.message)
    return ("rows", names, rows)


def random_checks(port, seed=1, queries=600, show=5):
    rng = random.Random(seed)
    data = gen_data(rng)
    parts = {"i": random_parts(data, "i"), "ts": random_parts(data, "ts"), "dt": random_parts(data, "dt")}  # (WITHIN orders by ts or dt)
    sql_exec(port, "DROP TABLE IF EXISTS seqs")
    sql_exec(port, "CREATE TABLE seqs (p INT, i INT, ts TIMESTAMP, dt DATE, ca BOOLEAN, cb BOOLEAN, cc BOOLEAN)")
    vals = [f"({p}, {r['i']}, {sql_value(r['ts'])}, {sql_value(r['dt'])}, {sql_value(r['ca'])}, {sql_value(r['cb'])}, {sql_value(r['cc'])})"
            for p, rows in data for r in rows]
    if vals:
        sql_exec(port, "INSERT INTO seqs VALUES " + ", ".join(vals))
    done = agreed = both_errors = regenerated = parser_class = 0
    per_mode = {m: [0, 0] for m in MODES}  # [queries, agreed]
    within_q = [0, 0]  # the WITHIN queries (by ts or by d): [queries, agreed]
    misses, took = [], []
    while done < queries:
        tree, text = gen_pattern(rng)
        skip = gen_skip(rng)
        mode = rng.choices(MODES, MODE_WEIGHTS)[0]
        if mode == "unmatched" and has_excl(tree):  # WITH UNMATCHED ROWS refuses {- -}: the same pattern without it
            text = render(strip_excl(tree))
            tree = parse_pattern(text)
        spec = random_spec(rng, mode)
        order, within = pick_within(rng)
        try:
            expect = reference_random(parts[order], tree, parse_skip(skip), mode, spec, order, within)
        except OutOfBudget:
            regenerated += 1
            continue
        done += 1
        per_mode[mode][0] += 1
        query = random_query(text, skip, mode, spec, order, within)
        r = post_sql(port, query, arrow=True)
        took.append(r["took"])
        if expect[0] == "error":
            ok = (not r["ok"]) and r["code"] == expect[1]
            both_errors += ok
        else:
            ok = bool(r["ok"] and r["value"][0] == expect[1] and Counter(r["value"][1]) == Counter(expect[2]))
        agreed += ok
        per_mode[mode][1] += ok
        if within is not None:
            within_q[0] += 1
            within_q[1] += ok
        if not ok:
            if (not r["ok"]) and r["code"] == "42601" and "Expected" in r["msg"]:
                parser_class += 1  # a form the grammar has, refused by the parser: a bug to report (PATTERN is Pondra's to read)
            size = (len(text), len(expect[2]) if expect[0] == "rows" else 0)
            label = mode if within is None else f"{mode}, WITHIN {within[1]} {within[0]} by {order}"
            misses.append((size, text, skip, label, query, expect, r, data))
    print(f"random: {agreed} of {done} queries agree with the reference ({both_errors} answered with 22000 by both); "
          f"{regenerated} patterns regenerated (the reference's search ran past its budget)")
    print("random, by rows per match: " + ", ".join(f"{m} {per_mode[m][1]} of {per_mode[m][0]}" for m in MODES))
    print(f"random, WITHIN (by ts in minutes, or by dt in days): {within_q[1]} of {within_q[0]} agree")
    print(f"random: {len(misses)} disagree; {parser_class} of them refused with 'Expected' (42601): each a parser bug to report")
    misses.sort(key=lambda x: x[0])  # the smallest first: the shortest pattern, then the fewest rows given
    for _, text, skip, mode, query, expect, r, data_ in misses[:show]:
        print(f"DISAGREE rows per match {mode}, pattern ({text})  skip {skip}\n  query: {query}")
        for p, rows in data_:
            print(f"  data p={p}: " + json.dumps([{"i": r_["i"], "ts": f"{r_['ts']:%H:%M}", "dt": str(r_["dt"]), "a": r_["ca"], "b": r_["cb"], "c": r_["cc"]}
                                                 for r_ in rows]))
        print(f"  reference: {expect[0]} {expect[1] if expect[0] == 'rows' else ''} "
              f"{expect[2] if expect[0] == 'rows' else ''}"[:600])
        print(f"  node:      {r['value'] if r['ok'] else ('[' + str(r['code']) + '] ' + r['msg'][:300])}"[:600])
    return agreed == done, {"queries": done, "agreed": agreed, "errors_both": both_errors, "regenerated": regenerated,
                            "per_mode": {m: f"{per_mode[m][1]}/{per_mode[m][0]}" for m in MODES},
                            "disagree": len(misses), "disagree_parser": parser_class, "within": f"{within_q[1]}/{within_q[0]}",
                            "slowest_ms": round(max(took, default=0) * 1000)}


# ---------------------------------------------------------------- self test: the reference alone

def ticker_rows():
    return partitions_of(TABLES["ticker"][1], "symbol", "day")[0]


def self_test():
    bad = []

    def check(name, ok, detail=""):
        print(f"{'OK  ' if ok else 'FAIL'} {name}" + ("" if ok else f": {detail}"))
        if not ok:
            bad.append(name)

    P = parse_pattern
    check("parse a+? b", P("a+? b") == ("seq", [("rep", ("sym", "a"), 1, None, False), ("sym", "b")]))
    check("parse (a | b c)*", P("(a | b c)*") == ("rep", ("alt", [("sym", "a"), ("seq", [("sym", "b"), ("sym", "c")])]), 0, None, True))
    check("parse PERMUTE(a, b)", P("PERMUTE(a, b)") == ("perm", [("sym", "a"), ("sym", "b")]))
    check("parse {- b -}", P("{- b -}") == ("excl", ("sym", "b")))
    check("parse a{2,3}?", P("a{2,3}?") == ("rep", ("sym", "a"), 2, 3, False))
    check("parse a{,2}", P("a{,2}") == ("rep", ("sym", "a"), 0, 2, True))
    check("parse a{3}", P("a{3}") == ("rep", ("sym", "a"), 3, 3, True))
    check("parse a??", P("a??") == ("rep", ("sym", "a"), 0, 1, False))
    check("parse ^a+$", P("^a+$") == ("seq", [("anchor", "start"), ("rep", ("sym", "a"), 1, None, True), ("anchor", "end")]))
    check("parse strt ({- down -})+ up+ (the exclusion as a group's start)",
          P("strt ({- down -})+ up+") == ("seq", [("sym", "strt"), ("rep", ("excl", ("sym", "down")), 1, None, True),
                                                  ("rep", ("sym", "up"), 1, None, True)]))
    check("parse strt {- down+ -} up+ (the brief's form: an exclusion of a quantified symbol)",
          P("strt {- down+ -} up+") == ("seq", [("sym", "strt"), ("excl", ("rep", ("sym", "down"), 1, None, True)),
                                                ("rep", ("sym", "up"), 1, None, True)]))
    check("WITH UNMATCHED ROWS drops the exclusions: strip_excl gives strt (down)+ up+",
          strip_excl(P("strt ({- down -})+ up+")) == P("strt (down)+ up+"))
    t = [{"a": True, "b": True}, {"a": False, "b": True}]
    check("alternation: a b | a takes a b (left first when both match at 0)",
          [(m.start, m.end) for m in find_matches(t, P("a b | a"), ("past_last",))] == [(0, 2)])
    check("alternation: a | a b takes a alone", [(m.start, m.end) for m in find_matches(t, P("a | a b"), ("past_last",))] == [(0, 1)])
    t3 = [{"a": True, "b": False}, {"a": True, "b": False}, {"a": False, "b": True}]
    check("a+ b takes all three rows", [(m.start, m.end) for m in find_matches(t3, P("a+ b"), ("past_last",))] == [(0, 3)])
    check("a+? b takes all three rows too (b is needed)", [(m.start, m.end) for m in find_matches(t3, P("a+? b"), ("past_last",))] == [(0, 3)])
    check("measure text: FINAL LAST(i) AS e", measure_text(("e", "last", "i", None, "FINAL")) == "FINAL LAST(i) AS e")
    check("measure text: count(a.*) AS na", measure_text(("na", "count", None, "a", "")) == "count(a.*) AS na")
    check("measure text: LAST(down.price) AS d", measure_text(("d", "last", "price", "down", "")) == "LAST(down.price) AS d")

    for ex in FIXED:
        if "spec" in ex:  # ALL ROWS: the names in order, and the rows as a multiset (the node's order is checked there)
            parts = [(rows, truth_of(rows, ex["define"])) for rows in partitions_of(TABLES[ex["table"]][1], ex.get("partition"), ex["order"])]
            names, got = reference_output(parts, P(ex["pattern"]), parse_skip(ex["skip"]), ex["spec"], ex["mode"],
                                          ex["pcols"], ex["order"], ex["rcols"])
            want_names = list(ex["expect"][0])
            want = [tuple(norm(d.get(n)) for n in want_names) for d in ex["expect"]]
            check(f"fixed (ALL ROWS): {ex['name']}", names == want_names and Counter(got) == Counter(want),
                  f"reference gave columns {names} and rows {got}; written columns {want_names} and rows {want}")
            continue
        got = reference_rows(ex)
        ok = [canon(d) for d in got] == [canon(d) for d in ex["expect"]]
        check(f"fixed: {ex['name']}", ok, f"reference gave {got}, written {ex['expect']}")

    # the ticker worked out by hand (see the comments on FIXED): days of each match, by SKIP rule
    rows = ticker_rows()
    tt = truth_of(rows, TICK_FN)
    for skip, want in (("TO LAST up", [(5, 6, 10), (10, 12, 13), (14, 16, 18)]),
                       ("PAST LAST ROW", [(5, 6, 10), (11, 12, 13), (14, 16, 18)])):
        ms = find_matches(tt, parse_pattern("strt down+ up+"), parse_skip(skip))
        got = [(rows[m.start]["day"], rows[pos_of(m, "down")[-1]]["day"], rows[pos_of(m, "up")[-1]]["day"]) for m in ms]
        check(f"ticker by hand, {skip}: (start, bottom, end) = {want}", got == want, f"reference gave {got}")

    try:
        find_matches(tt, parse_pattern("strt down+ | up+"), parse_skip("TO LAST up"))
        check("refusal by the reference: SKIP TO LAST up with no up in the match is 22000", False, "no error")
    except RefError as e:
        check("refusal by the reference: SKIP TO LAST up with no up in the match is 22000",
              e.sqlstate == "22000" and "has no row in match" in e.message, f"{e.sqlstate} {e.message}")

    # WITHIN, the reference alone (the times are ints here, minutes): a row past the interval ends its thread, so the
    # shorter match stands (the bound is inclusive); a later start wins when the first runs out of time; a NULL time is
    # in no match. The fixed examples above check WITHIN on the ticker's timestamps.
    x4 = [{"x": True, "y": False}] * 4
    check("WITHIN: a row past the interval ends the thread: x+ takes 0-2 (2 minutes is in), then 3 starts a match",
          [(m.start, m.end) for m in find_matches(x4, P("x+"), ("past_last",), within=([0, 1, 2, 3], 2))] == [(0, 3), (3, 4)])
    t3 = [{"x": True, "y": False}, {"x": True, "y": False}, {"x": False, "y": True}]
    check("WITHIN: the first start runs out of time, so the later start wins (without WITHIN, 0-3)",
          [(m.start, m.end) for m in find_matches(t3, P("x+ y"), ("past_last",), within=([0, 1, 3], 2))] == [(1, 3)]
          and [(m.start, m.end) for m in find_matches(t3, P("x+ y"), ("past_last",))] == [(0, 3)])
    t_null = [{"x": True, "y": False}] * 3
    check("WITHIN: a NULL time is in no match: rows 0 and 2 match, row 1 has no time",
          [(m.start, m.end) for m in find_matches(t_null, P("x+"), ("past_last",), within=([0, None, 1], 5))] == [(0, 1), (2, 3)])

    d3 = [{"x": True, "y": False}] * 3
    check("WITHIN by a date: 1 day is in, 2 days is out",
          [(m.start, m.end) for m in find_matches(d3, P("x+"), ("past_last",), within=([date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4)], timedelta(days=1)))] == [(0, 2), (2, 3)])

    rng = random.Random(7)
    done = hits = 0
    for _ in range(300):
        try:
            tree, text = gen_pattern(rng)
            mode = rng.choice(MODES)
            if mode == "unmatched" and has_excl(tree):
                tree = parse_pattern(render(strip_excl(tree)))
            data = gen_data(rng)
            order, within = pick_within(rng)
            reference_random(random_parts(data, order), tree, parse_skip(gen_skip(rng)), mode, random_spec(rng, mode), order, within)
            done += 1
        except OutOfBudget:
            hits += 1
        except AssertionError as e:
            check("generator: renders and parses back", False, str(e))
            break
    check(f"generator: 300 patterns render and parse back; the reference ran {done}, past its budget {hits}", True)
    print("SELF-TEST " + ("PASS" if not bad else f"FAIL ({len(bad)})"))
    return not bad


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--self-test", action="store_true", help="the reference alone, on the fixed examples")
    ap.add_argument("--port", type=int, default=9340)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--queries", type=int, default=600)
    A = ap.parse_args()
    if A.self_test:
        sys.exit(0 if self_test() else 1)
    ok_random, info = random_checks(A.port, A.seed, A.queries)
    ok_fixed = all(fixed_checks(A.port).values())
    ok_refused = all(refusal_checks(A.port).values())
    print(json.dumps({"random": info, "ok": ok_random and ok_fixed and ok_refused}, indent=1))
    sys.exit(0 if ok_random and ok_fixed and ok_refused else 1)


if __name__ == "__main__":
    main()
