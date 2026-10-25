#!/usr/bin/env python3
"""MATCH_RECOGNIZE (ADR-073, PR 1): the row-pattern matcher against a reference of our own.

  patterns_check.py --self-test                          the reference alone (no node needed)
  patterns_check.py [--port 9340] [--seed 1] [--queries 600]   sections 2-4 against a running node

The reference (`find_matches`) is a backtracking matcher over one partition, written from the standard's rules
(ISO/IEC 19075-5 §4.11-4.16, as Trino's MATCH_RECOGNIZE page states them) and kept apart from the Rust matcher. It
parses the pattern text itself and tries every alternative in preference order: alternation left first, greedy
repetition more iterations first, reluctant fewer, PERMUTE's orders in the order a b c, a c b, b a c, …. The first
complete match from a start wins; a start is the leftmost row the search has not passed. After an empty match the
next start is one row on; otherwise the skip rule decides. A skip to a symbol with no row in the match, or to the
match's first row, is an error (SQLSTATE 22000).

Sections: 2 random queries (`random_checks`), 3 fixed examples (`fixed_checks`), 4 refusals (`refusal_checks`).
Section 5, the doors (the Postgres port, three nodes, views), is harness.py's `patterns` section.
"""
import argparse, http.client, itertools, json, random, re, sys, time

SYMS = ("a", "b", "c", "d")  # d has no DEFINE: always true
RANDOM_KEYS = ("p", "mn", "s", "e", "n", "na", "nb", "nc", "nd", "cl", "fa", "lb")


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


def find_matches(truth, tree, skip, budget=200_000):
    """The matches of one partition, in order. truth[pos] maps a symbol to whether row pos may be it; a symbol
    with no DEFINE is absent from the map and always true. skip is ('past_last',), ('next_row',), ('first', sym)
    or ('last', sym). Items are (pos, symbol, excluded) in row order."""
    left = [budget]
    matches, frm, n = [], 0, len(truth)
    while frm < n:
        hit = None
        for start in range(frm, n):
            found = attempt(truth, tree, start, left)
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


def attempt(truth, tree, start, left):
    """The most preferred complete match that begins at start: (end, items), or None."""
    n, path, result = len(truth), [], {}

    def m(node, pos, excl, k):  # does node match from pos, with k(end) then succeeding?
        left[0] -= 1
        if left[0] < 0:
            raise OutOfBudget()
        kind = node[0]
        if kind == "sym":
            if pos < n and truth[pos].get(node[1], True):
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


def partitions_of(rows, partition, order):
    """The rows split by the PARTITION BY column (None: one partition), each sorted by ORDER BY; a tie keeps the
    order the rows were inserted in, which is the order the lake's _row_id gives them."""
    groups = {}
    for r in rows:
        groups.setdefault(r[partition] if partition else None, []).append(r)
    return [sorted(g, key=lambda r: r[order]) for g in groups.values()]


# ---------------------------------------------------------------- the node: one POST /sql

def sql_value(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def post_sql(port, sql):
    """One statement: {ok, value (the JSON answer, or the body), code (SQLSTATE), msg, took (seconds)}."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    t0 = time.time()
    try:
        conn.request("POST", "/sql", sql.encode())
        resp = conn.getresponse()
        body = resp.read().decode("utf-8", "replace")
    except (OSError, http.client.HTTPException) as e:
        return {"ok": False, "value": None, "code": None, "msg": f"no answer ({type(e).__name__}): {e}", "took": time.time() - t0}
    finally:
        conn.close()
    took = time.time() - t0
    if resp.status == 200:
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

def norm(v):
    return round(v, 6) if isinstance(v, float) else v


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
    "runs": ("day INT, flag BOOLEAN", [{"day": d, "flag": f} for d, f in zip(range(1, 5), [True, True, False, True])]),
    "pm": ("day INT, sym TEXT", [{"day": d, "sym": s} for d, s in zip(range(1, 6), ["c", "b", "a", "b", "a"])]),
    "rl": ("day INT, sym TEXT", [{"day": d, "sym": s} for d, s in zip(range(1, 4), ["a", "a", "b"])]),
    "bl": ("day INT, sym TEXT", [{"day": d, "sym": "a"} for d in range(1, 6)]),
    "excl_t": ("day INT, sym TEXT", [{"day": d, "sym": s} for d, s in zip(range(1, 4), ["a", "b", "c"])]),
}


def mr(table, partition, order, measures, pattern, define, skip, rows_per="ONE ROW PER MATCH", tail=""):
    part = f"PARTITION BY {partition} " if partition else ""
    return (f"SELECT * FROM {table} MATCH_RECOGNIZE ({part}ORDER BY {order} MEASURES {measures} {rows_per} "
            f"AFTER MATCH SKIP {skip} PATTERN ({pattern}) DEFINE {define}){tail}")


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


def lo_(rows, i):  # price below the price two rows on (NEXT(price, 2)); no such row is NULL, so false
    return i + 2 < len(rows) and rows[i]["price"] < rows[i + 2]["price"]


def hi_(rows, i):  # price above the price two rows back (PREV(price, 2))
    return i - 2 >= 0 and rows[i]["price"] > rows[i - 2]["price"]


def flag_(rows, i):
    return rows[i]["flag"]


TICK_SKIP = "TO LAST up"

# Each: the table, the query, the reference's inputs (partition, order, pattern, skip, DEFINE functions and the
# measures), and the answer written here by hand. Ticker answers are worked out in the comments.
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


def reference_rows(ex):
    out = []
    for rows in partitions_of(TABLES[ex["table"]][1], ex.get("partition"), ex["order"]):
        ms = find_matches(truth_of(rows, ex["define"]), parse_pattern(ex["pattern"]), parse_skip(ex["skip"]))
        out += [ex["measures"](m, rows) for m in ms]
    return out


def load_tables(port):
    for name, (cols, rows) in TABLES.items():
        names = [c.split()[0] for c in cols.split(", ")]
        sql_exec(port, f"DROP TABLE IF EXISTS {name}")
        sql_exec(port, f"CREATE TABLE {name} ({cols})")
        vals = ", ".join("(" + ", ".join(sql_value(r[c]) for c in names) + ")" for r in rows)
        sql_exec(port, f"INSERT INTO {name} VALUES {vals}")


def fixed_checks(port, show=True):
    """Section 3 on the node: each example's query, run `runs` times, equal to its written answer, in order."""
    load_tables(port)
    out = {}
    for ex in FIXED:
        want = [canon(d) for d in ex["expect"]]
        runs = [post_sql(port, ex["query"]) for _ in range(ex.get("runs", 1))]
        ok = all(r["ok"] and [canon(d) for d in r["value"]] == want for r in runs)
        out[ex["name"]] = ok
        if not show:
            continue
        if ok:
            print(f"fixed: OK   {ex['name']} ({len(runs)} run(s), {runs[0]['took'] * 1000:.0f} ms)")
            continue
        bad = next(r for r in runs if not (r["ok"] and [canon(d) for d in r["value"]] == want))
        got = bad["value"] if bad["ok"] else f"[{bad['code']}] {bad['msg'][:300]}"
        print(f"fixed: FAIL {ex['name']}\n  query: {ex['query']}\n  want:  {ex['expect']}\n  got:   {got}")
    return out


# ---------------------------------------------------------------- refusals (section 4)

TICK_Q = lambda **kw: mr("ticker", kw.get("partition", "symbol"), "day", kw.get("measures", "strt.day AS s"),
                         kw.get("pattern", "strt down+ up+"), kw.get("define", TICK_DEF), kw.get("skip", "PAST LAST ROW"),
                         rows_per=kw.get("rows_per", "ONE ROW PER MATCH"))

# (what it is, the statement, SQLSTATE, a word the message must contain)
REFUSED = [
    ("ALL ROWS PER MATCH", TICK_Q(rows_per="ALL ROWS PER MATCH"), "0A000", "ALL ROWS PER MATCH"),
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

RANDOM_MEASURES = ("MATCH_NUMBER() AS mn, FIRST(i) AS s, LAST(i) AS e, count(*) AS n, count(a.*) AS na, count(b.*) AS nb, "
                   "count(c.*) AS nc, count(d.*) AS nd, CLASSIFIER() AS cl, FIRST(a.i) AS fa, LAST(b.i) AS lb")


def random_query(pattern, skip):
    return (f"SELECT * FROM seqs MATCH_RECOGNIZE (PARTITION BY p ORDER BY i MEASURES {RANDOM_MEASURES} "
            f"ONE ROW PER MATCH AFTER MATCH SKIP {skip} PATTERN ({pattern}) DEFINE a AS ca, b AS cb, c AS cc) ORDER BY p, mn")


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
    if nullable(body):  # a quantifier over a sub-pattern that can match empty is out of scope
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
    """Four partitions of 0-25 rows: i is 3 * position + 1 (not the position, so a mix-up shows), each condition
    true with its own probability, and NULL one time in twelve (false in DEFINE)."""
    parts = []
    for p in range(1, 5):
        pr = {s: rng.uniform(0.2, 0.8) for s in "abc"}
        rows = []
        for pos in range(rng.randint(0, 25)):
            row = {"i": 3 * pos + 1}
            for s in "abc":
                row["c" + s] = None if rng.random() < 0.08 else (rng.random() < pr[s])
            rows.append(row)
        parts.append((p, rows))
    return parts


def random_row(p, rows, m):
    if not m.items:
        return {"p": p, "mn": m.number, "s": None, "e": None, "n": 0, "na": 0, "nb": 0, "nc": 0, "nd": 0,
                "cl": None, "fa": None, "lb": None}
    syms = [s for _, s, _ in m.items]
    fa = [rows[pos]["i"] for pos, s, _ in m.items if s == "a"]
    lb = [rows[pos]["i"] for pos, s, _ in m.items if s == "b"]
    return {"p": p, "mn": m.number, "s": rows[m.start]["i"], "e": rows[m.end - 1]["i"], "n": len(syms),
            "na": syms.count("a"), "nb": syms.count("b"), "nc": syms.count("c"), "nd": syms.count("d"),
            "cl": syms[-1], "fa": fa[0] if fa else None, "lb": lb[-1] if lb else None}


def reference_answer(parts, tree, skip):
    out = []
    try:
        for p, rows in parts:
            truth = [{"a": r["ca"] is True, "b": r["cb"] is True, "c": r["cc"] is True} for r in rows]
            out += [random_row(p, rows, m) for m in find_matches(truth, tree, skip)]
    except RefError as e:
        return ("error", e.sqlstate, e.message)
    return ("rows", out)


def random_checks(port, seed=1, queries=600, show=5):
    rng = random.Random(seed)
    parts = gen_data(rng)
    sql_exec(port, "DROP TABLE IF EXISTS seqs")
    sql_exec(port, "CREATE TABLE seqs (p INT, i INT, ca BOOLEAN, cb BOOLEAN, cc BOOLEAN)")
    vals = [f"({p}, {r['i']}, {sql_value(r['ca'])}, {sql_value(r['cb'])}, {sql_value(r['cc'])})" for p, rows in parts for r in rows]
    if vals:
        sql_exec(port, "INSERT INTO seqs VALUES " + ", ".join(vals))
    done = agreed = both_errors = regenerated = 0
    misses, took = [], []
    while done < queries:
        tree, text = gen_pattern(rng)
        skip = gen_skip(rng)
        try:
            expect = reference_answer(parts, tree, parse_skip(skip))
        except OutOfBudget:
            regenerated += 1
            continue
        done += 1
        query = random_query(text, skip)
        r = post_sql(port, query)
        took.append(r["took"])
        if expect[0] == "error":
            ok = (not r["ok"]) and r["code"] == expect[1]
            both_errors += ok
        else:
            ok = r["ok"] and [tuple(x.get(k) for k in RANDOM_KEYS) for x in r["value"]] == \
                [tuple(x.get(k) for k in RANDOM_KEYS) for x in expect[1]]
        agreed += ok
        if not ok:
            misses.append((text, skip, query, expect, r))
    print(f"random: {agreed} of {done} queries agree with the reference ({both_errors} answered with 22000 by both); "
          f"{regenerated} patterns regenerated (the reference's search ran past its budget)")
    for text, skip, query, expect, r in misses[:show]:
        print(f"DISAGREE pattern ({text})  skip {skip}\n  query: {query}")
        for p, rows in parts:
            print(f"  data p={p}: " + json.dumps([{"i": r_["i"], "a": r_["ca"], "b": r_["cb"], "c": r_["cc"]} for r_ in rows]))
        print(f"  reference: {expect}")
        print(f"  node:      {r['value'] if r['ok'] else ('[' + str(r['code']) + '] ' + r['msg'][:300])}")
    return agreed == done, {"queries": done, "agreed": agreed, "errors_both": both_errors,
                            "regenerated": regenerated, "slowest_ms": round(max(took, default=0) * 1000)}


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
    t = [{"a": True, "b": True}, {"a": False, "b": True}]
    check("alternation: a b | a takes a b (left first when both match at 0)",
          [(m.start, m.end) for m in find_matches(t, P("a b | a"), ("past_last",))] == [(0, 2)])
    check("alternation: a | a b takes a alone", [(m.start, m.end) for m in find_matches(t, P("a | a b"), ("past_last",))] == [(0, 1)])
    t3 = [{"a": True, "b": False}, {"a": True, "b": False}, {"a": False, "b": True}]
    check("a+ b takes all three rows", [(m.start, m.end) for m in find_matches(t3, P("a+ b"), ("past_last",))] == [(0, 3)])
    check("a+? b takes all three rows too (b is needed)", [(m.start, m.end) for m in find_matches(t3, P("a+? b"), ("past_last",))] == [(0, 3)])

    for ex in FIXED:
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

    rng = random.Random(7)
    done = hits = 0
    for _ in range(300):
        try:
            _, text = gen_pattern(rng)
            reference_answer(gen_data(rng), parse_pattern(text), parse_skip(gen_skip(rng)))
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
