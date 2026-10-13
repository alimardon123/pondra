"""TPC-H query texts with fresh parameters, for tools/serve_bench.py's --tpch mode.

The texts are tools/bench/tpch-queries/qN.sql. Each parameter is drawn by the rule of TPC-H spec
2.4 (the substitution rules qgen uses), and every place the file writes one literal gets the drawn
value. A result cache answers a repeated statement text again, so a load's statements also carry an
always-true predicate whose literal changes per request: `({k} >= 0)`, which loadgen fills with a new
random number for each request. It names no column, so the planner folds it away and no row pays for
it (a predicate on a column cost Q6 a fifth column to read). With `unique=False` it is left out: the
run with the cache as it is, where the parameters repeat."""
import os, re
from datetime import date, timedelta

QUERIES = ["q1", "q3", "q5", "q6", "q10", "q12", "q14", "q19"]
FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench", "tpch-queries")
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]  # spec 4.2.2.13 Segments
REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]             # spec 4.2.3 R_NAME
MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]           # spec 4.2.2.13 Modes


def months(first, count):
    """The first days of `count` months from `first` (year, month): spec 2.4.10 and 2.4.14 pick from them."""
    return [date(first[0] + (first[1] - 1 + i) // 12, (first[1] - 1 + i) % 12 + 1, 1) for i in range(count)]


def drawn(q, rng):
    """(text in the file, the drawn text) pairs for query q: one pair per parameter."""
    if q == "q1":  # DELTA within [60, 120], counted back from 1998-12-01
        return [("date '1998-09-02'", f"date '{date(1998, 12, 1) - timedelta(days=rng.randint(60, 120))}'")]
    if q == "q3":  # SEGMENT; DATE a day within March 1995
        day = date(1995, 3, 1) + timedelta(days=rng.randint(0, 30))
        return [("'BUILDING'", f"'{rng.choice(SEGMENTS)}'"), ("date '1995-03-15'", f"date '{day}'")]
    if q == "q5":  # REGION; DATE the first of January of a year within [1993, 1997]
        return [("'ASIA'", f"'{rng.choice(REGIONS)}'"), ("date '1994-01-01'", f"date '{rng.randint(1993, 1997)}-01-01'")]
    if q == "q6":  # DATE the first of January; DISCOUNT within [0.02, 0.09]; QUANTITY within [24, 25]
        discount = f"{rng.randint(2, 9) / 100:.2f}"
        return [("date '1994-01-01'", f"date '{rng.randint(1993, 1997)}-01-01'"),
                ("0.06 - 0.01 and 0.06 + 0.01", f"{discount} - 0.01 and {discount} + 0.01"),
                ("l_quantity < 24", f"l_quantity < {rng.randint(24, 25)}")]
    if q == "q10":  # DATE the first day of a month from February 1993 to January 1995
        return [("date '1993-10-01'", f"date '{rng.choice(months((1993, 2), 24))}'")]
    if q == "q12":  # SHIPMODE1 and SHIPMODE2, different; DATE the first of January of a year within [1993, 1997]
        first, second = rng.sample(MODES, 2)
        return [("'MAIL', 'SHIP'", f"'{first}', '{second}'"), ("date '1994-01-01'", f"date '{rng.randint(1993, 1997)}-01-01'")]
    if q == "q14":  # DATE the first day of a month within 1993 to 1997
        return [("date '1995-09-01'", f"date '{rng.choice(months((1993, 1), 60))}'")]
    if q == "q19":  # QUANTITY1..3 within [1, 10], [10, 20], [20, 30]; BRAND1..3 = Brand#MN with M, N within [1, 5]
        brands = [f"Brand#{rng.randint(1, 5)}{rng.randint(1, 5)}" for _ in range(3)]
        quantities = [rng.randint(1, 10), rng.randint(10, 20), rng.randint(20, 30)]
        pairs = [("Brand#12", brands[0]), ("Brand#23", brands[1]), ("Brand#34", brands[2])]
        for old, n in zip([1, 10, 20], quantities):
            pairs.append((f"l_quantity >= {old} and l_quantity <= {old} + 10", f"l_quantity >= {n} and l_quantity <= {n} + 10"))
        return pairs
    raise ValueError(f"no parameters for {q}")


_text = {}


def file_text(q):
    """The query's text in tools/bench/tpch-queries (read once)."""
    if q not in _text:
        _text[q] = open(os.path.join(FOLDER, q + ".sql")).read().strip().rstrip(";")
    return _text[q]


def statement(q, rng, unique=True, k="{k}"):
    """Query q with fresh parameters from rng, one line (loadgen reads a statement a line). unique: the
    predicate that makes its text new per request; `k` is its literal ("{k}" for loadgen to fill, or a
    number when the statement is checked by hand)."""
    text = file_text(q)
    for old, new in drawn(q, rng):
        assert text.count(old) >= 1, (q, old)  # (the file has every literal the rule draws)
        text = text.replace(old, new)
    if unique:  # the first WHERE is the query's own (none of these has a subquery before it)
        text = re.sub(r"(?i)\bwhere\b", "where (" + str(k) + " >= 0) and", text, count=1)
    return " ".join(text.split())
