#!/usr/bin/env python3
"""DataFusion's own SQL tests (sqllogictest) through Pondra: the pass rate, and why the rest fail
(roadmap D1).

  slt_check.py --slt <datafusion>/datafusion/sqllogictest/test_files [--nodes 1|3] [--only a,b]
               [--out logs/round23/slt.json]

The files come with DataFusion's source, at the version Pondra builds on (55.1.0):

  git clone --depth 1 --filter=blob:none --sparse --branch 55.1.0 https://github.com/apache/datafusion
  git -C datafusion sparse-checkout set datafusion/sqllogictest/test_files

Each file runs against a lake of its own, statement by statement, over `POST /sql` as any client
would send it: `statement ok|error|count`, and `query` records whose answers are compared with
the file's, the values written as DataFusion's runner writes them (NULL, true/false, (empty),
floats rounded to 12 places, decimals without trailing zeros, Arrow's display for the rest) and
sorted where the record says (`rowsort`, `valuesort`). `--nodes 3` runs every query spread over
three nodes (`?spread=1`), which must give what one node gives.

The nodes run where DataFusion's runner does (the folder above `test_files`), for the program
that started them (`PONDRA_OWNER_KEY`), so a file's `CREATE EXTERNAL TABLE … LOCATION
'../../testing/data/…'` reads DataFusion's test data. That data comes with the source's submodules
and two more folders:

  git -C datafusion sparse-checkout add datafusion/core/tests/data datafusion/core/tests/tpch-csv datafusion/datasource-arrow/tests/data
  git -C datafusion submodule update --init --depth 1 testing parquet-testing

A record passes when Pondra answers as the file says. An error the file expects counts as passed
when Pondra refuses too, whatever its words (`error_text_differs` counts those whose words differ).
The failures are grouped by their first line, so each can be understood: what Pondra doesn't do
(session `SET`, files stored as Arrow or Avro, compressed files), what
it does differently on purpose, and what is wrong.
"""
import argparse, collections, decimal, hashlib, io, json, os, re, shutil, sys, time, uuid
import numpy

decimal.getcontext().prec = 100  # (a big DOUBLE rounded to 12 places: more digits than the default 28)
PLACES = 12  # (DataFusion's runner rounds floats to 12 places; in the files of Spark's functions, to 15)

OWNER = {"x-pondra-owner": uuid.uuid4().hex}  # (the nodes' owner: this runner, as DataFusion's reads its files)

# The two choices of DataFusion's runner that Pondra makes otherwise: it plans with 4 partitions
# (so plans print the same on any machine), and reads `1.5` as a DOUBLE where Pondra reads it as a
# DECIMAL (Postgres's numeric). The nodes run with DataFusion's (`--pondra`: with Pondra's own).
AS_DATAFUSION = "datafusion.execution.target_partitions=4,datafusion.sql_parser.parse_float_as_decimal=false"

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import harness


def records(path):
    """The file's records: (kind, header words, sql, expected lines, line number)."""
    lines = open(path, encoding="utf-8", errors="replace").read().split("\n")
    i, out, skip = 0, [], False
    while i < len(lines):
        line = lines[i].rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            i += 1
            continue
        words = line.split()
        if words[0] in ("skipif", "onlyif"):
            skip = skip or (words[0] == "skipif" and words[1].lower() == "datafusion") or (words[0] == "onlyif" and words[1].lower() != "datafusion")
            i += 1
            continue
        start, i = i + 1, i + 1
        if words[0] in ("statement", "query"):
            while i < len(lines) and not lines[i].strip():
                i += 1  # (a blank line between a record's header and its SQL, as DataFusion's runner allows)
            body = []
            while i < len(lines) and lines[i].strip() and lines[i].strip() != "----":
                body.append(lines[i])
                i += 1
            expected = []
            if i < len(lines) and lines[i].strip() == "----":
                i += 1
                while i < len(lines) and lines[i].strip():
                    expected.append(lines[i].rstrip())
                    i += 1
            if not skip:
                out.append((words[0], words[1:], "\n".join(body), expected, start))
        elif words[0] == "include":
            out.extend(records(os.path.join(os.path.dirname(path), words[1])))
        # (halt, control, hash-threshold, sleep, system: not run)
        skip = False
    return out


def cell(v, t, top=True):
    """One value as DataFusion's sqllogictest runner writes it: its own rules at the top (NULL,
    true/false, (empty), numbers rounded), Arrow's display for what is inside a list or struct."""
    import pyarrow as pa
    if v is None:
        return "NULL"
    if pa.types.is_dictionary(t):
        t = t.value_type
    if pa.types.is_boolean(t):
        return "true" if v else "false"
    if pa.types.is_floating(t):
        f = float(v)
        if f != f:
            return "NaN"
        if f in (float("inf"), float("-inf")):
            return ("inf" if f > 0 else "-inf") if not top else ("Infinity" if f > 0 else "-Infinity")
        text = str(numpy.float32(f)) if pa.types.is_float32(t) or pa.types.is_float16(t) else repr(f)  # (a float32's own shortest digits, as Rust writes it)
        if not top:
            return text.replace("e+", "e")
        return plain(decimal.Decimal(text), PLACES)
    if pa.types.is_decimal(t):
        return plain(v, t.scale, rounded=False) if top else str(v)  # (a decimal's every digit, as DataFusion's runner shows it)
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t):
        return ("(empty)" if v == "" else v.rstrip("\n").replace("\x00", "\\0")) if top else v
    if pa.types.is_integer(t):
        return str(v)
    if pa.types.is_date(t):
        return v.isoformat() + ("T00:00:00" if pa.types.is_date64(t) else "")  # (a Date64 as Arrow shows it)
    if isinstance(v, Exact):
        return moment(v, t)
    if pa.types.is_timestamp(t) or pa.types.is_time(t) or pa.types.is_duration(t):
        return moment(Exact(t, v), t)
    if pa.types.is_interval(t):
        return interval(*v)
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t) or pa.types.is_list_view(t):
        return "[" + ", ".join(cell(x, t.value_type, False) for x in v) + "]"
    if pa.types.is_map(t):
        return "{" + ", ".join(f"{cell(k, t.key_type, False)}: {cell(x, t.item_type, False)}" for k, x in v) + "}"
    if pa.types.is_struct(t):
        return "{" + ", ".join(f"{f.name}: {cell(v.get(f.name), f.type, False)}" for f in t) + "}"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_binary_view(t) or pa.types.is_fixed_size_binary(t):
        return bytes(v).hex()
    return str(v)


class Exact:
    """A time, timestamp or duration as its whole number in its type's unit (Python's datetime
    keeps microseconds; Arrow's display shows nanoseconds)."""
    SCALE = {"s": 0, "ms": 3, "us": 6, "ns": 9}

    def __init__(self, t, v):
        import datetime
        self.digits = Exact.SCALE[t.unit]
        if isinstance(v, int):
            self.n = v
        elif isinstance(v, datetime.timedelta):
            self.n = (v.days * 86400 + v.seconds) * 10**self.digits + v.microseconds * 10**self.digits // 10**6
        else:  # (a datetime or time inside a list or struct: to the microsecond)
            if isinstance(v, datetime.time):
                micros = ((v.hour * 60 + v.minute) * 60 + v.second) * 10**6 + v.microsecond
            else:
                v = v if v.tzinfo else v.replace(tzinfo=datetime.timezone.utc)
                micros = (v - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)) // datetime.timedelta(microseconds=1)
            self.n = micros * 10**self.digits // 10**6
        self.nanos = self.n * 10 ** (9 - self.digits)


def values(c):
    """A column's values: times, timestamps and durations as `Exact`."""
    import pyarrow as pa
    t = c.type
    if pa.types.is_timestamp(t) or pa.types.is_time(t) or pa.types.is_duration(t):
        try:
            return [None if n is None else Exact(t, n) for n in c.cast(pa.int64()).to_pylist()]
        except Exception:
            pass
    return c.to_pylist()


def tdiv(a, b):
    """Division as Rust does it: toward zero."""
    return abs(a) // b * (1 if a >= 0 else -1)


def moment(v, t):
    """A time, timestamp or duration as Arrow shows it: seconds' fractions in 3, 6 or 9 digits as
    needed, an offset (`Z` for UTC) when the type has a zone; a duration in days, hours, mins and
    secs."""
    import datetime, pyarrow as pa
    if pa.types.is_duration(t):
        d = v.digits
        secs = tdiv(v.n, 10**d) if d else v.n
        mins, sub = tdiv(secs, 60), v.n - secs * 10**d
        hours = tdiv(mins, 60)
        days = tdiv(hours, 24)
        secs, mins, hours = secs - mins * 60, mins - hours * 60, hours - days * 24
        if d == 0:
            return f"{days} days {hours} hours {mins} mins {secs} secs"
        return f"{days} days {hours} hours {mins} mins {'-' if sub < 0 else ''}{abs(secs) if sub < 0 else secs}.{abs(sub):0{d}} secs"
    secs, nanos = divmod(v.nanos, 10**9)
    zone = ""
    if pa.types.is_time(t):
        base = f"{secs // 3600:02}:{secs // 60 % 60:02}:{secs % 60:02}"
    else:
        at = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=secs)
        if t.tz is not None:
            at = at.astimezone(tz(t.tz))
            mins = int(at.utcoffset().total_seconds()) // 60
            zone = "Z" if mins == 0 else f"{'+' if mins > 0 else '-'}{abs(mins) // 60:02}:{abs(mins) % 60:02}"
        base = f"{at.year:04}-{at.month:02}-{at.day:02}T{at.hour:02}:{at.minute:02}:{at.second:02}"
    frac = "" if nanos == 0 else f".{nanos // 1_000_000:03}" if nanos % 1_000_000 == 0 else f".{nanos // 1000:06}" if nanos % 1000 == 0 else f".{nanos:09}"
    return base + frac + zone


def tz(name):
    """A type's zone: an offset (`+08:00`) or a name (`America/New_York`)."""
    import datetime, zoneinfo
    m = re.fullmatch(r"([+-])(\d{2}):?(\d{2})?", name)
    if m:
        return datetime.timezone((1 if m.group(1) == "+" else -1) * datetime.timedelta(hours=int(m.group(2)), minutes=int(m.group(3) or 0)))
    return datetime.timezone.utc if name.upper() in ("UTC", "Z") else zoneinfo.ZoneInfo(name)


def interval(months, days, nanos):
    """An interval as Arrow shows it: `1 mons 2 days 3 hours 4 mins 5.000000000 secs`, what is
    nought left out."""
    if not (months or days or nanos):
        return "0 secs"
    parts = [f"{months} mons"] * bool(months) + [f"{days} days"] * bool(days)
    if nanos:
        secs = tdiv(nanos, 10**9)
        sub, mins = nanos - secs * 10**9, tdiv(secs, 60)
        hours = tdiv(mins, 60)
        secs, mins = secs - mins * 60, mins - hours * 60
        parts += [f"{hours} hours"] * bool(hours) + [f"{mins} mins"] * bool(mins)
        if secs or sub:
            parts.append(f"{'-' if secs < 0 or sub < 0 else ''}{abs(secs)}.{abs(sub):09} secs")
    return " ".join(parts)


def plain(d, places, rounded=True):
    d = decimal.Decimal(d).quantize(decimal.Decimal(1).scaleb(-(min(places, PLACES) if rounded else places)) if places > 0 else decimal.Decimal(1), rounding=decimal.ROUND_HALF_EVEN) if places is not None else decimal.Decimal(d)
    if d == 0:
        return "0"
    s = format(d.normalize(), "f")
    return s


def answer(port, sql, spread, headers):
    """The query's rows as the file writes them, or raises with Pondra's error."""
    import pyarrow as pa
    path = "/sql?format=arrow" + ("&spread=1" if spread else "")
    data = harness.call(port, "POST", path, sql.encode(), timeout=A.timeout, headers=headers)
    if isinstance(data, dict) and isinstance(data.get("rows", data.get("copied")), int):
        return [[str(data.get("rows", data.get("copied")))]]  # (rows written: DataFusion answers with their count)
    if isinstance(data, (dict, list)):
        return [[json.dumps(data)]]  # (a statement's answer, not rows)
    rows = []
    reader = pa.ipc.open_stream(io.BytesIO(data))
    for batch in reader:
        cols = [(values(c), c.type) for c in batch.columns]
        for r in range(batch.num_rows):
            row = [cell(vals[r], t) for vals, t in cols]
            lines = row[-1].split("\n") if row else []
            if len(lines) < 2:
                rows.append(row)
                continue
            rows.append(row[:-1])  # (a last value over several lines: a numbered line each, as the runner writes them)
            for n, line in enumerate(lines, 1):
                content = line.lstrip()
                rows.append([f"{n:02}){'-' * (len(line) - len(content))}{content}"])
    return rows


def same(actual, expected, header):
    words = lambda s: s.split()
    got = [" ".join(r) for r in actual]
    mode = next((w for w in header[1:] if w in ("rowsort", "valuesort", "nosort")), "nosort")
    if len(expected) == 1 and re.match(r"^\d+ values hashing to [0-9a-f]+$", expected[0]):
        values = [v for r in (sorted(got) if mode == "rowsort" else got) for v in r.split(" ")]
        if mode == "valuesort":
            values.sort()
        n, h = expected[0].split(" values hashing to ")
        return len(values) == int(n) and hashlib.md5("".join(v + "\n" for v in values).encode()).hexdigest() == h
    if mode == "rowsort":
        got, expected = sorted(got, key=words), sorted(expected, key=words)
    elif mode == "valuesort":
        return sorted(v for r in got for v in words(r)) == sorted(v for r in expected for v in words(r))
    if any("<slt:ignore>" in r for r in expected):  # (DataFusion's runner: any text there)
        pattern = lambda r: re.compile(".*".join(re.escape(" ".join(p.split())) for p in r.split("<slt:ignore>")) + "$", re.S)
        return len(got) == len(expected) and all(pattern(e).match(" ".join(words(g))) for g, e in zip(got, expected))
    return [words(r) for r in got] == [words(r) for r in expected]


# Records that fail for a named reason that isn't a wrong answer: how a plan is printed, a write
# explained, what DataFusion's runner makes in Rust before a file runs, the node's own memory.
# Each such failure is counted under its name and left out of the pass rate (`raw_pass_rate`
# keeps them in); anything else that fails is a failure.
EXCEPTIONS = {
    "plan text": "EXPLAIN prints Pondra's plan, made with rules and scans of its own (optimize.rs, scan.rs): the text differs where the answers agree",
    "a write explained": "EXPLAIN of an INSERT, UPDATE, DELETE or COPY: Pondra's writes aren't DataFusion's plans",
    "the runner's own": "a table or function DataFusion's runner makes in Rust before the file runs (range_partitioned, table_with_metadata, union_table, async_abs, …): no SQL in the file makes it",
    "the node's memory": "datafusion.runtime.* (memory limit, spill folder, caches) is the node's, set as it starts (PONDRA_MEMORY_MB, PONDRA_SPILL_DIR), not a session's",
    "microseconds": "a table's TIMESTAMP keeps microseconds, as Postgres, Delta and Iceberg do; DataFusion's keeps nanoseconds, which the answer shows",
    "an order not asked for": "the same rows in another order, from a query with no ORDER BY: SQL leaves the order open, and Pondra's plans give another",
    "interval arithmetic": "`n * INTERVAL '1 hour'` and `INTERVAL … / n`, which DataFusion refuses and Pondra computes as Postgres does",
}
RUNNER_FUNCTIONS = {"async_abs"}  # (async_udf.slt's: registered by the runner)
MADE = re.compile(r"\s*(?:create\s+(?:or\s+replace\s+)?(?:(?:temp|temporary|external|unbounded|materialized)\s+)*(?:table|view)\s+(?:if\s+not\s+exists\s+)?|select\b.*?\binto\s+)(\"[^\"]+\"|[\w.]+)", re.I | re.S)


def exception(sql, why, made):
    """The named exception a failed record is, if any (`made`: the tables the file makes)."""
    explain = re.match(r"\s*explain\b", sql, re.I)
    if why == "an error was expected" and re.search(r"interval\s+'[^']*'\s*[*/]|[*]\s*interval\s+'", sql, re.I):
        return "interval arithmetic"
    if explain and why.startswith("answer differs"):
        return "plan text"
    if explain and "DML not supported" in why:
        return "a write explained"
    table = re.search(r"table '(?:[^'.]+\.)*([^'.]+)' not found", why)
    function = re.search(r"Invalid function '([^']+)'", why)
    if (table and table.group(1).lower() not in made) or (function and function.group(1) in RUNNER_FUNCTIONS):
        return "the runner's own"
    if "datafusion.runtime." in why:
        return "the node's memory"
    if why.startswith("answer differs") and (re.search(r"want \[.*\d:\d\d\.\d{7,9}\b", why) or re.search(r"Timestamp\((Nanosecond|ns)\b", sql)):
        return "microseconds"
    if why.startswith("answer differs (in another order)") and not re.search(r"\border\s+by\b", outermost(sql), re.I):
        return "an order not asked for"
    return None


def outermost(sql):
    """A query without its parentheses' insides (subqueries, OVER (…)) or its strings."""
    s = re.sub(r"'[^']*'", "''", sql)
    while (t := re.sub(r"\([^()]*\)", "", s)) != s:
        s = t
    return s


def run_file(path, port, spread):
    """(counts, failures) for one file."""
    global PLACES
    counts, failures = collections.Counter(), []
    headers = {**OWNER, "x-pondra-session": uuid.uuid4().hex}  # (a session a file: its SETs and PREPAREs last to its end, as in DataFusion's runner)
    spark = f"{os.sep}spark{os.sep}" in path
    PLACES = 15 if spark else 12
    if spark:  # (DataFusion's runner gives these files Spark's functions; Pondra gives them to Spark's dialect)
        harness.call(port, "POST", "/sql", b"SET datafusion.sql_parser.dialect = 'spark'", timeout=A.timeout, headers=headers)
    all_records = records(path)
    made = {part.lower() for _, _, sql, _, _ in all_records if (m := MADE.match(sql)) for part in m.group(1).strip('"').split(".")}  # (each part: "foo.bar.baz" is read as a table baz)
    for kind, header, sql, expected, line in all_records:
        counts["records"] += 1
        wants_error = header[:1] == ["error"] or (kind == "query" and header[:1] == ["error"])
        try:
            if kind == "statement":
                harness.call(port, "POST", "/sql", sql.encode(), timeout=A.timeout, headers=headers)
                ok = not wants_error
                why = "an error was expected" if wants_error else ""
            else:
                rows = answer(port, sql, spread, headers)
                ok = not wants_error and same(rows, expected, header)
                why = "an error was expected" if wants_error else "answer differs (in another order)" if sorted(" ".join(" ".join(r).split()) for r in rows) == sorted(" ".join(e.split()) for e in expected) else "answer differs"
                if not ok and not wants_error:
                    why += f": got {[' '.join(r) for r in rows[:3]]}, want {expected[:3]}"
        except Exception as e:
            text = str(e)
            if wants_error:
                ok, why = True, ""
                pattern = " ".join(header[1:]) if header[:1] == ["error"] else ""
                if pattern and not re.search(pattern, text, re.S) and pattern not in text:
                    counts["error_text_differs"] += 1
            else:
                ok, why = False, text
        named = None if ok else exception(sql, why, made)
        counts["passed" if ok else "excepted" if named else "failed"] += 1
        if not ok:
            failures.append({"file": os.path.basename(path), "line": line, "sql": sql[:300], "why": why[:500], **({"exception": named} if named else {})})
    return counts, failures


def reason(why):
    """A failure's kind: its first line, with names, numbers and quoted text taken out."""
    first = why.split("\\n")[0].split("\n")[0]
    first = re.sub(r"^\d{3}: b['\"]", "", first)
    first = re.sub(r"'[^']*'|\"[^\"]*\"|`[^`]*`", "…", first)
    first = re.sub(r"\b\d+(\.\d+)?\b", "N", first)
    if first.startswith("answer differs"):
        return "answer differs"
    return first[:140]


def main():
    harness.A = argparse.Namespace(s3=False, keep=False, port=A.port)
    files = sorted(os.path.join(d, f) for d, _, fs in os.walk(A.slt) for f in fs if f.endswith(".slt"))
    if A.only:
        files = [f for f in files if any(o in os.path.relpath(f, A.slt) for o in A.only.split(","))]
    total, failures, per_file = collections.Counter(), [], {}
    t0 = time.time()
    for n, path in enumerate(files, 1):
        lake = harness.new_lake()
        scratch = os.path.join(os.path.dirname(A.slt.rstrip("/")), "test_files", "scratch", os.path.splitext(os.path.basename(path))[0])
        shutil.rmtree(scratch, ignore_errors=True)  # (each file's own, made afresh, as DataFusion's runner does)
        os.makedirs(scratch, exist_ok=True)
        env = {"PONDRA_OWNER_KEY": OWNER["x-pondra-owner"], **({} if A.pondra else {"PONDRA_SQL_OPTIONS": AS_DATAFUSION})}
        nodes = [harness.Node(lake, A.port + i, env=env, cwd=os.path.dirname(A.slt.rstrip("/"))).start() for i in range(A.nodes)]
        try:
            if A.nodes > 1:
                while len(harness.call(A.port, "GET", "/stats")["nodes"]) < A.nodes:
                    time.sleep(0.1)
            counts, fails = run_file(path, A.port, A.nodes > 1)
        except Exception as e:
            counts, fails = collections.Counter(records=1, failed=1), [{"file": os.path.basename(path), "line": 0, "sql": "", "why": f"the file didn't run: {e}"}]
        finally:
            [node.kill() for node in nodes]
            harness.clean_up()
        rel = os.path.relpath(path, A.slt)
        per_file[rel] = dict(counts)
        total.update(counts)
        failures.extend(fails)
        print(f"[{n}/{len(files)}] {rel}: {counts['passed']}/{counts['records']}", flush=True)
    kinds = collections.Counter(reason(f["why"]) for f in failures if "exception" not in f)
    named = collections.Counter(f["exception"] for f in failures if "exception" in f)
    rate = total["passed"] / max(total["records"] - total["excepted"], 1)
    raw = total["passed"] / max(total["records"], 1)
    out = {"datafusion": "55.1.0", "nodes": A.nodes, "defaults": "pondra" if A.pondra else AS_DATAFUSION, "files": len(files), "records": total["records"], "passed": total["passed"],
           "excepted": total["excepted"], "pass_rate": round(rate, 4), "raw_pass_rate": round(raw, 4), "error_text_differs": total["error_text_differs"], "secs": round(time.time() - t0),
           "exceptions": {k: {"records": named[k], "why": EXCEPTIONS[k]} for k in EXCEPTIONS}, "failure_kinds": kinds.most_common(), "per_file": per_file, "failures": failures}
    print(f"\n{total['passed']} of {total['records'] - total['excepted']} records pass ({100 * rate:.1f}%), {len(files)} files, {A.nodes} node(s); "
          f"{total['excepted']} more fail for a named reason ({100 * raw:.1f}% of all {total['records']})")
    for k in EXCEPTIONS:
        print(f"{named[k]:6}  [{k}] {EXCEPTIONS[k]}")
    print()
    for k, n in kinds.most_common(25):
        print(f"{n:6}  {k}")
    if A.out:
        json.dump(out, open(A.out, "w"), indent=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--slt", required=True)
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--only", default="")
    ap.add_argument("--port", type=int, default=8900)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--pondra", action="store_true", help="Pondra's own defaults, not DataFusion's runner's (AS_DATAFUSION)")
    ap.add_argument("--out")
    A = ap.parse_args()
    main()
