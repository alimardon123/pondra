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

A record passes when Pondra answers as the file says. An error the file expects counts as passed
when Pondra refuses too, whatever its words (`error_text_differs` counts those whose words differ).
The failures are grouped by their first line, so each can be understood: what Pondra doesn't do
(`CREATE EXTERNAL TABLE`, session `SET`, a file from DataFusion's test data it doesn't have), what
it does differently on purpose, and what is wrong.
"""
import argparse, collections, decimal, hashlib, io, json, os, re, sys, time
import numpy

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
        return plain(decimal.Decimal(text), 12)
    if pa.types.is_decimal(t):
        return plain(v, t.scale) if top else str(v)
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t):
        return ("(empty)" if v == "" else v.rstrip("\n").replace("\x00", "\\0")) if top else v
    if pa.types.is_integer(t):
        return str(v)
    if pa.types.is_date(t):
        return v.isoformat()
    if pa.types.is_timestamp(t) or pa.types.is_time(t):
        return moment(v, t)
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t) or pa.types.is_list_view(t):
        return "[" + ", ".join(cell(x, t.value_type, False) for x in v) + "]"
    if pa.types.is_map(t):
        return "{" + ", ".join(f"{cell(k, t.key_type, False)}: {cell(x, t.item_type, False)}" for k, x in v) + "}"
    if pa.types.is_struct(t):
        return "{" + ", ".join(f"{f.name}: {cell(v.get(f.name), f.type, False)}" for f in t) + "}"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_binary_view(t) or pa.types.is_fixed_size_binary(t):
        return bytes(v).hex()
    return str(v)


def moment(v, t):
    """A time or timestamp as Arrow shows it: seconds' fractions in 3, 6 or 9 digits as needed,
    an offset (`Z` for UTC) when the type has a zone."""
    import datetime, pyarrow as pa
    nanos = getattr(v, "nanosecond", 0) + 1000 * v.microsecond if hasattr(v, "microsecond") else 0
    base = v.strftime("%H:%M:%S") if pa.types.is_time(t) else v.strftime("%Y-%m-%dT%H:%M:%S")
    if pa.types.is_timestamp(t) and v.year < 1000:
        base = f"{v.year:04}" + base[len(str(v.year)):]
    frac = "" if nanos == 0 else f".{nanos // 1_000_000:03}" if nanos % 1_000_000 == 0 else f".{nanos // 1000:06}" if nanos % 1000 == 0 else f".{nanos:09}"
    zone = ""
    if pa.types.is_timestamp(t) and t.tz is not None and getattr(v, "tzinfo", None) is not None:
        off = v.utcoffset() or datetime.timedelta(0)
        mins = int(off.total_seconds()) // 60
        zone = "Z" if mins == 0 else f"{'+' if mins > 0 else '-'}{abs(mins) // 60:02}:{abs(mins) % 60:02}"
    return base + frac + zone


def plain(d, places):
    d = decimal.Decimal(d).quantize(decimal.Decimal(1).scaleb(-min(places, 12)) if places > 0 else decimal.Decimal(1), rounding=decimal.ROUND_HALF_EVEN) if places is not None else decimal.Decimal(d)
    if d == 0:
        return "0"
    s = format(d.normalize(), "f")
    return s


def answer(port, sql, spread):
    """The query's rows as the file writes them, or raises with Pondra's error."""
    import pyarrow as pa
    path = "/sql?format=arrow" + ("&spread=1" if spread else "")
    data = harness.call(port, "POST", path, sql.encode(), timeout=A.timeout)
    if isinstance(data, (dict, list)):
        return [[json.dumps(data)]]  # (a statement's answer, not rows)
    rows = []
    reader = pa.ipc.open_stream(io.BytesIO(data))
    for batch in reader:
        cols = [(c.to_pylist(), c.type) for c in batch.columns]
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
    return [words(r) for r in got] == [words(r) for r in expected]


def run_file(path, port, spread):
    """(counts, failures) for one file."""
    counts, failures = collections.Counter(), []
    for kind, header, sql, expected, line in records(path):
        counts["records"] += 1
        wants_error = header[:1] == ["error"] or (kind == "query" and header[:1] == ["error"])
        try:
            if kind == "statement":
                harness.call(port, "POST", "/sql", sql.encode(), timeout=A.timeout)
                ok = not wants_error
                why = "an error was expected" if wants_error else ""
            else:
                rows = answer(port, sql, spread)
                ok = not wants_error and same(rows, expected, header)
                why = "an error was expected" if wants_error else "answer differs"
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
        counts["passed" if ok else "failed"] += 1
        if not ok:
            failures.append({"file": os.path.basename(path), "line": line, "sql": sql[:300], "why": why[:500]})
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
        nodes = [harness.Node(lake, A.port + i).start() for i in range(A.nodes)]
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
    kinds = collections.Counter(reason(f["why"]) for f in failures)
    rate = total["passed"] / max(total["records"], 1)
    out = {"datafusion": "55.1.0", "nodes": A.nodes, "files": len(files), "records": total["records"], "passed": total["passed"],
           "pass_rate": round(rate, 4), "error_text_differs": total["error_text_differs"], "secs": round(time.time() - t0),
           "failure_kinds": kinds.most_common(), "per_file": per_file, "failures": failures}
    print(f"\n{total['passed']} of {total['records']} records pass ({100 * rate:.1f}%), {len(files)} files, {A.nodes} node(s)")
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
    ap.add_argument("--out")
    A = ap.parse_args()
    main()
