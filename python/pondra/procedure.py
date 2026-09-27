"""Runs a Python procedure for a node (`CALL name(…)`, ADR-023): `python -m pondra.procedure`.

The node writes a JSON line (the body, and how to reach it: a token with its caller's rights) and
the arguments as one Arrow row; the body runs with `con` (a connection back to that node), `pondra`
and each parameter as a name. The value of its last line is the answer, written to standard output
after a JSON line saying what it is: a frame's SQL (the node runs it), rows (Arrow), nothing, or
the error it raised.
Anything else the body prints goes to the node's log.
"""
import ast
import json
import os
import sys
import traceback


def main():
    answer = os.fdopen(os.dup(1), "wb")  # (the answer's own channel: print() goes to the node's log)
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    head = json.loads(sys.stdin.buffer.readline())
    import pyarrow as pa
    import pondra
    args = pa.ipc.open_stream(sys.stdin.buffer.read()).read_all().to_pylist()
    con = pondra.connect(head["url"], token=head["token"], headers={"x-pondra-depth": str(head["depth"])}, job=head.get("job"))
    scope = {"__name__": "__procedure__", "con": con, "pondra": pondra, **(args[0] if args else {})}
    try:
        kind, data = reply(run(head["body"], scope, head["name"]), head["name"])
    except Exception as e:  # (the traceback to the node's log; the error itself back to the caller)
        traceback.print_exc()
        kind, data = {"kind": "error", "error": f"{type(e).__name__}: {e}"}, b""
    answer.write(json.dumps(kind).encode() + b"\n" + data)
    answer.flush()


def run(body, scope, name):
    """The body, as a notebook cell runs: the value of its last line, if that is an expression."""
    tree = ast.parse(body, filename=name)
    last = tree.body.pop() if tree.body and isinstance(tree.body[-1], ast.Expr) else None
    exec(compile(tree, name, "exec"), scope)
    return eval(compile(ast.Expression(last.value), name, "eval"), scope) if last else None


def reply(value, name):
    import io
    import pyarrow as pa
    from pondra.client import Result, _arrow
    from pondra.frame import Frame
    if value is None or isinstance(value, dict) and not value:
        return {"kind": "none"}, b""
    if isinstance(value, Frame) and not value._sent and not value._params:
        return {"kind": "sql", "sql": value.sql}, b""  # (the node runs it: it may spread, and nothing crosses twice)
    if isinstance(value, (Frame, Result)):
        value = value.collect()
    elif isinstance(value, dict) and not all(isinstance(v, (list, tuple)) for v in value.values()):
        value = [value]  # (one row)
    elif not (isinstance(value, (list, tuple, dict)) or hasattr(value, "to_arrow") or hasattr(value, "schema") or type(value).__module__.startswith("pandas")):
        value = pa.table({name.split(".")[-1]: [value]})  # (one value)
    table = pa.table(value) if isinstance(value, dict) else _arrow(value)
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as w:
        w.write(table) if isinstance(table, pa.RecordBatch) else w.write_table(table)
    return {"kind": "rows"}, buf.getvalue()


if __name__ == "__main__":
    main()
