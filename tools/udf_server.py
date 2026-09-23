#!/usr/bin/env python3
"""A function for Pondra, in Python: an Arrow Flight server that takes a batch of arguments and
returns one column.

  udf_server.py [--port 8815] [--module my_functions]

  curl -XPOST localhost:8080/functions/shout -d '{"flight": "http://127.0.0.1:8815",
       "args": ["Utf8"], "returns": "Utf8"}'
  pondra sql "SELECT shout(name) FROM people"

Pondra sends the rows it has as one record batch, with the arguments as columns `arg0`, `arg1`, …
This server answers with a table of one column, the same length. Write the function here (or in
`--module`, as `run(table) -> pyarrow.Array | list`) and keep whatever it needs — a model, a
tokenizer, a GPU — loaded in this process.
"""
import argparse, importlib
import pyarrow as pa
import pyarrow.flight as fl


def run(table):
    """The default function: SHOUT the first argument (replace it with your own)."""
    return pa.array([None if v is None else str(v).upper() for v in table.column(0).to_pylist()])


class Server(fl.FlightServerBase):
    def __init__(self, location, fn):
        super().__init__(location)
        self.fn = fn

    def do_exchange(self, context, descriptor, reader, writer):
        table = reader.read_all()
        out = self.fn(table)
        if not isinstance(out, (pa.Array, pa.ChunkedArray)):
            out = pa.array(out)
        answer = pa.table({"out": out})
        writer.begin(answer.schema)
        writer.write_table(answer)
        writer.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8815)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--module", help="a module with run(table) -> column")
    a = ap.parse_args()
    fn = importlib.import_module(a.module).run if a.module else run
    print(f"function server on grpc://{a.host}:{a.port}", flush=True)
    Server(f"grpc://{a.host}:{a.port}", fn).serve()
