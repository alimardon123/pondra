#!/usr/bin/env python3
"""How fast rows leave a node, by each door: the Postgres protocol (psycopg, text and binary rows;
COPY binary; the ADBC Postgres driver, which reads through COPY into Arrow), Arrow Flight SQL
(ADBC) and HTTP (Arrow IPC). One table of `--rows` rows, four columns; the whole table each time,
best of `--runs`.
  pg_bench.py [--rows 1000000] [--runs 3]"""
import argparse, io, json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, new_lake


def main():
    import psycopg, pyarrow as pa, adbc_driver_postgresql.dbapi as pgadbc, adbc_driver_flightsql.dbapi as flight
    lake = new_lake()
    pg, fl = A.port + 1, A.port + 2
    node = Node(lake, A.port, pg=f"127.0.0.1:{pg}", flight=f"127.0.0.1:{fl}").start()
    call(A.port, "POST", "/sql", f"CREATE TABLE t AS SELECT value AS id, value % 1000 AS k, value * 0.5 AS v, 'name ' || (value % 97) AS s FROM generate_series(1, {A.rows})".encode(), timeout=600)
    q = "SELECT id, k, v, s FROM t"
    dsn = f"host=127.0.0.1 port={pg} user=u dbname=lake"
    def psycopg_rows(binary):
        with psycopg.connect(dsn) as c:
            return len(c.cursor(binary=binary).execute(q).fetchall())
    def psycopg_copy():
        with psycopg.connect(dsn) as c, c.cursor().copy(f"COPY ({q}) TO STDOUT (FORMAT binary)") as cp:
            cp.set_types(["int8", "int8", "float8", "text"])
            return sum(1 for _ in cp.rows())
    def adbc(module, uri):
        with module.connect(uri) as c, c.cursor() as cur:
            cur.execute(q)
            return cur.fetch_arrow_table().num_rows
    def http_arrow():
        body = call(A.port, "POST", "/sql?format=arrow", q.encode(), timeout=600)
        return pa.ipc.open_stream(io.BytesIO(body)).read_all().num_rows
    ways = {"Postgres, psycopg text rows": lambda: psycopg_rows(False), "Postgres, psycopg binary rows": lambda: psycopg_rows(True),
            "Postgres COPY binary, psycopg": psycopg_copy, "Postgres COPY binary, ADBC (Arrow)": lambda: adbc(pgadbc, f"postgresql://u@127.0.0.1:{pg}/lake"),
            "Flight SQL, ADBC (Arrow)": lambda: adbc(flight, f"grpc://127.0.0.1:{fl}"), "HTTP, Arrow IPC": http_arrow}
    out = {}
    for name, f in ways.items():
        best = None
        for _ in range(A.runs):
            t = time.time()
            n = f()
            best = min(best or 1e9, time.time() - t)
        out[name] = {"s": round(best, 2), "rows_per_s": round(n / best), "rows": n}
    node.kill()
    print(json.dumps({"rows": A.rows, "ways": out}, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--port", type=int, default=8600)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true")
    A = harness.A = ap.parse_args()
    main()
