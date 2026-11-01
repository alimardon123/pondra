#!/usr/bin/env python3
"""TPC-H's first (cold) and best (warm) runs over Parquet files on a paced local bucket: Pondra (views over the files)
against DuckDB 2.0 (httpfs), the request counts of each.

  paced_bucket.py --files DIR [--engine pondra|duckdb] [--queries 1,3,4,6,12,13,18,22] [--latency-ms 60] [--mbps 40]
  paced_bucket.py serve --files DIR --port 9300 [--latency-ms 60] [--mbps 40]

DIR holds a folder of Parquet files per table (lineitem/, orders/, customer/, …: `COPY … TO` writes them). The bucket is
an HTTP server with ranges whose every request is paced like one connection to an object store: a first-byte wait, then
at most --mbps on that connection, many connections at once each getting their own. So it rewards what R2 rewards
(ranges fetched side by side, and fewer bytes), which a simulator answering one request at a time (`sim_r2.py`) doesn't.
Its counts are at /__stats (?reset zeroes them). PONDRA_BIN picks the binary, as for the harness.
"""
import argparse, glob, json, os, re, subprocess, sys, threading, time, types, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


def serve(a):
    from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
    lock, stats = threading.Lock(), {"gets": 0, "heads": 0, "bytes": 0, "inflight": 0, "max_inflight": 0, "sizes": []}
    root = os.path.abspath(a.files)

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def path_of(self):
            p = os.path.abspath(os.path.join(root, self.path.split("?")[0].lstrip("/")))
            return p if p.startswith(root + os.sep) and os.path.isfile(p) else None

        def headers_of(self, p, status, n):
            st = os.stat(p)
            self.send_response(status)
            self.send_header("Content-Length", str(n))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", f'"{int(st.st_mtime)}-{st.st_size}"')
            self.send_header("Last-Modified", self.date_time_string(st.st_mtime))

        def do_HEAD(self):
            p = self.path_of()
            if not p:
                return self.send_error(404)
            with lock:
                stats["heads"] += 1
            time.sleep(a.latency_ms / 1000)
            self.headers_of(p, 200, os.path.getsize(p))
            self.end_headers()

        def do_GET(self):
            if self.path.startswith("/__stats"):
                with lock:
                    body = json.dumps({k: v for k, v in stats.items() if k != "sizes"} | {"mb_per_get": [round(s / 1e6, 2) for s in sorted(stats["sizes"])[-12:]]}).encode()
                    if "reset" in self.path:
                        stats.update(gets=0, heads=0, bytes=0, max_inflight=0, sizes=[])
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                return self.wfile.write(body)
            p = self.path_of()
            if not p:
                return self.send_error(404)
            size = os.path.getsize(p)
            start, end, status = 0, size - 1, 200
            m = re.match(r"bytes=(\d*)-(\d*)", self.headers.get("Range", ""))
            if m:
                status = 206
                if m.group(1) == "":
                    start = max(0, size - int(m.group(2)))
                else:
                    start = int(m.group(1))
                    end = min(size - 1, int(m.group(2))) if m.group(2) else size - 1
            n = end - start + 1
            with lock:
                stats.update(gets=stats["gets"] + 1, bytes=stats["bytes"] + n, inflight=stats["inflight"] + 1)
                stats["sizes"].append(n)
                stats["max_inflight"] = max(stats["max_inflight"], stats["inflight"])
            try:
                time.sleep(a.latency_ms / 1000)
                self.headers_of(p, status, n)
                if status == 206:
                    self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
                self.end_headers()
                rate = a.mbps * 1e6
                with open(p, "rb") as f:
                    f.seek(start)
                    left, t0, sent = n, time.monotonic(), 0
                    while left > 0:
                        b = f.read(min(256 << 10, left))
                        self.wfile.write(b)
                        left, sent = left - len(b), sent + len(b)
                        ahead = sent / rate - (time.monotonic() - t0)
                        if ahead > 0:
                            time.sleep(ahead)
            finally:
                with lock:
                    stats["inflight"] -= 1

    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer.request_queue_size = 256
    ThreadingHTTPServer(("127.0.0.1", a.port), H).serve_forever()


def run(a):
    srv = f"http://127.0.0.1:{a.port}"
    sim = subprocess.Popen([sys.executable, os.path.abspath(__file__), "serve", "--files", a.files, "--port", str(a.port), "--latency-ms", str(a.latency_ms), "--mbps", str(a.mbps)])
    stats = lambda reset=False: json.loads(urllib.request.urlopen(f"{srv}/__stats{'?reset' if reset else ''}").read())
    for _ in range(100):
        try:
            stats()
            break
        except OSError:
            time.sleep(0.1)
    tables = sorted(d for d in os.listdir(a.files) if glob.glob(os.path.join(a.files, d, "*.parquet")))
    files = {t: [f"{srv}/{t}/{os.path.basename(f)}" for f in sorted(glob.glob(os.path.join(a.files, t, "*.parquet")))] for t in tables}
    qs = [int(q) for q in a.queries.split(",")]
    text = {q: open(os.path.join(HERE, "tpch-queries", f"q{q}.sql")).read() for q in qs}
    out, node = {}, None
    try:
        if a.engine == "duckdb":
            import duckdb
            c = duckdb.connect()
            for t, fs in files.items():
                c.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet({fs!r})")
            ask = lambda sql: c.execute(sql).fetchall()
        else:
            sys.path.insert(0, os.path.dirname(HERE))
            import harness
            harness.A = types.SimpleNamespace(port=a.pondra_port, s3=False, keep=False)
            node = harness.Node(harness.new_lake(), a.pondra_port, env={"PONDRA_HOT_GB": "0"}).start()
            harness.sql(a.pondra_port, f"CREATE SECRET paced (TYPE http, BEARER_TOKEN 'x', SCOPE '{srv}')")
            for t, fs in files.items():
                harness.sql(a.pondra_port, f"CREATE VIEW {t} AS SELECT * FROM read_parquet([{', '.join(repr(f) for f in fs)}])")
            ask = lambda sql: harness.sql(a.pondra_port, sql)
        stats(True)
        for q in qs:
            times = []
            for r in range(a.runs):
                t0 = time.time()
                ask(f"{text[q]}\n-- run {r}")  # (a new text each run: Pondra's result cache never answers)
                times.append(round(time.time() - t0, 3))
                if r == 0:
                    out[q] = {"cold": times[0], "net": stats(True)}
            out[q]["warm"] = min(times[1:] or times)
    finally:
        if node:
            node.kill()
            harness.clean_up()
        sim.kill()
    for q, r in out.items():
        n = r["net"]
        print(f"q{q:<3} cold {r['cold']:>6.2f}  warm {r['warm']:>6.2f}  gets {n['gets']:>4} heads {n['heads']:>3} MB {n['bytes'] / 1e6:>7.1f} max_inflight {n['max_inflight']:>3} biggest {n['mb_per_get'][-3:]}")
    print(f"total cold {sum(r['cold'] for r in out.values()):.2f}  warm {sum(r['warm'] for r in out.values()):.2f}")
    if a.out:
        json.dump({"engine": a.engine, "latency_ms": a.latency_ms, "mbps": a.mbps, "queries": out}, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    serving = sys.argv[1:2] == ["serve"]
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", required=True)
    ap.add_argument("--port", type=int, default=9300)
    ap.add_argument("--latency-ms", type=float, default=60)
    ap.add_argument("--mbps", type=float, default=40)
    if not serving:
        ap.add_argument("--engine", choices=["pondra", "duckdb"], default="pondra")
        ap.add_argument("--queries", default="1,3,4,6,12,13,18,22")
        ap.add_argument("--runs", type=int, default=2)
        ap.add_argument("--pondra-port", type=int, default=8390)
        ap.add_argument("--out")
    a = ap.parse_args(sys.argv[2:] if serving else sys.argv[1:])
    serve(a) if serving else run(a)
