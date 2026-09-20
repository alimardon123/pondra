#!/usr/bin/env python3
"""Local S3-compatible server (moto) with R2-like latency, for testing without network access.

Latency per request is lognormal. Defaults follow Tigris's published R2 load-phase numbers
(PUT p50 ≈ 197 ms, p90 ≈ 340 ms; a competitor's benchmark, so treat as pessimistic).
GET/HEAD/LIST default to p50 100 ms, p90 170 ms (assumption). Request counts: GET /__sim/stats.

  sim_r2.py --port 9000 [--put-p50 197 --get-p50 100 --sigma 0.426] [--zero]
"""
import argparse, json, math, random, threading, time
from collections import Counter
from moto.moto_server.werkzeug_app import DomainDispatcherApplication, create_backend_app
from werkzeug.serving import run_simple

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, default=9000)
ap.add_argument("--put-p50", type=float, default=197)
ap.add_argument("--get-p50", type=float, default=100)
ap.add_argument("--sigma", type=float, default=0.426)  # p90/p50 ≈ 1.73, as in the published numbers
ap.add_argument("--zero", action="store_true", help="no added latency")
ap.add_argument("--trace", action="store_true", help="print every request")
a = ap.parse_args()

counts, lock = Counter(), threading.Lock()
backend = DomainDispatcherApplication(create_backend_app)


def app(environ, start_response):
    method, path = environ["REQUEST_METHOD"], environ.get("PATH_INFO", "")
    if path == "/__sim/stats":
        start_response("200 OK", [("Content-Type", "application/json")])
        with lock:
            return [json.dumps(counts).encode()]
    kind = "PUT" if method in ("PUT", "POST", "DELETE") else ("LIST" if "list-type" in environ.get("QUERY_STRING", "") else "GET")
    with lock:
        counts[kind] += 1
    t0 = time.time()
    if not a.zero:
        p50 = a.put_p50 if kind == "PUT" else a.get_p50
        time.sleep(math.exp(math.log(p50) + a.sigma * random.gauss(0, 1)) / 1000)
    if a.trace:
        print(f"{t0:.3f} {kind:4} {environ.get('PATH_INFO', '')}?{environ.get('QUERY_STRING', '')[:60]}", flush=True)
    return backend(environ, start_response)


run_simple("127.0.0.1", a.port, app, threaded=True)
