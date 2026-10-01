#!/usr/bin/env python3
"""Local S3-compatible server (moto) with R2-like latency, for testing without network access.

Latency per request is lognormal. Defaults follow Tigris's published R2 load-phase numbers
(PUT p50 ≈ 197 ms, p90 ≈ 340 ms; a competitor's benchmark, so treat as pessimistic).
GET/HEAD/LIST default to p50 100 ms, p90 170 ms (assumption). Request counts: GET /__sim/stats.

  sim_r2.py --port 9000 [--put-p50 197 --get-p50 100 --sigma 0.426] [--zero]
            [--writes-per-sec N] [--key-writes-per-sec 1]

The store's limits (C5), when asked for: more than N writes a second to a bucket are answered
503 SlowDown, as S3 answers a hot prefix; a key written again within the second is answered 429,
as R2 does. Refusals per second are in the stats (`second`: [writes, 503s, 429s]).
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
ap.add_argument("--writes-per-sec", type=int, default=0, help="503 SlowDown above this many writes a second (0: no limit)")
ap.add_argument("--key-writes-per-sec", type=int, default=0, help="429 for a key written again within the second (1: R2's)")
a = ap.parse_args()

counts, lock = Counter(), threading.Lock()
seconds, last_write = {}, {}  # (second -> [writes, 503s, 429s]; key -> when last written)
backend = DomainDispatcherApplication(create_backend_app)


def app(environ, start_response):
    method, path = environ["REQUEST_METHOD"], environ.get("PATH_INFO", "")
    if path == "/__sim/stats":
        start_response("200 OK", [("Content-Type", "application/json")])
        with lock:
            return [json.dumps({**counts, "second": {str(k): v for k, v in sorted(seconds.items())}}).encode()]
    kind = "PUT" if method in ("PUT", "POST", "DELETE") else ("LIST" if "list-type" in environ.get("QUERY_STRING", "") else "GET")
    t0 = time.time()
    refuse = None
    with lock:
        counts[kind] += 1
        if method in ("PUT", "POST"):
            sec = seconds.setdefault(int(t0), [0, 0, 0])
            sec[0] += 1
            if a.key_writes_per_sec and t0 - last_write.get(path, 0) < 1 / a.key_writes_per_sec:
                refuse, sec[2] = ("429 Too Many Requests", "TooManyRequests"), sec[2] + 1
            elif a.writes_per_sec and sec[0] - sec[1] - sec[2] > a.writes_per_sec:
                refuse, sec[1] = ("503 Slow Down", "SlowDown"), sec[1] + 1
            else:
                last_write[path] = t0
            if refuse:
                counts[refuse[1]] += 1
    if refuse:
        start_response(refuse[0], [("Content-Type", "application/xml")])
        return [f"<?xml version='1.0' encoding='UTF-8'?><Error><Code>{refuse[1]}</Code><Message>Please reduce your request rate.</Message></Error>".encode()]
    if not a.zero:
        p50 = a.put_p50 if kind == "PUT" else a.get_p50
        time.sleep(math.exp(math.log(p50) + a.sigma * random.gauss(0, 1)) / 1000)
    if a.trace:
        print(f"{t0:.3f} {kind:4} {environ.get('PATH_INFO', '')}?{environ.get('QUERY_STRING', '')[:60]}", flush=True)
    return backend(environ, start_response)


run_simple("127.0.0.1", a.port, app, threaded=True)
