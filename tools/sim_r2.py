#!/usr/bin/env python3
"""Local S3-compatible server (moto) with R2-like latency, for testing without network access.

Latency per request is lognormal. Defaults follow Tigris's published R2 load-phase numbers
(PUT p50 ≈ 197 ms, p90 ≈ 340 ms; a competitor's benchmark, so treat as pessimistic).
GET/HEAD/LIST default to p50 100 ms, p90 170 ms (assumption). Request counts: GET /__sim/stats.

  sim_r2.py --port 9000 [--put-p50 197 --get-p50 100 --sigma 0.426] [--zero] [--mbps 40]
            [--writes-per-sec N] [--key-writes-per-sec 1]

`--mbps` paces a GET's body as one connection to a bucket carries it (R2 gives each some tens of MB a second, and many
connections at once each their own), so ranges fetched side by side are rewarded as they are on R2 (a range is then
served without moto's copy of the whole object); the stats count the bytes served and the most GETs in flight at once.

The store's limits (C5), when asked for: more than N writes a second to a bucket are answered
503 SlowDown, as S3 answers a hot prefix; a key written again within the second is answered 429,
as R2 does. A key is written by a PUT of an object or a multipart upload's completion: an upload's
start and its parts, and a bulk delete (a POST to the bucket itself), write none (R2's documents say
only "concurrent writes to the same key"; limits-r2.yml logs what R2 itself refuses). Refusals per
second are in the stats (`second`: [writes, 503s, 429s]). `ops` counts requests as a bucket bills them:
`write` (PUT, a multipart upload's start, parts and end, a copy: class A), `list` (class A), `read`
(GET and HEAD: class B) and `delete` (one, or a bulk delete's POST: free).
"""
import argparse, json, math, random, re, threading, time, urllib.parse
from collections import Counter
from moto.moto_server.werkzeug_app import DomainDispatcherApplication, create_backend_app
from werkzeug.serving import run_simple

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, default=9000)
ap.add_argument("--put-p50", type=float, default=197)
ap.add_argument("--get-p50", type=float, default=100)
ap.add_argument("--sigma", type=float, default=0.426)  # p90/p50 ≈ 1.73, as in the published numbers
ap.add_argument("--zero", action="store_true", help="no added latency")
ap.add_argument("--mbps", type=float, default=0, help="a GET's body at most this many MB a second (0: no limit)")
ap.add_argument("--trace", action="store_true", help="print every request")
ap.add_argument("--writes-per-sec", type=int, default=0, help="503 SlowDown above this many writes a second (0: no limit)")
ap.add_argument("--key-writes-per-sec", type=int, default=0, help="429 for a key written again within the second (1: R2's)")
a = ap.parse_args()

counts, ops, lock = Counter(), Counter(), threading.Lock()
inflight = [0]
seconds, last_write = {}, {}  # (second -> [writes, 503s, 429s]; key -> when last written)
backend = DomainDispatcherApplication(create_backend_app)


def app(environ, start_response):
    method, path = environ["REQUEST_METHOD"], environ.get("PATH_INFO", "")
    if path == "/__sim/stats":
        start_response("200 OK", [("Content-Type", "application/json")])
        with lock:
            return [json.dumps({**counts, "ops": ops, "second": {str(k): v for k, v in sorted(seconds.items())}}).encode()]
    kind = "PUT" if method in ("PUT", "POST", "DELETE") else ("LIST" if "list-type" in environ.get("QUERY_STRING", "") else "GET")
    t0 = time.time()
    refuse, q = None, urllib.parse.parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
    keyed = (method == "PUT" and "partNumber" not in q) or (method == "POST" and "uploadId" in q)
    op = ("delete" if method == "DELETE" or (method == "POST" and "delete" in q) else "write" if method in ("PUT", "POST")
          else "list" if kind == "LIST" else "read")
    with lock:
        counts[kind] += 1
        ops[op] += 1
        if method in ("PUT", "POST"):
            sec = seconds.setdefault(int(t0), [0, 0, 0])
            sec[0] += 1
            if a.key_writes_per_sec and keyed and t0 - last_write.get(path, 0) < 1 / a.key_writes_per_sec:
                refuse, sec[2] = ("429 Too Many Requests", "TooManyRequests"), sec[2] + 1
            elif a.writes_per_sec and sec[0] - sec[1] - sec[2] > a.writes_per_sec:
                refuse, sec[1] = ("503 Slow Down", "SlowDown"), sec[1] + 1
            elif keyed:
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
        print(f"{t0:.3f} {op:6} {environ.get('PATH_INFO', '')}?{environ.get('QUERY_STRING', '')[:60]} {environ.get('HTTP_RANGE', '')}", flush=True)
    if a.mbps and method == "GET" and kind == "GET":
        fast = ranged(environ, start_response) if environ.get("HTTP_RANGE", "").startswith("bytes=") and not q else None
        return paced(fast if fast is not None else backend(environ, start_response))
    return backend(environ, start_response)


held = {}  # (bucket, key) -> (e-tag, bytes): an object's bytes read once, for `ranged` (at most 2 GB of them)


def ranged(environ, start_response):
    """With `--mbps`, a range of an object as moto answers it, but without moto's copy of the whole object at every
    request (which held 16 GETs at once to 50 MB a second, whatever `--mbps` gave each). None for anything else (a
    suffix, a query, an object moto doesn't hold): moto answers it."""
    from moto.core import DEFAULT_ACCOUNT_ID
    from moto.s3.models import s3_backends
    bucket, _, name = urllib.parse.unquote(environ.get("PATH_INFO", "")).lstrip("/").partition("/")
    try:
        key = s3_backends[DEFAULT_ACCOUNT_ID]["global"].get_object(bucket, name)
    except Exception:
        return None
    m = re.fullmatch(r"bytes=(\d*)-(\d*)", environ["HTTP_RANGE"].strip())
    if key is None or not m or m.group(1) == "":
        return None
    etag = key.etag
    if (bucket, name) not in held or held[(bucket, name)][0] != etag:
        if sum(len(b) for _, b in held.values()) > 2 << 30:
            held.clear()
        held[(bucket, name)] = (etag, key.value)
    data = held[(bucket, name)][1]
    first, last = int(m.group(1)), min(int(m.group(2) or len(data) - 1), len(data) - 1)
    if environ.get("HTTP_IF_MATCH") not in (None, etag):
        start_response("412 Precondition Failed", [("Content-Type", "application/xml")])
        return [b"<?xml version='1.0' encoding='UTF-8'?><Error><Code>PreconditionFailed</Code></Error>"]
    if first > last:
        return None
    start_response("206 Partial Content", [("Content-Type", "binary/octet-stream"), ("Content-Length", str(last - first + 1)), ("ETag", etag),
                                           ("Last-Modified", key.last_modified_RFC1123), ("Accept-Ranges", "bytes"), ("Content-Range", f"bytes {first}-{last}/{len(data)}")])
    return [memoryview(data)[first:last + 1]]


def paced(body):
    """A GET's body at `--mbps`, in 64 KB steps, counted in the stats."""
    rate, sent, t = a.mbps * 1e6, 0, time.time()
    with lock:
        inflight[0] += 1
        counts["max_inflight"] = max(counts["max_inflight"], inflight[0])
    try:
        for chunk in body:
            for i in range(0, len(chunk), 1 << 16):
                piece = bytes(chunk[i:i + (1 << 16)])
                sent += len(piece)
                ahead = sent / rate - (time.time() - t)
                if ahead > 0:
                    time.sleep(ahead)
                yield piece
    finally:
        with lock:
            inflight[0] -= 1
            counts["bytes"] += sent
        if hasattr(body, "close"):
            body.close()


run_simple("127.0.0.1", a.port, app, threaded=True)
