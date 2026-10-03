#!/usr/bin/env python3
"""An S3 endpoint that fails on purpose: a proxy in front of a real one (moto, sim_r2.py, MinIO)
that answers some requests with errors, slows them, hangs them, drops replies after the store
applied them, or refuses everything for a while (stdlib only).

  faulty_s3.py --upstream http://127.0.0.1:9000 --port 9100
  curl -X POST localhost:9100/__faults -d '{"error": 0.1, "lost": 0.05}'   # change them while it runs
  curl localhost:9100/__faults                                           # what it is doing, and counts

The faults (all off by default; probabilities per request; `methods` limits them, e.g. ["PUT"]):
  error  answered 500 InternalError or 503 SlowDown, never reaching the store
  lost   reaching the store, then the connection dropped before the reply: the store applied a
         PUT the client never heard about (the case exactly-once has to survive)
  hang   held `hang_secs` (default 30) and then dropped, never reaching the store
  slow_ms  added to every request
  down   every connection closed at once without an answer: the store unreachable

tools/resilience_check.py starts one in-process for each node (`Faulty`), so one node can lose
the bucket while the others keep it.

In front of a real bucket (R2, S3: `--sign`, or `Faulty(…, sign=(key, secret, region))`), it
signs each request again for the bucket's host with those keys: a client signs for the proxy's
address, which the bucket would refuse. It reuses a TLS connection per client connection, and goes
through `HTTPS_PROXY` when one is set.
"""
import argparse, hashlib, hmac, http.client, http.server, json, os, random, socketserver, ssl, threading, time, urllib.parse

OFF = {"error": 0.0, "lost": 0.0, "hang": 0.0, "hang_secs": 30.0, "slow_ms": 0, "down": False, "methods": []}
ERRORS = [("500 Internal Server Error", "InternalError", "We encountered an internal error. Please try again."),
          ("503 Slow Down", "SlowDown", "Please reduce your request rate.")]


class Faulty:
    """A proxy on 127.0.0.1:`port` (0: any free one) to `upstream`, its faults changed by `set`."""

    def __init__(self, upstream, port=0, sign=None):
        u = urllib.parse.urlsplit(upstream)
        self.tls, self.sign = u.scheme == "https", sign
        self.up = (u.hostname, u.port or (443 if self.tls else 80))
        self.host = u.hostname if u.port in (None, 443, 80) else f"{u.hostname}:{u.port}"
        self.faults, self.counts, self.lock = dict(OFF), {}, threading.Lock()
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"  # (keep-alive, as the store's clients use it)

            def log_message(self, *_):
                pass

            def handle_one_request(self):
                try:
                    super().handle_one_request()
                except (ConnectionError, TimeoutError):
                    self.close_connection = True

            def any(self):
                if self.path.startswith("/__faults"):
                    return proxy.control(self)
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                fault = proxy.draw(self.command)
                if fault == "down":
                    return self.drop()
                if fault == "hang":
                    time.sleep(proxy.faults["hang_secs"])
                    return self.drop()
                if proxy.faults["slow_ms"]:
                    time.sleep(proxy.faults["slow_ms"] / 1000)
                if fault == "error":
                    status, code, message = random.choice(ERRORS)
                    return self.answer(int(status[:3]), status[4:], [("Content-Type", "application/xml")],
                                       f"<?xml version='1.0' encoding='UTF-8'?><Error><Code>{code}</Code><Message>{message}</Message></Error>".encode())
                path, headers = self.path, {k: v for k, v in self.headers.items() if k.lower() != "connection"}
                if proxy.sign:
                    path, headers = signed(self.command, path, headers, body, proxy.host, *proxy.sign)
                if not proxy.tls:
                    c = http.client.HTTPConnection(*proxy.up, timeout=300)
                    try:
                        c.request(self.command, path, body, headers)
                        r = c.getresponse()
                        data = r.read()
                    finally:
                        c.close()
                else:  # (a TLS handshake a request would cost a real bucket's every request a round trip or two)
                    for again in (True, False):
                        reused = getattr(self, "up", None) is not None
                        self.up = self.up if reused else proxy.connect()
                        try:
                            self.up.request(self.command, path, body, headers)
                            r = self.up.getresponse()
                            data = r.read()
                            break
                        except (OSError, http.client.HTTPException):
                            self.up.close()
                            self.up = None
                            if not (again and reused):  # (only a kept connection the bucket closed is tried again)
                                raise
                if fault == "lost":
                    return self.drop()
                headers = [(k, v) for k, v in r.getheaders() if k.lower() not in ("transfer-encoding", "connection", "content-length")]
                self.answer(r.status, r.reason, headers, data, r.getheader("Content-Length") if self.command == "HEAD" else None)

            def finish(self):
                if getattr(self, "up", None):
                    self.up.close()
                super().finish()

            def answer(self, status, reason, headers, data, length=None):
                self.send_response_only(status, reason)
                for k, v in headers:
                    self.send_header(k, v)
                self.send_header("Content-Length", length or str(len(data)))  # (a HEAD's is the object's)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)

            def drop(self):
                self.close_connection = True
                try:
                    self.connection.shutdown(2)
                except OSError:
                    pass

            do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = any

        class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads, allow_reuse_address = True, True

        self.server = Server(("127.0.0.1", port), Handler)
        self.port = self.server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def connect(self):
        ctx = ssl.create_default_context(cafile=os.environ.get("SSL_CERT_FILE") or os.environ.get("AWS_CA_BUNDLE") or None)
        via = urllib.parse.urlsplit(os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or "")
        if not via.hostname:
            return http.client.HTTPSConnection(*self.up, timeout=300, context=ctx)
        c = http.client.HTTPSConnection(via.hostname, via.port or 80, timeout=300, context=ctx)
        c.set_tunnel(*self.up)
        return c

    def set(self, **faults):
        """Change the faults; `set()` with none turns every one off."""
        with self.lock:
            self.faults = {**self.faults, **faults} if faults else dict(OFF)

    def draw(self, method):
        with self.lock:
            f = self.faults
            self.counts[method] = self.counts.get(method, 0) + 1
            fault = None
            if not f["methods"] or method in f["methods"]:
                if f["down"]:
                    fault = "down"
                else:
                    x = random.random()
                    for name in ("error", "lost", "hang"):
                        if x < f[name]:
                            fault = name
                            break
                        x -= f[name]
            if fault:
                self.counts[fault] = self.counts.get(fault, 0) + 1
            return fault

    def control(self, h):
        if h.command == "POST":
            self.set(**json.loads(h.rfile.read(int(h.headers.get("Content-Length") or 0)) or b"{}"))
        with self.lock:
            data = json.dumps({"faults": self.faults, "counts": self.counts}).encode()
        h.answer(200, "OK", [("Content-Type", "application/json")], data)

    def close(self):
        self.server.shutdown()


def signed(method, path, headers, body, host, key, secret, region):
    """The request signed again (AWS Signature Version 4) for `host`: its path and query encoded
    as the signature has them, and its headers with a new `Authorization`."""
    q = lambda x, safe: urllib.parse.quote(urllib.parse.unquote(x), safe=safe)
    path, _, query = path.partition("?")
    path = q(path, "/-_.~")
    pairs = sorted((q(k, "-_.~"), q(v, "-_.~")) for k, _, v in (p.partition("=") for p in query.split("&") if p))
    query = "&".join(f"{k}={v}" for k, v in pairs)
    when = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    drop = ("authorization", "host", "x-amz-date", "x-amz-content-sha256", "x-amz-security-token")
    headers = {k: v for k, v in headers.items() if k.lower() not in drop}
    headers |= {"Host": host, "X-Amz-Date": when, "X-Amz-Content-Sha256": hashlib.sha256(body).hexdigest()}
    canon = {k.lower(): " ".join(str(v).split()) for k, v in headers.items() if k.lower() == "host" or k.lower().startswith("x-amz-")}
    names = ";".join(sorted(canon))
    request = "\n".join([method, path, query, *(f"{k}:{canon[k]}" for k in sorted(canon)), "", names, headers["X-Amz-Content-Sha256"]])
    scope = f"{when[:8]}/{region}/s3/aws4_request"
    k = f"AWS4{secret}".encode()
    for part in (when[:8], region, "s3", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    to_sign = f"AWS4-HMAC-SHA256\n{when}\n{scope}\n{hashlib.sha256(request.encode()).hexdigest()}"
    sig = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = f"AWS4-HMAC-SHA256 Credential={key}/{scope}, SignedHeaders={names}, Signature={sig}"
    return path + (f"?{query}" if query else ""), headers


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--upstream", required=True, help="the real S3 endpoint, e.g. http://127.0.0.1:9000")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--faults", default="{}", help='JSON, e.g. {"error": 0.1}')
    ap.add_argument("--sign", action="store_true", help="sign requests again with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION (a real bucket)")
    a = ap.parse_args()
    keys = (os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], os.environ.get("AWS_REGION", "auto")) if a.sign else None
    f = Faulty(a.upstream, a.port, keys)
    f.set(**json.loads(a.faults))
    print(f"faulty S3 on {f.url} -> {a.upstream}", flush=True)
    threading.Event().wait()
