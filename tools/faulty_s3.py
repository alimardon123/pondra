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
"""
import argparse, http.client, http.server, json, random, socketserver, threading, time, urllib.parse

OFF = {"error": 0.0, "lost": 0.0, "hang": 0.0, "hang_secs": 30.0, "slow_ms": 0, "down": False, "methods": []}
ERRORS = [("500 Internal Server Error", "InternalError", "We encountered an internal error. Please try again."),
          ("503 Slow Down", "SlowDown", "Please reduce your request rate.")]


class Faulty:
    """A proxy on 127.0.0.1:`port` (0: any free one) to `upstream`, its faults changed by `set`."""

    def __init__(self, upstream, port=0):
        u = urllib.parse.urlsplit(upstream)
        self.up = (u.hostname, u.port or 80)
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
                c = http.client.HTTPConnection(*proxy.up, timeout=300)
                try:
                    c.request(self.command, self.path, body, {k: v for k, v in self.headers.items() if k.lower() != "connection"})
                    r = c.getresponse()
                    data = r.read()
                finally:
                    c.close()
                if fault == "lost":
                    return self.drop()
                headers = [(k, v) for k, v in r.getheaders() if k.lower() not in ("transfer-encoding", "connection", "content-length")]
                self.answer(r.status, r.reason, headers, data, r.getheader("Content-Length") if self.command == "HEAD" else None)

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


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--upstream", required=True, help="the real S3 endpoint, e.g. http://127.0.0.1:9000")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--faults", default="{}", help='JSON, e.g. {"error": 0.1}')
    a = ap.parse_args()
    f = Faulty(a.upstream, a.port)
    f.set(**json.loads(a.faults))
    print(f"faulty S3 on {f.url} -> {a.upstream}", flush=True)
    threading.Event().wait()
