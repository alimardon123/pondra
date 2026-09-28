#!/usr/bin/env python3
"""An Iceberg REST catalog for tests (ADR-026): PyIceberg's SQL catalog behind the REST
protocol's reading endpoints, with OAuth's client credentials. Its tables are what PyIceberg (or
anything given its sqlite file and warehouse) wrote.

  sim_iceberg_rest.py --port 8181 --warehouse /tmp/wh [--client pondra:s3cret]
"""
import argparse, json, secrets, sys, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pyiceberg.catalog.sql import SqlCatalog

TOKENS = set()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8181)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--client", default="pondra:s3cret")
    ap.add_argument("--name", default="local", help="the SQL catalog's name (PyIceberg keys its tables by it)")
    a = ap.parse_args()
    catalog = SqlCatalog(a.name, uri=f"sqlite:///{a.warehouse}/catalog.db", warehouse=f"file://{a.warehouse}")
    client_id, client_secret = a.client.split(":", 1)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def error(self, status, message):
            self.reply(status, {"error": {"message": message, "type": "Error", "code": status}})

        def do_POST(self):
            parts = [urllib.parse.unquote(p) for p in urllib.parse.urlsplit(self.path).path.strip("/").split("/")]
            if len(parts) == 6 and parts[:3] == ["v1", "wh", "namespaces"] and parts[4] == "tables":  # (a commit)
                if self.headers.get("authorization", "").removeprefix("Bearer ") not in TOKENS:
                    return self.error(401, "not authorized")
                from pyiceberg.exceptions import CommitFailedException
                from pyiceberg.table import CommitTableRequest
                body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))))
                try:
                    t = catalog.load_table((*parts[3].split("\x1f"), parts[5]))
                    req = CommitTableRequest.model_validate({"identifier": {"namespace": parts[3].split("\x1f"), "name": parts[5]}, **body})
                    r = catalog.commit_table(t, req.requirements, req.updates)
                    return self.reply(200, {"metadata-location": r.metadata_location, "metadata": json.loads(r.metadata.model_dump_json(by_alias=True, exclude_none=True))})
                except CommitFailedException as e:
                    return self.error(409, str(e))
                except Exception as e:  # noqa: BLE001
                    return self.error(400, f"{type(e).__name__}: {e}")
            if self.path.rstrip("/") == "/v1/oauth/tokens":
                form = dict(urllib.parse.parse_qsl(self.rfile.read(int(self.headers.get("content-length", 0))).decode()))
                if form.get("client_id") != client_id or form.get("client_secret") != client_secret:
                    return self.reply(401, {"error": "invalid_client", "error_description": "wrong client id or secret"})
                t = secrets.token_hex(16)
                TOKENS.add(t)
                return self.reply(200, {"access_token": t, "token_type": "bearer", "expires_in": 3600, "issued_token_type": "urn:ietf:params:oauth:token-type:access_token"})
            self.error(404, "not here")

        def do_GET(self):
            if self.headers.get("authorization", "").removeprefix("Bearer ") not in TOKENS:
                return self.error(401, "not authorized")
            u = urllib.parse.urlsplit(self.path)
            parts = [urllib.parse.unquote(p) for p in u.path.strip("/").split("/")]
            if parts == ["v1", "config"]:
                return self.reply(200, {"defaults": {}, "overrides": {"prefix": "wh"}})
            if parts[:2] != ["v1", "wh"]:
                return self.error(404, "no such path")
            rest = parts[2:]
            try:
                if rest == ["namespaces"]:
                    return self.reply(200, {"namespaces": [list(n) for n in catalog.list_namespaces()]})
                if len(rest) == 3 and rest[0] == "namespaces" and rest[2] == "tables":
                    return self.reply(200, {"identifiers": [{"namespace": list(t[:-1]), "name": t[-1]} for t in catalog.list_tables(rest[1])]})
                if len(rest) == 4 and rest[0] == "namespaces" and rest[2] == "tables":
                    t = catalog.load_table((*rest[1].split("\x1f"), rest[3]))
                    return self.reply(200, {"metadata-location": t.metadata_location, "metadata": json.loads(t.metadata.model_dump_json(by_alias=True, exclude_none=True)), "config": {}})
            except Exception as e:  # noqa: BLE001 (the catalog's own answer)
                return self.error(404, f"{type(e).__name__}: {e}")
            self.error(404, "no such path")

    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()


if __name__ == "__main__":
    sys.exit(main())
