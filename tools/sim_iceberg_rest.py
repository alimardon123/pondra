#!/usr/bin/env python3
"""An Iceberg REST catalog for tests (ADR-026, ADR-028): PyIceberg's SQL catalog behind the REST
protocol. Its tables are what PyIceberg (or anything given its sqlite file and warehouse) wrote.

  sim_iceberg_rest.py --port 8181 --warehouse /tmp/wh [--client pondra:s3cret]

The AWS shapes and credential vending are options of their own:
  --warehouse s3://wh/ --db cat.db --s3-endpoint URL --s3-key K:S   the warehouse on S3 (moto, in tests)
  --sigv4 glue:us-east-1:K:S     every request must be signed with that service, region and key
  --prefix catalogs/123456789012 /v1/{prefix}/… (an encoded ARN works too); --warehouse-id names /v1/config's
  --vend K:S:T [--vend-endpoint URL]   loadTable with X-Iceberg-Access-Delegation: vended-credentials
                                       answers credentials for the table's location
GET /_stats (no sign-in) counts the requests and the vended-credentials asks, so a test can check them.
"""
import argparse, hmac, json, os, secrets, sys, threading, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import CommitTableRequest

TOKENS = set()
STATS = {"requests": 0, "delegation_headers": 0}
LOCK = threading.Lock()  # (requests are served on a thread each)
VENDED = "vended-credentials"


def sigv4_problem(headers, method, path, body, expect):
    """Why a request isn't signed for this service, region and key (None when it is). botocore signs
    over the headers a request names as signed, with the date it names, so the signature is rebuilt
    the same way and compared with the one sent: a changed header or body shows as a bad signature."""
    service, region, key, secret = expect
    auth = headers.get("authorization") or ""
    if not auth:
        return "missing Authorization header: this catalog takes AWS Signature Version 4 requests only"
    if not auth.startswith("AWS4-HMAC-SHA256 "):
        return "Authorization is not an AWS Signature Version 4 header"
    try:
        fields = dict(part.strip().split("=", 1) for part in auth[len("AWS4-HMAC-SHA256 "):].split(","))
        got_key, _date, got_region, got_service, _term = fields["Credential"].split("/")
        signed, sent = fields["SignedHeaders"].split(";"), fields["Signature"]
    except (KeyError, ValueError):
        return "malformed Authorization header: no Credential, SignedHeaders or Signature"
    if got_service != service:
        return f"signed for service {got_service}, this catalog is {service}"
    if got_region != region:
        return f"signed for region {got_region}, this catalog is {region}"
    if got_key != key:
        return f"signed with access key {got_key}, which this catalog does not have"
    if not headers.get("x-amz-date"):
        return "missing X-Amz-Date header"
    req = AWSRequest(method=method, url=f"http://{headers.get('host', '')}{path}", data=body,
                     headers={name: headers[name] for name in signed if headers.get(name) is not None})
    req.context["timestamp"] = headers["x-amz-date"]
    signer = SigV4Auth(Credentials(key, secret), service, region)
    expected = signer.signature(signer.string_to_sign(req, signer.canonical_request(req)), req)
    if not hmac.compare_digest(expected, sent):
        return "bad signature: not signed with this key's secret, or changed after it was signed"
    return None


def metadata_of(table):
    return json.loads(table.metadata.model_dump_json(by_alias=True, exclude_none=True))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8181)
    ap.add_argument("--warehouse", required=True, help="a folder, or s3://bucket/prefix (then --db and the S3 options)")
    ap.add_argument("--db", help="the SQL catalog's sqlite file (default: <warehouse>/catalog.db; required for s3)")
    ap.add_argument("--s3-endpoint", help="the S3 warehouse's endpoint (moto, in tests)")
    ap.add_argument("--s3-key", help="KEY:SECRET the S3 warehouse is read and written with")
    ap.add_argument("--client", default="pondra:s3cret", help="OAuth client credentials (not used with --sigv4)")
    ap.add_argument("--name", default="local", help="the SQL catalog's name (PyIceberg keys its tables by it)")
    ap.add_argument("--prefix", default="wh", help="the prefix /v1/config answers, and every route's /v1/{prefix}/")
    ap.add_argument("--warehouse-id", help="/v1/config must then name it: ?warehouse=ID (Glue's account id)")
    ap.add_argument("--sigv4", help="SERVICE:REGION:KEY_ID:SECRET: every request must be signed with these")
    ap.add_argument("--vend", help="KEY_ID:SECRET:TOKEN: loadTable answers these to a client asking for vended credentials")
    ap.add_argument("--vend-endpoint", help="the s3.endpoint vended credentials name (default: --s3-endpoint)")
    a = ap.parse_args()
    s3 = a.warehouse.startswith("s3://")
    if s3 and not a.db:
        ap.error("an s3 warehouse needs --db, its SQL catalog's sqlite file")
    props = {"uri": f"sqlite:///{a.db or os.path.join(a.warehouse, 'catalog.db')}",
             "warehouse": a.warehouse if s3 else f"file://{a.warehouse}"}
    if s3:  # (path-style: a bucket is in the path, as moto and R2 take it; PyIceberg's name for that is virtual addressing off)
        props.update({"s3.region": "us-east-1", "s3.force-virtual-addressing": "false"})
        if a.s3_endpoint:
            props["s3.endpoint"] = a.s3_endpoint
        if a.s3_key:
            props["s3.access-key-id"], props["s3.secret-access-key"] = a.s3_key.split(":", 1)
    catalog = SqlCatalog(a.name, **props)
    client_id, client_secret = a.client.split(":", 1)
    sigv4 = tuple(a.sigv4.split(":", 3)) if a.sigv4 else None
    vended = None
    if a.vend:
        key_id, secret, token = a.vend.split(":", 2)
        vended = {"s3.access-key-id": key_id, "s3.secret-access-key": secret, "s3.session-token": token,
                  "s3.region": "us-east-1", "s3.path-style-access": "true"}
        if a.vend_endpoint or a.s3_endpoint:
            vended["s3.endpoint"] = a.vend_endpoint or a.s3_endpoint

    def route(path):
        """The parts of a path after /v1/{prefix}/, unquoted, or None for another path. The prefix may
        hold a '/' (catalogs/123456789012) or be an encoded ARN, as PyIceberg sends what it was given."""
        for pre in {a.prefix, urllib.parse.quote(a.prefix, safe="")}:
            if path.startswith(f"/v1/{pre}/"):
                return [urllib.parse.unquote(p) for p in path[len(f"/v1/{pre}/"):].split("/")]
        return None

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

        def error(self, status, message, kind="Error"):
            self.reply(status, {"error": {"message": message, "type": kind, "code": status}})

        def do_GET(self):
            self.serve()

        do_POST = do_GET

        def serve(self):
            length = int(self.headers.get("content-length", 0))
            body = self.rfile.read(length) if length else b""  # (read first: the answer may come before it otherwise)
            u = urllib.parse.urlsplit(self.path)
            if u.path == "/_stats":
                with LOCK:
                    stats = dict(STATS)
                return self.reply(200, stats)
            with LOCK:
                STATS["requests"] += 1
                if self.headers.get("x-iceberg-access-delegation") == VENDED:
                    STATS["delegation_headers"] += 1
            if self.command == "POST" and u.path.rstrip("/") == "/v1/oauth/tokens":
                return self.tokens(body)
            if sigv4:  # (signed requests need no OAuth token)
                problem = sigv4_problem(self.headers, self.command, self.path, body, sigv4)
                if problem:
                    return self.error(403, problem, "ForbiddenException")
            elif self.headers.get("authorization", "").removeprefix("Bearer ") not in TOKENS:
                return self.error(401, "not authorized", "NotAuthorizedException")
            if self.command == "GET" and u.path == "/v1/config":
                if a.warehouse_id and urllib.parse.parse_qs(u.query).get("warehouse") != [a.warehouse_id]:
                    return self.error(400, f"/v1/config needs ?warehouse={a.warehouse_id}", "BadRequestException")
                return self.reply(200, {"defaults": {}, "overrides": {"prefix": a.prefix}})
            parts = route(u.path)
            if parts is None:
                return self.error(404, "no such path")
            self.catalog_call(parts, body)

        def tokens(self, body):
            form = dict(urllib.parse.parse_qsl(body.decode()))
            if form.get("client_id") != client_id or form.get("client_secret") != client_secret:
                return self.reply(401, {"error": "invalid_client", "error_description": "wrong client id or secret"})
            token = secrets.token_hex(16)
            TOKENS.add(token)
            self.reply(200, {"access_token": token, "token_type": "bearer", "expires_in": 3600,
                             "issued_token_type": "urn:ietf:params:oauth:token-type:access_token"})

        def catalog_call(self, parts, body):
            if self.command == "GET" and parts == ["namespaces"]:
                return self.reply(200, {"namespaces": [list(n) for n in catalog.list_namespaces()]})
            if len(parts) < 3 or parts[0] != "namespaces" or parts[2] != "tables":
                return self.error(404, "no such path")
            ns = tuple(parts[1].split("\x1f"))  # (a multi-level namespace is joined by the unit separator)
            if self.command == "GET" and len(parts) == 3:
                try:
                    ids = catalog.list_tables(ns)
                except Exception as e:  # noqa: BLE001 (the catalog's own answer)
                    return self.error(404, f"{type(e).__name__}: {e}", "NoSuchNamespaceException")
                return self.reply(200, {"identifiers": [{"namespace": list(t[:-1]), "name": t[-1]} for t in ids]})
            if len(parts) == 4 and self.command == "GET":
                return self.load(ns, parts[3])
            if len(parts) == 4 and self.command == "POST":
                return self.commit(ns, parts[3], body)
            self.error(404, "no such path")

        def load(self, ns, name):
            try:
                t = catalog.load_table((*ns, name))
            except Exception as e:  # noqa: BLE001 (the catalog's own answer)
                return self.error(404, f"{type(e).__name__}: {e}", "NoSuchTableException")
            answer = {"metadata-location": t.metadata_location, "metadata": metadata_of(t), "config": {}}
            if vended and self.headers.get("x-iceberg-access-delegation") == VENDED:
                # (the credentials for the table's location, as storage-credentials name them too)
                answer = {**answer, "config": vended, "storage-credentials": [{"prefix": t.metadata.location, "config": vended}]}
            self.reply(200, answer)

        def commit(self, ns, name, body):
            try:
                t = catalog.load_table((*ns, name))
                req = CommitTableRequest.model_validate({"identifier": {"namespace": list(ns), "name": name}, **json.loads(body)})
                r = catalog.commit_table(t, req.requirements, req.updates)
            except CommitFailedException as e:  # (a requirement failed: the writer reads again and retries)
                return self.error(409, str(e), "CommitFailedException")
            except Exception as e:  # noqa: BLE001
                return self.error(400, f"{type(e).__name__}: {e}", "BadRequestException")
            self.reply(200, {"metadata-location": r.metadata_location, "metadata": metadata_of(r)})

    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()


if __name__ == "__main__":
    sys.exit(main())
