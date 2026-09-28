#!/usr/bin/env python3
"""A Google Cloud Storage bucket in memory, speaking the XML API that `object_store` (Pondra's
GCS client) uses: objects put (whole, multipart, copied) with `x-goog-if-generation-match`,
read by range, listed (`list-type=2`, prefixes, pages) and deleted — enough for a lake on
`gs://` and for files read and written there (ADR-026). fake-gcs-server answers the JSON API but
refuses the XML API's plain PUT ("invalid uploadType"), which is what `object_store` sends.

  sim_gcs.py --port 9023 [--bucket lakes …]

Point a node at it with a key that turns OAuth off (as object_store's own tests do):
  GOOGLE_SERVICE_ACCOUNT_KEY='{"gcs_base_url": "http://127.0.0.1:9023", "disable_oauth": true,
  "client_email": "", "private_key": "", "private_key_id": ""}' GOOGLE_ALLOW_HTTP=true
"""
import argparse, email.utils, hashlib, http.server, itertools, re, threading, time, urllib.parse, uuid
from xml.sax.saxutils import escape

BUCKETS, UPLOADS, LOCK, GEN = {}, {}, threading.Lock(), itertools.count(int(time.time() * 1e6))


class Obj:
    def __init__(self, data):
        self.data, self.gen, self.at = data, next(GEN), time.time()
        self.etag = '"' + hashlib.md5(data).hexdigest() + '"'


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def reply(self, status, body=b"", headers=None):
        self.send_response(status)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def xml(self, status, body):
        self.reply(status, ('<?xml version="1.0" encoding="UTF-8"?>' + body).encode(), {"Content-Type": "application/xml"})

    def where(self):
        u = urllib.parse.urlsplit(self.path)
        bucket, _, key = u.path.lstrip("/").partition("/")
        return urllib.parse.unquote(bucket), urllib.parse.unquote(key), dict(urllib.parse.parse_qsl(u.query, keep_blank_values=True))

    def body(self):
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def objects(self, bucket):
        if bucket not in BUCKETS:
            self.xml(404, "<Error><Code>NoSuchBucket</Code></Error>")
            return None
        return BUCKETS[bucket]

    def meta(self, o):
        return {"ETag": o.etag, "x-goog-generation": str(o.gen), "Last-Modified": email.utils.formatdate(o.at, usegmt=True)}

    def do_GET(self):
        bucket, key, q = self.where()
        objs = self.objects(bucket)
        if objs is None:
            return
        if not key:
            return self.list(objs, q)
        with LOCK:
            o = objs.get(key)
        if o is None or ("generation" in q and q["generation"] != str(o.gen)):
            return self.xml(404, "<Error><Code>NoSuchKey</Code></Error>")
        if self.headers.get("If-Match") not in (None, o.etag, "*"):
            return self.reply(412)
        if self.headers.get("If-None-Match") in (o.etag, "*"):
            return self.reply(304, headers=self.meta(o))
        rng, data = self.headers.get("Range"), o.data
        if not rng:
            return self.reply(200, data, self.meta(o))
        lo, hi = rng.split("=", 1)[1].split("-")
        lo, hi = (max(len(data) - int(hi), 0), len(data) - 1) if lo == "" else (int(lo), min(int(hi), len(data) - 1) if hi else len(data) - 1)
        if lo >= len(data):
            return self.reply(416)
        return self.reply(206, data[lo:hi + 1], {**self.meta(o), "Content-Range": f"bytes {lo}-{hi}/{len(data)}"})

    do_HEAD = do_GET

    def list(self, objs, q):
        prefix, delim, after = q.get("prefix", ""), q.get("delimiter"), q.get("continuation-token") or q.get("start-after") or ""
        most = int(q.get("max-keys", 1000))
        with LOCK:
            keys = sorted(k for k in objs if k.startswith(prefix) and k > after)
        contents, prefixes, last, truncated = [], [], None, False
        for k in keys:
            if len(contents) + len(prefixes) >= most:
                truncated = True
                break
            rest = k[len(prefix):]
            if delim and delim in rest:
                p = prefix + rest.split(delim)[0] + delim
                if p not in prefixes:
                    prefixes.append(p)
                last = k
                continue
            o = objs[k]
            at = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(o.at))
            contents.append(f"<Contents><Key>{escape(k)}</Key><Size>{len(o.data)}</Size><LastModified>{at}</LastModified><ETag>{escape(o.etag)}</ETag></Contents>")
            last = k
        more = f"<IsTruncated>true</IsTruncated><NextContinuationToken>{escape(last)}</NextContinuationToken>" if truncated else "<IsTruncated>false</IsTruncated>"
        self.xml(200, f"<ListBucketResult><Prefix>{escape(prefix)}</Prefix>{more}{''.join(contents)}"
                      + "".join(f"<CommonPrefixes><Prefix>{escape(p)}</Prefix></CommonPrefixes>" for p in prefixes) + "</ListBucketResult>")

    def do_PUT(self):
        bucket, key, q = self.where()
        objs = self.objects(bucket)
        if objs is None:
            return
        data = self.body()
        if "uploadId" in q:  # (a part of a multipart upload)
            etag = '"' + hashlib.md5(data).hexdigest() + '"'
            with LOCK:
                UPLOADS[q["uploadId"]][1][int(q["partNumber"])] = data
            return self.reply(200, headers={"ETag": etag})
        source = self.headers.get("x-goog-copy-source")
        if source:
            sb, _, sk = urllib.parse.unquote(source).lstrip("/").partition("/")
            with LOCK:
                src = BUCKETS.get(sb, {}).get(sk)
            if src is None:
                return self.xml(404, "<Error><Code>NoSuchKey</Code></Error>")
            data = src.data
        return self.store(objs, key, data)

    def store(self, objs, key, data):
        match = self.headers.get("x-goog-if-generation-match")
        with LOCK:
            now = objs.get(key)
            if match is not None and (now.gen if now else 0) != int(match):
                return self.xml(412, "<Error><Code>PreconditionFailed</Code></Error>")
            o = objs[key] = Obj(data)
        self.reply(200, headers=self.meta(o))

    def do_POST(self):
        bucket, key, q = self.where()
        objs = self.objects(bucket)
        if objs is None:
            return
        body = self.body()
        if "uploads" in q:
            uid = uuid.uuid4().hex
            with LOCK:
                UPLOADS[uid] = (key, {})
            return self.xml(200, f"<InitiateMultipartUploadResult><Bucket>{escape(bucket)}</Bucket><Key>{escape(key)}</Key><UploadId>{uid}</UploadId></InitiateMultipartUploadResult>")
        if "uploadId" in q:
            with LOCK:
                _, parts = UPLOADS.pop(q["uploadId"])
            wanted = [int(n) for n in re.findall(rb"<PartNumber>(\d+)</PartNumber>", body)]
            data = b"".join(parts[n] for n in wanted)
            match = self.headers.get("x-goog-if-generation-match")
            with LOCK:
                now = objs.get(key)
                if match is not None and (now.gen if now else 0) != int(match):
                    return self.xml(412, "<Error><Code>PreconditionFailed</Code></Error>")
                o = objs[key] = Obj(data)
            self.send_response(200)
            body = f'<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Bucket>{escape(bucket)}</Bucket><Key>{escape(key)}</Key><ETag>{escape(o.etag)}</ETag></CompleteMultipartUploadResult>'.encode()
            for k, v in {**self.meta(o), "Content-Type": "application/xml", "Content-Length": str(len(body))}.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)
            return
        self.reply(400)

    def do_DELETE(self):
        bucket, key, q = self.where()
        objs = self.objects(bucket)
        if objs is None:
            return
        with LOCK:
            if "uploadId" in q:
                UPLOADS.pop(q["uploadId"], None)
                return self.reply(204)
            gone = objs.pop(key, None)
        self.reply(204 if gone else 404)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9023)
    ap.add_argument("--bucket", action="append", default=[])
    a = ap.parse_args()
    for b in a.bucket or ["lakes"]:
        BUCKETS[b] = {}
    http.server.ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
