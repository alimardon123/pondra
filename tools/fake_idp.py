#!/usr/bin/env python3
"""A stand-in OpenID Connect identity provider (like Okta or Keycloak), for the single sign-on tests.

It signs real tokens with real keys, so a client's checks run against what a provider sends: discovery,
the JWKS, the code flow with PKCE, an id token and an access token. /authorize signs the test user in at
once; there is no login page. Test-only routes (not the standard's): /_test/stats counts fetches and token
requests, /_test/mint makes a token of any claims, algorithm and key (forgeries too), /_test/rotate adds a
signing key, /_test/user says who signs in next. It needs `cryptography`, which comes with `moto` in
tools/requirements.txt, and otherwise the standard library.

  python3 tools/fake_idp.py --port 9100                           # serves until killed
  python3 tools/fake_idp.py --port 9100 --issuer http://idp.test  # discovery names another issuer
  python3 tools/fake_idp.py --selftest                            # checks every route, then exits

In-process: start(port=0, issuer=None, client_secret=None, access_audience="api://pondra") runs it in a
daemon thread and returns the server (its port is server.server_address[1], its issuer server.issuer);
stop(server) ends it.
"""
import argparse, base64, hashlib, hmac, json, secrets, sys, threading, time, urllib.error, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa, utils

ALGS = {"RS256": "rsa", "RS384": "rsa", "PS256": "rsa", "ES256": "ec", "HS256": "hmac", "none": "none"}
KEY_TYPES = {"rsa": rsa.RSAPrivateKey, "ec": ec.EllipticCurvePrivateKey}
HASHES = {"256": hashes.SHA256, "384": hashes.SHA384}
PUBLIC_PEM = (serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
TEST_USER = {"sub": "u1", "email": "ana@acme.com", "email_verified": True}


def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64d(text):
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def big(n, size=None):  # an integer as big-endian bytes: the minimum, or `size` bytes
    return n.to_bytes(size or (n.bit_length() + 7) // 8, "big")


def part(obj):  # a token's JSON part: compact, base64url, no padding
    return b64(json.dumps(obj, separators=(",", ":")).encode())


def new_key(kind):
    if kind == "rsa":
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return ec.generate_private_key(ec.SECP256R1())


def jwk(kid, key):
    nums = key.public_key().public_numbers()
    if isinstance(key, rsa.RSAPrivateKey):
        return {"kty": "RSA", "kid": kid, "alg": "RS256", "use": "sig", "n": b64(big(nums.n)), "e": b64(big(nums.e))}
    return {"kty": "EC", "kid": kid, "crv": "P-256", "alg": "ES256", "use": "sig",
            "x": b64(big(nums.x, 32)), "y": b64(big(nums.y, 32))}


def sign(alg, key, data):
    """The signature JWS wants: RSA's PKCS#1 v1.5 or PSS, r||s for ES256, HMAC for HS256, none for none."""
    if alg in ("RS256", "RS384"):
        return key.sign(data, padding.PKCS1v15(), HASHES[alg[2:]]())
    if alg == "PS256":
        return key.sign(data, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32), hashes.SHA256())
    if alg == "ES256":  # cryptography signs DER; a JWS carries the two integers, 32 bytes each
        r, s = utils.decode_dss_signature(key.sign(data, ec.ECDSA(hashes.SHA256())))
        return big(r, 32) + big(s, 32)
    if alg == "HS256":  # the key is the RSA public key's PEM: the classic algorithm-confusion forgery
        return hmac.new(key, data, hashlib.sha256).digest()
    return b""


class Idp:
    """The provider's state: its keys (the JWKS), the codes it gave, who signs in, and the counts."""

    def __init__(self, issuer, client_secret, access_audience):
        self.issuer, self.client_secret, self.access_audience = issuer, client_secret, access_audience
        self.keys, self.rsa_count = {}, 0  # kid -> private key, in the order the JWKS lists them
        self.add_rsa()
        self.keys["ec1"] = new_key("ec")
        self.codes, self.user = {}, dict(TEST_USER)
        self.stats = {"jwks_fetches": 0, "discovery_fetches": 0, "token_requests": 0}
        self.lock = threading.Lock()  # one request at a time, so the counts and codes stay right

    def add_rsa(self):
        self.rsa_count += 1
        self.rsa_newest = f"rsa{self.rsa_count}"
        self.keys[self.rsa_newest] = new_key("rsa")

    def signing_key(self, kind, kid, sign_with):
        if kind == "none":
            return None
        if kind == "hmac":
            return self.keys[self.rsa_newest].public_key().public_bytes(*PUBLIC_PEM)
        if sign_with == "stranger":  # a key the JWKS doesn't publish
            return new_key(kind)
        if sign_with != "jwks":
            raise ValueError(f"sign_with is jwks or stranger, not {sign_with!r}")
        if kid is None:  # no kid named: the newest key of the kind signs
            kid = self.rsa_newest if kind == "rsa" else "ec1"
        key = self.keys.get(kid)
        if not isinstance(key, KEY_TYPES[kind]):
            raise ValueError(f"the JWKS has no {kind} key with kid {kid!r}")
        return key

    def mint(self, body):
        alg = body.get("alg", "RS256")
        if alg not in ALGS:
            raise ValueError(f"unknown alg {alg!r}")
        kind = ALGS[alg]
        kid = body["kid"] if "kid" in body else (self.rsa_newest if kind == "rsa" else "ec1" if kind == "ec" else None)
        header = {"alg": alg, "typ": "JWT", **({"kid": kid} if kid is not None else {}), **(body.get("header") or {})}
        now = int(time.time())  # "kid": null leaves the kid out, and the newest key still signs
        claims = {"iss": self.issuer, "iat": now, "exp": now + int(body.get("ttl", 300)), **(body.get("claims") or {})}
        signing_input = f"{part(header)}.{part({k: v for k, v in claims.items() if v is not None})}".encode()
        key = self.signing_key(kind, kid, body.get("sign_with", "jwks"))
        return f"{signing_input.decode()}.{b64(sign(alg, key, signing_input))}"

    def authorize(self, q):
        if q.get("response_type") != "code":
            return 400, {"error": "unsupported_response_type"}
        if not (q.get("code_challenge") and q.get("code_challenge_method") == "S256" and q.get("redirect_uri")):
            return 400, {"error": "invalid_request", "error_description": "PKCE (S256) and a redirect_uri are required"}
        code = secrets.token_urlsafe(24)
        self.codes[code] = {"client_id": q.get("client_id"), "redirect_uri": q["redirect_uri"], "nonce": q.get("nonce"),
                            "challenge": q["code_challenge"], "user": dict(self.user), "expires": time.time() + 60,
                            "spent": False}
        params = {"code": code, **({"state": q["state"]} if "state" in q else {})}
        return 302, {"location": q["redirect_uri"] + ("&" if "?" in q["redirect_uri"] else "?") + urlencode(params)}

    def token(self, form, auth):
        self.stats["token_requests"] += 1
        user, secret = "", form.get("client_secret")
        if auth.startswith("Basic "):  # client_id:client_secret, as RFC 6749 sends it
            user, _, secret = base64.b64decode(auth[6:]).decode().partition(":")
        if self.client_secret is not None and secret != self.client_secret:
            return 400, {"error": "invalid_client"}
        entry = self.codes.get(form.get("code"))
        if form.get("grant_type") != "authorization_code" or entry is None or entry["spent"] or time.time() > entry["expires"]:
            return 400, {"error": "invalid_grant"}
        entry["spent"] = True  # the first request that finds a code spends it, right verifier or not
        client_id = form.get("client_id") or user
        verifier = b64(hashlib.sha256(form.get("code_verifier", "").encode()).digest())
        if (verifier, client_id, form.get("redirect_uri")) != (entry["challenge"], entry["client_id"], entry["redirect_uri"]):
            return 400, {"error": "invalid_grant"}
        now = int(time.time())
        id_claims = {**entry["user"], "iss": self.issuer, "aud": client_id, "iat": now, "exp": now + 300}
        if entry["nonce"]:
            id_claims["nonce"] = entry["nonce"]
        access = {**entry["user"], "iss": self.issuer, "aud": self.access_audience, "iat": now, "exp": now + 300}
        return 200, {"id_token": self.mint({"claims": id_claims}), "access_token": self.mint({"claims": access}),
                     "token_type": "Bearer", "expires_in": 300}

    def rotate(self, body):
        self.add_rsa()
        if body.get("drop_old"):  # every RSA key but the newest goes; ec1 stays
            self.keys = {k: v for k, v in self.keys.items() if k == self.rsa_newest or not isinstance(v, rsa.RSAPrivateKey)}
        return {"kids": list(self.keys)}

    def handle(self, method, path, query, raw, auth):
        """Answer one request as (status, body); a 302 carries its redirect as body["location"]."""
        route, i = (method, path), self.issuer
        if route == ("GET", "/.well-known/openid-configuration"):
            self.stats["discovery_fetches"] += 1
            return 200, {"issuer": i, "jwks_uri": i + "/jwks", "authorization_endpoint": i + "/authorize",
                         "token_endpoint": i + "/token", "response_types_supported": ["code"],
                         "id_token_signing_alg_values_supported": ["RS256", "ES256"],
                         "code_challenge_methods_supported": ["S256"]}
        if route == ("GET", "/jwks"):
            self.stats["jwks_fetches"] += 1
            return 200, {"keys": [jwk(kid, key) for kid, key in self.keys.items()]}
        if route == ("GET", "/_test/stats"):
            return 200, dict(self.stats)
        if route == ("GET", "/authorize"):
            return self.authorize({k: v[0] for k, v in query.items()})
        if route == ("POST", "/token"):
            return self.token({k: v[0] for k, v in parse_qs(raw.decode()).items()}, auth)
        if route == ("POST", "/_test/mint"):
            return 200, {"token": self.mint(json.loads(raw or b"{}"))}
        if route == ("POST", "/_test/rotate"):
            return 200, self.rotate(json.loads(raw or b"{}"))
        if route == ("POST", "/_test/user"):
            self.user = {k: v for k, v in json.loads(raw or b"{}").items() if v is not None}
            return 200, {"user": self.user}
        return 404, {"error": "not_found", "path": path}


class Handler(BaseHTTPRequestHandler):
    def _answer(self):
        url, idp = urlsplit(self.path), self.server.idp
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        try:
            with idp.lock:
                status, body = idp.handle(self.command, url.path, parse_qs(url.query), raw,
                                          self.headers.get("Authorization", ""))
        except (ValueError, TypeError, KeyError, AttributeError) as e:  # a body no route takes: a 400
            status, body = 400, {"error": "invalid_request", "error_description": repr(e)}
        data = json.dumps(body).encode()
        self.send_response(status)
        if status == 302:
            self.send_header("Location", body["location"])
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = _answer

    def log_message(self, *args):
        pass  # the console stays quiet; /_test/stats has the counts


def start(port=0, issuer=None, client_secret=None, access_audience="api://pondra"):
    """Serve in a daemon thread and return the server (port 0 picks a free one)."""
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.issuer = issuer or f"http://127.0.0.1:{server.server_address[1]}"
    server.idp = Idp(server.issuer, client_secret, access_audience)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def stop(server):
    server.shutdown()
    server.server_close()


def header(token):
    return json.loads(b64d(token.split(".")[0]))


def verify(token, jwk_dict):
    """The claims, if the signature checks out with the JWKS's key; raises InvalidSignature if not."""
    head, body, sig = token.split(".")
    data, raw, alg = f"{head}.{body}".encode(), b64d(sig), header(token)["alg"]
    if alg == "ES256":
        x, y = (int.from_bytes(b64d(jwk_dict[c]), "big") for c in ("x", "y"))
        der = utils.encode_dss_signature(int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:], "big"))
        ec.EllipticCurvePublicNumbers(x, y, ec.SECP256R1()).public_key().verify(der, data, ec.ECDSA(hashes.SHA256()))
    else:
        pub = rsa.RSAPublicNumbers(int.from_bytes(b64d(jwk_dict["e"]), "big"),
                                   int.from_bytes(b64d(jwk_dict["n"]), "big")).public_key()
        if alg == "PS256":
            pub.verify(raw, data, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32), hashes.SHA256())
        else:
            pub.verify(raw, data, padding.PKCS1v15(), HASHES[alg[2:]]())
    return json.loads(b64d(body))


def check(label, ok):
    print(("ok    " if ok else "FAIL  ") + label, flush=True)
    if not ok:
        raise SystemExit(1)  # the FAIL line says which check it was


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None  # a 302 comes back as an HTTPError: its Location is read, not followed


def selftest():
    server = start(0)
    base, issuer = f"http://127.0.0.1:{server.server_address[1]}", server.issuer
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect)  # this machine only
    redirect, client = "http://127.0.0.1:9999/callback", "pondra-cli"

    def call(method, path, body=None, form=None, headers=None, at=base):
        """(status, headers, JSON body) of one request; 4xx and 302 answers come back as they are."""
        data, hdrs = None, dict(headers or {})
        if form is not None:
            data, hdrs["Content-Type"] = urlencode(form).encode(), "application/x-www-form-urlencoded"
        elif body is not None:
            data, hdrs["Content-Type"] = json.dumps(body).encode(), "application/json"
        try:
            with opener.open(urllib.request.Request(at + path, data=data, method=method, headers=hdrs)) as r:
                return r.status, r.headers, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, e.headers, json.loads(e.read())

    def jwks():
        return {k["kid"]: k for k in call("GET", "/jwks")[2]["keys"]}

    def mint(body):
        return call("POST", "/_test/mint", body)[2]["token"]

    def pkce():
        verifier = b64(secrets.token_bytes(32))
        return verifier, b64(hashlib.sha256(verifier.encode()).digest())

    def params(challenge, **changes):  # what a client sends to /authorize
        return {"response_type": "code", "client_id": client, "redirect_uri": redirect, "state": "st1",
                "nonce": "n1", "code_challenge": challenge, "code_challenge_method": "S256", **changes}

    def sign_in(p, at=base):  # /authorize, not followed: (where it redirects, its query's values)
        loc = urlsplit(call("GET", "/authorize?" + urlencode(p), at=at)[1]["Location"])
        return loc.geturl().split("?")[0], {k: v[0] for k, v in parse_qs(loc.query).items()}

    def redeem(code, verifier, at=base, headers=None, **extra):
        form = {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect, "client_id": client,
                "code_verifier": verifier, **extra}
        return call("POST", "/token", form=form, headers=headers, at=at)

    disc = call("GET", "/.well-known/openid-configuration")[2]
    check("discovery names its issuer and endpoints", disc == {
        "issuer": issuer, "jwks_uri": issuer + "/jwks", "authorization_endpoint": issuer + "/authorize",
        "token_endpoint": issuer + "/token", "response_types_supported": ["code"],
        "id_token_signing_alg_values_supported": ["RS256", "ES256"], "code_challenge_methods_supported": ["S256"]})
    check("an unknown route answers 404", call("GET", "/nope")[0] == 404)
    keys = jwks()
    check("jwks: rsa1 is RSA 2048 (RS256); ec1 is P-256 (ES256) with 32-byte x and y",
          set(keys) == {"rsa1", "ec1"} and int.from_bytes(b64d(keys["rsa1"]["n"]), "big").bit_length() == 2048
          and keys["ec1"]["crv"] == "P-256" and len(b64d(keys["ec1"]["x"])) == len(b64d(keys["ec1"]["y"])) == 32)

    for alg, kid in (("RS256", "rsa1"), ("ES256", "ec1"), ("PS256", "rsa1"), ("RS384", "rsa1")):
        tok = mint({"alg": alg, "claims": {"sub": "x"}})
        check(f"mint {alg}: kid {kid}, and the signature verifies with the JWKS's key",
              header(tok) == {"alg": alg, "typ": "JWT", "kid": kid} and verify(tok, keys[kid])["sub"] == "x")
    tok = mint({"alg": "HS256"})
    head, body, sig = tok.split(".")
    pem = rsa.RSAPublicNumbers(int.from_bytes(b64d(keys["rsa1"]["e"]), "big"),
                               int.from_bytes(b64d(keys["rsa1"]["n"]), "big")).public_key().public_bytes(*PUBLIC_PEM)
    check("HS256: no kid, signed with the RSA public key's PEM as the secret",
          "kid" not in header(tok)
          and hmac.compare_digest(b64d(sig), hmac.new(pem, f"{head}.{body}".encode(), hashlib.sha256).digest()))
    tok = mint({"alg": "none"})
    check("alg none: header.payload. with an empty signature",
          tok.endswith(".") and tok.count(".") == 2 and header(tok)["alg"] == "none")
    tok = mint({"kid": None})
    check("kid null: no kid in the header, and the newest key still signs",
          "kid" not in header(tok) and verify(tok, keys["rsa1"])["iss"] == issuer)
    try:
        verify(mint({"sign_with": "stranger"}), keys["rsa1"])
        forged = False
    except InvalidSignature:
        forged = True
    check("a stranger's token claiming kid rsa1 doesn't verify with the JWKS key", forged)

    kids = call("POST", "/_test/rotate", {})[2]["kids"]
    keys = jwks()
    tok = mint({})
    check("rotate adds rsa2; RS256 with no kid is then signed by rsa2",
          set(kids) == {"rsa1", "ec1", "rsa2"} and header(tok)["kid"] == "rsa2" and verify(tok, keys["rsa2"])["iss"] == issuer)
    kids = call("POST", "/_test/rotate", {"drop_old": True})[2]["kids"]
    keys = jwks()
    check("rotate with drop_old keeps only the newest RSA key and ec1", set(kids) == set(keys) == {"ec1", "rsa3"})

    call("POST", "/_test/user", {"sub": "u1", "email": "ana@acme.com", "email_verified": True, "groups": ["analysts"]})
    verifier, challenge = pkce()
    target, got = sign_in(params(challenge))
    check("/authorize redirects to redirect_uri with a code and the state",
          target == redirect and got["state"] == "st1" and "code" in got)
    status, _, tok = redeem(got["code"], verifier)
    check("token: 200 for the right verifier, a Bearer token", status == 200 and tok["token_type"] == "Bearer")
    id_claims = verify(tok["id_token"], keys[header(tok["id_token"])["kid"]])
    access = verify(tok["access_token"], keys[header(tok["access_token"])["kid"]])
    check("id_token: nonce, aud = client_id, the user's claims; access_token: aud = the API, no nonce",
          id_claims["nonce"] == "n1" and id_claims["aud"] == client and id_claims["iss"] == issuer
          and id_claims["sub"] == "u1" and id_claims["groups"] == ["analysts"]
          and access["aud"] == "api://pondra" and "nonce" not in access)
    status, _, err = redeem(got["code"], verifier)
    check("a code used twice: 400 invalid_grant", status == 400 and err == {"error": "invalid_grant"})
    verifier, challenge = pkce()
    code = sign_in(params(challenge))[1]["code"]
    status, _, err = redeem(code, "not-the-verifier")
    check("a wrong code_verifier: 400 invalid_grant", status == 400 and err == {"error": "invalid_grant"})
    refused = [call("GET", "/authorize?" + urlencode(p))[0] for p in (
        {k: v for k, v in params(challenge).items() if k != "code_challenge"},
        params(challenge, code_challenge_method="plain"), params(challenge, response_type="token"))]
    check("/authorize refuses no PKCE, method plain and response_type token: 400", refused == [400, 400, 400])
    stats = call("GET", "/_test/stats")[2]
    check("stats: 3 token requests, 1 discovery fetch, the JWKS fetches counted",
          stats["token_requests"] == 3 and stats["discovery_fetches"] == 1 and stats["jwks_fetches"] >= 3)

    secured = start(0, client_secret="s3cret")
    at = f"http://127.0.0.1:{secured.server_address[1]}"
    verifier, challenge = pkce()
    code = sign_in(params(challenge), at=at)[1]["code"]
    wrong, missing = redeem(code, verifier, at=at, client_secret="wrong"), redeem(code, verifier, at=at)
    check("with a client secret set, a wrong or a missing one: 400 invalid_client",
          wrong[0] == missing[0] == 400 and wrong[2] == missing[2] == {"error": "invalid_client"})
    basic = {"Authorization": "Basic " + base64.b64encode(f"{client}:s3cret".encode()).decode()}
    by_basic = redeem(code, verifier, at=at, headers=basic)
    verifier, challenge = pkce()
    code = sign_in(params(challenge), at=at)[1]["code"]
    by_form = redeem(code, verifier, at=at, client_secret="s3cret")
    check("the right secret gets tokens, in the form or as HTTP Basic", by_basic[0] == by_form[0] == 200)
    stop(secured)
    stop(server)
    print("ok")
    return 0


def main():
    parser = argparse.ArgumentParser(description="A stand-in OpenID Connect provider (see the module docstring).")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--issuer", help="the issuer discovery and the tokens name (default http://127.0.0.1:PORT)")
    parser.add_argument("--client-secret", help="the secret /token asks for (default: none)")
    parser.add_argument("--access-audience", default="api://pondra", help="the aud of access tokens")
    parser.add_argument("--selftest", action="store_true", help="check every route, then exit")
    args = parser.parse_args()
    if args.selftest:
        try:
            return selftest()
        except Exception as e:  # a route answered wrongly, or not at all
            print(f"FAIL {e!r}", flush=True)
            return 1
    server = start(args.port, args.issuer, args.client_secret, args.access_audience)
    print(f"fake OpenID Connect provider: issuer {server.issuer}", flush=True)
    try:
        while True:  # the server runs in its own thread; Ctrl-C stops it
            time.sleep(3600)
    except KeyboardInterrupt:
        stop(server)
    return 0


if __name__ == "__main__":
    sys.exit(main())
