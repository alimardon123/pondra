#!/usr/bin/env python3
"""A stand-in for GitHub's REST API, for the checks to run against.

It keeps repositories in memory and answers the git-data, refs and pulls routes a Git folder uses
(ADR-060; the table is in the shared spec). Blob ids are git's, so they match what Pondra computes.
Test-only routes, not GitHub's: /_test/push makes a commit "by someone else", /_test/state shows
what a repository holds, /_test/requests lists every request as it was sent.

  python3 tools/fake_github.py --port 9990 --token secret-token   # serves until killed
  python3 tools/fake_github.py --selftest                        # checks the routes, then exits

In-process: start(port, token) runs it in a daemon thread and returns the server (its port is
server.server_address[1]); stop(server) ends it.
"""
import argparse, base64, hashlib, itertools, json, re, threading, urllib.error, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

DEFAULT_REPO = "acme/sales"


class ApiError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status, self.message = status, message


def found(table, key):
    if key not in table:
        raise ApiError(404, "Not Found")
    return table[key]


def git_blob_id(data):
    """git's id for a blob: sha1 of a header with the length, then the bytes."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def tree_id(flat):  # trees and commits need only a stable id, not git's format
    return hashlib.sha1(json.dumps(flat, sort_keys=True).encode()).hexdigest()


def ref_json(branch, sha):
    return {"ref": f"refs/heads/{branch}", "object": {"sha": sha, "type": "commit"}}


def commit_json(sha, c):
    return {"sha": sha, "message": c["message"], "tree": {"sha": c["tree"]},
            "parents": [{"sha": p} for p in c["parents"]]}


def wrap64(data):
    """base64 with a newline every 60 characters, as GitHub sends it."""
    b64 = base64.b64encode(data).decode()
    return "".join(b64[i:i + 60] + "\n" for i in range(0, len(b64), 60))


class Store:
    """The repositories, the token and the request log. A repository is a dict: blobs (id -> bytes),
    trees (id -> {path: blob id}), commits (id -> {tree, parents, message}), refs (branch -> commit)
    and pulls (a list)."""

    def __init__(self, token, repo, port):
        self.token, self.seeded, self.port = token, repo, port
        self.lock = threading.Lock()        # one request at a time, so the store never tears
        self.counter = itertools.count(1)   # makes every commit id unique
        self.log = []                       # every request, as it was sent
        self.repos = {}
        self.seed(repo)

    def seed(self, name):
        """The repository, with branch main and one commit holding two files."""
        repo = self.repos[name] = {"default_branch": "main", "blobs": {}, "trees": {},
                                   "commits": {}, "refs": {}, "pulls": []}
        files = {"pondra.toml": '[project]\nname = "sales"\n',
                 "objects/sales/orders.sql": "CREATE TABLE sales.orders (id BIGINT, amount DOUBLE);\n"}
        tree = self.put_tree(repo, {p: self.put_blob(repo, t.encode()) for p, t in files.items()})
        repo["refs"]["main"] = self.put_commit(repo, tree, [], "Initial commit")

    def repo(self, name):
        return found(self.repos, name)

    def put_blob(self, repo, data):
        sha = git_blob_id(data)
        repo["blobs"][sha] = data
        return sha

    def put_tree(self, repo, flat):
        sha = tree_id(flat)
        repo["trees"][sha] = dict(flat)
        return sha

    def put_commit(self, repo, tree, parents, message):
        commit = {"tree": tree, "parents": list(parents), "message": message}
        # The counter makes two commits with the same tree, parents and message differ.
        sha = hashlib.sha1((json.dumps(commit, sort_keys=True) + str(next(self.counter))).encode()).hexdigest()
        repo["commits"][sha] = commit
        return sha

    def ancestors(self, repo, sha):
        """sha and every commit it reaches through its parents."""
        seen, todo = set(), [sha]
        while todo:
            c = todo.pop()
            if c not in seen:
                seen.add(c)
                todo.extend(repo["commits"][c]["parents"])
        return seen

    def listing(self, repo, flat):
        """GitHub's recursive tree: every blob and a tree entry per directory. Each directory's subtree
        is stored as well, so the id a listing gives for it can be read."""
        entries = [{"path": p, "mode": "100644", "type": "blob", "sha": s, "size": len(repo["blobs"][s])}
                   for p, s in flat.items()]
        for d in {"/".join(p.split("/")[:i]) for p in flat for i in range(1, p.count("/") + 1)}:
            sub = {p[len(d) + 1:]: s for p, s in flat.items() if p.startswith(d + "/")}
            entries.append({"path": d, "mode": "040000", "type": "tree", "sha": self.put_tree(repo, sub)})
        return sorted(entries, key=lambda e: e["path"])


# Each route answers (status, body). Its arguments: the store, the regex match, the JSON body, the query.

def get_repo(st, m, body, query):
    return 200, {"full_name": m.group(1), "default_branch": st.repo(m.group(1))["default_branch"]}


def get_ref(st, m, body, query):
    return 200, ref_json(m.group(2), found(st.repo(m.group(1))["refs"], m.group(2)))


def get_commit(st, m, body, query):
    sha = m.group(2)
    return 200, commit_json(sha, found(st.repo(m.group(1))["commits"], sha))


def get_tree(st, m, body, query):
    repo, sha = st.repo(m.group(1)), m.group(2)
    return 200, {"sha": sha, "tree": st.listing(repo, found(repo["trees"], sha)), "truncated": False}


def get_blob(st, m, body, query):
    sha = m.group(2)
    data = found(st.repo(m.group(1))["blobs"], sha)
    return 200, {"sha": sha, "size": len(data), "encoding": "base64", "content": wrap64(data)}


def post_blob(st, m, body, query):
    data = base64.b64decode(body["content"]) if body.get("encoding") == "base64" else body["content"].encode()
    return 201, {"sha": st.put_blob(st.repo(m.group(1)), data)}


def post_tree(st, m, body, query):
    repo, base = st.repo(m.group(1)), body.get("base_tree")
    if base and base not in repo["trees"]:
        raise ApiError(422, "base_tree does not exist")
    flat = dict(repo["trees"][base]) if base else {}
    for e in body["tree"]:  # a null sha deletes the path
        if e.get("sha") is None:
            flat.pop(e["path"], None)
        elif e["sha"] in repo["blobs"]:
            flat[e["path"]] = e["sha"]
        else:
            raise ApiError(422, "No blob with that sha")
    return 201, {"sha": st.put_tree(repo, flat)}


def post_commit(st, m, body, query):
    repo, parents = st.repo(m.group(1)), body.get("parents", [])
    if body["tree"] not in repo["trees"] or any(p not in repo["commits"] for p in parents):
        raise ApiError(422, "Tree or parent does not exist")
    sha = st.put_commit(repo, body["tree"], parents, body["message"])
    return 201, commit_json(sha, repo["commits"][sha])


def post_ref(st, m, body, query):
    repo, ref, sha = st.repo(m.group(1)), body["ref"], body["sha"]
    if not ref.startswith("refs/heads/"):
        raise ApiError(422, "Only branches can be made")
    branch = ref[len("refs/heads/"):]
    if branch in repo["refs"]:
        raise ApiError(422, "Reference already exists")
    if sha not in repo["commits"]:
        raise ApiError(422, "Object does not exist")
    repo["refs"][branch] = sha
    return 201, ref_json(branch, sha)


def patch_ref(st, m, body, query):
    repo, branch, new = st.repo(m.group(1)), m.group(2), body["sha"]
    old = found(repo["refs"], branch)
    if new not in repo["commits"]:
        raise ApiError(422, "Object does not exist")
    # A fast-forward: the branch's commit is the new one or one of its ancestors.
    if not body.get("force") and old not in st.ancestors(repo, new):
        raise ApiError(422, "Update is not a fast forward")
    repo["refs"][branch] = new
    return 200, ref_json(branch, new)


def post_pull(st, m, body, query):
    name, repo = m.group(1), st.repo(m.group(1))
    head, base = body["head"], body["base"]
    if head not in repo["refs"] or base not in repo["refs"]:
        raise ApiError(422, "head and base must be branches")
    if any(p["head"]["ref"] == head and p["base"]["ref"] == base and p["state"] == "open" for p in repo["pulls"]):
        raise ApiError(422, "A pull request already exists")
    number = len(repo["pulls"]) + 1
    pull = {"number": number, "title": body["title"], "body": body.get("body", ""), "state": "open",
            "html_url": f"http://127.0.0.1:{st.port}/{name}/pull/{number}",
            "head": {"ref": head}, "base": {"ref": base}}
    repo["pulls"].append(pull)
    return 201, pull


def test_push(st, m, body, query):
    """A commit on top of a branch, made by someone else. A null file is deleted."""
    repo, branch = st.repo(body["repo"]), body["branch"]
    head = found(repo["refs"], branch)
    flat = dict(repo["trees"][repo["commits"][head]["tree"]])
    for path, text in body["files"].items():
        if text is None:
            flat.pop(path, None)
        else:
            flat[path] = st.put_blob(repo, text.encode())
    sha = st.put_commit(repo, st.put_tree(repo, flat), [head], body["message"])
    repo["refs"][branch] = sha
    return 200, {"sha": sha}


def test_state(st, m, body, query):
    repo = st.repo(query.get("repo", [st.seeded])[0])
    trees = {b: {p: repo["blobs"][s].decode("utf-8", "replace")
                 for p, s in repo["trees"][repo["commits"][c]["tree"]].items()}
             for b, c in repo["refs"].items()}
    commits = {c: {"parents": v["parents"], "message": v["message"]} for c, v in repo["commits"].items()}
    return 200, {"refs": dict(repo["refs"]), "trees": trees, "pulls": repo["pulls"], "commits": commits}


def test_requests(st, m, body, query):
    return 200, st.log


API = r"/api/v3/repos/([^/]+/[^/]+)"
ROUTES = [
    ("GET", API, get_repo),
    ("GET", API + r"/git/ref/heads/(.+)", get_ref),
    ("GET", API + r"/git/commits/([0-9a-f]+)", get_commit),
    ("GET", API + r"/git/trees/([0-9a-f]+)", get_tree),
    ("GET", API + r"/git/blobs/([0-9a-f]+)", get_blob),
    ("POST", API + r"/git/blobs", post_blob),
    ("POST", API + r"/git/trees", post_tree),
    ("POST", API + r"/git/commits", post_commit),
    ("POST", API + r"/git/refs", post_ref),
    ("PATCH", API + r"/git/refs/heads/(.+)", patch_ref),
    ("POST", API + r"/pulls", post_pull),
    ("POST", "/_test/push", test_push),
    ("GET", "/_test/state", test_state),
    ("GET", "/_test/requests", test_requests),
]


def handle(st, method, target, raw, auth):
    """Answer one request with (status, body). Every request is logged as sent, refusals too."""
    url = urlsplit(target)
    path, query = unquote(url.path), parse_qs(url.query)
    st.log.append({"method": method, "path": target})
    for verb, pattern, fn in ROUTES:
        m = re.fullmatch(pattern, path)
        if m and verb == method:
            break
    else:
        return 404, {"message": "Not Found"}
    if path.startswith("/api/") and auth not in (f"Bearer {st.token}", f"token {st.token}"):
        return 401, {"message": "Bad credentials"}
    try:
        body = json.loads(raw or b"{}")
    except ValueError:
        return 400, {"message": "Problems parsing JSON"}
    try:
        return fn(st, m, body, query)
    except ApiError as e:
        return e.status, {"message": e.message}
    except (KeyError, TypeError, ValueError, AttributeError) as e:
        return 422, {"message": f"Validation Failed: {e!r}"}


class Handler(BaseHTTPRequestHandler):
    def _answer(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        st = self.server.store
        with st.lock:
            status, body = handle(st, self.command, self.path, raw, self.headers.get("Authorization", ""))
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_PATCH = _answer

    def log_message(self, *args):
        pass  # the console stays quiet; GET /_test/requests has the record


def make_server(port, token, repo=DEFAULT_REPO):
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.store = Store(token, repo, server.server_address[1])
    return server


def start(port=0, token="secret-token", repo=DEFAULT_REPO):
    """Serve in a daemon thread and return the server (port 0 picks a free one)."""
    server = make_server(port, token, repo)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def stop(server):
    server.shutdown()
    server.server_close()


def selftest():
    server = start(0, "tok")
    port = server.server_address[1]
    base = f"http://127.0.0.1:{port}"
    api = f"{base}/api/v3/repos/acme/sales"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # stay on this machine

    def call(method, url, body=None, token="tok"):
        req = urllib.request.Request(url, method=method, headers={"Authorization": f"Bearer {token}"},
                                     data=None if body is None else json.dumps(body).encode())
        try:
            with opener.open(req) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    assert git_blob_id(b"hello\n") == "ce013625030ba8dba906f756967f9e9ca394464a"
    c0 = call("GET", f"{api}/git/ref/heads/main")[1]["object"]["sha"]
    t0 = call("GET", f"{api}/git/commits/{c0}")[1]["tree"]["sha"]
    tree = call("GET", f"{api}/git/trees/{t0}?recursive=1")[1]["tree"]
    assert {e["path"]: e["type"] for e in tree} == {"pondra.toml": "blob", "objects": "tree",
                                                    "objects/sales": "tree", "objects/sales/orders.sql": "blob"}
    toml = next(e["sha"] for e in tree if e["path"] == "pondra.toml")
    assert base64.b64decode(call("GET", f"{api}/git/blobs/{toml}")[1]["content"]) == b'[project]\nname = "sales"\n'
    hello = call("POST", f"{api}/git/blobs", {"content": base64.b64encode(b"hello\n").decode(), "encoding": "base64"})
    assert hello[1]["sha"] == "ce013625030ba8dba906f756967f9e9ca394464a"
    t2 = call("POST", f"{api}/git/trees", {"base_tree": t0, "tree": [
        {"path": "pondra.toml", "mode": "100644", "type": "blob", "sha": hello[1]["sha"]},
        {"path": "objects/sales/orders.sql", "mode": "100644", "type": "blob", "sha": None}]})[1]["sha"]
    status, c1 = call("POST", f"{api}/git/commits", {"message": "change", "tree": t2, "parents": [c0]})
    c1 = c1["sha"]
    assert status == 201
    assert call("PATCH", f"{api}/git/refs/heads/main", {"sha": c1, "force": False})[0] == 200
    assert call("PATCH", f"{api}/git/refs/heads/main", {"sha": c0, "force": False})[0] == 422
    assert call("POST", f"{api}/git/refs", {"ref": "refs/heads/feature", "sha": c1})[0] == 201
    status, pull = call("POST", f"{api}/pulls", {"title": "t", "head": "feature", "base": "main", "body": ""})
    assert status == 201 and pull["html_url"] == f"{base}/acme/sales/pull/1", pull
    pushed = call("POST", f"{base}/_test/push", {"repo": "acme/sales", "branch": "main",
                                                 "files": {"README.md": "hi\n"}, "message": "someone else"})[1]["sha"]
    state = call("GET", f"{base}/_test/state?repo=acme/sales")[1]
    assert state["refs"] == {"main": pushed, "feature": c1}, state["refs"]
    assert state["trees"]["main"] == {"pondra.toml": "hello\n", "README.md": "hi\n"}, state["trees"]
    assert state["commits"][pushed]["parents"] == [c1] and len(state["pulls"]) == 1
    reqs = call("GET", f"{base}/_test/requests")[1]
    assert any(r["path"].startswith("/api/v3/repos/acme/sales/git/trees/") for r in reqs)
    assert call("GET", f"{base}/api/v3/repos/acme/other")[0] == 404
    assert call("GET", f"{api}/git/ref/heads/main", token="wrong")[0] == 401
    stop(server)
    print("selftest ok")


def main():
    parser = argparse.ArgumentParser(description="A fake GitHub REST API for the checks (see the module docstring).")
    parser.add_argument("--port", type=int, default=9990)
    parser.add_argument("--token", default="secret-token", help="the bearer token every API request sends")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="the repository that starts with a commit on main")
    parser.add_argument("--selftest", action="store_true", help="check the routes, then exit")
    args = parser.parse_args()
    if args.selftest:
        return selftest()
    server = make_server(args.port, args.token, args.repo)
    print(f"fake GitHub on http://127.0.0.1:{server.server_address[1]}/api/v3, repository {args.repo}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass  # Ctrl-C stops it
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
