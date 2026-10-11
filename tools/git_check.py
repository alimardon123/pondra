#!/usr/bin/env python3
"""Git folders (ADR-060 §8): a folder of the workspace paired with a branch of a repository.

A node checks a branch out into the workspace, shows the folder's local changes, commits some or all
of them on top of the branch, pulls what someone else pushed, refuses a commit or a pull that would
lose work, makes a branch, switches back, and opens a pull request. The repository is fake_github.py
(a stand-in for GitHub's REST API, in this process), and the node talks to it over HTTP as it would to
GitHub. Checks, each a key in the output:

- refused without a git secret covering the repository; git_status of a folder that isn't one;
- git_checkout: the files and the record are in the workspace, git_status has no rows;
- CALL plan('files/sales') lists the project's tables;
- a file changed, one added, one deleted: git_status lists the three;
- git_commit of one path: a commit on main with that file only; the other two still listed;
- git_commit of the rest: no changes left, and main holds exactly the folder's files;
- someone else pushes: a commit here is refused ("moved on"); git_pull brings their file; the commit goes;
- a path changed here and there: git_pull refused by name, and nothing changed;
- git_checkout of a new branch: made at the folder's commit, local changes carried over; a commit on it;
  git_pull_request opens a pull request from it into main;
- git_checkout back to main: refused with local changes, naming the file; without, main's files replace it;
- a file deleted here and on main: git_pull takes it;
- refused by name, without a request to the repository: a procedure of their names, a git secret with no
  SCOPE, a secret named that doesn't cover the repository, a token over plain http to another machine,
  GitLab, a branch name git refuses, an argument the procedure doesn't take, the workspace itself;
- a reader is refused git_commit, and reads git_status.

  git_check.py [--new target/release/pondra] [--work DIR] [--port 9780]

Prints the checks as JSON and exits 1 if one fails (the lake and node log are then kept in --work).
"""
import argparse, json, os, shutil, sys, tempfile, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fake_github  # (a stand-in for GitHub's REST API, served in this process)
from upgrade_check import Node, Failed, OWNER, TOKEN, stop_all

GH_TOKEN = "git-check-github-token"
READ_TOKEN = "git-check-reader"
REPO = "acme/sales"
PONDRA_TOML = '[project]\nname = "sales"\n'
ORDERS = "CREATE TABLE sales.orders (id BIGINT, amount DOUBLE);\n"
ORDERS_NEW = "CREATE TABLE sales.orders (id BIGINT, amount DOUBLE, channel VARCHAR);\n"
ORDERS_THEIRS = "CREATE TABLE sales.orders (id BIGINT, amount DECIMAL(12, 2));\n"
ORDERS_LOCAL = "CREATE TABLE sales.orders (id BIGINT);\n"
OLD = "CREATE TABLE sales.old (id BIGINT);\n"
DISCOUNTS = "CREATE TABLE sales.discounts (channel VARCHAR, pct DOUBLE);\n"
FROM_THEM = "CREATE TABLE sales.from_them (id BIGINT);\n"
MINE = "CREATE TABLE sales.mine (id BIGINT);\n"
DEV_ONLY = "CREATE TABLE sales.dev_only (id BIGINT);\n"
OPEN = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # (this machine's own requests)


def fetch(n, method, path, body=None, headers=None, token=TOKEN):
    """(status, bytes, headers) of a request to the node, refusals too."""
    req = urllib.request.Request(f"http://127.0.0.1:{n.port}{path}", data=body, method=method,
                                 headers={"Authorization": f"Bearer {token}", "x-pondra-owner": OWNER, **(headers or {})})
    try:
        with OPEN.open(req, timeout=120) as r:
            return r.status, r.read(), r.headers  # (a Message: its names are case-blind)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), e.headers


def text(n, path):
    status, body, _ = fetch(n, "GET", f"/files/{path}")
    if status != 200:
        raise Failed(f"GET /files/{path}: {status} {body[:200]!r}")
    return body.decode()


def version(n, path):
    return fetch(n, "GET", f"/files/{path}")[2].get("ETag")  # (what If-Match takes)


def put(n, path, body, expect=None):
    status, out, _ = fetch(n, "PUT", f"/files/{path}", body.encode(), {"If-Match": expect} if expect else {})
    if status != 200:
        raise Failed(f"PUT /files/{path}: {status} {out[:200]!r}")


def delete(n, path):
    status, out, _ = fetch(n, "DELETE", f"/files/{path}")
    if status != 200:
        raise Failed(f"DELETE /files/{path}: {status} {out[:200]!r}")


def refusal(fn):
    """What the node said when `fn` (a statement) was refused; '' when it was not."""
    try:
        fn()
    except Failed as e:
        return str(e)
    return ""


def rows(x):
    return x if isinstance(x, list) else [x]


def changes(answer):
    return sorted((r["path"], r["change"]) for r in rows(answer))


def push(port, files, message, branch="main"):
    """A commit by someone else on the fake's branch (`/_test/push`)."""
    req = urllib.request.Request(f"http://127.0.0.1:{port}/_test/push", method="POST",
                                 data=json.dumps({"repo": REPO, "branch": branch, "files": files, "message": message}).encode())
    with OPEN.open(req, timeout=30) as r:
        return json.loads(r.read())["sha"]


def fake_state(port):
    with OPEN.open(f"http://127.0.0.1:{port}/_test/state?repo={REPO}", timeout=30) as r:
        return json.loads(r.read())


def api_calls(port):
    """How many requests the fake's API (not its /_test/ routes) has had."""
    with OPEN.open(f"http://127.0.0.1:{port}/_test/requests", timeout=30) as r:
        return sum(1 for x in json.loads(r.read()) if x["path"].startswith("/api/"))


def git_check(bin, work, port):
    lake = os.path.join(work, "lake")
    fp = port + 1  # (the fake's port)
    fake = fake_github.start(fp, GH_TOKEN)
    repo_url, scope = f"http://127.0.0.1:{fp}/acme/sales", f"http://127.0.0.1:{fp}/acme"
    c = {}
    try:
        push(fp, {"objects/sales/old.sql": OLD}, "old.sql, by someone else")
        n = Node(bin, lake, port, work, "--read-token", READ_TOKEN).start()
        q = n.q
        c["refused without a git secret covering the repository"] = "no git secret covers" in refusal(lambda: q(f"CALL git_checkout('sales', repository => '{repo_url}', branch => 'main')"))
        c["git_status of a folder that isn't a Git folder is refused"] = "not a Git folder" in refusal(lambda: q("CALL git_status('nothere')"))
        q(f"CREATE SECRET ann_git (TYPE git, TOKEN '{GH_TOKEN}', SCOPE '{scope}')")

        out = q(f"CALL git_checkout('sales', repository => '{repo_url}', branch => 'main')")
        record = json.loads(text(n, "sales/.pondra-git.json"))
        head = fake_state(fp)["refs"]["main"]
        c["checkout: the branch's files and the record are in the workspace"] = (
            out.get("files") == 3 and text(n, "sales/pondra.toml") == PONDRA_TOML and text(n, "sales/objects/sales/orders.sql") == ORDERS
            and text(n, "sales/objects/sales/old.sql") == OLD and record["branch"] == "main" and record["commit"] == head)
        c["git_status has no rows (CALL and pondra.git_status)"] = rows(q("CALL git_status('sales')")) == [] and rows(q("SELECT * FROM pondra.git_status('sales')")) == []
        c["CALL plan lists the project's tables"] = any("sales.orders" in json.dumps(r) for r in rows(q("CALL plan('files/sales')")))

        put(n, "sales/objects/sales/orders.sql", ORDERS_NEW, version(n, "sales/objects/sales/orders.sql"))
        put(n, "sales/objects/sales/discounts.sql", DISCOUNTS)
        delete(n, "sales/objects/sales/old.sql")
        three = [("objects/sales/discounts.sql", "added"), ("objects/sales/old.sql", "deleted"), ("objects/sales/orders.sql", "modified")]
        c["git_status lists the added, modified and deleted files"] = changes(q("CALL git_status('sales')")) == three
        c["pondra.git_status in a query lists the same"] = changes(q("SELECT path, change, branch, commit FROM pondra.git_status('sales')")) == three

        out = q("CALL git_commit('sales', 'Discounts by channel', paths => ['objects/sales/discounts.sql'])")
        state = fake_state(fp)
        mid = state["refs"]["main"]
        c["git_commit of one path: a commit on top of the record's, that file only"] = (
            mid != head and state["commits"][mid]["parents"] == [head] and state["trees"]["main"].get("objects/sales/discounts.sql") == DISCOUNTS
            and state["trees"]["main"]["objects/sales/orders.sql"] == ORDERS and "objects/sales/old.sql" in state["trees"]["main"])
        c["the other two changes are still listed"] = changes(q("CALL git_status('sales')")) == three[1:]

        q("CALL git_commit('sales', 'The rest of the discounts')")
        main_tree = fake_state(fp)["trees"]["main"]
        c["git_commit of the rest: no changes left"] = rows(q("CALL git_status('sales')")) == []
        c["main holds exactly the folder's files"] = main_tree == {"pondra.toml": PONDRA_TOML, "objects/sales/orders.sql": ORDERS_NEW, "objects/sales/discounts.sql": DISCOUNTS}

        push(fp, {"objects/sales/from_them.sql": FROM_THEM}, "from them")
        put(n, "sales/objects/sales/mine.sql", MINE)
        c["a commit after someone else's push is refused: moved on"] = "moved on" in refusal(lambda: q("CALL git_commit('sales', 'mine')"))
        pulled = q("CALL git_pull('sales')")
        c["git_pull brings their file, and counts it"] = text(n, "sales/objects/sales/from_them.sql") == FROM_THEM and pulled.get("pulled") == 1
        q("CALL git_commit('sales', 'mine')")
        c["then the commit goes through"] = fake_state(fp)["trees"]["main"].get("objects/sales/mine.sql") == MINE
        push(fp, {"objects/sales/mine.sql": None}, "mine.sql gone, by someone else")
        delete(n, "sales/objects/sales/mine.sql")
        pulled = q("CALL git_pull('sales')")
        c["a file deleted here and on main: git_pull takes it, nothing left to commit"] = pulled.get("pulled") == 1 and rows(q("CALL git_status('sales')")) == []

        push(fp, {"objects/sales/orders.sql": ORDERS_THEIRS}, "orders, by someone else")
        put(n, "sales/objects/sales/orders.sql", ORDERS_LOCAL, version(n, "sales/objects/sales/orders.sql"))
        before = text(n, "sales/.pondra-git.json")
        msg = refusal(lambda: q("CALL git_pull('sales')"))
        c["a path changed here and on main: git_pull refused, naming it"] = "changed here and on main: objects/sales/orders.sql" in msg
        c["… and nothing changed: the file and the record are as they were"] = text(n, "sales/objects/sales/orders.sql") == ORDERS_LOCAL and text(n, "sales/.pondra-git.json") == before

        out = q("CALL git_checkout('sales', branch => 'dev')")
        dev_at = fake_state(fp)["refs"].get("dev")
        c["a branch the repository lacks is made at the folder's commit"] = out.get("made") is True and dev_at == json.loads(before)["commit"]
        c["local changes carry over to it"] = changes(q("CALL git_status('sales')")) == [("objects/sales/orders.sql", "modified")]
        put(n, "sales/objects/sales/dev_only.sql", DEV_ONLY)
        q("CALL git_commit('sales', 'A file only on dev', paths => ['objects/sales/dev_only.sql'])")
        pr = q("CALL git_pull_request('sales', title => 'Discounts by channel')")
        pulls = fake_state(fp)["pulls"]
        c["git_pull_request: a pull request from dev into main, with its URL"] = (
            len(pulls) == 1 and pulls[0]["head"]["ref"] == "dev" and pulls[0]["base"]["ref"] == "main" and pr.get("url") == pulls[0]["html_url"] and pr.get("number") == 1)

        c["a checkout with local changes is refused, naming the file"] = "has changes not committed: objects/sales/orders.sql" in refusal(lambda: q("CALL git_checkout('sales', branch => 'main')"))
        put(n, "sales/objects/sales/orders.sql", ORDERS_NEW, version(n, "sales/objects/sales/orders.sql"))
        q("CALL git_checkout('sales', branch => 'main')")
        c["back on main with none: main's files, and dev's own file is gone"] = (
            fetch(n, "GET", "/files/sales/objects/sales/dev_only.sql")[0] == 404 and text(n, "sales/objects/sales/orders.sql") == ORDERS_THEIRS
            and text(n, "sales/objects/sales/from_them.sql") == FROM_THEM and json.loads(text(n, "sales/.pondra-git.json"))["branch"] == "main")

        q(f"CREATE SECRET other_git (TYPE git, TOKEN '{GH_TOKEN}', SCOPE 'http://127.0.0.1:{fp}/other')")
        q("CREATE SECRET far_git (TYPE git, TOKEN 'x', SCOPE 'http://127.0.0.1.example.com')")
        q("CREATE SECRET lab_git (TYPE git, TOKEN 'x', SCOPE 'https://gitlab.com/acme', PROVIDER 'gitlab')")
        asked = api_calls(fp)
        refused = {
            "CREATE PROCEDURE of one of the five": ("CREATE PROCEDURE git_commit() LANGUAGE sql AS $$ SELECT 1 $$", "Pondra's own procedures"),
            "a git secret with no SCOPE": ("CREATE SECRET bare_git (TYPE git, TOKEN 'x')", "SCOPE"),
            "a secret named that doesn't cover the repository": (f"CALL git_checkout('o', repository => '{repo_url}', secret => 'other_git')", "doesn't cover"),
            "a token over plain http to another machine": ("CALL git_checkout('o', repository => 'http://127.0.0.1.example.com/acme/sales')", "https only"),
            "GitLab, by name": ("CALL git_checkout('o', repository => 'https://gitlab.com/acme/sales')", "GitLab isn't spoken to yet"),
            "a branch name git refuses": (f"CALL git_checkout('o', repository => '{repo_url}', branch => '../main')", "a branch's name"),
            "an argument the procedure doesn't take": ("CALL git_pull('sales', force => true)", "has no force"),
            "the workspace itself": ("CALL git_status('files')", "not the workspace itself"),
        }
        said = {k: refusal(lambda sql=sql: q(sql)) for k, (sql, _) in refused.items()}
        c["refused by name: " + ", ".join(refused)] = all(refused[k][1] in said[k] for k in refused) or {k: said[k] for k in refused if refused[k][1] not in said[k]}
        c["… and none of them asked the repository anything"] = api_calls(fp) == asked

        status, body, _ = fetch(n, "POST", "/sql", b"CALL git_commit('sales', 'a reader')", token=READ_TOKEN)
        c["a reader is refused git_commit: it needs a write token"] = status >= 400 and b"write token" in body
        c["a reader reads git_status"] = fetch(n, "POST", "/sql", b"CALL git_status('sales')", token=READ_TOKEN)[0] == 200
    finally:
        stop_all()
        fake_github.stop(fake)
    return c


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new", default=os.path.join(HERE, "..", "target", "release", "pondra"), help="this build's binary")
    ap.add_argument("--work", default="", help="where the lake and logs go (default: a new temporary folder, removed if every check passes)")
    ap.add_argument("--port", type=int, default=9780, help="the node's port (the fake GitHub takes the next one)")
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="pondra-git-")
    os.makedirs(work, exist_ok=True)
    try:
        checks = git_check(os.path.abspath(a.new), work, a.port)
    except Exception as e:  # (a check that can't run ends the list, and says why)
        checks = {"ran to the end": False, "error": str(e)}
    finally:
        stop_all()
    ok = all(v is True for k, v in checks.items() if k != "error")
    print(json.dumps({**checks, "ok": ok}, indent=1))
    if ok and not a.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"(the lake and node log kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
