#!/usr/bin/env python3
"""One set of SQL cases on every form (ADR-072 §6, reviews/form-parity.md): each form answers as a node does, or
refuses by the name docs/forms.json gives it.

  forms_check.py [--form server,cluster,inprocess,memory,page] [--only keys,defaults] [--bin PATH] [--lib PATH]
                 [--pkg web/pkg] [--out PATH]

The cases are tests/forms/*.slt, sqllogictest records as slt_check.py reads and prints them, one behaviour a file,
each file on a lake (or a page) of its own:
- `statement ok`, `statement count N`, and `query <types> [rowsort|valuesort]` with its answer;
- `statement error <SQLSTATE> [words]` and `query error …`: the refusal's x-pondra-sqlstate, and words its message
  holds (a regular expression, as in slt_check.py);
- `put <path>` and its lines up to a blank line: a file every form reads by that name, given where its caller reads a
  relative name (designs/l2-p5-files.md): `PUT /files/<path>` for a client over the network (server, cluster, page),
  and the working folder for the program that started the node (inprocess, memory: a fresh folder for each file).

Every form is reached through the one request contract, request(method, path, headers, body):
- server: a node over HTTP; cluster: three nodes on one lake, each query spread over them (`?spread=1`);
- inprocess and memory: a node in this process through the library's pondra_request (python/pondra/inprocess.py),
  on a folder and on `:memory:`;
- page: web/pkg's Page, a member on the core's lake, in Node behind a port (page_serve.mjs), over HTTP as a node.

docs/forms.json's `refusals` are the only differences a form may have. On that form, the file's first record that
doesn't pass must be refused with the listed SQLSTATE and words, and the rest of the file isn't run. Each names its
`owner` and `why`. One that isn't yet named (a 0A000 in Pondra's own words, not DataFusion's) is printed as a note
with its words, for its owner to name or to make the form answer as a node does. Anything else
fails: a wrong answer, an error where none is wanted, a refusal that isn't listed (a 0A000 too), and a listed refusal
that no longer comes (take it out of forms.json). It also fails when src/lib.rs builds a module for the server only
that forms.json's `server_only` doesn't list with the reason a page lacks it, or lists one that every form builds.
Exit 1 on a failure.
"""
import argparse, http.client, json, os, re, shutil, subprocess, sys, tempfile, time, uuid

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import harness, slt_check

FORMS = ("server", "cluster", "inprocess", "memory", "page")
SQLSTATE = re.compile(r"^[0-9A-Z]{5}$")
# DataFusion's own words for what it lacks: a refusal in them isn't yet one the form names (ADR-072 §6)
DATAFUSION = re.compile(r"This feature is not implemented|not supported|Unsupported|SQL error|Schema error|Error during planning|Inconsistent data length|Placeholder")
TIMEOUT = 120


class Nodes:
    """server and cluster: `count` nodes on a fresh lake for each file, over HTTP."""

    def __init__(self, count, port):
        self.count, self.port, self.spread, self.nodes = count, port, count > 1, []

    def open(self):
        lake = harness.new_lake()
        self.nodes = [harness.Node(lake, self.port + i).start() for i in range(self.count)]
        while len(harness.call(self.port, "GET", "/stats").get("nodes", ())) < self.count:
            time.sleep(0.1)

    def request(self, method, path, headers, body):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=TIMEOUT)
        c.request(method, path, body, headers)
        r = c.getresponse()
        return r.status, {k.lower(): v for k, v in r.getheaders()}, r.read()

    def put(self, path, text, session):
        """A file a client over the network gives: `PUT /files/<path>`, read from the lake's files/."""
        status, head, body = self.request("PUT", "/files/" + path, session, text.encode())
        said = body.decode(errors="replace")
        return None if status < 300 else (f"PUT /files/{path}: {status} {said[:300]}", head.get("x-pondra-sqlstate"), said)

    def close(self):
        [node.kill() for node in self.nodes]
        harness.clean_up()


class InProcess:
    """inprocess and memory: a node in this process (the library), on a fresh folder, or on `:memory:`, for each file."""

    def __init__(self, memory):
        self.memory, self.spread, self.db, self.lake, self.home = memory, False, None, None, None

    def open(self):
        sys.path.insert(0, os.path.join(REPO, "python"))
        from pondra import inprocess
        self.lake = None if self.memory else tempfile.mkdtemp(prefix="pondra-forms-")
        self.home, self.folder = os.getcwd(), tempfile.mkdtemp(prefix="pondra-forms-cwd-")
        os.chdir(self.folder)  # (the program that started the node reads a relative name here: put writes into it)
        self.db = inprocess.connect(self.lake, python=False)
        if getattr(self.db, "_inproc", None) is None:  # (no library here: connect() started a node beside this process)
            raise RuntimeError(f"no library at {os.environ.get('PONDRA_LIBRARY')}: connect() gave a node beside this process, not in it")

    def request(self, method, path, headers, body):
        from pondra import client
        try:
            r = self.db._inproc(client._Request("http://in.process" + path, data=body, method=method, headers=headers))
            return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()
        except client._Refused as e:  # (a refusal, as the node in this process answers one: `Pondra._call` reads it so)
            return e.code, {k.lower(): v for k, v in e.headers.items()}, e.body

    def put(self, path, text, session):
        """A file the program that started the node gives: written into its working folder."""
        full = os.path.join(self.folder, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            f.write(text)

    def close(self):
        if self.db:
            self.db.close()
        os.chdir(self.home)
        for folder in (self.lake, self.folder):
            if folder:
                shutil.rmtree(folder, ignore_errors=True)


class Page(Nodes):
    """page: web/pkg's Page, a member on the core's lake, in Node behind a port (page_serve.mjs), made afresh
    (`POST /reset`) for each file."""

    def __init__(self, pkg):
        super().__init__(1, None)
        self.pkg, self.child = pkg, None

    def open(self):
        if self.child is None:
            cmd = ["node", os.path.join(HERE, "page_serve.mjs"), "--pkg", self.pkg]
            self.child = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
            self.port = int(json.loads(self.child.stdout.readline())["url"].rsplit(":", 1)[1])
        status, _, said = self.request("POST", "/reset", {}, b"")
        if status != 204:
            raise RuntimeError(f"the page didn't reset: {status} {said[:200]!r}")

    def close(self):
        pass

    def stop(self):
        self.child.terminate()
        self.child.wait()


def wanted(header):
    """What an `error` record wants: (SQLSTATE or None, the words its message holds)."""
    rest = header[1:]
    state = rest[0] if rest and SQLSTATE.match(rest[0]) else None
    return state, " ".join(rest[1:] if state else rest)


def outcome(form, session, kind, header, text, expected):
    """None when the record passes on the form, else (why, the refusal's SQLSTATE or None, its words)."""
    if kind == "put":
        return form.put(header[0], text, session)
    path = "/sql?format=arrow" + ("&spread=1" if kind == "query" and form.spread else "")
    status, head, body = form.request("POST", path, session, text.encode())
    refusal = header[:1] == ["error"]
    if status >= 400:
        state, said = head.get("x-pondra-sqlstate"), body.decode(errors="replace")
        if not refusal:
            return f"refused ({state}): {said[:300]}", state, said
        want, words = wanted(header)
        if want and state != want:
            return f"refused with {state}, not {want}: {said[:300]}", state, said
        if words and words not in said and not re.search(words, said, re.S):
            return f"refused without the words {words!r}: {said[:300]}", state, said
        return None
    if refusal:
        return "answered, where a refusal is wanted", None, ""
    data = json.loads(body) if body[:1] in (b"{", b"[") else body
    if kind == "statement":
        if header[:1] == ["count"] and slt_check.printed(data) != [[header[1]]]:
            return f"count {slt_check.printed(data)}, want {header[1]}", None, ""
        return None
    rows = slt_check.printed(data)
    if slt_check.same(rows, expected, header):
        return None
    return f"got {[' '.join(r) for r in rows[:5]]}, want {expected[:5]}", None, ""


def named(state, said):
    return state == "0A000" and not DATAFUSION.search(said)


def run_file(name, form, path, refusal):
    """(records passed, records the refusal left unrun, failures, the listed refusal's words when it isn't yet named)
    of one file on one form."""
    records = slt_check.records(path)
    passed, failures, session = 0, [], {"x-pondra-session": uuid.uuid4().hex}  # (one session a file: its SETs last to its end)
    form.open()
    try:
        for n, (kind, header, text, expected, line) in enumerate(records):
            try:
                bad = outcome(form, session, kind, header, text, expected)
            except Exception as e:
                bad = (f"the request failed: {e}", None, "")
            if bad is None:
                passed += 1
                continue
            why, state, said = bad
            if refusal and state == refusal["sqlstate"] and re.search(refusal["says"], said, re.S):
                return passed, len(records) - n, failures, None if named(state, said) else said
            if refusal:
                why += f" (forms.json lists a refusal here: {refusal['sqlstate']} saying {refusal['says']!r})"
                refusal = None  # (counted once: the rest of the file is judged as any other)
            failures.append({"form": name, "file": os.path.basename(path), "line": line, "sql": text[:300], "why": why[:600]})
    finally:
        form.close()
    if refusal:
        failures.append({"form": name, "file": os.path.basename(path), "line": 0, "sql": "",
                         "why": f"every record passed, so forms.json's refusal ({refusal['sqlstate']}: {refusal['says']!r}) no longer comes: take it out"})
    return passed, 0, failures, None


def split(table):
    """src/lib.rs's modules built for the server only (its `lake` and `doors`) that forms.json's `server_only` doesn't
    list, and those it lists that aren't."""
    built = set(re.findall(r'^#\[cfg\((?:all\()?feature = "(?:server|lake|doors)"[^\]]*\)\] (?:pub )?mod (\w+);', open(os.path.join(REPO, "src/lib.rs")).read(), re.M))
    listed = {m for group in table["server_only"].values() for m in group["modules"]}
    return sorted(built - listed), sorted(listed - built)


def form_of(name):
    return {"server": lambda: Nodes(1, A.port), "cluster": lambda: Nodes(3, A.port), "inprocess": lambda: InProcess(False),
            "memory": lambda: InProcess(True), "page": lambda: Page(A.pkg)}[name]()


def main():
    harness.A = argparse.Namespace(s3=False, keep=False, port=A.port)
    A.bin, A.lib, A.pkg = (os.path.abspath(p) for p in (A.bin, A.lib, A.pkg))  # (inprocess and memory leave this folder)
    harness.BIN = A.bin
    os.environ["PONDRA_LIBRARY"] = A.lib
    os.environ.setdefault("PONDRA_BIN", A.bin)  # (the library's launcher, as inprocess_check.py gives it)
    table = json.load(open(os.path.join(REPO, "docs/forms.json")))
    files = sorted(f for f in os.listdir(os.path.join(REPO, "tests/forms")) if f.endswith(".slt"))
    if A.only:
        files = [f for f in files if f[:-4] in A.only.split(",")]
    refusals = {(r["form"], r["file"]): r for r in table["refusals"]}
    unknown = sorted({f"{f}:{p}" for f, p in refusals if f not in FORMS or not os.path.isfile(os.path.join(REPO, "tests/forms", p))})
    failures = [{"form": "-", "file": "docs/forms.json", "line": 0, "sql": "", "why": f"a refusal for no form or file: {u}"} for u in unknown]
    unlisted, stale = split(table)
    failures += [{"form": "-", "file": "src/lib.rs", "line": 0, "sql": "", "why": f"mod {m} is built for the server only: list it in docs/forms.json's server_only with what a page lacks for it, or build it in every form"} for m in unlisted]
    failures += [{"form": "-", "file": "docs/forms.json", "line": 0, "sql": "", "why": f"server_only lists {m}, which src/lib.rs builds in every form (or no longer has): take it out"} for m in stale]
    summary, unnamed, t0 = {}, [], time.time()
    for name in A.form.split(","):
        form, counts = form_of(name), {"files": len(files), "passed": 0, "refused": 0, "unrun": 0, "failed": 0}
        for f in files:
            passed, unrun, failed, said = run_file(name, form, os.path.join(REPO, "tests/forms", f), refusals.get((name, f)))
            if said is not None:
                unnamed.append((refusals[(name, f)], said))
            counts["passed"] += passed
            counts["refused"] += 1 if unrun else 0
            counts["unrun"] += unrun
            counts["failed"] += len(failed)
            failures += failed
        if isinstance(form, Page) and form.child:
            form.stop()
        summary[name] = counts
        print(f"{name}: {counts['passed']} records pass, {counts['failed']} fail; {counts['refused']} files refused as forms.json lists, {counts['unrun']} records from there unrun", flush=True)
    for r, said in unnamed:  # (listed, but not yet the named refusal ADR-072 asks for)
        print(f"note {r['form']} {r['file']}: not yet named, refused with {r['sqlstate']} {' '.join(said.split())[:160]!r} ({r['owner']}: {r['why']})")
    for x in failures:
        print(f"FAIL {x['form']} {x['file']}:{x['line']} {x['why']}" + (f"\n     {' '.join(x['sql'].split())[:200]}" if x["sql"] else ""))
    out = {"forms": summary, "failures": failures, "secs": round(time.time() - t0), "ok": not failures,
           "unnamed refusals": [{"form": r["form"], "file": r["file"], "owner": r["owner"], "says": said[:300]} for r, said in unnamed]}
    print(json.dumps({"forms": summary, "unnamed refusals": len(unnamed), "secs": out["secs"], "ok": out["ok"]}))
    if A.out:
        json.dump(out, open(A.out, "w"), indent=1)
    sys.exit(0 if out["ok"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--form", default=",".join(FORMS), help="forms, comma-separated, by name: " + ", ".join(FORMS))
    ap.add_argument("--only", default="", help="files by name, without .slt: keys,defaults")
    ap.add_argument("--bin", default=os.environ.get("PONDRA_BIN", os.path.join(REPO, "target/release/pondra")))
    ap.add_argument("--lib", default=os.environ.get("PONDRA_LIBRARY", os.path.join(REPO, "target/release/libpondra_lib.so")))
    ap.add_argument("--pkg", default=os.path.join(REPO, "web/pkg"))
    ap.add_argument("--port", type=int, default=8950)
    ap.add_argument("--out")
    A = ap.parse_args()
    main()
