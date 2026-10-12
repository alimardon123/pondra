#!/usr/bin/env python3
"""Every example in the documentation website runs (ADR-030): each page's code blocks, in order,
against a fresh node, so the docs can't fall behind the code.

  python3 tools/docs_check.py                      # every page under site/src/content/docs
  python3 tools/docs_check.py site/src/content/docs/guides/views.mdx
  python3 tools/docs_check.py --links              # every link between pages lands on a page and a heading
  python3 tools/docs_check.py --write-glossary     # docs/glossary.md and the site's glossary, written from GET /kinds

A page gets its own lake and a node on the ports the docs use: HTTP 8080, Postgres 5432, Kafka
9092, Flight 8815 (and Python functions on). Its frontmatter's `setup:` SQL runs first, unseen.
Then its blocks run:
  sql      through POST /sql (several statements at once are fine);
  python   in one interpreter per page, so a block sees what the ones before it made;
  js       under Node, as an ES module, with `pondra` resolving to this repo's client;
  bash/sh  in bash, from the page's own folder, with `pondra` on PATH.
A `python cell` block is a console's Python cell: it runs on the node, as `DO LANGUAGE python`, in
the page's session (a page's cells share their variables, as a console's do).
A block whose info string says `norun` is shown but not run: things that can't run here (a real
cluster, Windows, credentials, a command that serves until stopped). Other languages (text,
json, toml, yaml, …) are never run.
A `<Ways sql={`…`} />` component is one more `sql` block: its SQL runs as one statement. Its five
ways (site/src/ways.json) are checked once a run, by ways_check.
"""
import argparse, json, os, re, shutil, socket, subprocess, sys, tempfile, textwrap, threading, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BIN = os.environ.get("PONDRA_BIN") or os.path.join(ROOT, "target", "release", "pondra")
DOC_PORTS = {"http": 8080, "pg": 5432, "kafka": 9092, "flight": 8815}  # (what the docs say)
PORTS = dict(DOC_PORTS)  # (where this run's node listens: --offset moves them, for runs side by side)
RUN = {"sql", "python", "py", "js", "javascript", "bash", "sh", "shell"}
BLOCK_SECS = 300  # (the longest a Python block may run)
FETCH_REFUSES = {1719, 1720, 1723, 2049, 3659, 4045, 4190, 5060, 5061, 6000, 6566, 6665, 6666, 6667, 6668, 6669, 6679, 6697, 10080}  # (ports Node's fetch won't call)
FENCE = re.compile(r"^(\s*)(`{3,}|~{3,})\s*([\w+-]*)\s*(.*)$")

# One Python per page: blocks come in on stdin as JSON lines, answers go out the same way.
PY_RUNNER = r"""
import json, sys, traceback, io, contextlib
scope = {"__name__": "__main__"}
for line in sys.stdin:
    code = json.loads(line)
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            exec(compile(code, "<example>", "exec"), scope)
        print(json.dumps({"ok": True, "out": out.getvalue()[-2000:]}), flush=True)
    except BaseException:
        print(json.dumps({"ok": False, "out": out.getvalue()[-2000:], "error": traceback.format_exc()[-3000:]}), flush=True)
"""


WAYS_ELEMENT = re.compile(r"<Ways\b([^>]*?)\bsql=\{`(.*?)`\}", re.S)


def ways_sql(raw):
    """The SQL in a `<Ways sql={`…`} />` as it reads. Text that starts on the backtick's line has no
    indentation there, so only the lines after it are dedented."""
    first, _, rest = raw.partition("\n")
    return "\n".join([first.strip(), textwrap.dedent(rest)]).strip()


def blocks(text):
    """The page's frontmatter (a dict) and its code blocks, fenced and `<Ways>`: (language, info, code, line),
    in page order."""
    front = {}
    if text.startswith("---\n"):
        end = text.index("\n---", 4)
        import yaml
        front = yaml.safe_load(text[4:end]) or {}
    out, lines, i, fenced = [], text.split("\n"), 0, set()
    while i < len(lines):
        m = FENCE.match(lines[i])
        if not m:
            i += 1
            continue
        indent, mark, lang, info = m.groups()
        body, j = [], i + 1
        while j < len(lines) and not (lines[j].strip().startswith(mark) and lines[j].strip().strip(mark[0]) == ""):
            body.append(lines[j][len(indent):] if lines[j].startswith(indent) else lines[j].lstrip())
            j += 1
        out.append((lang.lower(), info, "\n".join(body), i + 1))
        fenced.update(range(i, j + 1))  # (code shown in a fence is text, not a component)
        i = j + 1
    # The components are looked for outside the fences, with their lines kept, so a line number is the page's.
    outside = "\n".join("" if n in fenced else line for n, line in enumerate(lines))
    for m in WAYS_ELEMENT.finditer(outside):
        info = "norun" if re.search(r"\bnorun\b", m.group(1)) else ""
        out.append(("sql", info, ways_sql(m.group(2)), outside.count("\n", 0, m.start()) + 1))
    return front, sorted(out, key=lambda b: b[3])


def free(port):
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def wait_http(url, secs=30):
    deadline = time.time() + secs
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2).read()
            return True
        except Exception:
            time.sleep(0.2)
    return False


def sql(q, timeout=120, session=None):
    req = urllib.request.Request(f"http://127.0.0.1:{PORTS['http']}/sql", data=q.encode(), method="POST", headers={"x-pondra-session": session} if session else {})
    try:
        return urllib.request.urlopen(req, timeout=timeout).read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode(errors="replace")[:2000])


def js_env(work):
    """A folder where `import … from 'pondra'` finds this repo's client."""
    mods = os.path.join(work, "node_modules")
    os.makedirs(mods, exist_ok=True)
    link = os.path.join(mods, "pondra")
    if not os.path.exists(link):
        os.symlink(os.path.join(ROOT, "js"), link)
    return work


def shifted(code):
    """The example as it runs here: the docs' ports moved by --offset."""
    for name, port in DOC_PORTS.items():
        if PORTS[name] != port:
            code = re.sub(rf"(?<![\d]){port}(?![\d])", str(PORTS[name]), code)
    return code


def start_node(work):
    """A node on this run's ports, its lake in `work` (a page's examples, and the glossary's kinds)."""
    env = {**os.environ, "PATH": os.path.dirname(BIN) + os.pathsep + os.environ.get("PATH", ""), "PONDRA_URL": f"http://127.0.0.1:{PORTS['http']}",
           "PYTHONPATH": os.path.join(ROOT, "python") + os.pathsep + os.environ.get("PYTHONPATH", ""), "PONDRA_BIN": BIN}
    node = subprocess.Popen([BIN, "serve", "--dir", os.path.join(work, "lake"), "--addr", f"127.0.0.1:{PORTS['http']}", "--pg", f"127.0.0.1:{PORTS['pg']}",
                             "--kafka", f"127.0.0.1:{PORTS['kafka']}", "--flight", f"127.0.0.1:{PORTS['flight']}", "--python", "auto", "--stop-with-stdin", "--tier-secs", "1"],
                            cwd=work, env=env, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=open(os.path.join(work, "node.log"), "w"))
    return node, env


def stop_node(node):
    """Closing its standard input stops it (it gives its leader's term up); it is killed if that takes long."""
    node.stdin.close()
    try:
        node.wait(timeout=20)
    except subprocess.TimeoutExpired:
        node.kill()


def check_page(path, verbose):
    front, found = blocks(open(path, encoding="utf-8").read())
    found = [(lang, info, shifted(code), line) for lang, info, code, line in found]
    runnable = [b for b in found if b[0] in RUN and "norun" not in b[1].split()]
    if not runnable and not front.get("setup"):
        return {"page": path, "ran": 0, "ok": True}
    busy = [n for n, p in PORTS.items() if not free(p)]
    if busy:
        return {"page": path, "ok": False, "error": f"ports in use: {busy} (stop what holds them)"}
    work = tempfile.mkdtemp(prefix="pondra-docs-")
    node, env = start_node(work)
    py, report = None, {"page": path, "ran": 0, "ok": True}
    try:
        if not wait_http(f"http://127.0.0.1:{PORTS['http']}/stats"):
            raise RuntimeError("the node didn't start: " + open(os.path.join(work, "node.log")).read()[-1000:])
        if front.get("setup"):
            sql(front["setup"])
        for lang, info, code, line in runnable:
            where = f"{os.path.relpath(path, ROOT)}:{line}"
            try:
                if lang == "sql":
                    out = sql(code)
                elif lang in ("python", "py") and "cell" in info.split():
                    out = sql(f"DO LANGUAGE python $pondra$\n{code}\n$pondra$", session=f"docs-page-{os.getpid()}")  # (a console's Python cell: on the node, in the page's session, as the console runs it: a cell sees what the ones before it made)
                elif lang in ("python", "py"):
                    if py is None:
                        py = subprocess.Popen([sys.executable, "-c", PY_RUNNER], cwd=work, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
                    py.stdin.write(json.dumps(code) + "\n")
                    py.stdin.flush()
                    answer = None
                    stuck = threading.Timer(BLOCK_SECS, py.kill)  # (a wait that never ends fails the page, not the build)
                    stuck.start()
                    while answer is None:  # (a library writing to the real stdout, as DuckDB's progress bar does, isn't the answer)
                        line = py.stdout.readline()
                        try:
                            got = json.loads(line or json.dumps({"ok": False, "error": f"python exited (or ran past {BLOCK_SECS} s)"}))
                        except json.JSONDecodeError:
                            continue
                        answer = got if isinstance(got, dict) and "ok" in got else None
                    stuck.cancel()
                    if not answer["ok"]:
                        raise RuntimeError(answer.get("error", ""))
                    out = answer["out"]
                elif lang in ("js", "javascript"):
                    r = subprocess.run(["node", "--input-type=module"], input=code, cwd=js_env(work), env=env, capture_output=True, text=True, timeout=120)
                    if r.returncode:
                        raise RuntimeError(r.stderr[-2000:])
                    out = r.stdout
                else:
                    r = subprocess.run(["bash", "-e", "-c", code], cwd=work, env=env, capture_output=True, text=True, timeout=180)
                    if r.returncode:
                        raise RuntimeError((r.stdout + r.stderr)[-2000:])
                    out = r.stdout
                report["ran"] += 1
                if verbose:
                    print(f"  ok   {where} ({lang})", flush=True)
                    if (out or "").strip():
                        print("       " + str(out).strip()[:1500].replace("\n", "\n       "), flush=True)  # (what it said: to write under it)
            except Exception as e:
                report.update(ok=False, failed=where, lang=lang, error=str(e)[-2500:] + logs(work), code=code[:600])
                break
    except Exception as e:
        report.update(ok=False, error=str(e)[-2000:])
    finally:
        if py:
            py.stdin.close()
            py.wait(timeout=30)
        stop_node(node)
        shutil.rmtree(work, ignore_errors=True)
    return report


def logs(work):
    """The ends of the logs a page's examples wrote (its own nodes'), to see why one failed."""
    found = sorted(os.path.join(d, f) for d, _, fs in os.walk(work) for f in fs if f.endswith(".log") and f != "node.log")
    return "".join(f"\n--- {os.path.relpath(p, work)}:\n" + open(p, errors="replace").read()[-1200:] for p in found)[-6000:]


WAYS_JSON = os.path.join(ROOT, "site", "src", "ways.json")
WAYS_PREFIX = {  # (what a Python or JavaScript way needs first: its own connection)
    "python": 'import pondra\ndb = pondra.connect("http://localhost:8080")\n',
    "js": 'import { connect } from "pondra";\nconst db = connect("http://localhost:8080");\n',
}


def ways_check(verbose):
    """Every way in site/src/ways.json runs its query and its statement on a fresh node. The Ways component
    shows these templates, so a way that breaks breaks every page that shows one. The filled templates go
    into a page of their own, which check_page runs with the same runners (and the same node) as any page."""
    spec = json.load(open(WAYS_JSON, encoding="utf-8"))
    lines, spots = [], {}  # (the page's lines; each fence's first line -> the way and its kind)
    for way in spec["ways"]:
        samples = [("query", way["query"], "SELECT 1 AS one"),
                   ("statement", way["statement"], f"CREATE TABLE ways_check_{way['label'].lower()} (id BIGINT)")]
        for kind, template, sample in samples:
            code = WAYS_PREFIX.get(way["lang"], "") + template.replace("{sql}", sample)  # (check_page moves the ports)
            spots[len(lines) + 1] = (way["label"], kind)
            lines += [f"```{way['lang']}", *code.split("\n"), "```", ""]
    folder = tempfile.mkdtemp(prefix="pondra-ways-")
    try:
        path = os.path.join(folder, "ways.md")
        open(path, "w", encoding="utf-8").write("\n".join(lines))
        r = check_page(path, verbose)
    finally:
        shutil.rmtree(folder, ignore_errors=True)
    if r["ok"]:
        return {"page": WAYS_JSON, "ran": r.get("ran", 0), "ok": True}
    if "failed" in r:
        label, kind = spots[int(r["failed"].rsplit(":", 1)[1])]
        error = f"the {label} way's {kind} ({r['lang']}): {r['error']}"
    else:
        error = r.get("error", "")
    return {"page": WAYS_JSON, "ran": r.get("ran", 0), "ok": False, "error": error}


GLOSSARY_MD = os.path.join(ROOT, "docs", "glossary.md")
GLOSSARY_SITE = os.path.join(ROOT, "site", "src", "content", "docs", "sql", "glossary.mdx")
GLOSSARY_FILES = (GLOSSARY_MD, GLOSSARY_SITE)
GLOSSARY_NOTE = ("Generated from `pondra.kinds` (`src/objects.rs`): don't edit it here; "
                 "`python3 tools/docs_check.py --write-glossary` writes it again.")
GLOSSARY_DESCRIPTION = "Every kind of object, part and pattern Pondra has, what it is, and what other products call it."


def glossary(rows):
    """The glossary, from GET /kinds (`rows`): the markdown for docs/ and the site's MDX, which differ only
    in the site's frontmatter. Each kind, part and pattern is one paragraph, sorted by name; nothing in it
    is typed here, so a kind added to the registry is in it."""
    def sentence(text):
        return text if text.endswith(".") else text + "."
    def others(row):
        return f" Other products call it {', '.join(row['also'])}." if row.get("also") else ""
    def paragraphs(is_, line):
        return [line(r) for r in sorted((r for r in rows if r["is"] == is_), key=lambda r: r["kind"].lower())]
    sections = [
        ("## Objects", paragraphs("object", lambda r: f"**{r['kind']}**: {sentence(r['about'])}{others(r)}")),
        ("## Parts", paragraphs("part", lambda r: f"**{r['kind']}**, inside a {', '.join(r['inside'])}: {sentence(r['about'])}{others(r)}")),
        ("## Patterns", paragraphs("pattern", lambda r: f"**{r['kind']}**: {r['about'].rstrip('.')}; listed by `{r['lists']}`.{others(r)}")),
    ]
    body = "\n\n".join([GLOSSARY_NOTE] + [f"{head}\n\n" + "\n\n".join(paras) for head, paras in sections]) + "\n"
    site = f"---\ntitle: Glossary\ndescription: {GLOSSARY_DESCRIPTION}\n---\n\n{body}"
    return f"# Glossary\n\n{body}", site


def kinds_rows():
    """GET /kinds from a node of its own, on a lake made for the moment (the glossary is written from it)."""
    busy = [n for n, p in PORTS.items() if not free(p)]
    if busy:
        raise RuntimeError(f"ports in use: {busy} (stop what holds them)")
    work = tempfile.mkdtemp(prefix="pondra-glossary-")
    node, _ = start_node(work)
    try:
        if not wait_http(f"http://127.0.0.1:{PORTS['http']}/stats"):
            raise RuntimeError("the node didn't start: " + open(os.path.join(work, "node.log")).read()[-1000:])
        return json.loads(urllib.request.urlopen(f"http://127.0.0.1:{PORTS['http']}/kinds", timeout=30).read())
    finally:
        stop_node(node)
        shutil.rmtree(work, ignore_errors=True)


def glossary_checks():
    """Each glossary file as a page's report: out of date (or missing) when it isn't what GET /kinds says now."""
    reports = []
    for path, text in zip(GLOSSARY_FILES, glossary(kinds_rows())):
        ok = os.path.exists(path) and open(path, encoding="utf-8").read() == text
        reports.append({"page": path, "ran": 0, "ok": ok, **({} if ok else {"error": "out of date: python3 tools/docs_check.py --write-glossary"})})
    return reports


# What the references must name (`--coverage`, and with every page): everything a user can call,
# run or set, read from the code and the binary's own --help, so a new route, flag, method or
# variable fails the check until its reference page names it. A name left out on purpose is in
# UNLISTED with its reason, one by one, never a pattern.
DOCS = os.path.join(ROOT, "site", "src", "content", "docs")
UNLISTED = {
    "PONDRA_TEST_FORMAT": "a build posing as a later lake format, for upgrade_check.py",
    "PONDRA_TEST_PANICS": "turns on pondra_panic(), for harness.py safety",
    "PONDRA_CRASH": "injects crashes at named points, for harness.py crash",
    "PONDRA_TEST_NO_GATE": "turns the row locks' gate off, to show the lost update it prevents, for harness.py round7",
    "PONDRA_TEST_AUTOCOMMIT_PAUSE_MS": "holds an autocommitted write between its rows and its append, for harness.py round7 and autocommit",
    "PONDRA_DROP_BEATS": "drops heartbeats from the nodes a file names, for cluster.py isolate",
    "PONDRA_NO_DIRECT": "acts as if the leader were out of reach, for harness.py serverless",
    "load_ipython_extension": "IPython calls it for `%load_ext pondra`, which the Python reference shows",
    "expr": "frame.py's own helper (an Expr from a value or SQL), not one of Polars' names",
    "sql_type": "frame.py's own helper (a SQL type from a dtype), which client.py uses",
    "trailing_order": "frame.py's own helper (a query's last ORDER BY), which client.py uses",
}


def _text(*pages):
    return "\n".join(open(os.path.join(DOCS, p), encoding="utf-8").read() for p in pages)


def _shape(path):
    """A route with its parameters made alike: `/files/{*path}` and `/files/{path}` are one."""
    return re.sub(r"\{[^}]*\}", "{}", path.rstrip("/") or "/")


def _routes():
    """Every route a node or a folder of lakes answers, but the nodes' own (`/cluster/…`)."""
    out = set()
    for f in ["server.rs", "dbserver.rs", "iceberg.rs", "sharing.rs"]:
        for m in re.finditer(r'\.route\(\s*"([^"]+)"', open(os.path.join(ROOT, "src", f), encoding="utf-8").read()):
            if not m.group(1).startswith("/cluster/"):
                out.add(m.group(1))
    return out


def _commands(args=()):
    """`pondra`'s commands and their flags, from --help, every level down: {"pondra service install": {"--lake", …}}."""
    text = subprocess.run([BIN, *args, "--help"], capture_output=True, text=True, timeout=30).stdout
    name = " ".join(["pondra", *args])
    out = {name: {f for f in re.findall(r"(?<![\w-])(--[a-z][a-z0-9-]*)", text.split("Options:")[-1]) if f not in ("--help", "--version")}}
    if "Commands:" in text:
        for c in re.findall(r"^\s{2}([a-z][a-z-]*)\s", text.split("Commands:")[1].split("\n\n")[0], re.M):
            if c != "help":
                out.update(_commands((*args, c)))
    return out


def _public(path, classes):
    """A Python file's public functions, and the public methods of the classes named."""
    import ast
    tree = ast.parse(open(os.path.join(ROOT, path), encoding="utf-8").read())
    out = {n.name for n in tree.body if isinstance(n, ast.FunctionDef) and not n.name.startswith("_")}
    for n in tree.body:
        if isinstance(n, ast.ClassDef) and n.name in classes:
            out |= {m.name for m in n.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and not m.name.startswith("_")}
    return out


def _variables():
    """Every PONDRA_* variable the node or a client reads: the name as a string of its own."""
    out = set()
    for d in ["src", "python/pondra", "js"]:
        for base, _, files in os.walk(os.path.join(ROOT, d)):
            for f in files:
                if f.endswith((".rs", ".py", ".js")) and "node_modules" not in base:
                    text = open(os.path.join(base, f), encoding="utf-8", errors="replace").read()
                    out |= set(re.findall(r"""["'](PONDRA_[A-Z0-9_]+)["']""", text)) | set(re.findall(r"process\.env\.(PONDRA_[A-Z0-9_]+)", text))
    return out


def coverage():
    """Each reference as a page's report: ok when it names everything its door has."""
    word = lambda name, text: re.search(rf"(?<![\w-]){re.escape(name)}(?![\w-])", text)
    http = _text("reference/http.mdx", "reference/iceberg-rest.mdx")
    named = {_shape(m.group(0)) for m in re.finditer(r"/[A-Za-z0-9_{}*.\-/]*", http)}
    cli = _text("reference/cli.mdx")
    commands = _commands() if os.path.exists(BIN) else {}
    checks = {
        "reference/http.mdx": sorted(r for r in _routes() if _shape(r) not in named),
        "reference/cli.mdx": sorted(c for c in commands if c not in cli) + sorted({f for fs in commands.values() for f in fs if not word(f, cli)}),
        "reference/python.mdx": sorted(n for n in _public("python/pondra/client.py", {"Pondra", "Result", "Run", "Vars"}) | _public("python/pondra/__init__.py", ()) if not word(n, _text("reference/python.mdx"))),
        "reference/frames.mdx": sorted(n for n in _public("python/pondra/frame.py", {"Frame", "Expr", "_Str", "_Dt", "GroupBy", "Merge"}) if not word(n, _text("reference/frames.mdx"))),
        "reference/javascript.mdx": sorted(n for n in set(re.findall(r"^\s{2}(?:async\s+)?\*?([a-zA-Z]\w*)\s*\(", open(os.path.join(ROOT, "js", "index.js"), encoding="utf-8").read(), re.M)) - {"constructor", "if", "for", "while", "switch", "catch"} if not word(n, _text("reference/javascript.mdx"))),
        "reference/environment.mdx": sorted(v for v in _variables() if v not in _text("reference/environment.mdx")),
    }
    checks = {p: [n for n in missing if n not in UNLISTED] for p, missing in checks.items()}
    return [{"page": os.path.join(DOCS, p), "ran": 0, "ok": not missing, **({} if not missing else {"error": "doesn't name: " + ", ".join(missing)})} for p, missing in checks.items()]


def slug(heading):
    """A heading's anchor as Starlight makes it (github-slugger): lower case, punctuation gone, spaces as -."""
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", heading).strip().lower()
    return re.sub(r"[^\w\- ]", "", text).replace(" ", "-")


def anchors(path):
    """Every anchor a page has: its headings' slugs (a repeated one numbered, as github-slugger does)."""
    seen, out, fenced = {}, set(), False
    for line in open(path, encoding="utf-8").read().split("\n"):
        if FENCE.match(line):
            fenced = not fenced
        elif not fenced and (m := re.match(r"^#{1,6}\s+(.*?)\s*#*\s*$", line)):
            base = slug(m.group(1))
            n = seen.get(base, 0)
            seen[base] = n + 1
            out.add(base if n == 0 else f"{base}-{n}")
    return out


def links():
    """Each page as a report: ok when every link to another page of the site lands on a page (or a
    redirect) and, when it names one, a heading there. A page moved or a section renamed fails here."""
    config = open(os.path.join(ROOT, "site", "astro.config.mjs"), encoding="utf-8").read()
    redirects = set(re.findall(r"^\s*'(/[^']+)':\s*'/pondra/", config, re.M))
    found, out = {}, []
    def target(page):
        if page not in found:
            hit = [os.path.join(DOCS, page + ext) for ext in (".mdx", ".md", "/index.mdx", "/index.md")]
            found[page] = next((h for h in hit if os.path.exists(h)), None)
        return found[page]
    for d, _, fs in os.walk(DOCS):
        for f in sorted(fs):
            if not f.endswith((".md", ".mdx")):
                continue
            path, bad = os.path.join(d, f), []
            text = open(path, encoding="utf-8").read()
            for link in re.findall(r"""(?:\]\(|href=["'])/pondra/([^)"'\s]*)""", text):
                page, _, anchor = link.partition("#")
                page = page.strip("/")
                if not page or page.startswith(("assets", "_astro")) or "." in page.split("/")[-1]:
                    continue
                to = target(page)
                if to is None:
                    if "/" + page not in redirects:
                        bad.append(f"/pondra/{link} (no such page)")
                elif anchor and anchor not in anchors(to):
                    bad.append(f"/pondra/{link} (no such heading)")
            out.append({"page": path, "ran": 0, "ok": not bad, **({"error": "\n".join(sorted(set(bad)))} if bad else {})})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="*", help="pages or folders to check (default: every page of the site, and the glossary)")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--offset", type=int, default=0, help="move every port by this much (and the examples' too), to run beside another check")
    ap.add_argument("--write-glossary", action="store_true", help="write docs/glossary.md and the site's glossary from GET /kinds, and stop")
    ap.add_argument("--coverage", action="store_true", help="only check that the references name everything (also run with every page)")
    ap.add_argument("--links", action="store_true", help="only check that every link between pages lands on a page and a heading (also run with every page)")
    a = ap.parse_args()
    for name in PORTS:
        PORTS[name] = DOC_PORTS[name] + a.offset
    if PORTS["http"] in FETCH_REFUSES:
        sys.exit(f"--offset {a.offset} puts HTTP on {PORTS['http']}, a port Node's fetch refuses: pick another offset")
    if a.write_glossary:
        for path, text in zip(GLOSSARY_FILES, glossary(kinds_rows())):
            open(path, "w", encoding="utf-8").write(text)
            print(f"wrote {os.path.relpath(path, ROOT)}", flush=True)
        sys.exit(0)
    everything = not a.paths and not a.coverage and not a.links
    if everything:
        a.paths = [os.path.join(ROOT, "site", "src", "content", "docs")]
    pages = []
    for p in a.paths:
        if os.path.isdir(p):
            pages += sorted(os.path.join(d, f) for d, _, fs in os.walk(p) for f in fs if f.endswith((".md", ".mdx")))
        else:
            pages.append(p)
    results = []
    for page in pages:
        r = check_page(page, a.verbose)
        results.append(r)
        mark = "ok  " if r["ok"] else "FAIL"
        print(f"{mark} {os.path.relpath(page, ROOT)} ({r.get('ran', 0)} examples)", flush=True)
        if not r["ok"]:
            print(f"     at {r.get('failed', '?')} ({r.get('lang', '')}):\n{r.get('error', '')}\n", flush=True)
    if everything or any("<Ways" in open(p, encoding="utf-8").read() for p in pages):
        r = ways_check(a.verbose)
        results.append(r)
        print(f"{'ok  ' if r['ok'] else 'FAIL'} {os.path.relpath(WAYS_JSON, ROOT)} (every way runs)", flush=True)
        if not r["ok"]:
            print(f"     {r['error']}\n", flush=True)
    if everything or any(os.path.abspath(p) in GLOSSARY_FILES for p in a.paths):
        for r in glossary_checks():
            results.append(r)
            print(f"{'ok  ' if r['ok'] else 'FAIL'} {os.path.relpath(r['page'], ROOT)} (generated from GET /kinds)", flush=True)
            if not r["ok"]:
                print(f"     {r['error']}\n", flush=True)
    if everything or a.coverage:
        for r in coverage():
            results.append(r)
            print(f"{'ok  ' if r['ok'] else 'FAIL'} {os.path.relpath(r['page'], ROOT)} (names everything its door has)", flush=True)
            if not r["ok"]:
                print(f"     {r['error']}\n", flush=True)
    if everything or a.links:
        for r in links():
            results.append(r)
            if not r["ok"]:
                print(f"FAIL {os.path.relpath(r['page'], ROOT)} (links)\n     {r['error']}\n", flush=True)
    bad = [r for r in results if not r["ok"]]
    print(json.dumps({"pages": len(results), "examples": sum(r.get("ran", 0) for r in results), "failed": [os.path.relpath(r["page"], ROOT) for r in bad], "ok": not bad}))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
