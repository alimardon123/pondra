#!/usr/bin/env python3
"""Every example in the documentation website runs (ADR-030): each page's code blocks, in order,
against a fresh node, so the docs can't fall behind the code.

  python3 tools/docs_check.py                      # every page under site/src/content/docs
  python3 tools/docs_check.py site/src/content/docs/guides/streaming.mdx

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
"""
import argparse, json, os, re, shutil, socket, subprocess, sys, tempfile, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BIN = os.environ.get("PONDRA_BIN") or os.path.join(ROOT, "target", "release", "pondra")
DOC_PORTS = {"http": 8080, "pg": 5432, "kafka": 9092, "flight": 8815}  # (what the docs say)
PORTS = dict(DOC_PORTS)  # (where this run's node listens: --offset moves them, for runs side by side)
RUN = {"sql", "python", "py", "js", "javascript", "bash", "sh", "shell"}
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


def blocks(text):
    """The page's frontmatter (a dict) and its fenced code blocks: (language, info, code, line)."""
    front = {}
    if text.startswith("---\n"):
        end = text.index("\n---", 4)
        import yaml
        front = yaml.safe_load(text[4:end]) or {}
    out, lines, i = [], text.split("\n"), 0
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
        i = j + 1
    return front, out


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
    env = {**os.environ, "PATH": os.path.dirname(BIN) + os.pathsep + os.environ.get("PATH", ""), "PONDRA_URL": f"http://127.0.0.1:{PORTS['http']}",
           "PYTHONPATH": os.path.join(ROOT, "python") + os.pathsep + os.environ.get("PYTHONPATH", ""), "PONDRA_BIN": BIN}
    node = subprocess.Popen([BIN, "serve", "--dir", os.path.join(work, "lake"), "--addr", f"127.0.0.1:{PORTS['http']}", "--pg", f"127.0.0.1:{PORTS['pg']}",
                             "--kafka", f"127.0.0.1:{PORTS['kafka']}", "--flight", f"127.0.0.1:{PORTS['flight']}", "--python", "auto", "--stop-with-stdin", "--tier-secs", "1"],
                            cwd=work, env=env, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=open(os.path.join(work, "node.log"), "w"))
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
                    while answer is None:  # (a library writing to the real stdout, as DuckDB's progress bar does, isn't the answer)
                        line = py.stdout.readline()
                        try:
                            got = json.loads(line or '{"ok": false, "error": "python exited"}')
                        except json.JSONDecodeError:
                            continue
                        answer = got if isinstance(got, dict) and "ok" in got else None
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
        node.stdin.close()
        try:
            node.wait(timeout=20)
        except subprocess.TimeoutExpired:
            node.kill()
        shutil.rmtree(work, ignore_errors=True)
    return report


def logs(work):
    """The ends of the logs a page's examples wrote (its own nodes'), to see why one failed."""
    found = sorted(os.path.join(d, f) for d, _, fs in os.walk(work) for f in fs if f.endswith(".log") and f != "node.log")
    return "".join(f"\n--- {os.path.relpath(p, work)}:\n" + open(p, errors="replace").read()[-1200:] for p in found)[-6000:]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="*", default=[os.path.join(ROOT, "site", "src", "content", "docs")])
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--offset", type=int, default=0, help="move every port by this much (and the examples' too), to run beside another check")
    a = ap.parse_args()
    for name in PORTS:
        PORTS[name] = DOC_PORTS[name] + a.offset
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
    bad = [r for r in results if not r["ok"]]
    print(json.dumps({"pages": len(results), "examples": sum(r.get("ran", 0) for r in results), "failed": [os.path.relpath(r["page"], ROOT) for r in bad], "ok": not bad}))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
