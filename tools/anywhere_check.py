#!/usr/bin/env python3
"""Pondra where a user first meets it: the shell, a notebook, npm, and Linux as old as 2014.

  anywhere_check.py --bin target/x86_64-unknown-linux-gnu/dist/pondra --dist dist/ [--docker]

What this proves:

- `pondra <lake>` with statements piped in creates, writes and queries a lake, and prints tables;
  a second shell on the same lake starts at once: the first node handed the lake on as it left
  (`--stop-with-stdin`), where a killed one leaves a lease to wait out; and a proxy named in the
  environment doesn't come between the shell or Python and their own node;
- `pondra.local()` from Python starts a node and stops it with `close()`; and when the Python
  process is killed outright, the node notices its input close and stops too, handing the lake on;
- the wheel installs and works in a fresh virtualenv, and the npm packages in a fresh project;
- examples/quickstart.ipynb runs top to bottom on the wheel, from its own `%pip install` cell;
- with `--docker`: the binary runs on glibc 2.17 (CentOS 7, manylinux2014) and on Ubuntu 22.04,
  and the wheel works in both.
"""
import argparse, glob, json, os, shutil, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=600, **kw)


def shell_checks(work):
    lake = os.path.join(work, "lake")
    script = "CREATE TABLE t (id BIGINT, v VARCHAR);\nINSERT INTO t VALUES (1, 'a'), (2, 'b');\nSELECT v, count(*) AS n\n  FROM t GROUP BY v ORDER BY v;\n"
    t = time.time()
    first = run([A.bin, lake], input=script)
    first_s = time.time() - t
    t = time.time()
    again = run([A.bin, lake], input="INSERT INTO t VALUES (3, 'c');\nSELECT count(*) AS n FROM t;\n")  # (a write: it needs a leader)
    again_s = time.time() - t
    # ; inside strings, quoted names and comments doesn't end a statement; two on a line, a
    # comment after one, and a last one with no ; all run; an error doesn't end the session
    tricky = "INSERT INTO t VALUES (4, 'x;\ny'), (5, 'it''s; -- not a comment');  -- a comment; with a ;\n/* ; */ SELECT count(*) AS \"n;\" FROM t; SELECT nope;\nSELECT max(id) AS m\n  FROM t"
    third = run([A.bin, lake], input=tricky)
    return {
        "a piped shell writes and queries a lake": first.returncode == 0 and "| a | 1 |" in first.stdout and "| b | 1 |" in first.stdout,
        "a second shell on the same lake starts at once (the first handed the lake on)": again.returncode == 0 and "| 3 " in again.stdout and again_s < 8,
        "statements end at a ; outside strings and comments; errors don't end the session": third.returncode == 0 and "| n; |" in third.stdout and "| 5 " in third.stdout
                                                                                           and "Error:" in third.stderr and "| m |" in third.stdout,
    }, {"first_shell_s": round(first_s, 2), "second_shell_s": round(again_s, 2), "stderr": (first.stderr + again.stderr + third.stderr)[-400:], "third": third.stdout[-400:]}


def python_checks(work, python):
    lake = os.path.join(work, "pylake")
    env = {**os.environ, "PONDRA_BIN": A.bin, "PYTHONPATH": os.path.join(HERE, "..", "python")}  # (the client from the source tree)
    code = f"""
import pondra, os, sys
db = pondra.local({lake!r})
db.sql("CREATE TABLE t (id BIGINT)")
db.append("t", [{{"id": 1}}])
print(db.process.pid, flush=True)
if sys.argv[1] == "close":
    print(db.sql("SELECT count(*) AS n FROM t").rows(), flush=True)
    db.close()
else:
    os.kill(os.getpid(), 9)  # killed outright: no atexit, no close()
"""
    closed = run([python, "-c", code, "close"], env=env)
    p = subprocess.Popen([python, "-c", code, "kill"], env=env, stdout=subprocess.PIPE, text=True)
    node = int(p.stdout.readline())
    p.wait()
    deadline = time.time() + 15
    while time.time() < deadline and os.path.exists(f"/proc/{node}") and "Z" not in open(f"/proc/{node}/stat").read().split()[2]:
        time.sleep(0.1)
    node_gone = not os.path.exists(f"/proc/{node}") or "Z" in open(f"/proc/{node}/stat").read().split()[2]
    t = time.time()
    reopened = run([python, "-c", f"import pondra; db = pondra.local({lake!r}); db.append('t', [{{'id': 3}}]); print(db.sql('SELECT count(*) AS n FROM t').rows()); db.close()"], env=env)
    return {
        "pondra.local() starts a node, close() stops it": closed.returncode == 0 and "[{'n': 1}]" in closed.stdout,
        "when Python is killed, its node stops too": node_gone,
        "the lake reopens at once after that, for writes too": reopened.returncode == 0 and "[{'n': 3}]" in reopened.stdout and time.time() - t < 10,
    }, {"reopen_s": round(time.time() - t, 2), "stderr": (closed.stderr + reopened.stderr)[-400:]}


def proxy_checks(work):
    """Behind a proxy named in the environment (as in many companies' notebooks), the shell and
    `pondra.local()` still reach their own node directly: the proxy can't reach it."""
    dead = "http://127.0.0.1:9"  # (nothing listens there)
    env = {k: v for k, v in os.environ.items() if k.lower() != "no_proxy"}
    env.update({k: dead for k in ("HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy")}, PYTHONPATH=os.path.join(HERE, "..", "python"), PONDRA_BIN=A.bin)
    shell = run([A.bin, os.path.join(work, "proxied")], input="SELECT 41 + 1 AS n;\n", env=env)
    python = run([sys.executable, "-c", f"import pondra; db = pondra.local({os.path.join(work, 'proxied-py')!r}); print(db.sql('SELECT 41 + 1 AS n').rows()); db.close()"], env=env)
    return {"with a proxy in the environment, the shell and pondra.local() reach their node directly":
            shell.returncode == 0 and "| 42 |" in shell.stdout and python.returncode == 0 and "[{'n': 42}]" in python.stdout,
            }, {"shell": (shell.stdout + shell.stderr)[-300:], "python": (python.stdout + python.stderr)[-300:]}


def package_checks(work):
    wheel = glob.glob(os.path.join(A.dist, "pondra-*-manylinux*.whl"))[0]
    venv = os.path.join(work, "venv")
    run([sys.executable, "-m", "venv", venv])
    py = os.path.join(venv, "bin", "python")
    pip = run([py, "-m", "pip", "install", "-q", wheel, "pyarrow"])
    check = run([py, os.path.join(HERE, "package_check.py")], env={k: v for k, v in os.environ.items() if k != "PONDRA_BIN"})
    npm_dir = os.path.join(work, "npm")
    os.makedirs(npm_dir)
    run(["npm", "init", "-y"], cwd=npm_dir)
    tarballs = [os.path.abspath(p) for p in glob.glob(os.path.join(A.dist, "pondra-linux-x64-*.tgz")) + glob.glob(os.path.join(A.dist, "pondra-[0-9]*.tgz"))]
    npm = run(["npm", "install", "--no-audit", "--no-fund", *tarballs], cwd=npm_dir)
    shutil.copy(os.path.join(HERE, "package_check.mjs"), npm_dir)  # (it imports "pondra" from where it is)
    node = run(["node", "package_check.mjs"], cwd=npm_dir, env={k: v for k, v in os.environ.items() if k != "PONDRA_BIN"})
    notebook = notebook_check(py, wheel, work)
    return {
        "the wheel works in a fresh virtualenv": pip.returncode == 0 and check.returncode == 0 and "ok:" in check.stdout,
        "the npm packages work in a fresh project": npm.returncode == 0 and node.returncode == 0 and "ok:" in node.stdout,
        "the quick-start notebook runs, top to bottom, on the wheel": notebook.returncode == 0 and "ok:" in notebook.stdout,
    }, {"wheel": os.path.basename(wheel), "pip": pip.stderr[-300:], "check": (check.stdout + check.stderr)[-300:], "npm": (npm.stderr + node.stdout + node.stderr)[-400:],
        "notebook": (notebook.stdout + notebook.stderr)[-400:]}


def notebook_check(py, wheel, work):
    """examples/quickstart.ipynb, run as a notebook user would, from its own `%pip install` on:
    no cell may fail, and the last join must price order 5000 at the later tea price."""
    run([py, "-m", "pip", "install", "-q", "nbclient", "nbformat", "ipykernel", "pandas"])
    folder = os.path.join(work, "notebook")
    os.makedirs(folder)
    code = f"""
import nbformat, nbclient
nb = nbformat.read({os.path.join(HERE, "..", "examples", "quickstart.ipynb")!r}, as_version=4)
nbclient.NotebookClient(nb, timeout=300, kernel_name="python3", resources={{"metadata": {{"path": {folder!r}}}}}).execute()
text = "".join(o.get("data", {{}}).get("text/plain", "") for c in nb.cells if c.cell_type == "code" for o in c.outputs)
assert "5000   tea" in text and "35.0" in text, text[-600:]
print("ok:", len(nb.cells), "cells")
"""
    return run([py, "-c", code], env={**{k: v for k, v in os.environ.items() if k != "PONDRA_BIN"}, "PONDRA_WHEEL": wheel})


def docker_checks():
    """The binary on old and current Linux: its version, and a shell session on a new lake; on the
    oldest, the wheel too (that image has Python)."""
    dist, ok, out = os.path.abspath(A.dist), {}, {}
    session = "CREATE TABLE t (x BIGINT);\nINSERT INTO t VALUES (41), (1);\nSELECT sum(x) AS s FROM t;\n"
    for name, image, py in [("glibc 2.17 (CentOS 7)", "quay.io/pypa/manylinux2014_x86_64", "/opt/python/cp311-cp311/bin/python"), ("Ubuntu 22.04", "ubuntu:22.04", None)]:
        mounts = ["-v", f"{A.bin}:/usr/local/bin/pondra:ro", "-v", f"{dist}:/dist:ro", "-v", f"{HERE}:/tools:ro"]
        shell = run(["docker", "run", "--rm", "-i", *mounts, image, "bash", "-c", "pondra --version && pondra /tmp/lake"], input=session)
        ok[f"the binary runs on {name}: a shell session on a new lake"] = shell.returncode == 0 and "| 42 |" in shell.stdout
        out[name] = {"shell": (shell.stdout + shell.stderr).strip()[-300:]}
        if py:
            net = ["--network", "host", "-e", "HTTPS_PROXY", "-e", "PIP_CERT=/ca.crt", "-v", f"{os.environ.get('SSL_CERT_FILE', '/root/.ccr/ca-bundle.crt')}:/ca.crt:ro"]
            wheel = run(["docker", "run", "--rm", *net, *mounts, image, "bash", "-c", f"{py} -m pip install -q --only-binary=:all: /dist/pondra-*-manylinux*.whl 'pyarrow<19' && {py} /tools/package_check.py"])
            ok[f"the wheel works on {name}"] = wheel.returncode == 0 and "ok:" in wheel.stdout
            out[name]["wheel"] = (wheel.stdout + wheel.stderr).strip()[-300:]
    return ok, out


def main():
    work = tempfile.mkdtemp(prefix="pondra-anywhere-")
    checks, details = {}, {}
    for name, f in [("shell", lambda: shell_checks(work)), ("python", lambda: python_checks(work, sys.executable)), ("proxy", lambda: proxy_checks(work)),
                    ("packages", lambda: package_checks(work))] + ([("docker", docker_checks)] if A.docker else []):
        c, d = f()
        checks.update(c)
        details[name] = d
    result = {"binary": A.bin, "checks": checks, "details": details, "ok": all(checks.values())}
    print(json.dumps(result, indent=1))
    run(["rm", "-rf", work])
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True, help="the pondra binary to try")
    ap.add_argument("--dist", default="dist", help="where tools/package.py put the wheel and npm packages")
    ap.add_argument("--docker", action="store_true")
    A = ap.parse_args()
    A.bin = os.path.abspath(A.bin)
    main()
