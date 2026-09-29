#!/usr/bin/env python3
"""What `pip install pondra` gives, tried: the binary is found, a node starts on a new lake, and a
table, an exactly-once append, a view, a query, a frame, a Python procedure and function work. Run where the
wheel is installed (CI, and containers with old and new Linux).

Without pyarrow (`pip install pondra` alone), what needs none: rows, one value and a text table
as JSON and text; SQL procedures; `python -m pondra` (the binary wherever pip put it) and
`python -m pondra --add-to-path` (with a home of its own; on Windows only in CI: it sets your PATH)."""
import importlib.metadata, importlib.util, os, subprocess, sys, tempfile
import pondra

# The package's page on PyPI: its own short README (install, an example, what it does, the docs).
about = importlib.metadata.metadata("pondra")
about = about.get("Description") or about.get_payload() or ""
assert "pip install pondra" in about and "alimardon123.github.io/pondra" in about, "the package's description isn't its page"

arrow = importlib.util.find_spec("pyarrow") is not None
lake = os.path.join(tempfile.mkdtemp(prefix="pondra-check-"), "lake")
version = subprocess.run([pondra.binary(), "--version"], capture_output=True, text=True).stdout.strip()
print(version, "(with pyarrow)" if arrow else "(no pyarrow)")
with pondra.local(lake, python=arrow) as db:
    db.sql("CREATE TABLE t (id BIGINT, v VARCHAR)")
    db.view("per_v", "SELECT v, count(*) AS n FROM t GROUP BY v", materialized=True)  # (kept up to date)
    db.append("t", [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "a"}])
    rows = db.sql("SELECT v, n FROM per_v ORDER BY v").rows()
    assert rows == [{"v": "a", "n": 2}, {"v": "b", "n": 1}], rows
    assert db.view("recent", "SELECT * FROM t WHERE id > 1").select(pondra.len()).item() == 2  # (a stored query)
    # a frame, and SQL naming it
    frame = db.table("t").group_by("v").agg(pondra.len().alias("n")).sort("v")
    assert db.sql("SELECT * FROM frame WHERE n > $k", k=1).rows() == [{"v": "a", "n": 2}]
    shown = frame._repr_html_()  # (what a notebook shows; `all` in frame.py is Polars' all(), not Python's)
    assert shown and "<" in shown and "a" in shown, shown
    db.sql("CREATE PROCEDURE count_v(v VARCHAR) LANGUAGE sql AS $$ SELECT count(*) AS n FROM t WHERE v = $v $$")
    assert db.call("count_v", "b").rows() == [{"n": 1}]
    if arrow:  # a Python procedure (it runs in this very Python, beside the node)
        @db.procedure
        def count_py(con, v: str = "a"):
            return con.table("t").filter(pondra.col("v") == v).select(pondra.len().alias("n"))

        assert db.call("count_py", "b").rows() == [{"n": 1}] and db.sql("CALL count_py()").rows() == [{"n": 2}]

        @db.function  # (a Python function, run by the node's workers: the package's pondra.worker)
        def shout(v: str) -> str:
            return v.upper() + "!"

        assert db.sql("SELECT shout(v) AS s FROM t ORDER BY s LIMIT 1").rows() == [{"s": "A!"}]
    else:  # rows as JSON (a null is None), one value, a text table; tables say what they need
        assert db.sql("SELECT 1 AS a, NULL AS b UNION ALL SELECT 2, 'x' ORDER BY a").rows() == [{"a": 1, "b": None}, {"a": 2, "b": "x"}]
        assert db.table("t").select(pondra.len()).item() == 3
        frame.show()
        try:
            frame.collect()
            raise AssertionError("collect() without pyarrow")
        except ImportError as e:
            assert "pip install pyarrow" in str(e), e

# `python -m pondra`: the binary, from wherever pip put it
by_module = subprocess.run([sys.executable, "-m", "pondra", "--version"], capture_output=True, text=True)
assert by_module.returncode == 0 and by_module.stdout.strip() == version, by_module
# `--add-to-path`: once, then "already" (a home of its own; Windows's registry only in CI, and put
# back as it was: this check runs twice, without pyarrow and with it, on the same machine)
if os.name != "nt" or os.environ.get("CI"):
    env = {**os.environ, "HOME": tempfile.mkdtemp(prefix="pondra-home-"), "SHELL": "/bin/bash"}
    folder = os.path.dirname(pondra.binary())
    if os.name == "nt":
        import winreg
        key = lambda: winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment", 0, winreg.KEY_READ | winreg.KEY_WRITE)
        with key() as k:
            before = winreg.QueryValueEx(k, "Path")
    try:
        said = [subprocess.run([sys.executable, "-m", "pondra", "--add-to-path"], env=env, capture_output=True, text=True, check=True).stdout for _ in range(2)]
        assert "on your PATH now" in said[0] and "already" in said[1], said
        if os.name == "nt":
            with key() as k:
                assert folder.lower() in winreg.QueryValueEx(k, "Path")[0].lower(), "not in the user's Path"
        else:
            rc = os.path.join(env["HOME"], ".bash_profile" if sys.platform == "darwin" else ".bashrc")
            assert open(rc).read().count(folder) == 1, rc
    finally:
        if os.name == "nt":
            with key() as k:
                winreg.SetValueEx(k, "Path", 0, before[1], before[0])
print("ok:", rows)
