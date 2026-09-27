#!/usr/bin/env python3
"""What `pip install pondra` gives, tried: the binary is found, a node starts on a new lake, and a
table, an exactly-once append, a view, a query, a frame and a Python procedure work. Run where the wheel is installed (CI, and
containers with old and new Linux)."""
import os, subprocess, tempfile
import pondra

lake = os.path.join(tempfile.mkdtemp(prefix="pondra-check-"), "lake")
print(subprocess.run([pondra.binary(), "--version"], capture_output=True, text=True).stdout.strip())
with pondra.local(lake) as db:
    db.sql("CREATE TABLE t (id BIGINT, v VARCHAR)")
    db.view("per_v", "SELECT v, count(*) AS n FROM t GROUP BY v")
    db.append("t", [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "a"}])
    rows = db.sql("SELECT v, n FROM per_v ORDER BY v").rows()
    assert rows == [{"v": "a", "n": 2}, {"v": "b", "n": 1}], rows
    # a frame, SQL naming it, and a Python procedure (it runs in this very Python, beside the node)
    frame = db.table("t").group_by("v").agg(pondra.len().alias("n")).sort("v")
    assert db.sql("SELECT * FROM frame WHERE n > $k", k=1).rows() == [{"v": "a", "n": 2}]

    @db.procedure
    def count_v(con, v: str = "a"):
        return con.table("t").filter(pondra.col("v") == v).select(pondra.len().alias("n"))

    assert db.call("count_v", "b").rows() == [{"n": 1}] and db.sql("CALL count_v()").rows() == [{"n": 2}]
print("ok:", rows)
