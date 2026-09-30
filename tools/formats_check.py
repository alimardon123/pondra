#!/usr/bin/env python3
"""Other engines' tables, read by Pondra (ADR-026): Delta and Iceberg tables as Spark 4 (Delta
4.0, Iceberg 1.10), delta-rs and PyIceberg write them — deletion vectors, column mapping,
checkpoints (classic, in parts, v2), renamed and dropped columns, partitions of every kind,
position deletes, Iceberg's deletion vectors (v3) and equality deletes, nested and odd types —
read with `delta_scan` / `iceberg_scan` equal to what the writing engine reads itself (and to
DuckDB's readers where they read it), on one node and spread over three; and `version =>`,
`snapshot_from_id`; and a feature Pondra doesn't read refused by name.

  formats_check.py [--spark ~/venv-spark/bin/python] [--port 8860] [--only name,…]

Spark's tables need Java and PySpark with delta-spark (its jars come from Maven the first
time); without `--spark`, only delta-rs's and PyIceberg's tables are checked.
"""
import argparse, datetime as dt, decimal, glob, json, os, shutil, site, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------- Spark's tables (run with --make)

def session(root):
    os.environ["TZ"] = "UTC"
    time.tzset()
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[2]").appName("formats_check")
             .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.ice.type", "hadoop")
             .config("spark.sql.catalog.ice.warehouse", f"{root}/iceberg").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())


def spark_commits(url):
    """Spark (Iceberg 1.10) writes to a Pondra table through Pondra's Iceberg REST catalog
    (`--commit`): SQL's INSERT and DataFrame.writeTo().append() (ADR-028), then a DELETE, an UPDATE
    and a MERGE, copy-on-write, and an ALTER TABLE … ADD COLUMN (ADR-029 phase 2). Prints what Spark
    read back after the appends and after the changes, and what it was told."""
    os.environ["TZ"] = "UTC"
    time.tzset()
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("pondra_commits")
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.pondra", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.pondra.type", "rest")
             .config("spark.sql.catalog.pondra.uri", url).config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("INSERT INTO pondra.default.spark_in VALUES (1, 'a', TIMESTAMP '2026-09-28 10:00:00'), (2, NULL, NULL)")
    spark.createDataFrame([(3, "c", None)], "id BIGINT, name STRING, ts TIMESTAMP").writeTo("pondra.default.spark_in").append()
    read = lambda: [[plain(v) for v in r] for r in spark.sql("SELECT id, name, ts FROM pondra.default.spark_in ORDER BY id").collect()]
    out = {"rows": read()}
    for name, stmt in (("delete", "DELETE FROM pondra.default.spark_in WHERE id = 1"), ("update", "UPDATE pondra.default.spark_in SET name = 'bee' WHERE id = 2"),
                       ("merge", "MERGE INTO pondra.default.spark_in t USING (SELECT 3L AS id, 'sea' AS name UNION ALL SELECT 4L, 'd') s ON t.id = s.id "
                                 "WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name, ts) VALUES (s.id, s.name, NULL)"),
                       ("alter", "ALTER TABLE pondra.default.spark_in ADD COLUMN x INT")):
        try:
            spark.sql(stmt)
            out[name] = ""
        except Exception as e:  # noqa: BLE001 (what Spark was told)
            out[name] = str(e)[:2000]
    out["changed"] = read()
    print(json.dumps(out, default=str))
    spark.stop()


def commits(con, spark):
    """Spark writes to Pondra's tables through its Iceberg REST catalog (`spark_commits`): appends,
    the rows the table's, each with its row id; a DELETE, an UPDATE and a MERGE (copy-on-write),
    after which Pondra reads what Spark does; an ALTER TABLE … ADD COLUMN, Pondra's column too."""
    con.sql("CREATE TABLE spark_in (id BIGINT, name VARCHAR, ts TIMESTAMP) WITH (publish = 'iceberg')")
    run = subprocess.run([spark, os.path.abspath(__file__), "--commit", con.url], capture_output=True, text=True, env={**os.environ, "TZ": "UTC"}, timeout=1200)
    try:
        theirs = json.loads(run.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        theirs = {"rows": [], "changed": [], "delete": "", "update": "", "merge": "", "alter": "", "why": run.stderr[-1500:]}
    ours = con.sql("SELECT id, name, ts, _row_id FROM spark_in ORDER BY id").rows()
    want = [[1, "a", "2026-09-28 10:00:00"], [2, None, None], [3, "c", None]]
    changed = [[2, "bee", None], [3, "sea", None], [4, "d", None]]
    ok = want == theirs["rows"]
    same = [[r["id"], r.get("name"), plain(r.get("ts"))] for r in ours] == changed == theirs["changed"] and all(r.get("_row_id") is not None for r in ours) \
        and not theirs["delete"] and not theirs["update"] and not theirs["merge"]
    added = not theirs["alter"] and "x" in [c["column_name"] for c in con.sql("SELECT column_name FROM information_schema.columns WHERE table_name = 'spark_in'").rows()]
    print(json.dumps({"table": "Spark appends through Pondra's Iceberg REST catalog (INSERT, writeTo().append()): the table's rows, with row ids", "equal": ok, **({} if ok else {"ours": ours, "theirs": theirs})}, default=str), flush=True)
    print(json.dumps({"table": "…Spark's DELETE, UPDATE and MERGE (copy-on-write): Pondra reads what Spark does", "equal": same, **({} if same else {"ours": ours, "theirs": {k: v if isinstance(v, list) else v[:500] for k, v in theirs.items()}})}, default=str), flush=True)
    print(json.dumps({"table": "…an ALTER TABLE … ADD COLUMN from Spark: Pondra's table has the column", "equal": added, **({} if added else {"alter": theirs["alter"][:500]})}), flush=True)
    return {"commits:spark": ok, "commits:changes": same, "commits:altered": added}


def read_spark(tables):
    """What Spark reads of these tables now (`--read`): name -> its columns and rows."""
    spark = session(tempfile.mkdtemp())
    spark.sparkContext.setLogLevel("ERROR")
    print(json.dumps({name: rows(spark.read.format(kind).load(path)) for name, (kind, path) in tables.items()}, default=str))
    spark.stop()


def make_spark(root):
    """Spark writes its tables under `root`, and what it reads back as `root/expected.json`."""
    spark = session(root)
    spark.sparkContext.setLogLevel("ERROR")
    expected = {}

    def delta(name, statements):
        path = f"{root}/delta/{name}"
        for s in statements:
            spark.sql(s.format(t=f"delta.`{path}`", path=path))
        expected[f"delta:{name}"] = {"path": path, **rows(spark.read.format("delta").load(path))}

    def ice(name, statements):
        for s in statements:
            spark.sql(s.format(t=f"ice.db.{name}"))
        expected[f"iceberg:{name}"] = {"path": f"{root}/iceberg/db/{name}", **rows(spark.table(f"ice.db.{name}"))}

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
    values = lambda lo, hi: ", ".join(f"({i}, 'r{i % 4}', {i * 1.25}, DATE'2026-01-{1 + i % 28:02d}', TIMESTAMP'2026-01-01 00:00:00' + INTERVAL {i} MINUTE)" for i in range(lo, hi))
    cols = "id BIGINT, region STRING, amount DOUBLE, day DATE, ts TIMESTAMP"
    # Deletion vectors: DELETE and UPDATE mark rows, files stay; then a compaction.
    delta("dv", [f"CREATE TABLE {{t}} ({cols}) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
                 f"INSERT INTO {{t}} VALUES {values(0, 400)}", f"INSERT INTO {{t}} VALUES {values(400, 800)}",
                 "DELETE FROM {t} WHERE id % 7 = 0", "UPDATE {t} SET amount = -1 WHERE id % 11 = 0", "DELETE FROM {t} WHERE id BETWEEN 100 AND 130"])
    # Column mapping by name: a column renamed, one dropped, one added (old files lack it).
    delta("mapped", [f"CREATE TABLE {{t}} ({cols}) USING delta TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')",
                     f"INSERT INTO {{t}} VALUES {values(0, 300)}", "ALTER TABLE {t} RENAME COLUMN amount TO total", "ALTER TABLE {t} DROP COLUMN ts",
                     "ALTER TABLE {t} ADD COLUMN note STRING", f"INSERT INTO {{t}} (id, region, total, day, note) VALUES (1000, 'rX', 5.5, DATE'2026-02-01', 'late'), (1001, NULL, NULL, NULL, NULL)"])
    # Checkpoints, classic then v2 (with sidecars), with commits after them.
    delta("checkpoints", [f"CREATE TABLE {{t}} ({cols}) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '3')"]
          + [f"INSERT INTO {{t}} VALUES {values(i * 10, i * 10 + 10)}" for i in range(8)] + ["DELETE FROM {t} WHERE id < 15"])
    delta("v2checkpoint", [f"CREATE TABLE {{t}} ({cols}) USING delta TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpointInterval' = '2')"]
          + [f"INSERT INTO {{t}} VALUES {values(i * 10, i * 10 + 10)}" for i in range(5)])
    # Partitions by date and text (a slash, a space, NULL), and nested and odd types.
    delta("partitioned", ["CREATE TABLE {t} (id BIGINT, day DATE, tag STRING, n INT) USING delta PARTITIONED BY (day, tag)",
                          "INSERT INTO {t} VALUES (1, DATE'2026-01-01', 'a/b', 1), (2, DATE'2026-01-01', 'x y', 2), (3, DATE'2026-01-02', NULL, 3), (4, NULL, 'z', 4), (5, DATE'2026-01-02', 'x y', 5)"])
    delta("types", ["CREATE TABLE {t} (id BIGINT, b BOOLEAN, s SMALLINT, y TINYINT, f FLOAT, d DECIMAL(12, 3), ntz TIMESTAMP_NTZ, bin BINARY, s1 STRUCT<a: INT, b: STRING>, arr ARRAY<INT>) USING delta",
                    "INSERT INTO {t} VALUES (1, true, 7, 1, 1.5, 12.345, TIMESTAMP_NTZ'2026-01-01 10:00:00', X'00ff', named_struct('a', 1, 'b', 'x'), array(1, 2)), (2, NULL, NULL, NULL, NULL, -0.001, NULL, NULL, NULL, NULL)"])
    # Iceberg: merge-on-read position deletes (v2), deletion vectors (v3), renamed columns, partitions.
    ice("mor", [f"CREATE TABLE {{t}} ({cols}) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read')",
                f"INSERT INTO {{t}} VALUES {values(0, 400)}", f"INSERT INTO {{t}} VALUES {values(400, 800)}",
                "DELETE FROM {t} WHERE id % 7 = 0", "UPDATE {t} SET amount = -1 WHERE id % 11 = 0"])
    ice("dv", [f"CREATE TABLE {{t}} ({cols}) USING iceberg TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read')",
               f"INSERT INTO {{t}} VALUES {values(0, 500)}", "DELETE FROM {t} WHERE id % 5 = 0", "DELETE FROM {t} WHERE id % 3 = 0"])
    ice("renamed", [f"CREATE TABLE {{t}} ({cols}) USING iceberg", f"INSERT INTO {{t}} VALUES {values(0, 100)}",
                    "ALTER TABLE {t} RENAME COLUMN amount TO total", "ALTER TABLE {t} DROP COLUMN region", "ALTER TABLE {t} ADD COLUMN region STRING",
                    "INSERT INTO {t} (id, total, day, ts, region) VALUES (500, 1.0, DATE'2026-03-01', TIMESTAMP'2026-03-01 00:00:00', 'new')"])
    ice("partitioned", [f"CREATE TABLE {{t}} ({cols}) USING iceberg PARTITIONED BY (days(ts), bucket(4, id), region)", f"INSERT INTO {{t}} VALUES {values(0, 300)}"])
    # A checkpoint in parts (three actions a part), and a column mapped by id.
    spark.conf.set("spark.databricks.delta.checkpoint.partSize", "3")
    delta("parts", [f"CREATE TABLE {{t}} ({cols}) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '4')"] + [f"INSERT INTO {{t}} VALUES {values(i * 5, i * 5 + 5)}" for i in range(6)])
    spark.conf.unset("spark.databricks.delta.checkpoint.partSize")
    delta("by_id", [f"CREATE TABLE {{t}} ({cols}) USING delta TBLPROPERTIES ('delta.columnMapping.mode' = 'id', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')",
                    f"INSERT INTO {{t}} VALUES {values(0, 50)}", "ALTER TABLE {t} RENAME COLUMN region TO area"])
    # Older versions: Delta's `version =>`, Iceberg's snapshot by id.
    path = f"{root}/delta/dv"
    expected["delta:dv@2"] = {"path": path, "options": ", version => 2", **rows(spark.read.format("delta").option("versionAsOf", 2).load(path))}
    first = spark.sql("SELECT snapshot_id FROM ice.db.mor.snapshots ORDER BY committed_at").collect()[1][0]
    expected["iceberg:mor@2"] = {"path": f"{root}/iceberg/db/mor", "options": f", snapshot_from_id => {first}", **rows(spark.read.option("snapshot-id", first).table("ice.db.mor"))}
    # Iceberg's equality deletes (Flink's upserts write them; here, a file of them committed by
    # hand): on (id, region), NULL equal to NULL, only against rows older than the delete.
    ice("equality", [f"CREATE TABLE {{t}} ({cols}) USING iceberg TBLPROPERTIES ('format-version' = '2')", f"INSERT INTO {{t}} VALUES {values(0, 20)}",
                     "INSERT INTO {t} VALUES (5, NULL, 1.0, DATE'2026-01-01', TIMESTAMP'2026-01-01 00:00:00'), (6, NULL, 2.0, DATE'2026-01-01', TIMESTAMP'2026-01-01 00:00:00')"])
    equality_delete(spark, root, "ice.db.equality", [(3, "r3"), (5, None), (9, "nope")])
    spark.sql("INSERT INTO ice.db.equality VALUES (3, 'r3', 9.0, DATE'2026-01-09', TIMESTAMP'2026-01-09 00:00:00')")  # (newer than the delete: kept)
    expected["iceberg:equality"] = {"path": f"{root}/iceberg/db/equality", **rows(spark.table("ice.db.equality"))}
    # Tables for Pondra to INSERT into (and Spark to read back).
    delta("ins_dv", [f"CREATE TABLE {{t}} ({cols}) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')", f"INSERT INTO {{t}} VALUES {values(0, 50)}", "DELETE FROM {t} WHERE id < 5"])
    delta("ins_mapped", [f"CREATE TABLE {{t}} ({cols}) USING delta TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')",
                         f"INSERT INTO {{t}} VALUES {values(0, 20)}", "ALTER TABLE {t} RENAME COLUMN amount TO total"])
    delta("ins_part", ["CREATE TABLE {t} (id BIGINT, day DATE, tag STRING, n INT) USING delta PARTITIONED BY (day, tag)", "INSERT INTO {t} VALUES (1, DATE'2026-01-01', 'a', 1)"])
    ice("ins", [f"CREATE TABLE {{t}} ({cols}) USING iceberg TBLPROPERTIES ('format-version' = '2')", f"INSERT INTO {{t}} VALUES {values(0, 30)}"])
    ice("ins_part", [f"CREATE TABLE {{t}} ({cols}) USING iceberg PARTITIONED BY (days(ts), bucket(4, id), region, truncate(3, region)) TBLPROPERTIES ('format-version' = '2')",
                     f"INSERT INTO {{t}} VALUES {values(0, 30)}"])
    # What Pondra doesn't read is refused by name.
    spark.sql(f"CREATE TABLE delta.`{root}/delta/variant` (id BIGINT, v VARIANT) USING delta")
    spark.sql(f"INSERT INTO delta.`{root}/delta/variant` SELECT 1, parse_json('{{\"a\": 1}}')")
    expected["delta:variant"] = {"path": f"{root}/delta/variant", "columns": [], "rows": [], "refused": "variantType"}
    json.dump(expected, open(f"{root}/expected.json", "w"), default=str)
    spark.stop()


def equality_delete(spark, root, name, keys):
    """An equality-delete file on (id, region), written with pyarrow (field ids as Iceberg's) and
    committed through Iceberg's own API."""
    import pyarrow as pa, pyarrow.parquet as pq
    jvm = spark._jvm
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    ids = [table.schema().findField(c).fieldId() for c in ("id", "region")]
    rows = pa.table({"id": pa.array([k[0] for k in keys], pa.int64()), "region": pa.array([k[1] for k in keys], pa.string())})
    rows = rows.cast(pa.schema([pa.field("id", pa.int64(), metadata={"PARQUET:field_id": str(ids[0])}), pa.field("region", pa.string(), metadata={"PARQUET:field_id": str(ids[1])})]))
    path = f"{root}/iceberg/db/equality/data/eq-deletes-1.parquet"
    pq.write_table(rows, path)
    fields = spark.sparkContext._gateway.new_array(jvm.int, 2)
    fields[0], fields[1] = ids
    delete = (jvm.org.apache.iceberg.FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(fields).withPath(path).withFormat("parquet")
              .withFileSizeInBytes(os.path.getsize(path)).withRecordCount(len(keys)).build())
    table.newRowDelta().addDeletes(delete).commit()


def rows(df):
    """A Spark read as columns and rows (sorted), in JSON's terms."""
    return {"columns": df.columns, "rows": sorted([[plain(v) for v in r] for r in df.collect()], key=json.dumps)}


def plain(v):
    """A value as both sides are compared: dates, times and decimals as text, structs as dicts."""
    if hasattr(v, "asDict"):
        return {k: plain(x) for k, x in v.asDict().items()}
    if isinstance(v, dict):
        return {k: plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [plain(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    if isinstance(v, float):
        return round(v, 6)
    if isinstance(v, decimal.Decimal):
        return str(v.normalize())
    if isinstance(v, dt.datetime):
        return v.replace(tzinfo=None).isoformat(sep=" ")
    if isinstance(v, (dt.date, dt.time)):
        return v.isoformat()
    return v


# ---------------------------------------------------------------- the check

def duck():
    """DuckDB with its Delta and Iceberg extensions from their pip packages."""
    import duckdb
    con = duckdb.connect()
    for ext in ("delta", "avro", "iceberg"):
        found = [f for d in site.getsitepackages() for f in glob.glob(f"{d}/duckdb_extension_{ext}/**/{ext}.duckdb_extension", recursive=True)]
        con.execute(f"LOAD '{found[0]}'" if found else f"INSTALL {ext}; LOAD {ext}")
    return con


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--make", help=argparse.SUPPRESS)  # (run by --spark's Python: write Spark's tables here)
    ap.add_argument("--read", help=argparse.SUPPRESS)  # (run by --spark's Python: read these tables)
    ap.add_argument("--commit", help=argparse.SUPPRESS)  # (run by --spark's Python: append through this node's catalog)
    ap.add_argument("--spark", help="a Python with PySpark 4 and delta-spark: Spark's tables too")
    ap.add_argument("--port", type=int, default=8860)
    ap.add_argument("--only", default="")
    ap.add_argument("--dir", help="write the tables here and keep them; tables already there are used again")
    a = ap.parse_args()
    if a.make:
        return make_spark(a.make)
    if a.read:
        return read_spark(json.loads(a.read))
    if a.commit:
        return spark_commits(a.commit)
    sys.path.insert(0, os.path.join(HERE, "..", "python"))
    os.environ.setdefault("PONDRA_BIN", os.path.join(HERE, "..", "target", "release", "pondra"))
    import pondra
    root = a.dir or tempfile.mkdtemp(prefix="pondra-formats-")
    os.makedirs(root, exist_ok=True)
    expected = {}
    if a.spark and a.dir and os.path.exists(f"{root}/expected.json"):
        expected.update(json.load(open(f"{root}/expected.json")))
    elif a.spark:
        t0 = time.time()
        subprocess.run([a.spark, os.path.abspath(__file__), "--make", root], check=True, env={**os.environ, "TZ": "UTC"})
        expected.update(json.load(open(f"{root}/expected.json")))
        print(f"(Spark wrote its tables in {time.time() - t0:.0f} s)", flush=True)
    expected.update(make_others(root))
    only = set(filter(None, a.only.split(",")))
    con = pondra.local(os.path.join(root, "lake"), port=a.port)
    duckdb = duck()
    checks = {}
    try:
        for name, want in sorted(expected.items()):
            if only and name.split(":")[1] not in only:
                continue
            kind, fn = name.split(":")[0], {"delta": "delta_scan", "iceberg": "iceberg_scan"}[name.split(":")[0]]
            q = f"SELECT * FROM {fn}('{want['path']}'{want.get('options', '')})"
            try:
                got = con.sql(q).collect()
                ours = {"columns": got.column_names, "rows": sorted([[plain(v) for v in r.values()] for r in got.to_pylist()], key=json.dumps)}
                ok = ours == {"columns": want["columns"], "rows": want["rows"]}
                why = None if ok else diff(want, ours)
            except Exception as e:  # noqa: BLE001 (reported)
                ok, why = False, str(e)[:600]
            if want.get("refused"):
                ok, why = (why is not None and want["refused"] in why), why
            checks[name] = ok
            print(json.dumps({"table": name, "equal": ok, **({"why": why} if why else {})}, default=str), flush=True)
        if not only or "own" in only:
            checks.update(own(con, root))
        if not only or "attach" in only:
            checks.update(attach(con, root, expected, a.port + 5))
        if not only or "python" in only:
            checks.update(python_names(con, expected))
        if not only or "insert" in only:
            checks.update(inserts(con, root, expected, a.spark, a.port + 7))
        if a.spark and (not only or "commits" in only):
            checks.update(commits(con, a.spark))
        if a.spark and (not only or "spread" in only):
            checks.update(spread(root, expected, a.port + 10))
    finally:
        con.close()
        shutil.rmtree(os.path.join(root, "lake"), ignore_errors=True)
        if not a.dir:
            shutil.rmtree(root, ignore_errors=True)
    print(json.dumps({"equal": sum(checks.values()), "tables": len(checks)}))
    sys.exit(0 if all(checks.values()) else 1)


def attach(con, root, expected, port):
    """Other engines' tables attached, read by name: a folder of Delta tables, one Delta table, an
    Iceberg warehouse's folder, and an Iceberg REST catalog reached with OAuth (its client id and
    secret in a secret whose scope is the catalog); DETACH."""
    out, have = {}, lambda k: k in expected
    rows_of = lambda q: sorted([[plain(v) for v in r.values()] for r in con.sql(q).collect().to_pylist()], key=json.dumps)
    def same(label, q, want):
        try:
            ok = rows_of(q) == want["rows"]
            why = None if ok else "differs"
        except Exception as e:  # noqa: BLE001
            ok, why = False, str(e)[:400]
        out[f"attach:{label}"] = ok
        print(json.dumps({"table": f"attached: {label}", "equal": ok, **({"why": why} if why else {})}), flush=True)
    if have("delta:dv"):
        con.sql(f"ATTACH '{root}/delta' AS spark_delta (TYPE delta)")
        con.sql(f"ATTACH '{root}/delta/partitioned' AS one_table (TYPE delta)")
        con.sql(f"ATTACH '{root}/iceberg' AS warehouse (TYPE iceberg)")
        same("a folder of Delta tables, name.table", "SELECT * FROM spark_delta.dv", expected["delta:dv"])
        same("one Delta table, by its name alone", "SELECT * FROM one_table", expected["delta:partitioned"])
        same("an Iceberg warehouse's folder, name.namespace.table", "SELECT * FROM warehouse.db.mor", expected["iceberg:mor"])
        con.sql("DETACH spark_delta")
        try:
            con.sql("SELECT * FROM spark_delta.dv").collect()
            out["attach:DETACH"] = False
        except RuntimeError:
            out["attach:DETACH"] = True
        print(json.dumps({"table": "attached: DETACH, then not there", "equal": out["attach:DETACH"]}), flush=True)
    # An Iceberg REST catalog (PyIceberg's tables behind it), with OAuth.
    server = subprocess.Popen([sys.executable, os.path.join(HERE, "sim_iceberg_rest.py"), "--port", str(port), "--warehouse", f"{root}/pyiceberg"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(1.5)
        con.sql(f"CREATE SECRET rest (TYPE iceberg, CLIENT_ID 'pondra', CLIENT_SECRET 's3cret', SCOPE 'http://127.0.0.1:{port}')")
        con.sql(f"ATTACH 'http://127.0.0.1:{port}' AS rest_cat (TYPE iceberg, SECRET rest)")
        same("an Iceberg REST catalog with OAuth, name.namespace.table", "SELECT * FROM rest_cat.db.sales", expected["iceberg:pyiceberg"])
        con.sql(f"CREATE OR REPLACE SECRET rest (TYPE iceberg, CLIENT_ID 'pondra', CLIENT_SECRET 'wrong', SCOPE 'http://127.0.0.1:{port}')")
        try:
            con.sql("SELECT * FROM rest_cat.db.sales").collect()
            refused = ""
        except RuntimeError as e:
            refused = str(e)
        out["attach:wrong secret"] = "gave no token" in refused
        print(json.dumps({"table": "attached: a REST catalog with the wrong secret, refused", "equal": out["attach:wrong secret"], **({} if out["attach:wrong secret"] else {"why": refused[:300]})}), flush=True)
    finally:
        server.kill()
    return out


def python_names(con, expected):
    """The same tables from Python: frames' `scan_delta` / `scan_iceberg`, and pondra.spark's
    `spark.read.format("delta" | "iceberg")` with Spark's options for an older version."""
    from pondra.spark import SparkSession
    spark, out = SparkSession(con), {}
    as_rows = lambda t: sorted([[plain(v) for v in r.values()] for r in t.to_pylist()], key=json.dumps)
    cases = []
    if "delta:dv" in expected:
        snapshot = expected["iceberg:mor@2"]["options"].split("=>")[1].strip()
        cases = [("frames: scan_delta", lambda: con.scan_delta(expected["delta:dv"]["path"]).collect(), "delta:dv"),
                 ("frames: scan_iceberg", lambda: con.scan_iceberg(expected["iceberg:mor"]["path"]).collect(), "iceberg:mor"),
                 ("pondra.spark: format('delta'), versionAsOf", lambda: spark.read.format("delta").option("versionAsOf", 2).load(expected["delta:dv@2"]["path"])._f.collect(), "delta:dv@2"),
                 ("pondra.spark: format('iceberg'), snapshot-id", lambda: spark.read.format("iceberg").option("snapshot-id", snapshot).load(expected["iceberg:mor@2"]["path"])._f.collect(), "iceberg:mor@2")]
    cases.append(("frames: scan_delta of delta-rs's table", lambda: con.scan_delta(expected["delta:deltars"]["path"]).collect(), "delta:deltars"))
    for label, read, name in cases:
        try:
            ok = as_rows(read()) == expected[name]["rows"]
            why = None if ok else "differs"
        except Exception as e:  # noqa: BLE001
            ok, why = False, str(e)[:300]
        out[f"python:{label}"] = ok
        print(json.dumps({"table": label, "equal": ok, **({"why": why} if why else {})}), flush=True)
    return out


def inserts(con, root, expected, spark, port):
    """INSERT into other engines' tables, attached: Delta (deletion vectors on, columns mapped by
    name, partitions with odd values and NULL) and Iceberg (plain, and partitioned by day, bucket,
    identity and truncate), then read back by the engine that made them (Spark) == Pondra's read,
    every row there once; a retried INSERT (same job) applied once; a PyIceberg table in a REST
    catalog, committed through the catalog, read back by PyIceberg."""
    import uuid
    out = {}
    got = lambda q: sorted([[plain(v) for v in r.values()] for r in con.sql(q).collect().to_pylist()], key=json.dumps)
    new_rows = lambda lo, hi: " UNION ALL ".join(f"SELECT {i} AS id, 'r{i % 3}' AS region, {i}.5 AS amount, DATE '2026-02-{1 + i % 27:02d}' AS day, TIMESTAMP '2026-02-01 00:00:00' + INTERVAL '{i} minutes' AS ts" for i in range(lo, hi))
    if spark and "delta:ins_dv" in expected:
        con.sql(f"ATTACH '{root}/delta' AS dw (TYPE delta)")
        con.sql(f"ATTACH '{root}/iceberg' AS iw (TYPE iceberg)")
        job = uuid.uuid4().hex
        before = {t: con.sql(f"SELECT count(*) AS n FROM {t}").rows()[0]["n"] for t in ("dw.ins_dv", "dw.ins_mapped", "dw.ins_part", "iw.db.ins", "iw.db.ins_part")}
        con.sql(f"INSERT INTO dw.ins_dv {new_rows(1000, 1010)}", job=job)
        again = con.sql(f"INSERT INTO dw.ins_dv {new_rows(1000, 1010)}", job=job)  # (the same job, retried)
        con.sql("INSERT INTO dw.ins_mapped SELECT id + 100, region, total, day, ts FROM dw.ins_mapped WHERE id < 3")
        con.sql("INSERT INTO dw.ins_part VALUES (2, DATE '2026-01-01', 'a/b c', 2), (3, NULL, 'x', 3), (4, DATE '2026-01-02', NULL, 4)")
        con.sql(f"INSERT INTO iw.db.ins {new_rows(2000, 2025)}")
        con.sql(f"INSERT INTO iw.db.ins_part {new_rows(3000, 3040)}")
        added = {"dw.ins_dv": 10, "dw.ins_mapped": 3, "dw.ins_part": 3, "iw.db.ins": 25, "iw.db.ins_part": 40}
        places = {"dw.ins_dv": ("delta", f"{root}/delta/ins_dv"), "dw.ins_mapped": ("delta", f"{root}/delta/ins_mapped"), "dw.ins_part": ("delta", f"{root}/delta/ins_part"),
                  "iw.db.ins": ("iceberg", f"{root}/iceberg/db/ins"), "iw.db.ins_part": ("iceberg", f"{root}/iceberg/db/ins_part")}
        theirs = json.loads(subprocess.run([spark, os.path.abspath(__file__), "--read", json.dumps(places)], capture_output=True, text=True, check=True, env={**os.environ, "TZ": "UTC"}).stdout.strip().splitlines()[-1])
        for t in places:
            ours = got(f"SELECT * FROM {t}")
            ok = ours == theirs[t]["rows"] and len(ours) == before[t] + added[t]
            out[f"insert:{t}"] = ok
            print(json.dumps({"table": f"INSERT into {t}, read back by Spark == Pondra", "equal": ok, **({} if ok else {"why": diff(theirs[t], {"columns": theirs[t]["columns"], "rows": ours}), "before": before[t]})}), flush=True)
        # Partition values as Iceberg computes them (day, bucket, truncate): PyIceberg, pruning
        # files by them, finds every row Pondra wrote by its id and its time.
        from pyiceberg.table import StaticTable
        from pyiceberg.expressions import EqualTo
        meta = sorted(glob.glob(f"{root}/iceberg/db/ins_part/metadata/v*.metadata.json"), key=lambda p: int(p.rsplit("/v", 1)[1].split(".")[0]))[-1]
        static = StaticTable.from_metadata(meta)
        found = all(static.scan(row_filter=EqualTo("id", i)).to_arrow().num_rows == con.sql(f"SELECT count(*) AS n FROM iw.db.ins_part WHERE id = {i}").rows()[0]["n"] > 0 for i in range(3000, 3040, 3))
        out["insert:partitions"] = found
        print(json.dumps({"table": "INSERT into an Iceberg table by day, bucket and truncate: PyIceberg prunes by the values Pondra wrote", "equal": found}), flush=True)
        out["insert:retried"] = again.get("duplicate") is True
        print(json.dumps({"table": "INSERT retried with its job: applied once", "equal": out["insert:retried"]}), flush=True)
    # Through an Iceberg REST catalog: the catalog commits it.
    from pyiceberg.catalog.sql import SqlCatalog
    server = subprocess.Popen([sys.executable, os.path.join(HERE, "sim_iceberg_rest.py"), "--port", str(port), "--warehouse", f"{root}/pyiceberg"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(1.5)
        con.sql(f"CREATE OR REPLACE SECRET rest_w (TYPE iceberg, CLIENT_ID 'pondra', CLIENT_SECRET 's3cret', SCOPE 'http://127.0.0.1:{port}')")
        con.sql(f"ATTACH 'http://127.0.0.1:{port}' AS rest_w (TYPE iceberg)")
        n = con.sql("SELECT count(*) AS n FROM rest_w.db.sales").rows()[0]["n"]
        con.sql("INSERT INTO rest_w.db.sales SELECT id + 10000, region, total, day FROM rest_w.db.sales WHERE id < 30")
        table = SqlCatalog("local", uri=f"sqlite:///{root}/pyiceberg/catalog.db", warehouse=f"file://{root}/pyiceberg").load_table("db.sales")
        theirs = sorted([[plain(v) for v in r.values()] for r in table.scan().to_arrow().to_pylist()], key=json.dumps)
        ok = got("SELECT * FROM rest_w.db.sales") == theirs and len(theirs) == n + 20
        out["insert:rest"] = ok
        print(json.dumps({"table": "INSERT into a REST catalog's table, committed by the catalog, read back by PyIceberg", "equal": ok, **({} if ok else {"n": n, "theirs": len(theirs)})}), flush=True)
    finally:
        server.kill()
    return out


def own(con, root):
    """Pondra's own tables as it publishes them for other engines (Delta and Iceberg), read back
    by `delta_scan` and `iceberg_scan`: the table's rows, a renamed column and a change included."""
    con.sql("CREATE TABLE pub (id BIGINT, name VARCHAR, v DOUBLE) WITH (publish = 'delta,iceberg')")
    con.sql("INSERT INTO pub VALUES " + ", ".join(f"({i}, 'n{i % 4}', {i / 4})" for i in range(200)))
    con.sql("ALTER TABLE pub RENAME COLUMN v TO value")
    con.sql("UPDATE pub SET value = -1 WHERE id < 20")
    con.sql("CHECKPOINT")
    lake = os.path.join(root, "lake")
    want = sorted([[plain(v) for v in r.values()] for r in con.sql("SELECT * FROM pub").collect().to_pylist()], key=json.dumps)
    out = {}
    for fn in ("delta_scan", "iceberg_scan"):
        got = con.sql(f"SELECT * FROM {fn}('{lake}/data/pub')").collect()
        ok = got.column_names == ["id", "name", "value"] and sorted([[plain(v) for v in r.values()] for r in got.to_pylist()], key=json.dumps) == want
        out[f"own:{fn}"] = ok
        print(json.dumps({"table": f"Pondra's own, published, by {fn}", "equal": ok, **({} if ok else {"why": diff({"columns": ["id", "name", "value"], "rows": want}, {"columns": got.column_names, "rows": sorted([[plain(v) for v in r.values()] for r in got.to_pylist()], key=json.dumps)})})}), flush=True)
    return out


def spread(root, expected, port):
    """Spark's tables with deletes, copied to a bucket (a local S3), read by three nodes with a
    secret: spread over them (each node its share of the files, and of the deletes) == one node
    == Spark. Iceberg names its files by their whole path: read where they are now
    (`allow_moved_paths`)."""
    import boto3, pyarrow as pa
    sys.path.insert(0, HERE)
    import harness
    harness.A = argparse.Namespace(s3=False, keep=False)
    sim = subprocess.Popen([sys.executable, os.path.join(HERE, "sim_r2.py"), "--port", str(port + 50), "--zero"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port + 50}", region_name="us-east-1", aws_access_key_id="k", aws_secret_access_key="s")
    for _ in range(100):
        try:
            s3.create_bucket(Bucket="formats")
            break
        except Exception:
            time.sleep(0.1)
    tables = ["delta:dv", "delta:mapped", "delta:partitioned", "iceberg:mor", "iceberg:dv", "iceberg:equality"]
    for t in tables:
        local = expected[t]["path"]
        for d, _, fs in os.walk(local):
            for f in fs:
                s3.upload_file(os.path.join(d, f), "formats", os.path.relpath(os.path.join(d, f), root))
    lake = tempfile.mkdtemp(prefix="pondra-formats-spread-")
    nodes = [harness.Node(lake, port + i, env={"PONDRA_SECRET_KEY": "k"}).start() for i in range(3)]
    out = {}
    try:
        harness.sql(port, f"CREATE SECRET formats (TYPE s3, KEY_ID 'k', SECRET 's', ENDPOINT 'http://127.0.0.1:{port + 50}', SCOPE 's3://formats')")
        while len(harness.call(port + 1, "GET", "/stats")["nodes"]) < 3:
            time.sleep(0.1)
        for t in tables:
            want, kind = expected[t], t.split(":")[0]
            url = "s3://formats/" + os.path.relpath(want["path"], root)
            q = f"SELECT * FROM {kind}_scan('{url}'{', allow_moved_paths => true' if kind == 'iceberg' else ''})"
            spread_before = harness.metrics_of(port + 1)["pondra_spread_queries_total"]
            got = [pa.ipc.open_stream(harness.call(port + i, "POST", f"/sql?spread={s}&format=arrow", q.encode())).read_all().to_pylist() for i, s in ((0, 0), (1, 1))]
            spread_now = harness.metrics_of(port + 1)["pondra_spread_queries_total"] - spread_before
            as_rows = lambda rows: sorted([[plain(v) for v in r.values()] for r in rows], key=json.dumps)
            ok = all(as_rows(g) == want["rows"] for g in got) and spread_now == 1
            out[f"spread:{t}"] = ok
            print(json.dumps({"table": f"{t} on S3, one node and spread over three", "equal": ok, **({} if ok else {"spread": spread_now, "why": [diff(want, {"columns": want["columns"], "rows": as_rows(g)}) for g in got]})}), flush=True)
    finally:
        [n.kill() for n in nodes]
        sim.kill()
        shutil.rmtree(lake, ignore_errors=True)
    return out


def diff(want, ours):
    if want["columns"] != ours["columns"]:
        return f"columns {ours['columns']} != {want['columns']}"
    missing = [r for r in want["rows"] if r not in ours["rows"]][:3]
    extra = [r for r in ours["rows"] if r not in want["rows"]][:3]
    return f"{len(ours['rows'])} rows, want {len(want['rows'])}; missing {missing}; extra {extra}"


def make_others(root):
    """Tables written by delta-rs and PyIceberg (in this Python), and what they read back."""
    import pyarrow as pa, deltalake
    from pyiceberg.catalog.sql import SqlCatalog
    out = {}
    shutil.rmtree(f"{root}/deltars", ignore_errors=True)
    shutil.rmtree(f"{root}/pyiceberg", ignore_errors=True)
    arrow_rows = lambda t: {"columns": t.column_names, "rows": sorted([[plain(v) for v in r.values()] for r in t.to_pylist()], key=json.dumps)}
    data = lambda lo, hi: pa.table({"id": pa.array(range(lo, hi), pa.int64()), "region": [f"r{i % 3}" if i % 5 else None for i in range(lo, hi)],
                                    "amount": [i * 0.5 for i in range(lo, hi)], "day": pa.array([dt.date(2026, 1, 1 + i % 20) for i in range(lo, hi)])})
    # delta-rs: partitions, a delete (files rewritten), a checkpoint, a column added by a later append.
    path = f"{root}/deltars/sales"
    deltalake.write_deltalake(path, data(0, 200), partition_by=["region"])
    deltalake.write_deltalake(path, data(200, 300), mode="append", partition_by=["region"])
    deltalake.DeltaTable(path).delete("id % 9 = 0")
    deltalake.DeltaTable(path).create_checkpoint()
    more = data(300, 320).append_column("note", pa.array(["n"] * 20))
    deltalake.write_deltalake(path, more, mode="append", partition_by=["region"], schema_mode="merge")
    t = deltalake.DeltaTable(path)
    out["delta:deltars"] = {"path": path, **arrow_rows(t.to_pyarrow_table().select([f.name for f in t.schema().fields]))}
    # PyIceberg: a table in a SQL catalog, appends, a delete, a column renamed.
    warehouse = f"{root}/pyiceberg"
    os.makedirs(warehouse, exist_ok=True)
    catalog = SqlCatalog("local", uri=f"sqlite:///{warehouse}/catalog.db", warehouse=f"file://{warehouse}")
    catalog.create_namespace("db")
    table = catalog.create_table("db.sales", schema=data(1, 2).schema)
    table.append(data(0, 100))
    table.append(data(100, 150))
    table.delete("id < 10")
    with table.update_schema() as u:
        u.rename_column("amount", "total")
    table = catalog.load_table("db.sales")
    out["iceberg:pyiceberg"] = {"path": table.metadata_location, **arrow_rows(table.scan().to_arrow())}
    return out


if __name__ == "__main__":
    main()
