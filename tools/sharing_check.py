#!/usr/bin/env python3
"""Sharing (ADR-046): tables shared with another company through the Delta Sharing door, read by
the `delta-sharing` client (pandas, in the protocol's Parquet form and in Delta's) and by another
Pondra (`ATTACH '<profile>' AS acme (TYPE share)`), against what the provider's own SQL answers.

  sharing_check.py [--bin target/release/pondra] [--work DIR] [--port 9790] [--s3]

--s3: the provider's lake in a bucket (tools/sim_r2.py and AWS_ENDPOINT…, as AGENTS.md says), so
the links are the bucket's own signed URLs; otherwise on a disk, linking to the node. Run it with
a Python that has `delta-sharing` (1.4.2), in a venv of its own: it pins pandas < 3 and pyarrow <=
23, older than tools/requirements.txt's. Prints the checks as JSON and exits 1 if one fails (the lakes and the
nodes' logs are then kept in --work).
"""
import argparse, json, os, shutil, sys, tempfile, time, urllib.error, urllib.request, uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from upgrade_check import Node, Failed  # (a node started and stopped as a scheduler would)


def door(port, method, path, token, body=None, headers=None):
    """A request at the sharing door as a recipient's client makes it: (status, headers, body)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}/delta-sharing{path}", data=data, method=method,
                                 headers={**({"Authorization": f"Bearer {token}"} if token else {}), "Content-Type": "application/json", **(headers or {})})
    try:
        r = urllib.request.urlopen(req, timeout=60)
        return r.status, dict(r.headers), r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode(errors="replace")


def sharing_check(bin, work, port, s3):
    import delta_sharing
    lake = f"s3://{os.environ['PONDRA_BUCKET']}/sharing-{uuid.uuid4().hex[:8]}" if s3 else os.path.join(work, "provider")
    n = Node(bin, lake, port, work, "--retain-secs", "1", env={"PONDRA_PURGE_ROWS": "1"}).start()
    q = n.q

    def rows(sql):
        return sorted(tuple(r.values()) for r in q(sql))

    def frame(df):
        return sorted(tuple(None if v != v else (int(v) if isinstance(v, float) and v.is_integer() and c in ("id", "n") else v) for c, v in zip(df.columns, r)) for r in df.itertuples(index=False))

    def refused(sql):
        try:
            q(sql)
            return ""
        except Failed as e:
            return str(e)

    def tier():
        n.post("/tier")

    checks = {}
    q("CREATE SCHEMA sales")
    q("CREATE TABLE sales.orders (id BIGINT, region VARCHAR, total DOUBLE) WITH (partition_by = 'region')")
    q("INSERT INTO sales.orders SELECT x, CASE WHEN x % 3 = 0 THEN 'EU' WHEN x % 3 = 1 THEN 'US' ELSE 'UK' END, x * 1.5 FROM generate_series(1, 3000) AS s(x)")
    q("CREATE TABLE kv (id BIGINT PRIMARY KEY, v VARCHAR, n BIGINT)")
    q("INSERT INTO kv VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)")
    q("INSERT INTO kv VALUES (2, 'b2', 20)")
    q("CREATE TABLE secret_table (id BIGINT)")
    tier()

    q("CREATE SHARE acme COMMENT 'Orders for Acme'")
    q("ALTER SHARE acme ADD TABLE sales.orders WITH HISTORY")
    q("ALTER SHARE acme ADD TABLE sales.orders PARTITION (region = 'EU'), (region = 'UK') AS sales.orders_eu")
    q("GRANT SELECT ON TABLE kv TO SHARE acme")  # (Snowflake's way)
    made = q("CREATE RECIPIENT acme_corp COMMENT 'Acme Corp' EXPIRES IN '30 days'")
    profile = made["profile"] if isinstance(made, dict) else made[0]["profile"]
    q("GRANT SELECT ON SHARE acme TO RECIPIENT acme_corp")
    token = profile["bearerToken"]
    path = os.path.join(work, "acme.share")
    with open(path, "w") as f:
        json.dump(profile, f)
    tier()
    checks["CREATE RECIPIENT answers its profile once (endpoint, token, expiry); only its hash is kept"] = \
        profile["endpoint"].endswith("/delta-sharing") and token.startswith("pds_") and "expirationTime" in profile \
        and token not in json.dumps(q("SELECT * FROM pondra.recipients")) and rows("SELECT name, shares FROM pondra.recipients") == [("acme_corp", "acme")]

    client = delta_sharing.SharingClient(path)
    listed = sorted(f"{t.share}.{t.schema}.{t.name}" for t in client.list_all_tables())
    checks["the recipient lists the shares granted to it, their schemas and tables, and nothing else"] = \
        listed == ["acme.public.kv", "acme.sales.orders", "acme.sales.orders_eu"] and [s.name for s in client.list_shares()] == ["acme"] \
        and sorted(s.name for s in client.list_schemas(delta_sharing.Share("acme"))) == ["public", "sales"]

    every = rows("SELECT id, region, total FROM sales.orders")
    df = delta_sharing.load_as_pandas(f"{path}#acme.sales.orders")
    checks["a shared table read with pandas == the provider's rows"] = frame(df[["id", "region", "total"]]) == every and len(every) == 3000
    eu = delta_sharing.load_as_pandas(f"{path}#acme.sales.orders_eu")
    checks["a table's partitions shared: those rows only (whole files of the partition_by column)"] = \
        frame(eu[["id", "region", "total"]]) == rows("SELECT id, region, total FROM sales.orders WHERE region IN ('EU', 'UK')")
    kv = delta_sharing.load_as_pandas(f"{path}#acme.public.kv")
    checks["a keyed table shared: its latest row for each key"] = frame(kv[["id", "v", "n"]]) == rows("SELECT id, v, n FROM kv") == [(1, "a", 1), (2, "b2", 20), (3, "c", 3)]

    # A version before the change; rows deleted after: deletion vectors (Delta's form), never handed out whole.
    st, h, _ = door(port, "GET", "/shares/acme/schemas/sales/tables/orders/version", token)
    v0 = int(h.get("delta-table-version", -1))
    q("DELETE FROM sales.orders WHERE id % 10 = 0")
    q("UPDATE sales.orders SET total = -1 WHERE id = 7")
    tier()
    tier()
    now = rows("SELECT id, region, total FROM sales.orders")
    after = delta_sharing.load_as_pandas(f"{path}#acme.sales.orders")
    st_parquet, _, said = door(port, "POST", "/shares/acme/schemas/sales/tables/orders/query", token, {})
    st_delta, hd, _ = door(port, "POST", "/shares/acme/schemas/sales/tables/orders/query", token, {}, {"delta-sharing-capabilities": "responseformat=delta"})
    checks["rows deleted and updated: read through deletion vectors (Delta's form) == the provider's rows; a client of Parquet's form alone refused, never given deleted rows"] = \
        frame(after[["id", "region", "total"]]) == now and len(now) == 2700 and st_parquet == 400 and "deleted rows" in said \
        and st_delta == 200 and "responseformat=delta" in hd.get("delta-sharing-capabilities", "")
    old = delta_sharing.load_as_pandas(f"{path}#acme.sales.orders", version=v0)
    st_eu, _, eu_said = door(port, "POST", "/shares/acme/schemas/sales/tables/orders_eu/query", token, {"version": v0}, {"delta-sharing-capabilities": "responseformat=delta"})
    checks["WITH HISTORY: an older version reads as it was; a table shared without it refuses a version"] = \
        frame(old[["id", "region", "total"]]) == every and st_eu == 400 and "history" in eu_said

    # Columns renamed: Delta's column mapping, read by the new name.
    q("ALTER TABLE kv RENAME COLUMN v TO label")
    q("INSERT INTO kv VALUES (4, 'd', 4)")
    tier()
    kv2 = delta_sharing.load_as_pandas(f"{path}#acme.public.kv")
    checks["a column renamed: read under its new name (column mapping)"] = "label" in kv2.columns and frame(kv2[["id", "label", "n"]]) == rows("SELECT id, label, n FROM kv")

    # Another Pondra: the profile is an invite, attaching it accepts it.
    m = Node(bin, os.path.join(work, "recipient"), port + 1, work).start()
    m.q(f"ATTACH '{json.dumps(profile)}' AS acme (TYPE share)")
    theirs = sorted(tuple(r.values()) for r in m.q("SELECT id, region, total FROM acme.sales.orders"))
    theirs_eu = sorted(tuple(r.values()) for r in m.q("SELECT id, region, total FROM acme.sales.orders_eu"))
    joined = m.q("SELECT count(*) AS n FROM acme.sales.orders o JOIN acme.kv k ON o.id = k.id")
    audit_m = m.q("SELECT statement FROM pondra.audit WHERE statement LIKE 'ATTACH%'")
    checks["another Pondra attaches the share by its profile and reads it == the provider's rows (deletion vectors, a partition, a join)"] = \
        theirs == now and theirs_eu == rows("SELECT id, region, total FROM sales.orders WHERE region IN ('EU', 'UK')") \
        and joined == q("SELECT count(*) AS n FROM sales.orders o JOIN kv k ON o.id = k.id") and all(token not in r["statement"] for r in audit_m)

    # Refusals, each in the audit log.
    no_token = door(port, "GET", "/shares", None)[0]
    wrong = door(port, "GET", "/shares", "pds_not-a-token")[0]
    not_shared = door(port, "POST", "/shares/acme/schemas/public/tables/secret_table/query", token, {})[0]
    st_link, _, body = door(port, "POST", "/shares/acme/schemas/sales/tables/orders_eu/query", token, {}, {"delta-sharing-capabilities": "responseformat=delta"})
    link = next((json.loads(l)["file"]["deltaSingleAction"]["add"]["path"] for l in body.splitlines() if '"file"' in l), "")
    tampered = 0
    if not s3:
        bad = link[:-3] + ("AAA" if not link.endswith("AAA") else "BBB")
        try:
            urllib.request.urlopen(bad, timeout=30)
            tampered = 200
        except urllib.error.HTTPError as e:
            tampered = e.code
    checks["a link is the bucket's own signed URL (--s3), or the node's, signed with the lake's key"] = \
        ("X-Amz-Signature=" in link and f"/{os.environ['PONDRA_BUCKET']}/" in link) if s3 else ("/delta-sharing/files/" in link and "sig=" in link)
    q("REVOKE SELECT ON SHARE acme FROM RECIPIENT acme_corp")
    revoked = door(port, "GET", "/shares/acme/all-tables", token)[0]
    q("GRANT SELECT ON SHARE acme TO RECIPIENT acme_corp")
    rotated = q("ALTER RECIPIENT acme_corp ROTATE TOKEN")
    new_token = (rotated if isinstance(rotated, dict) else rotated[0])["profile"]["bearerToken"]
    old_after = door(port, "GET", "/shares", token)[0]
    new_after = door(port, "GET", "/shares", new_token)[0]
    q("CREATE RECIPIENT brief EXPIRES IN '1 second'")
    time.sleep(1.5)
    q("DROP RECIPIENT brief")
    checks["refused: no token, a wrong one, a table not in the share, a link changed, a grant revoked, a token rotated away"] = \
        (no_token, wrong, not_shared, revoked, old_after, new_after) == (401, 401, 404, 404, 401, 200) and (s3 or tampered == 403) and st_link == 200
    time.sleep(1.5)  # (the audit log's writer: a moment)
    audit = q("SELECT \"user\", door, class, statement, outcome FROM pondra.audit WHERE door = 'sharing' ORDER BY at")
    outcomes = {(a["user"], a["outcome"]) for a in audit}
    checks["every request at the door is in pondra.audit, refusals too"] = \
        ("recipient acme_corp", "ok") in outcomes and ("recipient acme_corp", "refused") in outcomes and any(a["outcome"] == "refused" and a["user"] == "" for a in audit) \
        and any("query acme.sales.orders" in a["statement"] for a in audit)

    checks["SHOW SHARES, SHOW RECIPIENTS, DESCRIBE SHARE"] = \
        [tuple(r.values())[:3] for r in q("SHOW SHARES")] == [("acme", "Orders for Acme", 3)] and [r["name"] for r in q("SHOW RECIPIENTS")] == ["acme_corp"] \
        and [r["name"] for r in q("DESCRIBE SHARE acme")] == ["public.kv", "sales.orders", "sales.orders_eu"]
    checks["refused by name: a view, a table not here, a partition of another column, a share or recipient not there, OR REPLACE"] = all([
        "materialized view" in refused("ALTER SHARE acme ADD TABLE information_schema.tables"),
        "no table" in refused("ALTER SHARE acme ADD TABLE nothing_here"),
        "partitioned by region" in refused("ALTER SHARE acme ADD TABLE sales.orders PARTITION (total = '1') AS sales.x"),
        "no share" in refused("GRANT SELECT ON SHARE nope TO RECIPIENT acme_corp"),
        "no recipient" in refused("GRANT SELECT ON SHARE acme TO RECIPIENT nobody"),
        "its tables and grants" in refused("CREATE OR REPLACE SHARE acme"),
        "its token" in refused("CREATE OR REPLACE RECIPIENT acme_corp"),
    ])

    # Shares and recipients are objects (ADR-049's registry): listed with their comments, described,
    # made again from SHOW CREATE after a drop.
    tables_of = lambda: sorted(f"{t['schema']}.{t['name']}" for t in json.loads(door(port, "GET", "/shares/acme/all-tables", new_token)[2]).get("items", []))
    q("COMMENT ON RECIPIENT acme_corp IS 'Acme Corp, buying'")
    listed = {(r["kind"], r["name"]): r for r in q("SELECT kind, name, comment, definition FROM pondra.objects WHERE kind IN ('share', 'recipient')")}
    shown = q("SHOW CREATE SHARE acme")[0]["definition"]
    q("DROP SHARE acme")
    left = q("SELECT count(*) AS n FROM pondra.objects WHERE comment = 'Orders for Acme'")[0]["n"]
    for stmt in shown.rstrip(";").split(";\n"):
        q(stmt)
    checks["shares and recipients in pondra.objects with their comments; SHOW CREATE SHARE run again after DROP SHARE makes the same share, its tables, grants and comment"] = \
        listed.get(("share", "acme"), {}).get("comment") == "Orders for Acme" and listed.get(("recipient", "acme_corp"), {}).get("comment") == "Acme Corp, buying" \
        and listed[("recipient", "acme_corp")].get("definition") is None and "token is shown once" in refused("SHOW CREATE RECIPIENT acme_corp") \
        and "PARTITION (region = 'EU'), (region = 'UK') AS sales.orders_eu" in shown and "GRANT SELECT ON SHARE acme TO RECIPIENT acme_corp" in shown \
        and left == 0 and q("SHOW CREATE SHARE acme")[0]["definition"] == shown and tables_of() == ["public.kv", "sales.orders", "sales.orders_eu"] \
        and [tuple(r.values())[:2] for r in q("SHOW SHARES")] == [("acme", "Orders for Acme")]

    # As in Snowflake: a share follows its table through a rename, and loses it with a drop.
    q("ALTER TABLE kv RENAME TO kv_2025")
    renamed = tables_of()
    q("INSERT INTO kv_2025 VALUES (5, 'e', 5)")
    tier()
    with open(path, "w") as f:
        json.dump((rotated if isinstance(rotated, dict) else rotated[0])["profile"], f)
    kv3 = delta_sharing.load_as_pandas(f"{path}#acme.public.kv")
    dropped = q("DROP TABLE kv_2025")
    q("CREATE TABLE kv_2025 (id BIGINT, label VARCHAR, n BIGINT)")
    checks["a shared table renamed: still shared under its name, with its new rows; dropped: it leaves the share, and a new table of its name isn't shared"] = \
        renamed == ["public.kv", "sales.orders", "sales.orders_eu"] and frame(kv3[["id", "label", "n"]]) == [(1, "a", 1), (2, "b2", 20), (3, "c", 3), (4, "d", 4), (5, "e", 5)] \
        and (dropped if isinstance(dropped, dict) else dropped[0]).get("unshared") == ["acme"] and tables_of() == ["sales.orders", "sales.orders_eu"] \
        and "kv" not in q("SHOW CREATE SHARE acme")[0]["definition"]

    q("DROP SHARE acme")
    checks["DROP SHARE: the recipient sees nothing"] = json.loads(door(port, "GET", "/shares", new_token)[2])["items"] == []
    return checks


def main():
    a = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    a.add_argument("--bin", default=os.path.join(HERE, "..", "target", "release", "pondra"))
    a.add_argument("--work")
    a.add_argument("--port", type=int, default=9790)
    a.add_argument("--s3", action="store_true")
    o = a.parse_args()
    work = o.work or tempfile.mkdtemp(prefix="pondra-sharing-")
    os.makedirs(work, exist_ok=True)
    try:
        checks = sharing_check(os.path.abspath(o.bin), work, o.port, o.s3)
    except Exception as e:
        checks = {"ran to the end": False, "error": repr(e)}
    print(json.dumps(checks, indent=1))
    ok = all(v is True for k, v in checks.items() if k != "error")
    if ok and not o.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"kept: {work}", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
