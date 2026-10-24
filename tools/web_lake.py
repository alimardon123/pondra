#!/usr/bin/env python3
"""Makes web/lake/, the fixture tools/web_check.mjs reads: a Delta table a node published, and what the node answers for
those same bytes. The page's reads of the table are checked against the node in CI, where there is no node binary.

  python3 tools/web_lake.py --pondra <node binary> [--out web/lake] [--port 18720]

A node runs over a new temporary lake and publishes `orders` (id, name, qty) as Delta: 20 rows, then 3 rows at a time
until version 10 (a checkpoint, and `_last_checkpoint`), a DELETE of one row (a deletion vector, inline in the log), a
RENAME COLUMN (a metaData with column mapping), and one more INSERT. Each change waits for its commit. The log's
`_delta_log/` (every commit, the checkpoints, `_last_checkpoint`) and the data files its adds name are copied to a
staging folder, and the node reads that copy: the latest version and every version as CSV, and one data file, into
expected.json. A version the node refuses is recorded as {"error": body}. Then `--out`/orders/ and `--out`/expected.json
replace the old fixture, the node is stopped and the temporary lake is removed.
"""
import argparse, glob, http.client, json, os, re, secrets, shutil, sys, tempfile, time

import harness  # its Node class (it stops the nodes it started at exit); importing it runs no test

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TABLE = "orders"
COLUMNS = ["id", "name", "qty"]
RENAMED = ["id", "name", "amount"]
FIRST_ROWS = 20
ROWS_PER_CHANGE = 3
CHECKPOINT = 10  # a Delta checkpoint every 10 versions (src/delta.rs)
MIN_VERSIONS = 13
COMMIT = re.compile(r"(\d{20})\.json")
KEPT = re.compile(r"\d{20}\.json|\d{20}\.checkpoint\.parquet|_last_checkpoint")  # what a reader of the log needs


class Door:
    """The node's SQL door. The owner's key lets it read a path on its own disk, as its owner may."""

    def __init__(self, node, key):
        self.node, self.key = node, key

    def sql(self, text, fmt=None):
        """(status, body) of one statement, whatever the answer."""
        conn = http.client.HTTPConnection("127.0.0.1", self.node.port, timeout=300)
        try:
            conn.request("POST", "/sql" + (f"?format={fmt}" if fmt else ""), text.encode(), {"x-pondra-owner": self.key})
            reply = conn.getresponse()
            return reply.status, reply.read().decode("utf-8", "replace")
        finally:
            conn.close()

    def must(self, text, fmt=None):
        status, body = self.sql(text, fmt)
        if status != 200:
            self.fail(f"{text[:100]!r} answered {status}: {body[:500]}")
        return body

    def fail(self, why):
        tail = open(self.node.log).read()[-800:] if os.path.exists(self.node.log) else ""
        raise SystemExit(f"web_lake: {why}\n--- the node's log ---\n{tail}")


class Table:
    """`orders` in the node's lake. Its Delta folder is found once the node has made its first commit."""

    def __init__(self, lake):
        self.lake, self.delta = lake, None

    def log(self):
        if self.delta is None:
            found = [p for p in glob.glob(os.path.join(self.lake, "**", "_delta_log"), recursive=True)
                     if os.path.basename(os.path.dirname(p)) == TABLE]
            self.delta = found[0] if found else None
        return self.delta

    def folder(self):
        return os.path.dirname(self.log())

    def versions(self):
        """The versions with a JSON commit, ascending."""
        if self.log() is None:
            return []
        return sorted(int(m.group(1)) for m in (COMMIT.fullmatch(f) for f in os.listdir(self.delta)) if m)

    def newest(self):
        return max(self.versions(), default=-1)

    def actions(self, after=-1):
        """Every action of the commits newer than `after`, in order."""
        for v in self.versions():
            if v > after:
                with open(os.path.join(self.delta, f"{v:020}.json")) as f:
                    for line in f:
                        if line.strip():
                            yield json.loads(line)

    def live(self):
        """The files live at the newest version, replayed from the JSON commits (all of them are kept)."""
        files = {}
        for action in self.actions():
            if "add" in action:
                files[action["add"]["path"]] = action["add"]
            elif "remove" in action:
                files.pop(action["remove"]["path"], None)
        return files

    def rows(self):
        """The rows a reader sees: each file's numRecords less the rows its deletion vector takes out."""
        return sum(json.loads(a["stats"])["numRecords"] - (a.get("deletionVector") or {}).get("cardinality", 0)
                   for a in self.live().values())


def wait_until(door, what, test, secs=120):
    """Asks `test` until it gives something but None, and returns that. A file still being written reads as None."""
    deadline = time.time() + secs
    while time.time() < deadline:
        try:
            got = test()
        except (OSError, ValueError):  # (a commit still being written: the next ask reads it whole)
            got = None
        if got is not None:
            return got
        time.sleep(0.2)
    door.fail(f"timed out waiting for {what}")


def insert(ids, columns):
    values = ", ".join(f"({i}, 'item-{i}', {i * 10})" for i in ids)
    return f"INSERT INTO {TABLE} ({', '.join(columns)}) VALUES {values}"


def has_deletion_vector(actions):
    return any("deletionVector" in a.get("add", {}) for a in actions)


def has_column_mapping(actions):
    return any(a.get("metaData", {}).get("configuration", {}).get("delta.columnMapping.mode") == "name" for a in actions)


def change(door, table, text, label, seen=None):
    """Runs one statement, then waits for its commit: a commit newer than the newest there was (holding what `seen`
    asks for, when given), and the log's rows equal to the table's. Returns the newest version by then: maintenance
    can commit after the change's own commit, so that is not always the change's own version."""
    before = table.newest()
    door.must(text)
    rows = json.loads(door.must(f"SELECT count(*) AS n FROM {TABLE}"))[0]["n"]

    def committed():
        if table.newest() <= before or (seen and not seen(list(table.actions(before)))):
            return None
        return table.newest() if table.rows() == rows else None

    version = wait_until(door, f"the commit of {label}", committed)
    print(f"  {label}: log through version {version}, {rows} rows", file=sys.stderr)
    return version


def checkpoint_ready(table):
    """The checkpoint at CHECKPOINT is there, and `_last_checkpoint` names it (it is written after the checkpoint)."""
    last = os.path.join(table.delta, "_last_checkpoint")
    named = None
    if os.path.exists(last):
        with open(last) as f:
            named = json.load(f)["version"]
    there = os.path.exists(os.path.join(table.delta, f"{CHECKPOINT:020}.checkpoint.parquet"))
    return True if there and named == CHECKPOINT else None


def publish(door, table):
    """The changes the fixture holds: 20 rows, 3 rows at a time up to version 10, a DELETE, a RENAME COLUMN, and one more
    INSERT. Returns the versions of the DELETE and of the RENAME."""
    door.must(f"CREATE TABLE {TABLE} (id BIGINT, name VARCHAR, qty INT) WITH (publish = 'delta')")
    change(door, table, insert(range(1, FIRST_ROWS + 1), COLUMNS), f"the first INSERT of {FIRST_ROWS} rows")
    next_id = FIRST_ROWS + 1
    while table.newest() < CHECKPOINT:
        change(door, table, insert(range(next_id, next_id + ROWS_PER_CHANGE), COLUMNS), f"an INSERT of {ROWS_PER_CHANGE} rows")
        next_id += ROWS_PER_CHANGE
    wait_until(door, f"the checkpoint of version {CHECKPOINT}", lambda: checkpoint_ready(table))
    delete = change(door, table, f"DELETE FROM {TABLE} WHERE id = 5", "a DELETE of id 5", seen=has_deletion_vector)
    rename = change(door, table, f"ALTER TABLE {TABLE} RENAME COLUMN qty TO amount", "a RENAME COLUMN qty", seen=has_column_mapping)
    change(door, table, insert(range(next_id, next_id + ROWS_PER_CHANGE), RENAMED), "a last INSERT")
    return delete, rename


def settle(door, table):
    """Waits until the log's folder stops changing: maintenance can still commit after the last change."""
    last, since, deadline = None, time.time(), time.time() + 120
    while time.time() - since < 3:
        now = sorted(os.listdir(table.log()))
        if now != last:
            last, since = now, time.time()
        if time.time() > deadline:
            door.fail("the log kept changing after the last change")
        time.sleep(0.2)


def check_log(door, table, delete, rename):
    """What the log must say (printed, and a failure stops the run): the versions, the deletion vector, the renamed column,
    and the rows the node counts."""
    newest, live = table.newest(), table.live()
    if newest + 1 < MIN_VERSIONS:
        door.fail(f"only {newest + 1} versions; the fixture needs {MIN_VERSIONS}")
    dvs = sorted(p for p, a in live.items() if "deletionVector" in a)
    if not dvs:
        door.fail("the latest version names no file with a deletion vector")
    dv = live[dvs[0]]
    print(f"  deletion vector on {dvs[0]}: {dv['stats']}, cardinality {dv['deletionVector']['cardinality']}", file=sys.stderr)
    metas = [a["metaData"] for a in table.actions() if "metaData" in a]
    mode = metas[-1]["configuration"].get("delta.columnMapping.mode")
    if mode != "name" or delete >= rename:
        door.fail(f"the metaData at version {rename} does not map columns by name (mode {mode})")
    counted = json.loads(door.must(f"SELECT count(*) AS n FROM {TABLE}"))[0]["n"]
    if counted != table.rows():
        door.fail(f"the node counts {counted} rows, the log {table.rows()}")
    return newest, dvs[0]


def stage(table, root):
    """Copies the table's log (commits, checkpoints, `_last_checkpoint`) and every data file an add names that is there,
    into root/orders/. Returns the names the adds name and those that aren't there."""
    dest = os.path.join(root, TABLE)
    os.makedirs(os.path.join(dest, "_delta_log"))
    for name in sorted(os.listdir(table.log())):
        if KEPT.fullmatch(name):
            shutil.copyfile(os.path.join(table.log(), name), os.path.join(dest, "_delta_log", name))
        else:
            print(f"  not copied (not part of the log): {name}", file=sys.stderr)
    names = sorted({a["add"]["path"] for a in table.actions() if "add" in a})
    for name in names:
        if not name or name.startswith("/") or "://" in name or ".." in name.split("/"):
            raise SystemExit(f"web_lake: an add names {name!r}, which is not a path from the table's folder")
    missing = []
    for name in names:
        src = os.path.join(table.folder(), name)
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(dest, name))
        else:
            missing.append(name)
    return names, missing


def read_answers(door, root, newest, dv_file):
    """The node's answers for the staged copy: the latest version, every version, and the one data file (CSV, or the
    body of a refusal)."""
    at = os.path.abspath(os.path.join(root, TABLE))

    def read(clause, label):
        status, body = door.sql(f"SELECT * FROM read_delta('{at}'{clause}) ORDER BY id", fmt="csv")
        if status == 200:
            return body
        print(f"  {label} refused ({status}): {body[:200]}", file=sys.stderr)
        return {"error": body}

    versions = {"latest": read("", "the latest version")}
    for v in range(newest + 1):
        versions[str(v)] = read(f", version => {v}", f"version {v}")
    status, body = door.sql(f"SELECT count(*) AS n FROM '{os.path.join(at, dv_file)}'", fmt="csv")
    if status != 200:
        door.fail(f"the node could not count {dv_file}: {status} {body[:300]}")
    return {"versions": versions, "file": {"name": dv_file, "out": body}}


def check_answers(versions, delete, rename):
    """Id 5 is in the version before its DELETE and in none after it; `amount` is the name from the RENAME on."""
    def rows(v):
        return versions[v].splitlines() if isinstance(versions[v], str) else []
    if not any(r.startswith("5,") for r in rows(str(delete - 1))):
        raise SystemExit("web_lake: id 5 is not in the version before its DELETE")
    if any(r.startswith("5,") for k in versions for r in rows(k) if k == "latest" or int(k) >= delete):
        raise SystemExit("web_lake: id 5 is still there after its DELETE")
    if rows("latest")[:1] != ["id,name,amount"] or rows(str(rename - 1))[:1] != ["id,name,qty"]:
        raise SystemExit("web_lake: the RENAME COLUMN does not show in the versions' headers")


def install(root, out, expected):
    """Replaces the old fixture: orders/ and expected.json under `out`."""
    os.makedirs(out, exist_ok=True)
    shutil.rmtree(os.path.join(out, TABLE), ignore_errors=True)
    shutil.copytree(os.path.join(root, TABLE), os.path.join(out, TABLE))
    with open(os.path.join(out, "expected.json"), "w") as f:
        json.dump(expected, f, indent=1, ensure_ascii=False)
        f.write("\n")


def fixture_bytes(out):
    total = os.path.getsize(os.path.join(out, "expected.json"))
    for d, _, files in os.walk(os.path.join(out, TABLE)):
        total += sum(os.path.getsize(os.path.join(d, f)) for f in files)
    return total


def main():
    ap = argparse.ArgumentParser(description="Makes the web lake fixture (see the module docstring).")
    ap.add_argument("--pondra", required=True, help="the node binary")
    ap.add_argument("--out", default=os.path.join(REPO, "web", "lake"), help="the fixture folder (orders/ and expected.json are replaced)")
    ap.add_argument("--port", type=int, default=18720, help="the node's port (18700 to 18799)")
    args = ap.parse_args()
    harness.BIN = os.path.abspath(args.pondra)
    if not os.access(harness.BIN, os.X_OK):
        raise SystemExit(f"web_lake: {args.pondra} is not an executable node binary")
    key = secrets.token_hex(16)  # (an owner key: 16 characters at least)
    lake = tempfile.mkdtemp(prefix="web-lake-")
    root = tempfile.mkdtemp(prefix="web-lake-out-")
    node = None
    try:
        # Every version is kept for an hour: the files an older version names stay on disk while it is copied.
        node = harness.Node(lake, args.port, env={"PONDRA_OWNER_KEY": key}, tier_secs=0.5, retain_secs=3600).start()
        door, table = Door(node, key), Table(lake)
        delete, rename = publish(door, table)
        settle(door, table)
        newest, dv_file = check_log(door, table, delete, rename)
        names, missing = stage(table, root)
        expected = read_answers(door, root, newest, dv_file)
        check_answers(expected["versions"], delete, rename)
        install(root, args.out, expected)
        refused = [k for k, v in expected["versions"].items() if isinstance(v, dict)]
        size = fixture_bytes(args.out)
        print(f"web_lake: versions 0-{newest} ({newest + 1} Delta versions, checkpoint at {CHECKPOINT}, deletion vector on {dv_file}, "
              f"column mapping from version {rename}), {len(names)} data files ({len(names) - len(missing)} copied, {len(missing)} not there), "
              f"{size} bytes ({'over' if size > 200_000 else 'under'} 200 KB) in {os.path.join(args.out, TABLE)} and expected.json, "
              f"refused: {', '.join(refused) or 'none'}, the table's folder {os.path.relpath(table.folder(), lake)}/ "
              f"with its _delta_log/ inside")
    finally:
        if node is not None:
            node.kill()
        shutil.rmtree(lake, ignore_errors=True)
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
