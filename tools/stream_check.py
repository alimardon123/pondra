#!/usr/bin/env python3
"""Streams on their own time, measured: windows, sessions and a point-in-time join over one stream.

  stream_check.py [--events 2000000] [--users 20000] [--producers 4] [--s3]

A stream of clicks goes in as Arrow batches from several producers at once. Event time runs at
2,000 clicks a second with up to 5 s of disorder; a tenth of the users are active in any minute,
then quiet for nine, so each user's clicks fall into sessions. It goes once into a table with no
views, and once into one with three:

- a window view: clicks and spend per user per minute, each minute emitted once (30 s lateness);
- a session view: each user's sessions (30 s gap, 30 s lateness), each emitted once;
- an inline view giving each click its user's tier as of the click (`ASOF JOIN` over a table of
  tier changes, five per user).

What it proves: once a last click moves event time past everything, every click is in exactly one
emitted window and one emitted session (both add up to every click, and to the spend), and every
enriched click has the tier a model of the tier changes gives it. What it measures: clicks per
second in, with no views, with each view alone (on a table of its own) and with all three, and how
long after that last click the last window and session are out.
"""
import argparse, bisect, datetime, io, json, os, random, sys, threading, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pyarrow as pa, pyarrow.ipc
import harness
from harness import Node, call, sql

BASE = 1_790_000_000 // 3600 * 3600
RATE = 2000  # clicks per second of event time


def clicks(i0, n, users):
    """Clicks i0 … i0+n: click i happens at i / RATE s (less up to 5 s), by one of the users of that
    minute's tenth."""
    rng = random.Random(i0)
    ids = list(range(i0, i0 + n))
    t = [BASE * 1_000_000 + i * 1_000_000 // RATE - rng.randrange(5_000_000) for i in ids]
    group = [(i // RATE // 60) % 10 for i in ids]
    user = [f"u{(rng.randrange(users // 10) * 10 + g)}" for g in group]
    return pa.table({"id": ids, "user": user, "ts": pa.array(t, pa.timestamp("us")), "amount": [i % 7 for i in ids]})


def arrow(table):
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
    return sink.getvalue()


def send(port, table, stream, producers, batch):
    """The stream into `table`, `producers` at once, in rounds (each sends its next batch), so no
    producer runs ahead of the others in event time by more than a round. Clicks per second."""
    seqs = [0] * producers
    start = time.time()
    for r in range(0, stream.num_rows, batch * producers):
        def one(p):
            part = stream.slice(r + p * batch, batch)
            if part.num_rows:
                seqs[p] += 1
                call(port, "POST", f"/append/{table}?producer={table}-{p}&seq={seqs[p]}", arrow(part), timeout=600,
                     headers={"content-type": "application/vnd.apache.arrow.stream"})
        ts = [threading.Thread(target=one, args=(p,)) for p in range(producers)]
        [t.start() for t in ts]
        [t.join() for t in ts]
    return stream.num_rows / (time.time() - start)


def main():
    lake = harness.new_lake()
    port = A.port
    node = Node(lake, port, tier_secs=5).start()
    q = lambda s: sql(port, s)
    alone = {"window view": "c_window", "session view": "c_session", "as-of view": "c_asof"}
    for t in ("plain", "clicks", *alone.values()):
        q(f"CREATE TABLE {t} (id BIGINT, user VARCHAR, ts TIMESTAMP, amount BIGINT)")
    q("CREATE TABLE tiers (user VARCHAR, ts TIMESTAMP, tier VARCHAR)")
    span = A.events // RATE
    rng = random.Random(7)
    changes = {f"u{u}": sorted(rng.sample(range(-60, span), 5)) for u in range(A.users)}  # (no two at once: which of them is "as of" then is anyone's guess)
    rows = [{"user": u, "ts": BASE * 1_000_000 + s * 1_000_000, "tier": f"t{k}"} for u, ss in changes.items() for k, s in enumerate(ss)]
    call(port, "POST", "/append/tiers?producer=tiers&seq=1", arrow(pa.Table.from_pylist(rows, schema=pa.schema([("user", pa.string()), ("ts", pa.timestamp("us")), ("tier", pa.string())]))),
         headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=600)
    views = {"window view": ("per_minute", "window=w&size_secs=60&lateness_secs=30", "SELECT date_bin(INTERVAL '1 minute', ts) AS w, user, count(*) AS n, sum(amount) AS spent FROM {t} GROUP BY 1, 2"),
             "session view": ("visits", "session=ts&gap_secs=30&lateness_secs=30", "SELECT user, count(*) AS n, sum(amount) AS spent FROM {t} GROUP BY user"),
             "as-of view": ("tiered", "", "SELECT c.id, c.user, c.ts, t.tier FROM {t} c ASOF JOIN tiers t MATCH_CONDITION (c.ts >= t.ts) ON c.user = t.user")}
    for what, (name, params, view) in views.items():  # all three on `clicks`, and each alone on a table of its own
        call(port, "POST", f"/views/{name}?{params}", view.format(t="clicks").encode())
        call(port, "POST", f"/views/{name}_alone?{params}", view.format(t=alone[what]).encode())
    stream = pa.concat_tables([clicks(i, min(100_000, A.events - i), A.users) for i in range(0, A.events, 100_000)])
    rates = {"no views": send(port, "plain", stream, A.producers, A.batch)}
    for what, t in alone.items():
        rates[what] = send(port, t, stream, A.producers, A.batch)
    rates["all three"] = send(port, "clicks", stream, A.producers, A.batch)
    last = time.time()
    closing = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(BASE + span + 3600))
    call(port, "POST", "/append/clicks?producer=closer&seq=1", json.dumps({"id": -1, "user": "closer", "ts": closing, "amount": 0}).encode())
    total, spent = A.events, sum(stream.column("amount").to_pylist())
    want = lambda: (total, spent)
    done, waited = {}, {}
    while time.time() - last < 600 and len(done) < 2:
        for name, table in (("windows", "per_minute_final"), ("sessions", "visits")):
            got = q(f"SELECT sum(n) AS n, sum(spent) AS s FROM {table} WHERE user <> 'closer'")[0]
            if name not in done and (got.get("n"), got.get("s")) == want():
                done[name], waited[name] = True, round(time.time() - last, 2)
        time.sleep(0.2)
    sessions = q("SELECT count(*) AS n, max(session_end - session_start) AS longest FROM visits")[0]
    # Tiers: a sample of clicks against the model (the change at or before the click).
    ids, users, ts = (stream.column(c).to_pylist() for c in ("id", "user", "ts"))
    sample = [{"id": ids[i], "user": users[i], "ts": ts[i]} for i in random.Random(1).sample(range(total), 2000)]
    got = {r["id"]: r.get("tier") for r in q(f"SELECT id, tier FROM tiered WHERE id IN ({','.join(str(r['id']) for r in sample)})")}
    def model(r):
        ss = changes[r["user"]]
        at = (r["ts"] - datetime.datetime(1970, 1, 1)) // datetime.timedelta(microseconds=1)
        k = bisect.bisect_right([BASE * 1_000_000 + s * 1_000_000 for s in ss], at) - 1
        return f"t{k}" if k >= 0 else None
    enriched = q("SELECT count(*) AS n FROM tiered WHERE id >= 0")[0]["n"]
    node.kill()
    checks = {
        "every click in exactly one emitted window": done.get("windows", False),
        "every click in exactly one emitted session": done.get("sessions", False),
        "every click enriched once, with its tier as of its time": enriched == total and all(got.get(r["id"]) == model(r) for r in sample),
    }
    result = {"events": A.events, "users": A.users, "producers": A.producers,
              "clicks_per_s": {k: round(v) for k, v in rates.items()},
              "seconds_after_the_last_click": waited, "sessions": sessions["n"], "checks": checks, "ok": all(checks.values())}
    print(json.dumps(result, indent=1))
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=2_000_000)
    ap.add_argument("--users", type=int, default=20_000)
    ap.add_argument("--producers", type=int, default=4)
    ap.add_argument("--batch", type=int, default=5000, help="clicks per append (per producer, per round)")
    ap.add_argument("--port", type=int, default=8230)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true")
    A = ap.parse_args()
    harness.A = A
    main()
