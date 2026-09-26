#!/usr/bin/env python3
"""A Nexmark subset: the same bid stream through Pondra and Flink, five of Nexmark's queries each.

Nexmark (the streaming benchmark Flink, Beam and RisingWave use) is an auction site's events.
Bids are 92% of them; this runs its queries over bids:

  q1  currency conversion   every bid, its price in euros              (row by row)
  q2  selection             the bids of a few auctions                 (a filter)
  q5  hot items             bids per auction, 10 s windows every 2 s   (sliding windows)
  q7  highest bid           the top price of each 10 s window          (tumbling windows)
  q11 user sessions         bids per bidder session, 3 s gap           (session windows)

Bids are a pure function of their number (`BID`), so both engines see the same events: Flink
makes them in its own process (datagen: no ingest cost at all), Pondra takes them over HTTP as
Arrow batches from a client, as a real stream arrives. Time: from the first bid until every
query's last row is out (windows and sessions closed by the stream's own end). Pondra's answers
are checked against DuckDB's over the same bids.

  nexmark.py [--bids 2000000] [--engines pondra,flink]   (Flink: /home/claude/venv-flink, PyFlink 2.3)
"""
import argparse, json, os, shutil, subprocess, sys, tempfile, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

BASE = 1_790_000_000_000  # (ms: the stream's first event time)
PER_MS = 10               # (10 bids per millisecond of event time: 10,000 a second)
BID = {  # column -> its value for bid `id` (Flink's SQL; Python below does the same)
    "auction": "id % 1000",
    "bidder": "(id / 100) % 1000",  # (a bidder bids 100 times in a row, then 10 s later again)
    "price": "CAST((id * 48271) % 10000 AS DOUBLE) / 100 + 1",
}
QUERIES = ["q1", "q2", "q5", "q7", "q11"]


def bids(start, end):
    import numpy as np, pyarrow as pa
    i = np.arange(start, end, dtype=np.int64)
    ts = (BASE + i // PER_MS) * 1000  # (µs)
    return pa.record_batch([pa.array(i % 1000), pa.array((i // 100) % 1000), pa.array(((i * 48271) % 10000) / 100 + 1),
                            pa.array(ts, pa.timestamp("us"))], names=["auction", "bidder", "price", "ts"])


def pondra(n, port=8290):
    import io, pyarrow as pa, harness
    harness.A = argparse.Namespace(s3=False, port=port)
    lake = tempfile.mkdtemp(prefix="pondra-nexmark-")
    node = harness.Node(lake, port).start()
    q = lambda s: harness.sql(port, s)
    q("CREATE TABLE bids (auction BIGINT, bidder BIGINT, price DOUBLE, ts TIMESTAMP)")
    q("CREATE MATERIALIZED VIEW q1 AS SELECT auction, bidder, price * 0.908 AS euros, ts FROM bids")
    q("CREATE MATERIALIZED VIEW q2 AS SELECT auction, price FROM bids WHERE auction % 123 = 0")
    q("""CREATE MATERIALIZED VIEW q5 WITH (window = 'w', size_secs = 10, slide_secs = 2) AS
         SELECT date_bin(INTERVAL '2 seconds', ts) AS w, auction, count(*) AS n FROM bids GROUP BY 1, 2""")
    q("""CREATE MATERIALIZED VIEW q7 WITH (window = 'w', size_secs = 10) AS
         SELECT date_bin(INTERVAL '10 seconds', ts) AS w, max(price) AS top FROM bids GROUP BY 1""")
    q("CREATE MATERIALIZED VIEW q11 WITH (session = 'ts', gap_secs = 3) AS SELECT bidder, count(*) AS n FROM bids GROUP BY bidder")
    want = model(n)  # (DuckDB's answers, worked out before the clock starts)
    t0, step, seq = time.time(), 100_000, 0
    for start in range(0, n, step):
        buf = io.BytesIO()
        b = bids(start, min(n, start + step))
        with pa.ipc.new_stream(buf, b.schema) as w:
            w.write_batch(b)
        seq += 1
        harness.call(port, "POST", f"/append/bids?producer=nexmark&seq={seq}", buf.getvalue(), timeout=120, headers={"content-type": "application/vnd.apache.arrow.stream"})
    sent = time.time() - t0
    # The stream's end: one bid far later closes every window and session (Flink: its bounded
    # source's last watermark).
    end = bids(n + 10**7, n + 10**7 + 1)
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, end.schema) as w:
        w.write_batch(end)
    harness.call(port, "POST", f"/append/bids?producer=nexmark&seq={seq + 1}", buf.getvalue(), headers={"content-type": "application/vnd.apache.arrow.stream"})
    got = lambda: {"q1": q("SELECT count(*) AS n, round(sum(euros), 2) AS s FROM q1 WHERE ts < TIMESTAMP '2100-01-01'")[0],
                   "q2": q("SELECT count(*) AS n FROM q2")[0]["n"],
                   "q5": q("SELECT count(*) AS n, sum(n) AS s FROM q5_final")[0],
                   "q7": q("SELECT count(*) AS n, round(sum(top), 2) AS s FROM q7_final")[0],
                   "q11": q("SELECT count(*) AS n, sum(n) AS s FROM q11")[0]}
    deadline = time.time() + 600
    while (g := got()) != want and time.time() < deadline:
        time.sleep(0.2)
    secs = time.time() - t0
    node.kill()
    shutil.rmtree(lake, ignore_errors=True)
    return {"engine": "pondra", "bids": n, "secs": round(secs, 2), "bids_per_s": round(n / secs), "sent_s": round(sent, 2), "same_as_duckdb": g == want, "answers": g, "duckdb": want}


def model(n):
    """What each query's rows add up to, by DuckDB over the same bids (plus the closing bid)."""
    import duckdb, pyarrow as pa
    t = pa.Table.from_batches([bids(0, n), bids(n + 10**7, n + 10**7 + 1)])
    d = duckdb.connect()
    d.register("bids", t)
    one = lambda s: d.execute(s).fetchone()
    n1, s1 = one("SELECT count(*), round(sum(price * 0.908), 2) FROM bids")
    n2 = one("SELECT count(*) FROM bids WHERE auction % 123 = 0")[0]
    # (sliding: a bid is in the 5 windows starting at its 2 s pane and the 4 before; a window is
    # out once the stream passes its end — every window but those still open at the closing bid)
    w5 = d.execute("""SELECT count(*), sum(n) FROM (SELECT p - INTERVAL (k * 2) SECOND AS w, auction, count(*) AS n
        FROM (SELECT time_bucket(INTERVAL 2 SECOND, ts, TIMESTAMP '1970-01-01') AS p, auction FROM bids), range(5) r(k)
        GROUP BY 1, 2) WHERE w + INTERVAL 10 SECOND <= (SELECT max(ts) FROM bids)""").fetchone()
    w7 = d.execute("""SELECT count(*), round(sum(top), 2) FROM (SELECT time_bucket(INTERVAL 10 SECOND, ts, TIMESTAMP '1970-01-01') AS w, max(price) AS top
        FROM bids GROUP BY 1) WHERE w + INTERVAL 10 SECOND <= (SELECT max(ts) FROM bids)""").fetchone()
    sessions = d.execute("""SELECT count(*), sum(n) FROM (SELECT bidder, s, count(*) AS n FROM (SELECT bidder, sum(new) OVER (PARTITION BY bidder ORDER BY ts) AS s
        FROM (SELECT bidder, ts, CASE WHEN ts - lag(ts) OVER (PARTITION BY bidder ORDER BY ts) <= INTERVAL 3 SECOND THEN 0 ELSE 1 END AS new FROM bids))
        GROUP BY 1, 2) x""").fetchone()
    # (the closing bid's own session stays open)
    return {"q1": {"n": n1, "s": s1}, "q2": n2, "q5": {"n": w5[0], "s": int(w5[1])}, "q7": {"n": w7[0], "s": w7[1]}, "q11": {"n": sessions[0] - 1, "s": int(sessions[1]) - 1}}


def flink(n, venv="/home/claude/venv-flink/bin/python"):
    """Each query alone, then all five over one source (`flink_nexmark.py`)."""
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flink_nexmark.py")
    r = subprocess.run([venv, here, str(n), json.dumps(BID), str(BASE), str(PER_MS)], capture_output=True, text=True, timeout=1800)
    lines = [l for l in r.stdout.splitlines() if l.startswith("{")]
    return json.loads(lines[-1]) if lines else {"engine": "flink", "error": r.stderr[-1200:]}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bids", type=int, default=2_000_000)
    ap.add_argument("--engines", default="pondra,flink")
    a = ap.parse_args()
    for e in a.engines.split(","):
        print(json.dumps({"pondra": pondra, "flink": flink}[e](a.bids)), flush=True)
