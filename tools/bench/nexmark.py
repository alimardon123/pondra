#!/usr/bin/env python3
"""Nexmark, the streaming benchmark Flink, RisingWave and Arroyo publish: an auction site's people,
auctions and bids, and the queries Flink's own suite runs over them.

The three streams have one definition (`STREAMS`), rendered as Flink SQL over a datagen sequence
and as the DuckDB SQL that makes the rows Pondra is sent, so both engines see the same events. In
each block of 50 events come a person, three auctions, then 46 bids (Nexmark's proportions). A bid
goes to one of the last 100 auctions, by one of the last 50 people. Flink makes the events in its
own process (datagen: no ingest cost). Pondra takes them over HTTP as Arrow batches, as a real
stream arrives. The time is from the first event until every query's answer is out, with windows
and sessions closed by the stream's end: `secs` with every query at once (Flink: one job of all of
them over one set of sources; Pondra: every view on one node), `alone_s` each by itself (Flink's
job start included, its JVM's not; Pondra with `--alone`), and Pondra's `done_s`, when each
answer was right while all of them ran. Pondra's answers are checked against DuckDB's over the
same events, and Flink's streams against DuckDB's rows (`--check`).

Three of Flink's queries are left out, each for a reason other than SQL: q6 (Flink doesn't run
it), q10 (it writes files), q12 (processing-time windows: answers depend on the machine's clock).

  nexmark.py [--events 10000000] [--engines pondra,flink] [--only q3,q9] [--alone] [--check] [--flink-state rocksdb|heap]
  (Flink: PyFlink 2.3 in $FLINK_PYTHON, or /home/claude/venv-flink; its state in RocksDB as Flink's own Nexmark
  setup keeps it, or on the heap, faster alone, which at 10M events on 15 GB failed with all twenty at once)
"""
import argparse, io, json, os, shutil, subprocess, sys, tempfile, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

BASE_MS = 1_790_000_000_000  # (the stream's first event time)
PER_MS = 10                  # (events per millisecond of event time: 10,000 a second)


class Dialect:
    """What the two engines spell differently. Times: Flink's are milliseconds; DuckDB's (Pondra's
    rows) microseconds, each event its own, so a newest or first row is never a tie."""
    def __init__(self, flink):
        self.flink = flink

    def div(self, a, b):
        return f"(({a}) / {b})" if self.flink else f"(({a}) // {b})"

    def time(self, e):
        return f"TO_TIMESTAMP_LTZ({BASE_MS} + {self.div(e, PER_MS)}, 3)" if self.flink else f"make_timestamp({BASE_MS * 1000} + ({e}) * {1000 // PER_MS})"

    def text(self, x):
        return f"CAST({x} AS {'STRING' if self.flink else 'VARCHAR'})"


def pick(x, values):
    """`x`-th of `values` (x from 0)."""
    show = lambda v: f"'{v}'" if isinstance(v, str) else str(v)
    return "(CASE " + " ".join(f"WHEN {x} = {i} THEN {show(v)}" for i, v in enumerate(values[:-1])) + f" ELSE {show(values[-1])} END)"


FIRST = ["Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie"]
LAST = ["Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones"]
CITIES = ["Phoenix", "Los Angeles", "San Francisco", "Boise", "Portland", "Bend", "Redmond", "Seattle", "Kent", "Cheyenne"]
STATES = ["AZ", "CA", "ID", "OR", "WA", "WY", "UT"]

# Each stream: its share of a block of 50, the event number of its k-th row, and its columns (type,
# SQL over k).
STREAMS = {
    "person": (1, lambda d: "k * 50", {
        "id": ("BIGINT", lambda d: "1000 + k"),
        "name": ("VARCHAR", lambda d: f"{pick('k % 8', FIRST)} || ' ' || {pick(d.div('k', 8) + ' % 8', LAST)}"),
        "email_address": ("VARCHAR", lambda d: f"'user' || {d.text('k')} || '@nexmark.com'"),
        "credit_card": ("VARCHAR", lambda d: d.text("4000000000000000 + k * 7919")),
        "city": ("VARCHAR", lambda d: pick("k % 10", CITIES)),
        "state": ("VARCHAR", lambda d: pick("(k * 3) % 7", STATES)),
        "extra": ("VARCHAR", lambda d: pick("k % 3", ["", "xx", "note"])),
    }),
    "auction": (3, lambda d: f"{d.div('k', 3)} * 50 + 1 + k % 3", {
        "id": ("BIGINT", lambda d: "1000 + k"),
        "item_name": ("VARCHAR", lambda d: f"'item-' || {d.text('k')}"),
        "description": ("VARCHAR", lambda d: f"'description of item ' || {d.text('k % 1000')}"),
        "initial_bid": ("BIGINT", lambda d: "100 + (k * 31) % 900"),
        "reserve": ("BIGINT", lambda d: "100 + (k * 31) % 900 + (k * 17) % 1000"),
        "expires": ("TIMESTAMP", lambda d: d.time(f"{d.div('k', 3)} * 50 + 1 + k % 3 + (1 + k % 10) * {1000 * PER_MS}")),  # (1 to 10 s on)
        "seller": ("BIGINT", lambda d: f"1000 + GREATEST(0, {d.div('k', 3)} - (k * 13) % 100)"),
        "category": ("BIGINT", lambda d: "10 + k % 5"),
        "extra": ("VARCHAR", lambda d: "''"),
    }),
    "bid": (46, lambda d: f"{d.div('k', 46)} * 50 + 4 + k % 46", {
        "auction": ("BIGINT", lambda d: f"1000 + GREATEST(0, 3 * {d.div('k', 46)} + 2 - (k * 7) % 100)"),
        "bidder": ("BIGINT", lambda d: f"1000 + GREATEST(0, {d.div('k', 46)} - (k * 11) % 50)"),
        "price": ("BIGINT", lambda d: f"(k * 48271) % {pick('k % 3', [10000, 1000000, 100000000])}"),
        "channel": ("VARCHAR", lambda d: pick("k % 6", ["apple", "google", "facebook", "baidu", "partner", "Google"])),
        "url": ("VARCHAR", lambda d: f"'https://www.nexmark.com/' || {pick('k % 7', ['a', 'bb', 'ccc', 'dd', 'e', 'ff', 'g'])} || '/' || "
                                     f"{pick('k % 5', ['x', 'yy', 'z', 'ww', 'v'])} || '/' || {pick('k % 3', ['item', 'lot', 'deal'])} || "
                                     f"'/item.htm?query=1' || (CASE WHEN k % 4 = 0 THEN '' ELSE '&channel_id=' || {d.text('k % 100')} END)"),
        "extra": ("VARCHAR", lambda d: pick("k % 4", ["", "cc", "abcabc", "ccccc"])),
    }),
}


def columns(stream, d):
    """`(name, type, sql)` of a stream's columns, its event time (`date_time`) among them."""
    per, event, cols = STREAMS[stream]
    out = [(c, t, f"CAST({f(d)} AS BIGINT)" if t == "BIGINT" else f(d)) for c, (t, f) in cols.items()]  # (Flink's MOD has its divisor's type)
    return out + [("date_time", "TIMESTAMP", d.time(event(d)))]


def counts(events):
    """Rows of each stream in `events` (whole blocks of 50)."""
    return {s: (events // 50) * per for s, (per, _, _) in STREAMS.items()}


def duck_rows(stream, start, end):
    """Rows `start` to `end` of a stream, by DuckDB, as Arrow."""
    import duckdb
    cols = ", ".join(f"{sql} AS {c}" for c, _, sql in columns(stream, Dialect(False)))
    return duckdb.connect().execute(f"SELECT {cols} FROM range({start}, {end}) r(k)").to_arrow_table()


CLOSE_MS = 10**8  # (the closing events come this much event time after the stream: a day and more)


def closing():
    """One event of each stream long after the rest: it moves every watermark past the last
    window and session. It meets nothing in a join (its person sells nothing, its bid is on no
    auction), and its own window and session stay open."""
    import pyarrow as pa
    ts = pa.array([(BASE_MS + CLOSE_MS) * 1000] * 1, pa.timestamp("us"))
    s, i = pa.array(["close"]), lambda v: pa.array([v], pa.int64())
    return {
        "person": pa.table({"id": i(1), "name": s, "email_address": s, "credit_card": s, "city": s, "state": s, "extra": s, "date_time": ts}),
        "auction": pa.table({"id": i(2), "item_name": s, "description": s, "initial_bid": i(1), "reserve": i(1), "expires": ts, "seller": i(999999),
                             "category": i(0), "extra": s, "date_time": ts}),
        "bid": pa.table({"auction": i(999998), "bidder": i(1), "price": i(1), "channel": pa.array(["apple"]), "url": s, "extra": s, "date_time": ts}),
    }


# The queries, after Flink's suite (github.com/nexmark/nexmark), as each engine takes them:
# `flink` (a SELECT into a blackhole sink), `pondra` (the views that keep it; `read` the one to read),
# `duck` (the same answer from all the rows; `regular`: over the stream without its closing events,
# as windows and sessions are emitted only once closed), and `fp`, what is compared (both engines
# run it over their answer).
CHANNEL_ID = "(&|^)channel_id=([^&]*)"
QUERIES = {
    "q0": dict(  # pass through
        flink="SELECT auction, bidder, price, date_time, extra FROM bid",
        pondra=["CREATE MATERIALIZED VIEW q0 AS SELECT auction, bidder, price, date_time, extra FROM bid"],
        duck="SELECT auction, bidder, price, date_time, extra FROM bid",
        fp="count(*) AS n, sum(auction) AS a, sum(price) AS p, sum(length(extra)) AS x"),
    "q1": dict(  # currency conversion
        flink="SELECT auction, bidder, 0.908 * price AS price, date_time, extra FROM bid",
        pondra=["CREATE MATERIALIZED VIEW q1 AS SELECT auction, bidder, 0.908 * price AS price, date_time, extra FROM bid"],
        duck="SELECT auction, bidder, 0.908 * price AS price, date_time, extra FROM bid",
        fp="count(*) AS n, sum(CAST(price * 1000 AS BIGINT)) AS p"),
    "q2": dict(  # selection
        flink="SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0",
        pondra=["CREATE MATERIALIZED VIEW q2 AS SELECT auction, price FROM bid WHERE auction % 123 = 0"],
        duck="SELECT auction, price FROM bid WHERE auction % 123 = 0",
        fp="count(*) AS n, sum(auction) AS a, sum(price) AS p"),
    "q3": dict(  # local item suggestion: a join of two streams
        flink="SELECT P.name, P.city, P.state, A.id FROM auction AS A INNER JOIN person AS P ON A.seller = P.id "
              "WHERE A.category = 10 AND (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA')",
        pondra=["CREATE MATERIALIZED VIEW q3 WITH (join = 'streams') AS SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id "
                "WHERE a.category = 10 AND p.state IN ('OR', 'ID', 'CA')"],
        duck="SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE a.category = 10 AND p.state IN ('OR', 'ID', 'CA')",
        fp="count(*) AS n, sum(id) AS i, sum(length(name)) AS l"),
    "q4": dict(  # average price for a category: an aggregate of an aggregate of a join
        flink="SELECT Q.category, AVG(Q.final) AS avg_final FROM (SELECT MAX(B.price) AS final, A.category FROM auction A, bid B "
              "WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires GROUP BY A.id, A.category) Q GROUP BY Q.category",
        pondra=["CREATE MATERIALIZED VIEW q4 AS SELECT q.category, avg(q.final) AS avg_final FROM (SELECT max(b.price) AS final, a.category "
                "FROM auction a JOIN bid b ON a.id = b.auction AND b.date_time BETWEEN a.date_time AND a.expires GROUP BY a.id, a.category) q GROUP BY q.category"],
        duck="SELECT q.category, avg(q.final) AS avg_final FROM (SELECT max(b.price) AS final, a.category "
             "FROM auction a JOIN bid b ON a.id = b.auction AND b.date_time BETWEEN a.date_time AND a.expires GROUP BY a.id, a.category) q GROUP BY q.category",
        fp="count(*) AS n, sum(category) AS c, round(sum(avg_final), 3) AS f"),
    "q5": dict(  # hot items: the most bid-on auctions of each sliding window
        flink="SELECT AuctionBids.auction, AuctionBids.num FROM (SELECT auction, count(*) AS num, window_start AS starttime, window_end AS endtime "
              "FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) GROUP BY auction, window_start, window_end) AS AuctionBids "
              "JOIN (SELECT max(CountBids.num) AS maxn, CountBids.starttime, CountBids.endtime FROM (SELECT count(*) AS num, window_start AS starttime, window_end AS endtime "
              "FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) GROUP BY auction, window_start, window_end) AS CountBids "
              "GROUP BY CountBids.starttime, CountBids.endtime) AS MaxBids "
              "ON AuctionBids.starttime = MaxBids.starttime AND AuctionBids.endtime = MaxBids.endtime AND AuctionBids.num >= MaxBids.maxn",
        pondra=["CREATE MATERIALIZED VIEW q5_counts WITH (window = 'w', size_secs = 10, slide_secs = 2) AS "
                "SELECT date_bin(INTERVAL '2 seconds', date_time) AS w, auction, count(*) AS num FROM bid GROUP BY 1, 2",
                "CREATE MATERIALIZED VIEW q5 AS SELECT c.w, c.auction, c.num FROM q5_counts_final c "
                "JOIN (SELECT w, max(num) AS maxn FROM q5_counts_final GROUP BY w) m ON c.w = m.w AND c.num >= m.maxn"],
        regular=True,
        duck="WITH c AS (SELECT p - INTERVAL (k * 2) SECOND AS w, auction, count(*) AS num "
             "FROM (SELECT time_bucket(INTERVAL 2 SECOND, date_time, TIMESTAMP '1970-01-01') AS p, auction FROM bid), range(5) r(k) GROUP BY 1, 2) "
             "SELECT c.w, c.auction, c.num FROM c JOIN (SELECT w, max(num) AS maxn FROM c GROUP BY w) m ON c.w = m.w AND c.num >= m.maxn",
        fp="count(*) AS n, sum(auction) AS a, sum(num) AS s"),
    "q7": dict(  # highest bid of each window, and who made it
        flink="SELECT B.auction, B.price, B.bidder, B.date_time, B.extra FROM bid B JOIN (SELECT MAX(price) AS maxprice, window_end AS date_time "
              "FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(date_time), INTERVAL '10' SECOND)) GROUP BY window_start, window_end) B1 "
              "ON B.price = B1.maxprice WHERE B.date_time BETWEEN B1.date_time - INTERVAL '10' SECOND AND B1.date_time",
        pondra=["CREATE MATERIALIZED VIEW q7_top AS SELECT date_bin(INTERVAL '10 seconds', date_time) AS w, max(price) AS maxprice FROM bid GROUP BY 1 EMIT FINAL",
                "CREATE MATERIALIZED VIEW q7 WITH (join = 'streams') AS SELECT b.auction, b.price, b.bidder, b.date_time, b.extra FROM bid b JOIN q7_top m "
                "ON b.price = m.maxprice AND b.date_time BETWEEN m.w AND m.w + INTERVAL '10 seconds'"],
        duck="SELECT b.auction, b.price, b.bidder, b.date_time, b.extra FROM bid b JOIN (SELECT time_bucket(INTERVAL 10 SECOND, date_time, TIMESTAMP '1970-01-01') AS w, "
             "max(price) AS maxprice FROM bid GROUP BY 1) m ON b.price = m.maxprice AND b.date_time BETWEEN m.w AND m.w + INTERVAL 10 SECOND",
        regular=True,
        fp="count(*) AS n, sum(auction) AS a, sum(price) AS p"),
    "q8": dict(  # monitor new users: people who sold something in the 10 s they joined
        flink="SELECT P.id, P.name, P.starttime FROM (SELECT id, name, window_start AS starttime, window_end AS endtime "
              "FROM TABLE(TUMBLE(TABLE person, DESCRIPTOR(date_time), INTERVAL '10' SECOND)) GROUP BY id, name, window_start, window_end) P "
              "JOIN (SELECT seller, window_start AS starttime, window_end AS endtime FROM TABLE(TUMBLE(TABLE auction, DESCRIPTOR(date_time), INTERVAL '10' SECOND)) "
              "GROUP BY seller, window_start, window_end) A ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime",
        pondra=["CREATE MATERIALIZED VIEW q8 AS SELECT p.id, p.name, p.w AS starttime FROM "
                "(SELECT id, name, date_bin(INTERVAL '10 seconds', date_time) AS w FROM person GROUP BY 1, 2, 3) p JOIN "
                "(SELECT seller, date_bin(INTERVAL '10 seconds', date_time) AS w FROM auction GROUP BY 1, 2) a ON p.id = a.seller AND p.w = a.w"],
        duck="SELECT p.id, p.name, p.w AS starttime FROM (SELECT id, name, time_bucket(INTERVAL 10 SECOND, date_time, TIMESTAMP '1970-01-01') AS w FROM person GROUP BY 1, 2, 3) p "
             "JOIN (SELECT seller, time_bucket(INTERVAL 10 SECOND, date_time, TIMESTAMP '1970-01-01') AS w FROM auction GROUP BY 1, 2) a ON p.id = a.seller AND p.w = a.w",
        fp="count(*) AS n, sum(id) AS i, sum(length(name)) AS l"),
    "q9": dict(  # winning bids: each auction's highest bid while it was open
        flink="SELECT id, item_name, description, initial_bid, reserve, date_time, expires, seller, category, extra, auction, bidder, price, bid_date_time, bid_extra FROM "
              "(SELECT A.*, B.auction, B.bidder, B.price, B.date_time AS bid_date_time, B.extra AS bid_extra, "
              "ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.date_time ASC) AS rownum "
              "FROM auction A, bid B WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires) WHERE rownum <= 1",
        pondra=["CREATE MATERIALIZED VIEW q9 AS SELECT id, item_name, seller, category, auction, bidder, price, bid_date_time FROM "
                "(SELECT a.id, a.item_name, a.seller, a.category, b.auction, b.bidder, b.price, b.date_time AS bid_date_time, "
                "row_number() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.date_time ASC) AS rownum "
                "FROM auction a JOIN bid b ON a.id = b.auction AND b.date_time BETWEEN a.date_time AND a.expires) WHERE rownum <= 1"],
        duck="SELECT id, item_name, seller, category, auction, bidder, price, bid_date_time FROM "
             "(SELECT a.id, a.item_name, a.seller, a.category, b.auction, b.bidder, b.price, b.date_time AS bid_date_time, "
             "row_number() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.date_time ASC) AS rownum "
             "FROM auction a JOIN bid b ON a.id = b.auction AND b.date_time BETWEEN a.date_time AND a.expires) WHERE rownum <= 1",
        fp="count(*) AS n, sum(id) AS i, sum(price) AS p, sum(bidder) AS b"),
    "q11": dict(  # user sessions: bids per bidder session, 10 s gap
        flink="SELECT B.bidder, count(*) AS bid_count, SESSION_START(B.date_time, INTERVAL '10' SECOND) AS starttime, "
              "SESSION_END(B.date_time, INTERVAL '10' SECOND) AS endtime FROM bid B GROUP BY B.bidder, SESSION(B.date_time, INTERVAL '10' SECOND)",
        pondra=["CREATE MATERIALIZED VIEW q11 AS SELECT bidder, count(*) AS bid_count FROM bid GROUP BY bidder, SESSION(date_time, INTERVAL '10 seconds') EMIT FINAL"],
        regular=True,
        duck="SELECT bidder, s, count(*) AS bid_count FROM (SELECT bidder, sum(new) OVER (PARTITION BY bidder ORDER BY date_time) AS s FROM "
             "(SELECT bidder, date_time, CASE WHEN date_time - lag(date_time) OVER (PARTITION BY bidder ORDER BY date_time) <= INTERVAL 10 SECOND THEN 0 ELSE 1 END AS new FROM bid)) "
             "GROUP BY 1, 2",
        fp="count(*) AS n, sum(bidder) AS b, sum(bid_count) AS c"),
    "q13": dict(  # a bounded side input: each bid with a value looked up by its auction
        # (Flink: a regular join. Nexmark's lookup join, FOR SYSTEM_TIME AS OF PROCTIME(), needs a lookup
        # source, and Flink 2.3's filesystem source isn't one.)
        flink="SELECT B.auction, B.bidder, B.price, B.date_time, S.label FROM bid B JOIN side_input S ON MOD(B.auction, 10000) = S.id",
        pondra=["CREATE MATERIALIZED VIEW q13 AS SELECT b.auction, b.bidder, b.price, b.date_time, s.label FROM bid b JOIN side_input s ON b.auction % 10000 = s.id"],
        duck="SELECT b.auction, b.bidder, b.price, b.date_time, s.label FROM bid b JOIN side_input s ON b.auction % 10000 = s.id",
        fp="count(*) AS n, sum(auction) AS a, sum(length(label)) AS l"),
    "q14": dict(  # calculation: a filter, a CASE and a count of characters
        flink="SELECT auction, bidder, 0.908 * price AS price, CASE WHEN HOUR(date_time) >= 8 AND HOUR(date_time) <= 18 THEN 'dayTime' "
              "WHEN HOUR(date_time) <= 6 OR HOUR(date_time) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, date_time, extra, "
              "CHAR_LENGTH(extra) - CHAR_LENGTH(REPLACE(extra, 'c', '')) AS c_counts FROM bid WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000",
        pondra=["CREATE MATERIALIZED VIEW q14 AS SELECT auction, bidder, 0.908 * price AS price, CASE WHEN extract(hour FROM date_time) >= 8 AND extract(hour FROM date_time) <= 18 "
                "THEN 'dayTime' WHEN extract(hour FROM date_time) <= 6 OR extract(hour FROM date_time) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, "
                "date_time, extra, char_length(extra) - char_length(replace(extra, 'c', '')) AS c_counts FROM bid WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000"],
        duck="SELECT auction, bidder, 0.908 * price AS price, CASE WHEN hour(date_time) >= 8 AND hour(date_time) <= 18 THEN 'dayTime' "
             "WHEN hour(date_time) <= 6 OR hour(date_time) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, date_time, extra, "
             "length(extra) - length(replace(extra, 'c', '')) AS c_counts FROM bid WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000",
        fp="count(*) AS n, sum(CAST(price * 1000 AS BIGINT)) AS p, sum(c_counts) AS c, sum(length(bid_time_type)) AS t"),
}

RANKS = [("rank1", "price < 10000"), ("rank2", "price >= 10000 AND price < 1000000"), ("rank3", "price >= 1000000")]


def stats(day, keys=""):
    """q15–q16's select list: bids, bidders and auctions a day, in all and by price rank."""
    out = [f"{day} AS bid_day{keys}", "count(*) AS total_bids"] + [f"count(*) FILTER (WHERE {w}) AS {r}_bids" for r, w in RANKS]
    for what, col in (("bidders", "bidder"), ("auctions", "auction")):
        out += [f"count(DISTINCT {col}) AS total_{what}"] + [f"count(DISTINCT {col}) FILTER (WHERE {w}) AS {r}_{what}" for r, w in RANKS]
    return ", ".join(out)


STATS_FP = "count(*) AS n, " + ", ".join(f"sum({c}) AS {c}" for c in ["total_bids", "rank1_bids", "rank2_bids", "rank3_bids", "total_bidders", "rank1_bidders",
                                                                        "rank2_bidders", "rank3_bidders", "total_auctions", "rank1_auctions", "rank2_auctions", "rank3_auctions"])
FLINK_DAY = "DATE_FORMAT(date_time, 'yyyy-MM-dd')"
DAY = "CAST(date_time AS DATE)"
MINUTE = {"flink": ", max(DATE_FORMAT(date_time, 'HH:mm')) AS bid_minute", "pondra": ", max(to_char(date_time, '%H:%M')) AS bid_minute",
          "duck": ", max(strftime(date_time, '%H:%M')) AS bid_minute"}
QUERIES.update({
    "q15": dict(  # bidding statistics a day: distinct counts, filtered
        flink=f"SELECT {stats(FLINK_DAY)} FROM bid GROUP BY {FLINK_DAY}",
        pondra=[f"CREATE MATERIALIZED VIEW q15 AS SELECT {stats(DAY)} FROM bid GROUP BY 1"],
        duck=f"SELECT {stats(DAY)} FROM bid GROUP BY 1",
        fp=STATS_FP),
    "q16": dict(  # channel statistics a day
        flink=f"SELECT channel, {stats(FLINK_DAY, MINUTE['flink'])} FROM bid GROUP BY channel, {FLINK_DAY}",
        pondra=[f"CREATE MATERIALIZED VIEW q16 AS SELECT channel, {stats(DAY, MINUTE['pondra'])} FROM bid GROUP BY 1, 2"],
        duck=f"SELECT channel, {stats(DAY, MINUTE['duck'])} FROM bid GROUP BY 1, 2",
        fp=STATS_FP + ", sum(length(channel)) AS ch"),
    "q17": dict(  # auction statistics a day
        flink="SELECT auction, DATE_FORMAT(date_time, 'yyyy-MM-dd') AS bid_day, count(*) AS total_bids, "
              + ", ".join(f"count(*) FILTER (WHERE {w}) AS {r}_bids" for r, w in RANKS)
              + ", min(price) AS min_price, max(price) AS max_price, avg(price) AS avg_price, sum(price) AS sum_price FROM bid GROUP BY auction, DATE_FORMAT(date_time, 'yyyy-MM-dd')",
        pondra=["CREATE MATERIALIZED VIEW q17 AS SELECT auction, CAST(date_time AS DATE) AS bid_day, count(*) AS total_bids, "
                + ", ".join(f"count(*) FILTER (WHERE {w}) AS {r}_bids" for r, w in RANKS)
                + ", min(price) AS min_price, max(price) AS max_price, avg(price) AS avg_price, sum(price) AS sum_price FROM bid GROUP BY 1, 2"],
        duck="SELECT auction, CAST(date_time AS DATE) AS bid_day, count(*) AS total_bids, " + ", ".join(f"count(*) FILTER (WHERE {w}) AS {r}_bids" for r, w in RANKS)
             + ", min(price) AS min_price, max(price) AS max_price, avg(price) AS avg_price, sum(price) AS sum_price FROM bid GROUP BY 1, 2",
        fp="count(*) AS n, sum(total_bids) AS t, sum(rank1_bids) AS r1, sum(rank2_bids) AS r2, sum(rank3_bids) AS r3, sum(min_price) AS lo, "
           "sum(max_price) AS hi, sum(sum_price) AS s, round(sum(avg_price), 1) AS av"),
    "q18": dict(  # each bidder's last bid on each auction
        flink="SELECT auction, bidder, price, channel, url, date_time, extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC) AS rank_number "
              "FROM bid) WHERE rank_number <= 1",
        pondra=["CREATE MATERIALIZED VIEW q18 AS SELECT auction, bidder, price, channel, url, date_time, extra FROM "
                "(SELECT *, row_number() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC) AS rank_number FROM bid) WHERE rank_number <= 1"],
        duck="SELECT auction, bidder, price, channel, url, date_time, extra FROM "
             "(SELECT *, row_number() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC) AS rank_number FROM bid) WHERE rank_number <= 1",
        fp="count(*) AS n, sum(auction) AS a, sum(price) AS p, sum(bidder) AS b"),
    "q19": dict(  # each auction's top ten bids
        flink="SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid) WHERE rank_number <= 10",
        pondra=["CREATE MATERIALIZED VIEW q19 AS SELECT * FROM (SELECT auction, bidder, price, channel, url, date_time, extra, "
                "row_number() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid) WHERE rank_number <= 10"],
        duck="SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid) WHERE rank_number <= 10",
        fp="count(*) AS n, sum(auction) AS a, sum(price) AS p"),
    "q20": dict(  # each bid with its auction, for one category: a join of two streams
        flink="SELECT auction, bidder, price, channel, url, B.date_time, B.extra, item_name, description, initial_bid, reserve, A.date_time AS a_date_time, expires, seller, "
              "category, A.extra AS a_extra FROM bid AS B INNER JOIN auction AS A ON B.auction = A.id WHERE A.category = 10",
        pondra=["CREATE MATERIALIZED VIEW q20 WITH (join = 'streams') AS SELECT b.auction, b.bidder, b.price, b.channel, b.url, b.date_time, b.extra, a.item_name, "
                "a.description, a.initial_bid, a.reserve, a.date_time AS a_date_time, a.expires, a.seller, a.category, a.extra AS a_extra "
                "FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10"],
        duck="SELECT b.auction, b.bidder, b.price, b.channel, b.url, b.date_time, b.extra, a.item_name, a.description, a.initial_bid, a.reserve, "
             "a.date_time AS a_date_time, a.expires, a.seller, a.category, a.extra AS a_extra FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10",
        fp="count(*) AS n, sum(auction) AS a, sum(price) AS p, sum(initial_bid) AS i"),
    "q21": dict(  # add a channel id: a CASE and a regular expression
        flink="SELECT auction, bidder, price, channel, CASE WHEN LOWER(channel) = 'apple' THEN '0' WHEN LOWER(channel) = 'google' THEN '1' "
              "WHEN LOWER(channel) = 'facebook' THEN '2' WHEN LOWER(channel) = 'baidu' THEN '3' "
              f"ELSE REGEXP_EXTRACT(url, '{CHANNEL_ID}', 2) END AS channel_id FROM bid "
              f"WHERE REGEXP_EXTRACT(url, '{CHANNEL_ID}', 2) IS NOT NULL OR LOWER(channel) IN ('apple', 'google', 'facebook', 'baidu')",
        pondra=["CREATE MATERIALIZED VIEW q21 AS SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' "
                "WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' "
                f"ELSE regexp_match(url, '{CHANNEL_ID}')[2] END AS channel_id FROM bid "
                f"WHERE regexp_match(url, '{CHANNEL_ID}')[2] IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')"],
        duck="SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' "
             "WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' "
             f"ELSE nullif(regexp_extract(url, '{CHANNEL_ID}', 2), '') END AS channel_id FROM bid "
             f"WHERE nullif(regexp_extract(url, '{CHANNEL_ID}', 2), '') IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')",
        fp="count(*) AS n, sum(auction) AS a, sum(length(channel_id)) AS c"),
    "q22": dict(  # a URL's folders
        flink="SELECT auction, bidder, price, channel, SPLIT_INDEX(url, '/', 3) AS dir1, SPLIT_INDEX(url, '/', 4) AS dir2, SPLIT_INDEX(url, '/', 5) AS dir3 FROM bid",
        pondra=["CREATE MATERIALIZED VIEW q22 AS SELECT auction, bidder, price, channel, split_part(url, '/', 4) AS dir1, split_part(url, '/', 5) AS dir2, "
                "split_part(url, '/', 6) AS dir3 FROM bid"],
        duck="SELECT auction, bidder, price, channel, split_part(url, '/', 4) AS dir1, split_part(url, '/', 5) AS dir2, split_part(url, '/', 6) AS dir3 FROM bid",
        fp="count(*) AS n, sum(auction) AS a, sum(length(dir1) + length(dir2) + length(dir3)) AS d"),
})
for name, q in QUERIES.items():
    q.setdefault("read", name)
    q.setdefault("regular", False)


def side_input():
    """q13's side input: 10,000 keys and a label each."""
    import pyarrow as pa
    return pa.table({"id": pa.array(range(10000), pa.int64()), "label": pa.array([f"label-{i % 97}" for i in range(10000)])})


def events(n, chunk=500_000):
    """The stream in event order, a chunk at a time: `[(table, Arrow IPC bytes)]`, made before the clock starts."""
    import pyarrow as pa
    out, blocks = [], n // 50
    for b0 in range(0, blocks, chunk // 50):
        b1 = min(blocks, b0 + chunk // 50)
        for s, (per, _, _) in STREAMS.items():
            buf = io.BytesIO()
            t = duck_rows(s, b0 * per, b1 * per)
            with pa.ipc.new_stream(buf, t.schema) as w:
                w.write_table(t)
            out.append((s, buf.getvalue()))
    return out


def model(n, names):
    """Each query's answer by DuckDB over the same events, its fingerprint."""
    import duckdb, pyarrow as pa
    d = duckdb.connect()
    close = closing()
    for s in STREAMS:
        rows = duck_rows(s, 0, counts(n)[s])
        d.register(f"{s}_regular", rows)
        d.register(f"{s}_all", pa.concat_tables([rows, close[s].select(rows.column_names).cast(rows.schema)]))
    d.register("side_input", side_input())
    out = {}
    for name in names:
        q = QUERIES[name]
        tables = "regular" if q["regular"] else "all"
        for s in STREAMS:
            d.execute(f"CREATE OR REPLACE TEMP VIEW {s} AS SELECT * FROM {s}_{tables}")
        out[name] = fingerprint(d.execute(f"SELECT {q['fp']} FROM ({q['duck']}) r").fetchone(), q)
    return out


def fingerprint(row, q):
    """A fingerprint's values as numbers (Pondra's JSON may give a big one as text)."""
    vals = row if isinstance(row, (list, tuple)) else [row[k] for k in row]
    return [None if v is None else round(float(v), 3) for v in vals]


def same(got, want):
    """Two fingerprints agree: equal, but for a sum of doubles' last digits. DuckDB adds doubles in
    whatever order its threads finish, and Pondra's sums are exact (`fsum.rs`). (q17's sum of
    120,000 averages is about 2e12: a run once waited its whole deadline on q17 alone, though the
    view equalled a fresh DuckDB answer when checked by hand.)"""
    if not isinstance(got, list) or len(got) != len(want):
        return False
    return all(g == w or (g is not None and w is not None and abs(g - w) <= 1e-11 * max(1.0, abs(w))) for g, w in zip(got, want))


def pondra(n, names, port=8290):
    import pyarrow as pa, harness
    harness.A = argparse.Namespace(s3=False, port=port)
    lake = tempfile.mkdtemp(prefix="pondra-nexmark-")
    node = harness.Node(lake, port).start()
    q = lambda s: harness.sql(port, s)
    arrow = {"content-type": "application/vnd.apache.arrow.stream"}
    try:
        for s in STREAMS:
            q(f"CREATE TABLE {s} (" + ", ".join(f"{c} {t}" for c, t, _ in columns(s, Dialect(False))) + ")")
        q("CREATE TABLE side_input (id BIGINT, label VARCHAR)")
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, side_input().schema) as w:
            w.write_table(side_input())
        harness.call(port, "POST", "/append/side_input?producer=side&seq=1", buf.getvalue(), headers=arrow)
        refused = {}
        for name in names:
            for stmt in QUERIES[name]["pondra"]:
                try:
                    q(stmt)
                except Exception as e:
                    refused[name] = str(e)[:300]
                    break
        names = [x for x in names if x not in refused]
        want = model(n, names)  # (DuckDB's answers and the events, worked out before the clock starts)
        stream = events(n)
        seqs = {s: 0 for s in STREAMS}
        t0 = time.time()
        for s, body in stream:
            seqs[s] += 1
            harness.call(port, "POST", f"/append/{s}?producer=nexmark-{s}&seq={seqs[s]}", body, timeout=300, headers=arrow)
        sent = time.time() - t0
        for s, t in closing().items():
            buf = io.BytesIO()
            t = t.select([c for c, _, _ in columns(s, Dialect(False))])
            with pa.ipc.new_stream(buf, t.schema) as w:
                w.write_table(t)
            harness.call(port, "POST", f"/append/{s}?producer=nexmark-{s}&seq={seqs[s] + 1}", buf.getvalue(), headers=arrow)
        done, got = {}, {}
        deadline = time.time() + max(600, 5 * sent)  # (an answer still wrong by then is reported with what it was)
        while len(done) < len(names) and time.time() < deadline:
            for name in names:
                if name in done:
                    continue
                try:
                    got[name] = fingerprint(q(f"SELECT {QUERIES[name]['fp']} FROM {QUERIES[name]['read']}")[0], QUERIES[name])
                except Exception as e:
                    got[name] = str(e)[:200]
                if same(got[name], want[name]):
                    done[name] = round(time.time() - t0, 2)
            time.sleep(0.2)
        secs = time.time() - t0
        return {"engine": "pondra", "events": n, "secs": round(secs, 2), "events_per_s": round(n / secs), "sent_s": round(sent, 2),
                "done_s": done, "wrong": {k: {"got": got.get(k), "duckdb": want[k]} for k in names if k not in done}, "refused": refused}
    finally:
        node.kill()
        shutil.rmtree(lake, ignore_errors=True)


def flink(n, names, venv=os.environ.get("FLINK_PYTHON", "/home/claude/venv-flink/bin/python"), check=False, state="rocksdb"):
    """Each query alone, then all of them over one source (`flink_nexmark.py`)."""
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flink_nexmark.py")
    d = Dialect(True)
    spec = {"counts": counts(n), "streams": {s: [(c, t, sql) for c, t, sql in columns(s, d)] for s in STREAMS},
            "queries": {k: QUERIES[k]["flink"] for k in names}, "side_input": [[i, f"label-{i % 97}"] for i in range(10000)], "check": check,
            "state": state, "managed": f"{max(ram_gb() // 3, 1)}g"}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(spec, f)
    # Flink's heap, unless JVM_ARGS says otherwise (flink_nexmark.py says why): two thirds of the machine's memory with
    # state on the heap, a third with RocksDB, which has another third as managed memory.
    jvm = os.environ.get("JVM_ARGS") or f"-Xmx{max(ram_gb() * (2 if state == 'heap' else 1) // 3, 1)}g"
    r = subprocess.run([venv, here, f.name], capture_output=True, text=True, timeout=7200, env={**os.environ, "JVM_ARGS": jvm})
    os.unlink(f.name)
    lines = [l for l in r.stdout.splitlines() if l.startswith("{")]
    return json.loads(lines[-1]) if lines else {"engine": "flink", "error": r.stderr[-2000:]}


def ram_gb():
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") >> 30


def check_streams(n):
    """Flink's rows of each stream == DuckDB's: counts, sums and lengths of every column."""
    import duckdb
    got = flink(n, [], check=True)
    want = {}
    for s in STREAMS:
        d = duckdb.connect()
        d.register("t", duck_rows(s, 0, counts(n)[s]))
        cols = columns(s, Dialect(False))
        parts = ["count(*)"] + [f"sum({c})" if t == "BIGINT" else f"sum(length({c}))" for c, t, _ in cols if t != "TIMESTAMP"] + \
                [f"sum(epoch_ms({c}) // 1000)" for c, t, _ in cols if t == "TIMESTAMP"]
        want[s] = [int(v) for v in d.execute(f"SELECT {', '.join(parts)} FROM t").fetchone()]
    return {"engine": "flink", "check": {s: {"flink": got.get("check", {}).get(s), "duckdb": want[s], "same": got.get("check", {}).get(s) == want[s]} for s in STREAMS},
            "error": got.get("error")}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=10_000_000)
    ap.add_argument("--engines", default="pondra,flink")
    ap.add_argument("--only", default=",".join(QUERIES))
    ap.add_argument("--alone", action="store_true", help="Pondra: each query by itself too, after all of them at once")
    ap.add_argument("--check", action="store_true", help="Flink's streams against DuckDB's rows, then stop")
    ap.add_argument("--flink-state", default="rocksdb", choices=["rocksdb", "heap"], help="where Flink keeps state (rocksdb: as Flink's own Nexmark setup)")
    a = ap.parse_args()
    names = a.only.split(",")
    if a.check:
        print(json.dumps(check_streams(a.events)), flush=True)
        sys.exit(0)
    for e in a.engines.split(","):
        out = pondra(a.events, names) if e == "pondra" else flink(a.events, names, state=a.flink_state)
        if e == "pondra" and a.alone:
            each = {k: pondra(a.events, [k]) for k in names if k not in out["refused"]}
            out["alone_s"] = {k: r["done_s"][k] for k, r in each.items() if k in r["done_s"]}
            out["wrong"].update({f"{k} alone": r["wrong"][k] for k, r in each.items() if k in r["wrong"]})
        print(json.dumps(out), flush=True)
