"""The shared benchmark: the same generated data and the same queries on every engine.
Rows are a pure function of the row id, so every engine produces identical data."""
BASE_TS = 1_700_000_000_000

# Column expressions over a bigint `id` (Spark/DataFusion dialect; Flink overrides below).
GEN = {
    "user_id": "(id * 2654435761) % 1000003",
    "amount": "CAST((id * 48271) % 10000 AS DOUBLE) / 100",
    "category": "concat('c', CAST(id % 1000 AS STRING))",
    "ts_ms": f"{BASE_TS} + id * 10",
}

QUERIES = {
    "q1_scan_agg": "SELECT count(*), sum(amount), avg(amount) FROM events",
    "q2_filter_group": "SELECT category, count(*) AS n, sum(amount) AS s FROM events WHERE amount > 50 GROUP BY category ORDER BY s DESC LIMIT 10",
    "q3_high_card_group": "SELECT user_id, sum(amount) AS s FROM events GROUP BY user_id ORDER BY s DESC LIMIT 10",
    "q4_count_distinct": "SELECT count(DISTINCT user_id) FROM events",
    "q5_join": "SELECT d.region, count(*) AS n, sum(e.amount) AS s FROM events e JOIN dims d ON e.category = d.category GROUP BY d.region ORDER BY d.region",
    "q6_time_bucket": "SELECT ts_ms - ts_ms % 60000 AS bucket, count(*) AS n FROM events GROUP BY ts_ms - ts_ms % 60000 ORDER BY n DESC, bucket LIMIT 5",
}


