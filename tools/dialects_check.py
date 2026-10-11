#!/usr/bin/env python3
"""Statements people from other databases type on their first days, run on a Pondra node.

  dialects_check.py [--port 9310] [--out dialects.json] [--only postgres,duckdb] [--report]

The cases are spelled the way Postgres, DuckDB, Snowflake, Spark, MySQL, BigQuery and SQL Server
users write them. Each must either run, or be refused with its SQLSTATE and words that say what to
write instead. SETUP makes the tables the cases read (any failure there stops the tool); then each
case is one POST /sql in its dialect's own session (x-pondra-session: dialects-<dialect>, as a
client's connection would be: SET, BEGIN … COMMIT, temporary tables and PREPARE keep to it), timed
out at 60 s, and its answer is ok (HTTP 200) or a refusal with its SQLSTATE and message. A case with no expectation must run; a case with (sqlstate, words) must be
refused with that code and words in its message. The answers go to --out as JSON, and the summary
counts what ran and was refused per dialect, then groups the refusals by their first 90 characters
(each run of digits as N), most common first. Without --report, a case that misses its expectation exits 1 after
its miss is printed; with --report the tool exits 0 once every case is answered, and the refusals are
the product. The binary is harness's: PONDRA_BIN, else target/release/pondra.
"""
import argparse, http.client, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import harness
from harness import Node

A = None  # parsed args

# Statements people from other databases type on their first days with Pondra.
# SETUP makes orders, customers and events; CASES are (dialect, sql) pairs, each
# statement spelled the way that dialect spells it. Cases only read those three
# tables or a t_case_N table they create themselves.

SETUP = [
    "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2), status VARCHAR, created_at TIMESTAMP, tags VARCHAR)",
    "INSERT INTO orders VALUES (1, 1, 120.50, 'paid', '2026-01-05 09:15:00', 'vip,repeat'), (2, 2, 35.00, 'shipped', '2026-01-07 14:02:00', NULL), (3, 1, 9.99, 'refunded', '2026-02-11 18:45:00', 'promo'), (4, 3, 250.00, 'paid', '2026-02-14 10:00:00', 'b2b'), (5, 4, 75.25, 'pending', '2026-03-01 08:30:00', NULL), (6, 5, 42.00, 'paid', '2026-03-03 12:20:00', 'gift'), (7, NULL, 18.00, 'paid', '2026-03-09 16:55:00', 'guest')",
    "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR, email VARCHAR, country VARCHAR, signed_up DATE)",
    "INSERT INTO customers VALUES (1, 'Ada Lovelace', 'ada@example.com', 'UK', '2024-11-02'), (2, 'Grace Hopper', 'grace@example.com', 'US', '2025-01-15'), (3, 'Linus Torvalds', NULL, 'FI', '2025-03-20'), (4, 'Margaret Hamilton', 'mh@example.com', 'US', '2025-06-01'), (5, 'Katherine Johnson', 'kj@example.com', NULL, '2025-08-30'), (6, 'Alan Turing', 'alan@example.com', 'UK', '2025-09-12')",
    "CREATE TABLE events (ts TIMESTAMP, user_id INT, kind VARCHAR, payload VARCHAR)",
    """INSERT INTO events VALUES ('2026-03-01 10:00:00', 1, 'page_view', '{"page": "/home", "ms": 120}'), ('2026-03-01 10:00:05', 1, 'click', '{"page": "/home", "button": "buy"}'), ('2026-03-01 10:03:00', 2, 'page_view', '{"page": "/pricing", "ms": 310}'), ('2026-03-02 09:12:00', 3, 'signup', NULL), ('2026-03-02 11:45:00', 2, 'purchase', '{"sku": "A-1", "qty": 2}'), ('2026-03-03 08:00:00', NULL, 'page_view', '{"page": "/docs", "ms": 95}'), ('2026-03-03 08:30:00', 4, 'click', '{"page": "/docs", "button": null}')""",
]

CASES = [
    # postgres: joins, subqueries and set operations
    ("postgres", "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON c.id = o.customer_id ORDER BY o.id"),
    ("postgres", "SELECT c.name, COUNT(o.id) AS order_count FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.name HAVING COUNT(o.id) = 0"),
    ("postgres", "SELECT o.id, c.name FROM orders o RIGHT JOIN customers c ON c.id = o.customer_id"),
    ("postgres", "SELECT c.id, o.id FROM customers c FULL OUTER JOIN orders o ON o.customer_id = c.id"),
    ("postgres", "SELECT a.id, b.id FROM orders a JOIN orders b USING (customer_id) WHERE a.id < b.id"),
    ("postgres", "SELECT c.name, top.amount FROM customers c LEFT JOIN LATERAL (SELECT o.amount FROM orders o WHERE o.customer_id = c.id ORDER BY o.amount DESC LIMIT 1) top ON true"),
    ("postgres", "SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100)"),
    ("postgres", "SELECT id, amount FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE country = 'US')"),
    ("postgres", "SELECT id FROM orders WHERE status = 'paid' UNION SELECT id FROM orders WHERE amount > 100 ORDER BY id"),
    ("postgres", "SELECT customer_id FROM orders INTERSECT SELECT id FROM customers"),
    ("postgres", "SELECT DISTINCT ON (customer_id) customer_id, id, created_at FROM orders ORDER BY customer_id, created_at DESC"),
    ("postgres", "SELECT * FROM (SELECT id, customer_id, amount, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn FROM orders) t WHERE rn = 1"),
    ("postgres", "SELECT id, customer_id, amount, SUM(amount) OVER (PARTITION BY customer_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM orders"),
    ("postgres", "WITH paid AS (SELECT * FROM orders WHERE status = 'paid') SELECT customer_id, SUM(amount) AS total FROM paid GROUP BY customer_id"),
    ("postgres", "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 10) SELECT n FROM nums"),
    # postgres: sorting, grouping and filters
    ("postgres", "SELECT id, amount FROM orders ORDER BY amount DESC LIMIT 3 OFFSET 2"),
    ("postgres", "SELECT id FROM orders ORDER BY id FETCH FIRST 3 ROWS ONLY"),
    ("postgres", "SELECT status, customer_id, SUM(amount) AS total FROM orders GROUP BY GROUPING SETS ((status), (customer_id), ())"),
    ("postgres", "SELECT country, signed_up, COUNT(*) AS n FROM customers GROUP BY ROLLUP (country, signed_up)"),
    ("postgres", "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE status = 'paid') AS paid_count FROM orders"),
    # postgres: functions
    ("postgres", "SELECT date_trunc('month', created_at) AS month, SUM(amount) AS total FROM orders GROUP BY 1 ORDER BY 1"),
    ("postgres", "SELECT id FROM orders WHERE created_at >= now() - interval '30 days'"),
    ("postgres", "SELECT to_char(created_at, 'YYYY-MM-DD HH24:MI') AS stamp, extract(epoch FROM created_at)::bigint AS unix_ts FROM orders"),
    ("postgres", "SELECT ts, payload::jsonb ->> 'page' AS page FROM events WHERE kind = 'page_view'"),
    ("postgres", "SELECT ts, payload::jsonb #>> '{page}' AS page FROM events WHERE payload IS NOT NULL"),
    ("postgres", "SELECT g::date AS day FROM generate_series('2026-03-01'::date, '2026-03-05'::date, interval '1 day') AS g"),
    ("postgres", "SELECT ARRAY[1, 2, 3] AS xs, cardinality(ARRAY[1, 2, 3]) AS n, unnest(ARRAY['a', 'b']) AS x"),
    ("postgres", "SELECT initcap(name) || ' <' || email || '>' AS contact, id % 3 AS bucket FROM customers"),
    # postgres: tables and rows
    ("postgres", "CREATE TABLE IF NOT EXISTS t_case_1 (id SERIAL PRIMARY KEY, order_id INT REFERENCES orders(id), note TEXT DEFAULT 'none', qty INT CHECK (qty > 0), created_at TIMESTAMPTZ DEFAULT now(), UNIQUE (order_id, note))"),
    ("postgres", "CREATE TABLE t_case_2 (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label VARCHAR(50) NOT NULL)"),
    ("postgres", "CREATE TABLE t_case_3 AS SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"),
    ("postgres", "CREATE TEMP TABLE t_case_4 AS SELECT * FROM customers WHERE country = 'US'"),
    ("postgres", "INSERT INTO t_case_1 (order_id, note, qty) VALUES (1, 'gift', 2), (2, 'rush', 1)"),
    ("postgres", "ALTER TABLE t_case_1 RENAME COLUMN note TO memo"),
    ("postgres", "CREATE INDEX CONCURRENTLY idx_t_case_1_memo ON t_case_1 USING gin (to_tsvector('simple', memo))"),
    ("postgres", "INSERT INTO orders (id, customer_id, amount, status, created_at, tags) VALUES (2, 2, 40.00, 'delivered', now(), NULL) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, amount = EXCLUDED.amount"),
    ("postgres", "UPDATE orders SET tags = c.country FROM customers c WHERE c.id = orders.customer_id AND orders.id = 4"),
    ("postgres", "DELETE FROM t_case_1 t USING orders o WHERE t.order_id = o.id AND o.status = 'refunded'"),
    ("postgres", "COPY (SELECT id, name, country FROM customers ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER true)"),
    # postgres: sessions and admin
    ("postgres", "SET search_path TO public, pg_catalog"),
    ("postgres", "EXPLAIN ANALYZE SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id"),
    ("postgres", "BEGIN"),
    ("postgres", "UPDATE orders SET status = 'paid' WHERE id = 5"),
    ("postgres", "COMMIT"),
    ("postgres", "CREATE EXTENSION IF NOT EXISTS pg_trgm"),
    ("postgres", """CREATE FUNCTION order_total(cust INT) RETURNS NUMERIC LANGUAGE plpgsql AS $$ BEGIN RETURN (SELECT COALESCE(SUM(amount), 0) FROM orders WHERE customer_id = cust); END; $$"""),
    ("postgres", "LISTEN new_orders"),
    ("postgres", "SELECT tablename, tableowner FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename"),
    # duckdb: queries
    ("duckdb", "FROM orders SELECT id, amount WHERE status = 'paid'"),
    ("duckdb", "SELECT customer_id, status, SUM(amount) AS total FROM orders GROUP BY ALL ORDER BY total DESC"),
    ("duckdb", "SELECT id, customer_id, amount FROM orders QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY amount DESC) = 1"),
    ("duckdb", "SELECT * EXCLUDE (tags) FROM orders WHERE amount > 20"),
    ("duckdb", "SELECT id FROM orders ORDER BY id LIMIT 3 OFFSET 2"),
    ("duckdb", "SELECT e.ts, e.kind, o.id FROM events e ASOF JOIN orders o ON e.user_id = o.customer_id AND e.ts >= o.created_at"),
    ("duckdb", "PIVOT orders ON status USING SUM(amount) GROUP BY customer_id"),
    ("duckdb", "UNPIVOT (SELECT id, amount, customer_id FROM orders) ON amount, customer_id INTO NAME metric VALUE reading"),
    ("duckdb", "SUMMARIZE orders"),
    ("duckdb", "SELECT * FROM orders USING SAMPLE 10%"),
    ("duckdb", "SELECT c.name, o.id FROM customers c CROSS JOIN LATERAL (SELECT id FROM orders o WHERE o.customer_id = c.id ORDER BY amount DESC LIMIT 1) o"),
    # duckdb: functions
    ("duckdb", "SELECT strftime(created_at, '%Y-%m-%d') AS day, epoch(created_at) AS unix_seconds FROM orders"),
    ("duckdb", "SELECT date_diff('day', signed_up, current_date) AS days_member, date_part('hour', now()) AS hr FROM customers"),
    ("duckdb", "SELECT TRY_CAST('12x' AS INTEGER) AS bad, amount::VARCHAR || ' USD' AS label FROM orders"),
    ("duckdb", "SELECT json_extract_string(payload, '$.page') AS page, payload->>'$.ms' AS ms FROM events"),
    ("duckdb", "SELECT regexp_extract(email, '@(.+)$', 1) AS domain, string_split(name, ' ') AS parts FROM customers"),
    ("duckdb", "SELECT [1, 2, 3] AS xs, list_transform([1, 2, 3], x -> x * 10) AS scaled, {'city': 'Oslo', 'zip': 1234}.city AS city"),
    ("duckdb", "SELECT range AS d FROM range(DATE '2026-03-01', DATE '2026-03-05', INTERVAL 1 DAY)"),
    ("duckdb", "SELECT id, random() AS r FROM orders ORDER BY r LIMIT 2"),
    ("duckdb", "CREATE OR REPLACE MACRO discount(price, pct) AS price * (1 - pct / 100.0)"),
    # duckdb: tables and rows
    ("duckdb", "CREATE TABLE t_case_101 (id INTEGER PRIMARY KEY, label VARCHAR DEFAULT 'new', score DOUBLE CHECK (score >= 0))"),
    ("duckdb", "CREATE OR REPLACE TABLE t_case_102 AS SELECT * FROM orders WHERE status = 'paid'"),
    ("duckdb", "INSERT OR REPLACE INTO t_case_101 VALUES (1, 'paid', 9.5), (2, 'vip', 4.0)"),
    ("duckdb", "INSERT INTO t_case_101 VALUES (4, 'vip', 7.0) RETURNING *"),
    ("duckdb", "INSERT INTO t_case_101 VALUES (3, 'gift', 1.5) ON CONFLICT (id) DO UPDATE SET score = excluded.score"),
    ("duckdb", "UPSERT INTO t_case_101 VALUES (5, 'new', 0.5)"),
    ("duckdb", "UPDATE orders SET tags = 'uk-buyer' FROM customers c WHERE c.id = orders.customer_id AND c.country = 'UK' AND orders.id = 1"),
    ("duckdb", "DELETE FROM t_case_101 USING customers c WHERE t_case_101.id = c.id AND c.country IS NULL"),
    ("duckdb", "ALTER TABLE t_case_101 ALTER COLUMN label TYPE VARCHAR"),
    ("duckdb", "ALTER TABLE t_case_101 RENAME COLUMN label TO tag"),
    ("duckdb", "CREATE SEQUENCE seq_ticket START 1"),
    ("duckdb", "COPY (SELECT * FROM customers) TO 'customers.parquet' (FORMAT PARQUET)"),
    # duckdb: sessions and admin
    ("duckdb", "SET threads = 4"),
    ("duckdb", "SHOW TABLES"),
    ("duckdb", "DESCRIBE orders"),
    ("duckdb", "PRAGMA table_info('orders')"),
    ("duckdb", "EXPLAIN ANALYZE SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"),
    ("duckdb", "CREATE SCHEMA IF NOT EXISTS sales"),
    ("duckdb", "ATTACH 'archive.duckdb' AS archive (READ_ONLY)"),
    ("duckdb", "CHECKPOINT"),
    ("duckdb", "SELECT version()"),
    # snowflake: queries
    ("snowflake", "SELECT id, customer_id, amount FROM orders QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) = 1"),
    ("snowflake", "SELECT status, customer_id, SUM(amount) AS total FROM orders GROUP BY ALL"),
    ("snowflake", "SELECT id FROM orders ORDER BY id LIMIT 2 OFFSET 1"),
    ("snowflake", "SELECT o.id, f.value::STRING AS tag FROM orders o, LATERAL FLATTEN(INPUT => SPLIT(o.tags, ',')) f WHERE o.tags IS NOT NULL"),
    ("snowflake", "SELECT * FROM (SELECT customer_id, status, amount FROM orders) PIVOT (SUM(amount) FOR status IN ('paid', 'shipped', 'pending')) AS p"),
    ("snowflake", "SELECT * FROM (SELECT id, amount, customer_id FROM orders) UNPIVOT (val FOR col IN (amount, customer_id))"),
    ("snowflake", "SELECT * FROM orders SAMPLE (50) SEED (42)"),
    ("snowflake", "SELECT * EXCLUDE (tags) FROM orders"),
    # snowflake: functions
    ("snowflake", "SELECT DATEADD(day, 7, created_at) AS due, DATEDIFF(day, created_at, CURRENT_TIMESTAMP()) AS age_days FROM orders"),
    ("snowflake", "SELECT DATE_TRUNC('MONTH', created_at) AS month, TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI') AS stamp FROM orders"),
    ("snowflake", "SELECT id, IFF(status = 'paid', amount, 0) AS paid_amount, NVL(tags, 'none') AS t, ZEROIFNULL(amount) AS z FROM orders"),
    ("snowflake", "SELECT SPLIT_PART(email, '@', 2) AS domain, REGEXP_LIKE(name, '^[A-Z][a-z]+ .*') AS is_full_name FROM customers"),
    ("snowflake", "SELECT PARSE_JSON(payload):page::STRING AS page, PARSE_JSON(payload):ms::INT AS ms FROM events WHERE kind = 'page_view'"),
    ("snowflake", "SELECT amount::NUMBER(10,2) AS amt, TRY_TO_NUMBER('abc') AS bad, TRY_CAST(customer_id AS VARCHAR) AS cid FROM orders"),
    ("snowflake", "SELECT ARRAY_CONSTRUCT(1, 2, 3) AS xs, ARRAY_SIZE(ARRAY_CONSTRUCT(1, 2, 3)) AS n, OBJECT_CONSTRUCT('id', id, 'status', status) AS doc FROM orders"),
    ("snowflake", "SELECT name, DECODE(country, 'US', 'America', 'UK', 'Britain', 'other') AS region FROM customers"),
    ("snowflake", "SELECT name FROM customers WHERE name ILIKE '%lovelace%'"),
    # snowflake: tables and rows
    ("snowflake", "CREATE OR REPLACE TABLE t_case_201 (id NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1, label VARCHAR(50) NOT NULL DEFAULT 'new', amount NUMBER(10,2), created DATE DEFAULT CURRENT_DATE()) CLUSTER BY (created) COMMENT = 'test table'"),
    ("snowflake", "INSERT INTO t_case_201 (id, label, amount) VALUES (1, 'a', 10.00), (2, 'b', 20.50)"),
    ("snowflake", "CREATE TRANSIENT TABLE t_case_202 CLONE orders"),
    ("snowflake", "MERGE INTO t_case_201 t USING orders o ON t.id = o.id WHEN MATCHED THEN UPDATE SET amount = o.amount WHEN NOT MATCHED THEN INSERT (id, label, amount) VALUES (o.id, o.status, o.amount)"),
    ("snowflake", "UPDATE orders SET tags = 'vip' FROM customers c WHERE c.id = orders.customer_id AND c.name = 'Ada Lovelace' AND orders.id = 1"),
    ("snowflake", "DELETE FROM t_case_201 USING orders o WHERE t_case_201.id = o.id AND o.status = 'refunded'"),
    ("snowflake", "ALTER TABLE t_case_201 RENAME COLUMN label TO status_label"),
    ("snowflake", "ALTER TABLE t_case_201 ALTER COLUMN status_label SET DATA TYPE VARCHAR(200)"),
    ("snowflake", "ALTER TABLE t_case_202 RENAME TO t_case_203"),
    ("snowflake", "COPY INTO t_case_203 FROM @my_stage/notes.csv FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)"),
    ("snowflake", "CREATE OR REPLACE STREAM orders_stream ON TABLE orders"),
    ("snowflake", "CREATE OR REPLACE TASK refresh_totals WAREHOUSE = compute_wh SCHEDULE = '60 MINUTE' AS INSERT INTO t_case_201 (id, status_label, amount) SELECT id, status, amount FROM orders_stream"),
    # snowflake: sessions and admin
    ("snowflake", "USE WAREHOUSE compute_wh"),
    ("snowflake", "USE DATABASE analytics"),
    ("snowflake", "SHOW TABLES LIKE 'ORD%' IN SCHEMA PUBLIC"),
    ("snowflake", "DESC TABLE customers"),
    ("snowflake", "EXPLAIN USING TEXT SELECT * FROM orders WHERE amount > 10"),
    ("snowflake", "SET min_amount = 20"),
    ("snowflake", "GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO ROLE analyst"),
    ("snowflake", "SELECT COUNT(*) FROM orders AT(OFFSET => -60*5)"),
    ("snowflake", "CREATE OR REPLACE FUNCTION double_it(x NUMBER) RETURNS NUMBER AS 'x * 2'"),
    # spark: queries
    ("spark", "SELECT o.id, tag FROM orders o LATERAL VIEW explode(split(o.tags, ',')) t AS tag WHERE o.tags IS NOT NULL"),
    ("spark", "SELECT id, posexplode(split(tags, ',')) AS (pos, tag) FROM orders WHERE tags IS NOT NULL"),
    ("spark", "SELECT customer_id, amount FROM orders DISTRIBUTE BY customer_id SORT BY amount DESC"),
    ("spark", "SELECT /*+ BROADCAST(c) */ o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"),
    ("spark", "SELECT status, customer_id, sum(amount) AS total FROM orders GROUP BY ROLLUP(status, customer_id) ORDER BY status, customer_id"),
    ("spark", "SELECT c.name FROM customers c LEFT SEMI JOIN orders o ON c.id = o.customer_id"),
    ("spark", "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, label)"),
    # spark: functions
    ("spark", "SELECT date_add(to_date(created_at), 7) AS due, datediff(current_date(), to_date(created_at)) AS age FROM orders"),
    ("spark", "SELECT date_format(created_at, 'yyyy-MM') AS ym, from_unixtime(unix_timestamp(created_at)) AS ts FROM orders"),
    ("spark", "SELECT get_json_object(payload, '$.page') AS page, from_json(payload, 'page STRING, ms INT') AS parsed FROM events"),
    ("spark", "SELECT transform(array(1, 2, 3), x -> x * 10) AS scaled, aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) AS total"),
    ("spark", "SELECT customer_id, collect_set(status) AS statuses, percentile_approx(amount, 0.5) AS median FROM orders GROUP BY customer_id"),
    ("spark", "SELECT try_cast('12x' AS INT) AS bad, CAST(amount AS BIGINT) AS whole, nvl2(tags, 'tagged', 'plain') AS kind, ifnull(tags, 'none') AS t FROM orders"),
    ("spark", "SELECT size(split(tags, ',')) AS n, array_contains(split(tags, ','), 'vip') AS has_vip FROM orders"),
    ("spark", "SELECT substring_index(email, '@', -1) AS domain, instr(name, 'o') AS pos, months_between(current_date(), signed_up) AS months FROM customers"),
    # spark: tables and rows
    ("spark", "CREATE TABLE IF NOT EXISTS t_case_301 (id INT, customer_id INT, amount DECIMAL(10,2), day DATE) USING DELTA PARTITIONED BY (day) COMMENT 'daily orders' TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"),
    ("spark", "CREATE TABLE t_case_302 (id INT, status STRING) CLUSTERED BY (id) INTO 4 BUCKETS STORED AS PARQUET"),
    ("spark", "CREATE TABLE t_case_303 USING PARQUET AS SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id"),
    ("spark", "CREATE OR REPLACE TEMPORARY VIEW paid_orders AS SELECT * FROM orders WHERE status = 'paid'"),
    ("spark", "INSERT INTO t_case_302 VALUES (1, 'new'), (2, 'paid')"),
    ("spark", "MERGE INTO t_case_302 t USING (SELECT id, status FROM orders) o ON t.id = o.id WHEN MATCHED THEN UPDATE SET t.status = o.status WHEN NOT MATCHED THEN INSERT (id, status) VALUES (o.id, o.status)"),
    ("spark", "UPDATE orders SET tags = 'checked' WHERE id = 3"),
    ("spark", "ALTER TABLE t_case_301 ADD COLUMNS (channel STRING COMMENT 'sales channel')"),
    ("spark", "OPTIMIZE t_case_301 ZORDER BY (customer_id)"),
    # spark: sessions and admin
    ("spark", "SET spark.sql.shuffle.partitions = 8"),
    ("spark", "SHOW TABLES IN default LIKE 'orders*'"),
    ("spark", "DESCRIBE EXTENDED orders"),
    ("spark", "EXPLAIN EXTENDED SELECT customer_id, sum(amount) FROM orders GROUP BY customer_id"),
    ("spark", "CACHE TABLE paid_orders"),
    ("spark", "ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, amount"),
    ("spark", "CREATE OR REPLACE FUNCTION discount(price DOUBLE, pct DOUBLE) RETURNS DOUBLE RETURN price * (1 - pct / 100)"),
    ("spark", "VACUUM t_case_301 RETAIN 168 HOURS"),
    # mysql: queries
    ("mysql", "SELECT id, amount FROM orders ORDER BY id LIMIT 2, 3"),
    ("mysql", "SELECT c.name, COUNT(o.id) AS n FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.id, c.name HAVING n > 0 ORDER BY n DESC"),
    ("mysql", "SELECT status, COUNT(*) AS n FROM orders GROUP BY status WITH ROLLUP"),
    ("mysql", "SELECT customer_id, GROUP_CONCAT(status ORDER BY id SEPARATOR ', ') AS statuses FROM orders GROUP BY customer_id"),
    ("mysql", "WITH RECURSIVE n AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM n WHERE i < 5) SELECT i FROM n"),
    ("mysql", "SELECT c.id, o.id FROM customers c FULL OUTER JOIN orders o ON o.customer_id = c.id"),
    ("mysql", "SELECT t.cust, t.total FROM (SELECT customer_id AS cust, SUM(amount) AS total FROM orders GROUP BY customer_id) AS t WHERE t.total > 50"),
    # mysql: functions
    ("mysql", "SELECT DATE_ADD(created_at, INTERVAL 7 DAY) AS due, DATEDIFF(CURDATE(), created_at) AS age, TIMESTAMPDIFF(HOUR, created_at, NOW()) AS hours FROM orders"),
    ("mysql", "SELECT DATE_FORMAT(created_at, '%Y-%m') AS ym, SUM(amount) AS total FROM orders GROUP BY ym"),
    ("mysql", "SELECT STR_TO_DATE('03/01/2026', '%m/%d/%Y') AS d, UNIX_TIMESTAMP(created_at) AS epoch, FROM_UNIXTIME(1767225600) AS ts FROM orders"),
    ("mysql", "SELECT id, IF(status = 'paid', amount, 0) AS paid_amount, IFNULL(tags, 'none') AS t FROM orders"),
    ("mysql", "SELECT payload->>'$.page' AS page, JSON_UNQUOTE(JSON_EXTRACT(payload, '$.ms')) AS ms FROM events WHERE kind = 'page_view'"),
    ("mysql", "SELECT CONCAT_WS(' ', name, CONCAT('<', email, '>')) AS contact, SUBSTRING_INDEX(email, '@', -1) AS domain, LPAD(id, 6, '0') AS code FROM customers"),
    ("mysql", "SELECT name FROM customers WHERE REGEXP_LIKE(name, '^[A-Z]') AND CHAR_LENGTH(name) > 5"),
    # mysql: tables and rows
    ("mysql", "CREATE TABLE IF NOT EXISTS t_case_401 (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, order_id INT NOT NULL, note VARCHAR(255) DEFAULT NULL, flag TINYINT(1) DEFAULT 0, created_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, KEY idx_order (order_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test'"),
    ("mysql", "CREATE TABLE t_case_402 (id INT PRIMARY KEY, customer_id INT, CONSTRAINT fk_cust FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE) ENGINE=InnoDB"),
    ("mysql", "CREATE TEMPORARY TABLE t_case_403 AS SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id"),
    ("mysql", "INSERT INTO t_case_401 (order_id, note) VALUES (1, 'gift'), (2, NULL)"),
    ("mysql", "REPLACE INTO t_case_401 (id, order_id, note) VALUES (1, 1, 'refund')"),
    ("mysql", "INSERT IGNORE INTO customers (id, name, email, country, signed_up) VALUES (1, 'Ada Lovelace', 'ada@example.com', 'UK', '2024-11-02')"),
    ("mysql", "INSERT INTO orders (id, customer_id, amount, status, created_at, tags) VALUES (1, 1, 120.50, 'paid', '2026-01-05 09:15:00', 'vip,repeat') ON DUPLICATE KEY UPDATE status = VALUES(status)"),
    ("mysql", "ALTER TABLE t_case_401 ADD COLUMN channel VARCHAR(20) AFTER note"),
    ("mysql", "ALTER TABLE t_case_401 CHANGE COLUMN flag is_flagged TINYINT(1) NOT NULL DEFAULT 0"),
    ("mysql", "UPDATE orders o JOIN customers c ON c.id = o.customer_id SET o.tags = 'uk' WHERE c.country = 'UK' AND o.id = 1"),
    ("mysql", "DELETE t FROM t_case_402 t JOIN customers c ON c.id = t.customer_id WHERE c.country IS NULL"),
    # mysql: sessions and admin
    ("mysql", "SET @min_amount := 50"),
    ("mysql", "SHOW CREATE TABLE orders"),
    ("mysql", "DESC events"),
    ("mysql", "EXPLAIN FORMAT=JSON SELECT * FROM orders WHERE customer_id = 1"),
    ("mysql", "START TRANSACTION"),
    ("mysql", "SELECT * FROM orders WHERE id = 1 FOR UPDATE"),
    ("mysql", "COMMIT"),
    ("mysql", "PREPARE by_status FROM 'SELECT id FROM orders WHERE status = ?'"),
    ("mysql", "CREATE PROCEDURE mark_paid(IN oid INT) UPDATE orders SET status = 'paid' WHERE id = oid"),
    ("mysql", "CALL mark_paid(2)"),
    ("mysql", "SELECT VERSION(), DATABASE(), CURRENT_USER()"),
    # bigquery: queries
    ("bigquery", "SELECT o.id, c.name, o.amount FROM `orders` AS o JOIN `customers` AS c ON c.id = o.customer_id ORDER BY o.id"),
    ("bigquery", "SELECT status, COUNT(*) AS n, SUM(amount) AS total FROM orders GROUP BY ALL"),
    ("bigquery", "SELECT id, customer_id, amount FROM orders QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) = 1"),
    ("bigquery", "SELECT o.id, tag FROM orders AS o, UNNEST(SPLIT(o.tags, ',')) AS tag WHERE o.tags IS NOT NULL"),
    ("bigquery", "SELECT customer_id, ARRAY_AGG(STRUCT(id, amount) ORDER BY amount DESC LIMIT 2) AS top_orders FROM orders GROUP BY customer_id"),
    ("bigquery", "SELECT * FROM (SELECT customer_id, status, amount FROM orders) PIVOT (SUM(amount) FOR status IN ('paid', 'shipped'))"),
    # bigquery: functions
    ("bigquery", "SELECT DATE_TRUNC(DATE(created_at), MONTH) AS month, DATE_ADD(DATE(created_at), INTERVAL 7 DAY) AS due FROM orders"),
    ("bigquery", "SELECT FORMAT_DATE('%Y-%m', DATE(created_at)) AS ym, TIMESTAMP_TRUNC(created_at, HOUR) AS hr, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), created_at, HOUR) AS hours_ago FROM orders"),
    ("bigquery", "SELECT PARSE_DATE('%d/%m/%Y', '01/03/2026') AS d, SAFE.PARSE_DATE('%Y-%m-%d', 'not a date') AS bad, EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) AS dow"),
    ("bigquery", "SELECT UNIX_SECONDS(created_at) AS epoch, TIMESTAMP_SECONDS(1767225600) AS ts FROM orders"),
    ("bigquery", "SELECT JSON_VALUE(payload, '$.page') AS page, CAST(JSON_VALUE(payload, '$.ms') AS INT64) AS ms FROM events"),
    ("bigquery", "SELECT REGEXP_CONTAINS(email, r'@example[.]com$') AS is_example, REGEXP_EXTRACT(email, r'@(.+)$') AS domain FROM customers"),
    ("bigquery", "SELECT customer_id, STRING_AGG(status, ', ' ORDER BY id) AS statuses, COUNTIF(status = 'paid') AS paid_n, SAFE_DIVIDE(SUM(amount), COUNT(*)) AS avg_amount FROM orders GROUP BY customer_id"),
    # bigquery: tables and rows
    ("bigquery", "CREATE OR REPLACE TABLE `t_case_501` PARTITION BY DATE(created_at) CLUSTER BY customer_id OPTIONS (description = 'orders by day') AS SELECT * FROM orders"),
    ("bigquery", "CREATE TABLE IF NOT EXISTS t_case_502 (id INT64 NOT NULL, label STRING, amount NUMERIC(10, 2), meta JSON, tags ARRAY<STRING>, addr STRUCT<city STRING, zip STRING>)"),
    ("bigquery", "INSERT INTO t_case_502 (id, label, amount) VALUES (1, 'a', 9.99), (2, NULL, NULL)"),
    ("bigquery", "MERGE INTO t_case_502 t USING orders o ON t.id = o.id WHEN MATCHED THEN UPDATE SET label = o.status WHEN NOT MATCHED THEN INSERT (id, label, amount) VALUES (o.id, o.status, o.amount)"),
    ("bigquery", "UPDATE orders o SET tags = c.country FROM customers c WHERE c.id = o.customer_id AND o.id = 4"),
    ("bigquery", "DELETE FROM t_case_502 WHERE amount IS NULL"),
    ("bigquery", "CREATE SCHEMA IF NOT EXISTS sales OPTIONS (location = 'US')"),
    ("bigquery", "CREATE OR REPLACE VIEW sales.paid_orders AS SELECT * FROM orders WHERE status = 'paid'"),
    ("bigquery", "CREATE MATERIALIZED VIEW sales.mv_totals AS SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"),
    # bigquery: sessions and admin
    ("bigquery", "DECLARE min_amt NUMERIC DEFAULT 50"),
    ("bigquery", "SELECT table_name, creation_time FROM INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE'"),
    ("bigquery", "BEGIN TRANSACTION"),
    ("bigquery", "UPDATE orders SET status = 'pending' WHERE id = 5"),
    ("bigquery", "COMMIT TRANSACTION"),
    ("bigquery", "CREATE TEMP FUNCTION initials(name STRING) AS (SUBSTR(name, 1, 1))"),
    ("bigquery", "CREATE OR REPLACE PROCEDURE sales.mark_paid(IN oid INT64) BEGIN UPDATE orders SET status = 'paid' WHERE id = oid; END"),
    ("bigquery", "CALL sales.mark_paid(2)"),
    ("bigquery", "EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM orders'"),
    ("bigquery", "SELECT COUNT(*) FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)"),
    # sqlserver: queries
    ("sqlserver", "SELECT TOP 3 id, amount FROM orders ORDER BY amount DESC"),
    ("sqlserver", "SELECT id, amount FROM orders ORDER BY id OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY"),
    ("sqlserver", "SELECT [o].[id], [c].[name] FROM [dbo].[orders] AS [o] INNER JOIN [dbo].[customers] AS [c] ON [c].[id] = [o].[customer_id]"),
    ("sqlserver", "SELECT c.name, x.id FROM customers AS c CROSS APPLY (SELECT TOP 1 o.id FROM orders AS o WHERE o.customer_id = c.id ORDER BY o.amount DESC) AS x"),
    ("sqlserver", "SELECT * FROM (SELECT customer_id, status, amount FROM orders) AS src PIVOT (SUM(amount) FOR status IN ([paid], [shipped])) AS p"),
    ("sqlserver", "SELECT status, customer_id, SUM(amount) AS total FROM orders GROUP BY ROLLUP (status, customer_id)"),
    ("sqlserver", "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 5) SELECT n FROM nums OPTION (MAXRECURSION 10)"),
    ("sqlserver", "SELECT id, status FROM orders WITH (NOLOCK) WHERE status = 'paid'"),
    # sqlserver: functions
    ("sqlserver", "SELECT GETDATE() AS now_local, DATEADD(day, 7, created_at) AS due FROM orders"),
    ("sqlserver", "SELECT DATEDIFF(day, signed_up, GETDATE()) AS days, EOMONTH(GETDATE()) AS month_end, DATEPART(weekday, GETDATE()) AS dow FROM customers"),
    ("sqlserver", "SELECT FORMAT(created_at, 'yyyy-MM-dd HH:mm') AS stamp, CONVERT(varchar(10), created_at, 120) AS day, CONVERT(int, amount) AS whole FROM orders"),
    ("sqlserver", "SELECT TRY_CAST('12x' AS INT) AS bad, TRY_CONVERT(date, '2026-03-01') AS d, ISNULL(tags, 'none') AS t, IIF(status = 'paid', amount, 0) AS paid_amount FROM orders"),
    ("sqlserver", "SELECT CHARINDEX('@', email) AS at_pos, LEFT(name, 3) AS a, RIGHT(name, 3) AS b, SUBSTRING(name, 2, 4) AS c FROM customers"),
    ("sqlserver", "SELECT customer_id, STRING_AGG(status, ', ') WITHIN GROUP (ORDER BY id) AS statuses FROM orders GROUP BY customer_id"),
    ("sqlserver", "SELECT JSON_VALUE(payload, '$.page') AS page, j.[key], j.value FROM events AS e CROSS APPLY OPENJSON(e.payload) AS j WHERE e.kind = 'page_view'"),
    # sqlserver: tables and rows
    ("sqlserver", "CREATE TABLE t_case_601 (id INT IDENTITY(1,1) PRIMARY KEY, order_id INT NOT NULL REFERENCES orders(id), note NVARCHAR(50) NULL, qty INT CONSTRAINT ck_qty CHECK (qty > 0) DEFAULT 1, created_at DATETIME2 DEFAULT SYSDATETIME(), flag BIT NOT NULL DEFAULT 0)"),
    ("sqlserver", "SELECT customer_id, SUM(amount) AS total INTO t_case_602 FROM orders GROUP BY customer_id"),
    ("sqlserver", "INSERT INTO t_case_601 (order_id, note, qty) VALUES (1, N'gift', 2), (2, N'rush', 1)"),
    ("sqlserver", "INSERT INTO t_case_601 (order_id, note) OUTPUT inserted.id, inserted.note VALUES (3, N'refund')"),
    ("sqlserver", "UPDATE o SET o.tags = c.country FROM orders AS o INNER JOIN customers AS c ON c.id = o.customer_id WHERE o.id = 4"),
    ("sqlserver", "MERGE INTO t_case_602 AS tgt USING customers AS src ON tgt.customer_id = src.id WHEN MATCHED THEN UPDATE SET tgt.total = tgt.total + 0 WHEN NOT MATCHED THEN INSERT (customer_id, total) VALUES (src.id, 0)"),
    ("sqlserver", "ALTER TABLE t_case_601 ADD channel NVARCHAR(20) NULL"),
    ("sqlserver", "CREATE NONCLUSTERED INDEX ix_t601_order ON t_case_601 (order_id) INCLUDE (qty)"),
    ("sqlserver", "CREATE OR ALTER VIEW dbo.paid_orders AS SELECT id, customer_id, amount FROM orders WHERE status = 'paid'"),
    # sqlserver: sessions and admin
    ("sqlserver", "SET NOCOUNT ON"),
    ("sqlserver", "BEGIN TRAN"),
    ("sqlserver", "COMMIT TRAN"),
    ("sqlserver", "SELECT @@VERSION AS version"),
    ("sqlserver", "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"),
    ("sqlserver", "CREATE OR ALTER PROCEDURE dbo.mark_paid @oid INT AS UPDATE orders SET status = 'paid' WHERE id = @oid"),
    ("sqlserver", "EXEC dbo.mark_paid @oid = 2"),
    # What a SET asks for is done as asked, or refused by name with the way out (a SET answered and not done
    # would be a quiet lie): each of these says which, its SQLSTATE and words.
    ("postgres", "SET ROLE analyst", ("0A000", "GRANT analyst TO")),
    ("postgres", "SET ROLE NONE"),
    ("postgres", "SET SESSION AUTHORIZATION 'ann'", ("0A000", "sign in as that user")),
    ("postgres", "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
    ("postgres", "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"),
    ("postgres", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", ("0A000", "ISOLATION LEVEL REPEATABLE READ")),
    ("postgres", "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", ("0A000", "granted SELECT only")),
    ("postgres", "SET default_transaction_isolation = 'serializable'", ("0A000", "ISOLATION LEVEL REPEATABLE READ")),
    ("postgres", "SET client_encoding = 'LATIN1'", ("22023", "client_encoding = 'UTF8'")),
    ("postgres", "SET client_encoding = 'UTF8'"),
    ("postgres", "SET DateStyle = 'SQL, DMY'", ("22023", "DateStyle = 'ISO'")),
    ("postgres", "SET DateStyle = 'ISO, MDY'"),
    ("postgres", "SET standard_conforming_strings = off", ("22023", "backslash")),
    ("postgres", "SHOW no_such_setting", ("42704", "no_such_setting")),
    ("postgres", "SET datafusion.runtime.memory_limit = '1G'", ("55P02", "--memory-gb")),
    ("duckdb", "SET memory_limit = '1GB'", ("55P02", "--memory-gb")),
    ("mysql", "SET NAMES utf8mb4"),
    ("mysql", "SET NAMES latin1", ("22023", "UTF-8")),
    ("mysql", "SET autocommit = 0", ("22023", "BEGIN")),
    ("mysql", "SET sql_safe_updates = 1, autocommit = 1"),
    ("mysql", "SET @region = 'eu'", ("0A000", "$region = …")),
    ("snowflake", "SET (lo, hi) = (1, 10)", ("0A000", "$lo = 1")),
    ("snowflake", "SELECT count(*) FROM orders WHERE amount > $min_amount", ("42P02", "$min_amount = 20 makes one")),
]


def run(port, sql, session=None):
    """One statement, one request (in `session`, if given): (ok, sqlstate, message). A refusal's SQLSTATE is its
    x-pondra-sqlstate header (src/server.rs's E sets it on every error); a JSON body's code and error
    serve when there is no header, and the body's text is the message otherwise."""
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    try:
        c.request("POST", "/sql", sql.encode(), {"x-pondra-session": session} if session else {})
        r = c.getresponse()
        body = r.read().decode("utf-8", "replace")
    except (OSError, http.client.HTTPException) as e:  # no answer within 60 s, or the node went away
        return False, None, f"no answer ({type(e).__name__}): {e}"
    finally:
        c.close()
    if r.status == 200:
        return True, None, ""
    state = r.getheader("x-pondra-sqlstate")
    try:
        j = json.loads(body)
    except ValueError:
        j = None
    if isinstance(j, dict):
        return False, state or j.get("code"), str(j.get("error", body))
    return False, state, body


def met(r):
    """Whether a case answered as it should: it ran (no expectation), or it was refused with its
    SQLSTATE and words."""
    if r["expect"] is None:
        return r["ok"]
    code, words = r["expect"]
    return not r["ok"] and r["sqlstate"] == code and words.lower() in r["message"].lower()


def answer(node, port, only=""):
    """SETUP, then every case (of the dialects in `only`, a comma list, or all), each dialect's in a
    session of its own: the rows, and how many times a case took the node down (it is started
    again on the same lake)."""
    for sql in SETUP:
        ok, code, msg = run(port, sql)
        if not ok:
            sys.exit(f"SETUP failed: {' '.join(sql.split())[:160]}\n  [{code}] {msg}")
    rows, downs = [], 0
    for case in CASES:
        if only and case[0] not in only.split(","):
            continue
        dialect, sql = case[0], case[1]
        expect = list(case[2]) if len(case) > 2 else None
        ok, code, msg = run(port, sql, f"dialects-{dialect}")
        rows.append({"dialect": dialect, "sql": sql, "ok": ok, "sqlstate": code, "message": msg, "expect": expect})
        if not node.alive():  # (the statement killed the node: start it again on the same lake)
            downs += 1
            node.start()
    return rows, downs


def main():
    harness.A = argparse.Namespace(s3=False, keep=False, port=A.port)
    node = Node(harness.new_lake(), A.port).start()
    rows, downs = answer(node, A.port, A.only)
    if downs:
        print(f"{downs} case(s) took the node down")
    with open(A.out, "w") as f:
        json.dump(rows, f, indent=1)

    print(f"{'dialect':<10} {'ran':>5} {'refused':>8}")
    for d in dict.fromkeys(r["dialect"] for r in rows):
        mine = [r for r in rows if r["dialect"] == d]
        ran = sum(r["ok"] for r in mine)
        print(f"{d:<10} {ran:>5} {len(mine) - ran:>8}")
    groups = {}
    for r in rows:
        if not r["ok"]:
            groups.setdefault(re.sub(r"\d+", "N", " ".join(r["message"].split())[:90]), []).append(r)
    print()
    for key, group in sorted(groups.items(), key=lambda g: -len(g[1])):
        states = ",".join(sorted({str(r["sqlstate"]) for r in group}))
        print(f"{len(group):>4}  [{states}]  {key}")
        print(f"      e.g. {' '.join(group[0]['sql'].split())[:160]}")

    misses = [r for r in rows if not met(r)]
    print(f"\n{len(rows)} cases answered, {len(misses)} missed their expectation")
    for r in ([] if A.report else misses):  # (--report: the groups above are the list)
        want = "ran" if r["expect"] is None else f"refused as {r['expect'][0]}, words {r['expect'][1]!r}"
        got = "ran" if r["ok"] else f"refused as {r['sqlstate']}: {' '.join(r['message'].split())[:160]}"
        print(f"MISS [{r['dialect']}] {' '.join(r['sql'].split())[:160]}\n     want {want}\n     got  {got}")
    sys.exit(0 if A.report or not misses else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9310)
    ap.add_argument("--out", default="dialects.json")
    ap.add_argument("--only", default="", help="dialects to run, a comma list (default: all)")
    ap.add_argument("--report", action="store_true", help="exit 0 once every case is answered, misses or not")
    A = ap.parse_args()
    main()
