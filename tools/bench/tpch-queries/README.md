The 22 TPC-H queries as the DataFusion benchmarks write them (`benchmarks/queries/q1.sql` …
`q22.sql` in apache/datafusion, Apache License 2.0), q15 as a view plus a query. `tools/bench/tpch.py`
turns q15 into one statement. The tools default to `~/tpch/queries`; pass this folder with
`--queries tools/bench/tpch-queries` (the cloud workflow does).
