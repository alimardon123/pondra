"""Spark 4 (local[*]) side of the benchmark. Prints one JSON line of timings.
  spark_bench.py batch  N DIR    generate N rows -> Parquet, then the shared queries (cold, warm)
  spark_bench.py stream N KEYS   keyed running aggregation over a bounded stream (rate-micro-batch)
  spark_bench.py etl    N        stateless streaming filter/projection"""
import json, os, sys, time
t0 = time.time()
from pyspark.sql import SparkSession
from queries import GEN, QUERIES

spark = (SparkSession.builder.master("local[*]").config("spark.driver.memory", "3g")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
mode, n = sys.argv[1], int(sys.argv[2])
out = {"engine": "spark", "mode": mode, "rows": n, "startup_s": round(time.time() - t0, 2)}

if mode == "batch":
    d = sys.argv[3]
    t = time.time()
    spark.range(n).selectExpr(*[f"{e} AS {c}" for c, e in GEN.items()]).write.mode("overwrite").parquet(f"{d}/events")
    out["write_s"] = round(time.time() - t, 2)
    spark.read.parquet(f"{d}/events").createOrReplaceTempView("events")
    spark.range(1000).selectExpr("concat('c', CAST(id AS STRING)) AS category", "concat('r', CAST(id % 10 AS STRING)) AS region").createOrReplaceTempView("dims")
    for name, q in QUERIES.items():
        times = []
        for _ in range(2):
            t = time.time()
            spark.sql(q).collect()
            times.append(round(time.time() - t, 3))
        out[name] = times
elif mode == "stream":
    keys, batch = int(sys.argv[3]), int(os.environ.get("SPARK_BATCH", 500_000))
    src = (spark.readStream.format("rate-micro-batch").option("rowsPerBatch", batch).option("numPartitions", 2).load()
           .selectExpr(f"value % {keys} AS user_id", "CAST(value % 10000 AS DOUBLE) / 100 AS amount"))
    agg = src.groupBy("user_id").agg({"amount": "sum", "*": "count"})
    q = agg.writeStream.outputMode("update").format("noop").option("checkpointLocation", f"/tmp/spark-ckpt-{time.time()}").start()
    t, durs = time.time(), []
    while q.recentProgress == [] or sum(p["numInputRows"] for p in q.recentProgress) < n:
        time.sleep(0.2)
    q.stop()
    p = q.recentProgress
    total = sum(x["numInputRows"] for x in p)
    durs = sorted(x["durationMs"]["triggerExecution"] for x in p if x["numInputRows"])
    out.update(processed=total, secs=round(time.time() - t, 2), rows_per_s=round(total / (time.time() - t)),
               batch_ms_p50=durs[len(durs) // 2], batch_ms_max=durs[-1], batches=len(durs))
elif mode == "etl":
    src = (spark.readStream.format("rate-micro-batch").option("rowsPerBatch", 500_000).option("numPartitions", 2).load()
           .selectExpr("value AS id").selectExpr(*[f"{e} AS {c}" for c, e in GEN.items()]).where("amount > 50"))
    q = src.writeStream.format("noop").option("checkpointLocation", f"/tmp/spark-ckpt-{time.time()}").start()
    t = time.time()
    while q.recentProgress == [] or sum(p["numInputRows"] for p in q.recentProgress) < n:
        time.sleep(0.2)
    q.stop()
    total = sum(x["numInputRows"] for x in q.recentProgress)
    out.update(processed=total, secs=round(time.time() - t, 2), rows_per_s=round(total / (time.time() - t)))
print(json.dumps(out), flush=True)
spark.stop()
