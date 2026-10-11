#!/usr/bin/env node
// The JavaScript side of tools/js_frames_check.py.
//
//   node js_frames_pipelines.mjs pipelines <url>   every pipeline of frames_check.py, as js/frame.js builds it:
//                                                  its SQL, its rows and its columns, as JSON on stdout
//   node js_frames_pipelines.mjs misc <dir>        the checks that read files and write tables, on a node this
//                                                  process starts in <dir> (its local files need that node: invariant 61)
//
// The pipelines are built in the order of frames_check.py's pipelines(), with the same base frames, so
// the CTE names (`_1`, `_2`, … from one counter per process) come out as Python's do.
import { col, concat, connect, len, lit, local, when } from "../js/index.js";

// [name, build(orders, users, quotes), ordered]: frames_check.py's pipelines, in its order.
const PIPELINES = [
  ["filter + select", (o) => o.filter(col("qty").gt(3)).select("id", "user", "qty")],
  ["with_columns: new and replaced", (o) => o.withColumns(col("qty").mul(col("price")).alias("total"), col("price").mul(2).alias("price"))],
  ["group_by: sum mean min max count n_unique len", (o) => o.groupBy("user").agg(
    col("qty").sum().alias("s"), col("price").mean().alias("m"), col("price").min().alias("lo"), col("price").max().alias("hi"),
    col("qty").count().alias("c"), col("item").nUnique().alias("items"), len())],
  ["group_by two keys, std and var", (o) => o.groupBy("user", "item").agg(col("price").std().alias("sd"), col("price").var().alias("v"))],
  ["sort desc (nulls first) + limit", (o) => o.sort("price", "id", { descending: [true, false] }).limit(15), true],
  ["sort nulls_last, then filter and select (the sort kept)", (o) => o.sort("qty", "id", { nullsLast: true }).filter(col("price").gt(20)).select("id", "qty").head(20), true],
  ["join inner", (o, u) => o.join(u, { on: "user" })],
  ["join left", (o, u) => o.join(u, { on: "user", how: "left" })],
  ["join full", (o, u) => u.join(o.select("user", "id"), { on: "user", how: "full" })],
  ["join semi / anti", (o, u) => concat([u.join(o.filter(col("qty").eq(9)), { on: "user", how: "semi" }), u.join(o, { on: "user", how: "anti" })])],
  ["join left_on/right_on, a clashing name", (o, u) => o.select("id", "user", "item").join(u.withColumns(col("tier").alias("item")), { leftOn: "user", rightOn: "user" })],
  ["join cross (small)", (o, u) => u.filter(col("tier").eq("gold")).select("user").join(u.filter(col("tier").eq("new")).select("user"), { how: "cross" })],
  ["join_asof backward by", (o, u, q) => o.filter(col("item").isIn(["tea", "cake"])).sort("ts").joinAsof(q.sort("at"), { leftOn: "ts", rightOn: "at", by: "item" }).select("id", "px")],
  ["unique", (o) => o.select("user", "item").unique()],
  ["drop, rename, cast", (o) => o.drop("ts", "region").rename({ qty: "n" }).cast({ n: "Float64" })],
  ["when / then / otherwise", (o) => o.select("id", when(col("qty").gt(6)).then(lit("big")).when(col("qty").gt(2)).then(lit("mid")).otherwise(lit("small")).alias("size"))],
  ["is_in, is_between, is_null, fill_null", (o) => o.select("id", col("item").isIn(["tea", "bun"]).alias("a"), col("price").isBetween(5, 10).alias("b"),
    col("qty").isNull().alias("c"), col("qty").fillNull(-1).alias("d"))],
  ["fill_null over the frame, drop_nulls", (o) => o.fillNull(0).dropNulls("region")],
  ["str: case, contains, starts_with, len, slice, replace_all", (o) => o.select("id", col("item").str.toUppercase().alias("up"), col("item").str.contains("ea").alias("ea"),
    col("item").str.startsWith("co").alias("co"), col("item").str.lenChars().alias("n"), col("item").str.slice(1, 2).alias("s"),
    col("item").str.replaceAll("e", "E").alias("r"))],
  ["dt: year month day weekday hour, truncate, date", (o) => o.select("id", col("ts").dt.year().alias("y"), col("ts").dt.month().alias("m"), col("ts").dt.day().alias("d"),
    col("ts").dt.weekday().alias("wd"), col("ts").dt.hour().alias("h"), col("ts").dt.truncate("1d").alias("day"), col("ts").dt.truncate("6h").alias("q"),
    col("ts").dt.date().alias("date"))],
  ["over: sum, rank dense, shift, cum_sum", (o) => o.filter(col("qty").isNotNull()).select("id", col("qty").sum().over("user").alias("s"),
    col("qty").rank("dense").over("user").alias("r"), col("qty").shift(1).over("user", { orderBy: "id" }).alias("prev"),
    col("qty").cumSum().over("user", { orderBy: "id" }).alias("run"))],
  ["rank average", (o) => o.filter(col("user").eq("u3")).select("id", col("qty").rank().alias("r"))],
  ["concat vertical and diagonal", (o, u) => concat([o.select("id", "user").limit(0),
    concat([u.select("user"), o.select("user", "id").filter(col("id").lt(5))], { how: "diagonal" })], { how: "diagonal" })],
  ["division, floor division, modulo", (o) => o.filter(col("qty").gt(0)).select("id", col("id").div(col("qty")).alias("d"),
    col("id").mod(col("qty")).alias("m"), col("price").floorDiv(3).alias("f"))],
  ["floor division, negatives too (rounded down; integers stay integers)", (o) => o.filter(col("qty").gt(0)).select("id",
    col("id").floorDiv(col("qty")).alias("a"), col("id").neg().floorDiv(col("qty")).alias("b"), col("price").floorDiv(-3).alias("c"),
    lit(100).floorDiv(col("qty")).alias("d"))],
  ["round, abs, sqrt, clip", (o) => o.select("id", col("price").round(1).alias("r"), col("price").neg().abs().alias("a"),
    col("price").sqrt().alias("s"), col("price").clip(5, 30).alias("c"))],
  ["SQL snippets inside frame methods", (o) => o.filter("qty > 2 AND item LIKE 't%'").withColumns("qty * price AS total")
    .groupBy("item").agg("sum(total) AS revenue", "count(*) AS n")],
];

const oneLine = (e) => `${e.name}: ${String(e.message).slice(0, 400)}`;

/** Every pipeline: its SQL text, its rows (the node's JSON: a null is left out) and its column names. */
async function pipelines(url) {
  const db = connect(url, { onNotice: null });
  const [o, u, q] = [db.table("orders"), db.table("users"), db.table("quotes")];
  const out = [];
  for (const [name, build, ordered = false] of PIPELINES) {
    const row = { name, ordered };
    try {
      const frame = build(o, u, q);
      row.sql = await frame.sql();
      row.rows = await frame.rows();
      row.columns = await frame.columns();
    } catch (e) {
      row.error = oneLine(e);
    }
    out.push(row);
  }
  console.log(JSON.stringify({ pipelines: out }));
}

// The rows the write checks send (the Python check expects them back: a Date is the naive UTC time).
const ROWS = [
  { id: 1, name: "ann", score: 2.5, ok: true, at: new Date("2026-09-01T12:00:00Z") },
  { id: 2, name: null, score: 3, ok: false, at: null },
];
const ROWS_AGAIN = [{ id: 7, name: "bo", score: 0.25, ok: false, at: new Date("2026-09-02T00:00:00Z") }];
const THIRD = [{ id: 9, name: "cy", score: 1, ok: true, at: null }];

/** The checks that read files and write tables, each reported as its value (or its error). */
async function misc(dir) {
  const db = await local(`${dir}/lake`, { python: false, onNotice: null });
  const out = {};
  const check = async (name, fn) => {
    try {
      out[name] = await fn();
    } catch (e) {
      out[name] = { error: oneLine(e) };
    }
  };
  await check("select1", () => db.sql("SELECT 1 AS x"));
  await check("create_t2", async () => {
    await db.sql("CREATE TABLE t2 (a INT)");
    return db.sql("SELECT count(*) AS n FROM t2").item();
  });
  await check("read_parquet", () => db.readParquet(`${dir}/readable.parquet`).rows());
  await check("write_rows", async () => {
    await db.writeTable("wrows", ROWS);
    return db.table("wrows").rows();
  });
  await check("write_overwrite", async () => {
    await db.writeTable("wrows", ROWS_AGAIN, { mode: "overwrite" });
    return db.table("wrows").rows();
  });
  await check("write_append", async () => {
    await db.writeTable("wrows", THIRD, { mode: "append" });
    return db.table("wrows").rows();
  });
  await check("write_frame", async () => {
    await db.readParquet(`${dir}/readable.parquet`).filter(col("v").gt(2)).writeTable("wframe");
    return db.table("wframe").rows();
  });
  console.log(JSON.stringify(out));
  await db.close();
}

const [mode, arg] = process.argv.slice(2);
if (mode === "pipelines") await pipelines(arg);
else if (mode === "misc") await misc(arg);
else {
  console.error("usage: js_frames_pipelines.mjs pipelines <url> | misc <dir>");
  process.exit(2);
}
