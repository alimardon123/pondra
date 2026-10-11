#!/usr/bin/env node
// The JavaScript side of tools/js_frames_check.py.
//
//   node js_frames_pipelines.mjs pipelines <url>   every pipeline of frames_check.py, as js/frame.js builds it:
//                                                  its SQL, its rows and its columns, as JSON on stdout
//   node js_frames_pipelines.mjs misc <dir>        the checks that read files and write tables, on a node this
//                                                  process starts in <dir> (its local files need that node: invariant 61)
//   node js_frames_pipelines.mjs client <url> <tls-url> <dir>
//                                                  the client's calls (transactions, routines, views, watch, lookup, fetch,
//                                                  ca) on the node Python started at <url>, and on a TLS node at <tls-url>;
//                                                  <dir> has ca.pem (that node's certificate, self-signed) and cases.json
//                                                  ({ key }: the lookup's key, which Python wrote). JSON on stdout
//   node js_frames_pipelines.mjs starting <dir>    local()'s own refusals and its node's log, in <dir>; JSON on stdout
//
// The pipelines are built in the order of frames_check.py's pipelines(), with the same base frames, so
// the CTE names (`_1`, `_2`, … from one counter per process) come out as Python's do.
import { readFileSync } from "node:fs";
import { binary, col, concat, connect, len, lit, local, Pondra, when } from "../js/index.js";

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
  ["unique(keep='none') on a subset: the values that occur once", (o) => o.filter(col("id").lt(300)).select("id", "user", "item").unique(["user", "item"], { keep: "none" })],
  ["unique(keep='none') of every column", (o) => o.filter(col("id").lt(300)).select("user", "item", "qty").unique(null, { keep: "none" })],
  ["sort, then unique(keep='first') on a subset, the sort kept", (o) => o.sort(["user", "id"]).select("id", "user", "item").unique("user", { keep: "first", maintainOrder: true }), true],
  ["sort, then unique(keep='last'), descending with nulls last", (o) => o.sort(["user", "qty", "id"], { descending: [false, true, false], nullsLast: [false, true, false] })
    .select("id", "user", "qty").unique("user", { keep: "last", maintainOrder: true }), true],
  ["sort, then unique(keep='last') of every column", (o) => o.sort(["user", "item"]).select("user", "item").unique(null, { keep: "last", maintainOrder: true }), true],
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
  // Polars' keep and maintain_order, and groupBy's maintainOrder: refused by name where a frame has no
  // row order, before anything runs (each refusal's message, or null if it was taken)
  const refusal = (fn) => {
    try {
      fn();
      return null;
    } catch (e) {
      return oneLine(e);
    }
  };
  out.refusals = {
    keepFirst: refusal(() => db.table("orders").unique("user", { keep: "first" })),
    keepLast: refusal(() => db.table("orders").unique("user", { keep: "last" })),
    keepUnknown: refusal(() => db.table("orders").unique("user", { keep: "x" })),
    maintainOrder: refusal(() => db.table("orders").unique("user", { maintainOrder: true })),
    groupMaintainOrder: refusal(() => db.table("orders").groupBy("user", { maintainOrder: true })),
  };
  console.log(JSON.stringify(out));
  await db.close();
}

/** The client's own calls, on the node Python started at `url` and on a TLS node at `tlsUrl` whose
 * certificate is dir/ca.pem. Each result is its value, or { error, code, cause, sqlstate } for the call that
 * threw. The order matters: a CA is trusted by this whole process from the call that first uses it on, so
 * the call that must fail without it comes before any that adds it. */
async function client(url, tlsUrl, dir) {
  const { key } = JSON.parse(readFileSync(`${dir}/cases.json`, "utf8"));
  const out = {};
  const check = async (name, fn) => {
    try {
      out[name] = await fn();
    } catch (e) {
      out[name] = { error: e?.message ?? String(e), code: e?.code ?? null, cause: e?.cause?.code ?? null, sqlstate: e?.sqlstate ?? null };
    }
  };
  const db = connect(url, { onNotice: null });
  const count = async (table) => Number((await db.sql(`SELECT count(*) AS n FROM ${table}`))[0].n);

  await check("lookup", () => db.lookup("kt", key));

  await check("transactionCommit", async () => {
    await db.sql("CREATE TABLE js_tx (a INT)");
    const inside = await db.transaction(async (tx) => {
      await tx.sql("INSERT INTO js_tx VALUES (1), (2)");
      return Number((await tx.sql("SELECT count(*) AS n FROM js_tx"))[0].n); // (its own writes, read back)
    });
    return { inside, after: await count("js_tx") };
  });

  await check("transactionRollback", async () => {
    let thrown = null;
    try {
      await db.transaction(async (tx) => {
        await tx.sql("INSERT INTO js_tx VALUES (3)");
        throw new Error("stop here");
      });
    } catch (e) {
      thrown = e.message; // (the block's own error, as it was thrown)
    }
    return { thrown, count: await count("js_tx") };
  });

  await check("routines", async () => {
    await db.createFunction("twice", { params: { x: "INT" }, returns: "INT", language: "sql", body: "SELECT x * 2" });
    const value = Number((await db.sql("SELECT twice(21) AS v"))[0].v);
    return { value, listed: Object.keys(await db.routines()).includes("twice") };
  });

  await check("procedure", async () => {
    await db.sql("CREATE TABLE js_procs (a INT)");
    await db.createProcedure("add_row", { params: { n: "INT" }, language: "sql", body: "INSERT INTO js_procs VALUES ($n)" });
    await db.call("add_row", 5);
    return (await db.sql("SELECT a FROM js_procs")).map((r) => Number(r.a));
  });

  await check("viewTemporary", async () => {
    const mine = await db.view("js_tv", "SELECT 7 AS x", { temporary: true }); // (awaited: its rows)
    const otherError = await connect(url, { onNotice: null }).sql("SELECT x FROM js_tv").then(() => null, (e) => e.message);
    return { mine, otherError };
  });

  await check("watchMarks", async () => {
    await db.sql("CREATE TABLE js_wt (a INT)");
    await db.sql("INSERT INTO js_wt VALUES (1), (2)");
    const it = db.watch("js_wt", { after: 0 });
    const got = [(await it.next()).value, (await it.next()).value];
    await db.sql("INSERT INTO js_wt VALUES (3)");
    got.push((await it.next()).value); // (the mark after 1 and 2 is read on the way, so position is set now)
    const position = db.position ?? null;
    await it.return();
    const later = db.watch("js_wt", { after: position });
    const first = (await later.next()).value;
    await later.return();
    return { rows: got.map((r) => r.a), markYielded: got.some((r) => "_after" in r), position, laterFirst: first.a };
  });

  await check("closeArgument", () => db.close(1).then(() => "closed", (e) => e.message));

  await check("caMissingFile", () => new Pondra(url, { ca: `${dir}/absent.pem`, onNotice: null }).sql("SELECT 1 AS x"));
  await check("tlsWithoutCa", () => new Pondra(tlsUrl, { onNotice: null }).sql("SELECT 1 AS x"));
  await check("tlsWithCa", () => new Pondra(tlsUrl, { ca: `${dir}/ca.pem`, onNotice: null }).sql("SELECT 1 AS x"));
  await check("caOnPlainNode", () => new Pondra(url, { ca: `${dir}/ca.pem`, onNotice: null }).sql("SELECT 1 AS x"));

  await check("fetchOption", async () => {
    const asked = [];
    const rows = await new Pondra(url, { onNotice: null, fetch: (u, o) => (asked.push(u), fetch(u, o)) }).sql("SELECT 1 AS x");
    return { rows, asked: asked.length };
  });

  // The SQL text of the CREATEs, as they are sent: Python's must be the same for the same arguments.
  const sent = [];
  const recorder = connect(url, { onNotice: null });
  const sendSql = recorder.sql.bind(recorder);
  recorder.sql = (query, params) => (sent.push(query), sendSql(query, params));
  await check("sqlText", async () => {
    await recorder.createFunction("text_twice", { params: { x: ["INT", 1] }, returns: "INT", language: "sql", body: "SELECT x * 2", strict: true, volatility: "immutable" });
    await recorder.createProcedure("text_proc", { params: { n: "INT" }, language: "sql", body: "SELECT $n" });
    await recorder.view("text_view", "SELECT 1 AS x");
    return sent.filter((t) => t.startsWith("CREATE"));
  });

  console.log(JSON.stringify(out));
}

/** local()'s own refusals, and a node that doesn't start: its error names its log (the file is in the temp
 * folder; Python reads it). */
async function starting(dir) {
  const out = {};
  const check = async (name, fn) => {
    try {
      await fn();
      out[name] = "no error";
    } catch (e) {
      out[name] = e.message;
    }
  };
  await check("timeoutMs", () => local(`${dir}/timeout-ms`, { timeoutMs: 1 }));
  await check("badFlag", () => local(`${dir}/bad-flag`, { python: false, flags: ["--no-such-flag"] }));
  console.log(JSON.stringify(out));
}

const [mode, ...args] = process.argv.slice(2);
if (mode === "pipelines") await pipelines(args[0]);
else if (mode === "misc") await misc(args[0]);
else if (mode === "client") await client(...args);
else if (mode === "starting") await starting(args[0]);
else {
  console.error("usage: js_frames_pipelines.mjs pipelines <url> | misc <dir> | client <url> <tls-url> <dir> | starting <dir>");
  process.exit(2);
}
