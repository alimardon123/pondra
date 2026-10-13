// `open()` in JavaScript, checked (ADR-061 step 6; js/index.js). inprocess_check.py runs each child here as a
// process of its own, under Node (under Bun for js_bun), because a process holds one node and its settings are
// fixed by its first open. A child prints one line, `FACTS <json>`, which the Python script judges.
//
//   node tools/inprocess_check.mjs <child>
//
// The library is found as open() finds it: PONDRA_LIBRARY, else the platform's npm package.
import { mkdtempSync, readdirSync, rmSync, writeFileSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { col, open } from "../js/index.js";

// (writeSync, not console.log: js_exit ends the process right after its line, and a pipe may be asynchronous)
const emit = (facts) => writeSync(1, `FACTS ${JSON.stringify(facts)}\n`);
const scratch = () => mkdtempSync(join(tmpdir(), "inprocess-check-"));
const settle = () => new Promise((r) => setTimeout(r, 1000)); // (a socket the node closed is let go a moment after)
// What is in the temporary folder, which is also the current one: inprocess_check.py gives js_sql a new, empty one.
const writtenHere = () => readdirSync(tmpdir(), { recursive: true }).map((p) => relative(tmpdir(), join(tmpdir(), p))).sort();

// The message of an open that is refused, or null when it opens (and is closed again).
async function refusal(dir, settings) {
  try {
    await (await open(dir, settings)).close();
    return null;
  } catch (e) {
    return e.message;
  }
}

// A table t of three rows, made the way a program would: CREATE, then an append.
async function sample(db) {
  await db.sql("CREATE TABLE t (id BIGINT, v VARCHAR)");
  await db.append("t", [{ id: 1, v: "a" }, { id: 2, v: "b" }, { id: 3, v: "c" }]);
}

async function js_sql() {
  const db = await open();
  const lake = db.lake;
  await sample(db);
  const rows = (await db.sql("SELECT * FROM t ORDER BY id")).length;
  const frame = (await db.table("t").filter(col("id").gt(1))).length; // (a frame: it runs when awaited)
  await db.writeTable("w", [{ a: 1 }, { a: 2 }]); // (a table made from rows, its columns from their values)
  const written = Number((await db.sql("SELECT count(*) AS n FROM w"))[0].n);
  const dir = scratch();
  const file = join(dir, "here.csv");
  writeFileSync(file, "id,name\n1,a\n2,b\n3,c\n");
  const fileRows = Number((await db.sql(`SELECT count(*) AS n FROM '${file}'`))[0].n); // (a file on this machine, by name)
  rmSync(dir, { recursive: true, force: true });
  let live = null; // (a live query's first answer; the break ends it on the node)
  for await (const answer of db.live("SELECT count(*) AS n FROM t")) {
    live = Number(answer[0].n);
    break;
  }
  const whileOpen = writtenHere();
  await db.close();
  return { lake, rows, frame, written, file_rows: fileRows, live, while_open: whileOpen, after_close: writtenHere() };
}

async function js_reopen() {
  const root = scratch();
  try {
    const lake = join(root, "lake");
    // (the settings the process's first open fixes: every open here gives the same ones, so the refusals below
    // are for what each check is about)
    const settings = { memory_gb: 1 };
    let db = await open(lake, settings);
    await db.sql("CREATE TABLE r (id INT)");
    await db.sql("INSERT INTO r VALUES (1), (2)");
    await db.close();
    db = await open(lake, settings);
    const count = Number((await db.sql("SELECT count(*) AS n FROM r"))[0].n);
    const second = await refusal(join(root, "other"), settings); // (one node at a time: a second open is refused)
    await db.close();
    const misspelt = await refusal(join(root, "two"), { memory_gbb: 1 });
    await (await open(join(root, "three"), settings)).close(); // (the same settings: this one opens)
    const changed = await refusal(join(root, "four"), { memory_gb: 2 });
    return { count, second, misspelt, changed };
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

async function js_cycles() {
  if (process.platform !== "linux") return { skip: true };
  const cycle = async () => {
    const db = await open();
    await db.sql("SELECT 1");
    await db.close();
  };
  const census = () => [readdirSync("/proc/self/task").length, readdirSync("/proc/self/fd").length];
  await cycle(); // (the first open starts the library's threads and the thread pool's, which stay)
  await settle();
  const [t0, f0] = census();
  for (let i = 0; i < 20; i++) await cycle();
  await settle();
  const [t1, f1] = census();
  return { threads: [t0, t1], files: [f0, f1], cycles: 20 };
}

async function js_exit() {
  const db = await open(join(scratch(), "lake")); // (a folder: what its node leaves there says it was closed)
  await db.sql("CREATE TABLE e (id INT)");
  await db.sql("INSERT INTO e VALUES (1)");
  emit({ lake: db.lake });
  process.exit(0); // (no close(): the exit event closes the node, which steps down from its lake)
}

async function js_end() {
  const db = await open(join(scratch(), "lake"));
  await db.sql("CREATE TABLE e (id INT)");
  await db.sql("INSERT INTO e VALUES (1)");
  emit({ lake: db.lake }); // (no close(), and nothing left to do: Node ends, and the library's cleanup hook closes the node)
}

async function js_bun() {
  const db = await open();
  await sample(db);
  const count = Number((await db.sql("SELECT count(*) AS n FROM t"))[0].n);
  const lake = db.lake;
  await db.close();
  return { count, lake };
}

const CHILDREN = { js_sql, js_reopen, js_cycles, js_exit, js_end, js_bun };
const child = CHILDREN[process.argv[2]];
if (!child) {
  console.error(`no child ${process.argv[2]}: ${Object.keys(CHILDREN).join(", ")}`);
  process.exit(2);
}
try {
  const facts = await child();
  if (facts !== undefined) emit(facts); // (js_exit and js_end said theirs before the process ends)
} catch (e) {
  console.error(e?.stack ?? e);
  process.exit(1);
}
