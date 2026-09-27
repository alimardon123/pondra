// What `npm install pondra` gives, tried: the binary for this platform is found, a node starts on
// a new lake, and a table, an exactly-once append, a view, a query, parameters and a procedure work.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { local } from "pondra";

const db = await local(join(mkdtempSync(join(tmpdir(), "pondra-check-")), "lake"));
try {
  await db.sql("CREATE TABLE t (id BIGINT, v VARCHAR)");
  await db.view("per_v", "SELECT v, count(*) AS n FROM t GROUP BY v", { materialized: true }); // (kept up to date)
  await db.view("recent", "SELECT * FROM t WHERE id > 1"); // (a stored query)
  await db.append("t", [{ id: 1, v: "a" }, { id: 2, v: "b" }, { id: 3, v: "a" }]);
  const rows = await db.sql("SELECT v, n FROM per_v ORDER BY v");
  if (JSON.stringify(rows) !== JSON.stringify([{ v: "a", n: 2 }, { v: "b", n: 1 }])) throw new Error(JSON.stringify(rows));
  const recent = await db.sql("SELECT count(*) AS n FROM recent");
  if (recent[0].n !== 2) throw new Error(JSON.stringify(recent));
  // $name parameters, and a stored procedure called from JavaScript
  await db.sql("CREATE PROCEDURE count_v(v VARCHAR) LANGUAGE sql AS $$ SELECT count(*) AS n FROM t WHERE v = $v $$");
  const [one, two] = [await db.sql("SELECT n FROM per_v WHERE v = $v", { v: "b" }), await db.call("count_v", "a")];
  if (one[0].n !== 1 || two[0].n !== 2) throw new Error(JSON.stringify([one, two]));
  console.log("ok:", JSON.stringify(rows));
} finally {
  await db.close();
}
