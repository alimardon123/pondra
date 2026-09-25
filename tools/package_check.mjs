// What `npm install pondra` gives, tried: the binary for this platform is found, a node starts on
// a new lake, and a table, an exactly-once append, a view and a query work.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { local } from "pondra";

const db = await local(join(mkdtempSync(join(tmpdir(), "pondra-check-")), "lake"));
try {
  await db.sql("CREATE TABLE t (id BIGINT, v VARCHAR)");
  await db.view("per_v", "SELECT v, count(*) AS n FROM t GROUP BY v");
  await db.append("t", [{ id: 1, v: "a" }, { id: 2, v: "b" }, { id: 3, v: "a" }]);
  const rows = await db.sql("SELECT v, n FROM per_v ORDER BY v");
  if (JSON.stringify(rows) !== JSON.stringify([{ v: "a", n: 2 }, { v: "b", n: 1 }])) throw new Error(JSON.stringify(rows));
  console.log("ok:", JSON.stringify(rows));
} finally {
  await db.close();
}
