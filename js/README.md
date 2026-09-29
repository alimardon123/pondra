# pondra

Pondra is a streamhouse in one binary: streaming ingest, a lake in your folder or bucket, and SQL
that spreads over any number of nodes. This package installs the binary for your platform and a
small JavaScript client (no dependencies; Node 18+).

```js
import { local } from "pondra";
const db = await local("lake");                       // a node on ./lake
await db.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)");
await db.append("events", [{ user: "ann", amount: 5 }]); // exactly once
console.log(await db.sql("SELECT user, sum(amount) AS total FROM events GROUP BY user"));
await db.call("send_report", "2026-09-27");            // a stored procedure; what it printed: db.notices
const run = await db.start("send_report", "2026-09-28"); // …started, not waited for (pondra.runs)
for await (const rows of db.live("SELECT user, sum(amount) AS total FROM events GROUP BY user")) {
  console.log(rows);                                   // now, and again each time a commit changes it
  break;
}
await db.sql("CREATE TEMP TABLE picked AS SELECT * FROM events WHERE amount > 1"); // this connection's own
await db.close();                                      // (its temporary tables end with it)
```

Files and other engines' tables are SQL's: `read_parquet`, `read_csv`, `read_json`, `read_delta`,
`read_iceberg`, and `COPY (…) TO '…' (FORMAT parquet | csv | json | delta | iceberg)`.

Functions and procedures written in SQL or Python (`CREATE FUNCTION`, `CREATE PROCEDURE … LANGUAGE
python`) run on the node; `local()` gives the node a Python with the `pondra` package if this
machine has one.

`npx pondra` opens a SQL shell on `./lake`; `npx pondra serve s3://bucket/lake` runs a node.
