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
await db.close();
```

`npx pondra` opens a SQL shell on `./lake`; `npx pondra serve --dir s3://bucket/lake` runs a node.
