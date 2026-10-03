// Pondra from JavaScript: SQL, exactly-once appends, key lookups and change feeds, over a node's
// HTTP API. No dependencies (Node 18+, or any runtime with fetch).
//
//   import { local, connect } from "pondra";
//   const db = await local("lake");                 // a node on ./lake, here; or
//   const db = connect("http://127.0.0.1:8080");    // one running somewhere
//   await db.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)");
//   await db.append("events", [{ user: "ann", amount: 5 }]);
//   console.log(await db.sql("SELECT user, sum(amount) AS total FROM events WHERE amount > $min GROUP BY user", { min: 1 }));
//   await db.call("load_day", "2026-09-27");             // a stored procedure (what it prints: db.notices)
//   for await (const row of db.watch("events")) { … }   // new rows as they commit
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { createServer } from "node:net";

const varName = (n) => { if (!/^[A-Za-z_]\w*$/.test(n)) throw new Error(`${n}: not a variable's name (letters, digits and _, not first a digit)`); return n; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export class Pondra {
  constructor(url = "http://127.0.0.1:8080", { token, user, password, onNotice = (n) => console.log(n) } = {}) {
    this.url = url.replace(/\/$/, "");
    this.token = token;
    this.basic = user ? `Basic ${Buffer.from(`${user}:${password}`).toString("base64")}` : null; // (a user: its name and password, or its token as the password)
    this.notices = []; // what the last statement's procedures printed
    this.onNotice = onNotice; // (each one, as it comes back; null: keep them quiet)
    this.producer = `js-${randomUUID().slice(0, 12)}`; // exactly-once: one name, increasing seq
    this.seq = 0;
    this.session = randomUUID(); // this connection's temporary tables and views, on the node until close()
  }

  /** One HTTP request to the node (what the methods below are made of). */
  async request(method, path, body, type) {
    const headers = { "x-pondra-session": this.session, ...(this.token ? { authorization: `Bearer ${this.token}` } : this.basic && { authorization: this.basic }), ...(type && { "content-type": type }), ...(this.owner && { "x-pondra-owner": this.owner }) };
    const r = await fetch(this.url + path, { method, body, headers });
    const said = r.headers.get("x-pondra-notices");
    this.notices = said ? JSON.parse(said) : [];
    if (this.onNotice) this.notices.forEach((n) => this.onNotice(n));
    if (!r.ok) throw Object.assign(new Error(`${r.status}: ${(await r.text()).slice(0, 500)}`), { sqlstate: r.headers.get("x-pondra-sqlstate") || "XX000", status: r.status }); // (Postgres's code: 23514 a CHECK, 40001 try again…)
    return r;
  }

  /** A query's rows, as objects; for other statements (CREATE, INSERT, UPDATE, DELETE, CALL,
   * several at once), the last one's outcome. `params`: values for `$name` in it. */
  async sql(query, params) {
    if (!params) return (await this.request("POST", "/sql", query)).json();
    return (await this.request("POST", "/sql", JSON.stringify({ sql: query, params }), "application/json")).json();
  }

  /** A file's statements in order, `$name` taking `params.name`: a `.sql` file here (or SQL itself)
   * runs from here; otherwise a file of the lake's (`etl/orders.sql`, `.py`, `.ipynb`: ADR-033) runs
   * on the node, as `CALL run(…)`, logged in `pondra.runs`. */
  async run(file, params = {}) {
    if (file.endsWith(".sql") && existsSync(file)) return this.sql(readFileSync(file, "utf8"), params);
    if (!/\.(sql|py|ipynb)$|^(files\/)?notebooks\/[\w.-]+$/.test(file)) return this.sql(file, params); // (else a file of the lake's, or a saved notebook by name)
    const names = Object.keys(params);
    const given = names.map((n, i) => `, "${n.replace(/"/g, '""')}" => $p${i}`).join("");
    return this.sql(`CALL run($file${given})`, { file, ...Object.fromEntries(names.map((n, i) => [`p${i}`, params[n]])) });
  }

  /** This connection's SQL variables (`DECLARE $day DATE = …`, `$day = …`: ADR-037), as an object
   * of their values. */
  async vars() {
    const names = (await this.sql("SELECT name FROM pondra.variables ORDER BY name")).map(r => r.name);
    if (!names.length) return {};
    const [row] = await this.sql(`SELECT ${names.map(n => `getvariable('${n}') AS "${n}"`).join(", ")}`);
    return Object.fromEntries(names.map(n => [n, row[n] ?? null]));
  }

  /** A variable's value (DuckDB's `getvariable`), null if none. */
  async getVariable(name) { return (await this.sql(`SELECT getvariable('${varName(name)}') AS value`))[0]?.value ?? null; }

  /** Set a variable (SQL's `$name = …`): `value` bound, never pasted in. */
  async setVariable(name, value) { await this.sql(`$${varName(name)} = $value`, { value }); }

  /** Forget a variable (`RESET VARIABLE`). */
  async resetVariable(name) { await this.sql(`RESET VARIABLE ${varName(name)}`); }

  /** A file's parameters (its DECLARE PARAMETERs: name, type, default, required, description), a file
   * of the lake's: `await db.parameters("etl/orders.sql")`. */
  async parameters(file) { return this.sql(`SELECT * FROM pondra.parameters('${String(file).replace(/'/g, "''")}')`); }

  /** A stored procedure (`CREATE PROCEDURE`), called, as Python's `con.call`: `await db.call("load_day", "2026-09-27")`. */
  async call(name, ...args) {
    return this.sql(`CALL ${name}(${args.map((_, i) => `$p${i}`).join(", ")})`, Object.fromEntries(args.map((a, i) => [`p${i}`, a])));
  }

  /** A stored procedure started on the node, not waited for (SQL's `pondra.start`): its run's id,
   * whose row of `pondra.runs` says how it went. */
  async start(name, ...args) {
    const given = args.map((_, i) => `, $p${i}`).join("");
    const params = { name, ...Object.fromEntries(args.map((a, i) => [`p${i}`, a])) };
    const [row] = await this.sql(`SELECT pondra.start($name${given})`, params);
    return row.run;
  }

  /** `call`'s name up to 0.22. */
  async callProcedure(name, ...args) {
    return this.call(name, ...args);
  }

  /** Append rows exactly once: a retry after a lost answer is recognised, not applied twice. */
  async append(table, rows, retries = 10) {
    const seq = ++this.seq;
    const body = rows.map((r) => JSON.stringify(r)).join("\n") + "\n";
    for (let attempt = 0; ; attempt++) {
      try {
        return await (await this.request("POST", `/append/${table}?producer=${this.producer}&seq=${seq}`, body, "application/x-ndjson")).json();
      } catch (e) {
        if (/^\d{3}:/.test(e.message) || attempt === retries - 1) throw e; // (refused: retrying won't help)
        await sleep(Math.min(100 * 2 ** attempt, 5000)); // the same seq again: applied once
      }
    }
  }

  /** A view others read by name, as SQL's `CREATE VIEW` and Python's `db.view` make one: `sql`
   * runs over the tables as they are when the view is read. `{ materialized: true }`: kept up to
   * date instead (`CREATE MATERIALIZED VIEW`), filled from the rows already there, then with each
   * batch of new rows (with GROUP BY, kept per key); its other options make it emit what is
   * final: `{ window: "w", size_secs: 60, lateness_secs: 10 }` (and `slide_secs`: sliding),
   * `{ session: "ts", gap_secs: 1800 }`, or `{ join: "streams", time: "ts", within_secs: 600 }`. */
  async view(name, sql, { materialized, replace = true, ...options } = {}) {
    const opts = Object.entries(options);
    if (materialized === undefined && opts.length) { // (up to 0.22, view() made every view a materialized one)
      console.warn("view(…) with window/session/join options: pass { materialized: true } (a view is a stored query unless asked)");
      materialized = true;
    }
    if (!materialized && opts.length) throw new Error(`${opts.map(([k]) => k).join(", ")}: options of a materialized view ({ materialized: true })`);
    const literal = (v) => (typeof v === "number" ? String(v) : `'${String(v).replaceAll("'", "''")}'`);
    const withs = opts.length ? ` WITH (${opts.map(([k, v]) => `${k} = ${literal(v)}`).join(", ")})` : "";
    return this.sql(materialized ? `CREATE MATERIALIZED VIEW ${name}${withs} AS ${sql}` : `CREATE ${replace ? "OR REPLACE " : ""}VIEW ${name} AS ${sql}`);
  }

  /** The current row of one key of a keyed table, or null. */
  async lookup(table, key) {
    const rows = await (await this.request("GET", `/lookup/${table}/${encodeURIComponent(key)}`)).json();
    return rows[0] ?? null;
  }

  /** New rows of a table as they commit; with `after`, a replay from there first. With
   * `changes`, every change: UPDATE's and DELETE's too, each row with its `_change_type`. */
  async *watch(table, { after, changes } = {}) {
    const q = [after === undefined ? "" : `after=${after}`, changes ? "changes=true" : ""].filter(Boolean).join("&");
    const r = await this.request("GET", `/watch/${table}` + (q ? `?${q}` : ""));
    const decoder = new TextDecoder();
    let rest = "";
    for await (const chunk of r.body) {
      const lines = (rest + decoder.decode(chunk, { stream: true })).split("\n");
      rest = lines.pop();
      for (const line of lines) if (line) yield JSON.parse(line);
    }
  }

  /** A query's answer now, and again each time a commit changes a table it reads: each is its
   * rows. An answer that comes out the same isn't sent again; a busy table is queried at most every
   * `everyMs` (100). Leaving the loop ends it on the node.
   *   for await (const rows of db.live("SELECT region, sum(amount) AS total FROM orders GROUP BY region")) redraw(rows); */
  async *live(sql, { params = {}, everyMs } = {}) {
    const r = await this.request("POST", "/live" + (everyMs ? `?every_ms=${everyMs}` : ""), JSON.stringify({ sql, params }), "application/json");
    const decoder = new TextDecoder();
    let rest = "";
    try {
      for await (const chunk of r.body) {
        const lines = (rest + decoder.decode(chunk, { stream: true })).split("\n");
        rest = lines.pop();
        for (const line of lines) {
          if (!line.trim()) continue; // (the node's keep-alive)
          const answer = JSON.parse(line);
          if (answer.error) throw new Error(answer.error);
          this.position = answer.at;
          yield answer.rows;
        }
      }
    } finally {
      await r.body.cancel().catch(() => {});
    }
  }

  /** End this connection's session (its temporary tables and views), and stop the node `local()`
   * started: closing its input stops it (it hands the lake on at once), on every OS; it would stop
   * the same way if this process were killed. */
  async close(timeoutMs = 15_000) {
    await this.request("DELETE", `/sessions/${this.session}`).catch(() => {});
    const node = this.process;
    if (!node || node.exitCode !== null || node.signalCode !== null) return;
    const exited = new Promise((resolve) => node.once("exit", resolve));
    node.stdin.end();
    const late = setTimeout(() => node.kill(), timeoutMs); // (it didn't stop in time)
    await exited;
    clearTimeout(late);
  }
}

export const connect = (url, options) => new Pondra(url, options);

/** Start a node on a lake here — a folder, or s3://bucket/prefix — and connect to it. It stops
 * when this process exits, or with `close()`; the lake stays. Its SQL may read files on this
 * machine (`SELECT * FROM 'jan.csv'`), as DuckDB's may; its Python functions and procedures run
 * with a Python here that has the `pondra` package, if there is one (`python: false`: none). */
export async function local(dir = "lake", { port, token, flags = [], timeoutMs = 120_000, python = true, onNotice } = {}) {
  port ??= await freePort();
  if (!dir.includes("://")) mkdirSync(dir, { recursive: true });
  const args = ["serve", "--dir", dir, "--addr", `127.0.0.1:${port}`, "--stop-with-stdin", ...(python ? ["--python", "auto"] : []), ...flags];
  const owner = randomUUID().replaceAll("-", ""); // (with it, the node lets this process's SQL read files here)
  const node = spawn(binary(), args, { stdio: ["pipe", "ignore", "ignore"], env: { ...process.env, PONDRA_OWNER_KEY: owner } });
  let failed = null;
  node.on("error", (e) => (failed = e)); // (no binary, say)
  const db = new Pondra(`http://127.0.0.1:${port}`, { token, ...(onNotice !== undefined && { onNotice }) });
  db.process = node;
  db.owner = owner;
  process.on("exit", () => node.stdin.end());
  for (const until = Date.now() + timeoutMs; ; await sleep(50)) {
    try {
      await db.request("GET", "/stats");
      return db;
    } catch (e) {
      if (failed || node.exitCode !== null || Date.now() > until) throw new Error(`the node didn't start: ${(failed ?? e).message}`);
    }
  }
}

/** The `pondra` executable: $PONDRA_BIN, or the one npm installed for this platform. */
export function binary() {
  if (process.env.PONDRA_BIN) return process.env.PONDRA_BIN;
  const os = { linux: "linux", darwin: "macos", win32: "windows" }[process.platform];
  const exe = process.platform === "win32" ? "pondra.exe" : "pondra";
  return createRequire(import.meta.url).resolve(`pondra-${os}-${process.arch}/${exe}`);
}

const freePort = () =>
  new Promise((resolve, reject) => {
    const s = createServer().once("error", reject).listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
