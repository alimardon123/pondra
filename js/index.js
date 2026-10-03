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

// A node answered that it ran nothing, or that it couldn't reach the bucket or another node: a
// request that may be sent again goes to the next node. (57P01 admin_shutdown, 57P03
// cannot_connect_now, 58030 io_error, 08xxx connection_exception)
const TRY_AGAIN = new Set(["57P01", "57P03", "58030", "08000", "08001", "08003", "08006"]);
const GONE = "08006"; // (connection_failure: the node holding the session went)
const code = (sql) => sql.replace(/'(?:[^']|'')*'|"(?:[^"]|"")*"|\$(\w*)\$[\s\S]*?\$\1\$|--[^\n]*|\/\*[\s\S]*?\*\//g, " ").trim().replace(/;$/, "");
const isQuery = (sql) => { const c = code(sql); return !c.includes(";") && /^\(*\s*(select|with|values|from|table|show|describe|desc|explain)\b/i.test(c) && !/\bstart\s*\(/i.test(c); };
const isChange = (sql) => { const c = code(sql); return !c.includes(";") && /^\s*(insert|update|delete|merge)\b/i.test(c); };

const quoted = (v) => (typeof v === "number" ? String(v) : `'${String(v).replace(/'/g, "''")}'`); // (a SQL literal: a task's schedule and options)
const varName = (n) => { if (!/^[A-Za-z_]\w*$/.test(n)) throw new Error(`${n}: not a variable's name (letters, digits and _, not first a digit)`); return n; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export class Pondra {
  /** `url`: a node, or several (an array, or separated by commas): any of a cluster's nodes
   * answers, and a node that goes away or turns work away (stopping, or a leader cut off from its
   * bucket) hands the work to the next, for up to `retrySecs` (a request it may have run goes again
   * only if that can't apply it twice: a query, an `append`, a single INSERT, UPDATE, DELETE or
   * MERGE, sent with a job). While a node holds this connection's session — a transaction,
   * temporary tables, settings, variables — every request goes to it alone. */
  constructor(url = "http://127.0.0.1:8080", { token, user, password, onNotice = (n) => console.log(n), retrySecs = 60 } = {}) {
    this.urls = (Array.isArray(url) ? url : url.split(",")).map((u) => u.trim().replace(/\/$/, "")).filter(Boolean);
    this.url = this.urls[0];
    this.retrySecs = retrySecs;
    this.held = null; // the node holding this connection's session, while one does (`x-pondra-session: held`)
    this.reached = false;
    this.jobs = 0;
    this.token = token;
    this.basic = user ? `Basic ${Buffer.from(`${user}:${password}`).toString("base64")}` : null; // (a user: its name and password, or its token as the password)
    this.notices = []; // what the last statement's procedures printed
    this.onNotice = onNotice; // (each one, as it comes back; null: keep them quiet)
    this.producer = `js-${randomUUID().slice(0, 12)}`; // exactly-once: one name, increasing seq
    this.seq = 0;
    this.session = randomUUID(); // this connection's temporary tables and views, on the node until close()
  }

  /** One HTTP request (what the methods below are made of). A node that is gone, or turns it away
   * unrun, hands it to the next of `urls`; one that may have run it does so only if `again` (it
   * can't be applied twice: GETs unless said otherwise), until `retrySecs` have passed. */
  async request(method, path, body, type, again = method === "GET") {
    const headers = { "x-pondra-session": this.session, ...(this.token ? { authorization: `Bearer ${this.token}` } : this.basic && { authorization: this.basic }), ...(type && { "content-type": type }), ...(this.owner && { "x-pondra-owner": this.owner }) };
    const deadline = Date.now() + this.retrySecs * 1000;
    for (let tried = 1, wait = 100; ; tried++) {
      const url = this.held ?? this.url;
      let error;
      try {
        const r = await fetch(url + path, { method, body, headers });
        this.reached = true; // (a node has answered: one that is gone now is waited for, not before)
        if (path.startsWith("/sql")) this.held = r.headers.get("x-pondra-session") === "held" ? url : null; // (keep to it while it holds the session)
        const said = r.headers.get("x-pondra-notices");
        this.notices = said ? JSON.parse(said) : [];
        if (this.onNotice) this.notices.forEach((n) => this.onNotice(n));
        if (r.ok) return r;
        const sqlstate = r.headers.get("x-pondra-sqlstate") || "XX000"; // (Postgres's code: 23514 a CHECK, 40001 try again…)
        error = Object.assign(new Error(`${r.status}: ${(await r.text()).slice(0, 500)}`), { sqlstate, status: r.status });
        const unrun = (r.status === 503 && r.headers.get("retry-after")) || sqlstate === "57P03"; // (stopping, or a leader cut off: it ran nothing)
        if (!(unrun || (again && ([502, 504].includes(r.status) || TRY_AGAIN.has(sqlstate))))) throw error;
      } catch (e) {
        if (e === error) throw e;
        const unrun = e.cause?.code === "ECONNREFUSED"; // (nobody there: it ran nothing)
        if (!(unrun || again) || (!this.reached && this.urls.length === 1)) throw e; // (a node never reached: a wrong address, or not started)
        error = e;
      }
      if (this.held) { // (its session's transaction, temporary tables, settings and variables went with it)
        this.held = null;
        throw Object.assign(new Error(`the node holding this connection's session (${url}) is gone, and its transaction, temporary tables, settings and variables with it; what was sent to it last may or may not have been applied (${error.message})`), { sqlstate: GONE, cause: error });
      }
      if (Date.now() > deadline || (this.process && (this.process.exitCode !== null || this.process.signalCode !== null))) throw error; // (out of time, or the node `local()` started has stopped)
      this.url = this.urls[(this.urls.indexOf(url) + 1) % this.urls.length];
      if (tried % this.urls.length === 0) { // (each node tried once: a moment before the next round)
        await sleep(Math.max(0, Math.min(wait, deadline - Date.now())));
        wait = Math.min(wait * 2, 2000);
      }
    }
  }

  /** A query's rows, as objects; for other statements (CREATE, INSERT, UPDATE, DELETE, CALL,
   * several at once), the last one's outcome. `params`: values for `$name` in it. */
  async sql(query, params) {
    const changes = isChange(query);
    const path = changes ? `/sql?job=${this.producer}-${++this.jobs}` : "/sql"; // (sent again after a node went, a change is applied once)
    const again = changes || isQuery(query);
    if (!params) return (await this.request("POST", path, query, undefined, again)).json();
    return (await this.request("POST", path, JSON.stringify({ sql: query, params }), "application/json", again)).json();
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

  /** A task (`CREATE TASK`): `sql` (a statement, a script, `CALL p(…)`) run on `schedule` ("5 minutes",
   * "cron 0 2 * * * UTC") or `after` other tasks (a name or a list) once they ended well in the same
   * run of their graph, if `when` (a SQL condition) holds; `retries`, `retryDelay`, `timeout` and
   * `onFailure` (a procedure called with the task's name and its error) are its options. */
  async task(name, sql, { schedule, after, when, retries, retryDelay, timeout, onFailure, replace = true } = {}) {
    if ((schedule === undefined) === (after === undefined)) throw new Error("a task takes schedule or after, one of them");
    const how = schedule !== undefined ? `SCHEDULE ${quoted(schedule)}` : `AFTER ${[after].flat().join(", ")}`;
    const opts = Object.entries({ retries, retry_delay: retryDelay, timeout, on_failure: onFailure }).filter(([, v]) => v !== undefined && v !== null);
    const options = opts.length ? ` WITH (${opts.map(([k, v]) => `${k} = ${quoted(v)}`).join(", ")})` : "";
    return this.sql(`CREATE ${replace ? "OR REPLACE " : ""}TASK ${name} ${how}${when ? ` WHEN ${when}` : ""}${options} AS ${sql}`);
  }

  /** `EXECUTE TASK name (k => v, …)`: its graph runs now, each task with these values for its `$k`s.
   * The task's run's id: `await db.wait(id)`. */
  async executeTask(name, values = {}) {
    const names = Object.keys(values);
    const given = names.length ? ` (${names.map((n, i) => `"${n.replace(/"/g, '""')}" => $p${i}`).join(", ")})` : "";
    return (await this.sql(`EXECUTE TASK ${name}${given}`, Object.fromEntries(names.map((n, i) => [`p${i}`, values[n]])))).run;
  }

  /** Wait for a run (`start`'s or `executeTask`'s id) to end, as SQL's `AWAIT 'id'`: its error if it failed. */
  async wait(id) { await this.sql(`AWAIT ${quoted(id)}`); }

  /** `call`'s name up to 0.22. */
  async callProcedure(name, ...args) {
    return this.call(name, ...args);
  }

  /** Append rows exactly once: sent again after a lost answer, on any node, it is recognised and
   * not applied twice (for up to the connection's `retrySecs`). */
  async append(table, rows) {
    const seq = ++this.seq;
    const body = rows.map((r) => JSON.stringify(r)).join("\n") + "\n";
    return (await this.request("POST", `/append/${table}?producer=${this.producer}&seq=${seq}`, body, "application/x-ndjson", true)).json();
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
    const r = await this.request("POST", "/live" + (everyMs ? `?every_ms=${everyMs}` : ""), JSON.stringify({ sql, params }), "application/json", true);
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
    const retrySecs = this.retrySecs;
    this.retrySecs = 0; // (a node that's gone isn't waited for here)
    await this.request("DELETE", `/sessions/${this.session}`).catch(() => {});
    this.retrySecs = retrySecs;
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
  const db = new Pondra(`http://127.0.0.1:${port}`, { token, retrySecs: 0, ...(onNotice !== undefined && { onNotice }) });
  db.process = node;
  db.owner = owner;
  process.on("exit", () => node.stdin.end());
  for (const until = Date.now() + timeoutMs; ; await sleep(50)) {
    try {
      await db.request("GET", "/stats");
      db.retrySecs = 60;
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
