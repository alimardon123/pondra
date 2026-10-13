// Pondra from JavaScript: SQL, exactly-once appends, key lookups and change feeds, over a node's
// HTTP API. No dependencies (Node 18+, or any runtime with fetch: the client runs in browsers, Deno
// and Bun too; a lake connected here needs Node, Bun or Deno (its node loads the library with
// `process.dlopen`), and its modules load when it is connected).
//
//   import { connect } from "pondra";
//   const db = connect();                           // a lake in memory, in this process; or
//   const db = connect("lake");                     // ./lake (or s3://bucket/prefix), its node in this process; or
//   const db = connect("http://127.0.0.1:8080");    // a node running somewhere
//   await db.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)");
//   await db.append("events", [{ user: "ann", amount: 5 }]);
//   console.log(await db.sql("SELECT user, sum(amount) AS total FROM events WHERE amount > $min GROUP BY user", { min: 1 }));
//   await db.call("load_day", "2026-09-27");             // a stored procedure (what it prints: db.notices)
//   for await (const row of db.watch("events")) { … }   // new rows as they commit
//   const top = db.table("events").filter(col("amount").gt(100)).groupBy("user").agg(col("amount").sum());
//   for (const row of await top) …                       // a frame: a query, run when awaited (frame.js)
import { Frame, literal, quote, scan } from "./frame.js";

export { Expr, Frame, Merge, all, coalesce, col, concat, concatStr, count, first, fn, last, len, lit, max, mean, median, min, nUnique, sqlExpr, sum, when } from "./frame.js";

// A random id: Web Crypto's where it exists (browsers, Deno, Bun, Node 19 and later). Node 18 has it only
// behind a flag, so there it is node:crypto's, loaded once.
const randomUUID = globalThis.crypto?.randomUUID ? () => globalThis.crypto.randomUUID() : (await import("node:crypto")).randomUUID;

// `pondra login` keeps a token per server in `$PONDRA_HOME/login.json` (else `~/.pondra/login.json`). Node, Bun and
// Deno can read it; a browser has no such file, so there these modules are not loaded.
const files = globalThis.process?.versions?.node ? await Promise.all([import("node:fs"), import("node:os"), import("node:path")]) : null;

/** The token `pondra login` kept for the server `url` is on: the longest saved server that `url` equals or starts
 * with and then `/` (as the command line picks it). Undefined when there is no such file, or none for `url`. */
function savedLogin(url) {
  if (!files) return undefined;
  const [fs, os, path] = files;
  try {
    const home = globalThis.process.env.PONDRA_HOME || path.join(os.homedir(), ".pondra");
    const saved = JSON.parse(fs.readFileSync(path.join(home, "login.json"), "utf8"));
    const hits = Object.entries(saved).filter(([server]) => url === server || url.startsWith(`${server}/`));
    return hits.sort(([a], [b]) => b.length - a.length)[0]?.[1];
  } catch {
    return undefined; // (no file, or one that isn't JSON: no token)
  }
}

// A node answered that it ran nothing, or that it couldn't reach the bucket or another node: a
// request that may be sent again goes to the next node. (57P01 admin_shutdown, 57P03
// cannot_connect_now, 58030 io_error, 08xxx connection_exception)
const TRY_AGAIN = new Set(["57P01", "57P03", "58030", "08000", "08001", "08003", "08006"]);
const GONE = "08006"; // (connection_failure: the node holding the session went)
const code = (sql) => sql.replace(/'(?:[^']|'')*'|"(?:[^"]|"")*"|\$(\w*)\$[\s\S]*?\$\1\$|--[^\n]*|\/\*[\s\S]*?\*\//g, " ").trim().replace(/;$/, "");
const isQuery = (sql) => { const c = code(sql); return !c.includes(";") && /^\(*\s*(select|with|values|from|table|show|describe|desc|explain)\b/i.test(c) && !/\bstart\s*\(/i.test(c); };
const isChange = (sql) => { const c = code(sql); return !c.includes(";") && /^\s*(insert|update|delete|merge)\b/i.test(c); };

const varName = (n) => { if (!/^[A-Za-z_]\w*$/.test(n)) throw new Error(`${n}: not a variable's name (letters, digits and _, not first a digit)`); return n; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Basic auth's base64 of a text's UTF-8 bytes: btoa reads one byte a character, and Buffer is Node's.
const base64 = (text) => btoa(String.fromCharCode(...new TextEncoder().encode(text)));

// The PEM texts this process already trusts: a CA file several connections read is added once.
const trusted = new Set();

/** The authority in `file` joins this process's trusted ones, as NODE_EXTRA_CA_CERTS adds it, so the
 * global fetch trusts a node whose certificate it signed. Node 22.19 and 24.5 (and later) have the calls
 * for it; older Nodes are told the two ways round it. */
async function trustCa(file) {
  const { readFileSync } = await import("node:fs");
  const pem = readFileSync(file, "utf8");
  const tls = await import("node:tls");
  if (!tls.setDefaultCACertificates || !tls.getCACertificates) throw new Error("{ ca } needs Node 22.19 or later; or start Node with NODE_EXTRA_CA_CERTS=<file>, or pass { fetch } with your own agent");
  if (trusted.has(pem)) return;
  tls.setDefaultCACertificates([...tls.getCACertificates("default"), pem]);
  trusted.add(pem);
}

/** A file on this machine as text, or null when there's none (or no file system, as in a browser). */
async function localFile(file) {
  const fs = await import("node:fs").catch(() => null);
  return fs?.existsSync(file) ? fs.readFileSync(file, "utf8") : null;
}

/** A routine's code: its text, or the text of its `file`, read here (Node only). */
async function bodyOf(body, file) {
  if (file !== undefined) return (await import("node:fs/promises")).readFile(file, "utf8");
  if (typeof body !== "string") throw new Error("a function or procedure takes body (its code as text) or file (a path to it)");
  return body;
}

// `name TYPE [DEFAULT value], …` from { name: "TYPE" } or { name: ["TYPE", default] } (Python's `_params`).
const paramsSql = (params = {}) => Object.entries(params).map(([name, spec]) => {
  const [type, ...rest] = Array.isArray(spec) ? spec : [spec];
  return `${name} ${type}${rest.length ? ` DEFAULT ${literal(rest[0])}` : ""}`;
}).join(", ");

// `WITH (k = v, …)` for the options that are set (Python's `_with`): not undefined, null, false or "".
// A list is its items, comma-separated.
const withOptions = (options) => {
  const given = Object.entries(options).filter(([, v]) => v !== undefined && v !== null && v !== false && v !== "");
  return given.length ? ` WITH (${given.map(([k, v]) => `${k} = ${literal(Array.isArray(v) ? v.join(", ") : v)}`).join(", ")})` : "";
};

// The body between dollar quotes it doesn't contain (Python's `_dollar`).
const dollar = (body) => {
  const tag = body.includes("$pondra$") ? `$p${randomUUID().slice(0, 8)}$` : "$pondra$";
  return `${tag}${body}${tag}`;
};

// Rows as NDJSON. A Date goes as its UTC time without a zone, the text a TIMESTAMP reads (JSON's own
// form would end in Z, which a TIMESTAMP has no place for).
const ndjson = (rows) => rows.map((r) => JSON.stringify(r, function (k, v) { return this[k] instanceof Date ? this[k].toISOString().replace("T", " ").replace("Z", "") : v; })).join("\n") + "\n";

/** The SQL type of a column from its values: whole numbers BIGINT, fractions and mixed numbers DOUBLE,
 * text VARCHAR, true and false BOOLEAN, dates TIMESTAMP. Nulls say nothing; a column of nothing is text. */
function typeOfColumn(name, values) {
  const types = new Set();
  for (const v of values) {
    if (v === null || v === undefined) continue;
    if (typeof v === "number" && !Number.isFinite(v)) throw new Error(`${name}: ${v} has no JSON form (NaN and the infinities are not sent)`);
    const t = typeof v === "boolean" ? "BOOLEAN" : typeof v === "string" ? "VARCHAR" : v instanceof Date ? "TIMESTAMP"
      : typeof v === "number" ? (Number.isSafeInteger(v) ? "BIGINT" : "DOUBLE") : null;
    if (!t) throw new Error(`${name}: ${typeof v} values have no column type here (write text, numbers, booleans or dates)`);
    types.add(t);
  }
  if (types.size === 2 && types.has("BIGINT") && types.has("DOUBLE")) return "DOUBLE";
  if (types.size > 1) throw new Error(`${name}: ${[...types].join(" and ")} values in one column`);
  return types.size ? [...types][0] : "VARCHAR";
}

/** The columns of some rows: each name any row has, in the order they first come, with its type. */
const columnsOf = (rows) => [...new Set(rows.flatMap((r) => Object.keys(r)))].map((name) => [name, typeOfColumn(name, rows.map((r) => r[name]))]);

export class Pondra {
  /** `url`: a node, or several (an array, or separated by commas): any of a cluster's nodes
   * answers, and a node that goes away or turns work away (stopping, or a leader cut off from its
   * bucket) hands the work to the next, for up to `retrySecs` (a request it may have run goes again
   * only if that can't apply it twice: a query, an `append`, a single INSERT, UPDATE, DELETE or
   * MERGE, sent with a job). While a node holds this connection's session — a transaction,
   * temporary tables, settings, variables — every request goes to it alone.
   * `ca`: a PEM file of the authority that signs an https node's certificate, trusted by this process
   * from the first request (see `trustCa`). `fetch`: a function used in place of the global fetch
   * (Bun's TLS, undici's Agent, Deno's client); it brings its own TLS, so `ca` doesn't apply to it. */
  constructor(url = "http://127.0.0.1:8080", { token, user, password, onNotice = (n) => console.log(n), retrySecs = 60, ca, fetch: fetcher } = {}) {
    this.urls = (Array.isArray(url) ? url : url.split(",")).map((u) => u.trim().replace(/\/$/, "")).filter(Boolean);
    this.url = this.urls[0];
    this.retrySecs = retrySecs;
    this.ca = ca;
    this.trusting = null; // (the CA file read and trusted, once: the first request does it)
    this.fetch = fetcher ?? null; // (a fetch of the caller's; null: the global one)
    this.held = null; // the node holding this connection's session, while one does (`x-pondra-session: held`)
    this.reached = false;
    this.jobs = 0;
    this.token = token;
    if (!token && !user && !password) this.token = this.urls.map((u) => savedLogin(u)).find(Boolean); // (signed in with `pondra login`: its token for this server, if it kept one)
    this.basic = user ? `Basic ${base64(`${user}:${password}`)}` : null; // (a user: its name and password, or its token as the password)
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
    if (this.ready) await this.ready; // (a lake connect() is still opening: its node first)
    if (this.ca) await (this.trusting ??= trustCa(this.ca)); // (the CA file, read and trusted once for this connection)
    const headers = { "x-pondra-session": this.session, ...(this.token ? { authorization: `Bearer ${this.token}` } : this.basic && { authorization: this.basic }), ...(type && { "content-type": type }), ...(this.owner && { "x-pondra-owner": this.owner }) };
    const deadline = Date.now() + this.retrySecs * 1000;
    const send = this.fetch ?? globalThis.fetch; // (called on its own: a browser's fetch refuses another object as `this`)
    for (let tried = 1, wait = 100; ; tried++) {
      const url = this.held ?? this.url;
      let error;
      try {
        const r = await send(url + path, { method, body, headers });
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
      if (Date.now() > deadline || (this.process && (this.process.exitCode !== null || this.process.signalCode !== null))) throw error; // (out of time, or the node `connect()` started beside this process has stopped)
      this.url = this.urls[(this.urls.indexOf(url) + 1) % this.urls.length];
      if (tried % this.urls.length === 0) { // (each node tried once: a moment before the next round)
        await sleep(Math.max(0, Math.min(wait, deadline - Date.now())));
        wait = Math.min(wait * 2, 2000);
      }
    }
  }

  /** A query, as a frame: nothing runs until it is awaited (its rows), or asked for (`item()`,
   * `writeTable()`…). Any other statement (CREATE, INSERT, UPDATE, DELETE, CALL, several at
   * once) runs now, and awaiting it gives the last one's outcome. `params`: values for `$name` in it. */
  sql(query, params) {
    return isQuery(query) ? Frame.fromSql(this, query, params) : this._statement(query, params);
  }

  async _statement(query, params) {
    const changes = isChange(query);
    const path = changes ? `/sql?job=${this.producer}-${++this.jobs}` : "/sql"; // (sent again after a node went, a change is applied once)
    return (await this._post(path, query, params, changes)).json();
  }

  /** A query's answer as the node sends it: JSON rows, or `format` "typed", "arrow" or "table". */
  _send(query, params, format = null) {
    return this._post(format ? `/sql?format=${format}` : "/sql", query, params, isQuery(query));
  }

  /** The text of a statement, or with `params` its JSON `{ sql, params }` (`$name`s bound). */
  _post(path, query, params, again) {
    if (params === undefined || params === null) return this.request("POST", path, query, undefined, again);
    return this.request("POST", path, JSON.stringify({ sql: query, params }), "application/json", again);
  }

  /** A table (or a view) as a frame: `db.table("orders").filter(col("qty").gt(2))`. */
  table(name) {
    return new Frame(this, `SELECT * FROM ${name}`, { rel: name, table: name });
  }

  // Files as frames, read where the node runs each time the frame runs (ADR-026). `source`: a path, a
  // URL (s3://, gs://, az://, https://), a folder, a glob or a list of them. Options are named as
  // Polars' readers name them (`hivePartitioning`, `separator`, `hasHeader`, `snapshotId`) or as SQL's
  // read_* do (`delim`, `header`…).
  readParquet(source, options = {}) { return scan(this, "read_parquet", source, options); }
  readCsv(source, options = {}) { return scan(this, "read_csv", source, options); }
  readJson(source, options = {}) { return scan(this, "read_json", source, options); }
  readNdjson(source, options = {}) { return this.readJson(source, options); }
  readDelta(source, options = {}) { return scan(this, "read_delta", source, options); }
  readIceberg(source, options = {}) { return scan(this, "read_iceberg", source, options); }

  /** Rows into a table: a frame, a query's SQL text, or an array of row objects. `mode`: "create" (a
   * new table: an error if it's there), "append", or "overwrite" (dropped, then made), as Python's
   * `write_table`. Rows make the table's columns from their values (see `typeOfColumn`), then are
   * appended exactly once. */
  async writeTable(name, data, { mode = "create" } = {}) {
    if (!["create", "append", "overwrite"].includes(mode)) throw new Error(`mode ${mode}: create, append or overwrite`);
    if (data instanceof Frame) return data.writeTable(name, { mode });
    if (typeof data === "string") {
      if (!isQuery(data)) throw new Error("writeTable(name, text): the text is a query (SELECT …)");
      return this.sql(data).writeTable(name, { mode });
    }
    if (!Array.isArray(data)) throw new Error("writeTable(name, data): data is a frame, a query's text, or an array of row objects");
    if (mode === "append") return data.length ? this.append(name, data) : null;
    if (!data.length) throw new Error("writeTable: no rows, so no columns to make the table with (write a frame, or some rows)");
    const columns = columnsOf(data);
    if (mode === "overwrite") await this.sql(`DROP TABLE IF EXISTS ${name}`);
    await this.sql(`CREATE TABLE ${name} (${columns.map(([c, t]) => `${quote(c)} ${t}`).join(", ")})`);
    return this.append(name, data);
  }

  /** A file's statements in order, `$name` taking `params.name`: a `.sql` file here (or SQL itself)
   * runs from here; otherwise a file of the lake's (`etl/orders.sql`, `.py`, `.ipynb`: ADR-033) runs
   * on the node, as `CALL run(…)`, logged in `pondra.runs`. */
  async run(file, params = {}) {
    const here = file.endsWith(".sql") ? await localFile(file) : null;
    if (here !== null) return this.sql(here, params); // (a file on this machine: it runs from here)
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

  /** Every object, part and concept whose name, comment or definition has each of the words, best
   * first (rank, is, kind, lake, schema, object, name, about, matched): `await db.find("revenue",
   * { kind: "column" })`. */
  async find(text, { kind } = {}) {
    const quote = (s) => `'${String(s).replace(/'/g, "''")}'`;
    return this.sql(`SELECT * FROM pondra.find(${quote(text)}${kind ? `, kind => ${quote(kind)}` : ""})`);
  }

  /** A stored procedure (`CREATE PROCEDURE`), called, as Python's `con.call`: `await db.call("load_day", "2026-09-27")`. */
  async call(name, ...args) {
    return this.sql(`CALL ${name}(${args.map((_, i) => `$p${i}`).join(", ")})`, Object.fromEntries(args.map((a, i) => [`p${i}`, a])));
  }

  /** A stored procedure started on the node, not waited for (SQL's `pondra.start`): its run's id,
   * whose row of `pondra.runs` says how it went. */
  async start(name, ...args) {
    // Written as Python's call(wait=False) writes it: the name and the arguments as literals, and the
    // column named (`AS run`), so the node reads the call as a start and answers its run's id.
    const given = args.map((a) => `, ${literal(a)}`).join("");
    const [row] = await this.sql(`SELECT pondra.start(${literal(name)}${given}) AS run`);
    return row.run;
  }

  /** A task (`CREATE TASK`): `sql` (a statement, a script, `CALL p(…)`) run on `schedule` ("5 minutes",
   * "cron 0 2 * * * UTC") or `after` other tasks (a name or a list) once they ended well in the same
   * run of their graph, if `when` (a SQL condition) holds; `retries`, `retryDelay`, `timeout` and
   * `onFailure` (a procedure called with the task's name and its error) are its options. */
  async task(name, sql, { schedule, after, when, retries, retryDelay, timeout, onFailure, replace = true } = {}) {
    if ((schedule === undefined) === (after === undefined)) throw new Error("a task takes schedule or after, one of them");
    const how = schedule !== undefined ? `SCHEDULE ${literal(schedule)}` : `AFTER ${[after].flat().join(", ")}`;
    const opts = Object.entries({ retries, retry_delay: retryDelay, timeout, on_failure: onFailure }).filter(([, v]) => v !== undefined && v !== null);
    const options = opts.length ? ` WITH (${opts.map(([k, v]) => `${k} = ${literal(v)}`).join(", ")})` : "";
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
  async wait(id) { await this.sql(`AWAIT ${literal(id)}`); }

  /** `call`'s name up to 0.22. */
  async callProcedure(name, ...args) {
    return this.call(name, ...args);
  }

  /** A stored function from code (or a file of it), as Python's `create_function` writes it. `language`
   * "python" (the default) runs `body` on a node's Python, with `entry` naming the module's function to
   * run; "sql" takes SQL that names its parameters as they are. `params` maps each parameter to its SQL
   * type, or to `[type, default]`; `returns` is a SQL type, or an object of columns and their types (a
   * table). `cache: "10 minutes"`: an answer is reused that long for the same arguments. */
  async createFunction(name, { body, file, params, returns, language = "python", entry, vectorized, strict, volatility, packages, timeout, cache, replace = true } = {}) {
    if (returns === undefined) throw new Error(`${name}: returns is a SQL type ("INT"), or an object of columns and their types (a table)`);
    const code = await bodyOf(body, file);
    const type = typeof returns === "string" ? returns : `TABLE (${Object.entries(returns).map(([c, t]) => `${quote(c)} ${t}`).join(", ")})`;
    const words = [strict ? "STRICT" : "", (volatility ?? "").toUpperCase()].filter(Boolean).join(" ");
    return this.sql(`CREATE ${replace ? "OR REPLACE " : ""}FUNCTION ${name}(${paramsSql(params)}) RETURNS ${type} LANGUAGE ${language} ${words}${withOptions({ entry, vectorized, packages, timeout, cache })} AS ${dollar(code)}`);
  }

  /** A stored procedure from code (or a file of it), as Python's `create_procedure` writes it: `params` as
   * `createFunction`'s. A SQL procedure's body names its parameters with `$` (`$n`), as a `CALL` binds them. */
  async createProcedure(name, { body, file, params, language = "python", entry, packages, timeout, replace = true } = {}) {
    const code = await bodyOf(body, file);
    return this.sql(`CREATE ${replace ? "OR REPLACE " : ""}PROCEDURE ${name}(${paramsSql(params)}) LANGUAGE ${language}${withOptions({ entry, packages, timeout })} AS ${dollar(code)}`);
  }

  /** This lake's functions and procedures (SQL's `pondra.routines`): each one's kind, parameters, language and body. */
  async routines() {
    return (await this.request("GET", "/routines")).json();
  }

  /** Append rows exactly once: sent again after a lost answer, on any node, it is recognised and
   * not applied twice (for up to the connection's `retrySecs`). */
  async append(table, rows) {
    const seq = ++this.seq;
    return (await this.request("POST", `/append/${table}?producer=${this.producer}&seq=${seq}`, ndjson(rows), "application/x-ndjson", true)).json();
  }

  /** A view others read by name, as SQL's `CREATE VIEW` and Python's `db.view` make one: `query` (SQL, or a
   * frame) runs over the tables as they are when the view is read. `{ materialized: true }`: kept up to
   * date instead (`CREATE MATERIALIZED VIEW`), filled from the rows already there, then with each batch of
   * new rows (with GROUP BY, kept per key); its other options make it emit what is final: `{ window: "w",
   * size_secs: 60, lateness_secs: 10 }` (and `slide_secs`: sliding), `{ session: "ts", gap_secs: 1800 }`, or
   * `{ join: "streams", time: "ts", within_secs: 600 }`. `{ temporary: true }`: this connection's only.
   * Resolves to the node's answer for the CREATE (`{ view: name }`), and a refusal rejects here, naming what
   * the node refused (a table the query names that isn't there, say). Read the view with `db.table(name)`. */
  async view(name, query, { materialized, temporary = false, replace, ...options } = {}) {
    const opts = Object.entries(options);
    if (materialized === undefined && opts.length) { // (up to 0.22, view() made every view a materialized one)
      console.warn("view(…) with window/session/join options: pass { materialized: true } (a view is a stored query unless asked)");
      materialized = true;
    }
    if (!materialized && opts.length) throw new Error(`${opts.map(([k]) => k).join(", ")}: options of a materialized view ({ materialized: true })`);
    if (typeof query === "string" && !isQuery(query)) throw new Error("view(name, query): the query is a SELECT (or WITH, VALUES…), not a statement");
    const frame = typeof query === "string" ? this.sql(query) : query;
    return frame.toView(name, { temporary, materialized, replace, ...options });
  }

  /** The current row of one key of a keyed table, or null. */
  async lookup(table, key) {
    const rows = await (await this.request("GET", `/lookup/${table}/${encodeURIComponent(key)}`)).json();
    return rows[0] ?? null;
  }

  /** `await db.transaction(async (tx) => { … })`: the block's statements as one commit (`BEGIN` … `COMMIT`),
   * reading their own writes. If the block throws, it is rolled back and its error goes on to the caller.
   * Another commit that changed one of its rows first refuses it: an error with `sqlstate` "40001"; run the
   * block again. Answers what the block returns. (Python's `with con.transaction():` is the same.) */
  async transaction(fn) {
    await this.sql("BEGIN");
    let answer;
    try {
      answer = await fn(this);
    } catch (e) {
      // (a node that went took the transaction with it, so there is nothing to roll back; and a ROLLBACK
      // that fails doesn't hide the error the block threw)
      if (e?.sqlstate !== GONE) await this.sql("ROLLBACK").catch(() => {});
      throw e;
    }
    await this.sql("COMMIT");
    return answer;
  }

  /** New rows of a table as they commit (a keyed table: every upsert and delete); with `after`, a replay
   * from there first, as far back as the node keeps its change feed. Without `after` (or with null), only
   * the rows committed from now on. With `changes`, every change:
   * UPDATE's and DELETE's too, each row with `_change_type`, `_row_id` and `_version`. After each batch the
   * node sends where it has got to (a line of its own, not yielded): `this.position` is that commit, the
   * place to resume from. `watch(table, { after: db.position })` then yields what came after it. */
  async *watch(table, { after, changes } = {}) {
    const q = ["marks=true", after == null ? "" : `after=${after}`, changes ? "changes=true" : ""].filter(Boolean).join("&");
    const r = await this.request("GET", `/watch/${table}?${q}`);
    const decoder = new TextDecoder();
    let rest = "";
    for await (const chunk of r.body) {
      const lines = (rest + decoder.decode(chunk, { stream: true })).split("\n");
      rest = lines.pop();
      for (const line of lines) {
        if (!line) continue;
        const row = JSON.parse(line);
        if ("_after" in row && Object.keys(row).length === 1) this.position = row._after; // (a mark: where to resume)
        else yield row;
      }
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

  /** The console, in a browser, as this connection. A node `connect()` started answers only its
   *  program, so the link carries its key (`#key=…`); the page keeps it for its tab, and it works
   *  while the node runs. Keep it to yourself, as a token. */
  get consoleUrl() {
    return this.owner ? `${this.url}/#key=${this.owner}` : `${this.url}/`;
  }

  /** End this connection's session (its temporary tables and views), then close the node `connect()` started
   * for it. In this process, once no other connection to it is open: drained, its term given back, and a lake
   * in memory let go. Beside this process, by closing its input (it hands the lake on at once), on every OS; it
   * would stop the same way if this process were killed. It waits up to 15 seconds for that node to stop, then
   * kills it. Takes no argument. */
  async close(...more) {
    if (more.length) throw new Error("close() takes no argument");
    if (this.ready && !(await this.ready.then(() => true, () => false))) return; // (its node never opened: nothing to close)
    const retrySecs = this.retrySecs;
    this.retrySecs = 0; // (a node that's gone isn't waited for here)
    await this.request("DELETE", `/sessions/${this.session}`).catch(() => {});
    this.retrySecs = retrySecs;
    if (this.opened) { // (a node in this process: a second close() finds nothing to let go)
      const opened = this.opened;
      this.opened = null;
      this.retrySecs = 0; // (a closed node refuses at once)
      await opened();
      return;
    }
    const node = this.process;
    if (!node || node.exitCode !== null || node.signalCode !== null) return;
    const exited = new Promise((resolve) => node.once("exit", resolve));
    node.stdin.end();
    const late = setTimeout(() => node.kill(), 15_000); // (it didn't stop in time)
    await exited;
    clearTimeout(late);
  }
}

/** The one way in to Pondra. `target` is:
 * - none, or ":memory:" as DuckDB spells it: a new lake in this process's memory, gone at `close()`;
 * - a folder, or a bucket's URL (s3://, gs://, az://): that lake;
 * - "http://…" or "https://…" (or an array of them: any of a cluster's nodes): a node running somewhere, and
 *   `options` are the connection's (`token`, `user`, `password`, `ca`, `fetch`, `retrySecs`, `onNotice`).
 * A lake's node runs in this process, through the library (Node, Bun, Deno): its SQL may read files on this
 * machine, Python's functions and procedures run with a Python here that has the `pondra` package (`python:
 * false`: none), it listens on 127.0.0.1 at `port` (any free one if none: the console's, and other programs'),
 * and the other options are its settings by name (`memory_gb: 2` is PONDRA_MEMORY_GB). A process
 * holds one such node, and the same lake connected again is another connection to it. Another lake connected
 * while it is open gets a node of its own beside this process, which stops with it; so does every lake where
 * this install has no library. It answers at once: the node opens meanwhile, and the first call waits for it
 * (`await db.ready` waits, and throws why if it didn't open). It is closed with `close()`, or when this process
 * ends (`process.exit()` too). */
export function connect(target, options = {}) {
  if (Array.isArray(target) || /^https?:\/\//.test(target ?? "")) return new Pondra(target, options);
  const { token, python = true, port, onNotice, ...settings } = options;
  const db = new Pondra("http://127.0.0.1", { token, ...(onNotice !== undefined && { onNotice }) });
  db.token = token; // (a node of this process's answers its key, never a token `pondra login` kept)
  const go = () => opening(db, target ?? ":memory:", { python, port }, settings);
  db.ready = turn = turn.then(go, go);
  db.ready.catch(() => {}); // (not unhandled: the first call, or `ready`, says why)
  return db;
}

// The node this process holds (one at a time: ADR-061) and the connections open to it, and the lakes'
// openings and closings one after another, so two connect()s at once agree on who holds what.
let held = null;
let turn = Promise.resolve();

/** Opens `lake` for `db` (connect()'s): another connection to the node held here, a node in this process,
 * or one beside it. */
async function opening(db, lake, { python, port }, settings) {
  if (!lake.includes("://") && lake !== ":memory:") {
    const { mkdirSync } = await import("node:fs");
    const { resolve } = await import("node:path");
    mkdirSync(lake, { recursive: true });
    lake = resolve(lake);
  }
  db.lake = lake; // (where the lake is: a folder, a bucket's URL, or ":memory:")
  if (!(held && lake !== ":memory:" && held.lake === lake)) {
    const addon = held ? null : await nativeModule().catch(() => null); // (no library: a node beside this process)
    // (a worker thread's connect() finds the library holding another thread's node: it says so, by name)
    const node = addon && (await here(addon, lake, { python, port }, settings).catch((e) => (/open in this process already/.test(e.message) ? null : Promise.reject(e))));
    if (!node) return beside(db, lake, { python, port, env: settingsEnv(settings) });
    held = node;
  }
  const node = held;
  node.connections++;
  db.urls = [node.url];
  db.url = node.url;
  db.fetch = node.fetch;
  db.owner = node.owner;
  db.opened = () => { // (`close()`: the last connection to the node closes it)
    if (--node.connections) return Promise.resolve();
    const close = () => {
      if (held === node) held = null;
      return node.close();
    };
    return (turn = turn.then(close, close));
  };
  return db;
}

/** A node on `lake` in this process (its first connect()). Its settings are fixed by the first node a process
 * opens: a later one may repeat them, never change one. */
async function here(addon, lake, { python, port }, settings) {
  port ??= await freePort();
  const owner = randomUUID().replaceAll("-", ""); // (with it, this process's SQL reads files here: invariant 61)
  const launcher = await binary().catch(() => null); // (only an ATTACH of a lake nobody leads needs it)
  // (a setting as text, String(true) being "true"; a list, `attach: ["sales=/data/sales"]`, as it is)
  const given = Object.fromEntries(Object.entries(settings).filter(([, v]) => v != null).map(([k, v]) => [k, Array.isArray(v) ? v.map(String) : String(v)]));
  const options = {
    ...given,
    lake, addr: `127.0.0.1:${port}`, owner, // (these win over a setting of the same name)
    ...(python && { python: "auto" }),
    ...(launcher && { launcher }),
  };
  const handle = await addon.open(JSON.stringify(options));
  // (every call goes to the node inside this process, no socket: its port is the console's, and Python's workers')
  return { lake, url: `http://127.0.0.1:${port}`, owner, fetch: inProcess(addon, handle), close: closer(addon, handle), connections: 0 };
}

/** Settings by name as the variables a node beside this process reads (`memory_gb` is PONDRA_MEMORY_GB). */
const settingsEnv = (settings) =>
  Object.fromEntries(Object.entries(settings).filter(([, v]) => v != null).map(([k, v]) => [`PONDRA_${k.toUpperCase()}`, Array.isArray(v) ? v.join(",") : String(v)]));

/** Starts a node on `dir` — a folder, s3://bucket/prefix, or ":memory:" — in a process of its own, for `db`.
 * It stops when this process exits, or with `close()`; a folder's or a bucket's lake stays. Its SQL may read
 * files on this machine (`SELECT * FROM 'jan.csv'`), as DuckDB's may; its Python functions and procedures run
 * with a Python here that has the `pondra` package, if there is one (`python: false`: none). `timeout`: seconds
 * to wait for the node to start (120). Its standard error goes to `pondra-<port>.log` in the temp folder: a node
 * that doesn't start says where. */
async function beside(db, dir, { port, flags = [], timeout = 120, python = true, env = {} } = {}) {
  const { spawn } = await import("node:child_process");
  const fs = await import("node:fs");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const exe = await binary();
  port ??= await freePort();
  if (!dir.includes("://") && dir !== ":memory:") fs.mkdirSync(dir, { recursive: true });
  const log = join(tmpdir(), `pondra-${port}.log`);
  const errors = fs.openSync(log, "a");
  const args = ["serve", "--dir", dir, "--addr", `127.0.0.1:${port}`, "--stop-with-stdin", ...(python ? ["--python", "auto"] : []), ...flags];
  const owner = randomUUID().replaceAll("-", ""); // (with it, the node lets this process's SQL read files here)
  const node = spawn(exe, args, { stdio: ["pipe", "ignore", errors], env: { ...process.env, ...env, PONDRA_OWNER_KEY: owner } });
  fs.closeSync(errors); // (the node has its own copy of the log's file)
  let failed = null;
  node.on("error", (e) => (failed = e)); // (no binary, say)
  process.on("exit", () => node.stdin.end());
  const url = `http://127.0.0.1:${port}`;
  const probe = new Pondra(url, { retrySecs: 0 }); // (asks with the node's key whether it is up, and ours)
  probe.owner = owner;
  probe.token = undefined; // (asks with the key alone, never a token `pondra login` kept for this address)
  for (const until = Date.now() + timeout * 1000; ; await sleep(50)) {
    let ours;
    try {
      ours = await (await probe.request("GET", "/stats")).json().then((s) => s?.owner, () => undefined); // (a node from before this check doesn't say)
    } catch {
      if (failed || node.exitCode !== null || Date.now() > until) {
        if (node.pid && node.exitCode === null) node.stdin.end(); // (still running: closing its input stops it, as close() does)
        throw new Error(`the node didn't start; its log: ${log}`, failed ? { cause: failed } : undefined); // (a failed spawn, say; else the log says why)
      }
      continue;
    }
    if (ours === false) {
      // Another node answers on this port: ours failed to bind it. Stop ours (it may still be starting) and
      // don't use the other one, whose lake isn't the one asked for. (Not close(): that would reach it.)
      if (node.pid && node.exitCode === null) node.stdin.end(); // (closing its input stops it, as close() does)
      throw new Error(`127.0.0.1:${port} is another node's, not the one this started (it failed to listen there); its log: ${log}`);
    }
    db.urls = [url];
    db.url = url;
    db.process = node;
    db.owner = owner;
    return db;
  }
}

// What goes at 1.0 says so once a process.
const warned = new Set();
function goes(name, instead) {
  if (warned.has(name)) return;
  warned.add(name);
  process.emitWarning(`pondra's ${name}() goes at 1.0: ${instead}`, "DeprecationWarning");
}

/** `connect(dir)` before 0.34: a node on a lake here, in a process of its own. It goes at 1.0. */
export async function local(dir = "lake", { port, token, flags = [], timeout = 120, python = true, onNotice, timeoutMs } = {}) {
  if (timeoutMs !== undefined) throw new Error("local({ timeoutMs }): it is { timeout } now, in seconds, as Python's");
  goes("local", "connect(dir) opens the lake, its node in this process");
  const db = new Pondra("http://127.0.0.1", { token, ...(onNotice !== undefined && { onNotice }) });
  return beside(db, dir, { port, flags, timeout, python });
}

/** `connect(dir)` before 0.34 (awaited). It goes at 1.0. */
export async function open(dir, options = {}) {
  goes("open", "await connect(dir).ready is the same (and connect() alone a lake in memory)");
  return connect(dir ?? ":memory:", options).ready;
}

// Statuses whose answers have no body (a Response refuses one).
const NO_BODY = new Set([204, 205, 304]);

/** fetch, for a node in this process: a request goes straight to the node's router inside this process, with no
 * socket, and its answer's body is read a piece at a time as the node writes it (a live query's answers, a
 * watched table's rows). Read to its end, cancelled, or collected unread, a body is let go of, and the work
 * writing it stops, as when a client goes. */
function inProcess(addon, handle) {
  const unread = new FinalizationRegistry((id) => addon.free(id));
  return async (url, { method = "GET", body = null, headers = {} } = {}) => {
    const { pathname, search } = new URL(url);
    const sent = body == null || typeof body === "string" || body instanceof Uint8Array ? body : new Uint8Array(await new Response(body).arrayBuffer());
    const [status, head, id, first] = await addon.request(handle, method, pathname + search, JSON.stringify(headers), sent);
    const init = { status, headers: JSON.parse(head) };
    if (!id || NO_BODY.has(status)) { // (the body came whole with its head, as most do)
      if (id) addon.free(id);
      return new Response(NO_BODY.has(status) ? null : first, init);
    }
    let start = first;
    let left = id; // (0 once let go of)
    const letGo = () => {
      left = 0;
      unread.unregister(stream);
    };
    const stream = new ReadableStream({
      async pull(c) {
        if (start) {
          c.enqueue(start);
          start = null;
          return;
        }
        try {
          let piece = addon.read(left); // (a piece written already comes at once; else a promise of the next)
          if (piece instanceof Promise) piece = await piece;
          if (piece === null) {
            letGo();
            c.close();
          } else c.enqueue(piece);
        } catch (e) {
          letGo(); // (a failed body is let go of by the library)
          throw e;
        }
      },
      cancel() {
        if (left) addon.free(left);
        letGo();
      },
    });
    unread.register(stream, id, stream);
    return new Response(stream, init);
  };
}

// The nodes opened in this process and nothing closed yet. `process.exit()` ends Node without the cleanup hook the
// library adds (that hook closes them when the process simply ends, or a worker thread does), so the
// process's `exit` event closes them, on this thread: drained, the term given back, a lake in memory let go.
const stillOpen = new Set();
let exitHooked = false;

/** How a node in this process is closed (its last connection's `close()`): until it is, the process's exit closes it. */
function closer(addon, handle) {
  if (!exitHooked) {
    exitHooked = true;
    process.on("exit", () => {
      for (const h of stillOpen) {
        try {
          addon.closeSync(h);
        } catch {
          // (the process is ending: there is no one left to tell)
        }
      }
      stillOpen.clear();
    });
  }
  const close = () => {
    stillOpen.delete(handle);
    return addon.close(handle);
  };
  stillOpen.add(handle);
  return close;
}

/** The `pondra` executable: $PONDRA_BIN, or the one npm installed for this platform. Async, as it loads
 * Node's module resolver only when it is asked. */
export async function binary() {
  if (process.env.PONDRA_BIN) return process.env.PONDRA_BIN;
  const { createRequire } = await import("node:module");
  const os = { linux: "linux", darwin: "macos", win32: "windows" }[process.platform];
  const exe = process.platform === "win32" ? "pondra.exe" : "pondra";
  return createRequire(import.meta.url).resolve(`pondra-${os}-${process.arch}/${exe}`);
}

/** The library's file: $PONDRA_LIBRARY, else the one beside the launcher in this platform's npm package. */
async function library() {
  if (process.env.PONDRA_LIBRARY) return process.env.PONDRA_LIBRARY;
  const { existsSync } = await import("node:fs");
  const { createRequire } = await import("node:module");
  const { dirname, join } = await import("node:path");
  const os = { linux: "linux", darwin: "macos", win32: "windows" }[process.platform];
  const pkg = `pondra-${os}-${process.arch}`;
  const file = { linux: "libpondra_lib.so", darwin: "libpondra_lib.dylib", win32: "pondra_lib.dll" }[process.platform];
  let beside = null;
  try {
    beside = join(dirname(createRequire(import.meta.url).resolve(`${pkg}/package.json`)), file);
  } catch {
    // (the package isn't installed here: the error below says what to do)
  }
  if (beside && existsSync(beside)) return beside;
  throw new Error(`a node in this process needs the library from the ${pkg} package (npm install pondra brings it), or PONDRA_LIBRARY naming it`);
}

// The library's exports, loaded by the first open in this process. While it loads this is a promise, so
// opens made at once share one load; a load that fails is tried again by the next open.
let native = null;

async function nativeModule() {
  native ??= loadNative().catch((e) => {
    native = null;
    throw e;
  });
  return native;
}

/** The library is a Node-API module: `process.dlopen` loads it, and it closes any node still open when this
 * process's environment ends. */
async function loadNative() {
  const module = { exports: {} };
  process.dlopen(module, await library());
  return module.exports;
}

/** A port nothing listens on here, for a node connect() starts. */
async function freePort() {
  const { createServer } = await import("node:net");
  return new Promise((resolve, reject) => {
    const s = createServer().once("error", reject).listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
}
