// Pondra from JavaScript: SQL, exactly-once appends, key lookups and change feeds, over a node's
// HTTP API. No dependencies (Node 18+, or any runtime with fetch).
//
//   import { local, connect } from "pondra";
//   const db = await local("lake");                 // a node on ./lake, here; or
//   const db = connect("http://127.0.0.1:8080");    // one running somewhere
//   await db.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)");
//   await db.append("events", [{ user: "ann", amount: 5 }]);
//   console.log(await db.sql("SELECT user, sum(amount) AS total FROM events GROUP BY user"));
//   for await (const row of db.watch("events")) { … }   // new rows as they commit
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { createRequire } from "node:module";
import { createServer } from "node:net";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export class Pondra {
  constructor(url = "http://127.0.0.1:8080", { token } = {}) {
    this.url = url.replace(/\/$/, "");
    this.token = token;
    this.producer = `js-${randomUUID().slice(0, 12)}`; // exactly-once: one name, increasing seq
    this.seq = 0;
  }

  async call(method, path, body, type) {
    const headers = { ...(this.token && { authorization: `Bearer ${this.token}` }), ...(type && { "content-type": type }), ...(this.owner && { "x-pondra-owner": this.owner }) };
    const r = await fetch(this.url + path, { method, body, headers });
    if (!r.ok) throw new Error(`${r.status}: ${(await r.text()).slice(0, 500)}`);
    return r;
  }

  /** A query's rows, as objects; for CREATE TABLE / INSERT / UPDATE / DELETE, the outcome. */
  async sql(query) {
    return (await this.call("POST", "/sql", query)).json();
  }

  /** Append rows exactly once: a retry after a lost answer is recognised, not applied twice. */
  async append(table, rows, retries = 10) {
    const seq = ++this.seq;
    const body = rows.map((r) => JSON.stringify(r)).join("\n") + "\n";
    for (let attempt = 0; ; attempt++) {
      try {
        return await (await this.call("POST", `/append/${table}?producer=${this.producer}&seq=${seq}`, body, "application/x-ndjson")).json();
      } catch (e) {
        if (/^\d{3}:/.test(e.message) || attempt === retries - 1) throw e; // (refused: retrying won't help)
        await sleep(Math.min(100 * 2 ** attempt, 5000)); // the same seq again: applied once
      }
    }
  }

  /** A view: `sql` over each new batch of rows, committed with them (with GROUP BY, kept per
   * key). Options make it emit what is final: `{ window: "w", size_secs: 60, lateness_secs: 10 }`,
   * or `{ session: "ts", gap_secs: 1800 }`. Asking again for the same view changes nothing. */
  async view(name, sql, options = {}) {
    const query = new URLSearchParams(options).toString();
    return (await this.call("POST", `/views/${name}` + (query ? `?${query}` : ""), sql)).json();
  }

  /** The current row of one key of a keyed table, or null. */
  async lookup(table, key) {
    const rows = await (await this.call("GET", `/lookup/${table}/${encodeURIComponent(key)}`)).json();
    return rows[0] ?? null;
  }

  /** New rows of a table as they commit; with `after`, a replay from there first. With
   * `changes`, every change: UPDATE's and DELETE's too, each row with its `_change_type`. */
  async *watch(table, { after, changes } = {}) {
    const q = [after === undefined ? "" : `after=${after}`, changes ? "changes=true" : ""].filter(Boolean).join("&");
    const r = await this.call("GET", `/watch/${table}` + (q ? `?${q}` : ""));
    const decoder = new TextDecoder();
    let rest = "";
    for await (const chunk of r.body) {
      const lines = (rest + decoder.decode(chunk, { stream: true })).split("\n");
      rest = lines.pop();
      for (const line of lines) if (line) yield JSON.parse(line);
    }
  }

  /** Stop the node `local()` started: closing its input stops it (it hands the lake on at once),
   * on every OS; it would stop the same way if this process were killed. */
  async close(timeoutMs = 15_000) {
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
 * machine (`SELECT * FROM 'jan.csv'`), as DuckDB's may. */
export async function local(dir = "lake", { port, token, flags = [], timeoutMs = 120_000 } = {}) {
  port ??= await freePort();
  if (!dir.includes("://")) mkdirSync(dir, { recursive: true });
  const args = ["serve", "--dir", dir, "--addr", `127.0.0.1:${port}`, "--stop-with-stdin", ...flags];
  const owner = randomUUID().replaceAll("-", ""); // (with it, the node lets this process's SQL read files here)
  const node = spawn(binary(), args, { stdio: ["pipe", "ignore", "ignore"], env: { ...process.env, PONDRA_OWNER_KEY: owner } });
  let failed = null;
  node.on("error", (e) => (failed = e)); // (no binary, say)
  const db = new Pondra(`http://127.0.0.1:${port}`, { token });
  db.process = node;
  db.owner = owner;
  process.on("exit", () => node.stdin.end());
  for (const until = Date.now() + timeoutMs; ; await sleep(50)) {
    try {
      await db.call("GET", "/stats");
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
