#!/usr/bin/env node
// TPC-H's 22 queries on the page form (web/, tools/web_check.mjs) and on DuckDB-WASM, in Node: answers compared as
// singlenode.py compares them, load, time and memory. Each engine runs in child processes of its own (this file,
// `--engine`), so a query that kills or hangs one is recorded and the rest go on.
//   node tools/web_bench.mjs --data DIR --duckdb NODE_MODULES [--arrow NODE_MODULES] [--pkg web/pkg]
//        [--queries tools/bench/tpch-queries] [--runs 3] [--timeout 600] [--out FILE]
// A page answers strings as views, as a node does, and apache-arrow older than 21 cannot read views. So --arrow names
// the apache-arrow that reads both engines' answers (default: --duckdb's, which is 17 and stops the run with that
// message); it must be 21 or newer.
// One table on stdout; exit 1 if an answer differs, a query failed on either engine, or an engine did not load.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { createInterface } from 'node:readline';
import { readFile, readdir, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = fileURLToPath(import.meta.url);
const NAMES = Array.from({ length: 22 }, (_, i) => `q${i + 1}`);
const dec = new TextDecoder();
// The packages of a node_modules folder, loaded as from a file beside it.
const loaderOf = (folder) => createRequire(join(folder, 'x.js'));

// ---- the command line -------------------------------------------------------------------------------------------

function options(argv) {
  const get = (name, dflt) => {
    const i = argv.indexOf(`--${name}`);
    return i < 0 ? dflt : argv[i + 1];
  };
  const root = resolve(dirname(HERE), '..');
  const duckdb = get('duckdb') && resolve(get('duckdb'));
  return {
    engine: get('engine'), // (a child's own: `pondra` or `duckdb`)
    only: (get('only', '') || '').split(',').filter(Boolean), // (a child's own: the queries it runs)
    data: get('data') && resolve(get('data')),
    duckdb,
    arrow: (get('arrow') && resolve(get('arrow'))) || duckdb, // (apache-arrow 21 or newer: reads a page's views)
    pkg: resolve(get('pkg', join(root, 'web/pkg'))),
    queries: resolve(get('queries', join(root, 'tools/bench/tpch-queries'))),
    runs: Number(get('runs', 3)),
    timeout: Number(get('timeout', 600)),
    out: get('out'),
  };
}

// ---- shared: query text, answers as rows, comparison ------------------------------------------------------------

// A query as tools/bench/tpch.py's queries() reads it: no trailing `;`, and q15's view as a CTE (the page refuses
// CREATE VIEW, and DuckDB answers only a batch's last statement).
async function queryText(folder, q) {
  let sql = (await readFile(join(folder, `${q}.sql`), 'utf8')).trim().replace(/;+$/, '');
  if (q === 'q15') {
    const m = sql.match(/^create view revenue0 \(supplier_no, total_revenue\) as\s*select\s*l_suppkey,\s*(sum\(.*?\))\s*from(.*?);\s*(select.*)/is);
    if (!m) throw new Error('q15 is not the view tpch.py rewrites');
    sql = `with revenue0 as (select l_suppkey as supplier_no, ${m[1]} as total_revenue from${m[2]}) ${m[3]}`.split(/;\s*drop view/i)[0];
  }
  return sql;
}

// An Arrow table's rows as plain values, read by column index. A decimal is 128 bits in four words (little endian,
// unscaled): it is read as a number, never through Number() of the raw value, which throws past 2^53.
function tableRows(table, T) {
  const cols = table.schema.fields.map((f, c) => [table.getChildAt(c), f.type]);
  const rows = [];
  for (let r = 0; r < table.numRows; r++) {
    rows.push(cols.map(([col, type]) => cell(col.get(r), type, T)));
  }
  return rows;
}

function cell(v, type, T) {
  if (v === null || v === undefined) return null;
  if (type.typeId === T.Decimal) return decimal(v, type.scale);
  if (type.typeId === T.Date || type.typeId === T.Timestamp) return new Date(Number(v)).toISOString().slice(0, 10);
  if (typeof v === 'bigint') return Number(v);
  return v;
}

function decimal(v, scale) {
  let x = 0n;
  for (let i = v.length - 1; i >= 0; i--) x = (x << 32n) | BigInt(v[i]);
  if (v[v.length - 1] & 0x80000000) x -= 1n << BigInt(32 * v.length); // (two's complement)
  const d = 10n ** BigInt(scale);
  return Number(x / d) + Number(x % d) / Number(d);
}

// singlenode.py's comparison: rows sorted by their values (numbers rounded to 2 places), then zipped.
const keyOf = (row) => JSON.stringify(row.map((v) => (typeof v === 'number' ? Math.round(v * 100) / 100 : v)));
const sortedRows = (rows) => rows.map((r) => [keyOf(r), r]).sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)).map(([, r]) => r);

function near(u, v) {
  if (typeof u === 'number' && typeof v === 'number') return Math.abs(u - v) <= 1e-6 * Math.max(1, Math.abs(u), Math.abs(v)) + 0.01;
  return String(u).trim() === String(v).trim();
}

const sameRow = (r, s) => r.length === s.length && r.every((u, c) => near(u, s[c]));

// Two answers ({cols, rows}) are the same when their widths and row counts match and every sorted row does.
function same(a, b) {
  if (a.cols !== b.cols || a.rows.length !== b.rows.length) return false;
  const y = sortedRows(b.rows);
  return sortedRows(a.rows).every((r, i) => sameRow(r, y[i]));
}

// The first row the two sorted answers differ in, for a `no` (a type, an order, a rounding or a wrong answer).
function firstDiff(a, b) {
  if (a.cols !== b.cols) return `widths ${a.cols} and ${b.cols}`;
  if (a.rows.length !== b.rows.length) return `${a.rows.length} rows and ${b.rows.length}`;
  const x = sortedRows(a.rows), y = sortedRows(b.rows);
  const i = x.findIndex((r, k) => !sameRow(r, y[k]));
  return i < 0 ? 'none' : `sorted row ${i}: pondra ${JSON.stringify(x[i])} duckdb ${JSON.stringify(y[i])}`;
}

// ---- a child: one engine, loaded, then its queries --------------------------------------------------------------

const emit = (line) => process.stdout.write(`${JSON.stringify(line)}\n`);
const maxrss = () => Math.round((process.resourceUsage().maxRSS / 1024) * 10) / 10; // (KiB to MiB)
const round1 = (x) => Math.round(x * 10) / 10;

async function readTables(dir) {
  const files = (await readdir(dir)).filter((f) => f.endsWith('.parquet')).sort();
  if (!files.length) throw new Error(`no .parquet files in ${dir}`);
  const tables = [];
  for (const f of files) tables.push({ name: f.slice(0, -'.parquet'.length), bytes: await readFile(join(dir, f)) });
  return tables;
}

// The page: `PUT /files/<table>.parquet` per table. Its counts are the rows each PUT answered.
async function openPage(o, arrow) {
  const { default: init, Page } = await import(pathToFileURL(join(o.pkg, 'pondra_web.js')).href);
  await init({ module_or_path: await readFile(join(o.pkg, 'pondra_web_bg.wasm')) });
  const page = new Page();
  const putRows = {};
  let version = 'unknown';
  try {
    version = (await readFile(join(o.pkg, '..', 'Cargo.toml'), 'utf8')).match(/^version = "(.*)"/m)[1];
  } catch {
    // (no Cargo.toml beside the module: the version stays unknown)
  }
  return {
    version,
    async load(tables) {
      for (const t of tables) {
        const a = await page.request('PUT', `/files/${t.name}.parquet`, t.bytes);
        if (a.status !== 200) throw new Error(`PUT ${t.name}: ${a.status} ${dec.decode(a.body)}`);
        const j = JSON.parse(dec.decode(a.body));
        if (j.table !== t.name) throw new Error(`PUT ${t.name} answered for ${j.table}`);
        putRows[t.name] = j.rows;
      }
    },
    counts: async (names) => Object.fromEntries(names.map((n) => [n, putRows[n]])),
    async run(sql) {
      const a = await page.request('POST', '/sql?format=arrow', sql);
      if (a.status !== 200) throw new Error(`${a.sqlstate}: ${dec.decode(a.body).split('\n')[0]}`);
      return arrow.tableFromIPC(a.body);
    },
  };
}

// DuckDB-WASM's blocking Node build, opened the way its own package opens it. Each table's bytes are registered
// under its file name and a table made from them; the counts are asked after the load's clock stops.
async function openDuck(o, req) {
  const duckdb = req('@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs');
  const dist = join(o.duckdb, '@duckdb/duckdb-wasm/dist');
  const db = await duckdb.createDuckDB(
    { mvp: { mainModule: join(dist, 'duckdb-mvp.wasm'), mainWorker: join(dist, 'duckdb-node-mvp.worker.cjs') },
      eh: { mainModule: join(dist, 'duckdb-eh.wasm'), mainWorker: join(dist, 'duckdb-node-eh.worker.cjs') } },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  );
  await db.instantiate(() => {});
  const conn = db.connect();
  return {
    version: String(await db.getVersion()),
    async load(tables) {
      for (const t of tables) {
        db.registerFileBuffer(`${t.name}.parquet`, t.bytes);
        conn.query(`CREATE TABLE ${t.name} AS SELECT * FROM '${t.name}.parquet'`);
      }
    },
    async counts(names) {
      return Object.fromEntries(names.map((n) => [n, Number(conn.query(`SELECT count(*) AS n FROM ${n}`).getChildAt(0).get(0))]));
    },
    run: async (sql) => conn.query(sql),
  };
}

async function child(o) {
  if (typeof globalThis.gc !== 'function') throw new Error('run with --expose-gc');
  // One apache-arrow for both (--arrow): DuckDB's tables and the page's answers are read by the same code.
  const req = loaderOf(o.duckdb);
  const arrow = loaderOf(o.arrow)('apache-arrow');
  let tables = await readTables(o.data); // (read before any clock starts)
  const names = tables.map((t) => t.name);
  const t0 = performance.now();
  const engine = o.engine === 'pondra' ? await openPage(o, arrow) : await openDuck(o, req);
  const open_ms = round1(performance.now() - t0);
  const t1 = performance.now();
  await engine.load(tables);
  const load_ms = round1(performance.now() - t1);
  const rows = await engine.counts(names);
  tables = null; // (the bytes are the engine's now; each keeps its own copy)
  globalThis.gc();
  const rss_after_load_mb = round1(process.memoryUsage().rss / 1048576);
  emit({ open_ms, load_ms, rss_after_load_mb, version: engine.version, rows, maxrss_mb: maxrss() });

  for (const q of o.only) {
    try {
      const sql = await queryText(o.queries, q);
      let best = Infinity;
      let answer;
      for (let r = 0; r < o.runs; r++) {
        const s = performance.now();
        answer = await engine.run(sql); // (the answer as an Arrow table: both engines' timed step)
        best = Math.min(best, performance.now() - s);
      }
      emit({ q, ms: round1(best), cols: answer.numCols, rows: tableRows(answer, arrow.Type), maxrss_mb: maxrss() });
    } catch (e) {
      emit({ q, error: String(e.message ?? e).split('\n')[0].slice(0, 300), maxrss_mb: maxrss() });
    }
  }
  process.exit(0); // (a blocking DuckDB build may keep its worker; the answers are out)
}

// ---- the parent: one child after another for each engine, then the table ----------------------------------------

// Runs one child on the queries left. Resolves with what it printed and how it ended.
function runChild(o, engine, only, progress) {
  return new Promise((resolve) => {
    const args = ['--expose-gc', HERE, '--engine', engine, '--data', o.data, '--duckdb', o.duckdb, '--arrow', o.arrow,
      '--pkg', o.pkg, '--queries', o.queries, '--runs', String(o.runs), '--only', only.join(',')];
    const proc = spawn(process.execPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    const seen = { load: null, queries: [], stderr: [], timedOut: false, code: null, signal: null };
    let last = Date.now();
    createInterface({ input: proc.stdout }).on('line', (line) => {
      last = Date.now();
      let j;
      try {
        j = JSON.parse(line);
      } catch {
        return; // (anything that isn't an answer line is ignored)
      }
      if (j.open_ms !== undefined) seen.load = j;
      else seen.queries.push(j);
      progress(j);
    });
    // (the lines that say why: stack frames and blank lines left out)
    createInterface({ input: proc.stderr }).on('line', (line) => {
      if (line.trim() && !line.startsWith('    at ') && !line.startsWith('Node.js')) seen.stderr = [...seen.stderr.slice(-5), line];
    });
    // A query silent for --timeout seconds is killed; the parent can do it while the child's engine blocks.
    const watch = setInterval(() => {
      if (Date.now() - last > o.timeout * 1000) {
        seen.timedOut = true;
        proc.kill('SIGKILL');
      }
    }, 1000);
    proc.on('close', (code, signal) => {
      clearInterval(watch);
      seen.code = code;
      seen.signal = signal;
      resolve(seen);
    });
  });
}

function why(c, o) {
  const how = c.timedOut ? `silent for ${o.timeout} s, killed` : c.signal ? `killed by ${c.signal}` : `exit ${c.code}`;
  const tail = c.stderr.slice(-3).join(' | ').slice(-300);
  return tail ? `${how}: ${tail}` : how;
}

// One engine: children until every query has an answer or failed. A child that dies on a query fails that query,
// with the last lines of its stderr, and the next child takes the rest (run_engine's rule). A child that dies before
// its load line fails the engine at load.
async function runEngine(o, engine, progress) {
  const out = { error: null, load: null, queries: {}, peak: 0 };
  let left = [...NAMES];
  while (left.length) {
    const c = await runChild(o, engine, left, progress);
    if (!c.load) {
      out.error = `load: ${why(c, o)}`;
      break;
    }
    out.load ??= c.load;
    out.peak = Math.max(out.peak, c.load.maxrss_mb, ...c.queries.map((j) => j.maxrss_mb));
    for (const j of c.queries) {
      out.queries[j.q] = j;
      left = left.filter((q) => q !== j.q);
    }
    if (left.length) {
      out.queries[left[0]] = { q: left[0], error: why(c, o), maxrss_mb: out.peak };
      left = left.slice(1);
    }
  }
  return out;
}

const fmt = (x) => (x === undefined ? 'error' : x.toFixed(1));

function report(o, engines) {
  const [P, D] = [engines.pondra, engines.duckdb];
  const pick = (e, q) => e.queries[q] ?? { error: e.error ? `not run (${e.error})` : 'not run' };
  const lines = [];
  // (an engine that did not load is a failure of its own: its rows are not compared)
  const rowsEqual = !P.load || !D.load || JSON.stringify(P.load.rows) === JSON.stringify(D.load.rows);
  let total = { p: 0, d: 0, same: 0 };
  const failures = [];
  lines.push(`pondra ${P.load?.version ?? '?'}   duckdb ${D.load?.version ?? '?'}   data ${o.data}   runs ${o.runs}`);
  lines.push(`${'query'.padEnd(7)}${'pondra_ms'.padEnd(12)}${'duckdb_ms'.padEnd(12)}${'same'.padEnd(6)}peak_mb (pondra / duckdb)`);
  const results = {};
  for (const q of NAMES) {
    const p = pick(P, q), d = pick(D, q);
    const ok = p.error === undefined && d.error === undefined && same(p, d);
    if (p.ms !== undefined) total.p += p.ms;
    if (d.ms !== undefined) total.d += d.ms;
    if (ok) total.same++;
    if (p.error) failures.push(`${q} pondra: ${p.error}`);
    if (d.error) failures.push(`${q} duckdb: ${d.error}`);
    if (!ok && !p.error && !d.error) failures.push(`${q} differs: ${firstDiff(p, d)}`);
    const mb = (x) => (x.maxrss_mb === undefined ? '-' : x.maxrss_mb.toFixed(0));
    lines.push(`${q.padEnd(7)}${fmt(p.ms).padEnd(12)}${fmt(d.ms).padEnd(12)}${(ok ? 'yes' : 'no').padEnd(6)}${mb(p)} / ${mb(d)}`);
    results[q] = { pondra: p, duckdb: d, same: ok };
  }
  lines.push(`${'total'.padEnd(7)}${total.p.toFixed(1).padEnd(12)}${total.d.toFixed(1).padEnd(12)}${`${total.same} of 22`}`);
  lines.push(`${'open'.padEnd(7)}${fmt(P.load?.open_ms).padEnd(12)}${fmt(D.load?.open_ms)}`);
  lines.push(`${'load'.padEnd(7)}${fmt(P.load?.load_ms).padEnd(12)}${fmt(D.load?.load_ms)}`);
  const mem = (e) => (e.load ? `${e.load.rss_after_load_mb.toFixed(0)} / ${e.peak.toFixed(0)}` : 'error');
  lines.push(`${'memory'.padEnd(7)}${mem(P).padEnd(12)}${mem(D).padEnd(12)}(MB, after load / peak)`);
  if (!rowsEqual) failures.push(`table rows differ: pondra ${JSON.stringify(P.load?.rows)} duckdb ${JSON.stringify(D.load?.rows)}`);
  if (P.error) failures.push(`pondra ${P.error}`);
  if (D.error) failures.push(`duckdb ${D.error}`);
  lines.push('both engines single-threaded (the page: one partition, no wasm threads; DuckDB-WASM\'s mvp/eh bundles: no pthreads)');
  console.log(lines.join('\n'));
  for (const f of failures) console.error(f);
  return { ok: failures.length === 0 && total.same === 22, results, total, rowsEqual, failures };
}

// A page's answers are views, which apache-arrow older than 21 cannot read: say so before any engine runs, not after
// 14 failed queries. The package's own main file sits at its root, so its package.json is beside it.
async function arrowVersion(o) {
  const main = loaderOf(o.arrow).resolve('apache-arrow');
  const version = JSON.parse(await readFile(join(dirname(main), 'package.json'), 'utf8')).version;
  if (Number(version.split('.')[0]) < 21) {
    throw new Error(`apache-arrow ${version} in ${o.arrow} cannot read a page's answers (strings are views, as on a node): `
      + 'pass --arrow NODE_MODULES with apache-arrow 21 or newer');
  }
  return version;
}

async function parent(o) {
  if (!o.data || !o.duckdb) throw new Error('--data DIR and --duckdb NODE_MODULES are needed');
  await readdir(o.data).catch(() => { throw new Error(`no folder ${o.data}`); });
  let arrowVer;
  try {
    arrowVer = await arrowVersion(o);
  } catch (e) {
    console.error(e.message);
    process.exitCode = 1;
    return;
  }
  const progress = (j) => console.error(j.open_ms !== undefined ? `  loaded in ${j.load_ms} ms`
    : j.error ? `  ${j.q} FAILED: ${j.error.slice(0, 160)}` : `  ${j.q} ${j.ms} ms`);
  const engines = {};
  for (const engine of ['pondra', 'duckdb']) {
    console.error(`${engine}:`);
    engines[engine] = await runEngine(o, engine, progress);
  }
  const r = report(o, engines);
  if (o.out) {
    const result = { versions: { pondra: engines.pondra.load?.version, duckdb: engines.duckdb.load?.version, arrow: arrowVer },
      data: o.data,
      runs: o.runs, timeout: o.timeout, open: { pondra: engines.pondra.load?.open_ms, duckdb: engines.duckdb.load?.open_ms },
      load: { pondra: engines.pondra.load?.load_ms, duckdb: engines.duckdb.load?.load_ms }, rows_loaded: engines.pondra.load?.rows,
      rows_loaded_equal: r.rowsEqual, memory: { pondra: engines.pondra.load && { after_load_mb: engines.pondra.load.rss_after_load_mb, peak_mb: engines.pondra.peak },
        duckdb: engines.duckdb.load && { after_load_mb: engines.duckdb.load.rss_after_load_mb, peak_mb: engines.duckdb.peak } },
      total: r.total, queries: r.results, failures: r.failures };
    await writeFile(o.out, `${JSON.stringify(result, null, 1)}\n`);
  }
  process.exitCode = r.ok ? 0 : 1;
}

const o = options(process.argv.slice(2));
if (o.engine) await child(o);
else await parent(o);
