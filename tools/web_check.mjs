#!/usr/bin/env node
// The page form's module (web/, designs/l2-b4-web.md) in Node: each request's status, sqlstate and body checked
// against what a node answers. `node tools/web_check.mjs [--pkg web/pkg] [--sizes]`; one JSON line, exit 1 on a failure.
import { readFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { brotliCompressSync, constants, gzipSync } from 'node:zlib';

const args = process.argv.slice(2);
const pkg = args.includes('--pkg')
  ? resolve(args[args.indexOf('--pkg') + 1])
  : resolve(dirname(fileURLToPath(import.meta.url)), '../web/pkg');
const sizes = args.includes('--sizes');

const { default: init, Page } = await import(pathToFileURL(join(pkg, 'pondra_web.js')).href);
const wasm = await readFile(join(pkg, 'pondra_web_bg.wasm'));
await init({ module_or_path: wasm });

const t0 = performance.now();
const page = new Page();
const open_ms = Number((performance.now() - t0).toFixed(1));

// [name, method, path, request body, status, sqlstate, body text]; undefined means not compared.
const CASES = [
  ['PUT /files/orders.csv', 'PUT', '/files/orders.csv', 'id,name,qty\n1,a,5\n2,b,7\n3,c,9\n', 200, null, '{"table":"orders","rows":3}'],
  ['PUT /files/names.csv', 'PUT', '/files/names.csv', 'id,label\n1,x\n3,z\n', 200, null, '{"table":"names","rows":2}'],
  ['filter as CSV', 'POST', '/sql?format=csv', 'FROM orders WHERE qty > 5', 200, null, 'id,name,qty\n2,b,7\n3,c,9\n'],
  ['EXCLUDE as JSON', 'POST', '/sql', 'SELECT * EXCLUDE (name) FROM orders ORDER BY id', 200, null,
    '[{"id":1,"qty":5},{"id":2,"qty":7},{"id":3,"qty":9}]'],
  ['join', 'POST', '/sql?format=csv', 'SELECT o.id, n.label FROM orders o JOIN names n ON o.id = n.id ORDER BY o.id', 200, null,
    'id,label\n1,x\n3,z\n'],
  // A join whose other side groups (EarlyProbes reads that side ahead: a page has no runtime to read on).
  ['join of a grouped subquery', 'POST', '/sql?format=csv',
    'SELECT n.id, g.total FROM names n JOIN (SELECT id, sum(qty) AS total FROM orders GROUP BY id) g ON n.id = g.id ORDER BY n.id',
    200, null, 'id,total\n1,5\n3,9\n'],
  ['window', 'POST', '/sql?format=csv', 'SELECT id, sum(qty) OVER (ORDER BY id) AS run FROM orders', 200, null,
    'id,run\n1,5\n2,12\n3,21\n'],
  ['syntax error', 'POST', '/sql', 'SELEC 1', 400, '42601', undefined],
  ['DDL refused', 'POST', '/sql', 'CREATE TABLE t (a INT)', undefined, '0A000', undefined],
  ['a file by name', 'POST', '/sql?format=csv', "SELECT sum(qty) AS s FROM 'orders.csv'", 200, null, 's\n21\n'],
  ['a glob', 'POST', '/sql?format=csv', "SELECT count(*) AS n FROM 'order*.csv'", 200, null, 'n\n3\n'],
  ['PIVOT', 'POST', '/sql?format=csv', 'PIVOT (SELECT name, qty FROM orders) ON name USING sum(qty)', 200, null, 'a,b,c\n5,7,9\n'],
  ['SUMMARIZE', 'POST', '/sql?format=csv', 'SUMMARIZE orders', 200, null, undefined],
  ['tables', 'GET', '/tables', undefined, 200, null, '["names","orders"]'],
  ['files', 'GET', '/files', undefined, 200, null, '["names.csv","orders.csv"]'],
  ['unknown route', 'GET', '/nowhere', undefined, 404, '0A000', undefined],
];

const dec = new TextDecoder();
const failed = [];
let passed = 0;

// Judges one answer against what a node answers; a failure names the check and says what came back.
function judge(name, answer, want) {
  const why = [];
  if (want.status !== undefined && answer.status !== want.status) why.push(`status ${answer.status}`);
  if (want.sqlstate !== undefined && answer.sqlstate !== want.sqlstate) why.push(`sqlstate ${answer.sqlstate}`);
  if (want.out !== undefined && dec.decode(answer.body) !== want.out) why.push(`body ${JSON.stringify(dec.decode(answer.body))}`);
  if (why.length) {
    failed.push(name);
    console.error(`${name}: ${why.join(', ')}`);
  } else passed++;
}

for (const [name, method, path, send, status, sqlstate, out] of CASES) {
  let answer;
  try {
    answer = await page.request(method, path, send);
  } catch (e) {
    failed.push(name);
    console.error(`${name}: threw ${e}`);
    continue;
  }
  judge(name, answer, { status, sqlstate, out });
}

// An Arrow answer PUT back as a file, and counted by SQL.
const arrow = await page.request('POST', '/sql?format=arrow', 'SELECT * FROM orders');
judge('arrow answer', arrow, { status: 200, sqlstate: null });
const copied = await page.request('PUT', '/files/copy.arrow', arrow.body);
judge('arrow PUT as a file', copied, { status: 200, sqlstate: null });
judge('count of the copy', await page.request('POST', '/sql?format=csv', 'SELECT count(*) AS n FROM copy'),
  { status: 200, sqlstate: null, out: 'n\n3\n' });

// Ten million rows generated and summed, timed.
const t1 = performance.now();
const series = await page.request('POST', '/sql?format=csv', 'SELECT count(*), sum(x) FROM generate_series(1, 10000000) t(x)');
const series_10m_ms = Number((performance.now() - t1).toFixed(1));
judge('generate_series of 10M rows', series, { status: 200, sqlstate: null, out: 'count(*),sum(t.x)\n10000000,50000005000000\n' });

const result = { ok: failed.length === 0, checks: passed, failed, open_ms, series_10m_ms, wasm_bytes: wasm.length };
if (sizes) {
  result.gzip_bytes = gzipSync(wasm, { level: 9 }).length;
  result.brotli_bytes = brotliCompressSync(wasm, {
    params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_SIZE_HINT]: wasm.length },
  }).length;
}
console.log(JSON.stringify(result));
process.exitCode = result.ok ? 0 : 1;
