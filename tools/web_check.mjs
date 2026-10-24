#!/usr/bin/env node
// The page form's module (web/, designs/l2-b4-web.md) in Node: each request's status, headers and body checked against
// what a node answers (ADR-061's contract: files are files, tables are made by SQL, the routes mean what a node's mean).
// `node tools/web_check.mjs [--pkg web/pkg] [--sizes]`; one JSON line, exit 1 on a failure.
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

const enc = new TextEncoder();
const dec = new TextDecoder();
const ORDERS = 'id,name,qty\n1,a,5\n2,b,7\n3,c,9\n';
const NAMES = 'id,label\n1,x\n3,z\n';
const TEXT = 'text/plain; charset=utf-8';
const JSON_TYPE = 'application/json';

const failed = [];
let passed = 0;
const report = {};

// Judges one answer: its status, its SQLSTATE (null: none), its content type, its quoted etag, its body text, or what
// `json` says of its body (a list of problems, none when it is right). A failure names the check and what came back.
function judge(name, answer, want = {}) {
  const why = [];
  const headers = answer.headers ?? {};
  const text = dec.decode(answer.body);
  if (want.status !== undefined && answer.status !== want.status) why.push(`status ${answer.status}`);
  const sqlstate = headers['x-pondra-sqlstate'] ?? null;
  if (want.sqlstate !== undefined && sqlstate !== want.sqlstate) why.push(`sqlstate ${sqlstate}`);
  if (!headers['content-type']) why.push('no content-type');
  if (want.type !== undefined && headers['content-type'] !== want.type) why.push(`content-type ${headers['content-type']}`);
  if (want.etag && !(headers.etag ?? '').startsWith('"')) why.push(`etag ${headers.etag}`);
  if (want.out !== undefined && text !== want.out) why.push(`body ${JSON.stringify(text)}`);
  if (want.json) {
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      why.push(`body is not JSON ${JSON.stringify(text)}`);
    }
    if (body !== undefined) why.push(...want.json(body));
  }
  if (why.length) {
    failed.push(name);
    console.error(`${name}: ${why.join(', ')}`);
  } else passed++;
  return text;
}

// One request of a check; a throw is a failure of that check, not of the run.
async function ask(name, method, path, headers, body) {
  try {
    return await page.request(method, path, headers, body);
  } catch (e) {
    failed.push(name);
    console.error(`${name}: threw ${e}`);
    return null;
  }
}

// Files are files, and a table is made from one by SQL (the node's contract): PUT answers the node's JSON and an etag,
// and makes no table; CREATE TABLE … AS FROM reads the file once.
const put = await ask('PUT /files/orders.csv', 'PUT', '/files/orders.csv', undefined, ORDERS);
if (put) {
  judge('PUT /files/orders.csv', put, {
    status: 200, sqlstate: null, type: JSON_TYPE, etag: true,
    json: (j) => (j.path === 'files/orders.csv' && j.bytes === enc.encode(ORDERS).length && typeof j.version === 'string' ? [] : [`body ${JSON.stringify(j)}`]),
  });
}
const putNames = await ask('PUT /files/names.csv', 'PUT', '/files/names.csv', undefined, NAMES);
if (putNames) judge('PUT /files/names.csv', putNames, { status: 200, type: JSON_TYPE, etag: true });

// The CTAS's body is what a page answers for a statement with no rows: it is recorded here, not compared, and a
// node's is aligned with it later (4h5).
for (const [name, file] of [['orders', 'orders.csv'], ['names', 'names.csv']]) {
  const made = await ask(`CREATE TABLE ${name}`, 'POST', '/sql', undefined, `CREATE TABLE ${name} AS FROM '${file}'`);
  if (made) report[`ctas_body_${name}`] = judge(`CREATE TABLE ${name} AS FROM '${file}'`, made, { status: 200, sqlstate: null });
}

// [name, method, path, request body, status, sqlstate, body text]; undefined means not compared. The queries run
// over the tables made above.
const CASES = [
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
  ['CREATE EXTERNAL TABLE refused', 'POST', '/sql', "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'orders.csv'", 501, '0A000', undefined],
  ['AT refused', 'POST', '/sql', 'FROM orders AT (VERSION => 1)', 501, '0A000', undefined],
  ['a file by name', 'POST', '/sql?format=csv', "SELECT sum(qty) AS s FROM 'orders.csv'", 200, null, 's\n21\n'],
  ['a file by its files/ path', 'POST', '/sql?format=csv', "SELECT count(*) AS n FROM 'files/orders.csv'", 200, null, 'n\n3\n'],
  ['a glob', 'POST', '/sql?format=csv', "SELECT count(*) AS n FROM 'order*.csv'", 200, null, 'n\n3\n'],
  ['PIVOT', 'POST', '/sql?format=csv', 'PIVOT (SELECT name, qty FROM orders) ON name USING sum(qty)', 200, null, 'a,b,c\n5,7,9\n'],
  ['SUMMARIZE', 'POST', '/sql?format=csv', 'SUMMARIZE orders', 200, null, undefined],
  ['MEASURE without a view', 'POST', '/sql', 'SELECT MEASURE(qty) FROM orders', 404, '42883', undefined],
  ['unknown route', 'GET', '/nowhere', undefined, 404, '0A000', undefined],
];

for (const [name, method, path, send, status, sqlstate, out] of CASES) {
  const answer = await ask(name, method, path, undefined, send);
  if (answer) judge(name, answer, { status, sqlstate, out });
}

// A file a client reads back is the bytes it put, with its etag and its content type by name (GET).
const got = await ask('GET /files/orders.csv', 'GET', '/files/orders.csv');
if (got) judge('GET /files/orders.csv', got, { status: 200, sqlstate: null, type: 'text/csv; charset=utf-8', etag: true, out: ORDERS });
const missing = await ask('GET a missing file', 'GET', '/files/nope.csv');
if (missing) judge('GET a missing file', missing, { status: 404, sqlstate: null, type: TEXT, out: 'no such file: files/nope.csv' });
const versions = await ask('GET ?versions refused', 'GET', '/files/orders.csv?versions');
if (versions) judge('GET ?versions refused', versions, { status: 501, sqlstate: '0A000' });

// A second PUT without If-Match is 409 in the node's words, with no SQLSTATE. With the etag the first PUT gave, quotes
// and all, it replaces the file and the version changes; with that first etag again it is 412.
const again = await ask('PUT again without If-Match', 'PUT', '/files/orders.csv', undefined, 'x\n');
if (again) {
  judge('PUT again without If-Match', again, {
    status: 409, sqlstate: null, type: TEXT,
    out: 'files/orders.csv is there already: send If-Match with the version you read to replace it, or put it under another name',
  });
}
const first = put?.headers.etag;
const replaced = first && (await ask('PUT with the etag', 'PUT', '/files/orders.csv', { 'If-Match': first }, ORDERS));
if (replaced) {
  judge('PUT with the etag it read', replaced, { status: 200, sqlstate: null, type: JSON_TYPE, etag: true });
  if (replaced.headers.etag === first) {
    failed.push('PUT with the etag it read changes the version');
    console.error('PUT with the etag it read changes the version: the etag did not change');
  } else passed++;
  const stale = await ask('PUT with the stale etag', 'PUT', '/files/orders.csv', { 'If-Match': first }, 'x\n');
  if (stale) {
    judge('PUT with the stale etag', stale, {
      status: 412, sqlstate: null, type: TEXT,
      out: 'files/orders.csv: the file changed since it was opened: open it again, or save under another name',
    });
  }
}

// DELETE answers the node's JSON, and the file is gone: FROM 'names.csv' is 42P01, and the table made from it stays.
const removed = await ask('DELETE /files/names.csv', 'DELETE', '/files/names.csv');
if (removed) judge('DELETE /files/names.csv', removed, { status: 200, sqlstate: null, type: JSON_TYPE, out: '{"removed":"files/names.csv"}' });
const gone = await ask('DELETE a missing file', 'DELETE', '/files/names.csv');
if (gone) judge('DELETE a missing file', gone, { status: 404, sqlstate: null, type: TEXT, out: 'no such file: files/names.csv' });
const read_gone = await ask("FROM 'names.csv' after its DELETE", 'POST', '/sql?format=csv', undefined, "FROM 'names.csv'");
if (read_gone) judge("FROM 'names.csv' after its DELETE", read_gone, { status: 404, sqlstate: '42P01' });
const kept = await ask('FROM names after its file is gone', 'POST', '/sql?format=csv', undefined, 'FROM names ORDER BY id');
if (kept) judge('FROM names after its file is gone', kept, { status: 200, sqlstate: null, out: 'id,label\n1,x\n3,z\n' });

// Tables by SQL: CREATE TABLE makes an empty one, which takes no INSERT (a page's SQL has no DML).
const empty = await ask('CREATE TABLE t (a INT)', 'POST', '/sql', undefined, 'CREATE TABLE t (a INT)');
if (empty) judge('CREATE TABLE t (a INT)', empty, { status: 200, sqlstate: null });
const insert = await ask('INSERT INTO t refused', 'POST', '/sql', undefined, 'INSERT INTO t VALUES (1)');
if (insert) judge('INSERT INTO t refused', insert, { status: 501, sqlstate: '0A000' });

// GET /objects lists the tables as a node does, sorted, and says where SQL reads a relative file from.
const objects = await ask('GET /objects', 'GET', '/objects');
if (objects) {
  judge('GET /objects', objects, {
    status: 200, sqlstate: null, type: JSON_TYPE,
    json: (j) => {
      const names = (j.objects ?? []).map((o) => o.name);
      const problems = [];
      if (JSON.stringify(names) !== JSON.stringify(['names', 'orders', 't'])) problems.push(`objects ${JSON.stringify(names)}`);
      if (j.files !== 'mem:///files/') problems.push(`files ${j.files}`);
      return problems;
    },
  });
}

// An Arrow answer PUT back as a file, and counted by SQL as a file.
const arrow = await ask('arrow answer', 'POST', '/sql?format=arrow', undefined, 'SELECT * FROM orders');
if (arrow) judge('arrow answer', arrow, { status: 200, sqlstate: null, type: 'application/vnd.apache.arrow.stream' });
const copied = arrow && (await ask('arrow PUT as a file', 'PUT', '/files/copy.arrow', undefined, arrow.body));
if (copied) judge('arrow PUT as a file', copied, { status: 200, sqlstate: null });
const counted = await ask('count of the copy', 'POST', '/sql?format=csv', undefined, "SELECT count(*) AS n FROM 'copy.arrow'");
if (counted) judge('count of the copy', counted, { status: 200, sqlstate: null, out: 'n\n3\n' });

// Ten million rows generated and summed, timed.
const t1 = performance.now();
const series = await ask('generate_series of 10M rows', 'POST', '/sql?format=csv', undefined,
  'SELECT count(*), sum(x) FROM generate_series(1, 10000000) t(x)');
const series_10m_ms = Number((performance.now() - t1).toFixed(1));
if (series) judge('generate_series of 10M rows', series, { status: 200, sqlstate: null, out: 'count(*),sum(t.x)\n10000000,50000005000000\n' });

const result = { ok: failed.length === 0, checks: passed, failed, open_ms, series_10m_ms, wasm_bytes: wasm.length, ...report };
if (sizes) {
  result.gzip_bytes = gzipSync(wasm, { level: 9 }).length;
  result.brotli_bytes = brotliCompressSync(wasm, {
    params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_SIZE_HINT]: wasm.length },
  }).length;
}
console.log(JSON.stringify(result));
process.exitCode = result.ok ? 0 : 1;
