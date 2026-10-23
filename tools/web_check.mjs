#!/usr/bin/env node
// The page form's module (web/, designs/l2-b4-web.md) in Node: each request's status, headers and body checked against
// what a node answers (ADR-061's contract: files are files, tables are made by SQL, the routes mean what a node's mean).
// `node tools/web_check.mjs [--pkg web/pkg] [--sizes] [--lake web/lake]`; one JSON line, exit 1 on a failure. The lake's
// checks run when its folder has expected.json (made by tools/web_lake.py): a Delta table a node published, read over a
// web server that lists nothing, each read compared with the node's own answer for the same bytes.
import { createHash } from 'node:crypto';
import { readFile, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { basename, dirname, extname, join, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { brotliCompressSync, constants, gzipSync } from 'node:zlib';

const args = process.argv.slice(2);
const pkg = args.includes('--pkg')
  ? resolve(args[args.indexOf('--pkg') + 1])
  : resolve(dirname(fileURLToPath(import.meta.url)), '../web/pkg');
const lake = args.includes('--lake')
  ? resolve(args[args.indexOf('--lake') + 1])
  : resolve(dirname(fileURLToPath(import.meta.url)), '../web/lake');
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
// `json` says of its body (a list of problems, none when it is right); `error` wants any status of 400 or more, and `says`
// a text the body holds. A failure names the check and what came back.
function judge(name, answer, want = {}) {
  const why = [];
  const headers = answer.headers ?? {};
  const text = dec.decode(answer.body);
  if (want.status !== undefined && answer.status !== want.status) why.push(`status ${answer.status}`);
  if (want.error && !(answer.status >= 400)) why.push(`status ${answer.status}, not an error`);
  if (want.says !== undefined && !text.includes(want.says)) why.push(`body ${JSON.stringify(text)}`);
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

// A web server over a folder, as a bucket's public address is: files by their bytes (GET, HEAD, a Range), and nothing
// listed (a folder, a path ending in `/`, a name not there: 404). It counts the requests it answers.
async function serveFolder(root) {
  const started = new Date().toUTCString();
  let served = 0;
  const server = createServer(async (req, res) => {
    served++;
    try {
      await answerFile(root, started, req, res);
    } catch (e) {
      reply(req, res, { status: 500, body: enc.encode(String(e)), modified: started });
    }
  });
  await new Promise((listening) => server.listen(0, '127.0.0.1', listening));
  return { server, port: server.address().port, served: () => served };
}

// One request: a file's bytes, or the Range of them; anything that is not a file is 404 (405 for a method but GET and HEAD).
async function answerFile(root, started, req, res) {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    return reply(req, res, { status: 405, body: enc.encode('only GET and HEAD'), modified: started, headers: { Allow: 'GET, HEAD' } });
  }
  const path = decodeURIComponent(new URL(req.url, 'http://127.0.0.1').pathname);
  const file = resolve(root, `.${path}`);
  const info = path.endsWith('/') || !file.startsWith(root + sep) ? null : await stat(file).catch(() => null);
  if (!info?.isFile()) return reply(req, res, { status: 404, body: enc.encode('not found'), modified: started });
  const body = await readFile(file);
  const etag = `"${createHash('sha256').update(body).digest('hex').slice(0, 32)}"`; // (the whole file's: a Range keeps it)
  const modified = info.mtime.toUTCString();
  const type = typeOf(basename(file));
  const range = rangeOf(req.headers.range, body.length);
  if (range === 'unsatisfiable') {
    return reply(req, res, { status: 416, body: new Uint8Array(0), etag, modified, headers: { 'Content-Range': `bytes */${body.length}` } });
  }
  if (!range) return reply(req, res, { status: 200, type, body, etag, modified });
  return reply(req, res, {
    status: 206, type, body: body.subarray(range.start, range.end + 1), etag, modified,
    headers: { 'Content-Range': `bytes ${range.start}-${range.end}/${body.length}` },
  });
}

// Writes one answer with the headers every answer carries: its length, its etag (a file's, else its body's) and when it
// last changed. A HEAD gets those headers and no body.
function reply(req, res, { status, type = TEXT, body, etag, modified, headers = {} }) {
  const tag = etag ?? `"${createHash('sha256').update(body).digest('hex').slice(0, 32)}"`;
  res.writeHead(status, {
    'Content-Type': type, 'Content-Length': body.length, ETag: tag, 'Last-Modified': modified, 'Accept-Ranges': 'bytes', ...headers,
  });
  res.end(req.method === 'HEAD' ? undefined : body);
}

// A file's content type by its name (`_last_checkpoint` is JSON too).
function typeOf(name) {
  if (name === '_last_checkpoint') return JSON_TYPE;
  const types = { '.json': JSON_TYPE, '.parquet': 'application/vnd.apache.parquet', '.csv': 'text/csv; charset=utf-8' };
  return types[extname(name)] ?? 'application/octet-stream';
}

// The bytes a Range header asks for, as {start, end}: `bytes=a-b`, `bytes=a-` (to the end) and `bytes=-n` (the last n).
// 'unsatisfiable' when it starts past the end, or asks for no bytes (416). Anything else is ignored and the file is
// answered whole, as RFC 9110 lets a server do with a range it does not do.
function rangeOf(header, size) {
  const m = /^bytes=(\d*)-(\d*)$/.exec(header ?? '');
  if (!m || (m[1] === '' && m[2] === '')) return null;
  if (m[1] === '') {
    const n = Number(m[2]);
    return n === 0 || size === 0 ? 'unsatisfiable' : { start: Math.max(0, size - n), end: size - 1 };
  }
  const start = Number(m[1]);
  if (m[2] !== '' && Number(m[2]) < start) return null; // (syntactically invalid: ignored)
  if (start >= size) return 'unsatisfiable';
  return { start, end: m[2] === '' ? size - 1 : Math.min(Number(m[2]), size - 1) };
}

// The lake's checks: the page reads a node's published Delta table over a web server that lists nothing, and each read is
// the node's own answer for the same bytes (expected.json). The latest version is read first: the page keeps what it
// read, and `Object.keys` would put "0", "1", ... first.
async function lakeChecks(dir) {
  let expected;
  try {
    expected = JSON.parse(await readFile(join(dir, 'expected.json'), 'utf8'));
  } catch (e) {
    if (e.code === 'ENOENT') return;
    throw e;
  }
  const web = await serveFolder(dir);
  const at = `http://127.0.0.1:${web.port}/orders`;
  const { versions, file } = expected;
  const order = ['latest', ...Object.keys(versions).filter((k) => k !== 'latest')];
  let latest_requests = 0;
  for (const key of order) {
    const from = web.served();
    const clause = key === 'latest' ? '' : `, version => ${key}`;
    const answer = await ask(`lake ${key}`, 'POST', '/sql?format=csv', undefined, `SELECT * FROM read_delta('${at}'${clause}) ORDER BY id`);
    if (key === 'latest') latest_requests = web.served() - from;
    const want = versions[key];
    if (answer) judge(`lake ${key}`, answer, typeof want === 'string' ? { status: 200, sqlstate: null, out: want } : { error: true });
  }
  const counted = await ask('lake file', 'POST', '/sql?format=csv', undefined, `SELECT count(*) AS n FROM '${at}/${file.name}'`);
  if (counted) judge('lake file', counted, { status: 200, sqlstate: null, out: file.out });
  const refusals = [
    ['lake read_delta of /none', `SELECT * FROM read_delta('http://127.0.0.1:${web.port}/none')`, { error: true, says: 'no Delta table there' }],
    ['lake read_parquet of a folder', `SELECT * FROM read_parquet('${at}/')`, { status: 501, sqlstate: '0A000' }],
    ['lake a file not there', `SELECT * FROM '${at}/nope.parquet'`, { status: 404, sqlstate: '42P01' }],
    ['lake read_delta with nothing listening', "SELECT * FROM read_delta('http://127.0.0.1:1/x')", { status: 503, sqlstate: '58030' }],
  ];
  for (const [name, text, want] of refusals) {
    const answer = await ask(name, 'POST', '/sql?format=csv', undefined, text);
    if (answer) judge(name, answer, want);
  }
  report.lake_versions = order.length - 1;
  report.lake_requests = latest_requests;
  web.server.closeAllConnections(); // (Node waits for kept-alive sockets otherwise)
  await new Promise((closed) => web.server.close(closed));
}

await lakeChecks(lake);
const result = { ok: failed.length === 0, checks: passed, failed, open_ms, series_10m_ms, wasm_bytes: wasm.length, ...report };
if (sizes) {
  result.gzip_bytes = gzipSync(wasm, { level: 9 }).length;
  result.brotli_bytes = brotliCompressSync(wasm, {
    params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_SIZE_HINT]: wasm.length },
  }).length;
}
console.log(JSON.stringify(result));
process.exitCode = result.ok ? 0 : 1;
