#!/usr/bin/env node
// The page form's module (web/, designs/l2-b4-web.md) checked against what a node answers (ADR-061's contract: files are
// files, tables are made by SQL, the routes mean what a node's mean): each request's status, headers and body.
// `node tools/web_check.mjs [--pkg web/pkg] [--sizes] [--lake web/lake] [--browser]`; one JSON line, exit 1 on a failure.
// The module runs in Node, or with `--browser` in headless Chromium: a page on 127.0.0.1 loads it, and the lake's checks
// read the lake from another origin, so CORS decides what the page may see. Playwright isn't a dependency of the
// repository: `--browser` loads it from the working folder or NODE_PATH, as in
// `NODE_PATH=/opt/node-tools/node_modules node tools/web_check.mjs --pkg web/pkg --browser`. The lake's checks run when
// its folder has expected.json (made by tools/web_lake.py): a Delta table a node published, read over a web server that
// lists nothing, each read compared with the node's own answer for the same bytes. In Chromium a kept page (`Page.kept`,
// its files and tables in the origin's private file system) is checked with reloads, then the demo page
// (web/demo/index.html, `demoChecks`) in a browser of its own; in Node the kept page is refused by name.
import { createHash } from 'node:crypto';
import { readFile, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { basename, delimiter, dirname, extname, join, resolve, sep } from 'node:path';
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
// Where the module runs. Nothing below depends on it but the lake's hidden server (`lakeChecks`): Node's fetch shows
// every header, so there that server's 206s carry no Content-Range, where a browser is sent one and hides it.
const IN_BROWSER = args.includes('--browser');

const wasm = await readFile(join(pkg, 'pondra_web_bg.wasm'));
const enc = new TextEncoder();
const dec = new TextDecoder();
const ORDERS = 'id,name,qty\n1,a,5\n2,b,7\n3,c,9\n';
const NAMES = 'id,label\n1,x\n3,z\n';
const TEXT = 'text/plain; charset=utf-8';
const JSON_TYPE = 'application/json';
// The demo page the browser checks (web/demo/index.html, served at /demo/), and the brand's colours it copies.
const DEMO = new URL('../web/demo/index.html', import.meta.url);
const BRAND_COLORS = new URL('../brand/colors.css', import.meta.url);

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

// One request of a check, to the page object `on` ('pondra', or 'kept' in Chromium); a throw is a failure of that check,
// not of the run.
async function ask(name, method, path, headers, body, on = 'pondra') {
  try {
    return await target.request(method, path, headers, body, on);
  } catch (e) {
    failed.push(name);
    console.error(`${name}: threw ${e}`);
    return null;
  }
}

// The module in Node: imported from the --pkg folder, and its Page made and timed here.
async function openInNode() {
  const { default: init, Page } = await import(pathToFileURL(join(pkg, 'pondra_web.js')).href);
  await init({ module_or_path: wasm });
  const t0 = performance.now();
  const page = new Page();
  const open_ms = Number((performance.now() - t0).toFixed(1));
  return {
    open_ms,
    Page,
    request: (method, path, headers, body, on = 'pondra') => {
      if (on !== 'pondra') throw new Error(`Node has no page ${on}: a kept page is in a browser`);
      return page.request(method, path, headers, body);
    },
    close: async () => {},
  };
}

// A page's console errors are printed, and its uncaught errors are failures of the run.
function watch(page) {
  page.on('console', (m) => {
    if (m.type() === 'error') console.error(`browser console error: ${m.text()}`);
  });
  page.on('pageerror', (e) => {
    failed.push(`page error: ${e.message}`);
    console.error(`browser page error: ${e.message}`);
  });
}

// The module in headless Chromium, on a page of its own: a 127.0.0.1 origin, which the lake's servers are not. Each
// request is a call of `window.pondra.request` in that page, and the browser is closed by `close` however the run ends.
async function openInBrowser() {
  const chromium = await playwrightChromium();
  const site = await serveSite();
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    watch(page);
    await page.goto(`http://127.0.0.1:${site.port}/`, { waitUntil: 'commit' }); // (the module takes long to start: waited for below)
    await untilStarted(page);
    return {
      browser: browser.version(),
      page,
      open_ms: await page.evaluate(() => window.open_ms),
      request: (method, path, headers, body, on) => requestInBrowser(page, method, path, headers, body, on),
      close: () => closeAll(browser, site),
    };
  } catch (e) {
    await closeAll(browser, site);
    throw e;
  }
}

// One request in the page, to the page object `on` (`window.pondra`, or `window.kept`). A Uint8Array can't cross into the
// page, so bytes go in as numbers and come back as numbers.
async function requestInBrowser(page, method, path, headers, body, on = 'pondra') {
  const sent = typeof body === 'string' || body === undefined ? body : Array.from(body);
  const answer = await page.evaluate(
    async ({ method, path, headers, body, on }) => {
      const bytes = typeof body === 'string' || body === undefined ? body : new Uint8Array(body);
      const out = await window[on].request(method, path, headers, bytes);
      return { status: out.status, headers: out.headers, body: Array.from(out.body) };
    },
    { method, path, headers, body: sent, on },
  );
  return { ...answer, body: Uint8Array.from(answer.body) };
}

async function closeAll(browser, site) {
  try {
    await browser?.close();
  } finally {
    await site.close();
  }
}

// Playwright from where it is installed: the working folder, then NODE_PATH's folders. Not found, one line says what to
// do and the run stops.
async function playwrightChromium() {
  const from = [process.cwd(), ...(process.env.NODE_PATH ?? '').split(delimiter).filter(Boolean)];
  let file;
  try {
    file = createRequire(import.meta.url).resolve('playwright', { paths: from });
  } catch {
    console.error('playwright is not installed where this can find it: install it, or set NODE_PATH to the folder holding it');
    process.exit(1);
  }
  const mod = await import(pathToFileURL(file).href);
  return mod.chromium ?? mod.default.chromium;
}

// The page the browser opens. Its module is the one under /pkg/, made and timed as Node makes and times it; a failure to
// start is kept for the run to say, rather than left to a page error and a timeout.
const MODULE_TYPES = { '.js': 'text/javascript; charset=utf-8', '.wasm': 'application/wasm' };
const PAGE = `<!doctype html>
<meta charset="utf-8">
<title>pondra web check</title>
<script type="module">
  try {
    const { default: init, Page } = await import('/pkg/pondra_web.js');
    await init();
    window.Page = Page; // (for the kept checks: Page.kept is made in the page)
    const t0 = performance.now();
    window.pondra = new Page();
    window.open_ms = Number((performance.now() - t0).toFixed(1));
    window.ready = true;
  } catch (e) {
    window.startError = String(e) || 'no message';
  }
</script>
`;

// The page's server on 127.0.0.1: the page at `/`, and the --pkg folder's files under `/pkg/`. The .wasm goes as
// `application/wasm`, which a browser needs to compile it while it streams in.
async function serveSite() {
  const server = createServer(async (req, res) => {
    try {
      await servePage(req, res);
    } catch (e) {
      send(res, 500, TEXT, String(e));
    }
  });
  await new Promise((listening) => server.listen(0, '127.0.0.1', listening));
  return {
    port: server.address().port,
    close: () => {
      server.closeAllConnections(); // (a kept-alive socket would hold the server open)
      return new Promise((closed) => server.close(closed));
    },
  };
}

async function servePage(req, res) {
  const { pathname } = new URL(req.url, 'http://127.0.0.1');
  if (pathname === '/') return send(res, 200, 'text/html; charset=utf-8', PAGE);
  if (pathname === '/demo/' || pathname === '/demo/index.html') {
    return send(res, 200, 'text/html; charset=utf-8', await readFile(DEMO, 'utf8'));
  }
  const name = /^\/pkg\/([\w.-]+)$/.exec(pathname)?.[1];
  const type = name && MODULE_TYPES[extname(name)];
  const body = type ? await readFile(join(pkg, name)).catch(() => null) : null;
  if (!body) return send(res, 404, TEXT, 'not found');
  res.writeHead(200, { 'Content-Type': type, 'Content-Length': body.length });
  res.end(body);
}

function send(res, status, type, text) {
  res.writeHead(status, { 'Content-Type': type, 'Content-Length': Buffer.byteLength(text) });
  res.end(text);
}

// The rows a kept page's table holds, before and after the INSERT, and the bytes of the file as it was put.
const KEPT_ROWS = 'id,name\n1,a\n2,b\n3,c\n';
const KEPT_ROWS_AFTER_INSERT = 'id,name\n1,a\n2,b\n3,c\n9,z\n';
// The big file of the kept checks: 524,288 rows of `id,text`, a 40-character text, about 24 MiB (made in the page).
const BIG_ROWS = 524288;
const BIG_TEXT = 'abcdefghij'.repeat(4);

// Same as `a` and `b`, counted as a check of its own.
function same(name, got, want) {
  if (got === want) passed++;
  else {
    failed.push(name);
    console.error(`${name}: ${JSON.stringify(got)} is not ${JSON.stringify(want)}`);
  }
}

// What a page's GET /objects must list: its tables' names, sorted, and the rows of the tables given.
function objectsProblems(j, names, rows = {}) {
  const found = j.objects ?? [];
  const problems = [];
  const listed = found.map((o) => o.name);
  if (JSON.stringify(listed) !== JSON.stringify(names)) problems.push(`objects ${JSON.stringify(listed)}`);
  for (const [name, count] of Object.entries(rows)) {
    const object = found.find((o) => o.name === name);
    if (object?.rows !== count) problems.push(`${name} rows ${object?.rows}`);
  }
  return problems;
}

// The lake's expected answers (made by tools/web_lake.py), or None when its folder has none.
async function expectedOf(dir) {
  try {
    return JSON.parse(await readFile(join(dir, 'expected.json'), 'utf8'));
  } catch (e) {
    if (e.code === 'ENOENT') return null;
    throw e;
  }
}

// Waits until the page's module has started in the browser, or has failed to start (the run then says why). It takes
// long to start, so this is waited for after each load of the page, the first and each reload.
async function untilStarted(page) {
  await page.waitForFunction(() => window.ready || window.startError, null, { timeout: 120_000 });
  const startError = await page.evaluate(() => window.startError);
  if (startError) throw new Error(`the module did not start in the browser: ${startError}`);
}

// `window.kept` is the kept page of `folder`, made in the page. A rejection is one of the run's failures, and the checks
// that need the page are then left out (the caller stops).
async function openKept(page, folder) {
  const error = await page.evaluate(async (folder) => {
    try {
      window.kept = await window.Page.kept(folder);
      return null;
    } catch (e) {
      return String(e);
    }
  }, folder);
  if (error) {
    failed.push(`Page.kept('${folder}')`);
    console.error(`Page.kept('${folder}'): ${error}`);
  }
  return !error;
}

// The version checks of one file on one page (`on`), the file already put: its etag is read, a second PUT without If-Match
// is 409, one with the etag read replaces the file (so its etag changes), and one with the old etag is 412.
async function versionChecks(on, path) {
  const read = await ask(`GET /files/${path} (${on})`, 'GET', `/files/${path}`, undefined, undefined, on);
  if (read) judge(`GET /files/${path} (${on})`, read, { status: 200, sqlstate: null, etag: true });
  const first = read?.headers.etag;
  const again = await ask(`PUT again without If-Match (${on})`, 'PUT', `/files/${path}`, undefined, 'x\n', on);
  if (again) {
    judge(`PUT again without If-Match (${on})`, again, {
      status: 409, sqlstate: null, type: TEXT,
      out: `files/${path} is there already: send If-Match with the version you read to replace it, or put it under another name`,
    });
  }
  const replaced = first && (await ask(`PUT with the etag it read (${on})`, 'PUT', `/files/${path}`, { 'If-Match': first }, ORDERS, on));
  if (replaced) {
    judge(`PUT with the etag it read (${on})`, replaced, { status: 200, sqlstate: null, type: JSON_TYPE, etag: true });
    if (replaced.headers.etag === first) {
      failed.push(`PUT with the etag it read changes the version (${on})`);
      console.error(`PUT with the etag it read changes the version (${on}): the etag did not change`);
    } else passed++;
    const stale = await ask(`PUT with the stale etag (${on})`, 'PUT', `/files/${path}`, { 'If-Match': first }, 'x\n', on);
    if (stale) {
      judge(`PUT with the stale etag (${on})`, stale, {
        status: 412, sqlstate: null, type: TEXT,
        out: `files/${path}: the file changed since it was opened: open it again, or save under another name`,
      });
    }
  }
}

// In Node there is no origin's private file system, so `Page.kept` is refused by name: the kept checks are left out.
async function keptRefused(Page) {
  const refused = await Page.kept().then(() => null, (e) => String(e));
  if (refused?.includes('OPFS')) passed++;
  else {
    failed.push('Page.kept() in Node');
    console.error(`Page.kept() in Node: ${refused ?? 'it was not refused'}`);
  }
  report.kept = 'left out: Node has no OPFS';
}

// The kept page in Chromium (`Page.kept`): its files and tables are kept in the origin's private file system, so they are
// there after a reload, and a large file is read by ranges, not whole. It runs last among the checks: a reload runs the
// page's script again, so `window.pondra` is then a new memory page.
async function keptChecks(page, lake) {
  const expected = await expectedOf(lake);
  // (a folder left by an earlier run is removed first)
  await page.evaluate(async () => {
    try {
      await (await navigator.storage.getDirectory()).removeEntry('pondra-check', { recursive: true });
    } catch (e) {
      if (e.name !== 'NotFoundError') throw e;
    }
  });
  if (!(await openKept(page, 'pondra-check'))) return;

  // (a) Files put, and tables made from them and changed by SQL.
  const kput = await ask('kept: PUT /files/k.csv', 'PUT', '/files/k.csv', undefined, KEPT_ROWS, 'kept');
  if (kput) judge('kept: PUT /files/k.csv', kput, { status: 200, sqlstate: null, type: JSON_TYPE, etag: true });
  for (const [name, text] of [
    ['kept: CREATE TABLE k AS FROM k.csv', "CREATE TABLE k AS FROM 'k.csv'"],
    ['kept: INSERT INTO k', "INSERT INTO k VALUES (9, 'z')"],
    ['kept: CREATE TABLE "a{b}"', 'CREATE TABLE "a{b}" AS SELECT 1 AS x'],
  ]) {
    const made = await ask(name, 'POST', '/sql', undefined, text, 'kept');
    if (made) judge(name, made, { status: 200, sqlstate: null });
  }
  const before = await ask('kept: SELECT * FROM k', 'POST', '/sql?format=csv', undefined, 'SELECT * FROM k ORDER BY id', 'kept');
  if (before) judge('kept: SELECT * FROM k', before, { status: 200, sqlstate: null, out: KEPT_ROWS_AFTER_INSERT });
  const etagBefore = await ask('kept: GET /files/k.csv', 'GET', '/files/k.csv', undefined, undefined, 'kept');
  if (etagBefore) {
    judge('kept: GET /files/k.csv', etagBefore, { status: 200, sqlstate: null, type: 'text/csv; charset=utf-8', etag: true, out: KEPT_ROWS });
  }
  // The big file is made in the page and put there, so its bytes never cross from Node.
  const big = await page.evaluate(async ({ rows, text }) => {
    const lines = new Array(rows);
    for (let i = 0; i < rows; i++) lines[i] = `${i + 1},${text}\n`;
    const bytes = new TextEncoder().encode(`id,text\n${lines.join('')}`);
    const out = await window.kept.request('PUT', '/files/big.csv', {}, bytes);
    return { status: out.status, headers: out.headers, body: Array.from(out.body), size: bytes.length };
  }, { rows: BIG_ROWS, text: BIG_TEXT });
  judge('kept: PUT /files/big.csv', { ...big, body: Uint8Array.from(big.body) }, { status: 200, sqlstate: null, type: JSON_TYPE, etag: true });
  if (expected) {
    // The lake's Parquet file, put as given: the kept page reads it by its name.
    const parquet = await readFile(join(lake, 'orders', expected.file.name));
    const oput = await ask('kept: PUT /files/o.parquet', 'PUT', '/files/o.parquet', undefined, parquet, 'kept');
    if (oput) judge('kept: PUT /files/o.parquet', oput, { status: 200, sqlstate: null, type: JSON_TYPE, etag: true });
    const counted = await ask('kept: count of o.parquet', 'POST', '/sql?format=csv', undefined, "SELECT count(*) AS n FROM 'o.parquet'", 'kept');
    if (counted) judge('kept: count of o.parquet', counted, { status: 200, sqlstate: null, out: expected.file.out });
  }

  // (b) The version checks on the kept page, on a file of its own, and a PUT with If-Match on a missing file is 412.
  const vput = await ask('kept: PUT /files/v.csv', 'PUT', '/files/v.csv', undefined, ORDERS, 'kept');
  if (vput) judge('kept: PUT /files/v.csv', vput, { status: 200, sqlstate: null, type: JSON_TYPE, etag: true });
  await versionChecks('kept', 'v.csv');
  const noFile = await ask('kept: PUT with If-Match on a missing file', 'PUT', '/files/nothere.csv', { 'If-Match': '"v"' }, 'x\n', 'kept');
  if (noFile) judge('kept: PUT with If-Match on a missing file', noFile, { status: 412, sqlstate: null, type: TEXT });

  // (c) A reload: the page's script runs again, and the kept page is made again from its folder.
  await page.reload({ waitUntil: 'commit' });
  await untilStarted(page);
  if (!(await openKept(page, 'pondra-check'))) return;
  const after = await ask('kept after reload: SELECT * FROM k', 'POST', '/sql?format=csv', undefined, 'SELECT * FROM k ORDER BY id', 'kept');
  if (after && before) judge('kept after reload: SELECT * FROM k', after, { status: 200, sqlstate: null, out: dec.decode(before.body) });
  const letters = await ask('kept after reload: FROM "a{b}"', 'POST', '/sql?format=csv', undefined, 'SELECT * FROM "a{b}"', 'kept');
  if (letters) judge('kept after reload: FROM "a{b}"', letters, { status: 200, sqlstate: null, out: 'x\n1\n' });
  const listed = await ask('kept after reload: GET /objects', 'GET', '/objects', undefined, undefined, 'kept');
  if (listed) {
    judge('kept after reload: GET /objects', listed, {
      status: 200, sqlstate: null, type: JSON_TYPE, json: (j) => objectsProblems(j, ['a{b}', 'k'], { k: 4 }),
    });
  }
  if (expected) {
    const counted = await ask('kept after reload: count of o.parquet', 'POST', '/sql?format=csv', undefined, "SELECT count(*) AS n FROM 'o.parquet'", 'kept');
    if (counted) judge('kept after reload: count of o.parquet', counted, { status: 200, sqlstate: null, out: expected.file.out });
  }
  const etagAfter = await ask('kept after reload: GET /files/k.csv', 'GET', '/files/k.csv', undefined, undefined, 'kept');
  if (etagAfter) {
    judge('kept after reload: GET /files/k.csv', etagAfter, { status: 200, sqlstate: null, etag: true, out: KEPT_ROWS });
    if (etagBefore) same('kept after reload: k.csv keeps its etag', etagAfter.headers.etag, etagBefore.headers.etag);
  }

  // (d) Read as asked, not whole: a LIMIT 1 over the big file slices less than half of its bytes, since the scan stops at
  // the limit. Each slice's size is added up while the read runs; the slice is restored after, however the read ends.
  const limited = await page.evaluate(async () => {
    const slice = Blob.prototype.slice;
    let read = 0;
    Blob.prototype.slice = function (...args) {
      const part = slice.apply(this, args);
      read += part.size;
      return part;
    };
    try {
      const out = await window.kept.request('POST', '/sql?format=csv', {}, "SELECT * FROM 'big.csv' LIMIT 1");
      return { status: out.status, headers: out.headers, body: Array.from(out.body), read };
    } finally {
      Blob.prototype.slice = slice;
    }
  });
  judge('kept: SELECT * FROM big.csv LIMIT 1', { ...limited, body: Uint8Array.from(limited.body) }, {
    status: 200, sqlstate: null, out: `id,text\n1,${BIG_TEXT}\n`,
  });
  const bigBytes = big.size;
  if (limited.read * 2 < bigBytes) passed++;
  else {
    failed.push('kept: LIMIT 1 reads less than half of big.csv');
    console.error(`kept: LIMIT 1 read ${limited.read} of ${bigBytes} bytes`);
  }
  report.kept_bytes_read = `${limited.read} of ${bigBytes} for LIMIT 1`;

  // (e) A file removed and a table dropped: after a reload, neither is there.
  const removed = await ask('kept: DELETE /files/k.csv', 'DELETE', '/files/k.csv', undefined, undefined, 'kept');
  if (removed) judge('kept: DELETE /files/k.csv', removed, { status: 200, sqlstate: null, type: JSON_TYPE, out: '{"removed":"files/k.csv"}' });
  const dropped = await ask('kept: DROP TABLE k', 'POST', '/sql', undefined, 'DROP TABLE k', 'kept');
  if (dropped) judge('kept: DROP TABLE k', dropped, { status: 200, sqlstate: null });
  await page.reload({ waitUntil: 'commit' });
  await untilStarted(page);
  if (!(await openKept(page, 'pondra-check'))) return;
  const droppedList = await ask('kept after the drop: GET /objects', 'GET', '/objects', undefined, undefined, 'kept');
  if (droppedList) {
    judge('kept after the drop: GET /objects', droppedList, { status: 200, sqlstate: null, type: JSON_TYPE, json: (j) => objectsProblems(j, ['a{b}']) });
  }
  const gone = await ask('kept after the drop: GET /files/k.csv', 'GET', '/files/k.csv', undefined, undefined, 'kept');
  if (gone) judge('kept after the drop: GET /files/k.csv', gone, { status: 404, sqlstate: null, type: TEXT, out: 'no such file: files/k.csv' });

  // (f) A folder name that is a path, or that reaches out of the page's folder, is refused by name.
  for (const folder of ['a/b', '..']) {
    const refused = await page.evaluate(async (folder) => {
      try {
        await window.Page.kept(folder);
        return null;
      } catch (e) {
        return String(e);
      }
    }, folder);
    if (refused?.includes('folder name')) passed++;
    else {
      failed.push(`Page.kept('${folder}') is refused by name`);
      console.error(`Page.kept('${folder}'): ${refused ?? 'it was not refused'}`);
    }
  }

  // (g) A folder is one page's at a time: while a kept page holds it, a second page given it is refused by name, and once
  // that page is freed the folder opens again. The folder is one of its own (`pondra-lock`, removed first), and no page a
  // later check uses is freed here.
  const held = await page.evaluate(async () => {
    try {
      await (await navigator.storage.getDirectory()).removeEntry('pondra-lock', { recursive: true });
    } catch (e) {
      if (e.name !== 'NotFoundError') throw e;
    }
    const first = await window.Page.kept('pondra-lock');
    const refused = await window.Page.kept('pondra-lock').then(() => null, (e) => String(e));
    first.free();
    const reopened = await window.Page.kept('pondra-lock').then((again) => { again.free(); return true; }, (e) => String(e));
    return { refused, reopened };
  });
  if (held.refused?.includes('pondra-lock') && held.reopened === true) passed++;
  else {
    failed.push("Page.kept('pondra-lock') while another page holds it");
    console.error(`Page.kept('pondra-lock'): refused ${held.refused ?? 'not refused'}, reopened ${held.reopened}`);
  }
}

// The demo page's files, and what its steps expect: the tables, the join's answer, and the three totals it gives.
const DEMO_PEOPLE = { name: 'people.csv', mimeType: 'text/csv', buffer: Buffer.from('id,name\n1,Ana\n2,Ben\n3,Cy\n') };
const DEMO_ORDERS = { name: 'orders.csv', mimeType: 'text/csv', buffer: Buffer.from('id,person,amount\n1,1,10\n2,2,20\n3,1,5\n4,3,7\n5,2,30\n') };
const DEMO_CITIES = '{"id":1,"city":"Oslo"}\n{"id":2,"city":"Bergen"}\n{"id":3,"city":"Tromso"}\n';
const TABLES_AB = [['orders', 5], ['people', 3]];
const TABLES_ABC = [['cities', 3], ['orders', 5], ['people', 3]];
const JOIN = 'SELECT p.name, sum(o.amount) AS total FROM people p JOIN orders o ON o.person = p.id GROUP BY p.name ORDER BY p.name';
const JOIN_ROWS = [['Ana', '15'], ['Ben', '50'], ['Cy', '7']];
const THREE_ROWS = /^3 rows in \d+ ms$/;

// The demo page (web/demo/index.html) in Chromium, in a browser of its own: a new context, so its private file system
// starts empty. Files are chosen and dropped, a join is answered, a reload keeps them, a table is removed, and an error
// is shown with its SQLSTATE. Each step is one check. A step that throws (a wait that times out) is a failure of that
// step, and the steps after it are left out, since they need what it made.
async function demoChecks(page) {
  const before = failed.length;
  const url = `${new URL(page.url()).origin}/demo/`;
  const demo = await page.context().browser().newPage();
  try {
    watch(demo);
    for (const [name, check] of demoSteps(demo, url)) {
      if (!(await demoCheck(name, check))) break;
    }
  } finally {
    await demo.close();
  }
  if (failed.length === before) report.demo = '3 files, a join answered, kept across a reload';
}

// One step's check: its function gives [what came back, what is wanted], judged as `same` judges them. A throw is a
// failure of the step; the result says whether the steps after it may run.
async function demoCheck(name, check) {
  let pair;
  try {
    pair = await check();
  } catch (e) {
    failed.push(name);
    console.error(`${name}: threw ${String(e).split('\n')[0]}`);
    return false;
  }
  same(name, JSON.stringify(pair[0]), JSON.stringify(pair[1]));
  return true;
}

// The steps in order, each a name and a function that drives the demo and returns [what came back, what is wanted].
function demoSteps(demo, url) {
  return [
    ['demo: accent colours are brand/colors.css values, light and dark', accentPair],
    ['demo: starts', async () => {
      await demo.goto(url, { waitUntil: 'commit' });
      await untilStarted(demo);
      return [await demo.evaluate(() => window.ready === true), true];
    }],
    ['demo: a. two files chosen, two tables made', async () => {
      await demo.setInputFiles('#files', [DEMO_PEOPLE, DEMO_ORDERS]);
      return [await untilListed(demo, TABLES_AB), TABLES_AB];
    }],
    ['demo: b. one file dropped, a third table made', async () => {
      const dataTransfer = await demo.evaluateHandle((text) => {
        const transfer = new DataTransfer();
        transfer.items.add(new File([text], 'cities.json', { type: 'application/json' }));
        return transfer;
      }, DEMO_CITIES);
      await demo.dispatchEvent('body', 'drop', { dataTransfer });
      return [await untilListed(demo, TABLES_ABC), TABLES_ABC];
    }],
    ['demo: c. a join typed and run, answered', async () => {
      await demo.fill('#sql', JOIN);
      await demo.click('#run');
      await untilAnswered(demo);
      const shown = await shownAnswer(demo);
      return [[shown.head, shown.rows, THREE_ROWS.test(shown.status)], [['name', 'total'], JOIN_ROWS, true]];
    }],
    ['demo: d. kept across a reload: tables listed, join answered with Ctrl+Enter', async () => {
      await demo.reload({ waitUntil: 'commit' });
      await untilStarted(demo);
      const tables = await untilListed(demo, TABLES_ABC);
      await demo.fill('#sql', JOIN);
      await demo.focus('#sql');
      await demo.keyboard.press('Control+Enter');
      await untilAnswered(demo);
      const shown = await shownAnswer(demo);
      return [
        [tables, shown.head, shown.rows, THREE_ROWS.test(shown.status)],
        [TABLES_ABC, ['name', 'total'], JOIN_ROWS, true],
      ];
    }],
    ['demo: e. the remove button drops a table', async () => {
      await demo.click('button[aria-label="Remove table cities"]');
      return [await untilListed(demo, TABLES_AB), TABLES_AB];
    }],
    ['demo: f. an error shown with its SQLSTATE', async () => {
      await demo.fill('#sql', 'SELECT * FROM nothere');
      await demo.click('#run');
      await demo.waitForFunction(() => document.querySelector('#status').classList.contains('error'));
      const shown = await shownAnswer(demo);
      return [[shown.error, shown.status.includes('42P01')], [true, true]];
    }],
  ];
}

// The demo's accent is brand/colors.css's, copied into the page (one file, nothing fetched but its module), so this
// fails when a copy drifts: the page's light and dark accent and soft accent, against the brand's.
async function accentPair() {
  const html = await readFile(DEMO, 'utf8');
  const css = await readFile(BRAND_COLORS, 'utf8');
  const valuesOf = (text, name) => [...text.matchAll(new RegExp(`--${name}:\\s*(#[0-9a-fA-F]{6})`, 'g'))].map((m) => m[1].toUpperCase());
  const brandPair = (name) => [...valuesOf(css, name), ...valuesOf(css, `${name}-dark`)];
  return [
    { accent: valuesOf(html, 'accent'), soft: valuesOf(html, 'accent-soft') },
    { accent: brandPair('pondra-accent'), soft: brandPair('pondra-accent-soft') },
  ];
}

// Waits until the page's status shows an answer's rows and time (a run, after a reload or not).
function untilAnswered(demo) {
  return demo.waitForFunction(() => /rows in \d+ ms/.test(document.querySelector('#status').textContent));
}

// What the page's answer shows: its header's cells, its rows' cells, its status, and whether that status is an error.
function shownAnswer(demo) {
  return demo.evaluate(() => ({
    head: [...document.querySelectorAll('#result thead th')].map((th) => th.textContent),
    rows: [...document.querySelectorAll('#result tbody tr')].map((tr) => [...tr.cells].map((td) => td.textContent)),
    status: document.querySelector('#status').textContent,
    error: document.querySelector('#status').classList.contains('error'),
  }));
}

// The tables `#tables` lists, as [name, rows] pairs in the order it lists them.
function listedTables(demo) {
  return demo.$$eval('#tables tbody tr', (trs) => trs
    .filter((tr) => tr.querySelector('.name'))
    .map((tr) => [tr.querySelector('.name').textContent, Number(tr.cells[1].textContent)]));
}

// Waits (up to 30 s) until `#tables` lists `want`, and returns what it lists then, so a mismatch shows what came back.
async function untilListed(demo, want) {
  const until = Date.now() + 30_000;
  let listed = await listedTables(demo);
  while (JSON.stringify(listed) !== JSON.stringify(want) && Date.now() < until) {
    await new Promise((wake) => setTimeout(wake, 100));
    listed = await listedTables(demo);
  }
  return listed;
}

const target = IN_BROWSER ? await openInBrowser() : await openInNode();
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

// The version checks of orders.csv, put above, on the memory page (`versionChecks`, below).
await versionChecks('pondra', 'orders.csv');

// DELETE answers the node's JSON, and the file is gone: FROM 'names.csv' is 42P01, and the table made from it stays.
const removed = await ask('DELETE /files/names.csv', 'DELETE', '/files/names.csv');
if (removed) judge('DELETE /files/names.csv', removed, { status: 200, sqlstate: null, type: JSON_TYPE, out: '{"removed":"files/names.csv"}' });
const gone = await ask('DELETE a missing file', 'DELETE', '/files/names.csv');
if (gone) judge('DELETE a missing file', gone, { status: 404, sqlstate: null, type: TEXT, out: 'no such file: files/names.csv' });
const read_gone = await ask("FROM 'names.csv' after its DELETE", 'POST', '/sql?format=csv', undefined, "FROM 'names.csv'");
if (read_gone) judge("FROM 'names.csv' after its DELETE", read_gone, { status: 404, sqlstate: '42P01' });
const kept = await ask('FROM names after its file is gone', 'POST', '/sql?format=csv', undefined, 'FROM names ORDER BY id');
if (kept) judge('FROM names after its file is gone', kept, { status: 200, sqlstate: null, out: 'id,label\n1,x\n3,z\n' });

// Tables by SQL: CREATE TABLE makes an empty one, and INSERT adds its rows (a page's own tables take DML).
const empty = await ask('CREATE TABLE t (a INT)', 'POST', '/sql', undefined, 'CREATE TABLE t (a INT)');
if (empty) judge('CREATE TABLE t (a INT)', empty, { status: 200, sqlstate: null });
const insert = await ask('INSERT INTO t', 'POST', '/sql', undefined, 'INSERT INTO t VALUES (1)');
if (insert) judge('INSERT INTO t', insert, { status: 200, sqlstate: null });
const inserted = await ask('count of t after its INSERT', 'POST', '/sql?format=csv', undefined, 'SELECT count(*) AS n FROM t');
if (inserted) judge('count of t after its INSERT', inserted, { status: 200, sqlstate: null, out: 'n\n1\n' });

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
// listed (a folder, a path ending in `/`, a name not there: 404). It counts the requests it answers, and the CORS
// preflights apart from them. Its answers allow any origin, and no browser keeps one (Chromium would keep one from
// Last-Modified). `hidden` is a server whose page can't see its Content-Range: it exposes none of its headers, and
// `sendsRange` false leaves the Content-Range out of its 206s, as a server that sends none does. It keeps the paths
// asked for, so a check can tell which files a read opened.
async function serveFolder(root, { hidden = false, sendsRange = true } = {}) {
  const started = new Date().toUTCString();
  const cors = { 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' };
  if (!hidden) cors['Access-Control-Expose-Headers'] = 'Content-Range, ETag, Last-Modified';
  let requests = 0;
  let preflights = 0;
  const paths = new Set();
  const server = createServer(async (req, res) => {
    if (req.method === 'OPTIONS') {
      preflights++;
      console.error(`preflight ${req.url}: asks for ${req.headers['access-control-request-headers'] ?? 'no headers'}`);
      return answerPreflight(res, cors);
    }
    requests++;
    paths.add(decodeURIComponent(new URL(req.url, 'http://127.0.0.1').pathname));
    for (const [name, value] of Object.entries(cors)) res.setHeader(name, value);
    try {
      await answerFile(root, started, req, res, sendsRange);
    } catch (e) {
      reply(req, res, { status: 500, body: enc.encode(String(e)), modified: started });
    }
  });
  await new Promise((listening) => server.listen(0, '127.0.0.1', listening));
  return { server, port: server.address().port, served: () => requests, preflights: () => preflights, paths: () => [...paths] };
}

// A CORS preflight, which a browser sends before a request CORS doesn't let through alone (a suffix Range is one).
function answerPreflight(res, cors) {
  res.writeHead(204, { ...cors, 'Access-Control-Allow-Methods': 'GET, HEAD', 'Access-Control-Allow-Headers': 'Range' });
  res.end();
}

// One request: a file's bytes, or the Range of them; anything that is not a file is 404 (405 for a method but GET and HEAD).
async function answerFile(root, started, req, res, sendsRange = true) {
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
    headers: sendsRange ? { 'Content-Range': `bytes ${range.start}-${range.end}/${body.length}` } : {},
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

// Whether a path the lake's server was asked for is one of the table's data files (not its log's).
function isData(path) {
  return path.endsWith('.parquet') && !path.includes('/_delta_log/');
}

// A port nothing listens on: one the system gave a server that is closed again.
async function closedPort() {
  const server = createServer();
  await new Promise((listening) => server.listen(0, '127.0.0.1', listening));
  const { port } = server.address();
  await new Promise((closed) => server.close(closed));
  return port;
}

// Stops a lake server: its kept-alive sockets close first, or Node waits for them.
function stop(web) {
  web.server.closeAllConnections();
  return new Promise((closed) => web.server.close(closed));
}

// The lake's checks: the page reads a node's published Delta table over a web server that lists nothing, and each read is
// the node's own answer for the same bytes (expected.json). The latest version is read first: the page keeps what it
// read, and `Object.keys` would put "0", "1", ... first.
async function lakeChecks(dir) {
  const expected = await expectedOf(dir);
  if (!expected) return;
  const web = await serveFolder(dir);
  const nobody = await closedPort(); // (not port 1: a browser refuses to ask it at all, so nothing would be tried)
  const hidden = await serveFolder(dir, { hidden: true, sendsRange: IN_BROWSER });
  const at = `http://127.0.0.1:${web.port}/orders`;
  const { versions, file } = expected;
  const order = ['latest', ...Object.keys(versions).filter((k) => k !== 'latest')];
  let [latest_requests, latest_files] = [0, 0];
  for (const key of order) {
    const from = web.served();
    const clause = key === 'latest' ? '' : `, version => ${key}`;
    const answer = await ask(`lake ${key}`, 'POST', '/sql?format=csv', undefined, `SELECT * FROM read_delta('${at}'${clause}) ORDER BY id`);
    if (key === 'latest') [latest_requests, latest_files] = [web.served() - from, web.paths().filter(isData).length];
    const want = versions[key];
    if (answer) judge(`lake ${key}`, answer, typeof want === 'string' ? { status: 200, sqlstate: null, out: want } : { error: true });
  }
  // A filter on one id opens the one file whose range holds it: the others are skipped by the ranges the log gives
  // (delta.rs `stats`), never fetched. Read from a server of its own, the folder one level up, so the files' paths are
  // new to the page too (a file's footer is kept by its path) and nothing it kept is reused.
  const fresh = await serveFolder(dirname(dir));
  const one = await ask('lake one id, one file opened', 'POST', '/sql?format=csv', undefined,
    `SELECT id, amount FROM read_delta('http://127.0.0.1:${fresh.port}/${basename(dir)}/orders') WHERE id = 3`);
  const opened = fresh.paths().filter(isData);
  if (one) judge('lake one id, one file opened', one, { status: 200, sqlstate: null, out: 'id,amount\n3,30\n' });
  if (opened.length !== 1) {
    failed.push('lake one id, one file opened');
    console.error(`lake one id, one file opened: it opened ${opened.length}: ${opened.join(', ')}`);
  }
  report.lake_files = `${opened.length} of ${latest_files} for one id`;
  await stop(fresh);
  const counted = await ask('lake file', 'POST', '/sql?format=csv', undefined, `SELECT count(*) AS n FROM '${at}/${file.name}'`);
  if (counted) judge('lake file', counted, { status: 200, sqlstate: null, out: file.out });
  const refusals = [
    ['lake read_delta of /none', `SELECT * FROM read_delta('http://127.0.0.1:${web.port}/none')`, { error: true, says: 'no Delta table there' }],
    ['lake read_parquet of a folder', `SELECT * FROM read_parquet('${at}/')`, { status: 501, sqlstate: '0A000' }],
    ['lake a file not there', `SELECT * FROM '${at}/nope.parquet'`, { status: 404, sqlstate: '42P01' }],
    ['lake read_delta with nothing listening', `SELECT * FROM read_delta('http://127.0.0.1:${nobody}/x')`, { status: 503, sqlstate: '58030' }],
  ];
  for (const [name, text, want] of refusals) {
    const answer = await ask(name, 'POST', '/sql?format=csv', undefined, text);
    if (answer) judge(name, answer, want);
  }
  // The latest version again, from the server that hides its Content-Range: another origin, so nothing the page kept is
  // reused, and each read takes the range it asked for (`unranged` in web/src/fetch.rs).
  const hiddenAt = `http://127.0.0.1:${hidden.port}/orders`;
  const hiddenRead = await ask('lake latest, its Content-Range hidden', 'POST', '/sql?format=csv', undefined,
    `SELECT * FROM read_delta('${hiddenAt}') ORDER BY id`);
  if (hiddenRead) judge('lake latest, its Content-Range hidden', hiddenRead, { status: 200, sqlstate: null, out: versions.latest });
  report.lake_versions = order.length - 1;
  report.lake_requests = latest_requests;
  report.lake_preflights = web.preflights() + hidden.preflights();
  await stop(web);
  await stop(hidden);
}

try {
  await lakeChecks(lake);
  if (IN_BROWSER) {
    await keptChecks(target.page, lake);
    await demoChecks(target.page);
  } else await keptRefused(target.Page);
} finally {
  await target.close();
}
const result = {
  ok: failed.length === 0, checks: passed, failed, open_ms: target.open_ms, series_10m_ms, wasm_bytes: wasm.length,
  browser: target.browser, // (absent in Node: JSON leaves out an undefined value)
  ...report,
};
if (sizes) {
  result.gzip_bytes = gzipSync(wasm, { level: 9 }).length;
  result.brotli_bytes = brotliCompressSync(wasm, {
    params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_SIZE_HINT]: wasm.length },
  }).length;
}
console.log(JSON.stringify(result));
process.exitCode = result.ok ? 0 : 1;
