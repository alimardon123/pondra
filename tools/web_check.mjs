#!/usr/bin/env node
// The page form's module (web/, designs/l2-b4-web.md) checked against what a node answers (ADR-061's contract: files are
// files, tables are made by SQL, the routes mean what a node's mean): each request's status, headers and body.
// `node tools/web_check.mjs [--pkg web/pkg] [--sizes] [--lake web/lake] [--browser] [--node-bin pondra]`; one JSON line,
// exit 1 on a failure. `--node-bin` (the `pondra` binary) starts a node for the run and checks a page's table joined with
// its tables, in Node and in Chromium from the site's origin (which the node names), and from another origin (refused).
// The module runs in Node, or with `--browser` in headless Chromium: a page on 127.0.0.1 loads it, and the lake's checks
// read the lake from another origin, so CORS decides what the page may see. Playwright isn't a dependency of the
// repository: `--browser` loads it from the working folder or NODE_PATH, as in
// `NODE_PATH=/opt/node-tools/node_modules node tools/web_check.mjs --pkg web/pkg --browser`. The lake's checks run when
// its folder has expected.json (made by tools/web_lake.py): a Delta table a node published, read over a web server that
// lists nothing, each read compared with the node's own answer for the same bytes. In Chromium a kept page (`Page.kept`,
// its files and tables in the origin's private file system) is checked with reloads, then the demo page
// (web/demo/index.html, `demoChecks`) in a browser of its own, a page in a Worker that spills past its pool to the browser's
// storage (`spillWorkerChecks`), and one at its default line whose memory must peak under the module's 4 GiB (`fullLineChecks`); in Node the kept page is refused by name, and a page past its
// memory line is checked in a child process of its own (`memoryChecks`). With `--node-bin` a page joins a follower of a node's
// lake (`joinRun`): two nodes on one folder, the page's reads and writes, what never reaches it, and a non-superuser's refusal.
// The 100 MB check (`w4Checks`, W4b, after (c4)) loads a generated CSV from the page's blob into a node's table, and measures
// the nodes' CPU (Linux) and the page's wasm memory around it.
// A module holds one page at a time, so each check frees the page it made before the next makes one. A check that needs a
// page while another lives has a tab of its own, on the bare page (`/?bare`: the module with no page of its own).
import { execFileSync, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
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
// The most a page may download (`--sizes`: the module at brotli 11, as a server sends it), designs/page-same-core.md P4. It
// moves down with each cut, never up without a reason in the commit that moves it. 9.2 → 9.206 MB: eager aggregation
// (`eager.rs`, 6.2 KB), a rule every form's planner runs (TPC-DS q22 3.46 → 0.42 s), the room above main kept as it was.
const BROTLI_BUDGET = 9_206_000;
// The pondra binary the node's checks start (without it they are left out): its `serve`, on a folder and port of the run's.
const NODE_BIN = args.includes('--node-bin') ? resolve(args[args.indexOf('--node-bin') + 1]) : null;
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
  // A wrong status or SQLSTATE says the answer's words too (cut at 300 characters): the run's failures are read from them.
  const words = JSON.stringify(text.slice(0, 300));
  if (want.status !== undefined && answer.status !== want.status) why.push(`status ${answer.status} ${words}`);
  if (want.error && !(answer.status >= 400)) why.push(`status ${answer.status}, not an error`);
  if (want.says !== undefined && !text.includes(want.says)) why.push(`body ${JSON.stringify(text)}`);
  const sqlstate = headers['x-pondra-sqlstate'] ?? null;
  if (want.sqlstate !== undefined && sqlstate !== want.sqlstate) why.push(`sqlstate ${sqlstate} ${words}`);
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

// One request of a check, to the page object `on` ('pondra', 'kept', or one `withNode` made) of `from`: the run's target,
// a browser tab's, or the node's own (which has no pages, so `on` is not read). A throw is a failure of that check, not
// of the run.
async function ask(name, method, path, headers, body, on = 'pondra', from = target) {
  try {
    return await from.request(method, path, headers, body, on);
  } catch (e) {
    failed.push(name);
    console.error(`${name}: threw ${e}`);
    return null;
  }
}

// The module in Node: imported from the --pkg folder, and its Page made and timed here.
async function openInNode() {
  const { default: init, Page } = await import(pathToFileURL(join(pkg, 'pondra_web.js')).href);
  const { memory } = await init({ module_or_path: wasm });
  const t0 = performance.now();
  const page = new Page();
  const open_ms = Number((performance.now() - t0).toFixed(1));
  const pages = new Map([['pondra', page]]); // (the pages by the name `on` gives: `withNode` makes one of its own, in place of this)
  return {
    open_ms,
    Page,
    request: (method, path, headers, body, on = 'pondra') => {
      const chosen = pages.get(on);
      if (!chosen) throw new Error(`Node has no page ${on}: a kept page is in a browser`);
      return chosen.request(method, path, headers, body);
    },
    // The module's wasm memory (one instance here, shared by its pages): its size in bytes.
    memoryBytes: () => memory.buffer.byteLength,
    // The 100 MB check's CSV, made in Node by `w4Csv`: its blob URL, and the rows, sum and bytes the generator counts.
    generate: async () => blobOf(w4Csv()),
    // A module has one page: the page before is closed and freed, and nothing after `withNode` asks for it.
    withNode: async (name, url) => {
      for (const before of pages.values()) {
        await before.close();
        before.free();
      }
      pages.clear();
      pages.set(name, new Page({ node: url }));
    },
    // The page `oneAtATime` makes is the module's one page: a second `new Page()` while it lives is refused (`refused`, the
    // words), and the first is freed (not closed: its member ends in the background), after which a page is made (`reopened`).
    oneAtATime: async () => {
      let refused = null;
      try {
        new Page();
      } catch (e) {
        refused = String(e);
      }
      pages.get('pondra').free();
      pages.set('pondra', new Page());
      return { refused, reopened: pages.get('pondra') instanceof Page };
    },
    // A page joined to the node at `url` under `session` (`Page.join`), as `name`: the module's pages are closed and freed
    // first, as `withNode` frees them. `error` is the words of a join that fails (null when it joined), and `asked` the fetches
    // the join made (the module calls the global `fetch` as it makes one).
    joinAs: async (name, url, session) => {
      for (const before of pages.values()) {
        await before.close();
        before.free();
      }
      pages.clear();
      const sent = globalThis.fetch;
      let asked = 0;
      globalThis.fetch = (...args) => {
        asked++;
        return sent(...args);
      };
      try {
        pages.set(name, await Page.join(url, session));
        return { error: null, asked };
      } catch (e) {
        return { error: String(e), asked };
      } finally {
        globalThis.fetch = sent;
      }
    },
    // A blob URL of `text`, made in this process (a `blob:nodedata:` URL): a tab's own file, which the module's pages read.
    blob: async (on, text) => URL.createObjectURL(new Blob([text], { type: 'text/csv' })),
    revoke: async (on, url) => URL.revokeObjectURL(url),
    // Closes and frees the joined page `name`: its member ends, and the module has no page of that name after.
    closeAs: async (name) => {
      const page = pages.get(name);
      pages.delete(name);
      await page.close();
      page.free();
    },
    close: async () => {
      for (const last of pages.values()) {
        await last.close();
        last.free();
      }
    },
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
    // One context for the checks' tabs: they share its origin's private file system and Web Locks. (`newPage` of a context
    // made by `browser.newPage()` is refused, so the context is made here.)
    const context = await browser.newContext();
    const page = await context.newPage();
    watch(page);
    const origin = `http://127.0.0.1:${site.port}`; // (the origin a node names for this page, in PONDRA_WEB_ORIGINS)
    await page.goto(`${origin}/`, { waitUntil: 'commit' }); // (the module takes long to start: waited for below)
    await untilStarted(page);
    // The tab each page object lives in, by the name `on` gives it: `pondra` is the page's own, and each other is in a tab
    // of its own (`tab`), since a module has one page.
    const tabs = new Map([['pondra', page]]);
    // A tab of the page's context on the bare page, registered as `name`, which the checks then call by that name.
    const tab = async (name) => {
      const made = await openBare(context, origin);
      tabs.set(name, made);
      return made;
    };
    return {
      browser, // (kept for check 5's tab of the same browser)
      version: browser.version(),
      port: site.port,
      origin,
      page,
      open_ms: await page.evaluate(() => window.open_ms),
      request: (method, path, headers, body, on = 'pondra') => {
        const chosen = tabs.get(on);
        if (!chosen) throw new Error(`the browser has no page ${on}: a check makes it first`);
        return requestInBrowser(chosen, method, path, headers, body, on);
      },
      tab,
      withNode: async (name, url) => pageWithNode(await tab(name), name, url),
      // The same check as Node's `oneAtATime`, on the page's own tab: the module's one page is freed (not closed: its member ends
      // in the background), and a page is made after it.
      oneAtATime: () => page.evaluate(() => {
        let refused = null;
        try {
          new window.Page();
        } catch (e) {
          refused = String(e);
        }
        window.pondra.free();
        window.pondra = new window.Page();
        return { refused, reopened: window.pondra instanceof window.Page };
      }),
      // A page joined to the node at `url` under `session`, in a tab of its own (a module has one page per tab), as `name`:
      // `error` and `asked` as in Node, the fetches counted on the tab's `fetch`.
      joinAs: async (name, url, session) => {
        const made = await tab(name);
        return made.evaluate(async ({ name, url, session }) => {
          const sent = window.fetch;
          let asked = 0;
          window.fetch = (...args) => {
            asked++;
            return sent.apply(window, args);
          };
          try {
            window[name] = await window.Page.join(url, session);
            return { error: null, asked };
          } catch (e) {
            return { error: String(e), asked };
          } finally {
            window.fetch = sent;
          }
        }, { name, url, session });
      },
      // A blob URL of `text`, made in the tab `on` (its own origin's: only that tab's page reads it), and its revocation.
      blob: (on, text) => tabs.get(on).evaluate((text) => URL.createObjectURL(new Blob([text], { type: 'text/csv' })), text),
      revoke: (on, url) => tabs.get(on).evaluate((url) => URL.revokeObjectURL(url), url),
      // The 100 MB check's CSV, made in the tab `on` from `w4Csv`'s own text, so its blob is that tab's: only the URL and the
      // counts come out of the tab. The module's wasm memory of that tab, and the tab itself (the Playwright page).
      generate: (on) => tabs.get(on).evaluate((src) => {
        const out = new Function(`return (${src});`)()();
        return { url: URL.createObjectURL(new Blob(out.parts, { type: 'text/csv' })), rows: out.rows, sum: out.sum, bytes: out.bytes };
      }, w4Csv.toString()),
      memoryBytes: (on) => tabs.get(on).evaluate(() => window.wasmMemory.buffer.byteLength),
      tabOf: (name) => tabs.get(name),
      closeAs: (name) => tabs.get(name).evaluate(async (name) => {
        await window[name].close();
        window[name].free();
        delete window[name];
      }, name),
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

// A page of the browser, given its node: `window[name]` is a Page with the node at `url`, made as `window.pondra` is.
function pageWithNode(page, name, url) {
  return page.evaluate(({ name, url }) => {
    window[name] = new window.Page({ node: url });
  }, { name, url });
}

// A tab of `context` on the site's bare page (`/?bare`), started when this returns. The context is the one a check's other
// tabs are in, so they share its origin's private file system and Web Locks (`page.context()`, not `browser.newPage()`,
// which makes a context of its own).
async function openBare(context, origin) {
  const tab = await context.newPage();
  watch(tab);
  await tab.goto(`${origin}/?bare`, { waitUntil: 'commit' });
  await untilStarted(tab);
  return tab;
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
// `/` is the module with its one page (`window.pondra`); `/?bare` is the module with none, for a check that makes the page it
// needs (a kept page, a node's page). Either way the module is started and timed here.
function pageHtml(bare) {
  return `<!doctype html>
<meta charset="utf-8">
<title>pondra web check</title>
<script type="module">
  try {
    const { default: init, Page } = await import('/pkg/pondra_web.js');
    window.wasmMemory = (await init()).memory; // (the 100 MB check reads its size, through memoryBytes)
    window.Page = Page; // (for the kept checks: Page.kept is made in the page)
    const t0 = performance.now();
    ${bare ? '' : 'window.pondra = new Page();'}
    window.open_ms = Number((performance.now() - t0).toFixed(1));
    window.ready = true;
  } catch (e) {
    window.startError = String(e) || 'no message';
  }
</script>
`;
}
const PAGE = pageHtml(false);
const PAGE_BARE = pageHtml(true);

// The queries a page in a Worker runs past its pool (`spillWorkerChecks`), each with its answer. The sort and the join are the
// ones the page's pool can't hold: the join's keys are sparse and unordered, so a dense key's perfect hash join (which fits) and
// `range()`'s declared order (a sort-merge join that sorts nothing) are both avoided.
const WORKER_QUERIES = [
  ['sort', 'SELECT value FROM range(20000000) ORDER BY value DESC OFFSET 19999999', 'value\n0\n'],
  ['aggregate', 'SELECT count(*) AS n FROM (SELECT value % 10000000 AS k FROM range(20000000) GROUP BY 1)', 'n\n10000000\n'],
  ['join', 'SELECT count(*) AS n FROM (SELECT value * 7919 % 5000000 * 1000003 AS k FROM range(5000000)) a JOIN (SELECT value * 7919 % 5000000 * 1000003 AS k FROM range(5000000)) b ON a.k = b.k', 'n\n5000000\n'],
  ['one', 'SELECT 1 AS n', 'n\n1\n'],
];
// The Worker's page (`/spill-worker.js`): the module in a Worker, its page with a pool of 0.1 GB under a 1 GB line, each query
// answered and posted with its ms, the bytes it spilled (`spilledBytes`) and the spill file's size after it (`spillFileBytes`).
// Then each EXPLAIN of the sort and the join, and the folder's files after the page is closed.
const SPILL_WORKER = `
import init, { Page, spilledBytes, spillFileBytes } from '/pkg/pondra_web.js';
await init();
try {
  const page = new Page({ memory_max_gb: 1, memory_gb: 0.1 });
  for (const [name, sql] of ${JSON.stringify(WORKER_QUERIES.map(([name, sql]) => [name, sql]))}) {
    const before = spilledBytes();
    const started = performance.now();
    const a = await page.request('POST', '/sql?format=csv', {}, sql);
    const ms = Number((performance.now() - started).toFixed(1));
    postMessage({ name, status: a.status, headers: a.headers, text: new TextDecoder().decode(a.body), ms, spilled: spilledBytes() - before, file: spillFileBytes() });
  }
  for (const [name, sql] of ${JSON.stringify(WORKER_QUERIES.filter(([name]) => name === 'sort' || name === 'join').map(([name, sql]) => [name, sql]))}) {
    const a = await page.request('POST', '/sql?format=csv', {}, 'EXPLAIN ' + sql);
    postMessage({ plan: name, text: new TextDecoder().decode(a.body) });
  }
  await page.close();
  page.free();
  const names = [];
  try {
    const dir = await (await navigator.storage.getDirectory()).getDirectoryHandle('.pondra-spill');
    for await (const [name, handle] of dir.entries()) names.push(handle.kind + ':' + name);
  } catch (e) {
    // (no folder: none was made, or it is gone)
  }
  postMessage({ done: true, folder: names, file: spillFileBytes(), spilled: spilledBytes() });
} catch (e) {
  postMessage({ error: String(e) });
}
`;

// The queries a page in a Worker runs at its default line, with no setting of its own (`fullLineChecks`), each with its
// answer: a sort of 3 GB of rows (past its share: it spills), two aggregates and two joins whose tables the share holds, and a
// 200 MB answer. Its query share is its line's trim line, as a node's is (`env::page_line_share`, audit 11-08 L4): what the
// pool doesn't count (the decoding, the batches in flight, the answer before it is copied out) must still fit under the
// module's 4 GiB, which a wasm32 memory can't pass.
const FULL_LINE_QUERIES = [
  ['sort', "SELECT value, length(p) AS n FROM (SELECT value, p FROM (SELECT value, repeat('x', 200) AS p FROM range(15000000)) ORDER BY value DESC OFFSET 14999999)", 'value,n\n0,200\n'],
  ['aggregate', "SELECT count(*) AS n FROM (SELECT value % 30000000 AS k, min(repeat('y', 40) || value) AS s FROM range(30000000) GROUP BY 1)", 'n\n30000000\n'],
  ['join', "SELECT count(*) AS n FROM (SELECT value * 7919 % 12000000 * 1000003 AS k, repeat('j', 100) AS p FROM range(12000000)) a JOIN (SELECT value * 7919 % 12000000 * 1000003 AS k FROM range(12000000)) b ON a.k = b.k", 'n\n12000000\n'],
  ['aggregate_big', "SELECT count(*) AS n FROM (SELECT value % 50000000 AS k, min(repeat('y', 40) || value) AS s FROM range(50000000) GROUP BY 1)", 'n\n50000000\n'],
  ['join_big', "SELECT count(*) AS n FROM (SELECT value * 7919 % 20000000 * 1000003 AS k, repeat('j', 100) AS p FROM range(20000000)) a JOIN (SELECT value * 7919 % 20000000 * 1000003 AS k FROM range(20000000)) b ON a.k = b.k", 'n\n20000000\n'],
  ['answer', "SELECT count(*) AS n FROM range(1)", 'n\n1\n'],
];
// The most of its 4 GiB the module may reach in them: past it, one more step that grows the memory could trap the page.
const FULL_LINE_PEAK_MB = 3840;
// The Worker's page (`/full-line-worker.js`): its queries in order, each posted with its ms, the bytes it spilled and the
// module's memory after it (a wasm32 memory never shrinks, so it is the peak so far), then the size of a 200 MB answer.
const FULL_LINE_WORKER = `
import init, { Page, spilledBytes } from '/pkg/pondra_web.js';
const wasm = await init();
try {
  const page = new Page({});
  for (const [name, sql] of ${JSON.stringify(FULL_LINE_QUERIES.map(([name, sql]) => [name, sql]))}) {
    const before = spilledBytes();
    const started = performance.now();
    const big = name === 'answer'; // (its answer is the 200 MB one: the count after it says the page still answers)
    if (big) {
      const a = await page.request('POST', '/sql?format=csv', {}, "SELECT value, repeat('z', 100) AS p FROM range(2000000)");
      if (a.status !== 200) throw new Error('the 200 MB answer: ' + a.status + ' ' + new TextDecoder().decode(a.body).slice(0, 300));
      postMessage({ answer_mb: Math.round(a.body.length / 2 ** 20) });
    }
    const a = await page.request('POST', '/sql?format=csv', {}, sql);
    const ms = Number((performance.now() - started).toFixed(1));
    postMessage({ name, status: a.status, headers: a.headers, text: new TextDecoder().decode(a.body), ms, spilled: spilledBytes() - before, memory: wasm.memory.buffer.byteLength });
  }
  await page.close();
  page.free();
  postMessage({ done: true });
} catch (e) {
  postMessage({ error: String(e) });
}
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
  const url = new URL(req.url, 'http://127.0.0.1');
  const { pathname } = url;
  if (pathname === '/') return send(res, 200, 'text/html; charset=utf-8', url.searchParams.has('bare') ? PAGE_BARE : PAGE);
  if (pathname === '/demo/' || pathname === '/demo/index.html') {
    return send(res, 200, 'text/html; charset=utf-8', await readFile(DEMO, 'utf8'));
  }
  if (pathname === '/spill-worker.js') return send(res, 200, MODULE_TYPES['.js'], SPILL_WORKER);
  if (pathname === '/full-line-worker.js') return send(res, 200, MODULE_TYPES['.js'], FULL_LINE_WORKER);
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

// Waits `ms` milliseconds.
function pause(ms) {
  return new Promise((wake) => setTimeout(wake, ms));
}

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
    const allowed = Array.isArray(count) ? count : [count]; // (a list: any of them)
    if (!allowed.includes(object?.rows)) problems.push(`${name} rows ${object?.rows}`);
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

// One page to a module: a second `new Page()` while one lives is refused by its words (55006), and a page is made once the
// first is freed. Checked where the module has its one page (`target.oneAtATime`), in Node and in Chromium alike.
async function onePageCheck(target) {
  let result;
  try {
    result = await target.oneAtATime();
  } catch (e) {
    failed.push('one page to a module');
    console.error(`one page to a module: threw ${e}`);
    return;
  }
  const { refused, reopened } = result;
  if (refused?.includes('a page is open in this module already') && reopened === true) passed++;
  else {
    failed.push('one page to a module');
    console.error(`one page to a module: refused ${refused ?? 'not refused'}, reopened ${reopened}`);
  }
}

// A tab's own file, a blob made in the page's own context (`URL.createObjectURL`), read by a page with no node: its rows are
// counted by the page, a bare name is refused with the readers' words, and a revoked blob is refused with its own words (the
// page's fetch of a blob is the tab's, and a blob is read only by the tab that made it, until it is revoked: `blob_with`).
const BLOB_CSV = 'id,name,amount\n1,ann,2.5\n2,bo,4\n3,cy,0.5\n';
async function blobChecks(target) {
  let url;
  try {
    url = await target.blob('pondra', BLOB_CSV);
  } catch (e) {
    failed.push('a blob made in the page');
    console.error(`a blob made in the page: threw ${e}`);
    return;
  }
  const counted = await ask('blob: count and sum of read_csv(blob)', 'POST', '/sql?format=csv', undefined, `SELECT count(*) AS n, CAST(sum(amount) AS BIGINT) AS total FROM read_csv('${url}')`, 'pondra');
  if (counted) judge('blob: count and sum of read_csv(blob)', counted, { status: 200, sqlstate: null, out: 'n,total\n3,7\n' });
  const bare = await ask('blob: a bare name is refused', 'POST', '/sql', undefined, `SELECT count(*) FROM '${url}'`, 'pondra');
  if (bare) judge('blob: a bare name is refused', bare, { error: true, sqlstate: '22023', says: "read the tab's file with read_csv" });
  await target.revoke('pondra', url);
  const revoked = await ask('blob: a revoked blob is refused', 'POST', '/sql', undefined, `SELECT count(*) FROM read_csv('${url}')`, 'pondra');
  if (revoked) judge('blob: a revoked blob is refused', revoked, { error: true, says: 'until it is revoked' });
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

// A page's query past its memory line is stopped (SQLSTATE 53200), and the page answers the next query. The settings are fixed
// once per module, so this runs in a child process of its own (`inChild`), given `memory_max_gb` 0.1 and a query pool
// (`memory_gb`) above it, so the watch stops the query, not the pool. Its first query grows the page past the line
// (array_agg over 50 M values, between whose batches the page's query yields).
async function memoryChecks() {
  const answers = await inChild('memory', '{ memory_max_gb: 0.1, memory_gb: 1 }', `
    const past = await answer('SELECT array_agg(value) AS a FROM range(50000000)');
    const after = await answer('SELECT 1 AS n');
    return { past, after };
  `);
  if (!answers) return;
  const before = failed.length;
  judge('memory: a query past its line', asJudged(answers.past), { status: 503, sqlstate: '53200', says: 'memory_max_gb' });
  // (the next query is checked without a judge of its own: the memory check is one check)
  if (answers.after.status !== 200 || answers.after.text !== 'n\n1\n') {
    failed.push('memory: the next query');
    console.error(`memory: the next query answered ${answers.after.status} ${JSON.stringify(answers.after.text)}`);
  }
  if (failed.length === before) report.memory = 'a query past its 0.1 GB line stopped with 53200, and the next one answered';
}

// A page's query past its query pool (`memory_gb`) with no folder to spill to is refused, not a trap of the module: a full sort of
// 20 M rows past a 0.1 GB pool (under a 1 GB line, so the pool runs out first) answers 53200 naming the page's setting, and the
// page answers its next query. In a child process of its own, as `memoryChecks`: `inChild` runs Node in either form, so this runs in
// Node only (the Chromium run has no check of its own for it).
async function spillChecks() {
  const answers = await inChild('spill', '{ memory_max_gb: 1, memory_gb: 0.1 }', `
    const past = await answer('SELECT value FROM range(20000000) ORDER BY random() OFFSET 19999999'); // (no LIMIT: with one it is a top-k, which holds its rows)
    const after = await answer('SELECT 1 AS n');
    return { past, after };
  `);
  if (!answers) return;
  const before = failed.length;
  judge('spill: a sort past its pool', asJudged(answers.past), { status: 503, sqlstate: '53200', says: 'memory_gb' });
  if (answers.after.status !== 200 || answers.after.text !== 'n\n1\n') {
    failed.push('spill: the next query');
    console.error(`spill: the next query answered ${answers.after.status} ${JSON.stringify(answers.after.text)}`);
  }
  if (failed.length === before) report.spill = 'a sort past its 0.1 GB pool with no folder to spill to stopped with 53200 naming memory_gb, and the next query answered';
}

// A page in a dedicated Worker spills past its pool to the browser's private storage (designs/l2-page-spill.md, S1): the sort,
// the aggregate and the join that the pool can't hold run in the Worker with their answers, each with the bytes it spilled, and
// the page answers its next query. The same sort on the main thread (a bare tab of its own: the module has its settings once
// per module) is refused 53200, its words naming the Worker. Chromium only, in a persistent context of its own: an incognito
// context may keep the origin's storage in memory under a small quota, so the check could pass without touching the disk.
const WORKER_LIMIT_MS = 300_000; // (a query's time limit: a trapped Worker answers nothing, and would hang the check)
async function spillWorkerChecks() {
  const before = failed.length;
  let chromium;
  try {
    chromium = await playwrightChromium();
  } catch (e) {
    failed.push('spill worker');
    console.error(`spill worker: ${e}`);
    return;
  }
  const site = await serveSite();
  const origin = `http://127.0.0.1:${site.port}`;
  const profile = await mkdtemp(join(tmpdir(), 'pondra-web-spill-'));
  let context = null;
  try {
    context = await chromium.launchPersistentContext(profile, { headless: true });
    const tab = await context.newPage();
    watch(tab);
    const messages = []; // (the Worker's posts, as the tab passes them here)
    await tab.exposeFunction('spillReport', (message) => {
      messages.push(message);
    });
    await tab.goto(`${origin}/?bare`, { waitUntil: 'commit' });
    await untilStarted(tab);
    const estimate = await tab.evaluate(storageEstimate);
    report.spill_worker = { estimate };

    // The main thread: the same settings, its sort refused and the next query answered. A spill file a crashed Worker left is
    // removed by this page's open, though the page keeps no spill file of its own (11-02 audit L7).
    const main = await tab.evaluate(async (sql) => {
      const folder = await (await navigator.storage.getDirectory()).getDirectoryHandle('.pondra-spill', { create: true });
      await folder.getFileHandle('left-by-a-crashed-worker', { create: true });
      const page = new window.Page({ memory_max_gb: 1, memory_gb: 0.1 });
      const ask = async (text) => {
        const a = await page.request('POST', '/sql?format=csv', {}, text);
        return { status: a.status, headers: a.headers, text: new TextDecoder().decode(a.body) };
      };
      const past = await ask(sql);
      const after = await ask('SELECT 1 AS n');
      await page.close();
      page.free();
      // (the sweep runs in the background of the page's open: the folder is read again for up to 5 s until it is gone)
      let left = [];
      for (const until = Date.now() + 5000; ; await new Promise((r) => setTimeout(r, 100))) {
        left = [];
        for await (const [name] of folder.entries()) left.push(name);
        if (left.length === 0 || Date.now() > until) break;
      }
      return { past, after, left };
    }, WORKER_QUERIES[0][1]);
    if (main.left.length === 0) passed++;
    else {
      failed.push("spill worker: the main thread's page removes a crashed Worker's spill file");
      console.error(`spill worker: the folder still holds ${JSON.stringify(main.left)} after the main thread's page opened`);
    }
    judge('spill worker: a sort past its pool on the main thread', asJudged(main.past), { status: 503, sqlstate: '53200', says: 'Worker' });
    judge("spill worker: the main thread's next query", asJudged(main.after), { status: 200, sqlstate: null, out: 'n\n1\n' });
    report.spill_worker.main_thread = main.past.text;

    // The Worker: its queries one at a time, each answered within its time limit, then its EXPLAINs and its close.
    await tab.evaluate(() => {
      const worker = new Worker('/spill-worker.js', { type: 'module' });
      worker.onmessage = (e) => window.spillReport(e.data);
      worker.onerror = (e) => window.spillReport({ error: e.message || 'the worker failed' });
      worker.onmessageerror = () => window.spillReport({ error: 'a message from the worker could not be read' });
    });
    const times = {};
    const plans = {};
    let finished = null;
    for (const [name, , want] of WORKER_QUERIES) {
      const m = await nextMessage(messages);
      if (!m || m.error || m.name !== name) {
        failed.push(`spill worker: ${name}`);
        console.error(`spill worker: ${name} answered ${m ? JSON.stringify(m.error ?? m.name) : 'nothing within 5 min'}`);
        break;
      }
      judge(`spill worker: ${name}`, asJudged(m), { status: 200, sqlstate: null, out: want });
      if (name !== 'one' && !(m.spilled > 0)) {
        failed.push(`spill worker: ${name} spilled nothing`);
        console.error(`spill worker: ${name} spilled nothing (spilledBytes did not move)`);
      }
      times[name] = { ms: m.ms, spilled_mb: Number((m.spilled / 2 ** 20).toFixed(1)) };
      if (name === 'one') same('spill worker: spillFileBytes() after the last query', m.file, 0);
    }
    if (failed.length === before) {
      for (const name of ['sort', 'join']) {
        const m = await nextMessage(messages);
        if (!m || m.error || m.plan !== name) {
          failed.push(`spill worker: EXPLAIN ${name}`);
          break;
        }
        plans[name] = m.text.split('\n').filter((line) => line.includes('SortExec') || line.includes('HashJoinExec'));
      }
      const sortPlan = plans.sort ?? [];
      if (!(sortPlan.length > 0 && sortPlan.every((line) => !/TopK|fetch=\d/.test(line)))) {
        failed.push('spill worker: EXPLAIN of the sort shows a SortExec with no fetch');
        console.error(`spill worker: EXPLAIN of the sort: ${JSON.stringify(sortPlan)}`);
      }
      if (!(plans.join ?? []).some((line) => line.includes('HashJoinExec'))) {
        failed.push('spill worker: EXPLAIN of the join shows a HashJoinExec');
        console.error(`spill worker: EXPLAIN of the join: ${JSON.stringify(plans.join ?? [])}`);
      }
      finished = await nextMessage(messages);
      if (!finished || finished.error || !finished.done) {
        failed.push('spill worker: the page closed');
        console.error(`spill worker: after its queries: ${JSON.stringify(finished?.error ?? finished ?? 'nothing within 5 min')}`);
      } else {
        same('spill worker: the .pondra-spill folder after close holds no file', JSON.stringify(finished.folder), '[]');
      }
    }
    report.spill_worker = { ...report.spill_worker, queries: times, plans, folder_after_close: finished?.folder ?? null };
    if (failed.length === before) report.spill_worker.run = `a page in a Worker spilled past its 0.1 GB pool and answered each query`;
  } catch (e) {
    failed.push('spill worker');
    console.error(`spill worker: threw ${e}`);
  } finally {
    await context?.close();
    await site.close();
    await rm(profile, { recursive: true, force: true });
  }
}

// A page in a Worker at its default line, with no memory setting of its own: each query past or near its share answers, and
// the module's memory peaks under FULL_LINE_PEAK_MB of its 4 GiB (audit 11-08 L4: the share is its line's trim line, as a
// node's is). Chromium only (a Worker with the browser's storage to spill to); it runs about a minute.
async function fullLineChecks() {
  const before = failed.length;
  let chromium;
  try {
    chromium = await playwrightChromium();
  } catch (e) {
    failed.push('full line');
    console.error(`full line: ${e}`);
    return;
  }
  const site = await serveSite();
  const profile = await mkdtemp(join(tmpdir(), 'pondra-web-full-line-'));
  let context = null;
  try {
    context = await chromium.launchPersistentContext(profile, { headless: true });
    const tab = await context.newPage();
    watch(tab);
    const messages = [];
    await tab.exposeFunction('fullLineReport', (message) => {
      messages.push(message);
    });
    await tab.goto(`http://127.0.0.1:${site.port}/?bare`, { waitUntil: 'commit' });
    await untilStarted(tab);
    const device_gb = await tab.evaluate(() => navigator.deviceMemory ?? null);
    await tab.evaluate(() => {
      const worker = new Worker('/full-line-worker.js', { type: 'module' });
      worker.onmessage = (e) => window.fullLineReport(e.data);
      worker.onerror = (e) => window.fullLineReport({ error: e.message || 'the worker failed' });
    });
    const queries = {};
    let peak = 0;
    let answer_mb = null;
    for (const [name, , want] of FULL_LINE_QUERIES) {
      let m = await nextMessage(messages);
      if (m && m.answer_mb !== undefined) {
        answer_mb = m.answer_mb;
        m = await nextMessage(messages);
      }
      if (!m || m.error || m.name !== name) {
        failed.push(`full line: ${name}`);
        console.error(`full line: ${name} answered ${m ? JSON.stringify(m.error ?? m.name) : 'nothing within 5 min'}`);
        break;
      }
      judge(`full line: ${name}`, asJudged(m), { status: 200, sqlstate: null, out: want });
      peak = Math.max(peak, m.memory);
      queries[name] = { ms: m.ms, spilled_mb: Math.round(m.spilled / 2 ** 20), memory_mb: Math.round(m.memory / 2 ** 20) };
    }
    if (!(queries.sort?.spilled_mb > 0)) {
      failed.push('full line: the sort spilled past its share');
      console.error(`full line: the sort spilled nothing: ${JSON.stringify(queries.sort ?? null)}`);
    }
    const peak_mb = Math.round(peak / 2 ** 20);
    if (peak_mb > 0 && peak_mb <= FULL_LINE_PEAK_MB) passed++;
    else {
      failed.push(`full line: the module's memory peaks under ${FULL_LINE_PEAK_MB} MB`);
      console.error(`full line: the module's memory peaked at ${peak_mb} MB, past ${FULL_LINE_PEAK_MB} MB of its 4096`);
    }
    report.full_line = { device_gb, queries, answer_mb, peak_mb };
    if (failed.length === before) report.full_line.run = `a page in a Worker at its default line answered each query, its memory peaking at ${peak_mb} MB of 4096`;
  } catch (e) {
    failed.push('full line');
    console.error(`full line: threw ${e}`);
  } finally {
    await context?.close();
    await site.close();
    await rm(profile, { recursive: true, force: true });
  }
}

// `navigator.storage.estimate()` of the tab's origin, in MB: the room a page's spill file is capped by (half of the free part).
async function storageEstimate() {
  const { quota, usage } = await navigator.storage.estimate();
  return { quota_mb: Math.round(quota / 2 ** 20), usage_mb: Math.round(usage / 2 ** 20) };
}

// The Worker's next post, or null after WORKER_LIMIT_MS: a post comes within the limit, or the query has hung.
async function nextMessage(messages) {
  const until = Date.now() + WORKER_LIMIT_MS;
  while (messages.length === 0) {
    if (Date.now() > until) return null;
    await pause(20);
  }
  return messages.shift();
}

// A node's answer past the room a page has for it is not read (the node's `big` is a view of 20 M rows, 160 MB as Arrow and
// more as CSV, past a 0.1 GB line): a query's answer and a copy's rows are each 53200, naming the room, and the next small
// answer is read. In a child process of its own, as `memoryChecks`.
async function nodeMemoryChecks(node) {
  const before = failed.length;
  const made = await ask('node: big', 'POST', '/sql', undefined, 'CREATE VIEW big AS SELECT value AS v FROM range(20000000)', undefined, node);
  if (made) judge('node: big', made, { status: 200, sqlstate: null });
  if (!made || failed.length > before) return;
  const answers = await inChild('node memory', `{ node: ${JSON.stringify(node.url)}, memory_max_gb: 0.1, memory_gb: 1 }`, `
    const query = await answer('SELECT v FROM big');
    const copy = await answer('CREATE TABLE mine AS SELECT * FROM big');
    const after = await answer('SELECT count(*) AS n FROM big');
    return { query, copy, after };
  `);
  if (!answers) return;
  const past = { status: 503, sqlstate: '53200', says: 'room for' };
  judge('node memory: a query past the room', asJudged(answers.query), past);
  judge('node memory: a copy past the room', asJudged(answers.copy), past);
  judge('node memory: the next answer', asJudged(answers.after), { status: 200, sqlstate: null, out: 'n\n20000000\n' });
  if (failed.length === before) report.node_memory = "a node's answer past the page's room stopped with 53200, and the next one was read";
}

// A page made with the options `opts` (JavaScript text) in a child process of its own, as the settings are fixed once per
// module: `body` runs with `answer(sql)` (POST /sql as CSV: its status, headers and text) and returns what the child
// answers, as JSON. The child is killed after 60 s, however it ends. Null, after naming the failure under `name`, when the
// child doesn't run or answer.
async function inChild(name, opts, body) {
  const module = pathToFileURL(join(pkg, 'pondra_web.js')).href;
  const code = `
    import { readFile } from 'node:fs/promises';
    const { default: init, Page } = await import(${JSON.stringify(module)});
    await init({ module_or_path: await readFile(${JSON.stringify(join(pkg, 'pondra_web_bg.wasm'))}) });
    const page = new Page(${opts});
    const answer = async (sql) => {
      const a = await page.request('POST', '/sql?format=csv', {}, sql);
      return { status: a.status, headers: a.headers, text: new TextDecoder().decode(a.body) };
    };
    process.stdout.write(JSON.stringify(await (async () => { ${body} })()));
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', code], { stdio: ['ignore', 'pipe', 'pipe'] });
  let out = '';
  let errors = '';
  child.stdout.on('data', (chunk) => {
    out += chunk;
  });
  child.stderr.on('data', (chunk) => {
    errors = (errors + chunk).slice(-2048);
  });
  const timer = setTimeout(() => {
    if (child.exitCode === null && child.signalCode === null) process.kill(child.pid, 'SIGKILL');
  }, 60_000);
  try {
    await once(child, 'exit');
  } catch (e) {
    failed.push(name);
    console.error(`${name}: the page's child did not run: ${e}`);
    return null;
  } finally {
    clearTimeout(timer);
  }
  try {
    return JSON.parse(out);
  } catch {
    failed.push(name);
    console.error(`${name}: the page's child did not answer: ${JSON.stringify(out)} ${errors}`);
    return null;
  }
}

// A child's answer (`inChild`) as `judge` reads one.
function asJudged(a) {
  return { status: a.status, headers: a.headers, body: enc.encode(a.text) };
}

// The kept page in Chromium (`Page.kept`), in `page`, a tab on the bare page (`tab('kept')`): its files and tables are kept in
// the origin's private file system, so they are there after a reload, and a large file is read by ranges, not whole. A
// reload runs the tab's script again, and `window.kept` is made again from its folder.
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
    ['kept: CREATE TABLE "a-b"', 'CREATE TABLE "a-b" AS SELECT 1 AS x'],
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

  // (b2) Two creates of one new file at once: the second finds the first's file, so one answers 200 and the other 409, and
  // the file is the 200's body. The page's lock on the object makes the check and the write one step.
  const raced = ['id\n1\n', 'id\n2\n'];
  const race = await page.evaluate(async (bodies) => {
    const enc = new TextEncoder();
    const outs = await Promise.all(bodies.map((text) => window.kept.request('PUT', '/files/race.csv', {}, enc.encode(text))));
    const got = await window.kept.request('GET', '/files/race.csv', {}, null);
    return { statuses: outs.map((out) => out.status), text: new TextDecoder().decode(got.body) };
  }, raced);
  same('kept: two creates of one file at once', JSON.stringify(race.statuses.slice().sort((a, b) => a - b)), '[200,409]');
  same('kept: the create that answered 200 is the file', race.text, raced[race.statuses.indexOf(200)]);

  // (c) A reload: the page's script runs again, and the kept page is made again from its folder.
  await page.reload({ waitUntil: 'commit' });
  await untilStarted(page);
  if (!(await openKept(page, 'pondra-check'))) return;
  const after = await ask('kept after reload: SELECT * FROM k', 'POST', '/sql?format=csv', undefined, 'SELECT * FROM k ORDER BY id', 'kept');
  if (after && before) judge('kept after reload: SELECT * FROM k', after, { status: 200, sqlstate: null, out: dec.decode(before.body) });
  const letters = await ask('kept after reload: FROM "a-b"', 'POST', '/sql?format=csv', undefined, 'SELECT * FROM "a-b"', 'kept');
  if (letters) judge('kept after reload: FROM "a-b"', letters, { status: 200, sqlstate: null, out: 'x\n1\n' });
  const listed = await ask('kept after reload: GET /objects', 'GET', '/objects', undefined, undefined, 'kept');
  // k's rows (a CREATE of 3 rows, then an INSERT of 1): /objects counts the rows of the lake's files, not the log, so it
  // says 3 while the INSERT's row is in the log, as on a node, and 4 once the reopened page's first tier round (at once on
  // open) has put it in a file. Which one this request sees is a race, so either is right.
  if (listed) {
    judge('kept after reload: GET /objects', listed, {
      status: 200, sqlstate: null, type: JSON_TYPE, json: (j) => objectsProblems(j, ['a-b', 'k'], { k: [3, 4] }),
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
    judge('kept after the drop: GET /objects', droppedList, { status: 200, sqlstate: null, type: JSON_TYPE, json: (j) => objectsProblems(j, ['a-b']) });
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
  // that page is freed the folder opens again. The holder and the second page are tabs of their own, in this context (the
  // same origin's folders and Web Locks, one module each), so the second is refused by the folder's name, not by the
  // module's one page. The folder is one of its own (`pondra-lock`, removed first), and no page a later check uses is freed.
  const origin = new URL(page.url()).origin;
  const holder = await openBare(page.context(), origin);
  const second = await openBare(page.context(), origin);
  await holder.evaluate(async () => {
    try {
      await (await navigator.storage.getDirectory()).removeEntry('pondra-lock', { recursive: true });
    } catch (e) {
      if (e.name !== 'NotFoundError') throw e;
    }
    window.heldLock = await window.Page.kept('pondra-lock');
  });
  const refused = await second.evaluate(() => window.Page.kept('pondra-lock').then(() => null, (e) => String(e)));
  await holder.evaluate(() => window.heldLock.free());
  const reopened = await second.evaluate(() => window.Page.kept('pondra-lock').then((again) => { again.free(); return true; }, (e) => String(e)));
  await holder.close();
  await second.close();
  if (refused?.includes('pondra-lock') && reopened === true) passed++;
  else {
    failed.push("Page.kept('pondra-lock') while another page holds it");
    console.error(`Page.kept('pondra-lock'): refused ${refused ?? 'not refused'}, reopened ${reopened}`);
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
    await pause(100);
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
  // The core answers these (a node does too, forms.json names no page refusal for them), so they are answered, not refused.
  // The external table is ext, not t: the checks below make t, and an external table over a file takes no INSERT.
  ['CREATE EXTERNAL TABLE', 'POST', '/sql', "CREATE EXTERNAL TABLE ext STORED AS CSV LOCATION 'orders.csv'", 200, null, undefined],
  ['AT', 'POST', '/sql', 'FROM orders AT (VERSION => 1)', 200, null, undefined],
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
const droppedExt = await ask('DROP TABLE ext', 'POST', '/sql', undefined, 'DROP TABLE ext');
if (droppedExt) judge('DROP TABLE ext', droppedExt, { status: 200, sqlstate: null });

// A file a client reads back is the bytes it put, with its etag and its content type by name (GET).
const got = await ask('GET /files/orders.csv', 'GET', '/files/orders.csv');
if (got) judge('GET /files/orders.csv', got, { status: 200, sqlstate: null, type: 'text/csv; charset=utf-8', etag: true, out: ORDERS });
const missing = await ask('GET a missing file', 'GET', '/files/nope.csv');
if (missing) judge('GET a missing file', missing, { status: 404, sqlstate: null, type: TEXT, out: 'no such file: files/nope.csv' });
// The versions of a file are answered as a JSON list, one version a row (the core's, as a node answers it).
const versions = await ask('GET ?versions', 'GET', '/files/orders.csv?versions');
if (versions) judge('GET ?versions', versions, { status: 200, sqlstate: null, type: JSON_TYPE });

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
      // Where SQL reads a relative file from: the page's files folder in its own memory store (memory://<id>/files/),
      // or a node's real folder (<folder>/files/). Its exact URL is the store's, so only its last part is checked.
      if (typeof j.files !== 'string' || !j.files.endsWith('/files/')) problems.push(`files ${j.files}`);
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
  // (The page's reads are a GET or HEAD with a Range, as fetch.rs makes them: nothing else crosses the origin.)
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
  // reused.
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

// The node's checks: a node `pondra serve` starts for the run is asked as a page's node is asked (B5): its own tables, a
// page's table joined with them, what a page refuses by name, and a table nobody has. In Chromium a tab on another origin
// is refused too (check 5). The node is stopped however its checks end, so none outlives the run.
async function nodeRun(target) {
  const node = await startNode(target.origin);
  if (!node) {
    report.node = 'left out: the node did not start';
    return;
  }
  try {
    await nodeChecks(target, node);
    if (!IN_BROWSER) await nodeMemoryChecks(node);
  } finally {
    await node.stop();
  }
}

// The node's tables, made before any page asks: `sales` is the node's own, and `page_orders` holds the rows of ORDERS.
const NODE_TABLES = [
  ['sales', 'CREATE TABLE sales AS SELECT * FROM (VALUES (1, 10), (1, 5), (3, 7), (4, 1)) t(id, amount)'],
  ['page_orders', "CREATE TABLE page_orders AS SELECT * FROM (VALUES (1, 'a', 5), (2, 'b', 7), (3, 'c', 9)) t(id, name, qty)"],
];
// The answer the node gives to the join of a table with `sales` (summed per id; a row with no sale is left out): every
// page's join must give the same body.
const JOINED = 'id,name,total\n1,a,15\n3,c,7\n';
function joinSql(table) {
  return `SELECT o.id, o.name, sum(s.amount) AS total FROM ${table} o JOIN sales s ON s.id = o.id GROUP BY o.id, o.name ORDER BY o.id`;
}
// What a page refuses by name, before anything is sent to its node: a write into the node's table (a page's copy of it is
// made from the node's rows instead, and the copy is checked below), and a file joined with it.
const REFUSED = ['INSERT INTO sales VALUES (9, 9)', "SELECT count(*) FROM 'orders.csv' o JOIN sales s ON s.id = o.id"];

// The checks in order. The node's tables are made first, and a step that needs the node's answer is left out when it gave
// none; the report names the join only when every check of the node ran and passed.
async function nodeChecks(target, node) {
  const before = failed.length;
  if (!(await nodeTables(node))) return;
  // (1) The node's own answer to the join of its own tables: the body every page's join must equal.
  const own = await ask('node: its own join', 'POST', '/sql?format=csv', undefined, joinSql('page_orders'), undefined, node);
  if (!own) return;
  const want = judge('node: its own join', own, { status: 200, sqlstate: null, out: JOINED });
  // (2) A page's table joined with the node's: the page sends the node the rows of its `orders`, and gives on the answer.
  await target.withNode('withNode', node.url);
  await makeOrders('withNode');
  const joined = await ask('withNode: orders joined with sales', 'POST', '/sql?format=csv', undefined, joinSql('orders'), 'withNode');
  if (joined) judge('withNode: orders joined with sales', joined, { status: 200, sqlstate: null, out: want });
  // (3) A copy of the node's table, made from the node's rows: the node answers the query, and the copy holds its four rows
  // (the node has no `copy`, so the count is the page's own table). Then what the page can't keep is refused by name.
  const copied = await ask('withNode: CREATE TABLE copy', 'POST', '/sql', undefined, 'CREATE TABLE copy AS SELECT * FROM sales', 'withNode');
  if (copied) judge('withNode: CREATE TABLE copy', copied, { status: 200, sqlstate: null });
  const copies = await ask('withNode: count of the copy', 'POST', '/sql?format=csv', undefined, 'SELECT count(*) AS n FROM copy', 'withNode');
  if (copies) judge('withNode: count of the copy', copies, { status: 200, sqlstate: null, out: 'n\n4\n' });
  for (const text of REFUSED) {
    const refused = await ask(`withNode: ${text}`, 'POST', '/sql', undefined, text, 'withNode');
    if (refused) judge(`withNode: ${text}`, refused, { status: 501, sqlstate: '0A000' });
  }
  // (4) A table nobody has: the node's own answer, 404 with 42P01.
  const nowhere = await ask('withNode: a table nobody has', 'POST', '/sql', undefined, 'SELECT * FROM nowhere', 'withNode');
  if (nowhere) judge('withNode: a table nobody has', nowhere, { status: 404, sqlstate: '42P01' });
  if (IN_BROWSER) await otherOriginCheck(target, node);
  const where = IN_BROWSER ? `from ${target.origin}` : '(Node)';
  if (failed.length === before) report.node = `a page's table joined with the node's ${where}`;
}

// The node's tables, made through its own /sql before any page asks. Each must answer 200, or the checks stop there.
async function nodeTables(node) {
  const before = failed.length;
  for (const [name, text] of NODE_TABLES) {
    const made = await ask(`node: ${name}`, 'POST', '/sql', undefined, text, undefined, node);
    if (made) judge(`node: ${name}`, made, { status: 200, sqlstate: null });
    if (failed.length > before) return false;
  }
  return true;
}

// A page's `orders`, made as a page makes a table: ORDERS put as a file, and the table made from that file by SQL.
async function makeOrders(on, from = target) {
  const put = await ask(`${on}: PUT /files/orders.csv`, 'PUT', '/files/orders.csv', undefined, ORDERS, on, from);
  if (put) judge(`${on}: PUT /files/orders.csv`, put, { status: 200, sqlstate: null, type: JSON_TYPE, etag: true });
  const made = await ask(`${on}: CREATE TABLE orders`, 'POST', '/sql', undefined, "CREATE TABLE orders AS FROM 'orders.csv'", on, from);
  if (made) judge(`${on}: CREATE TABLE orders`, made, { status: 200, sqlstate: null });
}

// Check 5, in Chromium: a tab of the same browser on `localhost` is the site under another origin, which the node doesn't
// name. Its join is refused: 503 with 08001, and the words say the node must allow the page's origin (`web_origins`).
async function otherOriginCheck(target, node) {
  const tab = await target.browser.newPage();
  try {
    watch(tab);
    await tab.goto(`http://localhost:${target.port}/?bare`, { waitUntil: 'commit' });
    await untilStarted(tab);
    await pageWithNode(tab, 'other', node.url);
    const from = { request: (method, path, headers, body, on) => requestInBrowser(tab, method, path, headers, body, on) };
    await makeOrders('other', from);
    const refused = await ask('other: orders joined with sales', 'POST', '/sql?format=csv', undefined, joinSql('orders'), 'other', from);
    if (refused) judge('other: orders joined with sales', refused, { status: 503, sqlstate: '08001', says: 'web_origins' });
  } finally {
    await tab.close();
  }
}

// A's statements while it is open (no user yet): a table and a row, a secret, and the superuser, the lake's first user, which
// closes the node to anyone without a sign-in (`App::open`). The rest go as boss, with Basic (`JOIN_AS_BOSS`).
const JOIN_SETUP = [
  'CREATE TABLE ev (id BIGINT, v VARCHAR)',
  "INSERT INTO ev VALUES (1, 'a')",
  'CREATE TABLE idt (id BIGINT GENERATED ALWAYS AS IDENTITY, v VARCHAR)',
  'CREATE TABLE keyed (k BIGINT PRIMARY KEY, v VARCHAR)',
  "INSERT INTO keyed VALUES (1, 'one')",
  "CREATE SECRET s3x (TYPE s3, KEY_ID 'k', SECRET 'pondra-sealed-j6', SCOPE 's3://nowhere')",
  "CREATE USER boss PASSWORD 'boss-password-1' SUPERUSER",
];
const JOIN_AS_BOSS = [
  "CREATE USER rd PASSWORD 'rd-password-1'",
  'GRANT SELECT ON ev TO rd',
  'CREATE TABLE w4 (id BIGINT, name TEXT, amount DOUBLE, at TIMESTAMP)', // (the blob check's table: a tab's file loaded into it)
  'CREATE TABLE w4big (id BIGINT, name TEXT, amount DOUBLE, at TIMESTAMP)', // (the 100 MB check's table: the joined page loads its CSV into it)
  'CREATE TABLE w4self (id BIGINT, name TEXT, amount DOUBLE, at TIMESTAMP)', // (the same CSV, loaded by B itself: the nodes' own load)
  'CREATE TABLE spans (id BIGINT, d INTERVAL)', // (the staged INTERVAL check's table: staged as its text, cast back by the node)
];
const BOSS_BASIC = `Basic ${Buffer.from('boss:boss-password-1').toString('base64')}`;
// What the page's objects and a node's catalog never name: the secret, its value, and the superuser.
const SECRET_WORDS = ['s3x', 'pondra-sealed-j6', 'boss'];

// The page joined to a follower (J6, `Page.join`): node A on a fresh folder, given JOIN_SETUP; node B on the same folder, a
// follower; the page joins B under an admin's session. Both nodes are stopped, and the folder removed, however the run ends.
// `report.join` is left out when a node didn't start.
async function joinRun(target) {
  const a = await startNode(target.origin);
  if (!a) {
    report.join = 'left out: the node did not start';
    return;
  }
  let b = null;
  try {
    if (!(await joinSetup(a))) return;
    b = await startNode(target.origin, a.folder, { PONDRA_AUDIT: 'read,role,ddl,function' }); // (B's read class is on: the page's own reads are B's audit lines, c6)
    if (!b) {
      report.join = 'left out: the second node did not start';
      return;
    }
    await joinChecks(target, a, b);
  } finally {
    if (b) await b.stop();
    await a.stop();
  }
}

// A's statements (`JOIN_SETUP`, then `JOIN_AS_BOSS` with Basic), each judged; false when one fails, and the run stops there.
async function joinSetup(a) {
  const before = failed.length;
  for (const text of JOIN_SETUP) {
    const made = await ask(`A: ${text}`, 'POST', '/sql', undefined, text, undefined, a);
    if (made) judge(`A: ${text}`, made, { status: 200, sqlstate: null });
    if (failed.length > before) return false;
  }
  for (const text of JOIN_AS_BOSS) {
    const made = await ask(`A: ${text}`, 'POST', '/sql', { Authorization: BOSS_BASIC }, text, undefined, a);
    if (made) judge(`A: ${text}`, made, { status: 200, sqlstate: null });
    if (failed.length > before) return false;
  }
  return true;
}

// The checks on the joined page and on B, in order: B's p50 before the join and after the page is closed (the page is closed
// between the two), the page's reads and writes, what it is never given, and a non-superuser's refusal. Each is judged, and the
// report names the run when all of them pass; the numbers are reported either way.
async function joinChecks(target, a, b) {
  const before = failed.length;
  if (!(await untilClosed(b))) {
    failed.push('B has the lake\'s users');
    console.error('B has the lake\'s users: it did not answer an unsigned /sql 401 within 30 s');
    return;
  }
  const session = await loginAs('B: POST /login as boss', 'boss', 'boss-password-1', b);
  if (session === null) return;
  const p50Before = await p50Of(b, BOSS_BASIC); // (B with no page of its own yet)
  const joined = await target.joinAs('joined', b.url, session);
  if (joined.error !== null) {
    failed.push('Page.join(B, boss)');
    console.error(`Page.join(B, boss): ${joined.error}`);
    return;
  }
  const measured = { fresh_ms: null, p50_before_ms: p50Before, p50_after_ms: null, load_ms: null };

  // (a) The page reads A's table: A's row, as csv.
  const read = await ask('joined: SELECT id, v FROM ev', 'POST', '/sql?format=csv', undefined, 'SELECT id, v FROM ev ORDER BY id', 'joined');
  if (read) judge('joined: SELECT id, v FROM ev', read, { status: 200, sqlstate: null, out: 'id,v\n1,a\n' });

  // (b) Freshness: A's second row, once A has answered it, is counted by the page within 1000 ms (fresh_ms).
  const inserted = await ask("A: INSERT INTO ev VALUES (2, 'b')", 'POST', '/sql', { Authorization: BOSS_BASIC }, "INSERT INTO ev VALUES (2, 'b')", undefined, a);
  if (inserted) {
    judge("A: INSERT INTO ev VALUES (2, 'b')", inserted, { status: 200, sqlstate: null });
    const seen = await untilCounted(target, 'joined', 2, performance.now(), 1000);
    measured.fresh_ms = seen.ms === null ? null : Number(seen.ms.toFixed(1));
    if (seen.ms !== null) passed++;
    else {
      failed.push("joined: A's second row is counted within 1000 ms");
      console.error(`joined: A's second row is counted within 1000 ms: the page counted ${seen.count}`);
    }
  }

  // (c) The page's own write comes back: its next count is 3, and A's is 3 too.
  const wrote = await ask("joined: INSERT INTO ev VALUES (3, 'c')", 'POST', '/sql', undefined, "INSERT INTO ev VALUES (3, 'c')", 'joined');
  if (wrote) judge("joined: INSERT INTO ev VALUES (3, 'c')", wrote, { status: 200, sqlstate: null });
  const counted = await ask('joined: SELECT count(*) FROM ev', 'POST', '/sql?format=csv', undefined, 'SELECT count(*) FROM ev', 'joined');
  if (counted) judge('joined: SELECT count(*) FROM ev after its INSERT', counted, { status: 200, sqlstate: null, out: 'count(*)\n3\n' });
  const aCount = await ask('A: SELECT count(*) FROM ev', 'POST', '/sql?format=csv', { Authorization: BOSS_BASIC }, 'SELECT count(*) FROM ev', undefined, a);
  if (aCount) judge('A: SELECT count(*) FROM ev', aCount, { status: 200, sqlstate: null, out: 'count(*)\n3\n' });

  // (c2) A bulk INSERT … SELECT the page writes itself (its files are put through its node's store, its commit through the node):
  // answered 200, and the page's count and A's are 100003 right after. `load_ms` is the page's time for the statement.
  const bulkSql = "INSERT INTO ev SELECT value + 10, 'r' FROM range(100000)";
  const loadStarted = performance.now();
  const loaded = await ask(`joined: ${bulkSql}`, 'POST', '/sql', undefined, bulkSql, 'joined');
  measured.load_ms = Number((performance.now() - loadStarted).toFixed(1));
  if (loaded) judge(`joined: ${bulkSql}`, loaded, { status: 200, sqlstate: null });
  const counted100k = await ask('joined: SELECT count(*) FROM ev after the bulk INSERT', 'POST', '/sql?format=csv', undefined, 'SELECT count(*) FROM ev', 'joined');
  if (counted100k) judge('joined: SELECT count(*) FROM ev after the bulk INSERT', counted100k, { status: 200, sqlstate: null, out: 'count(*)\n100003\n' });
  const aCount100k = await ask('A: SELECT count(*) FROM ev after the bulk INSERT', 'POST', '/sql?format=csv', { Authorization: BOSS_BASIC }, 'SELECT count(*) FROM ev', undefined, a);
  if (aCount100k) judge('A: SELECT count(*) FROM ev after the bulk INSERT', aCount100k, { status: 200, sqlstate: null, out: 'count(*)\n100003\n' });

  // (c3) The page's other INSERTs, each as the node answers it (brief P-B): an identity table's INSERT with a column list (the node
  // gives the identity), a VALUES INSERT, a RETURNING one (the node's answer), and a key that exists in a keyed table (23505).
  const idSql = "INSERT INTO idt (v) SELECT 'q' FROM range(2)";
  const idIns = await ask(`joined: ${idSql}`, 'POST', '/sql', undefined, idSql, 'joined');
  if (idIns) judge(`joined: ${idSql}`, idIns, { status: 200, sqlstate: null });
  const valSql = "INSERT INTO idt (v) VALUES ('val')";
  const valIns = await ask(`joined: ${valSql}`, 'POST', '/sql', undefined, valSql, 'joined');
  if (valIns) judge(`joined: ${valSql}`, valIns, { status: 200, sqlstate: null });
  const retSql = "INSERT INTO idt (v) SELECT 'ret' RETURNING id";
  const retIns = await ask(`joined: ${retSql}`, 'POST', '/sql', undefined, retSql, 'joined');
  if (retIns) judge(`joined: ${retSql}`, retIns, { status: 200, sqlstate: null });
  const noneSql = "INSERT INTO idt (v) SELECT 'none' WHERE false";
  const noneIns = await ask(`joined: ${noneSql}`, 'POST', '/sql', undefined, noneSql, 'joined');
  if (noneIns) judge(`joined: ${noneSql}`, noneIns, { status: 200, sqlstate: null }); // (no rows: one staged file of none, answered as any INSERT of none)
  const idCount = await ask('joined: SELECT id, v FROM idt ORDER BY id', 'POST', '/sql?format=csv', undefined, 'SELECT id, v FROM idt ORDER BY id', 'joined');
  if (idCount) judge('joined: SELECT id, v FROM idt ORDER BY id', idCount, { status: 200, sqlstate: null, out: 'id,v\n1,q\n2,q\n3,val\n4,ret\n' });
  const keySql = "INSERT INTO keyed (k, v) SELECT 1, 'again'";
  const keyIns = await ask(`joined: ${keySql}`, 'POST', '/sql', undefined, keySql, 'joined');
  if (keyIns) judge(`joined: ${keySql}`, keyIns, { sqlstate: '23505', error: true });
  const keyCount = await ask('joined: SELECT count(*) FROM keyed after the refused key', 'POST', '/sql?format=csv', undefined, 'SELECT count(*) FROM keyed', 'joined');
  if (keyCount) judge('joined: SELECT count(*) FROM keyed after the refused key', keyCount, { status: 200, sqlstate: null, out: 'count(*)\n1\n' });
  // (c3b) An upsert is the node's whole, never staged (11-02 audit M6): ON CONFLICT DO NOTHING keeps the row, OR REPLACE replaces it.
  const upsertSql = "INSERT INTO keyed (k, v) SELECT 1 AS k, 'again' AS v ON CONFLICT (k) DO NOTHING";
  const upserted = await ask(`joined: ${upsertSql}`, 'POST', '/sql', undefined, upsertSql, 'joined');
  if (upserted) judge(`joined: ${upsertSql}`, upserted, { status: 200, sqlstate: null });
  const replaceSql = "INSERT OR REPLACE INTO keyed (k, v) SELECT 1, 'replaced'";
  const replaced = await ask(`joined: ${replaceSql}`, 'POST', '/sql', undefined, replaceSql, 'joined');
  if (replaced) judge(`joined: ${replaceSql}`, replaced, { status: 200, sqlstate: null });
  const upsertedRows = await ask('joined: SELECT k, v FROM keyed after the upserts', 'POST', '/sql?format=csv', undefined, 'SELECT k, v FROM keyed ORDER BY k', 'joined');
  if (upsertedRows) judge('joined: SELECT k, v FROM keyed after the upserts', upsertedRows, { status: 200, sqlstate: null, out: 'k,v\n1,replaced\n' });
  // (c3c) A staged INSERT is the node's INSERT (11-02 audit M7, L6): an INTERVAL is staged as its text and the node casts it back
  // exactly; a width the columns don't take, and a column the table doesn't have, are refused in the words any INSERT says them with.
  const spanSql = "INSERT INTO spans SELECT 1, INTERVAL '1 month -2 days 3.000000004 seconds'";
  const spanIns = await ask(`joined: ${spanSql}`, 'POST', '/sql', undefined, spanSql, 'joined');
  if (spanIns) judge(`joined: ${spanSql}`, spanIns, { status: 200, sqlstate: null });
  const spanRead = "SELECT count(*) AS n FROM spans WHERE d = INTERVAL '1 month -2 days 3.000000004 seconds'";
  const spanRows = await ask(`joined: ${spanRead}`, 'POST', '/sql?format=csv', undefined, spanRead, 'joined');
  if (spanRows) judge(`joined: ${spanRead}`, spanRows, { status: 200, sqlstate: null, out: 'n\n1\n' });
  const wideSql = "INSERT INTO keyed (k) SELECT 2, 'x'";
  const wide = await ask(`joined: ${wideSql}`, 'POST', '/sql', undefined, wideSql, 'joined');
  if (wide) judge(`joined: ${wideSql}`, wide, { sqlstate: '42601', error: true, says: 'INSERT has more expressions than target columns' });
  const nopeSql = "INSERT INTO keyed (k, nope) SELECT 2, 'x'";
  const nope = await ask(`joined: ${nopeSql}`, 'POST', '/sql', undefined, nopeSql, 'joined');
  if (nope) judge(`joined: ${nopeSql}`, nope, { sqlstate: '42703', error: true, says: 'column "nope" of relation "keyed" does not exist' });

  // (c4) A tab's own file (a blob made in the joined page's tab) loaded into a lake table: the page stages its rows and its node
  // writes them, so A's count is right at once; an UPDATE from the same file is refused by the page's words (`BLOB_ON_PAGE`).
  const blobRows = 'id,name,amount,at\n1,ann,2.5,2026-01-01 00:00:00\n2,bo,4,2026-01-02 00:00:00\n3,cy,0.5,2026-01-03 00:00:00\n';
  const blobUrl = await target.blob('joined', blobRows);
  const loadSql = `INSERT INTO w4 SELECT * FROM read_csv('${blobUrl}')`;
  const loadedBlob = await ask(`joined: ${loadSql}`, 'POST', '/sql', undefined, loadSql, 'joined');
  if (loadedBlob) judge(`joined: ${loadSql}`, loadedBlob, { status: 200, sqlstate: null });
  const aBlob = await ask('A: SELECT count(*) FROM w4 right after the blob INSERT', 'POST', '/sql?format=csv', { Authorization: BOSS_BASIC }, 'SELECT count(*) FROM w4', undefined, a);
  if (aBlob) judge('A: SELECT count(*) FROM w4 right after the blob INSERT', aBlob, { status: 200, sqlstate: null, out: 'count(*)\n3\n' });
  const updateSql = `UPDATE w4 SET name = 'x' FROM read_csv('${blobUrl}') AS f WHERE w4.id = f.id`;
  const updated = await ask('joined: UPDATE w4 FROM the blob is refused by the page', 'POST', '/sql', undefined, updateSql, 'joined');
  if (updated) judge('joined: UPDATE w4 FROM the blob is refused by the page', updated, { error: true, sqlstate: '0A000', says: 'is read by the page alone' });
  await target.revoke('joined', blobUrl);

  // (c4b) M8: the page's read of its own lake (w4 is the page's to read: the page runs it there and keeps no records) is written by
  // its node B as the sign-in's: boss's name, the door `page-reported` (what the page says it ran), in B's audit log (its read class is on: `joinRun`) and in
  // pondra.history, each within 5 s. The page's row is a line the page sent, not one its own lake wrote.
  const pageRead = 'SELECT count(*) FROM w4';
  const counted3 = await ask(`joined: ${pageRead}`, 'POST', '/sql?format=csv', undefined, pageRead, 'joined');
  if (counted3) judge(`joined: ${pageRead}`, counted3, { status: 200, sqlstate: null, out: 'count(*)\n3\n' });
  const pageRow = 'boss,page-reported,SELECT count(*) FROM w4';
  if (await untilRow(b, `SELECT "user", door, statement FROM pondra.audit WHERE door = 'page-reported'`, pageRow, 5000)) passed++;
  else {
    failed.push("B: the page's read is in its audit log as boss, door page-reported, within 5 s");
    console.error("B: the page's read is in its audit log as boss, door page-reported, within 5 s: no such line");
  }
  if (await untilRow(b, `SELECT "user", door, statement FROM pondra.history WHERE door = 'page-reported'`, pageRow, 5000)) passed++;
  else {
    failed.push("B: the page's read is in pondra.history as boss, door page-reported, within 5 s");
    console.error("B: the page's read is in pondra.history as boss, door page-reported, within 5 s: no such row");
  }

  // (c5) The 100 MB check (W4b): the joined page loads a generated 100 MB CSV, and the nodes' CPU is measured around it.
  await w4Checks(target, a, b, session, measured);

  // (d) A transaction is refused by name: each statement is the node's own, run on its own.
  const begun = await ask('joined: BEGIN', 'POST', '/sql', undefined, 'BEGIN', 'joined');
  if (begun) judge('joined: BEGIN', begun, { status: 501, sqlstate: '0A000', says: 'a transaction through a client of the node' });

  // (e) Nothing secret reaches the page or the catalog a page reads: no secret, no user, and z/auth has no value.
  const objects = await ask('joined: GET /objects', 'GET', '/objects', undefined, undefined, 'joined');
  if (objects) {
    judge('joined: GET /objects', objects, { status: 200, sqlstate: null, type: JSON_TYPE });
    const text = dec.decode(objects.body);
    same('joined: GET /objects names no secret and no user', JSON.stringify(SECRET_WORDS.filter((w) => text.includes(w))), '[]');
  }
  const zAuth = Buffer.from('z/auth').toString('base64url');
  const kv = await ask('B: GET /cluster/kv?key=z/auth with the session', 'GET', `/cluster/kv?key=${zAuth}`, { Authorization: `Bearer ${session}` }, undefined, undefined, b);
  if (kv) judge('B: GET /cluster/kv?key=z/auth with the session', kv, { status: 200, sqlstate: null, type: JSON_TYPE, json: (j) => (j.value === null ? [] : [`value ${JSON.stringify(j)}`]) });

  // (h) Closing the page leaves B as it was: its p50 after is within 1.5 times its p50 before, plus 2 ms.
  await target.closeAs('joined');
  const p50After = await p50Of(b, BOSS_BASIC);
  measured.p50_after_ms = p50After;
  if (p50Before !== null && p50After !== null && p50After <= p50Before * 1.5 + 2) passed++;
  else {
    failed.push('B: p50 after the page is closed is within 1.5 × its p50 before + 2 ms');
    console.error(`B: p50 after the page is closed is within 1.5 × its p50 before + 2 ms: before ${p50Before} ms, after ${p50After} ms`);
  }

  // (f) A non-superuser can't join, and can't read the node's catalog key either.
  const reader = await loginAs('B: POST /login as rd', 'rd', 'rd-password-1', b);
  if (reader !== null) {
    const readerJoin = await target.joinAs('reader', b.url, reader);
    if (readerJoin.error !== null && readerJoin.error.includes('403')) passed++;
    else {
      failed.push('Page.join(B, rd) is refused, naming 403');
      console.error(`Page.join(B, rd) is refused, naming 403: ${readerJoin.error ?? 'it joined'}`);
    }
    if (readerJoin.error === null) await target.closeAs('reader');
    const readerKv = await ask('B: GET /cluster/kv?key=Yw as rd', 'GET', '/cluster/kv?key=Yw', { Authorization: `Bearer ${reader}` }, undefined, undefined, b);
    if (readerKv) judge('B: GET /cluster/kv?key=Yw as rd', readerKv, { status: 403, sqlstate: null });
  }

  // (g) A token is refused by name, with no request made.
  const token = await target.joinAs('token', b.url, 'pk_x');
  if (token.error !== null && token.error.includes('a ps_ session') && token.asked === 0) passed++;
  else {
    failed.push("Page.join(B, 'pk_x') is refused by name, with no request");
    console.error(`Page.join(B, 'pk_x'): ${token.error ?? 'it joined'}, ${token.asked} requests made`);
  }

  const where = IN_BROWSER ? `from ${target.origin}` : '(Node)';
  report.join = failed.length === before ? { run: `a page joined to a follower of the node on its folder ${where}`, ...measured } : measured;
}

// A blob URL of a generated CSV (`w4Csv`'s parts), made in this process, with the counts the generator gave.
function blobOf({ parts, rows, sum, bytes }) {
  return { url: URL.createObjectURL(new Blob(parts, { type: 'text/csv' })), rows, sum, bytes };
}

// The 100 MB CSV (W4b). One generator: its text runs in Node (`target.generate` calls this function) and in the tab (the same
// text, from `w4Csv.toString()`), so a page's blob and a node's file are the same bytes. It builds its parts as strings of
// about 1 MB, never one 100 MB string, and the bytes stay where they are made: a test that needs them builds them again from
// here. `amount` always has two decimals (inference reads only the first 1000 rows), `at` is a `YYYY-MM-DD HH:MM:SS`
// timestamp. Its text names nothing outside itself: the tab runs it as it is.
function w4Csv() {
  const ROWS = 2500000; // (about 100 MB: 100836392 bytes)
  const PART = 1 << 20;
  const pad = (n) => (n < 10 ? `0${n}` : `${n}`);
  const header = 'id,name,amount,at\n';
  const parts = [];
  let lines = [header];
  let inPart = header.length;
  let bytes = header.length;
  let sum = 0;
  for (let i = 1; i <= ROWS; i++) {
    const amount = (((i * 7919) % 1000000) / 100).toFixed(2);
    const at = `2026-01-${pad(1 + (i % 28))} ${pad(i % 24)}:${pad((i * 7) % 60)}:${pad((i * 13) % 60)}`;
    const line = `${i},n${i % 1000},${amount},${at}\n`;
    lines.push(line);
    inPart += line.length;
    bytes += line.length;
    sum += i;
    if (inPart >= PART) {
      parts.push(lines.join(''));
      lines = [];
      inPart = 0;
    }
  }
  if (lines.length) parts.push(lines.join(''));
  return { parts, rows: ROWS, sum, bytes };
}

// The clock ticks a second that /proc's CPU times count in: `getconf CLK_TCK`, read once, on Linux (null elsewhere, and where
// getconf is missing: the CPU is then left out).
const CLK_TCK = (() => {
  if (process.platform !== 'linux') return null;
  try {
    return Number(execFileSync('getconf', ['CLK_TCK'], { encoding: 'utf8' }).trim());
  } catch {
    return null;
  }
})();

// The CPU the nodes have used so far, in ms, all of them together: each process's utime and stime from /proc/<pid>/stat. The
// fields after the last ')' start at the state (field 3), so utime (field 14) is the 12th of them and stime (field 15) the 13th.
// Null when there is no CPU to read (`CLK_TCK`).
async function nodesCpuMs(...nodes) {
  if (CLK_TCK === null) return null;
  let ticks = 0;
  for (const { pid } of nodes) {
    const stat = await readFile(`/proc/${pid}/stat`, 'utf8');
    const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
    ticks += Number(fields[11]) + Number(fields[12]);
  }
  return (ticks * 1000) / CLK_TCK;
}

// (c5) The 100 MB check (W4b), after (c4): the joined page loads the generated CSV from its tab's blob with one
// `INSERT … SELECT … read_csv('blob:…')` into `w4big`, and A's count and sum are the generator's right after. The nodes' CPU is
// measured around that load, and around the same bytes loaded by B itself (`w4self`: the CSV is PUT to A's files as boss first,
// outside the measure). The page's wasm memory is read before and after. In Chromium a second page's tab is closed mid-load
// (`tabClosedMidLoad`). The numbers go in `measured.w4`; the gate is the CPU ratio, page over self, below 1.
async function w4Checks(target, a, b, session, measured) {
  const w4 = {
    bytes: null, rows: null, page_ms: null, nodes_cpu_ms_page: null, nodes_cpu_ms_self: null, cpu_ratio: null,
    wasm_memory_before: null, wasm_memory_after: null, tab_closed: null,
  };
  measured.w4 = w4;
  const cpuLeft = CLK_TCK === null ? "left out: the nodes' CPU is read from /proc, on Linux only" : null;
  let gen;
  try {
    gen = await target.generate('joined');
  } catch (e) {
    failed.push("the 100 MB CSV is made in the joined page's tab");
    console.error(`the 100 MB CSV is made in the joined page's tab: threw ${e}`);
    return;
  }
  w4.bytes = gen.bytes;
  w4.rows = gen.rows;
  const counts = `n,s\n${gen.rows},${gen.sum}\n`; // (count(*) and sum(id) of the generator's rows, as A's and B's SELECT answers them)

  // The page's load, with the nodes' CPU and the page's wasm memory measured around it.
  const loadName = "joined: INSERT INTO w4big SELECT * FROM read_csv(the tab's 100 MB blob)";
  w4.wasm_memory_before = await target.memoryBytes('joined');
  const cpuBefore = await nodesCpuMs(a, b);
  const loadStarted = performance.now();
  const loaded = await ask(loadName, 'POST', '/sql', undefined, `INSERT INTO w4big SELECT * FROM read_csv('${gen.url}')`, 'joined');
  w4.page_ms = Number((performance.now() - loadStarted).toFixed(1));
  const cpuAfter = await nodesCpuMs(a, b);
  w4.wasm_memory_after = await target.memoryBytes('joined');
  await target.revoke('joined', gen.url);
  if (loaded) judge(loadName, loaded, { status: 200, sqlstate: null });
  if (loaded === null || loaded.status !== 200) return;
  w4.nodes_cpu_ms_page = cpuBefore === null || cpuAfter === null ? cpuLeft : Number((cpuAfter - cpuBefore).toFixed(1));

  // The rows are on the node when the page answers: A's count and sum, right after.
  const afterName = "A: SELECT count(*) AS n, sum(id) AS s FROM w4big right after the page's load";
  const counted = await ask(afterName, 'POST', '/sql?format=csv', { Authorization: BOSS_BASIC }, 'SELECT count(*) AS n, sum(id) AS s FROM w4big', undefined, a);
  if (counted) judge(afterName, counted, { status: 200, sqlstate: null, out: counts });

  // The nodes' own load of the same bytes: built in Node by the same function, PUT to A's files as boss (outside the measure),
  // then loaded by B. The bytes built in Node are as many as the tab's blob.
  const csv = Buffer.concat(w4Csv().parts.map((part) => Buffer.from(part)));
  same("the CSV built in Node has the generator's bytes", csv.length, gen.bytes);
  const putName = 'A: PUT /files/w4big.csv (the same bytes, as boss)';
  const put = await ask(putName, 'PUT', '/files/w4big.csv', { Authorization: BOSS_BASIC }, csv, undefined, a);
  if (put) judge(putName, put, { status: 200, sqlstate: null, type: JSON_TYPE, etag: true });
  if (put === null || put.status !== 200) return;
  const selfName = "B: INSERT INTO w4self SELECT * FROM read_csv('w4big.csv')";
  const cpuSelfBefore = await nodesCpuMs(a, b);
  const selfLoaded = await ask(selfName, 'POST', '/sql', { Authorization: BOSS_BASIC }, "INSERT INTO w4self SELECT * FROM read_csv('w4big.csv')", undefined, b);
  const cpuSelfAfter = await nodesCpuMs(a, b);
  w4.nodes_cpu_ms_self = cpuSelfBefore === null || cpuSelfAfter === null ? cpuLeft : Number((cpuSelfAfter - cpuSelfBefore).toFixed(1));
  if (selfLoaded) judge(selfName, selfLoaded, { status: 200, sqlstate: null });
  const selfCounted = await ask('B: SELECT count(*) AS n, sum(id) AS s FROM w4self', 'POST', '/sql?format=csv', { Authorization: BOSS_BASIC }, 'SELECT count(*) AS n, sum(id) AS s FROM w4self', undefined, b);
  if (selfCounted) judge('B: SELECT count(*) AS n, sum(id) AS s FROM w4self', selfCounted, { status: 200, sqlstate: null, out: counts });

  // The gate: the page's load costs its nodes less CPU than their own load (the ratio below 1). Reported either way.
  if (cpuLeft !== null) w4.cpu_ratio = cpuLeft;
  else {
    w4.cpu_ratio = Number((w4.nodes_cpu_ms_page / w4.nodes_cpu_ms_self).toFixed(3));
    if (w4.cpu_ratio < 1) passed++;
    else {
      failed.push("the page's load costs its nodes less CPU than their own load: the ratio is below 1");
      console.error(`the page's load costs its nodes less CPU than their own load: ratio ${w4.cpu_ratio}`);
    }
  }

  // A tab closed mid-load (Chromium). Node has no tabs to close.
  if (IN_BROWSER) await tabClosedMidLoad(target, a, b, session, w4);
  else w4.tab_closed = "left out: Chromium only (a page's tab is closed by Playwright)";
}

// (c5, Chromium) A second joined page, in a tab of its own, starts `INSERT INTO w4big` from a fresh blob of the generator. Its tab
// is closed at the first request whose URL has `/stage/` (its staged rows), and that request is never continued (a CORS preflight
// is let through). Three seconds later A's count of w4big is what it was before the INSERT. Staged files are not looked for: a
// listing of `stage/` through A's /store is reported as the door answers it, and a refused listing fails nothing.
async function tabClosedMidLoad(target, a, b, session, w4) {
  const closed = { stage_request: null, page_closed: null, count_before: null, count_after: null, insert_ended: null, stage_listing: null };
  w4.tab_closed = closed;
  const joined = await target.joinAs('w4close', b.url, session);
  if (joined.error !== null) {
    failed.push('Page.join(B) in a second tab');
    console.error(`Page.join(B) in a second tab: ${joined.error}`);
    return;
  }
  const made = target.tabOf('w4close');
  let blob;
  try {
    blob = await target.generate('w4close');
  } catch (e) {
    failed.push("the 100 MB CSV is made in the second page's tab");
    console.error(`the 100 MB CSV is made in the second page's tab: threw ${e}`);
    return;
  }
  const countSql = 'SELECT count(*) AS n FROM w4big';
  const before = await ask('A: SELECT count(*) AS n FROM w4big before the second INSERT', 'POST', '/sql?format=csv', { Authorization: BOSS_BASIC }, countSql, undefined, a);
  if (before) judge('A: SELECT count(*) AS n FROM w4big before the second INSERT', before, { status: 200, sqlstate: null });
  if (before === null || before.status !== 200) return;
  closed.count_before = dec.decode(before.body);

  // The first /stage/ request of the page closes its tab; the request is left unanswered, so nothing of it goes on.
  let stage = null;
  await made.route((url) => url.href.includes('/stage/'), async (route) => {
    const request = route.request();
    if (request.method() === 'OPTIONS') return route.continue();
    if (stage !== null) return;
    stage = `${request.method()} ${new URL(request.url()).pathname}`;
    await made.close();
  });
  const insertSql = `INSERT INTO w4big SELECT * FROM read_csv('${blob.url}')`;
  const ended = requestInBrowser(made, 'POST', '/sql', undefined, insertSql, 'w4close').then(
    (answer) => `answered ${answer.status}`,
    (e) => `ended with the tab: ${String(e).split('\n')[0]}`,
  );
  const until = Date.now() + 120_000;
  while (stage === null && Date.now() < until) await pause(50);
  if (stage === null) {
    failed.push('w4close: the page made no /stage/ request within 120 s');
    console.error('w4close: the page made no /stage/ request within 120 s');
    if (!made.isClosed()) await made.close();
    closed.insert_ended = await ended;
    return;
  }
  const closeBy = Date.now() + 10_000;
  while (!made.isClosed() && Date.now() < closeBy) await pause(50);
  closed.stage_request = stage;
  closed.page_closed = made.isClosed();
  same('w4close: its tab is closed at that request', closed.page_closed, true);

  await pause(3000);
  const afterName = 'A: SELECT count(*) AS n FROM w4big 3 s after the tab was closed';
  const after = await ask(afterName, 'POST', '/sql?format=csv', { Authorization: BOSS_BASIC }, countSql, undefined, a);
  if (after) judge(afterName, after, { status: 200, sqlstate: null, out: closed.count_before });
  closed.count_after = after ? dec.decode(after.body) : null;
  closed.insert_ended = await ended;

  // A's listing of `stage/` through /store as boss: reported, not judged.
  closed.stage_listing = await a.request('GET', '/store/?prefix=stage/', { Authorization: BOSS_BASIC }, undefined).then(
    (answer) => ({ status: answer.status, text: dec.decode(answer.body).slice(0, 200) }),
    (e) => ({ error: String(e) }),
  );
}

// Waits until B answers an unsigned `/sql` 401: a node with the lake's users (an open one answers 200). The request waits for
// B to catch up with A (`guard`), so a sign-in after this is not refused for a user B hasn't replayed yet. Up to 30 s.
async function untilClosed(node) {
  const until = Date.now() + 30_000;
  while (Date.now() < until) {
    const answer = await node.request('POST', '/sql', undefined, 'SELECT 1').catch(() => null);
    if (answer?.status === 401) return true;
    await pause(100);
  }
  return false;
}

// A user's session at the node `from` (`POST /login`): its `ps_` token, judged as one; null when the answer has none.
async function loginAs(name, user, password, from) {
  const answer = await ask(name, 'POST', '/login', { 'content-type': JSON_TYPE }, JSON.stringify({ user, password }), undefined, from);
  if (!answer) return null;
  judge(name, answer, { status: 200, sqlstate: null, json: (j) => (String(j.token).startsWith('ps_') ? [] : [`body ${JSON.stringify(j)}`]) });
  try {
    return JSON.parse(dec.decode(answer.body)).token ?? null;
  } catch {
    return null;
  }
}

// B's p50 of 40 `POST /sql SELECT count(*) FROM ev` (Basic), in ms: the mean of the 20th and 21st of the 40 times, sorted. Each
// answer must be 200 (one check). Null when a request failed (`ask` names it).
async function p50Of(node, basic) {
  const times = [];
  const bad = [];
  for (let i = 0; i < 40; i++) {
    const started = performance.now();
    const answer = await ask('B: SELECT count(*) FROM ev (p50)', 'POST', '/sql', { Authorization: basic }, 'SELECT count(*) FROM ev', undefined, node);
    if (!answer) return null;
    times.push(performance.now() - started);
    if (answer.status !== 200) bad.push(answer.status);
  }
  same('B: its 40 counts answer 200', JSON.stringify(bad), '[]');
  times.sort((x, y) => x - y);
  return Number(((times[19] + times[20]) / 2).toFixed(2));
}

// Polls a node's answer to `sql` as boss (CSV) every 50 ms, for up to `within` ms: true once one of its lines is `row`, else false
// (the answer is not a 200, or the row came in no time).
async function untilRow(node, sql, row, within) {
  const from = performance.now();
  for (;;) {
    const answer = await ask(`B: ${sql} (poll)`, 'POST', '/sql?format=csv', { Authorization: BOSS_BASIC }, sql, undefined, node);
    if (!answer || answer.status !== 200) return false;
    if (dec.decode(answer.body).split('\n').includes(row)) return true;
    if (performance.now() - from > within) return false;
    await pause(50);
  }
}

// Polls the page's count of `ev` every 20 ms until it is `want`, for up to `within` ms from `from`: where it was seen (ms from
// `from`, null when it wasn't in time) and the last count the page answered.
async function untilCounted(target, on, want, from, within) {
  let count = null;
  for (;;) {
    const answer = await ask(`${on}: SELECT count(*) FROM ev (poll)`, 'POST', '/sql?format=csv', undefined, 'SELECT count(*) FROM ev', on, target);
    if (!answer) return { ms: null, count };
    count = Number(dec.decode(answer.body).split('\n')[1]);
    const ms = performance.now() - from;
    if (count === want) return { ms, count };
    if (ms > within) return { ms: null, count };
    await pause(20);
  }
}

// A node of the run's own: `pondra serve` on a folder and a free port. Its origins are those a browser page may call it
// from (`PONDRA_WEB_ORIGINS`), when there are any. On `folder` when one is given (another node's: this node doesn't remove it),
// else on a folder of its own. `env` adds to the node's environment (`joinRun` starts B with its audit classes). Returns
// { url, folder, request, stop }, or null after naming the failure: the node exited, or did not answer `/stats` within 60 s.
// `pid` is the process's, for its CPU (`nodesCpuMs`).
async function startNode(origins, folder, env) {
  const port = await closedPort();
  const own = folder === undefined;
  const dir = own ? await mkdtemp(join(tmpdir(), 'pondra-web-node-')) : folder;
  const removed = own ? dir : null;
  const node = spawnNode(origins, dir, port, env);
  const why = await untilNodeAnswers(node);
  if (why === null) {
    const { url } = node;
    return { url, folder: dir, pid: node.child.pid, request: (method, path, headers, body) => nodeRequest(url, method, path, headers, body), stop: () => endNode(node, removed) };
  }
  await endNode(node, removed);
  failed.push('the node starts');
  console.error(`the node starts: ${why}: ${node.stderr}`);
  return null;
}

// `pondra serve` as a child process, with the run's environment, `extra` added to it, and the origins given. Its stderr's last
// 2 KB are kept, for a failure to name.
function spawnNode(origins, folder, port, extra) {
  const env = { ...process.env, ...(extra ?? {}) };
  if (origins !== undefined) env.PONDRA_WEB_ORIGINS = origins;
  const child = spawn(NODE_BIN, ['serve', '--dir', folder, '--addr', `127.0.0.1:${port}`], { env, stdio: ['ignore', 'ignore', 'pipe'] });
  const node = { url: `http://127.0.0.1:${port}`, child, error: null, stderr: '' };
  child.on('error', (e) => {
    node.error = e.message; // (a binary that is not there: no process starts, and none exits)
  });
  child.stderr.on('data', (chunk) => {
    node.stderr = (node.stderr + chunk).slice(-2048);
  });
  return node;
}

// Waits until the node's `/stats` answers 200 (every 50 ms, for up to 60 s): null when it does, else why it did not, as
// text (the process failed or exited, or the time ran out).
async function untilNodeAnswers(node) {
  const until = Date.now() + 60_000;
  while (Date.now() < until) {
    if (node.error !== null) return node.error;
    if (node.child.exitCode !== null || node.child.signalCode !== null) {
      return `it exited with ${node.child.exitCode ?? node.child.signalCode}`;
    }
    if (await statsAnswers(node.url)) return null;
    await pause(50);
  }
  return 'it did not answer /stats within 60 s';
}

// Whether the node's `/stats` answers 200 now: no answer (refused, or cut off) is not yet.
async function statsAnswers(url) {
  try {
    return (await nodeRequest(url, 'GET', '/stats')).status === 200;
  } catch {
    return false;
  }
}

// A request to the node, with Node's fetch. Node sends no Origin header, so a browser's rule for a page's origin never
// applies to it; the answer is read as `judge` reads one.
async function nodeRequest(url, method, path, headers, body) {
  const answer = await fetch(`${url}${path}`, { method, headers, body });
  return { status: answer.status, headers: Object.fromEntries(answer.headers), body: new Uint8Array(await answer.arrayBuffer()) };
}

// Ends a node: its process, then its folder when it has one to remove (`startNode`'s own; none for another node's).
async function endNode(node, folder) {
  await stopProcess(node.child);
  if (folder) await rm(folder, { recursive: true, force: true });
}

// Ends a process with SIGTERM and waits for its exit. A process that has exited, or never started, is not waited for.
async function stopProcess(child) {
  if (child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, 'exit');
  child.kill('SIGTERM');
  await exited;
}

try {
  await lakeChecks(lake);
  await onePageCheck(target);
  await blobChecks(target);
  if (IN_BROWSER) {
    await keptChecks(await target.tab('kept'), lake);
    await demoChecks(target.page);
    await spillWorkerChecks();
    await fullLineChecks();
  } else {
    await keptRefused(target.Page);
    await memoryChecks();
    await spillChecks();
  }
  if (NODE_BIN) {
    await nodeRun(target);
    await joinRun(target);
  } else {
    report.node = 'left out: no --node-bin';
    report.join = 'left out: no --node-bin';
  }
} finally {
  await target.close();
}
const result = {
  ok: failed.length === 0, checks: passed, failed, open_ms: target.open_ms, series_10m_ms, wasm_bytes: wasm.length,
  browser: target.version, // (absent in Node: JSON leaves out an undefined value)
  ...report,
};
if (sizes) {
  result.gzip_bytes = gzipSync(wasm, { level: 9 }).length;
  result.brotli_bytes = brotliCompressSync(wasm, {
    params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_SIZE_HINT]: wasm.length },
  }).length;
  result.brotli_budget = BROTLI_BUDGET;
  if (result.brotli_bytes > BROTLI_BUDGET) {
    result.ok = false;
    result.failed.push(`the module's brotli is ${result.brotli_bytes} bytes, over its budget of ${BROTLI_BUDGET}`);
  }
}
console.log(JSON.stringify(result));
process.exitCode = result.ok ? 0 : 1;
