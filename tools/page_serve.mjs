#!/usr/bin/env node
// A page (web/, the module in Node) behind an HTTP port, so a check written for a node runs on a page unchanged
// (reviews/form-parity.md §1: one forms check over every form). Each request goes to `Page.request` as it came, and its
// answer (status, headers, body) goes back as the page gave it: the page's contract is the node's (ADR-061).
// `node tools/page_serve.mjs [--pkg web/pkg] [--port 0]`: one page in memory, made afresh by `POST /reset`. The page is a
// member on the core's lake, as every page is (`new Page()`: one page a module, so a reset closes and frees the last first).
// Once it listens it prints one JSON line, `{"url": "http://127.0.0.1:<port>"}` (port 0 takes a free one); it ends on
// SIGTERM or SIGINT, and its tables end with it.
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const args = process.argv.slice(2);
const option = (name, otherwise) => (args.includes(name) ? args[args.indexOf(name) + 1] : otherwise);
const pkg = resolve(option('--pkg', join(dirname(fileURLToPath(import.meta.url)), '../web/pkg')));
const port = Number(option('--port', '0'));

const { default: init, Page } = await import(pathToFileURL(join(pkg, 'pondra_web.js')).href);
await init({ module_or_path: await readFile(join(pkg, 'pondra_web_bg.wasm')) });
let page = new Page();

// One request, answered by the page. A page that throws (a bug, not a refusal: refusals are answers) is a 500 naming it,
// so a runner sees a failure where it would otherwise wait. `POST /reset` is this server's, not the page's: a fresh page
// with nothing in it, so a runner gives each file a page of its own without loading the module again.
async function answer(req, res) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (req.method === 'POST' && req.url === '/reset') {
    try {
      await page.close(); // (close before free: the member ends now, not in the background, and the next page opens on a clean lake)
      page.free();
      page = new Page();
    } catch (e) {
      res.writeHead(500, { 'content-type': 'text/plain' });
      res.end(`the page didn't reset: ${e?.message ?? e}`);
      return;
    }
    res.writeHead(204);
    res.end();
    return;
  }
  try {
    const out = await page.request(req.method, req.url, req.headers, new Uint8Array(Buffer.concat(chunks)));
    res.writeHead(out.status, out.headers);
    res.end(Buffer.from(out.body));
  } catch (e) {
    res.writeHead(500, { 'content-type': 'text/plain' });
    res.end(`the page threw: ${e?.message ?? e}`);
  }
}

const server = createServer((req, res) => void answer(req, res));
server.listen(port, '127.0.0.1', () => {
  console.log(JSON.stringify({ url: `http://127.0.0.1:${server.address().port}` }));
});
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => server.close(() => process.exit(0)));
