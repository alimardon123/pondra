#!/usr/bin/env node
// A page (web/, the module in Node) behind an HTTP port, so a check written for a node runs on a page unchanged
// (reviews/form-parity.md §1: one forms check over every form). Each request goes to `Page.request` as it came, and its
// answer (status, headers, body) goes back as the page gave it: the page's contract is the node's (ADR-061).
// `node tools/page_serve.mjs [--pkg web/pkg] [--port 0] [--lake]`: one page in memory, made afresh by `POST /reset`. With
// `--lake` it is the core's lake page (`lake({})`, from a module built with `--features lake`) instead of `new Page()`.
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
const lakeOn = args.includes('--lake');

const { default: init, Page, lake } = await import(pathToFileURL(join(pkg, 'pondra_web.js')).href);
await init({ module_or_path: await readFile(join(pkg, 'pondra_web_bg.wasm')) });
let page = lakeOn ? await lake({}) : new Page();

// One request, answered by the page. A page that throws (a bug, not a refusal: refusals are answers) is a 500 naming it,
// so a runner sees a failure where it would otherwise wait. `POST /reset` is this server's, not the page's: a fresh page
// with nothing in it, so a runner gives each file a page of its own without loading the module again.
async function answer(req, res) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (req.method === 'POST' && req.url === '/reset') {
    if (lakeOn) {
      try {
        await page.close(); // (close before free: close ends the member, and only one may be open in a module)
        page.free();
        page = await lake({});
      } catch (e) {
        res.writeHead(500, { 'content-type': 'text/plain' });
        res.end(`the page didn't reset: ${e?.message ?? e}`);
        return;
      }
    } else {
      page.free();
      page = new Page();
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
