// Live answers (ADR-028): every live query of the page on one connection to the node (`POST /live`
// with `queries`, each line naming its query). A browser opens six connections to a host at most:
// a stream each, six live cells held them all, and everything else the page asked for (a Python
// cell, a query) waited until one closed. Loaded with the first live query.
import { call } from './core.js';

const subs = new Map();
let ctl = null, timer = 0, made = 0;

/** Watch `sql`, in `session`: `on` gets each answer (`{at, rows}`), or `{error}` once it stopped.
 * Returns what stops it. */
export function watch(sql, session, on) {
  const id = 'q' + (++made).toString(36);
  subs.set(id, { sql, session, on });
  again();
  return () => { if (subs.delete(id)) again(); };
}
// (the queries changed: the stream again with all of them, once for changes made together; each
// query's answer comes once more, and the same rows aren't drawn again)
function again() { clearTimeout(timer); timer = setTimeout(open, 30); }

async function open() {
  ctl?.abort();
  ctl = null;
  if (!subs.size) return;
  const mine = ctl = new AbortController(), queries = [...subs].map(([id, s]) => ({ id, sql: s.sql, session: s.session }));
  try {
    const r = await call('/live', { method: 'POST', body: JSON.stringify({ queries }), headers: { 'content-type': 'application/json' }, signal: mine.signal });
    const reader = r.body.getReader(), dec = new TextDecoder();
    let buf = '';
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      for (let i; (i = buf.indexOf('\n')) >= 0;) {
        const line = buf.slice(0, i).trim();
        buf = buf.slice(i + 1);
        if (!line) continue; // (the node's line now and then, while nothing changes)
        const m = JSON.parse(line), s = subs.get(m.id);
        if (m.error) subs.delete(m.id);
        s?.on(m);
      }
    }
    if (ctl === mine) stopAll('the node closed the live queries');
  } catch (e) {
    if (ctl === mine && !mine.signal.aborted) stopAll(e.message);
  }
}
/** The stream ended on its own (the node stopped, or the network went): every query says so. */
function stopAll(error) {
  const all = [...subs.values()];
  subs.clear(); ctl = null;
  for (const s of all) s.on({ error });
}
