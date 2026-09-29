// The console (ADR-030, ADR-032): Pondra's notebook for SQL, Python and text, served by every node.
//
// This module is the core and its API, `window.pondra` (exported as `pondra`). Everything the page
// shows is registered through that API — the catalog's sections, the details panel's tabs, the
// kinds of cell, the views of an answer, the top bar's actions, the places of the rail, commands
// and keys — the built-in ones as any extension's would be. So a build of the console for a bigger
// platform keeps this one and adds to it:
//
//   pondra.register.section({ id: 'jobs', title: 'Jobs', render: box => box.append(…) })
//   pondra.register.panel({ id: 'lineage', title: 'Lineage', render: (box, picked) => … })
//   pondra.register.renderer({ id: 'map', match: r => …, render: (r, cell) => element })
//   pondra.register.action({ id: 'share', label: 'Share', run: () => … })
//   pondra.register.nav({ id: 'home', label: 'Home', icon: '<path …/>', run: () => … })
//   pondra.configure({ fetch, token, headers })   // (its own gateway and sign-in)
//   pondra.on('pick', picked => …)                 // (and 'run', 'ran', 'refresh', 'start')
//
// No framework and nothing from elsewhere: it loads in one request from the node, and draws only
// what is on screen.

// ------------------------------------------------------------------ small things
const $ = (s, el = document) => el.querySelector(s);
function h(tag, attrs = {}, ...kids) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === 'class') e.className = v;
    else if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
    else if (k === 'html') e.innerHTML = v;
    else e.setAttribute(k, v === true ? '' : v);
  }
  for (const k of kids.flat()) if (k != null && k !== false) e.append(k);
  return e;
}
const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { v == null ? localStorage.removeItem(k) : localStorage.setItem(k, v); } catch { /* (private windows) */ } },
};
const secs = ms => ms < 1000 ? `${Math.round(ms)} ms` : ms < 60000 ? `${(ms / 1000).toFixed(ms < 10000 ? 2 : 1)} s` : `${Math.floor(ms / 60000)} min ${Math.round(ms % 60000 / 1000)} s`;
const count = n => Number(n).toLocaleString('en-US');
const short = n => n >= 1e9 ? (n / 1e9).toFixed(1) + 'B' : n >= 1e6 ? (n / 1e6).toFixed(1) + 'M' : n >= 1e4 ? (n / 1e3).toFixed(1) + 'K' : count(n);
const utc = t => new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(t) ? t : t + 'Z');
function ago(t) {
  const s = (Date.now() - utc(t)) / 1000;
  return s < 60 ? 'now' : s < 3600 ? `${Math.floor(s / 60)} min` : s < 86400 ? `${Math.floor(s / 3600)} h` : s < 86400 * 30 ? `${Math.floor(s / 86400)} d` : utc(t).toISOString().slice(0, 10);
}
let toastTimer;
function toast(msg, bad) {
  const t = $('#toast');
  t.textContent = msg; t.className = 'on' + (bad ? ' bad' : '');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.className = '', bad ? 6000 : 2600);
}
const ICONS = {
  chev: '<path d="m9 6 6 6-6 6"/>',
  db: '<ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
  schema: '<path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>',
  folder: '<path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>',
  table: '<rect x="3" y="4.5" width="18" height="15" rx="2"/><path d="M3 9.5h18M3 14.5h18M9.5 9.5v10"/>',
  view: '<rect x="3" y="4.5" width="18" height="15" rx="2" stroke-dasharray="2.6 2.4"/><path d="M3 9.5h18M9.5 9.5v10"/>',
  matview: '<rect x="3" y="4.5" width="18" height="15" rx="2"/><path d="M3 9.5h18M3 14.5h18M9.5 9.5v10"/><path d="M5 4.5h14a2 2 0 0 1 2 2v3H3v-3a2 2 0 0 1 2-2z" fill="currentColor"/>',
  files: '<path d="M6 3h8l5 5v13H6z"/><path d="M14 3v5h5M9 12.5h7M9 16.5h7M12 12.5v6"/>',
  file: '<path d="M6 3h8l5 5v13H6z"/><path d="M14 3v5h5"/>',
  book: '<path d="M4 5a2 2 0 0 1 2-2h13v16H6a2 2 0 0 0-2 2z"/><path d="M4 21V5"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  key: '<circle cx="8" cy="15" r="4"/><path d="M10.8 12.2 20 3M17 6l3 3M14 9l2 2"/>',
  play: '<path d="M7 5v14l11-7z"/>',
  down: '<path d="M12 4v12m-5-5 5 5 5-5M5 20h14"/>',
  copy: '<rect x="8" y="8" width="12" height="12" rx="2"/><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2"/>',
  chart: '<path d="M4 20V11M10 20V5M16 20v-6M21 20H3"/>',
  t_num: '<path d="M9.5 4 7.5 20M16.5 4l-2 16M4.5 9h15M3.5 15h15"/>',
  t_text: '<path d="M5 7V5h14v2M12 5v14M9 19h6"/>',
  t_date: '<rect x="3.5" y="5" width="17" height="15" rx="2"/><path d="M3.5 10h17M8 3v4M16 3v4"/>',
  t_time: '<circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2"/>',
  t_bool: '<rect x="2.5" y="7" width="19" height="10" rx="5"/><circle cx="16.5" cy="12" r="2.6"/>',
  t_json: '<path d="M8 4C6 4 5.5 5 5.5 7v2c0 1.2-.8 3-2.5 3 1.7 0 2.5 1.8 2.5 3v2c0 2 .5 3 2.5 3M16 4c2 0 2.5 1 2.5 3v2c0 1.2.8 3 2.5 3-1.7 0-2.5 1.8-2.5 3v2c0 2-.5 3-2.5 3"/>',
  t_list: '<path d="M8.5 4H5v16h3.5M15.5 4H19v16h-3.5"/>',
  t_bin: '<rect x="4" y="5" width="6" height="14" rx="3"/><path d="M14.5 7.5 17.5 5v14"/>',
  t_other: '<circle cx="12" cy="12" r="3.5"/>',
  dots: '<circle cx="5" cy="12" r="1.3" fill="currentColor"/><circle cx="12" cy="12" r="1.3" fill="currentColor"/><circle cx="19" cy="12" r="1.3" fill="currentColor"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  refresh: '<path d="M20 11a8 8 0 1 0-2.3 5.7M20 4v7h-7"/>',
  panel: '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M15 4v16"/>',
  up: '<path d="M12 20V8m-5 5 5-5 5 5M5 4h14"/>',
  clear: '<path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13"/>',
  restart: '<path d="M4 12a8 8 0 1 0 2.3-5.7M4 4v5h5"/>',
  keyboard: '<rect x="2" y="6" width="20" height="13" rx="2"/><path d="M6 10h.01M10 10h.01M14 10h.01M18 10h.01M7 15h10"/>',
  var: '<path d="M7 4C5 4 4.5 5 4.5 7v3c0 1-.7 2-2 2 1.3 0 2 1 2 2v3c0 2 .5 3 2.5 3M17 4c2 0 2.5 1 2.5 3v3c0 1 .7 2 2 2-1.3 0-2 1-2 2v3c0 2-.5 3-2.5 3M9 9l6 6M15 9l-6 6"/>',
  arrowUp: '<path d="M12 19V5m-6 6 6-6 6 6"/>',
  arrowDown: '<path d="M12 5v14m-6-6 6 6 6-6"/>',
  trash: '<path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13"/>',
  eye: '<path d="M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/>',
  stop: '<rect x="6" y="6" width="12" height="12" rx="2"/>',
};
const icon = (name, cls = 'ic', size = { tg: 14, kk: 12 }[cls] || 15) => h('span', { class: cls, 'aria-hidden': 'true', html: `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${ICONS[name]}</svg>` });
/** A column type's glyph: numbers, text, dates, times, true/false, lists, JSON-like, bytes. */
const typeIcon = (t = '') => /^(U?Int|Float|Decimal)/.test(t) ? 't_num' : /^(Utf8|LargeUtf8|Utf8View|Dictionary)/.test(t) ? 't_text' : /^Date/.test(t) ? 't_date' : /^(Timestamp|Time|Duration|Interval)/.test(t) ? 't_time'
  : /^Boolean/.test(t) ? 't_bool' : /\[\]$|^(Large|FixedSize)?List/.test(t) ? 't_list' : /^(Struct|Map)/.test(t) ? 't_json' : /Binary/.test(t) ? 't_bin' : 't_other';


// ------------------------------------------------------------------ the API: registries and events
const hooks = new Map();
/** Call `fn` on an event: 'start', 'pick' (what the details panel shows), 'run' (a cell starts),
 * 'ran' (it answered), 'refresh' (the catalog was read again), 'changed' (the notebook). */
function on(event, fn) { (hooks.get(event) || hooks.set(event, []).get(event)).push(fn); }
function emit(event, ...args) { for (const fn of hooks.get(event) || []) { try { fn(...args); } catch (e) { console.error(`pondra: ${event}:`, e); } } }
const R = { sections: [], panels: [], kinds: new Map(), renderers: [], actions: [], nav: [], commands: new Map(), keys: [] };
let drawing = 0;
/** Draw the regions again once (after registrations: an extension adds after the core starts). */
function redraw() { if (!drawing) drawing = requestAnimationFrame(() => { drawing = 0; if (started) { drawActions(); drawSide(); drawTabs(); drawRail(); drawKeys(); } }); }
const byOrder = (a, b) => (a.order ?? 50) - (b.order ?? 50);
const register = {
  /** A section of the left side: `{ id, title, render(box), tools?: [{ icon, title, run }], order }`.
   * `render` fills its box, again on each refresh. */
  section(o) { R.sections = R.sections.filter(x => x.id !== o.id).concat(o).sort(byOrder); redraw(); },
  /** A tab of the details panel: `{ id, title, render(box, picked), order }`. */
  panel(o) { R.panels = R.panels.filter(x => x.id !== o.id).concat(o).sort(byOrder); redraw(); },
  /** A kind of cell: `{ id, label, placeholder, language ('sql' | 'python' | 'markdown'), run(text, signal) → answer, live?, complete? }`. */
  cellKind(o) { R.kinds.set(o.id, o); redraw(); },
  /** A view of an answer: `{ id, match(answer) → bool, render(answer, cell) → element, order }`: the first that matches draws it. */
  renderer(o) { R.renderers = R.renderers.filter(x => x.id !== o.id).concat(o).sort(byOrder); },
  /** An action of the top bar: `{ id, label?, icon?, title, run(), primary?, menu? (in the ⋯ menu), order }`. */
  action(o) { R.actions = R.actions.filter(x => x.id !== o.id).concat(o).sort(byOrder); redraw(); },
  /** A place in the rail at the far left (the rail shows once there is one): `{ id, label, icon (SVG paths), run(), order }`. */
  nav(o) { R.nav = R.nav.filter(x => x.id !== o.id).concat(o).sort(byOrder); redraw(); },
  /** A command, by name, for keys and menus: `{ id, title, run() }`. */
  command(o) { R.commands.set(o.id, o); },
  /** A key on a cell (after Esc): `{ keys: 'd d', title, run(cell), group }`; listed under ?. */
  key(o) { R.keys = R.keys.filter(x => x.keys !== o.keys).concat(o); redraw(); },
};
/** How the page reaches the node: `fetch`, the token, extra headers (an enterprise build's
 * gateway and sign-in replace them). */
const T = { fetch: (url, init) => fetch(url, init), token: () => store.get('pondra.token'), headers: () => ({}) };
function configure(o) { Object.assign(T, o); }

// ------------------------------------------------------------------ the page's state
const MODE = document.documentElement.dataset.mode; // lake: one lake (`serve --lake`); lakes: a folder of lakes, its databases (`serve --lakes`)
const SESSION = [...crypto.getRandomValues(new Uint8Array(12))].map(b => b.toString(16).padStart(2, '0')).join(''); // (this page's temporary tables)
const S = { db: null, lake: null, cells: [], sel: null, name: 'untitled', version: null, dirty: false, runs: 0, open: new Set(), trash: null, last: null, pick: null, objects: null, info: null, filesAt: '', tab: 'details', py: 'none', vars: [], place: null };
const base = () => MODE === 'lakes' && S.db ? '/db/' + encodeURIComponent(S.db) : '';

// ------------------------------------------------------------------ talking to the node
class Failure extends Error { constructor(message, status) { super(message); this.status = status; } }

async function call(path, { method = 'GET', body, headers = {}, signal, root = false } = {}) {
  const hd = { 'x-pondra-session': SESSION, ...T.headers(), ...headers };
  const token = T.token();
  if (token) hd.authorization = 'Bearer ' + token;
  let r;
  try {
    r = await T.fetch((root ? '' : base()) + path, { method, body, headers: hd, signal });
  } catch (e) {
    if (e.name === 'AbortError') throw e;
    throw new Failure(`The node did not answer (${e.message}). Is it still running?`, 0);
  }
  if (r.ok) return r;
  const text = (await r.text()).trim() || `${r.status} ${r.statusText}`;
  if (r.status === 401) askToken(text);
  throw new Failure(text, r.status);
}

/** A statement's answer: rows (the columns with their types) or what it did, and what it printed. */
async function run(sql, signal) {
  const r = await call('/sql?format=typed', { method: 'POST', body: sql, headers: { 'content-type': 'text/plain; charset=utf-8' }, signal });
  let notices = [];
  try { notices = JSON.parse(r.headers.get('x-pondra-notices') || '[]'); } catch { /* (none) */ }
  const v = await r.json();
  if (v && Array.isArray(v.columns) && Array.isArray(v.rows)) return { kind: 'rows', columns: v.columns, rows: v.rows, total: v.total ?? v.rows.length, notices };
  return { kind: 'done', value: v, notices };
}

/** A query's rows as objects (the console's own queries). */
async function rows(sql) {
  const r = await run(sql);
  return r.kind === 'rows' ? r.rows.map(a => Object.fromEntries(r.columns.map((c, i) => [c.name, a[i]]))) : [];
}

const RESERVED = new Set('ALL AND ANY ARRAY AS ASC BETWEEN BY CASE CAST CHECK COLUMN CREATE CROSS DEFAULT DELETE DESC DISTINCT DO ELSE END EXCEPT FALSE FETCH FOR FROM FULL GRANT GROUP HAVING IN INNER INSERT INTERSECT INTO IS JOIN LEFT LIKE LIMIT NATURAL NOT NULL OFFSET ON OR ORDER OUTER RIGHT SELECT SET TABLE THEN TO TRUE UNION UNIQUE UPDATE USER USING VALUES VIEW WHEN WHERE WINDOW WITH'.split(' '));
const ident = s => /^[a-z_][a-z0-9_]*$/.test(s) && !RESERVED.has(s.toUpperCase()) ? s : '"' + s.replace(/"/g, '""') + '"';
/** A table's name as a query here writes it: `t`, `schema.t`, or `lake.schema.t` for an attached lake's. */
const home = () => S.lake || S.db; // (a server's database is the lake of that name)
const qualified = (c, s, t) => [c !== home() ? c : null, c !== home() || s !== 'public' ? s : null, t].filter(Boolean).map(ident).join('.');

/** A Python cell as the node runs it: Postgres's anonymous code block. */
function doBlock(code) {
  let tag = 'pondra';
  while (code.includes('$' + tag + '$')) tag += '_';
  return `DO LANGUAGE python $${tag}$\n${code}\n$${tag}$`;
}

/** Arrow's name for a type, as SQL names it. */
function sqlType(t = '') {
  const b = (t.match(/^\w+/) || [t])[0];
  if (t.endsWith('[]')) return sqlType(t.slice(0, -2)) + '[]';
  if (b === 'Timestamp') return /,/.test(t) && !/None\)/.test(t) ? 'TIMESTAMPTZ' : 'TIMESTAMP';
  if (/^Decimal/.test(b)) { const p = t.match(/\((\d+),\s*(-?\d+)\)/); return p ? `DECIMAL(${p[1]},${p[2]})` : 'DECIMAL'; }
  if (/^(Large|FixedSize)?List/.test(b)) { const i = t.match(/\((?:nullable |non-null )?(?:Field \{[^}]*data_type: )?(\w+[^,)]*)/); return (i ? sqlType(i[1]) : '') + '[]'; }
  const m = { Int8: 'TINYINT', Int16: 'SMALLINT', Int32: 'INT', Int64: 'BIGINT', UInt8: 'UTINYINT', UInt16: 'USMALLINT', UInt32: 'UINT', UInt64: 'UBIGINT', Float16: 'HALF', Float32: 'REAL', Float64: 'DOUBLE', Utf8: 'VARCHAR', LargeUtf8: 'VARCHAR', Utf8View: 'VARCHAR', Boolean: 'BOOLEAN', Date32: 'DATE', Date64: 'DATE', Binary: 'BYTEA', LargeBinary: 'BYTEA', BinaryView: 'BYTEA', FixedSizeBinary: 'BYTEA', Null: 'NULL', Time32: 'TIME', Time64: 'TIME', Interval: 'INTERVAL', Duration: 'INTERVAL', Struct: 'STRUCT', Map: 'MAP', Dictionary: 'VARCHAR' };
  return m[b] || t;
}
const numeric = t => /^(U?Int|Float|Decimal)/.test(t || '');

// ------------------------------------------------------------------ highlighting
const SQL_KW = new Set(('SELECT FROM WHERE GROUP BY ORDER HAVING LIMIT OFFSET JOIN LEFT RIGHT FULL INNER OUTER CROSS NATURAL ON USING AS AND OR NOT NULL IS IN EXISTS BETWEEN LIKE ILIKE SIMILAR CASE WHEN THEN ELSE END DISTINCT ALL UNION INTERSECT EXCEPT WITH RECURSIVE INSERT INTO VALUES UPDATE SET DELETE CREATE TABLE VIEW MATERIALIZED REPLACE DROP ALTER ADD COLUMN RENAME TO IF PRIMARY KEY DEFAULT TRUE FALSE ASC DESC NULLS FIRST LAST OVER PARTITION ROWS RANGE UNBOUNDED PRECEDING FOLLOWING CURRENT ROW FILTER WITHIN CAST TRY_CAST INTERVAL DATE TIMESTAMP TIMESTAMPTZ TIME BIGINT INT INTEGER SMALLINT TINYINT DOUBLE PRECISION FLOAT REAL DECIMAL NUMERIC VARCHAR TEXT CHAR BOOLEAN BYTEA BINARY JSON EXPLAIN ANALYZE SHOW DESCRIBE CALL DO LANGUAGE FUNCTION PROCEDURE RETURNS RETURN BEGIN COMMIT ROLLBACK MERGE MATCHED SCHEMA DATABASE ATTACH DETACH COPY TEMP TEMPORARY SECRET TASK QUALIFY LATERAL UNNEST ANY SOME ARRAY STRUCT MAP AT OF FOR TRUNCATE GRANT REVOKE WINDOW FETCH NEXT ONLY UNIQUE REFERENCES CHECK CONSTRAINT INDEX OPTIMIZE VACUUM INSTALL LOAD').split(' '));
const PY_KW = new Set('False None True and as assert async await break class continue def del elif else except finally for from global if import in is lambda nonlocal not or pass raise return try while with yield match case'.split(' '));
const SQL_RE = /(--[^\n]*|\/\*[\s\S]*?(?:\*\/|$))|('(?:[^']|'')*'?)|("(?:[^"]|"")*"?)|(\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b)|([A-Za-z_][\w$]*)/g;
const PY_RE = /(#[^\n]*)|([rRbBfFuU]{0,2}(?:"""[\s\S]*?(?:"""|$)|'''[\s\S]*?(?:'''|$)|"(?:[^"\\\n]|\\.)*"?|'(?:[^'\\\n]|\\.)*'?))|(\x00)|(\b\d[\d_]*(?:\.\d+)?(?:[eE][+-]?\d+)?\b)|([A-Za-z_]\w*)/g;
function highlight(text, kind) {
  if (kind === 'markdown') return esc(text) + '\n';
  const [re, kw, up] = kind === 'python' ? [PY_RE, PY_KW, false] : [SQL_RE, SQL_KW, true];
  let out = '', last = 0;
  for (const m of text.matchAll(re)) {
    const t = m[0];
    const cls = m[1] ? 'c' : m[2] ? 's' : m[4] ? 'nu' : m[5] ? (kw.has(up ? t.toUpperCase() : t) ? 'k' : /^\s*\(/.test(text.slice(m.index + t.length, m.index + t.length + 3)) ? 'f' : '') : '';
    out += esc(text.slice(last, m.index)) + (cls ? `<span class="${cls}">${esc(t)}</span>` : esc(t));
    last = m.index + t.length;
  }
  return out + esc(text.slice(last)) + '\n'; // (a last empty line keeps its height)
}

// ------------------------------------------------------------------ text cells
function markdown(src) {
  const inline = s => {
    const codes = [];
    s = esc(s).replace(/`([^`]+)`/g, (_, c) => (codes.push(c), `\u0000${codes.length - 1}\u0000`));
    s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>').replace(/(^|[^*\w])\*([^*\s][^*]*?)\*(?!\w)/g, '$1<em>$2</em>')
      .replace(/(^|[^\w])_([^_\s][^_]*?)_(?!\w)/g, '$1<em>$2</em>')
      .replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, (_, text, url) => /^(https?:|mailto:|#|\/|\.)/i.test(url.replace(/&amp;/g, '&')) ? `<a href="${url}" target="_blank" rel="noopener noreferrer">${text}</a>` : text);
    return s.replace(/\u0000(\d+)\u0000/g, (_, i) => `<code>${codes[i]}</code>`);
  };
  const lines = src.replace(/\r/g, '').split('\n'), item = /^\s*([-*+]|\d+[.)])\s+/, block = /^(```|#{1,6}\s|>|\s*([-*+]|\d+[.)])\s+|\s*(---|\*\*\*|___)\s*$)/;
  let html = '', i = 0;
  while (i < lines.length) {
    const l = lines[i];
    let m;
    if (/^```/.test(l)) {
      const code = [];
      for (i++; i < lines.length && !/^```/.test(lines[i]); i++) code.push(lines[i]);
      i++;
      html += `<pre><code>${esc(code.join('\n'))}</code></pre>`;
    } else if ((m = l.match(/^(#{1,6})\s+(.*)$/))) {
      html += `<h${m[1].length}>${inline(m[2])}</h${m[1].length}>`; i++;
    } else if (item.test(l)) {
      const tag = /^\s*\d/.test(l) ? 'ol' : 'ul', items = [];
      while (i < lines.length && item.test(lines[i])) items.push(lines[i++].replace(item, ''));
      html += `<${tag}>${items.map(x => `<li>${inline(x)}</li>`).join('')}</${tag}>`;
    } else if (/^>/.test(l)) {
      const quote = [];
      while (i < lines.length && /^>/.test(lines[i])) quote.push(lines[i++].replace(/^>\s?/, ''));
      html += `<blockquote>${inline(quote.join(' '))}</blockquote>`;
    } else if (/^\s*(---|\*\*\*|___)\s*$/.test(l)) {
      html += '<hr>'; i++;
    } else if (!l.trim()) {
      i++;
    } else {
      const para = [];
      do para.push(lines[i++]); while (i < lines.length && lines[i].trim() && !block.test(lines[i]));
      html += `<p>${inline(para.join('\n')).replace(/\n/g, ' ')}</p>`;
    }
  }
  return html || '<p class="hint">Empty text. Double-click to write (Markdown).</p>';
}

// ------------------------------------------------------------------ answers
const ROW_H = 28; // a row's height in a long answer: only the rows in sight are drawn
function doneText(v) {
  if (v == null || typeof v !== 'object') return v == null ? 'Done.' : String(v);
  const entries = Object.entries(v).filter(([k]) => k !== 'called' || v.called !== 'do');
  if (!entries.length) return v.called === 'do' ? '' : 'Done.';
  return entries.map(([k, x]) => `${k.replace(/_/g, ' ')} ${typeof x === 'string' ? x : JSON.stringify(x)}`).join(' · ');
}
/** A value as the grid, the CSV and the column explorer write it. */
function shown(v, time, scale) {
  if (v == null) return null;
  let s = typeof v === 'object' ? JSON.stringify(v) : typeof v === 'number' && scale != null ? v.toFixed(scale) : String(v); // (a live answer's decimals: numbers)
  return time ? s.replace(/^(\d{4}-\d\d-\d\d)T/, '$1 ') : s;
}
function cellOf(s, num) {
  if (s == null) return h('td', { class: num ? 'num' : null }, h('span', { class: 'null' }, 'NULL'));
  const td = h('td', { class: s.includes('\n') ? 'pre' : num ? 'num' : null }, s.length > 5000 ? s.slice(0, 5000) + '…' : s);
  if (s.length > 50 && !s.includes('\n')) td.title = s.slice(0, 4000);
  return td;
}
let measurer;
const textWidth = s => { measurer ||= document.createElement('canvas').getContext('2d'); measurer.font = '13px ' + getComputedStyle(document.body).fontFamily; return measurer.measureText(s).width; };
/** Rows in a grid: sortable by a header's click (which also shows that column in the details
 * panel); a long answer draws only the rows in sight, so ten thousand scroll as smoothly as ten. */
function grid(r, cell) {
  const cols = r.columns, all = r.rows;
  if (!cols.length) return h('div', { class: 'done' }, 'No columns.');
  const nums = cols.map(c => numeric(c.type)), times = cols.map(c => /^Timestamp/.test(c.type || '')), scales = cols.map(c => { const s = +((c.type || '').match(/^Decimal\d*\(\d+,\s*(\d+)\)/)?.[1] ?? NaN); return Number.isNaN(s) ? null : s; });
  const text = (row, i) => shown(row[i], times[i], scales[i]);
  const multi = all.slice(0, 200).some(row => row.some(v => typeof v === 'string' && v.includes('\n')));
  const virtual = !multi && all.length > 60;
  let order = all.map((_, i) => i), sort = null;
  const heads = cols.map((c, i) => h('th', { class: nums[i] ? 'num' : null, title: `${c.type}: click to sort, and to see this column in the details panel`, onclick: () => sortBy(i) }, c.name, h('span', { class: 'srt' }), h('small', {}, sqlType(c.type))));
  const body = h('tbody'), box = h('div', { class: 'grid' + (virtual ? ' v' : '') });
  const table = h('table', {}, h('thead', {}, h('tr', {}, h('th', { class: 'i' }, ''), heads)), body);
  if (virtual) {
    const sample = all.slice(0, 300), widths = cols.map((c, i) => Math.min(440, Math.max(64, textWidth(c.name) * 1.08 + 38, textWidth(sqlType(c.type)) * .9 + 30, ...sample.map(row => textWidth(text(row, i) ?? 'NULL') + 26))));
    const iw = textWidth(count(all.length)) + 24;
    table.prepend(h('colgroup', {}, h('col', { style: `width:${iw}px` }), widths.map(w => h('col', { style: `width:${Math.ceil(w)}px` }))));
    table.style.width = Math.ceil(iw + widths.reduce((a, b) => a + b, 0)) + 'px';
  }
  const rowOf = k => { const row = all[order[k]]; return h('tr', {}, h('td', { class: 'i' }, String(order[k] + 1)), row.map((_, i) => cellOf(text(row, i), nums[i]))); };
  const gap = px => h('tr', { class: 'gap', 'aria-hidden': 'true' }, h('td', { colspan: cols.length + 1, style: `height:${px}px` }));
  let drawn = 0, frame = 0;
  const more = h('button', { class: 'btn', onclick: () => draw() });
  function draw() {
    if (virtual) {
      const top = box.scrollTop, seen = box.clientHeight || 520, from = Math.max(0, Math.floor(top / ROW_H) - 12), to = Math.min(all.length, Math.ceil((top + seen) / ROW_H) + 12);
      const frag = document.createDocumentFragment();
      frag.append(gap(from * ROW_H));
      for (let k = from; k < to; k++) frag.append(rowOf(k));
      frag.append(gap((all.length - to) * ROW_H));
      body.replaceChildren(frag);
      more.hidden = true;
      return;
    }
    const end = Math.min(all.length, drawn + (drawn ? 1000 : 200)), frag = document.createDocumentFragment();
    for (; drawn < end; drawn++) frag.append(rowOf(drawn));
    body.append(frag);
    more.textContent = `Show ${count(Math.min(1000, all.length - drawn))} more`;
    more.hidden = drawn >= all.length;
  }
  function sortBy(i) {
    const dir = sort?.i === i ? (sort.dir === 1 ? -1 : 0) : 1;
    sort = dir ? { i, dir } : null;
    order = all.map((_, k) => k);
    if (sort) {
      const key = v => v == null ? null : nums[i] ? Number(v) : String(v);
      order.sort((a, b) => { const x = key(all[a][i]), y = key(all[b][i]); return x === y ? a - b : x == null ? 1 : y == null ? -1 : (x < y ? -1 : 1) * dir; });
    }
    heads.forEach((th, k) => { th.querySelector('.srt').textContent = sort?.i === k ? (sort.dir === 1 ? '▲' : '▼') : ''; th.classList.toggle('on', sort?.i === k); });
    body.replaceChildren(); drawn = 0; draw();
    explore(r, i, cell);
  }
  box.append(table);
  if (virtual) {
    box.style.height = Math.min(520, 34 + all.length * ROW_H) + 'px';
    box.addEventListener('scroll', () => { if (!frame) frame = requestAnimationFrame(() => { frame = 0; draw(); }); });
    requestAnimationFrame(draw);
  }
  draw();
  const csv = h('button', { class: 'btn', title: 'The rows here, as CSV', onclick: () => saveAs(toCsv(r), 'text/csv', `${S.name || 'rows'}.csv`) }, 'CSV');
  const look = h('button', { class: 'btn', title: 'Each column: its nulls, distinct values, range and spread, in the details panel', onclick: () => explore(r, 0, cell) }, 'Explore');
  const said = r.total > all.length ? `${count(all.length)} of ${count(r.total)} rows here` : `${count(r.total)} row${r.total === 1 ? '' : 's'}`;
  return h('div', {}, box, h('div', { class: 'meta' }, h('span', { class: 'n-rows' }, said), more, look, csv));
}
function toCsv(r) {
  const field = v => { const s = v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v); return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
  return [r.columns.map(c => c.name), ...r.rows].map(row => row.map(field).join(',')).join('\r\n') + '\r\n';
}
function saveAs(text, type, name) {
  const a = h('a', { href: URL.createObjectURL(new Blob([text], { type })), download: name });
  document.body.append(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 2000);
}

// ------------------------------------------------------------------ cells
let made = 0;
const newId = () => 'c' + Date.now().toString(36) + (made++).toString(36); // (nbformat's cell ids)

class Cell {
  constructor(o = {}) {
    this.id = /^[A-Za-z0-9_-]{1,64}$/.test(o.id || '') ? o.id : newId();
    this.kind = o.kind || 'sql';
    this.result = null; this.count = null; this.ctl = null; this.stream = null;
    this.kindSel = h('select', { class: 'kind', 'aria-label': 'Kind of cell', title: 'SQL, Python or text (S, P, M)', onchange: e => { this.setKind(e.target.value); this.edit(); } },
      [...R.kinds.values()].map(k => h('option', { value: k.id }, k.label)));
    this.runBtn = h('button', { class: 'run', title: 'Run (Ctrl+Enter)', onclick: () => this.ctl ? this.ctl.abort() : this.run() });
    this.idle();
    this.liveBox = h('input', { type: 'checkbox', onchange: () => this.setLive(this.liveBox.checked) });
    this.liveEl = h('label', { class: 'live', title: 'Live: the answer again each time a commit changes what it reads (L)' }, this.liveBox, h('span', { class: 'switch' }), 'Live');
    this.num = h('span', { class: 'n' });
    this.status = h('span', { class: 'st', 'aria-live': 'polite' });
    const tool = (ic, title, fn) => h('button', { class: 'icon', title, 'aria-label': title, onclick: fn }, icon(ic));
    const i = () => S.cells.indexOf(this);
    this.bar = h('div', { class: 'bar' }, this.num, this.kindSel, this.runBtn, this.liveEl, this.status,
      h('span', { class: 'tools' }, tool('arrowUp', 'Move up', () => move(this, -1)), tool('arrowDown', 'Move down', () => move(this, 1)),
        tool('plus', 'Add a cell below (B)', () => add({ kind: this.kind === 'markdown' ? 'sql' : this.kind }, this, true).edit()),
        tool('dots', 'More', e => menu(e.currentTarget, [{ label: 'Run the cells above', icon: 'arrowUp', run: () => runSome(0, i()) }, { label: 'Run this and the cells below', icon: 'arrowDown', run: () => runSome(i()) }, '-',
          { label: this.el.classList.contains('folded') ? 'Show the output' : 'Hide the output', icon: 'eye', keys: 'O', run: () => this.fold() }, { label: 'Clear the output', icon: 'clear', run: () => this.clear() }, '-',
          { label: 'Delete the cell', icon: 'trash', keys: 'D D', run: () => remove(this) }]))));
    this.pre = h('pre', { 'aria-hidden': 'true' });
    this.ta = h('textarea', { spellcheck: 'false', autocapitalize: 'off', autocomplete: 'off', 'aria-label': 'Code', rows: '1', wrap: 'off' });
    this.md = h('div', { class: 'md', ondblclick: () => this.edit() });
    this.out = h('div', { class: 'out', onclick: () => { if (this.el.classList.contains('folded')) this.fold(false); } });
    this.el = h('section', { class: 'cell', tabindex: '-1', 'data-kind': this.kind }, this.bar, h('div', { class: 'ed' }, this.pre, this.ta), this.md, this.out);
    this.el.cell = this;
    this.ta.value = o.src || '';
    this.ta.addEventListener('input', () => { this.paint(); changed(); if (cm?.c === this) complete(this); });
    this.ta.addEventListener('scroll', () => { this.pre.scrollLeft = this.ta.scrollLeft; });
    this.ta.addEventListener('keydown', e => editing(e, this));
    this.ta.addEventListener('focus', () => { select(this); this.el.classList.add('editing'); S.last = this; });
    this.ta.addEventListener('blur', () => { if (this.kind === 'markdown') { this.md.innerHTML = markdown(this.ta.value); } this.el.classList.remove('editing'); this.ta.scrollLeft = 0; if (cm?.c === this) closeComplete(); });
    this.el.addEventListener('mousedown', e => { if (!e.target.closest('textarea,button,select,input,a,label')) select(this); });
    this.setKind(this.kind, true);
    if (o.live) { this.liveBox.checked = true; this.status.textContent = 'live once run'; }
    if (o.out) this.show(o.out, true);
  }
  get src() { return this.ta.value; }
  get type() { return R.kinds.get(this.kind) || R.kinds.get('sql'); }
  idle() { this.runBtn.replaceChildren(icon('play'), 'Run'); this.runBtn.classList.remove('stop'); this.runBtn.title = 'Run (Ctrl+Enter)'; }
  fold(on = !this.el.classList.contains('folded')) { this.el.classList.toggle('folded', on); }
  clear() { this.stopLive(); this.result = null; this.out.replaceChildren(); this.status.textContent = ''; changed(); }
  setKind(k, quiet) {
    this.kind = R.kinds.has(k) ? k : 'sql'; k = this.kind; this.el.dataset.kind = k; this.kindSel.value = k;
    this.liveEl.hidden = !this.type.live;
    if (!this.type.live) { this.stopLive(); this.liveBox.checked = false; }
    this.ta.placeholder = this.type.placeholder || '';
    if (k === 'python') kernel();
    if (k === 'markdown') this.md.innerHTML = markdown(this.ta.value);
    this.paint();
    if (!quiet) changed();
  }
  paint() {
    this.pre.innerHTML = highlight(this.ta.value, this.type.language || this.kind);
    this.ta.style.height = 'auto';
    this.ta.style.height = (this.ta.scrollHeight + 2) + 'px';
  }
  edit() {
    this.el.classList.add('editing');
    this.paint();
    this.ta.focus({ preventScroll: true });
    this.el.scrollIntoView({ block: 'nearest' });
  }
  async run() {
    if (!this.type.run) { this.md.innerHTML = markdown(this.ta.value); this.ta.blur(); this.el.focus({ preventScroll: true }); return { kind: 'done' }; }
    const text = this.src.trim();
    if (!text) return { kind: 'done' };
    this.stopLive();
    this.ctl?.abort();
    const ctl = this.ctl = new AbortController(), t0 = performance.now();
    this.count = ++S.runs; this.num.textContent = `[${this.count}]`;
    this.runBtn.replaceChildren(icon('stop'), 'Stop'); this.runBtn.classList.add('stop'); this.runBtn.title = 'Stop waiting for it';
    emit('run', this);
    this.status.className = 'st pulse'; this.status.textContent = 'running…';
    const tick = setInterval(() => { this.status.textContent = 'running… ' + secs(performance.now() - t0); }, 250);
    let r;
    try {
      r = await this.type.run(text, ctl.signal);
    } catch (e) {
      r = { kind: 'error', message: e.name === 'AbortError' ? 'Stopped waiting. (A statement already on its way may still finish on the node.)' : e.message, notices: [] };
    } finally {
      clearInterval(tick);
      if (this.ctl === ctl) this.ctl = null;
      this.idle();
    }
    r.ms = performance.now() - t0;
    this.show(r);
    if (r.kind === 'rows' && this.type.live && this.liveBox.checked) this.startLive();
    if (r.kind === 'done' || this.kind === 'python') later(refresh);
    changed(true);
    emit('ran', this, r);
    return r;
  }
  show(r, saved) {
    this.result = r;
    this.out.replaceChildren();
    if (r.notices?.length) this.out.append(h('pre', { class: 'said' }, r.notices.join('\n')));
    const view = R.renderers.find(v => { try { return v.match(r); } catch { return false; } });
    if (view) { const el = view.render(r, this); if (el) this.out.append(el); }
    if (saved) this.out.append(h('div', { class: 'meta' }, h('span', { class: 'badge', title: 'As it was when the notebook was saved: run the cell for the answer now' }, 'saved')));
    this.status.className = 'st' + (r.kind === 'error' ? ' bad' : '');
    this.status.textContent = saved ? '' : r.kind === 'error' ? `failed · ${secs(r.ms)}` : secs(r.ms);
  }
  setLive(on) {
    this.liveBox.checked = on;
    if (on) this.run(); else { this.stopLive(); this.status.textContent = ''; }
    changed();
  }
  stopLive() { if (this.stream) { this.stream.abort(); this.stream = null; } }
  async startLive() {
    this.stopLive();
    const ctl = this.stream = new AbortController(), cols = this.result.columns, first = this.result;
    this.status.className = 'st'; this.status.innerHTML = '<span class="dot"></span>waiting for changes';
    try {
      const r = await call('/live?sql=' + encodeURIComponent(this.src.trim()), { signal: ctl.signal });
      const reader = r.body.getReader(), dec = new TextDecoder();
      let buf = '', seen = 0;
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        for (let i; (i = buf.indexOf('\n')) >= 0;) {
          const line = buf.slice(0, i).trim();
          buf = buf.slice(i + 1);
          if (line) this.liveAnswer(JSON.parse(line), cols, first, !(seen++));
        }
      }
      if (this.stream === ctl) throw new Failure('the node closed the live query');
    } catch (e) {
      if (e.name === 'AbortError' || this.stream !== ctl) return;
      this.stream = null; this.liveBox.checked = false;
      this.status.className = 'st bad'; this.status.textContent = 'live stopped';
      this.out.prepend(h('pre', { class: 'err' }, e.message));
    }
  }
  liveAnswer(m, cols, first, opening) {
    if (m.error) throw new Failure(m.error);
    const names = cols.length ? cols.map(c => c.name) : Object.keys(m.rows[0] || {});
    const columns = cols.length ? cols : names.map(n => ({ name: n, type: '' }));
    const r = { kind: 'rows', columns, rows: m.rows.slice(0, 10000).map(o => names.map(n => o[n] ?? null)), total: m.rows.length, notices: [], ms: first.ms };
    if (JSON.stringify(r.rows) !== JSON.stringify(this.result.rows)) {
      this.show(r);
      if (!opening) this.out.querySelector('.grid')?.classList.add('flash'); // (what changed since)
    }
    this.status.className = 'st'; this.status.innerHTML = `<span class="dot"></span>updated ${new Date().toLocaleTimeString()} (commit ${esc(m.at)})`;
  }
}

function select(c, focus) {
  if (!c) return;
  if (S.sel && S.sel !== c) S.sel.el.classList.remove('sel');
  S.sel = c; c.el.classList.add('sel');
  if (focus) { c.el.focus({ preventScroll: true }); c.el.scrollIntoView({ block: 'nearest' }); }
}
function add(o, near, below = true) {
  const c = new Cell(o), i = near ? S.cells.indexOf(near) + (below ? 1 : 0) : S.cells.length;
  S.cells.splice(i, 0, c);
  $('#cells').insertBefore(c.el, S.cells[i + 1]?.el || null);
  c.paint(); // (its height, now that it is on the page)
  changed();
  return c;
}
function remove(c) {
  c.stopLive(); c.ctl?.abort();
  const i = S.cells.indexOf(c);
  S.cells.splice(i, 1); c.el.remove();
  S.trash = { c, i };
  if (!S.cells.length) add({});
  select(S.cells[Math.min(i, S.cells.length - 1)], true);
  changed();
}
function restore() {
  if (!S.trash) return;
  const { c, i } = S.trash;
  S.trash = null;
  S.cells.splice(Math.min(i, S.cells.length), 0, c);
  $('#cells').insertBefore(c.el, S.cells[i + 1]?.el || null);
  select(c, true); changed();
}
function move(c, d) {
  const i = S.cells.indexOf(c), j = i + d;
  if (j < 0 || j >= S.cells.length) return;
  S.cells.splice(i, 1); S.cells.splice(j, 0, c);
  $('#cells').insertBefore(c.el, S.cells[j + 1]?.el || null);
  c.el.scrollIntoView({ block: 'nearest' }); changed();
}
function next(c) {
  const i = S.cells.indexOf(c), made = !S.cells[i + 1];
  const n = S.cells[i + 1] || add({ kind: c.kind === 'markdown' ? 'sql' : c.kind });
  select(n, true);
  if (made) n.edit(); // (as Jupyter: a new cell is for typing in)
}
const runAll = () => runSome(0);

// ------------------------------------------------------------------ keys
function insert(ta, s) {
  if (document.execCommand('insertText', false, s)) return; // (so Ctrl+Z undoes it)
  ta.setRangeText(s, ta.selectionStart, ta.selectionEnd, 'end');
  ta.dispatchEvent(new Event('input'));
}
function indent(ta, back) {
  const v = ta.value, a = ta.selectionStart, b = ta.selectionEnd;
  if (!back && a === b) return insert(ta, '    ');
  const start = v.lastIndexOf('\n', a - 1) + 1, end = b > a && v[b - 1] === '\n' ? b - 1 : b;
  const stop = v.indexOf('\n', end) < 0 ? v.length : v.indexOf('\n', end);
  const lines = v.slice(start, stop).split('\n').map(l => back ? l.replace(/^( {1,4}|\t)/, '') : '    ' + l).join('\n');
  ta.setSelectionRange(start, stop); insert(ta, lines); ta.setSelectionRange(start, start + lines.length);
}
function editing(e, c) {
  const mod = e.ctrlKey || e.metaKey;
  if (cm?.c === c) {
    const n = cm.list.length, go = { ArrowDown: 1, ArrowUp: -1 }[e.key];
    if (go) { e.preventDefault(); cm.on = (cm.on + go + n) % n; drawComplete(); return; }
    if (e.key === 'Enter' || e.key === 'Tab') { e.preventDefault(); accept(); return; }
    if (e.key === 'Escape') { e.preventDefault(); closeComplete(); return; }
  }
  if (e.key === ' ' && e.ctrlKey) { e.preventDefault(); complete(c, true); return; }
  if (e.key === 'Tab' && !mod && !e.altKey && !e.shiftKey && c.ta.selectionStart === c.ta.selectionEnd && /[\w.$"]$/.test(c.ta.value.slice(0, c.ta.selectionStart)) && complete(c)) { e.preventDefault(); if (cm.list.length === 1) accept(); return; }
  if (e.key === 'Enter' && (mod || e.shiftKey || e.altKey)) {
    e.preventDefault();
    if (mod && e.shiftKey) return runAll();
    if (mod) return c.run();
    if (e.shiftKey) { c.run(); return next(c); }
    c.run(); return add({ kind: c.kind === 'markdown' ? 'sql' : c.kind }, c, true).edit();
  }
  if (e.key === 'Escape') { e.preventDefault(); c.ta.blur(); c.el.focus({ preventScroll: true }); return; }
  if (e.key === 'Tab' && !mod && !e.altKey) { e.preventDefault(); indent(c.ta, e.shiftKey); return; }
  if (e.key === 'Backspace' && !mod && c.ta.selectionStart === c.ta.selectionEnd) {
    const v = c.ta.value, a = c.ta.selectionStart, before = v.slice(v.lastIndexOf('\n', a - 1) + 1, a);
    if (before.length && /^ +$/.test(before)) { e.preventDefault(); c.ta.setSelectionRange(a - ((before.length - 1) % 4 + 1), a); insert(c.ta, ''); }
    return;
  }
  if (e.key === 'Enter' && !e.isComposing) {
    const v = c.ta.value, a = c.ta.selectionStart, line = v.slice(v.lastIndexOf('\n', a - 1) + 1, a);
    let ind = line.match(/^[ \t]*/)[0];
    if (c.kind === 'python' && /:\s*(#.*)?$/.test(line)) ind += '    ';
    if (ind) { e.preventDefault(); insert(c.ta, '\n' + ind); }
  }
}
let lastKey = '';
document.addEventListener('keydown', e => {
  const mod = e.ctrlKey || e.metaKey, t = e.target, c = S.sel;
  if (mod && e.key.toLowerCase() === 's') { e.preventDefault(); save(); return; }
  if (t.closest?.('textarea,input,select,dialog,button,a,label,summary')) return; // (typing, or a control's own key)
  if (e.key === '?') { e.preventDefault(); $('#helpDlg').showModal(); return; }
  if (!c || (t !== document.body && !t.closest?.('.cell,main'))) return;
  if (e.altKey || (mod && e.key !== 'Enter')) return;
  const key = e.key.length === 1 ? e.key.toLowerCase() : e.key, prev = lastKey;
  lastKey = key;
  const go = d => { const n = S.cells[S.cells.indexOf(c) + d]; if (n) select(n, true); };
  const acts = {
    Enter: () => mod && e.shiftKey ? runAll() : mod ? c.run() : e.shiftKey ? (c.run(), next(c)) : c.edit(),
    ArrowUp: () => go(-1), k: () => go(-1), ArrowDown: () => go(1), j: () => go(1),
    a: () => select(add({ kind: c.kind === 'markdown' ? 'sql' : c.kind }, c, false), true),
    b: () => select(add({ kind: c.kind === 'markdown' ? 'sql' : c.kind }, c, true), true),
    d: () => { if (prev === 'd') { lastKey = ''; remove(c); } },
    z: () => restore(),
    s: () => c.setKind('sql'), p: () => c.setKind('python'), m: () => c.setKind('markdown'),
    l: () => { if (c.type.live) c.setLive(!c.liveBox.checked); },
    o: () => c.fold(),
    0: () => { if (prev === '0') { lastKey = ''; if (confirm('Restart Python? Its variables go.')) restart(); } },
  };
  if (acts[key]) { e.preventDefault(); acts[key](); return; }
  const mine = R.keys.find(k => k.run && k.keys.toLowerCase() === key);
  if (mine) { e.preventDefault(); mine.run(c); }
});

// ------------------------------------------------------------------ notebooks (.ipynb)
const lines = s => s.split(/(?<=\n)/);
function textTable(columns, rows, total) {
  const cells = [columns.map(c => c.name), ...rows.map(r => r.map(v => { const s = v == null ? 'NULL' : typeof v === 'object' ? JSON.stringify(v) : String(v); return s.length > 60 ? s.slice(0, 59) + '…' : s.replace(/\n/g, ' '); }))];
  const w = columns.map((_, i) => Math.max(...cells.map(r => r[i].length)));
  const fmt = r => r.map((s, i) => numeric(columns[i].type) ? s.padStart(w[i]) : s.padEnd(w[i])).join(' | ').trimEnd();
  return [fmt(cells[0]), w.map(n => '-'.repeat(n)).join('-+-'), ...cells.slice(1).map(fmt), `(${count(total)} row${total === 1 ? '' : 's'})`].join('\n');
}
const KEPT = 100; // rows a saved notebook keeps of each answer
function outputs(c) {
  const r = c.result, out = [];
  if (!r || c.kind === 'markdown') return out;
  const n = c.count ?? null;
  if (r.notices?.length) out.push({ output_type: 'stream', name: 'stdout', text: lines(r.notices.join('\n') + '\n') });
  if (r.kind === 'error') out.push({ output_type: 'error', ename: 'Error', evalue: r.message, traceback: r.message.split('\n') });
  if (r.kind === 'rows') {
    const kept = r.rows.slice(0, KEPT);
    out.push({ output_type: 'execute_result', execution_count: n, metadata: {}, data: { 'text/plain': lines(textTable(r.columns, kept, r.total)), 'application/vnd.pondra.rows+json': { columns: r.columns, rows: kept, total: r.total } } });
  }
  for (const b of r.kind === 'done' && Array.isArray(r.value?.images) ? r.value.images : []) out.push({ output_type: 'display_data', metadata: {}, data: { 'image/png': b, 'text/plain': ['<Figure>'] } });
  const said = r.kind === 'text' ? r.text : r.kind === 'done' && !r.value?.images ? doneText(r.value) : '';
  if (said) out.push({ output_type: 'execute_result', execution_count: n, metadata: {}, data: { 'text/plain': lines(said) } });
  return out;
}
function notebook() {
  return {
    cells: S.cells.map(c => c.kind === 'markdown'
      ? { cell_type: 'markdown', id: c.id, metadata: {}, source: lines(c.src) }
      : { cell_type: 'code', id: c.id, metadata: c.kind === 'sql' && c.liveBox.checked ? { pondra: { live: true } } : {}, execution_count: c.result ? c.count ?? null : null, source: lines(c.kind === 'sql' ? '%%sql\n' + c.src : c.src), outputs: outputs(c) }),
    metadata: { kernelspec: { name: 'python3', display_name: 'Python 3', language: 'python' }, language_info: { name: 'python' }, pondra: { database: S.db || S.lake } },
    nbformat: 4, nbformat_minor: 5,
  };
}
function savedAnswer(outs) {
  const text = x => Array.isArray(x) ? x.join('') : String(x ?? '');
  const r = { kind: 'none', notices: [] };
  for (const o of outs || []) {
    if (o.output_type === 'stream') r.notices.push(text(o.text).replace(/\n$/, ''));
    else if (o.output_type === 'error') Object.assign(r, { kind: 'error', message: o.evalue || o.ename || 'error' });
    else if (o.data?.['image/png']) { const v = r.kind === 'done' && r.value?.images ? r.value : { images: [] }; v.images.push(text(o.data['image/png']).replace(/\s/g, '')); Object.assign(r, { kind: 'done', value: v }); }
    else if (o.data?.['application/vnd.pondra.rows+json']?.columns) { const d = o.data['application/vnd.pondra.rows+json']; Object.assign(r, { kind: 'rows', columns: d.columns, rows: d.rows || [], total: d.total ?? (d.rows || []).length }); }
    else if (o.data?.['text/plain'] != null) Object.assign(r, { kind: 'text', text: text(o.data['text/plain']) });
  }
  return r.kind === 'none' && !r.notices.length ? null : r.kind === 'none' ? { ...r, kind: 'done', value: null } : r;
}
function cellsOf(nb) {
  if (!nb || !Array.isArray(nb.cells)) throw new Error('this is not a notebook (.ipynb): it has no cells');
  return nb.cells.map(c => {
    const src = Array.isArray(c.source) ? c.source.join('') : String(c.source ?? '');
    if (c.cell_type !== 'code') return { kind: 'markdown', src, id: c.id };
    const magic = src.match(/^%%sql[^\n]*(\n|$)/);
    return { kind: magic ? 'sql' : 'python', src: magic ? src.slice(magic[0].length) : src, id: c.id, live: !!c.metadata?.pondra?.live, out: savedAnswer(c.outputs) };
  });
}
function load(nb, name, version) {
  const cells = cellsOf(nb);
  for (const c of S.cells) { c.stopLive(); c.ctl?.abort(); }
  S.cells = []; $('#cells').replaceChildren();
  for (const c of cells.length ? cells : [{}]) add(c);
  S.name = name; S.version = version; $('#nbname').value = name;
  if (S.pick?.type === 'result') { S.pick = null; detail(); } // (an answer of the notebook left)
  reoutline(0);
  select(S.cells[0]);
  saved();
}
const cleanName = s => s.trim().replace(/\.ipynb$/i, '').replace(/[^\w.-]+/g, '-').replace(/^[.-]+|-+$/g, '').slice(0, 80);
const stampOf = () => new Date().toISOString().replace(/[:.]/g, '-');
async function save() {
  const name = cleanName($('#nbname').value);
  if (!name) { toast('Give the notebook a name first', true); $('#nbname').focus(); return; }
  S.name = name; $('#nbname').value = name;
  const version = stampOf(), path = `notebooks/${name}/${version}.ipynb`;
  try {
    await call('/files/' + path.split('/').map(encodeURIComponent).join('/'), { method: 'PUT', body: JSON.stringify(notebook(), null, 1) + '\n', headers: { 'content-type': 'application/x-ipynb+json' } });
    S.version = version; saved();
    toast(`Saved in the lake: files/${path}`);
    notebooks();
  } catch (e) {
    toast('Not saved: ' + e.message, true);
  }
}
async function openSaved(name, version) {
  if (S.dirty && !confirm('Open another notebook? This one has changes that are not saved.')) return;
  try {
    const r = await call(`/files/notebooks/${encodeURIComponent(name)}/${encodeURIComponent(version)}.ipynb`);
    load(JSON.parse(await r.text()), name, version);
    notebooks();
  } catch (e) { toast(`Could not open ${name}: ${e.message}`, true); }
}
function changed(ran) {
  if (!ran) S.dirty = true;
  if (S.dirty) { $('#dirty').hidden = false; document.title = `• ${S.name} · Pondra`; }
  reoutline();
  emit('changed');
}
function saved() {
  S.dirty = false; $('#dirty').hidden = true;
  document.title = `${S.name} · Pondra`;
  const p = new URLSearchParams();
  if (S.db && MODE === 'lakes') p.set('db', S.db);
  if (S.version) p.set('notebook', S.name);
  history.replaceState(null, '', p.size ? '#' + p : location.pathname);
}
function blank() {
  load({ cells: [] }, 'untitled', null);
  S.cells[0].edit();
}

async function notebooks() {
  const box = $('#notebooks');
  let files;
  try { files = await rows(`SELECT path, written FROM files('notebooks/') ORDER BY path`); } catch (e) {
    box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to list them.' : e.message));
    return;
  }
  const by = new Map();
  for (const f of files) {
    const m = f.path.match(/^files\/notebooks\/([^/]+)\/([^/]+)\.ipynb$/);
    if (m) (by.get(m[1]) || by.set(m[1], []).get(m[1])).push({ version: m[2], written: f.written });
  }
  if (!by.size) { box.replaceChildren(h('div', { class: 'empty' }, 'None saved yet: Ctrl+S saves this one.')); return; }
  box.replaceChildren(...[...by.keys()].sort().map(name => {
    const vs = by.get(name).sort((a, b) => a.version < b.version ? 1 : -1), key = 'nb:' + name;
    const kids = h('div', { class: 'kids', hidden: !S.open.has(key) }, vs.map(v => h('button', { class: 'row' + (name === S.name && v.version === S.version ? ' cur' : ''), title: 'Open this version', onclick: () => openSaved(name, v.version) }, icon('clock'), h('span', { class: 'nm' }, utc(v.written).toLocaleString()))));
    const tw = twisty(key, kids, vs.length > 1);
    return h('div', { role: 'treeitem' }, h('div', { class: 'line' }, tw,
      h('button', { class: 'row' + (name === S.name ? ' cur' : ''), title: `Open (${vs.length} version${vs.length > 1 ? 's' : ''}; the latest saved ${utc(vs[0].written).toLocaleString()})`, onclick: () => openSaved(name, vs[0].version) },
        icon('book'), h('span', { class: 'nm' }, name), h('span', { class: 'ct' }, ago(vs[0].written)))), kids);
  }));
}

// ------------------------------------------------------------------ the catalog
function twisty(key, kids, any = true, load) {
  const b = h('button', { class: 'tw' + (any ? '' : ' none'), 'aria-expanded': String(!kids.hidden), 'aria-label': 'Open', tabindex: any ? null : '-1', html: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round">${ICONS.chev}</svg>` });
  b.onclick = e => {
    e?.stopPropagation();
    kids.hidden = !kids.hidden;
    b.setAttribute('aria-expanded', String(!kids.hidden));
    kids.hidden ? S.open.delete(key) : S.open.add(key);
    if (!kids.hidden) load?.();
  };
  if (!kids.hidden) load?.();
  return b;
}
/** A line of the tree: its twisty (or room for one), and a row that picks it. */
function line(tw, attrs, ...kids) { return h('div', { class: 'line' }, tw || h('span', { class: 'tw none' }), h('button', { class: 'row', ...attrs }, ...kids)); }
const KIND = { table: ['table', 'table'], view: ['view', 'view'], 'materialized view': ['matview', 'materialized view'], files: ['files', 'view of files'] };
async function catalog() {
  const [tables, columns, objects] = await Promise.all([
    rows(`SELECT table_catalog AS c, table_schema AS s, table_name AS t, table_type AS k FROM information_schema.tables WHERE table_schema <> 'information_schema' AND table_schema NOT IN (SELECT catalog_name FROM information_schema.schemata) ORDER BY 1, 2, 3`),
    rows(`SELECT table_catalog AS c, table_schema AS s, table_name AS t, column_name AS n, data_type AS d FROM information_schema.columns WHERE table_schema <> 'information_schema' ORDER BY 1, 2, 3, ordinal_position`),
    call('/objects').then(r => r.json(), () => ({ objects: [] })),
  ]);
  S.filesAt = objects.files;
  const about = new Map(objects.objects.map(o => [`${o.catalog}\u0000${o.schema}\u0000${o.name}`, o]));
  const lakes = new Map(), find = new Map();
  for (const t of tables) {
    const schemas = lakes.get(t.c) || lakes.set(t.c, new Map()).get(t.c);
    const list = schemas.get(t.s) || schemas.set(t.s, []).get(t.s);
    const k = `${t.c}\u0000${t.s}\u0000${t.t}`;
    const table = { ...t, columns: [], o: about.get(k) || { kind: t.k === 'VIEW' ? 'view' : 'table' }, q: qualified(t.c, t.s, t.t), key: 'o:' + k };
    list.push(table); find.set(k, table);
  }
  for (const c of columns) find.get(`${c.c}\u0000${c.s}\u0000${c.t}`)?.columns.push(c);
  S.objects = [...find.values()];
  return lakes;
}
function lakeNode(name, schemas, current, note) {
  const key = 'db:' + name;
  if (current && !S.open.has('closed:' + key)) S.open.add(key);
  const kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(key) });
  if (schemas) {
    const names = [...schemas.keys()].sort((a, b) => (a !== 'public') - (b !== 'public') || a.localeCompare(b));
    kids.append(...names.map(s => schemaNode(name, s, schemas.get(s), names.length === 1)));
    if (!names.length) kids.append(h('div', { class: 'empty' }, 'No tables yet.'));
  }
  const tw = twisty(key, kids, !!schemas);
  tw.addEventListener('click', () => kids.hidden ? S.open.add('closed:' + key) : S.open.delete('closed:' + key));
  const pick = () => { if (MODE === 'lakes' && name !== S.db) use(name); else tw.click(); };
  const row = line(tw, { class: 'row' + (current ? ' cur' : ''), title: MODE === 'lakes' && !current ? `Use database ${name}` : name, onclick: pick }, icon('db'), h('span', { class: 'nm' }, name), note ? h('span', { class: 'ct' }, note) : null);
  return h('div', { role: 'treeitem' }, row, kids);
}
function schemaNode(lake, schema, tables, only) {
  const key = `s:${lake}.${schema}`;
  if ((only || schema === 'public') && !S.open.has('closed:' + key)) S.open.add(key);
  const kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(key) }, tables.map(t => tableNode(t)));
  const tw = twisty(key, kids, true);
  tw.addEventListener('click', () => kids.hidden ? S.open.add('closed:' + key) : S.open.delete('closed:' + key));
  return h('div', { role: 'treeitem' }, line(tw, { title: `schema ${schema}`, onclick: () => tw.click() }, icon('schema'), h('span', { class: 'nm' }, schema)), kids);
}
function tableNode(t) {
  const [ic, word] = KIND[t.o.kind] || KIND.table, keyed = new Set(t.o.key || []);
  const kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(t.key) }, t.columns.map(c =>
    line(null, { class: 'row col', title: `${c.n}: ${sqlType(c.d)} (${c.d}). Click: put the name in the cell`, onclick: () => put(ident(c.n)) },
      icon(typeIcon(c.d), 'tg'), h('span', { class: 'nm' }, c.n), keyed.has(c.n) ? icon('key', 'kk') : null, h('span', { class: 'ty' }, sqlType(c.d)))));
  const row = line(twisty(t.key, kids, t.columns.length > 0), { title: `${t.q}: a ${word}. Click: its details; double-click: its first rows`, 'data-key': t.key, 'data-kind': t.o.kind, onclick: () => pick({ type: 'object', t }), ondblclick: () => peek(t.q) },
    icon(ic), h('span', { class: 'nm' }, t.t));
  return h('div', { role: 'treeitem' }, row, kids);
}
function put(text) {
  const c = S.last && S.cells.includes(S.last) ? S.last : S.sel;
  if (!c || c.kind === 'markdown') return;
  c.edit(); insert(c.ta, text);
}
function peek(q) {
  const empty = S.sel && !S.sel.src.trim() && S.sel.kind === 'sql' ? S.sel : null;
  const c = empty || add({ kind: 'sql' }, S.sel, true);
  c.ta.value = `SELECT * FROM ${q} LIMIT 100`; c.paint(); changed();
  select(c, true); c.run();
}

async function tree() {
  const box = $('#tree');
  if (MODE === 'lakes') {
    let dbs;
    try { dbs = await (await call('/databases', { root: true })).json(); } catch (e) {
      box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to list the databases.' : e.message));
      return;
    }
    if (!S.db || !dbs.some(d => d.name === S.db)) S.db = (dbs.find(d => d.default) || dbs[0])?.name || null;
    let lakes = null;
    try { lakes = S.db ? await catalog() : null; } catch (e) { toast(e.message, true); }
    box.replaceChildren(...dbs.map(d => lakeNode(d.name, d.name === S.db ? lakes?.get(d.name) || new Map() : null, d.name === S.db, d.name === S.db ? null : d.running ? 'running' : '')));
    if (!dbs.length) box.append(h('div', { class: 'empty' }, 'No databases yet: + makes one.'));
  } else {
    let lakes;
    try { lakes = await catalog(); } catch (e) {
      box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to see the tables.' : e.message));
      return;
    }
    const names = [S.lake, ...[...lakes.keys()].filter(n => n !== S.lake).sort()].filter(Boolean);
    box.replaceChildren(...names.map(n => lakeNode(n, lakes.get(n) || new Map(), n === S.lake, n === S.lake ? null : 'attached')));
  }
  if (S.pick?.type === 'object') S.pick.t = S.objects?.find(t => t.key === S.pick.t.key) || S.pick.t; // (as it is now)
  mark(); detail();
}

// ------------------------------------------------------------------ the lake's files
const DATA = /\.(parquet|pq|csv|tsv|json|jsonl|ndjson)$/i;
async function files() {
  const box = $('#files');
  let list;
  try { list = await rows(`SELECT path, size, written FROM files() ORDER BY path`); } catch (e) {
    box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to list them.' : e.message));
    return;
  }
  list = list.filter(f => !f.path.startsWith('files/notebooks/')); // (the notebooks are listed below)
  if (!list.length) { box.replaceChildren(h('div', { class: 'empty' }, 'None yet (PUT /files/… adds one).')); return; }
  const root = { dirs: new Map(), files: [] };
  for (const f of list) {
    const parts = f.path.replace(/^files\//, '').split('/');
    let d = root;
    for (const p of parts.slice(0, -1)) d = d.dirs.get(p) || d.dirs.set(p, { dirs: new Map(), files: [] }).get(p);
    d.files.push({ ...f, name: parts.at(-1), rel: parts.join('/'), key: 'file:' + f.path });
  }
  const render = (d, at) => [
    ...[...d.dirs.keys()].sort().map(n => {
      const key = 'dir:' + at + n, kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(key) }, render(d.dirs.get(n), at + n + '/'));
      const tw = twisty(key, kids);
      return h('div', { role: 'treeitem' }, line(tw, { onclick: () => tw.click() }, icon('folder'), h('span', { class: 'nm' }, n)), kids);
    }),
    ...d.files.map(f => line(null, { title: `files/${f.rel}: click for its details${DATA.test(f.name) ? ', double-click to query it' : ''}`, 'data-key': f.key, onclick: () => pick({ type: 'file', f }), ondblclick: () => DATA.test(f.name) && peek(fileSql(f)) },
      icon(DATA.test(f.name) ? 'files' : 'file'), h('span', { class: 'nm' }, f.name), h('span', { class: 'ct' }, bytes(f.size)))),
  ];
  box.replaceChildren(...render(root, ''));
  mark();
}
const bytes = n => n == null ? '' : n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(1)} KB` : n < 1073741824 ? `${(n / 1048576).toFixed(1)} MB` : `${(n / 1073741824).toFixed(2)} GB`;
/** A lake's file as SQL reads it (whoever reads the lake reads its files). */
const fileSql = f => `read_${/\.(csv|tsv)$/i.test(f.name) ? 'csv' : /\.(json|jsonl|ndjson)$/i.test(f.name) ? 'json' : 'parquet'}('${(S.filesAt + f.rel).replace(/'/g, "''")}')`;

// ------------------------------------------------------------------ the details panel
const facts = pairs => h('dl', { class: 'facts' }, pairs.filter(([, v]) => v != null && v !== '' && !(Array.isArray(v) && !v.length)).flatMap(([k, v]) => [h('dt', {}, k), h('dd', {}, Array.isArray(v) ? v.join(', ') : v)]));
const act = (ic, label, title, fn) => h('button', { class: 'btn', title, onclick: fn }, icon(ic), label);
function summary() {
  const objs = S.objects || [], by = k => objs.filter(t => t.c === home() && t.o.kind === k).length;
  const tabled = objs.filter(t => t.c === home() && t.o.rows != null);
  const s = S.info || {};
  return [h('div', { class: 'dh' }, icon('db'), h('div', {}, h('div', { class: 'dn' }, home() || 'Pondra'), h('div', { class: 'dk' }, MODE === 'lakes' ? 'a database' : 'this lake'))),
    facts([['Tables', count(by('table'))], ['Views', count(by('view') + by('files'))], ['Materialized', by('materialized view') ? count(by('materialized view')) : null],
      ['Rows in files', count(tabled.reduce((a, t) => a + (t.o.rows || 0), 0))], ['Size in files', bytes(tabled.reduce((a, t) => a + (t.o.bytes || 0), 0))],
      ['Nodes', s.nodes ? String(s.nodes.length) : null], ['This node', s.role], ['Leader', s.leader], ['Commits', s.hwm != null ? count(s.hwm) : null]]),
    h('p', { class: 'muted' }, 'Pick a table, a view or a file on the left, or a column of an answer, to see it here.')];
}
function objectDetail(t) {
  const o = t.o, [ic, word] = KIND[o.kind] || KIND.table, stored = o.kind === 'table' || o.kind === 'materialized view';
  const counted = h('span', { class: 'muted' }, '…');
  rows(`SELECT count(*) AS n FROM ${t.q}`).then(r => counted.textContent = count(r[0].n), e => counted.textContent = e.message.split('\n')[0].slice(0, 120));
  const keyed = new Set(o.key || []), nn = new Set(o.not_null || []);
  const cols = t.columns.map(c => {
    const flags = [keyed.has(c.n) ? 'key' : null, nn.has(c.n) && !keyed.has(c.n) ? 'not null' : null, o.defaults?.[c.n] ? `default ${o.defaults[c.n]}` : null].filter(Boolean);
    const box = h('div', { class: 'pc', 'data-col': c.n }, h('div', { class: 'line1' }, icon(typeIcon(c.d), 'tg'), h('span', { class: 'nm' }, c.n), keyed.has(c.n) ? icon('key', 'kk') : null, h('span', { class: 'ty' }, sqlType(c.d))),
      flags.length ? h('div', { class: 'sub' }, flags.join(' · ')) : null, h('div', { class: 'ps' }));
    return box;
  });
  const profileBtn = act('chart', 'Profile', 'Each column: nulls, distinct values, range and spread (reads the whole table)', () => profile(t, cols, profileBtn));
  return [h('div', { class: 'dh' }, icon(ic), h('div', {}, h('div', { class: 'dn' }, t.t), h('div', { class: 'dk' }, `${word} · ${t.c}.${t.s}`))),
    h('div', { class: 'acts2' }, act('play', 'Query', 'Its first rows, in a new cell (or double-click it)', () => peek(t.q)), profileBtn, act('copy', 'Name', `Copy ${t.q}`, () => navigator.clipboard?.writeText(t.q).then(() => toast(`Copied ${t.q}`)))),
    facts([['Rows', counted], ['Columns', String(t.columns.length)], ['Key', o.key], ['Partitioned by', o.partition], ['Clustered by', o.cluster],
      ['Published as', o.publish], ['Rows kept', o.ttl], ['In files', stored ? `${bytes(o.bytes)} · ${count(o.files || 0)} file${o.files === 1 ? '' : 's'}` : null]]),
    o.sql ? h('div', { class: 'dsect' }, o.kind === 'files' ? 'Reads' : 'Definition') : null, o.sql ? h('pre', { class: 'defn', html: highlight(o.sql, 'sql') }) : null,
    h('div', { class: 'dsect' }, 'Columns'), ...cols];
}
function fileDetail(f) {
  const data = DATA.test(f.name);
  return [h('div', { class: 'dh' }, icon(data ? 'files' : 'file'), h('div', {}, h('div', { class: 'dn' }, f.name), h('div', { class: 'dk' }, 'files/' + f.rel))),
    h('div', { class: 'acts2' }, data ? act('play', 'Query', 'Read it as a table, in a new cell (or double-click it)', () => peek(fileSql(f))) : null,
      act('down', 'Download', 'Download it', async () => { try { const r = await call('/files/' + f.rel.split('/').map(encodeURIComponent).join('/')); saveAs(await r.blob(), 'application/octet-stream', f.name); } catch (e) { toast(e.message, true); } })),
    facts([['Size', bytes(f.size)], ['Written', f.written ? utc(f.written).toLocaleString() : null], ['In SQL', data ? fileSql(f) : `file_read('${f.path}')`]])];
}

// ------------------------------------------------------------------ profiles: a table's (SQL) and an answer's (here)
/** A column's summary: its values, nulls, distinct values, least and greatest, and a histogram
 * (numbers, dates, times) or its commonest values (the rest). */
function summarize(values, type) {
  const vals = values.filter(v => v != null), distinct = new Set(vals.map(v => typeof v === 'object' ? JSON.stringify(v) : v));
  const s = { n: values.length, nulls: values.length - vals.length, distinct: distinct.size, exact: true };
  const at = spread(type);
  if (at) {
    const xs = vals.map(at).filter(Number.isFinite);
    if (xs.length) {
      const lo = Math.min(...xs), hi = Math.max(...xs), hist = Array(20).fill(0);
      for (const x of xs) hist[hi === lo ? 0 : Math.min(19, Math.floor((x - lo) / (hi - lo) * 20))]++;
      Object.assign(s, { min: vals[xs.indexOf(lo)], max: vals[xs.indexOf(hi)], hist });
    }
  } else if (vals.length) {
    const n = new Map();
    for (const v of vals) { const k = typeof v === 'object' ? JSON.stringify(v) : String(v); n.set(k, (n.get(k) || 0) + 1); }
    s.top = [...n].sort((a, b) => b[1] - a[1]).slice(0, 5).map(([v, c]) => ({ v, n: c }));
    if (/^(Utf8|LargeUtf8|Utf8View)/.test(type || '')) { const sorted = [...n.keys()].sort(); Object.assign(s, { min: sorted[0], max: sorted.at(-1) }); }
  }
  return s;
}
/** Where a value of this type sits on a line (a histogram's): numbers as they are, dates and times by their instant. */
const spread = t => numeric(t) ? v => Number(v) : /^(Date|Timestamp)/.test(t || '') ? v => Date.parse(/[zZ]|[+-]\d\d:?\d\d$/.test(v) ? v : String(v).replace(' ', 'T') + (String(v).length > 10 ? 'Z' : 'T00:00:00Z')) : null;
function statView(s, type) {
  const pct = n => s.n ? `${(100 * n / s.n).toFixed(n && n < s.n / 100 ? 1 : 0)}%` : '0%';
  const val = v => { const x = typeof v === 'object' ? JSON.stringify(v) : String(v); return x.length > 28 ? x.slice(0, 27) + '…' : x; };
  const nums = h('div', { class: 'nums' }, h('span', {}, `${pct(s.nulls)} null`), h('span', {}, `${s.exact ? '' : '≈ '}${count(s.distinct)} distinct`), s.min != null ? h('span', {}, `${val(s.min)} … ${val(s.max)}`) : null);
  const kids = [nums];
  if (s.hist) {
    const top = Math.max(...s.hist, 1), w = 12, g = 2;
    kids.push(h('div', { html: `<svg width="100%" height="30" preserveAspectRatio="none" viewBox="0 0 ${s.hist.length * (w + g)} 30" role="img" aria-label="histogram">${s.hist.map((c, i) => `<rect x="${i * (w + g)}" y="${30 - Math.max(c ? 2 : 0, 30 * c / top)}" width="${w}" height="${Math.max(c ? 2 : 0, 30 * c / top)}" rx="1.5" fill="var(--accent)" opacity=".75"><title>${count(c)}</title></rect>`).join('')}</svg>` }));
  } else if (s.top?.length) {
    const top = s.top[0].n;
    kids.push(h('div', { class: 'bars' }, s.top.flatMap(t => [h('span', { class: 'v', title: t.v }, t.v), h('span', {}, h('div', { class: 'b', style: `width:${Math.max(4, 100 * t.n / top)}%` })), h('span', { class: 'n' }, count(t.n))])));
  }
  return kids;
}
/** A table's columns profiled where it is: one pass for every column's counts and range, then a
 * histogram or the commonest values of each (at most 24 columns). */
async function profile(t, boxes, btn) {
  const cols = t.columns.slice(0, 24), q = ident, simple = c => !/^(List|LargeList|FixedSizeList|Struct|Map|Binary|LargeBinary|BinaryView)|\[\]$/.test(c.d);
  btn.disabled = true; btn.lastChild.textContent = 'Profiling…';
  try {
    const parts = cols.flatMap((c, i) => [`count(${q(c.n)}) AS "v${i}"`, simple(c) ? `approx_distinct(${q(c.n)}) AS "d${i}"` : `NULL AS "d${i}"`,
      simple(c) ? `CAST(min(${q(c.n)}) AS VARCHAR) AS "lo${i}"` : `NULL AS "lo${i}"`, simple(c) ? `CAST(max(${q(c.n)}) AS VARCHAR) AS "hi${i}"` : `NULL AS "hi${i}"`]);
    const [a] = await rows(`SELECT count(*) AS n, ${parts.join(', ')} FROM ${t.q}`);
    const stats = cols.map((c, i) => ({ n: Number(a.n), nulls: Number(a.n) - Number(a['v' + i]), distinct: Number(a['d' + i] ?? 0), exact: false, min: a['lo' + i], max: a['hi' + i] }));
    const show = i => { const box = boxes[i]?.querySelector('.ps'); if (box) box.replaceChildren(...statView(stats[i], cols[i].d)); };
    cols.forEach((_, i) => show(i));
    let next = 0;
    const one = async () => {
      for (let i; (i = next++) < cols.length;) {
        const c = cols[i], at = spread(c.d), s = stats[i];
        if (!simple(c) || s.n === s.nulls) continue;
        if (at && s.min != null) {
          const lo = at(s.min), hi = at(s.max);
          if (!Number.isFinite(lo) || !Number.isFinite(hi)) continue;
          const x = numeric(c.d) ? `CAST(${q(c.n)} AS DOUBLE)` : `CAST(date_part('epoch', CAST(${q(c.n)} AS TIMESTAMP)) AS DOUBLE) * 1000`;
          const b = hi === lo ? '0' : `least(19, CAST(floor((${x} - ${lo}) / ${(hi - lo) / 20}) AS BIGINT))`;
          const hist = Array(20).fill(0);
          for (const r of await rows(`SELECT ${b} AS b, count(*) AS n FROM ${t.q} WHERE ${q(c.n)} IS NOT NULL GROUP BY 1`)) hist[Math.max(0, Math.min(19, Number(r.b)))] += Number(r.n);
          s.hist = hist;
        } else if (!at) {
          s.top = (await rows(`SELECT CAST(${q(c.n)} AS VARCHAR) AS v, count(*) AS n FROM ${t.q} WHERE ${q(c.n)} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 5`)).map(r => ({ v: r.v, n: Number(r.n) }));
        }
        show(i);
      }
    };
    await Promise.all([one(), one(), one()]);
  } catch (e) { toast('Could not profile it: ' + e.message, true); }
  btn.disabled = false; btn.lastChild.textContent = 'Profile';
}
/** An answer's columns, summarized here from the rows it holds. */
function explore(r, i, cell) { pick({ type: 'result', r, i, cell }); }
function resultDetail(p) {
  const { r, i, cell } = p, times = r.columns.map(c => /^Timestamp/.test(c.type || ''));
  const out = [h('div', { class: 'dh' }, icon('chart'), h('div', {}, h('div', { class: 'dn' }, cell?.count ? `Answer [${cell.count}]` : 'Answer'), h('div', { class: 'dk' }, `${count(r.rows.length)} row${r.rows.length === 1 ? '' : 's'}${r.total > r.rows.length ? ` of ${count(r.total)} (the ones here)` : ''} · ${r.columns.length} column${r.columns.length === 1 ? '' : 's'}`))),
    h('div', { class: 'dsect' }, 'Columns')];
  r.columns.forEach((c, k) => {
    const s = summarize(r.rows.map(row => times[k] && row[k] != null ? String(row[k]).replace(/^(\d{4}-\d\d-\d\d)T/, '$1 ') : row[k]), c.type);
    const box = h('div', { class: 'pc' + (k === i ? ' on' : ''), 'data-col': c.name }, h('div', { class: 'line1' }, icon(typeIcon(c.type), 'tg'), h('span', { class: 'nm' }, c.name), h('span', { class: 'ty' }, sqlType(c.type))), h('div', { class: 'ps' }, ...statView(s, c.type)));
    out.push(box);
    if (k === i) requestAnimationFrame(() => box.scrollIntoView({ block: 'nearest' }));
  });
  return out;
}

async function stats() {
  const where = $('#where');
  try {
    const s = await (await call('/stats')).json();
    S.lake = s.lake; S.info = s;
    if (!S.pick) detail();
    const nodes = s.nodes || [], who = MODE === 'lakes' ? S.db : s.lake;
    where.innerHTML = `<span class="dot"></span><b>${esc(who || '')}</b> · ${esc(s.role)} · ${nodes.length} node${nodes.length === 1 ? '' : 's'}${s.live_queries ? ` · ${s.live_queries} live` : ''}`;
    where.title = `This database's cluster: ${nodes.join(', ')}. Its leader is ${s.leader}; commits so far: ${s.hwm}.`;
  } catch (e) {
    where.innerHTML = '<span class="dot off"></span>not reachable';
    where.title = e.message;
  }
}
async function use(db) {
  for (const c of S.cells) c.stopLive();
  S.db = db;
  await stats();
  S.pick = null;
  await Promise.all(R.sections.map(refreshSection));
  saved();
  toast(`Cells now run in database ${db}`);
}
let pending;
const later = f => { clearTimeout(pending); pending = setTimeout(f, 250); };
async function refresh() { await stats(); await Promise.all(R.sections.map(refreshSection)); emit('refresh'); }


// ------------------------------------------------------------------ the page's regions
let started = false;
/** A menu at `at` (an element or a point): items `{ label, icon?, keys?, run }`, or '-' for a line. */
function menu(at, items) {
  const m = $('#menu');
  m.replaceChildren(...items.filter(Boolean).map(i => i === '-' ? h('div', { class: 'sep' }) : h('button', { role: 'menuitem', onclick: () => { m.hidden = true; i.run(); } }, i.icon ? icon(i.icon) : null, i.label, i.keys ? h('kbd', {}, i.keys) : null)));
  m.hidden = false;
  const r = at.getBoundingClientRect ? at.getBoundingClientRect() : { left: at.x, right: at.x, bottom: at.y, top: at.y };
  m.style.top = Math.min(innerHeight - m.offsetHeight - 8, r.bottom + 4) + 'px';
  m.style.left = Math.max(8, Math.min(innerWidth - m.offsetWidth - 8, r.right - m.offsetWidth)) + 'px';
  m.querySelector('button')?.focus();
}
addEventListener('mousedown', e => { if (!e.target.closest('#menu')) $('#menu').hidden = true; });
addEventListener('keydown', e => { if (e.key === 'Escape' && !$('#menu').hidden) { $('#menu').hidden = true; e.stopPropagation(); } }, true);
function drawActions() {
  const box = $('#actions'), menued = R.actions.filter(a => a.menu);
  box.replaceChildren(...R.actions.filter(a => !a.menu && !a.hidden?.()).map(a => h('button', { class: a.label ? 'btn' + (a.primary ? ' primary' : '') : 'icon', id: a.id + 'Btn', title: a.title, 'aria-label': a.title, 'aria-pressed': a.pressed ? String(a.pressed()) : null, onclick: e => a.run(e) }, a.icon ? icon(a.icon) : null, a.label || null)),
    menued.length ? h('button', { class: 'icon', id: 'moreBtn', title: 'More', 'aria-label': 'More', onclick: e => menu(e.currentTarget, menued.filter(a => !a.hidden?.()).flatMap(a => [a.sep ? '-' : null, { label: a.title, icon: a.icon, keys: a.keys, run: a.run }])) }, icon('dots')) : null);
}
function drawSide() {
  $('#side').replaceChildren(...R.sections.flatMap(s => {
    s.box ||= h('div', { id: s.id, role: 'tree' });
    const tools = (s.tools || []).filter(t => !t.hidden?.()).map(t => h('button', { class: 'icon', title: t.title, 'aria-label': t.title, id: t.domId || null, onclick: t.run }, icon(t.icon)));
    return [h('div', { class: 'sect' }, h('h2', { id: s.id + 'Title' }, typeof s.title === 'function' ? s.title() : s.title), h('span', {}, tools)), s.box];
  }));
  return Promise.all(R.sections.filter(s => !s.drawn).map(s => { s.drawn = true; return refreshSection(s); }));
}
async function refreshSection(s) { try { await s.render(s.box); } catch (e) { s.box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to see this.' : e.message)); } }
function drawTabs() {
  if (!R.panels.some(p => p.id === S.tab)) S.tab = R.panels[0]?.id;
  $('#tabs').replaceChildren(...R.panels.map(p => h('button', { class: 'tab', role: 'tab', 'aria-selected': String(p.id === S.tab), onclick: () => { S.tab = p.id; drawTabs(); detail(); } }, p.title)));
  $('#tabs').hidden = R.panels.length < 2;
}
function drawRail() {
  $('#rail').hidden = !R.nav.length;
  $('#rail').replaceChildren(...R.nav.map(n => h('button', { class: 'icon' + (S.place === n.id ? ' on' : ''), title: n.label, 'aria-label': n.label, onclick: () => { S.place = n.id; drawRail(); n.run(); }, html: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round">${n.icon || ICONS.dots}</svg><span>${esc(n.label)}</span>` })));
}
function drawKeys() {
  const groups = [...new Set(R.keys.map(k => k.group))];
  $('#keys').replaceChildren(...groups.flatMap(g => [h('h4', {}, g), ...R.keys.filter(k => k.group === g).flatMap(k => [h('span', {}, ...k.keys.split(' ').map(x => h('kbd', {}, x))), h('span', {}, k.title)])]));
}
/** The details panel: the current tab's view of what was picked. */
function detail() {
  const box = $('#detail');
  if ($('#panel').hidden) return;
  const p = R.panels.find(x => x.id === S.tab);
  if (!p) return box.replaceChildren();
  box.replaceChildren();
  const n = ++detailed; // (a slow answer for an earlier pick never covers a later one's)
  Promise.resolve(p.render(box, S.pick)).then(kids => { if (n === detailed && Array.isArray(kids)) box.replaceChildren(...kids.filter(Boolean)); }, e => { if (n === detailed) box.replaceChildren(h('pre', { class: 'err' }, e.message)); });
}
let detailed = 0;
function panel(open) {
  $('#panel').hidden = !open;
  store.set('pondra.panel', open ? 'open' : 'closed');
  drawActions();
  if (open) detail();
}
function pick(p, tab) {
  S.pick = p; mark();
  if (tab) { S.tab = tab; drawTabs(); }
  else if (S.tab !== 'details' && R.panels.some(x => x.id === 'details')) { S.tab = 'details'; drawTabs(); }
  if ($('#panel').hidden) panel(true); else detail();
  emit('pick', p);
}
function mark() {
  const key = S.pick?.type === 'object' ? S.pick.t.key : S.pick?.type === 'file' ? S.pick.f.key : null;
  document.querySelectorAll('#side .row.on').forEach(r => r.classList.remove('on'));
  if (key) document.querySelectorAll(`#side .row[data-key="${CSS.escape(key)}"]`).forEach(r => r.classList.add('on'));
}

// ------------------------------------------------------------------ the page's Python (a session's, on the node)
/** Where this page's Python is: none yet, idle, busy. The pill says so, and restarts it. */
function kernel(state) {
  if (state) S.py = state;
  const k = $('#kernel');
  k.hidden = S.py === 'none' && !S.cells.some(c => c.kind === 'python');
  k.replaceChildren(h('span', { class: 'dot ' + (S.py === 'busy' ? 'busy' : S.py === 'idle' ? '' : 'idle') }), 'Python ', h('b', {}, S.py === 'none' ? 'not started' : S.py));
  k.title = 'This page\'s Python, on the node: its cells share their variables. Click for Restart and Variables';
}
async function restart() {
  try { await call(`/sessions/${SESSION}/python`, { method: 'DELETE' }); } catch (e) { toast(e.message, true); return; }
  kernel('none'); S.vars = []; toast('Python restarted: its variables are gone'); if (S.tab === 'variables') detail();
}

// ------------------------------------------------------------------ outline and variables
function outline(box) {
  const heads = S.cells.flatMap(c => c.kind !== 'markdown' ? [] : [...c.src.matchAll(/^(#{1,3})\s+(.+)$/gm)].map(m => ({ c, level: m[1].length, text: m[2].replace(/[*_`]/g, '') })));
  box.replaceChildren(...heads.length ? heads.map(x => line(null, { class: 'row h' + x.level, title: x.text, onclick: () => { select(x.c, true); x.c.el.scrollIntoView({ block: 'start', behavior: 'smooth' }); } }, h('span', { class: 'nm' }, x.text)))
    : [h('div', { class: 'empty' }, 'Headings of text cells (# …) show here.')]);
}
let outlined = 0;
const reoutline = (ms = 150) => { clearTimeout(outlined); outlined = setTimeout(() => { const s = R.sections.find(x => x.id === 'outline'); if (s?.box) outline(s.box); }, ms); };
/** The page's Python variables, as its kernel holds them now (for the tab, and for completion). */
async function readVars() { const v = await (await call(`/sessions/${SESSION}/python`)).json(); S.vars = v.variables || []; return v; }
async function variables(box) {
  box.replaceChildren(h('div', { class: 'muted' }, 'Reading…'));
  let v;
  try { v = await readVars(); } catch (e) { return [h('pre', { class: 'err' }, e.message)]; }
  const head = h('div', { class: 'dh' }, icon('var'), h('div', {}, h('div', { class: 'dn' }, 'Variables'), h('div', { class: 'dk' }, v.busy ? 'a cell is running: they show when it is done' : v.running ? `${S.vars.length} in this page's Python` : 'no Python yet: a Python cell starts it')));
  const acts = h('div', { class: 'acts2' }, act('restart', 'Restart', 'Stop this page\'s Python: its variables go (its temporary tables stay)', restart), act('refresh', 'Refresh', 'Read them again', () => detail()));
  return [head, acts, ...S.vars.map(x => h('div', { class: 'var' }, h('div', { class: 'line1' }, h('span', { class: 'nm' }, x.name), h('span', { class: 'ty' }, x.type + (x.size ? ` · ${x.size}` : ''))), h('div', { class: 'look' }, x.look)))];
}

// ------------------------------------------------------------------ completion
const FUNCS = 'abs avg ceil coalesce concat count date_bin date_part date_trunc extract floor greatest least length lower ltrim max min now nullif regexp_replace replace round row_number rank dense_rank lag lead first_value last_value split_part stddev strpos substr sum to_char to_date to_timestamp trim upper approx_distinct approx_percentile_cont median array_agg string_agg json_get json_get_str cosine_distance read_parquet read_csv read_json files file_read range generate_series'.split(' ');
let cm = null; // (the completion open now: its cell, where the word starts, the choices, the one on)
/** Names that complete what is typed before the caret: the tables and columns the lake has (those
 * of the tables the cell names first), SQL's words and functions; in Python, the page's variables. */
function complete(c, force) {
  const ta = c.ta, at = ta.selectionStart, before = ta.value.slice(0, at), m = before.match(/[\w.$"]*$/), word = m[0].replace(/"/g, '');
  if (!word && !force) return false;
  const low = word.toLowerCase(), seen = new Set(), all = [];
  const push = (text, ty, rank) => { if (!seen.has(text) && text.toLowerCase().startsWith(low) && text.toLowerCase() !== low) { seen.add(text); all.push({ text, ty, rank }); } };
  if (c.kind === 'sql') {
    const named = (S.objects || []).filter(t => new RegExp(`\\b${t.t.replace(/[^\w]/g, '')}\\b`, 'i').test(ta.value));
    for (const t of named) for (const col of t.columns) push(ident(col.n), sqlType(col.d), 0);
    for (const t of S.objects || []) push(t.q, t.o.kind, 1);
    for (const t of S.objects || []) for (const col of t.columns) push(ident(col.n), sqlType(col.d), 2);
    for (const f of FUNCS) push(f + '(', 'function', 3);
    for (const k of SQL_KW) push(/[a-z]/.test(word) ? k.toLowerCase() : k, '', 4);
  } else if (c.kind === 'python') {
    for (const v of S.vars || []) push(v.name, v.type, 0);
    for (const x of ['db.sql(', 'db.table(', 'db.tables()', 'db.insert(', 'pondra.col(', 'print(']) push(x, '', 1);
    for (const k of PY_KW) push(k, '', 2);
  } else return false;
  all.sort((a, b) => a.rank - b.rank || a.text.length - b.text.length || a.text.localeCompare(b.text));
  if (!all.length) { closeComplete(); return false; }
  cm = { c, from: at - m[0].length, list: all.slice(0, 50), on: 0 };
  drawComplete();
  return true;
}
function drawComplete() {
  const box = $('#complete'), { c, list, on, from } = cm;
  box.replaceChildren(...list.map((x, i) => h('div', { class: i === on ? 'on' : null, role: 'option', onmousedown: e => { e.preventDefault(); cm.on = i; accept(); } }, h('span', {}, x.text), x.ty ? h('span', { class: 'ty' }, x.ty) : null)));
  const ta = c.ta, style = getComputedStyle(ta), lineH = parseFloat(style.lineHeight), before = ta.value.slice(0, from), row = before.split('\n').length - 1, col = before.length - before.lastIndexOf('\n') - 1;
  measurer ||= document.createElement('canvas').getContext('2d'); measurer.font = style.font;
  const r = ta.getBoundingClientRect(), x = r.left + parseFloat(style.paddingLeft) + col * measurer.measureText('0').width - ta.scrollLeft, y = r.top + parseFloat(style.paddingTop) + (row + 1) * lineH;
  box.hidden = false;
  box.style.left = Math.min(innerWidth - box.offsetWidth - 8, x) + 'px';
  box.style.top = (y + box.offsetHeight > innerHeight - 8 ? y - lineH - box.offsetHeight : y + 2) + 'px';
  box.children[on]?.scrollIntoView({ block: 'nearest' });
}
function accept() {
  const { c, from, list, on } = cm, ta = c.ta;
  ta.setSelectionRange(from, ta.selectionStart);
  insert(ta, list[on].text);
  closeComplete();
}
function closeComplete() { cm = null; $('#complete').hidden = true; }

// ------------------------------------------------------------------ tokens
function askToken(why) {
  const d = $('#tokenDlg');
  if (d.open) return;
  $('#tokenWhy').textContent = `${why}. The token is kept in this browser only.`;
  $('#tokenIn').value = store.get('pondra.token') || '';
  d.returnValue = '';
  d.showModal();
}
$('#tokenDlg').addEventListener('close', () => {
  const v = $('#tokenDlg').returnValue;
  if (v === 'ok' && $('#tokenIn').value.trim()) store.set('pondra.token', $('#tokenIn').value.trim());
  else if (v === 'clear') store.set('pondra.token', null);
  else return;
  refresh();
});

// ------------------------------------------------------------------ what the core registers (as an extension would)
function upload() {
  const input = h('input', { type: 'file', accept: '.ipynb,application/json', hidden: true });
  input.onchange = async () => {
    const f = input.files[0];
    input.remove();
    if (!f || (S.dirty && !confirm('Open another notebook? This one has changes that are not saved.'))) return;
    try {
      load(JSON.parse(await f.text()), cleanName(f.name) || 'uploaded', null);
      changed();
      toast(`Opened ${f.name}: Ctrl+S keeps it in the lake`);
    } catch (err) { toast(`Could not open ${f.name}: ${err.message}`, true); }
  };
  document.body.append(input); input.click();
}
const fresh = () => { if (!S.dirty || confirm('Start a new notebook? This one has changes that are not saved.')) blank(); };
function clearOutputs() { for (const c of S.cells) { c.stopLive(); c.result = null; c.count = null; c.num.textContent = ''; c.out.replaceChildren(); c.status.textContent = ''; } changed(); }
async function runSome(from, to) {
  for (const c of S.cells.slice(from, to)) {
    select(c, true);
    const r = await c.run();
    if (r?.kind === 'error') { toast('Stopped at a cell that failed', true); return; }
  }
}
function core() {
  register.cellKind({ id: 'sql', label: 'SQL', language: 'sql', placeholder: 'SELECT …', live: true, run: (text, signal) => run(text, signal) });
  register.cellKind({ id: 'python', label: 'Python', language: 'python', placeholder: 'db.sql("SELECT …")      # runs on the node; cells share variables', run: async (text, signal) => { kernel('busy'); try { return await run(doBlock(text), signal); } finally { kernel('idle'); readVars().then(() => S.tab === 'variables' && !$('#panel').hidden && detail(), () => {}); } } });
  register.cellKind({ id: 'markdown', label: 'Text', language: 'markdown', placeholder: 'Text, in Markdown' });
  register.renderer({ id: 'error', order: 10, match: r => r.kind === 'error', render: r => h('pre', { class: 'err' }, r.message) });
  register.renderer({ id: 'rows', order: 20, match: r => r.kind === 'rows', render: (r, cell) => grid(r, cell) });
  register.renderer({ id: 'figures', order: 30, match: r => r.kind === 'done' && Array.isArray(r.value?.images), render: r => h('div', {}, r.value.images.map(b => h('img', { class: 'fig', alt: 'a figure the cell drew', src: 'data:image/png;base64,' + b }))) });
  register.renderer({ id: 'text', order: 40, match: r => r.kind === 'text', render: r => h('pre', { class: 'said' }, r.text) });
  register.renderer({ id: 'done', order: 90, match: r => r.kind === 'done', render: r => { const d = doneText(r.value) || (r.notices?.length ? '' : 'Done.'); return d && !(d === 'Done.' && r.notices?.length) ? h('div', { class: 'done' }, d) : null; } });
  register.section({ id: 'tree', order: 10, title: () => MODE === 'lakes' ? 'Databases' : 'Database', render: () => tree(), tools: [
    { icon: 'plus', title: 'New database', domId: 'newdb', hidden: () => MODE !== 'lakes', run: newDatabase },
    { icon: 'refresh', title: 'Refresh', domId: 'refresh', run: () => refresh() }] });
  register.section({ id: 'files', order: 20, title: 'Files', render: () => files() });
  register.section({ id: 'outline', order: 30, title: 'Outline', render: box => outline(box) });
  register.section({ id: 'notebooks', order: 40, title: 'Notebooks', render: () => notebooks(), tools: [{ icon: 'plus', title: 'New notebook', domId: 'newnb', run: fresh }] });
  register.panel({ id: 'details', order: 10, title: 'Details', render: (box, p) => p?.type === 'object' ? objectDetail(p.t) : p?.type === 'file' ? fileDetail(p.f) : p?.type === 'result' ? resultDetail(p) : summary() });
  register.panel({ id: 'variables', order: 20, title: 'Variables', render: box => variables(box) });
  register.action({ id: 'runall', order: 10, label: 'Run all', primary: true, title: 'Run every cell, in order (Ctrl+Shift+Enter)', run: () => runSome(0) });
  register.action({ id: 'save', order: 20, label: 'Save', title: 'Save to the lake, as a new version (Ctrl+S)', run: save });
  register.action({ id: 'panel', order: 80, icon: 'panel', title: 'Details panel', pressed: () => !$('#panel').hidden, run: () => panel($('#panel').hidden) });
  register.action({ id: 'new', order: 100, menu: true, icon: 'plus', title: 'New notebook', run: fresh });
  register.action({ id: 'download', order: 110, menu: true, icon: 'down', title: 'Download as .ipynb', run: () => saveAs(JSON.stringify(notebook(), null, 1) + '\n', 'application/x-ipynb+json', (cleanName(S.name) || 'notebook') + '.ipynb') });
  register.action({ id: 'upload', order: 120, menu: true, icon: 'up', title: 'Open an .ipynb…', run: upload });
  register.action({ id: 'clear', order: 130, menu: true, sep: true, icon: 'clear', title: 'Clear every output', run: clearOutputs });
  register.action({ id: 'restart', order: 140, menu: true, icon: 'restart', title: 'Restart Python', keys: '0 0', run: restart });
  register.action({ id: 'token', order: 150, menu: true, sep: true, icon: 'key', title: 'Token…', run: () => askToken('The token this node was started with') });
  register.action({ id: 'keys', order: 160, menu: true, icon: 'keyboard', title: 'Keys', keys: '?', run: () => $('#helpDlg').showModal() });
  const cellKey = (keys, title, fn) => register.key({ keys, title, run: fn, group: 'On a cell (after Esc)' });
  for (const [keys, title] of [['Ctrl Enter', 'Run it'], ['Shift Enter', 'Run it and go to the next cell'], ['Alt Enter', 'Run it and add a cell below'], ['Ctrl Shift Enter', 'Run every cell'], ['Tab', 'Complete a name (or indent)'], ['Ctrl Space', 'Complete a name'], ['Esc', 'Leave the cell: the keys below then work']]) register.key({ keys, title, group: 'In a cell' });
  cellKey('Enter', 'Edit it', c => c.edit());
  cellKey('↑ ↓', 'The cell above, below (or K J)');
  cellKey('A B', 'Add a cell above, below');
  cellKey('D D', 'Delete it (Z brings it back)');
  cellKey('S P M', 'Make it SQL, Python, text');
  cellKey('L', 'Live on or off: its answer again after each commit that changes it');
  cellKey('O', 'Hide or show its output');
  cellKey('0 0', 'Restart Python: its variables go');
  register.key({ keys: 'Ctrl S', title: 'Save the notebook in the lake', group: 'Anywhere' });
  register.key({ keys: '?', title: 'These keys', group: 'Anywhere' });
}
async function newDatabase() {
  const name = (prompt('A name for the new database (letters, digits and _):') || '').trim().toLowerCase();
  if (!name) return;
  try {
    await call('/databases', { method: 'POST', body: JSON.stringify({ name }), headers: { 'content-type': 'application/json' }, root: true });
    await use(name);
  } catch (e) { toast(e.message, true); }
}

// ------------------------------------------------------------------ the page
const pondra = {
  state: S, session: SESSION, mode: MODE,
  api: { call, run, rows, base, sql: run },
  ui: { h, icon, icons: ICONS, line, button: act, toast, menu, pick, detail, panel, refresh: () => refresh(), add: o => add(o), cells: () => S.cells.slice(),
    notebook: () => notebook(), open: (nb, name) => { load(nb, cleanName(name || '') || 'untitled', null); changed(); } },
  register, on, emit, configure,
};
window.pondra = pondra;
export { pondra };

$('#nbname').addEventListener('change', e => { S.name = cleanName(e.target.value) || 'untitled'; e.target.value = S.name; S.version = null; changed(); });
$('#nbname').addEventListener('keydown', e => { if (e.key === 'Enter') e.target.blur(); });
$('#kernel').onclick = e => menu(e.currentTarget, [{ label: 'Variables', icon: 'var', run: () => pick(S.pick, 'variables') }, { label: 'Restart Python', icon: 'restart', keys: '0 0', run: restart }]);
document.querySelectorAll('[data-add]').forEach(b => b.onclick = () => add({ kind: b.dataset.add }).edit());
addEventListener('beforeunload', e => { if (S.dirty && S.cells.some(c => c.src.trim())) { e.preventDefault(); e.returnValue = ''; } });
addEventListener('pagehide', () => {
  const token = T.token();
  T.fetch(base() + '/sessions/' + SESSION, { method: 'DELETE', keepalive: true, headers: { ...T.headers(), ...(token ? { authorization: 'Bearer ' + token } : {}) } }).catch(() => {}); // (this page's temporary tables, and its Python)
});

async function start() {
  const hash = new URLSearchParams(location.hash.slice(1));
  if (MODE === 'lakes') S.db = hash.get('db');
  core();
  drawActions(); drawTabs(); drawRail(); drawKeys(); kernel();
  blank();
  $('#panel').hidden = !(store.get('pondra.panel') ? store.get('pondra.panel') === 'open' : innerWidth >= 1180);
  // (extensions, loaded after this module, register meanwhile: drawn with the core's from here on)
  if (MODE === 'lakes') { await drawSide(); await stats(); } // (the tree picks the database /stats is asked of)
  else { await stats(); drawSide(); }                        // (the tree puts the node's own lake first)
  started = true;
  drawActions(); drawTabs(); drawRail(); drawKeys(); drawSide();
  const wanted = hash.get('notebook');
  if (wanted) {
    const vs = await rows(`SELECT path FROM files('notebooks/${wanted.replace(/'/g, "''")}/') ORDER BY path DESC LIMIT 1`).catch(() => []);
    const v = vs[0]?.path.match(/\/([^/]+)\.ipynb$/);
    if (v) await openSaved(wanted, v[1]);
  }
  setInterval(() => { if (document.visibilityState === 'visible') stats(); }, 15000);
  emit('start', pondra);
}
start();
