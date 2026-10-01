// The console's core (ADR-030, ADR-032, ADR-034): its API — registries and events — its state, and
// how it reaches the node. The other modules (editor, grid, notebook, files) and the shell
// (console.js) are built on this one, as an extension is.

// ------------------------------------------------------------------ small things
export const $ = (s, el = document) => el.querySelector(s);
export function h(tag, attrs = {}, ...kids) {
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
/** The rarer parts' style sheet (more.css), once: what more.js, chart.js and plan.js draw with,
 * loaded with them (their first `await`), so the page's first load doesn't carry it. */
let moreCss;
export const moreStyle = () => moreCss ||= new Promise(done => document.head.append(h('link', { rel: 'stylesheet', href: new URL('more.css', import.meta.url).href, onload: done, onerror: done })));
/** `el`'s children replaced by `kids`, leaving out the null and false ones (as `h` does). */
export const fill = (el, ...kids) => { el.replaceChildren(...kids.flat().filter(k => k != null && k !== false)); return el; };
export const esc = s => String(s).replace(/[&<>"']/g, c => `&#${c.charCodeAt(0)};`);
export const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { v == null ? localStorage.removeItem(k) : localStorage.setItem(k, v); } catch { /* (private windows) */ } },
  json(k, d) { try { return JSON.parse(store.get(k)) ?? d; } catch { return d; } },
};
export const secs = ms => ms < 1000 ? `${Math.round(ms)} ms` : ms < 60000 ? `${(ms / 1000).toFixed(ms < 10000 ? 2 : 1)} s` : `${Math.floor(ms / 60000)} min ${Math.round(ms % 60000 / 1000)} s`;
export const count = n => Number(n).toLocaleString('en-US');
export const utc = t => new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(t) ? t : t + 'Z');
export function ago(t) {
  const s = (Date.now() - utc(t)) / 1000;
  return s < 60 ? 'now' : s < 3600 ? `${Math.floor(s / 60)} min` : s < 86400 ? `${Math.floor(s / 3600)} h` : s < 86400 * 30 ? `${Math.floor(s / 86400)} d` : utc(t).toISOString().slice(0, 10);
}
export const bytes = n => n == null ? '' : n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(1)} KB` : n < 1073741824 ? `${(n / 1048576).toFixed(1)} MB` : `${(n / 1073741824).toFixed(2)} GB`;
export const VERSION = document.documentElement.dataset.version || '';
export function saveAs(data, type, name) {
  const a = h('a', { href: URL.createObjectURL(data instanceof Blob ? data : new Blob([data], { type })), download: name });
  document.body.append(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 2000);
}

// ------------------------------------------------------------------ icons (24 grid, stroked)
export const ICONS = {
  chev: '<path d="m9.5 6.5 5.5 5.5-5.5 5.5"/>',
  chevd: '<path d="m6.5 9.5 5.5 5.5 5.5-5.5"/>',
  db: '<ellipse cx="12" cy="6" rx="7" ry="3"/><path d="M5 6v12c0 1.66 3.13 3 7 3s7-1.34 7-3V6"/><path d="M5 12c0 1.66 3.13 3 7 3s7-1.34 7-3"/>',
  schema: '<path d="M12 3.5 3.5 8 12 12.5 20.5 8z"/><path d="m3.5 12 8.5 4.5 8.5-4.5M3.5 16l8.5 4.5 8.5-4.5"/>',
  table: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M3.5 9.5h17M3.5 14.5h17M9.5 9.5v10"/>',
  view: '<rect x="3.5" y="4.5" width="17" height="15" rx="2" stroke-dasharray="3 2.2"/><path d="M3.5 9.5h17"/><circle cx="12" cy="14.5" r="2"/>',
  matview: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M3.5 9.5h17M9.5 9.5v10"/><path d="m15.5 11.5-2 3h3l-2 3"/>',
  files: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M3.5 9.5h17M9.5 9.5v10"/><path d="M13 15h4.5"/>',
  folder: '<path d="M3.5 7.5A2.5 2.5 0 0 1 6 5h3.5l2 2H18a2.5 2.5 0 0 1 2.5 2.5v7A2.5 2.5 0 0 1 18 19H6a2.5 2.5 0 0 1-2.5-2.5z"/>',
  file: '<path d="M14 3H7.5A2.5 2.5 0 0 0 5 5.5v13A2.5 2.5 0 0 0 7.5 21h9a2.5 2.5 0 0 0 2.5-2.5V8z"/><path d="M14 3v5h5"/>',
  filesql: '<path d="M14 3H7.5A2.5 2.5 0 0 0 5 5.5v13A2.5 2.5 0 0 0 7.5 21h9a2.5 2.5 0 0 0 2.5-2.5V8z"/><path d="M14 3v5h5M8.5 13h7M8.5 16.5h5"/>',
  filepy: '<path d="M14 3H7.5A2.5 2.5 0 0 0 5 5.5v13A2.5 2.5 0 0 0 7.5 21h9a2.5 2.5 0 0 0 2.5-2.5V8z"/><path d="M14 3v5h5M9.5 12.5l-2 2 2 2M14.5 12.5l2 2-2 2"/>',
  filedata: '<path d="M14 3H7.5A2.5 2.5 0 0 0 5 5.5v13A2.5 2.5 0 0 0 7.5 21h9a2.5 2.5 0 0 0 2.5-2.5V8z"/><path d="M14 3v5h5M8.5 12.5h7M8.5 16h7M12 11v7"/>',
  notebook: '<rect x="5" y="3" width="14" height="18" rx="2.5"/><path d="M9 3v18M12.5 8h3.5M12.5 12h3.5"/>',
  hash: '<path d="M5 9h14M5 15h14M10.5 4 8.5 20M15.5 4l-2 16"/>',
  clock: '<circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2"/>',
  key: '<circle cx="8" cy="15" r="3.5"/><path d="m10.5 12.5 8-8M16 7l2.5 2.5"/>',
  play: '<path d="M8 5.5v13l10.5-6.5z" fill="currentColor" stroke="none"/>',
  stop: '<rect x="6.5" y="6.5" width="11" height="11" rx="2"/>',
  down: '<path d="M12 4.5v10M7.5 10.5 12 15l4.5-4.5M5 19.5h14"/>',
  up: '<path d="M12 15.5v-11M7.5 9 12 4.5 16.5 9M5 19.5h14"/>',
  copy: '<rect x="8.5" y="8.5" width="11" height="11" rx="2"/><path d="M15.5 8.5V6.5a2 2 0 0 0-2-2h-7a2 2 0 0 0-2 2v7a2 2 0 0 0 2 2h2"/>',
  chart: '<path d="M4 19.5h16M7 16v-5M12 16V7M17 16v-8"/>',
  plan: '<rect x="3.5" y="3.5" width="6" height="6" rx="1.5"/><rect x="14.5" y="14.5" width="6" height="6" rx="1.5"/><path d="M6.5 9.5v5a3 3 0 0 0 3 3h5"/>',
  dots: '<circle cx="5.5" cy="12" r="1.4" fill="currentColor" stroke="none"/><circle cx="12" cy="12" r="1.4" fill="currentColor" stroke="none"/><circle cx="18.5" cy="12" r="1.4" fill="currentColor" stroke="none"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  close: '<path d="M7 7l10 10M17 7 7 17"/>',
  refresh: '<path d="M4 12a8 8 0 1 0 2.4-5.7L4 8.5"/><path d="M4 4v4.5h4.5"/>',
  restart: '<path d="M4 12a8 8 0 1 0 2.4-5.7L4 8.5"/><path d="M4 4v4.5h4.5"/>',
  search: '<circle cx="11" cy="11" r="6.5"/><path d="m20 20-4.2-4.2"/>',
  settings: '<circle cx="12" cy="12" r="3"/><circle cx="12" cy="12" r="6.5"/><path d="M12 2.5v3M12 18.5v3M2.5 12h3M18.5 12h3M5.3 5.3l2.1 2.1M16.6 16.6l2.1 2.1M5.3 18.7l2.1-2.1M16.6 7.4l2.1-2.1"/>',
  check: '<path d="m5 12.5 4.5 4.5L19 7.5"/>',
  user: '<circle cx="12" cy="8.5" r="3.5"/><path d="M5 19.5c1.2-3.3 3.9-5 7-5s5.8 1.7 7 5"/>',
  save: '<path d="M5.5 4.5h10l3 3v10a2 2 0 0 1-2 2h-11a2 2 0 0 1-2-2v-11a2 2 0 0 1 2-2z"/><path d="M8.5 4.5v4h6v-4M8 19.5v-5h8v5"/>',
  filter: '<path d="M4 5.5h16l-6 7.5v5l-4 1.5v-6.5z"/>',
  format: '<path d="M4 6h16M4 10h10M4 14h16M4 18h10"/>',
  paneL: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M9.5 4.5v15"/>',
  paneL_on: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M5.5 6.5h3v11h-3z" fill="currentColor" stroke="none"/><path d="M9.5 4.5v15"/>',
  paneB: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M3.5 13.5h17"/>',
  paneB_on: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M5.5 15h13v2.5h-13z" fill="currentColor" stroke="none"/><path d="M3.5 13.5h17"/>',
  paneR: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M14.5 4.5v15"/>',
  paneR_on: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M15.5 6.5h3v11h-3z" fill="currentColor" stroke="none"/><path d="M14.5 4.5v15"/>',
  panelRight: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M13.5 4.5v15"/>',
  panelBelow: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="M3.5 13.5h17"/>',
  terminal: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><path d="m7.5 9.5 3 2.5-3 2.5M12.5 15h4"/>',
  var: '<path d="M7 4C5 4 4.5 5 4.5 7v3c0 1-.7 2-2 2 1.3 0 2 1 2 2v3c0 2 .5 3 2.5 3M17 4c2 0 2.5 1 2.5 3v3c0 1 .7 2 2 2-1.3 0-2 1-2 2v3c0 2-.5 3-2.5 3M9 9l6 6M15 9l-6 6"/>',
  arrowUp: '<path d="M12 19V5m-6 6 6-6 6 6"/>',
  arrowDown: '<path d="M12 5v14m-6-6 6 6 6-6"/>',
  sortUp: '<path d="M12 18V6m-5 5 5-5 5 5"/>',
  sortDown: '<path d="M12 6v12m-5-5 5 5 5-5"/>',
  sort: '<path d="M8 5v14M4.5 15.5 8 19l3.5-3.5M16 19V5m-3.5 3.5L16 5l3.5 3.5"/>',
  trash: '<path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13"/>',
  clear: '<path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13"/>',
  fn: '<path d="M15.5 4.5c-2.2-.5-3.4.6-3.8 2.6L9.6 17c-.4 2-1.6 3-3.6 2.6M8.5 10.5h7"/>',
  columns: '<rect x="4" y="4" width="16" height="16" rx="2.5"/><path d="M4 9h16"/><path d="M7.5 16.5l2.5-3 2.5 2 4-4.5"/>',
  calendar: '<rect x="3.5" y="5" width="17" height="15" rx="2.5"/><path d="M3.5 10h17M8.5 3v4M15.5 3v4M8 14h3"/>',
  pin: '<path d="M9.5 3.5h5l-.8 5.2 3.3 3.3v1.5H7v-1.5l3.3-3.3zM12 13.5v7"/>',
  eye: '<path d="M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/>',
  keyboard: '<rect x="2" y="6" width="20" height="13" rx="2"/><path d="M6 10h.01M10 10h.01M14 10h.01M18 10h.01M7 15h10"/>',
  pencil: '<path d="M4 20h4L19 9l-4-4L4 16z"/><path d="m13.5 6.5 4 4"/>',
  moveSide: '<path d="M4 12h16M14 6l6 6-6 6"/>',
  t_text: '<path d="M5.5 7.5v-2h13v2M12 5.5v13M9.5 18.5h5"/>',
  t_num: '<path d="M5 9h14M5 15h14M10.5 4 8.5 20M15.5 4l-2 16"/>',
  t_dec: '<circle cx="5" cy="17.5" r="1.4" fill="currentColor" stroke="none"/><rect x="8.5" y="6" width="6" height="12" rx="3"/><path d="m17.5 8 2.5-2v12"/>',
  t_date: '<rect x="4" y="5.5" width="16" height="14" rx="2"/><path d="M4 10h16M8.5 3.5v4M15.5 3.5v4"/>',
  t_time: '<circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2"/>',
  t_bool: '<rect x="3.5" y="7.5" width="17" height="9" rx="4.5"/><circle cx="15.5" cy="12" r="2.2" fill="currentColor" stroke="none"/>',
  t_json: '<path d="M9 4.5C7 4.5 6.5 5.5 6.5 7v2.5c0 1.2-.8 2-2 2.5 1.2.5 2 1.3 2 2.5V17c0 1.5.5 2.5 2.5 2.5M15 4.5c2 0 2.5 1 2.5 2.5v2.5c0 1.2.8 2 2 2.5-1.2.5-2 1.3-2 2.5V17c0 1.5-.5 2.5-2.5 2.5"/>',
  t_list: '<path d="M8.5 4H5v16h3.5M15.5 4H19v16h-3.5"/>',
  t_bin: '<rect x="4" y="5" width="6" height="14" rx="3"/><path d="M14.5 7.5 17.5 5v14"/>',
  t_other: '<circle cx="12" cy="12" r="3.5"/>',
};
/** An icon: `<span class=ic>` holding the SVG (the class sizes and colours it). */
export const icon = (name, cls = 'ic', size = { tg: 13, kk: 12 }[cls] || 16) => h('span', { class: cls, 'aria-hidden': 'true', html: svg(name, size) });
export const svg = (name, size = 16, sw = 1.7) => `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="${sw}" stroke-linecap="round" stroke-linejoin="round">${ICONS[name] || ICONS.t_other}</svg>`;
/** A column type's kind — text, number, decimal, date, time, true/false, JSON-like, list, bytes —
 * and its icon, coloured by the kind (`--t-text`, …). */
export function typeKind(t = '') {
  return /^(U?Int)/.test(t) ? 'num' : /^(Float|Decimal)/.test(t) ? 'dec' : /^(Utf8|LargeUtf8|Utf8View|Dictionary)/.test(t) ? 'text' : /^Date/.test(t) ? 'date'
    : /^(Timestamp|Time|Duration|Interval)/.test(t) ? 'time' : /^Boolean/.test(t) ? 'bool' : /\[\]$|^(Large|FixedSize)?List/.test(t) ? 'list' : /^(Struct|Map)/.test(t) ? 'json' : /Binary/.test(t) ? 'bin' : 'other';
}
export const typeIcon = t => 't_' + typeKind(t);
export const typeMark = t => h('span', { class: 'ty-i k-' + typeKind(t), 'aria-hidden': 'true', html: svg(typeIcon(t), 13, 2) });

/** Arrow's name for a type, as SQL names it. */
export function sqlType(t = '') {
  const b = (t.match(/^\w+/) || [t])[0];
  if (t.endsWith('[]')) return sqlType(t.slice(0, -2)) + '[]';
  if (b === 'Timestamp') return /,/.test(t) && !/None\)/.test(t) ? 'TIMESTAMPTZ' : 'TIMESTAMP';
  if (/^Decimal/.test(b)) { const p = t.match(/\((\d+),\s*(-?\d+)\)/); return p ? `DECIMAL(${p[1]},${p[2]})` : 'DECIMAL'; }
  if (/^(Large|FixedSize)?List/.test(b)) { const i = t.match(/\((?:nullable |non-null )?(?:Field \{[^}]*data_type: )?(\w+[^,)]*)/); return (i ? sqlType(i[1]) : '') + '[]'; }
  const m = { Int8: 'TINYINT', Int16: 'SMALLINT', Int32: 'INT', Int64: 'BIGINT', UInt8: 'UTINYINT', UInt16: 'USMALLINT', UInt32: 'UINT', UInt64: 'UBIGINT', Float16: 'HALF', Float32: 'REAL', Float64: 'DOUBLE', Utf8: 'VARCHAR', LargeUtf8: 'VARCHAR', Utf8View: 'VARCHAR', Boolean: 'BOOLEAN', Date32: 'DATE', Date64: 'DATE', Binary: 'BYTEA', LargeBinary: 'BYTEA', BinaryView: 'BYTEA', FixedSizeBinary: 'BYTEA', Null: 'NULL', Time32: 'TIME', Time64: 'TIME', Interval: 'INTERVAL', Duration: 'INTERVAL', Struct: 'STRUCT', Map: 'MAP', Dictionary: 'VARCHAR' };
  return m[b] || t;
}
export const numeric = t => /^(U?Int|Float|Decimal)/.test(t || '');

// ------------------------------------------------------------------ the API: registries and events
const hooks = new Map();
/** Call `fn` on an event: 'start', 'pick' (what the details show), 'run' (a cell or file starts),
 * 'ran' (it answered), 'refresh' (the catalog was read again), 'changed' (a document), 'open'
 * and 'close' (a document's tab), 'active' (the document in front). */
export function on(event, fn) { (hooks.get(event) || hooks.set(event, []).get(event)).push(fn); }
export function emit(event, ...args) { for (const fn of hooks.get(event) || []) { try { fn(...args); } catch (e) { console.error(`pondra: ${event}:`, e); } } }
export const R = { views: [], docs: [], kinds: new Map(), renderers: [], actions: [], nav: [], commands: new Map(), keys: [], helpers: {} }; // (helpers: what the shell offers the other modules)
export const byOrder = (a, b) => (a.order ?? 50) - (b.order ?? 50);
const put = (list, o) => list.filter(x => x.id !== o.id).concat(o).sort(byOrder);
/** Draw the regions again once (the shell sets how: registrations may come after it starts). */
export const shell = { redraw() {} };
export const register = {
  /** A view: `{ id, title, side: 'left' | 'right', render(box, picked) → elements?, tools?: [{ icon, title, run }], order }`.
   * On the left it is a group that folds; on the right a tab. Either can be moved to the other side.
   * A tool is a button and an item of the view's ⋯, unless it is `menu: false` (one that opens a menu at its button). */
  view(o) { R.views = put(R.views, { side: 'left', ...o }); shell.redraw(); },
  /** A group of the left side (a view on the left): `{ id, title, render(box), tools, order }`. */
  section(o) { register.view({ ...o, side: 'left' }); },
  /** A tab of the right side (a view on the right): `{ id, title, render(box, picked), order }`. */
  panel(o) { register.view({ ...o, side: 'right' }); },
  /** A kind of document, opened in a tab: `{ id, label, icon, match(path) → bool, open(path, init) → doc, order }`. */
  doc(o) { R.docs = put(R.docs, o); },
  /** A kind of notebook cell: `{ id, label, placeholder, language, run(text, signal) → answer, live? }`. */
  cellKind(o) { R.kinds.set(o.id, o); shell.redraw(); },
  /** A view of an answer: `{ id, match(answer) → bool, render(answer, cell) → element, order }`: the first that matches draws it. */
  renderer(o) { R.renderers = put(R.renderers, o); },
  /** An action: a button in the top bar (`label`), or an item of its menu (`menu: true`): `{ id, label?, icon?, title, run(), menu?, order }`. */
  action(o) { R.actions = put(R.actions, o); shell.redraw(); },
  /** A place in the rail at the far left (it shows once there is one): `{ id, label, icon (SVG paths), run(), order }`. */
  nav(o) { R.nav = put(R.nav, o); shell.redraw(); },
  /** A command, by name, for the search box (Ctrl K) and keys: `{ id, title, run(), keys? }`. */
  command(o) { R.commands.set(o.id, o); },
  /** A key, listed under ?: `{ keys: 'd d', title, run(cell), group }` (on a notebook cell after Esc, if it runs). */
  key(o) { R.keys = R.keys.filter(x => x.keys !== o.keys || x.group !== o.group).concat(o); shell.redraw(); },
  /** A kind of job, a section of Jobs: `{ id, title, load() → items, item(x) → element, empty, order }` (schedules; round 30's pipelines). */
  jobKind(o) { R.jobKinds = put(R.jobKinds || [], o); },
  /** A section of Settings: `{ id, title, group, icon, about, rows() → [[label, about, control]], order }` (an enterprise build's users, tokens, audit). */
  setting(o) { R.settings = put(R.settings || [], o); },
  /** A kind of object in the Data tree, a group under the lake: `{ id, title, icon, order, list(), item(x) → { name, icon, meta, title }, menu(x) → items, create() → SQL }` (users and roles, pipelines). */
  objectKind(o) { R.objectKinds = put(R.objectKinds || [], o); shell.redraw(); },
  /** An item of an object's menu in the Data tree: `{ id, kinds: ['table', 'view', 'materialized_view', 'column', 'schema', 'lake', 'functions', …], label, icon, run(object) }`. */
  objectAction(o) { R.objectActions = put(R.objectActions || [], o); },
};
/** How the page reaches the node: `fetch`, the token, extra headers (an enterprise build's gateway
 * and sign-in replace them). */
// (the shell's console link carries its key (#key=…): this page then works as the shell does)
export const T = { fetch: (url, init) => fetch(url, init), token: () => store.get('pondra.token'), headers: () => (OWNER ? { 'x-pondra-owner': OWNER } : {}) };
const OWNER = (() => { try { const k = new URLSearchParams(location.hash.slice(1)).get('key'); if (k) { sessionStorage.setItem('pondra.owner', k); history.replaceState(null, '', location.pathname + location.search); } return sessionStorage.getItem('pondra.owner'); } catch { return null; } })();
export function configure(o) { Object.assign(T, o); }

// ------------------------------------------------------------------ the page's state
export const MODE = document.documentElement.dataset.mode; // lake: one lake (serve --lake); lakes: a folder of lakes, its databases
export const SESSION = [...crypto.getRandomValues(new Uint8Array(12))].map(b => b.toString(16).padStart(2, '0')).join(''); // (the temporary tables and the Python of this page)
/** What the page holds: the database, the documents open (`doc` the one in front), the pick the
 * details show, the catalog, this page's Python and runs. */
export const S = { db: null, lake: null, docs: [], doc: null, sel: null, runs: 0, open: new Set(), pick: null, objects: null, info: null, filesAt: '', tab: 'details', py: 'none', vars: [], place: null, files: null, ran: [],
  /** The cells of the notebook in front (or of the last one in front). */
  get cells() { return (S.doc?.kind === 'notebook' ? S.doc : S.nb)?.cells || []; },
  /** Its name (ADR-032's `state.name`). */
  get name() { return (S.doc?.kind === 'notebook' ? S.doc : S.nb)?.name || null; }, nb: null };
export const base = () => MODE === 'lakes' && S.db ? '/db/' + encodeURIComponent(S.db) : '';

// ------------------------------------------------------------------ talking to the node
export class Failure extends Error { constructor(message, status) { super(message); this.status = status; } }
/** What a run that failed answers (a wait the page stopped says so). */
export const failed = (e, stopped = 'Stopped waiting. (A statement already on its way may still finish on the node.)') => ({ kind: 'error', message: e.name === 'AbortError' ? stopped : e.message, notices: [] });
export const ask = { token() {} }; // (the sign-in dialog of the shell)

export async function call(path, { method = 'GET', body, headers = {}, signal, root = false } = {}) {
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
  if (r.status === 401) ask.token(text);
  throw new Failure(text, r.status);
}

/** A statement's answer: rows (the columns with their types) or what it did, and what it printed. */
export async function run(sql, signal, params, page) {
  const at = '/sql?format=typed' + (page ? '&rows=' + page : ''); // (a page's rows: what Settings says for what the user runs; 10,000 else)
  const r = params && Object.keys(params).length // (values for its $names: bound on the node, never pasted in)
    ? await call(at, { method: 'POST', body: JSON.stringify({ sql, params }), headers: { 'content-type': 'application/json' }, signal })
    : await call(at, { method: 'POST', body: sql, headers: { 'content-type': 'text/plain; charset=utf-8' }, signal });
  let notices = [];
  try { notices = JSON.parse(r.headers.get('x-pondra-notices') || '[]'); } catch { /* (none) */ }
  const v = await r.json();
  if (v && Array.isArray(v.columns) && Array.isArray(v.rows)) return { kind: 'rows', columns: v.columns, rows: v.rows, total: v.total ?? v.rows.length, pages: v.pages, notices }; // (pages: its other rows' id on the node)
  return { kind: 'done', value: v, notices };
}
/** A query's rows as objects (the console's own queries). */
export async function rows(sql) {
  const r = await run(sql);
  return r.kind === 'rows' ? r.rows.map(a => Object.fromEntries(r.columns.map((c, i) => [c.name, a[i]]))) : [];
}
/** Python as the node runs it: Postgres's anonymous code block, in this page's session. */
export function doBlock(code) {
  let tag = 'pondra';
  while (code.includes('$' + tag + '$')) tag += '_';
  return `DO LANGUAGE python $${tag}$\n${code}\n$${tag}$`;
}
/** A path under the lake's files, for `/files/…`. */
export const fileUrl = path => '/files/' + path.replace(/^files\//, '').split('/').map(encodeURIComponent).join('/');
/** A file in the lake as text, with the version to replace it by (`If-Match`). */
export async function readFile(rel) {
  const r = await call(fileUrl(rel));
  return { text: await r.text(), version: (r.headers.get('etag') || '').replace(/"/g, '') || null };
}
/** Write a file back: in place if `version` is the one read (else 412: someone saved it
 * meanwhile), or a new file if it has none. The new version, or null if not saved. */
export async function writeFile(rel, body, version, type = 'text/plain; charset=utf-8') {
  try {
    const r = await call(fileUrl(rel), { method: 'PUT', body, headers: { 'content-type': type, ...(version ? { 'if-match': `"${version}"` } : {}) } });
    return (await r.json()).version || 'saved';
  } catch (e) {
    if (e.status === 412) toast(`Not saved: ${rel.split('/').pop()} was saved by someone else since you opened it. Save it under another name (⋯), or open it again.`, true);
    else if (e.status === 409) toast(`Not saved: files/${rel} is there already. Open it, or pick another name.`, true);
    else toast('Not saved: ' + e.message, true);
    return null;
  }
}

const RESERVED = new Set('ALL AND ANY ARRAY AS ASC BETWEEN BY CASE CAST CHECK COLUMN CREATE CROSS DEFAULT DELETE DESC DISTINCT DO ELSE END EXCEPT FALSE FETCH FOR FROM FULL GRANT GROUP HAVING IN INNER INSERT INTERSECT INTO IS JOIN LEFT LIKE LIMIT NATURAL NOT NULL OFFSET ON OR ORDER OUTER RIGHT SELECT SET TABLE THEN TO TRUE UNION UNIQUE UPDATE USER USING VALUES VIEW WHEN WHERE WINDOW WITH'.split(' '));
export const SQL_KW = new Set([...RESERVED, ...'EXISTS ILIKE SIMILAR RECURSIVE MATERIALIZED REPLACE DROP ALTER ADD RENAME IF PRIMARY KEY NULLS FIRST LAST OVER PARTITION ROWS RANGE UNBOUNDED PRECEDING FOLLOWING CURRENT ROW FILTER WITHIN TRY_CAST INTERVAL DATE TIMESTAMP TIMESTAMPTZ TIME BIGINT INT INTEGER SMALLINT TINYINT DOUBLE PRECISION FLOAT REAL DECIMAL NUMERIC VARCHAR TEXT CHAR BOOLEAN BYTEA BINARY JSON EXPLAIN ANALYZE SHOW DESCRIBE CALL LANGUAGE FUNCTION PROCEDURE RETURNS RETURN BEGIN COMMIT ROLLBACK MERGE MATCHED SCHEMA DATABASE ATTACH DETACH COPY TEMP TEMPORARY SECRET TASK QUALIFY LATERAL UNNEST SOME STRUCT MAP AT OF TRUNCATE REVOKE NEXT ONLY REFERENCES CONSTRAINT INDEX OPTIMIZE VACUUM INSTALL LOAD EXTERNAL STORED LOCATION'.split(' ')]);
export const ident = s => /^[a-z_][a-z0-9_]*$/.test(s) && !RESERVED.has(s.toUpperCase()) ? s : '"' + s.replace(/"/g, '""') + '"';
export const quote = s => "'" + String(s).replace(/'/g, "''") + "'";
/** The database the page's statements run in (a server's database is the lake of that name). */
export const home = () => S.lake || S.db;
/** A table's name as a query here writes it: `t`, `schema.t`, or `lake.schema.t` for an attached lake's. */
export const qualified = (c, s, t) => [c !== home() ? c : null, c !== home() || s !== 'public' ? s : null, t].filter(Boolean).map(ident).join('.');
/** A lake file as SQL reads it (whoever reads the lake reads its files). */
export const DATA = /\.(parquet|pq|csv|tsv|json|jsonl|ndjson)$/i;
export const fileSql = rel => `read_${/\.(csv|tsv)$/i.test(rel) ? 'csv' : /\.(json|jsonl|ndjson)$/i.test(rel) ? 'json' : 'parquet'}(${quote(S.filesAt + rel.replace(/^files\//, ''))}${/\.tsv$/i.test(rel) ? ", delimiter => '\t'" : ''})`;

/** Printed text as a cell or a console shows it: the first 5,000 lines (400 KB) drawn, and the
 * rest a click away, so a loop that printed a million lines never stalls the page. */
export function said(text, cls = 'said') {
  const lines = text.split('\n', 5001);
  if (lines.length <= 5000 && text.length <= 400000) return h('pre', { class: cls }, text);
  const pre = h('pre', { class: cls }, lines.slice(0, 5000).join('\n').slice(0, 400000));
  const all = text.split('\n').length, rest = `${count(all - pre.textContent.split('\n').length)} more lines`;
  const more = h('div', { class: 'more-out' }, h('span', {}, `… and ${rest}.`),
    h('button', { class: 'btn small', onclick: () => { pre.textContent = text; more.remove(); } }, 'Show them'),
    h('button', { class: 'btn small', onclick: () => saveAs(text, 'text/plain', 'output.txt') }, 'Download it all'));
  return h('div', { class: 'said-box' }, pre, more);
}

// ------------------------------------------------------------------ toasts, menus, dialogs
let toastTimer;
export function toast(msg, bad) {
  const t = $('#toast');
  t.textContent = msg; t.className = 'on' + (bad ? ' bad' : '');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.className = '', bad ? 6000 : 2600);
}
/** A menu at `at` (an element, an event or a point): items `{ label, icon?, keys?, run, disabled?,
 * checked? }`, '-' for a line, or `{ head }` for a heading. Arrow keys move in it, Enter picks, Esc
 * closes (and focus goes back). */
export function menu(at, items) {
  const m = $('#menu'), back = document.activeElement;
  const list = items.filter(Boolean).filter((x, i, a) => x !== '-' || (i > 0 && a[i - 1] !== '-' && i < a.length - 1));
  m.replaceChildren(...list.map(i => i === '-' ? h('div', { class: 'sep', role: 'separator' }) : i.head ? h('div', { class: 'mh' }, i.head)
    : h('button', { role: i.checked != null ? 'menuitemradio' : 'menuitem', 'aria-checked': i.checked != null ? String(!!i.checked) : null, tabindex: '-1', disabled: i.disabled, onclick: () => { close(); i.run(); } },
      i.checked ? icon('check') : i.icon ? icon(i.icon) : h('span', { class: 'ic' }), h('span', { class: 'lb' }, i.label), i.keys ? h('kbd', {}, i.keys) : null)));
  const close = () => { m.hidden = true; if (document.activeElement?.closest('#menu')) back?.focus?.(); };
  m.onkeydown = e => {
    const all = [...m.querySelectorAll('button:not(:disabled)')], i = all.indexOf(document.activeElement);
    const go = { ArrowDown: 1, ArrowUp: -1 }[e.key];
    if (go) { e.preventDefault(); all[(i + go + all.length) % all.length]?.focus(); }
    else if (e.key === 'Home' || e.key === 'End') { e.preventDefault(); all[e.key === 'Home' ? 0 : all.length - 1]?.focus(); }
    else if (e.key === 'Escape' || e.key === 'Tab') { e.preventDefault(); close(); }
  };
  m.hidden = false;
  const r = at.getBoundingClientRect ? at.getBoundingClientRect() : { left: at.clientX ?? at.x, right: at.clientX ?? at.x, bottom: at.clientY ?? at.y, top: at.clientY ?? at.y };
  const below = r.bottom + 4 + m.offsetHeight < innerHeight - 8;
  m.style.top = (below ? r.bottom + 4 : Math.max(8, r.top - m.offsetHeight - 4)) + 'px';
  // (under a button: from its left edge in the page's left half, to its right edge in the right half)
  const left = at.getBoundingClientRect && r.left + r.right > innerWidth ? r.right - m.offsetWidth : r.left;
  m.style.left = Math.max(8, Math.min(innerWidth - m.offsetWidth - 8, left)) + 'px';
  m.querySelector('button:not(:disabled)')?.focus();
}
addEventListener('mousedown', e => { if (!e.target.closest('#menu')) $('#menu').hidden = true; });
// A dialog closes when you press outside it, as Esc closes it (a press on the backdrop is the dialog's).
addEventListener('mousedown', e => {
  const d = e.target, r = d instanceof HTMLDialogElement && d.open && d.getBoundingClientRect();
  if (r && (e.clientX < r.left || e.clientX >= r.right || e.clientY < r.top || e.clientY >= r.bottom)) d.close();
});
/** A small dialog asking for one line of text; `null` if cancelled. */
export function prompt(title, label, value = '', hint = '') {
  const d = $('#askDlg');
  $('#askTitle').textContent = title; $('#askLabel').textContent = label; $('#askHint').textContent = hint;
  const input = $('#askIn');
  input.value = value;
  d.returnValue = '';
  d.showModal();
  const dot = value.lastIndexOf('.');
  input.setSelectionRange(value.lastIndexOf('/') + 1, dot > value.lastIndexOf('/') ? dot : value.length);
  return new Promise(done => d.addEventListener('close', () => done(d.returnValue === 'ok' && input.value.trim() ? input.value.trim() : null), { once: true }));
}
/** A yes or no; true for yes. */
export function confirmed(message) { return confirm(message); }
/** A small window in the middle of the page: a title, what it shows, and its buttons
 * (`[label, run, primary?]`); Esc, its ✕ or a press outside it closes it. */
export function pop(title, body, buttons = []) {
  const d = h('dialog', { class: 'pop', 'aria-label': title });
  const close = () => d.close();
  d.append(h('div', { class: 'pop-h' }, h('h3', {}, title), h('button', { class: 'icon', title: 'Close (Esc)', 'aria-label': 'Close', onclick: close }, icon('close'))),
    h('div', { class: 'pop-b' }, body), ...buttons.length ? [h('div', { class: 'acts' }, buttons.map(([label, run, primary]) => h('button', { class: 'btn' + (primary ? ' primary' : ''), onclick: () => { close(); run(); } }, label)))] : []);
  d.addEventListener('close', () => d.remove());
  document.body.append(d);
  d.showModal();
  return d;
}
/** Python code formatted by the node's Python, as ruff (or black) formats it (the code isn't run). */
export async function formatPython(code) {
  const r = await call('/python/format', { method: 'POST', body: JSON.stringify({ code }), headers: { 'content-type': 'application/json' } });
  return (await r.json()).code;
}
/** Stop the page's Python cell that is running, on the node: it ends with "Interrupted" and the
 * variables stay (Windows: Python starts again, without them). */
export async function interruptPython() {
  try {
    const r = await (await call(`/sessions/${SESSION}/python`, { method: 'POST' })).json();
    if (r.done === 'restarted') toast('Python was stopped and starts again: its variables are gone');
  } catch (e) { toast(e.message, true); }
}
