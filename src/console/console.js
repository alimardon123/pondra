// The console (ADR-030, ADR-032, ADR-034): Pondra's workspace for SQL, Python, notebooks and data
// files, served by every node. This module is the shell — panes, views, tabs, the top and status
// bars — and `window.pondra`, the API an extension (or an enterprise build) adds to:
//
//   pondra.register.view({ id: 'jobs', title: 'Jobs', side: 'left', render: box => box.append(…) })
//   pondra.register.panel({ id: 'lineage', title: 'Lineage', render: (box, picked) => … })  // (a view on the right)
//   pondra.register.doc({ id: 'dash', match: p => p.endsWith('.dash'), open: async path => doc })
//   pondra.register.renderer({ id: 'map', match: r => …, render: (r, cell) => element })
//   pondra.register.action({ id: 'share', label: 'Share', run: () => … })
//   pondra.register.nav({ id: 'home', label: 'Home', icon: '<path …/>', run: () => … })
//   pondra.configure({ fetch, token, headers })   // (its own gateway and sign-in)
//   pondra.on('pick', picked => …)                 // (and 'run', 'ran', 'refresh', 'start', 'open', 'active')
//
// No framework and nothing from anywhere else: the page, its modules and its fonts come from the node.
import { h, $, fill, said, esc, store, secs, count, bytes, ago, utc, ICONS, icon, svg, typeMark, sqlType, numeric, on, emit, R, byOrder, shell, register, T, configure,
  MODE, SESSION, S, base, call, run, rows, doBlock, ident, quote, qualified, home, fileSql, toast, menu, prompt, confirmed, VERSION, DATA, fileUrl, Failure, ask } from './core.js';
import { highlighted, closeComplete } from './editor.js';
import { grid, summarize, statView, spread } from './grid.js';
import { Notebook, openNotebook, versions, cleanName, doneText } from './notebook.js';
import { workspace, drawWorkspace, treeItem, upload, registerFiles, SqlDoc, PythonDoc, kindOf, iconOf, download } from './files.js';

R.helpers = {};
const H = R.helpers;
const PREFS = store.json('pondra.prefs', {});
const prefs = (k, v) => { if (v === undefined) return PREFS[k]; PREFS[k] = v; store.set('pondra.prefs', JSON.stringify(PREFS)); };
const narrow = () => innerWidth < 760;

// ------------------------------------------------------------------ the look: theme and fonts (Settings)
function look() {
  const d = document.documentElement;
  const theme = prefs('theme') || 'system';
  if (theme === 'system') delete d.dataset.theme; else d.dataset.theme = theme;
  d.classList.toggle('sysfont', prefs('font') === 'system');
}

// ------------------------------------------------------------------ panes: left, bottom, right
const PANE = { left: '#left', right: '#right' };
function pane(which, open) {
  const was = paneOpen(which);
  if (open === undefined) open = !was;
  if (which === 'bottom') { prefs('bottom', open); S.doc?.el.classList.toggle('nopanel', !open); }
  else { $(PANE[which]).hidden = !open; if (!narrow()) prefs(which, open); if (which === 'right' && open) drawRight(); }
  drawPanes();
}
H.pane = pane;
const paneOpen = which => which === 'bottom' ? prefs('bottom') !== false : !$(PANE[which]).hidden;
function drawPanes() {
  const one = (which, name, label, keys) => {
    const none = which === 'bottom' && !S.doc?.hasPanel, on = !none && paneOpen(which);
    return h('button', { class: 'icon pane' + (on ? ' on' : ''), disabled: none, 'aria-pressed': String(on), title: `${label} (${keys})${none ? ': this file has none' : ''}`, 'aria-label': label, html: svg(name + (on ? '_on' : ''), 18), onclick: () => pane(which) });
  };
  $('#panes').replaceChildren(one('left', 'paneL', 'Show or hide the left pane', 'Ctrl B'), one('bottom', 'paneB', 'Show or hide the bottom panel', 'Ctrl J'), one('right', 'paneR', 'Show or hide the right pane', 'Ctrl Alt B'));
}

// ------------------------------------------------------------------ views: groups on the left, tabs on the right; either moves to the other side
const sideOf = v => prefs('sides')?.[v.id] || v.side;
function moveView(v) {
  const sides = { ...prefs('sides') || {} }, to = sideOf(v) === 'left' ? 'right' : 'left';
  sides[v.id] = to;
  prefs('sides', sides);
  if (to === 'right') { S.tab = v.id; pane('right', true); } else pane('left', true);
  drawViews();
  toast(`${v.title} is now in the ${to} pane`);
}
const viewTitle = v => typeof v.title === 'function' ? v.title() : v.title;
function viewMenu(e, v) {
  menu(e.currentTarget || e, [...(v.tools || []).filter(t => !t.hidden?.()).map(t => ({ label: t.title, icon: t.icon, run: t.run })), (v.tools || []).length ? '-' : null,
    { label: sideOf(v) === 'left' ? 'Move to the right pane' : 'Move to the left pane', icon: 'moveSide', run: () => moveView(v) },
    sideOf(v) === 'left' ? { label: folded(v) ? 'Unfold' : 'Fold', run: () => fold(v) } : null]);
}
const folded = v => (prefs('folded') || []).includes(v.id);
function fold(v, on = !folded(v)) { prefs('folded', [...new Set((prefs('folded') || []).filter(x => x !== v.id).concat(on ? [v.id] : []))]); drawLeft(); }
/** The order of the left groups: Data first, unless Settings says Workspace first. */
const leftViews = () => R.views.filter(v => sideOf(v) === 'left').map(v => ({ v, o: v.id === 'workspace' && prefs('workspaceFirst') ? 5 : v.order ?? 50 })).sort((a, b) => a.o - b.o).map(x => x.v);
async function renderView(v) {
  v.box ||= h('div', { id: v.id, role: v.tree !== false ? 'tree' : null, class: 'vbox', 'aria-label': viewTitle(v) });
  const n = v.n = (v.n || 0) + 1; // (a slow answer for an earlier pick never covers a later one's)
  try {
    const out = await v.render(v.box, S.pick);
    if (n === v.n && Array.isArray(out)) v.box.replaceChildren(...out.filter(Boolean));
  } catch (e) {
    if (n === v.n) v.box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to see this.' : e.message));
  }
}
function drawLeft() {
  const views = leftViews(), weights = prefs('weights') || {};
  const parts = [];
  views.forEach((v, i) => {
    renderOnce(v);
    const f = folded(v), tools = (v.tools || []).filter(t => !t.hidden?.()).map(t => h('button', { class: 'icon sm', title: t.title, 'aria-label': t.title, id: t.domId || null, onclick: e => { e.stopPropagation(); t.run(e); } }, icon(t.icon)));
    const head = h('div', { class: 'ghead' }, h('button', { class: 'gtitle', 'aria-expanded': String(!f), onclick: () => fold(v) }, h('span', { class: 'tw', html: svg(f ? 'chev' : 'chevd', 14, 2) }), h('h2', { id: v.id + 'Title' }, viewTitle(v))),
      h('span', { class: 'gtools' }, tools, h('button', { class: 'icon sm', title: `${viewTitle(v)}: more`, 'aria-label': `${viewTitle(v)}: more`, onclick: e => viewMenu(e, v) }, icon('dots'))));
    const g = h('section', { class: 'group' + (f ? ' folded' : ''), 'data-view': v.id, style: !f && weights[v.id] ? `flex:${weights[v.id]} 1 0` : null }, head, f ? null : v.box);
    if (i && !f && parts.length && !parts.at(-1).classList.contains('folded')) parts.push(divider(parts.at(-1), g));
    parts.push(g);
  });
  $('#left').replaceChildren(FILTER, ...parts.length ? parts : [h('div', { class: 'empty pad' }, 'Nothing here: views moved to the right pane come back with their ⋯.')], EDGE);
  filterLeft();
}
const EDGE = $('#leftEdge'); // (the left pane's edge: kept as its groups are drawn again)
/** The left pane's filter: the trees show the names that hold what is typed (and what they are in). */
const FILTER = h('label', { class: 'lfilter' }, icon('filter'), h('input', { id: 'filter', type: 'search', placeholder: 'Filter', autocomplete: 'off', spellcheck: 'false', 'aria-label': 'Filter the tables and files',
  oninput: e => { S.filter = e.target.value.trim().toLowerCase(); filterLeft(); }, onkeydown: e => { if (e.key === 'Escape' && e.target.value) { e.stopPropagation(); e.target.value = ''; S.filter = ''; filterLeft(); } } }));
function filterLeft() {
  const q = S.filter || '', name = row => (row.querySelector(':scope > .nm')?.textContent || '').toLowerCase();
  const walk = (el, keep) => { // → whether `el`, or something in it, holds q
    const item = el.classList.contains('item'), row = item ? el.firstElementChild : el;
    if (!row?.classList.contains('row')) return false;
    const self = !!q && name(row).includes(q);
    let below = false;
    if (item) for (const k of el.children[1].children) below = walk(k, keep || self) || below;
    el.classList.toggle('fhide', !!q && !(keep || self || below));
    if (item) el.classList.toggle('fopen', !!q && below);
    return self || below;
  };
  for (const box of $('#left').querySelectorAll('.vbox')) for (const k of box.children) walk(k, false);
}
new MutationObserver(() => { if (S.filter && !filterLeft.soon) filterLeft.soon = requestAnimationFrame(() => { filterLeft.soon = 0; filterLeft(); }); }).observe($('#left'), { childList: true, subtree: true });

/** The side panes' edges: dragged (or arrows, when focused), they set a pane's width; a double-click resets it. */
function edges() {
  for (const [which, sign] of [['left', 1], ['right', -1]]) {
    const e = $(`#${which}Edge`), el = $(PANE[which]), keep = () => prefs('widths', { ...prefs('widths'), [which]: el.offsetWidth });
    const set = w => { el.style.width = Math.round(Math.max(200, Math.min(innerWidth * 0.5, w))) + 'px'; e.setAttribute('aria-valuenow', String(el.offsetWidth || w)); };
    e.setAttribute('aria-valuemin', '200'); e.setAttribute('aria-valuenow', String(el.offsetWidth || (which === 'left' ? 272 : 320)));
    e.addEventListener('pointerdown', ev => {
      ev.preventDefault(); e.setPointerCapture(ev.pointerId); e.classList.add('drag');
      const w0 = el.offsetWidth, x0 = ev.clientX, move = m => set(w0 + sign * (m.clientX - x0));
      e.addEventListener('pointermove', move);
      e.addEventListener('pointerup', () => { e.removeEventListener('pointermove', move); e.classList.remove('drag'); keep(); }, { once: true });
    });
    e.addEventListener('keydown', ev => { const d = { ArrowLeft: -16, ArrowRight: 16 }[ev.key]; if (d) { ev.preventDefault(); set(el.offsetWidth + sign * d); keep(); } });
    e.addEventListener('dblclick', () => { el.style.width = ''; prefs('widths', { ...prefs('widths'), [which]: undefined }); });
    const w = prefs('widths')?.[which];
    if (w) set(w);
  }
}
/** A divider between two open groups: dragged, it shares their height. */
function divider(a, b) {
  const d = h('div', { class: 'gsplit', role: 'separator', 'aria-orientation': 'horizontal' });
  d.addEventListener('pointerdown', e => {
    e.preventDefault(); d.setPointerCapture(e.pointerId);
    const ha = a.offsetHeight, hb = b.offsetHeight, y0 = e.clientY;
    const move = ev => {
      const dy = Math.max(60 - ha, Math.min(hb - 60, ev.clientY - y0)), w = { ...prefs('weights') || {} };
      w[a.dataset.view] = (ha + dy) / 100; w[b.dataset.view] = (hb - dy) / 100;
      a.style.flex = `${w[a.dataset.view]} 1 0`; b.style.flex = `${w[b.dataset.view]} 1 0`;
      prefs('weights', w);
    };
    d.addEventListener('pointermove', move);
    d.addEventListener('pointerup', () => d.removeEventListener('pointermove', move), { once: true });
  });
  return d;
}
const renderOnce = v => { if (!v.drawn) { v.drawn = true; renderView(v); } else v.box ||= h('div', { id: v.id, class: 'vbox' }); };
function drawRight() {
  const views = R.views.filter(v => sideOf(v) === 'right').sort(byOrder);
  if (!views.some(v => v.id === S.tab)) S.tab = views[0]?.id;
  const tab = v => h('button', { class: 'rtab', role: 'tab', id: 'rtab-' + v.id, tabindex: v.id === S.tab ? '0' : '-1', 'aria-selected': String(v.id === S.tab), 'aria-controls': 'rbody', onclick: () => { S.tab = v.id; drawRight(); } }, viewTitle(v));
  const cur = views.find(v => v.id === S.tab);
  fill($('#rtabs'), h('div', { class: 'tlist', role: 'tablist', 'aria-label': 'The right pane' }, views.map(tab)), h('span', { class: 'grow' }), cur ? h('button', { class: 'icon sm', title: `${viewTitle(cur)}: more`, 'aria-label': `${viewTitle(cur)}: more`, onclick: e => viewMenu(e, cur) }, icon('dots')) : null);
  if (!cur || $('#right').hidden) return;
  cur.box ||= h('div', { id: cur.id, class: 'vbox' });
  $('#rbody').replaceChildren(cur.box);
  $('#rbody').setAttribute('aria-labelledby', 'rtab-' + cur.id);
  renderView(cur);
}
const drawViews = () => { drawLeft(); drawRight(); };
/** The right pane's view again (what was picked changed). */
function detail() { if (!$('#right').hidden) { const v = R.views.find(x => x.id === S.tab && sideOf(x) === 'right'); if (v) renderView(v); } }
function show(id) { const v = R.views.find(x => x.id === id); if (!v) return; if (sideOf(v) === 'right') { S.tab = id; pane('right', true); } else { pane('left', true); fold(v, false); } }
H.show = show;
async function refreshViews(ids) { await Promise.all(R.views.filter(v => (!ids || ids.includes(v.id)) && v.drawn !== false && (sideOf(v) === 'left' || v.id === S.tab)).map(renderView)); }

// ------------------------------------------------------------------ documents in tabs
function addDoc(doc) {
  S.docs.push(doc);
  emit('open', doc);
  activate(doc);
  remember();
  return doc;
}
function activate(doc) {
  if (!doc) return;
  S.doc = doc;
  $('#docs').replaceChildren(doc.el);
  doc.el.classList.toggle('nopanel', prefs('bottom') === false);
  doc.activate?.();
  drawTabs(); toolbar(); status(); drawPanes();
  if (doc.kind === 'notebook') S.nb = doc;
  document.title = `${doc.dirty ? '• ' : ''}${doc.title} · Pondra`;
  laterWorkspace();
  if (narrow()) pane('left', false);
  emit('active', doc);
  hashNow();
}
H.activate = activate;
async function closeDoc(doc) {
  if (!(await doc.close?.() ?? true)) return;
  const i = S.docs.indexOf(doc);
  S.docs.splice(i, 1);
  emit('close', doc);
  if (S.doc === doc) {
    S.doc = null;
    if (S.docs.length) activate(S.docs[Math.min(i, S.docs.length - 1)]);
    else { $('#docs').replaceChildren(welcome()); drawTabs(); toolbar(); status(); drawPanes(); document.title = 'Pondra'; }
  } else drawTabs();
  if (S.nb === doc) S.nb = S.docs.filter(d => d.kind === 'notebook').at(-1) || null;
  remember(); laterWorkspace();
}
function drawTabs() {
  const tabs = S.docs.map((d, i) => {
    const t = h('div', { class: 'tab' + (d === S.doc ? ' on' : ''), role: 'tab', tabindex: d === S.doc ? '0' : '-1', 'aria-selected': String(d === S.doc), title: d.path ? `files/${d.path}` : d.title, 'aria-keyshortcuts': 'Delete',
      onclick: e => { if (!e.target.closest('.x')) activate(d); }, onauxclick: e => { if (e.button === 1) closeDoc(d); } },
      h('span', { class: 'ic k-' + d.kind, html: svg(d.icon, 15) }), h('span', { class: 'tn' }, d.title),
      d.dirty ? h('span', { class: 'dirty', title: 'Not saved', 'aria-label': 'not saved' }) : null,
      h('span', { class: 'x', title: `Close ${d.title} (Delete, when its tab has the focus)`, 'aria-hidden': 'true', html: svg('close', 14), onclick: () => closeDoc(d) })); // (a tab holds no other control: the keyboard closes it with Delete)
    t.dataset.i = i;
    return t;
  });
  $('#tabbar').replaceChildren(h('div', { class: 'tlist', role: 'tablist', 'aria-label': 'Open files' }, tabs), h('button', { class: 'icon newtab', title: 'New: a notebook, a SQL or a Python file', 'aria-label': 'New', html: svg('plus', 16), onclick: e => newMenu(e.currentTarget) }));
  if (S.doc) document.title = `${S.doc.dirty ? '• ' : ''}${S.doc.title} · Pondra`;
}
H.drawTabs = drawTabs;
function toolbar() {
  const bar = $('#docbar');
  bar.hidden = !S.doc;
  if (S.doc) bar.replaceChildren(...S.doc.toolbar().flat().filter(Boolean));
}
H.toolbar = toolbar;
function status() {
  const s = S.info || {}, nodes = s.nodes || [];
  const where = h('span', { class: 'st-where', title: s.leader ? `This database's cluster: ${nodes.join(', ')}. Its leader is ${s.leader}; commits so far: ${s.hwm}.` : '' }, h('span', { class: 'dot' + (S.down ? ' off' : '') }), `${home() || ''}: ${S.down ? 'not reachable' : 'ready'}`);
  $('#stl').replaceChildren(where, ...(S.doc?.status?.() || []).filter(Boolean).slice(0, 1).map(t => h('span', {}, t)));
  fill($('#str'), ...(S.doc?.status?.() || []).filter(Boolean).slice(1).map(t => h('span', {}, t)), s.role && nodes.length > 1 ? h('span', {}, `${s.role} · ${nodes.length} nodes`) : null, h('span', {}, `Pondra ${VERSION}`));
}
H.status = status;
function welcome() {
  const b = (ic, label, fn) => h('button', { class: 'btn', onclick: fn }, icon(ic), label);
  return h('div', { class: 'doc welcome' }, h('div', {}, h('h1', {}, 'Pondra'), h('p', {}, 'Open a file from the Workspace, a table from Data, or start something new.'),
    h('div', { class: 'acts2' }, b('notebook', 'New notebook', () => newNotebook()), b('filesql', 'New SQL file', () => newFile('sql')), b('filepy', 'New Python file', () => newFile('python')), b('up', 'Upload a file', () => upload()))));
}
function newMenu(at) {
  menu(at, [{ label: 'New notebook', icon: 'notebook', run: () => newNotebook() }, { label: 'New SQL file', icon: 'filesql', run: () => newFile('sql') }, { label: 'New Python file', icon: 'filepy', run: () => newFile('python') }, '-',
    { label: 'Open an .ipynb or put a file in the lake…', icon: 'up', run: () => upload() }]);
}
let untitled = 0;
function newNotebook(nb = { cells: [] }, name) { const d = addDoc(new Notebook({ name: name || nextName('untitled', n => S.docs.some(x => x.kind === 'notebook' && x.name === n)), nb })); if (!nb.cells.length) d.cells[0].edit(); return d; }
H.openNotebook = (nb, name) => { const d = newNotebook(nb, cleanName(name || '') || 'untitled'); d.changed(); return d; };
const nextName = (stem, taken) => { let n = stem; while (taken(n)) n = `${stem}-${++untitled + 1}`; return n; };
function newFile(kind, at = '') {
  const Cls = kind === 'python' ? PythonDoc : SqlDoc, ext = kind === 'python' ? 'py' : 'sql';
  const name = nextName('untitled', n => S.docs.some(d => d.title === `${n}.${ext}`)) + '.' + ext;
  const d = addDoc(new Cls({ untitled: at + name }));
  d.ed.focus();
  return d;
}
H.newFile = newFile;
/** Open a lake file (a path under `files/`, or `notebooks/<name>`) in its tab: the open one comes forward. */
const opening = new Map(); // (a path being opened: a second click waits for the same tab)
function openFile(path, opts = {}) {
  path = path.replace(/^\/?(files\/)?/, '');
  const open = S.docs.find(d => d.path === path);
  if (open) { activate(open); return Promise.resolve(open); }
  if (!opening.has(path)) opening.set(path, opened(path, opts).finally(() => opening.delete(path)));
  return opening.get(path);
}
async function opened(path, opts) {
  try {
    const nb = path.match(/^notebooks\/([^/]+?)(?:\/([^/]+)\.ipynb)?$/);
    if (nb) return addDoc(await openNotebook(nb[1], nb[2] || opts.version));
    const kind = R.docs.find(d => d.match(path));
    if (!kind) { pick({ type: 'file', f: S.files?.find(f => f.path === 'files/' + path) || { path: 'files/' + path, rel: path, name: path.split('/').pop() } }); return null; }
    const doc = await kind.open(path, opts);
    doc.path ||= path;
    return addDoc(doc);
  } catch (e) { if (!opts.quiet) toast(`Could not open ${path}: ${e.message}`, true); return null; }
}
H.openFile = openFile;
/** SQL in a tab and run: into the notebook in front (a cell), else a new SQL tab. */
function query(sql) {
  if (S.doc?.kind === 'notebook') return S.doc.peek(sql);
  const d = newFile('sql');
  d.ed.value = sql;
  d.run();
}
H.query = query;
/** The open tabs, remembered in this browser (per database), and opened again next time. */
const kept = d => d?.path && (d.kind !== 'notebook' || d.version); // (a tab that can open again: saved)
function remember() { store.set('pondra.tabs:' + (home() || ''), JSON.stringify(S.docs.filter(kept).map(d => d.path))); }
async function restoreTabs() {
  const paths = store.json('pondra.tabs:' + (home() || ''), []);
  for (const p of paths.slice(0, 12)) await openFile(p, { quiet: true }); // (one gone since is left out)
}
function hashNow() {
  const p = new URLSearchParams();
  if (S.db && MODE === 'lakes') p.set('db', S.db);
  if (S.doc?.kind === 'notebook' && S.doc.version) p.set('notebook', S.doc.name);
  else if (kept(S.doc)) p.set('file', S.doc.path);
  history.replaceState(null, '', p.size ? '#' + p : location.pathname);
}

// ------------------------------------------------------------------ the Data view: databases, schemas, tables, views, columns
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
const openKey = (key, dflt) => S.open.has(key) || (dflt && !S.open.has('closed:' + key));
function lakeNode(name, schemas, current, note, depth = 0) {
  const key = 'db:' + name;
  const kids = schemas ? h('div', { class: 'kids', role: 'group', hidden: !openKey(key, current) }) : null;
  if (schemas) {
    const names = [...schemas.keys()].sort((a, b) => (a !== 'public') - (b !== 'public') || a.localeCompare(b));
    kids.append(...names.map(s => schemaNode(name, s, schemas.get(s), names.length === 1)));
    if (!names.length) kids.append(h('div', { class: 'empty' }, 'No tables yet.'));
  }
  return treeItem({ key, kids, depth, icon: 'db', iconCls: 'k-db', name, cls: current ? 'cur' : '', meta: note, dataKind: 'database',
    title: MODE === 'lakes' && !current ? `Use database ${name}` : name, onclick: tw => { if (MODE === 'lakes' && name !== S.db) use(name); else tw.click(); } });
}
function schemaNode(lake, schema, tables, only) {
  const key = `s:${lake}.${schema}`;
  const kids = h('div', { class: 'kids', role: 'group', hidden: !openKey(key, only || schema === 'public') }, tables.map(t => tableNode(t)));
  return treeItem({ key, kids, depth: 1, icon: 'schema', iconCls: 'k-schema', name: schema, title: `schema ${schema}`, dataKind: 'schema', onclick: tw => tw.click() });
}
function tableNode(t) {
  const [ic, word] = KIND[t.o.kind] || KIND.table, keyed = new Set(t.o.key || []);
  const kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(t.key) }, t.columns.map(c =>
    h('div', { class: 'row col', role: 'treeitem', tabindex: '-1', 'aria-level': '4', style: 'padding-left:48px', title: `${c.n}: ${sqlType(c.d)} (${c.d}). Click: put the name where you are typing`, onclick: () => S.doc?.put?.(ident(c.n)) },
      typeMark(c.d), h('span', { class: 'nm' }, c.n), keyed.has(c.n) ? icon('key', 'kk') : null, h('span', { class: 'ty' }, sqlType(c.d)))));
  const it = treeItem({ key: t.key, kids: t.columns.length ? kids : null, depth: 2, icon: ic, iconCls: 'k-table', name: t.t, dataKey: t.key, dataKind: t.o.kind, on: S.pick?.type === 'object' && S.pick.t.key === t.key,
    title: `${t.q}: a ${word}. Click: its details; double-click: its first rows`, onclick: () => pick({ type: 'object', t }), ondblclick: () => query(`SELECT * FROM ${t.q} LIMIT 100`) });
  return it;
}
async function dataTree(box) {
  if (MODE === 'lakes') {
    let dbs;
    try { dbs = await (await call('/databases', { root: true })).json(); } catch (e) {
      box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to list the databases.' : e.message));
      return;
    }
    S.dbs = dbs;
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
    box.replaceChildren(...names.map(n => lakeNode(n, lakes.get(n) || new Map(), n === S.lake, n === S.lake ? 'this lake' : 'attached')));
  }
  if (S.pick?.type === 'object') S.pick.t = S.objects?.find(t => t.key === S.pick.t.key) || S.pick.t; // (as it is now)
  mark(); detail();
}
async function newDatabase() {
  const name = ((await prompt('New database', 'Its name (letters, digits and _)', '')) || '').trim().toLowerCase();
  if (!name) return;
  try {
    await call('/databases', { method: 'POST', body: JSON.stringify({ name }), headers: { 'content-type': 'application/json' }, root: true });
    await use(name);
  } catch (e) { toast(e.message, true); }
}
H.pickDb = at => menu(at, (S.dbs || []).map(d => ({ label: d.name, icon: 'db', run: () => use(d.name) })));

// ------------------------------------------------------------------ what was picked, and the details view
function pick(p, tab) {
  S.pick = p; mark();
  const details = R.views.find(v => v.id === (tab || 'details'));
  if (details && sideOf(details) === 'right') { S.tab = details.id; drawRight(); }
  if ($('#right').hidden) pane('right', true); else detail();
  emit('pick', p);
}
H.pick = pick;
function mark() {
  const key = S.pick?.type === 'object' ? S.pick.t.key : S.pick?.type === 'file' ? 'file:' + (S.pick.f.rel || S.pick.f.path?.replace(/^files\//, '')) : null;
  document.querySelectorAll('.vbox .row.on[data-key]').forEach(r => r.classList.remove('on'));
  if (key) document.querySelectorAll(`.vbox .row[data-key="${CSS.escape(key)}"]`).forEach(r => r.classList.add('on'));
}
const facts = pairs => h('dl', { class: 'facts' }, pairs.filter(([, v]) => v != null && v !== '' && !(Array.isArray(v) && !v.length)).flatMap(([k, v]) => [h('dt', {}, k), h('dd', {}, Array.isArray(v) ? v.join(', ') : v)]));
const act = (ic, label, title, fn) => h('button', { class: 'btn small', title, onclick: fn }, ic ? icon(ic) : null, label);
const head = (ic, name, kind, cls = '') => h('div', { class: 'dh' }, h('span', { class: 'dtile ' + cls, html: svg(ic, 20) }), h('div', {}, h('div', { class: 'dn' }, name), h('div', { class: 'dk' }, kind)));
function summary() {
  const objs = S.objects || [], by = k => objs.filter(t => t.c === home() && t.o.kind === k).length;
  const tabled = objs.filter(t => t.c === home() && t.o.rows != null);
  const s = S.info || {};
  return [head('db', home() || 'Pondra', MODE === 'lakes' ? 'a database' : 'this lake', 'k-db'),
    facts([['Tables', count(by('table'))], ['Views', count(by('view') + by('files'))], ['Materialized', by('materialized view') ? count(by('materialized view')) : null],
      ['Rows in files', count(tabled.reduce((a, t) => a + (t.o.rows || 0), 0))], ['Size in files', bytes(tabled.reduce((a, t) => a + (t.o.bytes || 0), 0))],
      ['Nodes', s.nodes ? String(s.nodes.length) : null], ['This node', s.role], ['Leader', s.leader], ['Commits', s.hwm != null ? count(s.hwm) : null]]),
    h('p', { class: 'muted' }, 'Pick a table, a view or a file, or a column of an answer, to see it here.')];
}
function objectDetail(t) {
  const o = t.o, [ic, word] = KIND[o.kind] || KIND.table, stored = o.kind === 'table' || o.kind === 'materialized view';
  const counted = h('span', { class: 'muted' }, '…');
  rows(`SELECT count(*) AS n FROM ${t.q}`).then(r => counted.textContent = count(r[0].n), e => counted.textContent = e.message.split('\n')[0].slice(0, 120));
  const keyed = new Set(o.key || []), nn = new Set(o.not_null || []);
  const cols = t.columns.map(c => {
    const flags = [keyed.has(c.n) ? 'key' : null, nn.has(c.n) && !keyed.has(c.n) ? 'not null' : null, o.defaults?.[c.n] ? `default ${o.defaults[c.n]}` : null].filter(Boolean);
    return h('div', { class: 'pc', 'data-col': c.n }, h('div', { class: 'line1' }, typeMark(c.d), h('span', { class: 'nm' }, c.n), keyed.has(c.n) ? icon('key', 'kk') : null, h('span', { class: 'ty' }, sqlType(c.d))),
      flags.length ? h('div', { class: 'sub' }, flags.join(' · ')) : null, h('div', { class: 'ps' }));
  });
  const profileBtn = act('chart', 'Profile', 'Each column: nulls, distinct values, range and spread (reads the whole table)', () => profile(t, cols, profileBtn));
  return [head(ic, t.t, `${word} · ${t.c}.${t.s}`, 'k-table'),
    h('div', { class: 'acts2' }, act('play', 'Preview', 'Its first rows (or double-click it)', () => query(`SELECT * FROM ${t.q} LIMIT 100`)), profileBtn, act('copy', 'Copy name', `Copy ${t.q}`, () => navigator.clipboard?.writeText(t.q).then(() => toast(`Copied ${t.q}`)))),
    facts([['Rows', counted], ['Columns', String(t.columns.length)], ['Key', o.key], ['Partitioned by', o.partition], ['Clustered by', o.cluster],
      ['Published as', o.publish], ['Rows kept', o.ttl], ['In files', stored ? `${bytes(o.bytes)} · ${count(o.files || 0)} file${o.files === 1 ? '' : 's'}` : null]]),
    o.sql ? h('div', { class: 'dsect' }, o.kind === 'files' ? 'Reads' : 'Definition') : null, o.sql ? h('pre', { class: 'defn', html: highlighted(o.sql, 'sql') }) : null,
    h('div', { class: 'dsect' }, 'Columns'), ...cols];
}
async function fileDetail(f) {
  const rel = f.rel || f.path.replace(/^files\//, ''), kind = f.notebook ? 'notebook' : kindOf(rel), doc = S.docs.find(d => d.path === rel);
  const out = [head(iconOf(f.notebook ? 'x.ipynb' : rel), f.name || rel.split('/').pop(), f.notebook ? `a notebook in notebooks/` : `a ${kind === 'data' ? 'data ' : kind === 'sql' ? 'SQL ' : kind === 'python' ? 'Python ' : ''}file in ${rel.includes('/') ? rel.slice(0, rel.lastIndexOf('/') + 1) : 'files/'}`, 'k-' + kind),
    h('div', { class: 'acts2' }, kind !== 'file' ? act('eye', 'Open', 'Open it in a tab', () => openFile(rel)) : null,
      kind === 'data' ? act('play', 'Query with SQL', 'Read it as a table, in a SQL tab', () => query(`SELECT * FROM ${fileSql(rel)} LIMIT 1000`)) : null,
      !f.notebook ? act('down', 'Download', 'Download it', () => download(rel)) : null)];
  if (f.notebook) {
    const vs = await versions(rel.slice(10)).catch(() => []);
    out.push(h('div', { class: 'dsect' }, `Versions (${vs.length})`), ...vs.map(v => h('div', { class: 'row', role: 'button', tabindex: '0', title: 'Open this version', onclick: async () => { const open = S.docs.find(d => d.path === rel); if (open && !open.close()) return; if (open) closeDoc(open); addDoc(await openNotebook(rel.slice(10), v.version)); } },
      icon('clock'), h('span', { class: 'nm' }, utc(v.written).toLocaleString()), h('span', { class: 'meta' }, ago(v.written)))));
    return out;
  }
  out.push(facts([['Size', bytes(f.size)], ['Written', f.written ? utc(f.written).toLocaleString() : null], ['In SQL', kind === 'data' ? fileSql(rel) : `file_read('files/${rel}')`]]));
  if (doc?.kind === 'data') {
    out.push(facts([['Rows', count(doc.data.length)], ['Columns', String(doc.cols.length)], ['Format', doc.status()[1]]]));
    const ch = doc.changes();
    if (ch.length) out.push(h('div', { class: 'dsect' }, 'Not saved'), ...ch.map(c => h('div', { class: 'change' }, c)));
    out.push(h('p', { class: 'note' }, 'CSV and JSON files are edited in place. Parquet files, and files too big to hold, open read-only: load one into a table to change it with SQL.'));
  }
  return out;
}
H.pickFile = path => { const rel = path.replace(/^files\//, ''), nb = rel.match(/^notebooks\/([^/]+)$/); pick({ type: 'file', f: nb ? { rel, name: nb[1] + '.ipynb', notebook: true } : S.files?.find(f => f.path === 'files/' + rel) || { rel, path: 'files/' + rel } }); };
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
    const showOne = i => { const box = boxes[i]?.querySelector('.ps'); if (box) box.replaceChildren(...statView(stats[i])); };
    cols.forEach((_, i) => showOne(i));
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
        showOne(i);
      }
    };
    await Promise.all([one(), one(), one()]);
  } catch (e) { toast('Could not profile it: ' + e.message, true); }
  btn.disabled = false; btn.lastChild.textContent = 'Profile';
}
/** An answer's columns, summarized from the rows it holds (a header clicked). */
function explore(r, i, cell) { pick({ type: 'result', r, i, cell }); }
H.explore = explore;
function resultDetail(p) {
  const { r, i, cell } = p, times = r.columns.map(c => /^Timestamp/.test(c.type || ''));
  const out = [head('chart', cell?.count ? `Answer [${cell.count}]` : 'Answer', `${count(r.rows.length)} row${r.rows.length === 1 ? '' : 's'}${r.total > r.rows.length ? ` of ${count(r.total)} (the ones here)` : ''} · ${r.columns.length} column${r.columns.length === 1 ? '' : 's'}`), h('div', { class: 'dsect' }, 'Columns')];
  r.columns.forEach((c, k) => {
    const s = summarize(r.rows.map(row => times[k] && row[k] != null ? String(row[k]).replace(/^(\d{4}-\d\d-\d\d)T/, '$1 ') : row[k]), c.type);
    const box = h('div', { class: 'pc' + (k === i ? ' on' : ''), 'data-col': c.name }, h('div', { class: 'line1' }, typeMark(c.type), h('span', { class: 'nm' }, c.name), h('span', { class: 'ty' }, sqlType(c.type))), h('div', { class: 'ps' }, ...statView(s)));
    out.push(box);
    if (k === i) requestAnimationFrame(() => box.scrollIntoView({ block: 'nearest' }));
  });
  return out;
}

// ------------------------------------------------------------------ this page's Python (a session's, on the node)
function kernel(state) {
  if (state) S.py = state;
  S.doc?.drawPill?.();
  if (S.doc?.kind === 'python') status();
}
H.kernel = kernel;
async function restart() {
  try { await call(`/sessions/${SESSION}/python`, { method: 'DELETE' }); } catch (e) { toast(e.message, true); return; }
  kernel('none'); S.vars = []; toast('Python restarted: its variables are gone'); if (S.tab === 'variables') detail();
}
H.restart = restart;
async function readVars() { const v = await (await call(`/sessions/${SESSION}/python`)).json(); S.vars = v.variables || []; return v; }
async function variables() {
  let v;
  try { v = await readVars(); } catch (e) { return [h('pre', { class: 'err' }, e.message)]; }
  return [head('var', 'Variables', v.busy ? 'a cell is running: they show when it is done' : v.running ? `${S.vars.length} in this page's Python` : 'no Python yet: a Python cell or file starts it'),
    h('div', { class: 'acts2' }, act('restart', 'Restart', 'Stop this page\'s Python: its variables go (its temporary tables stay)', restart), act('refresh', 'Refresh', 'Read them again', () => detail())),
    ...S.vars.map(x => h('div', { class: 'var' }, h('div', { class: 'line1' }, h('span', { class: 'nm' }, x.name), h('span', { class: 'ty' }, x.type + (x.size ? ` · ${x.size}` : ''))), h('div', { class: 'look' }, x.look)))];
}
/** Runs: the node's (jobs, files run, procedures and tasks: `pondra.runs`), the schedules
 * (`pondra.tasks`), and what this page ran, newest first. */
async function runs() {
  let node = [], tasks = [];
  try { node = await rows('SELECT id, routine, caller, status, started, ended, error FROM pondra.runs ORDER BY started DESC LIMIT 30'); } catch { /* (no run yet, or no rights) */ }
  try { tasks = await rows('SELECT name, schedule, statement, next_tick FROM pondra.tasks ORDER BY name'); } catch { /* (none) */ }
  clearTimeout(runs.again);
  if (node.some(x => x.status === 'running')) runs.again = setTimeout(() => { if (S.tab === 'runs') detail(); }, 2000); // (until it ends)
  const took = x => x.ended ? secs(utc(x.ended) - utc(x.started)) : 'running';
  const fileOf = x => /^files\/.+@/.test(x.routine) ? x.routine.replace(/^files\//, '').replace(/@[^@]*$/, '') : null;
  const nodeRun = x => h('div', { class: 'run-item', role: fileOf(x) ? 'button' : null, tabindex: fileOf(x) ? '0' : null, title: x.error || x.routine, onclick: () => fileOf(x) && openFile(fileOf(x)) },
    h('div', { class: 'line1' }, h('span', { class: 'ic', html: svg(fileOf(x) ? iconOf(fileOf(x)) : 'play', 14) }), h('span', { class: 'nm' }, fileOf(x) || x.routine),
      h('span', { class: 'meta ' + (x.status === 'failed' ? 'bad' : '') }, x.status === 'failed' ? 'failed' : took(x))),
    h('div', { class: 'sub' }, `${x.caller} · ${utc(x.started).toLocaleString()}`), x.error ? h('div', { class: 'sub bad' }, x.error.split('\n')[0].slice(0, 200)) : null);
  const task = t => h('div', { class: 'run-item', title: t.statement },
    h('div', { class: 'line1' }, h('span', { class: 'ic', html: svg('clock', 14) }), h('span', { class: 'nm' }, t.name), h('span', { class: 'meta' }, t.schedule),
      h('button', { class: 'icon sm', title: `Stop ${t.name}: DROP TASK`, 'aria-label': `Drop the task ${t.name}`, onclick: async () => { if (!confirmed(`Drop the task ${t.name}? It stops running.`)) return; try { await run(`DROP TASK ${ident(t.name)}`); detail(); } catch (e) { toast(e.message, true); } } }, icon('trash'))),
    h('div', { class: 'sub' }, `${t.statement.slice(0, 120)} · next ${utc(t.next_tick).toLocaleString()}`));
  const page = x => h('div', { class: 'run-item', role: 'button', tabindex: '0', title: x.src, onclick: () => { const d = newFile(x.kind === 'python' ? 'python' : 'sql'); d.ed.value = x.src; d.changed(); } },
    h('div', { class: 'line1' }, h('span', { class: 'ic k-' + x.kind, html: svg(x.kind === 'python' ? 'filepy' : 'filesql', 14) }), h('span', { class: 'nm' }, x.src.split('\n').find(l => l.trim()) || ''), h('span', { class: 'meta ' + (x.ok ? '' : 'bad') }, x.ok ? secs(x.ms) : 'failed')),
    h('div', { class: 'sub' }, `${x.where} · ${new Date(x.at).toLocaleTimeString()}`));
  return [head('clock', 'Runs', 'jobs and schedules on the node, and what this page ran'),
    h('div', { class: 'dsect' }, 'On the node'), ...node.length ? node.map(nodeRun) : [h('div', { class: 'empty' }, 'No job yet: a file\'s ⋯ runs it as one.')],
    tasks.length ? h('div', { class: 'dsect' }, 'Schedules') : null, ...tasks.map(task),
    h('div', { class: 'dsect' }, 'This page'), ...S.ran.length ? S.ran.map(page) : [h('div', { class: 'empty' }, 'Nothing run yet.')]];
}
/** A file (or a saved notebook) run on the node, not waited for (ADR-033): now, as a job
 * (`pondra.start('run', …)`), or on a schedule, as a task. What is saved runs, with the SQL file's
 * parameters as they are now; Runs shows it. */
async function job(doc, every) {
  if (doc.dirty && !(await doc.save())) return;
  if (every && !(every = await prompt('Schedule', 'How often', '1 hour', 'For example 15 minutes, 1 day, or cron 0 2 * * * UTC. It runs on the node as CALL run(…), and Runs lists it.'))) return;
  const path = doc.kind === 'notebook' ? `notebooks/${doc.name}` : doc.path, name = path.replace(/\.[^./]+$/, '').replace(/\W+/g, '_').replace(/^_+|_+$/g, '').toLowerCase() || 'job';
  const args = quote(path) + Object.entries(doc.params?.() || {}).map(([n, v]) => `, ${ident(n)} => ${typeof v === 'string' ? quote(v) : String(v).toUpperCase()}`).join('');
  try {
    await run(every ? `CREATE OR REPLACE TASK ${ident(name)} SCHEDULE ${quote(every)} AS CALL run(${args})` : `SELECT pondra.start('run', ${args})`);
    toast(every ? `Scheduled: ${name}, every ${every}` : `Started on the node: ${path}`); show('runs');
  } catch (e) { toast(e.message, true); }
}
Object.assign(H, { job, schedule: doc => job(doc, true) });
on('ran', (who, r, what) => {
  const src = what?.src ?? who?.src ?? '';
  if (!src.trim()) return;
  S.ran.unshift({ kind: what?.kind || who?.kind || 'sql', src, ms: r?.ms || 0, ok: r?.kind !== 'error', at: Date.now(), where: S.doc?.title || '' });
  S.ran.length = Math.min(S.ran.length, 100);
  if (S.tab === 'runs') detail();
  if (r?.kind === 'done' || what?.kind === 'python' || who?.kind === 'python') later(refresh);
  if (what?.kind === 'python' || who?.kind === 'python') readVars().then(() => S.tab === 'variables' && detail(), () => {});
});

// ------------------------------------------------------------------ the node: its stats, the database, refreshing
async function stats() {
  try {
    const s = await (await call('/stats')).json();
    S.lake = s.lake; S.info = s; S.down = false;
    if (!S.pick) detail();
    const nodes = s.nodes || [], who = MODE === 'lakes' ? S.db : s.lake;
    $('#where').innerHTML = `<span class="dot"></span><b>${esc(who || '')}</b> ${nodes.length > 1 ? `· ${esc(s.role)} · ${nodes.length} nodes` : 'on this machine'}${s.live_queries ? ` · ${s.live_queries} live` : ''}`;
    $('#where').title = `This database's cluster: ${nodes.join(', ')}. Its leader is ${s.leader}; commits so far: ${s.hwm}.`;
  } catch (e) {
    S.down = true;
    $('#where').innerHTML = '<span class="dot off"></span>not reachable';
    $('#where').title = e.message;
  }
  status();
}
async function use(db) {
  for (const d of S.docs) if (d.kind === 'notebook') for (const c of d.cells) c.stopLive();
  S.db = db;
  await stats();
  S.pick = null;
  await refreshViews();
  toolbar(); hashNow();
  toast(`Everything now runs in database ${db}`);
}
let pending;
const later = f => { clearTimeout(pending); pending = setTimeout(f, 250); };
async function refresh() { await stats(); await refreshViews(); emit('refresh'); }
H.refresh = refresh;
H.refreshFiles = () => refreshViews(['workspace']);
let wsTimer;
/** The Workspace again, from the files it has (a tab opened, closed, saved or changed). */
const laterWorkspace = () => { clearTimeout(wsTimer); wsTimer = setTimeout(() => { const v = R.views.find(x => x.id === 'workspace'); if (v?.box && S.files) drawWorkspace(v.box); mark(); }, 120); };
on('changed', doc => {
  if (!doc) return;
  if (doc.dirty !== doc.shown) { doc.shown = doc.dirty; drawTabs(); if (doc === S.doc) toolbar(); }
  if (doc === S.doc) status();
  laterWorkspace();
});
on('saved', (doc, path) => {
  if (path && S.files && !S.files.some(f => f.path === path)) { S.files.push({ path, written: new Date().toISOString().slice(0, -1) }); laterWorkspace(); } // (in the tree now; its size with the listing)
  toolbar(); remember(); hashNow(); later(() => refreshViews(['workspace']));
});

// ------------------------------------------------------------------ search (Ctrl K): tables, files, commands
function palette() {
  const d = $('#palette'), input = $('#palIn'), list = $('#palList');
  let items = [], on = 0;
  const all = () => [
    ...R.commands.size ? [...R.commands.values()].map(c => ({ kind: 'command', icon: c.icon || 'keyboard', label: c.title, note: c.keys || 'command', run: c.run })) : [],
    ...(S.objects || []).map(t => ({ kind: 'table', icon: (KIND[t.o.kind] || KIND.table)[0], label: t.q, note: (KIND[t.o.kind] || KIND.table)[1], run: () => { pick({ type: 'object', t }); } })),
    ...(S.files || []).filter(f => !/^files\/notebooks\/[^/]+\/[^/]+\.ipynb$/.test(f.path)).map(f => ({ kind: 'file', icon: iconOf(f.path), label: f.path.replace(/^files\//, ''), note: bytes(f.size), run: () => openFile(f.path) })),
    ...[...new Set((S.files || []).map(f => f.path.match(/^files\/notebooks\/([^/]+)\//)?.[1]).filter(Boolean))].map(n => ({ kind: 'notebook', icon: 'notebook', label: `notebooks/${n}.ipynb`, note: 'notebook', run: () => openFile('notebooks/' + n) })),
  ];
  const score = (text, q) => { if (!q) return 1; let i = 0; const t = text.toLowerCase(); for (const ch of q) { i = t.indexOf(ch, i); if (i < 0) return 0; i++; } return t.includes(q) ? 2 + (t.startsWith(q) ? 1 : 0) : 1; };
  const draw = () => {
    const q = input.value.trim().toLowerCase();
    items = all().map(x => ({ ...x, s: score(x.label, q) })).filter(x => x.s).sort((a, b) => b.s - a.s || a.label.length - b.label.length).slice(0, 60);
    on = Math.min(on, Math.max(0, items.length - 1));
    list.replaceChildren(...items.length ? items.map((x, i) => h('div', { class: 'pi' + (i === on ? ' on' : ''), role: 'option', 'aria-selected': String(i === on), onmousedown: e => { e.preventDefault(); on = i; go(); } },
      h('span', { class: 'ic k-' + x.kind, html: svg(x.icon, 15) }), h('span', { class: 'nm' }, x.label), h('span', { class: 'meta' }, x.note))) : [h('div', { class: 'empty' }, 'Nothing matches.')]);
    list.children[on]?.scrollIntoView({ block: 'nearest' });
  };
  const go = () => { const x = items[on]; d.close(); x?.run(); };
  input.value = ''; on = 0; draw();
  input.oninput = () => { on = 0; draw(); };
  input.onkeydown = e => {
    const mv = { ArrowDown: 1, ArrowUp: -1 }[e.key];
    if (mv) { e.preventDefault(); on = (on + mv + items.length) % Math.max(1, items.length); draw(); }
    else if (e.key === 'Enter') { e.preventDefault(); go(); }
  };
  d.showModal();
}

// ------------------------------------------------------------------ settings, sign-in, keys
function settings() {
  const d = $('#settingsDlg'), set = (id, v) => { $(id).value = v; };
  set('#setTheme', prefs('theme') || 'system'); set('#setGroups', prefs('workspaceFirst') ? 'workspace' : 'data'); set('#setFont', prefs('font') || 'geist'); set('#setStatements', prefs('statements') || 'each');
  d.onchange = () => { prefs('theme', $('#setTheme').value); prefs('workspaceFirst', $('#setGroups').value === 'workspace'); prefs('font', $('#setFont').value); prefs('statements', $('#setStatements').value); look(); drawLeft(); };
  $('#setReset').onclick = () => { for (const k of ['sides', 'folded', 'weights', 'widths', 'left', 'right', 'bottom']) prefs(k, null); store.set('pondra.split', null); $('#left').style.width = $('#right').style.width = ''; drawViews(); pane('left', true); toast('The layout is back as it was'); };
  d.showModal();
}
function signin() {
  const token = T.token();
  if (!token) return askToken('The token this node was started with (--admin-token, --write-token or --read-token)');
  menu($('#signin'), [{ label: 'Change the token…', icon: 'key', run: () => askToken('The token this node was started with') }, { label: 'Sign out (forget the token)', icon: 'close', run: () => { store.set('pondra.token', null); drawSignin(); refresh(); } }]);
}
function drawSignin() { const b = $('#signin'), on = !!T.token(); b.replaceChildren(icon(on ? 'key' : 'user'), on ? 'Signed in' : 'Sign in'); b.classList.toggle('on', on); }
function askToken(why) {
  const d = $('#tokenDlg');
  if (d.open) return;
  $('#tokenWhy').textContent = `${why}. The token is kept in this browser only.`;
  $('#tokenIn').value = store.get('pondra.token') || '';
  d.returnValue = '';
  d.showModal();
}
ask.token = askToken;
$('#tokenDlg').addEventListener('close', () => {
  const v = $('#tokenDlg').returnValue;
  if (v === 'ok' && $('#tokenIn').value.trim()) store.set('pondra.token', $('#tokenIn').value.trim());
  else if (v === 'clear') store.set('pondra.token', null);
  else return;
  drawSignin(); refresh();
});
function drawKeys() {
  const groups = [...new Set(R.keys.map(k => k.group))];
  $('#keys').replaceChildren(...groups.flatMap(g => [h('h4', {}, g), ...R.keys.filter(k => k.group === g).flatMap(k => [h('span', {}, ...k.keys.split(' ').map(x => h('kbd', {}, x))), h('span', {}, k.title)])]));
}
function drawActions() {
  $('#actions').replaceChildren(...R.actions.filter(a => !a.menu && !a.hidden?.()).map(a => h('button', { class: a.label ? 'btn' + (a.primary ? ' primary' : '') : 'icon', id: a.id + 'Btn', title: a.title, 'aria-label': a.title, onclick: e => a.run(e) }, a.icon ? icon(a.icon) : null, a.label || null)));
}
function moreMenu(at) {
  menu(at, [{ label: 'Settings…', icon: 'settings', run: settings }, '-',
    ...R.actions.filter(a => a.menu && !a.hidden?.()).flatMap(a => [a.sep ? '-' : null, { label: a.title, icon: a.icon, keys: a.keys, run: a.run }])]);
}
function drawRail() {
  $('#rail').hidden = !R.nav.length;
  $('#rail').replaceChildren(...R.nav.map(n => h('button', { class: 'rb' + (S.place === n.id ? ' on' : ''), title: n.label, 'aria-label': n.label, onclick: () => { S.place = n.id; drawRail(); n.run(); }, html: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${n.icon || ICONS.dots}</svg><span>${esc(n.label)}</span>` })));
}
let started = false, drawing = 0;
shell.redraw = () => { if (!drawing) drawing = requestAnimationFrame(() => { drawing = 0; if (started) { drawActions(); drawViews(); drawRail(); drawKeys(); } }); };

// ------------------------------------------------------------------ keys: the page's, the tree's and the tabs' (arrow keys, one Tab stop each)
document.addEventListener('keydown', e => {
  const mod = e.ctrlKey || e.metaKey, t = e.target, k = e.key.toLowerCase();
  if (mod && k === 's') { e.preventDefault(); S.doc?.save?.(); return; }
  if (mod && k === 'k') { e.preventDefault(); palette(); return; }
  if (mod && !e.altKey && k === 'b') { e.preventDefault(); pane('left'); return; }
  if (mod && e.altKey && k === 'b') { e.preventDefault(); pane('right'); return; }
  if (mod && k === 'j') { e.preventDefault(); if (S.doc?.hasPanel) pane('bottom'); return; }
  if (t.closest?.('textarea,input,select,dialog,[contenteditable]')) return; // (typing)
  if (t.closest?.('[role=tree]')) { treeKeys(e); return; }
  if (t.closest?.('[role=tablist]')) { tabKeys(e); return; }
  if (t.closest?.('button,a,summary,.grid')) return; // (a control's own keys)
  if (e.key === '?') { e.preventDefault(); $('#helpDlg').showModal(); return; }
  if (S.doc?.onkey && (t === document.body || t.closest?.('#docs'))) S.doc.onkey(e);
});
function treeKeys(e) {
  const tree = e.target.closest('[role=tree]'), rows = [...tree.querySelectorAll('[role=treeitem]')].filter(r => r.offsetParent), i = rows.indexOf(e.target.closest('[role=treeitem]'));
  const go = j => { const r = rows[Math.max(0, Math.min(rows.length - 1, j))]; if (!r) return; rows.forEach(x => x.tabIndex = -1); r.tabIndex = 0; r.focus(); };
  const row = rows[i], open = row?.getAttribute('aria-expanded');
  const acts = { ArrowDown: () => go(i + 1), ArrowUp: () => go(i - 1), Home: () => go(0), End: () => go(rows.length - 1),
    ArrowRight: () => open === 'false' ? (row.toggle ? row.toggle() : row.querySelector('.tw')?.click()) : open === 'true' ? go(i + 1) : null,
    ArrowLeft: () => { if (open === 'true') { row.toggle?.(); return; } const lvl = +row.getAttribute('aria-level'); for (let j = i - 1; j >= 0; j--) if (+rows[j].getAttribute('aria-level') < lvl) return go(j); },
    Enter: () => row.click(), ' ': () => row.click() };
  if (acts[e.key]) { e.preventDefault(); acts[e.key](); }
}
function tabKeys(e) {
  const list = e.target.closest('[role=tablist]'), tabs = [...list.querySelectorAll('[role=tab]')], i = tabs.indexOf(e.target.closest('[role=tab]'));
  const j = { ArrowRight: i + 1, ArrowLeft: i - 1, Home: 0, End: tabs.length - 1 }[e.key];
  if (j != null) { e.preventDefault(); const t = tabs[(j + tabs.length) % tabs.length]; t.click(); requestAnimationFrame(() => (list.querySelector('[aria-selected=true]') || t).focus()); } // (the list is drawn again: its tab in front)
  else if (e.key === 'Delete' && e.target.closest('#tabbar')) { const d = S.docs[+e.target.closest('.tab').dataset.i]; if (d) closeDoc(d); }
}
// (a tree takes one Tab stop: its first row, or the one last focused)
document.addEventListener('focusin', e => { const tree = e.target.closest?.('[role=tree]'); if (tree && !tree.querySelector('[role=treeitem][tabindex="0"]')) tree.querySelector('[role=treeitem]')?.setAttribute('tabindex', '0'); });

// ------------------------------------------------------------------ what the core registers (as an extension would)
function core() {
  register.cellKind({ id: 'sql', label: 'SQL', language: 'sql', placeholder: 'SELECT …', live: true, run: (text, signal) => run(text, signal) });
  register.cellKind({ id: 'python', label: 'Python', language: 'python', placeholder: 'db.sql("SELECT …")      # runs on the node; cells share variables', run: async (text, signal) => { kernel('busy'); try { return await run(doBlock(text), signal); } finally { kernel('idle'); } } });
  register.cellKind({ id: 'markdown', label: 'Text', language: 'markdown', placeholder: 'Text, in Markdown' });
  register.renderer({ id: 'error', order: 10, match: r => r.kind === 'error', render: r => h('pre', { class: 'err' }, r.message) });
  register.renderer({ id: 'rows', order: 20, match: r => r.kind === 'rows', render: (r, cell) => grid(r, { footer: !!cell, name: S.doc?.name, explore: i => explore(r, i, cell) }) });
  register.renderer({ id: 'figures', order: 30, match: r => r.kind === 'done' && Array.isArray(r.value?.images), render: r => h('div', { class: 'figs' }, r.value.images.map(b => h('img', { class: 'fig', alt: 'a figure the code drew', src: 'data:image/png;base64,' + b }))) });
  register.renderer({ id: 'text', order: 40, match: r => r.kind === 'text', render: r => said(r.text) });
  register.renderer({ id: 'done', order: 90, match: r => r.kind === 'done', render: r => { const d = doneText(r.value) || (r.notices?.length ? '' : 'Done.'); return d && !(d === 'Done.' && r.notices?.length) ? h('div', { class: 'done' }, d) : null; } });
  register.view({ id: 'data', side: 'left', order: 10, title: () => MODE === 'lakes' ? 'Databases' : 'Data', render: box => dataTree(box), tools: [
    { icon: 'plus', title: 'New database', domId: 'newdb', hidden: () => MODE !== 'lakes', run: newDatabase },
    { icon: 'refresh', title: 'Refresh', domId: 'refresh', run: () => refresh() }] });
  register.view({ id: 'workspace', side: 'left', order: 20, title: 'Workspace', render: box => workspace(box), tools: [
    { icon: 'plus', title: 'New: a notebook, a SQL or a Python file', domId: 'newfile', run: e => newMenu(e.currentTarget) }] });
  register.view({ id: 'details', side: 'right', order: 10, title: 'Details', tree: false, render: (box, p) => p?.type === 'object' ? objectDetail(p.t) : p?.type === 'file' ? fileDetail(p.f) : p?.type === 'result' ? resultDetail(p) : summary() });
  register.view({ id: 'variables', side: 'right', order: 20, title: 'Variables', tree: false, render: () => variables() });
  register.view({ id: 'runs', side: 'right', order: 30, title: 'Runs', tree: false, render: () => runs() });
  registerFiles(register);
  register.action({ id: 'newnb', order: 100, menu: true, icon: 'notebook', title: 'New notebook', run: () => newNotebook() });
  register.action({ id: 'newsql', order: 101, menu: true, icon: 'filesql', title: 'New SQL file', run: () => newFile('sql') });
  register.action({ id: 'newpy', order: 102, menu: true, icon: 'filepy', title: 'New Python file', run: () => newFile('python') });
  register.action({ id: 'upload', order: 120, menu: true, icon: 'up', title: 'Open an .ipynb or put a file in the lake…', run: () => upload() });
  register.action({ id: 'restart', order: 140, menu: true, sep: true, icon: 'restart', title: 'Restart Python', keys: '0 0', run: restart });
  register.action({ id: 'token', order: 150, menu: true, icon: 'key', title: 'Token…', run: () => askToken('The token this node was started with') });
  register.action({ id: 'keys', order: 160, menu: true, icon: 'keyboard', title: 'Keys', keys: '?', run: () => $('#helpDlg').showModal() });
  for (const [id, title, keys, fn] of [['search', 'Search tables, files and commands', 'Ctrl K', palette], ['left', 'Show or hide the left pane', 'Ctrl B', () => pane('left')], ['bottom', 'Show or hide the bottom panel', 'Ctrl J', () => pane('bottom')],
    ['right', 'Show or hide the right pane', 'Ctrl Alt B', () => pane('right')], ['settings', 'Settings', '', settings], ['newnb', 'New notebook', '', () => newNotebook()], ['newsql', 'New SQL file', '', () => newFile('sql')],
    ['newpy', 'New Python file', '', () => newFile('python')], ['restart', 'Restart Python', '', restart], ['refresh', 'Refresh the catalog', '', refresh], ['keys', 'Keys', '?', () => $('#helpDlg').showModal()]]) register.command({ id, title, keys, run: fn });
  const cellKey = (keys, title, fn) => register.key({ keys, title, run: fn, group: 'On a cell (after Esc)' });
  for (const [keys, title] of [['Ctrl K', 'Search tables, files and commands'], ['Ctrl S', 'Save the file or notebook in front'], ['Ctrl B', 'The left pane'], ['Ctrl J', 'The bottom panel'], ['Ctrl Alt B', 'The right pane'], ['?', 'These keys']]) register.key({ keys, title, group: 'Anywhere' });
  for (const [keys, title] of [['Ctrl Enter', 'Run it (a file: what is selected, or all of it)'], ['Shift Enter', 'Run it and go to the next cell'], ['Alt Enter', 'Run it and add a cell below'], ['Ctrl Shift Enter', 'Run every cell'], ['Tab', 'Complete a name (or indent)'], ['Ctrl Space', 'Complete a name'], ['Ctrl /', 'Comment the lines out, or in'], ['Esc', 'Leave the cell: the keys below then work']]) register.key({ keys, title, group: 'In a cell or a file' });
  cellKey('Enter', 'Edit it', c => c.edit());
  cellKey('↑ ↓', 'The cell above, below (or K J)');
  cellKey('A B', 'Add a cell above, below');
  cellKey('D D', 'Delete it (Z brings it back)');
  cellKey('S P M', 'Make it SQL, Python, text');
  cellKey('L', 'Live on or off: its answer again after each commit that changes it');
  cellKey('O', 'Hide or show its output');
  cellKey('0 0', 'Restart Python: its variables go');
  for (const [keys, title] of [['Click', 'A cell: its row lights up'], ['Shift Click', 'A range'], ['Ctrl C', 'Copy, tab-separated (Shift: with the headers)'], ['Ctrl A', 'Select every cell'], ['Enter', 'Edit a data file\'s cell']]) register.key({ keys, title, group: 'In a grid' });
}

// ------------------------------------------------------------------ the page's API, and starting
/** A tree row for an extension's view (ADR-032's `ui.line`): `line(null, { title, onclick }, ...kids)`. */
const line = (_tw, attrs, ...kids) => h('div', { class: 'row', role: 'treeitem', tabindex: '-1', ...attrs }, h('span', { class: 'tw none' }), ...kids);
function addCell(o) { const nb = S.doc?.kind === 'notebook' ? S.doc : S.nb && S.docs.includes(S.nb) ? (activate(S.nb), S.nb) : newNotebook(); return nb.add(o); }
const pondra = {
  state: S, session: SESSION, mode: MODE, version: VERSION,
  api: { call, run, rows, base, sql: run },
  ui: { h, icon, icons: ICONS, line, button: act, toast, menu, prompt, pick, detail, panel: open => pane('right', open), refresh: () => refresh(), add: addCell, cells: () => S.cells.slice(),
    notebook: () => (S.doc?.kind === 'notebook' ? S.doc : S.nb)?.notebook(), open: (nb, name) => H.openNotebook(nb, name), openFile, grid, activate, pane },
  docs: () => S.docs.slice(), register, on, emit, configure,
};
window.pondra = pondra;
export { pondra };

addEventListener('beforeunload', e => { if (S.docs.some(d => d.dirty && (d.kind !== 'notebook' || d.cells.some(c => c.src.trim())))) { e.preventDefault(); e.returnValue = ''; } });
addEventListener('pagehide', () => {
  const token = T.token();
  T.fetch(base() + '/sessions/' + SESSION, { method: 'DELETE', keepalive: true, headers: { ...T.headers(), ...(token ? { authorization: 'Bearer ' + token } : {}) } }).catch(() => {}); // (this page's temporary tables, and its Python)
});
let wasNarrow = narrow();
addEventListener('resize', () => { // (into a narrow window the side panes become drawers, closed; back out, they are as they were)
  if (narrow() !== wasNarrow) { wasNarrow = narrow(); sides(true); }
  drawPanes();
});
const sides = draw => {
  $('#left').hidden = narrow() || prefs('left') === false;
  $('#right').hidden = narrow() || (prefs('right') == null ? innerWidth < 1180 : !prefs('right'));
  if (draw && !$('#right').hidden) drawRight();
};
$('#main').addEventListener('pointerdown', () => { if (narrow()) { for (const w of ['left', 'right']) if (paneOpen(w)) pane(w, false); } }); // (a drawer closes when the page behind it is used)

async function start() {
  const hash = new URLSearchParams(location.hash.slice(1));
  if (MODE === 'lakes') S.db = hash.get('db');
  look(); core();
  $('#search').onclick = palette;
  $('#moreBtn').onclick = e => moreMenu(e.currentTarget);
  $('#helpBtn').onclick = () => $('#helpDlg').showModal();
  $('#runsBtn').onclick = () => show('runs');
  $('#signin').onclick = signin;
  edges();
  sides();
  drawSignin(); drawActions(); drawRail(); drawKeys(); drawPanes(); status();
  // (extensions, loaded after these modules, register meanwhile: drawn with the core's from here on)
  const data = R.views.find(v => v.id === 'data');
  if (MODE === 'lakes') { data.drawn = true; await renderView(data); await stats(); drawLeft(); } // (the tree picks the database /stats is asked of)
  else { await stats(); drawLeft(); }
  drawRight();
  started = true;
  drawActions(); drawRail(); drawKeys();
  const nb = hash.get('notebook'), file = hash.get('file');
  if (nb) await openFile('notebooks/' + nb);
  else if (file) await openFile(file);
  if (!S.docs.length) await restoreTabs();
  if (!S.docs.length) newNotebook();
  setInterval(() => { if (document.visibilityState === 'visible') stats(); }, 15000);
  emit('start', pondra);
}
start();
