// The Workspace and its files (ADR-034): the lake's own files, as a tree; SQL, Python, data and
// text files open in tabs, are edited there and saved back in place (`PUT /files` with
// `If-Match`: replaced only if nobody saved it meanwhile).
import { h, fill, icon, svg, esc, secs, count, bytes, ago, utc, S, R, emit, call, run, rows, doBlock, fileUrl, fileSql, quote, ident, toast, menu, prompt, confirmed, saveAs, MODE, DATA, Failure, store } from './core.js';
import { Editor, formatSql } from './editor.js';
import { grid, chart, toCsv, copyText } from './grid.js';
import { answer, doneText, cleanName } from './notebook.js';

const base = p => p.split('/').pop();
const dir = p => p.includes('/') ? p.slice(0, p.lastIndexOf('/')) : '';
/** A file's kind, by its name: what opens it and which icon it has. */
export const kindOf = p => /\.sql$/i.test(p) ? 'sql' : /\.py$/i.test(p) ? 'python' : DATA.test(p) ? 'data' : /\.(md|txt)$/i.test(p) ? 'text' : /\.ipynb$/i.test(p) ? 'notebook' : 'file';
export const iconOf = p => ({ sql: 'filesql', python: 'filepy', data: 'filedata', notebook: 'notebook', text: 'file', file: 'file' })[kindOf(p)];

// ------------------------------------------------------------------ the Workspace view: the lake's files, as a tree
/** The lake's files by folder. A notebook's versions (`notebooks/<name>/<time>.ipynb`) are one
 * entry, `<name>.ipynb`; the open one shows its outline under it. */
export async function workspace(box) {
  try { S.files = await rows(`SELECT path, size, written FROM files() ORDER BY path`); } catch (e) {
    box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to list the files.' : e.message));
    return;
  }
  drawWorkspace(box);
}
/** The tree again from the listing it has (a tab opened, closed, saved or changed). */
export function drawWorkspace(box) {
  const list = S.files || [];
  const root = { dirs: new Map(), files: [] };
  const into = parts => { let d = root; for (const p of parts) d = d.dirs.get(p) || d.dirs.set(p, { dirs: new Map(), files: [] }).get(p); return d; };
  const notebooks = new Map();
  for (const f of list) {
    const rel = f.path.replace(/^files\//, ''), nb = rel.match(/^notebooks\/([^/]+)\/([^/]+)\.ipynb$/);
    if (nb) { const n = notebooks.get(nb[1]) || notebooks.set(nb[1], { name: nb[1], written: f.written, n: 0 }).get(nb[1]); n.n++; if (f.written > n.written) n.written = f.written; continue; }
    const parts = rel.split('/');
    into(parts.slice(0, -1)).files.push({ ...f, rel, name: parts.at(-1) });
  }
  if (notebooks.size) into(['notebooks']).files.push(...[...notebooks.values()].map(n => ({ rel: `notebooks/${n.name}`, name: n.name + '.ipynb', written: n.written, versions: n.n, notebook: true })));
  const openNb = S.docs.filter(d => d.kind === 'notebook' && !d.version).map(d => ({ rel: d.path, name: d.title, notebook: true, unsaved: true }));
  if (openNb.length) into(['notebooks']).files.unshift(...openNb.filter(o => !notebooks.has(o.rel.slice(10))));
  const render = (d, at, depth) => [
    ...[...d.dirs.keys()].sort().map(n => {
      const key = 'dir:' + at + n, front = !!S.doc?.path?.startsWith(at + n + '/') && !S.open.has('closed:' + key); // (the folders of the file in front are open)
      const kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(key) && !front }, render(d.dirs.get(n), at + n + '/', depth + 1));
      return treeItem({ key, kids, depth, icon: 'folder', name: n, title: at + n, onclick: tw => tw.click(), menu: e => folderMenu(e, at + n) });
    }),
    ...d.files.sort((a, b) => a.name.localeCompare(b.name)).map(f => {
      const doc = S.docs.find(x => x.path === f.rel), kind = f.notebook ? 'notebook' : kindOf(f.rel), heads = doc?.kind === 'notebook' ? doc.outline() : [];
      const kids = heads.length ? h('div', { class: 'kids outline', role: 'group', hidden: S.doc !== doc && !S.open.has('nb:' + f.rel) }, heads.map(x =>
        treeItem({ depth: depth + 1, icon: 'hash', name: x.text, cls: 'h' + x.level, title: x.text, onclick: () => { R.helpers.activate(doc); doc.goto(x.c); } }))) : null;
      return treeItem({ key: 'nb:' + f.rel, kids, depth, icon: iconOf(f.notebook ? 'x.ipynb' : f.rel), iconCls: 'k-' + kind, name: f.name, dataKey: 'file:' + f.rel, cls: doc && S.doc === doc ? 'front' : '',
        meta: kind === 'data' || kind === 'file' ? bytes(f.size) : f.versions > 1 ? `${f.versions} versions` : '', dirty: doc?.dirty,
        title: f.notebook ? `${f.name}: ${f.versions || 0} version${f.versions === 1 ? '' : 's'}` : `files/${f.rel} · ${bytes(f.size)} · written ${f.written ? utc(f.written).toLocaleString() : ''}`,
        onclick: () => kind === 'file' ? R.helpers.pick({ type: 'file', f }) : R.helpers.openFile(f.rel),
        menu: e => fileMenu(e, f, kind) });
    }),
  ];
  const tree = render(root, '', 0);
  box.replaceChildren(...tree.length ? tree : [h('div', { class: 'empty' }, 'No files yet. + makes a notebook, a SQL or a Python file, or uploads one.')]);
}
/** A row of a tree: its twisty (it folds, when it has kids), icon, name, a note, and the unsaved dot. */
export function treeItem({ key, kids, depth = 0, icon: ic, iconCls = '', name, meta, dirty, on, title, onclick, ondblclick, menu: onmenu, cls = '', dataKey, dataKind }) {
  const open = kids && !kids.hidden;
  const tw = h('span', { class: 'tw' + (kids ? '' : ' none'), 'aria-hidden': 'true', html: kids ? svg('chev', 14, 2) : '' });
  const row = h('div', { class: `row ${cls}${on ? ' on' : ''}`, role: 'treeitem', tabindex: '-1', 'aria-level': String(depth + 1), 'aria-expanded': kids ? String(!!open) : null, title,
    'data-key': dataKey, 'data-kind': dataKind, style: `padding-left:${6 + depth * 14}px` },
    tw, h('span', { class: 'ic ' + iconCls, html: svg(ic, 16) }), h('span', { class: 'nm' }, name), meta ? h('span', { class: 'meta' }, meta) : null,
    dirty ? h('span', { class: 'dirty', title: 'Not saved', 'aria-label': 'not saved' }) : null);
  const toggle = () => {
    if (!kids) return;
    kids.hidden = !kids.hidden;
    row.setAttribute('aria-expanded', String(!kids.hidden));
    if (key) kids.hidden ? (S.open.delete(key), S.open.add('closed:' + key)) : (S.open.add(key), S.open.delete('closed:' + key));
  };
  tw.click = toggle;
  row.addEventListener('click', e => { if (e.target.closest('.tw')) toggle(); else onclick?.(tw, e); });
  if (ondblclick) row.addEventListener('dblclick', ondblclick);
  if (onmenu) row.addEventListener('contextmenu', e => { e.preventDefault(); onmenu(e); });
  row.toggle = toggle;
  return kids ? h('div', { class: 'item' }, row, kids) : row;
}
function fileMenu(e, f, kind) {
  menu(e, [kind !== 'file' ? { label: 'Open', icon: iconOf(f.notebook ? 'x.ipynb' : f.rel), run: () => R.helpers.openFile(f.rel) } : null,
    { label: 'Details', icon: 'eye', run: () => R.helpers.pick({ type: 'file', f }) },
    kind === 'data' ? { label: 'Query with SQL', icon: 'play', run: () => R.helpers.query(`SELECT * FROM ${fileSql(f.rel)} LIMIT 1000`) } : null, '-',
    !f.notebook ? { label: 'Rename…', icon: 'pencil', run: () => rename(f.rel) } : null,
    !f.notebook ? { label: 'Download', icon: 'down', run: () => download(f.rel) } : null,
    { label: 'Copy the path', icon: 'copy', run: () => copyText('files/' + f.rel, 'Path copied') }, '-',
    { label: f.notebook ? 'Delete every version…' : 'Delete…', icon: 'trash', run: () => remove(f) }]);
}
function folderMenu(e, at) {
  menu(e, [{ label: 'New SQL file here', icon: 'filesql', run: () => R.helpers.newFile('sql', at + '/') }, { label: 'New Python file here', icon: 'filepy', run: () => R.helpers.newFile('python', at + '/') },
    { label: 'Upload a file here…', icon: 'up', run: () => upload(at + '/') }]);
}
export async function download(rel) {
  try { const r = await call(fileUrl(rel)); saveAs(await r.blob(), 'application/octet-stream', base(rel)); } catch (e) { toast(e.message, true); }
}
/** A file from this computer into the lake's files (a notebook opens; the rest are put). */
export function upload(at = '') {
  const input = h('input', { type: 'file', hidden: true, multiple: true });
  input.onchange = async () => {
    const fs = [...input.files];
    input.remove();
    for (const f of fs) {
      if (/\.ipynb$/i.test(f.name)) { try { R.helpers.openNotebook(JSON.parse(await f.text()), cleanName(f.name) || 'uploaded'); toast(`Opened ${f.name}: Ctrl+S keeps it in the lake`); } catch (err) { toast(`Could not open ${f.name}: ${err.message}`, true); } continue; }
      const rel = at + f.name;
      try { await call(fileUrl(rel), { method: 'PUT', body: f }); toast(`Put in the lake: files/${rel}`); } catch (err) {
        if (err.status !== 409) { toast(`${f.name}: ${err.message}`, true); continue; }
        if (!confirmed(`files/${rel} is there already. Replace it with the one picked?`)) continue; // (a file is replaced only as it is: its version asked for)
        const version = ((await call(fileUrl(rel), { method: 'HEAD' })).headers.get('etag') || '').replace(/"/g, '');
        if (await writeFile(rel, f, version, f.type || 'application/octet-stream')) toast(`Replaced files/${rel}`);
      }
    }
    R.helpers.refreshFiles();
  };
  document.body.append(input); input.click();
}
async function rename(rel) {
  const to = await prompt('Rename', 'The new path, under the lake\'s files', rel);
  if (!to || to === rel) return;
  try {
    const r = await call(fileUrl(rel));
    await call(fileUrl(to), { method: 'PUT', body: await r.blob() });
    await call(fileUrl(rel), { method: 'DELETE' });
    const doc = S.docs.find(d => d.path === rel);
    if (doc) { doc.path = to; doc.version = null; await doc.reload?.(); R.helpers.drawTabs(); }
    toast(`Renamed to ${to}`);
  } catch (e) { toast('Not renamed: ' + e.message, true); }
  R.helpers.refreshFiles();
}
async function remove(f) {
  if (!confirm(f.notebook ? `Delete the notebook ${f.name}, every version of it?` : `Delete files/${f.rel}? This can't be undone.`)) return;
  try {
    const paths = f.notebook ? (await rows(`SELECT path FROM files('${f.rel.replace(/'/g, "''")}/')`)).map(x => x.path) : ['files/' + f.rel];
    for (const p of paths) await call(fileUrl(p), { method: 'DELETE' });
    const doc = S.docs.find(d => d.path === f.rel);
    if (doc) { doc.dirty = true; doc.version = null; doc.written = null; R.helpers.drawTabs(); }
    toast(`Deleted ${f.name}`);
  } catch (e) { toast('Not deleted: ' + e.message, true); }
  R.helpers.refreshFiles();
}

// ------------------------------------------------------------------ text files: SQL, Python, Markdown and text
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
    if (e.status === 412) toast(`Not saved: ${base(rel)} was saved by someone else since you opened it. Save it under another name (⋯), or open it again.`, true);
    else if (e.status === 409) toast(`Not saved: files/${rel} is there already. Open it, or pick another name.`, true);
    else toast('Not saved: ' + e.message, true);
    return null;
  }
}

class TextDoc {
  constructor({ path = null, text = '', version = null, kind = 'text', language = 'text', untitled = 'untitled.txt' }) {
    Object.assign(this, { path, version, kind, dirty: false, untitled, pos: { line: 1, col: 1 } });
    this.icon = iconOf(path || untitled);
    this.ed = new Editor({ gutter: true, language, value: text, label: this.title, oninput: () => this.changed(), onkey: e => this.key(e), oncursor: (line, col) => { this.pos = { line, col }; R.helpers.status(); } });
    this.main = h('div', { class: 'pane-ed' }, this.ed.el);
    this.el = h('div', { class: 'doc filedoc' }, this.main);
  }
  get title() { return base(this.path || this.untitled); }
  changed() { if (!this.dirty) { this.dirty = true; emit('changed', this); } }
  key(e) {
    const mod = e.ctrlKey || e.metaKey;
    if (mod && e.key === 'Enter' && this.run) { e.preventDefault(); this.run(); return true; }
    return false;
  }
  put(text) { this.ed.insert(text); }
  activate() { requestAnimationFrame(() => this.ed.focus()); }
  close() { return !this.dirty || confirm(`Close ${this.title}? It has changes that are not saved.`); }
  async reload() { if (!this.path) return; const f = await readFile(this.path); this.ed.value = f.text; this.version = f.version; this.dirty = false; emit('changed', this); }
  /** Save it where it is (a new file asks where first). */
  async save(as) {
    let path = this.path;
    if (!path || as) {
      path = await prompt(as ? 'Save as' : 'Save', 'Where, under the lake\'s files', path || (this.kind === 'sql' ? 'queries/' : this.kind === 'python' ? 'scripts/' : '') + this.untitled);
      if (!path) return false;
      path = path.replace(/^\/?(files\/)?/, '');
      if (as || !this.path) this.version = null;
    }
    const v = await writeFile(path, this.ed.value, this.version);
    if (!v) return false;
    this.path = path; this.version = v; this.dirty = false; this.icon = iconOf(path);
    toast(`Saved: files/${path}`);
    emit('changed', this); emit('saved', this, 'files/' + path);
    return true;
  }
  crumbs() {
    const parts = (this.path || this.untitled).split('/');
    return [...parts.slice(0, -1).flatMap(p => [h('span', { class: 'crumb' }, p), h('span', { class: 'slash' }, '/')]), h('b', { class: 'crumb cur' }, parts.at(-1)),
      h('span', { class: 'said-saved' }, this.dirty ? (this.path ? 'Edited, not saved' : 'Not saved yet') : this.path ? 'Saved' : '')];
  }
  more() { return [{ label: 'Save as…', icon: 'save', run: () => this.save(true) }, this.path ? { label: 'Download', icon: 'down', run: () => saveAs(this.ed.value, 'text/plain', this.title) } : null]; }
  toolbar() { return [...this.crumbs(), h('span', { class: 'grow' }), btn('save', 'Save', 'Save it (Ctrl+S)', () => this.save()), moreBtn(() => this.more())]; }
  status() { return [`Ln ${this.pos.line}, Col ${this.pos.col}`, { sql: 'SQL', python: 'Python', text: /\.md$/i.test(this.title) ? 'Markdown' : 'Text' }[this.kind] || '', 'Spaces: 4']; }
}
const btn = (ic, label, title, fn, cls = 'btn', id) => h('button', { class: cls, title, onclick: fn, id }, ic ? icon(ic) : null, label);
const moreBtn = items => h('button', { class: 'icon', title: 'More', 'aria-label': 'More', onclick: e => menu(e.currentTarget, items()) }, icon('dots'));

/** A panel under a file (SQL's results, Python's console), its height dragged; or at the right. */
function splitPanel(doc, panel) {
  const grip = h('div', { class: 'grip', role: 'separator', 'aria-label': 'The panel\'s size (arrows change it)', tabindex: '0', 'aria-valuemin': '120' });
  const size = store.json('pondra.split', { below: 320, right: 520 });
  const place = () => {
    const right = doc.layout === 'right';
    doc.el.classList.toggle('side', right);
    panel.style.flexBasis = (right ? size.right : size.below) + 'px';
    grip.setAttribute('aria-orientation', right ? 'vertical' : 'horizontal'); grip.setAttribute('aria-valuenow', String(Math.round(right ? size.right : size.below)));
  };
  grip.addEventListener('pointerdown', e => {
    e.preventDefault(); grip.setPointerCapture(e.pointerId);
    const right = doc.layout === 'right', start = right ? e.clientX : e.clientY, was = right ? size.right : size.below;
    const move = ev => { const d = (right ? ev.clientX : ev.clientY) - start, v = Math.max(120, Math.min((right ? innerWidth : innerHeight) - 220, was - d)); panel.style.flexBasis = v + 'px'; size[right ? 'right' : 'below'] = v; };
    grip.addEventListener('pointermove', move);
    grip.addEventListener('pointerup', () => { grip.removeEventListener('pointermove', move); store.set('pondra.split', JSON.stringify(size)); }, { once: true });
  });
  grip.addEventListener('keydown', e => { const d = { ArrowUp: 24, ArrowDown: -24, ArrowLeft: 24, ArrowRight: -24 }[e.key]; if (d) { e.preventDefault(); const k = doc.layout === 'right' ? 'right' : 'below'; size[k] = Math.max(120, size[k] + d); place(); store.set('pondra.split', JSON.stringify(size)); } });
  doc.el.append(grip, panel);
  doc.place = place;
  place();
}

// ------------------------------------------------------------------ a SQL file: its results below
export class SqlDoc extends TextDoc {
  constructor(o = {}) {
    super({ ...o, kind: 'sql', language: 'sql', untitled: o.untitled || 'untitled.sql' });
    this.layout = store.get('pondra.results') || 'below';
    this.tab = 'results'; this.result = null;
    this.body = h('div', { class: 'pbody' });
    this.tabs = h('div', { class: 'ptabs', role: 'tablist' });
    this.info = h('div', { class: 'pinfo' });
    this.panel = h('section', { class: 'panel results', 'aria-label': 'Results' }, h('div', { class: 'phead' }, this.tabs, h('span', { class: 'grow' }), this.info), this.body);
    splitPanel(this, this.panel);
    this.draw();
  }
  get hasPanel() { return true; }
  /** Run the file, or what is selected: each statement its own answer (in order, stopping at a
   * failure), or — as Settings may say — all of it at once, the last one's answer. */
  async run() {
    const text = (this.ed.selected() || this.ed.value).trim();
    if (!text) return;
    this.ctl?.abort();
    const ctl = this.ctl = new AbortController(), each = (store.json('pondra.prefs', {}).statements || 'each') === 'each';
    const list = each ? statements(text) : [text];
    this.running = true; this.plan = null; this.results = []; this.result = null; this.todo = list.length; R.helpers.toolbar();
    this.body.replaceChildren(h('div', { class: 'wait pulse' }, 'Running…'));
    emit('run', { kind: 'sql', src: text, doc: this });
    let r;
    for (const sql of list.length ? list : [text]) {
      const t0 = performance.now();
      try { r = await run(sql, ctl.signal); } catch (e) { r = { kind: 'error', message: e.name === 'AbortError' ? 'Stopped waiting. (A statement already on its way may still finish on the node.)' : e.message, notices: [] }; }
      if (this.ctl !== ctl) return;
      r.ms = performance.now() - t0; r.sql = sql;
      this.results.push(r); this.result = r;
      if (list.length > 1 && this.tab === 'messages' && r.kind === 'rows') this.tab = 'results';
      this.draw();
      if (r.kind === 'error') break;
    }
    this.ctl = null; this.running = false;
    if (this.results.length === 1 && r.kind !== 'rows' && this.tab === 'results' && r.kind !== 'error') this.tab = 'messages';
    if (r.kind === 'rows' && this.tab === 'messages') this.tab = 'results';
    R.helpers.toolbar(); R.helpers.pane('bottom', true);
    this.draw();
    emit('ran', { kind: 'sql', src: text, doc: this }, r, { kind: 'sql', src: text });
    return r;
  }
  stop() { this.ctl?.abort(); this.ctl = null; this.running = false; this.body.replaceChildren(h('div', { class: 'wait' }, 'Stopped waiting.')); R.helpers.toolbar(); }
  /** The panel: Results (the grid), Messages (what it printed and did), Chart, Plan (EXPLAIN). */
  draw() {
    const r = this.result, tab = (id, label, ic) => h('button', { class: 'ptab' + (this.tab === id ? ' on' : ''), role: 'tab', 'aria-selected': String(this.tab === id), onclick: () => { this.tab = id; this.draw(); } }, ic ? icon(ic) : null, label);
    this.tabs.replaceChildren(tab('results', 'Results'), tab('messages', 'Messages'), tab('chart', 'Chart', 'chart'), tab('plan', 'Plan', 'plan'));
    const sum = h('span', { class: 'sum' });
    fill(this.info, sum, r?.kind === 'rows' ? h('span', { class: 'n' }, r.total > r.rows.length ? `${count(r.rows.length)} of ${count(r.total)} rows` : `${count(r.total)} row${r.total === 1 ? '' : 's'}`) : null,
      r ? h('span', { class: 'bar-sep' }, '|') : null, r ? h('span', { class: 'n' }, secs(r.ms)) : null, h('span', { class: 'sep' }),
      h('button', { class: 'icon', title: 'Copy the rows (or the selection), tab-separated, with the headers', 'aria-label': 'Copy', onclick: () => this.gridEl?.grid ? copyText(this.gridEl.grid.selection() ? this.gridEl.grid.copy(true) : toCsv(r, '\t'), 'Copied, with the headers') : null }, icon('copy')),
      h('button', { class: 'icon', title: 'Download the rows as CSV', 'aria-label': 'Download CSV', onclick: () => r?.kind === 'rows' && saveAs(toCsv(r), 'text/csv', this.title.replace(/\.sql$/i, '') + '.csv') }, icon('down')),
      h('button', { class: 'icon', title: this.layout === 'right' ? 'Move the results below' : 'Move the results to the right', 'aria-label': 'Move the results', onclick: () => { this.layout = this.layout === 'right' ? 'below' : 'right'; store.set('pondra.results', this.layout); this.place(); this.draw(); } }, icon(this.layout === 'right' ? 'panelBelow' : 'panelRight')));
    if (!r) { this.body.replaceChildren(h('div', { class: 'wait' }, this.running ? 'Running…' : h('span', {}, 'Run the file, or what is selected: ', h('kbd', {}, 'Ctrl'), ' ', h('kbd', {}, 'Enter')))); return; }
    const said = x => [...x.notices || [], x.kind === 'done' ? doneText(x.value) : x.kind === 'error' ? x.message : `${count(x.total)} row${x.total === 1 ? '' : 's'}`, `(${secs(x.ms)})`].filter(Boolean).join('\n');
    const many = this.results?.length > 1 || this.running && this.todo > 1;
    if (this.tab === 'results') {
      const strip = many ? h('div', { class: 'stmts', role: 'group', 'aria-label': 'The statements\' answers' }, this.results.map((x, k) =>
        h('button', { class: 'stmt' + (x === r ? ' on' : '') + (x.kind === 'error' ? ' bad' : ''), 'aria-pressed': String(x === r), title: x.sql.split('\n').find(l => l.trim() && !l.trim().startsWith('--')) || x.sql,
          onclick: () => { this.result = x; this.draw(); } }, h('b', {}, String(k + 1)), x.kind === 'rows' ? `${count(x.total)} row${x.total === 1 ? '' : 's'}` : x.kind === 'error' ? 'failed' : 'done')),
        this.running ? h('span', { class: 'stmt wait pulse' }, h('b', {}, String(this.results.length + 1)), `of ${this.todo}…`)
          : this.results.length < this.todo ? h('span', { class: 'stmts-left' }, `${this.todo - this.results.length} after it not run`) : null) : null;
      if (r.kind === 'rows') { this.gridEl = grid(r, { fill: true, name: this.title.replace(/\.sql$/i, ''), onsum: t => { sum.textContent = t; }, explore: i => R.helpers.explore(r, i) }); fill(this.body, strip, this.gridEl); }
      else fill(this.body, strip, ...answer(r));
    } else if (this.tab === 'messages') {
      const all = many ? this.results.map((x, k) => `${k + 1}. ${x.sql.split('\n').find(l => l.trim() && !l.trim().startsWith('--')) || x.sql}\n${said(x)}`).join('\n\n') : said(r);
      this.body.replaceChildren(h('pre', { class: this.results?.some(x => x.kind === 'error') ? 'err' : 'said msgs' }, all));
    } else if (this.tab === 'chart') {
      this.body.replaceChildren(r.kind === 'rows' ? chart(r) : h('div', { class: 'wait' }, 'A chart needs rows.'));
    } else {
      const stmt = lastStatement(r.sql);
      if (this.plan?.sql !== stmt) { this.plan = { sql: stmt, text: null }; run('EXPLAIN ' + stmt).then(p => { this.plan.text = p.kind === 'rows' ? p.rows.map(x => x.join('\n')).join('\n\n') : doneText(p.value); if (this.tab === 'plan') this.draw(); }, e => { this.plan.text = e.message; if (this.tab === 'plan') this.draw(); }); }
      this.body.replaceChildren(h('pre', { class: 'said plan' }, this.plan.text ?? 'Reading the plan…'));
    }
  }
  toolbar() {
    const db = MODE === 'lakes' ? h('button', { class: 'btn', title: 'The database the file runs in', onclick: e => R.helpers.pickDb(e.currentTarget) }, icon('db'), S.db || '…', icon('chevd'))
      : h('span', { class: 'pill', title: 'The lake the file runs in' }, icon('db'), S.lake || '');
    return [...this.crumbs(), h('span', { class: 'grow' }),
      btn('play', 'Run', 'Run the file, or what is selected (Ctrl+Enter)', () => this.run(), 'btn primary', 'runBtn'),
      btn(null, 'Run selection', 'Run only what is selected', () => { if (!this.ed.selected()) toast('Select some SQL first'); else this.run(); }),
      h('button', { class: 'btn', title: 'Stop waiting for it', disabled: !this.running, onclick: () => this.stop() }, icon('stop'), 'Stop'), h('span', { class: 'sep' }), db,
      h('button', { class: 'icon', title: 'Format the SQL: its words in capitals, a clause a line', 'aria-label': 'Format', onclick: () => { const t = formatSql(this.ed.value); if (t !== this.ed.value) { this.ed.ta.select(); this.ed.insert(t); } } }, icon('format')),
      btn('save', 'Save', 'Save it (Ctrl+S)', () => this.save()), moreBtn(() => this.more())];
  }
}
/** A script's statements, split as the node splits them (routines.rs `statements`): each ends at
 * a `;` outside strings ('…', $$…$$, $tag$…$tag$), quoted names and comments; the last `;` optional. */
export function statements(text) {
  const out = [];
  let start = 0, i = 0, code = false;
  const upto = (end, from) => { const e = text.indexOf(end, i + from); return e < 0 ? text.length - i : e - i + end.length; };
  while (i < text.length) {
    const c = text[i];
    let skip = 1, isCode = false;
    if (c === '-' && text[i + 1] === '-') skip = upto('\n', 2);
    else if (c === '/' && text[i + 1] === '*') skip = upto('*/', 2);
    else if (c === "'" || c === '"') { skip = upto(c, 1); isCode = true; }
    else if (c === '$') { const tag = /^\$(?:[A-Za-z_]\w*)?\$/.exec(text.slice(i, i + 64))?.[0]; if (tag) skip = upto(tag, tag.length); isCode = true; }
    else if (c === ';') { if (code) out.push(text.slice(start, i)); start = i + 1; code = false; }
    else isCode = !/\s/.test(c);
    code ||= isCode;
    i += skip;
  }
  if (code) out.push(text.slice(start));
  return out.map(x => x.trim());
}
/** The last statement of a script (for its plan). */
export const lastStatement = sql => statements(sql).at(-1) || sql;

// ------------------------------------------------------------------ a Python file: a console below
export class PythonDoc extends TextDoc {
  constructor(o = {}) {
    super({ ...o, kind: 'python', language: 'python', untitled: o.untitled || 'untitled.py' });
    this.layout = 'below';
    this.log = h('div', { class: 'log', 'aria-live': 'polite' });
    this.hist = []; this.at = 0;
    this.input = h('input', { class: 'repl', spellcheck: 'false', 'aria-label': 'Python: a line to run in this page\'s Python', placeholder: 'A line of Python, in the page\'s session (Enter runs it; ↑ ↓: the ones before)' });
    this.input.addEventListener('keydown', e => {
      if (e.key === 'Enter' && this.input.value.trim()) { const code = this.input.value; this.hist.push(code); this.at = this.hist.length; this.input.value = ''; this.exec(code, `>>> ${code}`); }
      else if (e.key === 'ArrowUp' && this.at > 0) { e.preventDefault(); this.input.value = this.hist[--this.at]; }
      else if (e.key === 'ArrowDown') { e.preventDefault(); this.at = Math.min(this.hist.length, this.at + 1); this.input.value = this.hist[this.at] || ''; }
    });
    const tabs = h('div', { class: 'ptabs', role: 'tablist' }, h('button', { class: 'ptab on', role: 'tab', 'aria-selected': 'true' }, icon('terminal'), 'Console'));
    this.panel = h('section', { class: 'panel console', 'aria-label': 'Python console' }, h('div', { class: 'phead' }, tabs, h('span', { class: 'grow' }),
      h('button', { class: 'icon', title: 'Clear the console', 'aria-label': 'Clear the console', onclick: () => this.log.replaceChildren() }, icon('clear'))),
      h('div', { class: 'pbody term' }, this.log, h('div', { class: 'prompt' }, h('span', { class: 'ps1' }, '>>>'), this.input)));
    splitPanel(this, this.panel);
  }
  get hasPanel() { return true; }
  run() { const sel = this.ed.selected(); return this.exec(sel || this.ed.value, sel ? `» the selection of ${this.title}` : `» ${this.title}`); }
  /** Run code in the page's Python (the notebooks' too): what it printed, then its answer. */
  async exec(code, head) {
    if (!code.trim()) return;
    this.ctl?.abort();
    const ctl = this.ctl = new AbortController(), t0 = performance.now(), entry = h('div', { class: 'entry' }, h('div', { class: 'in' }, head), h('div', { class: 'wait pulse' }, 'running…'));
    this.log.append(entry); this.log.parentElement.scrollTop = 1e9;
    while (this.log.childElementCount > 200) this.log.firstElementChild.remove(); // (the last 200 runs kept)
    this.running = true; R.helpers.toolbar(); R.helpers.pane('bottom', true); R.helpers.kernel('busy');
    emit('run', { kind: 'python', src: code, doc: this });
    let r;
    try { r = await run(doBlock(code), ctl.signal); } catch (e) { r = { kind: 'error', message: e.name === 'AbortError' ? 'Stopped waiting.' : e.message, notices: [] }; }
    this.running = false; this.ctl = null; r.ms = performance.now() - t0;
    R.helpers.kernel('idle'); R.helpers.toolbar();
    entry.lastChild.replaceWith(h('div', { class: 'res' }, ...answer(r), h('div', { class: 'took' }, `${r.kind === 'error' ? 'failed' : 'done'} in ${secs(r.ms)}`)));
    this.log.parentElement.scrollTop = 1e9;
    emit('ran', { kind: 'python', src: code, doc: this }, r, { kind: 'python', src: code });
    return r;
  }
  toolbar() {
    return [...this.crumbs(), h('span', { class: 'grow' }),
      btn('play', 'Run file', 'Run the file, or what is selected, in the page\'s Python (Ctrl+Enter)', () => this.run(), 'btn primary', 'runBtn'),
      btn(null, 'Run selection', 'Run only what is selected', () => { if (!this.ed.selected()) toast('Select some Python first'); else this.run(); }),
      h('button', { class: 'btn', title: 'Stop waiting for it', disabled: !this.running, onclick: () => this.ctl?.abort() }, icon('stop'), 'Stop'), h('span', { class: 'sep' }),
      btn('restart', 'Restart', 'Restart the page\'s Python: its variables go', () => R.helpers.restart()),
      btn('save', 'Save', 'Save it (Ctrl+S)', () => this.save()), moreBtn(() => this.more())];
  }
  status() { return [`Ln ${this.pos.line}, Col ${this.pos.col}`, `Python · ${S.py === 'none' ? 'not started' : S.py}`, 'Spaces: 4']; }
}

// ------------------------------------------------------------------ a data file: a table, edited in place
const EDITABLE = 10 << 20; // a CSV or JSON file this big or smaller is edited in the browser; bigger ones open read-only
/** CSV's fields (RFC 4180): quotes, doubled quotes, separators and new lines inside quotes; each
 * record keeps its text, so rows not changed are written back exactly as they were. */
export function parseCsv(text, sep = ',') {
  const recs = [];
  let i = 0, row = [], field = '', quoted = false, start = 0;
  const end = at => { row.push(field); recs.push({ cells: row, raw: text.slice(start, at) }); row = []; field = ''; };
  while (i < text.length) {
    const ch = text[i];
    if (quoted) {
      if (ch === '"') { if (text[i + 1] === '"') { field += '"'; i += 2; continue; } quoted = false; i++; continue; }
      field += ch; i++; continue;
    }
    if (ch === '"' && field === '') { quoted = true; i++; }
    else if (ch === sep) { row.push(field); field = ''; i++; }
    else if (ch === '\r' || ch === '\n') { end(i); i += ch === '\r' && text[i + 1] === '\n' ? 2 : 1; start = i; }
    else { field += ch; i++; }
  }
  if (field !== '' || row.length) end(i);
  return { recs, crlf: /\r\n/.test(text.slice(0, 10000)), last: /\r?\n$/.test(text) };
}
const csvField = (v, sep) => { const s = v == null ? '' : String(v); return s.includes(sep) || /["\r\n]/.test(s) || /^\s|\s$/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };

export class DataDoc {
  constructor({ path }) {
    Object.assign(this, { path, kind: 'data', icon: 'filedata', dirty: false, version: null, marks: new WeakMap(), added: new WeakSet(), cols: [], data: [] });
    this.sep = /\.tsv$/i.test(path) ? '\t' : ',';
    this.format = /\.(csv|tsv)$/i.test(path) ? 'csv' : /\.(jsonl|ndjson)$/i.test(path) ? 'jsonl' : /\.json$/i.test(path) ? 'json' : 'parquet';
    this.box = h('div', { class: 'databox' }, h('div', { class: 'wait pulse' }, 'Reading…'));
    this.foot = h('div', { class: 'datafoot' });
    this.el = h('div', { class: 'doc datadoc' }, this.box, this.foot);
  }
  get title() { return base(this.path); }
  async load() {
    const size = S.files?.find(f => f.path === 'files/' + this.path)?.size;
    let types = [];
    try { types = (await run(`SELECT * FROM ${fileSql(this.path)} LIMIT 0`)).columns || []; } catch { /* (not readable as a table: every column text) */ }
    this.readonly = this.format === 'parquet' || (size != null && size > EDITABLE);
    if (this.readonly) {
      const r = await run(`SELECT * FROM ${fileSql(this.path)} LIMIT 10000`);
      this.cols = r.columns; this.data = r.rows; this.total = r.total;
      this.why = this.format === 'parquet' ? 'Parquet files open read-only: load it into a table to change it with SQL.' : `This file is ${bytes(size)}: it opens read-only (the first 10,000 rows). Load it into a table to change it with SQL.`;
    } else {
      const f = await readFile(this.path);
      this.version = f.version;
      if (this.format === 'csv') {
        const p = parseCsv(f.text, this.sep), [head, ...recs] = p.recs;
        this.crlf = p.crlf; this.last = p.last;
        const names = head?.cells || [];
        this.cols = names.map(n => ({ name: n, type: types.find(t => t.name === n)?.type || 'Utf8' }));
        this.data = recs.filter(r => !(r.cells.length === 1 && r.cells[0] === '')).map(r => { const cells = names.map((_, i) => r.cells[i] ?? ''); cells.raw = r.raw; return cells; });
      } else {
        const objs = this.format === 'jsonl' ? f.text.split('\n').filter(l => l.trim()).map(l => JSON.parse(l)) : JSON.parse(f.text);
        if (!Array.isArray(objs) || objs.some(o => !o || typeof o !== 'object' || Array.isArray(o))) {
          this.readonly = true; this.why = 'This JSON is not a list of objects: it opens read-only.';
          const r = await run(`SELECT * FROM ${fileSql(this.path)} LIMIT 10000`); this.cols = r.columns; this.data = r.rows;
        } else {
          const names = [...new Set(objs.flatMap(o => Object.keys(o)))];
          this.cols = names.map(n => ({ name: n, type: types.find(t => t.name === n)?.type || 'Utf8' }));
          this.data = objs.map(o => names.map(n => o[n] ?? null));
        }
      }
    }
    this.draw();
    return this;
  }
  async reload() { this.marks = new WeakMap(); this.added = new WeakSet(); this.dirty = false; await this.load(); emit('changed', this); }
  mark(row, c) { (this.marks.get(row) || this.marks.set(row, new Set()).get(row)).add(c); if (!this.dirty) { this.dirty = true; emit('changed', this); } }
  /** The grid, and what it may change: cells, rows, columns. */
  draw() {
    const d = this, r = { columns: this.cols, rows: this.data, total: this.total ?? this.data.length };
    const edit = this.readonly ? null : {
      set(i, c, text) { const row = d.data[i]; row[c] = d.format === 'csv' ? text : parseValue(text); d.mark(row, c); },
      changed: (i, c) => !!d.marks.get(d.data[i])?.has(c), added: i => d.added.has(d.data[i]),
      del(is) { const gone = new Set(is.map(i => d.data[i])); d.data = d.data.filter(row => !gone.has(row)); d.gridEl.remove(); d.draw(); d.dirty = true; d.removed = (d.removed || 0) + gone.size; emit('changed', d); },
    };
    this.gridEl = grid(r, { fill: true, edit, name: this.title, onsum: t => { this.sumText = t; this.footer(); }, explore: i => R.helpers.explore(r, i) });
    this.box.replaceChildren(...this.readonly ? [h('div', { class: 'note' }, icon('eye'), this.why)] : [], this.gridEl);
    this.footer();
  }
  addRow() { const row = this.cols.map(() => this.format === 'csv' ? '' : null); this.data.push(row); this.added.add(row); this.dirty = true; emit('changed', this); this.gridEl.remove(); this.draw(); this.gridEl.grid.refresh(this.data.length - 1); }
  async addColumn() {
    const name = await prompt('Add a column', 'Its name', 'column_' + (this.cols.length + 1));
    if (!name) return;
    this.cols.push({ name, type: 'Utf8' });
    for (const row of this.data) { row.push(this.format === 'csv' ? '' : null); this.mark(row, this.cols.length - 1); }
    this.dirty = true; emit('changed', this); this.draw();
  }
  footer() {
    const b = (ic, label, title, fn) => h('button', { class: 'btn ghost small', title, onclick: fn }, icon(ic), label);
    this.foot.replaceChildren(...this.readonly ? [] : [b('plus', 'Add row', 'A row at the end', () => this.addRow()), b('plus', 'Add column', 'A column at the right', () => this.addColumn())],
      h('span', { class: 'sum' }, this.sumText || ''), h('span', { class: 'grow' }),
      h('span', { class: 'hint' }, this.readonly ? 'Read-only' : 'Double-click or type to edit · Enter to keep · Esc to undo · Ctrl S to save to the file'));
  }
  /** The file's text as it will be saved: untouched CSV rows exactly as they were. */
  serialize() {
    if (this.format === 'csv') {
      const nl = this.crlf ? '\r\n' : '\n', line = row => row.raw != null && !this.marks.get(row) && !this.added.has(row) ? row.raw : row.map(v => csvField(v, this.sep)).join(this.sep);
      return [this.cols.map(c => csvField(c.name, this.sep)).join(this.sep), ...this.data.map(line)].join(nl) + (this.last === false ? '' : nl);
    }
    const objs = this.data.map(row => Object.fromEntries(this.cols.map((c, i) => [c.name, row[i]])));
    return this.format === 'jsonl' ? objs.map(o => JSON.stringify(o)).join('\n') + '\n' : JSON.stringify(objs, null, 2) + '\n';
  }
  /** What changed since it was opened (for the details). */
  changes() {
    let cells = 0, rowsAdded = 0;
    for (const row of this.data) { if (this.added.has(row)) rowsAdded++; else cells += this.marks.get(row)?.size || 0; }
    return [cells ? `${count(cells)} cell${cells === 1 ? '' : 's'} changed` : null, rowsAdded ? `${count(rowsAdded)} row${rowsAdded === 1 ? '' : 's'} added` : null, this.removed ? `${count(this.removed)} row${this.removed === 1 ? '' : 's'} deleted` : null].filter(Boolean);
  }
  async save() {
    if (this.readonly) { toast('This file is read-only here: load it into a table to change it', true); return false; }
    const v = await writeFile(this.path, this.serialize(), this.version, this.format === 'csv' ? 'text/csv; charset=utf-8' : 'application/json');
    if (!v) return false;
    this.version = v;
    for (const row of this.data) { if (this.marks.get(row) || this.added.has(row)) row.raw = null; }
    this.marks = new WeakMap(); this.added = new WeakSet(); this.removed = 0; this.dirty = false;
    this.draw();
    toast(`Saved: files/${this.path}`);
    emit('changed', this); emit('saved', this, 'files/' + this.path);
    return true;
  }
  async discard() { if (!this.dirty || confirm('Throw away the changes to ' + this.title + '?')) { this.gridEl?.remove(); await this.reload(); } }
  close() { return !this.dirty || confirm(`Close ${this.title}? It has changes that are not saved.`); }
  activate() { requestAnimationFrame(() => this.gridEl?.grid?.box.focus({ preventScroll: true })); }
  async loadIntoTable() {
    const name = await prompt('Load into a table', 'The new table\'s name', this.title.replace(/\.[^.]+$/, '').replace(/[^\w]+/g, '_').toLowerCase());
    if (!name) return;
    try { await run(`CREATE TABLE ${ident(name)} AS SELECT * FROM ${fileSql(this.path)}`); toast(`Loaded into the table ${name}`); R.helpers.refresh(); } catch (e) { toast(e.message, true); }
  }
  toolbar() {
    const parts = this.path.split('/');
    return [...parts.slice(0, -1).flatMap(p => [h('span', { class: 'crumb' }, p), h('span', { class: 'slash' }, '/')]), h('b', { class: 'crumb cur' }, parts.at(-1)),
      h('span', { class: 'said-saved' }, this.readonly ? 'Read-only' : this.dirty ? `${this.changes().length ? this.changes().join(', ') : 'Edited'}, not saved` : 'Saved'),
      h('span', { class: 'grow' }),
      this.readonly ? null : btn('save', 'Save', 'Save it to the file (Ctrl+S)', () => this.save(), this.dirty ? 'btn primary' : 'btn'),
      this.readonly ? null : h('button', { class: 'btn', disabled: !this.dirty, title: 'Throw away the changes', onclick: () => this.discard() }, 'Discard'), h('span', { class: 'sep' }),
      btn('play', 'Query with SQL', 'Query it, in a new SQL tab', () => R.helpers.query(`SELECT * FROM ${fileSql(this.path)} LIMIT 1000`)),
      moreBtn(() => [{ label: 'Load into a table…', icon: 'up', run: () => this.loadIntoTable() }, { label: 'Download', icon: 'down', run: () => download(this.path) }, { label: 'Copy the path', icon: 'copy', run: () => copyText('files/' + this.path, 'Path copied') }])];
  }
  status() { return [`${count(this.data.length)} rows · ${this.cols.length} columns`, this.format === 'csv' ? `CSV · UTF-8 · ${this.sep === '\t' ? 'tab' : 'comma'}` : this.format.toUpperCase()]; }
}
/** A JSON file's cell, as typed: a number, true, false, null, an object or a list stay what they are. */
const parseValue = t => { if (t === '') return null; if (/^(-?\d+(\.\d+)?([eE][+-]?\d+)?|true|false|null|[[{].*)$/s.test(t.trim())) { try { return JSON.parse(t); } catch { /* (text) */ } } return t; };

// ------------------------------------------------------------------ the core's kinds of file, as an extension would register them
export function registerFiles(register) {
  const text = (Cls, kind) => async path => { if (!path) return new Cls({}); const f = await readFile(path); return new Cls({ path, text: f.text, version: f.version, kind }); };
  register.doc({ id: 'sql', label: 'SQL file', icon: 'filesql', order: 20, match: p => /\.sql$/i.test(p), open: text(SqlDoc) });
  register.doc({ id: 'python', label: 'Python file', icon: 'filepy', order: 30, match: p => /\.py$/i.test(p), open: text(PythonDoc) });
  register.doc({ id: 'data', label: 'Data file', icon: 'filedata', order: 40, match: p => DATA.test(p), open: path => new DataDoc({ path }).load() });
  register.doc({ id: 'text', label: 'Text file', icon: 'file', order: 50, match: p => /\.(md|txt)$/i.test(p), open: async path => { const f = await readFile(path); return new TextDoc({ path, text: f.text, version: f.version, language: /\.md$/i.test(path) ? 'markdown' : 'text' }); } });
}
export { TextDoc, esc, ago, Failure, dir };
