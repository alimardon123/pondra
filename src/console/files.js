// The Workspace and its files (ADR-034): the lake's own files, as a tree; SQL, Python, data and
// text files open in tabs, are edited there and saved back in place (`PUT /files` with
// `If-Match`: replaced only if nobody saved it meanwhile).
import { h, fill, icon, svg, secs, count, bytes, utc, S, R, emit, call, run, rows, doBlock, fileUrl, fileSql, quote, toast, menu, prompt, confirmed, saveAs, MODE, DATA, store, failed, readFile, writeFile, interruptPython, formatPython } from './core.js';
import { Editor, formatSql } from './editor.js';
import { grid, copyText, COPIES } from './grid.js';
import { answer, doneText, cleanName, openPlain } from './notebook.js';

const base = p => p.split('/').pop();
/** An empty folder is a zero-byte object, `<folder>/.folder` (object storage has no folders): the tree and the search leave it out. */
export const FOLDER = '.folder';
/** Where a tab's file is, or will be saved. */
const target = d => d?.path || d?.untitled;
/** A file's kind, by its name: what opens it and which icon it has. */
export const kindOf = p => /\.sql$/i.test(p) ? 'sql' : /\.py$/i.test(p) ? 'python' : DATA.test(p) ? 'data' : /\.(md|txt)$/i.test(p) ? 'text' : /\.ipynb$/i.test(p) ? 'notebook' : 'file';
export const iconOf = p => ({ sql: 'filesql', python: 'filepy', data: 'filedata', notebook: 'notebook', text: 'file', file: 'file' })[kindOf(p)];

// ------------------------------------------------------------------ the Workspace view: the lake's files, as a tree
/** The lake's files by folder. A notebook's versions (`notebooks/<name>/<time>.ipynb`) are one
 * entry, `<name>.ipynb`; the open one shows its outline under it. A folder is there if it holds
 * a file or a `.folder` marker; a file that is only in its tab (new, or its file deleted) is there
 * too, marked not saved. */
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
  const notebooks = new Map(), here = new Set();
  for (const f of list) {
    const rel = f.path.replace(/^files\//, ''), nb = rel.match(/^notebooks\/([^/]+)\/([^/]+)\.ipynb$/), parts = rel.split('/');
    here.add(nb ? 'notebooks/' + nb[1] : rel);
    if (nb) { const n = notebooks.get(nb[1]) || notebooks.set(nb[1], { name: nb[1], written: f.written, n: 0 }).get(nb[1]); n.n++; if (f.written > n.written) n.written = f.written; continue; }
    const d = into(parts.slice(0, -1));
    if (parts.at(-1) !== FOLDER) d.files.push({ ...f, rel, name: parts.at(-1) });
  }
  if (notebooks.size) into(['notebooks']).files.push(...[...notebooks.values()].map(n => ({ rel: `notebooks/${n.name}`, name: n.name + '.ipynb', written: n.written, versions: n.n, notebook: true })));
  for (const d of S.docs) {
    const rel = target(d);
    if (rel && !here.has(rel)) into(rel.split('/').slice(0, -1)).files.push({ rel, name: d.title, notebook: d.kind === 'notebook', doc: d });
  }
  const render = (d, at, depth) => [
    ...[...d.dirs.keys()].sort().map(n => {
      const key = 'dir:' + at + n, front = !!target(S.doc)?.startsWith(at + n + '/') && !S.open.has('closed:' + key); // (the folders of the file in front are open)
      const kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(key) && !front }, render(d.dirs.get(n), at + n + '/', depth + 1));
      return treeItem({ key, kids, depth, icon: 'folder', name: n, title: at + n, onclick: tw => tw.click(), menu: e => folderMenu(e, at + n) });
    }),
    ...d.files.sort((a, b) => a.name.localeCompare(b.name)).map(f => {
      const doc = f.doc || S.docs.find(x => x.path === f.rel), kind = f.notebook ? 'notebook' : kindOf(f.rel), heads = doc?.kind === 'notebook' ? doc.outline() : [];
      const kids = heads.length ? h('div', { class: 'kids outline', role: 'group', hidden: S.doc !== doc && !S.open.has('nb:' + f.rel) }, heads.map(x =>
        treeItem({ depth: depth + 1, icon: 'hash', name: x.text, cls: 'h' + x.level, title: x.text, onclick: () => { R.helpers.activate(doc); doc.goto(x.c); } }))) : null;
      return treeItem({ key: 'nb:' + f.rel, kids, depth, icon: iconOf(f.notebook ? 'x.ipynb' : f.rel), iconCls: 'k-' + kind, name: f.name, dataKey: 'file:' + f.rel, cls: doc && S.doc === doc ? 'front' : '',
        meta: kind === 'data' || kind === 'file' ? bytes(f.size) : f.versions > 1 ? `${f.versions} versions` : '', dirty: doc?.dirty,
        title: f.doc ? `${f.name}: not saved yet` : f.notebook ? `${f.name}: ${f.versions || 0} version${f.versions === 1 ? '' : 's'}` : `files/${f.rel} · ${bytes(f.size)} · written ${f.written ? utc(f.written).toLocaleString() : ''}`,
        onclick: () => f.doc ? R.helpers.activate(f.doc) : kind === 'file' ? R.helpers.pick({ type: 'file', f }) : R.helpers.openFile(f.rel),
        menu: e => fileMenu(e, f, kind) });
    }),
  ];
  const tree = render(root, '', 0);
  box.replaceChildren(...tree.length ? tree : [h('div', { class: 'empty' }, 'No files yet. + makes a file or a folder, or uploads one.')]);
}
/** A row of a tree: its twisty (it folds, when it has kids), icon, name, a note, the unsaved dot, and (if it has a menu) a ⋯ that opens it. */
export function treeItem({ key, kids, depth = 0, icon: ic, iconCls = '', name, meta, dirty, on, title, onclick, ondblclick, menu: onmenu, cls = '', dataKey, dataKind }) {
  const open = kids && !kids.hidden;
  const tw = h('span', { class: 'tw' + (kids ? '' : ' none'), 'aria-hidden': 'true', html: kids ? svg('chev', 14, 2) : '' });
  const row = h('div', { class: `row ${cls}${on ? ' on' : ''}`, role: 'treeitem', tabindex: '-1', 'aria-level': String(depth + 1), 'aria-expanded': kids ? String(!!open) : null, title,
    'data-key': dataKey, 'data-kind': dataKind, style: `padding-left:${6 + depth * 14}px` },
    tw, h('span', { class: 'ic ' + iconCls, html: svg(ic, 16) }), h('span', { class: 'nm' }, name), meta ? h('span', { class: 'meta' }, meta) : null,
    dirty ? h('span', { class: 'dirty', title: 'Not saved', 'aria-label': 'not saved' }) : null,
    onmenu ? h('button', { class: 'icon sm more', title: 'More', 'aria-label': `${name}: more`, onclick: e => { e.stopPropagation(); onmenu(e.currentTarget); } }, icon('dots')) : null);
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
  // (a file only in its tab: not in the lake yet)
  if (f.doc) return menu(e, [{ label: 'Save…', icon: 'save', run: () => { R.helpers.activate(f.doc); f.doc.save(); } }, { label: 'Close', icon: 'close', run: () => R.helpers.close(f.doc) }]);
  menu(e, [kind !== 'file' ? { label: 'Open', icon: iconOf(f.notebook ? 'x.ipynb' : f.rel), run: () => R.helpers.openFile(f.rel) } : null,
    { label: 'Details', icon: 'eye', run: () => R.helpers.pick({ type: 'file', f }) },
    kind === 'data' ? { label: 'Query with SQL', icon: 'play', run: () => R.helpers.query(`SELECT * FROM ${fileSql(f.rel)} LIMIT 1000`) } : null, '-',
    !f.notebook ? { label: 'Rename…', icon: 'pencil', run: () => rename(f.rel) } : null,
    !f.notebook ? { label: 'Download', icon: 'down', run: () => download(f.rel) } : null,
    { label: 'Copy the path', icon: 'copy', run: () => copyText('files/' + f.rel, 'Path copied') }, '-',
    { label: f.notebook ? 'Delete every version…' : 'Delete…', icon: 'trash', run: () => remove(f) }]);
}
function folderMenu(e, at) {
  const make = R.helpers;
  menu(e, [{ label: 'New notebook here', icon: 'notebook', run: () => make.newNotebook(at) }, { label: 'New SQL file here', icon: 'filesql', run: () => make.newFile('sql', at + '/') },
    { label: 'New Python file here', icon: 'filepy', run: () => make.newFile('python', at + '/') }, { label: 'New folder here', icon: 'folder', run: () => newFolder(at + '/') }, '-',
    { label: 'Upload a file here…', icon: 'up', run: () => upload(at + '/') }, { label: 'Delete folder…', icon: 'trash', run: () => remove({ rel: at, name: at }, true) }]);
}
/** A folder in `at`, by name (`a/b` makes both): its marker is put as any file is. */
export async function newFolder(at = '') {
  const name = ((await prompt('New folder', 'Its name', '')) || '').replace(/^\/+|\/+$/g, ''), rel = at + name;
  if (!name) return;
  const bad = /(^|\/)(\.{0,2}|\s+)(\/|$)|^notebooks(\/|$)/.test(rel) ? 'Not a folder name: no empty part, . or .., nor in notebooks/'
    : S.files?.some(f => f.path === 'files/' + rel || f.path.startsWith(`files/${rel}/`)) ? `${rel} is there already` : '';
  if (bad) return toast(bad, true);
  try {
    await call(fileUrl(`${rel}/${FOLDER}`), { method: 'PUT', body: '' });
    S.open.add('dir:' + at.slice(0, -1)); // (so it shows)
    toast(`Made the folder ${rel}`);
  } catch (err) { toast('No folder made: ' + err.message, true); }
  R.helpers.refreshFiles();
}
export async function download(rel) {
  try { const r = await call(fileUrl(rel)); saveAs(await r.blob(), 'application/octet-stream', base(rel)); } catch (e) { toast(e.message, true); }
}
/** A file from this computer into the lake's files (a notebook opens, unless it is for a folder: then it is put there; the rest are put). */
export function upload(at = '') {
  const input = h('input', { type: 'file', hidden: true, multiple: true });
  input.onchange = async () => {
    const fs = [...input.files];
    input.remove();
    for (const f of fs) {
      if (/\.ipynb$/i.test(f.name) && (!at || at === 'notebooks/')) { try { R.helpers.openNotebook(JSON.parse(await f.text()), cleanName(f.name) || 'uploaded'); toast(`Opened ${f.name}: Ctrl+S keeps it in the lake`); } catch (err) { toast(`Could not open ${f.name}: ${err.message}`, true); } continue; }
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
    if (doc) { await R.helpers.close(doc); R.helpers.openFile(to); } // (its tab again, at the new path)
    toast(`Renamed to ${to}`);
  } catch (e) { toast('Not renamed: ' + e.message, true); }
  R.helpers.refreshFiles();
}
/** Delete a file, a notebook (every version) or a folder (every file in it, its marker too), once asked. */
async function remove(f, folder) {
  try {
    const paths = f.notebook || folder ? (await rows(`SELECT path FROM files(${quote(f.rel + '/')})`)).map(x => x.path) : ['files/' + f.rel], n = paths.filter(p => !p.endsWith('/' + FOLDER)).length;
    if (!confirmed(f.notebook ? `Delete the notebook ${f.name}, every version of it?` : folder ? `Delete the folder ${f.rel} and its ${n} file${n === 1 ? '' : 's'}? This can't be undone.` : `Delete files/${f.rel}? This can't be undone.`)) return;
    await Promise.all(paths.map(p => call(fileUrl(p), { method: 'DELETE' })));
    for (const d of S.docs) if (d.path === f.rel || folder && d.path?.startsWith(f.rel + '/')) Object.assign(d, { dirty: true, version: null, written: null }); // (a tab keeps what it holds, unsaved)
    R.helpers.drawTabs();
    toast(`Deleted ${f.name}`);
  } catch (e) { toast('Not deleted: ' + e.message, true); }
  R.helpers.refreshFiles();
}

// ------------------------------------------------------------------ text files: SQL, Python, Markdown and text
class TextDoc {
  constructor({ path = null, text = '', version = null, kind = 'text', language = 'text', untitled = 'untitled.txt' }) {
    Object.assign(this, { path, version, kind, dirty: false, untitled, pos: { line: 1, col: 1 } }); // (new and untouched: nothing to save yet)
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
  get blank() { return !this.path && !this.ed.value.trim(); } // (new and empty: nothing to lose)
  close() { return !this.dirty || this.blank || confirm(`Close ${this.title}? It has changes that are not saved.`); }
  async reload() { if (!this.path) return; const f = await readFile(this.path); this.ed.value = f.text; this.version = f.version; this.dirty = false; this.paramsBar?.(); emit('changed', this); }
  /** Save it where it is (a new file asks where first). */
  async save(as) {
    let path = this.path;
    if (!path || as) {
      path = await prompt(as ? 'Save as' : 'Save', 'Where, under the lake\'s files', path || this.untitled);
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
    return [...parts.slice(0, -1).flatMap(p => [h('span', { class: 'crumb' }, p), h('span', { class: 'slash' }, '/')]), h('b', { class: 'crumb cur' }, parts.at(-1))];
  }
  more() {
    return [{ label: 'Save as…', icon: 'save', run: () => this.save(true) }, this.path ? { label: 'Download', icon: 'down', run: () => saveAs(this.ed.value, 'text/plain', this.title) } : null];
  }
  toolbar() { return [...this.crumbs(), h('span', { class: 'grow' }), R.helpers.saveButton(this), moreBtn(() => this.more())]; }
  status() { return [`Ln ${this.pos.line}, Col ${this.pos.col}`, { sql: 'SQL', python: 'Python', text: /\.md$/i.test(this.title) ? 'Markdown' : 'Text' }[this.kind] || '', 'Spaces: 4']; }
}
export const btn = (ic, label, title, fn, cls = 'btn', id) => h('button', { class: cls, title, onclick: fn, id }, ic ? icon(ic) : null, label);
export const moreBtn = items => h('button', { class: 'icon', title: 'More', 'aria-label': 'More', onclick: e => menu(e.currentTarget, items()) }, icon('dots'));

/** A panel under a file (SQL's results, Python's console), its height dragged; or at the right. */
function splitPanel(doc, panel) {
  const grip = h('div', { class: 'grip', role: 'separator', 'aria-label': 'The panel\'s size (arrows change it)', tabindex: '0', 'aria-valuemin': '120' });
  const size = { below: 320, right: 520, ...R.helpers.prefs('split') }; // (kept with the settings: on this machine)
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
    grip.addEventListener('pointerup', () => { grip.removeEventListener('pointermove', move); R.helpers.prefs('split', { ...size }); }, { once: true });
  });
  grip.addEventListener('keydown', e => { const d = { ArrowUp: 24, ArrowDown: -24, ArrowLeft: 24, ArrowRight: -24 }[e.key]; if (d) { e.preventDefault(); const k = doc.layout === 'right' ? 'right' : 'below'; size[k] = Math.max(120, size[k] + d); place(); R.helpers.prefs('split', { ...size }); } });
  doc.el.append(grip, panel);
  doc.place = place;
  place();
}

// ------------------------------------------------------------------ a SQL file: its results below
export class SqlDoc extends TextDoc {
  constructor(o = {}) {
    super({ ...o, kind: 'sql', language: 'sql', untitled: o.untitled || 'queries/untitled.sql' });
    this.layout = R.helpers.prefs('results') || 'below';
    this.tab = 'results'; this.result = null;
    this.body = h('div', { class: 'pbody' });
    this.tabs = h('div', { class: 'ptabs', role: 'tablist' });
    this.info = h('div', { class: 'pinfo' });
    this.panel = h('section', { class: 'panel results', 'aria-label': 'Results' }, h('div', { class: 'phead' }, this.tabs, h('span', { class: 'grow' }), this.info), this.body);
    this.pbar = h('div', { class: 'params', role: 'group', 'aria-label': 'Parameters', hidden: true });
    this.main.prepend(this.pbar);
    splitPanel(this, this.panel);
    this.ed.menu = some => ['-', { label: some ? 'Run the selection' : 'Run the file', icon: 'play', keys: 'Ctrl Enter', run: () => this.run() },
      { label: some ? 'Format the selection' : 'Format the file', icon: 'format', keys: 'Shift Alt F', run: () => this.format() }];
    this.draw(); this.paramsBar();
  }
  /** Format the SQL selected (or all of it): its words in capitals, a clause a line. */
  format() { this.ed.reformat(formatSql); }
  key(e) {
    if (e.shiftKey && e.altKey && e.key.toLowerCase() === 'f') { e.preventDefault(); this.format(); return true; }
    return super.key(e);
  }
  get hasPanel() { return true; }
  changed() { super.changed(); clearTimeout(this.pt); this.pt = setTimeout(() => this.paramsBar(), 250); }
  /** The file's `$name`s, an input each above the editor: their values go with every run, bound
   * on the node (ADR-033), and are kept in this browser for the file. */
  paramsBar() {
    const names = parameters(this.ed.value), key = 'pondra.params:' + (this.path || this.untitled);
    this.kept ??= store.json(key, {});
    this.pbar.hidden = !names.length;
    if (names.join() === this.shownParams) return;
    this.shownParams = names.join();
    fill(this.pbar, h('span', { class: 'plabel' }, 'Parameters'), names.map(n => h('label', { class: 'param' }, h('span', {}, '$' + n),
      h('input', { value: this.kept[n] ?? '', spellcheck: 'false', placeholder: 'a value', 'aria-label': `The value of $${n}`,
        oninput: e => { this.kept[n] = e.target.value; store.set(key, JSON.stringify(this.kept)); }, onkeydown: e => { if (e.key === 'Enter') this.run(); } }))));
  }
  /** The parameters' values as the node takes them: numbers and true/false as such, the rest as text. */
  params() {
    return Object.fromEntries(parameters(this.ed.value).filter(n => (this.kept?.[n] ?? '') !== '').map(n => {
      const v = this.kept[n].trim();
      return [n, /^-?\d+(\.\d+)?$/.test(v) && Math.abs(+v) < 2 ** 53 ? +v : v === 'true' || v === 'false' ? v === 'true' : v];
    }));
  }
  /** Run the file, or what is selected: each statement its own answer (in order, stopping at a
   * failure), or — as Settings may say — all of it at once, the last one's answer. */
  async run() {
    const sel = this.ed.selected(), from = sel ? this.ed.ta.selectionStart : 0, text = (sel || this.ed.value).trim();
    if (!text) return;
    this.ctl?.abort();
    const ctl = this.ctl = new AbortController(), each = (R.helpers.prefs('statements') || 'each') === 'each';
    const list = each ? statements(text) : [text];
    const whole = sel || this.ed.value;
    let seek = 0;
    const places = list.map(q => { const at = whole.indexOf(q, seek); seek = at < 0 ? seek : at + q.length; return at < 0 ? null : [from + at, from + at + q.length]; }); // (where each is, to point at it)
    this.running = true; this.plan = null; this.results = []; this.result = null; this.todo = list.length; R.helpers.toolbar();
    this.body.replaceChildren(h('div', { class: 'wait pulse' }, 'Running…'));
    emit('run', { kind: 'sql', src: text, doc: this });
    let r;
    const values = this.params();
    for (const sql of list.length ? list : [text]) {
      const t0 = performance.now();
      try { r = await run(sql, ctl.signal, values); } catch (e) { r = failed(e); }
      if (this.ctl !== ctl) return;
      r.ms = performance.now() - t0; r.sql = sql; r.at = places[this.results.length]; r.params = values;
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
  stop() { this.ctl?.abort(); this.ctl = null; this.running = false; this.body.replaceChildren(h('div', { class: 'wait' }, 'Stopped waiting. (A statement already on its way may still finish on the node.)')); R.helpers.toolbar(); }
  /** Point at a statement that ran: the caret at its start, in sight, if the text there is still
   * it (not selected: Run would then run it alone). */
  point(x) { const [a, b] = x.at || []; if (a != null && this.ed.value.slice(a, b) === x.sql) { this.ed.focus(); this.ed.ta.setSelectionRange(a, a); this.ed.reveal(); } }
  /** The panel: Results (the grid), Messages (each statement: what it printed and did), Chart and
   * Plan (a graph of EXPLAIN, and a profile of the time each step took). */
  draw() {
    const r = this.result, tab = (id, label, ic) => h('button', { class: 'ptab' + (this.tab === id ? ' on' : ''), role: 'tab', 'aria-label': label, 'aria-selected': String(this.tab === id), onclick: () => { this.tab = id; this.draw(); } }, ic ? icon(ic) : null, h('span', { class: 'tl' }, label));
    this.tabs.replaceChildren(tab('results', 'Results'), tab('messages', 'Messages'), tab('chart', 'Chart', 'chart'), tab('plan', 'Plan', 'plan'));
    const sum = h('span', { class: 'sum' }), name = this.title.replace(/\.sql$/i, ''), rowsOk = r?.kind === 'rows';
    const text = (f, headers) => this.gridEl?.grid ? this.gridEl.grid.text(f, headers) : '';
    fill(this.info, sum, rowsOk ? h('span', { class: 'n' }, r.total > r.rows.length ? `${count(r.rows.length)} of ${count(r.total)} rows` : `${count(r.total)} row${r.total === 1 ? '' : 's'}`) : null,
      r ? h('span', { class: 'bar-sep' }, '|') : null, r ? h('span', { class: 'n t' }, secs(r.ms)) : null, h('span', { class: 'sep' }),
      split('copy', 'Copy the rows (or the selection), tab-separated, with the headers', () => rowsOk && copyText(text('tsv', true), 'Copied, with the headers'),
        () => [{ head: 'Copy the rows (or the selection)' }, ...COPIES.map(([f, label, headers]) => ({ label, run: () => rowsOk && copyText(text(f, headers), 'Copied') }))], !rowsOk),
      split('down', 'Download the rows as CSV (all of them: the statement runs again on the node)', () => rowsOk && fetchRows(r, 'csv', name),
        () => [{ head: 'Download every row (it runs again)' }, ...DOWNLOADS.map(([f, label]) => ({ label, run: () => fetchRows(r, f, name) }))], !rowsOk),
      h('button', { class: 'icon', title: this.layout === 'right' ? 'Move the results below' : 'Move the results to the right', 'aria-label': 'Move the results', onclick: () => { this.layout = this.layout === 'right' ? 'below' : 'right'; R.helpers.prefs('results', this.layout); this.place(); this.draw(); } }, icon(this.layout === 'right' ? 'panelBelow' : 'panelRight')));
    if (!r) { this.body.replaceChildren(h('div', { class: 'wait' }, this.running ? 'Running…' : h('span', {}, 'Run the file, or what is selected: ', h('kbd', {}, 'Ctrl'), ' ', h('kbd', {}, 'Enter')))); return; }
    const many = this.results?.length > 1 || this.running && this.todo > 1;
    if (this.tab === 'results') {
      const strip = many ? h('div', { class: 'stmts', role: 'group', 'aria-label': 'The statements\' answers' }, this.results.map((x, k) =>
        h('button', { class: 'stmt' + (x === r ? ' on' : '') + (x.kind === 'error' ? ' bad' : ''), 'aria-pressed': String(x === r), title: x.sql,
          onclick: () => { this.result = x; this.draw(); this.point(x); } }, h('b', {}, String(k + 1)), h('span', { class: 'q' }, oneLine(x.sql, 40)), h('span', { class: 'm' }, x.kind === 'rows' ? `${count(x.total)} row${x.total === 1 ? '' : 's'}` : x.kind === 'error' ? 'failed' : 'done'))),
        this.running ? h('span', { class: 'stmt wait pulse' }, h('b', {}, String(this.results.length + 1)), `of ${this.todo}…`)
          : this.results.length < this.todo ? h('span', { class: 'stmts-left' }, `${this.todo - this.results.length} after it not run`) : null) : null;
      if (r.kind === 'rows') { this.gridEl = grid(r, { fill: true, name, onsum: t => { sum.textContent = t; }, explore: i => R.helpers.explore(r, i) }); fill(this.body, strip, this.gridEl); }
      else fill(this.body, strip, ...answer(r));
    } else if (this.tab === 'messages') {
      const all = this.results?.length ? this.results : [r];
      this.body.replaceChildren(h('div', { class: 'msgs' }, all.map((x, k) => h('div', { class: 'msg' + (x.kind === 'error' ? ' bad' : '') + (x === r ? ' on' : '') },
        h('button', { class: 'msg-h', title: x.sql + '\n\n(click: its answer, and the statement selected in the file)', onclick: () => { this.result = x; this.tab = x.kind === 'rows' ? 'results' : 'messages'; this.draw(); this.point(x); } },
          h('b', {}, String(k + 1)), h('span', { class: 'ic', html: svg(x.kind === 'error' ? 'close' : 'check', 13) }), h('code', {}, oneLine(x.sql, 160)),
          h('span', { class: 'm' }, x.kind === 'rows' ? `${count(x.total)} row${x.total === 1 ? '' : 's'}` : x.kind === 'error' ? 'failed' : 'done', ' · ', secs(x.ms))),
        [...x.notices || [], x.kind === 'done' ? doneText(x.value) : x.kind === 'error' ? x.message : null].filter(Boolean).length
          ? h('pre', { class: x.kind === 'error' ? 'err' : 'said' }, [...x.notices || [], x.kind === 'done' ? doneText(x.value) : x.kind === 'error' ? x.message : null].filter(Boolean).join('\n')) : null)),
        this.results.length < this.todo && !this.running ? h('div', { class: 'stmts-left' }, `${this.todo - this.results.length} after it not run`) : null));
    } else if (this.tab === 'chart') {
      if (r.kind !== 'rows') { this.body.replaceChildren(h('div', { class: 'wait' }, 'A chart needs rows.')); return; }
      this.body.replaceChildren(h('div', { class: 'wait' }, 'Drawing…'));
      import('./chart.js').then(m => { if (this.tab === 'chart' && this.result === r) this.body.replaceChildren(m.chartView(r, name)); });
    } else {
      const stmt = lastStatement(r.sql);
      this.body.replaceChildren(h('div', { class: 'wait' }, 'Reading the plan…'));
      import('./plan.js').then(m => { if (this.tab === 'plan' && this.result === r) this.body.replaceChildren(m.planView(stmt, r.params)); });
    }
  }
  toolbar() {
    const db = MODE === 'lakes' ? h('button', { class: 'btn', title: 'The database the file runs in', onclick: e => R.helpers.pickDb(e.currentTarget) }, icon('db'), S.db || '…', icon('chevd'))
      : h('span', { class: 'pill', title: 'The lake the file runs in' }, icon('db'), S.lake || '');
    const some = () => !!this.ed.selected();
    return [...this.crumbs(), h('span', { class: 'grow' }),
      R.helpers.runButton(this.running, { label: 'Run', title: 'Run the file, or what is selected (Ctrl+Enter)', run: () => this.run(), stop: () => this.stop(), stopTitle: 'Stop waiting for it' }, [
        { label: 'Run the file, or what is selected', icon: 'play', keys: 'Ctrl Enter', run: () => this.run() },
        { label: 'Run the selection', run: () => { if (!some()) toast('Select some SQL first'); else this.run(); } },
        { label: 'Run the whole file', run: () => { this.ed.ta.setSelectionRange(0, 0); this.run(); } }, '-',
        { label: 'Format the file (or the selection)', icon: 'format', keys: 'Shift Alt F', run: () => this.format() }, '-',
        { label: 'Run as a job', icon: 'play', run: () => R.helpers.job(this) }, { label: 'Schedule…', icon: 'clock', run: () => R.helpers.schedule(this) }]),
      h('span', { class: 'sep' }), db, R.helpers.saveButton(this), moreBtn(() => this.more())];
  }
}
/** A statement on one line: its comments out, its spaces one, at most `n` characters. */
export const oneLine = (sql, n) => { const t = sql.replace(/--[^\n]*|\/\*[\s\S]*?\*\//g, ' ').replace(/\s+/g, ' ').trim(); return t.length > n ? t.slice(0, n - 1) + '…' : t; };
/** A copy's or a download's button, with a ▾ for its other forms. */
const split = (ic, title, run, items, disabled) => h('span', { class: 'split' }, h('button', { class: 'icon', title, 'aria-label': title, disabled, onclick: run }, icon(ic)),
  h('button', { class: 'icon caret', title: 'Other forms', 'aria-label': 'Other forms', 'aria-haspopup': 'menu', disabled, onclick: e => menu(e.currentTarget, items()) }, icon('chevd', 'ic', 12)));
const DOWNLOADS = [['csv', 'CSV'], ['tsv', 'TSV (tab-separated)'], ['json', 'JSON'], ['ndjson', 'JSON lines'], ['parquet', 'Parquet'], ['xlsx', 'Excel (.xlsx)']];
/** Every row of an answer, in a file to download: its statement again on the node, in that format. */
async function fetchRows(r, f, name) {
  toast('Preparing the download…');
  try {
    const body = r.params && Object.keys(r.params).length ? JSON.stringify({ sql: r.sql, params: r.params }) : r.sql;
    const res = await call('/sql?format=' + f, { method: 'POST', body, headers: { 'content-type': r.params && Object.keys(r.params).length ? 'application/json' : 'text/plain; charset=utf-8' } });
    saveAs(await res.blob(), res.headers.get('content-type') || 'application/octet-stream', `${name || 'rows'}.${f === 'ndjson' ? 'jsonl' : f}`);
  } catch (e) { toast('Not downloaded: ' + e.message, true); }
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
/** A script's `$name` parameters, in order, once each (not `$1`, nor what strings, comments and
 * `$$` bodies hold). */
export const parameters = sql => [...new Set([...sql.replace(/--[^\n]*|\/\*[\s\S]*?\*\/|'(?:[^']|'')*'|"(?:[^"]|"")*"|\$(\w*)\$[\s\S]*?\$\1\$/g, ' ')
  .matchAll(/\$([A-Za-z_]\w*)/g)].map(m => m[1]))];
/** The last statement of a script (for its plan). */
export const lastStatement = sql => statements(sql).at(-1) || sql;

// ------------------------------------------------------------------ a Python file: a console below
export class PythonDoc extends TextDoc {
  constructor(o = {}) {
    super({ ...o, kind: 'python', language: 'python', untitled: o.untitled || 'scripts/untitled.py' });
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
    this.ed.menu = some => ['-', { label: some ? 'Run the selection' : 'Run the file', icon: 'play', keys: 'Ctrl Enter', run: () => this.run() },
      { label: some ? 'Format the selection' : 'Format the file', icon: 'format', keys: 'Shift Alt F', run: () => this.format() }];
  }
  get hasPanel() { return true; }
  run() { const sel = this.ed.selected(); return this.exec(sel || this.ed.value, sel ? `» the selection of ${this.title}` : `» ${this.title}`); }
  /** Format the Python selected (or all of it): the node's Python formats it, as ruff (or black) does. */
  format() { this.ed.reformat(formatPython); }
  key(e) {
    if (e.shiftKey && e.altKey && e.key.toLowerCase() === 'f') { e.preventDefault(); this.format(); return true; }
    return super.key(e);
  }
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
    try { r = await run(doBlock(code), ctl.signal); } catch (e) { r = failed(e, 'Stopped waiting.'); }
    this.running = false; this.ctl = null; r.ms = performance.now() - t0;
    R.helpers.kernel('idle'); R.helpers.toolbar();
    entry.lastChild.replaceWith(h('div', { class: 'res' }, ...answer(r), h('div', { class: 'took' }, `${r.kind === 'error' ? 'failed' : 'done'} in ${secs(r.ms)}`)));
    this.log.parentElement.scrollTop = 1e9;
    emit('ran', { kind: 'python', src: code, doc: this }, r, { kind: 'python', src: code });
    return r;
  }
  toolbar() {
    const pill = R.helpers.pythonPill();
    this.drawPill = pill.draw;
    return [...this.crumbs(), h('span', { class: 'grow' }),
      R.helpers.runButton(this.running, { label: 'Run file', title: 'Run the file, or what is selected, in the page\'s Python (Ctrl+Enter)', run: () => this.run(), stop: () => interruptPython(), stopTitle: 'Interrupt it (its variables stay)' }, [
        { label: 'Run the file, or what is selected', icon: 'play', keys: 'Ctrl Enter', run: () => this.run() },
        { label: 'Run the selection', run: () => { if (!this.ed.selected()) toast('Select some Python first'); else this.run(); } }, '-',
        { label: 'Run as a job', icon: 'play', run: () => R.helpers.job(this) }, { label: 'Schedule…', icon: 'clock', run: () => R.helpers.schedule(this) }]),
      h('span', { class: 'sep' }), pill, R.helpers.saveButton(this), moreBtn(() => this.more())];
  }
  status() { return [`Ln ${this.pos.line}, Col ${this.pos.col}`, `Python · ${S.py === 'none' ? 'not started' : S.py}`, 'Spaces: 4']; }
}

// ------------------------------------------------------------------ the core's kinds of file, as an extension would register them
export function registerFiles(register) {
  const text = (Cls, kind) => async path => { if (!path) return new Cls({}); const f = await readFile(path); return new Cls({ path, text: f.text, version: f.version, kind }); };
  register.doc({ id: 'notebook', label: 'Notebook', icon: 'notebook', match: p => /\.ipynb$/i.test(p) && !p.startsWith('notebooks/'), open: openPlain }); // (a plain file, saved in place; notebooks keeps versions)
  register.doc({ id: 'sql', label: 'SQL file', icon: 'filesql', order: 20, match: p => /\.sql$/i.test(p), open: text(SqlDoc) });
  register.doc({ id: 'python', label: 'Python file', icon: 'filepy', order: 30, match: p => /\.py$/i.test(p), open: text(PythonDoc) });
  register.doc({ id: 'data', label: 'Data file', icon: 'filedata', order: 40, match: p => DATA.test(p), open: async path => { const { DataDoc } = await import('./data.js'); return new DataDoc({ path }).load(); } }); // (data.js: loaded when a data file first opens)
  register.doc({ id: 'text', label: 'Text file', icon: 'file', order: 50, match: p => /\.(md|txt)$/i.test(p), open: async path => { const f = await readFile(path); return new TextDoc({ path, text: f.text, version: f.version, language: /\.md$/i.test(path) ? 'markdown' : 'text' }); } });
}
