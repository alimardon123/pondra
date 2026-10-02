// The Workspace and its files (ADR-034): the lake's own files, as a tree; SQL, Python, data and
// text files open in tabs, are edited there and saved back in place (`PUT /files` with
// `If-Match`: replaced only if nobody saved it meanwhile).
import { h, icon, svg, count, bytes, utc, S, R, emit, call, run, rows, fileUrl, toast, menu, prompt, saveAs, DATA, failed, readFile, writeFile, renamed, crumbs, renaming } from './core.js';
import { Editor } from './editor.js';
import { copyText } from './grid.js';
import { openPlain } from './notebook.js';

const base = p => p.split('/').pop();
/** An empty folder is a zero-byte object, `<folder>/.folder` (object storage has no folders): the tree and the search leave it out. */
export const FOLDER = '.folder';
/** Where a tab's file is, or will be saved. */
const target = d => d?.path || d?.untitled;
/** A file's kind, by its name: what opens it and which icon it has. */
/** Files that open as text in the editor: Markdown, plain text, settings and code of other kinds, and a name with no kind (README, Makefile). */
export const TEXT = /\.(md|txt|ya?ml|toml|ini|cfg|conf|env|xml|html?|css|m?js|ts|sh|bat|ps1|r|log)$|(^|\/)[^./]+$/i;
export const kindOf = p => /\.sql$/i.test(p) ? 'sql' : /\.py$/i.test(p) ? 'python' : DATA.test(p) ? 'data' : TEXT.test(p) ? 'text' : /\.ipynb$/i.test(p) ? 'notebook' : 'file';
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
  dropping(box);
  drawWorkspace(box);
}
/** Dragging in the Workspace: a file or a folder onto a folder (or a file in it, or below the tree:
 * the top) is moved there (rename.js); files and folders from this computer are put there (upload.js). */
function dropping(box) {
  let lit;
  const into = e => { let r = e.target.closest?.('.row'); while (r && r.dataset.dir == null) r = r.parentElement.closest('.kids')?.previousElementSibling; return r; };
  const light = el => { if (lit !== el) { lit?.classList.remove('dropping'); (lit = el)?.classList.add('dropping'); } };
  const ours = e => ['Files', 'text/x-pondra'].some(t => e.dataTransfer.types.includes(t)); // (not a view dragged to the other pane)
  box.ondragover = e => { if (ours(e)) { e.preventDefault(); light(into(e)?.parentElement || box); } };
  box.ondragleave = e => { if (!box.contains(e.relatedTarget)) light(null); };
  box.ondrop = e => {
    if (!ours(e)) return;
    e.preventDefault(); light(null);
    const to = into(e)?.dataset.dir || '', from = e.dataTransfer.getData('text/x-pondra'), dropped = from ? null : [...e.dataTransfer.items].map(i => i.webkitGetAsEntry?.() || i.getAsFile()).filter(Boolean); // (read now: the drop's items go with its event)
    from ? import('./rename.js').then(m => m.move(from, to)) : dropped.length && import('./upload.js').then(m => m.dropped(dropped, to));
  };
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
  for (const n of notebooks.keys()) if (here.has(`notebooks/${n}.ipynb`)) notebooks.delete(n); // (saved again since: one file, these its versions)
  if (notebooks.size) into(['notebooks']).files.push(...[...notebooks.values()].map(n => ({ rel: `notebooks/${n.name}`, name: n.name + '.ipynb', written: n.written, versions: n.n, notebook: true })));
  for (const d of S.docs) {
    const rel = target(d);
    if (rel && !here.has(rel)) into(rel.split('/').slice(0, -1)).files.push({ rel, name: d.title, notebook: d.kind === 'notebook', doc: d });
  }
  const render = (d, at, depth) => [
    ...[...d.dirs.keys()].sort().map(n => {
      const key = 'dir:' + at + n, front = !!target(S.doc)?.startsWith(at + n + '/') && !S.open.has('closed:' + key); // (the folders of the file in front are open)
      const kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(key) && !front }, render(d.dirs.get(n), at + n + '/', depth + 1));
      return treeItem({ key, kids, depth, icon: 'folder', name: n, title: at + n, dir: at + n + '/', drag: at + n === 'notebooks' ? null : at + n + '/', onclick: tw => tw.click(), menu: e => menus(m => m.folderMenu(e, at + n)) });
    }),
    ...d.files.sort((a, b) => a.name.localeCompare(b.name)).map(f => {
      const doc = f.doc || S.docs.find(x => x.path === f.rel), kind = f.notebook ? 'notebook' : kindOf(f.rel), heads = doc?.kind === 'notebook' ? doc.outline() : [];
      const kids = heads.length ? h('div', { class: 'kids outline', role: 'group', hidden: S.doc !== doc && !S.open.has('nb:' + f.rel) }, heads.map(x =>
        treeItem({ depth: depth + 1, icon: 'hash', name: x.text, cls: 'h' + x.level, title: x.text, onclick: () => { R.helpers.activate(doc); doc.goto(x.c); } }))) : null;
      return treeItem({ key: 'nb:' + f.rel, kids, depth, icon: iconOf(f.notebook ? 'x.ipynb' : f.rel), iconCls: 'k-' + kind, name: f.name, dataKey: 'file:' + f.rel, cls: doc && S.doc === doc ? 'front' : '',
        meta: kind === 'data' || kind === 'file' ? bytes(f.size) : f.versions > 1 ? `${f.versions} versions` : '', dirty: doc?.dirty, drag: f.doc || f.notebook ? null : f.rel,
        title: f.doc ? `${f.name}: not saved yet` : f.notebook ? `${f.name}: ${f.versions || 0} version${f.versions === 1 ? '' : 's'}` : `files/${f.rel} · ${bytes(f.size)} · written ${f.written ? utc(f.written).toLocaleString() : ''}`,
        onclick: () => f.doc ? R.helpers.activate(f.doc) : kind === 'file' ? R.helpers.pick({ type: 'file', f }) : R.helpers.openFile(f.rel),
        menu: e => menus(m => m.fileMenu(e, f, kind)) });
    }),
  ];
  const tree = render(root, '', 0);
  box.replaceChildren(...tree.length ? tree : [h('div', { class: 'empty' }, 'No files yet. + makes a file or a folder, or uploads one.')]);
}
/** A row of a tree: its twisty (it folds, when it has kids), icon, name, a note, the unsaved dot, and (if it has a menu) a ⋯ that opens it. */
export function treeItem({ key, kids, depth = 0, icon: ic, iconCls = '', name, meta, dirty, on, title, onclick, ondblclick, menu: onmenu, onopen, cls = '', dataKey, dataKind, dir, drag }) {
  const open = kids && !kids.hidden;
  kids?.style.setProperty('--g', 13 + depth * 14 + 'px'); // (its guide: a line down from its arrow, along what it holds)
  const tw = h('span', { class: 'tw' + (kids ? '' : ' none'), 'aria-hidden': 'true', html: kids ? svg('chev', 14, 2) : '' });
  const row = h('div', { class: `row ${cls}${on ? ' on' : ''}`, role: 'treeitem', tabindex: '-1', 'aria-level': String(depth + 1), 'aria-expanded': kids ? String(!!open) : null, title,
    'data-key': dataKey, 'data-kind': dataKind, 'data-dir': dir, style: `padding-left:${6 + depth * 14}px`,
    draggable: drag ? 'true' : null, ondragstart: drag ? e => { e.dataTransfer.setData('text/x-pondra', drag); e.dataTransfer.setData('text/plain', 'files/' + drag); } : null }, // (into an editor: its path)
    tw, h('span', { class: 'ic ' + iconCls, html: svg(ic, 16) }), h('span', { class: 'nm' }, name), meta ? h('span', { class: 'meta' }, meta) : null,
    dirty ? h('span', { class: 'dirty', title: 'Not saved', 'aria-label': 'not saved' }) : null,
    onmenu ? h('button', { class: 'icon sm more', title: 'More', 'aria-label': `${name}: more`, onclick: e => { e.stopPropagation(); onmenu(e.currentTarget); } }, icon('dots')) : null);
  const toggle = () => {
    if (!kids) return;
    kids.hidden = !kids.hidden;
    row.setAttribute('aria-expanded', String(!kids.hidden));
    if (key) kids.hidden ? (S.open.delete(key), S.open.add('closed:' + key)) : (S.open.add(key), S.open.delete('closed:' + key));
    if (!kids.hidden) onopen?.(); // (what it holds, drawn when first opened)
  };
  tw.click = toggle;
  row.addEventListener('click', e => { if (e.target.closest('.tw')) toggle(); else onclick?.(tw, e); });
  if (ondblclick) row.addEventListener('dblclick', ondblclick);
  if (onmenu) row.addEventListener('contextmenu', e => { e.preventDefault(); onmenu(e); });
  row.toggle = toggle;
  return kids ? h('div', { class: 'item' }, row, kids) : row;
}
// (a file's and a folder's menus, renaming and moving: rename.js; what they do, but open and download: more.js; each loaded when first used)
const later = f => import('./more.js').then(f), menus = f => import('./rename.js').then(f);
export const newFolder = at => later(m => m.newFolder(at)), upload = (at, folder) => import('./upload.js').then(m => m.upload(at, folder)), newAny = at => later(m => m.newAny(at));
export async function download(rel) {
  try { const r = await call(fileUrl(rel)); saveAs(await r.blob(), 'application/octet-stream', base(rel)); } catch (e) { toast(e.message, true); }
}

// ------------------------------------------------------------------ text files: SQL, Python, Markdown and text
export class TextDoc {
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
    await this.naming;
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
  crumbs() { return crumbs(this); }
  rename(name) { return renamed(this, name); }
  /** The menus' items that make something of the file: a table or a view of its query (SQL), a job, a schedule, a copy. */
  jobs(sql, saveAs = true) {
    return [sql ? { label: 'Create as table or view…', icon: 'plus', run: () => R.helpers.createAs(this.current()) } : null, { label: 'Run as a job', icon: 'play', run: () => R.helpers.job(this) },
      { label: 'Schedule…', icon: 'clock', run: () => R.helpers.schedule(this) }, saveAs ? '-' : null, saveAs ? { label: 'Save as…', icon: 'save', run: () => this.save(true) } : null];
  }
  more() {
    return [{ label: 'Save as…', icon: 'save', run: () => this.save(true) }, { label: 'Rename…', icon: 'pencil', run: renaming }, this.path ? { label: 'Versions…', icon: 'clock', run: () => R.helpers.versions(this) } : null,
      this.path ? { label: 'Download', icon: 'down', run: () => saveAs(this.ed.value, 'text/plain', this.title) } : null, this.path ? { label: 'Copy path', icon: 'copy', run: () => copyText('files/' + this.path, 'Path copied') } : null];
  }
  /** A Markdown file drawn (md.js), or its text again. */
  preview(on) {
    this.previewing = on; this.shown ||= h('div', { class: 'md mdfile', tabindex: '0', ondblclick: () => this.preview(false) });
    if (on) import('./md.js').then(m => { m.render(this.shown, this.ed.value, (this.path || '').replace(/[^/]*$/, '')); if (this.previewing) this.main.replaceChildren(this.shown); });
    else { this.main.replaceChildren(this.ed.el); this.ed.focus(); }
    R.helpers.toolbar();
  }
  toolbar() {
    return [...this.crumbs(), h('span', { class: 'grow' }), this.ed.language === 'markdown' ? btn(this.previewing ? 'pencil' : 'eye', this.previewing ? 'Edit' : 'Preview', this.previewing ? 'Its text, to edit (or double-click it)' : 'It drawn, as Markdown', () => this.preview(!this.previewing), 'btn ghost') : null,
      R.helpers.saveButton(this), moreBtn(() => this.more())];
  }
  status() { return [`Ln ${this.pos.line}, Col ${this.pos.col}`, { sql: 'SQL', python: 'Python', text: /\.md$/i.test(this.title) ? 'Markdown' : 'Text' }[this.kind] || '', 'Spaces: 4']; }
}
export const btn = (ic, label, title, fn, cls = 'btn', id) => h('button', { class: cls, title, onclick: fn, id }, ic ? icon(ic) : null, label);
export const moreBtn = items => h('button', { class: 'icon', title: 'More', 'aria-label': 'More', onclick: e => menu(e.currentTarget, items()) }, icon('dots'));

/** A panel under a file (SQL's results, Python's console), its height dragged; or at the right. */
export function splitPanel(doc, panel) {
  const grip = h('div', { class: 'grip', role: 'separator', 'aria-label': 'The panel\'s size (arrows change it)', tabindex: '0', 'aria-valuemin': '120' });
  // (a size set by dragging is kept with the settings, on this machine; until then, a share of the file's height or width)
  const size = { ...R.helpers.prefs('split') }, now = right => Math.round(panel.getBoundingClientRect()[right ? 'width' : 'height']) || (right ? 520 : 320);
  const place = () => {
    const right = doc.layout === 'right', k = right ? 'right' : 'below';
    doc.el.classList.toggle('side', right);
    panel.style.flexBasis = size[k] ? size[k] + 'px' : right ? '45%' : '48%';
    grip.setAttribute('aria-orientation', right ? 'vertical' : 'horizontal'); grip.setAttribute('aria-valuenow', String(size[k] || now(right)));
  };
  grip.addEventListener('pointerdown', e => {
    e.preventDefault(); grip.setPointerCapture(e.pointerId);
    const right = doc.layout === 'right', start = right ? e.clientX : e.clientY, was = now(right);
    const move = ev => { const d = (right ? ev.clientX : ev.clientY) - start, v = Math.max(120, Math.min((right ? innerWidth : innerHeight) - 220, was - d)); panel.style.flexBasis = v + 'px'; size[right ? 'right' : 'below'] = v; };
    grip.addEventListener('pointermove', move);
    grip.addEventListener('pointerup', () => { grip.removeEventListener('pointermove', move); R.helpers.prefs('split', { ...size }); }, { once: true });
  });
  grip.addEventListener('keydown', e => { const d = { ArrowUp: 24, ArrowDown: -24, ArrowLeft: 24, ArrowRight: -24 }[e.key]; if (d) { e.preventDefault(); const right = doc.layout === 'right', k = right ? 'right' : 'below'; size[k] = Math.max(120, (size[k] || now(right)) + d); place(); R.helpers.prefs('split', { ...size }); } });
  doc.el.append(grip, panel);
  doc.place = place;
  place();
}

// ------------------------------------------------------------------ a SQL file (sqlfile.js): what its answers are called
export const said = x => x.kind === 'rows' ? `${count(x.total)} row${x.total === 1 ? '' : 's'}` : x.kind === 'error' ? 'failed' : x.kind === 'plan' ? (x.profile ? 'profiled' : 'plan') : 'done';
/** A statement on one line: its comments out, its spaces one, at most `n` characters. */
export const oneLine = (sql, n) => { const t = sql.replace(/--[^\n]*|\/\*[\s\S]*?\*\//g, ' ').replace(/\s+/g, ' ').trim(); return t.length > n ? t.slice(0, n - 1) + '…' : t; };
/** A copy's or a download's button, with a ▾ for its other forms. */
/** A script's statements, split as the node splits them (routines.rs `statements`): each ends at
 * a `;` outside strings ('…', $$…$$, $tag$…$tag$), quoted names and comments; the last `;` optional. */
export function statements(text, raw) {
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
  return raw ? out : out.map(x => x.trim()); // (raw: as written, what follows a `;` on its line too)
}
/** The last statement of a script (for its plan). */
export const lastStatement = sql => statements(sql).at(-1) || sql;

// ------------------------------------------------------------------ the core's kinds of file, as an extension would register them
export function registerFiles(register) {
  const text = (Cls, kind) => async path => { if (!path) return new Cls({}); const f = await readFile(path); return new Cls({ path, text: f.text, version: f.version, kind }); };
  register.doc({ id: 'notebook', label: 'Notebook', icon: 'notebook', match: p => /\.ipynb$/i.test(p) && !/^notebooks\/[^/]+\//.test(p), open: openPlain }); // (one file, saved in place, its versions kept by the node; notebooks/<name>/<time>.ipynb: saved before that)
  register.doc({ id: 'sql', label: 'SQL file', icon: 'filesql', order: 20, match: p => /\.sql$/i.test(p), open: async path => { const { SqlDoc } = await import('./sqlfile.js'); return text(SqlDoc)(path); } }); // (loaded when a SQL file first opens)
  register.doc({ id: 'python', label: 'Python file', icon: 'filepy', order: 30, match: p => /\.py$/i.test(p), open: async path => { const { PythonDoc } = await import('./pyfile.js'); return text(PythonDoc)(path); } }); // (loaded when a Python file first opens)
  register.doc({ id: 'data', label: 'Data file', icon: 'filedata', order: 40, match: p => DATA.test(p), open: async path => {
    const { DataDoc } = await import('./data.js'); // (loaded when a data file first opens)
    try { return await new DataDoc({ path }).load(); } catch (e) { if (!e.asText) throw e; const f = await readFile(path); return new TextDoc({ path, text: f.text, version: f.version, language: 'text' }); } // (a JSON document, not rows: its text)
  } });
  register.doc({ id: 'text', label: 'Text file', icon: 'file', order: 50, match: p => TEXT.test(p), open: async path => { const f = await readFile(path); return new TextDoc({ path, text: f.text, version: f.version, language: /\.md$/i.test(path) ? 'markdown' : 'text' }); } });
}
