// Notebooks (ADR-030, ADR-034): SQL, Python and Markdown cells in a tab, saved in the lake as
// Jupyter notebooks (`files/notebooks/<name>/<time>.ipynb`, a version per save), or, anywhere
// else, as one plain `.ipynb` file, saved in place like a SQL file.
import { h, $, icon, esc, secs, count, S, R, emit, call, rows, fileUrl, toast, menu, saveAs, numeric, Failure, said, failed, readFile, writeFile, interruptPython, formatPython } from './core.js';
import { Editor, formatSql } from './editor.js';
/** A Markdown cell's text drawn (md.js: loaded when a notebook first has one). */
const drawMd = async (el, src) => { const m = await import('./md.js'); m.render(el, src); };

// ------------------------------------------------------------------ what a cell answered
export function doneText(v) {
  if (v == null || typeof v !== 'object') return v == null ? 'Done.' : String(v);
  const entries = Object.entries(v).filter(([k, x]) => (k !== 'called' || v.called !== 'do') && x != null && !(Array.isArray(x) && !x.length)); // (nothing said for what is empty)
  if (!entries.length) return v.called === 'do' ? '' : 'Done.';
  return entries.map(([k, x]) => k === 'rows' && typeof x === 'number' ? `${count(x)} row${x === 1 ? '' : 's'}` : `${k.replace(/_/g, ' ')} ${typeof x === 'string' ? x : JSON.stringify(x)}`).join(' · ');
}
/** An answer drawn by the first view that takes it (errors, rows, figures, text, what it did). */
export function answer(r, cell) {
  const out = [];
  if (r.notices?.length) out.push(said(r.notices.join('\n').replace(/\n…$/, '\n… (it printed more: the node sends back the first 32 KB)')));
  const view = R.renderers.find(v => { try { return v.match(r); } catch { return false; } });
  if (view) { const el = view.render(r, cell); if (el) out.push(el); }
  return out;
}

// ------------------------------------------------------------------ cells
/** A cell's code as another kind's: SQL made Python is `db.sql("""…""")` (its answer the same rows), and back. */
function convert(src, from, to) {
  const s = src.trim(), q = s.includes('"""') ? "'''" : '"""';
  if (from === 'sql' && to === 'python') return `db.sql(${q}\n${s}\n${q})`;
  const m = from === 'python' && to === 'sql' && s.match(/^db\.sql\(\s*("""|'''|"|')([\s\S]*?)\1\s*\)$/);
  return m ? m[2].trim() : src;
}
let made = 0;
const newId = () => 'c' + Date.now().toString(36) + (made++).toString(36); // (cell ids as nbformat has them)

export class Cell {
  constructor(nb, o = {}) {
    this.nb = nb;
    this.id = /^[A-Za-z0-9_-]{1,64}$/.test(o.id || '') ? o.id : newId();
    this.kind = o.kind || 'sql';
    this.result = null; this.count = null; this.ctl = null; this.stream = null;
    // (a press on it keeps the text's focus: a Markdown cell being edited stays open while its menu is)
    this.kindSel = h('button', { class: 'kind', 'aria-haspopup': 'menu', 'aria-label': 'Kind of cell', title: 'SQL, Python or Markdown (S, P, M)', onmousedown: e => e.preventDefault(), onclick: e => this.kindMenu(e.currentTarget) });
    this.runBtn = h('button', { class: 'run', title: 'Run (Ctrl+Enter)', onclick: () => this.ctl ? this.stop() : this.run() });
    this.idle();
    this.liveBox = h('input', { type: 'checkbox', onchange: () => this.setLive(this.liveBox.checked) });
    this.liveEl = h('label', { class: 'live', title: 'Live: the answer again each time a commit changes what it reads (L)' }, this.liveBox, h('span', { class: 'switch' }), 'Live');
    this.num = h('span', { class: 'n' });
    this.status = h('span', { class: 'st', 'aria-live': 'polite' });
    const tool = (ic, title, fn) => h('button', { class: 'icon', title, 'aria-label': title, onclick: fn }, icon(ic));
    const i = () => nb.cells.indexOf(this);
    this.bar = h('div', { class: 'bar' }, this.num, this.kindSel, this.runBtn, this.liveEl, this.status,
      h('span', { class: 'tools' }, tool('arrowUp', 'Move up', () => nb.move(this, -1)), tool('arrowDown', 'Move down', () => nb.move(this, 1)),
        tool('plus', 'Add a cell below (B)', () => nb.add({ kind: this.kind === 'markdown' ? 'sql' : this.kind }, this, true).edit()),
        tool('dots', 'More', e => menu(e.currentTarget, [{ label: 'Run the cells above', icon: 'arrowUp', run: () => nb.runSome(0, i()) }, { label: 'Run this and the cells below', icon: 'arrowDown', run: () => nb.runSome(i()) }, '-',
          { label: 'Add a cell above', icon: 'plus', keys: 'A', run: () => nb.add({ kind: this.kind === 'markdown' ? 'sql' : this.kind }, this, false).edit() }, { label: 'Add a cell below', keys: 'B', run: () => nb.add({ kind: this.kind === 'markdown' ? 'sql' : this.kind }, this, true).edit() }, '-',
          { label: this.el.classList.contains('folded') ? 'Show the output' : 'Hide the output', icon: 'eye', keys: 'O', run: () => this.fold() }, { label: 'Clear the output', icon: 'clear', run: () => this.clear() }, '-',
          ...[...R.kinds.values()].map(k => ({ label: `Make it ${k.label}`, checked: k.id === this.kind, run: () => { this.setKind(k.id); this.edit(); } })), '-',
          { label: 'Delete the cell', icon: 'trash', keys: 'D D', run: () => nb.remove(this) }]))));
    this.ed = new Editor({ grow: true, value: o.src || '', label: 'Code', oninput: () => nb.changed(), onkey: e => this.key(e) });
    this.ed.menu = () => ['-', { label: 'Run cell', icon: 'play', keys: 'Ctrl Enter', run: () => this.run() }, { label: 'Run the cells above', run: () => nb.runSome(0, i()) }, { label: 'Run this and the cells below', run: () => nb.runSome(i()) },
      ...this.fmt ? ['-', ...this.ed.formats(this.fmt, 'cell', true)] : [], this.kind === 'sql' ? '-' : null, this.kind === 'sql' ? { label: 'Create as table or view…', icon: 'plus', run: () => R.helpers.createAs(this.src) } : null,
      ...['sql', 'python'].filter(k => k !== this.kind && this.kind !== 'markdown').map(k => ({ label: k === 'sql' ? 'Make it SQL' : 'Make it Python', run: () => this.setKind(k) }))];
    this.ta = this.ed.ta;
    this.md = h('div', { class: 'md', ondblclick: () => this.edit() });
    this.out = h('div', { class: 'out', onclick: () => { if (this.el.classList.contains('folded')) this.fold(false); } });
    // (the space above a cell, pointed at: a cell of each kind added there, between it and the one before)
    const here = h('div', { class: 'here' }, [...R.kinds.values()].map(k => h('button', { tabindex: '-1', title: `Add a ${k.label} cell here (A: above, B: below)`, onclick: () => nb.add({ kind: k.id }, this, false).edit() }, '+ ' + k.label)));
    this.el = h('section', { class: 'cell', tabindex: '-1', 'data-kind': this.kind }, here, this.bar, h('div', { class: 'ed' }, this.ed.el), this.md, this.out);
    this.el.cell = this;
    this.ta.addEventListener('focus', () => { nb.select(this); this.el.classList.add('editing'); nb.last = this; });
    this.ta.addEventListener('blur', () => { if (this.kind === 'markdown') drawMd(this.md, this.src); this.el.classList.remove('editing'); });
    this.el.addEventListener('mousedown', e => { if (!e.target.closest('textarea,button,select,input,a,label,.grid')) nb.select(this); });
    this.chartKeep = { st: o.chart }; this.view = o.view || null; // (its answer's chart, and the view of it open: kept with the notebook)
    this.setKind(this.kind, true);
    if (o.live) { this.liveBox.checked = true; this.status.textContent = 'live once run'; }
    if (o.out) this.show(o.out, true);
  }
  get src() { return this.ta.value; }
  kindMenu(at) { menu(at, [...R.kinds.values()].map(k => ({ label: k.label, checked: k.id === this.kind, run: () => { this.setKind(k.id); this.edit(); } }))); }
  /** Stop it: a Python cell is interrupted on the node (its variables stay); a SQL one is no longer waited for. */
  stop() { if (this.kind === 'python') interruptPython(); else this.ctl?.abort(); }
  get type() { return R.kinds.get(this.kind) || R.kinds.get('sql'); }
  idle() { this.runBtn.replaceChildren(icon('play'), 'Run'); this.runBtn.classList.remove('stop'); this.runBtn.title = 'Run (Ctrl+Enter)'; }
  fold(on = !this.el.classList.contains('folded')) { this.el.classList.toggle('folded', on); }
  clear() { this.stopLive(); this.result = null; this.out.replaceChildren(); this.status.textContent = ''; this.nb.changed(); }
  setKind(k, quiet) {
    const was = this.kind;
    this.kind = R.kinds.has(k) ? k : 'sql'; k = this.kind;
    if (!quiet && was !== k && this.src.trim()) this.ed.value = convert(this.src, was, k); // (SQL and Python cells: one written as the other)
    this.el.dataset.kind = k; this.kindSel.replaceChildren(this.type.label, icon('chevd', 'ic', 12));
    this.liveEl.hidden = !this.type.live;
    if (!this.type.live) { this.stopLive(); this.liveBox.checked = false; }
    this.ta.placeholder = this.type.placeholder || '';
    if (k === 'markdown') drawMd(this.md, this.src);
    this.ed.setLanguage(this.type.language || k);
    if (!quiet) this.nb.changed();
  }
  paint() { this.ed.paint(); }
  edit() {
    this.el.classList.add('editing');
    this.ed.focus();
    this.el.scrollIntoView({ block: 'nearest' });
  }
  /** Keys typed in the cell: running it, and leaving it. */
  key(e) {
    const mod = e.ctrlKey || e.metaKey, nb = this.nb;
    if (e.key === 'Enter' && (mod || e.shiftKey || e.altKey)) {
      e.preventDefault();
      if (mod && e.shiftKey) nb.runSome(0);
      else if (mod) this.run();
      else if (e.shiftKey) { this.run(); nb.next(this); }
      else { this.run(); nb.add({ kind: this.kind === 'markdown' ? 'sql' : this.kind }, this, true).edit(); }
      return true;
    }
    if (e.key === 'Escape') { e.preventDefault(); this.ta.blur(); this.el.focus({ preventScroll: true }); return true; }
    if (e.shiftKey && e.altKey && e.key.toLowerCase() === 'f' && this.fmt) { e.preventDefault(); this.format(); return true; }
    return false;
  }
  /** How its kind is formatted: SQL here, Python by the node's Python (ruff or black); Markdown isn't. */
  get fmt() { return this.kind === 'sql' ? formatSql : this.kind === 'python' ? formatPython : null; }
  format() { if (this.fmt) this.ed.reformat(this.fmt); }
  async run() {
    if (!this.type.run) { drawMd(this.md, this.src); this.ta.blur(); this.el.focus({ preventScroll: true }); return { kind: 'done' }; }
    const text = this.src.trim();
    if (!text) return { kind: 'done' };
    this.stopLive();
    this.ctl?.abort();
    const ctl = this.ctl = new AbortController(), t0 = performance.now();
    this.count = ++S.runs; this.num.textContent = `[${this.count}]`;
    this.runBtn.replaceChildren(icon('stop'), 'Stop'); this.runBtn.classList.add('stop'); this.runBtn.title = this.kind === 'python' ? 'Interrupt it (its variables stay)' : 'Stop waiting for it';
    emit('run', this); this.nb.running();
    this.status.className = 'st pulse'; this.status.textContent = 'running…';
    const tick = setInterval(() => { this.status.textContent = 'running… ' + secs(performance.now() - t0); }, 250);
    let r;
    try {
      r = await this.type.run(text, ctl.signal);
    } catch (e) {
      r = failed(e);
    } finally {
      clearInterval(tick);
      if (this.ctl === ctl) this.ctl = null;
      this.idle(); this.nb.running();
    }
    r.ms = performance.now() - t0;
    if (this.kind === 'sql') r.src = text; // (its plan: of what ran)
    // (one query: Download can fetch every row of it again, not only those shown)
    if (this.kind === 'sql' && r.kind === 'rows' && !text.replace(/;\s*$/, '').includes(';') && /^\s*(select|with|from|values|table)\b/i.test(text.replace(/--[^\n]*|\/\*[\s\S]*?\*\//g, ' '))) r.sql = text;
    this.show(r);
    if (r.kind === 'rows' && this.type.live && this.liveBox.checked) this.startLive();
    this.nb.changed(true);
    emit('ran', this, r, { kind: this.kind, src: text });
    return r;
  }
  show(r, saved) {
    this.result = r;
    this.out.replaceChildren(...answer(r, this));
    if (saved) this.out.append(h('div', { class: 'meta' }, h('span', { class: 'badge', title: 'As it was when the notebook was saved: run the cell for the answer now' }, 'saved')));
    this.status.className = 'st' + (r.kind === 'error' ? ' bad' : '');
    this.status.textContent = saved || r.kind === 'rows' ? '' : r.kind === 'error' ? `failed · ${secs(r.ms)}` : secs(r.ms); // (rows: their count and time under them)
  }
  setLive(on) {
    this.liveBox.checked = on;
    if (on) this.run(); else { this.stopLive(); this.status.textContent = ''; }
    this.nb.changed();
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

// ------------------------------------------------------------------ .ipynb
const lines = s => s.split(/(?<=\n)/);
function textTable(columns, rs, total) {
  const cells = [columns.map(c => c.name), ...rs.map(r => r.map(v => { const s = v == null ? 'NULL' : typeof v === 'object' ? JSON.stringify(v) : String(v); return s.length > 60 ? s.slice(0, 59) + '…' : s.replace(/\n/g, ' '); }))];
  const w = columns.map((_, i) => Math.max(...cells.map(r => r[i].length)));
  const fmt = r => r.map((s, i) => numeric(columns[i].type) ? s.padStart(w[i]) : s.padEnd(w[i])).join(' | ').trimEnd();
  return [fmt(cells[0]), w.map(n => '-'.repeat(n)).join('-+-'), ...cells.slice(1).map(fmt), `(${count(total)} row${total === 1 ? '' : 's'})`].join('\n');
}
const KEPT = 100; // rows a saved notebook keeps of each answer
/** A code cell's own metadata: live, and its answer's view (a chart, with its settings; a plan). */
const pondraOf = c => { const p = { live: c.kind === 'sql' && c.liveBox.checked || null, view: c.view, chart: c.view === 'chart' ? c.chartKeep.st : null }; for (const k in p) if (p[k] == null) delete p[k]; return Object.keys(p).length ? { pondra: p } : {}; };
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
export function cellsOf(nb) {
  if (!nb || !Array.isArray(nb.cells)) throw new Error('this is not a notebook (.ipynb): it has no cells');
  return nb.cells.map(c => {
    const src = Array.isArray(c.source) ? c.source.join('') : String(c.source ?? '');
    if (c.cell_type !== 'code') return { kind: 'markdown', src, id: c.id };
    const magic = src.match(/^%%sql[^\n]*(\n|$)/);
    const p = c.metadata?.pondra || {};
    return { kind: magic ? 'sql' : 'python', src: magic ? src.slice(magic[0].length) : src, id: c.id, live: !!p.live, view: p.view, chart: p.chart, out: savedAnswer(c.outputs) };
  });
}
export const cleanName = s => s.trim().replace(/\.ipynb$/i, '').replace(/^notebooks\//, '').replace(/[^\w.-]+/g, '-').replace(/^[.-]+|-+$/g, '').slice(0, 80);
const stampOf = () => new Date().toISOString().replace(/[:.]/g, '-');
/** A notebook's versions in the lake, newest first: `[{ version, written }]`. */
export async function versions(name) {
  const fs = await rows(`SELECT path, written FROM files('notebooks/${name.replace(/'/g, "''")}/') ORDER BY path DESC`);
  return fs.map(f => ({ version: f.path.match(/\/([^/]+)\.ipynb$/)?.[1], written: f.written })).filter(v => v.version);
}

// ------------------------------------------------------------------ the notebook, a document
/** A notebook in a tab: its cells, its toolbar (Run all, Interrupt, Restart, Python, Save), its keys.
 * With a `dir` (`''` or `reports/`) it is a plain file, `<dir><name>.ipynb`, saved in place. */
export class Notebook {
  constructor({ name = 'untitled', version = null, nb = { cells: [] }, dir = null } = {}) {
    this.kind = 'notebook'; this.icon = 'notebook'; this.dir = dir;
    this.cells = []; this.sel = null; this.last = null; this.trash = null; this.dirty = false;
    this.box = h('div', { class: 'cells', 'aria-label': 'Cells' });
    const add = kind => h('button', { class: 'btn ghost', 'data-add': kind, onclick: () => this.add({ kind }).edit() }, '+ ' + { sql: 'SQL', python: 'Python', markdown: 'Markdown' }[kind]);
    this.el = h('div', { class: 'doc nbdoc' }, h('div', { class: 'nbcol' }, this.box, h('div', { class: 'add' }, add('sql'), add('python'), add('markdown')),
      h('div', { class: 'hint' }, h('kbd', {}, 'Ctrl'), ' ', h('kbd', {}, 'Enter'), ' runs a cell · ', h('kbd', {}, 'Tab'), ' completes a name · ', h('kbd', {}, 'Esc'), ' then ', h('kbd', {}, '?'), ' lists every key')));
    this.load(nb, name, version);
  }
  get plain() { return this.dir != null; }
  get path() { return this.plain ? `${this.dir}${this.name}.ipynb` : `notebooks/${this.name}`; }
  get title() { return this.name + '.ipynb'; }
  get blank() { return !this.cells.some(c => c.src.trim()); } // (nothing typed: nothing to lose)
  /** The cells from a notebook's JSON (a saved version, an upload, an extension's). */
  load(nb, name, version) {
    const cells = cellsOf(nb);
    for (const c of this.cells) { c.stopLive(); c.ctl?.abort(); }
    this.cells = []; this.box.replaceChildren();
    for (const c of cells.length ? cells : [{}]) this.add(c, null, true, true);
    this.name = name; this.version = version; this.written = version ? Date.now() : null;
    this.select(this.cells[0]);
    this.dirty = false; emit('changed', this); // (new and untouched: nothing to save yet)
  }
  add(o, near, below = true, quiet) {
    const c = new Cell(this, o), i = near ? this.cells.indexOf(near) + (below ? 1 : 0) : this.cells.length;
    this.cells.splice(i, 0, c);
    this.box.insertBefore(c.el, this.cells[i + 1]?.el || null);
    c.paint();
    if (!quiet) this.changed();
    return c;
  }
  remove(c) {
    c.stopLive(); c.ctl?.abort();
    const i = this.cells.indexOf(c);
    this.cells.splice(i, 1); c.el.remove();
    this.trash = { c, i };
    if (!this.cells.length) this.add({});
    this.select(this.cells[Math.min(i, this.cells.length - 1)], true);
    this.changed();
  }
  restore() {
    if (!this.trash) return;
    const { c, i } = this.trash;
    this.trash = null;
    this.cells.splice(Math.min(i, this.cells.length), 0, c);
    this.box.insertBefore(c.el, this.cells[i + 1]?.el || null);
    this.select(c, true); this.changed();
  }
  move(c, d) {
    const i = this.cells.indexOf(c), j = i + d;
    if (j < 0 || j >= this.cells.length) return;
    this.cells.splice(i, 1); this.cells.splice(j, 0, c);
    this.box.insertBefore(c.el, this.cells[j + 1]?.el || null);
    c.el.scrollIntoView({ block: 'nearest' }); this.changed();
  }
  select(c, focus) {
    if (!c) return;
    if (this.sel && this.sel !== c) this.sel.el.classList.remove('sel');
    this.sel = c; c.el.classList.add('sel');
    if (S.doc === this) S.sel = c;
    if (focus) { c.el.focus({ preventScroll: true }); c.el.scrollIntoView({ block: 'nearest' }); }
  }
  next(c) {
    const i = this.cells.indexOf(c), made = !this.cells[i + 1];
    const n = this.cells[i + 1] || this.add({ kind: c.kind === 'markdown' ? 'sql' : c.kind });
    this.select(n, true);
    if (made) n.edit(); // (as Jupyter: a new cell is for typing in)
  }
  async runSome(from, to) {
    for (const c of this.cells.slice(from, to)) {
      this.select(c, true);
      const r = await c.run();
      if (r?.kind === 'error') { toast('Stopped at a cell that failed', true); return; }
    }
  }
  interrupt() { for (const c of this.cells) if (c.ctl) c.stop(); }
  /** Cells started or ended: the toolbar's Run all becomes Stop while one runs. */
  running() { const on = this.cells.some(c => c.ctl); if (on !== this.busy) { this.busy = on; if (S.doc === this) R.helpers.toolbar(); } }
  clearOutputs() { for (const c of this.cells) { c.stopLive(); c.result = null; c.count = null; c.num.textContent = ''; c.out.replaceChildren(); c.status.textContent = ''; } this.changed(); }
  /** A table's first rows, in the empty selected cell or a new one. */
  peek(sql) {
    const empty = this.sel && !this.sel.src.trim() && this.sel.kind === 'sql' ? this.sel : null;
    const c = empty || this.add({ kind: 'sql' }, this.sel, true);
    c.ed.value = sql; this.changed();
    this.select(c, true); c.run();
  }
  /** Text where the last cell typed in is (a column's name clicked in the Data view). */
  put(text) { const c = this.last && this.cells.includes(this.last) ? this.last : this.sel; if (!c || c.kind === 'markdown') return; c.edit(); c.ed.insert(text); }
  changed(ran) {
    if (!ran && !this.dirty) { this.dirty = true; emit('changed', this); }
    else emit('changed', this, ran);
  }
  saved() { this.dirty = false; emit('changed', this); }
  /** The headings of its Markdown cells, for the outline under its row in the Workspace. */
  outline() { return this.cells.flatMap(c => c.kind !== 'markdown' ? [] : [...c.src.matchAll(/^(#{1,3})\s+(.+)$/gm)].map(m => ({ c, level: m[1].length, text: m[2].replace(/[*_`]/g, '') }))); }
  goto(c) { this.select(c, true); c.el.scrollIntoView({ block: 'start', behavior: 'smooth' }); }
  notebook() {
    return {
      cells: this.cells.map(c => c.kind === 'markdown'
        ? { cell_type: 'markdown', id: c.id, metadata: {}, source: lines(c.src) }
        : { cell_type: 'code', id: c.id, metadata: pondraOf(c), execution_count: c.result ? c.count ?? null : null, source: lines(c.kind === 'sql' ? '%%sql\n' + c.src : c.src), outputs: outputs(c) }),
      metadata: { kernelspec: { name: 'python3', display_name: 'Python 3', language: 'python' }, language_info: { name: 'python' }, pondra: { database: S.db || S.lake } },
      nbformat: 4, nbformat_minor: 5,
    };
  }
  /** Save in the lake as a new version (a file in the lake is never replaced, so every version stays), or a plain file in place. */
  async save() {
    const input = $('#nbname');
    const name = input && input.value !== this.name ? cleanName(input.value) : this.name;
    if (!name) { toast('Give the notebook a name first', true); input?.focus(); return false; }
    this.name = name; if (input) input.value = name;
    const version = stampOf(), path = this.plain ? this.path : `notebooks/${name}/${version}.ipynb`, body = JSON.stringify(this.notebook(), null, 1) + '\n';
    try {
      if (this.plain) { const v = await writeFile(path, body, this.version, 'application/x-ipynb+json'); if (!v) return false; this.version = v; } // (refused, with a word, if someone saved it since)
      else { await call(fileUrl(path), { method: 'PUT', body, headers: { 'content-type': 'application/x-ipynb+json' } }); this.version = version; }
      this.written = Date.now(); this.saved();
      toast(this.plain ? `Saved: files/${path}` : `Saved: ${name} (a new version)`);
      emit('saved', this, `files/${path}`);
      return true;
    } catch (e) {
      toast('Not saved: ' + e.message, true);
      return false;
    }
  }
  /** Close it: its live answers stop, and it asks first if it has changes. */
  close() {
    if (this.dirty && !this.blank && !confirm(`Close ${this.title}? It has changes that are not saved.`)) return false;
    for (const c of this.cells) { c.stopLive(); c.ctl?.abort(); }
    return true;
  }
  activate() { S.nb = this; S.sel = this.sel; this.sel?.el.focus({ preventScroll: true }); }
  /** The toolbar: its name (edit it to rename), whether it is saved, and its actions. */
  toolbar() {
    const name = h('input', { id: 'nbname', value: this.name, 'aria-label': 'Notebook name', spellcheck: 'false', title: 'The notebook\'s name: Ctrl+S saves it under this one' });
    name.addEventListener('change', () => { this.name = cleanName(name.value) || 'untitled'; name.value = this.name; this.version = null; this.changed(); });
    name.addEventListener('keydown', e => { if (e.key === 'Enter') name.blur(); });
    const i = () => Math.max(0, this.cells.indexOf(this.sel));
    const pill = R.helpers.pythonPill();
    this.drawPill = pill.draw;
    return [h('span', { class: 'crumb' }, this.dir ?? 'notebooks/'), name, h('span', { class: 'grow' }),
      R.helpers.runButton(this.busy, { label: 'Run all', title: 'Run every cell, in order (Ctrl+Shift+Enter)', run: () => this.runSome(0), stop: () => this.interrupt(), stopTitle: 'Stop the cells running (Python\'s are interrupted, their variables kept)' }, [
        { label: 'Run all', icon: 'play', keys: 'Ctrl Shift Enter', run: () => this.runSome(0) }, { label: 'Run the cells above', icon: 'arrowUp', run: () => this.runSome(0, i()) },
        { label: 'Run this and the cells below', icon: 'arrowDown', run: () => this.runSome(i()) }, '-', { label: 'Clear every output', icon: 'clear', run: () => this.clearOutputs() },
        // (a job runs a notebook of notebooks/, with its versions: not a plain file)
        ...this.plain ? [] : ['-', { label: 'Run as a job', icon: 'play', run: () => R.helpers.job(this) }, { label: 'Schedule…', icon: 'clock', run: () => R.helpers.schedule(this) }]]),
      pill, R.helpers.saveButton(this),
      h('button', { class: 'icon', title: 'More', 'aria-label': 'More', onclick: e => menu(e.currentTarget, [
        this.plain ? null : { label: 'Versions…', icon: 'clock', run: () => R.helpers.pickFile(`files/${this.path}`) },
        { label: 'Download as .ipynb', icon: 'down', run: () => saveAs(JSON.stringify(this.notebook(), null, 1) + '\n', 'application/x-ipynb+json', this.name + '.ipynb') }]) }, icon('dots'))];
  }
  status() { return [`${this.cells.length} cell${this.cells.length === 1 ? '' : 's'}`, 'Notebook']; }
  /** Keys on the selected cell, after Esc (Jupyter's). */
  onkey(e) {
    const c = this.sel, mod = e.ctrlKey || e.metaKey;
    if (!c || e.altKey || (mod && e.key !== 'Enter')) return false;
    const key = e.key.length === 1 ? e.key.toLowerCase() : e.key, prev = this.lastKey;
    this.lastKey = key;
    const go = d => { const n = this.cells[this.cells.indexOf(c) + d]; if (n) this.select(n, true); };
    const acts = {
      Enter: () => mod && e.shiftKey ? this.runSome(0) : mod ? c.run() : e.shiftKey ? (c.run(), this.next(c)) : c.edit(),
      ArrowUp: () => go(-1), k: () => go(-1), ArrowDown: () => go(1), j: () => go(1),
      a: () => this.select(this.add({ kind: c.kind === 'markdown' ? 'sql' : c.kind }, c, false), true),
      b: () => this.select(this.add({ kind: c.kind === 'markdown' ? 'sql' : c.kind }, c, true), true),
      d: () => { if (prev === 'd') { this.lastKey = ''; this.remove(c); } },
      z: () => this.restore(),
      s: () => c.setKind('sql'), p: () => c.setKind('python'), m: () => c.setKind('markdown'),
      l: () => { if (c.type.live) c.setLive(!c.liveBox.checked); },
      o: () => c.fold(),
      0: () => { if (prev === '0') { this.lastKey = ''; if (confirm('Restart Python? Its variables go.')) R.helpers.restart(); } },
    };
    if (acts[key]) { e.preventDefault(); acts[key](); return true; }
    const mine = R.keys.find(k => k.run && k.group === 'On a cell (after Esc)' && k.keys.toLowerCase() === key);
    if (mine) { e.preventDefault(); mine.run(c); return true; }
    return false;
  }
}
/** A notebook that is one plain file (`<folder>/<name>.ipynb`), to be saved in place. */
export async function openPlain(path) {
  const f = await readFile(path), dir = path.slice(0, path.lastIndexOf('/') + 1);
  return new Notebook({ name: path.slice(dir.length).replace(/\.ipynb$/i, ''), dir, version: f.version, nb: JSON.parse(f.text) });
}
/** Open a saved notebook's version (the latest if none is named). */
export async function openNotebook(name, version) {
  if (!version) version = (await versions(name))[0]?.version;
  if (!version) throw new Error(`no notebook ${name} in the lake`);
  const r = await call(fileUrl(`notebooks/${name}/${version}.ipynb`));
  return new Notebook({ name, version, nb: JSON.parse(await r.text()) });
}
