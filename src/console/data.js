// A data file (ADR-032, ADR-034): a CSV, TSV, JSON, JSON lines or Parquet file of the lake's, as a
// table, edited in place. Loaded when a data file first opens, not with the page.
import { h, icon, bytes, count, S, R, emit, run, fileSql, ident, toast, prompt, readFile, writeFile, moreStyle, renaming, crumbs, renamed } from './core.js';
import { grid, copyText } from './grid.js';
import { btn, moreBtn, download } from './files.js';

await moreStyle();

const base = p => p.split('/').pop();
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
  rename(name) { return renamed(this, name); }
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
          throw Object.assign(new Error('not rows'), { asText: true }); // (a JSON document, not a table: it opens as text)
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
    await this.naming;
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
    return [...crumbs(this),
      this.readonly ? h('span', { class: 'chip', title: 'Parquet files, and files too big to hold, open read-only: load one into a table to change it with SQL' }, 'Read-only') : this.dirty && this.changes().length ? h('span', { class: 'muted' }, this.changes().join(', ')) : null,
      h('span', { class: 'grow' }),
      this.readonly || !this.dirty ? null : h('button', { class: 'btn', title: 'Throw away the changes', onclick: () => this.discard() }, 'Discard'), this.readonly ? null : R.helpers.saveButton(this), h('span', { class: 'sep' }),
      btn('play', 'Query with SQL', 'Query it, in a new SQL tab', () => R.helpers.query(`SELECT * FROM ${fileSql(this.path)} LIMIT 1000`)),
      moreBtn(() => [{ label: 'Load into a table…', icon: 'up', run: () => this.loadIntoTable() }, { label: 'Versions…', icon: 'clock', run: () => R.helpers.versions(this) }, { label: 'Download', icon: 'down', run: () => download(this.path) }, { label: 'Rename…', icon: 'pencil', run: renaming }, { label: 'Copy path', icon: 'copy', run: () => copyText('files/' + this.path, 'Path copied') }])];
  }
  status() { return [`${count(this.data.length)} rows · ${this.cols.length} columns`, this.format === 'csv' ? `CSV · UTF-8 · ${this.sep === '\t' ? 'tab' : 'comma'}` : this.format.toUpperCase()]; }
}
/** A JSON file's cell, as typed: a number, true, false, null, an object or a list stay what they are. */
const parseValue = t => { if (t === '') return null; if (/^(-?\d+(\.\d+)?([eE][+-]?\d+)?|true|false|null|[[{].*)$/s.test(t.trim())) { try { return JSON.parse(t); } catch { /* (text) */ } } return t; };

