// A table's own tab (a double-click in the Data tree): its rows in the grid, a WHERE and an ORDER BY
// to pick them, edited in place (a cell, a paste, rows added and deleted, undone and done again)
// and saved as one transaction; a view's, read only. Its details stay at the right. Loaded when
// one first opens, not with the page.
import { h, icon, count, S, R, emit, run, ident, quote, toast, sqlType, typeKind, sessionOf, moreStyle } from './core.js';
import { grid, copyText } from './grid.js';
import { Edits, addRow, history } from './gridmore.js';
import { btn, moreBtn } from './files.js';

await moreStyle();

const ICON = { table: 'table', view: 'view', 'materialized view': 'matview', files: 'files' };
const CAST = /^(BIGINT|INT|SMALLINT|TINYINT|DOUBLE|REAL|DECIMAL.*|DATE|TIMESTAMP|TIMESTAMPTZ|TIME|BOOLEAN)$/;
/** A value as SQL writes it into its column: text cast to the column's type, so the node reads it as typed. */
const lit = (v, c) => { if (v == null) return 'NULL'; const t = sqlType(c.type), s = quote(typeof v === 'object' ? JSON.stringify(v) : String(v)); return CAST.test(t) ? `CAST(${s} AS ${t})` : s; };
/** What a column of a kind takes (what can't be is refused before anything is sent). */
const TAKES = { num: [/^\s*[-+]?\d+\s*$/, 'a whole number'], dec: [/^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$|^\s*-?(inf|infinity|nan)\s*$/i, 'a number'], bool: [/^\s*(true|false|t|f|yes|no|on|off|1|0)\s*$/i, 'true or false'] };
const wait = ms => new Promise(r => setTimeout(r, ms));

export class TableDoc {
  constructor(path) {
    Object.assign(this, { path, q: path.slice(6), kind: 'table', dirty: false, cols: [], data: [], where: '', order: '', limit: 1000 });
    this.box = h('div', { class: 'databox' }, h('div', { class: 'wait pulse' }, 'Reading…'));
    this.foot = h('div', { class: 'datafoot' });
    this.el = h('div', { class: 'doc datadoc tabledoc' }, this.picker(), this.box, this.foot);
  }
  /** The table as the Data tree has it (its kind, its columns), when the catalog has been read. */
  get object() { return S.objects?.find(t => t.q === this.q); }
  get title() { return this.object?.t || this.q.split('.').pop(); }
  get icon() { return ICON[this.object?.o.kind] || 'table'; }
  get tip() { return `${this.q}: ${this.editable ? 'its rows, to edit' : 'its rows'}`; }
  /** What the details show while it is in front: the table. */
  pickOf() { const t = this.object; return t ? { type: 'object', t } : null; }
  /** The WHERE, the ORDER BY and how many rows: Enter reads them again. */
  picker() {
    const field = (label, key, hint) => h('label', {}, h('span', {}, label), h('input', { value: this[key], placeholder: hint, spellcheck: 'false', 'aria-label': label, onkeydown: e => { if (e.key === 'Enter') { this[key] = e.target.value.trim(); this.again(); } } }));
    const limit = h('select', { 'aria-label': 'Rows', title: 'How many rows to read', onchange: e => { this.limit = +e.target.value; this.again(); } }, [100, 1000, 10000].map(n => h('option', { value: n, selected: n === this.limit }, `${count(n)} rows`)));
    return h('div', { class: 'tpick' }, field('WHERE', 'where', 'amount > 10'), field('ORDER BY', 'order', 'id DESC'), limit, btn('refresh', '', 'Read the rows again', () => this.again(), 'icon'));
  }
  async load() {
    for (let i = 0; !S.objects && i < 50; i++) await wait(100); // (opened as the page starts: its kind is known once the tree is read)
    const t = this.object, sql = `SELECT ${t?.o.kind === 'table' ? '_row_id, ' : ''}* FROM ${this.q}${this.where ? ` WHERE ${this.where}` : ''}${this.order ? ` ORDER BY ${this.order}` : ''} LIMIT ${this.limit}`;
    let r, ids = /^SELECT _row_id/.test(sql);
    try { r = await run(sql); } catch (e) { if (!ids) throw e; ids = false; r = await run(sql.replace('_row_id, ', '')); } // (a table without row ids: read only)
    this.editable = ids;
    this.cols = r.columns.slice(ids ? 1 : 0);
    this.data = r.rows.map(a => { const row = ids ? a.slice(1) : a; row.id = a[0]; return row; });
    this.loaded = this.data.slice();
    this.edits = ids ? new Edits(this.cols, this.data, { blank: null, fixed: true, onchange: () => { this.dirty = this.edits.dirty; this.footer(); R.helpers.toolbar(); emit('changed', this); } }) : null;
    this.draw();
    return this;
  }
  /** The rows read again (after a save, a new WHERE or ORDER BY): changes not saved are asked about first. */
  async again() {
    if (this.dirty && !confirm(`Throw away the changes to ${this.title}?`)) return;
    this.dirty = false;
    try { await this.load(); } catch (e) { this.box.replaceChildren(h('div', { class: 'err' }, e.message)); }
    R.helpers.toolbar(); R.helpers.status(); emit('changed', this);
  }
  draw() {
    const r = { columns: this.cols, rows: this.data, total: this.data.length };
    this.gridEl = grid(r, { fill: true, footer: true, edit: this.edits, name: this.title, onsum: t => { this.sumText = t; this.footer(); }, explore: i => R.helpers.explore(r, i) });
    this.box.replaceChildren(...this.editable ? [] : [h('div', { class: 'note' }, icon('eye'), `A ${this.object?.o.kind || 'relation'} is read here, not edited: change it with SQL.`)], this.gridEl);
    this.footer();
  }
  get x() { return this.gridEl.grid.x; }
  footer() {
    const b = (ic, label, title, fn, off) => h('button', { class: 'btn ghost small', title, 'aria-label': label ? null : title, disabled: off, onclick: fn }, icon(ic), label), e = this.edits;
    this.foot.replaceChildren(...e ? [b('plus', 'Add row', 'A row at the end', () => addRow(this.x)), h('span', { class: 'sep' }), b('undo', '', 'Undo (Ctrl Z)', () => history(this.x), !e.past.length), b('redo', '', 'Redo (Ctrl Y)', () => history(this.x, true), !e.future.length)] : [],
      h('span', { class: 'sum' }, this.sumText || ''), h('span', { class: 'grow' }),
      h('span', { class: 'hint' }, this.data.length >= this.limit ? `The first ${count(this.limit)} rows${this.where ? ' that match' : ''}` : e ? 'Double-click or type to edit · Ctrl V pastes · Ctrl S saves' : 'Read-only'));
  }
  /** What a save runs: the rows added, then the cells changed, then the rows deleted (each table's
   * rows by `_row_id`), as one transaction. */
  statements() {
    const e = this.edits, cols = this.cols, now = new Set(this.data), out = [], sets = new Map();
    const added = this.data.filter(r => e.isAdded(r) && r.some(v => v != null)), gone = this.loaded.filter(r => !now.has(r));
    for (const r of added) { const at = cols.map((_, i) => i).filter(i => r[i] != null); out.push(`INSERT INTO ${this.q} (${at.map(i => ident(cols[i].name)).join(', ')}) VALUES (${at.map(i => lit(r[i], cols[i])).join(', ')})`); }
    for (const r of this.data) {
      if (e.isAdded(r) || !e.rowChanged(r)) continue;
      const set = cols.map((c, i) => e.changed(r, i) ? `${ident(c.name)} = ${lit(r[i], c)}` : null).filter(Boolean).join(', ');
      (sets.get(set) || sets.set(set, []).get(set)).push(r.id); // (cells set alike, one statement)
    }
    for (const [set, ids] of sets) out.push(`UPDATE ${this.q} SET ${set} WHERE _row_id ${ids.length > 1 ? `IN (${ids.join(', ')})` : '= ' + ids[0]}`);
    if (gone.length) out.push(`DELETE FROM ${this.q} WHERE _row_id IN (${gone.map(r => r.id).join(', ')})`);
    return out;
  }
  /** A value a column can't take, in words (nothing is sent then). */
  refused() {
    const e = this.edits;
    for (const r of this.data) for (let i = 0; i < this.cols.length; i++) {
      const v = r[i], [re, word] = TAKES[typeKind(this.cols[i].type)] || [];
      if (re && v != null && (e.isAdded(r) || e.changed(r, i)) && !re.test(String(v))) return `${this.cols[i].name} takes ${word}: '${v}' is not one`;
    }
  }
  sql() { const s = this.statements(); return s.length > 1 ? ['BEGIN', ...s, 'COMMIT'].join(';\n') + ';' : (s[0] || '') + ';'; }
  async save() {
    if (!this.dirty) return true;
    const no = this.refused();
    if (no) { toast('Not saved: ' + no, true); return false; }
    const said = this.edits.summary().join(', '), s = this.statements();
    if (s.length) {
      try { await run(this.sql(), undefined, undefined, undefined, sessionOf(this)); } // (its own session: a transaction there is the tab's alone)
      catch (e) { toast('Not saved: ' + e.message.split('\n').pop(), true); return false; }
    }
    this.dirty = false;
    toast(`Saved ${this.q}: ${said}`);
    await this.again();
    emit('saved', this);
    return true;
  }
  async discard() { await this.again(); }
  close() { return !this.dirty || confirm(`Close ${this.title}? It has changes that are not saved.`); }
  activate() { requestAnimationFrame(() => this.gridEl?.grid?.box.focus({ preventScroll: true })); }
  toolbar() {
    const t = this.object;
    return [h('span', { class: 'crumb' }, t ? `${t.c} / ${t.s}` : ''), h('span', { class: 'slash' }, '/'), h('span', { class: 'tname' }, this.title),
      this.editable ? this.dirty ? btn('filesql', this.edits.summary().join(', '), 'What Save runs: its SQL, in a new SQL tab', () => R.helpers.newWith('sql', this.sql()), 'btn ghost small') : null : h('span', { class: 'chip' }, 'Read-only'),
      h('span', { class: 'grow' }),
      this.dirty ? btn('', 'Discard', 'Throw away the changes', () => this.discard()) : null, R.helpers.saveButton(this), h('span', { class: 'sep' }),
      btn('play', 'Query with SQL', 'Query it, in a new SQL tab', () => R.helpers.query(`SELECT * FROM ${this.q} LIMIT 1000`)),
      moreBtn(() => [{ label: 'Watch it live', icon: 'refresh', run: () => R.helpers.query(`SELECT * FROM ${this.q} LIMIT 100`, true) }, { label: 'Copy the name', icon: 'copy', run: () => copyText(this.q, `Copied ${this.q}`) }])];
  }
  status() { return [`${count(this.data.length)} rows · ${this.cols.length} columns`, `${this.object?.o.kind || 'table'} · ${this.q}`]; }
}
