// The grid's less used parts (ADR-034 §7): its menus, the filter window, and editing (a cell, a
// paste, cut, undo and redo) over `Edits`, the model a data file's rows change through and a
// table's rows will. Loaded the first time one is used, not with the page.
import { h, count, numeric, menu, pop, toast, moreStyle, ICONS, ident } from './core.js';
import { OPS, COPIES, copyItems, copyText, downloadItems, fetchRows } from './grid.js';

await moreStyle();
Object.assign(ICONS, { undo: '<path d="M9 14 4 9l5-5"/><path d="M4 9h10.5a5.5 5.5 0 0 1 0 11H11"/>', redo: '<path d="m15 14 5-5-5-5"/><path d="M20 9H9.5a5.5 5.5 0 0 0 0 11H13"/>' });

const s = n => n === 1 ? '' : 's';
const same = (a, b) => a === b || a != null && b != null && typeof a === 'object' && JSON.stringify(a) === JSON.stringify(b);

// ------------------------------------------------------------------ the model
/** Rows as a document that changes a step at a time: each step (a cell typed, a paste, rows added or
 * deleted, a column added) is undone and done again whole. A cell counts as changed while it
 * differs from what it held when last saved (`row.base`), so undoing back to that unmarks it.
 * `blank` is an empty cell's value and `parse(text)` a typed one's. */
export class Edits {
  constructor(cols, rows, { blank = '', parse = t => t, onchange } = {}) {
    Object.assign(this, { cols, rows, blank, parse, onchange, past: [], future: [], step: null, added: new WeakSet() });
    this.saved();
  }
  /** As saved: nothing changed, the rows as they are now. */
  saved() { for (const row of this.rows) row.base = null; this.added = new WeakSet(); this.at = this.past.length; this.n = this.rows.length; this.width = this.cols.length; }
  get dirty() { return this.past.length !== this.at; }
  changed(row, c) { return !!row.base && !same(row.base[c], row[c]); }
  isAdded(row) { return this.added.has(row); }
  /** Whether a row differs from the one saved (a cell, or a column added). */
  rowChanged(row) { return this.added.has(row) || !!row.base && (row.length !== row.base.length || row.some((v, c) => !same(v, row.base[c]))); }
  /** One step: what `fn` does, undone and done again as one; `sel` is what to select after either. */
  do(fn, sel) {
    if (this.step) return fn();
    const parts = this.step = [];
    try { fn(); } finally { this.step = null; }
    if (!parts.length) return;
    if (this.at > this.past.length) this.at = -1; // (the version saved can't be reached again)
    this.past.push({ parts, sel }); this.future = [];
    this.onchange?.();
  }
  run(redo, undo, sel) { this.do(() => { redo(); this.step.push([redo, undo]); }, sel); }
  put(row, c, v) { if (!row.base && !this.added.has(row)) row.base = row.slice(); row[c] = v; }
  /** Cells set, `[row, column, text]` each (an empty text: an empty cell). */
  set(cells, sel) {
    const was = cells.map(([row, c]) => row[c]), now = cells.map(([, , t]) => t === '' ? this.blank : this.parse(t));
    this.run(() => cells.forEach(([row, c], i) => this.put(row, c, now[i])), () => cells.forEach(([row, c], i) => this.put(row, c, was[i])), sel);
  }
  /** `n` empty rows at the end: their objects, to fill. */
  addRows(n) {
    const add = Array.from({ length: n }, () => this.cols.map(() => this.blank));
    for (const row of add) this.added.add(row);
    this.run(() => { for (const row of add) this.rows.push(row); }, () => { this.rows.length -= n; }, { rows: add, c0: 0, c1: 0 });
    return add;
  }
  /** Rows taken out (their objects); undone, each goes back where it was. */
  del(gone) {
    const out = new Set(gone), at = [];
    this.rows.forEach((row, i) => { if (out.has(row)) at.push([i, row]); });
    const keep = this.rows.filter(row => !out.has(row));
    const swap = list => { this.rows.length = 0; for (const row of list) this.rows.push(row); };
    this.run(() => swap(keep), () => { const back = []; let j = 0, a = 0; for (let i = 0; i < keep.length + at.length; i++) back.push(a < at.length && at[a][0] === i ? at[a++][1] : keep[j++]); swap(back); },
      { rows: gone, c0: 0, c1: this.cols.length - 1 });
  }
  /** Columns added at the right, by name. */
  addCols(names) {
    const n = names.length;
    this.run(() => { for (const name of names) this.cols.push({ name, type: 'Utf8' }); for (const row of this.rows) { if (!row.base && !this.added.has(row)) row.base = row.slice(); for (let i = 0; i < n; i++) row.push(this.blank); } },
      () => { this.cols.length -= n; for (const row of this.rows) row.length -= n; });
  }
  undo() { const st = this.past.pop(); if (st) { for (const [, u] of [...st.parts].reverse()) u(); this.future.push(st); this.onchange?.(); } return st; }
  redo() { const st = this.future.pop(); if (st) { for (const [r] of st.parts) r(); this.past.push(st); this.onchange?.(); } return st; }
  /** What changed since it was saved, in words. */
  summary() {
    let cells = 0, added = 0;
    for (const row of this.rows) {
      if (this.added.has(row)) added++;
      else if (row.base) for (let c = 0; c < this.width; c++) if (!same(row.base[c], row[c])) cells++;
    }
    const gone = this.n - (this.rows.length - added), cols = this.cols.length - this.width;
    return [cells && `${count(cells)} cell${s(cells)} changed`, added && `${count(added)} row${s(added)} added`, gone > 0 && `${count(gone)} row${s(gone)} deleted`, cols > 0 && `${cols} column${s(cols)} added`].filter(Boolean);
  }
}
/** New columns' names: `column_N`, after the ones there. */
const newNames = (cols, n) => { const have = new Set(cols.map(c => c.name)), out = []; for (let i = cols.length + 1; out.length < n; i++) if (!have.has('column_' + i)) out.push('column_' + i); return out; };
/** Tab-separated cells, as spreadsheets copy them: a field in quotes may hold tabs, new lines and doubled quotes. */
export function cellsOf(text) {
  const rows = [];
  let row = [], field = '', quoted = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (quoted) { if (ch === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else quoted = false; } else field += ch; }
    else if (ch === '"' && field === '') quoted = true;
    else if (ch === '\t') { row.push(field); field = ''; }
    else if (ch === '\n' || ch === '\r') { if (ch === '\r' && text[i + 1] === '\n') i++; row.push(field); rows.push(row); row = []; field = ''; }
    else field += ch;
  }
  if (field !== '' || row.length) { row.push(field); rows.push(row); }
  return rows;
}

// ------------------------------------------------------------------ editing in the grid
/** After a change: the rows drawn again where they were, sorted and filtered as they were, and
 * what changed selected (`rows` by object, columns `c0…c1`). */
function after(x, st) {
  x.refresh(true);
  const want = new Set(st?.rows || []);
  if (!want.size) return;
  const ks = []; x.view.forEach((i, k) => { if (want.has(x.all[i])) ks.push(k); });
  if (!ks.length) return x.unselect();
  x.select(ks[0], st.c0 ?? 0, ks.at(-1), st.c1 ?? st.c0 ?? 0);
  x.reveal(ks[0], st.c0 ?? 0);
}
/** A cell edited in place: an input over it, its text where the cell's was; Enter keeps it (and goes
 * down), Tab goes right, Esc undoes. A long value widens it to the right. */
export function editCell(x, k, c, first) {
  const tr = x.row(k) || (x.reveal(k, c), x.row(k)), td = tr?.children[c + 1];
  if (!td) return x.box.focus({ preventScroll: true });
  const row = x.all[x.view[k]], v = row[c], was = v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v);
  const input = h('input', { class: 'celled', value: first ?? was, spellcheck: 'false', 'aria-label': `${x.cols[c].name}, row ${x.view[k] + 1}` });
  const grow = () => { input.style.width = ''; if (input.scrollWidth > input.clientWidth) input.style.width = Math.min(640, input.scrollWidth + 28) + 'px'; };
  td.classList.add('editing'); td.append(input); grow();
  input.focus({ preventScroll: true });
  if (first == null) input.select(); else input.setSelectionRange(first.length, first.length);
  let done = false;
  const finish = x.editing = Object.assign((keep, dk = 0, dc = 0) => {
    if (done) return;
    done = true; x.editing = null;
    input.remove(); td.classList.remove('editing');
    if (keep && input.value !== was) x.o.edit.set([[row, c, input.value]], { rows: [row], c0: c });
    x.redrawAll();
    x.select(k + dk, c + dc);
    x.reveal(x.sel.fr, x.sel.fc);
    x.box.focus({ preventScroll: true });
  }, { k });
  input.addEventListener('input', grow);
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter') { e.preventDefault(); finish(true, e.shiftKey ? -1 : 1); }
    else if (e.key === 'Tab') { e.preventDefault(); finish(true, 0, e.shiftKey ? -1 : 1); }
    else if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); finish(false); }
  });
  input.addEventListener('blur', () => queueMicrotask(() => finish(true))); // (not inside whatever took the focus)
}
/** A key the grid passed on (it edits): Enter or F2 edits the cell, typing starts it with that
 * key, Delete empties the selection, Ctrl+X cuts it, Ctrl+Z and Ctrl+Y (Ctrl+Shift+Z) undo and redo. */
export function editKey(x, e) {
  const key = e.key.toLowerCase();
  if (e.ctrlKey || e.metaKey) return key === 'x' ? cut(x) : history(x, key === 'y' || e.shiftKey);
  if (e.key === 'Enter' || e.key === 'F2') return editCell(x, x.sel.fr, x.sel.fc);
  if (e.key === 'Delete' || e.key === 'Backspace') return clear(x);
  editCell(x, x.sel.fr, x.sel.fc, e.key);
}
/** The cells selected emptied, as one step. */
function clear(x) {
  const { g, rows } = x.picked(), cells = [];
  for (const row of rows) for (let c = g.c0; c <= g.c1; c++) cells.push([row, c, '']);
  x.o.edit.set(cells, { rows, c0: g.c0, c1: g.c1 });
  after(x, { rows, c0: g.c0, c1: g.c1 });
}
function cut(x) { if (!x.sel) return; copyText(x.tsv(false), 'Cut: paste it where it goes'); clear(x); }
/** Undo (or, `again`, redo) a step: the rows drawn again, what it changed selected. */
export function history(x, again) {
  const ed = x.o.edit, st = again ? ed.redo() : ed.undo();
  if (!st) return toast(again ? 'Nothing to redo' : 'Nothing to undo');
  after(x, st.sel);
}
/** Cells pasted (tab-separated, as spreadsheets and the grid copy them) from the selection's first
 * cell: one value fills every cell selected; more go cell by cell, adding the rows and columns they
 * need past the last ones. One step, whatever it adds. */
export function paste(x, text) {
  const ed = x.o.edit, g = x.range() || { r0: 0, r1: 0, c0: 0, c1: 0 }, block = cellsOf(text.replace(/\r?\n$/, ''));
  if (!block.length) return;
  const one = block.length === 1 && block[0].length === 1, ht = one ? g.r1 - g.r0 + 1 : block.length, w = one ? g.c1 - g.c0 + 1 : Math.max(...block.map(b => b.length));
  const view = x.view, rows = [], sel = { rows, c0: g.c0, c1: g.c0 + w - 1 };
  ed.do(() => {
    if (g.c0 + w > x.cols.length) ed.addCols(newNames(x.cols, g.c0 + w - x.cols.length));
    const more = g.r0 + ht > view.length ? ed.addRows(g.r0 + ht - view.length) : [], cells = [];
    for (let i = 0; i < ht; i++) {
      const row = g.r0 + i < view.length ? x.all[view[g.r0 + i]] : more[g.r0 + i - view.length];
      rows.push(row);
      for (let j = 0; j < w; j++) cells.push([row, g.c0 + j, one ? block[0][0] : block[i][j] ?? '']);
    }
    ed.set(cells);
  }, sel);
  after(x, sel);
  toast(`Pasted ${count(ht * w)} cell${s(ht * w)}`);
}
/** A row added at the end, its first cell edited. */
export function addRow(x) {
  const [row] = x.o.edit.addRows(1);
  after(x, { rows: [row], c0: 0 });
  if (x.sel) editCell(x, x.sel.fr, 0);
}
/** The rows selected deleted, as one step. */
function delRows(x) { const { rows } = x.picked(); x.o.edit.del(rows); x.unselect(); x.refresh(true); }

// ------------------------------------------------------------------ menus
/** A right-click in the grid: the corner (every row), a header (its column), a row's number (its rows), a cell. */
export function context(x, e, th, p) {
  const n = x.cols.length, ed = x.o.edit;
  if (th === x.corner) { x.select(0, 0, x.view.length - 1, n - 1); return copyMenu(x, e, true); }
  if (th) return headMenu(x, e, +th.dataset.c);
  let g = x.range();
  if (p.c < 0) { if (!g || p.k < g.r0 || p.k > g.r1 || g.c0 || g.c1 < n - 1) x.select(p.k, 0, p.k, n - 1); return copyMenu(x, e); } // (a row's number: its row, or the rows selected with it)
  if (!g || p.k < g.r0 || p.k > g.r1 || p.c < g.c0 || p.c > g.c1) x.select(p.k, p.c);
  g = x.range();
  const c = x.sel.fc, name = x.cols[c].name, rows = g.r1 - g.r0 + 1;
  menu(e, [{ label: 'Copy', icon: 'copy', keys: 'Ctrl C', run: () => copyText(x.tsv(false)) }, { label: 'Copy with column names', keys: 'Ctrl Shift C', run: () => copyText(x.tsv(true), 'Copied with column names') },
    { label: 'Copy as…', run: () => menu(e, copyItems((f, hd) => copyText(x.as(f, hd)))) },
    ed ? { label: 'Cut', keys: 'Ctrl X', run: () => cut(x) } : null, ed ? { label: 'Paste', keys: 'Ctrl V', run: () => navigator.clipboard.readText().then(t => paste(x, t), () => toast('The browser keeps the clipboard here: press Ctrl V', true)) } : null, '-',
    { label: 'Filter to these values', icon: 'filter', run: () => filterTo(x, c) }, { label: `Filter ${name}…`, run: () => askFilter(x, c) }, x.filters.length ? { label: 'Clear the filters', run: () => x.setFilters([]) } : null,
    { label: 'Sort ascending', icon: 'sortUp', run: () => x.sortBy(c, 1) }, { label: 'Sort descending', icon: 'sortDown', run: () => x.sortBy(c, -1) }, '-',
    x.o.explore ? { label: `Profile ${name}`, icon: 'chart', run: () => x.o.explore(c) } : null,
    ...ed ? ['-', { label: 'Undo', icon: 'undo', keys: 'Ctrl Z', disabled: !ed.past.length, run: () => history(x) }, { label: 'Redo', icon: 'redo', keys: 'Ctrl Y', disabled: !ed.future.length, run: () => history(x, true) },
      { label: 'Add a row', icon: 'plus', run: () => addRow(x) }, { label: rows > 1 ? `Delete ${count(rows)} rows` : 'Delete the row', icon: 'trash', run: () => delRows(x) }] : []]);
}
/** The corner's menu (every row here), or a row number's (its rows), as SSMS has them: the rows copied, each way. */
function copyMenu(x, e, every) {
  const g = x.range(), n = g ? g.r1 - g.r0 + 1 : 0, r = x.r, paged = (r.total ?? x.all.length) > x.all.length;
  menu(e, [{ head: every ? `${paged ? 'This page' : 'Every row'}: ${count(n)} row${s(n)}` : `${count(n)} row${s(n)}` },
    { label: every ? 'Copy all' : 'Copy', icon: 'copy', keys: 'Ctrl C', run: () => copyText(x.tsv(false)) },
    { label: every ? 'Copy all with column names' : 'Copy with column names', keys: 'Ctrl Shift C', run: () => copyText(x.tsv(true), 'Copied with column names') },
    { label: 'Copy column names', run: () => copyText(x.as('names'), 'Column names copied') }, '-',
    ...COPIES.slice(3, -1).map(c => c === '-' ? c : { label: 'Copy as ' + c[1], run: () => copyText(x.as(c[0])) }),
    every ? '-' : null, every ? { label: 'Select all', keys: 'Ctrl A', run: () => { x.select(0, 0, x.view.length - 1, x.cols.length - 1); x.box.focus({ preventScroll: true }); } } : null,
    every && (r.sql || r.pages) ? { label: 'Download all rows', icon: 'down', run: () => menu(e, downloadItems(f => fetchRows(r, f, x.o.name))) } : null,
    !every && x.o.edit ? '-' : null, !every && x.o.edit ? { label: n > 1 ? `Delete ${count(n)} rows` : 'Delete the row', icon: 'trash', run: () => delRows(x) } : null]);
}
/** A column header's menu: its name, the names of the columns selected, sorting, filtering. */
function headMenu(x, e, c) {
  const g = x.range(), cols = x.cols, inSel = g && c >= g.c0 && c <= g.c1 && g.r0 === 0 && g.r1 === x.view.length - 1, names = inSel ? cols.slice(g.c0, g.c1 + 1).map(y => y.name) : [cols[c].name];
  if (!inSel) x.select(0, c, x.view.length - 1, c);
  const mine = x.filters.filter(f => f.c === c);
  menu(e, [{ label: `Copy name${s(names.length)}`, icon: 'copy', run: () => copyText(names.join(', ')) }, { label: 'Copy as SQL', hint: names.map(ident).join(', ').slice(0, 32), run: () => copyText(names.map(ident).join(', ')) },
    { label: 'Copy values with column name', run: () => copyText(x.tsv(true)) }, '-',
    { label: 'Sort ascending', icon: 'sortUp', run: () => x.sortBy(c, 1) }, { label: 'Sort descending', icon: 'sortDown', run: () => x.sortBy(c, -1) }, x.sort ? { label: 'Unsorted', run: () => { x.sort = null; x.order(); } } : null, '-',
    { label: mine.length ? `Change ${cols[c].name}'s filter…` : `Filter ${cols[c].name}…`, icon: 'filter', run: () => askFilter(x, c, mine[0]) }, mine.length ? { label: 'Clear its filter', run: () => x.setFilters(x.filters.filter(f => f.c !== c)) } : null,
    x.filters.length > mine.length ? { label: 'Clear every filter', run: () => x.setFilters([]) } : null, '-',
    x.o.explore ? { label: `Profile ${cols[c].name}`, icon: 'chart', run: () => x.o.explore(c) } : null]);
}
/** A filter to the values selected in a column (in place of one made so before). */
function filterTo(x, c) { const { rows } = x.picked(); x.setFilters([...x.filters.filter(f => f.c !== c || !f.values), { c, values: new Set(rows.map(row => x.text(row, c))) }]); }

/** A filter, asked for in a small window: on which column, how to compare, and with what. `now`, a
 * filter there, is changed in place; otherwise one more is added (several may be on a column). */
export function askFilter(x, c, now) {
  const cols = x.cols, list = now?.values ? [...now.values].map(v => v ?? 'NULL').join(', ') : null;
  const col = h('select', { 'aria-label': 'The column' }, cols.map((y, i) => h('option', { value: i, selected: i === c }, y.name)));
  const op = h('select', { 'aria-label': 'How to compare' }, list != null ? h('option', { value: 'in' }, 'is one of') : null, Object.entries(OPS).map(([k, [label]]) => h('option', { value: k, selected: now?.op === k }, label)));
  const val = h('input', { value: list ?? now?.v ?? '', 'aria-label': 'The value', spellcheck: 'false' }), vl = h('label', {}, 'Value');
  const hint = () => { const i = +col.value, sample = x.all.find(row => row[i] != null); val.placeholder = sample ? `for example ${x.text(sample, i).slice(0, 30)}` : 'a value'; };
  const sync = () => { val.hidden = vl.hidden = op.value !== 'in' && !OPS[op.value][1]; };
  if (!now) op.value = numeric(cols[c].type) ? 'eq' : 'has';
  col.onchange = hint; op.onchange = sync; hint(); sync();
  const made = () => {
    const i = +col.value;
    if (op.value !== 'in') return { c: i, op: op.value, v: val.value };
    return { c: i, values: val.value === list ? now.values : new Set(val.value.split(/\s*,\s*/).map(v => v === 'NULL' ? null : v)) };
  };
  const apply = () => { const f = made(); x.setFilters(now ? x.filters.map(y => y === now ? f : y) : [...x.filters, f]); };
  val.onkeydown = e => { if (e.key === 'Enter') { e.preventDefault(); d.close(); apply(); } };
  const d = pop(now ? 'Change the filter' : 'Filter the rows', h('div', { class: 'fform' }, h('label', {}, 'Column'), col, h('label', {}, 'Where it'), op, vl, val,
    h('p', { class: 'muted' }, `On the rows here; every filter applies (${x.filters.length ? `${x.filters.length} already` : 'none yet'}).`)),
  [[now ? 'Change' : 'Filter', apply, true], now ? ['Take it away', () => x.setFilters(x.filters.filter(f => f !== now))] : null, ['Cancel', () => {}]].filter(Boolean));
  requestAnimationFrame(() => (val.hidden ? op : val).focus());
}
