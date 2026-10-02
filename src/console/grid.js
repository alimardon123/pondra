// The result grid (ADR-034): query answers, notebook outputs and data files. It draws only the
// rows in sight; a cell, a range, a row or a column can be selected and copied (tab-separated,
// so a spreadsheet takes it as cells); a data file's cells are edited in place.
import { h, icon, svg, count, secs, fill, numeric, sqlType, typeKind, typeMark, menu, toast, saveAs, quote, ident, pop, call, run, prompt, esc, showTip, hideTip, S, R } from './core.js';

const ROW_H = 30; // the height of a row when only the rows in sight are drawn
let measurer;
const textWidth = s => { measurer ||= document.createElement('canvas').getContext('2d'); measurer.font = '13px ' + getComputedStyle(document.body).fontFamily; return measurer.measureText(s).width; };

/** A value as the grid, its copies and the column profile write it. */
export function shown(v, time, scale) {
  if (v == null) return null;
  let s = typeof v === 'object' ? JSON.stringify(v) : typeof v === 'number' && scale != null ? v.toFixed(scale) : String(v); // (the decimals of a live answer: numbers)
  return time ? s.replace(/^(\d{4}-\d\d-\d\d)T/, '$1 ') : s;
}
/** JSON's text with its keys, strings, numbers and true/false/null tinted (a grid's cell, its card). */
export const jsonHtml = s => esc(s).replace(/(&#34;(?:\\.|(?!&#34;).)*?&#34;)(\s*:)?|\b(true|false|null)\b|-?\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b/g,
  (m, str, key, lit) => `<span class="${str ? (key ? 'jk' : 'js') : lit ? 'jl' : 'jn'}">${str || m}</span>${key || ''}`);
/** A column holds JSON: structs, lists and maps, or text that is JSON objects or arrays (a VARIANT). */
const isJson = (c, rows, i) => /^(Struct|Map|List|LargeList|FixedSizeList)|\[\]$/.test(c.type || '') || /^(Utf8|LargeUtf8|Utf8View)$/.test(c.type || '') && (() => {
  const vs = []; for (const row of rows) { if (row[i] != null) vs.push(row[i]); if (vs.length === 20) break; }
  return vs.length > 0 && vs.every(v => typeof v === 'string' && /^\s*[[{]/.test(v) && (() => { try { JSON.parse(v); return true; } catch { return false; } })());
})();
export function toCsv(r, sep = ',') {
  const field = v => { const s = v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v); return new RegExp(`["${sep}\\n\\r]`).test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
  return [r.columns.map(c => c.name), ...r.rows].map(row => row.map(field).join(sep)).join('\r\n') + '\r\n';
}

/** A filter's ways to compare: [label, whether it takes a value]. */
const OPS = { has: ['contains', 1], starts: ['starts with', 1], not: ['does not contain', 1], eq: ['=', 1], ne: ['≠', 1], gt: ['>', 1], ge: ['≥', 1], lt: ['<', 1], le: ['≤', 1], null: ['is NULL', 0], notnull: ['is not NULL', 0] };
/** A copy's menu: the rows (the selection, else every row here) in each form, `copy(format, withNames)` making it. */
export const copyItems = copy => [{ head: 'Copy the selection, or every row here' }, ...COPIES.map(x => x === '-' ? x : { label: x[1], hint: x[3], run: () => copy(x[0], x[2]) })];
const COPIES = [['tsv', 'With column names', true, 'TSV'], ['tsv', 'Values only', false, 'TSV'], '-', ['csv', 'CSV', true], ['json', 'JSON', true], ['md', 'Markdown table', true], '-',
  ['values', 'SQL VALUES list', false, "(1, 'a'), …"], ['list', 'SQL IN list', false, '(1, 2, 3)'], ['names', 'Column names', true, 'a, b, c']];

// ------------------------------------------------------------------ copying (works without the clipboard API too)
let pending = null;
document.addEventListener('copy', e => { if (pending != null) { e.clipboardData.setData('text/plain', pending); e.preventDefault(); pending = null; } });
export function copyText(text, what = 'Copied') {
  pending = text;
  const ok = document.execCommand('copy');
  if (!ok) { pending = null; navigator.clipboard?.writeText(text); }
  toast(what);
}

// ------------------------------------------------------------------ a column's summary (the header's card, the details' profile)
export function summarize(values, type) {
  const vals = values.filter(v => v != null), distinct = new Set(vals.map(v => typeof v === 'object' ? JSON.stringify(v) : v));
  const s = { n: values.length, nulls: values.length - vals.length, distinct: distinct.size, exact: true };
  const at = spread(type);
  if (at) {
    const xs = vals.map(at).filter(Number.isFinite);
    if (xs.length) {
      let lo = 0, hi = 0;
      xs.forEach((x, i) => { if (x < xs[lo]) lo = i; if (x > xs[hi]) hi = i; });
      const [a, b] = [xs[lo], xs[hi]], hist = Array(20).fill(0);
      for (const x of xs) hist[b === a ? 0 : Math.min(19, Math.floor((x - a) / (b - a) * 20))]++;
      Object.assign(s, { min: vals[lo], max: vals[hi], hist });
    }
  } else if (vals.length) {
    const n = new Map();
    for (const v of vals) { const k = typeof v === 'object' ? JSON.stringify(v) : String(v); n.set(k, (n.get(k) || 0) + 1); }
    s.top = [...n].sort((a, b) => b[1] - a[1]).slice(0, 5).map(([v, c]) => ({ v, n: c }));
    if (typeKind(type) === 'text') { const sorted = [...n.keys()].sort(); Object.assign(s, { min: sorted[0], max: sorted.at(-1) }); }
  }
  return s;
}
/** Where a value of this type sits on a line: numbers as they are, dates and times by their instant. */
export const spread = t => numeric(t) ? v => Number(v) : /^(Date|Timestamp)/.test(t || '') ? v => Date.parse(/[zZ]|[+-]\d\d:?\d\d$/.test(v) ? v : String(v).replace(' ', 'T') + (String(v).length > 10 ? 'Z' : 'T00:00:00Z')) : null;
// ------------------------------------------------------------------ the grid
/** An answer's rows as a grid. Options:
 * - `explore(i)`: a header clicked (the column's profile in the details);
 * - `onsum(text)`: the selection's sum and average (else in the grid's own footer);
 * - `footer`: the footer with the row count and buttons (a notebook's answer);
 * - `fill`: as tall as its box (a results panel), else at most 520 px;
 * - `name`: for its CSV;
 * - `views`: more views of it under a notebook's answer, beside Chart: `[id, icon, label, title, make() → element]`;
 *   `view`, the one open to start with, and `onview(id)`, told when another opens; `chart`, where its chart's settings are kept;
 * - `edit`: a data file's `{ set(i, c, text), changed(i, c), added(i), add(), del(is), addColumn(name) }`.
 * An answer of more rows than came (`r.total`) turns its pages (`pageRows`): its rows are then those of a page. */
export function grid(r, o = {}) {
  const cols = r.columns, all = r.rows;
  if (!cols.length) return h('div', { class: 'done' }, 'No columns.');
  const nums = cols.map(c => numeric(c.type)), times = cols.map(c => /^Timestamp/.test(c.type || '')), scales = cols.map(c => { const s = +((c.type || '').match(/^Decimal\d*\(\d+,\s*(\d+)\)/)?.[1] ?? NaN); return Number.isNaN(s) ? null : s; });
  const text = (row, i) => shown(row[i], times[i], scales[i]), json = cols.map((c, i) => isJson(c, all, i));
  r.widths ||= {}; // (columns resized by dragging their edge: kept with the answer, so it is drawn again as it was)
  const multi = all.slice(0, 200).some(row => row.some(v => typeof v === 'string' && v.includes('\n')));
  const virtual = !multi && (all.length > 60 || !!o.edit);
  let view = all.map((_, i) => i), sort = null, filters = [], sel = null;
  let size = r.page ||= r.total > all.length && (r.pages || r.sql) ? all.length : 0; // (rows a page: as many as came first)
  let from = r.from || 0, turning = false;

  // the header: a type mark, the name, a sort arrow; the corner selects everything
  const heads = cols.map((c, i) => h('th', { class: nums[i] ? 'num' : null, 'data-c': i, scope: 'col' },
    h('span', { class: 'hd' }, typeMark(c.type), h('span', { class: 'hn' }, c.name), h('button', { class: 'srt', tabindex: '-1', title: 'Sort by it (again: the other way)', 'aria-label': `Sort by ${c.name}`, html: svg('sort', 13) })),
    h('small', {}, sqlType(c.type)), h('span', { class: 'rz', 'aria-hidden': 'true', title: 'Drag: its width. Double-click: to fit' })));
  const corner = h('th', { class: 'i', title: 'Select everything (Ctrl A). Right-click: copy it' }, h('span', { class: 'sr' }, 'Row'));
  const body = h('tbody'), colgroup = h('colgroup');
  const table = h('table', { class: 'gt' + (o.edit ? ' editable' : ''), role: 'grid', 'aria-rowcount': String((r.total ?? all.length) + 1) }, colgroup, h('thead', {}, h('tr', {}, corner, heads)), body);
  const box = h('div', { class: 'grid' + (virtual ? ' v' : '') + (o.fill ? ' fill' : ''), tabindex: '0', 'aria-label': 'Rows' }, table);
  const fit = (i, most = 440) => Math.min(most, Math.max(72, textWidth(cols[i].name) * 1.06 + 76, ...all.slice(0, 300).map(row => textWidth(text(row, i) ?? 'NULL') + 30)));
  const widths = () => {
    const ws = cols.map((c, i) => r.widths[i] ?? fit(i));
    const iw = Math.max(40, textWidth(count(Math.max(all.length, r.total || 0) + 5)) + 22);
    colgroup.replaceChildren(h('col', { style: `width:${iw}px` }), ...ws.map(w => h('col', { style: `width:${Math.ceil(w)}px` })));
    table.style.width = Math.ceil(iw + ws.reduce((a, b) => a + b, 0)) + 'px';
  };
  if (virtual || Object.keys(r.widths).length) { widths(); table.classList.add('sized'); }
  // a column's width, by dragging the edge of its header (a double-click fits it to what it holds)
  box.addEventListener('pointerdown', e => {
    const rz = e.target.closest('.rz');
    if (!rz) return;
    e.preventDefault(); e.stopPropagation(); hideCard();
    const c = +rz.closest('th').dataset.c, start = e.clientX;
    if (!table.classList.contains('sized')) { // (as drawn now, then fixed)
      const ths = [...table.tHead.rows[0].cells];
      colgroup.replaceChildren(...ths.map(th => h('col', { style: `width:${th.offsetWidth}px` })));
      ths.slice(1).forEach((th, i) => { r.widths[i] ??= th.offsetWidth; });
      table.style.width = ths.reduce((a, th) => a + th.offsetWidth, 0) + 'px'; table.classList.add('sized');
    }
    const was = r.widths[c] ?? fit(c);
    rz.setPointerCapture(e.pointerId); document.body.classList.add('colrz'); rz.classList.add('on');
    const move = ev => { r.widths[c] = Math.max(48, Math.round(was + ev.clientX - start)); widths(); };
    rz.addEventListener('pointermove', move);
    rz.addEventListener('pointerup', () => { rz.removeEventListener('pointermove', move); document.body.classList.remove('colrz'); rz.classList.remove('on'); }, { once: true });
  });
  box.addEventListener('dblclick', e => { const rz = e.target.closest('.rz'); if (rz) { const c = +rz.closest('th').dataset.c; r.widths[c] = fit(c, 900); widths(); table.classList.add('sized'); } });

  // rows
  const drawnRows = new Map(); // view index → its <tr>
  const cellOf = (row, k, i) => {
    const s = text(row, i), td = h('td', { 'data-c': i });
    if (s == null) td.append(h('span', { class: 'null' }, 'NULL'));
    else if (json[i]) { td.innerHTML = jsonHtml(s.length > 5000 ? s.slice(0, 5000) + '…' : s); td.classList.add('json'); }
    else td.textContent = s.length > 5000 ? s.slice(0, 5000) + '…' : s;
    if (nums[i]) td.classList.add('num');
    if (s && s.includes('\n') && !json[i]) td.classList.add('pre');
    else if (s && (s.length > 40 || json[i])) td.classList.add('long'); // (its whole value in a card, on a moment's hover)
    if (o.edit?.changed(view[k], i)) td.classList.add('chg');
    return td;
  };
  const rowOf = k => {
    const row = all[view[k]], tr = h('tr', { 'data-k': k, class: o.edit?.added(view[k]) ? 'new' : null }, h('td', { class: 'i' }, String(from + view[k] + 1)), row.map((_, i) => cellOf(row, k, i)));
    drawnRows.set(k, tr);
    return tr;
  };
  const gap = px => h('tr', { class: 'gap', 'aria-hidden': 'true' }, h('td', { colspan: cols.length + 1, style: `height:${px}px` }));
  let drawn = 0, frame = 0;
  const more = h('button', { class: 'btn small', onclick: () => draw() });
  let editing = null; // (a cell being edited: its rows stay while it is in sight; scrolled away, it is kept)
  let topGap = null, endGap = null, dFrom = 0, dTo = 0; // (the rows drawn: from dFrom to dTo, between two spacers)
  const rowsOf = (a, b) => { const f = document.createDocumentFragment(); for (let k = a; k < b; k++) f.append(rowOf(k)); return f; };
  function draw() {
    if (virtual) {
      const top = box.scrollTop, seen = box.clientHeight || 520, from = Math.max(0, Math.floor(top / ROW_H) - 12), to = Math.min(view.length, Math.ceil((top + seen) / ROW_H) + 12);
      if (editing) return editing.k >= from && editing.k < to ? undefined : editing(true);
      if (!drawnRows.size || to <= dFrom || from >= dTo || !topGap?.isConnected) { // (all of it again)
        drawnRows.clear();
        topGap = gap(from * ROW_H); endGap = gap((view.length - to) * ROW_H);
        body.replaceChildren(topGap, rowsOf(from, to), endGap);
      } else { // (scrolled: the rows that left go, those that came are added; the rest stay as they are)
        for (const [k, tr] of drawnRows) if (k < from || k >= to) { tr.remove(); drawnRows.delete(k); }
        if (from < dFrom) topGap.after(rowsOf(from, dFrom));
        if (to > dTo) endGap.before(rowsOf(dTo, to));
        topGap.firstChild.style.height = from * ROW_H + 'px'; endGap.firstChild.style.height = (view.length - to) * ROW_H + 'px';
      }
      dFrom = from; dTo = to;
      more.hidden = true;
    } else {
      const end = Math.min(view.length, drawn + (drawn ? 1000 : 200)), frag = document.createDocumentFragment();
      for (; drawn < end; drawn++) frag.append(rowOf(drawn));
      body.append(frag);
      more.textContent = `Show ${count(Math.min(1000, view.length - drawn))} more`;
      more.hidden = drawn >= view.length;
    }
    paintSel();
  }
  function redrawAll() { if (editing) editing(true); body.replaceChildren(); drawnRows.clear(); drawn = 0; draw(); said(); }
  if (virtual) {
    if (!o.fill) box.style.height = Math.min(520, 36 + Math.max(view.length, 1) * ROW_H + (o.edit ? 2 : 0)) + 'px';
    box.addEventListener('scroll', () => { if (!frame) frame = requestAnimationFrame(() => { frame = 0; draw(); }); });
    requestAnimationFrame(draw);
  }

  // selection: an anchor and a focus (view rows, columns); the focus's row is tinted
  const range = () => sel && { r0: Math.min(sel.ar, sel.fr), r1: Math.max(sel.ar, sel.fr), c0: Math.min(sel.ac, sel.fc), c1: Math.max(sel.ac, sel.fc) };
  function paintSel() {
    const g = range();
    box.classList.toggle('picked', !!sel); // (the selection shows where the keys go: no ring around it all)
    heads.forEach((th, i) => th.classList.toggle('hs', !!g && i >= g.c0 && i <= g.c1));
    for (const [k, tr] of drawnRows) {
      const inRows = !!g && k >= g.r0 && k <= g.r1;
      tr.classList.toggle('cur', !!sel && k === sel.fr);
      tr.firstChild.classList.toggle('hs', inRows);
      for (let i = 0; i < cols.length; i++) {
        const td = tr.children[i + 1], on = inRows && i >= g.c0 && i <= g.c1;
        td.classList.toggle('in', on);
        td.classList.toggle('act', !!sel && k === sel.fr && i === sel.fc);
        td.classList.toggle('et', on && k === g.r0); td.classList.toggle('eb', on && k === g.r1);
        td.classList.toggle('el', on && i === g.c0); td.classList.toggle('er', on && i === g.c1);
      }
    }
    said();
  }
  function select(ar, ac, fr = ar, fc = ac) {
    if (!view.length) return;
    const cl = (x, n) => Math.max(0, Math.min(n - 1, x));
    sel = { ar: cl(ar, view.length), ac: cl(ac, cols.length), fr: cl(fr, view.length), fc: cl(fc, cols.length) };
    paintSel();
  }
  function reveal(k, c) {
    if (virtual) {
      const y = k * ROW_H, head = 34;
      if (y < box.scrollTop) box.scrollTop = y;
      else if (y + ROW_H + head > box.scrollTop + box.clientHeight) box.scrollTop = y + ROW_H + head - box.clientHeight;
      draw();
    }
    const td = drawnRows.get(k)?.children[c + 1];
    if (td) {
      const left = td.offsetLeft - table.rows[0].cells[0].offsetWidth, right = td.offsetLeft + td.offsetWidth;
      if (left < box.scrollLeft) box.scrollLeft = left;
      else if (right > box.scrollLeft + box.clientWidth) box.scrollLeft = right - box.clientWidth;
      if (!virtual) td.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    }
  }
  const at = e => { const td = e.target.closest('td'), tr = td?.parentElement; return tr?.dataset.k == null ? null : { k: +tr.dataset.k, c: td.classList.contains('i') ? -1 : +td.dataset.c }; };
  let dragging = false;
  box.addEventListener('mousedown', e => {
    if (e.button !== 0 || e.target.closest('input.celled')) return;
    const th = e.target.closest('th');
    if (th) {
      if (e.target.closest('.srt')) { e.preventDefault(); sortBy(+th.dataset.c); return; }
      if (th === corner) { select(0, 0, view.length - 1, cols.length - 1); return; }
      const c = +th.dataset.c;
      if (e.shiftKey && sel) select(0, sel.ac, view.length - 1, c); else select(0, c, view.length - 1, c);
      o.explore?.(c);
      return;
    }
    const p = at(e);
    if (!p) return;
    if (p.c < 0) { if (e.shiftKey && sel) select(sel.ar, 0, p.k, cols.length - 1); else select(p.k, 0, p.k, cols.length - 1); return; }
    if (e.shiftKey && sel) select(sel.ar, sel.ac, p.k, p.c); else select(p.k, p.c);
    dragging = true;
  });
  box.addEventListener('mouseover', e => { if (!dragging || !sel) return; const p = at(e); if (p && p.c >= 0) select(sel.ar, sel.ac, p.k, p.c); });
  addEventListener('mouseup', () => { dragging = false; });
  box.addEventListener('dblclick', e => { const p = at(e); if (p && p.c >= 0 && o.edit) editCell(p.k, p.c); });

  // keys: arrows move (Shift extends), Ctrl+A everything, Ctrl+C copies; a data file's cells edit on Enter, F2 or typing
  box.addEventListener('keydown', e => {
    if (e.target.closest('input.celled')) return;
    const mod = e.ctrlKey || e.metaKey;
    if (mod && e.key.toLowerCase() === 'a') { e.preventDefault(); select(0, 0, view.length - 1, cols.length - 1); return; }
    if (mod && e.key.toLowerCase() === 'c') { if (sel) { e.preventDefault(); copyText(tsv(e.shiftKey), e.shiftKey ? 'Copied with column names' : 'Copied'); } return; }
    if (e.altKey && size && (e.key === 'PageDown' || e.key === 'PageUp')) { e.preventDefault(); turn(from / size + (e.key === 'PageDown' ? 1 : -1)); return; }
    const move = { ArrowUp: [-1, 0], ArrowDown: [1, 0], ArrowLeft: [0, -1], ArrowRight: [0, 1], PageUp: [-20, 0], PageDown: [20, 0], Home: [0, -1e9], End: [0, 1e9] }[e.key];
    if (move) {
      e.preventDefault();
      if (!sel) { select(0, 0); return; }
      const fr = sel.fr + move[0], fc = sel.fc + move[1];
      e.shiftKey ? select(sel.ar, sel.ac, fr, fc) : select(fr, fc);
      reveal(sel.fr, sel.fc);
      return;
    }
    if (!o.edit || !sel) return;
    if (e.key === 'Enter' || e.key === 'F2') { e.preventDefault(); editCell(sel.fr, sel.fc); }
    else if (e.key === 'Delete' || e.key === 'Backspace') { e.preventDefault(); const g = range(); for (let k = g.r0; k <= g.r1; k++) for (let c = g.c0; c <= g.c1; c++) o.edit.set(view[k], c, ''); redrawAll(); }
    else if (e.key.length === 1 && !mod && !e.altKey) { e.preventDefault(); editCell(sel.fr, sel.fc, e.key); }
  });
  box.addEventListener('contextmenu', e => {
    if (e.target.closest('th') === corner) { e.preventDefault(); select(0, 0, view.length - 1, cols.length - 1); copyMenu(e, true); return; }
    const th = e.target.closest('th[data-c]');
    if (th) { e.preventDefault(); headMenu(e, +th.dataset.c); return; }
    const p = at(e);
    if (!p) return;
    e.preventDefault();
    if (p.c < 0) { const g = range(); if (!g || p.k < g.r0 || p.k > g.r1 || g.c0 || g.c1 < cols.length - 1) select(p.k, 0, p.k, cols.length - 1); copyMenu(e); return; } // (a row's number: its row, or the rows selected with it)
    const g = range();
    if (!g || p.k < g.r0 || p.k > g.r1 || p.c < g.c0 || p.c > g.c1) select(p.k, p.c);
    const c = sel.fc, name = cols[c].name;
    menu(e, [{ label: 'Copy', icon: 'copy', keys: 'Ctrl C', run: () => copyText(tsv(false)) }, { label: 'Copy with column names', keys: 'Ctrl Shift C', run: () => copyText(tsv(true), 'Copied with column names') },
      { label: 'Copy as…', run: () => menu(e, copyItems((f, hd) => copyText(as(f, hd)))) }, '-',
      { label: 'Filter to these values', icon: 'filter', run: () => filterTo(c) }, { label: `Filter ${name}…`, run: () => askFilter(c) }, filters.length ? { label: 'Clear the filters', run: () => setFilters([]) } : null,
      { label: 'Sort ascending', icon: 'sortUp', run: () => sortBy(c, 1) }, { label: 'Sort descending', icon: 'sortDown', run: () => sortBy(c, -1) }, '-',
      o.explore ? { label: `Profile ${name}`, icon: 'chart', run: () => o.explore(c) } : null,
      o.edit ? '-' : null, o.edit ? { label: 'Delete the row' + (range().r1 > range().r0 ? 's' : ''), icon: 'trash', run: () => { const g2 = range(); o.edit.del(view.slice(g2.r0, g2.r1 + 1)); sel = null; refresh(); } } : null]);
  });
  /** The corner's menu (every row here), or a row number's (its rows), as SSMS has them: the rows copied, each way. */
  function copyMenu(e, every) {
    const g = range(), n = g ? g.r1 - g.r0 + 1 : 0, paged = (r.total ?? all.length) > all.length;
    menu(e, [{ head: every ? `${paged ? 'This page' : 'Every row'}: ${count(n)} row${n === 1 ? '' : 's'}` : `${count(n)} row${n === 1 ? '' : 's'}` },
      { label: every ? 'Copy all' : 'Copy', icon: 'copy', keys: 'Ctrl C', run: () => copyText(tsv(false)) },
      { label: every ? 'Copy all with column names' : 'Copy with column names', keys: 'Ctrl Shift C', run: () => copyText(tsv(true), 'Copied with column names') },
      { label: 'Copy column names', run: () => copyText(as('names'), 'Column names copied') }, '-',
      ...COPIES.slice(3, -1).map(x => x === '-' ? x : { label: 'Copy as ' + x[1], run: () => copyText(as(x[0])) }),
      every ? '-' : null, every ? { label: 'Select all', keys: 'Ctrl A', run: () => { select(0, 0, view.length - 1, cols.length - 1); box.focus({ preventScroll: true }); } } : null,
      every && (r.sql || r.pages) ? { label: 'Download all rows', icon: 'down', run: () => menu(e, downloadItems(f => fetchRows(r, f, o.name))) } : null]);
  }
  /** A column header's menu: its name, the names of the columns selected, sorting, filtering. */
  function headMenu(e, c) {
    clearTimeout(hover); hideCard();
    const g = range(), inSel = g && c >= g.c0 && c <= g.c1 && g.r0 === 0 && g.r1 === view.length - 1, names = inSel ? cols.slice(g.c0, g.c1 + 1).map(x => x.name) : [cols[c].name];
    if (!inSel) select(0, c, view.length - 1, c);
    menu(e, [{ label: `Copy name${names.length > 1 ? 's' : ''}`, icon: 'copy', run: () => copyText(names.join(', ')) }, { label: 'Copy as SQL', hint: names.map(ident).join(', ').slice(0, 32), run: () => copyText(names.map(ident).join(', ')) },
      { label: 'Copy values with column name', run: () => copyText(tsv(true)) }, '-',
      { label: 'Sort ascending', icon: 'sortUp', run: () => sortBy(c, 1) }, { label: 'Sort descending', icon: 'sortDown', run: () => sortBy(c, -1) }, sort ? { label: 'Unsorted', run: () => { sort = null; order(); } } : null, '-',
      { label: `Filter ${cols[c].name}…`, icon: 'filter', run: () => askFilter(c) }, filters.some(f => f.c === c) ? { label: 'Clear its filter', run: () => setFilters(filters.filter(f => f.c !== c)) } : null,
      filters.length ? { label: 'Clear every filter', run: () => setFilters([]) } : null, '-',
      o.explore ? { label: `Profile ${cols[c].name}`, icon: 'chart', run: () => o.explore(c) } : null]);
  }

  // what the selection holds, in the copies' forms
  const picked = () => { const g = range(); return { g, rows: view.slice(g.r0, g.r1 + 1).map(i => all[i]), cs: cols.slice(g.c0, g.c1 + 1).map((_, j) => g.c0 + j) }; };
  const tsv = headers => as('tsv', headers);
  /** The selection (or, with none, every row here) in one of COPIES' forms. */
  const as = (f, headers = true) => {
    const { rows, cs } = sel ? picked() : { rows: view.map(i => all[i]), cs: cols.map((_, i) => i) };
    const lit = (row, c) => row[c] == null ? 'NULL' : nums[c] ? String(row[c]) : typeof row[c] === 'boolean' ? String(row[c]).toUpperCase() : quote(text(row, c));
    const cell = (row, c) => (text(row, c) ?? '').replace(/[\t\n\r]+/g, ' ');
    switch (f) {
      case 'csv': return toCsv({ columns: cs.map(c => cols[c]), rows: rows.map(row => cs.map(c => row[c])) });
      case 'json': return JSON.stringify(rows.map(row => Object.fromEntries(cs.map(c => [cols[c].name, row[c]]))), null, 1);
      case 'md': { const esc = x => x.replace(/\|/g, '\\|'); return [cs.map(c => esc(cols[c].name)), cs.map(c => nums[c] ? '---:' : '---'), ...rows.map(row => cs.map(c => esc(cell(row, c))))].map(l => `| ${l.join(' | ')} |`).join('\n'); }
      case 'values': return 'VALUES\n  ' + rows.map(row => `(${cs.map(c => lit(row, c)).join(', ')})`).join(',\n  ');
      case 'list': { const one = cs.length === 1, seen = [...new Set(rows.map(row => cs.map(c => lit(row, c)).join(', ')))]; return '(' + seen.map(v => one ? v : `(${v})`).join(', ') + ')'; }
      case 'names': return cs.map(c => cols[c].name).join(', ');
      default: return [...(headers ? [cs.map(c => cols[c].name)] : []), ...rows.map(row => cs.map(c => cell(row, c)))].map(l => l.join('\t')).join('\n');
    }
  };
  function said() {
    const g = range();
    let msg = '';
    if (g && (g.r1 > g.r0 || g.c1 > g.c0)) {
      let sum = 0, n = 0, cells = 0, scale = 0;
      for (let k = g.r0; k <= g.r1; k++) for (let c = g.c0; c <= g.c1; c++) {
        cells++;
        const v = all[view[k]][c];
        if (nums[c] && v != null && v !== '' && Number.isFinite(+v)) { sum += +v; n++; scale = Math.max(scale, (String(v).split('.')[1] || '').length); }
      }
      const fmt = x => x.toLocaleString('en-US', { minimumFractionDigits: Math.min(scale, 6), maximumFractionDigits: Math.min(Math.max(scale, 2), 6) });
      msg = n ? `Sum ${fmt(sum)} · Avg ${fmt(sum / n)} · ${count(cells)} cells` : `${count(cells)} cells`;
    }
    if (o.onsum) o.onsum(msg); else if (sumBox) sumBox.textContent = msg;
  }

  // sorting and filtering (here, on the rows the answer holds)
  function order() {
    view = all.map((_, i) => i);
    for (const f of filters) view = view.filter(i => passes(f, all[i]));
    if (sort) {
      const i = sort.i, key = v => v == null ? null : nums[i] ? Number(v) : String(v);
      view.sort((a, b) => { const x = key(all[a][i]), y = key(all[b][i]); return x === y ? a - b : x == null ? 1 : y == null ? -1 : (x < y ? -1 : 1) * sort.dir; });
    }
    heads.forEach((th, k) => { th.classList.toggle('on', sort?.i === k); th.querySelector('.srt').innerHTML = svg(sort?.i === k ? (sort.dir === 1 ? 'sortUp' : 'sortDown') : 'sort', 13); });
    chip.hidden = !filters.length;
    chip.firstChild.textContent = filters.map(f => `${cols[f.c].name} ${f.values ? `in ${[...f.values].slice(0, 3).map(v => v ?? 'NULL').join(', ')}${f.values.size > 3 ? ` +${f.values.size - 3}` : ''}` : OPS[f.op][0] + (OPS[f.op][1] ? ' ' + f.v : '')}`).join(' · ');
    chip.firstChild.title = 'Click to change the filter';
    sel = null;
    if (virtual) box.scrollTop = 0;
    redrawAll();
  }
  function sortBy(i, dir) { sort = { i, dir: dir ?? (sort?.i === i ? (sort.dir === 1 ? -1 : 0) : 1) }; if (!sort.dir) sort = null; order(); }
  function filterTo(c) { const { rows } = picked(); setFilters([...filters.filter(f => f.c !== c), { c, values: new Set(rows.map(row => text(row, c))) }]); }
  function setFilters(fs) { filters = fs; order(); }
  const setFilter = f => setFilters(f ? [f] : []); // (the old one-filter API: extensions, tests)
  /** A test on a row, as a filter says: an operator and a value (or the values picked). */
  function passes(f, row) {
    const s = text(row, f.c);
    if (f.values) return f.values.has(s);
    if (f.op === 'null') return s == null;
    if (f.op === 'notnull') return s != null;
    if (s == null) return false;
    const a = nums[f.c] ? Number(row[f.c]) : s.toLowerCase(), b = nums[f.c] ? Number(f.v) : String(f.v).toLowerCase();
    return { eq: a === b, ne: a !== b, has: String(a).includes(b), starts: String(a).startsWith(b), gt: a > b, ge: a >= b, lt: a < b, le: a <= b, not: !String(a).includes(b) }[f.op];
  }
  /** A column's filter, asked for in a small window: how to compare, and with what. */
  function askFilter(c) {
    const now = filters.find(f => f.c === c && !f.values), sample = all.find(row => row[c] != null);
    const op = h('select', { 'aria-label': 'How to compare' }, Object.entries(OPS).map(([k, [label]]) => h('option', { value: k, selected: (now?.op || (nums[c] ? 'eq' : 'has')) === k }, label)));
    const val = h('input', { value: now?.v ?? '', placeholder: sample ? `for example ${text(sample, c).slice(0, 30)}` : 'a value', 'aria-label': 'The value', spellcheck: 'false' });
    const sync = () => { val.hidden = !OPS[op.value][1]; };
    op.onchange = sync; sync();
    const apply = () => setFilters([...filters.filter(f => f.c !== c), { c, op: op.value, v: val.value }]);
    val.onkeydown = e => { if (e.key === 'Enter') { e.preventDefault(); d.close(); apply(); } };
    const d = pop(`Filter ${cols[c].name}`, h('div', { class: 'fform' }, op, val, h('p', { class: 'muted' }, 'On the rows here; the other columns\' filters stay.')),
      [['Filter', apply, true], now ? ['Remove it', () => setFilters(filters.filter(f => f !== now))] : null, ['Cancel', () => {}]].filter(Boolean));
    requestAnimationFrame(() => (val.hidden ? op : val).focus());
  }
  const chip = h('span', { class: 'chip-f', hidden: true }, h('span', { onclick: () => askFilter(filters.at(-1)?.c ?? 0), role: 'button', tabindex: '0' }), h('button', { class: 'x', title: 'Clear the filters', 'aria-label': 'Clear the filters', html: svg('close', 12), onclick: () => setFilters([]) }));

  // editing a data file's cell: an input over it; Enter keeps (and goes down), Tab goes right, Esc undoes
  function editCell(k, c, first) {
    const tr = drawnRows.get(k) || (reveal(k, c), drawnRows.get(k));
    const td = tr?.children[c + 1];
    if (!td) return box.focus({ preventScroll: true }); // (typing then starts it)
    const i = view[k], was = all[i][c] == null ? '' : typeof all[i][c] === 'object' ? JSON.stringify(all[i][c]) : String(all[i][c]);
    const input = h('input', { class: 'celled', value: first ?? was, 'aria-label': `${cols[c].name}, row ${i + 1}` });
    td.classList.add('editing'); td.append(input);
    input.focus({ preventScroll: true });
    if (first == null) input.select(); else input.setSelectionRange(first.length, first.length);
    let done = false;
    const finish = editing = Object.assign((keep, dk = 0, dc = 0) => {
      if (done) return;
      done = true; editing = null;
      if (keep && input.value !== was) o.edit.set(i, c, input.value);
      input.remove(); td.classList.remove('editing');
      const kept = sel;
      redrawAll();
      sel = kept;
      select(k + dk, c + dc);
      reveal(sel.fr, sel.fc);
      box.focus({ preventScroll: true });
    }, { k });
    input.addEventListener('keydown', e => {
      if (e.key === 'Enter') { e.preventDefault(); finish(true, e.shiftKey ? -1 : 1); }
      else if (e.key === 'Tab') { e.preventDefault(); finish(true, 0, e.shiftKey ? -1 : 1); }
      else if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); finish(false); }
    });
    input.addEventListener('blur', () => queueMicrotask(() => finish(true))); // (not inside whatever took the focus)
  }
  /** The rows changed (added, deleted): order and draw them again. */
  function refresh(to) {
    if (virtual) { widths(); if (!o.fill) box.style.height = Math.min(520, 36 + Math.max(all.length, 1) * ROW_H + 2) + 'px'; }
    order();
    if (to != null) { const k = view.indexOf(to); if (k >= 0) { select(k, 0); reveal(k, 0); if (o.edit) editCell(k, 0); } }
  }

  // the header's card: the full type and what the rows hold
  let hover = 0, px = 0, py = 0;
  box.addEventListener('mouseover', e => {
    const th = e.target.closest('th[data-c]'), td = e.target.closest('td.long');
    clearTimeout(hover);
    if (td && !dragging) { const p = at(e); if (p) showTip(td, () => valueCard(all[view[p.k]][p.c], p.c), e.clientX); } else hideTip();
    if (!th || e.target.closest('.rz')) return;
    hover = setTimeout(() => card(th, +th.dataset.c), 450);
  });
  box.addEventListener('mouseleave', hideTip);
  /** A long value, or a JSON one, whole: JSON indented and tinted. */
  function valueCard(v, c) {
    let s = shown(v, times[c], scales[c]) ?? '';
    if (json[c]) { try { s = JSON.stringify(typeof v === 'string' ? JSON.parse(v) : v, null, 2); } catch { /* (as it is) */ } }
    const lines = s.slice(0, 6000).split('\n'), cut = lines.length > 24 || s.length > 6000;
    return [h('div', { class: 'th' }, h('b', {}, cols[c].name), sqlType(cols[c].type)),
      h('pre', { class: json[c] ? 'jv' : null, html: (json[c] ? jsonHtml : esc)(lines.slice(0, 24).join('\n')) }), cut ? h('div', { class: 'tk' }, 'More than this: copy the cell (Ctrl C) for all of it') : null];
  }
  box.addEventListener('mousemove', e => { px = e.clientX; py = e.clientY; }, { passive: true });
  box.addEventListener('mouseleave', () => { clearTimeout(hover); hideCard(); });
  box.addEventListener('mousedown', () => { clearTimeout(hover); hideCard(); });
  function card(th, c) {
    const s = summarize(all.map(row => row[c]), cols[c].type), el = cardEl();
    const v = x => { const s2 = typeof x === 'object' ? JSON.stringify(x) : String(x); return s2.length > 24 ? s2.slice(0, 23) + '…' : s2; };
    const range1 = s.min != null ? `${v(s.min)} … ${v(s.max)}` : null, t = sqlType(cols[c].type), arrow = (cols[c].type || '').toUpperCase() === t ? null : cols[c].type;
    el.replaceChildren(h('div', { class: 'cd-h' }, typeMark(cols[c].type), h('b', {}, cols[c].name), h('span', { class: 'chip', title: arrow || '' }, t)),
      h('div', { class: 'cd-f' }, [range1, `${count(s.distinct)} distinct`, s.nulls ? `${count(s.nulls)} null` : 'no nulls'].filter(Boolean).map(x => h('span', {}, x))));
    el.hidden = false;
    // (above the pointer, where it hides no rows; below it only when there is no room above)
    const b = th.getBoundingClientRect(), x = px || b.left + 12, y = py || b.top, w = el.offsetWidth, ht = el.offsetHeight;
    el.style.left = Math.min(innerWidth - w - 8, Math.max(8, x - 14)) + 'px';
    el.style.top = (y - ht - 12 >= 8 ? y - ht - 12 : y + 22) + 'px';
  }

  // its pages, at the right of the line under it: `1–500 ▾ ‹ ›` (▾: rows a page, go to a page, the first, the last)
  let pages = size ? Math.ceil(r.total / size) : 1;
  const rows1 = size ? h('button', { class: 'pg-r', 'aria-haspopup': 'menu', title: 'Rows a page, and other pages', onclick: e => pageMenu(e.currentTarget) }) : null;
  const step = (d, label, key) => h('button', { class: 'icon pg' + (d < 0 ? ' back' : ''), title: `${label} (Alt ${key})`, 'aria-label': label, onclick: () => turn(Math.floor(from / size) + d), html: svg('chev', 14, 2) });
  const prev = size && step(-1, 'The page before', 'Page Up'), next = size && step(1, 'The next page', 'Page Down');
  const pager = size ? h('span', { class: 'pages', role: 'navigation', 'aria-label': 'Pages of rows' }, rows1, prev, next) : null;
  function drawPager() {
    if (!pager) return;
    const p = Math.floor(from / size);
    fill(rows1, `${count(from + 1)}–${count(from + all.length)}`, icon('chevd', 'ic', 11));
    rows1.title = `Page ${count(p + 1)} of ${count(pages)}: rows a page, other pages`;
    prev.disabled = p <= 0 || turning; next.disabled = p >= pages - 1 || turning;
    pager.classList.toggle('busy', turning);
  }
  function pageMenu(at) {
    const p = Math.floor(from / size);
    const ask = async () => { const v = +(await prompt('Go to a page', `A page from 1 to ${count(pages)}`, String(p + 1))); if (v) turn(Math.min(pages, Math.max(1, Math.round(v))) - 1); };
    const per = n => { R.helpers.prefs?.('pageRows', n); S.pageRows = n; const first = from; size = r.page = n; pages = Math.ceil(r.total / n); turn(Math.floor(first / n), true); };
    menu(at, [{ head: `Page ${count(p + 1)} of ${count(pages)}` }, { label: 'Go to a page…', run: ask }, { label: 'The first page', disabled: p === 0, run: () => turn(0) },
      { label: 'The last page', disabled: p === pages - 1, run: () => turn(pages - 1) }, { head: 'Rows a page' }, ...PER_PAGE.map(n => ({ label: count(n), checked: n === size, run: () => per(n) }))]);
  }
  /** Show page `p`: its rows in place of these (sorted and filtered as these were). */
  async function turn(p, again) {
    if (!size || turning || p < 0 || p >= pages || p * size === from && !again) return;
    turning = true; drawPager();
    try {
      const rows = await pageRows(r, p * size, size);
      all.length = 0; for (const x of rows) all.push(x);
      from = r.from = p * size;
      if (virtual) { box.scrollTop = 0; widths(); }
      order(); o.onpage?.();
    } catch (e) { toast(e.message, true); }
    turning = false; drawPager();
  }
  drawPager();

  // the line under it, the same for a SQL file's answer and a cell's: how many rows and how long (a
  // cell's time is beside its Run), the filters, the selection's sum; the pages at the right
  const sumBox = o.onsum ? null : h('span', { class: 'sum' }), total = r.total ?? all.length;
  const what = `${count(total)} row${total === 1 ? '' : 's'}${!size && r.total > all.length ? ` (${count(all.length)} here)` : ''}${r.ms != null && !o.footer ? ' · ' + secs(r.ms) : ''}`;
  const foot = h('div', { class: 'gfoot' + (o.onsum ? ' own' : '') }, o.onsum ? null : h('span', { class: 'n-rows' }, what), chip, sumBox, more, h('span', { class: 'grow' }), pager); // (own: a data file's, its count and sum in its own footer)
  const wrap = h('div', { class: 'gridwrap' + (o.fill ? ' fill' : '') });
  if (o.footer) {
    // (a cell's answer: its views as a SQL file's pane has them, tabs above it, one at a time: the rows,
    // Chart, Data profile, and those given (a SQL cell's Plan); Copy and Download at the right)
    const views = [['results', 'Results'], ['chart', 'Chart', 'chart', () => import('./chart.js').then(m => m.chartView(r, o.name, o.chart))],
      ['profile', 'Data profile', 'columns', () => import('./details.js').then(m => m.dataProfile(r))], ...o.views || []];
    const tabs = h('div', { class: 'ptabs', role: 'tablist' }), body = h('div', { class: 'aview' });
    let open = 'results';
    const show = (id, quiet) => {
      open = views.some(v => v[0] === id) ? id : 'results';
      tabs.replaceChildren(...views.map(([k, label, ic]) => h('button', { class: 'ptab' + (k === open ? ' on' : ''), role: 'tab', 'aria-selected': String(k === open), onclick: () => show(k) }, ic ? icon(ic) : null, label)));
      if (pager) pager.hidden = open !== 'results';
      if (!quiet) o.onview?.(open === 'results' ? null : open);
      const v = views.find(x => x[0] === open);
      if (!v[3]) return body.replaceChildren(box);
      body.replaceChildren(h('div', { class: 'wait' }, 'Drawing…'));
      Promise.resolve(v[3]()).then(el => { if (open === v[0]) body.replaceChildren(el); });
    };
    wrap.append(h('div', { class: 'abar' }, tabs, h('span', { class: 'grow' }),
      split('copy', 'Copy with column names (tab-separated)', () => copyText(tsv(true), 'Copied with column names'), () => copyItems((f, hd) => copyText(as(f, hd), 'Copied'))),
      split('down', 'Download the rows here as CSV', () => saveAs(toCsv(r), 'text/csv', `${o.name || 'rows'}.csv`), () => [{ head: 'The rows here' },
        { label: 'CSV', hint: '.csv', run: () => saveAs(toCsv(r), 'text/csv', `${o.name || 'rows'}.csv`) }, { label: 'TSV', hint: '.tsv', run: () => saveAs(toCsv(r, '\t'), 'text/tab-separated-values', `${o.name || 'rows'}.tsv`) },
        { label: 'JSON', hint: '.json', run: () => saveAs(JSON.stringify(all.map(row => Object.fromEntries(cols.map((c, i) => [c.name, row[i]])))), 'application/json', `${o.name || 'rows'}.json`) },
        ...r.sql || r.pages ? ['-', ...downloadItems(f => fetchRows(r, f, o.name))] : []])), body, foot);
    show(o.view, true);
  } else wrap.append(box, foot);
  draw();
  wrap.grid = { refresh, select, sortBy, setFilter, setFilters, copy: tsv, text: as, box, count: () => view.length, selection: () => sel && { ...range(), row: view[sel.fr], col: sel.fc } };
  return wrap;
}
let cardBox;
const cardEl = () => cardBox ||= document.body.appendChild(h('div', { class: 'hcard', role: 'tooltip', hidden: true }));
const hideCard = () => { if (cardBox) cardBox.hidden = true; };
/** The rows a page an answer may come in (Settings, and the ▾ under a paged answer). */
export const PER_PAGE = [100, 500, 1000, 5000, 10000, 50000];
addEventListener('scroll', hideCard, true);

/** Rows `at…at + n` of an answer: from the node, which keeps a big answer a while (`pages.rs`); once
 * it doesn't, its statement run again for them (without an ORDER BY, rows may then come in another order). */
async function pageRows(r, at, n) {
  if (r.pages) {
    try { return (await (await call(`/sql/pages/${r.pages}?from=${at}&rows=${n}`)).json()).rows; } catch (e) { if (e.status !== 410 || !r.sql) throw e; r.pages = null; }
  }
  if (!r.sql) throw new Error('Its rows are no longer kept on the node: run it again');
  return (await run(`SELECT * FROM (\n${r.sql.replace(/[\s;]+$/, '')}\n) AS q LIMIT ${n} OFFSET ${at}`, undefined, r.params, n)).rows;
}

/** A SQL `IN` list's values quoted as a name (for the menus of other modules). */
export const names = cs => cs.map(c => ident(c.name)).join(', ');

// ------------------------------------------------------------------ copies and downloads: a button and its forms
/** A button and its ▾: the usual way on a click, the others in its menu (Copy, Download). */
export const split = (ic, title, run, items, disabled) => h('span', { class: 'split' }, h('button', { class: 'icon', title, 'aria-label': title, disabled, onclick: run }, icon(ic)),
  h('button', { class: 'icon caret', title: 'Other forms', 'aria-label': 'Other forms', 'aria-haspopup': 'menu', disabled, onclick: e => menu(e.currentTarget, items()) }, icon('chevd', 'ic', 12)));
/** A download's menu: every row, in each format `get(format)` asks the node for. */
export const downloadItems = (get, head = 'All rows') => [{ head }, ...DOWNLOADS.map(([f, label, ext]) => ({ label, hint: ext, run: () => get(f) }))];
const DOWNLOADS = [['csv', 'CSV', '.csv'], ['tsv', 'TSV', '.tsv'], ['json', 'JSON', '.json'], ['ndjson', 'JSON Lines', '.jsonl'], ['parquet', 'Parquet', '.parquet'], ['xlsx', 'Excel', '.xlsx']];
/** Every row of an answer, in a file to download: the answer as the node keeps it (`pages.rs`), or,
 * once it doesn't (or for a table), its statement run again there, in that format. */
export async function fetchRows(r, f, name) {
  toast('Preparing the download…');
  const keep = (res, rerun) => res.blob().then(b => { saveAs(b, res.headers.get('content-type') || 'application/octet-stream', `${name || 'rows'}.${f === 'ndjson' ? 'jsonl' : f}`); if (rerun) toast('Its rows were no longer kept on the node: it ran again for the download'); });
  if (r.pages) {
    try { return await keep(await call(`/sql/pages/${r.pages}?format=${f}`)); } catch (e) { if (e.status !== 410 || !r.sql) return toast('Not downloaded: ' + e.message, true); r.pages = null; }
  }
  try {
    const body = r.params && Object.keys(r.params).length ? JSON.stringify({ sql: r.sql, params: r.params }) : r.sql;
    const res = await call('/sql?format=' + f, { method: 'POST', body, headers: { 'content-type': r.params && Object.keys(r.params).length ? 'application/json' : 'text/plain; charset=utf-8' } });
    await keep(res, r.pages === null);
  } catch (e) { toast('Not downloaded: ' + e.message, true); }
}
