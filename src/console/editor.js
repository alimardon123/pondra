// The code editor (ADR-034): a textarea over its highlighted copy, for notebook cells and files.
// It re-renders only the lines a key changed, so typing stays fast in long files; no library.
import { h, esc, S, ident, sqlType, SQL_KW, menu, toast } from './core.js';

// ------------------------------------------------------------------ highlighting
const PY_KW = new Set('False None True and as assert async await break class continue def del elif else except finally for from global if import in is lambda nonlocal not or pass raise return try while with yield match case'.split(' '));
const SQL_TOKEN = /--.*|\/\*|'|"|\$\w+|\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b|[A-Za-z_][\w$]*/g;
const PY_TOKEN = /#.*|[rRbBfFuU]{0,2}(?:"""|'''|"|')|\b\d[\d_]*(?:\.\d+)?(?:[eE][+-]?\d+)?\b|[A-Za-z_]\w*/g;
// (Markdown: headings, list marks and quotes, code, emphasis, links and pictures; `md.js` draws it)
const MD_TOKEN = /^#{1,6}\s.*$|^\s*(?:[-*+]|\d+[.)])(?=\s)|^\s*>|^\s*(?:```|~~~).*$|`[^`]*`|\*\*[^*]+\*\*|(?<![\w*])[*_][^*_\s][^*_]*[*_](?![\w*])|~~[^~]+~~|!?\[[^\]]*\]\([^)]*\)/g;
const MD_CLS = { '#': 'k', '`': 's', '~': 'f', '*': 'f', _: 'f', '[': 'nu', '!': 'nu' };
const span = (cls, t) => !t ? '' : cls ? `<span class="${cls}">${esc(t)}</span>` : esc(t);
/** Where a quoted run ends, looking from `from`: after its closing `q` (a doubled quote, or with
 * `bs` one after a backslash, stays inside), or -1 when the line ends first. */
function closing(s, from, q, bs) {
  for (let j = from; ;) {
    const e = s.indexOf(q, j);
    if (e < 0) return -1;
    let k = e, n = 0;
    while (bs && k > from && s[k - 1] === '\\') { n++; k--; }
    if (bs && n % 2) { j = e + 1; continue; }
    if (!bs && q.length === 1 && s[e + 1] === q) { j = e + 2; continue; }
    return e + q.length;
  }
}
const word = (s, m, kw, up) => /\d/.test(m[0][0]) ? span('nu', m[0]) : kw.has(up ? m[0].toUpperCase() : m[0]) ? span('k', m[0])
  : /^\s*\(/.test(s.slice(m.index + m[0].length, m.index + m[0].length + 3)) ? span('f', m[0]) : esc(m[0]);
/** One line highlighted, from the state the line before left it in (inside a comment or a quote
 * that goes on): its HTML and the state it leaves. So a key highlights its line, not the file. */
const LINE = {
  sql(s, st) {
    let out = '', i = 0;
    // (the state is what opened the run: '/*', a quote; never a word, which could be `c`)
    const end = (q, from) => { if (q !== '/*') return closing(s, from, q, false); const e = s.indexOf('*/', from); return e < 0 ? -1 : e + 2; };
    const cls = q => q === '/*' ? 'c' : q === "'" ? 's' : '';
    if (st) { const e = end(st, 0); if (e < 0) return [span(cls(st), s), st]; out = span(cls(st), s.slice(0, e)); i = e; }
    SQL_TOKEN.lastIndex = i;
    for (let m; (m = SQL_TOKEN.exec(s));) {
      out += esc(s.slice(i, m.index));
      const t = m[0], q = t;
      if (q === '/*' || q === "'" || q === '"') {
        const e = end(q, m.index + t.length);
        if (e < 0) return [out + span(cls(q), s.slice(m.index)), q];
        out += span(cls(q), s.slice(m.index, e)); SQL_TOKEN.lastIndex = e;
      } else out += t[0] === '-' ? span('c', t) : t[0] === '$' ? span('nu', t) : word(s, m, SQL_KW, true);
      i = SQL_TOKEN.lastIndex;
    }
    return [out + esc(s.slice(i)), ''];
  },
  python(s, st) {
    let out = '', i = 0;
    if (st) { const e = closing(s, 0, st, true); if (e < 0) return [span('s', s), st]; out = span('s', s.slice(0, e)); i = e; }
    PY_TOKEN.lastIndex = i;
    for (let m; (m = PY_TOKEN.exec(s));) {
      out += esc(s.slice(i, m.index));
      const t = m[0];
      if (t[0] === '#') out += span('c', t);
      else if (/['"]$/.test(t)) {
        const q = t.replace(/^[rRbBfFuU]*/, ''), e = closing(s, m.index + t.length, q, true);
        if (e < 0) return [out + span('s', s.slice(m.index)), q.length === 3 ? q : '']; // (a short string ends with its line)
        out += span('s', s.slice(m.index, e)); PY_TOKEN.lastIndex = e;
      } else out += word(s, m, PY_KW, false);
      i = PY_TOKEN.lastIndex;
    }
    return [out + esc(s.slice(i)), ''];
  },
  markdown(s) {
    let out = '', i = 0;
    MD_TOKEN.lastIndex = 0;
    for (let m; (m = MD_TOKEN.exec(s));) { const t = m[0].trimStart(); out += esc(s.slice(i, m.index)) + span(/^([-*+>]|\d+[.)])$/.test(t) ? 'c' : /^(```|~~~)/.test(t) ? 's' : MD_CLS[t[0]] || 'c', m[0]); i = MD_TOKEN.lastIndex; }
    return [out + esc(s.slice(i)), ''];
  },
  text: s => [esc(s), ''],
};
const lineOf = lang => LINE[lang] || LINE.sql;

/** The text, highlighted, one HTML string per line. */
export function highlight(text, lang) {
  const f = lineOf(lang);
  let st = '';
  return text.split('\n').map(l => { const [html, next] = f(l, st); st = next; return html; });
}
/** Whole highlighted HTML (a view's definition, read-only). */
export const highlighted = (text, lang) => highlight(text, lang).join('\n');

// ------------------------------------------------------------------ the editor
export const LINE_H = 21; // (px: --code-lh in console.css)
let charW = 0;
export const measure = () => { if (!charW) { const c = document.createElement('canvas').getContext('2d'); c.font = `13px ${getComputedStyle(document.body).getPropertyValue('--mono')}`; charW = c.measureText('0').width || 7.8; } return charW; };

/** Code with its highlighting: `new Editor({ language, value, gutter, grow, placeholder })`.
 * `grow`: as tall as its lines (a notebook cell); else it fills its box and scrolls (a file).
 * Events: `oninput`, `onkey(e) → true if handled`, `oncursor(line, col)`. */
export class Editor {
  constructor(o = {}) {
    const { value = '', label = 'Code', ...rest } = o;
    Object.assign(this, { language: 'sql', gutter: false, grow: false, placeholder: '', oninput: null, onkey: null, oncursor: null, menu: null }, rest);
    this.src = []; this.html = []; this.states = ['']; this.width = 0; // (each line: its text, its HTML, the state it starts in; and the state the last leaves)
    this.pre = h('pre', { class: 'hl', 'aria-hidden': 'true' });
    this.ta = h('textarea', { spellcheck: 'false', autocapitalize: 'off', autocomplete: 'off', 'aria-label': label, wrap: 'off', placeholder: this.placeholder });
    this.cur = h('div', { class: 'curline', 'aria-hidden': 'true' });
    this.nums = this.gutter ? h('pre', { class: 'nums', 'aria-hidden': 'true' }, this.numText = document.createTextNode('')) : null;
    this.body = h('div', { class: 'code' }, this.cur, this.pre, this.ta);
    this.el = h('div', { class: 'editor' + (this.grow ? ' fit' : ' fill') }, this.nums, this.body);
    this.ta.value = value;
    this.ta.addEventListener('input', () => { this.paint(); this.oninput?.(); this.reveal(); if (cm?.ed === this) complete(this); });
    this.ta.addEventListener('keydown', e => keys(e, this));
    for (const ev of ['keyup', 'mouseup', 'focus']) this.ta.addEventListener(ev, () => this.cursor());
    this.ta.addEventListener('blur', () => { if (cm?.ed === this) closeComplete(); });
    this.ta.addEventListener('scroll', () => { this.ta.scrollTop = 0; this.ta.scrollLeft = 0; }); // (the box scrolls, never the textarea: it is as big as its text)
    this.ta.addEventListener('contextmenu', e => { e.preventDefault(); this.contextMenu(e); });
    this.ta.addEventListener('mousemove', e => this.onhover?.(e));
    this.paint();
  }
  /** Its right-click menu, as an application's: cut, copy, paste, select all, comment, and what its
   * document adds (`menu`: running, formatting). */
  contextMenu(at) {
    const ta = this.ta, some = ta.selectionStart !== ta.selectionEnd, mod = navigator.platform?.startsWith('Mac') ? 'Cmd' : 'Ctrl';
    const exec = cmd => { ta.focus(); document.execCommand(cmd); };
    menu(at, [{ label: 'Cut', keys: `${mod} X`, disabled: !some, run: () => exec('cut') }, { label: 'Copy', icon: 'copy', keys: `${mod} C`, disabled: !some, run: () => exec('copy') },
      { label: 'Paste', keys: `${mod} V`, run: () => navigator.clipboard?.readText ? navigator.clipboard.readText().then(t => { ta.focus(); insert(ta, t); }, () => toast(`${mod}+V pastes here`)) : toast(`${mod}+V pastes here`) },
      { label: 'Select all', keys: `${mod} A`, run: () => { ta.focus(); ta.select(); } }, '-',
      this.language !== 'markdown' ? { label: 'Comment the lines, or uncomment them', keys: `${mod} /`, run: () => { ta.focus(); comment(this); } } : null,
      ...this.menu?.(some) || []]);
  }
  /** Replace what is selected (or, with nothing selected or `whole`, all of it) by `f` of it (`f`
   * may answer later: the node's Python formats Python); if `f` fails, changes nothing, or the text
   * changed meanwhile, it stays as it was. Indented lines are formatted as if they weren't, then
   * indented back; a last new line stays as it was. */
  async reformat(f, whole) {
    const ta = this.ta, some = !whole && ta.selectionStart !== ta.selectionEnd, [a0, b0] = some ? [ta.selectionStart, ta.selectionEnd] : [0, ta.value.length], caret = ta.selectionStart;
    const before = ta.value, text = before.slice(a0, b0), pad = text.match(/^[ \t]*(?=\S)/gm)?.reduce((a, b) => b.length < a.length ? b : a) || '';
    let out;
    try { out = await f(pad ? text.replace(new RegExp('^' + pad, 'gm'), '') : text); }
    catch (e) { toast(`It can't be formatted as it is: left as it was${e?.message ? ` (${e.message.split('\n')[0].slice(0, 200)})` : ''}`, true); return; }
    if (out == null) return;
    if (pad) out = out.replace(/^(?=.)/gm, pad);
    out = /\n$/.test(text) ? out.replace(/\n*$/, '\n') : out.replace(/\n+$/, '');
    if (out === text) return;
    if (ta.value !== before) return toast('It changed while it was being formatted: left as it was', true);
    ta.focus(); ta.setSelectionRange(a0, b0);
    insert(ta, out);
    if (some) ta.setSelectionRange(a0, a0 + out.length); else { ta.setSelectionRange(Math.min(caret, out.length), Math.min(caret, out.length)); this.reveal(); } // (the whole: the caret about where it was)
  }
  /** A menu's Format items: the whole `what`'s, and the selection's (Shift+Alt+F formats the selection when there is one, else the whole). */
  formats(f, what = 'file', always) {
    const some = !!this.selected();
    return [{ label: `Format ${what}`, icon: 'format', keys: some ? null : 'Shift Alt F', run: () => this.reformat(f, true) }, some || always ? { label: 'Format selection', keys: some ? 'Shift Alt F' : null, disabled: !some, run: () => this.reformat(f) } : null];
  }
  get value() { return this.ta.value; }
  set value(v) { this.ta.value = v; this.paint(); }
  setLanguage(l) { this.language = l; this.src = []; this.html = []; this.states = ['']; this.pre.replaceChildren(); this.paint(); }
  /** Highlight again the lines that changed (and those after them whose state they changed);
   * size the box to the text: its width in steps, as a new width lays the whole file out again. */
  paint() {
    const lines = this.ta.value.split('\n'), old = this.src, n = lines.length, on = old.length, rows = this.pre.children, f = lineOf(this.language);
    let a = 0, b = 0;
    while (a < n && a < on && lines[a] === old[a]) a++;
    if (a === n && n === on) return;
    while (b < n - a && b < on - a && lines[n - 1 - b] === old[on - 1 - b]) b++;
    const shift = n - on, html = [], states = [];
    let i = a, st = this.states[a] ?? '';
    for (; i < n; i++) {
      if (i >= n - b && st === this.states[i - shift]) break; // (the rest is as it was, shift lines on)
      const [line, next] = f(lines[i], st);
      html.push(line); states.push(st); st = next;
    }
    const gone = i - shift - a; // (the old lines [a, a + gone) give way to these)
    for (let k = 0; k < Math.min(gone, html.length); k++) if (this.html[a + k] !== html[k]) rows[a + k].innerHTML = html[k] || ' ';
    for (let k = html.length; k < gone; k++) rows[a + html.length].remove();
    if (html.length > gone) {
      const more = document.createDocumentFragment();
      for (const x of html.slice(gone)) more.append(h('div', { html: x || ' ' }));
      this.pre.insertBefore(more, rows[a + gone] || null);
    }
    this.html.splice(a, gone, ...html); this.states.splice(a, gone, ...states);
    if (i === n) this.states[n] = st;
    this.src = lines;
    if (n !== on) {
      if (this.nums) this.numText.data = Array.from({ length: n }, (_, k) => k + 1).join('\n');
      if (this.grow) this.body.style.height = (n * LINE_H + 18) + 'px';
    }
    let longest = 0;
    for (const l of lines) if (l.length > longest) longest = l.length;
    const width = Math.ceil((longest * measure() + 40) / 240) * 240;
    if (width !== this.width) { this.width = width; this.body.style.minWidth = width + 'px'; }
  }
  /** The caret's line and column (1-based), and the current line's band. */
  cursor() {
    const v = this.ta.value, a = this.ta.selectionStart, line = v.slice(0, a).split('\n').length, col = a - v.lastIndexOf('\n', a - 1);
    this.cur.style.transform = `translateY(${(line - 1) * LINE_H}px)`;
    this.oncursor?.(line, col);
    return { line, col };
  }
  /** Keep the caret in sight (the box scrolls, the textarea never does). */
  reveal() {
    const box = this.el, { line, col } = this.cursor();
    const y = 9 + (line - 1) * LINE_H, x = (this.nums?.offsetWidth || 0) + 14 + (col - 1) * measure();
    if (!this.grow) {
      if (y < box.scrollTop) box.scrollTop = y - 4;
      else if (y + LINE_H > box.scrollTop + box.clientHeight) box.scrollTop = y + LINE_H - box.clientHeight + 10;
    }
    if (x < box.scrollLeft + (this.nums?.offsetWidth || 0)) box.scrollLeft = Math.max(0, x - 40);
    else if (x > box.scrollLeft + box.clientWidth - 20) box.scrollLeft = x - box.clientWidth + 60;
  }
  focus() { this.ta.focus({ preventScroll: true }); }
  /** The selected text, or '' when nothing is. */
  selected() { const { selectionStart: a, selectionEnd: b } = this.ta; return a === b ? '' : this.ta.value.slice(a, b); }
  insert(s) { insert(this.ta, s); }
}

// ------------------------------------------------------------------ keys in the editor
export function insert(ta, s) {
  ta.focus({ preventScroll: true });
  if (document.execCommand('insertText', false, s)) return; // (so Ctrl+Z undoes it)
  ta.setRangeText(s, ta.selectionStart, ta.selectionEnd, 'end');
  ta.dispatchEvent(new Event('input'));
}
function indent(ta, back) {
  const v = ta.value, a = ta.selectionStart, b = ta.selectionEnd;
  if (!back && a === b) return insert(ta, '    ');
  const start = v.lastIndexOf('\n', a - 1) + 1, end = b > a && v[b - 1] === '\n' ? b - 1 : b;
  const stop = v.indexOf('\n', end) < 0 ? v.length : v.indexOf('\n', end);
  const lines = v.slice(start, stop).split('\n').map(l => back ? l.replace(/^( {1,4}|\t)/, '') : '    ' + l).join('\n');
  ta.setSelectionRange(start, stop); insert(ta, lines); ta.setSelectionRange(start, start + lines.length);
}
function comment(ed) {
  const ta = ed.ta, v = ta.value, a = ta.selectionStart, b = ta.selectionEnd, mark = ed.language === 'python' ? '#' : '--';
  const start = v.lastIndexOf('\n', a - 1) + 1, stop = v.indexOf('\n', b) < 0 ? v.length : v.indexOf('\n', b);
  const lines = v.slice(start, stop).split('\n'), off = lines.every(l => !l.trim() || l.trimStart().startsWith(mark));
  const out = lines.map(l => !l.trim() ? l : off ? l.replace(new RegExp(`^(\\s*)${mark === '#' ? '#' : '--'} ?`), '$1') : l.replace(/^(\s*)/, `$1${mark} `)).join('\n');
  ta.setSelectionRange(start, stop); insert(ta, out); ta.setSelectionRange(start, start + out.length);
}
function keys(e, ed) {
  const ta = ed.ta, mod = e.ctrlKey || e.metaKey;
  if (cm?.ed === ed) {
    const n = cm.list.length, go = { ArrowDown: 1, ArrowUp: -1 }[e.key];
    if (go) { e.preventDefault(); cm.on = (cm.on + go + n) % n; drawComplete(); return; }
    if (e.key === 'Enter' || e.key === 'Tab') { e.preventDefault(); accept(); return; }
    if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); closeComplete(); return; }
  }
  if (ed.onkey?.(e)) return;
  if (e.key === ' ' && e.ctrlKey) { e.preventDefault(); complete(ed, true); return; }
  if (e.key === '/' && mod) { e.preventDefault(); comment(ed); return; }
  if (e.key === 'Tab' && !mod && !e.altKey && !e.shiftKey && ta.selectionStart === ta.selectionEnd && /[\w.$"]$/.test(ta.value.slice(0, ta.selectionStart)) && complete(ed)) { e.preventDefault(); if (cm.list.length === 1) accept(); return; }
  if (e.key === 'Tab' && !mod && !e.altKey) { e.preventDefault(); indent(ta, e.shiftKey); return; }
  if (e.key === 'Backspace' && !mod && ta.selectionStart === ta.selectionEnd) {
    const v = ta.value, a = ta.selectionStart, before = v.slice(v.lastIndexOf('\n', a - 1) + 1, a);
    if (before.length && /^ +$/.test(before)) { e.preventDefault(); ta.setSelectionRange(a - ((before.length - 1) % 4 + 1), a); insert(ta, ''); }
    return;
  }
  if (e.key === 'Enter' && !mod && !e.shiftKey && !e.altKey && !e.isComposing) {
    const v = ta.value, a = ta.selectionStart, line = v.slice(v.lastIndexOf('\n', a - 1) + 1, a);
    let ind = line.match(/^[ \t]*/)[0];
    if (ed.language === 'python' && /:\s*(#.*)?$/.test(line)) ind += '    ';
    if (ind) { e.preventDefault(); insert(ta, '\n' + ind); }
  }
}

// ------------------------------------------------------------------ completion
const FUNCS = 'abs avg ceil coalesce concat count date_bin date_part date_trunc extract floor greatest least length lower ltrim max min now nullif regexp_replace replace round row_number rank dense_rank lag lead first_value last_value split_part stddev strpos substr sum to_char to_date to_timestamp trim upper approx_distinct approx_percentile_cont median array_agg string_agg json_get json_get_str cosine_distance read_parquet read_csv read_json files file_read range generate_series'.split(' ');
let cm = null; // (the completion open now: its editor, where the word starts, the choices, the one on)
/** Names that complete the word before the caret: the lake's tables and columns (those of the
 * tables the text names first), SQL's words and functions; in Python, the page's variables. */
export function complete(ed, force) {
  const ta = ed.ta, at = ta.selectionStart, before = ta.value.slice(0, at), m = before.match(/[\w.$"]*$/), word = m[0].replace(/"/g, '');
  if (!word && !force) return false;
  if (ed.language === 'sql' && word.includes('.')) { // (after `o.`, `sales.orders.`, `sales.`: that table's columns, or that schema's tables)
    const dot = word.lastIndexOf('.'), q = word.slice(0, dot).toLowerCase(), rest = word.slice(dot + 1), objs = S.objects || [];
    const named = n => objs.filter(t => [t.t, `${t.s}.${t.t}`, t.q].some(x => x.toLowerCase() === n.toLowerCase()));
    let tables = named(q);
    if (!tables.length && /^\w+$/.test(q)) for (const a of ta.value.matchAll(new RegExp(`([\\w.]+)\\s+(?:as\\s+)?${q}\\b`, 'gi'))) tables.push(...named(a[1]));
    const list = tables.length ? [...new Set(tables.flatMap(t => t.columns.map(c => ident(c.n) + '\t' + sqlType(c.d))))].map(x => x.split('\t'))
      : objs.filter(t => t.s.toLowerCase() === q || t.c?.toLowerCase() === q).map(t => [ident(t.t), t.o.kind]);
    const fit = list.filter(([n]) => n.toLowerCase().startsWith(rest.toLowerCase()) && n.toLowerCase() !== rest.toLowerCase()).map(([text, ty]) => ({ text, ty }));
    if (fit.length) { cm = { ed, from: at - rest.length, list: fit.slice(0, 50), on: 0 }; drawComplete(); return true; }
  }
  const low = word.toLowerCase(), seen = new Set(), all = [];
  const push = (text, ty, rank) => { if (!seen.has(text) && text.toLowerCase().startsWith(low) && text.toLowerCase() !== low) { seen.add(text); all.push({ text, ty, rank }); } };
  if (ed.language === 'sql') {
    if (word[0] === '$') for (const n of [...ta.value.matchAll(/\$([A-Za-z_]\w*)/g)].map(m => m[1]).concat(S.sqlVars || [])) push('$' + n, 'variable', 0);
    const named = (S.objects || []).filter(t => new RegExp(`\\b${t.t.replace(/[^\w]/g, '')}\\b`, 'i').test(ta.value));
    for (const t of named) for (const col of t.columns) push(ident(col.n), sqlType(col.d), 0);
    for (const t of S.objects || []) push(t.q, t.o.kind, 1);
    for (const t of S.objects || []) for (const col of t.columns) push(ident(col.n), sqlType(col.d), 2);
    for (const f of FUNCS) push(f + '(', 'function', 3);
    for (const k of SQL_KW) push(/[a-z]/.test(word) ? k.toLowerCase() : k, '', 4);
  } else if (ed.language === 'python') {
    for (const v of S.vars || []) push(v.name, v.type, 0);
    for (const x of ['db.sql(', 'db.table(', 'db.tables()', 'db.insert(', 'pondra.col(', 'print(']) push(x, '', 1);
    for (const k of PY_KW) push(k, '', 2);
  } else return false;
  all.sort((a, b) => a.rank - b.rank || a.text.length - b.text.length || a.text.localeCompare(b.text));
  if (!all.length) { closeComplete(); return false; }
  cm = { ed, from: at - m[0].length, list: all.slice(0, 50), on: 0 };
  drawComplete();
  return true;
}
function drawComplete() {
  const box = document.getElementById('complete'), { ed, list, on, from } = cm, ta = ed.ta;
  box.replaceChildren(...list.map((x, i) => h('div', { class: i === on ? 'on' : null, role: 'option', 'aria-selected': String(i === on), onmousedown: e => { e.preventDefault(); cm.on = i; accept(); } }, h('span', {}, x.text), x.ty ? h('span', { class: 'ty' }, x.ty) : null)));
  const before = ta.value.slice(0, from), row = before.split('\n').length - 1, col = before.length - before.lastIndexOf('\n') - 1;
  const r = ed.body.getBoundingClientRect(), x = r.left + 14 + col * measure(), y = r.top + 9 + (row + 1) * LINE_H;
  box.hidden = false;
  box.style.left = Math.min(innerWidth - box.offsetWidth - 8, x) + 'px';
  box.style.top = (y + box.offsetHeight > innerHeight - 8 ? y - LINE_H - box.offsetHeight : y + 2) + 'px';
  box.children[on]?.scrollIntoView({ block: 'nearest' });
}
function accept() {
  const { ed, from, list, on } = cm, ta = ed.ta;
  ta.setSelectionRange(from, ta.selectionStart);
  insert(ta, list[on].text);
  closeComplete();
}
export function closeComplete() { cm = null; const b = document.getElementById('complete'); if (b) b.hidden = true; }

// ------------------------------------------------------------------ SQL formatting
const CLAUSE = /^(SELECT|FROM|WHERE|GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION|EXCEPT|INTERSECT|WINDOW|QUALIFY|VALUES|SET|RETURNING|(LEFT|RIGHT|FULL|INNER|CROSS|NATURAL)|JOIN)$/;
/** SQL tidied: its words in capitals, and each clause on a line of its own at the top level.
 * Strings, quoted names and comments are left exactly as written. */
export function formatSql(text) {
  let out = '', depth = 0, last = 0, prev = '';
  for (const m of text.matchAll(/(--[^\n]*|\/\*[\s\S]*?\*\/)|('(?:[^']|'')*')|("(?:[^"]|"")*")|([A-Za-z_][\w$]*)|([(),;])/g)) {
    out += text.slice(last, m.index);
    last = m.index + m[0].length;
    let t = m[0];
    if (m[4]) {
      const up = t.toUpperCase();
      if (SQL_KW.has(up)) {
        t = up;
        const joinAfterSide = up === 'JOIN' && /^(LEFT|RIGHT|FULL|INNER|CROSS|NATURAL|OUTER)$/.test(prev);
        if (!depth && CLAUSE.test(up) && !joinAfterSide && out.trim()) out = out.replace(/[ \t]*\n?[ \t]*$/, '') + '\n';
      }
      prev = up;
    } else if (m[5] === '(') depth++;
    else if (m[5] === ')') depth = Math.max(0, depth - 1);
    else if (m[5] === ';') { depth = 0; t = ';\n'; }
    out += t;
  }
  return (out + text.slice(last)).replace(/\n{3,}/g, '\n\n').replace(/[ \t]+\n/g, '\n').trim() + '\n';
}
