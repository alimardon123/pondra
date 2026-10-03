// A SQL file (ADR-034): the editor, and its answers below it (or at its right): Results, Messages,
// Chart, Data profile and Plan, an answer a statement. Loaded when a SQL file first opens, not with
// the page.
import { h, fill, icon, secs, S, R, emit, run, menu, MODE, failed, tip, sessionOf, moreStyle, toast } from './core.js';
import { formatSql, highlighted } from './editor.js';
import { grid, copyText, copyItems, split, downloadItems, fetchRows } from './grid.js';
import { answer } from './notebook.js';
import { TextDoc, splitPanel, btn, moreBtn, statements as pieces, lastStatement, oneLine, said } from './files.js';

moreStyle(); // (its strip's links, the parameters bar: more.css)

/** A script's statements, each block (BEGIN … END, IF … END IF, a loop) one, as the node joins them
 * (script.rs `depth`: a block's word counts where a statement starts; every CASE and END counts). */
export function statements(text, raw) {
  const out = [], depth = s => {
    const t = s.replace(/'(?:[^']|'')*'|"[^"]*"|--.*|\/\*[\s\S]*?\*\/|\$(\w*)\$[\s\S]*?\$\1\$/g, ' x ').toLowerCase().match(/\$?\w+|[^\s\w]/g) || [];
    let d = 0;
    const task = t[0] == 'create' && (t[1] == 'task' || t[3] == 'task'); // (CREATE TASK … AS BEGIN …)
    const close = i => { let n = 0; return t.findIndex((x, j) => j > i && !(n += (x == '(') - (x == ')'))); }; // (an IF's condition's end: `if(` before anything but THEN is the function)
    t.forEach((w, i) => {
      const b = t[i - 1], at = !i || /^(then|else|do|loop|repeat|begin|async)$/.test(b) || b == ':' && /^\w/.test(t[i - 2]) && t[i - 3] != ':' || task && b == 'as';
      d += w == 'end' ? -1 : b == 'end' ? 0 : +(w == 'case' || at && (/^(while|loop)$/.test(w) || w == 'repeat' && t[i + 1] != '(' || w == 'if' && (!i || t[i + 1] != '(' || t[close(i) + 1] == 'then') || w == 'for' && /^(in|as)$/.test(t[i + 2]) || w == 'begin' && !!t[i + 1] && !/^(transaction|work|isolation|read|deferrable|not)$/.test(t[i + 1])));
    });
    return d;
  };
  let open = null, d = 0;
  for (const p of pieces(text, true)) { d = Math.max(0, d + depth(p)); const w = open == null ? p : open + ';' + p; if (d) open = w; else { out.push(w); open = null; } }
  if (open != null) out.push(open);
  return raw ? out : out.map(x => x.trim());
}

export class SqlDoc extends TextDoc {
  constructor(o = {}) {
    super({ ...o, kind: 'sql', language: 'sql', untitled: o.untitled || 'queries/untitled.sql' });
    this.layout = R.helpers.prefs('results') || 'below';
    this.tab = 'results'; this.result = null;
    this.body = h('div', { class: 'pbody' });
    this.tabs = h('div', { class: 'ptabs', role: 'tablist' });
    this.info = h('div', { class: 'pinfo' });
    this.liveSt = h('span', { class: 'n t livest' });
    this.panel = h('section', { class: 'panel results', 'aria-label': 'Results' }, h('div', { class: 'phead' }, this.tabs, h('span', { class: 'grow' }), this.info), this.body);
    this.pbar = h('div', { class: 'params', role: 'group', 'aria-label': 'Parameters', hidden: true });
    this.main.prepend(this.pbar);
    splitPanel(this, this.panel);
    this.ed.top = some => [...this.runItems(some), ...this.explainItems()];
    this.ed.menu = () => [...this.ed.formats(formatSql, 'file', true), '-', ...this.jobs(true)];
    this.ed.onhover = e => /\$[A-Za-z_]/.test(this.ed.value) && import('./params.js').then(m => m.hover(this, e));
    this.draw(); this.paramsBar();
  }
  /** The statement the caret is in (just after its `;` too), and where it starts. */
  statementAt() {
    const v = this.ed.value, at = this.ed.ta.selectionStart;
    let seek = 0, last = ['', 0];
    for (const q of statements(v)) { const i = v.indexOf(q, seek); if (i < 0) continue; seek = i + q.length; last = [q, i]; if (at <= seek + 1) break; }
    return last;
  }
  /** What is selected, or the statement the caret is in: what Explain and Create as… take. */
  current() { return this.ed.selected() || this.statementAt()[0]; }
  /** The execution plans of the statements selected, the one the caret is in, or (`all`) the
   * file's, without running them (or, `profile`, run with EXPLAIN ANALYZE: each step's rows and
   * time): the Plan tab shows them, one each, as answers are. */
  explain(profile, all) {
    const [whole, from] = all ? [this.ed.value, 0] : this.ed.selected() ? [this.ed.selected(), this.ed.ta.selectionStart] : this.statementAt(), list = statements(whole);
    this.results = this.where(list, whole, from).map((at, i) => Object.assign(R.helpers.explain(list[i], this.params(), profile, sessionOf(this)), { at, sql: list[i] }));
    this.result = this.results[0]; this.todo = list.length; this.tab = 'plan'; this.draw(); R.helpers.pane('bottom', true);
  }
  /** Where each statement is in the file (to point at it), from `whole` found at `from`. */
  where(list, whole, from) { let seek = 0; return list.map(q => { const at = whole.indexOf(q, seek); seek = at < 0 ? seek : at + q.length; return at < 0 ? null : [from + at, from + at + q.length]; }); }
  /** Run: what is selected, else the statement at the caret; Run file: all of it. Ctrl+Enter runs
   * the one Settings name (the statement, unless they say the file), Ctrl+Shift+Enter the other. */
  runItems(some) {
    const file = R.helpers.prefs('enter') === 'file';
    return [{ label: some ? 'Run selection' : 'Run', icon: 'play', keys: some || !file ? 'Ctrl Enter' : 'Ctrl Shift Enter', run: () => this.run(some ? undefined : 'statement') },
      { label: 'Run file', icon: 'playall', keys: file && !some ? 'Ctrl Enter' : 'Ctrl Shift Enter', run: () => this.run('file') }];
  }
  explainItems() { return [...R.helpers.planItems(profile => this.explain(profile)), statements(this.ed.value).length > 1 ? { label: 'Show every statement\'s plan', run: () => this.explain(false, true) } : null]; }
  /** Format the SQL selected (or all of it): its words in capitals, a clause a line. */
  format() { this.ed.reformat(formatSql); }
  key(e) {
    if (e.shiftKey && e.altKey && e.key.toLowerCase() === 'f') { e.preventDefault(); this.format(); return true; }
    if (e.shiftKey && (e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'e') { e.preventDefault(); this.explain(); return true; }
    if (e.shiftKey && (e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); this.run(R.helpers.prefs('enter') === 'file' ? 'statement' : 'file'); return true; }
    return super.key(e);
  }
  get hasPanel() { return true; }
  changed() { super.changed(); clearTimeout(this.pt); this.pt = setTimeout(() => { this.paramsBar(); this.marks(); }, 250); }
  /** The file's parameters, an input each above the editor (params.js, loaded once it says `$name`). */
  paramsBar() { if (/\$[A-Za-z_]/.test(this.ed.value) || !this.pbar.hidden) import('./params.js').then(m => m.bar(this)); }
  /** The values its runs send for its parameters (as the bar has them). */
  params() { return this.given || {}; }
  /** Run what is selected, else the statement the caret is in or the file (`what`, else as Settings
   * say Ctrl+Enter does: the statement): each statement its own answer (in order, stopping at a
   * failure), or, as Settings may say, all of it at once, the last one's answer. */
  async run(what) {
    const sel = what ? '' : this.ed.selected(), [whole, from] = sel ? [sel, this.ed.ta.selectionStart] : (what || R.helpers.prefs('enter') || 'statement') === 'statement' ? this.statementAt() : [this.ed.value, 0], text = whole.trim();
    if (!text) return;
    this.ctl?.abort();
    const ctl = this.ctl = new AbortController(), each = (R.helpers.prefs('statements') || 'each') === 'each';
    const list = each ? statements(text) : [text];
    const places = this.where(list, whole, from);
    if (this.tab === 'plan' && this.result?.kind === 'plan') this.tab = 'results'; // (the plans were asked for, not the rows: now the rows)
    this.unwatch?.(); this.unwatch = this.watched = null;
    this.running = true; this.plan = null; this.results = []; this.result = null; this.todo = list.length; R.helpers.toolbar();
    this.body.replaceChildren(h('div', { class: 'wait pulse' }, 'Running…'));
    emit('run', { kind: 'sql', src: text, doc: this });
    let r;
    const values = this.params();
    for (const sql of list.length ? list : [text]) {
      const t0 = performance.now();
      try { r = await run(sql, ctl.signal, values, S.pageRows, sessionOf(this)); } catch (e) { r = failed(e); }
      if (this.ctl !== ctl) return;
      r.ms = performance.now() - t0; r.sql = sql; r.at = places[this.results.length]; r.params = values;
      this.results.push(r); this.result = r;
      if (list.length > 1 && this.tab === 'messages' && r.kind === 'rows') this.tab = 'results';
      this.draw();
      if (r.kind === 'error') break;
    }
    this.ctl = null; this.running = false;
    const shown = this.result = r.kind === 'done' ? this.results.findLast(x => x.kind === 'rows') || r : r; // (the last rows, after a DECLARE or an INSERT)
    if (shown.kind === 'done' && this.tab !== 'messages') this.tab = 'messages';
    if (shown.kind === 'rows' && this.tab === 'messages') this.tab = 'results';
    R.helpers.toolbar(); R.helpers.pane('bottom', true);
    this.draw();
    emit('ran', { kind: 'sql', src: text, doc: this }, r, { kind: 'sql', src: text });
    return r;
  }
  close() { const ok = super.close(); if (ok) this.setLive(false, true); return ok; }
  /** Live (as DataGrip's grid has it): the answer shown again, in place, each time a commit changes
   * what it reads; every live query of the page shares one connection to the node (`live.js`). */
  setLive(on, quiet) { this.live = on; this.unwatch?.(); this.unwatch = this.watched = null; this.liveSt.textContent = ''; if (!quiet) this.draw(); }
  follow(r) {
    this.unwatch?.(); this.unwatch = null; this.watched = r;
    if (!this.live || !liveable(r)) return;
    this.liveSt.innerHTML = '<span class="dot"></span>watching';
    import('./live.js').then(L => { if (this.watched === r && !this.unwatch) this.unwatch = L.watch(r.sql, sessionOf(this), a => {
      if (a.error) { toast('Live stopped: ' + a.error, true); return this.setLive(false); }
      const got = L.rowsOf(a, r.columns), g = this.gridEl?.grid;
      if (JSON.stringify(got.rows) !== JSON.stringify(r.rows)) {
        if (this.result === r && this.tab === 'results' && g && !r.page) g.update(got.rows, got.total);
        else { Object.assign(r, got, { page: 0, pages: null }); if (this.result === r) this.draw(); }
      }
      this.liveSt.innerHTML = `<span class="dot"></span>updated ${new Date().toLocaleTimeString()}`;
    }, r.params); });
  }
  stop() { this.ctl?.abort(); this.ctl = null; this.running = false; this.body.replaceChildren(h('div', { class: 'wait' }, 'Stopped waiting. (A statement already on its way may still finish on the node.)')); R.helpers.toolbar(); }
  /** Point at a statement that ran (a double-click on its number): the caret at its start, in sight,
   * if the text there is still it (not selected: Run would then run it alone). A click only shows its
   * answer, so the caret stays where the work is. */
  point(x) { const [a, b] = x.at || []; if (a != null && this.ed.value.slice(a, b) === x.sql) { this.ed.focus(); this.ed.ta.setSelectionRange(a, a); this.ed.reveal(); } }
  /** Each answer's number beside its statement in the file (while the text there is still it), the
   * one shown (or `hover`ed) with its lines tinted. */
  marks(hover) { if (this.results?.length > 1 || this.ed.marks) import('./stmts.js').then(m => m.marks(this, hover)); }
  /** A statement's card (its number in the strip, the gutter, Messages): what it did, its text. */
  card(x, hint) {
    const lines = x.sql.split('\n'), text = lines.length > 14 ? lines.slice(0, 13).join('\n') + '\n…' : x.sql;
    return [h('div', { class: 'th' }, h('b', {}, `Statement ${this.results.indexOf(x) + 1}`), said(x) + (x.ms != null ? ' · ' + secs(x.ms) : '')), h('pre', { html: highlighted(text, 'sql') }), hint ? h('div', { class: 'tk' }, hint) : null];
  }
  /** The panel: Results (the grid), Messages (each statement: what it printed and did), Chart, Data
   * profile (each column's NULLs, distinct values, range and spread) and Plan (a graph of EXPLAIN,
   * and its query profile: the time each step took). */
  draw() {
    // (Results, Chart, Data profile and Plan show the statements that answered: rows, a failure, a plan; Messages every one)
    const answers = (this.results || []).filter(x => x.kind !== 'done'), open = id => { this.tab = id; if (id !== 'messages' && this.result?.kind === 'done') this.result = answers.at(-1) || this.result; this.draw(); };
    const r = this.result, tab = (id, label, ic) => h('button', { class: 'ptab' + (this.tab === id ? ' on' : ''), role: 'tab', 'aria-label': label, 'aria-selected': String(this.tab === id), onclick: () => open(id) }, ic ? icon(ic) : null, h('span', { class: 'tl' }, label));
    this.marks();
    const plan = r?.kind === 'plan'; // (a plan alone: nothing ran, so no rows to show, chart or profile)
    if (plan && this.tab !== 'messages') this.tab = 'plan';
    this.tabs.replaceChildren(...[plan ? null : tab('results', 'Results'), tab('messages', 'Messages'), plan ? null : tab('chart', 'Chart', 'chart'), plan ? null : tab('profile', 'Data profile', 'columns'), tab('plan', 'Plan', 'plan')].filter(Boolean));
    const name = this.title.replace(/\.sql$/i, ''), rowsOk = r?.kind === 'rows';
    if (this.live && this.watched !== r && !this.running) this.follow(r);
    const sw = h('input', { type: 'checkbox', checked: !!this.live, onchange: () => this.setLive(sw.checked) });
    const text = (f, headers) => this.gridEl?.grid ? this.gridEl.grid.text(f, headers) : '';
    // (how many rows and how long: on the line under the rows, as a cell's answer has it)
    fill(this.info, r && !rowsOk && r.ms != null ? h('span', { class: 'n t' }, secs(r.ms)) : null,
      this.live ? this.liveSt : null, this.live || liveable(r) ? h('label', { class: 'live', title: 'Live: this answer again, in place, each time a commit changes what it reads' }, sw, h('span', { class: 'switch' }), 'Live') : null,
      split('copy', 'Copy with column names (tab-separated)', () => rowsOk && copyText(text('tsv', true), 'Copied with column names'), () => copyItems((f, hd) => rowsOk && copyText(text(f, hd), 'Copied')), !rowsOk),
      split('down', 'Download all rows as CSV', () => rowsOk && fetchRows(r, 'csv', name), () => downloadItems(f => fetchRows(r, f, name)), !rowsOk),
      h('button', { class: 'icon', title: this.layout === 'right' ? 'Move the results below' : 'Move the results to the right', 'aria-label': 'Move the results', onclick: () => { this.layout = this.layout === 'right' ? 'below' : 'right'; R.helpers.prefs('results', this.layout); this.place(); this.draw(); } }, icon(this.layout === 'right' ? 'panelBelow' : 'panelRight')));
    const keys = s => s.split(' ').map(x => h('kbd', {}, x)), file = R.helpers.prefs('enter') === 'file';
    if (!r) { this.body.replaceChildren(h('div', { class: 'wait' }, this.running ? 'Running…' : h('span', {}, 'Run the statement at the caret, or what is selected: ', ...keys(file ? 'Ctrl Shift Enter' : 'Ctrl Enter'), '. The whole file: ', ...keys(file ? 'Ctrl Enter' : 'Ctrl Shift Enter'), '.'))); return; }
    const done = (this.results?.length || 0) - answers.length, many = this.tab !== 'messages' && (answers.length > 1 || done > 0 && answers.length > 0 || this.running && this.todo > 1);
    // (which statement's answer: its number in the file; what the others did is in Messages)
    const strip = many ? h('div', { class: 'stmts', role: 'group', 'aria-label': 'The statements\' answers, numbered as in the file' }, answers.map(x => {
      const k = this.results.indexOf(x);
      return tip(h('button', { class: 'stmt' + (x === r ? ' on' : '') + (x.kind === 'error' ? ' bad' : ''), 'aria-pressed': String(x === r), 'aria-description': oneLine(x.sql, 200),
        onclick: () => { if (this.result !== x) { this.result = x; this.draw(); } }, ondblclick: () => this.point(x), onmouseenter: () => this.marks(k + 1), onmouseleave: () => this.marks() },
        h('b', {}, String(k + 1)), h('span', { class: 'm' }, said(x))), () => this.card(x, 'Double-click: go to its lines in the file'));
    }),
      this.running ? h('span', { class: 'stmt wait pulse' }, h('b', {}, String(this.results.length + 1)), `of ${this.todo}…`)
        : this.results.length < this.todo ? h('span', { class: 'stmts-left' }, `${this.todo - this.results.length} after it not run`) : null,
      done && !this.running ? h('button', { class: 'stmts-left more', title: 'What each statement did: Messages', onclick: () => open('messages') }, `${done} more in Messages`) : null) : null;
    const show = (...els) => fill(this.body, strip, ...els), later = (tab, el) => { if (this.tab === tab && this.result === r) show(el); };
    if (this.tab === 'results') {
      if (r.kind === 'rows') { this.gridEl = grid(r, { fill: true, name, explore: i => R.helpers.explore(r, i) }); show(this.gridEl); }
      else show(...answer(r), r.kind === 'done' ? h('div', { class: 'wait' }, 'No rows to show: ', h('button', { class: 'linkb', onclick: () => open('messages') }, 'Messages'), ' says what each statement did.') : null);
    } else if (this.tab === 'messages') {
      import('./stmts.js').then(m => later('messages', m.messages(this, r))); // (each statement: what it printed and did)
    } else if (this.tab === 'profile') {
      if (r.kind !== 'rows') return show(h('div', { class: 'wait' }, 'A data profile needs rows.'));
      import('./details.js').then(m => later('profile', m.dataProfile(r)));
    } else if (this.tab === 'chart') {
      if (r.kind !== 'rows') return show(h('div', { class: 'wait' }, 'A chart needs rows.'));
      show(h('div', { class: 'wait' }, 'Drawing…'));
      import('./chart.js').then(m => later('chart', m.chartView(r, name, this.chartKeep ||= {})));
    } else {
      show(h('div', { class: 'wait' }, 'Reading the plan…'));
      import('./plan.js').then(m => later('plan', m.planView(lastStatement(r.sql), r.params, { profile: r.profile, session: sessionOf(this) })));
    }
  }
  toolbar() {
    const db = h('button', { class: 'btn dbpick', title: MODE === 'lakes' ? 'The database the file runs in: pick another, or copy its name' : 'The lake the file runs in, and those attached', 'aria-haspopup': 'menu', onclick: e => R.helpers.pickDb(e.currentTarget) }, icon('db'), (MODE === 'lakes' ? S.db : S.lake) || '…', icon('chevd'));
    const some = () => !!this.ed.selected();
    return [...this.crumbs(), h('span', { class: 'grow' }),
      R.helpers.runButton(this.running, { label: 'Run', title: 'Run the statement at the caret, or what is selected (Ctrl+Enter)', run: () => this.run(some() ? undefined : 'statement'), all: [() => this.run('file'), 'Run every statement of the file, in order (Ctrl+Shift+Enter)'], stop: () => this.stop(), stopTitle: 'Stop waiting for it' }, () => [
        ...this.runItems(some()), ...this.explainItems(), '-', ...this.ed.formats(formatSql, 'file', true), '-', ...this.jobs(true, false)]),
      h('span', { class: 'sep' }), db, R.helpers.saveButton(this), moreBtn(() => this.more())];
  }
}
/** An answer that can be followed live: the rows of one query. */
const liveable = r => r?.kind === 'rows' && !!r.sql && /^\s*(select|with|from|values|table)\b/i.test(r.sql.replace(/--[^\n]*|\/\*[\s\S]*?\*\//g, ' '));
