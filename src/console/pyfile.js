// A Python file (ADR-034): the editor, and a console below it where the file runs in the page's
// Python, as a notebook's cells do. Loaded when a Python file first opens, not with the page.
import { h, icon, secs, S, R, emit, run, doBlock, failed, interruptPython, formatPython, moreStyle } from './core.js';
import { answer } from './notebook.js';
import { TextDoc, splitPanel, moreBtn } from './files.js';

await moreStyle();

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
    this.ed.menu = some => ['-', { label: some ? 'Run selection' : 'Run file', icon: 'play', keys: 'Ctrl Enter', run: () => this.run() }, ...this.ed.formats(formatPython, 'file', true), '-', ...this.jobs(false)];
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
    try { r = await run(doBlock(code), ctl.signal, undefined, S.pageRows); } catch (e) { r = failed(e, 'Stopped waiting.'); }
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
      R.helpers.runButton(this.running, { label: 'Run file', title: 'Run the file, or what is selected, in the page\'s Python (Ctrl+Enter)', run: () => this.run(), stop: () => interruptPython(), stopTitle: 'Interrupt it (its variables stay)' }, () => [
        { label: 'Run selection', icon: 'play', keys: this.ed.selected() ? 'Ctrl Enter' : null, disabled: !this.ed.selected(), run: () => this.run() },
        { label: 'Run file', keys: this.ed.selected() ? null : 'Ctrl Enter', run: () => { this.ed.ta.setSelectionRange(0, 0); this.run(); } }, '-',
        ...this.ed.formats(formatPython, 'file', true), '-', ...this.jobs(false, false)]),
      h('span', { class: 'sep' }), pill, R.helpers.saveButton(this), moreBtn(() => this.more())];
  }
  status() { return [`Ln ${this.pos.line}, Col ${this.pos.col}`, `Python · ${S.py === 'none' ? 'not started' : S.py}`, 'Spaces: 4']; }
}
