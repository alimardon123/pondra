// A SQL file's statements and their answers (round 31), loaded once a file has run several: each
// answer's number beside its statement in the file, with the lines of the one shown tinted, and the
// Messages tab (what each statement printed and did).
import { h, svg, secs, tip, moreStyle } from './core.js';
import { LINE_H } from './editor.js';
import { oneLine, said } from './files.js';
import { doneText } from './notebook.js';

await moreStyle();

/** Each answer's number beside its first line in the file (while the text there is still its
 * statement): the one shown filled, with its lines tinted (or the one `hover`ed), the others that
 * ran lightly tinted, so they stand apart from the line numbers. Fewer than two: none. */
export function marks(doc, hover) {
  const ed = doc.ed, all = doc.results?.length > 1 ? doc.results : [], v = ed.value, on = hover || all.indexOf(doc.result) + 1;
  if (!ed.nums) return;
  ed.marks ??= ed.nums.appendChild(h('div', { class: 'smarks' }));
  ed.band ??= ed.body.insertBefore(h('div', { class: 'sband' }), ed.pre);
  const line = at => v.slice(0, at).split('\n').length, top = l => `top:${9 + (l - 1) * LINE_H}px`;
  ed.band.hidden = true;
  ed.marks.replaceChildren(...all.filter(x => x.at && v.slice(...x.at) === x.sql).map(x => {
    // (its first line of code: past the blank lines and comments before it)
    const n = all.indexOf(x) + 1, a = line(x.at[0]) + v.slice(x.at[0]).match(/^(?:\s|--[^\n]*)*/)[0].split('\n').length - 1;
    if (n === on) { ed.band.hidden = false; ed.band.style.cssText = `${top(a)};height:${(line(x.at[1]) - a + 1) * LINE_H}px`; }
    return tip(h('button', { class: 'smark' + (n === on ? ' on' : '') + (x.kind === 'error' ? ' bad' : ''), style: top(a), 'aria-label': `Answer ${n}: ${said(x)}`, tabindex: -1,
      onclick: () => { if (doc.result !== x) { doc.result = x; doc.draw(); } } }, String(n)), () => doc.card(x, 'Click: show its answer'));
  }));
}

/** A SQL file's Messages: each statement it ran (the one shown marked), what it printed and what it
 * did. Its text can be selected and copied; a double-click (or Enter) shows its answer and goes to
 * its lines in the file. */
export function messages(doc, r) {
  const all = doc.results?.length ? doc.results : [r];
  const open = x => { doc.result = x; doc.tab = x.kind === 'rows' ? 'results' : x.kind === 'plan' ? 'plan' : 'messages'; doc.draw(); doc.point(x); };
  return h('div', { class: 'msgs' }, all.map((x, k) => {
    const did = x.kind === 'done' ? doneText(x.value) : null, printed = x.notices?.length ? x.notices.join('\n') : null;
    const head = tip(h('div', { class: 'msg-h', tabindex: '0', ondblclick: () => open(x), onkeydown: e => { if (e.key === 'Enter') { e.preventDefault(); open(x); } } },
      h('b', {}, String(k + 1)), h('span', { class: 'ic', html: svg(x.kind === 'error' ? 'close' : 'check', 13) }), h('code', {}, oneLine(x.sql, 160)),
      h('span', { class: 'm' }, said(x), x.ms != null ? ' · ' + secs(x.ms) : '')), () => doc.card(x, 'Double-click: its answer, and its lines in the file'));
    return h('div', { class: 'msg' + (x.kind === 'error' ? ' bad' : '') + (x === r ? ' on' : '') }, head,
      printed ? h('div', { class: 'out' }, h('div', { class: 'cap' }, 'Printed'), h('pre', {}, printed)) : null,
      did ? h('div', { class: 'did' }, did) : null,
      x.kind === 'error' ? h('pre', { class: 'err' }, x.message) : null);
  }), doc.results.length < doc.todo && !doc.running ? h('div', { class: 'stmts-left' }, `${doc.todo - doc.results.length} after it not run`) : null);
}
