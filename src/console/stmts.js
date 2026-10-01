// A SQL file's statements and their answers (round 31), loaded once a file has run several: each
// answer's number beside its statement in the file, with the lines of the one shown tinted, and the
// Messages tab (what each statement printed and did).
import { h, svg, secs, moreStyle } from './core.js';
import { LINE_H } from './editor.js';
import { oneLine, said } from './files.js';
import { doneText } from './notebook.js';

await moreStyle();

/** Each answer's number beside its first line in the file (while the text there is still its
 * statement); the one shown, or `hover`ed, with its lines tinted. Fewer than two: none. */
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
    return h('button', { class: 'smark' + (n === on ? ' on' : '') + (x.kind === 'error' ? ' bad' : ''), style: top(a), title: `Answer ${n}: ${said(x)}`, tabindex: -1, onclick: () => { doc.result = x; doc.tab = 'results'; doc.draw(); } }, String(n));
  }));
}

/** A SQL file's Messages: each statement it ran, what it printed and did; a click shows its answer
 * and selects it in the file. */
export function messages(doc, r) {
  const all = doc.results?.length ? doc.results : [r], told = x => [...x.notices || [], x.kind === 'done' ? doneText(x.value) : x.kind === 'error' ? x.message : null].filter(Boolean);
  return h('div', { class: 'msgs' }, all.map((x, k) => h('div', { class: 'msg' + (x.kind === 'error' ? ' bad' : '') + (x === r ? ' on' : '') },
    h('button', { class: 'msg-h', title: x.sql + '\n\n(click: its answer, and the statement selected in the file)', onclick: () => { doc.result = x; doc.tab = x.kind === 'rows' ? 'results' : 'messages'; doc.draw(); doc.point(x); } },
      h('b', {}, String(k + 1)), h('span', { class: 'ic', html: svg(x.kind === 'error' ? 'close' : 'check', 13) }), h('code', {}, oneLine(x.sql, 160)),
      h('span', { class: 'm' }, said(x), ' · ', secs(x.ms))),
    told(x).length ? h('pre', { class: x.kind === 'error' ? 'err' : 'said' }, told(x).join('\n')) : null)),
    doc.results.length < doc.todo && !doc.running ? h('div', { class: 'stmts-left' }, `${doc.todo - doc.results.length} after it not run`) : null);
}
