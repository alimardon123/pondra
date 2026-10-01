// A file's versions (ADR-035 §8): every save is kept; one is shown against the file as it is now,
// line by line, and restored. Loaded when first asked for (a file's ⋯ › Versions…).
import { h, icon, ago, bytes, call, fileUrl, toast, pop, S, R, moreStyle } from './core.js';

await moreStyle();

const TEXT = /\.(sql|py|md|txt|ipynb|json|jsonl|ndjson|csv|tsv|yaml|yml|toml|js|css|html|xml)$/i;

/** The versions of `doc`'s file (a tab, or a file of the Workspace), in a dialog. */
export async function open(doc) {
  const rel = doc.kind === 'notebook' && !doc.plain ? `notebooks/${doc.name}.ipynb` : doc.path || doc.rel;
  let list;
  try { list = await (await call(fileUrl(rel) + '?versions')).json(); } catch (e) { toast(e.message, true); return; }
  const now = await call(fileUrl(rel)).then(r => r.text(), () => null); // (deleted: nothing to compare with)
  const text = TEXT.test(rel), view = h('div', { class: 'vdiff' }), rows = h('div', { class: 'vlist', role: 'listbox', 'aria-label': 'Versions' });
  let on = null;
  const show = async v => {
    on = v;
    for (const r of rows.children) r.classList.toggle('on', r.dataset.id === v.id);
    if (!text) { view.replaceChildren(h('p', { class: 'muted' }, `${bytes(v.bytes)}: not text, so not compared.`)); return; }
    const was = await call(fileUrl(rel) + '?version=' + encodeURIComponent(v.id)).then(r => r.text());
    if (on === v) view.replaceChildren(...diff(was, now ?? ''));
  };
  rows.append(...list.map((v, i) => h('div', { class: 'row', role: 'option', tabindex: '0', 'data-id': v.id, onclick: () => show(v), onkeydown: e => e.key === 'Enter' && show(v) },
    icon('clock'), h('span', { class: 'nm' }, new Date(v.at).toLocaleString()), h('span', { class: 'meta' }, [v.who, i === 0 && now != null ? 'as it is now' : ago(new Date(v.at).toISOString())].filter(Boolean).join(' · ')))));
  if (!list.length) rows.append(h('div', { class: 'empty' }, 'None kept yet: each save from now on is.'));
  const restore = async () => {
    if (!on) return;
    const open = S.docs.find(d => d.path === rel || (d.kind === 'notebook' && `notebooks/${d.name}.ipynb` === rel));
    if (open?.dirty && !confirm(`${open.title} has changes that are not saved. Restore the version anyway (they go)?`)) return;
    try {
      await call(fileUrl(rel) + '?restore=' + encodeURIComponent(on.id), { method: 'POST' });
      toast(`Restored: ${rel}, as it was on ${new Date(on.at).toLocaleString()}`);
      if (open) { open.dirty = false; await R.helpers.close(open); await R.helpers.openFile(rel); } // (closed first: then it opens anew)
    } catch (e) { toast(e.message, true); }
  };
  pop(`Versions of ${rel}`, h('div', { class: 'vers' }, rows, view), [['Restore this version', restore, true], ['Close', () => {}]]).classList.add('wide');
  view.append(h('p', { class: 'muted' }, text ? 'Pick a version to see what changed since, line by line: − was there, + is now.' : 'Pick a version.'));
  if (list.length) show(list[now != null && list.length > 1 ? 1 : 0]); // (the one before now)
}

/** The lines of `a` against `b`: kept, gone (−) and added (+); long runs of kept lines folded. */
export function diff(a, b) {
  const x = a.split('\n'), y = b.split('\n');
  if (x.length * y.length > 4e6) return [h('p', { class: 'muted' }, `Too long to compare here (${x.length} and ${y.length} lines).`)];
  const n = x.length, m = y.length, L = new Uint32Array((n + 1) * (m + 1)), at = (i, j) => i * (m + 1) + j;
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) L[at(i, j)] = x[i] === y[j] ? L[at(i + 1, j + 1)] + 1 : Math.max(L[at(i + 1, j)], L[at(i, j + 1)]);
  const out = [];
  let i = 0, j = 0;
  while (i < n || j < m) {
    if (i < n && j < m && x[i] === y[j]) { out.push([' ', x[i]]); i++; j++; }
    else if (i < n && (j === m || L[at(i + 1, j)] >= L[at(i, j + 1)])) out.push(['−', x[i++]]); // (what went, then what came)
    else out.push(['+', y[j++]]);
  }
  if (!out.some(([k]) => k !== ' ')) return [h('p', { class: 'muted' }, 'The same as now.')];
  const lines = [];
  for (let k = 0; k < out.length; k++) {
    const near = out.slice(Math.max(0, k - 3), k + 4).some(([c]) => c !== ' ');
    if (out[k][0] === ' ' && !near) {
      let e = k;
      while (e < out.length && out[e][0] === ' ' && !out.slice(Math.max(0, e - 3), e + 4).some(([c]) => c !== ' ')) e++;
      lines.push(h('div', { class: 'dl fold' }, `… ${e - k} line${e - k === 1 ? '' : 's'} the same`));
      k = e - 1;
      continue;
    }
    lines.push(h('div', { class: 'dl ' + { '+': 'plus', '−': 'minus', ' ': '' }[out[k][0]] }, h('span', { class: 'mk' }, out[k][0]), out[k][1]));
  }
  return [h('pre', { class: 'dpre' }, lines)];
}
