// A file's versions (ADR-035 §8): every save is kept; one is shown against the file as it is now,
// line by line, and restored. Loaded when first asked for (a file's ⋯ › Versions…).
import { h, icon, ago, bytes, call, fileUrl, toast, pop, S, R, moreStyle } from './core.js';
import { highlight } from './editor.js';

await moreStyle();

const TEXT = /\.(sql|py|md|txt|ipynb|json|jsonl|ndjson|csv|tsv|yaml|yml|toml|js|css|html|xml)$/i;
const LANG = { sql: 'sql', py: 'python', md: 'markdown' };

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
    const ext = rel.split('.').pop().toLowerCase();
    if (on === v) view.replaceChildren(...(ext === 'ipynb' ? cellsDiff(was, now ?? '') : diff(was, now ?? '', LANG[ext] || 'text')));
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

/** A notebook's cells as they were against as they are, each as its kind shows it: a cell the same
 * folded, one changed its lines compared, one added or gone whole (outputs aren't compared). */
export function cellsDiff(a, b) {
  const cells = t => { try { return (JSON.parse(t).cells || []).map(c => {
    const src = Array.isArray(c.source) ? c.source.join('') : String(c.source ?? ''), magic = c.cell_type === 'code' && src.match(/^%%sql[^\n]*\n?/);
    return { id: c.id, kind: c.cell_type !== 'code' ? 'markdown' : magic ? 'sql' : 'python', src: magic ? src.slice(magic[0].length) : src, head: magic ? magic[0].trim() : '' };
  }); } catch { return null; } };
  const x = cells(a), y = cells(b);
  if (!x || !y) return diff(a, b, 'text');
  const key = c => c.id || c.kind + '\u0000' + c.src, n = x.length, m = y.length, L = new Uint32Array((n + 1) * (m + 1)), at = (i, j) => i * (m + 1) + j;
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) L[at(i, j)] = key(x[i]) === key(y[j]) ? L[at(i + 1, j + 1)] + 1 : Math.max(L[at(i + 1, j)], L[at(i, j + 1)]);
  const steps = [];
  for (let i = 0, j = 0; i < n || j < m;) {
    if (i < n && j < m && key(x[i]) === key(y[j])) steps.push([x[i++], y[j++]]);
    else if (i < n && (j === m || L[at(i + 1, j)] >= L[at(i, j + 1)])) steps.push([x[i++], null]);
    else steps.push([null, y[j++]]);
  }
  const label = { sql: 'SQL', python: 'Python', markdown: 'Markdown' }, out = [];
  let same = 0, k = 0;
  const flush = () => { if (same) out.push(h('div', { class: 'dl fold' }, `… ${same} cell${same === 1 ? '' : 's'} the same`)); same = 0; };
  for (const [was, now] of steps) {
    if (now) k++;
    const c = now || was, changed = was && now && (was.src !== now.src || was.head !== now.head || was.kind !== now.kind);
    if (was && now && !changed) { same++; continue; }
    flush();
    const what = !was ? 'added' : !now ? 'gone' : 'changed', head = [now ? `Cell ${k}` : 'A cell', label[c.kind], c.head.replace(/^%%sql\s*/, '') && `→ ${c.head.replace(/^%%sql\s*|\s*<<$/g, '')}`, what].filter(Boolean).join(' · ');
    out.push(h('div', { class: 'vcell ' + what }, h('div', { class: 'vhead' }, head), ...diff(was ? was.src : '', now ? now.src : '', c.kind, true)));
  }
  flush();
  return out.some(e => e.classList.contains('vcell')) ? out : [h('p', { class: 'muted' }, 'The same as now (outputs aren\'t compared).')];
}

/** The lines of `a` against `b`: kept, gone (−) and added (+), highlighted as `lang`; long runs of
 * kept lines folded (`all`: every line shown, as a cell's). */
export function diff(a, b, lang = 'text', all = false) {
  const lines = t => all && t === '' ? [] : t.split('\n'); // (a cell added or gone: no empty line against it)
  const x = lines(a), y = lines(b);
  if (x.length * y.length > 4e6) return [h('p', { class: 'muted' }, `Too long to compare here (${x.length} and ${y.length} lines).`)];
  const n = x.length, m = y.length, L = new Uint32Array((n + 1) * (m + 1)), at = (i, j) => i * (m + 1) + j;
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) L[at(i, j)] = x[i] === y[j] ? L[at(i + 1, j + 1)] + 1 : Math.max(L[at(i + 1, j)], L[at(i, j + 1)]);
  const hx = highlight(a, lang), hy = highlight(b, lang); // (each text whole: a comment or a string over lines stays one)
  const out = [];
  let i = 0, j = 0;
  while (i < n || j < m) {
    if (i < n && j < m && x[i] === y[j]) { out.push([' ', hy[j]]); i++; j++; }
    else if (i < n && (j === m || L[at(i + 1, j)] >= L[at(i, j + 1)])) out.push(['−', hx[i++]]); // (what went, then what came)
    else out.push(['+', hy[j++]]);
  }
  if (!out.some(([k]) => k !== ' ') && !all) return [h('p', { class: 'muted' }, 'The same as now.')];
  const shown = [];
  for (let k = 0; k < out.length; k++) {
    const near = all || out.slice(Math.max(0, k - 3), k + 4).some(([c]) => c !== ' ');
    if (out[k][0] === ' ' && !near) {
      let e = k;
      while (e < out.length && out[e][0] === ' ' && !out.slice(Math.max(0, e - 3), e + 4).some(([c]) => c !== ' ')) e++;
      shown.push(h('div', { class: 'dl fold' }, `… ${e - k} line${e - k === 1 ? '' : 's'} the same`));
      k = e - 1;
      continue;
    }
    shown.push(h('div', { class: 'dl ' + { '+': 'plus', '−': 'minus', ' ': '' }[out[k][0]] }, h('span', { class: 'mk' }, out[k][0]), h('span', { html: out[k][1] })));
  }
  return [h('pre', { class: 'dpre' }, shown)];
}
