// A statement's plan as a graph (ADR-034, round 29): EXPLAIN's steps as boxes, each above the
// steps it reads from, the ones that move rows (between cores, or nodes when a query is spread)
// marked; its Query profile runs the statement with EXPLAIN ANALYZE and puts each step's rows and time
// on it, the costliest in the strongest colour. Loaded when first shown.
import { h, icon, run, secs, count, moreStyle, menu, saveAs } from './core.js';
import { copyText } from './grid.js';
import { doneText } from './notebook.js';

await moreStyle();

const MOVES = /^(RepartitionExec|CoalescePartitionsExec|SortPreservingMergeExec|CoalesceBatchesExec)$/;

/** The Plan tab's view of `sql` (its `$name`s bound to `params`). */
export function planView(sql, params) {
  const box = h('div', { class: 'planv' }), st = { how: 'graph', plan: null, profile: null, busy: false };
  const read = /^\s*(\(|select\b|with\b|values\b|from\b|table\b)/i.test(sql.replace(/--[^\n]*|\/\*[\s\S]*?\*\//g, ' ')); // (a comment before it too)
  const draw = () => {
    const tab = (id, label) => h('button', { class: 'seg' + (st.how === id ? ' on' : ''), 'aria-pressed': String(st.how === id), onclick: () => { st.how = id; draw(); } }, label);
    const shown = st.profile || st.plan;
    box.replaceChildren(h('div', { class: 'cbar' }, h('span', { class: 'segs' }, tab('graph', 'Graph'), tab('text', 'Text')),
      h('span', { class: 'muted' }, st.profile ? `Query profile: ${secs(st.profile.ms)} in all` : st.plan ? 'The plan, before it runs' : ''), h('span', { class: 'grow' }),
      read ? h('button', { class: 'btn small', disabled: st.busy, title: 'Run it with EXPLAIN ANALYZE: each step\'s rows and time (it runs the query)', onclick: profile }, icon('play'), st.busy ? 'Profiling…' : st.profile ? 'Query profile again' : 'Query profile') : null,
      shown?.tree ? h('button', { class: 'icon', title: 'Copy or download the plan', 'aria-label': 'Copy or download the plan', onclick: e => menu(e.currentTarget, [
        { label: 'Copy as text', icon: 'copy', run: () => copyText(shown.text, 'Copied the plan') },
        { label: 'Download as text', icon: 'down', run: () => saveAs(shown.text, 'text/plain', 'plan.txt') },
        { label: 'Download as a picture (SVG)', run: () => saveAs(picture(shown.tree), 'image/svg+xml', 'plan.svg') },
        { label: 'Download as a picture (PNG)', run: () => png(picture(shown.tree)) }]) }, icon('down')) : null),
    !shown ? h('div', { class: 'wait' }, 'Reading the plan…') : shown.error ? h('pre', { class: 'err' }, shown.error)
      : st.how === 'text' ? h('pre', { class: 'said plan' }, shown.text) : graph(shown.tree));
  };
  const ask = async analyze => {
    const t0 = performance.now();
    try {
      const r = await run(`EXPLAIN ${analyze ? 'ANALYZE ' : ''}${sql}`, undefined, params);
      if (r.kind !== 'rows') return { error: doneText(r.value) || 'No plan.' };
      const byType = Object.fromEntries(r.rows.map(x => [String(x[0]), String(x[1])]));
      const physical = byType.physical_plan || byType['Plan with Metrics'] || Object.values(byType).at(-1) || '';
      return { text: r.rows.map(x => `${x[0]}\n${x[1]}`).join('\n\n'), tree: tree(physical), ms: performance.now() - t0 };
    } catch (e) { return { error: e.message }; }
  };
  async function profile() { st.busy = true; draw(); st.profile = await ask(true); st.busy = false; st.how = 'graph'; draw(); }
  ask(false).then(p => { st.plan = p; draw(); });
  draw();
  return box;
}

/** EXPLAIN's indented lines as a tree: each step, its details, its metrics (if analyzed), the steps under it. */
function tree(text) {
  const root = { kids: [] }, stack = [{ depth: -1, node: root }];
  for (const line of text.split('\n')) {
    if (!line.trim()) continue;
    const depth = line.search(/\S/), [, op, rest = ''] = line.trim().match(/^([\w]+)(?::\s*(.*))?$/) || [null, line.trim()];
    const m = rest.match(/,?\s*metrics=\[(.*)\]\s*$/), metrics = {};
    for (const [, k, v] of (m?.[1] || '').matchAll(/(\w+)=([^,\]]+)/g)) metrics[k] = v.trim();
    const node = { op, detail: m ? rest.slice(0, m.index) : rest, metrics, kids: [] };
    while (stack.at(-1).depth >= depth) stack.pop();
    stack.at(-1).node.kids.push(node);
    stack.push({ depth, node });
  }
  return root.kids;
}

/** A metric's time in ms: DataFusion's `1.2ms`, `350µs`, `2.1s`, `800ns`. */
const ms = v => { const m = String(v || '').match(/([\d.]+)\s*(ns|µs|us|ms|s)\b/); return m ? +m[1] * { ns: 1e-6, µs: 1e-3, us: 1e-3, ms: 1, s: 1e3 }[m[2]] : 0; };

/** The steps as boxes, each above what it reads from; time shares colour them when profiled. */
function graph(steps) {
  const all = [], walk = n => { all.push(n); n.kids.forEach(walk); };
  steps.forEach(walk);
  const total = all.reduce((a, n) => a + ms(n.metrics.elapsed_compute), 0);
  const box = n => {
    const t = ms(n.metrics.elapsed_compute), share = total ? t / total : 0, moves = MOVES.test(n.op);
    return h('li', {}, h('div', { class: 'pn' + (moves ? ' moves' : ''), style: share ? `--heat:${Math.round(share * 100)}%` : null, title: `${n.op}${n.detail ? ': ' + n.detail : ''}${Object.keys(n.metrics).length ? '\n' + Object.entries(n.metrics).map(([k, v]) => `${k} = ${v}`).join('\n') : ''}` },
      h('b', {}, n.op.replace(/Exec$/, '')), n.detail ? h('span', { class: 'pd' }, n.detail.length > 64 ? n.detail.slice(0, 63) + '…' : n.detail) : null,
      moves ? h('span', { class: 'pm' }, 'rows move here') : null,
      n.metrics.output_rows != null ? h('span', { class: 'pmx' }, `${count(+n.metrics.output_rows || 0)} rows · ${t ? secs(t) : '0 ms'}${share >= 0.005 ? ` · ${Math.round(share * 100)}%` : ''}`) : null),
    n.kids.length ? h('ul', {}, n.kids.map(box)) : null);
  };
  return steps.length ? h('div', { class: 'pgraph' }, h('ul', { class: 'pt' }, steps.map(box))) : h('div', { class: 'empty' }, 'No plan to draw.');
}

/** The plan as a picture (SVG): each step a box above the steps it reads from, with its rows and
 * time when profiled, on a light ground, for a document or a ticket. */
function picture(steps) {
  const esc = t => t.replace(/[&<>"]/g, c => `&#${c.charCodeAt(0)};`), GX = 16, GY = 30, BH = 50;
  const lines = n => [n.op.replace(/Exec$/, ''), n.detail.length > 56 ? n.detail.slice(0, 55) + '…' : n.detail,
    n.metrics.output_rows != null ? `${n.metrics.output_rows} rows · ${n.metrics.elapsed_compute || ''}` : ''].filter(Boolean);
  const span = n => { n.w = Math.max(...lines(n).map(l => l.length)) * 6.7 + 20; n.kw = n.kids.reduce((a, k) => a + span(k), 0) + GX * (n.kids.length - 1); return n.span = Math.max(n.w, n.kw); };
  let out = '', height = 0;
  const place = (n, x, y) => {
    const cx = x + n.span / 2;
    height = Math.max(height, y + BH);
    out += `<rect x="${cx - n.w / 2}" y="${y}" width="${n.w}" height="${BH}" rx="6"/>` + lines(n).map((l, i) => `<text x="${cx}" y="${y + 17 + i * 14}"${i ? '' : ' font-weight="600"'}>${esc(l)}</text>`).join('');
    let kx = x + (n.span - n.kw) / 2;
    for (const k of n.kids) { out += `<path d="M${cx} ${y + BH}V${y + BH + GY / 2}H${kx + k.span / 2}V${y + BH + GY}"/>`; place(k, kx, y + BH + GY); kx += k.span + GX; }
  };
  let x = 10;
  for (const n of steps) { span(n); place(n, x, 10); x += n.span + GX; }
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${x}" height="${height + 10}" font-family="ui-monospace,Menlo,Consolas,monospace" font-size="11" text-anchor="middle">`
    + `<style>rect{fill:#fff;stroke:#9aa1ab}path{fill:none;stroke:#9aa1ab}text{fill:#1f2328}</style><rect x="0" y="0" width="${x}" height="${height + 10}" rx="0" style="stroke:none;fill:#f6f7f9"/>${out}</svg>`;
}

/** The picture as a PNG, twice its size (a canvas draws the SVG). */
function png(svgText) {
  const img = new Image(), url = URL.createObjectURL(new Blob([svgText], { type: 'image/svg+xml' }));
  img.onload = () => {
    const c = h('canvas', { width: img.width * 2, height: img.height * 2 }), g = c.getContext('2d');
    g.scale(2, 2); g.drawImage(img, 0, 0); URL.revokeObjectURL(url);
    c.toBlob(b => saveAs(b, 'image/png', 'plan.png'));
  };
  img.src = url;
}
