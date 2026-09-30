// A statement's plan as a graph (ADR-034, round 29): EXPLAIN's steps as boxes, each above the
// steps it reads from, the ones that move rows (between cores, or nodes when a query is spread)
// marked; its Profile runs the statement with EXPLAIN ANALYZE and puts each step's rows and time
// on it, the costliest in the strongest colour. Loaded when first shown.
import { h, icon, run, secs, count, moreStyle } from './core.js';
import { doneText } from './notebook.js';

await moreStyle();

const MOVES = /^(RepartitionExec|CoalescePartitionsExec|SortPreservingMergeExec|CoalesceBatchesExec)$/;

/** The Plan tab's view of `sql` (its `$name`s bound to `params`). */
export function planView(sql, params) {
  const box = h('div', { class: 'planv' }), st = { how: 'graph', plan: null, profile: null, busy: false };
  const read = /^\s*(\(|select\b|with\b|values\b|from\b|table\b)/i.test(sql);
  const draw = () => {
    const tab = (id, label) => h('button', { class: 'seg' + (st.how === id ? ' on' : ''), 'aria-pressed': String(st.how === id), onclick: () => { st.how = id; draw(); } }, label);
    const shown = st.profile || st.plan;
    box.replaceChildren(h('div', { class: 'cbar' }, h('span', { class: 'segs' }, tab('graph', 'Graph'), tab('text', 'Text')),
      h('span', { class: 'muted' }, st.profile ? `Profiled: ${secs(st.profile.ms)} in all` : st.plan ? 'The plan, before it runs' : ''), h('span', { class: 'grow' }),
      read ? h('button', { class: 'btn small', disabled: st.busy, title: 'Run it with EXPLAIN ANALYZE: each step\'s rows and time (it runs the query)', onclick: profile }, icon('play'), st.busy ? 'Profiling…' : st.profile ? 'Profile again' : 'Profile') : null),
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
