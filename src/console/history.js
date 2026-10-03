// History (ADR-034, ADR-048), loaded when first shown: what ran, newest first. Statements as the node
// remembers them (`pondra.history`), and the node's runs (`pondra.runs`). What the shell gives it
// comes through `R.helpers`.
import { on, h, secs, count, utc, icon, svg, S, R, SESSION, rows, quote, menu, pop, moreStyle } from './core.js';
import { highlighted } from './editor.js';
import { copyText } from './grid.js';
import { iconOf, oneLine } from './files.js';

await moreStyle();

const H = R.helpers, { facts, head, detail, show, openFile } = H;
let again = 0;

/** History: what ran, newest first. Statements as the node remembers them (`pondra.history`,
 * ADR-048): this tab's, this page's, or all the user may see (an admin's: everyone's) but pages'
 * own queries; a slow one with its plan and where its time went. Runs: procedures, files run and
 * schedules' runs (`pondra.runs`). What runs on a schedule is in Jobs (jobs.js). */
export async function runs() {
  const o = S.hist ||= { of: 'tab' }, seg = (id, label, title) => h('button', { class: 'seg' + (o.of === id ? ' on' : ''), 'aria-pressed': String(o.of === id), title, onclick: () => { o.of = id; detail(); } }, label);
  const bar = h('div', { class: 'hbar' }, h('span', { class: 'segs' }, seg('tab', 'This tab', 'What the tab in front ran'), seg('page', 'This page', 'What every tab of this page ran'),
    seg('all', 'All', 'Every statement you may see (an admin: everyone\'s), from every door'), seg('runs', 'Runs', 'Procedures, files and schedules run on the node')),
  h('label', { class: 'check' }, h('input', { type: 'checkbox', checked: !!o.bad, onchange: e => { o.bad = e.target.checked; detail(); } }), 'Slow or failed'));
  clearTimeout(again);
  return [head('clock', 'History', 'what ran, newest first'), bar, ...o.of === 'runs' ? await jobRuns(o.bad) : await statements(o)];
}
const firstLine = code => { const ls = code.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#')); return ls.length ? oneLine(ls[0], 90) + (ls.length > 1 ? ' …' : '') : ''; };
// (a console's Python cell, as the node was sent it: `DO LANGUAGE python $tag$ … $tag$`)
const pyOf = s => /^\s*do\s+language\s+python\s+\$(\w*)\$\n?([\s\S]*?)\n?\$\1\$\s*;?\s*$/i.exec(s)?.[2] ?? null;
on('ran', () => { for (const t of [1500, 4000]) setTimeout(() => S.tab === 'runs' && detail(), t); }); // (the node writes its history a second at a time)
on('active', () => S.tab === 'runs' && S.hist?.of === 'tab' && detail());

async function statements(o) {
  const tab = S.doc?.session;
  if (o.of === 'tab' && !tab) return [h('div', { class: 'empty' }, 'Nothing run in this tab yet.')];
  // (the node keeps a session as `user~id`)
  const where = [o.of === 'tab' ? `session LIKE ${quote('%~' + tab)}` : o.of === 'page' ? `session LIKE ${quote(`%~${SESSION}-%`)}` : 'coalesce(session !~ \'~console-[0-9a-f]+$\', true)', o.bad ? '(outcome <> \'ok\' OR plan IS NOT NULL)' : ''].filter(Boolean).join(' AND ');
  if (Date.now() - (S.ran[0]?.at || 0) < 5000) again = setTimeout(() => S.tab === 'runs' && detail(), 1500); // (something just ran: the node writes it a second or so later)
  let list;
  try { list = await rows(`SELECT at, id, "user", door, "from", node, session, class, statement, outcome, error, ms, rows, nodes, plan IS NOT NULL AS slow FROM pondra.history WHERE ${where} ORDER BY at DESC LIMIT 50`); }
  catch { return S.ran.length ? S.ran.map(x => item(x.kind === 'python' ? 'filepy' : 'filesql', x.kind === 'python' ? firstLine(x.src) : oneLine(x.src, 90), x.ok ? secs(x.ms) : 'failed', !x.ok, // (a node from before pondra.history: what this page ran)
    `${x.where} · ${new Date(x.at).toLocaleTimeString()}`, x.error, () => pageLook(x), pageActs(x))) : [h('div', { class: 'empty' }, 'Nothing run yet.')]; }
  if (!list.length) return [h('div', { class: 'empty' }, o.bad ? 'No slow or failed statement.' : 'Nothing yet. (The node writes what ran a second or so later.)')];
  return list.map(x => {
    const py = pyOf(x.statement), ok = x.outcome === 'ok', n = +x.rows;
    const acts = pageActs({ kind: py != null ? 'python' : 'sql', src: py ?? x.statement, cut: /…$/.test(x.statement) || x.statement.includes("'***'") });
    const sub = [x.slow ? 'slow: its plan kept' : '', utc(x.at).toLocaleString(), x.rows != null && x.class !== 'skipped' ? `${count(n)} row${n === 1 ? '' : 's'}` : '', +x.nodes > 1 ? `on ${x.nodes} nodes` : '', o.of === 'all' ? [x.user, x.door].filter(Boolean).join(' by ') : ''];
    return item(py != null ? 'filepy' : 'filesql', py != null ? firstLine(py) || 'DO' : oneLine(x.statement, 90), ok ? secs(+x.ms) : x.outcome, !ok, sub.filter(Boolean).join(' · '), x.error, () => look(x, py, acts), [...acts, ['Copy its id', () => copyText(x.id)]]);
  });
}
/** A row of the History: an icon, what ran, how long it took (or how it ended), when and where, its error. */
const item = (ic, name, meta, bad, sub, error, open, acts, cls = '') => h('div', { class: 'run-item' + cls, role: 'button', tabindex: '0', title: 'Click: what it ran. Right-click: more', onclick: open, onkeydown: e => e.key === 'Enter' && open(),
  oncontextmenu: e => { e.preventDefault(); menu(e, [{ label: 'Show it', run: open }, ...acts.map(([label, run]) => ({ label, run }))]); } },
  h('div', { class: 'line1' }, h('span', { class: 'ic', html: svg(ic, 14) }), h('span', { class: 'nm' }, name), h('span', { class: 'meta' + (bad ? ' bad' : '') }, meta)),
  h('div', { class: 'sub' }, sub), error ? h('div', { class: 'sub bad' }, error.split('\n')[0].slice(0, 200)) : null);
/** A statement: who ran it, where and how it went; a slow one's plan as it ran, and each node's part. */
async function look(x, py, acts) {
  const more = h('div', {});
  pop(`Statement ${x.id.slice(0, 8)}`, h('div', {}, facts([['When', utc(x.at).toLocaleString()], ['Who', [x.user, x.door, x.from].filter(Boolean).join(' · ')], ['Node', x.node], ['Took', secs(+x.ms)],
    ['Rows', x.rows != null ? count(+x.rows) : null], ['Nodes', x.nodes != null ? String(x.nodes) : null], ['Kind', x.class], ['Ended', x.outcome], ['Session', x.session]]),
  h('pre', { class: 'defn', html: highlighted(py ?? x.statement, py != null ? 'python' : 'sql') }), x.error ? h('pre', { class: 'err' }, x.error) : null, more), acts);
  if (!x.slow) return;
  const [k] = await rows(`SELECT plan, trace FROM pondra.history WHERE id = ${quote(x.id)}`).catch(() => []);
  let spans = [];
  try { spans = JSON.parse(k?.trace || '[]'); } catch { /* (none) */ }
  const end = Math.max(1, ...spans.map(s => s.at + s.ms));
  more.append(...k?.plan ? [h('div', { class: 'dsect' }, 'Its plan, as it ran'), (await import('./plan.js')).keptPlan(k.plan)] : [],
    ...spans.length ? [h('div', { class: 'dsect' }, 'Where its time went'), h('div', { class: 'trace' }, spans.map(s => h('div', { class: 'tr', title: `${s.what} on ${s.node}: from ${secs(s.at)}, for ${secs(s.ms)}` },
      h('span', { class: 'tw' }, `${s.what} · ${s.node}`), h('span', { class: 'tb' }, h('i', { style: `left:${s.at / end * 100}%;width:${Math.max(s.ms / end * 100, 0.5)}%` })))))] : []);
}
/** Runs on the node (`pondra.runs`): jobs, files run, procedures, schedules' runs. */
async function jobRuns(bad) {
  let node = [];
  try { node = await rows(`SELECT id, routine, caller, status, started, ended, args, error FROM pondra.runs${bad ? ' WHERE status = \'failed\'' : ''} ORDER BY started DESC LIMIT 30`); } catch { /* (no run yet, or no rights) */ }
  // (until it ends; a run whose node stopped under it says running for good: not looked at again after a day)
  if (node.some(x => x.status === 'running' && Date.now() - utc(x.started) < 864e5)) again = setTimeout(() => { if (S.tab === 'runs') detail(); }, 2000);
  const sched = x => x.caller === 'schedule' ? x.routine : x.caller?.startsWith('task:') ? x.caller.slice(5) : null;
  const took = x => x.ended ? secs(utc(x.ended) - utc(x.started)) : 'running';
  const fileOf = x => /^files\/.+@/.test(x.routine) ? x.routine.replace(/^files\//, '').replace(/@[^@]*$/, '') : null;
  // (a DO block: its code, a console's Python cell or file run on the node)
  const codeOf = x => { if (x.routine !== 'do') return null; try { const a = JSON.parse(x.args || '{}'); return a.code ? { code: a.code, language: a.language || 'python' } : null; } catch { return null; } };
  const nameOf = x => { const c = codeOf(x); return fileOf(x) || (c ? firstLine(c.code) || 'DO' : x.routine === 'do' ? 'DO (a Python cell)' : x.routine); };
  const nodeActs = x => [fileOf(x) ? ['Open the file', () => openFile(fileOf(x)), true] : null, codeOf(x) ? ['Open in a new file', () => R.helpers.newWith(codeOf(x).language === 'python' ? 'python' : 'sql', codeOf(x).code), true] : null,
    codeOf(x) ? ['Copy the code', () => copyText(codeOf(x).code)] : null, ['Copy its id', () => copyText(String(x.id))]].filter(Boolean);
  const nodeLook = x => pop(`Run ${String(x.id).slice(0, 12)}`, h('div', {}, facts([['What', codeOf(x) ? `DO LANGUAGE ${codeOf(x).language}` : x.routine], ['Who', x.caller], ['Status', x.status], ['Started', utc(x.started).toLocaleString()], ['Ended', x.ended ? utc(x.ended).toLocaleString() : null], ['Took', took(x)], ['Id', String(x.id)]]),
    codeOf(x) ? h('pre', { class: 'defn', html: highlighted(codeOf(x).code, codeOf(x).language === 'python' ? 'python' : 'sql') }) : null, x.error ? h('pre', { class: 'err' }, x.error) : null), nodeActs(x));
  const nodeRun = x => {
    const r = item(fileOf(x) ? iconOf(fileOf(x)) : codeOf(x)?.language === 'python' || x.routine === 'do' ? 'filepy' : 'play', nameOf(x), x.status === 'failed' ? 'failed' : took(x), x.status === 'failed',
      `${sched(x) ? '' : `${String(x.id).slice(0, 8)} · ${x.caller} · `}${utc(x.started).toLocaleString()}`, x.error, () => nodeLook(x), nodeActs(x), String(x.id) === String(S.jobRun) ? ' fresh' : '');
    // (a schedule's run, and what it called: tagged with the schedule, which Jobs shows)
    if (sched(x)) r.children[1].prepend(h('button', { class: 'tag', title: 'It ran on a schedule: Jobs has the schedules', onclick: e => { e.stopPropagation(); show('jobs'); } }, icon('calendar', 'ic', 11), sched(x)));
    return r;
  };
  return node.length ? node.map(nodeRun) : [h('div', { class: 'empty' }, bad ? 'No failed run.' : 'No run yet: a file\'s Run ▾ runs it as a job.')];
}
/** What to do with something this page ran: open it as a new file, put it in the one in front,
 * copy it, run it again, see its plan. */
function pageActs(x) {
  const fresh = () => R.helpers.newWith(x.kind === 'python' ? 'python' : 'sql', x.src);
  return [['Open in a new file', fresh, true], S.doc?.put ? ['Put it in the tab in front', () => { S.doc.put(x.src); }] : null, ['Copy it', () => copyText(x.src)],
    x.cut ? null : ['Run it again', async () => (await fresh()).run()], x.kind === 'sql' ? ['See its plan', async () => { const d = await fresh(); d.tab = 'plan'; d.run(); }] : null].filter(Boolean);
}
function pageLook(x) {
  pop(`Run #${x.id}`, h('div', {}, facts([['Where', x.where], ['When', new Date(x.at).toLocaleString()], ['Took', secs(x.ms)], ['Answer', x.ok ? x.rows != null ? `${count(x.rows)} row${x.rows === 1 ? '' : 's'}` : 'done' : 'failed']]),
    h('pre', { class: 'defn', html: highlighted(x.src, x.kind === 'python' ? 'python' : 'sql') }), x.error ? h('pre', { class: 'err' }, x.error) : null), pageActs(x));
}
