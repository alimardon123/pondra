// The console's rarer parts (ADR-034, round 29), loaded when first used, so the page's first load
// doesn't carry them: the Runs view (the node's runs, the schedules, what this page ran), the
// Variables view, Settings, search (Ctrl K), choosing the Python, a table's profile, a file run as
// a job. What they use of the shell comes through `R.helpers`.
import { h, $, secs, count, bytes, utc, icon, svg, S, R, call, run, rows, ident, quote, toast, menu, prompt, confirmed, pop, numeric, moreStyle } from './core.js';
import { highlighted } from './editor.js';
import { copyText, statView, spread } from './grid.js';
import { iconOf, oneLine, FOLDER } from './files.js';

await moreStyle();

const H = R.helpers, { KIND, pick, act, facts, head, detail, drawLeft, drawViews, look, readVars, themeNow, prefs, pane, show, openFile, newFile, restart, kernel } = H;
let again = 0;

/** A table's columns profiled where it is: one pass for every column's counts and range, then a
 * histogram or the commonest values of each (at most 24 columns). */
export async function profile(t, boxes, btn) {
  const cols = t.columns.slice(0, 24), q = ident, simple = c => !/^(List|LargeList|FixedSizeList|Struct|Map|Binary|LargeBinary|BinaryView)|\[\]$/.test(c.d);
  btn.disabled = true; btn.lastChild.textContent = 'Profiling…';
  try {
    const parts = cols.flatMap((c, i) => [`count(${q(c.n)}) AS "v${i}"`, simple(c) ? `approx_distinct(${q(c.n)}) AS "d${i}"` : `NULL AS "d${i}"`,
      simple(c) ? `CAST(min(${q(c.n)}) AS VARCHAR) AS "lo${i}"` : `NULL AS "lo${i}"`, simple(c) ? `CAST(max(${q(c.n)}) AS VARCHAR) AS "hi${i}"` : `NULL AS "hi${i}"`]);
    const [a] = await rows(`SELECT count(*) AS n, ${parts.join(', ')} FROM ${t.q}`);
    const stats = cols.map((c, i) => ({ n: Number(a.n), nulls: Number(a.n) - Number(a['v' + i]), distinct: Number(a['d' + i] ?? 0), exact: false, min: a['lo' + i], max: a['hi' + i] }));
    const showOne = i => { const box = boxes[i]?.querySelector('.ps'); if (box) box.replaceChildren(...statView(stats[i])); };
    cols.forEach((_, i) => showOne(i));
    let next = 0;
    const one = async () => {
      for (let i; (i = next++) < cols.length;) {
        const c = cols[i], at = spread(c.d), s = stats[i];
        if (!simple(c) || s.n === s.nulls) continue;
        if (at && s.min != null) {
          const lo = at(s.min), hi = at(s.max);
          if (!Number.isFinite(lo) || !Number.isFinite(hi)) continue;
          const x = numeric(c.d) ? `CAST(${q(c.n)} AS DOUBLE)` : `CAST(date_part('epoch', CAST(${q(c.n)} AS TIMESTAMP)) AS DOUBLE) * 1000`;
          const b = hi === lo ? '0' : `least(19, CAST(floor((${x} - ${lo}) / ${(hi - lo) / 20}) AS BIGINT))`;
          const hist = Array(20).fill(0);
          for (const r of await rows(`SELECT ${b} AS b, count(*) AS n FROM ${t.q} WHERE ${q(c.n)} IS NOT NULL GROUP BY 1`)) hist[Math.max(0, Math.min(19, Number(r.b)))] += Number(r.n);
          s.hist = hist;
        } else if (!at) {
          s.top = (await rows(`SELECT CAST(${q(c.n)} AS VARCHAR) AS v, count(*) AS n FROM ${t.q} WHERE ${q(c.n)} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 5`)).map(r => ({ v: r.v, n: Number(r.n) }));
        }
        showOne(i);
      }
    };
    await Promise.all([one(), one(), one()]);
  } catch (e) { toast('Could not profile it: ' + e.message, true); }
  btn.disabled = false; btn.lastChild.textContent = 'Profile';
}

/** Choose the Python the node runs: each this machine has, tried (its version, whether pondra and
 * pyarrow import), the one in use marked; one that lacks them says how to install them. */
export async function choosePython() {
  const list = h('div', { class: 'pylist' }, h('div', { class: 'wait pulse' }, 'Looking for the Pythons on this machine (a few seconds)…'));
  const path = h('input', { placeholder: 'or a Python\'s path, for example C:\\Python312\\python.exe', spellcheck: 'false', 'aria-label': 'A Python\'s path' });
  const use = async p => {
    try { const r = await (await call('/python', { method: 'PUT', body: JSON.stringify({ path: p }), headers: { 'content-type': 'application/json' } })).json(); S.pyInfo = { version: r.version, python: r.path }; kernel('none'); toast(`Python ${r.version} it is, from now on (and next time)`); d.close(); }
    catch (e) { toast(e.message, true); }
  };
  const d = pop('Choose the Python', h('div', {}, h('p', { class: 'muted' }, 'The Python the node runs notebooks\' cells, Python files and functions with. It needs the pondra package and pyarrow. The choice is kept on this machine.'), list,
    h('div', { class: 'pypath' }, path, h('button', { class: 'btn', onclick: () => path.value.trim() && use(path.value.trim()) }, 'Use it'))));
  try {
    const v = await (await call('/python?all=1')).json(), now = v.python;
    list.replaceChildren(...(v.pythons || []).map(p => h('div', { class: 'py' + (p.ok ? '' : ' bad') + (p.path === now ? ' on' : '') },
      h('div', { class: 'line1' }, h('span', { class: 'ic', html: svg(p.ok ? 'check' : 'close', 14) }), h('b', {}, p.version ? `Python ${p.version}` : 'Python'), p.path === now ? h('span', { class: 'chip' }, 'in use') : null, h('span', { class: 'grow' }),
        p.ok && p.path !== now ? h('button', { class: 'btn small', onclick: () => use(p.path) }, 'Use it') : null),
      h('div', { class: 'sub' }, p.path),
      p.ok ? null : h('div', { class: 'sub bad' }, p.error || (!p.pondra || !p.pyarrow ? `It lacks ${[!p.pondra && 'pondra', !p.pyarrow && 'pyarrow'].filter(Boolean).join(' and ')}.` : 'It doesn\'t work.')),
      p.ok || !p.version ? null : h('div', { class: 'fix' }, h('code', {}, `"${p.path}" -m pip install pondra pyarrow`), h('button', { class: 'icon', title: 'Copy the command', 'aria-label': 'Copy the command', onclick: () => copyText(`"${p.path}" -m pip install pondra pyarrow`) }, icon('copy'))))));
    if (!list.children.length) list.replaceChildren(h('div', { class: 'empty' }, 'No Python found: install one (python.org), then pondra and pyarrow in it.'));
  } catch (e) { list.replaceChildren(h('pre', { class: 'err' }, e.message)); }
}

export async function variables() {
  let v;
  try { v = await readVars(); } catch (e) { return [h('pre', { class: 'err' }, e.message)]; }
  return [head('var', 'Variables', v.busy ? 'a cell is running: they show when it is done' : v.running ? `${S.vars.length} in this page's Python` : 'no Python yet: a Python cell or file starts it'),
    h('div', { class: 'acts2' }, act('restart', 'Restart', 'Stop this page\'s Python: its variables go (its temporary tables stay)', restart), act('refresh', 'Refresh', 'Read them again', () => detail())),
    ...S.vars.map(x => h('div', { class: 'var' }, h('div', { class: 'line1' }, h('span', { class: 'nm' }, x.name), h('span', { class: 'ty' }, x.type + (x.size ? ` · ${x.size}` : ''))), h('div', { class: 'look' }, x.look)))];
}

/** Runs: the node's (jobs, files run, procedures and tasks: `pondra.runs`), the schedules
 * (`pondra.tasks`), and what this page ran, newest first. */
export async function runs() {
  let node = [], tasks = [];
  try { node = await rows('SELECT id, routine, caller, status, started, ended, args, error FROM pondra.runs ORDER BY started DESC LIMIT 30'); } catch { /* (no run yet, or no rights) */ }
  try { tasks = await rows('SELECT name, schedule, statement, next_tick FROM pondra.tasks ORDER BY name'); } catch { /* (none) */ }
  clearTimeout(again);
  // (until it ends; a run whose node stopped under it says running for good: not looked at again after a day)
  if (node.some(x => x.status === 'running' && Date.now() - utc(x.started) < 864e5)) again = setTimeout(() => { if (S.tab === 'runs') detail(); }, 2000);
  const took = x => x.ended ? secs(utc(x.ended) - utc(x.started)) : 'running';
  const fileOf = x => /^files\/.+@/.test(x.routine) ? x.routine.replace(/^files\//, '').replace(/@[^@]*$/, '') : null;
  // (a DO block: its code, a console's Python cell or file run on the node)
  const codeOf = x => { if (x.routine !== 'do') return null; try { const a = JSON.parse(x.args || '{}'); return a.code ? { code: a.code, language: a.language || 'python' } : null; } catch { return null; } };
  const firstLine = code => { const ls = code.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#')); return ls.length ? oneLine(ls[0], 90) + (ls.length > 1 ? ' …' : '') : ''; };
  const nameOf = x => { const c = codeOf(x); return fileOf(x) || (c ? firstLine(c.code) || 'DO' : x.routine === 'do' ? 'DO (a Python cell)' : x.routine); };
  const nodeActs = x => [fileOf(x) ? ['Open the file', () => openFile(fileOf(x)), true] : null, codeOf(x) ? ['Open in a new file', () => { const d = newFile(codeOf(x).language === 'python' ? 'python' : 'sql'); d.ed.value = codeOf(x).code; d.changed(); }, true] : null,
    codeOf(x) ? ['Copy the code', () => copyText(codeOf(x).code)] : null, ['Copy its id', () => copyText(String(x.id))]].filter(Boolean);
  const nodeLook = x => pop(`Run ${String(x.id).slice(0, 12)}`, h('div', {}, facts([['What', codeOf(x) ? `DO LANGUAGE ${codeOf(x).language}` : x.routine], ['Who', x.caller], ['Status', x.status], ['Started', utc(x.started).toLocaleString()], ['Ended', x.ended ? utc(x.ended).toLocaleString() : null], ['Took', took(x)], ['Id', String(x.id)]]),
    codeOf(x) ? h('pre', { class: 'defn', html: highlighted(codeOf(x).code, codeOf(x).language === 'python' ? 'python' : 'sql') }) : null, x.error ? h('pre', { class: 'err' }, x.error) : null), nodeActs(x));
  const nodeRun = x => h('div', { class: 'run-item', role: 'button', tabindex: '0', title: 'Click: what it ran. Right-click: more', onclick: () => nodeLook(x), onkeydown: e => e.key === 'Enter' && nodeLook(x),
    oncontextmenu: e => { e.preventDefault(); menu(e, [{ label: 'Show it', run: () => nodeLook(x) }, ...nodeActs(x).map(([label, run]) => ({ label, run }))]); } },
    h('div', { class: 'line1' }, h('span', { class: 'ic', html: svg(fileOf(x) ? iconOf(fileOf(x)) : codeOf(x)?.language === 'python' || x.routine === 'do' ? 'filepy' : 'play', 14) }), h('span', { class: 'nm' }, nameOf(x)),
      h('span', { class: 'meta ' + (x.status === 'failed' ? 'bad' : '') }, x.status === 'failed' ? 'failed' : took(x))),
    h('div', { class: 'sub' }, `${String(x.id).slice(0, 8)} · ${x.caller} · ${utc(x.started).toLocaleString()}`), x.error ? h('div', { class: 'sub bad' }, x.error.split('\n')[0].slice(0, 200)) : null);
  const task = t => h('div', { class: 'run-item', title: t.statement },
    h('div', { class: 'line1' }, h('span', { class: 'ic', html: svg('clock', 14) }), h('span', { class: 'nm' }, t.name), h('span', { class: 'meta' }, t.schedule),
      h('button', { class: 'icon sm', title: `Stop ${t.name}: DROP TASK`, 'aria-label': `Drop the task ${t.name}`, onclick: async () => { if (!confirmed(`Drop the task ${t.name}? It stops running.`)) return; try { await run(`DROP TASK ${ident(t.name)}`); detail(); } catch (e) { toast(e.message, true); } } }, icon('trash'))),
    h('div', { class: 'sub' }, `${t.statement.slice(0, 120)} · next ${utc(t.next_tick).toLocaleString()}`));
  const page = x => h('div', { class: 'run-item', role: 'button', tabindex: '0', title: 'Click: what it ran, and what to do with it. Right-click: the same', onclick: () => pageLook(x), onkeydown: e => e.key === 'Enter' && pageLook(x),
    oncontextmenu: e => { e.preventDefault(); menu(e, pageActs(x).map(([label, run]) => ({ label, run }))); } },
    h('div', { class: 'line1' }, h('span', { class: 'ic k-' + x.kind, html: svg(x.kind === 'python' ? 'filepy' : 'filesql', 14) }), h('span', { class: 'nm' }, oneLine(x.src, 90)), h('span', { class: 'meta ' + (x.ok ? '' : 'bad') }, x.ok ? secs(x.ms) : 'failed')),
    h('div', { class: 'sub' }, `#${x.id} · ${x.where} · ${new Date(x.at).toLocaleTimeString()}${x.rows != null ? ` · ${count(x.rows)} row${x.rows === 1 ? '' : 's'}` : ''}`));
  return [head('clock', 'Runs', 'jobs and schedules on the node, and what this page ran'),
    h('div', { class: 'dsect' }, 'On the node'), ...node.length ? node.map(nodeRun) : [h('div', { class: 'empty' }, 'No job yet: a file\'s ⋯ runs it as one.')],
    tasks.length ? h('div', { class: 'dsect' }, 'Schedules') : null, ...tasks.map(task),
    h('div', { class: 'dsect' }, 'This page'), ...S.ran.length ? S.ran.map(page) : [h('div', { class: 'empty' }, 'Nothing run yet.')]];
}
/** What to do with something this page ran: open it as a new file, put it in the one in front,
 * copy it, run it again, see its plan. */
function pageActs(x) {
  const fresh = () => { const d = newFile(x.kind === 'python' ? 'python' : 'sql'); d.ed.value = x.src; d.changed(); return d; };
  return [['Open in a new file', fresh, true], S.doc?.put ? ['Put it in the tab in front', () => { S.doc.put(x.src); }] : null, ['Copy it', () => copyText(x.src)],
    ['Run it again', () => { const d = fresh(); d.run(); }], x.kind === 'sql' ? ['See its plan', () => { const d = fresh(); d.tab = 'plan'; d.run(); }] : null].filter(Boolean);
}
function pageLook(x) {
  pop(`Run #${x.id}`, h('div', {}, facts([['Where', x.where], ['When', new Date(x.at).toLocaleString()], ['Took', secs(x.ms)], ['Answer', x.ok ? x.rows != null ? `${count(x.rows)} row${x.rows === 1 ? '' : 's'}` : 'done' : 'failed']]),
    h('pre', { class: 'defn', html: highlighted(x.src, x.kind === 'python' ? 'python' : 'sql') }), x.error ? h('pre', { class: 'err' }, x.error) : null), pageActs(x));
}
/** A file (or a saved notebook) run on the node, not waited for (ADR-033): now, as a job
 * (`pondra.start('run', …)`), or on a schedule, as a task. What is saved runs, with the SQL file's
 * parameters as they are now; Runs shows it. */
export async function job(doc, every) {
  if (doc.dirty && !(await doc.save())) return;
  if (every && !(every = await prompt('Schedule', 'How often', '1 hour', 'For example 15 minutes, 1 day, or cron 0 2 * * * UTC. It runs on the node as CALL run(…), and Runs lists it.'))) return;
  const path = doc.kind === 'notebook' ? `notebooks/${doc.name}` : doc.path, name = path.replace(/\.[^./]+$/, '').replace(/\W+/g, '_').replace(/^_+|_+$/g, '').toLowerCase() || 'job';
  const args = quote(path) + Object.entries(doc.params?.() || {}).map(([n, v]) => `, ${ident(n)} => ${typeof v === 'string' ? quote(v) : String(v).toUpperCase()}`).join('');
  try {
    await run(every ? `CREATE OR REPLACE TASK ${ident(name)} SCHEDULE ${quote(every)} AS CALL run(${args})` : `SELECT pondra.start('run', ${args})`);
    toast(every ? `Scheduled: ${name}, every ${every}` : `Started on the node: ${path}`); show('runs');
  } catch (e) { toast(e.message, true); }
}

export function settings() {
  const d = $('#settingsDlg'), set = (id, v) => { $(id).value = v; };
  const colors = () => ({ ...prefs('colors') || {} }), mine = () => colors()[themeNow()] || {};
  const css = getComputedStyle(document.documentElement), hex = v => { const c = document.createElement('canvas').getContext('2d'); c.fillStyle = v; return c.fillStyle.startsWith('#') ? c.fillStyle : '#888888'; };
  const show = () => {
    set('#setTheme', prefs('theme') || 'light'); set('#setGroups', prefs('workspaceFirst') ? 'workspace' : 'data'); set('#setFont', prefs('font') || 'geist'); set('#setStatements', prefs('statements') || 'each');
    set('#setBg', mine().bg || hex(css.getPropertyValue('--surface').trim())); set('#setAccent', mine().accent || hex(css.getPropertyValue('--accent').trim()));
    $('#setColorsFor').textContent = `for the ${themeNow()} theme`;
    $('#setWhere').textContent = S.prefsHere ? 'Kept on this machine: every lake and session opened here has them.' : 'Kept in this browser (the node can\'t keep them on its machine for a page on another).';
  };
  const color = (k, v) => { const all = colors(); all[themeNow()] = { ...mine(), [k]: v }; if (!v) delete all[themeNow()][k]; prefs('colors', all); look(); show(); };
  d.onchange = e => {
    if (e.target.id === 'setBg' || e.target.id === 'setAccent') return;
    prefs('theme', $('#setTheme').value); prefs('workspaceFirst', $('#setGroups').value === 'workspace'); prefs('font', $('#setFont').value); prefs('statements', $('#setStatements').value); look(); drawLeft(); show();
  };
  $('#setBg').oninput = e => color('bg', e.target.value); $('#setAccent').oninput = e => color('accent', e.target.value);
  $('#setBgReset').onclick = () => color('bg', null); $('#setAccentReset').onclick = () => color('accent', null);
  $('#setReset').onclick = () => { for (const k of ['sides', 'folded', 'weights', 'widths', 'left', 'right', 'bottom', 'rorder', 'split', 'results']) prefs(k, null); $('#left').style.width = $('#right').style.width = ''; drawViews(); pane('left', true); toast('The layout is back as it was'); };
  show();
  d.showModal();
}

/** Search (Ctrl K): tables, files, notebooks and commands, as you type. */
export function palette() {
  const d = $('#palette'), input = $('#palIn'), list = $('#palList');
  let items = [], on = 0;
  const all = () => [
    ...R.commands.size ? [...R.commands.values()].map(c => ({ kind: 'command', icon: c.icon || 'keyboard', label: c.title, note: c.keys || 'command', run: c.run })) : [],
    ...(S.objects || []).map(t => ({ kind: 'table', icon: (KIND[t.o.kind] || KIND.table)[0], label: t.q, note: (KIND[t.o.kind] || KIND.table)[1], run: () => { pick({ type: 'object', t }); } })),
    ...(S.files || []).filter(f => !/^files\/notebooks\/[^/]+\/[^/]+\.ipynb$/.test(f.path) && !f.path.endsWith('/' + FOLDER)).map(f => ({ kind: 'file', icon: iconOf(f.path), label: f.path.replace(/^files\//, ''), note: bytes(f.size), run: () => openFile(f.path) })),
    ...[...new Set((S.files || []).map(f => f.path.match(/^files\/notebooks\/([^/]+)\//)?.[1]).filter(Boolean))].map(n => ({ kind: 'notebook', icon: 'notebook', label: `notebooks/${n}.ipynb`, note: 'notebook', run: () => openFile('notebooks/' + n) })),
  ];
  const score = (text, q) => { if (!q) return 1; let i = 0; const t = text.toLowerCase(); for (const ch of q) { i = t.indexOf(ch, i); if (i < 0) return 0; i++; } return t.includes(q) ? 2 + (t.startsWith(q) ? 1 : 0) : 1; };
  const draw = () => {
    const q = input.value.trim().toLowerCase();
    items = all().map(x => ({ ...x, s: score(x.label, q) })).filter(x => x.s).sort((a, b) => b.s - a.s || a.label.length - b.label.length).slice(0, 60);
    on = Math.min(on, Math.max(0, items.length - 1));
    list.replaceChildren(...items.length ? items.map((x, i) => h('div', { class: 'pi' + (i === on ? ' on' : ''), role: 'option', 'aria-selected': String(i === on), onmousedown: e => { e.preventDefault(); on = i; go(); } },
      h('span', { class: 'ic k-' + x.kind, html: svg(x.icon, 15) }), h('span', { class: 'nm' }, x.label), h('span', { class: 'meta' }, x.note))) : [h('div', { class: 'empty' }, 'Nothing matches.')]);
    list.children[on]?.scrollIntoView({ block: 'nearest' });
  };
  const go = () => { const x = items[on]; d.close(); x?.run(); };
  input.value = ''; on = 0; draw();
  input.oninput = () => { on = 0; draw(); };
  input.onkeydown = e => {
    const mv = { ArrowDown: 1, ArrowUp: -1 }[e.key];
    if (mv) { e.preventDefault(); on = (on + mv + items.length) % Math.max(1, items.length); draw(); }
    else if (e.key === 'Enter') { e.preventDefault(); go(); }
  };
  d.showModal();
}
