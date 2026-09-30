// The console's rarer parts (ADR-034, round 29), loaded when first used, so the page's first load
// doesn't carry them: the History view (the node's runs, what this page ran), the Variables view,
// search (Ctrl K), choosing the Python, a file run as a job or on a schedule. What they use of the shell comes through `R.helpers`.
import { h, $, secs, count, bytes, utc, icon, svg, S, R, call, run, rows, ident, quote, toast, menu, prompt, confirmed, pop, fileUrl, fileSql, writeFile, moreStyle } from './core.js';
import { highlighted } from './editor.js';
import { copyText } from './grid.js';
import { iconOf, oneLine, FOLDER, download } from './files.js';
import { cleanName } from './notebook.js';

await moreStyle();

const H = R.helpers, { KIND, pick, act, facts, head, detail, readVars, show, openFile, newFile, restart, kernel } = H;
let again = 0;


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

/** History: what ran, newest first: the node's runs (jobs, files run, procedures, schedules' runs:
 * `pondra.runs`), and what this page ran. What runs on a schedule is in Jobs (jobs.js). */
export async function runs() {
  let node = [];
  try { node = await rows('SELECT id, routine, caller, status, started, ended, args, error FROM pondra.runs ORDER BY started DESC LIMIT 30'); } catch { /* (no run yet, or no rights) */ }
  clearTimeout(again);
  // (until it ends; a run whose node stopped under it says running for good: not looked at again after a day)
  if (node.some(x => x.status === 'running' && Date.now() - utc(x.started) < 864e5)) again = setTimeout(() => { if (S.tab === 'runs') detail(); }, 2000);
  const sched = x => x.caller === 'schedule' ? x.routine : x.caller?.startsWith('task:') ? x.caller.slice(5) : null;
  const took = x => x.ended ? secs(utc(x.ended) - utc(x.started)) : 'running';
  const fileOf = x => /^files\/.+@/.test(x.routine) ? x.routine.replace(/^files\//, '').replace(/@[^@]*$/, '') : null;
  // (a DO block: its code, a console's Python cell or file run on the node)
  const codeOf = x => { if (x.routine !== 'do') return null; try { const a = JSON.parse(x.args || '{}'); return a.code ? { code: a.code, language: a.language || 'python' } : null; } catch { return null; } };
  const firstLine = code => { const ls = code.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#')); return ls.length ? oneLine(ls[0], 90) + (ls.length > 1 ? ' …' : '') : ''; };
  const nameOf = x => { const c = codeOf(x); return fileOf(x) || (c ? firstLine(c.code) || 'DO' : x.routine === 'do' ? 'DO (a Python cell)' : x.routine); };
  const nodeActs = x => [fileOf(x) ? ['Open the file', () => openFile(fileOf(x)), true] : null, codeOf(x) ? ['Open in a new file', async () => { const d = await newFile(codeOf(x).language === 'python' ? 'python' : 'sql'); d.ed.value = codeOf(x).code; d.changed(); }, true] : null,
    codeOf(x) ? ['Copy the code', () => copyText(codeOf(x).code)] : null, ['Copy its id', () => copyText(String(x.id))]].filter(Boolean);
  const nodeLook = x => pop(`Run ${String(x.id).slice(0, 12)}`, h('div', {}, facts([['What', codeOf(x) ? `DO LANGUAGE ${codeOf(x).language}` : x.routine], ['Who', x.caller], ['Status', x.status], ['Started', utc(x.started).toLocaleString()], ['Ended', x.ended ? utc(x.ended).toLocaleString() : null], ['Took', took(x)], ['Id', String(x.id)]]),
    codeOf(x) ? h('pre', { class: 'defn', html: highlighted(codeOf(x).code, codeOf(x).language === 'python' ? 'python' : 'sql') }) : null, x.error ? h('pre', { class: 'err' }, x.error) : null), nodeActs(x));
  const nodeRun = x => h('div', { class: 'run-item', role: 'button', tabindex: '0', title: 'Click: what it ran. Right-click: more', onclick: () => nodeLook(x), onkeydown: e => e.key === 'Enter' && nodeLook(x),
    oncontextmenu: e => { e.preventDefault(); menu(e, [{ label: 'Show it', run: () => nodeLook(x) }, ...nodeActs(x).map(([label, run]) => ({ label, run }))]); } },
    h('div', { class: 'line1' }, h('span', { class: 'ic', html: svg(fileOf(x) ? iconOf(fileOf(x)) : codeOf(x)?.language === 'python' || x.routine === 'do' ? 'filepy' : 'play', 14) }), h('span', { class: 'nm' }, nameOf(x)),
      h('span', { class: 'meta ' + (x.status === 'failed' ? 'bad' : '') }, x.status === 'failed' ? 'failed' : took(x))),
    // (a schedule's run, and what it called: tagged with the schedule, which Jobs shows)
    h('div', { class: 'sub' }, sched(x) ? h('button', { class: 'tag', title: 'It ran on a schedule: Jobs has the schedules', onclick: e => { e.stopPropagation(); show('jobs'); } }, icon('calendar', 'ic', 11), sched(x)) : null,
      `${sched(x) ? '' : `${String(x.id).slice(0, 8)} · ${x.caller} · `}${utc(x.started).toLocaleString()}`), x.error ? h('div', { class: 'sub bad' }, x.error.split('\n')[0].slice(0, 200)) : null);
  const page = x => h('div', { class: 'run-item', role: 'button', tabindex: '0', title: 'Click: what it ran, and what to do with it. Right-click: the same', onclick: () => pageLook(x), onkeydown: e => e.key === 'Enter' && pageLook(x),
    oncontextmenu: e => { e.preventDefault(); menu(e, pageActs(x).map(([label, run]) => ({ label, run }))); } },
    h('div', { class: 'line1' }, h('span', { class: 'ic k-' + x.kind, html: svg(x.kind === 'python' ? 'filepy' : 'filesql', 14) }), h('span', { class: 'nm' }, x.kind === 'python' ? firstLine(x.src) : oneLine(x.src, 90)), h('span', { class: 'meta ' + (x.ok ? '' : 'bad') }, x.ok ? secs(x.ms) : 'failed')),
    h('div', { class: 'sub' }, `#${x.id} · ${x.where} · ${new Date(x.at).toLocaleTimeString()}${x.rows != null ? ` · ${count(x.rows)} row${x.rows === 1 ? '' : 's'}` : ''}`));
  return [head('clock', 'History', 'what ran, on the node and on this page'),
    h('div', { class: 'dsect' }, 'On the node'), ...node.length ? node.map(nodeRun) : [h('div', { class: 'empty' }, 'No job yet: a file\'s Run ▾ runs it as one.')],
    h('div', { class: 'dsect' }, 'This page'), ...S.ran.length ? S.ran.map(page) : [h('div', { class: 'empty' }, 'Nothing run yet.')]];
}
/** What to do with something this page ran: open it as a new file, put it in the one in front,
 * copy it, run it again, see its plan. */
function pageActs(x) {
  const fresh = async () => { const d = await newFile(x.kind === 'python' ? 'python' : 'sql'); d.ed.value = x.src; d.changed(); return d; };
  return [['Open in a new file', fresh, true], S.doc?.put ? ['Put it in the tab in front', () => { S.doc.put(x.src); }] : null, ['Copy it', () => copyText(x.src)],
    ['Run it again', async () => (await fresh()).run()], x.kind === 'sql' ? ['See its plan', async () => { const d = await fresh(); d.tab = 'plan'; d.run(); }] : null].filter(Boolean);
}
function pageLook(x) {
  pop(`Run #${x.id}`, h('div', {}, facts([['Where', x.where], ['When', new Date(x.at).toLocaleString()], ['Took', secs(x.ms)], ['Answer', x.ok ? x.rows != null ? `${count(x.rows)} row${x.rows === 1 ? '' : 's'}` : 'done' : 'failed']]),
    h('pre', { class: 'defn', html: highlighted(x.src, x.kind === 'python' ? 'python' : 'sql') }), x.error ? h('pre', { class: 'err' }, x.error) : null), pageActs(x));
}
/** A file (or a saved notebook) run on the node, not waited for (ADR-033): now, as a job
 * (`pondra.start('run', …)`), or on a schedule, as a task. What is saved runs, with the SQL file's
 * parameters as they are now; History shows it. */
export async function job(doc, every) {
  if (doc.dirty && !(await doc.save())) return;
  if (every && !(every = await prompt('Schedule', 'How often', '1 hour', 'For example 15 minutes, 1 day, or cron 0 2 * * * UTC. It runs on the node as CALL run(…): Jobs lists it, History its runs.'))) return;
  const path = doc.kind === 'notebook' ? `notebooks/${doc.name}` : doc.path, name = path.replace(/\.[^./]+$/, '').replace(/\W+/g, '_').replace(/^_+|_+$/g, '').toLowerCase() || 'job';
  const args = quote(path) + Object.entries(doc.params?.() || {}).map(([n, v]) => `, ${ident(n)} => ${typeof v === 'string' ? quote(v) : String(v).toUpperCase()}`).join('');
  try {
    await run(every ? `CREATE OR REPLACE TASK ${ident(name)} SCHEDULE ${quote(every)} AS CALL run(${args})` : `SELECT pondra.start('run', ${args})`);
    toast(every ? `Scheduled: ${name}, every ${every}` : `Started on the node: ${path}`); show(every ? 'jobs' : 'runs');
  } catch (e) { toast(e.message, true); }
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

/** A tab's right-click menu: pin it, close it or some of the others (the pinned stay), copy its path. */
export function tabMenu(e, d) {
  const { closeDoc: close, pin } = H, i = S.docs.indexOf(d), some = f => S.docs.filter((x, j) => x !== d && !x.pinned && f(x, j));
  const closeAll = async list => { for (const x of list) await close(x); };
  const others = some(() => true), right = some((_, j) => j > i), saved = some(x => !x.dirty);
  menu(e, [{ label: d.pinned ? 'Unpin' : 'Pin', icon: 'pin', run: () => pin(d, !d.pinned) }, '-',
    { label: 'Close', icon: 'close', keys: 'Delete', run: () => close(d) },
    { label: 'Close others', disabled: !others.length, run: () => closeAll(others) }, { label: 'Close to the right', disabled: !right.length, run: () => closeAll(right) },
    { label: 'Close saved', disabled: !saved.length && (d.dirty || d.pinned), run: () => closeAll(saved.concat(d.dirty || d.pinned ? [] : [d])) },
    { label: S.docs.some(x => x.pinned) ? 'Close all but the pinned' : 'Close all', run: () => closeAll(S.docs.filter(x => !x.pinned)) },
    d.path ? '-' : null, d.path ? { label: 'Copy the path', icon: 'copy', run: () => copyText('files/' + d.path, 'Path copied') } : null]);
}

// ------------------------------------------------------------------ the Workspace's files: their menus, a folder, uploads, renaming, deleting
/** A folder in `at`, by name (`a/b` makes both): its marker is put as any file is. */
export async function newFolder(at = '') {
  const name = ((await prompt('New folder', 'Its name', '')) || '').replace(/^\/+|\/+$/g, ''), rel = at + name;
  if (!name) return;
  const bad = /(^|\/)(\.{0,2}|\s+)(\/|$)|^notebooks(\/|$)/.test(rel) ? 'Not a folder name: no empty part, . or .., nor in notebooks/'
    : S.files?.some(f => f.path === 'files/' + rel || f.path.startsWith(`files/${rel}/`)) ? `${rel} is there already` : '';
  if (bad) return toast(bad, true);
  try {
    await call(fileUrl(`${rel}/${FOLDER}`), { method: 'PUT', body: '' });
    S.open.add('dir:' + at.slice(0, -1)); // (so it shows)
    toast(`Made the folder ${rel}`);
  } catch (err) { toast('No folder made: ' + err.message, true); }
  R.helpers.refreshFiles();
}
/** A file from this computer into the lake's files (a notebook opens, unless it is for a folder: then it is put there; the rest are put). */
export function upload(at = '') {
  const input = h('input', { type: 'file', hidden: true, multiple: true });
  input.onchange = async () => {
    const fs = [...input.files];
    input.remove();
    for (const f of fs) {
      if (/\.ipynb$/i.test(f.name) && (!at || at === 'notebooks/')) { try { R.helpers.openNotebook(JSON.parse(await f.text()), cleanName(f.name) || 'uploaded'); toast(`Opened ${f.name}: Ctrl+S keeps it in the lake`); } catch (err) { toast(`Could not open ${f.name}: ${err.message}`, true); } continue; }
      const rel = at + f.name;
      try { await call(fileUrl(rel), { method: 'PUT', body: f }); toast(`Put in the lake: files/${rel}`); } catch (err) {
        if (err.status !== 409) { toast(`${f.name}: ${err.message}`, true); continue; }
        if (!confirmed(`files/${rel} is there already. Replace it with the one picked?`)) continue; // (a file is replaced only as it is: its version asked for)
        const version = ((await call(fileUrl(rel), { method: 'HEAD' })).headers.get('etag') || '').replace(/"/g, '');
        if (await writeFile(rel, f, version, f.type || 'application/octet-stream')) toast(`Replaced files/${rel}`);
      }
    }
    R.helpers.refreshFiles();
  };
  document.body.append(input); input.click();
}
export async function rename(rel) {
  const to = await prompt('Rename', 'The new path, under the lake\'s files', rel);
  if (!to || to === rel) return;
  try {
    const r = await call(fileUrl(rel));
    await call(fileUrl(to), { method: 'PUT', body: await r.blob() });
    await call(fileUrl(rel), { method: 'DELETE' });
    const doc = S.docs.find(d => d.path === rel);
    if (doc) { await R.helpers.close(doc); R.helpers.openFile(to); } // (its tab again, at the new path)
    toast(`Renamed to ${to}`);
  } catch (e) { toast('Not renamed: ' + e.message, true); }
  R.helpers.refreshFiles();
}
/** Delete a file, a notebook (every version) or a folder (every file in it, its marker too), once asked. */
export async function remove(f, folder) {
  try {
    const paths = f.notebook || folder ? (await rows(`SELECT path FROM files(${quote(f.rel + '/')})`)).map(x => x.path) : ['files/' + f.rel], n = paths.filter(p => !p.endsWith('/' + FOLDER)).length;
    if (!confirmed(f.notebook ? `Delete the notebook ${f.name}, every version of it?` : folder ? `Delete the folder ${f.rel} and its ${n} file${n === 1 ? '' : 's'}? This can't be undone.` : `Delete files/${f.rel}? This can't be undone.`)) return;
    await Promise.all(paths.map(p => call(fileUrl(p), { method: 'DELETE' })));
    for (const d of S.docs) if (d.path === f.rel || folder && d.path?.startsWith(f.rel + '/')) Object.assign(d, { dirty: true, version: null, written: null }); // (a tab keeps what it holds, unsaved)
    R.helpers.drawTabs();
    toast(`Deleted ${f.name}`);
  } catch (e) { toast('Not deleted: ' + e.message, true); }
  R.helpers.refreshFiles();
}


/** Create a table, a view or a materialized view from a query (the Run ▾'s, and the editor's, Create as…). */
export function createAs(sql) {
  const stmt = sql.trim().replace(/;\s*$/, ''), st = { kind: 'TABLE' }, code = h('pre', { class: 'defn' }), segs = h('span', { class: 'segs' });
  const input = h('input', { value: 'new_table', spellcheck: 'false', 'aria-label': 'Its name' });
  const text = () => `CREATE ${st.kind} ${input.value.trim() || '…'} AS\n${stmt};`;
  const draw = () => {
    segs.replaceChildren(...[['TABLE', 'Table'], ['VIEW', 'View'], ['MATERIALIZED VIEW', 'Materialized view']].map(([k, label]) => h('button', { type: 'button', class: 'seg' + (st.kind === k ? ' on' : ''), onclick: () => { st.kind = k; draw(); } }, label)));
    code.innerHTML = highlighted(text(), 'sql');
  };
  input.oninput = draw; draw();
  pop('Create as', h('div', { class: 'form' }, segs, h('label', {}, 'Its name: schema.name, or a name (in public)', input), code,
    h('small', {}, 'A table keeps the rows as they are now; a view runs its query each time it is read; a materialized view keeps its rows up to date as its tables change.')),
  [['Create', async () => { try { await run(text()); toast(`Made ${input.value.trim()}`); H.refresh(); } catch (e) { toast(e.message, true); } }, true],
    ['Open in a new tab', async () => { const d = await newFile('sql'); d.ed.value = text(); d.changed(); }], ['Cancel', () => {}]]);
  requestAnimationFrame(() => { input.focus(); input.select(); });
}
