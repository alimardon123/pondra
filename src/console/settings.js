// Settings (ADR-034, round 29): a window of sections, a list of settings each, and a search across
// them. The console's own (Appearance, Editor and results, Layout, Keys: kept on this machine, in
// console.json) and the node's (Python, About); and signing in. More come in with `register.setting`, under their
// own heading: an enterprise build's users and roles, tokens, audit and quotas. Loaded when opened.
import { h, $, icon, svg, count, ICONS, S, R, call, register, moreStyle, toast, store } from './core.js';
import { PER_PAGE } from './grid.js';

await moreStyle();

const H = R.helpers, { prefs, look, themeNow, drawLeft, drawViews, pane } = H;
Object.assign(ICONS, {
  look: '<circle cx="12" cy="12" r="8.5"/><path d="M12 3.5a8.5 8.5 0 0 1 0 17z" fill="currentColor" stroke="none"/>',
  code: '<path d="m8.5 7.5-4.5 4.5 4.5 4.5M15.5 7.5l4.5 4.5-4.5 4.5M13.5 5.5l-3 13"/>',
  info: '<circle cx="12" cy="12" r="8.5"/><path d="M12 11v5.5M12 7.9v.1"/>',
});
// (the core's keys, for Keys: the page's own handle them; an extension's keys come with `register.key`)
const KEYS = { Anywhere: [['Ctrl K', 'Search tables, files and commands'], ['Ctrl S', 'Save the file or notebook in front'], ['Ctrl B', 'The left pane'], ['Ctrl J', 'The bottom panel'], ['Ctrl Alt B', 'The right pane'], ['?', 'These keys']],
  'In a cell or a file': [['Ctrl Enter', 'Run it (a SQL file: what is selected, else the file, or the statement as Settings say)'], ['Shift Enter', 'Run it and go to the next cell'], ['Alt Enter', 'Run it and add a cell below'], ['Ctrl Shift Enter', 'Run every cell (a SQL file: the statement at the caret)'], ['Shift Alt F', 'Format the selection, or all of it'], ['Ctrl Shift E', 'Show the execution plan of the selection or the statement (not run)'], ['Tab', 'Complete a name (or indent)'], ['Ctrl Space', 'Complete a name'], ['Ctrl /', 'Comment the lines out, or in'], ['Esc', 'Leave the cell: the keys below then work']],
  'On a cell (after Esc)': [['Enter', 'Edit it'], ['↑ ↓', 'The cell above, below (or K J)'], ['A B', 'Add a cell above, below'], ['D D', 'Delete it (Z brings it back)'], ['S P M', 'Make it SQL, Python, Markdown'], ['L', 'Live on or off: its answer again after each commit that changes it'], ['O', 'Hide or show its output'], ['0 0', 'Restart Python: its variables go']],
  'In a grid': [['Click', 'A cell: its row lights up'], ['Shift Click', 'A range'], ['Ctrl C', 'Copy, tab-separated (Shift: with column names)'], ['Ctrl A', 'Select every cell'], ['Alt PgDn', 'The next page of rows (Alt PgUp: the one before)'], ['Enter', 'Edit a data file\'s cell']],
  'In a tab': [['← →', 'The tab before, after'], ['Delete', 'Close it'], ['Right-click', 'Pin it, close others']] };
R.keys = [...Object.entries(KEYS).flatMap(([group, ks]) => ks.map(([keys, title]) => ({ keys, title, group }))), ...R.keys];
const ACCENTS = [null, '#2563EB', '#7C3AED', '#DB2777', '#EA580C', '#0891B2', '#4B5563']; // (null: Pondra's own)
let at = 'look', dlg = null;

/** A choice of a few, as buttons side by side: `[[value, label]]`, the one now pressed. */
const segs = (now, options, set) => h('span', { class: 'segs', role: 'group' }, options.map(([v, label]) => h('button', { class: 'seg' + (now === v ? ' on' : ''), 'aria-pressed': String(now === v), onclick: () => { set(v); draw(); } }, label)));
const kbd = keys => h('span', { class: 'keys-k' }, keys.split(' ').map(k => h('kbd', {}, k)));
const colors = () => ({ ...prefs('colors') || {} }), mine = () => colors()[themeNow()] || {};
function color(k, v) { const all = colors(); all[themeNow()] = { ...mine(), [k]: v }; if (!v) delete all[themeNow()][k]; prefs('colors', all); look(); draw(); }
const hex = v => { const c = document.createElement('canvas').getContext('2d'); c.fillStyle = v; return c.fillStyle.startsWith('#') ? c.fillStyle : '#888888'; };
const now = v => getComputedStyle(document.documentElement).getPropertyValue(v).trim();

// ------------------------------------------------------------------ the sections: [label, what it does, its control]
register.setting({ id: 'look', group: 'Personal', icon: 'look', order: 10, title: 'Appearance', about: 'How the console looks.', rows: () => [
  ['Theme', 'Light, dark, or as the system is.', h('div', { class: 'themes' }, [['light', 'Light'], ['dark', 'Dark'], ['system', 'As the system']].map(([v, label]) =>
    h('button', { class: 'theme t-' + v + ((prefs('theme') || 'light') === v ? ' on' : ''), 'aria-pressed': String((prefs('theme') || 'light') === v), onclick: () => { prefs('theme', v); look(); draw(); } }, h('span', { class: 'pv' }, h('i'), h('i'), h('i')), label))), true],
  ['Accent', `Buttons, links and what is selected, in the ${themeNow()} theme.`, h('div', { class: 'swatches' }, ACCENTS.map(c => h('button', { class: 'sw' + ((mine().accent || null) === c ? ' on' : ''), style: `--c:${c || 'var(--pondra-accent)'}`, title: c || 'Pondra\'s', 'aria-label': c ? `Accent ${c}` : 'Pondra\'s accent', onclick: () => color('accent', c) })),
    h('label', { class: 'sw own', title: 'A colour of your own' }, h('input', { type: 'color', value: hex(now('--accent')), 'aria-label': 'An accent of your own', oninput: e => color('accent', e.target.value) }), icon('plus', 'ic', 14))), true],
  ['Background', `The ${themeNow()} theme's; its panes and lines are shaded from it.`, h('span', { class: 'colors' }, h('input', { type: 'color', value: mine().bg || hex(now('--surface')), 'aria-label': 'The background colour', oninput: e => color('bg', e.target.value) }),
    mine().bg ? h('button', { class: 'btn small', onclick: () => color('bg', null) }, 'Default') : null)],
  ['Font', 'The text\'s. Code is always Geist Mono.', segs(prefs('font') || 'geist', [['geist', 'Geist'], ['system', 'The system\'s']], v => { prefs('font', v); look(); })],
] });
register.setting({ id: 'editor', group: 'Personal', icon: 'code', order: 20, title: 'Editor and results', about: 'Running, formatting, and the answers.', rows: () => [
  ['Ctrl+Enter in a SQL file', 'What it runs when nothing is selected (a selection runs as it is). Ctrl+Shift+Enter runs the statement at the caret either way.', segs(prefs('enter') || 'file', [['file', 'The file'], ['statement', 'The statement at the caret']], v => prefs('enter', v))],
  ['A SQL file\'s statements', 'Run together: an answer for each, or only the last one\'s.', segs(prefs('statements') || 'each', [['each', 'An answer each'], ['last', 'The last one\'s']], v => prefs('statements', v))],
  ['Results', 'Where a SQL file\'s answers show.', segs(prefs('results') || 'below', [['below', 'Below'], ['right', 'At the right']], v => {
    prefs('results', v);
    for (const d of S.docs) if (d.kind === 'sql' && d.place) { d.layout = v; d.place(); d.draw(); }
  })],
  ['Rows a page', 'An answer of more rows comes a page at a time: ‹ 1 2 3 › under it turns them (Alt+Page Down, Alt+Page Up); the node keeps the rest, so a page is the same rows, not the query run again.',
    h('select', { 'aria-label': 'Rows a page', onchange: e => { prefs('pageRows', +e.target.value); S.pageRows = +e.target.value; } }, PER_PAGE.map(n => h('option', { value: n, selected: n === (S.pageRows || 10000) }, count(n))))],
  ['Format', 'SQL is formatted here, Python by the node\'s Python (ruff, else black). The selection, or with none the whole file or cell.', kbd('Shift Alt F')],
] });
register.setting({ id: 'layout', group: 'Personal', icon: 'paneL', order: 30, title: 'Layout', about: 'The panes, and the tabs.', rows: () => [
  ['The left pane', 'Which comes first in it.', segs(prefs('workspaceFirst') ? 'workspace' : 'data', [['data', 'Data first'], ['workspace', 'Workspace first']], v => { prefs('workspaceFirst', v === 'workspace'); drawLeft(); })],
  ['Pinned tabs', 'Right-click a tab to pin it: pinned tabs stay at the left, in sight, and aren\'t closed with the others.', h('span', { class: 'muted' }, `${S.docs.filter(d => d.pinned).length} pinned`)],
  ['Panes and tabs', 'Their sizes, sides and order, and what is folded, as they were at first.', h('button', { class: 'btn small', onclick: () => {
    for (const k of ['sides', 'folded', 'weights', 'widths', 'left', 'right', 'bottom', 'rorder', 'split', 'results']) prefs(k, null);
    document.getElementById('left').style.width = document.getElementById('right').style.width = ''; drawViews(); pane('left', true); toast('The layout is back as it was'); } }, 'Reset the layout')],
] });
register.setting({ id: 'keys', group: 'Personal', icon: 'keyboard', order: 40, title: 'Keys', about: 'What each key does, and where.', rows: () =>
  [...new Set(R.keys.map(k => k.group))].flatMap(g => [g, ...R.keys.filter(k => k.group === g).map(k => [k.title, '', kbd(k.keys)])]) });
register.setting({ id: 'python', group: 'This node', icon: 'filepy', order: 60, title: 'Python', about: 'The Python the node runs cells, Python files and functions with.', rows: async () => {
  let v = {};
  try { v = await (await call('/python')).json(); } catch (e) { return [['Python', e.message, null]]; }
  return [['In use', v.python || (v.given === 'auto' ? 'The first found with pondra and pyarrow' : 'None: start the node with --python auto'), v.given ? h('button', { class: 'btn small', onclick: () => { dlg.close(); H.choosePython(); } }, 'Choose…') : null],
    ['Kept', 'The one chosen is kept on this machine (python.txt, beside the settings), and runs from the next start too.', null],
    ['How it runs', 'As DO LANGUAGE python: db is the connection, print shows here, a figure (matplotlib) shows as a picture, and a last expression that is a frame or a table comes back as rows. Notebooks\' cells and Python files share the page\'s Python and its variables.', null]];
} });
register.setting({ id: 'about', group: 'This node', icon: 'info', order: 70, title: 'About', about: 'This console, its node and its lake.', rows: () => {
  const s = S.info || {}, nodes = s.nodes || [];
  return [['Pondra', 'The version of the node serving this page.', h('b', {}, document.documentElement.dataset.version || '')],
    ['Lake', 'What this page reads and writes.', h('code', {}, S.db || S.lake || '')],
    nodes.length ? ['Cluster', `Led by ${s.leader}; commits so far: ${s.hwm}.`, h('span', {}, `${nodes.length} node${nodes.length === 1 ? '' : 's'}`)] : null,
    ['Settings', S.prefsHere ? 'Kept on this machine, by the node (console.json in PONDRA_CONFIG_DIR, else the system\'s folder for a program\'s settings): every lake and session opened here has them.' : 'Kept in this browser: the node can\'t keep them on its machine for a page on another.', null]];
} });

// ------------------------------------------------------------------ the window
async function body(q) {
  const secs = R.settings || [], box = h('div', { class: 's-body' });
  const rowOf = r => typeof r === 'string' ? h('div', { class: 's-sub' }, r) : r && h('div', { class: 's-row' + (r[3] ? ' wide' : '') }, h('div', { class: 's-l' }, h('div', { class: 's-n' }, r[0]), r[1] ? h('div', { class: 's-a' }, r[1]) : null), r[2] ? h('div', { class: 's-c' }, r[2]) : null);
  if (q) { // (a search: the settings whose name or words have it, from every section)
    const found = [];
    for (const s of secs) {
      const hits = (await s.rows()).filter(r => Array.isArray(r) && (r[0] + ' ' + r[1] + ' ' + s.title).toLowerCase().includes(q));
      if (hits.length) found.push(h('div', { class: 's-sub' }, s.title), ...hits.map(rowOf));
    }
    box.append(...found.length ? found : [h('div', { class: 'empty' }, 'No setting has that.')]);
    return box;
  }
  const s = secs.find(x => x.id === at) || secs[0];
  box.append(h('h4', {}, s.title), s.about ? h('p', { class: 's-about' }, s.about) : null, ...(await s.rows()).map(rowOf).filter(Boolean));
  return box;
}
async function draw() {
  if (!dlg) return;
  const q = dlg.querySelector('.s-find').value.trim().toLowerCase(), groups = [...new Set((R.settings || []).map(s => s.group || 'More'))];
  dlg.querySelector('.s-nav').replaceChildren(...groups.flatMap(g => [h('div', { class: 's-g' }, g), ...(R.settings || []).filter(s => (s.group || 'More') === g).map(s =>
    h('button', { class: 's-i' + (s.id === at && !q ? ' on' : ''), 'aria-current': s.id === at && !q ? 'page' : null, onclick: () => { at = s.id; dlg.querySelector('.s-find').value = ''; draw(); } }, h('span', { class: 'ic', html: svg(s.icon || 'settings', 16) }), s.title))]));
  const b = await body(q);
  dlg.querySelector('.s-body').replaceWith(b);
}
export function settings(section) {
  if (section) at = section;
  if (!dlg) {
    dlg = h('dialog', { class: 'settings2', 'aria-label': 'Settings' },
      h('div', { class: 's-head' }, h('span', { class: 'ic', html: svg('settings', 18) }), h('h3', {}, 'Settings'), h('input', { class: 's-find', type: 'search', placeholder: 'Search settings', 'aria-label': 'Search settings', spellcheck: 'false', oninput: () => draw() }),
        h('button', { class: 'icon', title: 'Close (Esc)', 'aria-label': 'Close', onclick: () => dlg.close() }, icon('close'))),
      h('div', { class: 's-main' }, h('nav', { class: 's-nav', 'aria-label': 'Sections' }), h('div', { class: 's-body' })));
    document.body.append(dlg);
  }
  dlg.querySelector('.s-find').value = '';
  draw();
  dlg.showModal();
}

// ------------------------------------------------------------------ signing in: a token, or a user's name and password
let asking = false;
/** The sign-in dialog, saying why it is asked for. */
export function askToken(why) {
  const d = $('#tokenDlg');
  if (d.open) return;
  if (!asking) { asking = true; d.addEventListener('close', signedIn); }
  $('#tokenWhy').textContent = `${why}; or a user's name and password. It is kept in this browser only.`;
  $('#tokenIn').value = store.get('pondra.user') ? '' : store.get('pondra.token') || '';
  $('#userIn').value = store.get('pondra.user') || '';
  d.returnValue = '';
  d.showModal();
}
async function signedIn() {
  const v = $('#tokenDlg').returnValue, user = $('#userIn').value.trim(), secret = $('#tokenIn').value.trim();
  if (v === 'clear') { store.set('pondra.token', null); store.set('pondra.user', null); }
  else if (v !== 'ok' || !secret) return;
  else if (!user) { store.set('pondra.token', secret); store.set('pondra.user', null); }
  else {
    // (a user: a session for it, which the node signs: `POST /login`)
    const r = await fetch(new URL('login', location.href), { method: 'POST', body: JSON.stringify({ user, password: secret }) });
    if (!r.ok) { toast(await r.text(), true); return askToken('Sign in again'); }
    store.set('pondra.token', (await r.json()).token); store.set('pondra.user', user);
  }
  H.drawSignin(); H.refresh();
}
