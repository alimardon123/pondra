// The Data tree's other objects (ADR-034): functions, procedures, schedules and secrets, and an
// extension's kinds (`register.objectKind`), each a group under the lake listed when opened; a click
// shows one's details, a right-click its menu. Loaded when a group is first opened (objects.js has
// the tables', schemas' and lakes' menus).
import { h, svg, S, R, rows, ident, quote, menu, pop } from './core.js';
import { highlighted } from './editor.js';
import { copyText } from './grid.js';
import { tab, danger, more, created } from './objects.js';

const H = R.helpers;
const fnSql = x => `CREATE OR REPLACE ${x.kind === 'procedure' ? 'PROCEDURE' : x.kind === 'macro' ? 'MACRO' : 'FUNCTION'} ${x.name}(${x.arguments || ''})${x.returns ? ` RETURNS ${x.returns}` : ''}${x.language && x.language !== 'sql' ? ` LANGUAGE ${x.language}` : ''} AS $$\n${x.body}\n$$;`;
const drop = (what, x) => ({ label: 'Drop…', icon: 'trash', run: () => danger(`Drop the ${what.toLowerCase()} ${x.name}?`, `DROP ${what} ${ident(x.name)}`, `Dropped ${x.name}`) });
const show = (title, sql, extra = []) => ({ label: 'Its definition', icon: 'eye', run: () => pop(title, h('pre', { class: 'defn', html: highlighted(sql, 'sql') }), [['Open in a new tab', () => tab(sql), true], ['Copy', () => copyText(sql)], ...extra]) });
const KINDS = {
  functions: {
    list: () => rows("SELECT name, kind, language, arguments, returns, body FROM pondra.routines WHERE kind <> 'procedure' ORDER BY name"),
    item: x => ({ name: x.name, icon: 'fn', meta: x.kind === 'function' ? x.language : x.kind, title: `${x.name}(${x.arguments || ''})${x.returns ? ' → ' + x.returns : ''}`, word: `a ${x.kind}` }),
    facts: x => [['Language', x.language], ['Arguments', x.arguments || 'none'], ['Returns', x.returns]], sql: fnSql,
    menu: x => [{ label: 'Use it', icon: 'play', run: () => tab(x.kind === 'table function' ? `SELECT * FROM ${x.name}(${x.arguments ? '…' : ''});` : `SELECT ${x.name}(${x.arguments ? '…' : ''});`) },
      show(x.name, fnSql(x)), { label: 'Copy the name', icon: 'copy', run: () => copyText(x.name) }, '-', drop(x.kind === 'macro' ? 'MACRO' : 'FUNCTION', x)] },
  procedures: {
    list: () => rows("SELECT name, kind, language, arguments, returns, body FROM pondra.routines WHERE kind = 'procedure' ORDER BY name"),
    item: x => ({ name: x.name, icon: 'play', meta: x.language, title: `${x.name}(${x.arguments || ''})`, word: 'a procedure' }),
    facts: x => [['Language', x.language], ['Arguments', x.arguments || 'none']], sql: fnSql,
    menu: x => [{ label: 'Call…', icon: 'play', run: () => tab(`CALL ${x.name}(${x.arguments ? '…' : ''});`) },
      { label: 'Start as a job…', run: () => tab(`SELECT pondra.start('${x.name}'${x.arguments ? ', …' : ''});`) },
      { label: 'Schedule…', icon: 'clock', run: () => tab(`CREATE TASK ${x.name}_daily SCHEDULE '1 day'\nAS CALL ${x.name}(${x.arguments ? '…' : ''});`) },
      show(x.name, fnSql(x)), { label: 'Its runs', icon: 'clock', run: () => H.show('runs') }, '-', drop('PROCEDURE', x)] },
  schedules: {
    list: () => rows('SELECT name, schedule, statement, next_tick FROM pondra.tasks ORDER BY name'),
    item: x => ({ name: x.name, icon: 'calendar', meta: x.schedule, title: x.statement, word: 'a schedule' }),
    facts: x => [['Every', x.schedule], ['Next', x.next_tick ? new Date(x.next_tick).toLocaleString() : null]], sql: x => `CREATE OR REPLACE TASK ${x.name} SCHEDULE ${quote(x.schedule)}\nAS ${x.statement};`,
    menu: x => [{ label: 'In Jobs', icon: 'calendar', run: () => H.show('jobs') }, show(x.name, KINDS.schedules.sql(x)), '-', drop('TASK', x)] },
  secrets: {
    list: () => rows('SELECT * FROM secrets() ORDER BY name'),
    item: x => ({ name: x.name, icon: 'key', meta: x.type, title: x.scope ? `for ${x.scope}` : x.type, word: 'a secret (its values never show)' }),
    facts: x => [['Type', x.type], ['For', x.scope]],
    menu: x => [{ label: 'Replace its values…', icon: 'pencil', run: () => tab(`CREATE OR REPLACE SECRET ${x.name} (TYPE ${x.type}, KEY_ID '…', SECRET '…'${x.scope ? `, SCOPE ${quote(x.scope)}` : ''});`) }, '-', drop('SECRET', x)] },
};
/** A group's objects under the lake, drawn when it is opened. */
export async function fill(kind, box, schema) {
  const k = { ...R.objectKinds.find(x => x.id === kind), ...KINDS[kind] }, of = n => n.includes('.') ? n.slice(0, n.indexOf('.')) : 'public';
  let list;
  try { list = await k.list(); } catch (e) { box.replaceChildren(h('div', { class: 'empty' }, e.message.split('\n')[0])); return; }
  if (schema) list = list.filter(x => of(k.item(x).name) === schema); // (a schema's: `sales.f` in sales, `f` in public)
  box.replaceChildren(...list.length ? list.map(x => {
    const it = k.item(x), items = () => [...k.menu(x), ...more(kind, x)], open = e => menu(e.currentTarget || e, items()), key = `item:${kind}:${it.name}`;
    // (a click: its details, as a table's; a right-click: what can be done with it)
    const picked = () => H.pick({ type: 'item', key, render: () => detail(it, k, x, items) });
    return h('div', { class: 'row obj', role: 'treeitem', tabindex: '-1', 'aria-level': schema ? '4' : '3', 'data-key': key, title: it.title, style: `padding-left:${schema ? 48 : 34}px`, onclick: picked, onkeydown: e => e.key === 'Enter' && picked(), oncontextmenu: e => { e.preventDefault(); open(e); } },
      h('span', { class: 'tw none' }), h('span', { class: 'ic', html: svg(it.icon, 15) }), h('span', { class: 'nm' }, schema ? it.name.slice(it.name.indexOf('.') + 1) : it.name), it.meta ? h('span', { class: 'meta' }, it.meta) : null);
  }) : [h('div', { class: 'empty', style: `padding:2px 8px 4px ${schema ? 66 : 52}px`, title: 'The group\'s ⋯, or a right-click on it, makes one' }, 'None yet')]); // (under the group's name)
}
/** An object's details (a function, a procedure, a schedule, a secret, an extension's kind): what it
 * is, its first actions as buttons and the rest in More, its facts, its definition. */
function detail(it, k, x, items) {
  const list = items().filter(m => m && m !== '-' && !m.head), first = list.filter(m => !/^(Drop|Its definition)/.test(m.label)).slice(0, 3); // (the definition is shown below)
  const moreBtn = H.act('dots', 'More', 'Everything that can be done with it', e => menu(e.currentTarget, items())), sql = k.sql?.(x);
  return [H.head(it.icon, it.name, it.word || k.title || ''), h('div', { class: 'acts2' }, ...first.map(m => H.act(m.icon, m.label.replace(/…$/, ''), m.label, m.run)), moreBtn),
    k.facts ? H.facts(k.facts(x)) : null, sql ? h('div', { class: 'dsect' }, 'Definition') : null, sql ? h('pre', { class: 'defn', html: highlighted(sql, 'sql') }) : null];
}
export function groupMenu(at, kind, schema) {
  const k = R.objectKinds.find(x => x.id === kind), make = { functions: 'function', procedures: 'procedure', schedules: 'schedule', secrets: 'secret' }[kind];
  menu(at, [make ? created(make, schema && schema !== 'public' ? `${ident(schema)}.` : '') : null, k?.create ? { label: `New ${k.title.toLowerCase()}…`, icon: 'plus', run: () => tab(k.create()) } : null, { label: 'Refresh', icon: 'refresh', run: () => H.refresh() }, ...more(kind, null)]);
}
