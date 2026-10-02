// The Data tree's objects, and all that can be done with them (ADR-034, round 29): a menu for each
// kind — a lake, a schema, a table, a view, a materialized view, a view of files, a column, a
// function, a procedure, a schedule, a secret. Each action is the SQL it runs: shown in a new tab
// to edit and run (a new object, a script), or run at once after asking (rename, drop). Script as
// gives each in SQL and in Python. Kinds and actions are registries (`register.objectKind`,
// `register.objectAction`): users, roles and grants (round 29, part 2), flows (round 30) and
// an extension's own come in the same way. Loaded when the Data tree is first used.
import { h, svg, S, R, run, rows, ident, quote, toast, menu, prompt, pop, confirmed, sqlType, call, moreStyle, MODE, fileUrl, fileSql, home } from './core.js';
import { highlighted } from './editor.js';
import { copyText, fetchRows, downloadItems } from './grid.js';

await moreStyle();

const H = R.helpers;

/** A new database, in a folder of lakes (`pondra serve --lakes`): made by the server, then used. */
export async function newDatabase() {
  const name = ((await prompt('New database', 'Its name (letters, digits and _)', '')) || '').trim().toLowerCase();
  if (!name) return;
  try {
    await call('/databases', { method: 'POST', body: JSON.stringify({ name }), headers: { 'content-type': 'application/json' }, root: true });
    await H.use(name);
  } catch (e) { toast(e.message, true); }
}

// ------------------------------------------------------------------ what an action does: a script in a tab, or SQL run
/** SQL (or Python) in a new tab, to read, change and run. */
async function tab(text, kind = 'sql') { const d = await H.newFile(kind); d.ed.value = text; d.changed(); d.ed.focus(); return d; }
/** Run SQL now; say what it did; read the catalog again. */
async function exec(sql, said) {
  try { await run(sql); toast(said); H.refresh(); } catch (e) { toast(e.message, true); }
}
const danger = (question, sql, said) => { if (confirmed(`${question}\n\n${sql}`)) exec(sql, said); };
/** A file picked on this computer, put in the lake's files (imports/), and read by `into(SQL that
 * reads it, its name)`: the statement in a tab, run, to see what it did. */
function importFile(into) {
  const input = h('input', { type: 'file', hidden: true, accept: '.csv,.tsv,.json,.jsonl,.ndjson,.parquet' });
  input.onchange = async () => {
    const f = input.files[0], rel = `imports/${Date.now().toString(36)}-${f?.name.replace(/[^\w.-]/g, '_')}`;
    input.remove();
    if (!f) return;
    try { await call(fileUrl(rel), { method: 'PUT', body: f }); } catch (e) { toast(`${f.name}: ${e.message}`, true); return; }
    H.refreshFiles?.();
    (await tab(into(fileSql(rel), f.name))).run();
  };
  document.body.append(input); input.click();
}
const name = async (title, label, value) => ((await prompt(title, label, value)) || '').trim();
const py = s => /"""/.test(s) ? `'''\n${s}\n'''` : `"""\n${s}\n"""`;

/** A value to start a column with, by its type (an INSERT's template). */
const sample = d => /^(U?Int|Float|Decimal)/.test(d) ? '0' : /^Boolean/.test(d) ? 'false' : /^Date/.test(d) ? "DATE '2026-01-01'" : /^Timestamp/.test(d) ? "TIMESTAMP '2026-01-01 00:00:00'" : /^(Utf8|LargeUtf8|Utf8View)/.test(d) ? "''" : 'NULL';
const cols = t => t.columns.filter(c => !c.n.startsWith('_') || !/^_(row_id|version|created_at|updated_at|deleted)$/.test(c.n));
const keyOf = t => t.o.key?.length ? t.o.key : [cols(t)[0]?.n].filter(Boolean);

/** A table's CREATE, from what the catalog says of it (its columns, key, defaults and options). */
function ddl(t) {
  const o = t.o;
  if (o.kind === 'view' || o.kind === 'files') return `CREATE OR REPLACE VIEW ${t.q} AS\n${o.sql || 'SELECT …'};`;
  if (o.kind === 'materialized view') return `CREATE MATERIALIZED VIEW ${t.q} AS\n${o.sql || 'SELECT …'};`;
  const nn = new Set(o.not_null || []), key = o.key || [];
  const lines = cols(t).map(c => `  ${ident(c.n)} ${sqlType(c.d)}${nn.has(c.n) && !key.includes(c.n) ? ' NOT NULL' : ''}${o.defaults?.[c.n] ? ` DEFAULT ${o.defaults[c.n]}` : ''}`);
  if (key.length) lines.push(`  PRIMARY KEY (${key.map(ident).join(', ')})`);
  const opts = [o.partition && `partition_by = '${o.partition}'`, o.cluster?.length && `cluster_by = '${o.cluster.join(', ')}'`, o.publish?.length && `publish = '${o.publish.join(',')}'`,
    o.ttl_secs && `ttl = '${o.ttl_secs[0]}:${o.ttl_secs[1]}'`, o.order_by && `order_by = '${o.order_by}'`, Object.keys(o.merge || {}).length && `merge = '${Object.entries(o.merge).map(([c, f]) => `${c}:${f}`).join(', ')}'`].filter(Boolean);
  return `CREATE TABLE ${t.q} (\n${lines.join(',\n')}\n)${opts.length ? ` WITH (${opts.join(', ')})` : ''};`;
}
/** Every script of a table or view, in SQL and in Python: `[label, sql, python]`. */
function scripts(t) {
  const cs = cols(t), names = cs.map(c => ident(c.n)), key = keyOf(t), rest = cs.filter(c => !key.includes(c.n)), table = t.o.kind === 'table';
  const where = key.map(k => `${ident(k)} = ${sample(cs.find(c => c.n === k)?.d || '')}`).join(' AND ');
  const list = [['SELECT', `SELECT ${names.join(', ')}\nFROM ${t.q}\nLIMIT 100;`, `db.table("${t.q}").limit(100).to_pandas()`]];
  if (table) {
    const values = `(${cs.map(c => sample(c.d)).join(', ')})`;
    list.push(['INSERT', `INSERT INTO ${t.q} (${names.join(', ')}) VALUES\n  ${values};`, `db.append("${t.q}", [${'{' + cs.map(c => `"${c.n}": ${sample(c.d).replace(/^(DATE|TIMESTAMP) /, '').replace(/^false$/, 'False').replace(/^NULL$/, 'None')}`).join(', ') + '}'}])`]);
    if (t.o.key?.length) list.push(['Upsert', `INSERT INTO ${t.q} (${names.join(', ')}) VALUES\n  ${values}\nON CONFLICT (${key.map(ident).join(', ')}) DO UPDATE SET ${rest.map(c => `${ident(c.n)} = excluded.${ident(c.n)}`).join(', ') || `${names[0]} = excluded.${names[0]}`};`]);
    list.push(['UPDATE', `UPDATE ${t.q}\nSET ${rest.slice(0, 2).map(c => `${ident(c.n)} = ${sample(c.d)}`).join(', ') || `${names[0]} = ${names[0]}`}\nWHERE ${where};`],
      ['DELETE', `DELETE FROM ${t.q}\nWHERE ${where};`],
      ['MERGE', `MERGE INTO ${t.q} AS t\nUSING (SELECT ${cs.map(c => `${sample(c.d)} AS ${ident(c.n)}`).join(', ')}) AS s\nON ${key.map(k => `t.${ident(k)} = s.${ident(k)}`).join(' AND ')}\nWHEN MATCHED THEN UPDATE SET ${rest.map(c => `${ident(c.n)} = s.${ident(c.n)}`).join(', ') || `${names[0]} = s.${names[0]}`}\nWHEN NOT MATCHED THEN INSERT (${names.join(', ')}) VALUES (${names.map(n => 's.' + n).join(', ')});`],
      ['Load a file', `INSERT INTO ${t.q}\nSELECT * FROM read_csv('${S.filesAt || 'files/'}data/new.csv');`],
      ['Export', `COPY ${t.q} TO 's3://bucket/folder/' (FORMAT parquet);`]);
  }
  list.push(['CREATE', ddl(t), t.o.kind === 'view' ? `db.view("${t.q}", ${py(t.o.sql || 'SELECT …')})` : t.o.kind === 'materialized view' ? `db.view("${t.q}", ${py(t.o.sql || 'SELECT …')}, materialized=True)` : null],
    ['DROP', `DROP ${{ view: 'VIEW', files: 'VIEW', 'materialized view': 'MATERIALIZED VIEW' }[t.o.kind] || 'TABLE'} ${t.q};`]);
  return list.map(([label, sql, p]) => [label, sql, p || `db.sql(${py(sql)})`]);
}
/** Script as: each script of it, SQL or Python, shown; opened in a tab, or copied. */
export function scriptAs(t, first = 'SELECT') {
  const list = scripts(t), st = { at: Math.max(0, list.findIndex(x => x[0] === first)), lang: 'sql' };
  const kinds = h('div', { class: 'sa-k' }), code = h('pre', { class: 'defn sa-c' }), langs = h('span', { class: 'segs' });
  const draw = () => {
    kinds.replaceChildren(...list.map(([label], i) => h('button', { class: 'sa-i' + (i === st.at ? ' on' : ''), onclick: () => { st.at = i; draw(); } }, label)));
    langs.replaceChildren(...[['sql', 'SQL'], ['python', 'Python']].map(([k, label]) => h('button', { class: 'seg' + (st.lang === k ? ' on' : ''), onclick: () => { st.lang = k; draw(); } }, label)));
    code.innerHTML = highlighted(text(), st.lang);
  };
  const text = () => list[st.at][st.lang === 'sql' ? 1 : 2];
  draw();
  pop(`Script ${t.q}`, h('div', { class: 'sa' }, h('div', { class: 'cbar' }, langs, h('span', { class: 'muted' }, 'As SQL, or in Python: the same, from the client')), h('div', { class: 'sa-b' }, kinds, code)),
    [['Open in a new tab', () => tab(text(), st.lang), true], ['Copy', () => copyText(text())]]);
}

// ------------------------------------------------------------------ the menus
/** What an extension adds to a kind's menu (`register.objectAction`). */
const more = (kind, x) => { const a = (R.objectActions || []).filter(o => o.kinds.includes(kind)); return a.length ? ['-', ...a.map(o => ({ label: o.label, icon: o.icon, run: () => o.run(x) }))] : []; };
/** A second menu where the first was: a menu's item that has choices of its own. */
const then = (at, items) => () => requestAnimationFrame(() => menu(at, items));

export function tableMenu(at, t) {
  const kind = t.o.kind, table = kind === 'table', word = { view: 'VIEW', files: 'VIEW', 'materialized view': 'MATERIALIZED VIEW' }[kind] || 'TABLE';
  menu(at, [{ label: 'Preview', icon: 'play', keys: 'Double-click', run: () => H.query(`SELECT * FROM ${t.q} LIMIT 100`) },
    { label: 'Preview in Python', icon: 'filepy', run: () => tab(`db.table("${t.q}").limit(100)`, 'python') },
    { label: 'Details and data profile', icon: 'eye', run: () => H.pick({ type: 'object', t }) },
    { label: 'Watch it live', icon: 'refresh', run: () => H.query(`SELECT * FROM ${t.q} LIMIT 100`, true) }, '-',
    { label: 'Script as…', icon: 'filesql', run: () => scriptAs(t) },
    table ? { label: 'Insert rows…', run: () => tab(scripts(t).find(x => x[0] === 'INSERT')[1]) } : null,
    table ? { label: 'Import rows from a file…', icon: 'up', run: () => importFile((src, n) => { const c = cols(t).map(c => ident(c.n)).join(', '); return `-- ${n}'s rows into ${t.q}, its columns by name\nINSERT INTO ${t.q} (${c})\nSELECT ${c}\nFROM ${src};`; }) } : null,
    { label: 'Download all rows…', icon: 'down', run: then(at, downloadItems(f => fetchRows({ sql: `SELECT * FROM ${t.q}` }, f, t.t), 'All rows, as')) },
    kind === 'table' || kind === 'materialized view' ? { label: 'As a Kafka topic…', icon: 'terminal', run: () => kafka(t) } : null, '-',
    table ? { label: 'Add a column…', icon: 'plus', run: async () => { const c = await name('Add a column', `A column of ${t.q}: its name and type`, 'note VARCHAR'); if (c) exec(`ALTER TABLE ${t.q} ADD COLUMN ${c}`, `Added ${c.split(/\s/)[0]} to ${t.q}`); } } : null,
    kind !== 'materialized view' ? { label: 'Rename…', icon: 'pencil', run: async () => { const n = await name('Rename', `The new name of ${t.q}`, t.t); if (n && n !== t.t) exec(`ALTER ${word === 'VIEW' ? 'VIEW' : 'TABLE'} ${t.q} RENAME TO ${ident(n)}`, `Renamed to ${n}`); } } : null,
    { label: 'Copy the name', icon: 'copy', run: () => copyText(t.q, `Copied ${t.q}`) }, { label: 'Copy the column names', run: () => copyText(cols(t).map(c => ident(c.n)).join(', '), 'Copied the column names') }, { label: 'Copy as Python', run: () => copyText(`db.table("${t.q}")`, 'Copied') }, '-',
    table ? { label: 'Truncate…', run: () => danger(`Delete every row of ${t.q}? This can't be undone.`, `TRUNCATE TABLE ${t.q}`, `Emptied ${t.q}`) } : null,
    { label: 'Drop…', icon: 'trash', run: () => danger(`Drop ${t.q}?${kind === 'files' ? ' (its files stay)' : ' This can\'t be undone.'}`, `DROP ${word} ${t.q}`, `Dropped ${t.q}`) }, ...more(kind === 'files' ? 'view' : kind.replace(' ', '_'), t)]);
}
export function columnMenu(at, t, c) {
  const col = ident(c.n), table = t.o.kind === 'table';
  menu(at, [{ label: 'Copy the name', icon: 'copy', run: () => copyText(c.n, `Copied ${c.n}`) }, { label: 'Put it where I type', run: () => S.doc?.put?.(col) }, '-',
    { label: 'Its values, counted', icon: 'play', run: () => H.query(`SELECT ${col}, count(*) AS n\nFROM ${t.q}\nGROUP BY ${col}\nORDER BY n DESC\nLIMIT 100`) },
    { label: 'Its range and NULLs', run: () => H.query(`SELECT min(${col}) AS least, max(${col}) AS greatest, count(*) - count(${col}) AS nulls, count(DISTINCT ${col}) AS distinct_values\nFROM ${t.q}`) },
    table ? '-' : null,
    table ? { label: 'Rename…', icon: 'pencil', run: async () => { const n = await name('Rename a column', `The new name of ${c.n}`, c.n); if (n && n !== c.n) exec(`ALTER TABLE ${t.q} RENAME COLUMN ${col} TO ${ident(n)}`, `Renamed to ${n}`); } } : null,
    table ? { label: 'Change its type…', run: async () => { const ty = await name('Change the type', `${c.n} is ${sqlType(c.d)}: a wider type (INT to BIGINT, REAL to DOUBLE)`, sqlType(c.d)); if (ty) exec(`ALTER TABLE ${t.q} ALTER COLUMN ${col} TYPE ${ty}`, `${c.n} is ${ty}`); } } : null,
    table && !(t.o.key || []).includes(c.n) ? { label: 'Drop…', icon: 'trash', run: () => danger(`Drop the column ${c.n} of ${t.q}?`, `ALTER TABLE ${t.q} DROP COLUMN ${col}`, `Dropped ${c.n}`) } : null, ...more('column', { t, c })]);
}
/** New objects: each a script in a new tab, to fill in and run. */
const NEW = {
  table: q => `CREATE TABLE ${q}new_table (\n  id BIGINT PRIMARY KEY,            -- a key: rows upserted by it (leave it out for an append table)\n  name VARCHAR NOT NULL,\n  amount DOUBLE DEFAULT 0,\n  at TIMESTAMP\n);\n-- options: ) WITH (partition_by = 'day(at)', cluster_by = 'name', publish = 'delta,iceberg', ttl = 'at:86400')`,
  view: q => `CREATE VIEW ${q}new_view AS\nSELECT …\nFROM …;`,
  'materialized view': q => `-- kept up to date as its table changes; a window: WITH (window = 'at', size_secs = 60)\nCREATE MATERIALIZED VIEW ${q}new_view AS\nSELECT key, count(*) AS n, sum(amount) AS total\nFROM …\nGROUP BY key;`,
  'external table': q => `CREATE EXTERNAL TABLE ${q}new_files\nSTORED AS PARQUET\nLOCATION 's3://bucket/folder/';`,
  schema: () => 'CREATE SCHEMA new_schema;',
  function: (q = '') => `CREATE FUNCTION ${q}add_tax(amount DOUBLE, rate DOUBLE DEFAULT 0.2) RETURNS DOUBLE\nRETURN amount * (1 + rate);\n\n-- in Python (vectorized: a pyarrow array in, one out)\n-- CREATE FUNCTION shout(s VARCHAR) RETURNS VARCHAR LANGUAGE python AS $$\n-- return s.upper()\n-- $$;`,
  procedure: (q = '') => `CREATE PROCEDURE ${q}refresh_totals(day DATE) LANGUAGE sql AS $$\n  DELETE FROM totals WHERE day = $day;\n  INSERT INTO totals SELECT $day, sum(amount) FROM orders WHERE CAST(at AS DATE) = $day;\n$$;\n\nCALL ${q}refresh_totals(current_date);`,
  schedule: (q = '') => `CREATE TASK ${q}nightly SCHEDULE 'cron 0 2 * * * UTC'\nAS CALL run('queries/nightly.sql');`,
  secret: () => `-- its values are sealed; only a procedure's code reads them (pondra.secret)\nCREATE SECRET my_bucket (TYPE s3, KEY_ID '…', SECRET '…', REGION 'eu-west-1', SCOPE 's3://my-bucket');`,
  attach: () => `-- another lake, a Delta or Iceberg folder or catalog, or a Kafka cluster, as a catalog of tables\nATTACH 's3://bucket/warehouse' AS wh (TYPE delta);`,
};
const created = (kind, q = '') => ({ label: `New ${kind}…`, icon: 'plus', run: () => tab(NEW[kind](q)) });
export function schemaMenu(at, lake, schema) {
  const q = `${ident(schema)}.`;
  menu(at, [created('table', q), created('view', q), created('materialized view', q), created('external table', q),
    { label: 'New table from a file…', icon: 'up', run: () => importFile((src, n) => `-- a table of ${n}'s rows\nCREATE TABLE ${q}${ident(n.replace(/\.\w+$/, '').toLowerCase().replace(/\W+/g, '_').replace(/^(\d)/, '_$1'))} AS\nSELECT * FROM ${src};`) }, '-',
    created('function', q), created('procedure', q), created('schedule', q), '-',
    { label: 'Copy the name', icon: 'copy', run: () => copyText(schema, `Copied ${schema}`) },
    { label: 'Copy the table names', run: () => copyText(S.objects.filter(t => t.c === lake && t.s === schema).map(t => t.q).join('\n'), 'Copied the table names') },
    { label: 'Drop…', icon: 'trash', run: () => danger(`Drop the schema ${schema}, and every table and view in it?`, `DROP SCHEMA ${ident(schema)} CASCADE`, `Dropped ${schema}`) }, ...more('schema', { lake, schema })]);
}
export function lakeMenu(at, lake, current) {
  menu(at, [current ? created('schema') : null, current ? created('table') : null, current ? { label: 'Attach a lake or catalog…', icon: 'db', run: () => tab(NEW.attach()) } : null, '-',
    MODE === 'lakes' ? { label: 'New database…', icon: 'plus', run: () => H.newDatabase() } : null,
    current ? { label: 'Checkpoint', run: () => exec('CHECKPOINT', 'Checkpointed: what was in the log is in the tables\' files') } : null,
    { label: 'Refresh', icon: 'refresh', run: () => H.refresh() }, { label: 'Copy the name', icon: 'copy', run: () => copyText(lake, `Copied ${lake}`) },
    !current ? { label: 'Detach…', icon: 'trash', run: () => danger(`Detach ${lake}? Its data stays where it is.`, `DETACH ${ident(lake)}`, `Detached ${lake}`) } : null,
    MODE === 'lakes' && current ? { label: 'Drop the database…', icon: 'trash', run: () => danger(`Drop the database ${lake}, its folder and every table in it? This can't be undone.`, `DROP DATABASE ${ident(lake)}`, `Dropped ${lake}`) } : null, ...more('lake', lake)]);
}

/** The database pill's menu (a SQL file's toolbar): a server's databases to run in, or one by name;
 * a lake and those attached (a node runs in its lake: an attached one's tables are `name.table`);
 * its name copied. */
export function dbMenu(at) {
  const now = home(), copy = { label: 'Copy name', icon: 'copy', hint: now, run: () => copyText(now, 'Name copied') };
  if (MODE === 'lakes') return menu(at, [{ head: 'Run in' }, ...(S.dbs || []).map(d => ({ label: d.name, checked: d.name === now, hint: d.running && d.name !== now ? 'running' : null, run: () => d.name !== now && H.use(d.name) })), '-',
    { label: 'Another, by name…', icon: 'search', run: byName }, { label: 'New database…', icon: 'plus', run: () => H.newDatabase() }, copy]);
  const attached = (S.lakes || []).filter(n => n !== now);
  menu(at, [{ head: 'Runs in this lake' }, { label: now, checked: true, run: () => {} }, attached.length ? { head: 'Attached: click to put its name in' } : null,
    ...attached.map(n => ({ label: n, icon: 'db', run: () => S.doc?.put?.(ident(n) + '.') })), '-', copy]);
}
async function byName() {
  const name = ((await prompt('Run in', 'The database\'s name', '')) || '').trim().toLowerCase();
  if (!name || name === home()) return;
  if ((S.dbs || []).some(d => d.name === name)) return H.use(name);
  toast(`No database ${name}: New database… makes one`, true);
}

// ------------------------------------------------------------------ the lake's other objects: routines, schedules, secrets
const fnSql = x => `CREATE OR REPLACE ${x.kind === 'procedure' ? 'PROCEDURE' : x.kind === 'macro' ? 'MACRO' : 'FUNCTION'} ${x.name}(${x.arguments || ''})${x.returns ? ` RETURNS ${x.returns}` : ''}${x.language && x.language !== 'sql' ? ` LANGUAGE ${x.language}` : ''} AS $$\n${x.body}\n$$;`;
const drop = (what, x) => ({ label: 'Drop…', icon: 'trash', run: () => danger(`Drop the ${what.toLowerCase()} ${x.name}?`, `DROP ${what} ${ident(x.name)}`, `Dropped ${x.name}`) });
const show = (title, sql, extra = []) => ({ label: 'Its definition', icon: 'eye', run: () => pop(title, h('pre', { class: 'defn', html: highlighted(sql, 'sql') }), [['Open in a new tab', () => tab(sql), true], ['Copy', () => copyText(sql)], ...extra]) });
const KINDS = {
  functions: {
    list: () => rows("SELECT name, kind, language, arguments, returns, body FROM pondra.routines WHERE kind <> 'procedure' ORDER BY name"),
    item: x => ({ name: x.name, icon: 'fn', meta: x.kind === 'function' ? x.language : x.kind, title: `${x.name}(${x.arguments || ''})${x.returns ? ' → ' + x.returns : ''}` }),
    menu: x => [{ label: 'Use it', icon: 'play', run: () => tab(x.kind === 'table function' ? `SELECT * FROM ${x.name}(${x.arguments ? '…' : ''});` : `SELECT ${x.name}(${x.arguments ? '…' : ''});`) },
      show(x.name, fnSql(x)), { label: 'Copy the name', icon: 'copy', run: () => copyText(x.name) }, '-', drop(x.kind === 'macro' ? 'MACRO' : 'FUNCTION', x)] },
  procedures: {
    list: () => rows("SELECT name, kind, language, arguments, returns, body FROM pondra.routines WHERE kind = 'procedure' ORDER BY name"),
    item: x => ({ name: x.name, icon: 'play', meta: x.language, title: `${x.name}(${x.arguments || ''})` }),
    menu: x => [{ label: 'Call…', icon: 'play', run: () => tab(`CALL ${x.name}(${x.arguments ? '…' : ''});`) },
      { label: 'Start as a job…', run: () => tab(`SELECT pondra.start('${x.name}'${x.arguments ? ', …' : ''});`) },
      { label: 'Schedule…', icon: 'clock', run: () => tab(`CREATE TASK ${x.name}_daily SCHEDULE '1 day'\nAS CALL ${x.name}(${x.arguments ? '…' : ''});`) },
      show(x.name, fnSql(x)), { label: 'Its runs', icon: 'clock', run: () => H.show('runs') }, '-', drop('PROCEDURE', x)] },
  schedules: {
    list: () => rows('SELECT name, schedule, statement, next_tick FROM pondra.tasks ORDER BY name'),
    item: x => ({ name: x.name, icon: 'calendar', meta: x.schedule, title: x.statement }),
    menu: x => [{ label: 'In Jobs', icon: 'calendar', run: () => H.show('jobs') }, show(x.name, `CREATE OR REPLACE TASK ${x.name} SCHEDULE ${quote(x.schedule)}\nAS ${x.statement};`), '-', drop('TASK', x)] },
  secrets: {
    list: () => rows('SELECT * FROM secrets() ORDER BY name'),
    item: x => ({ name: x.name, icon: 'key', meta: x.type, title: x.scope ? `for ${x.scope}` : x.type }),
    menu: x => [{ label: 'Replace its values…', icon: 'pencil', run: () => tab(`CREATE OR REPLACE SECRET ${x.name} (TYPE ${x.type}, KEY_ID '…', SECRET '…'${x.scope ? `, SCOPE ${quote(x.scope)}` : ''});`) }, '-', drop('SECRET', x)] },
};
/** A group's objects under the lake, drawn when it is opened. */
export async function fill(kind, box, schema) {
  const k = { ...R.objectKinds.find(x => x.id === kind), ...KINDS[kind] }, of = n => n.includes('.') ? n.slice(0, n.indexOf('.')) : 'public';
  let list;
  try { list = await k.list(); } catch (e) { box.replaceChildren(h('div', { class: 'empty' }, e.message.split('\n')[0])); return; }
  if (schema) list = list.filter(x => of(k.item(x).name) === schema); // (a schema's: `sales.f` in sales, `f` in public)
  box.replaceChildren(...list.length ? list.map(x => {
    const it = k.item(x), open = e => menu(e.currentTarget || e, [...k.menu(x), ...more(kind, x)]);
    return h('div', { class: 'row obj', role: 'treeitem', tabindex: '-1', 'aria-level': schema ? '4' : '3', title: it.title, style: `padding-left:${schema ? 48 : 34}px`, onclick: e => open(e), oncontextmenu: e => { e.preventDefault(); open(e); } },
      h('span', { class: 'tw none' }), h('span', { class: 'ic', html: svg(it.icon, 15) }), h('span', { class: 'nm' }, schema ? it.name.slice(it.name.indexOf('.') + 1) : it.name), it.meta ? h('span', { class: 'meta' }, it.meta) : null);
  }) : [h('div', { class: 'empty', style: `padding:2px 8px 4px ${schema ? 66 : 52}px`, title: 'The group\'s ⋯, or a right-click on it, makes one' }, 'None yet')]); // (under the group's name)
}
export function groupMenu(at, kind, schema) {
  const k = R.objectKinds.find(x => x.id === kind), make = { functions: 'function', procedures: 'procedure', schedules: 'schedule', secrets: 'secret' }[kind];
  menu(at, [make ? created(make, schema && schema !== 'public' ? `${ident(schema)}.` : '') : null, k?.create ? { label: `New ${k.title.toLowerCase()}…`, icon: 'plus', run: () => tab(k.create()) } : null, { label: 'Refresh', icon: 'refresh', run: () => H.refresh() }, ...more(kind, null)]);
}

/** A table as a Kafka topic: where producers and consumers connect, and how. */
async function kafka(t) {
  let me = null;
  try { me = await (await call('/cluster/kafka')).json(); } catch { /* (no door) */ }
  const topic = t.s === 'public' ? t.t : `${t.s}.${t.t}`, at = me?.port ? `${me.host === '0.0.0.0' ? location.hostname : me.host}:${me.port}` : null;
  const code = at ? `from confluent_kafka import Producer, Consumer\nimport json\n\np = Producer({"bootstrap.servers": "${at}", "enable.idempotence": True})\np.produce("${topic}", value=json.dumps({${cols(t).slice(0, 3).map(c => `"${c.n}": ${sample(c.d).replace(/^(DATE|TIMESTAMP) /, '').replace(/^false$/, 'False').replace(/^NULL$/, 'None')}`).join(', ')}}))\np.flush()\n\nc = Consumer({"bootstrap.servers": "${at}", "group.id": "mine", "auto.offset.reset": "earliest"})\nc.subscribe(["${topic}"])\nprint(c.poll(5))` : '';
  pop(`${t.q} as a Kafka topic`, h('div', {}, h('p', { class: 'muted' }, at ? `Every table is a topic: producers append rows (a JSON row a record), consumers read them in order. One partition; tokens as SASL/PLAIN (the role as the user name, its token the password).` : 'This node has no Kafka door: start it with --kafka 0.0.0.0:9092.'),
    at ? h('dl', { class: 'facts' }, h('dt', {}, 'Bootstrap'), h('dd', {}, h('code', {}, at)), h('dt', {}, 'Topic'), h('dd', {}, h('code', {}, topic))) : null, at ? h('pre', { class: 'defn', html: highlighted(code, 'python') }) : null),
    at ? [['Copy the code', () => copyText(code), true], ['Copy the address', () => copyText(at)]] : []);
}
