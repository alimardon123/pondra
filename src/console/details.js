// The details of a table, a view, a file or an answer (ADR-034, round 29), and a data profile (a
// table's, an answer's): loaded the first time something is picked, not with the page. What they
// use of the shell comes through `R.helpers`.
import { h, count, bytes, utc, icon, typeMark, sqlType, fileSql, S, R, run, rows, ident, toast, numeric, moreStyle, home, MODE } from './core.js';
import { highlighted } from './editor.js';
import { spread, summarize } from './grid.js';
import { iconOf, kindOf, download } from './files.js';

await moreStyle();

const H = R.helpers, { KIND, addDoc, closeDoc, query, act, facts, head, openFile } = H;

/** A column's summary as a line of numbers and a histogram (or its most common values). */
export function statView(s) {
  const pct = n => s.n ? `${(100 * n / s.n).toFixed(n && n < s.n / 100 ? 1 : 0)}%` : '0%';
  const val = v => { const x = typeof v === 'object' ? JSON.stringify(v) : String(v); return x.length > 28 ? x.slice(0, 27) + '…' : x; };
  const kids = [h('div', { class: 'nums' }, h('span', {}, `${pct(s.nulls)} null`), h('span', {}, `${s.exact ? '' : '≈ '}${count(s.distinct)} distinct`), s.min != null ? h('span', {}, `${val(s.min)} … ${val(s.max)}`) : null)];
  if (s.hist) {
    const top = Math.max(...s.hist, 1), w = 12, g = 2;
    kids.push(h('div', { html: `<svg width="100%" height="30" preserveAspectRatio="none" viewBox="0 0 ${s.hist.length * (w + g)} 30" role="img" aria-label="histogram">${s.hist.map((c, i) => `<rect x="${i * (w + g)}" y="${30 - Math.max(c ? 2 : 0, 30 * c / top)}" width="${w}" height="${Math.max(c ? 2 : 0, 30 * c / top)}" rx="1.5" fill="var(--accent)" opacity=".75"><title>${count(c)}</title></rect>`).join('')}</svg>` }));
  } else if (s.top?.length) {
    const top = s.top[0].n;
    kids.push(h('div', { class: 'bars' }, s.top.flatMap(t => [h('span', { class: 'v', title: t.v }, t.v), h('span', {}, h('div', { class: 'b', style: `width:${Math.max(4, 100 * t.n / top)}%` })), h('span', { class: 'n' }, count(t.n))])));
  }
  return kids;
}

/** A data profile where the rows are (`from`: a table, or an answer's statement in brackets), of
 * `cols` (`[{ n, d }]`): one pass for every column's counts and range, then a histogram or the
 * commonest values of each (at most 24 columns), each shown in its box's `.ps` as it comes. */
const iso = v => typeof v === 'string' ? v.replace(/^(\d{4}-\d\d-\d\d)T(\d)/, '$1 $2') : v; // (a time as the grid shows it: a space, not a T)
export async function profile(from, all, boxes, btn, params) {
  const cols = all.slice(0, 24), q = ident, simple = c => !/^(List|LargeList|FixedSizeList|Struct|Map|Binary|LargeBinary|BinaryView)|\[\]$/.test(c.d), label = btn.lastChild.textContent;
  const rows = async sql => { const r = await run(sql, undefined, params); return r.kind === 'rows' ? r.rows.map(a => Object.fromEntries(r.columns.map((c, i) => [c.name, a[i]]))) : []; };
  const t = { q: from };
  btn.disabled = true; btn.lastChild.textContent = 'Profiling…';
  try {
    const distinct = c => /^Float/.test(c.d) ? `CAST(${q(c.n)} AS VARCHAR)` : q(c.n); // (DataFusion's approx_distinct takes no floats)
    const parts = cols.flatMap((c, i) => [`count(${q(c.n)}) AS "v${i}"`, simple(c) ? `approx_distinct(${distinct(c)}) AS "d${i}"` : `NULL AS "d${i}"`,
      simple(c) ? `CAST(min(${q(c.n)}) AS VARCHAR) AS "lo${i}"` : `NULL AS "lo${i}"`, simple(c) ? `CAST(max(${q(c.n)}) AS VARCHAR) AS "hi${i}"` : `NULL AS "hi${i}"`]);
    const [a] = await rows(`SELECT count(*) AS n, ${parts.join(', ')} FROM ${t.q}`);
    const stats = cols.map((c, i) => ({ n: Number(a.n), nulls: Number(a.n) - Number(a['v' + i]), distinct: Number(a['d' + i] ?? 0), exact: false, min: iso(a['lo' + i]), max: iso(a['hi' + i]) }));
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
  btn.disabled = false; btn.lastChild.textContent = label;
}
/** An answer's data profile (its Data profile view): each column's NULLs, distinct values, range and
 * spread, of the rows here; **Profile every row** reads them all, the statement run again. */
export function dataProfile(r) {
  const times = r.columns.map(c => /^Timestamp/.test(c.type || '')), paged = r.total > r.rows.length;
  const boxes = r.columns.map((c, k) => {
    const s = summarize(r.rows.map(row => times[k] && row[k] != null ? String(row[k]).replace(/^(\d{4}-\d\d-\d\d)T/, '$1 ') : row[k]), c.type), [nums, graph] = statView(s);
    return h('div', { class: 'dp-row', 'data-col': c.name }, h('div', { class: 'dp-c' }, typeMark(c.type), h('span', { class: 'nm', title: c.name }, c.name), h('span', { class: 'ty' }, sqlType(c.type))), h('div', { class: 'ps' }, nums, graph || h('div')));
  });
  const all = r.sql ? h('button', { class: 'btn small', title: 'Every row of the answer, counted on the node (its statement runs again)', onclick: () => { note.textContent = `Of all ${count(r.total)} rows`; profile(`(\n${r.sql.replace(/[\s;]+$/, '')}\n) AS q`, r.columns.map(c => ({ n: c.name, d: c.type })), boxes, all, r.params); } }, icon('play'), 'Profile every row') : null;
  const note = h('span', { class: 'muted' }, `Of the ${count(r.rows.length)} row${r.rows.length === 1 ? '' : 's'} ${paged ? 'on this page' : 'here'}`);
  return h('div', { class: 'dprof' }, h('div', { class: 'cbar' }, note, h('span', { class: 'grow' }), paged ? all : null), ...boxes);
}

export function objectDetail(t) {
  const o = t.o, [ic, word] = KIND[o.kind] || KIND.table, stored = o.kind === 'table' || o.kind === 'materialized view';
  const counted = h('span', { class: 'muted' }, '…');
  rows(`SELECT count(*) AS n FROM ${t.q}`).then(r => counted.textContent = count(r[0].n), e => counted.textContent = e.message.split('\n')[0].slice(0, 120));
  const keyed = new Set(o.key || []), nn = new Set(o.not_null || []);
  const cols = t.columns.map(c => {
    const flags = [keyed.has(c.n) ? 'key' : null, nn.has(c.n) && !keyed.has(c.n) ? 'not null' : null, o.defaults?.[c.n] ? `default ${o.defaults[c.n]}` : null].filter(Boolean);
    return h('div', { class: 'pc', 'data-col': c.n }, h('div', { class: 'line1' }, typeMark(c.d), h('span', { class: 'nm' }, c.n), keyed.has(c.n) ? icon('key', 'kk') : null, h('span', { class: 'ty' }, sqlType(c.d))),
      flags.length ? h('div', { class: 'sub' }, flags.join(' · ')) : null, h('div', { class: 'ps' }));
  });
  const flow = h('div');
  if (o.kind === 'table' || o.kind === 'materialized view') flowOf(t, flow);
  const profileBtn = act('columns', 'Data profile', 'Each column: NULLs, distinct values, range and spread (reads the whole table)', () => profile(t.q, t.columns, cols, profileBtn));
  return [head(ic, t.t, `${word} · ${t.c}.${t.s}`, 'k-table'),
    h('div', { class: 'acts2' }, act('play', 'Preview', 'Its first rows (or double-click it)', () => query(`SELECT * FROM ${t.q} LIMIT 100`)), profileBtn, act('copy', 'Copy name', `Copy ${t.q}`, () => navigator.clipboard?.writeText(t.q).then(() => toast(`Copied ${t.q}`)))),
    facts([['Rows', counted], ['Columns', String(t.columns.length)], ['Key', o.key], ['Partitioned by', o.partition], ['Clustered by', o.cluster],
      ['Published as', o.publish], ['Rows kept', o.ttl], ['In files', stored ? `${bytes(o.bytes)} · ${count(o.files || 0)} file${o.files === 1 ? '' : 's'}` : null]]),
    flow, o.sql ? h('div', { class: 'dsect' }, o.kind === 'files' ? 'Reads' : 'Definition') : null, o.sql ? h('pre', { class: 'defn', html: highlighted(o.sql, 'sql') }) : null,
    h('div', { class: 'dsect' }, 'Columns'), ...cols];
}
// A materialized view's flow: what it follows, back to its tables, and what follows it; its
// expectations with the rows that broke each (pondra.flows, pondra.expectations: ADR-036).
function flowOf(t, box) {
  const me = t.s === 'public' ? t.t : `${t.s}.${t.t}`;
  rows('SELECT name, follows FROM pondra.flows').then(async r => {
    const by = new Map(r.map(x => [x.name, x.follows])), up = [];
    for (let n = me; by.has(n) && up.length < 20;) up.unshift(n = by.get(n));
    const down = r.filter(x => x.follows === me).map(x => x.name);
    if (!up.length && !down.length) return;
    const nm = n => h('span', { class: n === me ? 'fl cur' : 'fl' }, n), arr = () => h('span', { class: 'arr' }, '→');
    box.append(h('div', { class: 'dsect' }, 'Flow'), h('div', { class: 'flow' }, ...[...up, me].flatMap((n, i) => [i ? arr() : null, nm(n)]),
      ...(down.length ? [arr(), ...down.map(nm)] : [])));
    if (!by.has(me)) return;
    const e = await rows(`SELECT expectation, condition, on_violation, failed_rows FROM pondra.expectations WHERE view = '${me.replace(/'/g, "''")}'`);
    if (e.length) box.append(h('div', { class: 'dsect' }, 'Expectations'), ...e.map(x => h('div', { class: 'pc' },
      h('div', { class: 'line1' }, h('span', { class: 'nm' }, x.expectation), h('span', { class: 'ty' }, { keep: 'kept, counted', drop: 'dropped', fail: 'refused' }[x.on_violation])),
      h('div', { class: 'sub' }, h('code', {}, x.condition), x.failed_rows > 0 ? ` · ${count(x.failed_rows)} row${x.failed_rows === 1 ? '' : 's'} broke it` : ''))));
  }, () => {});
}
export async function fileDetail(f) {
  const rel = f.rel || f.path.replace(/^files\//, ''), kind = f.notebook ? 'notebook' : kindOf(rel), doc = S.docs.find(d => d.path === rel);
  const out = [head(iconOf(f.notebook ? 'x.ipynb' : rel), f.name || rel.split('/').pop(), f.notebook ? `a notebook in notebooks/` : `a ${kind === 'data' ? 'data ' : kind === 'sql' ? 'SQL ' : kind === 'python' ? 'Python ' : ''}file in ${rel.includes('/') ? rel.slice(0, rel.lastIndexOf('/') + 1) : 'files/'}`, 'k-' + kind),
    h('div', { class: 'acts2' }, kind !== 'file' ? act('eye', 'Open', 'Open it in a tab', () => openFile(rel)) : null,
      kind === 'data' ? act('play', 'Query with SQL', 'Read it as a table, in a SQL tab', () => query(`SELECT * FROM ${fileSql(rel)} LIMIT 1000`)) : null,
      !f.notebook ? act('down', 'Download', 'Download it', () => download(rel)) : null)];
  out[1].append(act('clock', 'Versions…', 'Its saves, each kept: what changed, and a version back', () => R.helpers.versions({ path: f.notebook ? rel + '.ipynb' : rel })));
  if (f.notebook) return out;
  out.push(facts([['Size', bytes(f.size)], ['Written', f.written ? utc(f.written).toLocaleString() : null], ['In SQL', kind === 'data' ? fileSql(rel) : `file_read('files/${rel}')`]]));
  if (doc?.kind === 'data') {
    out.push(facts([['Rows', count(doc.data.length)], ['Columns', String(doc.cols.length)], ['Format', doc.status()[1]]]));
    const ch = doc.changes();
    if (ch.length) out.push(h('div', { class: 'dsect' }, 'Not saved'), ...ch.map(c => h('div', { class: 'change' }, c)));
    out.push(h('p', { class: 'note' }, 'CSV and JSON files are edited in place. Parquet files, and files too big to hold, open read-only: load one into a table to change it with SQL.'));
  }
  return out;
}
/** A tab's file not saved yet: what it is, Save while there is something to save. */
export function docDetail(doc) {
  return [head(doc.icon, doc.title, `a new ${doc.kind === 'notebook' ? 'notebook' : doc.kind === 'sql' ? 'SQL file' : doc.kind === 'python' ? 'Python file' : 'file'}, not saved yet`, 'k-' + doc.kind),
    doc.dirty ? h('div', { class: 'acts2' }, act('save', 'Save', 'Save it in the lake (Ctrl+S)', () => doc.save())) : null, h('p', { class: 'muted' }, 'Once saved, it is kept in the lake\'s files: the Workspace lists it, and its size and versions show here.')];
}
export function resultDetail(p) {
  const { r, i, cell } = p, times = r.columns.map(c => /^Timestamp/.test(c.type || ''));
  const out = [head('chart', cell?.count ? `Answer [${cell.count}]` : 'Answer', `${count(r.rows.length)} row${r.rows.length === 1 ? '' : 's'}${r.total > r.rows.length ? ` of ${count(r.total)} (the ones here)` : ''} · ${r.columns.length} column${r.columns.length === 1 ? '' : 's'}`), h('div', { class: 'dsect' }, 'Columns')];
  r.columns.forEach((c, k) => {
    const s = summarize(r.rows.map(row => times[k] && row[k] != null ? String(row[k]).replace(/^(\d{4}-\d\d-\d\d)T/, '$1 ') : row[k]), c.type);
    const box = h('div', { class: 'pc' + (k === i ? ' on' : ''), 'data-col': c.name }, h('div', { class: 'line1' }, typeMark(c.type), h('span', { class: 'nm' }, c.name), h('span', { class: 'ty' }, sqlType(c.type))), h('div', { class: 'ps' }, ...statView(s)));
    out.push(box);
    if (k === i) requestAnimationFrame(() => box.scrollIntoView({ block: 'nearest' }));
  });
  return out;
}
/** Nothing picked: the lake, its tables, views and rows, and its cluster. */
export function summary() {
  const objs = S.objects || [], by = k => objs.filter(t => t.c === home() && t.o.kind === k).length;
  const tabled = objs.filter(t => t.c === home() && t.o.rows != null);
  const s = S.info || {};
  return [head('db', home() || 'Pondra', MODE === 'lakes' ? 'a database' : 'this lake', 'k-db'),
    facts([['Tables', count(by('table'))], ['Views', count(by('view') + by('files'))], ['Materialized', by('materialized view') ? count(by('materialized view')) : null],
      ['Rows in files', count(tabled.reduce((a, t) => a + (t.o.rows || 0), 0))], ['Size in files', bytes(tabled.reduce((a, t) => a + (t.o.bytes || 0), 0))],
      ['Nodes', s.nodes ? String(s.nodes.length) : null], ['This node', s.role], ['Leader', s.leader], ['Commits', s.hwm != null ? count(s.hwm) : null]]),
    h('p', { class: 'muted' }, 'Pick a table, a view or a file, or a column of an answer, to see it here.')];
}
