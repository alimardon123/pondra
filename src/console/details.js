// The details of a table, a view, a file or an answer (ADR-034, round 29), and a table's profile:
// loaded the first time something is picked, not with the page. What they use of the shell comes
// through `R.helpers`.
import { h, count, bytes, utc, ago, icon, typeMark, sqlType, fileSql, S, R, rows, ident, toast, numeric, moreStyle } from './core.js';
import { highlighted } from './editor.js';
import { statView, spread, summarize } from './grid.js';
import { versions, openNotebook } from './notebook.js';
import { iconOf, kindOf, download } from './files.js';

await moreStyle();

const H = R.helpers, { KIND, addDoc, closeDoc, query, act, facts, head, openFile } = H;

/** A table's columns profiled where it is: one pass for every column's counts and range, then a
 * histogram or the commonest values of each (at most 24 columns). */
const iso = v => typeof v === 'string' ? v.replace(/^(\d{4}-\d\d-\d\d)T(\d)/, '$1 $2') : v; // (a time as the grid shows it: a space, not a T)
export async function profile(t, boxes, btn) {
  const cols = t.columns.slice(0, 24), q = ident, simple = c => !/^(List|LargeList|FixedSizeList|Struct|Map|Binary|LargeBinary|BinaryView)|\[\]$/.test(c.d);
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
  btn.disabled = false; btn.lastChild.textContent = 'Profile';
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
  const profileBtn = act('chart', 'Profile', 'Each column: nulls, distinct values, range and spread (reads the whole table)', () => profile(t, cols, profileBtn));
  return [head(ic, t.t, `${word} · ${t.c}.${t.s}`, 'k-table'),
    h('div', { class: 'acts2' }, act('play', 'Preview', 'Its first rows (or double-click it)', () => query(`SELECT * FROM ${t.q} LIMIT 100`)), profileBtn, act('copy', 'Copy name', `Copy ${t.q}`, () => navigator.clipboard?.writeText(t.q).then(() => toast(`Copied ${t.q}`)))),
    facts([['Rows', counted], ['Columns', String(t.columns.length)], ['Key', o.key], ['Partitioned by', o.partition], ['Clustered by', o.cluster],
      ['Published as', o.publish], ['Rows kept', o.ttl], ['In files', stored ? `${bytes(o.bytes)} · ${count(o.files || 0)} file${o.files === 1 ? '' : 's'}` : null]]),
    o.sql ? h('div', { class: 'dsect' }, o.kind === 'files' ? 'Reads' : 'Definition') : null, o.sql ? h('pre', { class: 'defn', html: highlighted(o.sql, 'sql') }) : null,
    h('div', { class: 'dsect' }, 'Columns'), ...cols];
}
export async function fileDetail(f) {
  const rel = f.rel || f.path.replace(/^files\//, ''), kind = f.notebook ? 'notebook' : kindOf(rel), doc = S.docs.find(d => d.path === rel);
  const out = [head(iconOf(f.notebook ? 'x.ipynb' : rel), f.name || rel.split('/').pop(), f.notebook ? `a notebook in notebooks/` : `a ${kind === 'data' ? 'data ' : kind === 'sql' ? 'SQL ' : kind === 'python' ? 'Python ' : ''}file in ${rel.includes('/') ? rel.slice(0, rel.lastIndexOf('/') + 1) : 'files/'}`, 'k-' + kind),
    h('div', { class: 'acts2' }, kind !== 'file' ? act('eye', 'Open', 'Open it in a tab', () => openFile(rel)) : null,
      kind === 'data' ? act('play', 'Query with SQL', 'Read it as a table, in a SQL tab', () => query(`SELECT * FROM ${fileSql(rel)} LIMIT 1000`)) : null,
      !f.notebook ? act('down', 'Download', 'Download it', () => download(rel)) : null)];
  if (f.notebook) {
    const vs = await versions(rel.slice(10)).catch(() => []);
    out.push(h('div', { class: 'dsect' }, `Versions (${vs.length})`), ...vs.map(v => h('div', { class: 'row', role: 'button', tabindex: '0', title: 'Open this version', onclick: async () => { const open = S.docs.find(d => d.path === rel); if (open && !open.close()) return; if (open) closeDoc(open); addDoc(await openNotebook(rel.slice(10), v.version)); } },
      icon('clock'), h('span', { class: 'nm' }, utc(v.written).toLocaleString()), h('span', { class: 'meta' }, ago(v.written)))));
    return out;
  }
  out.push(facts([['Size', bytes(f.size)], ['Written', f.written ? utc(f.written).toLocaleString() : null], ['In SQL', kind === 'data' ? fileSql(rel) : `file_read('files/${rel}')`]]));
  if (doc?.kind === 'data') {
    out.push(facts([['Rows', count(doc.data.length)], ['Columns', String(doc.cols.length)], ['Format', doc.status()[1]]]));
    const ch = doc.changes();
    if (ch.length) out.push(h('div', { class: 'dsect' }, 'Not saved'), ...ch.map(c => h('div', { class: 'change' }, c)));
    out.push(h('p', { class: 'note' }, 'CSV and JSON files are edited in place. Parquet files, and files too big to hold, open read-only: load one into a table to change it with SQL.'));
  }
  return out;
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
