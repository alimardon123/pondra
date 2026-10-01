// A SQL file's parameters and variables (ADR-037), loaded once a file says `$name`: the bar above
// it (a parameter's type, its default and what it means, from its DECLARE), the values it sends,
// and what a `$name` under the pointer is.
import { h, fill, S, rows, store, moreStyle } from './core.js';
import { measure, LINE_H } from './editor.js';
import { statements } from './files.js';

moreStyle();

/** The `$name`s a statement uses (not `$1`, nor what strings, comments and `$$` bodies hold). */
const used = sql => [...sql.replace(/--[^\n]*|\/\*[\s\S]*?\*\/|'(?:[^']|'')*'|"(?:[^"]|"")*"|\$(\w*)\$[\s\S]*?\$\1\$/g, ' ').matchAll(/\$([A-Za-z_]\w*)/g)].map(m => m[1]);
/** The comment lines just above a statement: what its DECLARE's parameter means. */
const above = lead => { const out = []; for (const l of lead.trim().split('\n').reverse()) { const t = l.trim(); if (!t.startsWith('--')) break; out.unshift(t.replace(/^-+/, '').trim()); } const t = out.join(' '); return t[0] === '$' ? '' : t; };

/** A script's parameters as the node lists them (`pondra.parameters`, vars.rs): each DECLARE (its
 * type and default as written, what the comment above it says), and each `$name` used before
 * anything sets it (required); in the order the script declares or first uses them. */
export function declared(sql) {
  const out = new Map(), set = new Set(), use = s => { for (const name of used(s)) if (!set.has(name) && !out.has(name)) out.set(name, { name, required: true }); };
  for (const st of statements(sql)) {
    const lead = st.match(/^(?:\s*(?:--[^\n]*|\/\*[\s\S]*?\*\/))*\s*/)[0], s = st.slice(lead.length);
    const d = /^declare\s+\$([a-z_]\w*)\b\s*([\s\S]*?)\s*$/i.exec(s), v = /^(?:\$|set\s+variable\s+)([a-z_]\w*)\s*(?:=|\bto\b)\s*([\s\S]*?)\s*$/i.exec(s);
    if (d && !set.has(d[1])) {
      const t = /^([\s\S]*?)\s*(?:=|\bdefault\b)\s*([\s\S]*)$/i.exec(d[2]);
      if (t) use(t[2]);
      out.delete(d[1]); // (used before its DECLARE: the DECLARE says what it is)
      out.set(d[1], { name: d[1], type: (t ? t[1] : d[2]) || null, default: t ? t[2] : null, required: !t, about: above(lead) });
      set.add(d[1]);
    } else if (v) { use(v[2]); set.add(v[1]); } else use(s);
  }
  for (const m of sql.matchAll(/^\s*--\s*\$([A-Za-z_]\w*)\s*[:—–-]\s*(.+?)\s*$/gm)) if (out.has(m[1])) out.get(m[1]).about = m[2];
  return [...out.values()];
}

const INPUT = { date: 'date', timestamp: 'datetime-local' };
/** The bar above a SQL file: an input a parameter, its type beside its name, its default as the
 * placeholder, what it means on hover. A value typed goes with every run, bound on the node (a
 * DECLARE takes it in place of its default, cast to its type); kept in this browser for the file. */
export function bar(doc) {
  const list = declared(doc.ed.value), key = 'pondra.params:' + (doc.path || doc.untitled), sig = JSON.stringify(list);
  doc.kept ??= store.json(key, {});
  doc.pbar.hidden = !list.length;
  given(doc, list);
  if (sig === doc.shownParams) return;
  doc.shownParams = sig;
  fill(doc.pbar, h('span', { class: 'plabel' }, 'Parameters'), list.map(p => {
    const ty = p.type?.toLowerCase().match(/^\w+/)?.[0], kind = INPUT[ty];
    return h('label', { class: 'param', title: [p.about, p.required ? 'Required' : p.default && `Default: ${p.default}`].filter(Boolean).join('\n') },
      h('span', {}, '$' + p.name), ty ? h('i', {}, p.type.toLowerCase()) : null,
      h('input', { type: kind || 'text', value: doc.kept[p.name] ?? '', spellcheck: 'false', placeholder: p.required ? 'required' : p.default || '', 'aria-label': `The value of $${p.name}`, 'aria-required': String(p.required),
        oninput: e => { doc.kept[p.name] = e.target.value; store.set(key, JSON.stringify(doc.kept)); given(doc); }, onkeydown: e => { if (e.key === 'Enter') doc.run(); } }));
  }));
}
/** The values the file's runs send: numbers and true/false as such, the rest as text. */
function given(doc, list = declared(doc.ed.value)) {
  doc.given = Object.fromEntries(list.filter(p => (doc.kept?.[p.name] ?? '') !== '').map(({ name }) => {
    const v = doc.kept[name].trim();
    return [name, /^-?\d+(\.\d+)?$/.test(v) && Math.abs(+v) < 2 ** 53 ? +v : v === 'true' || v === 'false' ? v === 'true' : v];
  }));
}

let known = { at: 0, list: [] };
/** What the `$name` under the pointer is, as the editor's tooltip: its DECLARE, and its value in
 * this page's session now (read at most every two seconds). */
export async function hover(doc, e) {
  const ta = doc.ed.ta, line = ta.value.split('\n')[Math.floor((e.offsetY - 9) / LINE_H)] || '', col = Math.floor((e.offsetX - 14) / measure());
  const m = [...line.matchAll(/\$([A-Za-z_]\w*)/g)].find(m => m.index <= col && col < m.index + m[0].length);
  if (!m) { ta.title = ''; return; }
  if (Date.now() - known.at > 2000) { known.at = Date.now(); known.list = await rows('SELECT name, value, coalesce(declared, type) AS type FROM pondra.variables').catch(() => []); S.sqlVars = known.list.map(v => v.name); }
  const p = declared(ta.value).find(p => p.name === m[1]), v = known.list.find(v => v.name === m[1]);
  ta.title = [`$${m[1]}${p?.type ? ' ' + p.type : ''}${p?.default ? ' = ' + p.default : ''}`, p?.about, v ? `Now: ${v.value ?? 'NULL'}${v.type ? ` (${v.type.toLowerCase()})` : ''}` : doc.given?.[m[1]] != null ? `Given: ${doc.given[m[1]]}` : 'Not set in this page yet'].filter(Boolean).join('\n');
}

/** The session's SQL variables, for the Variables view. */
export const sqlVars = () => rows('SELECT name, value, coalesce(declared, type) AS type FROM pondra.variables ORDER BY name').catch(() => []);
