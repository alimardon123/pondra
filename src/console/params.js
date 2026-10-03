// A SQL file's parameters and variables (ADR-037), loaded once a file says `$name`: the bar above
// it (a parameter's type, its default and what it means, from its DECLARE), the values it sends,
// and what a `$name` under the pointer is.
import { h, fill, S, rows, store, moreStyle, sessionOf } from './core.js';
import { measure, LINE_H } from './editor.js';
import { statements } from './files.js';

await moreStyle(); // (the bar's look: drawn styled)

/** The `$name`s a statement uses (not `$1`, nor what strings, comments and `$$` bodies hold). */
const used = sql => [...sql.replace(/--[^\n]*|\/\*[\s\S]*?\*\/|'(?:[^']|'')*'|"(?:[^"]|"")*"|\$(\w*)\$[\s\S]*?\$\1\$/g, ' ').matchAll(/\$([A-Za-z_]\w*)/g)].map(m => m[1]);
/** The comment lines just above a statement: what its DECLARE's parameter means. */
const above = lead => { const out = []; for (const l of lead.trim().split('\n').reverse()) { const t = l.trim(); if (!t.startsWith('--')) break; out.unshift(t.replace(/^-+/, '').trim()); } const t = out.join(' '); return t[0] === '$' ? '' : t; };

/** The comment after a statement on its line (the start of the next one's text). */
const after = next => { const t = next?.match(/^[ \t]*--+\s*([^\n]*)/)?.[1].trim() || ''; return t[0] === '$' ? '' : t; };
/** A script's parameters as the node lists them (`pondra.parameters`, vars.rs): each DECLARE
 * PARAMETER (its type and default as written, what the comment above it, or after it on its line,
 * says), and each `$name` used before anything sets it (required); in the order the script declares
 * or first uses them. Its own variables (a plain DECLARE) come too, marked `own`, for the tooltip. */
export function declared(sql) {
  const out = new Map(), set = new Set(), use = s => { for (const name of used(s)) if (!set.has(name) && !out.has(name)) out.set(name, { name, required: true }); };
  const all = statements(sql, true);
  all.forEach((raw, i) => {
    const st = (i ? raw.replace(/^[ \t]*(--[^\n]*)?\n/, '') : raw).trim(); // (what follows a `;` on its line is the statement before's)
    const lead = st.match(/^(?:\s*(?:--[^\n]*|\/\*[\s\S]*?\*\/))*\s*/)[0], s = st.slice(lead.length);
    const d = /^declare\s+(?:(parameter)\s+)?\$([a-z_]\w*)\b\s*([\s\S]*?)\s*$/i.exec(s), v = /^(?:\$|set\s+variable\s+)([a-z_]\w*)\s*(?:=|\bto\b)\s*([\s\S]*?)\s*$/i.exec(s);
    if (d && !set.has(d[2])) {
      const t = /^([\s\S]*?)\s*(?:=|\bdefault\b)\s*([\s\S]*)$/i.exec(d[3]);
      if (t) use(t[2]);
      out.delete(d[2]); // (used before its DECLARE: the DECLARE says what it is)
      out.set(d[2], { name: d[2], type: (t ? t[1] : d[3]) || null, default: t ? t[2] : null, required: !t && !!d[1], about: above(lead) || after(all[i + 1]), own: !d[1] });
      set.add(d[2]);
    } else if (v) { use(v[2]); set.add(v[1]); } else use(s);
  });
  for (const m of sql.matchAll(/^\s*--\s*\$([A-Za-z_]\w*)\s*[:—–-]\s*(.+?)\s*$/gm)) if (out.has(m[1])) out.get(m[1]).about = m[2];
  return [...out.values()];
}

const INPUT = { date: 'date', timestamp: 'datetime-local' };
/** A default as its input shows it: a literal's value (`'s1'` is s1, `DATE '2026-09-30'` the date);
 * an expression (`current_date - 1`) isn't one, and stays the placeholder. */
const literal = (p, kind) => {
  const d = p.default?.trim() || '', m = /^(?:(?:date|timestamp|time)\s+)?'((?:[^']|'')*)'$/i.exec(d), v = m ? m[1].replace(/''/g, "'") : /^(-?\d+(\.\d+)?|true|false)$/i.test(d) ? d : '';
  return kind === 'datetime-local' ? v.replace(' ', 'T').slice(0, 16) : v;
};
/** The bar above a SQL file: an input a parameter, its type beside its name, its default's value in
 * it, what it means beside it. A value changed goes with every run, bound on the node (a DECLARE
 * takes it in place of its default, cast to its type), marked, and kept in this browser for the
 * file; ↺ (or the default typed again) goes back to the default. */
export function bar(doc) {
  const list = declared(doc.ed.value).filter(p => !p.own), key = 'pondra.params:' + (doc.path || doc.untitled), sig = JSON.stringify(list);
  doc.kept ??= store.json(key, {});
  doc.pbar.hidden = !list.length;
  given(doc, list);
  if (sig === doc.shownParams) return;
  doc.shownParams = sig;
  const keep = () => { store.set(key, JSON.stringify(doc.kept)); given(doc); };
  fill(doc.pbar, h('span', { class: 'plabel' }, 'Parameters'), list.map(p => {
    const ty = p.type?.toLowerCase().match(/^\w+/)?.[0], kind = INPUT[ty], dflt = literal(p, kind), mine = () => doc.kept[p.name] != null;
    const input = h('input', { type: kind || 'text', value: doc.kept[p.name] ?? dflt, spellcheck: 'false', placeholder: p.required ? 'required' : dflt ? '' : p.default || '', 'aria-label': `The value of $${p.name}`, 'aria-required': String(p.required),
      oninput: e => { if (e.target.value === dflt && !p.required) delete doc.kept[p.name]; else doc.kept[p.name] = e.target.value; box.classList.toggle('set', mine()); keep(); }, onkeydown: e => { if (e.key === 'Enter') doc.run('file'); } });
    const box = h('label', { class: 'param' + (mine() ? ' set' : ''), title: [p.about, p.required ? 'Required: the file needs a value' : p.default && `Default: ${p.default}`].filter(Boolean).join('\n') },
      h('span', {}, '$' + p.name), ty ? h('i', {}, p.type.toLowerCase()) : null, input,
      p.required ? null : h('button', { class: 'preset', type: 'button', title: `Back to its default (${p.default})`, 'aria-label': `$${p.name} back to its default`, onclick: () => { delete doc.kept[p.name]; input.value = dflt; box.classList.remove('set'); keep(); } }, '↺'),
      p.about ? h('small', { class: 'pabout' }, p.about) : null);
    return box;
  }));
}
/** The values the file's runs send: numbers and true/false as such, the rest as text. */
function given(doc, list = declared(doc.ed.value).filter(p => !p.own)) {
  doc.given = Object.fromEntries(list.filter(p => (doc.kept?.[p.name] ?? '') !== '').map(({ name }) => {
    const v = doc.kept[name].trim();
    return [name, /^-?\d+(\.\d+)?$/.test(v) && Math.abs(+v) < 2 ** 53 ? +v : v === 'true' || v === 'false' ? v === 'true' : v];
  }));
}

let known = { at: 0, list: [], doc: null };
/** What the `$name` under the pointer is, as the editor's tooltip: its DECLARE, and its value in
 * its tab's session now (read at most every two seconds). */
export async function hover(doc, e) {
  const ta = doc.ed.ta, line = ta.value.split('\n')[Math.floor((e.offsetY - 9) / LINE_H)] || '', col = Math.floor((e.offsetX - 14) / measure());
  const m = [...line.matchAll(/\$([A-Za-z_]\w*)/g)].find(m => m.index <= col && col < m.index + m[0].length);
  if (!m) { ta.title = ''; return; }
  if (Date.now() - known.at > 2000 || known.doc !== doc) { known = { at: Date.now(), doc, list: await rows('SELECT name, value, coalesce(declared, type) AS type FROM pondra.variables', sessionOf(doc)).catch(() => []) }; S.sqlVars = known.list.map(v => v.name); }
  const p = declared(ta.value).find(p => p.name === m[1]), v = known.list.find(v => v.name === m[1]);
  ta.title = [`$${m[1]}${p?.type ? ' ' + p.type : ''}${p?.default ? ' = ' + p.default : ''}`, p?.about, v ? `Now: ${v.value ?? 'NULL'}${v.type ? ` (${v.type.toLowerCase()})` : ''}` : doc.given?.[m[1]] != null ? `Given: ${doc.given[m[1]]}` : 'Not set in this tab yet'].filter(Boolean).join('\n');
}

/** The tab's SQL variables, for the Variables view. */
export const sqlVars = (d = S.doc) => rows('SELECT name, value, coalesce(declared, type) AS type FROM pondra.variables ORDER BY name', sessionOf(d)).catch(() => []);
