// Sharing a table with another company (ADR-046): a share and a recipient picked or named, and the
// statements that does shown as they are built (CREATE SHARE, ALTER SHARE … ADD TABLE, CREATE
// RECIPIENT, GRANT SELECT ON SHARE). A new recipient's profile is shown once, to download or copy
// and send: their pandas, Spark, Power BI or Pondra reads the table's files with their own compute.
// Loaded when "Share with another company…" is first used.
import { h, R, run, rows, ident, toast, pop, saveAs } from './core.js';
import { highlighted } from './editor.js';
import { copyText } from './grid.js';

const H = R.helpers;
const plain = s => s.trim().toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/^(\d)/, '_$1');

/** Share `t` (a table or materialized view of the Data tree). */
export async function share(t) {
  const name = t.s === 'public' ? t.t : `${t.s}.${t.t}`, q = t.s === 'public' ? ident(t.t) : `${ident(t.s)}.${ident(t.t)}`;
  const [had, people] = await Promise.all([rows('SELECT share, "table" FROM pondra.shares'), rows('SELECT name FROM pondra.recipients ORDER BY name')]);
  const shares = [...new Set(had.map(r => r.share))], known = new Set(people.map(p => p.name));
  const list = (id, xs) => h('datalist', { id }, xs.map(x => h('option', { value: x })));
  const sh = h('input', { list: 'share-list', value: shares[0] || plain(t.t) + '_share', spellcheck: 'false', oninput: () => draw() });
  const who = h('input', { list: 'who-list', value: people[0]?.name || '', placeholder: 'acme_corp', spellcheck: 'false', oninput: () => draw() });
  const hist = h('input', { type: 'checkbox', onchange: () => draw() }), said = h('pre', { class: 'defn' }), err = h('div', { class: 'sub bad' });
  const sql = () => {
    const s = plain(sh.value), r = plain(who.value);
    if (!s || !r) return [];
    return [!shares.includes(s) && `CREATE SHARE ${s}`, !had.some(x => x.share === s && x.table === name) && `ALTER SHARE ${s} ADD TABLE ${q}${hist.checked ? ' WITH HISTORY' : ''}`,
      !known.has(r) && `CREATE RECIPIENT ${r} EXPIRES IN '90 days'`, `GRANT SELECT ON SHARE ${s} TO RECIPIENT ${r}`].filter(Boolean);
  };
  function draw() { said.innerHTML = highlighted(sql().join(';\n') || '-- a share and a recipient', 'sql'); }
  draw();
  async function go() {
    const all = sql();
    if (!all.length) { err.textContent = 'It needs a share and a recipient.'; return; }
    let profile = null;
    try {
      for (const s of all) { const r = await run(s); profile = r.value?.profile || profile; }
    } catch (e) { err.textContent = e.message.split('\n')[0]; return; }
    d.close(); H.refresh?.();
    const r = plain(who.value);
    if (profile) shown(r, profile);
    else toast(`${r} can read ${name}: with the profile it was given (ALTER RECIPIENT ${r} ROTATE TOKEN makes a new one)`);
  }
  const d = pop(`Share ${name} with another company`, h('div', { class: 'form access' },
    h('small', {}, 'They read its files from the bucket with their own tools and compute (pandas, Spark, Power BI, Pondra): nothing of yours runs for them, and no key to the bucket leaves. Only what has been written to files is shared; a filtered or narrower slice is a materialized view of it.'),
    h('label', {}, 'Share', sh, list('share-list', shares)), h('label', {}, 'Recipient (the company)', who, list('who-list', people.map(p => p.name))),
    h('label', { class: 'check' }, hist, 'With history: older versions too'), said, err,
    h('div', { class: 'acts' }, h('button', { class: 'btn primary', onclick: go }, 'Share'), h('span', { class: 'grow' }), h('button', { class: 'btn', onclick: () => d.close() }, 'Cancel'))));
}

/** A new recipient's profile: shown this once (only its hash is kept), to download and send. */
function shown(who, profile) {
  const text = JSON.stringify(profile, null, 2);
  pop(`${who}'s profile`, h('div', { class: 'form access' }, h('small', {}, 'Send it to them by a safe way: whoever has it reads the share until it expires. It is shown this once.'),
    h('pre', { class: 'defn' }, text), h('small', {}, `In Python: delta_sharing.load_as_pandas("${who}.share#<share>.<schema>.<table>"); in Pondra: ATTACH '<profile>' AS <name> (TYPE share)`)),
    [[`Download ${who}.share`, () => saveAs(text, 'application/json', `${who}.share`), true], ['Copy', () => copyText(text, 'Copied the profile')]]);
}
