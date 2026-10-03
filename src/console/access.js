// Who may do what (ADR-035), in the console: users and roles in the Data tree, each with its grants,
// and who has access to a table, a view, a schema, the lake or a secret, granted and taken back in
// place. Every change is the statement it runs (GRANT, REVOKE, CREATE USER…), shown as it is made.
// Loaded with the Data tree's groups (groups.js), or when "Who has access…" is first used.
import { h, svg, S, R, T, run, rows, ident, quote, toast, pop, home } from './core.js';
import { highlighted } from './editor.js';
import { copyText } from './grid.js';
import { tab, danger } from './objects.js';

const H = R.helpers;
const PRIVS = ['SELECT', 'INSERT', 'UPDATE', 'DELETE'];
const sqlName = n => n.split('.').map(ident).join('.');
const schemaOf = n => n.includes('.') ? n.slice(0, n.indexOf('.')) : 'public';
/** What a grant is on, as GRANT names it. */
const onSql = o => o.kind === 'lake' ? 'ALL TABLES' : `${o.kind.toUpperCase()} ${o.kind === 'table' ? sqlName(o.name) : ident(o.name)}`;
const onLabel = o => o.kind === 'lake' ? 'every table' : o.kind === 'table' ? o.name : `${o.kind} ${o.name}`;
const ICON = { table: 'table', schema: 'schema', lake: 'db', secret: 'key' };
const onOf = g => ({ kind: g.on_kind, name: g.on_name });
const revokeSql = g => `REVOKE ${g.privilege} ON ${onSql(onOf(g))} FROM ${ident(g.grantee)}`;
const load = () => Promise.all([rows('SELECT * FROM pondra.users ORDER BY name'), rows('SELECT * FROM pondra.grants ORDER BY grantee, on_kind, on_name, privilege')]);
/** Run a statement; say so, or why not (in `err` when there is one: a dialog stays to put it right). */
async function exec(sql, said, err) {
  try { await run(sql); if (err) err.textContent = ''; toast(said); H.detail(); return true; } catch (e) { const m = e.message.split('\n')[0]; err ? err.textContent = m : toast(m, true); return false; }
}
const pill = (text, x, title, cls = '') => h('span', { class: 'pill ' + cls }, h('span', { class: 'pl' }, text), x ? h('button', { class: 'x', title, 'aria-label': title, onclick: x }, '×') : null);
const grantPill = (g, via, done) => pill(`${g.privilege}${g.columns ? ` (${g.columns})` : ''}${via ? ` · ${via}` : ''}`, via ? null : async () => { if (await exec(revokeSql(g), `Took back ${g.privilege} from ${g.grantee}`)) done(); }, `Take it back: ${revokeSql(g)}`, via ? 'via' : '');
const line = (ic, name, sub, kids) => h('div', { class: 'gline' }, h('span', { class: 'ic', html: svg(ic, 15) }), h('span', { class: 'gn', title: name }, name), sub ? h('span', { class: 'meta' }, sub) : null, h('span', { class: 'pills' }, kids));
/** Children put in place, those left out (null) left out. */
const fill = (el, ...kids) => el.replaceChildren(...kids.filter(k => k != null && k !== false));
const kindOf = u => !u ? 'every user' : u.superuser ? 'superuser' : u.kind;

/** Who has access to `on` (a table, a schema, the lake, a secret), or what `who` (a user or role) may
 * do; given and taken back in place, the GRANT built from what is picked shown as it is. */
export async function access({ who, on } = {}) {
  const st = { privs: new Set([on?.kind === 'secret' ? 'USAGE' : 'SELECT']), cols: new Set(), who: who || '', on: on ? `${on.kind}\t${on.name}` : '' };
  const list = h('div', { class: 'glist' }), form = h('div', { class: 'gform' }), said = h('pre', { class: 'defn' }), err = h('div', { class: 'sub bad' });
  let us = [], gs = [], secrets = [];
  const target = () => { const [kind, name] = st.on.split('\t'); return kind ? { kind, name } : null; };
  const table = () => { const o = target(); return o?.kind === 'table' ? S.objects?.find(t => t.c === home() && (t.s === 'public' ? t.t : `${t.s}.${t.t}`) === o.name) : null; };
  const statement = () => {
    const o = target(), p = o?.kind === 'secret' ? 'USAGE' : st.privs.size === 4 ? 'ALL' : PRIVS.filter(x => st.privs.has(x)).join(', ');
    const cols = o?.kind === 'table' && p === 'SELECT' && st.cols.size ? ` (${[...st.cols].map(ident).join(', ')})` : '';
    return o && p && st.who ? `GRANT ${p}${cols} ON ${onSql(o)} TO ${ident(st.who)}` : '';
  };
  async function reload() { [us, gs] = await load(); drawList(); }
  function drawList() {
    const users = new Map(us.map(u => [u.name, u])), o = target();
    if (who) { // (what one user or role may do: its grants by what they are on)
      const by = new Map();
      for (const g of gs.filter(g => g.grantee === who)) (by.get(`${g.on_kind}\t${g.on_name}`) || by.set(`${g.on_kind}\t${g.on_name}`, []).get(`${g.on_kind}\t${g.on_name}`)).push(g);
      const roles = users.get(who)?.member_of;
      fill(list, ...[...by.values()].map(l => line(ICON[l[0].on_kind], onLabel(onOf(l[0])), null, l.map(g => grantPill(g, null, reload)))),
        ...by.size ? [] : [h('div', { class: 'empty' }, users.get(who)?.superuser ? 'A superuser may do everything.' : 'Nothing granted yet.')], roles ? h('div', { class: 'sub' }, `And what its roles may do: ${roles}`) : null);
      return;
    }
    // (who has access to it: granted on it, or on its schema or the lake; and the superusers)
    const direct = g => g.on_kind === o.kind && (o.kind === 'lake' || g.on_name === o.name);
    const via = g => direct(g) ? null : g.on_kind === 'lake' && o.kind !== 'secret' && o.kind !== 'lake' ? 'every table' : o.kind === 'table' && g.on_kind === 'schema' && g.on_name === schemaOf(o.name) ? `schema ${g.on_name}` : false;
    const by = new Map();
    for (const g of gs.filter(g => via(g) !== false)) (by.get(g.grantee) || by.set(g.grantee, []).get(g.grantee)).push(g);
    const supers = us.filter(u => u.superuser && !by.has(u.name));
    fill(list, ...[...by].map(([n, l]) => line('user', n, kindOf(users.get(n)), l.map(g => grantPill(g, via(g), reload)))),
      ...supers.map(u => line('user', u.name, 'superuser', [pill('everything', null, '', 'via')])), ...by.size || supers.length ? [] : [h('div', { class: 'empty' }, 'Only the admin token, so far.')]);
  }
  function drawForm() {
    const o = target(), t = table(), seg = (label, on, click, title) => h('button', { type: 'button', class: 'seg' + (on ? ' on' : ''), 'aria-pressed': String(on), title, onclick: click }, label);
    const toggle = (set, x, keep) => () => { set.has(x) && (!keep || set.size > 1) ? set.delete(x) : set.add(x); drawForm(); };
    const grantees = [...us.filter(u => !u.superuser), ...us.some(u => u.name === 'public') ? [] : [{ name: 'public', kind: 'every user' }]];
    const objects = [['lake\t', 'Every table'], ...[...new Set((S.objects || []).filter(t => t.c === home()).map(t => t.s))].map(s => [`schema\t${s}`, `Schema ${s}`]),
      ...(S.objects || []).filter(t => t.c === home()).map(t => { const n = t.s === 'public' ? t.t : `${t.s}.${t.t}`; return [`table\t${n}`, n]; }), ...secrets.map(s => [`secret\t${s}`, `Secret ${s}`])];
    fill(form, 
      who ? h('label', {}, 'On', h('select', { onchange: e => { st.on = e.target.value; st.cols.clear(); st.privs = new Set([target()?.kind === 'secret' ? 'USAGE' : 'SELECT']); drawForm(); } },
        h('option', { value: '', disabled: true, selected: !st.on }, 'Pick a table, a schema, a secret…'), objects.map(([v, l]) => h('option', { value: v, selected: v === st.on }, l))))
        : h('label', {}, 'To', h('select', { onchange: e => { st.who = e.target.value; drawForm(); } }, h('option', { value: '', disabled: true, selected: !st.who }, grantees.length > 1 ? 'Pick a user or role…' : 'No users or roles yet: public is every user'),
          grantees.map(u => h('option', { value: u.name, selected: u.name === st.who }, `${u.name} (${kindOf(u)})`)))),
      o?.kind === 'secret' ? h('div', { class: 'sub' }, 'USAGE: its queries may read what the secret\'s scope covers.') : o ? h('div', { class: 'opts' }, h('span', { class: 'segs', role: 'group', 'aria-label': 'May' }, PRIVS.map(p => seg(p, st.privs.has(p), toggle(st.privs, p, true))))) : null,
      t && st.privs.size === 1 && st.privs.has('SELECT') && t.columns.length ? h('div', {}, h('div', { class: 'lbl' }, 'Columns', h('small', {}, ' none picked: every column')),
        h('span', { class: 'segs cols' }, t.columns.filter(c => !/^_(row_id|version|created_at|updated_at)$/.test(c.n)).map(c => seg(c.n, st.cols.has(c.n), toggle(st.cols, c.n))))) : null);
    const sql = statement();
    said.innerHTML = highlighted(sql || '-- pick ' + (who ? 'what it is on' : 'whom it goes to'), 'sql');
  }
  [[us, gs], secrets] = await Promise.all([load(), who ? rows('SELECT name FROM secrets() ORDER BY name').then(r => r.map(x => x.name), () => []) : []]);
  drawList(); drawForm();
  const grant = async () => { const sql = statement(); if (!sql) { err.textContent = who ? 'Pick what it is on.' : 'Pick whom it goes to.'; return; } if (await exec(sql, 'Granted', err)) reload(); };
  const d = pop(who ? `What ${who} may do` : `Who has access to ${on.label || onLabel(on)}`, h('div', { class: 'form access' }, list, h('div', { class: 'dsect' }, 'Grant'), form, said, err,
    h('div', { class: 'acts' }, h('button', { class: 'btn primary', onclick: grant }, 'Grant'), h('span', { class: 'grow' }), h('button', { class: 'btn', onclick: () => d.close() }, 'Close'))));
}

/** A new user (who signs in) or role (grants given to users together), as CREATE USER and GRANTs. */
export async function newUser(role) {
  const us = await rows('SELECT name, kind FROM pondra.users ORDER BY name').catch(() => []), roles = us.filter(u => u.kind === 'role'), pick = new Set();
  const name = h('input', { placeholder: role ? 'analysts' : 'ann', spellcheck: 'false', oninput: () => draw() }), pw = h('input', { type: 'password', autocomplete: 'new-password', oninput: () => draw() });
  const sup = h('input', { type: 'checkbox', onchange: () => draw() }), said = h('pre', { class: 'defn' }), err = h('div', { class: 'sub bad' }), segs = h('span', { class: 'segs' });
  const sql = hide => { const n = name.value.trim(); return n ? [`CREATE ${role ? 'ROLE' : 'USER'} ${ident(n)}${!role && pw.value ? ` PASSWORD ${hide ? "'***'" : quote(pw.value)}` : ''}${sup.checked ? ' SUPERUSER' : ''}`, ...pick.size ? [`GRANT ${[...pick].map(ident).join(', ')} TO ${ident(n)}`] : []] : []; };
  function draw() {
    segs.replaceChildren(...roles.map(r => h('button', { type: 'button', class: 'seg' + (pick.has(r.name) ? ' on' : ''), 'aria-pressed': String(pick.has(r.name)), onclick: () => { pick.has(r.name) ? pick.delete(r.name) : pick.add(r.name); draw(); } }, r.name)));
    said.innerHTML = highlighted(sql(true).join(';\n') || `-- CREATE ${role ? 'ROLE' : 'USER'} …`, 'sql');
  }
  draw();
  const make = async () => { const list = sql(); if (!list.length) { err.textContent = 'It needs a name.'; return; } if (await exec(list.join(';\n'), `Made ${name.value.trim()}`, err)) { d.close(); H.refresh(); } };
  const d = pop(role ? 'New role' : 'New user', h('div', { class: 'form access' }, h('label', {}, 'Its name', name), role ? null : h('label', {}, 'Password', pw, h('small', {}, 'At least 8 characters; kept only as its SCRAM verifier. None: it signs in with a token (New token…)')),
    h('label', { class: 'check' }, sup, 'Superuser: may do everything, as the admin token'),
    role || T.token() || T.headers()['x-pondra-owner'] ? null : h('div', { class: 'sub warn' }, 'This node has no tokens: once a user who signs in exists, it asks everyone to sign in, this page too (as a user, or with a token).'), roles.length ? h('div', {}, h('div', { class: 'lbl' }, role ? 'Holds the grants of' : 'Member of'), segs) : null, said, err,
    h('div', { class: 'acts' }, h('button', { class: 'btn primary', onclick: make }, role ? 'Make the role' : 'Make the user'), h('span', { class: 'grow' }), h('button', { class: 'btn', onclick: () => d.close() }, 'Cancel'))));
  requestAnimationFrame(() => name.focus());
}

/** A user's or role's roles, held or not: each a click (GRANT r TO u, REVOKE r FROM u). */
async function roles(u) {
  const box = h('span', { class: 'segs' }), err = h('div', { class: 'sub bad' });
  const draw = async () => {
    const all = await rows('SELECT name, kind, member_of FROM pondra.users ORDER BY name'), held = new Set((all.find(x => x.name === u.name)?.member_of || '').split(', ').filter(Boolean));
    const list = all.filter(r => r.kind === 'role' && r.name !== u.name);
    box.replaceChildren(...list.length ? list.map(r => h('button', { type: 'button', class: 'seg' + (held.has(r.name) ? ' on' : ''), 'aria-pressed': String(held.has(r.name)),
      onclick: async () => { if (await exec(held.has(r.name) ? `REVOKE ${ident(r.name)} FROM ${ident(u.name)}` : `GRANT ${ident(r.name)} TO ${ident(u.name)}`, held.has(r.name) ? `${u.name} left ${r.name}` : `${u.name} joined ${r.name}`, err)) draw(); } }, r.name)) : [h('span', { class: 'sub' }, 'No roles yet: the group\'s New role… makes one')]);
  };
  await draw();
  pop(`${u.name}'s roles`, h('div', { class: 'form access' }, h('small', {}, `What each role may do, ${u.name} may too. A click joins or leaves it.`), box, err));
}

/** A new password, typed twice over: never shown, sent once (ALTER USER u PASSWORD '…'). */
function password(u) {
  const a = h('input', { type: 'password', autocomplete: 'new-password', 'aria-label': 'New password' }), b = h('input', { type: 'password', autocomplete: 'new-password', 'aria-label': 'Again' }), err = h('div', { class: 'sub bad' });
  const save = async () => { if (a.value !== b.value) { err.textContent = 'The two aren\'t the same.'; return; } if (await exec(`ALTER USER ${ident(u.name)} PASSWORD ${quote(a.value)}`, `${u.name}'s password is changed`, err)) d.close(); };
  const d = pop(`${u.name}'s password`, h('div', { class: 'form access' }, h('label', {}, 'New password', a), h('label', {}, 'Again', b), err,
    h('div', { class: 'acts' }, h('button', { class: 'btn primary', onclick: save }, 'Change it'), h('span', { class: 'grow' }), h('button', { class: 'btn', onclick: () => d.close() }, 'Cancel'))));
  requestAnimationFrame(() => a.focus());
}

/** A token for a script or a service: made, and shown once. */
function token(u) {
  const name = h('input', { value: 'script', spellcheck: 'false' }), exp = h('select', {}, ['30 days', '90 days', '1 year', ''].map(x => h('option', { value: x }, x || 'never'))), err = h('div', { class: 'sub bad' });
  const make = async () => {
    try {
      const r = await run(`CREATE TOKEN ${ident(name.value.trim())} FOR USER ${ident(u.name)}${exp.value ? ` EXPIRES IN ${quote(exp.value)}` : ''}`), t = r.value?.token;
      d.close(); H.detail();
      pop(`${u.name}'s token ${name.value.trim()}`, h('div', { class: 'form access' }, h('small', {}, 'Shown this once: only its hash is kept. A client sends it as Authorization: Bearer, or as the password.'), h('pre', { class: 'defn tok' }, t)), [['Copy it', () => copyText(t, 'Token copied'), true]]);
    } catch (e) { err.textContent = e.message.split('\n')[0]; }
  };
  const d = pop(`New token for ${u.name}`, h('div', { class: 'form access' }, h('label', {}, 'Its name', name), h('label', {}, 'Expires in', exp), err,
    h('div', { class: 'acts' }, h('button', { class: 'btn primary', onclick: make }, 'Make it'), h('span', { class: 'grow' }), h('button', { class: 'btn', onclick: () => d.close() }, 'Cancel'))));
}

/** A user's or role's CREATE and GRANTs: what made it, but its password and tokens. */
const definition = (u, gs) => [`CREATE ${u.kind === 'user' ? 'USER' : 'ROLE'} ${ident(u.name)}${u.superuser ? ' SUPERUSER' : ''}${u.max_queries != null ? ` MAX_QUERIES ${u.max_queries}` : ''}${u.statement_timeout ? ` STATEMENT_TIMEOUT ${quote(u.statement_timeout)}` : ''};${u.password ? ' -- and its PASSWORD' : ''}`,
  ...u.member_of ? [`GRANT ${u.member_of.split(', ').map(ident).join(', ')} TO ${ident(u.name)};`] : [], ...gs.map(g => `GRANT ${g.privilege}${g.columns ? ` (${g.columns.split(', ').map(ident).join(', ')})` : ''} ON ${onSql(onOf(g))} TO ${ident(g.grantee)};`)].join('\n');

/** The Data tree's "Users and roles": each one's details (how it signs in, its roles, its members,
 * its grants with ×, its definition), and what can be done with it. */
export const users = {
  list: () => rows('SELECT * FROM pondra.users ORDER BY kind DESC, name'),
  item: u => ({ name: u.name, icon: 'user', meta: u.superuser ? 'superuser' : u.kind === 'user' ? '' : u.kind === 'role' ? 'role' : 'everyone', title: `${u.name}: ${kindOf(u)}`, word: u.kind === 'user' ? 'a user' : u.kind === 'role' ? 'a role: grants given to users together' : 'every user' }),
  menu: u => {
    const user = u.kind === 'user', word = user ? 'USER' : 'ROLE';
    return [{ label: 'Grant…', icon: 'key', run: () => access({ who: u.name }) }, u.name !== 'public' ? { label: 'Its roles…', icon: 'user', run: () => roles(u) } : null,
      user ? { label: 'Set a password…', icon: 'pencil', run: () => password(u) } : null, user ? { label: 'New token…', icon: 'plus', run: () => token(u) } : null,
      u.name !== 'public' ? { label: 'Limits…', icon: 'clock', run: () => tab(`-- at most this many of its statements at once, each at most this long (0: no limit)\nALTER ${word} ${ident(u.name)} MAX_QUERIES ${u.max_queries ?? 4} STATEMENT_TIMEOUT '${u.statement_timeout || '5 minutes'}';`) } : null,
      { label: 'Copy the name', icon: 'copy', run: () => copyText(u.name, `Copied ${u.name}`) }, '-',
      u.name !== 'public' ? { label: u.superuser ? 'Not a superuser…' : 'Make a superuser…', run: () => danger(u.superuser ? `${u.name} may then do only what it is granted.` : `${u.name} may then do everything, as the admin token.`, `ALTER ${word} ${ident(u.name)} ${u.superuser ? 'NOSUPERUSER' : 'SUPERUSER'}`, 'Changed') } : null,
      u.name !== 'public' ? { label: 'Drop…', icon: 'trash', run: () => danger(`Drop the ${word.toLowerCase()} ${u.name}, its grants and its tokens?`, `DROP ${word} ${ident(u.name)}`, `Dropped ${u.name}`) } : null];
  },
  /** What the details show below its actions: read again each time they are drawn. */
  extra: u => {
    const box = h('div', {}, h('div', { class: 'empty' }, '…'));
    (async () => {
      const [all, gs] = await load(), me = all.find(x => x.name === u.name) || u, mine = gs.filter(g => g.grantee === u.name);
      const members = all.filter(m => (m.member_of || '').split(', ').includes(u.name)).map(m => m.name), by = new Map();
      for (const g of mine) (by.get(`${g.on_kind}\t${g.on_name}`) || by.set(`${g.on_kind}\t${g.on_name}`, []).get(`${g.on_kind}\t${g.on_name}`)).push(g);
      const toks = (me.tokens || '').split(', ').filter(Boolean);
      fill(box, H.facts([['Signs in with', me.kind !== 'user' ? null : [me.password ? 'a password' : null, toks.length ? `${toks.length > 1 ? 'tokens' : 'a token'}` : null].filter(Boolean).join(' and ') || 'nothing yet'],
        ['Member of', me.member_of], ['Members', members], ['At once', me.max_queries], ['Each statement', me.statement_timeout], ['Made', me.created ? new Date(me.created).toLocaleString() : null]]),
        toks.length ? h('div', { class: 'pills' }, toks.map(t => pill(`token ${t}`, () => danger(`Drop ${u.name}'s token ${t}? Whatever uses it is refused from then on.`, `DROP TOKEN ${ident(t)} FOR ${ident(u.name)}`, `Dropped ${t}`), `Drop the token ${t}`))) : null,
        h('div', { class: 'dsect' }, 'Grants'), me.superuser ? h('div', { class: 'sub' }, 'A superuser may do everything.') : null,
        ...[...by.values()].map(l => line(ICON[l[0].on_kind], onLabel(onOf(l[0])), null, l.map(g => grantPill(g, null, () => H.detail())))),
        by.size ? null : h('div', { class: 'sub' }, 'None of its own', ' ', h('button', { class: 'linkb', onclick: () => access({ who: u.name }) }, 'Grant…')),
        h('div', { class: 'dsect' }, 'Definition'), h('pre', { class: 'defn', html: highlighted(definition(me, mine), 'sql') }));
    })().catch(e => box.replaceChildren(h('div', { class: 'empty' }, e.message.split('\n')[0])));
    return box;
  },
};
