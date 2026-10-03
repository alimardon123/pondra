// Jobs (ADR-034, round 29): what runs on its own, apart from History (what ran). A section a kind:
// the schedules (`pondra.tasks`, each with its last runs from `pondra.runs`) now; round 30's
// flows are another (`register.jobKind`, as an extension adds its own). Loaded when first shown.
import { h, secs, utc, icon, svg, R, S, run, rows, ident, quote, toast, menu, pop, prompt, confirmed, moreStyle, register } from './core.js';
import { highlighted, Editor } from './editor.js';
import { copyText } from './grid.js';

await moreStyle();

const H = R.helpers;
let again = 0;
const open = new Set(); // (the jobs whose last runs are shown)

/** "in 12 min", "3 h ago": a time from now, in words. */
const ago = t => {
  const d = t - Date.now(), a = Math.abs(d), n = a < 6e4 ? `${Math.max(1, Math.round(a / 1e3))} s` : a < 36e5 ? `${Math.round(a / 6e4)} min` : a < 864e5 ? `${Math.round(a / 36e5)} h` : `${Math.round(a / 864e5)} d`;
  return d >= 0 ? `in ${n}` : `${n} ago`;
};
const cadence = s => /^cron\b/i.test(s) ? s : `every ${s}`;
/** The file a statement runs, when it is `CALL run('path', …)` (a file scheduled from its tab). */
const fileOf = sql => sql.match(/^\s*CALL\s+run\s*\(\s*'((?:[^']|'')+)'/i)?.[1].replace(/''/g, "'") || null;

// ------------------------------------------------------------------ schedules
register.jobKind({ id: 'schedules', title: 'Schedules', order: 10, empty: 'Nothing runs on a schedule yet. A file\'s Run ▾ → Schedule…, or New schedule here, sets one.',
  async load() {
    const [tasks, ran] = await Promise.all([rows('SELECT name, schedule, statement, last_tick, next_tick FROM pondra.tasks ORDER BY name').catch(() => []),
      rows("SELECT id, routine, status, started, ended, error FROM pondra.runs WHERE caller = 'schedule' ORDER BY started DESC LIMIT 300").catch(() => [])]);
    return tasks.map(t => ({ ...t, runs: ran.filter(x => x.routine === t.name).slice(0, 16) }));
  },
  item: schedule });

function schedule(t) {
  const last = t.runs[0], state = !last ? 'none' : last.status === 'failed' ? 'bad' : last.status === 'running' ? 'run' : 'ok';
  const took = x => x.ended ? utc(x.ended) - utc(x.started) : null, file = fileOf(t.statement), longest = Math.max(1, ...t.runs.map(x => took(x) || 0));
  const bars = h('span', { class: 'spark', title: 'Its last runs, the newest at the right: their time, and whether each worked' },
    [...t.runs].reverse().map(x => h('i', { class: x.status === 'failed' ? 'bad' : x.status === 'running' ? 'run' : '', style: `height:${Math.max(18, Math.round(100 * (took(x) || longest * 0.2) / longest))}%`, title: `${utc(x.started).toLocaleString()} · ${x.status}${took(x) != null ? ' · ' + secs(took(x)) : ''}` })));
  const acts = [['play', 'Run now', 'Start it now, on the node (as a job: History shows it)', () => runNow(t)], ['pencil', 'Edit', 'Change how often it runs, and what', () => scheduleDialog(t)]];
  const more = e => menu(e.currentTarget, [file ? { label: 'Open the file', icon: 'file', run: () => H.openFile(file) } : null, { label: 'Copy the statement', icon: 'copy', run: () => copyText(t.statement) },
    { label: 'Its runs in History', icon: 'clock', run: () => H.show('runs') }, '-', { label: 'Drop it', icon: 'trash', run: () => drop(t) }]);
  const el = h('div', { class: 'job' + (open.has(t.name) ? ' open' : '') },
    h('div', { class: 'line1', role: 'button', tabindex: '0', title: 'Click: its last runs', onclick: () => { open.has(t.name) ? open.delete(t.name) : open.add(t.name); H.detail(); }, onkeydown: e => e.key === 'Enter' && e.currentTarget.click() },
      h('span', { class: 'ic', html: svg('clock', 15) }), h('span', { class: 'nm' }, t.name),
      h('span', { class: 'state ' + state }, { none: 'not run yet', bad: 'failed', run: 'running', ok: 'ok' }[state])),
    h('div', { class: 'when' }, h('span', { class: 'cad' }, cadence(t.schedule)), t.next_tick ? h('span', { title: utc(t.next_tick).toLocaleString() }, `next ${ago(utc(t.next_tick))}`) : null),
    t.runs.length ? h('div', { class: 'hist' }, bars, h('span', { class: 'sub' }, `last ${ago(utc(last.started))}${took(last) != null ? ` · ${secs(took(last))}` : ''}`)) : null,
    h('pre', { class: 'jstmt', html: highlighted(t.statement, 'sql') }),
    last?.status === 'failed' && last.error ? h('div', { class: 'sub bad' }, last.error.split('\n')[0].slice(0, 240)) : null,
    open.has(t.name) ? h('div', { class: 'runs' }, t.runs.length ? t.runs.map(x => h('div', { class: 'r' + (x.status === 'failed' ? ' bad' : '') },
      h('span', { class: 'dot' }), h('span', {}, utc(x.started).toLocaleString()), h('span', { class: 'grow' }), h('span', {}, x.status === 'failed' ? 'failed' : x.status === 'running' ? 'running' : secs(took(x))))) : h('div', { class: 'empty' }, 'No run yet.')) : null,
    h('div', { class: 'acts2' }, acts.map(([ic, label, title, fn]) => h('button', { class: 'btn small', title, onclick: fn }, icon(ic), label)), h('span', { class: 'grow' }),
      h('button', { class: 'icon sm', title: 'More', 'aria-label': `${t.name}: more`, onclick: more }, icon('dots'))));
  return el;
}
/** Run a schedule's statement now: a CALL starts on the node as a job (`pondra.start`); another statement runs here. */
async function runNow(t) {
  const call = t.statement.match(/^\s*CALL\s+([\w.]+)\s*\(([\s\S]*)\)\s*;?\s*$/i);
  try {
    await run(call ? `SELECT pondra.start(${quote(call[1])}${call[2].trim() ? ', ' + call[2] : ''})` : t.statement);
    toast(call ? `Started ${t.name}: History shows it` : `Ran ${t.name}`);
  } catch (e) { toast(e.message, true); }
  H.detail();
}
async function drop(t) {
  if (!confirmed(`Drop the schedule ${t.name}? It stops running (its runs stay in History).`)) return;
  try { await run(`DROP TASK ${ident(t.name)}`); toast(`Dropped ${t.name}`); } catch (e) { toast(e.message, true); }
  H.detail();
}
// ------------------------------------------------------------------ a schedule made or changed: how often (simple, or cron, kept in step) and what it runs
const UNITS = ['minutes', 'hours', 'days', 'seconds'], DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'], ZONE = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
/** A schedule's text as the picker shows it: every n units, daily, weekly or monthly at a time, or any cron. */
export function parse(text) {
  const t = (text || '').trim(), n = t.match(/^(?:every\s+)?(\d+)\s*(second|minute|hour|day)s?$/i);
  if (n) return { how: 'every', n: +n[1], unit: n[2].toLowerCase() + 's' };
  const c = t.match(/^cron\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)(?:\s+(\S+))?$/i), num = x => /^\d+$/.test(x);
  if (!c) return { how: 'cron', cron: t.replace(/^cron\s+/i, ''), zone: ZONE };
  const [, mi, hr, dom, mon, dow, zone = 'UTC'] = c, at = num(mi) && num(hr) ? `${hr.padStart(2, '0')}:${mi.padStart(2, '0')}` : null, base = { at, zone, cron: c.slice(1, 6).join(' ') };
  if (at && mon === '*' && dom === '*' && dow === '*') return { ...base, how: 'daily' };
  if (at && mon === '*' && dom === '*' && /^[0-7](,[0-7])*$/.test(dow)) return { ...base, how: 'weekly', days: dow.split(',').map(d => (+d + 6) % 7) };
  if (at && mon === '*' && num(dom) && dow === '*') return { ...base, how: 'monthly', day: +dom };
  return { ...base, how: 'cron' };
}
/** The picker's state as the node takes it: '15 minutes', or 'cron 0 2 * * * Europe/Paris'. */
export function format(s) {
  if (s.how === 'every') return `${s.n} ${s.n === 1 ? s.unit.slice(0, -1) : s.unit}`;
  const [hr, mi] = (s.at || '00:00').split(':').map(Number), when = `${mi} ${hr}`;
  const cron = s.how === 'daily' ? `${when} * * *` : s.how === 'weekly' ? `${when} * * ${[...s.days || [0]].sort().map(d => (d + 1) % 7).join(',')}` : s.how === 'monthly' ? `${when} ${s.day || 1} * *` : s.cron;
  return `cron ${cron} ${s.zone || 'UTC'}`;
}
/** A schedule's editor: its name (new only), how often, and the statement it runs; saved, or dropped. */
function scheduleDialog(t) {
  let st = parse(t?.schedule || '1 day');
  const name = h('input', { value: t?.name || '', placeholder: 'nightly_report', spellcheck: 'false', disabled: !!t, 'aria-label': 'Its name' });
  const text = h('input', { class: 'mono', spellcheck: 'false', 'aria-label': 'The schedule as SQL takes it', oninput: () => { st = parse(text.value); draw(true); } });
  const ed = new Editor({ grow: true, value: t?.statement || '', label: 'What it runs', placeholder: "CALL run('queries/daily.sql')" });
  const how = h('span', { class: 'segs', role: 'group', 'aria-label': 'How often' }), opts = h('div', { class: 'opts' }), err = h('div', { class: 'sub bad' });
  const set = o => { st = { ...st, ...o }; draw(); };
  const time = () => h('input', { type: 'time', value: st.at || '02:00', 'aria-label': 'At', oninput: e => set({ at: e.target.value }) });
  const zone = () => h('input', { value: st.zone || ZONE, size: 16, spellcheck: 'false', 'aria-label': 'Time zone', title: 'A time zone: UTC, Europe/Paris, America/New_York…', onchange: e => set({ zone: e.target.value.trim() }) });
  function draw(typed) {
    how.replaceChildren(...[['every', 'Every'], ['daily', 'Daily'], ['weekly', 'Weekly'], ['monthly', 'Monthly'], ['cron', 'Cron']].map(([k, l]) => h('button', { type: 'button', class: 'seg' + (st.how === k ? ' on' : ''), 'aria-pressed': String(st.how === k),
      onclick: () => set(k === 'every' ? { how: k, n: st.n || 1, unit: st.unit || 'days' } : k === 'cron' ? { how: k, cron: parse(format(st)).cron || '0 2 * * *', zone: st.zone || ZONE } : { how: k, at: st.at || '02:00', zone: st.zone || ZONE, days: st.days || [0], day: st.day || 1 }) }, l)));
    opts.replaceChildren(...st.how === 'every' ? [h('input', { type: 'number', min: 1, value: st.n, 'aria-label': 'How many', oninput: e => set({ n: Math.max(1, +e.target.value || 1) }) }),
      h('select', { 'aria-label': 'Of what', onchange: e => set({ unit: e.target.value }) }, UNITS.map(u => h('option', { selected: u === st.unit }, u)))]
      : st.how === 'cron' ? [h('input', { class: 'mono', value: st.cron, size: 18, spellcheck: 'false', 'aria-label': 'Cron: minute hour day month weekday', onchange: e => set({ cron: e.target.value.trim() }) }), zone(), h('small', {}, 'minute hour day month weekday')]
      : [st.how === 'weekly' ? h('span', { class: 'segs days' }, DAYS.map((d, i) => h('button', { type: 'button', class: 'seg' + (st.days.includes(i) ? ' on' : ''), 'aria-pressed': String(st.days.includes(i)),
          onclick: () => set({ days: st.days.includes(i) && st.days.length > 1 ? st.days.filter(x => x !== i) : [...new Set([...st.days, i])] }) }, d))) : null,
        st.how === 'monthly' ? h('label', {}, 'on day ', h('input', { type: 'number', min: 1, max: 31, value: st.day, 'aria-label': 'Day of the month', oninput: e => set({ day: Math.min(31, Math.max(1, +e.target.value || 1)) }) })) : null,
        h('label', {}, 'at ', time()), zone()].filter(Boolean));
    if (!typed) text.value = format(st); // (the picker and the text kept in step: each changes the other)
  }
  draw();
  const save = async () => {
    const n = name.value.trim(), every = text.value.trim(), body = ed.value.trim().replace(/;\s*$/, '');
    if (!n || !body) { err.textContent = !n ? 'It needs a name.' : 'It needs a statement to run.'; return; }
    try { await run(`CREATE ${t ? 'OR REPLACE ' : ''}TASK ${ident(n)} SCHEDULE ${quote(every)} AS ${body}`); d.close(); toast(`${t ? 'Saved' : 'Scheduled'} ${n}: ${cadence(every)}`); H.detail(); }
    catch (e) { err.textContent = e.message.split('\n')[0]; } // (the dialog stays, to put it right)
  };
  const d = pop(t ? `Edit ${t.name}` : 'New schedule', h('div', { class: 'form sched' }, t ? null : h('label', {}, 'Its name', name), h('div', {}, h('div', { class: 'lbl' }, 'How often'), how, opts, h('label', { class: 'as' }, 'As SQL takes it', text)),
    h('div', {}, h('div', { class: 'lbl' }, 'What it runs'), ed.el, h('small', {}, 'A statement: CALL run(\'a file\') runs a SQL file, a Python file or a notebook of the lake\'s')), err,
    h('div', { class: 'acts' }, h('button', { class: 'btn primary', onclick: save }, t ? 'Save' : 'Schedule'), t ? h('button', { class: 'btn', onclick: () => { d.close(); drop(t); } }, 'Drop…') : null, h('span', { class: 'grow' }), h('button', { class: 'btn', onclick: () => d.close() }, 'Cancel'))));
  requestAnimationFrame(() => (t ? ed.ta : name).focus());
}
const newSchedule = () => scheduleDialog();

/** The Jobs view: a section a kind, each with what it has. */
export async function jobs() {
  const kinds = R.jobKinds || [], loaded = await Promise.all(kinds.map(k => Promise.resolve(k.load()).catch(e => ({ error: e.message }))));
  clearTimeout(again);
  // (looked at again when a schedule's next run is due, or a run ends)
  const due = loaded.flat().filter(t => t?.next_tick).map(t => utc(t.next_tick) - Date.now()).filter(d => d > 0), busy = loaded.flat().some(t => t?.runs?.[0]?.status === 'running');
  again = setTimeout(() => { if (S.tab === 'jobs' && !document.getElementById('right')?.hidden) H.detail(); }, busy ? 3000 : Math.min(60000, Math.max(2000, (due.length ? Math.min(...due) : 60000) + 1500)));
  const n = loaded.reduce((a, l) => a + (Array.isArray(l) ? l.length : 0), 0);
  return [H.head('calendar', 'Jobs', n ? `${n} on a schedule` : 'what runs on its own, on a schedule'),
    h('div', { class: 'acts2' }, H.act('plus', 'New schedule', 'A statement run on the node, on a schedule', newSchedule), H.act('refresh', 'Refresh', 'Look again', () => H.detail())),
    ...kinds.flatMap((k, i) => [h('div', { class: 'dsect' }, k.title, Array.isArray(loaded[i]) && loaded[i].length ? h('span', { class: 'cnt' }, String(loaded[i].length)) : null),
      ...loaded[i]?.error ? [h('pre', { class: 'err' }, loaded[i].error)] : loaded[i].length ? loaded[i].map(k.item) : [h('div', { class: 'empty' }, k.empty)]])];
}
