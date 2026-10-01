// Jobs (ADR-034, round 29): what runs on its own, apart from History (what ran). A section a kind:
// the schedules (`pondra.tasks`, each with its last runs from `pondra.runs`) now; round 30's
// flows are another (`register.jobKind`, as an extension adds its own). Loaded when first shown.
import { h, secs, utc, icon, svg, R, S, run, rows, ident, quote, toast, menu, pop, prompt, confirmed, moreStyle, register } from './core.js';
import { highlighted } from './editor.js';
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
  const acts = [['play', 'Run now', 'Start it now, on the node (as a job: History shows it)', () => runNow(t)], ['pencil', 'Edit', 'Change how often it runs', () => edit(t)]];
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
async function edit(t) {
  const every = await prompt('Edit ' + t.name, 'How often', t.schedule, 'For example 15 minutes, 1 day, or cron 0 2 * * * UTC. Its next run is counted from now.');
  if (!every || every === t.schedule) return;
  try { await run(`CREATE OR REPLACE TASK ${ident(t.name)} SCHEDULE ${quote(every)} AS ${t.statement}`); toast(`${t.name}: ${cadence(every)}`); } catch (e) { toast(e.message, true); }
  H.detail();
}
async function drop(t) {
  if (!confirmed(`Drop the schedule ${t.name}? It stops running (its runs stay in History).`)) return;
  try { await run(`DROP TASK ${ident(t.name)}`); toast(`Dropped ${t.name}`); } catch (e) { toast(e.message, true); }
  H.detail();
}
/** A new schedule: its name, how often, and the statement it runs. */
function newSchedule() {
  const name = h('input', { placeholder: 'nightly_report', spellcheck: 'false' }), every = h('input', { value: '1 day', spellcheck: 'false' });
  const sql = h('textarea', { class: 'code', rows: 4, spellcheck: 'false', placeholder: "CALL run('queries/daily.sql')" });
  const make = async () => {
    try { await run(`CREATE TASK ${ident(name.value.trim())} SCHEDULE ${quote(every.value.trim())} AS ${sql.value.trim().replace(/;\s*$/, '')}`); toast(`Scheduled ${name.value.trim()}: ${cadence(every.value.trim())}`); H.detail(); }
    catch (e) { toast(e.message, true); }
  };
  pop('New schedule', h('div', { class: 'form' }, h('label', {}, 'Its name', name), h('label', {}, 'How often', every, h('small', {}, 'For example 15 minutes, 1 hour, 1 day, or cron 0 2 * * * UTC')), h('label', {}, 'What it runs', sql, h('small', {}, 'A statement: CALL run(\'a file\') runs a SQL file, a Python file or a notebook of the lake\'s'))),
    [['Schedule', make, true], ['Cancel', () => {}]]);
  requestAnimationFrame(() => name.focus());
}

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
