// An extension of Pondra's console: what one adds, in one small file (ADR-032).
//
//   PONDRA_CONSOLE_EXTENSIONS=examples/console-extension.js pondra serve lake
//
// The node serves it at /console/ext/0.js and the page loads it after its own module (several
// files: separated as PATH is). Everything the console shows is registered the same way, so an
// extension's sections, tabs, answers and actions sit beside the built-in ones.
const { register, on, ui, api } = window.pondra;

// A section on the left: what this page ran, newest first. Click one to put it in a new cell.
const ran = [];
let historyBox = null;
function drawHistory() {
  historyBox?.replaceChildren(...(ran.length
    ? ran.map(r => ui.line(null, { title: r.src, onclick: () => ui.add({ kind: r.kind, src: r.src }).edit() },
        ui.icon(r.kind === 'python' ? 't_other' : 'play'), ui.h('span', { class: 'nm' }, r.src.split('\n')[0])))
    : [ui.h('div', { class: 'empty' }, 'What you run shows here.')]));
}
register.section({ id: 'history', title: 'History', order: 35, render: box => { historyBox = box; drawHistory(); } });
on('ran', (cell, answer) => {
  if (!cell.src.trim() || answer?.kind === 'error') return;
  ran.unshift({ kind: cell.kind, src: cell.src });
  ran.length = Math.min(ran.length, 20);
  drawHistory();
});

// A tab of the details panel: the first rows of the table or view picked on the left.
register.panel({
  id: 'sample', title: 'Sample', order: 30,
  async render(box, picked) {
    if (picked?.type !== 'object') return [ui.h('div', { class: 'empty' }, 'Pick a table or a view to see its first rows.')];
    const rows = await api.rows(`SELECT * FROM ${picked.t.q} LIMIT 5`);
    return [ui.h('h4', {}, picked.t.q), ...rows.map(r => ui.h('pre', { class: 'said' }, JSON.stringify(r)))];
  },
});

// A view of an answer: one number, drawn large.
register.renderer({
  id: 'figure', order: 15,
  match: r => r.kind === 'rows' && r.rows.length === 1 && r.columns.length === 1 && /^(U?Int|Float|Decimal)/.test(r.columns[0].type),
  render: r => ui.h('div', { class: 'ext-figure', style: 'font-size:32px;font-weight:650;padding:4px 2px' }, String(r.rows[0][0]),
    ui.h('div', { style: 'font-size:12px;font-weight:400;color:var(--muted)' }, r.columns[0].name)),
});

// An action in the ⋯ menu: a link to this notebook, as saved in the lake.
register.action({
  id: 'link', menu: true, icon: 'copy', title: 'Copy a link to this notebook', order: 115,
  run: () => navigator.clipboard?.writeText(`${location.origin}/#notebook=${encodeURIComponent(window.pondra.state.name)}`).then(() => ui.toast('Link copied')),
});
