// The console (ADR-030, ADR-032, ADR-034): Pondra's workspace for SQL, Python, notebooks and data
// files, served by every node. This module is the shell — panes, views, tabs, the top and status
// bars — and `window.pondra`, the API an extension (or an enterprise build) adds to:
//
//   pondra.register.view({ id: 'jobs', title: 'Jobs', side: 'left', render: box => box.append(…) })
//   pondra.register.panel({ id: 'lineage', title: 'Lineage', render: (box, picked) => … })  // (a view on the right)
//   pondra.register.doc({ id: 'dash', match: p => p.endsWith('.dash'), open: async path => doc })
//   pondra.register.renderer({ id: 'map', match: r => …, render: (r, cell) => element })
//   pondra.register.action({ id: 'share', label: 'Share', run: () => … })
//   pondra.register.nav({ id: 'home', label: 'Home', icon: '<path …/>', run: () => … })
//   pondra.configure({ fetch, token, headers })   // (its own gateway and sign-in)
//   pondra.on('pick', picked => …)                 // (and 'run', 'ran', 'refresh', 'start', 'open', 'active')
//
// No framework and nothing from anywhere else: the page, its modules and its fonts come from the node.
import { h, $, fill, said, esc, store, count, bytes,  ICONS, icon, svg, typeMark, sqlType, on, emit, R, byOrder, shell, register, T, configure,
  MODE, SESSION, sessionOf, S, base, call, run, rows, doBlock, ident, qualified, home, toast, menu, VERSION, ask } from './core.js';
import { grid, copyText } from './grid.js';
import { Notebook, openNotebook, openPlain, cleanName, doneText } from './notebook.js';
import { workspace, drawWorkspace, treeItem, upload, newFolder, newAny, registerFiles, lastStatement } from './files.js';

R.helpers = {};
const H = R.helpers;
// The settings (theme, colours, fonts, layout): kept on this machine by the node (`/console/settings`),
// the same for every lake and session opened here; this browser's copy where the node can't keep them.
const PREFS = store.json('pondra.prefs', {});
let keeping = 0;
const keepPrefs = () => call('/console/settings', { method: 'PUT', body: JSON.stringify(PREFS), headers: { 'content-type': 'application/json' }, root: true }).then(() => { S.prefsHere = true; }, () => { S.prefsHere = false; });
const prefs = (k, v) => { if (v === undefined) return PREFS[k]; PREFS[k] = v; store.set('pondra.prefs', JSON.stringify(PREFS)); clearTimeout(keeping); keeping = setTimeout(() => { keeping = 0; keepPrefs(); }, 300); };
H.prefs = prefs;
async function machinePrefs() {
  try {
    const kept = await (await call('/console/settings', { root: true })).json();
    if (Object.keys(kept).length) { Object.assign(PREFS, kept); store.set('pondra.prefs', JSON.stringify(PREFS)); S.prefsHere = true; } else if (Object.keys(PREFS).length) keepPrefs(); // (this browser's, kept for the machine from now on)
  } catch { /* (an older node: this browser's) */ }
}
const narrow = () => innerWidth <= 760; // (as console.css's max-width:760px)
/** A side pane over the page, not beside it: the left one in a narrow window, the right one up to 1180px (console.css). */
const drawer = which => which === 'right' ? innerWidth <= 1180 : narrow();

// ------------------------------------------------------------------ the look: theme, colours and fonts (Settings)
const DARK = matchMedia('(prefers-color-scheme: dark)');
const themeNow = () => { const t = prefs('theme') || 'light'; return t === 'system' ? (DARK.matches ? 'dark' : 'light') : t; };
/** The surfaces from one background colour (a shade darker for the chrome, lighter for pop-ups on
 * dark), and the accent's tints from one accent: what Settings' two colours set, per theme. */
const TONES = ['--surface', '--chrome', '--side', '--sunk', '--head', '--line', '--line2', '--pop', '--accent', '--accent-soft', '--cellsel', '--rowsel'];
function look() {
  const d = document.documentElement, theme = prefs('theme') || 'light', now = themeNow(), c = prefs('colors')?.[now] || {};
  if (theme === 'system') delete d.dataset.theme; else d.dataset.theme = theme;
  d.classList.toggle('sysfont', prefs('font') === 'system');
  for (const k of TONES) d.style.removeProperty(k);
  const mix = (p, to) => `color-mix(in srgb, ${c.bg} ${100 - p}%, ${to})`, dk = p => mix(p, '#000'), lt = p => mix(p, '#fff');
  const set = o => Object.entries(o).forEach(([k, v]) => d.style.setProperty(k, v));
  if (c.bg) set(now === 'dark' ? { '--surface': c.bg, '--chrome': dk(22), '--side': dk(12), '--sunk': dk(12), '--head': lt(5), '--line': lt(12), '--line2': lt(7), '--pop': lt(6) }
    : { '--surface': c.bg, '--chrome': dk(4), '--side': dk(2.5), '--sunk': dk(3), '--head': dk(5.5), '--line': dk(11), '--line2': dk(7.5), '--pop': c.bg });
  if (c.accent) set({ '--accent': c.accent, '--accent-soft': `color-mix(in srgb, ${c.accent} 14%, var(--surface))`, '--cellsel': `color-mix(in srgb, ${c.accent} 12%, var(--surface))`, '--rowsel': `color-mix(in srgb, ${c.accent} 8%, var(--surface))` });
}
DARK.addEventListener('change', look);

// ------------------------------------------------------------------ panes: left, bottom, right
const PANE = { left: '#left', right: '#right' };
function pane(which, open) {
  const was = paneOpen(which);
  if (open === undefined) open = !was;
  if (which === 'bottom') { prefs('bottom', open); S.doc?.el.classList.toggle('nopanel', !open); }
  else { $(PANE[which]).hidden = !open; if (!drawer(which)) prefs(which, open); if (which === 'right' && open) drawRight(); }
  drawPanes(); drawTop();
}
H.pane = pane;
const paneOpen = which => which === 'bottom' ? prefs('bottom') !== false : !$(PANE[which]).hidden;
function drawPanes() {
  const one = (which, name, label, keys) => {
    const none = which === 'bottom' && !S.doc?.hasPanel, on = !none && paneOpen(which);
    return h('button', { class: 'icon pane' + (on ? ' on' : ''), disabled: none, 'aria-pressed': String(on), title: `${label} (${keys})${none ? ': this file has none' : ''}`, 'aria-label': label, html: svg(name + (on ? '_on' : ''), 18), onclick: () => pane(which) });
  };
  $('#panes').replaceChildren(one('left', 'paneL', 'Show or hide the left pane', 'Ctrl B'), one('bottom', 'paneB', 'Show or hide the bottom panel', 'Ctrl J'), one('right', 'paneR', 'Show or hide the right pane', 'Ctrl Alt B'));
}

/** The top bar's History and Jobs buttons: pressed while theirs shows; pressed again, the pane closes. */
function drawTop() { for (const [id, b] of [['runs', '#runsBtn'], ['jobs', '#jobsBtn']]) { const on = !$('#right').hidden && S.tab === id; $(b).classList.toggle('on', on); $(b).setAttribute('aria-pressed', String(on)); } }

// ------------------------------------------------------------------ views: groups on the left, tabs on the right; either moves to the other side
const sideOf = v => prefs('sides')?.[v.id] || v.side;
const viewTitle = v => typeof v.title === 'function' ? v.title() : v.title;
const folded = v => (prefs('folded') || []).includes(v.id);
function fold(v, on = !folded(v)) { prefs('folded', [...new Set((prefs('folded') || []).filter(x => x !== v.id).concat(on ? [v.id] : []))]); drawLeft(); }
/** The order of the left groups: Data first, unless Settings says Workspace first. */
const leftViews = () => R.views.filter(v => sideOf(v) === 'left').map(v => ({ v, o: v.id === 'workspace' && prefs('workspaceFirst') ? 5 : v.order ?? 50 })).sort((a, b) => a.o - b.o).map(x => x.v);
async function renderView(v) {
  v.box ||= h('div', { id: v.id, role: v.tree !== false ? 'tree' : null, class: 'vbox', 'aria-label': viewTitle(v) });
  const n = v.n = (v.n || 0) + 1; // (a slow answer for an earlier pick never covers a later pick)
  try {
    const out = await v.render(v.box, S.pick);
    if (n === v.n && Array.isArray(out)) v.box.replaceChildren(...out.filter(Boolean));
  } catch (e) {
    if (n === v.n) v.box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to see this.' : e.message));
  }
}
function drawLeft() {
  const views = leftViews(), weights = prefs('weights') || {};
  const parts = [];
  views.forEach((v, i) => {
    renderOnce(v);
    const f = folded(v), tools = (v.tools || []).filter(t => !t.hidden?.()).map(t => h('button', { class: 'icon sm', title: t.title, 'aria-label': t.title, id: t.domId || null, onclick: e => { e.stopPropagation(); t.run(e); } }, icon(t.icon)));
    const head = h('div', { class: 'ghead', draggable: 'true', ondragstart: e => e.dataTransfer.setData(VIEW, v.id), oncontextmenu: e => { e.preventDefault(); viewMenu(e, v); } }, h('button', { class: 'gtitle', 'aria-expanded': String(!f), onclick: () => fold(v) }, h('span', { class: 'tw', html: svg(f ? 'chev' : 'chevd', 14, 2) }), h('h2', { id: v.id + 'Title' }, viewTitle(v))),
      h('span', { class: 'gtools' }, tools, h('button', { class: 'icon sm', title: `${viewTitle(v)}: more`, 'aria-label': `${viewTitle(v)}: more`, onclick: e => viewMenu(e.currentTarget, v) }, icon('dots'))));
    const g = h('section', { class: 'group' + (f ? ' folded' : ''), 'data-view': v.id, style: !f && weights[v.id] ? `flex:${weights[v.id]} 1 0` : null }, head, f ? null : v.box);
    if (i && !f && parts.length && !parts.at(-1).classList.contains('folded')) parts.push(divider(parts.at(-1), g));
    parts.push(g);
  });
  $('#left').replaceChildren(FILTER, ...parts.length ? parts : [h('div', { class: 'empty pad' }, 'Nothing here: views moved to the right pane come back with their ⋯.')], EDGE);
  filterLeft();
}
const EDGE = $('#leftEdge'); // (the edge of the left pane: kept as its groups are drawn again)
/** The left pane's filter: the trees show the names that hold what is typed (and what they are in). */
const FILTER = h('label', { class: 'lfilter' }, icon('filter'), h('input', { id: 'filter', type: 'search', placeholder: 'Filter', autocomplete: 'off', spellcheck: 'false', 'aria-label': 'Filter the tables and files',
  oninput: e => { S.filter = e.target.value.trim().toLowerCase(); filterLeft(); }, onkeydown: e => { if (e.key === 'Escape' && e.target.value) { e.stopPropagation(); e.target.value = ''; S.filter = ''; filterLeft(); } } }));
function filterLeft() {
  const q = S.filter || '', name = row => (row.querySelector(':scope > .nm')?.textContent || '').toLowerCase();
  const walk = (el, keep) => { // → whether el, or something in it, holds q
    const item = el.classList.contains('item'), row = item ? el.firstElementChild : el;
    if (!row?.classList.contains('row')) return false;
    const self = !!q && name(row).includes(q);
    let below = false;
    if (item) for (const k of el.children[1].children) below = walk(k, keep || self) || below;
    el.classList.toggle('fhide', !!q && !(keep || self || below));
    if (item) el.classList.toggle('fopen', !!q && below);
    return self || below;
  };
  for (const box of $('#left').querySelectorAll('.vbox')) for (const k of box.children) walk(k, false);
}
new MutationObserver(() => { if (S.filter && !filterLeft.soon) filterLeft.soon = requestAnimationFrame(() => { filterLeft.soon = 0; filterLeft(); }); }).observe($('#left'), { childList: true, subtree: true });

/** The side panes' edges: dragged (or arrows, when focused), they set a pane's width; a double-click resets it. */
function edges() {
  for (const [which, sign] of [['left', 1], ['right', -1]]) {
    const e = $(`#${which}Edge`), el = $(PANE[which]), keep = () => prefs('widths', { ...prefs('widths'), [which]: el.offsetWidth });
    const set = w => { el.style.width = Math.round(Math.max(200, Math.min(innerWidth * 0.5, w))) + 'px'; e.setAttribute('aria-valuenow', String(el.offsetWidth || w)); };
    e.setAttribute('aria-valuemin', '200'); e.setAttribute('aria-valuenow', String(el.offsetWidth || (which === 'left' ? 272 : 320)));
    e.addEventListener('pointerdown', ev => {
      ev.preventDefault(); e.setPointerCapture(ev.pointerId); e.classList.add('drag');
      const w0 = el.offsetWidth, x0 = ev.clientX, move = m => set(w0 + sign * (m.clientX - x0));
      e.addEventListener('pointermove', move);
      e.addEventListener('pointerup', () => { e.removeEventListener('pointermove', move); e.classList.remove('drag'); keep(); }, { once: true });
    });
    e.addEventListener('keydown', ev => { const d = { ArrowLeft: -16, ArrowRight: 16 }[ev.key]; if (d) { ev.preventDefault(); set(el.offsetWidth + sign * d); keep(); } });
    e.addEventListener('dblclick', () => { el.style.width = ''; prefs('widths', { ...prefs('widths'), [which]: undefined }); });
    const w = prefs('widths')?.[which];
    if (w) set(w);
  }
}
/** A divider between two open groups: dragged, it shares their height. */
function divider(a, b) {
  const d = h('div', { class: 'gsplit', role: 'separator', 'aria-orientation': 'horizontal' });
  d.addEventListener('pointerdown', e => {
    e.preventDefault(); d.setPointerCapture(e.pointerId);
    const ha = a.offsetHeight, hb = b.offsetHeight, y0 = e.clientY;
    const move = ev => {
      const dy = Math.max(60 - ha, Math.min(hb - 60, ev.clientY - y0)), w = { ...prefs('weights') || {} };
      w[a.dataset.view] = (ha + dy) / 100; w[b.dataset.view] = (hb - dy) / 100;
      a.style.flex = `${w[a.dataset.view]} 1 0`; b.style.flex = `${w[b.dataset.view]} 1 0`;
      prefs('weights', w);
    };
    d.addEventListener('pointermove', move);
    d.addEventListener('pointerup', () => d.removeEventListener('pointermove', move), { once: true });
  });
  return d;
}
const renderOnce = v => { if (!v.drawn) { v.drawn = true; renderView(v); } else v.box ||= h('div', { id: v.id, class: 'vbox' }); };
/** The right pane's views, in the order their tabs were dragged into (else their own). */
const rightViews = () => { const o = prefs('rorder') || [], at = v => { const i = o.indexOf(v.id); return i < 0 ? 99 : i; }; return R.views.filter(v => sideOf(v) === 'right').sort((a, b) => at(a) - at(b) || byOrder(a, b)); };
/** A right-pane tab moved: before `to` (dragged onto it), or `by` one place (its menu). */
function moveTab(id, to, by) {
  const ids = rightViews().map(v => v.id), i = ids.indexOf(id);
  ids.splice(i, 1);
  ids.splice(to ? Math.max(0, ids.indexOf(to) + (by || 0)) : Math.max(0, Math.min(ids.length, i + by)), 0, id);
  prefs('rorder', ids); drawRight();
}
function drawRight() {
  const views = rightViews();
  if (!views.some(v => v.id === S.tab)) S.tab = views[0]?.id;
  const tab = v => h('button', { class: 'rtab', role: 'tab', id: 'rtab-' + v.id, tabindex: v.id === S.tab ? '0' : '-1', 'aria-selected': String(v.id === S.tab), 'aria-controls': 'rbody', draggable: 'true', title: 'Drag it to move it',
    onclick: () => { S.tab = v.id; drawRight(); drawTop(); }, oncontextmenu: e => { e.preventDefault(); viewMenu(e, v); }, ondragstart: e => e.dataTransfer.setData(VIEW, v.id) }, viewTitle(v));
  const cur = views.find(v => v.id === S.tab);
  fill($('#rtabs'), h('div', { class: 'tlist', role: 'tablist', 'aria-label': 'The right pane' }, views.map(tab)), h('span', { class: 'grow' }), cur ? h('button', { class: 'icon sm', title: `${viewTitle(cur)}: more`, 'aria-label': `${viewTitle(cur)}: more`, onclick: e => viewMenu(e.currentTarget, cur) }, icon('dots')) : null);
  if (!cur || $('#right').hidden) return;
  cur.box ||= h('div', { id: cur.id, class: 'vbox' });
  $('#rbody').replaceChildren(cur.box);
  $('#rbody').setAttribute('aria-labelledby', 'rtab-' + cur.id);
  renderView(cur);
}
const drawViews = () => { drawLeft(); drawRight(); };
/** A view dragged by its group's head or its tab: onto the other pane it moves there, onto a tab of
 * the right pane it goes beside it (tabs.js). */
const VIEW = 'text/x-pondra-view';
function viewDrops() {
  let lit;
  const light = x => { if (lit !== x) { lit?.classList.remove('drop'); (lit = x)?.classList.add('drop'); } };
  for (const el of [$('#left'), $('#rtabs')]) {
    el.addEventListener('dragover', e => { if (e.dataTransfer.types.includes(VIEW)) { e.preventDefault(); light(e.target.closest('.rtab') || el); } });
    el.addEventListener('dragleave', e => { if (!el.contains(e.relatedTarget)) light(null); });
    el.addEventListener('drop', e => { const id = e.dataTransfer.getData(VIEW), t = e.target.closest('.rtab'); light(null); if (id) { e.preventDefault(); menus().then(m => m.dropView(id, el === $('#left') ? 'left' : 'right', t?.id.slice(5), t && e.offsetX > t.offsetWidth / 2)); } });
  }
}
/** The right pane's view again (what was picked changed). */
function detail() { if (!$('#right').hidden) { const v = R.views.find(x => x.id === S.tab && sideOf(x) === 'right'); if (v) renderView(v); } }
function show(id) { const v = R.views.find(x => x.id === id); if (!v) return; if (sideOf(v) === 'right') { S.tab = id; pane('right', true); } else { pane('left', true); fold(v, false); } }
H.show = show;
async function refreshViews(ids) { await Promise.all(R.views.filter(v => (!ids || ids.includes(v.id)) && v.drawn !== false && (sideOf(v) === 'left' || v.id === S.tab)).map(renderView)); }

// ------------------------------------------------------------------ documents in tabs
function addDoc(doc) {
  S.docs.push(doc);
  emit('open', doc);
  activate(doc);
  remember();
  return doc;
}
/** Where a tab was scrolled to, and what in it had the focus: kept while another is in front (out of
 * the page, its elements forget), given back when it comes forward again. */
const keep = d => { if (d?.el.isConnected) d.was = [[d.el, ...d.el.querySelectorAll('*')].filter(e => e.scrollTop || e.scrollLeft).map(e => [e, e.scrollTop, e.scrollLeft]), d.el.contains(document.activeElement) ? document.activeElement : null]; };
const back = was => { if (!was) return; for (const [e, t, l] of was[0]) { e.scrollTop = t; e.scrollLeft = l; } was[1]?.focus({ preventScroll: true }); };
function activate(doc) {
  if (!doc) return;
  if (doc !== S.doc) keep(S.doc);
  S.doc = doc;
  $('#docs').replaceChildren(doc.el);
  doc.el.classList.toggle('nopanel', prefs('bottom') === false);
  doc.activate?.();
  drawTabs(); toolbar(); status(); drawPanes();
  back(doc.was); doc.was = null;
  if (doc.kind === 'notebook') S.nb = doc;
  follow(doc);
  document.title = `${doc.dirty ? '• ' : ''}${doc.title} · Pondra`;
  laterWorkspace();
  if (narrow()) pane('left', false);
  emit('active', doc);
  hashNow();
}
H.activate = activate;
async function closeDoc(doc) {
  if (!(await doc.close?.() ?? true)) return;
  const i = S.docs.indexOf(doc);
  S.docs.splice(i, 1);
  emit('close', doc);
  if (doc.session) call('/sessions/' + doc.session, { method: 'DELETE' }).catch(() => {}); // (its temporary tables, variables and Python go with it)
  if (S.doc === doc) {
    S.doc = null;
    if (S.docs.length) activate(S.docs[Math.min(i, S.docs.length - 1)]);
    else { $('#docs').replaceChildren(welcome()); drawTabs(); toolbar(); status(); drawPanes(); document.title = 'Pondra'; }
  } else drawTabs();
  if (S.nb === doc) S.nb = S.docs.filter(d => d.kind === 'notebook').at(-1) || null;
  remember(); laterWorkspace();
}
// (the tabs: the pinned first, always in sight; the others scroll under them — the wheel scrolls
// them, a thin bar above them shows where they are, ⌄ lists them all; right-click: pin, close some)
function drawTabs() {
  const tab = (d, i) => {
    const t = h('div', { class: 'tab' + (d === S.doc ? ' on' : '') + (d.pinned ? ' pinned' : ''), role: 'tab', tabindex: d === S.doc ? '0' : '-1', 'aria-selected': String(d === S.doc), title: (d.tip || (d.path ? `files/${d.path}` : d.title)) + (d.pinned ? ' (pinned)' : ''), 'aria-keyshortcuts': 'Delete',
      draggable: 'true', ondragstart: e => e.dataTransfer.setData('text/x-pondra-doc', i),
      onclick: e => { if (!e.target.closest('.x')) activate(d); }, onauxclick: e => { if (e.button === 1) closeDoc(d); }, oncontextmenu: e => { e.preventDefault(); menus().then(m => m.tabMenu(e, d)); } },
      h('span', { class: 'ic k-' + d.kind, html: svg(d.icon, 15) }), h('span', { class: 'tn' }, d.title),
      d.dirty ? h('span', { class: 'dirty', title: 'Not saved', 'aria-label': 'not saved' }) : null,
      d.pinned ? h('span', { class: 'x pin', title: `Unpin ${d.title}`, 'aria-hidden': 'true', html: svg('pin', 14), onclick: () => pin(d, false) })
        : h('span', { class: 'x', title: `Close ${d.title} (Delete, when its tab has the focus)`, 'aria-hidden': 'true', html: svg('close', 14), onclick: () => closeDoc(d) })); // (a tab holds no other control: the keyboard closes it with Delete)
    t.dataset.i = i;
    return t;
  };
  const tabs = S.docs.map(tab), pins = h('span', { class: 'pins' }, tabs.filter((_, i) => S.docs[i].pinned));
  const list = h('div', { class: 'tlist', role: 'tablist', 'aria-label': 'Open files' }, pins, tabs.filter((_, i) => !S.docs[i].pinned));
  const all = h('button', { class: 'icon tmore', title: 'Every open tab', 'aria-label': 'Every open tab', 'aria-haspopup': 'menu', hidden: true, html: svg('chevd', 15), onclick: e => { const at = e.currentTarget; menus().then(m => m.tabList(at)); } });
  $('#tabbar').replaceChildren(list, h('div', { class: 'tthumb', 'aria-hidden': 'true' }), all, h('button', { class: 'icon newtab', title: 'New: a notebook, a file or a folder', 'aria-label': 'New', html: svg('plus', 16), onclick: e => newMenu(e.currentTarget) }));
  if (S.doc) document.title = `${S.doc.dirty ? '• ' : ''}${S.doc.title} · Pondra`;
  list.style.scrollPaddingLeft = pins.offsetWidth + 'px'; // (a tab scrolled to is not under the pinned)
  thumb(); // (⌄ shown first: the room left for the tabs is then known)
  $('#tabbar .tab.on')?.scrollIntoView({ block: 'nearest', inline: 'nearest' }); // (the tab in front in sight, its × too)
}
H.drawTabs = drawTabs;
/** Where the tabs are scrolled to: a thin bar above them (dragged, it scrolls them), shown when they don't all fit. */
function thumb() {
  const list = $('#tabbar .tlist'), th = $('#tabbar .tthumb');
  if (!list || !th) return;
  const w = list.clientWidth, all = list.scrollWidth, over = all > w + 1;
  th.hidden = !over; $('#tabbar .tmore').hidden = !over;
  if (over) Object.assign(th.style, { width: Math.max(24, w * w / all) + 'px', left: list.offsetLeft + list.scrollLeft * w / all + 'px' });
}
{
  const bar = $('#tabbar');
  bar.addEventListener('scroll', thumb, true);
  bar.addEventListener('wheel', e => { const list = e.target.closest('.tlist'); if (list && Math.abs(e.deltaY) > Math.abs(e.deltaX)) { list.scrollLeft += e.deltaY; e.preventDefault(); } }, { passive: false });
  bar.addEventListener('pointerdown', e => {
    const th = e.target.closest('.tthumb'), list = $('#tabbar .tlist');
    if (!th) return;
    e.preventDefault(); th.setPointerCapture(e.pointerId); th.classList.add('drag');
    const x0 = e.clientX, s0 = list.scrollLeft, k = list.scrollWidth / list.clientWidth, move = ev => { list.scrollLeft = s0 + (ev.clientX - x0) * k; };
    th.addEventListener('pointermove', move);
    th.addEventListener('pointerup', () => { th.removeEventListener('pointermove', move); th.classList.remove('drag'); }, { once: true });
  });
  new ResizeObserver(() => { thumb(); $('#tabbar .tab.on')?.scrollIntoView({ block: 'nearest', inline: 'nearest' }); }).observe(bar); // (narrower, the tab in front still in sight)
  // a tab dragged onto another goes before it (after it, past its middle); pinned ones stay in front of the rest
  const over = e => e.dataTransfer.types.includes('text/x-pondra-doc') && e.target.closest('.tab');
  bar.addEventListener('dragover', e => { const t = over(e); bar.querySelector('.tab.drop')?.classList.remove('drop'); if (t) { e.preventDefault(); t.classList.add('drop'); } });
  bar.addEventListener('drop', e => {
    const t = over(e), from = S.docs[e.dataTransfer.getData('text/x-pondra-doc')];
    if (!t || !from) return;
    e.preventDefault();
    const to = S.docs[t.dataset.i];
    S.docs.splice(S.docs.indexOf(from), 1);
    S.docs.splice(S.docs.indexOf(to) + (e.clientX > t.getBoundingClientRect().left + t.offsetWidth / 2), 0, from);
    drawTabs(); remember();
  });
  bar.addEventListener('dragend', () => bar.querySelector('.tab.drop')?.classList.remove('drop'));
}
/** Pin a tab (to the left, always in sight, not closed with the others), or unpin it. */
H.pin = pin;
function pin(d, on) {
  d.pinned = on;
  S.docs.splice(S.docs.indexOf(d), 1);
  S.docs.splice(S.docs.filter(x => x.pinned && x !== d).length, 0, d); // (the last of the pinned, or the first of the others)
  drawTabs(); remember();
}
function toolbar() {
  const bar = $('#docbar');
  bar.hidden = !S.doc;
  if (S.doc) bar.replaceChildren(...S.doc.toolbar().flat().filter(Boolean));
}
H.toolbar = toolbar;
H.copyPath = d => copyText('files/' + d.path, 'Path copied');
function status() {
  const s = S.info || {}, nodes = s.nodes || [];
  const where = h('span', { class: 'st-where', title: s.leader ? `This database's cluster: ${nodes.join(', ')}. Its leader is ${s.leader}; commits so far: ${s.hwm}.` : '' }, h('span', { class: 'dot' + (S.down ? ' off' : '') }), `${home() || ''}: ${S.down ? 'not reachable' : 'ready'}`);
  $('#stl').replaceChildren(where, ...(S.doc?.status?.() || []).filter(Boolean).slice(0, 1).map(t => h('span', {}, t)));
  fill($('#str'), ...(S.doc?.status?.() || []).filter(Boolean).slice(1).map(t => h('span', {}, t)), s.role && nodes.length > 1 ? h('span', {}, `${s.role} · ${nodes.length} nodes`) : null, h('span', {}, `Pondra ${VERSION}`));
}
H.status = status;
function welcome() {
  const b = ([, ic, label, fn]) => h('button', { class: 'btn', onclick: fn }, icon(ic), label);
  return h('div', { class: 'doc welcome' }, h('div', {}, h('h1', {}, 'Pondra'), h('p', {}, 'Open a file from the Workspace, a table from Data, or start something new.'), h('div', { class: 'acts2' }, NEW.filter(x => x[0] !== 'uploaddir').map(b))));
}
/** What the page makes new, for its welcome page, + menus, ⋯ menu and search: [id, icon, label, run]. */
const NEW = [['newnb', 'notebook', 'New notebook', () => newNotebook()], ['newsql', 'filesql', 'New SQL file', () => newFile('sql')], ['newpy', 'filepy', 'New Python file', () => newFile('python')],
  ['newany', 'file', 'New file…', () => newAny()], ['newdir', 'folder', 'New folder', () => newFolder()], ['upload', 'up', 'Upload files…', () => upload()], ['uploaddir', 'folder', 'Upload a folder…', () => upload('', true)]];
const newMenu = at => menu(at, [...NEW.slice(0, -2), '-', ...NEW.slice(-2)].map(x => x === '-' ? x : { icon: x[1], label: x[2], run: x[3] }));
let untitled = 0;
/** A name for a new file in `dir`: `untitled`, or `untitled-2`… (one no tab has). */
const taken = p => S.docs.some(d => (d.path || d.untitled) === p);
const nextName = (dir, ext) => { let n = 'untitled'; while (taken(dir + n + ext)) n = `untitled-${++untitled + 1}`; return n; };
/** A new notebook: one file, `<dir><name>.ipynb`, saved in place (the node keeps its versions). */
function newNotebook(nb = { cells: [] }, name, dir = 'notebooks/') {
  const d = addDoc(new Notebook({ name: name || nextName(dir, '.ipynb'), nb, dir }));
  if (!nb.cells.length) d.cells[0].edit();
  return d;
}
H.openNotebook = (nb, name) => newNotebook(nb, cleanName(name || '') || 'untitled');
H.newNotebook = dir => newNotebook(undefined, undefined, dir + '/');
H.versions = doc => import('./versions.js').then(m => m.open(doc));
/** A new SQL or Python file, to be saved in `at` (asked again when it is saved). */
function newFile(kind, at = kind === 'python' ? 'scripts/' : 'queries/') {
  const ext = kind === 'python' ? '.py' : '.sql', untitled = at + nextName(at, ext) + ext, made = d => { addDoc(d); d.ed.focus(); return d; };
  return kind === 'python' ? import('./pyfile.js').then(m => made(new m.PythonDoc({ untitled }))) : import('./sqlfile.js').then(m => made(new m.SqlDoc({ untitled }))); // (a promise of it: its module loaded when first needed)
}
H.newFile = newFile;
/** A new tab holding `text` (a definition, a job's code, a script): unchanged until it is typed in, so it closes without asking. */
H.newWith = async (kind, text) => { const d = await newFile(kind); d.ed.value = text; d.paramsBar?.(); d.marks?.(); d.ed.focus(); return d; };
H.close = closeDoc;
/** Open a lake file (a path under `files/`, or `notebooks/<name>`) in its tab: the open one comes forward. */
const opening = new Map(); // (a path being opened: a second click waits for the same tab)
function openFile(path, opts = {}) {
  path = path.replace(/^\/?(files\/)?/, '');
  const open = S.docs.find(d => d.path === path);
  if (open) { activate(open); return Promise.resolve(open); }
  if (!opening.has(path)) opening.set(path, opened(path, opts).finally(() => opening.delete(path)));
  return opening.get(path);
}
async function opened(path, opts) {
  try {
    const nb = path.match(/^notebooks\/([^/]+?)(?:\/([^/]+)\.ipynb)?$/);
    if (nb && !/\.ipynb$/i.test(nb[1])) {
      if (!nb[2] && !opts.version) try { return addDoc(await openPlain(path + '.ipynb')); } catch { /* (saved before versions: its newest save) */ }
      return addDoc(await openNotebook(nb[1], nb[2] || opts.version));
    }
    const kind = R.docs.find(d => d.match(path));
    if (!kind) { pick({ type: 'file', f: S.files?.find(f => f.path === 'files/' + path) || { path: 'files/' + path, rel: path, name: path.split('/').pop() } }); return null; }
    const doc = await kind.open(path, opts);
    doc.path ||= path;
    return addDoc(doc);
  } catch (e) { if (!opts.quiet) toast(`Could not open ${path}: ${e.message}`, true); return null; }
}
H.openFile = openFile;
/** SQL in a tab and run: into the notebook in front (a cell), else a new SQL tab; `live`: always a
 * SQL tab, its answer followed live. */
async function query(sql, live) {
  if (S.doc?.kind === 'notebook' && !live) return S.doc.peek(sql);
  const p = S.pick, d = await newFile('sql');
  if (p && p.type !== 'file' && p.type !== 'doc') { S.pick = p; S.pickedOn = d; mark(); detail(); } // (a table's first rows, in a tab of their own: its details stay)
  d.ed.value = sql; d.live = live;
  d.run();
}
H.query = query;
/** The open tabs, remembered in this browser (per database), and opened again next time. */
const kept = d => d?.path && (d.kind !== 'notebook' || d.version); // (a tab that can open again: saved)
function remember() { const k = home() || ''; store.set('pondra.tabs:' + k, JSON.stringify(S.docs.filter(kept).map(d => d.path))); store.set('pondra.pins:' + k, JSON.stringify(S.docs.filter(d => d.pinned && kept(d)).map(d => d.path))); }
async function restoreTabs() {
  const paths = store.json('pondra.tabs:' + (home() || ''), []), pins = store.json('pondra.pins:' + (home() || ''), []);
  for (const p of paths.slice(0, 16)) await openFile(p, { quiet: true }); // (one gone since is left out)
  for (const d of S.docs) if (pins.includes(d.path)) d.pinned = true;
  S.docs.sort((a, b) => !!b.pinned - !!a.pinned); drawTabs();
}
function hashNow() {
  const p = new URLSearchParams();
  if (S.db && MODE === 'lakes') p.set('db', S.db);
  if (S.doc?.kind === 'notebook' && S.doc.version && !S.doc.plain) p.set('notebook', S.doc.name);
  else if (kept(S.doc)) p.set('file', S.doc.path);
  history.replaceState(null, '', p.size ? '#' + p : location.pathname);
}

// ------------------------------------------------------------------ the Data view: databases, schemas, tables, views, columns
const KIND = { table: ['table', 'table'], view: ['view', 'view'], 'materialized view': ['matview', 'materialized view'], files: ['files', 'view of files'] };
async function catalog() {
  const [tables, columns, objects, schemata] = await Promise.all([
    rows(`SELECT table_catalog AS c, table_schema AS s, table_name AS t, table_type AS k FROM information_schema.tables WHERE table_schema <> 'information_schema' AND table_schema NOT IN (SELECT catalog_name FROM information_schema.schemata) ORDER BY 1, 2, 3`),
    rows(`SELECT table_catalog AS c, table_schema AS s, table_name AS t, column_name AS n, data_type AS d FROM information_schema.columns WHERE table_schema <> 'information_schema' ORDER BY 1, 2, 3, ordinal_position`),
    call('/objects').then(r => r.json(), () => ({ objects: [] })),
    rows(`SELECT catalog_name AS c, schema_name AS s FROM information_schema.schemata WHERE schema_name <> 'information_schema' AND schema_name NOT IN (SELECT catalog_name FROM information_schema.schemata)`),
  ]);
  S.filesAt = objects.files;
  const about = new Map(objects.objects.map(o => [`${o.catalog}\u0000${o.schema}\u0000${o.name}`, o]));
  const lakes = new Map(), find = new Map();
  for (const x of schemata) (lakes.get(x.c) || lakes.set(x.c, new Map()).get(x.c)).set(x.s, []); // (a schema with no tables yet too)
  for (const t of tables) {
    const schemas = lakes.get(t.c) || lakes.set(t.c, new Map()).get(t.c);
    const list = schemas.get(t.s) || schemas.set(t.s, []).get(t.s);
    const k = `${t.c}\u0000${t.s}\u0000${t.t}`;
    const table = { ...t, columns: [], o: about.get(k) || { kind: t.k === 'VIEW' ? 'view' : 'table' }, q: qualified(t.c, t.s, t.t), key: 'o:' + k };
    list.push(table); find.set(k, table);
  }
  for (const c of columns) find.get(`${c.c}\u0000${c.s}\u0000${c.t}`)?.columns.push(c);
  S.objects = [...find.values()];
  return lakes;
}
const openKey = (key, dflt) => S.open.has(key) || (dflt && !S.open.has('closed:' + key));
function lakeNode(name, schemas, current, note, depth = 0) {
  const key = 'db:' + name;
  const kids = schemas ? h('div', { class: 'kids', role: 'group', hidden: !openKey(key, current) }) : null;
  if (schemas) {
    const names = [...schemas.keys()].sort((a, b) => (a !== 'public') - (b !== 'public') || a.localeCompare(b));
    kids.append(...names.map(s => schemaNode(name, s, schemas.get(s), names.length === 1, current)));
    if (!names.length) kids.append(h('div', { class: 'empty' }, 'No tables yet.'));
    if (current) kids.append(...(R.objectKinds || []).filter(g => !g.schema).map(g => groupNode(name, g)));
  }
  return treeItem({ key, kids, depth, icon: 'db', iconCls: 'k-db', name, cls: current ? 'cur' : '', meta: note, dataKind: 'database', menu: e => objects(m => m.lakeMenu(e, name, current)),
    title: MODE === 'lakes' && !current ? `Use database ${name}` : name, onclick: tw => { if (MODE === 'lakes' && name !== S.db) use(name); else tw.click(); } });
}
/** The lake's other objects, a group each (secrets; users and roles, flows, an extension's:
 * `register.objectKind`), and a schema's (`schema: true`: functions, procedures, schedules, as
 * Postgres keeps them), filled when opened (objects.js). */
const objects = f => import('./objects.js').then(f);
const groups = f => import('./groups.js').then(f); // (functions, procedures, schedules, secrets)
function groupNode(lake, g, schema) {
  const key = `g:${lake}.${schema ? schema + '.' : ''}${g.id}`, kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(key) }, h('div', { class: 'empty' }, '…'));
  const fill = () => groups(m => m.fill(g.id, kids, schema));
  if (!kids.hidden) fill();
  return treeItem({ key, kids, depth: schema ? 2 : 1, icon: g.icon, iconCls: 'k-group', name: g.title, dataKind: 'group', onopen: fill, onclick: tw => tw.click(), menu: e => groups(m => m.groupMenu(e, g.id, schema)) });
}
function schemaNode(lake, schema, tables, only, current) {
  const key = `s:${lake}.${schema}`;
  const kids = h('div', { class: 'kids', role: 'group', hidden: !openKey(key, only || schema === 'public') }, tables.map(t => tableNode(t)), current ? (R.objectKinds || []).filter(g => g.schema).map(g => groupNode(lake, g, schema)) : null);
  return treeItem({ key, kids, depth: 1, icon: 'schema', iconCls: 'k-schema', name: schema, title: `schema ${schema}`, dataKind: 'schema', onclick: tw => tw.click(), menu: e => objects(m => m.schemaMenu(e, lake, schema)) });
}
function tableNode(t) {
  // (its columns a level in, under its name, past its guide)
  const [ic, word] = KIND[t.o.kind] || KIND.table, keyed = new Set(t.o.key || []);
  const kids = h('div', { class: 'kids', role: 'group', hidden: !S.open.has(t.key) }, t.columns.map(c =>
    h('div', { class: 'row col', role: 'treeitem', tabindex: '-1', 'aria-level': '4', style: 'padding-left:69px', title: `${c.n}: ${sqlType(c.d)} (${c.d}). Double-click: put the name where you are typing; right-click: more`, onclick: e => { for (const r of document.querySelectorAll('#data .row.on')) r.classList.remove('on'); e.currentTarget.classList.add('on'); }, ondblclick: () => S.doc?.put?.(ident(c.n)), oncontextmenu: e => { e.preventDefault(); objects(m => m.columnMenu(e, t, c)); } },
      typeMark(c.d), h('span', { class: 'nm' }, c.n), keyed.has(c.n) ? icon('key', 'kk') : null, h('span', { class: 'ty' }, sqlType(c.d)))));
  const it = treeItem({ key: t.key, kids: t.columns.length ? kids : null, depth: 2, icon: ic, iconCls: 'k-table', name: t.t, dataKey: t.key, dataKind: t.o.kind, on: S.pick?.type === 'object' && S.pick.t.key === t.key,
    title: `${t.q}: a ${word}. Click: its details; double-click: open it (its rows${t.o.kind === 'table' ? ', to edit' : ''}); right-click: more`, onclick: () => pick({ type: 'object', t }), ondblclick: () => openFile('table:' + t.q), menu: e => objects(m => m.tableMenu(e, t)) });
  return it;
}
async function dataTree(box) {
  if (MODE === 'lakes') {
    let dbs;
    try { dbs = await (await call('/databases', { root: true })).json(); } catch (e) {
      box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to list the databases.' : e.message));
      return;
    }
    S.dbs = dbs;
    if (!S.db || !dbs.some(d => d.name === S.db)) S.db = (dbs.find(d => d.default) || dbs[0])?.name || null;
    let lakes = null;
    try { lakes = S.db ? await catalog() : null; } catch (e) { toast(e.message, true); }
    box.replaceChildren(...dbs.map(d => lakeNode(d.name, d.name === S.db ? lakes?.get(d.name) || new Map() : null, d.name === S.db, d.name === S.db ? null : d.running ? 'running' : '')));
    if (!dbs.length) box.append(h('div', { class: 'empty' }, 'No databases yet: + makes one.'));
  } else {
    let lakes;
    try { lakes = await catalog(); } catch (e) {
      box.replaceChildren(h('div', { class: 'empty' }, e.status === 401 ? 'A token is needed to see the tables.' : e.message));
      return;
    }
    const names = S.lakes = [S.lake, ...[...lakes.keys()].filter(n => n !== S.lake).sort()].filter(Boolean);
    box.replaceChildren(...names.map(n => lakeNode(n, lakes.get(n) || new Map(), n === S.lake, n === S.lake ? 'this lake' : 'attached')));
  }
  if (S.pick?.type === 'object') S.pick.t = S.objects?.find(t => t.key === S.pick.t.key) || S.pick.t; // (as it is now)
  mark(); detail();
}
H.newDatabase = () => objects(m => m.newDatabase()); // (only a folder of lakes has databases to make)
H.explain = (sql, params, profile, session) => ({ kind: 'plan', sql: lastStatement(sql), params, profile, session });
/** The menus' items for a statement's plan: shown, not run; or its query profile, run with EXPLAIN ANALYZE. */
H.planItems = go => [{ label: 'Show execution plan', icon: 'plan', keys: 'Ctrl Shift E', run: () => go(false) }, { label: 'Run query profile', run: () => go(true) }];
// (the objects' menus, loaded as the pointer first comes over the Data tree: open at once when asked for)
const warm = e => { if (e.target.closest?.('#data')) { removeEventListener('pointerover', warm); import('./objects.js'); } };
addEventListener('pointerover', warm, { passive: true });
H.pickDb = at => objects(m => m.dbMenu(at)); // (a database's menu, beside its row's in the Data tree)
H.use = use;

// ------------------------------------------------------------------ what was picked, and the details view
function pick(p, tab) {
  S.pick = p; S.pickedOn = S.doc; mark(); // (a table, a column: shown while this tab is in front)
  const details = R.views.find(v => v.id === (tab || 'details'));
  if (details && sideOf(details) === 'right') { S.tab = details.id; drawRight(); }
  if (!$('#right').hidden) detail();
  else if (!(drawer('right') && p.type === 'file')) pane('right', true); // (a file opened from the tree doesn't cover itself with its details)
  emit('pick', p);
}
H.pick = pick;
function mark() {
  const key = S.pick?.type === 'object' ? S.pick.t.key : S.pick?.type === 'item' ? S.pick.key : S.pick?.type === 'file' ? 'file:' + (S.pick.f.rel || S.pick.f.path?.replace(/^files\//, '')) : null;
  document.querySelectorAll('.vbox .row.on[data-key], .row.col.on').forEach(r => r.classList.remove('on')); // (a column picked gives way too)
  if (key) document.querySelectorAll(`.vbox .row[data-key="${CSS.escape(key)}"]`).forEach(r => r.classList.add('on'));
}
const facts = pairs => h('dl', { class: 'facts' }, pairs.filter(([, v]) => v != null && v !== '' && !(Array.isArray(v) && !v.length)).flatMap(([k, v]) => [h('dt', {}, k), h('dd', {}, Array.isArray(v) ? v.join(', ') : v)]));
const act = (ic, label, title, fn) => h('button', { class: 'btn small', title, onclick: fn }, ic ? icon(ic) : null, label);
const head = (ic, name, kind, cls = '') => h('div', { class: 'dh' }, h('span', { class: 'dtile ' + cls, html: svg(ic, 20) }), h('div', {}, h('div', { class: 'dn' }, name), h('div', { class: 'dk' }, kind)));
const filePick = path => { const rel = path.replace(/^files\//, ''), nb = rel.match(/^notebooks\/([^/]+?)(?:\.ipynb)?$/); return { type: 'file', f: nb ? { rel: 'notebooks/' + nb[1], name: nb[1] + '.ipynb', notebook: true } : S.files?.find(f => f.path === 'files/' + rel) || { rel, path: 'files/' + rel } }; };
H.pickFile = path => pick(filePick(path));
/** The details follow the tab in front (its file, or what it is while it has none): a table or a
 * column picked stays while the tab it was picked with is in front, and gives way when another is. */
function follow(doc) {
  if (S.pick && S.pick.type !== 'file' && S.pick.type !== 'doc' && S.pickedOn === doc) return;
  const saved = doc.kind === 'notebook' ? doc.version && (doc.plain ? doc.path : `notebooks/${doc.name}`) : doc.path;
  S.pick = doc.pickOf ? doc.pickOf() : saved ? filePick(saved) : doc.kind ? { type: 'doc', doc } : null; // (a table's tab: the table)
  mark(); detail();
}

/** An answer's columns, summarized from the rows it holds (a header clicked). */
function explore(r, i, cell) { pick({ type: 'result', r, i, cell }); }
H.explore = explore;

// The rarer parts, loaded when first used (more.js): History, Variables, Settings, choosing the
// Python, a table's profile, a file run as a job.
const more = () => import('./more.js'), menus = () => import('./tabs.js'); // (tabs.js: the tabs' and the panes' menus)
const viewMenu = (at, v) => menus().then(m => m.viewMenu(at, v));
const details = () => import('./details.js'); // (a table's, a file's, an answer's: loaded with the first pick)
const objectDetail = async t => (await details()).objectDetail(t), fileDetail = async f => (await details()).fileDetail(f), resultDetail = async p => (await details()).resultDetail(p), docDetail = async d => (await details()).docDetail(d);
const runs = async () => (await more()).runs(), variables = async () => (await more()).variables(), settings = async at => (await import('./settings.js')).settings(typeof at === 'string' ? at : null);
const choosePython = async () => (await more()).choosePython();
Object.assign(H, { sideOf, moveTab, fold, folded, viewTitle, KIND, addDoc, closeDoc, act, facts, head, detail, drawLeft, drawViews, look, readVars, themeNow, choosePython, job: async (doc, every) => (await more()).job(doc, every), schedule: async doc => (await more()).job(doc, true), createAs: async sql => (await more()).createAs(sql) });

// ------------------------------------------------------------------ a tab's Python (its session's, on the node)
function kernel(state, d = S.doc) {
  if (state && d) d.py = state;
  S.doc?.drawPill?.();
  if (S.doc?.kind === 'python') status();
}
H.kernel = kernel;
async function restart(d = S.doc) {
  try { await call(`/sessions/${sessionOf(d)}/python`, { method: 'DELETE' }); } catch (e) { toast(e.message, true); return; }
  kernel('none', d); if (d) d.vars = []; toast('Python restarted: its variables are gone'); if (S.tab === 'variables') detail();
}
H.restart = restart;
/** A SQL cell: on the node, unless its notebook's Python is in it (`more.js`: a table of its named,
 * or its answer named `→ df`). */
const sqlCell = async (text, signal, cell) => cell?.as || cell?.nb.vars?.length ? (await more()).sqlCell(text, signal, cell) : run(text, signal, undefined, S.pageRows, sessionOf(cell?.nb));
async function readVars(d = S.doc) { const v = await (await call(`/sessions/${sessionOf(d)}/python`)).json(); if (d) d.vars = v.variables || []; if (v.python) S.pyInfo = v.python; return v; }
/** The Python chip of a notebook's or a Python file's toolbar: whether it runs, and its menu. */
H.pythonPill = () => {
  const pill = h('button', { class: 'pill kernel', id: 'kernel', 'aria-haspopup': 'menu', title: 'This tab\'s Python, on the node: its own variables, apart from other tabs\'',
    onclick: e => { const at = e.currentTarget; more().then(m => m.pythonMenu(at)); } });
  const draw = (py = S.doc?.py) => pill.replaceChildren(h('span', { class: 'dot ' + (py === 'busy' ? 'busy' : py === 'idle' ? '' : 'idle') }), 'Python ', h('b', {}, py || 'not started'), icon('chevd', 'ic', 12));
  draw(); pill.draw = draw;
  return pill;
};
/** A document's Run: a button, a ▾ with its other ways to run (and `all`, Run file beside it); while it runs, Stop in its place. */
H.runButton = (busy, o, items) => busy ? h('button', { class: 'btn stopb', id: 'runBtn', title: o.stopTitle, onclick: o.stop }, icon('stop'), 'Stop')
  : [h('span', { class: 'split' }, h('button', { class: 'btn primary', id: 'runBtn', title: o.title, onclick: o.run }, icon('play'), o.label),
    h('button', { class: 'btn primary caret', title: 'Other ways to run it', 'aria-label': 'Other ways to run it', 'aria-haspopup': 'menu', onclick: e => menu(e.currentTarget, typeof items === 'function' ? items() : items) }, icon('chevd', 'ic', 12))), // (items: made as the menu opens, when they depend on the selection)
    o.all ? h('button', { class: 'btn runall', id: 'runAll', title: o.all[1], onclick: o.all[0] }, icon('playall'), 'Run file') : null];
/** Save, shown only when there is something to save. */
H.saveButton = doc => doc.dirty ? h('button', { class: 'btn savep', id: 'saveBtn', title: 'Save it (Ctrl+S)', onclick: () => doc.save() }, icon('save'), 'Save') : null;
on('ran', (who, r, what) => {
  const src = what?.src ?? who?.src ?? '';
  if (!src.trim()) return;
  S.ranN = (S.ranN || 0) + 1;
  S.ran.unshift({ id: S.ranN, kind: what?.kind || who?.kind || 'sql', src, ms: r?.ms || 0, ok: r?.kind !== 'error', at: Date.now(), where: S.doc?.title || '', rows: r?.kind === 'rows' ? r.total : null, error: r?.kind === 'error' ? r.message : null });
  S.ran.length = Math.min(S.ran.length, 100);
  if (S.tab === 'runs') detail();
  if (r?.kind === 'done' || what?.kind === 'python' || who?.kind === 'python' || /\b(create|drop|alter|attach|detach)\b/i.test(src)) later(refresh); // (the last answer of a script may be rows after its DDL)
  if (what?.kind === 'python' || who?.kind === 'python') readVars(who?.nb || who?.doc).then(() => S.tab === 'variables' && detail(), () => {});
});

// ------------------------------------------------------------------ the node: its stats, the database, refreshing
async function stats() {
  try {
    const s = await (await call('/stats')).json();
    S.lake = s.lake; S.info = s; S.down = false;
    if (!S.pick) detail();
    const nodes = s.nodes || [], who = MODE === 'lakes' ? S.db : s.lake;
    $('#where').innerHTML = `<span class="dot"></span><b>${esc(who || '')}</b> ${nodes.length > 1 ? `· ${esc(s.role)} · ${nodes.length} nodes` : 'on this machine'}${s.live_queries ? ` · ${s.live_queries} live` : ''}`;
    $('#where').title = `This database's cluster: ${nodes.join(', ')}. Its leader is ${s.leader}; commits so far: ${s.hwm}.`;
  } catch (e) {
    S.down = true;
    $('#where').innerHTML = '<span class="dot off"></span>not reachable';
    $('#where').title = e.message;
  }
  status();
}
async function use(db) {
  for (const d of S.docs) if (d.kind === 'notebook') for (const c of d.cells) c.stopLive();
  S.db = db;
  await stats();
  S.pick = null;
  await refreshViews();
  toolbar(); hashNow();
  toast(`Everything now runs in database ${db}`);
}
let pending;
const later = f => { clearTimeout(pending); pending = setTimeout(f, 250); };
async function refresh() { await stats(); await refreshViews(); emit('refresh'); }
H.refresh = refresh;
H.refreshFiles = () => refreshViews(['workspace']);
let wsTimer;
/** The Workspace again, from the files it has (a tab opened, closed, saved or changed). */
const laterWorkspace = () => { clearTimeout(wsTimer); wsTimer = setTimeout(() => { const v = R.views.find(x => x.id === 'workspace'); if (v?.box && S.files) drawWorkspace(v.box); mark(); }, 120); };
on('changed', doc => {
  if (!doc) return;
  if (doc.dirty !== doc.shown) { doc.shown = doc.dirty; drawTabs(); if (doc === S.doc) toolbar(); if (S.pick?.doc === doc) detail(); }
  if (doc === S.doc) status();
  laterWorkspace();
});
on('saved', (doc, path) => {
  if (path && S.files && !S.files.some(f => f.path === path)) { S.files.push({ path, written: new Date().toISOString().slice(0, -1) }); laterWorkspace(); } // (in the tree now; its size with the listing)
  toolbar(); remember(); hashNow(); later(() => refreshViews(['workspace']));
  if (doc === S.doc) follow(doc); // (its details are of the file at its path now: saved, or renamed)
});

// ------------------------------------------------------------------ search (Ctrl K): in more.js
const palette = async () => (await more()).palette();

// ------------------------------------------------------------------ settings, sign-in, keys
function signin() {
  const token = T.token();
  if (!token) return askToken('The token this node was started with (--admin-token, --write-token or --read-token)');
  menu($('#signin'), [{ label: 'Sign in as someone else…', icon: 'key', run: () => askToken('A token this node takes') }, { label: 'Sign out', icon: 'close', run: () => { store.set('pondra.token', null); store.set('pondra.user', null); drawSignin(); refresh(); } }]);
}
function drawSignin() { const b = $('#signin'), on = !!T.token(), who = store.get('pondra.user'); b.replaceChildren(icon(on ? 'key' : 'user'), on ? who || 'Signed in' : 'Sign in'); b.classList.toggle('on', on); }
// (the sign-in dialog: settings.js)
const askToken = async why => (await import('./settings.js')).askToken(why);
ask.token = askToken;
H.drawSignin = drawSignin;
function drawActions() {
  $('#moreBtn').hidden = !R.actions.some(a => a.menu && !a.hidden?.());
  $('#actions').replaceChildren(...R.actions.filter(a => !a.menu && !a.hidden?.()).map(a => h('button', { class: a.label ? 'btn' + (a.primary ? ' primary' : '') : 'icon', id: a.id + 'Btn', title: a.title, 'aria-label': a.title, onclick: e => a.run(e) }, a.icon ? icon(a.icon) : null, a.label || null)));
}
/** The top bar's ⋯: extensions' menu actions (there only when some are). */
function moreMenu(at) { menu(at, R.actions.filter(a => a.menu && !a.hidden?.()).flatMap(a => [a.sep ? '-' : null, { label: a.title, icon: a.icon, keys: a.keys, run: a.run }])); }
function drawRail() {
  $('#rail').hidden = !R.nav.length;
  $('#rail').replaceChildren(...R.nav.map(n => h('button', { class: 'rb' + (S.place === n.id ? ' on' : ''), title: n.label, 'aria-label': n.label, onclick: () => { S.place = n.id; drawRail(); n.run(); }, html: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${n.icon || ICONS.dots}</svg><span>${esc(n.label)}</span>` })));
}
let started = false, drawing = 0;
shell.redraw = () => { if (!drawing) drawing = requestAnimationFrame(() => { drawing = 0; if (started) { drawActions(); drawViews(); drawRail(); } }); };

// ------------------------------------------------------------------ keys: the page's, the tree's and the tabs' (arrow keys, one Tab stop each)
document.addEventListener('keydown', e => {
  const mod = e.ctrlKey || e.metaKey, t = e.target, k = e.key.toLowerCase();
  if (mod && k === 's') { e.preventDefault(); S.doc?.save?.(); return; }
  if (mod && k === 'k') { e.preventDefault(); palette(); return; }
  if (mod && !e.altKey && k === 'b') { e.preventDefault(); pane('left'); return; }
  if (mod && e.altKey && k === 'b') { e.preventDefault(); pane('right'); return; }
  if (mod && k === 'j') { e.preventDefault(); if (S.doc?.hasPanel) pane('bottom'); return; }
  if (t.closest?.('textarea,input,select,dialog,[contenteditable]')) return; // (typing)
  if (t.closest?.('[role=tree]')) { treeKeys(e); return; }
  if (t.closest?.('[role=tablist]')) { tabKeys(e); return; }
  if (t.closest?.('button,a,summary,.grid')) return; // (the keys of a control)
  if (e.key === '?') { e.preventDefault(); settings('keys'); return; }
  if (S.doc?.onkey && (t === document.body || t.closest?.('#docs'))) S.doc.onkey(e);
});
function treeKeys(e) {
  if (e.target.closest('button')) return; // (the ⋯ of a row has its own keys)
  const tree = e.target.closest('[role=tree]'), rows = [...tree.querySelectorAll('[role=treeitem]')].filter(r => r.offsetParent), i = rows.indexOf(e.target.closest('[role=treeitem]'));
  const go = j => { const r = rows[Math.max(0, Math.min(rows.length - 1, j))]; if (!r) return; rows.forEach(x => x.tabIndex = -1); r.tabIndex = 0; r.focus(); };
  const row = rows[i], open = row?.getAttribute('aria-expanded');
  const acts = { ArrowDown: () => go(i + 1), ArrowUp: () => go(i - 1), Home: () => go(0), End: () => go(rows.length - 1),
    ArrowRight: () => open === 'false' ? (row.toggle ? row.toggle() : row.querySelector('.tw')?.click()) : open === 'true' ? go(i + 1) : null,
    ArrowLeft: () => { if (open === 'true') { row.toggle?.(); return; } const lvl = +row.getAttribute('aria-level'); for (let j = i - 1; j >= 0; j--) if (+rows[j].getAttribute('aria-level') < lvl) return go(j); },
    Enter: () => row.click(), ' ': () => row.click() };
  if (acts[e.key]) { e.preventDefault(); acts[e.key](); }
}
function tabKeys(e) {
  const list = e.target.closest('[role=tablist]'), tabs = [...list.querySelectorAll('[role=tab]')], i = tabs.indexOf(e.target.closest('[role=tab]'));
  const j = { ArrowRight: i + 1, ArrowLeft: i - 1, Home: 0, End: tabs.length - 1 }[e.key];
  if (j != null) { e.preventDefault(); const t = tabs[(j + tabs.length) % tabs.length]; t.click(); requestAnimationFrame(() => (list.querySelector('[aria-selected=true]') || t).focus()); } // (the list is drawn again: its tab in front)
  else if (e.key === 'Delete' && e.target.closest('#tabbar')) { const d = S.docs[+e.target.closest('.tab').dataset.i]; if (d) closeDoc(d); }
}
// (a tree takes one Tab stop: its first row, or the one last focused)
document.addEventListener('focusin', e => { const tree = e.target.closest?.('[role=tree]'); if (tree && !tree.querySelector('[role=treeitem][tabindex="0"]')) tree.querySelector('[role=treeitem]')?.setAttribute('tabindex', '0'); });

// ------------------------------------------------------------------ what the core registers (as an extension would)
function core() {
  register.cellKind({ id: 'sql', label: 'SQL', language: 'sql', placeholder: 'SELECT …', live: true, run: sqlCell });
  register.cellKind({ id: 'python', label: 'Python', language: 'python', placeholder: 'db.sql("SELECT …")      # runs on the node; cells share variables', run: async (text, signal, cell) => { kernel('busy', cell.nb); try { return await run(doBlock(text), signal, undefined, S.pageRows, sessionOf(cell.nb)); } finally { kernel('idle', cell.nb); } } });
  register.cellKind({ id: 'markdown', label: 'Markdown', language: 'markdown', placeholder: 'Markdown: # a heading, **bold**, *italic*, [a link](https://…), ![a picture](data/chart.png), - a list, | a | table |' });
  register.renderer({ id: 'error', order: 10, match: r => r.kind === 'error', render: r => h('pre', { class: 'err' }, r.message) });
  register.renderer({ id: 'rows', order: 20, match: r => r.kind === 'rows', render: (r, cell) => grid(r, { footer: true, name: S.doc?.name, explore: i => explore(r, i, cell),
    // (Chart and Data profile, and a SQL cell's Plan, as a SQL file's pane has them; a cell's open one, and its chart's settings, kept with the notebook)
    chart: cell?.chartKeep, view: cell?.view, onview: v => { if (cell) { cell.view = v; cell.nb.changed(); } },
    views: cell?.kind === 'sql' ? [['plan', 'Plan', 'plan', () => import('./plan.js').then(m => m.planView(lastStatement(r.src || cell.src)))]] : [] }) });
  register.renderer({ id: 'figures', order: 30, match: r => r.kind === 'done' && Array.isArray(r.value?.images), render: r => h('div', { class: 'figs' }, r.value.images.map(b => h('img', { class: 'fig', alt: 'a figure the code drew', src: 'data:image/png;base64,' + b }))) });
  register.renderer({ id: 'text', order: 40, match: r => r.kind === 'text', render: r => said(r.text) });
  // (a statement's execution plan, not run, or its query profile: H.explain)
  register.renderer({ id: 'plan', order: 50, match: r => r.kind === 'plan', render: r => { const box = h('div', { class: 'wait' }, 'Reading the plan…'); import('./plan.js').then(m => box.replaceWith(m.planView(r.sql, r.params, { profile: r.profile, session: r.session }))); return box; } });
  register.renderer({ id: 'done', order: 90, match: r => r.kind === 'done', render: r => { const d = doneText(r.value) || (r.notices?.length ? '' : 'Done.'); return d && !(d === 'Done.' && r.notices?.length) ? h('div', { class: 'done' }, d) : null; } });
  register.view({ id: 'data', side: 'left', order: 10, title: () => MODE === 'lakes' ? 'Databases' : 'Data', render: box => dataTree(box), tools: [
    { icon: 'plus', title: 'New database', domId: 'newdb', hidden: () => MODE !== 'lakes', run: () => H.newDatabase() },
    { icon: 'refresh', title: 'Refresh', domId: 'refresh', run: () => refresh() }] });
  register.view({ id: 'workspace', side: 'left', order: 20, title: 'Workspace', render: box => workspace(box), tools: [
    { icon: 'plus', title: 'New: a notebook, a file or a folder', domId: 'newfile', menu: false, run: e => newMenu(e.currentTarget) }] });
  register.view({ id: 'details', side: 'right', order: 10, title: 'Details', tree: false, render: (box, p) => p?.type === 'item' ? p.render() : p?.type === 'object' ? objectDetail(p.t) : p?.type === 'file' ? fileDetail(p.f) : p?.type === 'result' ? resultDetail(p) : p?.type === 'doc' && S.docs.includes(p.doc) ? docDetail(p.doc) : details().then(m => m.summary()) });
  register.view({ id: 'variables', side: 'right', order: 20, title: 'Variables', tree: false, render: () => variables() });
  register.view({ id: 'runs', side: 'right', order: 30, title: 'History', tree: false, render: () => runs() });
  register.view({ id: 'jobs', side: 'right', order: 40, title: 'Jobs', tree: false, render: async () => (await import('./jobs.js')).jobs() });
  for (const [id, title, ic, order, schema] of [['functions', 'Functions', 'fn', 10, 1], ['procedures', 'Procedures', 'play', 20, 1], ['schedules', 'Schedules', 'calendar', 30, 1], ['secrets', 'Secrets', 'key', 40], ['users', 'Users and roles', 'user', 50]]) register.objectKind({ id, title, icon: ic, order, schema: !!schema });
  registerFiles(register);
  NEW.forEach(([id, ic, title, run]) => register.command({ id, title, run }));
  for (const [id, title, keys, fn] of [['search', 'Search tables, files and commands', 'Ctrl K', palette], ['left', 'Show or hide the left pane', 'Ctrl B', () => pane('left')], ['bottom', 'Show or hide the bottom panel', 'Ctrl J', () => pane('bottom')],
    ['right', 'Show or hide the right pane', 'Ctrl Alt B', () => pane('right')], ['settings', 'Settings', '', settings],
    ['restart', 'Restart Python', '', restart], ['python', 'Choose the Python…', '', choosePython], ['token', 'Sign in with a token…', '', () => askToken('The token this node was started with')],
    ['refresh', 'Refresh the catalog', '', refresh], ['keys', 'Keys', '?', () => settings('keys')]]) register.command({ id, title, keys, run: fn });
}

// ------------------------------------------------------------------ the page's API, and starting
/** A tree row for an extension's view (ADR-032's `ui.line`): `line(null, { title, onclick }, ...kids)`. */
const line = (_tw, attrs, ...kids) => h('div', { class: 'row', role: 'treeitem', tabindex: '-1', ...attrs }, h('span', { class: 'tw none' }), ...kids);
H.addCell = o => addCell(o);
function addCell(o) { const nb = S.doc?.kind === 'notebook' ? S.doc : S.nb && S.docs.includes(S.nb) ? (activate(S.nb), S.nb) : newNotebook(); return nb.add(o); }
const pondra = {
  state: S, session: SESSION, mode: MODE, version: VERSION,
  api: { call, run, rows, base, sql: run },
  ui: { h, icon, icons: ICONS, line, button: act, toast, menu, prompt, pick, detail, panel: open => pane('right', open), refresh: () => refresh(), add: addCell, cells: () => S.cells.slice(),
    notebook: () => (S.doc?.kind === 'notebook' ? S.doc : S.nb)?.notebook(), open: (nb, name) => H.openNotebook(nb, name), openFile, grid, activate, pane },
  docs: () => S.docs.slice(), register, on, emit, configure,
};
window.pondra = pondra;
export { pondra };

addEventListener('beforeunload', e => { if (S.docs.some(d => d.dirty && !d.blank)) { e.preventDefault(); e.returnValue = ''; } });
addEventListener('pagehide', () => {
  const token = T.token();
  if (keeping) { clearTimeout(keeping); keeping = 0; T.fetch('/console/settings', { method: 'PUT', keepalive: true, body: JSON.stringify(PREFS), headers: { 'content-type': 'application/json' } }).catch(() => {}); } // (a setting changed just before the page went)
  for (const s of [SESSION, ...S.docs.map(d => d.session)]) if (s) T.fetch(base() + '/sessions/' + s, { method: 'DELETE', keepalive: true, headers: { ...T.headers(), ...(token ? { authorization: 'Bearer ' + token } : {}) } }).catch(() => {}); // (each tab's temporary tables, variables and Python)
});
const layout = () => `${narrow()} ${drawer('right')}`;
let wasLayout = layout();
addEventListener('resize', () => { // (into a narrow window the side panes become drawers, closed; back out, they are as they were)
  if (layout() !== wasLayout) { wasLayout = layout(); sides(true); }
  drawPanes();
});
const sides = draw => {
  $('#left').hidden = narrow() || prefs('left') === false;
  $('#right').hidden = drawer('right') || prefs('right') === false;
  if (draw && !$('#right').hidden) drawRight();
};
$('#main').addEventListener('pointerdown', () => { for (const w of ['left', 'right']) if (drawer(w) && paneOpen(w)) pane(w, false); }); // (a drawer closes when the page behind it is used)

// (a link to a file or notebook followed in the open page opens it, as the address does at the start)
addEventListener('hashchange', () => { const p = new URLSearchParams(location.hash.slice(1)), nb = p.get('notebook'), file = p.get('file'); if (nb) openFile('notebooks/' + nb); else if (file) openFile(file); });
async function start() {
  const hash = new URLSearchParams(location.hash.slice(1));
  if (MODE === 'lakes') S.db = hash.get('db');
  look(); await machinePrefs(); look(); S.pageRows = prefs('pageRows') || null; core();
  $('#search').onclick = palette;
  $('#moreBtn').onclick = e => moreMenu(e.currentTarget);
  $('#settingsBtn').onclick = settings;
  $('#helpBtn').onclick = () => settings('keys');
  for (const [id, b] of [['runs', '#runsBtn'], ['jobs', '#jobsBtn']]) $(b).onclick = () => { if (!$('#right').hidden && S.tab === id) pane('right', false); else show(id); };
  $('#signin').onclick = signin;
  edges(); viewDrops();
  sides();
  drawSignin(); drawActions(); drawRail(); drawPanes(); status();
  // (extensions, loaded after these modules, register meanwhile: drawn with the core's from here on)
  const data = R.views.find(v => v.id === 'data');
  if (MODE === 'lakes') { data.drawn = true; await renderView(data); await stats(); drawLeft(); } // (the tree picks the database that stats asks of)
  else { await stats(); drawLeft(); }
  drawRight();
  started = true;
  drawActions(); drawRail();
  const nb = hash.get('notebook'), file = hash.get('file');
  await restoreTabs(); // (the tabs open last time, then the one the address names in front)
  if (nb) await openFile('notebooks/' + nb);
  else if (file) await openFile(file);
  if (!S.docs.length) newNotebook();
  setInterval(() => { if (document.visibilityState === 'visible') stats(); }, 15000);
  emit('start', pondra);
}
start();
