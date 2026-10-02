// The tabs' and the panes' menus (ADR-034), loaded when first used, not with the page: a tab's
// right-click menu, the list of every tab (the tab bar's ⌄), and a view's menu (moving it to the
// other pane, folding it). What they use of the shell comes through `R.helpers`.
import { h, $, icon, S, R, toast, menu, moreStyle, renaming } from './core.js';

moreStyle();
const H = R.helpers, { sideOf, moveTab, fold, folded } = H;

/** A tab's right-click menu: pin it, close it or some of the others (the pinned stay), copy its path. */
export function tabMenu(e, d) {
  const { closeDoc: close, pin } = H, i = S.docs.indexOf(d), some = f => S.docs.filter((x, j) => x !== d && !x.pinned && f(x, j));
  const closeAll = async list => { for (const x of list) await close(x); };
  const others = some(() => true), right = some((_, j) => j > i), saved = some(x => !x.dirty);
  menu(e, [{ label: d.pinned ? 'Unpin' : 'Pin', icon: 'pin', run: () => pin(d, !d.pinned) }, '-',
    { label: 'Close', icon: 'close', keys: 'Delete', run: () => close(d) },
    { label: 'Close others', disabled: !others.length, run: () => closeAll(others) }, { label: 'Close to the right', disabled: !right.length, run: () => closeAll(right) },
    { label: 'Close saved', disabled: !saved.length && (d.dirty || d.pinned), run: () => closeAll(saved.concat(d.dirty || d.pinned ? [] : [d])) },
    { label: S.docs.some(x => x.pinned) ? 'Close all but the pinned' : 'Close all', run: () => closeAll(S.docs.filter(x => !x.pinned)) },
    d.rename ? '-' : null, d.rename ? { label: 'Rename…', icon: 'pencil', run: () => { H.activate(d); renaming(); } } : null,
    d.rename ? { label: 'Copy path', icon: 'copy', disabled: d.kind === 'notebook' ? !d.version : !d.path, hint: 'files/…', run: () => H.copyPath(d) } : null]);
}

/** Every open tab (the tab bar's ⌄), the pinned first: a click brings one forward, its × closes it,
 * a right-click gives its tab's menu; Ctrl or Shift picks several, to close at once. */
export function tabList(at, picked = new Set()) {
  const item = d => ({ label: d.title + (d.dirty ? ' •' : ''), icon: d.pinned ? 'pin' : d.icon, checked: d === S.doc, run: () => H.activate(d) }), pins = S.docs.filter(d => d.pinned), rest = S.docs.filter(d => !d.pinned), docs = [...pins, ...rest];
  const closeSome = async list => { for (const x of list) await H.closeDoc(x); const b = $('#tabbar .tmore'); if (b && !b.hidden) tabList(b); }; // (the list again, while the tabs still don't fit)
  menu(at, [...pins.map(item), pins.length ? '-' : null, ...rest.map(item)]);
  const m = $('#menu');
  m.querySelectorAll('button').forEach((b, i) => {
    const d = docs[i];
    b.classList.toggle('picked', picked.has(d));
    b.addEventListener('click', e => { // (before the menu's own: Ctrl or Shift picks, the menu stays)
      if (!e.ctrlKey && !e.metaKey && !e.shiftKey) return;
      e.stopImmediatePropagation();
      const last = picked.last ?? S.docs.indexOf(S.doc), j = docs.indexOf(d);
      if (e.shiftKey) docs.slice(Math.min(last, j), Math.max(last, j) + 1).forEach(x => picked.add(x)); else picked.has(d) ? picked.delete(d) : picked.add(d);
      picked.last = j; tabList(at, picked);
    }, true);
    b.addEventListener('contextmenu', e => { e.preventDefault(); tabMenu(e, d); });
    b.append(h('span', { class: 'mx', title: `Close ${d.title}`, onclick: e => { e.stopPropagation(); closeSome([d]); } }, icon('close')));
  });
  if (picked.size) m.append(h('div', { class: 'sep' }), h('button', { class: 'mclose', onclick: () => { m.hidden = true; closeSome([...picked]); } }, icon('close'), h('span', { class: 'lb' }, `Close ${picked.size} tab${picked.size > 1 ? 's' : ''}`)));
}

/** A view moved to the other pane (a group on the left, a tab on the right). */
function moveView(v) {
  const sides = { ...H.prefs('sides') || {} }, to = sideOf(v) === 'left' ? 'right' : 'left';
  sides[v.id] = to;
  H.prefs('sides', sides);
  if (to === 'right') { S.tab = v.id; H.pane('right', true); } else H.pane('left', true);
  H.drawViews();
  toast(`${H.viewTitle(v)} is now in the ${to} pane`);
}
/** A view's menu (its ⋯): its tools, moving it, folding it. */
export function viewMenu(at, v) {
  const tools = (v.tools || []).filter(t => !t.hidden?.() && t.menu !== false); // (a tool that opens a menu at its button is only a button)
  menu(at, [...tools.map(t => ({ label: t.title, icon: t.icon, run: t.run })), tools.length ? '-' : null,
    sideOf(v) === 'right' ? { label: 'Move its tab left', run: () => moveTab(v.id, null, -1) } : null, sideOf(v) === 'right' ? { label: 'Move its tab right', run: () => moveTab(v.id, null, 1) } : null,
    { label: sideOf(v) === 'left' ? 'Move to the right pane' : 'Move to the left pane', icon: 'moveSide', run: () => moveView(v) },
    sideOf(v) === 'left' ? { label: folded(v) ? 'Unfold' : 'Fold', run: () => fold(v) } : null]);
}
