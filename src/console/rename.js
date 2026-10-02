// Renaming a lake file (ADR-034): from its name in its tab's toolbar, its tab's menu or the
// Workspace. A lake holds objects, not folders, so a file is moved: its bytes put at the new path
// (refused if a file is there), then the old one deleted; its versions stay under the old name.
// Loaded when first used, not with the page.
import { S, R, emit, call, fileUrl, toast, prompt } from './core.js';
import { cleanName } from './notebook.js';

const H = R.helpers;
async function moveFile(from, to) {
  const r = await call(fileUrl(from));
  const put = await call(fileUrl(to), { method: 'PUT', body: await r.blob(), headers: { 'content-type': r.headers.get('content-type') || 'application/octet-stream' } });
  await call(fileUrl(from), { method: 'DELETE' });
  return (await put.json()).version || null;
}
/** Move it, or say why not: its new version, or undefined. */
const moved = (from, to) => moveFile(from, to).catch(e => { toast(e.status === 409 ? `Not renamed: files/${to} is there already` : 'Not renamed: ' + e.message, true); });

/** A tab renamed to `name`, in its folder, its kind kept: a file in the lake is moved there, one
 * not saved yet only takes the name. True if it was. */
export async function renameDoc(d, name) {
  if (d.kind === 'notebook') return renameNotebook(d, name);
  const was = d.path || d.untitled, ext = was.match(/.(\.[^./]+)$/)?.[1] || '';
  name = name.replace(/^\/+|\/+$/g, '');
  const to = was.replace(/[^/]*$/, '') + name + (name.toLowerCase().endsWith(ext.toLowerCase()) ? '' : ext);
  if (/(^|\/)(\.{0,2}|\s+)(\/|$)/.test(to)) { toast('Not a file name', true); return false; }
  if (to === was) return true;
  if (d.path) {
    const v = await moved(d.path, to);
    if (v === undefined) return false;
    Object.assign(d, { path: to, version: v }); toast(`Renamed to files/${to}`); emit('saved', d, 'files/' + to);
  } else d.untitled = to;
  H.drawTabs(); H.toolbar();
  return true;
}
/** A notebook saved in place is moved; one saved before as `notebooks/<name>/`, or not saved yet,
 * takes the name for its next save. */
async function renameNotebook(nb, to) {
  const name = cleanName(to);
  if (!name) { toast('Not a notebook name', true); return false; }
  if (name === nb.name) return true;
  if (nb.plain && nb.version) {
    const v = await moved(nb.path, `${nb.dir}${name}.ipynb`);
    if (v === undefined) return false;
    Object.assign(nb, { name, version: v }); toast(`Renamed to files/${nb.path}`); emit('saved', nb, `files/${nb.path}`);
  } else { Object.assign(nb, { name, version: null }); nb.changed(); }
  H.drawTabs(); H.toolbar();
  return true;
}
/** A Workspace file renamed to the path asked for; its open tab follows it (or, of another kind
 * now, opens again as one). */
export async function rename(rel) {
  const to = ((await prompt('Rename', 'The new path, under the lake\'s files', rel)) || '').replace(/^\/?(files\/)?/, '');
  if (!to || to === rel) return;
  const doc = S.docs.find(d => d.path === rel), kind = p => (p.match(/\.[^./]+$/)?.[0] || '').toLowerCase(), v = await moved(rel, to);
  if (v !== undefined) {
    if (doc && kind(to) === kind(rel)) {
      Object.assign(doc, doc.kind === 'notebook' ? { dir: to.replace(/[^/]*$/, ''), name: to.split('/').pop().replace(/\.ipynb$/i, '') } : { path: to }, { version: v });
      H.drawTabs(); if (doc === S.doc) H.toolbar();
    } else if (doc) { await H.close(doc); H.openFile(to); }
    toast(`Renamed to files/${to}`);
  }
  H.refreshFiles();
}
