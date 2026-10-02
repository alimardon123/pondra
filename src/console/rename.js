// A lake file's and a folder's menus in the Workspace (ADR-034); renaming a file, from its name in
// its tab's toolbar, its tab's menu or the Workspace; moving a file or a folder dragged onto another
// folder. A lake holds objects, not folders, so a file is moved: its bytes put at the new path
// (refused if a file is there), then the old one deleted; its versions stay under the old name.
// Loaded when first used, not with the page.
import { S, R, emit, call, rows, quote, fileUrl, fileSql, toast, prompt, menu } from './core.js';
import { cleanName } from './notebook.js';
import { copyText } from './grid.js';
import { iconOf, download, newFolder, upload, newAny } from './files.js';

const H = R.helpers, later = f => import('./more.js').then(f);
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
/** A file, or a folder and all it holds (`from` ends in /), dragged onto the folder `to` ('' the
 * top): moved file by file; its open tabs follow. */
export async function move(from, to) {
  const dir = from.endsWith('/'), dest = to + from.replace(/\/$/, '').split('/').pop() + (dir ? '/' : '');
  if (dest === from || dir && to.startsWith(from)) return; // (where it is, or into itself)
  if (dir && to.startsWith('notebooks/')) return toast('A folder in notebooks/ would be a notebook\'s versions: not moved there', true);
  const paths = dir ? (await rows(`SELECT path FROM files(${quote(from)})`)).map(x => x.path.slice(6)) : [from];
  let n = 0;
  for (const p of paths) {
    const v = await moved(p, dest + p.slice(from.length));
    if (v === undefined) break;
    n++;
    const doc = S.docs.find(d => d.path === p), at = dest + p.slice(from.length);
    if (doc) Object.assign(doc, doc.kind === 'notebook' ? { dir: at.replace(/[^/]*$/, '') } : { path: at }, { version: v });
  }
  if (n) { if (to) S.open.add('dir:' + to.slice(0, -1)); toast(`Moved ${dir ? `${n} file${n === 1 ? '' : 's'} of ` : ''}${from.replace(/\/$/, '')} to files/${to}`); H.drawTabs(); H.toolbar(); }
  H.refreshFiles();
}
/** A lake file's menu in the Workspace (its ⋯, or a right-click). */
export function fileMenu(e, f, kind) {
  // (a file only in its tab: not in the lake yet)
  if (f.doc) return menu(e, [{ label: 'Save…', icon: 'save', run: () => { R.helpers.activate(f.doc); f.doc.save(); } }, { label: 'Close', icon: 'close', run: () => R.helpers.close(f.doc) }]);
  menu(e, [kind !== 'file' ? { label: 'Open', icon: iconOf(f.notebook ? 'x.ipynb' : f.rel), run: () => R.helpers.openFile(f.rel) } : null,
    { label: 'Details', icon: 'eye', run: () => R.helpers.pick({ type: 'file', f }) },
    kind === 'data' ? { label: 'Query with SQL', icon: 'play', run: () => R.helpers.query(`SELECT * FROM ${fileSql(f.rel)} LIMIT 1000`) } : null, '-',
    !f.notebook ? { label: 'Rename…', icon: 'pencil', run: () => rename(f.rel) } : null,
    !f.notebook ? { label: 'Download', icon: 'down', run: () => download(f.rel) } : null, { label: 'Versions…', icon: 'clock', run: () => R.helpers.versions({ path: f.notebook ? f.rel + '.ipynb' : f.rel }) },
    { label: 'Copy path', icon: 'copy', run: () => copyText('files/' + f.rel, 'Path copied') }, '-',
    { label: f.notebook ? 'Delete every version…' : 'Delete…', icon: 'trash', run: () => later(m => m.remove(f)) }]);
}
/** A folder's. */
export function folderMenu(e, at) {
  const make = R.helpers;
  menu(e, [{ label: 'New notebook here', icon: 'notebook', run: () => make.newNotebook(at) }, { label: 'New SQL file here', icon: 'filesql', run: () => make.newFile('sql', at + '/') },
    { label: 'New Python file here', icon: 'filepy', run: () => make.newFile('python', at + '/') }, { label: 'New file here…', icon: 'file', run: () => newAny(at + '/') }, { label: 'New folder here', icon: 'folder', run: () => newFolder(at + '/') }, '-',
    { label: 'Upload files here…', icon: 'up', run: () => upload(at + '/') }, { label: 'Upload a folder here…', icon: 'folder', run: () => upload(at + '/', true) }, { label: 'Delete folder…', icon: 'trash', run: () => later(m => m.remove({ rel: at, name: at }, true)) }]);
}
