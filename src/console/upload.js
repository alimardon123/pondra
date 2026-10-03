// Files from this computer into the lake's files (ADR-034): picked (Upload files…, Upload a
// folder…) or dropped on the Workspace, a folder with all it holds, its own paths kept. A file
// there already is replaced only when asked, and only as it is (its version). Loaded when first used.
import { h, count, S, R, call, toast, confirmed, fileUrl, writeFile } from './core.js';
import { cleanName } from './notebook.js';

/** Each [path under the lake's files, file] put there; `top`: the folder they went to, said once (null: each said). */
async function put(list, top) {
  let n = 0;
  for (const [rel, f] of list) {
    try { await call(fileUrl(rel), { method: 'PUT', body: f }); n++; if (top == null) toast(`Put in the lake: files/${rel}`); } catch (err) {
      if (err.status !== 409) { toast(`${f.name}: ${err.message}`, true); continue; }
      if (!confirmed(`files/${rel} is there already. Replace it with the one from this computer?`)) continue;
      const version = ((await call(fileUrl(rel), { method: 'HEAD' })).headers.get('etag') || '').replace(/"/g, '');
      if (await writeFile(rel, f, version, f.type || 'application/octet-stream')) toast(`Replaced files/${rel}`);
    }
  }
  if (top != null && n) { if (top) S.open.add('dir:' + top.slice(0, -1)); toast(`Put ${count(n)} file${n === 1 ? '' : 's'} in the lake, in files/${top}`); }
  R.helpers.refreshFiles();
}
/** The upload buttons: files, or a folder (`folder`). A notebook picked alone opens, unless it is for a folder: then it is put there. */
export function upload(at = '', folder) {
  const input = h('input', { type: 'file', hidden: true, multiple: true });
  if (folder) input.webkitdirectory = true;
  input.onchange = async () => {
    const fs = [...input.files], list = [];
    input.remove();
    for (const f of fs) {
      if (!folder && /\.ipynb$/i.test(f.name) && (!at || at === 'notebooks/')) { try { R.helpers.openNotebook(JSON.parse(await f.text()), cleanName(f.name) || 'uploaded'); toast(`Opened ${f.name}: Ctrl+S keeps it in the lake`); } catch (err) { toast(`Could not open ${f.name}: ${err.message}`, true); } continue; }
      list.push([at + (folder && f.webkitRelativePath || f.name), f]);
    }
    put(list, folder && fs.length ? `${at}${fs[0].webkitRelativePath.split('/')[0]}/` : null);
  };
  document.body.append(input); input.click();
}
/** Files and folders dropped on the Workspace (their entries, or files where a browser gives none), into the folder `at`: every file a folder holds, however deep. */
export async function dropped(entries, at) {
  const list = [], file = e => new Promise(ok => e.file(ok, () => ok(null)));
  const all = async d => { const r = d.createReader(), out = []; for (let got; (got = await new Promise(ok => r.readEntries(ok, () => ok([])))).length;) out.push(...got); return out; }; // (a reader gives a hundred at a time)
  const walk = async (e, to) => { if (e instanceof File) list.push([to + e.name, e]); else if (e.isFile) { const f = await file(e); if (f) list.push([to + f.name, f]); } else for (const x of await all(e)) await walk(x, to + e.name + '/'); };
  for (const e of entries) await walk(e, at);
  put(list, entries.length === 1 && entries[0].isDirectory ? `${at}${entries[0].name}/` : list.length > 1 ? at : null);
}
