// Markdown (ADR-034, round 29): a notebook's Markdown cells and a .md file's preview, drawn as
// CommonMark and GitHub draw it: headings, paragraphs, bold, italic, strikethrough, code and code
// blocks (SQL and Python highlighted), links, pictures, lists (nested, numbered, of tasks), quotes,
// tables and rules. The text's own HTML is shown as text, but for a few harmless tags (<br>, <kbd>,
// <sub>, <sup>…). A link or a picture without a scheme is a file of the lake's: a link opens it in a
// tab, a picture is read from it. Loaded when first drawn.
import { esc, call, fileUrl, R, moreStyle } from './core.js';
import { highlighted } from './editor.js';

await moreStyle();

const TAGS = /<(\/?)(br|kbd|sub|sup|u|mark|small|b|i|em|strong|s|del|ins)\s*\/?>/gi;
const ITEM = /^( {0,3})([-*+]|\d{1,9}[.)])(\s+|$)/, FENCE = /^ {0,3}(`{3,}|~{3,})\s*([^`\s]*)/, RULE = /^ {0,3}([-*_])(?:[ \t]*\1){2,}[ \t]*$/;
const HEADING = /^ {0,3}(#{1,6})(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$/, QUOTE = /^ {0,3}>/, DEF = /^ {0,3}\[([^\]]+)\]:\s*<?(\S+?)>?(?:\s+["'(](.*)["')])?\s*$/;
const DIVIDER = /^ *\|? *:?-+:? *(\| *:?-+:? *)*\|? *$/;
const unescape = p => { try { return decodeURIComponent(p); } catch { return p; } }; // (%20 and the like decoded; a stray % stays as it is)
const slug = s => s.toLowerCase().replace(/<[^>]+>|&#\d+;/g, '').replace(/[^\w\- ]+/g, '').trim().replace(/ +/g, '-');

/** Draw `src` in `el`; `base` is the folder its lake paths are read from (`reports/`). */
export function render(el, src, base = '') {
  el.innerHTML = markdown(src, base) || '<p class="hint">Empty. Double-click to write, in Markdown.</p>';
  for (const img of el.querySelectorAll('img[data-file]')) picture(img);
  el.onclick = e => {
    const a = e.target.closest('a');
    if (!a) return;
    if (a.dataset.file) { e.preventDefault(); R.helpers.openFile(a.dataset.file); }
    else if (a.hash && a.getAttribute('href') === a.hash) { e.preventDefault(); el.querySelector(`[id="${CSS.escape(decodeURIComponent(a.hash.slice(1)))}"]`)?.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
  };
}

/** Markdown as HTML. */
export function markdown(src, base = '') {
  const lines = src.replace(/\r\n?/g, '\n').replace(/\t/g, '    ').split('\n'), refs = new Map();
  let fence = null;
  for (const l of lines) { // (the link definitions: `[name]: url "title"`, used anywhere)
    const f = l.match(FENCE);
    if (f && (!fence || f[1][0] === fence[0] && f[1].length >= fence.length)) fence = fence ? null : f[1];
    const d = !fence && l.match(DEF);
    if (d) refs.set(d[1].toLowerCase().trim(), { href: d[2], title: d[3] });
  }
  const keep = [], html = blocks(lines, { refs, keep, base });
  let out = html;
  for (let i = 0; i < 8 && out.includes('\u0000'); i++) out = out.replace(/\u0000(\d+)\u0000/g, (_, n) => keep[n]); // (spans in spans)
  return out;
}

/** Lines as blocks: each block's HTML, in order. */
function blocks(lines, ctx) {
  let html = '', i = 0;
  // (what ends a paragraph: a numbered item only from 1, as in CommonMark — "2024. A year" is text)
  const starts = l => HEADING.test(l) || FENCE.test(l) || QUOTE.test(l) || RULE.test(l) || ITEM.test(l) && /\S/.test(l.replace(ITEM, '')) && /^ {0,3}([-*+]|1[.)])/.test(l);
  while (i < lines.length) {
    const l = lines[i];
    let m;
    if (!l.trim() || DEF.test(l)) { i++; continue; }
    if ((m = l.match(FENCE))) {
      const code = [], close = new RegExp(`^ {0,3}${m[1][0] === '`' ? '`' : '~'}{${m[1].length},}\\s*$`);
      for (i++; i < lines.length && !close.test(lines[i]); i++) code.push(lines[i]);
      i++;
      const lang = m[2].toLowerCase(), hl = /^(sql|python|py)$/.test(lang) ? highlighted(code.join('\n'), lang === 'sql' ? 'sql' : 'python') : esc(code.join('\n'));
      html += `<pre${lang ? ` data-lang="${esc(lang)}"` : ''}><code>${hl}</code></pre>`;
    } else if ((m = l.match(HEADING))) {
      const inner = inline(m[2] || '', ctx), n = m[1].length;
      html += `<h${n} id="${esc(slug(m[2] || ''))}">${inner}</h${n}>`; i++;
    } else if (RULE.test(l)) {
      html += '<hr>'; i++;
    } else if (QUOTE.test(l)) {
      const q = [];
      while (i < lines.length && (QUOTE.test(lines[i]) || lines[i].trim() && q.length && !starts(lines[i]))) q.push(lines[i++].replace(/^ {0,3}> ?/, ''));
      html += `<blockquote>${blocks(q, ctx)}</blockquote>`;
    } else if (ITEM.test(l)) {
      const ordered = /\d/.test(l.match(ITEM)[2]), items = [];
      let loose = false;
      while (i < lines.length && (m = lines[i].match(ITEM)) && /\d/.test(m[2]) === ordered) {
        const pad = m[1].length + m[2].length + Math.min(Math.max(m[3].length, 1), 4), body = [lines[i].slice(m[0].length)];
        const start = items.length ? null : parseInt(m[2], 10);
        for (i++; i < lines.length; i++) {
          const x = lines[i];
          if (!x.trim()) { if (i + 1 < lines.length && (lines[i + 1].search(/\S/) >= pad)) { body.push(''); continue; } break; }
          if (x.search(/\S/) >= pad) body.push(x.slice(pad));
          else if (!ITEM.test(x) && !starts(x) && body.at(-1).trim()) body.push(x.trim()); // (a paragraph's lines go on)
          else break;
        }
        if (i < lines.length && !lines[i].trim() && ITEM.test(lines[i + 1] || '')) { loose = true; i++; }
        if (body.slice(0, -1).some(x => !x.trim())) loose = true;
        items.push({ body, start });
      }
      const tag = ordered ? 'ol' : 'ul', first = items[0].start;
      html += `<${tag}${ordered && first !== 1 ? ` start="${first}"` : ''}>` + items.map(({ body }) => {
        const task = body[0].match(/^\[([ xX])\]\s+/);
        if (task) body[0] = body[0].slice(task[0].length);
        let inner = blocks(body, ctx);
        if (!loose) inner = inner.replace(/<p>([\s\S]*?)<\/p>/g, '$1'); // (a tight list: its items' text without paragraphs)
        return task ? `<li class="task"><input type="checkbox" disabled${task[1] === ' ' ? '' : ' checked'}> ${inner}</li>` : `<li>${inner}</li>`;
      }).join('') + `</${tag}>`;
    } else if (l.includes('|') && DIVIDER.test(lines[i + 1] || '') && lines[i + 1].includes('-')) {
      const cells = x => x.trim().replace(/^\|/, '').replace(/(?<!\\)\|$/, '').split(/(?<!\\)\|/).map(c => c.trim().replace(/\\\|/g, '|'));
      const head = cells(l), align = cells(lines[i + 1]).map(c => c.startsWith(':') && c.endsWith(':') ? 'center' : c.endsWith(':') ? 'right' : c.startsWith(':') ? 'left' : '');
      const row = (cs, t) => `<tr>${head.map((_, k) => `<${t}${align[k] ? ` style="text-align:${align[k]}"` : ''}>${inline(cs[k] ?? '', ctx)}</${t}>`).join('')}</tr>`;
      const body = [];
      for (i += 2; i < lines.length && lines[i].trim() && lines[i].includes('|'); i++) body.push(row(cells(lines[i]), 'td'));
      html += `<div class="mdtable"><table><thead>${row(head, 'th')}</thead><tbody>${body.join('')}</tbody></table></div>`;
    } else if (/^ {4}/.test(l)) {
      const code = [];
      while (i < lines.length && (/^ {4}/.test(lines[i]) || !lines[i].trim())) code.push(lines[i++].slice(4));
      while (code.length && !code.at(-1).trim()) code.pop();
      html += `<pre><code>${esc(code.join('\n'))}</code></pre>`;
    } else {
      const para = [l];
      for (i++; i < lines.length && lines[i].trim() && !starts(lines[i]) && !(lines[i].includes('|') && DIVIDER.test(lines[i + 1] || '')); i++) {
        if (/^ {0,3}(=+|-+)\s*$/.test(lines[i])) break;
        para.push(lines[i]);
      }
      const under = lines[i]?.match(/^ {0,3}(=+|-+)\s*$/);
      if (under) { const n = under[1][0] === '=' ? 1 : 2, t = para.join(' '); html += `<h${n} id="${esc(slug(t))}">${inline(t, ctx)}</h${n}>`; i++; }
      else html += `<p>${inline(para.map(x => x.replace(/^ +/, '')).join('\n'), ctx)}</p>`;
    }
  }
  return html;
}

/** A paragraph's text: code, links, pictures and tags kept aside (`\0n\0`), the rest escaped, then emphasis. */
function inline(s, ctx) {
  const put = x => `\u0000${ctx.keep.push(x) - 1}\u0000`;
  s = s.replace(/(`+)(?!`)([\s\S]*?[^`])\1(?!`)/g, (_, q, c) => put(`<code>${esc(c.length > 2 && c[0] === ' ' && c.at(-1) === ' ' ? c.slice(1, -1) : c)}</code>`))
    .replace(/\\([!-/:-@[-`{-~])/g, (_, c) => put(esc(c)))
    .replace(/<(https?:\/\/[^\s<>]+|mailto:[^\s<>]+)>/g, (_, u) => put(link(u, esc(u.replace(/^mailto:/, '')), '', ctx)))
    .replace(TAGS, (_, end, t) => put(`<${end}${t.toLowerCase()}>`))
    .replace(/(!?)\[((?:[^[\]]|\[[^\]]*\])*)\](?:\(\s*(<[^>]*>|(?:[^\s()]|\([^\s()]*\))*)(?:\s+("[^"]*"|'[^']*'|\([^)]*\)))?\s*\)|\[([^\]]*)\])?/g, (m, bang, text, href, title, ref) => {
      const to = href != null ? { href: href.replace(/^<|>$/g, ''), title: title?.slice(1, -1) } : ctx.refs.get((ref || text).toLowerCase().trim());
      if (!to) return m;
      return put(bang ? picture0(to.href, text, to.title, ctx) : link(to.href, inline(text, ctx), to.title, ctx));
    })
    .replace(/(^|[\s(])(https?:\/\/[^\s<>()]*[^\s<>().,:;!?"'*_~])/g, (_, before, u) => before + put(link(u, esc(u), '', ctx)));
  return esc(s)
    .replace(/(\*\*\*|___)(?=\S)([\s\S]*?\S)\1(?![*_])/g, '<strong><em>$2</em></strong>')
    .replace(/\*\*(?=\S)([\s\S]*?\S)\*\*/g, '<strong>$1</strong>').replace(/(^|[^\w])__(?=\S)([\s\S]*?\S)__(?!\w)/g, '$1<strong>$2</strong>')
    .replace(/(^|[^*])\*(?=[^\s*])([\s\S]*?[^\s*])\*(?!\*)/g, '$1<em>$2</em>').replace(/(^|[^\w])_(?=[^\s_])([\s\S]*?[^\s_])_(?!\w)/g, '$1<em>$2</em>')
    .replace(/~~(?=\S)([\s\S]*?\S)~~/g, '<del>$1</del>')
    .replace(/(?: {2,}|\\)\n/g, '<br>');
}

/** Where a link or a picture goes: the web, an e-mail, a place on the page, or a file of the lake's. */
function target(href, base) {
  if (/^#/.test(href)) return { href };
  if (/^(https?:|mailto:)/i.test(href)) return { href, web: true };
  if (/^[a-z][\w+.-]*:/i.test(href)) return null; // (javascript: and the like: not followed)
  const parts = [];
  for (const p of (href.startsWith('/') ? href.slice(1) : base + href).split(/[?#]/)[0].split('/')) p === '..' ? parts.pop() : p && p !== '.' && parts.push(unescape(p));
  return { file: parts.join('/') };
}
function link(href, text, title, ctx) {
  const t = target(href, ctx.base), tt = title ? ` title="${esc(title)}"` : '';
  if (!t) return text;
  return t.file != null ? `<a href="#" data-file="${esc(t.file)}" title="${esc(title || 'Open files/' + t.file)}">${text}</a>`
    : `<a href="${esc(t.href)}"${tt}${t.web ? ' target="_blank" rel="noopener noreferrer"' : ''}>${text}</a>`;
}
function picture0(href, alt, title, ctx) {
  const t = /^data:image\/(png|jpe?g|gif|webp);/i.test(href) ? { href, web: true } : target(href, ctx.base), a = ` alt="${esc(alt)}"${title ? ` title="${esc(title)}"` : ''}`;
  return !t || t.href?.startsWith('#') ? esc(alt) : t.file != null ? `<img data-file="${esc(t.file)}"${a}>` : `<img src="${esc(t.href)}"${a} loading="lazy">`;
}

/** A picture of the lake's: read with the page's token, once, then kept for the page. */
const pictures = new Map();
function picture(img) {
  const path = img.dataset.file;
  if (!pictures.has(path)) pictures.set(path, call(fileUrl(path)).then(r => r.blob()).then(b => URL.createObjectURL(b)));
  pictures.get(path).then(u => { img.src = u; }, () => { pictures.delete(path); img.replaceWith(Object.assign(document.createElement('span'), { className: 'nopic', textContent: `(${img.alt || 'a picture'}: files/${path} can't be read)` })); });
}
