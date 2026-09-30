// Charts of an answer (ADR-034, round 29): bars, lines, areas, points or a pie, by any column,
// of any of its numeric columns, drawn as SVG and saved as SVG or PNG. Loaded when first shown:
// the page's first load doesn't carry it.
import { h, icon, numeric, menu, saveAs, toast, moreStyle } from './core.js';

await moreStyle();

const PALETTE = ['var(--accent)', 'var(--c2)', 'var(--c3)', 'var(--t-time)', 'var(--t-json)', 'var(--k-data)'];
const TYPES = [['auto', 'Automatic'], ['bar', 'Bars'], ['line', 'Lines'], ['area', 'Areas'], ['point', 'Points'], ['pie', 'Pie']];
const isTime = c => /^(Date|Timestamp)/.test(c?.type || '');

/** An answer's chart and its settings: how (the type), by what (x), of what (the numbers). `keep.st`
 * keeps them by the columns' names: the answer again (run again, a page turned) is charted as it was. */
export function chartView(r, name = 'chart', keep = {}) {
  const cols = r.columns, nums = cols.map((c, i) => numeric(c.type) ? i : -1).filter(i => i >= 0), at = n => cols.findIndex(c => c.name === n);
  let x = cols.findIndex(isTime);
  if (x < 0) x = cols.findIndex((c, i) => !numeric(c.type));
  let st = { type: 'auto', x, ys: nums.filter(i => i !== x).slice(0, 1) }; // (one number to start: others on its scale may not show; Of… adds them)
  const was = keep.st, ys = was?.ys.map(at).filter(i => nums.includes(i));
  if (ys?.length && (was.x == null || at(was.x) >= 0)) st = { type: was.type, x: was.x == null ? -1 : at(was.x), ys };
  const box = h('div', { class: 'chart' });
  const pick = (label, value, options, on) => h('label', { class: 'csel' }, label, h('select', { onchange: e => { on(e.target.value); draw(); } }, options.map(([v, t]) => h('option', { value: v, selected: String(v) === String(value) }, t))));
  const draw = () => {
    keep.st = { type: st.type, x: st.x >= 0 ? cols[st.x].name : null, ys: st.ys.map(i => cols[i].name) };
    const art = nums.length ? render(r, st) : h('div', { class: 'empty' }, 'Nothing to chart: the answer needs a column of numbers.');
    const ofWhat = h('button', { class: 'btn small', title: 'The numbers it shows', onclick: e => menu(e.currentTarget, nums.map(i => ({ label: cols[i].name, checked: st.ys.includes(i), run: () => { st.ys = st.ys.includes(i) ? st.ys.filter(j => j !== i) : [...st.ys, i].sort((a, b) => a - b); draw(); } }))) },
      `Of ${st.ys.map(i => cols[i].name).join(', ') || '…'}`, icon('chevd', 'ic', 12));
    const save = h('button', { class: 'btn small', title: 'Save the chart as a picture', onclick: e => menu(e.currentTarget, [{ label: 'PNG (a picture)', run: () => exportAs(art, 'png', name) }, { label: 'SVG (drawn, it scales)', run: () => exportAs(art, 'svg', name) }]) },
      icon('down'), 'Save', icon('chevd', 'ic', 12));
    box.replaceChildren(h('div', { class: 'cbar' }, pick('Chart', st.type, TYPES, v => { st.type = v; }), pick('By', st.x, [[-1, 'the row'], ...cols.map((c, i) => [i, c.name])], v => { st.x = +v; }), ofWhat, h('span', { class: 'grow' }), nums.length ? save : null), art);
  };
  draw();
  return box;
}

/** The chart itself: `auto` draws lines over time, else bars (a pie when asked: of the first number). */
function render(r, { type, x, ys }) {
  const cols = r.columns, time = isTime(cols[x]);
  if (type === 'auto') type = time ? 'line' : 'bar';
  if (!ys.length) return h('div', { class: 'empty' }, 'Pick a number to chart.');
  const W = 720, H = 260, L = 56, B = 34, T = 12, Rt = 14;
  let data = r.rows.map((row, i) => ({ x: x >= 0 ? row[x] : i + 1, y: ys.map(j => row[j] == null ? null : Number(row[j])) }));
  if (time) data = data.map(d => ({ ...d, t: Date.parse(String(d.x).replace(' ', 'T') + (String(d.x).length > 10 && !/[zZ+]/.test(String(d.x)) ? 'Z' : '')) })).filter(d => Number.isFinite(d.t)).sort((a, b) => a.t - b.t);
  const along = type !== 'bar' && type !== 'pie' && (time || numeric(cols[x]?.type) || x < 0); // (a line over x's values, not one bar each)
  data = data.slice(0, type === 'bar' ? 60 : type === 'pie' ? 12 : 5000);
  if (!data.length) return h('div', { class: 'empty' }, 'Nothing to chart.');
  const svgOf = (inner, label) => h('div', { class: 'art', html: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="${esc(label)}">${inner}</svg>` });
  const legend = ys.map((j, k) => `<span><i style="background:${PALETTE[k % PALETTE.length]}"></i>${esc(cols[j].name)}</span>`).join('');
  const what = `A ${type} chart of ${ys.map(j => cols[j].name).join(', ')} by ${x >= 0 ? cols[x].name : 'row'}`;
  if (type === 'pie') {
    const vals = data.map(d => Math.max(0, d.y[0] ?? 0)), sum = vals.reduce((a, b) => a + b, 0) || 1, cx = W / 2 - 120, cy = H / 2, rad = H / 2 - 16;
    let a0 = -Math.PI / 2, out = '';
    vals.forEach((v, i) => {
      const a1 = a0 + 2 * Math.PI * v / sum, big = a1 - a0 > Math.PI ? 1 : 0, c = `hsl(${(165 + i * 47) % 360} 55% 48%)`;
      out += `<path d="M${cx},${cy} L${cx + rad * Math.cos(a0)},${cy + rad * Math.sin(a0)} A${rad},${rad} 0 ${big} 1 ${cx + rad * Math.cos(a1)},${cy + rad * Math.sin(a1)} Z" fill="${c}" stroke="var(--surface)" stroke-width="1.5"><title>${esc(String(data[i].x))}: ${esc(String(v))}</title></path>`;
      out += `<rect x="${W / 2 + 10}" y="${20 + i * 19}" width="10" height="10" rx="2" fill="${c}"/><text x="${W / 2 + 26}" y="${29 + i * 19}">${esc(String(data[i].x ?? '').slice(0, 28))} · ${(100 * v / sum).toFixed(1)}%</text>`;
      a0 = a1;
    });
    return svgOf(out, what);
  }
  const vals = data.flatMap(d => d.y).filter(v => v != null && Number.isFinite(v)), lo = Math.min(0, ...vals), hi = Math.max(0, ...vals) || 1;
  const sy = v => T + (H - T - B) * (1 - (v - lo) / (hi - lo || 1));
  let out = [lo, lo + (hi - lo) / 2, hi].map(v => `<line x1="${L}" x2="${W - Rt}" y1="${sy(v)}" y2="${sy(v)}" stroke="var(--line2)"/><text x="${L - 8}" y="${sy(v) + 4}" text-anchor="end">${esc(short(v))}</text>`).join('');
  if (along) {
    const xv = d => time ? d.t : x >= 0 ? Number(d.x) : d.x, xs = data.map(xv).filter(Number.isFinite), x0 = Math.min(...xs), x1 = Math.max(...xs);
    const sx = v => L + (W - L - Rt) * ((v - x0) / (x1 - x0 || 1));
    ys.forEach((_, j) => {
      const pts = data.filter(d => d.y[j] != null && Number.isFinite(xv(d))).map(d => [sx(xv(d)), sy(d.y[j])]), c = PALETTE[j % PALETTE.length];
      if (type === 'point') out += pts.map(([a, b]) => `<circle cx="${a.toFixed(1)}" cy="${b.toFixed(1)}" r="3" fill="${c}" opacity=".8"/>`).join('');
      else {
        const line = pts.map(([a, b]) => `${a.toFixed(1)},${b.toFixed(1)}`).join(' ');
        if (type === 'area' && pts.length) out += `<polygon points="${pts[0][0].toFixed(1)},${sy(0)} ${line} ${pts.at(-1)[0].toFixed(1)},${sy(0)}" fill="${c}" opacity=".18"/>`;
        out += `<polyline fill="none" stroke="${c}" stroke-width="2" points="${line}"/>`;
      }
    });
    const lab = d => time ? String(d.x).slice(0, 16) : String(d.x);
    out += [data[0], data[Math.floor(data.length / 2)], data.at(-1)].map((d, i) => `<text x="${sx(xv(d))}" y="${H - 10}" text-anchor="${['start', 'middle', 'end'][i]}">${esc(lab(d))}</text>`).join('');
  } else {
    const band = (W - L - Rt) / data.length, bw = Math.max(2, band * 0.72 / ys.length);
    out += data.map((d, i) => d.y.map((v, j) => v == null ? '' : `<rect x="${(L + i * band + band * 0.14 + j * bw).toFixed(1)}" y="${Math.min(sy(v), sy(0)).toFixed(1)}" width="${bw.toFixed(1)}" height="${Math.abs(sy(0) - sy(v)).toFixed(1)}" rx="2" fill="${PALETTE[j % PALETTE.length]}"><title>${esc(String(d.x))}: ${esc(String(v))}</title></rect>`).join('')).join('');
    const every = Math.ceil(data.length / 12);
    out += data.map((d, i) => i % every ? '' : `<text x="${L + i * band + band / 2}" y="${H - 10}" text-anchor="middle">${esc(String(d.x ?? '').slice(0, 12))}</text>`).join('');
  }
  const el = svgOf(out, what);
  el.prepend(h('div', { class: 'legend', html: legend }));
  return el;
}

/** The chart as a file: its colours and text set from the page's (a file has no style sheet), on the page's background. */
function exportAs(art, kind, name) {
  const svg = art.querySelector('svg');
  if (!svg) return toast('Nothing to save', true);
  const css = getComputedStyle(document.documentElement), color = v => v?.replace(/var\((--[\w-]+)\)/g, (_, k) => css.getPropertyValue(k).trim());
  const copy = svg.cloneNode(true), [, , W, H] = copy.getAttribute('viewBox').split(' ').map(Number);
  copy.setAttribute('width', W); copy.setAttribute('height', H);
  copy.querySelectorAll('[fill],[stroke]').forEach(el => { for (const a of ['fill', 'stroke']) if (el.getAttribute(a)) el.setAttribute(a, color(el.getAttribute(a))); });
  copy.querySelectorAll('text').forEach(t => { t.setAttribute('fill', color('var(--muted)')); t.setAttribute('font-family', 'system-ui, sans-serif'); t.setAttribute('font-size', '11'); });
  copy.insertAdjacentHTML('afterbegin', `<rect width="100%" height="100%" fill="${color('var(--surface)')}"/>`);
  const text = new XMLSerializer().serializeToString(copy);
  if (kind === 'svg') return saveAs(text, 'image/svg+xml', name + '.svg');
  const img = new Image(), url = URL.createObjectURL(new Blob([text], { type: 'image/svg+xml' }));
  img.onload = () => {
    const c = h('canvas', { width: W * 2, height: H * 2 }), g = c.getContext('2d');
    g.scale(2, 2); g.drawImage(img, 0, 0, W, H); URL.revokeObjectURL(url);
    c.toBlob(b => saveAs(b, 'image/png', name + '.png'));
  };
  img.src = url;
}
const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
const short = n => Math.abs(n) >= 1e9 ? (n / 1e9).toFixed(1) + 'B' : Math.abs(n) >= 1e6 ? (n / 1e6).toFixed(1) + 'M' : Math.abs(n) >= 1e4 ? (n / 1e3).toFixed(1) + 'K' : (+n.toFixed(2)).toLocaleString('en-US');
