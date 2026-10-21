// Detail-panel "History": upload timeline (ticks per platform, or per-month bars for busy worlds, plus a package-size step line)
// and a visits / favorites sparkline with its true coverage. Pure SVG, no libraries.
import { dateOfDay } from "../engine";
import type { Engine } from "../engine";
import { loadHistory } from "../hist";
import type { UploadRow, WorldHistory } from "../hist";
import { esc, fmt } from "./util";

const DENSE_AT = 60; // more raw uploads than this: monthly bars instead of individual ticks
const W = 480;
const COL = ["var(--m-pc)", "var(--m-q)", "var(--m-ios)", "var(--dim)"];
const PNAME = ["PC", "Quest", "iOS", "other"];
const DAY = 86400000;
const EPOCH = Date.UTC(2010, 0, 1);
const dayMs = (d: number) => EPOCH + d * DAY;
const mb = (kb: number) => (kb >= 1024 ? (kb / 1024).toFixed(kb >= 10240 ? 0 : 1) + " MB" : kb + " KB");
const monthDay = (m: number) => Math.round((Date.UTC(2010 + Math.floor(m / 12), m % 12, 15) - EPOCH) / DAY); // mid-month as a day index

function ago(days: number): string {
  if (days <= 0) return "today";
  if (days === 1) return "yesterday";
  if (days < 60) return `${days} days ago`;
  if (days < 730) return `${Math.round(days / 30.4)} months ago`;
  return `${(days / 365.25).toFixed(1)} years ago`;
}
const ymd = (d: number) => dateOfDay(d);

/** year ticks for a day range, thinned to ~6 labels */
function xTicks(d0: number, d1: number, x: (d: number) => number): string {
  const y0 = new Date(dayMs(d0)).getUTCFullYear(), y1 = new Date(dayMs(d1)).getUTCFullYear();
  const step = Math.max(1, Math.ceil((y1 - y0 + 1) / 6));
  let s = "";
  for (let y = y0 + 1; y <= y1; y += step) {
    const d = Math.round((Date.UTC(y, 0, 1) - EPOCH) / DAY);
    if (d <= d0 || d >= d1) continue;
    s += `<line class="gx" x1="${x(d).toFixed(1)}" x2="${x(d).toFixed(1)}" y1="0" y2="H"/><text class="ax" x="${x(d).toFixed(1)}" y="AY" text-anchor="middle">${y}</text>`;
  }
  return s;
}

export function renderUploads(h: WorldHistory, asofDay: number, lastDay: number): string {
  const rows = h.recent;
  const firstDay = h.old ? Math.round((Date.UTC(2010 + Math.floor(h.old.m0 / 12), h.old.m0 % 12, 1) - EPOCH) / DAY) : rows[0]?.day ?? h.firstDay;
  const lastRow = rows.length ? rows[rows.length - 1].day : asofDay;
  const d0 = Math.min(firstDay, h.firstDay || firstDay), d1 = Math.max(asofDay, lastRow);
  const px0 = 34, px1 = W - 8;
  const x = (d: number) => px0 + ((d - d0) / Math.max(1, d1 - d0)) * (px1 - px0);
  const dense = h.rows > DENSE_AT;
  // size step lines (per platform): [day, kb]
  const pts: [number, number][][] = [[], [], [], []];
  if (h.old) h.old.count.forEach((_, k) => { const d = monthDay(h.old!.m0 + k); if (h.old!.pcKb[k]) pts[0].push([d, h.old!.pcKb[k]]); if (h.old!.questKb[k]) pts[1].push([d, h.old!.questKb[k]]); });
  for (const r of rows) if (r.kb) pts[r.plat].push([r.day, r.kb]);
  const maxKb = Math.max(1, ...pts.flat().map((p) => p[1]));
  const SH = 64, top = 6;
  const ys = (kb: number) => top + SH - (kb / maxKb) * SH;
  let svg = "";
  pts.forEach((p, k) => {
    if (!p.length) return;
    let d = `M${x(p[0][0]).toFixed(1)} ${ys(p[0][1]).toFixed(1)}`;
    for (let i = 1; i < p.length; i++) d += `H${x(p[i][0]).toFixed(1)}V${ys(p[i][1]).toFixed(1)}`;
    d += `H${x(d1).toFixed(1)}`;
    svg += `<path class="step" d="${d}" stroke="${COL[k]}"><title>${PNAME[k]} package size over time</title></path>`;
  });
  // lanes / density
  const laneTop = top + SH + 10;
  let bottom: number;
  let marks = "";
  if (!dense) {
    const present = [0, 1, 2, 3].filter((k) => rows.some((r) => r.plat === k));
    present.forEach((k, li) => {
      const y = laneTop + li * 13;
      marks += `<text class="ax" x="${px0 - 4}" y="${y + 9}" text-anchor="end">${PNAME[k]}</text>`;
      for (const r of rows) if (r.plat === k) marks += `<line data-testid="upload-tick" class="tick${r.deleted ? " del" : ""}" x1="${x(r.day).toFixed(1)}" x2="${x(r.day).toFixed(1)}" y1="${y}" y2="${y + 11}" stroke="${COL[k]}"><title>${PNAME[k]} · ${ymd(r.day)}${r.kb ? " · " + mb(r.kb) : ""}${r.deleted ? " · deleted" : ""}</title></line>`;
    });
    bottom = laneTop + Math.max(1, present.length) * 13;
  } else {
    // per-month stacked bars: kept rows by platform, older rows as one grey "older, all platforms" bar
    const mon = new Map<number, number[]>();
    const add = (m: number, k: number, n: number) => { const a = mon.get(m) ?? [0, 0, 0, 0, 0]; a[k] += n; mon.set(m, a); };
    if (h.old) h.old.count.forEach((c, k) => { if (c) add(h.old!.m0 + k, 4, c); });
    for (const r of rows) { const dt = new Date(dayMs(r.day)); add((dt.getUTCFullYear() - 2010) * 12 + dt.getUTCMonth(), r.plat, 1); }
    const mx = Math.max(1, ...[...mon.values()].map((a) => a.reduce((s, v) => s + v, 0)));
    const BH = 44;
    const mw = Math.max(1.2, ((px1 - px0) / Math.max(1, d1 - d0)) * 30.4 - 0.6);
    for (const [m, a] of mon) {
      const cx = x(Math.round((Date.UTC(2010 + Math.floor(m / 12), m % 12, 1) - EPOCH) / DAY));
      let yy = laneTop + BH;
      const tot = a.reduce((s, v) => s + v, 0);
      a.forEach((n, k) => {
        if (!n) return;
        const hgt = (n / mx) * BH; yy -= hgt;
        marks += `<rect data-testid="upload-month" x="${cx.toFixed(1)}" y="${yy.toFixed(1)}" width="${mw.toFixed(1)}" height="${hgt.toFixed(1)}" fill="${k === 4 ? "var(--dim)" : COL[k]}" opacity="${k === 4 ? 0.5 : 0.9}"><title>${new Date(Date.UTC(2010 + Math.floor(m / 12), m % 12, 1)).toISOString().slice(0, 7)}: ${tot} package upload${tot === 1 ? "" : "s"}${k === 4 ? " (older history, all platforms)" : ""}</title></rect>`;
      });
    }
    marks += `<text class="ax" x="${px0 - 4}" y="${laneTop + 9}" text-anchor="end">max ${mx}</text><text class="ax" x="${px0 - 4}" y="${laneTop + BH}" text-anchor="end">0</text>`;
    bottom = laneTop + BH;
  }
  const H = bottom + 16, AY = bottom + 12;
  const ticks = xTicks(d0, d1, x).replace(/y2="H"/g, `y2="${bottom}"`).replace(/y="AY"/g, `y="${AY}"`);
  const yl = `<text class="ax" x="${px0 - 4}" y="${top + 8}" text-anchor="end">${mb(maxKb)}</text><text class="ax" x="${px0 - 4}" y="${top + SH}" text-anchor="end">0</text>`;
  const axis = `<line class="base" x1="${px0}" x2="${px1}" y1="${top + SH}" y2="${top + SH}"/>`;
  const edge = `<text class="ax" x="${px0}" y="${AY}" text-anchor="start">${ymd(d0).slice(0, 7)}</text><text class="ax" x="${px1}" y="${AY}" text-anchor="end">${ymd(d1).slice(0, 7)}</text>`;
  const legend = [0, 1, 2, 3].filter((k) => pts[k].length || rows.some((r) => r.plat === k)).map((k) => `<span><i style="background:${COL[k]}"></i>${PNAME[k]}</span>`).join("")
    + (dense && h.old ? `<span><i style="background:var(--dim);opacity:.5"></i>older (all platforms)</span>` : "");
  // summary
  const first = (k: number) => (h.old ? (k === 0 ? h.old.pcKb : h.old.questKb).find((v) => v) : 0) || rows.find((r) => r.plat === k && r.kb)?.kb || 0;
  const lastKb = (k: number) => [...rows].reverse().find((r) => r.plat === k && r.kb)?.kb || (h.old ? [...(k === 0 ? h.old.pcKb : h.old.questKb)].reverse().find((v) => v) : 0) || 0;
  const sizes = [0, 1].map((k) => (first(k) ? `${PNAME[k]} ${mb(first(k))}→${mb(lastKb(k))}` : "")).filter(Boolean).join(" · ");
  const lastD = rows.length ? rows[rows.length - 1].day : lastDay;
  const sum = `${h.events.toLocaleString()} upload${h.events === 1 ? "" : "s"} · last <span title="${ymd(lastD)}">${ago(asofDay - lastD)}</span>${sizes ? " · " + esc(sizes) : ""}`
    + (h.old ? ` <span class="dim">· showing monthly counts before ${ymd(rows[0].day).slice(0, 7)}</span>` : "");
  return `<p class="hsum" data-testid="history-summary">${sum}</p>
    <svg class="hsvg" data-testid="upload-timeline" viewBox="0 0 ${W} ${H}" role="img" aria-label="package upload timeline" preserveAspectRatio="xMidYMid meet">${ticks}${axis}${svg}${marks}${yl}${edge}</svg>
    <div class="hleg">${legend}<span class="dim">size per package (line), ${dense ? "uploads per month (bars)" : "each tick is one upload"}</span></div>`;
}

export function renderMetrics(h: WorldHistory, asofDay: number, metricsFrom: string): string {
  const m = h.metrics;
  const from = Math.round((Date.parse(metricsFrom + "-01T00:00:00Z") - EPOCH) / DAY);
  if (!m || m.day.length < 1) return `<h4>Visits and favorites</h4><p class="dim" data-testid="visits-spark" data-n="0">No visit snapshots for this world (tracking started ${esc(metricsFrom)}; cold worlds are sampled rarely).</p>`;
  const d0 = from, d1 = Math.max(asofDay, m.day[m.day.length - 1]);
  const px0 = 40, px1 = W - 10;
  const x = (d: number) => px0 + ((d - d0) / Math.max(1, d1 - d0)) * (px1 - px0);
  const chart = (ser: number[], cls: string, label: string, hgt: number) => {
    const lo = Math.min(...ser), hi = Math.max(...ser), span = Math.max(1, hi - lo);
    const y = (v: number) => 4 + hgt - ((v - lo) / span) * hgt;
    let path = "", dots = "";
    ser.forEach((v, i) => {
      const cx = x(m.day[i]).toFixed(1), cy = y(v).toFixed(1);
      // join only neighbouring snapshots <= 21 days apart: sparse stretches stay as bare points
      if (i && m.day[i] - m.day[i - 1] <= 21) path += `L${cx} ${cy}`; else path += `M${cx} ${cy}`;
      dots += `<circle cx="${cx}" cy="${cy}" r="${ser.length > 40 ? 1.2 : 2.2}" class="pt ${cls}"><title>${ymd(m.day[i])}: ${v.toLocaleString()} ${label}</title></circle>`;
    });
    return `<path d="${path}" class="ln ${cls}"/>${dots}<text class="ax" x="${px0 - 4}" y="${y(hi) + 3}" text-anchor="end">${fmt(hi)}</text>${lo !== hi ? `<text class="ax" x="${px0 - 4}" y="${y(lo) + 3}" text-anchor="end">${fmt(lo)}</text>` : ""}`;
  };
  const VH = 38, FH = 22;
  const ticks = xTicks(d0, d1, x).replace(/y2="H"/g, `y2="${VH + FH + 22}"`).replace(/y="AY"/g, `y="${VH + FH + 34}"`);
  const H = VH + FH + 40;
  const svg = `<svg class="hsvg" data-testid="visits-spark" data-n="${m.day.length}" viewBox="0 0 ${W} ${H}" role="img" aria-label="visits and favorites over time">${ticks}
    <g>${chart(m.vis, "vis", "visits", VH)}</g><g transform="translate(0 ${VH + 12})">${chart(m.fav, "fav", "favorites", FH)}</g>
    <text class="ax" x="${px0}" y="${H - 2}" text-anchor="start">${esc(metricsFrom)} (tracking starts)</text><text class="ax" x="${px1}" y="${H - 2}" text-anchor="end">${ymd(d1).slice(0, 7)}</text></svg>`;
  const dv = m.vis[m.vis.length - 1] - m.vis[0];
  const cov = `${m.total.toLocaleString()} snapshot${m.total === 1 ? "" : "s"}${m.total > m.day.length ? ` (${m.day.length} shown, thinned)` : ""} between ${ymd(m.day[0])} and ${ymd(m.day[m.day.length - 1])}`;
  const note = m.day.length < 4 ? "Too few snapshots for a trend: points only." : dv > 0 ? `+${dv.toLocaleString()} visits over this window.` : "";
  return `<h4>Visits <span class="vis-dot"></span> and favorites <span class="fav-dot"></span></h4>${svg}<p class="hnote">${esc(cov)}. Lines join snapshots at most 3 weeks apart; gaps are not interpolated. ${esc(note)}</p>`;
}

/** fills `host` (an element inside the open detail panel); `alive()` says whether the panel still shows this world */
export async function fillHistory(eng: Engine, host: HTMLElement, idx: number, alive: () => boolean): Promise<void> {
  const hi = eng.m.history;
  if (!hi) { host.hidden = true; return; }
  let h: WorldHistory | null = null;
  try { h = await loadHistory(eng.m, idx); } catch { h = null; }
  if (!alive()) return;
  const asof = hi.asof ? Math.round((Date.parse(hi.asof + "T00:00:00Z") - EPOCH) / DAY) : eng.lastDay;
  if (!h || (!h.events && !h.metrics)) { host.innerHTML = `<h3>History</h3><p class="dim">No package upload history recorded for this world.</p>`; return; }
  const up = h.events || h.recent.length ? renderUploads(h, asof, h.recent.length ? h.recent[h.recent.length - 1].day : asof) : `<p class="dim">No package uploads recorded.</p>`;
  host.innerHTML = `<h3>History</h3>${up}${renderMetrics(h, asof, hi.metricsFrom)}<p class="hnote dim">as of ${esc(hi.asof ?? "")}; from the public data mirror</p>`;
}
export type { UploadRow };
