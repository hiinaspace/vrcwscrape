// Detail-panel "History": upload timeline (one tick per upload event, or per-month bars for busy worlds, plus a package-size step line per
// platform) and a visits / favorites sparkline with its true coverage. Pure SVG, no libraries.
// An "upload" is an upload EVENT (package rows of one world within 10 minutes of each other, any platform); ticks and bars count events.
import { dateOfDay } from "../engine";
import type { Engine } from "../engine";
import { loadHistory } from "../hist";
import type { WorldHistory } from "../hist";
import { esc, fmt } from "./util";

const DENSE_AT = 60; // more upload events than this: monthly bars instead of individual ticks
const W = 480;
const COL = ["var(--m-pc)", "var(--m-q)", "var(--m-ios)", "var(--dim)"];
const PNAME = ["PC", "Quest", "iOS", "other"];
const DAY = 86400000;
const EPOCH = Date.UTC(2010, 0, 1);
const dayMs = (d: number) => EPOCH + d * DAY;
const mb = (kb: number) => (kb >= 1024 ? (kb / 1024).toFixed(kb >= 10240 ? 0 : 1) + " MB" : kb + " KB");
const monthStart = (m: number) => Math.round((Date.UTC(2010 + Math.floor(m / 12), m % 12, 1) - EPOCH) / DAY);
const monthDay = (m: number) => monthStart(m) + 14;
const monthOf = (day: number) => { const dt = new Date(dayMs(day)); return (dt.getUTCFullYear() - 2010) * 12 + dt.getUTCMonth(); };
const ym = (m: number) => new Date(Date.UTC(2010 + Math.floor(m / 12), m % 12, 1)).toISOString().slice(0, 7);

/** gap before the data as-of date (never "ago": the data is not live) */
function before(days: number): string {
  if (days <= 0) return "on the data as-of date";
  if (days === 1) return "1 day before the data as-of date";
  if (days < 60) return `${days} days before the data as-of date`;
  if (days < 730) return `${Math.round(days / 30.4)} months before the data as-of date`;
  return `${(days / 365.25).toFixed(1)} years before the data as-of date`;
}
const ymd = (d: number) => dateOfDay(d);

/** year gridlines + labels for a day range; a label within `pad` units of either edge label is dropped */
function xTicks(d0: number, d1: number, x: (d: number) => number, bottom: number, ay: number, pad = 56): string {
  const y0 = new Date(dayMs(d0)).getUTCFullYear(), y1 = new Date(dayMs(d1)).getUTCFullYear();
  const step = Math.max(1, Math.ceil((y1 - y0 + 1) / 6));
  let s = "";
  for (let y = y0 + 1; y <= y1; y += step) {
    const d = Math.round((Date.UTC(y, 0, 1) - EPOCH) / DAY);
    if (d <= d0 || d >= d1) continue;
    const near = x(d) - x(d0) < pad || x(d1) - x(d) < pad;
    s += `<line class="gx" x1="${x(d).toFixed(1)}" x2="${x(d).toFixed(1)}" y1="0" y2="${bottom}"/>`;
    if (!near) s += `<text class="ax" x="${x(d).toFixed(1)}" y="${ay}" text-anchor="middle">${y}</text>`;
  }
  return s;
}

export function renderUploads(h: WorldHistory, asofDay: number, lastDay: number): string {
  const rows = h.recent;
  const firstDay = h.old ? monthStart(h.old.m0) : rows[0]?.day ?? h.firstDay;
  const lastRow = rows.length ? rows[rows.length - 1].day : asofDay;
  const d0 = Math.min(firstDay, h.firstDay || firstDay), d1 = Math.max(asofDay, lastRow);
  const px0 = 52, px1 = W - 8;
  const x = (d: number) => px0 + ((d - d0) / Math.max(1, d1 - d0)) * (px1 - px0);
  const dense = h.events > DENSE_AT;
  // size step lines (per platform: pc, quest, ios): [day, kb]
  const pts: [number, number][][] = [[], [], [], []];
  if (h.old) h.old.count.forEach((_, k) => { const d = monthDay(h.old!.m0 + k); if (h.old!.pcKb[k]) pts[0].push([d, h.old!.pcKb[k]]); if (h.old!.questKb[k]) pts[1].push([d, h.old!.questKb[k]]); });
  for (const r of rows) for (let p = 0; p < 4; p++) if (r.kb[p]) pts[p].push([r.day, r.kb[p]]);
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
  const laneTop = top + SH + 10;
  let bottom: number;
  let marks = "";
  if (!dense) {
    // one tick per upload event; a multi-platform event is one tick split into platform-coloured segments
    for (const r of rows) {
      const ps = [0, 1, 2, 3].filter((p) => r.mask & (1 << p));
      const seg = 14 / Math.max(1, ps.length);
      const tip = `${ymd(r.day)} · ${ps.map((p) => PNAME[p] + (r.kb[p] ? " " + mb(r.kb[p]) : "")).join(", ")}${r.deleted ? " · older version (since replaced)" : ""}`;
      marks += `<g data-testid="upload-tick" class="tick${r.deleted ? " del" : ""}"><title>${esc(tip)}</title>`
        + ps.map((p, i) => `<line x1="${x(r.day).toFixed(1)}" x2="${x(r.day).toFixed(1)}" y1="${(laneTop + i * seg).toFixed(1)}" y2="${(laneTop + (i + 1) * seg).toFixed(1)}" stroke="${COL[p]}"/>`).join("") + `</g>`;
    }
    bottom = laneTop + 14;
  } else {
    const mon = new Map<number, number>();
    if (h.old) h.old.count.forEach((c, k) => { if (c) mon.set(h.old!.m0 + k, (mon.get(h.old!.m0 + k) ?? 0) + c); });
    for (const r of rows) { const m = monthOf(r.day); mon.set(m, (mon.get(m) ?? 0) + 1); }
    const mx = Math.max(1, ...mon.values());
    const BH = 44;
    const mw = Math.max(1.2, ((px1 - px0) / Math.max(1, d1 - d0)) * 30.4 - 0.6);
    for (const [m, n] of mon) {
      const hgt = (n / mx) * BH;
      marks += `<rect data-testid="upload-month" x="${x(monthStart(m)).toFixed(1)}" y="${(laneTop + BH - hgt).toFixed(1)}" width="${mw.toFixed(1)}" height="${hgt.toFixed(1)}" class="mbar" data-count="${n}"><title>${ym(m)}: ${n} upload${n === 1 ? "" : "s"}</title></rect>`;
    }
    marks += `<text class="ax" x="${px0 - 4}" y="${laneTop + 9}" text-anchor="end">max ${mx}</text><text class="ax" x="${px0 - 4}" y="${laneTop + BH}" text-anchor="end">0</text>`;
    bottom = laneTop + BH;
  }
  const H = bottom + 16, AY = bottom + 12;
  const ticks = xTicks(d0, d1, x, bottom, AY);
  const yl = `<text class="ax" x="${px0 - 4}" y="${top + 8}" text-anchor="end">${mb(maxKb)}</text><text class="ax" x="${px0 - 4}" y="${top + SH}" text-anchor="end">0</text>`;
  const axis = `<line class="base" x1="${px0}" x2="${px1}" y1="${top + SH}" y2="${top + SH}"/>`;
  const edge = `<text class="ax" x="${px0}" y="${AY}" text-anchor="start">${ymd(d0).slice(0, 7)}</text><text class="ax" x="${px1}" y="${AY}" text-anchor="end">${ymd(d1)}</text>`;
  const legend = [0, 1, 2].filter((k) => pts[k].length).map((k) => `<span><i style="background:${COL[k]}"></i>${PNAME[k]}</span>`).join("")
    + `<span class="dim">line = package size per platform; ${dense ? "bars = uploads per month" : "ticks = one per upload, coloured by platform"}</span>`;
  // summary
  const firstKb = (k: number) => (h.old ? (k === 0 ? h.old.pcKb : h.old.questKb).find((v) => v) : 0) || rows.find((r) => r.kb[k])?.kb[k] || 0;
  const lastKb = (k: number) => [...rows].reverse().find((r) => r.kb[k])?.kb[k] || (h.old ? [...(k === 0 ? h.old.pcKb : h.old.questKb)].reverse().find((v) => v) : 0) || 0;
  const sizes = [0, 1].map((k) => (firstKb(k) ? `${PNAME[k]} ${mb(firstKb(k))}→${mb(lastKb(k))}` : "")).filter(Boolean).join(" · ");
  const lastD = rows.length ? rows[rows.length - 1].day : lastDay;
  const sum = `${h.events.toLocaleString()} upload${h.events === 1 ? "" : "s"} · last ${ymd(lastD)} <span class="dim">(${before(asofDay - lastD)})</span>${sizes ? " · " + esc(sizes) : ""}`
    + (h.old ? ` <span class="dim">· per-upload sizes from ${ymd(rows[0].day).slice(0, 7)}; earlier months combined</span>` : "");
  return `<p class="hsum" data-testid="history-summary">${sum}</p>
    <svg class="hsvg" data-testid="upload-timeline" data-events="${h.events}" viewBox="0 0 ${W} ${H}" role="img" aria-label="package upload timeline" preserveAspectRatio="xMidYMid meet">${ticks}${axis}${svg}${marks}${yl}${edge}</svg>
    <div class="hleg">${legend}</div>`;
}

export function renderMetrics(h: WorldHistory, asofDay: number, metricsFrom: string): string {
  const m = h.metrics;
  const from = Math.round((Date.parse(metricsFrom + "-01T00:00:00Z") - EPOCH) / DAY);
  if (!m || m.day.length < 1) return `<h4>Visits and favorites</h4><p class="dim" data-testid="visits-spark" data-n="0">No visit snapshots for this world (tracking started ${esc(metricsFrom)}; cold worlds are sampled rarely).</p>`;
  const d0 = from, d1 = Math.max(asofDay, m.day[m.day.length - 1]);
  const px0 = 46, px1 = W - 46, PH = 70, top = 6;
  const x = (d: number) => px0 + ((d - d0) / Math.max(1, d1 - d0)) * (px1 - px0);
  const series = (ser: number[], cls: string, label: string, side: "l" | "r") => {
    const lo = Math.min(...ser), hi = Math.max(...ser), span = Math.max(1, hi - lo);
    const y = (v: number) => top + PH - ((v - lo) / span) * PH;
    let path = "", dots = "";
    ser.forEach((v, i) => {
      const cx = x(m.day[i]).toFixed(1), cy = y(v).toFixed(1);
      path += i && m.day[i] - m.day[i - 1] <= 21 ? `L${cx} ${cy}` : `M${cx} ${cy}`; // sparse stretches stay bare points
      dots += `<circle cx="${cx}" cy="${cy}" r="${ser.length > 40 ? 1.2 : 2.2}" class="pt ${cls}"><title>${ymd(m.day[i])}: ${v.toLocaleString()} ${label}</title></circle>`;
    });
    const tx = side === "l" ? px0 - 5 : px1 + 5, anchor = side === "l" ? "end" : "start";
    return `<path d="${path}" class="ln ${cls}"/>${dots}<text class="axc ${cls}" x="${tx}" y="${top + 8}" text-anchor="${anchor}">${fmt(hi)}</text>${lo !== hi ? `<text class="axc ${cls}" x="${tx}" y="${top + PH}" text-anchor="${anchor}">${fmt(lo)}</text>` : ""}`;
  };
  const bottom = top + PH, H = bottom + 20;
  const svg = `<svg class="hsvg" data-testid="visits-spark" data-n="${m.day.length}" viewBox="0 0 ${W} ${H}" role="img" aria-label="visits and favorites over time">${xTicks(d0, d1, x, bottom, bottom + 12)}
    <line class="base" x1="${px0}" x2="${px1}" y1="${bottom}" y2="${bottom}"/>${series(m.vis, "vis", "visits", "l")}${series(m.fav, "fav", "favorites", "r")}
    <text class="ax" x="${px0}" y="${bottom + 12}" text-anchor="start">${esc(metricsFrom)}</text><text class="ax" x="${px1}" y="${bottom + 12}" text-anchor="end">${ymd(d1)}</text></svg>`;
  const dv = m.vis[m.vis.length - 1] - m.vis[0];
  const cov = `${m.total.toLocaleString()} snapshot${m.total === 1 ? "" : "s"}${m.total > m.day.length ? ` (${m.day.length} shown, thinned)` : ""} between ${ymd(m.day[0])} and ${ymd(m.day[m.day.length - 1])}; tracking started ${metricsFrom}`;
  const note = m.day.length < 4 ? "Too few snapshots for a trend: points only." : dv > 0 ? `+${dv.toLocaleString()} visits over this window.` : "";
  return `<h4><span class="vis-t">Visits (left axis)</span> and <span class="fav-t">favorites (right axis)</span></h4>${svg}<p class="hnote">${esc(cov)}. Lines join snapshots at most 3 weeks apart; gaps are not interpolated. ${esc(note)}</p>`;
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
  const up = h.events || h.recent.length ? renderUploads(h, asof, h.recent.length ? h.recent[h.recent.length - 1].day : asof) : `<p class="dim" data-testid="history-summary">No package uploads recorded.</p>`;
  host.dataset.events = String(h?.events ?? 0);
  host.innerHTML = `<h3>History</h3>${up}${renderMetrics(h, asof, hi.metricsFrom)}<p class="hnote dim">upload history as of ${esc(hi.asof ?? "")} (data is refreshed by hand); from the public data mirror</p>`;
}
