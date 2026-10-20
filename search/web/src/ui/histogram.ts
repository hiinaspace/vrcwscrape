// Brushable histogram of counts per month. Drag on the bars to select a range of months; drag a handle to resize,
// drag the selection body to move it; a plain click clears the brush. Emits month-bin indices (inclusive).
import { esc, fmt } from "./util";

export interface HistData { startYM: number; counts: number[] } // startYM = year*12 + month0
export interface Hist {
  el: HTMLElement;
  setData(d: HistData): void;
  /** brush as inclusive bin indices into the current data, or null; no event */
  setBrush(b: [number, number] | null): void;
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
export const ymLabel = (ym: number) => `${MONTHS[ym % 12]} ${Math.floor(ym / 12)}`;

export function histogram(opts: { testid?: string; onBrush: (b: [number, number] | null) => void }): Hist {
  const el = document.createElement("div");
  el.className = "hist";
  if (opts.testid) el.dataset.testid = opts.testid;
  el.innerHTML = `<div class="hist-plot" role="group" aria-label="uploads over time; drag to select a date range" tabindex="0">
      <div class="hist-bars"></div><div class="hist-brush" hidden><i class="h-l"></i><i class="h-r"></i></div></div>
    <div class="hist-axis"></div><div class="hist-tip" aria-live="polite"></div>`;
  const plot = el.querySelector(".hist-plot") as HTMLElement;
  const barsEl = el.querySelector(".hist-bars") as HTMLElement;
  const brushEl = el.querySelector(".hist-brush") as HTMLElement;
  const axis = el.querySelector(".hist-axis") as HTMLElement;
  const tip = el.querySelector(".hist-tip") as HTMLElement;
  let data: HistData = { startYM: 0, counts: [] };
  let brush: [number, number] | null = null;

  const nb = () => Math.max(1, data.counts.length);
  const binAt = (clientX: number) => {
    const r = plot.getBoundingClientRect();
    return Math.max(0, Math.min(nb() - 1, Math.floor(((clientX - r.left) / r.width) * nb())));
  };
  const paintBrush = () => {
    if (!brush) { brushEl.hidden = true; return; }
    brushEl.hidden = false;
    brushEl.style.left = (brush[0] / nb()) * 100 + "%";
    brushEl.style.width = ((brush[1] - brush[0] + 1) / nb()) * 100 + "%";
    tip.textContent = `${ymLabel(data.startYM + brush[0])} – ${ymLabel(data.startYM + brush[1])}`;
    barsEl.querySelectorAll<HTMLElement>("b").forEach((b, i) => b.classList.toggle("out", i < brush![0] || i > brush![1]));
  };
  const paintBars = () => {
    const mx = Math.max(1, ...data.counts);
    barsEl.innerHTML = data.counts.map((c, i) =>
      `<b title="${esc(ymLabel(data.startYM + i))}: ${c.toLocaleString()}" style="height:${c ? Math.max(3, Math.round((c / mx) * 100)) : 0}%"></b>`).join("");
    let ax = "";
    data.counts.forEach((_, i) => {
      const ym = data.startYM + i;
      if (ym % 12 === 0 && (nb() < 140 || (ym / 12) % 2 === 0)) ax += `<span style="left:${(i / nb()) * 100}%">${Math.floor(ym / 12)}</span>`;
    });
    axis.innerHTML = ax;
    if (!brush) tip.textContent = data.counts.length ? `${fmt(data.counts.reduce((a, b) => a + b, 0))} worlds · drag to select` : "";
    paintBrush();
  };

  type Drag = { kind: "new" | "l" | "r" | "move"; anchor: number; orig: [number, number] | null; moved: boolean };
  let drag: Drag | null = null;
  plot.addEventListener("pointerdown", (e) => {
    if (!data.counts.length) return;
    plot.setPointerCapture(e.pointerId);
    const b = binAt(e.clientX);
    const t = e.target as HTMLElement;
    if (brush && t.classList.contains("h-l")) drag = { kind: "l", anchor: b, orig: brush, moved: false };
    else if (brush && t.classList.contains("h-r")) drag = { kind: "r", anchor: b, orig: brush, moved: false };
    else if (brush && b >= brush[0] && b <= brush[1] && t.closest(".hist-brush")) drag = { kind: "move", anchor: b, orig: brush, moved: false };
    else drag = { kind: "new", anchor: b, orig: brush, moved: false };
    e.preventDefault();
  });
  plot.addEventListener("pointermove", (e) => {
    const b = binAt(e.clientX);
    if (!drag) { // hover readout
      if (!brush && data.counts.length) tip.textContent = `${ymLabel(data.startYM + b)}: ${data.counts[b].toLocaleString()}`;
      return;
    }
    const d = drag;
    if (b !== d.anchor) d.moved = true;
    if (!d.moved) return;
    if (d.kind === "new") brush = [Math.min(d.anchor, b), Math.max(d.anchor, b)];
    else if (d.kind === "l") brush = [Math.min(b, d.orig![1]), Math.max(b, d.orig![1])];
    else if (d.kind === "r") brush = [Math.min(b, d.orig![0]), Math.max(b, d.orig![0])];
    else {
      const w = d.orig![1] - d.orig![0], s = Math.max(0, Math.min(nb() - 1 - w, d.orig![0] + (b - d.anchor)));
      brush = [s, s + w];
    }
    paintBrush();
  });
  const end = () => {
    if (!drag) return;
    const d = drag;
    drag = null;
    if (!d.moved) {
      if (d.kind === "new") { // click: select that single month if nothing selected, else clear
        brush = d.orig ? null : [d.anchor, d.anchor];
        paintBrush();
        if (!brush) paintBars();
      }
    }
    opts.onBrush(brush);
  };
  plot.addEventListener("pointerup", end);
  plot.addEventListener("pointercancel", end);
  plot.addEventListener("pointerleave", () => { if (!drag && !brush) paintBars(); });
  plot.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && brush) { brush = null; paintBars(); opts.onBrush(null); }
  });

  return {
    el,
    setData(d) { data = d; paintBars(); },
    setBrush(b) { brush = b; paintBars(); },
  };
}
