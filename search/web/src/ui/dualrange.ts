// Double-ended range slider over a list of discrete stops, built from two overlaid native <input type=range>.
// Native inputs keep keyboard/touch behaviour; the track fill is drawn behind them.
import { esc } from "./util";

export interface DualRange {
  el: HTMLElement;
  /** set thumb positions as stop indices (no event) */
  set(lo: number, hi: number): void;
  get(): [number, number];
}

export function dualRange(opts: {
  stops: string[]; // tick labels, one per stop
  tickEvery?: number;
  testid?: string;
  onInput: (lo: number, hi: number) => void;
  onChange?: (lo: number, hi: number) => void;
}): DualRange {
  const n = opts.stops.length - 1;
  const el = document.createElement("div");
  el.className = "dr";
  if (opts.testid) el.dataset.testid = opts.testid;
  const every = opts.tickEvery ?? 1;
  el.innerHTML = `<div class="dr-track"><div class="dr-fill"></div></div>
    <input type="range" class="dr-lo" min="0" max="${n}" step="1" value="0" aria-label="minimum">
    <input type="range" class="dr-hi" min="0" max="${n}" step="1" value="${n}" aria-label="maximum">
    <div class="dr-ticks">${opts.stops.map((s, i) => `<span style="left:${(i / n) * 100}%">${i % every === 0 ? esc(s) : ""}</span>`).join("")}</div>`;
  const lo = el.querySelector(".dr-lo") as HTMLInputElement;
  const hi = el.querySelector(".dr-hi") as HTMLInputElement;
  const fill = el.querySelector(".dr-fill") as HTMLElement;
  const paint = () => {
    const a = +lo.value, b = +hi.value;
    fill.style.left = (a / n) * 100 + "%";
    fill.style.right = (1 - b / n) * 100 + "%";
    // keep the thumb that is stuck at the far end reachable
    lo.style.zIndex = a >= n - 1 ? "3" : "2";
  };
  const clamp = (which: "lo" | "hi") => {
    if (+lo.value > +hi.value) { if (which === "lo") lo.value = hi.value; else hi.value = lo.value; }
  };
  for (const [inp, w] of [[lo, "lo"], [hi, "hi"]] as const) {
    inp.addEventListener("input", () => { clamp(w); paint(); opts.onInput(+lo.value, +hi.value); });
    inp.addEventListener("change", () => opts.onChange?.(+lo.value, +hi.value));
  }
  paint();
  return {
    el,
    set(a, b) { lo.value = String(a); hi.value = String(b); paint(); },
    get: () => [+lo.value, +hi.value],
  };
}
