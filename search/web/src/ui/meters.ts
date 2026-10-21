// Bars encode PERCENTILE RANK among all worlds that have the value (0..1), not a log value.
// Sorted copies of the columns come from the already-loaded core (no extra bytes); one rank is two binary searches.
import type { Core } from "../data";
import { NOSIZE } from "./util";

export type Kind = "cap" | "pc" | "quest" | "visits" | "fav";

export class Meters {
  private s = {} as Record<Kind, Float64Array | Uint32Array>;
  constructor(c: Core) {
    const col = (a: ArrayLike<number>, skip: (v: number) => boolean) => {
      const xs = new Uint32Array(a.length);
      let n = 0;
      for (let i = 0; i < a.length; i++) if (!skip(a[i])) xs[n++] = a[i];
      return xs.subarray(0, n).sort();
    };
    this.s.cap = col(c.cap, (v) => v < 1);
    this.s.pc = col(c.pc, (v) => v === NOSIZE);
    this.s.quest = col(c.quest, (v) => v === NOSIZE);
    this.s.visits = col(c.visits, () => false);
    this.s.fav = col(c.fav, () => false);
  }
  /** mid-rank percentile in [0,1]: share of worlds below v plus half of those equal to v */
  frac(kind: Kind, v: number): number {
    const xs = this.s[kind];
    let lo = 0, hi = xs.length;
    while (lo < hi) { const m = (lo + hi) >> 1; if (xs[m] < v) lo = m + 1; else hi = m; }
    const below = lo;
    hi = xs.length;
    while (lo < hi) { const m = (lo + hi) >> 1; if (xs[m] <= v) lo = m + 1; else hi = m; }
    return xs.length ? (below + (lo - below) / 2) / xs.length : 0;
  }
  /** "top 0.3%" style text: share of worlds with a strictly higher value (mid-rank) */
  top(kind: Kind, v: number): string {
    const p = (1 - this.frac(kind, v)) * 100;
    return p < 0.1 ? "top 0.1%" : p < 10 ? `top ${p.toFixed(1)}%` : `top ${Math.round(Math.max(1, p))}%`;
  }
}
