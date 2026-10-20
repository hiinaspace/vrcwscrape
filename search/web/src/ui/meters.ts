// Log-scale bar lengths normalised to dataset quantiles (2nd..99.5th percentile of the sampled column),
// so a bar compares a world against the whole dataset, not against the rows on screen.
import type { Core } from "../data";
import { NOSIZE } from "./util";

type Kind = "cap" | "pc" | "quest" | "visits" | "fav";

export class Meters {
  private r = {} as Record<Kind, [number, number]>;
  constructor(c: Core) {
    const col = (a: ArrayLike<number>, skip: (v: number) => boolean): [number, number] => {
      const step = Math.max(1, Math.floor(a.length / 40000));
      const xs: number[] = [];
      for (let i = 0; i < a.length; i += step) if (!skip(a[i])) xs.push(Math.log10(1 + a[i]));
      xs.sort((x, y) => x - y);
      const q = (p: number) => xs[Math.min(xs.length - 1, Math.floor(p * xs.length))] ?? 0;
      const lo = q(0.02), hi = q(0.995);
      return [lo, Math.max(hi, lo + 0.1)];
    };
    this.r.cap = col(c.cap, (v) => v < 1);
    this.r.pc = col(c.pc, (v) => v === NOSIZE);
    this.r.quest = col(c.quest, (v) => v === NOSIZE);
    this.r.visits = col(c.visits, () => false);
    this.r.fav = col(c.fav, () => false);
  }
  /** 0..1, never fully empty for a present value so every bar is visible */
  frac(kind: Kind, v: number): number {
    const [lo, hi] = this.r[kind];
    return Math.max(0.04, Math.min(1, (Math.log10(1 + v) - lo) / (hi - lo)));
  }
}
