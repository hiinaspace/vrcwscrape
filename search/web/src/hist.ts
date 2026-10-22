// Per-world history (round 6): lazily fetched shards `hist/<k>.bin` (format: build/build_history.py docstring). Only the detail panel calls this.
import { cached, fetchBin } from "./data";
import type { Manifest } from "./data";

export const PLATFORMS = ["pc", "quest", "ios", "other"] as const;
/** one upload event: package rows of one world chained within 10 minutes; mask bits 0-3 = platforms present (pc, quest, ios, other); kb[p] = package size, 0 unknown */
export interface UploadRow { day: number; mask: number; deleted: boolean; kb: number[] }
export interface WorldHistory {
  /** true number of upload events (also when the payload is capped) */
  events: number;
  firstDay: number;
  /** events older than the per-world cap: monthly event counts + last known package size (KB, 0 none) per platform, month index since 2010-01 */
  old: { m0: number; count: number[]; pcKb: number[]; questKb: number[] } | null;
  recent: UploadRow[];
  metrics: { total: number; day: number[]; vis: number[]; fav: number[] } | null;
}

class Rd {
  p = 0;
  constructor(public b: Uint8Array) {}
  v(): number { let r = 0, s = 1, c: number; do { c = this.b[this.p++]; r += (c & 127) * s; s *= 128; } while (c & 128); return r; }
  z(): number { const x = this.v(); return x % 2 ? -(x + 1) / 2 : x / 2; }
  u8(): number { return this.b[this.p++]; }
}

function decode(buf: Uint8Array): WorldHistory | null {
  const r = new Rd(buf);
  const flags = r.u8();
  if (!flags) return null;
  const h: WorldHistory = { events: 0, firstDay: 0, old: null, recent: [], metrics: null };
  if (flags & 1) {
    h.events = r.v(); h.firstDay = r.v();
    const nOld = r.v();
    if (nOld) {
      const m0 = r.v(), o = { m0, count: [] as number[], pcKb: [] as number[], questKb: [] as number[] };
      for (let k = 0; k < nOld; k++) { o.count.push(r.v()); o.pcKb.push(r.v()); o.questKb.push(r.v()); }
      h.old = o;
    }
    const nk = r.v();
    let day = r.v();
    for (let k = 0; k < nk; k++) {
      day += r.v();
      const f = r.u8(), kb = [0, 0, 0, 0];
      for (let p = 0; p < 4; p++) if (f & (1 << p)) { const v = r.v(); kb[p] = v; }
      h.recent.push({ day, mask: f & 15, deleted: (f & 16) !== 0, kb });
    }
  }
  if (flags & 2) {
    const total = r.v(), n = r.v();
    const m = { total, day: [] as number[], vis: [] as number[], fav: [] as number[] };
    let d = 0, v = 0, f = 0;
    for (let k = 0; k < n; k++) { d += r.v(); v += r.z(); f += r.z(); m.day.push(d); m.vis.push(v); m.fav.push(f); }
    h.metrics = m;
  }
  return h;
}

/** null = no history data in this build, or none for this world */
export async function loadHistory(m: Manifest, idx: number): Promise<WorldHistory | null> {
  const hi = m.history;
  if (!hi) return null;
  let k = 0, lo = 0, up = hi.starts.length - 1;
  while (lo <= up) { const mid = (lo + up) >> 1; if (hi.starts[mid] <= idx) { k = mid; lo = mid + 1; } else up = mid - 1; }
  const end = k + 1 < hi.starts.length ? hi.starts[k + 1] : m.n;
  const sh = await cached("hist" + k, async () => {
    const buf = await fetchBin(`hist/${k}.bin`);
    const r = new Rd(buf);
    const nw = end - hi.starts[k];
    const lens: number[] = [];
    for (let i = 0; i < nw; i++) lens.push(r.v());
    const offs: number[] = [];
    let o = r.p;
    for (const l of lens) { offs.push(o); o += l; }
    return { buf, offs, lens };
  });
  const j = idx - hi.starts[k];
  return decode(sh.buf.subarray(sh.offs[j], sh.offs[j] + sh.lens[j]));
}
