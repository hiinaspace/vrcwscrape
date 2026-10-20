// Self-hosted AVIF thumbnails: packs of back-to-back AVIFs (build/pack_thumbs.py) fetched by HTTP Range.
// Requests issued in the same few ms are coalesced per pack (gap <= GAP bytes); if the host ignores Range (status 200) we fall back
// to downloading the whole pack once and slicing it.
import { Manifest, bytesFetched, cached, dataBase, dataVersion, fetchBin } from "./data";

const GAP = 8192; // merge two wanted ranges when the hole between them is at most this many bytes
const WAIT_MS = 3; // coalescing window

interface Head { nPacks: number; ws: number; bs: number; packFirst: number[]; maxBefore: Int32Array; bsBase: Uint32Array }
interface Want { pack: number; off: number; len: number; res: (b: Uint8Array | null) => void }

export interface ThumbStats { requests: number; bytes: number; payloadBytes: number; images: number; rangeSupported: boolean | null; fullPackFallbacks: number }

export class Thumbs {
  stats: ThumbStats = { requests: 0, bytes: 0, payloadBytes: 0, images: 0, rangeSupported: null, fullPackFallbacks: 0 };
  private idx: Promise<Head | null> | null = null;
  private urls = new Map<number, Promise<string | null>>(); // blob -> object URL
  private queue: Want[] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;
  private whole = new Map<number, Promise<Uint8Array>>();
  private base: URL;

  constructor(private m: Manifest) {
    this.base = new URL(m.thumbs!.base, dataBase);
  }

  private packUrl(p: number) {
    return new URL(`p-${String(p).padStart(3, "0")}.bin`, this.base) + (dataVersion() ? "?v=" + dataVersion() : "");
  }

  private head(): Promise<Head | null> {
    return (this.idx ??= (async () => {
      try {
        const buf = await fetchBin(this.m.thumbs!.index);
        const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
        if (String.fromCharCode(...buf.subarray(0, 4)) !== "THM2") return null;
        const n = dv.getUint32(4, true), nb = dv.getUint32(8, true), np = dv.getUint32(12, true), ws = dv.getUint32(16, true), bs = dv.getUint32(20, true);
        let p = 24;
        const packFirst: number[] = [];
        for (let k = 0; k < np; k++) { packFirst.push(dv.getUint32(p, true)); p += 8; }
        const nws = Math.ceil(n / ws), nbs = Math.ceil(nb / bs);
        const maxBefore = new Int32Array(nws);
        for (let k = 0; k < nws; k++) { maxBefore[k] = dv.getInt32(p, true); p += 4; }
        const bsBase = new Uint32Array(nbs);
        for (let k = 0; k < nbs; k++) { bsBase[k] = dv.getUint32(p, true); p += 4; }
        return { nPacks: np, ws, bs, packFirst, maxBefore, bsBase };
      } catch {
        return null;
      }
    })());
  }

  /** blob id of every world in world shard k (-1 = no thumbnail) */
  private worldShard(h: Head, k: number): Promise<Int32Array> {
    return cached("thw" + k, async () => {
      const buf = await fetchBin(`th/w${k}.bin`);
      const cnt = buf.length >> 2;
      const out = new Int32Array(cnt);
      let max = h.maxBefore[k];
      for (let i = 0; i < cnt; i++) {
        const c = (buf[i] | (buf[cnt + i] << 8) | (buf[2 * cnt + i] << 16) | (buf[3 * cnt + i] << 24)) >>> 0;
        out[i] = c === 0 ? ++max : c === 1 ? -1 : max - (c - 2);
      }
      return out;
    });
  }

  /** (length, offset-in-pack) of every blob in blob shard k */
  private blobShard(h: Head, k: number): Promise<{ len: Uint16Array; off: Uint32Array }> {
    return cached("thb" + k, async () => {
      const buf = await fetchBin(`th/b${k}.bin`);
      const cnt = buf.length >> 1;
      const len = new Uint16Array(cnt), off = new Uint32Array(cnt);
      const first = k * h.bs;
      let o = h.bsBase[k], pi = h.packFirst.findIndex((f, j) => f <= first && (j + 1 >= h.nPacks || h.packFirst[j + 1] > first));
      for (let i = 0; i < cnt; i++) {
        const b = first + i;
        if (pi + 1 < h.nPacks && b === h.packFirst[pi + 1]) { pi++; o = 0; }
        len[i] = buf[i] | (buf[cnt + i] << 8);
        off[i] = o;
        o += len[i];
      }
      return { len, off };
    });
  }

  private packOf(h: Head, b: number): number {
    let lo = 0, hi = h.nPacks - 1;
    while (lo < hi) { const mid = (lo + hi + 1) >> 1; if (h.packFirst[mid] <= b) lo = mid; else hi = mid - 1; }
    return lo;
  }

  /** object URL for world ordinal i, or null */
  async url(i: number): Promise<string | null> {
    const h = await this.head();
    if (!h) return null;
    let b: number;
    try {
      b = (await this.worldShard(h, Math.floor(i / h.ws)))[i % h.ws];
    } catch { return null; }
    if (b === undefined || b < 0) return null;
    let u = this.urls.get(b);
    if (!u) {
      u = (async () => {
        const sh = await this.blobShard(h, Math.floor(b / h.bs));
        const j = b % h.bs;
        const bytes = await new Promise<Uint8Array | null>((res) => this.want({ pack: this.packOf(h, b), off: sh.off[j], len: sh.len[j], res }));
        return bytes ? URL.createObjectURL(new Blob([bytes as BlobPart], { type: "image/avif" })) : null;
      })().catch(() => null);
      this.urls.set(b, u);
    }
    return u;
  }

  private want(w: Want) {
    this.queue.push(w);
    this.timer ??= setTimeout(() => { this.timer = null; const q = this.queue; this.queue = []; void this.flush(q); }, WAIT_MS);
  }

  private async flush(q: Want[]) {
    const byPack = new Map<number, Want[]>();
    for (const w of q) (byPack.get(w.pack) ?? byPack.set(w.pack, []).get(w.pack)!).push(w);
    await Promise.all([...byPack].map(async ([pack, ws]) => {
      ws.sort((a, b) => a.off - b.off);
      const groups: Want[][] = [];
      for (const w of ws) {
        const g = groups[groups.length - 1];
        const last = g && g[g.length - 1];
        if (g && w.off - (last.off + last.len) <= GAP) g.push(w); else groups.push([w]);
      }
      await Promise.all(groups.map((g) => this.fetchGroup(pack, g)));
    }));
  }

  private async fetchGroup(pack: number, g: Want[]) {
    const a = g[0].off, z = g[g.length - 1].off + g[g.length - 1].len - 1;
    const payload = g.reduce((s, w) => s + w.len, 0);
    try {
      let data: Uint8Array;
      let base = a;
      if (this.stats.rangeSupported === false) {
        data = await this.wholePack(pack);
        base = 0;
      } else {
        const r = await fetch(this.packUrl(pack), { headers: { Range: `bytes=${a}-${z}` } });
        if (r.status === 206) {
          data = new Uint8Array(await r.arrayBuffer());
          this.stats.rangeSupported = true;
          this.stats.requests++;
          this.stats.bytes += data.length;
          bytesFetched.total += data.length;
          bytesFetched.requests++;
        } else if (r.status === 200) { // server ignored Range: we already paid for the whole pack, keep it
          this.stats.rangeSupported = false;
          this.stats.fullPackFallbacks++;
          data = new Uint8Array(await r.arrayBuffer());
          this.whole.set(pack, Promise.resolve(data));
          this.stats.requests++;
          this.stats.bytes += data.length;
          bytesFetched.total += data.length;
          bytesFetched.requests++;
          base = 0;
        } else throw new Error(`thumb pack ${pack}: ${r.status}`);
      }
      this.stats.payloadBytes += payload;
      for (const w of g) { this.stats.images++; w.res(data.subarray(w.off - base, w.off - base + w.len)); }
    } catch {
      for (const w of g) w.res(null);
    }
  }

  private wholePack(pack: number): Promise<Uint8Array> {
    let p = this.whole.get(pack);
    if (!p) {
      p = (async () => {
        const r = await fetch(this.packUrl(pack));
        const d = new Uint8Array(await r.arrayBuffer());
        this.stats.requests++;
        this.stats.bytes += d.length;
        bytesFetched.total += d.length;
        bytesFetched.requests++;
        return d;
      })();
      this.whole.set(pack, p);
    }
    return p;
  }
}
