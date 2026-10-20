// Self-hosted AVIF thumbnails: packs of back-to-back AVIFs (build/pack_thumbs.py) fetched by HTTP Range.
// Requests issued in the same few ms are coalesced per pack (gap <= GAP bytes); if the host ignores Range (status 200) we fall back
// to downloading the whole pack once and slicing it.
import { Manifest, bytesFetched, dataBase, dataVersion, fetchBin } from "./data";

const GAP = 8192; // merge two wanted ranges when the hole between them is at most this many bytes
const WAIT_MS = 3; // coalescing window

interface Idx { worldBlob: Int32Array; blobOff: Uint32Array; blobLen: Uint16Array; blobPack: Uint16Array; packBytes: Uint32Array }
interface Want { pack: number; off: number; len: number; res: (b: Uint8Array | null) => void }

export interface ThumbStats { requests: number; bytes: number; payloadBytes: number; images: number; rangeSupported: boolean | null; fullPackFallbacks: number }

export class Thumbs {
  stats: ThumbStats = { requests: 0, bytes: 0, payloadBytes: 0, images: 0, rangeSupported: null, fullPackFallbacks: 0 };
  private idx: Promise<Idx | null> | null = null;
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

  private loadIndex(): Promise<Idx | null> {
    return (this.idx ??= (async () => {
      try {
        const buf = await fetchBin(this.m.thumbs!.index);
        const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
        if (String.fromCharCode(...buf.subarray(0, 4)) !== "THM1") return null;
        const n = dv.getUint32(4, true), nb = dv.getUint32(8, true), np = dv.getUint32(12, true);
        let p = 16;
        const first: number[] = [], pbytes = new Uint32Array(np);
        for (let k = 0; k < np; k++) { first.push(dv.getUint32(p, true)); pbytes[k] = dv.getUint32(p + 4, true); p += 8; }
        const code = new Uint32Array(n);
        for (let b = 0; b < 4; b++) for (let i = 0; i < n; i++) code[i] |= buf[p + b * n + i] << (8 * b);
        p += 4 * n;
        const blobLen = new Uint16Array(nb);
        for (let i = 0; i < nb; i++) blobLen[i] = buf[p + i] | (buf[p + nb + i] << 8);
        const worldBlob = new Int32Array(n);
        let max = -1;
        for (let i = 0; i < n; i++) {
          const c = code[i];
          if (c === 0) worldBlob[i] = ++max;
          else if (c === 1) worldBlob[i] = -1;
          else worldBlob[i] = max - (c - 2);
        }
        const blobOff = new Uint32Array(nb), blobPack = new Uint16Array(nb);
        for (let k = 0; k < np; k++) {
          const end = k + 1 < np ? first[k + 1] : nb;
          let off = 0;
          for (let b = first[k]; b < end; b++) { blobPack[b] = k; blobOff[b] = off; off += blobLen[b]; }
        }
        return { worldBlob, blobOff, blobLen, blobPack, packBytes: pbytes };
      } catch {
        return null;
      }
    })());
  }

  /** object URL for world ordinal i, or null */
  async url(i: number): Promise<string | null> {
    const idx = await this.loadIndex();
    if (!idx) return null;
    const b = idx.worldBlob[i];
    if (b === undefined || b < 0) return null;
    let u = this.urls.get(b);
    if (!u) {
      u = new Promise<Uint8Array | null>((res) => this.want({ pack: idx.blobPack[b], off: idx.blobOff[b], len: idx.blobLen[b], res }))
        .then((bytes) => (bytes ? URL.createObjectURL(new Blob([bytes as BlobPart], { type: "image/avif" })) : null));
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
