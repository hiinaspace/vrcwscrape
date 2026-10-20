import { TITLE_MARK, shardOf } from "./tok";

export interface Manifest {
  version: number;
  n: number;
  core: { file: string; columns: { name: string; dtype: string; len: number; off: number; bytes: number; kind: string }[] };
  ftsShards: number;
  stop: string[];
  metaChunk: number;
  cardChunk: number;
  nCoreTags: number;
  cw: [string, number][];
  minCoreTag: number;
  flagTags: number;
  sizeUnit: number;
  thumb: { prefix: string; suffix: string } | null;
  hasThumb: boolean;
}

export interface Core {
  n: number;
  cap: Uint8Array;
  pc: Uint32Array; // KB (ceil); 0xFFFFFFFF = none
  quest: Uint32Array;
  created: Uint16Array; // days since 2010-01-01
  updated: Uint16Array;
  visits: Uint32Array;
  fav: Uint32Array;
  plat: Uint8Array; // bit0 pc, bit1 quest
  flags: Uint8Array; // top flagTags tags as bits
  tagOff: Uint32Array; // CSR offsets (n+1)
  tagIds: Uint16Array;
}

export interface Meta { id: string[]; n: string[]; an: string[]; au: string[]; th?: string[] }
export interface Card { d: string[]; t: string[][]; hp: number[][] }

export const bytesFetched = { total: 0, requests: 0 };

const base = new URL("data/", document.baseURI);
let ver = "";

export async function fetchBin(rel: string): Promise<Uint8Array> {
  const r = await fetch(new URL(rel, base) + (ver ? "?v=" + ver : ""));
  if (!r.ok) throw new Error(`${rel}: ${r.status}`);
  const buf = new Uint8Array(await r.arrayBuffer());
  bytesFetched.total += buf.length;
  bytesFetched.requests++;
  if (buf[0] === 0x1f && buf[1] === 0x8b) {
    const ds = new Response(new Blob([buf]).stream().pipeThrough(new DecompressionStream("gzip")));
    return new Uint8Array(await ds.arrayBuffer());
  }
  return buf;
}

const jsonDec = new TextDecoder();
export async function fetchJson<T>(rel: string): Promise<T> {
  return JSON.parse(jsonDec.decode(await fetchBin(rel)));
}

const cache = new Map<string, Promise<unknown>>();
export function cached<T>(key: string, f: () => Promise<T>): Promise<T> {
  let p = cache.get(key) as Promise<T> | undefined;
  if (!p) {
    p = f();
    p.catch(() => cache.delete(key));
    cache.set(key, p);
  }
  return p;
}

export async function loadManifest(): Promise<Manifest> {
  const r = await fetch(new URL("manifest.json", base), { cache: "no-cache" });
  const m: Manifest = await r.json();
  ver = String(m.version);
  return m;
}

function unplane(src: Uint8Array, off: number, len: number, size: number, ctor: any) {
  const out = new ctor(len);
  const o8 = new Uint8Array(out.buffer);
  for (let b = 0; b < size; b++) {
    const s = off + b * len;
    for (let i = 0; i < len; i++) o8[i * size + b] = src[s + i];
  }
  return out;
}

export async function loadCore(m: Manifest): Promise<Core> {
  const raw = await fetchBin(m.core.file);
  const col: Record<string, any> = {};
  for (const c of m.core.columns) {
    if (c.dtype === "uint8") col[c.name] = raw.slice(c.off, c.off + c.len);
    else if (c.dtype === "uint16") col[c.name] = unplane(raw, c.off, c.len, 2, Uint16Array);
    else col[c.name] = unplane(raw, c.off, c.len, 4, Uint32Array);
  }
  const n = m.n;
  const d: Uint32Array = col.visits_delta;
  const visits = new Uint32Array(n);
  if (n) visits[0] = d[0];
  for (let i = 1; i < n; i++) visits[i] = visits[i - 1] - d[i];
  const tagOff = new Uint32Array(n + 1);
  const tc: Uint8Array = col.tagcnt;
  for (let i = 0; i < n; i++) tagOff[i + 1] = tagOff[i] + tc[i];
  return {
    n, cap: col.cap, pc: col.pc, quest: col.quest, created: col.created, updated: col.updated,
    visits, fav: col.fav, plat: col.plat, flags: col.flags, tagOff, tagIds: col.tagids,
  };
}

// ---- chunks
export function metaChunk(m: Manifest, k: number): Promise<Meta> {
  return cached("meta" + k, () => fetchJson<Meta>(`meta/${k}.bin`));
}
export function cardChunk(k: number): Promise<Card> {
  return cached("card" + k, () => fetchJson<Card>(`card/${k}.bin`));
}

export async function metaFor(m: Manifest, idx: number[]): Promise<Meta[]> {
  const ks = [...new Set(idx.map((i) => Math.floor(i / m.metaChunk)))];
  await Promise.all(ks.map((k) => metaChunk(m, k)));
  return idx.map((i) => cache.get("meta" + Math.floor(i / m.metaChunk)) as any);
}

export interface WorldInfo { id: string; name: string; author: string; authorId: string; thumb: string }
export async function worldInfo(m: Manifest, idxs: number[]): Promise<WorldInfo[]> {
  const ks = [...new Set(idxs.map((i) => Math.floor(i / m.metaChunk)))];
  const chunks = new Map<number, Meta>();
  await Promise.all(ks.map(async (k) => chunks.set(k, await metaChunk(m, k))));
  return idxs.map((i) => {
    const c = chunks.get(Math.floor(i / m.metaChunk))!;
    const j = i % m.metaChunk;
    const th = c.th?.[j];
    return {
      id: "wrld_" + c.id[j], name: c.n[j], author: c.an[j], authorId: "usr_" + c.au[j],
      thumb: th && m.thumb ? m.thumb.prefix + th + m.thumb.suffix : "",
    };
  });
}

export async function idToIdx(id: string): Promise<number | undefined> {
  const w = id.replace(/^wrld_/, "");
  if (w.length < 2) return undefined;
  const m = await cached("ids" + w.slice(0, 2), () => fetchJson<Record<string, number>>(`ids/${w.slice(0, 2)}.bin`).catch(() => ({} as Record<string, number>)));
  return m[w];
}

export async function authorWorlds(authorId: string): Promise<number[]> {
  const a = authorId.replace(/^usr_/, "");
  const m = await cached("au" + a.slice(0, 2), () => fetchJson<Record<string, number[]>>(`authors/${a.slice(0, 2)}.bin`).catch(() => ({} as Record<string, number[]>)));
  return m[a] ?? [];
}

export interface Rare { off: Uint32Array; ids: Uint16Array }
export async function loadRare(n: number): Promise<Rare> {
  const raw = await fetchBin("tags_rare.bin");
  const off = new Uint32Array(n + 1);
  for (let i = 0; i < n; i++) off[i + 1] = off[i] + raw[i];
  const len = (raw.length - n) >> 1;
  return { off, ids: unplane(raw, n, len, 2, Uint16Array) };
}

// ---- fts shards
export interface Shard { terms: string[]; counts: Uint32Array; lens: Uint32Array; offs: Uint32Array; blob: Uint8Array }

function parseShard(buf: Uint8Array): Shard {
  let p = 0;
  const vi = () => {
    let r = 0, s = 1, b: number;
    do { b = buf[p++]; r += (b & 127) * s; s *= 128; } while (b & 128);
    return r;
  };
  const nt = vi();
  const tl = new Uint32Array(nt);
  for (let i = 0; i < nt; i++) tl[i] = vi();
  const dec = new TextDecoder();
  const terms = new Array<string>(nt);
  for (let i = 0; i < nt; i++) { terms[i] = dec.decode(buf.subarray(p, p + tl[i])); p += tl[i]; }
  const counts = new Uint32Array(nt), lens = new Uint32Array(nt), offs = new Uint32Array(nt);
  for (let i = 0; i < nt; i++) counts[i] = vi();
  let tot = 0;
  for (let i = 0; i < nt; i++) { lens[i] = vi(); offs[i] = tot; tot += lens[i]; }
  return { terms, counts, lens, offs, blob: buf.subarray(p) };
}

export function getShard(m: Manifest, term: string): Promise<Shard> {
  const s = shardOf(term, m.ftsShards);
  return cached("fts" + s, async () => parseShard(await fetchBin(`fts/${s}.bin`)));
}

export function lowerBound(terms: string[], t: string): number {
  let lo = 0, hi = terms.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (terms[mid] < t) lo = mid + 1; else hi = mid;
  }
  return lo;
}

/** calls fn(doc, mask) for each posting of term index i */
export function forEachPosting(sh: Shard, i: number, fn: (doc: number, mask: number) => void) {
  const b = sh.blob;
  let p = sh.offs[i];
  const end = p + sh.lens[i];
  let doc = 0;
  while (p < end) {
    let r = 0, s = 1, c: number;
    do { c = b[p++]; r += (c & 127) * s; s *= 128; } while (c & 128);
    doc += Math.floor(r / 16);
    fn(doc, r & 15);
  }
}

export { TITLE_MARK };

// ---- similar
let simIndex: Promise<{ k: number; shard: number; shards: number[] } | null> | null = null;
export async function similarIdx(idx: number, k: number): Promise<number[]> {
  simIndex ??= fetchJson<any>("sim/index.json").catch(() => null);
  const si = await simIndex;
  if (!si) return [];
  const sn = Math.floor(idx / si.shard);
  if (!si.shards.includes(sn)) return [];
  const buf = await cached("sim" + sn, () => fetchBin(`sim/${sn}.bin`));
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const mcount = dv.getUint32(0, true);
  let lo = 0, hi = mcount - 1, row = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const v = dv.getUint32(4 + mid * 4, true);
    if (v === idx) { row = mid; break; }
    if (v < idx) lo = mid + 1; else hi = mid - 1;
  }
  if (row < 0) return [];
  const planesStart = 4 + mcount * 4;
  const total = mcount * si.k;
  const out: number[] = [];
  for (let j = 0; j < Math.min(k, si.k); j++) {
    const e = row * si.k + j;
    out.push((buf[planesStart + e] | (buf[planesStart + total + e] << 8) | (buf[planesStart + 2 * total + e] << 16) | (buf[planesStart + 3 * total + e] << 24)) >>> 0);
  }
  return out;
}

export async function loadSizeLo(n: number): Promise<{ pc: Uint16Array; quest: Uint16Array }> {
  const raw = await fetchBin("sizelo.bin");
  return { pc: unplane(raw, 0, n, 2, Uint16Array), quest: unplane(raw, 2 * n, n, 2, Uint16Array) };
}
