// Opt-in in-browser semantic search: static-similarity-mrl-multilingual-v1 (256-d, int8 token table),
// BERT WordPiece tokenizer, mean pooling incl. [CLS]/[SEP], 256-bit sign vectors + Hamming top-500 + float rescoring.
import { fetchBin, fetchJson } from "./data";

const DIM = 256;

function isCjkIdeo(cp: number) {
  return (cp >= 0x4e00 && cp <= 0x9fff) || (cp >= 0x3400 && cp <= 0x4dbf) || (cp >= 0x20000 && cp <= 0x2a6df) ||
    (cp >= 0x2a700 && cp <= 0x2b73f) || (cp >= 0x2b740 && cp <= 0x2b81f) || (cp >= 0x2b820 && cp <= 0x2ceaf) ||
    (cp >= 0xf900 && cp <= 0xfaff) || (cp >= 0x2f800 && cp <= 0x2fa1f);
}
const isPunct = (ch: string, cp: number) =>
  (cp >= 33 && cp <= 47) || (cp >= 58 && cp <= 64) || (cp >= 91 && cp <= 96) || (cp >= 123 && cp <= 126) || /\p{P}/u.test(ch);

function f16(h: number): number {
  const s = h & 0x8000 ? -1 : 1, e = (h >> 10) & 31, m = h & 1023;
  if (e === 0) return s * m * 2 ** -24;
  if (e === 31) return m ? NaN : s * Infinity;
  return s * (1 + m / 1024) * 2 ** (e - 15);
}

export class Sem {
  vocab = new Map<string, number>();
  table!: Int8Array;
  scale!: Float32Array;
  vecs!: Uint32Array; // n * 8 words
  n = 0;
  rowsPer = 0;

  static async load(onProgress?: (done: number, total: number) => void): Promise<Sem> {
    const s = new Sem();
    const meta = await fetchJson<{ vocab: number; chunks: number; rowsPerChunk: number; n: number }>("sem/meta.json");
    const files = ["sem/tokenizer.bin", "sem/scale.bin", "sem/vecs.bin", ...Array.from({ length: meta.chunks }, (_, i) => `sem/tokens.${i}.bin`)];
    let done = 0;
    const got = await Promise.all(files.map(async (f) => { const b = await fetchBin(f); onProgress?.(++done, files.length); return b; }));
    const tk = JSON.parse(new TextDecoder().decode(got[0]));
    for (const [t, id] of Object.entries<number>(tk.model.vocab)) s.vocab.set(t, id);
    const sc = new Uint16Array(got[1].buffer.slice(got[1].byteOffset, got[1].byteOffset + got[1].length));
    s.scale = new Float32Array(sc.length);
    for (let i = 0; i < sc.length; i++) s.scale[i] = f16(sc[i]);
    s.vecs = new Uint32Array(got[2].buffer.slice(got[2].byteOffset, got[2].byteOffset + got[2].length));
    s.n = meta.n;
    s.rowsPer = meta.rowsPerChunk;
    const total = got.slice(3).reduce((a, b) => a + b.length, 0);
    s.table = new Int8Array(total);
    let off = 0;
    for (const c of got.slice(3)) { s.table.set(new Int8Array(c.buffer, c.byteOffset, c.length), off); off += c.length; }
    return s;
  }

  /** BertNormalizer(clean, chinese chars, strip accents, lowercase) + BertPreTokenizer + WordPiece, with [CLS]/[SEP] */
  tokenize(text: string): number[] {
    let o = "";
    for (const ch of text) {
      const cp = ch.codePointAt(0)!;
      if (cp === 0 || cp === 0xfffd) continue;
      if (/\s/u.test(ch)) { o += " "; continue; }
      if (/[\p{Cc}\p{Cf}]/u.test(ch)) continue;
      o += isCjkIdeo(cp) ? ` ${ch} ` : ch;
    }
    o = o.normalize("NFD").replace(/\p{Mn}/gu, "").toLowerCase();
    const ids = [101];
    for (const word of o.split(/\s+/).filter(Boolean)) {
      // split punctuation into own tokens
      const parts: string[] = [];
      let cur = "";
      for (const ch of word) {
        if (isPunct(ch, ch.codePointAt(0)!)) { if (cur) parts.push(cur); parts.push(ch); cur = ""; } else cur += ch;
      }
      if (cur) parts.push(cur);
      for (const p of parts) ids.push(...this.wordpiece(p));
    }
    ids.push(102);
    return ids;
  }

  private wordpiece(w: string): number[] {
    const cs = Array.from(w);
    if (cs.length > 100) return [100];
    const out: number[] = [];
    let start = 0;
    while (start < cs.length) {
      let end = cs.length, found = -1;
      while (start < end) {
        const sub = (start > 0 ? "##" : "") + cs.slice(start, end).join("");
        const id = this.vocab.get(sub);
        if (id !== undefined) { found = id; break; }
        end--;
      }
      if (found < 0) return [100];
      out.push(found);
      start = end;
    }
    return out;
  }

  encode(q: string): Float32Array {
    const ids = this.tokenize(q);
    const v = new Float32Array(DIM);
    for (const id of ids) {
      const sc = this.scale[id], base = id * DIM;
      for (let d = 0; d < DIM; d++) v[d] += this.table[base + d] * sc;
    }
    let nn = 0;
    for (let d = 0; d < DIM; d++) { v[d] /= ids.length; nn += v[d] * v[d]; }
    nn = Math.sqrt(nn) || 1;
    for (let d = 0; d < DIM; d++) v[d] /= nn;
    return v;
  }

  /** returns world indices, best first. allow: optional filter mask (1 = eligible). */
  search(q: string, k: number, allow?: Uint8Array | null): number[] {
    const v = this.encode(q);
    const qb = new Uint8Array(32);
    for (let d = 0; d < DIM; d++) if (v[d] > 0) qb[d >> 3] |= 0x80 >> (d & 7);
    const qw = new Uint32Array(qb.buffer);
    const n = this.n, V = this.vecs;
    const dist = new Uint16Array(n).fill(999);
    const hist = new Uint32Array(300);
    for (let i = 0; i < n; i++) {
      if (allow && !allow[i]) continue;
      let c = 0;
      const b = i * 8;
      for (let j = 0; j < 8; j++) {
        let x = (V[b + j] ^ qw[j]) >>> 0;
        x = x - ((x >>> 1) & 0x55555555);
        x = (x & 0x33333333) + ((x >>> 2) & 0x33333333);
        c += (((x + (x >>> 4)) & 0x0f0f0f0f) * 0x01010101) >>> 24;
      }
      dist[i] = c;
      hist[c]++;
    }
    const M = Math.max(500, k * 5);
    let cut = 0, acc = 0;
    while (cut < 257 && acc + hist[cut] < M) acc += hist[cut++];
    const cand: [number, number][] = [];
    const bytes = new Uint8Array(V.buffer);
    for (let i = 0; i < n; i++) {
      if (dist[i] > cut) continue;
      let s = 0;
      const b = i * 32;
      for (let d = 0; d < DIM; d++) s += bytes[b + (d >> 3)] & (0x80 >> (d & 7)) ? v[d] : -v[d];
      cand.push([s, i]);
    }
    cand.sort((a, b) => b[0] - a[0] || a[1] - b[1]);
    return cand.slice(0, k).map((c) => c[1]);
  }
}
