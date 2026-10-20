import {
  Core, Manifest, TITLE_MARK, authorWorlds, fetchJson, forEachPosting, getShard, idToIdx, loadCore,
  loadManifest, lowerBound, loadRare, Rare, worldInfo, similarIdx, cached,
} from "./data";
import { Tok, titleKey, tokens } from "./tok";

export interface Filters {
  cap_min?: number; cap_max?: number; size_max_mb?: number;
  platform?: "pc" | "quest" | "both" | "";
  after?: string; before?: string; tags?: string[]; author?: string;
}
export type Sort = "relevance" | "visits" | "favorites" | "updated" | "published";
export interface QueryIn { q?: string; filters?: Filters; sort?: Sort; limit?: number; exactTail?: boolean }
export interface QueryOut { idx: number[]; total: number; facetCounts: FacetCounts; ms: number }
export interface FacetCounts {
  platform: { pc: number; quest: number; both: number };
  tags: Record<string, number>;
  capacity: Record<string, number>;
}

const EPOCH = Date.UTC(2010, 0, 1);
const dayOf = (iso: string) => Math.floor((Date.parse(iso + "T00:00:00Z") - EPOCH) / 86400000);
export const dateOfDay = (d: number) => new Date(EPOCH + d * 86400000).toISOString().slice(0, 10);
const NONE = 0xffffffff;
const W_TITLE = 10, W_AUTHOR = 6, W_TAG = 6, W_DESC = 1;

export class Engine {
  m!: Manifest;
  c!: Core;
  tagKeys: string[] = [];
  tagCountsAll: number[] = [];
  tagId = new Map<string, number>();
  stop = new Set<string>();
  prior!: Float32Array;
  private sc!: Float32Array;
  private cnt!: Uint8Array;
  private tw!: Float32Array;
  private mask!: Uint8Array;
  allTags: Promise<[string, number][]> | null = null;
  rare: Promise<{ r: Rare; keys: string[]; id: Map<string, number> }> | null = null;
  defaultFacets!: FacetCounts;

  async init() {
    this.m = await loadManifest();
    const [c, tags] = await Promise.all([loadCore(this.m), fetchJson<[string, number][]>("tags_core.bin")]);
    this.c = c;
    tags.forEach(([k, n], i) => { this.tagKeys.push(k); this.tagCountsAll.push(n); this.tagId.set(k, i); });
    this.stop = new Set(this.m.stop);
    const n = c.n;
    this.prior = new Float32Array(n);
    for (let i = 0; i < n; i++) this.prior[i] = 0.45 * Math.log10(1 + c.visits[i]) + 0.25 * Math.log10(1 + c.fav[i]);
    this.sc = new Float32Array(n);
    this.cnt = new Uint8Array(n);
    this.tw = new Float32Array(n);
    this.mask = new Uint8Array(n);
    return this;
  }

  ensureRare() {
    return (this.rare ??= (async () => {
      const [r, all] = await Promise.all([loadRare(this.c.n), this.tagsAll()]);
      const id = new Map<string, number>();
      all.forEach(([k], i) => id.set(k, i));
      return { r, keys: all.map((x) => x[0]), id };
    })());
  }

  tagsAll() {
    return (this.allTags ??= fetchJson<[string, number][]>("tags_all.bin"));
  }

  /** accept canonical keys plus raw author_tag_ spelling */
  resolveTag(t: string): string {
    t = t.trim();
    return t.startsWith("author_tag_") ? t.slice(11) : t;
  }

  private async filterMask(f: Filters): Promise<{ mask: Uint8Array; any: boolean }> {
    const c = this.c, n = c.n, mask = this.mask;
    const capLo = f.cap_min ?? -1, capHi = f.cap_max ?? 1e9;
    const useCap = f.cap_min !== undefined || f.cap_max !== undefined;
    const lim = f.size_max_mb !== undefined ? Math.floor(f.size_max_mb * (1048576 / this.m.sizeUnit) + 1e-6) : -1;
    const plat = f.platform || "";
    const dLo = f.after ? dayOf(f.after) : -1, dHi = f.before ? dayOf(f.before) : 1e9;
    const useDate = !!(f.after || f.before);
    // tags
    let flagReq = 0;
    const csrReq: number[] = [];
    const rareMasks: Uint8Array[] = [];
    for (const raw of f.tags ?? []) {
      const key = this.resolveTag(raw);
      const id = this.tagId.get(key);
      if (id === undefined) {
        const { r, id: aid } = await this.ensureRare();
        const rid = aid.get(key);
        const mk = new Uint8Array(n);
        if (rid !== undefined) for (let i = 0; i < n; i++) for (let j = r.off[i]; j < r.off[i + 1]; j++) if (r.ids[j] === rid) { mk[i] = 1; break; }
        rareMasks.push(mk);
      } else if (id < this.m.flagTags) flagReq |= 1 << id;
      else csrReq.push(id);
    }
    let authorMask: Uint8Array | null = null;
    if (f.author) {
      authorMask = new Uint8Array(n);
      for (const i of await authorWorlds(f.author)) authorMask[i] = 1;
    }
    const any = useCap || lim >= 0 || !!plat || useDate || flagReq > 0 || csrReq.length > 0 || rareMasks.length > 0 || !!authorMask;
    if (!any) { mask.fill(1); return { mask, any }; }
    const { cap, pc, quest, plat: pl, updated, flags, tagOff, tagIds } = c;
    for (let i = 0; i < n; i++) {
      let ok = true;
      if (useCap && (cap[i] < capLo || cap[i] > capHi)) ok = false;
      else if (plat) {
        const p = pl[i];
        if (plat === "pc" ? !(p & 1) : plat === "quest" ? !(p & 2) : p !== 3) ok = false;
      }
      if (ok && lim >= 0) {
        if (plat === "pc") ok = pc[i] <= lim;
        else if (plat === "quest") ok = quest[i] <= lim;
        else if (plat === "both") ok = pc[i] <= lim && quest[i] <= lim;
        else ok = pc[i] <= lim || quest[i] <= lim;
      }
      if (ok && useDate && (updated[i] < dLo || updated[i] > dHi)) ok = false;
      if (ok && flagReq && (flags[i] & flagReq) !== flagReq) ok = false;
      if (ok && csrReq.length) {
        for (const id of csrReq) {
          let found = false;
          for (let j = tagOff[i]; j < tagOff[i + 1]; j++) if (tagIds[j] === id) { found = true; break; }
          if (!found) { ok = false; break; }
        }
      }
      if (ok && authorMask && !authorMask[i]) ok = false;
      if (ok) for (const rm of rareMasks) if (!rm[i]) { ok = false; break; }
      mask[i] = ok ? 1 : 0;
    }
    return { mask, any };
  }

  /** full-text: fills this.sc / this.cnt; returns required token count (0 = no text constraint) */
  private async textMatch(q: string): Promise<number> {
    let toks = tokens(q);
    if (!toks.length) return 0;
    const nonStop = toks.filter((t) => !this.stop.has(t.term));
    if (nonStop.length) toks = nonStop;
    // dedupe
    const seen = new Set<string>();
    toks = toks.filter((t) => (seen.has(t.term) ? false : (seen.add(t.term), true)));
    const n = this.c.n;
    this.sc.fill(0);
    this.cnt.fill(0);
    const endsOpen = !/\s$/.test(q); // trailing space => finished word
    const lastIdx = toks.length - 1;
    const lastIsWord = toks[lastIdx].word && endsOpen && Array.from(toks[lastIdx].term).length >= 2;
    const shards = await Promise.all(toks.map((t) => getShard(this.m, t.term)));
    const tk = titleKey(q);
    const titleShard = tk ? await getShard(this.m, TITLE_MARK + tk) : null;
    const tw = this.tw;
    toks.forEach((t: Tok, ti) => {
      const sh = shards[ti];
      tw.fill(0);
      const prefix = lastIsWord && ti === lastIdx;
      let i = lowerBound(sh.terms, t.term);
      let guard = 0;
      for (; i < sh.terms.length; i++) {
        const term = sh.terms[i];
        const exact = term === t.term;
        if (!exact && !(prefix && term.startsWith(t.term))) break;
        if (++guard > 3000) break;
        const f = exact ? 1 : 0.6;
        forEachPosting(sh, i, (d, mk) => {
          const w = ((mk & 1 ? W_TITLE : 0) + (mk & 2 ? W_AUTHOR : 0) + (mk & 4 ? W_TAG : 0) + (mk & 8 ? W_DESC : 0)) * f;
          if (w > tw[d]) tw[d] = w;
        });
      }
      for (let d = 0; d < n; d++) if (tw[d] > 0) { this.sc[d] += tw[d]; this.cnt[d]++; }
    });
    if (titleShard && tk) {
      const key = TITLE_MARK + tk;
      let i = lowerBound(titleShard.terms, key);
      let guard = 0;
      for (; i < titleShard.terms.length && titleShard.terms[i].startsWith(key) && guard < 5000; i++, guard++) {
        const bonus = titleShard.terms[i] === key ? 1000 : 300;
        forEachPosting(titleShard, i, (d) => { this.sc[d] += bonus; });
      }
    }
    return toks.length;
  }

  async run(inp: QueryIn): Promise<QueryOut> {
    const t0 = performance.now();
    const f = inp.filters ?? {};
    const sort: Sort = inp.sort ?? "relevance";
    const limit = Math.max(0, inp.limit ?? 50);
    const need = (inp.q ?? "").trim() ? await this.textMatch(inp.q!) : 0;
    const { mask } = await this.filterMask(f);
    const c = this.c, n = c.n, cnt = this.cnt, sc = this.sc;
    let total = 0;
    // matched set (mask &= text)
    if (need) for (let i = 0; i < n; i++) { if (mask[i] && cnt[i] !== need) mask[i] = 0; }
    // facets
    const nf = this.m.flagTags, nt = this.tagKeys.length;
    const tagCnt = new Uint32Array(nt);
    const capB = [0, 0, 0, 0, 0];
    let pcN = 0, qN = 0, bothN = 0;
    const { plat, flags, tagOff, tagIds, cap } = c;
    for (let i = 0; i < n; i++) {
      if (!mask[i]) continue;
      total++;
      const p = plat[i];
      if (p & 1) pcN++;
      if (p & 2) qN++;
      if (p === 3) bothN++;
      const fl = flags[i];
      if (fl) for (let b = 0; b < nf; b++) if (fl & (1 << b)) tagCnt[b]++;
      for (let j = tagOff[i]; j < tagOff[i + 1]; j++) tagCnt[tagIds[j]]++;
      const cp = cap[i];
      if (cp >= 1) capB[cp <= 8 ? 0 : cp <= 16 ? 1 : cp <= 32 ? 2 : cp <= 64 ? 3 : 4]++;
    }
    const order = Array.from({ length: nt }, (_, i) => i).filter((i) => tagCnt[i] > 0)
      .sort((a, b) => tagCnt[b] - tagCnt[a] || (this.tagKeys[a] < this.tagKeys[b] ? -1 : 1)).slice(0, 60);
    let tagEntries: [string, number][] = order.map((i) => [this.tagKeys[i], tagCnt[i]]);
    if (inp.exactTail !== false && total > 0 && (tagEntries.length < 20 || tagEntries[19][1] < this.m.minCoreTag)) {
      // the 20th core count is below the vocabulary cutoff: rare tags could tie/overtake, count them exactly
      const { r, keys } = await this.ensureRare();
      const extra = new Map<number, number>();
      for (let i = 0; i < n; i++) if (mask[i]) for (let j = r.off[i]; j < r.off[i + 1]; j++) extra.set(r.ids[j], (extra.get(r.ids[j]) ?? 0) + 1);
      for (const [id, v] of extra) tagEntries.push([keys[id], v]);
      tagEntries.sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1));
      tagEntries = tagEntries.slice(0, 60);
    }
    const tags: Record<string, number> = {};
    for (const [k, v] of tagEntries) tags[k] = v;
    const capacity: Record<string, number> = {};
    ["1-8", "9-16", "17-32", "33-64", "65+"].forEach((k, i) => { if (capB[i]) capacity[k] = capB[i]; });
    const facetCounts: FacetCounts = { platform: { pc: pcN, quest: qN, both: bothN }, tags, capacity };
    // top-k
    const idx = this.topK(sort, need > 0, limit, total);
    return { idx, total, facetCounts, ms: performance.now() - t0 };
  }

  private topK(sort: Sort, hasText: boolean, k: number, total: number): number[] {
    const { mask, c, sc, prior } = this as any as { mask: Uint8Array; c: Core; sc: Float32Array; prior: Float32Array };
    const n = c.n;
    k = Math.min(k, total);
    if (!k) return [];
    const byPop = sort === "visits" || (sort === "relevance" && !hasText);
    if (byPop) { // index order is visits desc
      const out: number[] = [];
      for (let i = 0; i < n && out.length < k; i++) if (mask[i]) out.push(i);
      return out;
    }
    let key: (i: number) => number;
    if (sort === "relevance") key = (i) => sc[i] + prior[i];
    else if (sort === "favorites") key = (i) => c.fav[i] * 1e-3 + 0; // refined below
    else if (sort === "updated") key = (i) => c.updated[i];
    else key = (i) => c.created[i];
    const favSort = sort === "favorites";
    const better = (a: number, b: number) => {
      const ka = favSort ? c.fav[a] : key(a), kb = favSort ? c.fav[b] : key(b);
      return ka > kb || (ka === kb && a < b);
    };
    const buf: number[] = [];
    for (let i = 0; i < n; i++) {
      if (!mask[i]) continue;
      if (buf.length === k && !better(i, buf[k - 1])) continue;
      let j = buf.length < k ? buf.length : k - 1;
      if (buf.length < k) buf.push(i);
      while (j > 0 && better(i, buf[j - 1])) { buf[j] = buf[j - 1]; j--; }
      buf[j] = i;
    }
    return buf;
  }

  async similar(idx: number, k: number) { return similarIdx(idx, k); }
  idToIdx = idToIdx;
  info(idx: number[]) { return worldInfo(this.m, idx); }
  cacheKey = cached;
}
