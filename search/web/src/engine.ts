import {
  Core, Manifest, TITLE_MARK, authorWorlds, fetchJson, forEachPosting, getShard, idToIdx, loadCore,
  loadManifest, lowerBound, loadRare, loadSizeLo, Rare, worldInfo, similarIdx, cached,
} from "./data";
import { AUTHOR_MARK, PAIR_MARK, TAG_MARK, Tok, norm, pieces, titleKey, tokens } from "./tok";

import { ParseResult, parseQuery } from "./query";

export interface Filters {
  cap_min?: number; cap_max?: number; size_max_mb?: number;
  platform?: "pc" | "quest" | "both" | "";
  /** exclude worlds that have this platform */
  platform_ex?: "pc" | "quest" | "";
  /** UPDATED date range, inclusive calendar days (oracle contract) */
  after?: string; before?: string;
  /** PUBLISHED (created) date range, inclusive calendar days (round 5) */
  pub_after?: string; pub_before?: string;
  tags?: string[]; tags_ex?: string[]; author?: string;
  status?: string[]; status_ex?: string[]; cw?: string[]; cw_exclude?: string[];
}
export type Sort = "trending" | "relevance" | "text" | "visits" | "favorites" | "updated" | "published" | "capacity";
export interface QueryIn {
  whatIf?: boolean; q?: string; filters?: Filters; sort?: Sort;
  /** ascending instead of the default descending (ignored for trending/relevance/text) */
  asc?: boolean;
  limit?: number; offset?: number; exactTail?: boolean;
  /** skip facet counting (paging / "load more"); total is still exact */
  facets?: boolean;
  /** number of entries in facetCounts.userTagList (default 40) */
  tagLimit?: number;
}
export interface QueryOut {
  idx: number[]; total: number; facetCounts: FacetCounts; ms: number;
  /** parse of `q` (pills, errors); same object parseQuery(q) returns */
  parse: ParseResult;
  /** true when the query had a positive text condition, i.e. sort "relevance" ranked by text */
  textRanked: boolean;
}
/** month histogram: counts[k] is the number of worlds in month (m0 + k); month index = months since 2010-01 */
export interface Histo { m0: number; counts: number[] }
export interface FacetCounts {
  platform: { pc: number; quest: number; both: number };
  tags: Record<string, number>;
  capacity: Record<string, number>;
  /** extras for the UI (not part of the oracle contract) */
  status: Record<string, number>;
  cw: Record<string, number>;
  /** user-class tags, case/plural groups merged ("~group" keys), each world counted ONCE per group; top tagLimit, count desc */
  userTags: Record<string, number>;
  userTagList: [string, number][];
  /** what-if platform counts (UI): total if that platform option were selected, other filters kept */
  platformIf?: { any: number; pc: number; quest: number; both: number };
  /** published-date (created) histogram by month under all other filters + query, ignoring pub_after/pub_before */
  months: Histo;
  /** updated-date histogram by month, ignoring after/before */
  updatedMonths: Histo;
  /** worlds per capacity value (index 0..256), ignoring cap_min/cap_max */
  capacityHist: number[];
}

const HIDDEN = ["admin_", "adm_", "feature_", "debug_"];
export function tagClass(k: string): "hidden" | "status" | "cw" | "user" {
  if (k === "system_approved" || k === "system_labs") return "status";
  if (k.startsWith("system_") || HIDDEN.some((p) => k.startsWith(p))) return "hidden";
  return k.startsWith("content_") ? "cw" : "user";
}

const EPOCH = Date.UTC(2010, 0, 1);
const dayOf = (iso: string) => Math.floor((Date.parse(iso + "T00:00:00Z") - EPOCH) / 86400000);
export const dateOfDay = (d: number) => new Date(EPOCH + d * 86400000).toISOString().slice(0, 10);
/** month index = months since 2010-01 */
export const monthToDate = (m: number) => `${2010 + Math.floor(m / 12)}-${String((m % 12) + 1).padStart(2, "0")}-01`;
export const dateToMonth = (iso: string) => (+iso.slice(0, 4) - 2010) * 12 + (+iso.slice(5, 7) - 1);
const NONE = 0xffffffff;
const W_TITLE = 10, W_AUTHOR = 6, W_TAG = 9, W_DESC = 0.4;

export class Engine {
  m!: Manifest;
  c!: Core;
  tagKeys: string[] = [];
  tagCountsAll: number[] = [];
  tagId = new Map<string, number>();
  stop = new Set<string>();
  prior!: Float32Array; // multiplicative popularity prior
  lp!: Float32Array; // log popularity (also used for semantic hits)
  groupOf = new Map<string, string>();
  private sc!: Float32Array;
  private cnt!: Uint8Array;
  private tw!: Float32Array;
  private mask!: Uint8Array;
  allTags: Promise<[string, number][]> | null = null;
  sizeLo: Promise<{ pc: Uint16Array; quest: Uint16Array }> | null = null;
  rare: Promise<{ r: Rare; keys: string[]; id: Map<string, number> }> | null = null;
  defaultFacets!: FacetCounts;
  hasThumbs = false; // STUB
  trend: { basis: "recent_visits_per_day" | "popularity_fallback"; windowDays: number | null } = { basis: "popularity_fallback", windowDays: null }; // STUB
  months = { min: 0, max: 0 }; // STUB
  trendOf(_i: number): number { return NaN; } // STUB

  async init() {
    this.m = await loadManifest();
    const [c, tags] = await Promise.all([loadCore(this.m), fetchJson<[string, number][]>("tags_core.bin")]);
    this.c = c;
    tags.forEach(([k, n], i) => { this.tagKeys.push(k); this.tagCountsAll.push(n); this.tagId.set(k, i); });
    this.stop = new Set(this.m.stop);
    const n = c.n;
    this.prior = new Float32Array(n);
    this.lp = new Float32Array(n);
    for (let i = 0; i < n; i++) { this.lp[i] = Math.log10(1 + c.visits[i]) + 0.5 * Math.log10(1 + c.fav[i]); this.prior[i] = Math.exp(0.3 * this.lp[i]); }
    for (const [g, ks] of Object.entries(this.m.tagGroups ?? {})) for (const k of ks) this.groupOf.set(k, g);
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
    // size_max_mb is compared exactly: bytes <= floor(x * 2^20). K = ceil(bytes/1024) is in core; the low bits are lazy.
    let lim = -1, rem = 0;
    if (f.size_max_mb !== undefined) {
      const T = Math.floor(f.size_max_mb * 1048576);
      lim = Math.floor(T / 1024);
      rem = T - lim * 1024;
    }
    if (rem > 0) this.sizeLo ??= loadSizeLo(n);
    const lo = rem > 0 ? await this.sizeLo! : null;
    const loPc = lo?.pc, loQ = lo?.quest;
    const plat = f.platform || "";
    const dLo = f.after ? dayOf(f.after) : -1, dHi = f.before ? dayOf(f.before) : 1e9;
    const useDate = !!(f.after || f.before);
    // tags (explicit tags + status + content warnings): required (AND) and excluded
    const tagKeys = [...(f.tags ?? []), ...(f.status ?? []).map((x) => "system_" + x.replace(/^system_/, "")), ...(f.cw ?? []).map((x) => "content_" + x.replace(/^content_/, ""))];
    const exclKeys = (f.cw_exclude ?? []).map((x) => "content_" + x.replace(/^content_/, ""));
    let flagReq = 0, flagForbid = 0;
    const csrReq: number[] = [], csrForbid: number[] = [];
    const rareMasks: Uint8Array[] = [], rareForbid: Uint8Array[] = [];
    const orGroups: { fm: number; csr: number[] }[] = [];
    const resolve = async (raw: string, forbid: boolean) => {
      if (raw.startsWith("~")) { // UI tag group: any member (case/plural variants)
        const g = { fm: 0, csr: [] as number[] };
        for (const k of this.m.tagGroups?.[raw.slice(1)] ?? [raw.slice(1)]) {
          const id = this.tagId.get(k);
          if (id === undefined) continue;
          if (id < this.m.flagTags) g.fm |= 1 << id; else g.csr.push(id);
        }
        orGroups.push(g);
        return;
      }
      const key = this.resolveTag(raw);
      const id = this.tagId.get(key);
      if (id === undefined) {
        const { r, id: aid } = await this.ensureRare();
        const rid = aid.get(key);
        const mk = new Uint8Array(n);
        if (rid !== undefined) for (let i = 0; i < n; i++) for (let j = r.off[i]; j < r.off[i + 1]; j++) if (r.ids[j] === rid) { mk[i] = 1; break; }
        (forbid ? rareForbid : rareMasks).push(mk);
      } else if (id < this.m.flagTags) { if (forbid) flagForbid |= 1 << id; else flagReq |= 1 << id; }
      else (forbid ? csrForbid : csrReq).push(id);
    };
    for (const t of tagKeys) await resolve(t, false);
    for (const t of exclKeys) await resolve(t, true);
    let authorMask: Uint8Array | null = null;
    if (f.author) {
      authorMask = new Uint8Array(n);
      for (const i of await authorWorlds(f.author)) authorMask[i] = 1;
    }
    const any = useCap || lim >= 0 || !!plat || useDate || flagReq > 0 || flagForbid > 0 || csrReq.length > 0 || csrForbid.length > 0 || rareMasks.length > 0 || rareForbid.length > 0 || orGroups.length > 0 || !!authorMask;
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
        const okPc = pc[i] <= lim || (rem > 0 && pc[i] === lim + 1 && loPc![i] < rem);
        const okQ = quest[i] <= lim || (rem > 0 && quest[i] === lim + 1 && loQ![i] < rem);
        if (plat === "pc") ok = okPc;
        else if (plat === "quest") ok = okQ;
        else if (plat === "both") ok = okPc && okQ;
        else ok = okPc || okQ;
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
      if (ok && orGroups.length) {
        for (const g of orGroups) {
          let hit = (flags[i] & g.fm) !== 0;
          for (let j = tagOff[i]; !hit && j < tagOff[i + 1]; j++) if (g.csr.includes(tagIds[j])) hit = true;
          if (!hit) { ok = false; break; }
        }
      }
      if (ok && flagForbid && (flags[i] & flagForbid)) ok = false;
      if (ok && csrForbid.length) {
        for (let j = tagOff[i]; j < tagOff[i + 1] && ok; j++) if (csrForbid.includes(tagIds[j])) ok = false;
      }
      if (ok && authorMask && !authorMask[i]) ok = false;
      if (ok) for (const rm of rareForbid) if (rm[i]) { ok = false; break; }
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
    const clean = norm(q).trim().replace(/\s+/g, " ") === tk; // no punctuation in the query
    const add = async (term: string, bonus: (t: string) => number, prefix: boolean) => {
      const sh = await getShard(this.m, term);
      let i = lowerBound(sh.terms, term);
      for (let g = 0; i < sh.terms.length && g < 5000; i++, g++) {
        const t = sh.terms[i];
        if (prefix ? !t.startsWith(term) : t !== term) break;
        const bn = bonus(t);
        forEachPosting(sh, i, (d) => { this.sc[d] += bn; });
      }
    };
    const jobs: Promise<void>[] = [];
    if (tk) {
      if (clean) jobs.push(add(TITLE_MARK + tk, (t) => (t === TITLE_MARK + tk ? 150 : 35), true));
      jobs.push(add(TAG_MARK + tk, () => 45, false));
      jobs.push(add(AUTHOR_MARK + tk, () => 80, false));
    }
    const pcs = pieces(q);
    for (let i = 0; i + 1 < pcs.length; i++) jobs.push(add(PAIR_MARK + pcs[i] + " " + pcs[i + 1], () => (clean ? 20 : 40), false));
    await Promise.all(jobs);
    return toks.length;
  }

  async run(inp: QueryIn): Promise<QueryOut> {
    const t0 = performance.now();
    const f = inp.filters ?? {};
    const sort: Sort = inp.sort ?? "relevance"; // STUB: trending/capacity fall through to popularity/created for now
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
    const cntOf = (k: string) => { const id = this.tagId.get(k); return id === undefined ? 0 : tagCnt[id]; };
    const status = { approved: cntOf("system_approved"), labs: cntOf("system_labs") };
    const cw: Record<string, number> = {};
    for (const [k] of this.m.cw ?? []) { const v = cntOf(k); if (v || this.tagId.has(k)) cw[k.slice(8)] = v; }
    const userTags: Record<string, number> = {};
    for (const [k, v] of this.userOrder(tagCnt)) userTags[k] = v;
    const userTagList = this.userOrder(tagCnt);
    const facetCounts: FacetCounts = {
      platform: { pc: pcN, quest: qN, both: bothN }, tags, capacity, status, cw, userTags, userTagList,
      // STUB (round 5 step 1): real histograms land with the evaluator commit
      months: { m0: 0, counts: [] }, updatedMonths: { m0: 0, counts: [] }, capacityHist: [],
    };
    // top-k
    const idx = this.topK(sort, need > 0, limit, total);
    if (inp.whatIf) {
      const pi = { any: 0, pc: 0, quest: 0, both: 0 };
      for (const p of ["", "pc", "quest", "both"] as const) {
        const { mask: m2 } = await this.filterMask({ ...f, platform: p });
        let k = 0;
        for (let i = 0; i < n; i++) if (m2[i] && (!need || cnt[i] === need)) k++;
        pi[p || "any"] = k;
      }
      facetCounts.platformIf = pi;
    }
    return { idx, total, facetCounts, ms: performance.now() - t0, parse: parseQuery(inp.q ?? ""), textRanked: need > 0 };
  }

  /** v2 entry point (alias of run) */
  query(inp: QueryIn): Promise<QueryOut> { return this.run(inp); }

  /** STUB: always null until thumbs.parquet exists and is packed (never hotlinks) */
  async thumbUrl(_i: number): Promise<string | null> { return null; }

  private topK(sort: Sort, hasText: boolean, k: number, total: number): number[] {
    const { mask, c, sc, prior } = this as any as { mask: Uint8Array; c: Core; sc: Float32Array; prior: Float32Array };
    const n = c.n;
    k = Math.min(k, total);
    if (!k) return [];
    const byPop = sort === "visits" || ((sort === "relevance" || sort === "text") && !hasText);
    if (byPop) { // index order is visits desc
      const out: number[] = [];
      for (let i = 0; i < n && out.length < k; i++) if (mask[i]) out.push(i);
      return out;
    }
    let key: (i: number) => number;
    if (sort === "relevance") key = (i) => sc[i] * prior[i];
    else if (sort === "text") key = (i) => sc[i];
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

  private userIds: number[] | null = null;
  /** user tags grouped by case/plural variants (key '~g' for merged groups), count desc, top 40 */
  private userOrder(tagCnt: Uint32Array): [string, number][] {
    this.userIds ??= this.tagKeys.map((k, i) => (tagClass(k) === "user" ? i : -1)).filter((i) => i >= 0);
    const agg = new Map<string, number>();
    for (const i of this.userIds) {
      if (!tagCnt[i]) continue;
      const g = this.groupOf.get(this.tagKeys[i]);
      const key = g ? "~" + g : this.tagKeys[i];
      agg.set(key, (agg.get(key) ?? 0) + tagCnt[i]);
    }
    return [...agg].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)).slice(0, 40);
  }

  /** filter-only eligibility mask (no text); a view of internal state, copy before keeping */
  async allowMask(f: Filters): Promise<Uint8Array> { return (await this.filterMask(f)).mask; }

  async similar(idx: number, k: number) { return similarIdx(idx, k); }
  idToIdx = idToIdx;
  info(idx: number[]) { return worldInfo(this.m, idx); }
  cacheKey = cached;
}
