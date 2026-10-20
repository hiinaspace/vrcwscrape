import {
  Core, Manifest, Shard, TITLE_MARK, authorWorlds, fetchJson, forEachPosting, getShard, idToIdx, loadCore,
  loadManifest, lowerBound, loadRare, loadSizeLo, Rare, worldInfo, similarIdx, cached,
} from "./data";
import { AUTHOR_MARK, PAIR_MARK, TAG_MARK, Tok, norm, pieces, titleKey, tokens } from "./tok";
import { Node, parseQuery } from "./query";
import type { ParseResult } from "./query";
import { Thumbs } from "./thumbs";

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
  /** true when the whole query was read as literal words because it is exactly some world's title (e.g. "-Name-") */
  literal: boolean;
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
const F_TITLE = 1, F_AUTHOR = 2, F_TAG = 4, F_DESC = 8, F_ANY = 15;
// per-world filter failure bits (filterMask)
const X_CAP = 1, X_PUB = 2, X_UPD = 4, X_REST = 8;

/** a set of tags matching "any of": flag bits, core tag ids and/or a per-world mask (rare tags) */
interface TagSel { fm: number; csr: number[]; mask: Uint8Array | null }

function hasSyntax(n: Node | null): boolean {
  if (!n) return false;
  if (n.t === "term") return n.phrase;
  if (n.t === "and") return n.c.some(hasSyntax);
  return true;
}
function hasStructured(n: Node | null): boolean {
  if (!n) return false;
  if (n.t === "and" || n.t === "or") return n.c.some(hasStructured);
  if (n.t === "not") return hasStructured(n.c);
  return n.t === "field" || n.t === "num" || n.t === "date";
}

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
  private fail!: Uint8Array;
  private pool8: Uint8Array[] = [];
  private poolF: Float32Array[] = [];
  private lowerTags: Map<string, number[]> | null = null;
  private grpId!: Int16Array; // core tag id -> merged-group index, -1 none
  private grpKeys: string[] = [];
  private monC!: Uint16Array; // month index of created (0 = unknown)
  private monU!: Uint16Array;
  allTags: Promise<[string, number][]> | null = null;
  sizeLo: Promise<{ pc: Uint16Array; quest: Uint16Array }> | null = null;
  rare: Promise<{ r: Rare; keys: string[]; id: Map<string, number> }> | null = null;
  defaultFacets!: FacetCounts;
  thumbs: Thumbs | null = null;
  hasThumbs = false;
  trend: { basis: "recent_visits_per_day" | "popularity_fallback"; windowDays: number | null } = { basis: "popularity_fallback", windowDays: null };
  /** global month-index domain of published/updated dates (stable histogram axes) */
  months = { min: 0, max: 0 };
  private trendScale = 0;

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
    // merged tag groups (UI tag facet): core tag id -> group index
    this.grpId = new Int16Array(this.tagKeys.length).fill(-1);
    const gidx = new Map<string, number>();
    this.tagKeys.forEach((k, i) => {
      const g = this.groupOf.get(k);
      if (g === undefined) return;
      let gi = gidx.get(g);
      if (gi === undefined) { gi = this.grpKeys.length; gidx.set(g, gi); this.grpKeys.push("~" + g); }
      this.grpId[i] = gi;
    });
    this.sc = new Float32Array(n);
    this.cnt = new Uint8Array(n);
    this.tw = new Float32Array(n);
    this.mask = new Uint8Array(n);
    this.fail = new Uint8Array(n);
    // month tables
    const monOfDay = new Uint16Array(65536);
    {
      let last = -1;
      for (let d = 0; d < 65536; d++) {
        const dt = new Date(EPOCH + d * 86400000);
        last = (dt.getUTCFullYear() - 2010) * 12 + dt.getUTCMonth();
        monOfDay[d] = last;
      }
    }
    this.monC = new Uint16Array(n);
    this.monU = new Uint16Array(n);
    let mn = 1 << 20, mx = 0;
    for (let i = 0; i < n; i++) {
      const a = c.created[i], b = c.updated[i];
      if (a > 0) { this.monC[i] = monOfDay[a]; mn = Math.min(mn, this.monC[i]); mx = Math.max(mx, this.monC[i]); }
      if (b > 0) { this.monU[i] = monOfDay[b]; mn = Math.min(mn, this.monU[i]); mx = Math.max(mx, this.monU[i]); }
    }
    this.months = mn <= mx ? { min: mn, max: mx } : { min: 0, max: 0 };
    if (this.m.trend) { this.trend = { basis: this.m.trend.basis, windowDays: this.m.trend.windowDays }; this.trendScale = this.m.trend.scale; }
    if (this.m.thumbs) { this.thumbs = new Thumbs(this.m); this.hasThumbs = true; }
    return this;
  }

  /** approximate recent visits/day of world i (NaN when only the popularity fallback exists in this build) */
  trendOf(i: number): number {
    if (!this.c.trend || !this.trendScale) return NaN;
    return Math.pow(10, this.c.trend[i] / this.trendScale) - 1;
  }

  /** object URL of the self-hosted AVIF thumbnail of world ordinal i, or null (no thumb / not built). Never a remote URL. */
  async thumbUrl(i: number): Promise<string | null> {
    return this.thumbs ? this.thumbs.url(i) : null;
  }

  // ---- scratch buffers
  private u8(): Uint8Array { const a = this.pool8.pop(); if (a) { a.fill(0); return a; } return new Uint8Array(this.c.n); }
  private f32(): Float32Array { const a = this.poolF.pop(); if (a) { a.fill(0); return a; } return new Float32Array(this.c.n); }
  private free8(a: Uint8Array | null) { if (a) this.pool8.push(a); }
  private freeF(a: Float32Array | null) { if (a) this.poolF.push(a); }

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

  // ---------------------------------------------------------------- tag selectors
  private selOfIds(ids: number[]): TagSel {
    const s: TagSel = { fm: 0, csr: [], mask: null };
    for (const id of ids) { if (id < this.m.flagTags) s.fm |= 1 << id; else s.csr.push(id); }
    return s;
  }

  /** filter-style selector: exact raw key, or "~group" (any case/plural variant) */
  private async selFromKey(raw: string): Promise<TagSel> {
    if (raw.startsWith("~")) {
      const ids: number[] = [];
      for (const k of this.m.tagGroups?.[raw.slice(1)] ?? [raw.slice(1)]) { const id = this.tagId.get(k); if (id !== undefined) ids.push(id); }
      return this.selOfIds(ids);
    }
    const key = this.resolveTag(raw);
    const id = this.tagId.get(key);
    if (id !== undefined) return this.selOfIds([id]);
    return this.rareSel((k) => k === key);
  }

  private async rareSel(pred: (k: string) => boolean): Promise<TagSel> {
    const { r, keys } = await this.ensureRare();
    const want = new Uint8Array(keys.length);
    let any = false;
    keys.forEach((k, i) => { if (pred(k)) { want[i] = 1; any = true; } });
    const n = this.c.n, mask = new Uint8Array(n);
    if (any) for (let i = 0; i < n; i++) for (let j = r.off[i]; j < r.off[i + 1]; j++) if (want[r.ids[j]]) { mask[i] = 1; break; }
    return { fm: 0, csr: [], mask };
  }

  /** query-language selector: case-insensitive, merges case/plural variants of core tags */
  private async selFromName(name: string, prefix = ""): Promise<TagSel> {
    const lc = norm(name).trim();
    if (prefix) { // content_x / system_x are exact keys
      const id = this.tagId.get(prefix + lc);
      return id !== undefined ? this.selOfIds([id]) : this.rareSel((k) => k === prefix + lc);
    }
    if (!this.lowerTags) {
      this.lowerTags = new Map();
      this.tagKeys.forEach((k, i) => { const l = k.toLowerCase(); (this.lowerTags!.get(l) ?? this.lowerTags!.set(l, []).get(l)!).push(i); });
    }
    const g = this.m.tagGroups?.[lc] ?? this.m.tagGroups?.[lc.replace(/s$/, "")];
    if (g) return this.selOfIds(g.map((k) => this.tagId.get(k)).filter((x): x is number => x !== undefined));
    const ids = this.lowerTags.get(lc);
    if (ids) return this.selOfIds(ids);
    return this.rareSel((k) => k.toLowerCase() === lc);
  }

  private selMask(sel: TagSel, out: Uint8Array) {
    const { flags, tagOff, tagIds } = this.c;
    const n = this.c.n, { fm, csr, mask } = sel;
    for (let i = 0; i < n; i++) {
      let hit = (flags[i] & fm) !== 0 || (mask !== null && mask[i] === 1);
      if (!hit && csr.length) for (let j = tagOff[i]; j < tagOff[i + 1]; j++) if (csr.includes(tagIds[j])) { hit = true; break; }
      out[i] = hit ? 1 : 0;
    }
  }

  // ---------------------------------------------------------------- filters
  /** per-world failure bits (0 = passes every filter). Shared buffer: consume before the next call. */
  private async filterMask(f: Filters): Promise<{ fail: Uint8Array; any: boolean }> {
    const c = this.c, n = c.n, fail = this.fail;
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
    const platEx = f.platform_ex === "pc" ? 1 : f.platform_ex === "quest" ? 2 : 0;
    const uLo = f.after ? dayOf(f.after) : -1, uHi = f.before ? dayOf(f.before) : 1e9;
    const useUpd = !!(f.after || f.before);
    const pLo = f.pub_after ? dayOf(f.pub_after) : -1, pHi = f.pub_before ? dayOf(f.pub_before) : 1e9;
    const usePub = !!(f.pub_after || f.pub_before);
    // tags (explicit tags + status + content warnings): required (AND) and excluded
    const reqKeys = [...(f.tags ?? []), ...(f.status ?? []).map((x) => "system_" + x.replace(/^system_/, "")), ...(f.cw ?? []).map((x) => "content_" + x.replace(/^content_/, ""))];
    const exclKeys = [...(f.tags_ex ?? []), ...(f.status_ex ?? []).map((x) => "system_" + x.replace(/^system_/, "")), ...(f.cw_exclude ?? []).map((x) => "content_" + x.replace(/^content_/, ""))];
    const req: TagSel[] = [], forbid: TagSel[] = [];
    for (const t of reqKeys) req.push(await this.selFromKey(t));
    for (const t of exclKeys) forbid.push(await this.selFromKey(t));
    let authorMask: Uint8Array | null = null;
    if (f.author) {
      authorMask = new Uint8Array(n);
      for (const i of await authorWorlds(f.author)) authorMask[i] = 1;
    }
    const any = useCap || lim >= 0 || !!plat || !!platEx || useUpd || usePub || req.length > 0 || forbid.length > 0 || !!authorMask;
    if (!any) { fail.fill(0); return { fail, any }; }
    const { cap, pc, quest, plat: pl, updated, created, flags, tagOff, tagIds } = c;
    const hit = (s: TagSel, i: number) => {
      if ((flags[i] & s.fm) !== 0 || (s.mask !== null && s.mask[i] === 1)) return true;
      const csr = s.csr;
      if (csr.length) for (let j = tagOff[i]; j < tagOff[i + 1]; j++) if (csr.includes(tagIds[j])) return true;
      return false;
    };
    for (let i = 0; i < n; i++) {
      let fl = 0;
      if (useCap && (cap[i] < capLo || cap[i] > capHi)) fl |= X_CAP;
      if (usePub && (created[i] < pLo || created[i] > pHi)) fl |= X_PUB;
      if (useUpd && (updated[i] < uLo || updated[i] > uHi)) fl |= X_UPD;
      if (plat) {
        const p = pl[i];
        if (plat === "pc" ? !(p & 1) : plat === "quest" ? !(p & 2) : p !== 3) fl |= X_REST;
      }
      if (platEx && (pl[i] & platEx)) fl |= X_REST;
      if (lim >= 0) {
        const okPc = pc[i] <= lim || (rem > 0 && pc[i] === lim + 1 && loPc![i] < rem);
        const okQ = quest[i] <= lim || (rem > 0 && quest[i] === lim + 1 && loQ![i] < rem);
        let ok: boolean;
        if (plat === "pc") ok = okPc;
        else if (plat === "quest") ok = okQ;
        else if (plat === "both") ok = okPc && okQ;
        else ok = okPc || okQ;
        if (!ok) fl |= X_REST;
      }
      if (authorMask && !authorMask[i]) fl |= X_REST;
      if (!(fl & X_REST)) {
        for (let k = 0; k < req.length; k++) if (!hit(req[k], i)) { fl |= X_REST; break; }
        if (!(fl & X_REST)) for (let k = 0; k < forbid.length; k++) if (hit(forbid[k], i)) { fl |= X_REST; break; }
      }
      fail[i] = fl;
    }
    return { fail, any };
  }

  // ---------------------------------------------------------------- legacy plain-words text match
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
    const tw = this.tw;
    toks.forEach((t: Tok, ti) => {
      tw.fill(0);
      this.fillWeights(shards[ti], t.term, lastIsWord && ti === lastIdx, F_ANY, tw);
      for (let d = 0; d < n; d++) if (tw[d] > 0) { this.sc[d] += tw[d]; this.cnt[d]++; }
    });
    await this.addBonuses(q);
    return toks.length;
  }

  /** best field weight per doc of one term (exact, or prefix-expanded) restricted to field bits fmask */
  private fillWeights(sh: Shard, term: string, prefix: boolean, fmask: number, tw: Float32Array) {
    let i = lowerBound(sh.terms, term);
    let guard = 0;
    for (; i < sh.terms.length; i++) {
      const t = sh.terms[i];
      const exact = t === term;
      if (!exact && !(prefix && t.startsWith(term))) break;
      if (++guard > 3000) break;
      const f = exact ? 1 : 0.6;
      forEachPosting(sh, i, (d, mk) => {
        mk &= fmask;
        if (!mk) return;
        const w = ((mk & 1 ? W_TITLE : 0) + (mk & 2 ? W_AUTHOR : 0) + (mk & 4 ? W_TAG : 0) + (mk & 8 ? W_DESC : 0)) * f;
        if (w > tw[d]) tw[d] = w;
      });
    }
  }

  /** round-4 bonuses: exact/prefix title, exact tag phrase, exact author, adjacent title pairs (into this.sc) */
  private async addBonuses(q: string) {
    const tk = titleKey(q);
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
  }

  // ---------------------------------------------------------------- query-language evaluation
  private ranked = false;

  private leafToks(text: string): Tok[] {
    let toks = tokens(text);
    const nonStop = toks.filter((t) => !this.stop.has(t.term));
    if (nonStop.length) toks = nonStop;
    const seen = new Set<string>();
    return toks.filter((t) => (seen.has(t.term) ? false : (seen.add(t.term), true)));
  }

  /** AND of tokens (field-restricted); scores go to this.sc when pos */
  private async tokenAnd(toks: Tok[], prefixLast: boolean, fmask: number, pos: boolean): Promise<Uint8Array | null> {
    if (!toks.length) return null;
    const n = this.c.n;
    const shards = await Promise.all(toks.map((t) => getShard(this.m, t.term)));
    const cnt = this.u8(), lsc = this.f32(), tw = this.f32();
    const lastIdx = toks.length - 1;
    const lastIsWord = prefixLast && toks[lastIdx].word && Array.from(toks[lastIdx].term).length >= 2;
    toks.forEach((t, ti) => {
      tw.fill(0);
      this.fillWeights(shards[ti], t.term, lastIsWord && ti === lastIdx, fmask, tw);
      for (let d = 0; d < n; d++) if (tw[d] > 0) { lsc[d] += tw[d]; cnt[d]++; }
    });
    const k = toks.length, out = this.u8(), sc = this.sc;
    for (let d = 0; d < n; d++) if (cnt[d] === k) { out[d] = 1; if (pos) sc[d] += lsc[d]; }
    this.free8(cnt); this.freeF(lsc); this.freeF(tw);
    if (pos) this.ranked = true;
    return out;
  }

  /** postings of an exact marker term (title pair, tag phrase, author name) as a mask; adds `bonus` to scores when pos */
  private async markerMask(term: string, bonus: number, pos: boolean): Promise<Uint8Array> {
    const out = this.u8();
    const sh = await getShard(this.m, term);
    const i = lowerBound(sh.terms, term);
    if (i < sh.terms.length && sh.terms[i] === term) forEachPosting(sh, i, (d) => { out[d] = 1; if (pos) this.sc[d] += bonus; });
    return out;
  }

  private async evalPhrase(text: string, pos: boolean): Promise<Uint8Array | null> {
    const toks = this.leafToks(text);
    const pcs = pieces(text);
    const all = await this.tokenAnd(toks, false, F_ANY, pos);
    if (!all || pcs.length <= 1) return all;
    const tk = titleKey(text);
    const n = this.c.n;
    const adj = this.u8();
    adj.fill(1);
    for (let i = 0; i + 1 < pcs.length; i++) {
      const pm = await this.markerMask(PAIR_MARK + pcs[i] + " " + pcs[i + 1], 30, pos);
      for (let d = 0; d < n; d++) adj[d] &= pm[d];
      this.free8(pm);
    }
    // exact tag / author phrase and description words (no positions in the index: all words in the description field)
    const tagM = tk ? await this.markerMask(TAG_MARK + tk, 0, false) : null;
    const autM = tk ? await this.markerMask(AUTHOR_MARK + tk, 0, false) : null;
    const desc = await this.tokenAnd(toks, false, F_DESC, false);
    for (let d = 0; d < n; d++) {
      const ok = adj[d] || (tagM && tagM[d]) || (autM && autM[d]) || (desc && desc[d]);
      if (!ok) all[d] = 0;
    }
    this.free8(adj); this.free8(tagM); this.free8(autM); this.free8(desc);
    return all;
  }

  private numMask(nd: Extract<Node, { t: "num" }>): Uint8Array {
    const n = this.c.n, out = this.u8(), c = this.c;
    const ok = (v: number) => (nd.lo === null || (nd.loEx ? v > nd.lo : v >= nd.lo)) && (nd.hi === null || (nd.hiEx ? v < nd.hi : v <= nd.hi));
    for (let i = 0; i < n; i++) {
      let hit = false;
      switch (nd.field) {
        case "cap": hit = c.cap[i] >= 1 && ok(c.cap[i]); break;
        case "visits": hit = ok(c.visits[i]); break;
        case "favorites": hit = ok(c.fav[i]); break;
        case "size": {
          const a = c.pc[i], b = c.quest[i];
          const kb = Math.min(a === NONE ? Infinity : a, b === NONE ? Infinity : b);
          hit = kb !== Infinity && ok(kb / 1024);
          break;
        }
      }
      out[i] = hit ? 1 : 0;
    }
    return out;
  }

  private dateMask(nd: Extract<Node, { t: "date" }>): Uint8Array {
    const n = this.c.n, out = this.u8();
    const arr = nd.field === "updated" ? this.c.updated : this.c.created;
    const lo = nd.lo ? dayOf(nd.lo) : 1, hi = nd.hi ? dayOf(nd.hi) : 1e9;
    for (let i = 0; i < n; i++) out[i] = arr[i] > 0 && arr[i] >= lo && arr[i] <= hi ? 1 : 0;
    return out;
  }

  private async ev(node: Node, pos: boolean, lastLeaf: Node | null): Promise<Uint8Array | null> {
    const n = this.c.n;
    switch (node.t) {
      case "term": {
        if (node.phrase) return this.evalPhrase(node.text, pos);
        return this.tokenAnd(this.leafToks(node.text), node === lastLeaf, F_ANY, pos);
      }
      case "field": {
        switch (node.field) {
          case "tag": case "cw": case "status": {
            const sel = await this.selFromName(node.value, node.field === "cw" ? "content_" : node.field === "status" ? "system_" : "");
            const out = this.u8();
            this.selMask(sel, out);
            return out;
          }
          case "platform": {
            const out = this.u8(), pl = this.c.plat;
            for (let i = 0; i < n; i++) out[i] = node.value === "pc" ? (pl[i] & 1) : node.value === "quest" ? (pl[i] >> 1) & 1 : pl[i] === 3 ? 1 : 0;
            return out;
          }
          case "title": return this.tokenAnd(this.leafToks(node.value), node === lastLeaf, F_TITLE, pos);
          case "author": {
            if (/^usr_/.test(node.value)) {
              const out = this.u8();
              for (const i of await authorWorlds(node.value)) out[i] = 1;
              return out;
            }
            return this.tokenAnd(this.leafToks(node.value), node === lastLeaf, F_AUTHOR, pos);
          }
        }
        return null;
      }
      case "num": return this.numMask(node);
      case "date": return this.dateMask(node);
      case "not": {
        const c = await this.ev(node.c, !pos, lastLeaf);
        if (!c) return null;
        for (let i = 0; i < n; i++) c[i] ^= 1;
        return c;
      }
      case "and": case "or": {
        let acc: Uint8Array | null = null;
        for (const ch of node.c) {
          const m = await this.ev(ch, pos, lastLeaf);
          if (!m) continue;
          if (!acc) { acc = m; continue; }
          if (node.t === "and") for (let i = 0; i < n; i++) acc[i] &= m[i];
          else for (let i = 0; i < n; i++) acc[i] |= m[i];
          this.free8(m);
        }
        return acc;
      }
    }
  }

  private collectTerms(node: Node, out: Set<string>) {
    const toks = (t: string) => { for (const k of this.leafToks(t)) out.add(k.term); };
    switch (node.t) {
      case "term":
        toks(node.text);
        if (node.phrase) {
          const pcs = pieces(node.text), tk = titleKey(node.text);
          for (let i = 0; i + 1 < pcs.length; i++) out.add(PAIR_MARK + pcs[i] + " " + pcs[i + 1]);
          if (tk) { out.add(TAG_MARK + tk); out.add(AUTHOR_MARK + tk); }
        }
        break;
      case "field": if (node.field === "title" || (node.field === "author" && !/^usr_/.test(node.value))) toks(node.value); break;
      case "not": this.collectTerms(node.c, out); break;
      case "and": case "or": node.c.forEach((c) => this.collectTerms(c, out)); break;
    }
  }

  private lastTermNode(node: Node, end: number): Node | null {
    switch (node.t) {
      case "term": return !node.phrase && node.end === end ? node : null;
      case "field": return (node.field === "title" || node.field === "author") && node.end === end ? node : null;
      case "not": return this.lastTermNode(node.c, end);
      case "and": case "or": for (let i = node.c.length - 1; i >= 0; i--) { const r = this.lastTermNode(node.c[i], end); if (r) return r; } return null;
    }
    return null;
  }

  /** evaluate a parsed query; returns the text mask (null = no constraint) and fills this.sc with positive scores */
  private async evalQuery(ast: Node, q: string): Promise<Uint8Array | null> {
    this.ranked = false;
    this.sc.fill(0);
    const need = new Set<string>();
    this.collectTerms(ast, need);
    await Promise.all([...need].map((t) => getShard(this.m, t)));
    const endsOpen = !/\s$/.test(q);
    const last = endsOpen ? this.lastTermNode(ast, q.replace(/\s+$/, "").length) : null;
    const m = await this.ev(ast, true, last);
    // round-4 bonuses from the top-level plain terms (raw text, so punctuation still counts)
    const top = ast.t === "and" ? ast.c : [ast];
    const plain = top.filter((x): x is Extract<Node, { t: "term" }> => x.t === "term" && !x.phrase);
    if (plain.length) await this.addBonuses(plain.map((x) => q.slice(x.start, x.end)).join(" "));
    return m;
  }

  private async literalByPairs(q: string): Promise<boolean> {
    const pcs = pieces(q);
    if (pcs.length < 2) return false;
    let acc: Set<number> | null = null;
    for (let i = 0; i + 1 < pcs.length; i++) {
      const term = PAIR_MARK + pcs[i] + " " + pcs[i + 1];
      const sh = await getShard(this.m, term);
      const k = lowerBound(sh.terms, term);
      if (k >= sh.terms.length || sh.terms[k] !== term) return false;
      const cur = new Set<number>();
      forEachPosting(sh, k, (d) => { if (!acc || acc.has(d)) cur.add(d); });
      if (!cur.size) return false;
      acc = cur;
    }
    return !!acc && acc.size > 0;
  }

  // ---------------------------------------------------------------- run
  async run(inp: QueryIn): Promise<QueryOut> {
    const t0 = performance.now();
    const q = inp.q ?? "";
    const f = inp.filters ?? {};
    const sort: Sort = inp.sort ?? "relevance";
    const limit = Math.max(0, inp.limit ?? 50), offset = Math.max(0, inp.offset ?? 0);
    const doFacets = inp.facets !== false;
    const parse = parseQuery(q);
    const c = this.c, n = c.n;

    // --- text condition -> tm (null = none), scores in this.sc
    let tm: Uint8Array | null = null;
    let textRanked = false, literal = false;
    if (q.trim()) {
      let syntax = hasSyntax(parse.ast);
      // a query whose raw adjacent word pairs all occur in one world's title ("Foo -Bar", "(Fixed for") is that title, not operators
      if (syntax && !hasStructured(parse.ast) && (await this.literalByPairs(q))) { syntax = false; literal = true; }
      if (syntax && parse.ast) {
        tm = await this.evalQuery(parse.ast, q);
        textRanked = this.ranked;
      } else {
        const need = await this.textMatch(q);
        if (need) {
          tm = this.u8();
          for (let i = 0; i < n; i++) tm[i] = this.cnt[i] === need ? 1 : 0;
          textRanked = true;
        }
      }
    }
    const { fail } = await this.filterMask(f);
    const mask = this.mask;
    mask.fill(0);

    // --- one pass: total, facets, what-if histograms
    let total = 0;
    const nf = this.m.flagTags, nt = this.tagKeys.length;
    const tagCnt = new Uint32Array(nt);
    const ng = this.grpKeys.length;
    const gcnt = new Uint32Array(ng), gst = new Int32Array(ng);
    const capB = [0, 0, 0, 0, 0];
    let pcN = 0, qN = 0, bothN = 0;
    const m0 = this.months.min, span = this.months.max - m0 + 1;
    const mon = new Uint32Array(span), monU = new Uint32Array(span), capH = new Uint32Array(256);
    const { plat, flags, tagOff, tagIds, cap } = c;
    const grpId = this.grpId, monC = this.monC, monUa = this.monU;
    for (let i = 0; i < n; i++) {
      const fl = fail[i];
      if (fl & X_REST) continue;
      if (tm !== null && !tm[i]) continue;
      if (fl === 0) {
        mask[i] = 1;
        total++;
        if (!doFacets) continue;
        const p = plat[i];
        if (p & 1) pcN++;
        if (p & 2) qN++;
        if (p === 3) bothN++;
        const stamp = i + 1;
        const fb = flags[i];
        if (fb) for (let b = 0; b < nf; b++) if (fb & (1 << b)) {
          tagCnt[b]++;
          const g = grpId[b];
          if (g >= 0 && gst[g] !== stamp) { gst[g] = stamp; gcnt[g]++; }
        }
        for (let j = tagOff[i]; j < tagOff[i + 1]; j++) {
          const t = tagIds[j];
          tagCnt[t]++;
          const g = grpId[t];
          if (g >= 0 && gst[g] !== stamp) { gst[g] = stamp; gcnt[g]++; }
        }
        const cp = cap[i];
        if (cp >= 1) capB[cp <= 8 ? 0 : cp <= 16 ? 1 : cp <= 32 ? 2 : cp <= 64 ? 3 : 4]++;
      }
      if (!doFacets) continue;
      // what-if histograms: every filter except the histogram's own dimension
      if ((fl & ~X_PUB) === 0 && monC[i]) mon[monC[i] - m0]++;
      if ((fl & ~X_UPD) === 0 && monUa[i]) monU[monUa[i] - m0]++;
      if ((fl & ~X_CAP) === 0) capH[cap[i]]++;
    }

    let facetCounts: FacetCounts;
    if (doFacets) {
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
      const userTagList = this.userOrder(tagCnt, gcnt, inp.tagLimit ?? 40);
      const userTags: Record<string, number> = {};
      for (const [k, v] of userTagList) userTags[k] = v;
      facetCounts = {
        platform: { pc: pcN, quest: qN, both: bothN }, tags, capacity, status, cw, userTags, userTagList,
        months: { m0, counts: Array.from(mon) }, updatedMonths: { m0, counts: Array.from(monU) }, capacityHist: Array.from(capH),
      };
    } else {
      facetCounts = {
        platform: { pc: 0, quest: 0, both: 0 }, tags: {}, capacity: {}, status: {}, cw: {}, userTags: {}, userTagList: [],
        months: { m0, counts: [] }, updatedMonths: { m0, counts: [] }, capacityHist: [],
      };
    }
    // --- top-k
    const idx = this.topK(sort, textRanked, limit + offset, total, !!inp.asc).slice(offset);
    if (inp.whatIf && doFacets) {
      const pi = { any: 0, pc: 0, quest: 0, both: 0 };
      for (const p of ["", "pc", "quest", "both"] as const) {
        const { fail: f2 } = await this.filterMask({ ...f, platform: p });
        let k = 0;
        for (let i = 0; i < n; i++) if (f2[i] === 0 && (!tm || tm[i])) k++;
        pi[p || "any"] = k;
      }
      facetCounts.platformIf = pi;
    }
    this.free8(tm);
    return { idx, total, facetCounts, ms: performance.now() - t0, parse, textRanked, literal };
  }

  /** v2 entry point (alias of run) */
  query(inp: QueryIn): Promise<QueryOut> { return this.run(inp); }

  private topK(sort: Sort, hasText: boolean, k: number, total: number, asc: boolean): number[] {
    const { mask, c, sc, prior } = this as any as { mask: Uint8Array; c: Core; sc: Float32Array; prior: Float32Array };
    const n = c.n;
    k = Math.min(k, total);
    if (!k) return [];
    const byPop = sort === "visits" || ((sort === "relevance" || sort === "text") && !hasText) || (sort === "trending" && !c.trend);
    if (byPop) { // index order is visits desc
      const out: number[] = [];
      if (asc && sort === "visits") { for (let i = n - 1; i >= 0 && out.length < k; i--) if (mask[i]) out.push(i); }
      else for (let i = 0; i < n && out.length < k; i++) if (mask[i]) out.push(i);
      return out;
    }
    let key: (i: number) => number;
    let dir = 1; // 1: bigger is better
    switch (sort) {
      case "relevance": key = (i) => sc[i] * prior[i]; break;
      case "text": key = (i) => sc[i]; break;
      case "trending": { const t = c.trend!; key = (i) => t[i]; break; }
      case "favorites": key = (i) => c.fav[i]; dir = asc ? -1 : 1; break;
      case "updated": key = (i) => c.updated[i]; dir = asc ? -1 : 1; break;
      case "capacity": key = (i) => c.cap[i]; dir = asc ? -1 : 1; break;
      default: key = (i) => c.created[i]; dir = asc ? -1 : 1; // published
    }
    const better = (a: number, b: number) => {
      const ka = key(a) * dir, kb = key(b) * dir;
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
  /** user tags, case/plural variants merged into "~group" entries (each world counted once per group), count desc */
  private userOrder(tagCnt: Uint32Array, gcnt: Uint32Array, limit: number): [string, number][] {
    this.userIds ??= this.tagKeys.map((k, i) => (tagClass(k) === "user" ? i : -1)).filter((i) => i >= 0);
    const out: [string, number][] = [];
    for (const i of this.userIds) if (tagCnt[i] && this.grpId[i] < 0) out.push([this.tagKeys[i], tagCnt[i]]);
    for (let g = 0; g < this.grpKeys.length; g++) if (gcnt[g]) out.push([this.grpKeys[g], gcnt[g]]);
    return out.sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)).slice(0, limit);
  }

  /** filter-only eligibility mask (no text): fresh array, 1 = allowed */
  async allowMask(f: Filters): Promise<Uint8Array> {
    const { fail } = await this.filterMask(f);
    const out = new Uint8Array(this.c.n);
    for (let i = 0; i < out.length; i++) out[i] = fail[i] === 0 ? 1 : 0;
    return out;
  }

  async similar(idx: number, k: number) { return similarIdx(idx, k); }
  idToIdx = idToIdx;
  info(idx: number[]) { return worldInfo(this.m, idx); }
  cacheKey = cached;
}
