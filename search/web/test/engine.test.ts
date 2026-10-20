// Evaluator tests against the real built index (VRCW_DATA, default ../work/r5/data). Run: node test/run.mjs test/engine.test.ts
import test, { before } from "node:test";
import assert from "node:assert/strict";
import "./shim.mjs";
import { Engine, QueryIn } from "../src/engine";

let e: Engine;
before(async () => { e = await new Engine().init(); });
const Q = (q: string, extra: Partial<QueryIn> = {}) => e.query({ q, limit: 20, ...extra });
const total = async (q: string, extra: Partial<QueryIn> = {}) => (await Q(q, { ...extra, facets: false, limit: 0 })).total;
// finished-word variant (trailing space disables prefix expansion of the last term)
const T = (q: string, extra: Partial<QueryIn> = {}) => total(q + " ", extra);
const ids = async (q: string, extra: Partial<QueryIn> = {}) => (await Q(q, { ...extra, facets: false, limit: 5000 })).idx;
const set = async (q: string) => new Set(await ids(q));

test("negation is set difference", async () => {
  const a = await T("lake"), b = await T("lake hangout"), c = await T("lake -hangout");
  assert.ok(a > 100 && b > 0 && c > 0);
  assert.equal(a, b + c);
  assert.equal(await T("lake AND NOT hangout"), c);
  assert.equal(await T("lake NOT hangout"), c);
  const s = await set("lake -hangout "), all = await set("lake ");
  for (const i of s) assert.ok(all.has(i));
});

test("pure negative query = all worlds minus matches", async () => {
  const N = e.c.n;
  const h = await T("hangout");
  assert.equal(await T("-hangout"), N - h);
  assert.equal(await T("NOT hangout"), N - h);
  const r = await Q("-hangout", { limit: 3 });
  assert.equal(r.idx.length, 3);
  assert.equal(r.textRanked, false);
  assert.ok(r.idx[0] < r.idx[1] && r.idx[1] < r.idx[2]); // no positive text: popularity order
});

test("OR is union (inclusion-exclusion), parentheses group, De Morgan", async () => {
  for (const [a, b] of [["horror", "scary"], ["神社", "shrine"], ["cafe", "club"], ["lake", "river"]]) {
    const ta = await T(a), tb = await T(b), tab = await T(`${a} ${b}`);
    assert.equal(await T(`${a} OR ${b}`), ta + tb - tab, `${a} OR ${b}`);
    assert.equal(await T(`${a} | ${b}`), ta + tb - tab);
    assert.equal(await T(`(${a} | ${b})`), ta + tb - tab);
    // De Morgan: -(a b) == -a | -b
    assert.equal(await T(`-(${a} ${b})`), await T(`-${a} | -${b}`));
    assert.equal(await T(`-(${a} | ${b})`), await T(`-${a} -${b}`));
  }
  // OR binds tighter than the implicit AND
  const x = await T("lake horror | scary");
  assert.equal(x, await T("lake (horror | scary)"));
});

test("CJK terms and mixed queries", async () => {
  const s = await T("神社"), sh = await T("shrine");
  assert.ok(s > 20 && sh > 5);
  const u = await T("神社 OR shrine");
  assert.ok(u >= Math.max(s, sh) && u <= s + sh);
  const r = await Q("神社 -shrine ");
  assert.ok(r.total > 0 && r.total <= s);
  assert.equal(await T("神社 -神社"), 0);
  assert.equal(await T("神社 -shrine"), s - (await T("神社 shrine")));
});

test("fields: tag / cw / status / platform / numeric / date agree with the filters", async () => {
  assert.equal(await total("cap:>=16"), await total("", { filters: { cap_min: 16 } }));
  assert.equal(await total("cap:8..32"), await total("", { filters: { cap_min: 8, cap_max: 32 } }));
  assert.equal(await total("cap:<8"), await total("", { filters: { cap_min: 1, cap_max: 7 } }));
  assert.equal(await total("updated:>=2025"), await total("", { filters: { after: "2025-01-01" } }));
  assert.equal(await total("updated:2025"), await total("", { filters: { after: "2025-01-01", before: "2025-12-31" } }));
  assert.equal(await total("published:2024-06"), await total("", { filters: { pub_after: "2024-06-01", pub_before: "2024-06-30" } }));
  assert.equal(await total("platform:quest"), await total("", { filters: { platform: "quest" } }));
  assert.equal(await total("-platform:quest"), await total("", { filters: { platform_ex: "quest" } }));
  assert.equal(await total("status:approved"), await total("", { filters: { status: ["approved"] } }));
  assert.equal(await total("cw:horror"), await total("", { filters: { cw: ["horror"] } }));
  assert.equal(await total("-cw:horror"), await total("", { filters: { cw_exclude: ["horror"] } }));
  assert.equal(await total("tag:japan"), await total("", { filters: { tags: ["~japan"] } }));
  assert.equal(await total("-tag:japan"), await total("", { filters: { tags_ex: ["~japan"] } }));
  assert.equal(await total("-status:labs"), await total("", { filters: { status_ex: ["labs"] } }));
  let nv = 0; for (let i = 0; i < e.c.n; i++) if (e.c.visits[i] > 1000) nv++;
  assert.equal(await total("visits:>1000"), nv);
  const big = await ids("visits:>=100000");
  for (const i of big) assert.ok(e.c.visits[i] >= 100000);
  for (const i of await ids("size:<5")) assert.ok(Math.min(e.c.pc[i] >>> 0, e.c.quest[i] >>> 0) <= 5 * 1024);
  // query + filter are ANDed
  assert.equal(await T("lake cap:>=16"), await T("lake", { filters: { cap_min: 16 } }));
});

test("lake -hangout style combos run and are consistent", async () => {
  const qs = ["tag:japan -cw:horror", "(horror | scary) -tag:game cap:>=8", "title:lake -author:sam", '"escape room"', 'tag:"escape room"', "lake -hangout cap:8..32 platform:quest"];
  for (const q of qs) {
    const r = await Q(q);
    assert.ok(r.total >= 0 && r.parse.errors.length === 0, q);
  }
  assert.ok((await total('"escape room"')) > 0);
  assert.ok((await total("tag:japan")) > 100);
  // the phrase result is a subset of the plain-words result
  assert.ok((await total('"escape room"')) <= (await total("escape room")));
});

test("malformed queries never throw", async () => {
  const bad = ["(", ")", "a ||", "OR", "NOT", '"', "((lake", "lake))", "cap:abc", "tag:", "-", "- -", "published:2024-99", "lake OR", "()", "|", "-(", "cap:>=", "author:", 'title:"'];
  for (const q of bad) {
    const r = await Q(q);
    assert.ok(Number.isFinite(r.total), q);
  }
  assert.ok((await Q("cap:abc")).parse.errors.length === 1);
  let seed = 7;
  const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  const al = ["lake", "神", "社", " ", "-", "(", ")", "|", '"', "tag:", "cap:>=", "OR", "NOT", "x", "1", ".."];
  for (let i = 0; i < 300; i++) {
    let s = ""; for (let k = 0, n = 1 + Math.floor(rnd() * 8); k < n; k++) s += al[Math.floor(rnd() * al.length)];
    const r = await Q(s, { facets: false });
    assert.ok(Number.isFinite(r.total), s);
  }
});

test("literal title fallback: operator characters inside an exact title", async () => {
  // find a title containing a leading dash style by scanning a few popular chunks
  const infos = await e.info(Array.from({ length: 128 * 40 }, (_, i) => i));
  const t = infos.find((x) => /^-[A-Za-z]+-$/.test(x.name) || /^\(.+\)$/.test(x.name));
  if (!t) return;
  const r = await Q(t.name);
  const got = (await e.info(r.idx)).map((x) => x.id);
  assert.ok(got.includes(t.id), t.name);
});

test("trending sort uses recent visits (or the documented fallback)", async () => {
  const r = await Q("", { sort: "trending", limit: 10 });
  assert.equal(r.idx.length, 10);
  if (e.c.trend) {
    const t = e.c.trend;
    for (let k = 1; k < r.idx.length; k++) assert.ok(t[r.idx[k - 1]] >= t[r.idx[k]]);
    // global maximum
    let mx = 0; for (let i = 0; i < e.c.n; i++) mx = Math.max(mx, t[i]);
    assert.equal(t[r.idx[0]], mx);
    assert.equal(e.trend.basis, "recent_visits_per_day");
    assert.ok(e.trendOf(r.idx[0]) > 1000);
  }
  const lake = await Q("lake", { sort: "trending", limit: 10 });
  assert.ok(lake.total > 100);
});

test("sorts: capacity, updated, published, favorites (+asc) and offset paging", async () => {
  const cap = await Q("", { sort: "capacity", limit: 5 });
  for (let k = 1; k < 5; k++) assert.ok(e.c.cap[cap.idx[k - 1]] >= e.c.cap[cap.idx[k]]);
  const capA = await Q("lake", { sort: "capacity", asc: true, limit: 5 });
  for (let k = 1; k < 5; k++) assert.ok(e.c.cap[capA.idx[k - 1]] <= e.c.cap[capA.idx[k]]);
  const pub = await Q("", { sort: "published", asc: true, limit: 5, filters: { pub_after: "2018-01-01" } });
  for (let k = 1; k < 5; k++) assert.ok(e.c.created[pub.idx[k - 1]] <= e.c.created[pub.idx[k]]);
  const all = await Q("lake", { sort: "updated", limit: 30 });
  const pg = await Q("lake", { sort: "updated", limit: 10, offset: 10, facets: false });
  assert.deepEqual(pg.idx, all.idx.slice(10, 20));
  const vis = await Q("", { sort: "visits", asc: true, limit: 3 });
  assert.ok(vis.idx[0] > e.c.n / 2);
});

test("facets: histograms are what-if over their own dimension; userTags count once", async () => {
  const base = await Q("lake");
  const sum = (h: { counts: number[] }) => h.counts.reduce((a, b) => a + b, 0);
  // no date filter: months histogram sums to the total (modulo worlds with unknown dates)
  assert.ok(Math.abs(sum(base.facetCounts.months) - base.total) <= 5);
  const f = { pub_after: "2023-01-01", pub_before: "2023-12-31" };
  const filtered = await Q("lake", { filters: f });
  assert.ok(filtered.total < base.total);
  assert.deepEqual(filtered.facetCounts.months.counts, base.facetCounts.months.counts); // own-dimension filter ignored
  assert.deepEqual(filtered.facetCounts.months.m0, base.facetCounts.months.m0);
  // other dimension filters do restrict it
  const q = await Q("lake", { filters: { cap_min: 16 } });
  assert.ok(sum(q.facetCounts.months) < sum(base.facetCounts.months));
  // updatedMonths ignores after/before only
  const u = await Q("lake", { filters: { after: "2024-01-01", pub_after: "2023-01-01" } });
  const u0 = await Q("lake", { filters: { pub_after: "2023-01-01" } });
  assert.deepEqual(u.facetCounts.updatedMonths.counts, u0.facetCounts.updatedMonths.counts);
  // capacity what-if
  const c0 = await Q("lake"), c1 = await Q("lake", { filters: { cap_min: 20, cap_max: 40 } });
  assert.deepEqual(c1.facetCounts.capacityHist, c0.facetCounts.capacityHist);
  assert.equal(c0.facetCounts.capacityHist.reduce((a, b) => a + b, 0), c0.total);
  // merged tag groups: count equals the group-filter total exactly (no double count)
  const r = await Q("", { tagLimit: 200 });
  const groups = r.facetCounts.userTagList.filter(([k]) => k.startsWith("~")).slice(0, 12);
  assert.ok(groups.length > 0);
  for (const [k, n] of groups) {
    assert.equal(n, await total("", { filters: { tags: [k] } }), k);
    assert.ok(n <= r.total);
  }
  // counts sorted desc and unique keys
  const keys = new Set(r.facetCounts.userTagList.map((x) => x[0]));
  assert.equal(keys.size, r.facetCounts.userTagList.length);
  // same under a restrictive query
  const r2 = await Q("lake -hangout ", { tagLimit: 100 });
  for (const [k, n] of r2.facetCounts.userTagList.filter(([k]) => k.startsWith("~")).slice(0, 5)) assert.equal(n, await total("lake -hangout ", { filters: { tags: [k] } }), k);
});

test("filters: tags_ex, status_ex, platform_ex; filters AND with the query", async () => {
  const a = await total("", { filters: { tags: ["chill"] } });
  const b = await total("", { filters: { tags: ["chill"], tags_ex: ["chill"] } });
  assert.ok(a > 0); assert.equal(b, 0);
  const all = await total("");
  assert.equal(all, (await total("", { filters: { tags_ex: ["chill"] } })) + a);
  assert.equal(all, (await total("", { filters: { platform_ex: "quest" } })) + (await total("", { filters: { platform: "quest" } })));
});

test("whatIf platform counts match direct counts", async () => {
  const r = await Q("lake -hangout ", { whatIf: true, filters: { cap_min: 8 } });
  const p = r.facetCounts.platformIf!;
  assert.equal(p.any, r.total);
  for (const k of ["pc", "quest", "both"] as const) assert.equal(p[k], await total("lake -hangout ", { filters: { cap_min: 8, platform: k } }));
});

test("hook-style: window.vrcwSearch contract fields", async () => {
  const r = await Q("lake", { limit: 5 });
  assert.equal(r.idx.length, 5);
  assert.ok(r.facetCounts.platform && r.facetCounts.tags && r.facetCounts.capacity);
  assert.ok(Object.keys(r.facetCounts.tags).length >= 20);
});

test("allowMask(filters, q) honours typed conditions; rowCards come with the meta chunk", async () => {
  const m = await e.allowMask({}, "cap:>=16 -tag:game");
  let k = 0; for (let i = 0; i < m.length; i++) k += m[i];
  assert.equal(k, await total("cap:>=16 -tag:game"));
  const m2 = await e.allowMask({ platform: "quest" }, "");
  let k2 = 0; for (let i = 0; i < m2.length; i++) k2 += m2[i];
  assert.equal(k2, await total("", { filters: { platform: "quest" } }));
  const rc = await e.rowCards([0, 1, 5000]);
  assert.equal(rc.length, 3);
  assert.ok(rc[0].tags.length > 0 && rc[0].desc.length <= 160);
  assert.ok(e.maxDay > 5000);
});

test("cache: sort change / paging reuse the evaluated query", async () => {
  const a = await Q("lake -hangout ", { sort: "relevance", limit: 10 });
  const b = await Q("lake -hangout ", { sort: "updated", limit: 10, facets: false });
  const c = await Q("lake -hangout ", { sort: "relevance", limit: 10, offset: 5, facets: false });
  assert.deepEqual(c.idx, a.idx.slice(5).concat([]).slice(0, 5).length ? (await Q("lake -hangout ", { limit: 15 })).idx.slice(5) : []);
  assert.equal(a.total, b.total);
  assert.ok(b.ms < a.ms + 5);
  const fresh = await e.query({ q: "lake -hangout ", sort: "updated", limit: 10, filters: { cap_min: 0 } });
  assert.deepEqual(fresh.idx, b.idx);
});

test("phrases: adjacent words in a title/author/tag (punctuation ignored) or all words in the description", async () => {
  const wordsOf = (t: string) => t.normalize("NFKC").toLowerCase().match(/[\p{L}\p{N}\p{M}]+/gu) ?? [];
  const adjacent = (t: string, a: string, b: string) => { const w = wordsOf(t); return w.some((x, i) => x === a && w[i + 1] === b); };
  for (const [a, b] of [["mario", "kart"], ["particle", "live"], ["escape", "room"]]) {
    const phrase = await set(`"${a} ${b}"`);
    const and = await ids(`${a} ${b} `);
    assert.ok(phrase.size > 0 && phrase.size <= and.length, `${a} ${b}`);
    const infos = await e.info(and);
    let missed = 0;
    infos.forEach((w, k) => {
      const hit = adjacent(w.name, a, b) || adjacent(w.author, a, b) || w.tags.some((t) => adjacent(t, a, b));
      if (hit && !phrase.has(and[k])) missed++;
    });
    assert.equal(missed, 0, `${a} ${b}: title/author/tag adjacency must be in the phrase set`);
  }
  // punctuation between the words does not break the phrase
  const inf = await e.info(Array.from({ length: 128 * 60 }, (_, i) => i));
  const w = inf.find((x) => /^[A-Za-z]+[_\-][A-Za-z]+$/.test(x.name));
  if (w) { const [a, b] = wordsOf(w.name); assert.ok((await set(`"${a} ${b}"`)).size > 0); }
  assert.equal(await total('"zzqq xxyy"'), 0);
});

test("CJK a -b subtracts the negated term and keeps description matches", async () => {
  for (const a of ["ール", "ワー", "ルド", "神社"]) {
    const A = await T(a), B = await T(`${a} game`), D = await T(`${a} -game`);
    assert.equal(D, A - B, a);
  }
  assert.ok((await T("ール")) > 10000); // description-only matches count (no CJK stopword dropping)
});
