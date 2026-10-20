// Sidebar: uploads-over-time histogram + year range + presets, tag bar chart (+/-), platform, capacity range, size, content warnings, status, author.
import { dateOfDay, tagClass } from "../engine";
import type { FacetCounts } from "../engine";
import { authorWorlds } from "../data";
import type { Ctx } from "./ctx";
import { dualRange, DualRange } from "./dualrange";
import { histogram, Hist, ymLabel } from "./histogram";
import { facetCount } from "./state";
import { $, esc, fmt, label } from "./util";

export const CAP_STOPS = [1, 2, 4, 8, 16, 24, 32, 40, 50, 64, 80];
const SIZE_STOPS = [5, 10, 25, 50, 100, 200, 500, 1000]; // MB; one past the end = any
const EPOCH_YM = 2010 * 12;
const pad = (n: number) => String(n).padStart(2, "0");
const firstOf = (ym: number) => `${Math.floor(ym / 12)}-${pad((ym % 12) + 1)}-01`;
const lastOf = (ym: number) => new Date(Date.UTC(Math.floor(ym / 12), (ym % 12) + 1, 0)).toISOString().slice(0, 10);
const ymOfIso = (s: string) => parseInt(s.slice(0, 4), 10) * 12 + parseInt(s.slice(5, 7), 10) - 1;
const isoDay = (d: Date) => d.toISOString().slice(0, 10);

type Kind = "tag" | "cw" | "status" | "platform";
type St = "inc" | "exc" | "";

function frow(kind: Kind, key: string, name: string, count: number | null, max: number, st: St): string {
  const id = esc(kind === "tag" ? label(key) : key);
  const bar = count === null ? 0 : Math.max(2, Math.round((count / Math.max(1, max)) * 100));
  return `<li class="frow ${st}" data-kind="${kind}" data-k="${esc(key)}">
    <button type="button" class="pm plus" data-testid="${kind}-include-${id}" data-act="inc" aria-pressed="${st === "inc"}" title="only worlds with ${esc(name)}">+</button>
    <button type="button" class="pm minus" data-testid="${kind}-exclude-${id}" data-act="exc" aria-pressed="${st === "exc"}" title="hide worlds with ${esc(name)}">−</button>
    <span class="fname" title="${esc(name)}">${esc(name)}</span><span class="fbar"><i style="width:${bar}%"></i></span><small>${count === null ? "" : fmt(count)}</small></li>`;
}

export interface Facets {
  build(): void;
  render(fc: FacetCounts, total: number): Promise<void>;
  /** push state -> controls (after popstate / reset / pill edits) */
  sync(): void;
}

export function createFacets(ctx: Ctx, host: HTMLElement): Facets {
  const eng = ctx.eng;
  const S = () => ctx.state;
  let hist: Hist, years: DualRange, cap: DualRange, size: DualRange;
  let minDay = 0, maxDay = 0, startYM = 0, endYM = 0, y0 = 2017, y1 = 2026;
  let lastFc: FacetCounts | null = null;
  let tagsMore = false;

  function bounds() {
    const c = eng.c;
    let lo = 1e9, hi = 0;
    for (let i = 0; i < c.n; i++) {
      const a = c.created[i], u = c.updated[i];
      if (a > 0 && a < lo) lo = a;
      if (u > hi) hi = u;
      if (a > hi) hi = a;
    }
    minDay = lo; maxDay = eng.maxDay || hi;
    startYM = ymOfIso(dateOfDay(lo)); endYM = ymOfIso(dateOfDay(hi));
    y0 = Math.floor(startYM / 12); y1 = Math.floor(endYM / 12);
  }

  // ------------------------------------------------------------------ build
  function build() {
    bounds();
    host.innerHTML = `<div class="drawer-head"><b>Filters</b><button id="fdone" type="button">Show results</button></div>
    <section data-testid="facet-date" class="fs-date"><div class="fh"><h3>Uploads over time</h3>
        <div class="seg sm" id="f-datef"><button type="button" data-f="updated" data-testid="datef-updated">Updated</button><button type="button" data-f="published" data-testid="datef-published">Published</button></div></div>
      <div class="presets chips" id="f-presets"><button type="button" data-p="week" data-testid="date-preset-week" title="last 7 days of data">this week</button><button type="button" data-p="month" data-testid="date-preset-month" title="last 30 days of data">this month</button><button type="button" data-p="year" data-testid="date-preset-year" title="last 365 days of data">this year</button><button type="button" data-p="any" data-testid="date-preset-any">any</button></div>
      <div id="f-hist"></div><div id="f-years"></div><div class="rlabel" id="f-datelabel"></div></section>
    <section data-testid="facet-tags" class="fs-tags"><h3>Tags</h3><input id="f-tagq" type="search" placeholder="find a tag…" autocomplete="off" aria-label="find a tag">
      <ul id="f-tagres" class="flist found"></ul><ul id="f-tags" class="flist"></ul><button type="button" class="linkbtn" id="f-tagmore"></button></section>
    <section data-testid="facet-platform"><h3>Platform</h3><ul id="f-plat" class="flist"></ul></section>
    <section data-testid="facet-capacity"><div class="fh"><h3>Capacity</h3><span class="rlabel" id="f-caplabel"></span></div><div id="capbars" class="bars"></div><div id="f-cap"></div></section>
    <section data-testid="facet-size"><div class="fh"><h3>Download size</h3><span class="rlabel" id="f-sizelabel"></span></div><div id="f-sizer"></div>
      <div class="row"><span class="dim">≤</span><input id="f-size" type="number" min="0" step="1" placeholder="any" aria-label="max download size in MB"><span class="dim">MB</span></div></section>
    <section data-testid="facet-cw"><h3>Content warnings</h3><ul id="f-cw" class="flist"></ul></section>
    <section data-testid="facet-status"><h3>Status</h3><ul id="f-status" class="flist"></ul></section>
    <section data-testid="facet-author"><h3>Author</h3><div id="f-author"></div></section>
    <button id="f-reset" class="reset" type="button">Reset filters</button>`;

    hist = histogram({ testid: "date-hist", onBrush });
    $("#f-hist").appendChild(hist.el);
    const ysl = Array.from({ length: y1 - y0 + 1 }, (_, i) => "’" + String(y0 + i).slice(2));
    years = dualRange({ stops: ysl, tickEvery: ysl.length > 9 ? 2 : 1, testid: "date-years", onInput: onYears, onChange: () => ctx.change() });
    $("#f-years").appendChild(years.el);
    cap = dualRange({ stops: CAP_STOPS.map((v, i) => (i === CAP_STOPS.length - 1 ? v + "+" : String(v))), tickEvery: 1, testid: "cap-range", onInput: onCap });
    $("#f-cap").appendChild(cap.el);
    size = dualRange({ stops: [...SIZE_STOPS.map((v) => (v >= 1000 ? "1G" : String(v))), "any"], tickEvery: 1, testid: "size-range", onInput: onSize });
    // single-ended: hide the lower thumb
    size.el.classList.add("single");
    $("#f-sizer").appendChild(size.el);

    host.addEventListener("click", onClick);
    $("#f-datef").addEventListener("click", (e) => {
      const b = (e.target as HTMLElement).closest("button"); if (!b) return;
      S().datef = b.dataset.f as "updated" | "published"; ctx.change(); // histogram re-renders from the cached counts
      sync(); if (lastFc) paintHist(lastFc);
    });
    $("#f-presets").addEventListener("click", (e) => {
      const b = (e.target as HTMLElement).closest("button"); if (!b) return;
      preset(b.dataset.p!); sync(); ctx.change();
    });
    $("#f-size").addEventListener("input", (e) => { S().size_max_mb = (e.target as HTMLInputElement).value === "" ? undefined : Math.max(0, +(e.target as HTMLInputElement).value); sync(false); ctx.change(150); });
    $("#f-reset").addEventListener("click", () => {
      const s = S();
      Object.assign(s, { cap_min: undefined, cap_max: undefined, size_max_mb: undefined, platform: "", platform_ex: "", after: "", before: "", pub_after: "", pub_before: "", tags: [], tags_ex: [], author: "", status: [], status_ex: [], cw: [], cwx: [] });
      sync(); ctx.change();
    });
    $("#f-tagmore").addEventListener("click", () => { tagsMore = !tagsMore; if (lastFc) paintTags(lastFc); });
    $("#f-tagq").addEventListener("input", onTagSearch);
    $("#f-author").addEventListener("click", (e) => { if ((e.target as HTMLElement).closest("button")) { S().author = ""; ctx.change(); } });
    $("#fdone").addEventListener("click", () => document.body.classList.remove("drawer"));
    sync();
  }

  // ------------------------------------------------------------------ date
  const dfield = () => (S().datef === "published" ? (["pub_after", "pub_before"] as const) : (["after", "before"] as const));
  function onBrush(b: [number, number] | null) {
    const [ka, kb2] = dfield(), s = S() as any;
    if (!b) { s[ka] = ""; s[kb2] = ""; }
    else {
      const last = endYM - startYM;
      s[ka] = b[0] <= 0 ? "" : firstOf(startYM + b[0]);
      s[kb2] = b[1] >= last ? "" : lastOf(startYM + b[1]);
    }
    sync(true, true); ctx.change(120);
  }
  function onYears(a: number, b: number) {
    const [ka, kb2] = dfield(), s = S() as any, n = y1 - y0;
    s[ka] = a <= 0 ? "" : `${y0 + a}-01-01`;
    s[kb2] = b >= n ? "" : `${y0 + b}-12-31`;
    sync(true, false, true); ctx.change(150);
  }
  function preset(p: string) {
    const [ka, kb2] = dfield(), s = S() as any;
    s[kb2] = "";
    if (p === "any") { s[ka] = ""; return; }
    const days = p === "week" ? 6 : p === "month" ? 29 : 364;
    s[ka] = isoDay(new Date(Date.parse(dateOfDay(maxDay) + "T00:00:00Z") - days * 86400000));
  }
  function paintHist(fc: FacetCounts) {
    const src = S().datef === "published" ? fc.months : fc.updatedMonths;
    const counts = new Array(endYM - startYM + 1).fill(0);
    if (src) src.counts.forEach((v, k) => { const j = EPOCH_YM + src.m0 + k - startYM; if (j >= 0 && j < counts.length) counts[j] = v; });
    hist.setData({ startYM, counts });
    syncBrush();
  }
  function syncBrush() {
    const [ka, kb2] = dfield(), s = S() as any;
    const a: string = s[ka], b: string = s[kb2];
    if (!a && !b) hist.setBrush(null);
    else {
      const last = endYM - startYM;
      hist.setBrush([a ? Math.max(0, Math.min(last, ymOfIso(a) - startYM)) : 0, b ? Math.max(0, Math.min(last, ymOfIso(b) - startYM)) : last]);
    }
  }

  // ------------------------------------------------------------------ sync (state -> controls)
  function sync(withHist = true, skipHist = false, skipYears = false) {
    const s = S();
    $("#f-datef").querySelectorAll("button").forEach((b) => b.classList.toggle("on", (b as HTMLElement).dataset.f === s.datef));
    const [ka, kb2] = dfield(), a: string = (s as any)[ka], b: string = (s as any)[kb2];
    if (!skipYears) years.set(a ? Math.max(0, Math.min(y1 - y0, +a.slice(0, 4) - y0)) : 0, b ? Math.max(0, Math.min(y1 - y0, +b.slice(0, 4) - y0)) : y1 - y0);
    if (withHist && !skipHist) syncBrush();
    $("#f-datelabel").textContent = !a && !b ? "any time" : `${s.datef}: ${a || "…"} → ${b || "now"}`;
    const wk = isoDay(new Date(Date.parse(dateOfDay(maxDay) + "T00:00:00Z") - 6 * 86400000));
    const mo = isoDay(new Date(Date.parse(dateOfDay(maxDay) + "T00:00:00Z") - 29 * 86400000));
    const yr = isoDay(new Date(Date.parse(dateOfDay(maxDay) + "T00:00:00Z") - 364 * 86400000));
    $("#f-presets").querySelectorAll("button").forEach((x) => {
      const p = (x as HTMLElement).dataset.p;
      const on = p === "any" ? !a && !b : !b && a === (p === "week" ? wk : p === "month" ? mo : yr);
      x.classList.toggle("on", on);
    });
    // capacity
    const ci = (v: number | undefined, dflt: number) => { if (v === undefined) return dflt; let k = 0; CAP_STOPS.forEach((st, i) => { if (v >= st) k = i; }); return k; };
    const lo = ci(s.cap_min, 0), hi = s.cap_max === undefined ? CAP_STOPS.length - 1 : Math.max(lo, CAP_STOPS.findIndex((st) => st >= s.cap_max!) < 0 ? CAP_STOPS.length - 1 : CAP_STOPS.findIndex((st) => st >= s.cap_max!));
    cap.set(lo, hi);
    $("#f-caplabel").textContent = capLabel();
    // size
    const sv = s.size_max_mb === undefined ? SIZE_STOPS.length : Math.max(0, SIZE_STOPS.findIndex((v) => v >= s.size_max_mb!) < 0 ? SIZE_STOPS.length : SIZE_STOPS.findIndex((v) => v >= s.size_max_mb!));
    size.set(0, sv);
    $("#f-sizelabel").textContent = s.size_max_mb === undefined ? "any" : `≤ ${s.size_max_mb} MB`;
    const si = $<HTMLInputElement>("#f-size");
    if (document.activeElement !== si) si.value = s.size_max_mb === undefined ? "" : String(s.size_max_mb);
    ($("#fbtn") as HTMLElement).textContent = facetCount(s) ? `Filters (${facetCount(s)})` : "Filters";
  }
  function capLabel() {
    const s = S();
    if (s.cap_min === undefined && s.cap_max === undefined) return "any";
    return `${s.cap_min ?? 1} – ${s.cap_max ?? "80+"}`;
  }
  function onCap(a: number, b: number) {
    const s = S();
    s.cap_min = a <= 0 ? undefined : CAP_STOPS[a];
    s.cap_max = b >= CAP_STOPS.length - 1 ? undefined : CAP_STOPS[b];
    $("#f-caplabel").textContent = capLabel();
    ($("#fbtn") as HTMLElement).textContent = facetCount(s) ? `Filters (${facetCount(s)})` : "Filters";
    ctx.change(120);
  }
  function onSize(_a: number, b: number) {
    const s = S();
    s.size_max_mb = b >= SIZE_STOPS.length ? undefined : SIZE_STOPS[b];
    $("#f-sizelabel").textContent = s.size_max_mb === undefined ? "any" : `≤ ${s.size_max_mb} MB`;
    const si = $<HTMLInputElement>("#f-size"); si.value = s.size_max_mb === undefined ? "" : String(s.size_max_mb);
    ctx.change(120);
  }

  // ------------------------------------------------------------------ +/- toggling
  const flip = (arr: string[], k: string) => { const i = arr.indexOf(k); if (i >= 0) arr.splice(i, 1); else arr.push(k); };
  const drop = (arr: string[], k: string) => { const i = arr.indexOf(k); if (i >= 0) arr.splice(i, 1); };
  function toggle(kind: Kind, k: string, act: "inc" | "exc") {
    const s = S();
    if (kind === "tag") { if (act === "inc") { drop(s.tags_ex, k); flip(s.tags, k); } else { drop(s.tags, k); flip(s.tags_ex, k); } }
    else if (kind === "status") { if (act === "inc") { drop(s.status_ex, k); flip(s.status, k); } else { drop(s.status, k); flip(s.status_ex, k); } }
    else if (kind === "cw") { if (act === "inc") { drop(s.cwx, k); flip(s.cw, k); } else { drop(s.cw, k); flip(s.cwx, k); } }
    else {
      const inc = new Set<string>(s.platform === "both" ? ["pc", "quest"] : s.platform ? [s.platform] : []);
      if (act === "inc") { if (inc.has(k)) inc.delete(k); else inc.add(k); if (s.platform_ex === k) s.platform_ex = ""; }
      else { if (s.platform_ex === k) s.platform_ex = ""; else { s.platform_ex = k as "pc" | "quest"; inc.delete(k); } }
      s.platform = inc.size === 2 ? "both" : inc.size === 1 ? ([...inc][0] as "pc" | "quest") : "";
    }
  }
  function onClick(e: Event) {
    const b = (e.target as HTMLElement).closest("button.pm") as HTMLElement | null;
    if (b) {
      const li = b.closest("li") as HTMLElement;
      toggle(li.dataset.kind as Kind, li.dataset.k!, b.dataset.act as "inc" | "exc");
      if (li.dataset.kind === "tag" && li.closest("#f-tagres")) { $<HTMLInputElement>("#f-tagq").value = ""; $("#f-tagres").innerHTML = ""; }
      ctx.change();
      return;
    }
    const name = (e.target as HTMLElement).closest(".frow .fname, .frow .fbar, .frow small");
    if (name) { // clicking the label includes (the most common intent); minus is always explicit
      const li = name.closest("li") as HTMLElement;
      toggle(li.dataset.kind as Kind, li.dataset.k!, "inc"); ctx.change();
    }
  }

  // ------------------------------------------------------------------ render (data -> DOM)
  function stOf(kind: Kind, k: string): St {
    const s = S();
    if (kind === "tag") return s.tags.includes(k) ? "inc" : s.tags_ex.includes(k) ? "exc" : "";
    if (kind === "status") return s.status.includes(k) ? "inc" : s.status_ex.includes(k) ? "exc" : "";
    if (kind === "cw") return s.cw.includes(k) ? "inc" : s.cwx.includes(k) ? "exc" : "";
    const inc = s.platform === "both" || s.platform === k;
    return inc ? "inc" : s.platform_ex === k ? "exc" : "";
  }
  function paintTags(fc: FacetCounts) {
    const list = fc.userTagList ?? Object.entries(fc.userTags);
    const counts = new Map(list);
    const sel = [...S().tags, ...S().tags_ex];
    const rest = list.filter(([k]) => !sel.includes(k));
    const shown = rest.slice(0, tagsMore ? 40 : 14);
    const mx = Math.max(1, ...list.map((x) => x[1]));
    $("#f-tags").innerHTML = [...sel.map((k) => frow("tag", k, label(k), counts.get(k) ?? 0, mx, stOf("tag", k))), ...shown.map(([k, v]) => frow("tag", k, label(k), v, mx, stOf("tag", k)))].join("");
    const more = $("#f-tagmore");
    more.hidden = rest.length <= 14;
    more.textContent = tagsMore ? "fewer tags" : `more tags (${Math.min(40, rest.length) - 14})`;
  }
  async function render(fc: FacetCounts, total: number) {
    lastFc = fc;
    const s = S();
    paintHist(fc);
    paintTags(fc);
    const pm = Math.max(1, fc.platform.pc, fc.platform.quest);
    $("#f-plat").innerHTML = frow("platform", "pc", "PC", fc.platform.pc, pm, stOf("platform", "pc")) + frow("platform", "quest", "Quest", fc.platform.quest, pm, stOf("platform", "quest"));
    const cws = new Set([...Object.keys(fc.cw), ...s.cw, ...s.cwx]);
    const cmx = Math.max(1, ...Object.values(fc.cw));
    $("#f-cw").innerHTML = [...cws].sort((a, b) => (fc.cw[b] ?? 0) - (fc.cw[a] ?? 0)).map((k) => frow("cw", k, k, fc.cw[k] ?? 0, cmx, stOf("cw", k))).join("");
    const smx = Math.max(1, fc.status.approved ?? 0, fc.status.labs ?? 0);
    $("#f-status").innerHTML = ["approved", "labs"].map((k) => frow("status", k, k, fc.status[k] ?? 0, smx, stOf("status", k))).join("");
    // capacity histogram aligned to the slider stops
    const ch = fc.capacityHist ?? [];
    const bins = CAP_STOPS.map((st, i) => {
      const lo = i === 0 ? 0 : st, hi = i === CAP_STOPS.length - 1 ? 257 : CAP_STOPS[i + 1];
      let t = 0; for (let v = lo; v < hi && v < ch.length; v++) t += ch[v] || 0; return t;
    });
    const bm = Math.max(1, ...bins);
    $("#capbars").innerHTML = bins.map((v, i) => `<div title="${CAP_STOPS[i]}${i === bins.length - 1 ? "+" : "–" + (CAP_STOPS[i + 1] - 1)} players: ${v.toLocaleString()}"><i style="height:${Math.round((v / bm) * 100)}%"></i></div>`).join("");
    ($("#fdone") as HTMLElement).textContent = `Show ${total.toLocaleString()} results`;
    sync(false);
    const fa = $("#f-author");
    if (!s.author) { fa.textContent = "any (use the author link on a world)"; fa.className = "dim"; authorFor = ""; }
    else if (authorFor !== s.author) {
      authorFor = s.author;
      const first = (await authorWorlds(s.author))[0];
      const name = first === undefined ? s.author : (await eng.info([first]))[0].author;
      fa.className = ""; fa.innerHTML = `<button class="chipx on" type="button">${esc(name)} ✕</button>`;
    }
  }
  let authorFor = "";

  async function onTagSearch(e: Event) {
    const v = (e.target as HTMLInputElement).value.trim().toLowerCase().replace(/\s+/g, "_");
    const box = $("#f-tagres");
    if (!v) { box.innerHTML = ""; return; }
    const all = await eng.tagsAll();
    const grouped = new Map<string, number>();
    for (const [k, n] of all) {
      if (!k.toLowerCase().includes(v) || tagClass(k) !== "user") continue;
      const g = eng.groupOf.get(k);
      const key = g && eng.m.tagGroups[g] ? "~" + g : k;
      grouped.set(key, (grouped.get(key) ?? 0) + n);
    }
    const hits = [...grouped].sort((a, b) => (+label(b[0]).toLowerCase().startsWith(v) - +label(a[0]).toLowerCase().startsWith(v)) || b[1] - a[1]).slice(0, 10);
    const mx = Math.max(1, ...hits.map((h) => h[1]));
    box.innerHTML = hits.map(([k, n]) => frow("tag", k, label(k), n, mx, stOf("tag", k))).join("") || `<li class="dim">no tags match</li>`;
  }

  return { build, render, sync: () => { sync(); if (lastFc) { paintHist(lastFc); paintTags(lastFc); } } };
}
