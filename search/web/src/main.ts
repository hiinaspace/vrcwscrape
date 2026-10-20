import "./style.css";
import type { Sem } from "./sem";
import { Engine, Filters, QueryIn, Sort, FacetCounts, dateOfDay, tagClass } from "./engine";
import { bytesFetched, cardChunk, WorldInfo } from "./data";

declare global {
  interface Window { vrcwSearch: any; vrcwSearchReady: Promise<any>; vrcwStats: any }
}

interface State {
  q: string; cap_min?: number; cap_max?: number; size_max_mb?: number; platform: "" | "pc" | "quest" | "both";
  after: string; before: string; tags: string[]; author: string; sort: Sort; world: string;
  status: string[]; cw: string[]; cwx: string[]; adult: boolean;
}

const $ = <T extends HTMLElement>(s: string) => document.querySelector(s) as T;
const esc = (s: string) => s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]!));
const num = (v: string | null) => (v === null || v === "" || isNaN(+v) ? undefined : +v);
const SORTS: Sort[] = ["relevance", "text", "visits", "favorites", "updated", "published"];
const SORT_LABEL: Record<string, string> = { relevance: "relevance (popularity-weighted)", text: "best text match", visits: "visits", favorites: "favorites", updated: "recently updated", published: "recently published" };
const lsGet = (k: string) => { try { return localStorage.getItem(k); } catch { return null; } };
const lsSet = (k: string, v: string) => { try { localStorage.setItem(k, v); } catch { /* private mode */ } };

const csv = (v: string | null) => (v ?? "").split(",").map((s) => s.trim()).filter(Boolean);
function readUrl(): State {
  const p = new URLSearchParams(location.search);
  const sort = p.get("sort") as Sort;
  const pf = p.get("platform");
  return {
    q: p.get("q") ?? "", cap_min: num(p.get("cap_min")), cap_max: num(p.get("cap_max")), size_max_mb: num(p.get("size_max_mb")),
    platform: pf === "pc" || pf === "quest" || pf === "both" ? pf : "",
    after: p.get("after") ?? "", before: p.get("before") ?? "",
    tags: csv(p.get("tags")), adult: p.get("adult") === "1" || (p.get("adult") === null && lsGet("vrcw-adult") === "1"), status: csv(p.get("status")), cw: csv(p.get("cw")), cwx: csv(p.get("cw_exclude")),
    author: p.get("author") ?? "", sort: SORTS.includes(sort) ? sort : "relevance", world: p.get("world") ?? "",
  };
}
function toUrl(s: State): string {
  const p = new URLSearchParams();
  if (s.q) p.set("q", s.q);
  if (s.cap_min !== undefined) p.set("cap_min", String(s.cap_min));
  if (s.cap_max !== undefined) p.set("cap_max", String(s.cap_max));
  if (s.size_max_mb !== undefined) p.set("size_max_mb", String(s.size_max_mb));
  if (s.platform) p.set("platform", s.platform);
  if (s.after) p.set("after", s.after);
  if (s.before) p.set("before", s.before);
  if (s.tags.length) p.set("tags", s.tags.join(","));
  if (s.author) p.set("author", s.author);
  if (s.status.length) p.set("status", s.status.join(","));
  if (s.cw.length) p.set("cw", s.cw.join(","));
  if (s.cwx.length) p.set("cw_exclude", s.cwx.join(","));
  if (s.adult) p.set("adult", "1");
  if (s.sort !== "relevance") p.set("sort", s.sort);
  if (s.world) p.set("world", s.world);
  const q = p.toString();
  return location.pathname + (q ? "?" + q : "");
}
const ADULT = ["adult", "sex"]; // content_adult / content_sex
const filtersOf = (s: State): Filters => ({
  cap_min: s.cap_min, cap_max: s.cap_max, size_max_mb: s.size_max_mb, platform: s.platform, after: s.after || undefined,
  before: s.before || undefined, tags: s.tags, author: s.author || undefined,
  status: s.status, cw: s.cw,
  cw_exclude: [...new Set([...s.cwx, ...(s.adult ? [] : ADULT.filter((a) => !s.cw.includes(a)))])],
});
const fmt = (n: number) => (n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e4 ? Math.round(n / 1e3) + "k" : n >= 1e3 ? (n / 1e3).toFixed(1) + "k" : String(n));
const label = (k: string) => (k.startsWith("~") ? k.slice(1) : k);
const kb = (v: number) => (v === 0xffffffff ? "–" : v >= 1024 ? (v / 1024).toFixed(v >= 10240 ? 0 : 1) + " MB" : v + " KB");

const eng = new Engine();
let state = readUrl();
let shown = 24;
let seq = 0;
let facetsBuilt = false;

let resolveReady!: (v: any) => void;
window.vrcwSearchReady = new Promise((r) => (resolveReady = r));
window.vrcwStats = bytesFetched;

function normFilters(f: any): Filters {
  const o: Filters = { ...f };
  for (const k of ["tags", "status", "cw", "cw_exclude"] as const) if (typeof f?.[k] === "string") o[k] = (f[k] as string).split(",").filter(Boolean);
  for (const k of ["cap_min", "cap_max", "size_max_mb"] as const) if (o[k] === null || (o[k] as any) === "") delete o[k];
  return o;
}

function setStatus(t: string) { $("#status").textContent = t; }

async function boot() {
  const t0 = performance.now();
  setStatus("loading index…");
  // start the text shard early so it overlaps with core column download
  await eng.init();
  const info = {
    query: async (inp: QueryIn) => {
      const t1 = performance.now();
      const r = await eng.run({ q: inp.q ?? "", filters: normFilters(inp.filters ?? {}), sort: inp.sort ?? "relevance", limit: inp.limit ?? 50 });
      const infos = await eng.info(r.idx);
      return { ids: infos.map((i) => i.id), total: r.total, facetCounts: r.facetCounts, ms: Math.round((performance.now() - t1) * 100) / 100 };
    },
    semantic: async (q: string, k = 20) => {
      const sm = await ensureSem();
      const t1 = performance.now();
      const idx = sm.search(q, k, null, eng.lp);
      (window.vrcwStats.semMs ??= []).push(Math.round((performance.now() - t1) * 10) / 10);
      return (await eng.info(idx)).map((x) => x.id);
    },
    _semEncode: async (q: string) => Array.from((await ensureSem()).encode(q)),
    similar: async (worldId: string, k = 20) => {
      const i = await eng.idToIdx(worldId);
      if (i === undefined) return [];
      const nb = await eng.similar(i, k);
      return (await eng.info(nb)).map((x) => x.id);
    },
  };
  window.vrcwSearch = info;
  buildFacets();
  $("#sem-toggle").addEventListener("change", async (e) => {
    const cb = e.target as HTMLInputElement;
    if (!cb.checked) { sem = null; $("#semantic").hidden = true; $("#sem-note").textContent = ""; return; }
    if (!confirm("Semantic search downloads about 33 MB of model data (cached afterwards). Continue?")) { cb.checked = false; return; }
    if (semLoading) return;
    semLoading = true;
    const t0 = performance.now();
    try {
      await ensureSem((d, t) => ($("#sem-note").textContent = `downloading ${d}/${t}…`));
      $("#sem-note").textContent = `ready (${((performance.now() - t0) / 1000).toFixed(1)} s)`;
      refresh();
    } catch (err) { cb.checked = false; $("#sem-note").textContent = "failed: " + (err as Error).message; }
    semLoading = false;
  });
  await refresh();
  setStatus(`${eng.c.n.toLocaleString()} worlds · ready in ${Math.round(performance.now() - t0)} ms`);
  resolveReady(info);
  if (state.world) openCard(state.world, false);
}

// ---------------- UI
function commit(push = false) {
  const u = toUrl(state);
  if (u !== location.pathname + location.search) (push ? history.pushState : history.replaceState).call(history, null, "", u);
}

let timer: any;
function onChange(debounce = 0) {
  clearTimeout(timer);
  timer = setTimeout(() => { shown = 24; commit(); refresh(); }, debounce);
}

async function refresh() {
  const my = ++seq;
  const r = await eng.run({ q: state.q, filters: filtersOf(state), sort: state.sort, limit: shown, exactTail: false, whatIf: true });
  if (my !== seq) return;
  const infos = await eng.info(r.idx);
  if (my !== seq) return;
  renderResults(r.idx, infos, r.total, r.ms);
  renderFacets(r.facetCounts, r.total);
  if (sem) renderSemantic(my, new Set(r.idx));
}

function itemHtml(i: number, w: WorldInfo, tid = "result-item"): string {
  const c = eng.c;
  const badge = (on: boolean, name: string, size: number) => (on ? `<span class="pb ${name.toLowerCase()}">${name}${size !== 0xffffffff ? " " + kb(size) : ""}</span>` : "");
  const plats = badge(!!(c.plat[i] & 1), "PC", c.pc[i]) + badge(!!(c.plat[i] & 2), "Quest", c.quest[i]) || `<span class="pb">no package</span>`;
  const date = state.sort === "published" ? "pub " + dateOfDay(c.created[i]) : "upd " + dateOfDay(c.updated[i]);
  const thumb = w.thumb
    ? `<img decoding="async" data-src="${esc(w.thumb)}" alt="" referrerpolicy="no-referrer">`
    : `<div class="ph" style="--h:${(parseInt(w.id.slice(5, 9), 16) % 360) || 0}"></div>`;
  return `<li data-testid="${tid}" data-id="${esc(w.id)}"><a href="?world=${esc(w.id)}" class="thumb">${thumb}</a>
    <div class="body"><a class="title" href="?world=${esc(w.id)}">${esc(w.name || "(untitled)")}</a>
    <div class="by">${esc(w.author)}</div>
    <div class="facts">cap ${c.cap[i]} · ${fmt(c.visits[i])} visits · ${fmt(c.fav[i])} ♥ · ${date}</div><div class="plats">${plats}</div></div></li>`;
}

function renderResults(idx: number[], infos: WorldInfo[], total: number, ms: number) {
  $("#summary").textContent = `${total.toLocaleString()} worlds · ${ms.toFixed(0)} ms`;
  $("#results").innerHTML = idx.map((i, j) => itemHtml(i, infos[j])).join("");
  lazyThumbs($("#results"));
  ($("#more") as HTMLButtonElement).hidden = idx.length >= total;
}

// Thumbnails are ~75 KB PNGs hotlinked from vrchat.com: load only the ones in view
// immediately.
let thumbObs: IntersectionObserver | null = null;
function lazyThumbs(root: ParentNode) {
  thumbObs ??= new IntersectionObserver((es) => {
    for (const e of es) {
      if (!e.isIntersecting) continue;
      const img = e.target as HTMLImageElement;
      thumbObs!.unobserve(img);
      if (img.dataset.src) img.src = img.dataset.src;
    }
  }, { rootMargin: "100px" });
  root.querySelectorAll<HTMLImageElement>("img[data-src]").forEach((i) => thumbObs!.observe(i));
}

// ---- opt-in semantic search
let sem: Sem | null = null;
let semLoading = false;
async function ensureSem(progress?: (d: number, t: number) => void): Promise<Sem> {
  if (!sem) {
    const { Sem } = await import("./sem");
    sem = await Sem.load(progress);
  }
  return sem;
}
async function renderSemantic(my: number, shownIds: Set<number>) {
  const box = $("#semantic");
  if (!sem || state.q.trim().length < 2) { box.hidden = true; return; }
  const t0 = performance.now();
  const allow = (await eng.allowMask(filtersOf(state))).slice();
  const hits = sem.search(state.q, 40, allow, eng.lp).filter((i) => !shownIds.has(i)).slice(0, 8);
  const ms = performance.now() - t0;
  const infos = await eng.info(hits);
  if (my !== seq) return;
  box.hidden = false;
  $("#sem-list").innerHTML = hits.map((i, j) => itemHtml(i, infos[j], "semantic-item")).join("");
  lazyThumbs($("#sem-list"));
  $("#sem-ms").textContent = `${ms.toFixed(0)} ms`;
}

$("#results").addEventListener("click", (e) => {
  const a = (e.target as HTMLElement).closest("a[href^='?world=']") as HTMLAnchorElement | null;
  if (!a || (e as MouseEvent).ctrlKey || (e as MouseEvent).metaKey) return;
  e.preventDefault();
  openCard(a.getAttribute("href")!.slice(7), true);
});
$("#more").addEventListener("click", () => { shown += 24; refresh(); });

// facets
function buildFacets() {
  const f = $("#facets");
  const s = state;
  f.innerHTML = `<div class="drawer-head"><b>Filters</b><button id="fdone" type="button">Show results</button></div>
  
  <section data-testid="facet-sort"><h3>Sort</h3><select id="f-sort">${SORTS.map((x) => `<option value="${x}" ${x === s.sort ? "selected" : ""}>${SORT_LABEL[x]}</option>`).join("")}</select></section>
  <section data-testid="facet-platform"><h3>Platform</h3><div class="seg" id="f-plat">${["", "pc", "quest", "both"].map((p) => `<button data-v="${p}" class="${p === s.platform ? "on" : ""}">${p || "any"}<small></small></button>`).join("")}</div></section>
  <section data-testid="facet-capacity"><h3>Capacity</h3><div class="row"><input id="f-capmin" type="number" min="0" placeholder="min" value="${s.cap_min ?? ""}"><span>–</span><input id="f-capmax" type="number" min="0" placeholder="max" value="${s.cap_max ?? ""}"></div><div id="capbars" class="bars"></div></section>
  <section data-testid="facet-size"><h3>Download size ≤ (MB)</h3><div class="row"><input id="f-size" type="number" min="0" step="1" placeholder="any" value="${s.size_max_mb ?? ""}"><input id="f-sizer" type="range" min="0" max="500" step="5" value="${s.size_max_mb ?? 500}"></div></section>
  <section data-testid="facet-date"><h3>Updated</h3><div class="row"><input id="f-after" type="date" value="${s.after}"><span>–</span><input id="f-before" type="date" value="${s.before}"></div></section>
  <section data-testid="facet-author"><h3>Author</h3><div id="f-author"></div></section>
  <section data-testid="facet-status"><h3>Status</h3><div class="chips" id="f-status">${["approved", "labs"].map((x) => `<button data-s="${x}" class="${s.status.includes(x) ? "on" : ""}">${x} <small></small></button>`).join("")}</div></section>
  <section data-testid="facet-cw"><h3>Content warnings</h3><p class="hint">click: hide → require → off</p><ul id="f-cw" class="taglist cw"></ul></section>
  <section data-testid="facet-tags"><h3>Tags</h3><div id="f-sel" class="chips"></div><input id="f-tagq" type="search" placeholder="find tag…" autocomplete="off"><div id="f-tagres" class="chips found"></div><ul id="f-tags" class="taglist"></ul></section>
  <button id="f-reset" class="reset">Reset filters</button>`;
  const q = $<HTMLInputElement>("#q");
  q.value = s.q;
  q.addEventListener("input", () => { state.q = q.value; onChange(90); });
  const at = $<HTMLInputElement>("#adult-toggle");
  at.checked = state.adult;
  at.addEventListener("change", () => { state.adult = at.checked; lsSet("vrcw-adult", at.checked ? "1" : "0"); onChange(); });
  $("#f-sort").addEventListener("change", (e) => { state.sort = (e.target as HTMLSelectElement).value as Sort; onChange(); });
  $("#f-plat").addEventListener("click", (e) => {
    const b = (e.target as HTMLElement).closest("button"); if (!b) return;
    state.platform = b.dataset.v as State["platform"]; markPlat(); onChange();
  });
  const numIn = (id: string, key: "cap_min" | "cap_max" | "size_max_mb") =>
    $<HTMLInputElement>(id).addEventListener("input", (e) => { state[key] = num((e.target as HTMLInputElement).value); if (key === "size_max_mb") $<HTMLInputElement>("#f-sizer").value = String(state[key] ?? 500); onChange(150); });
  numIn("#f-capmin", "cap_min"); numIn("#f-capmax", "cap_max"); numIn("#f-size", "size_max_mb");
  $("#f-sizer").addEventListener("input", (e) => {
    const v = +(e.target as HTMLInputElement).value;
    state.size_max_mb = v >= 500 ? undefined : v; $<HTMLInputElement>("#f-size").value = state.size_max_mb === undefined ? "" : String(v); onChange(60);
  });
  $("#f-after").addEventListener("change", (e) => { state.after = (e.target as HTMLInputElement).value; onChange(); });
  $("#f-before").addEventListener("change", (e) => { state.before = (e.target as HTMLInputElement).value; onChange(); });
  $("#fbtn").addEventListener("click", () => document.body.classList.toggle("drawer"));
  $("#fdone").addEventListener("click", () => document.body.classList.remove("drawer"));
  $("#f-status").addEventListener("click", (e) => {
    const b = (e.target as HTMLElement).closest("button"); if (!b) return;
    const v = b.dataset.s!; const i = state.status.indexOf(v);
    if (i >= 0) state.status.splice(i, 1); else state.status.push(v);
    onChange();
  });
  $("#f-cw").addEventListener("click", (e) => {
    const li = (e.target as HTMLElement).closest("li[data-cw]") as HTMLElement | null; if (!li) return;
    const k = li.dataset.cw!;
    const hid = state.cwx.indexOf(k), req = state.cw.indexOf(k);
    if (hid >= 0) { state.cwx.splice(hid, 1); state.cw.push(k); }
    else if (req >= 0) state.cw.splice(req, 1);
    else state.cwx.push(k);
    onChange();
  });
  $("#f-reset").addEventListener("click", () => {
    state = { ...state, cap_min: undefined, cap_max: undefined, size_max_mb: undefined, platform: "", after: "", before: "", tags: [], author: "", status: [], cw: [], cwx: [] };
    syncInputs(); onChange();
  });
  $("#f-tags").addEventListener("click", (e) => {
    const li = (e.target as HTMLElement).closest("li[data-t]") as HTMLElement | null; if (li) toggleTag(li.dataset.t!);
  });
  $("#f-sel").addEventListener("click", (e) => { const b = (e.target as HTMLElement).closest("[data-t]") as HTMLElement | null; if (b) toggleTag(b.dataset.t!); });
  $("#f-tagres").addEventListener("click", (e) => { const b = (e.target as HTMLElement).closest("[data-t]") as HTMLElement | null; if (b) { toggleTag(b.dataset.t!); $<HTMLInputElement>("#f-tagq").value = ""; $("#f-tagres").innerHTML = ""; } });
  $("#f-author").addEventListener("click", (e) => { if ((e.target as HTMLElement).closest("button")) { state.author = ""; onChange(); } });
  $("#f-tagq").addEventListener("input", async (e) => {
    const v = (e.target as HTMLInputElement).value.trim().toLowerCase().replace(/\s+/g, "_");
    if (!v) { $("#f-tagres").innerHTML = ""; return; }
    const all = await eng.tagsAll();
    const grouped = new Map<string, [string, number]>();
    for (const [k, n] of all) {
      if (!k.toLowerCase().includes(v) || tagClass(k) !== "user") continue;
      const g = eng.groupOf.get(k);
      const key = g && eng.m.tagGroups[g] ? "~" + g : k;
      const e = grouped.get(key);
      if (e) e[1] += n; else grouped.set(key, [key, n]);
    }
    const hits = [...grouped.values()].sort((a, b) => (+label(b[0]).startsWith(v) - +label(a[0]).startsWith(v)) || b[1] - a[1]).slice(0, 14);
    $("#f-tagres").innerHTML = hits.map(([k, n]) => `<button data-t="${esc(k)}">${esc(label(k))} <small>${n}</small></button>`).join("") || "<em>no tags</em>";
  });
  window.addEventListener("popstate", () => { state = readUrl(); syncInputs(); refresh(); if (state.world) openCard(state.world, false); else closeCard(false); });
  $("#more");
  facetsBuilt = true;
}
function markPlat() { document.querySelectorAll("#f-plat button").forEach((b) => b.classList.toggle("on", (b as HTMLElement).dataset.v === state.platform)); }
function syncInputs() {
  const set = (id: string, v: any) => ($<HTMLInputElement>(id).value = v ?? "");
  set("#q", state.q); $<HTMLInputElement>("#adult-toggle").checked = state.adult; set("#f-capmin", state.cap_min); set("#f-capmax", state.cap_max); set("#f-size", state.size_max_mb);
  set("#f-sizer", state.size_max_mb ?? 500); set("#f-after", state.after); set("#f-before", state.before); set("#f-sort", state.sort);
  markPlat();
}
function toggleTag(t: string) {
  const i = state.tags.indexOf(t);
  if (i >= 0) state.tags.splice(i, 1); else state.tags.push(t);
  onChange();
}

let authorLabelFor = "";
async function renderFacets(fc: FacetCounts, total: number) {
  if (!facetsBuilt) return;
  const plat = $("#f-plat").querySelectorAll("button");
  const pi = fc.platformIf ?? { any: total, ...fc.platform };
  const pc = [pi.any, pi.pc, pi.quest, pi.both];
  plat.forEach((b, i) => (b.querySelector("small")!.textContent = " " + fmt(pc[i])));
  const mx = Math.max(1, ...Object.values(fc.capacity));
  $("#capbars").innerHTML = Object.entries(fc.capacity).map(([k, v]) => `<div title="${k}: ${v}"><i style="height:${Math.round((v / mx) * 100)}%"></i><b>${k}</b></div>`).join("");
  $("#f-sel").innerHTML = state.tags.map((t) => `<button data-t="${esc(t)}" class="on">${esc(label(t))} ✕</button>`).join("");
  $("#f-tags").innerHTML = Object.entries(fc.userTags).filter(([k]) => !state.tags.includes(k)).slice(0, 30)
    .map(([k, v]) => `<li data-t="${esc(k)}"><span>${esc(label(k))}</span><small>${fmt(v)}</small></li>`).join("");
  $("#f-status").querySelectorAll("button").forEach((b) => {
    const k = (b as HTMLElement).dataset.s as "approved" | "labs";
    b.classList.toggle("on", state.status.includes(k));
    b.querySelector("small")!.textContent = fmt(fc.status[k] ?? 0);
  });
  const names = new Set([...Object.keys(fc.cw).filter((k) => state.adult || !ADULT.includes(k)), ...state.cw, ...state.cwx]);
  $("#f-cw").innerHTML = [...names].sort((a, b) => (fc.cw[b] ?? 0) - (fc.cw[a] ?? 0)).map((k) => {
    const st = state.cwx.includes(k) ? "hide" : state.cw.includes(k) ? "req" : "";
    return `<li data-cw="${esc(k)}" class="${st}"><span>${st === "hide" ? "− " : st === "req" ? "+ " : ""}${esc(k)}</span><small>${fmt(fc.cw[k] ?? 0)}</small></li>`;
  }).join("");
  const nAct = (state.cap_min !== undefined || state.cap_max !== undefined ? 1 : 0) + (state.size_max_mb !== undefined ? 1 : 0) + (state.platform ? 1 : 0)
    + (state.after || state.before ? 1 : 0) + state.tags.length + (state.author ? 1 : 0) + state.status.length + state.cw.length + state.cwx.length;
  $("#fbtn").textContent = nAct ? `Filters (${nAct})` : "Filters";
  $("#fdone").textContent = `Show ${total.toLocaleString()} results`;
  const fa = $("#f-author");
  if (!state.author) { fa.textContent = "any (click an author on a world card)"; authorLabelFor = ""; }
  else if (authorLabelFor !== state.author) {
    authorLabelFor = state.author;
    const idx = (await import("./data")).authorWorlds(state.author);
    const first = (await idx)[0];
    const name = first === undefined ? state.author : (await eng.info([first]))[0].author;
    fa.innerHTML = `<button class="on">${esc(name)} ✕</button>`;
  }
}

// ---------------- world card
let cardSeq = 0;
async function openCard(id: string, push: boolean) {
  const my = ++cardSeq;
  state.world = id;
  if (push) commit(true);
  const idx = await eng.idToIdx(id);
  const root = $("#card-root");
  if (idx === undefined) { root.innerHTML = `<div class="overlay"><div class="card" data-testid="world-card"><button class="x">✕</button><p>World not found: ${esc(id)}</p></div></div>`; wireClose(); return; }
  const [w] = await eng.info([idx]);
  const c = eng.c;
  const cc = await cardChunk(Math.floor(idx / eng.m.cardChunk));
  if (my !== cardSeq) return;
  const j = idx % eng.m.cardChunk;
  const alltags = cc.t[j] ?? [];
  const tags = alltags.filter((t) => tagClass(t) === "user");
  const badges = alltags.filter((t) => tagClass(t) === "cw" || tagClass(t) === "status");
  const row = (k: string, v: string) => `<tr><th>${k}</th><td>${v}</td></tr>`;
  root.innerHTML = `<div class="overlay"><article class="card" data-testid="world-card" data-id="${esc(w.id)}">
    <button class="x" aria-label="close">✕</button>
    <div class="hero">${w.thumb ? `<img src="${esc(w.thumb.replace(/\/256$/, "/512"))}" data-fb="${esc(w.thumb)}" onerror="if(this.dataset.fb&&this.src!==this.dataset.fb)this.src=this.dataset.fb" alt="" referrerpolicy="no-referrer">` : `<div class="ph" style="--h:${parseInt(w.id.slice(5, 9), 16) % 360}"></div>`}</div>
    <h2>${esc(w.name)}</h2>
    <p class="by">by <a href="#" id="c-author">${esc(w.author)}</a> · <a href="https://vrchat.com/home/world/${esc(w.id)}" target="_blank" rel="noopener">open on vrchat.com ↗</a></p>
    <table>${row("Capacity", String(c.cap[idx]))}${row("Visits", c.visits[idx].toLocaleString())}${row("Favorites", c.fav[idx].toLocaleString())}
      ${row("PC package", c.plat[idx] & 1 ? kb(c.pc[idx]) : "none")}${row("Quest package", c.plat[idx] & 2 ? kb(c.quest[idx]) : "none")}
      ${row("Published", dateOfDay(c.created[idx]))}${row("Updated", dateOfDay(c.updated[idx]))}${row("World id", `<code>${esc(w.id)}</code>`)}</table>
    <div class="badges">${badges.map((t) => `<span class="badge ${tagClass(t)}">${esc(t.replace(/^(content_|system_)/, ""))}</span>`).join("")}</div>
    <div class="chips">${tags.map((t) => `<button data-t="${esc(t)}">${esc(t)}</button>`).join("")}</div>
    <p class="desc">${esc(cc.d[j] ?? "")}</p>
    <h3>Similar worlds</h3><ol data-testid="similar-list" class="similar"><li class="dim">loading…</li></ol></article></div>`;
  wireClose();
  $("#c-author").addEventListener("click", (e) => { e.preventDefault(); state.author = w.authorId; closeCard(true); syncInputs(); onChange(); });
  root.querySelectorAll(".chips [data-t]").forEach((b) => b.addEventListener("click", () => { const t = (b as HTMLElement).dataset.t!; if (!state.tags.includes(t)) state.tags.push(t); closeCard(true); onChange(); }));
  const nb = await eng.similar(idx, 20);
  const list = $("[data-testid=similar-list]");
  if (my !== cardSeq) return;
  if (!nb.length) { list.innerHTML = `<li class="dim">no neighbor data for this world</li>`; return; }
  const infos = await eng.info(nb);
  list.innerHTML = infos.map((x) => `<li data-id="${esc(x.id)}"><a href="?world=${esc(x.id)}">${esc(x.name)}</a> <small>${esc(x.author)}</small></li>`).join("");
  list.addEventListener("click", (e) => {
    const a = (e.target as HTMLElement).closest("a"); if (!a) return;
    e.preventDefault(); openCard(a.getAttribute("href")!.slice(7), true);
  });
}
function wireClose() {
  const ov = $(".overlay");
  ov.addEventListener("click", (e) => { if (e.target === ov || (e.target as HTMLElement).closest(".x")) closeCard(true); });
}
function closeCard(push: boolean) {
  cardSeq++;
  state.world = "";
  $("#card-root").innerHTML = "";
  if (push) commit(true);
}
window.addEventListener("keydown", (e) => { if (e.key === "Escape" && state.world) closeCard(true); });

boot().catch((e) => { console.error(e); setStatus("failed to load: " + e.message); });
