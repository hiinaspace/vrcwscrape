import "./style.css";
import type { Sem } from "./sem";
import { Engine, Filters, QueryIn, Sort, FacetCounts } from "./engine";
import { authorWorlds, bytesFetched, cardChunk } from "./data";
import { parseQuery } from "./query";
import type { Ctx } from "./ui/ctx";
import { createDetail } from "./ui/detail";
import { createFacets } from "./ui/facets";
import { Meters } from "./ui/meters";
import { createPills, createSyntaxHelp, setAuthorName } from "./ui/pills";
import { RowCard, fillCards, rowHtml } from "./ui/rows";
import { semUi, SemUi } from "./ui/semui";
import { SORTS, State, facetCount, filtersOf, readUrl, toUrl } from "./ui/state";
import { initThumbs, lazyThumbs } from "./ui/thumbs";
import { $, esc } from "./ui/util";

declare global {
  interface Window { vrcwSearch: any; vrcwSearchReady: Promise<any>; vrcwStats: any }
}

const PAGE = 24;
const SORT_LABEL: Record<string, string> = {
  trending: "trending", relevance: "relevance", visits: "visits", favorites: "favorites",
  updated: "updated", published: "published", capacity: "capacity",
};
const SORT_TIP: Record<string, string> = {
  trending: "recent visits per day", relevance: "text match x popularity", visits: "all-time visits", favorites: "favorites",
  updated: "last updated", published: "first published", capacity: "player capacity",
};
const NO_DIR = new Set<string>(["trending", "relevance", "text"]);

const eng = new Engine();
let shown = PAGE;
let seq = 0;
let semOn = false;
let sem: Sem | null = null;
let semCtl: SemUi;

const ctx = { eng, state: readUrl(), change } as unknown as Ctx;
let facets: ReturnType<typeof createFacets>;
let pills: ReturnType<typeof createPills>;
let detail: ReturnType<typeof createDetail>;

let resolveReady!: (v: any) => void;
window.vrcwSearchReady = new Promise((r) => (resolveReady = r));
window.vrcwStats = bytesFetched;

function normFilters(f: any): Filters {
  const o: Filters = { ...f };
  for (const k of ["tags", "tags_ex", "status", "status_ex", "cw", "cw_exclude"] as const) if (typeof f?.[k] === "string") o[k] = (f[k] as string).split(",").filter(Boolean);
  for (const k of ["cap_min", "cap_max", "size_max_mb"] as const) if (o[k] === null || (o[k] as any) === "") delete o[k];
  return o;
}
function setStatus(t: string) { $("#status").textContent = t; }

/** sort in effect: the user's explicit choice sticks; otherwise trending without a positive text condition, relevance with one */
function effSort(s: State = ctx.state): Sort {
  if (s.sort) return s.sort;
  const pos = parseQuery(s.q).pills.some((p) => !p.negated && (p.kind === "term" || p.kind === "phrase" || p.kind === "group" || (p.kind === "field" && (p.field === "title" || p.field === "author"))));
  return pos ? "relevance" : "trending";
}

async function boot() {
  const t0 = performance.now();
  setStatus("loading index…");
  await eng.init();
  ctx.meters = new Meters(eng.c);
  initThumbs(eng);
  const info = {
    query: async (inp: QueryIn) => {
      const t1 = performance.now();
      const r = await eng.query({ ...inp, q: inp.q ?? "", filters: normFilters(inp.filters ?? {}), sort: inp.sort ?? "relevance", limit: inp.limit ?? 50 });
      const infos = await eng.info(r.idx);
      return { ids: infos.map((i) => i.id), total: r.total, facetCounts: r.facetCounts, ms: Math.round((performance.now() - t1) * 100) / 100, parse: r.parse, textRanked: r.textRanked };
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
      return (await eng.info(await eng.similar(i, k))).map((x) => x.id);
    },
  };
  window.vrcwSearch = info;

  // ---- UI modules
  facets = createFacets(ctx, $("#facets"));
  facets.build();
  pills = createPills(ctx, $("#pills"), () => { $<HTMLInputElement>("#q").value = ctx.state.q; facets.sync(); change(); });
  detail = createDetail(ctx, $("#detail"), {
    commit, mark,
    addTag: (t) => { const s = ctx.state; if (!s.tags.includes(t)) s.tags.push(t); s.tags_ex = s.tags_ex.filter((x) => x !== t); if (!wide()) detail.close(true); change(); },
    setAuthor: (a) => { ctx.state.author = a; if (!wide()) detail.close(true); change(); },
  });
  const q = $<HTMLInputElement>("#q");
  q.value = ctx.state.q;
  q.addEventListener("input", () => { ctx.state.q = q.value; change(90); });
  createSyntaxHelp($("#syntax-btn"), $("#syntax-pop"), (ex) => { ctx.state.q = ctx.state.q.trim() ? ctx.state.q.trim() + " " + ex : ex; q.value = ctx.state.q; q.focus(); change(); });
  semCtl = semUi($("#sem-host"), $("#sem-panel"), {
    load: async (progress) => { await ensureSem(progress); },
    unload: () => { sem = null; },
    onState: (s) => { semOn = s === "on"; if (booted) void refresh(); },
  });
  $("#fbtn").addEventListener("click", () => document.body.classList.toggle("drawer"));
  $("#more").addEventListener("click", () => void loadMore());
  $("#sorts").addEventListener("click", (e) => {
    const b = (e.target as HTMLElement).closest("button[data-sort]") as HTMLElement | null; if (!b) return;
    const s = ctx.state, v = b.dataset.sort as Sort;
    if (effSort() === v && !NO_DIR.has(v)) s.asc = !s.asc; else s.asc = false;
    s.sort = v;
    change();
  });
  for (const id of ["#results", "#sem-list"]) $(id).addEventListener("click", onListClick);
  window.addEventListener("popstate", () => {
    ctx.state = readUrl();
    $<HTMLInputElement>("#q").value = ctx.state.q;
    facets.sync();
    void refresh();
    if (ctx.state.world) void detail.open(ctx.state.world, false); else detail.close(false);
  });

  await refresh();
  booted = true;
  setStatus(`${eng.c.n.toLocaleString()} worlds · ready in ${Math.round(performance.now() - t0)} ms`);
  resolveReady(info);
  if (ctx.state.world) void detail.open(ctx.state.world, false);
  if (semCtl.remembered()) void semCtl.enable(); // consent was given on an earlier visit; assets come from the HTTP cache
}
let booted = false;
const wide = () => window.matchMedia("(min-width:1024px)").matches;

// ---------------- state -> URL -> results
function commit(push = false) {
  const u = toUrl(ctx.state);
  if (u !== location.pathname + location.search) (push ? history.pushState : history.replaceState).call(history, null, "", u);
}
let timer: any;
function change(debounce = 0) {
  pills?.render();
  renderSorts();
  ($("#fbtn") as HTMLElement).textContent = facetCount(ctx.state) ? `Filters (${facetCount(ctx.state)})` : "Filters";
  clearTimeout(timer);
  timer = setTimeout(() => { shown = PAGE; commit(); void refresh(); }, debounce);
}

function renderSorts() {
  const eff = effSort(), s = ctx.state;
  $("#sorts").innerHTML = `<span class="lbl">Sort</span>` + SORTS.map((x) => {
    const on = x === eff;
    const arrow = on && !NO_DIR.has(x) ? (s.asc ? " ↑" : " ↓") : "";
    const tip = x === "trending" && eng.trend?.basis === "popularity_fallback" ? "recent visits per day (estimated from popularity; no recent-visit data in this build)" : SORT_TIP[x];
    return `<button type="button" data-sort="${x}" data-testid="sort-${x}" class="${on ? "on" : ""}" aria-pressed="${on}" title="${tip}">${SORT_LABEL[x]}${arrow}</button>`;
  }).join("");
}

async function cardsFor(idx: number[]): Promise<RowCard[]> {
  const cs = eng.m.cardChunk;
  const chunks = new Map<number, any>();
  await Promise.all([...new Set(idx.map((i) => Math.floor(i / cs)))].map(async (k) => chunks.set(k, await cardChunk(k))));
  return idx.map((i) => { const cc = chunks.get(Math.floor(i / cs)), j = i % cs; return { desc: cc.d[j] ?? "", tags: cc.t[j] ?? [] }; });
}

async function refresh() {
  const my = ++seq;
  const s = ctx.state;
  const r = await eng.query({ q: s.q, filters: filtersOf(s), sort: effSort(), asc: s.asc, limit: shown, tagLimit: 40 });
  if (my !== seq) return;
  const infos = await eng.info(r.idx);
  if (my !== seq) return;
  $("#summary").textContent = `${r.total.toLocaleString()} worlds · ${r.ms.toFixed(0)} ms`;
  $("#results").innerHTML = r.idx.length
    ? r.idx.map((i, j) => rowHtml(eng, ctx.meters, i, infos[j], undefined, "result-item", infos[j].id === s.world)).join("")
    : `<li class="empty-state" data-testid="no-results"><div><b>No worlds match.</b><br>Remove a pill above, or relax a filter.</div></li>`;
  lazyThumbs($("#results"));
  ($("#more") as HTMLButtonElement).hidden = r.idx.length >= r.total;
  pills.render();
  renderSorts();
  // tags + description arrive with the card chunks: patched into the rows already on screen
  const cardsP = cardsFor(r.idx).then((cards) => { if (my === seq) fillCards($("#results"), r.idx, cards); return cards; });
  await facets.render(r.facetCounts as FacetCounts, r.total);
  if (s.author) setAuthorName(await authorLabel(s.author));
  await cardsP;
  if (my !== seq) return;
  void renderSemantic(my, new Set(r.idx));
}

const authorNames = new Map<string, string>();
async function authorLabel(id: string): Promise<string> {
  if (!authorNames.has(id)) {
    const first = (await authorWorlds(id))[0];
    authorNames.set(id, first === undefined ? id : (await eng.info([first]))[0].author);
  }
  return authorNames.get(id)!;
}

async function loadMore() {
  const my = seq, s = ctx.state, off = shown;
  shown += PAGE;
  const r = await eng.query({ q: s.q, filters: filtersOf(s), sort: effSort(), asc: s.asc, limit: PAGE, offset: off, facets: false });
  if (my !== seq) return;
  const infos = await eng.info(r.idx);
  $("#results").insertAdjacentHTML("beforeend", r.idx.map((i, j) => rowHtml(eng, ctx.meters, i, infos[j], undefined, "result-item", infos[j].id === s.world)).join(""));
  lazyThumbs($("#results"));
  ($("#more") as HTMLButtonElement).hidden = off + r.idx.length >= r.total;
  fillCards($("#results"), r.idx, await cardsFor(r.idx));
}

function onListClick(e: Event) {
  const me = e as MouseEvent, t = e.target as HTMLElement;
  const tag = t.closest(".tag[data-t]") as HTMLElement | null;
  if (tag) { const s = ctx.state, k = tag.dataset.t!; if (!s.tags.includes(k)) s.tags.push(k); s.tags_ex = s.tags_ex.filter((x) => x !== k); change(); return; }
  const a = t.closest("a[href^='?world=']") as HTMLAnchorElement | null;
  if (!a || me.ctrlKey || me.metaKey || me.shiftKey) return;
  e.preventDefault();
  void detail.open(a.getAttribute("href")!.slice(7), true);
}
function mark(id: string) {
  document.querySelectorAll<HTMLElement>("#results > li, #sem-list > li").forEach((li) => li.classList.toggle("sel", !!id && li.dataset.id === id));
}

// ---------------- opt-in semantic search
async function ensureSem(progress?: (d: number, t: number) => void): Promise<Sem> {
  if (!sem) {
    const { Sem } = await import("./sem");
    sem = await Sem.load(progress);
  }
  return sem;
}
/** the text a semantic query should embed: the positive plain terms of the box (not -x, tag:x, cap:>=8 ...) */
function semText(): string {
  return parseQuery(ctx.state.q).pills.filter((p) => !p.negated && (p.kind === "term" || p.kind === "phrase")).map((p) => p.value).join(" ");
}
async function renderSemantic(my: number, shownIds: Set<number>) {
  const box = $("#semantic"), hint = $("#sem-hint"), list = $("#sem-list");
  if (!semOn || !sem) { box.hidden = true; return; }
  box.hidden = false;
  const text = semText();
  if (text.trim().length < 2) {
    hint.hidden = false;
    hint.textContent = "Semantic search is on. Type a search (2+ characters) to see worlds related by meaning.";
    list.innerHTML = ""; $("#sem-ms").textContent = "";
    return;
  }
  hint.hidden = true;
  const t0 = performance.now();
  const allow = (await eng.allowMask(filtersOf(ctx.state))).slice();
  const hits = sem.search(text, 40, allow, eng.lp).filter((i) => !shownIds.has(i)).slice(0, 8);
  const ms = performance.now() - t0;
  const [infos, cards] = await Promise.all([eng.info(hits), cardsFor(hits)]);
  if (my !== seq) return;
  list.innerHTML = hits.map((i, j) => rowHtml(eng, ctx.meters, i, infos[j], cards[j], "semantic-item", infos[j].id === ctx.state.world, true)).join("")
    || `<li class="dim empty">nothing new beyond the results above</li>`;
  lazyThumbs(list);
  $("#sem-ms").textContent = `${ms.toFixed(0)} ms`;
}

boot().catch((e) => { console.error(e); setStatus("failed to load: " + esc(String(e?.message ?? e))); });
