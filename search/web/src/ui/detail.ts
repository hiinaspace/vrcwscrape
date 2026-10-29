// Detail panel: right-hand sidebar on desktop (>=1024px), full-screen sheet on mobile. Replaces the old modal.
import { cardChunk } from "../data";
import { dateOfDay, tagClass } from "../engine";
import type { Ctx } from "./ctx";
import { meterBlock, staleSince } from "./rows";
import { fillHistory } from "./history";
import { lazyThumbs, thumbBox } from "./thumbs";
import { esc, fmt, label } from "./util";

export const OFFICIAL = (id: string) => `https://vrchat.com/home/world/${id}`;

export interface Detail {
  open(id: string, push: boolean): Promise<void>;
  close(push: boolean): void;
}

export function createDetail(ctx: Ctx, host: HTMLElement, hooks: {
  commit(push: boolean): void;
  mark(id: string): void; // highlight the selected result row
  addTag(tag: string): void;
  setAuthor(authorId: string): void;
}): Detail {
  const eng = ctx.eng;
  let seq = 0;

  async function open(id: string, push: boolean) {
    const my = ++seq;
    ctx.state.world = id;
    if (push) hooks.commit(true);
    hooks.mark(id);
    document.body.classList.add("has-detail");
    host.hidden = false;
    const idx = await eng.idToIdx(id);
    if (my !== seq) return;
    if (idx === undefined) {
      host.innerHTML = `<div class="dhead"><button type="button" class="back">‹ Back</button></div><article data-testid="world-card"><p>World not found: ${esc(id)}</p></article>`;
      wire(); return;
    }
    const [w] = await eng.info([idx]);
    const cc = await cardChunk(Math.floor(idx / eng.m.cardChunk));
    if (my !== seq) return;
    const c = eng.c, j = idx % eng.m.cardChunk;
    const all = cc.t[j] ?? [];
    const tags = all.filter((t) => tagClass(t) === "user");
    const badges = all.filter((t) => tagClass(t) === "cw" || tagClass(t) === "status");
    const row = (k: string, v: string) => `<tr><th>${k}</th><td>${v}</td></tr>`;
    host.innerHTML = `<div class="dhead"><button type="button" class="back" aria-label="back to results">‹ Back</button><button type="button" class="x" aria-label="close">✕</button></div>
      <article data-testid="world-card" data-id="${esc(w.id)}">
        <a class="hero" href="${OFFICIAL(w.id)}" target="_blank" rel="noopener" title="Open the official VRChat world page">${thumbBox(idx, w.id)}<span class="heroql">view on vrchat.com ↗</span></a>
        <h2>${esc(w.name)}</h2>
        <p class="by">by <a href="#" id="c-author">${esc(w.author)}</a> · <a href="${OFFICIAL(w.id)}" target="_blank" rel="noopener">vrchat.com ↗</a></p>
        <div class="mrow big">${meterBlock(eng, ctx.meters, idx, true)}</div>
        <table>${row("Capacity", String(c.cap[idx]))}${row("Visits", c.visits[idx].toLocaleString())}${c.metricsDay?.[idx] ? row("Visits as of", dateOfDay(c.metricsDay[idx]) + (staleSince(eng, idx) ? ` <span class="stale">· stale</span>` : "")) : ""}${row("Favorites", c.fav[idx].toLocaleString())}
          ${row("Published", dateOfDay(c.created[idx]))}${row("Updated", dateOfDay(c.updatedAt[idx])) + (c.lastUpload?.[idx] ? row("Last package upload", dateOfDay(c.lastUpload[idx])) : "")}${row("World id", `<code>${esc(w.id)}</code>`)}</table>
        <div class="badges">${badges.map((t) => `<span class="badge ${tagClass(t)}">${esc(t.replace(/^(content_|system_)/, ""))}</span>`).join("")}</div>
        <div class="tg">${tags.map((t) => `<button type="button" class="tag" data-t="${esc(t)}" title="show only worlds tagged ${esc(label(t))}">${esc(label(t))}</button>`).join("")}</div>
        <p class="desc">${esc(cc.d[j] ?? "")}</p>
        <section class="history" data-testid="history-panel" ${eng.m.history ? "" : "hidden"}><h3>History</h3><p class="dim">loading…</p></section>
        <h3>Similar worlds</h3><ol data-testid="similar-list" class="similar"><li class="dim">loading…</li></ol></article>`;
    host.scrollTop = 0;
    wire();
    lazyThumbs(host);
    void fillHistory(eng, host.querySelector(".history") as HTMLElement, idx, () => my === seq);
    host.querySelector("#c-author")!.addEventListener("click", (e) => { e.preventDefault(); hooks.setAuthor(w.authorId); });
    host.querySelectorAll<HTMLElement>(".tg [data-t]").forEach((b) => b.addEventListener("click", () => hooks.addTag(b.dataset.t!)));
    const nb = await eng.similar(idx, 20);
    const list = host.querySelector("[data-testid=similar-list]") as HTMLElement;
    if (my !== seq) return;
    if (!nb.length) { list.innerHTML = `<li class="dim">no neighbor data for this world</li>`; return; }
    const infos = await eng.info(nb);
    if (my !== seq) return;
    list.innerHTML = infos.map((x, k) => `<li data-id="${esc(x.id)}"><a href="?world=${esc(x.id)}" class="sthumb">${thumbBox(nb[k], x.id)}</a><span><a href="?world=${esc(x.id)}" class="stitle">${esc(x.name)}</a><small>${esc(x.author)} · cap ${c.cap[nb[k]]} · ${fmt(c.visits[nb[k]])} visits</small></span></li>`).join("");
    lazyThumbs(list);
    list.addEventListener("click", (e) => {
      const a = (e.target as HTMLElement).closest("a[href^='?world=']") as HTMLAnchorElement | null; if (!a) return;
      e.preventDefault(); void open(a.getAttribute("href")!.slice(7), true);
    });
  }
  function wire() {
    host.querySelector(".back")?.addEventListener("click", () => close(true));
    host.querySelector(".x")?.addEventListener("click", () => close(true));
  }
  function close(push: boolean) {
    seq++;
    ctx.state.world = "";
    host.hidden = true;
    host.innerHTML = "";
    document.body.classList.remove("has-detail");
    hooks.mark("");
    if (push) hooks.commit(true);
  }
  window.addEventListener("keydown", (e) => { if (e.key === "Escape" && ctx.state.world && !(document.activeElement instanceof HTMLInputElement)) close(true); });
  return { open, close };
}
