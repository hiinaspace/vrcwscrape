// Result row: thumbnail, title/author, ALL user tags, description start, log-scale comparison bars.
import { dateOfDay, tagClass } from "../engine";
import type { Engine } from "../engine";
import type { WorldInfo } from "../data";
import type { Meters } from "./meters";
import { thumbBox } from "./thumbs";
import { NOSIZE, esc, fmt, kb, label } from "./util";

export interface RowCard { desc: string; tags: string[] }

export function meter(cls: string, name: string, frac: number | null, text: string, title: string): string {
  return `<span class="m ${cls}${frac === null ? " none" : ""}" title="${esc(title)}"><em>${name}</em><span class="bar"><i style="width:${frac === null ? 0 : Math.round(frac * 100)}%"></i></span><b>${esc(text)}</b></span>`;
}

export function meterBlock(eng: Engine, meters: Meters, i: number): string {
  const c = eng.c;
  const hasPc = !!(c.plat[i] & 1), hasQ = !!(c.plat[i] & 2);
  const pc = hasPc ? (c.pc[i] === NOSIZE ? { f: null, t: "?", ti: "PC package (size unknown)" } : { f: meters.frac("pc", c.pc[i]), t: kb(c.pc[i]), ti: `PC download ${kb(c.pc[i])}` }) : { f: null, t: "–", ti: "no PC package" };
  const qu = hasQ ? (c.quest[i] === NOSIZE ? { f: null, t: "?", ti: "Quest package (size unknown)" } : { f: meters.frac("quest", c.quest[i]), t: kb(c.quest[i]), ti: `Quest download ${kb(c.quest[i])}` }) : { f: null, t: "–", ti: "no Quest package" };
  return meter("cap", "cap", c.cap[i] ? meters.frac("cap", c.cap[i]) : null, String(c.cap[i] || "?"), `Capacity: ${c.cap[i]} players`)
    + meter("pc", "PC", pc.f, pc.t, pc.ti)
    + meter("quest", "Quest", qu.f, qu.t, qu.ti)
    + meter("visits", "visits", meters.frac("visits", c.visits[i]), fmt(c.visits[i]), `${c.visits[i].toLocaleString()} visits`);
}

/** tags + description start; rendered into `.rowx`, which is filled in once the card chunk has arrived */
export function rowExtraHtml(card: RowCard | undefined): string {
  if (!card) return "";
  const tags = card.tags.filter((t) => tagClass(t) === "user");
  const desc = card.desc.replace(/\s+/g, " ").trim();
  const d160 = desc.length > 160 ? desc.slice(0, 160).trimEnd() + "…" : desc;
  return `<div class="tg">${tags.map((t) => `<span class="tag" data-t="${esc(t)}">${esc(label(t))}</span>`).join("")}</div>${d160 ? `<p class="rdesc">${esc(d160)}</p>` : ""}`;
}
export function rowBadges(card: RowCard | undefined): string {
  return (card?.tags ?? []).filter((t) => tagClass(t) === "cw" || tagClass(t) === "status")
    .map((t) => `<span class="badge ${tagClass(t)}">${esc(t.replace(/^(content_|system_)/, ""))}</span>`).join("");
}
/** patch card-derived parts into already rendered rows (rows are shown before the card chunks arrive) */
export function fillCards(root: ParentNode, idx: number[], cards: RowCard[]) {
  idx.forEach((i, j) => {
    const li = root.querySelector<HTMLElement>(`li[data-i="${i}"]`);
    if (!li) return;
    const x = li.querySelector(".rowx"); if (x) x.innerHTML = rowExtraHtml(cards[j]);
    const bd = li.querySelector(".rbadges"); if (bd) bd.innerHTML = rowBadges(cards[j]);
  });
}

export function rowHtml(eng: Engine, meters: Meters, i: number, w: WorldInfo, card: RowCard | undefined, tid: string, selected: boolean, compact = false): string {
  const c = eng.c;
  const href = `?world=${esc(w.id)}`;
  return `<li data-testid="${tid}" data-id="${esc(w.id)}" data-i="${i}" class="${selected ? "sel" : ""}">
    <a href="${href}" class="thumb" tabindex="-1" aria-hidden="true">${thumbBox(i, w.id)}</a>
    <div class="body">
      <div class="l1"><a class="title" href="${href}">${esc(w.name || "(untitled)")}</a><span class="by">${esc(w.author)}</span><span class="rbadges">${rowBadges(card)}</span></div>
      ${compact ? "" : `<div class="rowx">${rowExtraHtml(card)}</div>`}
      <div class="mrow">${meterBlock(eng, meters, i)}<span class="m fav" title="${c.fav[i].toLocaleString()} favorites">♥ ${fmt(c.fav[i])}</span>
        <span class="m date" title="updated ${dateOfDay(c.updated[i])}, published ${dateOfDay(c.created[i])}">upd ${dateOfDay(c.updated[i])}</span></div>
    </div></li>`;
}
