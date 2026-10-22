// Result row: thumbnail, title/author, ALL user tags, description start, log-scale comparison bars.
import { dateOfDay, tagClass } from "../engine";
import type { Engine } from "../engine";
import type { WorldInfo } from "../data";
import type { Kind, Meters } from "./meters";
import { thumbBox } from "./thumbs";
import { NOSIZE, esc, fmt, kb, label } from "./util";

export interface RowCard { desc: string; tags: string[] }

export function meter(cls: string, name: string, frac: number | null, text: string, title: string, rank = ""): string {
  return `<span class="m ${cls}${frac === null ? " none" : ""}" data-testid="bar-${cls}" title="${esc(title)}"><em>${name}</em><span class="bar"><i style="width:${frac === null ? 0 : Math.max(2, Math.round(frac * 100))}%"></i></span><b>${esc(text)}</b>${rank ? `<small class="rk">${esc(rank)}</small>` : ""}</span>`;
}

/** five fixed-column rank bars (percentile among all worlds that have the value); every row uses the same grid so the columns line up.
 *  `rank` = also print "top N%" / "larger than N% of worlds" as visible text (detail panel) */
export function meterBlock(eng: Engine, meters: Meters, i: number, rank = false): string {
  const c = eng.c;
  const hasPc = !!(c.plat[i] & 1), hasQ = !!(c.plat[i] & 2);
  const big = (k: Kind, v: number, what: string, shown: string) => { const r = meters.larger(k, v); return { f: meters.frac(k, v), t: shown, ti: `${shown} ${what} · ${r}`, rk: r }; };
  const top = (k: Kind, v: number, what: string, shown: string) => { const r = meters.top(k, v); return { f: meters.frac(k, v), t: shown, ti: `${v.toLocaleString()} ${what} · ${r}`, rk: r }; };
  const none = (t: string, ti: string) => ({ f: null as number | null, t, ti, rk: "" });
  const pc = hasPc ? (c.pc[i] === NOSIZE ? none("?", "PC package (size unknown)") : big("pc", c.pc[i], "PC download", kb(c.pc[i]))) : none("–", "no PC package");
  const qu = hasQ ? (c.quest[i] === NOSIZE ? none("?", "Quest package (size unknown)") : big("quest", c.quest[i], "Quest download", kb(c.quest[i]))) : none("–", "no Quest package");
  const cap = c.cap[i] ? big("cap", c.cap[i], "player capacity", String(c.cap[i])) : none("?", "capacity unknown");
  const vis = top("visits", c.visits[i], "visits", fmt(c.visits[i]));
  const fav = top("fav", c.fav[i], "favorites", fmt(c.fav[i]));
  const m = (cls: string, name: string, x: { f: number | null; t: string; ti: string; rk: string }) => meter(cls, name, x.f, x.t, x.ti, rank ? x.rk : "");
  return m("cap", "cap", cap) + m("pc", "PC", pc) + m("quest", "Quest", qu) + m("visits", "visits", vis) + m("fav", "favs", fav);
}

/** row date slot: the newer of updated_at and the newest package upload ("upd" / "uploaded"); with a recent/most-updated view, the upload counts */
let uploadsView = false;
export function setUploadsView(on: boolean) { uploadsView = on; }
export function dateSlot(eng: Engine, i: number): string {
  const c = eng.c;
  const upd = c.updatedAt[i], lu = c.lastUpload?.[i] ?? 0;
  const tip = `world updated ${dateOfDay(upd)}${lu ? `, newest package upload ${dateOfDay(lu)}` : ""}, published ${dateOfDay(c.created[i])}`;
  if (uploadsView && c.uploads90) return `<span class="m date" title="${tip}">${c.uploads90[i]} uploads/90d · last ${lu ? dateOfDay(lu) : "–"}</span>`;
  if (lu > upd) return `<span class="m date" title="${tip}">uploaded ${dateOfDay(lu)}</span>`;
  return `<span class="m date" title="${tip}">upd ${dateOfDay(upd)}</span>`;
}

/** tags + description start; rendered into `.rowx`, which is filled in once the card chunk has arrived */
export function rowExtraHtml(card: RowCard | undefined): string {
  if (!card) return "";
  const tags = card.tags.filter((t) => tagClass(t) === "user");
  let desc = card.desc.replace(/\s+/g, " ").trim();
  // the meta chunk cuts at 160 chars (mid-word): back up to a word boundary and mark the cut
  if (desc.length >= 155) desc = desc.replace(/\s+\S*$/, "") + "…";
  const d160 = desc;
  return `<div class="tg">${tags.map((t) => `<span class="tag" data-t="${esc(t)}">${esc(label(t))}</span>`).join("")}</div>${d160 ? `<p class="rdesc">${esc(d160)}</p>` : ""}`;
}
export function rowBadges(card: RowCard | undefined): string {
  return (card?.tags ?? []).filter((t) => tagClass(t) === "cw" || tagClass(t) === "status")
    .map((t) => `<span class="badge ${tagClass(t)}">${esc(t.replace(/^(content_|system_)/, ""))}</span>`).join("");
}
export function rowHtml(eng: Engine, meters: Meters, i: number, w: WorldInfo, tid: string, selected: boolean, compact = false): string {
  const c = eng.c;
  const card: RowCard = { desc: w.desc ?? "", tags: w.tags ?? [] }; // info() carries desc[0:160] + visible tags in the meta chunk
  const href = `?world=${esc(w.id)}`;
  return `<li data-testid="${tid}" data-id="${esc(w.id)}" data-i="${i}" tabindex="0" class="${selected ? "sel" : ""}">
    <a href="${href}" class="thumb" tabindex="-1" aria-hidden="true">${thumbBox(i, w.id)}</a>
    <div class="body">
      <div class="l1"><a class="title" href="${href}">${esc(w.name || "(untitled)")}</a><span class="by">${esc(w.author)}</span><span class="rbadges">${rowBadges(card)}</span></div>
      ${compact ? "" : `<div class="rowx">${rowExtraHtml(card)}</div>`}
      <div class="mrow">${meterBlock(eng, meters, i)}
        ${dateSlot(eng, i)}</div>
    </div></li>`;
}
