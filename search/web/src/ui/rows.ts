// Result row: thumbnail, title/author, ALL user tags, description start, log-scale comparison bars.
import { dateOfDay, tagClass } from "../engine";
import type { Engine } from "../engine";
import type { WorldInfo } from "../data";
import type { Kind, Meters } from "./meters";
import { thumbBox } from "./thumbs";
import { NOSIZE, esc, fmt, kb, label } from "./util";

export interface RowCard { desc: string; tags: string[] }

export function meter(cls: string, name: string, frac: number | null, text: string, title: string): string {
  return `<span class="m ${cls}${frac === null ? " none" : ""}" data-testid="bar-${cls}" title="${esc(title)}"><em>${name}</em><span class="bar"><i style="width:${frac === null ? 0 : Math.max(2, Math.round(frac * 100))}%"></i></span><b>${esc(text)}</b></span>`;
}

/** five fixed-column rank bars (percentile among all worlds that have the value); every row uses the same grid so the columns line up */
export function meterBlock(eng: Engine, meters: Meters, i: number): string {
  const c = eng.c;
  const hasPc = !!(c.plat[i] & 1), hasQ = !!(c.plat[i] & 2);
  const rk = (k: Kind, v: number, what: string, shown: string) => ({ f: meters.frac(k, v), t: shown, ti: `${shown} ${what} · ${meters.top(k, v)}` });
  const pc = hasPc ? (c.pc[i] === NOSIZE ? { f: null, t: "?", ti: "PC package (size unknown)" } : rk("pc", c.pc[i], "PC download", kb(c.pc[i]))) : { f: null, t: "–", ti: "no PC package" };
  const qu = hasQ ? (c.quest[i] === NOSIZE ? { f: null, t: "?", ti: "Quest package (size unknown)" } : rk("quest", c.quest[i], "Quest download", kb(c.quest[i]))) : { f: null, t: "–", ti: "no Quest package" };
  const cap = c.cap[i] ? rk("cap", c.cap[i], "player capacity", String(c.cap[i])) : { f: null, t: "?", ti: "capacity unknown" };
  const vis = rk("visits", c.visits[i], "visits", fmt(c.visits[i]));
  const fav = rk("fav", c.fav[i], "favorites", fmt(c.fav[i]));
  return meter("cap", "cap", cap.f, cap.t, cap.ti) + meter("pc", "PC", pc.f, pc.t, pc.ti) + meter("quest", "Quest", qu.f, qu.t, qu.ti)
    + meter("visits", "visits", vis.f, vis.t, `${c.visits[i].toLocaleString()} visits · ${meters.top("visits", c.visits[i])}`)
    + meter("fav", "favs", fav.f, fav.t, `${c.fav[i].toLocaleString()} favorites · ${meters.top("fav", c.fav[i])}`);
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
        <span class="m date" title="updated ${dateOfDay(c.updated[i])}, published ${dateOfDay(c.created[i])}">upd ${dateOfDay(c.updated[i])}</span></div>
    </div></li>`;
}
