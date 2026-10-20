// Query pills: parseQuery(q).pills (typed conditions) + facet conditions (clicked), each removable; the sign flips include/exclude.
import { parseQuery, flipPill, removePill } from "../query";
import type { Ctx } from "./ctx";
import { esc, label } from "./util";

interface PillView {
  src: "query" | "facet";
  kind: string;
  text: string;
  /** null = no include/exclude semantics (ranges, author) */
  neg: boolean | null;
  flip?: () => void;
  remove: () => void;
}

function facetPills(ctx: Ctx): PillView[] {
  const s = ctx.state, out: PillView[] = [];
  const drop = (arr: string[], k: string) => () => { const i = arr.indexOf(k); if (i >= 0) arr.splice(i, 1); };
  const move = (from: string[], to: string[], k: string) => () => { drop(from, k)(); if (!to.includes(k)) to.push(k); };
  for (const k of s.tags) out.push({ src: "facet", kind: "tag", text: `tag: ${label(k)}`, neg: false, flip: move(s.tags, s.tags_ex, k), remove: drop(s.tags, k) });
  for (const k of s.tags_ex) out.push({ src: "facet", kind: "tag", text: `tag: ${label(k)}`, neg: true, flip: move(s.tags_ex, s.tags, k), remove: drop(s.tags_ex, k) });
  for (const k of s.cw) out.push({ src: "facet", kind: "cw", text: `warning: ${k}`, neg: false, flip: move(s.cw, s.cwx, k), remove: drop(s.cw, k) });
  for (const k of s.cwx) out.push({ src: "facet", kind: "cw", text: `warning: ${k}`, neg: true, flip: move(s.cwx, s.cw, k), remove: drop(s.cwx, k) });
  for (const k of s.status) out.push({ src: "facet", kind: "status", text: `status: ${k}`, neg: false, flip: move(s.status, s.status_ex, k), remove: drop(s.status, k) });
  for (const k of s.status_ex) out.push({ src: "facet", kind: "status", text: `status: ${k}`, neg: true, flip: move(s.status_ex, s.status, k), remove: drop(s.status_ex, k) });
  if (s.platform) {
    const names = s.platform === "both" ? "PC + Quest" : s.platform === "pc" ? "PC" : "Quest";
    out.push({ src: "facet", kind: "platform", text: `platform: ${names}`, neg: false, remove: () => { s.platform = ""; },
      flip: s.platform === "both" ? undefined : () => { const k = s.platform as "pc" | "quest"; s.platform = ""; s.platform_ex = k; } });
  }
  if (s.platform_ex) out.push({ src: "facet", kind: "platform", text: `platform: ${s.platform_ex === "pc" ? "PC" : "Quest"}`, neg: true, remove: () => { s.platform_ex = ""; },
    flip: () => { const k = s.platform_ex as "pc" | "quest"; s.platform_ex = ""; s.platform = k; } });
  if (s.cap_min !== undefined || s.cap_max !== undefined)
    out.push({ src: "facet", kind: "cap", text: `capacity ${s.cap_min ?? 1}–${s.cap_max ?? "80+"}`, neg: null, remove: () => { s.cap_min = undefined; s.cap_max = undefined; } });
  if (s.size_max_mb !== undefined) out.push({ src: "facet", kind: "size", text: `size ≤ ${s.size_max_mb} MB`, neg: null, remove: () => { s.size_max_mb = undefined; } });
  if (s.after || s.before) out.push({ src: "facet", kind: "date", text: `updated ${s.after || "…"} → ${s.before || "now"}`, neg: null, remove: () => { s.after = ""; s.before = ""; } });
  if (s.pub_after || s.pub_before) out.push({ src: "facet", kind: "date", text: `published ${s.pub_after || "…"} → ${s.pub_before || "now"}`, neg: null, remove: () => { s.pub_after = ""; s.pub_before = ""; } });
  if (s.author) out.push({ src: "facet", kind: "author", text: `author: ${authorName || s.author}`, neg: null, remove: () => { s.author = ""; } });
  return out;
}
let authorName = "";
export function setAuthorName(n: string) { authorName = n; }

export function createPills(ctx: Ctx, host: HTMLElement, onEdit: () => void) {
  let views: PillView[] = [];
  function render() {
    const parse = parseQuery(ctx.state.q);
    const q = ctx.state.q;
    views = [
      ...parse.pills.map((p): PillView => ({
        src: "query", kind: p.kind, text: p.label, neg: p.negated,
        flip: () => { ctx.state.q = flipPill(q, p); }, remove: () => { ctx.state.q = removePill(q, p); },
      })),
      ...facetPills(ctx),
    ];
    host.hidden = views.length === 0 && parse.errors.length === 0;
    host.innerHTML = views.map((v, i) => `<span class="pill ${v.neg === null ? "neutral" : v.neg ? "exc" : "inc"}" data-testid="pill" data-src="${v.src}" data-kind="${esc(v.kind)}" data-neg="${v.neg === null ? "" : v.neg}" data-i="${i}">`
      + (v.neg === null ? "" : `<button type="button" class="sign" data-testid="pill-sign" ${v.flip ? "" : "disabled"} title="${v.flip ? "click to " + (v.neg ? "include" : "exclude") + " instead" : ""}" aria-label="${v.neg ? "excluded" : "included"}; flip">${v.neg ? "−" : "+"}</button>`)
      + `<span class="plabel">${esc(v.text)}</span><button type="button" class="x" data-testid="pill-remove" aria-label="remove ${esc(v.text)}">×</button></span>`).join("")
      + (parse.errors.length ? `<span class="perr" role="note">${esc(parse.errors[0])}</span>` : "");
  }
  host.addEventListener("click", (e) => {
    const t = e.target as HTMLElement;
    const pill = t.closest(".pill") as HTMLElement | null;
    if (!pill) return;
    const v = views[+pill.dataset.i!];
    if (!v) return;
    if (t.closest(".x")) v.remove();
    else if (t.closest(".sign") && v.flip) v.flip();
    else return;
    onEdit();
  });
  return { render };
}

const EXAMPLES: [string, string][] = [
  ["lake -hangout", "words AND together; a leading - excludes"],
  ["(horror | scary) -tag:game", "| or OR = either; parentheses group"],
  ['"escape room"', "exact phrase"],
  ["cap:>=16", "capacity: > >= < <= or 8..32"],
  ["size:<50", "download size in MB"],
  ["updated:>=2025", "also published:2024-06, visits:>1000"],
  ["tag:japan author:Foo cw:horror", "tag:, author:, title:, cw:, platform:quest, status:labs"],
];
export function createSyntaxHelp(btn: HTMLElement, pop: HTMLElement, pick: (q: string) => void) {
  pop.innerHTML = `<h4>Search syntax</h4><p class="dim">Terms are ANDed. Lowercase <code>and or not</code> are plain words.</p><table>`
    + EXAMPLES.map(([q, d]) => `<tr><td><button type="button" class="ex" data-q="${esc(q)}">${esc(q)}</button></td><td>${esc(d)}</td></tr>`).join("") + `</table>`;
  const toggle = (on?: boolean) => { const open = on ?? pop.hidden; pop.hidden = !open; btn.setAttribute("aria-expanded", String(open)); };
  btn.addEventListener("click", (e) => { e.stopPropagation(); toggle(); });
  pop.addEventListener("click", (e) => {
    const b = (e.target as HTMLElement).closest(".ex") as HTMLElement | null;
    if (b) { pick(b.dataset.q!); toggle(false); }
    e.stopPropagation();
  });
  document.addEventListener("click", () => toggle(false));
  window.addEventListener("keydown", (e) => { if (e.key === "Escape") toggle(false); });
}
