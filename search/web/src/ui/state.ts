// URL <-> UI state. All state lives in the URL (contract with the oracle harness); new round-5 keys are additive.
import type { Filters, Sort } from "../engine";
import { csv, num } from "./util";

export type DateField = "updated" | "published";
export interface State {
  q: string;
  cap_min?: number; cap_max?: number; size_max_mb?: number;
  platform: "" | "pc" | "quest" | "both";
  platform_ex: "" | "pc" | "quest";
  after: string; before: string;          // updated range (oracle contract)
  pub_after: string; pub_before: string;  // published range
  datef: DateField;                       // which range the histogram edits (UI only)
  tags: string[]; tags_ex: string[];
  status: string[]; status_ex: string[];
  cw: string[]; cwx: string[];
  author: string;
  sort: Sort | "";                        // "" = automatic (trending without a text query, relevance with one)
  asc: boolean;
  world: string;
}

export const SORTS: Sort[] = ["trending", "relevance", "visits", "favorites", "updated", "published", "capacity"];
const ALL_SORTS: string[] = [...SORTS, "text"];

export function readUrl(): State {
  const p = new URLSearchParams(location.search);
  const sort = p.get("sort") ?? "";
  const pf = p.get("platform"), px = p.get("platform_ex");
  return {
    q: p.get("q") ?? "", cap_min: num(p.get("cap_min")), cap_max: num(p.get("cap_max")), size_max_mb: num(p.get("size_max_mb")),
    platform: pf === "pc" || pf === "quest" || pf === "both" ? pf : "",
    platform_ex: px === "pc" || px === "quest" ? px : "",
    after: p.get("after") ?? "", before: p.get("before") ?? "", pub_after: p.get("pub_after") ?? "", pub_before: p.get("pub_before") ?? "",
    datef: p.get("datef") === "published" ? "published" : "updated",
    tags: csv(p.get("tags")), tags_ex: csv(p.get("tags_ex")), status: csv(p.get("status")), status_ex: csv(p.get("status_ex")),
    cw: csv(p.get("cw")), cwx: csv(p.get("cw_exclude")),
    author: p.get("author") ?? "", sort: (ALL_SORTS.includes(sort) ? sort : "") as Sort | "", asc: p.get("asc") === "1", world: p.get("world") ?? "",
  };
}

export function toUrl(s: State): string {
  const p = new URLSearchParams();
  const set = (k: string, v: string | number | undefined) => { if (v !== undefined && v !== "") p.set(k, String(v)); };
  set("q", s.q); set("cap_min", s.cap_min); set("cap_max", s.cap_max); set("size_max_mb", s.size_max_mb);
  set("platform", s.platform); set("platform_ex", s.platform_ex);
  set("after", s.after); set("before", s.before); set("pub_after", s.pub_after); set("pub_before", s.pub_before);
  if (s.datef === "published") p.set("datef", "published");
  if (s.tags.length) p.set("tags", s.tags.join(","));
  if (s.tags_ex.length) p.set("tags_ex", s.tags_ex.join(","));
  set("author", s.author);
  if (s.status.length) p.set("status", s.status.join(","));
  if (s.status_ex.length) p.set("status_ex", s.status_ex.join(","));
  if (s.cw.length) p.set("cw", s.cw.join(","));
  if (s.cwx.length) p.set("cw_exclude", s.cwx.join(","));
  set("sort", s.sort);
  if (s.asc) p.set("asc", "1");
  set("world", s.world);
  const q = p.toString();
  return location.pathname + (q ? "?" + q : "");
}

export const filtersOf = (s: State): Filters => ({
  cap_min: s.cap_min, cap_max: s.cap_max, size_max_mb: s.size_max_mb,
  platform: s.platform, platform_ex: s.platform_ex,
  after: s.after || undefined, before: s.before || undefined, pub_after: s.pub_after || undefined, pub_before: s.pub_before || undefined,
  tags: s.tags, tags_ex: s.tags_ex, author: s.author || undefined,
  status: s.status, status_ex: s.status_ex, cw: s.cw, cw_exclude: s.cwx,
});

/** how many sidebar-controlled conditions are active (for the mobile "Filters (n)" button) */
export function facetCount(s: State): number {
  return (s.cap_min !== undefined || s.cap_max !== undefined ? 1 : 0) + (s.size_max_mb !== undefined ? 1 : 0) + (s.platform ? 1 : 0) + (s.platform_ex ? 1 : 0)
    + (s.after || s.before ? 1 : 0) + (s.pub_after || s.pub_before ? 1 : 0) + s.tags.length + s.tags_ex.length + (s.author ? 1 : 0)
    + s.status.length + s.status_ex.length + s.cw.length + s.cwx.length;
}
