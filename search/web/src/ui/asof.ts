// Data freshness: the data is a weekly snapshot, not live, so everything that would say "now" is anchored to the data instead.
//   snapshot = newest world scrape (manifest.snapshot), history = upload-history as-of (manifest.history.asof).
//   anchor   = the older of the two (what the byline shows); date presets and open-ended ranges are expressed relative to it.
import type { Engine } from "../engine";
import { dateOfDay } from "../engine";

export interface Asof { snapshot: string | null; history: string | null; anchor: string; anchorDay: number }
export const DAY_MS = 86400000;
export const dayOfIso = (iso: string) => Math.round((Date.parse(iso.slice(0, 10) + "T00:00:00Z") - Date.UTC(2010, 0, 1)) / DAY_MS);

let cur: Asof = { snapshot: null, history: null, anchor: "", anchorDay: 0 };
export const asof = () => cur;

export function initAsof(eng: Engine): Asof {
  const snapshot = eng.m.snapshot?.slice(0, 10) || null;
  const history = eng.m.history?.asof?.slice(0, 10) || null;
  const known = [snapshot, history].filter(Boolean) as string[];
  const anchor = known.length ? known.sort()[0] : dateOfDay(eng.maxDay);
  cur = { snapshot, history, anchor, anchorDay: dayOfIso(anchor) };
  return cur;
}

export function asofTitle(a: Asof): string {
  const parts = [a.snapshot ? `World data as of ${a.snapshot}.` : "", a.history ? `Upload history: as of ${a.history}.` : "",
    "Data is refreshed weekly (Sundays): changes after these dates are not shown."].filter(Boolean);
  return parts.join(" ");
}
