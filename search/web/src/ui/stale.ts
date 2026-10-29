// Stale metrics: visits/favorites/heat come from the world's latest metrics scrape, which can be months older than the data date.
// THE rule (the only place it is defined): stale when (data as-of - last metrics) > max(14 d, 0.25 x (last metrics - born)),
// born = later of publish/update date: the scraper's own age-aware refresh interval, or two weeks, whichever is longer.
export const STALE_MIN_DAYS = 14, STALE_K = 0.25;
export function isStale(asofDay: number, metricsDay: number, bornDay: number): boolean {
  if (!metricsDay) return false; // unknown: say nothing
  return asofDay - metricsDay > Math.max(STALE_MIN_DAYS, STALE_K * (metricsDay - bornDay));
}
