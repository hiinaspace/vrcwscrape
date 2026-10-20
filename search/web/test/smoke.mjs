// Quick Playwright smoke: node test/smoke.mjs [baseUrl]
import { chromium } from "playwright";
const base = process.argv[2] ?? "http://localhost:4317/";
const b = await chromium.launch();
const pg = await b.newPage();
const errs = [];
pg.on("console", (m) => m.type() === "error" && errs.push(m.text()));
pg.on("pageerror", (e) => errs.push(String(e)));
let bytes = 0;
pg.on("response", async (r) => { try { bytes += (await r.body()).length; } catch {} });
const t0 = Date.now();
await pg.goto(base + "?q=" + encodeURIComponent(process.env.Q ?? "chill"));
await pg.waitForSelector('[data-testid=result-item]');
const first = Date.now() - t0;
await pg.evaluate(() => window.vrcwSearchReady);
const out = { firstResultsMs: first, transferKB: Math.round(bytes / 1024), errs };
out.items = await pg.locator('[data-testid=result-item]').count();
out.titles = await pg.locator('[data-testid=result-item] .title').allTextContents().then((x) => x.slice(0, 5));
const qs = [
  { q: "", filters: {}, sort: "visits", limit: 3 },
  { q: "", filters: { cap_min: 10, cap_max: 40, platform: "quest", size_max_mb: 50 } },
  { q: "", filters: { after: "2025-01-01", before: "2025-06-30", tags: ["chill"] } },
  { q: "", filters: { tags: ["system_approved", "chill"], platform: "both" } },
  { q: "ミラー", filters: {} },
  { q: "睡眠", filters: {} },
  { q: "cozy hang", filters: {} },
];
out.q = [];
for (const q of qs) {
  const r = await pg.evaluate(async (q) => { const r = await window.vrcwSearch.query(q); return { total: r.total, ms: r.ms, plat: r.facetCounts.platform, tags: Object.entries(r.facetCounts.tags).slice(0, 3), ids: r.ids.slice(0, 2) }; }, q);
  out.q.push({ in: JSON.stringify(q), ...r });
}
out.sim = await pg.evaluate(async () => { const r = await window.vrcwSearch.query({ q: "", sort: "visits", limit: 1 }); return (await window.vrcwSearch.similar(r.ids[0], 5)); });
// card
await pg.goto(base + "?world=" + out.q[0].ids[0]);
await pg.waitForSelector('[data-testid=world-card]');
await pg.waitForSelector('[data-testid=similar-list] li');
out.card = await pg.locator('[data-testid=world-card] h2').textContent();
out.similarItems = await pg.locator('[data-testid=similar-list] li').count();
console.log(JSON.stringify(out, null, 1));
await b.close();
