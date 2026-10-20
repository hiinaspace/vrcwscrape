import { chromium } from "playwright";
const b = await chromium.launch();
for (const q of ["chill", "ミラー", "cozy hangout", ""]) {
  const pg = await b.newPage(); const rs = [];
  pg.on("response", async (r) => { try { rs.push([new URL(r.url()).pathname, (await r.body()).length]); } catch {} });
  const t = Date.now(); await pg.goto("http://localhost:4317/?q=" + encodeURIComponent(q));
  await pg.waitForSelector('[data-testid=result-item]'); const ms = Date.now() - t; await pg.waitForTimeout(300);
  const data = rs.filter((x) => x[0].includes("/data/")); const tot = rs.reduce((a, x) => a + x[1], 0);
  console.log(JSON.stringify({ q, cold_first_results_ms: ms, requests: rs.length, total_KB: Math.round(tot / 1024), data_req: data.map((x) => x[0].split("/data/")[1] + ":" + Math.round(x[1] / 1024) + "K").join(" ") }));
  await pg.close();
}
// incremental: type a new query on warm page
const pg = await b.newPage(); await pg.goto("http://localhost:4317/"); await pg.evaluate(() => window.vrcwSearchReady);
for (const q of ["night club", "睡眠", "vr"]) {
  const before = await pg.evaluate(() => ({ ...window.vrcwStats }));
  const ms = await pg.evaluate(async (q) => { const t = performance.now(); await window.vrcwSearch.query({ q, limit: 50 }); return performance.now() - t; }, q);
  const after = await pg.evaluate(() => ({ ...window.vrcwStats }));
  console.log(JSON.stringify({ warm_page_new_query: q, ms: Math.round(ms), requests: after.requests - before.requests, KB: Math.round((after.total - before.total) / 1024) }));
}
await b.close();
