import { chromium } from "playwright";
import fs from "fs";
const gold = JSON.parse(fs.readFileSync("/tmp/gold.json"));
const b = await chromium.launch(); const pg = await b.newPage();
await pg.goto("http://localhost:4317/"); await pg.evaluate(() => window.vrcwSearchReady);
const res = await pg.evaluate(async (gold) => {
  const out = []; const lat = [];
  for (const [kind, q, id] of gold) {
    const t = performance.now(); const r = await window.vrcwSearch.query({ q, limit: 10 }); const dt = performance.now() - t;
    out.push({ kind, q, hit: r.ids.includes(id), n: r.total, ms: dt });
  }
  // warm repeat
  for (const [kind, q] of gold.slice(0, 200)) { const t = performance.now(); await window.vrcwSearch.query({ q, limit: 10 }); lat.push(performance.now() - t); }
  lat.sort((a, b) => a - b);
  return { out, p50: lat[100], p95: lat[190], max: lat[199] };
}, gold);
const by = {};
for (const o of res.out) { by[o.kind] ??= [0, 0]; by[o.kind][1]++; if (o.hit) by[o.kind][0]++; }
console.log(JSON.stringify(by), "warm p50/p95/max", res.p50.toFixed(1), res.p95.toFixed(1), res.max.toFixed(1));
console.log(res.out.filter((o) => !o.hit).slice(0, 15).map((o) => `${o.kind} "${o.q}" total=${o.n}`).join("\n"));
await b.close();
