import { chromium } from "playwright"; import fs from "fs";
const t = JSON.parse(fs.readFileSync("/tmp/sz.json"));
const b = await chromium.launch(); const pg = await b.newPage();
await pg.goto("http://localhost:4319/"); await pg.evaluate(() => window.vrcwSearchReady);
let bad = 0;
for (const [x, a, q, both, any] of t) {
  const g = await pg.evaluate(async (x) => { const o = []; for (const p of ["pc", "quest", "both", ""]) o.push((await window.vrcwSearch.query({ q: "", filters: { size_max_mb: x, platform: p }, limit: 1 })).total); return o; }, x);
  const e = [a, q, both, any]; if (g.join() !== e.join()) { bad++; console.log("MISMATCH", x, g, e); }
}
console.log("checked", t.length, "values x4, mismatches", bad);
await b.close();
