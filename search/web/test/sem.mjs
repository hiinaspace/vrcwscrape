// semantic check: JS embedding vs numpy reference (/tmp/semref.json), latency, sample hits
import { chromium } from "playwright"; import fs from "fs";
const ref = JSON.parse(fs.readFileSync("/tmp/semref.json"));
const b = await chromium.launch(); const pg = await b.newPage();
await pg.goto(process.argv[2] ?? "http://localhost:4317/"); await pg.evaluate(() => window.vrcwSearchReady);
const t0 = Date.now();
await pg.evaluate(() => window.vrcwSearch._semEncode("warmup"));
console.log("sem load ms", Date.now() - t0, "MB", Math.round(await pg.evaluate(() => window.vrcwStats.total) / 1e5) / 10);
const cos = [];
for (const [q, v] of Object.entries(ref)) {
  const u = await pg.evaluate((q) => window.vrcwSearch._semEncode(q), q);
  let d = 0, a = 0, c = 0; for (let i = 0; i < 256; i++) { d += u[i] * v[i]; a += u[i] * u[i]; c += v[i] * v[i]; }
  cos.push([q, d / Math.sqrt(a * c)]);
}
console.log("min cos", Math.min(...cos.map((x) => x[1])).toFixed(5), "n", cos.length, cos.filter((x) => x[1] < 0.99).map((x) => x[0]));
const lat = [];
for (const q of Object.keys(ref)) { for (let r = 0; r < 3; r++) await pg.evaluate((q) => window.vrcwSearch.semantic(q, 20), q); }
lat.push(...await pg.evaluate(() => window.vrcwStats.semMs));
lat.sort((a, b) => a - b);
console.log("semantic search ms (n=" + lat.length + ") p50", lat[lat.length >> 1], "p95", lat[Math.floor(lat.length * 0.95)], "max", lat.at(-1));
for (const q of ["horror escape room", "ホラー脱出", "cozy night cafe 夜のカフェ"]) {
  const ids = await pg.evaluate((q) => window.vrcwSearch.semantic(q, 5), q);
  const r = await pg.evaluate(async (ids) => { const out = []; for (const id of ids) { await window.vrcwSearch.query({ q: "", limit: 1 }); } return ids; }, ids);
  console.log(q, "->", ids.slice(0, 3));
}
// UI path
pg.on("dialog", (d) => d.accept());
await pg.goto((process.argv[2] ?? "http://localhost:4317/") + "?q=" + encodeURIComponent("horror escape room")); await pg.waitForSelector('[data-testid=result-item]');
await pg.check("#sem-toggle"); await pg.waitForSelector('[data-testid=semantic-item]', { timeout: 60000 });
console.log("semantic-items", await pg.locator('[data-testid=semantic-item] .title').allTextContents(), await pg.locator("#sem-ms").textContent());
await pg.screenshot({ path: "/tmp/sem.png", fullPage: false });
await b.close();
