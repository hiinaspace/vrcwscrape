import { chromium } from "playwright"; import fs from "fs";
const d = JSON.parse(fs.readFileSync("/tmp/tagq.json"));
const b = await chromium.launch(); const pg = await b.newPage();
await pg.goto("http://localhost:4317/"); await pg.evaluate(() => window.vrcwSearchReady);
const out = await pg.evaluate(async (d) => {
  const run = async (q) => (await window.vrcwSearch.query({ q, limit: 10 })).ids;
  const tag = [], punct = [];
  for (const q of d.qs) tag.push(await run(q));
  for (const q of d.punct) punct.push(await run(q));
  return { tag, punct };
}, d);
fs.writeFileSync("/tmp/tagq_out.json", JSON.stringify(out)); await b.close();
