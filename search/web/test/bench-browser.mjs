// usage: node test/bench-browser.mjs <baseUrl>/thumbtest.html -- engine latency in headless Chromium for complex queries (warm = shards cached)
import { chromium } from "playwright";
import os from "os";
const CH = os.homedir() + "/.cache/chromium-libs/usr/lib/x86_64-linux-gnu";
const b = await chromium.launch({ env: { ...process.env, LD_LIBRARY_PATH: CH } });
const pg = await b.newPage();
await pg.goto(process.argv[2]);
await pg.waitForFunction(() => window.ready, null, { timeout: 60000 });
const qs = ["lake -hangout", "(horror | scary) -tag:game cap:>=8", "tag:japan -cw:horror", "神社 OR shrine", "-hangout", '"escape room" tag:japan',
  "cafe | club -cw:adult platform:quest", "title:world -author:sam size:<100 visits:>10000", "published:2024 cw:horror", "(a | b | c | d) -e -f", "chill", "ミラー", ""];
const rows = await pg.evaluate(async (qs) => {
  const e = window.eng, out = [];
  for (const q of qs) {
    const t0 = performance.now();
    const r = await e.query({ q, limit: 24, sort: q ? "relevance" : "trending", whatIf: true, tagLimit: 40 });
    const cold = performance.now() - t0, ms = [];
    for (let k = 0; k < 20; k++) { const t = performance.now(); await e.query({ q, limit: 24, sort: q ? "relevance" : "trending", whatIf: true, tagLimit: 41 + k }); ms.push(performance.now() - t); }
    ms.sort((a, b) => a - b);
    out.push({ q, total: r.total, cold: +cold.toFixed(1), p50: +ms[10].toFixed(1), p95: +ms[18].toFixed(1) });
  }
  return out;
}, qs);
console.table(rows);
await b.close();
