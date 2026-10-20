// usage: node test/thumbs-browser.mjs <baseUrl-with-thumbtest.html>  -- decodes real AVIFs fetched via Range in headless Chromium, reports bytes per page
import { chromium } from "playwright";
import os from "os";
const CH = os.homedir() + "/.cache/chromium-libs/usr/lib/x86_64-linux-gnu";
const base = process.argv[2];
const b = await chromium.launch({ env: { ...process.env, LD_LIBRARY_PATH: CH } });
const pg = await b.newPage();
const reqs = [];
pg.on("response", async (r) => { if (r.url().includes("/thumbs/")) reqs.push([r.status(), r.headers()["content-range"] ?? "", +(r.headers()["content-length"] ?? 0)]); });
pg.on("pageerror", (e) => console.log("pageerror", e));
await pg.goto(base);
await pg.waitForFunction(() => window.ready, null, { timeout: 60000 });
for (const sort of ["trending", "relevance"]) {
  for (const q of ["", "lake", "-hangout"]) {
    const res = await pg.evaluate(async ([q, sort]) => {
      const e = window.eng;
      const before = { ...e.thumbs.stats };
      const t0 = performance.now();
      const r = await e.query({ q, sort, limit: 24, facets: false });
      const urls = await Promise.all(r.idx.map((i) => e.thumbUrl(i)));
      const t1 = performance.now();
      const ok = [];
      await Promise.all(urls.map(async (u) => { if (!u) return; const im = new Image(); im.src = u; try { await im.decode(); ok.push(im.naturalWidth); } catch { ok.push(-1); } }));
      const s = e.thumbs.stats;
      return { with: urls.filter(Boolean).length, decoded: ok.filter((w) => w > 0).length, widths: [...new Set(ok)], ms: Math.round(t1 - t0), reqs: s.requests - before.requests, bytes: s.bytes - before.bytes, payload: s.payloadBytes - before.payloadBytes };
    }, [q, sort]);
    console.log(JSON.stringify({ q, sort, ...res }));
  }
}
console.log("http", JSON.stringify(reqs.slice(0, 6)), "total thumb responses", reqs.length, "statuses", [...new Set(reqs.map((r) => r[0]))]);
await b.close();
