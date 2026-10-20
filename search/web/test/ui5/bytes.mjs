import { launch } from "./launch.mjs";
const b = await launch("chromium");
for (const url of ["", "?q=chill", "?q=lake+-hangout", "?sort=published"]) {
  const p = await (await b.newContext({ viewport: { width: 1440, height: 900 } })).newPage();
  let bytes = 0, n = 0;
  p.on("response", async (r) => { try { bytes += (await r.body()).length; n++; } catch {} });
  const t0 = Date.now();
  await p.goto("http://localhost:5199/" + url);
  await p.waitForFunction(() => document.querySelectorAll("[data-testid=result-item]").length > 0);
  const first = Date.now() - t0;
  await p.waitForTimeout(1500);
  console.log(JSON.stringify(url), "first results", first, "ms", "total KB", Math.round(bytes / 1024), "requests", n);
  await p.close();
}
await b.close();
