import { launch } from "./launch.mjs";
const base = process.argv[2];
const b = await launch();
for (const url of ["", "?q=lake+-hangout", "?q=cozy+hangout", "?q=%E7%A5%9E%E7%A4%BE"]) {
  const p = await (await b.newContext({ viewport: { width: 1440, height: 900 } })).newPage();
  const items = [];
  p.on("response", async (r) => { try { const buf = await r.body(); items.push([r.url().replace(base, ""), buf.length]); } catch {} });
  await p.goto(base + url);
  await p.waitForFunction(() => document.querySelectorAll("[data-testid=result-item]").length > 0);
  await p.waitForTimeout(2500);
  const tot = items.reduce((s, x) => s + x[1], 0);
  const th = items.filter((x) => x[0].startsWith("thumbs")).reduce((s, x) => s + x[1], 0);
  items.sort((a, c) => c[1] - a[1]);
  console.log(JSON.stringify(url), "total KiB", (tot / 1024).toFixed(0), "thumbs KiB", (th / 1024).toFixed(0), "n", items.length, items.slice(0, 4).map((x) => x[0].slice(0, 28) + ":" + Math.round(x[1] / 1024)).join(" "));
  await p.close();
}
await b.close();
