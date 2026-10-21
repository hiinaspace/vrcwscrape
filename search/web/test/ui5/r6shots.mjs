import { launch } from "./launch.mjs";
import fs from "fs";
const base = process.argv[2], out = process.argv[3];
fs.mkdirSync(out, { recursive: true });
const b = await launch("chromium");
const errs = [];
async function shot(name, vp, url, act) {
  const ctx = await b.newContext({ viewport: vp, deviceScaleFactor: 1 });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => errs.push(name + ": " + e.message));
  p.on("console", (m) => m.type() === "error" && errs.push(name + " console: " + m.text()));
  await p.goto(base + url);
  await p.waitForFunction(() => /worlds/.test(document.querySelector("#summary")?.textContent ?? ""), null, { timeout: 30000 });
  await p.waitForTimeout(1500);
  if (act) await act(p);
  await p.waitForTimeout(1000);
  await p.screenshot({ path: out + "/" + name + ".png" });
  await ctx.close();
}
const D = { width: 1440, height: 900 }, M = { width: 390, height: 844 };
const W = (id) => "?world=" + id;
await shot("desktop-home", D, "");
await shot("mobile-home", M, "");
for (const [n, id] of Object.entries(JSON.parse(process.argv[4] ?? "{}"))) {
  await shot("desktop-detail-" + n, D, W(id), async (p) => { await p.waitForSelector("[data-testid=similar-list] li"); await p.waitForTimeout(800); });
  await shot("mobile-detail-" + n, M, W(id), async (p) => { await p.waitForSelector("[data-testid=similar-list] li"); await p.waitForTimeout(800); });
}
console.log("errors:", errs);
await b.close();
