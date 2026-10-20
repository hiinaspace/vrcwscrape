// node test/ui5/shots.mjs <base> <outdir> [chromium|firefox]
import { launch } from "./launch.mjs";
import fs from "fs";
const base = process.argv[2] ?? "http://localhost:5199/", out = process.argv[3] ?? "/work/tmp-ui5/ui5shots", kind = process.argv[4] ?? "chromium";
fs.mkdirSync(out, { recursive: true });
const b = await launch(kind);
const errs = [];
async function shot(name, vp, url, act) {
  const ctx = await b.newContext({ viewport: vp, deviceScaleFactor: 1 });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => errs.push(name + ": " + e.message));
  p.on("console", (m) => m.type() === "error" && errs.push(name + " console: " + m.text()));
  await p.goto(base + url);
  await p.waitForFunction(() => /worlds/.test(document.querySelector("#summary")?.textContent ?? ""), null, { timeout: 30000 });
  await p.waitForTimeout(600);
  if (act) await act(p);
  await p.waitForTimeout(500);
  await p.screenshot({ path: out + "/" + kind + "-" + name + ".png" });
  await ctx.close();
}
const D = { width: 1440, height: 900 }, M = { width: 390, height: 844 };
await shot("desktop-home", D, "");
await shot("desktop-query", D, "?q=" + encodeURIComponent("lake -hangout (cozy | chill) cap:>=4") + "&tags_ex=game&after=2024-01-01");
await shot("desktop-detail", D, "", async (p) => { await p.click("[data-testid=result-item] .title"); await p.waitForSelector("[data-testid=similar-list] li"); });
await shot("mobile-home", M, "");
await shot("mobile-query", M, "?q=" + encodeURIComponent("lake -hangout") + "&tags=chill");
await shot("mobile-detail", M, "", async (p) => { await p.click("[data-testid=result-item] .title"); await p.waitForSelector("[data-testid=similar-list] li"); });
await shot("mobile-filters", M, "", async (p) => { await p.click("#fbtn"); });
console.log("errors:", errs);
await b.close();
