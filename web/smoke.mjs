import { chromium } from "playwright-core";

// Point CHROMIUM at a chrome/chromium binary; defaults to the playwright cache.
const EXE =
  process.env.CHROMIUM ||
  `${process.env.HOME}/.cache/ms-playwright/chromium-1223/chrome-linux64/chrome`;
const URL = process.env.URL || "http://localhost:5173/";

const browser = await chromium.launch({
  executablePath: EXE,
  args: ["--use-gl=angle", "--use-angle=swiftshader", "--no-sandbox"],
});
const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });

page.on("console", (m) => console.log(`[console.${m.type()}]`, m.text()));
page.on("pageerror", (e) => console.log("[pageerror]", e.message));

await page.goto(URL, { waitUntil: "networkidle" });
await page.waitForTimeout(8000);
const hasCanvas = (await page.$("canvas")) != null;
const loadingText = await page
  .$eval(".map-loading", (e) => e.textContent)
  .catch(() => null);
console.log("HAS CANVAS:", hasCanvas, "| loading div:", JSON.stringify(loadingText));
await page.screenshot({ path: "/tmp/map-fit.png" });
if (!hasCanvas) {
  await browser.close();
  process.exit(0);
}

const box = await page.$eval("canvas", (c) => {
  const r = c.getBoundingClientRect();
  return { x: r.x + r.width / 2, y: r.y + r.height / 2 };
});

await page.screenshot({ path: "/tmp/map-far.png" });

async function zoom(steps) {
  await page.mouse.move(box.x, box.y);
  for (let i = 0; i < steps; i++) {
    await page.mouse.wheel(0, -80);
    await page.waitForTimeout(140);
  }
  await page.waitForTimeout(1800);
}

await zoom(3); // -> mid tier
await page.screenshot({ path: "/tmp/map-mid.png" });

await zoom(5); // -> near tier (Voronoi cells + world titles)
await page.screenshot({ path: "/tmp/map-near.png" });

// select a world (via dev hook) to test the sidebar + cluster path + area list
const wid = await page.evaluate(() => window.__sampleWorldId);
await page.evaluate((id) => window.__select(id), wid);
await page.waitForTimeout(1200);
const sidebarTitle = await page
  .$eval(".sidebar .title", (e) => e.textContent)
  .catch(() => null);

const sidebar = await page.evaluate(() => ({
  crumbs: [...document.querySelectorAll(".sidebar .crumb")].map((e) => e.textContent),
  areaTitle: document.querySelector(".sidebar .area-title")?.textContent ?? null,
  areaCount: document.querySelectorAll(".sidebar .area-world").length,
}));

// hover near a cell to test the tooltip
await page.mouse.move(box.x + 60, box.y + 40);
await page.waitForTimeout(600);
await page.screenshot({ path: "/tmp/map-near.png" });

// click a breadcrumb (focus a cluster) and screenshot the highlight + flyTo
const vsBefore = await page.evaluate(() => window.__vs);
const crumb = await page.$(".sidebar .crumb");
if (crumb) {
  await crumb.click();
  await page.waitForTimeout(1600);
  await page.screenshot({ path: "/tmp/map-cluster.png" });
}
const vsAfter = await page.evaluate(() => window.__vs);

console.log("SIDEBAR TITLE AFTER CLICK:", JSON.stringify(sidebarTitle));
console.log("SIDEBAR:", JSON.stringify(sidebar));
console.log("VS BEFORE:", JSON.stringify(vsBefore), "AFTER:", JSON.stringify(vsAfter));

// ---------------------------------------------------------------------------
// Second pass: 2.5D extrude mode (?data=island-chen&extrude=1) smoke check.
// A fresh page + its own console/pageerror listeners, so this pass's
// assertions never mix with the default pass above. Only asserts canvas
// presence + zero console errors (a real visual review is a main-thread /
// Fable-vision task, not this script's job) + one screenshot for the record.
// ---------------------------------------------------------------------------
const extrudeErrors = [];
const extrudePage = await browser.newPage({ viewport: { width: 1400, height: 900 } });
extrudePage.on("console", (m) => {
  console.log(`[extrude console.${m.type()}]`, m.text());
  if (m.type() === "error") extrudeErrors.push(m.text());
});
extrudePage.on("pageerror", (e) => {
  console.log("[extrude pageerror]", e.message);
  extrudeErrors.push(e.message);
});

const extrudeUrl = `${URL}?data=island-chen&extrude=1`;
await extrudePage.goto(extrudeUrl, { waitUntil: "networkidle" });
await extrudePage.waitForTimeout(8000);
const extrudeHasCanvas = (await extrudePage.$("canvas")) != null;
console.log(
  "EXTRUDE HAS CANVAS:",
  extrudeHasCanvas,
  "| console errors:",
  extrudeErrors.length,
);
await extrudePage.screenshot({ path: "/tmp/map-extrude.png" });
if (!extrudeHasCanvas) {
  console.log("EXTRUDE SMOKE FAILED: no canvas");
  process.exitCode = 1;
}
if (extrudeErrors.length > 0) {
  console.log("EXTRUDE SMOKE FAILED: console errors present:", extrudeErrors);
  process.exitCode = 1;
}
await extrudePage.close();

await browser.close();
