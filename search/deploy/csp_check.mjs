// Drives headless Chromium against serve_headers.py (dist + its _headers, i.e. the CSP) and fails on any CSP violation,
// console error, failed request, or request to another origin. Exercises: first load, a text query, a facet click,
// opening the detail panel (similar worlds, thumbnails via blob: URLs), the about page, and semantic search (33 MB).
// usage: node search/deploy/csp_check.mjs http://127.0.0.1:5180/   (run on cc-0, where Chromium's libs live)
import { launch } from "../web/test/ui5/launch.mjs";

const base = process.argv[2] ?? "http://127.0.0.1:5180/";
const origin = new URL(base).origin;
const b = await launch("chromium");
const ctx = await b.newContext({ viewport: { width: 1440, height: 900 } });
const page = await ctx.newPage();
const problems = [], reqs = [], respHdr = {};
page.on("console", (m) => { if (["error", "warning"].includes(m.type())) problems.push(`console.${m.type()}: ${m.text()}`); });
page.on("pageerror", (e) => problems.push("pageerror: " + e.message));
page.on("requestfailed", (r) => problems.push(`requestfailed: ${r.url()} ${r.failure()?.errorText}`));
page.on("response", async (r) => {
  const u = new URL(r.url());
  reqs.push(u);
  if (u.origin === origin && r.status() >= 400) problems.push(`HTTP ${r.status()} ${u.pathname}`);
  const k = u.pathname.split("/")[1] || "/";
  respHdr[k] ??= { n: 0, status: {}, cc: new Set(), ct: new Set() };
  respHdr[k].n++; respHdr[k].status[r.status()] = (respHdr[k].status[r.status()] ?? 0) + 1;
  const h = r.headers(); respHdr[k].cc.add(h["cache-control"]); respHdr[k].ct.add(h["content-type"]);
});
await page.addInitScript(() => {
  window.__csp = [];
  document.addEventListener("securitypolicyviolation", (e) => window.__csp.push(`${e.violatedDirective} blocked ${e.blockedURI} @${e.sourceFile ?? ""}:${e.lineNumber ?? ""}`));
});
const step = async (name, fn) => { try { await fn(); console.log("ok  ", name); } catch (e) { problems.push(`${name}: ${e.message}`); console.log("FAIL", name, e.message); } };

await step("load + first results", async () => {
  await page.goto(base, { waitUntil: "load" });
  await page.waitForSelector('[data-testid="result-item"]', { timeout: 20000 });
});
await step("CSP header present on index", async () => {
  const r = await page.request.get(base);
  const csp = r.headers()["content-security-policy"];
  if (!csp?.includes("default-src 'none'")) throw new Error("no CSP header: " + csp);
  for (const h of ["x-content-type-options", "referrer-policy"]) if (!r.headers()[h]) throw new Error("missing " + h);
});
await step("text query", async () => {
  await page.fill('[data-testid="search-input"]', "lake -hangout");
  await page.waitForTimeout(800);
  const n = await page.locator('[data-testid="result-item"]').count();
  if (n < 5) throw new Error("only " + n + " results");
});
await step("facet click (tag include)", async () => {
  const btn = page.locator('[data-testid^="tag-include-"]').first();
  await btn.click(); await page.waitForTimeout(500);
});
await step("detail panel + similar worlds", async () => {
  await page.locator('[data-testid="result-item"]').first().click();
  await page.waitForSelector('[data-testid="world-card"]', { timeout: 10000 });
  await page.waitForTimeout(800);
});
const imgs = await page.evaluate(() => [...document.images].map((i) => [i.src.slice(0, 40), i.naturalWidth]));
console.log("images:", imgs.length, "decoded:", imgs.filter((x) => x[1] > 0).length, "blob:", imgs.filter((x) => x[0].startsWith("blob:")).length);
await step("semantic search on", async () => {
  await page.fill('[data-testid="search-input"]', "cozy cabin by a lake");
  await page.click('[data-testid="sem-toggle"]');
  await page.click('[data-testid="sem-enable"]', { timeout: 5000 });
  await page.waitForSelector('[data-testid="semantic-item"]', { timeout: 60000 });
});
await step("about page", async () => {
  const p2 = await ctx.newPage();
  p2.on("console", (m) => { if (m.type() === "error") problems.push("about console.error: " + m.text()); });
  await p2.addInitScript(() => { window.__csp = []; document.addEventListener("securitypolicyviolation", (e) => window.__csp.push(e.violatedDirective + " " + e.blockedURI)); });
  await p2.goto(new URL("about.html", base).href, { waitUntil: "load" });
  const t = await p2.textContent("body");
  if (!/hiina@hiina\.space/.test(t) || t.includes("@@")) throw new Error("about content wrong");
  const bg = await p2.evaluate(() => getComputedStyle(document.body).backgroundColor);
  if (!bg || bg === "rgba(0, 0, 0, 0)") throw new Error("about.css not applied");
  const v = await p2.evaluate(() => window.__csp);
  if (v.length) problems.push("about CSP: " + v.join("; "));
  await p2.close();
});
// follow the footer link from the app
await step("About link from app", async () => {
  const a = page.locator('a[href$="about.html"]').first();
  if (!(await a.count())) throw new Error("no about link");
});
const viol = await page.evaluate(() => window.__csp);
const foreign = [...new Set(reqs.filter((u) => u.origin !== origin && u.protocol !== "blob:" && u.protocol !== "data:").map((u) => u.origin))];
const unversioned = [...new Set(reqs.filter((u) => /^\/(data|thumbs)\//.test(u.pathname) && u.pathname !== "/data/manifest.json" && !u.searchParams.has("v")).map((u) => u.pathname))];
console.log("requests:", reqs.length, "| cross-origin:", foreign.length ? foreign : "none", "| unversioned data/thumbs requests:", unversioned.length ? unversioned.slice(0, 5) : "none");
for (const [k, v] of Object.entries(respHdr)) console.log(`  /${k}: n=${v.n} status=${JSON.stringify(v.status)} cache-control=${[...v.cc].join(" || ")} ct=${[...v.ct].join(" || ")}`);
console.log("CSP violations:", viol.length ? viol : "none");
if (viol.length) problems.push(...viol.map((v) => "CSP: " + v));
if (foreign.length) problems.push("cross-origin requests: " + foreign.join(","));
if (unversioned.length) problems.push("unversioned data requests: " + unversioned.join(","));
console.log(problems.length ? "PROBLEMS:\n  " + problems.join("\n  ") : "CSP CHECK PASSED");
await b.close();
process.exit(problems.length ? 1 : 0);
