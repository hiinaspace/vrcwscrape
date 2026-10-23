// Pre-launch checks: staleness byline, no "now"-style relative strings, semantic assets cached in Cache Storage (no second confirm).
// usage: node launchcheck.mjs <base-url> <chromium|firefox> <outdir>
import { launch } from "./launch.mjs";
import fs from "fs";
const [base, kind = "chromium", out = "/tmp/launchshots"] = process.argv.slice(2);
fs.mkdirSync(out, { recursive: true });
const b = await launch(kind);
let fails = 0;
const ok = (c, m) => { console.log(c ? "ok  " : "FAIL", m); if (!c) fails++; };
const BAD = /\bnow\b|\bago\b|this (week|month|year)|\btoday\b|yesterday|last (week|month|year)/i;
// UI chrome only: world-authored text (titles, authors, tags, descriptions) may legitimately say "now"/"today"
const text = (p) => p.evaluate(() => {
  const c = document.body.cloneNode(true);
  c.querySelectorAll(".rdesc,.title,.by,.tg,.tag,.desc,h2,#sem-list,[data-testid=detail-desc]").forEach((e) => e.remove());
  return c.textContent + "\n" + [...document.querySelectorAll("[title]")].filter((e) => !e.closest("li[data-id]")).map((e) => e.getAttribute("title")).join("\n");
});
const ready = async (p, url) => { await p.goto(url); await p.waitForFunction(() => window.vrcwSearchReady); await p.waitForSelector("[data-testid=result-item]"); };

for (const [name, vp] of [["desktop", { width: 1280, height: 800 }], ["mobile", { width: 390, height: 844 }]]) {
  const ctx = await b.newContext({ viewport: vp });
  const p = await ctx.newPage();
  await ready(p, base);
  const by = await p.$eval("#asof", (e) => ({ t: e.textContent, title: e.title, r: e.getBoundingClientRect().toJSON(), vis: getComputedStyle(e).display !== "none" }));
  ok(/^Data as of \d{4}-\d{2}-\d{2}$/.test(by.t) && by.vis && by.r.width > 0 && by.r.right <= vp.width + 1 && by.r.left >= 0, `${kind} ${name} byline "${by.t}" visible in viewport (${Math.round(by.r.left)}..${Math.round(by.r.right)} of ${vp.width})`);
  if (name === "desktop") console.log("     tooltip:", by.title);
  ok((await p.textContent(".aboutlink")).trim() === "About", `${name} header link is "About"`);
  let t = await text(p);
  ok(!BAD.test(t), `${name} home: no now/ago strings` + (BAD.test(t) ? " -> " + t.match(BAD)[0] : ""));
  await p.screenshot({ path: `${out}/${kind}-${name}-home.png` });
  // recently updated filter + date preset + pills
  await ready(p, base + "?recent=30");
  t = await text(p);
  ok(!BAD.test(t), `${name} recent=30 view: no now/ago` + (BAD.test(t) ? " -> " + t.match(BAD)[0] : ""));
  console.log("     recent note:", await p.evaluate(() => document.querySelector("#f-recentnote")?.textContent), "|", await p.evaluate(() => document.querySelector("[data-testid=pills]")?.innerText));
  if (name === "desktop") {
    await p.click("[data-testid=date-preset-week]");
    await p.waitForTimeout(400);
    t = await text(p);
    ok(!BAD.test(t), "preset 7 days: no now/ago");
    console.log("     date label:", await p.textContent("#f-datelabel"), "|", await p.textContent("#f-anchor"), "| pills:", await p.evaluate(() => document.querySelector("[data-testid=pills]")?.innerText));
    await p.click("[data-testid=date-preset-month]"); await p.waitForTimeout(200);
    await p.evaluate(() => { const s = document.querySelector("[data-testid=pills]"); });
  }
  // detail with history
  const id = await p.evaluate(async () => (await window.vrcwSearch.query({ q: "uploads:>=8 cap:>=8", filters: {}, sort: "uploads", limit: 3 })).ids[0]);
  await ready(p, base + "?sort=uploads&world=" + id);
  await p.waitForSelector("[data-testid=history-summary]", { timeout: 30000 });
  await p.waitForSelector("[data-testid=upload-timeline]");
  t = await text(p);
  ok(!BAD.test(t), `${name} detail+history: no now/ago` + (BAD.test(t) ? " -> " + t.match(BAD)[0] : ""));
  console.log("     summary:", await p.textContent("[data-testid=history-summary]"));
  await p.waitForTimeout(500);
  await p.screenshot({ path: `${out}/${kind}-${name}-detail-history.png` });
  await ctx.close();
}

// ---- semantic cache behaviour (secure context: 127.0.0.1)
{
  const ctx = await b.newContext({ viewport: { width: 1280, height: 800 } });
  const p = await ctx.newPage();
  await ready(p, base);
  const confirmVisible = () => p.$eval(".sem-confirm", (e) => !e.hidden);
  const state = () => p.$eval("#sem-host", (e) => e.dataset.state);
  await p.click("[data-testid=sem-toggle]");
  await p.waitForFunction(() => document.querySelector("#sem-host").dataset.state === "confirm");
  ok(await confirmVisible(), "first enable: confirm dialog shown");
  ok(await p.$eval("[data-testid=sem-soft]", (e) => e.hidden), "first enable: soft text hidden (Cache Storage available)");
  await p.click("[data-testid=sem-enable]");
  await p.waitForFunction(() => document.querySelector("#sem-host").dataset.state === "on", null, { timeout: 120000 });
  const net1 = await p.evaluate(() => window.vrcwStats.total);
  console.log("     note:", await p.textContent(".sem-note"), "| network bytes so far", net1);
  await p.click("[data-testid=sem-toggle]");
  ok((await state()) === "off", "disabled");
  const b0 = await p.evaluate(() => window.vrcwStats.total);
  await p.click("[data-testid=sem-toggle]");
  await p.waitForFunction(() => ["on", "confirm", "error"].includes(document.querySelector("#sem-host").dataset.state), null, { timeout: 60000 });
  ok((await state()) === "on" && !(await confirmVisible()), "re-enable: no dialog, state on");
  const note = await p.textContent(".sem-note");
  const b1 = await p.evaluate(() => window.vrcwStats.total);
  ok(/loaded from cache/.test(note), `re-enable note "${note}"`);
  ok(b1 - b0 < 1e6, `re-enable network bytes ${b1 - b0}`);
  await p.screenshot({ path: `${out}/${kind}-sem-reenabled.png` });
  // reload: remembered "0" after disable => off; toggle => no dialog
  await p.click("[data-testid=sem-toggle]"); // disable again (sets remembered=0)
  await ready(p, base);
  ok((await state()) === "off", "after reload: off");
  const r0 = await p.evaluate(() => window.vrcwStats.total);
  await p.click("[data-testid=sem-toggle]");
  await p.waitForFunction(() => ["on", "confirm", "error"].includes(document.querySelector("#sem-host").dataset.state), null, { timeout: 60000 });
  ok((await state()) === "on", "after reload: toggle enables without dialog; net bytes " + ((await p.evaluate(() => window.vrcwStats.total)) - r0));
  // reload while on => auto-enable from cache
  await ready(p, base);
  await p.waitForFunction(() => document.querySelector("#sem-host").dataset.state === "on", null, { timeout: 60000 });
  ok(true, "reload with remembered consent: auto-on from cache");
  // semantic query works from cached assets
  const ids = await p.evaluate(async () => await window.vrcwSearch.semantic("cozy night cabin", 5));
  ok(ids.length === 5, "semantic query returns 5 from cached assets");
  const keys = await p.evaluate(async () => await caches.keys());
  console.log("     caches:", keys.join(","));
  await ctx.close();
}
// ---- Cache Storage unavailable: softer dialog, still works
{
  const ctx = await b.newContext({ viewport: { width: 1280, height: 800 } });
  const p = await ctx.newPage();
  await p.addInitScript(() => { Object.defineProperty(window, "caches", { get() { throw new DOMException("denied", "SecurityError"); } }); });
  await ready(p, base);
  await p.click("[data-testid=sem-toggle]");
  await p.waitForFunction(() => document.querySelector("#sem-host").dataset.state === "confirm");
  ok(await p.$eval("[data-testid=sem-soft]", (e) => !e.hidden), "no Cache Storage: softer text shown with the dialog");
  console.log("     soft:", await p.textContent("[data-testid=sem-soft]"));
  await p.click("[data-testid=sem-enable]");
  await p.waitForFunction(() => document.querySelector("#sem-host").dataset.state === "on", null, { timeout: 120000 });
  ok(true, "no Cache Storage: enable still works (HTTP cache only)");
  await ctx.close();
}
await b.close();
console.log(fails ? `${fails} FAILED` : "ALL OK");
process.exit(fails ? 1 : 0);
