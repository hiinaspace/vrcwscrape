// Interaction checks with real clicks/drags. node test/ui5/interact.mjs <base> <chromium|firefox>
import { launch } from "./launch.mjs";
const base = process.argv[2] ?? "http://localhost:5199/", kind = process.argv[3] ?? "chromium";
const b = await launch(kind);
let fails = 0;
const ok = (name, cond, extra = "") => { if (!cond) fails++; console.log((cond ? "PASS " : "FAIL ") + name + (extra ? "  " + extra : "")); };
const errs = [];
async function page(vp = { width: 1440, height: 900 }, url = "") {
  const c = await b.newContext({ viewport: vp });
  const p = await c.newPage();
  p.on("pageerror", (e) => errs.push(e.message));
  await p.goto(base + url);
  await p.waitForFunction(() => /worlds/.test(document.querySelector("#summary")?.textContent ?? ""));
  return p;
}
const sp = (p) => new URL(p.url()).searchParams;
const settle = (p) => p.waitForTimeout(450);

let p = await page();
// sort defaults + explicit sticks
ok("default sort is trending", await p.locator("[data-testid=sort-trending]").getAttribute("aria-pressed") === "true");
await p.fill("#q", "cabin"); await settle(p);
ok("query -> relevance auto", await p.locator("[data-testid=sort-relevance]").getAttribute("aria-pressed") === "true");
await p.click("[data-testid=sort-visits]"); await settle(p);
ok("explicit sort in URL", sp(p).get("sort") === "visits");
await p.fill("#q", ""); await p.fill("#q", "lake"); await settle(p);
ok("explicit sort sticks", await p.locator("[data-testid=sort-visits]").getAttribute("aria-pressed") === "true");
await p.click("[data-testid=sort-visits]"); await settle(p);
ok("same chip again flips direction", sp(p).get("asc") === "1");
// tag include / exclude + pills
await p.fill("#q", ""); await settle(p);
await p.click("[data-testid=sort-trending]"); await settle(p);
const firstTag = await p.locator("#f-tags .frow").first().getAttribute("data-k");
await p.click("[data-testid=tag-include-" + firstTag.replace(/^~/, "") + "]"); await settle(p);
ok("tag include -> URL", sp(p).get("tags") === firstTag, firstTag);
ok("tag pill rendered", await p.locator("[data-testid=pill][data-kind=tag][data-neg=false]").count() === 1);
await p.click("[data-testid=pill] [data-testid=pill-sign]"); await settle(p);
ok("pill sign flips to exclude", sp(p).get("tags_ex") === firstTag && !sp(p).get("tags"));
await p.click("[data-testid=pill-remove]"); await settle(p);
ok("pill remove", !sp(p).get("tags_ex") && await p.locator("[data-testid=pill]").count() === 0);
// query pills
await p.fill("#q", "lake -hangout cap:>=16"); await settle(p);
ok("3 query pills", await p.locator("[data-testid=pill][data-src=query]").count() === 3);
await p.locator("[data-testid=pill][data-src=query]").nth(0).locator("[data-testid=pill-sign]").click(); await settle(p);
ok("query pill flip rewrites q", (await p.inputValue("#q")).startsWith("-lake"), await p.inputValue("#q"));
await p.locator("[data-testid=pill][data-src=query]").nth(1).locator("[data-testid=pill-remove]").click(); await settle(p);
ok("query pill remove rewrites q", !(await p.inputValue("#q")).includes("hangout"), await p.inputValue("#q"));
await p.fill("#q", ""); await settle(p);
// syntax help
await p.click("[data-testid=syntax-help]");
ok("syntax popover opens", await p.locator("#syntax-pop").isVisible());
await p.locator("#syntax-pop .ex").first().click(); await settle(p);
ok("syntax example fills query", (await p.inputValue("#q")).includes("lake"), await p.inputValue("#q"));
await p.fill("#q", ""); await settle(p);
// capacity double range via keyboard
await p.focus("[data-testid=cap-range] .dr-lo");
for (let i = 0; i < 3; i++) await p.keyboard.press("ArrowRight");
await settle(p);
ok("cap_min from slider", sp(p).get("cap_min") === "8", sp(p).get("cap_min"));
await p.focus("[data-testid=cap-range] .dr-hi");
for (let i = 0; i < 4; i++) await p.keyboard.press("ArrowLeft");
await settle(p);
ok("cap_max from slider", sp(p).get("cap_max") === "32", sp(p).get("cap_max"));
// date histogram brush drag
const plot = p.locator("[data-testid=date-hist] .hist-plot");
await plot.scrollIntoViewIfNeeded(); const bb = await plot.boundingBox();
await p.mouse.move(bb.x + bb.width * 0.7, bb.y + 30); await p.mouse.down();
await p.mouse.move(bb.x + bb.width * 0.85, bb.y + 30, { steps: 5 }); await p.mouse.up(); await settle(p);
ok("brush sets updated range", !!sp(p).get("after") && !!sp(p).get("before") , (sp(p).get("after") + ".." + sp(p).get("before")));
await p.click("[data-testid=datef-published]"); await settle(p);
ok("published switch carries the brushed range over", !sp(p).get("after") && !!sp(p).get("pub_after") && sp(p).get("datef") === "published");
await p.click("[data-testid=date-preset-month]"); await settle(p);
ok("preset month sets pub_after", !!sp(p).get("pub_after") && !sp(p).get("pub_before"), sp(p).get("pub_after"));
await p.click("[data-testid=date-preset-any]"); await settle(p);
ok("preset any clears published range", !sp(p).get("pub_after"));
// year slider
await p.focus("[data-testid=date-years] .dr-lo");
for (let i = 0; i < 4; i++) await p.keyboard.press("ArrowRight");
await settle(p);
ok("year slider sets pub_after to Jan 1", /^20\d\d-01-01$/.test(sp(p).get("pub_after") ?? ""), sp(p).get("pub_after"));
await p.click("#f-reset"); await settle(p);
ok("reset clears URL filters", ![...sp(p).keys()].some((k) => ["cap_min","cap_max","after","before","pub_after","tags"].includes(k)), p.url());
// platform + cw rows
await p.click("[data-testid=platform-include-quest]"); await settle(p);
ok("platform include", sp(p).get("platform") === "quest");
await p.click("[data-testid=platform-exclude-pc]"); await settle(p);
ok("platform exclude", sp(p).get("platform_ex") === "pc");
const cw = await p.locator("#f-cw .frow").first().getAttribute("data-k");
await p.click("[data-testid=cw-exclude-" + cw + "]"); await settle(p);
ok("cw exclude", sp(p).get("cw_exclude") === cw, cw);
await p.click("#f-reset"); await settle(p);
// ---- 5e: date orphan, history, typed tag state, author pill
await p.goto(base); await p.waitForFunction(() => /worlds/.test(document.querySelector("#summary")?.textContent ?? "")); await settle(p);
await p.click("[data-testid=date-preset-week]"); await settle(p);
ok("week preset sets after", !!sp(p).get("after"));
await p.click("[data-testid=datef-published]"); await settle(p);
ok("switching to Published carries the range (no orphan after)", !sp(p).get("after") && !!sp(p).get("pub_after") && sp(p).get("datef") === "published", p.url());
ok("date facet shows the active range", (await p.textContent("#f-datelabel")).includes("published"), await p.textContent("#f-datelabel"));
await p.click("[data-testid=datef-updated]"); await settle(p);
ok("switching back carries it back", !!sp(p).get("after") && !sp(p).get("pub_after"));
await p.click("[data-testid=datef-published]"); await settle(p);
await p.click("[data-testid=date-preset-any]"); await settle(p);
ok("any clears every date param", !sp(p).get("after") && !sp(p).get("pub_after") && !sp(p).get("before") && !sp(p).get("pub_before") && (await p.textContent("#f-datelabel")) === "any time");
await p.goto(base + "?after=2024-01-01&pub_before=2023-12-31"); await p.waitForFunction(() => /worlds/.test(document.querySelector("#summary")?.textContent ?? "")); await settle(p);
ok("both ranges reflected by the facet label", /updated.*published/.test(await p.textContent("#f-datelabel")), await p.textContent("#f-datelabel"));
await p.click("[data-testid=date-preset-any]"); await settle(p);
// history
await p.goto(base); await p.waitForFunction(() => /worlds/.test(document.querySelector("#summary")?.textContent ?? "")); await settle(p);
const tg = await p.locator("#f-tags .frow").first().getAttribute("data-k");
await p.click("[data-testid=tag-include-" + tg.replace(/^~/, "") + "]"); await settle(p);
await p.click("[data-testid=sort-visits]"); await settle(p);
ok("two discrete actions in URL", sp(p).get("tags") === tg && sp(p).get("sort") === "visits");
await p.goBack(); await settle(p);
ok("back undoes the sort only", sp(p).get("tags") === tg && !sp(p).get("sort") && await p.locator("[data-testid=pill]").count() === 1, p.url());
await p.goBack(); await settle(p);
ok("back again undoes the tag incl. pill", !sp(p).get("tags") && await p.locator("[data-testid=pill]").count() === 0, p.url());
await p.goForward(); await settle(p);
ok("forward restores tag + pill", sp(p).get("tags") === tg && await p.locator("[data-testid=pill]").count() === 1);
await p.goBack(); await settle(p);
await p.click("#q"); await p.keyboard.type("lake", { delay: 60 }); await p.waitForTimeout(500);
await p.keyboard.type(" -hang", { delay: 60 }); await settle(p);
const len = await p.evaluate(() => history.length);
await p.goBack(); await settle(p);
ok("typing is one history entry: back clears the whole query", (await p.inputValue("#q")) === "" && !sp(p).get("q"), await p.inputValue("#q"));
await p.goForward(); await settle(p);
ok("forward restores typed query + pills", (await p.inputValue("#q")) === "lake -hang" && await p.locator("[data-testid=pill][data-src=query]").count() === 2, await p.inputValue("#q"));
// typed tag state lights the facet
await p.fill("#q", "tag:" + tg.replace(/^~/, "")); await settle(p);
ok("tag:x typed lights the + button", await p.locator("[data-testid=tag-include-" + tg.replace(/^~/, "") + "]").first().getAttribute("aria-pressed") === "true");
await p.fill("#q", "-tag:" + tg.replace(/^~/, "")); await settle(p);
ok("-tag:x typed lights the - button", await p.locator("[data-testid=tag-exclude-" + tg.replace(/^~/, "") + "]").first().getAttribute("aria-pressed") === "true");
await p.locator("[data-testid=tag-exclude-" + tg.replace(/^~/, "") + "]").first().click(); await settle(p);
ok("clicking the lit - removes the typed condition", (await p.inputValue("#q")) === "" , await p.inputValue("#q"));
await p.fill("#q", "tag:japan"); await settle(p);
ok("typed rare-ish tag gets a row", await p.locator("[data-testid=tag-include-japan]").count() >= 1);
await p.fill("#q", ""); await settle(p);
// author pill shows the name
await p.locator("[data-testid=result-item] .rdesc").first().click();
await p.waitForSelector("[data-testid=similar-list]");
const an = (await p.textContent("#c-author")).trim();
await p.click("#c-author"); await settle(p);
const apill = await p.locator("[data-testid=pill][data-kind=author] .plabel").textContent();
ok("author pill shows the name, not usr_ id", !/usr_/.test(apill) && apill.includes(an), apill);
await p.click("#f-reset"); await settle(p);
ok("desc is line-clamped", (await p.locator(".rdesc").first().evaluate((e) => getComputedStyle(e).webkitLineClamp)) === "2");
// detail panel (desktop)
const n0 = await p.locator("[data-testid=result-item]").count();
await p.locator("[data-testid=result-item] .title").nth(1).click();
await p.waitForSelector("[data-testid=similar-list] li");
ok("detail panel visible beside list", await p.locator("[data-testid=detail-panel]").isVisible() && await p.locator("[data-testid=world-card]").isVisible());
ok("URL has world=", !!sp(p).get("world"));
ok("selected row highlighted", await p.locator("[data-testid=result-item].sel").count() === 1);
ok("list still present", await p.locator("[data-testid=result-item]").count() === n0);
const hero = await p.locator("[data-testid=world-card] a.hero").getAttribute("href");
ok("hero links to official page", /^https:\/\/vrchat\.com\/home\/world\/wrld_/.test(hero), hero);
// whole-row click, keyboard, inner controls
await p.click("[data-testid=detail-panel] .x"); await settle(p);
await p.locator("[data-testid=result-item] .rdesc").nth(2).click(); await settle(p);
ok("clicking row body opens panel", await p.locator("[data-testid=detail-panel]").isVisible() && await p.locator("[data-testid=result-item].sel").count() === 1);
const w1 = sp(p).get("world");
await p.locator("[data-testid=result-item]").nth(3).locator(".tag").first().click(); await settle(p);
ok("tag chip click filters, row not opened", !!sp(p).get("tags") && sp(p).get("world") === w1, p.url());
await p.click("#f-reset"); await settle(p);
await p.click("[data-testid=detail-panel] .x"); await settle(p);
await p.locator("[data-testid=result-item]").nth(1).focus(); await p.keyboard.press("Enter"); await settle(p);
ok("Enter on focused row opens panel", !!sp(p).get("world"));
await p.locator("[data-testid=result-item]").nth(4).focus(); await p.keyboard.press("Space"); await settle(p);
ok("Space on focused row opens other world", !!sp(p).get("world") && sp(p).get("world") !== w1);
await p.click("[data-testid=detail-panel] .x"); await settle(p);
ok("panel closes", !(await p.locator("[data-testid=detail-panel]").isVisible()) && !sp(p).get("world"));
await p.goBack(); await p.waitForTimeout(500);
ok("back reopens panel", await p.locator("[data-testid=detail-panel]").isVisible());
await p.close();
// deep link
p = await page(undefined, "?world=wrld_4432ea9b-729c-46e3-8eaf-846aa0a37fdd");
await p.waitForSelector("[data-testid=similar-list]");
ok("world= deep link opens panel", await p.locator("[data-testid=world-card] h2").textContent() !== "");
await p.close();
// mobile
p = await page({ width: 390, height: 844 });
await p.locator("[data-testid=result-item] .title").first().click();
await p.waitForSelector("[data-testid=similar-list] li");
const bx = await p.locator("[data-testid=detail-panel]").boundingBox();
ok("mobile sheet is full-screen", Math.abs(bx.width - 390) < 2 && bx.y <= 1, JSON.stringify(bx));
await p.click("[data-testid=detail-panel] .back"); await settle(p);
ok("mobile back closes sheet", !(await p.locator("[data-testid=detail-panel]").isVisible()));
const sw = await p.evaluate(() => document.documentElement.scrollWidth);
ok("no horizontal overflow at 390px", sw <= 391, "scrollWidth=" + sw);
ok("sort chips show an edge fade when scrollable", await p.locator("#sorts.fade-r").count() === 1);
await p.click("#fbtn"); await settle(p);
ok("mobile filter drawer opens", await p.locator("#facets").isVisible());
const pb = await p.locator("#f-tags .pm").first().boundingBox();
ok("mobile +/- hit area >= 32px", pb.width >= 32 && pb.height >= 32, JSON.stringify(pb));
await p.close();
console.log("errors:", errs, "fails:", fails);
await b.close();
