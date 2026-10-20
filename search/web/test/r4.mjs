import { chromium } from "playwright";
const B = "http://localhost:4320/";
const b = await chromium.launch(); const pg = await b.newPage({ viewport: { width: 1440, height: 900 } });
const errs = []; pg.on("pageerror", (e) => errs.push(String(e))); pg.on("dialog", (d) => d.accept());
const sum = async (u) => { await pg.goto(B + u); await pg.waitForSelector('[data-testid=result-item]'); await pg.waitForTimeout(400); return pg.locator("#summary").textContent(); };
console.log("default", await sum(""), "| adult=1", await sum("?adult=1"));
console.log("adult toggle checked default:", await pg.locator("#adult-toggle").isChecked());
await pg.check("#adult-toggle"); await pg.waitForTimeout(500); console.log("after check", await pg.locator("#summary").textContent(), pg.url());
console.log("ls", await pg.evaluate(() => localStorage.getItem("vrcw-adult")));
console.log("tag ~chill", await sum("?adult=1&tags=~chill"), "chill only", await sum("?adult=1&tags=chill"));
console.log("tags facet top:", (await pg.locator("#f-tags li").allTextContents()).slice(0, 8).join(" | "));
console.log("platform btns", (await pg.locator("#f-plat button").allTextContents()).join(" | "));
await sum("?q=chill&platform=quest&adult=1"); console.log("what-if (platform=quest)", (await pg.locator("#f-plat button").allTextContents()).join(" | "));
console.log("more hidden when few:", await (async () => { await sum("?q=spookyghostboo&adult=1"); return pg.locator("#more").isVisible(); })());
console.log("sort label opts", (await pg.locator("#f-sort option").allTextContents()).join(", "));
// semantic strip
await pg.goto(B + "?q=" + encodeURIComponent("horror escape room") + "&adult=1"); await pg.waitForSelector('[data-testid=result-item]');
await pg.check("#sem-toggle"); await pg.waitForSelector('[data-testid=semantic-item]', { timeout: 60000 });
const y = await pg.locator('[data-testid=semantic-item]').first().boundingBox();
console.log("semantic strip y", Math.round(y.y), (await pg.locator('[data-testid=semantic-item] .title').allTextContents()).join(" | "));
await pg.waitForTimeout(1500); await pg.screenshot({ path: "/tmp/r4.png" });
// card, no heat
await pg.goto(B + "?world=wrld_4432ea9b-729c-46e3-8eaf-846aa0a37fdd"); await pg.waitForSelector("[data-testid=world-card]"); console.log("card has Heat:", (await pg.locator(".card").innerText()).includes("Heat"));
// quest-no-size row
await pg.goto(B + "?platform=quest&adult=1&size_max_mb=0"); await pg.waitForTimeout(500);
console.log(errs);
await b.close();
