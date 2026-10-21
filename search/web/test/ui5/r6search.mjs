// round 6 search checks: recent filter, most-updated sort, uploads:/uploaded: syntax, testids
import { launch } from "./launch.mjs";
const base = process.argv[2];
const b = await launch(); const p = await b.newPage(); const errs = [];
p.on("pageerror", (e) => errs.push(e.message));
await p.goto(base); await p.evaluate(() => window.vrcwSearchReady);
const q = (inp) => p.evaluate((i) => window.vrcwSearch.query(i), inp);
const all = (await q({ q: "", limit: 1 })).total;
const r7 = await q({ q: "", filters: { recent_days: 7 }, limit: 1 }), r90 = await q({ q: "", filters: { recent_days: 90 }, limit: 1 });
const up5 = await q({ q: "uploads:>5", limit: 1 }), upd = await q({ q: "uploaded:>=2026-10", limit: 1 }), bad = await q({ q: "uploads:abc", limit: 1 });
const mu = await q({ q: "", sort: "uploads", limit: 3 });
console.log({ all, r7: r7.total, r90: r90.total, up5: up5.total, uploaded202610: upd.total, badErr: bad.parse.errors, mostUpdated: mu.ids });
const ok = r7.total > 0 && r7.total < r90.total && r90.total < all && up5.total > 0 && upd.total > 0;
// UI: chip click toggles filter + URL
await p.goto(base + "?recent=30"); await p.waitForFunction(() => /worlds/.test(document.querySelector("#summary")?.textContent ?? ""));
const on = await p.getAttribute("[data-testid=filter-recent-30]", "class");
await p.click("[data-testid=filter-recent-7]"); await p.waitForTimeout(600);
console.log("url", p.url(), "chip30 on:", on, "pill:", await p.locator("[data-testid=pill][data-kind=recent]").count());
await p.click("[data-testid=sort-uploads]"); await p.waitForTimeout(600); console.log("url", p.url(), await p.textContent("#summary"));
console.log("errors", errs, ok ? "OK" : "FAIL");
await b.close();
