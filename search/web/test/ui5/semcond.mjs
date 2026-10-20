// Semantic strip must honour typed conditions: enable, search "quiet forest cabin cap:>=40 -tag:game"; all strip hits need cap >= 40.
import { launch } from "./launch.mjs";
const b = await launch(process.argv[3] ?? "chromium");
const p = await (await b.newContext({ viewport: { width: 1280, height: 800 } })).newPage();
await p.goto((process.argv[2] ?? "http://localhost:5199/") + "?q=" + encodeURIComponent("quiet forest cabin cap:>=40"));
await p.waitForSelector("[data-testid=result-item]");
await p.click("[data-testid=sem-toggle]"); await p.click("[data-testid=sem-enable]");
await p.waitForFunction(() => document.querySelectorAll("[data-testid=semantic-item]").length > 0, null, { timeout: 60000 });
const caps = await p.$$eval("[data-testid=semantic-item] .m.cap b", (els) => els.map((e) => +e.textContent));
const bad = caps.filter((c) => !(c >= 40));
console.log("strip caps", caps.join(","));
console.log("strip items", caps.length, "violating cap>=40:", bad.length);
await b.close();
