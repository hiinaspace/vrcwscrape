// top-5 for a few queries: node test/top5.mjs <baseUrl> [q...]
import { chromium } from "playwright";
const base = process.argv[2]; const qs = process.argv.slice(3).length ? process.argv.slice(3) : ["horror", "club", "avatar", "chill", "spookyghostboo"];
const b = await chromium.launch(); const pg = await b.newPage();
for (const q of qs) {
  await pg.goto(base + "?q=" + encodeURIComponent(q) + "&adult=1"); await pg.waitForSelector('[data-testid=result-item]'); await pg.waitForTimeout(300);
  const rows = await pg.$$eval('[data-testid=result-item]', (els) => els.slice(0, 5).map((e) => (e.querySelector(".title")?.textContent ?? "") + " | " + (e.querySelector(".by")?.textContent ?? "") + " | " + (e.querySelector(".facts")?.textContent ?? "").split("·").slice(1, 3).join("/").trim()));
  console.log(q + "\n  " + rows.join("\n  "));
}
await b.close();
