import { launch } from "./launch.mjs";
const b = await launch(process.argv[2] ?? "firefox");
const p = await b.newPage();
await p.goto("http://localhost:5173/");
await p.waitForSelector("[data-testid=result-item]");
console.log(b.version(), await p.title());
await b.close();
