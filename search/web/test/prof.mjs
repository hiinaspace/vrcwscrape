import "./shim.mjs";
const mod = await import(process.cwd() + "/" + process.argv[2]);
const e = await new mod.Engine().init();
let uniq = 0; // distinct tagLimit defeats the result cache so every call is a real evaluation
const time = async (label, f, n = 30) => { await f(); const a = []; for (let i = 0; i < n; i++) { const t = performance.now(); await f(); a.push(performance.now() - t); } a.sort((x, y) => x - y); console.log(label.padEnd(44), "p50", a[n >> 1].toFixed(1), "p90", a[Math.floor(n * 0.9)].toFixed(1)); };
for (const q of ["chill", "lake -hangout", "", "(horror | scary) -tag:game cap:>=8"]) {
  console.log("== q:", JSON.stringify(q));
  await time("facets+whatIf (UI)", () => e.query({ q, limit: 24, whatIf: true, tagLimit: 40 + (uniq++ % 1000) }));
  await time("facets (hook)", () => e.query({ q, limit: 50, tagLimit: 40 + (uniq++ % 1000) }));
  await time("no facets (paging)", () => e.query({ q, limit: 24, facets: false, tagLimit: 40 + (uniq++ % 1000) }));
  await time("no facets, sort visits", () => e.query({ q, limit: 24, facets: false, sort: "visits", tagLimit: 40 + (uniq++ % 1000) }));
}
