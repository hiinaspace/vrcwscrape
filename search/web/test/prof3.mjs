import "./shim.mjs";
const mod = await import(process.cwd() + "/" + process.argv[2]);
const e = await new mod.Engine().init();
for (const [q, wi] of [["", false], ["", true], ["chill", false]]) {
  const acc = [0, 0, 0, 0]; const N = 20;
  for (let i = 0; i < N + 3; i++) { await e.query({ q, limit: 24, whatIf: wi, tagLimit: 40 + i }); if (i >= 3) globalThis.__ph.forEach((v, k) => (acc[k] += v / N)); }
  console.log(JSON.stringify(q), wi ? "whatIf" : "", "filterMask/mainloop/facetsBuild+whatIf/topK:", acc.map((x) => x.toFixed(1)).join(" / "));
}
