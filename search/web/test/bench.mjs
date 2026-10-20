// usage: node test/bench.mjs <enginebundle.mjs> -- latency of complex queries in node (real index, no network)
import "./shim.mjs";
const mod = await import(process.cwd() + "/" + process.argv[2]);
const e = await new mod.Engine().init();
const qs = [
  "lake -hangout", "(horror | scary) -tag:game cap:>=8", "tag:japan -cw:horror", "神社 OR shrine", "-hangout", "\"escape room\" tag:japan",
  "cafe | club -cw:adult platform:quest", "title:world -author:sam size:<100 visits:>10000", "published:2024 cw:horror", "(a | b | c | d) -e -f",
  "lake -hangout updated:>=2025 cap:8..32 sort-trending",
];
const rows = [];
for (const q of qs) {
  const t0 = performance.now();
  const r0 = await e.query({ q, limit: 24, sort: "relevance", whatIf: true });
  const cold = performance.now() - t0;
  const ms = [];
  for (let k = 0; k < 15; k++) { const t = performance.now(); await e.query({ q, limit: 24, sort: k % 2 ? "trending" : "relevance", whatIf: true }); ms.push(performance.now() - t); }
  ms.sort((a, b) => a - b);
  rows.push({ q, total: r0.total, cold_ms: +cold.toFixed(1), warm_p50: +ms[7].toFixed(1), warm_max: +ms[14].toFixed(1) });
}
console.table(rows);
