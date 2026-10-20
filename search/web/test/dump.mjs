// usage: node test/dump.mjs <enginebundle.mjs> <goldfile> <out.json> -- dumps top-10 idx for each gold query (regression vs old engine)
import "./shim.mjs";
import fs from "node:fs";
const mod = await import(process.cwd() + "/" + process.argv[2]);
const gold = fs.readFileSync(process.argv[3], "utf8").trim().split("\n").map((l) => JSON.parse(l).q);
const extra = ["chill", "horror", "club", "avatar", "night club", "lake", "ミラー", "睡眠", "cozy hang", "the", "vr chat", "a", "ab", "spookyghostboo", "Lounge -", "(Fixed for", "world of", "神社", "日本"];
const e = await new mod.Engine().init();
const out = {};
for (const q of [...gold, ...extra]) {
  const r = await (e.query ? e.query({ q, limit: 10 }) : e.run({ q, limit: 10 }));
  out[q] = { idx: r.idx, total: r.total };
}
fs.writeFileSync(process.argv[4], JSON.stringify(out));
console.log("dumped", Object.keys(out).length);
