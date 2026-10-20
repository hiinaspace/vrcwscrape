import "./shim.mjs";
const mod = await import(process.cwd() + "/" + process.argv[2]);
const e = await new mod.Engine().init();
const q = process.argv[3] ?? "";
for (let i = 0; i < 60; i++) await e.query({ q, limit: 24, whatIf: process.argv[4] === "w", tagLimit: 40 });
