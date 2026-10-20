// usage: node test/run.mjs [file...]  -- bundles test/*.test.ts with esbuild (from vite) and runs them with node:test
import { build } from "esbuild";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
const files = process.argv.slice(2).length ? process.argv.slice(2) : fs.readdirSync("test").filter((f) => f.endsWith(".test.ts")).map((f) => "test/" + f);
fs.mkdirSync("test/.out", { recursive: true });
const outs = [];
for (const f of files) {
  const out = "test/.out/" + f.split("/").pop().replace(/\.ts$/, ".mjs");
  await build({ entryPoints: [f], bundle: true, platform: "node", format: "esm", outfile: out, logLevel: "error", target: "node22", banner: { js: "import { createRequire } from 'module'; const require = createRequire(import.meta.url);" } });
  outs.push(out);
}
const r = spawnSync("node", ["--test", "--test-concurrency=1", ...outs], { stdio: "inherit", env: process.env });
process.exit(r.status ?? 1);
