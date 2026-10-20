import { build } from "esbuild";
await build({ entryPoints: [process.argv[2]], bundle: true, platform: "node", format: "esm", outfile: process.argv[3], logLevel: "error", target: "node22" });
