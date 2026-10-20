import { defineConfig } from "vite";

export default defineConfig({
  base: "./",
  build: { target: "es2022", outDir: "dist", emptyOutDir: true, assetsInlineLimit: 0 },
  preview: { host: "0.0.0.0", port: 4317 },
});
