import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// duckdb-wasm ships its own web workers + wasm; let it resolve those at runtime
// (we load the jsDelivr bundle) rather than having esbuild try to pre-bundle them.
export default defineConfig({
  plugins: [react()],
  optimizeDeps: { exclude: ["@duckdb/duckdb-wasm"] },
});
