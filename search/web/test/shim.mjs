// node shim: serves dist/data from disk so the real Engine runs headless (fetch + document.baseURI)
import fs from "node:fs";
import path from "node:path";
export function installShim(dataDir) {
  globalThis.document = { baseURI: "http://local/" };
  globalThis.fetch = async (url, init = {}) => {
    const u = new URL(String(url));
    const rel = decodeURIComponent(u.pathname).replace(/^\/data\//, "");
    const p = path.join(dataDir, rel);
    if (!fs.existsSync(p)) return new Response("nf", { status: 404 });
    const buf = fs.readFileSync(p);
    const h = new Headers();
    const rg = init.headers && (init.headers.Range || init.headers.range);
    if (rg) {
      const m = /bytes=(\d+)-(\d+)?/.exec(rg);
      const a = +m[1], b = m[2] ? +m[2] : buf.length - 1;
      return new Response(buf.subarray(a, b + 1), { status: 206, headers: { "Content-Range": `bytes ${a}-${b}/${buf.length}` } });
    }
    return new Response(buf, { status: 200, headers: h });
  };
}

// installed on import so it runs before the engine modules evaluate (ESM hoists imports)
installShim(process.env.VRCW_DATA ?? path.resolve("../work/r5/data"));
