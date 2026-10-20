// node shim: serves dist/data from disk so the real Engine runs headless (fetch + document.baseURI)
import fs from "node:fs";
import path from "node:path";
export function installShim(dataDir) {
  globalThis.document = { baseURI: "http://local/" };
  globalThis.fetch = async (url, init = {}) => {
    const u = new URL(String(url));
    let rel = decodeURIComponent(u.pathname).replace(/^\/data\//, "");
    let root = dataDir;
    if (rel.startsWith("/thumbs/")) { root = process.env.VRCW_THUMBS ?? path.resolve("../work/r5t/out"); rel = rel.slice(1); }
    globalThis.__fetchLog?.push([String(url), init.headers?.Range ?? null]);
    const p = path.join(root, rel);
    if (!fs.existsSync(p)) return new Response("nf", { status: 404 });
    const buf = fs.readFileSync(p);
    const h = new Headers();
    const rg = init.headers && (init.headers.Range || init.headers.range);
    if (rg && !process.env.VRCW_NO_RANGE) {
      const m = /bytes=(\d+)-(\d+)?/.exec(rg);
      const a = +m[1], b = m[2] ? +m[2] : buf.length - 1;
      return new Response(buf.subarray(a, b + 1), { status: 206, headers: { "Content-Range": `bytes ${a}-${b}/${buf.length}` } });
    }
    return new Response(buf, { status: 200, headers: h });
  };
}

// installed on import so it runs before the engine modules evaluate (ESM hoists imports)
installShim(process.env.VRCW_DATA ?? path.resolve("../work/r5/data"));
