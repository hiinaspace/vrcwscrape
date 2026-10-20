// Thumbnail pack tests against a packed fixture (VRCW_DATA=work/r5t/data, VRCW_THUMBS=work/r5t/out). Skipped when the build has no thumbs.
import test, { before } from "node:test";
import assert from "node:assert/strict";
import "./shim.mjs";
import { Engine } from "../src/engine";

let e: Engine;
before(async () => { e = await new Engine().init(); });
const skip = () => !e.hasThumbs;

test("thumbUrl returns object URLs of AVIF images and null for worlds without one", async () => {
  if (skip()) return;
  const r = await e.query({ q: "", sort: "visits", limit: 40, facets: false });
  const urls = await Promise.all(r.idx.map((i) => e.thumbUrl(i)));
  const got = urls.filter((u): u is string => !!u);
  assert.ok(got.length > 20 && got.length < 40, `got ${got.length}`); // fixture drops every 11th world
  for (const u of got.slice(0, 3)) assert.match(u, /^blob:/);
  // not-in-fixture worlds (ordinal 6000+) have no thumbnail
  assert.equal(await e.thumbUrl(190000), null);
  const st = e.thumbs!.stats;
  if (process.env.VRCW_NO_RANGE) { // host ignores Range: whole packs are downloaded once and sliced
    assert.equal(st.rangeSupported, false);
    assert.ok(st.fullPackFallbacks >= 1 && st.requests <= 4, JSON.stringify(st));
    return;
  }
  assert.equal(st.rangeSupported, true);
  assert.ok(st.bytes >= st.payloadBytes && st.bytes < st.payloadBytes + 40 * 8200, JSON.stringify(st));
});

test("same image is deduplicated across worlds and requests coalesce", async () => {
  if (skip()) return;
  const a = await e.thumbUrl(0), b = await e.thumbUrl(0);
  assert.equal(a, b);
});
