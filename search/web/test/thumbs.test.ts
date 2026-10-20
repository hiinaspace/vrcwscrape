// Thumbnail pack tests against a packed fixture (VRCW_DATA=work/r5t/data, VRCW_THUMBS=work/r5t/out). Skipped when the build has no thumbs.
import test, { before } from "node:test";
import assert from "node:assert/strict";
import "./shim.mjs";
import fs from "node:fs";
import crypto from "node:crypto";
import { resolveObjectURL } from "node:buffer";
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

// integrity: every fetched blob equals the AVIF the fixture assigned to that world (VRCW_EXPECT = json {ordinal: sha1}); exercises shard + pack boundaries
test("blob bytes match the fixture for worlds across shards and packs", async () => {
  if (skip() || !process.env.VRCW_EXPECT) return;
  const exp: Record<string, string | null> = JSON.parse(fs.readFileSync(process.env.VRCW_EXPECT, "utf8"));
  let checked = 0;
  for (const [k, sha] of Object.entries(exp)) {
    const u = await e.thumbUrl(+k);
    if (sha === null) { assert.equal(u, null, k); continue; }
    assert.ok(u, k);
    const b = resolveObjectURL(u!);
    const bytes = Buffer.from(await b!.arrayBuffer());
    assert.equal(crypto.createHash("sha1").update(bytes).digest("hex"), sha, `world ${k}`);
    checked++;
  }
  assert.ok(checked > 500, `checked ${checked}`);
});
