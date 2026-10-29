import test from "node:test";
import assert from "node:assert/strict";
import { isStale } from "../src/ui/stale";

test("stale rule: older than max(14 d, 0.25 x metrics age at scrape)", () => {
  assert.equal(isStale(1000, 990, 0), false);   // 10 d old, under the 14 d floor
  assert.equal(isStale(1000, 985, 0), false);   // 15 d vs 0.25 x 985 = 246: old world tolerates it
  assert.equal(isStale(1000, 985, 980), true);  // 15 d, born 5 d before the scrape: floor 14 d applies
  assert.equal(isStale(1000, 986, 980), false); // exactly 14 d is not stale
  assert.equal(isStale(1000, 800, 0), false);   // 200 d < 0.25 x 800
  assert.equal(isStale(1000, 700, 0), true);    // 300 d > 175
  assert.equal(isStale(1000, 0, 0), false);     // no metrics date: no marker
});
