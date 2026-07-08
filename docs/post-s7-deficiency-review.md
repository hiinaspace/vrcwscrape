# Post-S7 deficiency review (2026-07-08, autonomous session)

Context: after the S7c visual review, the user flagged four deficiency clusters
and asked for an autonomous pass (limited quota, overnight). This records what
was done and what each investigation found. Priorities were chosen by the user:
**building aspect cap + braided-roads investigation + full-dataset scale test**
(housekeeping deprioritized; 3D viewer = investigate-only).

## 1. Building aspect-ratio cap — DONE (committed)

**Defect (user):** some mostly-rectangular lots get a "tiny thin building" while
others fill almost entirely; wanted a max aspect ratio to bias buildings back to
squarish, without breaking the rowhouse terraces (which are good when they fill
to the lot edge).

**Diagnosis (measured on the committed idx4 bake):** detached-footprint OBB
aspect had a squarish median (1.42) but a long thin tail — **15% of detached
footprints had aspect > 3, out to ~1575** (degenerate slivers). The dominant
source is the **clamped-fallback path** (`_footprint_clamped`), used for lots too
shallow/narrow to seat the setback footprint — the S5.1 width cap never reaches
it. Row/landmark elongation is largely by-design (terraces / prominence).

**Fix (`r1_lots.py`):** a post-hoc aspect clamp wrapping `_oriented_footprint`,
so it reaches EVERY exit including the fallbacks. It measures the footprint OBB
and, if long/short > cap, intersects with a band centered on the OBB that shrinks
the long axis to `short * cap`. The shrink is **symmetric** → it only pulls edges
inward, so a frontage footprint's front setback can only grow (never encroach the
road), and the result stays inside the lot. Typology-gated like S5.1:
`detached_aspect_max = 2.5`, **row/landmark uncapped (0.0)**. Skipped for the S7b
rear-anchored degenerate case (squaring would fight the "shove off the arterial"
intent). Near-zero-width degenerate footprints are left untouched (can't square a
line without zero area).

**Validation (idx4 re-bake, before → after):**
- detached aspect **>3: 14.9% → 4.9%**; p90 3.88 → 2.50; p95 6.77 → 2.96.
- detached **p50 1.42 → 1.41** (normal houses untouched).
- **row + landmark byte-identical** (n and every percentile unchanged) — terraces
  and landmarks fully preserved, as intended.
- residual max ~785 / p99 8.67 = degenerate near-zero-width fallback lots that
  can't be squared; the bulk defect is fixed.

Knob for the visual gate: `MassingConfig.detached_aspect_max` (2.5). Drop toward
2.0 for a stronger squarish bias; the value is a taste call best set at the
in-engine gate.

## 2. Full-dataset scale test — DONE (PASS, one flagged issue)

The "~200k non-labs" set is **125,443 worlds across 15 island polygons, but only
3 are substantial**: idx12 (area 64, **90,540 worlds** — 72% of all data), idx0
(area 51, 22,564), idx4 (area 34, 12,312 — the dev island). **idx12 and idx0 had
never been laid out.** Baked both (`scripts/run_scale_test.sh`, target scaled by
point count to hold ~2.75 worlds/district):

| island | worlds | districts | max districts/block | invariant | runtime |
|--------|--------|-----------|---------------------|-----------|---------|
| idx4 (baseline) | 12.3k | 763 | 57 | 1.00 | 268s |
| idx0 | 22.5k | 1,429 | 60 | 1.00 | 501s |
| **idx12 (monster)** | **90.5k** | **5,102** | **593** | **1.00** | **1548s** |

**Verdict: the algorithm handles the full dataset** — 0 failed blocks, 100%
paper-invariant pass, no crashes, ~26 min for the 90k-world island.

**One flagged tuning issue:** idx12's densest downtown collapsed into a **single
593-district macro-block** (vs ~57 typical) that ate ~20% of runtime (312s of
1548s in one block). Chen per-block cost scales steeply with district count, so a
few ultra-dense blocks dominate. Not a crash, but on the densest island the macro
partition isn't granular enough where density is highest. **Revisit trigger:** if
the 90k-island downtowns look coarse/monolithic at the visual gate, subdivide
macro-blocks by mass (cap districts/block) before per-block Chen.

Artifacts: `mapgen/artifacts/r1/scale/idx{0,12}/` (gitignored). A full-res idx12
bake is available for the next in-viewer review.

## 3. Braided / overlapping roads — FIXED (root cause was the ring double-emit)

**User:** major roads look duplicated / "braided" with close overlapping roads
including minor ones; suspected a display artifact or an easy merge pass.

**Initial (partly-wrong) read, then corrected by a Fable senior review + my
verification:** I first measured ~79 near-parallel centerline runs (art-art 21 /
st-st 49 / art-st 9) and framed it as real geometry braiding. The senior review
**re-rooted it**: the dominant, visible effect the user saw is the **ring
double-emit at the draw layer**, not centerline braiding.
- The greybox export emits each ring both as a whole-ring record (`tier=ring`)
  AND as ~165 **promoted ring-arc records** (`tier=ring`+`ring_tier`) that are
  subsegments of it. After the S7c fillet (closed pass on whole rings, open
  endpoint-pinned pass on arcs) the arcs sit ~1 m off the whole ring. Both road
  draw consumers (`run_r1_app_export.py` → `roads.geojson`; `run_r1_greybox_mesh.py`
  → OBJ) iterated arterials with **no `ring_tier` filter** and styled rings/arcs
  identically → **every ring (18.7 km of the heaviest-styled road) stroked twice
  at ~1 m offset** = verbatim "major roads duplicated, braided with minor roads."
  This is the island-chen viewer dataset the user actually reviewed.
- The true art-art centerline braids (~40u) are **~19× smaller** and mostly
  legitimate junction tangents — 13/21 pairs involve rings (built AFTER
  `dedup_corridor_lines`, so structurally invisible to it), the rest sub-threshold
  spoke-meets-ring approaches. Extending centerline dedup was **rejected** (Do-NOT
  list risk, tiny payoff). st-st is Chen grain (ribbons don't even overlap).

**Shipped fix (commit, verified):** skip `ring_tier` arc records at the two
drawing consumers — whole rings already cover the full geometry, and neither
consumer used `ring_tier`, so it's pure de-duplication with zero width/style
change and **zero contact with the validated centerline stack**. idx4 re-export:
roads.geojson **341 → 176 features**; before/after render shows doubled ring
strokes collapse to single. The live island-chen viewer dataset was re-exported
(with the aspect cap too).

**Deferred (filed):** OBJ tier-priority ribbon flattening (all road ribbons are
coplanar at one y with per-tier materials → junction z-fighting in a depth-tested
viewer like VRChat) — braid-unrelated, invisible in the 2D viewer; do it when the
OBJ is the eyeballed deliverable.

## 4. 3D / extrude viewer polish — INVESTIGATED ONLY

Stack: **deck.gl** (React) — `OrthographicView` (2D), `OrbitView` (extrude),
`FirstPersonView` (?view=street, WASD), SDF `TextLayer` + `CollisionFilterExtension`
for label declutter, dual glyph atlas, custom Sutherland-Hodgman cell clipping.

**Root causes for the two complaints:**
- **"Clipping plane is weird" in extrude:** extrude mode uses
  `new OrbitView({ orthographic: true })` — a **pitched orthographic** view.
  Tilting an orthographic frustum clips geometry against a slab (the classic
  ortho-tilt clipping artifact) and gives no perspective, unlike Google Maps 3D.
  **Highest-leverage fix:** switch extrude to a **perspective** OrbitView (fovy +
  tuned near/far). FirstPerson/street already uses perspective with
  `near/far` from `STREETVIEW.nearMeters=0.5 / farMeters=4000` (config.js).
- **Labels at oblique/street angles:** the TextLayer is `sizeUnits:"pixels"`,
  billboarded, screen-space collision — so labels don't attenuate with distance
  and float above their 2D anchor rather than sitting on buildings. Google-maps
  feel needs **elevation-aware anchors** (labels on building tops), distance-based
  sizing, and depth occlusion (labels behind buildings hidden).

Both are interactive/visual tuning best done with the user + browser screenshots;
left for a dedicated viewer session. Also worth checking the full 125k dataset in
the viewer (only single-island reviewed so far).

## Suggested next priorities
1. Visual-gate the aspect cap in-engine (tune `detached_aspect_max` 2.0–2.5).
2. Road braiding merge slice (arterial↔street dedup) — biggest remaining "reads
   wrong" defect, and it's real geometry.
3. Viewer: perspective OrbitView for extrude (fast, high-impact), then labels.
4. idx12 dense-block granularity, if the 90k-island downtowns read coarse.
