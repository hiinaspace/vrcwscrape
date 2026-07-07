# Wave 2 plan — massing/typology + roads + terrain

Status: DESIGNED (2026-07-06), ready to implement. Follows the lots wave
([lots-wave-plan.md](lots-wave-plan.md)) and its oni exit-gate audit (see that
doc's "Exit-gate audit result"). Design settled via two senior (Fable) reviews +
Opus synthesis; roads/terrain grounding is [terrain-roads-research.md](terrain-roads-research.md).

## Why

The lots wave closed positive (parcels/streets beat Voronoi) but buildings read
as **lot-sized skyscraper towers with thin alleys that occlude — and literally
intersect — the roads**. Wave 2 replaces the massing model with a suburb-default,
row-housing-in-dense-areas typology so streets become legible at eye level, then
adds real terrain + road character on top.

## Root causes (confirmed in code, 2026-07-06)

1. **Height** `= 4.0 + 12.0·log10(1+visits)` m (`r1_lots.py:_height_from_visits`)
   → 28 m floor at 100 visits, 52 m at 10k, 76 m at 1M. Visits-driven mid/high-rise.
2. **Footprint fills ~85% of every lot.** `size_frac = sqrt(fill_ratio)` where
   `fill_ratio = lot.area/obb_area`; subdivision optimizes lots toward rectangles
   so `fill_ratio≈1` → clamps to the 0.85 ceiling nearly everywhere. The
   `(0.55,0.85)` band never engages — the fill-ratio *mechanism* is wrong for a
   suburb, not just its constants.
3. **Buildings intersect the road ribbons.** Districts are Chen parcels verbatim
   (`r1_seam.py:358`), so lot frontage lines ARE street centerlines; `buffer_ribbon`
   buffers half-width each side (Chen street 3.125 m), buildings inset only 1.25 m
   from the lot line and extrude from y=0 → walls stand ~1.9 m inside the ribbon
   and rise through the road surface. **Setbacks must be denominated from the road
   *edge* (half-width + margin), not the lot line.**

## Decisions locked (user, 2026-07-06)

- **Suburb scale by default; row-housing (not vertical stacking) in dense areas.**
  Proceed with real setbacks even though detached districts drop from ~72% to
  ~25–35% 2D footprint coverage — that sparse-fringe/dense-core gradient IS the
  Google-Maps legibility texture; rows/dense areas stay near-solid.
- **Popularity → a landmark tier**, not height-on-every-building. Top-N (~50–150)
  worlds get a distinct taller/civic building, capped so no canyons return;
  doubles as a navigation device. (3D form best refined against terrain in 2b.)
- **Terrain sign: direct/hill-core** (dense = peak), per research §1 — record in
  `large-scale-growth-research.md` to kill the documented-vs-shipped contradiction.
- **PC-only**; Quest is a separate project (LOD/imposters), not a budget line.
- Map density stays **aesthetic, not semantic** (density-as-artifact accepted
  permanently) — see the DR-basis decision in
  [terrain-roads-research.md](terrain-roads-research.md) §5 addendum.

## The massing model (2a core)

One typology decision per district, from **area-per-world**
`a = district.area · mpu² / N` (mpu = meters_per_unit = 25):

| condition | typology | massing |
|---|---|---|
| `a ≥ A_detached` (**150 m²**, tuned 2026-07-06 → ~72% detached / 28% row on the 698-district bake; median a/world ~274 m²) | `detached` | 1–2 stories, house + front/side/rear yards |
| `a < A_detached` | `row` | 2–3 stories, shared-wall terrace (side_setback = 0) |
| world in top-N by visits | `landmark` | distinct taller/civic, capped count |

- **Footprint** (replaces `_oriented_footprint` fill-ratio sizing): in the lot's
  frontage frame, `footprint = lot ∩ slab(front_setback ≤ depth ≤ front_setback +
  depth_max)` eroded by `side_setback` each side. `front_setback =
  road_halfwidth_front + sidewalk(1.5 m) + yard_front`. Detached: yard_front 3 m,
  side 2 m, depth_max 12 m, rear guard ≥3 m. Row: yard_front 1 m, side **0**
  (coincident shared walls; backface culling resolves the pair), depth_max 10 m;
  end-of-run lots get side 1 m so runs terminate visibly. Keep the existing
  containment fallbacks. Delete `size_frac`/`fill_ratio`/`building_*_frac`.
- **Height** (replaces `_height_from_visits`): `stories · 3.1 m + jitter`, stories
  per typology band, `hash(world_id)`-deterministic (not RNG-order), per-lot
  variation within a row run KEPT (terraces vary). Visits leaves geometry entirely
  (except landmark selection). Retire `h_base/h_scale`.
- **Config:** new frozen `MassingConfig` (thresholds, story height, per-typology
  setbacks/depths in METERS) alongside `LotConfig`; thread `meters_per_unit` into
  the G0 stage and record it in `greybox_manifest.json` (currently absent — meter-
  denominated constants would be off by 25× otherwise). Emit typology per district
  into the manifest + `districts.geojson` so both viewers + the audit see the map.
- **1 world = 1 lot = 1 rect** preserved → zero 2D-export schema change; `?extrude=1`
  previews massing before any bake. Row runs render as abutting rects (terraces
  near-zoom, near-solid frontage far-zoom → preserves dense-area map density).

## Sub-wave split

**2a — "streets readable"** (iterate in deck.gl `?extrude=1`, then ONE bake → oni
re-audit):
1. `MassingConfig` + typology classification + new footprint/height model in
   `r1_lots.py` (+ tests: footprint ⊂ lot AND footprint ∩ fronting-ribbon = ∅
   [the correction-3 regression], row shared-wall coincidence, height-band
   determinism, L1 partition/count/displacement invariants unregressed).
2. **Per-segment street width computed once at G0 export** (density × betweenness
   function), written as a `width` property on `arterials/streets.geojson`;
   BOTH bakers (`r1_mesh`, `r1_app_export`) consume it — delete both hardcoded
   width tables. This is a *dependency* of the setback model (setbacks need
   per-segment `road_halfwidth_front`), not just a road-character nicety.
3. `meters_per_unit` threaded + recorded in the G0 manifest.
- Betweenness: exact Brandes (sub-second at this scale). Clamp the width function
  so Chen "street" ≤ "local" (monotone tier ordering is a legibility invariant).
- **Acceptance:** in-browser 3D preview (main thread + user) → bake → oni audit:
  streets visible along their length; district edges read as streets not alleys;
  row districts read as terraces; **road hierarchy judgeable** (unblocks 2b eval).
- **Tooling — building-read 3D viewer (parallel web slice).** The current
  `?extrude=1` OrbitView is a pitched top-down; it doesn't validate the *eye-level*
  building read (canyon vs suburb, streets visible along their length) that
  currently forces an oni/Unity round-trip. Add a low-angle / `FirstPersonView`
  walk-through mode to the deck.gl viewer so massing is judged in-browser before
  baking. Cheapest path: a "street view" camera preset (near-horizontal `rotationX`)
  or deck.gl `FirstPersonView`; reuses the existing extrude massing data. Cuts most
  oni round-trips out of the 2a iteration loop. Independent of the mapgen slices
  (web/ only) — can run in parallel.

**2b — "terrain + road character"** (after 2a audit):
- Heightfield: promote `height_carved` (direct-density base) + a density-inverse
  roughness octave `fbm·roughness_gain·(1-norm_density)^k`, **masked/attenuated
  under district+road polygons** (else every lot needs grading). Export as
  per-block ground mesh (~15–25 m vertex spacing), grouped like buildings; drop
  the flat 2-tri slab. Re-grounds buildings (currently y=0) + drapes roads.
- Road character order (research §3): (1) density-scaled `guidance_strength` per
  block in `r1_seam.py` (one-line, replaces uniform 6.0), (2) grade-triggered
  arterial switchback mesh post-process (rural-only: low density AND grade > bound),
  (3) density-conditioned junction spacing (nice-to-have).
- Landmark tier 3D form refined here against real terrain.

## Deferred / conditional (recorded, not blocking)

- **Landlocked interior lots** (M≈N so nearly all lots get houses, incl. no-frontage
  interiors): accept for the 2a bake (implied alleys); if the audit flags them,
  queue a *separate reviewed slice* for over-subdivide (M>N) + frontage-penalty
  Hungarian cost → Radburn-style green interiors. Do NOT fold an assignment-behavior
  change (wave-1-validated) into the massing slice.
- `geom.py` OBB-helper extraction (carried from lots wave) — fold in when 2a
  touches this code.
- Main-street/commercial typology tier — plausible v2 once the audit reacts.
