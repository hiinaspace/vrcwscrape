# Lots/buildings wave (post-greybox wave 1)

Status: IN PROGRESS (started 2026-07-04). Follows the greybox GO verdict
([greybox-eval.md](greybox-eval.md)).

The greybox tracer bullet's per-world Voronoi lots read as noise, not parcels
(elongated cells, overlapping rotated-rect buildings, ~2k slivers) -- the G0
mechanism was deliberately naive and never meant to be the shipped lot
geometry. This wave replaces it with street-fronting subdivision + exact
assignment so parcels/buildings actually read as a city block, while keeping
district membership decided from each world's true DR coordinate (not the
lot geometry) so the map's spatial meaning doesn't silently shift.

## Goal

Replace the deliberately-naive G0 per-district Voronoi lots (`r1_lots.py`) with
**street-fronting subdivision + assignment**, fixing the shared loudest defect of
both the 2D `?data=island-chen` view and the 3D greybox: parcel/building
readability (elongated cells, overlapping rotated-rect buildings, ~2k slivers).

## Decisions (user, 2026-07-04)

- **Bounded displacement**: worlds move onto assigned lots within their district
  (membership still decided by the true DR coordinate). For DIRECTLY-assigned
  worlds (81% of worlds in the production run -- point-in-polygon membership),
  displacement is bounded by the district's own diameter (~150 m at 25 m/unit),
  since the true coordinate already lay inside the district. Worlds SNAPPED into
  a district from outside (the remaining 19% -- no district contained their true
  coordinate, so `assign_worlds_to_districts` snapped them to the nearest one)
  additionally carry their snap distance on top of that district-scale term, and
  are NOT bounded by district size -- observed max 433 m in the production run.
  `mapgen.r1_lots.displacement_stats` reports both the combined stats and a
  `direct`/`snapped` split so this distinction stays visible. Orig coords +
  displacement stats kept. Rationale: sub-district DR distances carry little
  meaning; Voronoi cells can never read as parcels. **User re-confirmed the
  corrected figures (2026-07-04): 433 m max for snapped worlds is acceptable —
  at the local level it's already hard to tell what the DR is clustering. The
  standing revisit trigger: if local structure starts feeling less coherent in
  practice, that motivates the "citygen-aware DR" / map-like spatialization
  direction upstream (open research thread, not this wave).**
- Chen ledger's two Blocking paper-fidelity items → Deferred (see ledger).
- Roads+terrain are wave 2; a parallel research-only spike seeds its design
  (deliverable: `docs/terrain-roads-research.md`).

## Slices

- **L1** — `r1_lots.py` core: recursive OBB/strip subdivision of each district
  into ≈N street-fronting lots (district perimeter = streets = frontage;
  interior remainders → greenspace), exact Hungarian world→lot assignment,
  frontage-oriented inset building footprints, old Voronoi path kept as
  degenerate fallback. Tests: partition/no-overlap invariants, count
  reconciliation, displacement bound, determinism.
- **L2** (after L1 review) — export plumbing: `lots.parquet` gains
  `lot_x,lot_y,kind` + manifest displacement stats; `run_r1_app_export.py` emits
  assigned positions as app x,y (orig preserved); `run_r1_greybox_mesh.py`
  re-verified + re-baked.
- **V** (parallel) — web viewer: render already-styled `blocks`/`landuse`
  layers; 2.5D building-extrusion toggle for massing preview without a
  natto→oni round trip.

## Gates

1. Regenerated island-chen dataset → main-thread visual review (2D + 2.5D
   before/after) — interim substitute for the missing G2 defect ranking.
2. `wave-review` workflow on the wave diff.
3. Re-bake G1 mesh locally (`mapgen/artifacts/` is gitignored — the OBJ stays
   OUT of git per the history scrub; transfer to oni over LAN ssh/rsync, or
   regenerate there) → **oni structured audit** (G2 checklist items 2/4:
   eye-level hierarchy legibility + ranked street-level defect list) = wave exit
   gate; its ranking orders wave 2.

## Exit-gate audit result (oni, 2026-07-06)

Walked the lots-wave bake in-headset. **Verdict: positive — the wave met its
goal.** Street/parcel readability is clearly better than the G0 Voronoi fabric;
lots read as street-aligned parcels, not noise. Wave 1 is **closed**.

Redirect for wave 2 (design inputs, not wave-1 defects):

1. **Buildings are lot-sized "skyscraper" towers with thin alleys between them.**
   Two causes, both confirmed in code:
   - Height `= h_base + h_scale·log10(1+visits)` (`r1_lots.py`, `h_base=4.0`,
     `h_scale=12.0` m) → ~28 m floor at 100 visits, ~52 m at 10k, ~76 m at 1M:
     a 9–25-story band even at baseline, and it's **visits-driven vertical
     stacking**.
   - Footprint fills 55–85 % of the lot OBB (`building_{width,depth}_frac`,
     inset 0.05 u ≈ 1.25 m) and lots **partition the whole district** — so
     blocks are near-wall-to-wall tall buildings.
2. **Roads are occluded** by those chunky towers → road hierarchy couldn't be
   evaluated at eye level (compounded by the 14/26 non-core blocks with no Chen
   interior streets). Massing must come down before roads/terrain can be judged.
3. **Intended direction:** suburb/house scale by default (more pleasant to
   "drive through" than urban canyons), with the **densest areas collapsing to
   row-housing-style lots** (shared-wall strips) rather than vertical stacking.
   Explicitly **no vertical stacking at first** — it would thin out the 2D map's
   footprint density, which the map view depends on. (Google-Maps-style
   Tokyo-density vertical handling is noted as out-of-scope until there's
   visceral need — user is US-based, no first-hand dense-vertical map UX.)
4. **Locomotion:** still the personal-vehicle + transit direction from G2;
   owned VRChat-side, not a mapgen wave.

Wave-2 scope therefore grows to **massing/typology + roads + terrain** (the
research doc `terrain-roads-research.md` covers roads/terrain only; massing is
the new, undesigned axis). Both are going through senior-review before design
lands.
