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
