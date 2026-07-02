# Greybox wave: citygen → walkable VRChat world (tracer bullet)

Decision record (user, 2026-07-02): before more fabric-quality waves, run the
cheapest possible **end-to-end chain** from citygen artifacts to a walkable
VRChat greybox, plus wire the citygen geometry into the existing 2D deck.gl
site. Rationale: the algorithm backlog is currently prioritized by 2D PNG
aesthetics; an in-engine walk will reorder it. Fallback stance: if the 3D
world proves unworkable at this scale, the product is the 2D map website with
streets — terrain generation and "smoothly navigable" street quality then drop
in priority. VRChat/Udon authoring specifics are in the user's wheelhouse;
their prior is that this scale is feasible.

Machine split:

- **natto** (this repo's usual dev box): Python export stages (G0, G1) and the
  2D website track (W).
- **oni** (Windows 11, Unity installed, native Claude Code session): Unity /
  VRChat SDK project and the walking eval (G2). oni consumes **committed
  artifacts** from this repo — it should not need to run the mapgen pipeline.

## Ground truth (verified 2026-07-02)

- Island frame: x ∈ [2.09, 198.01], y ∈ [1.96, 124.01] → **~196 × 122 units**.
  The frame is an affine of the UMAP app coordinates; `offset_x`, `offset_y`,
  `scale` are recorded in `mapgen/artifacts/r1/inputs/` manifest by
  `run_r1_island_inputs.py`, so geometry maps back into the web app's
  coordinate space exactly (needed by track W).
- `island_points.parquet`: 12,311 worlds with
  `world_id, x, y, visits (0..2.6e8), l0_id/l0_name, l1_id/l1_name`.
- Hybrid pipeline (`run_r1_hybrid.py`) currently produces ~13 cores,
  36 macro-blocks, ~700 Chen districts (avg ~18 worlds each), tiered
  arterials + core rings, unified street graph with T-junctions — but exports
  **only PNGs + `hybrid_junctions.geojson` + manifest**. District/street/
  block/arterial geometry never leaves the process. G0 fixes that.
- **Correction (recon 2026-07-02): per-world geometry DOES already exist in an
  older, non-Chen lineage.** `mapgen/src/mapgen/city_layout.py` (mesh/planar
  layout engines, ~6.4k lines) generated the `web/public/*-city*` dataset
  dirs: full-map coverage, Hungarian world→lot assignment, **rotated-rect
  buildings** (`building_width/depth/angle/height` columns in
  `app_points.parquet`), `roads{,_mid,_near}.geojson` (LineStrings with
  `kind ∈ arterial|collector|local|minor|service` + `width`), 6,776 blocks,
  125k parcels, `landuse.geojson` parks — all in the app/DR coordinate frame,
  and the deck.gl app already renders them (`isCity` mode, dataset selected by
  `?data=` URL param; `blocks`/`landuse` are styled in `config.js` but not yet
  fetched). The r1 hybrid island has no per-world geometry yet; G0 adds naive
  lots **there**. The city dataset schema is the interchange format track W
  targets, and `city_layout.py`'s assignment machinery is reference/reuse
  material for a later non-naive lot wave.

## Stage G0 — geometry + lots export (natto, Python)

New pure module `mapgen/src/mapgen/r1_lots.py`:

- `assign_worlds_to_districts(points, districts) -> assignment`
  Point-in-polygon via STRtree; worlds landing outside every district (street
  ribbons, failed blocks) snap to the nearest district. Report counts of
  direct vs snapped vs any dropped.
- `build_lots(district, member_worlds) -> list[Lot]`
  Per-district Voronoi of its member worlds, clipped to the district polygon
  (same metaphor the web app already uses island-wide). Inset each cell
  (fixed ~0.05 unit) for the building footprint.
  `Lot(world_id, footprint, lot_polygon, height, name, visits)`.
  Height: `h = h_base + h_scale * log10(1 + visits)` (defaults 4 m + 12 m →
  ~4 m for dead worlds, ~105 m for the 2.6e8 top world; a config knob, tune
  in-engine). Empty districts (no worlds) become parks — exported with
  `kind="park"`, no buildings.
- Degenerate cases: 1-world districts (lot = whole district), duplicate
  coordinates (jitter by epsilon before Voronoi), sliver cells (drop below
  min area, reassign world to nearest surviving lot — report count).

`run_r1_hybrid.py` gains a `--greybox-out DIR` flag writing, alongside the
existing outputs:

- `island.geojson` — boundary polygon.
- `arterials.geojson` — clipped arterial polylines with `tier`
  (highway/major/local) + core rings (`tier="ring"`).
- `blocks.geojson` — macro-block polygons with block id.
- `districts.geojson` — district polygons with `block_id`, `district_id`,
  `kind` (fabric/park), world count.
- `streets.geojson` — per-block Chen local street polylines
  (perimeter-duplicate paths excluded, as in the connectivity wave).
- `lots.parquet` — one row per world: `world_id, district_id, footprint_wkb,
  lot_wkb, height, name, visits, x, y, assigned` (direct | snapped).
- `greybox_manifest.json` — counts, config (heights, inset, widths), the
  island-frame affine copied from the inputs manifest, and the source hybrid
  manifest reference.

Determinism: same seed/inputs → byte-identical exports (sort features by id).
Zero `chen_*` edits. Tests: synthetic fixtures for assignment/Voronoi/edge
cases in `tests/test_r1_lots.py`; smoke via `--total-target 300`.

## Stage G1 — mesh bake (natto, Python; artifact committed for oni)

New `mapgen/scripts/run_r1_greybox_mesh.py`: reads the G0 export dir, writes
**OBJ + MTL** (Unity imports OBJ natively — no glTF package dependency for the
tracer bullet).

- Buildings: extrude footprints 0 → height. Grouped **per macro-block**
  (`g block_012_buildings`) so Unity gets ~36 building chunks for culling /
  static batching granularity, not 12k objects and not one megamesh.
- Streets: buffer polylines to flat ribbons at z = +0.02 (over ground,
  z-fight offset). Default widths (units): highway 1.0, major 0.7, local
  arterial 0.5, ring 0.6, Chen local 0.25. Config knobs.
- Ground: island polygon as a slab (top at z = 0); a large water quad at
  z = −0.5.
- Materials: one per semantic kind (buildings, per-tier streets, ground,
  water, parks) — flat colors, so oni can restyle by material without
  touching geometry.
- Coordinates: **meters, Y-up, with `--meters-per-unit` baked in** (default
  25 — see scale note below). Emit `greybox_mesh_manifest.json` recording the
  scale and group inventory.
- Optional `--labels-csv`: `world_id, name, x, y, z_roof` in meters for
  oni-side TextMeshPro signage experiments (no text in the mesh itself).
- Optional second input mode `--city-dataset DIR`: bake a `web/public/*-city*`
  dataset dir (rotated-rect buildings from `app_points.parquet`, roads from
  `roads_near.geojson`) instead of a G0 export. This lets G2 walk the
  existing **full-map** city-mesh layout as a comparison point alongside the
  hybrid island, from the same baker. Scale knob is per-run — the DR app
  frame and the island frame have very different unit sizes.

Size estimate: ~12k prisms ≈ low hundreds of thousands of triangles; OBJ in
the tens of MB. Commit under `mapgen/artifacts/r1/greybox/` if < ~50 MB,
otherwise gzip it (Unity side unpacks) — do not add git-lfs for this.

### Scale starting point (tune in-engine, G2's first job)

At **25 m/unit**: island ≈ 4.9 × 3.1 km; districts ≈ 150 m across; lots
≈ 25–35 m; Chen streets ≈ 6 m, highways 25 m. VRChat walking ≈ 2–4 m/s →
crossing the island on foot ≈ 25–40 min. That is deliberately "real city"
sized — the walk should answer whether that wants scale compression, faster
locomotion, transit/teleport infrastructure, or is fine as-is. G1 takes
`--meters-per-unit` so re-bakes at 15/20/30 are one command.

## Stage G2 — Unity / VRChat greybox (oni, user-driven)

Not prescriptive — this is the user's domain. Suggested shape: VRChat worlds
SDK project (VCC), import the OBJ, per-material flat colors, spawn on a core
plaza, walk. Nice-to-haves if cheap: name quads for the N tallest buildings
near spawn (from labels CSV), a second spawn at island edge.

**Eval checklist (the actual deliverable — write answers + screenshots back
to `docs/greybox-eval.md` and push):**

1. Scale/locomotion: right meters-per-unit? Walkable, or does it demand
   transit/teleports/compression? Street widths vs building heights sane?
2. Does the hierarchy read at eye level — cores/downtowns vs neighborhoods,
   arterials vs local streets? Does density-as-height make popular areas
   legible from a distance?
3. Perf ballpark: FPS in-headset or desktop, draw calls with the per-block
   grouping, anything obviously needing imposters/LOD earlier than expected.
4. What actually looks broken at street level, ranked. (Do empty coarse
   blocks — the interior-fabric gap — even register? Do sliver residuals?
   What did the 2D renders never show?)
5. Verdict: GO (product waves continue toward 3D) / NO-GO (2D-site-first;
   terrain + street navigability deprioritized) / GO-with-changes.

Item 4/5 output **reorders the mapgen backlog** (currently:
interior-fabric gap, residual guidance fans, wedge regularization,
betweenness road-width promotion). Don't resume fabric waves before this.

## Track W — hybrid island as a city dataset dir (natto, parallel with G2)

Revised after recon: the deck.gl app **already has a city view** (`isCity`
mode: rotated-rect buildings, tier-swapped roads, lazy parcels, selection →
sidebar), driven entirely by which dataset dir the `?data=` URL param picks.
So v1 needs **zero web code changes**: a new exporter emits the hybrid island
in the existing city dataset schema.

- New `mapgen/scripts/run_r1_app_export.py`: reads the G0 greybox export +
  the source DR export dir (`full-nolabs-localmap-island-toponymy`), writes
  `web/public/<name>-island-chen/`:
  - all geometry inverse-affined from island frame → app/DR frame (the
    affine is in the G0 manifest);
  - `app_points.parquet` for the 12,311 island worlds, base columns copied
    from the source export, plus `building_width/depth/angle` = oriented
    min bounding rect of each G0 lot footprint, `building_height`, `lot_id`,
    `block_id`;
  - `roads{,_mid,_near}.geojson` with tier→kind mapping (highway→arterial,
    major→collector, local arterial→local, ring→arterial, Chen streets→minor);
  - `parcels.geojson` from lots (`lot_id, block_id, world_id`),
    `blocks.geojson` from districts, `landuse.geojson` from park districts;
  - subset `worlds_meta.parquet` + region geojson from the source export;
    v3-style `manifest.json`.
- Payoff: instant **A/B against the old city-mesh layout** by switching the
  URL param; the app becomes the fast iteration viewer for hybrid output,
  replacing the matplotlib PNG loop for everything except paper-fidelity
  gates.
- Later (not v1): render the already-styled `blocks`/`landuse` layers in
  `Map.jsx`; 2.5D building extrusion from `building_height`; polygon (not
  rect) footprints.

## Status (2026-07-02)

G0, G1, and W are implemented and committed. The production G0 export
(total-target 1200, `--max-gate-spacing 20`: 118 T-junctions, 0.867
largest-component share) lives at `mapgen/artifacts/r1/greybox/`
(regenerable, not committed). The baked mesh — `greybox.obj` (~21 MB, 379k
tris, 44 groups), `greybox.mtl`, `labels.csv`, manifest — IS committed at
`mapgen/artifacts/r1/greybox_mesh/` for oni to pull; bounds ≈ 5.0 × 3.15 km
at the default 25 m/unit. The 2D dataset is generated at
`web/public/full-nolabs-localmap-island-chen/` (gitignored like all dataset
dirs; regenerate with `uv run python scripts/run_r1_app_export.py` from
mapgen/) and viewable at `http://localhost:5173/?data=island-chen` — verified
end-to-end: parcels/roads/labels render, lot click → sidebar works. Known
greybox artifacts for the eval: min-rotated-rect buildings can overlap
neighbors on elongated Voronoi cells; ~2k sliver lots render near-invisible
(stacked duplicate footprints); offshore region-outline blobs from the
bbox-filtered source regions. **Next: G2 on oni** (see checklist above), then
the backlog reorder.

## Sequencing

1. **G0** — implementation slice (Sonnet `chen-slice-implementer`), review
   gate, milestone run committed. Unblocks both tracks.
2. **G1** — small slice (Sonnet), bake + commit artifact. → **push; oni can
   start G2.**
3. **W** — natto continues: export→web data wiring, then view mode.
4. **G2 eval lands** → triage `docs/greybox-eval.md` → reorder backlog →
   next wave chosen by the gate, not by the PNG backlog.

Orchestration per AGENTS.md, plus quota routing: implementation slices run as
**Sonnet** subagents; Fable stays on orchestration and the visual/eval gates.
