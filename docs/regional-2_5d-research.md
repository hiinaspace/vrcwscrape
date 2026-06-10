# Regional Layer Research: Recursive Chen + 2.5D Terrain

Date: 2026-06-10. Companion to [road-layout-research.md](road-layout-research.md)
(the v3 design) and [chen-strict-reimplementation.md](chen-strict-reimplementation.md).

Goal restated (user, 2026-06-10): high-dimensional world embeddings → a
**2.5D terrain map with roads and simple simulated buildings**, not just a 2D
map with roads. The 2D LocalMAP layout is validated and stays the ground-truth
data term.

## Prior art the earlier passes missed

### Spatialization / "information landscape" literature

The embeddings→terrain idea is a known genre ("spatialization") with useful
results, all pointing at **elevation = density** as the standard, legible choice:

- ThemeScape (Wise et al.): document landscapes where surface height encodes
  topical frequency/density.
- Islands of Music (Pampalk): SOM over audio similarity rendered as an
  archipelago — sea = empty space, islands = genres, mountains = dense
  clusters. This is almost exactly the VRChat-worlds concept, validated on
  music in 2002.
- Fabrikant et al., "The natural landscape metaphor in information
  visualization": user studies showing laypeople read hills/valleys in
  information landscapes intuitively — supports the metaphor's legibility.
- Hograefer et al. 2020, "The State of the Art in Map-Like Visualization"
  (CGF survey): design space for imitating topographic maps from abstract
  data; good checklist for which cartographic elements carry meaning.

Implication: deriving terrain from the DR map is well-precedented; nobody in
this genre modifies the DR itself to get terrain — they derive relief from
density post-hoc.

### Hydrology-first procedural terrain

- Génevaux et al. 2013, "Terrain generation using procedural models based on
  hydrology" (ACM TOG): build the river/drainage network FIRST, then
  synthesize terrain consistent with it.
- Schott et al. 2023, "Large-scale Terrain Authoring through Interactive
  Erosion Simulation" (ACM TOG): stream-power erosion with user-controlled
  ridges/valleys, interactive rates.

Implication: instead of Perlin relief (current city_proto approach), carve the
density heightfield with cheap hydrological erosion. This yields drainage
valleys/ridges that are (a) natural-looking, (b) a deterministic function of
the DR density, and (c) **functional inputs**: valleys host rivers and
arterial roads, ridges and rivers become natural district separators — exactly
the constraint channels the B-field formulation admits. Terrain stops being
decoration and becomes the field constraint the literature audit asked for.

### Settlement-on-terrain (for the rural ladder)

- Emilien et al. 2012, "Procedural generation of villages on arbitrary
  terrains" (already in Chen 2024's references as [EBP*12]): terrain-aware
  village/hamlet generation — the right reference for sparse/rural zones
  where Chen's urban assumptions don't apply.
- Game-practice reference: Voronoi island generation with
  elevation/moisture layers (Patel, "Polygonal Map Generation") for
  archipelago readability tricks.

### Neural/generative track (checked, not applicable)

Recent latent-space world models (Terra 2025, Matrix-3D, terrain diffusion)
generate explorable 3D from latents directly — a different, non-deterministic
paradigm. Confirms the niche: a *cartographic, deterministic* pipeline from a
fixed embedding corpus appears genuinely under-explored. Keep generative
models out of the layout path.

## What "2.5D DR" could mean — options

1. **Derived elevation channel (recommended default).** Keep the validated 2D
   LocalMAP; elevation = f(local density KDE, hydrological carving), possibly
   modulated by visits/popularity for landmark peaks. Precedented
   (ThemeScape/Islands of Music), cheap, doesn't risk the validated layout.
2. **densMAP-style density-preserving DR.** densMAP (Narayan, Berger, Cho
   2020) extends UMAP to explicitly preserve local density as per-point radii
   — a principled way to get an honest density signal aligned with the
   embedding, rather than KDE over possibly-distorted 2D distances. Drop-in
   probe: umap-learn has `densmap=True`. Worth one comparison run against the
   current KDE-on-LocalMAP density.
3. **3D DR, z as elevation.** Semantically meaningful height (similar worlds
   at similar altitude) but fights terrain realism — drainage/slope semantics
   become arbitrary, and it destabilizes the validated 2D neighborhoods.
   Cheap to probe (we already store multiple DR coordinate sets) but low
   expected value; park it.
4. **Density-equalizing / contiguity warps post-DR.** Cartogram-style warps
   could open up overcrowded areas before layout. Note as a tool if dense
   cores end up too cramped for parcel layout; not a first move.

## R1 probe spec (next wave)

Objective: answer "is recursive Chen interesting at district scale on a real
island, or too regular?" with one artifact, while stress-testing the Chen
reimplementation on real concave polygons.

1. **Island selection**: one mid-size LocalMAP island with a dense core and
   sparse fringe, extracted from the existing full-export artifacts
   (`physical_hierarchy` layer parquets / `regions_l*.geojson`,
   `web/public/full-nolabs-localmap-island-toponymy-city-mesh/`).
2. **Inputs derived offline**: island boundary polygon (cleaned/simplified to
   a sane vertex budget), density raster (KDE; optionally densMAP radii),
   proto-terrain = density heightfield + light hydrological carving (probe
   scope: even simple D8 flow accumulation over inverted density is enough to
   get valleys; full erosion later).
3. **Arm A — recursive Chen**: coarse run with large `min_parcel_area`
   ("parcels" = districts, "streets" = arterials), B-field constrained by
   boundary + terrain gradient + density ridges. Chen knobs tuned for
   regional character: lambda weights favoring size balance + access over
   regularity, loop street pattern.
4. **Arm B — v3-style baseline**: least-cost arterials between density peaks
   (Galin 2010 style) over the same terrain/density cost field, polygonized
   into districts.
5. **Comparison vs current KMeans districts** (regions_l0): side-by-side
   renders; metrics: world displacement from DR positions (median/p95),
   density–district-area correlation, arterial length, district convexity /
   shape distribution, terrain-alignment of arterials (fraction of arterial
   length in low-slope cells).
6. Out of scope for R1: world→lot assignment, buildings, rural lots, web
   export. R1 is a static-artifact eyeball + metrics gate, per the
   established workflow.

Risks acknowledged: Chen's regular-parcel bias may show through at district
scale even with tuned weights; that's exactly what Arm B is for. The two arms
share the field/terrain layer, so the comparison isolates the partitioning
strategy.

## Sequencing

island-preset fix (in flight) → R1 probe → spike-parcel/field fidelity wave →
efficiency/profiling wave (thousands of districts; districts are
embarrassingly parallel) → world→lot assignment + rural ladder → full
archipelago integration.
