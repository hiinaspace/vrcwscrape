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

## R1 results (2026-06-10)

Probe ran on island 4 (12,311 worlds, highest core/fringe density contrast
8.3×). Artifacts: `mapgen/artifacts/r1/{inputs,arm_a,arm_b,compare}/`; scripts:
`mapgen/scripts/run_r1_{island_inputs,arm_a_chen,arm_b_baseline,compare}.py`.
Chen gained two optional, default-off extensions (`split_weights`,
`RasterGuidanceField` terrain guidance into the grid-smooth field; default path
byte-identical, noted in the contract doc).

Answers to the R1 question ("recursive Chen at district scale: interesting or
too regular?"):

1. **Robustness: pass.** All four Chen configs generated cleanly on the real
   99-vertex concave boundary (thin peninsulas included), zero invariant
   violations, ~21–45 s per run.
2. **Regularity: tunable, mostly.** Stock Chen (control) is visibly grid-like.
   Terrain guidance + size/access-weighted splits + cul-de-sac avoidance
   (`regional_strong`: guidance strength 6 + density-ridge boost) produce
   organic, terrain-aligned district edges that read as a plausible regional
   map. Residual orthogonal bias near the boundary remains. Guidance saturates
   at the internal cap (0.5× boundary weight); raising it is the next lever if
   needed.
3. **The real gap is density adaptivity, not regularity.** Chen's size term
   balances *geometric* area, which actively fights the dense-core→small-
   districts structure we want: density–area Spearman ρ is ≈0..+0.29 across
   all Arm A configs vs −0.93 for Arm B (degenerate: 9 districts, one fringe
   district is 66% of the island) and −0.43 for KMeans l0. Arm A wins on
   area balance (CV 0.17 vs 1.77/0.52) and arterial terrain alignment
   (74–79% of arterial length in low-slope cells vs Arm B's 63%); KMeans
   wins on world-count balance (CV 0.52) but has no cartographic structure.
4. **Deviation:** the "loop street pattern" knob assumed above does not exist
   in the implementation; `avoid_cul_de_sacs` is the closest lever and was
   used.

Conclusion: neither arm is the answer alone. Chen brings structure and
robustness; the density adaptivity must come from the split criterion itself.
**R2 direction:** a density-mass split/termination mode for Chen — measure
parcel "size" as integrated density mass (≈ world count) instead of geometric
area, so `min_parcel_area` becomes "max worlds per district" and splits chase
density automatically. Same extension pattern as guidance (optional,
default-off, byte-identical default). Secondary: seed level-0 streets from
Arm B-style least-cost arterials between density peaks, letting Chen recurse
inside the resulting macro-blocks.

## Sequencing

island-preset fix (done) → R1 probe (done, see results above) → R2
density-mass split criterion + rerun Arm A → spike-parcel/field fidelity wave →
efficiency/profiling wave (thousands of districts; districts are
embarrassingly parallel) → world→lot assignment + rural ladder → full
archipelago integration.
