# Wave 2 Research Spike: Real Terrain + Road Character Variation

Date: 2026-07-04. Research-only; no pipeline code changed. Companion to
[road-layout-research.md](road-layout-research.md),
[regional-2_5d-research.md](regional-2_5d-research.md),
[large-scale-growth-research.md](large-scale-growth-research.md),
[greybox-plan.md](greybox-plan.md)/[greybox-eval.md](greybox-eval.md).

Context: the greybox tracer bullet (G0-G2) landed a GO verdict on 2026-07-04 —
walkable VRChat greybox is feasible; the reordered backlog now prioritizes
geometry *aesthetic quality* (roads/terrain/buildings) over the 2D fabric
backlog. This doc scopes the "real terrain + road character" slice of that
work: current-state recon of the already-built synthetic terrain/cost
machinery, literature survey for the pieces that are missing, and a short
bounded scan of an unrelated DR-spatialization question.

## 0. Ground truth from code recon (read before anything below)

This matters more than any of the literature: **the pipeline already computes
a real derived heightfield, not just a cost scalar**, and it is already wired
into arterial routing *and* local street guidance. Several open questions in
the brief are already answered by existing code.

- `mapgen/src/mapgen/r1_inputs.py:compute_fields` (~line 413) derives, from
  the density raster alone: `height` (density, heavily Gaussian-smoothed at
  `height_smooth_sigma_cells=6`, normalized to `[0,1]`), `flow_accum` (D8
  drainage over `height`, with a boundary-outward epsilon ramp so every
  interior cell eventually drains to the coast), `height_carved` (`height -
  carve_k * norm(log1p(flow_accum))`, clipped `>= 0`), and `slope`
  (`|grad(height_carved)|`). This is a genuine elevation function, not a
  cost-only fabrication — `height_carved` *is* a heightfield today, just never
  exported as one.
- `mapgen/src/mapgen/r1_arm_b.py:build_cost_field` and
  `mapgen/src/mapgen/r1_macro.py:build_density_cost_field` (~line 485) build
  the macro-arterial routing cost **directly from these same rasters**: `cost
  = base(1.0) + w_slope(8)*norm_slope + w_river(6)*river_indicator -
  w_density(0.8)*norm_density`, floored at 0.05, `inf` outside the island mask.
  Arterials are least-cost (`route_through_array`) paths over this exact
  field.
- `mapgen/src/mapgen/r1_arm_a.py:build_terrain_guidance` builds a
  `RasterGuidanceField` (4-RoSy preferred street angle = terrain-gradient
  direction, weight = normalized slope × `strength`) from `height_carved`, and
  it is **already live in the production hybrid path**:
  `mapgen/src/mapgen/r1_seam.py` (`chen_in_block`, ~line 290-342) calls
  `build_terrain_guidance(fields, strength=guidance_strength,
  density_ridge_boost=...)` with `guidance_strength=6.0` and feeds it into
  every per-block Chen run. Road-terrain coupling is not a future feature —
  it exists, uniformly, today.
- Street widths are fixed by semantic tier only
  (`mapgen/src/mapgen/r1_mesh.py:DEFAULT_STREET_WIDTHS` — highway 1.0 /
  major 0.7 / local 0.5 / ring 0.6 / Chen "street" 0.25, island units; ×25
  m/unit → 25 m / 17.5 m / 12.5 m / 15 m / 6.25 m). This constant is
  **baker-only** (`run_r1_greybox_mesh.py` / `r1_mesh.py`) — it does not touch
  routing or topology, so width policy is a purely additive, safe change.
- Chen's density-mass split gate (`chen_field.RasterDensityField.mass`,
  wired via `max_parcel_mass` in `chen_generate.py`) already produces
  coarser, larger districts in low-density fringe and finer ones in dense
  cores (`regional-2_5d-research.md` R2 results: density-area Spearman
  ρ = −0.853, world-count CV 0.215, best of any layout tried). **The
  urban/rural fabric *coarseness* gradient is already solved**; what's
  missing is width/curvature/junction-spacing variation on top of it.
- `mapgen/src/mapgen/r1_connect.py:build_unified_street_graph` returns a
  plain `networkx.Graph` over the fused arterial+local street network (gates,
  T-junctions, the works). `nx.edge_betweenness_centrality` is a direct
  drop-in — the backlog's betweenness-width idea (`large-scale-growth-research.md`
  status section, item 4) has zero new infrastructure to build.
- Raster resolution: `long_side_cells=384` (`r1_inputs.py:241`) over a
  ~196×122-unit island → cell ≈ 0.51 unit ≈ **12.75 m/cell** at 25 m/unit.
  Comparable to a modest Unity terrain heightmap (384×~240), fine for macro
  relief, too coarse for literal per-road hairpin geometry without upsampling
  or a local detail layer.
- Mesh tri budget breakdown (`mapgen/artifacts/r1/greybox_mesh/greybox_mesh_manifest.json`,
  the committed G1 artifact): **379,093 tris total — buildings 356,706
  (94.1%), roads 22,562 (6.0%), ground+water+parks 415 (0.1%)**. Ground is
  currently a two-triangle flat slab. This number anchors the RQ4 budget
  discussion below.

### A documented-vs-shipped contradiction (flag per the brief's instructions)

`large-scale-growth-research.md`'s "Decisions locked in (2026-06-18)" section
records: *"height ≈ f(**inverse** density) + roughness — high DR density =
basin/valley, low density = rugged uplands... periphery roughness is what
motivates switchback/wiggly local roads."* The shipped `compute_fields`
implements the **opposite**: `height = normalized(smoothed density)` directly
— dense cores are literal terrain **peaks**, sparse fringe is **low ground**.
This matches the *earlier* `regional-2_5d-research.md` design note ("elevation
= density is the standard, legible choice", citing ThemeScape/Islands of
Music) and the literature below, but it directly contradicts the *later*
locked decision, which appears to have never been implemented. This is a live
discrepancy the wave-2 design review needs to resolve explicitly, not paper
over — see §1's recommendation.

One clarifying analysis: flipping the sign is **cheaper than it looks and
does not require re-justifying the already-routed arterials**, because the
arterial cost field's density-attraction term (`-w_density*norm_density`) is
additive and independent of the height/slope terms, and slope-magnitude cost
is sign-invariant (`|grad(h)| == |grad(-h)|`). What *does* change under a sign
flip is drainage topology — flow accumulation would then converge into
(inverted) dense-core basins rather than shedding from them, i.e. "rivers
converge at downtown" instead of "downtown sits on a summit, rivers shed
outward." Both are individually real urbanism tropes (hill-fort cities vs.
river-confluence cities); neither invalidates the routed macro network,
because the density-attraction that drove routing is a separate cost term.

## 1. Heightfield source

**Findings.** Three real options, evaluated against what's already routed:

- **(a) Promote `height_carved` to a real heightfield.** Already computed,
  already the literal source of the slope/river cost terms the arterials were
  routed against. Zero re-justification risk — the already-routed roads are
  *by construction* consistent with this exact terrain (same array). Cost:
  export + resample to whatever mesh/heightmap format Unity ingests. This is
  not "an option to build," it's a decision to stop discarding a value
  that's already computed.
- **(b) Synthesize new terrain from the density field with a different
  mapping** (e.g. the inverted low-density-is-hilly convention the
  large-scale-growth doc records, or density-conditioned fBm/ridge noise).
  Consistency with routed arterials: high, per the sign-invariance analysis
  above — the *routing* stays valid either way, only the *narrative* (hill
  city vs. valley city) and the *drainage layout* change. This is really "(a)
  plus a parameter change (sign, roughness) and an additional density-inverse
  roughness octave," not a separate pipeline.
- **(c) Standard procedural terrain (fBm/erosion) masked to the island,
  ignoring the density-derived heightfield.** This is the one option that
  actually risks invalidating the arterials: routing was optimized against
  the *density-derived* slope/river field, so if the exported terrain's
  elevation function has no relationship to that field, the previously
  "efficient" least-cost arterial paths become visually arbitrary relative to
  the new terrain (a route might cross what's now a cliff, because the cost
  field that avoided cliffs no longer describes the exported terrain). Not
  recommended standalone; only viable as a *detail layer* added on top of (a).

**The actual missing mechanism, independent of (a)/(b)/(c):** none of the
above alone produces "rural terrain that justifies switchbacks." Under the
*current* direct-density mapping, `slope = |grad(height_carved)|` is highest
at the density **gradient** (the ring between a dense core and its fringe),
and near-zero both at a core's summit (flat plateau, heavily Gaussian
smoothed) and in the far, uniformly-sparse fringe (flat too). So "rural
terrain" today is flat, not rugged — the opposite of the desired narrative.
This is exactly the problem the large-scale-growth doc's roughness clause was
reaching for; the fix is orthogonal to the hill-vs-valley sign question.

**Recommendation for wave 2 (confidence: high on mechanism, medium on
parameters).** Keep the current direct-density base elevation (cheapest,
zero re-routing risk, literature-consistent, already validated by the routed
arterials) and add a **separate density-inverse-weighted roughness octave**:
`elevation = height_carved * relief_scale + fbm_noise(x,y) * roughness_gain *
(1 - norm_density)^k`. This gives flat, buildable downtown cores (low
roughness where density is high) and increasingly rugged fringe/periphery
(high roughness where density is low) regardless of which way the base trend
points — reconciling both docs' intents without picking a fight over the sign
convention. Resolve the sign question as a taste call at the design review
(recommend: keep direct/hill-core, since it's zero-cost and already what
shipped and got validated in the G2 walk), and record it explicitly in
`large-scale-growth-research.md` so the doc stops contradicting the code.

## 2. Road-terrain coupling

**Findings — literature.**

- Parish & Müller 2001, *Procedural Modeling of Cities*
  ([ACM](https://dl.acm.org/doi/abs/10.1145/383259.383292)): highway
  L-system rules deviate toward population/height maps; superseded in spirit
  by cost-field least-cost routing (Galin), which generalizes rule-based
  deviation into a principled optimization. Already cited historically in
  `road-layout-research.md`; no new information for this pass.
- Galin et al. 2010, *Procedural Generation of Roads*
  ([PDF](https://perso.liris.cnrs.fr/eric.galin/Articles/2010-roads.pdf),
  [CGF](https://onlinelibrary.wiley.com/doi/abs/10.1111/j.1467-8659.2009.01612.x)):
  weighted anisotropic shortest-path roads; cost combines slope, curvature,
  and obstacle (river/lake/forest) terms; a transfer function returns
  infinity above a max grade to forbid unrealistic climbs; combining slope
  *and curvature* costs is what the paper credits for realistic mountain
  roads with switchbacks; the terrain is then locally excavated/smoothed
  along the chosen path (roads modify terrain, not just react to it); bridges
  and tunnels are chosen where they shorten an otherwise-long detour.
  **This is functionally what `build_density_cost_field` +
  `route_through_array` already do** (least-cost anisotropic-in-slope path
  over a raster), just retargeted attractive-to-density instead of
  repulsive-between-cities, per `large-scale-growth-research.md`'s own Galin
  audit. The one piece genuinely *not* present: a **curvature cost term** —
  the current cost field penalizes slope and river crossing but has no
  penalty/reward shaping path bending, so literal switchback geometry doesn't
  emerge from the existing solve.
- No implementation found (paper or practitioner) with a crisp, reusable
  formula for literal hairpin-turn insertion. A practitioner writeup of a
  Houdini anisotropic-A* implementation of Galin's method explicitly lists
  "limiting path curvature, adding bridges and tunnels" as **unimplemented
  future work**
  ([jflynn.xyz](https://jflynn.xyz/portfolio/houdini-anisotropic-procedural-roads/)).
  Confidence is **low** that any citation gives a ready-made switchback
  algorithm; treat this as a heuristic-engineering problem, not a
  literature-lookup problem.
- Vanegas/Weber/Benes lineage (already surveyed in
  `road-layout-research.md`/`large-scale-growth-research.md`): parcel/street
  coupling and growth-simulation techniques, not slope-curvature coupling;
  no new information for this question.

**Retrofit assessment: re-route vs. keep-routes-and-post-process.**
Re-routing the already-computed arterials over a "real" heightfield is
low-value if heightfield = option (a)/(b) above (same array, same routes by
construction). Keeping routes and adding local switchback insertion is
strongly preferred regardless of heightfield choice, because:
1. The raster is ~13 m/cell — too coarse for a single least-cost path to
   trace literal hairpin geometry (a hairpin's characteristic wavelength is
   comparable to or smaller than the cell size); a curvature-cost term added
   to the *existing* raster solve would mostly just avoid steep cells, not
   produce switchback shapes.
2. Chen's local street generation (Yang cross-field, 4-RoSy) fundamentally
   produces **straight parcel-edge splits**, never curved paths — literal
   winding/switchback streets are out of scope for the local (Chen) tier
   without diverging from the paper (a hard constraint per
   `docs/chen-strict-reimplementation.md`).

So "switchback" character is realistically a **macro-arterial, geometry
post-process** concern, not a re-routing or a Chen-extension concern: detect
arterial segments where the sampled terrain grade exceeds a bound *and*
local density is low (rural-only), and insert an explicit dogleg/hairpin
detour along the vertical profile in the exported *mesh* geometry — the same
spirit as Galin's terrain-excavation step, done as a lightweight geometric
post-process rather than a raster re-solve.

**Recommendation for wave 2 (confidence: medium).** Two options:
1. **(Preferred) Grade-triggered local switchback insertion as a mesh/geometry
   post-process** on macro arterial polylines only, gated by (low local
   density) AND (grade above a threshold, sampled from `height_carved`/slope
   along the route) — cheap, deterministic, doesn't touch Chen or the
   raster solve, and is honest about literal hairpins being an
   engineering hack rather than a paper-backed technique.
2. Add a curvature cost term to the macro raster solve (closer to Galin's
   letter) — more "principled" but won't produce literal switchbacks at this
   cell resolution without also upsampling the terrain raster near steep
   arterial segments; higher implementation cost for a shape the raster
   can't really resolve at 13 m/cell. Not recommended as the first move.

Chen's *existing* `guidance_strength` mechanism (already wired, uniform today
at 6.0) is the right hook for local-tier terrain response — see §3, this is
a policy/parameterization change, not a new mechanism.

## 3. Rural/urban road character policy

Concrete proposals, each tied to an existing knob:

- **Street width — promote from fixed-per-tier to context-scaled.**
  `DEFAULT_STREET_WIDTHS` is baker-only (`r1_mesh.py`/
  `run_r1_greybox_mesh.py`); scaling it doesn't touch topology/routing.
  Proposal: `width = tier_base_width * f(local_density_percentile,
  betweenness_percentile)`, sampled per street segment at bake time from the
  density raster (already available) and the unified street graph's edge
  betweenness (see below). Cheapest, safest, highest-value-per-effort change
  in this doc — recommend doing this first.
- **Betweenness-centrality width promotion (Weber 2009 lineage,
  `large-scale-growth-research.md` backlog item 4).** `build_unified_street_graph`
  (`r1_connect.py`) already returns an `nx.Graph`;
  `nx.edge_betweenness_centrality(g, weight="length")` is a direct drop-in
  (approximate/`k`-sampled if the full graph — low thousands of edges at
  current scale — makes exact Brandes too slow; profile before assuming it
  needs approximation). Feed betweenness alongside density into the width
  function above: this correctly promotes a "farm-to-market" through-route
  even where local density is low, which pure density-based width would
  miss.
- **Junction spacing.** Already a knob: `r1_connect.py`'s `densify_gates` /
  `--max-gate-spacing` controls how often connector streets are inserted
  along under-served boundary stretches. Proposal: make the default spacing
  density-conditioned (wider spacing / fewer junctions in low-density
  blocks, tighter in dense ones) instead of one global constant.
- **Curvature/jitter.** Per §2, don't fight Chen's 4-RoSy formalism for
  local streets. Two levers, cheapest first: (1) scale the *existing*
  `guidance_strength`/`density_ridge_boost` params per macro-block by
  `(1 - normalized_block_density)` instead of the current uniform 6.0 — more
  contour-chasing irregularity in sparse blocks, straighter grid in dense
  ones, using a mechanism that's already validated (`regional_strong` config
  in `regional-2_5d-research.md`'s R1 results already demonstrated organic,
  terrain-aligned output at guidance strength 6 with density-ridge boost).
  (2) For genuinely winding rural roads, don't extend Chen — the mass gate
  already produces coarse, few-district rural blocks; apply the arterial-tier
  switchback treatment (§2) as the primary "rural character" signal, and
  accept that Chen's contribution to rural blocks is coarseness + moderate
  guidance-driven irregularity, not literal winding streets.
- **Where this does *not* fit:** don't invent a new "rural Chen" generator
  for wave 2. It's a bigger lift (a second local-fabric engine, a
  block-classification threshold, a seam between two generators) for a
  problem the mass gate + guidance scaling + arterial switchbacks already
  cover more cheaply. Revisit only if wave-2 visual eval says the coarse
  Chen-guided fabric still doesn't read as "rural" after the above.

**Recommendation for wave 2 (confidence: high on sequencing, medium on exact
functional forms).** Ship in this order: (1) density-scaled street width at
bake time (no upstream changes, immediate visual payoff), (2) betweenness
promotion added to the same width function (cheap now that the graph
exists), (3) density-scaled `guidance_strength` per block (one-line change
to an existing call site in `r1_seam.py`), (4) arterial switchback
post-process (§2) as the dedicated "rural terrain" visual signal. Density-
conditioned junction spacing is a nice-to-have, lower priority than the
above four.

## 4. VRChat terrain mesh budget

**Findings.**

- VRChat has no formal world "performance rank" system analogous to
  avatars; the commonly cited community/creator-doc figures are advisory,
  not SDK-enforced: **~250k triangles for a PC-focused world**, and
  **~50k triangles for a Quest-compatible world**, plus a **100MB
  post-compression size cap on Quest**
  ([creators.vrchat.com/platforms/android/quest-content-optimization](https://creators.vrchat.com/platforms/android/quest-content-optimization/)).
  The current committed greybox is already **379k tris**, above the PC
  advisory figure, yet G2's in-headset eval called performance "trivial on
  modern GPUs" with "ample headroom." Read this as: either the 250k figure is
  conservative/dated for modern PC VR hardware, or this world is implicitly
  PC-only (no Quest scoping anywhere in the greybox docs). **Flag for the
  design review: decide and record whether Quest is in scope** — it changes
  the total available budget by ~5-7×, and terrain is exactly the kind of
  addition (100% new geometry) that would need to respect whichever ceiling
  applies.
- Given the committed budget breakdown (§0: buildings 94.1%, roads 6.0%,
  ground+water 0.1% of 379k), **terrain has enormous headroom under the
  PC-only reading** — even a terrain layer 10-50× the current ground tri
  count (i.e. 4k-20k tris) would only add 1-5% to the total. Under a
  Quest-inclusive reading (50k budget), the current world *already* doesn't
  fit, independent of terrain — a bigger prior question than this doc's
  scope.
- Unity Terrain (heightmap) vs. baked mesh chunks: community/practitioner
  consensus for VR titles favors explicit mesh chunks — Unity's heightmap
  terrain LODs at a coarse quadtree granularity and forces uniform density
  changes across a patch, whereas mesh chunks give explicit, localized
  vertex-density control and integrate with standard LOD-group/batching
  systems rather than a separate terrain-specific system
  ([Unity Discussions](https://discussions.unity.com/t/increase-terrain-performance/701146),
  [pinwheelstud.io](https://www.pinwheelstud.io/post/why-low-poly-mesh-based-terrain-is-a-better-fit-for-mobile-and-vr-games/)).
  This also matches the project's actual toolchain: the pipeline is
  Python/OBJ-based end to end (`run_r1_greybox_mesh.py`), and Unity's native
  `TerrainData` format isn't part of that path — adopting Unity Terrain would
  add an import bridge oni would have to own, for a technique the VR
  community doesn't obviously prefer anyway.
- Collider cost: not directly evidenced in sources found this pass (would
  need an oni-side profiling pass, not a literature answer). For a
  walking-only world, exact-vertex collision fidelity doesn't matter — a
  decimated low-poly collision proxy per block is the standard practice
  regardless of render-mesh choice.

**Recommendation for wave 2 (confidence: medium-high on the structural call,
low on exact tri budgets — those need an oni-side profiling smoke test, not
a doc).** Extend the G1 baker's existing per-macro-block grouping pattern:
replace the flat 2-triangle ground slab with a per-block heightfield mesh
sampled from `height_carved` (or the elevation composite from §1), triangulated
at ~15-25 m vertex spacing away from roads/buildings (denser only where
grading against a building footprint requires it), grouped exactly like
buildings (`block_NNN_ground`) for identical culling/batching behavior — zero
new Unity-side systems, stays inside the OBJ/MTL toolchain oni already
consumes. Pair with a separately-decimated low-poly mesh collider per block.
Do not adopt Unity Terrain for this pipeline; the toolchain and VR-perf
literature both point the other way. Resolve the Quest-scope question before
finalizing a terrain tri budget number.

## 5. Secondary bounded scan — citygen-aware DR / map-like spatialization

Kept short per the brief — this is a later go/no-go note, not a design.

**Findings.** The grid-constrained/overlap-removal DR family — Hagrid
(Hilbert/Gosper space-filling-curve gridification,
[dl.acm.org/10.1145/3481549.3481569](https://dl.acm.org/doi/10.1145/3481549.3481569)),
DGrid (distance-preserving overlap removal,
[arxiv.org/abs/1903.06262](https://arxiv.org/abs/1903.06262)), IsoMatch
(informative grid layouts,
[researchgate](https://www.researchgate.net/publication/280972332_IsoMatch_Creating_Informative_Grid_Layouts)) —
all solve a **different problem than this pipeline has**: removing
label/glyph overlap in a scatterplot by snapping points to a regular grid
lattice. That's the opposite of what a street/parcel generator needs (real
polygonal freedom for blocks and parcels, not lattice-snapped points), so
adopting one would fight the existing Chen/road machinery rather than help
it. Self-organizing maps / Carto-SOM are the classic "map-like DR"
alternative but target exploratory-clustering UIs on a neuron grid, not
walkable street networks with real geometry — no example found coupling
SOM/grid-DR output to procedural road/parcel generation. GMap (Gansner, Hu,
Kobourov 2010, "visualizing graphs and clusters as maps",
[yifanhu.net/PUB/pacvis2010.pdf](http://yifanhu.net/PUB/pacvis2010.pdf)) is
the closest in spirit — it turns cluster/graph structure into contiguous,
non-overlapping geographic-style regions — but it's a graph-clustering-to-map
layout algorithm, not a DR replacement. Code search found **no evidence this
codebase already uses GMap** (`island_toponymy.py`/`physical_hierarchy.py`
hand-build contiguous named regions from cluster labels, which is GMap-*flavored*
in spirit but not GMap's actual algorithm) — the brief's premise that it's
"already partially used upstream" isn't confirmed by recon; the closest
analog is the existing toponymy/region-labeling layer, and it's a hand-built
scheme, not an imported technique.

**Verdict (confidence: medium — bounded scan, not exhaustive).** No
plausibly-better end-to-end alternative found to "LocalMAP DR + bounded
within-district displacement." The grid/cellular DR family targets label
overlap, not road-network generation, and would need non-trivial inversion/
extension to feed a street generator at all. Switching cost would be high
(replacing the validated 2D ground-truth layer the whole multi-month pipeline
is keyed to) for a speculative benefit. Recommend: no action now. If the
current island's macro shape/legibility proves to be a real limiter after
wave-2 terrain lands, prefer a narrow, presentation-only post-hoc
declutter/grid-snap pass (Hagrid/DGrid-style) over replacing the DR stage.

## Sources

- Parish & Müller 2001, *Procedural Modeling of Cities* —
  https://dl.acm.org/doi/abs/10.1145/383259.383292
- Galin, Peytavie, Maréchal, Guérin 2010, *Procedural Generation of Roads* —
  https://perso.liris.cnrs.fr/eric.galin/Articles/2010-roads.pdf ,
  https://onlinelibrary.wiley.com/doi/abs/10.1111/j.1467-8659.2009.01612.x
- Anisotropic A* road pathfinding practitioner writeup (Houdini) —
  https://jflynn.xyz/portfolio/houdini-anisotropic-procedural-roads/
- Unity terrain vs. mesh-chunk performance discussion —
  https://discussions.unity.com/t/increase-terrain-performance/701146 ,
  https://www.pinwheelstud.io/post/why-low-poly-mesh-based-terrain-is-a-better-fit-for-mobile-and-vr-games/
- VRChat world/avatar optimization guidance —
  https://creators.vrchat.com/avatars/avatar-performance-ranking-system/ ,
  https://creators.vrchat.com/platforms/android/quest-content-optimization/
- Hagrid (Hilbert/Gosper scatterplot gridification) —
  https://dl.acm.org/doi/10.1145/3481549.3481569
- DGrid (distance-preserving DR overlap removal) —
  https://arxiv.org/abs/1903.06262
- IsoMatch (informative grid layouts) —
  https://www.researchgate.net/publication/280972332_IsoMatch_Creating_Informative_Grid_Layouts
- Gansner, Hu, Kobourov 2010, *GMap: Visualizing Graphs and Clusters as Maps* —
  http://yifanhu.net/PUB/pacvis2010.pdf
- Carto-SOM (self-organizing-map cartograms) —
  https://www.researchgate.net/publication/220650113_Carto-SOM_Cartogram_creation_using_self-organizing_maps
- Internal: `mapgen/src/mapgen/r1_inputs.py`, `r1_arm_a.py`, `r1_arm_b.py`,
  `r1_macro.py`, `r1_seam.py`, `r1_mesh.py`, `r1_connect.py`, `chen_field.py`,
  `chen_generate.py`; `mapgen/artifacts/r1/greybox_mesh/greybox_mesh_manifest.json`;
  `docs/regional-2_5d-research.md`, `docs/large-scale-growth-research.md`,
  `docs/greybox-eval.md`.
