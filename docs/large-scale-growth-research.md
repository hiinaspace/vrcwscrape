# Large-Scale & Growth Layout Research: Beyond Top-Down Chen

Date: 2026-06-18. Companion to [road-layout-research.md](road-layout-research.md)
(the v3 design + bibliography), [regional-2_5d-research.md](regional-2_5d-research.md)
(R2 density-mass Chen), and [chen-strict-reimplementation.md](chen-strict-reimplementation.md).

## Why this doc exists

R2 (density-mass split) made Chen density-adaptive and passes invariants, but
visual review of finer layouts (95–229 districts; see `artifacts/r1/zoom_review/`)
confirms two character gaps that are **structural, not parameter-tunable**:

1. **No road hierarchy.** Every district edge is an equal-weight street. There
   are no arterial/collector/local tiers, so the map reads uniform at every zoom.
2. **Top-down splitting, not organic growth.** Recursively splitting the *whole
   island* at once produces long streamline-aligned street fans and a "the entire
   island is one master-planned suburb" feel — rather than the "small towns
   growing into a sprawling city" character we want. This is also Chen applied
   **out of its design distribution**: Chen 2024 targets a single neighborhood
   region; we stretched it across the whole continent.

User direction (2026-06-18): prototype ideas that capture the agglomerative
growth character and/or proper hierarchy; pull the literature we already cite and
evaluate whether a technique better suited to large-scale procgen exists, instead
of forcing Chen out of distribution. We replicated Chen 2024 well, so keeping it
**where it is strong (local neighborhoods)** is fine — the question is the macro
structure above it.

## Four papers audited against our constraints

Our constraints are unusual and rule out naive adoption: the worlds are a **fixed
point cloud** (each world has a meaningful DR embedding coordinate we must roughly
honor), we have a **precomputed density raster** and **hierarchical cluster
labels** (~18 L0 clusters + finer), and the pipeline must be **offline,
deterministic, Python** (shapely/numpy/scipy/networkx). The map is a presentation
layer over real data; we cannot freely invent where things go.

| Paper | Growth character | Hierarchy | Scale to 10k+ | Net |
|---|---|---|---|---|
| **Weber 2009** — Interactive Geometric Simulation of 4D Cities | **High** | Medium | Low–Med | Best growth engine; the traffic subsystem (its scaling wall) is skippable |
| **Benes 2014** — Procedural Modelling of Urban Road Networks | Medium | Medium | **Low** | Right *look* (agglomeration), wrong *engine* (trade-traffic driver we lack) |
| **Galin 2011** — Authoring Hierarchical Road Networks | Medium | **High** | **High** | Cleanest hierarchy fit; our data maps directly onto its inputs |
| **Peng 2016** — Computational Network Design from Functional Specs | High | Medium | **Low** | Gurobi IP, 35–4900 s for 300–1200 edges; don't adopt, steal one constraint |

### Weber 2009 — 4D Cities (growth simulation)
Time-stepped planar-graph expansion. New streets sampled from "unfinished" nodes
with probability ∝ `exp(−f·‖pos − growthcenter‖²)`; cycles become blocks/quarters;
quarters fill with minor streets; land use re-optimizes continuously toward target
percentages ("dynamic disequilibrium"). Can be **seeded with an existing city**,
not only a blank canvas. Demonstrated at ~3k streets in ~1 min (2009 hardware);
the **quadratic-memory traffic/APSP subsystem is the scaling wall** and is mostly
orthogonal to our goals — droppable.
- **Steal:** growth-center-biased stochastic node expansion, with **our density
  raster / L0 centroids as the bias field** → organic coalescing growth that still
  honors fixed structure. And: **betweenness centrality on the street graph
  (networkx) as a cheap traffic proxy** to promote a sparse set of high-centrality
  edges to wide arterials — hierarchy without the traffic sim.

### Benes 2014 — multi-settlement growth
Settlements nucleate at road-network intersections and accrete block fabric
outward until neighboring settlements **coalesce into a conurbation**. The
agglomeration *look* is exactly our target, but the *engine* is inter-city
trade/traffic — data we don't have — and it runs minutes per *single* city.
- **Steal:** **nuclei at density peaks / L0 centroids (strength ∝ density), grow
  fabric outward until neighbors merge**; and **demand-driven arterials routed
  *around* built-up blocks** (with a gravity model between centroids as faked
  demand) for ring/bypass character.

### Galin 2011 — authoring hierarchical road networks (best hierarchy fit)
Input is exactly **weighted point nodes** (importance σ) + a **cost field** —
which maps directly onto **our cluster centroids/peaks (σ = peak magnitude) and
density raster (cost field)**. Generates 3 tiers highest-first; edge type
`τ_ij = min(σ_i, σ_j)`. A **proximity graph (Gabriel/RNG variant, one γ knob)
selects which peak-pairs connect** — endpoints are *not* hand-chosen. A control
cost penalizes lower tiers near higher trunks, and **Fréchet path-merging**
collapses near-parallel paths into shared trunks with Steiner junctions →
genuine **arterials-with-feeders**. Node count is tiny for us (~18 macro nodes),
so it is cheap and deterministic. Caveat: Galin routes *between* cities (avoids
dense cores); we want arterials *toward/through* peaks → **flip the density-cost
sign**. The unsolved seam is stitching Chen's local grid to arterial entry points.
- **Steal:** the whole macro layer — **importance-typed proximity graph over a
  density-geodesic cost field + control-cost + Fréchet merge**.

### Peng 2016 — functional network design
An integer program (Gurobi) over a mesh; networks grow root-like toward "sinks."
Beautiful control knobs (density, length-vs-travel-time, dead-end/loop/branch
policy) but **does not scale** (seconds–hour for hundreds of edges) and needs a
solver port. Don't adopt.
- **Steal:** the **no-island / distance-to-sink monotonicity constraint** — "every
  street strictly decreases distance to a destination, loops allowed" — is a
  deterministic graph rule expressible in networkx, giving loops-vs-cul-de-sac
  control our streamline fans lack. Sinks = density peaks/centroids.

## Convergent finding

All four papers — and our own Arm B (least-cost arterials between density peaks,
already implemented) — point at the **same cheap, deterministic restructuring**:
stop splitting the whole island top-down; instead build **macro structure from
the density field bottom-up, and keep Chen as the local engine inside it.**

## Recommended prototype: agglomerative macro + Chen micro

A hybrid that gets all three goals at once and keeps every validated piece:

1. **Macro hierarchy (Galin 2011 + existing Arm B).** Nodes = density peaks / L0
   (and optionally L1) centroids; σ = peak magnitude. Cost field = inverted
   density (arterials routed *toward* dense cores). Importance-typed proximity
   graph (γ knob) → tiered network via density-geodesic shortest paths (Arm B
   already does the path part). Control-cost + Fréchet merge → arterials-with-
   feeders. The merged graph's faces are **macro-blocks**.
2. **Local fabric (Chen, in-distribution).** Recurse Chen/R2 *inside each
   macro-block*. Chen is now bounded to neighborhood scale — exactly its design
   distribution — instead of stretched across the island. Per-block work is
   bounded and parallelizable (helps scale).
3. **Growth character (cheap Weber/Benes flavor).** The macro-block + density-
   driven Chen split already reads as distinct town cores. Optional enhancements:
   seed cores at peaks and let fabric coalesce (Benes), and/or promote arterials
   by **betweenness centrality** (Weber) as an alternative to the Galin γ knob.
4. **Connectivity polish (Peng).** Optionally enforce distance-to-sink
   monotonicity on local streets for loops-vs-cul-de-sac control.

**Why this over a wholesale growth-sim (Weber/Benes) rewrite:** it reuses Chen
(validated) and Arm B (built), keeps Chen in-distribution, is deterministic and
offline by construction, and scales by bounding Chen per macro-block. A full
time-stepped growth simulation is higher effort, harder to make deterministic,
and discards the local engine we already trust.

**Known risk / the real work:** the **seam** — stitching Chen's local grid to the
points where arterials enter a macro-block — is unspecified in all the papers and
is where prototype effort will concentrate. Worth a small spike before committing.

## Decisions locked in (2026-06-18, user)

- **Direction:** macro+micro hybrid (Galin-style macro hierarchy + Chen micro).
- **Seeding:** density peaks **+** L0 cluster centroids.
  - **Tier-1 "city" nodes** = L0 cluster centroids (visit-weighted mean position
    of each cluster's worlds), importance σ=2.
  - **Tier-2 "town" nodes** = density peaks (`find_density_peaks`) not already
    co-located with a centroid, σ=1. (Room for σ=0 "village" nodes later from
    finer clusters / minor peaks.)
  - Edge type `τ = min(σ_a, σ_b)`: city–city → highway, city–town → major,
    town–town → minor. Tiers drive arterial width/style and which faces become
    macro-blocks.
- **Cost field is density-ATTRACTING** (opposite of Galin's between-cities
  routing): arterials should hug dense ridges and run *toward* cores. Start from
  Arm B `build_cost_field` (slope + river) and **subtract** a normalized-density
  term so high-density cells are cheap.
- **Terrain is co-designed, inverted from the earlier 2.5D doc:** height ≈
  f(**inverse** density) + roughness — high DR density = basin/valley, low
  density = rugged uplands. Periphery roughness is what *motivates* switchback /
  wiggly local roads and irregular country lots. The same terrain feeds the road
  cost field, so surface and roads are co-generated.
- **Latitude:** world points may be **moved/relaxed** if it improves a style;
  generation may take **hours** (offline, re-run periodically as new worlds
  appear — not realtime/incremental). Technique is otherwise unconstrained.

## Staged build plan

1. **Macro hierarchy layer** (`r1_macro.py` + `run_r1_macro.py`): nodes →
   density-attracting cost field → importance-typed proximity graph (reuse Arm B
   Delaunay+β / geodesic paths) → tiered arterials → polygonize → macro-blocks.
   Checkpoint: render arterials (colored by tier) + macro-blocks over density.
   *(in progress)*
2. **Seam spike** — stitch Chen/R2 inside one macro-block, connecting local
   streets to arterial entry points. This is the known risk; validate early.
   **DONE (2026-06-18).** Chen runs cleanly inside a median-area block (invariants
   pass). Findings, which reshape stage 3:
   - *Geometric seam is a non-issue.* Chen is seeded from the block polygon, so
     the district union IS the block — outer edges coincide with the bounding
     arterials, zero gap/overshoot. A coverage/Hausdorff seam metric therefore
     can't see the real problem.
   - *The real seam is CONNECTIVITY.* Bounding arterials run alongside the block
     edge but form no T-junctions into the local grid; arterial and local network
     are parallel neighbors that never connect as roads. Stage 3 must explicitly
     create arterial↔local junctions (and a useful seam metric counts those, not
     area).
   - *Calibration seam.* The island-wide `default_max_parcel_mass` collapses
     per-block Chen to a single district (a block holds ~2% of island mass). The
     full hybrid needs **per-block mass calibration** (`density_field.mass(block)
     / target`), applied in the spike.
   - *Block shape matters.* Acute concave macro-block wedges produce
     near-degenerate fans of parallel districts → regularize / split very concave
     blocks before per-block Chen at scale.
3. **Full hybrid** — Chen/R2 in every macro-block (per-block mass calibration);
   add arterial↔local T-junctions at the seam; assemble + render at scale.
   **ASSEMBLY DONE (2026-06-18).** Single global district mass `M = total_mass /
   total_target` (default 600) applied uniformly to every block → consistent
   district size + automatic density grading. Result: 13 blocks, 192 districts,
   13/13 invariants pass, 127 s, 0 Chen failures (blocks were convex enough that
   the acute-wedge risk didn't fire). Mechanically sound and confirms viability,
   but does **not yet read as a coherent city**. Three artifacts, in stage-3.5
   priority order:
   - **(a) Connectivity is the dominant artifact.** Arterials run *parallel* to
     local Chen streets with no T-junctions — the deferred seam, and at scale it's
     what the eye lands on. Highest priority.
   - **(b) Macro-blocks too coarse.** Only 13 blocks ⇒ each holds 200+ worlds /
     30+ districts, so Chen is still partly out of distribution and the fabric
     reads as large regional super-districts, not neighborhoods. Need finer
     macro-blocks (more town/village nodes, or recursive block subdivision).
     **STAGE 3.5a DONE (2026-06-18).** Replaced graded-L0 seeding with a 3-tier
     SEMANTIC node hierarchy in `build_macro_nodes_hierarchical` (cities = 7 L1
     centroids σ=2; towns = 18 L0 centroids σ=1; villages = 23 loosened density
     peaks σ=0). Key data fact: on this island the cluster hierarchy is INVERTED
     — L0 (18) is the FINEST tier, L1 (7) coarser sub-regions — so L1=city /
     L0=town is what yields the *finer* structure. `compute_macro_arterials`
     generalized to N levels (one Galin network per distinct σ, highest first,
     higher-level pairs excluded): 8 highways / 37 majors / 68 locals. Blocks
     polygonized over ALL three tiers ⇒ **30 macro-blocks** (was 13; median area
     160 vs the old coarse blocks). Re-rendered hybrid at `--total-target 1200`
     (was 600): **599 districts, 0 Chen failures, 30/30 invariants pass, 414 s**.
     Result: most blocks now read at neighborhood scale (core #3 especially —
     varied irregular districts, all-tier convergence at a node). Remaining: the
     finer blocks did NOT fix artifact (c) — see below; cores #1/#2 still show a
     density ridge as a block *edge* with a degenerate parallel-district fan in
     the acute wedge beside it. (c) is now the dominant macro artifact.
   - **(c) Density-attracting arterials bisect cores.** Because arterials hug
     density ridges and block boundaries are polygonized *along* arterials, the
     densest ridges become block *edges* that split a core across two blocks
     (each then calibrated separately, subdividing coarsely). Peaks should sit in
     block *interiors*. Fix candidates: route arterials to skirt the very densest
     cells (connect peaks without crossing their summit), or snap peaks to block
     centers.
4. **Terrain co-generation** — inverse-density height + periphery roughness;
   feed back into cost field; optional switchback styling for steep local roads.
5. **Point relaxation (optional)** — nudge world points toward generated lots.

## Status

Stages 1–3 + stage-3.5a done. The hybrid now uses a 3-tier semantic node
hierarchy (L1 cities / L0 towns / peak villages) → 30 macro-blocks (was 13) →
599 districts at total-target 1200, 30/30 invariants pass, 414 s. The 3-tier
road hierarchy reads clearly and most blocks are at neighborhood scale.
Remaining stage-3.5 work, in priority order: (a) arterial↔local T-junction
connectivity (still the dominant artifact — arterials run parallel to local Chen
streets, no junctions); (c) density ridges still become block boundaries that
bisect cores, with acute-wedge parallel-district fans beside them (finer blocks
did NOT fix this — needs route-around-summit or peak-snap-to-center). Footholds:
`r1_macro.py` (incl. `cluster_centroids`, `build_macro_nodes_hierarchical`,
N-level `compute_macro_arterials`), `r1_seam.py` (incl. `chen_in_block`),
`run_r1_macro.py`, `run_r1_seam_spike.py`, `run_r1_hybrid.py`, plus the
zoom-review harness.
