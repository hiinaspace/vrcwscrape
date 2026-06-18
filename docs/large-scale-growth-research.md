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

## Status

Evaluation only; no code yet. Prototype scope is the open decision (see session).
Arm B (`r1_arm_b.py`) and the zoom-review harness (`run_r1_zoom_review.py`) are
the existing footholds for a macro-layer spike.
