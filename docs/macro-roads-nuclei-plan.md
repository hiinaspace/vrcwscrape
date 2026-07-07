# Macro roads + settlement nuclei — "sprawling city" wave (option B)

Status: DESIGNED (2026-07-07), ready to implement. Settled via a recon pass +
a Fable senior-review + Opus synthesis. Extends [wave2-plan.md](wave2-plan.md)
(massing landed) and supersedes the *macro* framing of
[terrain-roads-research.md](terrain-roads-research.md) (which was tactical only).

## The decision: spatial identity B

The shipped city reads as **one uniform master-planned suburb** — which a recon
confirmed is a **drift** from the original documented intent
(`large-scale-growth-research.md`:17-20, 2026-06-18, verbatim): *"'the entire
island is one master-planned suburb' feel — rather than the 'small towns growing
into a sprawling city' character we want."* That intent was never rescinded; it
was narrowed by accretion into "single city + density gradient."

**User decision (2026-07-07): option B — a single "sprawling city of distinct
nuclei."** Recover the original intent: distinct downtown centers with density
falloff between them, as realistic sprawling-city planning. Rejected: pure
single-dense-city (do-minimum, keeps the uniform feel) and continent/archipelago
(needs a DR/placement change + floating-origin fix — too big).

**Constraints B respects (fixed):** no DR/placement change (nuclei come from the
EXISTING density field; the parked DR-basis decision stays parked); within the
~10 km float-precision budget; continuous fabric (interstitial = low-density
sprawl, NOT empty land); validated suburban massing stays (except cores, below).

## What the review found (the reframes that shaped the plan)

1. **Root cause — downtowns have NO internal roads.** `clip_arterials_to_cores`
   subtracts cores from arterials, so every dense core is a single ring-fenced
   Chen block. **The densest places on the island have the least road structure —
   the inverse of a real city.** This is a first-order cause of the "uniform
   suburb" read. Fixing it (intra-nucleus avenues) is the highest-leverage move.
2. **Density already grades the fabric (ρ = −0.853) and it doesn't read.** So
   "make fabric express the falloff" is aimed at a partly-solved problem; what's
   missing is **discrete, spatially-coherent** signals — height contrast, landmark
   clustering, zone massing, street-pattern change at the core boundary — not more
   continuous grading.
3. **Smoothing must go INSIDE `build_macro_layer`** (between routing and
   clipping/polygonization), not bake-side. Everything downstream (macro-blocks,
   per-block Chen, gates, lots, exports) derives from the single `arterial_lines`/
   `ring_lines` source of truth; a bake-side-only smooth desyncs roads from blocks
   and puts buildings back on roads. Also: **core ring roads are jagged too**
   (pixel-blob boundaries) — smooth them, not just arterials.
4. **Seeding is already a 3-tier hierarchy** (7 L1 "cities" / 18 L0 "towns" /
   density-peak villages; highway tier already Delaunay over the 7 cities). Cities
   are *visit-weighted cluster centroids*, which can land in a density **saddle** —
   so the fix is **peak-snapping the seeds**, not re-seeding from cores (circular:
   cores grow from the seeds).

Jaggedness detail: arterials are 8-connected least-cost paths, DP-simplified at
tolerance 1.5 u = **37.5 m** — long straight chords joined at abrupt kinks, no
spline. Chaikin corner-cutting deviates *less* than DP already licensed, so it
adds no feasibility risk; endpoints preserved keep macro-node junctions fused.

## User taste calls (2026-07-07)

- **Taller downtowns (cores only):** top-K nuclei get landmarks ~8–12 stories,
  core row housing 3–5; inner/fringe keep the validated suburban massing. This
  intentionally relaxes the earlier "no vertical stacking at first" stance —
  contained to core downtowns, where height contrast is the strongest "distinct
  downtown" signal at map + street level. Final numbers at the visual gate.
- **No greenbelt pseudo-gaps:** sell "between towns" via low-density fabric + the
  §1 terrain roughness octave (rugged land falls in the sparse interstitial).
  Gaps are the rejected archipelago identity; polycentric sprawl reads through
  centers.
- **Terrain: keep density = height** (downtown-on-a-hill reinforces distinct
  centers, and it's what shipped + validated). **Rescinds** the unimplemented
  2026-06-18 inverse-density clause in `large-scale-growth-research.md`.
- **K (how many major nuclei):** deferred to the S2 ranked-core-mass table
  (default 6 of the ~13 detected cores) — decided when the table is visible.

## The wave (each = one review-gated slice; F-slices parallel with S-slices)

| # | Slice | Files | Gate |
|---|---|---|---|
| **S1** | `smooth_polyline` (Chaikin + light DP) wired INTO `build_macro_layer` on arterials; ring smoothing via morphological close + Chaikin; re-pin macro golden | `r1_macro.py` | unit + re-bake render |
| **F1** ∥ | sliver shape-floor: hard min-width/aspect **reject** + leaf merge-to-neighbor in `subdivide_district` (today only *scores down* aspect>4, never rejects) | `r1_lots.py` | unit + fallback-rate |
| **F2** ∥ | per-**edge** (not per-street) perimeter dedup — `_all_nodes_on_boundary` is all-or-nothing today, so one drifted vertex re-adds a whole boundary-hugging street as "local" | `r1_connect.py` + call sites | unit + seam render |
| **S2** | peak-snapped seeding + `NucleusSpec` (anchor = density-weighted core centroid, polygon, mass, rank, label, influence_radius); top-K majors; export `nuclei.geojson` + per-district `nucleus_id`/`nucleus_dist` | `r1_macro.py`, `run_r1_hybrid.py` | unit + ranked-core table (decide K) |
| **S3** | **intra-nucleus avenues** (the keystone): spokes from ring T-junction stations to an inner **plaza ring** (annular sectors dodge Chen's acute-wedge pathology); unclipped tier-1; plaza → `kind="park"` block; fusion/export via existing generic path | `r1_macro.py` | unit + core-zoom renders |
| **S4** (cond.) | per-tier cost: highways route over a Gaussian-blurred density field → deliberate inter-nuclei boulevards. Only if S1+S3 gate still shows wandering trunks | `r1_macro.py` | render |
| — | **VISUAL GATE** (main thread + user): full re-bake, 2D + core zooms; tune downtown-height / thresholds | | |
| **S5** | zone-graded massing (core/inner/fringe by `nucleus_dist`) + per-nucleus landmark quotas (∝ mass, replacing island-wide top-100) | `r1_lots.py`, `run_r1_hybrid.py` | histograms + render |
| **S6** | per-block `guidance_strength = 6·(1−norm_density)` grading + graded greenspace-share in sparse districts | `run_r1_hybrid.py`, `r1_seam.py`, `r1_lots.py` | fan-metric + render |
| **S7** | betweenness-scaled widths + per-segment setback (`road_clear_m` flat 4.6 m → per-tier) on final geometry — the real fix for buildings-on-arterials | `r1_mesh.py`, `r1_lots.py`, `r1_connect.py` | mesh bake + oni walk |

## Visual gate outcome (2026-07-07 — Opus vision + Fable senior review, concurring)

**Identity B: PASS.** Polycentric read achieved at overview (western arm reads as
a bead-chain of satellite towns feeding the metro); downtowns read as structured
ring+spokes+plaza at zoom. Arterial/ring jaggedness gone. **S4: SKIP** — trunks
do not wander post-Chaikin; the residual island-scale pathology is tier
fragmentation + corridor duplication, which a blurred cost field would not fix.

Two hard sequencing gates added (both concurring reviews):

| # | Slice | Files | Gate |
|---|---|---|---|
| **P** (pre-S5) | **plaza park+size fix** — **DONE (2026-07-07)**: mass-scaled radii landed (sqrt-of-mass 3.0–4.0 u; bake radii 4.00/3.60/3.34/3.18/3.13/3.00, all 6 plazas kept), plaza districts excluded from assignment + forced park (56 plaza districts, all zero-world, zero lots; +798 worlds snapped outward; `r1_lots.py` untouched — exclusion wraps `assign_worlds_to_districts` bake-side). Residual: Chen still subdivides plaza blocks (~9 park districts each) and 9 exported Chen streets cross plazas — flagged for orchestrator. Original scope: S3's "no extra plumbing needed" assumption was wrong: `kind` derives from world_count and worlds ARE assigned inside plaza discs (4/6 plazas subdivided as fabric). Exclude plaza-derived districts from `assign_worlds_to_districts` candidates, force `kind="park"` end-to-end (macro → lots → app export), and mass-scale plaza radius (majors ~3–4 u ≈ 75–100 m; villages keep small — 0.85 u is roundabout-scale, unreadable as a plaza in 3D) | `r1_macro.py`, `run_r1_hybrid.py`, `r1_lots.py` | unit + re-render cores 1/2/6 (zero dots in plazas) |
| ~~**T** (pre-S7)~~ | ~~trunk cleanup (tier-continuity label patching)~~ **SUPERSEDED** by the road-hierarchy restructure below — T's tier-continuity/handoff logic is obsoleted by functional tiers; its geometry half survives as slice T-geo | | |

Other gate findings folded into existing slices: S5 keeps landmark quota ∝ mass
(core4 is a weak island-edge nucleus — let quotas starve it naturally, don't
hand-boost); S7 additionally gives ring roads + spoke avenues explicit
tiers/widths so the 6 majors differentiate from village rings (all ~15 rings
currently use an identical "stamped wheel" motif). Minor/cosmetic, not slotted:
core-interior Chen grain no coarser than suburbs (S5 height contrast carries the
downtown read); onion-ring nested district edges on core6's north keyhole;
core4 render window off-data white band (artifact script only). S6 unchanged —
seam block 24 shows the deferred interior-fabric-gap trigger is NOT fired.

## Road-hierarchy restructure (2026-07-07, post-gate — USER ADOPTED option B)

The user's gate observation — highways/rings "degenerate into short segments
with parallel/frontage roads … the hierarchy levels are unaware of each other"
— is the verified mechanism: **tier = construction order, not function.** Each
Delaunay tier's edges are independently least-cost routed; tiers share only a
node-pair exclusion set, never geometry (braiding), and `clip_arterials_to_cores`
preserves tier per fragment (confetti). A Fable design review traced every tier
consumer: tier is read ONLY at render/width/export layers (`r1_mesh.py` width
dicts, `r1_app_export.py` TIER_KIND/TIER_WIDTH, render color tables, connect
metrics) — nothing structural. So re-deriving tier inside `build_macro_layer`
propagates everywhere automatically at near-zero risk to the validated stack.
Rejected: full bottom-up regrowth (discards gate-praised corridor geometry);
bake-side relabel (desyncs the single source of truth — same lesson as S1
smoothing); raw-betweenness tier thresholds (parallel near-core paths split
flow → holes in the red skeleton exactly where the eye looks).

**Adopted plan (replaces T and slims S7):**

| # | Slice | Content | Files | Gate |
|---|---|---|---|---|
| **T-geo** | **DONE (2026-07-07)**: same-corridor dedup PRE-clip landed (`dedup_corridor_lines`: priority-ordered absorb of sustained near-runs, tol 1.2 u / min-absorb 5 u so transversal crossings survive; merged tier = max of contributors; remainders stitched onto the corridor via exact shared vertices; untouched lines bit-identical) + post-clip `snap_endpoints_to_rings` (snapped station inserted as exact ring VERTEX, so `_ring_junction_stations` tol=1e-6 still matches) + `prune_short_dangles` (< 8 u, free degree-1 end; ring-terminating fragments and macro-node termini always spared). Every stage re-emits a fresh aligned `arterial_lines`/`edges` pair. Bake: 41 routed lines had runs absorbed, 6 endpoints snapped, 0 dangles pruned (dedup+snap removed all true dangles); arterials 136→109, blocks 79→77, districts 759→757, T-junctions 96→101, all 6 plazas + 15 nuclei survive, fallback 0, invariant 1.0. Residual (flagged): ring-terminating micro-fragments (< 1 u clip confetti crossing ring corners) are contract-spared and remain | `r1_macro.py` | unit + overview render (no braids, no hooks) |
| **B** | **DONE (2026-07-07)**: functional tiers landed (`assign_functional_tiers` in `build_macro_blocks_with_cores`, post clip+snap+prune+spokes; `MacroParams.functional_tiers` to disable). Final linework (clipped arterials + spokes + core/plaza rings) noded at endpoints + true intersections into a MultiGraph (exact shared vertices per T-geo, 1e-6 merge; parallel ring arcs keep independent identity); anchors → nearest graph node; highway = union of length-weighted least-cost paths over all major pairs, major = all-nuclei pairs minus highway, rest local; construction tier preserved as `MacroEdge.build_tier`; exact normalized length-weighted edge betweenness stored as `MacroEdge.betweenness` (S7-slim's input, unused otherwise). Arterials/spokes re-emitted per junction segment (aligned, station-ordered); ring arcs emitted as new `ring_arc_lines`/`ring_arc_edges` layer fields — `ring_lines` stays whole rings for existing consumers. Bake (vs T-geo baseline, structure unchanged: 77 blocks / 757 districts / 21 rings / 6 plazas / fallback 0 / invariant 1.0): arterial records 109→158 (junction re-segmentation), tier mix 54 hwy (406 u) / 37 major (330 u) / 67 local (562 u), ring arcs 51 hwy (221 u) / 39 major (148 u) / 76 local (416 u), 74 records changed tier, **0 disconnected nucleus pairs**; overview shows one continuous red skeleton linking the 6 majors with red spokes into every core. Residual (flagged): overview render + greybox export still draw rings as undifferentiated "ring" kind — promoted ring-arc tiers live on the layer only until S7-slim (or a one-line render tweak) consumes them, so the red skeleton visually breaks at ring traversals | `r1_macro.py` | unit + overview render (continuous red skeleton, red reaches into cores) |
| **R** | **DONE (2026-07-07)**: rank-scaled ring regularization landed (`_regularize_ring_polygons` in `build_macro_blocks_with_cores`, applied to `detect_core_regions` output BEFORE clip/snap/station-derivation so the clip boundary, block-protection region, mass/rank domain and spoke-station source all re-derive from the same regularized polygon). Morphological **opening** (`_open_ring_polygon`: `buffer(-w).buffer(+w)`, largest component if it splits, fall-back-to-input if it empties) at `w=1.75 u` for MAJOR cores (top-K by mass, read off a preliminary `build_nucleus_specs` pass matched by polygon identity), `w=0` (skip → today's cheap contour) for minor/village cores → severs keyhole necks/lobes on majors + de-stamps the shared "wheel" motif. Elliptic-Fourier/periodic-spline low-pass (`_fourier_low_pass_refit`, ~8 harmonics, `buffer(0)` validity guard + >20%-area-change guard, both fall back to the un-refit ring) implemented + tested but **flag OFF for the bake** (`MacroParams.ring_fourier_refit=False`). Promoted ring-arc rendering + export wired (slice B's `ring_arc_lines`/`edges`): `run_r1_hybrid._draw_layers` draws promoted arcs SOLID tier-colored above the dashed ring; `_greybox_arterials_geojson` emits them as extra records (`tier="ring"` kept for back-compat, new `ring_tier` field carries the promoted tier). **Never convex-hull.** Bake (vs B baseline, `--n-cores 6`): 77 blocks (=), arterial records 158→155 (46 hwy / 42 major / 67 local; tier re-segmentation shifts from the changed major ring geometry), 21 rings (=), 757→756 districts (−1), 56 plaza (=), **15 nuclei / 6 majors (=), no rank flip** (all 15 in identical order; majors lose ~1–3% mass to the opening, minors byte-identical mass; K=6 margin rank6=246.25 vs rank7=235.34, narrowed from 18 to 11 u but safe), n_fallback 0, invariant 1.0, **0 disconnected pairs**; 165 promoted ring-arc export records (52 hwy / 42 major / 71 local) | `r1_macro.py`, `run_r1_hybrid.py` | core-zoom renders 1/2/6 |
| — | **mini visual gate** (main thread + user; cheap 2D overview + 2–3 core zooms). Blocks/districts WILL reshuffle under dedup+R: re-pin goldens, re-check the S2 ranked-mass table (rank flips at the K=6 margin are the real risk) | | |
| S5 ∥ S6 | unchanged, but AFTER the mini gate — S5's `nucleus_dist` massing must not tune against pre-dedup fabric | | |
| **S7-slim** | widths from the stored betweenness scalar + per-tier setbacks (`road_clear_m` flat → per-tier) + mesh emission. Deleted from S7: tier assignment, ring/spoke tiering, betweenness derivation (all moved into B) | `r1_mesh.py`, `r1_lots.py` | mesh bake + oni walk |

Do-NOT-do (from the review): no network regrowth; no bake-side/export-side
relabel; no betweenness before dedup; no ring convex-hull; no silent
`arterial_lines`/`edges` desync; no S5 before the mini gate; no `chen_*` edits.
Fused-graph (Chen-local) betweenness deferred until a gate shows local street
widths need it. The user's "adjust T-junctions/curvature to be highway-like"
idea: optional cosmetic slice AFTER B's renders exist, decided then.

## Deferred / known-limits (recorded, not dropped)

- **Interior-fabric gap:** 14/26 non-core blocks have zero arterial↔local
  T-junctions (Chen emitted no interior streets — connectivity-wave residual).
  Under B these are the interstitial sprawl; fabric that visibly doesn't connect
  reads "unplanned." chen_*-adjacent → its own wave. **Revisit trigger:** if the
  wave gate shows disconnected sparse blocks.
- **Fréchet/control-cost trunk merging** (Galin): not built. **Revisit trigger:**
  if the gate still shows near-coincident parallel trunks after peak-snap + smooth.
- **Per-segment width (S7)** is also the arterial-tier fix promised in
  `wave2-plan.md` (slice-1 uses a flat Chen-width `road_clear_m` placeholder).
- **✅ RESOLVED (subdivision-robustness slice, 2026-07-07).** Two things fixed
  before S3: (1) **F1 Voronoi-fallback regression** — F1's hard shape-floor sent
  62/756 (8.2%) of dense districts to the ugly Voronoi path; a per-district floor
  RELAXATION ladder (`_relaxed_lot_configs`) drops that to **0/756**, `fallback_ok`
  true. (2) The "wedge overlap" the partition checker was flagged as missing turned
  out NOT to be a real overlap — leaves are valid, pairwise intersections exactly 0,
  `sum(area)` exact; GEOS's cascaded `unary_union` just computes a wrong (short) area
  because repeated `shapely_split` misaligns shared edges by a few ULPs. Fixed with a
  1e-9 precision-grid snap at the split site + defensively in `_assert_partition`
  (which already had the union-area check). So acute/wedge subdivision (which S3
  generates) is now robust.
