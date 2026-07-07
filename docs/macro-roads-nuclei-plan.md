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
- **⚠ Pre-existing `_assert_partition` blind spot (found during F1, 2026-07-07).**
  On some triangular/wedge districts + seeds, `subdivide_district` emits lots whose
  `unary_union(lots).area` is short of `district.area` (a real overlap) while
  `sum(area)` matches exactly — so the pairwise-STRtree `_assert_partition` (and the
  pairwise `.intersection().area` checks) miss an overlap that `unary_union` catches.
  Confirmed on UNMODIFIED code via `git stash`; not introduced by F1. **This matters
  for S3:** intra-nucleus avenues generate exactly this wedge geometry, so the
  checker could pass bad downtown lots. **Action:** an audit slice on
  `_best_split`/`shapely_split` robustness for acute/triangular geometry + tighten
  `_assert_partition` to a `unary_union`-area check — do this BEFORE or WITH S3.
