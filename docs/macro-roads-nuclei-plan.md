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
| **S5** | **DONE (2026-07-07)**: zone-graded massing (core/inner/fringe by `nucleus_dist`) + per-nucleus landmark quotas (∝ mass, replacing island-wide top-100). Zone is a district-level property (`_zone_for_district`), MAJOR-nucleus districts only — thresholds `core_zone_max_dist=0.3` / `inner_zone_max_dist=0.6` on the `[0,1]`-normalized `nucleus_dist` (chosen from the R-baseline per-major histogram: every major has ≥8 districts ≤0.3 + ≥11 in (0.3,0.6]; beyond 0.6 the tail is districts merely NEAREST the nucleus, not its downtown). Story bands (new named `MassingConfig` fields, `fringe`=pre-S5 byte-identical): core row 3–5 / landmark 8–12, inner row 2–4 / landmark 6–9, detached kept suburban (1–2) in all zones. `build_lots` gains a `zone` param (default `"fringe"` → pre-S5 output byte-identical, regression-guarded). Landmark quotas ∝ `NucleusSpec.mass` over `total_landmark_budget=100` (renamed from `landmark_count`); per-major top-`quota` member worlds by visits (reuses `top_landmark_ids` per-nucleus); minors get none. Bake (`--n-cores 6`, vs R: 77 blocks / 155 arterials / 21 rings / 756 districts / 56 plaza / 15 nuclei / 6 majors / n_fallback 0 / invariant 1.0, all `=`): zones core=85 / inner=168 / fringe=503 (+ 59 park spread across); quotas nucleus0..5 = 41/23/13/9/8/5 (sum 99, budget 100 − rounding), all filled exactly (actual==target); per-(zone,typology) occupied-lot heights all land inside band — fringe detached 2.70–6.60m / row 5.80–9.70m / landmark 12.08–18.96m (byte-identical bands to R), **core landmark 24.89–34.09m** vs fringe landmark 12–19m (the height-contrast skyline), core row 8.91–15.90m. `districts.geojson` gains `zone`; manifest gains `counts.zone_counts` / `counts.zone_typology_counts` / `lot_config.landmark_quota_by_nucleus` / `landmark_count_by_nucleus` | `r1_lots.py`, `run_r1_hybrid.py` | histograms + render |
| **S6** | **DONE (2026-07-07), guidance-grading half only** (greenspace grading — the other S6 half — DEFERRED, see below): per-block density-graded street guidance so street PATTERN varies by density (dense straighter/grid, sparse curvier/organic) on top of the already-present but flat density gradient. `block_norm_density` (mean `fields.density` over raster cell centers inside each block — mirrors `RasterDensityField.mass`'s sampling but averaged, not summed — min-max normalized `[0,1]` over all 77 blocks) feeds `graded_guidance_strength(norm_density) = clamp(BASE·(1−norm_density), floor, BASE)`, `BASE=6.0` (== `chen_in_block`'s pre-S6 uniform default -- the sparsest block alone reproduces old behavior), `floor=1.5` (dense blocks keep SOME guidance, never a pure lattice). Direction verified against `build_terrain_guidance` (weight scales linearly with `strength`) + `scripts/run_r2_fan_probe.py` (config A, today's uniform strength 6.0, is the summit-fan-inducing control; config C, `guidance=None`, removes it): higher strength = more terrain-following curvature, so dense blocks are graded DOWN and no block is ever graded ABOVE today's baseline. `run_all_blocks` gains a `guidance_strength: float \| Sequence[float] \| Callable[[int], float]` input (default = `BASE` uniform, byte-identical to pre-S6); manifest gains `s6_guidance_grading` (base/floor + norm_density/guidance_strength min/median/max) + per-block lists. **Fan-metric gate: PASS.** Reused the sliver-fan probe's elongation metric (`run_r2_fan_probe.py`'s OBB long/short ratio, same formula as `r1_lots._lot_aspect_ratio`) on raw Chen districts (pre-lot-subdivision, so F1's per-lot shape floor can't mask the signal), restricted to `zone=="core"` districts of each of the 6 majors (nucleus_id 0..5 == rank 1..6). Baseline (S5.1, uniform 6.0) vs S6 (graded), share elongation>4 / max: core1 0%/3.47→0%/3.70, core2 4.8%/9.57→4.2%/9.57 (same single pre-existing outlier, diluted by +3 benign districts — improved, not regressed), core3 0%/3.32→0%/3.36, core4 0%/2.16→0%/2.15, core5 0%/1.93→0%/2.00, core6 0%/1.89→0%/3.18 (still well under threshold). No core regresses. Bake (`--n-cores 6`, vs S5.1 baseline, `=` unless noted): 77 blocks (=), 46 hwy/42 major/67 local arterials (=), 15 core rings (=), fallback 0 (=), invariant 1.0 (=), districts 756→763 (+7), Chen streets 603→616 (+13), zones core 85→88 / inner 168→157 / fringe 503→518 (S5 reclassification tracking the slightly different district geometry — cosmetic). guidance_strength: min 1.5 (floor, densest) / median 3.17 / max 6.0 (sparsest, unchanged); norm_density: min 0.0 / median 0.47 / max 1.0. 531 tests green (514 baseline + 17 S6). Greenspace grading explicitly OUT OF SCOPE for this slice (already correlates with sparsity via surplus lots; revisit only if a gate shows sparse districts too built-up). | `run_r1_hybrid.py` | fan-metric ✅ + render (orchestrator vision-gate pending) |
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
| — | **mini visual gate** — **PASSED (2026-07-07, Opus vision + user at desk + codex cross-model review).** Confetti gone (continuous red skeleton links all 6 majors, red carries across rings → beltway-ringed downtowns), core6 keyhole severed, villages kept cheap contours, braids/hooks gone. User verdict: **red density is good — keep the full network** (no sparser-trunk change). Two nits raised → routed to S7 (below): (a) promoted highway tier reads rigid at ring↔arterial corners; (b) plazas too circular in 2D. Codex findings → R-harden (below). | | |
| **S5** | **DONE (2026-07-07)**: zone-graded massing + per-nucleus landmark quotas landed. Zone by normalized `nucleus_dist`, MAJOR nuclei only (minor/village → fringe = suburban, unchanged): `core` ≤0.3, `inner` ≤0.6, else `fringe`. Elevated story bands (named `MassingConfig` fields): core row 3–5 / landmark 8–12, inner row 2–4 / landmark 6–9, detached keeps suburban in all zones. Landmarks: island-wide top-100 replaced by per-major-nucleus quotas ∝ mass (`total_landmark_budget`=100 renamed from `landmark_count`; quotas 41/23/13/9/8/5 = 99). Bake (`--n-cores 6`, vs R baseline): 77 blocks / 756 districts / fallback 0 / invariant 1.0 (unchanged — massing doesn't touch geometry). Zones: core=85 / inner=168 / fringe=503 districts. Per-zone height (m): core p50 9.1 max 34.1, inner p50 6.3 max 28.1, fringe p50 6.0 max 19.0 — contrast lands, fringe suburban preserved. 506 tests green (490 baseline + 16 S5), app dataset re-exported. **Two tuning questions surfaced at gate (see below), not yet actioned:** (a) 50/99 landmarks fell in FRINGE districts of major nuclei (popular worlds far from center → modest 4–6 story towers scattered in the suburbs; only 12 landmarks hit the 8–12 core band); (b) 44% of core-zone lots are still `detached` (suburban houses in downtown — area-per-world ≥150 m² keeps them detached). | `r1_lots.py`, `run_r1_hybrid.py` | histograms ✅ + 3D extrude render (user) |
| **S5 gate** | **RESOLVED (2026-07-07, user at desk + extrude view).** Height/skyline accepted. (a) fringe landmarks — LEFT AS-IS (user fine with it). (b) core detached — LEFT AS-IS (hard to judge with box buildings; revisit if buildings get skinned). NEW nit → S5.1: outskirts buildings read like a WAREHOUSE district — footprints scale with lot size; want a max footprint so large lots keep farmhouse-scale buildings + more inter-building space. | | |
| **S5.1** | **DONE (2026-07-07)**: footprint WIDTH cap (the depth was already capped; width wasn't → wide lots got full-frontage warehouse slabs). Added `MassingConfig.{detached,row,landmark}_width_max_m` (0.0 = uncapped): detached=11 m, row/landmark=0.0. Cap applied in `_oriented_footprint` BOTH paths — frontage (center the along-frontage window to width_max → symmetric side yards) and landlocked-OBB (cap half-width). Detached-only: row keeps full width (continuous terrace), landmarks stay prominent. Bake (`--n-cores 6`): invariants unchanged (77 blocks / 756 districts / fallback 0 / invariant 1.0 / zones core=85 inner=168 fringe=503). Detached footprint m²: p50 88.8, **p95 132.0 (= the 11×12 cap)**, was much larger; row/landmark unchanged. **Residual:** 0.3% (27/8157) of detached exceed the cap via the degenerate-slab `_footprint_clamped` fallback (uses a lot-wide inward buffer) — negligible for the warehouse read; cap the fallback too if a future gate still shows warehouses. 514 tests green (+4). Done inline (not delegated — tight, well-scoped). | `r1_lots.py` | footprint histogram ✅ + extrude (user) |
| S6 | **DONE (2026-07-07)** — density-graded street guidance, fan-metric gate PASS (no core regresses vs S5.1 baseline). Full report in the S6 row of the wave table above. | | |
| **R-harden** (pre-S7) | **DONE (2026-07-07)**: both fixes landed + pushed (commit after S5). #3: `_open_ring_polygon` now keeps the anchor-bearing component (threads the density-weighted anchor from a preliminary `build_nucleus_specs` pass; falls back to largest-area with a counted `n_ring_open_anchor_fallback` if the opening eats the anchor). #2: `major_tier_total_length` in the hybrid manifest. Bake: invariants UNCHANGED (6 majors same rank, fallback 0), `n_ring_open_anchor_fallback=0` (latent, didn't fire), `major_tier_total_length=361.9` (healthy). 510 tests green. — codex cross-model review findings on the T-geo+B+R restructure: **(#3, real latent bug)** `_open_ring_polygon` keeps largest-**area** component after `buffer(-w).buffer(+w)` — must keep the component containing the nucleus anchor/peak (didn't fire this bake: majors lost only 1–3% mass, no split; silently moves a nucleus on a future dataset). **(#2, robustness)** add a major-tier total-length manifest metric so a future hub-spoke map with a near-empty major tier is visible. Codex #1 (one shortest path per pair, not all equal-cost) is mitigated — widths come from `betweenness` which splits across equal paths; only symmetric-branch tier COLOR is arbitrary (minor). #4 (partial-braid stitch attaches to nearest kept line, can pick wrong absorber near crossings) / #5 (nucleus anchor collapses to one gateway node — a multi-station core gets one functional entrance) carried as S5/S7 implementer notes. | `r1_macro.py`, `run_r1_hybrid.py` | unit + bake invariants |
| **S7** (user chose "full S7 now", 2026-07-07) — split into S7b + S7c (S7a plaza de-circle DEFERRED, below) | | | |
| **S7b** | **DONE (2026-07-07)**: per-fronting-tier front setback + near-highway displacement — the real buildings-off-arterials fix (wave2 slice-1 used a flat `road_clear_m=4.6 m` placeholder while a highway ribbon reaches ±12.5 m from its centerline, which runs along the macro-block boundary a boundary lot fronts → building sat INSIDE the ribbon). New `FrontingRoadIndex` (pure `r1_lots`, STRtree of tiered arterial + promoted-ring-arc + whole-ring centerlines built in `run_r1_hybrid._fronting_road_segments`) maps each lot's frontage edge to the nearest tiered road within a 0.3-unit threshold (exact-distance tie → widest tier, so a promoted highway ring arc beats the whole "ring" it overlaps), else → narrowest `"street"` tier. `road_clear_m(tier) = DEFAULT_STREET_WIDTHS[tier]·mpu/2 + pedestrian_clearance_m` (new `MassingConfig` field, 2 m; widths IMPORTED from `r1_mesh`, single source — NOT duplicated): highway 14.5 m / major 10.75 m / ring 9.5 m / local 8.25 m / street 5.125 m. `_oriented_footprint` gains a `road_clear_m_override` (None → flat `road_clear_m`, byte-identical regression path); `build_lots` gains a `fronting_index` param (None → pre-S7b byte-identical). Near-highway displacement: a highway/major-fronting lot too SHALLOW to fit any building behind its wide setback is EXCLUDED from the per-district Hungarian assignable set (→ greenspace, its world re-displaced onto a deeper lot in the SAME district), provided ≥ n buildable lots remain; else the exclusion is skipped for feasibility (no world ever lost — the shallow lot keeps its world with the degraded shallow-lot footprint). Within-tier betweenness width scaling **DEFERRED** to S7c (fixed per-tier widths used; `MacroEdge.betweenness` still stored). Bake (`--n-cores 6`, vs S6, `=` unless noted): 77 blocks (=), 763 districts (=), zones core 88 / inner 157 / fringe 518 (=), **n_fallback 0** (=), **invariant 1.0** (=), **12311 worlds placed, none lost** (=). S7b: 2220 occupied lots on wide (highway 1319 + major 901) frontage now set back past the ribbon; ring 1063 / local 823 / street 6718; **13 no-build lots excluded → greenspace (worlds re-displaced)**, **282 fabric districts hit the feasibility fallback** (shallow highway/major frontage with too few deeper lots to displace within-district — worlds kept, footprints degraded; the widest-arterial clearance in those is the accepted feasibility limit, orchestrator vision-gates). Manifest gains `counts.s7b_setback_by_tier` / `s7b_wide_setback_lots` / `s7b_no_build_displaced` / `s7b_no_build_infeasible_districts`. **Rear-anchored degraded footprint** (coordinator follow-up): when a highway/major-fronting lot degenerates (too shallow for the wide setback, `d_hi<=d_lo`), `_oriented_footprint(rear_anchor_degenerate=True)` places the building at the REAR of the frontage frame (a ≤`depth_max` slab hugging the far-from-arterial edge) instead of the lot-center clamped circle — can only push the degraded building away from the road. Re-measured (buffer highway+major arterial/ring-arc centerlines by `DEFAULT_STREET_WIDTHS[tier]/2`, union, count occupied footprints intersecting, /12311): in-ribbon footprint **area 196.2 → 166.0 u² (−15.4%)**; substantial (>75% of footprint in-ribbon) **~990 → 962**; any-intersect count 1757 → 1755 (~flat — the residual is dominated by lots whose WHOLE footprint sits inside the wide ribbon because Chen fabric abuts the arterial centerline, the accepted feasibility floor that per-lot rear-anchoring cannot clear; deferred to S7c mesh-side ribbon/setback). 554 tests green (+11 S7b). | `r1_lots.py`, `run_r1_hybrid.py` | render (buildings clear of wide roads) ✅ + displacement stats |
| **S7c** | mesh emission: betweenness-scaled road ribbons (consume the S7b width model) + **highway/ring-tier centerline fillet** (mesh-LAYER Chaikin/corner-round on the promoted-highway centerline — NOT source geometry, so blocks don't re-derive; user's escape hatch) + buildings → final OBJ. The ring fillet also breaks up the concentric-circle regularity around plazas (addresses the plaza-too-circular nit per the user's own suggestion — "smooth the highways that surround them"). | `r1_mesh.py`, `run_r1_greybox_mesh.py` | mesh bake → **oni walk (user)** |
| ~~S7a plaza de-circle~~ | **DEFERRED (2026-07-07)**: perturbing plaza-ring filler radii broke 3 exactness-invariant tests (exact plaza radius/centroid) for a minor 2D-only cosmetic the user was ambivalent about ("probably fine in 3d"). S7c's ring fillet addresses the concentric-regularity per the user's own suggested approach; revisit with an app-side render treatment only if the user still finds plazas too circular in 2D after S7. | | |

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
