# Chen/Yang Strict Reimplementation Contract

Last updated: 2026-06-09. History lives in git; this doc states only current truth.

## Mission

Reimplement Chen et al. 2024, "Hierarchical Co-generation of Parcels and Streets
in Urban Modeling", faithfully enough that square/oval/triangle layouts match
the paper's visual quality and Table 1 statistics. Yang et al. 2013 (+
supplements) is the guide for the cross-field/streamline machinery Chen depends
on. The output is a reusable local layout generator for the larger VRChat-worlds
city pipeline, validated on controlled shapes first.

## Non-Goals

- Do not optimize for metrics if output visibly misses paper behavior.
- Do not hide paper-fidelity gaps behind renames, smoothing, or presentation.
- Do not work on the full DR-world dataset until the controlled shapes pass.
- No third-party skills/plugins/hooks/MCP servers; repo-local docs and code are
  the control surface.

## Sources

Papers under `artifacts_full/literature/`: `chen2024_urban_modeling.{pdf,txt}`,
`yang2013_urban_pattern.{pdf,txt}` + supp1/supp2, `bouaziz-2012.pdf` (ShapeOp).
Body text extracts cleanly; **tables and figures do not** — read the PDF
directly (e.g. `pdftoppm -r 600` crops) rather than guessing. Paper Table 1
(p.11) reference rows for Rect/Triangle/Ellipse are transcribed (human-verified
at 1200 dpi) into `chen_metrics.py`.

## Architecture (post-2026-06-09 rewrite)

The generation loop follows paper Section 4 structurally: level-synchronized
binary splits of every splittable parcel, immediate local Fig. 7 short-edge
welds, per-level street extension, min-parcel-area termination, then Section 5
optimization.

- `chen_mesh.py` — mutable ring-based `ParcelMeshEditor`: local
  `split_parcel` (conforming vertex insertion into neighbor rings),
  `merge_short_edge` (Fig. 7 vertex weld with rollback; bowties structurally
  impossible), `snapshot()` to the frozen `chen_core.ParcelMesh` (vertex ids
  stable across snapshots).
- `chen_core.py` — frozen mesh/graph dataclasses (paper Fig. 4), Chen
  irregularity (Eq. 1), split score (Eq. 2) helpers, street decomposition,
  `evaluate_layout_invariants`.
- `chen_field.py` — per-parcel streamline candidates: default grid-smoothed
  4-RoSy; opt-in `yang_d_field` / `yang_b_field` modes (approximations,
  labeled). Optional `RasterGuidanceField` blend (R1 regional extension, off by
  default, not Chen/Yang paper machinery): adds a weighted 4-RoSy alignment
  constraint to the `grid_smooth` field, attenuated so boundary alignment
  dominates inside the boundary radius; `yang_*`/`boundary_blend` modes ignore
  it with a logged diagnostic. `generate_layout_for_boundary` also exposes
  `split_weights` (Eq. 2 lambda override). Both default to byte-identical
  behaviour. Also defines `RasterDensityField` for the R2 density-mass mode
  below (off by default, not Chen/Yang paper machinery).
- `chen_streets.py` — `extend_street_network` per Section 4.2: unreachable
  grouping, strict I-then-L access enumeration (Fig. 9, incl. case-5
  recursion), junction-weighted Dijkstra connection, cul-de-sac avoidance
  (default on).
- `chen_generate.py` — the hierarchical driver. `min_parcel_area` is the
  paper-faithful input; `parcel_count` is a convenience
  (`min_area = area / (1.5 * count)`, emergent counts ~1.1-1.35x target;
  squares quantize to powers of two). Optional R2 regional extension (off by
  default, **not** Chen/Yang paper machinery): passing `density_field`
  (`RasterDensityField`) + `max_parcel_mass` switches the parcel "size" measure
  from geometric area to integrated density mass — a parcel splits only while
  its mass exceeds `2·max_parcel_mass` (on top of the unchanged geometric floor)
  and Eq. 2's size term scores child mass balance, so splits chase the
  population surface (see `docs/regional-2_5d-research.md`, R2 results). With it
  `None` the generator is byte-identical to the paper path. v1 leaves streamline
  candidate spacing geometric; mass is the smoothed-density integral, not the
  true world count.
- `chen_optimize.py` — bounded ShapeOp-like projection (Section 5 energies).
  Untouched by the rewrite; see ledger.
- `chen_metrics.py` — paper Table 1 metrics, verified reference rows,
  `compare_to_paper` drift gauge, loose `paper_fidelity_bands` for tests.
- `chen_artifacts.py` — SVG/PNG/GeoJSON/manifest export with the slimmed
  (~74-key) summary including comparator deltas.

Scripts: `scripts/run_chen_strict_shapes.py`,
`scripts/run_chen_strict_scale_suite.py` (12/24/48/96 targets;
`--min-parcel-area` supported).

## Definition Of Done (per wave)

- Tests/diagnostics would catch the visual or algorithmic failure addressed.
- Square/oval/triangle artifacts regenerated and visually reviewed (read the
  PNGs; compare against paper Fig. 11) when generation behavior changes.
- From `mapgen/`: `uv run ruff check .`, `uv run ty check`, and the focused
  Chen tests run (quick lane `-m "not slow"` for loops, full suite before
  declaring a wave done), or skips reported with reasons.
- Ledger updated; a wave must not end with an unowned `Blocking` item.

## Replication Debt Ledger

| Area | Status | Evidence | Priority | Next action |
| --- | --- | --- | --- | --- |
| Curved-shape parcel polygon types | Gap | Oval at 48-target: 23 quad / 19 pent / 20 hex vs paper ellipse 124/128 quad; visuals look quad-like, so either the 135° corner rule misreads sampled curved sides or splits land off the field | Blocking (next wave) | Audit approximate-polygon corner detection on curved parcels vs paper Fig. 2 semantics, then field quality |
| Triangle junction angles | Gap | junction_angle_dev 21.7° vs paper 2.94° at 48-target | Blocking (next wave) | Likely same root as above + field fidelity near corners |
| Yang D/B field fidelity | Partial | Opt-in modes use clipped SciPy Delaunay/uniform Laplacian approximations, not constrained Delaunay/cotan | Non-blocking until corner/junction debt clears | CDT (`triangle` lib) + cotan Laplacian; exact DIV/DB/DS/CT scoring |
| Optimizer exactness | Partial | Local LSQR projection with guards, not ShapeOp; paper's optimizer barely perturbs (input already regular) | Non-blocking | Bounded `compas_shapeop` backend ablation behind the existing constraint layer; delete guard layers if regular input makes them unnecessary |
| Irregularity outliers | Gap | irregularity_max 2.0-4.1 on curved shapes (paper max 0.16); avg fine | Non-blocking | Inspect the outlier parcels (likely shape-corner parcels); may resolve with corner-detection fix |
| Interpolation-edge rendering | Deferred | Fig. 7 weld tags live on the editor, not the layout; only a count reaches metrics | Deferred | Expose on GeneratedChenLayout if dark-green rendering is wanted |
| Yang template system | Missing | No row/spike/area templates | Deferred (Chen-only scope) | Only if full Yang replication becomes a goal |

## Workflow

Main thread: orchestration, slice review, milestone visual eval (read artifact
PNGs vs paper figures), this doc. Implementation subagents (opus/sonnet): one
bounded slice each with explicit write scope and test target; report files
changed, commands+results, remaining debt (Blocking/Non-blocking/Deferred).
Regression surface for subagent loops = Table 1 comparator + invariants, not
visual judgment. Keep this doc slim — no changelogs.

## Verification Commands

From `mapgen/`:

```bash
uv run ruff check .          # 2 pre-existing errors outside chen_* are known
uv run ty check              # pre-existing diagnostics outside chen_* are known
uv run python -m pytest tests/test_chen_*.py -m "not slow"   # quick lane
uv run python -m pytest tests/test_chen_*.py                 # full, pre-wave-end
uv run python scripts/run_chen_strict_shapes.py --out-dir artifacts/chen_strict_shapes
uv run python scripts/run_chen_strict_scale_suite.py --out-dir artifacts/chen_strict_scale_suite
```
