# Chen/Yang Strict Reimplementation Contract

Last updated: 2026-06-07.

## Mission

Build a focused, practical reimplementation of Chen et al. 2024,
"Hierarchical Co-generation of Parcels and Streets in Urban Modeling", for the
mid-level parcel/street layout problem. Where Chen relies on earlier urban
pattern machinery, use Yang et al. 2013 and its supplements as the primary guide
for cross fields, streamline splitting, templates, and post-topology
optimization.

This is intentionally separated from the larger VRChat world city-generation
pipeline. The immediate target is a reusable local layout generator that behaves
like the papers on controlled square, oval, and triangle boundaries before it is
used inside larger region generation.

## Non-Goals

- Do not optimize only for the current metric suite if the output still visibly
  misses paper-level behavior.
- Do not hide paper-fidelity gaps behind new names, smoothing passes, or
  artifact presentation changes.
- Do not spend this phase on the full DR-world dataset unless the local
  paper-style shapes already pass the contract below.
- Do not install third-party skills, plugins, hooks, or MCP servers to solve this
  workflow problem. Repo-local docs and code are the trusted control surface for
  now.

## Source Hierarchy

Primary paper sources live under `artifacts_full/literature/`:

- `chen2024_urban_modeling.pdf`
- `chen2024_urban_modeling.txt`
- `yang2013_urban_pattern.pdf`
- `yang2013_urban_pattern.txt`
- `yang2013_supp1.pdf`
- `yang2013_supp1.txt`
- `yang2013_supp2.pdf`
- `yang2013_supp2.txt`
- `bouaziz-2012.pdf` (Shape-Up / ShapeOp-style projection solver reference
  for the optimizer track)

If extracted text is ambiguous, incomplete, or missing equations/figures, inspect
the PDF directly or call out the need for a heavier extraction/OCR pass. Do not
guess paper details from the current implementation.

Secondary project notes:

- [docs/road-layout-research.md](road-layout-research.md)
- [artifacts_full/literature/literature_audit.md](../artifacts_full/literature/literature_audit.md)

Workflow references checked on 2026-06-06:

- OpenAI Codex manual: `AGENTS.md` for durable repo rules, subagents for context
  isolation, worktrees for parallel write isolation, skills for reusable
  workflows, hooks for deterministic lifecycle checks, and goals for long-running
  objective tracking. See `https://developers.openai.com/codex/codex-manual.md`.
- Claude Code docs: `CLAUDE.md`/rules for persistent instructions, skills for
  on-demand workflows, subagents for noisy side work, and settings/plugins/hooks
  as executable-adjacent trust boundaries. See `https://code.claude.com/docs/`.
- Cline and Aider docs reinforce the same split: concise always-loaded rules,
  detailed project memory/reference docs, and explicit convention files loaded
  read-only. See `https://docs.cline.bot/` and
  `https://aider.chat/docs/usage/conventions.html`.

## Definition Of Done

A Chen/Yang implementation wave is done only when all of these are true:

- The intended paper subsystem has tests and artifact diagnostics that would
  catch the visual or algorithmic failure it was meant to fix.
- Standard square, oval, and triangle artifacts exist and render the relevant
  behavior clearly.
- The 12/24/48/96 parcel scale sweep exists for square, oval, and triangle when
  the change affects generation behavior across scale.
- `uv run ruff check .`, `uv run ty check`, and the focused Chen tests have been
  run from `mapgen/`, or any skipped command is reported with the concrete
  reason.
- Remaining paper deltas are recorded in the Replication Debt Ledger as
  `Blocking`, `Non-blocking`, or `Deferred`.
- A wave must not stop with an unresolved `Blocking` item unless the next worker
  prompt is already recorded or the user has explicitly accepted the deferral.

## Current Implementation State

Current modules:

- `mapgen/src/mapgen/chen_core.py`: strict mesh/layout data structures,
  invariants, Chen irregularity metrics, and topology helpers.
- `mapgen/src/mapgen/chen_field.py`: deterministic streamline candidates with
  the default grid-smoothed 4-RoSy approximation, an opt-in `yang_d_field` path,
  and an opt-in `yang_b_field` path. The Yang paths use retained mesh-vertex
  seeds, two active orientations, Yang-style same-orientation suppression, and
  approximate `DIV/DB/DS/CT` score diagnostics; the B-field also reports
  boundary-alignment weight, smoothness, anchor, residual, and approximation
  diagnostics.
- `mapgen/src/mapgen/chen_streets.py`: connected street subgraph selection with
  I/L access repair, Dijkstra connection, bounded junction completion, and
  optional cul-de-sac repair.
- `mapgen/src/mapgen/chen_optimize.py`: bounded ShapeOp-like projection pass with
  Chen Section 5 inspired energies, topology preservation checks, a local
  regular-polygon projection for parcel corner rings, and an opt-in boundary
  collinear slide constraint mode.
- `mapgen/src/mapgen/chen_generate.py`: shape presets, generator integration,
  generation-level accepted split-line provenance diagnostics for unexplained
  interior T nodes, and opt-in `streamline_mode="yang_d_field_candidates"` /
  `streamline_mode="yang_b_field_candidates"` ablation paths. Exact rectangles
  now use the same streamline candidate path as other boundaries.
- `mapgen/src/mapgen/chen_artifacts.py`: SVG/PNG/JSON artifact export and
  review metrics.

Current artifact scripts:

- `mapgen/scripts/run_chen_strict_shapes.py`
- `mapgen/scripts/run_chen_strict_scale_suite.py`

Known artifact locations:

- `mapgen/artifacts/chen_strict_shapes/manifest.json`
- `mapgen/artifacts/chen_strict_scale_suite/manifest.json`
- `mapgen/artifacts/chen_strict_shapes_yang_d_field_candidates/manifest.json`
- `mapgen/artifacts/chen_strict_scale_suite_yang_d_field_candidates/manifest.json`
- `mapgen/artifacts/chen_strict_shapes_yang_b_field_candidates/manifest.json`
- `mapgen/artifacts/chen_strict_scale_suite_yang_b_field_candidates/manifest.json`

Current stage labels used in artifacts include:

- `chen_grid_smooth_streamline_bounded_junction_shapeop_like_v1`
- `chen_yang_d_field_streamline_continuity_cleanup_bounded_junction_shapeop_like_v1`
- `chen_yang_b_field_streamline_endpoint_continuation_bounded_junction_shapeop_like_v1`
- `grid_smooth_4rosy_laplace_v1`
- `yang_d_field_weighted_footpoint_v1`
- `yang_b_field_boundary_laplacian_omega_v0`
- `yang_d_field_mesh_vertex_seeds_div_db_ds_ct_scoring_v0`
- `yang_b_field_mesh_vertex_seeds_div_db_ds_ct_scoring_uniform_clipped_mesh_laplacian_omega_boundary_alignment_v0`
- `chen_section_4_2_reachability_bounded_junctions_v0`
- `chen_section_5_shapeop_like_projection_v1`

These labels are useful because they make clear that the implementation is
paper-guided but not yet a full replication.

Important scope caveat: exact axis-aligned rectangles no longer bypass
streamline candidate generation. Square controls stay mostly grid-aligned, but
rectangular T-junction cleanup and optimizer fidelity remain visible paper
replication debt instead of being hidden behind a factor-grid oracle.

## Replication Debt Ledger

| Area | Status | Evidence | Priority | Next action |
| --- | --- | --- | --- | --- |
| Yang D-field and B-field construction | Partial | `yang_d_field` is opt-in with weighted footpoints; `yang_b_field_candidates` is opt-in with a Yang Supp. 1 Sec. 4.3-style boundary Laplacian solve and omega diagnostics. Both still use clipped SciPy Delaunay/uniform graph approximations, not true constrained Delaunay/Steiner/cotan construction. | Blocking | Improve D/B-field mesh, footpoint, anchor-vector, and discretization fidelity. |
| Yang candidate streamline generation | Partial | Opt-in `yang_mesh_vertices` seeds, two active orientations, same-orientation suppression, and approximate `DIV/DB/DS/CT` scoring exist. Defaults remain grid/heuristic. | Blocking | Tighten exact Yang scoring terms and template-width coupling; keep approximation labels honest. |
| Curved streamline split persistence | Partial | Curved candidate lines exist, but downstream mesh split/topology can still flatten or simplify behavior too aggressively. | Blocking | Preserve curve intent through splitting, street graph, and artifact metrics; add tests for oval boundary-following segments. |
| Chen split topology | Partial | Exact axis-aligned rectangles now use the same streamline candidate path as other shapes. Square controls remain axis-aligned, but rectangular sequential splits still expose T-junction cleanup debt and wider rectangles can still show cleanup/optimizer axis-deviation artifacts. Fresh D/B scale sweeps pass geometry, paper invariants, and reachability. Non-rectangular unexplained T nodes are currently straight-through and attributed to accepted split geometry, but Fig. 7 cleanup still has failed candidates. Boundary-projected merges and full-mesh-ring retention are labeled approximations. | Blocking | Reduce `failed_non_simple_ring_after_merge`, `failed_candidate_pair_still_adjacent_due_other_shared_edges`, and rectangle cleanup axis-deviation artifacts with local, paper-faithful topology operations or preserve them as explicit debt. |
| Chen Section 4.2 street generation | Partial | I/L repair plus Dijkstra exists, but mode names still record bounded approximation. | Non-blocking until field/split debt clears | Compare exact candidate enumeration and cul-de-sac behavior against paper text. |
| Yang template system | Missing | No semantic template model with region labels, boundary semantics, vertex semantics, and warp constraints. | Blocking for full Yang replication; deferrable for Chen-only experiments | Add minimal row/spike/area template data model after field and split topology are stable. |
| Global optimization | Partial | ShapeOp-like projection tracks Chen Section 5 energies, has an opt-in boundary collinear slide constraint, and now emits local regular-polygon projection equations for parcel corner rings with scale-normalized weights and no-flip orientation guards. It is still not exact Chen ShapeOp or Yang alternating strip-fit/road-mesh optimization. EPFL ShapeOp is open source and useful as a source reference; direct original SWIG bindings look too stale for default use, while `compas_shapeop` is available as an optional Python 3.12 probe. | Blocking for final replication | Source-audit exact Chen/Yang objective terms against Bouaziz 2012 / ShapeOp source, then compare the local projection solver against a bounded `compas_shapeop` backend/ablation. |
| Paper-figure calibration | Missing | No figure-specific parameter table or OCR audit for hard-to-extract equations/diagrams. | Non-blocking but important | Make a source audit artifact listing paper sections, extracted text quality, and any OCR needs. |
| Visual evaluation | Partial | Baseline, opt-in Yang D-field, and opt-in Yang B-field square/oval/triangle paths exist. Artifact summaries expose rectangular diagnostics, Yang score/B-field diagnostics, Fig. 7 short-edge diagnostics, accepted split-line provenance, labeled approximation counters, and acceptance-path counters. D-field gives more organic curved structure; B-field is cleaner on the oval but can form strong bands on the triangle. Reviewer-visible acceptance criteria are still informal. | Blocking | Add targeted metrics for curved boundary-following quality and remaining failed Fig. 7 cleanup. |

## Active Wave

Completed in the 2026-06-06 workflow wave:

- Added durable `AGENTS.md`, `mapgen/AGENTS.md`, `CLAUDE.md`, and this contract
  doc so high-level paper-fidelity intent survives compaction and subagent
  handoffs.
- Added rectangular axis-deviation and interior/boundary junction diagnostics,
  then fixed exact rectangles with a labeled factor-grid continuity path.
- Source-audited Yang 2013 field/streamline references and recorded extraction
  caveats for the supplemental B/H-field equations.
- Added opt-in Yang D-field mesh-vertex seeding, same-orientation suppression,
  approximate `DIV/DB/DS/CT` scoring, generation wiring, artifact summaries, and
  shape/scale artifact scripts for `--streamline-mode yang_d_field_candidates`.
- Added opt-in Yang B-field construction and
  `--streamline-mode yang_b_field_candidates`, with omega diagnostics and
  acceptance-path counters surfaced in artifact summaries and scale-suite
  review/aggregate metrics.
- Added generation-level provenance metrics for unexplained interior T nodes:
  endpoint on accepted split line, interior on accepted split line, and
  unknown/unattributed, with source-count aggregation. This is geometric
  lineage evidence, not proof of paper-invalid topology.
- Added Fig. 7 failure-detail diagnostics, failed-sample bucketing, labeled
  boundary-projected merge approximations, and a full-mesh-ring retention guard
  for degenerate corner-loss cases.
- Added an opt-in optimizer boundary collinear slide constraint mode.

Verification notes from this wave:

- `uv --project . run --with pytest python -m pytest tests/test_chen_*.py`
  passed before the final rectangle stage-label patch: `97 passed`.
- After the final stage-label patch,
  `uv --project . run --with pytest python -m pytest tests/test_chen_generate.py tests/test_chen_artifacts.py tests/test_chen_scale_suite.py`
  passed: `33 passed`.
- Scoped `ruff check` and `ty check` over touched Chen source/scripts passed.
- Provenance slice verification:
  `uv --project . run --with pytest python -m pytest tests/test_chen_generate.py tests/test_chen_artifacts.py tests/test_chen_scale_suite.py`
  passed: `42 passed`. Scoped `ruff check`, `ruff format --check` after
  formatting, and source/script `ty check` passed.
- Tiny 24-parcel oval+triangle probes:
  `yang_d_field_candidates` attributed `27/27` unexplained T nodes to split
  endpoints, with zero unknowns; `yang_b_field_candidates` attributed `19/20`
  to endpoints and `1/20` to a split-line interior, with zero unknowns. Both
  probes had `2/2` geometry and paper-invariant passes.
- Latest focused verification after Fig. 7 diagnostics/guardrails:
  scoped `ruff check` passed, scoped `ty check` passed, and
  `uv --project . run --with pytest python -m pytest tests/test_chen_core.py tests/test_chen_generate.py tests/test_chen_artifacts.py tests/test_chen_scale_suite.py tests/test_chen_optimize.py`
  passed with `86 passed`.
- Regenerated artifacts:
  `mapgen/artifacts/chen_strict_shapes_yang_d_field_candidates`,
  `mapgen/artifacts/chen_strict_shapes_yang_b_field_candidates`,
  `mapgen/artifacts/chen_strict_scale_suite_yang_d_field_candidates`, and
  `mapgen/artifacts/chen_strict_scale_suite_yang_b_field_candidates`.
- Latest D-field scale sweep: `12/12` geometry, paper-invariant, and
  reachability passes; `293` curved splits; `4/12` optimizer geometry-changed
  runs; `4790` regularity projection equations in aggregate; max regularity
  residual stayed `4.76541033195781 -> 4.76541033195781`; `62` Fig. 7
  cleanups applied;
  `125` Fig. 7 cleanup failed attempts but only `27` unique failed candidates
  (`98` duplicate attempts). Unique failure detail counts are `21`
  due-other-shared-edge, `4` conforming-graph, and `2` motif-ineligible
  non-candidate parcel-ring candidates; generic non-simple merge-ring failures
  are now `0`. There are `229` unexplained interior T nodes, all
  straight-through, with `0` kinked and `0` unknown provenance.
- Latest B-field scale sweep: `12/12` geometry, paper-invariant, and
  reachability passes; `316` curved splits; `5/12` optimizer geometry-changed
  runs; `4778` regularity projection equations in aggregate; max regularity
  residual improved slightly from `6.725843552283696` to
  `6.721843098444956`; `80` Fig. 7 cleanups applied;
  `261` Fig. 7 cleanup failed attempts but only `41` unique failed candidates
  (`220` duplicate attempts). Unique failure detail counts are `19`
  due-other-shared-edge, `6` conforming-graph, `13` motif-ineligible
  non-candidate parcel-ring candidates, and `3` remaining generic non-simple
  merge-ring candidates. There are `203` unexplained interior T nodes, all
  straight-through, with `0` kinked and `0` unknown provenance.
- Standard 48-parcel visual check: squares remain clean controls; D-field oval
  and triangle show curved subdivision but visible local cleanup roughness;
  B-field oval is more rectilinear and clean, while B-field triangle shows
  strong long-band behavior that may be a field/scoring/template gap rather
  than a topology bug.
- Review-agent conclusion: the Fig. 7 cleanup failures should be treated as
  real replication debt for now. `failed_non_simple_ring_after_merge` is likely
  either a too-crude local merge operation or an over-broad candidate detector.
  `failed_candidate_pair_still_adjacent_due_other_shared_edges` is ambiguous,
  but still blocking until samples distinguish duplicate/fragmented shared
  edges from genuinely disconnected multi-edge parcel adjacency.
- Source-audit conclusion: Chen Section 4.1 / Fig. 7 calls for graph-local
  contraction plus an explicit short interpolation segment between the
  misaligned partitioning lines. The current implementation's whole-ring
  midpoint substitution is an approximation and can create bowties where a
  half-edge/DCEL-style local rewrite might remain valid. Yang 2013 supports the
  general "collapse short edges, smooth later" idea but does not define Chen's
  parcel-pair semantics.
- Strongest Fig. 7 regression targets: B-field triangle 24/48/96 parcels for
  non-simple failures (`5/19/81`), B-field triangle 48/96 and oval 96 for
  due-other-shared-edge failures (`20/66` and `20`), and D-field triangle 96
  for due-other-shared-edge failures (`48`).
- After unique-candidate grouping, prioritize representative unique candidates
  over raw failed-attempt counts. Useful targets include B-field triangle 24
  (`7` attempts, `2` unique; one non-simple unique candidate at midpoint
  `[62811361, 77576381]`), B-field triangle 48 (`42` attempts, `10` unique),
  B-field triangle 96 (`160` attempts, `15` unique), and D-field triangle 96
  (`70` attempts, `12` unique).
- Debug-agent conclusion for the B-field triangle 24 non-simple sample: the
  self-intersecting parcel is parcel `24`, not either candidate-pair parcel
  `[10, 22]`. Naive replacement changes the local ring segment
  `213 -> 89 -> 211 -> 210 -> 209` into
  `213 -> midpoint -> 211 -> 210 -> 209`, crossing the later
  `210 -> 209` arm. The candidate pair also shares adjacent edge `(86, 88)`;
  collapsing that connected chain still self-intersects parcel `24`, so this is
  not fixed by simply broadening the replaced-node set.
- Added diagnostic failure detail
  `failed_fig7_motif_ineligible_non_candidate_parcel_ring_after_merge` for
  cases where whole-ring midpoint substitution invalidates a non-candidate
  parcel touched by the candidate path. This is still counted as Fig. 7 failure
  debt; it does not hide the failure or repair topology. In the latest B-field
  triangle sweep it accounts for `5/7`, `8/42`, and `81/160` failed attempts at
  24/48/96 parcels respectively.
- ShapeOp investigation: original EPFL ShapeOp exposes relevant constraints
  (`Closeness`, `Line`, `Rectangle`, `Parallelogram`, `Laplacian`, `Angle`,
  `EdgeStrain`) and is a strong reference for Chen Section 5. Treat the original
  repo as reference-only for now because the SWIG/Python path is 2014-era and
  release-light. `compas_shapeop` has modern Python wheels and should be probed
  as an optional backend, but may expose only a subset of constraints.
- Optional ShapeOp dependency status: `mapgen` now has a `shapeop` optional
  extra with `compas` and `compas-shapeop`. `uv run --extra shapeop` imports
  `compas_shapeop` `0.1.2`; `MeshSolver` exposes closeness, edge strain, plane,
  circle, similarity, regular polygon, and generic shape constraints, and a tiny
  flat 5-vertex mesh probe preserved `z=0` and topology counts. This is viable
  for a later optional backend ablation, but not yet the default optimizer.
- Implemented the Chen Section 5 / Bouaziz-style local parcel regularity
  projection in `chen_optimize.py`. It fits the closest regular `N`-gon to each
  parcel corner ring with deterministic cyclic/reversed template trials, rejects
  orientation-flipped fits, scale-normalizes equation weights by
  `regularity / (N * mean_side_length^2)`, and exposes projected/skipped parcel,
  residual, equation-count, and target-displacement diagnostics.
- Fixed the oval preset boundary sampler so all intermediate vertices lie on the
  ellipse arc instead of being linear interpolants inside the anchor chord. The
  old sampler introduced a boundary-level zigzag before optimization or street
  selection. `oval_boundary_*radial_error*` diagnostics now expose this contour
  fidelity in generated metrics and artifact summaries.
- Added a rectangular axis-preservation guard around Fig. 7 cleanup. Exact
  rectangles still use the streamline candidate path, but cleanup attempts that
  would introduce diagonal mesh/street artifacts are rejected and reported via
  `chen_fig7_short_edge_cleanup_rectangular_axis_guard_*` diagnostics.
- Improved Fig. 7 cleanup with a scoped candidate-pair contraction fallback for
  cases where whole-ring midpoint substitution invalidates a non-candidate
  parcel ring. Candidate-pair adjacency through other shared edges is now only
  tolerated when the remaining shared edges are contiguous with the same local
  shared-boundary chain; disconnected multi-edge adjacency remains explicit
  cleanup debt.
- Added `CHEN_SECTION5_STRICT_OPTIMIZATION_CONFIG` for ablations with Chen terms
  enabled and extra stabilizers disabled, while the generation default remains a
  more pragmatic bounded optimizer config.
- Promoted optimizer regularity diagnostics to generated metrics, artifact
  summaries, scale-suite review metrics, and scale-suite aggregates under
  `optimization_regularity_*`.
- Added `pytest` to the `mapgen` dev dependency group so focused verification
  can run with `uv run python -m pytest ...` instead of transient
  `uv run --with pytest ...`.
- Latest focused verification after optimizer regularity and artifact-summary
  wiring: strict-slice `ruff check`, `ruff format --check`, and source/script
  `ty check` passed; `uv run python -m pytest tests/test_chen_core.py
  tests/test_chen_generate.py tests/test_chen_artifacts.py
  tests/test_chen_scale_suite.py tests/test_chen_optimize.py
  tests/test_chen_field.py tests/test_chen_streets.py` passed with
  `127 passed`.
- Test-runtime workflow pass: a measured full focused Chen run took
  `349.32s` for `134 passed`; generation-heavy shape, Yang, Fig. 7, artifact,
  and legacy proto regressions now carry the existing `slow` marker. Use
  `uv run python -m pytest tests/test_chen_*.py -m "not slow"` for tight loops;
  latest quick-lane verification passed with `101 passed, 35 deselected` in
  `17.91s`. Continue to run the full focused suite before presenting
  paper-fidelity changes or regenerating final visual artifacts.
- Latest Fig. 7 topology verification: focused Fig. 7 core tests passed with
  `12 passed`; quick Chen lane passed with `101 passed, 35 deselected` in
  `17.91s`; scoped `ruff check`, `ruff format --check`, and `ty check` passed.
  The B-field triangle 24 fixture moved from `5` applied cleanups and `2`
  unique failed candidates (`due_other_shared_edges` plus `motif_ineligible`) to
  `6` applied cleanups and `1` unique failed candidate, now blocked by strict
  boundary coverage. B-field triangle 48 moved from `15` applied cleanups and
  `10` unique failed candidates to `24` applied cleanups and `5` unique failed
  candidates, with a new `failed_kinked_t_junction_regression` guard preventing
  cleanup attempts that would create unexplained kinked T-junction debt.
- Repo-wide `ruff check .` and `ty check` are not yet clean because of
  pre-existing unrelated issues outside this Chen slice; do not misread those as
  regressions without checking the concrete failure paths.

Next wave focus:

1. Implement coverage-preserving Fig. 7 local interpolation for the remaining
   B-field triangle 24 boundary-coverage failure. The scoped contraction
   fallback removes the previous motif-ineligible/non-candidate-ring failure,
   but the final candidate still creates a tiny boundary coverage gap and is
   correctly rejected.
2. Reduce the remaining B-field triangle 48 Fig. 7 failures: two boundary
   coverage failures, one conforming-graph failure, one non-simple merge
   failure, and one kinked-T-regression rejection. Keep disconnected multi-edge
   adjacency rejected unless a source audit proves the edges are one contiguous
   local chain.
3. Regenerate D/B shape and scale sweeps after the next Fig. 7 topology patch
   and compare unique failed-candidate counts, rectangular axis guard behavior,
   geometry validity, and unexplained kinked T-junction counts.
4. Tighten exact Yang field/scoring/template fidelity after the split-topology
   debt has a conforming cleanup path.
5. For the optimizer track, compare the local projection solver against
   `bouaziz-2012.pdf`, ShapeOp source/docs, and a bounded `compas_shapeop`
   backend/ablation. The local regularity projection is now implemented; the
   remaining question is exact objective/operator fidelity and whether the
   current acceptance/backtracking is over-constraining generated layouts.

The main thread should spawn bounded subagents for source audit, implementation,
and review. It should integrate results and decide whether a worker's remaining
gap is a blocker, not let the worker declare the larger project done.

## Subagent Workflow Contract

Use the main thread for:

- Maintaining this contract and the active wave.
- Assigning bounded slices with disjoint write scopes where practical.
- Reviewing subagent summaries for paper-fidelity gaps.
- Running or delegating final integrated verification.

Use implementation subagents for:

- One subsystem at a time: field, split topology, street generation, optimizer,
  artifacts, or tests.
- Direct code edits only inside the assigned write scope.
- Focused tests that catch the paper-fidelity issue for that slice.

Use review/source-audit subagents for:

- Reading papers, extracted text, and current code.
- Reporting exact sections, equations, figures, and extraction gaps.
- Critiquing whether the implementation is a faithful approximation or a metric
  workaround.

Subagent completion reports must include:

- Files changed.
- Commands run and results.
- Paper-fidelity improvement.
- Remaining replication debt, classified as `Blocking`, `Non-blocking`, or
  `Deferred`.
- Exact next worker prompt if a blocking gap remains.

## Standard Worker Prompt Template

```text
You are a focused implementation/review subagent for the Chen 2024/Yang 2013
strict reimplementation in /home/s/code/vrcwscrape.

Before working, read:
- AGENTS.md
- mapgen/AGENTS.md
- docs/chen-strict-reimplementation.md
- the relevant chen_* source/test files for your slice

High-level objective:
Replicate Chen 2024 mid-level parcel/street co-generation as faithfully as
practical, including Yang 2013 field machinery where Chen depends on it. Do not
stop at "tests pass" if the paper-fidelity gap in your slice remains material.

Your slice:
[FIELD / SPLIT TOPOLOGY / STREETS / OPTIMIZER / ARTIFACTS / REVIEW / SOURCE AUDIT]

Rules:
- Keep changes scoped to your slice.
- Preserve or improve artifact/metric visibility.
- Add tests that catch the visual/paper-fidelity failure your slice addresses.
- Do not Goodhart on existing metrics; if metrics are insufficient, add
  diagnostics/tests.
- Report remaining paper delta as Blocking / Non-blocking / Deferred.
- If a remaining gap should trigger a follow-up worker, provide the exact next
  worker prompt.

Return:
- Files changed
- Tests/commands run and results
- Paper-fidelity improvement
- Remaining replication debt
- Suggested next worker, if any
```

## Security And Extension Policy

- Do not download or install third-party skills, plugins, hook packs, or MCP
  servers for this project unless the user explicitly approves after review.
- Treat skills/plugins as executable-adjacent: they may contain scripts, hooks,
  MCP server config, or bundled instructions that can alter agent behavior.
- Prefer repo-local docs and, later, repo-local skills that we write ourselves.
- If hooks are added later, keep them deterministic and repo-owned. A reasonable
  first hook would be a reminder on `Stop` or `PreCompact` when the Active Wave
  or Replication Debt Ledger has not been updated, not an auto-fixer.

## Verification Commands

From `mapgen/`:

```bash
uv run ruff check .
uv run ty check
uv run pytest tests/test_chen_*.py
uv run python scripts/run_chen_strict_shapes.py
uv run python scripts/run_chen_strict_scale_suite.py
```
