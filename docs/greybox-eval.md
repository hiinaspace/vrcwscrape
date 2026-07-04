# Greybox eval (Stage G2) — verdict: **GO**

Eval date: 2026-07-04. Machine: **oni** (Windows 11, Unity 2022.3.22f1, VRChat
Worlds SDK 3.10.4). Consumes the committed G1 artifact
(`mapgen/artifacts/r1/greybox_mesh/greybox.obj`, 379k tris, 25 m/unit,
~5.0 × 3.15 km) imported into the `vrchatworld2026` VRChat world project.

This was a **feasibility-focused** pass — the goal was the GO / NO-GO gate from
[greybox-plan.md](greybox-plan.md), not the full structured street-level aesthetic
audit. The core hunch (a walkable citygen world at this scale is viable in VRChat)
is **validated**. The detailed fabric-quality audit is deliberately left as
follow-up work (see "Not yet assessed" below).

## Bottom line

**GO** — continue product waves toward the 3D world. The remaining work is
geometry *quality* (roads / terrain / buildings), not feasibility. The 2D-site
fallback (Track W) is no longer the primary direction.

## Findings, against the plan's checklist

1. **Scale / locomotion.** Physical scale at 25 m/unit reads fine; not re-tuned
   this pass. Traversing the island on foot is too slow to be tolerable as-is (as
   the plan anticipated) — the intended fix is **personal vehicles + some form of
   "public transit"** (teleport/transit network), which is expected both to work
   and to *sell* the city aesthetic rather than fight it. Not built yet; noted as
   the locomotion direction.

2. **Hierarchy legibility at eye level.** *Not formally assessed this pass* — flew
   around for feasibility; no structured cores-vs-neighborhoods / arterial-vs-local
   legibility audit. Deferred.

3. **Performance.** 379k tris is trivial on modern GPUs — for reference, VRChat
   avatars are nominally < 70k each and routinely exceed that. Frustum culling +
   the per-macro-block grouping (cull groups) help as designed. Judged to have
   **ample headroom even for future non-greybox assets**: real terrain, spline
   roads, more detailed buildings, and 2D canvases for text labels. No FPS/draw-call
   numbers captured; perf was clearly not the binding constraint.

4. **What looks broken (aesthetic).** The Voronoi-derived roads / terrain /
   buildings are the weak point: "plenty of work to get them more pleasing than the
   Voronoi versions." This is the known fabric-quality gap; a ranked street-level
   defect list was **not** captured this pass.

5. **Floating-origin precision (new finding, not in the checklist).** Unity/VRChat
   render in world space, so float precision practically bounds a world to ~10 km
   from origin. At 25 m/unit the island (~5 × 3.15 km) is within budget, but any
   scale-up or expansion would approach it. Workaround: floating-origin shifting
   (e.g. **JetSim FloatingOrigin**, as used by VRChat flight-sim worlds). Flagged
   for if/when the world grows.

## Bonus capability validated: in-world title search (Udon)

Separate spike, worlds SDK / UdonSharp: **in-memory substring search over world
titles, entirely client-side — no server query.** Modeled on `subsonic-udon`
(struct-of-arrays + precomputed lowercased titles + `String.Contains`).

- **12,311 greybox worlds:** ~6 ms per search.
- **All 217,998 titled worlds (~15 MB embedded TextAsset):** ~105 ms per search
  (linear scaling held), one-time parse of a few hundred ms at load — both well
  under Udon's ~10 s execution watchdog.
- VRChat text inputs are submit-only anyway, so per-keystroke latency is moot;
  search-on-submit is the right model. Frame-chunking is available if ever needed.

Conclusion: a **"world locator"** (search a title → later teleport to its building)
is feasible without the server-side query hacks. Assets live in the world project:
`vrchatworld2026/Assets/WorldSearch/` (`WorldSearch.cs` + `WorldSearchData*.txt` +
README). Data provenance: titles come from `mapgen/data/worlds_search.parquet`
joined on `world_id` (the mapgen pipeline / `labels.csv` never carried per-world
titles — see the memory note / that folder's README).

## Backlog reorder (the gate's output)

The GO verdict reprioritizes the mapgen backlog toward **geometry aesthetic
quality** — the roads/terrain/buildings that read poorly in-engine — ahead of the
previous 2D-PNG-driven ordering (interior-fabric gap, residual guidance fans,
wedge regularization, betweenness road-width promotion). Locomotion/transit and
floating-origin are known infra items for later, owned on the VRChat side. Track W
(2D site) drops to fallback.
