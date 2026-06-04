# Road Layout Research Notes

Date: 2026-06-04

This note summarizes a research pass for replacing the current road-parcel
layout generator. The immediate problem is that the v2 generator makes roads
and then places independent rows of parcel squares along each segment. That can
look evocative, but it breaks down at intersections, near-parallel roads, and
triangular road loops. Collision relaxation then scrambles positions without
solving the underlying layout problem.

Local paper cache for this pass: `/tmp/vrcw-road-papers`.

## Current Failure Mode

The current pass is architecturally inverted:

- Roads are generated as segment geometry first, largely from local cluster
  centers and graph edges.
- Worlds are assigned to the nearest segment and laid out as independent grids.
- Minor/service lanes and collision relaxation are added after the fact.

This creates predictable artifacts:

- Grids from adjacent road segments overlap at intersections and corners.
- Parallel or near-parallel roads create competing parcel rows in the same
  physical space.
- Triangular Delaunay/MST-like regions force three incompatible grids into one
  neighborhood.
- Parcel relaxation optimizes local collisions, not semantic position, so it can
  scramble the DR neighborhood structure.
- Road frontage is not guaranteed for every world, which matters if this ever
  becomes a walkable/drivable VRChat environment.

The next generator should make a planar street/block graph first, then create
lots inside those blocks, then assign worlds to lots.

## Sources Reviewed

- Parish and Mueller 2001, "Procedural Modeling of Cities":
  https://people.eecs.berkeley.edu/~sequin/CS285/PAPERS/Parish_Muller01.pdf
  introduced global goals plus local constraints for road growth from density,
  water, and pattern maps. Useful mainly as historical context.

- Chen et al. 2008, "Interactive Procedural Street Modeling":
  https://www.peterwonka.net/Publications/pdfs/2008.SG.Chen.InteractiveProceduralStreetModeling.pdf
  uses tensor fields to guide street-network streamlines. This is directly
  relevant for replacing triangular graph layouts with locally coherent street
  directions.

- Galin et al. 2010, "Procedural Generation of Roads":
  https://perso.liris.cnrs.fr/eric.galin/Articles/2010-roads.pdf
  generates roads as weighted anisotropic shortest paths. Useful for bridges,
  arterial roads, and connecting island/neighborhood centers without hard MST
  geometry.

- Galin et al. 2011, "Authoring Hierarchical Road Networks":
  https://www.cs.purdue.edu/cgvlab/www/resources/papers/Galin-Computer_Graphics_Forum-2011-Authoring_Hierarchical_Road_Networks.pdf
  extends the shortest-path idea to hierarchical networks with merging and
  branching. Relevant to separating arterials, collectors, and local streets.

- Weber et al. 2009, "Interactive Geometric Simulation of 4D Cities":
  https://www.peterwonka.net/Publications/pdfs/2009.EG.Weber.UrbanSimulation.FinalVersion.pdf
  treats streets, exact parcel boundaries, arbitrary street widths, building
  footprints, and 3D envelopes as geometric entities rather than a regular grid.
  This lines up with our possible 3D/instanced-building direction.

- Lipp et al. 2011, "Interactive Modeling of City Layouts using Layers of
  Procedural Content":
  https://cg.tuwien.ac.at/research/publications/2011/lipp2011a/lipp2011a-draft.pdf
  focuses on valid, intersection-free street/parcel layouts and persistent
  anchored assignments. The practical takeaway is to keep edits/regeneration in
  a valid planar layout space.

- Aliaga et al. 2008, "Interactive Example-Based Urban Layout Synthesis":
  https://cs.purdue.edu/homes/bbenes/papers/Aliaga08ToG.pdf
  synthesizes layouts from real examples. This is useful if we later want
  neighborhood style patches instead of hand-coded grid/radial/organic rules.

- Yu and Steed 2012, "Example-Based Road Network Synthesis":
  https://diglib.eg.org/bitstreams/12c03487-0acb-457c-8ed6-ff7fad99fd68/download
  grows road networks from example road graphs and can blend styles. Useful as a
  lightweight alternative to full example-based urban layout synthesis.

- Benes et al. 2014, "Procedural Modelling of Urban Road Networks":
  https://cgg.mff.cuni.cz/wp-content/uploads/2021/05/ProceduralModellingOfUrbanRoadNetworks.pdf
  grows several settlements together and emphasizes procedural controls. Useful
  for multi-island/multi-center growth behavior.

- Yang et al. 2013, "Urban Pattern: Layout Design by Hierarchical Domain
  Splitting":
  https://peterwonka.net/Publications/pdfs/2013.SGA.Yongliang.UrbanPattern.pdf
  hierarchically splits polygonal regions using cross-field streamlines and
  warped templates. This is one of the strongest matches for non-triangular
  neighborhoods with coherent local grids.

- Vanegas et al. 2012, "Procedural Generation of Parcels in Urban Modeling":
  https://www.cs.purdue.edu/cgvlab/www/resources/papers/Vanegas-Eurographics-2012-Procedural_Generation_of_Parcels_in_Urban_Modeling.pdf
  focuses on subdividing road-bounded blocks into plausible parcels. This is
  directly relevant once the road graph has been polygonized into blocks.

- Peng et al. 2016, "Computational Network Design from Functional
  Specifications":
  https://geometry.cs.ucl.ac.uk/group_website/projects/2016/functional-network-design/paper_docs/PengEtAl_FuncNetworkDesign_SIGG16.pdf
  generates networks from high-level constraints such as density, travel time,
  destinations, dead-end policy, and local features. This is useful for avoiding
  branchy/triangular graphs and tuning for loops versus cul-de-sacs.

- Nishida et al. 2016, "Example-Driven Procedural Urban Roads":
  https://www.cs.purdue.edu/cgvlab/www/resources/papers/Nishida-CGF-2016-Example_Driven_Procedual_Urban_Roads.pdf
  combines example patches with procedural growth. This could provide natural
  road styles while still fitting a target island mask.

- Chen et al. 2024, "Hierarchical Co-generation of Parcels and Streets in Urban
  Modeling":
  https://sutd-cgl.github.io/supp/Publication/papers/2024-EG-UrbanModeling.pdf
  is the most directly relevant recent paper. It explicitly argues that streets
  and parcels are coupled, then hierarchically splits land while adding streets
  to ensure every parcel has street access. This matches our current failure
  almost exactly.

- Jiang and Claramunt 2004/2007, "A Topological Pattern of Urban Street
  Networks":
  https://arxiv.org/pdf/physics/0703223
  is useful for validation metrics: degree distribution, hierarchy, connected
  components, and cycles.

## Design Direction

The replacement should be an offline, deterministic `road-layout-v3` pipeline:

1. Rasterize density and island masks from the no-labs LocalMAP coordinates.
   Keep the original DR points as the ground-truth data term; the road layout is
   a geometric presentation layer, not a new embedding.

2. Build a smoothed local orientation/cross field. Seed it from kNN PCA on the
   original DR points, island boundaries, and optional region/subregion
   orientation. This field guides local streets and block splits.

3. Generate major roads per island. Pick centers from density peaks and existing
   island/subregion structure, then connect them using least-cost paths over the
   density/land field. Costs should prefer land, density ridges, smooth curves,
   and short bridges only when they are visually intentional.

4. Generate collectors and local streets inside major-road-bounded areas. Use
   cross-field streamlines and domain splitting, not Delaunay triangles, for the
   main local structure. Allow template patches for grids, warped grids, rings,
   and organic neighborhoods.

5. Planarize the road graph. Snap close intersections, split crossing segments,
   remove duplicate edges, and polygonize with Shapely/GEOS. Merge sliver faces
   and reject acute triangular blocks unless intentionally marked as plazas,
   parks, or small transition blocks.

6. Subdivide blocks into lots. Use OBB/straight-skeleton/streamline subdivision
   depending on block shape. Every lot should have frontage on a road or a
   pedestrian/service lane. Vary lot width/depth mildly so the result keeps some
   of the organic character of the Voronoi view.

7. Assign worlds to lots with constrained matching. The cost should include
   distance from the original DR point to the lot center, island/region/subregion
   consistency, local rank/order preservation, and optional world-size hints.
   This replaces pairwise collision relaxation: lots cannot overlap by
   construction, and the optimizer only chooses which non-overlapping lot a world
   occupies.

8. Export instancing-friendly geometry. Suggested columns:
   `world_id`, `x`, `y`, `angle`, `width`, `depth`, `height`, `lot_id`,
   `block_id`, `road_id`, `orig_x`, `orig_y`, `layout_displacement`,
   `frontage_kind`. Roads and blocks can remain GeoJSON; lots/buildings should
   be Parquet for static range-loadable rendering.

## Implementation Sketch

Suggested new modules:

- `mapgen.road_field`: density raster, land mask, cross-field estimation, and
  streamline tracing.
- `mapgen.road_graph`: hierarchical road graph creation, least-cost routing,
  planarization, polygonization, and graph metrics.
- `mapgen.parcels`: block subdivision, lot frontage generation, sliver merging,
  and geometry quality metrics.
- `mapgen.road_assignment`: min-cost lot assignment that preserves DR locality.

Useful dependencies to consider through `uv`:

- `networkx` for graph topology and shortest-path prototyping.
- `scikit-image` for raster skeletonization, contours, medial axis, and
  distance fields.
- `scipy.optimize.linear_sum_assignment` or a min-cost-flow package for
  assignment; chunk by island/neighborhood to keep it tractable.

The first prototype should avoid a full integer-programming implementation. A
good enough v3 can likely be built from raster fields, Shapely polygonization,
shortest paths, streamline/domain splitting, and per-block matching.

## Quality Gates

Track these metrics for every export and compare against `road-parcels-v2`:

- No building/lot overlaps.
- Percent of worlds with road or service-lane frontage.
- Median, p90, and p99 displacement from original DR coordinates.
- Displacement by island and by labeled subregion.
- Spearman correlation between original local density and final building
  density.
- Road graph planarity violations after snapping/splitting.
- Degree distribution and cycle ratio; avoid pure trees and excess triangles.
- Count/area of sliver blocks and acute triangular blocks.
- Total road length per world and per island.
- Bridge length over ocean and count of bridge endpoints not attached to land.
- Frontend render cost for instanced buildings at high zoom.

## Recommended Next Prototype

Start with one or two dense LocalMAP islands:

1. Build density raster, land mask, and local cross field.
2. Generate a few major/collector roads with least-cost paths between density
   centers.
3. Fill the largest blocks using streamline or warped-grid splits.
4. Polygonize roads into blocks and subdivide blocks into lots.
5. Assign worlds to lots with a simple per-block Hungarian/matching pass.
6. Export only buildings, roads, blocks, and displacement metrics for inspection.

Only after that looks sane should the pipeline scale to all islands and replace
the current road-parcel export.
