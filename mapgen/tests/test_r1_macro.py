"""Focused unit tests for the macro hierarchy pure helpers (r1_macro)."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
import shapely.geometry as sg
from shapely.ops import unary_union

from mapgen.r1_arm_a import IslandFields
from mapgen.r1_connect import Gate, snap_gates_to_macro
from mapgen.r1_macro import (
    CITY_SIGMA,
    DEFAULT_CORE_SIMPLIFY_TOLERANCE,
    DEFAULT_R_PLAZA_MAJOR_MAX_UNITS,
    DEFAULT_R_PLAZA_MAJOR_MIN_UNITS,
    DEFAULT_SPOKE_MIN_ANGLE_DEG,
    SPOKE_NODE_SENTINEL,
    TOWN_SIGMA,
    VILLAGE_SIGMA,
    MacroEdge,
    MacroNode,
    MacroParams,
    NucleusSpec,
    _cells_to_polygon,
    _chaikin_pass_closed,
    _chaikin_pass_open,
    _dedupe_stations_by_angle,
    _delaunay_edges,
    _nearest_neighbor_chain,
    _polygonize_macro_blocks_protecting_nuclei,
    _ring_junction_stations,
    add_intra_nucleus_avenues,
    assign_nearest_nucleus,
    build_density_cost_field,
    build_macro_blocks_with_cores,
    build_macro_layer,
    build_macro_nodes_hierarchical,
    build_nucleus_avenues,
    build_nucleus_specs,
    clip_arterials_to_cores,
    cluster_centroids,
    compute_macro_arterials,
    detect_core_regions,
    grade_cities,
    major_plaza_radii,
    polygonize_macro_blocks,
    smooth_arterial_lines,
    smooth_polyline,
    snap_centroid_to_peak,
    snap_nodes_to_peaks,
    tier_for_tau,
    town_nodes_from_peaks,
    visit_weighted_centroids,
)


def test_grade_cities_keeps_top_mass_demotes_rest() -> None:
    """Top-n by mass stay sigma=2 cities; the rest become sigma=1 towns."""
    cities = [
        MacroNode(x=0.0, y=0.0, sigma=CITY_SIGMA, kind="city", label="a", mass=5.0),
        MacroNode(x=1.0, y=1.0, sigma=CITY_SIGMA, kind="city", label="b", mass=50.0),
        MacroNode(x=2.0, y=2.0, sigma=CITY_SIGMA, kind="city", label="c", mass=20.0),
    ]
    graded = grade_cities(cities, n_cities=2)
    # Order preserved; b (50) and c (20) stay cities, a (5) demoted to town.
    assert [g.kind for g in graded] == ["town", "city", "city"]
    assert [g.sigma for g in graded] == [TOWN_SIGMA, CITY_SIGMA, CITY_SIGMA]
    # Demoted node keeps its label/mass and position.
    assert graded[0].label == "a" and graded[0].mass == 5.0 and graded[0].x == 0.0
    # n_cities >= len is a no-op.
    assert grade_cities(cities, n_cities=9) == cities


def test_visit_weighted_centroid_math() -> None:
    """City centroid is the visit-weighted mean, with mass = sum(visits)."""
    points = pl.DataFrame(
        {
            "l0_id": [7, 7, 7],
            "l0_name": ["alpha", "alpha", "alpha"],
            "visits": [1, 3, 0],  # weights: the x=0 point dominates 3:1
            "x": [10.0, 0.0, 100.0],
            "y": [0.0, 4.0, 100.0],
        }
    )
    nodes = visit_weighted_centroids(points)
    assert len(nodes) == 1
    node = nodes[0]
    # weighted mean x = (10*1 + 0*3 + 100*0) / 4 = 2.5
    # weighted mean y = (0*1  + 4*3 + 100*0) / 4 = 3.0
    assert node.kind == "city"
    assert node.sigma == CITY_SIGMA
    assert node.label == "alpha"
    assert node.mass == 4.0
    assert node.x == 2.5
    assert node.y == 3.0


def test_cluster_centroids_generic_level() -> None:
    """``cluster_centroids`` works on any tier (here L1) with the given sigma/kind.

    Two L1 clusters, each visit-weighted; emitted in ascending l1_id order with
    the requested sigma=CITY_SIGMA and kind="city".
    """
    points = pl.DataFrame(
        {
            "l1_id": [3, 3, 1],
            "l1_name": ["region-b", "region-b", "region-a"],
            "visits": [1, 3, 5],
            "x": [10.0, 0.0, 7.0],
            "y": [0.0, 4.0, 9.0],
        }
    )
    nodes = cluster_centroids(points, "l1_id", "l1_name", CITY_SIGMA, "city")
    assert len(nodes) == 2
    # Ascending l1_id: id=1 first (region-a), then id=3 (region-b).
    a, b = nodes
    assert a.label == "region-a" and a.kind == "city" and a.sigma == CITY_SIGMA
    # Single-point cluster -> centroid is that point.
    assert a.x == 7.0 and a.y == 9.0 and a.mass == 5.0
    # region-b weighted: x=(10*1+0*3)/4=2.5, y=(0*1+4*3)/4=3.0, mass=4.
    assert b.label == "region-b"
    assert b.x == 2.5 and b.y == 3.0 and b.mass == 4.0
    # Same machinery as visit_weighted_centroids when pointed at L0.
    l0_points = points.rename({"l1_id": "l0_id", "l1_name": "l0_name"})
    assert cluster_centroids(
        l0_points, "l0_id", "l0_name", CITY_SIGMA, "city"
    ) == visit_weighted_centroids(l0_points)


def test_n_level_network_three_tiers_for_sigmas_012() -> None:
    """With sigmas {0,1,2} present the network builds 3 distinct tiers.

    One city (σ=2), one town (σ=1) and one village (σ=0) placed as a triangle on
    a flat cost field. Per-level construction (highest first, higher-level pairs
    excluded) must yield exactly one edge at each of tiers 2-absent... here the
    city subset is a single node (no highway), the city+town subset gives a major
    (tier 1), and the all-node subset adds the village's two local edges (tier 0).
    """
    cost = np.ones((60, 60), dtype=float)
    nodes = [
        MacroNode(x=10.0, y=10.0, sigma=CITY_SIGMA, kind="city"),
        MacroNode(x=45.0, y=10.0, sigma=TOWN_SIGMA, kind="town"),
        MacroNode(x=27.0, y=45.0, sigma=VILLAGE_SIGMA, kind="village"),
    ]
    _lines, edges = compute_macro_arterials(
        nodes, cost, x0=0.0, y0=0.0, cell=1.0, beta_ratio=np.inf
    )
    tiers = sorted(e.tier for e in edges)
    # city+town (σ>=1) Delaunay over 2 nodes -> 1 major edge (tier 1).
    # all-nodes (σ>=0) Delaunay (triangle) has 3 edges minus the realized major
    # -> 2 local edges (tier 0). City subset alone has 1 node -> no highway.
    assert tiers == [0, 0, 1]
    # tau is min(sigma) across endpoints; the major connects city(2)+town(1).
    major = next(e for e in edges if e.tier == 1)
    assert major.tau == 1
    # Each local touches the village (σ=0) so tau==0.
    assert all(e.tau == 0 for e in edges if e.tier == 0)


def test_two_tier_behavior_preserved_for_sigmas_12() -> None:
    """Sigmas {1,2} only -> legacy 2-tier split (highway over cities, major rest)."""
    cost = np.ones((60, 60), dtype=float)
    nodes = [
        MacroNode(x=10.0, y=10.0, sigma=CITY_SIGMA, kind="city"),
        MacroNode(x=45.0, y=10.0, sigma=CITY_SIGMA, kind="city"),
        MacroNode(x=27.0, y=45.0, sigma=TOWN_SIGMA, kind="town"),
    ]
    _lines, edges = compute_macro_arterials(
        nodes, cost, x0=0.0, y0=0.0, cell=1.0, beta_ratio=np.inf
    )
    # 1 highway (city-city) + 2 majors (town to each city). No tier-0 edges.
    assert sorted(e.tier for e in edges) == [1, 1, 2]


def test_town_peak_dedup_near_city() -> None:
    """A density peak co-located with a city is dropped; a far one is kept."""
    # 21x21 raster, cell=1, x0=y0=0 → cell centres at 0.5 .. 20.5.
    density = np.zeros((21, 21), dtype=float)
    density[5, 5] = 10.0  # peak near a city
    density[18, 18] = 9.0  # peak far from any city
    # City sits essentially on the (5,5) peak centre (x=y=5.5).
    city = MacroNode(x=5.5, y=5.5, sigma=CITY_SIGMA, kind="city", label="c", mass=1.0)

    towns = town_nodes_from_peaks(
        density,
        x0=0.0,
        y0=0.0,
        cell=1.0,
        cities=[city],
        merge_radius=6.0,
        peak_min_distance_units=2.0,
        peak_threshold_frac=0.01,
    )
    # Only the far peak survives dedup.
    assert all(t.kind == "town" and t.sigma == TOWN_SIGMA for t in towns)
    assert len(towns) == 1
    assert abs(towns[0].x - 18.5) < 1e-9
    assert abs(towns[0].y - 18.5) < 1e-9


def test_density_cost_field_finite_inside_inf_outside() -> None:
    """Cost is finite & strictly positive inside mask, inf outside."""
    slope = np.ones((8, 8), dtype=float) * 0.5
    flow_accum = np.zeros((8, 8), dtype=float)
    density = np.zeros((8, 8), dtype=float)
    density[3:5, 3:5] = 100.0  # dense core
    mask = np.zeros((8, 8), dtype=bool)
    mask[2:6, 2:6] = True

    cost = build_density_cost_field(
        slope,
        flow_accum,
        density,
        mask,
        base=1.0,
        w_slope=8.0,
        w_river=6.0,
        w_density=0.8,
        cost_floor=0.05,
    )
    inside = cost[mask]
    assert np.all(np.isfinite(inside))
    assert np.all(inside >= 0.05)
    assert np.all(np.isinf(cost[~mask]))
    # Dense core must be cheaper than a non-dense inside cell.
    assert cost[3, 3] < cost[2, 2]


def _connected(edges: set[tuple[int, int]], nodes: set[int]) -> bool:
    """True if ``edges`` form one connected component over ``nodes``."""
    if not nodes:
        return True
    adj: dict[int, set[int]] = {n: set() for n in nodes}
    for a, b in edges:
        adj[a].add(b)
        adj[b].add(a)
    seen = {next(iter(sorted(nodes)))}
    stack = list(seen)
    while stack:
        for nb in adj[stack.pop()]:
            if nb not in seen:
                seen.add(nb)
                stack.append(nb)
    return seen == nodes


def test_delaunay_edges_collinear_nodes_no_qhull_crash() -> None:
    """>=3 exactly collinear nodes must not raise (QhullError fallback).

    Regression: per-sigma-level subsets are tiny (3-4 nodes) so exact
    collinearity is routine on other islands; plain scipy Delaunay raises
    QhullError ("initial simplex is flat") there.
    """
    xs = np.array([0.0, 10.0, 20.0, 30.0])
    ys = np.array([0.0, 10.0, 20.0, 30.0])
    edges = _delaunay_edges([0, 1, 2, 3], xs, ys)
    touched = {i for pair in edges for i in pair}
    assert touched == {0, 1, 2, 3}
    assert _connected(edges, {0, 1, 2, 3})


def test_compute_macro_arterials_collinear_cities_no_crash() -> None:
    """End-to-end: 3 collinear cities route into a connected network."""
    cost = np.ones((60, 60), dtype=float)
    nodes = [
        MacroNode(x=10.0 + 15.0 * i, y=10.0 + 15.0 * i, sigma=CITY_SIGMA, kind="city")
        for i in range(3)
    ]
    _lines, edges = compute_macro_arterials(
        nodes, cost, x0=0.0, y0=0.0, cell=1.0, beta_ratio=np.inf
    )
    assert len(edges) >= 2
    touched = {e.node_a for e in edges} | {e.node_b for e in edges}
    assert touched == {0, 1, 2}


def test_nearest_neighbor_chain_connects_all_nodes() -> None:
    """The last-resort chain fallback yields a connected m-1 edge chain."""
    xs = np.array([0.0, 5.0, 1.0, 3.0])
    ys = np.zeros(4)
    edges = _nearest_neighbor_chain([0, 1, 2, 3], xs, ys)
    # Greedy from node 0: nearest is 2 (x=1), then 3 (x=3), then 1 (x=5).
    assert edges == {(0, 2), (2, 3), (1, 3)}
    assert _connected(edges, {0, 1, 2, 3})
    # Coincident nodes still chain deterministically.
    same = _nearest_neighbor_chain([4, 7, 9], np.zeros(10), np.zeros(10))
    assert same == {(4, 7), (7, 9)}


def test_pruned_highway_pair_stays_eligible_at_lower_tier() -> None:
    """A pair beta-PRUNED at the highway tier must stay eligible as a major.

    Regression: ``realized`` used to accumulate all Delaunay-ATTEMPTED pairs
    before pruning, so a city-city pair pruned at level 2 could never reappear
    at level 1, contradicting the docstring contract.

    Geometry (flat unit cost, beta=1.2): cities A=(5,5), B=(55,5) with C=(30,10)
    just off the A-B line. At level 2 the detour A-C-B (~54) undercuts
    1.2 * direct(A-B) (~60) so A-B is pruned. At level 1 a distant town
    T=(30,60) joins; the realized edges A-C / C-B are excluded from the level-1
    graph, the only alternative A-T-B (~130) exceeds the threshold, and A-B must
    now be KEPT as a tier-1 major.
    """
    cost = np.ones((70, 70), dtype=float)
    nodes = [
        MacroNode(x=5.0, y=5.0, sigma=CITY_SIGMA, kind="city"),  # A = 0
        MacroNode(x=55.0, y=5.0, sigma=CITY_SIGMA, kind="city"),  # B = 1
        MacroNode(x=30.0, y=10.0, sigma=CITY_SIGMA, kind="city"),  # C = 2
        MacroNode(x=30.0, y=60.0, sigma=TOWN_SIGMA, kind="town"),  # T = 3
    ]
    _lines, edges = compute_macro_arterials(
        nodes, cost, x0=0.0, y0=0.0, cell=1.0, beta_ratio=1.2
    )
    pairs_by_tier: dict[int, set[tuple[int, int]]] = {}
    for e in edges:
        pair = (min(e.node_a, e.node_b), max(e.node_a, e.node_b))
        pairs_by_tier.setdefault(e.tier, set()).add(pair)
    # Level 2 keeps the cheap detour legs and prunes the direct A-B highway.
    assert pairs_by_tier.get(2) == {(0, 2), (1, 2)}
    # The pruned A-B pair is re-attempted and kept at the major tier.
    assert (0, 1) in pairs_by_tier.get(1, set())


def test_edge_tier_is_min_sigma() -> None:
    """Edge tau/tier = min(sigma_a, sigma_b) over a flat 2-node cost field."""
    assert tier_for_tau(2) == 2
    assert tier_for_tau(1) == 1

    cost = np.ones((40, 40), dtype=float)
    nodes = [
        MacroNode(x=5.0, y=5.0, sigma=CITY_SIGMA, kind="city"),
        MacroNode(x=30.0, y=30.0, sigma=TOWN_SIGMA, kind="town"),
    ]
    _lines, edges = compute_macro_arterials(
        nodes, cost, x0=0.0, y0=0.0, cell=1.0, beta_ratio=np.inf
    )
    assert len(edges) == 1
    # min(city=2, town=1) = 1 → major tier.
    assert edges[0].tau == 1
    assert edges[0].tier == 1


# ---------------------------------------------------------------------------
# Core ring-roads (stage 3.5b)
# ---------------------------------------------------------------------------


def _two_peak_raster(
    sep_cells: int, *, shape: tuple[int, int] = (60, 60)
) -> np.ndarray:
    """Raster with two Gaussian-ish peaks separated by ``sep_cells`` columns."""
    nrows, ncols = shape
    rr, cc = np.mgrid[0:nrows, 0:ncols]
    cr = nrows // 2
    c1 = ncols // 2 - sep_cells // 2
    c2 = ncols // 2 + sep_cells // 2

    def blob(r0: int, c0: int) -> np.ndarray:
        return np.exp(-(((rr - r0) ** 2 + (cc - c0) ** 2) / (2 * 3.0**2)))

    return (blob(cr, c1) + blob(cr, c2)).astype(float)


def test_detect_core_regions_separate_peaks_two_cores() -> None:
    """Two well-separated peaks → two distinct core polygons."""
    # cell=1 so 6.0-unit max radius easily contains each ~3-cell blob but not the
    # gap between peaks 24 cells apart.
    density = _two_peak_raster(sep_cells=24)
    nodes = [
        MacroNode(x=18.5, y=30.5, sigma=TOWN_SIGMA, kind="town"),
        MacroNode(x=42.5, y=30.5, sigma=TOWN_SIGMA, kind="town"),
    ]
    cores = detect_core_regions(
        density,
        nodes,
        x0=0.0,
        y0=0.0,
        cell=1.0,
        core_max_radius_units=6.0,
        core_min_area_units2=4.0,
    )
    assert len(cores) == 2
    assert all(c.geom_type == "Polygon" and c.area > 0 for c in cores)


def test_detect_core_regions_adjacent_peaks_merge_to_one() -> None:
    """Two peaks within max-radius (touching grown regions) → merged into 1 core."""
    density = _two_peak_raster(sep_cells=6)
    nodes = [
        MacroNode(x=27.5, y=30.5, sigma=TOWN_SIGMA, kind="town"),
        MacroNode(x=33.5, y=30.5, sigma=TOWN_SIGMA, kind="town"),
    ]
    cores = detect_core_regions(
        density,
        nodes,
        x0=0.0,
        y0=0.0,
        cell=1.0,
        core_max_radius_units=6.0,
        core_min_area_units2=4.0,
    )
    assert len(cores) == 1


def test_detect_core_regions_skips_villages() -> None:
    """sigma=0 village nodes never seed a core."""
    density = _two_peak_raster(sep_cells=24)
    nodes = [
        MacroNode(x=18.5, y=30.5, sigma=VILLAGE_SIGMA, kind="village"),
        MacroNode(x=42.5, y=30.5, sigma=VILLAGE_SIGMA, kind="village"),
    ]
    cores = detect_core_regions(
        density, nodes, x0=0.0, y0=0.0, cell=1.0, core_min_area_units2=4.0
    )
    assert cores == []


def test_clip_arterials_to_cores_removes_interior_keeps_tier() -> None:
    """A line crossing a core loses its interior portion but keeps tier/tau."""
    core = sg.box(40.0, 40.0, 60.0, 60.0)
    # Horizontal line at y=50 spanning x in [0, 100], straight through the core.
    line = sg.LineString([(0.0, 50.0), (100.0, 50.0)])
    edge = MacroEdge(node_a=0, node_b=1, tau=1, tier=2, path_cost=1.0, length=100.0)
    clipped_lines, clipped_edges = clip_arterials_to_cores([line], [edge], [core])
    # Splits into two segments either side of the core.
    assert len(clipped_lines) == 2
    assert len(clipped_edges) == 2
    # Tier/tau/node ids preserved on every surviving segment.
    assert all(e.tier == 2 and e.tau == 1 for e in clipped_edges)
    assert all(e.node_a == 0 and e.node_b == 1 for e in clipped_edges)
    # No surviving segment lies inside the core interior.
    assert all(not core.buffer(-1e-6).intersects(ln) for ln in clipped_lines)
    # Lengths sum to less than the original (interior removed).
    assert sum(e.length for e in clipped_edges) < 100.0


def test_clip_arterials_to_cores_no_cores_is_identity() -> None:
    """With no cores, clipping returns the inputs unchanged."""
    line = sg.LineString([(0.0, 0.0), (10.0, 10.0)])
    edge = MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=14.0)
    lines, edges = clip_arterials_to_cores([line], [edge], [])
    assert lines == [line]
    assert edges == [edge]


# ---------------------------------------------------------------------------
# Composed macro-layer assembly (build_macro_layer)
# ---------------------------------------------------------------------------


def _synthetic_island() -> tuple[IslandFields, sg.Polygon, pl.DataFrame]:
    """Square 60x60 island with two Gaussian density blobs and 2 L1 clusters.

    Each L1 cluster ("west" @ ~(15,15), "east" @ ~(45,45)) holds two L0
    clusters offset either side of the blob centre, so the hierarchical
    seeding yields 2 cities + 4 towns at distinct positions.
    """
    n = 60
    rr, cc = np.mgrid[0:n, 0:n]

    def blob(r0: int, c0: int) -> np.ndarray:
        return np.exp(-(((rr - r0) ** 2 + (cc - c0) ** 2) / (2 * 4.0**2)))

    density = (blob(15, 15) + blob(45, 45)).astype(float)
    flat = np.zeros((n, n), dtype=float)
    fields = IslandFields(
        density=density,
        height=flat,
        flow_accum=flat,
        height_carved=flat,
        slope=flat,
        x0=0.0,
        y0=0.0,
        cell=1.0,
    )
    boundary = sg.box(0.0, 0.0, float(n), float(n))
    points = pl.DataFrame(
        {
            "world_id": [str(i) for i in range(8)],
            "l0_id": [0, 0, 1, 1, 2, 2, 3, 3],
            "l0_name": ["a", "a", "b", "b", "c", "c", "d", "d"],
            "l1_id": [0, 0, 0, 0, 1, 1, 1, 1],
            "l1_name": ["west"] * 4 + ["east"] * 4,
            "visits": [10] * 8,
            "x": [12.0, 12.0, 18.0, 18.0, 42.0, 42.0, 48.0, 48.0],
            "y": [15.0] * 4 + [45.0] * 4,
        }
    )
    return fields, boundary, points


def test_build_macro_layer_no_cores_matches_pre35b_polygonization() -> None:
    """Docstring invariant: with no detectable cores the blocks match pre-3.5b.

    ``core_min_area_units2`` is set impossibly high so every grown core region
    is dropped; the layer's blocks must then equal a plain
    ``polygonize_macro_blocks`` over the raw arterials, and the clipped
    network must equal the raw one.
    """
    fields, boundary, points = _synthetic_island()
    params = MacroParams(core_min_area_units2=1e9)
    layer = build_macro_layer(fields, boundary, points, params)

    assert layer.core_polys == []
    assert layer.ring_lines == []
    assert layer.arterial_lines == layer.raw_arterial_lines
    assert layer.edges == layer.raw_edges

    expected = polygonize_macro_blocks(
        layer.raw_arterial_lines, boundary, min_block_area=params.min_block_area
    )
    assert len(layer.blocks) == len(expected)
    for got, want in zip(layer.blocks, expected, strict=True):
        assert got.equals(want)


def test_build_macro_layer_smoke_square_island() -> None:
    """Composed pipeline smoke test: default params on the synthetic island."""
    fields, boundary, points = _synthetic_island()
    params = MacroParams()
    layer = build_macro_layer(fields, boundary, points, params)

    assert layer.params is params
    # Hierarchical seeding: 2 L1 cities + 4 L0 towns (villages deduped away or
    # few).
    assert sum(1 for nd in layer.nodes if nd.kind == "city") == 2
    assert sum(1 for nd in layer.nodes if nd.kind == "town") == 4
    # Peak-snapped seeding (S2): every node lands on one of its region's TWO
    # density peaks -- in this fixture the child towns sit close enough to
    # their parent city's blob that they snap onto the SAME peak (this is the
    # intended structural change: a saddle-seeded core is weak/dropped, so
    # nodes now sit on real summits rather than staying at their raw
    # visit-weighted centroid). The two REGIONS (west/east) stay distinct.
    positions = {(nd.x, nd.y) for nd in layer.nodes}
    assert len(positions) == 2
    assert {(nd.x, nd.y) for nd in layer.nodes if nd.kind == "city"} == positions
    # Lines and edge records stay parallel, raw and clipped.
    assert len(layer.raw_arterial_lines) == len(layer.raw_edges)
    assert len(layer.arterial_lines) == len(layer.edges)
    assert len(layer.raw_edges) > 0
    # Cost field matches the raster and is a valid routing cost inside.
    assert layer.cost.shape == fields.density.shape
    assert np.all(layer.cost[np.isfinite(layer.cost)] > 0)
    # Two dense blobs -> cores detected with default knobs; blocks tile within
    # the boundary.
    assert len(layer.core_polys) >= 1
    assert layer.blocks
    assert sum(b.area for b in layer.blocks) <= boundary.area * 1.01
    # Params round-trip to a JSON-ready dict (manifest contract).
    d = params.to_dict()
    assert d["hierarchical"] is True
    assert d["min_block_area"] == params.min_block_area


# ---------------------------------------------------------------------------
# Chaikin smoothing (S1: arterials + core rings)
# ---------------------------------------------------------------------------


def _max_turn_angle_deg(line: sg.LineString) -> float:
    """Largest single-vertex turn angle (degrees) along an open polyline."""
    coords = list(line.coords)
    if len(coords) < 3:
        return 0.0
    max_turn = 0.0
    for i in range(1, len(coords) - 1):
        (x0, y0), (x1, y1), (x2, y2) = coords[i - 1], coords[i], coords[i + 1]
        v1 = np.array([x1 - x0, y1 - y0])
        v2 = np.array([x2 - x1, y2 - y1])
        n1, n2 = np.linalg.norm(v1), np.linalg.norm(v2)
        if n1 == 0.0 or n2 == 0.0:
            continue
        cosang = np.clip(np.dot(v1, v2) / (n1 * n2), -1.0, 1.0)
        max_turn = max(max_turn, float(np.degrees(np.arccos(cosang))))
    return max_turn


def test_smooth_polyline_preserves_endpoints_exactly() -> None:
    """Endpoints must stay bit-for-bit identical (load-bearing for fusion)."""
    line = sg.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (20.0, 10.0)])
    smoothed = smooth_polyline(line, iterations=2, post_tol=0.15)
    assert list(smoothed.coords)[0] == (0.0, 0.0)
    assert list(smoothed.coords)[-1] == (20.0, 10.0)


def test_smooth_polyline_reduces_max_turn_angle() -> None:
    """Chaikin must strictly reduce the sharpest single-vertex turn."""
    line = sg.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (20.0, 10.0)])
    before = _max_turn_angle_deg(line)
    smoothed = smooth_polyline(line, iterations=2, post_tol=0.15)
    after = _max_turn_angle_deg(smoothed)
    assert before == 90.0  # two right-angle kinks in the input
    assert after < before
    # More vertices too (rounded corners), even after the light post-simplify.
    assert len(list(smoothed.coords)) > len(list(line.coords))


def test_smooth_polyline_degenerate_two_point_passthrough() -> None:
    """A 2-point line has no corner to cut; it must pass through unchanged."""
    line = sg.LineString([(0.0, 0.0), (5.0, 5.0)])
    smoothed = smooth_polyline(line, iterations=2, post_tol=0.15)
    assert list(smoothed.coords) == list(line.coords)


def test_smooth_polyline_zero_iterations_is_noop() -> None:
    """``iterations=0`` disables smoothing (used for the unsmoothed A/B compare)."""
    line = sg.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (20.0, 10.0)])
    smoothed = smooth_polyline(line, iterations=0, post_tol=0.15)
    assert list(smoothed.coords) == list(line.coords)


def test_smooth_polyline_deterministic() -> None:
    """Same input -> same output (no unseeded randomness)."""
    line = sg.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (20.0, 10.0)])
    a = smooth_polyline(line, iterations=2, post_tol=0.15)
    b = smooth_polyline(line, iterations=2, post_tol=0.15)
    assert list(a.coords) == list(b.coords)


def test_chaikin_pass_open_preserves_endpoints_and_cuts_corner() -> None:
    """Direct unit check of the endpoint-preserving open Chaikin pass."""
    pts = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)]
    out = _chaikin_pass_open(pts)
    assert out[0] == pts[0]
    assert out[-1] == pts[-1]
    # Interior corner is cut: R0 = 0.25*A + 0.75*B, Q1 = 0.75*B + 0.25*C.
    assert out == [(0.0, 0.0), (7.5, 0.0), (10.0, 2.5), (10.0, 10.0)]


def test_chaikin_pass_closed_wraps_and_drops_no_vertices() -> None:
    """Direct unit check of the closed-ring Chaikin pass (a unit square)."""
    ring = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]
    out = _chaikin_pass_closed(ring)
    assert out[0] == out[-1]  # still a closed ring
    assert len(out) == 2 * 4 + 1  # 4 edges -> 2 new points each, plus closure


def test_smooth_arterial_lines_falls_back_when_smoothed_vertex_leaves_mask() -> None:
    """Guard: a smoothed vertex stepping outside the mask reverts the WHOLE line.

    Constructed so the single Chaikin corner-cut point (R0 = 0.25*A + 0.75*B)
    for a right-angle A-B-C line lands in a specific cell that we block out.
    """
    mask = np.ones((10, 10), dtype=bool)
    line = sg.LineString([(2.5, 2.5), (5.5, 2.5), (5.5, 5.5)])
    # R0 = 0.25*(2.5,2.5) + 0.75*(5.5,2.5) = (4.75, 2.5) -> cell (row=2, col=4).
    mask[2, 4] = False
    out = smooth_arterial_lines(
        [line], mask, x0=0.0, y0=0.0, cell=1.0, iterations=1, post_tol=0.0
    )
    assert list(out[0].coords) == list(line.coords)

    # Control: with the cell unblocked, the line IS smoothed (not a no-op).
    mask[2, 4] = True
    out_ok = smooth_arterial_lines(
        [line], mask, x0=0.0, y0=0.0, cell=1.0, iterations=1, post_tol=0.0
    )
    assert list(out_ok[0].coords) != list(line.coords)
    assert list(out_ok[0].coords)[0] == (2.5, 2.5)
    assert list(out_ok[0].coords)[-1] == (5.5, 5.5)


def _staircase_cells(n: int) -> set[tuple[int, int]]:
    """A jagged staircase-shaped cell region (deliberately un-smooth)."""
    cells: set[tuple[int, int]] = set()
    for r in range(n):
        for c in range(n - r, n + 3):
            cells.add((r, c))
    return cells


def test_cells_to_polygon_ring_is_smoother_than_raw_pixel_blob() -> None:
    """Ring boundary smoothing (closing + Chaikin) beats the raw pixel union.

    Compares the smoothed ``_cells_to_polygon`` output against the raw,
    UNSIMPLIFIED cell-square union boundary (the literal "pixel-blob outline"
    the bug report describes) over the same jagged staircase cell region: the
    smoothed ring must have far fewer vertices and stay within a modest
    fraction of the raw footprint's area.
    """
    cells = _staircase_cells(6)
    boxes = [sg.box(c, r, c + 1, r + 1) for (r, c) in cells]
    raw_union = unary_union(boxes)
    raw_poly = sg.Polygon(raw_union.exterior)  # no simplify: the raw blob itself

    smoothed = _cells_to_polygon(
        cells,
        x0=0.0,
        y0=0.0,
        cell=1.0,
        simplify_tolerance=DEFAULT_CORE_SIMPLIFY_TOLERANCE,
    )
    assert smoothed is not None
    assert smoothed.is_valid
    assert len(smoothed.exterior.coords) < len(raw_poly.exterior.coords)
    sym_diff_area = smoothed.symmetric_difference(raw_union).area
    assert sym_diff_area < 0.35 * raw_union.area


def test_smoothing_preserves_shared_junction_coincidence_across_lines() -> None:
    """Load-bearing fusion invariant: a shared macro-node endpoint stays EXACT.

    Three arterials meeting at one macro-node junction, smoothed
    independently, must still meet at EXACTLY the same point afterward --
    that coincidence is what downstream T-junction fusion (gate snapping,
    block polygonization) depends on. Also checks smoothing actually moved at
    least one line's interior (the feature isn't silently a no-op).
    """
    shared = (20.0, 20.0)
    line_a = sg.LineString([(0.0, 0.0), (10.0, 5.0), shared])
    line_b = sg.LineString([shared, (25.0, 10.0), (40.0, 0.0)])
    line_c = sg.LineString([shared, (20.0, 40.0)])
    mask = np.ones((60, 60), dtype=bool)

    smoothed = smooth_arterial_lines(
        [line_a, line_b, line_c], mask, x0=0.0, y0=0.0, cell=1.0
    )

    assert list(smoothed[0].coords)[-1] == shared
    assert list(smoothed[1].coords)[0] == shared
    assert list(smoothed[2].coords)[0] == shared
    # Sanity: smoothing actually changed at least one line's interior geometry.
    assert list(smoothed[0].coords) != list(line_a.coords)
    assert list(smoothed[1].coords) != list(line_b.coords)


def test_build_macro_layer_smoothing_preserves_fusion_invariants() -> None:
    """Smoothing on vs off must not change junction coincidence / block count.

    Load-bearing safety check at the composed-pipeline level: the smoothed
    and unsmoothed (``smooth_iterations=0``) layers must agree on block count,
    core count, and every arterial's endpoints (so shared macro-node
    junctions stay fused). NB: this particular synthetic island's routed
    edges happen to come out perfectly straight (flat slope/river fields, no
    obstacle to route around), so there is nothing for Chaikin to visibly
    smooth here; the "smoothing actually changes geometry" check lives in
    ``test_smoothing_preserves_shared_junction_coincidence_across_lines`` and
    the ``smooth_polyline`` unit tests above instead.
    """
    fields, boundary, points = _synthetic_island()
    layer_smoothed = build_macro_layer(fields, boundary, points, MacroParams())
    layer_unsmoothed = build_macro_layer(
        fields, boundary, points, MacroParams(smooth_iterations=0)
    )

    assert len(layer_smoothed.blocks) == len(layer_unsmoothed.blocks)
    assert len(layer_smoothed.raw_arterial_lines) == len(
        layer_unsmoothed.raw_arterial_lines
    )
    assert len(layer_smoothed.core_polys) == len(layer_unsmoothed.core_polys)

    for ln_s, ln_u in zip(
        layer_smoothed.raw_arterial_lines,
        layer_unsmoothed.raw_arterial_lines,
        strict=True,
    ):
        cs, cu = list(ln_s.coords), list(ln_u.coords)
        assert cs[0] == cu[0]
        assert cs[-1] == cu[-1]


# ---------------------------------------------------------------------------
# Peak-snapped seeding (S2)
# ---------------------------------------------------------------------------


def test_snap_centroid_to_peak_saddle_snaps_to_known_peak() -> None:
    """A centroid sitting in a saddle near a strong peak snaps onto it."""
    peak_xs = np.array([20.0, 60.0])
    peak_ys = np.array([20.0, 20.0])
    peak_ds = np.array([5.0, 1.0])
    # Centroid a few units off the strong peak (a "saddle" position).
    snapped = snap_centroid_to_peak(23.0, 21.0, peak_xs, peak_ys, peak_ds, radius=6.0)
    assert snapped == (20.0, 20.0)


def test_snap_centroid_to_peak_no_peak_in_range_stays_put() -> None:
    """No peak within radius -> centroid unchanged."""
    peak_xs = np.array([100.0])
    peak_ys = np.array([100.0])
    peak_ds = np.array([5.0])
    snapped = snap_centroid_to_peak(0.0, 0.0, peak_xs, peak_ys, peak_ds, radius=6.0)
    assert snapped == (0.0, 0.0)


def test_snap_centroid_to_peak_picks_highest_density_in_range() -> None:
    """Two peaks in range -> the denser one wins, not the nearer one."""
    peak_xs = np.array([2.0, 5.0])
    peak_ys = np.array([0.0, 0.0])
    peak_ds = np.array([1.0, 9.0])  # far one is denser
    snapped = snap_centroid_to_peak(0.0, 0.0, peak_xs, peak_ys, peak_ds, radius=6.0)
    assert snapped == (5.0, 0.0)


def test_snap_centroid_to_peak_deterministic() -> None:
    """Same inputs -> same output (no unseeded randomness)."""
    peak_xs = np.array([2.0, 5.0])
    peak_ys = np.array([0.0, 0.0])
    peak_ds = np.array([1.0, 9.0])
    a = snap_centroid_to_peak(0.0, 0.0, peak_xs, peak_ys, peak_ds, radius=6.0)
    b = snap_centroid_to_peak(0.0, 0.0, peak_xs, peak_ys, peak_ds, radius=6.0)
    assert a == b


def test_snap_nodes_to_peaks_preserves_label_sigma_kind_mass() -> None:
    """Snapping moves x/y only; label/sigma/kind/mass survive untouched."""
    node = MacroNode(x=1.0, y=1.0, sigma=CITY_SIGMA, kind="city", label="a", mass=42.0)
    peak_xs = np.array([1.5])
    peak_ys = np.array([1.5])
    peak_ds = np.array([9.0])
    snapped = snap_nodes_to_peaks([node], peak_xs, peak_ys, peak_ds, radius=6.0)[0]
    assert (snapped.x, snapped.y) == (1.5, 1.5)
    assert snapped.label == "a"
    assert snapped.sigma == CITY_SIGMA
    assert snapped.kind == "city"
    assert snapped.mass == 42.0


def test_snap_nodes_to_peaks_out_of_range_node_unchanged_object() -> None:
    """A node with no peak in range is returned AS-IS (not just equal)."""
    node = MacroNode(x=1.0, y=1.0, sigma=CITY_SIGMA, kind="city", label="a", mass=1.0)
    peak_xs = np.array([100.0])
    peak_ys = np.array([100.0])
    peak_ds = np.array([9.0])
    out = snap_nodes_to_peaks([node], peak_xs, peak_ys, peak_ds, radius=6.0)
    assert out[0] is node


def test_build_macro_nodes_hierarchical_snaps_city_off_peak_onto_it() -> None:
    """End-to-end: a city centroid in a density saddle snaps onto the real peak.

    Two Gaussian blobs 20 units apart (unequal amplitude); the single L1/L0
    cluster's points straddle the saddle between them so the visit-weighted
    centroid lands near the midpoint x=40 (equidistant from both peaks, low
    density itself) -- peak-snap must pick the TALLER peak, not just the
    nearer one (both are equidistant here) and not leave the node at the
    saddle.
    """
    n = 80
    rr, cc = np.mgrid[0:n, 0:n]

    def blob(r0: int, c0: int, amp: float) -> np.ndarray:
        return amp * np.exp(-(((rr - r0) ** 2 + (cc - c0) ** 2) / (2 * 3.0**2)))

    density = (blob(40, 30, 10.0) + blob(40, 50, 4.0)).astype(float)
    points = pl.DataFrame(
        {
            "l0_id": [0, 0],
            "l0_name": ["only", "only"],
            "l1_id": [0, 0],
            "l1_name": ["only", "only"],
            "visits": [10, 10],
            "x": [26.0, 54.0],  # visit-weighted mean x = 40 -- the saddle
            "y": [40.5, 40.5],
        }
    )
    nodes = build_macro_nodes_hierarchical(
        density, x0=0.0, y0=0.0, cell=1.0, points=points, peak_snap_radius_units=15.0
    )
    city = next(nd for nd in nodes if nd.kind == "city")
    assert abs(city.x - 30.5) < 1.0
    assert abs(city.y - 40.5) < 1.0


def test_build_macro_nodes_hierarchical_disabled_snap_keeps_raw_centroid() -> None:
    """``peak_snap_radius_units<=0`` disables snapping (pre-S2 behavior)."""
    n = 80
    rr, cc = np.mgrid[0:n, 0:n]

    def blob(r0: int, c0: int, amp: float) -> np.ndarray:
        return amp * np.exp(-(((rr - r0) ** 2 + (cc - c0) ** 2) / (2 * 3.0**2)))

    density = (blob(40, 30, 10.0) + blob(40, 50, 4.0)).astype(float)
    points = pl.DataFrame(
        {
            "l0_id": [0, 0],
            "l0_name": ["only", "only"],
            "l1_id": [0, 0],
            "l1_name": ["only", "only"],
            "visits": [10, 10],
            "x": [26.0, 54.0],
            "y": [40.5, 40.5],
        }
    )
    nodes = build_macro_nodes_hierarchical(
        density, x0=0.0, y0=0.0, cell=1.0, points=points, peak_snap_radius_units=0.0
    )
    city = next(nd for nd in nodes if nd.kind == "city")
    assert city.x == 40.0 and city.y == 40.5


# ---------------------------------------------------------------------------
# Ranked settlement nuclei (S2: NucleusSpec)
# ---------------------------------------------------------------------------


def test_build_nucleus_specs_anchor_inside_polygon_and_ranks_ordered() -> None:
    """Anchor lies inside its own polygon; ranks strictly track mass."""
    density = _two_peak_raster(sep_cells=24)
    nodes = [
        MacroNode(x=18.5, y=30.5, sigma=TOWN_SIGMA, kind="town", label="alpha"),
        MacroNode(x=42.5, y=30.5, sigma=TOWN_SIGMA, kind="town", label="beta"),
    ]
    core_polys = detect_core_regions(
        density,
        nodes,
        x0=0.0,
        y0=0.0,
        cell=1.0,
        core_max_radius_units=6.0,
        core_min_area_units2=4.0,
    )
    assert len(core_polys) == 2
    # Two extra worlds near alpha's core, one near beta's -- density mass is
    # symmetric for this fixture, so the world-count term must be what tips
    # alpha's core to outrank beta's.
    points = pl.DataFrame({"x": [18.5, 18.5, 42.5], "y": [30.5, 30.5, 30.5]})
    specs = build_nucleus_specs(
        density, core_polys, nodes, points, x0=0.0, y0=0.0, cell=1.0, n_major=1
    )
    assert len(specs) == 2
    for spec in specs:
        assert spec.polygon.contains(sg.Point(*spec.anchor))

    by_rank = sorted(specs, key=lambda s: s.rank)
    assert [s.rank for s in by_rank] == [1, 2]
    assert by_rank[0].mass >= by_rank[1].mass
    assert by_rank[0].label == "alpha"
    # top-K=1 -> exactly 1 is_major, and it's the rank-1 nucleus.
    assert sum(1 for s in specs if s.is_major) == 1
    assert by_rank[0].is_major and not by_rank[1].is_major


def test_build_nucleus_specs_top_k_is_major_count() -> None:
    """``is_major`` count == ``min(K, n_cores)``, including K > n_cores."""
    density = _two_peak_raster(sep_cells=24)
    nodes = [
        MacroNode(x=18.5, y=30.5, sigma=TOWN_SIGMA, kind="town", label="a"),
        MacroNode(x=42.5, y=30.5, sigma=TOWN_SIGMA, kind="town", label="b"),
    ]
    core_polys = detect_core_regions(
        density,
        nodes,
        x0=0.0,
        y0=0.0,
        cell=1.0,
        core_max_radius_units=6.0,
        core_min_area_units2=4.0,
    )
    points = pl.DataFrame({"x": [18.5], "y": [30.5]})
    specs = build_nucleus_specs(
        density, core_polys, nodes, points, x0=0.0, y0=0.0, cell=1.0, n_major=10
    )
    assert sum(1 for s in specs if s.is_major) == min(10, len(core_polys))


def test_build_nucleus_specs_empty_cores_is_empty() -> None:
    """No surviving cores -> no nuclei (identity, doesn't crash)."""
    density = _two_peak_raster(sep_cells=24)
    nodes = [MacroNode(x=18.5, y=30.5, sigma=TOWN_SIGMA, kind="town", label="a")]
    points = pl.DataFrame({"x": [18.5], "y": [30.5]})
    specs = build_nucleus_specs(
        density, [], nodes, points, x0=0.0, y0=0.0, cell=1.0, n_major=6
    )
    assert specs == []


def test_assign_nearest_nucleus_small_fixture() -> None:
    """Nearest-anchor assignment + normalized clamped distance, small fixture."""
    nuclei = [
        NucleusSpec(
            anchor=(0.0, 0.0),
            polygon=sg.box(-1, -1, 1, 1),
            mass=10.0,
            rank=1,
            label="a",
            influence_radius=2.0,
            is_major=True,
        ),
        NucleusSpec(
            anchor=(20.0, 0.0),
            polygon=sg.box(19, -1, 21, 1),
            mass=5.0,
            rank=2,
            label="b",
            influence_radius=1.0,
            is_major=False,
        ),
    ]
    polys = [
        sg.box(-0.5, -0.5, 0.5, 0.5),  # centroid (0,0) -> nucleus 0, dist 0
        sg.box(0.5, -0.5, 1.5, 0.5),  # centroid (1,0) -> nucleus 0, dist 1/2=0.5
        sg.box(24.0, -0.5, 26.0, 0.5),  # centroid (25,0) -> nucleus 1, clamp 1.0
    ]
    result = assign_nearest_nucleus(polys, nuclei)
    assert result[0] == (0, 0.0)
    nid, ndist = result[1]
    assert nid == 0
    assert ndist is not None and abs(ndist - 0.5) < 1e-9
    assert result[2] == (1, 1.0)


def test_assign_nearest_nucleus_empty_nuclei_returns_none() -> None:
    """No nuclei detected -> every polygon gets ``(None, None)``."""
    result = assign_nearest_nucleus([sg.box(0.0, 0.0, 1.0, 1.0)], [])
    assert result == [(None, None)]


# ---------------------------------------------------------------------------
# Intra-nucleus avenues (S3: the keystone slice)
# ---------------------------------------------------------------------------

# A 10x10-unit box core centered on the origin -- axis-aligned so a station at
# a cardinal/diagonal-friendly angle lands EXACTLY on the boundary (no
# discretized-circle approximation error to fuss over in a unit test).
_BOX_CORE = sg.box(-5.0, -5.0, 5.0, 5.0)
_BOX_NUCLEUS = NucleusSpec(
    anchor=(0.0, 0.0),
    polygon=_BOX_CORE,
    mass=1.0,
    rank=1,
    label="downtown",
    influence_radius=7.0710678,
    is_major=True,
)


def _radial_line(
    angle_deg: float, station_radius: float, outer_radius: float
) -> sg.LineString:
    """A line from ``outer_radius`` in to a station on ``_BOX_CORE``'s boundary.

    ``station_radius`` must put the inner endpoint exactly on the box
    boundary (5.0 for the 4 cardinal directions; ``5 / cos/sin`` for others
    within the same quadrant works too, but callers stick to cardinals for
    exactness). Coordinates are rounded to 9 decimals so a cardinal angle's
    ~1e-16 ``cos``/``sin`` noise collapses to an EXACT 0.0 -- load-bearing
    for tests that check exact station-point equality.
    """
    ang = np.radians(angle_deg)
    outer = (
        round(outer_radius * float(np.cos(ang)), 9),
        round(outer_radius * float(np.sin(ang)), 9),
    )
    inner = (
        round(station_radius * float(np.cos(ang)), 9),
        round(station_radius * float(np.sin(ang)), 9),
    )
    return sg.LineString([outer, inner])


def test_ring_junction_stations_finds_endpoints_on_boundary_only() -> None:
    """Only the endpoint that actually sits on the polygon boundary counts."""
    on_ring = _radial_line(0.0, 5.0, 10.0)  # inner endpoint (5, 0) is on the box.
    off_ring = sg.LineString([(20.0, 20.0), (30.0, 30.0)])  # nowhere near the box.
    stations = _ring_junction_stations([on_ring, off_ring], _BOX_CORE)
    assert stations == [(5.0, 0.0)]


def test_ring_junction_stations_dedupes_exact_duplicate_points() -> None:
    """Two lines sharing the same station point contribute it only once."""
    line_a = _radial_line(90.0, 5.0, 10.0)
    line_b = sg.LineString(
        [(0.0, 5.0), (0.0, 8.0)]
    )  # same station, other end elsewhere.
    stations = _ring_junction_stations([line_a, line_b], _BOX_CORE)
    assert stations == [(0.0, 5.0)]


def test_dedupe_stations_by_angle_merges_close_pair() -> None:
    """Two stations within the min angular spacing dedupe to one."""
    near_dup = (5.0, float(5.0 * np.tan(np.radians(10.0))))  # ~10 deg, on the box edge.
    stations = [(5.0, 0.0), near_dup, (0.0, 5.0), (-5.0, 0.0), (0.0, -5.0)]
    deduped = _dedupe_stations_by_angle(
        stations, (0.0, 0.0), float(np.radians(DEFAULT_SPOKE_MIN_ANGLE_DEG))
    )
    assert len(deduped) == 4
    assert (5.0, 0.0) in deduped
    assert near_dup not in deduped


def test_dedupe_stations_by_angle_noop_for_0_or_1_station() -> None:
    """0 or 1 station -> nothing to dedupe, returned unchanged."""
    assert _dedupe_stations_by_angle([], (0.0, 0.0), 0.5) == []
    one = [(5.0, 0.0)]
    assert _dedupe_stations_by_angle(one, (0.0, 0.0), 0.5) == one


def test_build_nucleus_avenues_basic_spokes_and_plaza() -> None:
    """4 well-separated stations -> 4 spokes to an exact plaza ring."""
    lines = [
        _radial_line(0.0, 5.0, 10.0),
        _radial_line(90.0, 5.0, 10.0),
        _radial_line(180.0, 5.0, 10.0),
        _radial_line(270.0, 5.0, 10.0),
    ]
    result = build_nucleus_avenues(_BOX_NUCLEUS, lines)
    assert result is not None
    spoke_lines, spoke_edges, plaza_poly = result

    assert len(spoke_lines) == 4
    assert len(spoke_edges) == 4
    assert all(e.tier == 1 for e in spoke_edges)
    assert all(
        e.node_a == SPOKE_NODE_SENTINEL and e.node_b == SPOKE_NODE_SENTINEL
        for e in spoke_edges
    )

    # Outer endpoint of every spoke is exactly one of the 4 ring stations.
    stations = {tuple(ln.coords[0]) for ln in spoke_lines}
    assert stations == {(5.0, 0.0), (0.0, 5.0), (-5.0, 0.0), (0.0, -5.0)}

    # Plaza ring: closed, valid, radius r_plaza, centered on the anchor.
    assert list(plaza_poly.exterior.coords)[0] == list(plaza_poly.exterior.coords)[-1]
    assert plaza_poly.is_valid
    centroid = plaza_poly.centroid
    assert abs(centroid.x) < 1e-6 and abs(centroid.y) < 1e-6

    # Every spoke's inner (plaza-facing) endpoint lands EXACTLY on the plaza
    # ring -- this is what lets polygonize fuse them into clean wedge faces.
    for ln in spoke_lines:
        inner = sg.Point(ln.coords[-1])
        assert plaza_poly.exterior.distance(inner) < 1e-9
        assert abs(inner.distance(sg.Point(0.0, 0.0)) - 0.85) < 1e-9


def test_build_nucleus_avenues_deterministic() -> None:
    """Same inputs -> byte-identical spokes/plaza (no unseeded randomness)."""
    lines = [_radial_line(a, 5.0, 10.0) for a in (0.0, 90.0, 180.0, 270.0)]
    r1 = build_nucleus_avenues(_BOX_NUCLEUS, lines)
    r2 = build_nucleus_avenues(_BOX_NUCLEUS, lines)
    assert r1 is not None and r2 is not None
    lines1, edges1, plaza1 = r1
    lines2, edges2, plaza2 = r2
    assert [list(ln.coords) for ln in lines1] == [list(ln.coords) for ln in lines2]
    assert edges1 == edges2
    assert list(plaza1.exterior.coords) == list(plaza2.exterior.coords)


def test_build_nucleus_avenues_no_stations_returns_none() -> None:
    """An isolated core with no arterial reaching its ring -> no avenues."""
    unrelated = sg.LineString([(50.0, 50.0), (60.0, 60.0)])
    assert build_nucleus_avenues(_BOX_NUCLEUS, [unrelated]) is None


def test_build_nucleus_avenues_tiny_core_below_plaza_radius_returns_none() -> None:
    """A core smaller than ``r_plaza`` -> every spoke would be degenerate."""
    tiny_core = sg.box(-0.5, -0.5, 0.5, 0.5)
    tiny_nucleus = NucleusSpec(
        anchor=(0.0, 0.0),
        polygon=tiny_core,
        mass=1.0,
        rank=1,
        label="hamlet",
        influence_radius=0.7071,
        is_major=True,
    )
    line = sg.LineString([(10.0, 0.0), (0.5, 0.0)])
    assert build_nucleus_avenues(tiny_nucleus, [line], r_plaza=0.85) is None


def test_add_intra_nucleus_avenues_only_major_nuclei() -> None:
    """A non-major (town-center) nucleus gets no avenues at all."""
    major_core = sg.box(-5.0, -5.0, 5.0, 5.0)
    town_core = sg.box(95.0, -5.0, 105.0, 5.0)
    major = NucleusSpec(
        anchor=(0.0, 0.0),
        polygon=major_core,
        mass=10.0,
        rank=1,
        label="major",
        influence_radius=7.07,
        is_major=True,
    )
    town = NucleusSpec(
        anchor=(100.0, 0.0),
        polygon=town_core,
        mass=1.0,
        rank=2,
        label="town",
        influence_radius=7.07,
        is_major=False,
    )
    lines = [
        sg.LineString([(10.0, 0.0), (5.0, 0.0)]),  # touches the major core's ring.
        sg.LineString([(90.0, 0.0), (95.0, 0.0)]),  # touches the town core's ring.
    ]
    spoke_lines, spoke_edges, plaza_polys = add_intra_nucleus_avenues(
        [major, town], lines
    )
    assert len(plaza_polys) == 1
    assert len(spoke_lines) == 1
    assert list(spoke_lines[0].coords)[0] == (5.0, 0.0)
    assert abs(plaza_polys[0].centroid.x) < 1e-6


def test_add_intra_nucleus_avenues_empty_nuclei_is_empty() -> None:
    """No nuclei at all -> nothing built, doesn't crash."""
    spoke_lines, spoke_edges, plaza_polys = add_intra_nucleus_avenues([], [])
    assert spoke_lines == []
    assert spoke_edges == []
    assert plaza_polys == []


# --- Mass-scaled major plaza radii (slice P) ---


def _spec(rank: int, mass: float, *, is_major: bool = True) -> NucleusSpec:
    """Minimal NucleusSpec for radius-policy tests (geometry unused)."""
    return NucleusSpec(
        anchor=(0.0, 0.0),
        polygon=sg.box(-5.0, -5.0, 5.0, 5.0),
        mass=mass,
        rank=rank,
        label=f"n{rank}",
        influence_radius=7.07,
        is_major=is_major,
    )


def test_major_plaza_radii_mass_ordering_and_bounds() -> None:
    """Largest mass -> exactly r_max, smallest -> exactly r_min, sqrt between.

    Masses 100/25/4 have sqrt 10/5/2, so the middle major sits at
    t=(5-2)/(10-2)=0.375 of the way from r_min to r_max.
    """
    radii = major_plaza_radii([_spec(1, 100.0), _spec(2, 25.0), _spec(3, 4.0)])
    assert radii[1] == pytest.approx(DEFAULT_R_PLAZA_MAJOR_MAX_UNITS)
    assert radii[3] == pytest.approx(DEFAULT_R_PLAZA_MAJOR_MIN_UNITS)
    expected_mid = DEFAULT_R_PLAZA_MAJOR_MIN_UNITS + 0.375 * (
        DEFAULT_R_PLAZA_MAJOR_MAX_UNITS - DEFAULT_R_PLAZA_MAJOR_MIN_UNITS
    )
    assert radii[2] == pytest.approx(expected_mid)
    # Radius ordering follows mass ordering.
    assert radii[1] > radii[2] > radii[3]


def test_major_plaza_radii_non_major_gets_no_entry() -> None:
    """Town-center nuclei are unaffected: no radius entry at all."""
    radii = major_plaza_radii(
        [_spec(1, 100.0), _spec(2, 25.0), _spec(3, 900.0, is_major=False)]
    )
    assert set(radii) == {1, 2}
    # ...and the non-major's (huge) mass doesn't perturb the major scale.
    assert radii[1] == pytest.approx(DEFAULT_R_PLAZA_MAJOR_MAX_UNITS)
    assert radii[2] == pytest.approx(DEFAULT_R_PLAZA_MAJOR_MIN_UNITS)


def test_major_plaza_radii_degenerate_single_or_equal_masses_get_r_max() -> None:
    """A single major (or all-equal masses) is 'the largest' -> r_max."""
    assert major_plaza_radii([_spec(1, 42.0)]) == {1: DEFAULT_R_PLAZA_MAJOR_MAX_UNITS}
    equal = major_plaza_radii([_spec(1, 7.0), _spec(2, 7.0)])
    assert equal == {
        1: DEFAULT_R_PLAZA_MAJOR_MAX_UNITS,
        2: DEFAULT_R_PLAZA_MAJOR_MAX_UNITS,
    }
    assert major_plaza_radii([]) == {}


def test_major_plaza_radii_custom_bounds() -> None:
    """r_min/r_max are honored as the exact interpolation endpoints."""
    radii = major_plaza_radii([_spec(1, 100.0), _spec(2, 4.0)], r_min=1.0, r_max=2.0)
    assert radii == {1: pytest.approx(2.0), 2: pytest.approx(1.0)}


def test_add_intra_nucleus_avenues_uses_mass_scaled_radii() -> None:
    """The built plaza rings sit at the per-major mass-scaled radius."""
    big_core = sg.box(-10.0, -10.0, 10.0, 10.0)
    small_core = sg.box(90.0, -10.0, 110.0, 10.0)
    big = NucleusSpec(
        anchor=(0.0, 0.0),
        polygon=big_core,
        mass=100.0,
        rank=1,
        label="metro",
        influence_radius=14.14,
        is_major=True,
    )
    small = NucleusSpec(
        anchor=(100.0, 0.0),
        polygon=small_core,
        mass=4.0,
        rank=2,
        label="second",
        influence_radius=14.14,
        is_major=True,
    )
    lines = [
        sg.LineString([(20.0, 0.0), (10.0, 0.0)]),  # station on the big core ring.
        sg.LineString([(80.0, 0.0), (90.0, 0.0)]),  # station on the small core ring.
    ]
    spoke_lines, _spoke_edges, plaza_polys = add_intra_nucleus_avenues(
        [big, small], lines
    )
    assert len(plaza_polys) == 2
    assert len(spoke_lines) == 2
    # Plaza ring radius == distance from the anchor to any exterior vertex;
    # nuclei order is rank-ascending, so plaza_polys[0] is the big major.
    r_big = sg.Point(0.0, 0.0).distance(
        sg.Point(list(plaza_polys[0].exterior.coords)[0])
    )
    r_small = sg.Point(100.0, 0.0).distance(
        sg.Point(list(plaza_polys[1].exterior.coords)[0])
    )
    assert r_big == pytest.approx(DEFAULT_R_PLAZA_MAJOR_MAX_UNITS)
    assert r_small == pytest.approx(DEFAULT_R_PLAZA_MAJOR_MIN_UNITS)
    # Spokes terminate ON their own plaza ring (the S3 exact-fusion invariant
    # holds at mass-scaled radii too).
    assert plaza_polys[0].exterior.distance(sg.Point(spoke_lines[0].coords[-1])) < 1e-9
    assert plaza_polys[1].exterior.distance(sg.Point(spoke_lines[1].coords[-1])) < 1e-9


def test_polygonize_macro_blocks_protecting_nuclei_no_protect_matches_plain() -> None:
    """No protected polygons -> byte-identical to ``polygonize_districts``."""
    line = sg.LineString([(0.0, 30.0), (60.0, 30.0)])
    boundary = sg.box(0.0, 0.0, 60.0, 60.0)
    expected = polygonize_macro_blocks([line], boundary, min_block_area=60.0)
    got = _polygonize_macro_blocks_protecting_nuclei([line], boundary, 60.0, [])
    assert len(got) == len(expected)
    for g, e in zip(got, expected, strict=True):
        assert g.equals(e)


def test_polygonize_macro_blocks_protecting_nuclei_keeps_small_protected_faces() -> (
    None
):
    """A tiny face inside ``protect_polys`` survives the coarse merge floor.

    A small closed ring (a self-contained boundary a dangling line can't
    provide -- ``polygonize`` only closes loops) split by one chord into two
    9-unit^2 halves, both far below the 60-unit^2 merge floor, sitting
    entirely inside a matching ``protect_polys`` entry (the S3 stand-in for a
    major nucleus's core polygon).
    """
    boundary = sg.box(0.0, 0.0, 60.0, 60.0)
    ring_box = sg.box(20.0, 20.0, 26.0, 23.0)  # 6x3 = 18 unit^2.
    ring = sg.LineString(list(ring_box.exterior.coords))
    chord = sg.LineString([(23.0, 20.0), (23.0, 23.0)])  # splits it into two 9s.
    blocks = _polygonize_macro_blocks_protecting_nuclei(
        [ring, chord], boundary, 60.0, [ring_box]
    )
    small_faces = [b for b in blocks if b.area < 15.0]
    assert len(small_faces) == 2
    assert all(b.area == pytest.approx(9.0) for b in small_faces)
    assert sum(b.area for b in blocks) == pytest.approx(boundary.area, rel=1e-6)


def _downtown_fixture() -> tuple[
    np.ndarray,
    list[MacroNode],
    list[sg.LineString],
    list[MacroEdge],
    sg.Polygon,
    pl.DataFrame,
]:
    """One dense blob + 6 radial arterial spokes-in, 60 degrees apart.

    Reuses ``_two_peak_raster`` with ``sep_cells=0`` (both blobs coincide) as
    a convenient single-peak raster. The 6 arterials approach from outside
    radius 20 and terminate AT the blob center (well inside the eventual
    core), so ``clip_arterials_to_cores`` leaves exactly one outer segment
    (and one ring station) per line -- evenly spaced 60 degrees apart, well
    past the default 20-degree dedup spacing.
    """
    density = _two_peak_raster(sep_cells=0)
    cx, cy = 30.5, 30.5
    nodes = [MacroNode(x=cx, y=cy, sigma=TOWN_SIGMA, kind="town", label="downtown")]
    lines: list[sg.LineString] = []
    edges: list[MacroEdge] = []
    for i in range(6):
        ang = np.radians(60.0 * i)
        outer = (cx + 20.0 * np.cos(ang), cy + 20.0 * np.sin(ang))
        line = sg.LineString([outer, (cx, cy)])
        lines.append(line)
        edges.append(
            MacroEdge(
                node_a=0,
                node_b=-2,
                tau=2,
                tier=2,
                path_cost=round(line.length, 4),
                length=round(line.length, 3),
            )
        )
    boundary = sg.box(0.0, 0.0, 60.0, 60.0)
    points = pl.DataFrame(
        {"x": pl.Series([], dtype=pl.Float64), "y": pl.Series([], dtype=pl.Float64)}
    )
    return density, nodes, lines, edges, boundary, points


def test_build_macro_blocks_with_cores_downtown_wedges_and_plaza() -> None:
    """End-to-end S3 wiring: a downtown polygonizes into wedges + a plaza."""
    density, nodes, lines, edges, boundary, points = _downtown_fixture()
    bundle = build_macro_blocks_with_cores(
        density,
        nodes,
        lines,
        edges,
        boundary,
        points,
        x0=0.0,
        y0=0.0,
        cell=1.0,
        core_max_radius_units=6.0,
        core_min_area_units2=4.0,
        n_major_nuclei=1,
        # The fixture core (a sigma=3-cell blob at core_frac=0.45) has only a
        # ~3.8-unit radius -- smaller than the production mass-scaled major
        # plaza radii (3.0-4.0 u, slice P), which would swallow most ring
        # stations. Pin the S3-era radius here: these tests cover the S3
        # wedge/plaza WIRING, not the slice-P radius policy (covered by the
        # major_plaza_radii tests above).
        r_plaza_major_min=0.85,
        r_plaza_major_max=0.85,
    )
    assert len(bundle.core_polys) == 1
    assert len(bundle.nuclei) == 1
    assert bundle.nuclei[0].is_major
    assert len(bundle.plaza_polys) == 1

    spoke_edges = [e for e in bundle.clipped_edges if e.node_a == SPOKE_NODE_SENTINEL]
    assert len(spoke_edges) == 6
    assert all(e.tier == 1 for e in spoke_edges)

    # Fusion invariant: the block partition still exactly tiles the island
    # (no gap, no double-covered area) despite the new wedge/plaza faces.
    assert sum(b.area for b in bundle.blocks) == pytest.approx(boundary.area, rel=1e-6)
    for i, a in enumerate(bundle.blocks):
        for b in bundle.blocks[i + 1 :]:
            assert a.intersection(b).area < 1e-6

    # At least the plaza + the 6 wedges are present as distinct small blocks
    # (not dissolved back into one ring-fenced blob by the coarse
    # min_block_area=60 floor -- this is the whole point of the S3 protect
    # mechanism).
    core = bundle.core_polys[0]
    inside_core = [b for b in bundle.blocks if core.buffer(1e-6).contains(b)]
    assert len(inside_core) == 7  # 6 wedges + 1 plaza.
    plaza_area = bundle.plaza_polys[0].area
    assert any(abs(b.area - plaza_area) < 1e-6 for b in inside_core)


def test_spokes_and_plaza_ring_gate_snap_as_arterial_and_ring() -> None:
    """Fusion invariant: spokes/plaza are generic ``snap_gates_to_macro`` input.

    No fusion code changes were made for S3 -- ``snap_gates_to_macro`` is
    generic over ``arterial_lines + ring_lines``, so a gate sitting on a
    spoke or on the plaza ring should snap exactly like any other arterial/
    ring T-junction.
    """
    density, nodes, lines, edges, boundary, points = _downtown_fixture()
    bundle = build_macro_blocks_with_cores(
        density,
        nodes,
        lines,
        edges,
        boundary,
        points,
        x0=0.0,
        y0=0.0,
        cell=1.0,
        core_max_radius_units=6.0,
        core_min_area_units2=4.0,
        n_major_nuclei=1,
        # The fixture core (a sigma=3-cell blob at core_frac=0.45) has only a
        # ~3.8-unit radius -- smaller than the production mass-scaled major
        # plaza radii (3.0-4.0 u, slice P), which would swallow most ring
        # stations. Pin the S3-era radius here: these tests cover the S3
        # wedge/plaza WIRING, not the slice-P radius policy (covered by the
        # major_plaza_radii tests above).
        r_plaza_major_min=0.85,
        r_plaza_major_max=0.85,
    )
    spoke_idx = next(
        i for i, e in enumerate(bundle.clipped_edges) if e.node_a == SPOKE_NODE_SENTINEL
    )
    spoke_line = bundle.clipped_lines[spoke_idx]
    mid = spoke_line.interpolate(0.5, normalized=True)
    plaza_vertex = sg.Point(list(bundle.plaza_polys[0].exterior.coords)[0])

    gates_by_block = {
        0: [
            Gate(x=mid.x, y=mid.y, street_id=0, vertex_id=0),
            Gate(x=plaza_vertex.x, y=plaza_vertex.y, street_id=1, vertex_id=0),
        ]
    }
    junctions = snap_gates_to_macro(
        gates_by_block,
        bundle.clipped_lines,
        bundle.clipped_edges,
        bundle.ring_lines,
        boundary,
    )
    by_xy = {(round(j.x, 6), round(j.y, 6)): j for j in junctions}
    arterial_junction = by_xy[(round(mid.x, 6), round(mid.y, 6))]
    ring_junction = by_xy[(round(plaza_vertex.x, 6), round(plaza_vertex.y, 6))]
    assert arterial_junction.kind == "arterial"
    assert arterial_junction.tier == 1
    assert arterial_junction.distance < 1e-6
    assert ring_junction.kind == "ring"
    assert ring_junction.distance < 1e-6
