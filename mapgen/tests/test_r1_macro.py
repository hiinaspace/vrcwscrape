"""Focused unit tests for the macro hierarchy pure helpers (r1_macro)."""

from __future__ import annotations

import numpy as np
import polars as pl
import shapely.geometry as sg

from mapgen.r1_arm_a import IslandFields
from mapgen.r1_macro import (
    CITY_SIGMA,
    TOWN_SIGMA,
    VILLAGE_SIGMA,
    MacroEdge,
    MacroNode,
    MacroParams,
    _delaunay_edges,
    _nearest_neighbor_chain,
    build_density_cost_field,
    build_macro_layer,
    clip_arterials_to_cores,
    cluster_centroids,
    compute_macro_arterials,
    detect_core_regions,
    grade_cities,
    polygonize_macro_blocks,
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
    # few); positions are distinct.
    assert sum(1 for nd in layer.nodes if nd.kind == "city") == 2
    assert sum(1 for nd in layer.nodes if nd.kind == "town") == 4
    assert len({(nd.x, nd.y) for nd in layer.nodes}) == len(layer.nodes)
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
