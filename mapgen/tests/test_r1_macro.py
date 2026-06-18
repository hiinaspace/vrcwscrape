"""Focused unit tests for the macro hierarchy pure helpers (r1_macro)."""

from __future__ import annotations

import numpy as np
import polars as pl

from mapgen.r1_macro import (
    CITY_SIGMA,
    TOWN_SIGMA,
    VILLAGE_SIGMA,
    MacroNode,
    build_density_cost_field,
    cluster_centroids,
    compute_macro_arterials,
    grade_cities,
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
