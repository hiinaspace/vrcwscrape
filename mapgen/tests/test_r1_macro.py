"""Focused unit tests for the macro hierarchy pure helpers (r1_macro)."""

from __future__ import annotations

import numpy as np
import polars as pl

from mapgen.r1_macro import (
    CITY_SIGMA,
    TOWN_SIGMA,
    MacroNode,
    build_density_cost_field,
    compute_macro_arterials,
    tier_for_tau,
    town_nodes_from_peaks,
    visit_weighted_centroids,
)


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
