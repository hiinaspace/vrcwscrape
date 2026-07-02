"""R1 stage 1 — macro hierarchy layer (Galin 2011 + Arm B).

Builds a *tiered* arterial road network and macro-blocks from the staged island
inputs, by EXTENDING Arm B (``r1_arm_b``) rather than rewriting it. This is the
"macro" half of the macro+micro hybrid described in
``docs/large-scale-growth-research.md`` (decisions locked 2026-06-18).

Coordinate convention: island frame, ``x = x0 + (col + 0.5) * cell`` (cell
centres), same as ``r1_arm_b`` / ``r1_inputs``. All GeoJSON outputs are in the
island frame (no geographic CRS).

Pipeline
--------
1. **Macro nodes** — Tier-1 "city" nodes are the *visit-weighted* mean position
   of each L0 cluster (importance ``sigma = 2``); Tier-2 "town" nodes are the
   Arm B density peaks (``find_density_peaks``) that are not already co-located
   with a city (importance ``sigma = 1``).
2. **Density-attracting cost field** — start from Arm B ``build_cost_field``
   (base + slope + river, inf outside mask) then *subtract* a normalized-density
   term so dense cells are cheaper to route through (arterials hug dense cores).
   Clamped to a small positive floor inside the mask; ``inf`` outside.
3. **Importance-typed network** — Delaunay over ALL nodes → least-cost geodesic
   path per edge (Arm B ``route_through_array`` machinery, reused via
   ``r1_arm_b`` private helpers) → beta-ratio pruning (Arm B ``_prune_edges``).
   Each kept edge carries ``tau = min(sigma_a, sigma_b)`` and a tier
   (2 = highway, 1 = major).
4. **Macro-blocks** — polygonize (union of kept arterials + boundary exterior)
   and merge slivers, reusing Arm B ``polygonize_districts``.

The module is deterministic: it depends only on the staged rasters/points and a
fixed RNG seed (``DEFAULT_SEED``); no stochastic steps are used, but the seed is
set so any future jitter stays reproducible.
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import polars as pl
import shapely.geometry as sg
from scipy.spatial import Delaunay, QhullError
from shapely.ops import unary_union
from skimage.graph import route_through_array

from mapgen.r1_arm_a import IslandFields
from mapgen.r1_arm_b import (
    _prune_edges,
    _rowcol_to_xy,
    _xy_to_rowcol,
    boundary_mask,
    build_cost_field,
    find_density_peaks,
    polygonize_districts,
)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_SEED: int = 1234

# Importance (sigma) per node kind.
#
# Two seeding modes exist:
#
# * The legacy graded-l0 mode (``build_macro_nodes``): cities are the top L0
#   centroids by mass (sigma=2), towns are density peaks / minor L0 (sigma=1).
# * The 3-tier SEMANTIC hierarchy (``build_macro_nodes_hierarchical``): cities
#   are the 7 L1 cluster centroids (sigma=2), towns are the 18 L0 cluster
#   centroids (sigma=1), villages are density peaks (sigma=0). NB on this island
#   the cluster hierarchy is INVERTED from naive expectation — L0 (18) is the
#   FINEST tier and L1 (7) is coarser sub-regions — so L1=city / L0=town gives
#   the finer macro structure.
CITY_SIGMA: int = 2
TOWN_SIGMA: int = 1
VILLAGE_SIGMA: int = 0

# Number of L0 centroids that stay Tier-1 "cities" (sigma=2), ranked by cluster
# mass (visit sum). The remaining centroids are demoted to Tier-2 "towns"
# (sigma=1) so the network shows a primary highway skeleton between the few big
# cities plus a denser web of major roads, instead of every centroid-centroid
# edge becoming a highway.
DEFAULT_N_CITIES: int = 6

# Drop a density-peak town within this radius (island units) of an existing
# city node, so peaks don't duplicate visit-weighted cluster centroids.
# Smaller than the original 6.0 so more genuine local cores survive as towns
# (which both adds major roads and breaks up oversized macro-blocks).
DEFAULT_MERGE_RADIUS: float = 4.0

# Peak detection for town seeds (looser than Arm B's arterial-probe defaults so
# more secondary cores become Tier-2 town nodes).
DEFAULT_PEAK_MIN_DISTANCE_UNITS: float = 7.0
DEFAULT_PEAK_THRESHOLD_FRAC: float = 0.02

# Density-attracting cost term: cost = base_cost - w_density * norm_density,
# clamped to DEFAULT_COST_FLOOR inside the mask. w_density defaults to ~0.8 of
# the Arm B base cost (1.0).
DEFAULT_W_DENSITY: float = 0.8
DEFAULT_COST_FLOOR: float = 0.05

# --- 3-tier semantic hierarchy defaults (build_macro_nodes_hierarchical) ---
#
# Villages are density peaks, LOOSENED so there are plenty of them (the L1/L0
# centroids already cover the major/minor cores, so villages are the fine grain
# that breaks up macro-blocks to neighborhood scale).
DEFAULT_VILLAGE_PEAK_MIN_DISTANCE_UNITS: float = 4.0
DEFAULT_VILLAGE_PEAK_THRESHOLD_FRAC: float = 0.012
# Drop a village peak within this radius of any city/town centroid.
DEFAULT_VILLAGE_MERGE_RADIUS: float = 3.0

# Beta-ratio pruning (Arm B convention; inf keeps all edges).
DEFAULT_BETA_RATIO: float = 1.2

# Arterial simplification tolerance (island units).
DEFAULT_SIMPLIFY_TOLERANCE: float = 1.5

# Macro-block sliver-merge floor (island units²). Larger than Arm B's district
# floor because macro-blocks are coarse by construction.
DEFAULT_MIN_BLOCK_AREA: float = 60.0

# --- Core ring-road defaults (stage 3.5b) ---
#
# A dense CITY/TOWN core is region-grown on the density raster and turned into a
# ringed "downtown" block: arterials are clipped to its exterior (T-junctions on
# the ring) instead of converging on the summit.
#
# core_frac: a cell joins the core if density >= core_frac * local_peak.
DEFAULT_CORE_FRAC: float = 0.45
# Cap the grown region at this radius (island units) from the seed so cores stay
# downtown-scale rather than swallowing a whole region.
DEFAULT_CORE_MAX_RADIUS_UNITS: float = 6.0
# Drop grown regions whose area (island units²) is below this floor.
DEFAULT_CORE_MIN_AREA_UNITS2: float = 8.0
# Half-width (island units) of the window used to find the robust local peak
# around a seed (so the node need not sit exactly on the summit).
DEFAULT_CORE_PEAK_WINDOW_UNITS: float = 3.0
# Boundary simplification tolerance (island units) for the extracted ring.
DEFAULT_CORE_SIMPLIFY_TOLERANCE: float = 1.0

NodeKind = Literal["city", "town", "village"]


# ---------------------------------------------------------------------------
# 0. Staged-input IO helpers (single shared implementation)
# ---------------------------------------------------------------------------
#
# ``load_boundary`` below and ``boundary_mask`` (re-exported from ``r1_arm_b``)
# are THE implementations for every r1 stage script (run_r1_macro,
# run_r1_seam_spike, run_r1_hybrid, run_r1_zoom_review). Do not copy them into
# scripts.


def load_boundary(inputs_dir: Path) -> sg.Polygon:
    """Load the island boundary Polygon, handling FC/Feature/geometry forms."""
    raw = json.loads((inputs_dir / "island_boundary.geojson").read_text())
    if raw.get("type") == "FeatureCollection":
        geometry = raw["features"][0]["geometry"]
    elif raw.get("type") == "Feature":
        geometry = raw["geometry"]
    else:
        geometry = raw
    geom = sg.shape(geometry)
    if not isinstance(geom, sg.Polygon):
        raise ValueError(f"island boundary must be a Polygon, got {geom.geom_type}")
    return geom


# ---------------------------------------------------------------------------
# 1. Macro nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MacroNode:
    """A weighted macro node: a city (L0 centroid) or a town (density peak)."""

    x: float
    y: float
    sigma: int
    kind: NodeKind
    label: str | None = None
    mass: float | None = None


def cluster_centroids(
    points: pl.DataFrame,
    id_col: str,
    name_col: str,
    sigma: int,
    kind: NodeKind,
) -> list[MacroNode]:
    """Return one visit-weighted centroid ``MacroNode`` per distinct cluster.

    Generic over the cluster tier: pass ``("l0_id", "l0_name")`` for L0 towns,
    ``("l1_id", "l1_name")`` for L1 cities, etc. The node position is the
    visit-weighted mean of each cluster's ``(x, y)``; ``mass`` is the cluster's
    total ``visits`` and ``label`` its ``name_col`` value. Clusters are emitted
    in ascending ``id_col`` order for determinism.

    A cluster whose visit weights sum to ``<= 0`` falls back to an unweighted
    mean position (guards against NaN centroids).

    Parameters
    ----------
    points : polars frame with at least ``id_col, name_col, visits, x, y``.
    id_col, name_col : the cluster id / human-name columns for this tier.
    sigma : importance assigned to every emitted node.
    kind : the ``NodeKind`` tag for every emitted node.
    """
    grouped = (
        points.with_columns(
            (pl.col("x") * pl.col("visits")).alias("_wx"),
            (pl.col("y") * pl.col("visits")).alias("_wy"),
        )
        .group_by(id_col)
        .agg(
            pl.col(name_col).first().alias("_name"),
            pl.col("visits").sum().alias("mass"),
            pl.col("_wx").sum().alias("wx"),
            pl.col("_wy").sum().alias("wy"),
            pl.col("x").mean().alias("x_mean"),
            pl.col("y").mean().alias("y_mean"),
        )
        .sort(id_col)
    )

    nodes: list[MacroNode] = []
    for row in grouped.iter_rows(named=True):
        mass = float(row["mass"])
        if mass > 0:
            cx = float(row["wx"]) / mass
            cy = float(row["wy"]) / mass
        else:
            cx = float(row["x_mean"])
            cy = float(row["y_mean"])
        nodes.append(
            MacroNode(
                x=cx,
                y=cy,
                sigma=sigma,
                kind=kind,
                label=str(row["_name"]),
                mass=mass,
            )
        )
    return nodes


def visit_weighted_centroids(
    points: pl.DataFrame,
) -> list[MacroNode]:
    """Return one Tier-1 city node per L0 cluster (visit-weighted centroid).

    Thin wrapper over :func:`cluster_centroids` for the legacy L0-as-city path
    (importance ``sigma = CITY_SIGMA``, ``kind = "city"``). The position is the
    visit-weighted mean of each cluster's ``(x, y)``; the ``mass`` is the
    cluster's total ``visits`` and ``label`` its ``l0_name``. Clusters are
    emitted in ascending ``l0_id`` order for determinism.

    Parameters
    ----------
    points : polars frame with columns ``l0_id, l0_name, visits, x, y``.
    """
    return cluster_centroids(points, "l0_id", "l0_name", CITY_SIGMA, "city")


def town_nodes_from_peaks(
    density: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    cities: list[MacroNode],
    *,
    merge_radius: float = DEFAULT_MERGE_RADIUS,
    peak_min_distance_units: float = DEFAULT_PEAK_MIN_DISTANCE_UNITS,
    peak_threshold_frac: float = DEFAULT_PEAK_THRESHOLD_FRAC,
) -> list[MacroNode]:
    """Return Tier-2 town nodes: density peaks not co-located with a city.

    Peaks are found with Arm B ``find_density_peaks``; any peak within
    ``merge_radius`` island units of an already-added city centroid is dropped
    so cities and towns don't duplicate.
    """
    peak_xs, peak_ys, peak_ds = find_density_peaks(
        density,
        x0,
        y0,
        cell,
        min_distance_units=peak_min_distance_units,
        threshold_frac=peak_threshold_frac,
    )

    if cities:
        city_xy = np.array([(c.x, c.y) for c in cities], dtype=float)
    else:
        city_xy = np.empty((0, 2), dtype=float)

    r2 = merge_radius * merge_radius
    towns: list[MacroNode] = []
    for px, py, pd in zip(
        peak_xs.tolist(), peak_ys.tolist(), peak_ds.tolist(), strict=True
    ):
        if city_xy.shape[0] > 0:
            d2 = (city_xy[:, 0] - px) ** 2 + (city_xy[:, 1] - py) ** 2
            if float(d2.min()) <= r2:
                continue
        towns.append(
            MacroNode(
                x=float(px), y=float(py), sigma=TOWN_SIGMA, kind="town", mass=float(pd)
            )
        )
    return towns


def grade_cities(
    cities: list[MacroNode], n_cities: int = DEFAULT_N_CITIES
) -> list[MacroNode]:
    """Keep the top ``n_cities`` centroids (by mass) as cities; demote the rest.

    Ranks city nodes by ``mass`` (cluster visit sum) descending; the top
    ``n_cities`` stay Tier-1 (``sigma=2``, ``kind="city"``) and the remainder are
    demoted to Tier-2 towns (``sigma=1``, ``kind="town"``), keeping their label
    and mass. Original list order is preserved for determinism. This is what
    spreads edges across the highway/major tiers instead of making every
    centroid-centroid edge a highway.
    """
    if n_cities >= len(cities):
        return cities
    ranked = sorted(
        range(len(cities)),
        key=lambda i: (cities[i].mass or 0.0, -i),
        reverse=True,
    )
    keep = set(ranked[:n_cities])
    graded: list[MacroNode] = []
    for i, c in enumerate(cities):
        if i in keep:
            graded.append(c)
        else:
            graded.append(
                MacroNode(
                    x=c.x,
                    y=c.y,
                    sigma=TOWN_SIGMA,
                    kind="town",
                    label=c.label,
                    mass=c.mass,
                )
            )
    return graded


def build_macro_nodes(
    density: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    points: pl.DataFrame,
    *,
    n_cities: int = DEFAULT_N_CITIES,
    merge_radius: float = DEFAULT_MERGE_RADIUS,
    peak_min_distance_units: float = DEFAULT_PEAK_MIN_DISTANCE_UNITS,
    peak_threshold_frac: float = DEFAULT_PEAK_THRESHOLD_FRAC,
) -> list[MacroNode]:
    """Build the full macro node list: graded cities first, then deduped towns."""
    cities = grade_cities(visit_weighted_centroids(points), n_cities=n_cities)
    towns = town_nodes_from_peaks(
        density,
        x0,
        y0,
        cell,
        cities,
        merge_radius=merge_radius,
        peak_min_distance_units=peak_min_distance_units,
        peak_threshold_frac=peak_threshold_frac,
    )
    return [*cities, *towns]


def village_nodes_from_peaks(
    density: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    anchors: list[MacroNode],
    *,
    merge_radius: float = DEFAULT_VILLAGE_MERGE_RADIUS,
    peak_min_distance_units: float = DEFAULT_VILLAGE_PEAK_MIN_DISTANCE_UNITS,
    peak_threshold_frac: float = DEFAULT_VILLAGE_PEAK_THRESHOLD_FRAC,
) -> list[MacroNode]:
    """Return Tier-3 village nodes (``sigma=0``): density peaks not near anchors.

    Peaks are found with Arm B ``find_density_peaks`` using loosened parameters
    (so there are plenty of villages); any peak within ``merge_radius`` island
    units of an ``anchors`` node (the city/town centroids) is dropped so
    villages don't duplicate the higher tiers.
    """
    peak_xs, peak_ys, peak_ds = find_density_peaks(
        density,
        x0,
        y0,
        cell,
        min_distance_units=peak_min_distance_units,
        threshold_frac=peak_threshold_frac,
    )

    if anchors:
        anchor_xy = np.array([(a.x, a.y) for a in anchors], dtype=float)
    else:
        anchor_xy = np.empty((0, 2), dtype=float)

    r2 = merge_radius * merge_radius
    villages: list[MacroNode] = []
    for px, py, pd in zip(
        peak_xs.tolist(), peak_ys.tolist(), peak_ds.tolist(), strict=True
    ):
        if anchor_xy.shape[0] > 0:
            d2 = (anchor_xy[:, 0] - px) ** 2 + (anchor_xy[:, 1] - py) ** 2
            if float(d2.min()) <= r2:
                continue
        villages.append(
            MacroNode(
                x=float(px),
                y=float(py),
                sigma=VILLAGE_SIGMA,
                kind="village",
                mass=float(pd),
            )
        )
    return villages


def build_macro_nodes_hierarchical(
    density: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    points: pl.DataFrame,
    *,
    merge_radius: float = DEFAULT_VILLAGE_MERGE_RADIUS,
    peak_min_distance_units: float = DEFAULT_VILLAGE_PEAK_MIN_DISTANCE_UNITS,
    peak_threshold_frac: float = DEFAULT_VILLAGE_PEAK_THRESHOLD_FRAC,
) -> list[MacroNode]:
    """Build the 3-tier SEMANTIC macro node hierarchy (L1 / L0 / peaks).

    - **Cities (sigma=2):** the 7 L1 cluster centroids (visit-weighted).
    - **Towns (sigma=1):** the 18 L0 cluster centroids (visit-weighted).
    - **Villages (sigma=0):** loosened density peaks, deduped against any
      city/town within ``merge_radius``.

    NB: on this island the cluster hierarchy is INVERTED — L0 (18) is the finest
    tier and L1 (7) is coarser sub-regions — so using L1 as cities and L0 as
    towns yields finer macro structure (more nodes -> finer macro-blocks) than
    the legacy graded-L0 seeding.

    Nodes are returned cities-then-towns-then-villages (each block in its own
    deterministic cluster-id / peak order).
    """
    cities = cluster_centroids(points, "l1_id", "l1_name", CITY_SIGMA, "city")
    towns = cluster_centroids(points, "l0_id", "l0_name", TOWN_SIGMA, "town")
    villages = village_nodes_from_peaks(
        density,
        x0,
        y0,
        cell,
        [*cities, *towns],
        merge_radius=merge_radius,
        peak_min_distance_units=peak_min_distance_units,
        peak_threshold_frac=peak_threshold_frac,
    )
    return [*cities, *towns, *villages]


# ---------------------------------------------------------------------------
# 2. Density-attracting cost field
# ---------------------------------------------------------------------------


def build_density_cost_field(
    slope: np.ndarray,
    flow_accum: np.ndarray,
    density: np.ndarray,
    mask: np.ndarray,
    *,
    base: float = 1.0,
    w_slope: float = 8.0,
    w_river: float = 6.0,
    w_density: float = DEFAULT_W_DENSITY,
    cost_floor: float = DEFAULT_COST_FLOOR,
) -> np.ndarray:
    """Density-ATTRACTING cost field for macro arterial routing.

    Start from Arm B ``build_cost_field`` (base + slope + river; inf outside
    mask), then *subtract* a normalized-density term so high-density cells are
    cheaper and arterials hug dense ridges / run toward cores::

        cost = base_cost - w_density * norm_density_inside

    The result is clamped to ``cost_floor`` (a small positive value) inside the
    mask so it remains a valid positive routing cost, and kept ``inf`` outside.

    ``norm_density`` is min/max-normalized over inside-mask cells only.
    """
    base_cost = build_cost_field(
        slope, flow_accum, mask, base=base, w_slope=w_slope, w_river=w_river
    )

    # Normalize density to [0, 1] over inside-mask cells only.
    dens_inside = density[mask]
    if dens_inside.size > 0:
        dmin = float(dens_inside.min())
        dmax = float(dens_inside.max())
    else:
        dmin, dmax = 0.0, 1.0
    span = dmax - dmin
    if span <= 0:
        span = 1.0
    norm_density = np.clip((density.astype(np.float64) - dmin) / span, 0.0, 1.0)

    cost = base_cost - w_density * norm_density

    # Clamp inside-mask cells to the positive floor; keep inf outside.
    finite = np.isfinite(cost)
    cost[finite] = np.maximum(cost[finite], cost_floor)
    cost[~mask] = np.inf
    return cost.astype(np.float64)


# ---------------------------------------------------------------------------
# 3. Importance-typed network
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MacroEdge:
    """A kept arterial edge between two macro nodes."""

    node_a: int
    node_b: int
    tau: int  # min(sigma_a, sigma_b)
    tier: int  # construction level: 2 = highway, 1 = major, 0 = local
    path_cost: float
    length: float


def tier_for_tau(tau: int) -> int:
    """Map an edge ``tau = min(sigma)`` onto a tier label.

    With sigma in {1, 2}, tau in {1, 2}: tau == 2 -> highway (2), else major (1).

    Note: the network is built *per level* (see ``compute_macro_arterials``), so
    an edge's tier is its construction level, not necessarily ``tier_for_tau`` of
    its endpoints. This helper is retained for the nominal tau→tier mapping.
    """
    return 2 if tau >= 2 else 1


def _pair(a: int, b: int) -> tuple[int, int]:
    """Canonical (sorted) node-index pair."""
    return (a, b) if a <= b else (b, a)


def _nearest_neighbor_chain(
    indices: list[int], xs: np.ndarray, ys: np.ndarray
) -> set[tuple[int, int]]:
    """Deterministic nearest-neighbour chain over a degenerate node subset.

    Last-resort fallback when even joggled Delaunay fails (e.g. all nodes
    coincident): starting from the first node of ``indices``, repeatedly
    connect the current node to its nearest unvisited node (ties broken by
    position in ``indices``, which is deterministic). Returns a connected chain
    of ``m - 1`` edges.
    """
    m = len(indices)
    if m < 2:
        return set()
    remaining = [int(i) for i in indices[1:]]
    current = int(indices[0])
    edges: set[tuple[int, int]] = set()
    while remaining:
        rem = np.array(remaining, dtype=int)
        d2 = (xs[rem] - xs[current]) ** 2 + (ys[rem] - ys[current]) ** 2
        best = int(rem[int(np.argmin(d2))])  # argmin: first minimum wins
        edges.add(_pair(current, best))
        remaining.remove(best)
        current = best
    return edges


def _delaunay_edges(
    indices: list[int], xs: np.ndarray, ys: np.ndarray
) -> set[tuple[int, int]]:
    """Delaunay edge set (global node-index pairs) over a node subset.

    Degenerate subsets are handled deterministically: ``m < 2`` -> empty,
    ``m == 2`` -> the single pair, >=3 collinear nodes (QhullError: "initial
    simplex is flat") -> retry with joggled input (``QJ``), and if even that
    fails (e.g. coincident nodes) -> a nearest-neighbour chain. Per-sigma-level
    subsets are routinely tiny (3-4 nodes), so exact collinearity happens on
    real islands.
    """
    m = len(indices)
    if m < 2:
        return set()

    if m == 2:
        return {_pair(int(indices[0]), int(indices[1]))}
    sub = np.column_stack([xs[indices], ys[indices]])
    try:
        tri = Delaunay(sub)
    except QhullError:
        try:
            tri = Delaunay(sub, qhull_options="QJ")
        except QhullError:
            return _nearest_neighbor_chain(indices, xs, ys)
    edges: set[tuple[int, int]] = set()
    for simplex in tri.simplices:
        for i in range(3):
            a = int(indices[int(simplex[i])])
            b = int(indices[int(simplex[(i + 1) % 3])])
            edges.add(_pair(a, b))
    return edges


def compute_macro_arterials(
    nodes: list[MacroNode],
    cost: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    *,
    beta_ratio: float = DEFAULT_BETA_RATIO,
    simplify_tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE,
) -> tuple[list[sg.LineString], list[MacroEdge]]:
    """Build the importance-typed arterial network *per level* (Galin 2011).

    Generalized to N levels: one network per distinct sigma present, highest
    first. For each level ``L`` (descending distinct sigma) a Delaunay is built
    over the nodes with ``sigma >= L``, geodesic least-cost paths are routed
    (Arm B ``route_through_array``), the level is beta-ratio pruned (Arm B
    ``_prune_edges``) independently, and node-pairs already realized (i.e. kept
    after pruning) at a higher level are excluded — a pair PRUNED at a higher
    level stays eligible at lower levels. Edge ``tier`` is its construction
    level ``L``; ``tau`` records ``min(sigma_a, sigma_b)``.

    For semantic sigmas ``{0, 1, 2}`` this yields three tiers:

    - **Highways (tier 2):** network over cities (sigma>=2).
    - **Majors (tier 1):** network over cities+towns (sigma>=1) minus highways.
    - **Locals (tier 0):** network over all nodes (sigma>=0) minus the above.

    With only sigmas ``{1, 2}`` present (legacy graded-L0 seeding) it reduces to
    the previous 2-tier behavior (highways = city subset; majors = all minus
    highways), so existing tests stay green.

    Returns the kept LineStrings and parallel ``MacroEdge`` records.
    """
    nrows, ncols = cost.shape
    n = len(nodes)
    if n < 2:
        return [], []

    xs = np.array([nd.x for nd in nodes], dtype=float)
    ys = np.array([nd.y for nd in nodes], dtype=float)
    sigmas = [nd.sigma for nd in nodes]
    rows, cols = _xy_to_rowcol(xs, ys, x0, y0, cell, nrows, ncols)

    # One level per distinct sigma, highest first. The Delaunay at level L spans
    # all nodes with sigma >= L; pairs realized at a higher level are excluded.
    # Only pairs that SURVIVE beta pruning count as realized — a pair pruned at
    # a higher level stays eligible at lower levels (docstring contract).
    distinct_levels = sorted({int(s) for s in sigmas}, reverse=True)
    realized: set[tuple[int, int]] = set()

    lines: list[sg.LineString] = []
    records: list[MacroEdge] = []

    # Build each level independently (highest tier first so it sorts ahead).
    for tier in distinct_levels:
        subset = [i for i in range(n) if sigmas[i] >= tier]
        edge_list = sorted(_delaunay_edges(subset, xs, ys) - realized)
        if not edge_list:
            continue
        edge_paths: list[list[tuple[int, int]]] = []
        edge_costs: list[float] = []
        for i_a, i_b in edge_list:
            ra, ca = int(rows[i_a]), int(cols[i_a])
            rb, cb = int(rows[i_b]), int(cols[i_b])
            path, path_cost = route_through_array(
                cost, (ra, ca), (rb, cb), fully_connected=True, geometric=True
            )
            edge_paths.append(path)
            edge_costs.append(float(path_cost))

        kept = _prune_edges(edge_list, edge_costs, beta_ratio)

        for idx, (i_a, i_b) in enumerate(edge_list):
            if not kept[idx]:
                continue
            realized.add((i_a, i_b))
            path = edge_paths[idx]
            rows_p = np.array([p[0] for p in path])
            cols_p = np.array([p[1] for p in path])
            xs_p, ys_p = _rowcol_to_xy(rows_p, cols_p, x0, y0, cell)
            coords = list(zip(xs_p.tolist(), ys_p.tolist(), strict=True))
            if len(coords) < 2:
                continue

            line = sg.LineString(coords)
            if simplify_tolerance > 0:
                line = line.simplify(simplify_tolerance, preserve_topology=False)
            if line.is_empty or line.length < cell:
                continue

            lines.append(line)
            records.append(
                MacroEdge(
                    node_a=i_a,
                    node_b=i_b,
                    tau=min(sigmas[i_a], sigmas[i_b]),
                    tier=tier,
                    path_cost=round(edge_costs[idx], 4),
                    length=round(float(line.length), 3),
                )
            )

    return lines, records


# ---------------------------------------------------------------------------
# 4. Macro-blocks
# ---------------------------------------------------------------------------


def polygonize_macro_blocks(
    arterial_lines: list[sg.LineString],
    boundary: sg.Polygon,
    *,
    min_block_area: float = DEFAULT_MIN_BLOCK_AREA,
) -> list[sg.Polygon]:
    """Polygonize arterials + boundary into macro-blocks (Arm B reuse).

    Thin wrapper over Arm B ``polygonize_districts`` (union of arterials +
    boundary exterior → ``shapely.ops.polygonize`` → sliver merge), with a
    macro-scale area floor.
    """
    return polygonize_districts(
        arterial_lines, boundary, min_district_area=min_block_area
    )


# ---------------------------------------------------------------------------
# 5. Core ring-roads (stage 3.5b)
# ---------------------------------------------------------------------------


def _grow_core_region(
    density: np.ndarray,
    seed_rc: tuple[int, int],
    *,
    core_frac: float,
    peak_window_cells: int,
    max_radius_cells: float,
) -> set[tuple[int, int]]:
    """Region-grow a connected dense core on the density raster.

    Flood-fills (8-connectivity) outward from ``seed_rc`` over cells whose
    density is ``>= core_frac * local_peak``, where ``local_peak`` is the maximum
    density within a ``peak_window_cells`` window around the seed (robust to the
    seed not sitting exactly on the summit). Growth is capped at
    ``max_radius_cells`` (Euclidean, in cells) from the seed so cores stay
    downtown-scale.

    Returns the set of ``(row, col)`` cells in the region (always includes the
    seed).
    """
    nrows, ncols = density.shape
    sr, sc = seed_rc
    sr = int(np.clip(sr, 0, nrows - 1))
    sc = int(np.clip(sc, 0, ncols - 1))

    r0 = max(0, sr - peak_window_cells)
    r1 = min(nrows, sr + peak_window_cells + 1)
    c0 = max(0, sc - peak_window_cells)
    c1 = min(ncols, sc + peak_window_cells + 1)
    window = density[r0:r1, c0:c1]
    local_peak = float(window.max()) if window.size else float(density[sr, sc])
    if local_peak <= 0.0:
        return {(sr, sc)}

    threshold = core_frac * local_peak
    max_r2 = max_radius_cells * max_radius_cells

    region: set[tuple[int, int]] = {(sr, sc)}
    queue: deque[tuple[int, int]] = deque([(sr, sc)])
    while queue:
        r, c = queue.popleft()
        for dr in (-1, 0, 1):
            for dc in (-1, 0, 1):
                if dr == 0 and dc == 0:
                    continue
                nr, nc = r + dr, c + dc
                if not (0 <= nr < nrows and 0 <= nc < ncols):
                    continue
                if (nr, nc) in region:
                    continue
                if float(density[nr, nc]) < threshold:
                    continue
                if (nr - sr) ** 2 + (nc - sc) ** 2 > max_r2:
                    continue
                region.add((nr, nc))
                queue.append((nr, nc))
    return region


def _cells_to_polygon(
    cells: set[tuple[int, int]],
    x0: float,
    y0: float,
    cell: float,
    *,
    simplify_tolerance: float,
) -> sg.Polygon | None:
    """Union the cell squares of ``cells`` into a single simplified core polygon.

    Each ``(row, col)`` becomes its cell-centre-aligned square box; the union's
    largest exterior ring is taken (interior holes dropped) and simplified. Cell
    boxes span ``[x0 + col*cell, x0 + (col+1)*cell]`` etc., consistent with the
    cell-centre convention ``x = x0 + (col + 0.5) * cell``.
    """
    if not cells:
        return None
    boxes = [
        sg.box(
            x0 + c * cell,
            y0 + r * cell,
            x0 + (c + 1) * cell,
            y0 + (r + 1) * cell,
        )
        for (r, c) in cells
    ]
    merged = unary_union(boxes)
    if merged.is_empty:
        return None
    if merged.geom_type == "MultiPolygon":
        merged = max(merged.geoms, key=lambda p: p.area)
    if merged.geom_type != "Polygon":
        return None
    # Drop holes: keep only the exterior ring.
    poly = sg.Polygon(merged.exterior)
    if simplify_tolerance > 0:
        poly = poly.simplify(simplify_tolerance, preserve_topology=True)
    if poly.is_empty or not poly.is_valid:
        poly = poly.buffer(0)
    if poly.is_empty or poly.geom_type != "Polygon":
        return None
    return poly


def detect_core_regions(
    density: np.ndarray,
    nodes: list[MacroNode],
    x0: float,
    y0: float,
    cell: float,
    *,
    core_frac: float = DEFAULT_CORE_FRAC,
    core_max_radius_units: float = DEFAULT_CORE_MAX_RADIUS_UNITS,
    core_min_area_units2: float = DEFAULT_CORE_MIN_AREA_UNITS2,
    peak_window_units: float = DEFAULT_CORE_PEAK_WINDOW_UNITS,
    simplify_tolerance: float = DEFAULT_CORE_SIMPLIFY_TOLERANCE,
) -> list[sg.Polygon]:
    """Detect dense "downtown" core regions around CITY/TOWN nodes.

    For each node with ``sigma >= 1`` (cities and towns; villages skipped), a
    connected dense region is grown on ``density`` from the node's nearest cell
    (see :func:`_grow_core_region`). Regions whose island-frame area is below
    ``core_min_area_units2`` are dropped. Overlapping or touching regions from
    different nodes are UNIONED into a single core (so two nearby cores share one
    downtown block). Each surviving core is returned as a simplified shapely
    Polygon (largest ring only), in descending area order.

    Pure/deterministic: depends only on the raster, node positions and params.
    """
    max_radius_cells = core_max_radius_units / cell
    peak_window_cells = max(1, int(round(peak_window_units / cell)))
    cell_area = cell * cell
    min_cells = core_min_area_units2 / cell_area if cell_area > 0 else 0.0

    nrows, ncols = density.shape
    regions: list[set[tuple[int, int]]] = []
    for nd in nodes:
        if nd.sigma < 1:
            continue
        rows, cols = _xy_to_rowcol(
            np.array([nd.x]), np.array([nd.y]), x0, y0, cell, nrows, ncols
        )
        seed = (int(rows[0]), int(cols[0]))
        region = _grow_core_region(
            density,
            seed,
            core_frac=core_frac,
            peak_window_cells=peak_window_cells,
            max_radius_cells=max_radius_cells,
        )
        if len(region) < min_cells:
            continue
        regions.append(region)

    # Union overlapping/touching cell-regions (8-connectivity) into single cores.
    merged_regions = _union_touching_regions(regions)

    polys: list[sg.Polygon] = []
    for region in merged_regions:
        poly = _cells_to_polygon(
            region, x0, y0, cell, simplify_tolerance=simplify_tolerance
        )
        if poly is None:
            continue
        if poly.area < core_min_area_units2:
            continue
        polys.append(poly)
    polys.sort(key=lambda p: p.area, reverse=True)
    return polys


def _union_touching_regions(
    regions: list[set[tuple[int, int]]],
) -> list[set[tuple[int, int]]]:
    """Merge cell-regions that overlap or touch (8-connectivity) into one set.

    Two regions are merged if any cell of one is within the 8-neighbourhood of
    any cell of the other (so adjacent or overlapping cores from different nodes
    become a single downtown block). Union-find over region indices.
    """
    n = len(regions)
    if n <= 1:
        return [set(r) for r in regions]

    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[rj] = ri

    # Map each cell (and its 8-neighbourhood) to the regions that claim it.
    expanded: list[set[tuple[int, int]]] = []
    for region in regions:
        exp: set[tuple[int, int]] = set()
        for r, c in region:
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    exp.add((r + dr, c + dc))
        expanded.append(exp)

    for i in range(n):
        for j in range(i + 1, n):
            if regions[i] & expanded[j]:
                union(i, j)

    grouped: dict[int, set[tuple[int, int]]] = {}
    for i in range(n):
        root = find(i)
        grouped.setdefault(root, set()).update(regions[i])
    return list(grouped.values())


def core_ring_boundaries(core_polys: list[sg.Polygon]) -> list[sg.LineString]:
    """Return each core polygon's exterior ring as a LineString (the ring road)."""
    rings: list[sg.LineString] = []
    for poly in core_polys:
        if poly.is_empty:
            continue
        rings.append(sg.LineString(list(poly.exterior.coords)))
    return rings


def clip_arterials_to_cores(
    lines: list[sg.LineString],
    edges: list[MacroEdge],
    core_polys: list[sg.Polygon],
) -> tuple[list[sg.LineString], list[MacroEdge]]:
    """Clip arterials to the exterior of dense cores, preserving tier/tau.

    Subtracts ``unary_union(core_polys)`` from each arterial LineString so
    arterials STOP at ring boundaries (forming T-junctions) instead of spiking
    to a core's summit; the segment between two cores stays intact. A single
    input arterial may split into several surviving segments — each is emitted as
    its own LineString carrying the SAME ``MacroEdge`` attributes (tier, tau,
    node ids; ``length`` updated to the surviving segment length).

    With no cores this is an identity pass (returns the inputs unchanged).
    """
    if not core_polys:
        return list(lines), list(edges)

    cores_union = unary_union(core_polys)
    out_lines: list[sg.LineString] = []
    out_edges: list[MacroEdge] = []
    for line, rec in zip(lines, edges, strict=True):
        remainder = line.difference(cores_union)
        if remainder.is_empty:
            continue
        if remainder.geom_type == "LineString":
            parts: list[sg.LineString] = [remainder]
        elif remainder.geom_type == "MultiLineString":
            parts = [g for g in remainder.geoms if g.geom_type == "LineString"]
        else:  # GeometryCollection or Point fragments — keep only line parts.
            parts = [
                g
                for g in getattr(remainder, "geoms", [])
                if g.geom_type == "LineString"
            ]
        for part in parts:
            if part.is_empty or part.length <= 0.0:
                continue
            out_lines.append(part)
            out_edges.append(
                MacroEdge(
                    node_a=rec.node_a,
                    node_b=rec.node_b,
                    tau=rec.tau,
                    tier=rec.tier,
                    path_cost=rec.path_cost,
                    length=round(float(part.length), 3),
                )
            )
    return out_lines, out_edges


@dataclass(frozen=True)
class MacroBlockBundle:
    """Result of :func:`build_macro_blocks_with_cores`.

    Attributes
    ----------
    core_polys : detected dense-core "downtown" polygons (ringed blocks).
    ring_lines : each core's exterior ring as a LineString (drawn as ring roads).
    clipped_lines / clipped_edges : arterials with their core interiors removed.
    blocks : polygonized macro-blocks over clipped arterials + rings + boundary.
    """

    core_polys: list[sg.Polygon]
    ring_lines: list[sg.LineString]
    clipped_lines: list[sg.LineString]
    clipped_edges: list[MacroEdge]
    blocks: list[sg.Polygon]


def build_macro_blocks_with_cores(
    density: np.ndarray,
    nodes: list[MacroNode],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    boundary: sg.Polygon,
    x0: float,
    y0: float,
    cell: float,
    *,
    core_frac: float = DEFAULT_CORE_FRAC,
    core_max_radius_units: float = DEFAULT_CORE_MAX_RADIUS_UNITS,
    core_min_area_units2: float = DEFAULT_CORE_MIN_AREA_UNITS2,
    peak_window_units: float = DEFAULT_CORE_PEAK_WINDOW_UNITS,
    core_simplify_tolerance: float = DEFAULT_CORE_SIMPLIFY_TOLERANCE,
    min_block_area: float = DEFAULT_MIN_BLOCK_AREA,
) -> MacroBlockBundle:
    """Fold dense cores into ringed downtown blocks (stage 3.5b wrapper).

    A NEW step on top of the unchanged :func:`compute_macro_arterials` output:

    1. Detect dense core regions around CITY/TOWN nodes
       (:func:`detect_core_regions`).
    2. Clip arterials to the cores' exterior (:func:`clip_arterials_to_cores`)
       so density-attracting arterials terminate ON the ring (T-junctions)
       rather than converging on the summit.
    3. Polygonize ``clipped_arterials ∪ core_rings ∪ island_exterior`` (reusing
       Arm B ``polygonize_districts`` + sliver merge) so each core interior is
       its own block and non-core space splits along the clipped arterial web.

    Returns a :class:`MacroBlockBundle`. With no cores detected, the block set
    matches the pre-3.5b polygonization (arterials + boundary).
    """
    core_polys = detect_core_regions(
        density,
        nodes,
        x0,
        y0,
        cell,
        core_frac=core_frac,
        core_max_radius_units=core_max_radius_units,
        core_min_area_units2=core_min_area_units2,
        peak_window_units=peak_window_units,
        simplify_tolerance=core_simplify_tolerance,
    )
    ring_lines = core_ring_boundaries(core_polys)
    clipped_lines, clipped_edges = clip_arterials_to_cores(
        arterial_lines, edges, core_polys
    )
    block_lines = [*clipped_lines, *ring_lines]
    blocks = polygonize_districts(
        block_lines, boundary, min_district_area=min_block_area
    )
    return MacroBlockBundle(
        core_polys=core_polys,
        ring_lines=ring_lines,
        clipped_lines=clipped_lines,
        clipped_edges=clipped_edges,
        blocks=blocks,
    )


# ---------------------------------------------------------------------------
# 6. Composed macro-layer assembly (shared by the stage scripts)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MacroParams:
    """Frozen knob-set for :func:`build_macro_layer` (one macro-layer recipe).

    Captures every parameter the stage scripts (``run_r1_macro``,
    ``run_r1_seam_spike``, ``run_r1_hybrid``) feed into the nodes ->
    cost-field -> arterials -> blocks-with-cores pipeline, so all three build
    the SAME layer and record the exact recipe in their manifests
    (:meth:`to_dict`).
    """

    # Node seeding: 3-tier semantic hierarchy (default) vs legacy graded-L0.
    hierarchical: bool = True
    # Legacy graded-L0 seeding knobs (used when ``hierarchical=False``).
    n_cities: int = DEFAULT_N_CITIES
    merge_radius: float = DEFAULT_MERGE_RADIUS
    peak_min_distance_units: float = DEFAULT_PEAK_MIN_DISTANCE_UNITS
    peak_threshold_frac: float = DEFAULT_PEAK_THRESHOLD_FRAC
    # Hierarchical village-peak knobs (used when ``hierarchical=True``).
    village_merge_radius: float = DEFAULT_VILLAGE_MERGE_RADIUS
    village_peak_min_distance_units: float = DEFAULT_VILLAGE_PEAK_MIN_DISTANCE_UNITS
    village_peak_threshold_frac: float = DEFAULT_VILLAGE_PEAK_THRESHOLD_FRAC
    # Density-attracting cost field.
    cost_base: float = 1.0
    w_slope: float = 8.0
    w_river: float = 6.0
    w_density: float = DEFAULT_W_DENSITY
    cost_floor: float = DEFAULT_COST_FLOOR
    # Importance-typed network.
    beta_ratio: float = DEFAULT_BETA_RATIO
    simplify_tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE
    # Macro-blocks + dense-core ring-roads (stage 3.5b).
    min_block_area: float = DEFAULT_MIN_BLOCK_AREA
    core_frac: float = DEFAULT_CORE_FRAC
    core_max_radius_units: float = DEFAULT_CORE_MAX_RADIUS_UNITS
    core_min_area_units2: float = DEFAULT_CORE_MIN_AREA_UNITS2

    def to_dict(self) -> dict[str, Any]:
        """Plain-dict form for JSON manifests."""
        return asdict(self)


@dataclass(frozen=True)
class MacroLayer:
    """Fully-assembled stage-1 macro layer (result of :func:`build_macro_layer`).

    Attributes
    ----------
    params : the exact :class:`MacroParams` recipe used.
    nodes : macro nodes (cities/towns/villages).
    cost : the density-attracting routing cost field.
    raw_arterial_lines / raw_edges : the pre-clip arterial network.
    core_polys / ring_lines : dense-core downtown polygons and their rings.
    arterial_lines / edges : the CLIPPED arterials (core interiors removed;
        identical to the raw ones when no cores are detected). These are what
        the stage scripts draw and export.
    blocks : polygonized macro-blocks (clipped arterials + rings + boundary).
    """

    params: MacroParams
    nodes: list[MacroNode]
    cost: np.ndarray
    raw_arterial_lines: list[sg.LineString]
    raw_edges: list[MacroEdge]
    core_polys: list[sg.Polygon]
    ring_lines: list[sg.LineString]
    arterial_lines: list[sg.LineString]
    edges: list[MacroEdge]
    blocks: list[sg.Polygon]


def build_macro_layer(
    fields: IslandFields,
    boundary: sg.Polygon,
    points: pl.DataFrame,
    params: MacroParams | None = None,
) -> MacroLayer:
    """Assemble the full stage-1 macro layer from the staged inputs.

    THE single shared nodes -> cost-field -> arterials -> blocks-with-cores
    pipeline behind ``run_r1_macro``, ``run_r1_seam_spike`` and
    ``run_r1_hybrid`` (previously each script hand-assembled its own copy).
    Deterministic: depends only on the inputs and ``params``.

    With no detectable cores the returned ``blocks`` match the pre-3.5b
    polygonization (:func:`polygonize_macro_blocks` over the raw arterials) and
    ``arterial_lines``/``edges`` equal the raw network.
    """
    if params is None:
        params = MacroParams()

    density = fields.density
    x0, y0, cell = fields.x0, fields.y0, fields.cell
    nrows, ncols = density.shape
    mask = boundary_mask(boundary, x0, y0, cell, nrows, ncols)

    if params.hierarchical:
        nodes = build_macro_nodes_hierarchical(
            density,
            x0,
            y0,
            cell,
            points,
            merge_radius=params.village_merge_radius,
            peak_min_distance_units=params.village_peak_min_distance_units,
            peak_threshold_frac=params.village_peak_threshold_frac,
        )
    else:
        nodes = build_macro_nodes(
            density,
            x0,
            y0,
            cell,
            points,
            n_cities=params.n_cities,
            merge_radius=params.merge_radius,
            peak_min_distance_units=params.peak_min_distance_units,
            peak_threshold_frac=params.peak_threshold_frac,
        )

    cost = build_density_cost_field(
        fields.slope,
        fields.flow_accum,
        density,
        mask,
        base=params.cost_base,
        w_slope=params.w_slope,
        w_river=params.w_river,
        w_density=params.w_density,
        cost_floor=params.cost_floor,
    )

    raw_lines, raw_edges = compute_macro_arterials(
        nodes,
        cost,
        x0,
        y0,
        cell,
        beta_ratio=params.beta_ratio,
        simplify_tolerance=params.simplify_tolerance,
    )

    bundle = build_macro_blocks_with_cores(
        density,
        nodes,
        raw_lines,
        raw_edges,
        boundary,
        x0,
        y0,
        cell,
        core_frac=params.core_frac,
        core_max_radius_units=params.core_max_radius_units,
        core_min_area_units2=params.core_min_area_units2,
        min_block_area=params.min_block_area,
    )

    return MacroLayer(
        params=params,
        nodes=nodes,
        cost=cost,
        raw_arterial_lines=raw_lines,
        raw_edges=raw_edges,
        core_polys=bundle.core_polys,
        ring_lines=bundle.ring_lines,
        arterial_lines=bundle.clipped_lines,
        edges=bundle.clipped_edges,
        blocks=bundle.blocks,
    )


# ---------------------------------------------------------------------------
# GeoJSON serialization
# ---------------------------------------------------------------------------

# Tier -> human name (construction level). 2=highway, 1=major, 0=local.
_TIER_NAME: dict[int, str] = {2: "highway", 1: "major", 0: "local"}


def nodes_to_geojson(nodes: list[MacroNode]) -> dict[str, Any]:
    features = []
    for i, nd in enumerate(nodes):
        props: dict[str, Any] = {
            "node_id": i,
            "kind": nd.kind,
            "sigma": nd.sigma,
        }
        if nd.label is not None:
            props["label"] = nd.label
        if nd.mass is not None:
            props["mass"] = nd.mass
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [nd.x, nd.y]},
                "properties": props,
            }
        )
    return {"type": "FeatureCollection", "features": features}


def arterials_to_geojson(
    lines: list[sg.LineString],
    edges: list[MacroEdge],
) -> dict[str, Any]:
    features = []
    for line, rec in zip(lines, edges, strict=True):
        features.append(
            {
                "type": "Feature",
                "geometry": sg.mapping(line),
                "properties": {
                    "node_a": rec.node_a,
                    "node_b": rec.node_b,
                    "tau": rec.tau,
                    "tier": rec.tier,
                    "tier_name": _TIER_NAME.get(rec.tier, f"tier{rec.tier}"),
                    "path_cost": rec.path_cost,
                    "length": rec.length,
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def macro_blocks_to_geojson(blocks: list[sg.Polygon]) -> dict[str, Any]:
    features = []
    for i, poly in enumerate(blocks):
        features.append(
            {
                "type": "Feature",
                "geometry": sg.mapping(poly),
                "properties": {
                    "block_id": i,
                    "area": round(float(poly.area), 3),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def core_rings_to_geojson(core_polys: list[sg.Polygon]) -> dict[str, Any]:
    """Serialize dense-core ring polygons (the ringed downtown blocks)."""
    features = []
    for i, poly in enumerate(core_polys):
        features.append(
            {
                "type": "Feature",
                "geometry": sg.mapping(poly),
                "properties": {
                    "core_id": i,
                    "area": round(float(poly.area), 3),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}
