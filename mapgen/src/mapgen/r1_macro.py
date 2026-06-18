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

from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import polars as pl
import shapely.geometry as sg
from scipy.spatial import Delaunay
from skimage.graph import route_through_array

from mapgen.r1_arm_b import (
    _prune_edges,
    _rowcol_to_xy,
    _xy_to_rowcol,
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

NodeKind = Literal["city", "town", "village"]


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


def _delaunay_edges(
    indices: list[int], xs: np.ndarray, ys: np.ndarray
) -> set[tuple[int, int]]:
    """Delaunay edge set (global node-index pairs) over a node subset."""
    m = len(indices)
    if m < 2:
        return set()

    def _pair(a: int, b: int) -> tuple[int, int]:
        return (a, b) if a <= b else (b, a)

    if m == 2:
        return {_pair(int(indices[0]), int(indices[1]))}
    sub = np.column_stack([xs[indices], ys[indices]])
    tri = Delaunay(sub)
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
    ``_prune_edges``) independently, and node-pairs already realized at a higher
    level are excluded. Edge ``tier`` is its construction level ``L``; ``tau``
    records ``min(sigma_a, sigma_b)``.

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
    distinct_levels = sorted({int(s) for s in sigmas}, reverse=True)
    realized: set[tuple[int, int]] = set()
    level_edges: list[tuple[int, list[tuple[int, int]]]] = []
    for level in distinct_levels:
        subset = [i for i in range(n) if sigmas[i] >= level]
        edge_set = _delaunay_edges(subset, xs, ys) - realized
        realized |= edge_set
        level_edges.append((level, sorted(edge_set)))

    lines: list[sg.LineString] = []
    records: list[MacroEdge] = []

    # Build each level independently (highest tier first so it sorts ahead).
    for tier, edge_list in level_edges:
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
