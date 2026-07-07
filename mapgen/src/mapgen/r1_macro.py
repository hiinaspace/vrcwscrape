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
3.5. **Chaikin smoothing** — the routed+DP-simplified arterials are jagged
   (straight chords, abrupt kinks); Chaikin corner-cutting (endpoints pinned)
   rounds them, right after routing and before core-clipping/polygonization,
   so every downstream consumer (blocks, per-block Chen, gates, lots, both
   exports) inherits the same smoothed network. See ``smooth_polyline``.
3.6. **Same-corridor geometry cleanup** (slice T-geo) — tiers are routed
   independently, so near-coincident routes braid through shared corridors:
   ``dedup_corridor_lines`` collapses them PRE-clip (one corridor = one
   chain, merged tier = max of contributors); post-clip,
   ``snap_endpoints_to_rings`` turns near-ring trunk endpoints into exact
   ring T-junctions and ``prune_short_dangles`` removes short degree-1
   fragments (switchback hooks). Every stage re-emits a fresh index-aligned
   ``arterial_lines``/``edges`` pair.
4. **Macro-blocks** — polygonize (union of kept arterials + boundary exterior)
   and merge slivers, reusing Arm B ``polygonize_districts``.
5. **Ranked settlement nuclei** (S2) — the hierarchical city/town centroids are
   first PEAK-SNAPPED onto the nearest strong density peak within a radius
   (``snap_nodes_to_peaks``, so the dense-core growing step below starts from a
   real summit, not a visit-weighted saddle); each surviving core region is
   then wrapped into a ranked :class:`NucleusSpec` (:func:`build_nucleus_specs`)
   with an anchor point, integrated mass, rank and top-K ``is_major`` flag —
   the interface downstream zone-massing and the 2D map consume. See
   ``docs/macro-roads-nuclei-plan.md``, slice S2.
6. **Intra-nucleus avenues** (S3, the keystone) — every MAJOR nucleus's ring
   T-junction stations (where clipped arterials meet its core ring) fan a
   spoke in to a small inner plaza ring instead of leaving the core a single
   ring-fenced, roadless blob (:func:`add_intra_nucleus_avenues`); this is
   what a downtown's internal road structure comes from. Terminating on the
   plaza ring (an annular sector) rather than the anchor point (an acute
   pie-wedge) dodges Chen's acute-wedge subdivision pathology. See
   ``docs/macro-roads-nuclei-plan.md``, slice S3.
7. **Functional road hierarchy** (slice B) — construction tiers are an
   order-of-growth label and fragment under clipping ("confetti"); the final
   macro linework is therefore noded into a junction graph and tier is
   re-derived as PATH COVERAGE between the ranked nuclei anchors (highways =
   major-pair paths, majors = all-pair paths, rest local), with
   length-weighted edge betweenness stored per record for S7-slim's widths
   (:func:`assign_functional_tiers`).

The module is deterministic: it depends only on the staged rasters/points and a
fixed RNG seed (``DEFAULT_SEED``); no stochastic steps are used, but the seed is
set so any future jitter stays reproducible.
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

import networkx as nx
import numpy as np
import polars as pl
import shapely
import shapely.geometry as sg
from scipy.spatial import Delaunay, QhullError
from shapely.ops import polygonize, substring, unary_union
from shapely.strtree import STRtree
from skimage.graph import route_through_array

from mapgen.chen_field import RasterDensityField
from mapgen.r1_arm_a import IslandFields
from mapgen.r1_arm_b import (
    _merge_slivers,
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

# Peak-snap radius (island units, S2): a city/town centroid within this many
# units of a real density peak snaps onto it -- ``detect_core_regions`` grows
# cores FROM these exact node positions, so a visit-weighted centroid that
# lands in a density SADDLE seeds a weak or dropped core. A centroid with no
# peak in range stays put. See docs/macro-roads-nuclei-plan.md, slice S2.
DEFAULT_PEAK_SNAP_RADIUS_UNITS: float = 6.0

# Beta-ratio pruning (Arm B convention; inf keeps all edges).
DEFAULT_BETA_RATIO: float = 1.2

# Arterial simplification tolerance (island units).
DEFAULT_SIMPLIFY_TOLERANCE: float = 1.5

# --- Arterial Chaikin smoothing (stage 3.4, applied post-route/pre-clip) ---
#
# The DP-simplified routed arterials are long straight chords joined at
# abrupt kinks. Chaikin corner-cutting rounds those kinks; it deviates LESS
# than the DP tolerance above already licenses, so it introduces no new
# slope/river routing cost. See `smooth_polyline`.
DEFAULT_SMOOTH_ITERATIONS: int = 2
# Light post-Chaikin simplify (island units) to bound the vertex count back
# down (Chaikin roughly doubles vertices per pass).
DEFAULT_SMOOTH_POST_TOL: float = 0.15

# --- Same-corridor geometry cleanup (slice T-geo) ---
#
# Three passes: corridor dedup (stage 3.6, PRE-clip), endpoint snap-to-ring
# and degree-1 dangle prune (both POST-clip, stage 2b of
# ``build_macro_blocks_with_cores``).
#
# Tiers are routed INDEPENDENTLY over one shared cost grid, so two routes can
# run near-coincident through the same corridor (braiding). Dedup collapses
# those; the tolerance is just under one buildable lot depth at the production
# 25 m/unit, so twins that survive are far enough apart to hold a lot row
# between them (legitimate frontage roads -- no frontage detection needed).
CORRIDOR_DEDUP_TOL: float = 1.2
# A near-coincident run shorter than this (arc length along the candidate,
# island units) is NOT absorbed: a transversal crossing dips inside the
# corridor buffer for ~2*tol/sin(angle) and must survive as a crossing; only
# sustained side-by-side runs are braids.
CORRIDOR_MIN_ABSORB_LEN: float = 5.0
# Two absorbed runs separated by a shorter far-gap than this merge into one
# (the candidate wobbling across the buffer edge is still one corridor).
CORRIDOR_GAP_MERGE_LEN: float = 2.4
# Arc-length sampling step (island units) for the near/far distance profile.
CORRIDOR_SAMPLE_STEP: float = 0.25

# Endpoint snap-to-ring (post-clip): a clipped arterial endpoint within this
# distance (island units) of a core ring -- but not already ON it -- is
# snapped onto the ring, forming a real T-junction station instead of a stub
# hovering next to the ring.
RING_SNAP_TOL: float = 1.5

# Degree-1 dangle prune (post-clip): clipped fragments shorter than this
# (island units; ~200 m at 25 m/unit) whose free end has degree 1 in the
# macro graph are removed (the "tiny red switchback hooks" of the visual
# gate). Fragments terminating on a core ring are always spared -- they carry
# the ring T-junction stations the S3 spokes are derived from.
DANGLE_PRUNE_LEN: float = 8.0

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
# Light POST-Chaikin simplify tolerance (island units) for the extracted ring
# boundary -- bounds the vertex count Chaikin corner-cutting adds back down.
# Small relative to the historical raw simplify(1.0): the ring boundary is now
# smoothed by morphological closing + Chaikin first (see `_cells_to_polygon`),
# so this final pass only needs to trim near-collinear noise, not de-jag.
DEFAULT_CORE_SIMPLIFY_TOLERANCE: float = 0.2
# Chaikin passes applied to the closed core-ring boundary.
DEFAULT_CORE_SMOOTH_ITERATIONS: int = 2
# Morphological-closing half-width (island units) for the ring boundary; ``None``
# resolves to one raster cell width at call time ("w ~= 1 cell").
DEFAULT_CORE_CLOSE_WIDTH_UNITS: float | None = None

# --- Ring regularization (slice R) ---
#
# `_cells_to_polygon`'s close+Chaikin round the pixel-blob stair-step but
# never remove a genuinely concave feature the cell-region growth can leave
# -- a narrow "keyhole" neck feeding a bulge still reads as an organic blob,
# not a road-like ring, and every core shares the identical close+Chaikin
# motif ("stamped wheel"). Rank isn't known until `build_nucleus_specs`
# (mass integration needs the exact core polygon as its domain), so this is
# applied as a SECOND PASS on already-`_cells_to_polygon`'d core polygons
# (see `_regularize_ring_polygons`), rank-scaled: MAJOR nuclei (top-K by
# mass) get a real morphological OPENING that severs necks/lobes; minor/
# village cores get ``width=0`` (skip -- today's cheap contour), which also
# de-stamps the motif. See docs/macro-roads-nuclei-plan.md, slice R.
DEFAULT_RING_OPEN_WIDTH_MAJOR_UNITS: float = 1.75
DEFAULT_RING_OPEN_WIDTH_MINOR_UNITS: float = 0.0

# Optional low-pass elliptic-Fourier / periodic-spline refit of a MAJOR ring
# boundary, applied AFTER the opening above. Off by default
# (``MacroParams.ring_fourier_refit=False``) -- opening alone is expected to
# suffice; the mini visual gate decides whether to turn it on.
DEFAULT_RING_FOURIER_HARMONICS: int = 8
# Uniform arc-length resample count feeding the periodic FFT fit.
DEFAULT_RING_FOURIER_RESAMPLE_N: int = 128
# Reject the refit (fall back to the pre-refit ring) if the repaired
# polygon's area differs from the input's by more than this fraction.
DEFAULT_RING_FOURIER_MAX_AREA_CHANGE: float = 0.2

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


def snap_centroid_to_peak(
    cx: float,
    cy: float,
    peak_xs: np.ndarray,
    peak_ys: np.ndarray,
    peak_ds: np.ndarray,
    *,
    radius: float = DEFAULT_PEAK_SNAP_RADIUS_UNITS,
) -> tuple[float, float]:
    """Snap ``(cx, cy)`` onto the highest-density peak within ``radius``.

    Returns that peak's own ``(x, y)`` if at least one of ``(peak_xs,
    peak_ys)`` lies within ``radius`` island units (Euclidean) of ``(cx,
    cy)``; otherwise ``(cx, cy)`` unchanged -- a centroid with no peak in range
    stays put rather than being dragged toward a distant, unrelated summit.
    Ties (equal peak density) break on array order, which is
    ``find_density_peaks``'s own deterministic scan order, so this is
    deterministic given fixed inputs.
    """
    if peak_xs.size == 0 or radius <= 0.0:
        return cx, cy
    d2 = (peak_xs - cx) ** 2 + (peak_ys - cy) ** 2
    in_range = np.nonzero(d2 <= radius * radius)[0]
    if in_range.size == 0:
        return cx, cy
    best = in_range[int(np.argmax(peak_ds[in_range]))]
    return float(peak_xs[best]), float(peak_ys[best])


def snap_nodes_to_peaks(
    nodes: list[MacroNode],
    peak_xs: np.ndarray,
    peak_ys: np.ndarray,
    peak_ds: np.ndarray,
    *,
    radius: float = DEFAULT_PEAK_SNAP_RADIUS_UNITS,
) -> list[MacroNode]:
    """Peak-snap every node's position (see :func:`snap_centroid_to_peak`).

    Preserves ``sigma``/``kind``/``label``/``mass`` on every node -- only
    ``(x, y)`` moves.
    """
    out: list[MacroNode] = []
    for nd in nodes:
        nx, ny = snap_centroid_to_peak(
            nd.x, nd.y, peak_xs, peak_ys, peak_ds, radius=radius
        )
        out.append(nd if (nx == nd.x and ny == nd.y) else replace(nd, x=nx, y=ny))
    return out


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
    peak_snap_radius_units: float = DEFAULT_PEAK_SNAP_RADIUS_UNITS,
) -> list[MacroNode]:
    """Build the 3-tier SEMANTIC macro node hierarchy (L1 / L0 / peaks).

    - **Cities (sigma=2):** the 7 L1 cluster centroids (visit-weighted),
      PEAK-SNAPPED (see below).
    - **Towns (sigma=1):** the 18 L0 cluster centroids (visit-weighted),
      PEAK-SNAPPED (see below).
    - **Villages (sigma=0):** loosened density peaks, deduped against any
      city/town within ``merge_radius``.

    NB: on this island the cluster hierarchy is INVERTED — L0 (18) is the finest
    tier and L1 (7) is coarser sub-regions — so using L1 as cities and L0 as
    towns yields finer macro structure (more nodes -> finer macro-blocks) than
    the legacy graded-L0 seeding.

    **Peak-snap (S2):** before villages are picked, every city/town centroid is
    snapped onto the highest density peak within ``peak_snap_radius_units`` of
    it (:func:`snap_nodes_to_peaks`) -- a visit-weighted cluster centroid can
    land in a density SADDLE, and ``detect_core_regions`` grows cores FROM
    these exact node positions, so a saddle-seeded core is weak or dropped
    (see docs/macro-roads-nuclei-plan.md, slice S2, "What the review found"
    #4). Peaks are found with the SAME loose village-tier parameters
    (``peak_min_distance_units``/``peak_threshold_frac``) as the village pass
    below, so a snapped city/town coincides EXACTLY with a peak
    ``village_nodes_from_peaks`` would otherwise also emit as its own
    village -- its existing ``merge_radius`` dedup then drops that duplicate
    with no extra bookkeeping. Set ``peak_snap_radius_units <= 0`` to disable
    (nodes stay at their raw cluster centroids, the pre-S2 behavior).

    Nodes are returned cities-then-towns-then-villages (each block in its own
    deterministic cluster-id / peak order).
    """
    cities = cluster_centroids(points, "l1_id", "l1_name", CITY_SIGMA, "city")
    towns = cluster_centroids(points, "l0_id", "l0_name", TOWN_SIGMA, "town")

    if peak_snap_radius_units > 0.0:
        peak_xs, peak_ys, peak_ds = find_density_peaks(
            density,
            x0,
            y0,
            cell,
            min_distance_units=peak_min_distance_units,
            threshold_frac=peak_threshold_frac,
        )
        cities = snap_nodes_to_peaks(
            cities, peak_xs, peak_ys, peak_ds, radius=peak_snap_radius_units
        )
        towns = snap_nodes_to_peaks(
            towns, peak_xs, peak_ys, peak_ds, radius=peak_snap_radius_units
        )

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
    """A kept arterial edge between two macro nodes.

    ``tier`` starts life as the CONSTRUCTION level (2 = highway, 1 = major,
    0 = local -- the per-level Delaunay pass an edge was built in, see
    :func:`compute_macro_arterials`). The slice-B functional relabel
    (:func:`assign_functional_tiers`, run on the FINAL macro geometry) then
    overwrites ``tier`` with the path-coverage FUNCTIONAL tier, preserves the
    original construction level in ``build_tier`` (``-1`` = relabel not run /
    no construction tier, e.g. ring arcs) and fills ``betweenness`` with the
    edge's length-weighted betweenness centrality on the junction graph
    (0.0 before the relabel) -- S7-slim's width input.
    """

    node_a: int
    node_b: int
    tau: int  # min(sigma_a, sigma_b)
    tier: int  # construction level pre-relabel, functional tier post-relabel
    path_cost: float
    length: float
    build_tier: int = -1  # construction tier preserved by the B relabel
    betweenness: float = 0.0  # length-weighted edge betweenness (slice B)


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
# 3.5. Polyline smoothing (Chaikin corner-cutting)
# ---------------------------------------------------------------------------
#
# The routed+DP-simplified arterials (above) and the raw pixel-blob core
# rings (below, section 5) are both jagged: long straight chords / cell edges
# meeting at abrupt kinks, no curve. Chaikin corner-cutting rounds those kinks
# without moving far from the original path -- less, in fact, than the DP
# `simplify` tolerance already licenses -- so it adds no new routing-cost risk.
# `_chaikin_pass_open` is used for arterials (endpoints pinned, since shared
# macro-node junctions must stay coincident for downstream fusion);
# `_chaikin_pass_closed` is used for core-ring boundaries (no fixed point on a
# loop).


def _chaikin_pass_open(
    coords: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    """One Chaikin corner-cutting pass over an OPEN coordinate chain.

    Preserves the first and last coordinate EXACTLY (only interior corners are
    cut). Every interior corner ``coords[i]`` is replaced by the two points
    1/4 and 3/4 along its two flanking edges. No-op for chains shorter than 3
    points -- there is no interior corner to cut.
    """
    n = len(coords)
    if n < 3:
        return list(coords)
    out: list[tuple[float, float]] = [coords[0]]
    last_edge = n - 2
    for i in range(n - 1):
        (x0, y0), (x1, y1) = coords[i], coords[i + 1]
        q = (0.75 * x0 + 0.25 * x1, 0.75 * y0 + 0.25 * y1)
        r = (0.25 * x0 + 0.75 * x1, 0.25 * y0 + 0.75 * y1)
        if i == 0:
            out.append(r)
        elif i == last_edge:
            out.append(q)
        else:
            out.append(q)
            out.append(r)
    out.append(coords[-1])
    return out


def _chaikin_pass_closed(
    coords: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    """One Chaikin corner-cutting pass over a CLOSED ring coordinate chain.

    ``coords`` is a closed ring (``coords[0] == coords[-1]``, the shapely
    convention). Every original vertex is replaced -- there is no fixed
    endpoint on a loop -- by the 1/4 and 3/4 points of its two flanking edges,
    wrapping around. No-op for rings with fewer than 3 distinct vertices.
    """
    ring = coords[:-1] if coords[0] == coords[-1] else list(coords)
    n = len(ring)
    if n < 3:
        return list(coords)
    out: list[tuple[float, float]] = []
    for i in range(n):
        (x0, y0), (x1, y1) = ring[i], ring[(i + 1) % n]
        out.append((0.75 * x0 + 0.25 * x1, 0.75 * y0 + 0.25 * y1))
        out.append((0.25 * x0 + 0.75 * x1, 0.25 * y0 + 0.75 * y1))
    out.append(out[0])
    return out


def smooth_polyline(
    line: sg.LineString,
    *,
    iterations: int = DEFAULT_SMOOTH_ITERATIONS,
    post_tol: float = DEFAULT_SMOOTH_POST_TOL,
) -> sg.LineString:
    """Chaikin-smooth an OPEN arterial polyline, endpoints held EXACTLY fixed.

    Applies ``iterations`` passes of endpoint-preserving Chaikin corner-cutting
    (:func:`_chaikin_pass_open`) to round the abrupt kinks that Delaunay
    least-cost routing + DP ``simplify`` leave behind, then a light
    ``simplify(post_tol, preserve_topology=False)`` pass to bound the vertex
    count back down (each Chaikin pass roughly doubles the interior vertex
    count). Degenerate lines (fewer than 3 coordinates) or ``iterations <= 0``
    pass through UNCHANGED -- there is no corner to cut, and this is how
    smoothing is disabled for a comparison/no-op run.

    Endpoint preservation is load-bearing: shared macro-node junctions must
    stay bit-for-bit coincident across every smoothed arterial meeting there,
    or downstream T-junction fusion (gate snapping, block polygonization)
    desyncs. Douglas-Peucker ``simplify`` never relocates an open line's
    endpoints, so this holds through the post-simplify pass too; the endpoint
    coordinates are pinned back defensively regardless, in case of any
    floating-point drift.
    """
    coords = list(line.coords)
    if len(coords) < 3 or iterations <= 0:
        return line
    pts = coords
    for _ in range(iterations):
        pts = _chaikin_pass_open(pts)
    smoothed = sg.LineString(pts)
    if post_tol > 0:
        smoothed = smoothed.simplify(post_tol, preserve_topology=False)
    out_coords = list(smoothed.coords)
    if out_coords[0] != coords[0] or out_coords[-1] != coords[-1]:
        out_coords[0] = coords[0]
        out_coords[-1] = coords[-1]
        smoothed = sg.LineString(out_coords)
    return smoothed


def _line_stays_in_mask(
    line: sg.LineString, mask: np.ndarray, x0: float, y0: float, cell: float
) -> bool:
    """True if every vertex of ``line`` lands on an inside-mask raster cell."""
    nrows, ncols = mask.shape
    xs = np.array([c[0] for c in line.coords], dtype=float)
    ys = np.array([c[1] for c in line.coords], dtype=float)
    rows, cols = _xy_to_rowcol(xs, ys, x0, y0, cell, nrows, ncols)
    rows = np.clip(rows, 0, nrows - 1)
    cols = np.clip(cols, 0, ncols - 1)
    return bool(mask[rows, cols].all())


def smooth_arterial_lines(
    lines: list[sg.LineString],
    mask: np.ndarray,
    x0: float,
    y0: float,
    cell: float,
    *,
    iterations: int = DEFAULT_SMOOTH_ITERATIONS,
    post_tol: float = DEFAULT_SMOOTH_POST_TOL,
) -> list[sg.LineString]:
    """Chaikin-smooth every arterial, guarded against leaving the routing mask.

    Chaikin corner-cutting deviates LESS than the DP ``simplify`` tolerance the
    routed path already licenses, so it should never introduce new slope/river
    infeasibility -- but a coastline-hugging arterial can sit close enough to
    the mask boundary that a corner-cut noses a vertex into an outside-mask
    (``inf``-cost) cell. Cheap guard: if any vertex of a smoothed line would
    land outside ``mask``, that ENTIRE line falls back to its unsmoothed
    original rather than partially smoothing it (this should be rare -- see
    above -- so a whole-line fallback is an acceptable, simple, deterministic
    conservative choice).
    """
    out: list[sg.LineString] = []
    for line in lines:
        smoothed = smooth_polyline(line, iterations=iterations, post_tol=post_tol)
        if _line_stays_in_mask(smoothed, mask, x0, y0, cell):
            out.append(smoothed)
        else:
            out.append(line)
    return out


# ---------------------------------------------------------------------------
# 3.6. Same-corridor geometry dedup (slice T-geo, PRE-clip)
# ---------------------------------------------------------------------------
#
# Each tier's Delaunay edges are least-cost routed INDEPENDENTLY over the same
# cost grid, sharing only a node-pair exclusion set -- never geometry. Two
# routes through the same corridor therefore come out as near-coincident (but
# not identical) polylines that braid around each other. This pass collapses
# them so one corridor = one polyline chain: lines are visited in descending
# (tier, length) priority; each candidate's sustained near-runs (within
# ``CORRIDOR_DEDUP_TOL`` of an already-kept line for at least
# ``CORRIDOR_MIN_ABSORB_LEN`` of arc length) are ABSORBED into the kept
# corridor, whose tier is bumped to the max of its contributors; the
# candidate's surviving pieces are stitched onto the corridor at their cut
# ends (the stitch point is inserted as an exact shared VERTEX of the kept
# line, so downstream ``polygonize``/union noding fuses there -- the same
# ULP-exactness lesson as `_plaza_ring_polygon`). Lines with no sustained
# near-coincident partner are re-emitted as the ORIGINAL objects,
# bit-identical. Runs pre-clip so `clip_arterials_to_cores` /
# `_ring_junction_stations` exact-on-ring matching is untouched.


def _insert_vertex_at_station(
    coords: list[tuple[float, float]],
    station: float,
    *,
    eps: float = 1e-9,
) -> tuple[list[tuple[float, float]], tuple[float, float]]:
    """Insert (or reuse) a vertex at arc-length ``station`` along ``coords``.

    Returns ``(new_coords, q)`` where ``q`` is the exact vertex coordinate at
    ``station``. When ``station`` falls within ``eps`` of an existing vertex
    that vertex is REUSED (no near-duplicate inserted) and ``coords`` is
    returned unchanged; otherwise the interpolated point is inserted into the
    owning segment so it is an exact vertex of the returned chain.
    """
    cum = 0.0
    for j in range(len(coords) - 1):
        (x1, y1), (x2, y2) = coords[j], coords[j + 1]
        seg = float(np.hypot(x2 - x1, y2 - y1))
        if station <= cum + eps:
            return coords, coords[j]
        if station < cum + seg - eps:
            t = (station - cum) / seg
            q = (x1 + t * (x2 - x1), y1 + t * (y2 - y1))
            return [*coords[: j + 1], q, *coords[j + 1 :]], q
        cum += seg
    return coords, coords[-1]


def _absorbed_intervals(
    stations: np.ndarray,
    near: np.ndarray,
    *,
    min_absorb_len: float,
    gap_merge_len: float,
) -> list[tuple[float, float]]:
    """Station intervals of sustained near-runs along a sampled candidate.

    ``stations`` are ascending arc-length sample positions, ``near`` the
    parallel within-tolerance flags. Contiguous near-runs become candidate
    intervals; runs separated by a far-gap shorter than ``gap_merge_len``
    merge (one corridor, wobbling across the buffer edge); merged intervals
    shorter than ``min_absorb_len`` are discarded (transversal crossings).
    """
    idx = np.nonzero(near)[0]
    if idx.size == 0:
        return []
    runs: list[tuple[float, float]] = []
    start = prev = int(idx[0])
    for i in idx[1:].tolist():
        if i == prev + 1:
            prev = i
            continue
        runs.append((float(stations[start]), float(stations[prev])))
        start = prev = i
    runs.append((float(stations[start]), float(stations[prev])))

    merged: list[tuple[float, float]] = [runs[0]]
    for a, b in runs[1:]:
        if a - merged[-1][1] < gap_merge_len:
            merged[-1] = (merged[-1][0], b)
        else:
            merged.append((a, b))
    return [(a, b) for a, b in merged if b - a >= min_absorb_len]


@dataclass
class _CorridorEntry:
    """Mutable bookkeeping for one kept line during corridor dedup."""

    coords: list[tuple[float, float]]
    geom: sg.LineString
    rec: MacroEdge
    tier: int
    src: int  # original input index (output ordering)
    start: float  # interval start along the source line (output sub-order)
    orig_line: sg.LineString | None  # non-None => still bit-identical


def dedup_corridor_lines(
    lines: list[sg.LineString],
    edges: list[MacroEdge],
    *,
    tol: float = CORRIDOR_DEDUP_TOL,
    min_absorb_len: float = CORRIDOR_MIN_ABSORB_LEN,
    gap_merge_len: float = CORRIDOR_GAP_MERGE_LEN,
    sample_step: float = CORRIDOR_SAMPLE_STEP,
) -> tuple[list[sg.LineString], list[MacroEdge], int]:
    """Collapse near-coincident same-corridor arterials (slice T-geo, pre-clip).

    Lines are processed in descending ``(tier, length)`` priority (index
    tie-break -- deterministic). For each candidate, an arc-length distance
    profile against the already-kept lines (sampled every ``sample_step``)
    finds its sustained near-runs (see :func:`_absorbed_intervals`); those
    runs are absorbed -- each kept line they hug has its tier bumped to the
    max of its contributors -- and the candidate's surviving pieces
    (``shapely.ops.substring`` of the ORIGINAL geometry, so untouched interior
    vertices are preserved exactly) are stitched onto the nearest kept line at
    each cut end via an exact shared vertex. A candidate with no sustained
    near-run is kept as the ORIGINAL object, bit-identical; twins farther
    apart than ``tol`` everywhere always survive.

    Returns ``(lines, edges, n_merged)``: a FRESH index-aligned pair in
    input-source order (surviving pieces of one line stay adjacent, ordered
    by their interval start), plus the number of input lines that had at
    least one run absorbed (fully or partially). ``tol <= 0`` disables the
    pass entirely (identity on fresh lists).

    Edge records: absorbers keep their ``MacroEdge`` (tier possibly bumped,
    length re-rounded when a stitch vertex was inserted); surviving pieces
    copy their source record with ``length`` updated -- ``node_a``/
    ``node_b``/``path_cost`` stay informational, exactly like the fragments
    :func:`clip_arterials_to_cores` emits.
    """
    if tol <= 0.0 or len(lines) < 2:
        return list(lines), list(edges), 0

    order = sorted(
        range(len(lines)),
        key=lambda i: (-edges[i].tier, -lines[i].length, i),
    )

    kept: list[_CorridorEntry] = []
    n_merged = 0
    for i in order:
        line = lines[i]
        rec = edges[i]
        if not kept:
            kept.append(
                _CorridorEntry(
                    coords=list(line.coords),
                    geom=line,
                    rec=rec,
                    tier=rec.tier,
                    src=i,
                    start=0.0,
                    orig_line=line,
                )
            )
            continue

        total = float(line.length)
        n_samples = max(int(np.ceil(total / sample_step)) + 1, 2)
        stations = np.linspace(0.0, total, n_samples)
        pts = shapely.line_interpolate_point(line, stations)
        tree = STRtree([e.geom for e in kept])
        q_idx, q_dist = tree.query_nearest(pts, return_distance=True, all_matches=False)
        nearest = np.full(n_samples, -1, dtype=int)
        dist = np.full(n_samples, np.inf, dtype=float)
        nearest[q_idx[0]] = q_idx[1]
        dist[q_idx[0]] = q_dist

        absorbed = _absorbed_intervals(
            stations,
            dist <= tol,
            min_absorb_len=min_absorb_len,
            gap_merge_len=gap_merge_len,
        )
        if not absorbed:
            kept.append(
                _CorridorEntry(
                    coords=list(line.coords),
                    geom=line,
                    rec=rec,
                    tier=rec.tier,
                    src=i,
                    start=0.0,
                    orig_line=line,
                )
            )
            continue

        n_merged += 1
        # Bump every absorbing kept line's tier to the max of its contributors.
        for a, b in absorbed:
            in_run = (stations >= a - 1e-12) & (stations <= b + 1e-12) & (dist <= tol)
            for k in sorted({int(k) for k in nearest[in_run] if k >= 0}):
                kept[k].tier = max(kept[k].tier, rec.tier)

        # Surviving intervals: the complement, CLOSED at the near boundary
        # samples (so a cut end sits within tol of its absorber -- a short
        # stitch, not a gap).
        surviving: list[tuple[float, float]] = []
        prev_end = 0.0
        for a, b in absorbed:
            if a - prev_end > 1e-9:
                surviving.append((prev_end, a))
            prev_end = b
        if total - prev_end > 1e-9:
            surviving.append((prev_end, total))

        for a, b in surviving:
            piece = substring(line, a, b)
            coords = list(piece.coords)
            if len(coords) < 2:
                continue
            for at_start, is_cut in ((True, a > 1e-9), (False, b < total - 1e-9)):
                if not is_cut:
                    continue
                cut_pt = sg.Point(coords[0] if at_start else coords[-1])
                k = min(
                    range(len(kept)),
                    key=lambda k: (kept[k].geom.distance(cut_pt), k),
                )
                entry = kept[k]
                station = float(entry.geom.project(cut_pt))
                new_coords, q = _insert_vertex_at_station(entry.coords, station)
                if new_coords is not entry.coords:
                    entry.coords = new_coords
                    entry.geom = sg.LineString(new_coords)
                    entry.orig_line = None
                if at_start and q != coords[0]:
                    coords.insert(0, q)
                elif not at_start and q != coords[-1]:
                    coords.append(q)
            piece_line = sg.LineString(coords)
            kept.append(
                _CorridorEntry(
                    coords=coords,
                    geom=piece_line,
                    rec=rec,
                    tier=rec.tier,
                    src=i,
                    start=a,
                    orig_line=None,
                )
            )

    out_lines: list[sg.LineString] = []
    out_edges: list[MacroEdge] = []
    for entry in sorted(kept, key=lambda e: (e.src, e.start)):
        if entry.orig_line is not None and entry.tier == entry.rec.tier:
            out_lines.append(entry.orig_line)
            out_edges.append(entry.rec)
        else:
            out_lines.append(entry.geom)
            out_edges.append(
                replace(
                    entry.rec,
                    tier=entry.tier,
                    length=round(float(entry.geom.length), 3),
                )
            )
    return out_lines, out_edges, n_merged


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
    close_width: float | None = DEFAULT_CORE_CLOSE_WIDTH_UNITS,
    smooth_iterations: int = DEFAULT_CORE_SMOOTH_ITERATIONS,
) -> sg.Polygon | None:
    """Union the cell squares of ``cells`` into a single smoothed core polygon.

    Each ``(row, col)`` becomes its cell-centre-aligned square box; the union's
    largest exterior ring is taken (interior holes dropped). Cell boxes span
    ``[x0 + col*cell, x0 + (col+1)*cell]`` etc., consistent with the
    cell-centre convention ``x = x0 + (col + 0.5) * cell``.

    The raw union is a pixel-blob (stair-step boundary along cell edges).
    Boundary smoothing, in order:

    1. Morphological CLOSE (``buffer(+w).buffer(-w)``, ``w = close_width`` or
       one raster cell if ``None``) fills single-cell notches and rounds the
       stair-step boundary.
    2. Chaikin corner-cutting (:func:`_chaikin_pass_closed`, ``smooth_iterations``
       passes) rounds the remaining corners into a smooth loop.
    3. A final light ``simplify(simplify_tolerance)`` bounds the vertex count
       Chaikin adds back down.
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

    w = close_width if close_width is not None else cell
    if w > 0:
        closed = poly.buffer(w, quad_segs=2).buffer(-w, quad_segs=2)
        if closed.geom_type == "MultiPolygon":
            closed = max(closed.geoms, key=lambda p: p.area) if closed.geoms else poly
        if not closed.is_empty and closed.geom_type == "Polygon":
            poly = closed

    if smooth_iterations > 0:
        ring_coords = list(poly.exterior.coords)
        for _ in range(smooth_iterations):
            ring_coords = _chaikin_pass_closed(ring_coords)
        smoothed_poly = sg.Polygon(ring_coords)
        if smoothed_poly.is_valid and not smoothed_poly.is_empty:
            poly = smoothed_poly

    if simplify_tolerance > 0:
        poly = poly.simplify(simplify_tolerance, preserve_topology=True)
    if poly.is_empty or not poly.is_valid:
        poly = poly.buffer(0)
    if poly.is_empty or poly.geom_type != "Polygon":
        return None
    return poly


def _open_ring_polygon(poly: sg.Polygon, width: float) -> sg.Polygon:
    """Morphological OPENING (erode then dilate) to shed thin necks/lobes.

    ``buffer(-width).buffer(+width)`` severs any neck narrower than
    ``2 * width`` from the bulge it feeds -- the mechanism that turns a
    keyhole-shaped core (narrow neck + bulge) into just the main lobe. If the
    erosion splits the polygon into several pieces, only the LARGEST (by
    area) survives the dilation: a ring boundary must stay a single closed
    loop for :func:`core_ring_boundaries` / :func:`clip_arterials_to_cores` /
    :func:`snap_endpoints_to_rings` to treat it as one ring. ``width <= 0`` is
    an identity pass (villages/minor cores; see
    :func:`_regularize_ring_polygons`). Falls back to ``poly`` unchanged if
    the opened result is empty or degenerates to a non-polygon (e.g. the
    whole shape erodes away for a core slimmer than ``2 * width``).
    """
    if width <= 0.0:
        return poly
    opened = poly.buffer(-width, quad_segs=2).buffer(width, quad_segs=2)
    if opened.is_empty:
        return poly
    if opened.geom_type == "MultiPolygon":
        if not opened.geoms:
            return poly
        opened = max(opened.geoms, key=lambda p: p.area)
    if opened.geom_type != "Polygon" or opened.is_empty:
        return poly
    return opened


def _fourier_low_pass_refit(
    poly: sg.Polygon,
    *,
    n_harmonics: int = DEFAULT_RING_FOURIER_HARMONICS,
    n_samples: int = DEFAULT_RING_FOURIER_RESAMPLE_N,
    max_area_change: float = DEFAULT_RING_FOURIER_MAX_AREA_CHANGE,
) -> sg.Polygon:
    """Low-pass elliptic-Fourier refit of a ring polygon's boundary (slice R).

    Resamples the exterior ring at ``n_samples`` uniform ARC-LENGTH stations,
    then truncates the closed contour's Fourier series (``np.fft.rfft`` on
    the resampled ``x``/``y`` sequences) to ``n_harmonics`` and reconstructs
    -- a periodic low-pass spline that smooths the ring into simple arcs
    instead of the morphological-open+close+Chaikin's still-somewhat-faceted
    loop.

    Two guards, either of which falls back to the INPUT ``poly`` unchanged
    (the refit is cosmetic, never load-bearing):

    1. ``buffer(0)`` validity: the reconstructed contour can self-intersect
       (Fourier truncation is not shape-preserving) -- ``buffer(0)`` repairs
       it; if that yields anything other than a single ``Polygon`` (split
       into a ``MultiPolygon`` or emptied), the refit is rejected.
    2. Area-change guard: if the repaired polygon's area differs from the
       input's by more than ``max_area_change`` (a fraction), the refit is
       rejected -- it must smooth the ring, not resize it.

    Off by default (``MacroParams.ring_fourier_refit=False`` /
    :func:`_regularize_ring_polygons`'s ``fourier_refit``); the rank-scaled
    opening alone is expected to suffice, per the mini visual gate.
    """
    if n_harmonics <= 0 or poly.is_empty:
        return poly
    coords = np.asarray(poly.exterior.coords[:-1], dtype=np.float64)
    if len(coords) < 3:
        return poly

    seg = np.diff(coords, axis=0, append=coords[:1])
    seg_len = np.hypot(seg[:, 0], seg[:, 1])
    perimeter = float(seg_len.sum())
    if perimeter <= 0.0:
        return poly
    arc = np.concatenate([[0.0], np.cumsum(seg_len)])[:-1]

    samples = np.linspace(0.0, perimeter, n_samples, endpoint=False)
    xs = np.interp(samples, arc, coords[:, 0], period=perimeter)
    ys = np.interp(samples, arc, coords[:, 1], period=perimeter)

    fx = np.fft.rfft(xs)
    fy = np.fft.rfft(ys)
    keep = min(n_harmonics + 1, len(fx))
    fx[keep:] = 0.0
    fy[keep:] = 0.0
    rx = np.fft.irfft(fx, n=n_samples)
    ry = np.fft.irfft(fy, n=n_samples)

    refit_coords = list(zip(rx.tolist(), ry.tolist(), strict=True))
    refit_coords.append(refit_coords[0])
    if len(refit_coords) < 4:
        return poly
    candidate = sg.Polygon(refit_coords)
    if not candidate.is_valid:
        candidate = candidate.buffer(0)
    if candidate.geom_type != "Polygon" or candidate.is_empty:
        return poly
    orig_area = poly.area
    if orig_area <= 0.0:
        return poly
    if abs(candidate.area - orig_area) / orig_area > max_area_change:
        return poly
    return candidate


def _regularize_ring_polygons(
    core_polys: list[sg.Polygon],
    density: np.ndarray,
    nodes: list[MacroNode],
    points: pl.DataFrame,
    x0: float,
    y0: float,
    cell: float,
    *,
    n_major_nuclei: int,
    open_width_major: float = DEFAULT_RING_OPEN_WIDTH_MAJOR_UNITS,
    open_width_minor: float = DEFAULT_RING_OPEN_WIDTH_MINOR_UNITS,
    fourier_refit: bool = False,
    fourier_harmonics: int = DEFAULT_RING_FOURIER_HARMONICS,
) -> list[sg.Polygon]:
    """Rank-scaled ring regularization: morphological opening (+ optional
    Fourier low-pass refit), applied to ``detect_core_regions``'s output
    BEFORE clip/snap/station derivation (slice R).

    Rank is not known at ``_cells_to_polygon`` time -- mass integration needs
    the exact core polygon as its domain, so ranking has to happen on cores
    that already exist -- so this runs a lightweight PRELIMINARY
    :func:`build_nucleus_specs` pass purely to read off ``is_major``
    (matched back to ``core_polys`` by polygon object identity, since
    ``build_nucleus_specs`` reuses the input polygon objects unchanged),
    then opens major cores at ``open_width_major`` and minor cores at
    ``open_width_minor`` (``0`` = skip, today's cheap contour) -- de-blobbing
    the biggest downtowns while leaving villages cheap, which also de-stamps
    the identical "wheel" ring motif every core shared before.

    MUST run before :func:`clip_arterials_to_cores` /
    :func:`snap_endpoints_to_rings` / :func:`build_nucleus_specs`: the core
    polygon is simultaneously the arterial clip boundary, the block-
    protection region, the nucleus mass/rank integration domain, and the
    spoke-station source, so everything has to re-derive from the SAME
    regularized polygon rather than desyncing.

    Identity pass if ``core_polys`` is empty or every knob is a no-op
    (``open_width_major <= 0``, ``open_width_minor <= 0``, no Fourier
    refit).
    """
    if not core_polys:
        return core_polys
    if open_width_major <= 0.0 and open_width_minor <= 0.0 and not fourier_refit:
        return core_polys

    prelim_nuclei = build_nucleus_specs(
        density, core_polys, nodes, points, x0, y0, cell, n_major=n_major_nuclei
    )
    major_ids = {id(n.polygon) for n in prelim_nuclei if n.is_major}

    out: list[sg.Polygon] = []
    for poly in core_polys:
        is_major = id(poly) in major_ids
        width = open_width_major if is_major else open_width_minor
        opened = _open_ring_polygon(poly, width)
        if fourier_refit and is_major:
            opened = _fourier_low_pass_refit(opened, n_harmonics=fourier_harmonics)
        out.append(opened)
    return out


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
    close_width: float | None = DEFAULT_CORE_CLOSE_WIDTH_UNITS,
    smooth_iterations: int = DEFAULT_CORE_SMOOTH_ITERATIONS,
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
            region,
            x0,
            y0,
            cell,
            simplify_tolerance=simplify_tolerance,
            close_width=close_width,
            smooth_iterations=smooth_iterations,
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


def snap_endpoints_to_rings(
    lines: list[sg.LineString],
    edges: list[MacroEdge],
    core_polys: list[sg.Polygon],
    *,
    tol: float = RING_SNAP_TOL,
    on_tol: float = 1e-9,
) -> tuple[list[sg.LineString], list[MacroEdge], list[sg.Polygon], int]:
    """Snap near-but-not-on-ring arterial endpoints ONTO the ring (T-geo).

    A trunk whose macro node sits just OUTSIDE a core polygon is not cut by
    :func:`clip_arterials_to_cores`, so its endpoint hovers next to the ring
    instead of forming a T-junction on it. Every unique endpoint whose
    distance to the nearest core ring is in ``(on_tol, tol]`` is replaced by
    its projection onto that ring, and the projected point is also inserted
    as an exact VERTEX of the core polygon's exterior -- exact coordinate
    identity is what makes downstream ``polygonize`` noding fuse there AND
    what keeps ``_ring_junction_stations``'s exact-on-ring matching
    (tol=1e-6) finding it as a spoke station. Endpoints already ON a ring
    (within ``on_tol`` -- every clip-produced cut) are left strictly alone,
    as are endpoints farther than ``tol`` (legitimate termini near a core).

    The snap map is keyed by the unique endpoint COORDINATE, so all lines
    sharing a macro-node junction move together and stay fused. A line whose
    two endpoints collapse to the same point is dropped (degenerate).

    Returns ``(lines, edges, core_polys, n_snapped)``: fresh index-aligned
    line/edge lists (untouched lines re-emitted as the original objects),
    the core polygons WITH the inserted station vertices (geometrically
    identical -- only collinear vertices are added), and the number of
    unique endpoints snapped. ``tol <= 0`` or no cores is an identity pass.
    """
    if tol <= 0.0 or not core_polys or not lines:
        return list(lines), list(edges), list(core_polys), 0

    exteriors = [sg.LineString(list(p.exterior.coords)) for p in core_polys]

    # Unique endpoints, first-seen order (deterministic given `lines` order).
    endpoints: list[tuple[float, float]] = []
    seen: set[tuple[float, float]] = set()
    for line in lines:
        coords = list(line.coords)
        for pt in (coords[0], coords[-1]):
            if pt not in seen:
                seen.add(pt)
                endpoints.append(pt)

    # Ring index -> [(station, endpoint)] to snap onto that ring.
    per_ring: dict[int, list[tuple[float, float, tuple[float, float]]]] = {}
    for pt in endpoints:
        p = sg.Point(pt)
        dists = [ext.distance(p) for ext in exteriors]
        k = min(range(len(exteriors)), key=lambda j: (dists[j], j))
        if on_tol < dists[k] <= tol:
            per_ring.setdefault(k, []).append(
                (float(exteriors[k].project(p)), dists[k], pt)
            )

    snap_map: dict[tuple[float, float], tuple[float, float]] = {}
    out_polys = list(core_polys)
    n_snapped = 0
    for k in sorted(per_ring):
        coords = list(core_polys[k].exterior.coords)
        for station, _dist, pt in sorted(per_ring[k]):
            coords, q = _insert_vertex_at_station(coords, station)
            snap_map[pt] = q
            n_snapped += 1
        out_polys[k] = sg.Polygon(coords)

    if not snap_map:
        return list(lines), list(edges), list(core_polys), 0

    out_lines: list[sg.LineString] = []
    out_edges: list[MacroEdge] = []
    for line, rec in zip(lines, edges, strict=True):
        coords = list(line.coords)
        new_first = snap_map.get(coords[0])
        new_last = snap_map.get(coords[-1])
        if new_first is None and new_last is None:
            out_lines.append(line)
            out_edges.append(rec)
            continue
        if new_first is not None:
            coords[0] = new_first
        if new_last is not None:
            coords[-1] = new_last
        if len({(x, y) for x, y in coords}) < 2:
            continue  # degenerate: both ends collapsed to one station.
        snapped = sg.LineString(coords)
        if snapped.length <= 0.0:
            continue
        out_lines.append(snapped)
        out_edges.append(replace(rec, length=round(float(snapped.length), 3)))
    return out_lines, out_edges, out_polys, n_snapped


def prune_short_dangles(
    lines: list[sg.LineString],
    edges: list[MacroEdge],
    ring_lines: list[sg.LineString],
    *,
    max_len: float = DANGLE_PRUNE_LEN,
    protected_points: list[tuple[float, float]] | None = None,
    protect_tol: float = 0.0,
    junction_tol: float = 1e-6,
) -> tuple[list[sg.LineString], list[MacroEdge], int]:
    """Remove short degree-1 dangling fragments from the clipped network (T-geo).

    A fragment is pruned when its length is below ``max_len`` AND at least
    one of its endpoints is FREE -- i.e. has degree 1 in the macro graph:
    farther than ``junction_tol`` from every OTHER surviving line (endpoint
    coincidences AND mid-line touches both count as junctions, so dedup
    stitch points keep their piece), farther than ``junction_tol`` from
    every ring, and farther than ``protect_tol`` from every protected point
    (the macro NODE positions -- a legitimate short leaf arterial ending at
    a village node is not clip confetti and must survive). Fragments with
    ANY endpoint on a ring are never pruned, whatever their other end does:
    they carry the ring T-junction stations the S3 spokes derive from.

    Iterates to a fixpoint (pruning one stub can free its neighbour's end),
    which is order-independent within each sweep and therefore
    deterministic. Returns ``(lines, edges, n_pruned)`` -- a fresh aligned
    pair of the SURVIVING original objects (never modified, only dropped).
    ``max_len <= 0`` disables the pass.
    """
    if max_len <= 0.0 or not lines:
        return list(lines), list(edges), 0

    protected = [sg.Point(p) for p in (protected_points or [])]
    keep = [True] * len(lines)
    ends = [(sg.Point(line.coords[0]), sg.Point(line.coords[-1])) for line in lines]

    def _on_ring(p: sg.Point) -> bool:
        return any(ring.distance(p) <= junction_tol for ring in ring_lines)

    def _is_free(i: int, p: sg.Point) -> bool:
        if _on_ring(p):
            return False
        if protect_tol > 0.0 and any(q.distance(p) <= protect_tol for q in protected):
            return False
        return all(
            not keep[j] or j == i or lines[j].distance(p) > junction_tol
            for j in range(len(lines))
        )

    n_pruned = 0
    changed = True
    while changed:
        changed = False
        # Decide the whole sweep against the sweep-entry `keep` state so the
        # result does not depend on iteration order within the sweep.
        to_prune = [
            i
            for i in range(len(lines))
            if keep[i]
            and float(lines[i].length) < max_len
            and not _on_ring(ends[i][0])
            and not _on_ring(ends[i][1])
            and (_is_free(i, ends[i][0]) or _is_free(i, ends[i][1]))
        ]
        for i in to_prune:
            keep[i] = False
            n_pruned += 1
            changed = True

    out_lines = [line for line, k in zip(lines, keep, strict=True) if k]
    out_edges = [rec for rec, k in zip(edges, keep, strict=True) if k]
    return out_lines, out_edges, n_pruned


# ---------------------------------------------------------------------------
# 5.5. Ranked settlement nuclei (stage S2)
# ---------------------------------------------------------------------------
#
# Wraps each surviving core region (grown by `detect_core_regions`, UNCHANGED)
# into a ranked NucleusSpec: a downtown "anchor" point + integrated mass +
# rank + the seeding cluster's label. This is the interface the S3
# intra-nucleus avenues, the S5 zone-graded massing, and the 2D map all
# consume -- see docs/macro-roads-nuclei-plan.md, slice S2.

# Top-K nuclei (by mass) flagged `is_major=True`; the rest are town centers.
# Default matches the plan's starting guess (~6 of the ~13 detected cores);
# S2's ranked-mass table is what the orchestrator/user tune this against.
DEFAULT_N_MAJOR_NUCLEI: int = 6

# --- Intra-nucleus avenues (stage S3) ---
#
# Base radius (island units) of the inner plaza ring a nucleus's spokes
# terminate on, instead of the anchor point itself -- turns the would-be
# acute pie-wedges into annular sectors (dodges Chen's acute-wedge pathology;
# see docs/macro-roads-nuclei-plan.md, slice S3). This is roundabout-scale:
# it remains :func:`build_nucleus_avenues`'s per-call default, but MAJOR
# nuclei (the only ones that get plazas today) use the mass-scaled bounds
# below instead (slice P) -- 0.85 u (~21 m) was unreadable as a downtown
# plaza in 3D.
DEFAULT_R_PLAZA_UNITS: float = 0.85
# Mass-scaled MAJOR plaza radius bounds (island units; slice P): each major
# nucleus's plaza radius is interpolated between these by sqrt-of-mass among
# the majors (largest-mass major -> max, smallest -> min; see
# :func:`major_plaza_radii`). 3.0--4.0 u is 75--100 m at the production
# 25 m/unit -- a readable downtown plaza rather than a roundabout.
DEFAULT_R_PLAZA_MAJOR_MIN_UNITS: float = 3.0
DEFAULT_R_PLAZA_MAJOR_MAX_UNITS: float = 4.0
# Minimum angular spacing (degrees, around the nucleus anchor) between two ring
# T-junction stations before they're deduped to one spoke.
DEFAULT_SPOKE_MIN_ANGLE_DEG: float = 20.0
# Base vertex count for the (otherwise perfectly circular) plaza ring, before
# the exact spoke-endpoint vertices are folded in.
DEFAULT_PLAZA_RING_VERTICES: int = 24
# Sentinel `MacroEdge.node_a`/`node_b` for a spoke: spokes fan between a ring
# T-junction station and a plaza-ring point, neither of which is a real
# MacroNode, so there is no node index to record.
SPOKE_NODE_SENTINEL: int = -1


@dataclass(frozen=True)
class NucleusSpec:
    """A ranked settlement nucleus: one surviving dense core, wrapped.

    Built by :func:`build_nucleus_specs` from :func:`detect_core_regions`'s
    output (core polygons) -- one ``NucleusSpec`` per surviving core.

    Attributes
    ----------
    anchor : density-weighted centroid of the core region (the downtown
        center everything downstream -- exports, S5 zone massing, the 2D
        map -- treats as "the" nucleus point). Always inside ``polygon``.
    polygon : the core's ring polygon (same object ``detect_core_regions``
        returned).
    mass : ``RasterDensityField.mass(polygon)`` (integrated density) plus the
        raw in-polygon world count -- a combined density+population size
        proxy used only for ranking.
    rank : 1-indexed, highest mass first.
    label : the seeding CITY/TOWN node's cluster label (see
        :func:`build_nucleus_specs`'s docstring for the tie-break rule);
        ``None`` if there are no city/town seed nodes at all.
    influence_radius : half the core polygon's bounding-box diagonal --
        the reach used to normalize ``nucleus_dist`` downstream.
    is_major : ``True`` for the top ``n_major`` nuclei by mass.
    """

    anchor: tuple[float, float]
    polygon: sg.Polygon
    mass: float
    rank: int
    label: str | None
    influence_radius: float
    is_major: bool


def _density_weighted_centroid(
    density: np.ndarray,
    poly: sg.Polygon,
    x0: float,
    y0: float,
    cell: float,
) -> tuple[float, float]:
    """Density-weighted centroid of the raster cells whose centre falls in ``poly``.

    Midpoint rule (same convention as ``RasterDensityField.mass``): every
    inside cell contributes its ``(x, y)`` weighted by its density value.
    Falls back to ``poly.representative_point()`` (ALWAYS inside the polygon,
    unlike ``.centroid`` on a concave ring) when no cell centre falls inside
    the polygon, the in-polygon density sums to zero, or the weighted point
    itself somehow lands outside ``poly`` (a pathological, very concave core
    after ring smoothing) -- the anchor must always be inside its own core.
    """
    nrows, ncols = density.shape
    minx, miny, maxx, maxy = poly.bounds
    col_lo = max(int(np.floor((minx - x0) / cell - 0.5)), 0)
    col_hi = min(int(np.ceil((maxx - x0) / cell - 0.5)), ncols - 1)
    row_lo = max(int(np.floor((miny - y0) / cell - 0.5)), 0)
    row_hi = min(int(np.ceil((maxy - y0) / cell - 0.5)), nrows - 1)

    def _fallback() -> tuple[float, float]:
        rp = poly.representative_point()
        return float(rp.x), float(rp.y)

    if col_hi < col_lo or row_hi < row_lo:
        return _fallback()

    rows_idx, cols_idx = np.meshgrid(
        np.arange(row_lo, row_hi + 1), np.arange(col_lo, col_hi + 1), indexing="ij"
    )
    rows_flat = rows_idx.ravel()
    cols_flat = cols_idx.ravel()
    xs = x0 + (cols_flat + 0.5) * cell
    ys = y0 + (rows_flat + 0.5) * cell
    inside = shapely.contains_xy(poly, xs, ys)
    if not np.any(inside):
        return _fallback()

    d = density[rows_flat[inside], cols_flat[inside]].astype(np.float64)
    total = float(d.sum())
    if total <= 0.0:
        return _fallback()

    cx = float((xs[inside] * d).sum() / total)
    cy = float((ys[inside] * d).sum() / total)
    if not poly.contains(sg.Point(cx, cy)):
        return _fallback()
    return cx, cy


def _core_seed_label(poly: sg.Polygon, seed_nodes: list[MacroNode]) -> str | None:
    """Label a core from the seed node that grew it (see ``detect_core_regions``).

    Prefers a seed node (sigma >= 1: city or town) whose position falls
    INSIDE the core polygon, breaking ties by sigma (city over town), then
    mass, then original list order -- all deterministic. A core can be the
    UNION of several nodes' grown regions (``_union_touching_regions``), so
    more than one seed node's position can land inside; the highest-sigma /
    highest-mass one names the downtown. Falls back to the nearest seed node
    by distance to the polygon's centroid if none of them falls inside (only
    possible after the ring's buffer/Chaikin smoothing nudges the boundary
    past a seed that sat right at the grown region's edge). Returns ``None``
    if there are no seed nodes at all.
    """
    if not seed_nodes:
        return None
    inside = [nd for nd in seed_nodes if poly.contains(sg.Point(nd.x, nd.y))]
    if inside:
        best = max(
            range(len(inside)),
            key=lambda i: (inside[i].sigma, inside[i].mass or 0.0, -i),
        )
        return inside[best].label
    centroid = poly.centroid
    best = min(
        range(len(seed_nodes)),
        key=lambda i: (
            (seed_nodes[i].x - centroid.x) ** 2 + (seed_nodes[i].y - centroid.y) ** 2,
            i,
        ),
    )
    return seed_nodes[best].label


def build_nucleus_specs(
    density: np.ndarray,
    core_polys: list[sg.Polygon],
    nodes: list[MacroNode],
    points: pl.DataFrame,
    x0: float,
    y0: float,
    cell: float,
    *,
    n_major: int = DEFAULT_N_MAJOR_NUCLEI,
) -> list[NucleusSpec]:
    """Wrap each surviving core (``detect_core_regions`` output, UNCHANGED) into
    a ranked :class:`NucleusSpec`.

    ``mass`` combines the core's integrated density mass
    (``RasterDensityField.mass``, the same population-surface integral the
    Chen density-mass split/calibration uses elsewhere in this pipeline) with
    its raw in-polygon world count, as a single size proxy for ranking; cores
    are ranked descending by this ``mass`` (rank 1 = highest), ties broken by
    ``core_polys`` input order (already descending-area from
    ``detect_core_regions``) for determinism. The top ``n_major`` nuclei by
    mass are flagged ``is_major=True``.

    Returns the nuclei list ALREADY in ascending-rank order (index ``i`` ==
    ``rank - 1``); this is the id :func:`assign_nearest_nucleus` and
    :func:`nucleus_specs_to_geojson` both use.
    """
    density_field = RasterDensityField(density=density, x0=x0, y0=y0, cell=cell)
    xs = points["x"].to_numpy()
    ys = points["y"].to_numpy()
    seed_nodes = [nd for nd in nodes if nd.sigma >= 1]

    raw: list[tuple[tuple[float, float], sg.Polygon, float, str | None, float]] = []
    for poly in core_polys:
        anchor = _density_weighted_centroid(density, poly, x0, y0, cell)
        world_count = int(shapely.contains_xy(poly, xs, ys).sum())
        mass = float(density_field.mass(poly)) + float(world_count)
        minx, miny, maxx, maxy = poly.bounds
        influence_radius = 0.5 * float(np.hypot(maxx - minx, maxy - miny))
        label = _core_seed_label(poly, seed_nodes)
        raw.append((anchor, poly, mass, label, influence_radius))

    order = sorted(range(len(raw)), key=lambda i: (-raw[i][2], i))
    n_major_eff = min(max(n_major, 0), len(raw))
    specs: list[NucleusSpec] = []
    for rank0, i in enumerate(order):
        anchor, poly, mass, label, influence_radius = raw[i]
        specs.append(
            NucleusSpec(
                anchor=anchor,
                polygon=poly,
                mass=mass,
                rank=rank0 + 1,
                label=label,
                influence_radius=influence_radius,
                is_major=rank0 < n_major_eff,
            )
        )
    return specs


def assign_nearest_nucleus(
    polys: list[sg.Polygon],
    nuclei: list[NucleusSpec],
) -> list[tuple[int | None, float | None]]:
    """Nearest-nucleus assignment for a list of polygons (S5 / 2D-map interface).

    One ``(nucleus_id, nucleus_dist)`` pair per polygon, ``polys`` order
    preserved. ``nucleus_id`` is the 0-based index into ``nuclei`` (rank
    order, see :func:`build_nucleus_specs`); ``nucleus_dist`` is the polygon
    centroid -> nucleus anchor Euclidean distance, normalized by that
    nucleus's ``influence_radius`` and clamped to ``[0, 1]`` (0 = at the
    anchor, 1 = at or beyond its influence radius). Both entries are ``None``
    for every polygon when ``nuclei`` is empty (no cores detected -- nothing
    to assign to).
    """
    if not nuclei:
        return [(None, None) for _ in polys]
    anchors_x = np.array([n.anchor[0] for n in nuclei], dtype=float)
    anchors_y = np.array([n.anchor[1] for n in nuclei], dtype=float)
    radii = np.array([max(n.influence_radius, 1e-9) for n in nuclei], dtype=float)
    out: list[tuple[int | None, float | None]] = []
    for poly in polys:
        c = poly.centroid
        d2 = (anchors_x - c.x) ** 2 + (anchors_y - c.y) ** 2
        idx = int(np.argmin(d2))
        dist = float(np.sqrt(d2[idx]))
        out.append((idx, float(np.clip(dist / radii[idx], 0.0, 1.0))))
    return out


# ---------------------------------------------------------------------------
# 5.6. Intra-nucleus avenues (stage S3, the keystone slice)
# ---------------------------------------------------------------------------
#
# ``clip_arterials_to_cores`` (above) subtracts every dense core from the
# arterial network, so a raw core is a single ring-fenced, roadless blob --
# the densest places on the island have the LEAST internal road structure.
# For each MAJOR nucleus only, this section reconnects the ring back to the
# interior: it finds the ring T-junction "stations" (the on-ring endpoints
# ``clip_arterials_to_cores`` already created), dedupes stations that are too
# close together, and fans a spoke from each surviving station in to a small
# inner PLAZA ring centered on the nucleus anchor -- terminating on the plaza
# ring (an annular sector) rather than the anchor point itself (an acute
# pie-wedge) is what dodges Chen's acute-wedge subdivision pathology. See
# docs/macro-roads-nuclei-plan.md, slice S3.


def _ring_junction_stations(
    lines: list[sg.LineString],
    polygon: sg.Polygon,
    *,
    tol: float = 1e-6,
) -> list[tuple[float, float]]:
    """Endpoints of ``lines`` that sit on ``polygon``'s boundary (T-junctions).

    :func:`clip_arterials_to_cores` produces clipped arterial segments whose
    newly-cut endpoint lies (to floating precision) exactly ON the core ring
    it was clipped against -- this is how those "ring T-junction stations"
    are recovered for one nucleus's core polygon, without re-deriving them
    from the clip operation itself. Checks BOTH endpoints of every line (an
    arterial can be clipped against more than one core, or only one end may
    touch THIS particular ring); an endpoint that is a genuine macro-node
    terminus (or touches a different core) is simply not within ``tol`` and
    is skipped. Exact duplicate points (two segments meeting at the same
    station) are deduplicated, first-seen order preserved -- deterministic
    given a deterministic ``lines`` order.
    """
    boundary = polygon.exterior
    seen: set[tuple[float, float]] = set()
    stations: list[tuple[float, float]] = []
    for line in lines:
        if line.is_empty:
            continue
        coords = list(line.coords)
        for pt in (coords[0], coords[-1]):
            if pt in seen:
                continue
            if boundary.distance(sg.Point(pt)) > tol:
                continue
            seen.add(pt)
            stations.append(pt)
    return stations


def _dedupe_stations_by_angle(
    stations: list[tuple[float, float]],
    anchor: tuple[float, float],
    min_angle_rad: float,
) -> list[tuple[float, float]]:
    """Dedupe ring stations closer than ``min_angle_rad`` (circular) apart.

    Stations are ordered by their angle around ``anchor``, then the circle is
    "cut" at its single largest angular gap (so the dedup pass itself never
    has to reason about the -pi/pi wraparound: the closing gap, by
    definition the largest, is guaranteed >= every other gap and so never
    needs merging) and walked forward as a plain ascending sequence, keeping
    a station only once it is at least ``min_angle_rad`` past the last KEPT
    one. No-op for 0 or 1 stations (nothing to dedupe).
    """
    if len(stations) <= 1:
        return list(stations)
    ax, ay = anchor
    two_pi = 2.0 * np.pi
    angled = sorted((float(np.arctan2(y - ay, x - ax)), x, y) for x, y in stations)
    n = len(angled)
    gaps = [(angled[(i + 1) % n][0] - angled[i][0]) % two_pi for i in range(n)]
    cut = int(np.argmax(gaps))
    ordered = angled[cut + 1 :] + angled[: cut + 1]

    kept: list[tuple[float, float, float]] = [ordered[0]]
    for ang, x, y in ordered[1:]:
        diff = (ang - kept[-1][0]) % two_pi
        if diff >= min_angle_rad:
            kept.append((ang, x, y))
    return [(x, y) for _, x, y in kept]


def _plaza_ring_polygon(
    anchor: tuple[float, float],
    r_plaza: float,
    spoke_angles: list[float],
    *,
    n_base: int = DEFAULT_PLAZA_RING_VERTICES,
    angle_eps: float = 1e-6,
) -> sg.Polygon:
    """A circular plaza polygon at radius ``r_plaza`` around ``anchor``.

    Every spoke must terminate EXACTLY on this ring for ``polygonize`` to fuse
    them there (a spoke endpoint merely close to a discretized circle isn't
    good enough -- the same ULP-snap lesson as the resolved wedge-subdivision
    bug, see docs/macro-roads-nuclei-plan.md's "What the review found" #5/
    the deferred-section RESOLVED note), so the ring's vertex set is the union
    of ``n_base`` evenly-spaced base angles and every ``spoke_angles`` value,
    ALL placed at the same exact radius ``r_plaza``. A base angle within
    ``angle_eps`` (circular) of a spoke angle is dropped, keeping the spoke's
    own exact vertex instead of a near-duplicate.
    """
    ax, ay = anchor
    two_pi = 2.0 * np.pi

    def _circ_dist(a: float, b: float) -> float:
        return abs(((a - b + np.pi) % two_pi) - np.pi)

    # Normalize EVERY angle into the same [0, 2*pi) convention before sorting
    # -- ``spoke_angles`` come from ``atan2`` (range (-pi, pi]), which sorts
    # inconsistently against a plain ``[0, 2*pi)`` base sweep and silently
    # produces a self-intersecting (non-convex, wrongly-ordered) ring.
    norm_spokes = [float(s % two_pi) for s in spoke_angles]
    base_angles = [two_pi * k / n_base for k in range(n_base)]
    kept_base = [
        a for a in base_angles if all(_circ_dist(a, s) > angle_eps for s in norm_spokes)
    ]
    all_angles = sorted([*kept_base, *norm_spokes])
    coords = [
        (ax + r_plaza * float(np.cos(a)), ay + r_plaza * float(np.sin(a)))
        for a in all_angles
    ]
    coords.append(coords[0])
    return sg.Polygon(coords)


def build_nucleus_avenues(
    nucleus: NucleusSpec,
    clipped_lines: list[sg.LineString],
    *,
    r_plaza: float = DEFAULT_R_PLAZA_UNITS,
    min_spoke_angle_deg: float = DEFAULT_SPOKE_MIN_ANGLE_DEG,
    n_plaza_vertices: int = DEFAULT_PLAZA_RING_VERTICES,
) -> tuple[list[sg.LineString], list[MacroEdge], sg.Polygon] | None:
    """Build one MAJOR nucleus's intra-nucleus avenues (S3, the keystone slice).

    Finds the ring T-junction stations where ``clipped_lines`` meet
    ``nucleus.polygon``'s boundary (:func:`_ring_junction_stations`), dedupes
    stations closer than ``min_spoke_angle_deg`` apart around the anchor
    (:func:`_dedupe_stations_by_angle`), then emits one spoke LineString per
    surviving station, from the station to a point on the plaza ring (radius
    ``r_plaza``, centered on ``nucleus.anchor``) at THE SAME angle -- so the
    plaza ring built from those exact angles (:func:`_plaza_ring_polygon`)
    always meets every spoke exactly. Every spoke is tagged
    ``tier=1`` ("major"); ``node_a``/``node_b`` are ``SPOKE_NODE_SENTINEL``
    (a spoke doesn't tie to a real macro node, only to its ring station and
    the plaza).

    A station no farther from the anchor than ``r_plaza`` would produce a
    degenerate/inverted spoke and is dropped; if that empties the (deduped)
    station set entirely -- e.g. a core small enough that its ring sits
    inside the plaza radius -- returns ``None``: no avenues, no plaza, for
    that nucleus (a graceful degenerate-case fallback, not an error). Also
    returns ``None`` if there are no ring stations at all (an isolated core
    with no arterial reaching its ring).
    """
    stations = _ring_junction_stations(clipped_lines, nucleus.polygon)
    if not stations:
        return None
    deduped = _dedupe_stations_by_angle(
        stations, nucleus.anchor, float(np.radians(min_spoke_angle_deg))
    )
    ax, ay = nucleus.anchor
    valid = [(x, y) for x, y in deduped if np.hypot(x - ax, y - ay) > r_plaza]
    if not valid:
        return None

    angles = [float(np.arctan2(y - ay, x - ax)) for x, y in valid]
    plaza_poly = _plaza_ring_polygon(
        nucleus.anchor, r_plaza, angles, n_base=n_plaza_vertices
    )

    spoke_lines: list[sg.LineString] = []
    spoke_edges: list[MacroEdge] = []
    for (sx, sy), ang in zip(valid, angles, strict=True):
        px = ax + r_plaza * float(np.cos(ang))
        py = ay + r_plaza * float(np.sin(ang))
        spoke = sg.LineString([(sx, sy), (px, py)])
        spoke_lines.append(spoke)
        spoke_edges.append(
            MacroEdge(
                node_a=SPOKE_NODE_SENTINEL,
                node_b=SPOKE_NODE_SENTINEL,
                tau=1,
                tier=1,
                path_cost=round(float(spoke.length), 4),
                length=round(float(spoke.length), 3),
            )
        )
    return spoke_lines, spoke_edges, plaza_poly


def major_plaza_radii(
    nuclei: list[NucleusSpec],
    *,
    r_min: float = DEFAULT_R_PLAZA_MAJOR_MIN_UNITS,
    r_max: float = DEFAULT_R_PLAZA_MAJOR_MAX_UNITS,
) -> dict[int, float]:
    """Mass-scaled plaza radius per MAJOR nucleus, keyed by ``NucleusSpec.rank``.

    Interpolates between ``r_min`` and ``r_max`` on sqrt-of-mass among the
    majors (slice P): the largest-mass major gets exactly ``r_max``, the
    smallest exactly ``r_min``, the rest linear in sqrt(mass) between them.
    sqrt is the natural interpolant here because ``mass`` is an area-
    integrated (quadratic-in-length) size proxy, so sqrt(mass) is
    length-scale-proportional -- a plaza radius is a length. Non-major
    nuclei get NO entry (they get no plaza at all, see
    :func:`add_intra_nucleus_avenues`). Degenerate cases -- a single major,
    or all majors with equal mass -- make every major "the largest", so all
    get ``r_max``. Keys are ranks (1-indexed, unique per nucleus), values a
    deterministic function of the majors' masses alone.
    """
    majors = [n for n in nuclei if n.is_major]
    if not majors:
        return {}
    roots = {n.rank: float(np.sqrt(max(n.mass, 0.0))) for n in majors}
    lo = min(roots.values())
    hi = max(roots.values())
    if hi - lo <= 1e-12:
        return {rank: r_max for rank in roots}
    return {
        rank: r_min + (root - lo) / (hi - lo) * (r_max - r_min)
        for rank, root in roots.items()
    }


def add_intra_nucleus_avenues(
    nuclei: list[NucleusSpec],
    clipped_lines: list[sg.LineString],
    *,
    r_plaza_major_min: float = DEFAULT_R_PLAZA_MAJOR_MIN_UNITS,
    r_plaza_major_max: float = DEFAULT_R_PLAZA_MAJOR_MAX_UNITS,
    min_spoke_angle_deg: float = DEFAULT_SPOKE_MIN_ANGLE_DEG,
    n_plaza_vertices: int = DEFAULT_PLAZA_RING_VERTICES,
) -> tuple[list[sg.LineString], list[MacroEdge], list[sg.Polygon]]:
    """Build intra-nucleus avenues for every MAJOR nucleus (S3).

    Town-center (non-major) nuclei are untouched -- they keep the pre-S3
    ring-fenced, roadless core. Each major's plaza radius is mass-scaled
    between ``r_plaza_major_min`` and ``r_plaza_major_max``
    (:func:`major_plaza_radii`, slice P) rather than the single roundabout-
    scale ``DEFAULT_R_PLAZA_UNITS``. Iterates ``nuclei`` in its given (rank-
    ascending, see :func:`build_nucleus_specs`) order, so the result is
    deterministic and independent of any upstream construction order.
    Returns the combined spoke LineStrings, their parallel ``MacroEdge``
    records, and one plaza Polygon per major nucleus that got avenues (a
    nucleus :func:`build_nucleus_avenues` returned ``None`` for contributes
    nothing to any of the three lists).
    """
    radii = major_plaza_radii(nuclei, r_min=r_plaza_major_min, r_max=r_plaza_major_max)
    spoke_lines: list[sg.LineString] = []
    spoke_edges: list[MacroEdge] = []
    plaza_polys: list[sg.Polygon] = []
    for nucleus in nuclei:
        if not nucleus.is_major:
            continue
        result = build_nucleus_avenues(
            nucleus,
            clipped_lines,
            r_plaza=radii[nucleus.rank],
            min_spoke_angle_deg=min_spoke_angle_deg,
            n_plaza_vertices=n_plaza_vertices,
        )
        if result is None:
            continue
        lines, edges, plaza = result
        spoke_lines.extend(lines)
        spoke_edges.extend(edges)
        plaza_polys.append(plaza)
    return spoke_lines, spoke_edges, plaza_polys


def _polygonize_macro_blocks_protecting_nuclei(
    lines: list[sg.LineString],
    boundary: sg.Polygon,
    min_block_area: float,
    protect_polys: list[sg.Polygon],
) -> list[sg.Polygon]:
    """Polygonize macro-blocks, exempting major-nucleus interiors from the
    coarse sliver-merge floor.

    ``min_block_area`` (default 60 island-units^2) is calibrated for coarse,
    continent-scale macro-blocks; S3's intra-nucleus wedge/plaza faces are
    deliberately much finer (a downtown split into a handful of wedges around
    a ~2-unit^2 plaza) and would otherwise get dissolved right back into a
    neighboring face by the generic merge, undoing the whole slice. Any raw
    polygonize face whose area is MOSTLY covered by ``protect_polys`` (the
    MAJOR nuclei's own core polygons -- both the wedges and the central
    plaza fall entirely inside them by construction) is kept AS-IS regardless
    of area; every other face still goes through the ordinary
    :func:`mapgen.r1_arm_b._merge_slivers` pass, exactly as before. With no
    ``protect_polys`` this is byte-identical to
    :func:`mapgen.r1_arm_b.polygonize_districts` (the pre-S3 behavior).
    """
    if not protect_polys:
        return polygonize_districts(lines, boundary, min_district_area=min_block_area)

    boundary_ring = sg.LineString(list(boundary.exterior.coords))
    merged = unary_union([*lines, boundary_ring])
    raw_polys = list(polygonize(merged))
    inside = [p for p in raw_polys if boundary.contains(p) or boundary.covers(p)]
    if not inside:
        inside = [
            p.intersection(boundary)
            for p in raw_polys
            if not p.intersection(boundary).is_empty
        ]
        inside = [p for p in inside if p.geom_type == "Polygon"]
    if not inside:
        return [boundary]

    protect_union = unary_union(protect_polys)
    protected: list[sg.Polygon] = []
    mergeable: list[sg.Polygon] = []
    for p in inside:
        if p.area > 0 and p.intersection(protect_union).area >= 0.5 * p.area:
            protected.append(p)
        else:
            mergeable.append(p)

    merged_rest = _merge_slivers(mergeable, min_block_area) if mergeable else []
    out = [*protected, *merged_rest]
    out.sort(key=lambda poly: poly.area, reverse=True)
    return out


# ---------------------------------------------------------------------------
# 5.7. Functional road hierarchy (slice B: path-coverage tier promotion)
# ---------------------------------------------------------------------------
#
# Construction tiers (compute_macro_arterials) record the ORDER the network
# was grown in, not the role a segment plays in the final geometry: tiers are
# routed independently and `clip_arterials_to_cores` preserves tier per
# fragment, so at overview the highway tier reads as short red fragments
# interleaved along mostly-lower-tier corridors ("confetti") instead of a
# continuous skeleton between the major downtowns. This pass re-derives tier
# as a FUNCTION of the final macro geometry (post clip + snap + prune +
# spokes): the linework -- clipped arterials, spokes, core rings, plaza
# rings -- is noded into a junction graph, least-cost (length-weighted)
# paths are traced between the ranked nuclei anchors, and
#
#   highway (2) = union of edges on a MAJOR-nuclei pair path,
#   major   (1) = union of edges on any other nuclei pair path,
#   local   (0) = everything else.
#
# Corridors are continuous by construction (a path is connected), and rings/
# spokes participate identically: a ring arc that carries a city-city path IS
# a highway arc. Length-weighted edge betweenness centrality is computed
# once on the same graph and stored per record -- S7-slim's width input; it
# is NOT otherwise consumed in this slice.
# See docs/macro-roads-nuclei-plan.md, "Road-hierarchy restructure", slice B.

# Node-merge / on-line tolerance (island units) for the junction graph. Real
# junctions are EXACT shared vertices by T-geo/S3 construction; this matches
# `_ring_junction_stations`'s exact-on-ring tolerance so clip-cut arterial
# endpoints (on a ring edge to floating precision, not necessarily a ring
# vertex) still register as ring junctions.
JUNCTION_NODE_TOL: float = 1e-6
# Sentinel `MacroEdge.node_a`/`node_b` for a ring-arc record (like
# SPOKE_NODE_SENTINEL: a ring arc ties to no real MacroNode).
RING_ARC_NODE_SENTINEL: int = -2


def _iter_intersection_points(geom: Any, out: set[tuple[float, float]]) -> None:
    """Collect junction point coords from a pairwise line intersection.

    Point/MultiPoint components are junctions directly; a 1-D component (a
    collinear overlap, which post-dedup should not occur between distinct
    macro lines but is handled defensively) contributes its two end points.
    GeometryCollections recurse.
    """
    if geom.is_empty:
        return
    gtype = geom.geom_type
    if gtype == "Point":
        out.add((float(geom.x), float(geom.y)))
    elif gtype in ("LineString", "LinearRing"):
        coords = list(geom.coords)
        out.add((float(coords[0][0]), float(coords[0][1])))
        out.add((float(coords[-1][0]), float(coords[-1][1])))
    elif hasattr(geom, "geoms"):
        for g in geom.geoms:
            _iter_intersection_points(g, out)


def _merge_close_points(
    points: list[tuple[float, float]], tol: float
) -> dict[tuple[float, float], tuple[float, float]]:
    """Map each point to a canonical representative, clustering within ``tol``.

    Union-find over the (sorted) input points, joining pairs within ``tol``
    (Euclidean); each cluster's canonical point is its lexicographically
    smallest member, so the result is deterministic and independent of input
    order. Real junctions share EXACT coordinates by construction, so
    clusters are almost always singletons -- this guards the rare case of a
    transversal-crossing intersection landing within tolerance of an exact
    endpoint junction.
    """
    pts = sorted(set(points))
    if not pts:
        return {}
    parent = list(range(len(pts)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    geoms = [sg.Point(p) for p in pts]
    tree = STRtree(geoms)
    left, right = tree.query(geoms, predicate="dwithin", distance=tol)
    for i, j in zip(left.tolist(), right.tolist(), strict=True):
        if i >= j:
            continue
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[max(ri, rj)] = min(ri, rj)

    canon: dict[tuple[float, float], tuple[float, float]] = {}
    cluster_min: dict[int, tuple[float, float]] = {}
    for i, p in enumerate(pts):
        root = find(i)
        if root not in cluster_min or p < cluster_min[root]:
            cluster_min[root] = p
    for i, p in enumerate(pts):
        canon[p] = cluster_min[find(i)]
    return canon


@dataclass(frozen=True)
class _GraphSeg:
    """One junction-to-junction segment of one source line."""

    src: int  # index into the combined (arterials+spokes, then rings) list
    seg: int  # ordinal along the source line (station order)
    u: int  # graph node id (sorted-canonical-point index)
    v: int
    geom: sg.LineString


def build_macro_junction_graph(
    all_lines: list[sg.LineString],
    *,
    tol: float = JUNCTION_NODE_TOL,
) -> tuple[nx.MultiGraph, list[tuple[float, float]], list[_GraphSeg]]:
    """Node the final macro linework into a junction MultiGraph (slice B).

    Junction nodes are (a) every line endpoint (for a closed ring, its seam
    vertex -- an arbitrary degree-2 node that harmlessly splits one arc) and
    (b) every true pairwise intersection point, clustered within ``tol``
    (:func:`_merge_close_points`). Each line is then split at every node
    lying within ``tol`` of it; the pieces become graph edges weighted by
    geometric length, keyed ``(src, seg)`` so parallel edges between the
    same node pair (e.g. the two arcs of a ring between two stations) keep
    independent identities. Cut-end coordinates are replaced by the exact
    canonical node point, so pieces sharing a junction share an exact
    vertex.

    Returns ``(graph, node_points, segments)``: node ``i``'s coordinate is
    ``node_points[i]`` (lexicographically sorted -- deterministic ids), and
    ``segments`` lists every emitted piece in ``(src, station)`` order.
    Deterministic: node ids, adjacency insertion order and segment order are
    all functions of the input geometry alone.
    """
    G: nx.MultiGraph = nx.MultiGraph()
    if not all_lines:
        return G, [], []

    # --- Junction candidate points: endpoints + pairwise intersections. ---
    raw_pts: set[tuple[float, float]] = set()
    for line in all_lines:
        coords = line.coords
        raw_pts.add((float(coords[0][0]), float(coords[0][1])))
        raw_pts.add((float(coords[-1][0]), float(coords[-1][1])))
    tree = STRtree(all_lines)
    left, right = tree.query(all_lines, predicate="intersects")
    for i, j in zip(left.tolist(), right.tolist(), strict=True):
        if i >= j:
            continue
        _iter_intersection_points(all_lines[i].intersection(all_lines[j]), raw_pts)

    canon = _merge_close_points(list(raw_pts), tol)
    node_pts = sorted(set(canon.values()))
    node_id = {p: i for i, p in enumerate(node_pts)}
    node_geoms = [sg.Point(p) for p in node_pts]
    node_tree = STRtree(node_geoms)

    # --- Split every line at its on-line nodes; add pieces as edges. ---
    segments: list[_GraphSeg] = []
    for src, line in enumerate(all_lines):
        total = float(line.length)
        coords = list(line.coords)
        closed = coords[0] == coords[-1]
        near = sorted(node_tree.query(line, predicate="dwithin", distance=tol).tolist())
        stations: list[tuple[float, int]] = []
        for k in near:
            s = float(line.project(node_geoms[k]))
            if s <= tol:
                s = 0.0
            elif s >= total - tol and not closed:
                s = total
            stations.append((s, k))
        stations.sort()
        # Drop stations collapsing onto an earlier one (post-merge this only
        # happens when two merged candidates project to the same spot).
        deduped: list[tuple[float, int]] = []
        for s, k in stations:
            if deduped and (s - deduped[-1][0] <= 1e-9 or k == deduped[-1][1]):
                continue
            deduped.append((s, k))
        if not deduped:
            # Degenerate: no node within tol. Cannot happen for nonempty
            # lines (their endpoints are nodes); guarded for robustness.
            continue
        if closed:
            # The seam vertex projects to station 0 and is always a node, so
            # arcs are consecutive station pairs plus a closing arc back to
            # the seam node (no wraparound bookkeeping needed).
            pairs = list(zip(deduped, deduped[1:], strict=False))
            pairs.append(((deduped[-1][0], deduped[-1][1]), (total, deduped[0][1])))
        else:
            pairs = list(zip(deduped, deduped[1:], strict=False))
        seg_ord = 0
        for (s_a, k_a), (s_b, k_b) in pairs:
            if s_b - s_a <= 1e-9:
                continue
            piece = substring(line, s_a, s_b)
            pcoords = list(piece.coords)
            if len(pcoords) < 2:
                continue
            pcoords[0] = node_pts[k_a]
            pcoords[-1] = node_pts[k_b]
            if len(pcoords) == 2 and pcoords[0] == pcoords[-1]:
                continue
            piece = sg.LineString(pcoords)
            u, v = node_id[node_pts[k_a]], node_id[node_pts[k_b]]
            G.add_edge(u, v, key=(src, seg_ord), length=float(piece.length))
            segments.append(_GraphSeg(src=src, seg=seg_ord, u=u, v=v, geom=piece))
            seg_ord += 1
    return G, node_pts, segments


def _nearest_node(
    anchor: tuple[float, float], node_pts: list[tuple[float, float]]
) -> int:
    """Graph node nearest ``anchor`` (ties break on the lower node id)."""
    ax, ay = anchor
    return min(
        range(len(node_pts)),
        key=lambda i: (
            (node_pts[i][0] - ax) ** 2 + (node_pts[i][1] - ay) ** 2,
            i,
        ),
    )


def _min_parallel_key(G: nx.MultiGraph, a: int, b: int) -> tuple[int, int]:
    """The parallel edge between ``a``/``b`` a length-weighted Dijkstra uses.

    networkx Dijkstra on a MultiGraph relaxes with the MINIMUM weight over
    parallel edges, so the min-``length`` edge (key tie-break: smallest key)
    is the one a returned path traverses -- deterministic.
    """
    return min(G[a][b].items(), key=lambda kv: (kv[1]["length"], kv[0]))[0]


def _promote_pair_paths(
    G: nx.MultiGraph,
    anchor_nodes: list[int],
    pair_indices: list[tuple[int, int]],
) -> tuple[set[tuple[int, int]], int]:
    """Union of edge keys on least-cost paths between anchor pairs.

    Runs one ``single_source_dijkstra`` per unique source anchor node (cached)
    and walks each pair's path, collecting the traversed edge keys via
    :func:`_min_parallel_key`. Returns ``(keys, n_disconnected)`` where
    ``n_disconnected`` counts the pairs with NO path (distinct anchors in
    different graph components) -- reported, never raised.
    """
    keys: set[tuple[int, int]] = set()
    n_disconnected = 0
    paths_from: dict[int, dict[int, list[int]]] = {}
    for i, j in pair_indices:
        s, t = anchor_nodes[i], anchor_nodes[j]
        if s == t:
            continue  # two nuclei sharing one anchor node: trivially connected.
        if s not in paths_from:
            _dist, paths = nx.single_source_dijkstra(G, s, weight="length")
            paths_from[s] = paths
        path = paths_from[s].get(t)
        if path is None:
            n_disconnected += 1
            continue
        for a, b in zip(path, path[1:], strict=False):
            keys.add(_min_parallel_key(G, a, b))
    return keys, n_disconnected


def assign_functional_tiers(
    lines: list[sg.LineString],
    edges: list[MacroEdge],
    ring_lines: list[sg.LineString],
    nuclei: list[NucleusSpec],
    *,
    tol: float = JUNCTION_NODE_TOL,
) -> tuple[
    list[sg.LineString],
    list[MacroEdge],
    list[sg.LineString],
    list[MacroEdge],
    int,
    int,
]:
    """Re-derive road tier as path coverage on the final macro geometry (B).

    Nodes ``lines`` (clipped arterials + spokes) together with ``ring_lines``
    (core + plaza rings) into a junction graph
    (:func:`build_macro_junction_graph`), maps every nucleus anchor to its
    nearest graph node (:func:`_nearest_node` -- for a major with a plaza
    that is a plaza-ring spoke junction; for a ringed core with no stations,
    the nearest junction elsewhere, deterministically), then:

    1. **Highway (tier 2):** union of edges on least-cost (length-weighted)
       paths between all pairs of MAJOR nuclei (``NucleusSpec.is_major``).
    2. **Major (tier 1):** union of edges on paths between all nuclei pairs,
       minus the highway set.
    3. **Local (tier 0):** every other edge.

    Emission re-segments the inputs at graph junctions: each arterial/spoke
    record is re-emitted as one record PER junction-to-junction piece
    (fragments of one line stay adjacent, station-ordered -- the same
    fresh-aligned-pair convention as ``clip_arterials_to_cores``), carrying
    the functional ``tier``, the construction tier in ``build_tier``, its
    graph edge's length-weighted betweenness centrality (exact,
    ``normalized=True``) in ``betweenness``, and an updated ``length``.
    Ring arcs are emitted as a NEW aligned pair (``ring_lines`` itself stays
    whole rings for every existing consumer); their records use
    ``RING_ARC_NODE_SENTINEL`` node ids and ``build_tier=-1``.

    Determinism: node ids are sorted canonical coordinates, Dijkstra
    tie-breaks follow the deterministic adjacency insertion order, and
    parallel edges resolve via :func:`_min_parallel_key`.

    Degenerate inputs -- fewer than two nuclei (no pair to trace: relabeling
    would demote EVERYTHING to local) or no linework -- are an identity
    pass: the inputs are returned unchanged (fresh lists, same objects), no
    ring arcs, zero counters.

    Returns ``(lines, edges, ring_arc_lines, ring_arc_edges, n_tier_changed,
    n_disconnected_pairs)`` where ``n_tier_changed`` counts arterial/spoke
    records whose functional tier differs from their construction tier and
    ``n_disconnected_pairs`` counts nucleus pairs with no connecting path
    (over the all-nuclei pass, which includes the major pairs).
    """
    if len(nuclei) < 2 or not (lines or ring_lines):
        return list(lines), list(edges), [], [], 0, 0

    n_art = len(lines)
    all_lines = [*lines, *ring_lines]
    G, node_pts, segments = build_macro_junction_graph(all_lines, tol=tol)
    if not node_pts:
        return list(lines), list(edges), [], [], 0, 0

    anchor_nodes = [_nearest_node(n.anchor, node_pts) for n in nuclei]
    major_idx = [i for i, n in enumerate(nuclei) if n.is_major]
    major_pairs = [
        (major_idx[a], major_idx[b])
        for a in range(len(major_idx))
        for b in range(a + 1, len(major_idx))
    ]
    all_pairs = [(a, b) for a in range(len(nuclei)) for b in range(a + 1, len(nuclei))]

    highway_keys, _ = _promote_pair_paths(G, anchor_nodes, major_pairs)
    all_keys, n_disconnected = _promote_pair_paths(G, anchor_nodes, all_pairs)
    major_keys = all_keys - highway_keys

    bt_raw = nx.edge_betweenness_centrality(G, weight="length", normalized=True)
    bt_by_key: dict[tuple[int, int], float] = {
        k: b for (_u, _v, k), b in bt_raw.items()
    }

    def _tier_of(key: tuple[int, int]) -> int:
        if key in highway_keys:
            return 2
        if key in major_keys:
            return 1
        return 0

    out_lines: list[sg.LineString] = []
    out_edges: list[MacroEdge] = []
    ring_arc_lines: list[sg.LineString] = []
    ring_arc_edges: list[MacroEdge] = []
    n_tier_changed = 0
    pieces_per_src: dict[int, int] = {}
    for s in segments:
        pieces_per_src[s.src] = pieces_per_src.get(s.src, 0) + 1
    for s in segments:  # already in (src, station) order
        key = (s.src, s.seg)
        tier = _tier_of(key)
        bt = round(float(bt_by_key.get(key, 0.0)), 6)
        if s.src < n_art:
            rec = edges[s.src]
            geom = s.geom
            # A line emitted as a single unsplit piece keeps its original
            # geometry object (bit-identical), like every prior re-emit pass.
            if pieces_per_src[s.src] == 1 and list(geom.coords) == list(
                lines[s.src].coords
            ):
                geom = lines[s.src]
            out_lines.append(geom)
            out_edges.append(
                replace(
                    rec,
                    tier=tier,
                    build_tier=rec.tier,
                    betweenness=bt,
                    length=round(float(geom.length), 3),
                )
            )
            if tier != rec.tier:
                n_tier_changed += 1
        else:
            ring_arc_lines.append(s.geom)
            ring_arc_edges.append(
                MacroEdge(
                    node_a=RING_ARC_NODE_SENTINEL,
                    node_b=RING_ARC_NODE_SENTINEL,
                    tau=0,
                    tier=tier,
                    path_cost=round(float(s.geom.length), 4),
                    length=round(float(s.geom.length), 3),
                    build_tier=-1,
                    betweenness=bt,
                )
            )
    return (
        out_lines,
        out_edges,
        ring_arc_lines,
        ring_arc_edges,
        n_tier_changed,
        n_disconnected,
    )


@dataclass(frozen=True)
class MacroBlockBundle:
    """Result of :func:`build_macro_blocks_with_cores`.

    Attributes
    ----------
    core_polys : detected dense-core "downtown" polygons (ringed blocks),
        AFTER slice R's rank-scaled ring regularization
        (:func:`_regularize_ring_polygons`).
    ring_lines : each core's exterior ring as a LineString (drawn as ring
        roads), PLUS one plaza ring per major nucleus that got avenues (S3)
        -- both are "ring"-kind macro features downstream (gate snapping,
        connectivity metrics) treat identically.
    clipped_lines / clipped_edges : arterials with their core interiors
        removed, PLUS the S3 intra-nucleus spoke LineStrings/records
        (unclipped -- they are supposed to enter the core). After the
        slice-B functional relabel (:func:`assign_functional_tiers`, ``>= 2``
        nuclei) these are re-segmented at graph junctions and carry
        functional ``tier`` / ``build_tier`` / ``betweenness``.
    blocks : polygonized macro-blocks over clipped arterials + spokes + rings
        (core + plaza) + boundary.
    nuclei : ranked :class:`NucleusSpec` list, one per surviving core (S2).
    plaza_polys : one inner plaza disc per MAJOR nucleus that got avenues
        (S3), mass-scaled between ``DEFAULT_R_PLAZA_MAJOR_MIN/MAX_UNITS``
        (slice P). The greybox export (``scripts/run_r1_hybrid.py``) must
        EXCLUDE the districts covered by these discs from world assignment
        and force them ``kind="park"`` -- the S3 assumption that the
        zero-world typology alone would park them was wrong: worlds whose
        raw coordinates fall inside a plaza otherwise direct-assign there
        and turn the plaza into subdivided fabric. Shorter than ``nuclei``
        whenever a major nucleus's core is too small for any spoke to clear
        the plaza radius, or has no ring T-junction stations at all (see
        :func:`build_nucleus_avenues`).
    """

    core_polys: list[sg.Polygon]
    ring_lines: list[sg.LineString]
    clipped_lines: list[sg.LineString]
    clipped_edges: list[MacroEdge]
    blocks: list[sg.Polygon]
    nuclei: list[NucleusSpec]
    plaza_polys: list[sg.Polygon]
    # T-geo observability counters (informational; see snap_endpoints_to_rings
    # and prune_short_dangles).
    n_endpoints_snapped: int = 0
    n_dangles_pruned: int = 0
    # Slice B: junction-to-junction ring arcs with functional tiers +
    # betweenness (``ring_lines`` above stays whole rings for existing
    # consumers; these aligned arc records are S7-slim's per-arc width
    # input), plus the relabel counters (see assign_functional_tiers).
    ring_arc_lines: list[sg.LineString] = field(default_factory=list)
    ring_arc_edges: list[MacroEdge] = field(default_factory=list)
    n_tier_changed: int = 0
    n_disconnected_nucleus_pairs: int = 0


def build_macro_blocks_with_cores(
    density: np.ndarray,
    nodes: list[MacroNode],
    arterial_lines: list[sg.LineString],
    edges: list[MacroEdge],
    boundary: sg.Polygon,
    points: pl.DataFrame,
    x0: float,
    y0: float,
    cell: float,
    *,
    core_frac: float = DEFAULT_CORE_FRAC,
    core_max_radius_units: float = DEFAULT_CORE_MAX_RADIUS_UNITS,
    core_min_area_units2: float = DEFAULT_CORE_MIN_AREA_UNITS2,
    peak_window_units: float = DEFAULT_CORE_PEAK_WINDOW_UNITS,
    core_simplify_tolerance: float = DEFAULT_CORE_SIMPLIFY_TOLERANCE,
    core_close_width: float | None = DEFAULT_CORE_CLOSE_WIDTH_UNITS,
    core_smooth_iterations: int = DEFAULT_CORE_SMOOTH_ITERATIONS,
    ring_open_width_major: float = DEFAULT_RING_OPEN_WIDTH_MAJOR_UNITS,
    ring_open_width_minor: float = DEFAULT_RING_OPEN_WIDTH_MINOR_UNITS,
    ring_fourier_refit: bool = False,
    ring_fourier_harmonics: int = DEFAULT_RING_FOURIER_HARMONICS,
    min_block_area: float = DEFAULT_MIN_BLOCK_AREA,
    n_major_nuclei: int = DEFAULT_N_MAJOR_NUCLEI,
    r_plaza_major_min: float = DEFAULT_R_PLAZA_MAJOR_MIN_UNITS,
    r_plaza_major_max: float = DEFAULT_R_PLAZA_MAJOR_MAX_UNITS,
    spoke_min_angle_deg: float = DEFAULT_SPOKE_MIN_ANGLE_DEG,
    plaza_ring_vertices: int = DEFAULT_PLAZA_RING_VERTICES,
    ring_snap_tol: float = RING_SNAP_TOL,
    dangle_prune_len: float = DANGLE_PRUNE_LEN,
    functional_tiers: bool = True,
) -> MacroBlockBundle:
    """Fold dense cores into ringed downtown blocks (stage 3.5b wrapper).

    A NEW step on top of the unchanged :func:`compute_macro_arterials` output:

    1. Detect dense core regions around CITY/TOWN nodes
       (:func:`detect_core_regions`).
    1b. Rank-scaled ring regularization (:func:`_regularize_ring_polygons`,
       slice R): a morphological OPENING severs keyhole necks/lobes on MAJOR
       cores (minor cores keep the cheap ``_cells_to_polygon`` contour),
       optionally followed by a low-pass Fourier refit (off by default).
       Runs BEFORE clip/snap so the clip boundary, block-protection region,
       nucleus mass/rank domain and spoke-station source all re-derive from
       the SAME regularized polygon.
    2. Clip arterials to the cores' exterior (:func:`clip_arterials_to_cores`)
       so density-attracting arterials terminate ON the ring (T-junctions)
       rather than converging on the summit.
    2b. Snap near-ring endpoints onto the ring
       (:func:`snap_endpoints_to_rings`) and prune short degree-1 dangling
       fragments (:func:`prune_short_dangles`) -- slice T-geo. Both re-emit
       fresh aligned line/edge pairs; macro-node endpoints are protected
       from the prune (within one raster cell -- routed endpoints sit at the
       node's CELL CENTER, not the exact node xy).
    3. Wrap each surviving core into a ranked :class:`NucleusSpec`
       (:func:`build_nucleus_specs`, S2).
    4. Fan intra-nucleus avenues (spokes + a plaza ring) for every MAJOR
       nucleus (:func:`add_intra_nucleus_avenues`, S3) -- UNCLIPPED, they are
       supposed to enter the core.
    4b. Re-derive tier as PATH COVERAGE on the final geometry
       (:func:`assign_functional_tiers`, slice B, when ``functional_tiers``
       and at least two nuclei exist): arterials + spokes are re-segmented
       at graph junctions and relabeled (construction tier preserved in
       ``build_tier``, length-weighted betweenness stored), and ring arcs
       are emitted as a new aligned pair. Geometry union is unchanged, so
       the polygonization below is unaffected.
    5. Polygonize ``clipped_arterials ∪ spokes ∪ core_rings ∪ plaza_rings ∪
       island_exterior``, protecting the (deliberately fine-grained) wedge/
       plaza faces inside a major nucleus from the coarse macro-block
       sliver-merge floor (:func:`_polygonize_macro_blocks_protecting_nuclei`)
       so each core interior splits into its wedges + plaza rather than one
       ring-fenced block, and non-core space splits along the clipped
       arterial web exactly as before.

    Returns a :class:`MacroBlockBundle`. With no cores detected, the block set
    matches the pre-3.5b polygonization (arterials + boundary), ``nuclei`` is
    empty, and no avenues are built.
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
        close_width=core_close_width,
        smooth_iterations=core_smooth_iterations,
    )
    core_polys = _regularize_ring_polygons(
        core_polys,
        density,
        nodes,
        points,
        x0,
        y0,
        cell,
        n_major_nuclei=n_major_nuclei,
        open_width_major=ring_open_width_major,
        open_width_minor=ring_open_width_minor,
        fourier_refit=ring_fourier_refit,
        fourier_harmonics=ring_fourier_harmonics,
    )
    clipped_lines, clipped_edges = clip_arterials_to_cores(
        arterial_lines, edges, core_polys
    )
    # T-geo 2b: snap near-ring endpoints onto the ring (also inserts each
    # snapped station as an exact ring VERTEX -- core_polys is updated), then
    # prune short degree-1 dangles. Ring lines derive from the UPDATED polys.
    clipped_lines, clipped_edges, core_polys, n_endpoints_snapped = (
        snap_endpoints_to_rings(
            clipped_lines, clipped_edges, core_polys, tol=ring_snap_tol
        )
    )
    ring_lines = core_ring_boundaries(core_polys)
    clipped_lines, clipped_edges, n_dangles_pruned = prune_short_dangles(
        clipped_lines,
        clipped_edges,
        ring_lines,
        max_len=dangle_prune_len,
        protected_points=[(nd.x, nd.y) for nd in nodes],
        protect_tol=cell,
    )
    nuclei = build_nucleus_specs(
        density, core_polys, nodes, points, x0, y0, cell, n_major=n_major_nuclei
    )

    spoke_lines, spoke_edges, plaza_polys = add_intra_nucleus_avenues(
        nuclei,
        clipped_lines,
        r_plaza_major_min=r_plaza_major_min,
        r_plaza_major_max=r_plaza_major_max,
        min_spoke_angle_deg=spoke_min_angle_deg,
        n_plaza_vertices=plaza_ring_vertices,
    )
    final_lines = [*clipped_lines, *spoke_lines]
    final_edges = [*clipped_edges, *spoke_edges]
    plaza_ring_lines = [sg.LineString(list(p.exterior.coords)) for p in plaza_polys]
    final_ring_lines = [*ring_lines, *plaza_ring_lines]

    # Slice B: functional road hierarchy on the FINAL geometry (post clip +
    # snap + prune + spokes). Pure relabel/re-segmentation -- the geometry
    # union feeding the polygonization below is unchanged.
    ring_arc_lines: list[sg.LineString] = []
    ring_arc_edges: list[MacroEdge] = []
    n_tier_changed = 0
    n_disconnected_pairs = 0
    if functional_tiers:
        (
            final_lines,
            final_edges,
            ring_arc_lines,
            ring_arc_edges,
            n_tier_changed,
            n_disconnected_pairs,
        ) = assign_functional_tiers(final_lines, final_edges, final_ring_lines, nuclei)

    # Protect both the major cores AND the plaza discs themselves: a
    # mass-scaled plaza (slice P) can poke past its core ring when the
    # anchor sits off-center in an irregular core, and the resulting
    # plaza-outside-core lens faces must stay their own (plaza-covered,
    # later park-forced) faces rather than sliver-merging into a
    # neighboring fabric block. For plazas fully inside their core (the
    # common case) this is a no-op: those faces were already core-covered.
    protect_polys = [n.polygon for n in nuclei if n.is_major] + plaza_polys
    block_lines = [*final_lines, *final_ring_lines]
    blocks = _polygonize_macro_blocks_protecting_nuclei(
        block_lines, boundary, min_block_area, protect_polys
    )
    return MacroBlockBundle(
        core_polys=core_polys,
        ring_lines=final_ring_lines,
        clipped_lines=final_lines,
        clipped_edges=final_edges,
        blocks=blocks,
        nuclei=nuclei,
        plaza_polys=plaza_polys,
        n_endpoints_snapped=n_endpoints_snapped,
        n_dangles_pruned=n_dangles_pruned,
        ring_arc_lines=ring_arc_lines,
        ring_arc_edges=ring_arc_edges,
        n_tier_changed=n_tier_changed,
        n_disconnected_nucleus_pairs=n_disconnected_pairs,
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
    # Peak-snap radius for hierarchical city/town seeding (S2); <= 0 disables.
    peak_snap_radius_units: float = DEFAULT_PEAK_SNAP_RADIUS_UNITS
    # Density-attracting cost field.
    cost_base: float = 1.0
    w_slope: float = 8.0
    w_river: float = 6.0
    w_density: float = DEFAULT_W_DENSITY
    cost_floor: float = DEFAULT_COST_FLOOR
    # Importance-typed network.
    beta_ratio: float = DEFAULT_BETA_RATIO
    simplify_tolerance: float = DEFAULT_SIMPLIFY_TOLERANCE
    # Arterial Chaikin smoothing (stage 3.4, post-route/pre-clip).
    smooth_iterations: int = DEFAULT_SMOOTH_ITERATIONS
    smooth_post_tol: float = DEFAULT_SMOOTH_POST_TOL
    # Macro-blocks + dense-core ring-roads (stage 3.5b).
    min_block_area: float = DEFAULT_MIN_BLOCK_AREA
    core_frac: float = DEFAULT_CORE_FRAC
    core_max_radius_units: float = DEFAULT_CORE_MAX_RADIUS_UNITS
    core_min_area_units2: float = DEFAULT_CORE_MIN_AREA_UNITS2
    # Ring regularization (slice R): rank-scaled morphological opening
    # (majors only; minors default to ``0`` = today's cheap contour) +
    # optional low-pass Fourier refit (off by default).
    ring_open_width_major: float = DEFAULT_RING_OPEN_WIDTH_MAJOR_UNITS
    ring_open_width_minor: float = DEFAULT_RING_OPEN_WIDTH_MINOR_UNITS
    ring_fourier_refit: bool = False
    ring_fourier_harmonics: int = DEFAULT_RING_FOURIER_HARMONICS
    # Ranked settlement nuclei (S2): top-K by mass flagged is_major.
    n_major_nuclei: int = DEFAULT_N_MAJOR_NUCLEI
    # Intra-nucleus avenues (S3): spokes + a plaza ring, MAJOR nuclei only.
    # Plaza radius is mass-scaled per major between these bounds (slice P,
    # :func:`major_plaza_radii`).
    r_plaza_major_min: float = DEFAULT_R_PLAZA_MAJOR_MIN_UNITS
    r_plaza_major_max: float = DEFAULT_R_PLAZA_MAJOR_MAX_UNITS
    spoke_min_angle_deg: float = DEFAULT_SPOKE_MIN_ANGLE_DEG
    plaza_ring_vertices: int = DEFAULT_PLAZA_RING_VERTICES
    # Same-corridor geometry cleanup (slice T-geo); <= 0 disables each pass.
    corridor_dedup_tol: float = CORRIDOR_DEDUP_TOL
    ring_snap_tol: float = RING_SNAP_TOL
    dangle_prune_len: float = DANGLE_PRUNE_LEN
    # Functional road hierarchy (slice B): path-coverage tier relabel +
    # stored betweenness on the final geometry; False keeps construction
    # tiers (the pre-B behavior).
    functional_tiers: bool = True

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
    raw_arterial_lines / raw_edges : the pre-clip arterial network, Chaikin-
        smoothed (:func:`smooth_arterial_lines`) immediately after routing,
        then same-corridor DEDUPED (:func:`dedup_corridor_lines`, T-geo) --
        deduping pre-clip keeps this the single source of truth every
        downstream consumer derives from.
    core_polys / ring_lines : dense-core downtown polygons and their rings
        (``ring_lines`` also carries one plaza ring per major nucleus that
        got avenues, S3).
    arterial_lines / edges : the CLIPPED arterials (core interiors removed;
        identical to the raw ones when no cores are detected) PLUS the S3
        intra-nucleus spoke lines/records, UNCLIPPED; after the slice-B
        relabel (``params.functional_tiers``, >= 2 nuclei) re-segmented at
        graph junctions with functional ``tier`` / ``build_tier`` /
        ``betweenness``. These are what the stage scripts draw and export.
    blocks : polygonized macro-blocks (clipped arterials + spokes + rings +
        boundary).
    nuclei : ranked :class:`NucleusSpec` list, one per surviving core (S2).
    plaza_polys : one small inner plaza disc per major nucleus that got
        avenues (S3); see :attr:`MacroBlockBundle.plaza_polys`.
    ring_arc_lines / ring_arc_edges : junction-to-junction ring arcs with
        functional tiers + betweenness (slice B; ``ring_lines`` stays whole
        rings) -- S7-slim's per-arc ring width input.
    n_corridors_merged / n_endpoints_snapped / n_dangles_pruned :
        informational T-geo cleanup counters (how many routed lines had a
        near-coincident run absorbed pre-clip; how many endpoints snapped
        onto a core ring; how many short degree-1 fragments were pruned).
    n_tier_changed / n_disconnected_nucleus_pairs : slice-B counters (how
        many arterial/spoke records changed tier under the functional
        relabel; how many nucleus pairs had no connecting path).
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
    nuclei: list[NucleusSpec]
    plaza_polys: list[sg.Polygon]
    n_corridors_merged: int = 0
    n_endpoints_snapped: int = 0
    n_dangles_pruned: int = 0
    ring_arc_lines: list[sg.LineString] = field(default_factory=list)
    ring_arc_edges: list[MacroEdge] = field(default_factory=list)
    n_tier_changed: int = 0
    n_disconnected_nucleus_pairs: int = 0


def build_macro_layer(
    fields: IslandFields,
    boundary: sg.Polygon,
    points: pl.DataFrame,
    params: MacroParams | None = None,
) -> MacroLayer:
    """Assemble the full stage-1 macro layer from the staged inputs.

    THE single shared nodes -> cost-field -> arterials -> smoothing ->
    blocks-with-cores pipeline behind ``run_r1_macro``, ``run_r1_seam_spike``
    and ``run_r1_hybrid`` (previously each script hand-assembled its own
    copy). Deterministic: depends only on the inputs and ``params``.

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
            peak_snap_radius_units=params.peak_snap_radius_units,
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

    routed_lines, routed_edges = compute_macro_arterials(
        nodes,
        cost,
        x0,
        y0,
        cell,
        beta_ratio=params.beta_ratio,
        simplify_tolerance=params.simplify_tolerance,
    )

    # Chaikin-smooth the routed arterials HERE -- immediately after routing,
    # BEFORE core clipping / polygonization -- so macro-blocks, per-block Chen,
    # gate snapping, lots and both exports all derive from (and stay
    # consistent with) the SAME smoothed lines. Smoothing bake-side instead
    # would desync roads from blocks and put buildings back on the roads.
    smoothed_lines = smooth_arterial_lines(
        routed_lines,
        mask,
        x0,
        y0,
        cell,
        iterations=params.smooth_iterations,
        post_tol=params.smooth_post_tol,
    )
    smoothed_edges = [
        MacroEdge(
            node_a=rec.node_a,
            node_b=rec.node_b,
            tau=rec.tau,
            tier=rec.tier,
            path_cost=rec.path_cost,
            length=round(float(line.length), 3),
        )
        for line, rec in zip(smoothed_lines, routed_edges, strict=True)
    ]

    # Same-corridor dedup (T-geo) -- PRE-clip, so the deduped network is the
    # single pre-clip source of truth (raw_arterial_lines) AND so
    # `clip_arterials_to_cores` / `_ring_junction_stations`' exact-on-ring
    # matching runs on the deduped geometry.
    raw_lines, raw_edges, n_corridors_merged = dedup_corridor_lines(
        smoothed_lines,
        smoothed_edges,
        tol=params.corridor_dedup_tol,
    )

    bundle = build_macro_blocks_with_cores(
        density,
        nodes,
        raw_lines,
        raw_edges,
        boundary,
        points,
        x0,
        y0,
        cell,
        core_frac=params.core_frac,
        core_max_radius_units=params.core_max_radius_units,
        core_min_area_units2=params.core_min_area_units2,
        ring_open_width_major=params.ring_open_width_major,
        ring_open_width_minor=params.ring_open_width_minor,
        ring_fourier_refit=params.ring_fourier_refit,
        ring_fourier_harmonics=params.ring_fourier_harmonics,
        min_block_area=params.min_block_area,
        n_major_nuclei=params.n_major_nuclei,
        r_plaza_major_min=params.r_plaza_major_min,
        r_plaza_major_max=params.r_plaza_major_max,
        spoke_min_angle_deg=params.spoke_min_angle_deg,
        plaza_ring_vertices=params.plaza_ring_vertices,
        ring_snap_tol=params.ring_snap_tol,
        dangle_prune_len=params.dangle_prune_len,
        functional_tiers=params.functional_tiers,
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
        nuclei=bundle.nuclei,
        plaza_polys=bundle.plaza_polys,
        n_corridors_merged=n_corridors_merged,
        n_endpoints_snapped=bundle.n_endpoints_snapped,
        n_dangles_pruned=bundle.n_dangles_pruned,
        ring_arc_lines=bundle.ring_arc_lines,
        ring_arc_edges=bundle.ring_arc_edges,
        n_tier_changed=bundle.n_tier_changed,
        n_disconnected_nucleus_pairs=bundle.n_disconnected_nucleus_pairs,
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
                    "build_tier": rec.build_tier,
                    "betweenness": rec.betweenness,
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


def nucleus_specs_to_geojson(specs: list[NucleusSpec]) -> dict[str, Any]:
    """Serialize nuclei as anchor Point features (rank/mass/label/is_major).

    ``nucleus_id`` is the 0-based position in ``specs`` (== ``rank - 1``,
    since :func:`build_nucleus_specs` always returns nuclei in ascending-rank
    order) -- the same id :func:`assign_nearest_nucleus` returns and every
    district's ``nucleus_id`` refers to. The core polygon itself is already
    exported via :func:`core_rings_to_geojson`.
    """
    features = []
    for i, spec in enumerate(specs):
        props: dict[str, Any] = {
            "nucleus_id": i,
            "rank": spec.rank,
            "mass": round(spec.mass, 4),
            "is_major": spec.is_major,
            "influence_radius": round(spec.influence_radius, 4),
        }
        if spec.label is not None:
            props["label"] = spec.label
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [spec.anchor[0], spec.anchor[1]],
                },
                "properties": props,
            }
        )
    return {"type": "FeatureCollection", "features": features}
