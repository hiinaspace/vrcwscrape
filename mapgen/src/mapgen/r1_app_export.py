"""Track W (docs/greybox-plan.md): export the G0 hybrid-island greybox as a
``web/public/*-city*``-schema dataset dir, so the existing deck.gl app's
``isCity`` view (``Map.jsx``, driven by ``?data=``) renders it with zero web
code changes.

Pure logic only -- the island-frame -> app/DR-frame affine inverse (the exact
inverse of ``mapgen.r1_inputs.island_transform`` / ``to_island_frame`` /
``polygon_to_island_frame``), oriented building-rect fitting, tier -> kind/
width mapping, and the small GeoJSON feature/FeatureCollection builders. All
file IO (reading the G0 export + the source DR dataset dir, joining on
``world_id``, writing the output dataset dir) lives in
``scripts/run_r1_app_export.py``.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import shapely
import shapely.affinity
import shapely.geometry as sg

_LEVEL_SID_RE = re.compile(r"^l(\d+)_sid$")

# ---------------------------------------------------------------------------
# Island frame -> app/DR frame affine.
#
# Forward (mapgen.r1_inputs.to_island_frame / polygon_to_island_frame):
#   island = (app - offset) * scale
# Inverse (this module):
#   app = island / scale + offset
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IslandAffine:
    """``(offset_x, offset_y, scale)`` copied from a G0 ``greybox_manifest.json``'s
    ``island_frame`` (itself copied from ``inputs_manifest.json``, produced by
    ``mapgen.r1_inputs.island_transform``)."""

    offset_x: float
    offset_y: float
    scale: float


def invert_xy(
    xs: np.ndarray, ys: np.ndarray, affine: IslandAffine
) -> tuple[np.ndarray, np.ndarray]:
    """Island frame -> app/DR frame: exact inverse of ``to_island_frame``."""
    xs = np.asarray(xs, dtype=np.float64)
    ys = np.asarray(ys, dtype=np.float64)
    return xs / affine.scale + affine.offset_x, ys / affine.scale + affine.offset_y


def invert_geom(geom: Any, affine: IslandAffine) -> Any:
    """Island frame -> app/DR frame for any shapely geometry: exact inverse of
    ``polygon_to_island_frame``."""
    inv_scale = 1.0 / affine.scale
    return shapely.affinity.affine_transform(
        geom, [inv_scale, 0, 0, inv_scale, affine.offset_x, affine.offset_y]
    )


def island_length_to_app(length: float, affine: IslandAffine) -> float:
    """Scale a length (e.g. a plan-doc road-width constant) from island units
    to app units. Lengths don't see the ``offset_*`` translation, only the
    reciprocal of ``scale``."""
    return length / affine.scale


def max_affine_roundtrip_error(
    island_x: np.ndarray,
    island_y: np.ndarray,
    orig_x: np.ndarray,
    orig_y: np.ndarray,
    affine: IslandAffine,
) -> float:
    """Max abs error (app units) between inverse-affining ``(island_x,
    island_y)`` and ground-truth ``(orig_x, orig_y)`` -- the track-W
    acceptance check (they were produced by the forward transform from the
    same source coordinates, so this should be ~0 up to float32 round-trip
    noise)."""
    ax, ay = invert_xy(island_x, island_y, affine)
    orig_x = np.asarray(orig_x, dtype=np.float64)
    orig_y = np.asarray(orig_y, dtype=np.float64)
    return float(np.max(np.abs(np.concatenate([ax - orig_x, ay - orig_y]))))


# ---------------------------------------------------------------------------
# Oriented building rect (min-rotated-rectangle fit, canonicalized)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrientedRect:
    cx: float
    cy: float
    width: float  # >= depth
    depth: float
    angle: float  # radians, in (-pi/2, pi/2]


def _fold_angle(angle: float) -> float:
    """Fold a line's angle into ``(-pi/2, pi/2]``. A rectangle's long axis is
    pi-periodic (angle and angle + pi describe the same orientation), so this
    also absorbs the winding-direction ambiguity of an edge vector."""
    a = angle % math.pi  # Python's ``%`` gives [0, pi) for a positive modulus
    if a > math.pi / 2:
        a -= math.pi
    return a


def oriented_rect(poly: sg.Polygon) -> OrientedRect:
    """Deterministic oriented bounding rect for a lot/building footprint.

    Built from ``poly.minimum_rotated_rectangle``, canonicalized so the
    result is independent of which corner that rectangle's ring happens to
    start at or which way it winds (shapely's MRR can hand back the same
    rectangle starting from different corners/directions depending on
    internal hull bookkeeping, which can shift under a mere reordering of the
    input polygon's vertices): ``width`` is always the longer side, ``depth``
    the shorter, and ``angle`` (the long axis's direction) is folded into
    ``(-pi/2, pi/2]``.
    """
    rect = poly.minimum_rotated_rectangle
    if rect.geom_type != "Polygon":
        # Degenerate footprint (collinear/duplicate points): no rectangle.
        c = poly.centroid
        return OrientedRect(float(c.x), float(c.y), 0.0, 0.0, 0.0)
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    if len(coords) < 4:
        c = poly.centroid
        return OrientedRect(float(c.x), float(c.y), 0.0, 0.0, 0.0)
    cx, cy = coords.mean(axis=0)
    edges = np.roll(coords, -1, axis=0) - coords
    lens = np.linalg.norm(edges, axis=1)
    i_long = int(np.argmax(lens))
    i_short = (i_long + 1) % 4
    width = float(lens[i_long])
    depth = float(lens[i_short])
    long_edge = edges[i_long]
    angle = _fold_angle(float(math.atan2(long_edge[1], long_edge[0])))
    return OrientedRect(float(cx), float(cy), width, depth, angle)


def footprint_ring_xy(poly: sg.Polygon) -> tuple[list[float], list[float]]:
    """Exterior-ring coordinates of a TRUE building footprint polygon (``lot ∩
    setback-slab``, see ``mapgen.r1_lots._oriented_footprint``), as a pair of
    parallel ``x``/``y`` lists with the closing duplicate vertex dropped --
    ready for a per-world ``footprint_x``/``footprint_y`` ``List[Float64]``
    parquet column pair.

    This is the 2D-app counterpart of ``oriented_rect``: ``oriented_rect``
    fits an OBB *around* the footprint (used for the legacy
    ``building_width/depth/angle/cx/cy`` rect columns, kept for older-dataset
    fallback and the ``isCity`` sniff in ``web/src/duckdb.js``/``Map.jsx``);
    this instead hands back the polygon ITSELF so the web can render/extrude
    the real shape (a wedge/triangle footprint no longer draws as an
    overhanging bounding rect that pokes into roads/neighbors).

    Degenerate footprints (empty polygon, or a footprint whose exterior ring
    doesn't survive to a proper ring -- ``oriented_rect``'s same "no
    rectangle" case) return ``([], [])``; the web falls back to the OBB rect
    for those rows.
    """
    if poly.is_empty or poly.area <= 0.0 or poly.exterior is None:
        return [], []
    coords = list(poly.exterior.coords)
    if len(coords) > 1 and coords[0] == coords[-1]:
        coords = coords[:-1]
    if len(coords) < 3:
        return [], []
    return [float(x) for x, _y in coords], [float(y) for _x, y in coords]


# ---------------------------------------------------------------------------
# Road tier -> app-schema kind + width. Widths are the plan-doc (Stage G1)
# defaults in ISLAND units; ``island_length_to_app`` converts at export time.
# ---------------------------------------------------------------------------

TIER_KIND: dict[str, str] = {
    "highway": "arterial",
    "major": "collector",
    "local": "local",
    "ring": "arterial",
}
CHEN_STREET_KIND: str = "minor"

TIER_WIDTH_ISLAND_UNITS: dict[str, float] = {
    "highway": 1.0,
    "major": 0.7,
    "local": 0.5,
    "ring": 0.6,
}
CHEN_STREET_WIDTH_ISLAND_UNITS: float = 0.25


def road_kind(tier: str) -> str:
    """Map a G0 ``arterials.geojson`` ``tier`` to the app-schema road
    ``kind``."""
    return TIER_KIND[tier]


def road_width_island_units(tier: str) -> float:
    """Plan-doc default road width (island units) for a G0 arterial
    ``tier``."""
    return TIER_WIDTH_ISLAND_UNITS[tier]


# ---------------------------------------------------------------------------
# Bbox helper (regions_l*.geojson cheap filter)
# ---------------------------------------------------------------------------


def bbox_intersects(
    a: tuple[float, float, float, float], b: tuple[float, float, float, float]
) -> bool:
    """Axis-aligned bbox intersection test, ``(minx, miny, maxx, maxy)``."""
    return a[0] <= b[2] and b[0] <= a[2] and a[1] <= b[3] and b[1] <= a[3]


def filter_regions_by_bbox(
    regions: Mapping[str, Any], island_bbox: tuple[float, float, float, float]
) -> dict[str, Any]:
    """Subset a source ``regions_l*.geojson`` FeatureCollection to features
    whose geometry bbox intersects ``island_bbox``. A cheap AABB filter (not
    an exact polygon clip) -- enough to drop the background region shapes the
    island never touches, at json-parse cost only."""
    kept = [
        f
        for f in regions.get("features", [])
        if bbox_intersects(sg.shape(f["geometry"]).bounds, island_bbox)
    ]
    return {"type": regions.get("type", "FeatureCollection"), "features": kept}


# ---------------------------------------------------------------------------
# GeoJSON feature / FeatureCollection builders.
#
# Every FeatureCollection is sorted by its stated id property, independent of
# construction order, so the export is byte-identical across runs regardless
# of any upstream iteration-order subtlety (matches the ``_feature_collection``
# convention in ``scripts/run_r1_hybrid.py``'s Stage-G0 export).
# ---------------------------------------------------------------------------


def _sorted_feature_collection(
    features: Sequence[Mapping[str, Any]], sort_key: str
) -> dict[str, Any]:
    ordered = sorted(features, key=lambda f: f["properties"][sort_key])
    return {"type": "FeatureCollection", "features": list(ordered)}


def land_feature_collection(boundary: sg.Polygon) -> dict[str, Any]:
    """Single-feature FeatureCollection: the app's ``land.geojson`` (the
    ``fitBounds`` source) -- the inverse-affined G0 island boundary."""
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": sg.mapping(boundary),
                "properties": {"source": "r1-greybox-island"},
            }
        ],
    }


def road_feature(
    road_id: int, geom: Any, kind: str, width: float, weight: float
) -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": sg.mapping(geom),
        "properties": {
            "road_id": road_id,
            "id": road_id,
            "kind": kind,
            "width": width,
            "weight": weight,
        },
    }


def roads_feature_collection(features: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _sorted_feature_collection(features, "road_id")


def parcel_feature(
    lot_id: int, block_id: int, world_id: str, kind: str, geom: Any
) -> dict[str, Any]:
    """One ``parcels.geojson`` feature -- ``kind`` is ``"lot"`` (a world sits
    here) or ``"greenspace"`` (surplus subdivision piece, no world/building --
    docs/lots-wave-plan.md slice L2). Superseded the pre-L2 ``contains_building``
    boolean property, which assumed every parcel had a building; ``kind``
    covers that (and more) directly."""
    return {
        "type": "Feature",
        "geometry": sg.mapping(geom),
        "properties": {
            "lot_id": lot_id,
            "block_id": block_id,
            "world_id": world_id,
            "kind": kind,
        },
    }


def parcels_feature_collection(features: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _sorted_feature_collection(features, "lot_id")


def block_feature(
    block_id: int, area: float, assigned_worlds: int, geom: Any
) -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": sg.mapping(geom),
        "properties": {
            "block_id": block_id,
            "area": area,
            "assigned_worlds": assigned_worlds,
        },
    }


def blocks_feature_collection(features: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _sorted_feature_collection(features, "block_id")


def landuse_feature(landuse_id: int, geom: Any) -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": sg.mapping(geom),
        "properties": {"kind": "park", "landuse_id": landuse_id},
    }


def landuse_feature_collection(features: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _sorted_feature_collection(features, "landuse_id")


# ---------------------------------------------------------------------------
# manifest.json levels (mirrors ``web/src/duckdb.js``'s ``getLevels``)
# ---------------------------------------------------------------------------


def derive_levels(columns: Sequence[str]) -> tuple[list[int], int, int]:
    """Discover the ``l*_sid`` hierarchy depth present in an ``app_points``
    column list, the same way ``web/src/duckdb.js``'s ``getLevels`` does at
    runtime (regex ``^l(\\d+)_sid$``). Returns ``(levels, top, sub)``:
    ascending level numbers, the coarsest ("top") and next-coarsest ("sub").
    """
    levels = sorted(
        int(m.group(1)) for c in columns if (m := _LEVEL_SID_RE.match(c)) is not None
    )
    if len(levels) < 2:
        raise ValueError(
            f"expected at least 2 l*_sid columns, found {levels!r} in {columns!r}"
        )
    return levels, levels[-1], levels[-2]
