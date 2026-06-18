"""R1 Arm A — coarse recursive Chen on a real island (regional probe).

Reusable logic for the R1 regional probe Arm A (see
``docs/regional-2_5d-research.md``). This module does **not** touch any
``chen_*`` source; it only consumes their public extension points:

- ``generate_layout_for_boundary`` with ``parcel_count`` / ``split_weights`` /
  ``guidance`` / ``street_config``.
- ``RasterGuidanceField`` built from the offline terrain rasters.
- ``ChenSplitWeights`` regional lambda overrides.

It additionally emits the cross-arm interface files (``districts.geojson`` /
``arterials.geojson``) in the island frame so Arm A and Arm B share a schema.

The guidance field is built from ``height_carved``: the 4-RoSy preferred street
angle is the terrain gradient direction ``atan2(d/dy h, d/dx h)`` (under 4-RoSy
semantics gradient-aligned and contour-aligned coincide, so streets are nudged
to follow contours/ridges), and the weight is the normalized slope clipped to
``[0, 1]`` times a tunable strength scalar.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
from shapely import LineString, Point, Polygon
from shapely.strtree import STRtree

from mapgen.chen_core import ChenSplitWeights
from mapgen.chen_field import RasterGuidanceField


@dataclass(frozen=True)
class IslandFields:
    """Offline terrain/density rasters in the island frame.

    All rasters are ``(H, W)`` float arrays sharing the cell-center transform
    ``x = x0 + col * cell``, ``y = y0 + row * cell``.
    """

    density: np.ndarray
    height: np.ndarray
    flow_accum: np.ndarray
    height_carved: np.ndarray
    slope: np.ndarray
    x0: float
    y0: float
    cell: float

    @classmethod
    def from_npz(cls, path: str) -> IslandFields:
        z = np.load(path)
        return cls(
            density=np.asarray(z["density"], dtype=float),
            height=np.asarray(z["height"], dtype=float),
            flow_accum=np.asarray(z["flow_accum"], dtype=float),
            height_carved=np.asarray(z["height_carved"], dtype=float),
            slope=np.asarray(z["slope"], dtype=float),
            x0=float(z["x0"]),
            y0=float(z["y0"]),
            cell=float(z["cell"]),
        )


def _normalize01(array: np.ndarray) -> np.ndarray:
    finite = np.asarray(array, dtype=float)
    lo = float(np.nanmin(finite))
    hi = float(np.nanmax(finite))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi - lo <= 1e-12:
        return np.zeros_like(finite)
    return np.clip((finite - lo) / (hi - lo), 0.0, 1.0)


def build_terrain_guidance(
    fields: IslandFields,
    *,
    strength: float = 3.0,
    density_ridge_boost: float = 0.0,
) -> RasterGuidanceField:
    """Build a contour-following ``RasterGuidanceField`` from the terrain.

    The 4-RoSy preferred angle is the gradient direction of the carved height
    field; the weight is the normalized slope clipped to ``[0, 1]`` scaled by
    ``strength`` (the guidance is internally capped at 4.0, so values above that
    saturate). ``density_ridge_boost`` optionally adds weight where the density
    is high but its own gradient is low (ridge plateaus of the population
    surface), only if requested.
    """
    height = fields.height_carved
    # np.gradient returns (d/drow, d/dcol). Row increases with +y, col with +x,
    # so d/dy = grad_row / cell, d/dx = grad_col / cell. The constant cell
    # factor cancels in the angle.
    grad_y, grad_x = np.gradient(height)
    angle = np.arctan2(grad_y, grad_x).astype(float)

    slope_norm = _normalize01(fields.slope)
    weight = slope_norm * float(strength)

    if density_ridge_boost > 0.0:
        density_norm = _normalize01(fields.density)
        dg_y, dg_x = np.gradient(fields.density)
        density_grad_mag = np.hypot(dg_x, dg_y)
        density_grad_flat = 1.0 - _normalize01(density_grad_mag)
        ridge = density_norm * density_grad_flat
        weight = weight + ridge * float(density_ridge_boost)

    weight = np.clip(weight, 0.0, None)
    return RasterGuidanceField(
        angle=angle,
        weight=weight,
        x0=fields.x0,
        y0=fields.y0,
        cell=fields.cell,
    )


# Regional Eq. 2 lambda override: favor size balance + access over regularity.
REGIONAL_SPLIT_WEIGHTS = ChenSplitWeights(size=0.45, regularity=0.15, access=0.40)


@dataclass(frozen=True)
class WorldPoint:
    x: float
    y: float


def assign_worlds_to_parcels(
    parcel_polys: list[tuple[int, Polygon]],
    points: list[tuple[float, float]],
) -> dict[int, int]:
    """Count worlds inside each parcel by point-in-polygon (STRtree indexed)."""
    counts: dict[int, int] = {pid: 0 for pid, _ in parcel_polys}
    if not parcel_polys or not points:
        return counts
    geoms = [poly for _pid, poly in parcel_polys]
    pids = [pid for pid, _poly in parcel_polys]
    tree = STRtree(geoms)
    for px, py in points:
        pt = Point(px, py)
        candidate_indices = tree.query(pt)
        for idx in candidate_indices:
            if geoms[int(idx)].contains(pt):
                counts[pids[int(idx)]] += 1
                break
    return counts


def districts_feature_collection(
    parcel_polys: list[tuple[int, Polygon]],
    world_counts: dict[int, int],
) -> dict[str, Any]:
    """Cross-arm ``districts.geojson`` schema (matches Arm B)."""
    features: list[dict[str, Any]] = []
    for pid, poly in parcel_polys:
        features.append(
            {
                "type": "Feature",
                "geometry": _polygon_geojson(poly),
                "properties": {
                    "district_id": int(pid),
                    "area": round(float(poly.area), 3),
                    "world_count": int(world_counts.get(pid, 0)),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def arterials_feature_collection(
    streets: list[tuple[int, LineString]],
) -> dict[str, Any]:
    """Cross-arm ``arterials.geojson`` schema: street-network LineStrings."""
    features: list[dict[str, Any]] = []
    for sid, line in streets:
        coords = [[float(x), float(y)] for x, y in line.coords]
        if len(coords) < 2:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {
                    "arterial_id": int(sid),
                    "length": round(float(line.length), 3),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _polygon_geojson(poly: Polygon) -> dict[str, Any]:
    rings = [[[float(x), float(y)] for x, y in poly.exterior.coords]]
    for interior in poly.interiors:
        rings.append([[float(x), float(y)] for x, y in interior.coords])
    return {"type": "Polygon", "coordinates": rings}
