"""Approximate point-cloud polygons with a raster mask.

This is intentionally softer than exact geometry. For map backgrounds, we want a
stable, padded land/region shape more than a mathematically exact alpha shape.
Runtime is mostly bounded by grid size instead of by point count.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import shapely
from scipy import ndimage
from shapely import GeometryCollection, MultiLineString, MultiPolygon, Polygon
from shapely.ops import polygonize


def iter_polygons(geom):
    if isinstance(geom, Polygon):
        yield geom
    elif isinstance(geom, MultiPolygon):
        yield from geom.geoms
    elif isinstance(geom, GeometryCollection):
        for g in geom.geoms:
            yield from iter_polygons(g)


def filter_small_polygons(geom, min_area: float):
    if min_area <= 0:
        return geom
    parts = [p for p in iter_polygons(geom) if p.area >= min_area]
    if not parts:
        return geom
    return parts[0] if len(parts) == 1 else MultiPolygon(parts)


def estimate_cell_size(
    xy: np.ndarray,
    *,
    max_dim: int = 2048,
    median_nn: float | None = None,
    nn_cells: float = 2.0,
) -> float:
    """Choose a raster cell size from map extent, capped by a median-NN floor."""
    if len(xy) == 0:
        raise ValueError("need at least one point")
    span = np.ptp(xy, axis=0)
    extent_cell = float(max(span.max(), 1e-6) / max(max_dim, 1))
    if median_nn is None or median_nn <= 0 or nn_cells <= 0:
        return extent_cell
    return max(extent_cell, float(median_nn) / float(nn_cells))


def _disk(radius: int) -> np.ndarray:
    if radius <= 0:
        return np.ones((1, 1), dtype=bool)
    yy, xx = np.ogrid[-radius : radius + 1, -radius : radius + 1]
    return (xx * xx + yy * yy) <= radius * radius


def _mask_bounds(
    xy: np.ndarray,
    *,
    cell_size: float,
    pad_cells: int,
) -> tuple[float, float, int, int]:
    mn = xy.min(axis=0)
    mx = xy.max(axis=0)
    x0 = math.floor(float(mn[0]) / cell_size) * cell_size - pad_cells * cell_size
    y0 = math.floor(float(mn[1]) / cell_size) * cell_size - pad_cells * cell_size
    x1 = math.ceil(float(mx[0]) / cell_size) * cell_size + pad_cells * cell_size
    y1 = math.ceil(float(mx[1]) / cell_size) * cell_size + pad_cells * cell_size
    nx = max(1, int(math.ceil((x1 - x0) / cell_size)))
    ny = max(1, int(math.ceil((y1 - y0) / cell_size)))
    return x0, y0, nx, ny


def _points_to_mask(
    xy: np.ndarray,
    *,
    cell_size: float,
    pad_cells: int,
) -> tuple[np.ndarray, float, float]:
    x0, y0, nx, ny = _mask_bounds(xy, cell_size=cell_size, pad_cells=pad_cells)
    cols = np.floor((xy[:, 0] - x0) / cell_size).astype(np.int64)
    rows = np.floor((xy[:, 1] - y0) / cell_size).astype(np.int64)
    cols = np.clip(cols, 0, nx - 1)
    rows = np.clip(rows, 0, ny - 1)
    mask = np.zeros((ny, nx), dtype=bool)
    mask[rows, cols] = True
    return mask, x0, y0


def _boundary_lines(mask: np.ndarray, x0: float, y0: float, cell_size: float):
    xs = x0 + np.arange(mask.shape[1] + 1, dtype=np.float64) * cell_size
    ys = y0 + np.arange(mask.shape[0] + 1, dtype=np.float64) * cell_size
    false_col = np.zeros((mask.shape[0], 1), dtype=bool)
    false_row = np.zeros((1, mask.shape[1]), dtype=bool)
    top = mask & ~np.vstack([false_row, mask[:-1, :]])
    bottom = mask & ~np.vstack([mask[1:, :], false_row])
    left = mask & ~np.hstack([false_col, mask[:, :-1]])
    right = mask & ~np.hstack([mask[:, 1:], false_col])

    lines: list[tuple[tuple[float, float], tuple[float, float]]] = []
    for rows, cols, kind in (
        (*np.nonzero(top), "top"),
        (*np.nonzero(right), "right"),
        (*np.nonzero(bottom), "bottom"),
        (*np.nonzero(left), "left"),
    ):
        for r, c in zip(rows.tolist(), cols.tolist(), strict=True):
            x = xs[c]
            y = ys[r]
            x1 = xs[c + 1]
            y1 = ys[r + 1]
            if kind == "top":
                lines.append(((x, y), (x1, y)))
            elif kind == "right":
                lines.append(((x1, y), (x1, y1)))
            elif kind == "bottom":
                lines.append(((x1, y1), (x, y1)))
            else:
                lines.append(((x, y1), (x, y)))
    return lines


def mask_to_geometry(
    mask: np.ndarray,
    *,
    x0: float,
    y0: float,
    cell_size: float,
    simplify_cells: float = 0.75,
    smooth_cells: float = 0.0,
    min_area_cells: float = 4.0,
):
    lines = _boundary_lines(mask, x0, y0, cell_size)
    if not lines:
        return GeometryCollection()
    polys = list(polygonize(shapely.node(MultiLineString(lines))))
    if not polys:
        return GeometryCollection()
    geom = polys[0] if len(polys) == 1 else MultiPolygon(polys)
    if smooth_cells > 0:
        r = cell_size * smooth_cells
        geom = geom.buffer(r, quad_segs=2).buffer(-r, quad_segs=2)
    if simplify_cells > 0:
        geom = geom.simplify(cell_size * simplify_cells, preserve_topology=True)
    return filter_small_polygons(geom, (cell_size * cell_size) * min_area_cells)


def build_raster_geometry(
    xy: np.ndarray,
    *,
    cell_size: float | None = None,
    max_dim: int = 2048,
    median_nn: float | None = None,
    nn_cells: float = 2.0,
    radius: float | None = None,
    dilate_cells: int = 5,
    close_cells: int = 2,
    fill_holes: bool = True,
    simplify_cells: float = 0.75,
    smooth_cells: float = 0.0,
    min_area_cells: float = 4.0,
):
    """Return `(geometry, info)` for a padded raster polygon over `xy`."""
    xy = np.asarray(xy, dtype=np.float64)
    if len(xy) == 0:
        raise ValueError("need at least one point")
    if cell_size is None:
        cell_size = estimate_cell_size(
            xy, max_dim=max_dim, median_nn=median_nn, nn_cells=nn_cells
        )
    if radius is not None:
        dilate_cells = max(1, int(math.ceil(radius / cell_size)))
    pad_cells = max(2, int(dilate_cells + close_cells + smooth_cells + 3))
    mask, x0, y0 = _points_to_mask(xy, cell_size=cell_size, pad_cells=pad_cells)
    occupied_cells = int(mask.sum())
    if dilate_cells > 0:
        mask = ndimage.binary_dilation(mask, structure=_disk(dilate_cells))
    if close_cells > 0:
        mask = ndimage.binary_closing(mask, structure=_disk(close_cells))
    if fill_holes:
        mask = ndimage.binary_fill_holes(mask)

    geom = mask_to_geometry(
        mask,
        x0=x0,
        y0=y0,
        cell_size=cell_size,
        simplify_cells=simplify_cells,
        smooth_cells=smooth_cells,
        min_area_cells=min_area_cells,
    )
    parts = list(iter_polygons(geom))
    info: dict[str, Any] = {
        "method": "raster",
        "point_count": int(len(xy)),
        "cell_size": float(cell_size),
        "grid_width": int(mask.shape[1]),
        "grid_height": int(mask.shape[0]),
        "occupied_cells": occupied_cells,
        "filled_cells": int(mask.sum()),
        "dilate_cells": int(dilate_cells),
        "close_cells": int(close_cells),
        "fill_holes": bool(fill_holes),
        "simplify_cells": float(simplify_cells),
        "smooth_cells": float(smooth_cells),
        "min_area_cells": float(min_area_cells),
        "polygon_count": int(len(parts)),
    }
    return geom, info


def raster_region_polys(
    xy: np.ndarray,
    cluster_id: np.ndarray,
    cids: list[int],
    *,
    cell_size: float,
    radius: float,
    close_cells: int = 1,
    fill_holes: bool = True,
    simplify_cells: float = 0.75,
    smooth_cells: float = 0.0,
    min_area_cells: float = 4.0,
) -> dict[int, Polygon | MultiPolygon]:
    polys: dict[int, Polygon | MultiPolygon] = {}
    for c in cids:
        pts = xy[cluster_id == c]
        if len(pts) < 3:
            continue
        geom, _ = build_raster_geometry(
            pts,
            cell_size=cell_size,
            radius=radius,
            close_cells=close_cells,
            fill_holes=fill_holes,
            simplify_cells=simplify_cells,
            smooth_cells=smooth_cells,
            min_area_cells=min_area_cells,
        )
        if not geom.is_empty:
            polys[c] = geom
    return polys
