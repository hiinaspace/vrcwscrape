"""Build a static land polygon from exported map points.

The web map can render one precomputed filled polygon much more cheaply than a
full-dataset scatter layer. The shape is a Delaunay alpha shape: nearby worlds
merge into continuous land, and sparse groups become islands.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import polars as pl
import shapely
from scipy.spatial import KDTree
from shapely import (
    BufferJoinStyle,
    GeometryCollection,
    MultiLineString,
    MultiPolygon,
    Polygon,
)
from shapely.ops import polygonize


def _median_nn(xy: np.ndarray, sample: int = 8000) -> float:
    idx = np.random.default_rng(0).choice(
        len(xy), size=min(sample, len(xy)), replace=False
    )
    dist, _ = KDTree(xy).query(xy[idx], k=2)
    return float(np.median(dist[:, 1]))


def _iter_polygons(geom):
    if isinstance(geom, Polygon):
        yield geom
    elif isinstance(geom, MultiPolygon):
        yield from geom.geoms
    elif isinstance(geom, GeometryCollection):
        for g in geom.geoms:
            yield from _iter_polygons(g)


def _filter_small_islands(geom, min_area: float):
    if min_area <= 0:
        return geom
    parts = [p for p in _iter_polygons(geom) if p.area >= min_area]
    if not parts:
        return geom
    return parts[0] if len(parts) == 1 else MultiPolygon(parts)


def _fill_polygon_holes(geom):
    parts = [Polygon(p.exterior) for p in _iter_polygons(geom)]
    if not parts:
        return geom
    return parts[0] if len(parts) == 1 else MultiPolygon(parts)


def _edge_key(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a < b else (b, a)


def build_land_geometry(
    xy: np.ndarray,
    *,
    edge_quantile: float = 0.99,
    max_edge: float | None = None,
    simplify_scale: float = 0.35,
    min_area_scale: float = 0.0,
    fill_holes: bool = True,
    expand_scale: float = 0.25,
    expand_quad_segs: int = 2,
):
    """Return `(geometry, info)` for the landmass covering `xy` points."""
    if len(xy) < 3:
        raise ValueError("need at least 3 points to build a land polygon")
    from scipy.spatial import Delaunay

    tri = Delaunay(xy).simplices
    a = tri[:, 0]
    b = tri[:, 1]
    c = tri[:, 2]
    e01 = np.linalg.norm(xy[a] - xy[b], axis=1)
    e12 = np.linalg.norm(xy[b] - xy[c], axis=1)
    e20 = np.linalg.norm(xy[c] - xy[a], axis=1)
    all_edges = np.concatenate([e01, e12, e20])
    threshold = float(
        max_edge if max_edge is not None else np.quantile(all_edges, edge_quantile)
    )
    keep = np.maximum.reduce([e01, e12, e20]) <= threshold

    edge_counts: dict[tuple[int, int], int] = {}
    for i, j, k in tri[keep]:
        for u, v in (
            _edge_key(int(i), int(j)),
            _edge_key(int(j), int(k)),
            _edge_key(int(k), int(i)),
        ):
            edge_counts[(u, v)] = edge_counts.get((u, v), 0) + 1
    boundary = [edge for edge, n in edge_counts.items() if n == 1]
    if not boundary:
        raise ValueError("alpha shape produced no boundary edges")

    lines = MultiLineString([(xy[i].tolist(), xy[j].tolist()) for i, j in boundary])
    polys = list(polygonize(lines))
    if not polys:
        raise ValueError("alpha shape boundary could not be polygonized")
    geom = polys[0] if len(polys) == 1 else MultiPolygon(polys)
    if fill_holes:
        geom = _fill_polygon_holes(geom)
    expand_r = threshold * expand_scale
    if expand_r > 0:
        geom = geom.buffer(
            expand_r,
            quad_segs=expand_quad_segs,
            join_style=BufferJoinStyle.round,
        )
    geom = geom.simplify(threshold * simplify_scale, preserve_topology=True)
    geom = _filter_small_islands(geom, (threshold * threshold) * min_area_scale)
    parts = list(_iter_polygons(geom))
    return geom, {
        "point_count": int(len(xy)),
        "triangle_count": int(len(tri)),
        "kept_triangle_count": int(keep.sum()),
        "boundary_edge_count": int(len(boundary)),
        "polygon_count": int(len(parts)),
        "edge_quantile": float(edge_quantile),
        "max_edge": threshold,
        "median_nn": _median_nn(xy),
        "simplify_scale": float(simplify_scale),
        "min_area_scale": float(min_area_scale),
        "fill_holes": bool(fill_holes),
        "expand_radius": float(expand_r),
        "expand_scale": float(expand_scale),
        "expand_quad_segs": int(expand_quad_segs),
    }


def write_land_geojson(
    xy: np.ndarray,
    out_path: Path,
    *,
    edge_quantile: float = 0.99,
    max_edge: float | None = None,
    simplify_scale: float = 0.35,
    min_area_scale: float = 0.0,
    fill_holes: bool = True,
    expand_scale: float = 0.25,
    expand_quad_segs: int = 2,
) -> None:
    geom, info = build_land_geometry(
        xy,
        edge_quantile=edge_quantile,
        max_edge=max_edge,
        simplify_scale=simplify_scale,
        min_area_scale=min_area_scale,
        fill_holes=fill_holes,
        expand_scale=expand_scale,
        expand_quad_segs=expand_quad_segs,
    )
    feature = {
        "type": "Feature",
        "properties": info,
        "geometry": json.loads(shapely.to_geojson(geom)),
    }
    out_path.write_text(
        json.dumps({"type": "FeatureCollection", "features": [feature]})
    )
    print(
        f"  wrote {out_path} "
        f"({info['point_count']:,} points, max_edge={info['max_edge']:.5f}, "
        f"{info['polygon_count']:,} polygons)"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--points", type=Path, required=True, help="app_points.parquet")
    ap.add_argument("--out", type=Path, required=True, help="destination land.geojson")
    ap.add_argument("--edge-quantile", type=float, default=0.99)
    ap.add_argument("--max-edge", type=float, default=None)
    ap.add_argument("--simplify-scale", type=float, default=0.35)
    ap.add_argument("--keep-holes", action="store_true")
    ap.add_argument(
        "--expand-scale",
        type=float,
        default=0.25,
        help="buffer final alpha-shape polygons by this fraction of max-edge",
    )
    ap.add_argument("--expand-quad-segs", type=int, default=2)
    ap.add_argument(
        "--min-area-scale",
        type=float,
        default=0.0,
        help="drop islands smaller than this * max_edge^2; 0 keeps all islands",
    )
    args = ap.parse_args()

    xy = (
        pl.read_parquet(args.points)
        .select("x", "y")
        .drop_nulls()
        .to_numpy()
        .astype("float64")
    )
    write_land_geojson(
        xy,
        args.out,
        edge_quantile=args.edge_quantile,
        max_edge=args.max_edge,
        simplify_scale=args.simplify_scale,
        min_area_scale=args.min_area_scale,
        fill_holes=not args.keep_holes,
        expand_scale=args.expand_scale,
        expand_quad_segs=args.expand_quad_segs,
    )


if __name__ == "__main__":
    main()
