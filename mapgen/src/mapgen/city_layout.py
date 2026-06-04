"""Generate a city-style static app export from an existing map export.

This is an experimental replacement for Voronoi/road-parcel views. It treats the
2D DR coordinates as demand points, creates density-gated developed districts,
leaves sparse gaps as open/park land, and places each world as a compact
road-fronting building footprint.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import networkx as nx
import numpy as np
import polars as pl
import shapely
from scipy.optimize import linear_sum_assignment
from scipy.spatial import cKDTree
from shapely import GeometryCollection, LineString, MultiPolygon, Polygon

from mapgen.land import build_land_geometry
from mapgen.raster_poly import estimate_cell_size, iter_polygons, raster_region_polys


@dataclass
class Road:
    road_id: int
    coords: list[tuple[float, float]]
    kind: str
    island_id: int
    world_count: int
    width: float


@dataclass
class Block:
    block_id: int
    island_id: int
    geom: Polygon
    target_lots: int
    assigned_worlds: int


@dataclass
class GroupInfo:
    island_id: int
    center: np.ndarray
    world_count: int
    endpoint: np.ndarray


class Ids:
    def __init__(self) -> None:
        self.road = 0
        self.block = 0
        self.lot = 0

    def road_id(self) -> int:
        v = self.road
        self.road += 1
        return v

    def block_id(self) -> int:
        v = self.block
        self.block += 1
        return v

    def lot_id(self) -> int:
        v = self.lot
        self.lot += 1
        return v


def _level_numbers(columns: list[str]) -> list[int]:
    out = []
    for col in columns:
        if col.startswith("l") and col.endswith("_sid"):
            with suppress(ValueError):
                out.append(int(col[1:-4]))
    return sorted(out)


def _median_nn(xy: np.ndarray, sample: int = 12000) -> float:
    if len(xy) < 2:
        return 0.0
    rng = np.random.default_rng(0)
    idx = rng.choice(len(xy), size=min(sample, len(xy)), replace=False)
    dist, _ = cKDTree(xy).query(xy[idx], k=2, workers=-1)
    return float(np.median(dist[:, 1]))


def _pca_axes(xy: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    center = xy.mean(axis=0)
    if len(xy) < 3:
        return center, np.eye(2)
    cov = np.cov((xy - center).T)
    vals, vecs = np.linalg.eigh(cov)
    order = np.argsort(vals)[::-1]
    axes = vecs[:, order]
    if np.linalg.det(axes) < 0:
        axes[:, 1] *= -1
    return center, axes


def _angle_of_axes(axes: np.ndarray) -> float:
    return float(math.atan2(axes[1, 0], axes[0, 0]))


def _to_world(local: np.ndarray, center: np.ndarray, axes: np.ndarray) -> np.ndarray:
    return center + local @ axes.T


def _rect_geom(
    local_center: np.ndarray,
    width: float,
    depth: float,
    center: np.ndarray,
    axes: np.ndarray,
) -> Polygon:
    w = width / 2
    d = depth / 2
    local = np.array(
        [
            [local_center[0] - w, local_center[1] - d],
            [local_center[0] + w, local_center[1] - d],
            [local_center[0] + w, local_center[1] + d],
            [local_center[0] - w, local_center[1] + d],
        ],
        dtype=np.float64,
    )
    return Polygon(_to_world(local, center, axes))


def _safe_geom(geom):
    if geom.is_empty:
        return geom
    if not geom.is_valid:
        geom = shapely.make_valid(geom)
    if not geom.is_valid:
        geom = geom.buffer(0)
    return geom


def _feature_collection(features: list[dict]) -> dict:
    return {"type": "FeatureCollection", "features": features}


def _geom_feature(geom, properties: dict) -> dict:
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": json.loads(shapely.to_geojson(geom)),
    }


def _write_geojson(features: list[dict], out_path: Path) -> None:
    out_path.write_text(json.dumps(_feature_collection(features)))
    print(f"  wrote {out_path} ({len(features):,} features)")


def _filter_geom(geom, min_area: float):
    parts = [p for p in iter_polygons(geom) if p.area >= min_area]
    if not parts:
        return GeometryCollection()
    return parts[0] if len(parts) == 1 else MultiPolygon(parts)


def _robust_extent(local: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(local) < 4:
        lo = local.min(axis=0)
        hi = local.max(axis=0)
    else:
        lo, hi = np.quantile(local, [0.03, 0.97], axis=0)
    span = np.maximum(hi - lo, 1e-9)
    return lo, hi + (span <= 1e-9) * 1e-9


def _assign_slots(
    local: np.ndarray, slots: np.ndarray, max_hungarian: int
) -> np.ndarray:
    n = len(local)
    if n <= max_hungarian:
        d2 = ((local[:, None, :] - slots[None, :, :]) ** 2).sum(axis=2)
        rows, cols = linear_sum_assignment(d2)
        out = np.empty(n, dtype=np.int64)
        out[rows] = cols
        return out

    k = min(64, len(slots))
    dist, nbr = cKDTree(slots).query(local, k=k, workers=-1)
    dist = np.asarray(dist)
    nbr = np.asarray(nbr)
    if dist.ndim == 1:
        dist = dist[:, None]
        nbr = nbr[:, None]
    point_idx = np.repeat(np.arange(n, dtype=np.int64), dist.shape[1])
    order = np.argsort(dist.ravel(), kind="stable")
    out = np.full(n, -1, dtype=np.int64)
    used = np.zeros(len(slots), dtype=bool)
    for flat in order.tolist():
        p = int(point_idx[flat])
        s = int(nbr.ravel()[flat])
        if out[p] == -1 and not used[s]:
            out[p] = s
            used[s] = True
    while (out == -1).any():
        remaining_points = np.flatnonzero(out == -1)
        remaining_slots = np.flatnonzero(~used)
        tree = cKDTree(slots[remaining_slots])
        d, j = tree.query(local[remaining_points], k=1, workers=-1)
        order = np.argsort(np.asarray(d), kind="stable")
        progressed = False
        for pos in order.tolist():
            p = int(remaining_points[pos])
            s = int(remaining_slots[int(np.asarray(j)[pos])])
            if out[p] == -1 and not used[s]:
                out[p] = s
                used[s] = True
                progressed = True
        if not progressed:
            break
    if (out == -1).any():
        free = np.flatnonzero(~used)
        for p, s in zip(np.flatnonzero(out == -1), free, strict=False):
            out[int(p)] = int(s)
    return out


def _line_project(
    point: np.ndarray, a: np.ndarray, b: np.ndarray
) -> tuple[np.ndarray, float]:
    v = b - a
    den = float(v @ v)
    if den <= 1e-12:
        return a.copy(), float(np.linalg.norm(point - a))
    t = float(np.clip(((point - a) @ v) / den, 0.0, 1.0))
    q = a + v * t
    return q, float(np.linalg.norm(point - q))


def _add_road(
    roads: list[Road],
    ids: Ids,
    *,
    coords: list[tuple[float, float]],
    kind: str,
    island_id: int,
    world_count: int,
    width: float,
) -> int:
    if len(coords) < 2:
        return -1
    line = LineString(coords)
    if line.is_empty or line.length <= 1e-9:
        return -1
    rid = ids.road_id()
    roads.append(
        Road(
            road_id=rid,
            coords=[(float(x), float(y)) for x, y in line.coords],
            kind=kind,
            island_id=island_id,
            world_count=world_count,
            width=width,
        )
    )
    return rid


def _road_features(roads: list[Road]) -> list[dict]:
    feats = []
    for road in roads:
        line = LineString(road.coords)
        feats.append(
            _geom_feature(
                line,
                {
                    "road_id": int(road.road_id),
                    "id": int(road.road_id),
                    "kind": road.kind,
                    "island_id": int(road.island_id),
                    "region": int(road.island_id),
                    "subregion": -1,
                    "world_count": int(road.world_count),
                    "width": float(road.width),
                    "weight": float(line.length),
                },
            )
        )
    return feats


def _block_features(blocks: list[Block]) -> list[dict]:
    return [
        _geom_feature(
            block.geom,
            {
                "block_id": int(block.block_id),
                "island_id": int(block.island_id),
                "target_lots": int(block.target_lots),
                "assigned_worlds": int(block.assigned_worlds),
                "area": float(block.geom.area),
            },
        )
        for block in blocks
        if not block.geom.is_empty and block.geom.area > 0
    ]


def _layout_sparse_group(
    *,
    idx: np.ndarray,
    xy: np.ndarray,
    island_id: int,
    global_nn: float,
    args,
    ids: Ids,
    roads: list[Road],
    blocks: list[Block],
    out: dict[str, np.ndarray],
) -> GroupInfo:
    center, axes = _pca_axes(xy)
    local = (xy - center) @ axes
    order = np.argsort(local[:, 0])
    angle = _angle_of_axes(axes)
    size = max(global_nn * args.sparse_building_scale, _median_nn(xy) * 0.55)
    size = float(
        np.clip(size, global_nn * 0.8, global_nn * args.sparse_building_max_scale)
    )

    if len(xy) == 1:
        a = xy[0] - axes[:, 0] * size
        b = xy[0] + axes[:, 0] * size
        rid = _add_road(
            roads,
            ids,
            coords=[tuple(a), tuple(b)],
            kind="service",
            island_id=island_id,
            world_count=1,
            width=args.service_road_width,
        )
        block_geom = PointBuffer(xy[0], size * 2.0)
    else:
        coords = [tuple(p) for p in xy[order]]
        rid = _add_road(
            roads,
            ids,
            coords=coords,
            kind="service",
            island_id=island_id,
            world_count=len(xy),
            width=args.service_road_width,
        )
        block_geom = _safe_geom(
            LineString(coords).buffer(size * 2.2, cap_style="round")
        )

    bid = ids.block_id()
    blocks.append(
        Block(
            block_id=bid,
            island_id=island_id,
            geom=block_geom,
            target_lots=len(idx),
            assigned_worlds=len(idx),
        )
    )
    for row, p in zip(idx.tolist(), xy, strict=True):
        out["x"][row] = p[0]
        out["y"][row] = p[1]
        out["building_angle"][row] = angle
        out["building_width"][row] = size
        out["building_depth"][row] = size
        out["building_height"][row] = args.default_building_height
        out["lot_id"][row] = ids.lot_id()
        out["block_id"][row] = bid
        out["road_id"][row] = rid
        out["frontage_x"][row] = p[0]
        out["frontage_y"][row] = p[1]
    return GroupInfo(
        island_id=island_id,
        center=xy.mean(axis=0),
        world_count=len(xy),
        endpoint=xy[order[len(order) // 2]],
    )


def PointBuffer(xy: np.ndarray, radius: float):
    return shapely.Point(float(xy[0]), float(xy[1])).buffer(radius, quad_segs=2)


def _layout_dense_group(
    *,
    idx: np.ndarray,
    xy: np.ndarray,
    island_id: int,
    global_nn: float,
    args,
    ids: Ids,
    roads: list[Road],
    blocks: list[Block],
    out: dict[str, np.ndarray],
) -> GroupInfo:
    n = len(xy)
    center, axes = _pca_axes(xy)
    angle = _angle_of_axes(axes)
    local = (xy - center) @ axes
    lo, hi = _robust_extent(local)
    extent = np.maximum(hi - lo, global_nn * 2.0)
    aspect = float(np.clip(extent[0] / max(extent[1], 1e-9), 0.35, 3.0))
    cols = max(1, int(math.ceil(math.sqrt(n * aspect))))
    rows = int(math.ceil(n / cols))
    local_nn = _median_nn(xy)
    base_cell = max(
        global_nn * args.dense_spacing_global_scale,
        local_nn * args.dense_spacing_nn_scale,
        math.sqrt(float(extent[0] * extent[1]) / max(n, 1))
        * args.dense_spacing_area_scale,
    )
    base_cell = float(
        np.clip(
            base_cell,
            global_nn * 0.75,
            global_nn * args.dense_spacing_max_scale,
        )
    )
    sx = max(base_cell, float(extent[0] / max(cols - 1, 1)))
    sy = max(base_cell, float(extent[1] / max(rows - 1, 1)))
    grid_center = (lo + hi) / 2
    x0 = grid_center[0] - ((cols - 1) * sx) / 2
    y0 = grid_center[1] - ((rows - 1) * sy) / 2
    slots = []
    slot_rc = []
    for r in range(rows):
        for c in range(cols):
            slots.append([x0 + c * sx, y0 + r * sy])
            slot_rc.append((r, c))
    slots_arr = np.asarray(slots, dtype=np.float64)
    slot_idx = _assign_slots(local, slots_arr, args.max_hungarian)
    placed_local = slots_arr[slot_idx]
    rc = [slot_rc[i] for i in slot_idx.tolist()]

    x_min = x0 - sx * 0.75
    x_max = x0 + (cols - 1) * sx + sx * 0.75
    y_min = y0 - sy * 0.75
    y_max = y0 + (rows - 1) * sy + sy * 0.75
    road_lines: list[tuple[int, str, float, np.ndarray, np.ndarray]] = []

    vertical_cols = {0, cols}
    vertical_cols.update(range(args.block_cols, cols, args.block_cols))
    for c in sorted(vertical_cols):
        lx = x0 - sx / 2 if c == 0 else x0 + (c - 0.5) * sx
        a = np.array([lx, y_min])
        b = np.array([lx, y_max])
        coords = _to_world(np.stack([a, b]), center, axes)
        rid = _add_road(
            roads,
            ids,
            coords=[tuple(coords[0]), tuple(coords[1])],
            kind="local",
            island_id=island_id,
            world_count=n,
            width=args.local_road_width,
        )
        road_lines.append((rid, "v", lx, a, b))

    horizontal_rows = {0, rows}
    horizontal_rows.update(range(args.block_rows, rows, args.block_rows))
    for r in sorted(horizontal_rows):
        ly = y0 - sy / 2 if r == 0 else y0 + (r - 0.5) * sy
        a = np.array([x_min, ly])
        b = np.array([x_max, ly])
        coords = _to_world(np.stack([a, b]), center, axes)
        rid = _add_road(
            roads,
            ids,
            coords=[tuple(coords[0]), tuple(coords[1])],
            kind="local",
            island_id=island_id,
            world_count=n,
            width=args.local_road_width,
        )
        road_lines.append((rid, "h", ly, a, b))

    width = sx * args.building_width_scale
    depth = sy * args.building_depth_scale
    block_by_chunk: dict[tuple[int, int], int] = {}
    chunk_slots: dict[tuple[int, int], list[np.ndarray]] = defaultdict(list)
    for pos, (r, c) in zip(placed_local, rc, strict=True):
        chunk_slots[(r // args.block_rows, c // args.block_cols)].append(pos)

    for chunk, positions in chunk_slots.items():
        arr = np.asarray(positions)
        mn = arr.min(axis=0) - np.array([sx * 0.55, sy * 0.55])
        mx = arr.max(axis=0) + np.array([sx * 0.55, sy * 0.55])
        geom = _safe_geom(
            _rect_geom(
                (mn + mx) / 2,
                float(mx[0] - mn[0]),
                float(mx[1] - mn[1]),
                center,
                axes,
            )
        )
        bid = ids.block_id()
        block_by_chunk[chunk] = bid
        blocks.append(
            Block(
                block_id=bid,
                island_id=island_id,
                geom=geom,
                target_lots=len(positions),
                assigned_worlds=len(positions),
            )
        )

    for row, local_pos, (r, c) in zip(idx.tolist(), placed_local, rc, strict=True):
        best = None
        for rid, _kind, _coord, a, b in road_lines:
            q, dist = _line_project(local_pos, a, b)
            if best is None or dist < best[0]:
                best = (dist, rid, q)
        assert best is not None
        frontage = _to_world(best[2][None, :], center, axes)[0]
        world_pos = _to_world(local_pos[None, :], center, axes)[0]
        out["x"][row] = world_pos[0]
        out["y"][row] = world_pos[1]
        out["building_angle"][row] = angle
        out["building_width"][row] = width
        out["building_depth"][row] = depth
        out["building_height"][row] = args.default_building_height
        out["lot_id"][row] = ids.lot_id()
        out["block_id"][row] = block_by_chunk[
            (r // args.block_rows, c // args.block_cols)
        ]
        out["road_id"][row] = best[1]
        out["frontage_x"][row] = frontage[0]
        out["frontage_y"][row] = frontage[1]

    center_world = _to_world(np.array([[0.0, 0.0]]), center, axes)[0]
    return GroupInfo(
        island_id=island_id,
        center=center_world,
        world_count=n,
        endpoint=center_world,
    )


def _connect_groups(
    groups: list[GroupInfo],
    *,
    ids: Ids,
    roads: list[Road],
    args,
) -> None:
    by_island: dict[int, list[GroupInfo]] = defaultdict(list)
    for g in groups:
        by_island[g.island_id].append(g)
    for island_id, arr in by_island.items():
        if len(arr) < 2:
            continue
        xy = np.stack([g.center for g in arr])
        graph = nx.Graph()
        for i in range(len(arr)):
            graph.add_node(i)
        tree = cKDTree(xy)
        k = min(len(arr), max(3, args.collector_knn + 1))
        dist, nbr = tree.query(xy, k=k)
        for i in range(len(arr)):
            for d, j in zip(
                np.atleast_1d(dist[i]), np.atleast_1d(nbr[i]), strict=False
            ):
                j = int(j)
                if i == j:
                    continue
                graph.add_edge(i, j, weight=float(d))
        mst = nx.minimum_spanning_tree(graph, weight="weight")
        for a, b in mst.edges():
            pa = arr[a].endpoint
            pb = arr[b].endpoint
            _add_road(
                roads,
                ids,
                coords=[tuple(pa), tuple(pb)],
                kind="collector",
                island_id=island_id,
                world_count=arr[a].world_count + arr[b].world_count,
                width=args.collector_road_width,
            )


def _write_landuse(
    *,
    land_geom,
    blocks: list[Block],
    out_path: Path,
    min_area: float,
    park_buffer: float,
) -> dict[str, float | int]:
    developed = shapely.unary_union([b.geom for b in blocks if not b.geom.is_empty])
    developed = _safe_geom(developed.intersection(land_geom))
    open_space = _safe_geom(land_geom.difference(developed.buffer(park_buffer)))
    developed = _filter_geom(developed, min_area)
    open_space = _filter_geom(open_space, min_area)
    feats = []
    for i, poly in enumerate(iter_polygons(open_space)):
        feats.append(_geom_feature(poly, {"kind": "park", "landuse_id": i}))
    offset = len(feats)
    for i, poly in enumerate(iter_polygons(developed)):
        feats.append(
            _geom_feature(poly, {"kind": "developed", "landuse_id": offset + i})
        )
    _write_geojson(feats, out_path)
    return {
        "open_space_area": float(open_space.area if not open_space.is_empty else 0.0),
        "developed_area": float(developed.area if not developed.is_empty else 0.0),
        "open_space_parts": int(len(list(iter_polygons(open_space)))),
        "developed_parts": int(len(list(iter_polygons(developed)))),
    }


def _write_regions(
    points: pl.DataFrame,
    *,
    levels: list[int],
    out_dir: Path,
    global_nn: float,
    args,
) -> None:
    xy = points.select("x", "y").to_numpy().astype(np.float64)
    cell = estimate_cell_size(
        xy,
        max_dim=args.region_raster_max_dim,
        median_nn=global_nn,
        nn_cells=args.region_raster_nn_cells,
    )
    for lvl in levels:
        id_col = f"l{lvl}_id"
        name_col = f"l{lvl}_name"
        if id_col not in points.columns:
            id_col = f"l{lvl}_sid"
            name_col = f"l{lvl}_sname"
        cluster_id = points[id_col].fill_null(-1).cast(pl.Int64).to_numpy()
        cids = sorted(int(c) for c in set(cluster_id.tolist()) if c != -1)
        polys = raster_region_polys(
            xy,
            cluster_id,
            cids,
            cell_size=cell,
            radius=global_nn * args.region_buffer_scale,
            close_cells=args.region_raster_close_cells,
            simplify_cells=args.region_raster_simplify_cells,
            smooth_cells=args.region_raster_smooth_cells,
            min_area_cells=args.region_raster_min_area_cells,
        )
        meta = (
            points.with_columns(pl.Series("_cid", cluster_id))
            .group_by("_cid")
            .agg(
                pl.col(name_col).drop_nulls().first().alias("label"),
                pl.col("region").mode().first().alias("region"),
                pl.len().alias("size"),
            )
        )
        info = {int(r["_cid"]): r for r in meta.iter_rows(named=True)}
        feats = []
        for cid, geom in polys.items():
            row = info.get(int(cid), {})
            feats.append(
                _geom_feature(
                    geom,
                    {
                        "cluster_id": int(cid),
                        "region": int(row.get("region") or cid),
                        "label": str(row.get("label") or f"Region {cid}"),
                        "size": int(row.get("size") or 0),
                    },
                )
            )
        _write_geojson(feats, out_dir / f"regions_l{lvl}.geojson")


def _city_layout(
    points: pl.DataFrame, *, top_col: str, sub_col: str, args
) -> tuple[pl.DataFrame, list[Road], list[Block], dict]:
    n = points.height
    xy = points.select("x", "y").to_numpy().astype(np.float64)
    global_nn = _median_nn(xy)
    print(f"  global median nn={global_nn:.5f}")
    out = {
        "x": np.full(n, np.nan, dtype=np.float64),
        "y": np.full(n, np.nan, dtype=np.float64),
        "building_angle": np.zeros(n, dtype=np.float32),
        "building_width": np.zeros(n, dtype=np.float32),
        "building_depth": np.zeros(n, dtype=np.float32),
        "building_height": np.zeros(n, dtype=np.float32),
        "lot_id": np.full(n, -1, dtype=np.int64),
        "block_id": np.full(n, -1, dtype=np.int64),
        "road_id": np.full(n, -1, dtype=np.int64),
        "frontage_x": np.full(n, np.nan, dtype=np.float64),
        "frontage_y": np.full(n, np.nan, dtype=np.float64),
    }
    roads: list[Road] = []
    blocks: list[Block] = []
    groups: list[GroupInfo] = []
    ids = Ids()

    selected_islands = (
        points.group_by(top_col)
        .len()
        .sort("len", descending=True)
        .head(args.max_islands)
    )
    island_set = {int(v) for v in selected_islands[top_col].to_list()}
    print(
        "  islands: "
        + ", ".join(
            f"{int(r[top_col])} ({int(r['len']):,})"
            for r in selected_islands.iter_rows(named=True)
        )
    )

    work = (
        points.with_row_index("_idx")
        .filter(pl.col(top_col).is_in(sorted(island_set)))
        .sort([top_col, sub_col])
    )
    for row in (
        work.group_by([top_col, sub_col])
        .len()
        .sort("len", descending=True)
        .iter_rows(named=True)
    ):
        island_id = int(row[top_col])
        sub_id = int(row[sub_col])
        group = work.filter(
            (pl.col(top_col) == island_id) & (pl.col(sub_col) == sub_id)
        )
        idx = group["_idx"].to_numpy()
        group_xy = group.select("x", "y").to_numpy().astype(np.float64)
        if len(group_xy) <= args.sparse_max_worlds:
            info = _layout_sparse_group(
                idx=idx,
                xy=group_xy,
                island_id=island_id,
                global_nn=global_nn,
                args=args,
                ids=ids,
                roads=roads,
                blocks=blocks,
                out=out,
            )
        else:
            info = _layout_dense_group(
                idx=idx,
                xy=group_xy,
                island_id=island_id,
                global_nn=global_nn,
                args=args,
                ids=ids,
                roads=roads,
                blocks=blocks,
                out=out,
            )
        groups.append(info)
        print(f"    subregion {sub_id}: {len(group_xy):,} worlds")

    _connect_groups(groups, ids=ids, roads=roads, args=args)
    keep_idx = np.flatnonzero(np.isfinite(out["x"]))
    out_points = (
        points.with_row_index("_idx")
        .filter(pl.col("_idx").is_in(keep_idx.tolist()))
        .drop("_idx")
        .with_columns(
            pl.col("x").alias("orig_x"),
            pl.col("y").alias("orig_y"),
        )
        .with_columns(
            pl.Series("x", out["x"][keep_idx]),
            pl.Series("y", out["y"][keep_idx]),
            pl.Series("building_angle", out["building_angle"][keep_idx]),
            pl.Series("building_width", out["building_width"][keep_idx]),
            pl.Series("building_depth", out["building_depth"][keep_idx]),
            pl.Series("building_height", out["building_height"][keep_idx]),
            pl.Series("lot_id", out["lot_id"][keep_idx]),
            pl.Series("block_id", out["block_id"][keep_idx]),
            pl.Series("road_id", out["road_id"][keep_idx]),
            pl.Series("frontage_x", out["frontage_x"][keep_idx]),
            pl.Series("frontage_y", out["frontage_y"][keep_idx]),
        )
    )
    layout_xy = out_points.select("x", "y").to_numpy().astype(np.float64)
    orig_xy = out_points.select("orig_x", "orig_y").to_numpy().astype(np.float64)
    displacement = np.linalg.norm(layout_xy - orig_xy, axis=1)
    out_points = out_points.with_columns(pl.Series("layout_displacement", displacement))
    metrics = {
        "global_median_nn": float(global_nn),
        "point_count": int(out_points.height),
        "processed_islands": sorted(island_set),
        "road_count": int(len(roads)),
        "block_count": int(len(blocks)),
        "displacement_median": float(np.median(displacement)),
        "displacement_p95": float(np.quantile(displacement, 0.95)),
        "displacement_p99": float(np.quantile(displacement, 0.99)),
        "frontage_coverage": 1.0,
    }
    return out_points, roads, blocks, metrics


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--max-islands", type=int, default=3)
    ap.add_argument("--top-level", type=int, default=None)
    ap.add_argument("--sub-level", type=int, default=None)
    ap.add_argument("--sparse-max-worlds", type=int, default=80)
    ap.add_argument("--max-hungarian", type=int, default=1400)
    ap.add_argument("--block-rows", type=int, default=4)
    ap.add_argument("--block-cols", type=int, default=5)
    ap.add_argument("--dense-spacing-global-scale", type=float, default=1.35)
    ap.add_argument("--dense-spacing-nn-scale", type=float, default=1.75)
    ap.add_argument("--dense-spacing-area-scale", type=float, default=1.18)
    ap.add_argument("--dense-spacing-max-scale", type=float, default=5.0)
    ap.add_argument("--building-width-scale", type=float, default=0.52)
    ap.add_argument("--building-depth-scale", type=float, default=0.62)
    ap.add_argument("--sparse-building-scale", type=float, default=1.2)
    ap.add_argument("--sparse-building-max-scale", type=float, default=3.0)
    ap.add_argument("--default-building-height", type=float, default=0.08)
    ap.add_argument("--local-road-width", type=float, default=1.0)
    ap.add_argument("--collector-road-width", type=float, default=1.6)
    ap.add_argument("--service-road-width", type=float, default=0.6)
    ap.add_argument("--collector-knn", type=int, default=4)
    ap.add_argument("--park-buffer-scale", type=float, default=0.8)
    ap.add_argument("--land-raster-max-dim", type=int, default=2048)
    ap.add_argument("--land-raster-nn-cells", type=float, default=2.0)
    ap.add_argument("--land-raster-dilate-cells", type=int, default=8)
    ap.add_argument("--land-raster-close-cells", type=int, default=3)
    ap.add_argument("--land-raster-simplify-cells", type=float, default=0.75)
    ap.add_argument("--land-raster-smooth-cells", type=float, default=0.0)
    ap.add_argument("--land-raster-min-area-cells", type=float, default=4.0)
    ap.add_argument("--region-raster-max-dim", type=int, default=2048)
    ap.add_argument("--region-raster-nn-cells", type=float, default=2.0)
    ap.add_argument("--region-raster-close-cells", type=int, default=1)
    ap.add_argument("--region-raster-simplify-cells", type=float, default=0.75)
    ap.add_argument("--region-raster-smooth-cells", type=float, default=0.0)
    ap.add_argument("--region-raster-min-area-cells", type=float, default=4.0)
    ap.add_argument("--region-buffer-scale", type=float, default=2.0)
    args = ap.parse_args()

    manifest_path = args.in_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    points = pl.read_parquet(args.in_dir / "app_points.parquet")
    levels = _level_numbers(points.columns)
    if not levels:
        raise SystemExit("app_points has no l{n}_sid hierarchy columns")
    top_level = (
        args.top_level
        if args.top_level is not None
        else int(manifest.get("top", levels[-1]))
    )
    sub_level = (
        args.sub_level
        if args.sub_level is not None
        else int(manifest.get("sub", levels[-2]))
    )
    top_col = f"l{top_level}_sid"
    sub_col = f"l{sub_level}_sid"
    missing = [c for c in ("x", "y", top_col, sub_col) if c not in points.columns]
    if missing:
        raise SystemExit(f"app_points missing required columns: {missing}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"city layout from {points.height:,} points; top={top_col}, sub={sub_col}; "
        f"max_islands={args.max_islands}"
    )
    out_points, roads, blocks, metrics = _city_layout(
        points, top_col=top_col, sub_col=sub_col, args=args
    )
    out_points.write_parquet(args.out_dir / "app_points.parquet")
    print(
        f"  wrote {args.out_dir / 'app_points.parquet'} "
        f"({out_points.height:,} points)"
    )

    meta_path = args.in_dir / "worlds_meta.parquet"
    if meta_path.exists():
        shutil.copy2(meta_path, args.out_dir / "worlds_meta.parquet")
        print(f"  copied {args.out_dir / 'worlds_meta.parquet'}")

    layout_xy = out_points.select("x", "y").to_numpy().astype(np.float64)
    orig_xy = out_points.select("orig_x", "orig_y").to_numpy().astype(np.float64)
    global_nn = float(metrics["global_median_nn"])
    orig_land_geom, orig_land_info = build_land_geometry(
        orig_xy,
        method="raster",
        raster_max_dim=args.land_raster_max_dim,
        raster_nn_cells=args.land_raster_nn_cells,
        raster_dilate_cells=args.land_raster_dilate_cells,
        raster_close_cells=args.land_raster_close_cells,
        raster_simplify_cells=args.land_raster_simplify_cells,
        raster_smooth_cells=args.land_raster_smooth_cells,
        raster_min_area_cells=args.land_raster_min_area_cells,
    )
    layout_land_geom, layout_land_info = build_land_geometry(
        layout_xy,
        method="raster",
        raster_max_dim=args.land_raster_max_dim,
        raster_nn_cells=args.land_raster_nn_cells,
        raster_dilate_cells=args.land_raster_dilate_cells,
        raster_close_cells=args.land_raster_close_cells,
        raster_simplify_cells=args.land_raster_simplify_cells,
        raster_smooth_cells=args.land_raster_smooth_cells,
        raster_min_area_cells=args.land_raster_min_area_cells,
    )
    land_geom = _safe_geom(shapely.union_all([orig_land_geom, layout_land_geom]))
    land_info = {
        **orig_land_info,
        "method": "city_union",
        "original_polygon_count": int(orig_land_info["polygon_count"]),
        "layout_polygon_count": int(layout_land_info["polygon_count"]),
        "polygon_count": int(len(list(iter_polygons(land_geom)))),
    }
    land_feature = _geom_feature(land_geom, land_info)
    (args.out_dir / "land.geojson").write_text(
        json.dumps(_feature_collection([land_feature]))
    )
    print(f"  wrote {args.out_dir / 'land.geojson'}")

    landuse_metrics = _write_landuse(
        land_geom=land_geom,
        blocks=blocks,
        out_path=args.out_dir / "landuse.geojson",
        min_area=(global_nn * global_nn) * 4.0,
        park_buffer=global_nn * args.park_buffer_scale,
    )
    metrics.update(landuse_metrics)
    _write_geojson(_road_features(roads), args.out_dir / "roads.geojson")
    _write_geojson(_block_features(blocks), args.out_dir / "blocks.geojson")
    _write_regions(
        out_points,
        levels=[sub_level, top_level],
        out_dir=args.out_dir,
        global_nn=global_nn,
        args=args,
    )

    out_manifest = {
        **manifest,
        "version": max(3, int(manifest.get("version", 1))),
        "point_count": out_points.height,
        "levels": levels,
        "top": top_level,
        "sub": sub_level,
        "layout": "city-blocks-v3",
    }
    assets = dict(out_manifest.get("assets") or {})
    assets.update(
        {
            "points": "app_points.parquet",
            "meta": "worlds_meta.parquet",
            "land": "land.geojson",
            "landuse": "landuse.geojson",
            "roads": "roads.geojson",
            "blocks": "blocks.geojson",
            "regions": [
                f"regions_l{sub_level}.geojson",
                f"regions_l{top_level}.geojson",
            ],
            "metrics": "layout_metrics.json",
        }
    )
    out_manifest["assets"] = assets
    (args.out_dir / "manifest.json").write_text(json.dumps(out_manifest))
    (args.out_dir / "layout_metrics.json").write_text(json.dumps(metrics, indent=2))
    print(f"  wrote {args.out_dir / 'manifest.json'}")
    print(
        "  displacement: "
        f"median={metrics['displacement_median']:.4f}, "
        f"p95={metrics['displacement_p95']:.4f}, "
        f"p99={metrics['displacement_p99']:.4f}"
    )
    print(f"  layout bounds points={len(layout_xy):,}")


if __name__ == "__main__":
    main()
