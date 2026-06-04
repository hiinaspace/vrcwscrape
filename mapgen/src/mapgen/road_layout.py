"""Generate a road-aware static app export from an existing app export.

The first road pass drew streets over the original latent coordinates. This pass
uses the streets as structure: worlds are moved onto parcel-like square plots
along local street segments, while retaining the existing hierarchy ids and
labels. The output is another fully static app dataset directory.
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

import numpy as np
import polars as pl
import shapely
from scipy.spatial import cKDTree

from mapgen.land import write_land_geojson
from mapgen.raster_poly import estimate_cell_size, raster_region_polys
from mapgen.roads import (
    RoadFeature,
    _kmeans_nodes,
    _level_numbers,
    _looped_graph,
    _median_nn,
    _pca_axes,
    _straighten_nodes,
    _write_geojson,
)


@dataclass(frozen=True)
class Segment:
    a: np.ndarray
    b: np.ndarray
    kind: str
    region: int
    subregion: int
    world_count: int


@dataclass(frozen=True)
class Connector:
    xy: np.ndarray
    region: int
    subregion: int
    world_count: int


def _angle_of_axis(axes: np.ndarray) -> float:
    return float(math.atan2(axes[1, 0], axes[0, 0]))


def _robust_extent(local_xy: np.ndarray) -> np.ndarray:
    if len(local_xy) < 3:
        return np.ptp(local_xy, axis=0)
    lo, hi = np.quantile(local_xy, [0.05, 0.95], axis=0)
    return np.maximum(hi - lo, 1e-9)


def _parcel_size(
    xy: np.ndarray,
    *,
    global_nn: float,
    size_scale: float,
    min_scale: float,
    max_scale: float,
) -> float:
    if len(xy) <= 1:
        return global_nn * size_scale
    center, axes = _pca_axes(xy)
    local = (xy - center) @ axes
    extent = _robust_extent(local)
    bbox_spacing = math.sqrt(float(extent[0] * extent[1]) / max(len(xy), 1))
    nn = _median_nn(xy)
    base = max(nn * 1.05, bbox_spacing * size_scale)
    return float(np.clip(base, global_nn * min_scale, global_nn * max_scale))


def _grid_points(
    xy: np.ndarray,
    *,
    parcel_size: float,
    spacing_scale: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Fallback for small groups: make a compact oriented parcel grid."""
    n = len(xy)
    center, axes = _pca_axes(xy)
    angle = _angle_of_axis(axes)
    if n == 1:
        return xy.copy(), np.array([angle], dtype=np.float32)

    local = (xy - center) @ axes
    extent = _robust_extent(local)
    aspect = float(np.clip(extent[0] / max(extent[1], 1e-9), 0.35, 3.0))
    cols = max(1, int(math.ceil(math.sqrt(n * aspect))))
    rows = int(math.ceil(n / cols))
    spacing = parcel_size * spacing_scale
    order = np.lexsort((local[:, 0], local[:, 1]))
    out = np.empty_like(xy)
    x0 = -((cols - 1) * spacing) / 2
    y0 = -((rows - 1) * spacing) / 2
    for rank, idx in enumerate(order):
        c = rank % cols
        r = rank // cols
        pos = np.array([x0 + c * spacing, y0 + r * spacing])
        out[idx] = center + pos @ axes.T
    return out, np.full(n, angle, dtype=np.float32)


def _segments_from_nodes(
    *,
    source_xy: np.ndarray,
    nodes: np.ndarray,
    edges: set[tuple[int, int]],
    region: int,
    subregion: int,
    world_count: int,
    grid_strength: float,
    grid_scale: float,
) -> tuple[list[Segment], np.ndarray]:
    straight, _center, _axes = _straighten_nodes(
        source_xy,
        nodes,
        strength=grid_strength,
        grid_scale=grid_scale,
    )
    segments = []
    for a, b in sorted(edges):
        p = straight[a]
        q = straight[b]
        if float(np.linalg.norm(q - p)) <= 1e-9:
            continue
        segments.append(
            Segment(
                a=p,
                b=q,
                kind="local",
                region=region,
                subregion=subregion,
                world_count=world_count,
            )
        )
    return segments, straight


def _nearest_segments(
    xy: np.ndarray,
    segments: list[Segment],
) -> tuple[np.ndarray, np.ndarray]:
    a = np.stack([s.a for s in segments])
    b = np.stack([s.b for s in segments])
    v = b - a
    len2 = np.maximum((v * v).sum(axis=1), 1e-12)
    rel = xy[:, None, :] - a[None, :, :]
    t = np.clip((rel * v[None, :, :]).sum(axis=2) / len2[None, :], 0.0, 1.0)
    proj = a[None, :, :] + t[:, :, None] * v[None, :, :]
    d2 = ((xy[:, None, :] - proj) ** 2).sum(axis=2)
    nearest = d2.argmin(axis=1)
    return nearest, t[np.arange(len(xy)), nearest]


def _seeded_rng(seed: int, *parts: int) -> np.random.Generator:
    state = seed & 0xFFFFFFFF
    for p in parts:
        state ^= (int(p) + 0x9E3779B9 + ((state << 6) & 0xFFFFFFFF) + (state >> 2))
        state &= 0xFFFFFFFF
    return np.random.default_rng(state)


def _trim_segment_for_members(
    seg: Segment,
    tvals: np.ndarray,
    *,
    parcel_size: float,
    trim_quantile: float,
    trim_pad_scale: float,
    min_span_scale: float,
) -> Segment:
    length = float(np.linalg.norm(seg.b - seg.a))
    if length <= 1e-9 or len(tvals) == 0:
        return seg
    q = float(np.clip(trim_quantile, 0.0, 0.45))
    lo = float(np.quantile(tvals, q))
    hi = float(np.quantile(tvals, 1.0 - q))
    pad = min(0.20, parcel_size * trim_pad_scale / length)
    lo = max(0.0, lo - pad)
    hi = min(1.0, hi + pad)
    min_span = min(1.0, max(0.08, parcel_size * min_span_scale / length))
    if hi - lo < min_span:
        mid = float(np.median(tvals))
        lo = max(0.0, mid - min_span / 2)
        hi = min(1.0, mid + min_span / 2)
    if hi <= lo:
        return seg
    v = seg.b - seg.a
    return Segment(
        a=seg.a + v * lo,
        b=seg.a + v * hi,
        kind=seg.kind,
        region=seg.region,
        subregion=seg.subregion,
        world_count=seg.world_count,
    )


def _relax_parcels(
    xy: np.ndarray,
    *,
    anchors: np.ndarray,
    original_xy: np.ndarray,
    widths: np.ndarray,
    depths: np.ndarray,
    iterations: int,
    clearance: float,
    repel_strength: float,
    anchor_strength: float,
    original_strength: float,
) -> np.ndarray:
    if iterations <= 0 or len(xy) < 2:
        return xy
    pos = xy.copy()
    size = np.maximum(widths, depths).astype(np.float64)
    search_r = max(float(np.quantile(size, 0.98) * clearance), float(size.max()))
    max_step = float(np.median(size) * 0.65)
    for _ in range(iterations):
        pairs = np.array(list(cKDTree(pos).query_pairs(search_r)), dtype=np.int64)
        if len(pairs) == 0:
            break
        a = pairs[:, 0]
        b = pairs[:, 1]
        delta = pos[a] - pos[b]
        dist = np.linalg.norm(delta, axis=1)
        target = (size[a] + size[b]) * 0.5 * clearance
        mask = dist < target
        if not np.any(mask):
            pos += (anchors - pos) * anchor_strength
            continue
        a = a[mask]
        b = b[mask]
        delta = delta[mask]
        dist = np.maximum(dist[mask], 1e-9)
        overlap = target[mask] - dist
        move = (delta / dist[:, None]) * (overlap * repel_strength)[:, None]
        offset = np.zeros_like(pos)
        np.add.at(offset, a, move)
        np.add.at(offset, b, -move)
        step = np.linalg.norm(offset, axis=1)
        too_far = step > max_step
        if np.any(too_far):
            offset[too_far] *= (max_step / step[too_far])[:, None]
        pos += offset
        pos += (anchors - pos) * anchor_strength
        pos += (original_xy - pos) * original_strength
    return pos


def _minor_roads_from_rows(
    xy: np.ndarray,
    *,
    segments: list[Segment],
    seg_ids: np.ndarray,
    lane_sides: np.ndarray,
    lane_rows: np.ndarray,
    widths: np.ndarray,
    min_worlds: int,
) -> list[RoadFeature]:
    groups: dict[tuple[int, int, int], list[int]] = defaultdict(list)
    for i, sid in enumerate(seg_ids.tolist()):
        if sid >= 0:
            groups[(sid, int(lane_sides[i]), int(lane_rows[i]))].append(i)

    roads: list[RoadFeature] = []
    for (sid, _side, _row), members in groups.items():
        seg = segments[sid]
        tangent = seg.b - seg.a
        length = float(np.linalg.norm(tangent))
        if length <= 1e-9:
            continue
        tangent /= length
        normal = np.array([-tangent[1], tangent[0]])
        arr = np.array(members, dtype=np.int64)
        along = (xy[arr] - seg.a) @ tangent
        offset = float(np.mean((xy[arr] - seg.a) @ normal))
        lane_count = len(arr)
        if lane_count < min_worlds:
            mid = float(np.mean(along))
            start = seg.a + tangent * mid
            end = start + normal * offset
            coords = [
                (float(start[0]), float(start[1])),
                (float(end[0]), float(end[1])),
            ]
            roads.append(
                RoadFeature(
                    coords=coords,
                    kind="minor",
                    region=seg.region,
                    subregion=seg.subregion,
                    world_count=lane_count,
                    weight=math.dist(coords[0], coords[1]),
                )
            )
            continue

        pad = float(np.median(widths[arr]) * 0.6)
        lo = max(0.0, float(np.min(along) - pad))
        hi = min(length, float(np.max(along) + pad))
        if hi - lo <= 1e-9:
            continue
        lane_a = seg.a + tangent * lo + normal * offset
        lane_b = seg.a + tangent * hi + normal * offset
        lane_coords = [
            (float(lane_a[0]), float(lane_a[1])),
            (float(lane_b[0]), float(lane_b[1])),
        ]
        roads.append(
            RoadFeature(
                coords=lane_coords,
                kind="minor",
                region=seg.region,
                subregion=seg.subregion,
                world_count=lane_count,
                weight=math.dist(lane_coords[0], lane_coords[1]),
            )
        )

        mid = (lo + hi) / 2
        conn_a = seg.a + tangent * mid
        conn_b = conn_a + normal * offset
        conn_coords = [
            (float(conn_a[0]), float(conn_a[1])),
            (float(conn_b[0]), float(conn_b[1])),
        ]
        roads.append(
            RoadFeature(
                coords=conn_coords,
                kind="minor",
                region=seg.region,
                subregion=seg.subregion,
                world_count=0,
                weight=math.dist(conn_coords[0], conn_coords[1]),
            )
        )
    return roads


def _place_along_segments(
    xy: np.ndarray,
    segments: list[Segment],
    *,
    parcel_size: float,
    spacing_scale: float,
    road_gap_scale: float,
    row_spacing_scale: float,
    max_rows_per_side: int,
    end_gap_scale: float,
    segment_trim_quantile: float,
    segment_trim_pad_scale: float,
    segment_min_span_scale: float,
    width_jitter: float,
    depth_jitter: float,
    angle_jitter: float,
    skew_jitter: float,
    seed: int,
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    list[Segment],
    np.ndarray,
    np.ndarray,
    np.ndarray,
]:
    nearest, t_of_point = _nearest_segments(xy, segments)
    out = np.empty_like(xy)
    angles = np.empty(len(xy), dtype=np.float32)
    widths = np.empty(len(xy), dtype=np.float32)
    depths = np.empty(len(xy), dtype=np.float32)
    skews = np.empty(len(xy), dtype=np.float32)
    seg_ids = np.full(len(xy), -1, dtype=np.int32)
    lane_sides = np.zeros(len(xy), dtype=np.int8)
    lane_rows = np.zeros(len(xy), dtype=np.int16)
    active_segments: list[Segment] = []

    for si, seg in enumerate(segments):
        members = np.flatnonzero(nearest == si)
        if len(members) == 0:
            continue
        seg = _trim_segment_for_members(
            seg,
            t_of_point[members],
            parcel_size=parcel_size,
            trim_quantile=segment_trim_quantile,
            trim_pad_scale=segment_trim_pad_scale,
            min_span_scale=segment_min_span_scale,
        )
        active_id = len(active_segments)
        active_segments.append(seg)
        tangent = seg.b - seg.a
        length = float(np.linalg.norm(tangent))
        if length <= 1e-9:
            continue
        tangent /= length
        normal = np.array([-tangent[1], tangent[0]])
        signed = (xy[members] - seg.a) @ normal
        order = np.lexsort((signed, t_of_point[members]))
        members = members[order]

        target_step = parcel_size * spacing_scale
        rows_per_side = int(math.ceil(len(members) * target_step / (2 * length)))
        rows_per_side = max(1, min(max_rows_per_side, rows_per_side))
        lane_count = rows_per_side * 2
        slots = int(math.ceil(len(members) / lane_count))
        end_gap = min(
            length * 0.28,
            parcel_size * end_gap_scale * (1.0 + 0.18 * rows_per_side),
        )
        usable = max(length - 2 * end_gap, length * 0.40)
        start_gap = (length - usable) / 2
        step = usable / (slots + 1)
        local_size = min(parcel_size, max(parcel_size * 0.55, step / spacing_scale))
        row_spacing = local_size * row_spacing_scale
        road_gap = local_size * road_gap_scale
        angle = float(math.atan2(tangent[1], tangent[0]))
        rng = _seeded_rng(seed, seg.region, seg.subregion, si)

        for rank, idx in enumerate(members):
            slot = rank // lane_count
            lane = rank % lane_count
            side = 1.0 if lane % 2 == 0 else -1.0
            row = lane // 2
            along = start_gap + (slot + 1) * step
            offset = side * (road_gap + local_size / 2 + row * row_spacing)
            width = local_size * float(
                np.clip(1.0 + rng.normal(0.0, width_jitter), 0.72, 1.34)
            )
            depth = local_size * float(
                np.clip(1.0 + rng.normal(0.0, depth_jitter), 0.72, 1.42)
            )
            out[idx] = seg.a + tangent * along + normal * offset
            angles[idx] = angle + float(rng.normal(0.0, angle_jitter))
            widths[idx] = width
            depths[idx] = depth
            skews[idx] = float(np.clip(rng.normal(0.0, skew_jitter), -0.28, 0.28))
            seg_ids[idx] = active_id
            lane_sides[idx] = int(side)
            lane_rows[idx] = row

    return (
        out,
        angles,
        widths,
        depths,
        skews,
        active_segments,
        seg_ids,
        lane_sides,
        lane_rows,
    )


def _road_feature(seg: Segment, *, world_count: int | None = None) -> RoadFeature:
    coords = [
        (float(seg.a[0]), float(seg.a[1])),
        (float(seg.b[0]), float(seg.b[1])),
    ]
    return RoadFeature(
        coords=coords,
        kind=seg.kind,
        region=seg.region,
        subregion=seg.subregion,
        world_count=seg.world_count if world_count is None else world_count,
        weight=math.dist(coords[0], coords[1]),
    )


def _local_layout(
    points: pl.DataFrame,
    *,
    top_col: str,
    sub_col: str,
    global_nn: float,
    args: argparse.Namespace,
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    list[RoadFeature],
    list[Connector],
]:
    xy_all = points.select("x", "y").to_numpy().astype(np.float64)
    out_xy = xy_all.copy()
    out_angle = np.zeros(len(xy_all), dtype=np.float32)
    out_size = np.full(len(xy_all), global_nn, dtype=np.float32)
    out_width = np.full(len(xy_all), global_nn, dtype=np.float32)
    out_depth = np.full(len(xy_all), global_nn, dtype=np.float32)
    out_skew = np.zeros(len(xy_all), dtype=np.float32)
    roads: list[RoadFeature] = []
    connectors: list[Connector] = []

    for row in (
        points.group_by([top_col, sub_col])
        .len()
        .sort("len", descending=True)
        .iter_rows(named=True)
    ):
        region = int(row[top_col])
        subregion = int(row[sub_col])
        n = int(row["len"])
        sub = points.filter(
            (pl.col(top_col) == region) & (pl.col(sub_col) == subregion)
        )
        idx = sub["_idx"].to_numpy()
        xy = sub.select("x", "y").to_numpy().astype(np.float64)
        parcel_size = _parcel_size(
            xy,
            global_nn=global_nn,
            size_scale=args.parcel_size_scale,
            min_scale=args.parcel_min_scale,
            max_scale=args.parcel_max_scale,
        )

        if n < args.local_min_worlds:
            grid_xy, grid_angle = _grid_points(
                xy,
                parcel_size=parcel_size,
                spacing_scale=args.parcel_spacing,
            )
            out_xy[idx] = grid_xy
            out_angle[idx] = grid_angle
            out_size[idx] = parcel_size
            out_width[idx] = parcel_size
            out_depth[idx] = parcel_size
            out_skew[idx] = 0.0
            continue

        k = int(round(n / max(args.local_target_worlds, 1)))
        k = min(
            args.local_max_nodes,
            max(args.local_min_nodes, k),
            max(2, min(args.local_max_nodes, len(xy))),
        )
        if k < 2:
            continue
        nodes = _kmeans_nodes(xy, k, args.seed + subregion * 17)
        edges = _looped_graph(
            nodes,
            loop_factor=args.local_loop_factor,
            max_edge_scale=args.local_max_edge_scale,
            max_edge_quantile=args.local_max_edge_quantile,
        )
        segments, straight_nodes = _segments_from_nodes(
            source_xy=xy,
            nodes=nodes,
            edges=edges,
            region=region,
            subregion=subregion,
            world_count=n,
            grid_strength=args.grid_strength,
            grid_scale=args.grid_scale,
        )
        if not segments:
            grid_xy, grid_angle = _grid_points(
                xy,
                parcel_size=parcel_size,
                spacing_scale=args.parcel_spacing,
            )
            out_xy[idx] = grid_xy
            out_angle[idx] = grid_angle
            out_size[idx] = parcel_size
            out_width[idx] = parcel_size
            out_depth[idx] = parcel_size
            out_skew[idx] = 0.0
            continue

        (
            placed,
            angles,
            widths,
            depths,
            skews,
            active_segments,
            seg_ids,
            lane_sides,
            lane_rows,
        ) = _place_along_segments(
            xy,
            segments,
            parcel_size=parcel_size,
            spacing_scale=args.parcel_spacing,
            road_gap_scale=args.road_gap_scale,
            row_spacing_scale=args.row_spacing_scale,
            max_rows_per_side=args.max_rows_per_side,
            end_gap_scale=args.end_gap_scale,
            segment_trim_quantile=args.segment_trim_quantile,
            segment_trim_pad_scale=args.segment_trim_pad_scale,
            segment_min_span_scale=args.segment_min_span_scale,
            width_jitter=args.parcel_width_jitter,
            depth_jitter=args.parcel_depth_jitter,
            angle_jitter=args.parcel_angle_jitter,
            skew_jitter=args.parcel_skew_jitter,
            seed=args.seed,
        )
        anchors = placed.copy()
        placed = _relax_parcels(
            placed,
            anchors=anchors,
            original_xy=xy,
            widths=widths,
            depths=depths,
            iterations=args.relax_iterations,
            clearance=args.relax_clearance,
            repel_strength=args.relax_repel_strength,
            anchor_strength=args.relax_anchor_strength,
            original_strength=args.relax_original_strength,
        )
        out_xy[idx] = placed
        out_angle[idx] = angles
        out_width[idx] = widths
        out_depth[idx] = depths
        out_size[idx] = (widths + depths) / 2
        out_skew[idx] = skews

        valid_seg = seg_ids >= 0
        counts = np.bincount(
            seg_ids[valid_seg],
            minlength=len(active_segments),
        )
        for si, seg in enumerate(active_segments):
            if counts[si] > 0:
                roads.append(_road_feature(seg, world_count=int(counts[si])))
        if args.minor_roads:
            roads.extend(
                _minor_roads_from_rows(
                    placed,
                    segments=active_segments,
                    seg_ids=seg_ids,
                    lane_sides=lane_sides,
                    lane_rows=lane_rows,
                    widths=widths,
                    min_worlds=args.minor_min_worlds,
                )
            )

        centroid = xy.mean(axis=0)
        if active_segments:
            connector_nodes = np.vstack(
                [seg.a for seg in active_segments] + [seg.b for seg in active_segments]
            )
        else:
            connector_nodes = straight_nodes
        nearest_node = int(
            np.argmin(((connector_nodes - centroid) ** 2).sum(axis=1))
        )
        connectors.append(
            Connector(
                xy=connector_nodes[nearest_node],
                region=region,
                subregion=subregion,
                world_count=n,
            )
        )

    return (
        out_xy,
        out_angle,
        out_size,
        out_width,
        out_depth,
        out_skew,
        roads,
        connectors,
    )


def _arterial_roads(
    connectors: list[Connector],
    *,
    args: argparse.Namespace,
) -> list[RoadFeature]:
    roads: list[RoadFeature] = []
    by_region: dict[int, list[Connector]] = defaultdict(list)
    for c in connectors:
        by_region[c.region].append(c)
    for region, conns in by_region.items():
        if len(conns) < args.arterial_min_subregions:
            continue
        xy = np.stack([c.xy for c in conns])
        edges = _looped_graph(
            xy,
            loop_factor=args.arterial_loop_factor,
            max_edge_scale=args.arterial_max_edge_scale,
            max_edge_quantile=args.arterial_max_edge_quantile,
        )
        for a, b in sorted(edges):
            seg = Segment(
                a=xy[a],
                b=xy[b],
                kind="arterial",
                region=region,
                subregion=-1,
                world_count=conns[a].world_count + conns[b].world_count,
            )
            roads.append(_road_feature(seg))
    return roads


def _mode_map(
    df: pl.DataFrame,
    key_col: str,
    value_col: str,
    *,
    default: str = "",
) -> dict[int, str]:
    if value_col not in df.columns:
        return {}
    tmp = (
        df.select(key_col, value_col)
        .filter(pl.col(value_col).is_not_null() & (pl.col(value_col) != default))
        .group_by(key_col)
        .agg(pl.col(value_col).mode().first().alias("_v"))
    )
    return {
        int(r[key_col]): str(r["_v"])
        for r in tmp.iter_rows(named=True)
        if r["_v"] is not None
    }


def _write_regions(
    points: pl.DataFrame,
    *,
    out_dir: Path,
    levels: list[int],
    top_level: int,
    args: argparse.Namespace,
) -> None:
    xy = points.select("x", "y").to_numpy().astype(np.float64)
    nn = _median_nn(xy)
    cell_size = estimate_cell_size(
        xy,
        max_dim=args.region_raster_max_dim,
        median_nn=nn,
        nn_cells=args.region_raster_nn_cells,
    )
    print(
        f"  region raster: cell_size={cell_size:.5f}, "
        f"buffer={nn * args.region_buffer_scale:.5f}"
    )

    region_soft = points[f"l{top_level}_sid"].to_numpy()
    for lvl in levels:
        id_col = f"l{lvl}_sid"
        if id_col not in points.columns:
            print(f"  !! level {lvl} missing ({id_col}); skipping geojson")
            continue
        name_col = f"l{lvl}_sname"
        label_by_id = _mode_map(points, id_col, name_col)
        cluster_id = points[id_col].to_numpy()
        cids = sorted(int(c) for c in set(cluster_id.tolist()) if int(c) != -1)
        polys = raster_region_polys(
            xy,
            cluster_id,
            cids,
            cell_size=cell_size,
            radius=nn * args.region_buffer_scale,
            close_cells=args.region_raster_close_cells,
            simplify_cells=args.region_raster_simplify_cells,
            smooth_cells=args.region_raster_smooth_cells,
            min_area_cells=args.region_raster_min_area_cells,
        )

        modal = (
            pl.DataFrame({"_c": cluster_id, "_r": region_soft})
            .group_by("_c")
            .agg(pl.col("_r").mode().first().alias("_rr"))
        )
        region_by_id = {
            int(r["_c"]): int(r["_rr"])
            for r in modal.iter_rows(named=True)
            if r["_rr"] is not None
        }
        color_by_region = _mode_map(points, f"l{top_level}_sid", "color")
        feats = []
        for cid, geom in polys.items():
            region = region_by_id.get(int(cid), -1)
            feats.append(
                {
                    "type": "Feature",
                    "properties": {
                        "cluster_id": int(cid),
                        "label": label_by_id.get(int(cid), str(cid)),
                        "color": color_by_region.get(region, "#888888"),
                        "region": region,
                        "size": int((cluster_id == cid).sum()),
                    },
                    "geometry": json.loads(shapely.to_geojson(geom)),
                }
            )
        out_path = out_dir / f"regions_l{lvl}.geojson"
        out_path.write_text(
            json.dumps({"type": "FeatureCollection", "features": feats})
        )
        print(f"  wrote {out_path} ({len(feats)} regions)")


def _geojson_levels(manifest: dict, fallback: list[int]) -> list[int]:
    assets = manifest.get("assets") or {}
    out = []
    for name in assets.get("regions") or []:
        stem = Path(name).stem
        if stem.startswith("regions_l"):
            with suppress(ValueError):
                out.append(int(stem.removeprefix("regions_l")))
    return sorted(set(out or fallback))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--top-level", type=int, default=None)
    ap.add_argument("--sub-level", type=int, default=None)
    ap.add_argument("--local-target-worlds", type=int, default=220)
    ap.add_argument("--local-min-worlds", type=int, default=160)
    ap.add_argument("--local-min-nodes", type=int, default=4)
    ap.add_argument("--local-max-nodes", type=int, default=96)
    ap.add_argument("--local-loop-factor", type=float, default=0.55)
    ap.add_argument("--local-max-edge-scale", type=float, default=2.1)
    ap.add_argument("--local-max-edge-quantile", type=float, default=0.66)
    ap.add_argument("--arterial-min-subregions", type=int, default=4)
    ap.add_argument("--arterial-loop-factor", type=float, default=0.22)
    ap.add_argument("--arterial-max-edge-scale", type=float, default=2.4)
    ap.add_argument("--arterial-max-edge-quantile", type=float, default=0.70)
    ap.add_argument("--grid-strength", type=float, default=0.45)
    ap.add_argument("--grid-scale", type=float, default=0.95)
    ap.add_argument("--parcel-size-scale", type=float, default=0.82)
    ap.add_argument("--parcel-min-scale", type=float, default=0.70)
    ap.add_argument("--parcel-max-scale", type=float, default=2.40)
    ap.add_argument("--parcel-spacing", type=float, default=1.28)
    ap.add_argument("--road-gap-scale", type=float, default=0.70)
    ap.add_argument("--row-spacing-scale", type=float, default=1.18)
    ap.add_argument("--max-rows-per-side", type=int, default=12)
    ap.add_argument("--end-gap-scale", type=float, default=2.4)
    ap.add_argument("--segment-trim-quantile", type=float, default=0.03)
    ap.add_argument("--segment-trim-pad-scale", type=float, default=3.0)
    ap.add_argument("--segment-min-span-scale", type=float, default=7.0)
    ap.add_argument("--parcel-width-jitter", type=float, default=0.10)
    ap.add_argument("--parcel-depth-jitter", type=float, default=0.13)
    ap.add_argument("--parcel-angle-jitter", type=float, default=0.025)
    ap.add_argument("--parcel-skew-jitter", type=float, default=0.045)
    ap.add_argument("--relax-iterations", type=int, default=7)
    ap.add_argument("--relax-clearance", type=float, default=1.08)
    ap.add_argument("--relax-repel-strength", type=float, default=0.28)
    ap.add_argument("--relax-anchor-strength", type=float, default=0.18)
    ap.add_argument("--relax-original-strength", type=float, default=0.025)
    ap.add_argument(
        "--minor-roads",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="emit thin service lanes/stubs connecting parcels to local roads",
    )
    ap.add_argument("--minor-min-worlds", type=int, default=2)
    ap.add_argument("--land-raster-max-dim", type=int, default=2048)
    ap.add_argument("--land-raster-nn-cells", type=float, default=2.0)
    ap.add_argument("--land-raster-dilate-cells", type=int, default=7)
    ap.add_argument("--land-raster-close-cells", type=int, default=3)
    ap.add_argument("--land-raster-simplify-cells", type=float, default=0.75)
    ap.add_argument("--land-raster-smooth-cells", type=float, default=0.0)
    ap.add_argument("--land-raster-min-area-cells", type=float, default=4.0)
    ap.add_argument("--region-buffer-scale", type=float, default=1.35)
    ap.add_argument("--region-raster-max-dim", type=int, default=2048)
    ap.add_argument("--region-raster-nn-cells", type=float, default=2.0)
    ap.add_argument("--region-raster-close-cells", type=int, default=1)
    ap.add_argument("--region-raster-simplify-cells", type=float, default=0.75)
    ap.add_argument("--region-raster-smooth-cells", type=float, default=0.0)
    ap.add_argument("--region-raster-min-area-cells", type=float, default=4.0)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    points_path = args.in_dir / "app_points.parquet"
    manifest_path = args.in_dir / "manifest.json"
    points = pl.read_parquet(points_path)
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    levels = manifest.get("levels") or _level_numbers(points.columns)
    levels = sorted(int(v) for v in levels)
    if not levels:
        raise SystemExit("input app_points has no l{n}_sid hierarchy columns")
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
    missing = [
        c
        for c in ("world_id", "x", "y", top_col, sub_col)
        if c not in points.columns
    ]
    if missing:
        raise SystemExit(f"input app_points missing required columns: {missing}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    work = points.with_row_index("_idx")
    xy = work.select("x", "y").to_numpy().astype(np.float64)
    global_nn = _median_nn(xy)
    print(
        f"road layout from {work.height:,} points; top={top_col}, sub={sub_col}; "
        f"global_nn={global_nn:.5f}"
    )

    (
        layout_xy,
        angle,
        size,
        width,
        depth,
        skew,
        local_roads,
        connectors,
    ) = _local_layout(
        work,
        top_col=top_col,
        sub_col=sub_col,
        global_nn=global_nn,
        args=args,
    )
    arterial_roads = _arterial_roads(connectors, args=args)
    roads = arterial_roads + local_roads

    displacement = np.linalg.norm(layout_xy - xy, axis=1)
    print(
        "  displacement: "
        f"median={np.median(displacement):.4f}, "
        f"p95={np.quantile(displacement, 0.95):.4f}"
    )

    out_points = (
        work.drop("_idx")
        .with_columns(
            pl.col("x").alias("orig_x"),
            pl.col("y").alias("orig_y"),
        )
        .with_columns(
            pl.Series("x", layout_xy[:, 0]),
            pl.Series("y", layout_xy[:, 1]),
            pl.Series("parcel_angle", angle),
            pl.Series("parcel_size", size),
            pl.Series("parcel_width", width),
            pl.Series("parcel_depth", depth),
            pl.Series("parcel_skew", skew),
        )
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

    out_manifest = {
        **manifest,
        "version": max(2, int(manifest.get("version", 1))),
        "point_count": out_points.height,
        "levels": levels,
        "top": top_level,
        "sub": sub_level,
        "layout": "road-parcels-v2",
    }
    assets = dict(out_manifest.get("assets") or {})
    assets.update(
        {
            "points": "app_points.parquet",
            "meta": "worlds_meta.parquet",
            "land": "land.geojson",
            "roads": "roads.geojson",
        }
    )
    out_manifest["assets"] = assets
    (args.out_dir / "manifest.json").write_text(json.dumps(out_manifest))
    print(f"  wrote {args.out_dir / 'manifest.json'}")

    _write_geojson(roads, args.out_dir / "roads.geojson")
    write_land_geojson(
        layout_xy,
        args.out_dir / "land.geojson",
        method="raster",
        raster_max_dim=args.land_raster_max_dim,
        raster_nn_cells=args.land_raster_nn_cells,
        raster_dilate_cells=args.land_raster_dilate_cells,
        raster_close_cells=args.land_raster_close_cells,
        raster_simplify_cells=args.land_raster_simplify_cells,
        raster_smooth_cells=args.land_raster_smooth_cells,
        raster_min_area_cells=args.land_raster_min_area_cells,
    )
    _write_regions(
        out_points,
        out_dir=args.out_dir,
        levels=_geojson_levels(manifest, [sub_level, top_level]),
        top_level=top_level,
        args=args,
    )


if __name__ == "__main__":
    main()
