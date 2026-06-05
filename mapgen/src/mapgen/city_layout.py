"""Generate a city-style static app export from an existing map export.

This is an experimental replacement for Voronoi/road-parcel views. It treats the
2D DR coordinates as demand points, creates island-level curved street fields,
and places every world as a compact road-fronting building footprint.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import networkx as nx
import numpy as np
import polars as pl
import shapely
from scipy.optimize import linear_sum_assignment
from scipy.spatial import cKDTree
from shapely import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPolygon,
    Polygon,
)
from shapely.ops import polygonize
from shapely.ops import split as split_geom

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


@dataclass
class RoadSpec:
    coords: list[tuple[float, float]]
    kind: str
    island_id: int
    width: float
    family: int
    depth: int = -1


@dataclass
class SlotSet:
    xy: np.ndarray
    frontage: np.ndarray
    angle: np.ndarray
    width: np.ndarray
    depth: np.ndarray
    road_index: np.ndarray
    side: np.ndarray
    along: np.ndarray


@dataclass
class SplitParcel:
    geom: Polygon
    point_idx: np.ndarray
    depth: int


@dataclass
class SplitLineCandidate:
    line: LineString
    normal_axis: int
    tangent_axis: int
    major_axis: int
    shape_ratio: float
    tangent_span: float


@dataclass
class PlanarStreetResult:
    road_specs: list[RoadSpec]
    leaves: list[SplitParcel]
    build_geom: object
    splits: float
    metrics: dict[str, float | int]


@dataclass
class DensityGuide:
    centers: np.ndarray


@dataclass
class MeshCenter:
    xy: np.ndarray
    axes: np.ndarray
    members: np.ndarray
    radius: float


@dataclass
class MeshBlock:
    geom: Polygon
    point_idx: np.ndarray


@dataclass
class MeshSlotResult:
    slots: SlotSet
    block_index: np.ndarray
    rejected_outside: int
    rejected_roadless: int


@dataclass
class IslandLayoutResult:
    idx: np.ndarray
    island_id: int
    columns: dict[str, np.ndarray]
    roads: list[Road]
    blocks: list[Block]
    info: dict[str, int | float]


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


_KDTREE_WORKERS = 1


def _set_spatial_workers(workers: int) -> None:
    global _KDTREE_WORKERS
    _KDTREE_WORKERS = int(workers)


def _spatial_workers() -> int:
    return int(_KDTREE_WORKERS)


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
    dist, _ = cKDTree(xy).query(xy[idx], k=2, workers=_spatial_workers())
    return float(np.median(dist[:, 1]))


def _nn_quantile(xy: np.ndarray, q: float, sample: int = 12000) -> float:
    if len(xy) < 2:
        return 0.0
    rng = np.random.default_rng(0)
    idx = rng.choice(len(xy), size=min(sample, len(xy)), replace=False)
    dist, _ = cKDTree(xy).query(xy[idx], k=2, workers=_spatial_workers())
    return float(np.quantile(dist[:, 1], q))


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


def _iter_lines(geom):
    if isinstance(geom, LineString):
        yield geom
    elif isinstance(geom, MultiLineString):
        yield from geom.geoms
    elif isinstance(geom, GeometryCollection):
        for g in geom.geoms:
            yield from _iter_lines(g)


def _chaikin_ring(coords, iterations: int) -> list[tuple[float, float]]:
    arr = np.asarray(coords, dtype=np.float64)
    if len(arr) < 4 or iterations <= 0:
        return [(float(x), float(y)) for x, y in arr]
    arr = arr[:-1]
    for _ in range(iterations):
        nxt = np.roll(arr, -1, axis=0)
        q = arr * 0.75 + nxt * 0.25
        r = arr * 0.25 + nxt * 0.75
        out = np.empty((len(arr) * 2, 2), dtype=np.float64)
        out[0::2] = q
        out[1::2] = r
        arr = out
    closed = np.vstack([arr, arr[0]])
    return [(float(x), float(y)) for x, y in closed]


def _chaikin_line_coords(
    coords: np.ndarray,
    *,
    iterations: int,
    closed: bool,
) -> np.ndarray:
    if len(coords) < 3 or iterations <= 0:
        return coords
    arr = np.asarray(coords, dtype=np.float64)
    if closed and np.linalg.norm(arr[0] - arr[-1]) <= 1e-9:
        arr = arr[:-1]
    for _ in range(iterations):
        if closed:
            nxt = np.roll(arr, -1, axis=0)
            q = arr * 0.75 + nxt * 0.25
            r = arr * 0.25 + nxt * 0.75
            out = np.empty((len(arr) * 2, 2), dtype=np.float64)
            out[0::2] = q
            out[1::2] = r
            arr = out
        else:
            seg_a = arr[:-1]
            seg_b = arr[1:]
            q = seg_a * 0.75 + seg_b * 0.25
            r = seg_a * 0.25 + seg_b * 0.75
            out = np.empty((2 * (len(arr) - 1) + 2, 2), dtype=np.float64)
            out[0] = arr[0]
            out[-1] = arr[-1]
            out[1:-1:2] = q
            out[2:-1:2] = r
            arr = out
    if closed:
        arr = np.vstack([arr, arr[0]])
    return arr


def _resample_line_coords(
    coords: np.ndarray,
    *,
    step: float,
    closed: bool,
) -> np.ndarray:
    if len(coords) < 2 or step <= 0:
        return coords
    arr = np.asarray(coords, dtype=np.float64)
    if closed and np.linalg.norm(arr[0] - arr[-1]) > 1e-9:
        arr = np.vstack([arr, arr[0]])
    seg = arr[1:] - arr[:-1]
    seg_len = np.linalg.norm(seg, axis=1)
    good = seg_len > 1e-9
    if not np.any(good):
        return coords
    seg = seg[good]
    starts = arr[:-1][good]
    seg_len = seg_len[good]
    cum = np.concatenate([[0.0], np.cumsum(seg_len)])
    total = float(cum[-1])
    if total <= step:
        return arr
    if closed:
        n = max(8, int(math.ceil(total / step)))
        dist = np.linspace(0.0, total, n, endpoint=False)
    else:
        n = max(2, int(math.ceil(total / step)) + 1)
        dist = np.linspace(0.0, total, n)
    si = np.searchsorted(cum, dist, side="right") - 1
    si = np.clip(si, 0, len(seg_len) - 1)
    t = (dist - cum[si]) / seg_len[si]
    out = starts[si] + seg[si] * t[:, None]
    if closed:
        out = np.vstack([out, out[0]])
    return out


def _relax_line_curvature(
    coords: np.ndarray,
    *,
    max_turn_deg: float,
    iterations: int,
    strength: float,
    closed: bool,
) -> np.ndarray:
    if len(coords) < 4 or iterations <= 0 or strength <= 0:
        return coords
    arr = np.asarray(coords, dtype=np.float64)
    if closed and np.linalg.norm(arr[0] - arr[-1]) <= 1e-9:
        arr = arr[:-1]
    max_turn = math.radians(max_turn_deg)
    if max_turn <= 0:
        return coords
    for _ in range(iterations):
        prev = np.roll(arr, 1, axis=0) if closed else arr[:-2]
        cur = arr if closed else arr[1:-1]
        nxt = np.roll(arr, -1, axis=0) if closed else arr[2:]
        if len(cur) == 0:
            break
        a = cur - prev
        b = nxt - cur
        al = np.linalg.norm(a, axis=1)
        bl = np.linalg.norm(b, axis=1)
        good = (al > 1e-9) & (bl > 1e-9)
        if not np.any(good):
            break
        cos = np.ones(len(cur), dtype=np.float64)
        cos[good] = np.sum(a[good] * b[good], axis=1) / (al[good] * bl[good])
        turn = np.arccos(np.clip(cos, -1.0, 1.0))
        excess = np.clip((turn - max_turn) / max(math.pi - max_turn, 1e-9), 0.0, 1.0)
        weight = (excess * strength)[:, None]
        target = (prev + nxt) * 0.5
        updated = cur * (1.0 - weight) + target * weight
        if closed:
            arr = updated
        else:
            arr = arr.copy()
            arr[1:-1] = updated
        if float(excess.max(initial=0.0)) <= 1e-4:
            break
    if closed:
        arr = np.vstack([arr, arr[0]])
    return arr


def _smooth_coastline_geom(geom, *, iterations: int, simplify: float):
    if geom.is_empty:
        return geom
    if iterations <= 0 and simplify <= 0:
        return geom
    parts = []
    for poly in iter_polygons(geom):
        exterior = _chaikin_ring(poly.exterior.coords, iterations)
        holes = [_chaikin_ring(r.coords, iterations) for r in poly.interiors]
        smoothed = Polygon(exterior, holes)
        if simplify > 0:
            smoothed = smoothed.simplify(simplify, preserve_topology=True)
        smoothed = _safe_geom(smoothed)
        if not smoothed.is_empty:
            parts.extend(iter_polygons(smoothed))
    if not parts:
        return geom
    out = parts[0] if len(parts) == 1 else MultiPolygon(parts)
    return _safe_geom(out)


def _slotset_empty() -> SlotSet:
    return SlotSet(
        xy=np.empty((0, 2), dtype=np.float64),
        frontage=np.empty((0, 2), dtype=np.float64),
        angle=np.empty(0, dtype=np.float32),
        width=np.empty(0, dtype=np.float32),
        depth=np.empty(0, dtype=np.float32),
        road_index=np.empty(0, dtype=np.int32),
        side=np.empty(0, dtype=np.int8),
        along=np.empty(0, dtype=np.float32),
    )


def _take_slots(slots: SlotSet, keep: np.ndarray) -> SlotSet:
    return SlotSet(
        xy=slots.xy[keep],
        frontage=slots.frontage[keep],
        angle=slots.angle[keep],
        width=slots.width[keep],
        depth=slots.depth[keep],
        road_index=slots.road_index[keep],
        side=slots.side[keep],
        along=slots.along[keep],
    )


def _concat_slots(slotsets: list[SlotSet]) -> SlotSet:
    slotsets = [s for s in slotsets if len(s.xy)]
    if not slotsets:
        return _slotset_empty()
    return SlotSet(
        xy=np.vstack([s.xy for s in slotsets]),
        frontage=np.vstack([s.frontage for s in slotsets]),
        angle=np.concatenate([s.angle for s in slotsets]),
        width=np.concatenate([s.width for s in slotsets]),
        depth=np.concatenate([s.depth for s in slotsets]),
        road_index=np.concatenate([s.road_index for s in slotsets]),
        side=np.concatenate([s.side for s in slotsets]),
        along=np.concatenate([s.along for s in slotsets]),
    )


def _density_scaled_slots(
    slots: SlotSet,
    *,
    demand_xy: np.ndarray,
    global_nn: float,
    base_building_scale: float,
    island_id: int,
    args,
) -> SlotSet:
    if (
        len(slots.xy) == 0
        or len(demand_xy) < 2
        or args.building_density_scale_power <= 0
    ):
        return slots
    k = min(max(1, int(args.building_density_knn)), len(demand_xy))
    tree = cKDTree(demand_xy)
    slot_dist, _slot_idx = tree.query(
        slots.frontage,
        k=k,
        workers=_spatial_workers(),
    )
    slot_dist = np.asarray(slot_dist, dtype=np.float64)
    slot_radius = slot_dist if slot_dist.ndim == 1 else slot_dist[:, -1]

    sample_n = min(max(1, int(args.building_density_reference_sample)), len(demand_xy))
    rng = np.random.default_rng(args.seed + island_id * 3301)
    sample = rng.choice(len(demand_xy), size=sample_n, replace=False)
    ref_k = min(k + 1, len(demand_xy))
    ref_dist, _ref_idx = tree.query(
        demand_xy[sample],
        k=ref_k,
        workers=_spatial_workers(),
    )
    ref_dist = np.asarray(ref_dist, dtype=np.float64)
    ref_radius = ref_dist if ref_dist.ndim == 1 else ref_dist[:, -1]
    ref = float(
        np.quantile(
            ref_radius[np.isfinite(ref_radius)],
            args.building_density_reference_quantile,
        )
    )
    ref = max(ref, global_nn * 0.15, 1e-9)
    scale = np.clip(
        (np.maximum(slot_radius, ref * 0.05) / ref)
        ** args.building_density_scale_power,
        args.building_density_scale_min,
        args.building_density_scale_max,
    )
    width = (slots.width.astype(np.float64) * scale).astype(np.float32)
    depth = (slots.depth.astype(np.float64) * scale).astype(np.float32)
    normal = np.column_stack([-np.sin(slots.angle), np.cos(slots.angle)]).astype(
        np.float64
    )
    setback = (
        global_nn
        * args.building_setback_scale
        * np.sqrt(np.maximum(base_building_scale * scale, 1e-9))
    )
    offset = depth.astype(np.float64) * 0.5 + setback
    xy = (
        slots.frontage
        + normal * slots.side[:, None].astype(np.float64) * offset[:, None]
    )
    return SlotSet(
        xy=xy,
        frontage=slots.frontage,
        angle=slots.angle,
        width=width,
        depth=depth,
        road_index=slots.road_index,
        side=slots.side,
        along=slots.along,
    )


def _covers_xy(geom, xy: np.ndarray) -> np.ndarray:
    if len(xy) == 0:
        return np.zeros(0, dtype=bool)
    pts = shapely.points(xy[:, 0], xy[:, 1])
    return np.asarray(shapely.covers(geom, pts), dtype=bool)


def _grid_filter_slots(
    slots: SlotSet,
    *,
    min_dist: float,
    seed: int,
    priority: np.ndarray | None = None,
    footprint_radius_scale: float = 0.0,
) -> SlotSet:
    if min_dist <= 0 or len(slots.xy) < 2:
        return slots
    rng = np.random.default_rng(seed)
    radii = None
    max_check_dist = float(min_dist)
    if footprint_radius_scale > 0:
        radii = 0.5 * np.hypot(slots.width, slots.depth)
        max_check_dist = max(
            max_check_dist,
            float(radii.max(initial=0.0) * 2.0 * footprint_radius_scale),
        )
    if priority is None:
        order = rng.permutation(len(slots.xy))
    else:
        rank = np.where(priority, 0, 1)
        order = np.lexsort((rng.random(len(slots.xy)), rank))
    cell = max(float(max_check_dist), 1e-12)
    min_d2 = float(min_dist * min_dist)
    accepted: list[int] = []
    buckets: dict[tuple[int, int], list[int]] = defaultdict(list)
    for i in order.tolist():
        p = slots.xy[i]
        key = (math.floor(float(p[0]) / cell), math.floor(float(p[1]) / cell))
        ok = True
        for gx in range(key[0] - 1, key[0] + 2):
            for gy in range(key[1] - 1, key[1] + 2):
                for j in buckets.get((gx, gy), []):
                    limit2 = min_d2
                    if radii is not None:
                        limit = max(
                            float(min_dist),
                            float((radii[i] + radii[j]) * footprint_radius_scale),
                        )
                        limit2 = limit * limit
                    if float(np.sum((p - slots.xy[j]) ** 2)) < limit2:
                        ok = False
                        break
                if not ok:
                    break
            if not ok:
                break
        if ok:
            buckets[key].append(i)
            accepted.append(i)
    keep = np.asarray(sorted(accepted), dtype=np.int64)
    return _take_slots(slots, keep)


def _relax_assignment_targets(
    xy: np.ndarray,
    *,
    global_nn: float,
    island_id: int,
    args,
) -> tuple[np.ndarray, dict[str, float]]:
    if args.assignment_relax_iterations <= 0 or len(xy) < 2:
        return xy, {
            "assignment_relax_median": 0.0,
            "assignment_relax_p95": 0.0,
            "assignment_relax_last_pairs": 0.0,
        }
    pos = xy.astype(np.float64, copy=True)
    orig = pos.copy()
    min_dist = global_nn * args.assignment_relax_min_dist_scale
    if min_dist <= 0:
        return xy, {
            "assignment_relax_median": 0.0,
            "assignment_relax_p95": 0.0,
            "assignment_relax_last_pairs": 0.0,
        }
    rng = np.random.default_rng(args.seed + island_id * 1709)
    max_step = min_dist * args.assignment_relax_max_step_scale
    max_displacement = global_nn * args.assignment_relax_max_displacement_scale
    eps = max(min_dist * 1e-6, 1e-12)
    last_pairs = 0
    for _ in range(args.assignment_relax_iterations):
        pairs = cKDTree(pos).query_pairs(min_dist, output_type="ndarray")
        last_pairs = int(len(pairs))
        if last_pairs == 0:
            break
        a = pairs[:, 0]
        b = pairs[:, 1]
        delta = pos[a] - pos[b]
        dist = np.linalg.norm(delta, axis=1)
        zero = dist < eps
        if np.any(zero):
            angle = rng.uniform(0.0, math.tau, int(np.count_nonzero(zero)))
            delta[zero, 0] = np.cos(angle) * eps
            delta[zero, 1] = np.sin(angle) * eps
            dist[zero] = eps
        overlap = min_dist - dist
        move = delta * (
            (overlap * args.assignment_relax_strength * 0.5) / dist
        )[:, None]
        offset = np.zeros_like(pos)
        np.add.at(offset, a, move)
        np.add.at(offset, b, -move)
        step = np.linalg.norm(offset, axis=1)
        too_far = step > max_step
        if np.any(too_far):
            offset[too_far] *= (max_step / step[too_far])[:, None]
        pos += offset
        if args.assignment_relax_anchor > 0:
            pos += (orig - pos) * args.assignment_relax_anchor
        if max_displacement > 0:
            disp = pos - orig
            dist0 = np.linalg.norm(disp, axis=1)
            clipped = dist0 > max_displacement
            if np.any(clipped):
                pos[clipped] = orig[clipped] + disp[clipped] * (
                    max_displacement / dist0[clipped]
                )[:, None]
    disp = np.linalg.norm(pos - orig, axis=1)
    return pos, {
        "assignment_relax_median": float(np.median(disp)),
        "assignment_relax_p95": float(np.quantile(disp, 0.95)),
        "assignment_relax_last_pairs": float(last_pairs),
    }


def _filter_slots_for_major_corridors(
    slots: SlotSet,
    road_specs: list[RoadSpec],
    *,
    global_nn: float,
    args,
) -> SlotSet:
    if len(slots.xy) == 0:
        return slots
    major_mask = np.asarray(
        [spec.kind == "arterial" for spec in road_specs],
        dtype=bool,
    )
    if not np.any(major_mask):
        return slots
    lines = [
        LineString(spec.coords)
        for spec, is_major in zip(road_specs, major_mask, strict=True)
        if is_major
    ]
    major_geom = shapely.unary_union(lines)
    if major_geom.is_empty:
        return slots
    clearance = max(
        global_nn * args.major_corridor_clearance_scale,
        float(np.median(np.maximum(slots.width, slots.depth)))
        * args.major_corridor_building_clearance_scale,
    )
    corridor = major_geom.buffer(clearance, cap_style="round", join_style="round")
    pts = shapely.points(slots.xy[:, 0], slots.xy[:, 1])
    blocked = np.asarray(shapely.covers(corridor, pts), dtype=bool)
    own_major = major_mask[np.clip(slots.road_index, 0, len(major_mask) - 1)]
    return _take_slots(slots, own_major | ~blocked)


def _filter_slots_for_road_corridors(
    slots: SlotSet,
    road_specs: list[RoadSpec],
    *,
    global_nn: float,
    args,
) -> SlotSet:
    if (
        len(slots.xy) == 0
        or (
            args.road_corridor_clearance_scale <= 0
            and args.road_corridor_building_clearance_scale <= 0
        )
    ):
        return slots
    lines = [LineString(spec.coords) for spec in road_specs]
    road_geom = shapely.unary_union(lines)
    if road_geom.is_empty:
        return slots
    clearance = max(
        global_nn * args.road_corridor_clearance_scale,
        float(np.median(np.maximum(slots.width, slots.depth)))
        * args.road_corridor_building_clearance_scale,
    )
    corridor = road_geom.buffer(clearance, cap_style="round", join_style="round")
    pts = shapely.points(slots.xy[:, 0], slots.xy[:, 1])
    blocked = np.asarray(shapely.covers(corridor, pts), dtype=bool)
    return _take_slots(slots, ~blocked)


def _slot_footprint_geoms(slots: SlotSet):
    if len(slots.xy) == 0:
        return np.asarray([], dtype=object)
    tangent = np.column_stack([np.cos(slots.angle), np.sin(slots.angle)]).astype(
        np.float64,
    )
    normal = np.column_stack([-tangent[:, 1], tangent[:, 0]])
    half_w = slots.width.astype(np.float64)[:, None] * 0.5
    half_d = slots.depth.astype(np.float64)[:, None] * 0.5
    xy = slots.xy.astype(np.float64)
    corners = np.stack(
        [
            xy - tangent * half_w - normal * half_d,
            xy + tangent * half_w - normal * half_d,
            xy + tangent * half_w + normal * half_d,
            xy - tangent * half_w + normal * half_d,
            xy - tangent * half_w - normal * half_d,
        ],
        axis=1,
    )
    return shapely.polygons(corners)


def _filter_slots_by_footprints(
    slots: SlotSet,
    *,
    land_geom,
    road_specs: list[RoadSpec],
    global_nn: float,
    args,
) -> SlotSet:
    if len(slots.xy) == 0 or not args.slot_footprint_validation:
        return slots
    footprints = _slot_footprint_geoms(slots)
    keep = np.asarray(shapely.covers(land_geom, footprints), dtype=bool)
    if road_specs and args.slot_footprint_road_clearance_scale > 0:
        road_hits = _footprint_other_road_hits(
            footprints,
            road_specs=road_specs,
            road_index=slots.road_index,
            clearance=global_nn * args.slot_footprint_road_clearance_scale,
        )
        keep &= ~road_hits
    return _take_slots(slots, keep)


def _slot_other_road_hit_mask(
    slots: SlotSet,
    *,
    road_specs: list[RoadSpec],
    global_nn: float,
    args,
) -> np.ndarray:
    if len(slots.xy) == 0 or args.slot_footprint_road_clearance_scale <= 0:
        return np.zeros(len(slots.xy), dtype=bool)
    return _footprint_other_road_hits(
        _slot_footprint_geoms(slots),
        road_specs=road_specs,
        road_index=slots.road_index,
        clearance=global_nn * args.slot_footprint_road_clearance_scale,
    )


def _footprint_other_road_hits(
    footprints,
    *,
    road_specs: list[RoadSpec],
    road_index: np.ndarray,
    clearance: float,
) -> np.ndarray:
    hits = np.zeros(len(footprints), dtype=bool)
    if len(footprints) == 0 or clearance <= 0:
        return hits
    corridors = []
    corridor_road_index = []
    for i, spec in enumerate(road_specs):
        if len(spec.coords) < 2:
            continue
        line = LineString(spec.coords)
        if line.length <= 1e-9:
            continue
        corridors.append(
            line.buffer(clearance, cap_style="round", join_style="round")
        )
        corridor_road_index.append(i)
    if not corridors:
        return hits
    pairs = shapely.STRtree(corridors).query(footprints, predicate="intersects")
    pairs = np.asarray(pairs, dtype=np.int64)
    if pairs.size == 0:
        return hits
    input_idx = pairs[0]
    tree_idx = pairs[1]
    corridor_road_index_arr = np.asarray(corridor_road_index, dtype=np.int32)
    other = corridor_road_index_arr[tree_idx] != road_index[input_idx]
    if np.any(other):
        hits[np.unique(input_idx[other])] = True
    return hits


def _selected_footprint_metrics(
    *,
    selected_xy: np.ndarray,
    selected_width: np.ndarray,
    selected_depth: np.ndarray,
    selected_angle: np.ndarray,
    selected_road: np.ndarray,
    road_specs: list[RoadSpec],
    global_nn: float,
    args,
) -> dict[str, int]:
    slots = SlotSet(
        xy=selected_xy,
        frontage=selected_xy,
        angle=selected_angle,
        width=selected_width,
        depth=selected_depth,
        road_index=selected_road.astype(np.int32, copy=False),
        side=np.zeros(len(selected_xy), dtype=np.int8),
        along=np.zeros(len(selected_xy), dtype=np.float32),
    )
    footprints = _slot_footprint_geoms(slots)
    road_hits = 0
    if len(road_specs) and args.slot_footprint_road_clearance_scale > 0:
        road_hits = int(
            np.count_nonzero(
                _footprint_other_road_hits(
                    footprints,
                    road_specs=road_specs,
                    road_index=slots.road_index,
                    clearance=global_nn * args.slot_footprint_road_clearance_scale,
                )
            )
        )
    overlap_pairs = 0
    if len(selected_xy) > 1:
        radius = float(
            np.quantile(
                np.hypot(
                    selected_width.astype(np.float64),
                    selected_depth.astype(np.float64),
                ),
                args.slot_footprint_overlap_query_quantile,
            )
        )
        if radius > 0:
            pairs = cKDTree(selected_xy).query_pairs(
                radius * args.slot_footprint_overlap_query_scale,
            )
            for a, b in pairs:
                if footprints[int(a)].intersects(footprints[int(b)]):
                    overlap_pairs += 1
    return {
        "footprint_road_hits": int(road_hits),
        "footprint_overlap_pairs": int(overlap_pairs),
    }


def _sample_slots_for_road(
    line: LineString,
    *,
    road_index: int,
    road_spacing: float,
    slot_step: float,
    global_nn: float,
    building_scale: float,
    args,
    rng: np.random.Generator,
) -> SlotSet:
    coords = np.asarray(line.coords, dtype=np.float64)
    if len(coords) < 2:
        return _slotset_empty()
    closed = float(np.linalg.norm(coords[0] - coords[-1])) <= 1e-9
    seg = coords[1:] - coords[:-1]
    seg_len = np.linalg.norm(seg, axis=1)
    good = seg_len > 1e-9
    if not np.any(good):
        return _slotset_empty()
    seg = seg[good]
    starts = coords[:-1][good]
    seg_len = seg_len[good]
    cum = np.concatenate([[0.0], np.cumsum(seg_len)])
    total = float(cum[-1])
    end_gap = (
        0.0
        if closed
        else min(total * 0.22, slot_step * args.road_slot_end_gap_scale)
    )
    if total <= end_gap * 2 + slot_step:
        return _slotset_empty()
    start = slot_step * 0.5 if closed else end_gap
    dist = np.arange(start, total - end_gap, slot_step, dtype=np.float64)
    if len(dist) == 0:
        return _slotset_empty()
    si = np.searchsorted(cum, dist, side="right") - 1
    si = np.clip(si, 0, len(seg_len) - 1)
    t = (dist - cum[si]) / seg_len[si]
    tangent = seg[si] / seg_len[si, None]
    frontage = starts[si] + tangent * (t * seg_len[si])[:, None]
    normal = np.column_stack([-tangent[:, 1], tangent[:, 0]])
    base_width = slot_step * args.building_width_scale * building_scale
    base_depth = min(
        slot_step * args.building_depth_scale * building_scale,
        road_spacing * args.building_depth_road_spacing_scale,
    )
    setback = global_nn * args.building_setback_scale * math.sqrt(building_scale)
    offset = base_depth / 2 + setback
    xy = np.vstack([frontage + normal * offset, frontage - normal * offset])
    frontage2 = np.vstack([frontage, frontage])
    angle = np.concatenate(
        [
            np.arctan2(tangent[:, 1], tangent[:, 0]),
            np.arctan2(tangent[:, 1], tangent[:, 0]),
        ]
    ).astype(np.float32)
    width = np.full(len(xy), base_width, dtype=np.float32)
    depth = np.full(len(xy), base_depth, dtype=np.float32)
    width *= np.clip(
        1.0 + rng.normal(0, args.building_width_jitter, len(xy)),
        0.72,
        1.28,
    ).astype(np.float32)
    depth *= np.clip(
        1.0 + rng.normal(0, args.building_depth_jitter, len(xy)),
        0.72,
        1.35,
    ).astype(np.float32)
    angle += rng.normal(0, args.building_angle_jitter, len(xy)).astype(np.float32)
    return SlotSet(
        xy=xy,
        frontage=frontage2,
        angle=angle,
        width=width,
        depth=depth,
        road_index=np.full(len(xy), road_index, dtype=np.int32),
        side=np.concatenate(
            [
                np.ones(len(frontage), dtype=np.int8),
                -np.ones(len(frontage), dtype=np.int8),
            ]
        ),
        along=np.concatenate([dist, dist]).astype(np.float32),
    )


def _generate_warped_streets(
    *,
    xy: np.ndarray,
    land_geom,
    island_id: int,
    global_nn: float,
    road_spacing: float,
    slot_step: float,
    args,
) -> tuple[list[RoadSpec], SlotSet]:
    center, axes = _pca_axes(xy)
    local = (xy - center) @ axes
    q = args.road_extent_quantile
    lo, hi = np.quantile(local, [q, 1.0 - q], axis=0)
    pad = road_spacing * args.road_extent_pad_scale
    lo = lo - pad
    hi = hi + pad
    rng = np.random.default_rng(args.seed + island_id * 1009 + int(road_spacing * 1e6))
    roads: list[RoadSpec] = []
    slotsets: list[SlotSet] = []
    road_index = 0

    for family in (0, 1):
        cross_axis = 0 if family == 0 else 1
        along_axis = 1 if family == 0 else 0
        offsets = np.arange(
            lo[cross_axis] - road_spacing,
            hi[cross_axis] + road_spacing,
            road_spacing,
            dtype=np.float64,
        )
        offsets += rng.uniform(
            -road_spacing * args.road_jitter_scale,
            road_spacing * args.road_jitter_scale,
            len(offsets),
        )
        along_span = float(hi[along_axis] - lo[along_axis])
        sample_step = max(global_nn * 8.0, road_spacing * 1.35)
        sample_n = int(math.ceil(max(along_span, sample_step) / sample_step)) + 1
        sample_n = int(np.clip(sample_n, 32, args.road_curve_max_vertices))
        along = np.linspace(lo[along_axis], hi[along_axis], sample_n)
        wavelength = max(road_spacing * args.road_curve_wavelength_scale, global_nn)
        amp = road_spacing * args.road_curve_amplitude_scale
        for line_no, off in enumerate(offsets.tolist()):
            phase = rng.uniform(0.0, math.tau)
            phase2 = rng.uniform(0.0, math.tau)
            warp = amp * np.sin((along / wavelength) * math.tau + phase)
            warp += amp * 0.35 * np.sin(
                (along / (wavelength * 2.7)) * math.tau + phase2
            )
            local_line = np.zeros((len(along), 2), dtype=np.float64)
            local_line[:, along_axis] = along
            local_line[:, cross_axis] = off + warp
            world = _to_world(local_line, center, axes)
            clipped = LineString(world).intersection(land_geom)
            for seg_line in _iter_lines(clipped):
                if seg_line.length < slot_step * args.min_road_length_slots:
                    continue
                if args.road_simplify_scale > 0:
                    seg_line = seg_line.simplify(
                        global_nn * args.road_simplify_scale,
                        preserve_topology=False,
                    )
                if seg_line.length < slot_step * args.min_road_length_slots:
                    continue
                kind = (
                    "collector"
                    if line_no % max(args.collector_every, 1) == 0
                    else "local"
                )
                roads.append(
                    RoadSpec(
                        coords=[
                            (float(px), float(py)) for px, py in seg_line.coords
                        ],
                        kind=kind,
                        island_id=island_id,
                        width=(
                            args.collector_road_width
                            if kind == "collector"
                            else args.local_road_width
                        ),
                        family=family,
                    )
                )
                slotsets.append(
                    _sample_slots_for_road(
                        seg_line,
                        road_index=road_index,
                        road_spacing=road_spacing,
                        slot_step=slot_step,
                        global_nn=global_nn,
                        building_scale=1.0,
                        args=args,
                        rng=rng,
                    )
                )
                road_index += 1

    slots = _concat_slots(slotsets)
    if len(slots.xy):
        keep = _covers_xy(land_geom, slots.xy)
        slots = _take_slots(slots, keep)
        min_dist = max(
            global_nn * args.slot_filter_min_global_scale,
            float(np.median(np.maximum(slots.width, slots.depth)))
            * args.slot_filter_building_scale,
        )
        slots = _grid_filter_slots(
            slots,
            min_dist=min_dist,
            seed=args.seed + island_id * 9176,
            footprint_radius_scale=args.slot_filter_footprint_radius_scale,
        )
    return roads, slots


def _polygon_axes(poly: Polygon, pts: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(pts) >= 8:
        return _pca_axes(pts)
    rect = poly.minimum_rotated_rectangle
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    if len(coords) < 4:
        c = np.array([poly.centroid.x, poly.centroid.y], dtype=np.float64)
        return c, np.eye(2)
    edges = np.roll(coords, -1, axis=0) - coords
    lens = np.linalg.norm(edges, axis=1)
    i = int(np.argmax(lens))
    axis0 = edges[i] / max(lens[i], 1e-12)
    axis1 = np.array([-axis0[1], axis0[0]], dtype=np.float64)
    axes = np.column_stack([axis0, axis1])
    if np.linalg.det(axes) < 0:
        axes[:, 1] *= -1
    return coords.mean(axis=0), axes


def _classify_points_to_polys(
    parts: list[Polygon],
    xy: np.ndarray,
    members: np.ndarray,
) -> list[np.ndarray]:
    if not parts:
        return []
    if len(members) == 0:
        return [np.empty(0, dtype=np.int64) for _ in parts]
    pts_xy = xy[members]
    pts = shapely.points(pts_xy[:, 0], pts_xy[:, 1])
    assigned = np.full(len(members), -1, dtype=np.int32)
    for i, poly in enumerate(parts):
        mask = np.asarray(shapely.covers(poly, pts), dtype=bool) & (assigned == -1)
        assigned[mask] = i
    missing = np.flatnonzero(assigned == -1)
    if len(missing):
        reps = np.array(
            [[p.representative_point().x, p.representative_point().y] for p in parts],
            dtype=np.float64,
        )
        _d, near = cKDTree(reps).query(
            pts_xy[missing],
            k=1,
            workers=_spatial_workers(),
        )
        assigned[missing] = np.asarray(near, dtype=np.int32)
    return [members[assigned == i] for i in range(len(parts))]


def _line_specs_from_geom(
    geom,
    *,
    island_id: int,
    kind: str,
    width: float,
    family: int,
    min_length: float,
    depth: int = -1,
) -> list[RoadSpec]:
    specs = []
    for line in _iter_lines(geom):
        if line.length < min_length:
            continue
        specs.append(
            RoadSpec(
                coords=[(float(x), float(y)) for x, y in line.coords],
                kind=kind,
                island_id=island_id,
                width=width,
                family=family,
                depth=depth,
            )
        )
    return specs


def _road_smooth_iterations(kind: str, args) -> int:
    if kind == "arterial":
        return args.road_smooth_arterial_iterations
    if kind == "collector":
        return args.road_smooth_collector_iterations
    if kind == "local":
        return args.road_smooth_local_iterations
    return args.road_smooth_service_iterations


def _road_max_turn_deg(kind: str, args) -> float:
    if kind == "arterial":
        return args.road_curvature_arterial_max_turn_deg
    if kind == "collector":
        return args.road_curvature_collector_max_turn_deg
    if kind == "local":
        return args.road_curvature_local_max_turn_deg
    if kind == "service":
        return args.road_curvature_service_max_turn_deg
    return 0.0


def _smooth_road_line(
    line: LineString,
    *,
    land_geom,
    global_nn: float,
    kind: str,
    min_length: float,
    args,
) -> list[LineString]:
    coords = np.asarray(line.coords, dtype=np.float64)
    if len(coords) < 2 or line.length < min_length:
        return []
    closed = float(np.linalg.norm(coords[0] - coords[-1])) <= 1e-9
    iterations = max(0, int(_road_smooth_iterations(kind, args)))
    if iterations > 0:
        target_vertices = max(
            12,
            int(args.road_smooth_max_vertices) // max(1, 2**iterations),
        )
        step = max(
            global_nn * args.road_smooth_resample_scale,
            float(line.length) / max(target_vertices, 1),
        )
        coords = _resample_line_coords(coords, step=step, closed=closed)
        coords = _chaikin_line_coords(coords, iterations=iterations, closed=closed)
    max_turn = _road_max_turn_deg(kind, args)
    if max_turn > 0 and args.road_curvature_relax_iterations > 0:
        coords = _relax_line_curvature(
            coords,
            max_turn_deg=max_turn,
            iterations=args.road_curvature_relax_iterations,
            strength=args.road_curvature_relax_strength,
            closed=closed,
        )
    smoothed = LineString(coords)
    if (
        iterations > 0
        and args.road_smooth_simplify_scale > 0
        and len(smoothed.coords) > 3
    ):
        smoothed = smoothed.simplify(
            global_nn * args.road_smooth_simplify_scale,
            preserve_topology=False,
        )
    smoothed = _safe_geom(smoothed)
    if land_geom is not None and not smoothed.is_empty:
        clip_geom = land_geom
        if kind in {"arterial", "collector"} and args.major_road_bridge_scale > 0:
            clip_geom = land_geom.buffer(
                global_nn * args.major_road_bridge_scale,
                join_style="round",
            )
        smoothed = smoothed.intersection(clip_geom)
    return [seg for seg in _iter_lines(smoothed) if seg.length >= min_length]


def _postprocess_road_specs(
    specs: list[RoadSpec],
    *,
    land_geom,
    global_nn: float,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    out: list[RoadSpec] = []
    for spec in _merge_major_road_specs(specs):
        min_length = (
            slot_step * args.boundary_road_min_length_slots
            if spec.kind in {"local", "service", "slot"}
            else slot_step * args.min_road_length_slots
        )
        lines = _smooth_road_line(
            LineString(spec.coords),
            land_geom=land_geom,
            global_nn=global_nn,
            kind=spec.kind,
            min_length=min_length,
            args=args,
        )
        for line in lines:
            out.append(
                RoadSpec(
                    coords=[(float(x), float(y)) for x, y in line.coords],
                    kind=spec.kind,
                    island_id=spec.island_id,
                    width=spec.width,
                    family=spec.family,
                    depth=spec.depth,
                )
            )
    return out


def _merge_major_road_specs(specs: list[RoadSpec]) -> list[RoadSpec]:
    grouped: dict[tuple[str, int, float, int, int], list[LineString]] = defaultdict(
        list
    )
    passthrough: list[RoadSpec] = []
    for spec in specs:
        if spec.kind in {"arterial", "collector"}:
            grouped[
                (spec.kind, spec.island_id, spec.width, spec.family, spec.depth)
            ].append(LineString(spec.coords))
        else:
            passthrough.append(spec)
    merged_specs = list(passthrough)
    for (kind, island_id, width, family, depth), lines in grouped.items():
        geom = shapely.line_merge(shapely.unary_union(lines))
        for line in _iter_lines(geom):
            if line.length <= 1e-9:
                continue
            merged_specs.append(
                RoadSpec(
                    coords=[(float(x), float(y)) for x, y in line.coords],
                    kind=kind,
                    island_id=island_id,
                    width=width,
                    family=family,
                    depth=depth,
                )
            )
    return merged_specs


def _nearest_point_on_geom(point: np.ndarray, geom) -> np.ndarray | None:
    if geom.is_empty:
        return None
    p = shapely.Point(float(point[0]), float(point[1]))
    d = geom.project(p)
    try:
        q = geom.interpolate(d)
    except (shapely.GEOSException, TypeError):
        return None
    if q.is_empty:
        return None
    return np.array([q.x, q.y], dtype=np.float64)


def _endpoint_connector_specs(
    road_specs: list[RoadSpec],
    *,
    land_geom,
    island_id: int,
    global_nn: float,
    args,
) -> list[RoadSpec]:
    if args.road_endpoint_connector_max_scale <= 0:
        return []
    arterial = [
        LineString(spec.coords)
        for spec in road_specs
        if spec.kind == "arterial" and LineString(spec.coords).length > 1e-9
    ]
    trunk = [
        LineString(spec.coords)
        for spec in road_specs
        if spec.kind in {"arterial", "collector"}
        and LineString(spec.coords).length > 1e-9
    ]
    arterial_geom = shapely.unary_union(arterial) if arterial else GeometryCollection()
    trunk_geom = shapely.unary_union(trunk) if trunk else GeometryCollection()
    if arterial_geom.is_empty and trunk_geom.is_empty:
        return []
    min_dist = global_nn * args.road_endpoint_connector_min_scale
    max_dist = global_nn * args.road_endpoint_connector_max_scale
    allowed = land_geom.buffer(
        global_nn * args.major_road_bridge_scale,
        join_style="round",
    )
    out: list[RoadSpec] = []
    seen: set[tuple[int, int, int, int]] = set()
    for spec in road_specs:
        if spec.kind not in {"collector", "local"}:
            continue
        coords = np.asarray(spec.coords, dtype=np.float64)
        if len(coords) < 2:
            continue
        dest = arterial_geom if spec.kind == "collector" else trunk_geom
        if dest.is_empty:
            continue
        for endpoint in (coords[0], coords[-1]):
            q = _nearest_point_on_geom(endpoint, dest)
            if q is None:
                continue
            dist = float(np.linalg.norm(endpoint - q))
            if dist < min_dist or dist > max_dist:
                continue
            coords = [
                (float(endpoint[0]), float(endpoint[1])),
                (float(q[0]), float(q[1])),
            ]
            line = LineString(coords)
            if line.length <= 1e-9 or not shapely.covers(allowed, line):
                continue
            key = tuple(int(round(v / max(global_nn, 1e-9))) for v in (*endpoint, *q))
            rkey = (key[2], key[3], key[0], key[1])
            if key in seen or rkey in seen:
                continue
            seen.add(key)
            out.append(
                RoadSpec(
                    coords=coords,
                    kind=spec.kind,
                    island_id=island_id,
                    width=spec.width,
                    family=-4,
                    depth=spec.depth,
                )
            )
    return out


def _boundary_road_specs(
    boundary,
    *,
    trunk_specs: list[RoadSpec],
    island_id: int,
    global_nn: float,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    min_length = slot_step * args.boundary_road_min_length_slots
    trunk_lines = [
        LineString(spec.coords)
        for spec in trunk_specs
        if spec.kind == "collector" and spec.depth <= args.recursive_collector_depth
    ]
    trunk_geom = (
        shapely.unary_union(trunk_lines) if trunk_lines else GeometryCollection()
    )
    trunk_band = (
        trunk_geom.buffer(
            max(global_nn * args.boundary_trunk_snap_scale, slot_step * 0.25),
            cap_style="flat",
            join_style="round",
        )
        if not trunk_geom.is_empty
        else GeometryCollection()
    )
    specs: list[RoadSpec] = []
    for line in _iter_lines(boundary):
        if line.length < min_length:
            continue
        kind = "local"
        width = args.local_road_width
        depth = -1
        if not trunk_band.is_empty:
            overlap = line.intersection(trunk_band).length / max(line.length, 1e-9)
            if overlap >= args.boundary_trunk_overlap_frac:
                kind = "collector"
                width = args.collector_road_width
                depth = 0
        specs.append(
            RoadSpec(
                coords=[(float(x), float(y)) for x, y in line.coords],
                kind=kind,
                island_id=island_id,
                width=width,
                family=-3,
                depth=depth,
            )
        )
    return specs


def _coastal_road_specs(
    poly: Polygon,
    *,
    island_id: int,
    kind: str,
    width: float,
    offset: float,
    min_length: float,
    args,
) -> list[RoadSpec]:
    base = poly
    soften = offset * args.coastal_road_soften_scale
    if soften > 0:
        softened = _safe_geom(
            poly.buffer(soften, join_style="round").buffer(
                -soften,
                join_style="round",
            )
        )
        parts = [p for p in iter_polygons(softened) if p.area > 0]
        if parts:
            base = max(parts, key=lambda p: p.intersection(poly).area)
    inner = base.buffer(-offset, join_style="round")
    lines = []
    for p in iter_polygons(inner):
        if p.area > 0:
            lines.append(LineString(p.exterior.coords))
    if not lines:
        lines = [LineString(base.exterior.coords)]
    specs = []
    for line in lines:
        specs.extend(
            _line_specs_from_geom(
                line,
                island_id=island_id,
                kind=kind,
                width=width,
                family=-2,
                min_length=min_length,
                depth=0,
            )
        )
    return specs


def _planning_parts(
    land_geom,
    *,
    coastal_offset: float,
    min_area: float,
    args,
) -> list[Polygon]:
    parts: list[Polygon] = []
    for poly in iter_polygons(land_geom):
        if poly.area <= 0:
            continue
        buildable = poly
        if (
            coastal_offset > 0
            and poly.area >= min_area * args.planar_coastal_inset_min_area_scale
        ):
            inset = _safe_geom(poly.buffer(-coastal_offset, join_style="round"))
            inset_parts = [p for p in iter_polygons(inset) if p.area >= min_area]
            if inset_parts:
                inset_union = shapely.unary_union(inset_parts)
                if inset_union.area >= poly.area * args.planar_coastal_inset_min_frac:
                    buildable = inset_union
        parts.extend(p for p in iter_polygons(buildable) if p.area >= min_area)
    return sorted(parts, key=lambda p: p.area, reverse=True)


def _planar_boundary_road_specs(
    parts: list[Polygon],
    *,
    members_by_part: list[np.ndarray],
    island_id: int,
    global_nn: float,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    specs: list[RoadSpec] = []
    min_length = slot_step * args.min_road_length_slots
    for part_no, poly in enumerate(parts):
        count = len(members_by_part[part_no]) if part_no < len(members_by_part) else 0
        ring_min_area = (
            global_nn * global_nn * args.planar_boundary_ring_min_area_scale
        )
        ring_allowed = (
            count >= args.planar_boundary_ring_min_worlds
            and poly.area >= ring_min_area
        )
        if not ring_allowed:
            specs.extend(
                _planar_spine_road_specs(
                    poly,
                    island_id=island_id,
                    part_no=part_no,
                    world_count=count,
                    slot_step=slot_step,
                    args=args,
                )
            )
            continue
        if count < args.coastal_arterial_min_worlds or (
            poly.area
            < (global_nn * global_nn) * args.coastal_arterial_min_area_scale
        ):
            kind = "collector"
            width = args.collector_road_width
        else:
            kind = "arterial"
            width = args.arterial_road_width
        coords = _resample_line_coords(
            np.asarray(poly.exterior.coords, dtype=np.float64),
            step=max(global_nn * args.planar_boundary_resample_scale, slot_step),
            closed=True,
        )
        coords = _relax_line_curvature(
            coords,
            max_turn_deg=args.road_curvature_arterial_max_turn_deg,
            iterations=args.road_curvature_relax_iterations,
            strength=args.road_curvature_relax_strength,
            closed=True,
        )
        line = LineString(coords)
        if line.length >= min_length:
            specs.append(
                RoadSpec(
                    coords=[(float(x), float(y)) for x, y in line.coords],
                    kind=kind,
                    island_id=island_id,
                    width=width,
                    family=-10000 - part_no,
                    depth=0,
                )
            )
        for ring_no, hole in enumerate(poly.interiors):
            line = LineString(hole.coords)
            if line.length < min_length:
                continue
            specs.append(
                RoadSpec(
                    coords=[(float(x), float(y)) for x, y in line.coords],
                    kind="collector",
                    island_id=island_id,
                    width=args.collector_road_width,
                    family=-(20000 + part_no * 100 + ring_no),
                    depth=0,
                )
            )
    return specs


def _planar_spine_road_specs(
    poly: Polygon,
    *,
    island_id: int,
    part_no: int,
    world_count: int,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    if world_count <= 0:
        return []
    center, axes = _parcel_shape_axes(poly)
    lo, hi = _project_poly_bounds(poly, center, axes)
    span = np.maximum(hi - lo, 1e-9)
    along_axis = int(np.argmax(span))
    cross_axis = 1 - along_axis
    local = np.zeros((2, 2), dtype=np.float64)
    local[:, along_axis] = [lo[along_axis], hi[along_axis]]
    local[:, cross_axis] = (lo[cross_axis] + hi[cross_axis]) * 0.5
    line = LineString(_to_world(local, center, axes)).intersection(poly)
    candidates = [seg for seg in _iter_lines(line) if seg.length > 1e-9]
    if not candidates:
        return []
    line = max(candidates, key=lambda seg: seg.length)
    min_length = slot_step * args.boundary_road_min_length_slots
    if line.length < min_length:
        return []
    kind = "local" if world_count >= args.recursive_min_split_worlds else "service"
    width = args.local_road_width if kind == "local" else args.service_road_width
    return [
        RoadSpec(
            coords=[(float(x), float(y)) for x, y in line.coords],
            kind=kind,
            island_id=island_id,
            width=width,
            family=-30000 - part_no,
            depth=0,
        )
    ]


def _polygon_irregularity(poly: Polygon) -> float:
    if poly.is_empty or poly.area <= 1e-12:
        return 1e6
    rect = poly.minimum_rotated_rectangle
    rect_area = max(float(rect.area), 1e-12)
    fill_penalty = max(0.0, 1.0 - float(poly.area) / rect_area)
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    if len(coords) >= 4:
        edges = np.roll(coords, -1, axis=0) - coords
        lens = np.linalg.norm(edges, axis=1)
        long = float(max(lens.max(), 1e-12))
        short = float(max(lens.min(), 1e-12))
        aspect_penalty = abs(math.log(np.clip(long / short, 1.0, 1e6))) * 0.18
        rect_perimeter = max(float(lens.sum()), 1e-12)
    else:
        aspect_penalty = 1.0
        rect_perimeter = max(math.sqrt(rect_area) * 4.0, 1e-12)
    perimeter_penalty = max(0.0, float(poly.length) / rect_perimeter - 1.0) * 0.35
    return fill_penalty + aspect_penalty + perimeter_penalty


def _polygon_aspect_ratio(poly: Polygon) -> float:
    if poly.is_empty or poly.area <= 1e-12:
        return 1e6
    rect = poly.minimum_rotated_rectangle
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    if len(coords) < 4:
        return 1.0
    edges = np.roll(coords, -1, axis=0) - coords
    lens = np.linalg.norm(edges, axis=1)
    short = float(max(lens.min(), 1e-12))
    return float(max(lens.max(), short) / short)


def _parcel_shape_axes(poly: Polygon) -> tuple[np.ndarray, np.ndarray]:
    rect = poly.minimum_rotated_rectangle
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    if len(coords) < 4:
        c = np.array([poly.centroid.x, poly.centroid.y], dtype=np.float64)
        return c, np.eye(2)
    edges = np.roll(coords, -1, axis=0) - coords
    lens = np.linalg.norm(edges, axis=1)
    i = int(np.argmax(lens))
    axis0 = edges[i] / max(float(lens[i]), 1e-12)
    axis1 = np.array([-axis0[1], axis0[0]], dtype=np.float64)
    axes = np.column_stack([axis0, axis1])
    if np.linalg.det(axes) < 0:
        axes[:, 1] *= -1
    return coords.mean(axis=0), axes


def _density_guide_for_points(
    xy: np.ndarray,
    *,
    global_nn: float,
    args,
) -> DensityGuide | None:
    if (
        not args.planar_density_isoline_enabled
        or len(xy) < args.planar_density_min_worlds
    ):
        return None
    k = min(max(2, int(args.planar_density_knn)), len(xy))
    dist, _idx = cKDTree(xy).query(xy, k=k, workers=_spatial_workers())
    radius = np.asarray(dist, dtype=np.float64)
    radius = radius if radius.ndim == 1 else radius[:, -1]
    finite = np.isfinite(radius) & (radius > 0)
    if not np.any(finite):
        return None
    threshold = float(
        np.quantile(radius[finite], args.planar_density_center_quantile)
    )
    candidates = np.flatnonzero(finite & (radius <= threshold))
    if len(candidates) == 0:
        return None
    order = candidates[np.argsort(radius[candidates], kind="stable")]
    min_sep = max(global_nn * args.planar_density_center_min_sep_scale, 1e-9)
    centers: list[np.ndarray] = []
    for i in order.tolist():
        p = xy[i]
        if any(float(np.linalg.norm(p - q)) < min_sep for q in centers):
            continue
        centers.append(p.astype(np.float64, copy=True))
        if len(centers) >= args.planar_density_max_centers:
            break
    if not centers:
        return None
    return DensityGuide(centers=np.vstack(centers))


def _density_isoline_split_candidates(
    parcel: SplitParcel,
    *,
    density_guide: DensityGuide | None,
    global_nn: float,
    args,
) -> list[SplitLineCandidate]:
    if (
        density_guide is None
        or len(density_guide.centers) == 0
        or parcel.depth > args.planar_density_isoline_max_depth
    ):
        return []
    centroid = np.array(
        [parcel.geom.centroid.x, parcel.geom.centroid.y],
        dtype=np.float64,
    )
    extent = max(math.sqrt(max(float(parcel.geom.area), 1e-12)), global_nn)
    center_dist = np.linalg.norm(density_guide.centers - centroid, axis=1)
    nearest_order = np.argsort(center_dist, kind="stable")[
        : args.planar_density_centers_per_parcel
    ]
    out: list[SplitLineCandidate] = []
    for center_idx in nearest_order.tolist():
        dense_center = density_guide.centers[center_idx]
        dense_point = shapely.Point(float(dense_center[0]), float(dense_center[1]))
        if parcel.geom.distance(dense_point) > max(
            extent * args.planar_density_center_reach_scale,
            global_nn * args.planar_density_center_reach_min_scale,
        ):
            continue
        radial = centroid - dense_center
        radius = float(np.linalg.norm(radial))
        if radius < global_nn * args.planar_density_min_radius_scale:
            continue
        radial /= radius
        tangent = np.array([-radial[1], radial[0]], dtype=np.float64)
        axes = np.column_stack([tangent, radial])
        coords = np.asarray(parcel.geom.exterior.coords, dtype=np.float64)
        local_poly = (coords - centroid) @ axes
        lo = local_poly.min(axis=0)
        hi = local_poly.max(axis=0)
        span = np.maximum(hi - lo, 1e-9)
        if span.min() < global_nn * args.planar_min_split_width_scale:
            continue
        pad = span[0] * 0.42 + global_nn * 8.0
        t0 = lo[0] - pad
        t1 = hi[0] + pad
        n = int(
            np.clip(
                math.ceil((t1 - t0) / max(global_nn * 2.0, span[0] / 180)),
                36,
                args.road_curve_max_vertices,
            )
        )
        tangent_pos = np.linspace(t0, t1, n)
        safe_radius = max(radius, global_nn * args.planar_density_min_radius_scale)
        curve = -(tangent_pos * tangent_pos) / (2.0 * safe_radius)
        curve = np.clip(
            curve * args.planar_density_isoline_curve_strength,
            -span[1] * args.planar_density_isoline_max_curve_frac,
            span[1] * args.planar_density_isoline_max_curve_frac,
        )
        margin = max(
            span[1] * args.planar_split_margin_frac,
            global_nn * args.planar_min_split_margin_scale,
        )
        offsets = [0.0]
        if args.planar_density_isoline_offsets > 1:
            offsets.extend(
                [
                    -span[1] * args.planar_density_isoline_offset_frac,
                    span[1] * args.planar_density_isoline_offset_frac,
                ][: args.planar_density_isoline_offsets - 1]
            )
        for off in offsets:
            normal = np.clip(curve + off, lo[1] + margin, hi[1] - margin)
            if np.allclose(normal, lo[1] + margin) or np.allclose(
                normal,
                hi[1] - margin,
            ):
                continue
            local = np.column_stack([tangent_pos, normal])
            line = LineString(_to_world(local, centroid, axes))
            if line.length > global_nn:
                out.append(
                    SplitLineCandidate(
                        line=line,
                        normal_axis=1,
                        tangent_axis=0,
                        major_axis=0,
                        shape_ratio=float(span.max() / max(span.min(), 1e-9)),
                        tangent_span=float(span[0]),
                    )
                )
    return out


def _split_road_kind(depth: int, args) -> tuple[str, float]:
    if depth < args.planar_collector_depth:
        return "collector", args.collector_road_width
    if depth < args.planar_service_depth:
        return "local", args.local_road_width
    return "service", args.service_road_width


def _planar_split_candidate_lines(
    parcel: SplitParcel,
    xy: np.ndarray,
    *,
    density_guide: DensityGuide | None,
    global_nn: float,
    args,
    rng: np.random.Generator,
) -> list[SplitLineCandidate]:
    pts = xy[parcel.point_idx]
    center, axes = _parcel_shape_axes(parcel.geom)
    lo, hi = _project_poly_bounds(parcel.geom, center, axes)
    span = np.maximum(hi - lo, 1e-9)
    if span.min() < global_nn * args.planar_min_split_width_scale:
        return []

    local_pts = (pts - center) @ axes if len(pts) else np.empty((0, 2))
    shape_ratio = float(span.max() / max(span.min(), 1e-9))
    major_axis = int(np.argmax(span))
    minor_axis = 1 - major_axis
    use_axis_bias = (
        parcel.depth < args.planar_axis_split_depth
        and len(parcel.point_idx) >= args.planar_axis_split_min_worlds
        and shape_ratio >= args.planar_axis_split_min_aspect
    )
    if use_axis_bias:
        order = [minor_axis, major_axis]
    elif shape_ratio >= args.planar_force_crosscut_ratio:
        order = [int(np.argmax(span))]
    else:
        order = [parcel.depth % 2, 1 - (parcel.depth % 2)]
        if shape_ratio >= args.recursive_alternate_ratio:
            order = [int(np.argmax(span)), int(np.argmin(span))]
    quantiles = [0.5, 0.44, 0.56, 0.38, 0.62, 0.32, 0.68]
    quantiles = quantiles[: args.planar_split_candidates_per_family]
    candidates: list[SplitLineCandidate] = _density_isoline_split_candidates(
        parcel,
        density_guide=density_guide,
        global_nn=global_nn,
        args=args,
    )
    for normal_axis in order[: args.planar_split_families]:
        tangent_axis = 1 - normal_axis
        tangent_pad = span[tangent_axis] * 0.42 + global_nn * 10.0
        t0 = lo[tangent_axis] - tangent_pad
        t1 = hi[tangent_axis] + tangent_pad
        n = int(
            np.clip(
                math.ceil((t1 - t0) / max(global_nn * 2.4, span[tangent_axis] / 160)),
                32,
                args.road_curve_max_vertices,
            )
        )
        tangent = np.linspace(t0, t1, n)
        margin = max(
            span[normal_axis] * args.planar_split_margin_frac,
            global_nn * args.planar_min_split_margin_scale,
        )
        if margin * 2 >= span[normal_axis]:
            continue
        if len(local_pts):
            split_values = np.quantile(local_pts[:, normal_axis], quantiles)
        else:
            split_values = np.full(
                len(quantiles),
                (lo[normal_axis] + hi[normal_axis]) / 2,
            )
        for cand_no, split_at in enumerate(split_values.tolist()):
            split_at = float(
                np.clip(
                    split_at,
                    lo[normal_axis] + margin,
                    hi[normal_axis] - margin,
                )
            )
            amp = min(
                span[normal_axis] * args.planar_streamline_curve_span_scale,
                global_nn * args.planar_streamline_curve_global_scale,
            )
            wavelength = max(
                span[tangent_axis] * args.planar_streamline_wavelength_scale,
                global_nn * 3.0,
            )
            phase = rng.uniform(0.0, math.tau)
            phase2 = rng.uniform(0.0, math.tau)
            curve = amp * np.sin((tangent / wavelength) * math.tau + phase)
            curve += (
                amp
                * args.planar_streamline_secondary_curve_scale
                * np.sin((tangent / (wavelength * 2.6)) * math.tau + phase2)
            )
            if cand_no == 0:
                curve *= 0.25
            local = np.zeros((n, 2), dtype=np.float64)
            local[:, tangent_axis] = tangent
            local[:, normal_axis] = split_at + curve
            line = LineString(_to_world(local, center, axes))
            if line.length > global_nn:
                candidates.append(
                    SplitLineCandidate(
                        line=line,
                        normal_axis=int(normal_axis),
                        tangent_axis=int(tangent_axis),
                        major_axis=major_axis,
                        shape_ratio=shape_ratio,
                        tangent_span=float(span[tangent_axis]),
                    )
                )
    return candidates


def _try_split_planar_parcel(
    parcel: SplitParcel,
    xy: np.ndarray,
    *,
    density_guide: DensityGuide | None,
    global_nn: float,
    island_id: int,
    slot_step: float,
    target_leaf_area: float,
    args,
    rng: np.random.Generator,
) -> tuple[list[SplitParcel], RoadSpec] | None:
    if parcel.depth >= args.recursive_max_depth:
        return None
    area_split = parcel.geom.area > target_leaf_area * args.planar_area_split_scale
    min_worlds = (
        args.planar_area_split_min_worlds
        if area_split
        else args.recursive_min_split_worlds
    )
    if len(parcel.point_idx) < min_worlds:
        return None
    min_area = (global_nn * global_nn) * args.planar_min_parcel_area_scale
    best: tuple[float, list[Polygon], list[np.ndarray], LineString] | None = None
    for cand in _planar_split_candidate_lines(
        parcel,
        xy,
        density_guide=density_guide,
        global_nn=global_nn,
        args=args,
        rng=rng,
    ):
        line = cand.line
        if line.length <= slot_step * args.planar_min_split_road_length_slots:
            continue
        try:
            result = split_geom(parcel.geom, line)
        except (ValueError, shapely.GEOSException):
            continue
        parts = [
            p
            for p in sorted(iter_polygons(result), key=lambda g: g.area, reverse=True)
            if p.area >= min_area
        ]
        if len(parts) != 2:
            continue
        area_sum = sum(p.area for p in parts)
        if area_sum < parcel.geom.area * 0.92:
            continue
        child_members = _classify_points_to_polys(parts, xy, parcel.point_idx)
        counts = np.array([len(m) for m in child_members], dtype=np.int64)
        if np.count_nonzero(counts) < 2:
            continue
        if (
            counts.max(initial=0)
            >= len(parcel.point_idx) * args.planar_max_child_frac
        ):
            continue
        areas = np.array([p.area for p in parts], dtype=np.float64)
        area_balance = float(areas.min() / max(areas.max(), 1e-12))
        count_balance = float(counts.min() / max(counts.max(), 1))
        regularity = 1.0 / (1.0 + sum(_polygon_irregularity(p) for p in parts))
        max_aspect = max(_polygon_aspect_ratio(p) for p in parts)
        aspect_score = 1.0 / (
            1.0
            + max(
                0.0,
                math.log(max_aspect / max(args.planar_target_child_aspect, 1e-9)),
            )
        )
        split_geom_line = line.intersection(parcel.geom)
        split_len = sum(seg.length for seg in _iter_lines(split_geom_line))
        length_score = math.sqrt(float(parcel.geom.area)) / max(split_len, 1e-9)
        axis_score = 0.0
        if (
            parcel.depth < args.planar_axis_split_depth
            and len(parcel.point_idx) >= args.planar_axis_split_min_worlds
            and cand.shape_ratio >= args.planar_axis_split_min_aspect
            and cand.tangent_axis == cand.major_axis
        ):
            through_score = min(split_len / max(cand.tangent_span, 1e-9), 1.0)
            axis_score = through_score * math.sqrt(area_balance * count_balance)
        score = (
            args.planar_quality_size_weight * area_balance
            + args.planar_quality_count_weight * count_balance
            + args.planar_quality_regular_weight * regularity
            + args.planar_quality_aspect_weight * aspect_score
            + args.planar_quality_length_weight * min(length_score, 1.0)
            + args.planar_quality_axis_weight * axis_score
        )
        if best is None or score > best[0]:
            best = (score, parts, child_members, split_geom_line)
    if best is None:
        return None
    _score, parts, child_members, road_geom = best
    road_lines = [
        seg
        for seg in _iter_lines(road_geom)
        if seg.length >= slot_step * args.planar_min_split_road_length_slots
    ]
    if not road_lines:
        return None
    road_line = max(road_lines, key=lambda seg: seg.length)
    kind, width = _split_road_kind(parcel.depth, args)
    children = [
        SplitParcel(geom=part, point_idx=members, depth=parcel.depth + 1)
        for part, members in zip(parts, child_members, strict=True)
        if len(members) > 0
    ]
    if len(children) < 2:
        return None
    spec = RoadSpec(
        coords=[(float(x), float(y)) for x, y in road_line.coords],
        kind=kind,
        island_id=island_id,
        width=width,
        family=parcel.depth,
        depth=parcel.depth,
    )
    return children, spec


def _choose_split_leaf(
    leaves: list[SplitParcel],
    *,
    global_nn: float,
    target_leaf_worlds: int,
    target_leaf_area: float,
    failed: set[int],
    args,
) -> list[int]:
    scored: list[tuple[float, int]] = []
    for i, leaf in enumerate(leaves):
        if i in failed:
            continue
        n = len(leaf.point_idx)
        area = float(leaf.geom.area)
        area_excess = max(0.0, area / max(target_leaf_area, 1e-12) - 1.0)
        area_split = area_excess > args.planar_area_split_scale - 1.0
        min_worlds = (
            args.planar_area_split_min_worlds
            if area_split
            else args.recursive_min_split_worlds
        )
        if n < min_worlds:
            continue
        excess = max(0.0, n - target_leaf_worlds)
        if excess <= 0 and area_excess <= 0 and leaf.depth > 0:
            continue
        density = n / max(float(leaf.geom.area), global_nn * global_nn)
        irregularity = _polygon_irregularity(leaf.geom)
        scored.append(
            (
                excess * 8.0
                + area_excess * args.planar_area_split_score_weight
                + density * global_nn
                + irregularity,
                i,
            )
        )
    scored.sort(reverse=True)
    return [i for _score, i in scored[: args.recursive_split_search]]


def _planar_junction_snap_tol(spec: RoadSpec, global_nn: float, args) -> float:
    if spec.kind in {"arterial", "collector"}:
        return global_nn * args.planar_junction_snap_scale
    if spec.kind == "local":
        return global_nn * args.planar_local_junction_snap_scale
    return global_nn * args.planar_service_junction_snap_scale


def _planar_junction_merge_tol(spec: RoadSpec, global_nn: float, args) -> float:
    if spec.kind in {"arterial", "collector"}:
        return global_nn * args.planar_junction_merge_scale
    if spec.kind == "local":
        return global_nn * args.planar_local_junction_merge_scale
    return global_nn * args.planar_service_junction_merge_scale


def _planar_junction_max_turn_deg(spec: RoadSpec, args) -> float:
    if spec.kind in {"arterial", "collector"}:
        return args.planar_major_junction_max_turn_deg
    if spec.kind == "local":
        return args.planar_local_junction_max_turn_deg
    return args.planar_service_junction_max_turn_deg


def _endpoint_snap_turn_ok(
    coords: np.ndarray,
    end_idx: int,
    target: np.ndarray,
    *,
    max_turn_deg: float,
) -> bool:
    if len(coords) < 2 or max_turn_deg <= 0:
        return True
    endpoint = coords[end_idx]
    neighbor = coords[1] if end_idx == 0 else coords[-2]
    old_vec = endpoint - neighbor
    new_vec = target - neighbor
    old_len = float(np.linalg.norm(old_vec))
    new_len = float(np.linalg.norm(new_vec))
    if old_len <= 1e-9 or new_len <= 1e-9:
        return True
    cos = float(np.dot(old_vec, new_vec) / (old_len * new_len))
    turn = math.degrees(math.acos(float(np.clip(cos, -1.0, 1.0))))
    return turn <= max_turn_deg


def _snap_planar_junctions(
    specs: list[RoadSpec],
    *,
    global_nn: float,
    args,
) -> list[RoadSpec]:
    if (
        len(specs) < 2
        or args.planar_junction_snap_scale <= 0
        or args.planar_junction_merge_scale <= 0
    ):
        return specs
    lines = [LineString(spec.coords) for spec in specs]
    tree = shapely.STRtree(lines)
    snap_tols = np.asarray(
        [_planar_junction_snap_tol(spec, global_nn, args) for spec in specs],
        dtype=np.float64,
    )
    merge_tols = np.asarray(
        [_planar_junction_merge_tol(spec, global_nn, args) for spec in specs],
        dtype=np.float64,
    )
    proposals: list[tuple[int, int, np.ndarray, float]] = []
    for i, line in enumerate(lines):
        coords = np.asarray(line.coords, dtype=np.float64)
        if len(coords) < 2:
            continue
        closed = np.linalg.norm(coords[0] - coords[-1]) <= 1e-9
        if closed:
            continue
        snap_tol = float(snap_tols[i])
        if snap_tol <= 0:
            continue
        for end_idx in (0, len(coords) - 1):
            endpoint = coords[end_idx]
            p = shapely.Point(float(endpoint[0]), float(endpoint[1]))
            cand = np.asarray(tree.query(p.buffer(snap_tol).envelope), dtype=np.int64)
            best_q = None
            best_d = float("inf")
            for j in cand.tolist():
                if j == i:
                    continue
                q = _nearest_point_on_geom(endpoint, lines[j])
                if q is None:
                    continue
                d = float(np.linalg.norm(endpoint - q))
                if (
                    d <= snap_tol
                    and d < best_d
                    and _endpoint_snap_turn_ok(
                        coords,
                        end_idx,
                        q,
                        max_turn_deg=_planar_junction_max_turn_deg(specs[i], args),
                    )
                ):
                    best_q = q
                    best_d = d
            if best_q is not None:
                proposals.append((i, end_idx, best_q, float(merge_tols[i])))
    if not proposals:
        return specs

    pts = np.stack([p for _i, _end, p, _merge in proposals])
    proposal_merge_tols = np.asarray([m for *_rest, m in proposals], dtype=np.float64)
    parent = np.arange(len(proposals), dtype=np.int64)

    def find(v: int) -> int:
        while parent[v] != v:
            parent[v] = parent[parent[v]]
            v = int(parent[v])
        return v

    def union(a: int, b: int) -> None:
        ra = find(a)
        rb = find(b)
        if ra != rb:
            parent[rb] = ra

    max_merge_tol = float(proposal_merge_tols.max(initial=0.0))
    nbrs = cKDTree(pts).query_pairs(max_merge_tol)
    for a, b in nbrs:
        if (
            float(np.linalg.norm(pts[int(a)] - pts[int(b)]))
            <= min(
                float(proposal_merge_tols[int(a)]),
                float(proposal_merge_tols[int(b)]),
            )
        ):
            union(int(a), int(b))
    clusters: dict[int, list[int]] = defaultdict(list)
    for i in range(len(proposals)):
        clusters[find(i)].append(i)

    line_union = shapely.unary_union(lines)
    snap_points: dict[tuple[int, int], np.ndarray] = {}
    for members in clusters.values():
        center = pts[members].mean(axis=0)
        snapped = _nearest_point_on_geom(center, line_union)
        if snapped is None:
            snapped = center
        for member in members:
            spec_idx, end_idx, _q, _merge = proposals[member]
            snap_points[(spec_idx, end_idx)] = snapped

    out: list[RoadSpec] = []
    for i, spec in enumerate(specs):
        coords = np.asarray(spec.coords, dtype=np.float64).copy()
        max_move = float(snap_tols[i] + merge_tols[i])
        for end_idx in (0, len(coords) - 1):
            q = snap_points.get((i, end_idx))
            if q is None:
                continue
            if float(np.linalg.norm(coords[end_idx] - q)) <= max_move and (
                _endpoint_snap_turn_ok(
                    coords,
                    end_idx,
                    q,
                    max_turn_deg=_planar_junction_max_turn_deg(spec, args),
                )
            ):
                coords[end_idx] = q
        out.append(
            RoadSpec(
                coords=[(float(x), float(y)) for x, y in coords],
                kind=spec.kind,
                island_id=spec.island_id,
                width=spec.width,
                family=spec.family,
                depth=spec.depth,
            )
        )
    return out


def _node_planar_road_specs_once(
    specs: list[RoadSpec],
    *,
    island_id: int,
    global_nn: float,
    args,
) -> list[RoadSpec]:
    source_lines = [LineString(spec.coords) for spec in specs]
    source_lines = [line for line in source_lines if line.length > 1e-9]
    if not source_lines:
        return []
    noded = shapely.unary_union(source_lines)
    segments = [seg for seg in _iter_lines(noded) if seg.length > 1e-9]
    if not segments:
        return []
    tree = shapely.STRtree(source_lines)
    priority = {"arterial": 0, "collector": 1, "local": 2, "service": 3, "slot": 4}
    tol = max(global_nn * args.planar_source_match_tolerance_scale, 1e-9)
    min_len = max(global_nn * args.planar_min_noded_segment_scale, 1e-9)
    out: list[RoadSpec] = []
    for seg in segments:
        if seg.length < min_len:
            continue
        query_geom = seg.buffer(tol, cap_style="flat", join_style="round").envelope
        candidates = np.asarray(tree.query(query_geom), dtype=np.int64)
        best_idx = -1
        best_rank = 999
        best_overlap = -1.0
        for idx in candidates.tolist():
            src = source_lines[idx]
            if seg.distance(src) > tol:
                continue
            spec = specs[idx]
            rank = priority.get(spec.kind, 99)
            overlap = seg.intersection(src.buffer(tol, cap_style="flat")).length
            if rank < best_rank or (rank == best_rank and overlap > best_overlap):
                best_idx = idx
                best_rank = rank
                best_overlap = overlap
        if best_idx < 0:
            best_idx = 0
        spec = specs[best_idx]
        out.append(
            RoadSpec(
                coords=[(float(x), float(y)) for x, y in seg.coords],
                kind=spec.kind,
                island_id=island_id,
                width=spec.width,
                family=spec.family,
                depth=spec.depth,
            )
        )
    return out


def _endpoint_node_key(p: np.ndarray, tol: float = 1e-8) -> tuple[int, int]:
    return (int(round(float(p[0]) / tol)), int(round(float(p[1]) / tol)))


def _repair_planar_road_topology(
    specs: list[RoadSpec],
    *,
    global_nn: float,
    args,
) -> tuple[list[RoadSpec], dict[str, int]]:
    if len(specs) < 2 or args.planar_topology_merge_scale <= 0:
        return specs, {"topology_junction_merges": 0, "topology_short_edge_merges": 0}
    endpoints: list[tuple[int, int, np.ndarray]] = []
    node_degree: dict[tuple[int, int], set[int]] = defaultdict(set)
    for i, spec in enumerate(specs):
        coords = np.asarray(spec.coords, dtype=np.float64)
        if len(coords) < 2:
            continue
        closed = float(np.linalg.norm(coords[0] - coords[-1])) <= 1e-9
        if closed:
            continue
        for end_idx in (0, len(coords) - 1):
            p = coords[end_idx]
            node_degree[_endpoint_node_key(p)].add(i)
            endpoints.append((i, end_idx, p))
    if len(endpoints) < 2:
        return specs, {"topology_junction_merges": 0, "topology_short_edge_merges": 0}

    pts = np.stack([p for _i, _end, p in endpoints])
    merge_tol = max(global_nn * args.planar_topology_merge_scale, 1e-9)
    short_edge_tol = max(global_nn * args.planar_topology_short_edge_scale, 0.0)
    parent = np.arange(len(endpoints), dtype=np.int64)

    def find(v: int) -> int:
        while parent[v] != v:
            parent[v] = parent[parent[v]]
            v = int(parent[v])
        return v

    def union(a: int, b: int) -> None:
        ra = find(a)
        rb = find(b)
        if ra != rb:
            parent[rb] = ra

    tree = cKDTree(pts)
    for a, b in tree.query_pairs(merge_tol):
        union(int(a), int(b))

    by_spec_end = {
        (spec_idx, end_idx): endpoint_no
        for endpoint_no, (spec_idx, end_idx, _p) in enumerate(endpoints)
    }
    short_edge_merges = 0
    for spec_idx, spec in enumerate(specs):
        line = LineString(spec.coords)
        if line.length > short_edge_tol or spec.kind == "arterial":
            continue
        coords = np.asarray(spec.coords, dtype=np.float64)
        if len(coords) < 2:
            continue
        a_key = _endpoint_node_key(coords[0])
        b_key = _endpoint_node_key(coords[-1])
        if (
            len(node_degree.get(a_key, set())) < 2
            or len(node_degree.get(b_key, set())) < 2
        ):
            continue
        a = by_spec_end.get((spec_idx, 0))
        b = by_spec_end.get((spec_idx, len(coords) - 1))
        if a is None or b is None:
            continue
        union(a, b)
        short_edge_merges += 1

    clusters: dict[int, list[int]] = defaultdict(list)
    for i in range(len(endpoints)):
        clusters[find(i)].append(i)
    snap_points: dict[tuple[int, int], np.ndarray] = {}
    junction_merges = 0
    for members in clusters.values():
        if len(members) < 2:
            continue
        center = pts[members].mean(axis=0)
        junction_merges += len(members) - 1
        for member in members:
            spec_idx, end_idx, _p = endpoints[member]
            snap_points[(spec_idx, end_idx)] = center
    if not snap_points:
        return specs, {
            "topology_junction_merges": 0,
            "topology_short_edge_merges": short_edge_merges,
        }

    out: list[RoadSpec] = []
    max_move = merge_tol * args.planar_topology_max_move_scale
    for i, spec in enumerate(specs):
        coords = np.asarray(spec.coords, dtype=np.float64).copy()
        for end_idx in (0, len(coords) - 1):
            q = snap_points.get((i, end_idx))
            if q is None:
                continue
            if float(np.linalg.norm(coords[end_idx] - q)) > max_move:
                continue
            if not _endpoint_snap_turn_ok(
                coords,
                end_idx,
                q,
                max_turn_deg=args.planar_topology_endpoint_max_turn_deg,
            ):
                continue
            coords[end_idx] = q
        if LineString(coords).length <= 1e-9:
            continue
        out.append(
            RoadSpec(
                coords=[(float(x), float(y)) for x, y in coords],
                kind=spec.kind,
                island_id=spec.island_id,
                width=spec.width,
                family=spec.family,
                depth=spec.depth,
            )
        )
    return out, {
        "topology_junction_merges": int(junction_merges),
        "topology_short_edge_merges": int(short_edge_merges),
    }


def _node_planar_road_specs(
    specs: list[RoadSpec],
    *,
    island_id: int,
    global_nn: float,
    args,
) -> tuple[list[RoadSpec], dict[str, int]]:
    noded = _node_planar_road_specs_once(
        specs,
        island_id=island_id,
        global_nn=global_nn,
        args=args,
    )
    repaired, metrics = _repair_planar_road_topology(
        noded,
        global_nn=global_nn,
        args=args,
    )
    if repaired is noded:
        return noded, metrics
    noded = _node_planar_road_specs_once(
        repaired,
        island_id=island_id,
        global_nn=global_nn,
        args=args,
    )
    return noded, metrics


def _generate_planar_streets(
    *,
    xy: np.ndarray,
    land_geom,
    island_id: int,
    global_nn: float,
    slot_step: float,
    args,
) -> PlanarStreetResult:
    min_area = (global_nn * global_nn) * args.planar_min_parcel_area_scale
    coastal_offset = max(global_nn * args.coastal_road_offset_scale, slot_step * 1.4)
    parts = _planning_parts(
        land_geom,
        coastal_offset=coastal_offset,
        min_area=min_area,
        args=args,
    )
    if not parts:
        parts = sorted(iter_polygons(land_geom), key=lambda p: p.area, reverse=True)
    build_geom = _safe_geom(shapely.unary_union(parts))
    members_by_part = _classify_points_to_polys(
        parts,
        xy,
        np.arange(len(xy), dtype=np.int64),
    )
    leaves = [
        SplitParcel(geom=p, point_idx=m, depth=0)
        for p, m in zip(parts, members_by_part, strict=True)
        if len(m) > 0
    ]
    road_specs = _planar_boundary_road_specs(
        parts,
        members_by_part=members_by_part,
        island_id=island_id,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
    )
    density_guide = _density_guide_for_points(xy, global_nn=global_nn, args=args)
    rng = np.random.default_rng(args.seed + island_id * 7919)
    target_leaf_worlds = max(args.planar_target_block_worlds, 6)
    target_leaf_area = (
        global_nn
        * global_nn
        * max(args.planar_target_block_area_scale, 1.0)
    )
    splits = 0
    failed: set[int] = set()
    while splits < args.recursive_max_splits and leaves:
        if (
            max(len(leaf.point_idx) for leaf in leaves) <= target_leaf_worlds * 1.20
            and max(float(leaf.geom.area) for leaf in leaves)
            <= target_leaf_area * args.planar_area_stop_scale
        ):
            break
        progressed = False
        for i in _choose_split_leaf(
            leaves,
            global_nn=global_nn,
            target_leaf_worlds=target_leaf_worlds,
            target_leaf_area=target_leaf_area,
            failed=failed,
            args=args,
        ):
            split = _try_split_planar_parcel(
                leaves[i],
                xy,
                density_guide=density_guide,
                global_nn=global_nn,
                island_id=island_id,
                slot_step=slot_step,
                target_leaf_area=target_leaf_area,
                args=args,
                rng=rng,
            )
            if split is None:
                failed.add(i)
                continue
            children, spec = split
            leaves = leaves[:i] + children + leaves[i + 1 :]
            road_specs.append(spec)
            failed.clear()
            splits += 1
            progressed = True
            break
        if not progressed:
            break
    road_specs.extend(
        _leaf_access_lane_specs(
            leaves,
            xy=xy,
            island_id=island_id,
            global_nn=global_nn,
            slot_step=slot_step,
            args=args,
        )
    )
    road_specs = _snap_planar_junctions(
        road_specs,
        global_nn=global_nn,
        args=args,
    )
    road_specs, topology_metrics = _node_planar_road_specs(
        road_specs,
        island_id=island_id,
        global_nn=global_nn,
        args=args,
    )
    return PlanarStreetResult(
        road_specs=road_specs,
        leaves=leaves,
        build_geom=build_geom,
        splits=float(splits),
        metrics={
            **topology_metrics,
            "density_centers": (
                0
                if density_guide is None
                else int(len(density_guide.centers))
            ),
            "target_leaf_area": float(target_leaf_area),
        },
    )


def _project_poly_bounds(
    poly: Polygon,
    center: np.ndarray,
    axes: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    coords = np.asarray(poly.exterior.coords, dtype=np.float64)
    local = (coords - center) @ axes
    return local.min(axis=0), local.max(axis=0)


def _split_candidate_line(
    parcel: SplitParcel,
    xy: np.ndarray,
    *,
    global_nn: float,
    quantile: float,
    args,
    rng: np.random.Generator,
) -> LineString | None:
    pts = xy[parcel.point_idx]
    center, axes = _polygon_axes(parcel.geom, pts)
    lo, hi = _project_poly_bounds(parcel.geom, center, axes)
    span = np.maximum(hi - lo, 1e-9)
    if span.min() < global_nn * args.recursive_min_split_width_scale:
        return None
    ratio = float(span.max() / max(span.min(), 1e-9))
    normal_axis = int(np.argmax(span))
    if ratio < args.recursive_alternate_ratio:
        normal_axis = parcel.depth % 2
    tangent_axis = 1 - normal_axis
    if len(pts) >= 3:
        local_pts = (pts - center) @ axes
        split_at = float(np.quantile(local_pts[:, normal_axis], quantile))
    else:
        split_at = float((lo[normal_axis] + hi[normal_axis]) / 2)
    margin = max(
        span[normal_axis] * args.recursive_split_margin_frac,
        global_nn * args.recursive_min_split_margin_scale,
    )
    if margin * 2 >= span[normal_axis]:
        return None
    split_at = float(
        np.clip(split_at, lo[normal_axis] + margin, hi[normal_axis] - margin)
    )
    tangent_pad = span[tangent_axis] * 0.35 + global_nn * 8
    t0 = lo[tangent_axis] - tangent_pad
    t1 = hi[tangent_axis] + tangent_pad
    n = int(
        np.clip(
            math.ceil((t1 - t0) / max(global_nn * 3.5, span[tangent_axis] / 96)),
            24,
            args.road_curve_max_vertices,
        )
    )
    tangent = np.linspace(t0, t1, n)
    amp = min(
        span[normal_axis] * args.recursive_curve_span_scale,
        global_nn * args.recursive_curve_global_scale,
    )
    phase = rng.uniform(0.0, math.tau)
    wavelength = max(
        span[tangent_axis] * args.recursive_curve_wavelength_scale,
        global_nn,
    )
    curve = amp * np.sin((tangent / wavelength) * math.tau + phase)
    local = np.zeros((n, 2), dtype=np.float64)
    local[:, tangent_axis] = tangent
    local[:, normal_axis] = split_at + curve
    return LineString(_to_world(local, center, axes))


def _try_split_parcel(
    parcel: SplitParcel,
    xy: np.ndarray,
    *,
    global_nn: float,
    island_id: int,
    slot_step: float,
    args,
    rng: np.random.Generator,
) -> tuple[list[SplitParcel], list[RoadSpec]] | None:
    if parcel.depth >= args.recursive_max_depth:
        return None
    if len(parcel.point_idx) < args.recursive_min_split_worlds:
        return None
    min_area = (global_nn * global_nn) * args.recursive_min_parcel_area_scale
    quantiles = [0.5, 0.46, 0.54, 0.40, 0.60, 0.35, 0.65]
    for q in quantiles[: args.recursive_split_attempts]:
        line = _split_candidate_line(
            parcel,
            xy,
            global_nn=global_nn,
            quantile=q,
            args=args,
            rng=rng,
        )
        if line is None or line.length <= slot_step * args.min_road_length_slots:
            continue
        try:
            result = split_geom(parcel.geom, line)
        except (ValueError, shapely.GEOSException):
            continue
        parts = [
            p
            for p in sorted(iter_polygons(result), key=lambda g: g.area, reverse=True)
            if p.area >= min_area
        ]
        if len(parts) < 2:
            continue
        area_sum = sum(p.area for p in parts)
        if area_sum < parcel.geom.area * 0.86:
            continue
        child_members = _classify_points_to_polys(parts, xy, parcel.point_idx)
        counts = np.array([len(m) for m in child_members], dtype=np.int64)
        if (
            counts.max(initial=0)
            >= len(parcel.point_idx) * args.recursive_max_child_frac
        ):
            continue
        if np.count_nonzero(counts) < 2:
            continue
        road_geom = line.intersection(parcel.geom)
        road_specs = _line_specs_from_geom(
            road_geom,
            island_id=island_id,
            kind=(
                "collector"
                if parcel.depth < args.recursive_collector_depth
                else "local"
            ),
            width=(
                args.collector_road_width
                if parcel.depth < args.recursive_collector_depth
                else args.local_road_width
            ),
            family=parcel.depth,
            min_length=slot_step * args.min_road_length_slots,
            depth=parcel.depth,
        )
        if not road_specs:
            continue
        children = [
            SplitParcel(geom=part, point_idx=members, depth=parcel.depth + 1)
            for part, members in zip(parts, child_members, strict=True)
            if len(members) > 0
        ]
        if len(children) < 2:
            continue
        return children, road_specs
    return None


def _generate_recursive_streets(
    *,
    xy: np.ndarray,
    land_geom,
    island_id: int,
    global_nn: float,
    slot_step: float,
    args,
) -> tuple[list[RoadSpec], list[SplitParcel], float]:
    parts = sorted(iter_polygons(land_geom), key=lambda p: p.area, reverse=True)
    members_by_part = _classify_points_to_polys(
        parts,
        xy,
        np.arange(len(xy), dtype=np.int64),
    )
    leaves = [
        SplitParcel(geom=p, point_idx=m, depth=0)
        for p, m in zip(parts, members_by_part, strict=True)
        if len(m) > 0
    ]
    coastal_specs: list[RoadSpec] = []
    planning_specs: list[RoadSpec] = []
    coastal_offset = max(global_nn * args.coastal_road_offset_scale, slot_step * 1.4)
    min_road_length = slot_step * args.min_road_length_slots
    for leaf in leaves:
        large_enough_for_ring = (
            len(leaf.point_idx) >= args.coastal_arterial_min_worlds
            and leaf.geom.area
            >= (global_nn * global_nn) * args.coastal_arterial_min_area_scale
        )
        if large_enough_for_ring:
            coastal_specs.extend(
                _coastal_road_specs(
                    leaf.geom,
                    island_id=island_id,
                    kind="arterial",
                    width=args.arterial_road_width,
                    offset=coastal_offset,
                    min_length=min_road_length,
                    args=args,
                )
            )
    coastal_specs = _postprocess_road_specs(
        coastal_specs,
        land_geom=land_geom,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
    )
    planning_specs.extend(coastal_specs)

    rng = np.random.default_rng(args.seed + island_id * 7919)
    target_slots = len(xy) * args.slot_capacity_target
    target_leaf_worlds = max(args.recursive_target_block_worlds, 8)
    splits = 0
    failed: set[int] = set()
    while splits < args.recursive_max_splits and leaves:
        road_len = sum(LineString(s.coords).length for s in planning_specs)
        capacity = (
            road_len * 2.0 / max(slot_step, 1e-9)
        ) * args.recursive_capacity_efficiency
        max_leaf = max(len(leaf.point_idx) for leaf in leaves)
        if capacity >= target_slots and max_leaf <= target_leaf_worlds * 1.35:
            break
        scored = []
        for i, leaf in enumerate(leaves):
            if i in failed:
                continue
            n = len(leaf.point_idx)
            if n < args.recursive_min_split_worlds:
                continue
            perimeter_capacity = (
                leaf.geom.length / max(slot_step, 1e-9)
            ) * args.recursive_capacity_efficiency
            excess = max(0.0, n - target_leaf_worlds)
            frontage_need = max(0.0, n - perimeter_capacity)
            density = n / max(float(leaf.geom.area), global_nn * global_nn)
            scored.append((excess * 5.0 + frontage_need + density * global_nn, i))
        if not scored:
            break
        scored.sort(reverse=True)
        progressed = False
        for _score, i in scored[: args.recursive_split_search]:
            split = _try_split_parcel(
                leaves[i],
                xy,
                global_nn=global_nn,
                island_id=island_id,
                slot_step=slot_step,
                args=args,
                rng=rng,
            )
            if split is None:
                failed.add(i)
                continue
            children, new_roads = split
            planning_specs.extend(new_roads)
            leaves = leaves[:i] + children + leaves[i + 1 :]
            failed.clear()
            splits += 1
            progressed = True
            break
        if not progressed:
            break

    collector_specs = [
        spec
        for spec in planning_specs
        if spec.kind == "collector" and spec.depth < args.recursive_collector_depth
    ]
    local_split_specs = [spec for spec in planning_specs if spec.kind == "local"]
    boundary = shapely.unary_union([leaf.geom.boundary for leaf in leaves])
    boundary_slot_specs = _line_specs_from_geom(
        boundary,
        island_id=island_id,
        kind="slot",
        width=args.local_road_width,
        family=-3,
        min_length=slot_step * args.boundary_road_min_length_slots,
    )
    access_specs = _leaf_access_lane_specs(
        leaves,
        xy=xy,
        island_id=island_id,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
    )
    road_specs = coastal_specs + _postprocess_road_specs(
        collector_specs + local_split_specs + boundary_slot_specs + access_specs,
        land_geom=land_geom,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
    )
    road_specs.extend(
        _endpoint_connector_specs(
            road_specs,
            land_geom=land_geom,
            island_id=island_id,
            global_nn=global_nn,
            args=args,
        )
    )
    return road_specs, leaves, float(splits)


def _leaf_access_lane_specs(
    leaves: list[SplitParcel],
    *,
    xy: np.ndarray,
    island_id: int,
    global_nn: float,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    specs: list[RoadSpec] = []
    target = max(args.recursive_target_block_worlds, 8)
    min_length = slot_step * args.boundary_road_min_length_slots
    rng = np.random.default_rng(args.seed + island_id * 104729)
    for leaf_no, leaf in enumerate(leaves):
        n_worlds = len(leaf.point_idx)
        if n_worlds <= target * args.access_lane_trigger_scale:
            continue
        density_norm = (
            n_worlds * global_nn * global_nn / max(float(leaf.geom.area), 1e-12)
        )
        if density_norm < args.access_lane_density_scale:
            continue
        pts = xy[leaf.point_idx]
        center, axes = _polygon_axes(leaf.geom, pts)
        lo, hi = _project_poly_bounds(leaf.geom, center, axes)
        span = np.maximum(hi - lo, 1e-9)
        tangent_axis = int(np.argmax(span))
        cross_axis = 1 - tangent_axis
        if span[cross_axis] < global_nn * args.recursive_min_split_width_scale:
            continue
        lane_count = min(
            args.access_lane_max_per_block,
            max(1, math.ceil(n_worlds / target) - 1),
        )
        local_pts = (pts - center) @ axes
        qs = np.linspace(0.0, 1.0, lane_count + 2)[1:-1]
        offsets = np.quantile(local_pts[:, cross_axis], qs)
        margin = max(
            span[cross_axis] * args.access_lane_margin_frac,
            global_nn * args.recursive_min_split_margin_scale,
        )
        available = float(span[cross_axis] - margin * 2)
        min_lane_spacing = max(
            global_nn * args.access_lane_min_spacing_scale,
            slot_step * args.access_lane_min_spacing_slot_scale,
        )
        if available <= 0:
            continue
        max_lanes_by_spacing = max(1, int(math.floor(available / min_lane_spacing)) - 1)
        if lane_count > max_lanes_by_spacing:
            lane_count = max_lanes_by_spacing
            qs = np.linspace(0.0, 1.0, lane_count + 2)[1:-1]
            offsets = np.quantile(local_pts[:, cross_axis], qs)
        lo_cross = lo[cross_axis] + margin
        hi_cross = hi[cross_axis] - margin
        even_offsets = np.linspace(lo_cross, hi_cross, lane_count + 2)[1:-1]
        offsets = np.clip(offsets, lo_cross, hi_cross)
        offsets = np.sort(offsets * 0.55 + even_offsets * 0.45)
        spaced_offsets = []
        for offset in offsets.tolist():
            if not spaced_offsets or offset - spaced_offsets[-1] >= min_lane_spacing:
                spaced_offsets.append(offset)
        if len(spaced_offsets) < lane_count:
            spaced_offsets = even_offsets.tolist()
        offsets = np.asarray(spaced_offsets, dtype=np.float64)
        tangent_pad = span[tangent_axis] * 0.25 + global_nn * 6
        t = np.linspace(
            lo[tangent_axis] - tangent_pad,
            hi[tangent_axis] + tangent_pad,
            args.access_lane_curve_vertices,
        )
        kind = (
            "local"
            if density_norm >= args.access_lane_local_density_scale
            else "service"
        )
        width = args.local_road_width if kind == "local" else args.service_road_width
        for lane_no, offset in enumerate(offsets.tolist()):
            local = np.zeros((len(t), 2), dtype=np.float64)
            local[:, tangent_axis] = t
            amp = min(
                span[cross_axis] * args.access_lane_curve_span_scale,
                global_nn * args.access_lane_curve_global_scale,
            )
            wavelength = max(
                span[tangent_axis] * args.access_lane_curve_wavelength_scale,
                global_nn,
            )
            phase = rng.uniform(0.0, math.tau)
            curve = amp * np.sin((t / wavelength) * math.tau + phase)
            local[:, cross_axis] = offset + curve
            line = LineString(_to_world(local, center, axes)).intersection(leaf.geom)
            specs.extend(
                _line_specs_from_geom(
                    line,
                    island_id=island_id,
                    kind=kind,
                    width=width,
                    family=leaf_no * 1000 + lane_no,
                    min_length=min_length,
                )
            )
    return specs


def _slots_for_road_specs(
    road_specs: list[RoadSpec],
    *,
    land_geom,
    demand_xy: np.ndarray,
    global_nn: float,
    slot_step: float,
    road_spacing: float,
    building_scale: float,
    island_id: int,
    args,
    min_slots: int = 0,
) -> SlotSet:
    rng = np.random.default_rng(args.seed + island_id * 3571)
    slotsets = [
        _sample_slots_for_road(
            LineString(spec.coords),
            road_index=i,
            road_spacing=road_spacing,
            slot_step=slot_step,
            global_nn=global_nn,
            building_scale=building_scale,
            args=args,
            rng=rng,
        )
        for i, spec in enumerate(road_specs)
    ]
    slots = _concat_slots(slotsets)
    slots = _density_scaled_slots(
        slots,
        demand_xy=demand_xy,
        global_nn=global_nn,
        base_building_scale=building_scale,
        island_id=island_id,
        args=args,
    )
    if len(slots.xy):
        slots = _take_slots(slots, _covers_xy(land_geom, slots.xy))
        slots = _filter_slots_for_major_corridors(
            slots,
            road_specs,
            global_nn=global_nn,
            args=args,
        )
        slots = _filter_slots_for_road_corridors(
            slots,
            road_specs,
            global_nn=global_nn,
            args=args,
        )
    covered_slots = slots
    if len(slots.xy):
        min_dist = max(
            global_nn * args.slot_filter_min_global_scale,
            float(np.median(np.maximum(slots.width, slots.depth)))
            * args.slot_filter_building_scale,
        )
        visible_priority = np.asarray(
            [road_specs[int(i)].kind != "slot" for i in slots.road_index],
            dtype=bool,
        )
        slots = _grid_filter_slots(
            slots,
            min_dist=min_dist,
            seed=args.seed + island_id * 9176,
            priority=visible_priority,
            footprint_radius_scale=args.slot_filter_footprint_radius_scale,
        )
        slots = _filter_slots_by_footprints(
            slots,
            land_geom=land_geom,
            road_specs=road_specs,
            global_nn=global_nn,
            args=args,
        )
        if (
            args.slot_filter_capacity_fallback
            and min_slots
            and len(slots.xy) < min_slots <= len(covered_slots.xy)
        ):
            return covered_slots
    return slots


def _fallback_service_slots(
    *,
    xy: np.ndarray,
    island_id: int,
    global_nn: float,
    road_index_offset: int,
    args,
) -> tuple[list[RoadSpec], SlotSet]:
    center, axes = _pca_axes(xy)
    local = (xy - center) @ axes
    order = np.argsort(local[:, 0])
    chunk = max(8, args.service_chunk_worlds)
    road_specs = []
    slotsets = []
    angle = _angle_of_axes(axes)
    size = global_nn * args.fallback_building_scale
    for chunk_no, start in enumerate(range(0, len(order), chunk)):
        members = order[start : start + chunk]
        pts = xy[members]
        if len(pts) == 1:
            a = pts[0] - axes[:, 0] * size
            b = pts[0] + axes[:, 0] * size
            coords = [(float(a[0]), float(a[1])), (float(b[0]), float(b[1]))]
        else:
            pts = pts[np.argsort((pts - center) @ axes[:, 0])]
            coords = [(float(px), float(py)) for px, py in pts]
        ridx = road_index_offset + chunk_no
        road_specs.append(
            RoadSpec(
                coords=coords,
                kind="slot",
                island_id=island_id,
                width=args.service_road_width,
                family=-1,
            )
        )
        slotsets.append(
            SlotSet(
                xy=xy[members],
                frontage=xy[members],
                angle=np.full(len(members), angle, dtype=np.float32),
                width=np.full(len(members), size, dtype=np.float32),
                depth=np.full(len(members), size, dtype=np.float32),
                road_index=np.full(len(members), ridx, dtype=np.int32),
                side=np.zeros(len(members), dtype=np.int8),
                along=np.arange(len(members), dtype=np.float32),
            )
        )
    return road_specs, _concat_slots(slotsets)


def _road_width_for_kind(kind: str, args) -> float:
    if kind == "arterial":
        return args.arterial_road_width
    if kind == "collector":
        return args.collector_road_width
    if kind == "local":
        return args.local_road_width
    return args.service_road_width


def _mesh_density_centers(
    xy: np.ndarray,
    *,
    global_nn: float,
    args,
) -> list[MeshCenter]:
    if len(xy) < max(args.mesh_density_min_worlds, 2):
        return []
    k = min(max(2, int(args.mesh_density_knn)), len(xy))
    tree = cKDTree(xy)
    dist, _idx = tree.query(xy, k=k, workers=_spatial_workers())
    radius = np.asarray(dist, dtype=np.float64)
    radius = radius if radius.ndim == 1 else radius[:, -1]
    finite = np.isfinite(radius) & (radius > 0)
    if not np.any(finite):
        return []
    threshold = float(np.quantile(radius[finite], args.mesh_density_quantile))
    candidates = np.flatnonzero(finite & (radius <= threshold))
    if len(candidates) == 0:
        return []

    min_sep = max(global_nn * args.mesh_density_center_min_sep_scale, 1e-9)
    centers_xy: list[np.ndarray] = []
    for i in candidates[np.argsort(radius[candidates], kind="stable")].tolist():
        p = xy[i]
        if any(float(np.linalg.norm(p - q)) < min_sep for q in centers_xy):
            continue
        centers_xy.append(p.astype(np.float64, copy=True))
        if len(centers_xy) >= args.mesh_density_max_centers:
            break
    if not centers_xy:
        return []

    centers_arr = np.vstack(centers_xy)
    _d, assigned = cKDTree(centers_arr).query(xy, k=1, workers=_spatial_workers())
    out: list[MeshCenter] = []
    for center_no, center_xy in enumerate(centers_arr):
        members = np.flatnonzero(np.asarray(assigned) == center_no).astype(np.int64)
        if len(members) < args.mesh_center_min_worlds:
            continue
        center, axes = _pca_axes(xy[members])
        # Keep the high-density seed as the radial field origin, but use local PCA
        # axes from its Voronoi demand catchment.
        local_dist = np.linalg.norm(xy[members] - center_xy, axis=1)
        finite_dist = local_dist[np.isfinite(local_dist)]
        if len(finite_dist) == 0:
            continue
        reach = float(np.quantile(finite_dist, args.mesh_center_radius_quantile))
        reach = max(reach, global_nn * args.mesh_city_min_radius_scale)
        out.append(
            MeshCenter(
                xy=center_xy.astype(np.float64, copy=True),
                axes=axes,
                members=members,
                radius=reach,
            )
        )
    if not out and len(xy) >= args.mesh_center_min_worlds:
        center, axes = _pca_axes(xy)
        dist = np.linalg.norm(xy - center, axis=1)
        out.append(
            MeshCenter(
                xy=center,
                axes=axes,
                members=np.arange(len(xy), dtype=np.int64),
                radius=max(
                    float(np.quantile(dist, args.mesh_center_radius_quantile)),
                    global_nn * args.mesh_city_min_radius_scale,
                ),
            )
        )
    return out


def _mesh_clip_line_specs(
    line: LineString,
    poly: Polygon,
    *,
    island_id: int,
    kind: str,
    family: int,
    min_length: float,
    args,
    depth: int = -1,
) -> list[RoadSpec]:
    clipped = line.intersection(poly)
    return _line_specs_from_geom(
        clipped,
        island_id=island_id,
        kind=kind,
        width=_road_width_for_kind(kind, args),
        family=family,
        min_length=min_length,
        depth=depth,
    )


def _mesh_ellipse_line(
    center: np.ndarray,
    axes: np.ndarray,
    rx: float,
    ry: float,
    *,
    vertices: int,
) -> LineString:
    n = max(16, int(vertices))
    t = np.linspace(0.0, math.tau, n, endpoint=False)
    local = np.column_stack([np.cos(t) * rx, np.sin(t) * ry])
    world = _to_world(local, center, axes)
    world = np.vstack([world, world[0]])
    return LineString(world)


def _mesh_ellipse_polygon(
    center: np.ndarray,
    axes: np.ndarray,
    rx: float,
    ry: float,
    *,
    vertices: int,
) -> Polygon:
    line = _mesh_ellipse_line(center, axes, rx, ry, vertices=vertices)
    return _safe_geom(Polygon(line.coords))


def _mesh_ellipse_direction_radius(rx: float, ry: float, theta: float) -> float:
    c = math.cos(theta)
    s = math.sin(theta)
    return 1.0 / math.sqrt((c / max(rx, 1e-12)) ** 2 + (s / max(ry, 1e-12)) ** 2)


def _mesh_poly_diag(poly: Polygon) -> float:
    minx, miny, maxx, maxy = poly.bounds
    return float(math.hypot(maxx - minx, maxy - miny))


def _mesh_axis_line(
    origin: np.ndarray,
    direction: np.ndarray,
    *,
    length: float,
) -> LineString:
    d = np.asarray(direction, dtype=np.float64)
    norm = float(np.linalg.norm(d))
    if norm <= 1e-12:
        d = np.array([1.0, 0.0], dtype=np.float64)
    else:
        d /= norm
    a = origin - d * length * 0.5
    b = origin + d * length * 0.5
    return LineString([(float(a[0]), float(a[1])), (float(b[0]), float(b[1]))])


def _mesh_contour_road_specs(
    poly: Polygon,
    *,
    world_count: int,
    island_id: int,
    part_no: int,
    global_nn: float,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    if (
        world_count < args.mesh_contour_min_worlds
        or poly.area < (global_nn * global_nn) * args.mesh_contour_min_area_scale
    ):
        return []
    spacing = max(
        global_nn * args.mesh_contour_spacing_scale,
        slot_step * args.mesh_contour_slot_spacing_scale,
    )
    if spacing <= 0:
        return []
    max_offset = min(
        math.sqrt(float(poly.area)) * args.mesh_contour_max_offset_frac,
        spacing * args.mesh_contour_max_rings,
    )
    offsets = np.arange(spacing * 1.15, max_offset + spacing * 0.25, spacing)
    min_length = slot_step * args.boundary_road_min_length_slots
    specs: list[RoadSpec] = []
    for ring_no, offset in enumerate(offsets.tolist()):
        inner = _safe_geom(poly.buffer(-offset, join_style="round"))
        parts = [p for p in iter_polygons(inner) if p.area > 0]
        if not parts:
            break
        kind = (
            "collector"
            if ring_no % max(args.mesh_contour_collector_every, 1) == 0
            else "local"
        )
        for inner_no, part in enumerate(parts):
            if part.area < (global_nn * global_nn) * args.planar_min_parcel_area_scale:
                continue
            line = LineString(part.exterior.coords)
            specs.extend(
                _line_specs_from_geom(
                    line,
                    island_id=island_id,
                    kind=kind,
                    width=_road_width_for_kind(kind, args),
                    family=-(41000 + part_no * 1000 + ring_no * 20 + inner_no),
                    min_length=min_length,
                    depth=ring_no + 1,
                )
            )
    return specs


def _mesh_downtown_grid_specs(
    poly: Polygon,
    *,
    center: MeshCenter,
    rx: float,
    ry: float,
    member_count: int,
    island_id: int,
    center_no: int,
    global_nn: float,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    if member_count < args.mesh_downtown_min_worlds:
        return []
    district = _mesh_ellipse_polygon(
        center.xy,
        center.axes,
        rx,
        ry,
        vertices=args.mesh_ring_vertices,
    )
    clip_geom = _safe_geom(poly.intersection(district))
    if clip_geom.is_empty:
        return []
    min_length = slot_step * args.boundary_road_min_length_slots
    grid_lines = int(
        np.clip(
            math.ceil(math.sqrt(member_count) / args.mesh_downtown_grid_worlds_scale),
            args.mesh_downtown_min_lines,
            args.mesh_downtown_max_lines,
        )
    )
    specs: list[RoadSpec] = []
    extents = [
        rx * args.mesh_downtown_grid_extent_frac,
        ry * args.mesh_downtown_grid_extent_frac,
    ]
    for family in (0, 1):
        normal_axis = family
        tangent_axis = 1 - family
        half_normal = extents[normal_axis]
        half_tangent = extents[tangent_axis] * 1.35
        if half_normal <= global_nn or half_tangent <= global_nn:
            continue
        offsets = np.linspace(-half_normal, half_normal, grid_lines)
        for line_no, offset in enumerate(offsets.tolist()):
            local = np.zeros((2, 2), dtype=np.float64)
            local[:, normal_axis] = offset
            local[:, tangent_axis] = [-half_tangent, half_tangent]
            line = LineString(_to_world(local, center.xy, center.axes))
            specs.extend(
                _line_specs_from_geom(
                    line.intersection(clip_geom),
                    island_id=island_id,
                    kind="local",
                    width=args.local_road_width,
                    family=-(56000 + center_no * 1000 + family * 100 + line_no),
                    min_length=min_length,
                    depth=1,
                )
            )
    return specs


def _mesh_connector_spoke_specs(
    poly: Polygon,
    *,
    center: MeshCenter,
    rx: float,
    ry: float,
    reach: float,
    member_count: int,
    island_id: int,
    center_no: int,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    spoke_count = int(
        np.clip(
            math.ceil(math.sqrt(member_count) / args.mesh_spoke_worlds_scale),
            args.mesh_min_spokes,
            args.mesh_max_spokes,
        )
    )
    min_length = slot_step * args.boundary_road_min_length_slots
    specs: list[RoadSpec] = []
    for spoke_no in range(spoke_count):
        theta = (math.pi * spoke_no) / max(spoke_count, 1)
        for side in (-1, 1):
            direction_theta = theta + (0.0 if side > 0 else math.pi)
            local_dir = np.array(
                [math.cos(direction_theta), math.sin(direction_theta)],
                dtype=np.float64,
            )
            belt_r = _mesh_ellipse_direction_radius(rx, ry, direction_theta)
            start_r = belt_r * args.mesh_spoke_start_ring_frac
            end_r = max(reach, belt_r * args.mesh_spoke_min_end_frac)
            if end_r <= start_r + min_length:
                continue
            local = np.vstack([local_dir * start_r, local_dir * end_r])
            line = LineString(_to_world(local, center.xy, center.axes))
            kind = (
                "collector"
                if spoke_no % max(args.mesh_spoke_collector_every, 1) == 0
                else "local"
            )
            specs.extend(
                _mesh_clip_line_specs(
                    line,
                    poly,
                    island_id=island_id,
                    kind=kind,
                    family=-(52000 + center_no * 1000 + spoke_no * 10 + side),
                    min_length=min_length,
                    args=args,
                    depth=1,
                )
            )
    return specs


def _mesh_center_road_specs(
    poly: Polygon,
    *,
    center: MeshCenter,
    xy: np.ndarray,
    island_id: int,
    center_no: int,
    global_nn: float,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    members = center.members
    if len(members) < args.mesh_center_min_worlds:
        return []
    pts = xy[members]
    local = (pts - center.xy) @ center.axes
    abs_local = np.abs(local)
    if len(abs_local) == 0:
        return []

    ring_count = int(
        np.clip(
            math.ceil(math.sqrt(len(members)) / args.mesh_ring_worlds_scale),
            1,
            args.mesh_max_city_rings,
        )
    )
    qs = np.linspace(
        args.mesh_ring_inner_quantile,
        args.mesh_ring_outer_quantile,
        ring_count,
    )
    min_radius = global_nn * args.mesh_city_min_radius_scale
    min_length = slot_step * args.boundary_road_min_length_slots
    specs: list[RoadSpec] = []

    downtown_q = args.mesh_downtown_radius_quantile
    downtown_rx = max(
        float(np.quantile(abs_local[:, 0], downtown_q)) * 1.12,
        min_radius * args.mesh_downtown_min_radius_frac,
    )
    downtown_ry = max(
        float(np.quantile(abs_local[:, 1], downtown_q)) * 1.12,
        min_radius * args.mesh_downtown_min_radius_frac,
    )
    long = max(downtown_rx, downtown_ry)
    short = max(
        min(downtown_rx, downtown_ry),
        long / max(args.mesh_ring_max_aspect, 1.0),
    )
    if downtown_rx >= downtown_ry:
        downtown_rx, downtown_ry = long, short
    else:
        downtown_rx, downtown_ry = short, long
    specs.extend(
        _mesh_downtown_grid_specs(
            poly,
            center=center,
            rx=downtown_rx,
            ry=downtown_ry,
            member_count=len(members),
            island_id=island_id,
            center_no=center_no,
            global_nn=global_nn,
            slot_step=slot_step,
            args=args,
        )
    )

    last_rx = downtown_rx
    last_ry = downtown_ry
    for ring_no, q in enumerate(qs.tolist()):
        rx = max(float(np.quantile(abs_local[:, 0], q)) * 1.08, min_radius)
        ry = max(float(np.quantile(abs_local[:, 1], q)) * 1.08, min_radius)
        long = max(rx, ry)
        short = max(min(rx, ry), long / max(args.mesh_ring_max_aspect, 1.0))
        if rx >= ry:
            rx, ry = long, short
        else:
            rx, ry = short, long
        if (
            rx < downtown_rx * args.mesh_ring_min_downtown_gap
            and ry < downtown_ry * args.mesh_ring_min_downtown_gap
        ):
            continue
        last_rx = max(last_rx, rx)
        last_ry = max(last_ry, ry)
        kind = "collector" if ring_no == ring_count - 1 else "local"
        line = _mesh_ellipse_line(
            center.xy,
            center.axes,
            rx,
            ry,
            vertices=args.mesh_ring_vertices,
        )
        specs.extend(
            _mesh_clip_line_specs(
                line,
                poly,
                island_id=island_id,
                kind=kind,
                family=-(50000 + center_no * 100 + ring_no),
                min_length=min_length,
                args=args,
                depth=ring_no + 1,
            )
        )

    reach = max(last_rx, last_ry, center.radius) * args.mesh_spoke_reach_scale
    reach = max(reach, min_radius * 2.0)
    specs.extend(
        _mesh_connector_spoke_specs(
            poly,
            center=center,
            rx=last_rx,
            ry=last_ry,
            reach=reach,
            member_count=len(members),
            island_id=island_id,
            center_no=center_no,
            slot_step=slot_step,
            args=args,
        )
    )
    return specs


def _mesh_axis_road_specs(
    poly: Polygon,
    pts: np.ndarray,
    *,
    island_id: int,
    part_no: int,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    if len(pts) < args.mesh_axis_min_worlds:
        return []
    center, axes = _pca_axes(pts)
    diag = _mesh_poly_diag(poly) * 1.6
    min_length = slot_step * args.min_road_length_slots
    specs: list[RoadSpec] = []
    for axis_no in range(2):
        if axis_no == 1 and len(pts) < args.mesh_cross_axis_min_worlds:
            continue
        kind = "collector" if axis_no == 0 else "local"
        line = _mesh_axis_line(center, axes[:, axis_no], length=diag)
        specs.extend(
            _mesh_clip_line_specs(
                line,
                poly,
                island_id=island_id,
                kind=kind,
                family=-(54000 + part_no * 10 + axis_no),
                min_length=min_length,
                args=args,
                depth=0,
            )
        )
    return specs


def _mesh_initial_road_specs(
    *,
    parts: list[Polygon],
    members_by_part: list[np.ndarray],
    centers: list[MeshCenter],
    xy: np.ndarray,
    island_id: int,
    global_nn: float,
    slot_step: float,
    args,
) -> list[RoadSpec]:
    specs = _planar_boundary_road_specs(
        parts,
        members_by_part=members_by_part,
        island_id=island_id,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
    )
    for part_no, poly in enumerate(parts):
        members = members_by_part[part_no]
        pts = xy[members]
        specs.extend(
            _mesh_contour_road_specs(
                poly,
                world_count=len(members),
                island_id=island_id,
                part_no=part_no,
                global_nn=global_nn,
                slot_step=slot_step,
                args=args,
            )
        )
        specs.extend(
            _mesh_axis_road_specs(
                poly,
                pts,
                island_id=island_id,
                part_no=part_no,
                slot_step=slot_step,
                args=args,
            )
        )
        if centers:
            member_set = set(members.tolist())
            for center_no, center in enumerate(centers):
                center_point = shapely.Point(
                    float(center.xy[0]),
                    float(center.xy[1]),
                )
                if not poly.covers(center_point):
                    continue
                overlap = np.asarray(
                    [i for i in center.members.tolist() if i in member_set],
                    dtype=np.int64,
                )
                if len(overlap) < args.mesh_center_min_worlds:
                    continue
                local_center = MeshCenter(
                    xy=center.xy,
                    axes=center.axes,
                    members=overlap,
                    radius=center.radius,
                )
                specs.extend(
                    _mesh_center_road_specs(
                        poly,
                        center=local_center,
                        xy=xy,
                        island_id=island_id,
                        center_no=center_no,
                        global_nn=global_nn,
                        slot_step=slot_step,
                        args=args,
                    )
                )
    return specs


def _mesh_finalize_roads(
    specs: list[RoadSpec],
    *,
    land_geom,
    island_id: int,
    global_nn: float,
    slot_step: float,
    args,
    smooth: bool,
) -> tuple[list[RoadSpec], dict[str, int]]:
    if smooth:
        specs = _postprocess_road_specs(
            specs,
            land_geom=land_geom,
            global_nn=global_nn,
            slot_step=slot_step,
            args=args,
        )
    specs = _snap_planar_junctions(specs, global_nn=global_nn, args=args)
    return _node_planar_road_specs(
        specs,
        island_id=island_id,
        global_nn=global_nn,
        args=args,
    )


def _mesh_polygonize_blocks(
    *,
    parts: list[Polygon],
    road_specs: list[RoadSpec],
    xy: np.ndarray,
    global_nn: float,
    args,
) -> list[MeshBlock]:
    boundary_lines: list[LineString] = []
    for poly in parts:
        boundary_lines.append(LineString(poly.exterior.coords))
        boundary_lines.extend(LineString(ring.coords) for ring in poly.interiors)
    road_lines = [
        LineString(spec.coords)
        for spec in road_specs
        if len(spec.coords) >= 2 and LineString(spec.coords).length > 1e-9
    ]
    if not boundary_lines:
        return []
    land_union = _safe_geom(shapely.unary_union(parts))
    min_area = (global_nn * global_nn) * args.mesh_min_block_area_scale
    linework = shapely.unary_union([*boundary_lines, *road_lines])
    faces = []
    for face in polygonize(linework):
        if face.is_empty:
            continue
        if not land_union.covers(face.representative_point()):
            continue
        clipped = _safe_geom(face.intersection(land_union))
        for poly in iter_polygons(clipped):
            if poly.area >= min_area:
                faces.append(poly)
    if not faces:
        faces = [p for p in parts if p.area >= min_area]
    members_by_face = _classify_points_to_polys(
        faces,
        xy,
        np.arange(len(xy), dtype=np.int64),
    )
    return [
        MeshBlock(geom=poly, point_idx=members)
        for poly, members in zip(faces, members_by_face, strict=True)
        if len(members) > 0
    ]


def _mesh_block_capacity(block: MeshBlock, *, slot_step: float, args) -> int:
    spacing = max(slot_step * args.mesh_capacity_spacing_scale, 1e-9)
    return max(
        1,
        int(
            math.floor(
                block.geom.exterior.length
                * args.mesh_block_frontage_efficiency
                / spacing
            )
        ),
    )


def _mesh_curved_split_line(
    *,
    origin: np.ndarray,
    tangent: np.ndarray,
    normal: np.ndarray,
    length: float,
    radius: float,
    args,
) -> LineString:
    n = int(
        np.clip(
            length / max(radius * 0.05, 1e-9),
            24,
            args.road_curve_max_vertices,
        )
    )
    t = np.linspace(-length * 0.5, length * 0.5, n)
    curve = -(t * t) / max(radius * 2.0, 1e-9)
    curve = np.clip(
        curve * args.mesh_split_curve_strength,
        -length * args.mesh_split_curve_max_frac,
        length * args.mesh_split_curve_max_frac,
    )
    pts = origin + tangent[None, :] * t[:, None] + normal[None, :] * curve[:, None]
    return LineString([(float(x), float(y)) for x, y in pts])


def _mesh_split_candidate_lines(
    block: MeshBlock,
    xy: np.ndarray,
    centers: list[MeshCenter],
    *,
    global_nn: float,
    args,
) -> list[tuple[LineString, float]]:
    poly = block.geom
    centroid = np.array([poly.centroid.x, poly.centroid.y], dtype=np.float64)
    diag = _mesh_poly_diag(poly) * 1.5
    out: list[tuple[LineString, float]] = []
    if centers:
        nearest = min(centers, key=lambda c: float(np.linalg.norm(centroid - c.xy)))
        radial = centroid - nearest.xy
        radius = float(np.linalg.norm(radial))
        if radius > global_nn * 0.5:
            radial /= radius
            tangent = np.array([-radial[1], radial[0]], dtype=np.float64)
            out.append(
                (
                    _mesh_curved_split_line(
                        origin=centroid,
                        tangent=tangent,
                        normal=-radial,
                        length=diag,
                        radius=max(radius, global_nn),
                        args=args,
                    ),
                    args.mesh_split_tensor_bonus,
                )
            )
            out.append(
                (
                    _mesh_axis_line(centroid, radial, length=diag),
                    args.mesh_split_tensor_bonus * 0.65,
                )
            )
    pts = xy[block.point_idx]
    center, axes = _polygon_axes(poly, pts)
    lo, hi = _project_poly_bounds(poly, center, axes)
    span = np.maximum(hi - lo, 1e-9)
    major = int(np.argmax(span))
    minor = 1 - major
    for normal_axis, bonus in ((major, 0.0), (minor, -0.05)):
        tangent_axis = 1 - normal_axis
        local = np.zeros((2, 2), dtype=np.float64)
        local[:, tangent_axis] = [
            lo[tangent_axis] - span[tangent_axis] * 0.35,
            hi[tangent_axis] + span[tangent_axis] * 0.35,
        ]
        if len(pts) > 1:
            local_pts = (pts - center) @ axes
            split_at = float(np.median(local_pts[:, normal_axis]))
        else:
            split_at = float((lo[normal_axis] + hi[normal_axis]) * 0.5)
        local[:, normal_axis] = split_at
        out.append((LineString(_to_world(local, center, axes)), bonus))
    return out


def _mesh_best_block_split(
    block: MeshBlock,
    xy: np.ndarray,
    centers: list[MeshCenter],
    *,
    island_id: int,
    family: int,
    global_nn: float,
    slot_step: float,
    args,
) -> RoadSpec | None:
    if len(block.point_idx) < args.mesh_refine_min_worlds:
        return None
    min_area = (global_nn * global_nn) * args.mesh_min_block_area_scale
    best: tuple[float, LineString] | None = None
    for line, bonus in _mesh_split_candidate_lines(
        block,
        xy,
        centers,
        global_nn=global_nn,
        args=args,
    ):
        try:
            result = split_geom(block.geom, line)
        except (ValueError, shapely.GEOSException):
            continue
        parts = [
            p
            for p in sorted(iter_polygons(result), key=lambda g: g.area, reverse=True)
            if p.area >= min_area
        ]
        if len(parts) != 2:
            continue
        members_by_part = _classify_points_to_polys(parts, xy, block.point_idx)
        counts = np.asarray([len(m) for m in members_by_part], dtype=np.float64)
        if np.count_nonzero(counts) < 2:
            continue
        count_balance = float(counts.min() / max(counts.max(), 1.0))
        if count_balance < args.mesh_split_min_count_balance:
            continue
        area = np.asarray([p.area for p in parts], dtype=np.float64)
        area_balance = float(area.min() / max(area.max(), 1e-12))
        aspect = max(_polygon_aspect_ratio(p) for p in parts)
        aspect_score = 1.0 / (
            1.0 + max(0.0, math.log(aspect / max(args.mesh_target_block_aspect, 1e-9)))
        )
        clipped = line.intersection(block.geom)
        segments = [seg for seg in _iter_lines(clipped) if seg.length > 1e-9]
        if not segments:
            continue
        road_line = max(segments, key=lambda seg: seg.length)
        if road_line.length < slot_step * args.boundary_road_min_length_slots:
            continue
        score = (
            count_balance * 0.44
            + area_balance * 0.22
            + aspect_score * 0.24
            + min(road_line.length / max(_mesh_poly_diag(block.geom), 1e-9), 1.0)
            * 0.10
            + bonus
        )
        if best is None or score > best[0]:
            best = (score, road_line)
    if best is None:
        return None
    _score, road_line = best
    density_norm = len(block.point_idx) * global_nn * global_nn / max(
        float(block.geom.area),
        1e-12,
    )
    kind = "local" if density_norm >= args.mesh_refine_local_density else "service"
    return RoadSpec(
        coords=[(float(x), float(y)) for x, y in road_line.coords],
        kind=kind,
        island_id=island_id,
        width=_road_width_for_kind(kind, args),
        family=family,
        depth=2,
    )


def _mesh_refine_roads(
    *,
    road_specs: list[RoadSpec],
    parts: list[Polygon],
    xy: np.ndarray,
    centers: list[MeshCenter],
    island_id: int,
    land_geom,
    global_nn: float,
    slot_step: float,
    args,
) -> tuple[list[RoadSpec], list[MeshBlock], dict[str, int]]:
    road_specs, topo_metrics = _mesh_finalize_roads(
        road_specs,
        land_geom=land_geom,
        island_id=island_id,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
        smooth=True,
    )
    metrics = {
        "mesh_refine_added_roads": 0,
        "mesh_refine_rounds": 0,
        **topo_metrics,
    }
    if args.mesh_progress:
        print(
            f"      mesh island {island_id}: polygonize initial "
            f"{len(road_specs):,} roads",
            flush=True,
        )
    blocks = _mesh_polygonize_blocks(
        parts=parts,
        road_specs=road_specs,
        xy=xy,
        global_nn=global_nn,
        args=args,
    )
    if args.mesh_progress:
        print(
            f"      mesh island {island_id}: initial {len(blocks):,} blocks",
            flush=True,
        )
    target_block_area = (
        global_nn
        * global_nn
        * max(args.mesh_target_block_area_scale, args.mesh_min_block_area_scale)
    )
    for round_no in range(max(0, int(args.mesh_refine_rounds))):
        scored: list[tuple[float, int, MeshBlock]] = []
        for block_no, block in enumerate(blocks):
            capacity = _mesh_block_capacity(block, slot_step=slot_step, args=args)
            overload = len(block.point_idx) - capacity * args.mesh_capacity_target
            area_excess = max(
                0.0,
                float(block.geom.area) / max(target_block_area, 1e-12) - 1.0,
            )
            area_score = (
                area_excess * args.mesh_area_refine_score_scale
                if len(block.point_idx) >= args.mesh_area_refine_min_worlds
                else 0.0
            )
            score = max(float(overload), area_score)
            if score <= 0:
                continue
            scored.append((score, block_no, block))
        scored.sort(reverse=True, key=lambda x: x[0])
        if not scored:
            break
        additions: list[RoadSpec] = []
        for _overload, block_no, block in scored[: args.mesh_refine_batch]:
            spec = _mesh_best_block_split(
                block,
                xy,
                centers,
                island_id=island_id,
                family=round_no * 100000 + block_no,
                global_nn=global_nn,
                slot_step=slot_step,
                args=args,
            )
            if spec is not None:
                additions.append(spec)
        if not additions:
            break
        if args.mesh_progress:
            worst = scored[0][0] if scored else 0.0
            print(
                f"      mesh island {island_id}: refine {round_no + 1} "
                f"adding {len(additions):,} roads "
                f"(worst score {worst:.1f})",
                flush=True,
            )
        road_specs.extend(additions)
        metrics["mesh_refine_added_roads"] += int(len(additions))
        metrics["mesh_refine_rounds"] = round_no + 1
        road_specs, topo = _mesh_finalize_roads(
            road_specs,
            land_geom=land_geom,
            island_id=island_id,
            global_nn=global_nn,
            slot_step=slot_step,
            args=args,
            smooth=False,
        )
        metrics["topology_junction_merges"] += topo.get("topology_junction_merges", 0)
        metrics["topology_short_edge_merges"] += topo.get(
            "topology_short_edge_merges",
            0,
        )
        blocks = _mesh_polygonize_blocks(
            parts=parts,
            road_specs=road_specs,
            xy=xy,
            global_nn=global_nn,
            args=args,
        )
        if args.mesh_progress:
            print(
                f"      mesh island {island_id}: refine {round_no + 1} -> "
                f"{len(road_specs):,} roads, {len(blocks):,} blocks",
                flush=True,
            )
    return road_specs, blocks, metrics


def _line_tangent_at(line: LineString, distance: float, eps: float) -> np.ndarray:
    total = max(float(line.length), 1e-12)
    a = line.interpolate(max(0.0, distance - eps))
    b = line.interpolate(min(total, distance + eps))
    v = np.array([b.x - a.x, b.y - a.y], dtype=np.float64)
    norm = float(np.linalg.norm(v))
    if norm <= 1e-12:
        return np.array([1.0, 0.0], dtype=np.float64)
    return v / norm


def _nearest_road_for_frontage(
    point: np.ndarray,
    *,
    road_lines: list[LineString],
    tree: shapely.STRtree,
    match_tol: float,
) -> tuple[int, np.ndarray, float] | None:
    p = shapely.Point(float(point[0]), float(point[1]))
    cand = np.asarray(tree.query(p.buffer(match_tol).envelope), dtype=np.int64)
    best: tuple[int, np.ndarray, float] | None = None
    for road_idx in cand.tolist():
        line = road_lines[road_idx]
        d = float(line.distance(p))
        if d > match_tol:
            continue
        q = line.interpolate(line.project(p))
        qxy = np.array([q.x, q.y], dtype=np.float64)
        if best is None or d < best[2]:
            best = (road_idx, qxy, d)
    return best


def _footprint_poly(
    xy: np.ndarray,
    tangent: np.ndarray,
    width: float,
    depth: float,
) -> Polygon:
    normal = np.array([-tangent[1], tangent[0]], dtype=np.float64)
    hw = width * 0.5
    hd = depth * 0.5
    corners = np.array(
        [
            xy - tangent * hw - normal * hd,
            xy + tangent * hw - normal * hd,
            xy + tangent * hw + normal * hd,
            xy - tangent * hw + normal * hd,
            xy - tangent * hw - normal * hd,
        ],
        dtype=np.float64,
    )
    return Polygon(corners)


def _mesh_filter_slot_indices(
    slots: SlotSet,
    *,
    seed: int,
    global_nn: float,
    args,
) -> np.ndarray:
    if len(slots.xy) < 2:
        return np.arange(len(slots.xy), dtype=np.int64)
    rng = np.random.default_rng(seed)
    radii = 0.5 * np.hypot(slots.width, slots.depth)
    min_dist = max(
        global_nn * args.slot_filter_min_global_scale,
        float(np.median(np.maximum(slots.width, slots.depth)))
        * args.mesh_slot_filter_building_scale,
    )
    cell = max(
        float(min_dist),
        float(radii.max(initial=0.0) * args.mesh_slot_filter_radius_scale),
        1e-12,
    )
    order = rng.permutation(len(slots.xy))
    accepted: list[int] = []
    buckets: dict[tuple[int, int], list[int]] = defaultdict(list)
    for i in order.tolist():
        p = slots.xy[i]
        key = (math.floor(float(p[0]) / cell), math.floor(float(p[1]) / cell))
        ok = True
        for gx in range(key[0] - 1, key[0] + 2):
            for gy in range(key[1] - 1, key[1] + 2):
                for j in buckets.get((gx, gy), []):
                    limit = max(
                        min_dist,
                        float(
                            (radii[i] + radii[j])
                            * args.mesh_slot_filter_radius_scale
                        ),
                    )
                    if float(np.linalg.norm(p - slots.xy[j])) < limit:
                        ok = False
                        break
                if not ok:
                    break
            if not ok:
                break
        if ok:
            buckets[key].append(i)
            accepted.append(i)
    return np.asarray(sorted(accepted), dtype=np.int64)


def _mesh_slots_for_blocks(
    blocks: list[MeshBlock],
    road_specs: list[RoadSpec],
    *,
    global_nn: float,
    slot_step: float,
    road_spacing: float,
    building_scale: float,
    island_id: int,
    args,
) -> MeshSlotResult:
    road_lines = [
        LineString(spec.coords)
        for spec in road_specs
        if len(spec.coords) >= 2 and LineString(spec.coords).length > 1e-9
    ]
    if not road_lines:
        return MeshSlotResult(
            slots=_slotset_empty(),
            block_index=np.empty(0, dtype=np.int32),
            rejected_outside=0,
            rejected_roadless=0,
        )
    tree = shapely.STRtree(road_lines)
    road_corridor = GeometryCollection()
    road_clearance = global_nn * args.slot_footprint_road_clearance_scale
    if road_clearance > 0:
        road_corridor = shapely.unary_union(road_lines).buffer(
            road_clearance,
            cap_style="round",
            join_style="round",
        )
    rng = np.random.default_rng(args.seed + island_id * 2207)
    match_tol = max(global_nn * args.mesh_frontage_match_scale, slot_step * 0.18)
    setback = global_nn * args.building_setback_scale * math.sqrt(building_scale)
    base_width = slot_step * args.building_width_scale * building_scale
    base_depth = min(
        slot_step * args.building_depth_scale * building_scale,
        road_spacing * args.building_depth_road_spacing_scale,
    )
    xy_out: list[np.ndarray] = []
    frontage_out: list[np.ndarray] = []
    angle_out: list[float] = []
    width_out: list[float] = []
    depth_out: list[float] = []
    road_out: list[int] = []
    side_out: list[int] = []
    along_out: list[float] = []
    block_out: list[int] = []
    rejected_outside = 0
    rejected_roadless = 0
    for block_no, block in enumerate(blocks):
        if block.geom.is_empty or block.geom.area <= 0:
            continue
        ring = LineString(block.geom.exterior.coords)
        total = float(ring.length)
        if total <= slot_step:
            continue
        start = slot_step * args.mesh_slot_phase
        distances = np.arange(start, total, slot_step, dtype=np.float64)
        eps = min(slot_step * 0.45, total * 0.01)
        for d in distances.tolist():
            p = ring.interpolate(d)
            frontage = np.array([p.x, p.y], dtype=np.float64)
            nearest = _nearest_road_for_frontage(
                frontage,
                road_lines=road_lines,
                tree=tree,
                match_tol=match_tol,
            )
            if nearest is None:
                rejected_roadless += 1
                continue
            road_idx, road_frontage, _road_dist = nearest
            tangent = _line_tangent_at(ring, d, eps)
            normal = np.array([-tangent[1], tangent[0]], dtype=np.float64)
            probe = frontage + normal * max(global_nn * 0.1, 1e-9)
            if not block.geom.covers(shapely.Point(float(probe[0]), float(probe[1]))):
                normal = -normal
                probe = frontage + normal * max(global_nn * 0.1, 1e-9)
            if not block.geom.covers(shapely.Point(float(probe[0]), float(probe[1]))):
                rejected_outside += 1
                continue
            width = base_width * float(
                np.clip(1.0 + rng.normal(0, args.building_width_jitter), 0.72, 1.28)
            )
            depth = base_depth * float(
                np.clip(1.0 + rng.normal(0, args.building_depth_jitter), 0.72, 1.35)
            )
            center = frontage + normal * (depth * 0.5 + setback)
            angle = math.atan2(tangent[1], tangent[0]) + float(
                rng.normal(0, args.building_angle_jitter)
            )
            tangent2 = np.array([math.cos(angle), math.sin(angle)], dtype=np.float64)
            footprint = _footprint_poly(center, tangent2, width, depth)
            if not block.geom.covers(footprint):
                rejected_outside += 1
                continue
            if not road_corridor.is_empty and footprint.intersects(road_corridor):
                rejected_outside += 1
                continue
            xy_out.append(center)
            frontage_out.append(road_frontage)
            angle_out.append(angle)
            width_out.append(width)
            depth_out.append(depth)
            road_out.append(road_idx)
            side_out.append(1)
            along_out.append(d)
            block_out.append(block_no)
    if not xy_out:
        return MeshSlotResult(
            slots=_slotset_empty(),
            block_index=np.empty(0, dtype=np.int32),
            rejected_outside=rejected_outside,
            rejected_roadless=rejected_roadless,
        )
    slots = SlotSet(
        xy=np.vstack(xy_out).astype(np.float64),
        frontage=np.vstack(frontage_out).astype(np.float64),
        angle=np.asarray(angle_out, dtype=np.float32),
        width=np.asarray(width_out, dtype=np.float32),
        depth=np.asarray(depth_out, dtype=np.float32),
        road_index=np.asarray(road_out, dtype=np.int32),
        side=np.asarray(side_out, dtype=np.int8),
        along=np.asarray(along_out, dtype=np.float32),
    )
    block_index = np.asarray(block_out, dtype=np.int32)
    keep = _mesh_filter_slot_indices(
        slots,
        seed=args.seed + island_id * 9176,
        global_nn=global_nn,
        args=args,
    )
    return MeshSlotResult(
        slots=_take_slots(slots, keep),
        block_index=block_index[keep],
        rejected_outside=rejected_outside,
        rejected_roadless=rejected_roadless,
    )


def _assign_slots_by_mesh_blocks(
    xy: np.ndarray,
    slots: SlotSet,
    slot_block: np.ndarray,
    blocks: list[MeshBlock],
    *,
    global_nn: float,
    args,
) -> tuple[np.ndarray, dict[str, int]]:
    out = np.full(len(xy), -1, dtype=np.int64)
    used = np.zeros(len(slots.xy), dtype=bool)
    cross_block = 0
    shortage = 0
    for block_no, block in sorted(
        enumerate(blocks),
        key=lambda item: len(item[1].point_idx),
        reverse=True,
    ):
        members = block.point_idx
        if len(members) == 0:
            continue
        cand = np.flatnonzero((slot_block == block_no) & ~used)
        if len(cand) == 0:
            shortage += len(members)
            continue
        assign_members = members
        if len(cand) < len(members):
            shortage += len(members) - len(cand)
            dist, _nbr = cKDTree(slots.xy[cand]).query(
                xy[members],
                k=1,
                workers=_spatial_workers(),
            )
            keep = np.argsort(np.asarray(dist), kind="stable")[: len(cand)]
            assign_members = members[keep]
        local = _assign_slots(
            xy[assign_members],
            slots.xy[cand],
            args.max_hungarian,
            candidate_k=args.assignment_candidate_k,
        )
        chosen = cand[local]
        out[assign_members] = chosen
        used[chosen] = True
    missing = np.flatnonzero(out < 0)
    if len(missing):
        free = np.flatnonzero(~used)
        if len(free):
            fill_count = min(len(missing), len(free))
            fill = _assign_slots(
                xy[missing[:fill_count]],
                slots.xy[free],
                args.max_hungarian,
                candidate_k=args.assignment_candidate_k,
            )
            chosen = free[fill]
            out[missing[:fill_count]] = chosen
            used[chosen] = True
            cross_block += fill_count
    return out, {
        "mesh_slot_shortage": int(shortage),
        "mesh_cross_block_assignments": int(cross_block),
        "mesh_unassigned_after_slots": int(np.count_nonzero(out < 0)),
    }


def _mesh_road_graph_metrics(
    road_specs: list[RoadSpec],
    *,
    global_nn: float,
) -> dict[str, float | int]:
    node_dirs: dict[tuple[int, int], list[np.ndarray]] = defaultdict(list)
    tol = max(global_nn * 1e-4, 1e-10)
    for spec in road_specs:
        coords = np.asarray(spec.coords, dtype=np.float64)
        if len(coords) < 2:
            continue
        for end_idx, nbr_idx in ((0, 1), (len(coords) - 1, len(coords) - 2)):
            p = coords[end_idx]
            q = coords[nbr_idx]
            v = q - p
            norm = float(np.linalg.norm(v))
            if norm <= 1e-12:
                continue
            node_dirs[_endpoint_node_key(p, tol)].append(v / norm)
    degrees = np.asarray([len(v) for v in node_dirs.values()], dtype=np.int64)
    angle_devs: list[float] = []
    for dirs in node_dirs.values():
        if len(dirs) < 2:
            continue
        for i in range(len(dirs)):
            for j in range(i + 1, len(dirs)):
                dot = float(np.dot(dirs[i], dirs[j]))
                angle = math.degrees(math.acos(np.clip(dot, -1.0, 1.0)))
                angle = min(angle, 180.0 - angle)
                if angle < 25.0:
                    continue
                angle_devs.append(abs(angle - 90.0))
    dev = np.asarray(angle_devs, dtype=np.float64)
    return {
        "road_nodes": int(len(degrees)),
        "road_degree1": int(np.count_nonzero(degrees == 1)),
        "road_degree2": int(np.count_nonzero(degrees == 2)),
        "road_degree3": int(np.count_nonzero(degrees == 3)),
        "road_degree4": int(np.count_nonzero(degrees == 4)),
        "road_degree5_plus": int(np.count_nonzero(degrees >= 5)),
        "road_degree8_plus": int(np.count_nonzero(degrees >= 8)),
        "road_degree12_plus": int(np.count_nonzero(degrees >= 12)),
        "road_degree_max": int(degrees.max(initial=0)),
        "junction_angle_dev_median": float(np.median(dev)) if len(dev) else 0.0,
        "junction_angle_dev_p90": float(np.quantile(dev, 0.90)) if len(dev) else 0.0,
    }


def _generate_mesh_streets(
    *,
    xy: np.ndarray,
    land_geom,
    island_id: int,
    global_nn: float,
    slot_step: float,
    args,
) -> tuple[list[RoadSpec], list[MeshBlock], object, dict[str, int | float]]:
    min_area = (global_nn * global_nn) * args.planar_min_parcel_area_scale
    parts = sorted(
        [p for p in iter_polygons(land_geom) if p.area >= min_area],
        key=lambda p: p.area,
        reverse=True,
    )
    if not parts:
        parts = sorted(iter_polygons(land_geom), key=lambda p: p.area, reverse=True)
    build_geom = _safe_geom(shapely.unary_union(parts))
    members_by_part = _classify_points_to_polys(
        parts,
        xy,
        np.arange(len(xy), dtype=np.int64),
    )
    centers = _mesh_density_centers(xy, global_nn=global_nn, args=args)
    initial_specs = _mesh_initial_road_specs(
        parts=parts,
        members_by_part=members_by_part,
        centers=centers,
        xy=xy,
        island_id=island_id,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
    )
    road_specs, blocks, metrics = _mesh_refine_roads(
        road_specs=initial_specs,
        parts=parts,
        xy=xy,
        centers=centers,
        island_id=island_id,
        land_geom=build_geom,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
    )
    overloaded = 0
    shortage = 0.0
    for block in blocks:
        capacity = _mesh_block_capacity(block, slot_step=slot_step, args=args)
        excess = len(block.point_idx) - capacity * args.mesh_capacity_target
        if excess > 0:
            overloaded += 1
            shortage += float(excess)
    metrics.update(
        {
            "density_centers": int(len(centers)),
            "mesh_initial_roads": int(len(initial_specs)),
            "mesh_blocks_over_capacity": int(overloaded),
            "mesh_capacity_shortage": float(shortage),
        }
    )
    metrics.update(_mesh_road_graph_metrics(road_specs, global_nn=global_nn))
    return road_specs, blocks, build_geom, metrics


def _frontage_blocks(
    *,
    island_id: int,
    selected_xy: np.ndarray,
    selected_road: np.ndarray,
    selected_side: np.ndarray,
    selected_along: np.ndarray,
    selected_depth: np.ndarray,
    ids: Ids,
    args,
) -> tuple[list[Block], np.ndarray]:
    block_ids = np.full(len(selected_xy), -1, dtype=np.int64)
    blocks: list[Block] = []
    groups: dict[tuple[int, int], list[int]] = defaultdict(list)
    for i, (road, side) in enumerate(zip(selected_road, selected_side, strict=True)):
        groups[(int(road), int(side))].append(i)
    for members in groups.values():
        members.sort(key=lambda i: float(selected_along[i]))
        for start in range(0, len(members), args.block_target_worlds):
            chunk = members[start : start + args.block_target_worlds]
            arr = np.asarray(chunk, dtype=np.int64)
            pts = selected_xy[arr]
            buffer_r = max(
                float(np.median(selected_depth[arr])) * args.block_buffer_scale,
                1e-6,
            )
            if len(pts) == 1:
                geom = shapely.Point(float(pts[0, 0]), float(pts[0, 1])).buffer(
                    buffer_r,
                    quad_segs=3,
                )
            else:
                geom = LineString([(float(x), float(y)) for x, y in pts]).buffer(
                    buffer_r,
                    cap_style="round",
                    join_style="round",
                )
            geom = _safe_geom(geom)
            bid = ids.block_id()
            block_ids[arr] = bid
            blocks.append(
                Block(
                    block_id=bid,
                    island_id=island_id,
                    geom=geom,
                    target_lots=len(arr),
                    assigned_worlds=len(arr),
                )
            )
    return blocks, block_ids


def _layout_streamline_island(
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
) -> dict[str, int | float]:
    island_land, _info = build_land_geometry(
        xy,
        method="raster",
        raster_max_dim=args.road_land_raster_max_dim,
        raster_nn_cells=args.land_raster_nn_cells,
        raster_dilate_cells=args.land_raster_dilate_cells,
        raster_close_cells=args.land_raster_close_cells,
        raster_simplify_cells=args.land_raster_simplify_cells,
        raster_smooth_cells=args.land_raster_smooth_cells,
        raster_min_area_cells=args.land_raster_min_area_cells,
    )
    island_land = _smooth_coastline_geom(
        _safe_geom(island_land),
        iterations=args.land_chaikin_iterations,
        simplify=global_nn * args.land_chaikin_simplify_scale,
    )
    slot_step = max(
        global_nn * args.frontage_spacing_scale,
        global_nn * args.frontage_spacing_min_scale,
    )
    road_spacing = max(
        global_nn * args.road_spacing_min_scale,
        slot_step * args.recursive_road_spacing_slot_scale,
    )
    local_nn_q = _nn_quantile(xy, args.building_scale_nn_quantile)
    building_scale = float(
        np.clip(
            (local_nn_q / max(global_nn, 1e-9)) ** args.building_local_nn_power,
            args.building_local_scale_min,
            args.building_local_scale_max,
        )
    )
    street_layout = _generate_planar_streets(
        xy=xy,
        land_geom=island_land,
        island_id=island_id,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
    )
    road_specs = street_layout.road_specs
    leaves = street_layout.leaves
    build_geom = street_layout.build_geom
    splits = street_layout.splits
    street_metrics = street_layout.metrics
    slots = _slots_for_road_specs(
        road_specs,
        land_geom=build_geom,
        demand_xy=xy,
        global_nn=global_nn,
        slot_step=slot_step,
        road_spacing=road_spacing,
        building_scale=building_scale,
        island_id=island_id,
        args=args,
        min_slots=int(math.ceil(len(xy) * args.slot_capacity_min)),
    )
    generated_slots = int(len(slots.xy))
    fallback_slots = 0
    slot_road_hit = _slot_other_road_hit_mask(
        slots,
        road_specs=road_specs,
        global_nn=global_nn,
        args=args,
    )
    slot_penalty = slot_road_hit.astype(np.float64)
    assignment_xy, assignment_relax_metrics = _relax_assignment_targets(
        xy,
        global_nn=global_nn,
        island_id=island_id,
        args=args,
    )

    local_block_ids = np.full(len(xy), -1, dtype=np.int64)
    for leaf in leaves:
        if len(leaf.point_idx) == 0:
            continue
        bid = ids.block_id()
        local_block_ids[leaf.point_idx] = bid
        blocks.append(
            Block(
                block_id=bid,
                island_id=island_id,
                geom=leaf.geom,
                target_lots=len(leaf.point_idx),
                assigned_worlds=len(leaf.point_idx),
            )
        )
    if np.any(local_block_ids < 0):
        bid = ids.block_id()
        missing = np.flatnonzero(local_block_ids < 0)
        local_block_ids[missing] = bid
        blocks.append(
            Block(
                block_id=bid,
                island_id=island_id,
                geom=_safe_geom(shapely.MultiPoint(xy[missing]).convex_hull),
                target_lots=len(missing),
                assigned_worlds=len(missing),
            )
        )

    if len(slots.xy) >= len(xy):
        assigned = _assign_slots_by_leaves(
            assignment_xy,
            slots.xy,
            slot_penalty,
            leaves,
            global_nn=global_nn,
            args=args,
        )
    else:
        assigned = _assign_slots_by_leaves_partial(
            assignment_xy,
            slots.xy,
            slot_penalty,
            leaves,
            global_nn=global_nn,
            args=args,
        )
        missing = np.flatnonzero(assigned < 0)
        if len(missing):
            fallback_roads, fallback_slotset = _fallback_service_slots(
                xy=xy[missing],
                island_id=island_id,
                global_nn=global_nn,
                road_index_offset=len(road_specs),
                args=args,
            )
            road_specs.extend(fallback_roads)
            offset = len(slots.xy)
            fallback_assign = _assign_slots(
                assignment_xy[missing],
                fallback_slotset.xy,
                args.max_hungarian,
                candidate_k=args.assignment_candidate_k,
            )
            slots = _concat_slots([slots, fallback_slotset])
            slot_penalty = np.concatenate(
                [
                    slot_penalty,
                    np.zeros(len(fallback_slotset.xy), dtype=np.float64),
                ]
            )
            assigned[missing] = offset + fallback_assign
            fallback_slots = int(len(fallback_slotset.xy))
    selected_xy = slots.xy[assigned]
    selected_frontage = slots.frontage[assigned]
    selected_road = slots.road_index[assigned]
    selected_width = slots.width[assigned]
    selected_depth = slots.depth[assigned]
    selected_angle = slots.angle[assigned]
    footprint_metrics = _selected_footprint_metrics(
        selected_xy=selected_xy,
        selected_width=selected_width,
        selected_depth=selected_depth,
        selected_angle=selected_angle,
        selected_road=selected_road,
        road_specs=road_specs,
        global_nn=global_nn,
        args=args,
    )

    active_counts = np.bincount(selected_road, minlength=len(road_specs))
    road_id_by_temp: dict[int, int] = {}
    for temp_id, spec in enumerate(road_specs):
        count = int(active_counts[temp_id]) if temp_id < len(active_counts) else 0
        if count <= 0 and spec.kind not in {"arterial", "collector", "local"}:
            continue
        road_id_by_temp[temp_id] = _add_road(
            roads,
            ids,
            coords=spec.coords,
            kind=spec.kind,
            island_id=island_id,
            world_count=count,
            width=spec.width,
        )

    for local_i, row in enumerate(idx.tolist()):
        out["x"][row] = selected_xy[local_i, 0]
        out["y"][row] = selected_xy[local_i, 1]
        out["building_angle"][row] = selected_angle[local_i]
        out["building_width"][row] = selected_width[local_i]
        out["building_depth"][row] = selected_depth[local_i]
        out["building_height"][row] = args.default_building_height
        out["lot_id"][row] = ids.lot_id()
        out["block_id"][row] = local_block_ids[local_i]
        out["road_id"][row] = road_id_by_temp[int(selected_road[local_i])]
        out["frontage_x"][row] = selected_frontage[local_i, 0]
        out["frontage_y"][row] = selected_frontage[local_i, 1]

    return {
        "slots": int(len(slots.xy)),
        "generated_slots": generated_slots,
        "fallback_slots": fallback_slots,
        "active_roads": int(np.count_nonzero(active_counts)),
        "blocks": int(len(leaves)),
        "splits": int(splits),
        "building_scale": float(building_scale),
        "local_nn_q": float(local_nn_q),
        "slot_road_hit_candidates": int(np.count_nonzero(slot_road_hit)),
        **street_metrics,
        **footprint_metrics,
        **assignment_relax_metrics,
    }


def _layout_mesh_island(
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
) -> dict[str, int | float]:
    island_land, _info = build_land_geometry(
        xy,
        method="raster",
        raster_max_dim=args.road_land_raster_max_dim,
        raster_nn_cells=args.land_raster_nn_cells,
        raster_dilate_cells=args.land_raster_dilate_cells,
        raster_close_cells=args.land_raster_close_cells,
        raster_simplify_cells=args.land_raster_simplify_cells,
        raster_smooth_cells=args.land_raster_smooth_cells,
        raster_min_area_cells=args.land_raster_min_area_cells,
    )
    island_land = _smooth_coastline_geom(
        _safe_geom(island_land),
        iterations=args.land_chaikin_iterations,
        simplify=global_nn * args.land_chaikin_simplify_scale,
    )
    slot_step = max(
        global_nn * args.frontage_spacing_scale,
        global_nn * args.frontage_spacing_min_scale,
    )
    road_spacing = max(
        global_nn * args.road_spacing_min_scale,
        slot_step * args.recursive_road_spacing_slot_scale,
    )
    local_nn_q = _nn_quantile(xy, args.building_scale_nn_quantile)
    building_scale = float(
        np.clip(
            (local_nn_q / max(global_nn, 1e-9)) ** args.building_local_nn_power,
            args.building_local_scale_min,
            args.building_local_scale_max,
        )
    )
    road_specs, mesh_blocks, build_geom, street_metrics = _generate_mesh_streets(
        xy=xy,
        land_geom=island_land,
        island_id=island_id,
        global_nn=global_nn,
        slot_step=slot_step,
        args=args,
    )
    slot_result = _mesh_slots_for_blocks(
        mesh_blocks,
        road_specs,
        global_nn=global_nn,
        slot_step=slot_step,
        road_spacing=road_spacing,
        building_scale=building_scale,
        island_id=island_id,
        args=args,
    )
    slots = slot_result.slots
    slot_block = slot_result.block_index
    generated_slots = int(len(slots.xy))
    slot_road_hit = _slot_other_road_hit_mask(
        slots,
        road_specs=road_specs,
        global_nn=global_nn,
        args=args,
    )
    assignment_xy, assignment_relax_metrics = _relax_assignment_targets(
        xy,
        global_nn=global_nn,
        island_id=island_id,
        args=args,
    )
    if len(slots.xy):
        assigned, assign_metrics = _assign_slots_by_mesh_blocks(
            assignment_xy,
            slots,
            slot_block,
            mesh_blocks,
            global_nn=global_nn,
            args=args,
        )
    else:
        assigned = np.full(len(xy), -1, dtype=np.int64)
        assign_metrics = {
            "mesh_slot_shortage": int(len(xy)),
            "mesh_cross_block_assignments": 0,
            "mesh_unassigned_after_slots": int(len(xy)),
        }

    missing = np.flatnonzero(assigned < 0)
    fallback_slots = 0
    if len(missing):
        fallback_roads, fallback_slotset = _fallback_service_slots(
            xy=xy[missing],
            island_id=island_id,
            global_nn=global_nn,
            road_index_offset=len(road_specs),
            args=args,
        )
        road_specs.extend(fallback_roads)
        offset = len(slots.xy)
        fallback_assign = _assign_slots(
            assignment_xy[missing],
            fallback_slotset.xy,
            args.max_hungarian,
            candidate_k=args.assignment_candidate_k,
        )
        slots = _concat_slots([slots, fallback_slotset])
        slot_block = np.concatenate(
            [
                slot_block,
                np.full(len(fallback_slotset.xy), -1, dtype=np.int32),
            ]
        )
        if len(slot_road_hit):
            slot_road_hit = np.concatenate(
                [
                    slot_road_hit,
                    np.zeros(len(fallback_slotset.xy), dtype=bool),
                ]
            )
        else:
            slot_road_hit = np.zeros(len(slots.xy), dtype=bool)
        assigned[missing] = offset + fallback_assign
        fallback_slots = int(len(fallback_slotset.xy))
        assign_metrics["mesh_unassigned_after_slots"] = int(
            np.count_nonzero(assigned < 0)
        )

    selected_xy = slots.xy[assigned]
    selected_frontage = slots.frontage[assigned]
    selected_road = slots.road_index[assigned]
    selected_width = slots.width[assigned]
    selected_depth = slots.depth[assigned]
    selected_angle = slots.angle[assigned]
    selected_block = slot_block[assigned]
    footprint_metrics = _selected_footprint_metrics(
        selected_xy=selected_xy,
        selected_width=selected_width,
        selected_depth=selected_depth,
        selected_angle=selected_angle,
        selected_road=selected_road,
        road_specs=road_specs,
        global_nn=global_nn,
        args=args,
    )

    local_block_ids = np.full(len(xy), -1, dtype=np.int64)
    for block_no, block in enumerate(mesh_blocks):
        members = np.flatnonzero(selected_block == block_no)
        if len(members) == 0:
            continue
        bid = ids.block_id()
        local_block_ids[members] = bid
        blocks.append(
            Block(
                block_id=bid,
                island_id=island_id,
                geom=block.geom,
                target_lots=len(block.point_idx),
                assigned_worlds=len(members),
            )
        )
    fallback_members = np.flatnonzero(selected_block < 0)
    if len(fallback_members):
        pts = selected_xy[fallback_members]
        geom = _safe_geom(
            shapely.MultiPoint(pts).convex_hull.buffer(
                global_nn * args.fallback_building_scale,
                cap_style="round",
                join_style="round",
            )
        )
        bid = ids.block_id()
        local_block_ids[fallback_members] = bid
        blocks.append(
            Block(
                block_id=bid,
                island_id=island_id,
                geom=geom,
                target_lots=len(fallback_members),
                assigned_worlds=len(fallback_members),
            )
        )
    if np.any(local_block_ids < 0):
        bid = ids.block_id()
        missing_blocks = np.flatnonzero(local_block_ids < 0)
        local_block_ids[missing_blocks] = bid
        blocks.append(
            Block(
                block_id=bid,
                island_id=island_id,
                geom=_safe_geom(shapely.MultiPoint(selected_xy[missing_blocks]).convex_hull),
                target_lots=len(missing_blocks),
                assigned_worlds=len(missing_blocks),
            )
        )

    active_counts = np.bincount(selected_road, minlength=len(road_specs))
    road_id_by_temp: dict[int, int] = {}
    for temp_id, spec in enumerate(road_specs):
        count = int(active_counts[temp_id]) if temp_id < len(active_counts) else 0
        if count <= 0 and spec.kind not in {"arterial", "collector", "local"}:
            continue
        road_id_by_temp[temp_id] = _add_road(
            roads,
            ids,
            coords=spec.coords,
            kind=spec.kind,
            island_id=island_id,
            world_count=count,
            width=spec.width,
        )

    for local_i, row in enumerate(idx.tolist()):
        out["x"][row] = selected_xy[local_i, 0]
        out["y"][row] = selected_xy[local_i, 1]
        out["building_angle"][row] = selected_angle[local_i]
        out["building_width"][row] = selected_width[local_i]
        out["building_depth"][row] = selected_depth[local_i]
        out["building_height"][row] = args.default_building_height
        out["lot_id"][row] = ids.lot_id()
        out["block_id"][row] = local_block_ids[local_i]
        out["road_id"][row] = road_id_by_temp[int(selected_road[local_i])]
        out["frontage_x"][row] = selected_frontage[local_i, 0]
        out["frontage_y"][row] = selected_frontage[local_i, 1]

    return {
        "slots": int(len(slots.xy)),
        "generated_slots": generated_slots,
        "fallback_slots": fallback_slots,
        "active_roads": int(np.count_nonzero(active_counts)),
        "blocks": int(len(mesh_blocks)),
        "building_scale": float(building_scale),
        "local_nn_q": float(local_nn_q),
        "slot_road_hit_candidates": int(np.count_nonzero(slot_road_hit)),
        "mesh_slot_rejected_outside": int(slot_result.rejected_outside),
        "mesh_slot_rejected_roadless": int(slot_result.rejected_roadless),
        **street_metrics,
        **assign_metrics,
        **footprint_metrics,
        **assignment_relax_metrics,
    }


def _robust_extent(local: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(local) < 4:
        lo = local.min(axis=0)
        hi = local.max(axis=0)
    else:
        lo, hi = np.quantile(local, [0.03, 0.97], axis=0)
    span = np.maximum(hi - lo, 1e-9)
    return lo, hi + (span <= 1e-9) * 1e-9


def _assign_slots(
    local: np.ndarray,
    slots: np.ndarray,
    max_hungarian: int,
    *,
    slot_penalty: np.ndarray | None = None,
    penalty_cost: float = 0.0,
    candidate_k: int = 64,
) -> np.ndarray:
    n = len(local)
    penalty = None
    if slot_penalty is not None and penalty_cost > 0:
        penalty = np.asarray(slot_penalty, dtype=np.float64) * float(penalty_cost)
    if n <= max_hungarian:
        d2 = ((local[:, None, :] - slots[None, :, :]) ** 2).sum(axis=2)
        if penalty is not None:
            d2 = d2 + penalty[None, :]
        rows, cols = linear_sum_assignment(d2)
        out = np.empty(n, dtype=np.int64)
        out[rows] = cols
        return out

    k = min(max(1, int(candidate_k)), len(slots))
    dist, nbr = cKDTree(slots).query(local, k=k, workers=_spatial_workers())
    dist = np.asarray(dist)
    nbr = np.asarray(nbr)
    if dist.ndim == 1:
        dist = dist[:, None]
        nbr = nbr[:, None]
    point_idx = np.repeat(np.arange(n, dtype=np.int64), dist.shape[1])
    cost = dist.ravel() ** 2
    if penalty is not None:
        cost = cost + penalty[nbr.ravel()]
    order = np.argsort(cost, kind="stable")
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
        d, j = tree.query(
            local[remaining_points],
            k=min(k, len(remaining_slots)),
            workers=_spatial_workers(),
        )
        d = np.asarray(d)
        j = np.asarray(j)
        if d.ndim == 1:
            d = d[:, None]
            j = j[:, None]
        fill_point_idx = np.repeat(
            np.arange(len(remaining_points), dtype=np.int64),
            d.shape[1],
        )
        fill_cost = d.ravel() ** 2
        flat_slots = remaining_slots[j.ravel()]
        if penalty is not None:
            fill_cost = fill_cost + penalty[flat_slots]
        order = np.argsort(fill_cost, kind="stable")
        progressed = False
        for pos in order.tolist():
            p = int(remaining_points[int(fill_point_idx[pos])])
            s = int(flat_slots[pos])
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


def _slot_candidates_for_leaf(
    leaf: SplitParcel,
    slot_xy: np.ndarray,
    free: np.ndarray,
    *,
    buffer: float,
) -> np.ndarray:
    if not np.any(free):
        return np.empty(0, dtype=np.int64)
    minx, miny, maxx, maxy = leaf.geom.bounds
    mask = (
        free
        & (slot_xy[:, 0] >= minx - buffer)
        & (slot_xy[:, 0] <= maxx + buffer)
        & (slot_xy[:, 1] >= miny - buffer)
        & (slot_xy[:, 1] <= maxy + buffer)
    )
    cand = np.flatnonzero(mask)
    if len(cand) == 0:
        return cand
    geom = leaf.geom.buffer(buffer, join_style="round") if buffer > 0 else leaf.geom
    pts = shapely.points(slot_xy[cand, 0], slot_xy[cand, 1])
    return cand[np.asarray(shapely.covers(geom, pts), dtype=bool)]


def _assign_slots_by_leaves(
    xy: np.ndarray,
    slots_xy: np.ndarray,
    slot_penalty: np.ndarray,
    leaves: list[SplitParcel],
    *,
    global_nn: float,
    args,
) -> np.ndarray:
    out = np.full(len(xy), -1, dtype=np.int64)
    used = np.zeros(len(slots_xy), dtype=bool)
    buffer = global_nn * args.local_assignment_buffer_scale
    for leaf in sorted(leaves, key=lambda p: len(p.point_idx), reverse=True):
        members = leaf.point_idx
        if len(members) == 0:
            continue
        cand = _slot_candidates_for_leaf(leaf, slots_xy, ~used, buffer=buffer)
        if len(cand) < len(members):
            free = np.flatnonzero(~used)
            if len(free) == 0:
                break
            reps = min(
                len(free),
                max(
                    len(members),
                    int(
                        math.ceil(
                            len(members) * args.local_assignment_candidate_factor
                        )
                    ),
                ),
            )
            center = xy[members].mean(axis=0)
            dist = np.sum((slots_xy[free] - center) ** 2, axis=1)
            near = free[np.argpartition(dist, reps - 1)[:reps]]
            cand = np.unique(np.concatenate([cand, near]))
        if len(cand) == 0:
            continue
        if len(cand) < len(members):
            members = members[: len(cand)]
        local_assign = _assign_slots(
            xy[members],
            slots_xy[cand],
            args.max_hungarian,
            slot_penalty=slot_penalty[cand],
            penalty_cost=(
                global_nn * args.assignment_footprint_road_penalty_scale
            )
            ** 2,
            candidate_k=args.assignment_candidate_k,
        )
        chosen = cand[local_assign]
        out[members] = chosen
        used[chosen] = True

    missing = np.flatnonzero(out < 0)
    if len(missing):
        free = np.flatnonzero(~used)
        if len(free) < len(missing):
            raise RuntimeError(
                f"not enough generated slots: {len(slots_xy)} for {len(xy)} points"
            )
        fill = _assign_slots(
            xy[missing],
            slots_xy[free],
            args.max_hungarian,
            slot_penalty=slot_penalty[free],
            penalty_cost=(
                global_nn * args.assignment_footprint_road_penalty_scale
            )
            ** 2,
            candidate_k=args.assignment_candidate_k,
        )
        out[missing] = free[fill]
    return out


def _assign_slots_by_leaves_partial(
    xy: np.ndarray,
    slots_xy: np.ndarray,
    slot_penalty: np.ndarray,
    leaves: list[SplitParcel],
    *,
    global_nn: float,
    args,
) -> np.ndarray:
    out = np.full(len(xy), -1, dtype=np.int64)
    if len(slots_xy) == 0:
        return out
    used = np.zeros(len(slots_xy), dtype=bool)
    buffer = global_nn * args.local_assignment_buffer_scale
    for leaf in sorted(leaves, key=lambda p: len(p.point_idx), reverse=True):
        members = leaf.point_idx
        if len(members) == 0:
            continue
        cand = _slot_candidates_for_leaf(leaf, slots_xy, ~used, buffer=buffer)
        if len(cand) == 0:
            continue
        assign_members = members
        if len(cand) < len(members):
            dist, _nbr = cKDTree(slots_xy[cand]).query(
                xy[members],
                k=1,
                workers=_spatial_workers(),
            )
            keep = np.argsort(np.asarray(dist), kind="stable")[: len(cand)]
            assign_members = members[keep]
        local_assign = _assign_slots(
            xy[assign_members],
            slots_xy[cand],
            args.max_hungarian,
            slot_penalty=slot_penalty[cand],
            penalty_cost=(
                global_nn * args.assignment_footprint_road_penalty_scale
            )
            ** 2,
            candidate_k=args.assignment_candidate_k,
        )
        chosen = cand[local_assign]
        out[assign_members] = chosen
        used[chosen] = True

    missing = np.flatnonzero(out < 0)
    free = np.flatnonzero(~used)
    if len(missing) and len(free):
        dist, _nbr = cKDTree(slots_xy[free]).query(
            xy[missing],
            k=1,
            workers=_spatial_workers(),
        )
        fill_members = missing[
            np.argsort(np.asarray(dist), kind="stable")[: len(free)]
        ]
        fill = _assign_slots(
            xy[fill_members],
            slots_xy[free],
            args.max_hungarian,
            slot_penalty=slot_penalty[free],
            penalty_cost=(
                global_nn * args.assignment_footprint_road_penalty_scale
            )
            ** 2,
            candidate_k=args.assignment_candidate_k,
        )
        out[fill_members] = free[fill]
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


def _road_features(
    roads: list[Road],
    *,
    simplify: float = 0.0,
) -> list[dict]:
    feats = []
    for road in roads:
        if road.kind == "slot":
            continue
        line = LineString(road.coords)
        if simplify > 0 and len(line.coords) > 3:
            line = line.simplify(simplify, preserve_topology=False)
            if line.is_empty or line.length <= 1e-9:
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


def _block_shape_metrics(blocks: list[Block]) -> dict[str, float]:
    aspects = np.asarray(
        [
            _polygon_aspect_ratio(block.geom)
            for block in blocks
            if not block.geom.is_empty and block.geom.area > 0
        ],
        dtype=np.float64,
    )
    if len(aspects) == 0:
        return {
            "block_aspect_median": 0.0,
            "block_aspect_p90": 0.0,
            "block_aspect_p95": 0.0,
            "block_aspect_p99": 0.0,
            "block_aspect_max": 0.0,
        }
    return {
        "block_aspect_median": float(np.median(aspects)),
        "block_aspect_p90": float(np.quantile(aspects, 0.90)),
        "block_aspect_p95": float(np.quantile(aspects, 0.95)),
        "block_aspect_p99": float(np.quantile(aspects, 0.99)),
        "block_aspect_max": float(np.max(aspects)),
    }


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
    simplify: float,
) -> dict[str, float | int]:
    developed = shapely.unary_union([b.geom for b in blocks if not b.geom.is_empty])
    developed = _safe_geom(developed.intersection(land_geom))
    open_space = _safe_geom(land_geom.difference(developed.buffer(park_buffer)))
    if simplify > 0:
        developed = _safe_geom(developed.simplify(simplify, preserve_topology=True))
        open_space = _safe_geom(open_space.simplify(simplify, preserve_topology=True))
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


def _empty_layout_columns(n: int) -> dict[str, np.ndarray]:
    return {
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


def _layout_island_worker(payload) -> IslandLayoutResult:
    island_id, idx, island_xy, global_nn, args = payload
    _set_spatial_workers(1)
    local_out = _empty_layout_columns(len(island_xy))
    roads: list[Road] = []
    blocks: list[Block] = []
    layout_fn = (
        _layout_mesh_island
        if args.layout_engine == "mesh"
        else _layout_streamline_island
    )
    info = layout_fn(
        idx=np.arange(len(island_xy), dtype=np.int64),
        xy=island_xy,
        island_id=island_id,
        global_nn=global_nn,
        args=args,
        ids=Ids(),
        roads=roads,
        blocks=blocks,
        out=local_out,
    )
    return IslandLayoutResult(
        idx=idx,
        island_id=island_id,
        columns=local_out,
        roads=roads,
        blocks=blocks,
        info=info,
    )


def _remap_island_result(
    result: IslandLayoutResult,
    *,
    ids: Ids,
    out: dict[str, np.ndarray],
    roads: list[Road],
    blocks: list[Block],
) -> dict[str, int | float]:
    road_offset = ids.road
    block_offset = ids.block
    lot_offset = ids.lot

    local_road_ids = [road.road_id for road in result.roads]
    local_block_ids = [block.block_id for block in result.blocks]
    road_count = (max(local_road_ids) + 1) if local_road_ids else 0
    block_count = (max(local_block_ids) + 1) if local_block_ids else 0
    lot_count = int(np.nanmax(result.columns["lot_id"]) + 1) if len(result.idx) else 0

    for road in result.roads:
        roads.append(
            Road(
                road_id=road.road_id + road_offset,
                coords=road.coords,
                kind=road.kind,
                island_id=road.island_id,
                world_count=road.world_count,
                width=road.width,
            )
        )
    for block in result.blocks:
        blocks.append(
            Block(
                block_id=block.block_id + block_offset,
                island_id=block.island_id,
                geom=block.geom,
                target_lots=block.target_lots,
                assigned_worlds=block.assigned_worlds,
            )
        )

    columns = {name: values.copy() for name, values in result.columns.items()}
    for name, offset in (
        ("road_id", road_offset),
        ("block_id", block_offset),
        ("lot_id", lot_offset),
    ):
        mask = columns[name] >= 0
        columns[name][mask] += offset
    for name, values in columns.items():
        out[name][result.idx] = values

    ids.road += road_count
    ids.block += block_count
    ids.lot += lot_count
    return result.info


def _city_layout(
    points: pl.DataFrame, *, top_col: str, sub_col: str, args
) -> tuple[pl.DataFrame, list[Road], list[Block], dict]:
    n = points.height
    xy = points.select("x", "y").to_numpy().astype(np.float64)
    global_nn = _median_nn(xy)
    print(f"  global median nn={global_nn:.5f}")
    out = _empty_layout_columns(n)
    roads: list[Road] = []
    blocks: list[Block] = []
    ids = Ids()
    island_metrics = []

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
        .sort(top_col)
    )
    island_rows = list(
        work.group_by(top_col).len().sort("len", descending=True).iter_rows(
            named=True,
        )
    )
    payloads = []
    for row in island_rows:
        island_id = int(row[top_col])
        island = work.filter(pl.col(top_col) == island_id)
        idx = island["_idx"].to_numpy()
        island_xy = island.select("x", "y").to_numpy().astype(np.float64)
        payloads.append((island_id, idx, island_xy, global_nn, args))

    if args.layout_workers == 0:
        layout_workers = min(
            len(payloads),
            max(1, os.cpu_count() or 1),
        )
    else:
        layout_workers = max(1, min(int(args.layout_workers), len(payloads)))

    if layout_workers > 1:
        print(f"  layout workers={layout_workers}")
        with ProcessPoolExecutor(max_workers=layout_workers) as executor:
            results = list(executor.map(_layout_island_worker, payloads))
    else:
        results = [_layout_island_worker(payload) for payload in payloads]

    for result in results:
        info = _remap_island_result(
            result,
            ids=ids,
            out=out,
            roads=roads,
            blocks=blocks,
        )
        island_metrics.append(
            {"island_id": result.island_id, "worlds": len(result.idx), **info}
        )
        print(
            f"    island {result.island_id}: {len(result.idx):,} worlds, "
            f"{info['active_roads']:,} roads, {info['slots']:,} slots"
        )

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
        "layout_engine": args.layout_engine,
        "global_median_nn": float(global_nn),
        "point_count": int(out_points.height),
        "processed_islands": sorted(island_set),
        "road_count": int(len(roads)),
        "block_count": int(len(blocks)),
        "displacement_median": float(np.median(displacement)),
        "displacement_p95": float(np.quantile(displacement, 0.95)),
        "displacement_p99": float(np.quantile(displacement, 0.99)),
        "frontage_coverage": 1.0,
        "islands": island_metrics,
    }
    metrics.update(_block_shape_metrics(blocks))
    return out_points, roads, blocks, metrics


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--max-islands", type=int, default=3)
    ap.add_argument("--layout-engine", choices=["mesh", "planar"], default="mesh")
    ap.add_argument("--layout-workers", type=int, default=0)
    ap.add_argument("--spatial-workers", type=int, default=-1)
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
    ap.add_argument("--building-width-scale", type=float, default=0.64)
    ap.add_argument("--building-depth-scale", type=float, default=0.70)
    ap.add_argument("--building-depth-road-spacing-scale", type=float, default=0.24)
    ap.add_argument("--building-setback-scale", type=float, default=0.55)
    ap.add_argument("--building-scale-nn-quantile", type=float, default=0.90)
    ap.add_argument("--building-local-nn-power", type=float, default=0.52)
    ap.add_argument("--building-local-scale-min", type=float, default=0.94)
    ap.add_argument("--building-local-scale-max", type=float, default=1.35)
    ap.add_argument("--building-density-knn", type=int, default=12)
    ap.add_argument("--building-density-reference-quantile", type=float, default=0.50)
    ap.add_argument("--building-density-reference-sample", type=int, default=12000)
    ap.add_argument("--building-density-scale-power", type=float, default=0.0)
    ap.add_argument("--building-density-scale-min", type=float, default=0.74)
    ap.add_argument("--building-density-scale-max", type=float, default=1.18)
    ap.add_argument("--building-width-jitter", type=float, default=0.09)
    ap.add_argument("--building-depth-jitter", type=float, default=0.12)
    ap.add_argument("--building-angle-jitter", type=float, default=0.018)
    ap.add_argument("--sparse-building-scale", type=float, default=1.2)
    ap.add_argument("--sparse-building-max-scale", type=float, default=3.0)
    ap.add_argument("--fallback-building-scale", type=float, default=0.85)
    ap.add_argument("--default-building-height", type=float, default=0.08)
    ap.add_argument("--arterial-road-width", type=float, default=2.2)
    ap.add_argument("--local-road-width", type=float, default=1.0)
    ap.add_argument("--collector-road-width", type=float, default=1.6)
    ap.add_argument("--service-road-width", type=float, default=0.6)
    ap.add_argument("--collector-knn", type=int, default=4)
    ap.add_argument("--collector-every", type=int, default=7)
    ap.add_argument("--road-land-raster-max-dim", type=int, default=1536)
    ap.add_argument("--road-spacing-min-scale", type=float, default=3.2)
    ap.add_argument("--road-spacing-max-scale", type=float, default=9.0)
    ap.add_argument("--road-spacing-retry-scale", type=float, default=0.78)
    ap.add_argument("--max-road-attempts", type=int, default=6)
    ap.add_argument("--frontage-spacing-scale", type=float, default=0.44)
    ap.add_argument("--frontage-spacing-min-scale", type=float, default=0.36)
    ap.add_argument("--slot-capacity-target", type=float, default=2.2)
    ap.add_argument("--slot-capacity-min", type=float, default=1.0)
    ap.add_argument("--slot-filter-min-global-scale", type=float, default=0.28)
    ap.add_argument("--slot-filter-building-scale", type=float, default=0.88)
    ap.add_argument("--slot-filter-footprint-radius-scale", type=float, default=0.80)
    ap.add_argument("--slot-filter-capacity-fallback", action="store_true")
    ap.add_argument(
        "--slot-footprint-validation",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    ap.add_argument("--slot-footprint-road-clearance-scale", type=float, default=0.12)
    ap.add_argument("--slot-footprint-overlap-query-quantile", type=float, default=0.99)
    ap.add_argument("--slot-footprint-overlap-query-scale", type=float, default=1.05)
    ap.add_argument("--road-slot-end-gap-scale", type=float, default=0.9)
    ap.add_argument("--road-extent-quantile", type=float, default=0.006)
    ap.add_argument("--road-extent-pad-scale", type=float, default=4.0)
    ap.add_argument("--road-jitter-scale", type=float, default=0.17)
    ap.add_argument("--road-curve-amplitude-scale", type=float, default=0.48)
    ap.add_argument("--road-curve-wavelength-scale", type=float, default=18.0)
    ap.add_argument("--road-curve-max-vertices", type=int, default=420)
    ap.add_argument("--road-simplify-scale", type=float, default=1.2)
    ap.add_argument("--road-smooth-resample-scale", type=float, default=1.15)
    ap.add_argument("--road-smooth-max-vertices", type=int, default=2200)
    ap.add_argument("--road-smooth-simplify-scale", type=float, default=0.0)
    ap.add_argument("--road-smooth-arterial-iterations", type=int, default=4)
    ap.add_argument("--road-smooth-collector-iterations", type=int, default=0)
    ap.add_argument("--road-smooth-local-iterations", type=int, default=1)
    ap.add_argument("--road-smooth-service-iterations", type=int, default=1)
    ap.add_argument("--road-curvature-relax-iterations", type=int, default=12)
    ap.add_argument("--road-curvature-relax-strength", type=float, default=0.50)
    ap.add_argument("--road-curvature-arterial-max-turn-deg", type=float, default=8.0)
    ap.add_argument("--road-curvature-collector-max-turn-deg", type=float, default=28.0)
    ap.add_argument("--road-curvature-local-max-turn-deg", type=float, default=36.0)
    ap.add_argument("--road-curvature-service-max-turn-deg", type=float, default=42.0)
    ap.add_argument("--road-lod-simplify-scale", type=float, default=0.95)
    ap.add_argument("--road-lod-mid-simplify-scale", type=float, default=0.28)
    ap.add_argument("--major-road-bridge-scale", type=float, default=8.0)
    ap.add_argument("--road-endpoint-connector-min-scale", type=float, default=0.80)
    ap.add_argument("--road-endpoint-connector-max-scale", type=float, default=12.0)
    ap.add_argument("--major-corridor-clearance-scale", type=float, default=0.30)
    ap.add_argument(
        "--major-corridor-building-clearance-scale",
        type=float,
        default=0.45,
    )
    ap.add_argument("--road-corridor-clearance-scale", type=float, default=0.0)
    ap.add_argument(
        "--road-corridor-building-clearance-scale",
        type=float,
        default=0.0,
    )
    ap.add_argument("--min-road-length-slots", type=float, default=6.0)
    ap.add_argument("--service-chunk-worlds", type=int, default=48)
    ap.add_argument("--block-target-worlds", type=int, default=96)
    ap.add_argument("--block-buffer-scale", type=float, default=1.35)
    ap.add_argument("--recursive-target-block-worlds", type=int, default=20)
    ap.add_argument("--recursive-min-split-worlds", type=int, default=18)
    ap.add_argument("--recursive-max-depth", type=int, default=16)
    ap.add_argument("--recursive-max-splits", type=int, default=6000)
    ap.add_argument("--recursive-split-search", type=int, default=12)
    ap.add_argument("--recursive-split-attempts", type=int, default=7)
    ap.add_argument("--recursive-max-child-frac", type=float, default=0.97)
    ap.add_argument("--recursive-min-parcel-area-scale", type=float, default=25.0)
    ap.add_argument("--recursive-min-split-width-scale", type=float, default=1.4)
    ap.add_argument("--recursive-split-margin-frac", type=float, default=0.14)
    ap.add_argument("--recursive-min-split-margin-scale", type=float, default=1.2)
    ap.add_argument("--recursive-alternate-ratio", type=float, default=1.35)
    ap.add_argument("--recursive-collector-depth", type=int, default=3)
    ap.add_argument("--recursive-capacity-efficiency", type=float, default=0.72)
    ap.add_argument("--recursive-road-spacing-slot-scale", type=float, default=3.0)
    ap.add_argument("--recursive-curve-span-scale", type=float, default=0.045)
    ap.add_argument("--recursive-curve-global-scale", type=float, default=3.5)
    ap.add_argument("--recursive-curve-wavelength-scale", type=float, default=0.65)
    ap.add_argument("--planar-target-block-worlds", type=int, default=14)
    ap.add_argument("--planar-target-block-area-scale", type=float, default=70.0)
    ap.add_argument("--planar-area-stop-scale", type=float, default=1.35)
    ap.add_argument("--planar-area-split-scale", type=float, default=1.15)
    ap.add_argument("--planar-area-split-min-worlds", type=int, default=6)
    ap.add_argument("--planar-area-split-score-weight", type=float, default=2.5)
    ap.add_argument("--planar-min-parcel-area-scale", type=float, default=5.5)
    ap.add_argument("--planar-min-split-width-scale", type=float, default=0.35)
    ap.add_argument("--planar-split-margin-frac", type=float, default=0.08)
    ap.add_argument("--planar-min-split-margin-scale", type=float, default=0.35)
    ap.add_argument("--planar-min-split-road-length-slots", type=float, default=1.2)
    ap.add_argument("--planar-max-child-frac", type=float, default=0.995)
    ap.add_argument("--planar-collector-depth", type=int, default=3)
    ap.add_argument("--planar-service-depth", type=int, default=7)
    ap.add_argument("--planar-split-families", type=int, default=2)
    ap.add_argument("--planar-split-candidates-per-family", type=int, default=5)
    ap.add_argument("--planar-quality-size-weight", type=float, default=0.28)
    ap.add_argument("--planar-quality-count-weight", type=float, default=0.24)
    ap.add_argument("--planar-quality-regular-weight", type=float, default=0.40)
    ap.add_argument("--planar-quality-aspect-weight", type=float, default=0.55)
    ap.add_argument("--planar-quality-length-weight", type=float, default=0.08)
    ap.add_argument("--planar-quality-axis-weight", type=float, default=0.14)
    ap.add_argument("--planar-target-child-aspect", type=float, default=4.0)
    ap.add_argument("--planar-force-crosscut-ratio", type=float, default=2.35)
    ap.add_argument("--planar-axis-split-depth", type=int, default=0)
    ap.add_argument("--planar-axis-split-min-worlds", type=int, default=2500)
    ap.add_argument("--planar-axis-split-min-aspect", type=float, default=1.18)
    ap.add_argument("--planar-streamline-curve-span-scale", type=float, default=0.060)
    ap.add_argument("--planar-streamline-curve-global-scale", type=float, default=3.4)
    ap.add_argument("--planar-streamline-wavelength-scale", type=float, default=0.82)
    ap.add_argument(
        "--planar-streamline-secondary-curve-scale",
        type=float,
        default=0.28,
    )
    ap.add_argument(
        "--planar-density-isoline-enabled",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    ap.add_argument("--planar-density-min-worlds", type=int, default=2500)
    ap.add_argument("--planar-density-knn", type=int, default=32)
    ap.add_argument("--planar-density-center-quantile", type=float, default=0.08)
    ap.add_argument("--planar-density-max-centers", type=int, default=14)
    ap.add_argument("--planar-density-center-min-sep-scale", type=float, default=24.0)
    ap.add_argument("--planar-density-centers-per-parcel", type=int, default=2)
    ap.add_argument("--planar-density-isoline-max-depth", type=int, default=8)
    ap.add_argument("--planar-density-center-reach-scale", type=float, default=1.15)
    ap.add_argument("--planar-density-center-reach-min-scale", type=float, default=36.0)
    ap.add_argument("--planar-density-min-radius-scale", type=float, default=5.0)
    ap.add_argument("--planar-density-isoline-curve-strength", type=float, default=0.75)
    ap.add_argument("--planar-density-isoline-max-curve-frac", type=float, default=0.32)
    ap.add_argument("--planar-density-isoline-offsets", type=int, default=1)
    ap.add_argument("--planar-density-isoline-offset-frac", type=float, default=0.18)
    ap.add_argument("--mesh-density-min-worlds", type=int, default=1800)
    ap.add_argument("--mesh-density-knn", type=int, default=36)
    ap.add_argument("--mesh-density-quantile", type=float, default=0.11)
    ap.add_argument("--mesh-density-max-centers", type=int, default=18)
    ap.add_argument("--mesh-density-center-min-sep-scale", type=float, default=22.0)
    ap.add_argument("--mesh-center-min-worlds", type=int, default=900)
    ap.add_argument("--mesh-center-radius-quantile", type=float, default=0.76)
    ap.add_argument("--mesh-city-min-radius-scale", type=float, default=9.0)
    ap.add_argument("--mesh-contour-min-worlds", type=int, default=6500)
    ap.add_argument("--mesh-contour-min-area-scale", type=float, default=1800.0)
    ap.add_argument("--mesh-contour-spacing-scale", type=float, default=13.0)
    ap.add_argument("--mesh-contour-slot-spacing-scale", type=float, default=30.0)
    ap.add_argument("--mesh-contour-max-offset-frac", type=float, default=0.22)
    ap.add_argument("--mesh-contour-max-rings", type=int, default=2)
    ap.add_argument("--mesh-contour-collector-every", type=int, default=2)
    ap.add_argument("--mesh-downtown-min-worlds", type=int, default=900)
    ap.add_argument("--mesh-downtown-radius-quantile", type=float, default=0.42)
    ap.add_argument("--mesh-downtown-min-radius-frac", type=float, default=1.35)
    ap.add_argument("--mesh-downtown-grid-worlds-scale", type=float, default=38.0)
    ap.add_argument("--mesh-downtown-min-lines", type=int, default=3)
    ap.add_argument("--mesh-downtown-max-lines", type=int, default=8)
    ap.add_argument("--mesh-downtown-grid-extent-frac", type=float, default=0.78)
    ap.add_argument("--mesh-ring-worlds-scale", type=float, default=70.0)
    ap.add_argument("--mesh-max-city-rings", type=int, default=2)
    ap.add_argument("--mesh-ring-inner-quantile", type=float, default=0.66)
    ap.add_argument("--mesh-ring-outer-quantile", type=float, default=0.88)
    ap.add_argument("--mesh-ring-max-aspect", type=float, default=2.8)
    ap.add_argument("--mesh-ring-min-downtown-gap", type=float, default=1.22)
    ap.add_argument("--mesh-ring-vertices", type=int, default=192)
    ap.add_argument("--mesh-spoke-worlds-scale", type=float, default=85.0)
    ap.add_argument("--mesh-min-spokes", type=int, default=4)
    ap.add_argument("--mesh-max-spokes", type=int, default=8)
    ap.add_argument("--mesh-spoke-reach-scale", type=float, default=1.28)
    ap.add_argument("--mesh-spoke-collector-every", type=int, default=4)
    ap.add_argument("--mesh-spoke-start-ring-frac", type=float, default=0.98)
    ap.add_argument("--mesh-spoke-min-end-frac", type=float, default=1.55)
    ap.add_argument("--mesh-axis-min-worlds", type=int, default=10000)
    ap.add_argument("--mesh-cross-axis-min-worlds", type=int, default=26000)
    ap.add_argument("--mesh-min-block-area-scale", type=float, default=5.0)
    ap.add_argument("--mesh-target-block-area-scale", type=float, default=42.0)
    ap.add_argument("--mesh-area-refine-min-worlds", type=int, default=12)
    ap.add_argument("--mesh-area-refine-score-scale", type=float, default=10.0)
    ap.add_argument("--mesh-capacity-spacing-scale", type=float, default=1.25)
    ap.add_argument("--mesh-block-frontage-efficiency", type=float, default=0.62)
    ap.add_argument("--mesh-capacity-target", type=float, default=1.0)
    ap.add_argument(
        "--mesh-progress",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    ap.add_argument("--mesh-refine-rounds", type=int, default=8)
    ap.add_argument("--mesh-refine-batch", type=int, default=240)
    ap.add_argument("--mesh-refine-min-worlds", type=int, default=8)
    ap.add_argument("--mesh-refine-local-density", type=float, default=0.38)
    ap.add_argument("--mesh-split-curve-strength", type=float, default=0.55)
    ap.add_argument("--mesh-split-curve-max-frac", type=float, default=0.18)
    ap.add_argument("--mesh-split-tensor-bonus", type=float, default=0.10)
    ap.add_argument("--mesh-split-min-count-balance", type=float, default=0.18)
    ap.add_argument("--mesh-target-block-aspect", type=float, default=3.2)
    ap.add_argument("--mesh-frontage-match-scale", type=float, default=0.55)
    ap.add_argument("--mesh-slot-phase", type=float, default=0.5)
    ap.add_argument("--mesh-slot-filter-building-scale", type=float, default=0.78)
    ap.add_argument("--mesh-slot-filter-radius-scale", type=float, default=0.86)
    ap.add_argument("--planar-boundary-resample-scale", type=float, default=2.35)
    ap.add_argument("--planar-min-noded-segment-scale", type=float, default=0.08)
    ap.add_argument("--planar-source-match-tolerance-scale", type=float, default=0.12)
    ap.add_argument("--planar-topology-merge-scale", type=float, default=0.55)
    ap.add_argument("--planar-topology-short-edge-scale", type=float, default=0.85)
    ap.add_argument("--planar-topology-max-move-scale", type=float, default=1.8)
    ap.add_argument(
        "--planar-topology-endpoint-max-turn-deg",
        type=float,
        default=70.0,
    )
    ap.add_argument("--planar-junction-snap-scale", type=float, default=2.05)
    ap.add_argument("--planar-junction-merge-scale", type=float, default=1.85)
    ap.add_argument("--planar-local-junction-snap-scale", type=float, default=1.75)
    ap.add_argument("--planar-local-junction-merge-scale", type=float, default=1.55)
    ap.add_argument("--planar-service-junction-snap-scale", type=float, default=1.15)
    ap.add_argument("--planar-service-junction-merge-scale", type=float, default=0.95)
    ap.add_argument("--planar-major-junction-max-turn-deg", type=float, default=55.0)
    ap.add_argument("--planar-local-junction-max-turn-deg", type=float, default=28.0)
    ap.add_argument("--planar-service-junction-max-turn-deg", type=float, default=22.0)
    ap.add_argument("--planar-boundary-ring-min-worlds", type=int, default=650)
    ap.add_argument("--planar-boundary-ring-min-area-scale", type=float, default=1200.0)
    ap.add_argument("--planar-coastal-inset-min-area-scale", type=float, default=4.0)
    ap.add_argument("--planar-coastal-inset-min-frac", type=float, default=2.0)
    ap.add_argument("--coastal-road-offset-scale", type=float, default=4.2)
    ap.add_argument("--coastal-road-soften-scale", type=float, default=2.4)
    ap.add_argument("--coastal-arterial-min-worlds", type=int, default=350)
    ap.add_argument("--coastal-arterial-min-area-scale", type=float, default=900.0)
    ap.add_argument("--boundary-road-min-length-slots", type=float, default=2.2)
    ap.add_argument("--boundary-trunk-snap-scale", type=float, default=1.4)
    ap.add_argument("--boundary-trunk-overlap-frac", type=float, default=0.42)
    ap.add_argument("--access-lane-trigger-scale", type=float, default=0.50)
    ap.add_argument("--access-lane-density-scale", type=float, default=999.0)
    ap.add_argument("--access-lane-local-density-scale", type=float, default=4.0)
    ap.add_argument("--access-lane-max-per-block", type=int, default=40)
    ap.add_argument("--access-lane-margin-frac", type=float, default=0.18)
    ap.add_argument("--access-lane-min-spacing-scale", type=float, default=0.20)
    ap.add_argument("--access-lane-min-spacing-slot-scale", type=float, default=0.24)
    ap.add_argument("--access-lane-curve-span-scale", type=float, default=0.045)
    ap.add_argument("--access-lane-curve-global-scale", type=float, default=1.60)
    ap.add_argument("--access-lane-curve-wavelength-scale", type=float, default=0.55)
    ap.add_argument("--access-lane-curve-vertices", type=int, default=56)
    ap.add_argument("--local-assignment-buffer-scale", type=float, default=2.5)
    ap.add_argument("--local-assignment-candidate-factor", type=float, default=2.0)
    ap.add_argument("--assignment-candidate-k", type=int, default=96)
    ap.add_argument(
        "--assignment-footprint-road-penalty-scale",
        type=float,
        default=4.0,
    )
    ap.add_argument("--assignment-relax-iterations", type=int, default=14)
    ap.add_argument("--assignment-relax-min-dist-scale", type=float, default=1.25)
    ap.add_argument("--assignment-relax-strength", type=float, default=0.42)
    ap.add_argument("--assignment-relax-anchor", type=float, default=0.04)
    ap.add_argument("--assignment-relax-max-step-scale", type=float, default=0.65)
    ap.add_argument(
        "--assignment-relax-max-displacement-scale",
        type=float,
        default=8.0,
    )
    ap.add_argument("--park-buffer-scale", type=float, default=0.8)
    ap.add_argument("--land-raster-max-dim", type=int, default=2048)
    ap.add_argument("--land-raster-nn-cells", type=float, default=2.0)
    ap.add_argument("--land-raster-dilate-cells", type=int, default=8)
    ap.add_argument("--land-raster-close-cells", type=int, default=3)
    ap.add_argument("--land-raster-simplify-cells", type=float, default=0.75)
    ap.add_argument("--land-raster-smooth-cells", type=float, default=1.0)
    ap.add_argument("--land-chaikin-iterations", type=int, default=3)
    ap.add_argument("--land-chaikin-simplify-scale", type=float, default=0.55)
    ap.add_argument("--land-union-min-area-scale", type=float, default=80.0)
    ap.add_argument("--landuse-min-area-scale", type=float, default=20.0)
    ap.add_argument("--landuse-simplify-scale", type=float, default=3.0)
    ap.add_argument("--land-raster-min-area-cells", type=float, default=4.0)
    ap.add_argument("--region-raster-max-dim", type=int, default=2048)
    ap.add_argument("--region-raster-nn-cells", type=float, default=2.0)
    ap.add_argument("--region-raster-close-cells", type=int, default=1)
    ap.add_argument("--region-raster-simplify-cells", type=float, default=0.75)
    ap.add_argument("--region-raster-smooth-cells", type=float, default=0.0)
    ap.add_argument("--region-raster-min-area-cells", type=float, default=4.0)
    ap.add_argument("--region-buffer-scale", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    _set_spatial_workers(args.spatial_workers)

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
    land_geom = _filter_geom(
        land_geom,
        (global_nn * global_nn) * args.land_union_min_area_scale,
    )
    land_geom = _smooth_coastline_geom(
        land_geom,
        iterations=args.land_chaikin_iterations,
        simplify=global_nn * args.land_chaikin_simplify_scale,
    )
    land_info = {
        **orig_land_info,
        "method": "city_union",
        "original_polygon_count": int(orig_land_info["polygon_count"]),
        "layout_polygon_count": int(layout_land_info["polygon_count"]),
        "polygon_count": int(len(list(iter_polygons(land_geom)))),
        "chaikin_iterations": int(args.land_chaikin_iterations),
        "chaikin_simplify": float(global_nn * args.land_chaikin_simplify_scale),
        "union_min_area": float(
            (global_nn * global_nn) * args.land_union_min_area_scale
        ),
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
        min_area=(global_nn * global_nn) * args.landuse_min_area_scale,
        park_buffer=global_nn * args.park_buffer_scale,
        simplify=global_nn * args.landuse_simplify_scale,
    )
    metrics.update(landuse_metrics)
    _write_geojson(
        _road_features(roads, simplify=global_nn * args.road_lod_simplify_scale),
        args.out_dir / "roads.geojson",
    )
    _write_geojson(
        _road_features(roads, simplify=global_nn * args.road_lod_mid_simplify_scale),
        args.out_dir / "roads_mid.geojson",
    )
    _write_geojson(_road_features(roads), args.out_dir / "roads_near.geojson")
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
        "layout": (
            "city-mesh-v2" if args.layout_engine == "mesh" else "city-planar-v20"
        ),
    }
    assets = dict(out_manifest.get("assets") or {})
    assets.update(
        {
            "points": "app_points.parquet",
            "meta": "worlds_meta.parquet",
            "land": "land.geojson",
            "landuse": "landuse.geojson",
            "roads": "roads.geojson",
            "roads_mid": "roads_mid.geojson",
            "roads_near": "roads_near.geojson",
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
