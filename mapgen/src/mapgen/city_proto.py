"""Artifact-first compressed city prototype generator.

The prototype operates on deterministic inspection crops from a full app export.
It writes static render sheets, GeoJSON geometry, Parquet parcel/building tables,
and metrics before any web wiring is attempted.
"""
# ruff: noqa: E501

from __future__ import annotations

import argparse
import json
import math
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import networkx as nx
import numpy as np
import polars as pl
import shapely
from scipy import ndimage
from scipy.optimize import linear_sum_assignment
from scipy.spatial import Delaunay, QhullError, cKDTree
from shapely import GeometryCollection, LineString, Point, Polygon
from shapely.ops import unary_union
from sklearn.cluster import DBSCAN, MiniBatchKMeans

from mapgen.land import build_land_geometry


@dataclass(frozen=True)
class CropSpec:
    crop_id: str
    kind: str
    center: tuple[float, float]
    radius: float
    island_id: int


@dataclass
class RoadRec:
    road_id: int
    crop_id: str
    kind: str
    geom: LineString
    bridge: bool = False


@dataclass(frozen=True)
class SettlementRec:
    settlement_id: int
    island_id: int
    center: tuple[float, float]
    world_count: int
    settlement_class: str


@dataclass
class BlockRec:
    block_id: int
    crop_id: str
    geom: Polygon


@dataclass
class ZoneRec:
    zone_id: int
    crop_id: str
    zone_kind: str
    geom: Polygon
    world_count: int
    density: float
    sparse_score: float
    z_range: float


@dataclass
class LotRec:
    lot_id: int
    crop_id: str
    block_id: int
    geom: Polygon
    centroid: tuple[float, float]
    road_angle: float


@dataclass(frozen=True)
class TerrainModel:
    center: tuple[float, float]
    axes: tuple[tuple[float, float], tuple[float, float]]
    lo: tuple[float, float]
    span: tuple[float, float]
    relief: float
    valley_strength: float
    seed: int

    def _local(self, x: float, y: float) -> tuple[float, float]:
        cx, cy = self.center
        ax0 = np.asarray(self.axes[0], dtype=np.float64)
        ax1 = np.asarray(self.axes[1], dtype=np.float64)
        delta = np.array([x - cx, y - cy], dtype=np.float64)
        return float(delta @ ax0), float(delta @ ax1)

    def sample_local(self, lx: float, ly: float) -> float:
        sx = max(self.span[0], 1e-9)
        sy = max(self.span[1], 1e-9)
        u = (lx - self.lo[0]) / sx
        v = (ly - self.lo[1]) / sy
        relief = float(self.relief)
        phase = float(self.seed) * 0.137
        n = (
            0.48 * math.sin((u * 2.2 + v * 0.45 + phase) * math.tau)
            + 0.31 * math.cos((u * 0.75 - v * 1.85 + phase * 0.7) * math.tau)
            + 0.17 * math.sin((u * 5.0 + v * 3.1 + phase * 1.9) * math.tau)
            + 0.09 * math.cos((u * 9.5 - v * 7.0 + phase * 2.7) * math.tau)
        )
        # Broad valley: the semantic/density center remains lower and flatter,
        # while sparse outskirts pick up more relief.
        du = u - 0.5
        dv = v - 0.5
        valley = (du * du + dv * dv) * self.valley_strength
        return relief * (0.50 * n + valley)

    def sample(self, x: float, y: float) -> float:
        lx, ly = self._local(x, y)
        return self.sample_local(lx, ly)

    def grade_between(self, a: tuple[float, float], b: tuple[float, float]) -> float:
        dx = float(np.linalg.norm(np.asarray(a) - np.asarray(b)))
        if dx <= 1e-9:
            return 0.0
        dz = self.sample(*b) - self.sample(*a)
        return abs(float(dz) / dx)


def _median_nn(xy: np.ndarray) -> float:
    if len(xy) < 2:
        return 1.0
    d, _idx = cKDTree(xy).query(xy, k=2, workers=-1)
    return float(np.median(d[:, 1]))


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


def _to_world(local: np.ndarray, center: np.ndarray, axes: np.ndarray) -> np.ndarray:
    return center + local @ axes.T


def _rect_poly(
    cx: float,
    cy: float,
    width: float,
    height: float,
    center: np.ndarray,
    axes: np.ndarray,
) -> Polygon:
    hw = width * 0.5
    hh = height * 0.5
    pts = np.array(
        [
            [cx - hw, cy - hh],
            [cx + hw, cy - hh],
            [cx + hw, cy + hh],
            [cx - hw, cy + hh],
        ],
        dtype=np.float64,
    )
    return Polygon(_to_world(pts, center, axes))


def _feature_collection(features: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": features}


def _geom_feature(geom, props: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "properties": props,
        "geometry": json.loads(shapely.to_geojson(geom)),
    }


def _write_geojson(features: list[dict[str, Any]], path: Path) -> None:
    path.write_text(json.dumps(_feature_collection(features)))
    print(f"  wrote {path} ({len(features):,} features)")


def _assign_islands(xy: np.ndarray, eps_scale: float) -> np.ndarray:
    if len(xy) == 0:
        return np.empty(0, dtype=np.int64)
    eps = max(_median_nn(xy) * eps_scale, 1e-6)
    labels = DBSCAN(eps=eps, min_samples=1).fit_predict(xy)
    counts = np.bincount(labels + 1)
    order = sorted(set(labels.tolist()), key=lambda lab: counts[lab + 1], reverse=True)
    remap = {old: new for new, old in enumerate(order)}
    return np.asarray([remap[int(v)] for v in labels], dtype=np.int64)


def select_crops(
    xy: np.ndarray,
    islands: np.ndarray,
    *,
    max_crops: int,
    radius_scale: float,
) -> list[CropSpec]:
    nn = _median_nn(xy)
    radius = max(nn * radius_scale, 1e-6)
    crops: list[CropSpec] = []
    island_counts = sorted(
        ((int(i), int(np.count_nonzero(islands == i))) for i in set(islands.tolist())),
        key=lambda item: item[1],
        reverse=True,
    )
    for island_id, _count in island_counts[:3]:
        pts = xy[islands == island_id]
        center = tuple(np.median(pts, axis=0).astype(float).tolist())
        crops.append(CropSpec(f"crop_{len(crops):02d}_island_{island_id}", "top_island", center, radius, island_id))

    if island_counts:
        main_island = island_counts[0][0]
        idx = np.flatnonzero(islands == main_island)
        pts = xy[idx]
        if len(pts) >= 2:
            k = min(16, len(pts) - 1)
            dist, _near = cKDTree(pts).query(pts, k=k + 1, workers=-1)
            density = 1.0 / np.maximum(dist[:, 1:].mean(axis=1), 1e-9)
            chosen: list[int] = []
            for rel in np.argsort(-density):
                p = pts[int(rel)]
                if all(np.linalg.norm(p - pts[c]) >= radius * 1.2 for c in chosen):
                    chosen.append(int(rel))
                if len(chosen) == 3:
                    break
            for rel in chosen:
                center = tuple(pts[rel].astype(float).tolist())
                crops.append(CropSpec(f"crop_{len(crops):02d}_dense_{len(crops)}", "density_center", center, radius, main_island))

            main_center = pts.mean(axis=0)
            edge_rel = int(np.argmax(np.linalg.norm(pts - main_center, axis=1)))
            crops.append(
                CropSpec(
                    f"crop_{len(crops):02d}_edge",
                    "sparse_edge",
                    tuple(pts[edge_rel].astype(float).tolist()),
                    radius,
                    main_island,
                )
            )

    small = [item for item in island_counts[3:] if item[1] >= 3]
    if small:
        island_id = small[0][0]
        pts = xy[islands == island_id]
        crops.append(
            CropSpec(
                f"crop_{len(crops):02d}_small_island",
                "small_island",
                tuple(np.median(pts, axis=0).astype(float).tolist()),
                radius,
                island_id,
            )
        )

    out: list[CropSpec] = []
    seen: set[tuple[int, int, int]] = set()
    for crop in crops:
        key = (
            crop.island_id,
            int(round(crop.center[0] / max(radius, 1e-9))),
            int(round(crop.center[1] / max(radius, 1e-9))),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(crop)
        if len(out) >= max_crops:
            break
    return out


def _classify_settlements(xy: np.ndarray, seed: int) -> tuple[np.ndarray, np.ndarray, list[str]]:
    if len(xy) == 0:
        return np.empty((0, 2)), np.empty(0, dtype=np.int64), []
    n_clusters = max(1, min(12, int(round(math.sqrt(len(xy) / 28)))))
    if n_clusters == 1:
        labels = np.zeros(len(xy), dtype=np.int64)
        centers = xy.mean(axis=0, keepdims=True)
    else:
        km = MiniBatchKMeans(n_clusters=n_clusters, random_state=seed, n_init="auto")
        labels = km.fit_predict(xy)
        centers = km.cluster_centers_
    classes = []
    for i in range(len(centers)):
        count = int(np.count_nonzero(labels == i))
        if count >= 900:
            classes.append("city")
        elif count >= 180:
            classes.append("town")
        else:
            classes.append("village")
    return centers.astype(np.float64), labels.astype(np.int64), classes


def _settlement_roads(
    centers: np.ndarray,
    crop_id: str,
    next_road_id: int,
) -> tuple[list[RoadRec], int]:
    roads: list[RoadRec] = []
    if len(centers) < 2:
        return roads, next_road_id
    edges: set[tuple[int, int]] = set()
    try:
        tri = Delaunay(centers)
        for simplex in tri.simplices:
            for a, b in (
                (simplex[0], simplex[1]),
                (simplex[1], simplex[2]),
                (simplex[2], simplex[0]),
            ):
                edges.add(tuple(sorted((int(a), int(b)))))
    except QhullError:
        order = np.argsort(centers[:, 0])
        edges.update(
            tuple(sorted((int(a), int(b))))
            for a, b in zip(order[:-1], order[1:], strict=False)
        )
    graph = nx.Graph()
    for a, b in edges:
        graph.add_edge(a, b, weight=float(np.linalg.norm(centers[a] - centers[b])))
    for a, b in nx.minimum_spanning_edges(graph, data=False):
        line = LineString([centers[int(a)].tolist(), centers[int(b)].tolist()])
        roads.append(RoadRec(next_road_id, crop_id, "regional", line))
        next_road_id += 1
    return roads, next_road_id


def _settlement_class(count: int) -> str:
    if count >= 900:
        return "city"
    if count >= 180:
        return "town"
    return "village"


def _global_settlements(
    xy: np.ndarray,
    islands: np.ndarray,
    *,
    max_islands: int,
    seed: int,
) -> list[SettlementRec]:
    rng = np.random.default_rng(seed)
    island_counts = sorted(
        ((int(i), int(np.count_nonzero(islands == i))) for i in set(islands.tolist())),
        key=lambda item: item[1],
        reverse=True,
    )
    settlements: list[SettlementRec] = []
    next_id = 0
    for island_id, _count in island_counts[:max_islands]:
        idx = np.flatnonzero(islands == island_id)
        pts = xy[idx]
        if len(pts) == 0:
            continue
        k = max(1, min(18, int(round(math.sqrt(len(pts)) / 11.0))))
        if len(pts) <= k:
            labels = np.arange(len(pts), dtype=np.int64)
            centers = pts
        else:
            sample_idx = (
                rng.choice(len(pts), size=30000, replace=False)
                if len(pts) > 30000
                else np.arange(len(pts))
            )
            km = MiniBatchKMeans(
                n_clusters=k,
                random_state=seed + island_id,
                n_init="auto",
                batch_size=4096,
            )
            km.fit(pts[sample_idx])
            labels = km.predict(pts)
            centers = km.cluster_centers_
        for sid, center in enumerate(centers):
            local_count = int(np.count_nonzero(labels == sid))
            settlements.append(
                SettlementRec(
                    settlement_id=next_id,
                    island_id=island_id,
                    center=(float(center[0]), float(center[1])),
                    world_count=local_count,
                    settlement_class=_settlement_class(local_count),
                )
            )
            next_id += 1
    return settlements


def _regional_candidate_edges(centers: np.ndarray) -> set[tuple[int, int]]:
    if len(centers) < 2:
        return set()
    if len(centers) == 2:
        return {(0, 1)}
    edges: set[tuple[int, int]] = set()
    try:
        tri = Delaunay(centers)
        for simplex in tri.simplices:
            for a, b in (
                (simplex[0], simplex[1]),
                (simplex[1], simplex[2]),
                (simplex[2], simplex[0]),
            ):
                edges.add(tuple(sorted((int(a), int(b)))))
    except QhullError:
        order = np.argsort(centers[:, 0] + centers[:, 1] * 1e-6)
        edges.update(
            tuple(sorted((int(a), int(b))))
            for a, b in zip(order[:-1], order[1:], strict=False)
        )
    k = min(3, len(centers) - 1)
    if k > 0:
        _dist, near = cKDTree(centers).query(centers, k=k + 1, workers=-1)
        for i, row in enumerate(near[:, 1:]):
            for j in row.tolist():
                edges.add(tuple(sorted((int(i), int(j)))))
    return edges


def _regional_path(a: np.ndarray, b: np.ndarray, *, salt: int) -> LineString:
    delta = b - a
    dist = float(np.linalg.norm(delta))
    if dist <= 1e-9:
        return LineString([a.tolist(), b.tolist()])
    tangent = delta / dist
    normal = np.array([-tangent[1], tangent[0]], dtype=np.float64)
    bend = math.sin((salt + 1) * 1.61803398875) * dist * 0.08
    p1 = a * 0.67 + b * 0.33 + normal * bend
    p2 = a * 0.33 + b * 0.67 + normal * bend * 0.65
    return LineString([a.tolist(), p1.tolist(), p2.tolist(), b.tolist()])


def _regional_roads(
    settlements: list[SettlementRec],
    *,
    next_road_id: int,
) -> tuple[list[RoadRec], int]:
    roads: list[RoadRec] = []
    by_island: dict[int, list[SettlementRec]] = {}
    for settlement in settlements:
        by_island.setdefault(settlement.island_id, []).append(settlement)
    for island_id, island_settlements in sorted(by_island.items()):
        if len(island_settlements) < 2:
            continue
        centers = np.asarray([s.center for s in island_settlements], dtype=np.float64)
        candidates = _regional_candidate_edges(centers)
        graph = nx.Graph()
        for a, b in candidates:
            dist = float(np.linalg.norm(centers[a] - centers[b]))
            graph.add_edge(a, b, weight=dist)
        selected = {
            tuple(sorted((int(a), int(b))))
            for a, b in nx.minimum_spanning_edges(graph, data=False)
        }
        extras = sorted(
            (
                float(np.linalg.norm(centers[a] - centers[b])),
                a,
                b,
            )
            for a, b in candidates
            if tuple(sorted((a, b))) not in selected
        )
        loop_target = max(1, int(round(len(island_settlements) * 0.28)))
        for _dist, a, b in extras[:loop_target]:
            selected.add(tuple(sorted((int(a), int(b)))))
        for a, b in sorted(selected):
            kind = (
                "regional_primary"
                if max(
                    island_settlements[a].world_count,
                    island_settlements[b].world_count,
                )
                >= 900
                else "regional_secondary"
            )
            roads.append(
                RoadRec(
                    next_road_id,
                    "__regional__",
                    kind,
                    _regional_path(centers[a], centers[b], salt=next_road_id + island_id),
                )
            )
            next_road_id += 1
    return roads, next_road_id


def _make_terrain_model(
    *,
    center: np.ndarray,
    axes: np.ndarray,
    lo: np.ndarray,
    hi: np.ndarray,
    sparse: np.ndarray,
    crop: CropSpec,
    seed: int,
) -> TerrainModel:
    span = np.maximum(hi - lo, 1e-9)
    relief = float(max(span.max(), 1.0) * (0.004 + 0.012 * float(np.mean(sparse))))
    valley_strength = float(0.35 + 0.75 * float(np.mean(sparse)))
    return TerrainModel(
        center=(float(center[0]), float(center[1])),
        axes=(
            (float(axes[0, 0]), float(axes[1, 0])),
            (float(axes[0, 1]), float(axes[1, 1])),
        ),
        lo=(float(lo[0]), float(lo[1])),
        span=(float(span[0]), float(span[1])),
        relief=relief,
        valley_strength=valley_strength,
        seed=seed + crop.island_id * 1009 + len(crop.crop_id) * 17,
    )


def _terrain_gradient_local(
    terrain: TerrainModel,
    lx: float,
    ly: float,
    step: float,
) -> np.ndarray:
    hpx = terrain.sample_local(lx + step, ly)
    hmx = terrain.sample_local(lx - step, ly)
    hpy = terrain.sample_local(lx, ly + step)
    hmy = terrain.sample_local(lx, ly - step)
    return np.array([(hpx - hmx) / (2 * step), (hpy - hmy) / (2 * step)])


def _adaptive_breaks(
    values: np.ndarray,
    lo: float,
    hi: float,
    count: int,
    *,
    density_weight: float,
) -> np.ndarray:
    if count <= 1 or len(values) < 8:
        return np.linspace(lo, hi, max(count, 1) + 1)
    quantiles = np.quantile(values, np.linspace(0.0, 1.0, count + 1))
    uniform = np.linspace(lo, hi, count + 1)
    breaks = uniform * (1.0 - density_weight) + quantiles * density_weight
    breaks[0] = lo
    breaks[-1] = hi
    min_spacing = (hi - lo) / max(count, 1) * 0.42
    for i in range(1, len(breaks)):
        breaks[i] = max(breaks[i], breaks[i - 1] + min_spacing)
    if breaks[-1] > hi:
        overflow = breaks[-1] - hi
        for i in range(1, len(breaks) - 1):
            breaks[i] -= overflow * (i / (len(breaks) - 1))
        breaks[-1] = hi
    for i in range(len(breaks) - 2, -1, -1):
        breaks[i] = min(breaks[i], breaks[i + 1] - min_spacing)
    breaks[0] = lo
    return breaks


def _coarse_indices(n: int, zones: int) -> list[int]:
    zones = max(1, min(zones, max(n - 1, 1)))
    idx = sorted({int(round(v)) for v in np.linspace(0, n - 1, zones + 1)})
    if idx[0] != 0:
        idx.insert(0, 0)
    if idx[-1] != n - 1:
        idx.append(n - 1)
    return idx


def _zone_kind(world_count: int, density: float, sparse_score: float, z_range: float) -> str:
    if world_count >= 90 and sparse_score < 0.35:
        return "core"
    if world_count >= 45 and sparse_score < 0.55:
        return "urban"
    if z_range > 0.010 or sparse_score > 0.72:
        return "hillside"
    if world_count >= 18:
        return "suburban"
    return "outskirt"


def _grid_layout(
    xy: np.ndarray,
    crop: CropSpec,
    *,
    next_road_id: int,
    next_block_id: int,
    lot_start: int,
    zone_start: int,
    seed: int,
    max_grade: float,
) -> tuple[list[RoadRec], list[BlockRec], list[LotRec], list[ZoneRec], TerrainModel, int, int, int, int]:
    center, axes = _pca_axes(xy)
    local = (xy - center) @ axes
    lo = local.min(axis=0)
    hi = local.max(axis=0)
    span = np.maximum(hi - lo, _median_nn(xy) * 4)
    pad = np.maximum(span * 0.12, _median_nn(xy) * 2)
    lo -= pad
    hi += pad
    span = hi - lo

    target_blocks = max(1, int(math.ceil(len(xy) / 7)))
    cols = max(1, int(math.ceil(math.sqrt(target_blocks * span[0] / max(span[1], 1e-9)))))
    rows = max(1, int(math.ceil(target_blocks / cols)))
    xs = _adaptive_breaks(local[:, 0], float(lo[0]), float(hi[0]), cols, density_weight=0.42)
    ys = _adaptive_breaks(local[:, 1], float(lo[1]), float(hi[1]), rows, density_weight=0.42)
    base_dx = float(np.median(np.diff(xs))) if len(xs) > 1 else float(span[0])
    base_dy = float(np.median(np.diff(ys))) if len(ys) > 1 else float(span[1])
    base_cell = max(min(base_dx, base_dy), 1e-9)

    tree = cKDTree(local)
    k = min(24, max(len(local) - 1, 1))
    node_local = np.array([[x, y] for y in ys for x in xs], dtype=np.float64)
    dist, _near = tree.query(node_local, k=k + 1, workers=-1)
    local_spacing = dist[:, 1:].mean(axis=1) if dist.ndim == 2 else dist
    q_lo = float(np.quantile(local_spacing, 0.20))
    q_hi = float(np.quantile(local_spacing, 0.92))
    sparse = np.clip((local_spacing - q_lo) / max(q_hi - q_lo, 1e-9), 0.0, 1.0)
    sparse = sparse.reshape((len(ys), len(xs)))

    nodes = np.zeros((len(ys), len(xs), 2), dtype=np.float64)
    phase = (crop.island_id + 1) * 1.371
    for r, y in enumerate(ys):
        for c, x in enumerate(xs):
            boundary = min(r, c, len(ys) - 1 - r, len(xs) - 1 - c)
            boundary_scale = 0.45 if boundary == 0 else 1.0
            amp = base_cell * (0.05 + 0.28 * float(sparse[r, c])) * boundary_scale
            wx = (
                math.sin((y - lo[1]) / max(span[1], 1e-9) * math.tau * 1.7 + phase)
                + 0.55 * math.sin((x + y) / max(span.max(), 1e-9) * math.tau * 2.3)
            )
            wy = (
                math.cos((x - lo[0]) / max(span[0], 1e-9) * math.tau * 1.4 + phase)
                + 0.45 * math.sin((x - y) / max(span.max(), 1e-9) * math.tau * 1.9)
            )
            nodes[r, c] = [x + amp * wx, y + amp * wy]

    terrain = _make_terrain_model(
        center=center,
        axes=axes,
        lo=lo,
        hi=hi,
        sparse=sparse,
        crop=crop,
        seed=seed,
    )
    grad_step = base_cell * 0.35
    for r in range(len(ys)):
        for c in range(len(xs)):
            s = float(sparse[r, c])
            if s <= 0.05:
                continue
            grad = _terrain_gradient_local(
                terrain,
                float(nodes[r, c, 0]),
                float(nodes[r, c, 1]),
                grad_step,
            )
            gnorm = float(np.linalg.norm(grad))
            if gnorm <= 1e-9:
                continue
            contour = np.array([-grad[1], grad[0]], dtype=np.float64) / gnorm
            nodes[r, c] += contour * base_cell * 0.12 * s

    for _round in range(2):
        next_nodes = nodes.copy()
        for r in range(1, len(ys) - 1):
            for c in range(1, len(xs) - 1):
                p = _to_world(nodes[r, c][None, :], center, axes)[0]
                neighbor_grades = []
                for rr, cc in ((r - 1, c), (r + 1, c), (r, c - 1), (r, c + 1)):
                    q = _to_world(nodes[rr, cc][None, :], center, axes)[0]
                    neighbor_grades.append(
                        terrain.grade_between(tuple(p), tuple(q))
                    )
                if max(neighbor_grades, default=0.0) > max_grade:
                    next_nodes[r, c] = (
                        nodes[r, c] * 0.55
                        + (
                            nodes[r - 1, c]
                            + nodes[r + 1, c]
                            + nodes[r, c - 1]
                            + nodes[r, c + 1]
                        )
                        * 0.1125
                    )
        nodes = next_nodes

    def world_node(r: int, c: int) -> np.ndarray:
        return _to_world(nodes[r, c][None, :], center, axes)[0]

    def cell_point(
        p00: np.ndarray,
        p10: np.ndarray,
        p11: np.ndarray,
        p01: np.ndarray,
        u: float,
        v: float,
    ) -> np.ndarray:
        return (
            p00 * (1.0 - u) * (1.0 - v)
            + p10 * u * (1.0 - v)
            + p11 * u * v
            + p01 * (1.0 - u) * v
        )

    district_cols = max(2, min(5, int(round(math.sqrt(cols)))))
    district_rows = max(2, min(5, int(round(math.sqrt(rows)))))
    coarse_cols = _coarse_indices(len(xs), district_cols)
    coarse_rows = _coarse_indices(len(ys), district_rows)
    collector_cols = set(coarse_cols[1:-1])
    collector_rows = set(coarse_rows[1:-1])
    primary_col = (
        min(collector_cols, key=lambda c: abs(c - len(xs) / 2))
        if collector_cols
        else len(xs) // 2
    )
    primary_row = (
        min(collector_rows, key=lambda r: abs(r - len(ys) / 2))
        if collector_rows
        else len(ys) // 2
    )

    zones_out: list[ZoneRec] = []
    zone_id = zone_start
    for rr0, rr1 in zip(coarse_rows[:-1], coarse_rows[1:], strict=True):
        for cc0, cc1 in zip(coarse_cols[:-1], coarse_cols[1:], strict=True):
            poly_pts = [
                world_node(rr0, cc0),
                world_node(rr0, cc1),
                world_node(rr1, cc1),
                world_node(rr1, cc0),
            ]
            geom = Polygon(poly_pts)
            local_min = np.minimum(nodes[rr0, cc0], nodes[rr1, cc1])
            local_max = np.maximum(nodes[rr0, cc0], nodes[rr1, cc1])
            in_zone = (
                (local[:, 0] >= local_min[0])
                & (local[:, 0] <= local_max[0])
                & (local[:, 1] >= local_min[1])
                & (local[:, 1] <= local_max[1])
            )
            count = int(np.count_nonzero(in_zone))
            density = float(count / max(geom.area, 1e-9))
            sparse_score = float(np.mean(sparse[rr0 : rr1 + 1, cc0 : cc1 + 1]))
            z_vals = [terrain.sample(float(p[0]), float(p[1])) for p in poly_pts]
            z_range = float(max(z_vals) - min(z_vals)) if z_vals else 0.0
            zones_out.append(
                ZoneRec(
                    zone_id=zone_id,
                    crop_id=crop.crop_id,
                    zone_kind=_zone_kind(count, density, sparse_score, z_range),
                    geom=geom,
                    world_count=count,
                    density=density,
                    sparse_score=sparse_score,
                    z_range=z_range,
                )
            )
            zone_id += 1

    roads: list[RoadRec] = []
    for c, _x in enumerate(xs):
        pts = _to_world(nodes[:, c, :], center, axes).tolist()
        if c == primary_col:
            kind = "collector"
        elif c in collector_cols:
            kind = "district"
        else:
            kind = "local"
        roads.append(RoadRec(next_road_id, crop.crop_id, kind, LineString(pts)))
        next_road_id += 1
    for r, _y in enumerate(ys):
        pts = _to_world(nodes[r, :, :], center, axes).tolist()
        if r == primary_row:
            kind = "collector"
        elif r in collector_rows:
            kind = "district"
        else:
            kind = "local"
        roads.append(RoadRec(next_road_id, crop.crop_id, kind, LineString(pts)))
        next_road_id += 1

    blocks: list[BlockRec] = []
    lots: list[LotRec] = []
    lot_id = lot_start
    for r in range(rows):
        for c in range(cols):
            p00 = world_node(r, c)
            p10 = world_node(r, c + 1)
            p11 = world_node(r + 1, c + 1)
            p01 = world_node(r + 1, c)
            block = Polygon([p00, p10, p11, p01])
            if not block.is_valid or block.area <= 1e-10:
                p00, p10, p11, p01 = (
                    _to_world(
                        np.array(
                            [
                                [xs[c], ys[r]],
                                [xs[c + 1], ys[r]],
                                [xs[c + 1], ys[r + 1]],
                                [xs[c], ys[r + 1]],
                            ],
                            dtype=np.float64,
                        ),
                        center,
                        axes,
                    )
                )
                block = Polygon([p00, p10, p11, p01])
            block_rec = BlockRec(next_block_id, crop.crop_id, block)
            blocks.append(block_rec)
            sub_cols = 4
            sub_rows = 4
            for sr in range(sub_rows):
                for sc in range(sub_cols):
                    if 0 < sr < sub_rows - 1 and 0 < sc < sub_cols - 1:
                        continue
                    u0 = sc / sub_cols
                    u1 = (sc + 1) / sub_cols
                    v0 = sr / sub_rows
                    v1 = (sr + 1) / sub_rows
                    q00 = cell_point(p00, p10, p11, p01, u0, v0)
                    q10 = cell_point(p00, p10, p11, p01, u1, v0)
                    q11 = cell_point(p00, p10, p11, p01, u1, v1)
                    q01 = cell_point(p00, p10, p11, p01, u0, v1)
                    lot = Polygon([q00, q10, q11, q01])
                    if not lot.is_valid or lot.area <= 1e-10:
                        continue
                    centroid = lot.centroid
                    tangent = (
                        q10 - q00
                        if sr == 0 or sr == sub_rows - 1
                        else q01 - q00
                    )
                    angle = math.atan2(float(tangent[1]), float(tangent[0]))
                    lots.append(
                        LotRec(
                            lot_id,
                            crop.crop_id,
                            block_rec.block_id,
                            lot,
                            (float(centroid.x), float(centroid.y)),
                            angle,
                        )
                    )
                    lot_id += 1
            next_block_id += 1

    return roads, blocks, lots, zones_out, terrain, next_road_id, next_block_id, lot_id, zone_id


def _building_in_lot(lot: Polygon, angle: float, scale: float) -> Polygon:
    center = np.array([lot.centroid.x, lot.centroid.y], dtype=np.float64)
    axis = np.array([math.cos(angle), math.sin(angle)], dtype=np.float64)
    axes = np.column_stack([axis, np.array([-axis[1], axis[0]])])
    coords = np.asarray(lot.exterior.coords[:-1], dtype=np.float64)
    local = (coords - center) @ axes
    span = np.maximum(local.max(axis=0) - local.min(axis=0), 1e-9)
    width = span[0] * scale
    depth = span[1] * scale
    setback = span[1] * (0.5 - scale * 0.5) * 0.35
    return _rect_poly(0.0, -setback, width, depth, center, axes)


def _template_lot_cells(zone_kind: str, block: Polygon, *, seed: int) -> tuple[int, int]:
    aspect = _aspect(block)
    long_x = aspect >= 1.18
    if zone_kind == "core":
        return (4, 2) if long_x else (2, 4)
    if zone_kind == "urban":
        flip = seed % 3 == 0
        if long_x ^ flip:
            return 3, 2
        return 2, 3
    if zone_kind == "suburban":
        if seed % 7 == 0:
            return 2, 2
        if long_x:
            return 3, 2
        return 2, 3
    if zone_kind == "hillside":
        if seed % 3 == 0:
            return 2, 2
        return (3, 2) if long_x else (2, 3)
    return 3, 2


def _terrain_guided_warp(
    pt: np.ndarray,
    *,
    lo: np.ndarray,
    span: np.ndarray,
    base_cell: float,
    terrain: TerrainModel,
    seed: int,
    strength: float,
) -> np.ndarray:
    if strength <= 1e-9:
        return pt
    u = float((pt[0] - lo[0]) / max(span[0], 1e-9))
    v = float((pt[1] - lo[1]) / max(span[1], 1e-9))
    phase = seed * 0.173
    grad = _terrain_gradient_local(terrain, float(pt[0]), float(pt[1]), base_cell * 0.45)
    if np.linalg.norm(grad) > 1e-9:
        contour = np.array([-grad[1], grad[0]], dtype=np.float64)
        contour /= max(float(np.linalg.norm(contour)), 1e-9)
    else:
        contour = np.array([1.0, 0.0], dtype=np.float64)
    wave = (
        0.62 * math.sin((u * 1.35 + v * 0.45 + phase) * math.tau)
        + 0.38 * math.cos((u * 0.70 - v * 1.15 + phase * 0.7) * math.tau)
    )
    cross = np.array(
        [
            math.sin((v * 1.1 + phase * 0.3) * math.tau),
            math.cos((u * 1.0 - phase * 0.2) * math.tau),
        ],
        dtype=np.float64,
    )
    return pt + base_cell * strength * (contour * wave * 0.72 + cross * 0.28)


def _split_fraction(seed: int, level: int, index: int) -> float:
    value = math.sin((seed + 1) * 12.9898 + (level + 3) * 78.233 + (index + 5) * 37.719)
    return 0.50 + 0.12 * value


def _valley_bend(
    pt: np.ndarray,
    *,
    lo: np.ndarray,
    span: np.ndarray,
    base_cell: float,
    sparse_score: float,
    seed: int,
    boundary_scale: float,
) -> np.ndarray:
    u = float((pt[0] - lo[0]) / max(span[0], 1e-9))
    v = float((pt[1] - lo[1]) / max(span[1], 1e-9))
    phase = seed * 0.097
    valley_u = 0.50 + 0.13 * math.sin((v * 0.92 + phase) * math.tau)
    valley_v = 0.50 + 0.10 * math.cos((u * 0.78 - phase * 0.8) * math.tau)
    sparse_amp = 0.012 + 0.210 * sparse_score
    dense_hold = 0.18 + 0.82 * sparse_score
    x_pull = math.exp(-((u - valley_u) ** 2) / 0.085)
    y_pull = math.exp(-((v - valley_v) ** 2) / 0.110)
    offset = np.array(
        [
            math.sin((v * 1.05 + phase) * math.tau) * x_pull,
            math.cos((u * 0.90 - phase * 0.7) * math.tau) * y_pull,
        ],
        dtype=np.float64,
    )
    return pt + offset * base_cell * sparse_amp * dense_hold * boundary_scale


def _domain_layout(
    xy: np.ndarray,
    crop: CropSpec,
    *,
    next_road_id: int,
    next_block_id: int,
    lot_start: int,
    zone_start: int,
    seed: int,
    max_grade: float,
) -> tuple[list[RoadRec], list[BlockRec], list[LotRec], list[ZoneRec], TerrainModel, int, int, int, int]:
    center, axes = _pca_axes(xy)
    local = (xy - center) @ axes
    lo = local.min(axis=0)
    hi = local.max(axis=0)
    span = np.maximum(hi - lo, _median_nn(xy) * 4)
    pad = np.maximum(span * 0.14, _median_nn(xy) * 2)
    lo -= pad
    hi += pad
    span = hi - lo
    aspect = float(span[0] / max(span[1], 1e-9))
    district_cols = max(2, min(5, int(round(math.sqrt(len(xy) / 120) * math.sqrt(aspect)))))
    district_rows = max(2, min(5, int(math.ceil(district_cols / max(aspect, 1e-9)))))
    district_rows = max(2, min(5, district_rows))
    cx = _adaptive_breaks(local[:, 0], float(lo[0]), float(hi[0]), district_cols, density_weight=0.28)
    cy = _adaptive_breaks(local[:, 1], float(lo[1]), float(hi[1]), district_rows, density_weight=0.28)

    tree = cKDTree(local)
    coarse_nodes_local = np.array([[x, y] for y in cy for x in cx], dtype=np.float64)
    k = min(24, max(len(local) - 1, 1))
    dist, _near = tree.query(coarse_nodes_local, k=k + 1, workers=-1)
    local_spacing = dist[:, 1:].mean(axis=1) if dist.ndim == 2 else dist
    q_lo = float(np.quantile(local_spacing, 0.20))
    q_hi = float(np.quantile(local_spacing, 0.92))
    sparse = np.clip((local_spacing - q_lo) / max(q_hi - q_lo, 1e-9), 0.0, 1.0)
    sparse_grid = sparse.reshape((len(cy), len(cx)))
    terrain = _make_terrain_model(
        center=center,
        axes=axes,
        lo=lo,
        hi=hi,
        sparse=sparse_grid,
        crop=crop,
        seed=seed,
    )

    base_cell = max(min(np.median(np.diff(cx)), np.median(np.diff(cy))), 1e-9)

    def interp_sparse(rf: float, cf: float) -> float:
        r0 = int(np.clip(math.floor(rf), 0, len(cy) - 1))
        c0 = int(np.clip(math.floor(cf), 0, len(cx) - 1))
        r1 = min(r0 + 1, len(cy) - 1)
        c1 = min(c0 + 1, len(cx) - 1)
        tr = float(np.clip(rf - r0, 0.0, 1.0))
        tc = float(np.clip(cf - c0, 0.0, 1.0))
        return float(
            sparse_grid[r0, c0] * (1.0 - tr) * (1.0 - tc)
            + sparse_grid[r1, c0] * tr * (1.0 - tc)
            + sparse_grid[r0, c1] * (1.0 - tr) * tc
            + sparse_grid[r1, c1] * tr * tc
        )

    def flow_point(rf: float, cf: float, *, extra_strength: float = 1.0) -> np.ndarray:
        r0 = int(np.clip(math.floor(rf), 0, len(cy) - 2))
        c0 = int(np.clip(math.floor(cf), 0, len(cx) - 2))
        tr = float(np.clip(rf - r0, 0.0, 1.0))
        tc = float(np.clip(cf - c0, 0.0, 1.0))
        x = float(cx[c0] * (1.0 - tc) + cx[c0 + 1] * tc)
        y = float(cy[r0] * (1.0 - tr) + cy[r0 + 1] * tr)
        boundary = min(rf, cf, len(cy) - 1 - rf, len(cx) - 1 - cf)
        boundary_scale = 0.35 if boundary <= 0.02 else 1.0
        sparse_score = interp_sparse(rf, cf)
        strength = (0.006 + 0.185 * sparse_score) * boundary_scale * extra_strength
        warped = _terrain_guided_warp(
            np.array([x, y], dtype=np.float64),
            lo=lo,
            span=span,
            base_cell=base_cell,
            terrain=terrain,
            seed=seed + crop.island_id * 37,
            strength=strength,
        )
        return _valley_bend(
            warped,
            lo=lo,
            span=span,
            base_cell=base_cell,
            sparse_score=sparse_score,
            seed=seed + crop.island_id * 53,
            boundary_scale=boundary_scale * extra_strength,
        )

    coarse = np.zeros((len(cy), len(cx), 2), dtype=np.float64)
    for r, y in enumerate(cy):
        for c, x in enumerate(cx):
            _ = (x, y)
            coarse[r, c] = flow_point(float(r), float(c), extra_strength=1.0)

    def world(local_pt: np.ndarray) -> np.ndarray:
        return _to_world(local_pt[None, :], center, axes)[0]

    def bilinear(p00, p10, p11, p01, u: float, v: float) -> np.ndarray:
        return (
            p00 * (1.0 - u) * (1.0 - v)
            + p10 * u * (1.0 - v)
            + p11 * u * v
            + p01 * (1.0 - u) * v
        )

    roads: list[RoadRec] = []
    blocks: list[BlockRec] = []
    lots: list[LotRec] = []
    zones: list[ZoneRec] = []
    lot_id = lot_start
    zone_id = zone_start
    primary_col = len(cx) // 2
    primary_row = len(cy) // 2

    for c in range(len(cx)):
        samples = np.linspace(0.0, len(cy) - 1, max(9, (len(cy) - 1) * 5 + 1))
        pts = _to_world(np.asarray([flow_point(float(r), float(c), extra_strength=1.0) for r in samples]), center, axes).tolist()
        kind = "collector" if c == primary_col else "district"
        roads.append(RoadRec(next_road_id, crop.crop_id, kind, LineString(pts)))
        next_road_id += 1
    for r in range(len(cy)):
        samples = np.linspace(0.0, len(cx) - 1, max(9, (len(cx) - 1) * 5 + 1))
        pts = _to_world(np.asarray([flow_point(float(r), float(c), extra_strength=1.0) for c in samples]), center, axes).tolist()
        kind = "collector" if r == primary_row else "district"
        roads.append(RoadRec(next_road_id, crop.crop_id, kind, LineString(pts)))
        next_road_id += 1

    for zr in range(len(cy) - 1):
        for zc in range(len(cx) - 1):
            p00 = coarse[zr, zc]
            p10 = coarse[zr, zc + 1]
            p11 = coarse[zr + 1, zc + 1]
            p01 = coarse[zr + 1, zc]
            wpoly = [world(p) for p in (p00, p10, p11, p01)]
            zone_geom = Polygon(wpoly)
            local_min = np.minimum.reduce([p00, p10, p11, p01])
            local_max = np.maximum.reduce([p00, p10, p11, p01])
            in_zone = (
                (local[:, 0] >= local_min[0])
                & (local[:, 0] <= local_max[0])
                & (local[:, 1] >= local_min[1])
                & (local[:, 1] <= local_max[1])
            )
            zone_count = int(np.count_nonzero(in_zone))
            zone_sparse = float(np.mean(sparse_grid[zr : zr + 2, zc : zc + 2]))
            z_vals = [terrain.sample(float(p[0]), float(p[1])) for p in wpoly]
            z_range = float(max(z_vals) - min(z_vals)) if z_vals else 0.0
            density = float(zone_count / max(zone_geom.area, 1e-9))
            zone_kind = _zone_kind(zone_count, density, zone_sparse, z_range)
            zones.append(
                ZoneRec(
                    zone_id=zone_id,
                    crop_id=crop.crop_id,
                    zone_kind=zone_kind,
                    geom=zone_geom,
                    world_count=zone_count,
                    density=density,
                    sparse_score=zone_sparse,
                    z_range=z_range,
                )
            )
            zone_id += 1

            target_blocks = max(2, int(math.ceil(zone_count / 6))) if zone_count else 2
            zspan = np.maximum(local_max - local_min, 1e-9)
            cols = max(2, min(10, int(round(math.sqrt(target_blocks * zspan[0] / max(zspan[1], 1e-9))))))
            rows = max(2, min(10, int(math.ceil(target_blocks / cols))))
            if zone_sparse > 0.62:
                cols = max(2, cols - 1)
                rows = max(2, rows - 1)
            local_zone_geom = Polygon([p00, p10, p11, p01])
            local_zone_center = np.asarray(local_zone_geom.centroid.coords[0], dtype=np.float64)

            if zone_kind == "outskirt" and zone_count < 8:
                continue

            cogen_mode = zone_kind in {"suburban", "hillside"} and zone_count >= 12
            if cogen_mode:
                target_leaves = max(3, min(28, int(math.ceil(max(zone_count, 12) / 3.5))))
                avg_lots_per_leaf = 5 if zone_kind == "hillside" else 6
                if target_leaves * avg_lots_per_leaf < zone_count * 1.80:
                    cogen_mode = False
            if cogen_mode:
                cells: list[tuple[float, float, float, float, int]] = [(0.0, 1.0, 0.0, 1.0, 0)]
                split_segments: list[tuple[float, float, float, float]] = []
                while len(cells) < target_leaves:
                    best_i = -1
                    best_score = -1.0
                    for i, (u0, u1, v0, v1, level) in enumerate(cells):
                        du = u1 - u0
                        dv = v1 - v0
                        if du < 0.16 or dv < 0.16 or level >= 5:
                            continue
                        score = du * dv * (1.0 + 0.25 * abs(math.log(max(du / max(dv, 1e-9), 1e-9))))
                        if score > best_score:
                            best_score = score
                            best_i = i
                    if best_i < 0:
                        break
                    u0, u1, v0, v1, level = cells.pop(best_i)
                    du = u1 - u0
                    dv = v1 - v0
                    split_u = du >= dv * (0.82 + 0.25 * zone_sparse)
                    if not split_u and dv < du * (0.82 + 0.25 * zone_sparse):
                        split_u = (best_i + zone_id + seed + level) % 2 == 0
                    frac = float(np.clip(_split_fraction(seed + zone_id, level, best_i), 0.38, 0.62))
                    if split_u:
                        um = u0 + du * frac
                        if min(um - u0, u1 - um) < 0.12:
                            break
                        split_segments.append((um, um, v0, v1))
                        cells.append((u0, um, v0, v1, level + 1))
                        cells.append((um, u1, v0, v1, level + 1))
                    else:
                        vm = v0 + dv * frac
                        if min(vm - v0, v1 - vm) < 0.12:
                            break
                        split_segments.append((u0, u1, vm, vm))
                        cells.append((u0, u1, v0, vm, level + 1))
                        cells.append((u0, u1, vm, v1, level + 1))

                zone_r = float(zr)
                zone_c = float(zc)

                def uv_point(u: float, v: float, zone_r: float = zone_r, zone_c: float = zone_c) -> np.ndarray:
                    return flow_point(zone_r + float(v), zone_c + float(u), extra_strength=1.0)

                for u0, u1, v0, v1 in split_segments:
                    if abs(u1 - u0) < 1e-9:
                        samples = np.linspace(v0, v1, 7)
                        pts = _to_world(np.asarray([uv_point(u0, v) for v in samples]), center, axes).tolist()
                    else:
                        samples = np.linspace(u0, u1, 7)
                        pts = _to_world(np.asarray([uv_point(u, v0) for u in samples]), center, axes).tolist()
                    roads.append(RoadRec(next_road_id, crop.crop_id, "local", LineString(pts)))
                    next_road_id += 1

                for u0, u1, v0, v1, level in cells:
                    _ = level
                    q00 = world(uv_point(u0, v0))
                    q10 = world(uv_point(u1, v0))
                    q11 = world(uv_point(u1, v1))
                    q01 = world(uv_point(u0, v1))
                    block = Polygon([q00, q10, q11, q01])
                    if not block.is_valid or block.area <= 1e-10:
                        continue
                    blocks.append(BlockRec(next_block_id, crop.crop_id, block))
                    sub_cols, sub_rows = _template_lot_cells(
                        zone_kind,
                        block,
                        seed=next_block_id + zone_id + seed,
                    )
                    for sr in range(sub_rows):
                        for sc in range(sub_cols):
                            lu0 = u0 + (u1 - u0) * sc / sub_cols
                            lu1 = u0 + (u1 - u0) * (sc + 1) / sub_cols
                            lv0 = v0 + (v1 - v0) * sr / sub_rows
                            lv1 = v0 + (v1 - v0) * (sr + 1) / sub_rows
                            l00 = world(uv_point(lu0, lv0))
                            l10 = world(uv_point(lu1, lv0))
                            l11 = world(uv_point(lu1, lv1))
                            l01 = world(uv_point(lu0, lv1))
                            lot = Polygon([l00, l10, l11, l01])
                            if not lot.is_valid or lot.area <= 1e-10:
                                continue
                            centroid = lot.centroid
                            tangent = l10 - l00 if sr in {0, sub_rows - 1} else l01 - l00
                            angle = math.atan2(float(tangent[1]), float(tangent[0]))
                            lots.append(
                                LotRec(
                                    lot_id,
                                    crop.crop_id,
                                    next_block_id,
                                    lot,
                                    (float(centroid.x), float(centroid.y)),
                                    angle,
                                )
                            )
                            lot_id += 1
                    next_block_id += 1
                continue

            us = np.linspace(0.0, 1.0, cols + 1)
            vs = np.linspace(0.0, 1.0, rows + 1)
            if zone_kind in {"core", "urban"}:
                shear_scale = 0.025
                wobble = 0.006
                field_strength = 0.004
            elif zone_kind == "suburban":
                shear_scale = 0.070
                wobble = 0.020 + 0.024 * zone_sparse
                field_strength = 0.030 + 0.030 * zone_sparse
            else:
                shear_scale = 0.115
                wobble = 0.035 + 0.045 * zone_sparse
                field_strength = 0.060 + 0.050 * zone_sparse
            shear = math.sin((zone_id + seed) * 1.618) * shear_scale
            grid = np.zeros((rows + 1, cols + 1, 2), dtype=np.float64)
            for rr, v in enumerate(vs):
                for cc, u in enumerate(us):
                    fade = math.sin(math.pi * u) * math.sin(math.pi * v)
                    uu = np.clip(u + shear * (v - 0.5) * fade, 0.0, 1.0)
                    vv = np.clip(v - shear * (u - 0.5) * fade * 0.65, 0.0, 1.0)
                    base_q = flow_point(float(zr) + float(vv), float(zc) + float(uu), extra_strength=1.0)
                    q = base_q
                    tangent_u = bilinear(p10 - p00, p10 - p00, p11 - p01, p11 - p01, uu, vv)
                    grad = _terrain_gradient_local(terrain, float(q[0]), float(q[1]), base_cell * 0.35)
                    if np.linalg.norm(grad) > 1e-9:
                        contour = np.array([-grad[1], grad[0]]) / max(np.linalg.norm(grad), 1e-9)
                    else:
                        contour = tangent_u / max(np.linalg.norm(tangent_u), 1e-9)
                    local_curve = contour * base_cell * wobble * fade * math.sin((u * 2.7 + v * 3.1 + zone_id) * math.tau)
                    q = q + local_curve * (0.35 + field_strength)
                    if not local_zone_geom.covers(Point(float(q[0]), float(q[1]))):
                        q = base_q * 0.85 + local_zone_center * 0.15
                    if not local_zone_geom.covers(Point(float(q[0]), float(q[1]))):
                        q = base_q
                    grid[rr, cc] = q

            for _smooth in range(2):
                next_grid = grid.copy()
                for rr in range(1, rows):
                    for cc in range(1, cols):
                        axis_avg = (
                            grid[rr - 1, cc]
                            + grid[rr + 1, cc]
                            + grid[rr, cc - 1]
                            + grid[rr, cc + 1]
                        ) * 0.25
                        candidate = grid[rr, cc] * 0.68 + axis_avg * 0.32
                        if local_zone_geom.covers(Point(float(candidate[0]), float(candidate[1]))):
                            next_grid[rr, cc] = candidate
                grid = next_grid

            for cc in range(1, cols):
                pts = _to_world(grid[:, cc, :], center, axes).tolist()
                roads.append(RoadRec(next_road_id, crop.crop_id, "local", LineString(pts)))
                next_road_id += 1
            for rr in range(1, rows):
                pts = _to_world(grid[rr, :, :], center, axes).tolist()
                roads.append(RoadRec(next_road_id, crop.crop_id, "local", LineString(pts)))
                next_road_id += 1

            for rr in range(rows):
                for cc in range(cols):
                    q00 = world(grid[rr, cc])
                    q10 = world(grid[rr, cc + 1])
                    q11 = world(grid[rr + 1, cc + 1])
                    q01 = world(grid[rr + 1, cc])
                    block = Polygon([q00, q10, q11, q01])
                    if not block.is_valid or block.area <= 1e-10:
                        continue
                    blocks.append(BlockRec(next_block_id, crop.crop_id, block))
                    sub_cols, sub_rows = _template_lot_cells(
                        zone_kind,
                        block,
                        seed=next_block_id + zone_id + seed,
                    )
                    for sr in range(sub_rows):
                        for sc in range(sub_cols):
                            u0 = sc / sub_cols
                            u1 = (sc + 1) / sub_cols
                            v0 = sr / sub_rows
                            v1 = (sr + 1) / sub_rows
                            l00 = bilinear(q00, q10, q11, q01, u0, v0)
                            l10 = bilinear(q00, q10, q11, q01, u1, v0)
                            l11 = bilinear(q00, q10, q11, q01, u1, v1)
                            l01 = bilinear(q00, q10, q11, q01, u0, v1)
                            lot = Polygon([l00, l10, l11, l01])
                            if not lot.is_valid or lot.area <= 1e-10:
                                continue
                            centroid = lot.centroid
                            tangent = l10 - l00 if sr in {0, sub_rows - 1} else l01 - l00
                            angle = math.atan2(float(tangent[1]), float(tangent[0]))
                            lots.append(
                                LotRec(
                                    lot_id,
                                    crop.crop_id,
                                    next_block_id,
                                    lot,
                                    (float(centroid.x), float(centroid.y)),
                                    angle,
                                )
                            )
                            lot_id += 1
                    next_block_id += 1

    return roads, blocks, lots, zones, terrain, next_road_id, next_block_id, lot_id, zone_id


def _assign_worlds_to_lots(
    points: pl.DataFrame,
    xy: np.ndarray,
    lots: list[LotRec],
    *,
    building_scale: float,
    terrain: TerrainModel | None = None,
    roads: list[RoadRec] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if len(lots) < len(xy):
        raise RuntimeError(f"not enough candidate lots: {len(lots)} for {len(xy)} worlds")
    lot_xy = np.asarray([lot.centroid for lot in lots], dtype=np.float64)
    cost = np.linalg.norm(xy[:, None, :] - lot_xy[None, :, :], axis=2)
    row, col = linear_sum_assignment(cost)
    if len(row) != len(xy):
        raise RuntimeError("sparse assignment failed to assign every processed world")
    rows_p: list[dict[str, Any]] = []
    rows_b: list[dict[str, Any]] = []
    point_rows = points.to_dicts()
    assigned_lots: set[int] = set()
    road_geom = unary_union([road.geom for road in roads]) if roads else GeometryCollection()
    for world_idx, lot_idx in zip(row.tolist(), col.tolist(), strict=True):
        lot = lots[lot_idx]
        if lot.lot_id in assigned_lots:
            raise RuntimeError("duplicate lot assignment")
        assigned_lots.add(lot.lot_id)
        world = point_rows[world_idx]
        building = _building_in_lot(lot.geom, lot.road_angle, building_scale)
        if building.is_empty or not lot.geom.covers(building):
            inset = lot.geom.buffer(-math.sqrt(max(lot.geom.area, 1e-12)) * 0.08, join_style=2)
            building = inset if not inset.is_empty else building
        if building.is_empty or not lot.geom.covers(building):
            pt = lot.geom.representative_point()
            radius = math.sqrt(max(lot.geom.area, 1e-12)) * 0.08
            building = pt.buffer(radius, quad_segs=2).intersection(lot.geom)
        if building.is_empty:
            pt = lot.geom.representative_point()
            building = pt.buffer(math.sqrt(max(lot.geom.area, 1e-12)) * 0.02, quad_segs=2)
        if roads and not building.is_empty and building.intersects(road_geom):
            inset = lot.geom.buffer(-math.sqrt(max(lot.geom.area, 1e-12)) * 0.16, join_style=2)
            if not inset.is_empty and lot.geom.covers(inset) and not inset.intersects(road_geom):
                building = inset
            else:
                open_area = lot.geom.difference(road_geom.buffer(1e-7, cap_style="flat"))
                if not open_area.is_empty:
                    pt = open_area.representative_point()
                    radius = math.sqrt(max(lot.geom.area, 1e-12)) * 0.03
                    candidate = pt.buffer(radius, quad_segs=2).intersection(open_area)
                    if not candidate.is_empty and lot.geom.covers(candidate):
                        building = candidate
        lot_z = terrain.sample(*lot.centroid) if terrain is not None else 0.0
        b_z = terrain.sample(building.centroid.x, building.centroid.y) if terrain is not None else 0.0
        lot_coords = list(lot.geom.exterior.coords[:-1])
        lot_z_values = (
            [terrain.sample(float(x), float(y)) for x, y in lot_coords]
            if terrain is not None
            else [0.0]
        )
        rows_p.append(
            {
                "lot_id": lot.lot_id,
                "world_id": world["world_id"],
                "crop_id": lot.crop_id,
                "block_id": lot.block_id,
                "x": float(lot.centroid[0]),
                "y": float(lot.centroid[1]),
                "z": float(lot_z),
                "z_min": float(min(lot_z_values)),
                "z_max": float(max(lot_z_values)),
                "area": float(lot.geom.area),
                "geometry_wkt": lot.geom.wkt,
            }
        )
        rows_b.append(
            {
                "building_id": lot.lot_id,
                "lot_id": lot.lot_id,
                "world_id": world["world_id"],
                "crop_id": lot.crop_id,
                "x": float(building.centroid.x),
                "y": float(building.centroid.y),
                "z": float(b_z),
                "area": float(building.area),
                "angle": float(lot.road_angle),
                "geometry_wkt": building.wkt,
            }
        )
    return pl.DataFrame(rows_p), pl.DataFrame(rows_b)


def _filter_lots_for_roads(
    lots: list[LotRec],
    roads: list[RoadRec],
    *,
    clearance: float,
) -> list[LotRec]:
    if not roads:
        return lots
    # Local/collector grid roads are frontage boundaries. Reject only lots cut by
    # regional connectors through interiors.
    cutters = [road.geom for road in roads if road.kind == "regional"]
    if not cutters:
        return lots
    road_geom = unary_union(cutters)
    conflict = road_geom.buffer(max(clearance, 0.0), cap_style="flat")
    return [lot for lot in lots if not lot.geom.intersects(conflict)]


def _filter_lots_for_frontage(
    lots: list[LotRec],
    roads: list[RoadRec],
    *,
    tolerance: float,
) -> list[LotRec]:
    if not roads:
        return []
    road_geom = unary_union([road.geom for road in roads])
    return [lot for lot in lots if lot.geom.distance(road_geom) <= tolerance]


def _filter_overlapping_lots(lots: list[LotRec], *, tolerance: float) -> list[LotRec]:
    if not lots:
        return lots
    geoms = [lot.geom for lot in lots]
    tree = shapely.STRtree(geoms)
    rejected: set[int] = set()
    order = sorted(range(len(lots)), key=lambda i: (lots[i].geom.area, -lots[i].lot_id), reverse=True)
    rank = {idx: pos for pos, idx in enumerate(order)}
    for i in order:
        if i in rejected:
            continue
        for j_raw in tree.query(geoms[i]):
            j = int(j_raw)
            if j == i or j in rejected:
                continue
            if geoms[i].intersection(geoms[j]).area <= tolerance:
                continue
            if rank[j] > rank[i]:
                rejected.add(j)
            else:
                rejected.add(i)
                break
    return [lot for i, lot in enumerate(lots) if i not in rejected]


def _filter_building_road_conflicts(
    lots: list[LotRec],
    roads: list[RoadRec],
    *,
    building_scale: float,
) -> list[LotRec]:
    if not roads:
        return lots
    road_geom = unary_union([road.geom for road in roads])
    out = []
    for lot in lots:
        building = _building_in_lot(lot.geom, lot.road_angle, building_scale)
        if building.is_empty or not lot.geom.covers(building):
            pt = lot.geom.representative_point()
            radius = math.sqrt(max(lot.geom.area, 1e-12)) * 0.08
            building = pt.buffer(radius, quad_segs=2).intersection(lot.geom)
        inset = lot.geom.buffer(-math.sqrt(max(lot.geom.area, 1e-12)) * 0.10, join_style=2)
        interior_clear = inset.is_empty or not inset.intersects(road_geom)
        if not building.is_empty and not building.intersects(road_geom) and interior_clear:
            out.append(lot)
    return out


def _overlap_count(polys: list[Polygon], tolerance: float) -> int:
    count = 0
    tree = shapely.STRtree(polys)
    for i, poly in enumerate(polys):
        for j in tree.query(poly):
            j = int(j)
            if j <= i:
                continue
            inter = poly.intersection(polys[j]).area
            if inter > tolerance:
                count += 1
    return count


def _aspect(poly: Polygon) -> float:
    rect = poly.minimum_rotated_rectangle
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    edges = np.linalg.norm(np.roll(coords, -1, axis=0) - coords, axis=1)
    vals = edges[edges > 1e-9]
    if len(vals) == 0:
        return 0.0
    return float(vals.max() / max(vals.min(), 1e-9))


def _road_degree_metrics(roads: list[RoadRec]) -> dict[str, Any]:
    graph = nx.Graph()
    for road in roads:
        coords = list(road.geom.coords)
        for a, b in zip(coords[:-1], coords[1:], strict=False):
            pa = (round(float(a[0]), 6), round(float(a[1]), 6))
            pb = (round(float(b[0]), 6), round(float(b[1]), 6))
            graph.add_edge(pa, pb)
    degrees = [d for _n, d in graph.degree()]
    return {
        "road_node_count": int(graph.number_of_nodes()),
        "road_edge_count": int(graph.number_of_edges()),
        "road_degree_p95": float(np.quantile(degrees, 0.95)) if degrees else 0.0,
        "road_degree_gt8_hubs": int(sum(1 for d in degrees if d > 8)),
    }


def _road_grade_props(road: RoadRec, terrain: TerrainModel | None) -> dict[str, Any]:
    if terrain is None:
        return {"z_min": 0.0, "z_max": 0.0, "max_grade": 0.0, "mean_grade": 0.0}
    coords = [(float(x), float(y)) for x, y in road.geom.coords]
    zs = [terrain.sample(x, y) for x, y in coords]
    grades = [
        terrain.grade_between(a, b)
        for a, b in zip(coords[:-1], coords[1:], strict=False)
    ]
    return {
        "z_min": float(min(zs)) if zs else 0.0,
        "z_max": float(max(zs)) if zs else 0.0,
        "max_grade": float(max(grades)) if grades else 0.0,
        "mean_grade": float(np.mean(grades)) if grades else 0.0,
    }


def _road_grade_metrics(
    roads: list[RoadRec],
    terrain_by_crop: dict[str, TerrainModel],
) -> dict[str, Any]:
    grades = []
    for road in roads:
        props = _road_grade_props(road, terrain_by_crop.get(road.crop_id))
        grades.append(float(props["max_grade"]))
    arr = np.asarray(grades, dtype=np.float64)
    return {
        "road_grade_p50": float(np.quantile(arr, 0.50)) if len(arr) else 0.0,
        "road_grade_p95": float(np.quantile(arr, 0.95)) if len(arr) else 0.0,
        "road_grade_max": float(np.max(arr)) if len(arr) else 0.0,
    }


def _hard_metrics(
    source_xy: np.ndarray,
    parcels: pl.DataFrame,
    buildings: pl.DataFrame,
    roads: list[RoadRec],
    blocks: list[BlockRec],
) -> dict[str, Any]:
    parcel_rows = parcels.to_dicts()
    building_rows = buildings.to_dicts()
    parcel_polys = [shapely.from_wkt(row["geometry_wkt"]) for row in parcel_rows]
    building_polys = [shapely.from_wkt(row["geometry_wkt"]) for row in building_rows]
    assigned = parcels.height
    duplicate_lots = assigned - parcels["lot_id"].n_unique()
    outside = sum(
        1 for b, p in zip(building_polys, parcel_polys, strict=True) if not p.covers(b)
    )
    overlap_count = 0
    building_road = 0
    no_frontage = 0
    roads_by_crop: dict[str, list[RoadRec]] = {}
    for road in roads:
        roads_by_crop.setdefault(road.crop_id, []).append(road)
    for crop_id in sorted({str(row["crop_id"]) for row in parcel_rows}):
        crop_parcels = [
            poly
            for row, poly in zip(parcel_rows, parcel_polys, strict=True)
            if row["crop_id"] == crop_id
        ]
        crop_buildings = [
            poly
            for row, poly in zip(building_rows, building_polys, strict=True)
            if row["crop_id"] == crop_id
        ]
        overlap_count += _overlap_count(crop_parcels, 1e-7)
        crop_roads = roads_by_crop.get(crop_id, [])
        road_geom = (
            unary_union([r.geom for r in crop_roads])
            if crop_roads
            else GeometryCollection()
        )
        building_road += sum(1 for b in crop_buildings if b.intersects(road_geom))
        no_frontage += sum(1 for p in crop_parcels if p.distance(road_geom) > 1e-7)
    disp = np.linalg.norm(
        source_xy - parcels.select("x", "y").to_numpy().astype(np.float64), axis=1
    )
    parcel_aspects = np.asarray([_aspect(p) for p in parcel_polys], dtype=np.float64)
    block_aspects = np.asarray([_aspect(b.geom) for b in blocks], dtype=np.float64)
    block_areas = np.asarray([b.geom.area for b in blocks], dtype=np.float64)
    return {
        "processed_worlds": int(len(source_xy)),
        "assigned_worlds": int(assigned),
        "assignment_rate": float(assigned / max(len(source_xy), 1)),
        "duplicate_lot_assignments": int(duplicate_lots),
        "building_outside_parcel": int(outside),
        "parcel_overlap_count": int(overlap_count),
        "parcel_no_frontage": int(no_frontage),
        "parcel_frontage_rate": float((assigned - no_frontage) / max(assigned, 1)),
        "building_road_intersections": int(building_road),
        "planar_road_graph": True,
        "displacement_p50": float(np.quantile(disp, 0.50)) if len(disp) else 0.0,
        "displacement_p95": float(np.quantile(disp, 0.95)) if len(disp) else 0.0,
        "displacement_p99": float(np.quantile(disp, 0.99)) if len(disp) else 0.0,
        "block_aspect_p95": float(np.quantile(block_aspects, 0.95)) if len(block_aspects) else 0.0,
        "block_aspect_p99": float(np.quantile(block_aspects, 0.99)) if len(block_aspects) else 0.0,
        "sliver_block_count": int(np.count_nonzero(block_aspects > 8.0)),
        "parcel_aspect_p95": float(np.quantile(parcel_aspects, 0.95)) if len(parcel_aspects) else 0.0,
        "parcel_aspect_p99": float(np.quantile(parcel_aspects, 0.99)) if len(parcel_aspects) else 0.0,
        "unsubdivided_interior_area": float(block_areas.sum() - sum(p.area for p in parcel_polys)),
    }


def _raster_debug(xy: np.ndarray, path: Path, *, mode: str) -> None:
    if len(xy) == 0:
        _write_png(np.zeros((64, 64, 3), dtype=np.uint8), path)
        return
    size = 512
    mn = xy.min(axis=0)
    mx = xy.max(axis=0)
    span = np.maximum(mx - mn, 1e-9)
    pix = np.floor((xy - mn) / span * (size - 1)).astype(int)
    arr = np.zeros((size, size), dtype=np.float64)
    arr[size - 1 - pix[:, 1], pix[:, 0]] = 1.0
    if mode == "density":
        arr = ndimage.gaussian_filter(arr, sigma=5.0)
        arr = arr / max(float(arr.max()), 1e-9)
    elif mode == "height":
        occupied = ndimage.binary_dilation(arr > 0, iterations=5)
        occupied = ndimage.binary_closing(occupied, iterations=4)
        distance = ndimage.distance_transform_edt(occupied)
        distance = ndimage.gaussian_filter(distance, sigma=4.0)
        distance = distance / max(float(distance.max()), 1e-9)
        yy, xx = np.mgrid[0:size, 0:size]
        waves = (
            0.45 * np.sin(xx / 29.0 + yy / 83.0)
            + 0.30 * np.cos(xx / 71.0 - yy / 37.0)
            + 0.20 * np.sin((xx + yy) / 19.0)
        )
        waves = (waves - waves.min()) / max(float(waves.max() - waves.min()), 1e-9)
        arr = 0.65 * distance + 0.35 * waves
        arr = (arr - arr.min()) / max(float(arr.max() - arr.min()), 1e-9)
    else:
        arr = ndimage.binary_dilation(arr > 0, iterations=3).astype(float)
    rgb = np.zeros((size, size, 3), dtype=np.uint8)
    rgb[..., 0] = np.clip(arr * 230, 0, 255).astype(np.uint8)
    rgb[..., 1] = np.clip(arr * (180 if mode == "density" else 220), 0, 255).astype(np.uint8)
    rgb[..., 2] = np.clip((1.0 - arr) * 90 + arr * 80, 0, 255).astype(np.uint8)
    _write_png(rgb, path)


def _terrain_contours(
    terrain: TerrainModel,
    *,
    levels: int = 9,
    grid: int = 90,
) -> list[LineString]:
    xs = np.linspace(terrain.lo[0], terrain.lo[0] + terrain.span[0], grid)
    ys = np.linspace(terrain.lo[1], terrain.lo[1] + terrain.span[1], grid)
    zz = np.zeros((grid, grid), dtype=np.float64)
    for r, y in enumerate(ys):
        for c, x in enumerate(xs):
            zz[r, c] = terrain.sample_local(float(x), float(y))
    zmin = float(np.quantile(zz, 0.08))
    zmax = float(np.quantile(zz, 0.92))
    if zmax <= zmin:
        return []
    contour_levels = np.linspace(zmin, zmax, levels + 2)[1:-1]
    center = np.asarray(terrain.center, dtype=np.float64)
    axes = np.column_stack(
        [
            np.asarray(terrain.axes[0], dtype=np.float64),
            np.asarray(terrain.axes[1], dtype=np.float64),
        ]
    )

    def interp(
        p0: tuple[float, float],
        p1: tuple[float, float],
        z0: float,
        z1: float,
        level: float,
    ) -> tuple[float, float]:
        t = 0.5 if abs(z1 - z0) <= 1e-12 else (level - z0) / (z1 - z0)
        t = max(0.0, min(1.0, float(t)))
        return (p0[0] * (1 - t) + p1[0] * t, p0[1] * (1 - t) + p1[1] * t)

    lines: list[LineString] = []
    for level in contour_levels:
        for r in range(grid - 1):
            for c in range(grid - 1):
                corners = [
                    ((xs[c], ys[r]), float(zz[r, c])),
                    ((xs[c + 1], ys[r]), float(zz[r, c + 1])),
                    ((xs[c + 1], ys[r + 1]), float(zz[r + 1, c + 1])),
                    ((xs[c], ys[r + 1]), float(zz[r + 1, c])),
                ]
                hits: list[tuple[float, float]] = []
                for (p0, z0), (p1, z1) in zip(
                    corners, corners[1:] + corners[:1], strict=True
                ):
                    if (
                        ((z0 <= level <= z1) or (z1 <= level <= z0))
                        and abs(z1 - z0) > 1e-12
                    ):
                        hits.append(interp(p0, p1, z0, z1, float(level)))
                if len(hits) == 2:
                    world = _to_world(np.asarray(hits, dtype=np.float64), center, axes)
                    line = LineString(world.tolist())
                    if line.length > 1e-9:
                        lines.append(line)
                elif len(hits) == 4:
                    for pair in ((hits[0], hits[1]), (hits[2], hits[3])):
                        world = _to_world(np.asarray(pair, dtype=np.float64), center, axes)
                        line = LineString(world.tolist())
                        if line.length > 1e-9:
                            lines.append(line)
    return lines


def _write_png(rgb: np.ndarray, path: Path) -> None:
    h, w, _c = rgb.shape
    raw = b"".join(b"\x00" + rgb[y].tobytes() for y in range(h))

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    png = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, 6))
        + chunk(b"IEND", b"")
    )
    path.write_bytes(png)


def _render_svg(
    path: Path,
    *,
    points: np.ndarray,
    roads: list[RoadRec],
    blocks: list[BlockRec],
    parcels: list[Polygon],
    buildings: list[Polygon],
    title: str,
    contours: list[LineString] | None = None,
) -> None:
    contours = contours or []
    geoms = (
        [r.geom for r in roads]
        + [b.geom for b in blocks]
        + parcels
        + buildings
        + contours
    )
    if len(points):
        geoms.extend(Point(float(x), float(y)) for x, y in points)
    bounds = unary_union(geoms).bounds if geoms else (0, 0, 1, 1)
    minx, miny, maxx, maxy = bounds
    pad = max(maxx - minx, maxy - miny, 1.0) * 0.05
    minx -= pad
    miny -= pad
    maxx += pad
    maxy += pad
    width = 1200
    height = 900

    def tx(x: float) -> float:
        return (x - minx) / max(maxx - minx, 1e-9) * width

    def ty(y: float) -> float:
        return height - (y - miny) / max(maxy - miny, 1e-9) * height

    def poly_points(poly: Polygon) -> str:
        return " ".join(f"{tx(x):.2f},{ty(y):.2f}" for x, y in poly.exterior.coords)

    def line_points(line: LineString) -> str:
        return " ".join(f"{tx(x):.2f},{ty(y):.2f}" for x, y in line.coords)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#f6f3ea"/>',
        f'<text x="24" y="36" font-family="sans-serif" font-size="22" fill="#222">{title}</text>',
    ]
    for block in blocks:
        parts.append(f'<polygon points="{poly_points(block.geom)}" fill="#e2dccd" stroke="#d1c7b4" stroke-width="1"/>')
    for contour in contours:
        parts.append(f'<polyline points="{line_points(contour)}" fill="none" stroke="#b8ad93" stroke-width="0.7" stroke-opacity="0.55"/>')
    for parcel in parcels:
        parts.append(f'<polygon points="{poly_points(parcel)}" fill="none" stroke="#8f8676" stroke-width="0.75" stroke-opacity="0.85"/>')
    for building in buildings:
        parts.append(f'<polygon points="{poly_points(building)}" fill="#785f4f" stroke="#4d3c33" stroke-width="0.8"/>')
    for road in roads:
        color = {
            "regional": "#cc513d",
            "regional_primary": "#c74432",
            "regional_secondary": "#d9834d",
            "collector": "#3b6b91",
            "district": "#8aa9b4",
            "local": "#f8f7f1",
        }.get(road.kind, "#333")
        sw = {
            "regional": 4,
            "regional_primary": 5,
            "regional_secondary": 3,
            "collector": 3,
            "district": 2,
            "local": 2,
        }.get(road.kind, 2)
        if road.kind in {"local", "district", "collector"}:
            parts.append(f'<polyline points="{line_points(road.geom)}" fill="none" stroke="#a9a296" stroke-width="{sw + 1.4}" stroke-linecap="round" stroke-linejoin="round" stroke-opacity="0.75"/>')
        parts.append(f'<polyline points="{line_points(road.geom)}" fill="none" stroke="{color}" stroke-width="{sw}" stroke-linecap="round"/>')
    for x, y in points:
        parts.append(f'<circle cx="{tx(float(x)):.2f}" cy="{ty(float(y)):.2f}" r="1.8" fill="#252525" opacity="0.45"/>')
    parts.append("</svg>")
    path.write_text("\n".join(parts))


def _render_png_from_svg_inputs(
    path: Path,
    *,
    points: np.ndarray,
    roads: list[RoadRec],
    blocks: list[BlockRec],
    parcels: list[Polygon],
    buildings: list[Polygon],
    contours: list[LineString] | None = None,
) -> None:
    # Lightweight inspection PNG: draw roads/buildings/points; SVG carries exact polygons.
    size = 900
    contours = contours or []
    geoms = (
        [r.geom for r in roads]
        + [b.geom for b in blocks]
        + parcels
        + buildings
        + contours
    )
    if len(points):
        geoms.extend(Point(float(x), float(y)) for x, y in points)
    minx, miny, maxx, maxy = unary_union(geoms).bounds if geoms else (0, 0, 1, 1)
    pad = max(maxx - minx, maxy - miny, 1.0) * 0.05
    minx -= pad
    miny -= pad
    maxx += pad
    maxy += pad
    img = np.full((size, size, 3), [246, 243, 234], dtype=np.uint8)

    def pix(p: tuple[float, float]) -> tuple[int, int]:
        x = int(round((p[0] - minx) / max(maxx - minx, 1e-9) * (size - 1)))
        y = int(round((1 - (p[1] - miny) / max(maxy - miny, 1e-9)) * (size - 1)))
        return max(0, min(size - 1, x)), max(0, min(size - 1, y))

    def draw_line(a: tuple[int, int], b: tuple[int, int], color: tuple[int, int, int], width: int) -> None:
        x0, y0 = a
        x1, y1 = b
        steps = max(abs(x1 - x0), abs(y1 - y0), 1)
        for t in np.linspace(0.0, 1.0, steps + 1):
            x = int(round(x0 * (1 - t) + x1 * t))
            y = int(round(y0 * (1 - t) + y1 * t))
            img[max(0, y - width) : min(size, y + width + 1), max(0, x - width) : min(size, x + width + 1)] = color

    for contour in contours:
        coords = list(contour.coords)
        for a, b in zip(coords[:-1], coords[1:], strict=False):
            draw_line(pix(a), pix(b), (184, 173, 147), 0)
    for parcel in parcels:
        coords = list(parcel.exterior.coords)
        for a, b in zip(coords[:-1], coords[1:], strict=False):
            draw_line(pix(a), pix(b), (143, 134, 118), 0)
    for road in roads:
        color = {
            "regional": (204, 81, 61),
            "regional_primary": (199, 68, 50),
            "regional_secondary": (217, 131, 77),
            "collector": (59, 107, 145),
            "district": (138, 169, 180),
            "local": (255, 255, 252),
        }.get(road.kind, (40, 40, 40))
        width = {
            "regional": 2,
            "regional_primary": 3,
            "regional_secondary": 2,
            "collector": 2,
            "district": 1,
            "local": 1,
        }.get(road.kind, 1)
        coords = list(road.geom.coords)
        for a, b in zip(coords[:-1], coords[1:], strict=False):
            if road.kind in {"local", "district", "collector"}:
                draw_line(pix(a), pix(b), (169, 162, 150), width + 1)
            draw_line(pix(a), pix(b), color, width)
    for poly in buildings:
        x, y = pix((poly.centroid.x, poly.centroid.y))
        img[max(0, y - 2) : min(size, y + 3), max(0, x - 2) : min(size, x + 3)] = (120, 95, 79)
    for x0, y0 in points:
        x, y = pix((float(x0), float(y0)))
        img[max(0, y - 1) : min(size, y + 2), max(0, x - 1) : min(size, x + 2)] = (30, 30, 30)
    _write_png(img, path)


def build_city_proto(args: argparse.Namespace) -> dict[str, Any]:
    args.out_dir.mkdir(parents=True, exist_ok=True)
    points_path = args.points or (args.in_dir / "app_points.parquet")
    points = pl.read_parquet(points_path)
    missing = [c for c in ("world_id", "x", "y") if c not in points.columns]
    if missing:
        raise SystemExit(f"points missing required columns: {missing}")
    if args.max_points and points.height > args.max_points:
        points = points.head(args.max_points)
    xy = points.select("x", "y").to_numpy().astype(np.float64)
    islands = _assign_islands(xy, args.island_eps_scale)
    points = points.with_columns(island_id=pl.Series(islands))
    crops = select_crops(
        xy,
        islands,
        max_crops=args.max_crops,
        radius_scale=args.crop_radius_scale,
    )
    if not crops:
        raise RuntimeError("no inspection crops selected")
    global_settlements = _global_settlements(
        xy,
        islands,
        max_islands=args.regional_max_islands,
        seed=args.seed,
    )
    regional_roads, _next_regional_road_id = _regional_roads(
        global_settlements,
        next_road_id=1,
    )

    all_roads: list[RoadRec] = []
    all_blocks: list[BlockRec] = []
    all_lots: list[LotRec] = []
    all_zones: list[ZoneRec] = []
    parcel_frames: list[pl.DataFrame] = []
    building_frames: list[pl.DataFrame] = []
    settlement_features: list[dict[str, Any]] = []
    terrain_by_crop: dict[str, TerrainModel] = {}
    settlement_features.extend(
        _geom_feature(
            Point(*settlement.center),
            {
                "scope": "global",
                "settlement_id": settlement.settlement_id,
                "class": settlement.settlement_class,
                "world_count": settlement.world_count,
                "island_id": settlement.island_id,
            },
        )
        for settlement in global_settlements
    )
    next_road_id = 1
    next_block_id = 1
    next_lot_id = 1
    next_zone_id = 1
    crop_manifest: list[dict[str, Any]] = []
    used_indices: set[int] = set()

    for crop in crops:
        center = np.asarray(crop.center, dtype=np.float64)
        in_crop = (islands == crop.island_id) & (np.linalg.norm(xy - center, axis=1) <= crop.radius)
        idx = np.asarray(
            [int(i) for i in np.flatnonzero(in_crop) if int(i) not in used_indices],
            dtype=np.int64,
        )
        if len(idx) > args.max_crop_worlds:
            order = np.argsort(np.linalg.norm(xy[idx] - center, axis=1))
            idx = idx[order[: args.max_crop_worlds]]
        if len(idx) < 3:
            continue
        used_indices.update(int(i) for i in idx.tolist())
        crop_points = points[idx]
        crop_xy = xy[idx]
        settlements, labels, classes = _classify_settlements(crop_xy, args.seed)
        for sid, (sx, sy) in enumerate(settlements):
            count = int(np.count_nonzero(labels == sid))
            settlement_features.append(
                _geom_feature(
                    Point(float(sx), float(sy)),
                    {
                        "scope": "crop",
                        "crop_id": crop.crop_id,
                        "settlement_id": sid,
                        "class": classes[sid],
                        "world_count": count,
                        "island_id": crop.island_id,
                    },
                )
            )
        regional = []
        (
            grid_roads,
            blocks,
            lots,
            zones,
            crop_terrain,
            next_road_id,
            next_block_id,
            next_lot_id,
            next_zone_id,
        ) = _domain_layout(
            crop_xy,
            crop,
            next_road_id=next_road_id,
            next_block_id=next_block_id,
            lot_start=next_lot_id,
            zone_start=next_zone_id,
            seed=args.seed,
            max_grade=args.terrain_max_grade,
        )
        roads = regional + grid_roads
        terrain_by_crop[crop.crop_id] = crop_terrain
        lots = _filter_lots_for_roads(
            lots,
            roads,
            clearance=_median_nn(crop_xy) * 0.16,
        )
        lots = _filter_lots_for_frontage(
            lots,
            roads,
            tolerance=1e-7,
        )
        lots = _filter_overlapping_lots(
            lots,
            tolerance=1e-7,
        )
        lots = _filter_building_road_conflicts(
            lots,
            roads,
            building_scale=args.building_scale,
        )
        parcels, buildings = _assign_worlds_to_lots(
            crop_points,
            crop_xy,
            lots,
            building_scale=args.building_scale,
            terrain=crop_terrain,
            roads=roads,
        )
        all_roads.extend(roads)
        all_blocks.extend(blocks)
        all_lots.extend(lots)
        all_zones.extend(zones)
        parcel_frames.append(parcels)
        building_frames.append(buildings)
        crop_manifest.append(
            {
                "crop_id": crop.crop_id,
                "kind": crop.kind,
                "island_id": crop.island_id,
                "center": [float(crop.center[0]), float(crop.center[1])],
                "radius": float(crop.radius),
                "world_count": int(len(crop_xy)),
                "settlement_count": int(len(settlements)),
            }
        )

    if not parcel_frames:
        raise RuntimeError("selected crops did not contain enough points to process")
    parcels_df = pl.concat(parcel_frames)
    buildings_df = pl.concat(building_frames)
    source_xy = parcels_df.join(points.select("world_id", "x", "y"), on="world_id").select("x_right", "y_right").to_numpy().astype(np.float64)

    land_geom, land_info = build_land_geometry(
        xy,
        raster_max_dim=args.land_raster_max_dim,
        raster_nn_cells=2.0,
        raster_dilate_cells=6,
        raster_close_cells=2,
        raster_simplify_cells=0.75,
    )
    land_features = [_geom_feature(land_geom, {"kind": "land", **land_info})]
    _write_geojson(land_features, args.out_dir / "land.geojson")
    _write_geojson(settlement_features, args.out_dir / "settlements.geojson")
    _write_geojson(
        [
            _geom_feature(
                r.geom,
                {
                    "road_id": r.road_id,
                    "crop_id": r.crop_id,
                    "kind": r.kind,
                    "bridge": r.bridge,
                    **_road_grade_props(r, terrain_by_crop.get(r.crop_id)),
                },
            )
            for r in all_roads
        ],
        args.out_dir / "roads.geojson",
    )
    _write_geojson(
        [
            _geom_feature(
                r.geom,
                {
                    "road_id": r.road_id,
                    "scope": "regional",
                    "kind": r.kind,
                    "bridge": r.bridge,
                },
            )
            for r in regional_roads
        ],
        args.out_dir / "regional_roads.geojson",
    )
    _write_geojson(
        [
            _geom_feature(
                b.geom,
                {"block_id": b.block_id, "crop_id": b.crop_id, "area": float(b.geom.area)},
            )
            for b in all_blocks
        ],
        args.out_dir / "blocks.geojson",
    )
    _write_geojson(
        [
            _geom_feature(
                z.geom,
                {
                    "zone_id": z.zone_id,
                    "crop_id": z.crop_id,
                    "kind": z.zone_kind,
                    "world_count": z.world_count,
                    "density": z.density,
                    "sparse_score": z.sparse_score,
                    "z_range": z.z_range,
                },
            )
            for z in all_zones
        ],
        args.out_dir / "zones.geojson",
    )
    parcels_df.write_parquet(args.out_dir / "parcels.parquet")
    buildings_df.write_parquet(args.out_dir / "buildings.parquet")

    # Also emit GeoJSON for quick GIS inspection even though Parquet is canonical.
    _write_geojson(
        [
            _geom_feature(
                shapely.from_wkt(row["geometry_wkt"]),
                {k: v for k, v in row.items() if k != "geometry_wkt"},
            )
            for row in parcels_df.to_dicts()
        ],
        args.out_dir / "parcels.geojson",
    )

    metrics = _hard_metrics(source_xy, parcels_df, buildings_df, all_roads, all_blocks)
    metrics.update(_road_degree_metrics(all_roads))
    metrics.update(_road_grade_metrics(all_roads, terrain_by_crop))
    metrics.update(
        {
            "input_worlds": int(points.height),
            "crop_count": int(len(crop_manifest)),
            "island_count": int(len(set(islands.tolist()))),
            "largest_island_share": float(np.bincount(islands).max() / max(len(islands), 1)),
            "global_settlement_count": int(len(global_settlements)),
            "candidate_lot_count": int(len(all_lots)),
            "zone_count": int(len(all_zones)),
            "block_count": int(len(all_blocks)),
            "road_count": int(len(all_roads)),
            "regional_road_count": int(len(regional_roads)),
        }
    )
    for kind in sorted({z.zone_kind for z in all_zones}):
        metrics[f"zone_{kind}_count"] = int(
            sum(1 for z in all_zones if z.zone_kind == kind)
        )
    regional_graph_metrics = _road_degree_metrics(regional_roads)
    metrics.update(
        {f"regional_{key}": value for key, value in regional_graph_metrics.items()}
    )
    hard_failures = [
        "assignment_rate",
        "duplicate_lot_assignments",
        "building_outside_parcel",
        "parcel_overlap_count",
        "parcel_no_frontage",
        "building_road_intersections",
    ]
    metrics["hard_metrics_pass"] = bool(
        metrics["assignment_rate"] == 1.0
        and metrics["duplicate_lot_assignments"] == 0
        and metrics["building_outside_parcel"] == 0
        and metrics["parcel_overlap_count"] == 0
        and metrics["parcel_no_frontage"] == 0
        and metrics["building_road_intersections"] == 0
    )
    metrics["hard_metric_keys"] = hard_failures
    (args.out_dir / "layout_metrics.json").write_text(json.dumps(metrics, indent=2))

    dr_metrics = {
        "note": "city-proto consumes existing DR coordinates; run mapgen-dr-sweep for variant comparisons",
        "point_count": int(points.height),
        "density": _density_summary(xy),
    }
    (args.out_dir / "dr_metrics.json").write_text(json.dumps(dr_metrics, indent=2))
    _raster_debug(xy, args.out_dir / "debug_density.png", mode="density")
    _raster_debug(xy, args.out_dir / "debug_height.png", mode="height")

    overview_points = np.asarray(
        [settlement.center for settlement in global_settlements],
        dtype=np.float64,
    )
    _render_svg(
        args.out_dir / "overview_roads.svg",
        points=overview_points,
        roads=regional_roads,
        blocks=[],
        parcels=[],
        buildings=[],
        title="Overview Roads",
    )
    _render_png_from_svg_inputs(
        args.out_dir / "overview_roads.png",
        points=overview_points,
        roads=regional_roads,
        blocks=[],
        parcels=[],
        buildings=[],
    )
    for crop_info in crop_manifest:
        cid = crop_info["crop_id"]
        crop_points = parcels_df.filter(pl.col("crop_id") == cid).select("x", "y").to_numpy().astype(np.float64)
        crop_roads = [r for r in all_roads if r.crop_id == cid]
        crop_blocks = [b for b in all_blocks if b.crop_id == cid]
        crop_parcels = [
            shapely.from_wkt(w)
            for w in parcels_df.filter(pl.col("crop_id") == cid)["geometry_wkt"].to_list()
        ]
        crop_buildings = [
            shapely.from_wkt(w)
            for w in buildings_df.filter(pl.col("crop_id") == cid)["geometry_wkt"].to_list()
        ]
        crop_contours = (
            _terrain_contours(terrain_by_crop[cid])
            if cid in terrain_by_crop
            else []
        )
        _render_svg(
            args.out_dir / f"{cid}_roads_blocks.svg",
            points=crop_points,
            roads=crop_roads,
            blocks=crop_blocks,
            parcels=[],
            buildings=[],
            title=f"{cid} roads blocks",
            contours=crop_contours,
        )
        _render_png_from_svg_inputs(
            args.out_dir / f"{cid}_roads_blocks.png",
            points=crop_points,
            roads=crop_roads,
            blocks=crop_blocks,
            parcels=[],
            buildings=[],
            contours=crop_contours,
        )
        _render_svg(
            args.out_dir / f"{cid}_parcels_buildings.svg",
            points=crop_points,
            roads=crop_roads,
            blocks=crop_blocks,
            parcels=crop_parcels,
            buildings=crop_buildings,
            title=f"{cid} parcels buildings",
            contours=crop_contours,
        )
        _render_png_from_svg_inputs(
            args.out_dir / f"{cid}_parcels_buildings.png",
            points=crop_points,
            roads=crop_roads,
            blocks=crop_blocks,
            parcels=crop_parcels,
            buildings=crop_buildings,
            contours=crop_contours,
        )

    manifest = {
        "version": 1,
        "layout": "city-proto-game-compressed",
        "input_points": str(points_path),
        "crops": crop_manifest,
        "assets": {
            "metrics": "layout_metrics.json",
            "dr_metrics": "dr_metrics.json",
            "settlements": "settlements.geojson",
            "roads": "roads.geojson",
            "regional_roads": "regional_roads.geojson",
            "blocks": "blocks.geojson",
            "zones": "zones.geojson",
            "parcels": "parcels.parquet",
            "buildings": "buildings.parquet",
            "overview_roads": ["overview_roads.png", "overview_roads.svg"],
            "debug_density": "debug_density.png",
            "debug_height": "debug_height.png",
        },
    }
    (args.out_dir / "proto_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"  wrote {args.out_dir / 'proto_manifest.json'}")
    return metrics


def _density_summary(xy: np.ndarray) -> dict[str, float]:
    if len(xy) < 2:
        return {"density_q50": 0.0, "density_q99": 0.0, "density_q99_q50": 0.0}
    d, _idx = cKDTree(xy).query(xy, k=2, workers=-1)
    density = 1.0 / np.maximum(d[:, 1], 1e-9) ** 2
    q50 = float(np.quantile(density, 0.50))
    q99 = float(np.quantile(density, 0.99))
    return {"density_q50": q50, "density_q99": q99, "density_q99_q50": float(q99 / max(q50, 1e-12))}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in-dir", type=Path, default=Path("."))
    ap.add_argument("--points", type=Path)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--max-points", type=int, default=0)
    ap.add_argument("--max-crop-worlds", type=int, default=1600)
    ap.add_argument("--max-crops", type=int, default=8)
    ap.add_argument("--crop-radius-scale", type=float, default=65.0)
    ap.add_argument("--island-eps-scale", type=float, default=8.0)
    ap.add_argument("--regional-max-islands", type=int, default=12)
    ap.add_argument("--terrain-max-grade", type=float, default=0.08)
    ap.add_argument("--building-scale", type=float, default=0.56)
    ap.add_argument("--land-raster-max-dim", type=int, default=1536)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    build_city_proto(args)


if __name__ == "__main__":
    main()
