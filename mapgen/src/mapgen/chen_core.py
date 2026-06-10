"""Strict Chen 2024 layout core data structures and invariants.

This module is intentionally generation-free. It defines the graph/mesh state
that later Chen/Yang implementation slices should maintain, plus formula and
invariant helpers that are small enough to unit test directly.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
from shapely import (
    LineString,
    MultiPolygon,
    Polygon,
)
from shapely import (
    Point as ShapelyPoint,
)
from shapely.ops import unary_union
from shapely.validation import explain_validity

CHEN_LAMBDA_SIZE = 0.30
CHEN_LAMBDA_REGULARITY = 0.50
CHEN_LAMBDA_ACCESS = 0.20
CHEN_GAMMA_ANGLE = 0.75
CHEN_GAMMA_LENGTH = 0.25
CHEN_ACCESS_RATIO_TAU = 0.50
CHEN_COLLINEAR_THRESHOLD_RAD = math.radians(135.0)
CHEN_SHORT_EDGE_RATIO = 0.20
NODE_SCALE = 1_000_000

Point = tuple[float, float]
EdgeKey = tuple[int, int]


@dataclass(frozen=True)
class ChenSplitWeights:
    size: float = CHEN_LAMBDA_SIZE
    regularity: float = CHEN_LAMBDA_REGULARITY
    access: float = CHEN_LAMBDA_ACCESS


DEFAULT_SPLIT_WEIGHTS = ChenSplitWeights()


@dataclass(frozen=True)
class ChenSplitScore:
    q_size: float
    q_regu: float
    q_acce: float
    access_ratio_min: float
    total: float


@dataclass(frozen=True)
class MeshVertex:
    vertex_id: int
    point: Point
    on_boundary: bool = False


@dataclass(frozen=True)
class ParcelFace:
    parcel_id: int
    ring: tuple[int, ...]
    geom: Polygon


@dataclass(frozen=True)
class ParcelMesh:
    vertices: dict[int, MeshVertex]
    parcels: dict[int, ParcelFace]

    @property
    def edges(self) -> set[EdgeKey]:
        out: set[EdgeKey] = set()
        for parcel in self.parcels.values():
            out.update(ring_edges(parcel.ring))
        return out


@dataclass(frozen=True)
class ParcelCornerGraph:
    vertices: dict[int, MeshVertex]
    edges: set[EdgeKey]
    edge_paths: dict[EdgeKey, tuple[int, ...]]
    parcel_corner_rings: dict[int, tuple[int, ...]]
    parcel_approx_points: dict[int, tuple[Point, ...]]


@dataclass(frozen=True)
class ParcelGraph:
    neighbors: dict[int, set[int]]
    shared_edges: dict[tuple[int, int], set[EdgeKey]]
    edge_paths: dict[EdgeKey, tuple[int, ...]]
    parcel_corner_rings: dict[int, tuple[int, ...]]
    parcel_approx_points: dict[int, tuple[Point, ...]]


@dataclass(frozen=True)
class StreetNetworkGraph:
    edges: set[EdgeKey]

    @property
    def vertices(self) -> set[int]:
        return {vertex for edge in self.edges for vertex in edge}


@dataclass(frozen=True)
class StreetPath:
    street_id: int
    nodes: tuple[int, ...]
    edges: tuple[EdgeKey, ...]


@dataclass(frozen=True)
class StreetGraph:
    streets: tuple[StreetPath, ...]
    junctions: dict[int, set[int]]


@dataclass(frozen=True)
class ChenLayout:
    mesh: ParcelMesh
    corner_graph: ParcelCornerGraph
    parcel_graph: ParcelGraph
    street_network: StreetNetworkGraph
    street_graph: StreetGraph


@dataclass(frozen=True)
class ChenInvariantReport:
    paper_invariant_pass: bool
    geometry_valid_pass: bool
    chen_formula_pass: bool
    diagnostic_metric_pass: bool
    metrics: dict[str, Any]


@dataclass(frozen=True)
class ChenShortEdgeCandidate:
    edge: EdgeKey
    parcel_ids: tuple[int, int]
    path: tuple[int, ...]
    path_points: tuple[Point, ...]
    path_length: float
    threshold: float
    avg_side_lengths: tuple[float, float]
    midpoint: Point


@dataclass(frozen=True)
class ChenFig7CleanupResult:
    layout: ChenLayout
    metrics: dict[str, Any]


@dataclass(frozen=True)
class _Fig7CleanupAttempt:
    layout: ChenLayout | None
    failure_reason: str | None = None
    merge_point_mode: str | None = None
    projection_distance: float = 0.0
    failure_detail: str | None = None
    retained_full_mesh_ring_parcel_ids: tuple[int, ...] = ()
    failure_parcel_id: int | None = None
    failure_validity_reason: str | None = None
    operation_scope: str | None = None


def node_key(point: Point) -> tuple[int, int]:
    return int(round(point[0] * NODE_SCALE)), int(round(point[1] * NODE_SCALE))


def normalized_edge(a: int, b: int) -> EdgeKey:
    if a == b:
        raise ValueError("self edges are not valid in the parcel corner graph")
    return (a, b) if a < b else (b, a)


def ring_edges(ring: tuple[int, ...]) -> list[EdgeKey]:
    if len(ring) < 3:
        return []
    return [
        normalized_edge(a, b) for a, b in zip(ring, ring[1:] + ring[:1], strict=True)
    ]


def polygon_exterior_points(poly: Polygon) -> list[Point]:
    return [(float(x), float(y)) for x, y in poly.exterior.coords[:-1]]


def point_edges(points: list[Point] | tuple[Point, ...]) -> list[tuple[Point, Point]]:
    if len(points) < 3:
        return []
    return list(zip(points, points[1:] + points[:1], strict=True))


def polygon_edges(poly: Polygon) -> list[tuple[Point, Point]]:
    return point_edges(polygon_exterior_points(poly))


def signed_area(points: list[Point] | tuple[Point, ...]) -> float:
    return 0.5 * sum(
        a[0] * b[1] - b[0] * a[1]
        for a, b in zip(points, points[1:] + points[:1], strict=True)
    )


def ccw_points(points: list[Point]) -> list[Point]:
    return points if signed_area(points) >= 0.0 else list(reversed(points))


def included_angle_unsigned(prev: Point, point: Point, nxt: Point) -> float:
    a0 = math.atan2(prev[1] - point[1], prev[0] - point[0])
    a1 = math.atan2(nxt[1] - point[1], nxt[0] - point[0])
    return abs(math.atan2(math.sin(a1 - a0), math.cos(a1 - a0)))


def interior_angle_ccw(prev: Point, point: Point, nxt: Point) -> float:
    incoming = (point[0] - prev[0], point[1] - prev[1])
    outgoing = (nxt[0] - point[0], nxt[1] - point[1])
    turn = math.atan2(
        incoming[0] * outgoing[1] - incoming[1] * outgoing[0],
        incoming[0] * outgoing[0] + incoming[1] * outgoing[1],
    )
    interior = math.pi - turn
    if interior <= 0.0:
        interior += 2.0 * math.pi
    return interior


def largest_polygon(geom) -> Polygon | None:
    if isinstance(geom, Polygon):
        return geom if geom.area > 1e-12 else None
    if isinstance(geom, MultiPolygon):
        polys = [poly for poly in geom.geoms if poly.area > 1e-12]
        return max(polys, key=lambda poly: poly.area) if polys else None
    return None


def approximate_polygon_points(poly: Polygon) -> list[Point]:
    """Return Chen's approximate polygon by dropping near-collinear vertices."""
    coords = polygon_exterior_points(poly)
    if len(coords) <= 3:
        return coords
    approx: list[Point] = []
    for i, point in enumerate(coords):
        prev = coords[i - 1]
        nxt = coords[(i + 1) % len(coords)]
        if included_angle_unsigned(prev, point, nxt) > CHEN_COLLINEAR_THRESHOLD_RAD:
            continue
        approx.append(point)
    return approx if len(approx) >= 3 else coords


def chen_irregularity(poly: Polygon) -> float:
    """Chen 2024 Eq. 1 irregularity on the approximate polygon."""
    coords = ccw_points(approximate_polygon_points(poly))
    n = len(coords)
    if n < 3:
        return 0.0

    lengths: list[float] = []
    angles: list[float] = []
    for i, point in enumerate(coords):
        prev = coords[i - 1]
        nxt = coords[(i + 1) % n]
        lengths.append(math.hypot(nxt[0] - point[0], nxt[1] - point[1]))
        angles.append(interior_angle_ccw(prev, point, nxt))

    mean_angle = (n - 2) * math.pi / n
    mean_length = float(np.mean(lengths)) if lengths else 0.0
    if mean_length <= 1e-12:
        return 0.0
    angle_term = float(np.mean([(angle - mean_angle) ** 2 for angle in angles]))
    length_term = float(
        np.mean([(length - mean_length) ** 2 for length in lengths])
        / (mean_length * mean_length)
    )
    return CHEN_GAMMA_ANGLE * angle_term + CHEN_GAMMA_LENGTH * length_term


def line_segments(line: LineString) -> list[tuple[Point, Point]]:
    coords = [(float(x), float(y)) for x, y in line.coords]
    return list(zip(coords, coords[1:], strict=False))


def segment_overlap_interval_on_edge(
    edge_start: Point,
    edge_end: Point,
    access_start: Point,
    access_end: Point,
    tolerance: float,
) -> tuple[float, float] | None:
    ex = edge_end[0] - edge_start[0]
    ey = edge_end[1] - edge_start[1]
    ax = access_end[0] - access_start[0]
    ay = access_end[1] - access_start[1]
    edge_length = math.hypot(ex, ey)
    access_length = math.hypot(ax, ay)
    if edge_length <= 1e-12 or access_length <= 1e-12:
        return None

    parallel_error = abs(ex * ay - ey * ax) / (edge_length * access_length)
    if parallel_error > 1e-6:
        return None

    def line_distance(point: Point) -> float:
        px = point[0] - edge_start[0]
        py = point[1] - edge_start[1]
        return abs(ex * py - ey * px) / edge_length

    if max(line_distance(access_start), line_distance(access_end)) > tolerance:
        return None

    ux = ex / edge_length
    uy = ey / edge_length

    def project(point: Point) -> float:
        return (point[0] - edge_start[0]) * ux + (point[1] - edge_start[1]) * uy

    a = project(access_start)
    b = project(access_end)
    lo = max(0.0, min(a, b))
    hi = min(edge_length, max(a, b))
    if hi <= lo:
        return None
    return lo, hi


def merged_interval_length(intervals: list[tuple[float, float]]) -> float:
    if not intervals:
        return 0.0
    intervals = sorted(intervals)
    merged: list[tuple[float, float]] = [intervals[0]]
    for lo, hi in intervals[1:]:
        prev_lo, prev_hi = merged[-1]
        if lo <= prev_hi + 1e-9:
            merged[-1] = (prev_lo, max(prev_hi, hi))
        else:
            merged.append((lo, hi))
    return sum(hi - lo for lo, hi in merged)


def line_access_overlap_length(
    line: LineString, access_lines: list[LineString], tolerance: float
) -> float:
    overlap_length = 0.0
    for edge_start, edge_end in line_segments(line):
        intervals: list[tuple[float, float]] = []
        for access_line in access_lines:
            for access_start, access_end in line_segments(access_line):
                interval = segment_overlap_interval_on_edge(
                    edge_start,
                    edge_end,
                    access_start,
                    access_end,
                    tolerance,
                )
                if interval is not None:
                    intervals.append(interval)
        overlap_length += merged_interval_length(intervals)
    return overlap_length


def polygon_street_access_ratio(
    poly: Polygon,
    access_lines: list[LineString],
    tolerance: float,
) -> float:
    side_edges = point_edges(approximate_polygon_points(poly))
    side_lengths = [math.hypot(b[0] - a[0], b[1] - a[1]) for a, b in side_edges]
    avg_side = float(np.mean(side_lengths)) if side_lengths else 0.0
    if avg_side <= 1e-12:
        return 0.0
    access_length = sum(
        line_access_overlap_length(LineString([a, b]), access_lines, tolerance)
        for a, b in side_edges
    )
    return access_length / avg_side


def parcel_access_ratio_from_path_geometry(
    parcel: ParcelFace,
    mesh: ParcelMesh,
    corner_graph: ParcelCornerGraph,
    access_lines: list[LineString],
    *,
    corner_ring: tuple[int, ...] | None = None,
    tolerance: float = 1e-7,
) -> float:
    """Compute Eq. 2 access using exact side paths from the corner graph."""
    corner_ring = corner_ring or corner_graph.parcel_corner_rings.get(parcel.parcel_id)
    if corner_ring is None:
        return polygon_street_access_ratio(parcel.geom, access_lines, tolerance)

    side_lengths: list[float] = []
    access_length = 0.0
    for edge in ring_edges(corner_ring):
        path = corner_edge_path(corner_graph, edge)
        points = tuple(mesh.vertices[node].point for node in path)
        length = vertex_path_length(mesh, path)
        side_lengths.append(length)
        access_length += line_access_overlap_length(
            LineString(points), access_lines, tolerance
        )

    avg_side = float(np.mean(side_lengths)) if side_lengths else 0.0
    return access_length / max(avg_side, 1e-12)


def chen_access_tolerance(target_area: float) -> float:
    return max(math.sqrt(max(target_area, 1e-12)) * 1e-6, 1e-7)


def chen_split_score_from_access_ratios(
    parts: tuple[Polygon, Polygon],
    access_ratios: list[float] | tuple[float, ...],
    *,
    target_area: float,
    weights: ChenSplitWeights | None = None,
    tau: float = CHEN_ACCESS_RATIO_TAU,
) -> ChenSplitScore:
    weights = weights or DEFAULT_SPLIT_WEIGHTS
    areas = sorted([float(part.area) for part in parts])
    q_size = areas[0] / max(areas[1], 1e-12)
    q_regu = 1.0 / (1.0 + sum(chen_irregularity(part) for part in parts))
    access_ratio_min = min(access_ratios) if access_ratios else 0.0
    q_acce = 2.0 if access_ratio_min >= tau else 1.0
    total = (
        weights.size * q_size + weights.regularity * q_regu + weights.access * q_acce
    )
    return ChenSplitScore(q_size, q_regu, q_acce, access_ratio_min, total)


def chen_split_score(
    parts: tuple[Polygon, Polygon],
    access_lines: list[LineString],
    *,
    target_area: float,
    weights: ChenSplitWeights | None = None,
    tau: float = CHEN_ACCESS_RATIO_TAU,
) -> ChenSplitScore:
    tolerance = chen_access_tolerance(target_area)
    access_ratios = [
        polygon_street_access_ratio(part, access_lines, tolerance) for part in parts
    ]
    return chen_split_score_from_access_ratios(
        parts,
        access_ratios,
        target_area=target_area,
        weights=weights,
        tau=tau,
    )


def parcel_mesh_from_polygons(
    polygons: list[tuple[int, Polygon]],
    *,
    boundary: Polygon | None = None,
    boundary_tolerance: float = 1e-7,
) -> ParcelMesh:
    node_ids: dict[tuple[int, int], int] = {}
    vertices: dict[int, MeshVertex] = {}
    parcels: dict[int, ParcelFace] = {}

    def vertex_id(point: Point) -> int:
        key = node_key(point)
        found = node_ids.get(key)
        if found is not None:
            return found
        new_id = len(node_ids)
        node_ids[key] = new_id
        on_boundary = (
            boundary is not None
            and LineString(boundary.exterior.coords).distance(ShapelyPoint(point))
            <= boundary_tolerance
        )
        vertices[new_id] = MeshVertex(new_id, point, on_boundary)
        return new_id

    for parcel_id, poly in polygons:
        if parcel_id in parcels:
            raise ValueError(f"duplicate parcel id: {parcel_id}")
        if not poly.is_valid or poly.area <= 1e-12:
            raise ValueError(f"invalid parcel polygon: {parcel_id}")
        ring = tuple(
            vertex_id(point) for point in ccw_points(polygon_exterior_points(poly))
        )
        parcels[parcel_id] = ParcelFace(parcel_id, ring, poly)

    return ParcelMesh(vertices, parcels)


def mesh_adjacency(mesh: ParcelMesh) -> dict[int, set[int]]:
    adjacency: dict[int, set[int]] = {vertex_id: set() for vertex_id in mesh.vertices}
    for a, b in mesh.edges:
        adjacency[a].add(b)
        adjacency[b].add(a)
    return adjacency


def parcel_corner_candidate_ids(mesh: ParcelMesh) -> set[int]:
    adjacency = mesh_adjacency(mesh)
    candidates: set[int] = {
        vertex_id for vertex_id, neighbors in adjacency.items() if len(neighbors) != 2
    }
    for parcel in mesh.parcels.values():
        ring = parcel.ring
        if len(ring) < 3:
            continue
        for i, vertex_id in enumerate(ring):
            prev = mesh.vertices[ring[i - 1]].point
            point = mesh.vertices[vertex_id].point
            nxt = mesh.vertices[ring[(i + 1) % len(ring)]].point
            if (
                included_angle_unsigned(prev, point, nxt)
                <= CHEN_COLLINEAR_THRESHOLD_RAD
            ):
                candidates.add(vertex_id)
    return candidates


def parcel_corner_ring(
    parcel: ParcelFace,
    mesh: ParcelMesh,
    corner_ids: set[int],
) -> tuple[int, ...]:
    ring, _retained_full_mesh_ring = _parcel_corner_ring_with_retention(
        parcel,
        corner_ids,
    )
    return ring


def _parcel_corner_ring_with_retention(
    parcel: ParcelFace,
    corner_ids: set[int],
) -> tuple[tuple[int, ...], bool]:
    ring = tuple(vertex_id for vertex_id in parcel.ring if vertex_id in corner_ids)
    if len(ring) >= 3:
        return ring, False

    # Match approximate_polygon_points(): if Chen's 135-degree collinearity
    # simplification would collapse a still-valid parcel below a polygonal face,
    # retain its local mesh ring instead of fabricating geometry or deleting it.
    if len(parcel.ring) >= 3:
        return parcel.ring, True
    raise ValueError(
        f"parcel has fewer than three corner-graph nodes: {parcel.parcel_id}"
    )


def _parcel_corner_ring_retention_parcel_ids(mesh: ParcelMesh) -> tuple[int, ...]:
    corner_ids = parcel_corner_candidate_ids(mesh)
    retained: list[int] = []
    for parcel in mesh.parcels.values():
        ring, retained_full_mesh_ring = _parcel_corner_ring_with_retention(
            parcel,
            corner_ids,
        )
        if retained_full_mesh_ring:
            retained.append(int(parcel.parcel_id))
        corner_ids.update(ring)
    return tuple(retained)


def ring_path_between(
    ring: tuple[int, ...],
    start: int,
    end: int,
) -> tuple[int, ...]:
    """Return the forward path from ``start`` to ``end`` along a closed ring."""
    if start == end:
        raise ValueError("corner edge endpoints must be distinct")
    if not ring:
        raise ValueError("cannot find a path in an empty ring")
    try:
        index = ring.index(start)
    except ValueError as exc:
        raise ValueError(f"ring does not contain start vertex: {start}") from exc

    path = [start]
    for offset in range(1, len(ring) + 1):
        vertex_id = ring[(index + offset) % len(ring)]
        path.append(vertex_id)
        if vertex_id == end:
            return tuple(path)
    raise ValueError(f"ring does not contain end vertex: {end}")


def normalized_path_for_edge(path: tuple[int, ...]) -> tuple[int, ...]:
    if len(path) < 2:
        raise ValueError("corner edge paths must contain at least two vertices")
    edge = normalized_edge(path[0], path[-1])
    return path if (path[0], path[-1]) == edge else tuple(reversed(path))


def parcel_corner_edge_paths(
    parcel: ParcelFace,
    corner_ring: tuple[int, ...],
) -> dict[EdgeKey, tuple[int, ...]]:
    paths: dict[EdgeKey, tuple[int, ...]] = {}
    for start, end in zip(corner_ring, corner_ring[1:] + corner_ring[:1], strict=True):
        path = ring_path_between(parcel.ring, start, end)
        edge = normalized_edge(start, end)
        paths[edge] = normalized_path_for_edge(path)
    return paths


def parcel_corner_graph(mesh: ParcelMesh) -> ParcelCornerGraph:
    corner_ids = parcel_corner_candidate_ids(mesh)
    parcel_corner_rings: dict[int, tuple[int, ...]] = {}
    for parcel in mesh.parcels.values():
        ring, _retained_full_mesh_ring = _parcel_corner_ring_with_retention(
            parcel,
            corner_ids,
        )
        parcel_corner_rings[parcel.parcel_id] = ring
        corner_ids.update(ring)
    edges: set[EdgeKey] = set()
    edge_paths: dict[EdgeKey, tuple[int, ...]] = {}
    for parcel_id, ring in parcel_corner_rings.items():
        edges.update(ring_edges(ring))
        for edge, path in parcel_corner_edge_paths(
            mesh.parcels[parcel_id], ring
        ).items():
            existing = edge_paths.get(edge)
            if existing is not None and existing != path:
                raise ValueError(
                    "non-conforming parcel corner edge path for edge "
                    f"{edge}: {existing} != {path}"
                )
            edge_paths[edge] = path
    vertices = {vertex_id: mesh.vertices[vertex_id] for vertex_id in corner_ids}
    parcel_approx_points = {
        parcel.parcel_id: tuple(approximate_polygon_points(parcel.geom))
        for parcel in mesh.parcels.values()
    }
    return ParcelCornerGraph(
        vertices,
        edges,
        edge_paths,
        parcel_corner_rings,
        parcel_approx_points,
    )


def edge_length(mesh: ParcelMesh, edge: EdgeKey) -> float:
    a, b = edge
    pa = mesh.vertices[a].point
    pb = mesh.vertices[b].point
    return math.hypot(pb[0] - pa[0], pb[1] - pa[1])


def vertex_path_length(mesh: ParcelMesh, path: tuple[int, ...]) -> float:
    return sum(
        edge_length(mesh, normalized_edge(a, b))
        for a, b in zip(path, path[1:], strict=False)
    )


def corner_edge_path(
    corner_graph: ParcelCornerGraph,
    edge: EdgeKey,
) -> tuple[int, ...]:
    normalized = normalized_edge(*edge)
    path = corner_graph.edge_paths.get(normalized)
    if path is None:
        raise KeyError(f"corner edge is not in the corner graph: {edge}")
    return path


def corner_edge_path_points(
    mesh: ParcelMesh,
    corner_graph: ParcelCornerGraph,
    edge: EdgeKey,
) -> tuple[Point, ...]:
    return tuple(
        mesh.vertices[node].point for node in corner_edge_path(corner_graph, edge)
    )


def edge_path_length(
    mesh: ParcelMesh,
    corner_graph: ParcelCornerGraph,
    edge: EdgeKey,
) -> float:
    return vertex_path_length(mesh, corner_edge_path(corner_graph, edge))


def vertex_path_midpoint(mesh: ParcelMesh, path: tuple[int, ...]) -> Point:
    if not path:
        raise ValueError("cannot compute midpoint for an empty path")
    if len(path) == 1:
        return mesh.vertices[path[0]].point

    total_length = vertex_path_length(mesh, path)
    if total_length <= 1e-12:
        return mesh.vertices[path[0]].point

    target = total_length * 0.5
    walked = 0.0
    for start, end in zip(path, path[1:], strict=False):
        start_point = mesh.vertices[start].point
        end_point = mesh.vertices[end].point
        segment_length = edge_length(mesh, normalized_edge(start, end))
        if walked + segment_length >= target:
            t = (target - walked) / max(segment_length, 1e-12)
            return (
                start_point[0] + (end_point[0] - start_point[0]) * t,
                start_point[1] + (end_point[1] - start_point[1]) * t,
            )
        walked += segment_length
    return mesh.vertices[path[-1]].point


def average_approximate_side_length(points: tuple[Point, ...]) -> float:
    side_lengths = [
        math.hypot(end[0] - start[0], end[1] - start[1])
        for start, end in point_edges(points)
    ]
    return float(np.mean(side_lengths)) if side_lengths else 0.0


def chen_fig7_short_edge_candidates(
    layout: ChenLayout,
    *,
    ratio: float = CHEN_SHORT_EDGE_RATIO,
) -> tuple[ChenShortEdgeCandidate, ...]:
    """Return Chen Section 4.1 Figure 7 short shared-edge candidates."""
    avg_side_lengths = {
        parcel_id: average_approximate_side_length(points)
        for parcel_id, points in layout.parcel_graph.parcel_approx_points.items()
    }
    candidates: list[ChenShortEdgeCandidate] = []
    for parcel_ids, shared_edges in sorted(layout.parcel_graph.shared_edges.items()):
        first_id, second_id = parcel_ids
        first_avg = avg_side_lengths.get(first_id, 0.0)
        second_avg = avg_side_lengths.get(second_id, 0.0)
        threshold = float(ratio) * min(first_avg, second_avg)
        if threshold <= 1e-12:
            continue
        for edge in sorted(shared_edges):
            path = corner_edge_path(layout.corner_graph, edge)
            length = vertex_path_length(layout.mesh, path)
            if length >= threshold:
                continue
            path_points = tuple(layout.mesh.vertices[node].point for node in path)
            candidates.append(
                ChenShortEdgeCandidate(
                    edge=edge,
                    parcel_ids=parcel_ids,
                    path=path,
                    path_points=path_points,
                    path_length=float(length),
                    threshold=float(threshold),
                    avg_side_lengths=(float(first_avg), float(second_avg)),
                    midpoint=vertex_path_midpoint(layout.mesh, path),
                )
            )
    return tuple(candidates)


def chen_fig7_short_edge_diagnostics(
    layout: ChenLayout,
    *,
    sample_limit: int = 16,
) -> dict[str, Any]:
    candidates = chen_fig7_short_edge_candidates(layout)
    lengths = [candidate.path_length for candidate in candidates]
    thresholds = [candidate.threshold for candidate in candidates]
    ratios = [
        candidate.path_length / candidate.threshold
        for candidate in candidates
        if candidate.threshold > 1e-12
    ]

    interior_t_nodes = _interior_t_junction_nodes(layout)
    candidate_path_nodes = {node for candidate in candidates for node in candidate.path}
    attached_t_nodes = interior_t_nodes & candidate_path_nodes
    unexplained_t_nodes = interior_t_nodes - attached_t_nodes
    straight_t_nodes, kinked_t_nodes = _classify_unexplained_t_junction_nodes(
        layout, unexplained_t_nodes
    )

    return {
        "chen_fig7_short_edge_detection_stage": (
            "chen_section_4_1_shared_edge_threshold_v0"
        ),
        "chen_fig7_short_edge_cleanup_stage": "diagnostic_only_v0",
        "chen_fig7_short_edge_cleanup_applied": False,
        "chen_fig7_short_edge_cleanup_scope": (
            "classifies_shared_corner_graph_edges_below_0_2_avg_approx_side_"
            "length_without_vertex_merge"
        ),
        "chen_fig7_short_edge_cleanup_blocking_reason": (
            "midpoint_vertex_merge_and_partition_line_interpolation_not_implemented"
        ),
        "chen_fig7_short_shared_edge_candidate_count": int(len(candidates)),
        "chen_fig7_short_shared_edge_length_min": _safe_min(lengths),
        "chen_fig7_short_shared_edge_length_mean": _safe_mean(lengths),
        "chen_fig7_short_shared_edge_length_max": _safe_max(lengths),
        "chen_fig7_short_shared_edge_threshold_min": _safe_min(thresholds),
        "chen_fig7_short_shared_edge_threshold_mean": _safe_mean(thresholds),
        "chen_fig7_short_shared_edge_threshold_max": _safe_max(thresholds),
        "chen_fig7_short_shared_edge_length_threshold_ratio_max": _safe_max(ratios),
        "chen_fig7_short_shared_edge_samples": tuple(
            _short_edge_candidate_sample(candidate)
            for candidate in candidates[:sample_limit]
        ),
        "chen_fig7_raw_interior_t_junction_count": int(len(interior_t_nodes)),
        "chen_fig7_short_edge_attached_interior_t_junction_count": int(
            len(attached_t_nodes)
        ),
        "chen_fig7_unexplained_interior_t_junction_count": int(
            len(unexplained_t_nodes)
        ),
        "chen_fig7_unexplained_t_junction_classification_stage": (
            "chen_section_4_parcel_corner_collinearity_135deg_v0"
        ),
        "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count": (
            int(len(straight_t_nodes))
        ),
        "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count": int(
            len(kinked_t_nodes)
        ),
        "chen_fig7_unexplained_t_junction_split_provenance_stage": (
            "todo_requires_generation_split_line_metadata_v0"
        ),
        "chen_fig7_unexplained_t_junction_split_endpoint_count": None,
        "chen_fig7_unexplained_t_junction_lies_on_split_line_count": None,
        "chen_fig7_short_edge_attached_interior_t_junction_points_sample": (
            _junction_point_sample(layout, attached_t_nodes, sample_limit)
        ),
        "chen_fig7_unexplained_interior_t_junction_points_sample": (
            _junction_point_sample(layout, unexplained_t_nodes, sample_limit)
        ),
        (
            "chen_fig7_unexplained_straight_through_side_insertion_"
            "t_junction_points_sample"
        ): (_junction_point_sample(layout, straight_t_nodes, sample_limit)),
        ("chen_fig7_unexplained_kinked_split_topology_debt_t_junction_points_sample"): (
            _junction_point_sample(layout, kinked_t_nodes, sample_limit)
        ),
    }


def apply_chen_fig7_short_edge_cleanup(
    layout: ChenLayout,
    *,
    boundary: Polygon | None = None,
    ratio: float = CHEN_SHORT_EDGE_RATIO,
    max_passes: int = 32,
) -> ChenFig7CleanupResult:
    """Merge Chen Fig. 7 short shared edges and rebuild parcel topology.

    Chen Section 4.1 removes a short edge by merging its two vertices to the
    edge midpoint and letting the adjacent partition lines interpolate through
    that merged point. In this mesh representation the interpolation is encoded
    by replacing every occurrence of the short-edge path vertices in incident
    parcel rings with the midpoint, then rebuilding the mesh/corner/parcel
    graphs. This cleanup is meant to run before street generation; streets are
    therefore intentionally regenerated by callers rather than remapped here.
    """
    max_passes = max(0, int(max_passes))
    pre_diagnostics = chen_fig7_short_edge_diagnostics(layout)
    current = layout
    applied_count = 0
    skipped_boundary_count = 0
    failed_reason_counts: dict[str, int] = {
        "failed_overlap": 0,
        "failed_invalid_polygon": 0,
        "failed_sliver_or_corner_loss": 0,
        "failed_boundary_coverage": 0,
        "failed_conforming_graph": 0,
    }
    failed_detail_counts: dict[str, int] = {
        "failed_degenerate_ring_after_merge": 0,
        "failed_fig7_motif_ineligible_non_candidate_parcel_ring_after_merge": 0,
        "failed_non_simple_ring_after_merge": 0,
        "failed_candidate_pair_still_adjacent": 0,
        "failed_candidate_pair_still_adjacent_due_other_shared_edges": 0,
        "failed_kinked_t_junction_regression": 0,
        "failed_nonlocal_neighbor_delta": 0,
    }
    merge_point_mode_counts: dict[str, int] = {}
    operation_scope_counts: dict[str, int] = {}
    projection_distances: list[float] = []
    retained_full_mesh_ring_attempt_count = 0
    retained_full_mesh_ring_parcel_count = 0
    retained_full_mesh_ring_parcel_ids_sample: list[int] = []
    failed_candidate_signatures_by_detail: dict[str, set[tuple[Any, ...]]] = {}
    samples: list[dict[str, Any]] = []
    failed_samples: list[dict[str, Any]] = []
    failed_samples_by_detail: dict[str, list[dict[str, Any]]] = {}

    for _pass_index in range(max_passes):
        candidates = sorted(
            chen_fig7_short_edge_candidates(current, ratio=ratio),
            key=lambda candidate: (
                candidate.path_length / max(candidate.threshold, 1e-12),
                candidate.path_length,
                candidate.edge,
            ),
        )
        if not candidates:
            break

        applied_this_pass = False
        for candidate in candidates:
            attempt = _layout_after_fig7_candidate_cleanup(
                current,
                candidate,
                boundary=boundary,
            )
            if attempt.layout is None:
                if attempt.failure_reason == "skipped_boundary":
                    skipped_boundary_count += 1
                    continue
                reason = attempt.failure_reason or "failed_invalid_polygon"
                failed_reason_counts[reason] = failed_reason_counts.get(reason, 0) + 1
                if attempt.failure_detail is not None:
                    failed_detail_counts[attempt.failure_detail] = (
                        failed_detail_counts.get(attempt.failure_detail, 0) + 1
                    )
                failure_key = attempt.failure_detail or reason
                failed_candidate_signatures_by_detail.setdefault(
                    failure_key,
                    set(),
                ).add(_short_edge_candidate_failure_signature(candidate))
                if len(failed_samples) < 16:
                    sample = _short_edge_candidate_sample(candidate)
                    sample["failure_reason"] = reason
                    sample["failure_detail"] = attempt.failure_detail
                    sample["failure_candidate_signature"] = (
                        _short_edge_candidate_failure_signature_sample(candidate)
                    )
                    if attempt.failure_parcel_id is not None:
                        sample["failure_parcel_id"] = int(attempt.failure_parcel_id)
                    if attempt.failure_validity_reason is not None:
                        sample["failure_validity_reason"] = (
                            attempt.failure_validity_reason
                        )
                    sample["merge_point_mode"] = attempt.merge_point_mode
                    sample["boundary_projection_distance"] = float(
                        attempt.projection_distance
                    )
                    failed_samples.append(sample)
                detail_samples = failed_samples_by_detail.setdefault(failure_key, [])
                if len(detail_samples) < 8:
                    sample = _short_edge_candidate_sample(candidate)
                    sample["failure_reason"] = reason
                    sample["failure_detail"] = attempt.failure_detail
                    sample["failure_candidate_signature"] = (
                        _short_edge_candidate_failure_signature_sample(candidate)
                    )
                    if attempt.failure_parcel_id is not None:
                        sample["failure_parcel_id"] = int(attempt.failure_parcel_id)
                    if attempt.failure_validity_reason is not None:
                        sample["failure_validity_reason"] = (
                            attempt.failure_validity_reason
                        )
                    sample["merge_point_mode"] = attempt.merge_point_mode
                    sample["boundary_projection_distance"] = float(
                        attempt.projection_distance
                    )
                    detail_samples.append(sample)
                continue
            current = attempt.layout
            applied_count += 1
            merge_point_mode = attempt.merge_point_mode or "unknown"
            merge_point_mode_counts[merge_point_mode] = (
                merge_point_mode_counts.get(merge_point_mode, 0) + 1
            )
            operation_scope = attempt.operation_scope or "unknown"
            operation_scope_counts[operation_scope] = (
                operation_scope_counts.get(operation_scope, 0) + 1
            )
            if merge_point_mode == "boundary_projected":
                projection_distances.append(float(attempt.projection_distance))
            if attempt.retained_full_mesh_ring_parcel_ids:
                retained_full_mesh_ring_attempt_count += 1
                retained_full_mesh_ring_parcel_count += len(
                    attempt.retained_full_mesh_ring_parcel_ids
                )
                if len(retained_full_mesh_ring_parcel_ids_sample) < 16:
                    remaining = 16 - len(retained_full_mesh_ring_parcel_ids_sample)
                    retained_full_mesh_ring_parcel_ids_sample.extend(
                        int(parcel_id)
                        for parcel_id in attempt.retained_full_mesh_ring_parcel_ids[
                            :remaining
                        ]
                    )
            if len(samples) < 16:
                sample = _short_edge_candidate_sample(candidate)
                sample["merge_point_mode"] = merge_point_mode
                sample["boundary_projection_distance"] = float(
                    attempt.projection_distance
                )
                sample["retained_full_mesh_ring_parcel_ids"] = tuple(
                    int(parcel_id)
                    for parcel_id in attempt.retained_full_mesh_ring_parcel_ids
                )
                sample["operation_scope"] = operation_scope
                samples.append(sample)
            applied_this_pass = True
            break

        if not applied_this_pass:
            break

    post_diagnostics = chen_fig7_short_edge_diagnostics(current)
    blocking_reason = None
    post_candidate_count = int(
        post_diagnostics["chen_fig7_short_shared_edge_candidate_count"]
    )
    failed_count = sum(failed_reason_counts.values())
    failed_reasons = tuple(
        reason for reason, count in sorted(failed_reason_counts.items()) if count > 0
    )
    failed_details = tuple(
        detail for detail, count in sorted(failed_detail_counts.items()) if count > 0
    )
    failed_unique_candidate_counts_by_detail = {
        detail: len(signatures)
        for detail, signatures in sorted(failed_candidate_signatures_by_detail.items())
    }
    failed_unique_candidate_count = sum(
        failed_unique_candidate_counts_by_detail.values()
    )
    if failed_reasons:
        blocking_reason = "short_edge_cleanup_candidates_rejected_by_validation"
    elif post_candidate_count > 0 and skipped_boundary_count > 0:
        blocking_reason = "boundary_touching_short_edge_candidates_remain_after_cleanup"
    elif post_candidate_count > 0 and applied_count >= max_passes:
        blocking_reason = "short_edge_candidates_remain_after_cleanup_pass_budget"
    elif post_candidate_count > 0:
        blocking_reason = "short_edge_candidates_remain_after_cleanup"
    elif applied_count == 0:
        blocking_reason = "no_applicable_non_boundary_short_shared_edges_found"

    boundary_projected_merge_count = int(
        merge_point_mode_counts.get("boundary_projected", 0)
    )
    midpoint_merge_count = int(merge_point_mode_counts.get("midpoint", 0))
    cleanup_scope = (
        "iterative_short_shared_edge_path_vertex_midpoint_merge_"
        "parcel_mesh_corner_graph_rebuild_before_street_generation"
        if applied_count > 0
        else (
            "classifies_shared_corner_graph_edges_below_0_2_avg_approx_side_"
            "length_without_vertex_merge"
        )
    )
    if boundary_projected_merge_count > 0:
        cleanup_scope = (
            f"{cleanup_scope}_with_boundary_projected_merge_point_"
            "approximation_not_exact_midpoint_merge"
        )
    if retained_full_mesh_ring_attempt_count > 0:
        cleanup_scope = (
            f"{cleanup_scope}_with_full_mesh_ring_retention_representation_guard_"
            "not_exact_fig7_interpolation"
        )
    if operation_scope_counts.get("candidate_pair_only", 0) > 0:
        cleanup_scope = (
            f"{cleanup_scope}_with_graph_local_candidate_pair_contraction_fallback"
        )
    labeled_approximation_reasons: list[str] = []
    if boundary_projected_merge_count > 0:
        labeled_approximation_reasons.append(
            "boundary_projected_merge_point_not_exact_midpoint"
        )
    if retained_full_mesh_ring_attempt_count > 0:
        labeled_approximation_reasons.append(
            "full_mesh_ring_retention_after_corner_simplification"
        )

    metrics = {
        "chen_fig7_short_edge_cleanup_stage": (
            "chen_section_4_1_midpoint_merge_interpolation_segment_v0"
            if applied_count > 0
            else "diagnostic_only_v0"
        ),
        "chen_fig7_short_edge_cleanup_applied": bool(applied_count > 0),
        "chen_fig7_short_edge_cleanup_scope": cleanup_scope,
        "chen_fig7_short_edge_cleanup_blocking_reason": blocking_reason,
        "chen_fig7_short_edge_cleanup_has_labeled_approximations": bool(
            labeled_approximation_reasons
        ),
        "chen_fig7_short_edge_cleanup_labeled_approximation_reasons": tuple(
            labeled_approximation_reasons
        ),
        "chen_fig7_short_edge_cleanup_applied_count": int(applied_count),
        "chen_fig7_short_edge_cleanup_midpoint_merge_count": midpoint_merge_count,
        "chen_fig7_short_edge_cleanup_boundary_projected_merge_count": (
            boundary_projected_merge_count
        ),
        "chen_fig7_short_edge_cleanup_merge_point_modes": tuple(
            sorted(merge_point_mode_counts)
        ),
        "chen_fig7_short_edge_cleanup_operation_scopes": tuple(
            sorted(operation_scope_counts)
        ),
        "chen_fig7_short_edge_cleanup_graph_local_candidate_pair_count": int(
            operation_scope_counts.get("candidate_pair_only", 0)
        ),
        "chen_fig7_short_edge_cleanup_boundary_projection_distance_min": _safe_min(
            projection_distances
        ),
        "chen_fig7_short_edge_cleanup_boundary_projection_distance_mean": _safe_mean(
            projection_distances
        ),
        "chen_fig7_short_edge_cleanup_boundary_projection_distance_max": _safe_max(
            projection_distances
        ),
        "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_stage": (
            "representation_guard_after_chen_135deg_corner_simplification_v0"
        ),
        "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_applied_count": int(
            retained_full_mesh_ring_attempt_count
        ),
        "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_count": int(
            retained_full_mesh_ring_parcel_count
        ),
        "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_ids_sample": (
            tuple(retained_full_mesh_ring_parcel_ids_sample)
        ),
        "chen_fig7_short_edge_cleanup_failed_count": int(failed_count),
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_stage": (
            "groups_failed_attempts_by_parcel_pair_midpoint_and_path_geometry_v0"
        ),
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_count": int(
            failed_unique_candidate_count
        ),
        "chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count": int(
            failed_count - failed_unique_candidate_count
        ),
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail": (
            failed_unique_candidate_counts_by_detail
        ),
        "chen_fig7_short_edge_cleanup_failed_reasons": failed_reasons,
        "chen_fig7_short_edge_cleanup_failed_details": failed_details,
        "chen_fig7_short_edge_cleanup_failed_overlap_count": int(
            failed_reason_counts["failed_overlap"]
        ),
        "chen_fig7_short_edge_cleanup_failed_invalid_polygon_count": int(
            failed_reason_counts["failed_invalid_polygon"]
        ),
        "chen_fig7_short_edge_cleanup_failed_sliver_or_corner_loss_count": int(
            failed_reason_counts["failed_sliver_or_corner_loss"]
        ),
        "chen_fig7_short_edge_cleanup_failed_boundary_coverage_count": int(
            failed_reason_counts["failed_boundary_coverage"]
        ),
        "chen_fig7_short_edge_cleanup_failed_conforming_graph_count": int(
            failed_reason_counts["failed_conforming_graph"]
        ),
        "chen_fig7_short_edge_cleanup_failed_degenerate_ring_after_merge_count": int(
            failed_detail_counts["failed_degenerate_ring_after_merge"]
        ),
        (
            "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
            "non_candidate_parcel_ring_after_merge_count"
        ): int(
            failed_detail_counts[
                "failed_fig7_motif_ineligible_non_candidate_parcel_ring_after_merge"
            ]
        ),
        "chen_fig7_short_edge_cleanup_failed_non_simple_ring_after_merge_count": int(
            failed_detail_counts["failed_non_simple_ring_after_merge"]
        ),
        "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_count": int(
            failed_detail_counts["failed_candidate_pair_still_adjacent"]
            + failed_detail_counts[
                "failed_candidate_pair_still_adjacent_due_other_shared_edges"
            ]
        ),
        (
            "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_"
            "due_other_shared_edges_count"
        ): int(
            failed_detail_counts[
                "failed_candidate_pair_still_adjacent_due_other_shared_edges"
            ]
        ),
        "chen_fig7_short_edge_cleanup_failed_nonlocal_neighbor_delta_count": int(
            failed_detail_counts["failed_nonlocal_neighbor_delta"]
        ),
        "chen_fig7_short_edge_cleanup_failed_kinked_t_junction_regression_count": int(
            failed_detail_counts["failed_kinked_t_junction_regression"]
        ),
        "chen_fig7_short_edge_cleanup_skipped_boundary_count": int(
            skipped_boundary_count
        ),
        "chen_fig7_short_edge_cleanup_pre_candidate_count": int(
            pre_diagnostics["chen_fig7_short_shared_edge_candidate_count"]
        ),
        "chen_fig7_short_edge_cleanup_post_candidate_count": int(
            post_diagnostics["chen_fig7_short_shared_edge_candidate_count"]
        ),
        "chen_fig7_short_edge_cleanup_pre_attached_t_junction_count": int(
            pre_diagnostics["chen_fig7_short_edge_attached_interior_t_junction_count"]
        ),
        "chen_fig7_short_edge_cleanup_post_attached_t_junction_count": int(
            post_diagnostics["chen_fig7_short_edge_attached_interior_t_junction_count"]
        ),
        "chen_fig7_short_edge_cleanup_pre_unexplained_t_junction_count": int(
            pre_diagnostics["chen_fig7_unexplained_interior_t_junction_count"]
        ),
        "chen_fig7_short_edge_cleanup_post_unexplained_t_junction_count": int(
            post_diagnostics["chen_fig7_unexplained_interior_t_junction_count"]
        ),
        "chen_fig7_short_edge_cleanup_samples": tuple(samples),
        "chen_fig7_short_edge_cleanup_failed_samples": tuple(failed_samples),
        "chen_fig7_short_edge_cleanup_failed_samples_by_detail": {
            detail: tuple(detail_samples)
            for detail, detail_samples in sorted(failed_samples_by_detail.items())
        },
    }
    return ChenFig7CleanupResult(layout=current, metrics=metrics)


def _layout_after_fig7_candidate_cleanup(
    layout: ChenLayout,
    candidate: ChenShortEdgeCandidate,
    *,
    boundary: Polygon | None,
) -> _Fig7CleanupAttempt:
    (
        merge_point,
        merge_failure,
        merge_point_mode,
        projection_distance,
    ) = _fig7_candidate_merge_point(
        layout,
        candidate,
        boundary=boundary,
    )
    if merge_point is None:
        return _Fig7CleanupAttempt(
            None,
            merge_failure or "failed_invalid_polygon",
            merge_point_mode,
            projection_distance,
        )

    attempt = _layout_after_fig7_candidate_cleanup_with_replaced_parcels(
        layout,
        candidate,
        boundary=boundary,
        merge_point=merge_point,
        merge_point_mode=merge_point_mode,
        projection_distance=projection_distance,
        replaced_parcel_ids=None,
        operation_scope="all_incident_rings",
    )
    if (
        attempt.layout is not None
        or attempt.failure_detail
        != "failed_fig7_motif_ineligible_non_candidate_parcel_ring_after_merge"
    ):
        return attempt

    return _layout_after_fig7_candidate_cleanup_with_replaced_parcels(
        layout,
        candidate,
        boundary=boundary,
        merge_point=merge_point,
        merge_point_mode=merge_point_mode,
        projection_distance=projection_distance,
        replaced_parcel_ids=set(candidate.parcel_ids),
        operation_scope="candidate_pair_only",
    )


def _layout_after_fig7_candidate_cleanup_with_replaced_parcels(
    layout: ChenLayout,
    candidate: ChenShortEdgeCandidate,
    *,
    boundary: Polygon | None,
    merge_point: Point,
    merge_point_mode: str | None,
    projection_distance: float,
    replaced_parcel_ids: set[int] | None,
    operation_scope: str,
) -> _Fig7CleanupAttempt:
    replaced_nodes = set(candidate.path)
    polygons: list[tuple[int, Polygon]] = []
    for parcel_id, parcel in sorted(layout.mesh.parcels.items()):
        if replaced_parcel_ids is None or parcel_id in replaced_parcel_ids:
            coords = _fig7_replaced_ring_points(
                parcel.ring,
                layout,
                replaced_nodes,
                merge_point,
            )
        else:
            coords = [
                layout.mesh.vertices[vertex_id].point for vertex_id in parcel.ring
            ]
        if len(coords) < 3:
            return _Fig7CleanupAttempt(
                None,
                "failed_sliver_or_corner_loss",
                merge_point_mode,
                projection_distance,
                failure_detail="failed_degenerate_ring_after_merge",
                failure_parcel_id=parcel_id,
            )
        (
            poly,
            failure_reason,
            failure_detail,
            failure_validity_reason,
        ) = _strict_polygon_from_points(coords)
        if poly is None:
            failure_detail = _fig7_failure_detail_for_replaced_ring(
                layout,
                candidate,
                parcel_id=parcel_id,
                failure_detail=failure_detail,
            )
            return _Fig7CleanupAttempt(
                None,
                failure_reason,
                merge_point_mode,
                projection_distance,
                failure_detail=failure_detail,
                failure_parcel_id=parcel_id,
                failure_validity_reason=failure_validity_reason,
            )
        polygons.append((parcel_id, poly))

    try:
        mesh = parcel_mesh_from_polygons(polygons, boundary=boundary)
        cleaned = build_chen_layout(mesh, set())
    except ValueError as exc:
        failure_reason, failure_detail = _fig7_failure_from_exception(exc)
        return _Fig7CleanupAttempt(None, failure_reason, failure_detail=failure_detail)

    failure_reason, failure_detail = _validate_fig7_candidate_layout(
        cleaned,
        boundary=boundary,
        expected_parcel_count=len(layout.mesh.parcels),
        original_layout=layout,
        candidate=candidate,
    )
    if failure_reason is not None:
        return _Fig7CleanupAttempt(
            None,
            failure_reason,
            merge_point_mode,
            projection_distance,
            failure_detail,
        )
    retained_full_mesh_ring_parcel_ids = _parcel_corner_ring_retention_parcel_ids(
        cleaned.mesh
    )
    return _Fig7CleanupAttempt(
        cleaned,
        merge_point_mode=merge_point_mode,
        projection_distance=projection_distance,
        retained_full_mesh_ring_parcel_ids=retained_full_mesh_ring_parcel_ids,
        operation_scope=operation_scope,
    )


def _fig7_candidate_merge_point(
    layout: ChenLayout,
    candidate: ChenShortEdgeCandidate,
    *,
    boundary: Polygon | None,
) -> tuple[Point | None, str | None, str | None, float]:
    if not any(layout.mesh.vertices[node].on_boundary for node in candidate.path):
        return candidate.midpoint, None, "midpoint", 0.0
    if boundary is None:
        return None, "skipped_boundary", "boundary_projected", 0.0

    boundary_line = LineString(boundary.exterior.coords)
    midpoint = ShapelyPoint(candidate.midpoint)
    projected = boundary_line.interpolate(boundary_line.project(midpoint))
    merge_point = (float(projected.x), float(projected.y))

    projection_distance = float(midpoint.distance(projected))
    tolerance = max(math.sqrt(max(float(boundary.area), 1e-12)) * 1e-7, 1e-7)
    if projection_distance > candidate.path_length + tolerance:
        return None, "skipped_boundary", "boundary_projected", projection_distance
    return merge_point, None, "boundary_projected", projection_distance


def _fig7_replaced_ring_points(
    ring: tuple[int, ...],
    layout: ChenLayout,
    replaced_nodes: set[int],
    midpoint: Point,
) -> list[Point]:
    coords: list[Point] = []
    for node in ring:
        point = midpoint if node in replaced_nodes else layout.mesh.vertices[node].point
        if coords and _point_distance(coords[-1], point) <= 1e-9:
            continue
        coords.append((float(point[0]), float(point[1])))
    if len(coords) >= 2 and _point_distance(coords[0], coords[-1]) <= 1e-9:
        coords.pop()
    return coords


def _fig7_failure_detail_for_replaced_ring(
    layout: ChenLayout,
    candidate: ChenShortEdgeCandidate,
    *,
    parcel_id: int,
    failure_detail: str | None,
) -> str | None:
    if failure_detail != "failed_non_simple_ring_after_merge":
        return failure_detail
    if parcel_id in candidate.parcel_ids:
        return failure_detail

    parcel = layout.mesh.parcels.get(parcel_id)
    if parcel is None or set(candidate.path).isdisjoint(parcel.ring):
        return failure_detail
    return "failed_fig7_motif_ineligible_non_candidate_parcel_ring_after_merge"


def _strict_polygon_from_points(
    points: list[Point],
) -> tuple[Polygon | None, str, str | None, str | None]:
    unique = {node_key(point) for point in points}
    if len(unique) < 3:
        return (
            None,
            "failed_sliver_or_corner_loss",
            "failed_degenerate_ring_after_merge",
            "fewer_than_three_unique_points_after_merge",
        )
    try:
        poly = Polygon(points)
    except ValueError:
        return (
            None,
            "failed_invalid_polygon",
            "failed_non_simple_ring_after_merge",
            "polygon_constructor_value_error",
        )
    if not poly.is_valid:
        return (
            None,
            "failed_invalid_polygon",
            "failed_non_simple_ring_after_merge",
            explain_validity(poly),
        )
    if poly.area <= 1e-12:
        return (
            None,
            "failed_sliver_or_corner_loss",
            "failed_degenerate_ring_after_merge",
            "zero_area_after_merge",
        )
    return poly, "", None, None


def _fig7_failure_from_exception(exc: ValueError) -> tuple[str, str | None]:
    message = str(exc)
    if "fewer than three corner-graph nodes" in message:
        return "failed_sliver_or_corner_loss", "failed_degenerate_ring_after_merge"
    if "non-conforming parcel corner edge path" in message:
        return "failed_conforming_graph", None
    if "invalid parcel polygon" in message:
        return "failed_invalid_polygon", "failed_non_simple_ring_after_merge"
    return "failed_conforming_graph", None


def _validate_fig7_candidate_layout(
    layout: ChenLayout,
    *,
    boundary: Polygon | None,
    expected_parcel_count: int,
    original_layout: ChenLayout,
    candidate: ChenShortEdgeCandidate,
) -> tuple[str | None, str | None]:
    if len(layout.mesh.parcels) != expected_parcel_count:
        return "failed_sliver_or_corner_loss", "failed_degenerate_ring_after_merge"

    if any(
        (not parcel.geom.is_valid) or parcel.geom.area <= 1e-12
        for parcel in layout.mesh.parcels.values()
    ):
        return "failed_invalid_polygon", "failed_non_simple_ring_after_merge"

    if parcel_overlap_count(layout.mesh) > 0:
        return "failed_overlap", None

    if boundary is not None:
        parcel_union = unary_union(
            [parcel.geom for parcel in layout.mesh.parcels.values()]
        )
        boundary_area = max(float(boundary.area), 1e-12)
        gap_rate = float(boundary.difference(parcel_union).area) / boundary_area
        spillover_rate = float(parcel_union.difference(boundary).area) / boundary_area
        if gap_rate > 1e-6 or spillover_rate > 1e-6:
            return "failed_boundary_coverage", None

    neighbor_delta_detail = _validate_fig7_local_neighbor_delta(
        original_layout, layout, candidate
    )
    if neighbor_delta_detail is not None:
        return "failed_conforming_graph", neighbor_delta_detail

    if _fig7_kinked_t_junction_count(layout) > _fig7_kinked_t_junction_count(
        original_layout
    ):
        return "failed_conforming_graph", "failed_kinked_t_junction_regression"

    return None, None


def _fig7_kinked_t_junction_count(layout: ChenLayout) -> int:
    diagnostics = chen_fig7_short_edge_diagnostics(layout)
    return int(
        diagnostics["chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count"]
    )


def _validate_fig7_local_neighbor_delta(
    original_layout: ChenLayout,
    cleaned_layout: ChenLayout,
    candidate: ChenShortEdgeCandidate,
) -> str | None:
    """Reject Fig. 7 cleanups that mutate parcel graph topology outside the edit.

    Chen Fig. 7 updates the parcel graph around the short edge: the two parcels
    sharing the removed short edge cease to be neighbors, while incident parcels
    can acquire the short interpolation segment. A cleanup candidate should not
    silently change adjacencies between parcels that do not touch the removed
    short-edge path.
    """
    impacted_parcels = {
        int(candidate.parcel_ids[0]),
        int(candidate.parcel_ids[1]),
    }
    replaced_nodes = set(candidate.path)
    for parcel_id, parcel in original_layout.mesh.parcels.items():
        if replaced_nodes.intersection(parcel.ring):
            impacted_parcels.add(parcel_id)

    before_pairs = _parcel_neighbor_pairs(original_layout.parcel_graph)
    after_pairs = _parcel_neighbor_pairs(cleaned_layout.parcel_graph)
    candidate_pair = _parcel_pair_key(candidate.parcel_ids)
    if candidate_pair in after_pairs:
        other_original_shared_edges = _fig7_candidate_pair_other_shared_edges(
            original_layout, candidate
        )
        if other_original_shared_edges and _fig7_other_shared_edges_are_contiguous(
            original_layout,
            candidate,
            other_original_shared_edges,
        ):
            return None
        if other_original_shared_edges:
            return "failed_candidate_pair_still_adjacent_due_other_shared_edges"
        return "failed_candidate_pair_still_adjacent"

    changed_pairs = before_pairs.symmetric_difference(after_pairs)
    for left, right in changed_pairs:
        if left not in impacted_parcels and right not in impacted_parcels:
            return "failed_nonlocal_neighbor_delta"
    return None


def _parcel_pair_key(parcel_ids: tuple[int, int]) -> tuple[int, int]:
    return min(parcel_ids[0], parcel_ids[1]), max(parcel_ids[0], parcel_ids[1])


def _fig7_candidate_pair_other_shared_edges(
    original_layout: ChenLayout,
    candidate: ChenShortEdgeCandidate,
) -> set[EdgeKey]:
    original_shared_edges = original_layout.parcel_graph.shared_edges.get(
        _parcel_pair_key(candidate.parcel_ids),
        set(),
    )
    return set(original_shared_edges) - {normalized_edge(*candidate.edge)}


def _fig7_other_shared_edges_are_contiguous(
    original_layout: ChenLayout,
    candidate: ChenShortEdgeCandidate,
    other_shared_edges: set[EdgeKey],
) -> bool:
    """Return true when other shared edges are in the same local edge chain.

    This keeps the paper-facing rule narrow: a candidate pair may remain
    adjacent only when the remaining adjacency is part of the same contiguous
    fragmented shared boundary as the removed short edge. Disconnected
    multi-edge adjacency remains explicit cleanup debt.
    """
    shared_edges = set(other_shared_edges)
    candidate_edge = normalized_edge(*candidate.edge)
    adjacency: dict[int, set[int]] = {}
    for edge in shared_edges | {candidate_edge}:
        path = original_layout.corner_graph.edge_paths.get(edge, edge)
        for start, end in zip(path, path[1:], strict=False):
            adjacency.setdefault(start, set()).add(end)
            adjacency.setdefault(end, set()).add(start)

    candidate_nodes = set(
        original_layout.corner_graph.edge_paths.get(
            candidate_edge,
            candidate_edge,
        )
    )
    stack = list(candidate_nodes)
    seen: set[int] = set()
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(adjacency.get(node, set()) - seen)

    for edge in shared_edges:
        path = original_layout.corner_graph.edge_paths.get(edge, edge)
        if not set(path).issubset(seen):
            return False
    return True


def _parcel_neighbor_pairs(graph: ParcelGraph) -> set[tuple[int, int]]:
    return {
        (min(parcel_id, neighbor), max(parcel_id, neighbor))
        for parcel_id, neighbors in graph.neighbors.items()
        for neighbor in neighbors
        if parcel_id != neighbor
    }


def _interior_t_junction_nodes(layout: ChenLayout) -> set[int]:
    adjacency = _corner_graph_adjacency(layout)
    return {
        node
        for node, neighbors in adjacency.items()
        if len(neighbors) == 3 and not layout.mesh.vertices[node].on_boundary
    }


def _corner_graph_adjacency(layout: ChenLayout) -> dict[int, set[int]]:
    adjacency: dict[int, set[int]] = {}
    for start, end in layout.corner_graph.edges:
        adjacency.setdefault(start, set()).add(end)
        adjacency.setdefault(end, set()).add(start)
    return adjacency


def _classify_unexplained_t_junction_nodes(
    layout: ChenLayout, nodes: set[int]
) -> tuple[set[int], set[int]]:
    adjacency = _corner_graph_adjacency(layout)
    straight_through: set[int] = set()
    kinked: set[int] = set()
    for node in nodes:
        max_angle = _max_incident_angle(layout, node, adjacency.get(node, set()))
        if max_angle + 1e-12 >= CHEN_COLLINEAR_THRESHOLD_RAD:
            straight_through.add(node)
        else:
            kinked.add(node)
    return straight_through, kinked


def _max_incident_angle(layout: ChenLayout, node: int, neighbors: set[int]) -> float:
    origin = layout.mesh.vertices[node].point
    vectors: list[Point] = []
    for neighbor in sorted(neighbors):
        point = layout.mesh.vertices[neighbor].point
        vector = (float(point[0] - origin[0]), float(point[1] - origin[1]))
        if math.hypot(vector[0], vector[1]) > 1e-12:
            vectors.append(vector)
    max_angle = 0.0
    for left_index, left in enumerate(vectors):
        for right in vectors[left_index + 1 :]:
            left_length = math.hypot(left[0], left[1])
            right_length = math.hypot(right[0], right[1])
            if left_length <= 1e-12 or right_length <= 1e-12:
                continue
            dot = (left[0] * right[0] + left[1] * right[1]) / (
                left_length * right_length
            )
            max_angle = max(max_angle, math.acos(max(-1.0, min(1.0, dot))))
    return max_angle


def _short_edge_candidate_sample(candidate: ChenShortEdgeCandidate) -> dict[str, Any]:
    return {
        "edge": tuple(int(node) for node in candidate.edge),
        "parcels": tuple(int(parcel_id) for parcel_id in candidate.parcel_ids),
        "path_length": float(candidate.path_length),
        "threshold": float(candidate.threshold),
        "avg_side_lengths": tuple(float(value) for value in candidate.avg_side_lengths),
        "midpoint": _rounded_metric_point(candidate.midpoint),
        "path_points_sample": tuple(
            _rounded_metric_point(point) for point in candidate.path_points[:8]
        ),
    }


def _short_edge_candidate_failure_signature(
    candidate: ChenShortEdgeCandidate,
) -> tuple[Any, ...]:
    sample = _short_edge_candidate_failure_signature_sample(candidate)
    return (
        tuple(sample["parcel_pair"]),
        tuple(sample["midpoint_key"]),
        tuple(tuple(point_key) for point_key in sample["path_point_keys"]),
    )


def _short_edge_candidate_failure_signature_sample(
    candidate: ChenShortEdgeCandidate,
) -> dict[str, Any]:
    return {
        "parcel_pair": _parcel_pair_key(candidate.parcel_ids),
        "midpoint_key": node_key(candidate.midpoint),
        "path_point_keys": tuple(node_key(point) for point in candidate.path_points),
    }


def _junction_point_sample(
    layout: ChenLayout, nodes: set[int], sample_limit: int
) -> tuple[Point, ...]:
    return tuple(
        _rounded_metric_point(layout.mesh.vertices[node].point)
        for node in sorted(nodes)[:sample_limit]
    )


def _rounded_metric_point(point: Point) -> Point:
    return (round(float(point[0]), 9), round(float(point[1]), 9))


def _point_distance(left: Point, right: Point) -> float:
    return math.hypot(float(left[0] - right[0]), float(left[1] - right[1]))


def _safe_min(values: list[float]) -> float:
    return float(min(values)) if values else 0.0


def _safe_mean(values: list[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _safe_max(values: list[float]) -> float:
    return float(max(values)) if values else 0.0


def parcel_graph(
    mesh: ParcelMesh, corner_graph: ParcelCornerGraph | None = None
) -> ParcelGraph:
    corner_graph = corner_graph or parcel_corner_graph(mesh)
    by_edge: dict[EdgeKey, list[int]] = {}
    for parcel_id, corner_ring in corner_graph.parcel_corner_rings.items():
        for edge in ring_edges(corner_ring):
            by_edge.setdefault(edge, []).append(parcel_id)

    neighbors: dict[int, set[int]] = {parcel_id: set() for parcel_id in mesh.parcels}
    shared_edges: dict[tuple[int, int], set[EdgeKey]] = {}
    for edge, parcel_ids in by_edge.items():
        if len(parcel_ids) != 2:
            continue
        a, b = sorted(parcel_ids)
        neighbors[a].add(b)
        neighbors[b].add(a)
        shared_edges.setdefault((a, b), set()).add(edge)

    parcels = list(mesh.parcels.values())
    for i, parcel in enumerate(parcels):
        for other in parcels[i + 1 :]:
            a, b = sorted((parcel.parcel_id, other.parcel_id))
            exact_shared_length = sum(
                edge_path_length(mesh, corner_graph, edge)
                for edge in shared_edges.get((a, b), set())
            )
            boundary_shared_length = float(
                parcel.geom.boundary.intersection(other.geom.boundary).length
            )
            tolerance = max(1e-7, boundary_shared_length * 1e-6)
            if boundary_shared_length > tolerance and (
                abs(boundary_shared_length - exact_shared_length) > tolerance
            ):
                raise ValueError(
                    "non-conforming parcel boundary between "
                    f"{parcel.parcel_id} and {other.parcel_id}"
                )
    return ParcelGraph(
        neighbors,
        shared_edges,
        corner_graph.edge_paths,
        corner_graph.parcel_corner_rings,
        corner_graph.parcel_approx_points,
    )


def build_chen_layout(mesh: ParcelMesh, street_edges: set[EdgeKey]) -> ChenLayout:
    corner = parcel_corner_graph(mesh)
    normalized = {normalized_edge(a, b) for a, b in street_edges}
    network = StreetNetworkGraph(normalized)
    street_graph = decompose_street_network(mesh.vertices, network)
    return ChenLayout(mesh, corner, parcel_graph(mesh, corner), network, street_graph)


def edge_angle(vertices: dict[int, MeshVertex], edge: EdgeKey, from_node: int) -> float:
    a, b = edge
    other = b if from_node == a else a
    p = vertices[from_node].point
    q = vertices[other].point
    return math.atan2(q[1] - p[1], q[0] - p[0])


def included_angle_at(
    vertices: dict[int, MeshVertex],
    prev_node: int,
    node: int,
    next_node: int,
) -> float:
    p = vertices[node].point
    prev = vertices[prev_node].point
    nxt = vertices[next_node].point
    a0 = math.atan2(prev[1] - p[1], prev[0] - p[0])
    a1 = math.atan2(nxt[1] - p[1], nxt[0] - p[0])
    return abs(math.atan2(math.sin(a1 - a0), math.cos(a1 - a0)))


def street_adjacency(network: StreetNetworkGraph) -> dict[int, set[int]]:
    adjacency: dict[int, set[int]] = {}
    for a, b in network.edges:
        adjacency.setdefault(a, set()).add(b)
        adjacency.setdefault(b, set()).add(a)
    return adjacency


def connected_component_count(network: StreetNetworkGraph) -> int:
    adjacency = street_adjacency(network)
    seen: set[int] = set()
    components = 0
    for start in adjacency:
        if start in seen:
            continue
        components += 1
        stack = [start]
        seen.add(start)
        while stack:
            node = stack.pop()
            for nxt in adjacency.get(node, set()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
    return components


def _street_next_edge(
    vertices: dict[int, MeshVertex],
    adjacency: dict[int, set[int]],
    used_edges: set[EdgeKey],
    prev_node: int,
    node: int,
) -> int | None:
    candidates = [
        nxt
        for nxt in adjacency.get(node, set())
        if nxt != prev_node and normalized_edge(node, nxt) not in used_edges
    ]
    collinear = [
        nxt
        for nxt in candidates
        if included_angle_at(vertices, prev_node, node, nxt)
        > CHEN_COLLINEAR_THRESHOLD_RAD
    ]
    if len(collinear) != 1:
        return None
    return collinear[0]


def decompose_street_network(
    vertices: dict[int, MeshVertex],
    network: StreetNetworkGraph,
) -> StreetGraph:
    adjacency = street_adjacency(network)
    used_edges: set[EdgeKey] = set()
    streets: list[StreetPath] = []

    for start_edge in sorted(network.edges):
        if start_edge in used_edges:
            continue
        a, b = start_edge
        used_edges.add(start_edge)
        nodes = [a, b]

        while True:
            nxt = _street_next_edge(
                vertices, adjacency, used_edges, nodes[-2], nodes[-1]
            )
            if nxt is None:
                break
            used_edges.add(normalized_edge(nodes[-1], nxt))
            nodes.append(nxt)

        while True:
            nxt = _street_next_edge(vertices, adjacency, used_edges, nodes[1], nodes[0])
            if nxt is None:
                break
            used_edges.add(normalized_edge(nodes[0], nxt))
            nodes.insert(0, nxt)

        edges = tuple(
            normalized_edge(x, y) for x, y in zip(nodes, nodes[1:], strict=False)
        )
        streets.append(StreetPath(len(streets) + 1, tuple(nodes), edges))

    street_by_node: dict[int, set[int]] = {}
    for street in streets:
        for node in street.nodes:
            street_by_node.setdefault(node, set()).add(street.street_id)
    junctions = {
        node: street_ids
        for node, street_ids in street_by_node.items()
        if len(street_ids) >= 2
    }
    return StreetGraph(tuple(streets), junctions)


def parcel_access_ratio_from_edges(
    parcel: ParcelFace,
    mesh: ParcelMesh,
    street_edges: set[EdgeKey],
    corner_ring: tuple[int, ...] | None = None,
) -> float:
    side_lengths: list[float] = []
    street_length = 0.0

    if corner_ring is None:
        side_paths = tuple(
            (a, b)
            for a, b in zip(parcel.ring, parcel.ring[1:] + parcel.ring[:1], strict=True)
        )
    else:
        side_paths = tuple(
            ring_path_between(parcel.ring, start, end)
            for start, end in zip(
                corner_ring, corner_ring[1:] + corner_ring[:1], strict=True
            )
        )

    for side_path in side_paths:
        edge = normalized_edge(side_path[0], side_path[-1])
        length = vertex_path_length(mesh, side_path)
        side_lengths.append(length)
        if edge in street_edges:
            street_length += length
    avg_side = float(np.mean(side_lengths)) if side_lengths else 0.0
    return street_length / max(avg_side, 1e-12)


def parcel_has_street_edge(
    parcel: ParcelFace,
    street_edges: set[EdgeKey],
    corner_ring: tuple[int, ...] | None = None,
) -> bool:
    return any(edge in street_edges for edge in ring_edges(corner_ring or parcel.ring))


def street_network_subset_of_corner_graph(layout: ChenLayout) -> bool:
    return layout.street_network.edges <= layout.corner_graph.edges


def parcel_overlap_count(mesh: ParcelMesh) -> int:
    parcels = list(mesh.parcels.values())
    overlaps = 0
    for i, parcel in enumerate(parcels):
        for other in parcels[i + 1 :]:
            if parcel.geom.intersection(other.geom).area > 1e-9:
                overlaps += 1
    return overlaps


def evaluate_layout_invariants(
    layout: ChenLayout,
    *,
    target_boundary: Polygon | None = None,
    tau: float = CHEN_ACCESS_RATIO_TAU,
) -> ChenInvariantReport:
    component_count = connected_component_count(layout.street_network)
    subset_ok = street_network_subset_of_corner_graph(layout)
    access_ratios = {
        parcel.parcel_id: parcel_access_ratio_from_edges(
            parcel,
            layout.mesh,
            layout.street_network.edges,
            layout.corner_graph.parcel_corner_rings.get(parcel.parcel_id),
        )
        for parcel in layout.mesh.parcels.values()
    }
    below_tau = [parcel_id for parcel_id, ratio in access_ratios.items() if ratio < tau]
    unreachable = [
        parcel.parcel_id
        for parcel in layout.mesh.parcels.values()
        if not parcel_has_street_edge(
            parcel,
            layout.street_network.edges,
            layout.corner_graph.parcel_corner_rings.get(parcel.parcel_id),
        )
    ]
    overlaps = parcel_overlap_count(layout.mesh)
    valid_parcels = all(
        parcel.geom.is_valid and parcel.geom.area > 1e-12
        for parcel in layout.mesh.parcels.values()
    )
    coverage_rate = None
    gap_rate = None
    spillover_rate = None
    if target_boundary is not None:
        parcel_union = unary_union(
            [parcel.geom for parcel in layout.mesh.parcels.values()]
        )
        boundary_area = max(float(target_boundary.area), 1e-12)
        coverage_rate = (
            float(parcel_union.intersection(target_boundary).area) / boundary_area
        )
        gap_rate = float(target_boundary.difference(parcel_union).area) / boundary_area
        spillover_rate = (
            float(parcel_union.difference(target_boundary).area) / boundary_area
        )

    irregularities = [
        chen_irregularity(parcel.geom) for parcel in layout.mesh.parcels.values()
    ]
    metrics: dict[str, Any] = {
        "corner_node_count": int(len(layout.corner_graph.vertices)),
        "corner_edge_count": int(len(layout.corner_graph.edges)),
        "street_network_edge_count": int(len(layout.street_network.edges)),
        "street_graph_component_count": int(component_count),
        "street_graph_street_count": int(len(layout.street_graph.streets)),
        "street_graph_junction_count": int(len(layout.street_graph.junctions)),
        "street_network_subset_of_corner_graph": bool(subset_ok),
        "parcel_count": int(len(layout.mesh.parcels)),
        "parcel_overlap_count": int(overlaps),
        "parcel_access_ratio_min": float(min(access_ratios.values()))
        if access_ratios
        else 0.0,
        "parcel_access_ratio_below_tau_count": int(len(below_tau)),
        "unreachable_parcel_count": int(len(unreachable)),
        "parcel_irregularity_avg": float(np.mean(irregularities))
        if irregularities
        else 0.0,
        "parcel_irregularity_p95": float(np.quantile(irregularities, 0.95))
        if irregularities
        else 0.0,
    }
    if coverage_rate is not None:
        metrics["coverage_rate"] = float(coverage_rate)
    if gap_rate is not None:
        metrics["coverage_gap_rate"] = float(gap_rate)
    if spillover_rate is not None:
        metrics["coverage_spillover_rate"] = float(spillover_rate)

    paper_invariant_pass = subset_ok and component_count == 1 and not unreachable
    geometry_valid_pass = (
        valid_parcels
        and overlaps == 0
        and (coverage_rate is None or coverage_rate >= 0.995)
        and (spillover_rate is None or spillover_rate <= 0.005)
    )
    chen_formula_pass = all(
        math.isfinite(float(value))
        for key, value in metrics.items()
        if isinstance(value, int | float) and "count" not in key
    )
    diagnostic_metric_pass = all(value is not None for value in metrics.values())
    return ChenInvariantReport(
        paper_invariant_pass=paper_invariant_pass,
        geometry_valid_pass=geometry_valid_pass,
        chen_formula_pass=chen_formula_pass,
        diagnostic_metric_pass=diagnostic_metric_pass,
        metrics=metrics,
    )
