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
    # A parcel whose simplified corner ring would collapse below three vertices
    # retains its full mesh ring (see _parcel_corner_ring_with_retention). Those
    # retained vertices must be promoted to corners for *every* parcel that
    # shares them, or the shared boundary becomes non-conforming (one neighbour
    # simplifies an arc the other keeps verbatim). A single pass only propagates
    # to parcels visited later in iteration order, so iterate to a fixpoint.
    parcel_corner_rings: dict[int, tuple[int, ...]] = {}
    while True:
        promoted = False
        parcel_corner_rings = {}
        for parcel in mesh.parcels.values():
            ring, _retained_full_mesh_ring = _parcel_corner_ring_with_retention(
                parcel,
                corner_ids,
            )
            parcel_corner_rings[parcel.parcel_id] = ring
            for vertex_id in ring:
                if vertex_id not in corner_ids:
                    corner_ids.add(vertex_id)
                    promoted = True
        if not promoted:
            break
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
