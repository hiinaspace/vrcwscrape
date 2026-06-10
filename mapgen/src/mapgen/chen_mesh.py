"""Mutable ring-based parcel-mesh editor for the strict Chen 2024 pipeline.

This module owns the per-level editable mesh state that the hierarchical driver
mutates as it splits parcels and welds short edges (Chen 2024 Section 4.1,
Fig. 7). It is the single source of truth for parcel geometry between levels;
frozen snapshots for scoring/invariants are produced via :meth:`snapshot`, which
emits a :class:`chen_core.ParcelMesh` accepted by ``parcel_corner_graph`` /
``parcel_graph`` / ``build_chen_layout`` / ``evaluate_layout_invariants``.

State is kept conforming incrementally: when a split endpoint lands on a shared
edge, the inserted vertex is added to every ring that uses that edge, so
neighbour parcels keep matching vertex sequences along shared boundaries. The
short-edge weld is a true local vertex merge across all incident rings rather
than a global re-polygonize, which makes Fig. 7 bowties structurally impossible.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from shapely import LineString, Polygon
from shapely import Point as ShapelyPoint

from mapgen.chen_core import (
    CHEN_COLLINEAR_THRESHOLD_RAD,
    CHEN_SHORT_EDGE_RATIO,
    NODE_SCALE,
    EdgeKey,
    MeshVertex,
    ParcelFace,
    ParcelMesh,
    Point,
    average_approximate_side_length,
    ccw_points,
    included_angle_unsigned,
    node_key,
    normalized_edge,
    polygon_exterior_points,
    ring_edges,
    signed_area,
)

SNAP_TOLERANCE = 2.0 / NODE_SCALE
AREA_EPSILON = 1e-12


class ShortEdgeMergeRejected(Exception):
    """Raised when a Fig. 7 weld would violate the paper's local invariants.

    The editor state is left unchanged; the caller is expected to skip the
    offending candidate and continue.
    """


@dataclass(frozen=True)
class SplitResult:
    new_parcel_ids: tuple[int, int]
    split_edges: tuple[EdgeKey, ...]
    edge_replacements: list[tuple[EdgeKey, tuple[EdgeKey, EdgeKey]]]


@dataclass(frozen=True)
class MergeResult:
    merged_vertex: int
    edge_replacements: list[tuple[EdgeKey, EdgeKey]]
    removed_adjacency: tuple[int, int]


def _point_distance(a: Point, b: Point) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])


def _ring_area(points: list[Point]) -> float:
    return abs(signed_area(points))


def _ring_is_simple(ring: list[int], positions: dict[int, Point]) -> bool:
    points = [positions[v] for v in ring]
    if len(set(ring)) != len(ring):
        return False
    poly = Polygon(points)
    return bool(poly.is_valid) and poly.area > AREA_EPSILON


@dataclass
class ParcelMeshEditor:
    positions: dict[int, Point] = field(default_factory=dict)
    rings: dict[int, list[int]] = field(default_factory=dict)
    edge_parcels: dict[EdgeKey, set[int]] = field(default_factory=dict)
    boundary_vertex_ids: set[int] = field(default_factory=set)
    interpolation_edges: set[EdgeKey] = field(default_factory=set)
    next_vertex_id: int = 0
    next_parcel_id: int = 0

    @classmethod
    def from_boundary(cls, polygon: Polygon) -> ParcelMeshEditor:
        if not polygon.is_valid or polygon.area <= AREA_EPSILON:
            raise ValueError("boundary polygon must be valid with positive area")
        editor = cls()
        points = ccw_points(polygon_exterior_points(polygon))
        deduped: list[Point] = []
        for point in points:
            if deduped and _point_distance(deduped[-1], point) <= SNAP_TOLERANCE:
                continue
            deduped.append(point)
        if (
            len(deduped) >= 2
            and _point_distance(deduped[0], deduped[-1]) <= SNAP_TOLERANCE
        ):
            deduped.pop()
        if len(deduped) < 3:
            raise ValueError("boundary polygon must have at least three corners")
        ring = [editor._allocate_vertex(point, on_boundary=True) for point in deduped]
        parcel_id = editor._allocate_parcel()
        editor.rings[parcel_id] = ring
        editor._index_ring(parcel_id, ring)
        return editor

    def _allocate_vertex(self, point: Point, *, on_boundary: bool) -> int:
        vertex_id = self.next_vertex_id
        self.next_vertex_id += 1
        self.positions[vertex_id] = (float(point[0]), float(point[1]))
        if on_boundary:
            self.boundary_vertex_ids.add(vertex_id)
        return vertex_id

    def _allocate_parcel(self) -> int:
        parcel_id = self.next_parcel_id
        self.next_parcel_id += 1
        return parcel_id

    def _index_ring(self, parcel_id: int, ring: list[int]) -> None:
        for edge in ring_edges(tuple(ring)):
            self.edge_parcels.setdefault(edge, set()).add(parcel_id)

    def _deindex_ring(self, parcel_id: int, ring: list[int]) -> None:
        for edge in ring_edges(tuple(ring)):
            parcels = self.edge_parcels.get(edge)
            if parcels is None:
                continue
            parcels.discard(parcel_id)
            if not parcels:
                del self.edge_parcels[edge]

    def _find_existing_vertex(self, point: Point) -> int | None:
        key = node_key(point)
        best: int | None = None
        best_dist = SNAP_TOLERANCE
        for vertex_id, existing in self.positions.items():
            if node_key(existing) == key:
                return vertex_id
            dist = _point_distance(existing, point)
            if dist <= best_dist:
                best_dist = dist
                best = vertex_id
        return best

    # -- split ---------------------------------------------------------------

    def split_parcel(self, parcel_id: int, polyline: LineString) -> SplitResult:
        if parcel_id not in self.rings:
            raise ValueError(f"unknown parcel id: {parcel_id}")
        coords = [(float(x), float(y)) for x, y in polyline.coords]
        if len(coords) < 2:
            raise ValueError("split polyline needs at least two points")

        parcel_poly = self._parcel_polygon(parcel_id)
        start_point, end_point = coords[0], coords[-1]
        edge_replacements: list[tuple[EdgeKey, tuple[EdgeKey, EdgeKey]]] = []

        start_vid = self._resolve_endpoint(parcel_id, start_point, edge_replacements)
        end_vid = self._resolve_endpoint(parcel_id, end_point, edge_replacements)
        if start_vid == end_vid:
            raise ValueError("split polyline endpoints resolve to the same vertex")

        ring = self.rings[parcel_id]
        if start_vid not in ring or end_vid not in ring:
            raise ValueError("split endpoints are not on the parcel ring")

        interior_points = coords[1:-1]
        interior_vids = [
            self._allocate_vertex(point, on_boundary=False) for point in interior_points
        ]

        ring_a, ring_b = self._partition_ring(ring, start_vid, end_vid, interior_vids)
        for child in (ring_a, ring_b):
            points = [self.positions[v] for v in child]
            if len(child) < 3 or _ring_area(points) <= AREA_EPSILON:
                raise ValueError("split produces a degenerate child parcel")
            if not _ring_is_simple(child, self.positions):
                raise ValueError("split produces a non-simple child parcel")
            poly = Polygon(points)
            if not parcel_poly.buffer(SNAP_TOLERANCE).contains(
                poly.buffer(-SNAP_TOLERANCE)
            ):
                raise ValueError("child parcel is not contained in the parent parcel")

        ordered = sorted((ring_a, ring_b), key=self._ring_sort_key)

        self._deindex_ring(parcel_id, ring)
        del self.rings[parcel_id]
        new_ids = (self._allocate_parcel(), self._allocate_parcel())
        for new_id, child in zip(new_ids, ordered, strict=True):
            self.rings[new_id] = child
            self._index_ring(new_id, child)

        split_edges = tuple(
            normalized_edge(a, b)
            for a, b in zip(
                [start_vid, *interior_vids, end_vid],
                [*interior_vids, end_vid],
                strict=False,
            )
        )
        return SplitResult(new_ids, split_edges, edge_replacements)

    def _resolve_endpoint(
        self,
        parcel_id: int,
        point: Point,
        edge_replacements: list[tuple[EdgeKey, tuple[EdgeKey, EdgeKey]]],
    ) -> int:
        existing = self._find_existing_vertex(point)
        ring = self.rings[parcel_id]
        if existing is not None and existing in ring:
            return existing
        return self._insert_on_host_edge(parcel_id, point, edge_replacements)

    def _insert_on_host_edge(
        self,
        parcel_id: int,
        point: Point,
        edge_replacements: list[tuple[EdgeKey, tuple[EdgeKey, EdgeKey]]],
    ) -> int:
        host_edge, projected = self._host_edge_for_point(parcel_id, point)
        a, b = host_edge
        on_boundary = host_edge in self._boundary_edges()
        new_vid = self._allocate_vertex(projected, on_boundary=on_boundary)
        incident = sorted(self.edge_parcels.get(host_edge, set()))
        for incident_id in incident:
            self._insert_vertex_into_ring(incident_id, host_edge, new_vid)
        edge_replacements.append(
            (host_edge, (normalized_edge(a, new_vid), normalized_edge(new_vid, b)))
        )
        return new_vid

    def _host_edge_for_point(
        self, parcel_id: int, point: Point
    ) -> tuple[EdgeKey, Point]:
        ring = self.rings[parcel_id]
        best_edge: EdgeKey | None = None
        best_proj: Point | None = None
        best_dist = math.inf
        for a, b in zip(ring, ring[1:] + ring[:1], strict=True):
            pa, pb = self.positions[a], self.positions[b]
            proj, dist = _project_onto_segment(point, pa, pb)
            if dist < best_dist:
                best_dist = dist
                best_edge = normalized_edge(a, b)
                best_proj = proj
        if best_edge is None or best_proj is None:
            raise ValueError("could not find a host edge for split endpoint")
        if best_dist > SNAP_TOLERANCE * 10.0 + 1e-9:
            raise ValueError("split endpoint does not lie on the parcel boundary")
        return best_edge, best_proj

    def _insert_vertex_into_ring(
        self, parcel_id: int, edge: EdgeKey, new_vid: int
    ) -> None:
        ring = self.rings[parcel_id]
        self._deindex_ring(parcel_id, ring)
        rebuilt: list[int] = []
        for a, b in zip(ring, ring[1:] + ring[:1], strict=True):
            rebuilt.append(a)
            if normalized_edge(a, b) == edge:
                rebuilt.append(new_vid)
        self.rings[parcel_id] = rebuilt
        self._index_ring(parcel_id, rebuilt)

    @staticmethod
    def _partition_ring(
        ring: list[int],
        start_vid: int,
        end_vid: int,
        interior_vids: list[int],
    ) -> tuple[list[int], list[int]]:
        i = ring.index(start_vid)
        j = ring.index(end_vid)
        forward: list[int] = []
        k = i
        while True:
            forward.append(ring[k])
            if k == j:
                break
            k = (k + 1) % len(ring)
        backward: list[int] = []
        k = j
        while True:
            backward.append(ring[k])
            if k == i:
                break
            k = (k + 1) % len(ring)
        ring_a = forward + list(reversed(interior_vids))
        ring_b = backward + interior_vids
        return ring_a, ring_b

    def _ring_sort_key(self, ring: list[int]) -> tuple[int, ...]:
        rotation = min(range(len(ring)), key=lambda i: ring[i])
        return tuple(ring[rotation:] + ring[:rotation])

    # -- merge ---------------------------------------------------------------

    def merge_short_edge(self, edge: EdgeKey) -> MergeResult:
        v1, v2 = edge
        if v1 not in self.positions or v2 not in self.positions:
            raise ShortEdgeMergeRejected("edge vertex no longer present")
        incident = self.edge_parcels.get(edge)
        if not incident or len(incident) != 2:
            raise ShortEdgeMergeRejected("edge is not a shared two-parcel edge")
        ordered_pair = sorted(incident)
        pair: tuple[int, int] = (ordered_pair[0], ordered_pair[1])
        v1_boundary = v1 in self.boundary_vertex_ids
        v2_boundary = v2 in self.boundary_vertex_ids
        if v1_boundary and v2_boundary:
            raise ShortEdgeMergeRejected("both endpoints lie on the boundary")

        if v1_boundary:
            target, merged_position, merged_on_boundary = v2, self.positions[v1], True
            survivor = v1
        elif v2_boundary:
            target, merged_position, merged_on_boundary = v1, self.positions[v2], True
            survivor = v2
        else:
            p1, p2 = self.positions[v1], self.positions[v2]
            merged_position = ((p1[0] + p2[0]) / 2.0, (p1[1] + p2[1]) / 2.0)
            survivor, target, merged_on_boundary = v1, v2, False

        affected = sorted(
            pid
            for pid, ring in self.rings.items()
            if survivor in ring or target in ring
        )
        trial_rings: dict[int, list[int]] = {}
        for pid in affected:
            collapsed = _weld_ring(self.rings[pid], survivor, target)
            trial_rings[pid] = collapsed

        for pid in affected:
            new_ring = trial_rings[pid]
            if len(set(new_ring)) < 3:
                raise ShortEdgeMergeRejected(
                    f"parcel {pid} ring collapses below three vertices"
                )

        trial_positions = dict(self.positions)
        trial_positions[survivor] = merged_position
        for pid in affected:
            if not _ring_is_simple(trial_rings[pid], trial_positions):
                raise ShortEdgeMergeRejected(
                    f"parcel {pid} ring is not simple after weld"
                )

        interp = self._partition_line_ends(pair, survivor, target, trial_rings)

        for pid in affected:
            self._deindex_ring(pid, self.rings[pid])
        edge_replacements = self._collect_edge_replacements(affected, survivor, target)
        self.positions[survivor] = merged_position
        if merged_on_boundary:
            self.boundary_vertex_ids.add(survivor)
        for pid in affected:
            self.rings[pid] = trial_rings[pid]
            self._index_ring(pid, trial_rings[pid])
        self._remap_vertex_in_sets(survivor, target)
        del self.positions[target]
        self.boundary_vertex_ids.discard(target)

        self.interpolation_edges.update(interp)

        a, b = pair
        remaining_shared = [
            e for e, pids in self.edge_parcels.items() if pids == {a, b}
        ]
        assert not remaining_shared, "merged parcels still share an edge"

        return MergeResult(survivor, edge_replacements, pair)

    def _partition_line_ends(
        self,
        pair: tuple[int, int],
        survivor: int,
        target: int,
        trial_rings: dict[int, list[int]],
    ) -> set[EdgeKey]:
        interp: set[EdgeKey] = set()
        for pid in pair:
            ring = trial_rings[pid]
            if survivor not in ring:
                continue
            idx = ring.index(survivor)
            prev_v = ring[idx - 1]
            next_v = ring[(idx + 1) % len(ring)]
            for other in (prev_v, next_v):
                if other != survivor:
                    interp.add(normalized_edge(survivor, other))
        return interp

    def _collect_edge_replacements(
        self, affected: list[int], survivor: int, target: int
    ) -> list[tuple[EdgeKey, EdgeKey]]:
        replacements: dict[EdgeKey, EdgeKey] = {}
        for pid in affected:
            ring = self.rings[pid]
            for a, b in zip(ring, ring[1:] + ring[:1], strict=True):
                if target not in (a, b):
                    continue
                if a == target and b == target:
                    continue
                old = normalized_edge(a, b)
                na = survivor if a == target else a
                nb = survivor if b == target else b
                if na == nb:
                    continue
                replacements[old] = normalized_edge(na, nb)
        return sorted(replacements.items())

    def _remap_vertex_in_sets(self, survivor: int, target: int) -> None:
        remapped: set[EdgeKey] = set()
        for edge in self.interpolation_edges:
            a, b = edge
            na = survivor if a == target else a
            nb = survivor if b == target else b
            if na != nb:
                remapped.add(normalized_edge(na, nb))
        self.interpolation_edges = remapped

    # -- short-edge detection ------------------------------------------------

    def short_edge_candidates(
        self, scope_vertex_ids: set[int] | None = None
    ) -> list[EdgeKey]:
        avg_sides = {
            pid: average_approximate_side_length(self._approx_points(pid))
            for pid in self.rings
        }
        candidates: list[EdgeKey] = []
        for edge in sorted(self.edge_parcels):
            parcels = self.edge_parcels[edge]
            if len(parcels) != 2:
                continue
            if scope_vertex_ids is not None and not (set(edge) & scope_vertex_ids):
                continue
            first, second = sorted(parcels)
            threshold = CHEN_SHORT_EDGE_RATIO * min(
                avg_sides.get(first, 0.0), avg_sides.get(second, 0.0)
            )
            if threshold <= 1e-12:
                continue
            a, b = edge
            length = _point_distance(self.positions[a], self.positions[b])
            if length < threshold:
                candidates.append(edge)
        return candidates

    def _approx_points(self, parcel_id: int) -> tuple[Point, ...]:
        ring = self.rings[parcel_id]
        points = [self.positions[v] for v in ring]
        if len(points) <= 3:
            return tuple(points)
        approx: list[Point] = []
        n = len(points)
        for i, point in enumerate(points):
            prev = points[i - 1]
            nxt = points[(i + 1) % n]
            if included_angle_unsigned(prev, point, nxt) > CHEN_COLLINEAR_THRESHOLD_RAD:
                continue
            approx.append(point)
        return tuple(approx) if len(approx) >= 3 else tuple(points)

    # -- snapshot ------------------------------------------------------------

    def _parcel_polygon(self, parcel_id: int) -> Polygon:
        return Polygon([self.positions[v] for v in self.rings[parcel_id]])

    def snapshot(self, boundary: Polygon) -> ParcelMesh:
        boundary_line = LineString(boundary.exterior.coords)
        vertices: dict[int, MeshVertex] = {}
        used: set[int] = {v for ring in self.rings.values() for v in ring}
        for vertex_id in sorted(used):
            point = self.positions[vertex_id]
            on_boundary = (
                vertex_id in self.boundary_vertex_ids
                or boundary_line.distance(ShapelyPoint(point)) <= SNAP_TOLERANCE
            )
            vertices[vertex_id] = MeshVertex(vertex_id, point, on_boundary)
        parcels: dict[int, ParcelFace] = {}
        for parcel_id in sorted(self.rings):
            ring = self.rings[parcel_id]
            points = [self.positions[v] for v in ring]
            if signed_area(points) < 0.0:
                ring = [ring[0], *reversed(ring[1:])]
                points = [self.positions[v] for v in ring]
            poly = Polygon(points)
            if not poly.is_valid or poly.area <= AREA_EPSILON:
                raise ValueError(f"snapshot parcel {parcel_id} is degenerate")
            parcels[parcel_id] = ParcelFace(parcel_id, tuple(ring), poly)
        return ParcelMesh(vertices, parcels)

    def _boundary_edges(self) -> set[EdgeKey]:
        return {
            edge
            for edge in self.edge_parcels
            if edge[0] in self.boundary_vertex_ids
            and edge[1] in self.boundary_vertex_ids
            and len(self.edge_parcels[edge]) == 1
        }


def _weld_ring(ring: list[int], survivor: int, target: int) -> list[int]:
    remapped = [survivor if v == target else v for v in ring]
    collapsed: list[int] = []
    for v in remapped:
        if collapsed and collapsed[-1] == v:
            continue
        collapsed.append(v)
    while len(collapsed) >= 2 and collapsed[0] == collapsed[-1]:
        collapsed.pop()
    return collapsed


def _project_onto_segment(point: Point, a: Point, b: Point) -> tuple[Point, float]:
    ax, ay = a
    bx, by = b
    px, py = point
    dx, dy = bx - ax, by - ay
    length_sq = dx * dx + dy * dy
    if length_sq <= 1e-18:
        return a, _point_distance(point, a)
    t = ((px - ax) * dx + (py - ay) * dy) / length_sq
    t = max(0.0, min(1.0, t))
    proj = (ax + t * dx, ay + t * dy)
    return proj, _point_distance(point, proj)
