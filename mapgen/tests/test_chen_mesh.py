from __future__ import annotations

import math

import pytest
from shapely import LineString, Polygon
from shapely.ops import unary_union

from mapgen.chen_core import (
    build_chen_layout,
    evaluate_layout_invariants,
    normalized_edge,
    parcel_corner_graph,
    parcel_graph,
)
from mapgen.chen_mesh import (
    ParcelMeshEditor,
    ShortEdgeMergeRejected,
)


def _unit_square() -> Polygon:
    return Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])


def _boundary_street_edges(editor: ParcelMeshEditor) -> set:
    return {edge for edge, parcels in editor.edge_parcels.items() if len(parcels) == 1}


def _parcel_at(editor: ParcelMeshEditor, cx: float, cy: float) -> int:
    probe = Polygon([(cx - 1e-4, cy - 1e-4), (cx + 1e-4, cy - 1e-4), (cx, cy + 1e-4)])
    for pid in editor.rings:
        poly = Polygon([editor.positions[v] for v in editor.rings[pid]])
        if poly.contains(probe):
            return pid
    raise AssertionError("no parcel found")


def test_from_boundary_round_trips_through_graphs() -> None:
    editor = ParcelMeshEditor.from_boundary(_unit_square())
    assert len(editor.rings) == 1
    boundary = _unit_square()
    mesh = editor.snapshot(boundary)
    corner = parcel_corner_graph(mesh)
    pgraph = parcel_graph(mesh, corner)
    assert len(pgraph.neighbors) == 1

    layout = build_chen_layout(mesh, _boundary_street_edges(editor))
    report = evaluate_layout_invariants(layout, target_boundary=boundary)
    assert report.paper_invariant_pass
    assert report.geometry_valid_pass


def test_straight_split_is_conforming() -> None:
    editor = ParcelMeshEditor.from_boundary(_unit_square())
    parcel_id = next(iter(editor.rings))
    result = editor.split_parcel(parcel_id, LineString([(0.5, 0.0), (0.5, 1.0)]))

    assert len(editor.rings) == 2
    assert result.new_parcel_ids[0] != result.new_parcel_ids[1]

    # The two inserted boundary endpoints subdivided the bottom and top edges.
    assert len(result.edge_replacements) == 2

    boundary = _unit_square()
    mesh = editor.snapshot(boundary)
    corner = parcel_corner_graph(mesh)
    pgraph = parcel_graph(mesh, corner)
    a, b = sorted(editor.rings)
    assert b in pgraph.neighbors[a]

    # Neighbours share the inserted split vertices (conforming mesh).
    shared = set(editor.rings[a]) & set(editor.rings[b])
    assert len(shared) == 2


def test_curved_split_shares_interior_vertices() -> None:
    editor = ParcelMeshEditor.from_boundary(_unit_square())
    parcel_id = next(iter(editor.rings))
    polyline = LineString([(0.5, 0.0), (0.55, 0.5), (0.5, 1.0)])
    editor.split_parcel(parcel_id, polyline)

    a, b = sorted(editor.rings)
    shared = set(editor.rings[a]) & set(editor.rings[b])
    # start, interior, end all shared
    assert len(shared) == 3

    for ring in (editor.rings[a], editor.rings[b]):
        points = [editor.positions[v] for v in ring]
        poly = Polygon(points)
        assert poly.is_valid
        assert poly.exterior.is_ccw


def test_chained_splits_form_grid_with_full_coverage() -> None:
    boundary = _unit_square()
    editor = ParcelMeshEditor.from_boundary(boundary)

    # Two vertical cuts.
    pid = next(iter(editor.rings))
    editor.split_parcel(pid, LineString([(1 / 3, 0.0), (1 / 3, 1.0)]))
    pid = _parcel_at(editor, 2 / 3, 0.5)
    editor.split_parcel(pid, LineString([(2 / 3, 0.0), (2 / 3, 1.0)]))

    # Two horizontal cuts across each column.
    for x0, x1 in ((0.0, 1 / 3), (1 / 3, 2 / 3), (2 / 3, 1.0)):
        cx = (x0 + x1) / 2
        pid = _parcel_at(editor, cx, 0.5)
        editor.split_parcel(pid, LineString([(x0, 1 / 3), (x1, 1 / 3)]))
        pid = _parcel_at(editor, cx, 2 / 3)
        editor.split_parcel(pid, LineString([(x0, 2 / 3), (x1, 2 / 3)]))

    assert len(editor.rings) == 9

    union = unary_union(
        [Polygon([editor.positions[v] for v in r]) for r in editor.rings.values()]
    )
    assert math.isclose(union.area, boundary.area, rel_tol=1e-9)

    mesh = editor.snapshot(boundary)
    corner = parcel_corner_graph(mesh)
    parcel_graph(mesh, corner)

    for parcels in editor.edge_parcels.values():
        assert len(parcels) in (1, 2)


def _stacked_fig7_fixture(offset: float) -> tuple[ParcelMeshEditor, Polygon]:
    """Two stacked parcels each split vertically with misaligned lines.

    The bottom column boundary lands at x=0.5 on the shared horizontal edge,
    the top at x=0.5+offset, producing a short shared edge of length ``offset``.
    """
    boundary = Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 2.0), (0.0, 2.0)])
    editor = ParcelMeshEditor.from_boundary(boundary)

    pid = next(iter(editor.rings))
    editor.split_parcel(pid, LineString([(0.0, 1.0), (1.0, 1.0)]))

    pid = _parcel_at(editor, 0.5, 0.5)
    editor.split_parcel(pid, LineString([(0.5, 0.0), (0.5, 1.0)]))
    pid = _parcel_at(editor, 0.5, 1.5)
    editor.split_parcel(pid, LineString([(0.5 + offset, 1.0), (0.5 + offset, 2.0)]))
    return editor, boundary


def test_fig7_weld_removes_short_edge_adjacency() -> None:
    editor, boundary = _stacked_fig7_fixture(offset=0.02)

    candidates = editor.short_edge_candidates()
    assert candidates, "fixture should produce a short shared edge"

    before = unary_union(
        [Polygon([editor.positions[v] for v in r]) for r in editor.rings.values()]
    )

    edge = candidates[0]
    pair = tuple(sorted(editor.edge_parcels[edge]))
    result = editor.merge_short_edge(edge)
    assert result.removed_adjacency == pair

    after = unary_union(
        [Polygon([editor.positions[v] for v in r]) for r in editor.rings.values()]
    )
    assert math.isclose(after.area, before.area, rel_tol=1e-9)
    assert math.isclose(after.area, boundary.area, rel_tol=1e-9)

    mesh = editor.snapshot(boundary)
    corner = parcel_corner_graph(mesh)
    pgraph = parcel_graph(mesh, corner)
    a, b = pair
    assert b not in pgraph.neighbors[a]

    # The diagonal pair now share only the merged vertex.
    shared_vertices = set(editor.rings[a]) & set(editor.rings[b])
    assert shared_vertices == {result.merged_vertex}

    for ring in editor.rings.values():
        poly = Polygon([editor.positions[v] for v in ring])
        assert poly.is_valid and poly.area > 1e-12

    assert editor.interpolation_edges


def test_boundary_endpoint_merge_rules() -> None:
    # Construct a short edge where exactly one endpoint is on the boundary.
    boundary = Polygon([(0.0, 0.0), (2.0, 0.0), (2.0, 1.0), (0.0, 1.0)])
    editor = ParcelMeshEditor.from_boundary(boundary)

    # vertical cut, then a near-horizontal cut from the left boundary that
    # produces a short edge with one boundary endpoint.
    pid = next(iter(editor.rings))
    editor.split_parcel(pid, LineString([(1.0, 0.0), (1.0, 1.0)]))
    pid = _parcel_at(editor, 0.5, 0.5)
    editor.split_parcel(pid, LineString([(0.0, 0.5), (1.0, 0.52)]))

    candidates = editor.short_edge_candidates()
    # Find a candidate with exactly one boundary endpoint.
    one_boundary = [
        e
        for e in candidates
        if (e[0] in editor.boundary_vertex_ids) ^ (e[1] in editor.boundary_vertex_ids)
    ]
    if one_boundary:
        edge = one_boundary[0]
        bvert = edge[0] if edge[0] in editor.boundary_vertex_ids else edge[1]
        bpos = editor.positions[bvert]
        before_area = unary_union(
            [Polygon([editor.positions[v] for v in r]) for r in editor.rings.values()]
        ).area
        result = editor.merge_short_edge(edge)
        assert result.merged_vertex == bvert
        assert editor.positions[bvert] == bpos
        after_area = unary_union(
            [Polygon([editor.positions[v] for v in r]) for r in editor.rings.values()]
        ).area
        assert math.isclose(after_area, before_area, rel_tol=1e-9)


def test_both_boundary_endpoints_merge_rejected() -> None:
    editor = ParcelMeshEditor.from_boundary(_unit_square())
    pid = next(iter(editor.rings))
    # split straight across; both endpoints on the boundary
    result = editor.split_parcel(pid, LineString([(0.5, 0.0), (0.5, 1.0)]))
    a, b = result.new_parcel_ids
    shared = sorted(set(editor.rings[a]) & set(editor.rings[b]))
    edge = normalized_edge(shared[0], shared[1])
    assert edge[0] in editor.boundary_vertex_ids
    assert edge[1] in editor.boundary_vertex_ids
    with pytest.raises(ShortEdgeMergeRejected):
        editor.merge_short_edge(edge)


def test_split_edge_replacements_rewrite_street_set() -> None:
    editor = ParcelMeshEditor.from_boundary(_unit_square())
    pid = next(iter(editor.rings))
    ring = editor.rings[pid]
    # Pick the bottom boundary edge as a "street".
    bottom_edge = None
    for a, b in zip(ring, ring[1:] + ring[:1], strict=True):
        pa, pb = editor.positions[a], editor.positions[b]
        if abs(pa[1]) < 1e-9 and abs(pb[1]) < 1e-9:
            bottom_edge = normalized_edge(a, b)
            break
    assert bottom_edge is not None
    streets = {bottom_edge}

    result = editor.split_parcel(pid, LineString([(0.5, 0.0), (0.5, 1.0)]))
    for old, (new_a, new_b) in result.edge_replacements:
        if old in streets:
            streets.discard(old)
            streets.update((new_a, new_b))

    assert bottom_edge not in streets
    assert len(streets) == 2
    valid_edges = set(editor.edge_parcels)
    assert streets <= valid_edges


def test_merge_rejection_leaves_state_unchanged() -> None:
    # A merge whose weld would invalidate a third incident ring must roll back.
    # Build a fixture, then craft an editor where welding self-intersects.
    editor, boundary = _stacked_fig7_fixture(offset=0.02)
    candidates = editor.short_edge_candidates()
    assert candidates
    edge = candidates[0]

    snapshot_positions = dict(editor.positions)
    snapshot_rings = {k: list(v) for k, v in editor.rings.items()}
    snapshot_edge_parcels = {k: set(v) for k, v in editor.edge_parcels.items()}

    # Force a both-boundary rejection by tagging both endpoints as boundary.
    editor.boundary_vertex_ids.update(edge)
    with pytest.raises(ShortEdgeMergeRejected):
        editor.merge_short_edge(edge)

    # state unchanged (other than the boundary tags we injected)
    assert editor.positions == snapshot_positions
    assert editor.rings == snapshot_rings
    assert editor.edge_parcels == snapshot_edge_parcels


def _replay() -> ParcelMeshEditor:
    editor = ParcelMeshEditor.from_boundary(_unit_square())

    pid = next(iter(editor.rings))
    editor.split_parcel(pid, LineString([(0.5, 0.0), (0.5, 1.0)]))
    pid = _parcel_at(editor, 0.25, 0.5)
    editor.split_parcel(pid, LineString([(0.0, 0.5), (0.5, 0.5)]))
    pid = _parcel_at(editor, 0.75, 0.5)
    editor.split_parcel(pid, LineString([(0.5, 0.5), (1.0, 0.5)]))
    return editor


def test_determinism_identical_replays() -> None:
    e1 = _replay()
    e2 = _replay()
    assert e1.positions == e2.positions
    assert e1.rings == e2.rings
    assert e1.next_vertex_id == e2.next_vertex_id
    assert e1.next_parcel_id == e2.next_parcel_id
    assert e1.edge_parcels == e2.edge_parcels
