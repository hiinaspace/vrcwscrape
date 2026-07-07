"""Tests for R1 connectivity (``mapgen.r1_connect``): stage-1 gate extraction
and stage-2 macro-network snapping / the connectivity metric."""

from __future__ import annotations

import json

import networkx as nx
import pytest
from shapely import LineString, Polygon

from mapgen.chen_artifacts import _street_lines
from mapgen.chen_core import (
    ChenLayout,
    build_chen_layout,
    normalized_edge,
    parcel_mesh_from_polygons,
)
from mapgen.r1_connect import (
    Gate,
    SeamJunction,
    build_unified_street_graph,
    connectivity_metrics,
    densify_gates,
    extract_block_gates,
    graph_connectivity_summary,
    snap_gates_to_macro,
    split_line_at_stations,
    street_perimeter_flags,
    unplugged_runs,
)
from mapgen.r1_macro import MacroEdge

# ---------------------------------------------------------------------------
# Synthetic fixture
# ---------------------------------------------------------------------------
#
# A 20x10 rectangle boundary cut into a 5-parcel grid:
#
#   (0,10)---(10,10)---(15,10)---(20,10)
#     | top-left | tr-left | tr-right |
#   (0,5)----(10,5)----(15,5)----(20,5)
#     |  bottom-left   |  bottom-right |
#   (0,0)-----(10,0)------------(20,0)
#
# This gives three interior-vertex categories on the mesh:
#   - (10,5): interior (strictly inside the rectangle).
#   - (15,5): interior.
#   - every other vertex sits on the outer rectangle boundary.
#
# Street edges (a curated subset of the mesh, mirroring how the level-0 loop
# seeds the boundary ring and later levels add interior streets):
#   - the four boundary-ring sides -> decompose into 4 streets whose nodes
#     are ALL on the boundary (the "perimeter-duplicate" case).
#   - (10,0)-(10,5)-(10,10): a straight interior street whose ENDPOINTS are
#     on the boundary and whose one interior node is not (the "gate" case).
#   - (10,5)-(15,5): a straight interior street with NEITHER endpoint on the
#     boundary (the "fully interior, no gate" case).


def _boundary_rectangle() -> Polygon:
    return Polygon([(0.0, 0.0), (20.0, 0.0), (20.0, 10.0), (0.0, 10.0)])


def _layout_and_nodes() -> tuple[ChenLayout, dict[tuple[float, float], int]]:
    bottom_left = Polygon([(0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)])
    top_left = Polygon([(0.0, 5.0), (10.0, 5.0), (10.0, 10.0), (0.0, 10.0)])
    # (15.0, 5.0) is inserted mid-edge to conform with the T-junction where
    # top_right_left/top_right_right meet the bottom_right/top_left edge.
    bottom_right = Polygon(
        [(10.0, 0.0), (20.0, 0.0), (20.0, 5.0), (15.0, 5.0), (10.0, 5.0)]
    )
    top_right_left = Polygon([(10.0, 5.0), (15.0, 5.0), (15.0, 10.0), (10.0, 10.0)])
    top_right_right = Polygon([(15.0, 5.0), (20.0, 5.0), (20.0, 10.0), (15.0, 10.0)])

    mesh = parcel_mesh_from_polygons(
        [
            (1, bottom_left),
            (2, top_left),
            (3, bottom_right),
            (4, top_right_left),
            (5, top_right_right),
        ],
        boundary=_boundary_rectangle(),
    )
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }

    def n(x: float, y: float) -> int:
        return point_to_node[(x, y)]

    street_edges = {
        # Boundary ring (level-0 seed, per _editor_boundary_ring_edges).
        normalized_edge(n(0, 0), n(10, 0)),
        normalized_edge(n(10, 0), n(20, 0)),
        normalized_edge(n(20, 0), n(20, 5)),
        normalized_edge(n(20, 5), n(20, 10)),
        normalized_edge(n(20, 10), n(15, 10)),
        normalized_edge(n(15, 10), n(10, 10)),
        normalized_edge(n(10, 10), n(0, 10)),
        normalized_edge(n(0, 10), n(0, 5)),
        normalized_edge(n(0, 5), n(0, 0)),
        # Interior "gate" street: boundary -> interior -> boundary.
        normalized_edge(n(10, 0), n(10, 5)),
        normalized_edge(n(10, 5), n(10, 10)),
        # Fully interior street: interior -> interior.
        normalized_edge(n(10, 5), n(15, 5)),
    }

    return build_chen_layout(mesh, street_edges), point_to_node


def _find_street_id(layout: ChenLayout, node_set: set[int]) -> int:
    for street in layout.street_graph.streets:
        if set(street.nodes) == node_set:
            return street.street_id
    raise AssertionError(f"no street with node set {node_set}")


def test_extract_block_gates_classifies_perimeter_gate_and_interior_streets() -> None:
    layout, nodes = _layout_and_nodes()

    gate_street_id = _find_street_id(
        layout, {nodes[(10.0, 0.0)], nodes[(10.0, 5.0)], nodes[(10.0, 10.0)]}
    )
    interior_street_id = _find_street_id(
        layout, {nodes[(10.0, 5.0)], nodes[(15.0, 5.0)]}
    )

    result = extract_block_gates(layout)

    # Four perimeter-ring streets (bottom/right/top/left), none contribute gates.
    assert len(result.perimeter_street_ids) == 4
    assert gate_street_id not in result.perimeter_street_ids
    assert interior_street_id not in result.perimeter_street_ids

    # The gate street contributes exactly two gates, one per boundary endpoint.
    gate_ids = {g.vertex_id for g in result.gates}
    assert gate_ids == {nodes[(10.0, 0.0)], nodes[(10.0, 10.0)]}
    for gate in result.gates:
        assert gate.street_id == gate_street_id
    coords = {(g.x, g.y) for g in result.gates}
    assert coords == {(10.0, 0.0), (10.0, 10.0)}

    # The fully-interior street (no boundary-touching endpoint) yields no gate.
    assert all(g.street_id != interior_street_id for g in result.gates)

    # Exactly the gate street's two endpoints -> no gates from perimeter streets.
    assert len(result.gates) == 2


def test_extract_block_gates_gate_matches_mesh_vertex_coordinate() -> None:
    layout, nodes = _layout_and_nodes()
    result = extract_block_gates(layout)

    for gate in result.gates:
        vertex = layout.mesh.vertices[gate.vertex_id]
        assert vertex.on_boundary
        assert (gate.x, gate.y) == vertex.point


def test_extract_block_gates_is_deterministic_and_sorted() -> None:
    layout, _nodes = _layout_and_nodes()
    first = extract_block_gates(layout)
    second = extract_block_gates(layout)
    assert first == second

    keys = [(g.street_id, g.vertex_id) for g in first.gates]
    assert keys == sorted(keys)


def test_street_perimeter_flags_aligns_with_street_lines_and_extract_block_gates() -> (
    None
):
    layout, _nodes = _layout_and_nodes()
    gates_result = extract_block_gates(layout)
    lines = _street_lines(layout)
    flags = street_perimeter_flags(layout)

    assert len(flags) == len(lines)
    for flag, (_line, properties) in zip(flags, lines, strict=True):
        expected = properties["street_id"] in gates_result.perimeter_street_ids
        assert flag == expected

    # Sanity: exactly 4 perimeter streets, 2 non-perimeter (gate + interior).
    assert sum(flags) == 4
    assert len(flags) - sum(flags) == 2


def test_street_perimeter_flags_all_true_for_boundary_only_layout() -> None:
    # A single-parcel layout whose only street is the boundary ring itself.
    square = Polygon([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
    mesh = parcel_mesh_from_polygons([(1, square)], boundary=square)
    layout = build_chen_layout(mesh, mesh.edges)

    flags = street_perimeter_flags(layout)
    result = extract_block_gates(layout)

    assert flags
    assert all(flags)
    assert not result.gates
    assert result.perimeter_street_ids


def test_extract_block_gates_no_gates_for_fully_interior_only_network() -> None:
    # Street network containing only the fully-interior street: no gates,
    # and it is not classified as perimeter either.
    layout, nodes = _layout_and_nodes()
    interior_street_id = _find_street_id(
        layout, {nodes[(10.0, 5.0)], nodes[(15.0, 5.0)]}
    )
    result = extract_block_gates(layout)
    assert interior_street_id not in result.perimeter_street_ids
    assert all(g.street_id != interior_street_id for g in result.gates)


# ---------------------------------------------------------------------------
# Stage 2 fixture: a synthetic macro network (two arterials, one ring, one
# coastline) and six single-gate blocks exercising every SeamJunction kind.
# ---------------------------------------------------------------------------
#
#   arterial_0: (0,10)--------------------(20,10)   tier=2 highway, length 20
#   arterial_1: (10,0) | (10,20)                     tier=1 major,  length 20
#   ring_0:     a small closed square ring at (15..18, 15..18)
#   boundary:   the (-5,-5)-(25,25) square; its exterior is the "coast"
#
# Gates (one per block, by design):
#   block 0 -> (5, 10)     exactly on arterial_0            -> arterial, d=0
#   block 1 -> (15, 10.03) 0.03 off arterial_0 (< tol=0.05) -> arterial, d=0.03
#   block 2 -> (10, 5)     exactly on arterial_1             -> arterial, d=0
#   block 3 -> (16.5, 15)  exactly on ring_0                 -> ring, d=0
#   block 4 -> (-5, 10)    exactly on the boundary exterior   -> coast, d=0
#   block 5 -> (5, 10.2)   0.2 off arterial_0 (> tol)         -> unmatched
#
# Blocks 0 and 1 both land on arterial_0's station list (stations 5.0 and
# 15.0) -- the two-blocks-share-one-arterial case slice 3 needs. A 7th block
# (id 6) has no gates at all, exercising the "no gates" / n_blocks_no_gates
# bookkeeping.


def _stage2_fixture() -> tuple[
    dict[int, list[Gate]],
    list[LineString],
    list[MacroEdge],
    list[LineString],
    Polygon,
    list[Polygon],
]:
    arterial_lines = [
        LineString([(0.0, 10.0), (20.0, 10.0)]),  # arterial_0, tier=2, length 20
        LineString([(10.0, 0.0), (10.0, 20.0)]),  # arterial_1, tier=1, length 20
    ]
    edges = [
        MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=20.0),
        MacroEdge(node_a=0, node_b=2, tau=1, tier=1, path_cost=1.0, length=20.0),
    ]
    ring_lines = [
        LineString(
            [(15.0, 15.0), (18.0, 15.0), (18.0, 18.0), (15.0, 18.0), (15.0, 15.0)]
        )
    ]
    boundary = Polygon([(-5.0, -5.0), (25.0, -5.0), (25.0, 25.0), (-5.0, 25.0)])

    gates_by_block: dict[int, list[Gate]] = {
        0: [Gate(x=5.0, y=10.0, street_id=1, vertex_id=1)],
        1: [Gate(x=15.0, y=10.03, street_id=2, vertex_id=2)],
        2: [Gate(x=10.0, y=5.0, street_id=3, vertex_id=3)],
        3: [Gate(x=16.5, y=15.0, street_id=4, vertex_id=4)],
        4: [Gate(x=-5.0, y=10.0, street_id=5, vertex_id=5)],
        5: [Gate(x=5.0, y=10.2, street_id=6, vertex_id=6)],
    }

    # 7 blocks (0..6); block 6 has no gates at all.
    blocks = [
        Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]) for _ in range(7)
    ]

    return gates_by_block, arterial_lines, edges, ring_lines, boundary, blocks


def test_snap_gates_to_macro_classifies_kinds_tiers_stations_distances() -> None:
    gates_by_block, arterial_lines, edges, ring_lines, boundary, _blocks = (
        _stage2_fixture()
    )

    junctions = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, ring_lines, boundary
    )

    by_block = {j.block_id: j for j in junctions}
    assert set(by_block) == {0, 1, 2, 3, 4, 5}

    j0 = by_block[0]
    assert j0.kind == "arterial"
    assert j0.macro_index == 0
    assert j0.tier == 2
    assert j0.station == pytest.approx(5.0)
    assert j0.distance == pytest.approx(0.0, abs=1e-9)

    j1 = by_block[1]
    assert j1.kind == "arterial"
    assert j1.macro_index == 0
    assert j1.tier == 2
    assert j1.station == pytest.approx(15.0)
    assert j1.distance == pytest.approx(0.03)

    j2 = by_block[2]
    assert j2.kind == "arterial"
    assert j2.macro_index == 1
    assert j2.tier == 1
    assert j2.station == pytest.approx(5.0)
    assert j2.distance == pytest.approx(0.0, abs=1e-9)

    j3 = by_block[3]
    assert j3.kind == "ring"
    assert j3.macro_index == 0
    assert j3.tier == -1
    assert j3.distance == pytest.approx(0.0, abs=1e-9)

    j4 = by_block[4]
    assert j4.kind == "coast"
    assert j4.tier == -1
    assert j4.distance == pytest.approx(0.0, abs=1e-9)

    j5 = by_block[5]
    assert j5.kind == "unmatched"
    assert j5.macro_index == -1
    assert j5.tier == -1
    assert j5.distance == pytest.approx(0.2)

    # Output sorted by (block_id, x, y).
    keys = [(j.block_id, j.x, j.y) for j in junctions]
    assert keys == sorted(keys)


def test_snap_gates_to_macro_two_blocks_share_one_arterial_station_list() -> None:
    gates_by_block, arterial_lines, edges, ring_lines, boundary, _blocks = (
        _stage2_fixture()
    )
    junctions = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, ring_lines, boundary
    )

    arterial0_stations = sorted(
        j.station for j in junctions if j.kind == "arterial" and j.macro_index == 0
    )
    assert arterial0_stations == pytest.approx([5.0, 15.0])
    arterial0_blocks = {
        j.block_id for j in junctions if j.kind == "arterial" and j.macro_index == 0
    }
    assert arterial0_blocks == {0, 1}


def test_snap_gates_to_macro_handles_empty_block_gracefully() -> None:
    gates_by_block, arterial_lines, edges, ring_lines, boundary, _blocks = (
        _stage2_fixture()
    )
    gates_by_block = {**gates_by_block, 6: []}
    junctions = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, ring_lines, boundary
    )
    assert all(j.block_id != 6 for j in junctions)
    assert len(junctions) == 6


def test_snap_gates_to_macro_no_candidates_falls_back_to_coast_or_unmatched() -> None:
    boundary = Polygon([(-5.0, -5.0), (25.0, -5.0), (25.0, 25.0), (-5.0, 25.0)])
    gates_by_block = {
        0: [Gate(x=-5.0, y=0.0, street_id=1, vertex_id=1)],  # on coast
        1: [Gate(x=5.0, y=5.0, street_id=2, vertex_id=2)],  # nowhere near coast
    }
    junctions = snap_gates_to_macro(gates_by_block, [], [], [], boundary)
    by_block = {j.block_id: j for j in junctions}
    assert by_block[0].kind == "coast"
    assert by_block[1].kind == "unmatched"


def test_snap_gates_to_macro_is_deterministic() -> None:
    gates_by_block, arterial_lines, edges, ring_lines, boundary, _blocks = (
        _stage2_fixture()
    )
    first = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, ring_lines, boundary
    )
    second = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, ring_lines, boundary
    )
    assert first == second

    # Insertion order of the input dict must not affect the (sorted) output.
    reordered = dict(reversed(list(gates_by_block.items())))
    third = snap_gates_to_macro(reordered, arterial_lines, edges, ring_lines, boundary)
    assert first == third


# ---------------------------------------------------------------------------
# unplugged_runs
# ---------------------------------------------------------------------------


def test_unplugged_runs_hand_computed_gaps() -> None:
    # stations [5, 15] on a length-20 line -> gaps [5, 10, 5].
    assert unplugged_runs(20.0, [5.0, 15.0]) == pytest.approx([5.0, 10.0, 5.0])
    # Unsorted/duplicate input is handled the same way.
    assert unplugged_runs(20.0, [15.0, 5.0, 5.0]) == pytest.approx([5.0, 10.0, 5.0])


def test_unplugged_runs_no_stations_is_one_full_span() -> None:
    assert unplugged_runs(20.0, []) == pytest.approx([20.0])


def test_unplugged_runs_station_at_each_endpoint_drops_degenerate_gaps() -> None:
    # A station sitting exactly on an endpoint contributes no zero-length gap.
    assert unplugged_runs(20.0, [0.0, 20.0]) == pytest.approx([20.0])


def test_unplugged_runs_degenerate_line_is_empty() -> None:
    assert unplugged_runs(0.0, [1.0, 2.0]) == []
    assert unplugged_runs(-1.0, []) == []


# ---------------------------------------------------------------------------
# connectivity_metrics
# ---------------------------------------------------------------------------


def test_connectivity_metrics_exact_values() -> None:
    gates_by_block, arterial_lines, edges, ring_lines, boundary, blocks = (
        _stage2_fixture()
    )
    junctions = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, ring_lines, boundary
    )
    metrics = connectivity_metrics(junctions, arterial_lines, edges, ring_lines, blocks)

    assert metrics["n_gates_total"] == 6
    assert metrics["n_tjunctions"] == 4
    assert metrics["n_arterial"] == 3
    assert metrics["n_arterial_by_tier"] == {"2": 2, "1": 1}
    assert metrics["n_ring"] == 1
    assert metrics["n_coast"] == 1
    assert metrics["n_unmatched"] == 1

    assert metrics["n_blocks_total"] == 7
    assert metrics["n_blocks_with_gates"] == 6
    assert metrics["n_blocks_no_gates"] == 1

    # Per-block T-junction counts (arterial+ring only) over the 6 non-empty
    # blocks: [1, 1, 1, 1, 0, 0] (blocks 4=coast and 5=unmatched score 0).
    assert metrics["per_block_tjunctions_min"] == 0
    assert metrics["per_block_tjunctions_median"] == pytest.approx(1.0)
    assert metrics["per_block_tjunctions_max"] == 1
    assert metrics["n_blocks_zero_tjunctions"] == 2

    assert metrics["arterial_length_total"] == pytest.approx(40.0)
    # n_tjunctions=4, arterial_length_total=40 -> 4*10/40 = 1.0.
    assert metrics["tjunctions_per_10_units_arterial"] == pytest.approx(1.0)

    # Hand-computed: arterial_0 gaps [5, 10, 5], arterial_1 gaps [5, 15].
    assert metrics["unplugged_run_max"] == pytest.approx(15.0)
    assert metrics["unplugged_run_median"] == pytest.approx(5.0)

    # Matched distances: [0, 0.03, 0, 0, 0] (arterial x3, ring, coast).
    assert metrics["snap_distance_max"] == pytest.approx(0.03)
    assert metrics["snap_distance_mean"] == pytest.approx(0.006)


def test_connectivity_metrics_empty_junctions_no_division_by_zero() -> None:
    _gates, arterial_lines, edges, ring_lines, boundary, blocks = _stage2_fixture()
    metrics = connectivity_metrics([], arterial_lines, edges, ring_lines, blocks)

    assert metrics["n_gates_total"] == 0
    assert metrics["n_tjunctions"] == 0
    assert metrics["n_blocks_with_gates"] == 0
    assert metrics["n_blocks_no_gates"] == len(blocks)
    assert metrics["per_block_tjunctions_min"] == 0
    assert metrics["per_block_tjunctions_median"] == pytest.approx(0.0)
    assert metrics["per_block_tjunctions_max"] == 0
    assert metrics["n_blocks_zero_tjunctions"] == 0
    assert metrics["tjunctions_per_10_units_arterial"] == pytest.approx(0.0)
    # No junctions at all -> each of the two length-20 arterials is one
    # single unplugged run spanning its whole length.
    assert metrics["unplugged_run_max"] == pytest.approx(20.0)
    assert metrics["unplugged_run_median"] == pytest.approx(20.0)
    assert metrics["snap_distance_max"] == pytest.approx(0.0)
    assert metrics["snap_distance_mean"] == pytest.approx(0.0)


def test_connectivity_metrics_no_arterials_no_division_by_zero() -> None:
    boundary = Polygon([(-5.0, -5.0), (25.0, -5.0), (25.0, 25.0), (-5.0, 25.0)])
    gates_by_block = {0: [Gate(x=-5.0, y=0.0, street_id=1, vertex_id=1)]}
    junctions = snap_gates_to_macro(gates_by_block, [], [], [], boundary)
    metrics = connectivity_metrics(junctions, [], [], [], [])

    assert metrics["n_coast"] == 1
    assert metrics["arterial_length_total"] == pytest.approx(0.0)
    assert metrics["tjunctions_per_10_units_arterial"] == pytest.approx(0.0)


def test_connectivity_metrics_rejects_misaligned_arterials_and_edges() -> None:
    _gates, arterial_lines, edges, ring_lines, boundary, blocks = _stage2_fixture()
    with pytest.raises(ValueError):
        connectivity_metrics([], arterial_lines, edges[:1], ring_lines, blocks)


def test_connectivity_metrics_is_deterministic() -> None:
    gates_by_block, arterial_lines, edges, ring_lines, boundary, blocks = (
        _stage2_fixture()
    )
    junctions = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, ring_lines, boundary
    )
    first = connectivity_metrics(junctions, arterial_lines, edges, ring_lines, blocks)
    second = connectivity_metrics(junctions, arterial_lines, edges, ring_lines, blocks)
    assert first == second


def test_seam_junction_is_frozen_and_comparable() -> None:
    a = SeamJunction(
        block_id=0,
        x=1.0,
        y=2.0,
        kind="arterial",
        macro_index=0,
        tier=2,
        station=1.0,
        distance=0.0,
    )
    b = SeamJunction(
        block_id=0,
        x=1.0,
        y=2.0,
        kind="arterial",
        macro_index=0,
        tier=2,
        station=1.0,
        distance=0.0,
    )
    assert a == b
    with pytest.raises(AttributeError):
        a.x = 5.0  # ty: ignore[invalid-assignment]


# ---------------------------------------------------------------------------
# split_line_at_stations
# ---------------------------------------------------------------------------


def test_split_line_at_stations_correct_segments_and_lengths() -> None:
    line = LineString([(0.0, 0.0), (20.0, 0.0)])
    segments = split_line_at_stations(line, [5.0, 15.0])

    assert len(segments) == 3
    assert [round(s.length, 6) for s in segments] == [5.0, 10.0, 5.0]
    assert segments[0].coords[0] == (0.0, 0.0)
    assert segments[-1].coords[-1] == (20.0, 0.0)
    # Chained: each segment's end is the next segment's start.
    for a, b in zip(segments, segments[1:], strict=False):
        assert a.coords[-1] == b.coords[0]


def test_split_line_at_stations_unsorted_and_duplicate_input() -> None:
    line = LineString([(0.0, 0.0), (20.0, 0.0)])
    segments = split_line_at_stations(line, [15.0, 5.0, 5.0])
    assert [round(s.length, 6) for s in segments] == [5.0, 10.0, 5.0]


def test_split_line_at_stations_dedups_near_coincident_and_endpoint_stations() -> None:
    line = LineString([(0.0, 0.0), (20.0, 0.0)])
    # 0.0 and ~20.0 collapse into the endpoints; the two ~5.0 stations
    # collapse into a single cut.
    stations = [0.0, 5.0, 5.0000001, 20.0, 19.9999999]
    segments = split_line_at_stations(line, stations, min_gap=1e-6)

    assert len(segments) == 2
    assert round(segments[0].length, 6) == 5.0
    assert round(segments[1].length, 6) == 15.0


def test_split_line_at_stations_empty_stations_returns_whole_line() -> None:
    line = LineString([(0.0, 0.0), (20.0, 0.0)])
    segments = split_line_at_stations(line, [])

    assert len(segments) == 1
    assert list(segments[0].coords) == list(line.coords)


def test_split_line_at_stations_degenerate_line_returns_whole_line() -> None:
    line = LineString([(3.0, 3.0), (3.0, 3.0)])
    segments = split_line_at_stations(line, [1.0])
    assert segments == [line]


# ---------------------------------------------------------------------------
# build_unified_street_graph / graph_connectivity_summary
# ---------------------------------------------------------------------------
#
# One arterial (y=10, x in [0,20], tier=2) is gated by two blocks at exact
# stations 5.0 and 15.0 (distance 0 -- a real coincidence, unlike the
# stage-2 "unmatched"/near-miss cases above). Block 0 hangs a two-segment
# local street off its gate (also exercising same-block endpoint fusion at
# (5,5)) plus one perimeter-flagged street that must be dropped entirely.
# Block 1 hangs a single local segment off its gate. This is deliberately a
# fresh, self-contained fixture (build_unified_street_graph needs actual
# local-street LineStrings, which the stage-2 SeamJunction fixture above
# does not carry) but mirrors that fixture's "two blocks share one arterial"
# shape.


def _graph_fixture() -> tuple[
    list[LineString],
    list[MacroEdge],
    list[LineString],
    list[SeamJunction],
    dict[int, list[LineString]],
    dict[int, list[bool]],
]:
    arterial_lines = [LineString([(0.0, 10.0), (20.0, 10.0)])]
    edges = [MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=20.0)]
    ring_lines: list[LineString] = []

    junctions = [
        SeamJunction(
            block_id=0,
            x=5.0,
            y=10.0,
            kind="arterial",
            macro_index=0,
            tier=2,
            station=5.0,
            distance=0.0,
        ),
        SeamJunction(
            block_id=1,
            x=15.0,
            y=10.0,
            kind="arterial",
            macro_index=0,
            tier=2,
            station=15.0,
            distance=0.0,
        ),
    ]

    streets_by_block = {
        0: [
            LineString([(5.0, 10.0), (5.0, 5.0)]),
            LineString([(5.0, 5.0), (2.0, 5.0)]),
            # Perimeter-duplicate (block boundary ring subpath) -- dropped.
            LineString([(2.0, 10.0), (2.0, 8.0), (8.0, 8.0), (8.0, 10.0)]),
        ],
        1: [
            LineString([(15.0, 10.0), (15.0, 5.0)]),
        ],
    }
    perimeter_flags_by_block = {
        0: [False, False, True],
        1: [False],
    }

    return (
        arterial_lines,
        edges,
        ring_lines,
        junctions,
        streets_by_block,
        perimeter_flags_by_block,
    )


def test_build_unified_street_graph_gate_produces_real_t_junction() -> None:
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )

    node_5_10 = (5_000_000, 10_000_000)
    node_15_10 = (15_000_000, 10_000_000)
    assert node_5_10 in g
    assert node_15_10 in g
    # Arterial split (2 edges) + local branch (1 edge) => degree 3.
    assert g.degree[node_5_10] == 3
    assert g.degree[node_15_10] == 3


def test_build_unified_street_graph_drops_perimeter_streets() -> None:
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )

    dropped_coords = [(2.0, 10.0), (2.0, 8.0), (8.0, 8.0), (8.0, 10.0)]
    for x, y in dropped_coords:
        assert (round(x * 1_000_000), round(y * 1_000_000)) not in g


def test_build_unified_street_graph_fuses_coincident_endpoints() -> None:
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )

    # Nodes: arterial (0,10),(5,10),(15,10),(20,10) + block-0 local (5,5),
    # (2,5) + block-1 local (15,5) = 7, not the naive per-line vertex sum
    # (4 + 2 + 2 + 1 = 9) a non-fusing implementation would produce.
    assert g.number_of_nodes() == 7
    node_5_5 = (5_000_000, 5_000_000)
    assert node_5_5 in g
    assert g.degree[node_5_5] == 2  # shared endpoint of the two block-0 streets.


def test_build_unified_street_graph_two_blocks_share_one_component() -> None:
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )
    assert nx.number_connected_components(g) == 1


def test_build_unified_street_graph_edge_attrs() -> None:
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )

    kinds = {data["kind"] for _u, _v, data in g.edges(data=True)}
    assert kinds == {"arterial", "local"}
    for _u, _v, data in g.edges(data=True):
        if data["kind"] == "arterial":
            assert data["tier"] == 2
        elif data["kind"] == "local":
            assert data["tier"] == -1
    local_block_ids = {
        data["block_id"]
        for _u, _v, data in g.edges(data=True)
        if data["kind"] == "local"
    }
    assert local_block_ids == {0, 1}


def test_build_unified_street_graph_is_deterministic() -> None:
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    g1 = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )
    g2 = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )

    assert sorted(g1.nodes) == sorted(g2.nodes)
    assert sorted(g1.edges) == sorted(g2.edges)
    for n in g1.nodes:
        assert g1.nodes[n] == g2.nodes[n]
    for u, v in g1.edges:
        assert g1.edges[u, v] == g2.edges[u, v]


def test_build_unified_street_graph_rejects_misaligned_arterials_and_edges() -> None:
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    with pytest.raises(ValueError):
        build_unified_street_graph(
            arterial_lines, edges[:0], ring_lines, junctions, streets_by_block, flags
        )


def test_graph_connectivity_summary_dominant_component_and_tjunctions() -> None:
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )
    summary = graph_connectivity_summary(g)

    assert summary["n_nodes"] == 7
    assert summary["n_edges"] == 6
    assert summary["n_components"] == 1
    assert summary["largest_component_local_length_fraction"] == pytest.approx(1.0)
    # (5,10) and (15,10) are the only nodes touching both an arterial and a
    # local edge.
    assert summary["n_tjunctions_realized"] == 2


def test_graph_connectivity_summary_is_json_serializable() -> None:
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )
    summary = graph_connectivity_summary(g)
    json.dumps(summary)  # must not raise


def test_graph_connectivity_summary_empty_graph_no_division_by_zero() -> None:
    summary = graph_connectivity_summary(nx.Graph())
    assert summary["n_nodes"] == 0
    assert summary["n_edges"] == 0
    assert summary["n_components"] == 0
    assert summary["largest_component_local_length_fraction"] == pytest.approx(0.0)
    assert summary["n_tjunctions_realized"] == 0


# ---------------------------------------------------------------------------
# build_unified_street_graph -- off-line-but-within-tol snap fusion
# ---------------------------------------------------------------------------
#
# snap_gates_to_macro accepts snaps up to tol=0.05 away from the matched
# arterial/ring line, but node identity (_node_key / chen_core.node_key) is
# exact-coordinate quantization at 1e-6. A gate snapped at a nonzero distance
# (e.g. 0.03, well within tol) has a raw (x, y) that does NOT round to the
# same node key as the on-line point (line.interpolate(station)) the arterial
# was actually cut at, UNLESS build_unified_street_graph explicitly re-snaps
# the local street's matching coordinate onto that on-line point first.


def test_build_unified_street_graph_fuses_off_line_gate_within_tol() -> None:
    # Arterial at y=10; the "gate" is 0.03 off the line (within tol=0.05, the
    # same offset test_snap_gates_to_macro_classifies_kinds_tiers_stations_
    # distances uses for its within-tol case). The local street's boundary
    # endpoint carries that same raw, off-line coordinate -- exactly what
    # extract_block_gates/chen_in_block would hand back for a genuinely (if
    # atypically) off-line-but-snapped gate.
    arterial_lines = [LineString([(0.0, 10.0), (20.0, 10.0)])]
    edges = [MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=20.0)]
    ring_lines: list[LineString] = []
    junctions = [
        SeamJunction(
            block_id=0,
            x=5.0,
            y=10.03,
            kind="arterial",
            macro_index=0,
            tier=2,
            station=5.0,
            distance=0.03,
        ),
    ]
    streets_by_block = {0: [LineString([(5.0, 10.03), (5.0, 5.0)])]}
    flags = {0: [False]}

    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )
    summary = graph_connectivity_summary(g)

    # The off-line gate coordinate itself must NOT survive as its own node --
    # it is re-snapped onto the arterial's exact on-line split point.
    assert (5_000_000, 10_030_000) not in g
    node_on_line = (5_000_000, 10_000_000)
    assert node_on_line in g
    assert g.degree[node_on_line] == 3
    assert nx.number_connected_components(g) == 1
    assert summary["n_tjunctions_realized"] == 1


def test_build_unified_street_graph_off_line_gate_beyond_snap_untouched() -> None:
    # A local street endpoint that does NOT match any junction's raw gate
    # coordinate (e.g. it belongs to an "unmatched"/never-snapped street, or
    # simply isn't a gate at all) is left exactly as given -- the re-snap only
    # ever touches coordinates that equal a snapped arterial/ring junction's
    # own (x, y), scoped to that junction's block.
    arterial_lines = [LineString([(0.0, 10.0), (20.0, 10.0)])]
    edges = [MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=20.0)]
    ring_lines: list[LineString] = []
    junctions: list[SeamJunction] = []
    streets_by_block = {0: [LineString([(5.0, 10.03), (5.0, 5.0)])]}
    flags = {0: [False]}

    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )
    assert (5_000_000, 10_030_000) in g
    assert (5_000_000, 10_000_000) not in g


# ---------------------------------------------------------------------------
# Consistency cross-check: stage-2 n_tjunctions vs graph n_tjunctions_realized
# ---------------------------------------------------------------------------
#
# The correct relationship (confirmed against a real densified hybrid run,
# artifacts/r1/hybrid_conn_dense/hybrid_manifest.json: n_tjunctions=118,
# graph.n_tjunctions_realized=123) is n_tjunctions_realized >= n_tjunctions,
# NOT equality. n_tjunctions (connectivity_metrics) counts snapped GATES --
# only the two *endpoints* of a decomposed street/connector ever become a
# Gate (extract_block_gates / densify_gates). n_tjunctions_realized
# (graph_connectivity_summary) counts GRAPH NODES with both a macro- and a
# local-kind incident edge, over the FULL vertex chain of every arterial/ring
# input line AND every local street, including vertices that were never
# extracted as a gate. Two arterial (or ring) *edges* sharing an endpoint is
# exactly such a case: that shared macro-graph node is unconditionally in the
# graph (both edges' own literal endpoint), regardless of whether any gate
# ever snapped there. Traced against a real densified hybrid run
# (artifacts/r1/hybrid_conn_dense), a densify_gates connector -- a
# multi-vertex shortest path over the parcel corner graph from a boundary
# point to an interior street node -- concretely produced this shape: its
# path threaded through an arterial-arterial macro-graph node mid-route (not
# at either of the connector's own endpoints), realizing a T-junction stage-2
# never counted. This test reproduces that shape directly with a local street
# whose middle vertex (not either of its own endpoints) coincides with a
# shared arterial-arterial macro-graph node.


def test_tjunctions_realized_can_exceed_snapped_tjunctions_via_midpath_touch() -> None:
    # Two arterial edges sharing the macro-graph node (10, 10) -- e.g. an
    # arterial intersection -- unconditionally present in the graph.
    arterial_lines = [
        LineString([(0.0, 10.0), (10.0, 10.0)]),
        LineString([(10.0, 10.0), (20.0, 10.0)]),
    ]
    edges = [
        MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=10.0),
        MacroEdge(node_a=1, node_b=2, tau=2, tier=2, path_cost=1.0, length=10.0),
    ]
    ring_lines: list[LineString] = []
    boundary = Polygon([(-5.0, -5.0), (25.0, -5.0), (25.0, 25.0), (-5.0, 25.0)])

    # One genuine, extracted gate (the block's only Gate) snapping cleanly.
    gates_by_block = {0: [Gate(x=5.0, y=10.0, street_id=1, vertex_id=1)]}
    junctions = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, ring_lines, boundary
    )
    assert len(junctions) == 1

    # A second local street (a densify-connector-shaped path) whose middle
    # vertex -- not either of its own endpoints -- happens to sit exactly on
    # the shared arterial-arterial node (10, 10). This vertex was never
    # extracted as a Gate (it is interior to this street's node chain), so it
    # contributes nothing to stage-2's n_tjunctions, but
    # build_unified_street_graph fuses it there anyway (pure coordinate
    # identity, no station/gate machinery involved).
    connector = LineString([(15.0, 8.0), (10.0, 10.0), (5.0, 5.0)])
    streets_by_block = {
        0: [LineString([(5.0, 10.0), (5.0, 5.0)]), connector],
    }
    perimeter_flags_by_block = {0: [False, False]}

    conn_metrics = connectivity_metrics(
        junctions,
        arterial_lines,
        edges,
        ring_lines,
        [Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])],
    )
    g = build_unified_street_graph(
        arterial_lines,
        edges,
        ring_lines,
        junctions,
        streets_by_block,
        perimeter_flags_by_block,
    )
    graph_summary = graph_connectivity_summary(g)

    assert conn_metrics["n_tjunctions"] == 1
    # The midpath touch at (10, 10) realizes a SECOND T-junction the stage-2
    # count never saw -- the documented n_tjunctions_realized >= n_tjunctions
    # relationship, not equality.
    assert graph_summary["n_tjunctions_realized"] == 2
    assert graph_summary["n_tjunctions_realized"] > conn_metrics["n_tjunctions"]


# ---------------------------------------------------------------------------
# densify_gates (stage 4 -- assembly-layer boundary-run densification)
# ---------------------------------------------------------------------------
#
# Reuses the ``_layout_and_nodes`` fixture (20x10 rectangle, 5 parcels). Its
# two organic gates (from the gate street at x=10) sit at stations 10 and 40
# on the 60-unit boundary ring, splitting it into two equal 30-unit runs:
#   - run A: (10,0) -> (20,0) -> (20,10)-ish -> (10,10ish), midpoint station
#     25 lands exactly on boundary vertex (20,5); nearest interior street
#     node is (15,5) (the fully-interior street's endpoint), distance 5.
#   - run B: the wraparound run, midpoint station 55 lands exactly on
#     boundary vertex (0,5); nearest interior street node is (10,5) (the
#     gate street's own interior node), distance 10.
# Both midpoints land exactly on a mesh boundary vertex by construction, so
# the "nearest boundary vertex" tie-break never has to break a real tie.


def test_densify_gates_routes_connector_to_nearest_interior_node() -> None:
    layout, nodes = _layout_and_nodes()
    existing_gates = list(extract_block_gates(layout).gates)
    assert {(g.x, g.y) for g in existing_gates} == {(10.0, 0.0), (10.0, 10.0)}

    connectors, new_gates = densify_gates(
        layout,
        _boundary_rectangle(),
        existing_gates,
        max_gate_spacing=25.0,
    )

    assert len(connectors) == 2
    assert len(new_gates) == 2

    starts = sorted(
        (round(c.coords[0][0], 6), round(c.coords[0][1], 6)) for c in connectors
    )
    assert starts == [(0.0, 5.0), (20.0, 5.0)]

    ends = sorted(
        (round(c.coords[-1][0], 6), round(c.coords[-1][1], 6)) for c in connectors
    )
    assert ends == [(10.0, 5.0), (15.0, 5.0)]

    gate_coords = sorted((g.x, g.y) for g in new_gates)
    assert gate_coords == [(0.0, 5.0), (20.0, 5.0)]
    assert all(g.street_id < 0 for g in new_gates)
    assert len({g.street_id for g in new_gates}) == 2
    assert {g.vertex_id for g in new_gates} == {
        nodes[(0.0, 5.0)],
        nodes[(20.0, 5.0)],
    }


def test_densify_gates_respects_max_spacing_threshold() -> None:
    layout, _nodes = _layout_and_nodes()
    existing_gates = list(extract_block_gates(layout).gates)

    # Both boundary runs are exactly 30 units; a threshold >= 30 leaves them
    # alone (spacing "respected"), a threshold just under 30 flags both.
    connectors_at_30, gates_at_30 = densify_gates(
        layout, _boundary_rectangle(), existing_gates, max_gate_spacing=30.0
    )
    assert connectors_at_30 == []
    assert gates_at_30 == []

    connectors_below, gates_below = densify_gates(
        layout, _boundary_rectangle(), existing_gates, max_gate_spacing=29.9
    )
    assert len(connectors_below) == 2
    assert len(gates_below) == 2


def test_densify_gates_min_connector_frac_filters_short_paths() -> None:
    layout, _nodes = _layout_and_nodes()
    existing_gates = list(extract_block_gates(layout).gates)

    # Run A's shortest connector path is length 5.0 ((20,5) -> (15,5)); run
    # B's is length 10.0 ((0,5) -> (10,5)). A frac threshold demanding >= 6.0
    # drops run A's connector but keeps run B's.
    connectors, gates = densify_gates(
        layout,
        _boundary_rectangle(),
        existing_gates,
        max_gate_spacing=25.0,
        min_connector_frac=6.0 / 25.0,
    )
    assert len(connectors) == 1
    assert len(gates) == 1
    assert connectors[0].length == pytest.approx(10.0)
    assert (gates[0].x, gates[0].y) == (0.0, 5.0)


def test_densify_gates_skips_run_with_no_reachable_interior_node() -> None:
    # Same 5-parcel mesh, but the street network is ONLY the boundary ring --
    # no interior street node exists anywhere in the block, so every run
    # (the whole 60-unit perimeter, since there are no gates either) must be
    # skipped rather than inventing a connector with no real target.
    bottom_left = Polygon([(0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)])
    top_left = Polygon([(0.0, 5.0), (10.0, 5.0), (10.0, 10.0), (0.0, 10.0)])
    bottom_right = Polygon(
        [(10.0, 0.0), (20.0, 0.0), (20.0, 5.0), (15.0, 5.0), (10.0, 5.0)]
    )
    top_right_left = Polygon([(10.0, 5.0), (15.0, 5.0), (15.0, 10.0), (10.0, 10.0)])
    top_right_right = Polygon([(15.0, 5.0), (20.0, 5.0), (20.0, 10.0), (15.0, 10.0)])

    mesh = parcel_mesh_from_polygons(
        [
            (1, bottom_left),
            (2, top_left),
            (3, bottom_right),
            (4, top_right_left),
            (5, top_right_right),
        ],
        boundary=_boundary_rectangle(),
    )
    point_to_node = {v.point: v.vertex_id for v in mesh.vertices.values()}

    def n(x: float, y: float) -> int:
        return point_to_node[(x, y)]

    street_edges = {
        normalized_edge(n(0, 0), n(10, 0)),
        normalized_edge(n(10, 0), n(20, 0)),
        normalized_edge(n(20, 0), n(20, 5)),
        normalized_edge(n(20, 5), n(20, 10)),
        normalized_edge(n(20, 10), n(15, 10)),
        normalized_edge(n(15, 10), n(10, 10)),
        normalized_edge(n(10, 10), n(0, 10)),
        normalized_edge(n(0, 10), n(0, 5)),
        normalized_edge(n(0, 5), n(0, 0)),
    }
    layout = build_chen_layout(mesh, street_edges)

    connectors, new_gates = densify_gates(
        layout, _boundary_rectangle(), [], max_gate_spacing=1.0
    )
    assert connectors == []
    assert new_gates == []


def test_densify_gates_is_deterministic() -> None:
    layout, _nodes = _layout_and_nodes()
    existing_gates = list(extract_block_gates(layout).gates)

    first = densify_gates(
        layout, _boundary_rectangle(), existing_gates, max_gate_spacing=25.0
    )
    second = densify_gates(
        layout, _boundary_rectangle(), existing_gates, max_gate_spacing=25.0
    )
    assert [list(c.coords) for c in first[0]] == [list(c.coords) for c in second[0]]
    assert first[1] == second[1]

    # Gate insertion order must not matter either.
    reordered = list(reversed(existing_gates))
    third = densify_gates(
        layout, _boundary_rectangle(), reordered, max_gate_spacing=25.0
    )
    assert [list(c.coords) for c in first[0]] == [list(c.coords) for c in third[0]]
    assert first[1] == third[1]


def test_densify_gates_degenerate_boundary_returns_nothing() -> None:
    layout, _nodes = _layout_and_nodes()
    existing_gates = list(extract_block_gates(layout).gates)
    degenerate = Polygon([(0.0, 0.0), (0.0, 0.0), (0.0, 0.0)])

    connectors, new_gates = densify_gates(
        layout, degenerate, existing_gates, max_gate_spacing=1.0
    )
    assert connectors == []
    assert new_gates == []


def test_densify_gates_and_graph_fusion_reduces_components_and_adds_tjunction() -> None:
    """Hand-built integration check: feeding a densify_gates-style connector
    through snap_gates_to_macro + build_unified_street_graph actually fuses
    an orphaned local-street component onto the arterial and adds a
    T-junction -- the whole point of this slice.
    """
    arterial_lines = [LineString([(0.0, 10.0), (20.0, 10.0)])]
    edges = [MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=20.0)]
    ring_lines: list[LineString] = []
    boundary = Polygon([(-5.0, -5.0), (25.0, -5.0), (25.0, 25.0), (-5.0, 25.0)])

    # Block 0's existing gate plugs into the arterial at station 5.
    existing_gate = Gate(x=5.0, y=10.0, street_id=1, vertex_id=1)
    # Block 0 also has an orphaned local street with a free end at (15, 8) --
    # no gate ever reached the boundary above it, mirroring the real
    # under-served-boundary failure mode this slice fixes.
    orphan_street = LineString([(15.0, 8.0), (15.0, 5.0)])

    streets_by_block = {0: [LineString([(5.0, 10.0), (5.0, 5.0)]), orphan_street]}
    perimeter_flags_by_block = {0: [False, False]}
    gates_by_block = {0: [existing_gate]}

    junctions_before = snap_gates_to_macro(
        gates_by_block, arterial_lines, edges, ring_lines, boundary
    )
    g_before = build_unified_street_graph(
        arterial_lines,
        edges,
        ring_lines,
        junctions_before,
        streets_by_block,
        perimeter_flags_by_block,
    )
    summary_before = graph_connectivity_summary(g_before)
    assert summary_before["n_components"] == 2

    # A densify_gates-shaped connector: straight parcel-edge hop from the
    # boundary point directly above the orphan street's free end.
    connector = LineString([(15.0, 10.0), (15.0, 8.0)])
    connector_gate = Gate(x=15.0, y=10.0, street_id=-1, vertex_id=99)

    gates_by_block_after = {0: [existing_gate, connector_gate]}
    streets_by_block_after = {
        0: [LineString([(5.0, 10.0), (5.0, 5.0)]), orphan_street, connector]
    }
    perimeter_flags_by_block_after = {0: [False, False, False]}

    junctions_after = snap_gates_to_macro(
        gates_by_block_after, arterial_lines, edges, ring_lines, boundary
    )
    g_after = build_unified_street_graph(
        arterial_lines,
        edges,
        ring_lines,
        junctions_after,
        streets_by_block_after,
        perimeter_flags_by_block_after,
    )
    summary_after = graph_connectivity_summary(g_after)

    assert summary_after["n_components"] == 1
    assert (
        summary_after["n_tjunctions_realized"] > summary_before["n_tjunctions_realized"]
    )


# ---------------------------------------------------------------------------
# build_unified_street_graph -- F2 near-coincident local-street dedup
# ---------------------------------------------------------------------------
#
# ``street_perimeter_flags``/``_all_nodes_on_boundary`` is an all-or-nothing
# per-street classification (stage 1): if a single node's ``on_boundary``
# flag is wrong (numeric drift, or a merge that didn't update it), a street
# that is really the block's own boundary ring is classified "local" instead
# of "perimeter" and reaches ``build_unified_street_graph`` with its
# perimeter flag ``False``, exactly as these fixtures construct directly
# (this slice's fix lives downstream of stage 1, so the tests exercise it at
# that boundary rather than re-deriving a real ``on_boundary`` bug from a
# full Chen layout).


def test_build_unified_street_graph_dedups_misclassified_boundary_hugging_street() -> (
    None
):
    arterial_lines = [LineString([(0.0, 10.0), (20.0, 10.0)])]
    edges = [MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=20.0)]
    ring_lines: list[LineString] = []
    junctions: list[SeamJunction] = []

    # Boundary-hugging street misclassified as "local" (flag False): every
    # node sits on the arterial except one interior vertex nudged 0.02 off
    # the line -- well within DEFAULT_LOCAL_DEDUP_TOLERANCE (0.05) -- mirroring
    # numeric drift rather than a genuine interior street.
    streets_by_block = {
        0: [
            LineString(
                [(0.0, 10.0), (5.0, 10.0), (10.0, 10.02), (15.0, 10.0), (20.0, 10.0)]
            )
        ]
    }
    perimeter_flags_by_block = {0: [False]}

    g = build_unified_street_graph(
        arterial_lines,
        edges,
        ring_lines,
        junctions,
        streets_by_block,
        perimeter_flags_by_block,
    )

    # No doubled local edge chain tracing the arterial: the only edges are
    # the arterial's own (unsplit -- no junctions here) two endpoints.
    kinds = {data["kind"] for _u, _v, data in g.edges(data=True)}
    assert kinds == {"arterial"}
    assert g.number_of_edges() == 1
    assert g.number_of_nodes() == 2


def test_build_unified_street_graph_dedup_tolerance_zero_disables_check() -> None:
    # Same fixture as above, but dedup_tolerance=0.0 opts out: the misclassified
    # street is added in full, reproducing the doubled-road bug this slice
    # fixes -- demonstrates the parameter actually gates the behavior.
    arterial_lines = [LineString([(0.0, 10.0), (20.0, 10.0)])]
    edges = [MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=20.0)]
    ring_lines: list[LineString] = []
    junctions: list[SeamJunction] = []
    streets_by_block = {
        0: [
            LineString(
                [(0.0, 10.0), (5.0, 10.0), (10.0, 10.02), (15.0, 10.0), (20.0, 10.0)]
            )
        ]
    }
    perimeter_flags_by_block = {0: [False]}

    g = build_unified_street_graph(
        arterial_lines,
        edges,
        ring_lines,
        junctions,
        streets_by_block,
        perimeter_flags_by_block,
        dedup_tolerance=0.0,
    )

    kinds = {data["kind"] for _u, _v, data in g.edges(data=True)}
    assert kinds == {"arterial", "local"}
    assert g.number_of_edges() > 1


def test_build_unified_street_graph_dedup_is_per_segment_not_whole_street() -> None:
    # A street that is PART duplicate-of-arterial, part genuinely interior:
    # (0,10)->(5,10)->(10,10) hugs the arterial (dropped segment by segment),
    # (10,10)->(10,5) is a real perpendicular interior branch (kept). Neither
    # extreme (drop-nothing / drop-the-whole-street) is right here -- only
    # the hugging segments should go.
    arterial_lines = [LineString([(0.0, 10.0), (20.0, 10.0)])]
    edges = [MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=20.0)]
    ring_lines: list[LineString] = []
    junctions: list[SeamJunction] = []
    streets_by_block = {
        0: [LineString([(0.0, 10.0), (5.0, 10.0), (10.0, 10.0), (10.0, 5.0)])]
    }
    perimeter_flags_by_block = {0: [False]}

    g = build_unified_street_graph(
        arterial_lines,
        edges,
        ring_lines,
        junctions,
        streets_by_block,
        perimeter_flags_by_block,
    )

    node_5_10 = (5_000_000, 10_000_000)
    node_10_10 = (10_000_000, 10_000_000)
    node_10_5 = (10_000_000, 5_000_000)

    # (5,10) touched only dropped segments -- never enters the graph.
    assert node_5_10 not in g
    # (10,10) and (10,5) are the kept segment's endpoints.
    assert node_10_10 in g
    assert node_10_5 in g
    assert g.edges[node_10_10, node_10_5]["kind"] == "local"
    local_edges = [
        (u, v) for u, v, data in g.edges(data=True) if data["kind"] == "local"
    ]
    assert local_edges == [(node_10_10, node_10_5)] or local_edges == [
        (node_10_5, node_10_10)
    ]


def test_build_unified_street_graph_dedup_does_not_affect_genuine_local_streets() -> (
    None
):
    # Control: the existing gate-fixture local streets (which run away from
    # the arterial, not alongside it) must be unaffected by dedup being on by
    # default -- this just re-runs the existing gate/graph fixture explicitly
    # under the new default to guard against a regression in that direction.
    arterial_lines, edges, ring_lines, junctions, streets_by_block, flags = (
        _graph_fixture()
    )
    g = build_unified_street_graph(
        arterial_lines, edges, ring_lines, junctions, streets_by_block, flags
    )
    local_block_ids = {
        data["block_id"]
        for _u, _v, data in g.edges(data=True)
        if data["kind"] == "local"
    }
    assert local_block_ids == {0, 1}


def test_build_unified_street_graph_dedup_is_deterministic() -> None:
    arterial_lines = [LineString([(0.0, 10.0), (20.0, 10.0)])]
    edges = [MacroEdge(node_a=0, node_b=1, tau=2, tier=2, path_cost=1.0, length=20.0)]
    ring_lines: list[LineString] = []
    junctions: list[SeamJunction] = []
    streets_by_block = {
        0: [
            LineString(
                [(0.0, 10.0), (5.0, 10.0), (10.0, 10.02), (15.0, 10.0), (20.0, 10.0)]
            )
        ]
    }
    perimeter_flags_by_block = {0: [False]}

    g1 = build_unified_street_graph(
        arterial_lines,
        edges,
        ring_lines,
        junctions,
        streets_by_block,
        perimeter_flags_by_block,
    )
    g2 = build_unified_street_graph(
        arterial_lines,
        edges,
        ring_lines,
        junctions,
        streets_by_block,
        perimeter_flags_by_block,
    )
    assert sorted(g1.nodes) == sorted(g2.nodes)
    assert sorted(g1.edges) == sorted(g2.edges)
