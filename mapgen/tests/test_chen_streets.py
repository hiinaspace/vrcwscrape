from __future__ import annotations

import math

from shapely import Polygon

from mapgen.chen_core import (
    build_chen_layout,
    connected_component_count,
    evaluate_layout_invariants,
    normalized_edge,
    parcel_mesh_from_polygons,
)
from mapgen.chen_streets import (
    StreetConfig,
    _included_angle,
    _shortest_path_between_node_sets,
    _street_context,
    boundary_ring_seed_edges,
    extend_street_network,
)


def _grid_mesh(cols: int, rows: int):
    boundary = Polygon([(0, 0), (cols, 0), (cols, rows), (0, rows)])
    polygons = []
    for y in range(rows):
        for x in range(cols):
            parcel_id = y * cols + x + 1
            polygons.append(
                (
                    parcel_id,
                    Polygon(
                        [
                            (float(x), float(y)),
                            (float(x + 1), float(y)),
                            (float(x + 1), float(y + 1)),
                            (float(x), float(y + 1)),
                        ]
                    ),
                )
            )
    return parcel_mesh_from_polygons(polygons, boundary=boundary), boundary


def _point_ids(mesh) -> dict[tuple[float, float], int]:
    return {vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()}


def _edges_for_coords(
    points: dict[tuple[float, float], int],
    coords: list[tuple[float, float]],
):
    return {
        normalized_edge(points[start], points[end])
        for start, end in zip(coords, coords[1:], strict=False)
    }


# --------------------------------------------------------------------------
# Step 1/2: unreachable groups and I/L access selection.
# --------------------------------------------------------------------------


def test_3x3_center_parcel_is_repaired_to_full_reachability() -> None:
    mesh, boundary = _grid_mesh(3, 3)
    seed = boundary_ring_seed_edges(mesh)

    result = extend_street_network(mesh, set(seed))
    street_edges = set(seed) | result.added_edges
    layout = build_chen_layout(mesh, street_edges)
    report = evaluate_layout_invariants(layout, target_boundary=boundary)

    assert result.diagnostics["group_count"] == 1
    assert result.diagnostics["unreachable_parcel_count_after"] == 0
    assert result.diagnostics["street_graph_component_count"] == 1
    assert result.diagnostics["street_network_subset_of_corner_graph"]
    assert connected_component_count(layout.street_network) == 1
    assert report.paper_invariant_pass


def test_center_parcel_repair_prefers_i_shaped_access() -> None:
    mesh, _boundary = _grid_mesh(3, 3)
    seed = boundary_ring_seed_edges(mesh)

    result = extend_street_network(mesh, set(seed))

    # A single I-shaped access edge reaches the center parcel; it must be chosen
    # over any L-shaped access (which would add a junction).
    assert result.diagnostics["i_chosen_count"] == 1
    assert result.diagnostics["l_chosen_count"] == 0
    chosen = result.selected_access_candidates[0]
    assert chosen.kind == "I"
    assert len(chosen.edges) == 1

    center_reach = frozenset({5})
    i_lengths = [
        c.length
        for c in result.evaluated_candidates
        if c.kind == "I" and c.reached_parcels == center_reach
    ]
    l_lengths = [
        c.length
        for c in result.evaluated_candidates
        if c.kind == "L" and c.reached_parcels == center_reach
    ]
    assert i_lengths
    assert l_lengths
    assert min(i_lengths) < min(l_lengths)


# --------------------------------------------------------------------------
# Step 3: Dijkstra connection to the existing network.
# --------------------------------------------------------------------------


def test_access_is_connected_to_seed_network_by_dijkstra() -> None:
    mesh, _boundary = _grid_mesh(3, 3)
    seed = boundary_ring_seed_edges(mesh)

    result = extend_street_network(mesh, set(seed))
    connection_edges = {
        edge
        for candidate in result.selected_access_candidates
        for edge in candidate.connection_edges
    }

    assert result.diagnostics["dijkstra_connection_count"] == 1
    assert connection_edges
    assert connection_edges <= result.added_edges
    assert connection_edges.isdisjoint(seed)
    assert result.diagnostics["street_graph_component_count"] == 1


def _count_turns(
    ctx, path: tuple[int, ...], threshold: float = math.radians(135.0)
) -> int:
    turns = 0
    for prev, node, nxt in zip(path, path[1:], path[2:], strict=False):
        if _included_angle(ctx, prev, node, nxt) < threshold:
            turns += 1
    return turns


def test_dijkstra_weight_prefers_fewer_junction_turns_over_equal_length_path() -> None:
    # 2x2 corner graph. Routing from the bottom-left corner to the top-right
    # corner has two equal-length (4-unit) routes:
    #   * through the centre: 3 interior turns
    #   * along the L-perimeter: a single turn at the corner
    # A dominant junction weight must pick the perimeter route (fewer junctions),
    # while a zero junction weight is free to cut through the centre.
    mesh, _boundary = _grid_mesh(2, 2)
    ctx = _street_context(mesh)
    points = _point_ids(mesh)
    start = {points[(0.0, 0.0)]}
    target = {points[(2.0, 2.0)]}

    straight_path, _c1 = _shortest_path_between_node_sets(
        ctx, start, target, StreetConfig(junction_weight=100.0)
    )
    cheap_path, _c2 = _shortest_path_between_node_sets(
        ctx, start, target, StreetConfig(junction_weight=0.0)
    )

    # Same geometric length, but the junction-penalised route has fewer turns.
    assert _count_turns(ctx, straight_path) < _count_turns(ctx, cheap_path)
    assert _count_turns(ctx, straight_path) == 1


# --------------------------------------------------------------------------
# Group recursion (paper Fig. 9 case #5).
# --------------------------------------------------------------------------


def _serpentine_polygons():
    # An S-shaped (serpentine) chain of seven unit parcels: a bottom row, a
    # one-parcel vertical connector, then a top row running back. The chain needs
    # two bends, so no single I- (straight) or L- (one bend) access can reach all
    # seven parcels, forcing the paper's recurse-on-remainder branch (Fig. 9
    # case #5). Parcel 8 below the start carries the seed edge.
    boundary = Polygon([(0, -1), (3, -1), (3, 3), (0, 3)])
    polygons = [
        (1, Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])),
        (2, Polygon([(1, 0), (2, 0), (2, 1), (1, 1)])),
        (3, Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])),
        (4, Polygon([(2, 1), (3, 1), (3, 2), (2, 2)])),
        (5, Polygon([(2, 2), (3, 2), (3, 3), (2, 3)])),
        (6, Polygon([(1, 2), (2, 2), (2, 3), (1, 3)])),
        (7, Polygon([(0, 2), (1, 2), (1, 3), (0, 3)])),
        (8, Polygon([(0, -1), (1, -1), (1, 0), (0, 0)])),
    ]
    return parcel_mesh_from_polygons(polygons, boundary=boundary), boundary


def test_group_recursion_makes_all_parcels_reachable_with_multiple_accesses() -> None:
    mesh, boundary = _serpentine_polygons()
    points = _point_ids(mesh)
    # Seed only the outer edge of parcel 8, leaving the serpentine chain
    # (parcels 1..7) as one unreachable group with no complete single access.
    seed = {normalized_edge(points[(0.0, -1.0)], points[(1.0, -1.0)])}

    result = extend_street_network(mesh, set(seed))
    street_edges = set(seed) | result.added_edges
    layout = build_chen_layout(mesh, street_edges)
    report = evaluate_layout_invariants(layout, target_boundary=boundary)

    assert result.diagnostics["unreachable_parcel_count_after"] == 0
    # No single access could reach every parcel, so the recurse-on-remainder
    # branch must have fired at least once.
    assert result.diagnostics["recursed_count"] >= 1
    assert len(result.selected_access_candidates) >= 2
    assert result.diagnostics["street_graph_component_count"] == 1
    assert report.paper_invariant_pass


# --------------------------------------------------------------------------
# Determinism and per-level incrementality.
# --------------------------------------------------------------------------


def test_extension_tie_breaking_is_deterministic() -> None:
    mesh, _boundary = _grid_mesh(3, 3)
    seed = boundary_ring_seed_edges(mesh)

    first = extend_street_network(mesh, set(seed))
    second = extend_street_network(mesh, set(seed))

    assert first.added_edges == second.added_edges
    assert first.diagnostics == second.diagnostics
    assert [
        (c.kind, tuple(sorted(c.edges)), tuple(sorted(c.connection_edges)))
        for c in first.selected_access_candidates
    ] == [
        (c.kind, tuple(sorted(c.edges)), tuple(sorted(c.connection_edges)))
        for c in second.selected_access_candidates
    ]


def test_extension_is_noop_when_all_parcels_already_reachable() -> None:
    mesh, _boundary = _grid_mesh(3, 3)
    seed = boundary_ring_seed_edges(mesh)

    first = extend_street_network(mesh, set(seed))
    grown = set(seed) | first.added_edges

    # Calling again with the grown edge set must add nothing.
    second = extend_street_network(mesh, grown)

    assert second.added_edges == set()
    assert second.diagnostics["group_count"] == 0
    assert second.diagnostics["unreachable_parcel_count_after"] == 0
    assert second.diagnostics["i_chosen_count"] == 0
    assert second.diagnostics["l_chosen_count"] == 0


def test_unknown_existing_edges_are_rejected() -> None:
    mesh, _boundary = _grid_mesh(2, 2)

    bogus = {(10_000, 10_001)}
    try:
        extend_street_network(mesh, bogus)
    except ValueError as exc:
        assert "not in the parcel corner graph" in str(exc)
    else:  # pragma: no cover - guard
        raise AssertionError("expected ValueError for unknown edges")


# --------------------------------------------------------------------------
# Step 4: optional cul-de-sac avoidance.
# --------------------------------------------------------------------------


def test_cul_de_sac_avoidance_repairs_street_ends() -> None:
    mesh, _boundary = _grid_mesh(3, 3)
    seed = boundary_ring_seed_edges(mesh)

    baseline = extend_street_network(
        mesh, set(seed), StreetConfig(avoid_cul_de_sacs=False)
    )
    repaired = extend_street_network(
        mesh, set(seed), StreetConfig(avoid_cul_de_sacs=True)
    )

    def _dead_ends(edges: set) -> int:
        adjacency: dict[int, set[int]] = {}
        for a, b in edges:
            adjacency.setdefault(a, set()).add(b)
            adjacency.setdefault(b, set()).add(a)
        return sum(1 for n in adjacency.values() if len(n) == 1)

    baseline_edges = set(seed) | baseline.added_edges
    repaired_edges = set(seed) | repaired.added_edges

    assert _dead_ends(baseline_edges) >= 1
    assert repaired.diagnostics["cul_de_sacs_repaired_count"] >= 1
    assert _dead_ends(repaired_edges) < _dead_ends(baseline_edges)
    assert repaired.diagnostics["unreachable_parcel_count_after"] == 0
    assert repaired.diagnostics["street_graph_component_count"] == 1


def test_cul_de_sac_avoidance_default_is_off() -> None:
    assert StreetConfig().avoid_cul_de_sacs is False


def test_collinear_threshold_is_135_degrees() -> None:
    # Sanity: the default collinear threshold is the paper's 135 degrees.
    assert math.isclose(
        StreetConfig().collinear_angle_threshold_rad, math.radians(135.0)
    )
