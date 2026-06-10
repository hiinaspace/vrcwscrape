from __future__ import annotations

from shapely import Polygon

from mapgen.chen_core import (
    build_chen_layout,
    connected_component_count,
    evaluate_layout_invariants,
    normalized_edge,
    parcel_mesh_from_polygons,
)
from mapgen.chen_streets import StreetGenerationConfig, generate_street_network


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


def _perimeter_seed(points: dict[tuple[float, float], int], cols: int, rows: int):
    coords = (
        [(float(x), 0.0) for x in range(cols + 1)]
        + [(float(cols), float(y)) for y in range(1, rows + 1)]
        + [(float(x), float(rows)) for x in range(cols - 1, -1, -1)]
        + [(0.0, float(y)) for y in range(rows - 1, 0, -1)]
        + [(0.0, 0.0)]
    )
    return _edges_for_coords(points, coords)


def test_3x3_perimeter_seed_repairs_center_parcel() -> None:
    mesh, boundary = _grid_mesh(3, 3)

    result = generate_street_network(mesh)
    layout = build_chen_layout(mesh, set(result.street_edges))
    report = evaluate_layout_invariants(layout, target_boundary=boundary)

    assert result.diagnostics["unreachable_parcel_ids_before"] == (5,)
    assert result.diagnostics["reachable_parcel_count_before"] == 8
    assert result.diagnostics["unreachable_parcel_count_after"] == 0
    assert result.diagnostics["reachable_parcel_count_after"] == 9
    assert result.diagnostics["street_graph_component_count"] == 1
    assert connected_component_count(layout.street_network) == 1
    assert report.paper_invariant_pass


def test_i_shaped_candidate_is_selected_over_longer_l_candidate() -> None:
    mesh, _boundary = _grid_mesh(3, 3)

    result = generate_street_network(mesh)
    first = result.selected_access_candidates[0]
    center_reach = frozenset({5})
    i_lengths = [
        candidate.length
        for candidate in result.evaluated_candidates
        if candidate.kind == "I" and candidate.reached_parcels == center_reach
    ]
    l_lengths = [
        candidate.length
        for candidate in result.evaluated_candidates
        if candidate.kind == "L" and candidate.reached_parcels == center_reach
    ]

    assert result.diagnostics["i_candidate_count"] >= 4
    assert result.diagnostics["l_candidate_count"] >= 4
    assert first.kind == "I"
    assert len(first.edges) == 1
    assert min(i_lengths) < min(l_lengths)


def test_access_candidate_is_connected_to_seed_network_by_dijkstra() -> None:
    mesh, _boundary = _grid_mesh(3, 3)

    result = generate_street_network(mesh)
    connection_edges = {
        edge
        for candidate in result.selected_access_candidates
        for edge in candidate.connection_edges
    }

    assert result.diagnostics["dijkstra_connection_count"] == 1
    assert connection_edges
    assert connection_edges <= result.street_edges
    assert connection_edges.isdisjoint(result.seed_edges)
    assert result.diagnostics["street_graph_component_count"] == 1


def test_default_square_grid_selection_adds_four_way_and_stays_sparse() -> None:
    mesh, boundary = _grid_mesh(3, 3)

    result = generate_street_network(mesh)
    layout = build_chen_layout(mesh, set(result.street_edges))
    report = evaluate_layout_invariants(layout, target_boundary=boundary)

    assert (
        result.diagnostics["street_selection_strategy"]
        == "section_4_2_connected_junctions"
    )
    assert result.diagnostics["junction_completion_applied"]
    assert result.diagnostics["junction_completion_added_edge_count"] > 0
    assert result.diagnostics["junction_completion_parcel_touch_count"] > 0
    assert result.diagnostics["street_four_way_intersection_count"] >= 1
    assert result.diagnostics["unreachable_parcel_count_after"] == 0
    assert result.diagnostics["street_graph_component_count"] == 1
    assert len(result.street_edges) < len(mesh.edges)
    assert report.paper_invariant_pass


def test_generation_tie_breaking_is_deterministic() -> None:
    mesh, _boundary = _grid_mesh(3, 3)

    first = generate_street_network(mesh)
    second = generate_street_network(mesh)

    assert first.street_edges == second.street_edges
    assert first.diagnostics == second.diagnostics
    assert [
        (
            candidate.kind,
            tuple(sorted(candidate.edges)),
            tuple(sorted(candidate.connection_edges)),
        )
        for candidate in first.selected_access_candidates
    ] == [
        (
            candidate.kind,
            tuple(sorted(candidate.edges)),
            tuple(sorted(candidate.connection_edges)),
        )
        for candidate in second.selected_access_candidates
    ]


def test_explicit_seed_edges_are_validated_against_corner_graph() -> None:
    mesh, _boundary = _grid_mesh(2, 2)
    points = _point_ids(mesh)
    seed = {normalized_edge(points[(0.0, 0.0)], points[(1.0, 0.0)])}

    result = generate_street_network(mesh, seed_edges=seed)

    assert result.seed_edges == frozenset(seed)


def test_junction_completion_adds_bounded_four_way_grid_connection() -> None:
    mesh, boundary = _grid_mesh(4, 4)
    points = _point_ids(mesh)
    seed = _perimeter_seed(points, 4, 4)
    seed.update(
        _edges_for_coords(
            points,
            [(2.0, float(y)) for y in range(5)],
        )
    )
    seed.update(
        _edges_for_coords(
            points,
            [(0.0, 2.0), (1.0, 2.0), (2.0, 2.0)],
        )
    )

    baseline = generate_street_network(
        mesh,
        seed_edges=seed,
        config=StreetGenerationConfig(complete_junctions=False),
    )
    completed = generate_street_network(
        mesh,
        seed_edges=seed,
        config=StreetGenerationConfig(
            complete_junctions=True,
            junction_completion_max_added_edges=2,
            junction_completion_max_added_edge_ratio=1.0,
        ),
    )
    completed_layout = build_chen_layout(mesh, set(completed.street_edges))
    report = evaluate_layout_invariants(completed_layout, target_boundary=boundary)

    assert baseline.diagnostics["street_four_way_intersection_count"] == 0
    assert completed.diagnostics["junction_completion_applied"]
    assert completed.diagnostics["junction_completion_added_edge_count"] == 2
    assert (
        completed.diagnostics["street_four_way_intersection_count"]
        > baseline.diagnostics["street_four_way_intersection_count"]
    )
    assert completed.diagnostics["unreachable_parcel_count_after"] == 0
    assert completed.diagnostics["street_graph_component_count"] == 1
    assert report.paper_invariant_pass


def test_cul_de_sac_pruning_removes_dispensable_leaf_edge() -> None:
    mesh, _boundary = _grid_mesh(2, 2)
    points = _point_ids(mesh)
    seed = _perimeter_seed(points, 2, 2)
    seed.add(normalized_edge(points[(1.0, 0.0)], points[(1.0, 1.0)]))

    unpruned = generate_street_network(
        mesh,
        seed_edges=seed,
        config=StreetGenerationConfig(
            complete_junctions=False,
            avoid_cul_de_sacs=False,
        ),
    )
    pruned = generate_street_network(
        mesh,
        seed_edges=seed,
        config=StreetGenerationConfig(
            complete_junctions=False,
            avoid_cul_de_sacs=True,
        ),
    )

    assert unpruned.diagnostics["cul_de_sac_count"] == 1
    assert pruned.diagnostics["cul_de_sac_avoidance_applied"]
    assert pruned.diagnostics["cul_de_sac_avoidance_changed_network"]
    assert pruned.diagnostics["cul_de_sac_pruned_edge_count"] == 1
    assert pruned.diagnostics["cul_de_sac_repair_success_count"] == 0
    assert pruned.diagnostics["cul_de_sac_count"] == 0
    assert pruned.diagnostics["reachable_parcel_count_after"] == 4
    assert pruned.diagnostics["street_graph_component_count"] == 1


def test_cul_de_sac_avoidance_repairs_leaf_when_pruning_would_remove_four_way() -> None:
    mesh, _boundary = _grid_mesh(3, 3)

    baseline = generate_street_network(mesh)
    repaired = generate_street_network(
        mesh,
        config=StreetGenerationConfig(avoid_cul_de_sacs=True),
    )

    assert baseline.diagnostics["street_four_way_intersection_count"] >= 1
    assert baseline.diagnostics["cul_de_sac_count"] == 1
    assert repaired.diagnostics["cul_de_sac_avoidance_applied"]
    assert repaired.diagnostics["cul_de_sac_avoidance_changed_network"]
    assert repaired.diagnostics["cul_de_sac_pruned_edge_count"] == 0
    assert repaired.diagnostics["cul_de_sac_repair_attempt_count"] >= 1
    assert repaired.diagnostics["cul_de_sac_repair_candidate_count"] >= 1
    assert repaired.diagnostics["cul_de_sac_repair_success_count"] == 1
    assert repaired.diagnostics["cul_de_sac_repair_added_edge_count"] == 1
    assert repaired.diagnostics["cul_de_sac_count"] == 0
    assert (
        repaired.diagnostics["street_four_way_intersection_count"]
        >= baseline.diagnostics["street_four_way_intersection_count"]
    )
    assert repaired.diagnostics["unreachable_parcel_count_after"] == 0
    assert repaired.diagnostics["street_graph_component_count"] == 1
    assert len(repaired.street_edges) == len(baseline.street_edges) + 1
