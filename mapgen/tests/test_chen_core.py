from __future__ import annotations

import math

import pytest
from shapely import LineString, Polygon

from mapgen.chen_core import (
    CHEN_ACCESS_RATIO_TAU,
    build_chen_layout,
    chen_irregularity,
    chen_split_score,
    connected_component_count,
    corner_edge_path_points,
    edge_length,
    edge_path_length,
    evaluate_layout_invariants,
    interior_angle_ccw,
    normalized_edge,
    parcel_access_ratio_from_edges,
    parcel_access_ratio_from_path_geometry,
    parcel_mesh_from_polygons,
    polygon_street_access_ratio,
    signed_area,
    street_adjacency,
)


def test_chen_irregularity_matches_paper_shape_intuition() -> None:
    square = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    rectangle = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
    notched = Polygon(
        [(0, 0), (20, 0), (20, 10), (12, 10), (12, 6), (8, 6), (8, 10), (0, 10)]
    )

    assert math.isclose(chen_irregularity(square), 0.0, abs_tol=1e-12)
    assert chen_irregularity(rectangle) > 0.0
    assert chen_irregularity(notched) > chen_irregularity(rectangle)


def test_reflex_angles_are_preserved_for_concave_irregularity() -> None:
    assert math.isclose(
        interior_angle_ccw((12.0, 10.0), (12.0, 6.0), (8.0, 6.0)),
        1.5 * math.pi,
        abs_tol=1e-12,
    )


def test_chen_irregularity_matches_numeric_rectangle_case() -> None:
    rectangle = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])

    assert math.isclose(chen_irregularity(rectangle), 1.0 / 36.0, abs_tol=1e-12)


def test_chen_split_score_uses_access_bonus_and_default_weights() -> None:
    left = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    right = Polygon([(10, 0), (20, 0), (20, 10), (10, 10)])
    target_area = 100.0

    no_access = chen_split_score((left, right), [], target_area=target_area)
    with_access = chen_split_score(
        (left, right),
        [LineString([(0, 0), (20, 0)])],
        target_area=target_area,
    )

    assert math.isclose(no_access.q_size, 1.0, abs_tol=1e-12)
    assert math.isclose(no_access.q_regu, 1.0, abs_tol=1e-12)
    assert no_access.q_acce == 1.0
    assert with_access.q_acce == 2.0
    assert with_access.access_ratio_min >= CHEN_ACCESS_RATIO_TAU
    assert math.isclose(no_access.total, 1.0, abs_tol=1e-12)
    assert math.isclose(with_access.total, 1.2, abs_tol=1e-12)


def test_street_access_counts_collinear_overlap_only() -> None:
    square = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    subdivided_square = Polygon([(0, 0), (5, 0), (10, 0), (10, 10), (0, 10)])

    assert math.isclose(
        polygon_street_access_ratio(
            square,
            [LineString([(0, 0), (10, 0)])],
            tolerance=1e-7,
        ),
        1.0,
        abs_tol=1e-12,
    )
    assert math.isclose(
        polygon_street_access_ratio(
            subdivided_square,
            [LineString([(0, 0), (10, 0)])],
            tolerance=1e-7,
        ),
        1.0,
        abs_tol=1e-12,
    )
    assert math.isclose(
        polygon_street_access_ratio(
            square,
            [LineString([(5, -5), (5, 15)])],
            tolerance=1e-7,
        ),
        0.0,
        abs_tol=1e-12,
    )
    assert math.isclose(
        polygon_street_access_ratio(
            square,
            [LineString([(-10, 0), (0, 0)])],
            tolerance=1e-7,
        ),
        0.0,
        abs_tol=1e-12,
    )


def test_mesh_corner_graph_and_parcel_graph_share_edges() -> None:
    left = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    right = Polygon([(10, 0), (20, 0), (20, 10), (10, 10)])

    mesh = parcel_mesh_from_polygons([(1, left), (2, right)])
    layout = build_chen_layout(mesh, set())

    assert len(mesh.vertices) == 6
    assert len(layout.corner_graph.edges) == 7
    assert layout.parcel_graph.neighbors == {1: {2}, 2: {1}}

    shared = next(iter(layout.parcel_graph.shared_edges[(1, 2)]))
    shared_points = {mesh.vertices[node].point for node in shared}
    assert shared_points == {(10.0, 0.0), (10.0, 10.0)}


def test_corner_graph_collapses_valence_two_side_vertices() -> None:
    parcel = Polygon([(0, 0), (5, 0), (10, 0), (10, 10), (0, 10)])

    mesh = parcel_mesh_from_polygons([(1, parcel)])
    layout = build_chen_layout(mesh, set())
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }

    assert len(mesh.vertices) == 5
    assert point_to_node[(5.0, 0.0)] not in layout.corner_graph.vertices
    assert len(layout.corner_graph.vertices) == 4
    assert len(layout.corner_graph.edges) == 4
    assert layout.parcel_graph.parcel_corner_rings == {
        1: (
            point_to_node[(0.0, 0.0)],
            point_to_node[(10.0, 0.0)],
            point_to_node[(10.0, 10.0)],
            point_to_node[(0.0, 10.0)],
        )
    }


def test_curved_corner_edge_paths_preserve_intermediate_side_vertices() -> None:
    curved_points = [
        (0.0, 0.0),
        (10.0, 0.0),
        *[
            (10.0 + 2.5 * math.sin(math.pi * t / 8.0), 10.0 * t / 8.0)
            for t in range(1, 8)
        ],
        (10.0, 10.0),
        (0.0, 10.0),
    ]
    parcel = Polygon(curved_points)

    mesh = parcel_mesh_from_polygons([(1, parcel)])
    layout = build_chen_layout(mesh, set())
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }
    bottom_right = point_to_node[(10.0, 0.0)]
    top_right = point_to_node[(10.0, 10.0)]
    curved_edge = normalized_edge(bottom_right, top_right)

    path = layout.corner_graph.edge_paths[curved_edge]
    path_points = corner_edge_path_points(mesh, layout.corner_graph, curved_edge)
    path_length = edge_path_length(mesh, layout.corner_graph, curved_edge)
    chord_length = edge_length(mesh, curved_edge)

    assert len(mesh.vertices) == len(curved_points)
    assert len(layout.corner_graph.vertices) == 4
    assert len(path) == 9
    assert path_points[0] == (10.0, 0.0)
    assert path_points[-1] == (10.0, 10.0)
    assert path_length > chord_length

    access_ratio = parcel_access_ratio_from_edges(
        mesh.parcels[1],
        mesh,
        {curved_edge},
        layout.corner_graph.parcel_corner_rings[1],
    )
    expected = path_length / ((10.0 + path_length + 10.0 + 10.0) / 4.0)

    assert math.isclose(access_ratio, expected, rel_tol=1e-12)
    assert access_ratio > 1.0


def test_path_geometry_access_counts_curved_access_frontage() -> None:
    curved_points = [
        (0.0, 0.0),
        (10.0, 0.0),
        *[
            (10.0 + 2.5 * math.sin(math.pi * t / 8.0), 10.0 * t / 8.0)
            for t in range(1, 8)
        ],
        (10.0, 10.0),
        (0.0, 10.0),
    ]
    parcel = Polygon(curved_points)
    mesh = parcel_mesh_from_polygons([(1, parcel)])
    layout = build_chen_layout(mesh, set())
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }
    curved_edge = normalized_edge(
        point_to_node[(10.0, 0.0)], point_to_node[(10.0, 10.0)]
    )
    curved_access_line = LineString(
        corner_edge_path_points(mesh, layout.corner_graph, curved_edge)
    )

    path_length = edge_path_length(mesh, layout.corner_graph, curved_edge)
    chord_length = edge_length(mesh, curved_edge)
    path_ratio = parcel_access_ratio_from_path_geometry(
        mesh.parcels[1],
        mesh,
        layout.corner_graph,
        [curved_access_line],
        corner_ring=layout.corner_graph.parcel_corner_rings[1],
        tolerance=1e-7,
    )
    expected_path_ratio = path_length / ((10.0 + path_length + 10.0 + 10.0) / 4.0)
    chord_ratio = chord_length / ((10.0 + chord_length + 10.0 + 10.0) / 4.0)

    assert math.isclose(path_ratio, expected_path_ratio, rel_tol=1e-12)
    assert path_ratio > chord_ratio
    assert (
        polygon_street_access_ratio(
            parcel,
            [curved_access_line],
            tolerance=1e-7,
        )
        < path_ratio
    )


def test_rectangular_corner_edge_paths_keep_straight_side_behavior() -> None:
    parcel = Polygon([(0, 0), (5, 0), (10, 0), (10, 10), (0, 10)])

    mesh = parcel_mesh_from_polygons([(1, parcel)])
    layout = build_chen_layout(mesh, set())
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }
    bottom_edge = normalized_edge(point_to_node[(0.0, 0.0)], point_to_node[(10.0, 0.0)])

    assert layout.corner_graph.edge_paths[bottom_edge] == (
        point_to_node[(0.0, 0.0)],
        point_to_node[(5.0, 0.0)],
        point_to_node[(10.0, 0.0)],
    )
    assert math.isclose(
        edge_path_length(mesh, layout.corner_graph, bottom_edge),
        edge_length(mesh, bottom_edge),
        abs_tol=1e-12,
    )
    assert math.isclose(
        parcel_access_ratio_from_edges(
            mesh.parcels[1],
            mesh,
            {bottom_edge},
            layout.corner_graph.parcel_corner_rings[1],
        ),
        1.0,
        abs_tol=1e-12,
    )


def test_parcel_corner_rings_are_counter_clockwise() -> None:
    clockwise = Polygon([(0, 0), (0, 10), (10, 10), (10, 0)])

    layout = build_chen_layout(parcel_mesh_from_polygons([(1, clockwise)]), set())
    ring_points = [
        layout.mesh.vertices[node].point
        for node in layout.parcel_graph.parcel_corner_rings[1]
    ]

    assert signed_area(ring_points) > 0.0


def test_nonconforming_t_junction_boundaries_are_rejected() -> None:
    large = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    lower = Polygon([(10, 0), (20, 0), (20, 5), (10, 5)])
    upper = Polygon([(10, 5), (20, 5), (20, 10), (10, 10)])

    mesh = parcel_mesh_from_polygons([(1, large), (2, lower), (3, upper)])

    with pytest.raises(ValueError, match="non-conforming"):
        build_chen_layout(mesh, set())


def test_street_network_decomposition_splits_at_junctions() -> None:
    left = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    right = Polygon([(10, 0), (20, 0), (20, 10), (10, 10)])
    mesh = parcel_mesh_from_polygons([(1, left), (2, right)])
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }

    bottom_left = point_to_node[(0.0, 0.0)]
    bottom_mid = point_to_node[(10.0, 0.0)]
    bottom_right = point_to_node[(20.0, 0.0)]
    top_mid = point_to_node[(10.0, 10.0)]
    street_edges = {
        normalized_edge(bottom_left, bottom_mid),
        normalized_edge(bottom_mid, bottom_right),
        normalized_edge(bottom_mid, top_mid),
    }

    layout = build_chen_layout(mesh, street_edges)

    assert connected_component_count(layout.street_network) == 1
    assert street_adjacency(layout.street_network)[bottom_mid] == {
        bottom_left,
        bottom_right,
        top_mid,
    }
    assert len(layout.street_graph.streets) == 2
    assert layout.street_graph.junctions == {bottom_mid: {1, 2}}


def test_invariant_report_separates_paper_and_geometry_passes() -> None:
    left = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    right = Polygon([(10, 0), (20, 0), (20, 10), (10, 10)])
    boundary = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
    mesh = parcel_mesh_from_polygons([(1, left), (2, right)], boundary=boundary)
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }

    bottom_left = point_to_node[(0.0, 0.0)]
    bottom_mid = point_to_node[(10.0, 0.0)]
    bottom_right = point_to_node[(20.0, 0.0)]
    top_right = point_to_node[(20.0, 10.0)]
    top_mid = point_to_node[(10.0, 10.0)]
    top_left = point_to_node[(0.0, 10.0)]
    street_edges = {
        normalized_edge(bottom_left, bottom_mid),
        normalized_edge(bottom_mid, bottom_right),
        normalized_edge(bottom_right, top_right),
        normalized_edge(top_right, top_mid),
        normalized_edge(top_mid, top_left),
        normalized_edge(top_left, bottom_left),
    }

    passing = evaluate_layout_invariants(
        build_chen_layout(mesh, street_edges),
        target_boundary=boundary,
    )
    assert passing.paper_invariant_pass
    assert passing.geometry_valid_pass
    assert passing.chen_formula_pass
    assert passing.diagnostic_metric_pass
    assert passing.metrics["street_graph_component_count"] == 1
    assert passing.metrics["parcel_access_ratio_below_tau_count"] == 0
    assert math.isclose(passing.metrics["coverage_rate"], 1.0, abs_tol=1e-12)

    disconnected = evaluate_layout_invariants(
        build_chen_layout(mesh, {normalized_edge(bottom_left, bottom_mid)}),
        target_boundary=boundary,
    )
    assert not disconnected.paper_invariant_pass
    assert disconnected.geometry_valid_pass
    assert disconnected.metrics["unreachable_parcel_count"] == 1


def test_paper_reachability_is_not_qacce_tau_threshold() -> None:
    parcel = Polygon([(0, 0), (100, 0), (100, 10), (0, 10)])
    mesh = parcel_mesh_from_polygons([(1, parcel)])
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }
    short_street = {
        normalized_edge(point_to_node[(100.0, 0.0)], point_to_node[(100.0, 10.0)])
    }

    report = evaluate_layout_invariants(build_chen_layout(mesh, short_street))

    assert report.paper_invariant_pass
    assert report.metrics["parcel_access_ratio_min"] < CHEN_ACCESS_RATIO_TAU
    assert report.metrics["parcel_access_ratio_below_tau_count"] == 1
    assert report.metrics["unreachable_parcel_count"] == 0


def test_boundary_coverage_rejects_spillover_area() -> None:
    boundary = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    oversize = Polygon([(0, 0), (11, 0), (11, 10), (0, 10)])
    mesh = parcel_mesh_from_polygons([(1, oversize)], boundary=boundary)
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in mesh.vertices.values()
    }
    street_edges = {
        normalized_edge(point_to_node[(0.0, 0.0)], point_to_node[(11.0, 0.0)]),
        normalized_edge(point_to_node[(11.0, 0.0)], point_to_node[(11.0, 10.0)]),
        normalized_edge(point_to_node[(11.0, 10.0)], point_to_node[(0.0, 10.0)]),
        normalized_edge(point_to_node[(0.0, 10.0)], point_to_node[(0.0, 0.0)]),
    }

    report = evaluate_layout_invariants(
        build_chen_layout(mesh, street_edges),
        target_boundary=boundary,
    )

    assert not report.geometry_valid_pass
    assert math.isclose(report.metrics["coverage_rate"], 1.0, abs_tol=1e-12)
    assert report.metrics["coverage_spillover_rate"] > 0.0
