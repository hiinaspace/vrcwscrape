from __future__ import annotations

import math

import pytest
from shapely import LineString, Polygon

from mapgen.chen_core import (
    CHEN_ACCESS_RATIO_TAU,
    ChenLayout,
    ChenShortEdgeCandidate,
    MeshVertex,
    ParcelCornerGraph,
    ParcelGraph,
    ParcelMesh,
    StreetGraph,
    StreetNetworkGraph,
    _strict_polygon_from_points,
    _validate_fig7_candidate_layout,
    apply_chen_fig7_short_edge_cleanup,
    build_chen_layout,
    chen_fig7_short_edge_candidates,
    chen_fig7_short_edge_diagnostics,
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
from mapgen.chen_generate import STREAMLINE_MODE_YANG_B_FIELD, generate_named_layout


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


def test_chen_fig7_diagnostics_classify_t_junctions_on_full_short_edge_path() -> None:
    vertices = {
        0: MeshVertex(0, (0.0, 0.0)),
        1: MeshVertex(1, (1.0, 0.0)),
        2: MeshVertex(2, (2.0, 0.0)),
        3: MeshVertex(3, (1.0, 1.0)),
        4: MeshVertex(4, (1.0, -1.0)),
        5: MeshVertex(5, (0.0, 1.0)),
        6: MeshVertex(6, (8.0, 0.0)),
        7: MeshVertex(7, (8.0, 1.0)),
        8: MeshVertex(8, (8.0, -1.0)),
        9: MeshVertex(9, (9.0, 0.0)),
    }
    short_edge = normalized_edge(0, 2)
    layout = ChenLayout(
        mesh=ParcelMesh(vertices=vertices, parcels={}),
        corner_graph=ParcelCornerGraph(
            vertices=vertices,
            edges={
                short_edge,
                normalized_edge(1, 3),
                normalized_edge(1, 4),
                normalized_edge(1, 5),
                normalized_edge(6, 7),
                normalized_edge(6, 8),
                normalized_edge(6, 9),
            },
            edge_paths={short_edge: (0, 1, 2)},
            parcel_corner_rings={},
            parcel_approx_points={},
        ),
        parcel_graph=ParcelGraph(
            neighbors={1: {2}, 2: {1}},
            shared_edges={(1, 2): {short_edge}},
            edge_paths={short_edge: (0, 1, 2)},
            parcel_corner_rings={},
            parcel_approx_points={
                1: ((0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)),
                2: ((0.0, 0.0), (20.0, 0.0), (20.0, -20.0), (0.0, -20.0)),
            },
        ),
        street_network=StreetNetworkGraph(edges=set()),
        street_graph=StreetGraph(streets=(), junctions={}),
    )

    diagnostics = chen_fig7_short_edge_diagnostics(layout)

    assert diagnostics["chen_fig7_short_edge_cleanup_stage"] == "diagnostic_only_v0"
    assert diagnostics["chen_fig7_short_edge_cleanup_applied"] is False
    assert diagnostics["chen_fig7_short_shared_edge_candidate_count"] == 1
    assert diagnostics["chen_fig7_raw_interior_t_junction_count"] == 2
    assert diagnostics["chen_fig7_short_edge_attached_interior_t_junction_count"] == 1
    assert diagnostics["chen_fig7_unexplained_interior_t_junction_count"] == 1
    assert (
        diagnostics[
            "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count"
        ]
        == 1
    )
    assert (
        diagnostics["chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count"]
        == 0
    )
    assert diagnostics[
        "chen_fig7_short_edge_attached_interior_t_junction_points_sample"
    ] == ((1.0, 0.0),)
    assert diagnostics["chen_fig7_unexplained_interior_t_junction_points_sample"] == (
        (8.0, 0.0),
    )
    assert diagnostics[
        "chen_fig7_unexplained_straight_through_side_insertion_t_junction_points_sample"
    ] == ((8.0, 0.0),)
    assert (
        diagnostics[
            "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_points_sample"
        ]
        == ()
    )
    sample = diagnostics["chen_fig7_short_shared_edge_samples"][0]
    assert sample["edge"] == short_edge
    assert sample["path_points_sample"] == ((0.0, 0.0), (1.0, 0.0), (2.0, 0.0))


def test_chen_fig7_diagnostics_split_unexplained_t_junction_collinearity() -> None:
    vertices = {
        0: MeshVertex(0, (0.0, 0.0)),
        1: MeshVertex(1, (-1.0, 0.0)),
        2: MeshVertex(2, (1.0, 0.0)),
        3: MeshVertex(3, (0.0, 1.0)),
        10: MeshVertex(10, (5.0, 0.0)),
        11: MeshVertex(11, (6.0, 0.0)),
        12: MeshVertex(12, (4.5, 0.866025404)),
        13: MeshVertex(13, (4.5, -0.866025404)),
    }
    layout = ChenLayout(
        mesh=ParcelMesh(vertices=vertices, parcels={}),
        corner_graph=ParcelCornerGraph(
            vertices=vertices,
            edges={
                normalized_edge(0, 1),
                normalized_edge(0, 2),
                normalized_edge(0, 3),
                normalized_edge(10, 11),
                normalized_edge(10, 12),
                normalized_edge(10, 13),
            },
            edge_paths={},
            parcel_corner_rings={},
            parcel_approx_points={},
        ),
        parcel_graph=ParcelGraph(
            neighbors={},
            shared_edges={},
            edge_paths={},
            parcel_corner_rings={},
            parcel_approx_points={},
        ),
        street_network=StreetNetworkGraph(edges=set()),
        street_graph=StreetGraph(streets=(), junctions={}),
    )

    diagnostics = chen_fig7_short_edge_diagnostics(layout)

    assert diagnostics["chen_fig7_short_shared_edge_candidate_count"] == 0
    assert diagnostics["chen_fig7_raw_interior_t_junction_count"] == 2
    assert diagnostics["chen_fig7_unexplained_interior_t_junction_count"] == 2
    assert (
        diagnostics["chen_fig7_unexplained_t_junction_classification_stage"]
        == "chen_section_4_parcel_corner_collinearity_135deg_v0"
    )
    assert (
        diagnostics[
            "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count"
        ]
        == 1
    )
    assert (
        diagnostics["chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count"]
        == 1
    )
    assert diagnostics[
        "chen_fig7_unexplained_straight_through_side_insertion_t_junction_points_sample"
    ] == ((0.0, 0.0),)
    assert diagnostics[
        "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_points_sample"
    ] == ((5.0, 0.0),)
    assert (
        diagnostics["chen_fig7_unexplained_t_junction_split_provenance_stage"]
        == "todo_requires_generation_split_line_metadata_v0"
    )
    assert diagnostics["chen_fig7_unexplained_t_junction_split_endpoint_count"] is None
    assert (
        diagnostics["chen_fig7_unexplained_t_junction_lies_on_split_line_count"] is None
    )


def test_chen_fig7_cleanup_merges_short_shared_edge_to_midpoint() -> None:
    boundary = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    a1 = Polygon([(0, 5), (5, 5), (5, 10), (0, 10)])
    a2 = Polygon([(5, 5), (5.4, 5), (10, 5), (10, 10), (5, 10)])
    b1 = Polygon([(0, 0), (5.4, 0), (5.4, 5), (5, 5), (0, 5)])
    b2 = Polygon([(5.4, 0), (10, 0), (10, 5), (5.4, 5)])
    layout = build_chen_layout(
        parcel_mesh_from_polygons(
            [(1, a1), (2, a2), (3, b1), (4, b2)],
            boundary=boundary,
        ),
        set(),
    )

    before = chen_fig7_short_edge_diagnostics(layout)
    result = apply_chen_fig7_short_edge_cleanup(layout, boundary=boundary)
    cleaned = result.layout
    after = chen_fig7_short_edge_diagnostics(cleaned)

    assert before["chen_fig7_short_shared_edge_candidate_count"] == 1
    assert before["chen_fig7_short_edge_attached_interior_t_junction_count"] == 2
    assert result.metrics["chen_fig7_short_edge_cleanup_stage"] == (
        "chen_section_4_1_midpoint_merge_interpolation_segment_v0"
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_applied"] is True
    assert result.metrics["chen_fig7_short_edge_cleanup_applied_count"] == 1
    assert result.metrics["chen_fig7_short_edge_cleanup_midpoint_merge_count"] == 1
    assert (
        result.metrics["chen_fig7_short_edge_cleanup_boundary_projected_merge_count"]
        == 0
    )
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_applied_count"
        ]
        == 0
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_merge_point_modes"] == (
        "midpoint",
    )
    assert (
        result.metrics["chen_fig7_short_edge_cleanup_boundary_projection_distance_max"]
        == 0.0
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_pre_candidate_count"] == 1
    assert result.metrics["chen_fig7_short_edge_cleanup_post_candidate_count"] == 0
    assert result.metrics["chen_fig7_short_edge_cleanup_blocking_reason"] is None
    assert (
        result.metrics["chen_fig7_short_edge_cleanup_has_labeled_approximations"]
        is False
    )
    assert (
        result.metrics["chen_fig7_short_edge_cleanup_labeled_approximation_reasons"]
        == ()
    )
    assert after["chen_fig7_short_shared_edge_candidate_count"] == 0
    assert after["chen_fig7_unexplained_interior_t_junction_count"] == 0

    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in cleaned.mesh.vertices.values()
    }
    assert (5.0, 5.0) not in point_to_node
    assert (5.4, 5.0) not in point_to_node
    midpoint = point_to_node[(5.2, 5.0)]
    adjacency = street_adjacency(StreetNetworkGraph(cleaned.corner_graph.edges))
    assert len(adjacency[midpoint]) == 4
    assert cleaned.parcel_graph.neighbors[2] == {1, 4}
    assert cleaned.parcel_graph.neighbors[3] == {1, 4}
    assert (2, 3) not in cleaned.parcel_graph.shared_edges


def test_chen_fig7_cleanup_retains_valid_lens_ring_after_corner_collapse() -> None:
    left = (-0.05, 0.0)
    right = (0.05, 0.0)
    upper_lens = Polygon(
        [
            left,
            right,
            (0.06, 1.0),
            (0.08, 2.5),
            (0.06, 4.0),
            (0.0, 5.0),
            (-0.06, 4.0),
            (-0.08, 2.5),
            (-0.06, 1.0),
        ]
    )
    lower = Polygon([right, left, (-1.0, -1.0), (1.0, -1.0)])
    layout = build_chen_layout(
        parcel_mesh_from_polygons([(1, upper_lens), (2, lower)]),
        set(),
    )
    candidates = chen_fig7_short_edge_candidates(layout)

    result = apply_chen_fig7_short_edge_cleanup(layout)
    cleaned = result.layout
    report = evaluate_layout_invariants(cleaned)
    validation = _validate_fig7_candidate_layout(
        cleaned,
        boundary=None,
        expected_parcel_count=len(layout.mesh.parcels),
        original_layout=layout,
        candidate=candidates[0],
    )

    assert len(candidates) == 1
    assert candidates[0].path_points == (left, right)
    assert result.metrics["chen_fig7_short_edge_cleanup_applied"] is True
    assert result.metrics["chen_fig7_short_edge_cleanup_applied_count"] == 1
    assert result.metrics["chen_fig7_short_edge_cleanup_midpoint_merge_count"] == 1
    assert (
        result.metrics["chen_fig7_short_edge_cleanup_boundary_projected_merge_count"]
        == 0
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_merge_point_modes"] == (
        "midpoint",
    )
    assert (
        result.metrics["chen_fig7_short_edge_cleanup_full_mesh_ring_retention_stage"]
        == "representation_guard_after_chen_135deg_corner_simplification_v0"
    )
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_applied_count"
        ]
        == 1
    )
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_count"
        ]
        == 1
    )
    assert result.metrics[
        "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_ids_sample"
    ] == (1,)
    assert (
        "full_mesh_ring_retention_representation_guard_not_exact_fig7"
        in result.metrics["chen_fig7_short_edge_cleanup_scope"]
    )
    assert (
        result.metrics["chen_fig7_short_edge_cleanup_has_labeled_approximations"]
        is True
    )
    assert result.metrics[
        "chen_fig7_short_edge_cleanup_labeled_approximation_reasons"
    ] == ("full_mesh_ring_retention_after_corner_simplification",)
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_count"] == 0
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_failed_degenerate_ring_after_merge_count"
        ]
        == 0
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_post_candidate_count"] == 0
    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in cleaned.mesh.vertices.values()
    }
    assert left not in point_to_node
    assert right not in point_to_node
    assert (0.0, 0.0) in point_to_node
    assert (1, 2) not in cleaned.parcel_graph.shared_edges
    assert cleaned.parcel_graph.neighbors[1] == set()
    assert cleaned.parcel_graph.neighbors[2] == set()
    assert len(cleaned.parcel_graph.parcel_corner_rings[1]) >= 3
    assert validation == (None, None)
    assert report.geometry_valid_pass is True
    assert report.chen_formula_pass is True
    assert report.diagnostic_metric_pass is True
    assert report.metrics["parcel_overlap_count"] == 0


def _fig7_cleanup_layout_with_nonlocal_neighbor_pair() -> tuple[
    ChenLayout, ChenShortEdgeCandidate
]:
    a1 = Polygon([(0, 5), (5, 5), (5, 10), (0, 10)])
    a2 = Polygon([(5, 5), (5.4, 5), (10, 5), (10, 10), (5, 10)])
    b1 = Polygon([(0, 0), (5.4, 0), (5.4, 5), (5, 5), (0, 5)])
    b2 = Polygon([(5.4, 0), (10, 0), (10, 5), (5.4, 5)])
    c1 = Polygon([(20, 0), (25, 0), (25, 5), (20, 5)])
    c2 = Polygon([(25, 0), (30, 0), (30, 5), (25, 5)])
    layout = build_chen_layout(
        parcel_mesh_from_polygons(
            [(1, a1), (2, a2), (3, b1), (4, b2), (5, c1), (6, c2)],
        ),
        set(),
    )
    candidates = chen_fig7_short_edge_candidates(layout)

    assert len(candidates) == 1
    return layout, candidates[0]


def test_chen_fig7_cleanup_handles_boundary_touching_short_edge() -> None:
    boundary = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    left_lower = Polygon([(0, 0), (5, 0), (0, 5)])
    left_upper = Polygon([(0, 5), (5, 0), (5, 0.4), (5, 10), (0, 10)])
    right_lower = Polygon([(5, 0), (10, 0), (10, 5), (5, 0.4)])
    right_upper = Polygon([(5, 0.4), (10, 5), (10, 10), (5, 10)])
    layout = build_chen_layout(
        parcel_mesh_from_polygons(
            [
                (1, left_lower),
                (2, left_upper),
                (3, right_lower),
                (4, right_upper),
            ],
            boundary=boundary,
        ),
        set(),
    )

    before = chen_fig7_short_edge_diagnostics(layout)
    result = apply_chen_fig7_short_edge_cleanup(layout, boundary=boundary)
    cleaned = result.layout
    report = evaluate_layout_invariants(cleaned, target_boundary=boundary)

    assert before["chen_fig7_short_shared_edge_candidate_count"] == 1
    assert before["chen_fig7_short_shared_edge_samples"][0]["path_points_sample"] == (
        (5.0, 0.0),
        (5.0, 0.4),
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_applied"] is True
    assert result.metrics["chen_fig7_short_edge_cleanup_applied_count"] == 1
    assert result.metrics["chen_fig7_short_edge_cleanup_midpoint_merge_count"] == 0
    assert (
        result.metrics["chen_fig7_short_edge_cleanup_boundary_projected_merge_count"]
        == 1
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_merge_point_modes"] == (
        "boundary_projected",
    )
    assert (
        "boundary_projected_merge_point_approximation"
        in result.metrics["chen_fig7_short_edge_cleanup_scope"]
    )
    assert result.metrics[
        "chen_fig7_short_edge_cleanup_labeled_approximation_reasons"
    ] == ("boundary_projected_merge_point_not_exact_midpoint",)
    assert result.metrics[
        "chen_fig7_short_edge_cleanup_boundary_projection_distance_max"
    ] == pytest.approx(0.2)
    assert result.metrics["chen_fig7_short_edge_cleanup_skipped_boundary_count"] == 0
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_count"] == 0
    assert result.metrics["chen_fig7_short_edge_cleanup_post_candidate_count"] == 0
    assert report.geometry_valid_pass is True
    assert math.isclose(
        float(report.metrics["coverage_gap_rate"]),
        0.0,
        abs_tol=1e-12,
    )
    assert math.isclose(
        float(report.metrics["coverage_spillover_rate"]),
        0.0,
        abs_tol=1e-12,
    )

    point_to_node = {
        vertex.point: vertex.vertex_id for vertex in cleaned.mesh.vertices.values()
    }
    assert (5.0, 0.4) not in point_to_node
    assert (5.0, 0.0) in point_to_node
    assert (2, 3) not in cleaned.parcel_graph.shared_edges
    assert cleaned.parcel_graph.neighbors[2] == {1, 4}
    assert cleaned.parcel_graph.neighbors[3] == {4}


def test_chen_fig7_cleanup_classifies_sliver_without_polygon_repair() -> None:
    top = Polygon([(0.0, 0.0), (0.1, 0.0), (0.0, 1.0)])
    bottom = Polygon([(0.0, 0.0), (0.0, -1.0), (0.1, 0.0)])
    layout = build_chen_layout(
        parcel_mesh_from_polygons([(1, top), (2, bottom)]),
        set(),
    )

    before = chen_fig7_short_edge_diagnostics(layout)
    result = apply_chen_fig7_short_edge_cleanup(layout)

    assert before["chen_fig7_short_shared_edge_candidate_count"] == 1
    assert result.layout is layout
    assert result.metrics["chen_fig7_short_edge_cleanup_applied"] is False
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_count"] == 1
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_failed_sliver_or_corner_loss_count"
        ]
        == 1
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_details"] == (
        "failed_degenerate_ring_after_merge",
    )
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_failed_degenerate_ring_after_merge_count"
        ]
        == 1
    )
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_failed_non_simple_ring_after_merge_count"
        ]
        == 0
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_reasons"] == (
        "failed_sliver_or_corner_loss",
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_blocking_reason"] == (
        "short_edge_cleanup_candidates_rejected_by_validation"
    )
    failed_samples = result.metrics["chen_fig7_short_edge_cleanup_failed_samples"]
    failed_samples_by_detail = result.metrics[
        "chen_fig7_short_edge_cleanup_failed_samples_by_detail"
    ]
    assert failed_samples[0]["failure_reason"] == "failed_sliver_or_corner_loss"
    assert failed_samples[0]["failure_detail"] == "failed_degenerate_ring_after_merge"
    assert failed_samples_by_detail["failed_degenerate_ring_after_merge"][0][
        "path_points_sample"
    ] == ((0.0, 0.0), (0.1, 0.0))


def test_chen_fig7_cleanup_rejects_boundary_touching_sliver() -> None:
    boundary = Polygon([(0, -1), (1, -1), (1, 1), (0, 1)])
    top = Polygon([(0.0, 0.0), (0.1, 0.0), (0.0, 1.0)])
    bottom = Polygon([(0.0, 0.0), (0.0, -1.0), (0.1, 0.0)])
    layout = build_chen_layout(
        parcel_mesh_from_polygons([(1, top), (2, bottom)], boundary=boundary),
        set(),
    )

    before = chen_fig7_short_edge_diagnostics(layout)
    result = apply_chen_fig7_short_edge_cleanup(layout, boundary=boundary)

    assert before["chen_fig7_short_shared_edge_candidate_count"] == 1
    assert result.layout is layout
    assert result.metrics["chen_fig7_short_edge_cleanup_applied"] is False
    assert result.metrics["chen_fig7_short_edge_cleanup_skipped_boundary_count"] == 0
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_count"] == 1
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_failed_sliver_or_corner_loss_count"
        ]
        == 1
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_details"] == (
        "failed_degenerate_ring_after_merge",
    )
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_failed_degenerate_ring_after_merge_count"
        ]
        == 1
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_reasons"] == (
        "failed_sliver_or_corner_loss",
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_blocking_reason"] == (
        "short_edge_cleanup_candidates_rejected_by_validation"
    )


def test_chen_fig7_cleanup_validation_rejects_candidate_pair_still_adjacent() -> None:
    layout, candidate = _fig7_cleanup_layout_with_nonlocal_neighbor_pair()

    failure_reason, failure_detail = _validate_fig7_candidate_layout(
        layout,
        boundary=None,
        expected_parcel_count=len(layout.mesh.parcels),
        original_layout=layout,
        candidate=candidate,
    )

    assert failure_reason == "failed_conforming_graph"
    assert failure_detail == "failed_candidate_pair_still_adjacent"


def test_chen_fig7_cleanup_removes_short_edge_when_pair_has_other_shared_edges() -> (
    None
):
    left = Polygon([(0, 0), (5, 0), (5, 5), (4.8, 5), (4.8, 10), (0, 10)])
    right = Polygon([(5, 0), (10, 0), (10, 10), (4.8, 10), (4.8, 5), (5, 5)])
    layout = build_chen_layout(
        parcel_mesh_from_polygons([(1, left), (2, right)]),
        set(),
    )
    candidates = chen_fig7_short_edge_candidates(layout)

    result = apply_chen_fig7_short_edge_cleanup(layout)

    assert len(candidates) == 1
    assert candidates[0].path_points == ((5.0, 5.0), (4.8, 5.0))
    assert len(layout.parcel_graph.shared_edges[(1, 2)]) == 3
    assert result.metrics["chen_fig7_short_edge_cleanup_applied"] is True
    assert result.metrics["chen_fig7_short_edge_cleanup_applied_count"] == 1
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_count"] == 0
    assert result.metrics["chen_fig7_short_edge_cleanup_post_candidate_count"] == 0
    assert result.metrics["chen_fig7_short_edge_cleanup_operation_scopes"] == (
        "all_incident_rings",
    )
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_count"
        ]
        == 0
    )
    assert (
        result.metrics[
            (
                "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_"
                "due_other_shared_edges_count"
            )
        ]
        == 0
    )
    assert result.metrics["chen_fig7_short_edge_cleanup_failed_details"] == ()
    assert (
        result.metrics[
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail"
        ]
        == {}
    )
    assert (
        result.metrics["chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count"]
        == 0
    )
    assert len(result.layout.parcel_graph.shared_edges[(1, 2)]) == 1
    assert chen_fig7_short_edge_candidates(result.layout) == ()


@pytest.mark.slow
def test_chen_fig7_cleanup_classifies_triangle_24_motif_ineligible() -> None:
    generated = generate_named_layout(
        "triangle",
        parcel_count=24,
        seed=200_029,
        apply_optimization=False,
        streamline_mode=STREAMLINE_MODE_YANG_B_FIELD,
    )
    metrics = generated.metrics
    motif_count_key = (
        "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
        "non_candidate_parcel_ring_after_merge_count"
    )

    assert metrics["coverage_rate"] == pytest.approx(1.0)
    assert metrics["coverage_gap_rate"] == pytest.approx(0.0)
    assert metrics["chen_fig7_short_shared_edge_candidate_count"] == 1
    assert metrics["chen_fig7_short_edge_cleanup_applied_count"] == 6
    assert metrics["chen_fig7_short_edge_cleanup_failed_count"] == 6
    assert metrics["chen_fig7_short_edge_cleanup_failed_unique_candidate_count"] == 1
    assert metrics["chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count"] == 5
    assert (
        metrics["chen_fig7_short_edge_cleanup_failed_non_simple_ring_after_merge_count"]
        == 0
    )
    assert metrics[motif_count_key] == 0
    assert (
        metrics[
            (
                "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_"
                "due_other_shared_edges_count"
            )
        ]
        == 0
    )
    assert metrics["chen_fig7_short_edge_cleanup_failed_boundary_coverage_count"] == 6
    assert metrics["chen_fig7_short_edge_cleanup_graph_local_candidate_pair_count"] == 0
    assert metrics["chen_fig7_short_edge_cleanup_operation_scopes"] == (
        "all_incident_rings",
    )
    assert metrics[
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail"
    ] == {"failed_boundary_coverage": 1}
    failed_samples = metrics["chen_fig7_short_edge_cleanup_failed_samples_by_detail"][
        "failed_boundary_coverage"
    ]
    assert failed_samples[0]["path_points_sample"] == (
        (62.71232365, 77.204908741),
        (62.910399082, 77.94785242),
    )


def test_chen_fig7_non_simple_ring_failure_reports_validity_reason() -> None:
    poly, failure_reason, failure_detail, validity_reason = _strict_polygon_from_points(
        [(0.0, 0.0), (2.0, 2.0), (0.0, 2.0), (2.0, 0.0)]
    )

    assert poly is None
    assert failure_reason == "failed_invalid_polygon"
    assert failure_detail == "failed_non_simple_ring_after_merge"
    assert validity_reason is not None
    assert "Self-intersection" in validity_reason


def test_chen_fig7_cleanup_validation_rejects_nonlocal_neighbor_delta() -> None:
    layout, candidate = _fig7_cleanup_layout_with_nonlocal_neighbor_pair()
    cleaned = apply_chen_fig7_short_edge_cleanup(layout).layout
    neighbors = {
        parcel_id: set(parcel_neighbors)
        for parcel_id, parcel_neighbors in cleaned.parcel_graph.neighbors.items()
    }
    neighbors[5].discard(6)
    neighbors[6].discard(5)
    shared_edges = dict(cleaned.parcel_graph.shared_edges)
    shared_edges.pop((5, 6), None)
    bad_graph = ParcelGraph(
        neighbors=neighbors,
        shared_edges=shared_edges,
        edge_paths=cleaned.parcel_graph.edge_paths,
        parcel_corner_rings=cleaned.parcel_graph.parcel_corner_rings,
        parcel_approx_points=cleaned.parcel_graph.parcel_approx_points,
    )
    bad_layout = ChenLayout(
        mesh=cleaned.mesh,
        corner_graph=cleaned.corner_graph,
        parcel_graph=bad_graph,
        street_network=cleaned.street_network,
        street_graph=cleaned.street_graph,
    )

    failure_reason, failure_detail = _validate_fig7_candidate_layout(
        bad_layout,
        boundary=None,
        expected_parcel_count=len(layout.mesh.parcels),
        original_layout=layout,
        candidate=candidate,
    )

    assert failure_reason == "failed_conforming_graph"
    assert failure_detail == "failed_nonlocal_neighbor_delta"


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
