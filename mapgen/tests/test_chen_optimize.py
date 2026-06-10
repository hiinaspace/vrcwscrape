from __future__ import annotations

import math

import numpy as np
import pytest
from shapely import LineString, Polygon

from mapgen.chen_core import (
    build_chen_layout,
    evaluate_layout_invariants,
    parcel_corner_graph,
    parcel_mesh_from_polygons,
)
from mapgen.chen_optimize import (
    CHEN_SECTION5_STRICT_OPTIMIZATION_CONFIG,
    OptimizationConfig,
    OptimizationWeights,
    _best_regular_polygon_targets,
    _regularity_projection_summary,
    _signed_area_array,
    is_near_axis_rectangular_layout,
    optimize_layout,
)


def _grid_layout(cols: int, rows: int, *, jitter: bool = False):
    boundary = Polygon([(0, 0), (cols, 0), (cols, rows), (0, rows)])
    points: dict[tuple[int, int], tuple[float, float]] = {}
    for y in range(rows + 1):
        for x in range(cols + 1):
            if not jitter or x in {0, cols} or y in {0, rows}:
                points[(x, y)] = (float(x), float(y))
            else:
                dx = 0.055 * math.sin(x * 12.9898 + y * 78.233)
                dy = 0.055 * math.cos(x * 37.719 + y * 19.191)
                points[(x, y)] = (float(x) + dx, float(y) + dy)

    polygons = []
    for y in range(rows):
        for x in range(cols):
            parcel_id = y * cols + x + 1
            polygons.append(
                (
                    parcel_id,
                    Polygon(
                        [
                            points[(x, y)],
                            points[(x + 1, y)],
                            points[(x + 1, y + 1)],
                            points[(x, y + 1)],
                        ]
                    ),
                )
            )
    mesh = parcel_mesh_from_polygons(polygons, boundary=boundary)
    street_edges = set(parcel_corner_graph(mesh).edges)
    return build_chen_layout(mesh, street_edges), boundary


def _topology_key(layout):
    return (
        tuple(sorted(layout.mesh.vertices)),
        tuple(
            (parcel_id, parcel.ring)
            for parcel_id, parcel in sorted(layout.mesh.parcels.items())
        ),
        tuple(sorted(layout.corner_graph.edges)),
        tuple(sorted(layout.street_network.edges)),
        tuple(
            (parcel_id, tuple(sorted(neighbors)))
            for parcel_id, neighbors in sorted(layout.parcel_graph.neighbors.items())
        ),
    )


def _wavy_shared_edge_layout():
    boundary = Polygon([(0, 0), (4, 0), (4, 3), (0, 3)])
    shared = [
        (2.0 + 0.35 * math.sin(math.pi * t / 8.0), 3.0 * t / 8.0) for t in range(9)
    ]
    left = Polygon([(0, 0), *shared, (0, 3)])
    right = Polygon([shared[0], (4, 0), (4, 3), shared[-1], *reversed(shared[1:-1])])
    mesh = parcel_mesh_from_polygons([(1, left), (2, right)], boundary=boundary)
    street_edges = set(parcel_corner_graph(mesh).edges)
    return build_chen_layout(mesh, street_edges), boundary


def _center_offset_grid_layout(center: tuple[float, float]):
    boundary = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    points = {
        (0, 0): (0.0, 0.0),
        (1, 0): (1.0, 0.0),
        (2, 0): (2.0, 0.0),
        (0, 1): (0.0, 1.0),
        (1, 1): center,
        (2, 1): (2.0, 1.0),
        (0, 2): (0.0, 2.0),
        (1, 2): (1.0, 2.0),
        (2, 2): (2.0, 2.0),
    }
    polygons = []
    for y in range(2):
        for x in range(2):
            polygons.append(
                (
                    y * 2 + x + 1,
                    Polygon(
                        [
                            points[(x, y)],
                            points[(x + 1, y)],
                            points[(x + 1, y + 1)],
                            points[(x, y + 1)],
                        ]
                    ),
                )
            )
    mesh = parcel_mesh_from_polygons(polygons, boundary=boundary)
    street_edges = set(parcel_corner_graph(mesh).edges)
    return build_chen_layout(mesh, street_edges), boundary


def _skewed_boundary_split_layout():
    boundary = Polygon([(0, 0), (2, 0), (2, 1), (0, 1)])
    bottom_split = (0.35, 0.0)
    top_split = (1.65, 1.0)
    left = Polygon([(0, 0), bottom_split, top_split, (0, 1)])
    right = Polygon([bottom_split, (2, 0), (2, 1), top_split])
    mesh = parcel_mesh_from_polygons([(1, left), (2, right)], boundary=boundary)
    street_edges = set(parcel_corner_graph(mesh).edges)
    return build_chen_layout(mesh, street_edges), boundary, bottom_split, top_split


def _longest_edge_path(layout):
    return max(layout.corner_graph.edge_paths.values(), key=len)


def _path_chord_residual(layout, path) -> float:
    points = [layout.mesh.vertices[vertex_id].point for vertex_id in path]
    start = points[0]
    end = points[-1]
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    length_sq = dx * dx + dy * dy
    assert length_sq > 0.0

    total = 0.0
    for point in points[1:-1]:
        t = ((point[0] - start[0]) * dx + (point[1] - start[1]) * dy) / length_sq
        projected = (start[0] + t * dx, start[1] + t * dy)
        px = point[0] - projected[0]
        py = point[1] - projected[1]
        total += px * px + py * py
    return total


def test_optimizer_preserves_topology_and_does_not_mutate_input() -> None:
    layout, boundary = _grid_layout(3, 3, jitter=True)
    before_points = {
        vertex_id: vertex.point for vertex_id, vertex in layout.mesh.vertices.items()
    }
    before_wkb = {
        parcel_id: parcel.geom.wkb_hex
        for parcel_id, parcel in layout.mesh.parcels.items()
    }

    result = optimize_layout(layout, boundary)

    assert result.layout is not layout
    assert _topology_key(result.layout) == _topology_key(layout)
    assert set(result.layout.mesh.parcels) == set(layout.mesh.parcels)
    assert len(result.layout.corner_graph.edges) == len(layout.corner_graph.edges)
    assert len(result.layout.street_network.edges) == len(layout.street_network.edges)
    assert {
        vertex_id: vertex.point for vertex_id, vertex in layout.mesh.vertices.items()
    } == before_points
    assert {
        parcel_id: parcel.geom.wkb_hex
        for parcel_id, parcel in layout.mesh.parcels.items()
    } == before_wkb


def test_boundary_vertices_stay_fixed_and_coverage_passes() -> None:
    layout, boundary = _grid_layout(4, 3, jitter=True)
    boundary_line = LineString(boundary.exterior.coords)
    boundary_vertices = {
        vertex_id: vertex.point
        for vertex_id, vertex in layout.mesh.vertices.items()
        if vertex.on_boundary
    }

    result = optimize_layout(layout, boundary)

    for vertex_id, before in boundary_vertices.items():
        after = result.layout.mesh.vertices[vertex_id].point
        assert after == pytest.approx(before, abs=1e-12)
        assert boundary_line.distance(LineString([after, after])) <= 1e-12
    assert result.metrics["coverage_after"] >= 0.999
    assert result.metrics["spillover_after"] <= 1e-9


def test_default_boundary_constraint_mode_keeps_all_boundary_vertices_fixed() -> None:
    layout, boundary, bottom_split, top_split = _skewed_boundary_split_layout()
    split_vertex_ids = {
        vertex_id
        for vertex_id, vertex in layout.mesh.vertices.items()
        if vertex.point in {bottom_split, top_split}
    }

    result = optimize_layout(
        layout,
        boundary,
        OptimizationConfig(
            weights=OptimizationWeights(
                regularity=0.0,
                side_smoothness=0.0,
                street_smoothness=0.0,
                junction_orthogonality=0.0,
                closeness=0.001,
                axis_alignment=0.0,
                area_regularization=0.0,
                rectangularity=8.0,
                edge_length_smoothness=0.0,
            ),
            iterations=8,
            max_vertex_displacement_ratio=0.4,
            enable_axis_alignment=False,
            enable_area_regularization=False,
            enable_edge_length_smoothness=False,
        ),
    )

    assert result.metrics["boundary_constraint_mode"] == "fixed_all_boundary"
    assert result.metrics["boundary_sliding_vertex_count"] == 0
    assert split_vertex_ids
    for vertex_id in split_vertex_ids:
        assert result.layout.mesh.vertices[vertex_id].point == pytest.approx(
            layout.mesh.vertices[vertex_id].point
        )


def test_chen_collinear_boundary_vertices_slide_only_on_boundary_segments() -> None:
    layout, boundary, bottom_split, top_split = _skewed_boundary_split_layout()
    boundary_line = LineString(boundary.exterior.coords)
    corner_points = {(0.0, 0.0), (2.0, 0.0), (2.0, 1.0), (0.0, 1.0)}
    corner_vertex_ids = {
        vertex_id
        for vertex_id, vertex in layout.mesh.vertices.items()
        if vertex.point in corner_points
    }
    split_vertex_ids = {
        vertex_id
        for vertex_id, vertex in layout.mesh.vertices.items()
        if vertex.point in {bottom_split, top_split}
    }

    result = optimize_layout(
        layout,
        boundary,
        OptimizationConfig(
            weights=OptimizationWeights(
                regularity=0.0,
                side_smoothness=0.0,
                street_smoothness=0.0,
                junction_orthogonality=0.0,
                closeness=0.001,
                axis_alignment=0.0,
                area_regularization=0.0,
                rectangularity=8.0,
                edge_length_smoothness=0.0,
            ),
            boundary_constraint_mode="chen_collinear_slide",
            iterations=8,
            max_vertex_displacement_ratio=0.4,
            enable_axis_alignment=False,
            enable_area_regularization=False,
            enable_edge_length_smoothness=False,
        ),
    )
    report = evaluate_layout_invariants(result.layout, target_boundary=boundary)

    assert result.metrics["boundary_constraint_mode"] == "chen_collinear_slide"
    assert result.metrics["boundary_sliding_vertex_count"] == len(split_vertex_ids)
    assert result.metrics["projection_boundary_shape_constraint_equation_count_before"]
    assert _topology_key(result.layout) == _topology_key(layout)
    assert report.metrics["parcel_overlap_count"] == 0
    assert report.metrics["coverage_rate"] >= 0.999
    assert report.metrics["coverage_spillover_rate"] <= 1e-9
    for vertex_id in corner_vertex_ids:
        assert result.layout.mesh.vertices[vertex_id].point == pytest.approx(
            layout.mesh.vertices[vertex_id].point
        )
    moved = 0
    for vertex_id in split_vertex_ids:
        before = layout.mesh.vertices[vertex_id].point
        after = result.layout.mesh.vertices[vertex_id].point
        assert boundary_line.distance(LineString([after, after])) <= 1e-12
        if abs(after[0] - before[0]) > 1e-6 or abs(after[1] - before[1]) > 1e-6:
            moved += 1
        if before == bottom_split:
            assert after[1] == pytest.approx(0.0, abs=1e-12)
            assert 0.0 <= after[0] <= 2.0
        if before == top_split:
            assert after[1] == pytest.approx(1.0, abs=1e-12)
            assert 0.0 <= after[0] <= 2.0
    assert moved > 0


def test_invalid_boundary_constraint_mode_is_rejected() -> None:
    layout, boundary = _grid_layout(1, 1)

    with pytest.raises(ValueError, match="boundary_constraint_mode"):
        optimize_layout(
            layout,
            boundary,
            OptimizationConfig(boundary_constraint_mode="slide_everything"),
        )


def test_jittered_axis_grid_improves_smoothness_or_axis_deviation() -> None:
    layout, boundary = _grid_layout(4, 4, jitter=True)

    assert is_near_axis_rectangular_layout(layout)
    result = optimize_layout(
        layout,
        boundary,
        OptimizationConfig(iterations=24, max_vertex_displacement_ratio=0.05),
    )
    report = evaluate_layout_invariants(result.layout, target_boundary=boundary)

    improved = (
        result.metrics["side_smoothness_after"]
        < result.metrics["side_smoothness_before"]
        or result.metrics["street_smoothness_after"]
        < result.metrics["street_smoothness_before"]
        or result.metrics["axis_deviation_after"]
        < result.metrics["axis_deviation_before"]
    )
    assert improved
    assert report.metrics["parcel_overlap_count"] == 0
    assert report.metrics["coverage_rate"] >= 0.999
    assert report.metrics["coverage_spillover_rate"] <= 1e-9
    assert result.metrics["energy_after"] <= result.metrics["energy_before"]
    assert result.metrics["optimizer_kind"] == "shapeop_like_projection"
    assert result.metrics["projection_equation_count_before"] > 0
    assert (
        result.metrics["projection_residual_after"]
        <= result.metrics["projection_residual_before"] + 1e-9
    )
    assert result.metrics["regularity_projection_equation_count"] > 0
    assert result.metrics["projection_regularity_equation_count_before"] > 0
    assert (
        result.metrics["regularity_projection_kind"]
        == "regular_polygon_similarity_transform_v0"
    )


def test_regular_polygon_projection_improves_irregular_parcels() -> None:
    layout, boundary = _center_offset_grid_layout((0.64, 1.28))
    weights = OptimizationWeights(
        regularity=8.0,
        side_smoothness=0.0,
        street_smoothness=0.0,
        junction_orthogonality=0.0,
        closeness=0.001,
        axis_alignment=0.0,
        area_regularization=0.0,
        rectangularity=0.0,
        edge_length_smoothness=0.0,
    )

    result = optimize_layout(
        layout,
        boundary,
        OptimizationConfig(
            weights=weights,
            iterations=16,
            max_vertex_displacement_ratio=0.35,
            enable_axis_alignment=False,
            enable_area_regularization=False,
            enable_rectangularity=False,
            enable_edge_length_smoothness=False,
        ),
    )
    report = evaluate_layout_invariants(result.layout, target_boundary=boundary)

    assert result.metrics["active_constraint_regularity"]
    assert (
        result.metrics["regularity_projection_kind"]
        == "regular_polygon_similarity_transform_v0"
    )
    assert result.metrics["regularity_projected_parcel_count_before"] == 4
    assert result.metrics["regularity_projected_parcel_count_after"] == 4
    assert result.metrics["regularity_skipped_parcel_count_before"] == 0
    assert result.metrics["regularity_skipped_parcel_count_after"] == 0
    assert result.metrics["regularity_skipped_by_reason_before"] == {}
    assert result.metrics["regularity_skipped_by_reason_after"] == {}
    assert result.metrics["regularity_projection_equation_count"] == 32
    assert result.metrics["projection_regularity_equation_count_before"] == 32
    assert result.metrics["regularity_projection_residual_before"] > 0.0
    assert (
        result.metrics["regularity_projection_residual_after"]
        <= result.metrics["regularity_projection_residual_before"]
    )
    assert result.metrics["regularity_projection_target_displacement_rms_before"] > 0.0
    assert result.metrics["regularity_projection_target_displacement_max_before"] > 0.0
    assert (
        result.metrics["regularity_projection_target_displacement_rms_after"]
        <= result.metrics["regularity_projection_target_displacement_rms_before"]
    )
    assert (
        result.metrics["regularity_projection_target_displacement_max_after"]
        <= result.metrics["regularity_projection_target_displacement_max_before"]
    )
    assert result.metrics["irregularity_after"] < result.metrics["irregularity_before"]
    assert result.metrics["energy_after"] <= result.metrics["energy_before"]
    assert _topology_key(result.layout) == _topology_key(layout)
    assert report.metrics["parcel_overlap_count"] == 0
    assert report.metrics["coverage_rate"] >= 0.999
    assert report.metrics["coverage_spillover_rate"] <= 1e-9


def test_regular_polygon_projection_preserves_orientation_and_normalizes_weight() -> (
    None
):
    clockwise_points = np.array(
        [(0.0, 0.0), (0.0, 1.2), (1.4, 1.0), (1.0, 0.0)],
        dtype=float,
    )

    targets, skip_reason = _best_regular_polygon_targets(clockwise_points)

    assert skip_reason is None
    assert targets is not None
    assert _signed_area_array(targets) * _signed_area_array(clockwise_points) > 0.0

    layout, _boundary = _center_offset_grid_layout((0.64, 1.28))
    positions = {
        vertex_id: np.array(vertex.point, dtype=float)
        for vertex_id, vertex in layout.mesh.vertices.items()
    }
    summary = _regularity_projection_summary(layout, positions, weight=8.0)
    weights_by_parcel: dict[int, set[float]] = {}
    for target in summary.targets:
        weights_by_parcel.setdefault(target.parcel_id, set()).add(
            target.equation_weight
        )

    assert summary.projected_parcel_count == 4
    assert summary.skipped_by_reason == {}
    assert all(len(weights) == 1 for weights in weights_by_parcel.values())
    assert all(next(iter(weights)) < 8.0 for weights in weights_by_parcel.values())
    for parcel_id, weights in weights_by_parcel.items():
        ring = layout.corner_graph.parcel_corner_rings[parcel_id]
        points = np.array([positions[vertex_id] for vertex_id in ring], dtype=float)
        lengths = [
            float(np.linalg.norm(points[(index + 1) % len(points)] - points[index]))
            for index in range(len(points))
        ]
        mean_side_length = float(np.mean(lengths))
        expected = 8.0 / (len(ring) * mean_side_length * mean_side_length)
        assert next(iter(weights)) == pytest.approx(expected)


def test_strict_chen_section5_config_uses_paper_terms_without_extra_stabilizers() -> (
    None
):
    config = CHEN_SECTION5_STRICT_OPTIMIZATION_CONFIG

    assert config.projection_mode == "shapeop_like_projection"
    assert config.boundary_constraint_mode == "chen_collinear_slide"
    assert config.weights.regularity > 0.0
    assert config.weights.side_smoothness > 0.0
    assert config.weights.street_smoothness > 0.0
    assert config.weights.junction_orthogonality > 0.0
    assert config.weights.closeness > 0.0
    assert config.weights.axis_alignment == 0.0
    assert config.weights.area_regularization == 0.0
    assert config.weights.rectangularity == 0.0
    assert config.weights.edge_length_smoothness == 0.0
    assert not config.enable_axis_alignment
    assert not config.enable_area_regularization
    assert not config.enable_rectangularity
    assert not config.enable_edge_length_smoothness


def test_optimization_is_deterministic() -> None:
    layout, boundary = _grid_layout(4, 4, jitter=True)

    first = optimize_layout(layout, boundary)
    second = optimize_layout(layout, boundary)

    assert first.metrics == second.metrics
    assert {
        parcel_id: parcel.geom.wkb_hex
        for parcel_id, parcel in first.layout.mesh.parcels.items()
    } == {
        parcel_id: parcel.geom.wkb_hex
        for parcel_id, parcel in second.layout.mesh.parcels.items()
    }


def test_constraint_energy_terms_decrease() -> None:
    area_layout, area_boundary = _center_offset_grid_layout((0.72, 1.26))
    area_weights = OptimizationWeights(
        regularity=0.0,
        side_smoothness=0.0,
        street_smoothness=0.0,
        junction_orthogonality=0.0,
        closeness=0.001,
        axis_alignment=0.0,
        area_regularization=8.0,
        rectangularity=4.0,
        edge_length_smoothness=0.0,
    )

    area_result = optimize_layout(
        area_layout,
        area_boundary,
        OptimizationConfig(
            weights=area_weights,
            iterations=12,
            parcel_area_target=1.0,
            max_vertex_displacement_ratio=0.3,
            enable_axis_alignment=False,
            enable_edge_length_smoothness=False,
        ),
    )
    area_before = area_result.metrics["energy_terms_before"]
    area_after = area_result.metrics["energy_terms_after"]

    assert area_after["area_regularization"] < area_before["area_regularization"]
    assert area_after["rectangularity"] < area_before["rectangularity"]
    assert area_result.metrics["energy_after"] <= area_result.metrics["energy_before"]

    edge_layout, edge_boundary = _wavy_shared_edge_layout()
    edge_weights = OptimizationWeights(
        regularity=0.0,
        side_smoothness=0.0,
        street_smoothness=0.0,
        junction_orthogonality=0.0,
        closeness=0.001,
        axis_alignment=0.0,
        area_regularization=0.0,
        rectangularity=0.0,
        edge_length_smoothness=6.0,
    )
    edge_result = optimize_layout(
        edge_layout,
        edge_boundary,
        OptimizationConfig(
            weights=edge_weights,
            iterations=8,
            max_vertex_displacement_ratio=0.2,
            enable_axis_alignment=False,
        ),
    )
    edge_before = edge_result.metrics["energy_terms_before"]
    edge_after = edge_result.metrics["energy_terms_after"]

    assert edge_after["edge_length_smoothness"] < edge_before["edge_length_smoothness"]
    assert edge_result.metrics["energy_after"] <= edge_result.metrics["energy_before"]


def test_non_inversion_rejections_are_reported_and_guard_topology() -> None:
    layout, boundary = _center_offset_grid_layout((0.08, 0.08))
    weights = OptimizationWeights(
        regularity=0.0,
        side_smoothness=0.0,
        street_smoothness=0.0,
        junction_orthogonality=0.0,
        closeness=0.0,
        axis_alignment=0.0,
        area_regularization=0.0,
        rectangularity=12.0,
        edge_length_smoothness=0.0,
    )

    result = optimize_layout(
        layout,
        boundary,
        OptimizationConfig(
            weights=weights,
            iterations=2,
            step_size=40.0,
            backtracking_steps=12,
            max_vertex_displacement=20.0,
            max_iteration_displacement=20.0,
            enable_axis_alignment=False,
            enable_area_regularization=False,
            enable_edge_length_smoothness=False,
        ),
    )
    report = evaluate_layout_invariants(result.layout, target_boundary=boundary)

    assert result.metrics["rejected_proposals"].get("non_inversion", 0) > 0
    assert result.metrics["active_constraint_non_inversion_guard"]
    assert _topology_key(result.layout) == _topology_key(layout)
    assert report.metrics["parcel_overlap_count"] == 0
    assert report.metrics["coverage_rate"] >= 0.999


def test_axis_aligned_square_grid_is_stationary() -> None:
    layout, boundary = _grid_layout(2, 2)
    before_points = {
        vertex_id: vertex.point for vertex_id, vertex in layout.mesh.vertices.items()
    }

    result = optimize_layout(layout, boundary)

    assert result.metrics["near_axis_rectangular"]
    assert result.metrics["max_vertex_displacement"] == pytest.approx(0.0, abs=1e-12)
    assert result.metrics["accepted_iteration_count"] == 0
    assert result.metrics["rejected_proposal_count"] == 0
    assert result.metrics["energy_after"] == pytest.approx(
        result.metrics["energy_before"], abs=1e-12
    )
    assert {
        vertex_id: vertex.point
        for vertex_id, vertex in result.layout.mesh.vertices.items()
    } == before_points
    for edge in result.layout.mesh.edges:
        a, b = edge
        pa = result.layout.mesh.vertices[a].point
        pb = result.layout.mesh.vertices[b].point
        assert pa[0] == pytest.approx(pb[0], abs=1e-12) or pa[1] == pytest.approx(
            pb[1], abs=1e-12
        )


def test_v0_relaxation_mode_is_still_reported() -> None:
    layout, boundary = _grid_layout(3, 3, jitter=True)

    result = optimize_layout(
        layout,
        boundary,
        OptimizationConfig(projection_mode="v0_relaxation", iterations=2),
    )

    assert result.metrics["optimizer_kind"] == "v0_relaxation"
    assert result.metrics["projection_equation_count_before"] == 0


def test_area_regularization_energy_and_projection_metrics_are_reported() -> None:
    layout, boundary = _grid_layout(2, 2)
    weights = OptimizationWeights(
        regularity=0.0,
        side_smoothness=0.0,
        street_smoothness=0.0,
        junction_orthogonality=0.0,
        closeness=0.01,
        axis_alignment=0.0,
        area_regularization=3.0,
        rectangularity=0.0,
        edge_length_smoothness=0.0,
    )

    result = optimize_layout(
        layout,
        boundary,
        OptimizationConfig(
            weights=weights,
            iterations=0,
            parcel_area_target=0.75,
            enable_axis_alignment=False,
            enable_rectangularity=False,
            enable_edge_length_smoothness=False,
        ),
    )

    assert _topology_key(result.layout) == _topology_key(layout)
    assert result.metrics["active_constraint_area_regularization"]
    assert result.metrics["area_target_mode"] == "uniform_configured"
    assert result.metrics["area_regularization_before"] > 0.0
    assert result.metrics["area_regularization_after"] == pytest.approx(
        result.metrics["area_regularization_before"]
    )
    assert result.metrics["projection_area_regularization_equation_count_before"] > 0
    assert result.metrics["projection_area_regularization_residual_before"] > 0.0


def test_shapeop_like_projection_straightens_edge_path_more_coherently() -> None:
    layout, boundary = _wavy_shared_edge_layout()
    path = _longest_edge_path(layout)
    weights = OptimizationWeights(
        regularity=0.0,
        side_smoothness=10.0,
        street_smoothness=0.0,
        junction_orthogonality=0.0,
        closeness=0.01,
        axis_alignment=0.0,
        area_regularization=0.0,
        rectangularity=0.0,
        edge_length_smoothness=0.0,
    )
    local = optimize_layout(
        layout,
        boundary,
        OptimizationConfig(
            weights=weights,
            projection_mode="v0_relaxation",
            iterations=1,
            max_vertex_displacement_ratio=0.25,
            max_iteration_displacement=2.0,
            enable_axis_alignment=False,
        ),
    )
    global_projection = optimize_layout(
        layout,
        boundary,
        OptimizationConfig(
            weights=weights,
            projection_mode="shapeop_like_projection",
            iterations=1,
            max_vertex_displacement_ratio=0.25,
            max_iteration_displacement=2.0,
            enable_axis_alignment=False,
        ),
    )
    report = evaluate_layout_invariants(
        global_projection.layout,
        target_boundary=boundary,
    )

    assert _topology_key(local.layout) == _topology_key(layout)
    assert _topology_key(global_projection.layout) == _topology_key(layout)
    assert (
        global_projection.metrics["energy_after"]
        <= global_projection.metrics["energy_before"] + 1e-9
    )
    assert (
        global_projection.metrics[
            "projection_side_path_projection_equation_count_before"
        ]
        > 0
    )
    before_residual = _path_chord_residual(layout, path)
    local_residual = _path_chord_residual(local.layout, path)
    global_residual = _path_chord_residual(global_projection.layout, path)
    assert local_residual < before_residual
    assert global_residual < local_residual
    assert report.metrics["parcel_overlap_count"] == 0
    assert report.metrics["coverage_rate"] >= 0.999
    assert report.metrics["coverage_spillover_rate"] <= 1e-9
