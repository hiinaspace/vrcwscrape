from __future__ import annotations

import math
from typing import Any

import pytest
from shapely import LineString, Polygon

from mapgen.chen_core import build_chen_layout, parcel_mesh_from_polygons
from mapgen.chen_field import StreamlineConfig
from mapgen.chen_generate import (
    STREAMLINE_MODE_YANG_B_FIELD,
    STREAMLINE_MODE_YANG_D_FIELD,
    BoundarySpec,
    _chen_fig7_split_provenance_metrics,
    boundary_preset,
    generate_layout_for_boundary,
    generate_named_layout,
)


@pytest.mark.slow
@pytest.mark.parametrize(
    "name",
    [
        "square",
        pytest.param("oval", marks=pytest.mark.slow),
        pytest.param("triangle", marks=pytest.mark.slow),
    ],
)
def test_generate_named_layout_default_generates_48_parcels(name: str) -> None:
    generated = generate_named_layout(name)

    assert generated.name == name
    assert len(generated.layout.mesh.parcels) == 48
    assert len(generated.split_lines) == 47
    assert generated.metrics["boundary"] == name
    assert generated.metrics["requested_parcel_count"] == 48
    assert (
        generated.metrics["implementation_stage"]
        == "chen_grid_smooth_streamline_bounded_junction_shapeop_like_v1"
    )
    assert generated.metrics["streamline_field_stage"] == "grid_smooth_4rosy_laplace_v1"
    assert (
        generated.metrics["streamline_field_scope"]
        == "compact_grid_smooth_not_full_yang_solver"
    )
    assert (
        generated.metrics["street_selection_mode"]
        == "chen_section_4_2_reachability_bounded_junctions_v0"
    )
    assert generated.metrics["street_topology_reachability_pass"]
    assert generated.metrics["deprecated_chen_street_generation_pass_alias"]
    assert generated.metrics["chen_street_generation_scope"] == (
        "reachability_plus_bounded_junction_completion_v0"
    )
    assert "chen_street_generation_pass" not in generated.metrics
    assert (
        generated.metrics["optimization_stage"]
        == "chen_section_5_shapeop_like_projection_v1"
    )
    assert generated.metrics["optimization_applied"]
    assert generated.metrics["optimization_layout_used"]
    assert (
        generated.metrics["optimization_energy_after"]
        <= generated.metrics["optimization_energy_before"]
    )
    assert generated.metrics["paper_invariant_pass"]
    assert generated.metrics["geometry_valid_pass"]
    assert generated.metrics["corner_graph_edge_count"] > 0
    assert generated.metrics["street_edge_count"] > 0
    assert generated.metrics["street_edge_density"] == pytest.approx(
        generated.metrics["street_edge_count"]
        / generated.metrics["corner_graph_edge_count"]
    )


@pytest.mark.slow
@pytest.mark.parametrize(
    "name",
    [
        "square",
        pytest.param("oval", marks=pytest.mark.slow),
        pytest.param("triangle", marks=pytest.mark.slow),
    ],
)
def test_generate_named_layout_hits_strict_invariants(name: str) -> None:
    generated = generate_named_layout(name, parcel_count=24, seed=7)

    assert generated.name == name
    assert generated.boundary.is_valid
    assert len(generated.layout.mesh.parcels) == 24
    assert len(generated.split_lines) == 23
    assert generated.metrics["boundary"] == name
    assert generated.metrics["accepted_split_count"] == 23
    assert generated.metrics["paper_invariant_pass"]
    assert generated.metrics["geometry_valid_pass"]
    assert generated.metrics["chen_formula_pass"]
    assert generated.metrics["diagnostic_metric_pass"]
    assert generated.metrics["path_access_score_count"] > 0
    assert generated.metrics["path_access_score_fallback_count"] == 0
    assert generated.metrics["street_network_subset_of_corner_graph"]
    assert generated.metrics["unreachable_parcel_count"] == 0
    assert generated.metrics["parcel_overlap_count"] == 0
    assert math.isclose(generated.metrics["coverage_rate"], 1.0, rel_tol=1e-9)
    assert generated.metrics["coverage_spillover_rate"] <= 1e-9


@pytest.mark.slow
def test_generated_mesh_is_conforming_and_uses_split_linework() -> None:
    generated = generate_named_layout("square", parcel_count=16, seed=3)
    layout = generated.layout

    assert generated.split_lines
    assert len(layout.parcel_graph.neighbors) == 16
    assert all(neighbors for neighbors in layout.parcel_graph.neighbors.values())
    assert layout.street_network.edges < layout.corner_graph.edges
    assert generated.metrics["polygonized_face_count"] == 16


@pytest.mark.slow
def test_generate_layout_for_boundary_accepts_explicit_boundary_spec() -> None:
    boundary = boundary_preset("triangle", width=120.0, height=90.0)

    generated = generate_layout_for_boundary(boundary, parcel_count=12, seed=11)

    assert generated.name == "triangle"
    assert len(generated.layout.mesh.parcels) == 12
    assert len(generated.split_lines) == 11
    assert generated.metrics["requested_parcel_count"] == 12
    assert generated.metrics["boundary_area"] == pytest.approx(boundary.geom.area)


@pytest.mark.slow
def test_generation_can_explicitly_disable_optimization() -> None:
    generated = generate_named_layout("oval", parcel_count=8, apply_optimization=False)

    assert generated.metrics["optimization_stage"] == "disabled"
    assert not generated.metrics["optimization_applied"]
    assert not generated.metrics["optimization_layout_used"]
    assert not generated.metrics["optimization_geometry_changed"]
    assert generated.metrics["optimization_skip_reason"] == "apply_optimization_false"
    assert generated.metrics["optimization_diagnostics"] == {}


@pytest.mark.slow
def test_generation_is_deterministic_for_same_seed() -> None:
    first = generate_named_layout("oval", parcel_count=18, seed=13)
    second = generate_named_layout("oval", parcel_count=18, seed=13)

    assert [line.wkb_hex for line in first.split_lines] == [
        line.wkb_hex for line in second.split_lines
    ]
    assert [parcel.geom.wkb_hex for parcel in first.layout.mesh.parcels.values()] == [
        parcel.geom.wkb_hex for parcel in second.layout.mesh.parcels.values()
    ]
    assert first.metrics == second.metrics


@pytest.mark.slow
def test_square_default_uses_only_axis_aligned_split_lines() -> None:
    generated = generate_named_layout("square", parcel_count=24, seed=0)

    assert generated.metrics["line_candidate_mode"] == "streamline_field"
    assert (
        generated.metrics["optimization_stage"]
        == "chen_section_5_shapeop_like_projection_v1"
    )
    assert generated.metrics["optimization_applied"]
    assert generated.metrics["streamline_candidate_count"] > 0
    assert generated.metrics["accepted_streamline_split_count"] > 0
    assert generated.metrics["accepted_axis_fallback_split_count"] == 0
    assert (
        generated.metrics["axis_aligned_split_line_count"]
        == generated.metrics["split_line_count"]
    )
    assert generated.metrics["curved_split_line_count"] == 0
    assert generated.metrics["non_axis_aligned_split_segment_count"] == 0
    assert generated.metrics["non_axis_aligned_street_segment_count"] == 0
    assert (
        generated.metrics["axis_aligned_street_segment_count"]
        == generated.metrics["street_segment_count"]
    )
    assert generated.metrics["curved_street_edge_count"] == 0
    assert generated.metrics["max_mesh_axis_deviation"] <= 1e-10
    assert generated.metrics["max_split_axis_deviation"] <= 1e-10
    assert generated.metrics["max_street_axis_deviation"] <= 1e-10
    assert generated.metrics["interior_corner_graph_t_junction_count"] > 0
    assert generated.metrics["interior_corner_graph_four_way_intersection_count"] == 0
    assert all(_line_is_axis_aligned(line) for line in generated.split_lines)


@pytest.mark.slow
def test_explicit_rectangle_streamlines_report_cleanup_debt() -> None:
    boundary = _explicit_rectangle_boundary()

    generated = generate_layout_for_boundary(boundary, parcel_count=20, seed=5)

    assert generated.metrics["line_candidate_mode"] == "streamline_field"
    assert (
        generated.metrics["optimization_stage"]
        == "chen_section_5_shapeop_like_projection_v1"
    )
    assert generated.metrics["optimization_applied"]
    assert generated.metrics["streamline_candidate_count"] > 0
    assert generated.metrics["accepted_streamline_split_count"] > 0
    assert generated.metrics["accepted_axis_fallback_split_count"] == 0
    assert (
        generated.metrics["axis_aligned_split_line_count"]
        == generated.metrics["split_line_count"]
    )
    assert generated.metrics["curved_split_line_count"] == 0
    assert generated.metrics["non_axis_aligned_split_segment_count"] == 0
    assert generated.metrics["non_axis_aligned_street_segment_count"] > 0
    assert (
        generated.metrics["axis_aligned_street_segment_count"]
        < (generated.metrics["street_segment_count"])
    )
    assert generated.metrics["curved_street_edge_count"] == 0
    assert generated.metrics["max_split_axis_deviation"] <= 1e-10
    assert generated.metrics["max_mesh_axis_deviation"] > 0.0
    assert generated.metrics["max_street_axis_deviation"] > 0.0
    assert generated.metrics["interior_corner_graph_t_junction_count"] > 0
    assert generated.metrics["interior_corner_graph_four_way_intersection_count"] > 0
    assert all(_line_is_axis_aligned(line) for line in generated.split_lines)


@pytest.mark.slow
def test_square_mesh_edges_remain_axis_aligned_after_optimization() -> None:
    generated = generate_named_layout("square", parcel_count=24, seed=0)
    layout = generated.layout

    mesh_deviation = _max_axis_deviation_for_mesh_edges(layout)
    split_deviation = _max_axis_deviation_for_lines(generated.split_lines)
    corner_graph_deviation = _max_axis_deviation_for_edge_paths(
        layout, layout.corner_graph.edges
    )
    street_deviation = _max_axis_deviation_for_edge_paths(
        layout, layout.street_network.edges
    )

    assert mesh_deviation <= 1e-10
    assert split_deviation <= 1e-10
    assert corner_graph_deviation <= 1e-10
    assert street_deviation <= 1e-10
    assert generated.metrics["max_mesh_axis_deviation"] == pytest.approx(mesh_deviation)
    assert generated.metrics["max_split_axis_deviation"] == pytest.approx(
        split_deviation
    )
    assert generated.metrics["max_street_axis_deviation"] == pytest.approx(
        street_deviation
    )


@pytest.mark.slow
def test_axis_aligned_rectangle_has_no_interior_corner_graph_t_junctions() -> None:
    generated = generate_layout_for_boundary(
        _explicit_rectangle_boundary(), parcel_count=20, seed=5
    )
    sample = generated.metrics["rectangular_interior_t_junction_points_sample"]

    assert generated.metrics["line_candidate_mode"] == "streamline_field"
    assert generated.metrics["interior_corner_graph_t_junction_count"] > 0, (
        f"interior T-junctions: {sample}"
    )
    assert sample == ()


@pytest.mark.slow
def test_axis_aligned_rectangle_reports_rectangular_fidelity_metrics() -> None:
    generated = generate_layout_for_boundary(
        _explicit_rectangle_boundary(), parcel_count=20, seed=5
    )
    layout = generated.layout
    metrics = generated.metrics

    for key in (
        "max_mesh_axis_deviation",
        "max_split_axis_deviation",
        "max_street_axis_deviation",
        "interior_corner_graph_t_junction_count",
        "interior_corner_graph_four_way_intersection_count",
        "boundary_corner_graph_t_junction_count",
        "interior_street_t_junction_count",
        "interior_street_four_way_intersection_count",
        "rectangular_interior_t_junction_points_sample",
    ):
        assert key in metrics

    assert metrics["max_mesh_axis_deviation"] == pytest.approx(
        _max_axis_deviation_for_mesh_edges(layout)
    )
    assert metrics["max_split_axis_deviation"] == pytest.approx(
        _max_axis_deviation_for_lines(generated.split_lines)
    )
    assert metrics["max_street_axis_deviation"] == pytest.approx(
        _max_axis_deviation_for_edge_paths(layout, layout.street_network.edges)
    )
    assert metrics["interior_corner_graph_t_junction_count"] == _degree_count(
        layout, layout.corner_graph.edges, degree=3, on_boundary=False
    )
    assert metrics["interior_corner_graph_four_way_intersection_count"] == (
        _degree_count(layout, layout.corner_graph.edges, degree=4, on_boundary=False)
    )
    assert metrics["boundary_corner_graph_t_junction_count"] == _degree_count(
        layout, layout.corner_graph.edges, degree=3, on_boundary=True
    )
    assert metrics["interior_street_t_junction_count"] == _degree_count(
        layout, layout.street_network.edges, degree=3, on_boundary=False
    )


def test_fig7_unexplained_t_junction_split_provenance_fixture_is_geometric() -> None:
    boundary = Polygon([(0, 0), (30, 0), (30, 10), (0, 10)])
    polygons = [
        (1, Polygon([(0, 0), (30, 0), (30, 5), (15, 5), (5, 5), (0, 5)])),
        (2, Polygon([(0, 5), (5, 5), (5, 10), (0, 10)])),
        (3, Polygon([(5, 5), (15, 5), (15, 10), (5, 10)])),
        (4, Polygon([(15, 5), (30, 5), (30, 10), (15, 10)])),
    ]
    mesh = parcel_mesh_from_polygons(polygons, boundary=boundary)
    layout = build_chen_layout(mesh, set())
    split_lines = [
        LineString([(0, 5), (30, 5)]),
        LineString([(15, 5), (15, 10)]),
    ]
    split_diagnostics = [
        {"source": "axis_aligned"},
        {"source": "streamline_continuation"},
    ]
    parcel_wkb_before = [
        parcel.geom.wkb_hex
        for parcel in sorted(
            layout.mesh.parcels.values(), key=lambda item: item.parcel_id
        )
    ]

    metrics = _chen_fig7_split_provenance_metrics(
        layout,
        split_lines,
        split_diagnostics,
        sample_limit=8,
    )

    assert metrics["chen_fig7_unexplained_t_junction_split_provenance_stage"] == (
        "accepted_split_line_geometry_endpoint_or_interior_v0"
    )
    assert metrics["chen_fig7_unexplained_t_junction_split_endpoint_count"] == 1
    assert metrics["chen_fig7_unexplained_t_junction_lies_on_split_line_count"] == 1
    assert metrics["chen_fig7_unexplained_t_junction_split_unknown_count"] == 0
    assert (
        metrics[
            "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_count"
        ]
        == 1
    )
    assert (
        metrics[
            "chen_fig7_unexplained_t_junction_lies_on_split_line_straight_through_count"
        ]
        == 1
    )
    assert metrics["chen_fig7_unexplained_t_junction_split_endpoint_source_counts"] == {
        "streamline_continuation": 1
    }
    assert metrics[
        "chen_fig7_unexplained_t_junction_lies_on_split_line_source_counts"
    ] == {"axis_aligned": 1}
    assert metrics[
        "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_source_counts"
    ] == {"streamline_continuation": 1}
    assert (
        metrics["chen_fig7_unexplained_t_junction_split_endpoint_kinked_source_counts"]
        == {}
    )
    assert metrics[
        "chen_fig7_unexplained_t_junction_lies_on_split_line_"
        "straight_through_source_counts"
    ] == {"axis_aligned": 1}
    assert (
        metrics[
            "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_source_counts"
        ]
        == {}
    )
    assert (
        metrics[
            "chen_fig7_unexplained_t_junction_split_unknown_"
            "straight_through_source_counts"
        ]
        == {}
    )
    assert (
        metrics["chen_fig7_unexplained_t_junction_split_unknown_kinked_source_counts"]
        == {}
    )
    endpoint_sample = metrics["chen_fig7_unexplained_t_junction_split_endpoint_samples"]
    on_line_sample = metrics[
        "chen_fig7_unexplained_t_junction_lies_on_split_line_samples"
    ]
    assert endpoint_sample[0]["point"] == (15.0, 5.0)
    assert endpoint_sample[0]["split_id"] == 2
    assert on_line_sample[0]["point"] == (5.0, 5.0)
    assert on_line_sample[0]["split_id"] == 1
    assert [
        parcel.geom.wkb_hex
        for parcel in sorted(
            layout.mesh.parcels.values(), key=lambda item: item.parcel_id
        )
    ] == parcel_wkb_before


def test_fig7_split_provenance_tracks_kinked_endpoint_source() -> None:
    root3 = math.sqrt(3.0)
    center = (0.0, 0.0)
    right = (10.0, 0.0)
    upper_left = (-5.0, 5.0 * root3)
    lower_left = (-5.0, -5.0 * root3)
    boundary = Polygon([right, upper_left, lower_left])
    polygons = [
        (1, Polygon([center, right, upper_left])),
        (2, Polygon([center, upper_left, lower_left])),
        (3, Polygon([center, lower_left, right])),
    ]
    mesh = parcel_mesh_from_polygons(polygons, boundary=boundary)
    layout = build_chen_layout(mesh, set())
    split_lines = [LineString([center, right])]
    split_diagnostics = [{"source": "streamline"}]

    metrics = _chen_fig7_split_provenance_metrics(
        layout,
        split_lines,
        split_diagnostics,
        sample_limit=8,
    )

    assert metrics["chen_fig7_unexplained_t_junction_split_endpoint_count"] == 1
    assert metrics["chen_fig7_unexplained_t_junction_split_endpoint_kinked_count"] == 1
    assert metrics[
        "chen_fig7_unexplained_t_junction_split_endpoint_kinked_source_counts"
    ] == {"streamline": 1}
    assert (
        metrics[
            "chen_fig7_unexplained_t_junction_split_endpoint_"
            "straight_through_source_counts"
        ]
        == {}
    )
    assert (
        metrics["chen_fig7_unexplained_t_junction_split_unknown_kinked_source_counts"]
        == {}
    )


@pytest.mark.parametrize(
    ("mode", "name"),
    [
        pytest.param(STREAMLINE_MODE_YANG_D_FIELD, "oval", marks=pytest.mark.slow),
        pytest.param(STREAMLINE_MODE_YANG_D_FIELD, "triangle", marks=pytest.mark.slow),
        pytest.param(STREAMLINE_MODE_YANG_B_FIELD, "oval", marks=pytest.mark.slow),
        pytest.param(STREAMLINE_MODE_YANG_B_FIELD, "triangle", marks=pytest.mark.slow),
    ],
)
def test_yang_probe_keeps_straight_through_side_insertions_non_actionable(
    mode: str, name: str
) -> None:
    generated = generate_named_layout(
        name,
        parcel_count=24,
        seed=0,
        streamline_mode=mode,
    )
    metrics = generated.metrics

    assert (
        metrics["chen_fig7_unexplained_interior_t_junction_count"]
        == metrics[
            "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count"
        ]
    )
    assert (
        metrics["chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count"]
        == 0
    )
    assert metrics["chen_fig7_unexplained_t_junction_split_unknown_count"] == 0
    assert metrics["chen_fig7_unexplained_t_junction_split_endpoint_kinked_count"] == 0
    assert (
        metrics["chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_count"] == 0
    )
    assert (
        metrics["chen_fig7_unexplained_t_junction_split_endpoint_count"]
        + metrics["chen_fig7_unexplained_t_junction_lies_on_split_line_count"]
        == metrics["chen_fig7_unexplained_interior_t_junction_count"]
    )
    assert (
        metrics["chen_fig7_unexplained_t_junction_split_endpoint_kinked_source_counts"]
        == {}
    )
    assert (
        metrics[
            "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_source_counts"
        ]
        == {}
    )
    assert (
        metrics["chen_fig7_unexplained_t_junction_split_unknown_kinked_source_counts"]
        == {}
    )


@pytest.mark.slow
def test_oval_uses_curved_streamlines_and_preserves_edge_paths() -> None:
    generated = generate_named_layout("oval", parcel_count=16, seed=0)

    assert generated.metrics["line_candidate_mode"] == "streamline_field"
    assert generated.metrics["streamline_candidate_count"] > 0
    assert generated.metrics["accepted_streamline_split_count"] > 0
    assert generated.metrics["curved_split_line_count"] > 0
    assert generated.metrics["optimization_applied"]
    assert generated.metrics["optimization_geometry_changed"]
    assert generated.metrics["optimization_max_vertex_displacement"] > 0.0
    assert any(_line_is_curved(line) for line in generated.split_lines)
    assert any(
        len(path) > 2 for path in generated.layout.corner_graph.edge_paths.values()
    )


@pytest.mark.slow
def test_default_streamline_generation_reports_heuristic_mode_without_stage_drift() -> (
    None
):
    generated = generate_named_layout("oval", parcel_count=12, seed=0)
    metrics = generated.metrics

    assert metrics["implementation_stage"] == (
        "chen_grid_smooth_streamline_bounded_junction_shapeop_like_v1"
    )
    assert metrics["streamline_field_stage"] == "grid_smooth_4rosy_laplace_v1"
    assert metrics["streamline_field_scope"] == (
        "compact_grid_smooth_not_full_yang_solver"
    )
    assert metrics["streamline_config_mode"] == "baseline"
    assert metrics["streamline_config_field_mode"] == "default"
    assert metrics["streamline_config_candidate_seed_mode"] == "default"
    assert metrics["streamline_config_score_mode"] == "default"
    assert (
        metrics["accepted_streamline_candidate_count"]
        == metrics["accepted_streamline_split_count"]
    )
    assert metrics["accepted_streamline_score_mode_counts"] == {
        "heuristic": metrics["accepted_streamline_candidate_count"]
    }
    assert metrics["accepted_streamline_yang_score_count"] == 0
    assert metrics["accepted_streamline_score_div_count"] == 0
    assert metrics["accepted_streamline_score_div_mean"] == 0.0


def test_yang_streamline_mode_surfaces_field_seed_and_score_metrics() -> None:
    generated = generate_named_layout(
        "oval",
        parcel_count=8,
        seed=0,
        apply_optimization=False,
        streamline_mode=STREAMLINE_MODE_YANG_D_FIELD,
    )
    metrics = generated.metrics

    assert metrics["streamline_config_mode"] == STREAMLINE_MODE_YANG_D_FIELD
    assert metrics["streamline_config_field_mode"] == "yang_d_field"
    assert metrics["streamline_config_candidate_seed_mode"] == "yang_mesh_vertices"
    assert metrics["streamline_config_score_mode"] == "yang_div_db_ds_ct"
    assert metrics["implementation_stage"] == (
        "chen_yang_d_field_streamline_continuity_cleanup_bounded_junction_"
        "shapeop_like_v1"
    )
    assert metrics["streamline_field_stage"] == "yang_d_field_weighted_footpoint_v1"
    assert metrics["streamline_field_scope"] == (
        "yang_d_field_mesh_vertex_seeds_div_db_ds_ct_scoring_v0"
    )
    assert metrics["split_topology_cleanup_stage"] == (
        "yang_streamline_endpoint_continuation_v0"
    )
    assert metrics["split_topology_cleanup_scope"] == (
        "local_tangent_continuation_priority_candidates_not_full_chen_cleanup"
    )
    assert metrics["streamline_candidate_count"] > 0
    assert metrics["accepted_streamline_candidate_count"] > 0
    assert metrics["accepted_streamline_field_mode_counts"] == {
        "yang_d_field": metrics["accepted_streamline_candidate_count"]
    }
    assert metrics["accepted_streamline_trace_seed_source_counts"] == {
        "yang_mesh_vertices": metrics["accepted_streamline_candidate_count"]
    }
    assert metrics["accepted_streamline_score_mode_counts"] == {
        "yang_div_db_ds_ct": metrics["accepted_streamline_candidate_count"]
    }
    assert (
        metrics["accepted_streamline_yang_score_count"]
        == metrics["accepted_streamline_candidate_count"]
    )
    for key in ("score_div", "score_db", "score_ds", "score_ct"):
        assert (
            metrics[f"accepted_streamline_{key}_count"]
            == metrics["accepted_streamline_candidate_count"]
        )
        assert metrics[f"accepted_streamline_{key}_min"] >= 0.0
        assert (
            metrics[f"accepted_streamline_{key}_max"]
            >= metrics[f"accepted_streamline_{key}_min"]
        )
    assert 0.0 <= metrics["accepted_streamline_score_total_normalized_mean"] <= 1.0
    assert metrics["accepted_streamline_score_approximation_scopes"]


@pytest.mark.slow
def test_yang_b_field_streamline_mode_surfaces_omega_mesh_and_score_metrics() -> None:
    generated = generate_named_layout(
        "oval",
        parcel_count=8,
        seed=0,
        apply_optimization=False,
        streamline_mode=STREAMLINE_MODE_YANG_B_FIELD,
    )
    metrics = generated.metrics

    assert metrics["streamline_config_mode"] == STREAMLINE_MODE_YANG_B_FIELD
    assert metrics["streamline_config_field_mode"] == "yang_b_field"
    assert metrics["streamline_config_candidate_seed_mode"] == "yang_mesh_vertices"
    assert metrics["streamline_config_score_mode"] == "yang_div_db_ds_ct"
    assert metrics["implementation_stage"] == (
        "chen_yang_b_field_streamline_endpoint_continuation_bounded_junction_"
        "shapeop_like_v1"
    )
    assert metrics["streamline_field_stage"] == (
        "yang_b_field_boundary_laplacian_omega_v0"
    )
    assert metrics["streamline_field_scope"] == (
        "yang_b_field_mesh_vertex_seeds_div_db_ds_ct_scoring_uniform_clipped_"
        "mesh_laplacian_omega_boundary_alignment_v0"
    )
    assert metrics["split_topology_cleanup_stage"] == (
        "yang_streamline_endpoint_continuation_v0"
    )
    assert metrics["accepted_streamline_candidate_count"] > 0
    assert metrics["accepted_streamline_field_mode_counts"] == {
        "yang_b_field": metrics["accepted_streamline_candidate_count"]
    }
    assert metrics["accepted_streamline_trace_seed_source_counts"] == {
        "yang_mesh_vertices": metrics["accepted_streamline_candidate_count"]
    }
    assert metrics["accepted_streamline_score_mode_counts"] == {
        "yang_div_db_ds_ct": metrics["accepted_streamline_candidate_count"]
    }
    assert metrics["accepted_streamline_trace_mesh_interior_seed_count_min"] > 0
    assert metrics["accepted_streamline_field_mesh_vertex_count_min"] > 0
    assert metrics["accepted_streamline_field_mesh_retained_vertex_count_min"] > 0
    assert metrics["accepted_streamline_field_b_boundary_alignment_weight_mean"] == (
        pytest.approx(0.9)
    )
    assert metrics["accepted_streamline_field_b_boundary_anchor_count_min"] > 0
    assert metrics["accepted_streamline_field_b_boundary_alignment_error_mean"] >= 0.0
    assert metrics["accepted_streamline_field_b_smoothness_energy_mean"] >= 0.0
    assert metrics["accepted_streamline_field_b_solver_residual_max"] >= 0.0
    assert metrics["accepted_streamline_field_b_approximation_scopes"] == (
        "yang2013_supp1_sec4_3_uniform_mesh_graph_laplacian_boundary_vertices",
    )
    assert metrics["accepted_streamline_field_b_boundary_anchor_methods"] == (
        "boundary_samples_with_polygon_vertices_averaged_incident_edges",
    )
    assert (
        metrics["accepted_streamline_yang_score_count"]
        == metrics["accepted_streamline_candidate_count"]
    )
    assert 0.0 <= metrics["accepted_streamline_score_total_normalized_mean"] <= 1.0
    assert metrics["chen_fig7_unexplained_t_junction_split_provenance_stage"] == (
        "accepted_split_line_geometry_endpoint_or_interior_v0"
    )
    assert (
        metrics["chen_fig7_unexplained_t_junction_split_endpoint_count"]
        + metrics["chen_fig7_unexplained_t_junction_lies_on_split_line_count"]
        + metrics["chen_fig7_unexplained_t_junction_split_unknown_count"]
        == metrics["chen_fig7_unexplained_interior_t_junction_count"]
    )


@pytest.mark.slow
def test_yang_b_field_fig7_cleanup_rejects_invalid_triangle_without_optimizer() -> None:
    generated = generate_named_layout(
        "triangle",
        parcel_count=24,
        seed=0,
        apply_optimization=False,
        streamline_mode=STREAMLINE_MODE_YANG_B_FIELD,
    )
    metrics = generated.metrics

    assert metrics["geometry_valid_pass"]
    assert metrics["parcel_overlap_count"] == 0
    assert metrics["coverage_rate"] == pytest.approx(1.0)
    assert metrics["coverage_spillover_rate"] == pytest.approx(0.0)
    assert metrics["chen_fig7_short_edge_cleanup_applied_count"] > 0
    assert metrics["chen_fig7_short_edge_cleanup_failed_count"] > 0
    assert metrics["chen_fig7_short_edge_cleanup_failed_reasons"]
    assert (
        metrics["chen_fig7_short_edge_cleanup_failed_invalid_polygon_count"]
        + metrics["chen_fig7_short_edge_cleanup_failed_overlap_count"]
        + metrics["chen_fig7_short_edge_cleanup_failed_sliver_or_corner_loss_count"]
        + metrics["chen_fig7_short_edge_cleanup_failed_boundary_coverage_count"]
        + metrics["chen_fig7_short_edge_cleanup_failed_conforming_graph_count"]
        == metrics["chen_fig7_short_edge_cleanup_failed_count"]
    )


def test_yang_streamline_mode_rectangle_uses_yang_candidates() -> None:
    generated = generate_named_layout(
        "square",
        parcel_count=12,
        seed=0,
        apply_optimization=False,
        streamline_mode=STREAMLINE_MODE_YANG_D_FIELD,
    )
    metrics = generated.metrics

    assert metrics["streamline_config_mode"] == STREAMLINE_MODE_YANG_D_FIELD
    assert metrics["streamline_config_field_mode"] == "yang_d_field"
    assert metrics["streamline_config_candidate_seed_mode"] == "yang_mesh_vertices"
    assert metrics["streamline_config_score_mode"] == "yang_div_db_ds_ct"
    assert metrics["implementation_stage"] == (
        "chen_yang_d_field_streamline_continuity_cleanup_bounded_junction_"
        "shapeop_like_v1"
    )
    assert metrics["streamline_field_stage"] == "yang_d_field_weighted_footpoint_v1"
    assert metrics["streamline_field_scope"] == (
        "yang_d_field_mesh_vertex_seeds_div_db_ds_ct_scoring_v0"
    )
    assert metrics["split_topology_cleanup_stage"] == (
        "yang_streamline_endpoint_continuation_v0"
    )
    assert metrics["split_topology_cleanup_scope"] == (
        "local_tangent_continuation_priority_candidates_not_full_chen_cleanup"
    )
    assert metrics["chen_fig7_short_edge_cleanup_stage"] == "diagnostic_only_v0"
    assert not metrics["chen_fig7_short_edge_cleanup_applied"]
    assert metrics["chen_fig7_short_shared_edge_candidate_count"] == 0
    assert metrics["streamline_candidate_count"] > 0
    assert metrics["accepted_streamline_candidate_count"] > 0
    assert (
        metrics["accepted_streamline_yang_score_count"]
        == (metrics["accepted_streamline_candidate_count"])
    )


@pytest.mark.slow
def test_yang_b_field_streamline_mode_rectangle_uses_yang_candidates() -> None:
    generated = generate_named_layout(
        "square",
        parcel_count=12,
        seed=0,
        apply_optimization=False,
        streamline_mode=STREAMLINE_MODE_YANG_B_FIELD,
    )
    metrics = generated.metrics

    assert metrics["streamline_config_mode"] == STREAMLINE_MODE_YANG_B_FIELD
    assert metrics["streamline_config_field_mode"] == "yang_b_field"
    assert metrics["streamline_config_candidate_seed_mode"] == "yang_mesh_vertices"
    assert metrics["streamline_config_score_mode"] == "yang_div_db_ds_ct"
    assert metrics["implementation_stage"] == (
        "chen_yang_b_field_streamline_endpoint_continuation_bounded_junction_"
        "shapeop_like_v1"
    )
    assert metrics["streamline_field_stage"] == (
        "yang_b_field_boundary_laplacian_omega_v0"
    )
    assert metrics["streamline_field_scope"] == (
        "yang_b_field_mesh_vertex_seeds_div_db_ds_ct_scoring_uniform_clipped_"
        "mesh_laplacian_omega_boundary_alignment_v0"
    )
    assert metrics["split_topology_cleanup_stage"] == (
        "yang_streamline_endpoint_continuation_v0"
    )
    assert metrics["streamline_candidate_count"] > 0
    assert metrics["accepted_streamline_candidate_count"] > 0
    assert metrics["accepted_streamline_field_b_boundary_alignment_weight_mean"] == (
        pytest.approx(0.9)
    )


@pytest.mark.parametrize(
    (
        "name",
        "max_interior_t_junctions",
        "min_interior_four_ways",
        "min_curved_split_lines",
    ),
    [
        pytest.param("oval", 20, 4, 21, marks=pytest.mark.slow),
        pytest.param("triangle", 20, 3, 22, marks=pytest.mark.slow),
    ],
)
def test_yang_streamline_mode_continues_non_rectangular_split_topology(
    name: str,
    max_interior_t_junctions: int,
    min_interior_four_ways: int,
    min_curved_split_lines: int,
) -> None:
    generated = generate_named_layout(
        name,
        parcel_count=24,
        seed=0,
        streamline_mode=STREAMLINE_MODE_YANG_D_FIELD,
    )
    metrics = generated.metrics

    assert metrics["split_topology_cleanup_stage"] == (
        "yang_streamline_endpoint_continuation_v0"
    )
    assert metrics["accepted_streamline_continuation_split_count"] > 0
    assert (
        metrics["streamline_continuation_candidate_count"]
        >= metrics["accepted_streamline_continuation_split_count"]
    )
    assert metrics["interior_corner_graph_t_junction_count"] <= (
        max_interior_t_junctions
    )
    assert metrics["interior_corner_graph_four_way_intersection_count"] >= (
        min_interior_four_ways
    )
    assert metrics["curved_split_line_count"] >= min_curved_split_lines
    _assert_chen_fig7_short_edge_classification(metrics)


def test_generation_accepts_explicit_streamline_config() -> None:
    config = StreamlineConfig(
        field_mode="yang_d_field",
        candidate_seed_mode="yang_mesh_vertices",
        streamline_score_mode="yang_div_db_ds_ct",
        field_grid_side=9,
        trace_budget_factor=8.0,
        trace_budget_min=80,
        trace_budget_max=80,
    )

    generated = generate_named_layout(
        "triangle",
        parcel_count=8,
        seed=2,
        apply_optimization=False,
        streamline_config=config,
    )

    assert generated.metrics["streamline_config_mode"] == "custom_streamline_config"
    assert generated.metrics["streamline_config_field_mode"] == "yang_d_field"
    assert generated.metrics["streamline_config_candidate_seed_mode"] == (
        "yang_mesh_vertices"
    )
    assert generated.metrics["streamline_config_score_mode"] == "yang_div_db_ds_ct"
    assert generated.metrics["accepted_streamline_yang_score_count"] > 0


def test_boundary_preset_validates_names_and_dimensions() -> None:
    square = boundary_preset("square", width=180.0, height=140.0)

    assert square.name == "square"
    assert square.geom.area == pytest.approx(140.0 * 140.0)
    with pytest.raises(ValueError, match="unknown"):
        boundary_preset("rectangle")
    with pytest.raises(ValueError, match="positive"):
        boundary_preset("square", width=0.0)


def test_oval_boundary_preset_samples_intermediate_vertices_on_ellipse_arc() -> None:
    oval = boundary_preset("oval", width=180.0, height=140.0)
    coords = list(oval.geom.exterior.coords[:-1])
    center = (90.0, 70.0)
    radius_x = 180.0 * 0.47
    radius_y = 140.0 * 0.43

    assert len(coords) == 32
    assert (
        max(
            abs(
                math.hypot(
                    (point[0] - center[0]) / radius_x,
                    (point[1] - center[1]) / radius_y,
                )
                - 1.0
            )
            for point in coords
        )
        <= 1e-12
    )


@pytest.mark.slow
def test_oval_generation_reports_boundary_contour_fidelity_metrics() -> None:
    generated = generate_named_layout(
        "oval", parcel_count=8, seed=0, apply_optimization=False
    )

    assert (
        generated.metrics["boundary_contour_fidelity_stage"]
        == "preset_ellipse_radial_error_v0"
    )
    assert generated.metrics["oval_boundary_vertex_count"] == 32
    assert generated.metrics["oval_boundary_normalized_radial_error_max"] <= 1e-12
    assert generated.metrics["oval_boundary_radial_error_max"] <= 1e-10


def _line_is_axis_aligned(line: LineString) -> bool:
    coords = [(float(x), float(y)) for x, y in line.coords]
    return all(
        abs(a[0] - b[0]) <= 1e-7 or abs(a[1] - b[1]) <= 1e-7
        for a, b in zip(coords, coords[1:], strict=False)
    )


def _explicit_rectangle_boundary() -> BoundarySpec:
    return BoundarySpec("rectangle", Polygon([(0, 0), (200, 0), (200, 100), (0, 100)]))


def _max_axis_deviation_for_mesh_edges(layout: Any) -> float:
    deviations = [
        _axis_deviation(
            layout.mesh.vertices[start].point,
            layout.mesh.vertices[end].point,
        )
        for start, end in layout.mesh.edges
    ]
    return max(deviations) if deviations else 0.0


def _max_axis_deviation_for_edge_paths(
    layout: Any, edges: set[tuple[int, int]] | frozenset[tuple[int, int]]
) -> float:
    lines = []
    for edge in edges:
        path = layout.corner_graph.edge_paths.get(edge, edge)
        lines.append(LineString(layout.mesh.vertices[node].point for node in path))
    return _max_axis_deviation_for_lines(lines)


def _max_axis_deviation_for_lines(
    lines: tuple[LineString, ...] | list[LineString],
) -> float:
    deviations = []
    for line in lines:
        coords = [(float(x), float(y)) for x, y in line.coords]
        deviations.extend(
            _axis_deviation(start, end)
            for start, end in zip(coords, coords[1:], strict=False)
        )
    return max(deviations) if deviations else 0.0


def _axis_deviation(start: tuple[float, float], end: tuple[float, float]) -> float:
    dx = float(end[0] - start[0])
    dy = float(end[1] - start[1])
    length = math.hypot(dx, dy)
    if length <= 1e-12:
        return 0.0
    return min(abs(dx), abs(dy)) / length


def _degree_count(
    layout: Any,
    edges: set[tuple[int, int]] | frozenset[tuple[int, int]],
    *,
    degree: int,
    on_boundary: bool,
) -> int:
    adjacency: dict[int, set[int]] = {}
    for start, end in edges:
        adjacency.setdefault(start, set()).add(end)
        adjacency.setdefault(end, set()).add(start)
    return sum(
        1
        for node, neighbors in adjacency.items()
        if len(neighbors) == degree
        and layout.mesh.vertices[node].on_boundary is on_boundary
    )


def _assert_chen_fig7_short_edge_classification(metrics: dict[str, Any]) -> None:
    assert metrics["chen_fig7_short_edge_detection_stage"] == (
        "chen_section_4_1_shared_edge_threshold_v0"
    )
    assert metrics["chen_fig7_short_edge_cleanup_stage"] == (
        "chen_section_4_1_midpoint_merge_interpolation_segment_v0"
    )
    assert metrics["chen_fig7_short_edge_cleanup_applied"]
    assert metrics["chen_fig7_short_edge_cleanup_applied_count"] > 0
    assert metrics["chen_fig7_short_edge_cleanup_pre_candidate_count"] > 0
    assert (
        metrics["chen_fig7_short_edge_cleanup_post_candidate_count"]
        <= metrics["chen_fig7_short_edge_cleanup_pre_candidate_count"]
    )
    assert (
        metrics["chen_fig7_short_edge_cleanup_post_attached_t_junction_count"]
        <= (metrics["chen_fig7_short_edge_cleanup_pre_attached_t_junction_count"])
    )
    assert (
        metrics["chen_fig7_raw_interior_t_junction_count"]
        == metrics["interior_corner_graph_t_junction_count"]
    )
    assert (
        metrics["chen_fig7_short_edge_attached_interior_t_junction_count"]
        + metrics["chen_fig7_unexplained_interior_t_junction_count"]
        == metrics["interior_corner_graph_t_junction_count"]
    )
    assert metrics["chen_fig7_unexplained_interior_t_junction_count"] <= 14

    samples = metrics["chen_fig7_short_shared_edge_samples"]
    for sample in samples:
        assert sample["path_length"] < sample["threshold"]
        assert len(sample["midpoint"]) == 2
        assert sample["path_points_sample"]

    attached_sample = metrics[
        "chen_fig7_short_edge_attached_interior_t_junction_points_sample"
    ]
    if metrics["chen_fig7_short_edge_attached_interior_t_junction_count"] > 0:
        assert attached_sample


def _line_is_curved(line: LineString) -> bool:
    coords = [(float(x), float(y)) for x, y in line.coords]
    if len(coords) <= 2:
        return False
    start = coords[0]
    end = coords[-1]
    chord = math.hypot(end[0] - start[0], end[1] - start[1])
    if chord <= 1e-7:
        return False
    return any(
        abs(
            (end[0] - start[0]) * (start[1] - point[1])
            - (start[0] - point[0]) * (end[1] - start[1])
        )
        / chord
        > max(1e-6, chord * 1e-4)
        for point in coords[1:-1]
    )
