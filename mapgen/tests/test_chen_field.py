from __future__ import annotations

import math

import numpy as np
import pytest
import shapely.affinity
from shapely import LineString, Point, Polygon

from mapgen.chen_field import (
    RasterDensityField,
    RasterGuidanceField,
    StreamlineConfig,
    candidate_streamlines,
    cross_field_angle,
)


def _axis_delta(angle: float) -> float:
    return min(
        abs(math.atan2(math.sin(angle), math.cos(angle))),
        abs(
            math.atan2(math.sin(angle - math.pi * 0.5), math.cos(angle - math.pi * 0.5))
        ),
        abs(math.atan2(math.sin(angle - math.pi), math.cos(angle - math.pi))),
    )


def _cross_delta(a: float, b: float) -> float:
    return abs(0.25 * math.atan2(math.sin(4.0 * (a - b)), math.cos(4.0 * (a - b))))


def _stable_candidate_diagnostics(candidate) -> dict:
    aggregate_trace_keys = {
        "trace_active_option_remaining_count",
        "trace_active_option_total_count",
        "trace_active_orientation_suppression_count",
        "trace_attempt_count",
        "trace_accept_count",
        "trace_budget",
        "trace_budget_exhausted",
        "trace_orientation_0_attempt_count",
        "trace_orientation_1_attempt_count",
        "trace_seed_count",
        "trace_return_count",
    }
    return {
        key: value
        for key, value in candidate.diagnostics.items()
        if key not in aggregate_trace_keys
    }


def _ellipse(width: float = 180.0, height: float = 92.0) -> Polygon:
    return shapely.affinity.scale(
        Point(width * 0.5, height * 0.5).buffer(1.0, quad_segs=32),
        xfact=width * 0.48,
        yfact=height * 0.44,
        origin=(width * 0.5, height * 0.5),
    )


def _boundary_tangent_angle(poly: Polygon, distance: float) -> float:
    ring = poly.exterior
    perimeter = float(ring.length)
    before = ring.interpolate((distance - 0.75) % perimeter)
    after = ring.interpolate((distance + 0.75) % perimeter)
    return math.atan2(float(after.y - before.y), float(after.x - before.x))


def _inset_boundary_point(
    poly: Polygon, fraction: float, inset: float
) -> tuple[float, float]:
    ring = poly.exterior
    raw = ring.interpolate(float(ring.length) * fraction)
    centroid = poly.centroid
    vx = float(centroid.x - raw.x)
    vy = float(centroid.y - raw.y)
    length = math.hypot(vx, vy)
    assert length > 0.0
    return float(raw.x + vx / length * inset), float(raw.y + vy / length * inset)


def test_rectangle_streamline_candidates_are_predominantly_axis_aligned() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])

    candidates = candidate_streamlines(rectangle, target_count=16, seed=0)

    assert len(candidates) >= 12
    assert candidates[0].diagnostics["field_mode"] == "grid_smooth"
    assert candidates[0].diagnostics["field_solver"] == "grid_smooth_4rosy_laplace_v1"
    assert candidates[0].diagnostics["field_solver_iterations"] > 0
    assert candidates[0].diagnostics["field_valid_node_count"] > 0
    best_angle = candidates[0].diagnostics["midpoint_angle"]
    assert _axis_delta(best_angle) <= math.radians(3.0)
    axis_aligned_count = sum(
        1
        for candidate in candidates
        if _axis_delta(candidate.diagnostics["midpoint_angle"]) <= math.radians(6.0)
    )
    assert axis_aligned_count / len(candidates) >= 0.75


def test_yang_d_field_rectangle_recovers_cartesian_grid() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    config = StreamlineConfig(field_mode="yang_d_field")

    angles = [
        cross_field_angle(rectangle, point, config=config)
        for point in ((32.0, 22.0), (80.0, 40.0), (128.0, 58.0))
    ]
    candidates = candidate_streamlines(
        rectangle, target_count=12, seed=0, config=config
    )

    assert all(_axis_delta(angle) <= math.radians(3.0) for angle in angles)
    assert candidates
    assert candidates[0].diagnostics["field_mode"] == "yang_d_field"
    assert (
        candidates[0].diagnostics["field_solver"]
        == "yang_d_field_weighted_footpoint_v1"
    )
    axis_aligned_count = sum(
        1
        for candidate in candidates
        if _axis_delta(candidate.diagnostics["midpoint_angle"]) <= math.radians(6.0)
    )
    assert axis_aligned_count / len(candidates) >= 0.75


def test_yang_d_field_boundary_piece_weights_change_influence() -> None:
    trapezoid = Polygon([(0.0, 0.0), (120.0, 0.0), (120.0, 60.0), (0.0, 100.0)])
    sample = (50.0, 35.0)
    diagonal_piece_angle = math.atan2(40.0, -120.0)
    weighted_config = StreamlineConfig(
        field_mode="yang_d_field",
        field_boundary_piece_weights=(1.0, 1.0, 8.0, 1.0),
    )
    grid_config = StreamlineConfig(
        field_mode="grid_smooth",
        field_boundary_piece_weights=(1.0, 1.0, 8.0, 1.0),
    )

    weighted_angle = cross_field_angle(trapezoid, sample, config=weighted_config)
    grid_angle = cross_field_angle(trapezoid, sample, config=grid_config)

    assert _cross_delta(weighted_angle, diagonal_piece_angle) <= math.radians(3.0)
    assert _cross_delta(grid_angle, diagonal_piece_angle) >= math.radians(8.0)


def test_yang_d_field_l_shape_uses_concave_tangents_without_corner_swirl() -> None:
    l_shape = Polygon(
        [
            (0.0, 0.0),
            (80.0, 0.0),
            (80.0, 30.0),
            (30.0, 30.0),
            (30.0, 80.0),
            (0.0, 80.0),
        ]
    )
    config = StreamlineConfig(field_mode="yang_d_field")
    samples = ((26.0, 26.0), (34.0, 26.0), (26.0, 34.0))

    for sample in samples:
        angle = cross_field_angle(l_shape, sample, config=config)
        corner_vector_angle = math.atan2(30.0 - sample[1], 30.0 - sample[0])
        assert _axis_delta(angle) <= math.radians(4.0)
        assert _cross_delta(angle, corner_vector_angle) >= math.radians(20.0)

    candidates = candidate_streamlines(l_shape, target_count=4, seed=0, config=config)
    assert candidates
    assert candidates[0].diagnostics["field_concave_tangent_use_count"] > 0


def test_yang_d_field_reports_mesh_footpoint_and_smoothing_diagnostics() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    config = StreamlineConfig(field_mode="yang_d_field")

    candidates = candidate_streamlines(rectangle, target_count=4, seed=0, config=config)

    assert candidates
    diagnostics = candidates[0].diagnostics
    assert diagnostics["field_mesh_kind"] == "scipy_delaunay_clipped"
    assert diagnostics["field_mesh_vertex_count"] > 0
    assert diagnostics["field_mesh_triangle_count"] > 0
    assert diagnostics["field_boundary_piece_count"] == 4
    assert diagnostics["field_weighted_footpoint_count"] > 0
    assert (
        diagnostics["field_weighted_footpoint_count"]
        == diagnostics["field_mesh_vertex_count"]
    )
    assert diagnostics["field_boundary_piece_weight_min"] == 1.0
    assert diagnostics["field_boundary_piece_weight_max"] == 1.0
    assert diagnostics["field_smoothing_rounds"] == 1
    assert "field_near_zero_vector_count" in diagnostics
    assert "field_singularity_candidate_count" in diagnostics


def test_yang_b_field_reports_section_4_3_diagnostics() -> None:
    trapezoid = Polygon([(0.0, 0.0), (140.0, 0.0), (112.0, 74.0), (0.0, 92.0)])
    config = StreamlineConfig(
        field_mode="yang_b_field",
        field_grid_side=13,
        field_solver_iterations=90,
        b_field_boundary_alignment_weight=0.9,
    )

    candidates = candidate_streamlines(trapezoid, target_count=6, seed=2, config=config)

    assert candidates
    diagnostics = candidates[0].diagnostics
    assert diagnostics["field_mode"] == "yang_b_field"
    assert diagnostics["field_solver"] == "yang_b_field_boundary_laplacian_omega_v0"
    assert "yang2013_supp1_sec4_3" in diagnostics["field_b_approximation_scope"]
    assert diagnostics["field_mesh_kind"] == "scipy_delaunay_clipped"
    assert diagnostics["field_mesh_vertex_count"] > 0
    assert diagnostics["field_mesh_triangle_count"] > 0
    assert diagnostics["field_boundary_piece_count"] == 4
    assert diagnostics["field_b_boundary_anchor_count"] > 4
    assert diagnostics["field_b_boundary_vertex_anchor_count"] == 4
    assert diagnostics["field_b_boundary_segment_anchor_count"] > 0
    assert (
        diagnostics["field_boundary_constraint_count"]
        == diagnostics["field_b_boundary_anchor_count"]
    )
    assert diagnostics["field_b_boundary_alignment_weight"] == pytest.approx(0.9)
    assert diagnostics["field_b_solver_iterations"] > 0
    assert diagnostics["field_b_solver_residual"] >= 0.0
    assert diagnostics["field_b_smoothness_energy"] >= 0.0
    assert diagnostics["field_b_boundary_alignment_error"] >= 0.0
    assert (
        diagnostics["field_b_renormalized_vector_count"]
        == diagnostics["field_mesh_vertex_count"]
    )
    assert (
        diagnostics["field_b_boundary_piece_weight_scope"]
        == "ignored_by_sec4_3_b_field_energy"
    )


def test_yang_b_field_omega_trades_smoothness_for_alignment() -> None:
    trapezoid = Polygon([(0.0, 0.0), (140.0, 0.0), (112.0, 74.0), (0.0, 92.0)])
    low_weight = StreamlineConfig(
        field_mode="yang_b_field",
        field_grid_side=15,
        field_solver_iterations=140,
        b_field_boundary_alignment_weight=0.05,
    )
    high_weight = StreamlineConfig(
        field_mode="yang_b_field",
        field_grid_side=15,
        field_solver_iterations=140,
        b_field_boundary_alignment_weight=8.0,
    )

    low_candidates = candidate_streamlines(
        trapezoid, target_count=6, seed=4, config=low_weight
    )
    high_candidates = candidate_streamlines(
        trapezoid, target_count=6, seed=4, config=high_weight
    )

    assert low_candidates
    assert high_candidates
    low_diagnostics = low_candidates[0].diagnostics
    high_diagnostics = high_candidates[0].diagnostics
    assert (
        high_diagnostics["field_b_boundary_alignment_error"]
        < low_diagnostics["field_b_boundary_alignment_error"]
    )
    assert (
        high_diagnostics["field_b_smoothness_energy"]
        > low_diagnostics["field_b_smoothness_energy"]
    )

    sample = (108.0, 52.0)
    slanted_boundary_angle = math.atan2(74.0, -28.0)
    low_angle = cross_field_angle(trapezoid, sample, config=low_weight)
    high_angle = cross_field_angle(trapezoid, sample, config=high_weight)
    assert _cross_delta(high_angle, slanted_boundary_angle) < _cross_delta(
        low_angle, slanted_boundary_angle
    )


def test_candidate_streamlines_reports_grid_seed_mode_by_default() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])

    candidates = candidate_streamlines(rectangle, target_count=8, seed=0)

    assert candidates
    diagnostics = candidates[0].diagnostics
    assert diagnostics["trace_seed_source"] == "grid"
    assert diagnostics["trace_mesh_interior_seed_count"] == 0
    assert diagnostics["trace_suppression_distance"] == 0.0
    assert diagnostics["trace_parcel_length"] is None
    assert diagnostics["trace_parcel_length_source"] is None
    assert (
        diagnostics["trace_active_option_total_count"]
        == diagnostics["trace_total_option_count"]
    )
    assert diagnostics["trace_active_orientation_suppression_count"] == 0


def test_yang_mesh_seed_mode_requires_mesh_backed_yang_field() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    config = StreamlineConfig(candidate_seed_mode="yang_mesh_vertices")

    with pytest.raises(ValueError, match="requires a mesh-backed Yang field_mode"):
        candidate_streamlines(rectangle, target_count=8, seed=0, config=config)


def test_yang_mesh_seed_mode_uses_retained_interior_mesh_vertices() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    grid_config = StreamlineConfig(
        field_mode="yang_d_field",
        candidate_seed_mode="grid",
        seed_grid_side_min=5,
        seed_grid_side_max=5,
        trace_budget_min=20,
        trace_budget_max=20,
    )
    yang_config = StreamlineConfig(
        field_mode="yang_d_field",
        candidate_seed_mode="yang_mesh_vertices",
        field_grid_side=9,
        seed_grid_side_min=5,
        seed_grid_side_max=5,
        trace_budget_min=20,
        trace_budget_max=20,
    )

    grid_candidates = candidate_streamlines(
        rectangle, target_count=8, seed=0, config=grid_config
    )
    yang_candidates = candidate_streamlines(
        rectangle, target_count=8, seed=0, config=yang_config
    )

    assert grid_candidates
    assert yang_candidates
    grid_diagnostics = grid_candidates[0].diagnostics
    yang_diagnostics = yang_candidates[0].diagnostics
    assert grid_diagnostics["trace_seed_source"] == "grid"
    assert yang_diagnostics["trace_seed_source"] == "yang_mesh_vertices"
    assert yang_diagnostics["trace_mesh_interior_seed_count"] > 0
    assert (
        yang_diagnostics["trace_mesh_interior_seed_count"]
        == yang_diagnostics["trace_seed_total_count"]
    )
    assert (
        yang_diagnostics["trace_seed_total_count"]
        != grid_diagnostics["trace_seed_total_count"]
    )


def test_yang_mesh_seed_mode_supports_b_field_mesh_vertices() -> None:
    trapezoid = Polygon([(0.0, 0.0), (140.0, 0.0), (112.0, 74.0), (0.0, 92.0)])
    config = StreamlineConfig(
        field_mode="yang_b_field",
        candidate_seed_mode="yang_mesh_vertices",
        field_grid_side=13,
        field_solver_iterations=90,
        b_field_boundary_alignment_weight=0.9,
        trace_budget_factor=8.0,
        trace_budget_min=80,
        trace_budget_max=80,
    )

    candidates = candidate_streamlines(trapezoid, target_count=8, seed=2, config=config)

    assert candidates
    diagnostics = candidates[0].diagnostics
    assert diagnostics["field_mode"] == "yang_b_field"
    assert diagnostics["field_solver"] == "yang_b_field_boundary_laplacian_omega_v0"
    assert diagnostics["trace_seed_source"] == "yang_mesh_vertices"
    assert diagnostics["trace_mesh_interior_seed_count"] > 0
    assert (
        diagnostics["trace_mesh_interior_seed_count"]
        == diagnostics["trace_seed_total_count"]
    )
    assert diagnostics["field_b_boundary_alignment_weight"] == pytest.approx(0.9)
    assert diagnostics["field_b_boundary_alignment_error"] >= 0.0
    assert diagnostics["field_b_smoothness_energy"] >= 0.0


def test_yang_mesh_seed_mode_attempts_both_active_orientations() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    config = StreamlineConfig(
        field_mode="yang_d_field",
        candidate_seed_mode="yang_mesh_vertices",
        field_grid_side=9,
        parcel_length=1.0,
        trace_budget_factor=8.0,
        trace_budget_min=80,
        trace_budget_max=80,
    )

    candidates = candidate_streamlines(
        rectangle, target_count=12, seed=0, config=config
    )

    assert candidates
    diagnostics = candidates[0].diagnostics
    assert diagnostics["trace_orientation_0_attempt_count"] > 0
    assert diagnostics["trace_orientation_1_attempt_count"] > 0
    assert {candidate.orientation_index for candidate in candidates} == {0, 1}


def test_yang_mesh_seed_mode_suppresses_nearby_same_orientation_options() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    config = StreamlineConfig(
        field_mode="yang_d_field",
        candidate_seed_mode="yang_mesh_vertices",
        field_grid_side=9,
        parcel_length=100.0,
        trace_budget_factor=8.0,
        trace_budget_min=80,
        trace_budget_max=80,
    )

    candidates = candidate_streamlines(
        rectangle, target_count=12, seed=0, config=config
    )

    assert candidates
    diagnostics = candidates[0].diagnostics
    assert diagnostics["trace_parcel_length"] == 100.0
    assert diagnostics["trace_parcel_length_source"] == "config"
    assert diagnostics["trace_suppression_distance"] == pytest.approx(30.0)
    assert diagnostics["trace_active_orientation_suppression_count"] > 0
    assert (
        diagnostics["trace_active_option_remaining_count"]
        < diagnostics["trace_active_option_total_count"]
    )


def test_heuristic_score_mode_preserves_default_candidate_quality_order() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    explicit_config = StreamlineConfig(streamline_score_mode="heuristic")

    default = candidate_streamlines(rectangle, target_count=8, seed=0)
    explicit = candidate_streamlines(
        rectangle, target_count=8, seed=0, config=explicit_config
    )

    assert [candidate.line.wkb_hex for candidate in default] == [
        candidate.line.wkb_hex for candidate in explicit
    ]
    assert [candidate.quality for candidate in default] == [
        candidate.quality for candidate in explicit
    ]
    assert default[0].quality == pytest.approx(160.0 / math.hypot(160.0, 80.0))
    assert default[0].diagnostics["score_mode"] == "heuristic"
    assert "score_div" not in default[0].diagnostics
    assert "score_total_normalized" not in default[0].diagnostics


def test_yang_score_mode_reports_div_db_ds_ct_diagnostics() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    config = StreamlineConfig(
        field_mode="yang_d_field",
        candidate_seed_mode="yang_mesh_vertices",
        field_grid_side=9,
        parcel_length=40.0,
        streamline_score_mode="yang_div_db_ds_ct",
        template_widths=(40.0,),
        trace_budget_factor=8.0,
        trace_budget_min=80,
        trace_budget_max=80,
    )

    candidates = candidate_streamlines(rectangle, target_count=8, seed=0, config=config)

    assert candidates
    diagnostics = candidates[0].diagnostics
    assert diagnostics["score_mode"] == "yang_div_db_ds_ct"
    for key in ("score_div", "score_db", "score_ds", "score_ct"):
        assert key in diagnostics
        assert diagnostics[key] >= 0.0
    assert 0.0 <= diagnostics["score_total_normalized"] <= 1.0
    assert diagnostics["score_total_normalized"] == pytest.approx(candidates[0].quality)
    scope = diagnostics["score_approximation_scope"]
    assert "DIV=sampled_field_angle_change_proxy" in scope
    assert "DB=boundary_distance_template_width_fit_1" in scope
    assert "DS=no_field_singularity_candidates_available" in scope
    assert "CT=no_existing_line_endpoints_available" in scope


def test_yang_score_mode_can_affect_candidate_ordering() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    base_config = StreamlineConfig(
        field_mode="yang_d_field",
        candidate_seed_mode="yang_mesh_vertices",
        field_grid_side=9,
        parcel_length=40.0,
        trace_budget_factor=8.0,
        trace_budget_min=80,
        trace_budget_max=80,
    )
    yang_score_config = StreamlineConfig(
        field_mode="yang_d_field",
        candidate_seed_mode="yang_mesh_vertices",
        field_grid_side=9,
        parcel_length=40.0,
        streamline_score_mode="yang_div_db_ds_ct",
        template_widths=(40.0,),
        trace_budget_factor=8.0,
        trace_budget_min=80,
        trace_budget_max=80,
    )

    heuristic = candidate_streamlines(
        rectangle, target_count=8, seed=0, config=base_config
    )
    yang_scored = candidate_streamlines(
        rectangle, target_count=8, seed=0, config=yang_score_config
    )

    assert heuristic
    assert yang_scored
    assert [candidate.line.wkb_hex for candidate in heuristic] != [
        candidate.line.wkb_hex for candidate in yang_scored
    ]
    assert heuristic[0].seed == (80.0, 20.0)
    assert yang_scored[0].seed == (80.0, 40.0)
    assert yang_scored[0].diagnostics["score_total_normalized"] > 0.0


def test_cross_field_blends_toward_oval_boundary_tangent() -> None:
    oval = _ellipse()
    top_near_boundary = (90.0, 82.0)

    angle = cross_field_angle(oval, top_near_boundary)

    assert _axis_delta(angle) <= math.radians(12.0)


def test_grid_smooth_field_is_continuous_across_interior_samples() -> None:
    oval = _ellipse()
    samples = [(42.0 + index * 8.0, 64.0) for index in range(13)]
    assert all(oval.buffer(-4.0).covers(Point(sample)) for sample in samples)

    angles = [cross_field_angle(oval, sample) for sample in samples]
    adjacent_deltas = [
        _cross_delta(left, right)
        for left, right in zip(angles, angles[1:], strict=False)
    ]

    assert max(adjacent_deltas) <= math.radians(7.0)
    assert sum(adjacent_deltas) / len(adjacent_deltas) <= math.radians(3.5)


def test_grid_smooth_field_tracks_oval_boundary_tangents_in_boundary_band() -> None:
    oval = _ellipse()
    fractions = (0.07, 0.18, 0.31, 0.43, 0.57, 0.69, 0.82, 0.93)
    deltas: list[float] = []

    for fraction in fractions:
        sample = _inset_boundary_point(oval, fraction, inset=9.0)
        assert oval.covers(Point(sample))
        angle = cross_field_angle(oval, sample)
        distance = float(oval.exterior.project(Point(sample)))
        tangent = _boundary_tangent_angle(oval, distance)
        deltas.append(_cross_delta(angle, tangent))

    assert max(deltas) <= math.radians(6.0)
    assert sum(deltas) / len(deltas) <= math.radians(3.0)


def test_oval_candidates_include_curved_boundary_to_boundary_streamline() -> None:
    oval = _ellipse()

    candidates = candidate_streamlines(oval, target_count=24, seed=3)

    curved = [
        candidate
        for candidate in candidates
        if candidate.diagnostics["point_count"] > 6
        and candidate.bend >= math.radians(8.0)
    ]
    assert curved
    candidate = curved[0]
    assert candidate.line.is_simple
    assert candidate.endpoint_distance_max <= 3.0
    assert oval.exterior.distance(Point(candidate.line.coords[0])) <= 3.0
    assert oval.exterior.distance(Point(candidate.line.coords[-1])) <= 3.0


@pytest.mark.slow
def test_candidate_streamlines_are_deterministic_for_same_seed() -> None:
    oval = _ellipse()

    first = candidate_streamlines(oval, target_count=12, seed=7)
    second = candidate_streamlines(oval, target_count=12, seed=7)

    assert [candidate.line.wkb_hex for candidate in first] == [
        candidate.line.wkb_hex for candidate in second
    ]
    assert [
        (
            candidate.seed,
            candidate.orientation_index,
            candidate.length,
            candidate.bend,
            candidate.endpoint_distance_max,
            candidate.quality,
            candidate.diagnostics,
        )
        for candidate in first
    ] == [
        (
            candidate.seed,
            candidate.orientation_index,
            candidate.length,
            candidate.bend,
            candidate.endpoint_distance_max,
            candidate.quality,
            candidate.diagnostics,
        )
        for candidate in second
    ]


@pytest.mark.slow
def test_candidate_streamline_order_is_stable_across_target_count() -> None:
    oval = _ellipse()

    small = candidate_streamlines(oval, target_count=8, seed=11)
    large = candidate_streamlines(oval, target_count=24, seed=11)

    assert len(small) == 8
    assert len(large) >= len(small)
    assert [candidate.line.wkb_hex for candidate in small] == [
        candidate.line.wkb_hex for candidate in large[: len(small)]
    ]
    assert [
        (
            candidate.seed,
            candidate.orientation_index,
            _stable_candidate_diagnostics(candidate),
        )
        for candidate in small
    ] == [
        (
            candidate.seed,
            candidate.orientation_index,
            _stable_candidate_diagnostics(candidate),
        )
        for candidate in large[: len(small)]
    ]


def test_candidate_streamlines_reports_default_trace_budget() -> None:
    oval = _ellipse()

    candidates = candidate_streamlines(oval, target_count=48, seed=5)

    assert candidates
    diagnostics = candidates[0].diagnostics
    assert diagnostics["trace_budget"] <= 64
    assert diagnostics["trace_attempt_count"] <= diagnostics["trace_budget"]
    assert diagnostics["trace_total_option_count"] > diagnostics["trace_attempt_count"]
    assert diagnostics["trace_budget_exhausted"] is True
    assert diagnostics["trace_accept_count"] >= diagnostics["trace_return_count"]
    assert all(
        candidate.diagnostics["trace_attempt_count"]
        == diagnostics["trace_attempt_count"]
        for candidate in candidates
    )


def test_candidate_streamlines_trace_budget_scales_without_timing() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    config = StreamlineConfig(
        trace_budget_factor=3.0,
        trace_budget_min=4,
        trace_budget_max=14,
    )

    small = candidate_streamlines(rectangle, target_count=2, seed=0, config=config)
    large = candidate_streamlines(rectangle, target_count=5, seed=0, config=config)

    assert small
    assert large
    small_diagnostics = small[0].diagnostics
    large_diagnostics = large[0].diagnostics
    assert small_diagnostics["trace_budget"] == 6
    assert large_diagnostics["trace_budget"] == 14
    assert small_diagnostics["trace_attempt_count"] == 6
    assert large_diagnostics["trace_attempt_count"] == 14
    assert (
        small_diagnostics["trace_attempt_count"]
        < large_diagnostics["trace_attempt_count"]
    )


def test_existing_line_suppression_removes_near_duplicate_candidates() -> None:
    rectangle = Polygon([(0.0, 0.0), (160.0, 0.0), (160.0, 80.0), (0.0, 80.0)])
    existing = LineString([(0.0, 40.0), (160.0, 40.0)])

    baseline = candidate_streamlines(rectangle, target_count=24, seed=0)
    suppressed = candidate_streamlines(
        rectangle,
        target_count=24,
        seed=0,
        existing_lines=(existing,),
    )

    baseline_near_duplicates = [
        candidate
        for candidate in baseline
        if candidate.line.distance(existing) <= 1.0
        and _axis_delta(candidate.diagnostics["midpoint_angle"]) <= math.radians(6.0)
    ]
    suppressed_near_duplicates = [
        candidate
        for candidate in suppressed
        if candidate.line.distance(existing) <= 1.0
        and _axis_delta(candidate.diagnostics["midpoint_angle"]) <= math.radians(6.0)
    ]
    assert baseline_near_duplicates
    assert len(suppressed_near_duplicates) < len(baseline_near_duplicates)


def test_candidate_streamlines_validates_boundary() -> None:
    with pytest.raises(ValueError, match="valid non-empty"):
        candidate_streamlines(Polygon(), target_count=4)


# ---------------------------------------------------------------------------
# R1 regional extension: RasterGuidanceField (off by default, not paper).
# ---------------------------------------------------------------------------


def _constant_guidance(
    *,
    angle: float,
    weight: float,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    cells: int = 12,
) -> RasterGuidanceField:
    """A uniform guidance raster covering the [min, max] world box."""
    span = max(maxx - minx, maxy - miny)
    cell = span / max(cells - 1, 1)
    angle_raster = np.full((cells, cells), float(angle), dtype=float)
    weight_raster = np.full((cells, cells), float(weight), dtype=float)
    return RasterGuidanceField(
        angle=angle_raster,
        weight=weight_raster,
        x0=minx,
        y0=miny,
        cell=cell,
    )


def test_guidance_none_is_byte_identical_to_default_field() -> None:
    square = Polygon([(0.0, 0.0), (120.0, 0.0), (120.0, 120.0), (0.0, 120.0)])
    samples = [(30.0, 30.0), (60.0, 60.0), (90.0, 40.0), (45.0, 95.0)]
    for sample in samples:
        baseline = cross_field_angle(square, sample)
        with_none = cross_field_angle(square, sample, guidance=None)
        assert with_none == baseline


def test_guidance_rotates_interior_field_angles() -> None:
    square = Polygon([(0.0, 0.0), (120.0, 0.0), (120.0, 120.0), (0.0, 120.0)])
    guidance = _constant_guidance(
        angle=math.radians(45.0),
        weight=6.0,
        minx=0.0,
        miny=0.0,
        maxx=120.0,
        maxy=120.0,
    )
    interior = (60.0, 60.0)

    baseline_angle = cross_field_angle(square, interior)
    guided_angle = cross_field_angle(square, interior, guidance=guidance)

    # The default square field is axis-aligned at the center; 45 deg guidance
    # must pull the interior 4-RoSy orientation measurably off-axis toward 45.
    assert _axis_delta(baseline_angle) <= math.radians(5.0)
    assert _cross_delta(guided_angle, math.radians(45.0)) < _cross_delta(
        baseline_angle, math.radians(45.0)
    )
    assert _cross_delta(guided_angle, baseline_angle) >= math.radians(10.0)


def test_guidance_does_not_override_boundary_alignment() -> None:
    square = Polygon([(0.0, 0.0), (120.0, 0.0), (120.0, 120.0), (0.0, 120.0)])
    guidance = _constant_guidance(
        angle=math.radians(45.0),
        weight=50.0,  # deliberately huge; must still not flip the boundary
        minx=0.0,
        miny=0.0,
        maxx=120.0,
        maxy=120.0,
    )
    # A node hard against the bottom edge: boundary tangent is horizontal.
    near_boundary = (60.0, 1.0)

    guided_angle = cross_field_angle(square, near_boundary, guidance=guidance)
    # Despite 45 deg guidance at 50x weight, the near-boundary orientation stays
    # boundary-aligned (axis), not rotated to 45.
    assert _axis_delta(guided_angle) <= math.radians(12.0)
    assert _cross_delta(guided_angle, math.radians(45.0)) > math.radians(20.0)


def test_guidance_zero_weight_leaves_field_unchanged() -> None:
    square = Polygon([(0.0, 0.0), (120.0, 0.0), (120.0, 120.0), (0.0, 120.0)])
    guidance = _constant_guidance(
        angle=math.radians(45.0),
        weight=0.0,
        minx=0.0,
        miny=0.0,
        maxx=120.0,
        maxy=120.0,
    )
    for sample in [(60.0, 60.0), (30.0, 90.0)]:
        baseline = cross_field_angle(square, sample)
        zero_weight = cross_field_angle(square, sample, guidance=guidance)
        assert zero_weight == pytest.approx(baseline, abs=1e-9)


def test_guidance_outside_raster_has_no_influence() -> None:
    square = Polygon([(0.0, 0.0), (120.0, 0.0), (120.0, 120.0), (0.0, 120.0)])
    # Raster footprint covers only the lower-left corner of the square.
    guidance = _constant_guidance(
        angle=math.radians(45.0),
        weight=8.0,
        minx=0.0,
        miny=0.0,
        maxx=30.0,
        maxy=30.0,
        cells=6,
    )
    far_interior = (100.0, 100.0)
    baseline = cross_field_angle(square, far_interior)
    guided = cross_field_angle(square, far_interior, guidance=guidance)
    # The far node is outside the raster footprint, so the solved field there is
    # only weakly perturbed via smoothing; assert it stays near the baseline.
    assert _cross_delta(guided, baseline) <= math.radians(8.0)


def test_guidance_field_eq_does_not_compare_arrays() -> None:
    # eq=False keeps the frozen dataclass from raising on array comparison.
    guidance = _constant_guidance(
        angle=0.0, weight=1.0, minx=0.0, miny=0.0, maxx=10.0, maxy=10.0
    )
    assert guidance == guidance  # identity-based, no ValueError
    assert isinstance(guidance.digest(), bytes)


# ---------------------------------------------------------------------------
# R2 regional extension: RasterDensityField.mass (off by default, not paper).
# ---------------------------------------------------------------------------


def test_density_mass_uniform_field_equals_area_times_density() -> None:
    # Uniform density 2.0 on a unit-cell 10x10 grid (cell centers at 0.5..9.5).
    field = RasterDensityField(density=np.full((10, 10), 2.0), x0=0.0, y0=0.0, cell=1.0)
    # A 3x3 square aligned to cell centers covers exactly 9 cells -> 9 * 2 = 18.
    square = Polygon([(1.0, 1.0), (4.0, 1.0), (4.0, 4.0), (1.0, 4.0)])
    assert field.mass(square) == pytest.approx(18.0)
    # The full grid: 100 cells * 2.0 -> 200 (== area * density for uniform).
    full = Polygon([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
    assert field.mass(full) == pytest.approx(200.0)


def test_density_mass_picks_out_single_hot_cell() -> None:
    density = np.zeros((10, 10))
    density[5, 7] = 4.0  # center at (7.5, 5.5)
    field = RasterDensityField(density=density, x0=0.0, y0=0.0, cell=1.0)
    hot = Polygon([(7.0, 5.0), (8.0, 5.0), (8.0, 6.0), (7.0, 6.0)])
    assert field.mass(hot) == pytest.approx(4.0)
    # A polygon over a cold neighbour integrates to zero.
    cold = Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])
    assert field.mass(cold) == pytest.approx(0.0)


def test_density_mass_hot_cell_with_nonzero_origin() -> None:
    """Same hot-cell integral with a shifted raster origin (x0, y0 != 0).

    The other mass tests all use x0 = y0 = 0, so a sign/offset bug in the
    row/col bbox-window or cell-center arithmetic would still pass them.
    """
    density = np.zeros((10, 10))
    density[5, 7] = 4.0  # center at (x0 + 7.5, y0 + 5.5) = (10.5, -14.5)
    field = RasterDensityField(density=density, x0=3.0, y0=-20.0, cell=1.0)
    hot = Polygon([(10.0, -15.0), (11.0, -15.0), (11.0, -14.0), (10.0, -14.0)])
    assert field.mass(hot) == pytest.approx(4.0)
    # The same window one cell up in y (row 6) is cold.
    cold = Polygon([(10.0, -14.0), (11.0, -14.0), (11.0, -13.0), (10.0, -13.0)])
    assert field.mass(cold) == pytest.approx(0.0)
    # Whole-raster integral must be independent of the origin shift.
    whole = Polygon([(3.0, -20.0), (13.0, -20.0), (13.0, -10.0), (3.0, -10.0)])
    assert field.mass(whole) == pytest.approx(4.0)


def test_density_mass_scales_with_cell_area() -> None:
    # Same density, cell=2 -> each cell carries 4x the mass of a unit cell.
    field = RasterDensityField(density=np.full((10, 10), 1.0), x0=0.0, y0=0.0, cell=2.0)
    # Square covering a single cell center (center at (1,1)).
    one_cell = Polygon([(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)])
    assert field.mass(one_cell) == pytest.approx(1.0 * 2.0 * 2.0)


def test_density_mass_outside_raster_is_zero() -> None:
    field = RasterDensityField(density=np.full((4, 4), 5.0), x0=0.0, y0=0.0, cell=1.0)
    far = Polygon([(100.0, 100.0), (101.0, 100.0), (101.0, 101.0), (100.0, 101.0)])
    assert field.mass(far) == 0.0


def test_density_field_eq_does_not_compare_arrays() -> None:
    field = RasterDensityField(density=np.ones((3, 3)), x0=0.0, y0=0.0, cell=1.0)
    assert field == field  # identity-based, no ValueError on array compare
    assert isinstance(field.digest(), bytes)
