from __future__ import annotations

import importlib.util
import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from mapgen.chen_generate import (
    STREAMLINE_MODE_YANG_B_FIELD,
    STREAMLINE_MODE_YANG_D_FIELD,
)

OPTIMIZATION_REGULARITY_REVIEW_KEYS = (
    "optimization_regularity_projection_kind",
    "optimization_regularity_projected_parcel_count_before",
    "optimization_regularity_projected_parcel_count_after",
    "optimization_regularity_skipped_parcel_count_before",
    "optimization_regularity_skipped_parcel_count_after",
    "optimization_regularity_skipped_by_reason_before",
    "optimization_regularity_skipped_by_reason_after",
    "optimization_regularity_projection_equation_count",
    "optimization_regularity_projection_residual_before",
    "optimization_regularity_projection_residual_after",
    "optimization_regularity_projection_target_displacement_rms_before",
    "optimization_regularity_projection_target_displacement_rms_after",
    "optimization_regularity_projection_target_displacement_max_before",
    "optimization_regularity_projection_target_displacement_max_after",
)


@dataclass(frozen=True)
class GeneratedSuiteStub:
    name: str
    parcel_count: int
    seed: int
    streamline_mode: str = "baseline"


def test_scale_suite_writes_grouped_manifest_with_review_metrics(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, int, int, float, float, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int,
        seed: int,
        width: float,
        height: float,
        streamline_mode: str = "baseline",
    ) -> GeneratedSuiteStub:
        calls.append((name, parcel_count, seed, width, height, streamline_mode))
        return GeneratedSuiteStub(
            name=name,
            parcel_count=parcel_count,
            seed=seed,
            streamline_mode=streamline_mode,
        )

    _install_fake_mapgen_modules(
        monkeypatch,
        generate_named_layout=generate_named_layout,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )

    runner = _load_scale_runner_module()

    manifest = runner.run_suite(
        out_dir=tmp_path,
        parcel_counts=(12, 24),
        seed=5,
        width=40.0,
        height=30.0,
    )

    assert calls == [
        ("square", 12, 17, 40.0, 30.0, "baseline"),
        ("square", 24, 29, 40.0, 30.0, "baseline"),
        ("oval", 12, 100_017, 40.0, 30.0, "baseline"),
        ("oval", 24, 100_029, 40.0, 30.0, "baseline"),
        ("triangle", 12, 200_017, 40.0, 30.0, "baseline"),
        ("triangle", 24, 200_029, 40.0, 30.0, "baseline"),
    ]
    assert json.loads((tmp_path / "manifest.json").read_text()) == manifest

    square_12 = manifest["shapes"]["square"]["12"]
    assert square_12["dir"] == "square/parcels_0012"
    assert square_12["manifest"] == "square/parcels_0012/manifest.json"
    assert square_12["summary"]["parcel_count"] == 12
    assert square_12["review_metrics"]["implementation_stage"] == (
        "chen_grid_smooth_streamline_bounded_junction_shapeop_like_v1"
    )
    assert (
        square_12["review_metrics"]["streamline_field_stage"]
        == "grid_smooth_4rosy_laplace_v1"
    )
    assert square_12["review_metrics"]["street_selection_mode"] == (
        "chen_section_4_2_reachability_bounded_junctions_v0"
    )
    assert square_12["review_metrics"]["chen_street_generation_scope"] == (
        "reachability_plus_bounded_junction_completion_v0"
    )
    assert square_12["review_metrics"]["corner_graph_edge_count"] == 24
    assert square_12["review_metrics"]["street_edge_density"] == pytest.approx(11 / 24)
    assert square_12["review_metrics"]["street_t_junction_count"] == 3
    assert square_12["review_metrics"]["streamline_config_mode"] == "baseline"
    assert square_12["review_metrics"]["accepted_streamline_yang_score_count"] == 0
    assert square_12["review_metrics"]["candidate_split_reject_count"] == 12
    assert square_12["review_metrics"]["candidate_topology_reject_count"] == 0
    assert square_12["review_metrics"]["path_access_score_count"] == 24
    assert square_12["review_metrics"]["chen_fig7_short_edge_cleanup_stage"] == (
        "diagnostic_only_v0"
    )
    assert square_12["review_metrics"]["chen_fig7_short_edge_cleanup_applied"] is False
    assert (
        square_12["review_metrics"]["chen_fig7_short_shared_edge_candidate_count"] == 0
    )
    assert (
        square_12["review_metrics"]["optimization_regularity_projection_kind"]
        == "regular_polygon_similarity_transform_v0"
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projected_parcel_count_before"
        ]
        == 12
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projected_parcel_count_after"
        ]
        == 12
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_skipped_parcel_count_before"
        ]
        == 0
    )
    assert (
        square_12["review_metrics"]["optimization_regularity_skipped_by_reason_before"]
        == {}
    )
    assert (
        square_12["review_metrics"]["optimization_regularity_projection_equation_count"]
        == 48
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projection_residual_before"
        ]
        == 12.0
    )
    assert (
        square_12["review_metrics"]["optimization_regularity_projection_residual_after"]
        == 6.0
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projection_target_displacement_rms_before"
        ]
        == 1.2
    )
    assert (
        square_12["review_metrics"][
            "optimization_regularity_projection_target_displacement_max_after"
        ]
        == 1.2
    )
    assert square_12["review_metrics"]["runtime_seconds"] >= 0.0
    assert (tmp_path / "square" / "parcels_0012" / "manifest.json").exists()

    overall = manifest["aggregate_metrics"]["overall"]
    assert overall["run_count"] == 6
    assert overall["parcel_counts"] == [12, 24]
    assert overall["reachability_pass_count"] == 6
    assert overall["optimization_applied_count"] == 6
    assert overall["optimization_changed_count"] == 4
    assert overall["street_edge_count"] == 102
    assert overall["corner_graph_edge_count"] == 216
    assert overall["street_edge_density"] == pytest.approx(102 / 216)
    assert overall["curved_split_line_count"] == 36
    assert overall["non_axis_aligned_split_segment_count"] == 72
    assert overall["street_t_junction_count"] == 18
    assert overall["street_four_way_intersection_count"] == 12
    assert overall["accepted_streamline_continuation_split_count"] == 36
    assert overall["accepted_axis_fallback_split_count"] == 2
    assert overall["candidate_split_reject_count"] == 180
    assert overall["candidate_topology_reject_count"] == 18
    assert overall["path_access_score_count"] == 216
    assert overall["path_access_score_fallback_count"] == 6
    assert overall["chen_fig7_short_shared_edge_candidate_count"] == 12
    assert overall["chen_fig7_raw_interior_t_junction_count"] == 18
    assert overall["chen_fig7_short_edge_attached_interior_t_junction_count"] == 8
    assert overall["chen_fig7_unexplained_interior_t_junction_count"] == 10
    assert (
        overall[
            "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count"
        ]
        == 6
    )
    assert (
        overall["chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count"]
        == 4
    )
    assert overall["chen_fig7_unexplained_t_junction_split_endpoint_count"] == 6
    assert overall["chen_fig7_unexplained_t_junction_lies_on_split_line_count"] == 4
    assert overall["chen_fig7_unexplained_t_junction_split_unknown_count"] == 0
    assert (
        overall[
            "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_count"
        ]
        == 6
    )
    assert (
        overall["chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_count"] == 4
    )
    assert overall["chen_fig7_unexplained_t_junction_split_endpoint_source_counts"] == {
        "streamline_continuation": 6
    }
    assert overall[
        "chen_fig7_unexplained_t_junction_lies_on_split_line_source_counts"
    ] == {"streamline": 4}
    assert overall["chen_fig7_short_shared_edge_length_threshold_ratio_max"] == 0.5
    assert overall["chen_fig7_short_edge_cleanup_applied_count"] == 6
    assert overall["chen_fig7_short_edge_cleanup_midpoint_merge_count"] == 6
    assert overall["chen_fig7_short_edge_cleanup_boundary_projected_merge_count"] == 0
    assert overall["chen_fig7_short_edge_cleanup_failed_count"] == 2
    assert overall["chen_fig7_short_edge_cleanup_failed_unique_candidate_count"] == 2
    assert overall["chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count"] == 0
    assert overall[
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail"
    ] == {"failed_invalid_polygon": 2}
    assert overall["chen_fig7_short_edge_cleanup_failed_invalid_polygon_count"] == 2
    assert (
        overall[
            "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
            "non_candidate_parcel_ring_after_merge_count"
        ]
        == 2
    )
    assert overall["chen_fig7_short_edge_cleanup_skipped_boundary_count"] == 2
    assert overall["optimization_regularity_projected_parcel_count_before"] == 108
    assert overall["optimization_regularity_projected_parcel_count_after"] == 108
    assert overall["optimization_regularity_skipped_parcel_count_before"] == 4
    assert overall["optimization_regularity_skipped_parcel_count_after"] == 4
    assert overall["optimization_regularity_skipped_by_reason_before"] == {
        "flipped_orientation": 4
    }
    assert overall["optimization_regularity_skipped_by_reason_after"] == {
        "flipped_orientation": 4
    }
    assert overall["optimization_regularity_projection_equation_count"] == 432
    assert overall["optimization_regularity_projection_residual_before"] == 24.0
    assert overall["optimization_regularity_projection_residual_after"] == 12.0
    assert (
        overall["optimization_regularity_projection_target_displacement_rms_before"]
        == 2.4
    )
    assert (
        overall["optimization_regularity_projection_target_displacement_rms_after"]
        == 1.2
    )
    assert (
        overall["optimization_regularity_projection_target_displacement_max_before"]
        == 4.8
    )
    assert (
        overall["optimization_regularity_projection_target_displacement_max_after"]
        == 2.4
    )

    oval = manifest["aggregate_metrics"]["by_shape"]["oval"]
    assert oval["run_count"] == 2
    assert oval["street_edge_density"] == pytest.approx(34 / 72)
    assert oval["curved_split_line_count"] == 36
    assert oval["non_axis_aligned_street_segment_count"] == 36
    assert oval["accepted_streamline_continuation_split_count"] == 18
    assert oval["candidate_topology_reject_count"] == 9
    assert oval["chen_fig7_short_shared_edge_candidate_count"] == 6
    assert oval["chen_fig7_unexplained_interior_t_junction_count"] == 5
    assert (
        oval["chen_fig7_unexplained_straight_through_side_insertion_t_junction_count"]
        == 3
    )
    assert (
        oval["chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count"] == 2
    )
    assert oval["chen_fig7_unexplained_t_junction_split_endpoint_count"] == 3
    assert oval["chen_fig7_unexplained_t_junction_lies_on_split_line_count"] == 2
    assert oval["chen_fig7_unexplained_t_junction_split_unknown_count"] == 0
    assert oval["chen_fig7_short_edge_cleanup_applied_count"] == 3
    assert oval["chen_fig7_short_edge_cleanup_failed_count"] == 1
    assert oval["chen_fig7_short_edge_cleanup_failed_unique_candidate_count"] == 1
    assert oval["optimization_regularity_projected_parcel_count_before"] == 36
    assert oval["optimization_regularity_skipped_parcel_count_before"] == 2
    assert oval["optimization_regularity_skipped_by_reason_before"] == {
        "flipped_orientation": 2
    }
    assert oval["optimization_regularity_projection_equation_count"] == 144

    parcels_12 = manifest["aggregate_metrics"]["by_parcel_count"]["12"]
    assert parcels_12["run_count"] == 3
    assert parcels_12["parcel_counts"] == [12]
    assert parcels_12["shapes"] == ["square", "oval", "triangle"]
    assert parcels_12["street_edge_density"] == pytest.approx(33 / 72)
    assert parcels_12["curved_split_line_count"] == 12
    assert parcels_12["non_axis_aligned_split_segment_count"] == 24
    assert parcels_12["street_t_junction_count"] == 9
    assert parcels_12["accepted_streamline_continuation_split_count"] == 12
    assert parcels_12["candidate_split_reject_count"] == 60
    assert parcels_12["path_access_score_fallback_count"] == 3
    assert parcels_12["chen_fig7_short_shared_edge_candidate_count"] == 4
    assert parcels_12["chen_fig7_unexplained_interior_t_junction_count"] == 4
    assert (
        parcels_12[
            "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count"
        ]
        == 2
    )
    assert (
        parcels_12["chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count"]
        == 2
    )
    assert parcels_12["chen_fig7_unexplained_t_junction_split_endpoint_count"] == 2
    assert parcels_12["chen_fig7_unexplained_t_junction_lies_on_split_line_count"] == 2
    assert parcels_12["chen_fig7_unexplained_t_junction_split_unknown_count"] == 0
    assert parcels_12["chen_fig7_short_edge_cleanup_applied_count"] == 2
    assert parcels_12["chen_fig7_short_edge_cleanup_failed_count"] == 0
    assert parcels_12["chen_fig7_short_edge_cleanup_failed_unique_candidate_count"] == 0
    assert parcels_12["optimization_regularity_projected_parcel_count_before"] == 36
    assert parcels_12["optimization_regularity_skipped_parcel_count_before"] == 2
    assert parcels_12["optimization_regularity_skipped_by_reason_before"] == {
        "flipped_orientation": 2
    }
    assert parcels_12["optimization_regularity_projection_equation_count"] == 144
    assert parcels_12["optimization_regularity_projection_residual_before"] == 12.0

    oval_24_review = manifest["shapes"]["oval"]["24"]["review_metrics"]
    assert (
        oval_24_review["chen_fig7_short_edge_cleanup_failed_unique_candidate_stage"]
        == "groups_failed_attempts_by_parcel_pair_midpoint_and_path_geometry_v0"
    )
    assert oval_24_review[
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail"
    ] == {"failed_invalid_polygon": 1}
    assert oval_24_review["chen_fig7_short_edge_cleanup_failed_samples"][0][
        "failure_candidate_signature"
    ] == {
        "parcel_pair": [1, 2],
        "midpoint": [1.0, 0.0],
        "path_points": [[0.0, 0.0], [1.0, 0.0], [2.0, 0.0]],
    }


def test_scale_suite_subset_uses_canonical_shape_seed(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, int, int, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int,
        seed: int,
        width: float,
        height: float,
        streamline_mode: str = "baseline",
    ) -> GeneratedSuiteStub:
        calls.append((name, parcel_count, seed, streamline_mode))
        return GeneratedSuiteStub(
            name=name,
            parcel_count=parcel_count,
            seed=seed,
            streamline_mode=streamline_mode,
        )

    _install_fake_mapgen_modules(
        monkeypatch,
        generate_named_layout=generate_named_layout,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )

    runner = _load_scale_runner_module()

    manifest = runner.run_suite(
        out_dir=tmp_path,
        parcel_counts=(12,),
        seed=5,
        shapes=("oval",),
    )

    assert calls == [("oval", 12, 100_017, "baseline")]
    assert manifest["runs"][0]["seed"] == 100_017
    assert (
        manifest["parameters"]["seed_policy"]
        == "base_seed + canonical_shape_index * 100000 + parcel_count"
    )
    assert manifest["parameters"]["output_dir"] == str(tmp_path)
    assert manifest["parameters"]["streamline_mode"] == "baseline"
    assert manifest["limitations"] == [
        "grid_smooth_4rosy_laplace_v1_not_full_yang_global_solver",
        "bounded_junction_street_selection_v0_not_full_section_4_2_solver",
        "shapeop_like_projection_v1_not_exact_shapeop_solver",
    ]


@pytest.mark.parametrize(
    ("run_kwargs", "message"),
    [
        (
            {"parcel_counts": (12, 12), "shapes": ("square",)},
            "duplicate parcel count",
        ),
        (
            {"parcel_counts": (12,), "shapes": ("oval", "oval")},
            "duplicate Chen strict shape",
        ),
    ],
)
def test_scale_suite_rejects_duplicate_output_keys(
    tmp_path: Path, monkeypatch: Any, run_kwargs: dict[str, Any], message: str
) -> None:
    _install_fake_mapgen_modules(
        monkeypatch,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )
    runner = _load_scale_runner_module()
    out_dir = tmp_path / "suite"

    with pytest.raises(ValueError, match=message):
        runner.run_suite(out_dir=out_dir, **run_kwargs)

    assert not out_dir.exists()


def test_scale_suite_cli_defaults_are_stable(monkeypatch: Any) -> None:
    _install_fake_mapgen_modules(
        monkeypatch,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )
    runner = _load_scale_runner_module()

    args = runner.parse_args([])

    assert args.out_dir == runner.DEFAULT_OUT_DIR
    assert args.parcel_counts == [12, 24, 48, 96]
    assert args.shapes == ["square", "oval", "triangle"]
    assert args.seed == 0
    assert args.width == 180.0
    assert args.height == 140.0
    assert args.streamline_mode == "baseline"


def test_scale_suite_can_request_yang_streamline_mode(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, int, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int,
        seed: int,
        width: float,
        height: float,
        streamline_mode: str = "baseline",
    ) -> GeneratedSuiteStub:
        calls.append((name, parcel_count, streamline_mode))
        return GeneratedSuiteStub(
            name=name,
            parcel_count=parcel_count,
            seed=seed,
            streamline_mode=streamline_mode,
        )

    _install_fake_mapgen_modules(
        monkeypatch,
        generate_named_layout=generate_named_layout,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )

    runner = _load_scale_runner_module()

    manifest = runner.run_suite(
        out_dir=tmp_path,
        parcel_counts=(12,),
        seed=5,
        shapes=("oval",),
        streamline_mode=STREAMLINE_MODE_YANG_D_FIELD,
    )

    assert calls == [("oval", 12, STREAMLINE_MODE_YANG_D_FIELD)]
    assert manifest["parameters"]["streamline_mode"] == STREAMLINE_MODE_YANG_D_FIELD
    assert (
        "yang_d_field_candidates_mode_is_opt_in_and_still_approximate"
        in manifest["limitations"]
    )
    assert manifest["runs"][0]["review_metrics"]["streamline_config_mode"] == (
        STREAMLINE_MODE_YANG_D_FIELD
    )


def test_scale_suite_can_request_yang_b_field_streamline_mode(
    tmp_path: Path, monkeypatch: Any
) -> None:
    calls: list[tuple[str, int, str]] = []

    def generate_named_layout(
        name: str,
        parcel_count: int,
        seed: int,
        width: float,
        height: float,
        streamline_mode: str = "baseline",
    ) -> GeneratedSuiteStub:
        calls.append((name, parcel_count, streamline_mode))
        return GeneratedSuiteStub(
            name=name,
            parcel_count=parcel_count,
            seed=seed,
            streamline_mode=streamline_mode,
        )

    _install_fake_mapgen_modules(
        monkeypatch,
        generate_named_layout=generate_named_layout,
        write_strict_chen_artifacts=_write_fake_artifacts,
    )

    runner = _load_scale_runner_module()

    manifest = runner.run_suite(
        out_dir=tmp_path,
        parcel_counts=(12,),
        seed=5,
        shapes=("oval",),
        streamline_mode=STREAMLINE_MODE_YANG_B_FIELD,
    )

    assert calls == [("oval", 12, STREAMLINE_MODE_YANG_B_FIELD)]
    assert manifest["parameters"]["streamline_mode"] == STREAMLINE_MODE_YANG_B_FIELD
    assert (
        "yang_b_field_candidates_mode_is_opt_in_uniform_clipped_mesh_laplacian_"
        "omega_boundary_alignment_and_still_approximate"
    ) in manifest["limitations"]
    review = manifest["runs"][0]["review_metrics"]
    assert review["streamline_config_mode"] == STREAMLINE_MODE_YANG_B_FIELD
    assert review["streamline_config_field_mode"] == "yang_b_field"
    for key in (
        "accepted_streamline_continuation_split_count",
        "accepted_axis_fallback_split_count",
        "candidate_split_reject_count",
        "candidate_topology_reject_count",
        "path_access_score_count",
        "path_access_score_fallback_count",
        "chen_fig7_short_edge_cleanup_has_labeled_approximations",
        "chen_fig7_short_edge_cleanup_labeled_approximation_reasons",
        "chen_fig7_short_edge_cleanup_applied_count",
        "chen_fig7_short_edge_cleanup_midpoint_merge_count",
        "chen_fig7_short_edge_cleanup_boundary_projected_merge_count",
        "chen_fig7_short_edge_cleanup_merge_point_modes",
        "chen_fig7_short_edge_cleanup_boundary_projection_distance_max",
        "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_applied_count",
        "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_count",
        "chen_fig7_short_edge_cleanup_failed_count",
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_stage",
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_count",
        "chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count",
        "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail",
        "chen_fig7_short_edge_cleanup_failed_reasons",
        "chen_fig7_short_edge_cleanup_failed_details",
        "chen_fig7_short_edge_cleanup_failed_overlap_count",
        "chen_fig7_short_edge_cleanup_failed_invalid_polygon_count",
        "chen_fig7_short_edge_cleanup_failed_sliver_or_corner_loss_count",
        "chen_fig7_short_edge_cleanup_failed_boundary_coverage_count",
        "chen_fig7_short_edge_cleanup_failed_conforming_graph_count",
        "chen_fig7_short_edge_cleanup_failed_degenerate_ring_after_merge_count",
        (
            "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
            "non_candidate_parcel_ring_after_merge_count"
        ),
        "chen_fig7_short_edge_cleanup_failed_non_simple_ring_after_merge_count",
        "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_count",
        (
            "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_"
            "due_other_shared_edges_count"
        ),
        "chen_fig7_short_edge_cleanup_failed_nonlocal_neighbor_delta_count",
        "chen_fig7_short_edge_cleanup_skipped_boundary_count",
        "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count",
        "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count",
        "chen_fig7_unexplained_t_junction_split_endpoint_count",
        "chen_fig7_unexplained_t_junction_lies_on_split_line_count",
        "chen_fig7_unexplained_t_junction_split_unknown_count",
        "chen_fig7_unexplained_t_junction_split_endpoint_source_counts",
        "chen_fig7_unexplained_t_junction_lies_on_split_line_source_counts",
        "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_source_counts",
        "chen_fig7_unexplained_t_junction_split_endpoint_kinked_source_counts",
        (
            "chen_fig7_unexplained_t_junction_lies_on_split_line_"
            "straight_through_source_counts"
        ),
        "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_source_counts",
        (
            "chen_fig7_unexplained_t_junction_split_unknown_"
            "straight_through_source_counts"
        ),
        "chen_fig7_unexplained_t_junction_split_unknown_kinked_source_counts",
    ):
        assert key in review
    for key in OPTIMIZATION_REGULARITY_REVIEW_KEYS:
        assert key in review
    assert review["accepted_streamline_field_b_boundary_alignment_weight_mean"] == 0.9
    assert review["accepted_streamline_trace_mesh_interior_seed_count_max"] > 0
    assert (
        review["optimization_regularity_projection_kind"]
        == "regular_polygon_similarity_transform_v0"
    )
    assert review["optimization_regularity_projected_parcel_count_before"] == 12
    assert review["optimization_regularity_skipped_parcel_count_before"] == 1
    assert review["optimization_regularity_skipped_by_reason_before"] == {
        "flipped_orientation": 1
    }
    assert review["optimization_regularity_projection_equation_count"] == 48
    assert review["optimization_regularity_projection_residual_before"] == 12.0
    assert (
        manifest["aggregate_metrics"]["overall"][
            "accepted_streamline_field_b_boundary_alignment_weight_max"
        ]
        == 0.9
    )
    assert (
        manifest["aggregate_metrics"]["overall"][
            "accepted_streamline_continuation_split_count"
        ]
        == 6
    )
    assert (
        manifest["aggregate_metrics"]["overall"]["candidate_topology_reject_count"] == 3
    )
    assert (
        manifest["aggregate_metrics"]["overall"][
            "chen_fig7_short_edge_cleanup_applied_count"
        ]
        == 1
    )
    assert (
        manifest["aggregate_metrics"]["overall"][
            "chen_fig7_short_edge_cleanup_failed_count"
        ]
        == 0
    )
    assert (
        manifest["aggregate_metrics"]["overall"][
            "optimization_regularity_projected_parcel_count_before"
        ]
        == 12
    )
    assert (
        manifest["aggregate_metrics"]["overall"][
            "optimization_regularity_skipped_parcel_count_before"
        ]
        == 1
    )
    assert manifest["aggregate_metrics"]["overall"][
        "optimization_regularity_skipped_by_reason_before"
    ] == {"flipped_orientation": 1}
    assert (
        manifest["aggregate_metrics"]["overall"][
            "optimization_regularity_projection_equation_count"
        ]
        == 48
    )
    assert (
        manifest["aggregate_metrics"]["overall"][
            "optimization_regularity_projection_target_displacement_max_before"
        ]
        == 2.4
    )


def _write_fake_artifacts(
    generated: GeneratedSuiteStub, out_dir: Path
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "manifest.json").write_text("{}\n")
    is_square = generated.name == "square"
    is_oval = generated.name == "oval"
    is_d_field = generated.streamline_mode == STREAMLINE_MODE_YANG_D_FIELD
    is_b_field = generated.streamline_mode == STREAMLINE_MODE_YANG_B_FIELD
    is_yang = is_d_field or is_b_field
    accepted_streamline_count = 0 if is_square else generated.parcel_count // 2
    fig7_candidate_count = 0 if is_square else generated.parcel_count // 6
    fig7_raw_t_count = 0 if is_square else generated.parcel_count // 4
    fig7_attached_t_count = 0 if is_square else generated.parcel_count // 8
    fig7_unexplained_t_count = fig7_raw_t_count - fig7_attached_t_count
    fig7_kinked_t_count = 0 if is_square else 1
    fig7_straight_t_count = max(fig7_unexplained_t_count - fig7_kinked_t_count, 0)
    fig7_endpoint_t_count = fig7_straight_t_count
    fig7_on_line_t_count = fig7_kinked_t_count
    fig7_unknown_t_count = max(
        fig7_unexplained_t_count - fig7_endpoint_t_count - fig7_on_line_t_count,
        0,
    )
    fig7_cleanup_applied_count = 0 if is_square else generated.parcel_count // 12
    fig7_cleanup_failed_count = 0 if is_square else generated.parcel_count // 24
    fig7_cleanup_failed_unique_candidate_count = min(fig7_cleanup_failed_count, 1)
    failed_sample = {
        "edge": (1, 2),
        "parcels": (1, 2),
        "failure_reason": "failed_invalid_polygon",
        "failure_detail": "failed_invalid_polygon",
        "failure_candidate_signature": {
            "parcel_pair": (1, 2),
            "midpoint": (1.0, 0.0),
            "path_points": ((0.0, 0.0), (1.0, 0.0), (2.0, 0.0)),
        },
        "failure_validity_reason": "Self-intersection[1 0]",
    }
    failed_samples = [] if fig7_cleanup_failed_count == 0 else [failed_sample]
    fig7_cleanup_skipped_boundary_count = (
        0 if is_square else generated.parcel_count // 24
    )
    regularity_skipped_count = 0 if is_square else 1
    regularity_skipped_reasons = (
        {} if is_square else {"flipped_orientation": regularity_skipped_count}
    )
    regularity_residual_before = float(generated.parcel_count)
    regularity_rms_before = float(generated.parcel_count) / 10.0
    regularity_max_before = float(generated.parcel_count) / 5.0
    return {
        "files": {
            "manifest": "manifest.json",
            "metrics": "layout_metrics.json",
            "svg": "chen_strict_layout.svg",
            "png": "chen_strict_layout.png",
            "boundary": "boundary.geojson",
            "parcels": "parcels.geojson",
            "streets": "streets.geojson",
            "split_lines": "split_lines.geojson",
        },
        "summary": {
            "parcel_count": generated.parcel_count,
            "street_edge_count": generated.parcel_count - 1,
            "corner_graph_edge_count": generated.parcel_count * 2,
            "street_edge_density": (generated.parcel_count - 1)
            / (generated.parcel_count * 2),
            "split_line_count": generated.parcel_count - 1,
            "axis_aligned_split_line_count": generated.parcel_count - 1
            if is_square
            else 0,
            "curved_split_line_count": generated.parcel_count if is_oval else 0,
            "non_axis_aligned_split_segment_count": 0
            if is_square
            else generated.parcel_count,
            "non_axis_aligned_street_segment_count": 0
            if is_square
            else generated.parcel_count,
            "streamline_candidate_count": 0 if is_square else generated.parcel_count,
            "accepted_streamline_candidate_count": accepted_streamline_count,
            "accepted_streamline_continuation_split_count": 0
            if is_square
            else generated.parcel_count // 2,
            "accepted_axis_fallback_split_count": 1 if is_square else 0,
            "candidate_split_reject_count": generated.parcel_count
            if is_square
            else generated.parcel_count * 2,
            "candidate_topology_reject_count": 0
            if is_square
            else generated.parcel_count // 4,
            "path_access_score_count": generated.parcel_count * 2,
            "path_access_score_fallback_count": 1,
            "accepted_streamline_field_mode_counts": {
                (
                    "yang_b_field"
                    if is_b_field
                    else "yang_d_field"
                    if is_d_field
                    else "grid_smooth"
                ): accepted_streamline_count
            },
            "accepted_streamline_trace_seed_source_counts": {
                ("yang_mesh_vertices" if is_yang else "grid"): accepted_streamline_count
            },
            "accepted_streamline_score_mode_counts": {
                (
                    "yang_div_db_ds_ct" if is_yang else "heuristic"
                ): accepted_streamline_count
            },
            "accepted_streamline_yang_score_count": accepted_streamline_count
            if is_yang
            else 0,
            "accepted_streamline_score_div_mean": 0.0,
            "accepted_streamline_score_total_normalized_max": 0.0,
            "accepted_streamline_trace_mesh_interior_seed_count_min": 0.0
            if is_square or not is_yang
            else 4.0,
            "accepted_streamline_trace_mesh_interior_seed_count_mean": 0.0
            if is_square or not is_yang
            else 5.0,
            "accepted_streamline_trace_mesh_interior_seed_count_max": 0.0
            if is_square or not is_yang
            else 6.0,
            "accepted_streamline_field_mesh_kinds": []
            if not is_yang
            else ["scipy_delaunay_clipped"],
            "accepted_streamline_field_mesh_vertex_count_max": 0.0
            if not is_yang
            else 21.0,
            "accepted_streamline_field_mesh_retained_vertex_count_max": 0.0
            if not is_yang
            else 19.0,
            "accepted_streamline_field_mesh_triangle_count_max": 0.0
            if not is_yang
            else 30.0,
            "accepted_streamline_field_b_approximation_scopes": []
            if not is_b_field
            else [
                "yang2013_supp1_sec4_3_uniform_mesh_graph_laplacian_boundary_vertices"
            ],
            "accepted_streamline_field_b_boundary_anchor_methods": []
            if not is_b_field
            else ["boundary_samples_with_polygon_vertices_averaged_incident_edges"],
            "accepted_streamline_field_b_boundary_alignment_weight_mean": 0.9
            if is_b_field
            else 0.0,
            "accepted_streamline_field_b_boundary_alignment_weight_max": 0.9
            if is_b_field
            else 0.0,
            "accepted_streamline_field_b_boundary_anchor_count_max": 9.0
            if is_b_field
            else 0.0,
            "accepted_streamline_field_b_boundary_alignment_error_mean": 0.2
            if is_b_field
            else 0.0,
            "accepted_streamline_field_b_boundary_alignment_error_max": 0.25
            if is_b_field
            else 0.0,
            "accepted_streamline_field_b_smoothness_energy_mean": 0.4
            if is_b_field
            else 0.0,
            "accepted_streamline_field_b_smoothness_energy_max": 0.5
            if is_b_field
            else 0.0,
            "accepted_streamline_field_b_solver_residual_max": 0.001
            if is_b_field
            else 0.0,
            "streamline_config_mode": generated.streamline_mode,
            "streamline_config_field_mode": "yang_b_field"
            if is_b_field
            else "yang_d_field"
            if is_d_field
            else "default",
            "corner_graph_t_junction_count": 2,
            "corner_graph_four_way_intersection_count": 1,
            "street_t_junction_count": 3,
            "street_four_way_intersection_count": 2,
            "chen_fig7_short_edge_detection_stage": (
                "chen_section_4_1_shared_edge_threshold_v0"
            ),
            "chen_fig7_short_edge_cleanup_stage": "diagnostic_only_v0",
            "chen_fig7_short_edge_cleanup_applied": bool(fig7_cleanup_applied_count),
            "chen_fig7_short_edge_cleanup_scope": (
                "classifies_shared_corner_graph_edges_below_0_2_avg_approx_side_"
                "length_without_vertex_merge"
            ),
            "chen_fig7_short_edge_cleanup_blocking_reason": (
                "midpoint_vertex_merge_and_partition_line_interpolation_not_implemented"
            ),
            "chen_fig7_short_edge_cleanup_applied_count": (fig7_cleanup_applied_count),
            "chen_fig7_short_edge_cleanup_midpoint_merge_count": (
                fig7_cleanup_applied_count
            ),
            "chen_fig7_short_edge_cleanup_boundary_projected_merge_count": 0,
            "chen_fig7_short_edge_cleanup_merge_point_modes": []
            if fig7_cleanup_applied_count == 0
            else ["midpoint"],
            "chen_fig7_short_edge_cleanup_boundary_projection_distance_min": 0.0,
            "chen_fig7_short_edge_cleanup_boundary_projection_distance_mean": 0.0,
            "chen_fig7_short_edge_cleanup_boundary_projection_distance_max": 0.0,
            "chen_fig7_short_edge_cleanup_failed_count": fig7_cleanup_failed_count,
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_stage": (
                "groups_failed_attempts_by_parcel_pair_midpoint_and_path_geometry_v0"
            ),
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_count": (
                fig7_cleanup_failed_unique_candidate_count
            ),
            "chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count": (
                fig7_cleanup_failed_count - fig7_cleanup_failed_unique_candidate_count
            ),
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail": (
                {}
                if fig7_cleanup_failed_unique_candidate_count == 0
                else {
                    "failed_invalid_polygon": fig7_cleanup_failed_unique_candidate_count
                }
            ),
            "chen_fig7_short_edge_cleanup_failed_reasons": []
            if fig7_cleanup_failed_count == 0
            else ["failed_invalid_polygon"],
            "chen_fig7_short_edge_cleanup_failed_overlap_count": 0,
            "chen_fig7_short_edge_cleanup_failed_invalid_polygon_count": (
                fig7_cleanup_failed_count
            ),
            "chen_fig7_short_edge_cleanup_failed_sliver_or_corner_loss_count": 0,
            "chen_fig7_short_edge_cleanup_failed_boundary_coverage_count": 0,
            "chen_fig7_short_edge_cleanup_failed_conforming_graph_count": 0,
            (
                "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
                "non_candidate_parcel_ring_after_merge_count"
            ): fig7_cleanup_failed_count,
            "chen_fig7_short_edge_cleanup_skipped_boundary_count": (
                fig7_cleanup_skipped_boundary_count
            ),
            "chen_fig7_short_edge_cleanup_failed_samples": failed_samples,
            "chen_fig7_short_edge_cleanup_failed_samples_by_detail": {}
            if not failed_samples
            else {"failed_invalid_polygon": failed_samples},
            "chen_fig7_short_edge_cleanup_pre_candidate_count": fig7_candidate_count,
            "chen_fig7_short_edge_cleanup_post_candidate_count": max(
                fig7_candidate_count - fig7_cleanup_applied_count,
                0,
            ),
            "chen_fig7_short_edge_cleanup_pre_attached_t_junction_count": (
                fig7_attached_t_count
            ),
            "chen_fig7_short_edge_cleanup_post_attached_t_junction_count": max(
                fig7_attached_t_count - fig7_cleanup_applied_count,
                0,
            ),
            "chen_fig7_short_edge_cleanup_pre_unexplained_t_junction_count": (
                fig7_unexplained_t_count
            ),
            "chen_fig7_short_edge_cleanup_post_unexplained_t_junction_count": (
                fig7_unexplained_t_count
            ),
            "chen_fig7_short_shared_edge_candidate_count": fig7_candidate_count,
            "chen_fig7_short_shared_edge_length_min": 0.0 if is_square else 1.0,
            "chen_fig7_short_shared_edge_length_mean": 0.0 if is_square else 1.5,
            "chen_fig7_short_shared_edge_length_max": 0.0 if is_square else 2.0,
            "chen_fig7_short_shared_edge_threshold_max": 0.0 if is_square else 4.0,
            "chen_fig7_short_shared_edge_length_threshold_ratio_max": 0.0
            if is_square
            else 0.5,
            "chen_fig7_raw_interior_t_junction_count": fig7_raw_t_count,
            "chen_fig7_short_edge_attached_interior_t_junction_count": (
                fig7_attached_t_count
            ),
            "chen_fig7_unexplained_interior_t_junction_count": (
                fig7_unexplained_t_count
            ),
            "chen_fig7_unexplained_t_junction_classification_stage": (
                "chen_section_4_parcel_corner_collinearity_135deg_v0"
            ),
            "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count": (
                fig7_straight_t_count
            ),
            "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count": (
                fig7_kinked_t_count
            ),
            "chen_fig7_unexplained_t_junction_split_provenance_stage": (
                "accepted_split_line_geometry_endpoint_or_interior_v0"
            ),
            "chen_fig7_unexplained_t_junction_split_provenance_scope": (
                "geometric_attribution_only_not_paper_violation_proof"
            ),
            "chen_fig7_unexplained_t_junction_split_provenance_tolerance": 1e-5,
            "chen_fig7_unexplained_t_junction_split_endpoint_count": (
                fig7_endpoint_t_count
            ),
            "chen_fig7_unexplained_t_junction_lies_on_split_line_count": (
                fig7_on_line_t_count
            ),
            "chen_fig7_unexplained_t_junction_split_unknown_count": (
                fig7_unknown_t_count
            ),
            "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_count": (
                fig7_endpoint_t_count
            ),
            "chen_fig7_unexplained_t_junction_split_endpoint_kinked_count": 0,
            (
                "chen_fig7_unexplained_t_junction_lies_on_split_line_"
                "straight_through_count"
            ): 0,
            "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_count": (
                fig7_on_line_t_count
            ),
            "chen_fig7_unexplained_t_junction_split_unknown_straight_through_count": 0,
            "chen_fig7_unexplained_t_junction_split_unknown_kinked_count": 0,
            "chen_fig7_unexplained_t_junction_split_endpoint_source_counts": {}
            if is_square
            else {"streamline_continuation": fig7_endpoint_t_count},
            "chen_fig7_unexplained_t_junction_lies_on_split_line_source_counts": {}
            if is_square
            else {"streamline": fig7_on_line_t_count},
            "chen_fig7_short_shared_edge_samples": []
            if is_square
            else [
                {
                    "edge": (1, 2),
                    "parcels": (1, 2),
                    "path_length": 2.0,
                    "threshold": 4.0,
                    "midpoint": (1.0, 0.0),
                    "path_points_sample": ((0.0, 0.0), (1.0, 0.0), (2.0, 0.0)),
                }
            ],
            "chen_fig7_short_edge_attached_interior_t_junction_points_sample": (
                [] if is_square else [(1.0, 0.0)]
            ),
            "chen_fig7_unexplained_interior_t_junction_points_sample": (
                [] if is_square else [(8.0, 0.0)]
            ),
            (
                "chen_fig7_unexplained_straight_through_side_insertion_"
                "t_junction_points_sample"
            ): ([] if is_square else [(8.0, 0.0)]),
            (
                "chen_fig7_unexplained_kinked_split_topology_debt_"
                "t_junction_points_sample"
            ): ([] if is_square else [(9.0, 1.0)]),
            "chen_fig7_unexplained_t_junction_split_endpoint_samples": []
            if is_square
            else [{"point": (8.0, 0.0), "kind": "endpoint"}],
            "chen_fig7_unexplained_t_junction_lies_on_split_line_samples": []
            if is_square
            else [{"point": (9.0, 1.0), "kind": "on_line"}],
            "chen_fig7_unexplained_t_junction_split_unknown_samples": [],
            "street_topology_reachability_pass": True,
            "implementation_stage": (
                "chen_grid_smooth_streamline_bounded_junction_shapeop_like_v1"
            ),
            "streamline_field_stage": "grid_smooth_4rosy_laplace_v1",
            "streamline_field_scope": "compact_grid_smooth_not_full_yang_solver",
            "street_selection_mode": (
                "chen_section_4_2_reachability_bounded_junctions_v0"
            ),
            "chen_street_generation_scope": (
                "reachability_plus_bounded_junction_completion_v0"
            ),
            "optimization_stage": "chen_section_5_shapeop_like_projection_v1",
            "optimization_applied": True,
            "optimization_geometry_changed": not is_square,
            "optimization_accepted_iteration_count": 4,
            "optimization_energy_before": 10.0,
            "optimization_energy_after": 8.0,
            "optimization_regularity_projection_kind": (
                "regular_polygon_similarity_transform_v0"
            ),
            "optimization_regularity_projected_parcel_count_before": (
                generated.parcel_count
            ),
            "optimization_regularity_projected_parcel_count_after": (
                generated.parcel_count
            ),
            "optimization_regularity_skipped_parcel_count_before": (
                regularity_skipped_count
            ),
            "optimization_regularity_skipped_parcel_count_after": (
                regularity_skipped_count
            ),
            "optimization_regularity_skipped_by_reason_before": (
                regularity_skipped_reasons
            ),
            "optimization_regularity_skipped_by_reason_after": (
                regularity_skipped_reasons
            ),
            "optimization_regularity_projection_equation_count": (
                generated.parcel_count * 4
            ),
            "optimization_regularity_projection_residual_before": (
                regularity_residual_before
            ),
            "optimization_regularity_projection_residual_after": (
                regularity_residual_before / 2.0
            ),
            "optimization_regularity_projection_target_displacement_rms_before": (
                regularity_rms_before
            ),
            "optimization_regularity_projection_target_displacement_rms_after": (
                regularity_rms_before / 2.0
            ),
            "optimization_regularity_projection_target_displacement_max_before": (
                regularity_max_before
            ),
            "optimization_regularity_projection_target_displacement_max_after": (
                regularity_max_before / 2.0
            ),
            "paper_invariant_pass": True,
            "geometry_valid_pass": True,
        },
    }


def _install_fake_mapgen_modules(
    monkeypatch: Any,
    *,
    write_strict_chen_artifacts: Any,
    generate_named_layout: Any | None = None,
) -> None:
    fake_package = types.ModuleType("mapgen")
    fake_package.__path__ = []
    fake_artifacts = types.ModuleType("mapgen.chen_artifacts")
    fake_artifacts.write_strict_chen_artifacts = write_strict_chen_artifacts
    monkeypatch.setitem(sys.modules, "mapgen", fake_package)
    monkeypatch.setitem(sys.modules, "mapgen.chen_artifacts", fake_artifacts)

    if generate_named_layout is not None:
        fake_generate = types.ModuleType("mapgen.chen_generate")
        fake_generate.generate_named_layout = generate_named_layout
        monkeypatch.setitem(sys.modules, "mapgen.chen_generate", fake_generate)


def _load_scale_runner_module() -> Any:
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_chen_strict_scale_suite.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_chen_strict_scale_suite_test", path
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module
