#!/usr/bin/env python
"""Run strict Chen generation for paper-style shape/scale artifacts."""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from mapgen.chen_artifacts import write_strict_chen_artifacts

SHAPES = ("square", "oval", "triangle")
STREAMLINE_MODES = (
    "baseline",
    "yang_d_field_candidates",
    "yang_b_field_candidates",
)
DEFAULT_PARCEL_COUNTS = (12, 24, 48, 96)
DEFAULT_OUT_DIR = (
    Path(__file__).resolve().parents[1] / "artifacts" / "chen_strict_scale_suite"
)

REVIEW_METRIC_KEYS = (
    "parcel_count",
    "street_edge_count",
    "corner_graph_edge_count",
    "street_edge_density",
    "split_line_count",
    "axis_aligned_split_line_count",
    "curved_split_line_count",
    "non_axis_aligned_split_segment_count",
    "non_axis_aligned_street_segment_count",
    "max_mesh_axis_deviation",
    "max_split_axis_deviation",
    "max_street_axis_deviation",
    "mean_mesh_axis_deviation",
    "mean_split_axis_deviation",
    "mean_street_axis_deviation",
    "streamline_candidate_count",
    "accepted_streamline_candidate_count",
    "accepted_streamline_continuation_split_count",
    "accepted_axis_fallback_split_count",
    "candidate_split_reject_count",
    "candidate_topology_reject_count",
    "path_access_score_count",
    "path_access_score_fallback_count",
    "accepted_streamline_field_mode_counts",
    "accepted_streamline_trace_seed_source_counts",
    "accepted_streamline_score_mode_counts",
    "accepted_streamline_yang_score_count",
    "accepted_streamline_score_approximation_scopes",
    "accepted_streamline_score_div_min",
    "accepted_streamline_score_div_mean",
    "accepted_streamline_score_div_max",
    "accepted_streamline_score_db_min",
    "accepted_streamline_score_db_mean",
    "accepted_streamline_score_db_max",
    "accepted_streamline_score_ds_min",
    "accepted_streamline_score_ds_mean",
    "accepted_streamline_score_ds_max",
    "accepted_streamline_score_ct_min",
    "accepted_streamline_score_ct_mean",
    "accepted_streamline_score_ct_max",
    "accepted_streamline_score_total_normalized_min",
    "accepted_streamline_score_total_normalized_mean",
    "accepted_streamline_score_total_normalized_max",
    "accepted_streamline_trace_mesh_interior_seed_count_min",
    "accepted_streamline_trace_mesh_interior_seed_count_mean",
    "accepted_streamline_trace_mesh_interior_seed_count_max",
    "accepted_streamline_field_mesh_kinds",
    "accepted_streamline_field_mesh_vertex_count_min",
    "accepted_streamline_field_mesh_vertex_count_mean",
    "accepted_streamline_field_mesh_vertex_count_max",
    "accepted_streamline_field_mesh_retained_vertex_count_min",
    "accepted_streamline_field_mesh_retained_vertex_count_mean",
    "accepted_streamline_field_mesh_retained_vertex_count_max",
    "accepted_streamline_field_mesh_triangle_count_min",
    "accepted_streamline_field_mesh_triangle_count_mean",
    "accepted_streamline_field_mesh_triangle_count_max",
    "accepted_streamline_field_b_approximation_scopes",
    "accepted_streamline_field_b_boundary_anchor_methods",
    "accepted_streamline_field_b_boundary_alignment_weight_min",
    "accepted_streamline_field_b_boundary_alignment_weight_mean",
    "accepted_streamline_field_b_boundary_alignment_weight_max",
    "accepted_streamline_field_b_boundary_anchor_count_min",
    "accepted_streamline_field_b_boundary_anchor_count_mean",
    "accepted_streamline_field_b_boundary_anchor_count_max",
    "accepted_streamline_field_b_boundary_alignment_error_min",
    "accepted_streamline_field_b_boundary_alignment_error_mean",
    "accepted_streamline_field_b_boundary_alignment_error_max",
    "accepted_streamline_field_b_smoothness_energy_min",
    "accepted_streamline_field_b_smoothness_energy_mean",
    "accepted_streamline_field_b_smoothness_energy_max",
    "accepted_streamline_field_b_solver_residual_max",
    "implementation_stage",
    "streamline_field_stage",
    "streamline_field_scope",
    "streamline_config_mode",
    "streamline_config_field_mode",
    "streamline_config_candidate_seed_mode",
    "streamline_config_score_mode",
    "street_selection_mode",
    "chen_street_generation_scope",
    "corner_graph_t_junction_count",
    "corner_graph_four_way_intersection_count",
    "corner_graph_t_junction_ratio",
    "corner_graph_four_way_intersection_ratio",
    "interior_corner_graph_t_junction_count",
    "interior_corner_graph_four_way_intersection_count",
    "boundary_corner_graph_t_junction_count",
    "boundary_corner_graph_four_way_intersection_count",
    "street_t_junction_count",
    "street_four_way_intersection_count",
    "street_t_junction_ratio",
    "street_four_way_intersection_ratio",
    "interior_street_t_junction_count",
    "interior_street_four_way_intersection_count",
    "boundary_street_t_junction_count",
    "boundary_street_four_way_intersection_count",
    "rectangular_interior_t_junction_points_sample",
    "chen_fig7_short_edge_detection_stage",
    "chen_fig7_short_edge_cleanup_stage",
    "chen_fig7_short_edge_cleanup_applied",
    "chen_fig7_short_edge_cleanup_scope",
    "chen_fig7_short_edge_cleanup_blocking_reason",
    "chen_fig7_short_edge_cleanup_has_labeled_approximations",
    "chen_fig7_short_edge_cleanup_labeled_approximation_reasons",
    "chen_fig7_short_edge_cleanup_applied_count",
    "chen_fig7_short_edge_cleanup_midpoint_merge_count",
    "chen_fig7_short_edge_cleanup_boundary_projected_merge_count",
    "chen_fig7_short_edge_cleanup_merge_point_modes",
    "chen_fig7_short_edge_cleanup_boundary_projection_distance_min",
    "chen_fig7_short_edge_cleanup_boundary_projection_distance_mean",
    "chen_fig7_short_edge_cleanup_boundary_projection_distance_max",
    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_stage",
    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_applied_count",
    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_count",
    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_ids_sample",
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
    "chen_fig7_short_edge_cleanup_pre_candidate_count",
    "chen_fig7_short_edge_cleanup_post_candidate_count",
    "chen_fig7_short_edge_cleanup_pre_attached_t_junction_count",
    "chen_fig7_short_edge_cleanup_post_attached_t_junction_count",
    "chen_fig7_short_edge_cleanup_pre_unexplained_t_junction_count",
    "chen_fig7_short_edge_cleanup_post_unexplained_t_junction_count",
    "chen_fig7_short_edge_cleanup_failed_samples",
    "chen_fig7_short_edge_cleanup_failed_samples_by_detail",
    "chen_fig7_short_shared_edge_candidate_count",
    "chen_fig7_short_shared_edge_length_min",
    "chen_fig7_short_shared_edge_length_mean",
    "chen_fig7_short_shared_edge_length_max",
    "chen_fig7_short_shared_edge_threshold_max",
    "chen_fig7_short_shared_edge_length_threshold_ratio_max",
    "chen_fig7_raw_interior_t_junction_count",
    "chen_fig7_short_edge_attached_interior_t_junction_count",
    "chen_fig7_unexplained_interior_t_junction_count",
    "chen_fig7_unexplained_t_junction_classification_stage",
    "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count",
    "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count",
    "chen_fig7_unexplained_t_junction_split_provenance_stage",
    "chen_fig7_unexplained_t_junction_split_provenance_scope",
    "chen_fig7_unexplained_t_junction_split_provenance_tolerance",
    "chen_fig7_unexplained_t_junction_split_endpoint_count",
    "chen_fig7_unexplained_t_junction_lies_on_split_line_count",
    "chen_fig7_unexplained_t_junction_split_unknown_count",
    "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_count",
    "chen_fig7_unexplained_t_junction_split_endpoint_kinked_count",
    "chen_fig7_unexplained_t_junction_lies_on_split_line_straight_through_count",
    "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_count",
    "chen_fig7_unexplained_t_junction_split_unknown_straight_through_count",
    "chen_fig7_unexplained_t_junction_split_unknown_kinked_count",
    "chen_fig7_unexplained_t_junction_split_endpoint_source_counts",
    "chen_fig7_unexplained_t_junction_lies_on_split_line_source_counts",
    "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_source_counts",
    "chen_fig7_unexplained_t_junction_split_endpoint_kinked_source_counts",
    (
        "chen_fig7_unexplained_t_junction_lies_on_split_line_"
        "straight_through_source_counts"
    ),
    "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_source_counts",
    "chen_fig7_unexplained_t_junction_split_unknown_straight_through_source_counts",
    "chen_fig7_unexplained_t_junction_split_unknown_kinked_source_counts",
    "chen_fig7_short_shared_edge_samples",
    "chen_fig7_short_edge_attached_interior_t_junction_points_sample",
    "chen_fig7_unexplained_interior_t_junction_points_sample",
    "chen_fig7_unexplained_straight_through_side_insertion_t_junction_points_sample",
    "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_points_sample",
    "chen_fig7_unexplained_t_junction_split_endpoint_samples",
    "chen_fig7_unexplained_t_junction_lies_on_split_line_samples",
    "chen_fig7_unexplained_t_junction_split_unknown_samples",
    "street_topology_reachability_pass",
    "optimization_stage",
    "optimization_applied",
    "optimization_geometry_changed",
    "optimization_accepted_iteration_count",
    "optimization_energy_before",
    "optimization_energy_after",
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
    "paper_invariant_pass",
    "geometry_valid_pass",
)

SUMMED_AGGREGATE_KEYS = (
    "street_edge_count",
    "corner_graph_edge_count",
    "curved_split_line_count",
    "non_axis_aligned_split_segment_count",
    "non_axis_aligned_street_segment_count",
    "corner_graph_t_junction_count",
    "corner_graph_four_way_intersection_count",
    "interior_corner_graph_t_junction_count",
    "interior_corner_graph_four_way_intersection_count",
    "boundary_corner_graph_t_junction_count",
    "boundary_corner_graph_four_way_intersection_count",
    "street_t_junction_count",
    "street_four_way_intersection_count",
    "interior_street_t_junction_count",
    "interior_street_four_way_intersection_count",
    "boundary_street_t_junction_count",
    "boundary_street_four_way_intersection_count",
    "accepted_streamline_candidate_count",
    "accepted_streamline_continuation_split_count",
    "accepted_axis_fallback_split_count",
    "candidate_split_reject_count",
    "candidate_topology_reject_count",
    "path_access_score_count",
    "path_access_score_fallback_count",
    "accepted_streamline_yang_score_count",
    "chen_fig7_short_edge_cleanup_applied_count",
    "chen_fig7_short_edge_cleanup_midpoint_merge_count",
    "chen_fig7_short_edge_cleanup_boundary_projected_merge_count",
    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_applied_count",
    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_count",
    "chen_fig7_short_edge_cleanup_failed_count",
    "chen_fig7_short_edge_cleanup_failed_unique_candidate_count",
    "chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count",
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
    "chen_fig7_short_edge_cleanup_pre_candidate_count",
    "chen_fig7_short_edge_cleanup_post_candidate_count",
    "chen_fig7_short_edge_cleanup_pre_attached_t_junction_count",
    "chen_fig7_short_edge_cleanup_post_attached_t_junction_count",
    "chen_fig7_short_edge_cleanup_pre_unexplained_t_junction_count",
    "chen_fig7_short_edge_cleanup_post_unexplained_t_junction_count",
    "chen_fig7_short_shared_edge_candidate_count",
    "chen_fig7_raw_interior_t_junction_count",
    "chen_fig7_short_edge_attached_interior_t_junction_count",
    "chen_fig7_unexplained_interior_t_junction_count",
    "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count",
    "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count",
    "chen_fig7_unexplained_t_junction_split_endpoint_count",
    "chen_fig7_unexplained_t_junction_lies_on_split_line_count",
    "chen_fig7_unexplained_t_junction_split_unknown_count",
    "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_count",
    "chen_fig7_unexplained_t_junction_split_endpoint_kinked_count",
    "chen_fig7_unexplained_t_junction_lies_on_split_line_straight_through_count",
    "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_count",
    "chen_fig7_unexplained_t_junction_split_unknown_straight_through_count",
    "chen_fig7_unexplained_t_junction_split_unknown_kinked_count",
    "optimization_regularity_projected_parcel_count_before",
    "optimization_regularity_projected_parcel_count_after",
    "optimization_regularity_skipped_parcel_count_before",
    "optimization_regularity_skipped_parcel_count_after",
    "optimization_regularity_projection_equation_count",
)

MAX_AGGREGATE_KEYS = (
    "max_mesh_axis_deviation",
    "max_split_axis_deviation",
    "max_street_axis_deviation",
    "accepted_streamline_score_div_max",
    "accepted_streamline_score_db_max",
    "accepted_streamline_score_ds_max",
    "accepted_streamline_score_ct_max",
    "accepted_streamline_score_total_normalized_max",
    "accepted_streamline_trace_mesh_interior_seed_count_max",
    "accepted_streamline_field_mesh_vertex_count_max",
    "accepted_streamline_field_mesh_retained_vertex_count_max",
    "accepted_streamline_field_mesh_triangle_count_max",
    "accepted_streamline_field_b_boundary_alignment_weight_max",
    "accepted_streamline_field_b_boundary_anchor_count_max",
    "accepted_streamline_field_b_boundary_alignment_error_max",
    "accepted_streamline_field_b_smoothness_energy_max",
    "accepted_streamline_field_b_solver_residual_max",
    "chen_fig7_short_shared_edge_length_max",
    "chen_fig7_short_shared_edge_threshold_max",
    "chen_fig7_short_shared_edge_length_threshold_ratio_max",
    "chen_fig7_short_edge_cleanup_boundary_projection_distance_max",
    "optimization_regularity_projection_residual_before",
    "optimization_regularity_projection_residual_after",
    "optimization_regularity_projection_target_displacement_rms_before",
    "optimization_regularity_projection_target_displacement_rms_after",
    "optimization_regularity_projection_target_displacement_max_before",
    "optimization_regularity_projection_target_displacement_max_after",
)

DICT_COUNT_AGGREGATE_KEYS = (
    "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail",
    "optimization_regularity_skipped_by_reason_before",
    "optimization_regularity_skipped_by_reason_after",
    "chen_fig7_unexplained_t_junction_split_endpoint_source_counts",
    "chen_fig7_unexplained_t_junction_lies_on_split_line_source_counts",
    "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_source_counts",
    "chen_fig7_unexplained_t_junction_split_endpoint_kinked_source_counts",
    (
        "chen_fig7_unexplained_t_junction_lies_on_split_line_"
        "straight_through_source_counts"
    ),
    "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_source_counts",
    "chen_fig7_unexplained_t_junction_split_unknown_straight_through_source_counts",
    "chen_fig7_unexplained_t_junction_split_unknown_kinked_source_counts",
)


def _load_generate_named_layout() -> Callable[..., Any]:
    try:
        from mapgen.chen_generate import generate_named_layout
    except ModuleNotFoundError as exc:
        if exc.name == "mapgen.chen_generate":
            raise SystemExit(
                "mapgen.chen_generate is not available yet; run this after the "
                "strict Chen generation slice lands."
            ) from exc
        raise
    return generate_named_layout


def seed_for_run(*, base_seed: int, shape: str, parcel_count: int) -> int:
    """Return the stable per-shape/per-count deterministic seed."""
    try:
        shape_index = SHAPES.index(shape)
    except ValueError as exc:
        raise ValueError(f"unknown Chen strict shape: {shape}") from exc
    return int(base_seed) + int(shape_index) * 100_000 + int(parcel_count)


def run_suite(
    *,
    out_dir: Path,
    parcel_counts: Iterable[int] = DEFAULT_PARCEL_COUNTS,
    seed: int = 0,
    width: float = 180.0,
    height: float = 140.0,
    shapes: Sequence[str] = SHAPES,
    streamline_mode: str = "baseline",
) -> dict[str, Any]:
    counts = _validated_counts(parcel_counts)
    shape_names = _validated_shapes(shapes)
    generate_named_layout = _load_generate_named_layout()
    out_dir.mkdir(parents=True, exist_ok=True)

    suite_started = time.perf_counter()
    grouped: dict[str, dict[str, dict[str, Any]]] = {shape: {} for shape in shape_names}
    runs: list[dict[str, Any]] = []

    for shape in shape_names:
        for parcel_count in counts:
            run_seed = seed_for_run(
                base_seed=seed, shape=shape, parcel_count=parcel_count
            )
            run_dir = _run_dir(out_dir, shape, parcel_count)
            run_started = time.perf_counter()
            generated = generate_named_layout(
                shape,
                parcel_count=parcel_count,
                seed=run_seed,
                width=width,
                height=height,
                streamline_mode=streamline_mode,
            )
            artifacts = write_strict_chen_artifacts(generated, run_dir)
            runtime_seconds = time.perf_counter() - run_started
            summary = dict(artifacts.get("summary", {}))
            run_record = {
                "shape": shape,
                "parcel_count": parcel_count,
                "seed": run_seed,
                "dir": _relative_posix(run_dir, out_dir),
                "manifest": _relative_posix(run_dir / "manifest.json", out_dir),
                "files": dict(artifacts.get("files", {})),
                "summary": summary,
                "review_metrics": _review_metrics(summary, runtime_seconds),
            }
            grouped[shape][str(parcel_count)] = run_record
            runs.append(run_record)

    suite_runtime_seconds = time.perf_counter() - suite_started
    manifest = {
        "layout": "chen-strict-scale-suite",
        "artifact_version": 1,
        "shapes": grouped,
        "runs": runs,
        "parameters": {
            "parcel_counts": list(counts),
            "seed": seed,
            "seed_policy": (
                "base_seed + canonical_shape_index * 100000 + parcel_count"
            ),
            "output_dir": str(out_dir),
            "width": width,
            "height": height,
            "shapes": list(shape_names),
            "streamline_mode": streamline_mode,
        },
        "aggregate_metrics": _aggregate_metrics(runs, suite_runtime_seconds),
        "limitations": _limitations(streamline_mode),
    }
    manifest = _json_ready_manifest(manifest)
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    return manifest


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write strict Chen square/oval/triangle artifacts at multiple parcel "
            "counts. Default output: mapgen/artifacts/chen_strict_scale_suite "
            "with per-run directories shaped like square/parcels_0012/."
        )
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=(
            "suite output directory; defaults to "
            "mapgen/artifacts/chen_strict_scale_suite"
        ),
    )
    parser.add_argument(
        "--parcel-counts",
        type=int,
        nargs="+",
        default=list(DEFAULT_PARCEL_COUNTS),
        help="final parcel counts to generate; default: 12 24 48 96",
    )
    parser.add_argument(
        "--shapes",
        nargs="+",
        choices=SHAPES,
        default=list(SHAPES),
        help="boundary presets to generate; default: square oval triangle",
    )
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--width", type=float, default=180.0)
    parser.add_argument("--height", type=float, default=140.0)
    parser.add_argument(
        "--streamline-mode",
        choices=STREAMLINE_MODES,
        default="baseline",
        help=(
            "streamline candidate mode for non-rectangular shapes; "
            "yang_d_field_candidates and yang_b_field_candidates enable opt-in "
            "Yang mesh seed and DIV/DB/DS/CT score paths"
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    manifest = run_suite(
        out_dir=args.out_dir,
        parcel_counts=args.parcel_counts,
        seed=args.seed,
        width=args.width,
        height=args.height,
        shapes=tuple(args.shapes),
        streamline_mode=args.streamline_mode,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


def _validated_counts(parcel_counts: Iterable[int]) -> tuple[int, ...]:
    counts = tuple(int(count) for count in parcel_counts)
    if not counts:
        raise ValueError("at least one parcel count is required")
    if any(count < 1 for count in counts):
        raise ValueError("parcel counts must be positive")
    duplicates = _duplicate_values(counts)
    if duplicates:
        raise ValueError(
            f"duplicate parcel count(s) are not allowed: {_format_values(duplicates)}"
        )
    return counts


def _limitations(streamline_mode: str) -> list[str]:
    limitations = [
        "grid_smooth_4rosy_laplace_v1_not_full_yang_global_solver",
        "bounded_junction_street_selection_v0_not_full_section_4_2_solver",
        "shapeop_like_projection_v1_not_exact_shapeop_solver",
    ]
    if streamline_mode == "yang_d_field_candidates":
        limitations.append(
            "yang_d_field_candidates_mode_is_opt_in_and_still_approximate"
        )
    if streamline_mode == "yang_b_field_candidates":
        limitations.append(
            "yang_b_field_candidates_mode_is_opt_in_uniform_clipped_mesh_"
            "laplacian_omega_boundary_alignment_and_still_approximate"
        )
    return limitations


def _validated_shapes(shapes: Sequence[str]) -> tuple[str, ...]:
    shape_names = tuple(shapes)
    if not shape_names:
        raise ValueError("at least one shape is required")
    unknown = sorted(set(shape_names) - set(SHAPES))
    if unknown:
        raise ValueError(f"unknown Chen strict shape(s): {', '.join(unknown)}")
    duplicates = _duplicate_values(shape_names)
    if duplicates:
        raise ValueError(
            "duplicate Chen strict shape(s) are not allowed: "
            f"{_format_values(duplicates)}"
        )
    return shape_names


def _duplicate_values(values: Iterable[Any]) -> tuple[Any, ...]:
    seen: set[Any] = set()
    duplicates: list[Any] = []
    for value in values:
        if value not in seen:
            seen.add(value)
        elif value not in duplicates:
            duplicates.append(value)
    return tuple(duplicates)


def _format_values(values: Iterable[Any]) -> str:
    return ", ".join(str(value) for value in values)


def _json_ready_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(manifest))


def _run_dir(out_dir: Path, shape: str, parcel_count: int) -> Path:
    return out_dir / shape / f"parcels_{parcel_count:04d}"


def _relative_posix(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _review_metrics(summary: dict[str, Any], runtime_seconds: float) -> dict[str, Any]:
    metrics = {
        key: summary.get(key, _metric_default(key)) for key in REVIEW_METRIC_KEYS
    }
    metrics["runtime_seconds"] = round(runtime_seconds, 6)
    return metrics


def _metric_default(key: str) -> Any:
    if key == "optimization_regularity_projection_kind":
        return "disabled"
    if "_by_reason" in key:
        return {}
    if key.endswith("_counts"):
        return {}
    if key.endswith("_reasons"):
        return []
    if key.endswith(("_scopes", "_kinds", "_methods")):
        return []
    if key.endswith("_samples"):
        return []
    if key.endswith(("_stage", "_scope", "_mode", "_reason")):
        return "unknown"
    if key.endswith(("_ratio", "_axis_deviation", "_min", "_mean", "_max")):
        return 0.0
    if key.endswith("_points_sample"):
        return []
    if (
        key.endswith("_pass")
        or key.endswith("_applied")
        or key
        in {
            "optimization_geometry_changed",
        }
    ):
        return False
    return 0


def _aggregate_metrics(
    runs: Sequence[dict[str, Any]], suite_runtime_seconds: float
) -> dict[str, Any]:
    by_shape: dict[str, dict[str, Any]] = {}
    by_parcel_count: dict[str, dict[str, Any]] = {}
    for run in runs:
        shape = str(run["shape"])
        shape_aggregate = by_shape.setdefault(shape, _empty_aggregate())
        shape_aggregate["parcel_counts"].append(run["parcel_count"])
        _add_run_to_aggregate(shape_aggregate, run)

        parcel_count = int(run["parcel_count"])
        count_aggregate = by_parcel_count.setdefault(
            str(parcel_count), _empty_aggregate()
        )
        count_aggregate["parcel_counts"].append(parcel_count)
        count_aggregate.setdefault("shapes", []).append(shape)
        _add_run_to_aggregate(count_aggregate, run)

    overall = _empty_aggregate()
    for run in runs:
        overall["parcel_counts"].append(run["parcel_count"])
        _add_run_to_aggregate(overall, run)
    overall["total_runtime_seconds"] = round(suite_runtime_seconds, 6)

    for aggregate in [overall, *by_shape.values(), *by_parcel_count.values()]:
        aggregate["parcel_counts"] = sorted(set(aggregate["parcel_counts"]))
        if "shapes" in aggregate:
            aggregate["shapes"] = sorted(set(aggregate["shapes"]), key=SHAPES.index)
        aggregate["street_edge_density"] = _safe_ratio(
            aggregate.get("street_edge_count", 0),
            aggregate.get("corner_graph_edge_count", 0),
        )

    return {
        "overall": overall,
        "by_shape": by_shape,
        "by_parcel_count": by_parcel_count,
    }


def _empty_aggregate() -> dict[str, Any]:
    aggregate: dict[str, Any] = {
        "run_count": 0,
        "parcel_counts": [],
        "reachability_pass_count": 0,
        "paper_invariant_pass_count": 0,
        "geometry_valid_pass_count": 0,
        "optimization_applied_count": 0,
        "optimization_changed_count": 0,
        "total_runtime_seconds": 0.0,
    }
    aggregate.update({key: 0 for key in SUMMED_AGGREGATE_KEYS})
    aggregate.update({key: 0.0 for key in MAX_AGGREGATE_KEYS})
    aggregate.update({key: {} for key in DICT_COUNT_AGGREGATE_KEYS})
    return aggregate


def _add_run_to_aggregate(aggregate: dict[str, Any], run: dict[str, Any]) -> None:
    metrics = run["review_metrics"]
    aggregate["run_count"] += 1
    aggregate["total_runtime_seconds"] = round(
        aggregate["total_runtime_seconds"] + float(metrics["runtime_seconds"]), 6
    )
    if metrics["street_topology_reachability_pass"]:
        aggregate["reachability_pass_count"] += 1
    if metrics["paper_invariant_pass"]:
        aggregate["paper_invariant_pass_count"] += 1
    if metrics["geometry_valid_pass"]:
        aggregate["geometry_valid_pass_count"] += 1
    if metrics["optimization_applied"]:
        aggregate["optimization_applied_count"] += 1
    if metrics["optimization_geometry_changed"]:
        aggregate["optimization_changed_count"] += 1
    for key in SUMMED_AGGREGATE_KEYS:
        aggregate[key] += int(metrics.get(key, 0) or 0)
    for key in MAX_AGGREGATE_KEYS:
        aggregate[key] = max(float(aggregate[key]), float(metrics.get(key, 0.0) or 0.0))
    for key in DICT_COUNT_AGGREGATE_KEYS:
        _add_count_dict(aggregate[key], metrics.get(key, {}))


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    denominator_float = float(denominator)
    if denominator_float == 0.0:
        return 0.0
    return float(numerator) / denominator_float


def _add_count_dict(target: dict[str, int], source: Any) -> None:
    if not isinstance(source, dict):
        return
    for key, value in source.items():
        target[str(key)] = target.get(str(key), 0) + int(value or 0)


if __name__ == "__main__":
    main()
