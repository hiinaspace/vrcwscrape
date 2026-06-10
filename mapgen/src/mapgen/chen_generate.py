"""Deterministic strict-core Chen layout generation.

The generator in this module is intentionally geometry-first: it accepts split
lines into one global linework set, polygonizes the noded arrangement, and then
hands the final parcel mesh to :mod:`mapgen.chen_core`.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any

import shapely
from shapely import LineString, MultiLineString, Point, Polygon
from shapely.ops import linemerge, polygonize, split, unary_union

from mapgen.chen_core import (
    CHEN_COLLINEAR_THRESHOLD_RAD,
    ChenLayout,
    ChenSplitScore,
    EdgeKey,
    ParcelCornerGraph,
    ParcelFace,
    ParcelMesh,
    StreetNetworkGraph,
    apply_chen_fig7_short_edge_cleanup,
    build_chen_layout,
    chen_access_tolerance,
    chen_fig7_short_edge_candidates,
    chen_fig7_short_edge_diagnostics,
    chen_split_score,
    chen_split_score_from_access_ratios,
    evaluate_layout_invariants,
    largest_polygon,
    parcel_access_ratio_from_path_geometry,
    parcel_corner_graph,
    parcel_graph,
    parcel_mesh_from_polygons,
    street_adjacency,
)
from mapgen.chen_field import StreamlineConfig, candidate_streamlines
from mapgen.chen_optimize import (
    OptimizationConfig,
    OptimizationResult,
    optimize_layout,
)
from mapgen.chen_streets import generate_street_network

_AREA_EPSILON = 1e-10
_GEOMETRY_TOLERANCE = 1e-7
_OPTIMIZATION_GEOMETRY_CHANGE_TOLERANCE = 1e-8
_OPTIMIZATION_REGULARITY_DIAGNOSTIC_DEFAULTS: dict[str, Any] = {
    "regularity_projection_kind": "disabled",
    "regularity_projected_parcel_count_before": 0,
    "regularity_projected_parcel_count_after": 0,
    "regularity_skipped_parcel_count_before": 0,
    "regularity_skipped_parcel_count_after": 0,
    "regularity_skipped_by_reason_before": {},
    "regularity_skipped_by_reason_after": {},
    "regularity_projection_equation_count": 0,
    "regularity_projection_residual_before": 0.0,
    "regularity_projection_residual_after": 0.0,
    "regularity_projection_target_displacement_rms_before": 0.0,
    "regularity_projection_target_displacement_rms_after": 0.0,
    "regularity_projection_target_displacement_max_before": 0.0,
    "regularity_projection_target_displacement_max_after": 0.0,
}
DEFAULT_GENERATION_OPTIMIZATION_CONFIG = OptimizationConfig()
STREAMLINE_MODE_BASELINE = "baseline"
STREAMLINE_MODE_YANG_D_FIELD = "yang_d_field_candidates"
STREAMLINE_MODE_YANG_B_FIELD = "yang_b_field_candidates"
STREAMLINE_MODES = (
    STREAMLINE_MODE_BASELINE,
    STREAMLINE_MODE_YANG_D_FIELD,
    STREAMLINE_MODE_YANG_B_FIELD,
)
_BASELINE_IMPLEMENTATION_STAGE = (
    "chen_grid_smooth_streamline_bounded_junction_shapeop_like_v1"
)
_YANG_D_FIELD_IMPLEMENTATION_STAGE = (
    "chen_yang_d_field_streamline_continuity_cleanup_bounded_junction_shapeop_like_v1"
)
_YANG_B_FIELD_IMPLEMENTATION_STAGE = (
    "chen_yang_b_field_streamline_endpoint_continuation_bounded_junction_"
    "shapeop_like_v1"
)
_BASELINE_STREAMLINE_FIELD_STAGE = "grid_smooth_4rosy_laplace_v1"
_YANG_D_FIELD_STREAMLINE_FIELD_STAGE = "yang_d_field_weighted_footpoint_v1"
_YANG_B_FIELD_STREAMLINE_FIELD_STAGE = "yang_b_field_boundary_laplacian_omega_v0"
_BASELINE_STREAMLINE_FIELD_SCOPE = "compact_grid_smooth_not_full_yang_solver"
_YANG_D_FIELD_STREAMLINE_FIELD_SCOPE = (
    "yang_d_field_mesh_vertex_seeds_div_db_ds_ct_scoring_v0"
)
_YANG_B_FIELD_STREAMLINE_FIELD_SCOPE = (
    "yang_b_field_mesh_vertex_seeds_div_db_ds_ct_scoring_uniform_clipped_mesh_"
    "laplacian_omega_boundary_alignment_v0"
)
_SPLIT_TOPOLOGY_CLEANUP_STAGE_DISABLED = "disabled"
_SPLIT_TOPOLOGY_CLEANUP_STAGE_YANG_CONTINUATION = (
    "yang_streamline_endpoint_continuation_v0"
)
_SPLIT_TOPOLOGY_CLEANUP_SCOPE_YANG_CONTINUATION = (
    "local_tangent_continuation_priority_candidates_not_full_chen_cleanup"
)
_SCORE_DIAGNOSTIC_KEYS = (
    "score_div",
    "score_db",
    "score_ds",
    "score_ct",
    "score_total_normalized",
)
_ACCEPTED_STREAMLINE_NUMERIC_DIAGNOSTIC_KEYS = (
    *_SCORE_DIAGNOSTIC_KEYS,
    "trace_mesh_interior_seed_count",
    "trace_seed_total_count",
    "trace_suppression_distance",
    "trace_parcel_length",
    "field_mesh_vertex_count",
    "field_mesh_retained_vertex_count",
    "field_mesh_triangle_count",
    "field_mesh_edge_count",
    "field_mesh_isolated_vertex_count",
    "field_solver_iterations",
    "field_solver_residual",
    "field_boundary_constraint_count",
    "field_valid_node_count",
    "field_b_boundary_alignment_weight",
    "field_b_boundary_anchor_count",
    "field_b_boundary_vertex_anchor_count",
    "field_b_boundary_segment_anchor_count",
    "field_b_boundary_anchor_fraction",
    "field_b_solver_iterations",
    "field_b_solver_residual",
    "field_b_smoothness_energy_initial",
    "field_b_smoothness_energy",
    "field_b_boundary_alignment_error_initial",
    "field_b_boundary_alignment_error",
    "field_b_renormalized_vector_count",
)


def _optimization_config_for_line_mode(line_mode: str) -> OptimizationConfig:
    return DEFAULT_GENERATION_OPTIMIZATION_CONFIG


def _resolve_streamline_config(
    *,
    streamline_mode: str,
    streamline_config: StreamlineConfig | None,
    target_area: float,
) -> tuple[StreamlineConfig | None, str]:
    if streamline_mode not in STREAMLINE_MODES:
        raise ValueError(
            "unknown streamline_mode: "
            f"{streamline_mode!r}; expected one of {', '.join(STREAMLINE_MODES)}"
        )
    if streamline_config is not None:
        if streamline_mode != STREAMLINE_MODE_BASELINE:
            raise ValueError(
                "streamline_config cannot be combined with a non-baseline "
                "streamline_mode"
            )
        return (
            _streamline_config_with_target_scale(streamline_config, target_area),
            "custom_streamline_config",
        )
    if streamline_mode == STREAMLINE_MODE_YANG_D_FIELD:
        parcel_length = _target_parcel_length(target_area)
        return (
            StreamlineConfig(
                field_mode="yang_d_field",
                candidate_seed_mode="yang_mesh_vertices",
                parcel_length=parcel_length,
                streamline_score_mode="yang_div_db_ds_ct",
                template_widths=(parcel_length,),
            ),
            STREAMLINE_MODE_YANG_D_FIELD,
        )
    if streamline_mode == STREAMLINE_MODE_YANG_B_FIELD:
        parcel_length = _target_parcel_length(target_area)
        return (
            StreamlineConfig(
                field_mode="yang_b_field",
                candidate_seed_mode="yang_mesh_vertices",
                parcel_length=parcel_length,
                streamline_score_mode="yang_div_db_ds_ct",
                template_widths=(parcel_length,),
            ),
            STREAMLINE_MODE_YANG_B_FIELD,
        )
    return None, STREAMLINE_MODE_BASELINE


def _streamline_config_with_target_scale(
    config: StreamlineConfig, target_area: float
) -> StreamlineConfig:
    parcel_length = _target_parcel_length(target_area)
    updates: dict[str, Any] = {}
    if (
        config.candidate_seed_mode == "yang_mesh_vertices"
        and config.parcel_length is None
    ):
        updates["parcel_length"] = parcel_length
    if (
        config.streamline_score_mode == "yang_div_db_ds_ct"
        and not config.template_widths
    ):
        updates["template_widths"] = (parcel_length,)
    if not updates:
        return config
    return replace(config, **updates)


def _target_parcel_length(target_area: float) -> float:
    return math.sqrt(max(float(target_area), _AREA_EPSILON))


@dataclass(frozen=True)
class BoundarySpec:
    name: str
    geom: Polygon


@dataclass(frozen=True)
class GeneratedChenLayout:
    name: str
    boundary: Polygon
    layout: ChenLayout
    split_lines: tuple[LineString, ...]
    metrics: dict[str, Any]


@dataclass(frozen=True)
class _SplitCandidate:
    score: ChenSplitScore
    segment: LineString
    parts: tuple[Polygon, Polygon]
    score_key: tuple[float, ...]
    source: str
    is_axis_aligned: bool
    is_curved: bool
    diagnostics: dict[str, Any]


@dataclass(frozen=True)
class _CutLineCandidate:
    line: LineString
    source: str
    quality: float
    diagnostics: dict[str, Any]


def boundary_preset(
    name: str, width: float = 180.0, height: float = 140.0
) -> BoundarySpec:
    """Return one of the strict generator boundary presets."""
    if width <= 0.0 or height <= 0.0:
        raise ValueError("boundary width and height must be positive")

    if name == "square":
        side = min(width, height)
        x0 = (width - side) * 0.5
        y0 = (height - side) * 0.5
        geom = Polygon(
            [
                (x0, y0),
                (x0 + side, y0),
                (x0 + side, y0 + side),
                (x0, y0 + side),
            ]
        )
    elif name == "oval":
        geom = _anchored_oval(
            center=(width * 0.5, height * 0.5),
            radius_x=width * 0.47,
            radius_y=height * 0.43,
        )
    elif name == "triangle":
        geom = Polygon(
            [
                (width * 0.06, height * 0.08),
                (width * 0.94, height * 0.12),
                (width * 0.50, height * 0.94),
            ]
        )
    else:
        raise ValueError(f"unknown Chen boundary preset: {name}")

    return BoundarySpec(name=name, geom=_clean_polygon(geom))


def _anchored_oval(
    *,
    center: tuple[float, float],
    radius_x: float,
    radius_y: float,
    anchor_count: int = 8,
) -> Polygon:
    """Return a curved oval with stable corner-graph anchors.

    The current strict core intentionally collapses smooth valence-2 parcel
    vertices. Eight anchored corners keep early oval parcels valid while the
    intermediate points give artifact/core edge paths enough geometry to follow
    the contour instead of drawing long chords.
    """
    cx, cy = center
    points: list[tuple[float, float]] = []
    for index in range(anchor_count):
        for fraction in (0.0, 0.18, 0.5, 0.82):
            angle = 2.0 * math.pi * (index + fraction) / anchor_count
            points.append(
                (
                    cx + radius_x * math.cos(angle),
                    cy + radius_y * math.sin(angle),
                )
            )
    return Polygon(points)


def _boundary_contour_metrics(
    boundary: BoundarySpec, boundary_geom: Polygon
) -> dict[str, Any]:
    if boundary.name != "oval":
        return {
            "boundary_contour_fidelity_stage": "not_applicable",
            "oval_boundary_vertex_count": 0,
            "oval_boundary_normalized_radial_error_max": 0.0,
            "oval_boundary_normalized_radial_error_mean": 0.0,
            "oval_boundary_radial_error_max": 0.0,
            "oval_boundary_radial_error_mean": 0.0,
        }

    minx, miny, maxx, maxy = boundary_geom.bounds
    center_x = (minx + maxx) * 0.5
    center_y = (miny + maxy) * 0.5
    radius_x = (maxx - minx) * 0.5
    radius_y = (maxy - miny) * 0.5
    if radius_x <= _GEOMETRY_TOLERANCE or radius_y <= _GEOMETRY_TOLERANCE:
        return {
            "boundary_contour_fidelity_stage": "preset_ellipse_radial_error_v0",
            "oval_boundary_vertex_count": 0,
            "oval_boundary_normalized_radial_error_max": 0.0,
            "oval_boundary_normalized_radial_error_mean": 0.0,
            "oval_boundary_radial_error_max": 0.0,
            "oval_boundary_radial_error_mean": 0.0,
        }

    normalized_errors: list[float] = []
    radial_errors: list[float] = []
    for x, y in boundary_geom.exterior.coords[:-1]:
        dx = float(x) - center_x
        dy = float(y) - center_y
        normalized_radius = math.hypot(dx / radius_x, dy / radius_y)
        normalized_error = abs(normalized_radius - 1.0)
        normalized_errors.append(normalized_error)
        if normalized_radius <= _GEOMETRY_TOLERANCE:
            radial_errors.append(0.0)
        else:
            ideal_x = center_x + dx / normalized_radius
            ideal_y = center_y + dy / normalized_radius
            radial_errors.append(math.hypot(float(x) - ideal_x, float(y) - ideal_y))

    return {
        "boundary_contour_fidelity_stage": "preset_ellipse_radial_error_v0",
        "oval_boundary_vertex_count": int(len(normalized_errors)),
        "oval_boundary_normalized_radial_error_max": (
            float(max(normalized_errors)) if normalized_errors else 0.0
        ),
        "oval_boundary_normalized_radial_error_mean": (
            float(sum(normalized_errors) / len(normalized_errors))
            if normalized_errors
            else 0.0
        ),
        "oval_boundary_radial_error_max": (
            float(max(radial_errors)) if radial_errors else 0.0
        ),
        "oval_boundary_radial_error_mean": (
            float(sum(radial_errors) / len(radial_errors)) if radial_errors else 0.0
        ),
    }


def generate_named_layout(
    name: str,
    parcel_count: int = 48,
    seed: int = 0,
    width: float = 180.0,
    height: float = 140.0,
    apply_optimization: bool = True,
    optimization_config: OptimizationConfig | None = None,
    streamline_mode: str = STREAMLINE_MODE_BASELINE,
    streamline_config: StreamlineConfig | None = None,
) -> GeneratedChenLayout:
    """Generate a strict-core Chen layout for a named boundary preset."""
    return generate_layout_for_boundary(
        boundary_preset(name, width=width, height=height),
        parcel_count=parcel_count,
        seed=seed,
        apply_optimization=apply_optimization,
        optimization_config=optimization_config,
        streamline_mode=streamline_mode,
        streamline_config=streamline_config,
    )


def generate_layout_for_boundary(
    boundary: BoundarySpec,
    parcel_count: int = 48,
    seed: int = 0,
    apply_optimization: bool = True,
    optimization_config: OptimizationConfig | None = None,
    streamline_mode: str = STREAMLINE_MODE_BASELINE,
    streamline_config: StreamlineConfig | None = None,
) -> GeneratedChenLayout:
    """Generate a deterministic Chen layout for ``boundary``."""
    if parcel_count < 1:
        raise ValueError("parcel_count must be at least 1")

    boundary_geom = _clean_polygon(boundary.geom)
    target_area = float(boundary_geom.area) / float(parcel_count)
    resolved_streamline_config, resolved_streamline_mode = _resolve_streamline_config(
        streamline_mode=streamline_mode,
        streamline_config=streamline_config,
        target_area=target_area,
    )
    line_mode = "streamline_field"
    split_lines: list[LineString] = []
    accepted_split_diagnostics: list[dict[str, Any]] = []
    diagnostics: dict[str, Any] = {
        "boundary": boundary.name,
        "boundary_name": boundary.name,
        "boundary_area": float(boundary_geom.area),
        "requested_parcel_count": int(parcel_count),
        "seed": int(seed),
        "target_area": float(target_area),
        "line_candidate_mode": line_mode,
        "candidate_face_attempt_count": 0,
        "candidate_split_attempt_count": 0,
        "candidate_split_reject_count": 0,
        "candidate_topology_reject_count": 0,
        "path_access_score_count": 0,
        "path_access_score_fallback_count": 0,
        "axis_aligned_candidate_count": 0,
        "streamline_candidate_count": 0,
        "streamline_config_mode": resolved_streamline_mode,
        "streamline_config_field_mode": (
            resolved_streamline_config.field_mode
            if resolved_streamline_config is not None
            else "default"
        ),
        "streamline_config_candidate_seed_mode": (
            resolved_streamline_config.candidate_seed_mode
            if resolved_streamline_config is not None
            else "default"
        ),
        "streamline_config_score_mode": (
            resolved_streamline_config.streamline_score_mode
            if resolved_streamline_config is not None
            else "default"
        ),
        "streamline_continuation_candidate_count": 0,
    }
    diagnostics.update(_boundary_contour_metrics(boundary, boundary_geom))

    faces = _polygonize_faces(boundary_geom, split_lines)
    max_iterations = max(parcel_count * 4, 8)
    iterations = 0
    while len(faces) < parcel_count and iterations < max_iterations:
        iterations += 1
        accepted = _next_split(
            boundary_geom,
            faces,
            split_lines,
            target_area=target_area,
            seed=seed,
            iteration=iterations,
            line_mode=line_mode,
            streamline_config=resolved_streamline_config,
            diagnostics=diagnostics,
        )
        if accepted is None:
            break
        split_lines.append(accepted.segment)
        accepted_split_diagnostics.append(_accepted_split_diagnostic(accepted))
        faces = _polygonize_faces(boundary_geom, split_lines)

    if len(faces) != parcel_count:
        raise RuntimeError(
            "strict Chen generator could not reach requested parcel count: "
            f"requested={parcel_count} produced={len(faces)}"
        )

    polygons = [(index + 1, face) for index, face in enumerate(faces)]
    mesh = parcel_mesh_from_polygons(polygons, boundary=boundary_geom)
    fig7_cleanup_metrics: dict[str, Any] = {}
    cleanup_input_layout = build_chen_layout(mesh, set())
    cleanup_result = apply_chen_fig7_short_edge_cleanup(
        cleanup_input_layout,
        boundary=boundary_geom,
        max_passes=max(parcel_count * 2, 8),
    )
    if _cleanup_preserves_rectangular_axis(
        boundary_geom,
        before=cleanup_input_layout,
        after=cleanup_result.layout,
    ):
        mesh = cleanup_result.layout.mesh
        fig7_cleanup_metrics = {
            **cleanup_result.metrics,
            "chen_fig7_short_edge_cleanup_rectangular_axis_guard_applied": bool(
                _is_axis_aligned_rectangle(boundary_geom)
            ),
            "chen_fig7_short_edge_cleanup_rectangular_axis_guard_rejected": False,
        }
    else:
        fig7_cleanup_metrics = {
            **cleanup_result.metrics,
            "chen_fig7_short_edge_cleanup_applied": False,
            "chen_fig7_short_edge_cleanup_applied_count": 0,
            "chen_fig7_short_edge_cleanup_blocking_reason": (
                "rectangular_axis_preservation_guard_rejected_cleanup"
            ),
            "chen_fig7_short_edge_cleanup_rectangular_axis_guard_applied": True,
            "chen_fig7_short_edge_cleanup_rectangular_axis_guard_rejected": True,
            "chen_fig7_short_edge_cleanup_rectangular_axis_guard_before_max": (
                _mesh_axis_deviation_metrics(cleanup_input_layout)[
                    "max_mesh_axis_deviation"
                ]
            ),
            "chen_fig7_short_edge_cleanup_rectangular_axis_guard_after_max": (
                _mesh_axis_deviation_metrics(cleanup_result.layout)[
                    "max_mesh_axis_deviation"
                ]
            ),
        }
    street_result = generate_street_network(mesh)
    street_edges = set(street_result.street_edges)
    initial_layout = build_chen_layout(mesh, street_edges)
    layout = initial_layout
    optimization_metrics = _optimization_not_applied_metrics(
        "disabled", "apply_optimization_false"
    )
    if apply_optimization:
        config = optimization_config or _optimization_config_for_line_mode(line_mode)
        optimization_result = optimize_layout(layout, boundary_geom, config)
        layout = optimization_result.layout
        split_lines = list(
            _remap_split_lines_to_layout_edges(
                initial_layout, layout, boundary_geom, split_lines
            )
        )
        optimization_metrics = _optimization_applied_metrics(optimization_result)

    report = evaluate_layout_invariants(layout, target_boundary=boundary_geom)
    street_topology_reachability_pass = (
        bool(report.metrics.get("street_network_subset_of_corner_graph"))
        and int(report.metrics.get("street_graph_component_count", 0)) == 1
        and int(report.metrics.get("unreachable_parcel_count", 0)) == 0
    )
    axis_aligned_split_count = sum(_line_is_axis_aligned(line) for line in split_lines)
    curved_split_count = sum(_line_is_curved(line) for line in split_lines)
    split_orientation_metrics = _line_orientation_metrics(split_lines, "split")
    street_orientation_metrics = _street_orientation_metrics(layout)
    mesh_axis_metrics = _mesh_axis_deviation_metrics(layout)
    corner_degree_metrics = _graph_degree_metrics(
        layout, layout.corner_graph.edges, "corner_graph"
    )
    street_degree_metrics = _graph_degree_metrics(
        layout, layout.street_network.edges, "street"
    )
    street_density_metrics = _street_density_metrics(layout)
    rectangular_fidelity_metrics = _rectangular_fidelity_metrics(layout, line_mode)
    short_edge_metrics = chen_fig7_short_edge_diagnostics(layout)
    split_provenance_metrics = _chen_fig7_split_provenance_metrics(
        layout,
        split_lines,
        accepted_split_diagnostics,
        sample_limit=16,
    )
    accepted_streamline_metrics = _accepted_streamline_diagnostic_metrics(
        accepted_split_diagnostics
    )
    streamline_stage_metrics = _streamline_stage_metrics(
        resolved_streamline_config,
        resolved_streamline_mode,
        line_mode=line_mode,
    )

    metrics = dict(report.metrics)
    metrics.update(diagnostics)
    metrics.update(
        {
            "accepted_split_count": int(len(split_lines)),
            "split_line_count": int(len(split_lines)),
            "axis_aligned_split_line_count": int(axis_aligned_split_count),
            "curved_split_line_count": int(curved_split_count),
            "straight_split_line_count": int(len(split_lines) - curved_split_count),
            "accepted_streamline_split_count": int(
                sum(
                    1
                    for item in accepted_split_diagnostics
                    if item.get("source") == "streamline"
                )
            ),
            "accepted_streamline_continuation_split_count": int(
                sum(
                    1
                    for item in accepted_split_diagnostics
                    if item.get("source") == "streamline_continuation"
                )
            ),
            "accepted_axis_fallback_split_count": int(
                sum(
                    1
                    for item in accepted_split_diagnostics
                    if item.get("source") == "axis_fallback"
                )
            ),
            "accepted_split_diagnostics": tuple(accepted_split_diagnostics),
            "generated_parcel_count": int(len(layout.mesh.parcels)),
            "polygonized_face_count": int(len(faces)),
            **streamline_stage_metrics,
            "street_selection_mode": (
                "chen_section_4_2_reachability_bounded_junctions_v0"
            ),
            "street_topology_reachability_pass": bool(
                street_topology_reachability_pass
            ),
            "deprecated_chen_street_generation_pass_alias": bool(
                street_topology_reachability_pass
            ),
            "chen_street_generation_scope": (
                "reachability_plus_bounded_junction_completion_v0"
            ),
            "street_generation_diagnostics": street_result.diagnostics,
            "paper_invariant_pass": bool(report.paper_invariant_pass),
            "geometry_valid_pass": bool(report.geometry_valid_pass),
            "chen_formula_pass": bool(report.chen_formula_pass),
            "diagnostic_metric_pass": bool(report.diagnostic_metric_pass),
            **split_orientation_metrics,
            **street_orientation_metrics,
            **mesh_axis_metrics,
            **street_density_metrics,
            **corner_degree_metrics,
            **street_degree_metrics,
            **rectangular_fidelity_metrics,
            **short_edge_metrics,
            **split_provenance_metrics,
            **fig7_cleanup_metrics,
            **accepted_streamline_metrics,
        }
    )
    metrics.update(optimization_metrics)

    return GeneratedChenLayout(
        name=boundary.name,
        boundary=boundary_geom,
        layout=layout,
        split_lines=tuple(split_lines),
        metrics=metrics,
    )


def _clean_polygon(poly: Polygon) -> Polygon:
    cleaned = largest_polygon(poly if poly.is_valid else poly.buffer(0.0))
    if cleaned is None or cleaned.area <= _AREA_EPSILON:
        raise ValueError("boundary polygon must be valid and non-empty")
    deduped = _dedupe_polygon_vertices(cleaned)
    if deduped is not None and deduped.is_valid and deduped.area > _AREA_EPSILON:
        return deduped
    return cleaned


def _dedupe_polygon_vertices(poly: Polygon) -> Polygon | None:
    coords: list[tuple[float, float]] = []
    for x, y in poly.exterior.coords[:-1]:
        point = (float(x), float(y))
        if coords and math.hypot(
            point[0] - coords[-1][0], point[1] - coords[-1][1]
        ) <= (_GEOMETRY_TOLERANCE * 0.1):
            continue
        coords.append(point)
    if len(coords) >= 2 and math.hypot(
        coords[0][0] - coords[-1][0], coords[0][1] - coords[-1][1]
    ) <= (_GEOMETRY_TOLERANCE * 0.1):
        coords.pop()
    if len(coords) < 3:
        return None
    try:
        return Polygon(coords)
    except (ValueError, shapely.GEOSException):
        return None


def _is_axis_aligned_rectangle(poly: Polygon) -> bool:
    minx, miny, maxx, maxy = poly.bounds
    if maxx - minx <= _GEOMETRY_TOLERANCE or maxy - miny <= _GEOMETRY_TOLERANCE:
        return False
    box = Polygon([(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy)])
    tolerance = max(float(poly.area), float(box.area), 1.0) * 1e-9
    return float(poly.symmetric_difference(box).area) <= tolerance


def _line_is_axis_aligned(line: LineString) -> bool:
    coords = [(float(x), float(y)) for x, y in line.coords]
    if len(coords) < 2:
        return False
    return all(
        abs(a[0] - b[0]) <= _GEOMETRY_TOLERANCE
        or abs(a[1] - b[1]) <= _GEOMETRY_TOLERANCE
        for a, b in zip(coords, coords[1:], strict=False)
    )


def _line_is_curved(line: LineString) -> bool:
    coords = [(float(x), float(y)) for x, y in line.coords]
    if len(coords) <= 2:
        return False
    start = coords[0]
    end = coords[-1]
    chord = math.hypot(end[0] - start[0], end[1] - start[1])
    if chord <= _GEOMETRY_TOLERANCE:
        return False
    max_offset = 0.0
    for point in coords[1:-1]:
        area2 = abs(
            (end[0] - start[0]) * (start[1] - point[1])
            - (start[0] - point[0]) * (end[1] - start[1])
        )
        max_offset = max(max_offset, area2 / chord)
    return max_offset > max(_GEOMETRY_TOLERANCE * 10.0, chord * 1e-4)


def _optimization_not_applied_metrics(stage: str, reason: str) -> dict[str, Any]:
    return {
        "optimization_stage": stage,
        "optimization_applied": False,
        "optimization_layout_used": False,
        "optimization_geometry_changed": False,
        "optimization_skip_reason": reason,
        "optimization_diagnostics": {},
        **_optimization_regularity_metrics({}),
    }


def _optimization_applied_metrics(result: OptimizationResult) -> dict[str, Any]:
    diagnostics = dict(result.metrics)
    max_displacement = float(diagnostics.get("max_vertex_displacement", 0.0))
    optimizer_kind = str(diagnostics.get("optimizer_kind", "unknown"))
    optimization_stage = (
        "chen_section_5_shapeop_like_projection_v1"
        if optimizer_kind == "shapeop_like_projection"
        else f"chen_section_5_{optimizer_kind}"
    )
    return {
        "optimization_stage": optimization_stage,
        "optimization_applied": True,
        "optimization_layout_used": True,
        "optimization_geometry_changed": bool(
            max_displacement > _OPTIMIZATION_GEOMETRY_CHANGE_TOLERANCE
        ),
        "optimization_skip_reason": None,
        "optimization_accepted_iteration_count": int(
            diagnostics.get("accepted_iteration_count", 0)
        ),
        "optimization_converged": bool(diagnostics.get("converged", False)),
        "optimization_energy_before": float(diagnostics.get("energy_before", 0.0)),
        "optimization_energy_after": float(diagnostics.get("energy_after", 0.0)),
        "optimization_max_vertex_displacement": max_displacement,
        "optimization_diagnostics": diagnostics,
        **_optimization_regularity_metrics(diagnostics),
    }


def _optimization_regularity_metrics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for key, default in _OPTIMIZATION_REGULARITY_DIAGNOSTIC_DEFAULTS.items():
        metrics[f"optimization_{key}"] = _optimizer_diagnostic_value(
            diagnostics.get(key, default),
            default,
        )
    return metrics


def _optimizer_diagnostic_value(value: Any, default: Any) -> Any:
    if isinstance(default, dict):
        if not isinstance(value, dict):
            return {}
        return {
            str(key): _optimizer_diagnostic_count_value(count)
            for key, count in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(default, float):
        return float(value or 0.0)
    if isinstance(default, int):
        return int(value or 0)
    if isinstance(default, str):
        return str(default if value is None else value)
    return value


def _optimizer_diagnostic_count_value(value: Any) -> int:
    return int(value or 0)


def _remap_split_lines_to_layout_edges(
    before_layout: ChenLayout,
    after_layout: ChenLayout,
    boundary: Polygon,
    split_lines: list[LineString],
) -> tuple[LineString, ...]:
    """Project construction split overlays onto optimized mesh edge geometry."""
    if not split_lines:
        return ()

    boundary_line = LineString(boundary.exterior.coords)
    tolerance = max(math.sqrt(max(boundary.area, _AREA_EPSILON)) * 1e-7, 1e-6)
    remapped: list[LineString] = []
    for split_line in split_lines:
        matched_parts: list[
            tuple[float, float, tuple[float, float], tuple[float, float]]
        ] = []
        for edge in sorted(before_layout.mesh.edges):
            start_id, end_id = edge
            before_start = before_layout.mesh.vertices[start_id].point
            before_end = before_layout.mesh.vertices[end_id].point
            midpoint = (
                (before_start[0] + before_end[0]) * 0.5,
                (before_start[1] + before_end[1]) * 0.5,
            )
            midpoint_point = Point(midpoint)
            if boundary_line.distance(midpoint_point) <= tolerance:
                continue
            if split_line.distance(midpoint_point) > tolerance:
                continue
            if (
                max(
                    split_line.distance(Point(before_start)),
                    split_line.distance(Point(before_end)),
                )
                > tolerance
            ):
                continue

            start_projection = float(split_line.project(Point(before_start)))
            end_projection = float(split_line.project(Point(before_end)))
            after_start = after_layout.mesh.vertices[start_id].point
            after_end = after_layout.mesh.vertices[end_id].point
            if end_projection < start_projection:
                start_projection, end_projection = end_projection, start_projection
                after_start, after_end = after_end, after_start
            matched_parts.append(
                (start_projection, end_projection, after_start, after_end)
            )

        if not matched_parts:
            remapped.append(split_line)
            continue

        coords: list[tuple[float, float]] = []
        for _start_projection, _end_projection, start, end in sorted(matched_parts):
            if not coords:
                coords.extend([start, end])
                continue
            if _point_distance(coords[-1], start) > tolerance:
                coords.append(start)
            coords.append(end)
        coords = _dedupe_line_coords(coords)
        remapped.append(LineString(coords) if len(coords) >= 2 else split_line)
    return tuple(remapped)


def _point_distance(left: tuple[float, float], right: tuple[float, float]) -> float:
    return math.hypot(left[0] - right[0], left[1] - right[1])


def _dedupe_line_coords(
    coords: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    deduped: list[tuple[float, float]] = []
    for point in coords:
        if deduped and _point_distance(deduped[-1], point) <= _GEOMETRY_TOLERANCE:
            continue
        deduped.append(point)
    return deduped


def _line_orientation_metrics(
    lines: list[LineString] | tuple[LineString, ...], prefix: str
) -> dict[str, Any]:
    segment_count = 0
    axis_aligned_count = 0
    axis_deviations: list[float] = []
    for line in lines:
        coords = [(float(x), float(y)) for x, y in line.coords]
        for a, b in zip(coords, coords[1:], strict=False):
            if math.hypot(b[0] - a[0], b[1] - a[1]) <= _GEOMETRY_TOLERANCE:
                continue
            segment_count += 1
            axis_deviation = _segment_axis_deviation(a, b)
            axis_deviations.append(axis_deviation)
            if (
                abs(a[0] - b[0]) <= _GEOMETRY_TOLERANCE
                or abs(a[1] - b[1]) <= _GEOMETRY_TOLERANCE
            ):
                axis_aligned_count += 1
    return {
        f"{prefix}_segment_count": int(segment_count),
        f"axis_aligned_{prefix}_segment_count": int(axis_aligned_count),
        f"non_axis_aligned_{prefix}_segment_count": int(
            segment_count - axis_aligned_count
        ),
        f"max_{prefix}_axis_deviation": (
            float(max(axis_deviations)) if axis_deviations else 0.0
        ),
        f"mean_{prefix}_axis_deviation": (
            float(sum(axis_deviations) / len(axis_deviations))
            if axis_deviations
            else 0.0
        ),
    }


def _street_orientation_metrics(layout: ChenLayout) -> dict[str, Any]:
    lines: list[LineString] = []
    curved_edge_count = 0
    for edge in sorted(layout.street_network.edges):
        points = _edge_path_points(layout, edge)
        if len(points) < 2:
            continue
        line = LineString(points)
        lines.append(line)
        if _line_is_curved(line):
            curved_edge_count += 1
    metrics = _line_orientation_metrics(lines, "street")
    metrics["curved_street_edge_count"] = int(curved_edge_count)
    return metrics


def _edge_path_points(layout: ChenLayout, edge: EdgeKey) -> list[tuple[float, float]]:
    path = layout.corner_graph.edge_paths.get(edge, edge)
    return [layout.mesh.vertices[node].point for node in path]


def _mesh_axis_deviation_metrics(layout: ChenLayout) -> dict[str, Any]:
    deviations: list[float] = []
    for edge in sorted(layout.mesh.edges):
        a, b = edge
        start = layout.mesh.vertices[a].point
        end = layout.mesh.vertices[b].point
        if _point_distance(start, end) <= _GEOMETRY_TOLERANCE:
            continue
        deviations.append(_segment_axis_deviation(start, end))
    return {
        "max_mesh_axis_deviation": float(max(deviations)) if deviations else 0.0,
        "mean_mesh_axis_deviation": (
            float(sum(deviations) / len(deviations)) if deviations else 0.0
        ),
    }


def _cleanup_preserves_rectangular_axis(
    boundary: Polygon, *, before: ChenLayout, after: ChenLayout
) -> bool:
    if not _is_axis_aligned_rectangle(boundary):
        return True
    before_max = _mesh_axis_deviation_metrics(before)["max_mesh_axis_deviation"]
    after_max = _mesh_axis_deviation_metrics(after)["max_mesh_axis_deviation"]
    return after_max <= max(float(before_max), _GEOMETRY_TOLERANCE * 10.0)


def _segment_axis_deviation(
    start: tuple[float, float], end: tuple[float, float]
) -> float:
    dx = float(end[0] - start[0])
    dy = float(end[1] - start[1])
    length = math.hypot(dx, dy)
    if length <= _GEOMETRY_TOLERANCE:
        return 0.0
    return float(min(abs(dx), abs(dy)) / length)


def _graph_degree_metrics(
    layout: ChenLayout, edges: set[EdgeKey] | frozenset[EdgeKey], prefix: str
) -> dict[str, Any]:
    adjacency = street_adjacency(StreetNetworkGraph(set(edges)))
    degree_counts: dict[int, int] = {}
    interior_degree_counts: dict[int, int] = {}
    boundary_degree_counts: dict[int, int] = {}
    for vertex_id, neighbors in adjacency.items():
        degree = len(neighbors)
        degree_counts[degree] = degree_counts.get(degree, 0) + 1
        if layout.mesh.vertices[vertex_id].on_boundary:
            boundary_degree_counts[degree] = boundary_degree_counts.get(degree, 0) + 1
        else:
            interior_degree_counts[degree] = interior_degree_counts.get(degree, 0) + 1
    return {
        **_degree_count_metrics(degree_counts, prefix),
        **_degree_count_metrics(interior_degree_counts, f"interior_{prefix}"),
        **_degree_count_metrics(boundary_degree_counts, f"boundary_{prefix}"),
    }


def _degree_count_metrics(degree_counts: dict[int, int], prefix: str) -> dict[str, Any]:
    node_count = sum(degree_counts.values())
    junction_count = sum(
        count for degree, count in degree_counts.items() if degree >= 3
    )
    t_junction_count = degree_counts.get(3, 0)
    four_way_count = degree_counts.get(4, 0)
    high_degree_count = sum(
        count for degree, count in degree_counts.items() if degree >= 5
    )
    return {
        f"{prefix}_node_count": int(node_count),
        f"{prefix}_dead_end_node_count": int(degree_counts.get(1, 0)),
        f"{prefix}_degree_2_node_count": int(degree_counts.get(2, 0)),
        f"{prefix}_junction_count": int(junction_count),
        f"{prefix}_t_junction_count": int(t_junction_count),
        f"{prefix}_four_way_intersection_count": int(four_way_count),
        f"{prefix}_high_degree_intersection_count": int(high_degree_count),
        f"{prefix}_t_junction_ratio": _safe_degree_ratio(
            t_junction_count, junction_count
        ),
        f"{prefix}_four_way_intersection_ratio": _safe_degree_ratio(
            four_way_count, junction_count
        ),
        f"{prefix}_degree_counts": dict(sorted(degree_counts.items())),
    }


def _safe_degree_ratio(numerator: int, denominator: int) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _rectangular_fidelity_metrics(layout: ChenLayout, line_mode: str) -> dict[str, Any]:
    adjacency = street_adjacency(StreetNetworkGraph(set(layout.corner_graph.edges)))
    sample: list[tuple[float, float]] = []
    if line_mode == "axis_aligned_rectangle":
        for vertex_id, neighbors in sorted(adjacency.items()):
            if layout.mesh.vertices[vertex_id].on_boundary or len(neighbors) != 3:
                continue
            point = layout.mesh.vertices[vertex_id].point
            sample.append((round(float(point[0]), 9), round(float(point[1]), 9)))
            if len(sample) >= 16:
                break
    return {
        "rectangular_interior_t_junction_points_sample": tuple(sample),
    }


def _chen_fig7_split_provenance_metrics(
    layout: ChenLayout,
    split_lines: list[LineString],
    accepted_split_diagnostics: list[dict[str, Any]],
    *,
    sample_limit: int,
) -> dict[str, Any]:
    """Classify non-Fig.7 interior T nodes against accepted split line geometry.

    This is lineage evidence only: it says where an unexplained T node falls
    relative to accepted split overlays, not whether Chen would consider that
    T node a cleanup error.
    """
    unexplained_nodes, straight_nodes, kinked_nodes = (
        _chen_fig7_unexplained_t_junction_nodes(layout)
    )
    tolerance = _split_provenance_tolerance(layout, split_lines)
    endpoint_nodes: set[int] = set()
    on_line_nodes: set[int] = set()
    unknown_nodes: set[int] = set()
    endpoint_source_counts: dict[str, int] = {}
    on_line_source_counts: dict[str, int] = {}
    endpoint_straight_source_counts: dict[str, int] = {}
    endpoint_kinked_source_counts: dict[str, int] = {}
    on_line_straight_source_counts: dict[str, int] = {}
    on_line_kinked_source_counts: dict[str, int] = {}
    unknown_straight_source_counts: dict[str, int] = {}
    unknown_kinked_source_counts: dict[str, int] = {}
    endpoint_samples: list[dict[str, Any]] = []
    on_line_samples: list[dict[str, Any]] = []
    unknown_samples: list[dict[str, Any]] = []

    for node in sorted(unexplained_nodes):
        point = layout.mesh.vertices[node].point
        attribution = _split_line_attribution(
            point,
            split_lines,
            accepted_split_diagnostics,
            tolerance=tolerance,
        )
        sample = _split_line_attribution_sample(layout, node, attribution)
        if attribution["kind"] == "endpoint":
            endpoint_nodes.add(node)
            _increment_count(endpoint_source_counts, attribution["source"])
            if node in straight_nodes:
                _increment_count(endpoint_straight_source_counts, attribution["source"])
            elif node in kinked_nodes:
                _increment_count(endpoint_kinked_source_counts, attribution["source"])
            if len(endpoint_samples) < sample_limit:
                endpoint_samples.append(sample)
        elif attribution["kind"] == "on_line":
            on_line_nodes.add(node)
            _increment_count(on_line_source_counts, attribution["source"])
            if node in straight_nodes:
                _increment_count(on_line_straight_source_counts, attribution["source"])
            elif node in kinked_nodes:
                _increment_count(on_line_kinked_source_counts, attribution["source"])
            if len(on_line_samples) < sample_limit:
                on_line_samples.append(sample)
        else:
            unknown_nodes.add(node)
            if node in straight_nodes:
                _increment_count(unknown_straight_source_counts, attribution["source"])
            elif node in kinked_nodes:
                _increment_count(unknown_kinked_source_counts, attribution["source"])
            if len(unknown_samples) < sample_limit:
                unknown_samples.append(sample)

    return {
        "chen_fig7_unexplained_t_junction_split_provenance_stage": (
            "accepted_split_line_geometry_endpoint_or_interior_v0"
        ),
        "chen_fig7_unexplained_t_junction_split_provenance_scope": (
            "geometric_attribution_only_not_paper_violation_proof"
        ),
        "chen_fig7_unexplained_t_junction_split_provenance_tolerance": float(tolerance),
        "chen_fig7_unexplained_t_junction_split_endpoint_count": int(
            len(endpoint_nodes)
        ),
        "chen_fig7_unexplained_t_junction_lies_on_split_line_count": int(
            len(on_line_nodes)
        ),
        "chen_fig7_unexplained_t_junction_split_unknown_count": int(len(unknown_nodes)),
        "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_count": int(
            len(endpoint_nodes & straight_nodes)
        ),
        "chen_fig7_unexplained_t_junction_split_endpoint_kinked_count": int(
            len(endpoint_nodes & kinked_nodes)
        ),
        (
            "chen_fig7_unexplained_t_junction_lies_on_split_line_straight_through_count"
        ): int(len(on_line_nodes & straight_nodes)),
        "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_count": int(
            len(on_line_nodes & kinked_nodes)
        ),
        "chen_fig7_unexplained_t_junction_split_unknown_straight_through_count": int(
            len(unknown_nodes & straight_nodes)
        ),
        "chen_fig7_unexplained_t_junction_split_unknown_kinked_count": int(
            len(unknown_nodes & kinked_nodes)
        ),
        "chen_fig7_unexplained_t_junction_split_endpoint_source_counts": (
            dict(sorted(endpoint_source_counts.items()))
        ),
        "chen_fig7_unexplained_t_junction_lies_on_split_line_source_counts": (
            dict(sorted(on_line_source_counts.items()))
        ),
        (
            "chen_fig7_unexplained_t_junction_split_endpoint_"
            "straight_through_source_counts"
        ): dict(sorted(endpoint_straight_source_counts.items())),
        ("chen_fig7_unexplained_t_junction_split_endpoint_kinked_source_counts"): dict(
            sorted(endpoint_kinked_source_counts.items())
        ),
        (
            "chen_fig7_unexplained_t_junction_lies_on_split_line_"
            "straight_through_source_counts"
        ): dict(sorted(on_line_straight_source_counts.items())),
        (
            "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_source_counts"
        ): dict(sorted(on_line_kinked_source_counts.items())),
        (
            "chen_fig7_unexplained_t_junction_split_unknown_"
            "straight_through_source_counts"
        ): dict(sorted(unknown_straight_source_counts.items())),
        ("chen_fig7_unexplained_t_junction_split_unknown_kinked_source_counts"): dict(
            sorted(unknown_kinked_source_counts.items())
        ),
        "chen_fig7_unexplained_t_junction_split_endpoint_samples": tuple(
            endpoint_samples
        ),
        "chen_fig7_unexplained_t_junction_lies_on_split_line_samples": tuple(
            on_line_samples
        ),
        "chen_fig7_unexplained_t_junction_split_unknown_samples": tuple(
            unknown_samples
        ),
    }


def _chen_fig7_unexplained_t_junction_nodes(
    layout: ChenLayout,
) -> tuple[set[int], set[int], set[int]]:
    adjacency = _corner_graph_adjacency(layout)
    interior_t_nodes = {
        node
        for node, neighbors in adjacency.items()
        if len(neighbors) == 3 and not layout.mesh.vertices[node].on_boundary
    }
    candidate_path_nodes = {
        node
        for candidate in chen_fig7_short_edge_candidates(layout)
        for node in candidate.path
    }
    unexplained_nodes = interior_t_nodes - candidate_path_nodes
    straight_nodes: set[int] = set()
    kinked_nodes: set[int] = set()
    for node in unexplained_nodes:
        max_angle = _max_incident_angle(layout, node, adjacency.get(node, set()))
        if max_angle + 1e-12 >= CHEN_COLLINEAR_THRESHOLD_RAD:
            straight_nodes.add(node)
        else:
            kinked_nodes.add(node)
    return unexplained_nodes, straight_nodes, kinked_nodes


def _corner_graph_adjacency(layout: ChenLayout) -> dict[int, set[int]]:
    adjacency: dict[int, set[int]] = {}
    for start, end in layout.corner_graph.edges:
        adjacency.setdefault(start, set()).add(end)
        adjacency.setdefault(end, set()).add(start)
    return adjacency


def _max_incident_angle(layout: ChenLayout, node: int, neighbors: set[int]) -> float:
    origin = layout.mesh.vertices[node].point
    vectors: list[tuple[float, float]] = []
    for neighbor in sorted(neighbors):
        point = layout.mesh.vertices[neighbor].point
        vector = (float(point[0] - origin[0]), float(point[1] - origin[1]))
        if math.hypot(vector[0], vector[1]) > 1e-12:
            vectors.append(vector)

    max_angle = 0.0
    for left_index, left in enumerate(vectors):
        for right in vectors[left_index + 1 :]:
            left_length = math.hypot(left[0], left[1])
            right_length = math.hypot(right[0], right[1])
            if left_length <= 1e-12 or right_length <= 1e-12:
                continue
            dot = (left[0] * right[0] + left[1] * right[1]) / (
                left_length * right_length
            )
            max_angle = max(max_angle, math.acos(max(-1.0, min(1.0, dot))))
    return max_angle


def _split_provenance_tolerance(
    layout: ChenLayout, split_lines: list[LineString]
) -> float:
    bounds = [
        parcel.geom.bounds
        for parcel in layout.mesh.parcels.values()
        if not parcel.geom.is_empty
    ]
    bounds.extend(line.bounds for line in split_lines if not line.is_empty)
    if not bounds:
        return _GEOMETRY_TOLERANCE * 10.0
    minx = min(bound[0] for bound in bounds)
    miny = min(bound[1] for bound in bounds)
    maxx = max(bound[2] for bound in bounds)
    maxy = max(bound[3] for bound in bounds)
    span = math.hypot(maxx - minx, maxy - miny)
    return max(span * 1e-7, _GEOMETRY_TOLERANCE * 10.0)


def _split_line_attribution(
    point: tuple[float, float],
    split_lines: list[LineString],
    accepted_split_diagnostics: list[dict[str, Any]],
    *,
    tolerance: float,
) -> dict[str, Any]:
    point_geom = Point(point)
    best_endpoint: tuple[float, int, int, str, float] | None = None
    best_on_line: tuple[float, int, str, float] | None = None
    for split_index, line in enumerate(split_lines):
        coords = [(float(x), float(y)) for x, y in line.coords]
        if len(coords) < 2:
            continue
        source = _accepted_split_source(accepted_split_diagnostics, split_index)
        for endpoint_index, endpoint in ((0, coords[0]), (len(coords) - 1, coords[-1])):
            distance = _point_distance(point, endpoint)
            if distance <= tolerance and (
                best_endpoint is None or distance < best_endpoint[0]
            ):
                projection = float(line.project(Point(endpoint)))
                best_endpoint = (
                    distance,
                    split_index,
                    endpoint_index,
                    source,
                    projection,
                )
        distance_to_line = float(line.distance(point_geom))
        if distance_to_line > tolerance:
            continue
        projection = float(line.project(point_geom))
        if projection <= tolerance or line.length - projection <= tolerance:
            continue
        if best_on_line is None or distance_to_line < best_on_line[0]:
            best_on_line = (distance_to_line, split_index, source, projection)

    if best_endpoint is not None:
        distance, split_index, endpoint_index, source, projection = best_endpoint
        return {
            "kind": "endpoint",
            "split_index": int(split_index),
            "split_id": int(split_index + 1),
            "source": source,
            "endpoint_index": int(endpoint_index),
            "distance": float(distance),
            "projection": float(projection),
        }
    if best_on_line is not None:
        distance, split_index, source, projection = best_on_line
        return {
            "kind": "on_line",
            "split_index": int(split_index),
            "split_id": int(split_index + 1),
            "source": source,
            "endpoint_index": None,
            "distance": float(distance),
            "projection": float(projection),
        }
    return {
        "kind": "unknown",
        "split_index": None,
        "split_id": None,
        "source": "unknown",
        "endpoint_index": None,
        "distance": None,
        "projection": None,
    }


def _accepted_split_source(
    accepted_split_diagnostics: list[dict[str, Any]], split_index: int
) -> str:
    if 0 <= split_index < len(accepted_split_diagnostics):
        source = accepted_split_diagnostics[split_index].get("source")
        if source is not None:
            return str(source)
    return "unknown"


def _split_line_attribution_sample(
    layout: ChenLayout, node: int, attribution: dict[str, Any]
) -> dict[str, Any]:
    point = layout.mesh.vertices[node].point
    return {
        "node": int(node),
        "point": _rounded_metric_point(point),
        "kind": attribution["kind"],
        "split_id": attribution["split_id"],
        "source": attribution["source"],
        "endpoint_index": attribution["endpoint_index"],
        "distance": attribution["distance"],
        "projection": attribution["projection"],
    }


def _rounded_metric_point(point: tuple[float, float]) -> tuple[float, float]:
    return (round(float(point[0]), 9), round(float(point[1]), 9))


def _increment_count(counts: dict[str, int], value: Any) -> None:
    key = str(value)
    counts[key] = counts.get(key, 0) + 1


def _street_density_metrics(layout: ChenLayout) -> dict[str, Any]:
    street_edge_count = int(len(layout.street_network.edges))
    corner_graph_edge_count = int(len(layout.corner_graph.edges))
    return {
        "street_edge_count": street_edge_count,
        "corner_graph_edge_count": corner_graph_edge_count,
        "street_edge_density": (
            float(street_edge_count) / float(corner_graph_edge_count)
            if corner_graph_edge_count
            else 0.0
        ),
    }


def _streamline_stage_metrics(
    config: StreamlineConfig | None, mode: str, *, line_mode: str
) -> dict[str, Any]:
    if mode == STREAMLINE_MODE_BASELINE and config is None:
        return {
            "implementation_stage": _BASELINE_IMPLEMENTATION_STAGE,
            "streamline_field_stage": _BASELINE_STREAMLINE_FIELD_STAGE,
            "streamline_field_scope": _BASELINE_STREAMLINE_FIELD_SCOPE,
            "split_topology_cleanup_stage": _SPLIT_TOPOLOGY_CLEANUP_STAGE_DISABLED,
            "split_topology_cleanup_scope": "not_enabled_for_baseline_streamlines",
        }
    if (
        config is not None
        and config.field_mode == "yang_d_field"
        and config.candidate_seed_mode == "yang_mesh_vertices"
        and config.streamline_score_mode == "yang_div_db_ds_ct"
    ):
        return {
            "implementation_stage": _YANG_D_FIELD_IMPLEMENTATION_STAGE,
            "streamline_field_stage": _YANG_D_FIELD_STREAMLINE_FIELD_STAGE,
            "streamline_field_scope": _YANG_D_FIELD_STREAMLINE_FIELD_SCOPE,
            "split_topology_cleanup_stage": (
                _SPLIT_TOPOLOGY_CLEANUP_STAGE_YANG_CONTINUATION
            ),
            "split_topology_cleanup_scope": (
                _SPLIT_TOPOLOGY_CLEANUP_SCOPE_YANG_CONTINUATION
            ),
        }
    if (
        config is not None
        and config.field_mode == "yang_b_field"
        and config.candidate_seed_mode == "yang_mesh_vertices"
        and config.streamline_score_mode == "yang_div_db_ds_ct"
    ):
        return {
            "implementation_stage": _YANG_B_FIELD_IMPLEMENTATION_STAGE,
            "streamline_field_stage": _YANG_B_FIELD_STREAMLINE_FIELD_STAGE,
            "streamline_field_scope": _YANG_B_FIELD_STREAMLINE_FIELD_SCOPE,
            "split_topology_cleanup_stage": (
                _SPLIT_TOPOLOGY_CLEANUP_STAGE_YANG_CONTINUATION
            ),
            "split_topology_cleanup_scope": (
                _SPLIT_TOPOLOGY_CLEANUP_SCOPE_YANG_CONTINUATION
            ),
        }
    if config is None:
        field_stage = _BASELINE_STREAMLINE_FIELD_STAGE
        scope = _BASELINE_STREAMLINE_FIELD_SCOPE
    else:
        field_stage = f"{config.field_mode}_custom_streamline_config"
        scope = (
            "custom_streamline_config:"
            f"seed={config.candidate_seed_mode}:score={config.streamline_score_mode}"
        )
    return {
        "implementation_stage": (
            "chen_custom_streamline_bounded_junction_shapeop_like_v1"
        ),
        "streamline_field_stage": field_stage,
        "streamline_field_scope": scope,
        "split_topology_cleanup_stage": _SPLIT_TOPOLOGY_CLEANUP_STAGE_DISABLED,
        "split_topology_cleanup_scope": "not_enabled_for_custom_streamline_config",
    }


def _accepted_streamline_diagnostic_metrics(
    accepted_split_diagnostics: list[dict[str, Any]],
) -> dict[str, Any]:
    accepted = [
        diagnostic
        for diagnostic in accepted_split_diagnostics
        if diagnostic.get("source") == "streamline"
    ]
    metrics: dict[str, Any] = {
        "accepted_streamline_candidate_count": int(len(accepted)),
        "accepted_streamline_field_mode_counts": _value_counts(
            diagnostic.get("field_mode") for diagnostic in accepted
        ),
        "accepted_streamline_trace_seed_source_counts": _value_counts(
            diagnostic.get("trace_seed_source") for diagnostic in accepted
        ),
        "accepted_streamline_score_mode_counts": _value_counts(
            diagnostic.get("score_mode") for diagnostic in accepted
        ),
        "accepted_streamline_yang_score_count": int(
            sum(
                1
                for diagnostic in accepted
                if diagnostic.get("score_mode") == "yang_div_db_ds_ct"
            )
        ),
        "accepted_streamline_score_approximation_scopes": tuple(
            sorted(
                {
                    str(diagnostic["score_approximation_scope"])
                    for diagnostic in accepted
                    if diagnostic.get("score_approximation_scope")
                }
            )
        ),
    }
    metrics["accepted_streamline_field_b_approximation_scopes"] = tuple(
        sorted(
            {
                str(diagnostic["field_b_approximation_scope"])
                for diagnostic in accepted
                if diagnostic.get("field_b_approximation_scope")
            }
        )
    )
    metrics["accepted_streamline_field_b_boundary_anchor_methods"] = tuple(
        sorted(
            {
                str(diagnostic["field_b_boundary_anchor_method"])
                for diagnostic in accepted
                if diagnostic.get("field_b_boundary_anchor_method")
            }
        )
    )
    metrics["accepted_streamline_field_mesh_kinds"] = tuple(
        sorted(
            {
                str(diagnostic["field_mesh_kind"])
                for diagnostic in accepted
                if diagnostic.get("field_mesh_kind")
            }
        )
    )
    for key in _ACCEPTED_STREAMLINE_NUMERIC_DIAGNOSTIC_KEYS:
        values = [
            float(diagnostic[key])
            for diagnostic in accepted
            if isinstance(diagnostic.get(key), (int, float))
            and math.isfinite(float(diagnostic[key]))
        ]
        metrics[f"accepted_streamline_{key}_count"] = int(len(values))
        if values:
            metrics[f"accepted_streamline_{key}_min"] = float(min(values))
            metrics[f"accepted_streamline_{key}_mean"] = float(
                sum(values) / len(values)
            )
            metrics[f"accepted_streamline_{key}_max"] = float(max(values))
        else:
            metrics[f"accepted_streamline_{key}_min"] = 0.0
            metrics[f"accepted_streamline_{key}_mean"] = 0.0
            metrics[f"accepted_streamline_{key}_max"] = 0.0
    return metrics


def _value_counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if value is None:
            continue
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _accepted_split_diagnostic(accepted: _SplitCandidate) -> dict[str, Any]:
    return {
        "source": accepted.source,
        "is_axis_aligned": accepted.is_axis_aligned,
        "is_curved": accepted.is_curved,
        "point_count": len(accepted.segment.coords),
        "score_total": accepted.score.total,
        "q_size": accepted.score.q_size,
        "q_regu": accepted.score.q_regu,
        "q_acce": accepted.score.q_acce,
        **accepted.diagnostics,
    }


def _next_split(
    boundary: Polygon,
    faces: list[Polygon],
    split_lines: list[LineString],
    *,
    target_area: float,
    seed: int,
    iteration: int,
    line_mode: str,
    streamline_config: StreamlineConfig | None,
    diagnostics: dict[str, Any],
) -> _SplitCandidate | None:
    for face in sorted(faces, key=_largest_face_key):
        diagnostics["candidate_face_attempt_count"] += 1
        candidate = _best_split_for_face(
            boundary,
            face,
            len(faces),
            split_lines,
            target_area=target_area,
            seed=seed,
            iteration=iteration,
            line_mode=line_mode,
            streamline_config=streamline_config,
            diagnostics=diagnostics,
        )
        if candidate is not None:
            return candidate
    return None


def _best_split_for_face(
    boundary: Polygon,
    face: Polygon,
    current_face_count: int,
    split_lines: list[LineString],
    *,
    target_area: float,
    seed: int,
    iteration: int,
    line_mode: str,
    streamline_config: StreamlineConfig | None,
    diagnostics: dict[str, Any],
) -> _SplitCandidate | None:
    access_lines = [LineString(boundary.exterior.coords), *split_lines]
    candidate_groups: list[list[_CutLineCandidate]] = []
    if line_mode != "axis_aligned_rectangle" and _yang_continuation_cleanup_enabled(
        streamline_config
    ):
        continuation = _streamline_continuation_cut_line_candidates(
            boundary,
            face,
            tuple(split_lines),
        )
        diagnostics["streamline_continuation_candidate_count"] += len(continuation)
        candidate_groups.append(continuation)
    primary = _candidate_cut_lines(
        face,
        line_mode=line_mode,
        seed=seed,
        iteration=iteration,
        existing_lines=tuple(split_lines),
        streamline_config=streamline_config,
        diagnostics=diagnostics,
    )
    candidate_groups.append(primary)
    if line_mode != "axis_aligned_rectangle":
        fallback = _axis_aligned_cut_line_candidates(face, source="axis_fallback")
        diagnostics["axis_aligned_candidate_count"] += len(fallback)
        candidate_groups.append(fallback)

    for cut_lines in candidate_groups:
        candidates = _score_cut_lines(
            boundary,
            face,
            current_face_count,
            split_lines,
            cut_lines,
            access_lines,
            target_area=target_area,
            diagnostics=diagnostics,
        )
        if candidates:
            return max(candidates, key=lambda candidate: candidate.score_key)
    return None


def _score_cut_lines(
    boundary: Polygon,
    face: Polygon,
    current_face_count: int,
    split_lines: list[LineString],
    cut_lines: list[_CutLineCandidate],
    access_lines: list[LineString],
    *,
    target_area: float,
    diagnostics: dict[str, Any],
) -> list[_SplitCandidate]:
    candidates: list[_SplitCandidate] = []
    seen_segments: set[tuple[tuple[int, int], ...]] = set()
    for cut in cut_lines:
        diagnostics["candidate_split_attempt_count"] += 1
        split_result = _split_face(face, cut.line)
        if split_result is None:
            diagnostics["candidate_split_reject_count"] += 1
            continue
        parts, segment = split_result
        segment_key = _line_key(segment)
        if segment_key in seen_segments:
            diagnostics["candidate_split_reject_count"] += 1
            continue
        seen_segments.add(segment_key)

        trial_faces = _polygonize_faces(boundary, [*split_lines, segment])
        if len(trial_faces) != current_face_count + 1:
            diagnostics["candidate_split_reject_count"] += 1
            continue
        trial_state = _trial_corner_graph_state(boundary, trial_faces)
        if trial_state is None:
            diagnostics["candidate_split_reject_count"] += 1
            diagnostics["candidate_topology_reject_count"] += 1
            continue
        trial_mesh, trial_corner_graph = trial_state

        score = _chen_split_score_for_trial_parts(
            parts,
            trial_mesh,
            trial_corner_graph,
            access_lines,
            target_area=target_area,
            diagnostics=diagnostics,
        )
        area_error = sum(abs(part.area - target_area) for part in parts)
        score_key = (
            score.total,
            score.q_size,
            score.q_regu,
            score.q_acce,
            -area_error / max(target_area, _AREA_EPSILON),
            cut.quality,
            segment.length,
        )
        candidates.append(
            _SplitCandidate(
                score,
                segment,
                parts,
                score_key,
                cut.source,
                _line_is_axis_aligned(segment),
                _line_is_curved(segment),
                cut.diagnostics,
            )
        )
    return candidates


def _chen_split_score_for_trial_parts(
    parts: tuple[Polygon, Polygon],
    trial_mesh: ParcelMesh,
    trial_corner_graph: ParcelCornerGraph,
    access_lines: list[LineString],
    *,
    target_area: float,
    diagnostics: dict[str, Any],
) -> ChenSplitScore:
    trial_parcels = _matching_trial_parcels(parts, trial_mesh)
    if trial_parcels is None:
        diagnostics["path_access_score_fallback_count"] += 1
        return chen_split_score(parts, access_lines, target_area=target_area)

    tolerance = chen_access_tolerance(target_area)
    access_ratios = [
        parcel_access_ratio_from_path_geometry(
            parcel,
            trial_mesh,
            trial_corner_graph,
            access_lines,
            corner_ring=trial_corner_graph.parcel_corner_rings.get(parcel.parcel_id),
            tolerance=tolerance,
        )
        for parcel in trial_parcels
    ]
    diagnostics["path_access_score_count"] += 1
    return chen_split_score_from_access_ratios(
        parts,
        access_ratios,
        target_area=target_area,
    )


def _matching_trial_parcels(
    parts: tuple[Polygon, Polygon], trial_mesh: ParcelMesh
) -> tuple[ParcelFace, ParcelFace] | None:
    remaining = set(trial_mesh.parcels)
    matched: list[ParcelFace] = []
    for part in parts:
        best_id: int | None = None
        best_difference = math.inf
        for parcel_id in remaining:
            parcel = trial_mesh.parcels[parcel_id]
            difference = float(parcel.geom.symmetric_difference(part).area)
            if difference < best_difference:
                best_id = parcel_id
                best_difference = difference
        if best_id is None:
            return None
        parcel = trial_mesh.parcels[best_id]
        tolerance = max(float(part.area), float(parcel.geom.area), 1.0) * 1e-8
        if best_difference > tolerance:
            return None
        remaining.remove(best_id)
        matched.append(parcel)
    return (matched[0], matched[1])


def _trial_corner_graph_state(
    boundary: Polygon, faces: list[Polygon]
) -> tuple[ParcelMesh, ParcelCornerGraph] | None:
    try:
        mesh = parcel_mesh_from_polygons(
            [(index + 1, face) for index, face in enumerate(faces)],
            boundary=boundary,
        )
        corner_graph = parcel_corner_graph(mesh)
        parcel_graph(mesh, corner_graph)
    except ValueError:
        return None
    return mesh, corner_graph


def _candidate_cut_lines(
    poly: Polygon,
    *,
    line_mode: str,
    seed: int,
    iteration: int,
    existing_lines: tuple[LineString, ...],
    streamline_config: StreamlineConfig | None,
    diagnostics: dict[str, Any],
) -> list[_CutLineCandidate]:
    if line_mode == "axis_aligned_rectangle":
        candidates = _axis_aligned_cut_line_candidates(poly, source="axis_aligned")
        diagnostics["axis_aligned_candidate_count"] += len(candidates)
        return candidates
    try:
        streamlines = candidate_streamlines(
            poly,
            target_count=12,
            seed=seed * 1_000_003 + iteration * 97_409,
            existing_lines=existing_lines,
            config=streamline_config
            if streamline_config is not None
            else StreamlineConfig(),
        )
    except ValueError:
        streamlines = ()
    diagnostics["streamline_candidate_count"] += len(streamlines)
    return [
        _CutLineCandidate(
            candidate.line,
            "streamline",
            candidate.quality,
            {
                **candidate.diagnostics,
                "streamline_bend": candidate.bend,
                "streamline_quality": candidate.quality,
                "streamline_endpoint_distance_max": candidate.endpoint_distance_max,
            },
        )
        for candidate in streamlines
    ]


def _yang_continuation_cleanup_enabled(config: StreamlineConfig | None) -> bool:
    return (
        config is not None
        and config.field_mode in {"yang_d_field", "yang_b_field"}
        and config.candidate_seed_mode == "yang_mesh_vertices"
        and config.streamline_score_mode == "yang_div_db_ds_ct"
    )


def _streamline_continuation_cut_line_candidates(
    boundary: Polygon,
    face: Polygon,
    split_lines: tuple[LineString, ...],
) -> list[_CutLineCandidate]:
    if not split_lines:
        return []

    scale = math.sqrt(max(boundary.area, _AREA_EPSILON))
    tolerance = max(scale * 1e-6, _GEOMETRY_TOLERANCE * 10.0)
    face_tolerance = max(scale * 1e-5, _GEOMETRY_TOLERANCE * 100.0)
    boundary_line = LineString(boundary.exterior.coords)
    face_boundary = face.boundary
    minx, miny, maxx, maxy = face.bounds
    face_span = max(
        math.hypot(maxx - minx, maxy - miny),
        _GEOMETRY_TOLERANCE * 100.0,
    )
    candidates: list[_CutLineCandidate] = []
    seen: set[tuple[tuple[int, int], ...]] = set()

    for split_line_index, split_line in enumerate(split_lines):
        coords = [(float(x), float(y)) for x, y in split_line.coords]
        if len(coords) < 2:
            continue
        source_is_curved = _line_is_curved(split_line)
        for at_start in (True, False):
            endpoint = coords[0] if at_start else coords[-1]
            endpoint_point = Point(endpoint)
            if boundary_line.distance(endpoint_point) <= tolerance:
                continue
            if face_boundary.distance(endpoint_point) > face_tolerance:
                continue

            continuation_line = _continuation_line_from_endpoint(
                coords,
                at_start=at_start,
                span=face_span * 2.25,
            )
            continuation_coords = [
                (float(x), float(y)) for x, y in continuation_line.coords
            ]
            if len(continuation_coords) < 2:
                continue
            probe = continuation_coords[min(2, len(continuation_coords) - 1)]
            if not face.buffer(face_tolerance).covers(Point(probe)):
                continue
            if face_boundary.distance(Point(probe)) <= tolerance:
                continue

            key = _line_key(continuation_line)
            reverse_key = tuple(reversed(key))
            if key in seen or reverse_key in seen:
                continue
            seen.add(key)
            candidates.append(
                _CutLineCandidate(
                    continuation_line,
                    "streamline_continuation",
                    2.0 if source_is_curved else 1.5,
                    {
                        "continuation_source_split_index": int(split_line_index),
                        "continuation_source_endpoint": (
                            "start" if at_start else "end"
                        ),
                        "continuation_source_is_curved": bool(source_is_curved),
                        "continuation_cleanup_stage": (
                            _SPLIT_TOPOLOGY_CLEANUP_STAGE_YANG_CONTINUATION
                        ),
                    },
                )
            )
    return candidates


def _continuation_line_from_endpoint(
    coords: list[tuple[float, float]],
    *,
    at_start: bool,
    span: float,
) -> LineString:
    ordered = list(reversed(coords)) if at_start else list(coords)
    endpoint = ordered[-1]
    previous = ordered[-2]
    heading = math.atan2(endpoint[1] - previous[1], endpoint[0] - previous[0])
    if len(ordered) >= 3:
        before_previous = ordered[-3]
        previous_heading = math.atan2(
            previous[1] - before_previous[1],
            previous[0] - before_previous[0],
        )
        turn = _signed_angle_delta(heading, previous_heading)
    else:
        turn = 0.0
    turn = _clip_float(turn, -math.radians(14.0), math.radians(14.0))

    recent_lengths = [
        _point_distance(a, b)
        for a, b in zip(ordered[-6:], ordered[-5:], strict=False)
        if _point_distance(a, b) > _GEOMETRY_TOLERANCE
    ]
    step = sum(recent_lengths) / len(recent_lengths) if recent_lengths else span / 10.0
    step = _clip_float(step, span / 18.0, span / 7.0)

    points = [endpoint]
    current = endpoint
    traveled = 0.0
    while traveled < span and len(points) < 24:
        heading += turn
        current = (
            current[0] + math.cos(heading) * step,
            current[1] + math.sin(heading) * step,
        )
        points.append(current)
        traveled += step
    return LineString(points)


def _signed_angle_delta(angle: float, reference: float) -> float:
    return math.atan2(math.sin(angle - reference), math.cos(angle - reference))


def _clip_float(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _axis_aligned_cut_line_candidates(
    poly: Polygon, *, source: str
) -> list[_CutLineCandidate]:
    lines: list[_CutLineCandidate] = []
    for line in _axis_aligned_cut_lines(poly):
        lines.append(_CutLineCandidate(line, source, 0.0, {}))
    return lines


def _axis_aligned_cut_lines(poly: Polygon) -> list[LineString]:
    cut_fractions = (0.5, 0.375, 0.625, 0.25, 0.75, 0.125, 0.875)
    return [
        line
        for angle in (0.0, math.pi * 0.5)
        for line in _parallel_cut_lines(
            poly,
            angle=angle,
            cut_fractions=cut_fractions,
        )
    ]


def _principal_cut_lines(
    poly: Polygon, *, seed: int, iteration: int
) -> list[LineString]:
    base_angle = _principal_angle(poly)
    angle_offsets = (
        0.0,
        math.pi * 0.5,
        math.pi * 0.25,
        -math.pi * 0.25,
        math.pi * 0.125,
        -math.pi * 0.125,
    )
    cut_fractions = (0.5, 0.42, 0.58, 0.34, 0.66, 0.26, 0.74)
    return [
        line
        for offset in angle_offsets
        for line in _parallel_cut_lines(
            poly,
            angle=base_angle + offset,
            cut_fractions=cut_fractions,
        )
    ]


def _parallel_cut_lines(
    poly: Polygon, *, angle: float, cut_fractions: tuple[float, ...]
) -> list[LineString]:
    minx, miny, maxx, maxy = poly.bounds
    span = math.hypot(maxx - minx, maxy - miny) * 2.0
    if span <= _GEOMETRY_TOLERANCE:
        return []

    ux = math.cos(angle)
    uy = math.sin(angle)
    nx = -uy
    ny = ux
    coords = [(float(x), float(y)) for x, y in poly.exterior.coords[:-1]]
    projections = [x * nx + y * ny for x, y in coords]
    lo = min(projections)
    hi = max(projections)
    if hi - lo <= _GEOMETRY_TOLERANCE:
        return []

    centroid = poly.centroid
    centroid_projection = float(centroid.x) * nx + float(centroid.y) * ny
    lines: list[LineString] = []
    for fraction in cut_fractions:
        cut_projection = lo + (hi - lo) * fraction
        dx = nx * (cut_projection - centroid_projection)
        dy = ny * (cut_projection - centroid_projection)
        px = float(centroid.x) + dx
        py = float(centroid.y) + dy
        lines.append(
            LineString(
                [
                    (px - ux * span, py - uy * span),
                    (px + ux * span, py + uy * span),
                ]
            )
        )
    return lines


def _principal_angle(poly: Polygon) -> float:
    rect = poly.minimum_rotated_rectangle
    coords = [(float(x), float(y)) for x, y in rect.exterior.coords[:-1]]
    if len(coords) < 2:
        return 0.0
    edges = [
        (
            math.hypot(b[0] - a[0], b[1] - a[1]),
            math.atan2(b[1] - a[1], b[0] - a[0]),
        )
        for a, b in zip(coords, coords[1:] + coords[:1], strict=True)
    ]
    return max(edges, key=lambda item: item[0])[1]


def _split_face(
    face: Polygon, cut_line: LineString
) -> tuple[tuple[Polygon, Polygon], LineString] | None:
    cut_line = _extended_split_line(face, cut_line)
    try:
        pieces = split(face, cut_line)
    except (ValueError, shapely.GEOSException):
        return None

    parts = [
        poly
        for poly in (largest_polygon(piece) for piece in pieces.geoms)
        if poly is not None and poly.area > face.area * _AREA_EPSILON
    ]
    if len(parts) != 2:
        return None

    shared = parts[0].boundary.intersection(parts[1].boundary)
    line_parts = _line_parts(shared)
    if len(line_parts) != 1:
        line_parts = _line_parts(linemerge(shared))
    if len(line_parts) != 1:
        return None
    segment = _canonical_line(line_parts[0])
    if segment.length <= _GEOMETRY_TOLERANCE:
        return None

    return (parts[0], parts[1]), segment


def _extended_split_line(face: Polygon, line: LineString) -> LineString:
    coords = [(float(x), float(y)) for x, y in line.coords]
    if len(coords) < 2:
        return line
    extension = max(
        math.hypot(face.bounds[2] - face.bounds[0], face.bounds[3] - face.bounds[1])
        * 1e-6,
        _GEOMETRY_TOLERANCE * 10.0,
    )
    coords[0] = _extend_endpoint(coords[0], coords[1], extension)
    coords[-1] = _extend_endpoint(coords[-1], coords[-2], extension)
    return LineString(coords)


def _extend_endpoint(
    endpoint: tuple[float, float], neighbor: tuple[float, float], amount: float
) -> tuple[float, float]:
    dx = endpoint[0] - neighbor[0]
    dy = endpoint[1] - neighbor[1]
    length = math.hypot(dx, dy)
    if length <= 1e-12:
        return endpoint
    return endpoint[0] + dx / length * amount, endpoint[1] + dy / length * amount


def _polygonize_faces(
    boundary: Polygon, split_lines: list[LineString]
) -> list[Polygon]:
    linework = unary_union([LineString(boundary.exterior.coords), *split_lines])
    min_area = max(boundary.area * _AREA_EPSILON, _AREA_EPSILON)
    boundary_cover = boundary.buffer(_GEOMETRY_TOLERANCE)
    faces = [
        _clean_polygon(face)
        for face in polygonize(linework)
        if face.area > min_area and boundary_cover.covers(face.representative_point())
    ]
    faces.sort(key=_face_order_key)
    return faces


def _line_parts(geom) -> list[LineString]:
    if isinstance(geom, LineString):
        return [geom] if geom.length > _GEOMETRY_TOLERANCE else []
    if isinstance(geom, MultiLineString):
        return [
            part
            for part in geom.geoms
            if isinstance(part, LineString) and part.length > _GEOMETRY_TOLERANCE
        ]
    if hasattr(geom, "geoms"):
        parts: list[LineString] = []
        for part in geom.geoms:
            parts.extend(_line_parts(part))
        return parts
    return []


def _canonical_line(line: LineString) -> LineString:
    coords = [(float(x), float(y)) for x, y in line.coords]
    if len(coords) < 2:
        return line
    if coords[-1] < coords[0]:
        coords.reverse()
    return LineString(coords)


def _line_key(line: LineString) -> tuple[tuple[int, int], ...]:
    return tuple(
        (round(x / _GEOMETRY_TOLERANCE), round(y / _GEOMETRY_TOLERANCE))
        for x, y in line.coords
    )


def _face_order_key(poly: Polygon) -> tuple[float, ...]:
    minx, miny, maxx, maxy = poly.bounds
    centroid = poly.centroid
    return (
        round(float(centroid.y), 9),
        round(float(centroid.x), 9),
        round(float(minx), 9),
        round(float(miny), 9),
        round(float(maxx), 9),
        round(float(maxy), 9),
        round(float(poly.area), 9),
    )


def _largest_face_key(poly: Polygon) -> tuple[float, ...]:
    order = _face_order_key(poly)
    return (-round(float(poly.area), 9), *order)
