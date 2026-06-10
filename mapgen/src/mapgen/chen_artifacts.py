"""Static artifacts for strict Chen layout runs."""

from __future__ import annotations

import html
import json
import math
import struct
import zlib
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import shapely
from shapely import LineString, MultiLineString, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry

from mapgen.chen_core import (
    ChenLayout,
    chen_irregularity,
    evaluate_layout_invariants,
    normalized_edge,
    parcel_access_ratio_from_edges,
)

if TYPE_CHECKING:
    from mapgen.chen_generate import GeneratedChenLayout
else:
    GeneratedChenLayout = Any


SVG_SIZE = 920
PNG_SIZE = 960
MARGIN = 54.0

ARTIFACT_FILES = {
    "svg": "chen_strict_layout.svg",
    "png": "chen_strict_layout.png",
    "metrics": "layout_metrics.json",
    "boundary": "boundary.geojson",
    "parcels": "parcels.geojson",
    "streets": "streets.geojson",
    "split_lines": "split_lines.geojson",
    "manifest": "manifest.json",
}

_OPTIMIZATION_REGULARITY_SUMMARY_DEFAULTS: dict[str, Any] = {
    "optimization_regularity_projection_kind": "disabled",
    "optimization_regularity_projected_parcel_count_before": 0,
    "optimization_regularity_projected_parcel_count_after": 0,
    "optimization_regularity_skipped_parcel_count_before": 0,
    "optimization_regularity_skipped_parcel_count_after": 0,
    "optimization_regularity_skipped_by_reason_before": {},
    "optimization_regularity_skipped_by_reason_after": {},
    "optimization_regularity_projection_equation_count": 0,
    "optimization_regularity_projection_residual_before": 0.0,
    "optimization_regularity_projection_residual_after": 0.0,
    "optimization_regularity_projection_target_displacement_rms_before": 0.0,
    "optimization_regularity_projection_target_displacement_rms_after": 0.0,
    "optimization_regularity_projection_target_displacement_max_before": 0.0,
    "optimization_regularity_projection_target_displacement_max_after": 0.0,
}

PARCEL_PALETTE = (
    (226, 239, 218),
    (222, 235, 247),
    (254, 232, 200),
    (237, 226, 240),
    (224, 243, 219),
    (253, 224, 221),
    (229, 245, 249),
    (242, 240, 247),
)
BACKGROUND = (248, 249, 247)
BOUNDARY_FILL = (241, 244, 238)
PARCEL_STROKE = (124, 132, 124)
BOUNDARY_STROKE = (24, 31, 36)
SPLIT_STROKE = (204, 112, 34)
STREET_STROKE = (33, 45, 56)


class _Transform:
    def __init__(
        self,
        bounds: tuple[float, float, float, float],
        width: int,
        height: int,
        margin: float,
    ) -> None:
        minx, miny, maxx, maxy = bounds
        world_w = max(maxx - minx, 1e-9)
        world_h = max(maxy - miny, 1e-9)
        usable_w = max(width - 2.0 * margin, 1.0)
        usable_h = max(height - 2.0 * margin, 1.0)
        scale = min(usable_w / world_w, usable_h / world_h)
        self.minx = minx
        self.miny = miny
        self.width = float(width)
        self.height = float(height)
        self.scale = scale
        self.xoff = (width - world_w * scale) * 0.5
        self.yoff = (height - world_h * scale) * 0.5

    def point(self, pt: tuple[float, float]) -> tuple[float, float]:
        x = self.xoff + (pt[0] - self.minx) * self.scale
        y = self.height - (self.yoff + (pt[1] - self.miny) * self.scale)
        return x, y


def write_strict_chen_artifacts(
    generated: GeneratedChenLayout, out_dir: Path
) -> dict[str, Any]:
    """Write static visual, GeoJSON, and metric artifacts for one generated run."""
    out_dir.mkdir(parents=True, exist_ok=True)
    layout = _layout(generated)
    boundary = _boundary(generated)
    split_lines = _split_lines(generated)

    metrics = _artifact_metrics(generated, boundary, layout)
    _write_json(out_dir / ARTIFACT_FILES["metrics"], metrics)
    _write_json(
        out_dir / ARTIFACT_FILES["boundary"],
        _feature_collection(
            [
                _geom_feature(
                    boundary,
                    {"kind": "boundary", "name": _name(generated)},
                )
            ]
        ),
    )
    _write_json(
        out_dir / ARTIFACT_FILES["parcels"],
        _feature_collection(_parcel_features(layout)),
    )
    _write_json(
        out_dir / ARTIFACT_FILES["streets"],
        _feature_collection(_street_features(layout)),
    )
    _write_json(
        out_dir / ARTIFACT_FILES["split_lines"],
        _feature_collection(
            [
                _geom_feature(
                    line,
                    {
                        "kind": "split_line",
                        "split_id": split_id,
                        "point_count": len(line.coords),
                        "is_axis_aligned": _line_is_axis_aligned(line),
                        "is_curved": _line_is_curved(line),
                    },
                )
                for split_id, line in enumerate(split_lines, start=1)
            ]
        ),
    )

    _render_svg(
        out_dir / ARTIFACT_FILES["svg"], _name(generated), boundary, layout, split_lines
    )
    _render_png(out_dir / ARTIFACT_FILES["png"], boundary, layout, split_lines)

    manifest = _json_ready(_manifest(generated, metrics))
    _write_json(out_dir / ARTIFACT_FILES["manifest"], manifest)
    return manifest


def _name(generated: GeneratedChenLayout) -> str:
    return str(getattr(generated, "name", "chen-strict"))


def _layout(generated: GeneratedChenLayout) -> ChenLayout:
    layout = getattr(generated, "layout", None)
    if layout is None:
        raise ValueError("generated layout is missing a 'layout' attribute")
    return layout


def _boundary(generated: GeneratedChenLayout) -> Polygon:
    boundary = getattr(generated, "boundary", None)
    if not isinstance(boundary, Polygon):
        raise ValueError("generated layout boundary must be a shapely Polygon")
    return boundary


def _split_lines(generated: GeneratedChenLayout) -> list[LineString]:
    lines: list[LineString] = []
    for value in getattr(generated, "split_lines", ()) or ():
        line = _as_linestring(value)
        if line is not None and len(line.coords) >= 2 and line.length > 1e-12:
            lines.append(line)
    return lines


def _as_linestring(value: Any) -> LineString | None:
    if isinstance(value, LineString):
        return value
    geom = getattr(value, "geom", None)
    if isinstance(geom, LineString):
        return geom
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        try:
            return LineString(value)
        except (TypeError, ValueError):
            return None
    return None


def _artifact_metrics(
    generated: GeneratedChenLayout, boundary: Polygon, layout: ChenLayout
) -> dict[str, Any]:
    input_metrics = getattr(generated, "metrics", {}) or {}
    metrics = dict(input_metrics)
    report = evaluate_layout_invariants(layout, target_boundary=boundary)
    corner_graph_edge_count = int(
        metrics.get(
            "corner_graph_edge_count",
            metrics.get("corner_edge_count", len(layout.corner_graph.edges)),
        )
    )
    street_edge_count = int(
        metrics.get(
            "street_edge_count",
            metrics.get("street_network_edge_count", len(layout.street_network.edges)),
        )
    )
    metrics.setdefault("name", _name(generated))
    metrics.setdefault("parcel_count", len(layout.mesh.parcels))
    metrics.setdefault("street_edge_count", street_edge_count)
    metrics.setdefault("corner_graph_edge_count", corner_graph_edge_count)
    metrics.setdefault(
        "street_edge_density",
        _safe_ratio(street_edge_count, corner_graph_edge_count),
    )
    metrics["strict_invariants"] = {
        "paper_invariant_pass": report.paper_invariant_pass,
        "geometry_valid_pass": report.geometry_valid_pass,
        "chen_formula_pass": report.chen_formula_pass,
        "diagnostic_metric_pass": report.diagnostic_metric_pass,
        "metrics": report.metrics,
    }
    return _json_ready(metrics)


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    denominator_float = float(denominator)
    if denominator_float == 0.0:
        return 0.0
    return float(numerator) / denominator_float


def _parcel_features(layout: ChenLayout) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for parcel_id, parcel in sorted(layout.mesh.parcels.items()):
        corner_ring = layout.corner_graph.parcel_corner_rings.get(parcel_id)
        properties = {
            "kind": "parcel",
            "parcel_id": parcel_id,
            "mesh_vertex_count": len(parcel.ring),
            "corner_count": len(corner_ring or ()),
            "access_ratio": parcel_access_ratio_from_edges(
                parcel, layout.mesh, layout.street_network.edges, corner_ring
            ),
            "irregularity": chen_irregularity(parcel.geom),
        }
        features.append(_geom_feature(parcel.geom, properties))
    return features


def _street_features(layout: ChenLayout) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for line, properties in _street_lines(layout):
        features.append(
            _geom_feature(
                line,
                properties,
            )
        )
    if features:
        return features
    for edge_id, edge in enumerate(sorted(layout.street_network.edges), start=1):
        a, b = edge
        features.append(
            _geom_feature(
                _edge_path_line(layout, a, b),
                {"kind": "street_edge", "street_id": edge_id, "edge": list(edge)},
            )
        )
    return features


def _street_lines(layout: ChenLayout) -> list[tuple[LineString, dict[str, Any]]]:
    lines: list[tuple[LineString, dict[str, Any]]] = []
    for street in layout.street_graph.streets:
        line = _street_path_line(layout, street.nodes)
        if line is None:
            continue
        lines.append(
            (
                line,
                {
                    "kind": "street",
                    "street_id": street.street_id,
                    "node_count": len(street.nodes),
                    "edge_count": len(street.edges),
                    "point_count": len(line.coords),
                    "is_curved": _line_is_curved(line),
                },
            )
        )
    return lines


def _street_path_line(layout: ChenLayout, nodes: tuple[int, ...]) -> LineString | None:
    if len(nodes) < 2:
        return None
    points: list[tuple[float, float]] = []
    for start, end in zip(nodes, nodes[1:], strict=False):
        segment = _edge_path_points(layout, start, end)
        if not points:
            points.extend(segment)
        else:
            points.extend(segment[1:])
    if len(points) < 2:
        return None
    return LineString(points)


def _edge_path_line(layout: ChenLayout, start: int, end: int) -> LineString:
    return LineString(_edge_path_points(layout, start, end))


def _edge_path_points(
    layout: ChenLayout, start: int, end: int
) -> list[tuple[float, float]]:
    edge = normalized_edge(start, end)
    path = layout.corner_graph.edge_paths.get(edge, (start, end))
    if path[0] == end and path[-1] == start:
        path = tuple(reversed(path))
    elif path[0] != start or path[-1] != end:
        path = (start, end)
    return [layout.mesh.vertices[node].point for node in path]


def _line_is_axis_aligned(line: LineString) -> bool:
    coords = [(float(x), float(y)) for x, y in line.coords]
    if len(coords) < 2:
        return False
    return all(
        abs(a[0] - b[0]) <= 1e-7 or abs(a[1] - b[1]) <= 1e-7
        for a, b in zip(coords, coords[1:], strict=False)
    )


def _line_is_curved(line: LineString) -> bool:
    coords = [(float(x), float(y)) for x, y in line.coords]
    if len(coords) <= 2:
        return False
    start = coords[0]
    end = coords[-1]
    chord = math.hypot(end[0] - start[0], end[1] - start[1])
    if chord <= 1e-7:
        return False
    max_offset = 0.0
    for point in coords[1:-1]:
        area2 = abs(
            (end[0] - start[0]) * (start[1] - point[1])
            - (start[0] - point[0]) * (end[1] - start[1])
        )
        max_offset = max(max_offset, area2 / chord)
    return max_offset > max(1e-6, chord * 1e-4)


def _geom_feature(geom: BaseGeometry, properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": json.loads(shapely.to_geojson(geom)),
        "properties": _json_ready(properties),
    }


def _feature_collection(features: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": features}


def _manifest(
    generated: GeneratedChenLayout, metrics: dict[str, Any]
) -> dict[str, Any]:
    return {
        "layout": "chen-strict",
        "artifact_version": 1,
        "name": _name(generated),
        "summary": {
            "parcel_count": metrics.get("parcel_count", 0),
            "street_edge_count": metrics.get("street_edge_count", 0),
            "corner_graph_edge_count": metrics.get("corner_graph_edge_count", 0),
            "street_edge_density": metrics.get("street_edge_density", 0.0),
            "implementation_stage": metrics.get("implementation_stage", "unknown"),
            "boundary_contour_fidelity_stage": metrics.get(
                "boundary_contour_fidelity_stage", "unknown"
            ),
            "oval_boundary_vertex_count": metrics.get("oval_boundary_vertex_count", 0),
            "oval_boundary_normalized_radial_error_max": metrics.get(
                "oval_boundary_normalized_radial_error_max", 0.0
            ),
            "oval_boundary_normalized_radial_error_mean": metrics.get(
                "oval_boundary_normalized_radial_error_mean", 0.0
            ),
            "oval_boundary_radial_error_max": metrics.get(
                "oval_boundary_radial_error_max", 0.0
            ),
            "oval_boundary_radial_error_mean": metrics.get(
                "oval_boundary_radial_error_mean", 0.0
            ),
            "streamline_field_stage": metrics.get("streamline_field_stage", "unknown"),
            "streamline_field_scope": metrics.get("streamline_field_scope", "unknown"),
            "streamline_config_mode": metrics.get("streamline_config_mode", "unknown"),
            "streamline_config_field_mode": metrics.get(
                "streamline_config_field_mode", "unknown"
            ),
            "streamline_config_candidate_seed_mode": metrics.get(
                "streamline_config_candidate_seed_mode", "unknown"
            ),
            "streamline_config_score_mode": metrics.get(
                "streamline_config_score_mode", "unknown"
            ),
            "street_selection_mode": metrics.get("street_selection_mode", "unknown"),
            "split_line_count": metrics.get("split_line_count", 0),
            "axis_aligned_split_line_count": metrics.get(
                "axis_aligned_split_line_count", 0
            ),
            "curved_split_line_count": metrics.get("curved_split_line_count", 0),
            "non_axis_aligned_split_segment_count": metrics.get(
                "non_axis_aligned_split_segment_count", 0
            ),
            "non_axis_aligned_street_segment_count": metrics.get(
                "non_axis_aligned_street_segment_count", 0
            ),
            "max_mesh_axis_deviation": metrics.get("max_mesh_axis_deviation", 0.0),
            "max_split_axis_deviation": metrics.get("max_split_axis_deviation", 0.0),
            "max_street_axis_deviation": metrics.get("max_street_axis_deviation", 0.0),
            "mean_mesh_axis_deviation": metrics.get("mean_mesh_axis_deviation", 0.0),
            "mean_split_axis_deviation": metrics.get("mean_split_axis_deviation", 0.0),
            "mean_street_axis_deviation": metrics.get(
                "mean_street_axis_deviation", 0.0
            ),
            "streamline_candidate_count": metrics.get("streamline_candidate_count", 0),
            "accepted_streamline_candidate_count": metrics.get(
                "accepted_streamline_candidate_count", 0
            ),
            "accepted_streamline_continuation_split_count": metrics.get(
                "accepted_streamline_continuation_split_count", 0
            ),
            "accepted_axis_fallback_split_count": metrics.get(
                "accepted_axis_fallback_split_count", 0
            ),
            "candidate_split_reject_count": metrics.get(
                "candidate_split_reject_count", 0
            ),
            "candidate_topology_reject_count": metrics.get(
                "candidate_topology_reject_count", 0
            ),
            "path_access_score_count": metrics.get("path_access_score_count", 0),
            "path_access_score_fallback_count": metrics.get(
                "path_access_score_fallback_count", 0
            ),
            "accepted_streamline_field_mode_counts": metrics.get(
                "accepted_streamline_field_mode_counts", {}
            ),
            "accepted_streamline_trace_seed_source_counts": metrics.get(
                "accepted_streamline_trace_seed_source_counts", {}
            ),
            "accepted_streamline_score_mode_counts": metrics.get(
                "accepted_streamline_score_mode_counts", {}
            ),
            "accepted_streamline_yang_score_count": metrics.get(
                "accepted_streamline_yang_score_count", 0
            ),
            "accepted_streamline_score_approximation_scopes": metrics.get(
                "accepted_streamline_score_approximation_scopes", []
            ),
            "accepted_streamline_score_div_min": metrics.get(
                "accepted_streamline_score_div_min", 0.0
            ),
            "accepted_streamline_score_div_mean": metrics.get(
                "accepted_streamline_score_div_mean", 0.0
            ),
            "accepted_streamline_score_div_max": metrics.get(
                "accepted_streamline_score_div_max", 0.0
            ),
            "accepted_streamline_score_db_min": metrics.get(
                "accepted_streamline_score_db_min", 0.0
            ),
            "accepted_streamline_score_db_mean": metrics.get(
                "accepted_streamline_score_db_mean", 0.0
            ),
            "accepted_streamline_score_db_max": metrics.get(
                "accepted_streamline_score_db_max", 0.0
            ),
            "accepted_streamline_score_ds_min": metrics.get(
                "accepted_streamline_score_ds_min", 0.0
            ),
            "accepted_streamline_score_ds_mean": metrics.get(
                "accepted_streamline_score_ds_mean", 0.0
            ),
            "accepted_streamline_score_ds_max": metrics.get(
                "accepted_streamline_score_ds_max", 0.0
            ),
            "accepted_streamline_score_ct_min": metrics.get(
                "accepted_streamline_score_ct_min", 0.0
            ),
            "accepted_streamline_score_ct_mean": metrics.get(
                "accepted_streamline_score_ct_mean", 0.0
            ),
            "accepted_streamline_score_ct_max": metrics.get(
                "accepted_streamline_score_ct_max", 0.0
            ),
            "accepted_streamline_score_total_normalized_min": metrics.get(
                "accepted_streamline_score_total_normalized_min", 0.0
            ),
            "accepted_streamline_score_total_normalized_mean": metrics.get(
                "accepted_streamline_score_total_normalized_mean", 0.0
            ),
            "accepted_streamline_score_total_normalized_max": metrics.get(
                "accepted_streamline_score_total_normalized_max", 0.0
            ),
            "accepted_streamline_trace_mesh_interior_seed_count_min": metrics.get(
                "accepted_streamline_trace_mesh_interior_seed_count_min", 0.0
            ),
            "accepted_streamline_trace_mesh_interior_seed_count_mean": metrics.get(
                "accepted_streamline_trace_mesh_interior_seed_count_mean", 0.0
            ),
            "accepted_streamline_trace_mesh_interior_seed_count_max": metrics.get(
                "accepted_streamline_trace_mesh_interior_seed_count_max", 0.0
            ),
            "accepted_streamline_field_mesh_kinds": metrics.get(
                "accepted_streamline_field_mesh_kinds", []
            ),
            "accepted_streamline_field_mesh_vertex_count_min": metrics.get(
                "accepted_streamline_field_mesh_vertex_count_min", 0.0
            ),
            "accepted_streamline_field_mesh_vertex_count_mean": metrics.get(
                "accepted_streamline_field_mesh_vertex_count_mean", 0.0
            ),
            "accepted_streamline_field_mesh_vertex_count_max": metrics.get(
                "accepted_streamline_field_mesh_vertex_count_max", 0.0
            ),
            "accepted_streamline_field_mesh_retained_vertex_count_min": metrics.get(
                "accepted_streamline_field_mesh_retained_vertex_count_min", 0.0
            ),
            "accepted_streamline_field_mesh_retained_vertex_count_mean": metrics.get(
                "accepted_streamline_field_mesh_retained_vertex_count_mean", 0.0
            ),
            "accepted_streamline_field_mesh_retained_vertex_count_max": metrics.get(
                "accepted_streamline_field_mesh_retained_vertex_count_max", 0.0
            ),
            "accepted_streamline_field_mesh_triangle_count_min": metrics.get(
                "accepted_streamline_field_mesh_triangle_count_min", 0.0
            ),
            "accepted_streamline_field_mesh_triangle_count_mean": metrics.get(
                "accepted_streamline_field_mesh_triangle_count_mean", 0.0
            ),
            "accepted_streamline_field_mesh_triangle_count_max": metrics.get(
                "accepted_streamline_field_mesh_triangle_count_max", 0.0
            ),
            "accepted_streamline_field_b_approximation_scopes": metrics.get(
                "accepted_streamline_field_b_approximation_scopes", []
            ),
            "accepted_streamline_field_b_boundary_anchor_methods": metrics.get(
                "accepted_streamline_field_b_boundary_anchor_methods", []
            ),
            "accepted_streamline_field_b_boundary_alignment_weight_min": metrics.get(
                "accepted_streamline_field_b_boundary_alignment_weight_min", 0.0
            ),
            "accepted_streamline_field_b_boundary_alignment_weight_mean": metrics.get(
                "accepted_streamline_field_b_boundary_alignment_weight_mean", 0.0
            ),
            "accepted_streamline_field_b_boundary_alignment_weight_max": metrics.get(
                "accepted_streamline_field_b_boundary_alignment_weight_max", 0.0
            ),
            "accepted_streamline_field_b_boundary_anchor_count_min": metrics.get(
                "accepted_streamline_field_b_boundary_anchor_count_min", 0.0
            ),
            "accepted_streamline_field_b_boundary_anchor_count_mean": metrics.get(
                "accepted_streamline_field_b_boundary_anchor_count_mean", 0.0
            ),
            "accepted_streamline_field_b_boundary_anchor_count_max": metrics.get(
                "accepted_streamline_field_b_boundary_anchor_count_max", 0.0
            ),
            "accepted_streamline_field_b_boundary_alignment_error_min": metrics.get(
                "accepted_streamline_field_b_boundary_alignment_error_min", 0.0
            ),
            "accepted_streamline_field_b_boundary_alignment_error_mean": metrics.get(
                "accepted_streamline_field_b_boundary_alignment_error_mean", 0.0
            ),
            "accepted_streamline_field_b_boundary_alignment_error_max": metrics.get(
                "accepted_streamline_field_b_boundary_alignment_error_max", 0.0
            ),
            "accepted_streamline_field_b_smoothness_energy_min": metrics.get(
                "accepted_streamline_field_b_smoothness_energy_min", 0.0
            ),
            "accepted_streamline_field_b_smoothness_energy_mean": metrics.get(
                "accepted_streamline_field_b_smoothness_energy_mean", 0.0
            ),
            "accepted_streamline_field_b_smoothness_energy_max": metrics.get(
                "accepted_streamline_field_b_smoothness_energy_max", 0.0
            ),
            "accepted_streamline_field_b_solver_residual_max": metrics.get(
                "accepted_streamline_field_b_solver_residual_max", 0.0
            ),
            "street_topology_reachability_pass": metrics.get(
                "street_topology_reachability_pass", False
            ),
            "deprecated_chen_street_generation_pass_alias": metrics.get(
                "deprecated_chen_street_generation_pass_alias", False
            ),
            "chen_street_generation_scope": metrics.get(
                "chen_street_generation_scope", "unknown"
            ),
            "corner_graph_t_junction_count": metrics.get(
                "corner_graph_t_junction_count", 0
            ),
            "corner_graph_four_way_intersection_count": metrics.get(
                "corner_graph_four_way_intersection_count", 0
            ),
            "corner_graph_t_junction_ratio": metrics.get(
                "corner_graph_t_junction_ratio", 0.0
            ),
            "corner_graph_four_way_intersection_ratio": metrics.get(
                "corner_graph_four_way_intersection_ratio", 0.0
            ),
            "interior_corner_graph_t_junction_count": metrics.get(
                "interior_corner_graph_t_junction_count", 0
            ),
            "interior_corner_graph_four_way_intersection_count": metrics.get(
                "interior_corner_graph_four_way_intersection_count", 0
            ),
            "boundary_corner_graph_t_junction_count": metrics.get(
                "boundary_corner_graph_t_junction_count", 0
            ),
            "boundary_corner_graph_four_way_intersection_count": metrics.get(
                "boundary_corner_graph_four_way_intersection_count", 0
            ),
            "street_t_junction_count": metrics.get("street_t_junction_count", 0),
            "street_four_way_intersection_count": metrics.get(
                "street_four_way_intersection_count", 0
            ),
            "street_t_junction_ratio": metrics.get("street_t_junction_ratio", 0.0),
            "street_four_way_intersection_ratio": metrics.get(
                "street_four_way_intersection_ratio", 0.0
            ),
            "interior_street_t_junction_count": metrics.get(
                "interior_street_t_junction_count", 0
            ),
            "interior_street_four_way_intersection_count": metrics.get(
                "interior_street_four_way_intersection_count", 0
            ),
            "boundary_street_t_junction_count": metrics.get(
                "boundary_street_t_junction_count", 0
            ),
            "boundary_street_four_way_intersection_count": metrics.get(
                "boundary_street_four_way_intersection_count", 0
            ),
            "rectangular_interior_t_junction_points_sample": metrics.get(
                "rectangular_interior_t_junction_points_sample", []
            ),
            "chen_fig7_short_edge_detection_stage": metrics.get(
                "chen_fig7_short_edge_detection_stage", "unknown"
            ),
            "chen_fig7_short_edge_cleanup_stage": metrics.get(
                "chen_fig7_short_edge_cleanup_stage", "unknown"
            ),
            "chen_fig7_short_edge_cleanup_applied": metrics.get(
                "chen_fig7_short_edge_cleanup_applied", False
            ),
            "chen_fig7_short_edge_cleanup_scope": metrics.get(
                "chen_fig7_short_edge_cleanup_scope", "unknown"
            ),
            "chen_fig7_short_edge_cleanup_blocking_reason": metrics.get(
                "chen_fig7_short_edge_cleanup_blocking_reason", "unknown"
            ),
            "chen_fig7_short_edge_cleanup_has_labeled_approximations": metrics.get(
                "chen_fig7_short_edge_cleanup_has_labeled_approximations", False
            ),
            "chen_fig7_short_edge_cleanup_labeled_approximation_reasons": metrics.get(
                "chen_fig7_short_edge_cleanup_labeled_approximation_reasons", []
            ),
            "chen_fig7_short_edge_cleanup_applied_count": metrics.get(
                "chen_fig7_short_edge_cleanup_applied_count", 0
            ),
            "chen_fig7_short_edge_cleanup_midpoint_merge_count": metrics.get(
                "chen_fig7_short_edge_cleanup_midpoint_merge_count", 0
            ),
            "chen_fig7_short_edge_cleanup_boundary_projected_merge_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_boundary_projected_merge_count",
                    0,
                )
            ),
            "chen_fig7_short_edge_cleanup_merge_point_modes": metrics.get(
                "chen_fig7_short_edge_cleanup_merge_point_modes", []
            ),
            "chen_fig7_short_edge_cleanup_boundary_projection_distance_min": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_boundary_projection_distance_min",
                    0.0,
                )
            ),
            "chen_fig7_short_edge_cleanup_boundary_projection_distance_mean": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_boundary_projection_distance_mean",
                    0.0,
                )
            ),
            "chen_fig7_short_edge_cleanup_boundary_projection_distance_max": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_boundary_projection_distance_max",
                    0.0,
                )
            ),
            "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_stage": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_stage",
                    "unknown",
                )
            ),
            "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_applied_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_applied_count",
                    0,
                )
            ),
            "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_count",
                    0,
                )
            ),
            "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_ids_sample": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_full_mesh_ring_retention_parcel_ids_sample",
                    [],
                )
            ),
            "chen_fig7_short_edge_cleanup_failed_count": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_count", 0
            ),
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_stage": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_unique_candidate_stage", "unknown"
            ),
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_count": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_unique_candidate_count", 0
            ),
            "chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_duplicate_attempt_count", 0
            ),
            "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_failed_unique_candidate_counts_by_detail",
                    {},
                )
            ),
            "chen_fig7_short_edge_cleanup_failed_reasons": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_reasons", []
            ),
            "chen_fig7_short_edge_cleanup_failed_details": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_details", []
            ),
            "chen_fig7_short_edge_cleanup_failed_overlap_count": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_overlap_count", 0
            ),
            "chen_fig7_short_edge_cleanup_failed_invalid_polygon_count": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_invalid_polygon_count", 0
            ),
            "chen_fig7_short_edge_cleanup_failed_sliver_or_corner_loss_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_failed_sliver_or_corner_loss_count",
                    0,
                )
            ),
            "chen_fig7_short_edge_cleanup_failed_boundary_coverage_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_failed_boundary_coverage_count",
                    0,
                )
            ),
            "chen_fig7_short_edge_cleanup_failed_conforming_graph_count": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_conforming_graph_count", 0
            ),
            "chen_fig7_short_edge_cleanup_failed_degenerate_ring_after_merge_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_failed_degenerate_ring_after_merge_count",
                    0,
                )
            ),
            (
                "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
                "non_candidate_parcel_ring_after_merge_count"
            ): metrics.get(
                "chen_fig7_short_edge_cleanup_failed_fig7_motif_ineligible_"
                "non_candidate_parcel_ring_after_merge_count",
                0,
            ),
            "chen_fig7_short_edge_cleanup_failed_non_simple_ring_after_merge_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_failed_non_simple_ring_after_merge_count",
                    0,
                )
            ),
            "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_count",
                    0,
                )
            ),
            (
                "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_"
                "due_other_shared_edges_count"
            ): metrics.get(
                "chen_fig7_short_edge_cleanup_failed_candidate_pair_still_adjacent_"
                "due_other_shared_edges_count",
                0,
            ),
            "chen_fig7_short_edge_cleanup_failed_nonlocal_neighbor_delta_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_failed_nonlocal_neighbor_delta_count",
                    0,
                )
            ),
            "chen_fig7_short_edge_cleanup_skipped_boundary_count": metrics.get(
                "chen_fig7_short_edge_cleanup_skipped_boundary_count", 0
            ),
            "chen_fig7_short_edge_cleanup_pre_candidate_count": metrics.get(
                "chen_fig7_short_edge_cleanup_pre_candidate_count", 0
            ),
            "chen_fig7_short_edge_cleanup_post_candidate_count": metrics.get(
                "chen_fig7_short_edge_cleanup_post_candidate_count", 0
            ),
            "chen_fig7_short_edge_cleanup_pre_attached_t_junction_count": metrics.get(
                "chen_fig7_short_edge_cleanup_pre_attached_t_junction_count", 0
            ),
            "chen_fig7_short_edge_cleanup_post_attached_t_junction_count": metrics.get(
                "chen_fig7_short_edge_cleanup_post_attached_t_junction_count", 0
            ),
            "chen_fig7_short_edge_cleanup_pre_unexplained_t_junction_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_pre_unexplained_t_junction_count",
                    0,
                )
            ),
            "chen_fig7_short_edge_cleanup_post_unexplained_t_junction_count": (
                metrics.get(
                    "chen_fig7_short_edge_cleanup_post_unexplained_t_junction_count",
                    0,
                )
            ),
            "chen_fig7_short_edge_cleanup_failed_samples": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_samples", []
            ),
            "chen_fig7_short_edge_cleanup_failed_samples_by_detail": metrics.get(
                "chen_fig7_short_edge_cleanup_failed_samples_by_detail", {}
            ),
            "chen_fig7_short_shared_edge_candidate_count": metrics.get(
                "chen_fig7_short_shared_edge_candidate_count", 0
            ),
            "chen_fig7_short_shared_edge_length_min": metrics.get(
                "chen_fig7_short_shared_edge_length_min", 0.0
            ),
            "chen_fig7_short_shared_edge_length_mean": metrics.get(
                "chen_fig7_short_shared_edge_length_mean", 0.0
            ),
            "chen_fig7_short_shared_edge_length_max": metrics.get(
                "chen_fig7_short_shared_edge_length_max", 0.0
            ),
            "chen_fig7_short_shared_edge_threshold_max": metrics.get(
                "chen_fig7_short_shared_edge_threshold_max", 0.0
            ),
            "chen_fig7_short_shared_edge_length_threshold_ratio_max": metrics.get(
                "chen_fig7_short_shared_edge_length_threshold_ratio_max", 0.0
            ),
            "chen_fig7_raw_interior_t_junction_count": metrics.get(
                "chen_fig7_raw_interior_t_junction_count", 0
            ),
            "chen_fig7_short_edge_attached_interior_t_junction_count": metrics.get(
                "chen_fig7_short_edge_attached_interior_t_junction_count", 0
            ),
            "chen_fig7_unexplained_interior_t_junction_count": metrics.get(
                "chen_fig7_unexplained_interior_t_junction_count", 0
            ),
            "chen_fig7_unexplained_t_junction_classification_stage": metrics.get(
                "chen_fig7_unexplained_t_junction_classification_stage",
                "unknown",
            ),
            (
                "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count"
            ): (
                metrics.get(
                    "chen_fig7_unexplained_straight_through_side_insertion_t_junction_count",
                    0,
                )
            ),
            ("chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count"): (
                metrics.get(
                    "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_count",
                    0,
                )
            ),
            "chen_fig7_unexplained_t_junction_split_provenance_stage": metrics.get(
                "chen_fig7_unexplained_t_junction_split_provenance_stage",
                "unknown",
            ),
            "chen_fig7_unexplained_t_junction_split_provenance_scope": metrics.get(
                "chen_fig7_unexplained_t_junction_split_provenance_scope",
                "unknown",
            ),
            "chen_fig7_unexplained_t_junction_split_provenance_tolerance": metrics.get(
                "chen_fig7_unexplained_t_junction_split_provenance_tolerance",
                0.0,
            ),
            "chen_fig7_unexplained_t_junction_split_endpoint_count": metrics.get(
                "chen_fig7_unexplained_t_junction_split_endpoint_count"
            ),
            "chen_fig7_unexplained_t_junction_lies_on_split_line_count": metrics.get(
                "chen_fig7_unexplained_t_junction_lies_on_split_line_count"
            ),
            "chen_fig7_unexplained_t_junction_split_unknown_count": metrics.get(
                "chen_fig7_unexplained_t_junction_split_unknown_count"
            ),
            (
                "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_count"
            ): metrics.get(
                "chen_fig7_unexplained_t_junction_split_endpoint_straight_through_count"
            ),
            "chen_fig7_unexplained_t_junction_split_endpoint_kinked_count": (
                metrics.get(
                    "chen_fig7_unexplained_t_junction_split_endpoint_kinked_count"
                )
            ),
            (
                "chen_fig7_unexplained_t_junction_lies_on_split_line_"
                "straight_through_count"
            ): metrics.get(
                "chen_fig7_unexplained_t_junction_lies_on_split_line_straight_through_count"
            ),
            "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_count": (
                metrics.get(
                    "chen_fig7_unexplained_t_junction_lies_on_split_line_kinked_count"
                )
            ),
            (
                "chen_fig7_unexplained_t_junction_split_unknown_straight_through_count"
            ): metrics.get(
                "chen_fig7_unexplained_t_junction_split_unknown_straight_through_count"
            ),
            "chen_fig7_unexplained_t_junction_split_unknown_kinked_count": (
                metrics.get(
                    "chen_fig7_unexplained_t_junction_split_unknown_kinked_count"
                )
            ),
            "chen_fig7_unexplained_t_junction_split_endpoint_source_counts": (
                metrics.get(
                    "chen_fig7_unexplained_t_junction_split_endpoint_source_counts",
                    {},
                )
            ),
            "chen_fig7_unexplained_t_junction_lies_on_split_line_source_counts": (
                metrics.get(
                    "chen_fig7_unexplained_t_junction_lies_on_split_line_source_counts",
                    {},
                )
            ),
            (
                "chen_fig7_unexplained_t_junction_split_endpoint_"
                "straight_through_source_counts"
            ): metrics.get(
                "chen_fig7_unexplained_t_junction_split_endpoint_"
                "straight_through_source_counts",
                {},
            ),
            "chen_fig7_unexplained_t_junction_split_endpoint_kinked_source_counts": (
                metrics.get(
                    "chen_fig7_unexplained_t_junction_split_endpoint_kinked_source_counts",
                    {},
                )
            ),
            (
                "chen_fig7_unexplained_t_junction_lies_on_split_line_"
                "straight_through_source_counts"
            ): metrics.get(
                "chen_fig7_unexplained_t_junction_lies_on_split_line_"
                "straight_through_source_counts",
                {},
            ),
            (
                "chen_fig7_unexplained_t_junction_lies_on_split_line_"
                "kinked_source_counts"
            ): metrics.get(
                "chen_fig7_unexplained_t_junction_lies_on_split_line_"
                "kinked_source_counts",
                {},
            ),
            (
                "chen_fig7_unexplained_t_junction_split_unknown_"
                "straight_through_source_counts"
            ): metrics.get(
                "chen_fig7_unexplained_t_junction_split_unknown_"
                "straight_through_source_counts",
                {},
            ),
            "chen_fig7_unexplained_t_junction_split_unknown_kinked_source_counts": (
                metrics.get(
                    "chen_fig7_unexplained_t_junction_split_unknown_kinked_source_counts",
                    {},
                )
            ),
            "chen_fig7_short_shared_edge_samples": metrics.get(
                "chen_fig7_short_shared_edge_samples", []
            ),
            "chen_fig7_short_edge_attached_interior_t_junction_points_sample": (
                metrics.get(
                    "chen_fig7_short_edge_attached_interior_t_junction_points_sample",
                    [],
                )
            ),
            "chen_fig7_unexplained_interior_t_junction_points_sample": metrics.get(
                "chen_fig7_unexplained_interior_t_junction_points_sample", []
            ),
            (
                "chen_fig7_unexplained_straight_through_side_insertion_"
                "t_junction_points_sample"
            ): (
                metrics.get(
                    "chen_fig7_unexplained_straight_through_side_insertion_t_junction_points_sample",
                    [],
                )
            ),
            (
                "chen_fig7_unexplained_kinked_split_topology_debt_"
                "t_junction_points_sample"
            ): (
                metrics.get(
                    "chen_fig7_unexplained_kinked_split_topology_debt_t_junction_points_sample",
                    [],
                )
            ),
            "chen_fig7_unexplained_t_junction_split_endpoint_samples": metrics.get(
                "chen_fig7_unexplained_t_junction_split_endpoint_samples", []
            ),
            "chen_fig7_unexplained_t_junction_lies_on_split_line_samples": (
                metrics.get(
                    "chen_fig7_unexplained_t_junction_lies_on_split_line_samples", []
                )
            ),
            "chen_fig7_unexplained_t_junction_split_unknown_samples": metrics.get(
                "chen_fig7_unexplained_t_junction_split_unknown_samples", []
            ),
            "optimization_stage": metrics.get("optimization_stage", "unknown"),
            "optimization_applied": metrics.get("optimization_applied", False),
            "optimization_geometry_changed": metrics.get(
                "optimization_geometry_changed", False
            ),
            "optimization_accepted_iteration_count": metrics.get(
                "optimization_accepted_iteration_count", 0
            ),
            "optimization_energy_before": metrics.get("optimization_energy_before", 0),
            "optimization_energy_after": metrics.get("optimization_energy_after", 0),
            **_summary_metric_defaults(
                metrics, _OPTIMIZATION_REGULARITY_SUMMARY_DEFAULTS
            ),
            "street_generation_diagnostics": metrics.get(
                "street_generation_diagnostics", {}
            ),
            "paper_invariant_pass": metrics.get("strict_invariants", {}).get(
                "paper_invariant_pass", False
            ),
            "geometry_valid_pass": metrics.get("strict_invariants", {}).get(
                "geometry_valid_pass", False
            ),
        },
        "files": dict(ARTIFACT_FILES),
    }


def _summary_metric_defaults(
    metrics: dict[str, Any], defaults: dict[str, Any]
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for key, default in defaults.items():
        values[key] = _summary_metric_value(metrics.get(key, default), default)
    return values


def _summary_metric_value(value: Any, default: Any) -> Any:
    if isinstance(default, dict):
        if not isinstance(value, dict):
            return {}
        return {
            str(key): _summary_metric_count_value(count)
            for key, count in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(default, float):
        return float(value or 0.0)
    if isinstance(default, int):
        return int(value or 0)
    if isinstance(default, str):
        return str(default if value is None else value)
    return value


def _summary_metric_count_value(value: Any) -> int:
    return int(value or 0)


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(value), indent=2, sort_keys=True) + "\n")


def _json_ready(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(asdict(value))
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return [_json_ready(item) for item in value.tolist()]
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _render_svg(
    path: Path,
    name: str,
    boundary: Polygon,
    layout: ChenLayout,
    split_lines: list[LineString],
) -> None:
    transform = _Transform(
        _bounds(boundary, layout, split_lines), SVG_SIZE, SVG_SIZE, MARGIN
    )
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{SVG_SIZE}" '
        f'height="{SVG_SIZE}" viewBox="0 0 {SVG_SIZE} {SVG_SIZE}">',
        "<style>",
        "text{font-family:Inter,Arial,sans-serif;fill:#1f2933}",
        ".title{font-size:22px;font-weight:700}",
        ".subtitle{font-size:12px;fill:#52606d}",
        ".label{font-size:10px;fill:#3e4c59;text-anchor:middle}",
        "</style>",
        f'<rect width="{SVG_SIZE}" height="{SVG_SIZE}" fill="{_hex(BACKGROUND)}"/>',
        f'<path d="{_polygon_path(boundary, transform)}" '
        f'fill="{_hex(BOUNDARY_FILL)}" stroke="none" fill-rule="evenodd"/>',
    ]

    for index, parcel in enumerate(
        sorted(layout.mesh.parcels.values(), key=lambda p: p.parcel_id)
    ):
        fill = _hex(PARCEL_PALETTE[index % len(PARCEL_PALETTE)])
        parts.append(
            f'<path d="{_polygon_path(parcel.geom, transform)}" fill="{fill}" '
            f'stroke="#818a80" stroke-width="1.1" fill-rule="evenodd"/>'
        )

    for line in split_lines:
        parts.append(
            f'<path d="{_line_path(line, transform)}" fill="none" stroke="#cc7022" '
            'stroke-width="1.5" stroke-dasharray="5 5" stroke-linecap="round"/>'
        )

    for street in layout.street_graph.streets:
        line = _street_path_line(layout, street.nodes)
        if line is None:
            continue
        parts.append(
            f'<path d="{_line_path(line, transform)}" fill="none" '
            'stroke="#212d38" stroke-width="4.2" stroke-linecap="round" '
            'stroke-linejoin="round"/>'
        )

    parts.append(
        f'<path d="{_polygon_path(boundary, transform)}" fill="none" '
        'stroke="#181f24" stroke-width="3.2" fill-rule="evenodd"/>'
    )

    for parcel in sorted(layout.mesh.parcels.values(), key=lambda p: p.parcel_id):
        point = parcel.geom.representative_point()
        x, y = transform.point((float(point.x), float(point.y)))
        parts.append(
            f'<text class="label" x="{x:.1f}" y="{y:.1f}">{parcel.parcel_id}</text>'
        )

    title = html.escape(f"Chen Strict Layout: {name}")
    subtitle = html.escape(
        f"{len(layout.mesh.parcels)} parcels, "
        f"{len(layout.street_network.edges)} street-network edges"
    )
    parts.append(f'<text class="title" x="28" y="34">{title}</text>')
    parts.append(f'<text class="subtitle" x="28" y="54">{subtitle}</text>')
    parts.append("</svg>")
    path.write_text("\n".join(parts) + "\n")


def _render_png(
    path: Path,
    boundary: Polygon,
    layout: ChenLayout,
    split_lines: list[LineString],
) -> None:
    transform = _Transform(
        _bounds(boundary, layout, split_lines), PNG_SIZE, PNG_SIZE, MARGIN
    )
    img = np.zeros((PNG_SIZE, PNG_SIZE, 3), dtype=np.uint8)
    img[:, :] = BACKGROUND

    _fill_geom(img, boundary, transform, BOUNDARY_FILL)
    for index, parcel in enumerate(
        sorted(layout.mesh.parcels.values(), key=lambda p: p.parcel_id)
    ):
        _fill_geom(
            img, parcel.geom, transform, PARCEL_PALETTE[index % len(PARCEL_PALETTE)]
        )

    for parcel in sorted(layout.mesh.parcels.values(), key=lambda p: p.parcel_id):
        _stroke_geom(img, parcel.geom.boundary, transform, PARCEL_STROKE, width=2)

    for line in split_lines:
        _stroke_geom(img, line, transform, SPLIT_STROKE, width=2)

    for line, _properties in _street_lines(layout):
        _stroke_geom(img, line, transform, STREET_STROKE, width=5)

    _stroke_geom(img, boundary.boundary, transform, BOUNDARY_STROKE, width=4)
    _write_png(img, path)


def _bounds(
    boundary: Polygon, layout: ChenLayout, split_lines: list[LineString]
) -> tuple[float, float, float, float]:
    bounds = [boundary.bounds]
    bounds.extend(parcel.geom.bounds for parcel in layout.mesh.parcels.values())
    bounds.extend(line.bounds for line in split_lines)
    minx = min(item[0] for item in bounds)
    miny = min(item[1] for item in bounds)
    maxx = max(item[2] for item in bounds)
    maxy = max(item[3] for item in bounds)
    pad = max(maxx - minx, maxy - miny, 1.0) * 0.04
    return minx - pad, miny - pad, maxx + pad, maxy + pad


def _polygon_path(poly: Polygon | MultiPolygon, transform: _Transform) -> str:
    paths: list[str] = []
    for polygon in _polygons(poly):
        for ring in [polygon.exterior, *polygon.interiors]:
            coords = [transform.point((float(x), float(y))) for x, y in ring.coords]
            paths.append(
                "M " + " L ".join(f"{x:.2f},{y:.2f}" for x, y in coords) + " Z"
            )
    return " ".join(paths)


def _line_path(line: LineString | MultiLineString, transform: _Transform) -> str:
    parts: list[str] = []
    for item in _lines(line):
        coords = [transform.point((float(x), float(y))) for x, y in item.coords]
        if len(coords) >= 2:
            parts.append("M " + " L ".join(f"{x:.2f},{y:.2f}" for x, y in coords))
    return " ".join(parts)


def _polygons(geom: Polygon | MultiPolygon) -> list[Polygon]:
    if isinstance(geom, Polygon):
        return [geom]
    return [poly for poly in geom.geoms if isinstance(poly, Polygon)]


def _lines(geom: BaseGeometry) -> list[LineString]:
    if isinstance(geom, LineString):
        return [geom]
    if isinstance(geom, MultiLineString):
        return [line for line in geom.geoms if isinstance(line, LineString)]
    return []


def _hex(rgb: tuple[int, int, int]) -> str:
    return f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"


def _fill_geom(
    img: np.ndarray,
    geom: Polygon | MultiPolygon,
    transform: _Transform,
    color: tuple[int, int, int],
) -> None:
    for polygon in _polygons(geom):
        _fill_ring(img, _screen_coords(polygon.exterior.coords, transform), color)
        for interior in polygon.interiors:
            _fill_ring(img, _screen_coords(interior.coords, transform), BACKGROUND)


def _stroke_geom(
    img: np.ndarray,
    geom: BaseGeometry,
    transform: _Transform,
    color: tuple[int, int, int],
    *,
    width: int,
) -> None:
    if isinstance(geom, Polygon | MultiPolygon):
        for polygon in _polygons(geom):
            _draw_polyline(
                img, _screen_coords(polygon.exterior.coords, transform), color, width
            )
            for interior in polygon.interiors:
                _draw_polyline(
                    img, _screen_coords(interior.coords, transform), color, width
                )
        return
    for line in _lines(geom):
        _draw_polyline(img, _screen_coords(line.coords, transform), color, width)


def _screen_coords(coords: Any, transform: _Transform) -> list[tuple[float, float]]:
    return [transform.point((float(x), float(y))) for x, y in coords]


def _fill_ring(
    img: np.ndarray, coords: list[tuple[float, float]], color: tuple[int, int, int]
) -> None:
    if len(coords) < 3:
        return
    height, width = img.shape[:2]
    ys = [point[1] for point in coords]
    y0 = max(0, int(math.floor(min(ys))))
    y1 = min(height - 1, int(math.ceil(max(ys))))
    edges = list(zip(coords, coords[1:], strict=False))
    for y in range(y0, y1 + 1):
        scan_y = y + 0.5
        xs: list[float] = []
        for (x_a, y_a), (x_b, y_b) in edges:
            if y_a == y_b:
                continue
            low_y = min(y_a, y_b)
            high_y = max(y_a, y_b)
            if scan_y < low_y or scan_y >= high_y:
                continue
            t = (scan_y - y_a) / (y_b - y_a)
            xs.append(x_a + t * (x_b - x_a))
        xs.sort()
        for left, right in zip(xs[0::2], xs[1::2], strict=False):
            x0 = max(0, int(math.ceil(left)))
            x1 = min(width - 1, int(math.floor(right)))
            if x1 >= x0:
                img[y, x0 : x1 + 1] = color


def _draw_polyline(
    img: np.ndarray,
    coords: list[tuple[float, float]],
    color: tuple[int, int, int],
    width: int,
) -> None:
    for start, end in zip(coords, coords[1:], strict=False):
        _draw_line(img, start, end, color, width)


def _draw_line(
    img: np.ndarray,
    start: tuple[float, float],
    end: tuple[float, float],
    color: tuple[int, int, int],
    width: int,
) -> None:
    x0, y0 = start
    x1, y1 = end
    steps = max(int(max(abs(x1 - x0), abs(y1 - y0)) * 2.0), 1)
    radius = max(width // 2, 0)
    height, canvas_width = img.shape[:2]
    for t in np.linspace(0.0, 1.0, steps + 1):
        x = int(round(x0 + (x1 - x0) * float(t)))
        y = int(round(y0 + (y1 - y0) * float(t)))
        xa = max(0, x - radius)
        xb = min(canvas_width - 1, x + radius)
        ya = max(0, y - radius)
        yb = min(height - 1, y + radius)
        if xa <= xb and ya <= yb:
            img[ya : yb + 1, xa : xb + 1] = color


def _write_png(rgb: np.ndarray, path: Path) -> None:
    if rgb.dtype != np.uint8 or rgb.ndim != 3 or rgb.shape[2] != 3:
        raise ValueError("PNG input must be a uint8 RGB array")
    height, width = rgb.shape[:2]
    raw = b"".join(b"\x00" + bytes(row) for row in rgb)

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    png = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, 6))
        + chunk(b"IEND", b"")
    )
    path.write_bytes(png)
