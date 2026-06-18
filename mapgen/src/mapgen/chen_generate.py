"""Chen 2024 Section 4 level-synchronized hierarchical layout driver.

This module is a thin orchestration layer over the strict Chen pipeline:

- ``chen_mesh.ParcelMeshEditor`` owns the mutable per-level parcel mesh.
- ``chen_field.candidate_streamlines`` produces ~20 streamline partition-line
  candidates per parcel (Yang 2013 machinery; paper Section 4.1).
- Each candidate is scored geometry-only with the paper's Eq. 2 quality metric
  ``Q = 0.3*Q_size + 0.5*Q_regu + 0.2*Q_acce`` against the *real* street network.
- After every accepted split the new short edges are welded immediately
  (paper Fig. 7) via the editor.
- At the end of each level, unreachable parcels trigger a per-level street
  extension (``chen_streets.extend_street_network``; paper Section 4.2).
- The loop terminates when a level accepts zero splits; a parcel is splittable
  iff its area is at least twice the minimum parcel area.

Street edges are tracked as mesh edges (vertex-id pairs) so that they remain
valid across split/merge ``edge_replacements`` events. Reachability and street
extension operate on the corner graph, so mesh street edges are translated to
corner-graph edges per level (a corner edge is "street" iff every mesh sub-edge
along its path is a street edge) and added corner edges are expanded back to
mesh sub-edges.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, replace
from typing import Any

import shapely
from shapely import LineString, MultiLineString, Polygon
from shapely.ops import linemerge, split

from mapgen.chen_core import (
    DEFAULT_SPLIT_WEIGHTS,
    ChenLayout,
    ChenSplitWeights,
    EdgeKey,
    ParcelMesh,
    build_chen_layout,
    chen_access_tolerance,
    chen_irregularity,
    corner_edge_path,
    evaluate_layout_invariants,
    largest_polygon,
    normalized_edge,
    polygon_street_access_ratio,
)
from mapgen.chen_field import (
    RasterDensityField,
    RasterGuidanceField,
    StreamlineConfig,
    candidate_streamlines,
)
from mapgen.chen_mesh import ParcelMeshEditor, ShortEdgeMergeRejected
from mapgen.chen_optimize import (
    OptimizationConfig,
    OptimizationResult,
    optimize_layout,
)
from mapgen.chen_streets import StreetConfig, extend_street_network

_AREA_EPSILON = 1e-10
_GEOMETRY_TOLERANCE = 1e-7
_OPTIMIZATION_GEOMETRY_CHANGE_TOLERANCE = 1e-8

# The optimizer emits these regularity diagnostics (Chen Section 5 local
# regular-polygon projection); the generator promotes them to top-level metric
# keys so artifacts and the scale suite can aggregate them.
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

GENERATION_STAGE = "chen_section4_hierarchical_co_generation_v2"

#: Paper-faithful Eq. 2 access threshold. The size/regularity/access lambda
#: weights live in ``chen_core.ChenSplitWeights`` (default 0.3/0.5/0.2) and are
#: threaded through ``_score_candidate`` so callers can override them.
_ACCESS_TAU = 0.5

DEFAULT_GENERATION_OPTIMIZATION_CONFIG = OptimizationConfig()
DEFAULT_STREET_CONFIG = StreetConfig(avoid_cul_de_sacs=True)

STREAMLINE_MODE_BASELINE = "baseline"
STREAMLINE_MODE_YANG_D_FIELD = "yang_d_field_candidates"
STREAMLINE_MODE_YANG_B_FIELD = "yang_b_field_candidates"
STREAMLINE_MODES = (
    STREAMLINE_MODE_BASELINE,
    STREAMLINE_MODE_YANG_D_FIELD,
    STREAMLINE_MODE_YANG_B_FIELD,
)

#: Number of streamline candidates requested per parcel (paper: "around 20").
_CANDIDATE_TARGET_COUNT = 20


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
    partition_lines: tuple[tuple[int, LineString], ...]
    metrics: dict[str, Any]

    @property
    def split_lines(self) -> tuple[LineString, ...]:
        """Partition lines without the level tag (artifact compatibility)."""
        return tuple(line for _level, line in self.partition_lines)


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
    elif name == "island":
        points = []
        for index in range(48):
            theta = math.tau * index / 48
            wave = (
                1.0
                + 0.13 * math.sin(theta * 3.0 + 1.3)
                + 0.07 * math.cos(theta * 5.0 - 0.7)
            )
            points.append(
                (
                    width * (0.5 + 0.46 * wave * math.cos(theta)),
                    height * (0.5 + 0.42 * wave * math.sin(theta)),
                )
            )
        geom = Polygon(points)
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

    The strict core collapses smooth valence-2 parcel vertices. Eight anchored
    corners keep early oval parcels valid while the intermediate points give
    edge paths enough geometry to follow the contour instead of drawing chords.
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
    *,
    min_parcel_area: float | None = None,
    parcel_count: int | None = None,
    seed: int = 0,
    apply_optimization: bool = True,
    optimization_config: OptimizationConfig | None = None,
    streamline_mode: str = STREAMLINE_MODE_BASELINE,
    streamline_config: StreamlineConfig | None = None,
    street_config: StreetConfig | None = None,
    split_weights: ChenSplitWeights | None = None,
    guidance: RasterGuidanceField | None = None,
    density_field: RasterDensityField | None = None,
    max_parcel_mass: float | None = None,
) -> GeneratedChenLayout:
    """Generate a deterministic Chen layout for ``boundary``.

    Exactly one of ``min_parcel_area`` / ``parcel_count`` must be given. The
    ``parcel_count`` convenience maps to ``min_parcel_area = area /
    (1.5 * parcel_count)`` (calibrated so emergent counts land in the
    [0.6, 1.4]× band around the requested target for the standard shapes).

    ``split_weights`` overrides the paper Eq. 2 ``(size, regularity, access)``
    lambda weights; ``None`` uses the paper defaults. ``guidance`` (R1 regional
    extension, off by default, not Chen/Yang paper machinery) blends an external
    ``RasterGuidanceField`` into the default ``grid_smooth`` field.

    ``density_field`` + ``max_parcel_mass`` (R2 regional extension, off by
    default, not Chen/Yang paper machinery) switch the parcel-"size" measure from
    geometric area to integrated density mass: a parcel keeps splitting while its
    mass exceeds ``2 * max_parcel_mass`` and Eq. 2's size term scores the
    *mass* balance of the two children, so dense regions subdivide into smaller
    districts while sparse fringe terminates early as a few large ones. They must
    be supplied together. ``min_parcel_area`` / ``parcel_count`` are still
    required even in density mode: they set the streamline candidate spacing and
    a geometric robustness floor (keep the area target small enough that the mass
    gate, not the floor, governs termination). With ``density_field`` /
    ``guidance`` / ``split_weights`` all ``None`` the result is byte-identical to
    the pre-extension generator.
    """
    if (min_parcel_area is None) == (parcel_count is None):
        raise ValueError("provide exactly one of min_parcel_area or parcel_count")
    if (density_field is None) != (max_parcel_mass is None):
        raise ValueError("density_field and max_parcel_mass must be supplied together")
    if max_parcel_mass is not None and max_parcel_mass <= 0.0:
        raise ValueError("max_parcel_mass must be positive")

    boundary_geom = _clean_polygon(boundary.geom)
    boundary_area = float(boundary_geom.area)
    if parcel_count is not None:
        if parcel_count < 1:
            raise ValueError("parcel_count must be at least 1")
        resolved_min_area = boundary_area / (1.5 * float(parcel_count))
    else:
        if min_parcel_area is None or min_parcel_area <= 0.0:
            raise ValueError("min_parcel_area must be positive")
        resolved_min_area = float(min_parcel_area)

    street_cfg = street_config if street_config is not None else DEFAULT_STREET_CONFIG
    resolved_split_weights = (
        split_weights if split_weights is not None else DEFAULT_SPLIT_WEIGHTS
    )
    resolved_streamline_config, resolved_streamline_mode = _resolve_streamline_config(
        streamline_mode=streamline_mode,
        streamline_config=streamline_config,
        target_area=resolved_min_area,
    )
    streamline_config_for_candidates = (
        resolved_streamline_config
        if resolved_streamline_config is not None
        else StreamlineConfig()
    )

    generation_start = time.perf_counter()
    loop_result = _run_level_loop(
        boundary_geom,
        min_parcel_area=resolved_min_area,
        seed=seed,
        streamline_config=streamline_config_for_candidates,
        street_config=street_cfg,
        split_weights=resolved_split_weights,
        guidance=guidance,
        density_field=density_field,
        max_parcel_mass=max_parcel_mass,
    )
    generation_seconds = time.perf_counter() - generation_start

    snapshot = loop_result.editor.snapshot(boundary_geom)
    street_corner_edges = _street_corner_edges(snapshot, loop_result.street_mesh_edges)
    initial_layout = build_chen_layout(snapshot, street_corner_edges)

    layout = initial_layout
    optimization_seconds = 0.0
    optimization_metrics = _optimization_not_applied_metrics(
        "disabled", "apply_optimization_false"
    )
    if apply_optimization:
        config = optimization_config or _optimization_config_for_line_mode(
            "streamline_field"
        )
        optimization_start = time.perf_counter()
        optimization_result = optimize_layout(layout, boundary_geom, config)
        optimization_seconds = time.perf_counter() - optimization_start
        layout = optimization_result.layout
        optimization_metrics = _optimization_applied_metrics(optimization_result)

    report = evaluate_layout_invariants(layout, target_boundary=boundary_geom)

    # Table 1 metrics imported lazily to avoid an import cycle at module load.
    from mapgen.chen_metrics import paper_table1_metrics

    table1 = paper_table1_metrics(
        layout,
        boundary=boundary_geom,
        max_hierarchical_level=loop_result.max_level,
        generation_seconds=generation_seconds,
        optimization_seconds=optimization_seconds,
    )

    street_topology_reachability_pass = (
        bool(report.metrics.get("street_network_subset_of_corner_graph"))
        and int(report.metrics.get("street_graph_component_count", 0)) == 1
        and int(report.metrics.get("unreachable_parcel_count", 0)) == 0
    )

    metrics = dict(report.metrics)
    metrics.update(table1)
    metrics.update(_boundary_contour_metrics(boundary, boundary_geom))
    metrics.update(
        {
            "generation_stage": GENERATION_STAGE,
            "boundary": boundary.name,
            "boundary_name": boundary.name,
            "boundary_area": boundary_area,
            "boundary_perimeter_world": float(boundary_geom.length),
            "requested_parcel_count": (
                int(parcel_count) if parcel_count is not None else None
            ),
            "min_parcel_area": float(resolved_min_area),
            "seed": int(seed),
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
            "split_weight_size": float(resolved_split_weights.size),
            "split_weight_regularity": float(resolved_split_weights.regularity),
            "split_weight_access": float(resolved_split_weights.access),
            "guidance_field_applied": bool(guidance is not None),
            "density_mass_mode": bool(density_field is not None),
            "max_parcel_mass": (
                float(max_parcel_mass) if max_parcel_mass is not None else None
            ),
            "density_field_digest": (
                density_field.digest().hex() if density_field is not None else None
            ),
            "max_hierarchical_level": int(loop_result.max_level),
            "level_count": int(loop_result.max_level + 1),
            "accepted_split_count": int(loop_result.total_splits),
            "splits_per_level": tuple(loop_result.splits_per_level),
            "weld_applied_count": int(loop_result.weld_applied),
            "weld_rejected_count": int(loop_result.weld_rejected),
            "street_extension_diagnostics_per_level": tuple(
                loop_result.street_diagnostics_per_level
            ),
            "generation_seconds": float(generation_seconds),
            "optimization_seconds": float(optimization_seconds),
            "interpolation_edge_count": int(
                len(loop_result.editor.interpolation_edges)
            ),
            "generated_parcel_count": int(len(layout.mesh.parcels)),
            "split_line_count": int(loop_result.total_splits),
            "street_edge_count": int(len(layout.street_network.edges)),
            "corner_graph_edge_count": int(len(layout.corner_graph.edges)),
            "street_edge_density": (
                float(len(layout.street_network.edges))
                / float(len(layout.corner_graph.edges))
                if layout.corner_graph.edges
                else 0.0
            ),
            "street_selection_mode": ("chen_section_4_2_per_level_extension_v2"),
            "street_topology_reachability_pass": bool(
                street_topology_reachability_pass
            ),
            "paper_invariant_pass": bool(report.paper_invariant_pass),
            "geometry_valid_pass": bool(report.geometry_valid_pass),
            "chen_formula_pass": bool(report.chen_formula_pass),
            "diagnostic_metric_pass": bool(report.diagnostic_metric_pass),
        }
    )
    metrics.update(optimization_metrics)

    return GeneratedChenLayout(
        name=boundary.name,
        boundary=boundary_geom,
        layout=layout,
        partition_lines=tuple(loop_result.partition_lines),
        metrics=metrics,
    )


# ---------------------------------------------------------------------------
# Level loop
# ---------------------------------------------------------------------------


@dataclass
class _LevelLoopResult:
    editor: ParcelMeshEditor
    street_mesh_edges: set[EdgeKey]
    partition_lines: list[tuple[int, LineString]]
    splits_per_level: list[int]
    street_diagnostics_per_level: list[dict[str, Any]]
    max_level: int
    total_splits: int
    weld_applied: int
    weld_rejected: int


def _run_level_loop(
    boundary_geom: Polygon,
    *,
    min_parcel_area: float,
    seed: int,
    streamline_config: StreamlineConfig,
    street_config: StreetConfig,
    split_weights: ChenSplitWeights = DEFAULT_SPLIT_WEIGHTS,
    guidance: RasterGuidanceField | None = None,
    density_field: RasterDensityField | None = None,
    max_parcel_mass: float | None = None,
) -> _LevelLoopResult:
    editor = ParcelMeshEditor.from_boundary(boundary_geom)
    # Level-0 street = the whole input boundary ring (mesh edges).
    street_mesh_edges: set[EdgeKey] = _editor_boundary_ring_edges(editor)

    partition_lines: list[tuple[int, LineString]] = []
    splits_per_level: list[int] = []
    street_diagnostics_per_level: list[dict[str, Any]] = []
    weld_applied = 0
    weld_rejected = 0
    splittable_threshold = 2.0 * min_parcel_area
    # Density-mass mode (R2): on top of the geometric floor, a parcel is only
    # splittable while its integrated density mass exceeds 2x the per-district
    # mass target, so splits chase the population surface and sparse fringe
    # parcels terminate early. ``None`` => the gate is geometry-only (paper mode).
    splittable_mass_threshold = (
        2.0 * max_parcel_mass if max_parcel_mass is not None else None
    )

    level = 0
    # Hard ceiling on levels; the paper's max hierarchical level is single digits
    # and each level at least halves a parcel, so this is comfortably generous.
    max_levels = 64
    while level < max_levels:
        level += 1
        parcel_ids_this_level = sorted(editor.rings)
        level_splits = 0
        for parcel_id in parcel_ids_this_level:
            if parcel_id not in editor.rings:
                continue  # consumed by a split this level (shouldn't happen)
            parcel_poly = Polygon(
                [editor.positions[v] for v in editor.rings[parcel_id]]
            )
            if parcel_poly.area < splittable_threshold:
                continue
            # Mass gate is checked only after the geometric floor passes (the
            # mass integral is the expensive part, and a sub-floor parcel can
            # never split regardless of its mass).
            if (
                splittable_mass_threshold is not None
                and density_field is not None
                and density_field.mass(parcel_poly) < splittable_mass_threshold
            ):
                continue
            applied = _try_split_parcel(
                editor,
                parcel_id,
                parcel_poly,
                street_mesh_edges,
                min_parcel_area=min_parcel_area,
                seed=seed,
                level=level,
                streamline_config=streamline_config,
                split_weights=split_weights,
                guidance=guidance,
                density_field=density_field,
            )
            if applied is None:
                continue
            segment, new_ids, weld_a, weld_r = applied
            partition_lines.append((level, segment))
            weld_applied += weld_a
            weld_rejected += weld_r
            level_splits += 1

        splits_per_level.append(level_splits)

        # End-of-level street generation if any parcel is unreachable.
        diagnostics = _extend_streets_for_level(
            editor, boundary_geom, street_mesh_edges, street_config
        )
        street_diagnostics_per_level.append(diagnostics)

        if level_splits == 0:
            break

    return _LevelLoopResult(
        editor=editor,
        street_mesh_edges=street_mesh_edges,
        partition_lines=partition_lines,
        splits_per_level=splits_per_level,
        street_diagnostics_per_level=street_diagnostics_per_level,
        max_level=level,
        total_splits=len(partition_lines),
        weld_applied=weld_applied,
        weld_rejected=weld_rejected,
    )


def _try_split_parcel(
    editor: ParcelMeshEditor,
    parcel_id: int,
    parcel_poly: Polygon,
    street_mesh_edges: set[EdgeKey],
    *,
    min_parcel_area: float,
    seed: int,
    level: int,
    streamline_config: StreamlineConfig,
    split_weights: ChenSplitWeights = DEFAULT_SPLIT_WEIGHTS,
    guidance: RasterGuidanceField | None = None,
    density_field: RasterDensityField | None = None,
) -> tuple[LineString, tuple[int, int], int, int] | None:
    """Score candidates, apply the best split, and weld new short edges.

    Returns ``(segment, new_parcel_ids, weld_applied, weld_rejected)`` or
    ``None`` when no viable candidate exists for this parcel at this level.
    """
    candidate_seed = _stable_mix(seed, level, parcel_id)
    try:
        streamlines = candidate_streamlines(
            parcel_poly,
            target_count=_CANDIDATE_TARGET_COUNT,
            seed=candidate_seed,
            existing_lines=(),
            config=streamline_config,
            guidance=guidance,
        )
    except ValueError:
        return None
    if not streamlines:
        return None

    street_segments = _street_world_segments(editor, street_mesh_edges)

    best: tuple[tuple[float, ...], LineString] | None = None
    for index, candidate in enumerate(streamlines):
        scored = _score_candidate(
            parcel_poly,
            candidate.line,
            street_segments,
            min_parcel_area=min_parcel_area,
            split_weights=split_weights,
            density_field=density_field,
        )
        if scored is None:
            continue
        score, segment = scored
        key = (
            score,
            float(candidate.quality),
            float(segment.length),
            -index,
        )
        if best is None or key > best[0]:
            best = (key, segment)

    if best is None:
        return None
    _key, segment = best

    try:
        split_result = editor.split_parcel(parcel_id, segment)
    except ValueError:
        return None

    _remap_street_edges_for_split(street_mesh_edges, split_result.edge_replacements)

    new_vertex_ids = {node for edge in split_result.split_edges for node in edge}
    weld_applied, weld_rejected = _weld_new_short_edges(
        editor, street_mesh_edges, new_vertex_ids
    )
    return segment, split_result.new_parcel_ids, weld_applied, weld_rejected


def _score_candidate(
    parcel_poly: Polygon,
    line: LineString,
    street_segments: list[tuple[tuple[float, float], tuple[float, float]]],
    *,
    min_parcel_area: float,
    split_weights: ChenSplitWeights = DEFAULT_SPLIT_WEIGHTS,
    density_field: RasterDensityField | None = None,
) -> tuple[float, LineString] | None:
    """Eq. 2 scoring of a candidate partition line.

    Returns ``(Q, clipped_segment)`` or ``None`` when the split is invalid (a
    child below the geometric minimum parcel area, or a degenerate cut).
    ``split_weights`` defaults to the paper Eq. 2 ``(0.3, 0.5, 0.2)`` lambdas.

    With a ``density_field`` (R2 density-mass mode) the size term Q_size scores
    the *mass* balance of the two children rather than their area balance; child
    validity still uses only the geometric floor, so a large-area/low-mass fringe
    child is allowed (that is what lets sparse regions become a few big
    districts).
    """
    split_result = _split_face(parcel_poly, line)
    if split_result is None:
        return None
    (part_a, part_b), segment = split_result
    area_a = float(part_a.area)
    area_b = float(part_b.area)
    if area_a < min_parcel_area or area_b < min_parcel_area:
        return None

    if density_field is not None:
        size_a = density_field.mass(part_a)
        size_b = density_field.mass(part_b)
    else:
        size_a = area_a
        size_b = area_b
    s_small = min(size_a, size_b)
    s_large = max(size_a, size_b)
    q_size = s_small / s_large if s_large > 0.0 else 0.0

    irr_a = chen_irregularity(part_a)
    irr_b = chen_irregularity(part_b)
    q_regu = 1.0 / (1.0 + irr_a + irr_b)

    q_acce = _candidate_access_score(
        part_a, part_b, street_segments, min_parcel_area=min_parcel_area
    )

    q_total = (
        split_weights.size * q_size
        + split_weights.regularity * q_regu
        + split_weights.access * q_acce
    )
    return q_total, segment


def _candidate_access_score(
    part_a: Polygon,
    part_b: Polygon,
    street_segments: list[tuple[tuple[float, float], tuple[float, float]]],
    *,
    min_parcel_area: float,
) -> float:
    """Paper Q_acce: 2 if both children have s >= tau, else 1.

    ``s`` is the length of the child boundary overlapping the current street
    network divided by the average side length of the child's approximate
    polygon (chen_core's ``polygon_street_access_ratio``).
    """
    if not street_segments:
        return 1.0
    access_lines = [LineString([a, b]) for a, b in street_segments]
    tolerance = chen_access_tolerance(min_parcel_area)
    s_a = polygon_street_access_ratio(part_a, access_lines, tolerance)
    s_b = polygon_street_access_ratio(part_b, access_lines, tolerance)
    return 2.0 if (s_a >= _ACCESS_TAU and s_b >= _ACCESS_TAU) else 1.0


def _weld_new_short_edges(
    editor: ParcelMeshEditor,
    street_mesh_edges: set[EdgeKey],
    new_vertex_ids: set[int],
) -> tuple[int, int]:
    """Immediately weld short edges introduced by the split (paper Fig. 7)."""
    applied = 0
    rejected = 0
    # Re-query candidates after each successful weld since the merge mutates
    # rings and vertex ids; bound the loop to avoid pathological cycles.
    safety = max(len(new_vertex_ids) * 4, 4)
    scope = set(new_vertex_ids)
    for _ in range(safety):
        candidates = editor.short_edge_candidates(scope_vertex_ids=scope)
        # A Fig. 7 short edge is THE single shared edge between two parcels;
        # collapsing it makes the pair non-neighbours (paper Section 4.1). Skip
        # short sub-segments of a longer shared boundary chain (e.g. collinear
        # mesh discretisation along a straight cut), which are not Fig. 7 edges
        # and whose collapse would leave the parcels still sharing an edge.
        candidates = [edge for edge in candidates if _is_fig7_short_edge(editor, edge)]
        if not candidates:
            break
        edge = candidates[0]
        try:
            merge_result = editor.merge_short_edge(edge)
        except ShortEdgeMergeRejected:
            rejected += 1
            # Drop this edge's vertices from scope so we do not retry it forever.
            scope.discard(edge[0])
            scope.discard(edge[1])
            if not scope:
                break
            continue
        applied += 1
        _remap_street_edges_for_merge(street_mesh_edges, merge_result.edge_replacements)
        # The merged vertex survives; widen scope to it.
        scope.discard(merge_result.merged_vertex)
        scope.add(merge_result.merged_vertex)
    return applied, rejected


def _is_fig7_short_edge(editor: ParcelMeshEditor, edge: EdgeKey) -> bool:
    """True when ``edge`` is the single shared edge between exactly two parcels.

    Collapsing such an edge makes the two parcels non-neighbours, matching the
    paper's Fig. 7 semantics. Short sub-segments of a longer shared chain share
    more than one edge between the same pair and are not Fig. 7 short edges.
    """
    pair = editor.edge_parcels.get(edge)
    if pair is None or len(pair) != 2:
        return False
    shared = sum(1 for parcels in editor.edge_parcels.values() if parcels == pair)
    return shared == 1


def _extend_streets_for_level(
    editor: ParcelMeshEditor,
    boundary_geom: Polygon,
    street_mesh_edges: set[EdgeKey],
    street_config: StreetConfig,
) -> dict[str, Any]:
    snapshot = editor.snapshot(boundary_geom)
    street_corner_edges = _street_corner_edges(snapshot, street_mesh_edges)
    layout = build_chen_layout(snapshot, street_corner_edges)

    unreachable = int(
        evaluate_layout_invariants(layout).metrics.get("unreachable_parcel_count", 0)
    )
    if unreachable == 0:
        return {"unreachable_before": 0, "added_edge_count": 0}

    result = extend_street_network(layout, set(street_corner_edges), street_config)
    # Expand each added corner edge into its mesh sub-edges so the persistent
    # street-edge set stays expressed in mesh-vertex terms.
    for corner_edge in result.added_edges:
        path = corner_edge_path(layout.corner_graph, corner_edge)
        for a, b in zip(path, path[1:], strict=False):
            street_mesh_edges.add(normalized_edge(a, b))

    diagnostics = dict(result.diagnostics)
    diagnostics["unreachable_before"] = unreachable
    return diagnostics


# ---------------------------------------------------------------------------
# Street-edge bookkeeping
# ---------------------------------------------------------------------------


def _editor_boundary_ring_edges(editor: ParcelMeshEditor) -> set[EdgeKey]:
    edges: set[EdgeKey] = set()
    for ring in editor.rings.values():
        for a, b in zip(ring, ring[1:] + ring[:1], strict=True):
            if a in editor.boundary_vertex_ids and b in editor.boundary_vertex_ids:
                edges.add(normalized_edge(a, b))
    return edges


def _remap_street_edges_for_split(
    street_mesh_edges: set[EdgeKey],
    edge_replacements: list[tuple[EdgeKey, tuple[EdgeKey, EdgeKey]]],
) -> None:
    for old_edge, (sub_a, sub_b) in edge_replacements:
        if old_edge in street_mesh_edges:
            street_mesh_edges.discard(old_edge)
            street_mesh_edges.add(sub_a)
            street_mesh_edges.add(sub_b)


def _remap_street_edges_for_merge(
    street_mesh_edges: set[EdgeKey],
    edge_replacements: list[tuple[EdgeKey, EdgeKey]],
) -> None:
    for old_edge, new_edge in edge_replacements:
        if old_edge in street_mesh_edges:
            street_mesh_edges.discard(old_edge)
            street_mesh_edges.add(new_edge)


def _street_world_segments(
    editor: ParcelMeshEditor, street_mesh_edges: set[EdgeKey]
) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    segments: list[tuple[tuple[float, float], tuple[float, float]]] = []
    for a, b in street_mesh_edges:
        if a in editor.positions and b in editor.positions:
            segments.append((editor.positions[a], editor.positions[b]))
    return segments


def _street_corner_edges(
    mesh: ParcelMesh, street_mesh_edges: set[EdgeKey]
) -> set[EdgeKey]:
    """Translate mesh street edges into corner-graph street edges.

    A corner-graph edge is part of the street network iff every mesh sub-edge
    along its corner path is in ``street_mesh_edges``.
    """
    from mapgen.chen_core import parcel_corner_graph

    corner_graph = parcel_corner_graph(mesh)
    corner_edges: set[EdgeKey] = set()
    for corner_edge in corner_graph.edges:
        path = corner_graph.edge_paths.get(corner_edge)
        if path is None or len(path) < 2:
            continue
        if all(
            normalized_edge(a, b) in street_mesh_edges
            for a, b in zip(path, path[1:], strict=False)
        ):
            corner_edges.add(normalized_edge(*corner_edge))
    return corner_edges


def _stable_mix(seed: int, level: int, parcel_id: int) -> int:
    """Deterministic integer mix (no Python ``hash()``)."""
    value = (int(seed) & 0xFFFFFFFFFFFFFFFF) * 0x9E3779B97F4A7C15
    value ^= (int(level) + 0x165667B19E3779F9) * 0xC2B2AE3D27D4EB4F
    value &= 0xFFFFFFFFFFFFFFFF
    value ^= int(parcel_id) * 0xD6E8FEB86659FD93
    value &= 0xFFFFFFFFFFFFFFFF
    value ^= value >> 29
    value = (value * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 32
    return value & 0x7FFFFFFF


# ---------------------------------------------------------------------------
# Optimization metric shaping (kept for artifact/test compatibility)
# ---------------------------------------------------------------------------


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
            diagnostics.get(key, default), default
        )
    return metrics


def _optimizer_diagnostic_value(value: Any, default: Any) -> Any:
    if isinstance(default, dict):
        if not isinstance(value, dict):
            return {}
        items: list[tuple[Any, Any]] = list(value.items())
        result: dict[str, int] = {}
        for key, count in sorted(items, key=lambda item: str(item[0])):
            try:
                result[str(key)] = int(count)
            except (TypeError, ValueError):
                result[str(key)] = 0
        return result
    if isinstance(default, float):
        return float(value or 0.0)
    if isinstance(default, int):
        return int(value or 0)
    if isinstance(default, str):
        return str(default if value is None else value)
    return value


# ---------------------------------------------------------------------------
# Geometry helpers (retained from the previous geometry-first generator)
# ---------------------------------------------------------------------------


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


__all__ = [
    "BoundarySpec",
    "ChenSplitWeights",
    "GeneratedChenLayout",
    "RasterDensityField",
    "RasterGuidanceField",
    "STREAMLINE_MODE_BASELINE",
    "STREAMLINE_MODE_YANG_B_FIELD",
    "STREAMLINE_MODE_YANG_D_FIELD",
    "STREAMLINE_MODES",
    "boundary_preset",
    "generate_layout_for_boundary",
    "generate_named_layout",
]
