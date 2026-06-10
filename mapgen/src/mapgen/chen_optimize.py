"""Practical Chen 2024 Section 5 geometric optimization.

This module keeps the strict Chen topology fixed and only moves mesh vertex
positions.  The default pass is a bounded, deterministic ShapeOp-like
projection/global least-squares solver.  It is still not the paper's exact
ShapeOp implementation, but the exposed energy and projection diagnostics track
the paper's Section 5 objective terms: parcel regularity, parcel side
smoothness, street smoothness, street junction orthogonality, and layout
closeness.  The previous local bounded relaxation remains available as
``projection_mode="v0_relaxation"``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import shapely
from scipy import sparse
from scipy.sparse import linalg as sparse_linalg
from shapely import LineString, Polygon
from shapely import Point as ShapelyPoint

from mapgen.chen_core import (
    CHEN_COLLINEAR_THRESHOLD_RAD,
    ChenLayout,
    EdgeKey,
    MeshVertex,
    ParcelFace,
    ParcelMesh,
    StreetPath,
    build_chen_layout,
    chen_irregularity,
    evaluate_layout_invariants,
    included_angle_unsigned,
    signed_area,
)

_EPSILON = 1e-12
_VALID_PROJECTION_MODES = frozenset({"v0_relaxation", "shapeop_like_projection"})
_VALID_BOUNDARY_CONSTRAINT_MODES = frozenset(
    {"fixed_all_boundary", "chen_collinear_slide"}
)
_REGULARITY_PROJECTION_KIND = "regular_polygon_similarity_transform_v0"


@dataclass(frozen=True)
class OptimizationWeights:
    """Weights for Chen Section 5 terms plus optional near-axis cleanup."""

    regularity: float = 0.25
    side_smoothness: float = 4.0
    street_smoothness: float = 4.0
    junction_orthogonality: float = 1.0
    closeness: float = 0.02
    axis_alignment: float = 2.0
    area_regularization: float = 1.0
    rectangularity: float = 2.0
    edge_length_smoothness: float = 1.0


@dataclass(frozen=True)
class OptimizationConfig:
    """Configuration for deterministic bounded geometric optimization."""

    weights: OptimizationWeights = field(default_factory=OptimizationWeights)
    projection_mode: str = "shapeop_like_projection"
    iterations: int = 32
    step_size: float = 0.45
    backtracking_steps: int = 6
    convergence_tolerance: float = 1e-9
    boundary_tolerance: float = 1e-7
    boundary_constraint_mode: str = "fixed_all_boundary"
    boundary_shape_constraint_weight: float = 1e6
    project_boundary_vertices: bool = True
    max_vertex_displacement: float | None = None
    max_vertex_displacement_ratio: float = 0.03
    max_iteration_displacement: float | None = None
    min_coverage: float = 0.995
    max_spillover: float = 0.005
    enable_axis_alignment: bool = True
    axis_angle_tolerance_rad: float = math.radians(8.0)
    min_axis_edge_fraction: float = 0.75
    solver_anchor_weight: float = 1e-9
    solver_tolerance: float = 1e-12
    enable_area_regularization: bool = True
    parcel_area_target: float | None = None
    area_projection_step: float = 0.35
    enable_rectangularity: bool = True
    rectangularity_projection_step: float = 0.45
    enable_edge_length_smoothness: bool = True
    edge_length_projection_step: float = 0.65
    preserve_topology: bool = True
    reject_inversions: bool = True
    min_parcel_area_ratio: float = 1e-6


@dataclass(frozen=True)
class OptimizationResult:
    """Optimized layout and diagnostics."""

    layout: ChenLayout
    metrics: dict[str, Any]


@dataclass(frozen=True)
class _AxisGroup:
    coordinate_index: int
    target: float
    vertices: frozenset[int]


@dataclass(frozen=True)
class _AxisPlan:
    groups: tuple[_AxisGroup, ...]


@dataclass(frozen=True)
class _BoundarySlideSegment:
    start: np.ndarray
    end: np.ndarray


@dataclass(frozen=True)
class _BoundaryConstraints:
    fixed_positions: dict[int, np.ndarray]
    slide_segments: dict[int, _BoundarySlideSegment]


@dataclass(frozen=True)
class _ScalarProjectionEquation:
    term: str
    coeffs: tuple[tuple[int, int, float], ...]
    target: float
    weight: float


@dataclass(frozen=True)
class _RegularityProjectionTarget:
    parcel_id: int
    vertex_id: int
    target: np.ndarray
    equation_weight: float


@dataclass(frozen=True)
class _RegularityProjectionSummary:
    kind: str
    projected_parcel_count: int
    skipped_parcel_count: int
    skipped_by_reason: dict[str, int]
    equation_count: int
    residual: float
    target_displacement_rms: float
    target_displacement_max: float
    targets: tuple[_RegularityProjectionTarget, ...]


@dataclass
class _ProposalStats:
    attempts: int = 0
    rejections: dict[str, int] = field(default_factory=dict)
    last_max_raw_step: float = 0.0
    last_accepted_step: float = 0.0


DEFAULT_OPTIMIZATION_CONFIG = OptimizationConfig()
CHEN_SECTION5_STRICT_OPTIMIZATION_CONFIG = OptimizationConfig(
    weights=OptimizationWeights(
        regularity=0.25,
        side_smoothness=4.0,
        street_smoothness=4.0,
        junction_orthogonality=1.0,
        closeness=0.02,
        axis_alignment=0.0,
        area_regularization=0.0,
        rectangularity=0.0,
        edge_length_smoothness=0.0,
    ),
    boundary_constraint_mode="chen_collinear_slide",
    enable_axis_alignment=False,
    enable_area_regularization=False,
    enable_rectangularity=False,
    enable_edge_length_smoothness=False,
)


def optimize_layout(
    layout: ChenLayout,
    boundary: Polygon,
    config: OptimizationConfig = DEFAULT_OPTIMIZATION_CONFIG,
) -> OptimizationResult:
    """Return a new optimized layout without changing graph topology.

    The optimizer only changes vertex coordinates.  Each accepted candidate is
    rebuilt through :func:`build_chen_layout` and must preserve parcel ids,
    parcel rings, corner graph edges/paths, parcel graph topology, and street
    network edges exactly.
    """

    if config.iterations < 0:
        raise ValueError("iterations must be non-negative")
    if config.projection_mode not in _VALID_PROJECTION_MODES:
        modes = ", ".join(sorted(_VALID_PROJECTION_MODES))
        raise ValueError(f"projection_mode must be one of: {modes}")
    if config.step_size < 0.0:
        raise ValueError("step_size must be non-negative")
    if config.backtracking_steps < 0:
        raise ValueError("backtracking_steps must be non-negative")
    if config.solver_anchor_weight < 0.0:
        raise ValueError("solver_anchor_weight must be non-negative")
    if config.solver_tolerance <= 0.0:
        raise ValueError("solver_tolerance must be positive")
    if config.boundary_constraint_mode not in _VALID_BOUNDARY_CONSTRAINT_MODES:
        modes = ", ".join(sorted(_VALID_BOUNDARY_CONSTRAINT_MODES))
        raise ValueError(f"boundary_constraint_mode must be one of: {modes}")
    if config.boundary_shape_constraint_weight < 0.0:
        raise ValueError("boundary_shape_constraint_weight must be non-negative")
    if config.parcel_area_target is not None and config.parcel_area_target <= 0.0:
        raise ValueError("parcel_area_target must be positive when provided")
    if config.area_projection_step < 0.0:
        raise ValueError("area_projection_step must be non-negative")
    if config.rectangularity_projection_step < 0.0:
        raise ValueError("rectangularity_projection_step must be non-negative")
    if config.edge_length_projection_step < 0.0:
        raise ValueError("edge_length_projection_step must be non-negative")
    if config.min_parcel_area_ratio < 0.0:
        raise ValueError("min_parcel_area_ratio must be non-negative")

    original_positions = _layout_positions(layout)
    positions = _copy_positions(original_positions)
    boundary_constraints = _boundary_constraints(layout, boundary, config)
    _apply_boundary_constraints(positions, boundary_constraints)
    area_targets = _parcel_area_targets(layout, original_positions, config)
    area_target_mode = (
        "uniform_configured"
        if config.parcel_area_target is not None
        else "initial_parcel_area"
    )
    original_signed_areas = _parcel_signed_areas(layout, original_positions)

    topology = _topology_signature(layout)
    axis_plan = (
        _axis_alignment_plan(layout, config)
        if config.enable_axis_alignment
        and is_near_axis_rectangular_layout(
            layout,
            angle_tolerance_rad=config.axis_angle_tolerance_rad,
            min_axis_edge_fraction=config.min_axis_edge_fraction,
        )
        else _AxisPlan(())
    )
    axis_active = bool(axis_plan.groups)
    displacement_limit = _max_vertex_displacement(layout, boundary, config)
    iteration_limit = config.max_iteration_displacement
    if iteration_limit is None:
        iteration_limit = max(displacement_limit * 0.35, config.convergence_tolerance)

    current_layout = _layout_from_positions(layout, positions)
    initial_layout = current_layout
    initial_positions = _copy_positions(positions)
    current_components = _energy_components(
        current_layout,
        original_positions,
        area_targets,
        config,
        axis_active=axis_active,
    )
    current_energy = _weighted_energy(current_components, config.weights)
    before_components = dict(current_components)
    before_energy = current_energy
    before_report = evaluate_layout_invariants(layout, target_boundary=boundary)
    accepted_iterations = 0
    converged = False
    convergence_reason = "iteration_limit"
    proposal_stats = _ProposalStats()
    active_constraints = _active_constraints(
        config,
        boundary_constraints=boundary_constraints,
        axis_active=axis_active,
    )

    for _iteration in range(config.iterations):
        if config.projection_mode == "shapeop_like_projection":
            raw_step = _shapeop_like_projection_step(
                current_layout,
                positions,
                original_positions,
                area_targets,
                boundary_constraints,
                axis_plan,
                config,
            )
        else:
            raw_step = _relaxation_step(
                current_layout,
                positions,
                original_positions,
                area_targets,
                boundary_constraints,
                axis_plan,
                config,
            )
        if not raw_step:
            converged = True
            convergence_reason = "no_projection_step"
            break

        max_raw_norm = max(float(np.linalg.norm(step)) for step in raw_step.values())
        proposal_stats.last_max_raw_step = max_raw_norm
        if max_raw_norm <= config.convergence_tolerance:
            converged = True
            convergence_reason = "step_tolerance"
            break

        accepted = False
        for backtrack in range(config.backtracking_steps + 1):
            factor = 0.5**backtrack
            proposal_stats.attempts += 1
            candidate_positions = _candidate_positions(
                positions,
                original_positions,
                boundary_constraints,
                axis_plan,
                raw_step,
                factor=factor,
                displacement_limit=displacement_limit,
                iteration_limit=iteration_limit,
            )
            max_step = _max_position_delta(positions, candidate_positions)
            if max_step <= config.convergence_tolerance:
                converged = True
                convergence_reason = "candidate_step_tolerance"
                accepted = True
                break

            if config.reject_inversions and not _non_inversion_guard_pass(
                layout,
                candidate_positions,
                original_signed_areas,
                config,
            ):
                _record_rejection(proposal_stats, "non_inversion")
                continue

            candidate_layout = _try_layout_from_positions(layout, candidate_positions)
            if candidate_layout is None:
                _record_rejection(proposal_stats, "invalid_geometry")
                continue
            if (
                config.preserve_topology
                and _topology_signature(candidate_layout) != topology
            ):
                _record_rejection(proposal_stats, "topology_changed")
                continue
            if not _geometry_constraints_pass(candidate_layout, boundary, config):
                _record_rejection(proposal_stats, "geometry_constraints")
                continue

            candidate_components = _energy_components(
                candidate_layout,
                original_positions,
                area_targets,
                config,
                axis_active=axis_active,
            )
            candidate_energy = _weighted_energy(candidate_components, config.weights)
            if candidate_energy > current_energy + 1e-10:
                _record_rejection(proposal_stats, "energy_increase")
                continue

            positions = candidate_positions
            current_layout = candidate_layout
            current_components = candidate_components
            current_energy = candidate_energy
            accepted_iterations += 1
            proposal_stats.last_accepted_step = max_step
            accepted = True
            break

        if converged:
            break
        if not accepted:
            convergence_reason = "line_search_exhausted"
            break

    after_report = evaluate_layout_invariants(current_layout, target_boundary=boundary)
    after_components = _energy_components(
        current_layout,
        original_positions,
        area_targets,
        config,
        axis_active=axis_active,
    )
    after_energy = _weighted_energy(after_components, config.weights)
    projection_before = _projection_diagnostics(
        initial_layout,
        initial_positions,
        original_positions,
        area_targets,
        boundary_constraints,
        axis_plan,
        config,
    )
    projection_after = _projection_diagnostics(
        current_layout,
        positions,
        original_positions,
        area_targets,
        boundary_constraints,
        axis_plan,
        config,
    )
    metrics = _result_metrics(
        layout,
        current_layout,
        boundary,
        original_positions,
        before_components,
        after_components,
        before_energy,
        after_energy,
        before_report.metrics,
        after_report.metrics,
        accepted_iterations=accepted_iterations,
        converged=converged,
        convergence_reason=convergence_reason,
        axis_active=axis_active,
        optimizer_kind=config.projection_mode,
        active_constraints=active_constraints,
        proposal_stats=proposal_stats,
        area_target_mode=area_target_mode,
        projection_before=projection_before,
        projection_after=projection_after,
    )
    return OptimizationResult(layout=current_layout, metrics=metrics)


def is_near_axis_rectangular_layout(
    layout: ChenLayout,
    *,
    angle_tolerance_rad: float = math.radians(8.0),
    min_axis_edge_fraction: float = 0.75,
) -> bool:
    """Return whether most mesh edges are close to horizontal or vertical."""

    lengths_and_deviations = _edge_axis_deviations(layout)
    if not lengths_and_deviations:
        return False
    axis_like = [
        length
        for length, deviation in lengths_and_deviations
        if deviation <= angle_tolerance_rad
    ]
    total_length = sum(length for length, _deviation in lengths_and_deviations)
    if total_length <= _EPSILON:
        return False
    return sum(axis_like) / total_length >= min_axis_edge_fraction


def _layout_positions(layout: ChenLayout) -> dict[int, np.ndarray]:
    return {
        vertex_id: np.array(vertex.point, dtype=float)
        for vertex_id, vertex in layout.mesh.vertices.items()
    }


def _copy_positions(positions: dict[int, np.ndarray]) -> dict[int, np.ndarray]:
    return {vertex_id: point.copy() for vertex_id, point in positions.items()}


def _boundary_constraints(
    layout: ChenLayout, boundary: Polygon, config: OptimizationConfig
) -> _BoundaryConstraints:
    boundary_line = LineString(boundary.exterior.coords)
    fixed: dict[int, np.ndarray] = {}
    slide_segments: dict[int, _BoundarySlideSegment] = {}
    for vertex_id, vertex in layout.mesh.vertices.items():
        point = ShapelyPoint(vertex.point)
        distance = float(boundary_line.distance(point))
        if not vertex.on_boundary and distance > config.boundary_tolerance:
            continue
        slide_segment = (
            _boundary_slide_segment(vertex.point, boundary, config.boundary_tolerance)
            if config.boundary_constraint_mode == "chen_collinear_slide"
            and distance <= config.boundary_tolerance
            else None
        )
        if slide_segment is not None:
            slide_segments[vertex_id] = slide_segment
            continue
        if config.project_boundary_vertices and distance <= config.boundary_tolerance:
            projected = boundary_line.interpolate(boundary_line.project(point))
            fixed[vertex_id] = np.array((float(projected.x), float(projected.y)))
        else:
            fixed[vertex_id] = np.array(vertex.point, dtype=float)
    return _BoundaryConstraints(fixed, slide_segments)


def _boundary_slide_segment(
    point: tuple[float, float], boundary: Polygon, tolerance: float
) -> _BoundarySlideSegment | None:
    point_array = np.array(point, dtype=float)
    coords = [np.array(coord, dtype=float) for coord in boundary.exterior.coords[:-1]]
    if len(coords) < 3:
        return None

    best_segment: _BoundarySlideSegment | None = None
    best_distance = math.inf
    for start, end in zip(coords, coords[1:] + coords[:1], strict=True):
        direction = end - start
        length_sq = float(np.dot(direction, direction))
        if length_sq <= _EPSILON:
            continue
        t = float(np.dot(point_array - start, direction) / length_sq)
        if t <= tolerance or t >= 1.0 - tolerance:
            continue
        projected = start + t * direction
        distance = float(np.linalg.norm(point_array - projected))
        if distance <= tolerance and distance < best_distance:
            best_distance = distance
            best_segment = _BoundarySlideSegment(start.copy(), end.copy())
    if best_segment is not None:
        return best_segment

    for index, vertex in enumerate(coords):
        if float(np.linalg.norm(point_array - vertex)) > tolerance:
            continue
        prev_vertex = coords[index - 1]
        next_vertex = coords[(index + 1) % len(coords)]
        if _is_collinear_boundary_corner(prev_vertex, vertex, next_vertex, tolerance):
            return _BoundarySlideSegment(prev_vertex.copy(), next_vertex.copy())
    return None


def _is_collinear_boundary_corner(
    prev_vertex: np.ndarray,
    vertex: np.ndarray,
    next_vertex: np.ndarray,
    tolerance: float,
) -> bool:
    prev_vec = prev_vertex - vertex
    next_vec = next_vertex - vertex
    prev_norm = float(np.linalg.norm(prev_vec))
    next_norm = float(np.linalg.norm(next_vec))
    if prev_norm <= _EPSILON or next_norm <= _EPSILON:
        return False
    cross = abs(float(prev_vec[0] * next_vec[1] - prev_vec[1] * next_vec[0]))
    return cross <= tolerance * max(prev_norm, next_norm, 1.0)


def _apply_fixed_positions(
    positions: dict[int, np.ndarray], fixed_positions: dict[int, np.ndarray]
) -> None:
    for vertex_id, fixed in fixed_positions.items():
        positions[vertex_id] = fixed.copy()


def _apply_boundary_constraints(
    positions: dict[int, np.ndarray], constraints: _BoundaryConstraints
) -> None:
    _apply_boundary_slide_positions(positions, constraints.slide_segments)
    _apply_fixed_positions(positions, constraints.fixed_positions)


def _apply_boundary_slide_positions(
    positions: dict[int, np.ndarray],
    slide_segments: dict[int, _BoundarySlideSegment],
) -> None:
    for vertex_id, segment in slide_segments.items():
        point = positions.get(vertex_id)
        if point is None:
            continue
        positions[vertex_id] = _project_to_segment(point, segment)


def _project_to_segment(
    point: np.ndarray, segment: _BoundarySlideSegment
) -> np.ndarray:
    direction = segment.end - segment.start
    length_sq = float(np.dot(direction, direction))
    if length_sq <= _EPSILON:
        return segment.start.copy()
    t = float(np.dot(point - segment.start, direction) / length_sq)
    t = min(max(t, 0.0), 1.0)
    return segment.start + t * direction


def _max_vertex_displacement(
    layout: ChenLayout, boundary: Polygon, config: OptimizationConfig
) -> float:
    if config.max_vertex_displacement is not None:
        return max(float(config.max_vertex_displacement), 0.0)
    minx, miny, maxx, maxy = boundary.bounds
    diagonal = math.hypot(maxx - minx, maxy - miny)
    if diagonal <= _EPSILON:
        minx, miny, maxx, maxy = _positions_bounds(_layout_positions(layout))
        diagonal = math.hypot(maxx - minx, maxy - miny)
    return max(diagonal * config.max_vertex_displacement_ratio, 0.0)


def _positions_bounds(
    positions: dict[int, np.ndarray],
) -> tuple[float, float, float, float]:
    xs = [float(point[0]) for point in positions.values()]
    ys = [float(point[1]) for point in positions.values()]
    return min(xs), min(ys), max(xs), max(ys)


def _parcel_area_targets(
    layout: ChenLayout,
    original_positions: dict[int, np.ndarray],
    config: OptimizationConfig,
) -> dict[int, float]:
    if config.parcel_area_target is not None:
        target = max(float(config.parcel_area_target), _EPSILON)
        return {parcel_id: target for parcel_id in layout.mesh.parcels}
    return {
        parcel_id: max(
            abs(_parcel_signed_area(parcel.ring, original_positions)), _EPSILON
        )
        for parcel_id, parcel in layout.mesh.parcels.items()
    }


def _parcel_signed_areas(
    layout: ChenLayout, positions: dict[int, np.ndarray]
) -> dict[int, float]:
    return {
        parcel_id: _parcel_signed_area(parcel.ring, positions)
        for parcel_id, parcel in layout.mesh.parcels.items()
    }


def _parcel_signed_area(
    ring: tuple[int, ...], positions: dict[int, np.ndarray]
) -> float:
    return signed_area([_point_tuple(positions[vertex_id]) for vertex_id in ring])


def _non_inversion_guard_pass(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    original_signed_areas: dict[int, float],
    config: OptimizationConfig,
) -> bool:
    for parcel_id, parcel in layout.mesh.parcels.items():
        original = original_signed_areas[parcel_id]
        candidate = _parcel_signed_area(parcel.ring, positions)
        min_area = max(abs(original) * config.min_parcel_area_ratio, _EPSILON)
        if abs(candidate) <= min_area:
            return False
        if original * candidate <= 0.0:
            return False
    return True


def _active_constraints(
    config: OptimizationConfig,
    *,
    boundary_constraints: _BoundaryConstraints,
    axis_active: bool,
) -> dict[str, Any]:
    fixed_count = len(boundary_constraints.fixed_positions)
    sliding_count = len(boundary_constraints.slide_segments)
    return {
        "fixed_vertex_count": fixed_count,
        "boundary_fixed_vertex_count": fixed_count,
        "boundary_sliding_vertex_count": sliding_count,
        "boundary_constraint_mode": config.boundary_constraint_mode,
        "boundary_preservation": fixed_count > 0 or sliding_count > 0,
        "boundary_collinear_slide": sliding_count > 0,
        "regularity": config.weights.regularity > 0.0,
        "side_smoothness": config.weights.side_smoothness > 0.0,
        "street_smoothness": config.weights.street_smoothness > 0.0,
        "junction_orthogonality": config.weights.junction_orthogonality > 0.0,
        "axis_alignment": axis_active and config.weights.axis_alignment > 0.0,
        "area_regularization": (
            config.enable_area_regularization
            and config.weights.area_regularization > 0.0
        ),
        "rectangularity": (
            config.enable_rectangularity and config.weights.rectangularity > 0.0
        ),
        "edge_length_smoothness": (
            config.enable_edge_length_smoothness
            and config.weights.edge_length_smoothness > 0.0
            and not axis_active
        ),
        "closeness": config.weights.closeness > 0.0,
        "topology_guard": config.preserve_topology,
        "non_inversion_guard": config.reject_inversions,
    }


def _record_rejection(stats: _ProposalStats, reason: str) -> None:
    stats.rejections[reason] = stats.rejections.get(reason, 0) + 1


def _relaxation_step(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    original_positions: dict[int, np.ndarray],
    area_targets: dict[int, float],
    boundary_constraints: _BoundaryConstraints,
    axis_plan: _AxisPlan,
    config: OptimizationConfig,
) -> dict[int, np.ndarray]:
    fixed_positions = boundary_constraints.fixed_positions
    deltas = {
        vertex_id: np.zeros(2, dtype=float)
        for vertex_id in layout.mesh.vertices
        if vertex_id not in fixed_positions
    }
    weights = {vertex_id: 0.0 for vertex_id in deltas}
    if not deltas:
        return {}

    _add_path_smoothing_deltas(
        _side_smoothing_paths(layout),
        positions,
        deltas,
        weights,
        config.weights.side_smoothness,
    )
    _add_path_smoothing_deltas(
        _street_smoothing_paths(layout),
        positions,
        deltas,
        weights,
        config.weights.street_smoothness,
    )
    _add_junction_deltas(layout, positions, deltas, weights, config)
    _add_area_regularization_deltas(
        layout,
        positions,
        area_targets,
        deltas,
        weights,
        config,
    )
    _add_rectangularity_deltas(layout, positions, deltas, weights, config)
    if not axis_plan.groups:
        _add_edge_length_smoothing_deltas(
            _edge_length_smoothing_paths(layout),
            positions,
            deltas,
            weights,
            config,
        )
    _add_axis_deltas(
        axis_plan, positions, deltas, weights, config.weights.axis_alignment
    )
    _add_closeness_deltas(
        positions,
        original_positions,
        deltas,
        weights,
        config.weights.closeness,
    )

    step: dict[int, np.ndarray] = {}
    for vertex_id, delta in deltas.items():
        weight = weights[vertex_id]
        if weight <= _EPSILON:
            continue
        step[vertex_id] = config.step_size * delta / weight
    return step


def _shapeop_like_projection_step(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    original_positions: dict[int, np.ndarray],
    area_targets: dict[int, float],
    boundary_constraints: _BoundaryConstraints,
    axis_plan: _AxisPlan,
    config: OptimizationConfig,
) -> dict[int, np.ndarray]:
    fixed_positions = boundary_constraints.fixed_positions
    equations = _projection_equations(
        layout,
        positions,
        original_positions,
        area_targets,
        boundary_constraints,
        axis_plan,
        config,
        include_solver_anchor=True,
    )
    if not equations:
        return {}

    solved_positions = _solve_projection_positions(
        positions,
        fixed_positions,
        equations,
        solver_tolerance=config.solver_tolerance,
    )
    step: dict[int, np.ndarray] = {}
    for vertex_id, point in sorted(positions.items()):
        if vertex_id in fixed_positions:
            continue
        delta = solved_positions[vertex_id] - point
        if float(np.linalg.norm(delta)) <= _EPSILON:
            continue
        step[vertex_id] = config.step_size * delta
    return step


def _projection_equations(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    original_positions: dict[int, np.ndarray],
    area_targets: dict[int, float],
    boundary_constraints: _BoundaryConstraints,
    axis_plan: _AxisPlan,
    config: OptimizationConfig,
    *,
    include_solver_anchor: bool,
) -> tuple[_ScalarProjectionEquation, ...]:
    equations: list[_ScalarProjectionEquation] = []
    _add_regularity_projection_equations(
        layout,
        positions,
        config.weights.regularity,
        equations,
    )
    _add_path_laplacian_equations(
        _side_smoothing_paths(layout),
        "side_smoothness",
        config.weights.side_smoothness,
        equations,
    )
    _add_path_laplacian_equations(
        _street_smoothing_paths(layout),
        "street_smoothness",
        config.weights.street_smoothness,
        equations,
    )
    _add_path_chord_projection_equations(
        _side_smoothing_paths(layout),
        positions,
        "side_path_projection",
        config.weights.side_smoothness,
        equations,
    )
    _add_path_chord_projection_equations(
        _street_smoothing_paths(layout),
        positions,
        "street_path_projection",
        config.weights.street_smoothness,
        equations,
    )
    _add_junction_projection_equations(
        layout,
        positions,
        config.weights.junction_orthogonality,
        equations,
    )
    _add_area_projection_equations(
        layout,
        positions,
        area_targets,
        config,
        equations,
    )
    _add_rectangularity_projection_equations(
        layout,
        positions,
        config,
        equations,
    )
    if config.enable_edge_length_smoothness and not axis_plan.groups:
        _add_path_equal_spacing_projection_equations(
            _edge_length_smoothing_paths(layout),
            positions,
            "edge_length_smoothness",
            config.weights.edge_length_smoothness * config.edge_length_projection_step,
            equations,
        )
    _add_axis_projection_equations(
        axis_plan,
        config.weights.axis_alignment,
        equations,
    )
    _add_boundary_slide_projection_equations(
        boundary_constraints.slide_segments,
        config.boundary_shape_constraint_weight,
        equations,
    )
    _add_closeness_projection_equations(
        positions,
        original_positions,
        config.weights.closeness,
        equations,
    )
    if include_solver_anchor:
        _add_solver_anchor_equations(
            positions,
            boundary_constraints.fixed_positions,
            config.solver_anchor_weight,
            equations,
        )
    return tuple(equations)


def _add_regularity_projection_equations(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    summary = _regularity_projection_summary(layout, positions, weight)
    for target in summary.targets:
        _add_vector_target_equations(
            target.vertex_id,
            target.target,
            "regularity",
            target.equation_weight,
            equations,
        )


def _regularity_projection_summary(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    weight: float,
) -> _RegularityProjectionSummary:
    if weight <= 0.0:
        return _RegularityProjectionSummary(
            kind="disabled",
            projected_parcel_count=0,
            skipped_parcel_count=0,
            skipped_by_reason={},
            equation_count=0,
            residual=0.0,
            target_displacement_rms=0.0,
            target_displacement_max=0.0,
            targets=(),
        )

    targets: list[_RegularityProjectionTarget] = []
    projected_parcel_count = 0
    skipped_by_reason: dict[str, int] = {}
    weighted_residual = 0.0
    displacement_sum_sq = 0.0
    displacement_max = 0.0
    displacement_count = 0
    for parcel_id, corner_ring in sorted(
        layout.corner_graph.parcel_corner_rings.items()
    ):
        if len(corner_ring) < 3:
            _increment_count(skipped_by_reason, "too_few_vertices")
            continue
        points = np.array(
            [positions[vertex_id] for vertex_id in corner_ring], dtype=float
        )
        regular_targets, skip_reason = _best_regular_polygon_targets(points)
        if regular_targets is None:
            _increment_count(skipped_by_reason, skip_reason or "no_valid_fit")
            continue
        mean_side_length = _mean_ring_side_length(points)
        if mean_side_length <= _EPSILON:
            _increment_count(skipped_by_reason, "degenerate_mean_side_length")
            continue
        equation_weight = weight / (
            len(corner_ring) * mean_side_length * mean_side_length
        )
        projected_parcel_count += 1
        deltas = points - regular_targets
        delta_lengths = np.linalg.norm(deltas, axis=1)
        weighted_residual += equation_weight * float(np.sum(deltas * deltas))
        displacement_sum_sq += float(np.sum(delta_lengths * delta_lengths))
        displacement_max = max(displacement_max, float(np.max(delta_lengths)))
        displacement_count += len(delta_lengths)
        for vertex_id, target in zip(corner_ring, regular_targets, strict=True):
            targets.append(
                _RegularityProjectionTarget(
                    parcel_id=parcel_id,
                    vertex_id=vertex_id,
                    target=np.array(target, dtype=float),
                    equation_weight=equation_weight,
                )
            )

    skipped_parcel_count = int(sum(skipped_by_reason.values()))
    target_displacement_rms = (
        math.sqrt(displacement_sum_sq / displacement_count)
        if displacement_count
        else 0.0
    )
    return _RegularityProjectionSummary(
        kind=_REGULARITY_PROJECTION_KIND if projected_parcel_count else "none",
        projected_parcel_count=projected_parcel_count,
        skipped_parcel_count=skipped_parcel_count,
        skipped_by_reason=dict(sorted(skipped_by_reason.items())),
        equation_count=2 * len(targets),
        residual=weighted_residual,
        target_displacement_rms=target_displacement_rms,
        target_displacement_max=displacement_max,
        targets=tuple(targets),
    )


def _increment_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _best_regular_polygon_targets(
    points: np.ndarray,
) -> tuple[np.ndarray | None, str | None]:
    vertex_count = int(points.shape[0])
    if vertex_count < 3:
        return None, "too_few_vertices"
    centered = points - np.mean(points, axis=0)
    if float(np.sum(centered * centered)) <= _EPSILON:
        return None, "degenerate_current_ring"
    current_orientation = _signed_area_array(points)
    if abs(current_orientation) <= _EPSILON:
        return None, "degenerate_current_ring"

    base = _unit_regular_polygon(vertex_count)
    best_targets: np.ndarray | None = None
    best_residual = math.inf
    orientation_flip_count = 0
    for reflected in (False, True):
        for offset in range(vertex_count):
            candidate = np.array(
                [
                    base[(offset - index) % vertex_count]
                    if reflected
                    else base[(offset + index) % vertex_count]
                    for index in range(vertex_count)
                ],
                dtype=float,
            )
            fit = _similarity_fit_targets(candidate, points)
            if current_orientation * _signed_area_array(fit) <= _EPSILON:
                orientation_flip_count += 1
                continue
            residual = float(np.sum((points - fit) * (points - fit)))
            if residual < best_residual - _EPSILON:
                best_residual = residual
                best_targets = fit
    if best_targets is None:
        if orientation_flip_count:
            return None, "orientation_flip"
        return None, "no_valid_fit"
    return best_targets, None


def _mean_ring_side_length(points: np.ndarray) -> float:
    if points.shape[0] < 2:
        return 0.0
    lengths = [
        float(np.linalg.norm(points[(index + 1) % len(points)] - points[index]))
        for index in range(len(points))
    ]
    return float(np.mean(lengths)) if lengths else 0.0


def _signed_area_array(points: np.ndarray) -> float:
    if points.shape[0] < 3:
        return 0.0
    shifted = np.roll(points, -1, axis=0)
    return 0.5 * float(
        np.sum(points[:, 0] * shifted[:, 1] - shifted[:, 0] * points[:, 1])
    )


def _unit_regular_polygon(vertex_count: int) -> np.ndarray:
    return np.array(
        [
            (
                math.cos(2.0 * math.pi * index / vertex_count),
                math.sin(2.0 * math.pi * index / vertex_count),
            )
            for index in range(vertex_count)
        ],
        dtype=float,
    )


def _similarity_fit_targets(source: np.ndarray, target: np.ndarray) -> np.ndarray:
    source_centroid = np.mean(source, axis=0)
    target_centroid = np.mean(target, axis=0)
    source_centered = source - source_centroid
    target_centered = target - target_centroid
    denominator = float(np.sum(source_centered * source_centered))
    if denominator <= _EPSILON:
        return np.repeat(target_centroid[None, :], source.shape[0], axis=0)

    source_complex = source_centered[:, 0] + 1j * source_centered[:, 1]
    target_complex = target_centered[:, 0] + 1j * target_centered[:, 1]
    transform = np.vdot(source_complex, target_complex) / denominator
    fitted_complex = transform * source_complex + complex(
        float(target_centroid[0]), float(target_centroid[1])
    )
    return np.column_stack((np.real(fitted_complex), np.imag(fitted_complex)))


def _add_path_laplacian_equations(
    paths: tuple[tuple[int, ...], ...],
    term: str,
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    for path in paths:
        for prev_node, node, next_node in zip(path, path[1:], path[2:], strict=False):
            for coordinate_index in range(2):
                equations.append(
                    _ScalarProjectionEquation(
                        term=term,
                        coeffs=(
                            (prev_node, coordinate_index, 1.0),
                            (node, coordinate_index, -2.0),
                            (next_node, coordinate_index, 1.0),
                        ),
                        target=0.0,
                        weight=weight,
                    )
                )


def _add_path_chord_projection_equations(
    paths: tuple[tuple[int, ...], ...],
    positions: dict[int, np.ndarray],
    term: str,
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    for path in paths:
        if len(path) < 3:
            continue
        targets = _path_chord_targets(path, positions)
        for node in path[1:-1]:
            target = targets.get(node)
            if target is None:
                continue
            for coordinate_index in range(2):
                equations.append(
                    _ScalarProjectionEquation(
                        term=term,
                        coeffs=((node, coordinate_index, 1.0),),
                        target=float(target[coordinate_index]),
                        weight=weight,
                    )
                )


def _add_path_equal_spacing_projection_equations(
    paths: tuple[tuple[int, ...], ...],
    positions: dict[int, np.ndarray],
    term: str,
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    for path in paths:
        if len(path) < 3:
            continue
        targets = _path_equal_spacing_targets(path, positions)
        for node in path[1:-1]:
            target = targets.get(node)
            if target is None:
                continue
            for coordinate_index in range(2):
                equations.append(
                    _ScalarProjectionEquation(
                        term=term,
                        coeffs=((node, coordinate_index, 1.0),),
                        target=float(target[coordinate_index]),
                        weight=weight,
                    )
                )


def _path_chord_targets(
    path: tuple[int, ...], positions: dict[int, np.ndarray]
) -> dict[int, np.ndarray]:
    start = positions[path[0]]
    end = positions[path[-1]]
    chord = end - start
    total_length = 0.0
    cumulative = [0.0]
    for prev_node, node in zip(path, path[1:], strict=False):
        total_length += float(np.linalg.norm(positions[node] - positions[prev_node]))
        cumulative.append(total_length)
    if total_length <= _EPSILON:
        return {}
    return {
        node: start + chord * (distance / total_length)
        for node, distance in zip(path[1:-1], cumulative[1:-1], strict=True)
    }


def _path_equal_spacing_targets(
    path: tuple[int, ...], positions: dict[int, np.ndarray]
) -> dict[int, np.ndarray]:
    start = positions[path[0]]
    end = positions[path[-1]]
    denominator = len(path) - 1
    if denominator <= 0:
        return {}
    return {
        node: start + (end - start) * (index / denominator)
        for index, node in enumerate(path[1:-1], start=1)
    }


def _area_regularization_vertex_targets(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    area_targets: dict[int, float],
    config: OptimizationConfig,
) -> dict[int, np.ndarray]:
    target_sums: dict[int, np.ndarray] = {}
    target_weights: dict[int, float] = {}
    for parcel_id, parcel in sorted(layout.mesh.parcels.items()):
        target_area = max(area_targets[parcel_id], _EPSILON)
        current_signed_area = _parcel_signed_area(parcel.ring, positions)
        current_area = abs(current_signed_area)
        normalized_error = (target_area - current_area) / target_area
        if abs(normalized_error) <= config.convergence_tolerance:
            continue
        orientation = 1.0 if current_signed_area >= 0.0 else -1.0
        for index, vertex_id in enumerate(parcel.ring):
            prev_point = positions[parcel.ring[index - 1]]
            next_point = positions[parcel.ring[(index + 1) % len(parcel.ring)]]
            signed_area_gradient = np.array(
                (
                    0.5 * (next_point[1] - prev_point[1]),
                    0.5 * (prev_point[0] - next_point[0]),
                ),
                dtype=float,
            )
            correction = (
                config.area_projection_step
                * normalized_error
                * orientation
                * signed_area_gradient
            )
            _accumulate_vector_target(
                vertex_id,
                positions[vertex_id] + correction,
                max(abs(normalized_error), 0.05),
                target_sums,
                target_weights,
            )
    return _averaged_vector_targets(target_sums, target_weights)


def _rectangularity_vertex_targets(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    config: OptimizationConfig,
) -> dict[int, np.ndarray]:
    target_sums: dict[int, np.ndarray] = {}
    target_weights: dict[int, float] = {}
    projection_step = config.rectangularity_projection_step
    if projection_step <= 0.0:
        return {}
    for parcel in sorted(layout.mesh.parcels.values(), key=lambda item: item.parcel_id):
        ring = parcel.ring
        for index, vertex_id in enumerate(ring):
            prev_id = ring[index - 1]
            next_id = ring[(index + 1) % len(ring)]
            point = positions[vertex_id]
            left_vec = positions[prev_id] - point
            right_vec = positions[next_id] - point
            left_len_sq = float(np.dot(left_vec, left_vec))
            right_len_sq = float(np.dot(right_vec, right_vec))
            if left_len_sq <= _EPSILON or right_len_sq <= _EPSILON:
                continue
            dot = float(np.dot(left_vec, right_vec))
            cos_abs = abs(dot) / math.sqrt(left_len_sq * right_len_sq)
            if cos_abs <= 1e-6:
                continue
            left_target = (
                positions[prev_id] - projection_step * (dot / right_len_sq) * right_vec
            )
            right_target = (
                positions[next_id] - projection_step * (dot / left_len_sq) * left_vec
            )
            corner_target = point + projection_step * dot * (
                left_vec + right_vec
            ) / max(left_len_sq + right_len_sq, _EPSILON)
            _accumulate_vector_target(
                prev_id, left_target, cos_abs, target_sums, target_weights
            )
            _accumulate_vector_target(
                vertex_id, corner_target, cos_abs, target_sums, target_weights
            )
            _accumulate_vector_target(
                next_id, right_target, cos_abs, target_sums, target_weights
            )
    return _averaged_vector_targets(target_sums, target_weights)


def _accumulate_vector_target(
    vertex_id: int,
    target: np.ndarray,
    weight: float,
    target_sums: dict[int, np.ndarray],
    target_weights: dict[int, float],
) -> None:
    if weight <= 0.0:
        return
    if vertex_id not in target_sums:
        target_sums[vertex_id] = np.zeros(2, dtype=float)
        target_weights[vertex_id] = 0.0
    target_sums[vertex_id] += weight * target
    target_weights[vertex_id] += weight


def _averaged_vector_targets(
    target_sums: dict[int, np.ndarray],
    target_weights: dict[int, float],
) -> dict[int, np.ndarray]:
    return {
        vertex_id: target_sum / target_weights[vertex_id]
        for vertex_id, target_sum in target_sums.items()
        if target_weights[vertex_id] > _EPSILON
    }


def _add_junction_projection_equations(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    streets_by_id = {street.street_id: street for street in layout.street_graph.streets}
    for junction, street_ids in sorted(layout.street_graph.junctions.items()):
        streets = [streets_by_id[street_id] for street_id in sorted(street_ids)]
        for left_index, left in enumerate(streets):
            left_neighbor = _street_neighbor_at_junction(left, junction)
            if left_neighbor is None:
                continue
            left_vec = positions[left_neighbor] - positions[junction]
            left_len_sq = float(np.dot(left_vec, left_vec))
            if left_len_sq <= _EPSILON:
                continue
            for right in streets[left_index + 1 :]:
                right_neighbor = _street_neighbor_at_junction(right, junction)
                if right_neighbor is None:
                    continue
                right_vec = positions[right_neighbor] - positions[junction]
                right_len_sq = float(np.dot(right_vec, right_vec))
                if right_len_sq <= _EPSILON:
                    continue
                dot = float(np.dot(left_vec, right_vec))
                cos_abs = abs(dot) / math.sqrt(left_len_sq * right_len_sq)
                if cos_abs <= 0.01:
                    continue
                left_target = (
                    positions[junction] + left_vec - (dot / right_len_sq) * right_vec
                )
                right_target = (
                    positions[junction] + right_vec - (dot / left_len_sq) * left_vec
                )
                projection_weight = weight * cos_abs
                _add_vector_target_equations(
                    left_neighbor,
                    left_target,
                    "junction_orthogonality",
                    projection_weight,
                    equations,
                )
                _add_vector_target_equations(
                    right_neighbor,
                    right_target,
                    "junction_orthogonality",
                    projection_weight,
                    equations,
                )


def _add_area_projection_equations(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    area_targets: dict[int, float],
    config: OptimizationConfig,
    equations: list[_ScalarProjectionEquation],
) -> None:
    weight = config.weights.area_regularization
    if not config.enable_area_regularization or weight <= 0.0:
        return
    targets = _area_regularization_vertex_targets(
        layout, positions, area_targets, config
    )
    for vertex_id, target in sorted(targets.items()):
        _add_vector_target_equations(
            vertex_id,
            target,
            "area_regularization",
            weight,
            equations,
        )


def _add_rectangularity_projection_equations(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    config: OptimizationConfig,
    equations: list[_ScalarProjectionEquation],
) -> None:
    weight = config.weights.rectangularity
    if not config.enable_rectangularity or weight <= 0.0:
        return
    targets = _rectangularity_vertex_targets(layout, positions, config)
    for vertex_id, target in sorted(targets.items()):
        _add_vector_target_equations(
            vertex_id,
            target,
            "rectangularity",
            weight,
            equations,
        )


def _add_axis_projection_equations(
    axis_plan: _AxisPlan,
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    for group in axis_plan.groups:
        for vertex_id in sorted(group.vertices):
            equations.append(
                _ScalarProjectionEquation(
                    term="axis_alignment",
                    coeffs=((vertex_id, group.coordinate_index, 1.0),),
                    target=group.target,
                    weight=weight,
                )
            )


def _add_boundary_slide_projection_equations(
    slide_segments: dict[int, _BoundarySlideSegment],
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    for vertex_id, segment in sorted(slide_segments.items()):
        direction = segment.end - segment.start
        length = float(np.linalg.norm(direction))
        if length <= _EPSILON:
            continue
        normal = np.array((-direction[1] / length, direction[0] / length), dtype=float)
        target = float(np.dot(normal, segment.start))
        equations.append(
            _ScalarProjectionEquation(
                term="boundary_shape_constraint",
                coeffs=(
                    (vertex_id, 0, float(normal[0])),
                    (vertex_id, 1, float(normal[1])),
                ),
                target=target,
                weight=weight,
            )
        )


def _add_closeness_projection_equations(
    positions: dict[int, np.ndarray],
    original_positions: dict[int, np.ndarray],
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    for vertex_id in sorted(positions):
        _add_vector_target_equations(
            vertex_id,
            original_positions[vertex_id],
            "closeness",
            weight,
            equations,
        )


def _add_solver_anchor_equations(
    positions: dict[int, np.ndarray],
    fixed_positions: dict[int, np.ndarray],
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    for vertex_id, point in sorted(positions.items()):
        if vertex_id in fixed_positions:
            continue
        _add_vector_target_equations(
            vertex_id,
            point,
            "solver_anchor",
            weight,
            equations,
        )


def _add_vector_target_equations(
    vertex_id: int,
    target: np.ndarray,
    term: str,
    weight: float,
    equations: list[_ScalarProjectionEquation],
) -> None:
    if weight <= 0.0:
        return
    for coordinate_index in range(2):
        equations.append(
            _ScalarProjectionEquation(
                term=term,
                coeffs=((vertex_id, coordinate_index, 1.0),),
                target=float(target[coordinate_index]),
                weight=weight,
            )
        )


def _solve_projection_positions(
    positions: dict[int, np.ndarray],
    fixed_positions: dict[int, np.ndarray],
    equations: tuple[_ScalarProjectionEquation, ...],
    *,
    solver_tolerance: float,
) -> dict[int, np.ndarray]:
    free_ids = sorted(
        vertex_id for vertex_id in positions if vertex_id not in fixed_positions
    )
    if not free_ids:
        return _copy_positions(positions)

    variable_index = {
        (vertex_id, coordinate_index): 2 * index + coordinate_index
        for index, vertex_id in enumerate(free_ids)
        for coordinate_index in range(2)
    }
    rows: list[int] = []
    cols: list[int] = []
    data: list[float] = []
    rhs: list[float] = []
    row_index = 0
    for equation in equations:
        if equation.weight <= 0.0:
            continue
        scale = math.sqrt(equation.weight)
        target = equation.target
        row_terms: list[tuple[int, float]] = []
        for vertex_id, coordinate_index, coefficient in equation.coeffs:
            fixed = fixed_positions.get(vertex_id)
            if fixed is not None:
                target -= coefficient * float(fixed[coordinate_index])
                continue
            column = variable_index.get((vertex_id, coordinate_index))
            if column is None:
                continue
            row_terms.append((column, coefficient))
        if not row_terms:
            continue
        for column, coefficient in row_terms:
            rows.append(row_index)
            cols.append(column)
            data.append(scale * coefficient)
        rhs.append(scale * target)
        row_index += 1

    if row_index == 0:
        return _copy_positions(positions)

    matrix = sparse.csr_matrix(
        (data, (rows, cols)),
        shape=(row_index, 2 * len(free_ids)),
        dtype=float,
    )
    solution = sparse_linalg.lsqr(
        matrix,
        np.array(rhs, dtype=float),
        atol=solver_tolerance,
        btol=solver_tolerance,
        iter_lim=max(100, matrix.shape[1] * 8),
    )[0]
    solved = _copy_positions(positions)
    for vertex_id in free_ids:
        solved[vertex_id] = np.array(
            (
                solution[variable_index[(vertex_id, 0)]],
                solution[variable_index[(vertex_id, 1)]],
            ),
            dtype=float,
        )
    _apply_fixed_positions(solved, fixed_positions)
    return solved


def _add_path_smoothing_deltas(
    paths: tuple[tuple[int, ...], ...],
    positions: dict[int, np.ndarray],
    deltas: dict[int, np.ndarray],
    weights: dict[int, float],
    weight: float,
) -> None:
    if weight <= 0.0:
        return
    for path in paths:
        for prev_node, node, next_node in zip(path, path[1:], path[2:], strict=False):
            if node not in deltas:
                continue
            target = 0.5 * (positions[prev_node] + positions[next_node])
            _add_delta(node, target - positions[node], weight, deltas, weights)


def _add_junction_deltas(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    deltas: dict[int, np.ndarray],
    weights: dict[int, float],
    config: OptimizationConfig,
) -> None:
    weight = config.weights.junction_orthogonality
    if weight <= 0.0:
        return
    streets_by_id = {street.street_id: street for street in layout.street_graph.streets}
    for junction, street_ids in sorted(layout.street_graph.junctions.items()):
        streets = [streets_by_id[street_id] for street_id in sorted(street_ids)]
        for left_index, left in enumerate(streets):
            left_neighbor = _street_neighbor_at_junction(left, junction)
            if left_neighbor is None:
                continue
            left_vec = positions[left_neighbor] - positions[junction]
            left_len_sq = float(np.dot(left_vec, left_vec))
            if left_len_sq <= _EPSILON:
                continue
            for right in streets[left_index + 1 :]:
                right_neighbor = _street_neighbor_at_junction(right, junction)
                if right_neighbor is None:
                    continue
                right_vec = positions[right_neighbor] - positions[junction]
                right_len_sq = float(np.dot(right_vec, right_vec))
                if right_len_sq <= _EPSILON:
                    continue
                cos_abs = abs(
                    float(np.dot(left_vec, right_vec))
                    / math.sqrt(left_len_sq * right_len_sq)
                )
                if cos_abs <= 0.01:
                    continue
                if left_neighbor in deltas:
                    correction = (
                        -0.5
                        * (float(np.dot(left_vec, right_vec)) / right_len_sq)
                        * right_vec
                    )
                    _add_delta(
                        left_neighbor,
                        correction,
                        weight * cos_abs,
                        deltas,
                        weights,
                    )
                if right_neighbor in deltas:
                    correction = (
                        -0.5
                        * (float(np.dot(left_vec, right_vec)) / left_len_sq)
                        * left_vec
                    )
                    _add_delta(
                        right_neighbor,
                        correction,
                        weight * cos_abs,
                        deltas,
                        weights,
                    )


def _add_area_regularization_deltas(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    area_targets: dict[int, float],
    deltas: dict[int, np.ndarray],
    weights: dict[int, float],
    config: OptimizationConfig,
) -> None:
    weight = config.weights.area_regularization
    if not config.enable_area_regularization or weight <= 0.0:
        return
    for vertex_id, target in _area_regularization_vertex_targets(
        layout, positions, area_targets, config
    ).items():
        if vertex_id not in deltas:
            continue
        _add_delta(vertex_id, target - positions[vertex_id], weight, deltas, weights)


def _add_rectangularity_deltas(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    deltas: dict[int, np.ndarray],
    weights: dict[int, float],
    config: OptimizationConfig,
) -> None:
    weight = config.weights.rectangularity
    if not config.enable_rectangularity or weight <= 0.0:
        return
    for vertex_id, target in _rectangularity_vertex_targets(
        layout, positions, config
    ).items():
        if vertex_id not in deltas:
            continue
        _add_delta(vertex_id, target - positions[vertex_id], weight, deltas, weights)


def _add_edge_length_smoothing_deltas(
    paths: tuple[tuple[int, ...], ...],
    positions: dict[int, np.ndarray],
    deltas: dict[int, np.ndarray],
    weights: dict[int, float],
    config: OptimizationConfig,
) -> None:
    weight = config.weights.edge_length_smoothness
    if not config.enable_edge_length_smoothness or weight <= 0.0:
        return
    for path in paths:
        targets = _path_equal_spacing_targets(path, positions)
        for vertex_id, target in targets.items():
            if vertex_id not in deltas:
                continue
            delta = config.edge_length_projection_step * (target - positions[vertex_id])
            _add_delta(vertex_id, delta, weight, deltas, weights)


def _add_axis_deltas(
    axis_plan: _AxisPlan,
    positions: dict[int, np.ndarray],
    deltas: dict[int, np.ndarray],
    weights: dict[int, float],
    weight: float,
) -> None:
    if weight <= 0.0:
        return
    for group in axis_plan.groups:
        for vertex_id in sorted(group.vertices):
            if vertex_id not in deltas:
                continue
            delta = np.zeros(2, dtype=float)
            delta[group.coordinate_index] = (
                group.target - positions[vertex_id][group.coordinate_index]
            )
            _add_delta(vertex_id, delta, weight, deltas, weights)


def _add_closeness_deltas(
    positions: dict[int, np.ndarray],
    original_positions: dict[int, np.ndarray],
    deltas: dict[int, np.ndarray],
    weights: dict[int, float],
    weight: float,
) -> None:
    if weight <= 0.0:
        return
    for vertex_id in deltas:
        _add_delta(
            vertex_id,
            original_positions[vertex_id] - positions[vertex_id],
            weight,
            deltas,
            weights,
        )


def _add_delta(
    vertex_id: int,
    delta: np.ndarray,
    weight: float,
    deltas: dict[int, np.ndarray],
    weights: dict[int, float],
) -> None:
    if weight <= 0.0:
        return
    deltas[vertex_id] += weight * delta
    weights[vertex_id] += weight


def _candidate_positions(
    positions: dict[int, np.ndarray],
    original_positions: dict[int, np.ndarray],
    boundary_constraints: _BoundaryConstraints,
    axis_plan: _AxisPlan,
    raw_step: dict[int, np.ndarray],
    *,
    factor: float,
    displacement_limit: float,
    iteration_limit: float,
) -> dict[int, np.ndarray]:
    candidate = _copy_positions(positions)
    for vertex_id, step in raw_step.items():
        step = factor * step
        step_norm = float(np.linalg.norm(step))
        if step_norm > iteration_limit > 0.0:
            step = step * (iteration_limit / step_norm)
        moved = positions[vertex_id] + step
        total = moved - original_positions[vertex_id]
        total_norm = float(np.linalg.norm(total))
        if total_norm > displacement_limit > 0.0:
            moved = original_positions[vertex_id] + total * (
                displacement_limit / total_norm
            )
        candidate[vertex_id] = moved
    _apply_axis_group_positions(candidate, axis_plan)
    _apply_boundary_constraints(candidate, boundary_constraints)
    return candidate


def _apply_axis_group_positions(
    positions: dict[int, np.ndarray], axis_plan: _AxisPlan
) -> None:
    for group in axis_plan.groups:
        for vertex_id in group.vertices:
            if vertex_id in positions:
                positions[vertex_id][group.coordinate_index] = group.target


def _max_position_delta(
    before: dict[int, np.ndarray], after: dict[int, np.ndarray]
) -> float:
    return max(
        (
            float(np.linalg.norm(after[vertex_id] - point))
            for vertex_id, point in before.items()
        ),
        default=0.0,
    )


def _try_layout_from_positions(
    template: ChenLayout, positions: dict[int, np.ndarray]
) -> ChenLayout | None:
    try:
        return _layout_from_positions(template, positions)
    except (ValueError, shapely.GEOSException):
        return None


def _layout_from_positions(
    template: ChenLayout, positions: dict[int, np.ndarray]
) -> ChenLayout:
    vertices = {
        vertex_id: MeshVertex(
            vertex_id=vertex_id,
            point=_point_tuple(positions[vertex_id]),
            on_boundary=template.mesh.vertices[vertex_id].on_boundary,
        )
        for vertex_id in template.mesh.vertices
    }
    parcels: dict[int, ParcelFace] = {}
    for parcel_id, parcel in template.mesh.parcels.items():
        coords = [vertices[vertex_id].point for vertex_id in parcel.ring]
        poly = Polygon(coords)
        if not poly.is_valid or poly.area <= _EPSILON:
            raise ValueError(f"optimized parcel became invalid: {parcel_id}")
        parcels[parcel_id] = ParcelFace(parcel_id, parcel.ring, poly)
    mesh = ParcelMesh(vertices, parcels)
    return build_chen_layout(mesh, set(template.street_network.edges))


def _point_tuple(point: np.ndarray) -> tuple[float, float]:
    return float(point[0]), float(point[1])


def _geometry_constraints_pass(
    layout: ChenLayout, boundary: Polygon, config: OptimizationConfig
) -> bool:
    report = evaluate_layout_invariants(layout, target_boundary=boundary)
    metrics = report.metrics
    return (
        all(
            parcel.geom.is_valid and parcel.geom.area > _EPSILON
            for parcel in layout.mesh.parcels.values()
        )
        and metrics["parcel_overlap_count"] == 0
        and metrics.get("coverage_rate", 0.0) >= config.min_coverage
        and metrics.get("coverage_spillover_rate", math.inf) <= config.max_spillover
    )


def _energy_components(
    layout: ChenLayout,
    original_positions: dict[int, np.ndarray],
    area_targets: dict[int, float],
    config: OptimizationConfig,
    *,
    axis_active: bool,
) -> dict[str, float]:
    positions = _layout_positions(layout)
    return {
        "irregularity": float(
            sum(
                chen_irregularity(parcel.geom)
                for parcel in layout.mesh.parcels.values()
            )
        ),
        "side_smoothness": _path_smoothness(_side_smoothing_paths(layout), positions),
        "street_smoothness": _path_smoothness(
            _street_smoothing_paths(layout), positions
        ),
        "junction_orthogonality": _junction_orthogonality(layout),
        "closeness": _layout_closeness(layout, original_positions),
        "axis_alignment": _axis_deviation(layout)
        if axis_active and config.weights.axis_alignment > 0.0
        else 0.0,
        "area_regularization": _area_regularization_energy(
            layout, positions, area_targets
        )
        if config.enable_area_regularization
        and config.weights.area_regularization > 0.0
        else 0.0,
        "rectangularity": _rectangularity_energy(layout, positions, config)
        if config.enable_rectangularity and config.weights.rectangularity > 0.0
        else 0.0,
        "edge_length_smoothness": _edge_length_smoothness_energy(
            _edge_length_smoothing_paths(layout), positions
        )
        if config.enable_edge_length_smoothness
        and config.weights.edge_length_smoothness > 0.0
        and not axis_active
        else 0.0,
    }


def _weighted_energy(
    components: dict[str, float], weights: OptimizationWeights
) -> float:
    return float(
        weights.regularity * components["irregularity"]
        + weights.side_smoothness * components["side_smoothness"]
        + weights.street_smoothness * components["street_smoothness"]
        + weights.junction_orthogonality * components["junction_orthogonality"]
        + weights.closeness * components["closeness"]
        + weights.axis_alignment * components["axis_alignment"]
        + weights.area_regularization * components["area_regularization"]
        + weights.rectangularity * components["rectangularity"]
        + weights.edge_length_smoothness * components["edge_length_smoothness"]
    )


def _side_smoothing_paths(layout: ChenLayout) -> tuple[tuple[int, ...], ...]:
    paths = {
        tuple(path)
        for path in layout.corner_graph.edge_paths.values()
        if len(path) >= 3
    }
    for parcel in layout.mesh.parcels.values():
        ring = parcel.ring
        if len(ring) < 3:
            continue
        for index, node in enumerate(ring):
            prev_node = ring[index - 1]
            next_node = ring[(index + 1) % len(ring)]
            prev = layout.mesh.vertices[prev_node].point
            point = layout.mesh.vertices[node].point
            nxt = layout.mesh.vertices[next_node].point
            if included_angle_unsigned(prev, point, nxt) > CHEN_COLLINEAR_THRESHOLD_RAD:
                paths.add((prev_node, node, next_node))
    return tuple(sorted(paths))


def _street_smoothing_paths(layout: ChenLayout) -> tuple[tuple[int, ...], ...]:
    return tuple(
        street.nodes for street in layout.street_graph.streets if len(street.nodes) >= 3
    )


def _edge_length_smoothing_paths(layout: ChenLayout) -> tuple[tuple[int, ...], ...]:
    return tuple(
        sorted(
            set(_side_smoothing_paths(layout)).union(_street_smoothing_paths(layout))
        )
    )


def _area_regularization_energy(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    area_targets: dict[int, float],
) -> float:
    total = 0.0
    for parcel_id, parcel in layout.mesh.parcels.items():
        target = max(area_targets[parcel_id], _EPSILON)
        area = abs(_parcel_signed_area(parcel.ring, positions))
        normalized_error = (area - target) / target
        total += normalized_error * normalized_error
    return float(total)


def _rectangularity_energy(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    config: OptimizationConfig,
) -> float:
    del config
    total = 0.0
    for parcel in layout.mesh.parcels.values():
        ring = parcel.ring
        for index, vertex_id in enumerate(ring):
            point = positions[vertex_id]
            left_vec = positions[ring[index - 1]] - point
            right_vec = positions[ring[(index + 1) % len(ring)]] - point
            left_len_sq = float(np.dot(left_vec, left_vec))
            right_len_sq = float(np.dot(right_vec, right_vec))
            if left_len_sq <= _EPSILON or right_len_sq <= _EPSILON:
                continue
            cos = float(np.dot(left_vec, right_vec)) / math.sqrt(
                left_len_sq * right_len_sq
            )
            total += cos * cos
    return float(total)


def _edge_length_smoothness_energy(
    paths: tuple[tuple[int, ...], ...],
    positions: dict[int, np.ndarray],
) -> float:
    total = 0.0
    for path in paths:
        lengths = [
            float(np.linalg.norm(positions[node] - positions[prev_node]))
            for prev_node, node in zip(path, path[1:], strict=False)
        ]
        if len(lengths) < 2:
            continue
        mean_length = float(np.mean(lengths))
        if mean_length <= _EPSILON:
            continue
        total += float(
            sum((length - mean_length) ** 2 for length in lengths)
            / (mean_length * mean_length)
        )
    return float(total)


def _path_smoothness(
    paths: tuple[tuple[int, ...], ...], positions: dict[int, np.ndarray]
) -> float:
    total = 0.0
    for path in paths:
        for prev_node, node, next_node in zip(path, path[1:], path[2:], strict=False):
            second_difference = (
                positions[prev_node] - 2.0 * positions[node] + positions[next_node]
            )
            total += float(np.dot(second_difference, second_difference))
    return total


def _junction_orthogonality(layout: ChenLayout) -> float:
    positions = _layout_positions(layout)
    streets_by_id = {street.street_id: street for street in layout.street_graph.streets}
    total = 0.0
    for junction, street_ids in sorted(layout.street_graph.junctions.items()):
        directions: list[np.ndarray] = []
        for street_id in sorted(street_ids):
            tangent = _street_tangent_at_junction(
                streets_by_id[street_id], junction, positions
            )
            if tangent is not None:
                directions.append(tangent)
        for left_index, left in enumerate(directions):
            for right in directions[left_index + 1 :]:
                dot = float(np.dot(left, right))
                total += dot * dot
    return total


def _street_tangent_at_junction(
    street: StreetPath, junction: int, positions: dict[int, np.ndarray]
) -> np.ndarray | None:
    try:
        index = street.nodes.index(junction)
    except ValueError:
        return None
    if len(street.nodes) < 2:
        return None
    if index == 0:
        vector = positions[street.nodes[1]] - positions[junction]
    elif index == len(street.nodes) - 1:
        vector = positions[street.nodes[-2]] - positions[junction]
    else:
        vector = positions[street.nodes[index + 1]] - positions[street.nodes[index - 1]]
    norm = float(np.linalg.norm(vector))
    if norm <= _EPSILON:
        return None
    return vector / norm


def _street_neighbor_at_junction(street: StreetPath, junction: int) -> int | None:
    try:
        index = street.nodes.index(junction)
    except ValueError:
        return None
    if len(street.nodes) < 2:
        return None
    if index == 0:
        return street.nodes[1]
    if index == len(street.nodes) - 1:
        return street.nodes[-2]
    left = street.nodes[index - 1]
    right = street.nodes[index + 1]
    return min(left, right)


def _layout_closeness(
    layout: ChenLayout, original_positions: dict[int, np.ndarray]
) -> float:
    return float(
        sum(
            np.dot(
                np.array(vertex.point, dtype=float) - original_positions[vertex_id],
                np.array(vertex.point, dtype=float) - original_positions[vertex_id],
            )
            for vertex_id, vertex in layout.mesh.vertices.items()
        )
    )


def _axis_alignment_plan(layout: ChenLayout, config: OptimizationConfig) -> _AxisPlan:
    horizontal_edges: set[EdgeKey] = set()
    vertical_edges: set[EdgeKey] = set()
    for edge in layout.mesh.edges:
        a, b = edge
        pa = layout.mesh.vertices[a].point
        pb = layout.mesh.vertices[b].point
        angle = abs(math.atan2(pb[1] - pa[1], pb[0] - pa[0]))
        folded = angle % math.pi
        if folded > math.pi * 0.5:
            folded = math.pi - folded
        horizontal_deviation = folded
        vertical_deviation = abs(math.pi * 0.5 - folded)
        if horizontal_deviation <= config.axis_angle_tolerance_rad:
            horizontal_edges.add(edge)
        elif vertical_deviation <= config.axis_angle_tolerance_rad:
            vertical_edges.add(edge)

    groups: list[_AxisGroup] = []
    fixed_ids = {
        vertex_id
        for vertex_id, vertex in layout.mesh.vertices.items()
        if vertex.on_boundary
    }
    groups.extend(_axis_groups(layout, horizontal_edges, 1, fixed_ids))
    groups.extend(_axis_groups(layout, vertical_edges, 0, fixed_ids))
    return _AxisPlan(tuple(groups))


def _axis_groups(
    layout: ChenLayout,
    edges: set[EdgeKey],
    coordinate_index: int,
    fixed_ids: set[int],
) -> list[_AxisGroup]:
    adjacency: dict[int, set[int]] = {}
    for a, b in edges:
        adjacency.setdefault(a, set()).add(b)
        adjacency.setdefault(b, set()).add(a)

    groups: list[_AxisGroup] = []
    seen: set[int] = set()
    for start in sorted(adjacency):
        if start in seen:
            continue
        stack = [start]
        component: set[int] = set()
        seen.add(start)
        while stack:
            node = stack.pop()
            component.add(node)
            for nxt in sorted(adjacency.get(node, set())):
                if nxt in seen:
                    continue
                seen.add(nxt)
                stack.append(nxt)
        if len(component) < 2:
            continue
        target_ids = sorted(component & fixed_ids) or sorted(component)
        values = [
            layout.mesh.vertices[vertex_id].point[coordinate_index]
            for vertex_id in target_ids
        ]
        groups.append(
            _AxisGroup(
                coordinate_index=coordinate_index,
                target=float(np.median(values)),
                vertices=frozenset(component),
            )
        )
    return groups


def _axis_deviation(layout: ChenLayout) -> float:
    lengths_and_deviations = _edge_axis_deviations(layout)
    total_length = sum(length for length, _deviation in lengths_and_deviations)
    if total_length <= _EPSILON:
        return 0.0
    return float(
        sum(
            length * deviation * deviation
            for length, deviation in lengths_and_deviations
        )
        / total_length
    )


def _edge_axis_deviations(layout: ChenLayout) -> list[tuple[float, float]]:
    deviations: list[tuple[float, float]] = []
    for edge in sorted(layout.mesh.edges):
        a, b = edge
        pa = layout.mesh.vertices[a].point
        pb = layout.mesh.vertices[b].point
        dx = pb[0] - pa[0]
        dy = pb[1] - pa[1]
        length = math.hypot(dx, dy)
        if length <= _EPSILON:
            continue
        angle = abs(math.atan2(dy, dx)) % math.pi
        if angle > math.pi * 0.5:
            angle = math.pi - angle
        deviations.append((length, min(angle, abs(math.pi * 0.5 - angle))))
    return deviations


def _topology_signature(layout: ChenLayout) -> tuple[Any, ...]:
    return (
        tuple(sorted(layout.mesh.vertices)),
        tuple(
            (parcel_id, parcel.ring)
            for parcel_id, parcel in sorted(layout.mesh.parcels.items())
        ),
        tuple(sorted(layout.corner_graph.vertices)),
        tuple(sorted(layout.corner_graph.edges)),
        tuple(
            (edge, path)
            for edge, path in sorted(layout.corner_graph.edge_paths.items())
        ),
        tuple(
            (parcel_id, ring)
            for parcel_id, ring in sorted(
                layout.corner_graph.parcel_corner_rings.items()
            )
        ),
        tuple(
            (parcel_id, tuple(sorted(neighbors)))
            for parcel_id, neighbors in sorted(layout.parcel_graph.neighbors.items())
        ),
        tuple(
            (parcel_ids, tuple(sorted(edges)))
            for parcel_ids, edges in sorted(layout.parcel_graph.shared_edges.items())
        ),
        tuple(sorted(layout.street_network.edges)),
    )


def _projection_diagnostics(
    layout: ChenLayout,
    positions: dict[int, np.ndarray],
    original_positions: dict[int, np.ndarray],
    area_targets: dict[int, float],
    boundary_constraints: _BoundaryConstraints,
    axis_plan: _AxisPlan,
    config: OptimizationConfig,
) -> dict[str, Any]:
    if config.projection_mode != "shapeop_like_projection":
        return {
            "total_residual": 0.0,
            "equation_count": 0,
            "residuals_by_term": {},
            "equation_counts_by_term": {},
            "regularity_projection_kind": "disabled",
            "regularity_projected_parcel_count": 0,
            "regularity_skipped_parcel_count": 0,
            "regularity_skipped_by_reason": {},
            "regularity_projection_equation_count": 0,
            "regularity_projection_residual": 0.0,
            "regularity_projection_target_displacement_rms": 0.0,
            "regularity_projection_target_displacement_max": 0.0,
        }
    equations = _projection_equations(
        layout,
        positions,
        original_positions,
        area_targets,
        boundary_constraints,
        axis_plan,
        config,
        include_solver_anchor=False,
    )
    residuals_by_term: dict[str, float] = {}
    counts_by_term: dict[str, int] = {}
    for equation in equations:
        if equation.weight <= 0.0:
            continue
        residual = (
            sum(
                coefficient * float(positions[vertex_id][coordinate_index])
                for vertex_id, coordinate_index, coefficient in equation.coeffs
            )
            - equation.target
        )
        residuals_by_term[equation.term] = residuals_by_term.get(equation.term, 0.0) + (
            equation.weight * residual * residual
        )
        counts_by_term[equation.term] = counts_by_term.get(equation.term, 0) + 1
    regularity_summary = _regularity_projection_summary(
        layout,
        positions,
        config.weights.regularity,
    )
    return {
        "total_residual": float(sum(residuals_by_term.values())),
        "equation_count": int(sum(counts_by_term.values())),
        "residuals_by_term": residuals_by_term,
        "equation_counts_by_term": counts_by_term,
        "regularity_projection_kind": regularity_summary.kind,
        "regularity_projected_parcel_count": int(
            regularity_summary.projected_parcel_count
        ),
        "regularity_skipped_parcel_count": int(regularity_summary.skipped_parcel_count),
        "regularity_skipped_by_reason": dict(regularity_summary.skipped_by_reason),
        "regularity_projection_equation_count": int(regularity_summary.equation_count),
        "regularity_projection_residual": float(regularity_summary.residual),
        "regularity_projection_target_displacement_rms": float(
            regularity_summary.target_displacement_rms
        ),
        "regularity_projection_target_displacement_max": float(
            regularity_summary.target_displacement_max
        ),
    }


def _flatten_projection_metrics(
    before: dict[str, Any],
    after: dict[str, Any],
) -> dict[str, Any]:
    terms = (
        "regularity",
        "side_smoothness",
        "side_path_projection",
        "street_smoothness",
        "street_path_projection",
        "junction_orthogonality",
        "area_regularization",
        "rectangularity",
        "edge_length_smoothness",
        "axis_alignment",
        "boundary_shape_constraint",
        "closeness",
    )
    before_residuals = before["residuals_by_term"]
    after_residuals = after["residuals_by_term"]
    before_counts = before["equation_counts_by_term"]
    after_counts = after["equation_counts_by_term"]
    metrics: dict[str, Any] = {
        "projection_residual_before": float(before["total_residual"]),
        "projection_residual_after": float(after["total_residual"]),
        "projection_equation_count_before": int(before["equation_count"]),
        "projection_equation_count_after": int(after["equation_count"]),
        "regularity_projection_kind": str(
            before.get(
                "regularity_projection_kind",
                after.get("regularity_projection_kind", "disabled"),
            )
        ),
        "regularity_projected_parcel_count_before": int(
            before.get("regularity_projected_parcel_count", 0)
        ),
        "regularity_projected_parcel_count_after": int(
            after.get("regularity_projected_parcel_count", 0)
        ),
        "regularity_skipped_parcel_count_before": int(
            before.get("regularity_skipped_parcel_count", 0)
        ),
        "regularity_skipped_parcel_count_after": int(
            after.get("regularity_skipped_parcel_count", 0)
        ),
        "regularity_skipped_by_reason_before": dict(
            before.get("regularity_skipped_by_reason", {})
        ),
        "regularity_skipped_by_reason_after": dict(
            after.get("regularity_skipped_by_reason", {})
        ),
        "regularity_projection_equation_count": int(before_counts.get("regularity", 0)),
        "regularity_projection_residual_before": float(
            before.get(
                "regularity_projection_residual",
                before_residuals.get("regularity", 0.0),
            )
        ),
        "regularity_projection_residual_after": float(
            after.get(
                "regularity_projection_residual",
                after_residuals.get("regularity", 0.0),
            )
        ),
        "regularity_projection_target_displacement_rms_before": float(
            before.get("regularity_projection_target_displacement_rms", 0.0)
        ),
        "regularity_projection_target_displacement_rms_after": float(
            after.get("regularity_projection_target_displacement_rms", 0.0)
        ),
        "regularity_projection_target_displacement_max_before": float(
            before.get("regularity_projection_target_displacement_max", 0.0)
        ),
        "regularity_projection_target_displacement_max_after": float(
            after.get("regularity_projection_target_displacement_max", 0.0)
        ),
    }
    for term in terms:
        metrics[f"projection_{term}_residual_before"] = float(
            before_residuals.get(term, 0.0)
        )
        metrics[f"projection_{term}_residual_after"] = float(
            after_residuals.get(term, 0.0)
        )
        metrics[f"projection_{term}_equation_count_before"] = int(
            before_counts.get(term, 0)
        )
        metrics[f"projection_{term}_equation_count_after"] = int(
            after_counts.get(term, 0)
        )
    return metrics


def _result_metrics(
    before_layout: ChenLayout,
    after_layout: ChenLayout,
    boundary: Polygon,
    original_positions: dict[int, np.ndarray],
    before_components: dict[str, float],
    after_components: dict[str, float],
    before_energy: float,
    after_energy: float,
    before_report: dict[str, Any],
    after_report: dict[str, Any],
    *,
    accepted_iterations: int,
    converged: bool,
    convergence_reason: str,
    axis_active: bool,
    optimizer_kind: str,
    active_constraints: dict[str, Any],
    proposal_stats: _ProposalStats,
    area_target_mode: str,
    projection_before: dict[str, Any],
    projection_after: dict[str, Any],
) -> dict[str, Any]:
    del boundary
    after_positions = _layout_positions(after_layout)
    max_displacement = max(
        (
            float(np.linalg.norm(after_positions[vertex_id] - original))
            for vertex_id, original in original_positions.items()
        ),
        default=0.0,
    )
    active_constraint_count = sum(
        1 for value in active_constraints.values() if isinstance(value, bool) and value
    )
    rejected_proposals = {
        reason: int(count)
        for reason, count in sorted(proposal_stats.rejections.items())
    }
    energy_terms_before = {
        name: float(value) for name, value in sorted(before_components.items())
    }
    energy_terms_after = {
        name: float(value) for name, value in sorted(after_components.items())
    }
    energy_term_delta = {
        name: energy_terms_after[name] - energy_terms_before[name]
        for name in energy_terms_before
    }
    metrics = {
        "optimizer_kind": optimizer_kind,
        "solver_mode": optimizer_kind,
        "active_constraints": dict(sorted(active_constraints.items())),
        "active_constraint_count": int(active_constraint_count),
        "rejected_proposals": rejected_proposals,
        "rejected_proposal_count": int(sum(rejected_proposals.values())),
        "energy_terms_before": energy_terms_before,
        "energy_terms_after": energy_terms_after,
        "energy_term_delta": energy_term_delta,
        "energy_before": float(before_energy),
        "energy_after": float(after_energy),
        "irregularity_before": before_components["irregularity"],
        "irregularity_after": after_components["irregularity"],
        "side_smoothness_before": before_components["side_smoothness"],
        "side_smoothness_after": after_components["side_smoothness"],
        "street_smoothness_before": before_components["street_smoothness"],
        "street_smoothness_after": after_components["street_smoothness"],
        "junction_orthogonality_before": before_components["junction_orthogonality"],
        "junction_orthogonality_after": after_components["junction_orthogonality"],
        "layout_closeness_before": before_components["closeness"],
        "layout_closeness_after": after_components["closeness"],
        "axis_alignment_before": before_components["axis_alignment"],
        "axis_alignment_after": after_components["axis_alignment"],
        "area_regularization_before": before_components["area_regularization"],
        "area_regularization_after": after_components["area_regularization"],
        "rectangularity_before": before_components["rectangularity"],
        "rectangularity_after": after_components["rectangularity"],
        "edge_length_smoothness_before": before_components["edge_length_smoothness"],
        "edge_length_smoothness_after": after_components["edge_length_smoothness"],
        "axis_deviation_before": _axis_deviation(before_layout),
        "axis_deviation_after": _axis_deviation(after_layout),
        "near_axis_rectangular": bool(axis_active),
        "accepted_iteration_count": int(accepted_iterations),
        "converged": bool(converged),
        "convergence_reason": convergence_reason,
        "proposal_attempt_count": int(proposal_stats.attempts),
        "proposal_last_max_raw_step": float(proposal_stats.last_max_raw_step),
        "proposal_last_accepted_step": float(proposal_stats.last_accepted_step),
        "convergence": {
            "converged": bool(converged),
            "reason": convergence_reason,
            "accepted_iteration_count": int(accepted_iterations),
            "proposal_attempt_count": int(proposal_stats.attempts),
            "last_max_raw_step": float(proposal_stats.last_max_raw_step),
            "last_accepted_step": float(proposal_stats.last_accepted_step),
            "energy_delta": float(after_energy - before_energy),
        },
        "area_target_mode": area_target_mode,
        "boundary_constraint_mode": active_constraints["boundary_constraint_mode"],
        "boundary_fixed_vertex_count": int(
            active_constraints["boundary_fixed_vertex_count"]
        ),
        "boundary_sliding_vertex_count": int(
            active_constraints["boundary_sliding_vertex_count"]
        ),
        "max_vertex_displacement": float(max_displacement),
        "coverage_before": float(before_report.get("coverage_rate", 0.0)),
        "coverage_after": float(after_report.get("coverage_rate", 0.0)),
        "coverage_gap_after": float(after_report.get("coverage_gap_rate", 0.0)),
        "spillover_before": float(before_report.get("coverage_spillover_rate", 0.0)),
        "spillover_after": float(after_report.get("coverage_spillover_rate", 0.0)),
        "parcel_overlap_count_after": int(after_report["parcel_overlap_count"]),
        "corner_edge_count_before": int(len(before_layout.corner_graph.edges)),
        "corner_edge_count_after": int(len(after_layout.corner_graph.edges)),
        "street_edge_count_before": int(len(before_layout.street_network.edges)),
        "street_edge_count_after": int(len(after_layout.street_network.edges)),
        "parcel_count_before": int(len(before_layout.mesh.parcels)),
        "parcel_count_after": int(len(after_layout.mesh.parcels)),
    }
    for name, value in sorted(active_constraints.items()):
        metrics[f"active_constraint_{name}"] = value
    for reason, count in sorted(proposal_stats.rejections.items()):
        metrics[f"proposal_rejection_{reason}_count"] = int(count)
    metrics.update(_flatten_projection_metrics(projection_before, projection_after))
    return metrics
