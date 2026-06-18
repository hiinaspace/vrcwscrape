"""Yang/Chen cross-field streamline candidates for Chen splitting.

The functions here intentionally stop before Chen Eq. 2 scoring. They provide
deterministic candidate curves and diagnostics so the generator can score and
select split results in a separate step.

R1 regional extension (not Chen/Yang paper machinery): an optional
``RasterGuidanceField`` can be blended into the default ``grid_smooth`` field as
a weighted 4-RoSy alignment constraint (e.g. terrain-gradient / density-ridge
guidance for the regional probe). It is off by default; with ``guidance=None``
the field is byte-identical to the strict implementation. Boundary alignment
still dominates inside the boundary radius. The opt-in ``yang_*`` field modes
ignore guidance and record a ``guidance_ignored_*`` diagnostic.
"""

from __future__ import annotations

import hashlib
import logging
import math
from dataclasses import dataclass, replace
from typing import Any

import numpy as np
from scipy.spatial import Delaunay, QhullError
from shapely import LineString, Point, Polygon, contains_xy

PointLike = tuple[float, float] | Point
XY = tuple[float, float]

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class StreamlineConfig:
    step_fraction: float = 0.035
    min_step: float = 0.60
    max_step: float = 5.20
    max_steps: int = 160
    boundary_blend_radius_fraction: float = 0.42
    max_boundary_weight: float = 0.88
    min_length_fraction: float = 0.18
    duplicate_distance_fraction: float = 0.045
    duplicate_angle_tolerance: float = math.radians(14.0)
    endpoint_tolerance_fraction: float = 0.018
    seed_grid_side_min: int = 5
    seed_grid_side_max: int = 8
    candidate_seed_mode: str = "grid"
    parcel_length: float | None = None
    streamline_score_mode: str = "heuristic"
    template_widths: tuple[float, ...] = ()
    field_mode: str = "grid_smooth"
    field_grid_side: int = 33
    field_boundary_radius_fraction: float = 0.18
    field_boundary_weight: float = 8.0
    field_interior_weight: float = 0.06
    field_solver_iterations: int = 180
    field_solver_tolerance: float = 1e-5
    field_boundary_piece_weights: tuple[float, ...] = ()
    b_field_boundary_alignment_weight: float = 0.9
    trace_budget_factor: float = 3.0
    trace_budget_min: int = 44
    trace_budget_max: int = 64


DEFAULT_STREAMLINE_CONFIG = StreamlineConfig()


@dataclass(frozen=True, eq=False)
class RasterGuidanceField:
    """Optional external 4-RoSy alignment guidance for ``grid_smooth`` fields.

    R1 regional extension, **not** Chen/Yang paper machinery. When supplied to
    ``generate_layout_for_boundary`` / the cross-field builders, the guidance is
    blended into the default ``grid_smooth`` solve as an extra weighted unit
    vector in the doubled/quadrupled ``(cos 4t, sin 4t)`` space, analogous to how
    boundary constraints enter. Boundary alignment still dominates within the
    boundary radius.

    ``angle`` is a preferred street direction in radians (4-RoSy: only its value
    mod ``pi/2`` matters). ``weight`` is a non-negative constraint strength; 0 (or
    sampling outside the raster) means no influence. The cell-center transform is
    ``x = x0 + col*cell``, ``y = y0 + row*cell``. ``eq=False`` keeps the frozen
    dataclass from trying to compare/hash its numpy arrays.
    """

    angle: np.ndarray  # (H, W) preferred street direction, radians
    weight: np.ndarray  # (H, W) >= 0 constraint strength; 0 = no influence
    x0: float
    y0: float
    cell: float

    def digest(self) -> bytes:
        """Content digest for cache keying (arrays are not hashable)."""
        hasher = hashlib.sha1()
        angle = np.ascontiguousarray(self.angle, dtype=float)
        weight = np.ascontiguousarray(self.weight, dtype=float)
        hasher.update(repr(angle.shape).encode("ascii"))
        hasher.update(angle.tobytes())
        hasher.update(weight.tobytes())
        hasher.update(repr((float(self.x0), float(self.y0), float(self.cell))).encode())
        return hasher.digest()


@dataclass(frozen=True, eq=False)
class RasterDensityField:
    """Optional density raster for the density-mass split/termination mode.

    R1/R2 regional extension, **not** Chen/Yang paper machinery. The Chen split
    criterion balances geometric *area*; supplying this field to
    ``generate_layout_for_boundary`` switches the size measure to integrated
    density *mass*, so termination chases a "max worlds per district" target and
    splits get denser where the population surface is denser (see
    ``docs/regional-2_5d-research.md``, R2).

    ``density`` is a non-negative ``(H, W)`` raster (e.g. the smoothed
    ``r1_inputs.compute_density`` output). The cell-center transform matches the
    raster builders: the center of cell ``(row, col)`` is at
    ``x = x0 + (col + 0.5) * cell``, ``y = y0 + (row + 0.5) * cell`` (row 0 =
    bottom). ``mass`` integrates density over a polygon by midpoint rule (sum of
    the density of cells whose center falls inside, times ``cell**2``), so its
    units are ``density * cell**2``; ``max_parcel_mass`` is interpreted in the
    same units. ``eq=False`` keeps the frozen dataclass from comparing/hashing
    its numpy array.
    """

    density: np.ndarray  # (H, W) >= 0 density surface
    x0: float
    y0: float
    cell: float

    def mass(self, polygon: Polygon) -> float:
        """Integrated density mass of ``polygon`` (midpoint rule, ``density*cell^2``).

        Restricts the work to the polygon's bbox sub-window of the raster, then
        sums the density of cells whose center falls inside the polygon (the same
        ``contains_xy`` test the raster builders use for the island mask). Returns
        ``0.0`` for an empty/degenerate polygon or one that misses the raster.
        """
        nrows, ncols = self.density.shape
        if nrows == 0 or ncols == 0 or self.cell <= 0.0:
            return 0.0
        minx, miny, maxx, maxy = polygon.bounds
        # Cell-center col index c satisfies x0 + (c + 0.5)*cell == center_x.
        col_lo = int(np.floor((minx - self.x0) / self.cell - 0.5))
        col_hi = int(np.ceil((maxx - self.x0) / self.cell - 0.5))
        row_lo = int(np.floor((miny - self.y0) / self.cell - 0.5))
        row_hi = int(np.ceil((maxy - self.y0) / self.cell - 0.5))
        col_lo = max(col_lo, 0)
        row_lo = max(row_lo, 0)
        col_hi = min(col_hi, ncols - 1)
        row_hi = min(row_hi, nrows - 1)
        if col_hi < col_lo or row_hi < row_lo:
            return 0.0

        cols = np.arange(col_lo, col_hi + 1)
        rows = np.arange(row_lo, row_hi + 1)
        cx = self.x0 + (cols + 0.5) * self.cell
        cy = self.y0 + (rows + 0.5) * self.cell
        gx, gy = np.meshgrid(cx, cy)
        inside = contains_xy(polygon, gx.ravel(), gy.ravel()).reshape(gx.shape)
        window = self.density[row_lo : row_hi + 1, col_lo : col_hi + 1]
        return float(window[inside].sum()) * self.cell * self.cell

    def digest(self) -> bytes:
        """Content digest for cache keying (the array is not hashable)."""
        hasher = hashlib.sha1()
        density = np.ascontiguousarray(self.density, dtype=float)
        hasher.update(repr(density.shape).encode("ascii"))
        hasher.update(density.tobytes())
        hasher.update(repr((float(self.x0), float(self.y0), float(self.cell))).encode())
        return hasher.digest()


@dataclass(frozen=True)
class StreamlineCandidate:
    line: LineString
    seed: XY
    orientation_index: int
    seed_angle: float
    length: float
    bend: float
    endpoint_distance_max: float
    min_existing_distance: float
    quality: float
    diagnostics: dict[str, Any]


@dataclass(frozen=True)
class _CandidateSeedPlan:
    points: tuple[XY, ...]
    diagnostics: dict[str, Any]
    suppression_distance: float


_FIELD_MODE_BOUNDARY_BLEND = "boundary_blend"
_FIELD_MODE_GRID_SMOOTH = "grid_smooth"
_FIELD_MODE_YANG_D_FIELD = "yang_d_field"
_FIELD_MODE_YANG_B_FIELD = "yang_b_field"
_CANDIDATE_SEED_MODE_GRID = "grid"
_CANDIDATE_SEED_MODE_YANG_MESH_VERTICES = "yang_mesh_vertices"
_STREAMLINE_SCORE_MODE_HEURISTIC = "heuristic"
_STREAMLINE_SCORE_MODE_YANG_DIV_DB_DS_CT = "yang_div_db_ds_ct"
_BOUNDARY_BLEND_SOLVER = "boundary_blend_v0"
_GRID_SMOOTH_SOLVER = "grid_smooth_4rosy_laplace_v1"
_YANG_D_FIELD_SOLVER = "yang_d_field_weighted_footpoint_v1"
_YANG_B_FIELD_SOLVER = "yang_b_field_boundary_laplacian_omega_v0"
_YANG_D_FIELD_MESH_KIND = "scipy_delaunay_clipped"
_YANG_B_FIELD_APPROXIMATION_SCOPE = (
    "yang2013_supp1_sec4_3_uniform_mesh_graph_laplacian_boundary_vertices"
)
_FIELD_CACHE_LIMIT = 32


@dataclass(frozen=True)
class _BoundaryBlendField:
    boundary: Polygon
    config: StreamlineConfig
    base_angle: float

    @property
    def diagnostics(self) -> dict[str, Any]:
        return {
            "field_mode": _FIELD_MODE_BOUNDARY_BLEND,
            "field_solver": _BOUNDARY_BLEND_SOLVER,
            "field_grid_shape": None,
            "field_solver_iterations": 0,
            "field_solver_residual": 0.0,
            "field_boundary_constraint_count": 0,
            "field_valid_node_count": 0,
        }

    def angle_at(self, point: XY) -> float:
        return _boundary_blend_angle(self.boundary, point, self.base_angle, self.config)


@dataclass(frozen=True)
class _GridSmoothField:
    boundary: Polygon
    config: StreamlineConfig
    xs: np.ndarray
    ys: np.ndarray
    vectors: np.ndarray
    valid: np.ndarray
    base_angle: float
    iteration_count: int
    residual: float
    boundary_constraint_count: int

    @property
    def diagnostics(self) -> dict[str, Any]:
        return {
            "field_mode": _FIELD_MODE_GRID_SMOOTH,
            "field_solver": _GRID_SMOOTH_SOLVER,
            "field_grid_shape": (int(len(self.ys)), int(len(self.xs))),
            "field_solver_iterations": int(self.iteration_count),
            "field_solver_residual": float(self.residual),
            "field_boundary_constraint_count": int(self.boundary_constraint_count),
            "field_valid_node_count": int(np.count_nonzero(self.valid)),
        }

    def angle_at(self, point: XY) -> float:
        return _sample_grid_angle(self, point)


@dataclass(frozen=True)
class _BoundaryPiece:
    index: int
    start: XY
    end: XY
    angle: float
    length: float
    weight: float


@dataclass(frozen=True)
class _FootpointChoice:
    angle: float
    weighted_distance: float
    boundary_piece_index: int
    uses_concave_tangent: bool


@dataclass(frozen=True)
class _YangDField:
    boundary: Polygon
    config: StreamlineConfig
    points: np.ndarray
    vectors: np.ndarray
    triangulation: Any
    retained_simplex_indices: frozenset[int]
    diagnostics_payload: dict[str, Any]
    base_angle: float

    @property
    def diagnostics(self) -> dict[str, Any]:
        return {
            "field_mode": _FIELD_MODE_YANG_D_FIELD,
            "field_solver": _YANG_D_FIELD_SOLVER,
            "field_grid_shape": None,
            "field_solver_iterations": self.diagnostics_payload[
                "field_smoothing_rounds"
            ],
            "field_solver_residual": 0.0,
            "field_boundary_constraint_count": self.diagnostics_payload[
                "field_weighted_footpoint_count"
            ],
            "field_valid_node_count": self.diagnostics_payload[
                "field_mesh_vertex_count"
            ],
            **self.diagnostics_payload,
        }

    def angle_at(self, point: XY) -> float:
        return _sample_yang_mesh_field_angle(self, point)


@dataclass(frozen=True)
class _YangBField:
    boundary: Polygon
    config: StreamlineConfig
    points: np.ndarray
    vectors: np.ndarray
    triangulation: Any
    retained_simplex_indices: frozenset[int]
    diagnostics_payload: dict[str, Any]
    base_angle: float

    @property
    def diagnostics(self) -> dict[str, Any]:
        return {
            "field_mode": _FIELD_MODE_YANG_B_FIELD,
            "field_solver": _YANG_B_FIELD_SOLVER,
            "field_grid_shape": None,
            "field_solver_iterations": self.diagnostics_payload[
                "field_b_solver_iterations"
            ],
            "field_solver_residual": self.diagnostics_payload[
                "field_b_solver_residual"
            ],
            "field_boundary_constraint_count": self.diagnostics_payload[
                "field_b_boundary_anchor_count"
            ],
            "field_valid_node_count": self.diagnostics_payload[
                "field_mesh_vertex_count"
            ],
            **self.diagnostics_payload,
        }

    def angle_at(self, point: XY) -> float:
        return _sample_yang_mesh_field_angle(self, point)


_CrossField = _BoundaryBlendField | _GridSmoothField | _YangDField | _YangBField
_YangMeshField = _YangDField | _YangBField
_FIELD_CACHE: dict[tuple[bytes, StreamlineConfig, str, bytes], _CrossField] = {}


def cross_field_angle(
    boundary: Polygon,
    point: PointLike,
    *,
    mode: str | None = None,
    config: StreamlineConfig = DEFAULT_STREAMLINE_CONFIG,
    guidance: RasterGuidanceField | None = None,
) -> float:
    """Return a deterministic 4-RoSy cross-field base orientation at ``point``.

    By default this uses a small grid-based harmonic solve over the 4-RoSy
    representation ``(cos(4t), sin(4t))``. Boundary nodes are constrained by the
    nearest boundary tangent and interior nodes are smoothed with a weak global
    domain-axis prior. ``mode="boundary_blend"`` keeps the previous direct
    approximation available for compatibility.

    ``guidance`` (R1 regional extension, off by default) blends an external
    ``RasterGuidanceField`` into the ``grid_smooth`` solve; other modes ignore it.
    """
    field = _build_cross_field(boundary, config=config, mode=mode, guidance=guidance)
    return field.angle_at(_xy(point))


def trace_streamline(
    boundary: Polygon,
    seed: XY,
    angle: float,
    config: StreamlineConfig = DEFAULT_STREAMLINE_CONFIG,
    *,
    guidance: RasterGuidanceField | None = None,
) -> LineString:
    """Trace a field streamline in both directions until it reaches boundary."""
    if not boundary.is_valid or boundary.is_empty or boundary.area <= 1e-12:
        raise ValueError("boundary must be a valid non-empty polygon")
    if not boundary.covers(Point(seed)):
        raise ValueError("streamline seed must be inside or on the boundary")

    field = _build_cross_field(boundary, config=config, guidance=guidance)
    return _trace_streamline_with_field(boundary, seed, angle, config, field)


def _trace_streamline_with_field(
    boundary: Polygon,
    seed: XY,
    angle: float,
    config: StreamlineConfig,
    field: _CrossField,
) -> LineString:
    step = _trace_step(boundary, config)
    backward = _trace_one_direction(
        boundary, seed, angle + math.pi, step, config, field
    )
    forward = _trace_one_direction(boundary, seed, angle, step, config, field)
    points = list(reversed(backward)) + [seed] + forward
    points = _dedupe_consecutive(points)
    if len(points) < 2:
        return LineString([seed, seed])
    return LineString(points)


def candidate_streamlines(
    boundary: Polygon,
    *,
    target_count: int = 20,
    seed: int = 0,
    existing_lines: tuple[LineString, ...] = (),
    config: StreamlineConfig = DEFAULT_STREAMLINE_CONFIG,
    guidance: RasterGuidanceField | None = None,
) -> tuple[StreamlineCandidate, ...]:
    """Return deterministic Yang-style streamline candidates for ``boundary``.

    ``guidance`` (R1 regional extension, off by default) blends an external
    ``RasterGuidanceField`` into the ``grid_smooth`` field used for tracing.
    """
    if target_count <= 0:
        return ()
    if not boundary.is_valid or boundary.is_empty or boundary.area <= 1e-12:
        raise ValueError("boundary must be a valid non-empty polygon")

    scale = _domain_scale(boundary)
    score_mode = _streamline_score_mode(config)
    template_widths = _validated_template_widths(config)
    endpoint_tolerance = max(
        config.endpoint_tolerance_fraction * scale, _trace_step(boundary, config) * 1.5
    )
    min_length = config.min_length_fraction * scale
    duplicate_distance = config.duplicate_distance_fraction * scale
    field = _build_cross_field(boundary, config=config, guidance=guidance)
    field_diagnostics = field.diagnostics
    seed_plan = _candidate_seed_plan(
        boundary,
        field=field,
        target_count=target_count,
        seed=seed,
        config=config,
    )
    seed_points = seed_plan.points
    trace_total_option_count = len(seed_points) * 2
    trace_budget = _trace_attempt_budget(
        target_count=target_count,
        total_option_count=trace_total_option_count,
        config=config,
    )

    candidates: list[StreamlineCandidate] = []
    seen_keys: set[tuple[tuple[int, int], ...]] = set()
    trace_attempt_count = 0
    trace_accept_count = 0
    trace_active_orientation_suppression_count = 0
    orientation_attempt_counts = [0, 0]
    attempted_seed_indices: set[int] = set()
    active_options = [[True, True] for _ in seed_points]
    for seed_index, seed_xy in enumerate(seed_points):
        if trace_attempt_count >= trace_budget:
            break
        base_angle = field.angle_at(seed_xy)
        for orientation_index, seed_angle in enumerate(
            (base_angle, base_angle + math.pi * 0.5)
        ):
            if trace_attempt_count >= trace_budget:
                break
            if not active_options[seed_index][orientation_index]:
                continue
            active_options[seed_index][orientation_index] = False
            trace_attempt_count += 1
            orientation_attempt_counts[orientation_index] += 1
            attempted_seed_indices.add(seed_index)
            line = _trace_streamline_with_field(
                boundary, seed_xy, seed_angle, config, field
            )
            if line.length < min_length or not line.is_simple:
                continue

            endpoint_distance_max = _endpoint_distance_max(boundary, line)
            if endpoint_distance_max > endpoint_tolerance:
                continue

            line_key = _line_key(line)
            reverse_key = tuple(reversed(line_key))
            if line_key in seen_keys or reverse_key in seen_keys:
                continue

            mid_angle = _midpoint_angle(line)
            if _is_suppressed(
                line,
                mid_angle,
                (candidate.line for candidate in candidates),
                duplicate_distance=duplicate_distance,
                angle_tolerance=config.duplicate_angle_tolerance,
            ):
                continue

            existing_distance = _min_distance(line, existing_lines)
            if _is_suppressed(
                line,
                mid_angle,
                existing_lines,
                duplicate_distance=duplicate_distance,
                angle_tolerance=config.duplicate_angle_tolerance,
            ):
                continue

            bend = _line_bend(line)
            heuristic_quality = _candidate_quality(
                line,
                bend=bend,
                endpoint_distance_max=endpoint_distance_max,
                scale=scale,
                min_existing_distance=existing_distance,
            )
            score_diagnostics = _candidate_score_diagnostics(
                boundary=boundary,
                field=field,
                line=line,
                existing_lines=existing_lines,
                config=config,
                scale=scale,
                score_mode=score_mode,
                template_widths=template_widths,
            )
            candidates.append(
                StreamlineCandidate(
                    line=line,
                    seed=seed_xy,
                    orientation_index=orientation_index,
                    seed_angle=_normalize_pi(seed_angle),
                    length=float(line.length),
                    bend=float(bend),
                    endpoint_distance_max=float(endpoint_distance_max),
                    min_existing_distance=float(existing_distance),
                    quality=float(heuristic_quality),
                    diagnostics={
                        **field_diagnostics,
                        **seed_plan.diagnostics,
                        **score_diagnostics,
                        "point_count": len(line.coords),
                        "midpoint_angle": _normalize_pi(mid_angle),
                        "endpoint_distance_max": float(endpoint_distance_max),
                        "bend": float(bend),
                        "is_simple": bool(line.is_simple),
                        "trace_attempt_index": int(trace_attempt_count),
                        "trace_seed_index": int(seed_index),
                    },
                )
            )
            trace_accept_count += 1
            seen_keys.add(line_key)
            if config.candidate_seed_mode == _CANDIDATE_SEED_MODE_YANG_MESH_VERTICES:
                trace_active_orientation_suppression_count += (
                    _suppress_yang_seed_orientation_options(
                        seed_points=seed_points,
                        active_options=active_options,
                        line=line,
                        orientation_index=orientation_index,
                        suppression_distance=seed_plan.suppression_distance,
                    )
                )

    if score_mode == _STREAMLINE_SCORE_MODE_YANG_DIV_DB_DS_CT:
        candidates = _apply_yang_normalized_scores(candidates)

    candidates.sort(
        key=lambda candidate: (
            -candidate.quality,
            -candidate.length,
            candidate.orientation_index,
            round(candidate.seed[0], 9),
            round(candidate.seed[1], 9),
        )
    )
    returned = tuple(candidates[:target_count])
    active_option_remaining_count = sum(
        int(option_active)
        for seed_options in active_options
        for option_active in seed_options
    )
    if config.candidate_seed_mode == _CANDIDATE_SEED_MODE_GRID:
        trace_budget_exhausted = trace_attempt_count < trace_total_option_count
        trace_seed_count = int(math.ceil(trace_attempt_count / 2.0))
    else:
        trace_budget_exhausted = (
            trace_attempt_count >= trace_budget and active_option_remaining_count > 0
        )
        trace_seed_count = len(attempted_seed_indices)
    trace_diagnostics = {
        **seed_plan.diagnostics,
        "trace_attempt_count": int(trace_attempt_count),
        "trace_accept_count": int(trace_accept_count),
        "trace_budget": int(trace_budget),
        "trace_budget_exhausted": bool(trace_budget_exhausted),
        "trace_total_option_count": int(trace_total_option_count),
        "trace_active_option_total_count": int(trace_total_option_count),
        "trace_active_option_remaining_count": int(active_option_remaining_count),
        "trace_active_orientation_suppression_count": int(
            trace_active_orientation_suppression_count
        ),
        "trace_orientation_0_attempt_count": int(orientation_attempt_counts[0]),
        "trace_orientation_1_attempt_count": int(orientation_attempt_counts[1]),
        "trace_seed_count": int(trace_seed_count),
        "trace_seed_total_count": int(len(seed_points)),
        "trace_return_count": int(len(returned)),
    }
    return tuple(
        _with_call_diagnostics(candidate, trace_diagnostics, return_rank=index)
        for index, candidate in enumerate(returned)
    )


def _streamline_score_mode(config: StreamlineConfig) -> str:
    if config.streamline_score_mode in {
        _STREAMLINE_SCORE_MODE_HEURISTIC,
        _STREAMLINE_SCORE_MODE_YANG_DIV_DB_DS_CT,
    }:
        return config.streamline_score_mode
    raise ValueError(f"unknown streamline_score_mode: {config.streamline_score_mode}")


def _validated_template_widths(config: StreamlineConfig) -> tuple[float, ...]:
    widths: list[float] = []
    for width in config.template_widths:
        value = float(width)
        if value <= 0.0 or not math.isfinite(value):
            raise ValueError("template_widths must contain positive finite values")
        widths.append(value)
    return tuple(sorted(widths))


def _candidate_score_diagnostics(
    *,
    boundary: Polygon,
    field: _CrossField,
    line: LineString,
    existing_lines: tuple[LineString, ...],
    config: StreamlineConfig,
    scale: float,
    score_mode: str,
    template_widths: tuple[float, ...],
) -> dict[str, Any]:
    if score_mode == _STREAMLINE_SCORE_MODE_HEURISTIC:
        return {"score_mode": _STREAMLINE_SCORE_MODE_HEURISTIC}

    score_div, div_scope = _yang_divergence_score(field, line)
    score_db, db_scope = _yang_boundary_width_score(
        boundary, line, config=config, template_widths=template_widths
    )
    score_ds, ds_scope = _yang_singularity_distance_score(field, line, scale=scale)
    score_ct, ct_scope = _yang_continuity_score(line, existing_lines)
    return {
        "score_mode": _STREAMLINE_SCORE_MODE_YANG_DIV_DB_DS_CT,
        "score_div": float(score_div),
        "score_db": float(score_db),
        "score_ds": float(score_ds),
        "score_ct": float(score_ct),
        "score_approximation_scope": "; ".join(
            (div_scope, db_scope, ds_scope, ct_scope)
        ),
    }


def _apply_yang_normalized_scores(
    candidates: list[StreamlineCandidate],
) -> list[StreamlineCandidate]:
    if not candidates:
        return candidates

    score_keys = ("score_div", "score_db", "score_ds", "score_ct")
    component_quality: dict[str, list[float]] = {}
    for key in score_keys:
        values = [float(candidate.diagnostics[key]) for candidate in candidates]
        minimum = min(values)
        maximum = max(values)
        span = maximum - minimum
        if span <= 1e-12:
            component_quality[key] = [0.5 for _ in values]
            continue
        component_quality[key] = [1.0 - (value - minimum) / span for value in values]

    normalized: list[StreamlineCandidate] = []
    for index, candidate in enumerate(candidates):
        score_total = sum(component_quality[key][index] for key in score_keys) / len(
            score_keys
        )
        normalized.append(
            replace(
                candidate,
                quality=float(score_total),
                diagnostics={
                    **candidate.diagnostics,
                    "score_total_normalized": float(score_total),
                },
            )
        )
    return normalized


def _yang_divergence_score(field: _CrossField, line: LineString) -> tuple[float, str]:
    samples = _line_sample_points(line, count=max(5, min(33, len(line.coords))))
    if len(samples) < 2 or line.length <= 1e-12:
        return 1.0, "DIV=degenerate_line"

    previous_angle = _midpoint_angle(line)
    field_angles: list[float] = []
    for sample in samples:
        previous_angle = _active_field_angle(field, sample, previous_angle)
        field_angles.append(previous_angle)

    angle_change = sum(
        _angle_delta_2pi(left, right)
        for left, right in zip(field_angles, field_angles[1:], strict=False)
    )
    length_weight = 1.0 / max(line.length, 1e-9) ** 0.9
    return (
        float(angle_change * length_weight),
        "DIV=sampled_field_angle_change_proxy_length_power_0.9",
    )


def _yang_boundary_width_score(
    boundary: Polygon,
    line: LineString,
    *,
    config: StreamlineConfig,
    template_widths: tuple[float, ...],
) -> tuple[float, str]:
    samples = _line_sample_points(line, count=13, include_endpoints=False)
    if not samples:
        return 1.0, "DB=no_interior_samples"

    distances = [float(boundary.exterior.distance(Point(sample))) for sample in samples]
    max_distance = max(distances, default=0.0)
    if template_widths:
        targets = _template_width_targets(template_widths, max_distance=max_distance)
        normalizer = max(max(template_widths), 1e-9)
        scope = f"DB=boundary_distance_template_width_fit_{len(template_widths)}"
    else:
        parcel_length, parcel_length_source = _yang_candidate_parcel_length(
            boundary, config
        )
        steps = max(1, int(math.ceil(max_distance / max(parcel_length, 1e-9))) + 1)
        targets = tuple(float(parcel_length * index) for index in range(steps + 1))
        normalizer = max(parcel_length, 1e-9)
        scope = f"DB=boundary_distance_parcel_length_proxy_{parcel_length_source}"

    residuals = [
        min(abs(distance - target) for target in targets) / normalizer
        for distance in distances
    ]
    return float(sum(residuals) / len(residuals)), scope


def _template_width_targets(
    widths: tuple[float, ...], *, max_distance: float
) -> tuple[float, ...]:
    if not widths:
        return (0.0,)
    limit = max(0.0, float(max_distance)) + max(widths)
    targets: set[float] = {0.0}
    frontier: set[float] = {0.0}
    max_terms = max(1, int(math.ceil(limit / min(widths))) + 1)
    for _ in range(max_terms):
        next_frontier: set[float] = set()
        for current in frontier:
            for width in widths:
                value = current + width
                if value > limit + 1e-9:
                    continue
                rounded = round(value, 9)
                if rounded not in targets:
                    targets.add(rounded)
                    next_frontier.add(rounded)
        if not next_frontier or len(targets) >= 512:
            break
        frontier = next_frontier
    return tuple(sorted(targets))


def _yang_singularity_distance_score(
    field: _CrossField, line: LineString, *, scale: float
) -> tuple[float, str]:
    singularity_points = _field_singularity_points(field)
    if not singularity_points:
        return 1.0, "DS=no_field_singularity_candidates_available"
    distance = min(float(line.distance(Point(point))) for point in singularity_points)
    return (
        float(distance / max(scale, 1e-9)),
        f"DS=nearest_available_field_singularity_candidate_{len(singularity_points)}",
    )


def _field_singularity_points(field: _CrossField) -> tuple[XY, ...]:
    points: list[XY] = []
    if isinstance(field, (_YangDField, _YangBField)):
        retained_simplex_ids = sorted(field.retained_simplex_indices)
        for simplex_id in retained_simplex_ids:
            simplex = np.asarray(field.triangulation.simplices[simplex_id], dtype=int)
            vector = np.sum(field.vectors[simplex], axis=0)
            if float(np.linalg.norm(vector)) <= 0.12:
                centroid = np.mean(field.points[simplex], axis=0)
                points.append((float(centroid[0]), float(centroid[1])))
        for point, vector in zip(field.points, field.vectors, strict=False):
            if float(np.linalg.norm(vector)) <= 0.12:
                points.append((float(point[0]), float(point[1])))
    elif isinstance(field, _GridSmoothField):
        norms = np.linalg.norm(field.vectors, axis=2)
        for iy, ix in np.argwhere(field.valid & (norms <= 0.12)):
            points.append((float(field.xs[int(ix)]), float(field.ys[int(iy)])))

    unique: dict[tuple[int, int], XY] = {}
    for point in points:
        unique[(round(point[0] * 1_000_000), round(point[1] * 1_000_000))] = point
    return tuple(unique.values())


def _yang_continuity_score(
    line: LineString, existing_lines: tuple[LineString, ...]
) -> tuple[float, str]:
    existing_endpoints = [
        endpoint
        for existing_line in existing_lines
        for endpoint in _line_endpoint_directions(existing_line)
    ]
    if not existing_endpoints:
        return 1.0, "CT=no_existing_line_endpoints_available"

    continuous_count = 0
    for point, direction in _line_endpoint_directions(line):
        for existing_point, existing_direction in existing_endpoints:
            if (
                math.hypot(point[0] - existing_point[0], point[1] - existing_point[1])
                > 4.0
            ):
                continue
            if _angle_delta_pi(direction, existing_direction) <= math.radians(20.0):
                continuous_count += 1
                break

    score = {0: 1.0, 1: 0.5, 2: 0.0}.get(min(continuous_count, 2), 1.0)
    return (
        float(score),
        f"CT=endpoint_continuity_proxy_{continuous_count}_ddist4_ddir20deg",
    )


def _line_endpoint_directions(line: LineString) -> tuple[tuple[XY, float], ...]:
    coords = [(float(x), float(y)) for x, y in line.coords]
    if len(coords) < 2:
        return ()

    start, after_start = coords[0], coords[1]
    before_end, end = coords[-2], coords[-1]
    start_length = math.hypot(after_start[0] - start[0], after_start[1] - start[1])
    end_length = math.hypot(before_end[0] - end[0], before_end[1] - end[1])
    endpoints: list[tuple[XY, float]] = []
    if start_length > 1e-12:
        endpoints.append(
            (start, math.atan2(after_start[1] - start[1], after_start[0] - start[0]))
        )
    if end_length > 1e-12:
        endpoints.append(
            (end, math.atan2(before_end[1] - end[1], before_end[0] - end[0]))
        )
    return tuple(endpoints)


def _line_sample_points(
    line: LineString, *, count: int, include_endpoints: bool = True
) -> list[XY]:
    if line.length <= 1e-12:
        coords = list(line.coords)
        return [(float(coords[0][0]), float(coords[0][1]))] if coords else []

    sample_count = max(1, int(count))
    if include_endpoints:
        if sample_count == 1:
            distances = (line.length * 0.5,)
        else:
            distances = tuple(
                line.length * index / (sample_count - 1)
                for index in range(sample_count)
            )
    else:
        distances = tuple(
            line.length * (index + 1) / (sample_count + 1)
            for index in range(sample_count)
        )
    return [
        (float(point.x), float(point.y))
        for point in (line.interpolate(distance) for distance in distances)
    ]


def _xy(point: PointLike) -> XY:
    if isinstance(point, Point):
        return float(point.x), float(point.y)
    return float(point[0]), float(point[1])


def _domain_scale(boundary: Polygon) -> float:
    minx, miny, maxx, maxy = boundary.bounds
    return max(math.hypot(maxx - minx, maxy - miny), math.sqrt(boundary.area), 1e-9)


def _trace_step(boundary: Polygon, config: StreamlineConfig) -> float:
    return float(
        np.clip(
            _domain_scale(boundary) * config.step_fraction,
            config.min_step,
            config.max_step,
        )
    )


def _trace_attempt_budget(
    *, target_count: int, total_option_count: int, config: StreamlineConfig
) -> int:
    if target_count <= 0 or total_option_count <= 0:
        return 0
    factor_budget = int(
        math.ceil(max(0.0, float(config.trace_budget_factor)) * target_count)
    )
    min_budget = max(1, int(config.trace_budget_min))
    max_budget = max(min_budget, int(config.trace_budget_max))
    requested = min(max_budget, max(min_budget, factor_budget))
    return min(total_option_count, requested)


def _with_call_diagnostics(
    candidate: StreamlineCandidate,
    call_diagnostics: dict[str, Any],
    *,
    return_rank: int,
) -> StreamlineCandidate:
    return StreamlineCandidate(
        line=candidate.line,
        seed=candidate.seed,
        orientation_index=candidate.orientation_index,
        seed_angle=candidate.seed_angle,
        length=candidate.length,
        bend=candidate.bend,
        endpoint_distance_max=candidate.endpoint_distance_max,
        min_existing_distance=candidate.min_existing_distance,
        quality=candidate.quality,
        diagnostics={
            **candidate.diagnostics,
            **call_diagnostics,
            "trace_return_rank": int(return_rank),
        },
    )


def _normalize_pi(angle: float) -> float:
    out = math.fmod(angle, math.pi)
    if out < 0.0:
        out += math.pi
    return out


def _angle_delta_pi(a: float, b: float) -> float:
    return abs(0.5 * math.atan2(math.sin(2.0 * (a - b)), math.cos(2.0 * (a - b))))


def _angle_delta_2pi(a: float, b: float) -> float:
    return abs(math.atan2(math.sin(a - b), math.cos(a - b)))


def _blend_cross_periodic(a: float, b: float, t: float) -> float:
    t = float(np.clip(t, 0.0, 1.0))
    x = math.cos(4.0 * a) * (1.0 - t) + math.cos(4.0 * b) * t
    y = math.sin(4.0 * a) * (1.0 - t) + math.sin(4.0 * b) * t
    if abs(x) <= 1e-12 and abs(y) <= 1e-12:
        return a
    return _normalize_pi(0.25 * math.atan2(y, x))


def _cross_vector(angle: float) -> np.ndarray:
    return np.array([math.cos(4.0 * angle), math.sin(4.0 * angle)], dtype=float)


def _angle_from_cross_vector(vector: np.ndarray, fallback: float) -> float:
    x = float(vector[0])
    y = float(vector[1])
    if math.hypot(x, y) <= 1e-12:
        return _normalize_pi(fallback)
    return _normalize_pi(0.25 * math.atan2(y, x))


def _normalize_vector(vector: np.ndarray, fallback: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(vector))
    if norm <= 1e-12:
        return fallback.copy()
    return np.array([float(vector[0] / norm), float(vector[1] / norm)], dtype=float)


def _build_cross_field(
    boundary: Polygon,
    *,
    config: StreamlineConfig,
    mode: str | None = None,
    guidance: RasterGuidanceField | None = None,
) -> _CrossField:
    selected_mode = config.field_mode if mode is None else mode
    # The guidance digest keeps the cache key hashable (numpy arrays are not) and
    # distinct per guidance content. guidance=None ⇒ empty digest ⇒ the exact
    # pre-extension cache key, so default behaviour is byte-identical.
    guidance_digest = b"" if guidance is None else guidance.digest()
    cache_key = (bytes(boundary.wkb), config, selected_mode, guidance_digest)
    cached = _FIELD_CACHE.get(cache_key)
    if cached is not None:
        return cached

    base_angle = _principal_cross_axis(boundary)
    if selected_mode == _FIELD_MODE_BOUNDARY_BLEND:
        if guidance is not None:
            _LOGGER.warning(
                "guidance_ignored_for_field_mode: RasterGuidanceField is only "
                "blended into the grid_smooth field; mode=%s ignores it",
                selected_mode,
            )
        field: _CrossField = _BoundaryBlendField(boundary, config, base_angle)
    elif selected_mode == _FIELD_MODE_GRID_SMOOTH:
        field = _build_grid_smooth_field(
            boundary, config=config, base_angle=base_angle, guidance=guidance
        )
    elif selected_mode == _FIELD_MODE_YANG_D_FIELD:
        if guidance is not None:
            _LOGGER.warning(
                "guidance_ignored_for_field_mode: RasterGuidanceField is only "
                "blended into the grid_smooth field; mode=%s ignores it",
                selected_mode,
            )
        field = _build_yang_d_field(boundary, config=config, base_angle=base_angle)
    elif selected_mode == _FIELD_MODE_YANG_B_FIELD:
        if guidance is not None:
            _LOGGER.warning(
                "guidance_ignored_for_field_mode: RasterGuidanceField is only "
                "blended into the grid_smooth field; mode=%s ignores it",
                selected_mode,
            )
        field = _build_yang_b_field(boundary, config=config, base_angle=base_angle)
    else:
        raise ValueError(f"unknown cross-field mode: {selected_mode}")

    if len(_FIELD_CACHE) >= _FIELD_CACHE_LIMIT:
        _FIELD_CACHE.pop(next(iter(_FIELD_CACHE)))
    _FIELD_CACHE[cache_key] = field
    return field


def _boundary_blend_angle(
    boundary: Polygon,
    point: XY,
    base_angle: float,
    config: StreamlineConfig,
) -> float:
    tangent_angle, boundary_distance = _nearest_boundary_tangent(boundary, point)
    scale = _domain_scale(boundary)
    radius = max(scale * config.boundary_blend_radius_fraction, 1e-9)
    proximity = max(0.0, 1.0 - boundary_distance / radius)
    weight = min(config.max_boundary_weight, proximity * proximity)
    return _normalize_pi(_blend_cross_periodic(base_angle, tangent_angle, weight))


def _build_grid_smooth_field(
    boundary: Polygon,
    *,
    config: StreamlineConfig,
    base_angle: float,
    guidance: RasterGuidanceField | None = None,
) -> _CrossField:
    minx, miny, maxx, maxy = boundary.bounds
    width = max(maxx - minx, 1e-9)
    height = max(maxy - miny, 1e-9)
    largest = max(width, height)
    target_side = max(5, int(config.field_grid_side))
    nx = max(5, int(round((width / largest) * (target_side - 1))) + 1)
    ny = max(5, int(round((height / largest) * (target_side - 1))) + 1)
    xs = np.linspace(minx, maxx, nx)
    ys = np.linspace(miny, maxy, ny)
    valid = np.zeros((ny, nx), dtype=bool)
    vectors = np.zeros((ny, nx, 2), dtype=float)
    boundary_vectors = np.zeros((ny, nx, 2), dtype=float)
    boundary_weights = np.zeros((ny, nx), dtype=float)
    guidance_vectors = np.zeros((ny, nx, 2), dtype=float)
    guidance_weights = np.zeros((ny, nx), dtype=float)
    base_vector = _cross_vector(base_angle)
    boundary_constraint_count = 0
    field_boundary_weight = max(0.0, float(config.field_boundary_weight))

    interior_weight = max(0.0, float(config.field_interior_weight))
    for iy, y in enumerate(ys):
        for ix, x in enumerate(xs):
            point = (float(x), float(y))
            if not boundary.covers(Point(point)):
                continue
            valid[iy, ix] = True
            tangent_angle, boundary_distance = _nearest_boundary_tangent(
                boundary, point
            )
            boundary_vector = _cross_vector(tangent_angle)
            boundary_weight = _boundary_constraint_weight(
                boundary_distance, boundary, config
            )
            if boundary_weight > 1e-9:
                boundary_constraint_count += 1
            boundary_vectors[iy, ix] = boundary_vector
            boundary_weights[iy, ix] = boundary_weight
            if guidance is not None:
                guide_vector, guide_weight = _guidance_constraint(
                    guidance,
                    point,
                    boundary_weight=boundary_weight,
                    field_boundary_weight=field_boundary_weight,
                )
                guidance_vectors[iy, ix] = guide_vector
                guidance_weights[iy, ix] = guide_weight
            vectors[iy, ix] = _normalize_vector(
                boundary_vector * boundary_weight
                + guidance_vectors[iy, ix] * guidance_weights[iy, ix]
                + base_vector * interior_weight,
                base_vector,
            )

    if not np.any(valid):
        return _BoundaryBlendField(boundary, config, base_angle)

    residual = 0.0
    iteration_count = 0
    max_iterations = max(0, int(config.field_solver_iterations))
    tolerance = max(0.0, float(config.field_solver_tolerance))
    neighbor_offsets = ((-1, 0), (1, 0), (0, -1), (0, 1))
    for iteration in range(1, max_iterations + 1):
        iteration_count = iteration
        old = vectors
        new = old.copy()
        residual = 0.0
        for iy in range(ny):
            for ix in range(nx):
                if not valid[iy, ix]:
                    continue
                neighbor_sum = np.zeros(2, dtype=float)
                neighbor_count = 0
                for dy, dx in neighbor_offsets:
                    jy = iy + dy
                    jx = ix + dx
                    if 0 <= jy < ny and 0 <= jx < nx and valid[jy, jx]:
                        neighbor_sum += old[jy, jx]
                        neighbor_count += 1
                smooth_target = (
                    neighbor_sum / neighbor_count if neighbor_count > 0 else old[iy, ix]
                )
                target = (
                    smooth_target
                    + boundary_vectors[iy, ix] * boundary_weights[iy, ix]
                    + guidance_vectors[iy, ix] * guidance_weights[iy, ix]
                    + base_vector * interior_weight
                )
                normalized = _normalize_vector(target, old[iy, ix])
                new[iy, ix] = normalized
                residual = max(
                    residual, float(np.linalg.norm(normalized - old[iy, ix]))
                )
        vectors = new
        if residual <= tolerance:
            break

    return _GridSmoothField(
        boundary=boundary,
        config=config,
        xs=xs,
        ys=ys,
        vectors=vectors,
        valid=valid,
        base_angle=base_angle,
        iteration_count=iteration_count,
        residual=residual,
        boundary_constraint_count=boundary_constraint_count,
    )


def _guidance_constraint(
    guidance: RasterGuidanceField,
    point: XY,
    *,
    boundary_weight: float,
    field_boundary_weight: float,
) -> tuple[np.ndarray, float]:
    """Return the (4-RoSy unit vector, effective weight) for external guidance.

    R1 regional extension. The raster angle/weight are sampled bilinearly (0
    outside the raster). The returned weight is attenuated so boundary alignment
    still dominates within the boundary radius: it is capped below
    ``field_boundary_weight`` and faded out as the local boundary constraint
    grows. With ``boundary_weight >= field_boundary_weight`` (right at the
    boundary) the guidance is fully suppressed.
    """
    angle, raw_weight = _sample_guidance(guidance, point)
    if raw_weight <= 0.0:
        return np.zeros(2, dtype=float), 0.0
    # Keep guidance strictly weaker than the full boundary constraint so it can
    # never out-vote a boundary-aligned node.
    cap = 0.5 * field_boundary_weight if field_boundary_weight > 0.0 else raw_weight
    capped = min(float(raw_weight), cap) if cap > 0.0 else float(raw_weight)
    if field_boundary_weight > 0.0:
        fade = max(0.0, 1.0 - boundary_weight / field_boundary_weight)
    else:
        fade = 1.0
    effective = capped * fade
    if effective <= 0.0:
        return np.zeros(2, dtype=float), 0.0
    return _cross_vector(angle), effective


def _sample_guidance(guidance: RasterGuidanceField, point: XY) -> tuple[float, float]:
    """Bilinearly sample (angle, weight); weight 0 outside the raster.

    Cell-center transform: ``x = x0 + col*cell``, ``y = y0 + row*cell``. Angle is
    interpolated in the doubled-angle ``(cos 4t, sin 4t)`` space so the 4-RoSy
    periodicity is respected; weight is interpolated linearly. Nearest-cell is
    used at the raster edges.
    """
    angle_raster = guidance.angle
    weight_raster = guidance.weight
    rows, cols = angle_raster.shape
    if rows == 0 or cols == 0 or guidance.cell <= 0.0:
        return 0.0, 0.0
    px, py = point
    fc = (px - guidance.x0) / guidance.cell
    fr = (py - guidance.y0) / guidance.cell
    # Outside the raster footprint ⇒ no influence.
    if fc < -0.5 or fc > cols - 0.5 or fr < -0.5 or fr > rows - 0.5:
        return 0.0, 0.0
    fc = float(np.clip(fc, 0.0, cols - 1))
    fr = float(np.clip(fr, 0.0, rows - 1))
    c0 = min(max(int(math.floor(fc)), 0), cols - 1)
    r0 = min(max(int(math.floor(fr)), 0), rows - 1)
    c1 = min(c0 + 1, cols - 1)
    r1 = min(r0 + 1, rows - 1)
    tc = fc - c0
    tr = fr - r0
    corners = (
        (r0, c0, (1.0 - tc) * (1.0 - tr)),
        (r0, c1, tc * (1.0 - tr)),
        (r1, c0, (1.0 - tc) * tr),
        (r1, c1, tc * tr),
    )
    cross = np.zeros(2, dtype=float)
    weight_total = 0.0
    blend_norm = 0.0
    for r, c, blend in corners:
        if blend <= 0.0:
            continue
        cell_weight = float(weight_raster[r, c])
        weight_total += cell_weight * blend
        blend_norm += blend
        cross += _cross_vector(float(angle_raster[r, c])) * blend
    if blend_norm <= 0.0:
        return 0.0, 0.0
    sampled_weight = max(0.0, weight_total / blend_norm)
    # A near-zero cross vector means the interpolated angles cancel (ambiguous
    # direction); contribute no constraint rather than a spurious axis pull.
    if sampled_weight <= 0.0 or float(np.linalg.norm(cross)) <= 1e-12:
        return 0.0, 0.0
    angle = _angle_from_cross_vector(cross, 0.0)
    return angle, sampled_weight


def _boundary_constraint_weight(
    boundary_distance: float, boundary: Polygon, config: StreamlineConfig
) -> float:
    scale = _domain_scale(boundary)
    radius = max(scale * config.field_boundary_radius_fraction, 1e-9)
    proximity = max(0.0, 1.0 - boundary_distance / radius)
    return max(0.0, float(config.field_boundary_weight)) * proximity * proximity


def _build_yang_d_field(
    boundary: Polygon, *, config: StreamlineConfig, base_angle: float
) -> _CrossField:
    pieces = _weighted_boundary_pieces(boundary, config)
    if len(pieces) < 3:
        return _BoundaryBlendField(boundary, config, base_angle)

    points = _yang_mesh_points(boundary, pieces, config)
    if len(points) < 3:
        return _BoundaryBlendField(boundary, config, base_angle)

    try:
        triangulation = Delaunay(points)
    except QhullError:
        return _BoundaryBlendField(boundary, config, base_angle)

    retained_simplex_ids: list[int] = []
    for simplex_id, simplex in enumerate(triangulation.simplices):
        triangle = points[np.asarray(simplex, dtype=int)]
        centroid = np.mean(triangle, axis=0)
        if boundary.covers(Point(float(centroid[0]), float(centroid[1]))):
            retained_simplex_ids.append(int(simplex_id))

    if not retained_simplex_ids:
        return _BoundaryBlendField(boundary, config, base_angle)

    adjacency = _mesh_adjacency(
        vertex_count=len(points),
        simplices=triangulation.simplices[np.asarray(retained_simplex_ids, dtype=int)],
    )
    concave_vertices = _concave_vertex_indices(boundary)
    raw_vectors, footpoint_diagnostics = _yang_d_raw_vectors(
        points=points,
        boundary=boundary,
        pieces=pieces,
        concave_vertices=concave_vertices,
        fallback_angle=base_angle,
    )
    vectors, smooth_diagnostics = _smooth_yang_vectors(raw_vectors, adjacency)
    singularity_candidate_count = _singularity_candidate_count(
        vectors,
        triangulation.simplices[np.asarray(retained_simplex_ids, dtype=int)],
    )

    retained_vertices = {
        int(vertex)
        for simplex_id in retained_simplex_ids
        for vertex in triangulation.simplices[simplex_id]
    }
    diagnostics_payload = {
        "field_mesh_kind": _YANG_D_FIELD_MESH_KIND,
        "field_mesh_vertex_count": int(len(points)),
        "field_mesh_retained_vertex_count": int(len(retained_vertices)),
        "field_mesh_triangle_count": int(len(retained_simplex_ids)),
        "field_mesh_edge_count": int(sum(len(n) for n in adjacency) // 2),
        "field_mesh_isolated_vertex_count": int(
            sum(1 for neighbors in adjacency if not neighbors)
        ),
        "field_boundary_piece_count": int(len(pieces)),
        "field_boundary_piece_weight_min": float(min(piece.weight for piece in pieces)),
        "field_boundary_piece_weight_max": float(max(piece.weight for piece in pieces)),
        "field_concave_corner_count": int(len(concave_vertices)),
        "field_smoothing_rounds": 1,
        "field_near_zero_vector_count": int(
            smooth_diagnostics["near_zero_vector_count"]
        ),
        "field_singularity_candidate_count": int(singularity_candidate_count),
        **footpoint_diagnostics,
    }
    return _YangDField(
        boundary=boundary,
        config=config,
        points=points,
        vectors=vectors,
        triangulation=triangulation,
        retained_simplex_indices=frozenset(retained_simplex_ids),
        diagnostics_payload=diagnostics_payload,
        base_angle=base_angle,
    )


def _build_yang_b_field(
    boundary: Polygon, *, config: StreamlineConfig, base_angle: float
) -> _CrossField:
    pieces = _weighted_boundary_pieces(boundary, config)
    if len(pieces) < 3:
        return _BoundaryBlendField(boundary, config, base_angle)

    points = _yang_mesh_points(boundary, pieces, config)
    if len(points) < 3:
        return _BoundaryBlendField(boundary, config, base_angle)

    try:
        triangulation = Delaunay(points)
    except QhullError:
        return _BoundaryBlendField(boundary, config, base_angle)

    retained_simplex_ids: list[int] = []
    for simplex_id, simplex in enumerate(triangulation.simplices):
        triangle = points[np.asarray(simplex, dtype=int)]
        centroid = np.mean(triangle, axis=0)
        if boundary.covers(Point(float(centroid[0]), float(centroid[1]))):
            retained_simplex_ids.append(int(simplex_id))

    if not retained_simplex_ids:
        return _BoundaryBlendField(boundary, config, base_angle)

    retained_simplices = triangulation.simplices[
        np.asarray(retained_simplex_ids, dtype=int)
    ]
    adjacency = _mesh_adjacency(vertex_count=len(points), simplices=retained_simplices)
    boundary_anchors, anchor_diagnostics = _yang_b_boundary_anchors(
        points=points,
        boundary=boundary,
        pieces=pieces,
        base_angle=base_angle,
    )
    if not boundary_anchors:
        return _BoundaryBlendField(boundary, config, base_angle)

    vectors, solver_diagnostics = _solve_yang_b_vectors(
        vertex_count=len(points),
        adjacency=adjacency,
        boundary_anchors=boundary_anchors,
        config=config,
        base_angle=base_angle,
    )
    singularity_candidate_count = _singularity_candidate_count(
        vectors, retained_simplices
    )
    retained_vertices = {
        int(vertex)
        for simplex_id in retained_simplex_ids
        for vertex in triangulation.simplices[simplex_id]
    }
    diagnostics_payload = {
        "field_mesh_kind": _YANG_D_FIELD_MESH_KIND,
        "field_mesh_vertex_count": int(len(points)),
        "field_mesh_retained_vertex_count": int(len(retained_vertices)),
        "field_mesh_triangle_count": int(len(retained_simplex_ids)),
        "field_mesh_edge_count": int(sum(len(n) for n in adjacency) // 2),
        "field_mesh_isolated_vertex_count": int(
            sum(1 for neighbors in adjacency if not neighbors)
        ),
        "field_boundary_piece_count": int(len(pieces)),
        "field_boundary_piece_weight_min": float(min(piece.weight for piece in pieces)),
        "field_boundary_piece_weight_max": float(max(piece.weight for piece in pieces)),
        "field_singularity_candidate_count": int(singularity_candidate_count),
        "field_b_approximation_scope": _YANG_B_FIELD_APPROXIMATION_SCOPE,
        "field_b_boundary_anchor_method": (
            "boundary_samples_with_polygon_vertices_averaged_incident_edges"
        ),
        "field_b_boundary_piece_weight_scope": "ignored_by_sec4_3_b_field_energy",
        **anchor_diagnostics,
        **solver_diagnostics,
    }
    return _YangBField(
        boundary=boundary,
        config=config,
        points=points,
        vectors=vectors,
        triangulation=triangulation,
        retained_simplex_indices=frozenset(retained_simplex_ids),
        diagnostics_payload=diagnostics_payload,
        base_angle=base_angle,
    )


def _weighted_boundary_pieces(
    boundary: Polygon, config: StreamlineConfig
) -> tuple[_BoundaryPiece, ...]:
    segments = _boundary_segments(boundary)
    weights = config.field_boundary_piece_weights
    if weights and len(weights) != len(segments):
        raise ValueError(
            "field_boundary_piece_weights must be empty or match the exterior "
            f"boundary piece count ({len(segments)})"
        )

    pieces: list[_BoundaryPiece] = []
    for index, (start, end) in enumerate(segments):
        sx, sy = start
        ex, ey = end
        length = math.hypot(ex - sx, ey - sy)
        weight = 1.0 if not weights else float(weights[index])
        if weight <= 0.0 or not math.isfinite(weight):
            raise ValueError(
                "field_boundary_piece_weights must be positive finite values"
            )
        if length <= 1e-12:
            continue
        pieces.append(
            _BoundaryPiece(
                index=index,
                start=start,
                end=end,
                angle=math.atan2(ey - sy, ex - sx),
                length=length,
                weight=weight,
            )
        )
    return tuple(pieces)


def _yang_mesh_points(
    boundary: Polygon,
    pieces: tuple[_BoundaryPiece, ...],
    config: StreamlineConfig,
) -> np.ndarray:
    minx, miny, maxx, maxy = boundary.bounds
    width = max(maxx - minx, 1e-9)
    height = max(maxy - miny, 1e-9)
    largest = max(width, height)
    target_side = max(5, int(config.field_grid_side))
    nx = max(5, int(round((width / largest) * (target_side - 1))) + 1)
    ny = max(5, int(round((height / largest) * (target_side - 1))) + 1)
    spacing = largest / max(target_side - 1, 1)

    keyed: dict[tuple[int, int], XY] = {}

    def add(point: XY) -> None:
        keyed[(round(point[0] * 1_000_000), round(point[1] * 1_000_000))] = point

    for piece in pieces:
        steps = max(1, int(math.ceil(piece.length / max(spacing, 1e-9))))
        sx, sy = piece.start
        ex, ey = piece.end
        for step in range(steps):
            t = step / steps
            add((float(sx + (ex - sx) * t), float(sy + (ey - sy) * t)))
    for piece in pieces:
        add(piece.end)

    interior = boundary.buffer(-min(spacing * 0.08, 1e-6))
    for iy, y in enumerate(np.linspace(miny, maxy, ny)):
        for ix, x in enumerate(np.linspace(minx, maxx, nx)):
            if ix in (0, nx - 1) or iy in (0, ny - 1):
                continue
            point = (float(x), float(y))
            if interior.covers(Point(point)):
                add(point)

    centroid = (float(boundary.centroid.x), float(boundary.centroid.y))
    if boundary.covers(Point(centroid)):
        add(centroid)

    ordered = sorted(keyed.values(), key=lambda point: (point[0], point[1]))
    return np.asarray(ordered, dtype=float)


def _mesh_adjacency(
    *, vertex_count: int, simplices: np.ndarray
) -> tuple[tuple[int, ...], ...]:
    neighbors: list[set[int]] = [set() for _ in range(vertex_count)]
    for simplex in simplices:
        vertices = [int(vertex) for vertex in simplex]
        for index, left in enumerate(vertices):
            for right in vertices[index + 1 :]:
                neighbors[left].add(right)
                neighbors[right].add(left)
    return tuple(tuple(sorted(entry)) for entry in neighbors)


def _yang_b_boundary_anchors(
    *,
    points: np.ndarray,
    boundary: Polygon,
    pieces: tuple[_BoundaryPiece, ...],
    base_angle: float,
) -> tuple[dict[int, np.ndarray], dict[str, Any]]:
    scale = _domain_scale(boundary)
    tolerance = max(scale * 1e-8, 1e-7)
    ring_coords = [(float(x), float(y)) for x, y in boundary.exterior.coords[:-1]]
    vertex_anchor_count = 0
    segment_anchor_count = 0
    anchors: dict[int, np.ndarray] = {}

    for index, point in enumerate(points):
        point_xy = (float(point[0]), float(point[1]))
        if boundary.exterior.distance(Point(point_xy)) > tolerance:
            continue

        vertex_index = _matching_boundary_vertex_index(
            point_xy, ring_coords, tolerance=tolerance
        )
        if vertex_index is not None:
            anchor_angle = _boundary_vertex_average_angle(
                vertex_index=vertex_index,
                pieces=pieces,
                base_angle=base_angle,
            )
            vertex_anchor_count += 1
        else:
            anchor_angle = _nearest_boundary_piece_angle(point_xy, pieces)
            segment_anchor_count += 1
        anchors[index] = _cross_vector(anchor_angle)

    return anchors, {
        "field_b_boundary_anchor_count": int(len(anchors)),
        "field_b_boundary_vertex_anchor_count": int(vertex_anchor_count),
        "field_b_boundary_segment_anchor_count": int(segment_anchor_count),
        "field_b_boundary_anchor_fraction": float(len(anchors) / max(len(points), 1)),
    }


def _matching_boundary_vertex_index(
    point: XY, ring_coords: list[XY], *, tolerance: float
) -> int | None:
    px, py = point
    tolerance_squared = tolerance * tolerance
    for index, vertex in enumerate(ring_coords):
        dx = px - vertex[0]
        dy = py - vertex[1]
        if dx * dx + dy * dy <= tolerance_squared:
            return index
    return None


def _boundary_vertex_average_angle(
    *,
    vertex_index: int,
    pieces: tuple[_BoundaryPiece, ...],
    base_angle: float,
) -> float:
    if not pieces:
        return base_angle
    previous_piece = pieces[(vertex_index - 1) % len(pieces)]
    next_piece = pieces[vertex_index % len(pieces)]
    vector = np.array(
        [
            math.cos(previous_piece.angle) + math.cos(next_piece.angle),
            math.sin(previous_piece.angle) + math.sin(next_piece.angle),
        ],
        dtype=float,
    )
    if float(np.linalg.norm(vector)) <= 1e-12:
        return base_angle
    return math.atan2(float(vector[1]), float(vector[0]))


def _nearest_boundary_piece_angle(
    point: XY, pieces: tuple[_BoundaryPiece, ...]
) -> float:
    px, py = point
    best_distance = math.inf
    best_angle = 0.0
    for piece in pieces:
        ax, ay = piece.start
        bx, by = piece.end
        vx = bx - ax
        vy = by - ay
        denom = vx * vx + vy * vy
        if denom <= 1e-12:
            continue
        t = float(np.clip(((px - ax) * vx + (py - ay) * vy) / denom, 0.0, 1.0))
        qx = ax + vx * t
        qy = ay + vy * t
        distance = math.hypot(px - qx, py - qy)
        if distance < best_distance:
            best_distance = distance
            best_angle = piece.angle
    return best_angle


def _solve_yang_b_vectors(
    *,
    vertex_count: int,
    adjacency: tuple[tuple[int, ...], ...],
    boundary_anchors: dict[int, np.ndarray],
    config: StreamlineConfig,
    base_angle: float,
) -> tuple[np.ndarray, dict[str, Any]]:
    omega = float(config.b_field_boundary_alignment_weight)
    if omega < 0.0 or not math.isfinite(omega):
        raise ValueError("b_field_boundary_alignment_weight must be finite and >= 0")

    base_vector = _cross_vector(base_angle)
    vectors = np.repeat(base_vector[np.newaxis, :], vertex_count, axis=0)
    for index, anchor in boundary_anchors.items():
        vectors[index] = anchor

    initial_smoothness = _mesh_smoothness_energy(vectors, adjacency)
    initial_alignment_error = _boundary_alignment_error(vectors, boundary_anchors)
    max_iterations = max(0, int(config.field_solver_iterations))
    tolerance = max(0.0, float(config.field_solver_tolerance))
    residual = 0.0
    iteration_count = 0

    for iteration in range(1, max_iterations + 1):
        iteration_count = iteration
        old = vectors
        new = old.copy()
        residual = 0.0
        for index, neighbors in enumerate(adjacency):
            if not neighbors:
                continue
            neighbor_sum = np.zeros(2, dtype=float)
            for neighbor in neighbors:
                neighbor_sum += old[neighbor]
            denominator = float(len(neighbors))
            target = neighbor_sum
            anchor = boundary_anchors.get(index)
            if anchor is not None and omega > 0.0:
                target = target + anchor * omega
                denominator += omega
            if denominator <= 1e-12:
                continue
            updated = target / denominator
            new[index] = updated
            residual = max(residual, float(np.linalg.norm(updated - old[index])))
        vectors = new
        if residual <= tolerance:
            break

    normalized = np.zeros_like(vectors)
    renormalized_count = 0
    for index, vector in enumerate(vectors):
        normalized[index] = _normalize_vector(vector, base_vector)
        if float(np.linalg.norm(vector)) > 1e-12:
            renormalized_count += 1

    final_smoothness = _mesh_smoothness_energy(normalized, adjacency)
    final_alignment_error = _boundary_alignment_error(normalized, boundary_anchors)
    return normalized, {
        "field_b_boundary_alignment_weight": float(omega),
        "field_b_solver_iterations": int(iteration_count),
        "field_b_solver_residual": float(residual),
        "field_b_smoothness_energy_initial": float(initial_smoothness),
        "field_b_smoothness_energy": float(final_smoothness),
        "field_b_boundary_alignment_error_initial": float(initial_alignment_error),
        "field_b_boundary_alignment_error": float(final_alignment_error),
        "field_b_renormalized_vector_count": int(renormalized_count),
    }


def _mesh_smoothness_energy(
    vectors: np.ndarray, adjacency: tuple[tuple[int, ...], ...]
) -> float:
    total = 0.0
    edge_count = 0
    for left, neighbors in enumerate(adjacency):
        for right in neighbors:
            if right <= left:
                continue
            delta = vectors[left] - vectors[right]
            total += float(np.dot(delta, delta))
            edge_count += 1
    return total / max(edge_count, 1)


def _boundary_alignment_error(
    vectors: np.ndarray, boundary_anchors: dict[int, np.ndarray]
) -> float:
    if not boundary_anchors:
        return 0.0
    total = 0.0
    for index, anchor in boundary_anchors.items():
        delta = vectors[index] - anchor
        total += float(np.dot(delta, delta))
    return total / len(boundary_anchors)


def _yang_d_raw_vectors(
    *,
    points: np.ndarray,
    boundary: Polygon,
    pieces: tuple[_BoundaryPiece, ...],
    concave_vertices: set[int],
    fallback_angle: float,
) -> tuple[np.ndarray, dict[str, Any]]:
    vectors = np.zeros((len(points), 2), dtype=float)
    weighted_distances: list[float] = []
    concave_tangent_use_count = 0
    footpoint_count = 0

    for index, point in enumerate(points):
        choice = _yang_d_footpoint_choice(
            boundary=boundary,
            point=(float(point[0]), float(point[1])),
            pieces=pieces,
            concave_vertices=concave_vertices,
            fallback_angle=fallback_angle,
        )
        if choice is None:
            vectors[index] = _cross_vector(fallback_angle)
            continue
        vectors[index] = _cross_vector(choice.angle)
        weighted_distances.append(choice.weighted_distance)
        concave_tangent_use_count += int(choice.uses_concave_tangent)
        footpoint_count += 1

    if weighted_distances:
        min_distance = min(weighted_distances)
        max_distance = max(weighted_distances)
        mean_distance = sum(weighted_distances) / len(weighted_distances)
    else:
        min_distance = max_distance = mean_distance = 0.0

    return vectors, {
        "field_weighted_footpoint_count": int(footpoint_count),
        "field_weighted_footpoint_distance_min": float(min_distance),
        "field_weighted_footpoint_distance_max": float(max_distance),
        "field_weighted_footpoint_distance_mean": float(mean_distance),
        "field_concave_tangent_use_count": int(concave_tangent_use_count),
    }


def _yang_d_footpoint_choice(
    *,
    boundary: Polygon,
    point: XY,
    pieces: tuple[_BoundaryPiece, ...],
    concave_vertices: set[int],
    fallback_angle: float,
) -> _FootpointChoice | None:
    px, py = point
    ring_coords = [(float(x), float(y)) for x, y in boundary.exterior.coords[:-1]]
    best: _FootpointChoice | None = None

    def consider(choice: _FootpointChoice) -> None:
        nonlocal best
        if best is None:
            best = choice
            return
        if choice.weighted_distance < best.weighted_distance - 1e-10:
            best = choice
            return
        if (
            abs(choice.weighted_distance - best.weighted_distance) <= 1e-10
            and choice.boundary_piece_index < best.boundary_piece_index
        ):
            best = choice

    for piece in pieces:
        ax, ay = piece.start
        bx, by = piece.end
        vx = bx - ax
        vy = by - ay
        denom = vx * vx + vy * vy
        if denom <= 1e-12:
            continue
        raw_t = ((px - ax) * vx + (py - ay) * vy) / denom
        if 0.0 < raw_t < 1.0:
            qx = ax + vx * raw_t
            qy = ay + vy * raw_t
            distance = math.hypot(px - qx, py - qy)
            angle = piece.angle if distance <= 1e-12 else math.atan2(qy - py, qx - px)
            consider(
                _FootpointChoice(
                    angle=angle,
                    weighted_distance=distance / piece.weight,
                    boundary_piece_index=piece.index,
                    uses_concave_tangent=False,
                )
            )
            continue

        endpoint_index = (
            piece.index if raw_t <= 0.0 else (piece.index + 1) % len(pieces)
        )
        endpoint = ring_coords[endpoint_index]
        if endpoint_index in concave_vertices:
            choice = _concave_tangent_choice(
                point=point,
                vertex_index=endpoint_index,
                pieces=pieces,
                boundary=boundary,
            )
            consider(choice)
            continue

        distance = math.hypot(px - endpoint[0], py - endpoint[1])
        angle = (
            piece.angle
            if distance <= 1e-12
            else math.atan2(endpoint[1] - py, endpoint[0] - px)
        )
        consider(
            _FootpointChoice(
                angle=angle,
                weighted_distance=distance / piece.weight,
                boundary_piece_index=piece.index,
                uses_concave_tangent=False,
            )
        )

    return best


def _concave_tangent_choice(
    *,
    point: XY,
    vertex_index: int,
    pieces: tuple[_BoundaryPiece, ...],
    boundary: Polygon,
) -> _FootpointChoice:
    previous_piece = pieces[(vertex_index - 1) % len(pieces)]
    next_piece = pieces[vertex_index % len(pieces)]
    previous_distance = _signed_interior_line_distance(
        point, previous_piece.start, previous_piece.end, boundary
    )
    next_distance = _signed_interior_line_distance(
        point, next_piece.start, next_piece.end, boundary
    )
    previous_weighted = previous_distance / previous_piece.weight
    next_weighted = next_distance / next_piece.weight
    if previous_weighted >= next_weighted:
        return _FootpointChoice(
            angle=previous_piece.angle,
            weighted_distance=max(0.0, previous_weighted),
            boundary_piece_index=previous_piece.index,
            uses_concave_tangent=True,
        )
    return _FootpointChoice(
        angle=next_piece.angle,
        weighted_distance=max(0.0, next_weighted),
        boundary_piece_index=next_piece.index,
        uses_concave_tangent=True,
    )


def _signed_interior_line_distance(
    point: XY, start: XY, end: XY, boundary: Polygon
) -> float:
    px, py = point
    ax, ay = start
    bx, by = end
    vx = bx - ax
    vy = by - ay
    length = math.hypot(vx, vy)
    if length <= 1e-12:
        return 0.0

    left_normal = np.array([-vy / length, vx / length], dtype=float)
    if not boundary.exterior.is_ccw:
        left_normal *= -1.0
    return float((px - ax) * left_normal[0] + (py - ay) * left_normal[1])


def _concave_vertex_indices(boundary: Polygon) -> set[int]:
    coords = [(float(x), float(y)) for x, y in boundary.exterior.coords[:-1]]
    if len(coords) < 3:
        return set()
    orientation = 1.0 if boundary.exterior.is_ccw else -1.0
    concave: set[int] = set()
    for index, current in enumerate(coords):
        previous = coords[index - 1]
        nxt = coords[(index + 1) % len(coords)]
        ax = current[0] - previous[0]
        ay = current[1] - previous[1]
        bx = nxt[0] - current[0]
        by = nxt[1] - current[1]
        turn = ax * by - ay * bx
        if turn * orientation < -1e-10:
            concave.add(index)
    return concave


def _smooth_yang_vectors(
    vectors: np.ndarray, adjacency: tuple[tuple[int, ...], ...]
) -> tuple[np.ndarray, dict[str, int]]:
    smoothed = vectors.copy()
    near_zero_count = 0
    for index, neighbors in enumerate(adjacency):
        if not neighbors:
            continue
        vector_sum = vectors[index].copy()
        for neighbor in neighbors:
            vector_sum += vectors[neighbor]
        norm = float(np.linalg.norm(vector_sum))
        if norm <= 1e-8:
            near_zero_count += 1
            smoothed[index] = vectors[index]
            continue
        smoothed[index] = vector_sum / norm
    return smoothed, {"near_zero_vector_count": near_zero_count}


def _singularity_candidate_count(vectors: np.ndarray, simplices: np.ndarray) -> int:
    count = 0
    for simplex in simplices:
        vector = np.sum(vectors[np.asarray(simplex, dtype=int)], axis=0)
        if float(np.linalg.norm(vector)) <= 0.12:
            count += 1
    return count


def _sample_yang_mesh_field_angle(field: _YangMeshField, point: XY) -> float:
    sample = np.asarray(point, dtype=float)
    simplex_id = int(field.triangulation.find_simplex(sample))
    if simplex_id >= 0 and simplex_id in field.retained_simplex_indices:
        transform = field.triangulation.transform[simplex_id]
        bary_prefix = np.dot(transform[:2], sample - transform[2])
        barycentric = np.asarray(
            [bary_prefix[0], bary_prefix[1], 1.0 - bary_prefix.sum()],
            dtype=float,
        )
        if np.all(barycentric >= -1e-7):
            simplex = np.asarray(field.triangulation.simplices[simplex_id], dtype=int)
            vector = np.sum(field.vectors[simplex] * barycentric[:, np.newaxis], axis=0)
            if float(np.linalg.norm(vector)) > 1e-8:
                return _angle_from_cross_vector(vector, field.base_angle)

    return _sample_nearest_yang_mesh_angle(field, sample)


def _sample_nearest_yang_mesh_angle(field: _YangMeshField, sample: np.ndarray) -> float:
    if len(field.points) == 0:
        return _boundary_blend_angle(
            field.boundary,
            (float(sample[0]), float(sample[1])),
            field.base_angle,
            field.config,
        )

    deltas = field.points - sample
    squared_distances = np.sum(deltas * deltas, axis=1)
    nearest_count = min(6, len(field.points))
    nearest_indices = np.argpartition(squared_distances, nearest_count - 1)[
        :nearest_count
    ]
    exact_matches = [
        int(index)
        for index in nearest_indices
        if squared_distances[int(index)] <= 1e-16
    ]
    if exact_matches:
        return _angle_from_cross_vector(
            field.vectors[exact_matches[0]], field.base_angle
        )

    weights = 1.0 / np.maximum(squared_distances[nearest_indices], 1e-12)
    vector = np.sum(field.vectors[nearest_indices] * weights[:, np.newaxis], axis=0)
    if float(np.linalg.norm(vector)) <= 1e-8:
        return _boundary_blend_angle(
            field.boundary,
            (float(sample[0]), float(sample[1])),
            field.base_angle,
            field.config,
        )
    return _angle_from_cross_vector(vector, field.base_angle)


def _sample_grid_angle(field: _GridSmoothField, point: XY) -> float:
    x, y = point
    minx = float(field.xs[0])
    maxx = float(field.xs[-1])
    miny = float(field.ys[0])
    maxy = float(field.ys[-1])
    fx = 0.0 if maxx <= minx else (x - minx) / (maxx - minx) * (len(field.xs) - 1)
    fy = 0.0 if maxy <= miny else (y - miny) / (maxy - miny) * (len(field.ys) - 1)
    fx = float(np.clip(fx, 0.0, len(field.xs) - 1))
    fy = float(np.clip(fy, 0.0, len(field.ys) - 1))
    ix0 = min(max(int(math.floor(fx)), 0), len(field.xs) - 1)
    iy0 = min(max(int(math.floor(fy)), 0), len(field.ys) - 1)
    ix1 = min(ix0 + 1, len(field.xs) - 1)
    iy1 = min(iy0 + 1, len(field.ys) - 1)
    tx = fx - ix0
    ty = fy - iy0
    samples = (
        (iy0, ix0, (1.0 - tx) * (1.0 - ty)),
        (iy0, ix1, tx * (1.0 - ty)),
        (iy1, ix0, (1.0 - tx) * ty),
        (iy1, ix1, tx * ty),
    )
    vector = np.zeros(2, dtype=float)
    total_weight = 0.0
    for iy, ix, weight in samples:
        if weight <= 0.0 or not field.valid[iy, ix]:
            continue
        vector += field.vectors[iy, ix] * weight
        total_weight += weight

    if total_weight <= 1e-12:
        return _boundary_blend_angle(
            field.boundary, point, field.base_angle, field.config
        )
    vector /= total_weight

    tangent_angle, boundary_distance = _nearest_boundary_tangent(field.boundary, point)
    boundary_weight = _boundary_constraint_weight(
        boundary_distance, field.boundary, field.config
    )
    if boundary_weight > 1e-9:
        vector += _cross_vector(tangent_angle) * boundary_weight
    return _angle_from_cross_vector(vector, field.base_angle)


def _principal_cross_axis(boundary: Polygon) -> float:
    edges = _boundary_segments(boundary)
    x = 0.0
    y = 0.0
    for a, b in edges:
        angle = math.atan2(b[1] - a[1], b[0] - a[0])
        length = math.hypot(b[0] - a[0], b[1] - a[1])
        x += length * math.cos(4.0 * angle)
        y += length * math.sin(4.0 * angle)
    if math.hypot(x, y) > 1e-9:
        return _normalize_pi(0.25 * math.atan2(y, x))

    rect = boundary.minimum_rotated_rectangle
    rect_coords = [(float(px), float(py)) for px, py in rect.exterior.coords[:-1]]
    rect_edges = [
        (
            math.hypot(b[0] - a[0], b[1] - a[1]),
            math.atan2(b[1] - a[1], b[0] - a[0]),
        )
        for a, b in zip(rect_coords, rect_coords[1:] + rect_coords[:1], strict=True)
    ]
    if not rect_edges:
        return 0.0
    return _normalize_pi(max(rect_edges, key=lambda item: item[0])[1])


def _boundary_segments(boundary: Polygon) -> list[tuple[XY, XY]]:
    coords = [(float(x), float(y)) for x, y in boundary.exterior.coords]
    return list(zip(coords, coords[1:], strict=False))


def _nearest_boundary_tangent(boundary: Polygon, point: XY) -> tuple[float, float]:
    px, py = point
    best_distance = math.inf
    best_angle = 0.0
    for a, b in _boundary_segments(boundary):
        ax, ay = a
        bx, by = b
        vx = bx - ax
        vy = by - ay
        denom = vx * vx + vy * vy
        if denom <= 1e-12:
            continue
        t = np.clip(((px - ax) * vx + (py - ay) * vy) / denom, 0.0, 1.0)
        qx = ax + vx * float(t)
        qy = ay + vy * float(t)
        distance = math.hypot(px - qx, py - qy)
        if distance < best_distance:
            best_distance = distance
            best_angle = math.atan2(vy, vx)
    return best_angle, best_distance


def _active_field_angle(field: _CrossField, point: XY, previous_angle: float) -> float:
    angle = field.angle_at(point)
    options = (
        angle,
        angle + math.pi * 0.5,
        angle + math.pi,
        angle - math.pi * 0.5,
    )
    return min(options, key=lambda option: _angle_delta_2pi(option, previous_angle))


def _field_vector(
    field: _CrossField, point: XY, previous_angle: float
) -> tuple[np.ndarray, float]:
    angle = _active_field_angle(field, point, previous_angle)
    return np.array([math.cos(angle), math.sin(angle)], dtype=float), angle


def _midpoint_step(
    field: _CrossField, point: XY, heading: float, step: float
) -> tuple[XY, float]:
    pos = np.array(point, dtype=float)
    k1, a1 = _field_vector(field, (float(pos[0]), float(pos[1])), heading)
    k2, a2 = _field_vector(
        field,
        (float(pos[0] + 0.5 * step * k1[0]), float(pos[1] + 0.5 * step * k1[1])),
        a1,
    )
    delta = step * k2
    next_pos = pos + delta
    next_heading = (
        a2
        if np.linalg.norm(delta) <= 1e-12
        else math.atan2(float(delta[1]), float(delta[0]))
    )
    return (float(next_pos[0]), float(next_pos[1])), next_heading


def _trace_one_direction(
    boundary: Polygon,
    seed: XY,
    initial_heading: float,
    step: float,
    config: StreamlineConfig,
    field: _CrossField,
) -> list[XY]:
    points: list[XY] = []
    current = seed
    heading = initial_heading
    for _ in range(config.max_steps):
        proposed, heading = _midpoint_step(field, current, heading, step)
        segment = LineString([current, proposed])
        if not boundary.covers(Point(proposed)):
            hit = _first_boundary_hit(boundary, segment, current)
            points.append(hit)
            break
        points.append(proposed)
        current = proposed
    return points


def _first_boundary_hit(boundary: Polygon, segment: LineString, start: XY) -> XY:
    candidates = _geometry_points(segment.intersection(boundary.boundary))
    if not candidates:
        return _nearest_boundary_point(boundary, _xy(Point(segment.coords[-1])))
    start_point = Point(start)
    return min(candidates, key=lambda xy: start_point.distance(Point(xy)))


def _geometry_points(geom) -> list[XY]:
    if geom.is_empty:
        return []
    geom_type = geom.geom_type
    if geom_type == "Point":
        return [(float(geom.x), float(geom.y))]
    if geom_type == "MultiPoint":
        return [(float(point.x), float(point.y)) for point in geom.geoms]
    if geom_type == "LineString":
        coords = list(geom.coords)
        return [
            (float(coords[0][0]), float(coords[0][1])),
            (float(coords[-1][0]), float(coords[-1][1])),
        ]
    if geom_type == "GeometryCollection":
        points: list[XY] = []
        for part in geom.geoms:
            points.extend(_geometry_points(part))
        return points
    return []


def _nearest_boundary_point(boundary: Polygon, point: XY) -> XY:
    distance = boundary.exterior.project(Point(point))
    nearest = boundary.exterior.interpolate(distance)
    return float(nearest.x), float(nearest.y)


def _dedupe_consecutive(points: list[XY]) -> list[XY]:
    out: list[XY] = []
    for point in points:
        if not out or math.hypot(point[0] - out[-1][0], point[1] - out[-1][1]) > 1e-8:
            out.append(point)
    return out


def _candidate_seed_plan(
    boundary: Polygon,
    *,
    field: _CrossField,
    target_count: int,
    seed: int,
    config: StreamlineConfig,
) -> _CandidateSeedPlan:
    if config.candidate_seed_mode == _CANDIDATE_SEED_MODE_GRID:
        points = tuple(
            _candidate_seed_points(
                boundary, target_count=target_count, seed=seed, config=config
            )
        )
        return _CandidateSeedPlan(
            points=points,
            diagnostics={
                "trace_seed_source": _CANDIDATE_SEED_MODE_GRID,
                "trace_mesh_interior_seed_count": 0,
                "trace_suppression_distance": 0.0,
                "trace_parcel_length": None,
                "trace_parcel_length_source": None,
            },
            suppression_distance=0.0,
        )

    if config.candidate_seed_mode == _CANDIDATE_SEED_MODE_YANG_MESH_VERTICES:
        if not isinstance(field, (_YangDField, _YangBField)):
            raise ValueError(
                "candidate_seed_mode='yang_mesh_vertices' requires "
                "a mesh-backed Yang field_mode"
            )
        points = tuple(_yang_mesh_interior_seed_points(field))
        parcel_length, parcel_length_source = _yang_candidate_parcel_length(
            boundary, config
        )
        suppression_distance = 0.3 * parcel_length
        return _CandidateSeedPlan(
            points=points,
            diagnostics={
                "trace_seed_source": _CANDIDATE_SEED_MODE_YANG_MESH_VERTICES,
                "trace_mesh_interior_seed_count": int(len(points)),
                "trace_suppression_distance": float(suppression_distance),
                "trace_parcel_length": float(parcel_length),
                "trace_parcel_length_source": parcel_length_source,
            },
            suppression_distance=float(suppression_distance),
        )

    raise ValueError(f"unknown candidate_seed_mode: {config.candidate_seed_mode}")


def _yang_mesh_interior_seed_points(field: _YangMeshField) -> list[XY]:
    retained_vertices = sorted(
        {
            int(vertex)
            for simplex_id in field.retained_simplex_indices
            for vertex in field.triangulation.simplices[simplex_id]
        }
    )
    scale = _domain_scale(field.boundary)
    boundary_tolerance = max(scale * 1e-8, 1e-8)
    interior = field.boundary.buffer(-boundary_tolerance)
    points: list[XY] = []
    for vertex_index in retained_vertices:
        point = (
            float(field.points[vertex_index][0]),
            float(field.points[vertex_index][1]),
        )
        if interior.covers(Point(point)):
            points.append(point)

    center = Point(float(field.boundary.centroid.x), float(field.boundary.centroid.y))
    distance_sorted = sorted(
        points,
        key=lambda point: (
            center.distance(Point(point)),
            round(point[0], 9),
            round(point[1], 9),
        ),
    )
    return _stratified_seed_order(distance_sorted)


def _yang_candidate_parcel_length(
    boundary: Polygon, config: StreamlineConfig
) -> tuple[float, str]:
    if config.parcel_length is not None:
        parcel_length = float(config.parcel_length)
        if parcel_length <= 0.0 or not math.isfinite(parcel_length):
            raise ValueError("parcel_length must be a positive finite value")
        return parcel_length, "config"

    return _domain_scale(boundary) * 0.15, "boundary_scale_fraction_0.15"


def _suppress_yang_seed_orientation_options(
    *,
    seed_points: tuple[XY, ...],
    active_options: list[list[bool]],
    line: LineString,
    orientation_index: int,
    suppression_distance: float,
) -> int:
    if suppression_distance <= 0.0:
        return 0

    suppressed_count = 0
    for seed_index, seed_xy in enumerate(seed_points):
        if not active_options[seed_index][orientation_index]:
            continue
        if line.distance(Point(seed_xy)) <= suppression_distance:
            active_options[seed_index][orientation_index] = False
            suppressed_count += 1
    return suppressed_count


def _candidate_seed_points(
    boundary: Polygon, *, target_count: int, seed: int, config: StreamlineConfig
) -> list[XY]:
    minx, miny, maxx, maxy = boundary.bounds
    centroid = (float(boundary.centroid.x), float(boundary.centroid.y))
    min_side = max(1, int(config.seed_grid_side_min))
    max_side = max(min_side, int(config.seed_grid_side_max))
    side = int(np.clip(7, min_side, max_side))
    rng = np.random.default_rng(seed)
    interior = boundary.buffer(-1e-8)
    points: list[XY] = []
    if boundary.covers(Point(centroid)):
        points.append(centroid)

    for iy in range(side):
        for ix in range(side):
            frac_x = (ix + 0.5) / side
            frac_y = (iy + 0.5) / side
            jitter_x = (float(rng.uniform(-0.18, 0.18)) / side) if seed != 0 else 0.0
            jitter_y = (float(rng.uniform(-0.18, 0.18)) / side) if seed != 0 else 0.0
            point = (
                float(minx + (frac_x + jitter_x) * (maxx - minx)),
                float(miny + (frac_y + jitter_y) * (maxy - miny)),
            )
            if interior.covers(Point(point)):
                points.append(point)

    boundary_points = _inward_boundary_seed_points(boundary, count=max(8, side * 2))
    points.extend(boundary_points)
    unique: dict[tuple[int, int], XY] = {}
    for point in points:
        unique[(round(point[0] * 1_000_000), round(point[1] * 1_000_000))] = point
    center = Point(centroid)
    distance_sorted = sorted(
        unique.values(),
        key=lambda point: (
            center.distance(Point(point)),
            round(point[0], 9),
            round(point[1], 9),
        ),
    )
    return _stratified_seed_order(distance_sorted)


def _stratified_seed_order(distance_sorted: list[XY]) -> list[XY]:
    count = len(distance_sorted)
    if count <= 2:
        return list(distance_sorted)

    bits = max(1, int(math.ceil(math.log2(count))))
    ordered: list[XY] = []
    for raw_index in range(1 << bits):
        index = _bit_reverse(raw_index, bits)
        if index < count:
            ordered.append(distance_sorted[index])
    return ordered


def _bit_reverse(value: int, bits: int) -> int:
    out = 0
    for _ in range(bits):
        out = (out << 1) | (value & 1)
        value >>= 1
    return out


def _inward_boundary_seed_points(boundary: Polygon, *, count: int) -> list[XY]:
    centroid = np.array([float(boundary.centroid.x), float(boundary.centroid.y)])
    perimeter = float(boundary.exterior.length)
    scale = _domain_scale(boundary)
    interior = boundary.buffer(-1e-8)
    points: list[XY] = []
    if perimeter <= 1e-12:
        return points
    for index in range(count):
        boundary_point = boundary.exterior.interpolate(
            perimeter * (index + 0.5) / count
        )
        raw = np.array([float(boundary_point.x), float(boundary_point.y)])
        inward = centroid - raw
        length = float(np.linalg.norm(inward))
        if length <= 1e-12:
            continue
        candidate = raw + inward / length * scale * 0.16
        point = (float(candidate[0]), float(candidate[1]))
        if interior.covers(Point(point)):
            points.append(point)
    return points


def _endpoint_distance_max(boundary: Polygon, line: LineString) -> float:
    coords = list(line.coords)
    return max(
        boundary.exterior.distance(Point(coords[0])),
        boundary.exterior.distance(Point(coords[-1])),
    )


def _line_key(line: LineString) -> tuple[tuple[int, int], ...]:
    return tuple(
        (round(float(x) * 1_000_000), round(float(y) * 1_000_000))
        for x, y in line.coords
    )


def _midpoint_angle(line: LineString) -> float:
    if line.length <= 1e-12:
        return 0.0
    coords = [(float(x), float(y)) for x, y in line.coords]
    half = line.length * 0.5
    accum = 0.0
    for a, b in zip(coords, coords[1:], strict=False):
        seg_len = math.hypot(b[0] - a[0], b[1] - a[1])
        if accum + seg_len >= half and seg_len > 1e-12:
            return math.atan2(b[1] - a[1], b[0] - a[0])
        accum += seg_len
    a, b = coords[-2], coords[-1]
    return math.atan2(b[1] - a[1], b[0] - a[0])


def _is_suppressed(
    line: LineString,
    angle: float,
    others,
    *,
    duplicate_distance: float,
    angle_tolerance: float,
) -> bool:
    for other in others:
        if line.distance(other) > duplicate_distance:
            continue
        other_angle = _midpoint_angle(other)
        if _angle_delta_pi(angle, other_angle) <= angle_tolerance:
            return True
    return False


def _min_distance(line: LineString, others: tuple[LineString, ...]) -> float:
    if not others:
        return math.inf
    return min(float(line.distance(other)) for other in others)


def _line_bend(line: LineString) -> float:
    coords = [(float(x), float(y)) for x, y in line.coords]
    if len(coords) < 3:
        return 0.0
    headings: list[float] = []
    for a, b in zip(coords, coords[1:], strict=False):
        if math.hypot(b[0] - a[0], b[1] - a[1]) > 1e-9:
            headings.append(math.atan2(b[1] - a[1], b[0] - a[0]))
    return sum(
        _angle_delta_2pi(prev, nxt)
        for prev, nxt in zip(headings, headings[1:], strict=False)
    )


def _candidate_quality(
    line: LineString,
    *,
    bend: float,
    endpoint_distance_max: float,
    scale: float,
    min_existing_distance: float,
) -> float:
    length_score = line.length / scale
    endpoint_penalty = endpoint_distance_max / scale
    spacing_bonus = (
        0.0
        if math.isinf(min_existing_distance)
        else min(min_existing_distance / scale, 0.25)
    )
    bend_penalty = min(bend / math.pi, 0.8) * 0.08
    return float(length_score + spacing_bonus - endpoint_penalty - bend_penalty)
