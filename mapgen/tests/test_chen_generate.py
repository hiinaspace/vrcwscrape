"""Tests for the Chen 2024 Section 4 hierarchical co-generation driver.

These exercise the level-synchronized loop: emergent parcel counts within a
calibration band, geometry/paper invariants, street reachability, min-area
termination, determinism, the Eq. 2 Q_acce scorer, and the Yang ablation modes.
Counts are emergent (the paper-faithful input is ``min_parcel_area``), so all
count assertions use ranges rather than exact values.
"""

from __future__ import annotations

import hashlib

import numpy as np
import pytest
from shapely import LineString, Polygon

from mapgen.chen_core import ChenSplitWeights
from mapgen.chen_field import RasterDensityField, RasterGuidanceField
from mapgen.chen_generate import (
    GENERATION_STAGE,
    STREAMLINE_MODE_YANG_B_FIELD,
    STREAMLINE_MODE_YANG_D_FIELD,
    BoundarySpec,
    _candidate_access_score,
    _score_candidate,
    _stable_mix,
    boundary_preset,
    generate_layout_for_boundary,
    generate_named_layout,
)

SHAPES = ["square", "oval", "triangle"]

# Calibration band for emergent parcel counts relative to the requested target.
# The convenience mapping min_area = area / (1.5 * target) was calibrated in
# slice G.  Square always produces counts at a power-of-2 boundary (structural
# limit of binary splits), so the 1.33× ratio for squares is expected.
# oval/triangle land 1.0–1.2×.  The band [0.6, 1.4] covers all three shapes.
_COUNT_LOWER = 0.6
_COUNT_UPPER = 1.4


def _partition_line_keys(generated) -> list:
    return [
        (level, tuple((round(x, 6), round(y, 6)) for x, y in line.coords))
        for level, line in generated.partition_lines
    ]


def _metrics_core(generated) -> dict:
    m = generated.metrics
    return {
        "parcel_total_count": m["parcel_total_count"],
        "max_hierarchical_level": m["max_hierarchical_level"],
        "accepted_split_count": m["accepted_split_count"],
        "weld_applied_count": m["weld_applied_count"],
        "weld_rejected_count": m["weld_rejected_count"],
        "splits_per_level": m["splits_per_level"],
        "street_edge_count": m["street_edge_count"],
    }


# ---------------------------------------------------------------------------
# Emergent count band + invariants
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", SHAPES)
def test_emergent_count_band_small(name: str) -> None:
    target = 12
    generated = generate_named_layout(name, parcel_count=target, seed=0)
    count = len(generated.layout.mesh.parcels)
    assert _COUNT_LOWER * target <= count <= _COUNT_UPPER * target, (
        f"{name}: emergent={count} target={target}"
    )
    assert generated.metrics["geometry_valid_pass"]
    assert generated.metrics["paper_invariant_pass"]
    assert generated.metrics["generation_stage"] == GENERATION_STAGE


@pytest.mark.slow
@pytest.mark.parametrize("name", SHAPES)
def test_emergent_count_band_large(name: str) -> None:
    target = 48
    generated = generate_named_layout(name, parcel_count=target, seed=0)
    count = len(generated.layout.mesh.parcels)
    assert _COUNT_LOWER * target <= count <= _COUNT_UPPER * target, (
        f"{name}: emergent={count} target={target}"
    )
    assert generated.metrics["geometry_valid_pass"]
    assert generated.metrics["paper_invariant_pass"]
    # quad fraction sanity. The square control is fully rectilinear; the
    # curved-field oval/triangle land well below the paper's quad-only target
    # (~0.32-0.38) — that is the field-fidelity debt, not a generation bug.
    # This loose floor catches gross failure only; slice G/field wave tightens.
    floor = 0.8 if name == "square" else 0.25
    assert generated.metrics["parcel_quad_fraction"] > floor


# ---------------------------------------------------------------------------
# Concave many-vertex island preset (regression for corner-graph conformity)
#
# The 48-gon "island" boundary is the first concave preset whose interior
# splits produce a smoothly curved shared boundary that one parcel simplifies
# away (135 deg collinearity) while a neighbour retains verbatim (its corner
# ring would otherwise collapse below three vertices). A single-pass corner
# propagation left such shared boundaries non-conforming, tripping the
# parcel_graph conformity validator. These tests pin the fix.
# ---------------------------------------------------------------------------


def test_island_small_generates_and_passes_invariants() -> None:
    target = 12
    generated = generate_named_layout("island", parcel_count=target, seed=0)
    count = len(generated.layout.mesh.parcels)
    assert _COUNT_LOWER * target <= count <= _COUNT_UPPER * target, (
        f"island: emergent={count} target={target}"
    )
    m = generated.metrics
    assert m["geometry_valid_pass"]
    assert m["paper_invariant_pass"]
    assert m["street_topology_reachability_pass"]
    assert m["unreachable_parcel_count"] == 0
    assert m["street_graph_component_count"] == 1


@pytest.mark.slow
def test_island_large_generates_and_passes_invariants() -> None:
    target = 48
    generated = generate_named_layout("island", parcel_count=target, seed=0)
    count = len(generated.layout.mesh.parcels)
    assert _COUNT_LOWER * target <= count <= _COUNT_UPPER * target, (
        f"island: emergent={count} target={target}"
    )
    m = generated.metrics
    assert m["geometry_valid_pass"]
    assert m["paper_invariant_pass"]
    assert m["street_topology_reachability_pass"]


@pytest.mark.parametrize("name", SHAPES)
def test_street_reachability_single_component(name: str) -> None:
    generated = generate_named_layout(name, parcel_count=12, seed=0)
    m = generated.metrics
    assert m["unreachable_parcel_count"] == 0
    assert m["street_graph_component_count"] == 1
    assert m["street_network_subset_of_corner_graph"]
    assert m["street_topology_reachability_pass"]


# ---------------------------------------------------------------------------
# Termination correctness (min-area)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", SHAPES)
def test_termination_respects_min_parcel_area(name: str) -> None:
    boundary = boundary_preset(name)
    min_area = boundary.geom.area / 24.0
    generated = generate_layout_for_boundary(boundary, min_parcel_area=min_area, seed=1)
    # Every parcel must be at least the minimum area (the weaker, always-true
    # form of the termination condition; a parcel above 2*min_area may remain
    # only if it had no viable candidate).
    for parcel in generated.layout.mesh.parcels.values():
        assert parcel.geom.area >= min_area - 1e-6, (
            f"{name}: parcel below min area: {parcel.geom.area} < {min_area}"
        )


def _max_parcel_elongation(generated) -> float:
    """Max min-rotated-rectangle long/short ratio over all parcels."""
    import math

    worst = 1.0
    for parcel in generated.layout.mesh.parcels.values():
        rect = parcel.geom.minimum_rotated_rectangle
        coords = list(rect.exterior.coords)
        if len(coords) < 4:
            continue
        edge_a = math.dist(coords[0], coords[1])
        edge_b = math.dist(coords[1], coords[2])
        short, long_ = sorted((edge_a, edge_b))
        worst = max(worst, long_ / max(short, 1e-9))
    return worst


@pytest.mark.parametrize("aspect,width,height", [(4, 160.0, 40.0), (8, 320.0, 40.0)])
def test_paper_mode_elongated_rectangle_does_not_ratchet_slivers(
    aspect: int, width: float, height: float
) -> None:
    """Paper-default elongated rectangles must not degrade into sliver fans.

    Regression for the streamline min-length gate deviation: the old absolute
    gate (0.18 x bbox diagonal, no basis in Yang Sec. 5 / Chen Sec. 4.1)
    rejected every short-axis candidate above ~5.5:1 aspect, so each split ran
    parallel to the long axis and doubled the child aspect — an 8:1 rectangle
    at parcel_count=16 ratcheted to 128:1 slivers. With the Yang rejection set
    (self-intersection + boundary-endpoint only) plus orientation-fair
    candidate selection, Eq. 2 picks the aspect-reducing perpendicular cuts:
    the 8:1 case settles into an 8x2 grid of 2:1 parcels and the 4:1 case into
    16 squares. The <4.0 band is loose against field/selection drift.
    """
    boundary = BoundarySpec(
        name=f"rect{aspect}x1",
        geom=Polygon([(0.0, 0.0), (width, 0.0), (width, height), (0.0, height)]),
    )
    generated = generate_layout_for_boundary(boundary, parcel_count=16, seed=0)
    assert generated.metrics["geometry_valid_pass"]
    assert generated.metrics["paper_invariant_pass"]
    assert _max_parcel_elongation(generated) < 4.0


def test_min_parcel_area_and_parcel_count_are_mutually_exclusive() -> None:
    boundary = boundary_preset("square")
    with pytest.raises(ValueError):
        generate_layout_for_boundary(boundary)
    with pytest.raises(ValueError):
        generate_layout_for_boundary(boundary, min_parcel_area=10.0, parcel_count=12)


# ---------------------------------------------------------------------------
# Determinism / variation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", SHAPES)
def test_same_seed_is_deterministic(name: str) -> None:
    a = generate_named_layout(name, parcel_count=12, seed=5)
    b = generate_named_layout(name, parcel_count=12, seed=5)
    assert _partition_line_keys(a) == _partition_line_keys(b)
    assert _metrics_core(a) == _metrics_core(b)


# The perfectly symmetric square yields the same canonical streamline splits
# regardless of seed; only the asymmetric shapes are expected to vary.
@pytest.mark.parametrize("name", ["oval", "triangle"])
def test_different_seed_varies(name: str) -> None:
    a = generate_named_layout(name, parcel_count=12, seed=1)
    b = generate_named_layout(name, parcel_count=12, seed=2)
    # At least the partition geometry should differ; counts may coincide.
    assert _partition_line_keys(a) != _partition_line_keys(b)


def test_stable_mix_is_pure_and_varies() -> None:
    assert _stable_mix(0, 1, 2) == _stable_mix(0, 1, 2)
    assert _stable_mix(0, 1, 2) != _stable_mix(0, 1, 3)
    assert _stable_mix(0, 1, 2) != _stable_mix(1, 1, 2)
    assert _stable_mix(0, 1, 2) != _stable_mix(0, 2, 2)
    assert _stable_mix(0, 0, 0) >= 0


# ---------------------------------------------------------------------------
# Level-0 boundary street + partition-line layer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", SHAPES)
def test_level0_boundary_street_present(name: str) -> None:
    generated = generate_named_layout(name, parcel_count=12, seed=0)
    layout = generated.layout
    # The boundary ring corner-graph edges must all be in the street network
    # (level-0 street = whole input boundary).
    boundary_corner_edges = {
        edge
        for edge in layout.corner_graph.edges
        if all(layout.mesh.vertices[node].on_boundary for node in edge)
    }
    # Boundary corner edges connect two on-boundary corners; the perimeter ring
    # is a subset. Require that the street network covers the boundary loop.
    perimeter_edges = {
        edge for edge, parcels in _edge_parcel_counts(layout).items() if parcels == 1
    }
    assert perimeter_edges, f"{name}: no perimeter edges found"
    assert perimeter_edges <= set(layout.street_network.edges), (
        f"{name}: boundary ring not fully in street network"
    )
    assert boundary_corner_edges  # sanity


def _edge_parcel_counts(layout) -> dict:
    from mapgen.chen_core import ring_edges

    counts: dict = {}
    for ring in layout.corner_graph.parcel_corner_rings.values():
        for edge in ring_edges(ring):
            counts[edge] = counts.get(edge, 0) + 1
    return counts


@pytest.mark.parametrize("name", SHAPES)
def test_partition_lines_carry_level_tags(name: str) -> None:
    generated = generate_named_layout(name, parcel_count=12, seed=0)
    assert generated.partition_lines
    levels = [level for level, _line in generated.partition_lines]
    assert all(level >= 1 for level in levels)
    assert levels == sorted(levels)  # appended in level order
    # split_lines compatibility property
    assert generated.split_lines == tuple(
        line for _level, line in generated.partition_lines
    )


# ---------------------------------------------------------------------------
# Q_acce regression (scorer unit test)
# ---------------------------------------------------------------------------


def test_q_acce_prefers_street_adjacent_split() -> None:
    """A street-adjacent split must outscore an otherwise-equal interior one."""
    square = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    min_area = 0.1
    # Vertical cut: with a street along the bottom edge both children touch the
    # street, so Q_acce = 2.
    vertical = LineString([(0.5, -0.1), (0.5, 1.1)])
    streets = [((0.0, 0.0), (1.0, 0.0))]
    with_street = _score_candidate(square, vertical, streets, min_parcel_area=min_area)
    without_street = _score_candidate(square, vertical, [], min_parcel_area=min_area)
    assert with_street is not None and without_street is not None
    # The only difference is Q_acce (2 vs 1, weight 0.2): +0.2.
    assert with_street[0] == pytest.approx(without_street[0] + 0.2, abs=1e-9)


def test_q_acce_values_match_paper_two_or_one() -> None:
    streets = [((0.0, 0.0), (1.0, 0.0))]
    left = Polygon([(0, 0), (0.5, 0), (0.5, 1), (0, 1)])
    right = Polygon([(0.5, 0), (1, 0), (1, 1), (0.5, 1)])
    both = _candidate_access_score(left, right, streets, min_parcel_area=0.1)
    assert both == 2.0
    bottom = Polygon([(0, 0), (1, 0), (1, 0.5), (0, 0.5)])
    top = Polygon([(0, 0.5), (1, 0.5), (1, 1), (0, 1)])
    one = _candidate_access_score(bottom, top, streets, min_parcel_area=0.1)
    assert one == 1.0
    assert _candidate_access_score(left, right, [], min_parcel_area=0.1) == 1.0


def test_score_rejects_undersized_child() -> None:
    square = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    # A cut very close to an edge produces a sliver below min area.
    sliver_cut = LineString([(0.01, -0.1), (0.01, 1.1)])
    assert _score_candidate(square, sliver_cut, [], min_parcel_area=0.1) is None


# ---------------------------------------------------------------------------
# Part A: ChenSplitWeights plumbing through _score_candidate.
# ---------------------------------------------------------------------------


def test_split_weights_default_matches_paper_lambdas() -> None:
    """split_weights=None must reproduce the inline Eq. 2 (0.3/0.5/0.2) score."""
    square = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    cut = LineString([(1.0, -0.1), (1.0, 2.1)])
    streets = [((0.0, 0.0), (2.0, 0.0))]
    implicit = _score_candidate(square, cut, streets, min_parcel_area=0.01)
    explicit = _score_candidate(
        square,
        cut,
        streets,
        min_parcel_area=0.01,
        split_weights=ChenSplitWeights(),
    )
    assert implicit is not None and explicit is not None
    assert explicit[0] == pytest.approx(implicit[0], abs=1e-12)


def test_split_weights_change_selected_candidate() -> None:
    """Extreme weights flip the argmax between two constructed candidates.

    Cut A splits the square into two equal-area triangles (balanced size, higher
    irregularity); cut B into a thin slab + larger rectangle (unbalanced size,
    lower irregularity). Size-heavy weights must prefer A; regularity-heavy
    weights must prefer B. This proves the weights reach _score_candidate and
    drive selection.
    """
    square = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    cut_balanced = LineString([(-0.1, -0.1), (2.1, 2.1)])
    cut_regular = LineString([(0.6, -0.1), (0.6, 2.1)])

    size_heavy = ChenSplitWeights(size=1.0, regularity=0.0, access=0.0)
    regu_heavy = ChenSplitWeights(size=0.0, regularity=1.0, access=0.0)

    def score(cut: LineString, weights: ChenSplitWeights) -> float:
        scored = _score_candidate(
            square, cut, [], min_parcel_area=0.01, split_weights=weights
        )
        assert scored is not None
        return scored[0]

    assert score(cut_balanced, size_heavy) > score(cut_regular, size_heavy)
    assert score(cut_regular, regu_heavy) > score(cut_balanced, regu_heavy)


def test_generate_accepts_custom_split_weights_and_surfaces_them() -> None:
    boundary = boundary_preset("square")
    custom = ChenSplitWeights(size=0.6, regularity=0.3, access=0.1)
    generated = generate_layout_for_boundary(
        boundary, parcel_count=12, seed=0, split_weights=custom
    )
    m = generated.metrics
    assert m["split_weight_size"] == pytest.approx(0.6)
    assert m["split_weight_regularity"] == pytest.approx(0.3)
    assert m["split_weight_access"] == pytest.approx(0.1)
    assert m["guidance_field_applied"] is False
    assert generated.layout.mesh.parcels


# ---------------------------------------------------------------------------
# Part B: RasterGuidanceField is off by default and identity-preserving.
# ---------------------------------------------------------------------------


def _layout_signature(generated) -> tuple:
    parcels = tuple(
        tuple(round(c, 9) for c in parcel.geom.exterior.coords[0])
        + (round(parcel.geom.area, 9),)
        for parcel in sorted(
            generated.layout.mesh.parcels.values(), key=lambda p: p.parcel_id
        )
    )
    streets = tuple(sorted(generated.layout.street_network.edges))
    lines = tuple(
        (level, tuple(round(c, 9) for xy in line.coords for c in xy))
        for level, line in generated.partition_lines
    )
    return parcels, streets, lines


@pytest.mark.parametrize("name", SHAPES)
def test_guidance_and_weights_none_is_identical_to_pre_change_signature(
    name: str,
) -> None:
    boundary = boundary_preset(name)
    # The pre-change call signature (no split_weights / guidance kwargs).
    baseline = generate_layout_for_boundary(boundary, parcel_count=12, seed=0)
    # Explicitly None for all extension kwargs (split_weights / guidance / R2).
    with_none = generate_layout_for_boundary(
        boundary,
        parcel_count=12,
        seed=0,
        split_weights=None,
        guidance=None,
        density_field=None,
        max_parcel_mass=None,
    )
    assert _layout_signature(with_none) == _layout_signature(baseline)


def _default_path_digest(generated) -> str:
    """Stable content digest of a generated layout for golden pinning.

    SHA-256 over: sorted mesh vertex coordinates rounded to 1e-6, parcel ring
    vertex-id tuples sorted by parcel id, and sorted street-network edge keys.
    """
    mesh = generated.layout.mesh
    vertices = tuple(
        (vertex_id, round(float(vertex.point[0]), 6), round(float(vertex.point[1]), 6))
        for vertex_id, vertex in sorted(mesh.vertices.items())
    )
    rings = tuple(
        (parcel_id, parcel.ring) for parcel_id, parcel in sorted(mesh.parcels.items())
    )
    streets = tuple(sorted(generated.layout.street_network.edges))
    return hashlib.sha256(repr((vertices, rings, streets)).encode("ascii")).hexdigest()


# Golden digest of the paper-default square layout (parcel_count=12, seed=0)
# with every extension kwarg left at its default. This MUST only change when
# the paper-default generation path is INTENTIONALLY changed — it is the
# byte-identity contract that the default-off extensions (split_weights /
# guidance / density_field) hang off. If it changes unexpectedly, a default
# path perturbation leaked in. To regenerate after an INTENTIONAL default-path
# change: run the pinned test below and copy the actual digest from the
# assertion failure (it is _default_path_digest of the square parcel_count=12
# seed=0 default-kwargs layout), then note the intentional change in the diff.
#
# 2026-07 candidate-gate fix (Yang Sec. 5 rejection set + orientation-fair
# selection + direction-neutral Eq. 2 tiebreak): an INTENTIONAL default-path
# change, so this digest was re-derived — and verified UNCHANGED. On the
# symmetric square every candidate already passed the old min-length gate,
# and the reordered candidates/tiebreak resolve the Eq. 2 ties to the same
# cuts, so the pinned layout is byte-identical (checked twice for
# determinism). Elongated boundaries do change; see
# test_paper_mode_elongated_rectangle_does_not_ratchet_slivers.
_SQUARE_12_SEED0_GOLDEN_DIGEST = (
    "d04f64111b6ca0cbf029828191152e96904a0b7e6356a4dc9655ba8befb269f7"
)


def test_default_path_matches_pinned_golden_digest() -> None:
    """The kwargs-omitted default path reproduces the pinned golden layout.

    Unlike the omitted-vs-None comparison above (which would silently pass if
    the default path itself were perturbed), this pins the actual default
    output across time.
    """
    boundary = boundary_preset("square")
    generated = generate_layout_for_boundary(boundary, parcel_count=12, seed=0)
    assert _default_path_digest(generated) == _SQUARE_12_SEED0_GOLDEN_DIGEST


def test_generate_with_guidance_runs_and_flags_metric() -> None:
    boundary = boundary_preset("square")
    geom = boundary.geom
    minx, miny, maxx, maxy = geom.bounds
    cells = 16
    cell = max(maxx - minx, maxy - miny) / (cells - 1)
    guidance = RasterGuidanceField(
        angle=np.full((cells, cells), np.radians(45.0)),
        weight=np.full((cells, cells), 4.0),
        x0=minx,
        y0=miny,
        cell=cell,
    )
    generated = generate_layout_for_boundary(
        boundary, parcel_count=12, seed=0, guidance=guidance
    )
    assert generated.metrics["guidance_field_applied"] is True
    assert generated.layout.mesh.parcels
    # Invariants must still hold with guidance active.
    assert generated.metrics["paper_invariant_pass"] is True
    assert generated.metrics["geometry_valid_pass"] is True


# ---------------------------------------------------------------------------
# Part C: RasterDensityField density-mass split/termination (R2 extension).
# ---------------------------------------------------------------------------


def _corner_blob_density_field(
    geom: Polygon, *, cells: int = 64, blob_fraction: float = 0.30
) -> RasterDensityField:
    """Density raster: a high plateau in the lower-left corner, low elsewhere."""
    minx, miny, maxx, maxy = geom.bounds
    cell = max(maxx - minx, maxy - miny) / (cells - 1)
    density = np.full((cells, cells), 0.05, dtype=float)
    blob = max(int(cells * blob_fraction), 1)
    density[:blob, :blob] = 1.0  # rows = +y (bottom), cols = +x (left): corner
    return RasterDensityField(density=density, x0=minx, y0=miny, cell=cell)


def test_density_field_and_max_mass_must_be_supplied_together() -> None:
    boundary = boundary_preset("square")
    field = _corner_blob_density_field(boundary.geom)
    with pytest.raises(ValueError, match="supplied together"):
        generate_layout_for_boundary(
            boundary, parcel_count=12, seed=0, density_field=field
        )
    with pytest.raises(ValueError, match="supplied together"):
        generate_layout_for_boundary(
            boundary, parcel_count=12, seed=0, max_parcel_mass=1.0
        )


def test_max_parcel_mass_must_be_positive() -> None:
    boundary = boundary_preset("square")
    field = _corner_blob_density_field(boundary.geom)
    with pytest.raises(ValueError, match="max_parcel_mass must be positive"):
        generate_layout_for_boundary(
            boundary,
            parcel_count=12,
            seed=0,
            density_field=field,
            max_parcel_mass=0.0,
        )


def test_density_mode_off_by_default_metrics() -> None:
    boundary = boundary_preset("square")
    generated = generate_layout_for_boundary(boundary, parcel_count=12, seed=0)
    assert generated.metrics["density_mass_mode"] is False
    assert generated.metrics["max_parcel_mass"] is None
    assert generated.metrics["density_field_digest"] is None


def test_score_candidate_mass_size_term_prefers_mass_balance() -> None:
    """Eq. 2's Q_size scores child MASS balance when a density_field is given.

    Rectangle [0,4]x[0,1] with density 3.0 for x < 1 and 1.0 elsewhere: the cut
    at x=1 balances mass (3 vs 3) but not area (1 vs 3); the cut at x=2
    balances area (2 vs 2) but not mass (4 vs 2). Size-only weights isolate
    Q_size (established pattern above), so the argmax must flip between area
    mode and density-mass mode. This pins the scoring mechanism independently
    of the termination gate.
    """
    rect = Polygon([(0.0, 0.0), (4.0, 0.0), (4.0, 1.0), (0.0, 1.0)])
    # 2x8 raster, cell 0.5, covering [0,4]x[0,1]; column centers 0.25..3.75.
    density = np.full((2, 8), 1.0)
    density[:, :2] = 3.0  # centers x = 0.25, 0.75 -> the x < 1 strip
    field = RasterDensityField(density=density, x0=0.0, y0=0.0, cell=0.5)
    mass_balanced_cut = LineString([(1.0, -0.1), (1.0, 1.1)])
    area_balanced_cut = LineString([(2.0, -0.1), (2.0, 1.1)])
    size_only = ChenSplitWeights(size=1.0, regularity=0.0, access=0.0)

    def score(cut: LineString, density_field: RasterDensityField | None) -> float:
        scored = _score_candidate(
            rect,
            cut,
            [],
            min_parcel_area=0.05,
            split_weights=size_only,
            density_field=density_field,
        )
        assert scored is not None
        return scored[0]

    # Sanity: the construction disagrees (area mode prefers the x=2 cut).
    assert score(area_balanced_cut, None) > score(mass_balanced_cut, None)
    # Density-mass mode must flip the preference to the mass-balanced cut ...
    assert score(mass_balanced_cut, field) > score(area_balanced_cut, field)
    # ... with exact mass ratios (3/3=1.0, 2/4=0.5), not area ratios (1/3, 1).
    assert score(mass_balanced_cut, field) == pytest.approx(1.0)
    assert score(area_balanced_cut, field) == pytest.approx(0.5)


def _uniform_density_field(geom: Polygon, *, cells: int = 32) -> RasterDensityField:
    """Uniform density 1.0 raster covering ``geom``'s bounding box."""
    minx, miny, maxx, maxy = geom.bounds
    cell = max(maxx - minx, maxy - miny) / (cells - 1)
    return RasterDensityField(
        density=np.full((cells, cells), 1.0), x0=minx, y0=miny, cell=cell
    )


def test_mass_gate_governs_termination_and_tracks_total_mass() -> None:
    """The mass gate (not the geometric floor) terminates splitting.

    Uniform density over the square: the geometric floor (parcel_count=48)
    would allow ~48-64 parcels, but the mass gate must stop binary splitting
    once every parcel's mass drops below 2*max_parcel_mass, so the emergent
    district count tracks N = total_mass / max_parcel_mass (loose band; the
    targets total/6 and total/12 sit off the power-of-two boundaries the
    square's near-perfect halving produces, so termination is unambiguous)
    and shrinking max_parcel_mass must increase the count. This pins the gate
    mechanism independently of the Eq. 2 scoring term.
    """
    boundary = boundary_preset("square")
    field = _uniform_density_field(boundary.geom)
    total_mass = field.mass(boundary.geom)
    floor_count = 48  # floor fine enough that only the mass gate can stop early

    def emergent_count(target_districts: float) -> int:
        generated = generate_layout_for_boundary(
            boundary,
            parcel_count=floor_count,
            seed=0,
            density_field=field,
            max_parcel_mass=total_mass / target_districts,
        )
        assert generated.metrics["density_mass_mode"] is True
        return len(generated.layout.mesh.parcels)

    count_coarse = emergent_count(6.0)
    count_fine = emergent_count(12.0)

    # Loose band around N = total_mass / max_parcel_mass. Without the gate both
    # runs would grind down to the geometric floor (~48-64 parcels).
    assert 0.4 * 6.0 <= count_coarse <= 1.2 * 6.0, f"coarse={count_coarse}"
    assert 0.4 * 12.0 <= count_fine <= 1.2 * 12.0, f"fine={count_fine}"
    assert count_fine > count_coarse


@pytest.mark.slow
def test_density_mass_mode_makes_dense_regions_smaller() -> None:
    """Density-mass mode subdivides the dense corner finer than the sparse fringe.

    Concretely: with a corner density blob, the Spearman correlation between a
    parcel's mean density and its area must be clearly more negative under the
    density-mass criterion than under stock geometric-area splitting (which has
    no reason to make dense parcels smaller).
    """
    from scipy.stats import spearmanr

    boundary = boundary_preset("square")
    field = _corner_blob_density_field(boundary.geom)
    total_mass = field.mass(boundary.geom)
    # Geometric floor fine enough that the mass gate, not the floor, governs;
    # mass target ~16 districts' worth of worlds.
    floor_count = 48
    max_parcel_mass = total_mass / 16.0

    def density_area_corr(generated) -> float:
        areas: list[float] = []
        mean_densities: list[float] = []
        for parcel in generated.layout.mesh.parcels.values():
            area = float(parcel.geom.area)
            if area <= 0.0:
                continue
            areas.append(area)
            mean_densities.append(field.mass(parcel.geom) / area)
        rho, _ = spearmanr(mean_densities, areas)
        return float(rho)

    area_mode = generate_layout_for_boundary(boundary, parcel_count=floor_count, seed=0)
    density_mode = generate_layout_for_boundary(
        boundary,
        parcel_count=floor_count,
        seed=0,
        density_field=field,
        max_parcel_mass=max_parcel_mass,
    )

    assert density_mode.metrics["density_mass_mode"] is True
    assert density_mode.metrics["max_parcel_mass"] == pytest.approx(max_parcel_mass)
    assert density_mode.metrics["geometry_valid_pass"] is True

    rho_area = density_area_corr(area_mode)
    rho_density = density_area_corr(density_mode)
    # Density mode chases density: dense parcels get smaller, sparse stay large.
    assert rho_density < rho_area
    assert rho_density < -0.2


# ---------------------------------------------------------------------------
# Paper Table 1 metrics wiring
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", SHAPES)
def test_paper_table1_metric_keys_present(name: str) -> None:
    generated = generate_named_layout(name, parcel_count=12, seed=0)
    m = generated.metrics
    for key in (
        "parcel_quad_count",
        "parcel_total_count",
        "parcel_quad_fraction",
        "irregularity_min",
        "irregularity_max",
        "irregularity_avg",
        "street_count",
        "street_junction_count",
        "street_end_count",
        "street_length_total",
        "street_length_avg",
        "junction_angle_dev_from_90_avg",
        "max_hierarchical_level",
        "boundary_perimeter_world",
    ):
        assert key in m, f"missing metric key: {key}"
    assert m["boundary_perimeter_world"] > 0.0


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def test_generation_diagnostics_shape() -> None:
    generated = generate_named_layout("square", parcel_count=12, seed=0)
    m = generated.metrics
    assert m["requested_parcel_count"] == 12
    assert m["min_parcel_area"] > 0.0
    assert m["seed"] == 0
    assert m["level_count"] == m["max_hierarchical_level"] + 1
    assert len(m["splits_per_level"]) == m["max_hierarchical_level"]
    assert m["accepted_split_count"] == sum(m["splits_per_level"])
    assert m["weld_applied_count"] >= 0
    assert m["weld_rejected_count"] >= 0
    assert isinstance(m["street_extension_diagnostics_per_level"], tuple)
    assert m["optimization_applied"]
    assert m["optimization_energy_after"] <= m["optimization_energy_before"]


def test_min_parcel_area_input_path() -> None:
    boundary = boundary_preset("square")
    min_area = boundary.geom.area / 30.0
    generated = generate_layout_for_boundary(boundary, min_parcel_area=min_area, seed=0)
    assert generated.metrics["requested_parcel_count"] is None
    assert generated.metrics["min_parcel_area"] == pytest.approx(min_area)
    assert generated.metrics["geometry_valid_pass"]
    assert generated.metrics["paper_invariant_pass"]


# ---------------------------------------------------------------------------
# Yang ablation modes (smoke)
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.parametrize(
    "mode",
    [STREAMLINE_MODE_YANG_D_FIELD, STREAMLINE_MODE_YANG_B_FIELD],
)
def test_yang_modes_run_and_pass_invariants(mode: str) -> None:
    generated = generate_named_layout(
        "oval", parcel_count=24, seed=0, streamline_mode=mode
    )
    m = generated.metrics
    assert m["geometry_valid_pass"]
    assert m["paper_invariant_pass"]
    assert m["unreachable_parcel_count"] == 0
    assert m["street_graph_component_count"] == 1
    assert m["streamline_config_mode"] == mode


def test_custom_streamline_config_rejects_non_baseline_mode() -> None:
    from mapgen.chen_field import StreamlineConfig

    boundary = boundary_preset("square")
    with pytest.raises(ValueError):
        generate_layout_for_boundary(
            boundary,
            parcel_count=12,
            streamline_mode=STREAMLINE_MODE_YANG_D_FIELD,
            streamline_config=StreamlineConfig(),
        )


def test_unknown_streamline_mode_raises() -> None:
    boundary = boundary_preset("square")
    with pytest.raises(ValueError):
        generate_layout_for_boundary(
            boundary, parcel_count=12, streamline_mode="nonsense"
        )


def test_boundary_spec_dataclass() -> None:
    spec = boundary_preset("triangle")
    assert isinstance(spec, BoundarySpec)
    assert spec.name == "triangle"
    assert spec.geom.area > 0.0
