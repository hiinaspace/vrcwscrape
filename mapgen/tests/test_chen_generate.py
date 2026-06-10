"""Tests for the Chen 2024 Section 4 hierarchical co-generation driver.

These exercise the level-synchronized loop: emergent parcel counts within a
calibration band, geometry/paper invariants, street reachability, min-area
termination, determinism, the Eq. 2 Q_acce scorer, and the Yang ablation modes.
Counts are emergent (the paper-faithful input is ``min_parcel_area``), so all
count assertions use ranges rather than exact values.
"""

from __future__ import annotations

import pytest
from shapely import LineString, Polygon

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
