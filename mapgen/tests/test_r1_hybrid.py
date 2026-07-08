"""Tests for the pure/orchestration helpers in ``scripts/run_r1_hybrid.py``.

``scripts/`` is not an installed package, so its directory is added to
``sys.path`` here (mirroring how ``run_r1_hybrid.py`` itself extends
``sys.path`` for ``src/``) before importing it as a plain module.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import shapely.geometry as sg

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

# ty (unlike pytest at runtime) doesn't see the sys.path.insert above, so it
# can't resolve this as a first-party module -- same reason a `noqa: E402` is
# needed for the runtime import-position lint.
import run_r1_hybrid  # ty: ignore[unresolved-import]  # noqa: E402
from run_r1_hybrid import (  # ty: ignore[unresolved-import]  # noqa: E402
    S6_BASE_GUIDANCE_STRENGTH,
    S6_GUIDANCE_STRENGTH_FLOOR,
    BlockResult,
    _block_mean_density,
    _clip_footprints_to_ribbon,
    _fillet_highway_and_ring_lines,
    _fronting_road_segments,
    _resolve_guidance_strength,
    _s7c_road_ribbons,
    _select_seam_block,
    _zone_for_district,
    assign_worlds_excluding,
    block_norm_density,
    export_greybox,
    graded_guidance_strength,
    junctions_to_geojson,
    landmark_ids_by_nucleus,
    landmark_quotas_by_nucleus,
    plaza_district_ids,
    run_all_blocks,
)

from mapgen.r1_arm_a import IslandFields  # noqa: E402
from mapgen.r1_connect import SeamJunction  # noqa: E402
from mapgen.r1_lots import Lot, MassingConfig, assign_worlds_to_districts  # noqa: E402
from mapgen.r1_macro import (  # noqa: E402
    MacroEdge,
    MacroLayer,
    MacroParams,
    NucleusSpec,
)
from mapgen.r1_mesh import (  # noqa: E402
    DEFAULT_STREET_WIDTHS,
    buffer_ribbon,
    fillet_centerline,
)
from mapgen.r1_seam import ChenInBlockResult  # noqa: E402

# ---------------------------------------------------------------------------
# junctions_to_geojson
# ---------------------------------------------------------------------------


def _junction(
    *,
    block_id: int = 0,
    x: float = 1.0,
    y: float = 2.0,
    kind: str = "arterial",
    macro_index: int = 3,
    tier: int = 2,
    station: float = 5.0,
    distance: float = 0.0,
) -> SeamJunction:
    return SeamJunction(
        block_id=block_id,
        x=x,
        y=y,
        kind=kind,
        macro_index=macro_index,
        tier=tier,
        station=station,
        distance=distance,
    )


def test_junctions_to_geojson_empty_is_valid_feature_collection() -> None:
    result = junctions_to_geojson([])
    assert result == {"type": "FeatureCollection", "features": []}


def test_junctions_to_geojson_round_trips_coordinates_and_properties() -> None:
    junctions = [
        _junction(block_id=0, x=1.0, y=2.0, kind="arterial", tier=2, macro_index=3),
        _junction(block_id=1, x=-4.5, y=6.25, kind="ring", tier=-1, macro_index=0),
        _junction(
            block_id=2,
            x=0.0,
            y=0.0,
            kind="unmatched",
            tier=-1,
            macro_index=-1,
            distance=0.3,
        ),
    ]
    result = junctions_to_geojson(junctions)

    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 3
    for feature, junction in zip(result["features"], junctions, strict=True):
        assert feature["type"] == "Feature"
        assert feature["geometry"] == {
            "type": "Point",
            "coordinates": [junction.x, junction.y],
        }
        assert feature["properties"] == {
            "block_id": junction.block_id,
            "kind": junction.kind,
            "tier": junction.tier,
            "macro_index": junction.macro_index,
            "distance": junction.distance,
        }


def test_junctions_to_geojson_preserves_input_order() -> None:
    junctions = [_junction(block_id=i) for i in (5, 1, 3)]
    result = junctions_to_geojson(junctions)
    block_ids = [f["properties"]["block_id"] for f in result["features"]]
    assert block_ids == [5, 1, 3]


# ---------------------------------------------------------------------------
# _select_seam_block
# ---------------------------------------------------------------------------
#
# Five blocks laid out as disjoint unit squares at x=0,2,4,6,8 (ids 0..4), so
# each block's own representative_point() sits inside only itself -- a block
# is "core" iff it is (or is buffer(1e-6)-contained by) one of core_polys.


def _seam_blocks() -> list[sg.Polygon]:
    return [sg.box(x0, 0.0, x0 + 1.0, 1.0) for x0 in (0.0, 2.0, 4.0, 6.0, 8.0)]


def test_select_seam_block_odd_length_is_true_median() -> None:
    blocks = _seam_blocks()
    # counts by block id: [5, 1, 1, 9, 0] -- sorted [0, 1, 1, 5, 9], true
    # median (index 2) is 1, achieved at block_id 2 (the smaller of the two
    # count=1 ids since ties break on block_id).
    tjunctions_by_block = {0: 5, 1: 1, 2: 1, 3: 9}  # block 4 implicitly 0
    block_id, count = _select_seam_block(blocks, [], tjunctions_by_block)
    assert (block_id, count) == (2, 1)


def test_select_seam_block_even_length_is_upper_middle_tie_broken_by_id() -> None:
    blocks = _seam_blocks()
    # Block 2 is core (excluded) -> 4 non-core candidates (0, 1, 3, 4) with
    # counts [3, 1, 0, 2] -- ranked by (count, id): (0,3),(1,1),(2,4),(3,0).
    # len=4 -> index len//2=2 -> the *upper*-middle element (2, 4), not the
    # lower-middle (1, 1) a naive floor-median might pick.
    core_polys = [blocks[2]]
    tjunctions_by_block = {0: 3, 1: 1, 4: 2}  # block 3 implicitly 0
    block_id, count = _select_seam_block(blocks, core_polys, tjunctions_by_block)
    assert (block_id, count) == (4, 2)


def test_select_seam_block_excludes_core_blocks_from_ranking() -> None:
    blocks = _seam_blocks()
    core_polys = [blocks[3]]  # block 3 (count=9, the outlier) is core.
    tjunctions_by_block = {0: 5, 1: 1, 2: 1, 3: 9}
    block_id, _count = _select_seam_block(blocks, core_polys, tjunctions_by_block)
    assert block_id != 3


def test_select_seam_block_falls_back_to_all_blocks_when_every_block_is_core() -> None:
    blocks = _seam_blocks()
    tjunctions_by_block = {0: 5, 1: 1, 2: 1, 3: 9}
    ranked_all_core = _select_seam_block(blocks, blocks, tjunctions_by_block)
    ranked_no_core = _select_seam_block(blocks, [], tjunctions_by_block)
    # Every block core -> non_core_ids is empty -> falls back to ranking over
    # every block, same result as passing no core_polys at all.
    assert ranked_all_core == ranked_no_core


# ---------------------------------------------------------------------------
# run_all_blocks -- stage-4 densify wiring, both max_gate_spacing branches
# ---------------------------------------------------------------------------
#
# Reuses the flat-density fixture shape from test_r1_seam.py's
# `_flat_fields`/`_square`: a 10x10 flat-density block with a mass small
# enough (max_parcel_mass=6.0) to force at least one interior street/gate
# (see test_r1_seam.test_chen_in_block_surfaces_gates_and_perimeter_flags).


def _square(x0: float, y0: float, side: float) -> sg.Polygon:
    return sg.box(x0, y0, x0 + side, y0 + side)


def _flat_fields(side: float, *, ncells: int = 40) -> IslandFields:
    shape = (ncells, ncells)
    density = np.ones(shape, dtype=float)
    flat = np.zeros(shape, dtype=float)
    return IslandFields(
        density=density,
        height=flat,
        flow_accum=flat,
        height_carved=flat,
        slope=flat,
        x0=0.0,
        y0=0.0,
        cell=side / ncells,
    )


def test_run_all_blocks_max_gate_spacing_none_leaves_streets_and_gates_untouched() -> (
    None
):
    block = _square(0, 0, 10)
    fields = _flat_fields(10.0)
    results = run_all_blocks(
        [block],
        fields,
        max_parcel_mass=6.0,
        min_parcel_area=10.0 * 10.0 / (40 * 4),
        max_gate_spacing=None,
    )
    assert len(results) == 1
    result = results[0]
    assert not result.failed
    assert result.n_connectors == 0
    assert len(result.street_perimeter_flags) == len(result.streets)


def test_run_all_blocks_max_gate_spacing_set_appends_connectors_gates_and_flags() -> (
    None
):
    block = _square(0, 0, 10)
    fields = _flat_fields(10.0)
    min_parcel_area = 10.0 * 10.0 / (40 * 4)

    baseline = run_all_blocks(
        [block],
        fields,
        max_parcel_mass=6.0,
        min_parcel_area=min_parcel_area,
        max_gate_spacing=None,
    )[0]
    densified = run_all_blocks(
        [block],
        fields,
        max_parcel_mass=6.0,
        min_parcel_area=min_parcel_area,
        max_gate_spacing=5.0,
    )[0]

    assert not densified.failed
    assert densified.n_connectors > 0
    # Every connector street + gate is APPENDED on top of the organic ones.
    assert len(densified.streets) == len(baseline.streets) + densified.n_connectors
    assert len(densified.gates) == len(baseline.gates) + densified.n_connectors
    # perimeter_flags stays index-aligned with streets (build_unified_street_
    # graph's zip(streets, flags, strict=True) depends on this).
    assert len(densified.street_perimeter_flags) == len(densified.streets)
    # The appended flags are exactly [False] * n_connectors (a connector is
    # never a perimeter-duplicate street).
    assert densified.street_perimeter_flags[len(baseline.streets) :] == (
        [False] * densified.n_connectors
    )
    # Connector gates use synthetic negative street_id (mapgen.r1_connect.
    # densify_gates's contract) -- distinguishable from organic gates.
    new_gates = densified.gates[len(baseline.gates) :]
    assert all(g.street_id < 0 for g in new_gates)


# ---------------------------------------------------------------------------
# Slice S6: density-graded street guidance
# (block_norm_density / graded_guidance_strength / _resolve_guidance_strength)
# ---------------------------------------------------------------------------


def _gradient_fields(
    *, nrows: int = 10, ncols: int = 30, cell: float = 1.0
) -> IslandFields:
    """Density increases with column (x): cell (r, c) density == c.

    Cell centers land at x = c + 0.5, y = r + 0.5 (x0=y0=0, cell=1.0), and the
    density VALUE at column c is c itself (not its center x-coordinate), so a
    block spanning columns [lo, hi) has a known, hand-computable mean density
    of (lo + hi - 1) / 2.
    """
    density = np.tile(np.arange(ncols, dtype=float), (nrows, 1))
    flat = np.zeros((nrows, ncols), dtype=float)
    return IslandFields(
        density=density,
        height=flat,
        flow_accum=flat,
        height_carved=flat,
        slope=flat,
        x0=0.0,
        y0=0.0,
        cell=cell,
    )


def test_block_mean_density_matches_hand_computed_column_average() -> None:
    fields = _gradient_fields()
    block = _square(0, 0, 5)  # columns 0..4 (density values 0..4) -> mean 2.0
    assert _block_mean_density(block, fields) == pytest.approx(2.0)


def test_block_norm_density_min_max_and_monotonic_mid() -> None:
    fields = _gradient_fields()
    low = _square(0, 0, 5)  # columns 0..4    -> mean 2.0   (least dense)
    mid = sg.box(10, 0, 20, 10)  # columns 10..19 -> mean 14.5
    high = sg.box(24, 0, 30, 10)  # columns 24..29 -> mean 26.5 (most dense)

    norm = block_norm_density([low, mid, high], fields)

    assert norm[0] == pytest.approx(0.0)
    assert norm[2] == pytest.approx(1.0)
    assert 0.0 < norm[1] < 1.0
    # Monotonic: denser block -> higher norm_density.
    assert norm[0] < norm[1] < norm[2]


def test_block_norm_density_uniform_field_is_all_zero() -> None:
    fields = _flat_fields(10.0)
    blocks = [_square(0, 0, 5), _square(5, 0, 5), _square(0, 5, 5)]
    norm = block_norm_density(blocks, fields)
    assert norm == pytest.approx([0.0, 0.0, 0.0])


def test_block_norm_density_empty_blocks_is_empty() -> None:
    assert block_norm_density([], _flat_fields(10.0)) == []


def test_graded_guidance_strength_sparsest_block_is_unchanged_baseline() -> None:
    assert graded_guidance_strength(0.0) == pytest.approx(S6_BASE_GUIDANCE_STRENGTH)


def test_graded_guidance_strength_densest_block_is_floor() -> None:
    assert graded_guidance_strength(1.0) == pytest.approx(S6_GUIDANCE_STRENGTH_FLOOR)


def test_graded_guidance_strength_mid_density_unclamped() -> None:
    # 0.5 -> BASE * 0.5 == 3.0, strictly between floor (1.5) and BASE (6.0).
    assert graded_guidance_strength(0.5) == pytest.approx(3.0)


def test_graded_guidance_strength_clamps_at_floor_before_reaching_zero() -> None:
    # raw = 6.0 * (1 - 0.9) = 0.6, below the 1.5 floor -> clamped up to it.
    assert graded_guidance_strength(0.9) == pytest.approx(S6_GUIDANCE_STRENGTH_FLOOR)


def test_graded_guidance_strength_monotonic_decreasing_in_density() -> None:
    sparse = graded_guidance_strength(0.1)
    mid = graded_guidance_strength(0.6)
    dense = graded_guidance_strength(0.9)
    assert sparse > mid > dense
    # Never above today's uniform baseline, never below the floor.
    for nd in (0.0, 0.1, 0.5, 0.9, 1.0):
        strength = graded_guidance_strength(nd)
        assert S6_GUIDANCE_STRENGTH_FLOOR <= strength <= S6_BASE_GUIDANCE_STRENGTH


def test_graded_guidance_strength_deterministic() -> None:
    assert graded_guidance_strength(0.37) == graded_guidance_strength(0.37)


def test_block_norm_density_deterministic() -> None:
    fields = _gradient_fields()
    blocks = [_square(0, 0, 5), sg.box(10, 0, 20, 10), sg.box(24, 0, 30, 10)]
    first = block_norm_density(blocks, fields)
    second = block_norm_density(blocks, fields)
    assert first == second


def test_uniform_density_synthetic_collapses_grading_to_base_for_every_block() -> None:
    """Contract: a flat density field grades every block to ~BASE (today's
    pre-S6 uniform behavior), not some arbitrary mid-range value."""
    fields = _flat_fields(10.0)
    blocks = [_square(0, 0, 5), _square(5, 0, 5), _square(0, 5, 5)]
    norm = block_norm_density(blocks, fields)
    strengths = [graded_guidance_strength(nd) for nd in norm]
    assert strengths == pytest.approx([S6_BASE_GUIDANCE_STRENGTH] * 3)


def test_resolve_guidance_strength_float_is_uniform() -> None:
    assert _resolve_guidance_strength(4.5, 0) == 4.5
    assert _resolve_guidance_strength(4.5, 3) == 4.5


def test_resolve_guidance_strength_sequence_is_indexed() -> None:
    values = [1.0, 2.0, 3.0]
    assert _resolve_guidance_strength(values, 0) == 1.0
    assert _resolve_guidance_strength(values, 2) == 3.0


def test_resolve_guidance_strength_callable_is_invoked_with_block_id() -> None:
    calls: list[int] = []

    def strength_fn(block_id: int) -> float:
        calls.append(block_id)
        return float(block_id) * 2.0

    assert _resolve_guidance_strength(strength_fn, 5) == 10.0
    assert calls == [5]


def test_run_all_blocks_threads_per_block_guidance_strength(monkeypatch) -> None:
    """``run_all_blocks`` passes the per-block resolved strength through to
    ``chen_in_block`` (spied via monkeypatch so the test stays fast/cheap --
    the actual grading math is covered by the pure-function tests above)."""
    captured: list[float] = []

    def _fake_chen_in_block(block, fields, **kwargs):  # noqa: ANN001, ANN003
        captured.append(kwargs["guidance_strength"])
        return ChenInBlockResult(generated=None, info={"seed_used": "all_failed"})

    monkeypatch.setattr(run_r1_hybrid, "chen_in_block", _fake_chen_in_block)

    blocks = [_square(0, 0, 5), _square(5, 0, 5), _square(0, 5, 5)]
    fields = _flat_fields(10.0)
    run_all_blocks(
        blocks,
        fields,
        max_parcel_mass=6.0,
        min_parcel_area=1.0,
        guidance_strength=[1.5, 3.0, 6.0],
    )
    assert captured == [1.5, 3.0, 6.0]


def test_run_all_blocks_default_guidance_strength_is_pre_s6_uniform_baseline(
    monkeypatch,
) -> None:
    """No ``guidance_strength`` argument -> every block gets
    ``S6_BASE_GUIDANCE_STRENGTH`` (== ``chen_in_block``'s own pre-S6 default),
    i.e. byte-identical to the pre-S6 uniform call."""
    captured: list[float] = []

    def _fake_chen_in_block(block, fields, **kwargs):  # noqa: ANN001, ANN003
        captured.append(kwargs["guidance_strength"])
        return ChenInBlockResult(generated=None, info={"seed_used": "all_failed"})

    monkeypatch.setattr(run_r1_hybrid, "chen_in_block", _fake_chen_in_block)

    blocks = [_square(0, 0, 5), _square(5, 0, 5)]
    fields = _flat_fields(10.0)
    run_all_blocks(blocks, fields, max_parcel_mass=6.0, min_parcel_area=1.0)
    assert captured == [S6_BASE_GUIDANCE_STRENGTH, S6_BASE_GUIDANCE_STRENGTH]


# ---------------------------------------------------------------------------
# Slice P: plaza districts -- identification, world-assignment exclusion, and
# the end-to-end park flow through the greybox export.
# ---------------------------------------------------------------------------


def test_plaza_district_ids_cover_frac_threshold() -> None:
    """>= 50% of the district's area inside ONE plaza poly counts; less doesn't."""
    districts = [
        sg.box(0.0, 0.0, 4.0, 4.0),  # exactly 50% covered by the plaza below.
        sg.box(4.0, 0.0, 8.0, 4.0),  # 25% covered -- NOT a plaza district.
        sg.box(0.0, 4.0, 4.0, 8.0),  # untouched by any plaza.
    ]
    plaza = sg.box(0.0, 0.0, 2.0, 4.0).union(sg.box(4.0, 0.0, 5.0, 4.0))
    # A single plaza member covering 50% of district 0 but only 25% of
    # district 1 (the >=50% rule is per-district, against ANY one member).
    assert plaza_district_ids(districts, [sg.box(0.0, 0.0, 2.0, 4.0)]) == frozenset({0})
    # A (weird, multipart-ish) bigger plaza member still only flags where
    # coverage crosses the threshold.
    assert plaza_district_ids(districts, [plaza]) == frozenset({0})


def test_plaza_district_ids_handles_many_districts_per_plaza() -> None:
    """Chen may subdivide a plaza block: EVERY covered sub-district is flagged."""
    districts = [
        sg.box(0.0, 0.0, 1.0, 2.0),  # left half of the plaza.
        sg.box(1.0, 0.0, 2.0, 2.0),  # right half of the plaza.
        sg.box(5.0, 0.0, 7.0, 2.0),  # unrelated fabric district.
    ]
    assert plaza_district_ids(districts, [sg.box(0.0, 0.0, 2.0, 2.0)]) == frozenset(
        {0, 1}
    )


def test_plaza_district_ids_no_plazas_is_empty() -> None:
    assert plaza_district_ids([sg.box(0.0, 0.0, 1.0, 1.0)], []) == frozenset()


def _assignment_points() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "world_id": ["w_in_fabric", "w_in_plaza"],
            "x": [5.0, 15.0],
            "y": [5.0, 5.0],
            "visits": [10, 20],
        }
    )


def test_assign_worlds_excluding_snaps_plaza_worlds_to_nearest_non_plaza() -> None:
    """A world inside an excluded (plaza) district snaps OUT of it."""
    districts = [sg.box(0.0, 0.0, 10.0, 10.0), sg.box(10.0, 0.0, 20.0, 10.0)]
    assignment, kinds = assign_worlds_excluding(
        _assignment_points(), districts, frozenset({1})
    )
    # Both worlds land in district 0: the fabric one directly, the plaza one
    # snapped to the nearest non-excluded district.
    assert assignment == {0: [0, 1], 1: []}
    assert kinds == ["direct", "snapped"]


def test_assign_worlds_excluding_nothing_excluded_matches_plain_assignment() -> None:
    districts = [sg.box(0.0, 0.0, 10.0, 10.0), sg.box(10.0, 0.0, 20.0, 10.0)]
    points = _assignment_points()
    assert assign_worlds_excluding(points, districts, frozenset()) == (
        assign_worlds_to_districts(points, districts)
    )


def _plaza_export_fixture() -> tuple[
    sg.Polygon, MacroLayer, list[BlockResult], pl.DataFrame
]:
    """Two districts in one block; the right one IS a plaza disc's district.

    Geometry-only stand-ins: the MacroLayer carries empty macro layers except
    ``plaza_polys`` (all ``export_greybox`` reads from it besides ``nuclei``),
    and the single BlockResult carries the two district polygons directly.
    """
    boundary = sg.box(0.0, 0.0, 20.0, 10.0)
    fabric = sg.box(0.0, 0.0, 10.0, 10.0)
    plaza_district = sg.box(10.0, 0.0, 20.0, 10.0)
    layer = MacroLayer(
        params=MacroParams(),
        nodes=[],
        cost=np.zeros((2, 2)),
        raw_arterial_lines=[],
        raw_edges=[],
        core_polys=[],
        ring_lines=[],
        arterial_lines=[],
        edges=[],
        blocks=[boundary],
        nuclei=[],
        plaza_polys=[plaza_district],
    )
    result = BlockResult(
        block_id=0,
        districts=[fabric, plaza_district],
        streets=[],
        failed=False,
        seed_used=0,
        district_count=2,
        geometry_valid_pass=True,
        paper_invariant_pass=True,
        seconds=0.0,
        gates=[],
        street_perimeter_flags=[],
        n_connectors=0,
    )
    points = pl.DataFrame(
        {
            "world_id": ["wa", "wb", "wc", "w_plaza"],
            "x": [2.0, 5.0, 8.0, 15.0],  # w_plaza sits INSIDE the plaza disc.
            "y": [2.0, 5.0, 8.0, 5.0],
            "visits": [10, 20, 30, 40],
        }
    )
    return boundary, layer, [result], points


def test_export_greybox_plaza_district_flows_as_park_end_to_end(tmp_path) -> None:
    """Slice P pin: plaza district -> kind/typology park, zero worlds, NO lots."""
    import json

    boundary, layer, results, points = _plaza_export_fixture()
    out_dir = tmp_path / "greybox"
    manifest = export_greybox(
        out_dir,
        tmp_path,  # no inputs_manifest.json here -- island_frame stays empty.
        boundary=boundary,
        layer=layer,
        results=results,
        points=points,
    )

    counts = manifest["counts"]
    assert counts["n_districts"] == 2
    assert counts["n_plaza_districts"] == 1
    assert counts["n_park_districts"] == 1
    assert counts["n_fabric_districts"] == 1
    # The plaza-interior world snapped OUT (to the fabric district).
    assert counts["n_direct"] == 3
    assert counts["n_snapped"] == 1

    with (out_dir / "districts.geojson").open() as f:
        districts_gj = json.load(f)
    by_id = {
        feat["properties"]["district_id"]: feat["properties"]
        for feat in districts_gj["features"]
    }
    assert by_id[1]["kind"] == "park"
    assert by_id[1]["typology"] == "park"
    assert by_id[1]["world_count"] == 0
    assert by_id[0]["kind"] == "fabric"
    assert by_id[0]["world_count"] == 4

    lots = pl.read_parquet(out_dir / "lots.parquet")
    # NO lots (occupied or greenspace) inside the plaza district; every world
    # -- including the snapped plaza one -- got a lot in the fabric district.
    assert set(lots["district_id"].to_list()) == {0}
    occupied = lots.filter(pl.col("kind") == "lot")
    assert set(occupied["world_id"].to_list()) == {"wa", "wb", "wc", "w_plaza"}


# ---------------------------------------------------------------------------
# S5: zone-graded massing + per-nucleus landmark quotas
# (docs/macro-roads-nuclei-plan.md slice S5)
# ---------------------------------------------------------------------------


def _nucleus(
    *,
    anchor: tuple[float, float] = (0.0, 0.0),
    mass: float = 100.0,
    is_major: bool = True,
    influence_radius: float = 10.0,
    rank: int = 1,
) -> NucleusSpec:
    return NucleusSpec(
        anchor=anchor,
        polygon=sg.Point(anchor).buffer(1.0),
        mass=mass,
        rank=rank,
        label=None,
        influence_radius=influence_radius,
        is_major=is_major,
    )


def test_zone_for_district_none_nucleus_is_fringe() -> None:
    massing = MassingConfig()
    assert _zone_for_district(None, None, [], massing) == "fringe"


def test_zone_for_district_minor_nucleus_is_always_fringe() -> None:
    massing = MassingConfig()
    minor = _nucleus(is_major=False)
    # Even a tiny nucleus_dist (right at the anchor) stays fringe -- only
    # MAJOR-nucleus districts ever grade up.
    assert _zone_for_district(0, 0.0, [minor], massing) == "fringe"


def test_zone_for_district_major_nucleus_thresholds() -> None:
    massing = MassingConfig()
    major = _nucleus(is_major=True)
    assert _zone_for_district(0, 0.0, [major], massing) == "core"
    assert _zone_for_district(0, massing.core_zone_max_dist, [major], massing) == "core"
    just_past_core = massing.core_zone_max_dist + 1e-6
    assert _zone_for_district(0, just_past_core, [major], massing) == "inner"
    assert (
        _zone_for_district(0, massing.inner_zone_max_dist, [major], massing) == "inner"
    )
    just_past_inner = massing.inner_zone_max_dist + 1e-6
    assert _zone_for_district(0, just_past_inner, [major], massing) == "fringe"
    assert _zone_for_district(0, 1.0, [major], massing) == "fringe"


def test_landmark_quotas_by_nucleus_mass_proportional_and_sums_near_budget() -> None:
    nuclei = [
        _nucleus(mass=600.0, is_major=True, rank=1),
        _nucleus(mass=300.0, is_major=True, rank=2),
        _nucleus(mass=100.0, is_major=True, rank=3),
        _nucleus(mass=1000.0, is_major=False, rank=4),  # minor -- excluded
    ]
    quotas = landmark_quotas_by_nucleus(nuclei, 100)
    assert set(quotas) == {0, 1, 2}
    assert quotas[0] == 60
    assert quotas[1] == 30
    assert quotas[2] == 10
    assert sum(quotas.values()) == 100  # exact here (round numbers)
    # "Within rounding" of the budget in general -- exact in this fixture.
    assert abs(sum(quotas.values()) - 100) <= len(quotas)


def test_landmark_quotas_by_nucleus_deterministic() -> None:
    nuclei = [_nucleus(mass=7.0, rank=1), _nucleus(mass=13.0, rank=2)]
    first = landmark_quotas_by_nucleus(nuclei, 50)
    second = landmark_quotas_by_nucleus(nuclei, 50)
    assert first == second


def test_landmark_quotas_by_nucleus_no_majors_or_zero_budget_is_empty() -> None:
    assert landmark_quotas_by_nucleus([_nucleus(is_major=False)], 100) == {}
    assert landmark_quotas_by_nucleus([_nucleus(is_major=True)], 0) == {}
    assert landmark_quotas_by_nucleus([], 100) == {}


def test_landmark_ids_by_nucleus_grouping_and_minor_exclusion() -> None:
    """A high-visits world in a MAJOR nucleus's district becomes a landmark;
    the same-visits world in a MINOR nucleus's district does not."""
    points = pl.DataFrame(
        {
            "world_id": ["major_hi", "major_lo", "minor_hi", "minor_lo"],
            "visits": [1000, 1, 1000, 1],
        }
    )
    # district 0 -> major nucleus 0 (rows 0, 1); district 1 -> minor nucleus 1
    # (rows 2, 3).
    assignment = {0: [0, 1], 1: [2, 3]}
    nucleus_by_district: list[tuple[int | None, float | None]] = [
        (0, 0.1),
        (1, 0.1),
    ]
    # Only the major nucleus (0) has a quota -- landmark_quotas_by_nucleus
    # never gives a minor one, so the minor's worlds are never grouped.
    quotas = {0: 1}
    result = landmark_ids_by_nucleus(points, assignment, nucleus_by_district, quotas)
    assert set(result) == {0}
    assert result[0] == frozenset({"major_hi"})


def test_landmark_ids_by_nucleus_empty_quotas_is_empty() -> None:
    points = pl.DataFrame({"world_id": ["a"], "visits": [1]})
    assert landmark_ids_by_nucleus(points, {0: [0]}, [(0, 0.0)], {}) == {}


def test_landmark_ids_by_nucleus_quota_never_exceeded() -> None:
    points = pl.DataFrame(
        {"world_id": [f"w{i}" for i in range(10)], "visits": list(range(10))}
    )
    assignment = {0: list(range(10))}
    nucleus_by_district: list[tuple[int | None, float | None]] = [(0, 0.0)]
    result = landmark_ids_by_nucleus(points, assignment, nucleus_by_district, {0: 3})
    assert len(result[0]) == 3
    # Top-3 by visits: w9, w8, w7.
    assert result[0] == frozenset({"w9", "w8", "w7"})


def _zone_export_fixture() -> tuple[
    sg.Polygon, MacroLayer, list[BlockResult], pl.DataFrame
]:
    """One district near a MAJOR nucleus (-> "core"), one near a MINOR
    nucleus (-> "fringe", since only major-nucleus districts grade up)."""
    boundary = sg.box(0.0, 0.0, 30.0, 10.0)
    near_major = sg.box(0.0, 0.0, 4.0, 4.0)  # centroid (2, 2)
    near_minor = sg.box(20.0, 0.0, 24.0, 4.0)  # centroid (22, 2)
    major = _nucleus(anchor=(2.0, 2.0), mass=100.0, is_major=True, rank=1)
    minor = _nucleus(anchor=(22.0, 2.0), mass=50.0, is_major=False, rank=2)
    layer = MacroLayer(
        params=MacroParams(),
        nodes=[],
        cost=np.zeros((2, 2)),
        raw_arterial_lines=[],
        raw_edges=[],
        core_polys=[],
        ring_lines=[],
        arterial_lines=[],
        edges=[],
        blocks=[boundary],
        nuclei=[major, minor],
        plaza_polys=[],
    )
    result = BlockResult(
        block_id=0,
        districts=[near_major, near_minor],
        streets=[],
        failed=False,
        seed_used=0,
        district_count=2,
        geometry_valid_pass=True,
        paper_invariant_pass=True,
        seconds=0.0,
        gates=[],
        street_perimeter_flags=[],
        n_connectors=0,
    )
    points = pl.DataFrame(
        {
            "world_id": ["major_lo", "major_hi", "minor_lo", "minor_hi"],
            "x": [1.0, 3.0, 21.0, 23.0],
            "y": [1.0, 3.0, 1.0, 3.0],
            "visits": [5, 500, 5, 500],
        }
    )
    return boundary, layer, [result], points


def test_export_greybox_zone_and_landmark_quota_end_to_end(tmp_path) -> None:
    """S5 end-to-end: the major-nucleus district grades to "core" and wins
    the landmark quota; the minor-nucleus district stays "fringe" and NEVER
    gets a landmark, regardless of visits."""
    import json

    boundary, layer, results, points = _zone_export_fixture()
    massing = MassingConfig(total_landmark_budget=1)
    out_dir = tmp_path / "greybox"
    manifest = export_greybox(
        out_dir,
        tmp_path,
        boundary=boundary,
        layer=layer,
        results=results,
        points=points,
        massing=massing,
    )

    with (out_dir / "districts.geojson").open() as f:
        districts_gj = json.load(f)
    by_district_id = {
        feat["properties"]["district_id"]: feat["properties"]
        for feat in districts_gj["features"]
    }
    assert by_district_id[0]["zone"] == "core"
    assert by_district_id[1]["zone"] == "fringe"

    zone_counts = manifest["counts"]["zone_counts"]
    assert zone_counts == {"core": 1, "inner": 0, "fringe": 1}

    lot_config = manifest["lot_config"]
    assert lot_config["landmark_quota_by_nucleus"] == {"0": 1}
    assert lot_config["landmark_count_by_nucleus"] == {"0": 1}

    lots = pl.read_parquet(out_dir / "lots.parquet")
    by_world = {row["world_id"]: row for row in lots.to_dicts()}
    # The major nucleus's high-visits world wins the (single) landmark slot
    # and gets the core landmark story band.
    assert by_world["major_hi"]["typology"] == "landmark"
    core_lo, core_hi = massing.core_landmark_stories
    lo_bound = core_lo * massing.story_height_m - massing.height_jitter_m
    hi_bound = core_hi * massing.story_height_m + massing.height_jitter_m
    assert lo_bound - 1e-6 <= by_world["major_hi"]["height"] <= hi_bound + 1e-6
    # The minor nucleus's equally-high-visits world is NEVER a landmark --
    # minor nuclei get no landmark tier by default (S5).
    assert by_world["minor_hi"]["typology"] != "landmark"


# ---------------------------------------------------------------------------
# S7b: fronting-road segment assembly (docs/macro-roads-nuclei-plan.md)
# ---------------------------------------------------------------------------


def _edge(tier: int) -> MacroEdge:
    return MacroEdge(node_a=0, node_b=1, tau=tier, tier=tier, path_cost=1.0, length=1.0)


def test_fronting_road_segments_maps_arterial_ring_and_promoted_arc_tiers() -> None:
    hwy = sg.LineString([(0.0, 0.0), (10.0, 0.0)])
    local = sg.LineString([(0.0, 5.0), (10.0, 5.0)])
    whole_ring = sg.LineString([(0.0, 0.0), (0.0, 10.0)])
    promoted_arc = sg.LineString([(0.0, 0.0), (0.0, 5.0)])  # tier 2 (highway)
    local_arc = sg.LineString([(0.0, 5.0), (0.0, 10.0)])  # tier 0 -> omitted
    layer = MacroLayer(
        params=MacroParams(),
        nodes=[],
        cost=np.zeros((2, 2)),
        raw_arterial_lines=[],
        raw_edges=[],
        core_polys=[],
        ring_lines=[whole_ring],
        arterial_lines=[hwy, local],
        edges=[_edge(2), _edge(0)],
        blocks=[],
        nuclei=[],
        plaza_polys=[],
        ring_arc_lines=[promoted_arc, local_arc],
        ring_arc_edges=[_edge(2), _edge(0)],
    )
    segments = _fronting_road_segments(layer)
    tiers = [tier for _line, tier in segments]
    # highway + local arterials, the promoted (tier 2) arc, and the whole ring;
    # the local-tier (0) arc is omitted (it coincides with the whole "ring").
    assert tiers == ["highway", "local", "highway", "ring"]


# ---------------------------------------------------------------------------
# S7c: highway/ring mesh-layer fillet + building-footprint ribbon clip
# (docs/macro-roads-nuclei-plan.md)
# ---------------------------------------------------------------------------


def test_fillet_highway_and_ring_lines_only_highway_and_ring() -> None:
    hwy = sg.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)])
    major = sg.LineString([(0.0, 5.0), (10.0, 5.0), (10.0, 15.0)])
    ring = sg.LineString(
        [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]
    )
    arc = sg.LineString([(0.0, 0.0), (5.0, 0.0), (5.0, 5.0)])
    layer = MacroLayer(
        params=MacroParams(),
        nodes=[],
        cost=np.zeros((2, 2)),
        raw_arterial_lines=[],
        raw_edges=[],
        core_polys=[],
        ring_lines=[ring],
        arterial_lines=[hwy, major],
        edges=[_edge(2), _edge(1)],
        blocks=[],
        nuclei=[],
        plaza_polys=[],
        ring_arc_lines=[arc],
        ring_arc_edges=[_edge(2)],
    )
    arterial_lines, ring_lines, ring_arc_lines = _fillet_highway_and_ring_lines(layer)
    # Highway (tier 2) is filleted -- more vertices, endpoints preserved.
    assert len(list(arterial_lines[0].coords)) > len(list(hwy.coords))
    assert list(arterial_lines[0].coords)[0] == list(hwy.coords)[0]
    assert list(arterial_lines[0].coords)[-1] == list(hwy.coords)[-1]
    # Major (tier 1) passes through UNCHANGED (same object -- leave-as-is
    # tiers, per the fix's scope).
    assert arterial_lines[1] is major
    # Every ring / ring-arc line is filleted regardless of its promoted tier
    # -- both always export "tier": "ring" (see _greybox_arterials_geojson).
    assert ring_lines[0] is not ring
    assert len(list(ring_lines[0].coords)) > len(list(ring.coords))
    assert ring_arc_lines[0] is not arc
    assert len(list(ring_arc_lines[0].coords)) > len(list(arc.coords))


def test_fillet_highway_and_ring_lines_deterministic() -> None:
    hwy = sg.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)])
    layer = MacroLayer(
        params=MacroParams(),
        nodes=[],
        cost=np.zeros((2, 2)),
        raw_arterial_lines=[],
        raw_edges=[],
        core_polys=[],
        ring_lines=[],
        arterial_lines=[hwy],
        edges=[_edge(2)],
        blocks=[],
        nuclei=[],
        plaza_polys=[],
    )
    first = _fillet_highway_and_ring_lines(layer)
    second = _fillet_highway_and_ring_lines(layer)
    assert [list(line.coords) for line in first[0]] == [
        list(line.coords) for line in second[0]
    ]


def _s7c_lot(
    *,
    world_id: str = "w",
    footprint: sg.Polygon,
    kind: str = "lot",
) -> Lot:
    return Lot(
        world_id=world_id,
        district_id=0,
        footprint=footprint,
        lot=footprint,
        height=6.0,
        name=None,
        visits=1,
        x=0.0,
        y=0.0,
        assigned="direct",
        lot_x=0.0,
        lot_y=0.0,
        kind=kind,
        displacement=0.0,
        typology="detached",
    )


def test_clip_footprints_to_ribbon_overlapping_lot_clips_flush() -> None:
    """A first-row footprint whose left half sits inside the ribbon clips
    down to the surviving (non-overlapping, smaller) piece."""
    footprint = sg.box(0.0, 0.0, 10.0, 10.0)
    ribbon = sg.box(-5.0, -5.0, 5.0, 15.0)
    lots, n_clipped, n_kept = _clip_footprints_to_ribbon(
        [_s7c_lot(footprint=footprint)], [ribbon]
    )
    assert (n_clipped, n_kept) == (1, 0)
    clipped = lots[0].footprint
    assert clipped.area < footprint.area
    # No remaining OVERLAP with the ribbon (a shared boundary edge still
    # counts as "intersects", so compare intersection area instead).
    assert clipped.intersection(ribbon).area < 1e-9


def test_clip_footprints_to_ribbon_wholly_inside_keeps_original() -> None:
    """A lot almost entirely inside a wide ribbon keeps its ORIGINAL
    footprint (a building slightly on the road beats a missing world)."""
    footprint = sg.box(0.0, 0.0, 1.0, 1.0)
    ribbon = sg.box(-10.0, -10.0, 10.0, 10.0)  # engulfs the tiny footprint
    lots, n_clipped, n_kept = _clip_footprints_to_ribbon(
        [_s7c_lot(footprint=footprint)], [ribbon]
    )
    assert (n_clipped, n_kept) == (0, 1)
    assert lots[0].footprint.equals(footprint)


def test_clip_footprints_to_ribbon_no_overlap_uncounted() -> None:
    footprint = sg.box(100.0, 100.0, 101.0, 101.0)
    ribbon = sg.box(-5.0, -5.0, 5.0, 5.0)
    lots, n_clipped, n_kept = _clip_footprints_to_ribbon(
        [_s7c_lot(footprint=footprint)], [ribbon]
    )
    assert (n_clipped, n_kept) == (0, 0)
    assert lots[0].footprint.equals(footprint)


def test_clip_footprints_to_ribbon_skips_greenspace_and_empty_footprint() -> None:
    greenspace = _s7c_lot(footprint=sg.Polygon(), kind="greenspace")
    ribbon = sg.box(-100.0, -100.0, 100.0, 100.0)
    lots, n_clipped, n_kept = _clip_footprints_to_ribbon([greenspace], [ribbon])
    assert (n_clipped, n_kept) == (0, 0)
    assert lots[0] is greenspace


def test_clip_footprints_to_ribbon_no_ribbons_is_noop() -> None:
    lot = _s7c_lot(footprint=sg.box(0.0, 0.0, 1.0, 1.0))
    lots, n_clipped, n_kept = _clip_footprints_to_ribbon([lot], [])
    assert (lots, n_clipped, n_kept) == ([lot], 0, 0)


def test_clip_footprints_to_ribbon_deterministic() -> None:
    footprint = sg.box(0.0, 0.0, 10.0, 10.0)
    ribbon = sg.box(-5.0, -5.0, 5.0, 15.0)
    lots1, *_rest1 = _clip_footprints_to_ribbon(
        [_s7c_lot(footprint=footprint)], [ribbon]
    )
    lots2, *_rest2 = _clip_footprints_to_ribbon(
        [_s7c_lot(footprint=footprint)], [ribbon]
    )
    assert lots1[0].footprint.equals(lots2[0].footprint)


def test_s7c_road_ribbons_uses_the_passed_in_filleted_lines() -> None:
    """The ribbon list must be built from whatever line the caller passes in
    (the FILLETED copy, per Fix 1) -- not re-derived from raw layer
    geometry -- so ribbon and exported/baked road always agree."""
    ring = sg.LineString(
        [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]
    )
    filleted_ring = fillet_centerline(ring)
    layer = MacroLayer(
        params=MacroParams(),
        nodes=[],
        cost=np.zeros((2, 2)),
        raw_arterial_lines=[],
        raw_edges=[],
        core_polys=[],
        ring_lines=[ring],  # raw -- must NOT be what actually gets buffered
        arterial_lines=[],
        edges=[],
        blocks=[],
        nuclei=[],
        plaza_polys=[],
    )
    ribbons = _s7c_road_ribbons(
        [], [filleted_ring], [], layer, [], DEFAULT_STREET_WIDTHS
    )
    assert len(ribbons) == 1
    expected = buffer_ribbon(filleted_ring, DEFAULT_STREET_WIDTHS["ring"])
    assert ribbons[0].equals(expected)
    # Sanity: the filleted ring's ribbon really does differ from the raw
    # ring's ribbon -- otherwise this test wouldn't exercise anything.
    assert not ribbons[0].equals(buffer_ribbon(ring, DEFAULT_STREET_WIDTHS["ring"]))


def test_export_greybox_filets_highway_not_major_arterials(tmp_path) -> None:
    """End-to-end Fix 1: arterials.geojson carries the FILLETED highway
    centerline (more vertices, endpoints pinned) but the UNCHANGED major
    one -- the fix lands at export, per docs/macro-roads-nuclei-plan.md."""
    import json

    boundary = sg.box(-20.0, -20.0, 20.0, 20.0)
    district = sg.box(-10.0, -10.0, 10.0, 10.0)
    hwy = sg.LineString([(-15.0, 0.0), (0.0, 0.0), (0.0, 15.0)])
    major = sg.LineString([(-15.0, 5.0), (0.0, 5.0), (0.0, 20.0)])
    layer = MacroLayer(
        params=MacroParams(),
        nodes=[],
        cost=np.zeros((2, 2)),
        raw_arterial_lines=[],
        raw_edges=[],
        core_polys=[],
        ring_lines=[],
        arterial_lines=[hwy, major],
        edges=[_edge(2), _edge(1)],
        blocks=[boundary],
        nuclei=[],
        plaza_polys=[],
    )
    result = BlockResult(
        block_id=0,
        districts=[district],
        streets=[],
        failed=False,
        seed_used=0,
        district_count=1,
        geometry_valid_pass=True,
        paper_invariant_pass=True,
        seconds=0.0,
        gates=[],
        street_perimeter_flags=[],
        n_connectors=0,
    )
    points = pl.DataFrame({"world_id": ["w1"], "x": [0.0], "y": [0.0], "visits": [10]})
    out_dir = tmp_path / "greybox"
    export_greybox(
        out_dir,
        tmp_path,
        boundary=boundary,
        layer=layer,
        results=[result],
        points=points,
    )
    with (out_dir / "arterials.geojson").open() as f:
        arterials_gj = json.load(f)
    by_tier: dict[str, list] = {}
    for feat in arterials_gj["features"]:
        by_tier.setdefault(feat["properties"]["tier"], []).append(feat)
    [hwy_feat] = by_tier["highway"]
    [major_feat] = by_tier["major"]
    hwy_coords = hwy_feat["geometry"]["coordinates"]
    major_coords = major_feat["geometry"]["coordinates"]
    assert len(hwy_coords) > len(list(hwy.coords))  # filleted: more vertices
    assert [tuple(c) for c in major_coords] == list(major.coords)  # unfilleted
    # Endpoints of the filleted highway stay pinned to the raw source.
    assert tuple(hwy_coords[0]) == hwy.coords[0]
    assert tuple(hwy_coords[-1]) == hwy.coords[-1]
