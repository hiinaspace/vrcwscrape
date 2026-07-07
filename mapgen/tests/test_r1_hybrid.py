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
import shapely.geometry as sg

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

# ty (unlike pytest at runtime) doesn't see the sys.path.insert above, so it
# can't resolve this as a first-party module -- same reason a `noqa: E402` is
# needed for the runtime import-position lint.
from run_r1_hybrid import (  # ty: ignore[unresolved-import]  # noqa: E402
    BlockResult,
    _select_seam_block,
    assign_worlds_excluding,
    export_greybox,
    junctions_to_geojson,
    plaza_district_ids,
    run_all_blocks,
)

from mapgen.r1_arm_a import IslandFields  # noqa: E402
from mapgen.r1_connect import SeamJunction  # noqa: E402
from mapgen.r1_lots import assign_worlds_to_districts  # noqa: E402
from mapgen.r1_macro import MacroLayer, MacroParams  # noqa: E402

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
