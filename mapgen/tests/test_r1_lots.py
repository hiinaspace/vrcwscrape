"""Tests for R1 greybox lot geometry (``mapgen.r1_lots``): world -> district
assignment and per-district Voronoi lot/footprint tessellation."""

from __future__ import annotations

import math
from typing import Any

import polars as pl
import pytest
import shapely.geometry as sg
from shapely.ops import unary_union

from mapgen.r1_lots import (
    DEFAULT_H_BASE,
    DEFAULT_H_SCALE,
    Lot,
    _footprint,
    assign_worlds_to_districts,
    build_lots,
    count_sliver_reassignments,
    select_member_points,
)

# ---------------------------------------------------------------------------
# assign_worlds_to_districts
# ---------------------------------------------------------------------------


def _grid_districts() -> list[sg.Polygon]:
    """Two side-by-side 10x10 squares: [0,10]x[0,10] and [10,20]x[0,10]."""
    return [sg.box(0.0, 0.0, 10.0, 10.0), sg.box(10.0, 0.0, 20.0, 10.0)]


def test_assign_worlds_to_districts_direct_hits() -> None:
    points = pl.DataFrame(
        {
            "world_id": ["a", "b", "c"],
            "x": [2.0, 15.0, 8.0],
            "y": [2.0, 5.0, 9.0],
            "visits": [1, 2, 3],
        }
    )
    assignment, kind = assign_worlds_to_districts(points, _grid_districts())
    assert assignment == {0: [0, 2], 1: [1]}
    assert kind == ["direct", "direct", "direct"]


def test_assign_worlds_to_districts_outside_point_snaps_to_nearest() -> None:
    points = pl.DataFrame(
        {
            "world_id": ["a", "outside"],
            "x": [5.0, 25.0],
            "y": [5.0, 5.0],
            "visits": [1, 1],
        }
    )
    assignment, kind = assign_worlds_to_districts(points, _grid_districts())
    # (25, 5) is 5 units past district 1's right edge, 15 past district 0's --
    # nearest is district 1.
    assert assignment[1] == [1]
    assert kind == ["direct", "snapped"]


def test_assign_worlds_to_districts_is_deterministic() -> None:
    points = pl.DataFrame(
        {
            "world_id": ["a", "b", "c", "outside"],
            "x": [2.0, 15.0, 8.0, 25.0],
            "y": [2.0, 5.0, 9.0, 5.0],
            "visits": [1, 2, 3, 4],
        }
    )
    districts = _grid_districts()
    first = assign_worlds_to_districts(points, districts)
    second = assign_worlds_to_districts(points, districts)
    assert first == second


def test_assign_worlds_to_districts_empty_inputs() -> None:
    empty_points = pl.DataFrame({"world_id": [], "x": [], "y": [], "visits": []})
    assignment, kind = assign_worlds_to_districts(empty_points, _grid_districts())
    assert assignment == {0: [], 1: []}
    assert kind == []

    points = pl.DataFrame({"world_id": ["a"], "x": [1.0], "y": [1.0], "visits": [1]})
    assignment2, kind2 = assign_worlds_to_districts(points, [])
    assert assignment2 == {}
    assert kind2 == ["direct"]


def test_select_member_points_preserves_order_and_attaches_assigned() -> None:
    points = pl.DataFrame(
        {
            "world_id": ["a", "b", "c"],
            "x": [1.0, 2.0, 3.0],
            "y": [1.0, 2.0, 3.0],
            "visits": [1, 2, 3],
        }
    )
    kind = ["direct", "snapped", "direct"]
    member = select_member_points(points, [2, 0], kind)
    assert member["world_id"].to_list() == ["c", "a"]
    assert member["assigned"].to_list() == ["direct", "direct"]


# ---------------------------------------------------------------------------
# build_lots — 5-point square: disjoint, tiles the district, contains generator
# ---------------------------------------------------------------------------


def _member_points(
    world_ids: list[str],
    xs: list[float],
    ys: list[float],
    visits: list[int],
    assigned: list[str] | None = None,
    names: list[str | None] | None = None,
) -> pl.DataFrame:
    data: dict[str, Any] = {
        "world_id": world_ids,
        "x": xs,
        "y": ys,
        "visits": visits,
        "assigned": assigned or ["direct"] * len(world_ids),
    }
    if names is not None:
        data["name"] = names
    return pl.DataFrame(data)


def test_build_lots_five_points_disjoint_and_tiles_district() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["a", "b", "c", "d", "e"],
        [2.0, 8.0, 2.0, 8.0, 5.0],
        [2.0, 2.0, 8.0, 8.0, 5.0],
        [0, 10, 100, 1000, 100_000_000],
    )
    lots = build_lots(district, 0, member)
    assert len(lots) == 5

    # Union of lots equals the district area (tiles it); pairwise interiors
    # are disjoint (sum of areas == union area, i.e. no double-counted area).
    union = unary_union([lot.lot for lot in lots])
    assert union.area == pytest.approx(district.area, abs=1e-6)
    assert sum(lot.lot.area for lot in lots) == pytest.approx(district.area, abs=1e-6)

    for i, lot in enumerate(lots):
        for j, other in enumerate(lots):
            if i == j:
                continue
            assert lot.lot.intersection(other.lot).area == pytest.approx(0.0, abs=1e-9)

    # Every lot contains (or at least touches, at the boundary) its own
    # generator point.
    for lot in lots:
        gen = sg.Point(lot.x, lot.y)
        assert lot.lot.intersects(gen)

    # Footprints are strictly inside their lot (inset by the default margin).
    for lot in lots:
        assert lot.footprint.area < lot.lot.area
        assert lot.lot.contains(lot.footprint) or lot.lot.covers(lot.footprint)


def test_build_lots_preserves_world_metadata() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["a", "b"],
        [3.0, 7.0],
        [3.0, 7.0],
        [5, 50],
        assigned=["direct", "snapped"],
        names=["World A", None],
    )
    lots = build_lots(district, 7, member)
    by_id = {lot.world_id: lot for lot in lots}
    assert by_id["a"].name == "World A"
    assert by_id["b"].name is None
    assert by_id["a"].assigned == "direct"
    assert by_id["b"].assigned == "snapped"
    assert all(lot.district_id == 7 for lot in lots)
    assert by_id["a"].visits == 5
    assert by_id["a"].x == 3.0 and by_id["a"].y == 3.0


# ---------------------------------------------------------------------------
# Degenerate cases
# ---------------------------------------------------------------------------


def test_build_lots_single_point_district_is_whole_district() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(["solo"], [5.0], [5.0], [42])
    lots = build_lots(district, 0, member)
    assert len(lots) == 1
    assert lots[0].lot.equals(district)
    assert lots[0].footprint.area > 0.0
    assert district.contains(lots[0].footprint) or district.covers(lots[0].footprint)


def test_build_lots_duplicate_coordinates_still_yields_two_disjoint_lots() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(["a", "b"], [5.0, 5.0], [5.0, 5.0], [1, 2])
    lots = build_lots(district, 0, member)
    assert len(lots) == 2
    # Original (pre-jitter) coordinates are preserved on the Lot.
    assert (lots[0].x, lots[0].y) == (5.0, 5.0)
    assert (lots[1].x, lots[1].y) == (5.0, 5.0)
    # But the two lots are still two distinct, essentially-disjoint polygons.
    assert lots[0].lot.area > 0.0
    assert lots[1].lot.area > 0.0
    assert lots[0].lot.intersection(lots[1].lot).area == pytest.approx(0.0, abs=1e-6)
    union = unary_union([lot.lot for lot in lots])
    assert union.area == pytest.approx(district.area, abs=1e-6)


def test_build_lots_sliver_reassignment_duplicates_survivor_geometry() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    # Three well-separated "big" generators plus a near-duplicate "tiny" one
    # right next to big3, which should be squeezed into a sliver cell.
    member = _member_points(
        ["big1", "big2", "big3", "tiny"],
        [1.0, 9.0, 5.0, 5.0001],
        [1.0, 9.0, 9.0, 9.0001],
        [1, 1, 1, 1],
    )
    lots = build_lots(district, 2, member, min_lot_area_frac=0.2)
    by_id = {lot.world_id: lot for lot in lots}
    assert len(lots) == 4
    # The tiny world's lot/footprint duplicate big3's exactly.
    assert by_id["tiny"].lot.equals(by_id["big3"].lot)
    assert by_id["tiny"].footprint.equals(by_id["big3"].footprint)
    assert count_sliver_reassignments(lots) == 1


def test_build_lots_no_sliver_reassignment_when_floor_is_low() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["big1", "big2", "big3", "tiny"],
        [1.0, 9.0, 5.0, 5.0001],
        [1.0, 9.0, 9.0, 9.0001],
        [1, 1, 1, 1],
    )
    lots = build_lots(district, 2, member, min_lot_area_frac=1e-9)
    assert count_sliver_reassignments(lots) == 0


def test_build_lots_inward_buffer_collapse_still_yields_footprint() -> None:
    # A lot far too thin for the default 0.05 inset to survive a negative
    # buffer -- the fallback (representative_point().buffer(inset)) must
    # still produce a nonempty footprint.
    thin_lot = sg.box(0.0, 0.0, 0.02, 0.02)
    footprint = _footprint(thin_lot, inset=0.05)
    assert not footprint.is_empty
    assert footprint.area == pytest.approx(math.pi * 0.05**2, rel=1e-2)


def test_footprint_empty_lot_falls_back_to_nonempty_circle() -> None:
    footprint = _footprint(sg.Polygon(), inset=0.05)
    assert not footprint.is_empty
    assert footprint.area == pytest.approx(math.pi * 0.05**2, rel=1e-2)


def test_build_lots_zero_points_returns_empty_list() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    empty_member = pl.DataFrame(
        {"world_id": [], "x": [], "y": [], "visits": [], "assigned": []}
    )
    assert build_lots(district, 0, empty_member) == []


# ---------------------------------------------------------------------------
# Height formula
# ---------------------------------------------------------------------------


def test_height_formula_zero_visits_is_h_base() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(["a"], [5.0], [5.0], [0])
    lots = build_lots(district, 0, member)
    assert lots[0].height == pytest.approx(DEFAULT_H_BASE)


def test_height_formula_monotone_in_visits() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["a", "b", "c"], [2.0, 5.0, 8.0], [2.0, 5.0, 8.0], [0, 1_000, 100_000_000]
    )
    lots = build_lots(district, 0, member)
    heights = {lot.world_id: lot.height for lot in lots}
    assert heights["a"] < heights["b"] < heights["c"]
    # Spot value at the top-visits end of the doc's stated range (~105m).
    expected_top = DEFAULT_H_BASE + DEFAULT_H_SCALE * math.log10(1.0 + 100_000_000)
    assert heights["c"] == pytest.approx(expected_top)
    assert 100.0 < heights["c"] < 110.0


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_build_lots_is_deterministic() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["a", "b", "c", "d", "e"],
        [2.0, 8.0, 2.0, 8.0, 5.0],
        [2.0, 2.0, 8.0, 8.0, 5.0],
        [0, 10, 100, 1000, 100_000_000],
    )
    first = build_lots(district, 0, member)
    second = build_lots(district, 0, member)

    assert [lot.world_id for lot in first] == [lot.world_id for lot in second]
    for a, b in zip(first, second, strict=True):
        assert a == b or (
            a.world_id == b.world_id
            and a.lot.equals(b.lot)
            and a.footprint.equals(b.footprint)
            and a.height == b.height
        )


def test_lot_is_frozen() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    lot = Lot(
        world_id="a",
        district_id=0,
        footprint=district,
        lot=district,
        height=4.0,
        name=None,
        visits=0,
        x=5.0,
        y=5.0,
        assigned="direct",
    )
    with pytest.raises(AttributeError):
        lot.height = 10.0  # ty: ignore[invalid-assignment]
