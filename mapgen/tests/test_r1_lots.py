"""Tests for R1 greybox lot geometry (``mapgen.r1_lots``): world -> district
assignment and per-district street-fronting subdivision + Hungarian
assignment (falling back to the old per-district Voronoi tessellation on
subdivision failure)."""

from __future__ import annotations

import math
from typing import Any

import polars as pl
import pytest
import shapely.geometry as sg
from shapely.ops import unary_union

import mapgen.r1_lots as r1_lots
from mapgen.r1_lots import (
    DEFAULT_H_BASE,
    DEFAULT_H_SCALE,
    Lot,
    LotConfig,
    _build_lots_voronoi,
    _footprint,
    _frontage_direction,
    _obb_axes,
    _oriented_footprint,
    assign_worlds_to_districts,
    build_lots,
    count_sliver_reassignments,
    displacement_stats,
    select_member_points,
    subdivide_district,
)

# ---------------------------------------------------------------------------
# assign_worlds_to_districts (UNCHANGED behavior -- not part of this wave)
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
# Helpers
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


def _obb_diagonal(poly: sg.Polygon) -> float:
    """Length of ``poly``'s ``minimum_rotated_rectangle`` diagonal."""
    _center, _axl, _axs, long_len, short_len = _obb_axes(poly)
    return math.hypot(long_len, short_len)


def _no_overlap(polys: list[sg.Polygon], tol: float = 1e-6) -> bool:
    for i, a in enumerate(polys):
        for b in polys[i + 1 :]:
            if a.intersection(b).area > tol:
                return False
    return True


# ---------------------------------------------------------------------------
# subdivide_district -- partition invariants, count reconciliation, determinism
# ---------------------------------------------------------------------------


def test_subdivide_district_partitions_a_square() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    lots = subdivide_district(district, 6, seed=1, cfg=LotConfig())
    assert len(lots) >= 6
    union = unary_union(lots)
    assert union.area == pytest.approx(district.area, rel=1e-6)
    assert sum(p.area for p in lots) == pytest.approx(district.area, rel=1e-6)
    assert _no_overlap(lots)


def test_subdivide_district_partitions_a_concave_l_shape() -> None:
    # L-shaped (gamma) concave district.
    district = sg.Polygon([(0, 0), (10, 0), (10, 4), (4, 4), (4, 10), (0, 10)])
    lots = subdivide_district(district, 5, seed=3, cfg=LotConfig())
    assert len(lots) >= 5
    union = unary_union(lots)
    assert union.area == pytest.approx(district.area, rel=1e-6)
    assert _no_overlap(lots)


def test_subdivide_district_no_lot_overlap_across_seeds() -> None:
    district = sg.box(0.0, 0.0, 20.0, 8.0)
    for seed in range(5):
        lots = subdivide_district(district, 10, seed=seed, cfg=LotConfig())
        assert _no_overlap(lots), f"overlap at seed={seed}"
        union = unary_union(lots)
        assert union.area == pytest.approx(district.area, rel=1e-6)


def test_subdivide_district_n_le_1_returns_whole_district() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    assert subdivide_district(district, 1, seed=0, cfg=LotConfig())[0].equals(district)
    assert subdivide_district(district, 0, seed=0, cfg=LotConfig())[0].equals(district)


def test_subdivide_district_is_deterministic() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    cfg = LotConfig()
    first = subdivide_district(district, 7, seed=42, cfg=cfg)
    second = subdivide_district(district, 7, seed=42, cfg=cfg)
    assert [p.wkb for p in first] == [p.wkb for p in second]


def test_subdivide_district_deficit_path_forces_resplit() -> None:
    # An astronomically high stop_area_factor means the MAIN recursion never
    # splits at all (the whole district is already "below threshold") -- the
    # entire lot count has to come from the largest-first deficit-fill retry
    # path (_fill_deficit), exercising it directly.
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    cfg = LotConfig(stop_area_factor=1e9)
    lots = subdivide_district(district, 5, seed=0, cfg=cfg)
    assert len(lots) >= 5
    union = unary_union(lots)
    assert union.area == pytest.approx(district.area, rel=1e-6)
    assert _no_overlap(lots)


# ---------------------------------------------------------------------------
# build_lots -- count reconciliation, bijection, displacement bound
# ---------------------------------------------------------------------------


def test_build_lots_count_reconciliation_and_bijection() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["a", "b", "c", "d", "e"],
        [2.0, 8.0, 2.0, 8.0, 5.0],
        [2.0, 2.0, 8.0, 8.0, 5.0],
        [0, 10, 100, 1000, 100_000_000],
    )
    lots = build_lots(district, 0, member)
    occupied = [lot for lot in lots if lot.kind == "lot"]
    greenspace = [lot for lot in lots if lot.kind == "greenspace"]
    assert len(occupied) == 5
    assert len(lots) == len(occupied) + len(greenspace)
    assert {lot.world_id for lot in occupied} == {"a", "b", "c", "d", "e"}
    for lot in greenspace:
        assert lot.world_id == ""
        assert lot.footprint.is_empty

    # Bijection: every occupied lot maps to a distinct lot polygon.
    lot_wkbs = [lot.lot.wkb for lot in occupied]
    assert len(set(lot_wkbs)) == len(lot_wkbs)

    # Partition invariant over ALL returned lots (occupied + greenspace).
    all_polys = [lot.lot for lot in lots]
    union = unary_union(all_polys)
    assert union.area == pytest.approx(district.area, rel=1e-6)
    assert _no_overlap(all_polys)

    # Displacement bound: every occupied lot's assigned anchor is within the
    # district's own OBB diagonal of the world's original coordinate.
    diag = _obb_diagonal(district)
    for lot in occupied:
        assert lot.displacement <= diag + 1e-6
        assert lot.displacement == pytest.approx(
            math.hypot(lot.x - lot.lot_x, lot.y - lot.lot_y)
        )


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
    by_id = {lot.world_id: lot for lot in lots if lot.kind == "lot"}
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
    assert lots[0].kind == "lot"
    assert lots[0].displacement == 0.0
    assert lots[0].footprint.area > 0.0
    assert district.contains(lots[0].footprint) or district.covers(lots[0].footprint)


def test_build_lots_duplicate_coordinates_still_yields_disjoint_lots() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(["a", "b"], [5.0, 5.0], [5.0, 5.0], [1, 2])
    lots = build_lots(district, 0, member)
    occupied = [lot for lot in lots if lot.kind == "lot"]
    assert len(occupied) == 2
    # Original coordinates are preserved on the Lot (both worlds sat at the
    # same point) even though their assigned lots (and therefore lot_x/lot_y)
    # differ.
    assert (occupied[0].x, occupied[0].y) == (5.0, 5.0)
    assert (occupied[1].x, occupied[1].y) == (5.0, 5.0)
    assert occupied[0].lot.area > 0.0
    assert occupied[1].lot.area > 0.0
    assert occupied[0].lot.intersection(occupied[1].lot).area == pytest.approx(
        0.0, abs=1e-6
    )


def test_build_lots_zero_points_returns_empty_list() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    empty_member = pl.DataFrame(
        {"world_id": [], "x": [], "y": [], "visits": [], "assigned": []}
    )
    assert build_lots(district, 0, empty_member) == []


def test_build_lots_fallback_path_triggers_on_subdivision_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Force EVERY split attempt to fail so subdivide_district can never reach
    # the requested lot count -- deterministically exercising build_lots's
    # exception-driven fallback to _build_lots_voronoi, without depending on
    # incidental geometric luck.
    monkeypatch.setattr(r1_lots, "_best_split", lambda *args, **kwargs: None)

    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["a", "b", "c"],
        [2.0, 8.0, 5.0],
        [2.0, 2.0, 8.0],
        [1, 1, 1],
    )
    stats: dict[str, Any] = {}
    lots = build_lots(district, 0, member, fallback_stats=stats)

    assert stats["n_fallback"] == 1
    assert stats["fallback_districts"][0]["district_id"] == 0
    assert len(lots) == 3
    # The Voronoi fallback never produces greenspace and never displaces a
    # world off its own generator point.
    assert all(lot.kind == "lot" for lot in lots)
    assert all(lot.displacement == 0.0 for lot in lots)
    assert all(lot.lot_x == lot.x and lot.lot_y == lot.y for lot in lots)


def test_build_lots_sliver_reassignment_duplicates_survivor_geometry() -> None:
    # Exercises the Voronoi-specific sliver reassignment directly (this
    # behavior is _build_lots_voronoi's, not the default subdivision path's).
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["big1", "big2", "big3", "tiny"],
        [1.0, 9.0, 5.0, 5.0001],
        [1.0, 9.0, 9.0, 9.0001],
        [1, 1, 1, 1],
    )
    lots = _build_lots_voronoi(district, 2, member, min_lot_area_frac=0.2)
    by_id = {lot.world_id: lot for lot in lots}
    assert len(lots) == 4
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
    lots = _build_lots_voronoi(district, 2, member, min_lot_area_frac=1e-9)
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


# ---------------------------------------------------------------------------
# Building footprint: fully inside its lot, frontage-oriented
# ---------------------------------------------------------------------------


def test_oriented_footprint_fully_inside_lot() -> None:
    district = sg.box(0.0, 0.0, 20.0, 8.0)
    for lot_poly in subdivide_district(district, 6, seed=5, cfg=LotConfig()):
        footprint = _oriented_footprint(lot_poly, district.exterior, LotConfig())
        assert not footprint.is_empty
        assert lot_poly.contains(footprint) or lot_poly.covers(footprint)


def test_frontage_direction_matches_long_edge_of_rectangular_district() -> None:
    # A wide, short rectangle: the long edges run horizontally (dx, 0).
    district = sg.box(0.0, 0.0, 20.0, 8.0)
    direction = _frontage_direction(district, district.exterior, min_frontage=0.0)
    assert direction is not None
    angle = abs(math.atan2(direction[1], direction[0]))
    # Horizontal, mod pi (direction sign is not fixed).
    angle_mod_pi = min(angle, abs(math.pi - angle))
    assert angle_mod_pi == pytest.approx(0.0, abs=1e-6)


def test_oriented_footprint_angle_matches_long_district_edge() -> None:
    district = sg.box(0.0, 0.0, 20.0, 8.0)
    footprint = _oriented_footprint(district, district.exterior, LotConfig())
    _center, axis_long, _axis_short, _long_len, _short_len = _obb_axes(footprint)
    angle = abs(math.atan2(axis_long[1], axis_long[0]))
    angle_mod_pi = min(angle, abs(math.pi - angle))
    assert angle_mod_pi == pytest.approx(0.0, abs=0.05)


# ---------------------------------------------------------------------------
# displacement_stats
# ---------------------------------------------------------------------------


def test_displacement_stats_empty() -> None:
    assert displacement_stats([]) == {"n": 0, "median": 0.0, "p95": 0.0, "max": 0.0}


def test_displacement_stats_ignores_greenspace_and_computes_percentiles() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)

    def _lot(disp: float, kind: str) -> Lot:
        return Lot(
            world_id="w" if kind == "lot" else "",
            district_id=0,
            footprint=district,
            lot=district,
            height=4.0,
            name=None,
            visits=0,
            x=0.0,
            y=0.0,
            assigned="direct" if kind == "lot" else "",
            lot_x=disp,
            lot_y=0.0,
            kind=kind,
            displacement=disp,
        )

    lots = [
        _lot(1.0, "lot"),
        _lot(2.0, "lot"),
        _lot(3.0, "lot"),
        _lot(100.0, "greenspace"),
    ]
    stats = displacement_stats(lots)
    assert stats["n"] == 3
    assert stats["median"] == pytest.approx(2.0)
    assert stats["max"] == pytest.approx(3.0)
    assert stats["p95"] <= 3.0


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
    heights = {lot.world_id: lot.height for lot in lots if lot.kind == "lot"}
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
    assert [lot.kind for lot in first] == [lot.kind for lot in second]
    for a, b in zip(first, second, strict=True):
        assert a == b or (
            a.world_id == b.world_id
            and a.lot.equals(b.lot)
            and a.footprint.equals(b.footprint)
            and a.height == b.height
            and a.lot_x == b.lot_x
            and a.lot_y == b.lot_y
            and a.displacement == b.displacement
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
        lot_x=5.0,
        lot_y=5.0,
        kind="lot",
        displacement=0.0,
    )
    with pytest.raises(AttributeError):
        lot.height = 10.0  # ty: ignore[invalid-assignment]
