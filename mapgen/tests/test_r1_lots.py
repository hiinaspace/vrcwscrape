"""Tests for R1 greybox lot geometry (``mapgen.r1_lots``): world -> district
assignment and per-district street-fronting subdivision + Hungarian
assignment (falling back to the old per-district Voronoi tessellation on
subdivision failure)."""

from __future__ import annotations

import itertools
import math
from typing import Any

import polars as pl
import pytest
import shapely.geometry as sg
from shapely.ops import unary_union

import mapgen.r1_lots as r1_lots
from mapgen.r1_lots import (
    DEFAULT_METERS_PER_UNIT,
    Lot,
    LotConfig,
    MassingConfig,
    _build_lots_voronoi,
    _footprint,
    _frontage_direction,
    _massing_height,
    _obb_axes,
    _oriented_footprint,
    _touches_ext_ring,
    assign_worlds_to_districts,
    build_lots,
    classify_typology,
    count_sliver_reassignments,
    displacement_stats,
    select_member_points,
    subdivide_district,
    top_landmark_ids,
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


def test_subdivide_district_degenerate_zero_area_raises_when_n_gt_1() -> None:
    # A collapsed/duplicate-vertex "polygon" (zero area) -- n_lots > 1 can
    # never honor the M >= N contract for it, so subdivide_district must
    # raise rather than silently returning a single-lot [district] (which
    # would violate build_lots's downstream count-reconciliation contract).
    degenerate = sg.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 0.0), (0.0, 0.0)])
    with pytest.raises(r1_lots._SubdivisionFailure):
        subdivide_district(degenerate, 2, seed=0, cfg=LotConfig())


def test_subdivide_district_impossible_shape_floor_raises() -> None:
    # An impossibly strict width floor means NO candidate split can ever
    # pass -- _best_split must reject every one of them outright (not fall
    # back to picking the "least bad" candidate the way the soft
    # aspect_clamp scoring does), so subdivision can never progress past a
    # single leaf and must raise rather than ever emit a sub-floor sliver.
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    cfg = LotConfig(min_lot_width=100.0)
    with pytest.raises(r1_lots._SubdivisionFailure):
        subdivide_district(district, 5, seed=0, cfg=cfg)


def test_subdivide_district_wedge_yields_no_sub_floor_lots() -> None:
    # A converging wedge (apex at the origin, base 6 units wide at x=20)
    # simulating a macro-arterial-converged block
    # (docs/macro-roads-nuclei-plan.md F1 row): pre-fix, splitting deep
    # enough toward the apex would eventually carve out a thin sliver there
    # -- the old aspect_clamp only SCORES that down, never rejects it. With
    # the hard shape-floor active (LotConfig defaults), every split that
    # would produce such a child is rejected outright (leaving the
    # apex-side piece whole/unsplit) or, if one still slips through some
    # other path, the merge pass absorbs it -- either way, no final lot may
    # violate the floor.
    # NOTE: seed=0 here -- a few OTHER seeds on this exact raw-triangle
    # fixture used to trip a PRE-EXISTING partition/union-area mismatch in
    # shapely_split unrelated to the shape floor (confirmed present on
    # unmodified subdivide_district via `git stash`); see
    # test_subdivide_district_triangle_wedge_partitions_cleanly_at_bad_seeds
    # below, which covers exactly those seeds now that _best_split snaps
    # every split's children to _SPLIT_PRECISION_GRID (macro-roads-nuclei-plan
    # wave-2 "subdivision robustness" fix).
    district = sg.Polygon([(0.0, 0.0), (20.0, 3.0), (20.0, -3.0)])
    cfg = LotConfig()
    lots = subdivide_district(district, 8, seed=0, cfg=cfg)
    assert len(lots) >= 8
    union = unary_union(lots)
    assert union.area == pytest.approx(district.area, rel=1e-6)
    assert _no_overlap(lots)
    for p in lots:
        assert r1_lots._meets_shape_floor(p, cfg), f"sub-floor sliver: {p.wkb}"


def test_subdivide_district_triangle_wedge_partitions_cleanly_at_bad_seeds() -> None:
    # Same acute triangle as the wedge test above, at the SPECIFIC seeds
    # (2, 3, 10) that pre-fix produced a real GEOS unary_union robustness
    # failure: every leaf individually is_valid, no pairwise
    # `.intersection().area` is ever nonzero, sum(area) matches the district
    # area exactly, yet raw `unary_union` computed an area short by ~10-15%
    # (confirmed via `git stash` before this fix -- root-caused to adjacent
    # leaves' shared edges drifting by a few ULPs across repeated
    # shapely_split calls, which trips GEOS's cascaded union on this
    # near-degenerate acute geometry). `_best_split` now snaps every split's
    # two children to `_SPLIT_PRECISION_GRID` immediately, which keeps every
    # leaf on one shared grid throughout the recursion and eliminates the
    # discrepancy -- verified here directly against `unary_union`, not just
    # via `_assert_partition` (which now ALSO snaps defensively, see its own
    # tests below).
    district = sg.Polygon([(0.0, 0.0), (20.0, 3.0), (20.0, -3.0)])
    cfg = LotConfig()
    for seed in (2, 3, 10):
        lots = subdivide_district(district, 8, seed=seed, cfg=cfg)
        assert len(lots) >= 8
        union = unary_union(lots)
        assert union.area == pytest.approx(district.area, rel=1e-6), (
            f"seed={seed}: union {union.area} != district {district.area}"
        )
        assert _no_overlap(lots), f"seed={seed}: pairwise overlap"


def test_merge_slivers_absorbs_subfloor_leaf_into_widest_shared_neighbor() -> None:
    # Four axis-aligned pieces partitioning the same 10x10 square: a normal
    # 6-wide block, a deliberately sub-floor 0.2-wide sliver (below the
    # default min_lot_width=0.3), and two normal blocks to its right split
    # top/bottom -- so the sliver shares a FULL-height (10) boundary with
    # the left block but only PARTIAL-height (6 and 4) boundaries with the
    # two right blocks. The merge must pick the left block (longest shared
    # boundary), not just the first candidate encountered.
    a = sg.box(0.0, 0.0, 6.0, 10.0)
    sliver = sg.box(6.0, 0.0, 6.2, 10.0)
    b = sg.box(6.2, 0.0, 10.0, 6.0)
    c = sg.box(6.2, 6.0, 10.0, 10.0)
    leaves = [a, sliver, b, c]
    cfg = LotConfig()
    total_area_before = sum(p.area for p in leaves)

    merged = r1_lots._merge_slivers(leaves, cfg)

    assert len(merged) == 3
    assert sum(p.area for p in merged) == pytest.approx(total_area_before)
    assert _no_overlap(merged)
    for p in merged:
        assert r1_lots._meets_shape_floor(p, cfg)
    # The sliver merged into `a` (the longest shared boundary), yielding a
    # single 6.2x10 rectangle -- not into `b` or `c`.
    expected = sg.box(0.0, 0.0, 6.2, 10.0)
    assert any(p.equals(expected) for p in merged)


def test_merge_slivers_noop_when_all_leaves_pass_floor() -> None:
    leaves = [
        sg.box(0.0, 0.0, 5.0, 10.0),
        sg.box(5.0, 0.0, 10.0, 10.0),
    ]
    cfg = LotConfig()
    merged = r1_lots._merge_slivers(leaves, cfg)
    assert len(merged) == 2
    assert {p.wkb for p in merged} == {p.wkb for p in leaves}


def test_meets_shape_floor_width_and_aspect() -> None:
    cfg = LotConfig(min_lot_width=1.0, max_aspect_reject=3.0)
    assert r1_lots._meets_shape_floor(sg.box(0.0, 0.0, 3.0, 3.0), cfg)
    # Width 0.5 < min_lot_width=1.0 -- fails on width alone.
    assert not r1_lots._meets_shape_floor(sg.box(0.0, 0.0, 10.0, 0.5), cfg)
    # Width 2.0 >= 1.0, but aspect 8.0/2.0=4.0 > max_aspect_reject=3.0.
    assert not r1_lots._meets_shape_floor(sg.box(0.0, 0.0, 8.0, 2.0), cfg)
    assert not r1_lots._meets_shape_floor(sg.Polygon(), cfg)
    # Both checks disabled (<= 0) -- always passes for a nonempty polygon.
    disabled = LotConfig(min_lot_width=0.0, max_aspect_reject=0.0)
    assert r1_lots._meets_shape_floor(sg.box(0.0, 0.0, 100.0, 0.01), disabled)


def test_subdivide_district_prefers_frontage_touching_splits() -> None:
    # A square district subdivided deep enough (n_lots=20) that some interior
    # pieces can only keep frontage if the scoring actively prefers it -- a
    # naive mid-split with no frontage preference would landlock several
    # interior lots with no contact on the district's own exterior ring.
    #
    # Empirically verified (per the fix-4 instructions, not left in this
    # diff): temporarily inverting the sense of `frontage_bad` in
    # `_best_split` (`not all(...)` -> `all(...)`) drops the touching
    # fraction on this exact fixture from 1.0 to 0.65 -- well below the
    # threshold below, which the current (correct) scoring clears easily.
    district = sg.box(0.0, 0.0, 20.0, 20.0)
    lots = subdivide_district(district, 20, seed=7, cfg=LotConfig())
    ext_ring = district.exterior
    touching = sum(1 for p in lots if _touches_ext_ring(p, ext_ring, 0.0))
    frac = touching / len(lots)
    assert frac >= 0.95


# ---------------------------------------------------------------------------
# _assert_partition -- defensive negative checks
# ---------------------------------------------------------------------------


def test_assert_partition_overlapping_lots_raises() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    a = sg.box(0.0, 0.0, 6.0, 10.0)
    b = sg.box(4.0, 0.0, 10.0, 10.0)  # overlaps a on [4, 6] x [0, 10]
    with pytest.raises(r1_lots._SubdivisionFailure):
        r1_lots._assert_partition(district, [a, b])


def test_assert_partition_catches_cumulative_overlap_pairwise_check_missed() -> None:
    # Hand-constructed adversarial fixture (docs/macro-roads-nuclei-plan.md's
    # pre-existing blind-spot note): 300 adjacent vertical strips, each
    # overlapping only its immediate neighbor by a sliver so small that EVERY
    # individual pairwise `.intersection().area` (~0.003) sits well below
    # _assert_partition's own rel_tol threshold (1e-6 * district.area == 0.01
    # here) -- so a check that only ever looked at pairwise overlaps in
    # isolation would pass every single one of them. The overlaps
    # accumulate, though: summed across ~299 adjacent pairs they leave
    # unary_union's total area measurably short of the district's, which
    # _assert_partition's union-area check (now snapped to
    # _SPLIT_PRECISION_GRID first, see that check's docstring) still catches.
    n = 300
    width = 100.0 / n
    overlap_w = 0.00003
    step = width - overlap_w
    district = sg.box(0.0, 0.0, 100.0, 100.0)  # area 10_000
    strips = [sg.box(i * step, 0.0, i * step + width, 100.0) for i in range(n)]

    tol = 1e-6 * district.area
    for i in range(n - 1):
        overlap = strips[i].intersection(strips[i + 1]).area
        assert 0.0 < overlap < tol, f"fixture invariant broken at pair {i}: {overlap}"

    with pytest.raises(r1_lots._SubdivisionFailure):
        r1_lots._assert_partition(district, strips)


def test_assert_partition_area_mismatch_raises() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)  # area 100
    a = sg.box(0.0, 0.0, 5.0, 10.0)
    b = sg.box(5.0, 0.0, 9.0, 10.0)  # union area 90 != district area 100
    with pytest.raises(r1_lots._SubdivisionFailure):
        r1_lots._assert_partition(district, [a, b])


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


def test_build_lots_hungarian_assignment_is_globally_optimal() -> None:
    # Two tight world clusters in opposite corners of a wide rectangular
    # district, subdivided into exactly n lots (stop_area_factor=1e9 forces
    # the ENTIRE lot count through _fill_deficit's one-leaf-becomes-two-per-
    # split path, so len(lot_polys) == n exactly -- see
    # test_subdivide_district_deficit_path_forces_resplit). n <= 6 makes an
    # exact brute-force check over itertools.permutations cheap.
    district = sg.box(0.0, 0.0, 20.0, 8.0)
    cfg = LotConfig(stop_area_factor=1e9)
    n = 5
    world_ids = [f"w{i}" for i in range(n)]
    xs = [0.3, 0.6, 0.9, 19.1, 19.4]
    ys = [0.3, 0.6, 0.9, 7.7, 7.4]
    member = _member_points(world_ids, xs, ys, [1] * n)

    lots = build_lots(district, 0, member, cfg=cfg)
    occupied = [lot for lot in lots if lot.kind == "lot"]
    assert len(occupied) == n

    # Independently reproduce the exact lot anchors build_lots assigned
    # against (same district/n/seed=district_id/cfg -> deterministic).
    lot_polys = subdivide_district(district, n, seed=0, cfg=cfg)
    assert len(lot_polys) == n
    anchors = []
    for poly in lot_polys:
        centroid = poly.centroid
        anchor = centroid if poly.contains(centroid) else poly.representative_point()
        anchors.append((anchor.x, anchor.y))

    cost = [
        [(xs[i] - ax) ** 2 + (ys[i] - ay) ** 2 for (ax, ay) in anchors]
        for i in range(n)
    ]
    best = min(
        sum(cost[i][perm[i]] for i in range(n))
        for perm in itertools.permutations(range(n))
    )
    actual = sum(lot.displacement**2 for lot in occupied)
    assert actual == pytest.approx(best, rel=1e-6, abs=1e-9)


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


def test_build_lots_degenerate_district_falls_back_instead_of_crashing() -> None:
    # A collapsed/duplicate-vertex "polygon" (zero area) with n_lots > 1:
    # subdivide_district now raises _SubdivisionFailure for this (see
    # test_subdivide_district_degenerate_zero_area_raises_when_n_gt_1) rather
    # than returning a single [district] lot that violates build_lots's own
    # M >= N contract for its Hungarian assignment step. build_lots must
    # catch that and fall back to the Voronoi mechanism, not crash.
    degenerate = sg.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 0.0), (0.0, 0.0)])
    member = _member_points(["a", "b"], [0.0, 1.0], [0.0, 0.0], [1, 1])
    stats: dict[str, Any] = {}

    lots = build_lots(degenerate, 3, member, fallback_stats=stats)

    assert stats["n_fallback"] == 1
    assert stats["fallback_districts"][0]["district_id"] == 3
    occupied = [lot for lot in lots if lot.kind == "lot"]
    assert len(occupied) == 2
    assert {lot.world_id for lot in occupied} == {"a", "b"}


# ---------------------------------------------------------------------------
# Per-district shape-floor relaxation (docs/macro-roads-nuclei-plan.md
# wave-2 "subdivision robustness": F1's regression where a hard shape-floor
# over-triggers the whole-district Voronoi fallback on dense districts)
# ---------------------------------------------------------------------------


def test_relaxed_lot_configs_ladder_progressively_relaxes_then_disables() -> None:
    cfg = LotConfig()  # min_lot_width=0.3, max_aspect_reject=8.0 defaults
    ladder = r1_lots._relaxed_lot_configs(cfg)

    assert ladder[0] is cfg  # strict floor tried first, unmodified
    # Every step before the final one strictly relaxes (width down,
    # aspect-reject up) -- the FINAL step instead disables each half
    # outright (0 means "off", not "most permissive nonzero value", so it's
    # excluded from the monotonic-relax comparison and checked separately
    # below).
    for prev, nxt in zip(ladder[:-2], ladder[1:-1], strict=True):
        assert nxt.min_lot_width <= prev.min_lot_width
        assert nxt.max_aspect_reject >= prev.max_aspect_reject
    # ... ending with the floor fully disabled.
    assert ladder[-1].min_lot_width == 0.0
    assert ladder[-1].max_aspect_reject == 0.0
    # Every other field is carried through untouched.
    for step in ladder:
        assert step.stop_area_factor == cfg.stop_area_factor
        assert step.aspect_clamp == cfg.aspect_clamp
        assert step.inset == cfg.inset


def test_relaxed_lot_configs_noop_when_floor_already_disabled() -> None:
    cfg = LotConfig(min_lot_width=0.0, max_aspect_reject=0.0)
    assert r1_lots._relaxed_lot_configs(cfg) == [cfg]


def test_subdivide_district_dense_square_needs_relaxed_floor() -> None:
    # A small, dense district: target_area = 9 / 100 = 0.09, sqrt ~ 0.3 --
    # right at DEFAULT_MIN_LOT_WIDTH, so the strict floor cannot reach 100
    # lots (confirmed: raises), but the SAME district reaches 100 once the
    # floor is relaxed -- this is the per-district ladder build_lots now
    # drives, exercised here directly against subdivide_district so the
    # relaxation mechanism itself (not build_lots's fallback bookkeeping) is
    # under test.
    district = sg.box(0.0, 0.0, 3.0, 3.0)
    strict = LotConfig()
    with pytest.raises(r1_lots._SubdivisionFailure):
        subdivide_district(district, 100, seed=0, cfg=strict)

    relaxed = None
    for candidate in r1_lots._relaxed_lot_configs(strict):
        try:
            relaxed = subdivide_district(district, 100, seed=0, cfg=candidate)
            break
        except r1_lots._SubdivisionFailure:
            continue
    assert relaxed is not None, "no rung of the relaxation ladder reached 100 lots"
    assert len(relaxed) >= 100


def test_build_lots_dense_district_relaxes_instead_of_falling_back_to_voronoi() -> None:
    # Same dense 3x3-square/100-lot scenario, but through build_lots end to
    # end: pre-fix this district hit _SubdivisionFailure on its FIRST (only)
    # attempt and fell back to the whole-district Voronoi mechanism. It must
    # now resolve via the relaxation ladder instead -- n_fallback stays
    # unset/zero and n_relaxed records that this district needed relaxation.
    district = sg.box(0.0, 0.0, 3.0, 3.0)
    n = 100
    member = _member_points(
        [f"w{i}" for i in range(n)],
        [(i % 10) * 0.3 + 0.05 for i in range(n)],
        [(i // 10) * 0.3 + 0.05 for i in range(n)],
        [1] * n,
    )
    stats: dict[str, Any] = {}

    lots = build_lots(district, 0, member, seed=0, fallback_stats=stats)

    assert stats.get("n_fallback", 0) == 0
    assert "fallback_districts" not in stats
    assert stats["n_relaxed"] == 1
    assert stats["relaxed_districts"][0]["district_id"] == 0
    assert stats["relaxed_districts"][0]["relax_step"] > 0
    occupied = [lot for lot in lots if lot.kind == "lot"]
    assert len(occupied) == n
    assert {lot.world_id for lot in occupied} == set(member["world_id"].to_list())


def test_build_lots_normal_district_never_relaxes_the_strict_floor() -> None:
    # A normal (non-dense) district must take exactly the strict-floor
    # attempt -- the relaxation ladder is per-district, not a global
    # softening of the default: build_lots must record neither fallback nor
    # relaxation stats here.
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["a", "b", "c"],
        [2.0, 8.0, 5.0],
        [2.0, 2.0, 8.0],
        [1, 1, 1],
    )
    stats: dict[str, Any] = {}

    lots = build_lots(district, 0, member, fallback_stats=stats)

    assert stats.get("n_fallback", 0) == 0
    assert stats.get("n_relaxed", 0) == 0
    assert "fallback_districts" not in stats
    assert "relaxed_districts" not in stats
    assert len([lot for lot in lots if lot.kind == "lot"]) == 3


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
    massing = MassingConfig()
    for lot_poly in subdivide_district(district, 6, seed=5, cfg=LotConfig()):
        footprint = _oriented_footprint(
            lot_poly,
            district.exterior,
            "detached",
            massing,
            DEFAULT_METERS_PER_UNIT,
            LotConfig(),
        )
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
    footprint = _oriented_footprint(
        district,
        district.exterior,
        "detached",
        MassingConfig(),
        DEFAULT_METERS_PER_UNIT,
        LotConfig(),
    )
    _center, axis_long, _axis_short, _long_len, _short_len = _obb_axes(footprint)
    angle = abs(math.atan2(axis_long[1], axis_long[0]))
    angle_mod_pi = min(angle, abs(math.pi - angle))
    assert angle_mod_pi == pytest.approx(0.0, abs=0.05)


# ---------------------------------------------------------------------------
# displacement_stats
# ---------------------------------------------------------------------------


_ZERO_STATS = {"n": 0, "median": 0.0, "p95": 0.0, "max": 0.0}


def test_displacement_stats_empty() -> None:
    stats = displacement_stats([])
    assert stats["n"] == 0
    assert stats["median"] == 0.0
    assert stats["p95"] == 0.0
    assert stats["max"] == 0.0
    # Per-kind splits are present (and zero) even with no lots at all.
    assert stats["direct"] == _ZERO_STATS
    assert stats["snapped"] == _ZERO_STATS


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
            typology="detached" if kind == "lot" else "",
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
    # All three occupied lots are "direct" here -- the split mirrors combined.
    assert stats["direct"]["n"] == 3
    assert stats["snapped"] == _ZERO_STATS


def test_displacement_stats_p95_matches_ceil_index_formula() -> None:
    # 20 values 1..20: p95_idx = min(19, ceil(0.95*20) - 1) = min(19, 18) = 18
    # (0-indexed) -> sorted values[18] == 19.0, distinct from max == 20.0 --
    # independently derived expected values, not copied from the implementation.
    district = sg.box(0.0, 0.0, 10.0, 10.0)

    def _lot(disp: float) -> Lot:
        return Lot(
            world_id="w",
            district_id=0,
            footprint=district,
            lot=district,
            height=4.0,
            name=None,
            visits=0,
            x=0.0,
            y=0.0,
            assigned="direct",
            lot_x=disp,
            lot_y=0.0,
            kind="lot",
            displacement=disp,
            typology="detached",
        )

    lots = [_lot(float(v)) for v in range(1, 21)]
    stats = displacement_stats(lots)
    assert stats["n"] == 20
    assert stats["p95"] == pytest.approx(19.0)
    assert stats["max"] == pytest.approx(20.0)


def test_displacement_stats_splits_direct_and_snapped() -> None:
    # A snapped world's true coordinate sits far outside the district (the
    # module docstring's bounded-displacement decision only applies to
    # "direct" worlds) -- the combined stats would hide this outlier inside
    # an otherwise-tight direct-only distribution; the split must not.
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(
        ["a", "b", "snapped_far"],
        [2.0, 8.0, 1000.0],
        [2.0, 8.0, 1000.0],
        [1, 1, 1],
        assigned=["direct", "direct", "snapped"],
    )
    lots = build_lots(district, 0, member)
    occupied = [lot for lot in lots if lot.kind == "lot"]
    diag = _obb_diagonal(district)

    direct_lots = [lot for lot in occupied if lot.assigned == "direct"]
    snapped_lots = [lot for lot in occupied if lot.assigned == "snapped"]
    assert len(direct_lots) == 2
    assert len(snapped_lots) == 1
    for lot in direct_lots:
        assert lot.displacement <= diag + 1e-6
    assert snapped_lots[0].displacement > diag

    stats = displacement_stats(lots)
    assert stats["direct"]["n"] == 2
    assert stats["snapped"]["n"] == 1
    assert stats["direct"]["max"] <= diag + 1e-6
    assert stats["snapped"]["max"] > diag


# ---------------------------------------------------------------------------
# Massing model: typology classification, height, footprint setbacks
# (docs/wave2-plan.md "The massing model (2a core)")
# ---------------------------------------------------------------------------


def test_classify_typology_area_threshold() -> None:
    cfg = MassingConfig()
    mpu = DEFAULT_METERS_PER_UNIT
    # area-per-world = district_area_units * mpu**2 / n_worlds.
    # Pick a district area comfortably above/below cfg.a_detached_m2.
    big_area_units = (cfg.a_detached_m2 * 2.0) / (mpu**2)  # a = 2*threshold for n=1
    small_area_units = (cfg.a_detached_m2 * 0.5) / (mpu**2)  # a = 0.5*threshold for n=1
    assert (
        classify_typology(big_area_units, 1, "w", frozenset(), cfg, mpu) == "detached"
    )
    assert classify_typology(small_area_units, 1, "w", frozenset(), cfg, mpu) == "row"


def test_classify_typology_landmark_overrides_area() -> None:
    cfg = MassingConfig()
    mpu = DEFAULT_METERS_PER_UNIT
    # A huge area-per-world would classify "detached" -- landmark membership
    # must win regardless.
    huge_area_units = (cfg.a_detached_m2 * 100.0) / (mpu**2)
    assert (
        classify_typology(huge_area_units, 1, "w", frozenset({"w"}), cfg, mpu)
        == "landmark"
    )
    # A non-member at the same area still classifies normally.
    assert (
        classify_typology(huge_area_units, 1, "other", frozenset({"w"}), cfg, mpu)
        == "detached"
    )


def test_classify_typology_is_deterministic() -> None:
    cfg = MassingConfig()
    mpu = DEFAULT_METERS_PER_UNIT
    args = (12.0, 3, "world_42", frozenset({"landmark_1"}), cfg, mpu)
    assert classify_typology(*args) == classify_typology(*args)


def test_top_landmark_ids_picks_highest_visits_with_deterministic_ties() -> None:
    points = pl.DataFrame(
        {
            "world_id": ["a", "b", "c", "d"],
            "visits": [10, 100, 100, 1],
        }
    )
    # Top 2 by visits: b and c tie at 100 -- both picked since count=2 exactly
    # covers the tie; ascending world_id breaks any ordering ambiguity.
    top = top_landmark_ids(points, 2)
    assert top == frozenset({"b", "c"})
    assert top_landmark_ids(points, 0) == frozenset()
    assert top_landmark_ids(points, -1) == frozenset()
    empty = pl.DataFrame({"world_id": [], "visits": []})
    assert top_landmark_ids(empty, 5) == frozenset()


def test_massing_height_within_typology_band_and_deterministic() -> None:
    cfg = MassingConfig()
    for typology, (lo, hi) in (
        ("detached", cfg.detached_stories),
        ("row", cfg.row_stories),
        ("landmark", cfg.landmark_stories),
    ):
        for world_id in ("world_a", "world_b", "world_c", "another_world"):
            h = _massing_height(world_id, typology, cfg)
            lo_bound = lo * cfg.story_height_m - cfg.height_jitter_m
            hi_bound = hi * cfg.story_height_m + cfg.height_jitter_m
            assert lo_bound - 1e-9 <= h <= hi_bound + 1e-9
            # Pure function of its inputs: identical across calls.
            assert _massing_height(world_id, typology, cfg) == pytest.approx(h)


def test_massing_height_visits_do_not_affect_height() -> None:
    # Wave 2 retires the visits-driven height formula entirely -- visits only
    # ever feeds typology via top_landmark_ids, never the height itself.
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member_low = _member_points(["a"], [5.0], [5.0], [0])
    member_high = _member_points(["a"], [5.0], [5.0], [100_000_000])
    lots_low = build_lots(district, 0, member_low)
    lots_high = build_lots(district, 0, member_high)
    assert lots_low[0].height == pytest.approx(lots_high[0].height)


def test_build_lots_uses_massing_typology_and_landmark_ids() -> None:
    district = sg.box(0.0, 0.0, 10.0, 10.0)
    member = _member_points(["a", "b"], [3.0, 7.0], [3.0, 7.0], [1, 1])
    massing = MassingConfig()
    lots = build_lots(
        district, 0, member, massing=massing, landmark_ids=frozenset({"a"})
    )
    by_id = {lot.world_id: lot for lot in lots if lot.kind == "lot"}
    assert by_id["a"].typology == "landmark"
    assert by_id["b"].typology in ("detached", "row")
    assert by_id["a"].height == pytest.approx(_massing_height("a", "landmark", massing))


# ---------------------------------------------------------------------------
# Footprint invariants (docs/wave2-plan.md's correction-2/3 regression tests)
# ---------------------------------------------------------------------------


def test_build_lots_footprint_inside_lot_for_every_lot() -> None:
    district = sg.box(0.0, 0.0, 20.0, 8.0)
    world_ids = [f"w{i}" for i in range(8)]
    xs = [1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 13.0, 19.0]
    ys = [1.0, 6.0, 2.0, 5.0, 3.0, 4.0, 6.5, 1.5]
    member = _member_points(world_ids, xs, ys, [10] * 8)
    lots = build_lots(district, 0, member)
    for lot in lots:
        if lot.footprint.is_empty:
            continue
        eps = lot.lot.buffer(1e-6)
        assert lot.lot.contains(lot.footprint) or eps.covers(lot.footprint)


def test_build_lots_footprint_never_intersects_fronting_street_ribbon() -> None:
    # THE key regression (docs/wave2-plan.md correction 2/3): the OLD
    # fill-ratio footprint model inset only ~1.25m from the lot line while
    # the Chen street ribbon reached ~1.9m past it -- buildings intersected
    # the road. The new setback model's front_setback is denominated from
    # the FRONTAGE LOT LINE and is >= the street half-width by construction
    # (road_clear_m = half-width + sidewalk), so no footprint should ever
    # reach into the fronting street's ribbon.
    district = sg.box(0.0, 0.0, 20.0, 8.0)
    mpu = DEFAULT_METERS_PER_UNIT
    massing = MassingConfig()

    # single full-district lot (n=1 build_lots path) -- frontage = bottom edge
    # (y=0), first-encountered/longest edge on the district's own exterior.
    member = _member_points(["solo"], [10.0], [1.0], [10])
    lots = build_lots(district, 0, member, massing=massing, meters_per_unit=mpu)
    footprint = lots[0].footprint
    assert not footprint.is_empty

    # Chen "street" ribbon half-width, island units (mapgen.r1_mesh
    # DEFAULT_STREET_WIDTHS["street"]=0.25 island units, buffer_ribbon halves
    # it): independently derived here rather than importing r1_mesh, to keep
    # this module's tests decoupled from that one.
    street_half_width_units = (0.25 * mpu / 2.0) / mpu
    frontage_edge = sg.LineString([(0.0, 0.0), (20.0, 0.0)])
    ribbon = frontage_edge.buffer(street_half_width_units)

    assert footprint.intersection(ribbon).area == pytest.approx(0.0, abs=1e-9)


def test_row_typology_footprints_share_wall_with_zero_gap() -> None:
    # Row typology uses side_setback = 0 (shared wall, continuous terrace) --
    # adjacent lots' footprints must abut exactly (no gap, no overlap).
    # The enclosing district is DELIBERATELY wider/taller than the row so
    # each lot's own left/right edges are interior cuts, not additional
    # frontage on the district's own exterior ring (only the shared bottom
    # edge is real frontage here).
    district = sg.Polygon([(-5.0, 0.0), (20.0, 0.0), (20.0, 8.0), (-5.0, 8.0)])
    lot1 = sg.Polygon([(0.0, 0.0), (5.0, 0.0), (5.0, 8.0), (0.0, 8.0)])
    lot2 = sg.Polygon([(5.0, 0.0), (10.0, 0.0), (10.0, 8.0), (5.0, 8.0)])
    lot3 = sg.Polygon([(10.0, 0.0), (15.0, 0.0), (15.0, 8.0), (10.0, 8.0)])
    massing = MassingConfig()
    mpu = DEFAULT_METERS_PER_UNIT
    cfg = LotConfig()

    footprints = [
        _oriented_footprint(lot, district.exterior, "row", massing, mpu, cfg)
        for lot in (lot1, lot2, lot3)
    ]
    for fp in footprints:
        assert not fp.is_empty

    for a, b in ((footprints[0], footprints[1]), (footprints[1], footprints[2])):
        assert a.intersection(b).area == pytest.approx(0.0, abs=1e-9)
        assert a.distance(b) == pytest.approx(0.0, abs=1e-9)
        shared = a.boundary.intersection(b.boundary)
        assert shared.length > 0.0


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
        typology="detached",
    )
    with pytest.raises(AttributeError):
        lot.height = 10.0  # ty: ignore[invalid-assignment]
