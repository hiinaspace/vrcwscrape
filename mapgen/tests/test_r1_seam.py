"""Focused tests for the seam-spike pure helpers (``mapgen.r1_seam``)."""

from __future__ import annotations

import numpy as np
import shapely.geometry as sg

from mapgen.r1_arm_a import IslandFields
from mapgen.r1_seam import chen_in_block, seam_gap, select_block_by_rank


def _square(x0: float, y0: float, side: float) -> sg.Polygon:
    """Axis-aligned square with lower-left corner (x0, y0)."""
    return sg.box(x0, y0, x0 + side, y0 + side)


def test_select_block_by_rank_picks_median_and_drops_slivers() -> None:
    # Areas: 1, 4, 9, 16, 25 (sides 1..5). Sliver floor 1.5*1=1.5 drops the
    # area-1 sliver, leaving [4, 9, 16, 25]; median rank (frac=0.5) -> index
    # round(0.5*3)=2 -> area 16 (side 4).
    blocks = [_square(0, 0, side) for side in (1, 2, 3, 4, 5)]
    idx, poly = select_block_by_rank(blocks, 0.5, min_block_area=1.0)
    assert poly.area == 16.0
    assert idx == 3  # original-list index of the side-4 square

    # frac=0.0 -> smallest survivor (area 4); frac=1.0 -> largest (area 25).
    _, lo = select_block_by_rank(blocks, 0.0, min_block_area=1.0)
    _, hi = select_block_by_rank(blocks, 1.0, min_block_area=1.0)
    assert lo.area == 4.0
    assert hi.area == 25.0


def test_seam_gap_zero_when_one_district_tiles_block() -> None:
    block = _square(0, 0, 10)
    # Single district exactly equal to the block: no uncovered area, and every
    # boundary sample lies on the fabric edge -> ~zero gap.
    gap = seam_gap([_square(0, 0, 10)], block, sample_spacing=1.0)
    assert gap.uncovered_frac < 1e-9
    assert gap.uncovered_area < 1e-9
    assert gap.max_boundary_gap < 1e-6
    assert gap.overshoot_area < 1e-9


def test_seam_gap_large_when_districts_cover_half() -> None:
    block = _square(0, 0, 10)
    # Districts fill only the left half (x in [0, 5]); the right half is bare.
    half = sg.box(0, 0, 5, 10)
    gap = seam_gap([half], block, sample_spacing=1.0)
    # ~Half the block uncovered.
    assert 0.45 < gap.uncovered_frac < 0.55
    # The right edge (x=10) is ~5 units from the nearest fabric edge (x=5).
    assert gap.max_boundary_gap > 4.0
    # No fabric spills outside the block.
    assert gap.overshoot_area < 1e-9


def test_seam_gap_no_districts_reports_fully_bare() -> None:
    block = _square(0, 0, 10)
    gap = seam_gap([], block, sample_spacing=2.0)
    assert gap.uncovered_frac == 1.0
    assert gap.max_boundary_gap > 0.0


def _flat_fields(side: float, *, ncells: int = 40) -> IslandFields:
    """Synthetic IslandFields: a uniform unit-density square, flat terrain.

    The raster spans ``[0, side]^2`` with ``ncells`` cells per axis so a small
    ``max_parcel_mass`` can force the density-mass gate to split.
    """
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


def test_chen_in_block_splits_square_for_small_mass() -> None:
    # A flat unit-density 10x10 square has total mass ~100. A per-district mass
    # cap of ~12 should force the density-mass gate to split it into >1 district.
    block = _square(0, 0, 10)
    fields = _flat_fields(10.0)
    result = chen_in_block(
        block,
        fields,
        max_parcel_mass=12.0,
        min_parcel_area=10.0 * 10.0 / (40 * 4),
    )
    assert result.generated is not None, result.info
    assert len(result.districts) > 1
    assert len(result.streets) >= 1
    # Districts should tile (roughly) the block: union covers most of its area.
    union_area = sum(p.area for p in result.districts)
    assert union_area > 0.8 * block.area


def test_chen_in_block_surfaces_gates_and_perimeter_flags() -> None:
    # R1 connectivity stage 1 (mapgen.r1_connect): chen_in_block should surface
    # per-street boundary gates and perimeter flags alongside districts/streets,
    # index-aligned with .streets. A smaller max_parcel_mass than the
    # `_splits_for_small_mass` case above is needed here: at the coarser masses
    # every district still touches the boundary ring directly (no interior
    # street is ever generated, so gates == 0 -- a real, legitimate Chen
    # outcome, not a bug), so this uses a mass small enough to force at least
    # one interior street.
    #
    # Not marked slow: measured ~1-3s (comparable to the mass=12 case above,
    # not the "real Chen generation is expensive" case slow is meant for) --
    # and this is the only test exercising the real chen_in_block -> r1_connect
    # integration (gate content, street/flag alignment), so it belongs in the
    # quick lane `pytest tests/test_r1_*.py -m "not slow"` runs by default.
    block = _square(0, 0, 10)
    fields = _flat_fields(10.0)
    result = chen_in_block(
        block,
        fields,
        max_parcel_mass=6.0,
        min_parcel_area=10.0 * 10.0 / (40 * 4),
    )
    assert result.generated is not None, result.info
    assert len(result.street_perimeter_flags) == len(result.streets)
    assert len(result.gates) >= 1
    # Every gate must land exactly on the block boundary (the whole point of
    # a "gate": a per-block street endpoint on the block-boundary ring that
    # a later stage snaps onto the macro network).
    boundary = block.exterior
    for gate in result.gates:
        assert boundary.distance(sg.Point(gate.x, gate.y)) < 1e-9
