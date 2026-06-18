"""Focused tests for the seam-spike pure helpers (``mapgen.r1_seam``)."""

from __future__ import annotations

import shapely.geometry as sg

from mapgen.r1_seam import seam_gap, select_block_by_rank


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
