"""R1 stage 2 — seam-spike pure helpers (macro+micro hybrid).

The "seam spike" probes the literature-flagged RISK in
``docs/large-scale-growth-research.md``: stitching Chen/R2's *local* street
fabric to the *macro* arterials that bound a single macro-block. This module
holds the small, deterministic, testable pieces used by
``scripts/run_r1_seam_spike.py``:

1. :func:`select_block_by_rank` — pick one mid-complexity macro-block (drop
   slivers, then take a block at a fractional area rank).
2. :func:`seam_gap` — quantify how cleanly the union of Chen districts fills the
   chosen block: (a) the max boundary-to-fabric distance (are bounding arterials
   left bare?) and (b) the uncovered-area fraction.

``load_boundary`` and ``_boundary_mask`` mirror the same-named helpers in
``scripts/run_r1_macro.py`` (copied here, ~10 lines each, because that file is a
script and not cleanly importable). Keep them in sync with run_r1_macro so the
macro layer the spike builds matches the one already reviewed.

Coordinate convention: island frame (``x = x0 + (col + 0.5) * cell``), same as
``r1_macro`` / ``r1_arm_b``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import shapely
import shapely.geometry as sg
from shapely.ops import unary_union

# ---------------------------------------------------------------------------
# Boundary / mask helpers (mirror scripts/run_r1_macro.py)
# ---------------------------------------------------------------------------


def load_boundary(inputs_dir: Path) -> sg.Polygon:
    """Load the island boundary Polygon, handling FC/Feature/geometry forms.

    Mirrors ``scripts/run_r1_macro.py::load_boundary`` (kept in sync).
    """
    raw = json.loads((inputs_dir / "island_boundary.geojson").read_text())
    if raw.get("type") == "FeatureCollection":
        geometry = raw["features"][0]["geometry"]
    elif raw.get("type") == "Feature":
        geometry = raw["geometry"]
    else:
        geometry = raw
    geom = sg.shape(geometry)
    if not isinstance(geom, sg.Polygon):
        raise ValueError(f"island boundary must be a Polygon, got {geom.geom_type}")
    return geom


def boundary_mask(
    boundary: sg.Polygon,
    x0: float,
    y0: float,
    cell: float,
    nrows: int,
    ncols: int,
) -> np.ndarray:
    """Inside-island boolean mask from boundary + cell-centre meshgrid.

    Mirrors ``scripts/run_r1_macro.py::_boundary_mask`` (kept in sync).
    """
    cols = np.arange(ncols)
    rows = np.arange(nrows)
    xc = x0 + (cols + 0.5) * cell
    yc = y0 + (rows + 0.5) * cell
    xx, yy = np.meshgrid(xc, yc)
    inside = shapely.contains_xy(boundary, xx.ravel(), yy.ravel())
    return inside.reshape(nrows, ncols)


# ---------------------------------------------------------------------------
# Block selection
# ---------------------------------------------------------------------------


def select_block_by_rank(
    blocks: list[sg.Polygon],
    frac: float = 0.5,
    *,
    sliver_floor: float | None = None,
    min_block_area: float | None = None,
    sliver_factor: float = 1.5,
) -> tuple[int, sg.Polygon]:
    """Select one mid-complexity macro-block by fractional area rank.

    Sort blocks by area ascending, drop slivers (area below ``sliver_floor``),
    then return the block at fractional rank ``frac`` among the survivors
    (``frac=0.5`` -> median area, ``frac=0.0`` -> smallest survivor,
    ``frac=1.0`` -> largest).

    The sliver floor is ``sliver_floor`` when given, else
    ``sliver_factor * min_block_area`` (the macro layer's sliver-merge floor;
    e.g. ``1.5x``), else 0 (keep all). The returned index is the position in the
    *original* ``blocks`` list (so the caller can cross-reference geojson ids).

    Raises ``ValueError`` if ``blocks`` is empty or every block is a sliver.
    """
    if not blocks:
        raise ValueError("no macro-blocks to select from")
    if not 0.0 <= frac <= 1.0:
        raise ValueError(f"frac must be in [0, 1], got {frac}")

    if sliver_floor is None:
        if min_block_area is not None:
            sliver_floor = sliver_factor * min_block_area
        else:
            sliver_floor = 0.0

    # (original_index, area) for non-sliver blocks, sorted by area ascending.
    survivors = [
        (i, float(b.area))
        for i, b in enumerate(blocks)
        if float(b.area) >= sliver_floor
    ]
    if not survivors:
        raise ValueError(
            f"all {len(blocks)} blocks below sliver floor {sliver_floor:.3f}"
        )
    survivors.sort(key=lambda t: (t[1], t[0]))

    # Fractional rank -> integer index into the survivor list.
    rank = int(round(frac * (len(survivors) - 1)))
    rank = max(0, min(rank, len(survivors) - 1))
    chosen_idx = survivors[rank][0]
    return chosen_idx, blocks[chosen_idx]


# ---------------------------------------------------------------------------
# Seam metric
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SeamGap:
    """How cleanly a Chen district fabric fills a macro-block.

    Attributes
    ----------
    max_boundary_gap:
        Directed-Hausdorff-style max distance from any point sampled along the
        *block boundary* to the nearest Chen-district edge. Large => stretches
        of the bounding arterial are left bare (no local street meets them).
    mean_boundary_gap:
        Mean of the same per-sample boundary-to-fabric distances (a softer read
        than the max, which a single concave notch can dominate).
    uncovered_area:
        ``block.area - union(districts).area`` clipped at 0 (covered area can
        slightly exceed via Chen overshoot; see ``overshoot_area``).
    uncovered_frac:
        ``uncovered_area / block.area``. ~0 => fabric tiles the block;
        large => districts cover only part of it.
    overshoot_area:
        Area of the Chen union that falls *outside* the block (fabric spilling
        over the bounding arterials). Usually small; non-trivial values flag a
        boundary mismatch.
    n_boundary_samples:
        Number of boundary sample points used for the gap distances.
    """

    max_boundary_gap: float
    mean_boundary_gap: float
    uncovered_area: float
    uncovered_frac: float
    overshoot_area: float
    n_boundary_samples: int

    def to_dict(self) -> dict[str, float | int]:
        return {
            "max_boundary_gap": round(self.max_boundary_gap, 4),
            "mean_boundary_gap": round(self.mean_boundary_gap, 4),
            "uncovered_area": round(self.uncovered_area, 4),
            "uncovered_frac": round(self.uncovered_frac, 6),
            "overshoot_area": round(self.overshoot_area, 4),
            "n_boundary_samples": self.n_boundary_samples,
        }


def _sample_ring(ring: sg.LineString, spacing: float) -> list[sg.Point]:
    """Sample points along a ring at ~``spacing`` intervals (endpoints incl.)."""
    length = float(ring.length)
    if length <= 0:
        return []
    n = max(2, int(np.ceil(length / max(spacing, 1e-9))) + 1)
    return [ring.interpolate(float(d)) for d in np.linspace(0.0, length, n)]


def seam_gap(
    districts: list[sg.Polygon],
    block: sg.Polygon,
    *,
    sample_spacing: float = 1.0,
) -> SeamGap:
    """Quantify how well a Chen district fabric fills ``block``.

    Two complementary measures (kept deliberately simple):

    (a) **Boundary gap.** Sample the block boundary at ``sample_spacing``
        intervals and, for each sample, measure the distance to the nearest
        Chen *district edge* (the union's boundary). The max is a
        directed-Hausdorff-style read of the worst bare stretch of bounding
        arterial; the mean is a softer summary. If there are no districts, both
        gaps are reported as the longest block dimension (a definite "bare").

    (b) **Coverage.** ``uncovered_area = block.area - area(union ∩ block)`` and
        its fraction; plus ``overshoot_area`` for union spilling outside.

    All distances/areas are in island units (units²). Pure and deterministic.
    """
    block_area = float(block.area)
    minx, miny, maxx, maxy = block.bounds
    diag = float(np.hypot(maxx - minx, maxy - miny))

    # Boundary samples: exterior + any holes.
    rings: list[sg.LineString] = [block.exterior, *list(block.interiors)]
    samples: list[sg.Point] = []
    for ring in rings:
        samples.extend(_sample_ring(sg.LineString(ring), sample_spacing))
    n_samples = len(samples)

    if not districts:
        # No fabric: every boundary point is maximally bare; nothing covered.
        return SeamGap(
            max_boundary_gap=diag,
            mean_boundary_gap=diag,
            uncovered_area=max(block_area, 0.0),
            uncovered_frac=1.0 if block_area > 0 else 0.0,
            overshoot_area=0.0,
            n_boundary_samples=n_samples,
        )

    union = unary_union(districts)
    fabric_edge = union.boundary

    if n_samples > 0 and not fabric_edge.is_empty:
        dists = [float(p.distance(fabric_edge)) for p in samples]
        max_gap = max(dists)
        mean_gap = float(np.mean(dists))
    else:
        max_gap = diag
        mean_gap = diag

    inside = union.intersection(block)
    covered = float(inside.area)
    uncovered = max(block_area - covered, 0.0)
    uncovered_frac = (uncovered / block_area) if block_area > 0 else 0.0
    overshoot = max(float(union.area) - covered, 0.0)

    return SeamGap(
        max_boundary_gap=max_gap,
        mean_boundary_gap=mean_gap,
        uncovered_area=uncovered,
        uncovered_frac=uncovered_frac,
        overshoot_area=overshoot,
        n_boundary_samples=n_samples,
    )
