"""R1 stage G0 — world -> district assignment and per-world lot geometry.

Greybox wave (``docs/greybox-plan.md``, Stage G0): the hybrid pipeline
(``mapgen.r1_macro`` / ``mapgen.r1_seam`` / ``mapgen.r1_connect``) produces
macro-blocks and per-block Chen districts, but no per-world geometry exists
anywhere yet. This module is the new (deliberately naive) piece: assign every
world to a district (:func:`assign_worlds_to_districts`), then tessellate a
district's member worlds into disjoint lots with inset building footprints
(:func:`build_lots`).

Pure module: no IO, no plotting. ``scripts/run_r1_hybrid.py`` (``--greybox-out``)
is the only caller that touches disk.

Coordinate convention: island frame (same units as ``r1_macro``/``r1_arm_b`` --
NOT meters). ``Lot.height``, however, IS meters already (``h_base``/``h_scale``
are meter constants from ``docs/greybox-plan.md``) even though ``x``/``y``/
``footprint``/``lot`` stay in island units -- G1's mesh bake applies
``--meters-per-unit`` to the planar geometry only, not to height.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass

import polars as pl
import shapely.geometry as sg
from shapely.ops import voronoi_diagram
from shapely.strtree import STRtree

# ---------------------------------------------------------------------------
# Defaults (docs/greybox-plan.md Stage G0)
# ---------------------------------------------------------------------------

DEFAULT_INSET: float = 0.05
DEFAULT_MIN_LOT_AREA_FRAC: float = 0.01
DEFAULT_H_BASE: float = 4.0
DEFAULT_H_SCALE: float = 12.0

# Deterministic duplicate-coordinate jitter radius (island units), well below
# any realistic min_lot_area_frac floor so it never visibly perturbs a lot.
_JITTER_EPS: float = 1e-6


@dataclass(frozen=True)
class Lot:
    """One world's district membership + tessellated lot + building footprint.

    ``lot`` is the full Voronoi-cell-clipped-to-district polygon; ``footprint``
    is ``lot`` inset by a fixed margin (the building pad). ``assigned`` is
    ``"direct"`` (point-in-polygon) or ``"snapped"`` (nearest-district fallback,
    see :func:`assign_worlds_to_districts`). ``x``/``y`` are the world's
    ORIGINAL (pre-jitter) coordinates.
    """

    world_id: str
    district_id: int
    footprint: sg.Polygon
    lot: sg.Polygon
    height: float
    name: str | None
    visits: int
    x: float
    y: float
    assigned: str  # "direct" | "snapped"


# ---------------------------------------------------------------------------
# World -> district assignment
# ---------------------------------------------------------------------------


def assign_worlds_to_districts(
    points: pl.DataFrame,
    districts: list[sg.Polygon],
) -> tuple[dict[int, list[int]], list[str]]:
    """Assign every row of ``points`` to a district index by point-in-polygon.

    ``points`` needs at least ``world_id, x, y, visits`` columns (``name`` is
    optional and unused here -- it is only consumed by :func:`build_lots`).
    Point-in-polygon uses a :class:`~shapely.strtree.STRtree` over
    ``districts`` (``intersects``, so a world sitting exactly on a shared
    district edge still counts as "direct" rather than falling through to the
    snap path). A world matching no district (street ribbons, failed macro
    blocks, coastal slivers) snaps to the NEAREST district by point-to-polygon
    distance (``STRtree.query_nearest``, ``all_matches=True`` so an exact
    distance tie breaks deterministically on the lowest district index rather
    than on the tree's internal, unspecified visitation order).

    Returns ``(assignment, assigned_kind)``:

    - ``assignment``: ``district index -> list of point row indices`` (row
      position in ``points``), ascending and therefore deterministic
      regardless of any dict/set iteration.
    - ``assigned_kind``: ``"direct"`` or ``"snapped"``, one entry per row, in
      the SAME order as ``points`` (not the assignment's per-district order).

    A ``points`` with zero rows or an empty ``districts`` list returns empty
    (all-districts-empty) results without raising.
    """
    n = points.height
    assignment: dict[int, list[int]] = {i: [] for i in range(len(districts))}
    assigned_kind: list[str] = ["direct"] * n
    if n == 0 or not districts:
        return assignment, assigned_kind

    xs = points["x"].to_numpy()
    ys = points["y"].to_numpy()

    tree = STRtree(districts)
    for row in range(n):
        pt = sg.Point(float(xs[row]), float(ys[row]))
        candidates = sorted(int(i) for i in tree.query(pt))
        matched: int | None = None
        for idx in candidates:
            if districts[idx].intersects(pt):
                matched = idx
                break
        if matched is None:
            idx_arr, _dist_arr = tree.query_nearest(
                pt, return_distance=True, all_matches=True
            )
            matched = int(min(int(i) for i in idx_arr))
            assigned_kind[row] = "snapped"
        assignment[matched].append(row)

    return assignment, assigned_kind


def select_member_points(
    points: pl.DataFrame,
    row_indices: list[int],
    assigned_kind: list[str],
) -> pl.DataFrame:
    """Slice ``points`` to ``row_indices`` (order preserved) with ``assigned``.

    Thin convenience for callers wiring :func:`assign_worlds_to_districts`
    into :func:`build_lots`: picks the rows for one district (in the SAME
    order given -- ascending, if ``row_indices`` came straight from
    ``assignment[district_id]``) and attaches the matching per-row
    ``assigned_kind`` entries as a new ``"assigned"`` column. That row order
    is what :func:`build_lots` treats as "row index" for its deterministic
    duplicate-coordinate jitter.
    """
    sub = points[row_indices]
    return sub.with_columns(
        pl.Series("assigned", [assigned_kind[i] for i in row_indices])
    )


# ---------------------------------------------------------------------------
# Per-district lot tessellation
# ---------------------------------------------------------------------------


def _height_from_visits(visits: int, h_base: float, h_scale: float) -> float:
    """``h_base + h_scale * log10(1 + visits)`` (meters); ``visits < 0`` clamps to 0."""
    return h_base + h_scale * math.log10(1.0 + float(max(visits, 0)))


def _largest_polygon_part(geom: sg.base.BaseGeometry) -> sg.Polygon:
    """Return ``geom``'s largest ``Polygon`` part, or an empty Polygon."""
    if geom.is_empty:
        return sg.Polygon()
    if geom.geom_type == "Polygon":
        return geom
    if geom.geom_type in ("MultiPolygon", "GeometryCollection"):
        parts = [g for g in geom.geoms if g.geom_type == "Polygon" and not g.is_empty]
        if parts:
            return max(parts, key=lambda p: p.area)
    return sg.Polygon()


def _footprint(lot_poly: sg.Polygon, inset: float) -> sg.Polygon:
    """Inset ``lot_poly`` by ``inset`` (negative buffer) for the building pad.

    Falls back to the largest part when the inward buffer splits into a
    MultiPolygon, and to ``lot_poly.representative_point().buffer(inset)``
    when the buffer collapses to empty entirely (a tiny/sliver lot) or
    ``lot_poly`` itself is degenerate -- every world gets SOME footprint.
    """
    if lot_poly.is_empty or lot_poly.area <= 0.0:
        point = lot_poly.centroid if not lot_poly.is_empty else sg.Point(0.0, 0.0)
        return point.buffer(inset)
    buffered = lot_poly.buffer(-inset)
    buffered = _largest_polygon_part(buffered) if not buffered.is_empty else buffered
    if buffered.is_empty or buffered.geom_type != "Polygon":
        return lot_poly.representative_point().buffer(inset)
    return buffered


def _jitter_duplicates(
    xs: list[float], ys: list[float]
) -> tuple[list[float], list[float]]:
    """Deterministic epsilon jitter for exact/near-duplicate coordinates.

    Keyed by POSITION in ``xs``/``ys`` (the "row index" within the frame the
    caller built) -- not by any external id -- so re-running with the same
    input frame always produces byte-identical jitter. The first occurrence
    of a coordinate is left untouched; each later duplicate is nudged
    ``_JITTER_EPS * duplicate_count`` along a row-index-derived angle so
    Voronoi never sees two coincident generator points (which it cannot
    tessellate between).
    """
    seen: dict[tuple[float, float], int] = {}
    jx = list(xs)
    jy = list(ys)
    for i, (x, y) in enumerate(zip(xs, ys, strict=True)):
        key = (round(x, 9), round(y, 9))
        count = seen.get(key, 0)
        if count > 0:
            angle = 2.0 * math.pi * ((i * 2654435761 + count) % 360) / 360.0
            radius = _JITTER_EPS * count
            jx[i] = x + radius * math.cos(angle)
            jy[i] = y + radius * math.sin(angle)
        seen[key] = count + 1
    return jx, jy


def _match_generator_to_cell(
    cells: list[sg.Polygon], cell_tree: STRtree, x: float, y: float
) -> int:
    """Index into ``cells`` of the Voronoi cell generated by point ``(x, y)``.

    Prefers exact containment (``intersects``, boundary-inclusive); falls
    back to the nearest cell (ties broken by lowest index) for the rare
    floating-point edge case where a generator sits exactly on a cell
    boundary and no candidate reports ``intersects`` true.
    """
    pt = sg.Point(x, y)
    candidates = sorted(int(i) for i in cell_tree.query(pt))
    for idx in candidates:
        if cells[idx].intersects(pt):
            return idx
    idx_arr, _dist_arr = cell_tree.query_nearest(
        pt, return_distance=True, all_matches=True
    )
    return int(min(int(i) for i in idx_arr))


def build_lots(
    district: sg.Polygon,
    district_id: int,
    member_points: pl.DataFrame,
    *,
    inset: float = DEFAULT_INSET,
    min_lot_area_frac: float = DEFAULT_MIN_LOT_AREA_FRAC,
    h_base: float = DEFAULT_H_BASE,
    h_scale: float = DEFAULT_H_SCALE,
) -> list[Lot]:
    """Tessellate ``district``'s member worlds into lots + building footprints.

    ``member_points`` needs ``world_id, x, y, visits, assigned`` columns
    (``name`` optional, else every ``Lot.name`` is ``None``) -- see
    :func:`select_member_points`. Its ROW ORDER is the "row index" the
    duplicate-coordinate jitter is keyed on, so calling this twice with the
    same frame is always byte-identical.

    Degenerate cases:

    - **0 points**: returns ``[]`` (empty/park districts are the caller's
      concern -- they simply never call this).
    - **1 point**: the lot IS the whole district (no Voronoi needed).
    - **Duplicate/near-duplicate coordinates**: jittered by
      :func:`_jitter_duplicates` before the Voronoi call; the ORIGINAL
      (pre-jitter) ``x``/``y`` are kept on the returned ``Lot``.
    - **Sliver cells**: any post-clip lot polygon whose area falls below
      ``min_lot_area_frac * median(nonzero lot areas)`` is dropped; that
      world is reassigned the geometry of its nearest SURVIVING lot (by
      generator-to-generator distance) -- so its ``footprint``/``lot``
      duplicate the survivor's exactly. (Callers can count these after the
      fact with a same-district-same-WKB check; see the module tests.)
    - **Inward-buffer collapse**: handled by :func:`_footprint`.

    Per-district Voronoi: :func:`shapely.ops.voronoi_diagram` over the
    (jittered) points, clipped to an envelope no larger than ``district``
    itself (belt-and-suspenders: cells are ALSO explicitly intersected with
    ``district`` afterward), then matched back to their generating point
    (:func:`_match_generator_to_cell`).
    """
    n = member_points.height
    if n == 0:
        return []

    world_ids = [str(v) for v in member_points["world_id"].to_list()]
    xs = [float(v) for v in member_points["x"].to_list()]
    ys = [float(v) for v in member_points["y"].to_list()]
    visits = [int(v) for v in member_points["visits"].to_list()]
    assigned = [str(v) for v in member_points["assigned"].to_list()]
    if "name" in member_points.columns:
        names: list[str | None] = [
            None if v is None else str(v) for v in member_points["name"].to_list()
        ]
    else:
        names = [None] * n

    heights = [_height_from_visits(v, h_base, h_scale) for v in visits]

    if n == 1:
        lot_poly = district
        return [
            Lot(
                world_id=world_ids[0],
                district_id=district_id,
                footprint=_footprint(lot_poly, inset),
                lot=lot_poly,
                height=heights[0],
                name=names[0],
                visits=visits[0],
                x=xs[0],
                y=ys[0],
                assigned=assigned[0],
            )
        ]

    jx, jy = _jitter_duplicates(xs, ys)

    points = sg.MultiPoint(list(zip(jx, jy, strict=True)))
    diagram = voronoi_diagram(points, envelope=district)
    cells = [g for g in diagram.geoms if g.geom_type == "Polygon" and not g.is_empty]
    if not cells:
        # Degenerate arrangement (e.g. all points collinear): fall back to
        # every world getting the whole district, so output is never empty.
        return [
            Lot(
                world_id=world_ids[i],
                district_id=district_id,
                footprint=_footprint(district, inset),
                lot=district,
                height=heights[i],
                name=names[i],
                visits=visits[i],
                x=xs[i],
                y=ys[i],
                assigned=assigned[i],
            )
            for i in range(n)
        ]

    cell_tree = STRtree(cells)
    clipped: list[sg.Polygon] = []
    for i in range(n):
        cell_idx = _match_generator_to_cell(cells, cell_tree, jx[i], jy[i])
        clipped.append(_largest_polygon_part(cells[cell_idx].intersection(district)))

    areas = [c.area for c in clipped]
    nonzero_areas = [a for a in areas if a > 0.0]
    median_area = statistics.median(nonzero_areas) if nonzero_areas else 0.0
    floor = min_lot_area_frac * median_area

    survivor_idx = [i for i in range(n) if areas[i] >= floor and areas[i] > 0.0]
    if not survivor_idx:
        # Every cell is a "sliver" (or degenerate to zero area) -- keep them
        # all rather than producing an empty district.
        survivor_idx = list(range(n))

    final_polys: list[sg.Polygon] = list(clipped)
    survivor_set = set(survivor_idx)
    for i in range(n):
        if i in survivor_set:
            continue
        nearest = min(
            survivor_idx,
            key=lambda j: (math.hypot(xs[j] - xs[i], ys[j] - ys[i]), j),
        )
        final_polys[i] = final_polys[nearest]

    lots: list[Lot] = []
    for i in range(n):
        lot_poly = final_polys[i]
        lots.append(
            Lot(
                world_id=world_ids[i],
                district_id=district_id,
                footprint=_footprint(lot_poly, inset),
                lot=lot_poly,
                height=heights[i],
                name=names[i],
                visits=visits[i],
                x=xs[i],
                y=ys[i],
                assigned=assigned[i],
            )
        )
    return lots


def count_sliver_reassignments(lots: list[Lot]) -> int:
    """Count lots whose ``lot`` polygon exactly duplicates an earlier lot's.

    Pure post-hoc detection (no extra state threads through :func:`build_lots`
    itself): groups by ``(district_id, lot.wkb)`` and counts every occurrence
    after the first. A legitimate pair of DISTINCT lots sharing byte-identical
    WKB is not something Voronoi tessellation produces in practice, so this is
    exactly the sliver-reassignment duplication :func:`build_lots` documents.
    """
    seen: set[tuple[int, bytes]] = set()
    count = 0
    for lot in lots:
        key = (lot.district_id, lot.lot.wkb)
        if key in seen:
            count += 1
        else:
            seen.add(key)
    return count
