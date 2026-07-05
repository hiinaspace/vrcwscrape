"""R1 stage G0 — world -> district assignment and per-world lot geometry.

Greybox wave (``docs/greybox-plan.md``, Stage G0) introduced this module as a
deliberately naive per-district Voronoi tessellation. The lots/buildings wave
(``docs/lots-wave-plan.md``, slice L1) replaces the DEFAULT lot mechanism with
street-fronting subdivision + exact Hungarian assignment (:func:`build_lots`
-> :func:`subdivide_district`), keeping the old Voronoi tessellation as a named
fallback (:func:`_build_lots_voronoi`) for districts where subdivision fails
(invalid geometry / pathological concavity / cannot reach the requested lot
count).

Design decision (user-approved, ``docs/lots-wave-plan.md``): worlds are
allowed BOUNDED displacement -- district membership is decided from a world's
TRUE (DR) coordinate (:func:`assign_worlds_to_districts`, unchanged), but each
world is then moved onto its assigned lot's anchor point. Displacement is
bounded by the district's own OBB diagonal, since membership was already
locked in from the true coordinate before any lot geometry exists. The
original coordinate is preserved on ``Lot.x``/``Lot.y``; ``Lot.lot_x``/
``Lot.lot_y`` carry the assigned anchor and ``Lot.displacement`` the distance
between them.

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
from typing import Any

import numpy as np
import polars as pl
import shapely.geometry as sg
from scipy.optimize import linear_sum_assignment
from shapely.ops import split as shapely_split
from shapely.ops import unary_union, voronoi_diagram
from shapely.strtree import STRtree

# ---------------------------------------------------------------------------
# Defaults (docs/greybox-plan.md Stage G0 / docs/lots-wave-plan.md slice L1)
# ---------------------------------------------------------------------------

DEFAULT_INSET: float = 0.05
DEFAULT_MIN_LOT_AREA_FRAC: float = 0.01
DEFAULT_H_BASE: float = 4.0
DEFAULT_H_SCALE: float = 12.0

# Subdivision defaults (LotConfig).
DEFAULT_STOP_AREA_FACTOR: float = 1.5
DEFAULT_SPLIT_JITTER: float = 0.15
DEFAULT_ASPECT_CLAMP: float = 4.0
DEFAULT_MIN_FRONTAGE: float = 0.0
DEFAULT_BUILDING_WIDTH_FRAC: tuple[float, float] = (0.55, 0.85)
DEFAULT_BUILDING_DEPTH_FRAC: tuple[float, float] = (0.55, 0.85)
DEFAULT_MAX_SPLIT_DEPTH: int = 24

# Deterministic duplicate-coordinate jitter radius (island units), well below
# any realistic min_lot_area_frac floor so it never visibly perturbs a lot.
# Only used by the Voronoi fallback path (_build_lots_voronoi).
_JITTER_EPS: float = 1e-6

# Buffer tolerance (island units) used to test "lies on the district exterior
# ring" -- absorbs float noise from repeated split() calls, not a design knob.
_FRONTAGE_TOUCH_TOL: float = 1e-6


class _SubdivisionFailure(Exception):
    """Raised internally when :func:`subdivide_district` cannot produce a
    valid >= ``n_lots`` partition of the district (pathological concavity,
    invalid geometry, or a split failure that persists after the bounded
    largest-first re-split retries). Caught by :func:`build_lots`, which
    falls back to :func:`_build_lots_voronoi` for that district."""


@dataclass(frozen=True)
class LotConfig:
    """Tunables for the street-fronting subdivision + Hungarian assignment path.

    - ``stop_area_factor``: a subdivided piece stops splitting once its area
      is <= ``stop_area_factor * target_area`` (``target_area = district_area
      / n_lots``).
    - ``split_jitter``: each candidate split's quantile is drawn from
      ``U(0.5 - split_jitter, 0.5 + split_jitter)`` of the OBB axis extent
      (seeded ``numpy.random.default_rng``), so cuts are never perfectly
      regular grid lines.
    - ``aspect_clamp``: candidate splits are scored down once a child's OBB
      aspect ratio (long/short side) exceeds this.
    - ``min_frontage``: minimum contact length (island units) with the
      district exterior ring for a boundary edge/piece to count as real
      frontage; ``0.0`` means any nonzero contact counts.
    - ``building_width_frac`` / ``building_depth_frac``: (min, max) clamp on
      the fraction of the lot's frontage-aligned OBB width/depth used to size
      the building footprint (the raw fraction is the lot's own OBB fill
      ratio, sqrt'd so it scales like a length rather than an area).
    - ``max_split_depth``: recursion guard (subdivision naturally halts via
      ``stop_area_factor`` long before this in any non-pathological case).
    - ``inset``: building footprint inward buffer -- same meaning as the
      module-level ``DEFAULT_INSET`` kwarg on :func:`build_lots`.
    - ``h_base`` / ``h_scale``: unchanged height formula constants.
    - ``voronoi_min_lot_area_frac``: sliver floor used ONLY by the Voronoi
      FALLBACK path (:func:`_build_lots_voronoi`), same meaning as the
      existing ``min_lot_area_frac`` kwarg.
    """

    stop_area_factor: float = DEFAULT_STOP_AREA_FACTOR
    split_jitter: float = DEFAULT_SPLIT_JITTER
    aspect_clamp: float = DEFAULT_ASPECT_CLAMP
    min_frontage: float = DEFAULT_MIN_FRONTAGE
    building_width_frac: tuple[float, float] = DEFAULT_BUILDING_WIDTH_FRAC
    building_depth_frac: tuple[float, float] = DEFAULT_BUILDING_DEPTH_FRAC
    max_split_depth: int = DEFAULT_MAX_SPLIT_DEPTH
    inset: float = DEFAULT_INSET
    h_base: float = DEFAULT_H_BASE
    h_scale: float = DEFAULT_H_SCALE
    voronoi_min_lot_area_frac: float = DEFAULT_MIN_LOT_AREA_FRAC


@dataclass(frozen=True)
class Lot:
    """One world's district membership + tessellated lot + building footprint.

    ``lot`` is the full subdivided-lot (or Voronoi-cell-clipped-to-district,
    on the fallback path) polygon; ``footprint`` is the inset building pad
    inside it. ``assigned`` is ``"direct"`` (point-in-polygon), ``"snapped"``
    (nearest-district fallback, see :func:`assign_worlds_to_districts`), or
    ``""`` for a ``kind="greenspace"`` lot (no world assigned, see below).
    ``x``/``y`` are the world's ORIGINAL (true DR) coordinates -- these never
    move. ``lot_x``/``lot_y`` are the assigned lot's anchor point (the point
    the world's building actually sits at); ``displacement`` is the Euclidean
    distance between them, bounded by the district's own OBB diagonal (see
    the module docstring's bounded-displacement design decision).

    ``kind`` is ``"lot"`` (occupied: a world assigned to it) or
    ``"greenspace"`` (surplus subdivision piece with no world -- ``world_id``
    is ``""``, ``footprint`` is an empty ``Polygon`` (no building), ``height``
    is ``0.0``, ``name`` is ``None``, ``visits`` is ``0``, and ``x``/``y``/
    ``lot_x``/``lot_y`` all equal the piece's own anchor point since there is
    no original world coordinate to preserve).
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
    assigned: str  # "direct" | "snapped" | "" (greenspace)
    lot_x: float
    lot_y: float
    kind: str  # "lot" | "greenspace"
    displacement: float


# ---------------------------------------------------------------------------
# World -> district assignment (UNCHANGED -- see docs/chen-strict-reimplementation.md
# sibling contract doc's extension pattern; this function is not part of the
# lots-wave-plan L1 scope, only build_lots's internals are).
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
    duplicate-coordinate jitter (Voronoi fallback path only).
    """
    sub = points[row_indices]
    return sub.with_columns(
        pl.Series("assigned", [assigned_kind[i] for i in row_indices])
    )


# ---------------------------------------------------------------------------
# Shared geometry helpers
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


def _poly_sort_key(poly: sg.Polygon) -> tuple[float, float]:
    """Deterministic ordering key: rounded centroid, so a fixed traversal
    order never depends on shapely's internal geometry-collection ordering."""
    c = poly.centroid
    return (round(c.x, 9), round(c.y, 9))


# ---------------------------------------------------------------------------
# Street-fronting subdivision (docs/lots-wave-plan.md slice L1)
# ---------------------------------------------------------------------------


def _obb_axes(
    poly: sg.Polygon,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, float]:
    """``poly``'s ``minimum_rotated_rectangle`` center + unit long/short axes
    (as ``(x, y)`` numpy vectors) + their lengths. Degenerate polygons (a
    rotated rect with < 4 distinct corners, or a zero-area sliver) fall back
    to an axis-aligned unit square sized to ``sqrt(area)``."""
    rect = poly.minimum_rotated_rectangle
    if rect.geom_type != "Polygon":
        c = np.array([poly.centroid.x, poly.centroid.y])
        side = math.sqrt(max(poly.area, 1e-12))
        return c, np.array([1.0, 0.0]), np.array([0.0, 1.0]), side, side
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    if len(coords) < 4:
        c = np.array([poly.centroid.x, poly.centroid.y])
        side = math.sqrt(max(poly.area, 1e-12))
        return c, np.array([1.0, 0.0]), np.array([0.0, 1.0]), side, side
    edges = np.roll(coords, -1, axis=0) - coords
    lens = np.linalg.norm(edges, axis=1)
    i = int(np.argmax(lens))
    j = (i + 1) % len(lens)
    axis_long = edges[i] / max(float(lens[i]), 1e-12)
    axis_short = np.array([-axis_long[1], axis_long[0]])
    center = coords.mean(axis=0)
    return center, axis_long, axis_short, float(lens[i]), float(lens[j])


def _lot_irregularity(poly: sg.Polygon) -> float:
    """Adapted from ``city_layout._polygon_irregularity``: fill-ratio penalty
    (how much of the OBB the polygon actually fills) + OBB aspect penalty +
    perimeter-vs-OBB-perimeter penalty. Lower is more rectangle-like."""
    if poly.is_empty or poly.area <= 1e-12:
        return 1e6
    rect = poly.minimum_rotated_rectangle
    rect_area = max(float(rect.area), 1e-12)
    fill_penalty = max(0.0, 1.0 - float(poly.area) / rect_area)
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    if len(coords) >= 4:
        edges = np.roll(coords, -1, axis=0) - coords
        lens = np.linalg.norm(edges, axis=1)
        long = float(max(lens.max(), 1e-12))
        short = float(max(lens.min(), 1e-12))
        aspect_penalty = abs(math.log(max(long / short, 1e-9))) * 0.18
        rect_perimeter = max(float(lens.sum()), 1e-12)
    else:
        aspect_penalty = 1.0
        rect_perimeter = max(math.sqrt(rect_area) * 4.0, 1e-12)
    perimeter_penalty = max(0.0, float(poly.length) / rect_perimeter - 1.0) * 0.35
    return fill_penalty + aspect_penalty + perimeter_penalty


def _lot_aspect_ratio(poly: sg.Polygon) -> float:
    """Adapted from ``city_layout._polygon_aspect_ratio``: OBB long/short side ratio."""
    if poly.is_empty or poly.area <= 1e-12:
        return 1e6
    rect = poly.minimum_rotated_rectangle
    if rect.geom_type != "Polygon":
        return 1.0
    coords = np.asarray(rect.exterior.coords[:-1], dtype=np.float64)
    if len(coords) < 4:
        return 1.0
    edges = np.roll(coords, -1, axis=0) - coords
    lens = np.linalg.norm(edges, axis=1)
    short = float(max(lens.min(), 1e-12))
    return float(max(lens.max(), short) / short)


def _frontage_length(piece: sg.Polygon, ext_ring: sg.LinearRing) -> float:
    """Total boundary length of ``piece`` lying on ``ext_ring`` (small buffer tol)."""
    if piece.is_empty:
        return 0.0
    buffered = ext_ring.buffer(_FRONTAGE_TOUCH_TOL)
    shared = piece.boundary.intersection(buffered)
    return float(shared.length) if hasattr(shared, "length") else 0.0


def _touches_ext_ring(
    piece: sg.Polygon, ext_ring: sg.LinearRing, min_frontage: float
) -> bool:
    return _frontage_length(piece, ext_ring) > max(min_frontage, 1e-9)


def _split_candidates(
    poly: sg.Polygon, cfg: LotConfig, rng: np.random.Generator
) -> list[sg.LineString]:
    """Candidate cut lines: 2 jittered quantiles on the OBB long axis + 2 on
    the short axis (``rng.uniform`` draws, in that fixed order -- the
    determinism contract for :func:`subdivide_district`)."""
    center, axis_long, axis_short, long_len, short_len = _obb_axes(poly)
    half_len = 2.0 * math.hypot(long_len, short_len) + 1e-6
    lines: list[sg.LineString] = []
    for along_dir, along_len, cut_dir in (
        (axis_long, long_len, axis_short),
        (axis_short, short_len, axis_long),
    ):
        for _ in range(2):
            q = rng.uniform(0.5 - cfg.split_jitter, 0.5 + cfg.split_jitter)
            offset = (q - 0.5) * along_len
            point = center + offset * along_dir
            p0 = point - half_len * cut_dir
            p1 = point + half_len * cut_dir
            lines.append(sg.LineString([tuple(p0), tuple(p1)]))
    return lines


def _best_split(
    poly: sg.Polygon,
    ext_ring: sg.LinearRing,
    cfg: LotConfig,
    rng: np.random.Generator,
) -> list[sg.Polygon] | None:
    """Try the candidate cut lines from :func:`_split_candidates`, score valid
    binary splits (area balance + child rectangularity/aspect), and return the
    best pair -- preferring candidates where BOTH children still touch the
    district exterior ring. ``None`` if no candidate produces a clean 2-piece
    split (the caller treats ``poly`` as an (oversized or unsplittable) leaf)."""
    lines = _split_candidates(poly, cfg, rng)
    min_area = 1e-9 * max(poly.area, 1e-12)
    scored: list[tuple[bool, float, int, list[sg.Polygon]]] = []
    for order_idx, line in enumerate(lines):
        try:
            result = shapely_split(poly, line)
        except Exception:
            continue
        parts = [g for g in result.geoms if g.geom_type == "Polygon" and not g.is_empty]
        if len(parts) != 2:
            continue
        if parts[0].area <= min_area or parts[1].area <= min_area:
            continue
        balance = abs(parts[0].area - parts[1].area) / max(poly.area, 1e-12)
        irregularity = _lot_irregularity(parts[0]) + _lot_irregularity(parts[1])
        aspect_pen = 0.0
        for p in parts:
            ar = _lot_aspect_ratio(p)
            if ar > cfg.aspect_clamp:
                aspect_pen += ar - cfg.aspect_clamp
        score = balance * 0.5 + irregularity + aspect_pen
        frontage_bad = not all(
            _touches_ext_ring(p, ext_ring, cfg.min_frontage) for p in parts
        )
        children = sorted(parts, key=_poly_sort_key)
        scored.append((frontage_bad, score, order_idx, children))
    if not scored:
        return None
    scored.sort(key=lambda t: (t[0], t[1], t[2]))
    return scored[0][3]


def _fill_deficit(
    leaves: list[sg.Polygon],
    n_needed: int,
    ext_ring: sg.LinearRing,
    cfg: LotConfig,
    rng: np.random.Generator,
) -> list[sg.Polygon] | None:
    """Re-split the largest still-splittable leaf until ``len(leaves) >=
    n_needed``. Returns ``None`` (subdivision failure) if every remaining
    leaf refuses to split further while a deficit remains -- this always
    terminates: each iteration either grows the leaf count or permanently
    moves one leaf to ``unsplittable``, so the loop cannot spin forever."""
    splittable = list(leaves)
    unsplittable: list[sg.Polygon] = []
    while len(splittable) + len(unsplittable) < n_needed:
        if not splittable:
            return None
        idx = max(
            range(len(splittable)),
            key=lambda i: (splittable[i].area, _poly_sort_key(splittable[i])),
        )
        target = splittable.pop(idx)
        children = _best_split(target, ext_ring, cfg, rng)
        if children is None:
            unsplittable.append(target)
            continue
        splittable.extend(children)
    return splittable + unsplittable


def subdivide_district(
    district: sg.Polygon,
    n_lots: int,
    seed: int,
    cfg: LotConfig | None = None,
) -> list[sg.Polygon]:
    """Recursive OBB/strip subdivision of ``district`` into >= ``n_lots`` lots
    that PARTITION it (union == district, interiors pairwise disjoint).

    That partition invariant is achieved NATURALLY, never by independent
    clipping: every cut is a ``shapely.ops.split`` of one piece into exactly
    two, so each step's pieces exactly tile their parent by construction and
    the whole recursion tiles ``district`` inductively.

    At each step the current piece is split across its OBB long axis at a
    jittered midpoint quantile (seeded ``numpy.random.default_rng(seed)``),
    recursing until pieces reach target area (``district.area / n_lots``)
    times ``cfg.stop_area_factor``. Candidate cut lines also try the short
    axis / a second quantile (:func:`_best_split`), scored on area balance +
    child rectangularity/aspect, preferring cuts where both children still
    touch the district's exterior ring (the street network for these Chen
    leaf-fabric districts) -- pieces that end up with no such contact are
    still kept (not discarded), they simply participate normally in
    :func:`build_lots`'s Hungarian assignment / greenspace bucketing same as
    any other piece.

    If the main recursion under-delivers (fewer than ``n_lots`` leaves --
    e.g. every piece already sits at or below the stop-area threshold), the
    largest leaf is re-split (ignoring the stop-area threshold) until the
    count is met (:func:`_fill_deficit`). Raises :class:`_SubdivisionFailure`
    if that deficit cannot be filled (pathological concavity / a shape no
    candidate line can ever cleanly bisect) -- :func:`build_lots` catches
    this and falls back to :func:`_build_lots_voronoi`.

    Deterministic: identical ``(district, n_lots, seed, cfg)`` always yields
    identical output (same WKBs, same order) -- the RNG is only ever advanced
    in one fixed traversal order (children always processed in
    ``_poly_sort_key`` order), never influenced by dict/set iteration.
    """
    if cfg is None:
        cfg = LotConfig()
    if n_lots <= 1 or district.is_empty or district.area <= 0.0:
        return [district]

    ext_ring = district.exterior
    rng = np.random.default_rng(seed)
    target_area = district.area / float(n_lots)

    leaves: list[sg.Polygon] = []

    def _recurse(poly: sg.Polygon, depth: int) -> None:
        if (
            poly.area <= cfg.stop_area_factor * target_area
            or depth >= cfg.max_split_depth
        ):
            leaves.append(poly)
            return
        children = _best_split(poly, ext_ring, cfg, rng)
        if children is None:
            leaves.append(poly)
            return
        for child in children:
            _recurse(child, depth + 1)

    _recurse(district, 0)

    if len(leaves) < n_lots:
        filled = _fill_deficit(leaves, n_lots, ext_ring, cfg, rng)
        if filled is None:
            raise _SubdivisionFailure(
                f"could not reach {n_lots} lots from district (got {len(leaves)})"
            )
        leaves = filled

    leaves.sort(key=_poly_sort_key)
    return leaves


def _assert_partition(
    district: sg.Polygon, lot_polys: list[sg.Polygon], rel_tol: float = 1e-6
) -> None:
    """Defensive runtime check of the partition invariant :func:`subdivide_district`
    documents as "achieved naturally" -- guards against an unexpected
    topology edge case slipping through, in which case :func:`build_lots`
    falls back to Voronoi rather than exporting bad geometry."""
    if not lot_polys:
        raise _SubdivisionFailure("empty subdivision result")
    total = max(district.area, 1e-12)
    union = unary_union(lot_polys)
    if abs(union.area - district.area) > rel_tol * total:
        raise _SubdivisionFailure("subdivision union area does not match district area")
    tree = STRtree(lot_polys)
    for i, poly in enumerate(lot_polys):
        for j in sorted(int(k) for k in tree.query(poly)):
            if j <= i:
                continue
            if poly.intersection(lot_polys[j]).area > rel_tol * total:
                raise _SubdivisionFailure("subdivision lots overlap")


def _frontage_direction(
    lot_poly: sg.Polygon, ext_ring: sg.LinearRing, min_frontage: float
) -> np.ndarray | None:
    """Unit direction of the LONGEST boundary edge of ``lot_poly`` that lies
    on ``ext_ring`` (the district's street frontage), or ``None`` if no edge
    qualifies (``>= min_frontage`` long and fully covered by a small buffer
    around ``ext_ring``) -- the caller falls back to the lot's own OBB long
    axis in that case."""
    buffered = ext_ring.buffer(_FRONTAGE_TOUCH_TOL)
    coords = list(lot_poly.exterior.coords)
    best_len = max(min_frontage, 1e-9)
    best_dir: np.ndarray | None = None
    for i in range(len(coords) - 1):
        p0, p1 = coords[i], coords[i + 1]
        seg = sg.LineString([p0, p1])
        length = seg.length
        if length <= best_len:
            continue
        if buffered.covers(seg):
            best_len = length
            d = np.array([p1[0] - p0[0], p1[1] - p0[1]])
            best_dir = d / max(float(np.linalg.norm(d)), 1e-12)
    return best_dir


def _oriented_footprint(
    lot_poly: sg.Polygon, ext_ring: sg.LinearRing, cfg: LotConfig
) -> sg.Polygon:
    """Building footprint oriented to ``lot_poly``'s frontage edge (fallback:
    its own OBB long axis), sized from its OBB fill ratio clamped into
    ``cfg.building_width_frac`` / ``cfg.building_depth_frac``, inset by
    ``cfg.inset`` and guaranteed fully inside the lot (intersected with an
    inward-buffered guard copy of it, same collapse-to-nonempty fallback as
    :func:`_footprint`)."""
    if lot_poly.is_empty or lot_poly.area <= 0.0:
        return _footprint(lot_poly, cfg.inset)

    center, axis_long, axis_short, long_len, short_len = _obb_axes(lot_poly)
    frontage_dir = _frontage_direction(lot_poly, ext_ring, cfg.min_frontage)
    if frontage_dir is not None:
        along = frontage_dir
        perp = np.array([-along[1], along[0]])
        coords = np.asarray(lot_poly.exterior.coords[:-1], dtype=np.float64)
        rel = coords - center
        proj_along = rel @ along
        proj_perp = rel @ perp
        width_dim = float(proj_along.max() - proj_along.min())
        depth_dim = float(proj_perp.max() - proj_perp.min())
        axis_w, axis_d = along, perp
    else:
        width_dim, depth_dim = long_len, short_len
        axis_w, axis_d = axis_long, axis_short

    obb_area = max(width_dim * depth_dim, 1e-12)
    fill_ratio = max(0.0, min(1.0, lot_poly.area / obb_area))
    size_frac = math.sqrt(fill_ratio)
    w_frac = min(max(size_frac, cfg.building_width_frac[0]), cfg.building_width_frac[1])
    d_frac = min(max(size_frac, cfg.building_depth_frac[0]), cfg.building_depth_frac[1])
    w = width_dim * w_frac
    d = depth_dim * d_frac

    centroid = lot_poly.centroid
    anchor = (
        centroid if lot_poly.contains(centroid) else lot_poly.representative_point()
    )
    cx, cy = anchor.x, anchor.y
    half_w, half_d = w / 2.0, d / 2.0
    corners = [
        (
            cx + s1 * half_w * axis_w[0] + s2 * half_d * axis_d[0],
            cy + s1 * half_w * axis_w[1] + s2 * half_d * axis_d[1],
        )
        for s1, s2 in ((-1, -1), (1, -1), (1, 1), (-1, 1))
    ]
    rect = sg.Polygon(corners)

    guard = lot_poly.buffer(-cfg.inset)
    guard = _largest_polygon_part(guard) if not guard.is_empty else guard
    if guard.is_empty or guard.geom_type != "Polygon":
        return _footprint(lot_poly, cfg.inset)

    footprint = _largest_polygon_part(rect.intersection(guard))
    if footprint.is_empty or footprint.area <= 0.0:
        return _footprint(lot_poly, cfg.inset)
    return footprint


def displacement_stats(lots: list[Lot]) -> dict[str, float | int]:
    """Median/p95/max ``Lot.displacement`` over OCCUPIED (``kind == "lot"``)
    lots. Greenspace lots carry no world so their (always-0.0) displacement
    isn't meaningful and is excluded. Returns zeros (never NaN/None) when
    there are no occupied lots, so callers can always index the dict."""
    values = sorted(lot.displacement for lot in lots if lot.kind == "lot")
    n = len(values)
    if n == 0:
        return {"n": 0, "median": 0.0, "p95": 0.0, "max": 0.0}
    p95_idx = min(n - 1, math.ceil(0.95 * n) - 1)
    return {
        "n": n,
        "median": float(statistics.median(values)),
        "p95": float(values[p95_idx]),
        "max": float(values[-1]),
    }


# ---------------------------------------------------------------------------
# Voronoi fallback path (the pre-L1 mechanism, kept verbatim for districts
# where subdivide_district fails -- see build_lots).
# ---------------------------------------------------------------------------


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


def _build_lots_voronoi(
    district: sg.Polygon,
    district_id: int,
    member_points: pl.DataFrame,
    *,
    inset: float = DEFAULT_INSET,
    min_lot_area_frac: float = DEFAULT_MIN_LOT_AREA_FRAC,
    h_base: float = DEFAULT_H_BASE,
    h_scale: float = DEFAULT_H_SCALE,
) -> list[Lot]:
    """The pre-L1 naive per-district Voronoi tessellation, kept verbatim as
    :func:`build_lots`'s fallback for districts where street-fronting
    subdivision fails. No world ever moves here: each Voronoi cell IS the
    generating world's own lot, so ``lot_x``/``lot_y`` == ``x``/``y`` and
    ``displacement`` is always ``0.0`` and every lot's ``kind`` is ``"lot"``
    (this path never produces greenspace).

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
                lot_x=xs[0],
                lot_y=ys[0],
                kind="lot",
                displacement=0.0,
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
                lot_x=xs[i],
                lot_y=ys[i],
                kind="lot",
                displacement=0.0,
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
                lot_x=xs[i],
                lot_y=ys[i],
                kind="lot",
                displacement=0.0,
            )
        )
    return lots


# ---------------------------------------------------------------------------
# Public entry point: street-fronting subdivision + Hungarian assignment,
# falling back to _build_lots_voronoi on subdivision failure.
# ---------------------------------------------------------------------------


def build_lots(
    district: sg.Polygon,
    district_id: int,
    member_points: pl.DataFrame,
    *,
    inset: float = DEFAULT_INSET,
    min_lot_area_frac: float = DEFAULT_MIN_LOT_AREA_FRAC,
    h_base: float = DEFAULT_H_BASE,
    h_scale: float = DEFAULT_H_SCALE,
    seed: int | None = None,
    cfg: LotConfig | None = None,
    fallback_stats: dict[str, Any] | None = None,
) -> list[Lot]:
    """Tessellate ``district`` into street-fronting lots and assign its
    member worlds onto them (Hungarian, exact) + inset building footprints.

    ``member_points`` needs ``world_id, x, y, visits, assigned`` columns
    (``name`` optional, else every ``Lot.name`` is ``None``) -- see
    :func:`select_member_points`.

    ``inset``/``min_lot_area_frac``/``h_base``/``h_scale`` are the pre-L1
    kwargs (still accepted directly so existing callers -- e.g.
    ``scripts/run_r1_hybrid.py``'s ``export_greybox`` -- keep working
    unmodified); passing an explicit ``cfg`` overrides all four at once.
    ``seed`` defaults to ``district_id`` (deterministic given a fixed
    district walk order, decorrelated across districts sharing a shape).
    ``fallback_stats``, if given a dict, gets ``"n_fallback"`` incremented
    and a ``"fallback_districts"`` list entry appended (``{"district_id",
    "reason"}``) whenever THIS district falls back to
    :func:`_build_lots_voronoi`.

    Degenerate cases:

    - **0 points**: returns ``[]`` (empty/park districts are the caller's
      concern -- they simply never call this).
    - **1 point**: the lot IS the whole district (no subdivision needed).
    - **Subdivision failure** (invalid geometry, pathological concavity, or
      :func:`subdivide_district` cannot reach ``n`` lots even after its
      largest-first re-split retries): falls back to
      :func:`_build_lots_voronoi` for this ENTIRE district (never a partial
      fallback -- mixing the two lot mechanisms within one district would
      make the partition invariant meaningless).

    Otherwise: ``subdivide_district`` produces ``M >= N`` lot polygons
    (``N = len(member_points)``); ``scipy.optimize.linear_sum_assignment``
    finds the exact minimum-total-squared-distance matching from each
    world's ORIGINAL ``(x, y)`` to every lot's anchor (``representative_point()``,
    or the centroid when it lies inside -- always a point inside the lot).
    The ``N`` matched lots become occupied (``kind="lot"``, ``displacement``
    = distance from the world's original coordinate to its lot anchor); the
    ``M - N`` unmatched surplus lots become ``kind="greenspace"`` (no world,
    no building -- see the ``Lot`` docstring for its field convention there).
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

    if cfg is None:
        cfg = LotConfig(
            inset=inset,
            h_base=h_base,
            h_scale=h_scale,
            voronoi_min_lot_area_frac=min_lot_area_frac,
        )
    if seed is None:
        seed = district_id

    ext_ring = district.exterior

    if n == 1:
        lot_poly = district
        return [
            Lot(
                world_id=world_ids[0],
                district_id=district_id,
                footprint=_oriented_footprint(lot_poly, ext_ring, cfg),
                lot=lot_poly,
                height=heights[0],
                name=names[0],
                visits=visits[0],
                x=xs[0],
                y=ys[0],
                assigned=assigned[0],
                lot_x=xs[0],
                lot_y=ys[0],
                kind="lot",
                displacement=0.0,
            )
        ]

    # Subdivision path (falls back to the old Voronoi mechanism for this
    # whole district on ANY failure -- invalid geometry, pathological
    # concavity, or an unfillable deficit -- deliberately broad because the
    # fallback exists precisely to absorb geometry pathologies we cannot
    # enumerate in advance).
    try:
        lot_polys = subdivide_district(district, n, seed, cfg)
        _assert_partition(district, lot_polys)
    except Exception as exc:
        if fallback_stats is not None:
            fallback_stats["n_fallback"] = fallback_stats.get("n_fallback", 0) + 1
            fallback_stats.setdefault("fallback_districts", []).append(
                {"district_id": district_id, "reason": str(exc)}
            )
        return _build_lots_voronoi(
            district,
            district_id,
            member_points,
            inset=cfg.inset,
            min_lot_area_frac=cfg.voronoi_min_lot_area_frac,
            h_base=cfg.h_base,
            h_scale=cfg.h_scale,
        )

    anchors: list[tuple[float, float]] = []
    for poly in lot_polys:
        centroid = poly.centroid
        anchor = centroid if poly.contains(centroid) else poly.representative_point()
        anchors.append((anchor.x, anchor.y))

    xs_arr = np.asarray(xs, dtype=np.float64)
    ys_arr = np.asarray(ys, dtype=np.float64)
    anchor_arr = np.asarray(anchors, dtype=np.float64)
    cost = (xs_arr[:, None] - anchor_arr[None, :, 0]) ** 2 + (
        ys_arr[:, None] - anchor_arr[None, :, 1]
    ) ** 2
    row_ind, col_ind = linear_sum_assignment(cost)
    lot_for_world = {int(r): int(c) for r, c in zip(row_ind, col_ind, strict=True)}

    lots: list[Lot] = []
    used_lot_idx = set(lot_for_world.values())
    for i in range(n):
        lot_idx = lot_for_world[i]
        lot_poly = lot_polys[lot_idx]
        ax, ay = anchors[lot_idx]
        lots.append(
            Lot(
                world_id=world_ids[i],
                district_id=district_id,
                footprint=_oriented_footprint(lot_poly, ext_ring, cfg),
                lot=lot_poly,
                height=heights[i],
                name=names[i],
                visits=visits[i],
                x=xs[i],
                y=ys[i],
                assigned=assigned[i],
                lot_x=ax,
                lot_y=ay,
                kind="lot",
                displacement=math.hypot(xs[i] - ax, ys[i] - ay),
            )
        )

    for j, poly in enumerate(lot_polys):
        if j in used_lot_idx:
            continue
        ax, ay = anchors[j]
        lots.append(
            Lot(
                world_id="",
                district_id=district_id,
                footprint=sg.Polygon(),
                lot=poly,
                height=0.0,
                name=None,
                visits=0,
                x=ax,
                y=ay,
                assigned="",
                lot_x=ax,
                lot_y=ay,
                kind="greenspace",
                displacement=0.0,
            )
        )

    return lots


def count_sliver_reassignments(lots: list[Lot]) -> int:
    """Count lots whose ``lot`` polygon exactly duplicates an earlier lot's.

    Pure post-hoc detection (no extra state threads through :func:`build_lots`
    itself): groups by ``(district_id, lot.wkb)`` and counts every occurrence
    after the first. A legitimate pair of DISTINCT lots sharing byte-identical
    WKB is not something the Voronoi fallback tessellation produces in
    practice (nor does the subdivision path, which never duplicates a leaf
    polygon across two ``Lot`` rows), so this is exactly the sliver-
    reassignment duplication :func:`_build_lots_voronoi` documents.
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
