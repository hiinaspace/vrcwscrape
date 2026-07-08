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
world is then moved onto its assigned lot's anchor point. For an
``assigned="direct"`` world (point-in-polygon membership), displacement IS
bounded by the district's own OBB diagonal, since its true coordinate already
lay inside the district before any lot geometry existed. For an
``assigned="snapped"`` world (no district contained its true coordinate, so
:func:`assign_worlds_to_districts` snapped it to the NEAREST district), the
true coordinate can sit arbitrarily far OUTSIDE that district -- its
displacement is the snap distance PLUS a district-scale term and is NOT
bounded by the district's own size. :func:`displacement_stats` reports both
the combined (manifest-compat) stats and a per-``assigned``-kind split so this
distinction stays visible rather than getting averaged away. The original
coordinate is preserved on ``Lot.x``/``Lot.y``; ``Lot.lot_x``/``Lot.lot_y``
carry the assigned anchor and ``Lot.displacement`` the distance between them.

Pure module: no IO, no plotting. ``scripts/run_r1_hybrid.py`` (``--greybox-out``)
is the only caller that touches disk.

Coordinate convention: island frame (same units as ``r1_macro``/``r1_arm_b`` --
NOT meters). ``Lot.height``, however, IS meters already (:class:`MassingConfig`
constants are meter constants) even though ``x``/``y``/``footprint``/``lot``
stay in island units -- G1's mesh bake applies ``--meters-per-unit`` to the
planar geometry only, not to height.

Wave 2 sub-wave 2a (``docs/wave2-plan.md``, "The massing model") replaces the
old visits-driven height / fill-ratio footprint sizing with a suburb-default
typology model: :func:`classify_typology` picks ``"detached"``/``"row"``/
``"landmark"`` per world from area-per-world + a popularity top-N
(:func:`top_landmark_ids`); :func:`_massing_height` and :func:`_oriented_footprint`
size the building from real METER setbacks (:class:`MassingConfig`) converted
to island units via ``meters_per_unit``, denominated from the FRONTAGE LOT
LINE (not the lot's own OBB) -- this is what keeps buildings off the road
ribbon (docs/wave2-plan.md root cause 3), unlike the old inset-only footprint.
"""

from __future__ import annotations

import hashlib
import math
import statistics
from dataclasses import dataclass, replace
from typing import Any

import numpy as np
import polars as pl
import shapely.geometry as sg
from scipy.optimize import linear_sum_assignment
from shapely import set_precision
from shapely.ops import split as shapely_split
from shapely.ops import unary_union, voronoi_diagram
from shapely.strtree import STRtree

# ---------------------------------------------------------------------------
# Defaults (docs/greybox-plan.md Stage G0 / docs/lots-wave-plan.md slice L1)
# ---------------------------------------------------------------------------

DEFAULT_INSET: float = 0.05
DEFAULT_MIN_LOT_AREA_FRAC: float = 0.01

# Subdivision defaults (LotConfig).
DEFAULT_STOP_AREA_FACTOR: float = 1.5
DEFAULT_SPLIT_JITTER: float = 0.15
DEFAULT_ASPECT_CLAMP: float = 4.0
DEFAULT_MIN_FRONTAGE: float = 0.0
DEFAULT_MAX_SPLIT_DEPTH: int = 24

# Hard shape-floor (docs/macro-roads-nuclei-plan.md F1 "sliver shape-floor"):
# unlike aspect_clamp above (a soft score-down), these REJECT a candidate
# split outright once either child would violate them -- see
# _meets_shape_floor. Defaults are ON (not 0/disabled like min_frontage)
# since this IS the fix for the reported sliver defect: 0.3 island units
# (~7.5m at DEFAULT_METERS_PER_UNIT) sits comfortably below production's
# typical ~0.66-unit lot width (area-per-world ~274 m2 -> sqrt(274/25**2),
# docs/lots-wave-plan.md) while still catching genuine near-zero-width wedge
# slivers; 8.0 is 2x the soft aspect_clamp, catching only genuinely extreme
# (needle-thin) aspects while leaving moderately-elongated, already-score-
# penalized splits alone. Either can be set to 0 to disable that half of the
# floor.
DEFAULT_MIN_LOT_WIDTH: float = 0.3
DEFAULT_MAX_ASPECT_REJECT: float = 8.0

# Massing/typology defaults (docs/wave2-plan.md "The massing model (2a core)").
# All setback/height constants are METERS; MassingConfig / the footprint and
# height helpers convert to island units via meters_per_unit themselves.
# 150 m2/world -> ~72% detached / ~28% row on the production 698-district bake
# (median area-per-world ~274 m2): "suburb by default, densest areas -> row",
# tuned against the real distribution + in-viewer massing review (2026-07-06).
DEFAULT_A_DETACHED_M2: float = 150.0
DEFAULT_TOTAL_LANDMARK_BUDGET: int = 100
DEFAULT_STORY_HEIGHT_M: float = 3.1
DEFAULT_HEIGHT_JITTER_M: float = 0.4
DEFAULT_DETACHED_STORIES: tuple[int, int] = (1, 2)
DEFAULT_ROW_STORIES: tuple[int, int] = (2, 3)
DEFAULT_LANDMARK_STORIES: tuple[int, int] = (4, 6)
# S5 zone-graded massing (docs/macro-roads-nuclei-plan.md): a district's zone
# is "core"/"inner"/"fringe", major-nucleus districts only (see
# run_r1_hybrid.py's _zone_for_district) -- thresholded against
# mapgen.r1_macro.assign_nearest_nucleus's ``nucleus_dist`` (Euclidean
# distance from the district centroid to its nearest nucleus anchor,
# normalized by that nucleus's influence_radius, clamped to [0, 1]; 0 = at
# the anchor, 1 = at/beyond the influence radius). Chosen from the R-baseline
# bake's per-major nucleus_dist histogram (artifacts/r1/greybox/districts.
# geojson, all 756 districts, 2026-07-07): every one of the 6 majors has >= 8
# districts at/within 0.3 and >= 11 more within (0.3, 0.6] (nucleus 5, the
# smallest major, has only 15 districts total -- 3 core + 8 inner at these
# thresholds); beyond 0.6 the tail is dominated by districts that are merely
# NEAREST that nucleus rather than part of its downtown (e.g. nucleus 3/4
# have >60% of their mass beyond dist 0.9).
DEFAULT_CORE_ZONE_MAX_DIST: float = 0.3
DEFAULT_INNER_ZONE_MAX_DIST: float = 0.6
# Core/inner story bands (user taste call, docs/macro-roads-nuclei-plan.md
# "User taste calls (2026-07-07)"): core row 3-5, core landmark 8-12 --
# per-taste-call numbers, verbatim. Inner is an intermediate step between
# core and the suburb-wide fringe bands (DEFAULT_ROW_STORIES/
# DEFAULT_LANDMARK_STORIES) rather than a hard cliff at the core boundary.
# Detached keeps the suburban band in BOTH graded zones -- a dense downtown
# core rarely classifies "detached" at all (area-per-world there is small,
# see DEFAULT_A_DETACHED_M2), so grading it up would mostly be inert; kept as
# its own named fields (not hardcoded to DEFAULT_DETACHED_STORIES at the call
# site) so a future tune can move it without touching code.
DEFAULT_CORE_DETACHED_STORIES: tuple[int, int] = DEFAULT_DETACHED_STORIES
DEFAULT_CORE_ROW_STORIES: tuple[int, int] = (3, 5)
DEFAULT_CORE_LANDMARK_STORIES: tuple[int, int] = (8, 12)
DEFAULT_INNER_DETACHED_STORIES: tuple[int, int] = DEFAULT_DETACHED_STORIES
DEFAULT_INNER_ROW_STORIES: tuple[int, int] = (2, 4)
DEFAULT_INNER_LANDMARK_STORIES: tuple[int, int] = (6, 9)
# Representative front clearance from the frontage LOT LINE: Chen street
# half-width (0.25 island-unit "street" ribbon * DEFAULT_METERS_PER_UNIT 25 /
# 2 = 3.125m, see mapgen.r1_mesh.DEFAULT_STREET_WIDTHS/buffer_ribbon) +
# sidewalk (1.5m). TODO(wave2 2a slice 2): replace with a per-segment value
# once street width is computed once at G0 export (density x betweenness) --
# this flat constant cannot vary by street tier/segment the way the real
# road network does.
DEFAULT_ROAD_CLEAR_M: float = 4.6
DEFAULT_DETACHED_YARD_FRONT_M: float = 3.0
DEFAULT_DETACHED_SIDE_M: float = 2.0
DEFAULT_DETACHED_DEPTH_MAX_M: float = 12.0
DEFAULT_ROW_YARD_FRONT_M: float = 1.0
DEFAULT_ROW_SIDE_M: float = 0.0  # shared wall -- continuous terrace, v1
DEFAULT_ROW_DEPTH_MAX_M: float = 10.0
DEFAULT_LANDMARK_YARD_FRONT_M: float = 3.0
DEFAULT_LANDMARK_SIDE_M: float = 2.0
DEFAULT_LANDMARK_DEPTH_MAX_M: float = 20.0
# S5.1 footprint WIDTH cap (along-frontage extent, meters; 0.0 = uncapped).
# The depth-max above bounds how far a building runs back from the street, but
# NOT its width -- so a wide outskirts lot got a full-frontage slab that read
# as a warehouse. Capping detached width to ~farmhouse scale keeps large lots
# to a modest building centered with generous side yards (more inter-building
# space). Row stays uncapped (0.0): its full-width footprint IS the continuous
# terrace. Landmark stays uncapped: downtown landmarks should stay prominent.
DEFAULT_DETACHED_WIDTH_MAX_M: float = 11.0
DEFAULT_ROW_WIDTH_MAX_M: float = 0.0
DEFAULT_LANDMARK_WIDTH_MAX_M: float = 0.0
# Footprint ASPECT cap (long/short OBB ratio; 0.0 = uncapped). The width cap
# above bounds absolute footprint width, but a lot NARROWER than the cap (or one
# whose rear-guard forced a shallow slab) still yields a thin, needle-like
# building -- the "tiny thin building in a rectangular lot" defect (measured on
# the idx4 bake: 15% of detached footprints had OBB aspect > 3, tail to ~1575).
# Capping detached aspect biases those back to squarish by shrinking the LONGER
# footprint dimension (width symmetric; depth trimmed from the REAR so the front
# setback is preserved). Row stays uncapped (0.0): its elongation IS the
# continuous terrace, which the user wants. Landmark stays uncapped: downtown
# landmarks are intentionally large/prominent. 2.5 clears the genuinely-thin
# tail while leaving normal rectangular houses (<= 2.5:1) untouched.
DEFAULT_DETACHED_ASPECT_MAX: float = 2.5
DEFAULT_ROW_ASPECT_MAX: float = 0.0
DEFAULT_LANDMARK_ASPECT_MAX: float = 0.0
DEFAULT_REAR_MIN_M: float = 3.0
# S7b (docs/macro-roads-nuclei-plan.md): pedestrian/sidewalk margin (METERS)
# added to a fronting road's ribbon HALF-WIDTH to form that lot's front
# clearance -- see FrontingRoadIndex. Only consumed on the S7b fronting-index
# path; the flat pre-S7b road_clear_m default (DEFAULT_ROAD_CLEAR_M) is used
# whenever no index is threaded in.
DEFAULT_PEDESTRIAN_CLEARANCE_M: float = 2.0
# S7b: max distance (island units) from a lot's frontage edge to a tiered road
# for that road's tier to denominate the lot's front setback. A boundary
# district's frontage edge lies ON the macro-block boundary == the arterial
# centerline (distance ~0), while an INTERIOR district's nearest arterial is a
# whole Chen district (~0.66 island units) away -- 0.3 (~7.5 m) cleanly
# separates the two, so interior frontages resolve to the narrowest "street"
# tier.
DEFAULT_FRONTING_MATCH_THRESHOLD_UNITS: float = 0.3
# Matches mapgen.r1_mesh.DEFAULT_METERS_PER_UNIT -- duplicated here (rather
# than importing r1_mesh) so this pure module keeps its own zero-dependency
# default; run_r1_hybrid.py wires the SAME value through --meters-per-unit.
DEFAULT_METERS_PER_UNIT: float = 25.0

# Deterministic duplicate-coordinate jitter radius (island units), well below
# any realistic min_lot_area_frac floor so it never visibly perturbs a lot.
# Only used by the Voronoi fallback path (_build_lots_voronoi).
_JITTER_EPS: float = 1e-6

# Buffer tolerance (island units) used to test "lies on the district exterior
# ring" -- absorbs float noise from repeated split() calls, not a design knob.
_FRONTAGE_TOUCH_TOL: float = 1e-6

# Precision grid (island units) snapped onto every child polygon immediately
# after a shapely_split() call (docs/macro-roads-nuclei-plan.md's pre-existing
# `_assert_partition` blind spot, root-caused during the wave-2 "subdivision
# robustness" slice). On some acute/near-degenerate districts (confirmed on
# the raw triangle `Polygon([(0,0),(20,3),(20,-3)])` across several seeds),
# repeated shapely_split calls leave adjacent leaves' shared edges misaligned
# by a few ULPs -- geometrically meaningless, but enough that GEOS's cascaded
# `unary_union` (used by both `_assert_partition` and this constant's other
# call site) computes a WRONG total area, off by a large, non-precision-scale
# margin, even though every leaf is individually `is_valid` and NO pairwise
# `.intersection().area` is ever nonzero (verified directly -- this is a
# `unary_union` robustness defect, not a real overlap in the split output).
# Snapping every split's children to this grid (`shapely.set_precision`,
# `mode="valid_output"`) keeps every leaf on a single shared grid throughout
# the recursion, which eliminates the discrepancy (verified across seeds
# 0-100 on the fixture above) while being far too small (1e-9 island units ~=
# 2.5e-8 m at DEFAULT_METERS_PER_UNIT) to visibly perturb any real lot.
_SPLIT_PRECISION_GRID: float = 1e-9


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
    - ``min_lot_width``: HARD shape-floor (docs/macro-roads-nuclei-plan.md
      F1) -- a candidate split is REJECTED outright (not merely scored down)
      if either child's OBB short side falls below this (island units); ``<=
      0`` disables the width half of the floor. See :func:`_meets_shape_floor`.
    - ``max_aspect_reject``: the hard-reject counterpart to ``aspect_clamp``
      -- a candidate split is REJECTED outright if either child's OBB
      aspect ratio exceeds this (rather than merely scored down, as
      ``aspect_clamp`` does); ``<= 0`` disables the aspect half of the floor.
      Distinct from ``aspect_clamp`` and normally set higher than it, so
      moderately elongated splits are still allowed (just penalized) while
      only genuinely extreme (needle-thin) ones are refused entirely.
    - ``max_split_depth``: recursion guard (subdivision naturally halts via
      ``stop_area_factor`` long before this in any non-pathological case).
    - ``inset``: building footprint inward buffer -- same meaning as the
      module-level ``DEFAULT_INSET`` kwarg on :func:`build_lots`. Only used
      by :func:`_footprint`'s degenerate-collapse fallback and the Voronoi
      path now -- the primary :func:`_oriented_footprint` model sizes the
      footprint from real :class:`MassingConfig` setbacks instead (wave 2,
      ``docs/wave2-plan.md``; retired the old fill-ratio ``building_*_frac``
      sizing entirely).
    - ``voronoi_min_lot_area_frac``: sliver floor used ONLY by the Voronoi
      FALLBACK path (:func:`_build_lots_voronoi`), same meaning as the
      existing ``min_lot_area_frac`` kwarg.
    """

    stop_area_factor: float = DEFAULT_STOP_AREA_FACTOR
    split_jitter: float = DEFAULT_SPLIT_JITTER
    aspect_clamp: float = DEFAULT_ASPECT_CLAMP
    min_frontage: float = DEFAULT_MIN_FRONTAGE
    min_lot_width: float = DEFAULT_MIN_LOT_WIDTH
    max_aspect_reject: float = DEFAULT_MAX_ASPECT_REJECT
    max_split_depth: int = DEFAULT_MAX_SPLIT_DEPTH
    inset: float = DEFAULT_INSET
    voronoi_min_lot_area_frac: float = DEFAULT_MIN_LOT_AREA_FRAC


@dataclass(frozen=True)
class MassingConfig:
    """Suburb/row/landmark massing model tunables (``docs/wave2-plan.md``
    "The massing model (2a core)"). ALL setback/height/depth constants below
    are METERS -- :func:`_oriented_footprint`/:func:`_massing_height` convert
    to island units via a ``meters_per_unit`` argument threaded separately
    (this dataclass itself carries no unit-conversion state).

    - ``a_detached_m2`` / ``total_landmark_budget``: typology thresholds (see
      :func:`classify_typology`) -- area-per-world >= ``a_detached_m2`` is
      ``"detached"``, else ``"row"``; ``total_landmark_budget`` worlds total
      are ``"landmark"`` regardless of area, split into per-MAJOR-nucleus
      quotas proportional to nucleus mass rather than one island-wide top-N
      (S5, :func:`mapgen.r1_macro.NucleusSpec`; the quota math itself lives
      in ``scripts/run_r1_hybrid.py`` -- this field is just the total budget.
      Renamed from the pre-S5 ``landmark_count``, same default value).
    - ``story_height_m`` / ``height_jitter_m`` / ``*_stories``: height model
      (:func:`_massing_height`) -- ``stories`` (drawn from the typology's
      inclusive ``(lo, hi)`` band, deterministic per ``world_id``) times
      ``story_height_m``, plus jitter uniform in ``+-height_jitter_m``.
    - ``core_zone_max_dist`` / ``inner_zone_max_dist`` / ``core_*_stories`` /
      ``inner_*_stories``: S5 zone-graded story bands for MAJOR-nucleus
      districts (:func:`_stories_for_typology`'s ``zone`` argument) --
      ``detached``/``row``/``landmark`` each get their own ``core_*``/
      ``inner_*`` band, the plain (unprefixed) ``*_stories`` fields above
      stay the ``"fringe"`` (suburb-wide, pre-S5) band. See the module-level
      ``DEFAULT_CORE_ZONE_MAX_DIST`` comment for the threshold units and
      how the defaults were chosen.
    - ``road_clear_m``: representative front clearance from the FRONTAGE LOT
      LINE (not the lot's own OBB) -- see the module-level constant's TODO:
      slice 2 replaces this flat value with a per-segment one.
    - ``*_yard_front_m`` / ``*_side_m`` / ``*_depth_max_m``: per-typology
      footprint setbacks (:func:`_oriented_footprint`) -- ``front_setback =
      road_clear_m + yard_front_m``; ``side_m`` erodes the footprint on each
      along-frontage end (``0`` for ``"row"``: shared walls, continuous
      terrace); ``depth_max_m`` caps how far back the footprint runs from the
      frontage line.
    - ``rear_min_m``: guard so a shallow lot always keeps at least this much
      UNBUILT depth behind the footprint, capping ``depth_max_m`` down when
      the lot itself is too shallow to fit the full setback + depth_max.
    - ``pedestrian_clearance_m``: S7b (docs/macro-roads-nuclei-plan.md) --
      pedestrian/sidewalk margin added to a fronting road's ribbon HALF-WIDTH
      to form that lot's front clearance when a :class:`FrontingRoadIndex` is
      threaded into :func:`build_lots`. Inert on the flat pre-S7b path (no
      index): ``road_clear_m`` stays the flat clearance there.
    """

    a_detached_m2: float = DEFAULT_A_DETACHED_M2
    total_landmark_budget: int = DEFAULT_TOTAL_LANDMARK_BUDGET
    story_height_m: float = DEFAULT_STORY_HEIGHT_M
    height_jitter_m: float = DEFAULT_HEIGHT_JITTER_M
    detached_stories: tuple[int, int] = DEFAULT_DETACHED_STORIES
    row_stories: tuple[int, int] = DEFAULT_ROW_STORIES
    landmark_stories: tuple[int, int] = DEFAULT_LANDMARK_STORIES
    core_zone_max_dist: float = DEFAULT_CORE_ZONE_MAX_DIST
    inner_zone_max_dist: float = DEFAULT_INNER_ZONE_MAX_DIST
    core_detached_stories: tuple[int, int] = DEFAULT_CORE_DETACHED_STORIES
    core_row_stories: tuple[int, int] = DEFAULT_CORE_ROW_STORIES
    core_landmark_stories: tuple[int, int] = DEFAULT_CORE_LANDMARK_STORIES
    inner_detached_stories: tuple[int, int] = DEFAULT_INNER_DETACHED_STORIES
    inner_row_stories: tuple[int, int] = DEFAULT_INNER_ROW_STORIES
    inner_landmark_stories: tuple[int, int] = DEFAULT_INNER_LANDMARK_STORIES
    road_clear_m: float = DEFAULT_ROAD_CLEAR_M
    detached_yard_front_m: float = DEFAULT_DETACHED_YARD_FRONT_M
    detached_side_m: float = DEFAULT_DETACHED_SIDE_M
    detached_depth_max_m: float = DEFAULT_DETACHED_DEPTH_MAX_M
    row_yard_front_m: float = DEFAULT_ROW_YARD_FRONT_M
    row_side_m: float = DEFAULT_ROW_SIDE_M
    row_depth_max_m: float = DEFAULT_ROW_DEPTH_MAX_M
    landmark_yard_front_m: float = DEFAULT_LANDMARK_YARD_FRONT_M
    landmark_side_m: float = DEFAULT_LANDMARK_SIDE_M
    landmark_depth_max_m: float = DEFAULT_LANDMARK_DEPTH_MAX_M
    detached_width_max_m: float = DEFAULT_DETACHED_WIDTH_MAX_M
    row_width_max_m: float = DEFAULT_ROW_WIDTH_MAX_M
    landmark_width_max_m: float = DEFAULT_LANDMARK_WIDTH_MAX_M
    detached_aspect_max: float = DEFAULT_DETACHED_ASPECT_MAX
    row_aspect_max: float = DEFAULT_ROW_ASPECT_MAX
    landmark_aspect_max: float = DEFAULT_LANDMARK_ASPECT_MAX
    rear_min_m: float = DEFAULT_REAR_MIN_M
    pedestrian_clearance_m: float = DEFAULT_PEDESTRIAN_CLEARANCE_M


@dataclass(frozen=True, eq=False)
class FrontingRoadIndex:
    """Spatial index of tiered arterial/ring centerlines that denominates a
    lot's FRONT setback from the WIDTH of the road its frontage actually fronts
    (S7b, docs/macro-roads-nuclei-plan.md).

    Road ribbon widths in the mesh are tier-differentiated (a highway ribbon
    spans a full island unit -> +-12.5 m from its centerline at
    ``meters_per_unit=25``), but the arterial CENTERLINE runs along the
    macro-block boundary a boundary district's frontage edge sits on. A flat
    ``MassingConfig.road_clear_m`` therefore set a highway-fronting building
    only ~4.6 m back from a centerline whose ribbon reached 12.5 m -- the
    building landed INSIDE the ribbon. This index maps each lot's frontage edge
    to the tier of the nearest tiered road (within ``match_threshold`` island
    units) so :func:`build_lots` can set ``road_clear_m`` from that tier's
    half-width instead.

    Built by :func:`build_fronting_road_index`, which owns the STRtree
    construction and the per-tier ``road_clear_m`` precompute. ``street_widths``
    (``mapgen.r1_mesh.DEFAULT_STREET_WIDTHS`` -- the SINGLE source of truth for
    ribbon widths) is threaded IN by the caller rather than imported here,
    keeping this module free of an r1_mesh dependency (same discipline as
    ``DEFAULT_METERS_PER_UNIT``).

    - ``road_clear_by_tier``: tier name -> front clearance (METERS) =
      ``street_widths[tier] * meters_per_unit / 2 + pedestrian_clearance_m``
      (ribbon half-width + pedestrian margin). Covers EVERY tier in
      ``street_widths`` (incl. the interior ``"street"`` tier) so a lookup
      resolves even for a tier not present among ``segments``.
    - ``half_width_by_tier``: tier name -> ribbon half-width (METERS), used
      only to break an exact nearest-distance TIE toward the WIDEST road
      (deterministic + conservative -- keeps buildings off the widest nearby
      ribbon, and lets a promoted highway ring arc win over the whole "ring"
      it overlaps).

    ``eq=False`` because the stored :class:`~shapely.strtree.STRtree` is not
    meaningfully comparable/hashable and the index is only ever passed by
    reference, never compared.
    """

    lines: tuple[sg.LineString, ...]
    tiers: tuple[str, ...]
    tree: STRtree
    match_threshold: float
    road_clear_by_tier: dict[str, float]
    half_width_by_tier: dict[str, float]

    def fronting_tier(self, edge: sg.LineString) -> str | None:
        """Tier name of the nearest tiered road within ``match_threshold`` of
        ``edge`` (an exact-distance tie broken toward the WIDEST tier, then by
        tier name for full determinism), or ``None`` when no tiered road is
        that close -- an interior frontage on a Chen local street, which
        :func:`build_lots` maps to the narrowest ``"street"`` tier."""
        if not self.lines:
            return None
        idx = self.tree.query_nearest(
            edge, max_distance=self.match_threshold, all_matches=True
        )
        cands = [int(i) for i in idx]
        if not cands:
            return None
        best_i = min(
            cands,
            key=lambda i: (
                round(float(edge.distance(self.lines[i])), 9),
                -self.half_width_by_tier.get(self.tiers[i], 0.0),
                self.tiers[i],
            ),
        )
        return self.tiers[best_i]

    def road_clear_m(self, tier: str) -> float:
        """Front clearance (METERS) for ``tier`` (ribbon half-width +
        pedestrian margin); an unknown tier falls back to the narrowest
        (``"street"``) clearance, else 0.0."""
        if tier in self.road_clear_by_tier:
            return self.road_clear_by_tier[tier]
        return self.road_clear_by_tier.get("street", 0.0)


def build_fronting_road_index(
    segments: list[tuple[sg.LineString, str]],
    *,
    street_widths: dict[str, float],
    meters_per_unit: float,
    pedestrian_clearance_m: float = DEFAULT_PEDESTRIAN_CLEARANCE_M,
    match_threshold: float = DEFAULT_FRONTING_MATCH_THRESHOLD_UNITS,
) -> FrontingRoadIndex:
    """Assemble a :class:`FrontingRoadIndex` from ``(line, tier_name)`` pairs
    (empty/non-``LineString`` geometries skipped).

    ``street_widths`` is ``mapgen.r1_mesh.DEFAULT_STREET_WIDTHS`` (ribbon width
    in island units, per tier name) -- the SAME table drives both the mesh
    ribbons and these setbacks, so there is a single source of truth for the
    numbers. Every tier in ``street_widths`` gets a precomputed
    ``road_clear_m`` and half-width, so a lookup for a tier not present among
    ``segments`` (notably the interior ``"street"`` tier) still resolves.
    """
    lines: list[sg.LineString] = []
    tiers: list[str] = []
    for line, tier in segments:
        if line is None or line.is_empty or line.geom_type != "LineString":
            continue
        lines.append(line)
        tiers.append(tier)
    road_clear_by_tier = {
        tier: width * meters_per_unit / 2.0 + pedestrian_clearance_m
        for tier, width in street_widths.items()
    }
    half_width_by_tier = {
        tier: width * meters_per_unit / 2.0 for tier, width in street_widths.items()
    }
    return FrontingRoadIndex(
        lines=tuple(lines),
        tiers=tuple(tiers),
        tree=STRtree(lines),
        match_threshold=match_threshold,
        road_clear_by_tier=road_clear_by_tier,
        half_width_by_tier=half_width_by_tier,
    )


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
    distance between them. For ``assigned="direct"`` worlds this IS bounded by
    the district's own OBB diagonal (see the module docstring's bounded-
    displacement design decision); for ``assigned="snapped"`` worlds it is
    NOT -- it additionally carries the snap distance from the world's true
    coordinate to the district it got snapped into, which can be arbitrarily
    large.

    ``kind`` is ``"lot"`` (occupied: a world assigned to it) or
    ``"greenspace"`` (surplus subdivision piece with no world -- ``world_id``
    is ``""``, ``footprint`` is an empty ``Polygon`` (no building), ``height``
    is ``0.0``, ``name`` is ``None``, ``visits`` is ``0``, and ``x``/``y``/
    ``lot_x``/``lot_y`` all equal the piece's own anchor point since there is
    no original world coordinate to preserve).

    ``typology`` is ``"detached"``/``"row"``/``"landmark"`` (see
    :func:`classify_typology`) for an occupied lot, or ``""`` for
    ``kind="greenspace"`` (no world, no massing decision to make). The
    Voronoi fallback path (:func:`_build_lots_voronoi`) always classifies
    ``"detached"`` -- see that function's docstring.
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
    typology: str  # "detached" | "row" | "landmark" | "" (greenspace)


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


def top_landmark_ids(points: pl.DataFrame, landmark_count: int) -> frozenset[str]:
    """The ``landmark_count`` ``world_id``s with the highest ``visits`` in
    ``points`` -- ties broken by ascending ``world_id`` for determinism
    regardless of ``points``' row order. ``landmark_count <= 0`` or an empty
    ``points`` returns an empty set. Feeds :func:`classify_typology` via
    :func:`build_lots`'s ``landmark_ids`` kwarg (docs/wave2-plan.md "The
    massing model"). A generic top-N-by-visits helper: pre-S5 callers pass
    the WHOLE island's ``points`` for one island-wide top ``landmark_count``;
    S5's per-nucleus quota (``scripts/run_r1_hybrid.py``'s
    ``landmark_ids_by_nucleus``) reuses it UNCHANGED, calling it once per
    MAJOR nucleus with that nucleus's member-world subset and its own quota."""
    if landmark_count <= 0 or points.height == 0:
        return frozenset()
    ordered = points.select(["world_id", "visits"]).sort(
        ["visits", "world_id"], descending=[True, False]
    )
    top = ordered.head(landmark_count)
    return frozenset(str(v) for v in top["world_id"].to_list())


def classify_typology(
    district_area_units: float,
    n_worlds: int,
    world_id: str,
    landmark_ids: frozenset[str],
    cfg: MassingConfig,
    meters_per_unit: float,
) -> str:
    """One of ``"landmark"``/``"detached"``/``"row"`` (docs/wave2-plan.md
    "The massing model"): ``"landmark"`` if ``world_id in landmark_ids``
    (independent of area -- popularity always wins); else area-per-world
    ``district_area_units * meters_per_unit**2 / n_worlds`` thresholded
    against ``cfg.a_detached_m2`` (``>=`` is ``"detached"``, else ``"row"``).

    Pure function of its inputs (no RNG, no iteration-order dependence) --
    the SAME ``(district_area_units, n_worlds, world_id, landmark_ids)``
    always yields the SAME typology. ``n_worlds <= 0`` is a degenerate guard
    (:func:`build_lots` never calls this with zero worlds -- see its own
    0-point early return) classifying as ``"detached"`` rather than dividing
    by zero.
    """
    if world_id in landmark_ids:
        return "landmark"
    if n_worlds <= 0:
        return "detached"
    a = district_area_units * meters_per_unit**2 / float(n_worlds)
    return "detached" if a >= cfg.a_detached_m2 else "row"


def _setbacks_for_typology(
    typology: str, cfg: MassingConfig
) -> tuple[float, float, float]:
    """``(yard_front_m, side_m, depth_max_m)`` for ``typology`` -- unknown
    typologies fall back to the detached clearances (same convention as
    :func:`_stories_for_typology`)."""
    if typology == "row":
        return cfg.row_yard_front_m, cfg.row_side_m, cfg.row_depth_max_m
    if typology == "landmark":
        return cfg.landmark_yard_front_m, cfg.landmark_side_m, cfg.landmark_depth_max_m
    return cfg.detached_yard_front_m, cfg.detached_side_m, cfg.detached_depth_max_m


def _width_max_for_typology(typology: str, cfg: MassingConfig) -> float:
    """Max along-frontage footprint width in METERS for ``typology`` (S5.1);
    ``0.0`` means uncapped. Unknown typologies use the detached cap (same
    convention as :func:`_setbacks_for_typology`)."""
    if typology == "row":
        return cfg.row_width_max_m
    if typology == "landmark":
        return cfg.landmark_width_max_m
    return cfg.detached_width_max_m


def _aspect_max_for_typology(typology: str, cfg: MassingConfig) -> float:
    """Max footprint OBB aspect (long/short) for ``typology``; ``0.0`` means
    uncapped. Unknown typologies use the detached cap (same convention as
    :func:`_width_max_for_typology`)."""
    if typology == "row":
        return cfg.row_aspect_max
    if typology == "landmark":
        return cfg.landmark_aspect_max
    return cfg.detached_aspect_max


def _stories_for_typology(
    typology: str, cfg: MassingConfig, *, zone: str = "fringe"
) -> tuple[int, int]:
    """Inclusive ``(lo, hi)`` story band for ``(typology, zone)`` (S5,
    docs/macro-roads-nuclei-plan.md).

    ``zone`` grades the band for MAJOR-nucleus districts: ``"core"``/
    ``"inner"`` pick ``cfg``'s ``core_*``/``inner_*`` fields; any other value
    -- including the default ``"fringe"`` -- reproduces the pre-S5
    suburb-wide band (``cfg.row_stories``/``cfg.landmark_stories``/
    ``cfg.detached_stories``) byte-identically, so every pre-S5 caller that
    doesn't pass ``zone`` is unaffected. Unknown typologies (should not
    occur -- :func:`classify_typology` only ever returns the three known
    values) fall back to the resolved zone's detached band.
    """
    if zone == "core":
        if typology == "row":
            return cfg.core_row_stories
        if typology == "landmark":
            return cfg.core_landmark_stories
        return cfg.core_detached_stories
    if zone == "inner":
        if typology == "row":
            return cfg.inner_row_stories
        if typology == "landmark":
            return cfg.inner_landmark_stories
        return cfg.inner_detached_stories
    if typology == "row":
        return cfg.row_stories
    if typology == "landmark":
        return cfg.landmark_stories
    return cfg.detached_stories


def _massing_height(
    world_id: str, typology: str, cfg: MassingConfig, *, zone: str = "fringe"
) -> float:
    """``stories * cfg.story_height_m + jitter`` (meters). ``stories`` is
    picked within the ``(typology, zone)`` inclusive ``(lo, hi)`` band (S5,
    :func:`_stories_for_typology`; ``zone`` defaults to ``"fringe"``, the
    pre-S5 suburb-wide band) and ``jitter`` is drawn uniformly from
    ``[-height_jitter_m, height_jitter_m]`` -- BOTH keyed off a deterministic
    hash of ``world_id`` (``hashlib.sha256``, stable across processes/
    ``PYTHONHASHSEED`` -- unlike the builtin ``hash()``, which is salted
    per-process by default) rather than any RNG draw or dict/set iteration
    order, per the module's determinism contract (same discipline as
    :func:`_jitter_duplicates`). Two independent hashes (distinct salt
    strings) so story selection and jitter don't correlate. Per-lot variation
    within a row run is intentionally KEPT (real terraces vary story-to-
    story) -- only the visits-driven height formula is retired.
    """
    lo, hi = _stories_for_typology(typology, cfg, zone=zone)
    n_options = max(hi - lo + 1, 1)
    story_digest = hashlib.sha256(f"massing_stories:{world_id}".encode()).digest()
    stories = lo + (int.from_bytes(story_digest[:8], "big") % n_options)
    jitter_digest = hashlib.sha256(f"massing_jitter:{world_id}".encode()).digest()
    unit = (int.from_bytes(jitter_digest[:8], "big") % 1_000_003) / 1_000_003.0
    jitter = (unit * 2.0 - 1.0) * cfg.height_jitter_m
    return stories * cfg.story_height_m + jitter


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


def _meets_shape_floor(poly: sg.Polygon, cfg: LotConfig) -> bool:
    """Hard shape-floor test (docs/macro-roads-nuclei-plan.md F1): ``False``
    if ``poly``'s OBB short side falls below ``cfg.min_lot_width`` or its OBB
    aspect ratio exceeds ``cfg.max_aspect_reject`` (each half independently
    disabled by setting the corresponding ``cfg`` field ``<= 0``) -- or if
    ``poly`` itself is empty/zero-area. Used by :func:`_best_split` to REJECT
    a candidate split outright (rather than merely score it down, as
    ``cfg.aspect_clamp`` does) and by :func:`_merge_slivers` to find leaves
    that need absorbing into a neighbor."""
    if poly.is_empty or poly.area <= 1e-12:
        return False
    if cfg.min_lot_width > 0.0:
        _center, _axis_long, _axis_short, _long_len, short_len = _obb_axes(poly)
        if short_len < cfg.min_lot_width:
            return False
    return not (
        cfg.max_aspect_reject > 0.0 and _lot_aspect_ratio(poly) > cfg.max_aspect_reject
    )


def _frontage_length(
    piece: sg.Polygon,
    ext_ring: sg.LinearRing,
    ext_buffer: sg.base.BaseGeometry | None = None,
) -> float:
    """Total boundary length of ``piece`` lying on ``ext_ring`` (small buffer tol).

    ``ext_buffer``, if given, must be ``ext_ring.buffer(_FRONTAGE_TOUCH_TOL)``
    precomputed by the caller -- :func:`subdivide_district`/:func:`build_lots`
    thread it through so that buffer (which doesn't depend on ``piece``) is
    computed ONCE per call rather than once per query across an entire
    recursion. Recomputed on the fly when omitted, so direct test callers
    keep working unmodified."""
    if piece.is_empty:
        return 0.0
    buffered = (
        ext_ring.buffer(_FRONTAGE_TOUCH_TOL) if ext_buffer is None else ext_buffer
    )
    shared = piece.boundary.intersection(buffered)
    return float(shared.length) if hasattr(shared, "length") else 0.0


def _touches_ext_ring(
    piece: sg.Polygon,
    ext_ring: sg.LinearRing,
    min_frontage: float,
    ext_buffer: sg.base.BaseGeometry | None = None,
) -> bool:
    return _frontage_length(piece, ext_ring, ext_buffer) > max(min_frontage, 1e-9)


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
    ext_buffer: sg.base.BaseGeometry | None = None,
) -> list[sg.Polygon] | None:
    """Try the candidate cut lines from :func:`_split_candidates`, score valid
    binary splits (area balance + child rectangularity/aspect), and return the
    best pair -- preferring candidates where BOTH children still touch the
    district exterior ring. ``None`` if no candidate produces a clean 2-piece
    split (the caller treats ``poly`` as an (oversized or unsplittable) leaf)
    -- this INCLUDES the case where every candidate's children would violate
    ``cfg``'s hard shape-floor (:func:`_meets_shape_floor`): such candidates
    are rejected outright, not merely scored down, so a piece that cannot be
    cleanly split without producing a sub-floor child simply stays whole
    (docs/macro-roads-nuclei-plan.md F1) rather than emitting a sliver.

    Every candidate's two raw ``shapely_split`` parts are snapped to
    ``_SPLIT_PRECISION_GRID`` before any further check (docs/
    macro-roads-nuclei-plan.md's pre-existing ``_assert_partition`` blind
    spot -- see that constant's docstring) so leaves stay on a shared
    precision grid throughout the whole recursion, not just at the final
    :func:`_assert_partition` check.

    ``ext_buffer``, if given, is ``ext_ring``'s precomputed frontage-touch
    buffer (see :func:`_frontage_length`) -- threaded through by
    :func:`subdivide_district`/:func:`_fill_deficit` so it's computed once per
    top-level call rather than once per candidate split."""
    if ext_buffer is None:
        ext_buffer = ext_ring.buffer(_FRONTAGE_TOUCH_TOL)
    lines = _split_candidates(poly, cfg, rng)
    min_area = 1e-9 * max(poly.area, 1e-12)
    scored: list[tuple[bool, float, int, list[sg.Polygon]]] = []
    for order_idx, line in enumerate(lines):
        try:
            result = shapely_split(poly, line)
        except Exception:
            continue
        raw_parts = [
            g for g in result.geoms if g.geom_type == "Polygon" and not g.is_empty
        ]
        if len(raw_parts) != 2:
            continue
        parts = [set_precision(p, _SPLIT_PRECISION_GRID) for p in raw_parts]
        if any(p.is_empty or p.geom_type != "Polygon" for p in parts):
            continue
        if parts[0].area <= min_area or parts[1].area <= min_area:
            continue
        if any(not _meets_shape_floor(p, cfg) for p in parts):
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
            _touches_ext_ring(p, ext_ring, cfg.min_frontage, ext_buffer) for p in parts
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
    ext_buffer: sg.base.BaseGeometry | None = None,
) -> list[sg.Polygon] | None:
    """Re-split the largest still-splittable leaf until ``len(leaves) >=
    n_needed``. Returns ``None`` (subdivision failure) if every remaining
    leaf refuses to split further while a deficit remains -- this always
    terminates: each iteration either grows the leaf count or permanently
    moves one leaf to ``unsplittable``, so the loop cannot spin forever.

    ``ext_buffer`` is threaded through to :func:`_best_split` (see
    :func:`_frontage_length`)."""
    if ext_buffer is None:
        ext_buffer = ext_ring.buffer(_FRONTAGE_TOUCH_TOL)
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
        children = _best_split(target, ext_ring, cfg, rng, ext_buffer)
        if children is None:
            unsplittable.append(target)
            continue
        splittable.extend(children)
    return splittable + unsplittable


def _merge_slivers(leaves: list[sg.Polygon], cfg: LotConfig) -> list[sg.Polygon]:
    """Absorb any leaf failing :func:`_meets_shape_floor` into whichever OTHER
    leaf it shares the LONGEST boundary with, rather than emitting it as its
    own lot (docs/macro-roads-nuclei-plan.md F1).

    :func:`_best_split`'s per-candidate reject keeps every leaf it PRODUCES
    on-floor, but a leaf that was never split at all can still start out
    below the floor -- e.g. an already-thin wedge district piece that hits
    ``cfg.stop_area_factor`` (or has every further split rejected) before it
    ever reaches an acceptable shape. This is the fallback for that
    unavoidable case.

    Each merge replaces the two pieces with their union (``unary_union``,
    reduced to its largest polygonal part -- two partition-adjacent pieces
    share only a zero-area boundary, so the union is a single clean
    polygon), which preserves total coverage EXACTLY: no area is gained,
    lost, or re-clipped, only two adjacent pieces of the same partition
    become one. Repeats (bounded to ``len(leaves)`` passes -- each merge
    strictly reduces the leaf count by one, so this always terminates)
    since resolving one sliver can reveal the merged piece -- or another
    leaf -- as still/newly below floor. A sliver with no boundary-sharing
    neighbor (can only happen once ``len(current) <= 1``) is left alone --
    there is nothing to merge it into.

    Note this can drop the leaf count below the caller's ``n_lots`` target;
    :func:`subdivide_district` re-runs :func:`_fill_deficit` afterward to
    compensate if so.
    """
    current = list(leaves)
    for _ in range(len(leaves)):
        if len(current) <= 1:
            break
        sliver_idx = next(
            (i for i, p in enumerate(current) if not _meets_shape_floor(p, cfg)),
            None,
        )
        if sliver_idx is None:
            break
        sliver = current[sliver_idx]
        best_j: int | None = None
        best_len = 0.0
        for j, other in enumerate(current):
            if j == sliver_idx:
                continue
            shared = sliver.boundary.intersection(other.boundary)
            length = float(shared.length) if hasattr(shared, "length") else 0.0
            if length > best_len:
                best_len = length
                best_j = j
        if best_j is None:
            # No adjacent neighbor at all (shouldn't happen for a proper
            # partition with > 1 piece) -- leave it rather than looping.
            break
        merged = _largest_polygon_part(unary_union([sliver, current[best_j]]))
        current = [
            p for k, p in enumerate(current) if k not in (sliver_idx, best_j)
        ] + [merged]
    return current


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

    Every split :func:`_best_split` performs REJECTS (not merely scores down)
    a candidate whose child would violate ``cfg``'s hard shape-floor
    (``min_lot_width``/``max_aspect_reject``, see :func:`_meets_shape_floor`)
    -- a piece with no floor-respecting split just stays whole. That alone
    cannot rule out an "unavoidable" sub-floor leaf that was never split at
    all (e.g. an already-thin wedge district piece), so a final
    :func:`_merge_slivers` pass absorbs any leaf still below the floor into
    its most-shared-boundary neighbor; if that drops the leaf count back
    below ``n_lots``, :func:`_fill_deficit` runs once more (splitting
    elsewhere to compensate, itself still floor-respecting) followed by one
    more merge pass -- raising :class:`_SubdivisionFailure` if that still
    cannot reach ``n_lots``.

    Deterministic: identical ``(district, n_lots, seed, cfg)`` always yields
    identical output (same WKBs, same order) -- the RNG is only ever advanced
    in one fixed traversal order (children always processed in
    ``_poly_sort_key`` order), never influenced by dict/set iteration.
    """
    if cfg is None:
        cfg = LotConfig()
    if n_lots <= 1:
        return [district]
    if district.is_empty or district.area <= 0.0:
        # M >= N is a hard contract once n_lots > 1: a degenerate district
        # (empty/zero-area geometry, e.g. a collapsed/duplicate-vertex
        # polygon slipping through upstream) can never honor it -- raise so
        # build_lots falls back to _build_lots_voronoi instead of silently
        # returning a single lot for a district that needs >= 2.
        raise _SubdivisionFailure(
            f"district is empty or has zero area but n_lots={n_lots} > 1"
        )

    ext_ring = district.exterior
    # Computed ONCE per subdivide_district call and threaded through every
    # frontage query below (_best_split -> _touches_ext_ring -> _frontage_length)
    # instead of re-buffering ext_ring on every single candidate-split check
    # (docs/lots-wave-plan.md perf fix) -- the buffer doesn't depend on the
    # piece being tested, only on the district's own exterior ring.
    ext_buffer = ext_ring.buffer(_FRONTAGE_TOUCH_TOL)
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
        children = _best_split(poly, ext_ring, cfg, rng, ext_buffer)
        if children is None:
            leaves.append(poly)
            return
        for child in children:
            _recurse(child, depth + 1)

    _recurse(district, 0)

    if len(leaves) < n_lots:
        filled = _fill_deficit(leaves, n_lots, ext_ring, cfg, rng, ext_buffer)
        if filled is None:
            raise _SubdivisionFailure(
                f"could not reach {n_lots} lots from district (got {len(leaves)})"
            )
        leaves = filled

    leaves = _merge_slivers(leaves, cfg)
    if len(leaves) < n_lots:
        # A merge can drop the count below n_lots -- re-split elsewhere
        # (still floor-respecting) to compensate, then merge once more in
        # case that re-split's own leftovers exposed a new sub-floor leaf.
        filled = _fill_deficit(leaves, n_lots, ext_ring, cfg, rng, ext_buffer)
        if filled is None:
            raise _SubdivisionFailure(
                f"could not reach {n_lots} lots after sliver-merge (got {len(leaves)})"
            )
        leaves = _merge_slivers(filled, cfg)
        if len(leaves) < n_lots:
            raise _SubdivisionFailure(
                f"sliver-merge dropped below {n_lots} lots and could not "
                f"recover (got {len(leaves)})"
            )

    leaves.sort(key=_poly_sort_key)
    return leaves


def _assert_partition(
    district: sg.Polygon, lot_polys: list[sg.Polygon], rel_tol: float = 1e-6
) -> None:
    """Defensive runtime check of the partition invariant :func:`subdivide_district`
    documents as "achieved naturally" -- guards against an unexpected
    topology edge case slipping through, in which case :func:`build_lots`
    falls back to Voronoi rather than exporting bad geometry.

    Both the ``unary_union``-area check AND the pairwise-STRtree check below
    run against ``lot_polys`` snapped to ``_SPLIT_PRECISION_GRID`` first --
    without that snap, GEOS's cascaded ``unary_union`` can compute a WRONG
    (short) area for a set of otherwise-valid, genuinely non-overlapping
    polygons whose adjacent edges are misaligned by only a few ULPs (see
    ``_SPLIT_PRECISION_GRID``'s docstring), which would make this the
    unreliable check :func:`subdivide_district`'s callers already suspected
    it to be (docs/macro-roads-nuclei-plan.md's pre-existing blind spot).
    :func:`_best_split` already snaps every split's children as they're
    produced, so this is normally a no-op for subdivision-path input; it
    still matters for any ``lot_polys`` that didn't come through that path
    (e.g. a hand-constructed test fixture)."""
    if not lot_polys:
        raise _SubdivisionFailure("empty subdivision result")
    total = max(district.area, 1e-12)
    snapped = [set_precision(p, _SPLIT_PRECISION_GRID) for p in lot_polys]
    union = unary_union(snapped)
    if abs(union.area - district.area) > rel_tol * total:
        raise _SubdivisionFailure("subdivision union area does not match district area")
    tree = STRtree(snapped)
    for i, poly in enumerate(snapped):
        for j in sorted(int(k) for k in tree.query(poly)):
            if j <= i:
                continue
            if poly.intersection(snapped[j]).area > rel_tol * total:
                raise _SubdivisionFailure("subdivision lots overlap")


def _frontage_edge(
    lot_poly: sg.Polygon,
    ext_ring: sg.LinearRing,
    min_frontage: float,
    ext_buffer: sg.base.BaseGeometry | None = None,
) -> tuple[np.ndarray, np.ndarray] | None:
    """``(anchor_point, unit_direction)`` of the LONGEST boundary edge of
    ``lot_poly`` that lies on ``ext_ring`` (the district's street frontage),
    or ``None`` if no edge qualifies (``>= min_frontage`` long and fully
    covered by a small buffer around ``ext_ring``) -- the caller falls back
    to the lot's own OBB axes in that case. ``anchor_point`` is one endpoint
    of that edge, used by :func:`_oriented_footprint` as the origin of its
    along/depth coordinate frame (depth 0 sits ON the frontage line).
    ``ext_buffer``, if given, is ``ext_ring``'s precomputed frontage-touch
    buffer (see :func:`_frontage_length`)."""
    buffered = (
        ext_ring.buffer(_FRONTAGE_TOUCH_TOL) if ext_buffer is None else ext_buffer
    )
    coords = list(lot_poly.exterior.coords)
    best_len = max(min_frontage, 1e-9)
    best: tuple[np.ndarray, np.ndarray] | None = None
    for i in range(len(coords) - 1):
        p0, p1 = coords[i], coords[i + 1]
        seg = sg.LineString([p0, p1])
        length = seg.length
        if length <= best_len:
            continue
        if buffered.covers(seg):
            best_len = length
            d = np.array([p1[0] - p0[0], p1[1] - p0[1]])
            direction = d / max(float(np.linalg.norm(d)), 1e-12)
            best = (np.array([p0[0], p0[1]]), direction)
    return best


def _frontage_direction(
    lot_poly: sg.Polygon,
    ext_ring: sg.LinearRing,
    min_frontage: float,
    ext_buffer: sg.base.BaseGeometry | None = None,
) -> np.ndarray | None:
    """Unit direction of the LONGEST boundary edge of ``lot_poly`` that lies
    on ``ext_ring`` (the district's street frontage), or ``None`` if no edge
    qualifies (``>= min_frontage`` long and fully covered by a small buffer
    around ``ext_ring``) -- the caller falls back to the lot's own OBB long
    axis in that case. ``ext_buffer``, if given, is ``ext_ring``'s precomputed
    frontage-touch buffer (see :func:`_frontage_length`). Thin wrapper around
    :func:`_frontage_edge` (kept as its own function: callers that only need
    the direction, e.g. this module's tests, don't need the anchor point)."""
    edge = _frontage_edge(lot_poly, ext_ring, min_frontage, ext_buffer)
    return None if edge is None else edge[1]


def _longest_frontage_segment(
    lot_poly: sg.Polygon,
    ext_ring: sg.LinearRing,
    min_frontage: float,
    ext_buffer: sg.base.BaseGeometry | None = None,
) -> sg.LineString | None:
    """The LONGEST boundary edge of ``lot_poly`` lying on ``ext_ring`` (the
    district's street frontage), returned as a ``LineString`` -- or ``None``
    when no edge qualifies.

    Mirrors :func:`_frontage_edge`'s edge SELECTION exactly (same
    longest-covered-segment rule / tie behavior) but yields the full segment
    geometry (both endpoints) that :func:`FrontingRoadIndex.fronting_tier`
    needs for its nearest-road lookup (S7b), where :func:`_frontage_edge`
    yields only the anchor + direction its footprint frame needs. Kept as its
    own helper rather than widening :func:`_frontage_edge`'s return so that
    function -- and its byte-identical regression tests -- stays untouched."""
    buffered = (
        ext_ring.buffer(_FRONTAGE_TOUCH_TOL) if ext_buffer is None else ext_buffer
    )
    coords = list(lot_poly.exterior.coords)
    best_len = max(min_frontage, 1e-9)
    best: sg.LineString | None = None
    for i in range(len(coords) - 1):
        p0, p1 = coords[i], coords[i + 1]
        seg = sg.LineString([p0, p1])
        length = seg.length
        if length <= best_len:
            continue
        if buffered.covers(seg):
            best_len = length
            best = seg
    return best


def _frontage_depth_units(
    lot_poly: sg.Polygon, anchor: np.ndarray, along: np.ndarray
) -> float:
    """Perpendicular depth (island units) of ``lot_poly`` from its frontage
    edge INTO the lot -- the max projection of the lot's vertices onto the
    inward perpendicular of the ``(anchor, along)`` frontage frame. Mirrors
    the ``total_depth`` :func:`_oriented_footprint` computes internally, used
    by :func:`build_lots` to decide (S7b near-highway displacement) whether a
    lot is too SHALLOW to fit any building behind a wide arterial setback."""
    perp_candidate = np.array([-along[1], along[0]])
    centroid = lot_poly.centroid
    rel_c = np.array([centroid.x, centroid.y]) - anchor
    perp = perp_candidate if float(rel_c @ perp_candidate) >= 0.0 else -perp_candidate
    coords = np.asarray(lot_poly.exterior.coords[:-1], dtype=np.float64)
    rel = coords - anchor
    return float((rel @ perp).max())


def _strip_polygon(
    anchor: np.ndarray,
    along: np.ndarray,
    perp: np.ndarray,
    s_lo: float,
    s_hi: float,
    d_lo: float,
    d_hi: float,
) -> sg.Polygon:
    """Rectangle in the ``(along, perp)`` frame anchored at ``anchor``: spans
    ``s in [s_lo, s_hi]`` along ``along`` and ``d in [d_lo, d_hi]`` along
    ``perp``. :func:`_oriented_footprint` carves the setback slab and its
    side erosion out of a lot polygon via plain ``Polygon.intersection``
    against rectangles built by this helper."""
    corners = [
        anchor + s * along + d * perp
        for s, d in ((s_lo, d_lo), (s_hi, d_lo), (s_hi, d_hi), (s_lo, d_hi))
    ]
    return sg.Polygon([tuple(c) for c in corners])


def _footprint_clamped(lot_poly: sg.Polygon, inset: float) -> sg.Polygon:
    """:func:`_footprint`'s fallback, additionally clamped to stay INSIDE
    ``lot_poly``.

    :func:`_footprint`'s own degenerate case -- a ``representative_point()``
    circle of radius ``inset`` when the lot is too thin for the inward
    buffer to survive -- is deliberately UNCLAMPED there (see that function's
    own tests: a lot smaller than the circle gets the full circle, not a
    sliver) since ``_footprint``'s only contract is "every world gets SOME
    footprint." :func:`_oriented_footprint` needs a STRICTER guarantee
    (footprint subset of lot, this module's own invariant tests) for every
    lot the massing model produces, so its fallback calls go through this
    wrapper instead: intersect the circle with ``lot_poly`` (never empty --
    ``representative_point()`` sits inside ``lot_poly`` by contract, so the
    circle always overlaps it at least at that point) and fall back to the
    unclamped circle only for a genuinely empty/degenerate ``lot_poly``
    (``_footprint`` itself already special-cases that)."""
    fp = _footprint(lot_poly, inset)
    if lot_poly.is_empty or lot_poly.area <= 0.0:
        return fp
    clipped = _largest_polygon_part(fp.intersection(lot_poly))
    if clipped.is_empty or clipped.area <= 0.0:
        return fp
    return clipped


def _apply_footprint_aspect_cap(fp: sg.Polygon, aspect_max: float) -> sg.Polygon:
    """Bias a needle-thin footprint back to squarish: if ``fp``'s OBB long/short
    ratio exceeds ``aspect_max`` (> 0), intersect it with a band centered on the
    OBB, keeping the short axis full and shrinking the LONG axis to
    ``short * aspect_max``. The shrink is SYMMETRIC about the OBB center, so it
    only pulls the two long-axis edges inward -- a frontage footprint's front
    edge moves AWAY from the road (its setback can only grow, never shrink), and
    the result always stays inside the original footprint (hence inside the lot).
    ``aspect_max <= 0`` (row/landmark, or any disabled cap) returns ``fp``
    unchanged. A degenerate footprint (empty / zero-area / zero-width OBB) is
    returned untouched -- there is no long axis to square."""
    if aspect_max <= 0.0 or fp.is_empty or fp.area <= 0.0:
        return fp
    center, axis_long, axis_short, long_len, short_len = _obb_axes(fp)
    if short_len <= 1e-12 or long_len <= short_len * aspect_max:
        return fp
    half_long = 0.5 * short_len * aspect_max
    half_short = 0.5 * short_len + 1.0  # generous: the band spans the short axis
    corners = [
        (
            center[0] + s1 * half_long * axis_long[0] + s2 * half_short * axis_short[0],
            center[1] + s1 * half_long * axis_long[1] + s2 * half_short * axis_short[1],
        )
        for s1, s2 in ((-1, -1), (1, -1), (1, 1), (-1, 1))
    ]
    capped = _largest_polygon_part(fp.intersection(sg.Polygon(corners)))
    return capped if (not capped.is_empty and capped.area > 0.0) else fp


def _oriented_footprint(
    lot_poly: sg.Polygon,
    ext_ring: sg.LinearRing,
    typology: str,
    massing: MassingConfig,
    meters_per_unit: float,
    cfg: LotConfig,
    ext_buffer: sg.base.BaseGeometry | None = None,
    road_clear_m_override: float | None = None,
    rear_anchor_degenerate: bool = False,
) -> sg.Polygon:
    """Public footprint entry: build the raw footprint
    (:func:`_oriented_footprint_raw`), then apply the typology's aspect cap
    (:func:`_apply_footprint_aspect_cap`) so a squarish bias reaches EVERY path
    -- including the clamped-fallback slivers that the raw construction emits for
    lots too shallow/narrow to seat a setback footprint (empirically the main
    source of thin detached buildings). The cap is skipped for the S7b rear-
    anchored degenerate case, whose whole purpose is to shove a building off a
    wide arterial -- a symmetric squaring would pull it back toward the road."""
    fp = _oriented_footprint_raw(
        lot_poly,
        ext_ring,
        typology,
        massing,
        meters_per_unit,
        cfg,
        ext_buffer,
        road_clear_m_override,
        rear_anchor_degenerate,
    )
    if rear_anchor_degenerate:
        return fp
    return _apply_footprint_aspect_cap(fp, _aspect_max_for_typology(typology, massing))


def _oriented_footprint_raw(
    lot_poly: sg.Polygon,
    ext_ring: sg.LinearRing,
    typology: str,
    massing: MassingConfig,
    meters_per_unit: float,
    cfg: LotConfig,
    ext_buffer: sg.base.BaseGeometry | None = None,
    road_clear_m_override: float | None = None,
    rear_anchor_degenerate: bool = False,
) -> sg.Polygon:
    """Building footprint from REAL metric setbacks (``docs/wave2-plan.md``
    "The massing model"), replacing the old OBB-fill-ratio sizing.

    In the lot's frontage frame (anchored at the longest boundary edge on
    ``ext_ring``, :func:`_frontage_edge`): ``footprint = lot_poly ∩
    depth_slab``, where ``depth_slab`` runs from ``front_setback`` to
    ``front_setback + depth_max`` (meters, converted to island units via
    ``meters_per_unit``) measured PERPENDICULAR to the frontage edge and INTO
    the lot -- then further intersected with a side strip eroded by
    ``side_setback`` on each along-frontage end (``0`` for ``"row"``: shared
    walls, so adjacent row lots' footprints abut and the run reads as one
    continuous terrace; a real end-of-run inset is deferred, see
    ``docs/wave2-plan.md``'s row footprint note).

    ``front_setback = massing.road_clear_m + <typology>.yard_front_m`` --
    ``road_clear_m`` is denominated from the FRONTAGE LOT LINE (not the
    lot's own OBB), which is what keeps buildings off the road ribbon
    (docs/wave2-plan.md root cause 3: the old fill-ratio footprint inset only
    ~1.25m from the lot line while the street itself reached ~1.9m past it).
    A rear guard caps the slab's far edge (``front_setback + depth_max``
    vs. ``total_lot_depth - rear_min_m``) so a shallow lot always keeps some
    unbuilt depth behind the footprint rather than eating the whole lot.

    Falls back to :func:`_footprint_clamped` (:func:`_footprint`'s inward-
    buffer / representative-point circle, clamped to stay inside the lot)
    whenever the slab construction degenerates (lot too shallow for any
    setback+depth+rear-guard combination, a strip intersection collapses to
    empty, or ``lot_poly`` itself is degenerate) -- every world still gets
    SOME footprint, same guarantee as the pre-2a footprint model, but never
    one that pokes outside its own lot.

    No frontage edge (interior/landlocked lot -- accepted for the 2a bake
    per ``docs/wave2-plan.md``'s "Deferred" list, implied alleys): centers a
    rectangle on the lot's own OBB long/short axes using the DETACHED
    typology's clearances SYMMETRICALLY on both ends of each axis, since
    there is no candidate street edge to set back from directionally --
    a narrower reading of "the current OBB-long-axis behavor with the
    detached clearances" than a one-directional front setback (design
    decision; see this slice's report).

    ``road_clear_m_override`` (S7b, docs/macro-roads-nuclei-plan.md), when not
    ``None``, REPLACES ``massing.road_clear_m`` as the front clearance in the
    frontage-frame branch -- :func:`build_lots` passes the per-lot value it
    derived from the lot's fronting road tier (:class:`FrontingRoadIndex`) so a
    highway-fronting lot sets its building back past the wide highway ribbon
    while a local/interior lot keeps a narrow clearance. ``None`` (every
    pre-S7b caller, and any lot with no fronting-tier info) keeps the flat
    ``massing.road_clear_m``, so those footprints are byte-identical to before.
    The landlocked/interior (no-frontage-edge) branch is unaffected: it never
    used ``road_clear_m`` at all.

    ``rear_anchor_degenerate`` (S7b near-highway follow-up), when ``True`` and
    the frontage slab degenerates because the lot is too SHALLOW to fit the
    front setback (``d_hi <= d_lo`` -- the case a highway/major-fronting lot
    hits under its wide clearance), REPLACES the lot-center clamped fallback
    with a slab anchored at the REAR of the frontage frame: a building of up to
    ``depth_max`` hugging the lot's far (from-the-arterial) edge instead of its
    center. This can only push the degraded building AWAY from the fronting
    road (never toward it); a truly-shallow lot whose whole depth sits inside
    the ribbon still cannot fully clear it (the accepted feasibility floor).
    ``False`` (the default, every non-highway/major lot and every pre-S7b
    caller) keeps the lot-center clamped fallback byte-identically.
    """
    if lot_poly.is_empty or lot_poly.area <= 0.0:
        return _footprint_clamped(lot_poly, cfg.inset)

    yard_front_m, side_m, depth_max_m = _setbacks_for_typology(typology, massing)
    edge = _frontage_edge(lot_poly, ext_ring, cfg.min_frontage, ext_buffer)

    if edge is not None:
        anchor, along = edge
        perp_candidate = np.array([-along[1], along[0]])
        centroid = lot_poly.centroid
        rel_c = np.array([centroid.x, centroid.y]) - anchor
        perp = (
            perp_candidate if float(rel_c @ perp_candidate) >= 0.0 else -perp_candidate
        )

        coords = np.asarray(lot_poly.exterior.coords[:-1], dtype=np.float64)
        rel = coords - anchor
        proj_along = rel @ along
        proj_perp = rel @ perp
        total_depth = float(proj_perp.max())

        road_clear_m = (
            massing.road_clear_m
            if road_clear_m_override is None
            else road_clear_m_override
        )
        front_setback = (road_clear_m + yard_front_m) / meters_per_unit
        depth_max = depth_max_m / meters_per_unit
        rear_min = massing.rear_min_m / meters_per_unit
        d_lo = front_setback
        d_hi = min(front_setback + depth_max, total_depth - rear_min)
        if d_hi <= d_lo:
            if not rear_anchor_degenerate:
                return _footprint_clamped(lot_poly, cfg.inset)
            # S7b near-highway follow-up: the lot is too shallow for the wide
            # front setback, so anchor the building at the REAR of the frontage
            # frame (far edge from the arterial) instead of the lot-center
            # clamped fallback -- a slab of up to depth_max hugging total_depth.
            # Runs through the SAME depth/side-strip machinery below, just with
            # a rear-anchored band; falls back to the clamped footprint only if
            # even that band is degenerate (a zero-depth lot).
            d_hi = total_depth
            d_lo = max(0.0, total_depth - depth_max)
            if d_hi <= d_lo:
                return _footprint_clamped(lot_poly, cfg.inset)

        s_min, s_max = float(proj_along.min()), float(proj_along.max())
        pad = max(s_max - s_min, total_depth, 1.0) * 2.0 + 1.0
        depth_strip = _strip_polygon(
            anchor, along, perp, s_min - pad, s_max + pad, d_lo, d_hi
        )
        candidate = _largest_polygon_part(lot_poly.intersection(depth_strip))
        if candidate.is_empty or candidate.area <= 0.0:
            return _footprint_clamped(lot_poly, cfg.inset)

        side_setback = side_m / meters_per_unit
        cand_coords = np.asarray(candidate.exterior.coords[:-1], dtype=np.float64)
        cand_along = (cand_coords - anchor) @ along
        cs_min, cs_max = float(cand_along.min()), float(cand_along.max())
        s_lo = cs_min + side_setback
        s_hi = cs_max - side_setback
        if s_hi <= s_lo:
            return _footprint_clamped(lot_poly, cfg.inset)
        # S5.1: cap along-frontage width so a wide lot keeps a farmhouse-scale
        # building centered in its frontage span (symmetric side yards), rather
        # than a full-width warehouse slab. 0.0 = uncapped (row terraces,
        # landmarks); detached caps to ~11 m.
        width_max_m = _width_max_for_typology(typology, massing)
        if width_max_m > 0.0:
            width_max = width_max_m / meters_per_unit
            if s_hi - s_lo > width_max:
                s_mid = 0.5 * (s_lo + s_hi)
                s_lo = s_mid - 0.5 * width_max
                s_hi = s_mid + 0.5 * width_max
        side_strip = _strip_polygon(
            anchor, along, perp, s_lo, s_hi, d_lo - pad, d_hi + pad
        )
        footprint = _largest_polygon_part(candidate.intersection(side_strip))
    else:
        center, axis_long, axis_short, long_len, short_len = _obb_axes(lot_poly)
        d_yard_m, d_side_m, d_depth_max_m = _setbacks_for_typology("detached", massing)
        half_w = max(0.0, long_len / 2.0 - d_side_m / meters_per_unit)
        # S5.1: same farmhouse width cap on the landlocked/interior path (uses
        # the detached clearances, so cap with the detached width_max too).
        d_width_max_m = _width_max_for_typology("detached", massing)
        if d_width_max_m > 0.0:
            half_w = min(half_w, 0.5 * d_width_max_m / meters_per_unit)
        half_d = max(
            0.0,
            min(
                short_len / 2.0 - d_yard_m / meters_per_unit,
                d_depth_max_m / (2.0 * meters_per_unit),
            ),
        )
        if half_w <= 0.0 or half_d <= 0.0:
            return _footprint_clamped(lot_poly, cfg.inset)
        corners = [
            (
                center[0] + s1 * half_w * axis_long[0] + s2 * half_d * axis_short[0],
                center[1] + s1 * half_w * axis_long[1] + s2 * half_d * axis_short[1],
            )
            for s1, s2 in ((-1, -1), (1, -1), (1, 1), (-1, 1))
        ]
        footprint = _largest_polygon_part(sg.Polygon(corners).intersection(lot_poly))

    if footprint.is_empty or footprint.area <= 0.0:
        return _footprint_clamped(lot_poly, cfg.inset)
    return footprint


def _percentile_stats(values: list[float]) -> dict[str, float | int]:
    """Shared n/median/p95/max computation for :func:`displacement_stats`'s
    combined stats and its per-``assigned``-kind splits. ``p95`` indexes the
    sorted values at ``ceil(0.95 * n) - 1`` (clamped to the last index), so it
    equals ``max`` for small ``n`` but diverges once ``n`` is large enough for
    the ceiling to land short of the final element."""
    values = sorted(values)
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


def displacement_stats(lots: list[Lot]) -> dict[str, Any]:
    """Median/p95/max ``Lot.displacement`` over OCCUPIED (``kind == "lot"``)
    lots. Greenspace lots carry no world so their (always-0.0) displacement
    isn't meaningful and is excluded. Returns zeros (never NaN/None) when
    there are no occupied lots, so callers can always index the dict.

    The top-level ``n``/``median``/``p95``/``max`` keys are the COMBINED
    stats over every occupied lot regardless of ``assigned`` kind (kept for
    manifest/back-compat). ``"direct"`` and ``"snapped"`` are the SAME four
    keys computed separately per :class:`Lot`'s ``assigned`` field -- see the
    module docstring's bounded-displacement design decision: only the
    ``"direct"`` split is bounded by district size, so collapsing the two
    together (the combined stats alone) can hide a snapped-world outlier
    inside an otherwise-tight distribution."""
    occupied = [lot for lot in lots if lot.kind == "lot"]
    combined: dict[str, Any] = dict(
        _percentile_stats([lot.displacement for lot in occupied])
    )
    combined["direct"] = _percentile_stats(
        [lot.displacement for lot in occupied if lot.assigned == "direct"]
    )
    combined["snapped"] = _percentile_stats(
        [lot.displacement for lot in occupied if lot.assigned == "snapped"]
    )
    return combined


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
    massing: MassingConfig | None = None,
    zone: str = "fringe",
) -> list[Lot]:
    """The pre-L1 naive per-district Voronoi tessellation, kept verbatim as
    :func:`build_lots`'s fallback for districts where street-fronting
    subdivision fails. No world ever moves here: each Voronoi cell IS the
    generating world's own lot, so ``lot_x``/``lot_y`` == ``x``/``y`` and
    ``displacement`` is always ``0.0`` and every lot's ``kind`` is ``"lot"``
    (this path never produces greenspace).

    Height uses :func:`_massing_height` with typology fixed to ``"detached"``
    for every lot (this rare-fallback path predates the wave-2 massing model
    and doesn't attempt area-per-world typology classification against
    Hungarian-displaced Chen-district geometry it never touches -- see
    ``docs/wave2-plan.md``'s massing model, which targets the NORMAL
    subdivision path). ``zone`` (S5) still applies to that fixed "detached"
    typology's story band -- passed through from :func:`build_lots`'s own
    ``zone`` kwarg so a major-nucleus district that happens to fall back to
    Voronoi doesn't silently lose its zone grading (in practice inert by
    default: ``core``/``inner`` detached bands equal the suburb-wide one, see
    ``DEFAULT_CORE_DETACHED_STORIES``). Footprint is unchanged from pre-2a:
    the plain inset (:func:`_footprint`), not the frontage-setback model.

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
    if massing is None:
        massing = MassingConfig()
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

    heights = [
        _massing_height(wid, "detached", massing, zone=zone) for wid in world_ids
    ]

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
                typology="detached",
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
                typology="detached",
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
                typology="detached",
            )
        )
    return lots


# ---------------------------------------------------------------------------
# Public entry point: street-fronting subdivision + Hungarian assignment,
# falling back to _build_lots_voronoi on subdivision failure.
# ---------------------------------------------------------------------------


def _relaxed_lot_configs(cfg: LotConfig) -> list[LotConfig]:
    """Progressive shape-floor relaxation ladder for :func:`build_lots`'s
    per-district subdivision retry (docs/macro-roads-nuclei-plan.md wave-2
    "subdivision robustness": F1's hard shape-floor -- reject-outright, not
    merely score-down -- made dense districts (where the target per-lot area
    forces a width below ``cfg.min_lot_width``) trip an unfillable deficit and
    fall back to the whole-district Voronoi mechanism far more often than the
    pre-F1 code did).

    ``cfg`` itself is always first (the caller's strict floor -- a district
    that already reaches ``n_lots`` under it never sees any other entry, so a
    normal district's behavior is byte-identical to before this fix). Each
    following step halves ``min_lot_width`` and raises ``max_aspect_reject``
    by 50% (three steps), and the final entry disables the floor entirely
    (``min_lot_width=0``, ``max_aspect_reject=0``) -- :func:`build_lots` only
    falls back to :func:`_build_lots_voronoi` once even THAT no-floor attempt
    fails (which then means a genuine geometry pathology, e.g. empty/zero-area
    district, rather than an over-tight shape floor). Returns ``[cfg]``
    unchanged when ``cfg`` already has both halves of the floor disabled --
    nothing left to relax. Every other ``LotConfig`` field (``stop_area_factor``,
    ``aspect_clamp``, ``inset``, etc.) is copied from ``cfg`` untouched at every
    step."""
    if cfg.min_lot_width <= 0.0 and cfg.max_aspect_reject <= 0.0:
        return [cfg]
    ladder = [cfg]
    width = cfg.min_lot_width
    aspect = cfg.max_aspect_reject
    for _ in range(3):
        if width <= 0.0 and aspect <= 0.0:
            break
        width = width * 0.5 if width > 0.0 else 0.0
        aspect = aspect * 1.5 if aspect > 0.0 else 0.0
        ladder.append(replace(cfg, min_lot_width=width, max_aspect_reject=aspect))
    ladder.append(replace(cfg, min_lot_width=0.0, max_aspect_reject=0.0))
    return ladder


def _record_s7b_tier(stats: dict[str, Any] | None, tier: str | None) -> None:
    """Tally one OCCUPIED lot's fronting-road tier into ``stats`` (S7b report
    counters). No-op when ``stats``/``tier`` is ``None`` (no index threaded /
    landlocked lot), so the flat pre-S7b path leaves ``fallback_stats``
    byte-identical (no ``s7b_*`` keys)."""
    if stats is None or tier is None:
        return
    by_tier = stats.setdefault("s7b_setback_by_tier", {})
    by_tier[tier] = by_tier.get(tier, 0) + 1
    if tier in ("highway", "major"):
        stats["s7b_wide_setback_lots"] = stats.get("s7b_wide_setback_lots", 0) + 1


def build_lots(
    district: sg.Polygon,
    district_id: int,
    member_points: pl.DataFrame,
    *,
    inset: float = DEFAULT_INSET,
    min_lot_area_frac: float = DEFAULT_MIN_LOT_AREA_FRAC,
    seed: int | None = None,
    cfg: LotConfig | None = None,
    massing: MassingConfig | None = None,
    meters_per_unit: float = DEFAULT_METERS_PER_UNIT,
    landmark_ids: frozenset[str] = frozenset(),
    fallback_stats: dict[str, Any] | None = None,
    zone: str = "fringe",
    fronting_index: FrontingRoadIndex | None = None,
) -> list[Lot]:
    """Tessellate ``district`` into street-fronting lots, assign its member
    worlds onto them (Hungarian, exact), and give each a massing-model
    typology + setback footprint + height (``docs/wave2-plan.md``).

    ``member_points`` needs ``world_id, x, y, visits, assigned`` columns
    (``name`` optional, else every ``Lot.name`` is ``None``) -- see
    :func:`select_member_points`.

    ``inset``/``min_lot_area_frac`` are the pre-L1 kwargs (still accepted
    directly so existing callers -- e.g. ``scripts/run_r1_hybrid.py``'s
    ``export_greybox`` -- keep working unmodified); passing an explicit
    ``cfg`` overrides both at once. ``massing``/``meters_per_unit``/
    ``landmark_ids`` drive :func:`classify_typology`/:func:`_massing_height`/
    :func:`_oriented_footprint` for every world in ``member_points`` --
    ``massing`` defaults to ``MassingConfig()`` and ``landmark_ids`` defaults
    to an empty set (no world classifies ``"landmark"``) so existing callers
    that don't yet pass them keep the suburb-default behavior. ``zone`` (S5,
    docs/macro-roads-nuclei-plan.md) is this WHOLE district's massing zone --
    ``"core"``/``"inner"``/``"fringe"``, a district-level property the caller
    derives once from ``nucleus_dist`` (see ``scripts/run_r1_hybrid.py``'s
    ``_zone_for_district``) and passes straight through to every world's
    :func:`_massing_height` here; it does NOT affect typology (
    :func:`classify_typology` is unchanged) or footprint, only the story
    band. Defaults to ``"fringe"``, which reproduces the pre-S5 suburb-wide
    story bands byte-identically, so existing callers that don't pass it are
    unaffected. ``seed``
    defaults to ``district_id`` (deterministic given a fixed district walk
    order, decorrelated across districts sharing a shape). ``fallback_stats``,
    if given a dict, gets ``"n_fallback"`` incremented and a
    ``"fallback_districts"`` list entry appended (``{"district_id",
    "reason"}``) whenever THIS district falls back to
    :func:`_build_lots_voronoi` -- i.e. even the fully-relaxed (floor-disabled)
    end of :func:`_relaxed_lot_configs`'s ladder failed. It also gets
    ``"n_relaxed"`` incremented and a ``"relaxed_districts"`` list entry
    appended (``{"district_id", "relax_step"}``, ``relax_step`` the index
    (>= 1) into :func:`_relaxed_lot_configs`'s ladder that finally succeeded --
    equivalently, how many relaxation steps beyond ``cfg``'s own strict floor
    were needed) whenever this district
    needed ANY relaxation to reach ``n`` lots -- a district that reaches ``n``
    under ``cfg``'s own strict floor records neither.

    ``fronting_index`` (S7b, docs/macro-roads-nuclei-plan.md), when given,
    denominates each lot's FRONT setback from the WIDTH of the road its
    frontage edge actually fronts (highway -> widest) instead of the flat
    ``massing.road_clear_m``, and makes a lot too SHALLOW to fit any building
    behind a highway/major setback NO-BUILD: such lots are EXCLUDED from the
    Hungarian assignable set (so their worlds re-displace onto deeper lots in
    the SAME district and the shallow lots become ``kind="greenspace"``),
    provided enough buildable lots remain (``>= n``); if too few do, the
    exclusion is skipped (feasibility) so no world is ever lost -- those worlds
    keep their shallow lot and degrade to the existing shallow-lot footprint
    fallback. ``None`` (the default, every pre-S7b caller) keeps the flat
    ``road_clear_m`` and never excludes a lot, so output is byte-identical to
    before S7b (regression guard). When ``fallback_stats`` is given, S7b tallies
    are recorded under ``s7b_setback_by_tier`` / ``s7b_wide_setback_lots`` /
    ``s7b_no_build_displaced`` / ``s7b_no_build_infeasible_districts``.

    Typology/height are computed PER WORLD from the district-level
    ``(district.area, len(member_points))`` pair (:func:`classify_typology`)
    -- every world in the SAME district shares the same area-per-world ratio,
    so only ``landmark_ids`` membership can make two worlds in one district
    classify differently.

    Degenerate cases:

    - **0 points**: returns ``[]`` (empty/park districts are the caller's
      concern -- they simply never call this).
    - **1 point**: the lot IS the whole district (no subdivision needed).
    - **Subdivision failure** (invalid geometry, pathological concavity, or
      :func:`subdivide_district` cannot reach ``n`` lots even after its
      largest-first re-split retries -- AND after :func:`_relaxed_lot_configs`'s
      whole progressive-relaxation ladder, up to and including a no-floor
      attempt, is exhausted): falls back to :func:`_build_lots_voronoi` for
      this ENTIRE district (never a partial fallback -- mixing the two lot
      mechanisms within one district would make the partition invariant
      meaningless).

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

    if cfg is None:
        cfg = LotConfig(inset=inset, voronoi_min_lot_area_frac=min_lot_area_frac)
    if massing is None:
        massing = MassingConfig()
    if seed is None:
        seed = district_id

    typologies = [
        classify_typology(district.area, n, wid, landmark_ids, massing, meters_per_unit)
        for wid in world_ids
    ]
    heights = [
        _massing_height(wid, typ, massing, zone=zone)
        for wid, typ in zip(world_ids, typologies, strict=True)
    ]

    ext_ring = district.exterior
    # Computed ONCE per build_lots call and threaded through every
    # _oriented_footprint call below, instead of re-buffering ext_ring per
    # lot (docs/lots-wave-plan.md perf fix).
    ext_buffer = ext_ring.buffer(_FRONTAGE_TOUCH_TOL)

    if n == 1:
        lot_poly = district
        # S7b: a single-world district still sets its building back from the
        # tier of the road it fronts (no exclusion possible -- one world, one
        # lot -- so a shallow highway-fronting solo lot just degrades to the
        # existing shallow-lot footprint fallback rather than becoming a lost
        # greenspace). rc stays None (flat road_clear_m) when no index / no
        # frontage edge -- byte-identical to pre-S7b.
        rc0: float | None = None
        rear0 = False
        if fronting_index is not None:
            seg0 = _longest_frontage_segment(
                lot_poly, ext_ring, cfg.min_frontage, ext_buffer
            )
            if seg0 is not None:
                tier0 = fronting_index.fronting_tier(seg0) or "street"
                rc0 = fronting_index.road_clear_m(tier0)
                rear0 = tier0 in ("highway", "major")
                _record_s7b_tier(fallback_stats, tier0)
        return [
            Lot(
                world_id=world_ids[0],
                district_id=district_id,
                footprint=_oriented_footprint(
                    lot_poly,
                    ext_ring,
                    typologies[0],
                    massing,
                    meters_per_unit,
                    cfg,
                    ext_buffer,
                    road_clear_m_override=rc0,
                    rear_anchor_degenerate=rear0,
                ),
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
                typology=typologies[0],
            )
        ]

    # Subdivision path: try cfg's strict shape-floor first, then -- ONLY if
    # that can't reach n lots -- progressively relax it per _relaxed_lot_configs
    # (docs/macro-roads-nuclei-plan.md wave-2 "subdivision robustness": F1's
    # hard-reject shape-floor made dense districts trip an unfillable deficit
    # and fall back to the whole-district Voronoi mechanism far more often
    # than intended). Falls back to _build_lots_voronoi only once even the
    # ladder's final no-floor attempt fails -- invalid geometry, pathological
    # concavity, or a genuinely unfillable deficit -- deliberately broad
    # because the fallback exists precisely to absorb geometry pathologies we
    # cannot enumerate in advance. A district that already succeeds under
    # cfg's strict floor (the common case) takes exactly one attempt, so its
    # behavior is unchanged from before this fix.
    lot_polys: list[sg.Polygon] | None = None
    last_exc: Exception | None = None
    relaxed_step = 0
    for attempt_idx, attempt_cfg in enumerate(_relaxed_lot_configs(cfg)):
        try:
            candidate = subdivide_district(district, n, seed, attempt_cfg)
            if len(candidate) < n:
                # Belt-and-braces: subdivide_district's own contract is M >=
                # N, but this is the IO/logic boundary where a future change
                # to that function (or a pathological case it doesn't itself
                # detect) could silently under-deliver. Treat it exactly like
                # any other subdivision failure rather than letting a short
                # candidate list reach the Hungarian assignment below (which
                # would raise or silently mismatch instead of retrying/
                # falling back cleanly).
                raise _SubdivisionFailure(
                    f"subdivide_district returned {len(candidate)} lots < {n} requested"
                )
            _assert_partition(district, candidate)
        except Exception as exc:
            last_exc = exc
            continue
        lot_polys = candidate
        relaxed_step = attempt_idx
        break

    if lot_polys is None:
        if fallback_stats is not None:
            fallback_stats["n_fallback"] = fallback_stats.get("n_fallback", 0) + 1
            fallback_stats.setdefault("fallback_districts", []).append(
                {"district_id": district_id, "reason": str(last_exc)}
            )
        return _build_lots_voronoi(
            district,
            district_id,
            member_points,
            inset=cfg.inset,
            min_lot_area_frac=cfg.voronoi_min_lot_area_frac,
            massing=massing,
            zone=zone,
        )
    if fallback_stats is not None and relaxed_step > 0:
        fallback_stats["n_relaxed"] = fallback_stats.get("n_relaxed", 0) + 1
        fallback_stats.setdefault("relaxed_districts", []).append(
            {"district_id": district_id, "relax_step": relaxed_step}
        )

    anchors: list[tuple[float, float]] = []
    for poly in lot_polys:
        centroid = poly.centroid
        anchor = centroid if poly.contains(centroid) else poly.representative_point()
        anchors.append((anchor.x, anchor.y))

    # S7b (docs/macro-roads-nuclei-plan.md): per-lot fronting tier -> front
    # clearance override + near-highway no-build. INERT unless a
    # FrontingRoadIndex is threaded in: with fronting_index=None every
    # lot_road_clear stays None (flat massing.road_clear_m) and lot_no_build
    # stays all-False, so the assignment + footprints below are byte-identical
    # to pre-S7b (regression guard). The no-build depth test uses the
    # district's AREA-ONLY base typology's front yard (a lot's world -- and so
    # its true typology -- isn't known until AFTER assignment; the highway
    # half-width dominates the ~2 m yard difference, so the base tier is a fine
    # proxy for "fundamentally too shallow for a highway building").
    lot_road_clear: list[float | None] = [None] * len(lot_polys)
    lot_tier: list[str | None] = [None] * len(lot_polys)
    lot_no_build: list[bool] = [False] * len(lot_polys)
    if fronting_index is not None:
        base_typology = classify_typology(
            district.area, n, "", frozenset(), massing, meters_per_unit
        )
        base_yard_m, _base_side_m, _base_depth_m = _setbacks_for_typology(
            base_typology, massing
        )
        rear_min = massing.rear_min_m / meters_per_unit
        for j, poly in enumerate(lot_polys):
            seg = _longest_frontage_segment(
                poly, ext_ring, cfg.min_frontage, ext_buffer
            )
            if seg is None:
                continue
            tier = fronting_index.fronting_tier(seg) or "street"
            lot_tier[j] = tier
            rc = fronting_index.road_clear_m(tier)
            lot_road_clear[j] = rc
            if tier in ("highway", "major"):
                edge = _frontage_edge(poly, ext_ring, cfg.min_frontage, ext_buffer)
                if edge is not None:
                    total_depth = _frontage_depth_units(poly, edge[0], edge[1])
                    front_setback = (rc + base_yard_m) / meters_per_unit
                    if total_depth - rear_min - front_setback <= 0.0:
                        lot_no_build[j] = True

    # Exclude no-build (too-shallow highway/major) lots from the Hungarian
    # assignable columns so their worlds re-displace onto deeper lots in this
    # SAME district and the shallow lots fall through to greenspace -- but only
    # while enough buildable lots remain (>= n); otherwise keep every lot
    # assignable so no world is ever lost (feasibility). cols==range(M) is the
    # pre-S7b behavior (byte-identical when nothing is excluded).
    buildable_cols = [j for j in range(len(lot_polys)) if not lot_no_build[j]]
    n_excluded = len(lot_polys) - len(buildable_cols)
    if n_excluded > 0 and len(buildable_cols) >= n:
        cols = buildable_cols
        if fallback_stats is not None:
            fallback_stats["s7b_no_build_displaced"] = (
                fallback_stats.get("s7b_no_build_displaced", 0) + n_excluded
            )
    else:
        cols = list(range(len(lot_polys)))
        if n_excluded > 0 and fallback_stats is not None:
            fallback_stats["s7b_no_build_infeasible_districts"] = (
                fallback_stats.get("s7b_no_build_infeasible_districts", 0) + 1
            )

    xs_arr = np.asarray(xs, dtype=np.float64)
    ys_arr = np.asarray(ys, dtype=np.float64)
    anchor_arr = np.asarray(anchors, dtype=np.float64)
    anchor_cols = anchor_arr[cols]
    cost = (xs_arr[:, None] - anchor_cols[None, :, 0]) ** 2 + (
        ys_arr[:, None] - anchor_cols[None, :, 1]
    ) ** 2
    row_ind, col_ind = linear_sum_assignment(cost)
    lot_for_world = {
        int(r): cols[int(c)] for r, c in zip(row_ind, col_ind, strict=True)
    }

    lots: list[Lot] = []
    used_lot_idx = set(lot_for_world.values())
    for i in range(n):
        lot_idx = lot_for_world[i]
        lot_poly = lot_polys[lot_idx]
        ax, ay = anchors[lot_idx]
        _record_s7b_tier(fallback_stats, lot_tier[lot_idx])
        lots.append(
            Lot(
                world_id=world_ids[i],
                district_id=district_id,
                footprint=_oriented_footprint(
                    lot_poly,
                    ext_ring,
                    typologies[i],
                    massing,
                    meters_per_unit,
                    cfg,
                    ext_buffer,
                    road_clear_m_override=lot_road_clear[lot_idx],
                    rear_anchor_degenerate=lot_tier[lot_idx] in ("highway", "major"),
                ),
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
                typology=typologies[i],
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
                typology="",
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
