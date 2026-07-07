"""R1 connectivity stage 1 — extract block-boundary street "gates".

The macro/micro hybrid citygen (``docs/large-scale-growth-research.md``)
generates each macro-block's local street fabric with a per-block Chen run
(``mapgen.r1_seam.chen_in_block``) whose boundary *is* the macro-block
polygon. Chen already seeds that boundary ring as the level-0 street
(``chen_generate._run_level_loop``, ``mapgen/src/mapgen/chen_generate.py``
line 620-621: ``street_mesh_edges: set[EdgeKey] = _editor_boundary_ring_edges(
editor)``), so every Chen street path that reaches the block edge terminates
on a boundary-ring vertex. ``chen_mesh.ParcelMeshEditor.snapshot`` (``chen_mesh.py``
lines 486-496) marks those vertices ``MeshVertex.on_boundary=True`` with their
exact projected boundary coordinates.

This module is pure/generation-free: it reads an already-built
:class:`~mapgen.chen_core.ChenLayout` and extracts those boundary-touching
street endpoints as "gates" — the junction points where a per-block Chen
street network should later be fused to the macro arterial network. It does
not modify Chen generation or street topology.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import networkx as nx
from shapely.geometry import LineString, Point, Polygon
from shapely.ops import substring
from shapely.strtree import STRtree

from mapgen.chen_artifacts import _street_lines
from mapgen.chen_core import ChenLayout, StreetPath, node_key
from mapgen.r1_macro import MacroEdge


@dataclass(frozen=True)
class Gate:
    """One block-boundary street endpoint, a candidate arterial junction."""

    x: float
    y: float
    street_id: int
    vertex_id: int


@dataclass(frozen=True)
class BlockGates:
    """Boundary gates for one block's Chen layout, deterministically ordered."""

    gates: tuple[Gate, ...]
    perimeter_street_ids: frozenset[int]


def _all_nodes_on_boundary(layout: ChenLayout, street: StreetPath) -> bool:
    """True if every node of ``street`` sits on the block boundary.

    Such a street coincides with the level-0 boundary ring itself (the
    arterial/coast the block was cut against), not an interior street that
    merely *touches* the boundary at its ends. See module docstring for the
    boundary-ring seeding this relies on.
    """
    return all(layout.mesh.vertices[node].on_boundary for node in street.nodes)


def extract_block_gates(layout: ChenLayout) -> BlockGates:
    """Extract boundary-touching street endpoints as junction candidates.

    For each decomposed street path (``layout.street_graph.streets``):

    - if every node is on the boundary, the path coincides with the
      boundary ring (arterial/ring/coast) rather than an interior street;
      its ``street_id`` is recorded in ``perimeter_street_ids`` and no gate
      is emitted for it (it is handled separately, by the ring/arterial
      fusion, not as a per-street gate).
    - otherwise, a :class:`Gate` is emitted for each *endpoint* node
      (``street.nodes[0]`` / ``street.nodes[-1]``) that is on the boundary
      (deduplicated if both endpoints coincide, e.g. a closed loop).
      Interior (non-endpoint) nodes are never gates, even if incidentally
      on the boundary.

    Gates are sorted deterministically by ``(street_id, vertex_id)``.
    """
    gates: list[Gate] = []
    perimeter_street_ids: set[int] = set()
    for street in layout.street_graph.streets:
        if _all_nodes_on_boundary(layout, street):
            perimeter_street_ids.add(street.street_id)
            continue
        endpoint_ids = {street.nodes[0], street.nodes[-1]}
        for vertex_id in endpoint_ids:
            vertex = layout.mesh.vertices[vertex_id]
            if vertex.on_boundary:
                gates.append(
                    Gate(
                        x=vertex.point[0],
                        y=vertex.point[1],
                        street_id=street.street_id,
                        vertex_id=vertex_id,
                    )
                )
    gates.sort(key=lambda gate: (gate.street_id, gate.vertex_id))
    return BlockGates(tuple(gates), frozenset(perimeter_street_ids))


def street_perimeter_flags(layout: ChenLayout) -> list[bool]:
    """One bool per street line in ``_street_lines(layout)`` order.

    ``True`` marks a boundary-ring-coincident street (see
    :func:`extract_block_gates`'s ``perimeter_street_ids`` classification).
    Iterates ``mapgen.chen_artifacts._street_lines`` directly (rather than
    ``layout.street_graph.streets``) so the result is index-aligned with the
    ``streets`` list ``chen_in_block``/the seam-spike scripts build from that
    same helper, including its skip of degenerate (``None``-line) paths.
    """
    streets_by_id = {street.street_id: street for street in layout.street_graph.streets}
    flags: list[bool] = []
    for _line, properties in _street_lines(layout):
        street = streets_by_id[properties["street_id"]]
        flags.append(_all_nodes_on_boundary(layout, street))
    return flags


# ---------------------------------------------------------------------------
# Stage 2 — snap gates to the macro network; connectivity metric.
# ---------------------------------------------------------------------------
#
# A gate (above) is a per-block Chen street endpoint sitting exactly on the
# block boundary. That boundary is exact sub-geometry of the macro network the
# block was polygonized from (``mapgen.r1_macro.build_macro_layer``): a
# clipped arterial segment (``MacroLayer.arterial_lines`` / ``.edges``,
# index-aligned — see ``arterials_to_geojson`` in ``r1_macro.py``), a
# dense-core ring (``MacroLayer.ring_lines``), or the island coastline
# (``mapgen.r1_macro.load_boundary``). This section snaps each gate back onto
# whichever macro feature it came from and reports a connectivity metric over
# the result.


@dataclass(frozen=True)
class SeamJunction:
    """One gate snapped onto the macro network (or left unmatched).

    Attributes
    ----------
    block_id : the macro-block the gate belongs to.
    x, y : the gate's (island-frame) coordinates, copied from ``Gate``.
    kind : ``"arterial"``, ``"ring"``, ``"coast"``, or ``"unmatched"``.
    macro_index : index into the matched geometry list — ``arterial_lines``
        for ``"arterial"``, ``ring_lines`` for ``"ring"``, ``0`` for
        ``"coast"`` (there is exactly one coastline), ``-1`` for
        ``"unmatched"`` (nothing matched).
    tier : the arterial's construction tier (``MacroEdge.tier``:
        2=highway, 1=major, 0=local) for ``"arterial"``; ``-1`` otherwise.
    station : arc-length position of the gate along the matched line
        (``line.project(point)``); ``0.0`` for ``"unmatched"``.
    distance : the gate's distance to the matched feature. For
        ``"unmatched"`` this is the best (smallest) distance found among the
        macro-network nearest-neighbour and the coastline, so callers can
        still see how close a near-miss was.
    """

    block_id: int
    x: float
    y: float
    kind: str
    macro_index: int
    tier: int
    station: float
    distance: float


def snap_gates_to_macro(
    gates_by_block: dict[int, list[Gate]],
    arterial_lines: list[LineString],
    edges: list[MacroEdge],
    ring_lines: list[LineString],
    boundary: Polygon,
    *,
    tol: float = 0.05,
) -> list[SeamJunction]:
    """Snap every block's gates onto the macro network (arterial/ring/coast).

    For each gate, the nearest of ``arterial_lines + ring_lines`` is found via
    a shared :class:`~shapely.strtree.STRtree`; if within ``tol`` it is
    classified ``"arterial"`` (tier taken from the paired ``edges`` entry,
    ``arterial_lines``/``edges`` must be index-aligned, as they are in
    ``MacroLayer``) or ``"ring"``. Otherwise the gate is checked against the
    island boundary exterior (``boundary.exterior``); within ``tol`` it is
    ``"coast"``. Anything left over is ``"unmatched"``.

    Because gates are exact boundary points and a block edge is exact
    sub-geometry of the polygonized macro lines, the matched distance should
    be ~0 in practice — ``tol`` is a safety net, and the real distance is
    still recorded on every :class:`SeamJunction` so the connectivity metric
    can report the distance distribution (a coincidence sanity check).

    Blocks with no gates (``gates_by_block[block_id] == []``, common for
    coarse splits) simply contribute nothing. Output is sorted by
    ``(block_id, x, y)`` for determinism, independent of ``gates_by_block``
    iteration/dict order.
    """
    n_arterial = len(arterial_lines)
    candidates: list[LineString] = [*arterial_lines, *ring_lines]
    tree = STRtree(candidates) if candidates else None
    coast_line = LineString(boundary.exterior.coords)

    junctions: list[SeamJunction] = []
    for block_id in sorted(gates_by_block):
        for gate in gates_by_block[block_id]:
            point = Point(gate.x, gate.y)

            macro_index: int | None = None
            macro_distance = float("inf")
            if tree is not None:
                idx_arr, dist_arr = tree.query_nearest(
                    point, return_distance=True, all_matches=False
                )
                if idx_arr.size > 0:
                    macro_index = int(idx_arr[0])
                    macro_distance = float(dist_arr[0])

            coast_distance = float(point.distance(coast_line))

            if macro_index is not None and macro_distance <= tol:
                line = candidates[macro_index]
                if macro_index < n_arterial:
                    kind = "arterial"
                    feature_index = macro_index
                    tier = edges[macro_index].tier
                else:
                    kind = "ring"
                    feature_index = macro_index - n_arterial
                    tier = -1
                station = float(line.project(point))
                distance = macro_distance
            elif coast_distance <= tol:
                kind = "coast"
                feature_index = 0
                tier = -1
                station = float(coast_line.project(point))
                distance = coast_distance
            else:
                kind = "unmatched"
                feature_index = -1
                tier = -1
                station = 0.0
                distance = min(macro_distance, coast_distance)

            junctions.append(
                SeamJunction(
                    block_id=block_id,
                    x=gate.x,
                    y=gate.y,
                    kind=kind,
                    macro_index=feature_index,
                    tier=tier,
                    station=station,
                    distance=distance,
                )
            )

    junctions.sort(key=lambda j: (j.block_id, j.x, j.y))
    return junctions


def unplugged_runs(line_length: float, stations: list[float]) -> list[float]:
    """Gap lengths between consecutive junction stations on one line.

    ``stations`` need not include the line's own endpoints — the two ends
    (``0.0`` and ``line_length``) always count as gap boundaries too (a run
    from the end of a line to its first junction is just as "unplugged" as a
    run between two junctions), so they are added internally before sorting.
    Input need not be pre-sorted or de-duplicated. Returns the positive gaps
    between consecutive (deduplicated) stations, in ascending-station order.

    ``line_length <= 0`` (degenerate line) returns ``[]``.
    """
    if line_length <= 0:
        return []
    clamped = sorted(
        {0.0, line_length, *(min(max(s, 0.0), line_length) for s in stations)}
    )
    return [b - a for a, b in zip(clamped, clamped[1:], strict=False) if b - a > 0]


def connectivity_metrics(
    junctions: list[SeamJunction],
    arterial_lines: list[LineString],
    edges: list[MacroEdge],
    ring_lines: list[LineString],
    blocks: list[Polygon],
) -> dict[str, Any]:
    """Summarize seam connectivity as a plain JSON-serializable dict.

    ``ring_lines`` is accepted for interface symmetry with
    :func:`snap_gates_to_macro` (a future revision may report per-ring
    unplugged runs the same way arterials do); it is not otherwise used here.
    """
    if len(arterial_lines) != len(edges):
        raise ValueError("arterial_lines and edges must be index-aligned")

    n_gates_total = len(junctions)
    by_kind: dict[str, int] = {"arterial": 0, "ring": 0, "coast": 0, "unmatched": 0}
    arterial_by_tier: dict[int, int] = {}
    for j in junctions:
        by_kind[j.kind] = by_kind.get(j.kind, 0) + 1
        if j.kind == "arterial":
            arterial_by_tier[j.tier] = arterial_by_tier.get(j.tier, 0) + 1
    n_tjunctions = by_kind["arterial"] + by_kind["ring"]

    # Per-block T-junction counts, over blocks that have at least one gate
    # (a genuinely gate-less block, common for coarse splits, isn't a data
    # point for "did its gates connect" — it never had gates to connect).
    tjunctions_by_block: dict[int, int] = {}
    gates_by_block_count: dict[int, int] = {}
    for j in junctions:
        gates_by_block_count[j.block_id] = gates_by_block_count.get(j.block_id, 0) + 1
        if j.kind in ("arterial", "ring"):
            tjunctions_by_block[j.block_id] = tjunctions_by_block.get(j.block_id, 0) + 1

    non_empty_block_ids = sorted(gates_by_block_count)
    per_block_counts = [tjunctions_by_block.get(bid, 0) for bid in non_empty_block_ids]
    if per_block_counts:
        per_block_min = min(per_block_counts)
        per_block_median = float(statistics.median(per_block_counts))
        per_block_max = max(per_block_counts)
        n_blocks_zero_tjunctions = sum(1 for c in per_block_counts if c == 0)
    else:
        per_block_min = 0
        per_block_median = 0.0
        per_block_max = 0
        n_blocks_zero_tjunctions = 0

    arterial_length_total = float(sum(line.length for line in arterial_lines))
    tjunctions_per_10 = (
        n_tjunctions * 10.0 / arterial_length_total
        if arterial_length_total > 0
        else 0.0
    )

    all_gaps: list[float] = []
    for i, line in enumerate(arterial_lines):
        stations = [
            j.station for j in junctions if j.kind == "arterial" and j.macro_index == i
        ]
        all_gaps.extend(unplugged_runs(float(line.length), stations))
    unplugged_max = max(all_gaps) if all_gaps else 0.0
    unplugged_median = float(statistics.median(all_gaps)) if all_gaps else 0.0

    # Snap-distance sanity check over MATCHED junctions only (arterial/ring/
    # coast); "unmatched" distances are by definition > tol and would drown
    # out the "did the matches actually coincide" signal this is for.
    matched_distances = [j.distance for j in junctions if j.kind != "unmatched"]
    snap_distance_max = max(matched_distances) if matched_distances else 0.0
    snap_distance_mean = (
        float(statistics.mean(matched_distances)) if matched_distances else 0.0
    )

    return {
        "n_gates_total": n_gates_total,
        "n_tjunctions": n_tjunctions,
        "n_arterial": by_kind["arterial"],
        "n_arterial_by_tier": {
            str(tier): count for tier, count in sorted(arterial_by_tier.items())
        },
        "n_ring": by_kind["ring"],
        "n_coast": by_kind["coast"],
        "n_unmatched": by_kind["unmatched"],
        "n_blocks_total": len(blocks),
        "n_blocks_with_gates": len(non_empty_block_ids),
        "n_blocks_no_gates": len(blocks) - len(non_empty_block_ids),
        "per_block_tjunctions_min": per_block_min,
        "per_block_tjunctions_median": per_block_median,
        "per_block_tjunctions_max": per_block_max,
        "n_blocks_zero_tjunctions": n_blocks_zero_tjunctions,
        "arterial_length_total": round(arterial_length_total, 3),
        "tjunctions_per_10_units_arterial": round(tjunctions_per_10, 4),
        "unplugged_run_max": round(unplugged_max, 4),
        "unplugged_run_median": round(unplugged_median, 4),
        "snap_distance_max": round(snap_distance_max, 6),
        "snap_distance_mean": round(snap_distance_mean, 6),
    }


# ---------------------------------------------------------------------------
# Stage 3 — fuse arterial/ring/local geometry into one unified street graph.
# ---------------------------------------------------------------------------
#
# Stage 2 tells us *where* a per-block gate lands on the macro network
# (station + kind + macro_index). This section actually cuts the macro lines
# at those stations and stitches every feature — split arterial segments,
# split ring segments, and non-perimeter local street paths — into one
# ``networkx`` graph. Node identity is coordinate identity at Chen's own
# quantization (``mapgen.chen_core.node_key`` / ``NODE_SCALE``), so a gate
# and the arterial split-point it snapped to (station-projected back onto the
# same line) fuse into a single graph node — that fusion, not any explicit
# "connect gate to junction" step, is what turns two independently-generated
# per-block street networks into one connected T-junction.
#
# ``_all_nodes_on_boundary`` (stage 1) is an all-or-nothing per-street test: if
# a single node's ``on_boundary`` flag is wrong (numeric drift, or a mesh merge
# that didn't update it), a street that is otherwise the block's own boundary
# ring gets classified "local" instead of "perimeter" and would be re-added
# here as its own edge chain, running alongside the independently-added
# arterial/ring line it actually traces — a doubled near-coincident road
# (wave-2 F2, ``docs/macro-roads-nuclei-plan.md``). Rather than restructure
# stage 1 into a per-edge classification (which would ripple
# ``street_perimeter_flags``'s per-*street* bool-list shape through
# ``r1_seam.py``/``run_r1_hybrid.py``, both of which consume it for several
# unrelated purposes), the fix is applied here, geometrically:
# :func:`build_unified_street_graph` drops (does not add) any "local" segment
# whose both endpoints and midpoint sit within ``dedup_tolerance`` of an
# arterial/ring line, before adding it. A genuinely local street only grazes
# the boundary at its gate end, so this only ever catches segments that hug a
# macro line over their whole length.


def _node_key(x: float, y: float) -> tuple[int, int]:
    """Chen's coordinate-identity key (``mapgen.chen_core.node_key``, ``NODE_SCALE``).

    Thin ``(x, y)`` wrapper around the shared helper so fused nodes here use
    exactly the same quantization Chen's own mesh vertices do.
    """
    return node_key((x, y))


def split_line_at_stations(
    line: LineString, stations: list[float], *, min_gap: float = 1e-6
) -> list[LineString]:
    """Split ``line`` into ordered sub-segments at the given arc-length stations.

    ``stations`` need not be sorted or de-duplicated. Stations are clamped to
    ``[0, line.length]``, then any station within ``min_gap`` of the previous
    kept station *or* of either endpoint is dropped (endpoints never produce a
    degenerate zero-length segment; near-coincident stations collapse to one
    cut). Empty (or fully-dropped) ``stations`` returns a single-element list
    containing the whole line.
    """
    length = line.length
    if length <= 0.0:
        return [line]

    clamped = sorted(min(max(s, 0.0), length) for s in stations)
    kept: list[float] = []
    for station in clamped:
        if station <= min_gap:
            continue
        if length - station <= min_gap:
            continue
        if kept and station - kept[-1] <= min_gap:
            continue
        kept.append(station)

    cut_points = [0.0, *kept, length]
    return [
        substring(line, a, b)
        for a, b in zip(cut_points, cut_points[1:], strict=False)
        if b - a > 0.0
    ]


# Default near-coincident dedup tolerance (island units) for local-street
# segments against arterial/ring lines (see the stage-3 preamble above and
# ``build_unified_street_graph``'s ``dedup_tolerance`` param). Same band as
# ``snap_gates_to_macro``'s default ``tol`` -- both describe "close enough to
# be the same feature", just applied to a segment instead of a single gate.
DEFAULT_LOCAL_DEDUP_TOLERANCE: float = 0.05


def _segment_hugs_macro(
    x0: float, y0: float, x1: float, y1: float, macro_tree: STRtree, tol: float
) -> bool:
    """True if segment ``(x0, y0)-(x1, y1)`` runs alongside a macro line.

    Sampled at both endpoints and the midpoint (three points) against the
    nearest of ``macro_tree``'s lines; ``tol`` must hold at all three, not
    just one -- a segment that merely touches a macro line at one end (a
    genuine local street's access point) must not be mistaken for one that
    hugs it over its whole length.
    """
    for x, y in ((x0, y0), ((x0 + x1) / 2.0, (y0 + y1) / 2.0), (x1, y1)):
        idx_arr, dist_arr = macro_tree.query_nearest(
            Point(x, y), return_distance=True, all_matches=False
        )
        if idx_arr.size == 0 or float(dist_arr[0]) > tol:
            return False
    return True


def _add_polyline_edges(
    g: nx.Graph,
    line: LineString,
    *,
    skip_segment: Callable[[float, float, float, float], bool] | None = None,
    **edge_attrs: Any,
) -> None:
    """Add ``line``'s vertex chain to ``g`` as consecutive edges.

    Coincident endpoints (by :func:`_node_key`) fuse into one node regardless
    of which feature introduced them first; a node's ``x``/``y`` attrs are
    fixed at first insertion. Degenerate zero-length hops (two consecutive
    coordinates rounding to the same node) are skipped.

    ``skip_segment``, if given, is called with each segment's raw
    ``(x0, y0, x1, y1)`` *before* either endpoint is added as a node; a
    ``True`` return drops the segment entirely (no edge, and no node added
    on its account -- a node only ever enters ``g`` via a segment that is
    actually kept, so a fully-dropped line contributes nothing, not a chain
    of isolated points). Used by :func:`build_unified_street_graph` to dedup
    near-coincident local-street segments against arterial/ring lines (wave-2
    F2).
    """
    coords = list(line.coords)
    for (x0, y0), (x1, y1) in zip(coords, coords[1:], strict=False):
        u = _node_key(x0, y0)
        v = _node_key(x1, y1)
        if u == v:
            continue
        if skip_segment is not None and skip_segment(x0, y0, x1, y1):
            continue
        if u not in g:
            g.add_node(u, x=x0, y=y0)
        if v not in g:
            g.add_node(v, x=x1, y=y1)
        g.add_edge(u, v, length=math.hypot(x1 - x0, y1 - y0), **edge_attrs)


def build_unified_street_graph(
    arterial_lines: list[LineString],
    edges: list[MacroEdge],
    ring_lines: list[LineString],
    junctions: list[SeamJunction],
    streets_by_block: dict[int, list[LineString]],
    perimeter_flags_by_block: dict[int, list[bool]],
    *,
    dedup_tolerance: float = DEFAULT_LOCAL_DEDUP_TOLERANCE,
) -> nx.Graph:
    """Fuse arterials, rings, and per-block local streets into one graph.

    Arterial ``i`` (index-aligned with ``edges``, as in ``MacroLayer``) is cut
    at the stations of every ``"arterial"`` junction with ``macro_index ==
    i`` (:func:`split_line_at_stations`); each resulting sub-segment becomes a
    chain of ``kind="arterial", tier=edges[i].tier`` edges. Ring ``i`` is cut
    the same way against ``"ring"`` junctions, tagged ``kind="ring",
    tier=-1``. For each block (iterated in sorted ``block_id`` order for
    determinism), every street in ``streets_by_block[block_id]`` whose
    index-aligned ``perimeter_flags_by_block[block_id]`` flag is ``False``
    becomes a chain of ``kind="local", tier=-1, block_id=block_id`` edges;
    perimeter-flagged paths are dropped (they duplicate the arterial/ring/
    coast geometry they run along, not new topology).

    ``perimeter_flags_by_block`` classification is per *street*, not per
    edge (see :func:`_all_nodes_on_boundary`): if it misses a
    boundary-hugging street (one drifted node, see the stage-3 preamble
    above), that street's segments still pass through here as "local". Each
    local segment is therefore also checked against ``dedup_tolerance``: one
    whose both endpoints and midpoint sit within ``dedup_tolerance`` of an
    arterial/ring line is dropped (see :func:`_segment_hugs_macro`), so a
    misclassified boundary-hugging street does not surface as a doubled,
    near-coincident road alongside the arterial/ring it already duplicates.
    ``dedup_tolerance <= 0`` (or no arterial/ring lines at all) disables this
    check entirely.

    Node identity is :func:`_node_key`: a junction's gate coordinate and the
    station-projected point on its matched arterial/ring line round to the
    same key, so they fuse into one node — this is what turns independently
    generated block street networks and the macro network into a single
    connected graph with real (degree >= 3) T-junctions, not a disjoint union.
    ``tol`` in :func:`snap_gates_to_macro` means that coordinate identity is
    not automatic: a gate snapped at a nonzero (but within-``tol``) distance
    has a raw coordinate that does NOT round to the same :func:`_node_key` as
    the on-line point the arterial/ring was actually cut at. Before adding a
    block's local streets, every coordinate matching a snapped ``"arterial"``
    or ``"ring"`` junction's raw ``(x, y)`` (by ``_node_key``, scoped to that
    junction's ``block_id``) is replaced with the exact on-line point
    (``line.interpolate(station)``) so fusion holds across the whole ``tol``
    band, not just the (typical, ~0-distance) exact-coincidence case.

    Every edge also carries a ``length`` attr (planar distance between its
    two endpoints) for later weighting/summary work. Returns a plain
    ``nx.Graph`` (not a MultiGraph); a duplicate node-pair edge from
    overlapping input geometry simply overwrites the earlier one.
    """
    if len(arterial_lines) != len(edges):
        raise ValueError("arterial_lines and edges must be index-aligned")

    stations_by_arterial: dict[int, list[float]] = {}
    stations_by_ring: dict[int, list[float]] = {}
    # Raw gate coordinate -> exact on-line point, scoped per block, for the
    # off-line-but-within-tol snap fusion fix (see docstring above). Junctions
    # are processed in (block_id, x, y) order regardless of input order so a
    # (practically impossible) raw-coordinate collision within one block still
    # resolves deterministically.
    snap_targets_by_block: dict[int, dict[tuple[int, int], tuple[float, float]]] = {}
    for junction in sorted(junctions, key=lambda j: (j.block_id, j.x, j.y)):
        if junction.kind == "arterial":
            stations_by_arterial.setdefault(junction.macro_index, []).append(
                junction.station
            )
            line = arterial_lines[junction.macro_index]
        elif junction.kind == "ring":
            stations_by_ring.setdefault(junction.macro_index, []).append(
                junction.station
            )
            line = ring_lines[junction.macro_index]
        else:
            continue
        snapped = line.interpolate(junction.station)
        snap_targets_by_block.setdefault(junction.block_id, {})[
            _node_key(junction.x, junction.y)
        ] = (snapped.x, snapped.y)

    g: nx.Graph = nx.Graph()

    for i, line in enumerate(arterial_lines):
        stations = stations_by_arterial.get(i, [])
        for segment in split_line_at_stations(line, stations):
            _add_polyline_edges(
                g, segment, kind="arterial", tier=edges[i].tier, block_id=-1
            )

    for i, line in enumerate(ring_lines):
        stations = stations_by_ring.get(i, [])
        for segment in split_line_at_stations(line, stations):
            _add_polyline_edges(g, segment, kind="ring", tier=-1, block_id=-1)

    macro_lines: list[LineString] = [*arterial_lines, *ring_lines]
    macro_tree = STRtree(macro_lines) if macro_lines and dedup_tolerance > 0.0 else None
    skip_segment: Callable[[float, float, float, float], bool] | None = None
    if macro_tree is not None:
        _macro_tree = macro_tree

        def skip_segment(x0: float, y0: float, x1: float, y1: float) -> bool:
            return _segment_hugs_macro(x0, y0, x1, y1, _macro_tree, dedup_tolerance)

    for block_id in sorted(streets_by_block):
        streets = streets_by_block[block_id]
        flags = perimeter_flags_by_block.get(block_id, [False] * len(streets))
        if len(flags) != len(streets):
            raise ValueError(f"perimeter flags misaligned for block {block_id}")
        block_snap_targets = snap_targets_by_block.get(block_id, {})
        for line, is_perimeter in zip(streets, flags, strict=True):
            if is_perimeter:
                continue
            if block_snap_targets:
                coords = [
                    block_snap_targets.get(_node_key(x, y), (x, y))
                    for x, y in line.coords
                ]
                if coords != list(line.coords):
                    line = LineString(coords)
            _add_polyline_edges(
                g,
                line,
                kind="local",
                tier=-1,
                block_id=block_id,
                skip_segment=skip_segment,
            )

    return g


# ---------------------------------------------------------------------------
# Stage 4 (assembly-only) — densify sparse-gate boundary runs.
# ---------------------------------------------------------------------------
#
# Slices 1-3 above extract and snap gates that Chen actually produced. On
# real islands, many blocks produce very few gates because Chen's boundary
# ring is itself a free level-0 street (see module docstring): parcels
# touching the block edge never need a perpendicular access to be
# "reachable". :func:`densify_gates` is an optional, assembly-layer fix —
# it reads an already-built layout and manufactures additional connector
# streets/gates along Chen's own parcel corner graph. It does not change
# Chen generation, chen_in_block's signature, or r1_seam.


def _cyclic_distance(a: float, b: float, perimeter: float) -> float:
    """Shorter of the two arc-length distances between ``a``/``b`` on a ring."""
    if perimeter <= 0.0:
        return abs(a - b)
    d = abs(a - b) % perimeter
    return min(d, perimeter - d)


def _cyclic_boundary_runs(
    perimeter: float, stations: list[float]
) -> list[tuple[float, float]]:
    """Gap intervals along a closed boundary ring (station space wraps).

    ``stations`` need not be sorted or de-duplicated. Returns ``(start, end)``
    pairs in ascending ``start`` order; the wraparound run (crossing the
    ring's start/end seam) is listed last with ``end`` possibly exceeding
    ``perimeter``, so ``end - start`` is always its true length regardless of
    the wrap. No stations (or a degenerate ``perimeter <= 0``) yields a
    single run spanning the whole ring (``[]`` for the degenerate case).
    """
    if perimeter <= 0.0:
        return []
    unique = sorted({min(max(s, 0.0), perimeter) for s in stations})
    if not unique:
        return [(0.0, perimeter)]
    runs = [(a, b) for a, b in zip(unique, unique[1:], strict=False) if b > a]
    wrap_start, wrap_end = unique[-1], unique[0] + perimeter
    if wrap_end > wrap_start:
        runs.append((wrap_start, wrap_end))
    return runs


def densify_gates(
    layout: ChenLayout,
    block_boundary: Polygon,
    existing_gates: list[Gate],
    *,
    max_gate_spacing: float,
    min_connector_frac: float = 0.0,
) -> tuple[list[LineString], list[Gate]]:
    """Route connector streets into under-served stretches of a block boundary.

    Chen seeds the block perimeter itself as a level-0 street (see this
    module's docstring), so boundary-adjacent parcels are "reachable" for
    free and Chen rarely grows a perpendicular access all the way to the
    edge -- many blocks end up with long stretches of boundary that never
    produced an :func:`extract_block_gates` gate. This finds those
    stretches and manufactures a connector: a shortest path, over Chen's own
    parcel corner graph (``layout.corner_graph``, real parcel edges only --
    no invented geometry), from a boundary vertex near the stretch's
    midpoint to the nearest interior local-street node.

    A "boundary run" is a gap between consecutive ``existing_gates`` stations
    projected onto ``block_boundary.exterior`` (arc-length, wrapping — the
    boundary is a closed ring, see :func:`_cyclic_boundary_runs`); no gates
    at all means the whole perimeter is one run. Only runs longer than
    ``max_gate_spacing`` get a connector.

    For each such run, the boundary corner-graph vertex nearest the run's
    midpoint (by arc-length station, not straight-line distance -- see
    :func:`_cyclic_distance`) is the connector's boundary endpoint; Dijkstra
    over ``layout.corner_graph.edges`` (Euclidean edge weights) finds the
    nearest node that is both a ``layout.street_graph`` node and NOT
    ``on_boundary`` (a genuine interior street, not another boundary-ring
    point). If no interior street node is reachable at all in this block
    (e.g. a fallback/near-empty block with no interior fabric), every run is
    skipped -- this never invents geometry with no real target.

    ``min_connector_frac``: skip an individual run's connector if the routed
    path length is below ``min_connector_frac * max_gate_spacing`` (a
    near-zero connector -- the nearest interior node already sits right on
    the boundary -- adds a T-junction but essentially no new street).
    ``0.0`` (the default) never filters on path length.

    Returns ``(connectors, gates)``: parallel lists (one connector + one gate
    per satisfied run), both empty if nothing qualifies. Each returned
    :class:`Gate` uses a synthetic negative ``street_id`` (distinct per
    connector) so callers can tell it apart from a real Chen street endpoint;
    its ``vertex_id`` is the boundary corner-graph vertex the connector
    starts from. Pure and deterministic: runs are processed in ascending
    boundary-station order (with the wraparound run last) and every
    tie-break (nearest boundary vertex, Dijkstra's own tie handling) falls
    back to vertex id or is otherwise structurally determined.
    """
    ring = LineString(block_boundary.exterior.coords)
    perimeter = float(ring.length)
    if perimeter <= 0.0:
        return [], []

    gate_stations = sorted(
        {float(ring.project(Point(gate.x, gate.y))) for gate in existing_gates}
    )
    runs = _cyclic_boundary_runs(perimeter, gate_stations)
    under_served = [(a, b) for a, b in runs if (b - a) > max_gate_spacing]
    if not under_served:
        return [], []

    corner_vertices = layout.corner_graph.vertices
    g: nx.Graph = nx.Graph()
    for u, v in layout.corner_graph.edges:
        pu = corner_vertices[u].point
        pv = corner_vertices[v].point
        g.add_edge(u, v, weight=math.hypot(pv[0] - pu[0], pv[1] - pu[1]))

    boundary_candidates: list[tuple[int, float]] = sorted(
        (vid, float(ring.project(Point(vertex.point))))
        for vid, vertex in corner_vertices.items()
        if vertex.on_boundary and vid in g
    )
    if not boundary_candidates:
        return [], []

    street_node_ids = {
        node for street in layout.street_graph.streets for node in street.nodes
    }
    interior_targets = {
        vid
        for vid in street_node_ids
        if vid in g and vid in corner_vertices and not corner_vertices[vid].on_boundary
    }
    if not interior_targets:
        return [], []

    connectors: list[LineString] = []
    new_gates: list[Gate] = []
    connector_id = 0
    for start, end in under_served:
        midpoint = ((start + end) / 2.0) % perimeter
        boundary_vertex_id = min(
            boundary_candidates,
            key=lambda vs: (_cyclic_distance(vs[1], midpoint, perimeter), vs[0]),
        )[0]

        try:
            _length, path = nx.multi_source_dijkstra(
                g,
                sources=interior_targets,
                target=boundary_vertex_id,
                weight="weight",
            )
        except nx.NetworkXNoPath:
            continue

        path_points = [corner_vertices[vid].point for vid in reversed(path)]
        path_length = sum(
            math.hypot(x1 - x0, y1 - y0)
            for (x0, y0), (x1, y1) in zip(path_points, path_points[1:], strict=False)
        )
        if path_length < min_connector_frac * max_gate_spacing:
            continue

        connector_id -= 1
        connectors.append(LineString(path_points))
        boundary_point = corner_vertices[boundary_vertex_id].point
        new_gates.append(
            Gate(
                x=boundary_point[0],
                y=boundary_point[1],
                street_id=connector_id,
                vertex_id=boundary_vertex_id,
            )
        )

    return connectors, new_gates


def graph_connectivity_summary(g: nx.Graph) -> dict[str, Any]:
    """Summarize the unified street graph as a plain JSON-serializable dict.

    ``largest_component_local_length_fraction`` is the largest connected
    component's share of total ``kind="local"`` edge length (by
    ``sum(length)``), not node/edge count — the metric this wave cares about
    is "how much of the *local street fabric* ended up plugged into the same
    component", not raw graph size. A T-junction is "realized" when a node is
    incident to at least one arterial-or-ring edge AND at least one local
    edge — that is the graph-level confirmation that a stage-2 junction
    actually produced connected topology, not just a coincident point.
    """
    n_nodes = g.number_of_nodes()
    n_edges = g.number_of_edges()
    components = list(nx.connected_components(g))
    n_components = len(components)

    total_local_length = sum(
        data["length"]
        for _u, _v, data in g.edges(data=True)
        if data.get("kind") == "local"
    )
    if components and total_local_length > 0.0:
        largest = max(components, key=len)
        largest_local_length = sum(
            data["length"]
            for u, v, data in g.edges(data=True)
            if data.get("kind") == "local" and u in largest and v in largest
        )
        largest_component_local_fraction = largest_local_length / total_local_length
    else:
        largest_component_local_fraction = 0.0

    n_tjunctions_realized = 0
    for node in g.nodes:
        kinds = {data.get("kind") for data in g.adj[node].values()}
        has_macro = "arterial" in kinds or "ring" in kinds
        has_local = "local" in kinds
        if has_macro and has_local:
            n_tjunctions_realized += 1

    return {
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "n_components": n_components,
        "largest_component_local_length_fraction": round(
            largest_component_local_fraction, 6
        ),
        "n_tjunctions_realized": n_tjunctions_realized,
    }
