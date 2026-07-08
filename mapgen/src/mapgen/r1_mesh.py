"""R1 stage G1 — greybox mesh bake: G0 export geometry -> OBJ/MTL triangles.

Pure geometry module (``docs/greybox-plan.md`` Stage G1): turns the Stage-G0
export's shapely polygons/lines (:mod:`mapgen.r1_lots`, ``run_r1_hybrid.py``
``--greybox-out``) into an in-memory triangle :class:`Mesh` plus OBJ/MTL text,
grouped and materialed by semantic kind (buildings per macro-block, streets
per tier, ground, parks, water). No IO, no parquet/GeoJSON parsing --
``scripts/run_r1_greybox_mesh.py`` reads the G0 export, decodes WKB/GeoJSON
into the small record types below, and writes the files this module returns
strings/dicts for.

Coordinate convention: island-frame x/y (NOT meters) are scaled by
``meters_per_unit`` into a Y-up meters frame -- island x -> OBJ x, island y ->
OBJ z, up -> OBJ y. ``LotRecord.height`` is ALREADY meters (see
``mapgen.r1_lots`` module docstring) and is never scaled. Unity's OBJ importer
flips handedness itself; this module does not pre-flip anything.

Winding/normals: cap triangles (building roofs/floors, ground top/bottom,
street/park/water pads) are oriented by directly computing each candidate
triangle's Y-normal component and flipping if it doesn't match the wanted
direction (:func:`_oriented_face`) -- this sidesteps depending on
``trimesh.creation.triangulate_polygon``'s internal winding convention. Wall
triangles are built from :func:`shapely.geometry.polygon.orient`-normalized
rings (exterior CCW, holes CW), which was verified (see
``tests/test_r1_mesh.py``) to produce outward-facing normals for both a solid's
outer boundary and its holes with one fixed triangle-index formula.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import shapely.geometry as sg
import trimesh.creation as _trimesh_creation
from shapely.geometry.polygon import orient as _orient

# ---------------------------------------------------------------------------
# Defaults (docs/greybox-plan.md Stage G1)
# ---------------------------------------------------------------------------

DEFAULT_METERS_PER_UNIT: float = 25.0

# Street ribbon widths, ISLAND UNITS (pre-``meters_per_unit`` scaling), keyed
# by the tier names ``run_r1_hybrid.py``'s greybox export uses (arterials'
# "highway"/"major"/"local"/"ring"; Chen per-block local streets have no tier
# property in ``streets.geojson`` -- callers assign them tier ``"street"``).
DEFAULT_STREET_WIDTHS: dict[str, float] = {
    "highway": 1.0,
    "major": 0.7,
    "local": 0.5,
    "ring": 0.6,
    "street": 0.25,
}

DEFAULT_STREET_Y: float = 0.3
DEFAULT_PARK_Y: float = 0.15
DEFAULT_GROUND_DEPTH: float = 2.0
DEFAULT_WATER_Y: float = -0.5
DEFAULT_WATER_MARGIN_FRAC: float = 0.3

# Flat greybox diffuse colors (Kd, 0..1), one per material name this module
# emits. Roads get a tier-differentiated grey (higher tier = darker/wider).
DEFAULT_MATERIAL_COLORS: dict[str, tuple[float, float, float]] = {
    "building": (0.75, 0.75, 0.75),
    "road_highway": (0.12, 0.12, 0.12),
    "road_major": (0.22, 0.22, 0.22),
    "road_local": (0.32, 0.32, 0.32),
    "road_ring": (0.28, 0.28, 0.34),
    "road_street": (0.40, 0.40, 0.40),
    "ground": (0.55, 0.50, 0.35),
    "park": (0.25, 0.55, 0.25),
    "water": (0.20, 0.35, 0.65),
}

# Deterministic weld tolerance for near-duplicate 2D triangulation vertices
# (matches the rounding convention ``mapgen.r1_lots._jitter_duplicates`` uses).
_WELD_DECIMALS: int = 9


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SurfaceMesh:
    """Local-indexed triangle soup in Y-up meters, not yet placed in a group."""

    vertices: tuple[tuple[float, float, float], ...]
    faces: tuple[tuple[int, int, int], ...]


@dataclass(frozen=True)
class MeshGroup:
    """One OBJ ``g``/``usemtl`` group: a self-contained, locally-indexed mesh."""

    name: str
    material: str
    vertices: tuple[tuple[float, float, float], ...]
    faces: tuple[tuple[int, int, int], ...]


@dataclass(frozen=True)
class Mesh:
    """A full bake: groups in OBJ emission order."""

    groups: tuple[MeshGroup, ...]


@dataclass(frozen=True)
class LotRecord:
    """One building input: a G0 lot decoded into a shapely footprint.

    ``footprint``/``x``/``y`` are ISLAND UNITS; ``height`` is already meters
    (``mapgen.r1_lots`` convention). ``block_id`` is the macro-block a
    building's district belongs to (``districts.geojson`` ``block_id``),
    which decides which ``block_<id>_buildings`` group it lands in.
    """

    world_id: str
    block_id: int
    footprint: sg.Polygon
    height: float
    name: str | None
    x: float
    y: float


@dataclass(frozen=True)
class RoadSegment:
    """One road centerline to buffer into a ribbon: a tier + a LineString."""

    geometry: sg.LineString
    tier: str


@dataclass(frozen=True)
class BuildingStats:
    n_ok: int
    n_skipped: int
    n_holes_simplified: int


@dataclass(frozen=True)
class RoadStats:
    n_ok: int
    n_skipped: int


@dataclass(frozen=True)
class ParkStats:
    n_ok: int
    n_skipped: int


@dataclass(frozen=True)
class BakeStats:
    """Degenerate-input counters across every group (the G1 crash-avoidance
    contract: empty/invalid geometry is skipped and counted, never raised)."""

    buildings: BuildingStats
    roads: RoadStats
    parks: ParkStats
    ground_holes_simplified: bool


class _GroupBuilder:
    """Mutable vertex/face accumulator local to one group's assembly loop.

    Internal helper only -- every public function in this module remains a
    pure function of its arguments; this just avoids re-deriving vertex-offset
    bookkeeping at every call site that merges several :class:`SurfaceMesh`
    into one group.
    """

    def __init__(self) -> None:
        self.vertices: list[tuple[float, float, float]] = []
        self.faces: list[tuple[int, int, int]] = []

    def add(self, mesh: SurfaceMesh) -> None:
        offset = len(self.vertices)
        self.vertices.extend(mesh.vertices)
        self.faces.extend(
            (a + offset, b + offset, c + offset) for a, b, c in mesh.faces
        )


# ---------------------------------------------------------------------------
# Polygon cleanup + triangulation
# ---------------------------------------------------------------------------


def _largest_polygon(geom: sg.base.BaseGeometry) -> sg.Polygon | None:
    """Return ``geom``'s largest ``Polygon`` part, or ``None`` if it has none."""
    if geom.is_empty:
        return None
    if geom.geom_type == "Polygon":
        return geom
    if geom.geom_type in ("MultiPolygon", "GeometryCollection"):
        parts = [g for g in geom.geoms if g.geom_type == "Polygon" and not g.is_empty]
        if parts:
            return max(parts, key=lambda p: p.area)
    return None


def _clean_polygon(polygon: sg.Polygon | None) -> sg.Polygon | None:
    """Fix minor invalidity (self-intersection) and reject degenerate input.

    Returns ``None`` for ``None``/empty/still-invalid-after-buffer(0)/zero-area
    polygons -- the uniform "skip and count" signal every builder below uses.
    """
    if polygon is None or polygon.is_empty:
        return None
    poly = polygon
    if not poly.is_valid:
        poly = poly.buffer(0)
    cleaned = _largest_polygon(poly)
    if cleaned is None or cleaned.is_empty or cleaned.area <= 0.0:
        return None
    return cleaned


def _weld_2d_vertices(
    verts2d: np.ndarray,
) -> tuple[list[tuple[float, float]], list[int]]:
    """Merge near-duplicate 2D vertices (``_WELD_DECIMALS`` rounding).

    ``trimesh.creation.triangulate_polygon`` repeats each input ring's closing
    coordinate as an extra (otherwise-unreferenced) vertex; welding keeps
    vertex/face counts minimal and deterministic instead of depending on that
    incidental behavior. Returns ``(unique_verts, remap)`` where ``remap[i]``
    is the welded index for original row ``i``.
    """
    seen: dict[tuple[float, float], int] = {}
    unique: list[tuple[float, float]] = []
    remap: list[int] = []
    for x, y in verts2d:
        key = (round(float(x), _WELD_DECIMALS), round(float(y), _WELD_DECIMALS))
        idx = seen.get(key)
        if idx is None:
            idx = len(unique)
            seen[key] = idx
            unique.append((float(x), float(y)))
        remap.append(idx)
    return unique, remap


def triangulate_with_holes(
    polygon: sg.Polygon,
) -> tuple[list[tuple[float, float]], list[tuple[int, int, int]], bool]:
    """Triangulate ``polygon`` (interiors supported) for a flat cap mesh.

    Returns ``(verts2d, faces, holes_simplified)``. Empty/invalid/zero-area
    input returns ``([], [], False)``. If triangulating with holes fails (or
    yields no triangles for nonzero area), retries on the exterior ring alone
    and reports ``holes_simplified=True`` -- the "count what was simplified"
    degenerate-hole path from ``docs/greybox-plan.md``.
    """
    poly = _clean_polygon(polygon)
    if poly is None:
        return [], [], False

    holes_simplified = False
    try:
        verts2d, tris = _trimesh_creation.triangulate_polygon(poly, engine="earcut")
        if len(tris) == 0:
            raise ValueError("earcut produced no triangles for nonzero-area polygon")
    except Exception:
        if not poly.interiors:
            return [], [], False
        holes_simplified = True
        exterior_only = sg.Polygon(poly.exterior)
        try:
            verts2d, tris = _trimesh_creation.triangulate_polygon(
                exterior_only, engine="earcut"
            )
        except Exception:
            return [], [], True
        if len(tris) == 0:
            return [], [], True

    unique, remap = _weld_2d_vertices(verts2d)
    faces = [
        (remap[a], remap[b], remap[c])
        for a, b, c in tris
        if len({remap[a], remap[b], remap[c]}) == 3
    ]
    if not faces:
        return [], [], holes_simplified
    return unique, faces, holes_simplified


# ---------------------------------------------------------------------------
# Face orientation
# ---------------------------------------------------------------------------


def _normal_y(
    pa: tuple[float, float, float],
    pb: tuple[float, float, float],
    pc: tuple[float, float, float],
) -> float:
    """Y-component of ``cross(pb - pa, pc - pa)``.

    For any triangle lying in a constant-Y plane (both cap layers do) this is
    the ONLY nonzero component, so its sign alone tells which way the cap
    faces -- positive means the triangle's ``(pa, pb, pc)`` winding faces up.
    """
    v1 = (pb[0] - pa[0], pb[1] - pa[1], pb[2] - pa[2])
    v2 = (pc[0] - pa[0], pc[1] - pa[1], pc[2] - pa[2])
    return v1[2] * v2[0] - v1[0] * v2[2]


def _oriented_face(
    level_verts: list[tuple[float, float, float]],
    tri: tuple[int, int, int],
    *,
    want_up: bool,
    offset: int,
) -> tuple[int, int, int]:
    """``tri`` (local indices into ``level_verts``), flipped if its winding
    doesn't already face the wanted direction, offset into a shared pool."""
    a, b, c = tri
    ny = _normal_y(level_verts[a], level_verts[b], level_verts[c])
    if (want_up and ny < 0.0) or (not want_up and ny > 0.0):
        b, c = c, b
    return (offset + a, offset + b, offset + c)


def _oriented_rings(polygon: sg.Polygon) -> list[list[tuple[float, float]]]:
    """Exterior (CCW) then each hole (CW) ring, closing point dropped.

    ``shapely.geometry.polygon.orient(sign=1.0)`` normalizes winding; combined
    with the fixed wall-triangle formula in :func:`extrude_solid`, this gives
    outward-facing normals for both the outer boundary and every hole (see
    ``tests/test_r1_mesh.py`` for the verified derivation).
    """
    poly = _orient(polygon, sign=1.0)
    rings = [list(poly.exterior.coords)[:-1]]
    for interior in poly.interiors:
        rings.append(list(interior.coords)[:-1])
    return rings


# ---------------------------------------------------------------------------
# Solid (prism) + flat cap builders
# ---------------------------------------------------------------------------


def extrude_solid(
    polygon: sg.Polygon, y_bottom: float, y_top: float, meters_per_unit: float
) -> tuple[SurfaceMesh | None, bool]:
    """Extrude ``polygon`` (island units) into a closed Y-up solid (meters).

    ``y_bottom``/``y_top`` are ALREADY meters (never scaled by
    ``meters_per_unit``, which only scales the planar x/y). Builds a bottom
    cap (outward normal -Y), a top cap (+Y), and one wall quad (2 triangles)
    per ring edge (exterior + every hole), each contributing its own
    unwelded-across-layers vertices (deliberate: hard edges at every cap/wall
    seam read correctly as flat-shaded box geometry). Returns
    ``(None, holes_simplified)`` for empty/invalid/zero-area input -- the
    caller counts the skip.
    """
    verts2d, cap_faces, holes_simplified = triangulate_with_holes(polygon)
    if not verts2d or not cap_faces:
        return None, holes_simplified

    verts: list[tuple[float, float, float]] = []
    faces: list[tuple[int, int, int]] = []

    bottom_verts = [
        (x * meters_per_unit, y_bottom, y * meters_per_unit) for x, y in verts2d
    ]
    b_off = len(verts)
    verts.extend(bottom_verts)
    for tri in cap_faces:
        faces.append(_oriented_face(bottom_verts, tri, want_up=False, offset=b_off))

    top_verts = [(x * meters_per_unit, y_top, y * meters_per_unit) for x, y in verts2d]
    t_off = len(verts)
    verts.extend(top_verts)
    for tri in cap_faces:
        faces.append(_oriented_face(top_verts, tri, want_up=True, offset=t_off))

    for ring in _oriented_rings(polygon):
        m = len(ring)
        if m < 2:
            continue
        ring_bottom = [
            (x * meters_per_unit, y_bottom, y * meters_per_unit) for x, y in ring
        ]
        ring_top = [(x * meters_per_unit, y_top, y * meters_per_unit) for x, y in ring]
        w_off = len(verts)
        verts.extend(ring_bottom)
        verts.extend(ring_top)
        for i in range(m):
            j = (i + 1) % m
            a_b, b_b = w_off + i, w_off + j
            a_t, b_t = w_off + m + i, w_off + m + j
            # Derived + verified (tests/test_r1_mesh.py) for CCW exterior /
            # CW hole rings from _oriented_rings: outward-facing normals.
            faces.append((a_b, b_t, b_b))
            faces.append((a_b, a_t, b_t))

    return SurfaceMesh(vertices=tuple(verts), faces=tuple(faces)), holes_simplified


def flat_cap(
    polygon: sg.Polygon, y: float, meters_per_unit: float
) -> tuple[SurfaceMesh | None, bool]:
    """A single upward-facing (+Y) triangle layer at constant ``y`` (meters).

    Used for streets/parks/water: a flat pad, not a solid. Returns
    ``(None, holes_simplified)`` for empty/invalid/zero-area input.
    """
    verts2d, cap_faces, holes_simplified = triangulate_with_holes(polygon)
    if not verts2d or not cap_faces:
        return None, holes_simplified
    level_verts = [(x * meters_per_unit, y, yy * meters_per_unit) for x, yy in verts2d]
    faces = [
        _oriented_face(level_verts, tri, want_up=True, offset=0) for tri in cap_faces
    ]
    return SurfaceMesh(
        vertices=tuple(level_verts), faces=tuple(faces)
    ), holes_simplified


def buffer_ribbon(line: sg.LineString, width_units: float) -> sg.Polygon | None:
    """Buffer ``line`` (island units) into a flat ribbon polygon.

    ``width_units`` is the FULL ribbon width (buffered by half on each side).
    Round joins/caps (shapely's buffer default) -- simplest choice for a
    greybox tracer bullet: overlapping round caps at junctions never leave a
    gap, unlike flat caps on centerlines that don't meet exactly. Returns
    ``None`` for an empty line or non-positive width (caller counts the skip).
    """
    if line is None or line.is_empty or width_units <= 0.0:
        return None
    return _clean_polygon(line.buffer(width_units / 2.0))


# ---------------------------------------------------------------------------
# Centerline fillet (S7c mesh-layer corner rounding,
# docs/macro-roads-nuclei-plan.md)
#
# The promoted-highway/ring-tier road network reads rigid at ring<->arterial
# junctions -- long DP-simplified chords meeting at abrupt kinks, the same
# jaggedness S1 fixed at the SOURCE for arterials/rings inside
# ``r1_macro.build_macro_layer``. This is a separate, mesh/export-LAYER-only
# fillet: it smooths the EXPORTED copy of a road centerline (feeding
# ``arterials.geojson`` and, through it, the OBJ road ribbons and the
# building-clip ribbon union) without touching ``r1_macro``'s source
# geometry -- the macro-blocks are already polygonized from the pre-fillet
# centerlines, so re-deriving them here would desync roads from blocks (the
# same lesson S1's docstring states, just applied in the opposite direction:
# fillet the export copy, never re-derive from it).
#
# Mirrors ``r1_macro.smooth_polyline``'s endpoint-preserving open-chain
# Chaikin pass (``_chaikin_pass_open``) plus a closed-loop variant for whole
# rings (``_chaikin_pass_closed``) -- duplicated rather than imported to keep
# this module's dependency footprint minimal (numpy/shapely/trimesh only, no
# r1_macro/networkx/scipy/skimage), matching this module's "pure geometry,
# decoupled from the macro pipeline" contract (see the module docstring).
# ---------------------------------------------------------------------------

DEFAULT_FILLET_ITERATIONS: int = 2
# Island units; matches r1_macro.DEFAULT_SMOOTH_POST_TOL (same rationale:
# bound the ~2x-per-pass vertex growth Chaikin adds back down).
DEFAULT_FILLET_POST_TOL: float = 0.15


def _fillet_pass_open(
    coords: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    """One Chaikin corner-cutting pass over an OPEN coordinate chain.

    Preserves the first and last coordinate EXACTLY (only interior corners
    are cut) -- mirrors ``mapgen.r1_macro._chaikin_pass_open``.
    """
    n = len(coords)
    if n < 3:
        return list(coords)
    out: list[tuple[float, float]] = [coords[0]]
    last_edge = n - 2
    for i in range(n - 1):
        (x0, y0), (x1, y1) = coords[i], coords[i + 1]
        q = (0.75 * x0 + 0.25 * x1, 0.75 * y0 + 0.25 * y1)
        r = (0.25 * x0 + 0.75 * x1, 0.25 * y0 + 0.75 * y1)
        if i == 0:
            out.append(r)
        elif i == last_edge:
            out.append(q)
        else:
            out.append(q)
            out.append(r)
    out.append(coords[-1])
    return out


def _fillet_pass_closed(
    coords: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    """One Chaikin corner-cutting pass over a CLOSED ring coordinate chain.

    ``coords`` is a closed ring (``coords[0] == coords[-1]``). Mirrors
    ``mapgen.r1_macro._chaikin_pass_closed``.
    """
    ring = coords[:-1] if coords[0] == coords[-1] else list(coords)
    n = len(ring)
    if n < 3:
        return list(coords)
    out: list[tuple[float, float]] = []
    for i in range(n):
        (x0, y0), (x1, y1) = ring[i], ring[(i + 1) % n]
        out.append((0.75 * x0 + 0.25 * x1, 0.75 * y0 + 0.25 * y1))
        out.append((0.25 * x0 + 0.75 * x1, 0.25 * y0 + 0.75 * y1))
    out.append(out[0])
    return out


def fillet_centerline(
    line: sg.LineString,
    *,
    iterations: int = DEFAULT_FILLET_ITERATIONS,
    post_tol: float = DEFAULT_FILLET_POST_TOL,
) -> sg.LineString:
    """Chaikin-smooth one road centerline for mesh/export emission (S7c).

    Auto-detects OPEN vs CLOSED: a ring (``line.coords[0] ==
    line.coords[-1]``, e.g. a whole core ring) is smoothed with the
    endpoint-free closed pass (:func:`_fillet_pass_closed`); an open chain
    (an arterial, or a ring-arc segment cut at T-junction stations) is
    smoothed with the endpoint-PRESERVED open pass (:func:`_fillet_pass_open`)
    so shared junction points -- where a ring arc meets an arterial, or two
    arterial segments meet at a macro-node -- stay bit-for-bit coincident.
    Callers that buffer these lines into a ribbon union (the S7c building
    clip) rely on that coincidence never opening a gap. Degenerate lines
    (fewer than 3 coordinates) or ``iterations <= 0`` pass through UNCHANGED.
    """
    coords = list(line.coords)
    if len(coords) < 3 or iterations <= 0:
        return line
    closed = coords[0] == coords[-1]
    pts = coords
    for _ in range(iterations):
        pts = _fillet_pass_closed(pts) if closed else _fillet_pass_open(pts)
    smoothed = sg.LineString(pts)
    if post_tol > 0:
        smoothed = smoothed.simplify(post_tol, preserve_topology=False)
    out_coords = list(smoothed.coords)
    if closed:
        # simplify() always keeps a chain's first/last vertex, but pin
        # defensively (mirrors smooth_polyline's open-line pin) in case of
        # any floating-point drift.
        if out_coords[0] != out_coords[-1]:
            out_coords[-1] = out_coords[0]
            smoothed = sg.LineString(out_coords)
    elif out_coords[0] != coords[0] or out_coords[-1] != coords[-1]:
        out_coords[0] = coords[0]
        out_coords[-1] = coords[-1]
        smoothed = sg.LineString(out_coords)
    return smoothed


# ---------------------------------------------------------------------------
# Group builders
# ---------------------------------------------------------------------------


def build_building_groups(
    lots: list[LotRecord], meters_per_unit: float
) -> tuple[list[MeshGroup], BuildingStats]:
    """One ``block_<id>_buildings`` / ``building`` group per macro-block.

    ``lots`` order is not relied on for determinism: grouped by
    ``block_id`` (sorted ascending) then by ``world_id`` (sorted ascending)
    within a block. A block whose every lot is degenerate is omitted (no
    empty group emitted); its skip count still lands in the returned stats.
    """
    by_block: dict[int, list[LotRecord]] = {}
    for lot in lots:
        by_block.setdefault(lot.block_id, []).append(lot)

    groups: list[MeshGroup] = []
    n_ok = 0
    n_skipped = 0
    n_holes_simplified = 0
    for block_id in sorted(by_block):
        block_lots = sorted(by_block[block_id], key=lambda lot: lot.world_id)
        gb = _GroupBuilder()
        for lot in block_lots:
            mesh, holes_simplified = extrude_solid(
                lot.footprint, 0.0, lot.height, meters_per_unit
            )
            if holes_simplified:
                n_holes_simplified += 1
            if mesh is None:
                n_skipped += 1
                continue
            gb.add(mesh)
            n_ok += 1
        if gb.faces:
            groups.append(
                MeshGroup(
                    name=f"block_{block_id:03d}_buildings",
                    material="building",
                    vertices=tuple(gb.vertices),
                    faces=tuple(gb.faces),
                )
            )
    return groups, BuildingStats(
        n_ok=n_ok, n_skipped=n_skipped, n_holes_simplified=n_holes_simplified
    )


def build_road_groups(
    segments: list[RoadSegment],
    widths: dict[str, float],
    meters_per_unit: float,
    y: float,
) -> tuple[list[MeshGroup], RoadStats]:
    """One ``roads_<tier>`` / ``road_<tier>`` group per tier, alphabetical.

    A tier missing from ``widths`` (or with a non-positive width) skips every
    one of its segments (counted, never raised). A tier whose every segment
    is degenerate is omitted from the output groups.
    """
    by_tier: dict[str, list[RoadSegment]] = {}
    for seg in segments:
        by_tier.setdefault(seg.tier, []).append(seg)

    groups: list[MeshGroup] = []
    n_ok = 0
    n_skipped = 0
    for tier in sorted(by_tier):
        width = widths.get(tier)
        segs = by_tier[tier]
        gb = _GroupBuilder()
        if width is None or width <= 0.0:
            n_skipped += len(segs)
        else:
            for seg in segs:
                ribbon = buffer_ribbon(seg.geometry, width)
                if ribbon is None:
                    n_skipped += 1
                    continue
                mesh, _holes_simplified = flat_cap(ribbon, y, meters_per_unit)
                if mesh is None:
                    n_skipped += 1
                    continue
                gb.add(mesh)
                n_ok += 1
        if gb.faces:
            groups.append(
                MeshGroup(
                    name=f"roads_{tier}",
                    material=f"road_{tier}",
                    vertices=tuple(gb.vertices),
                    faces=tuple(gb.faces),
                )
            )
    return groups, RoadStats(n_ok=n_ok, n_skipped=n_skipped)


def build_ground_group(
    island: sg.Polygon, meters_per_unit: float, depth: float
) -> tuple[MeshGroup | None, bool]:
    """The island boundary as a slab: top at y=0, extruded down ``depth``."""
    mesh, holes_simplified = extrude_solid(island, -depth, 0.0, meters_per_unit)
    if mesh is None:
        return None, holes_simplified
    return (
        MeshGroup(
            name="ground", material="ground", vertices=mesh.vertices, faces=mesh.faces
        ),
        holes_simplified,
    )


def build_parks_group(
    parks: list[sg.Polygon], meters_per_unit: float, y: float
) -> tuple[MeshGroup | None, ParkStats]:
    """A single ``parks``/``park`` group: every park-district polygon as a flat
    pad at constant ``y``. ``parks`` order should already be deterministic
    (callers pass district features in ``district_id`` order)."""
    gb = _GroupBuilder()
    n_ok = 0
    n_skipped = 0
    for poly in parks:
        mesh, _holes_simplified = flat_cap(poly, y, meters_per_unit)
        if mesh is None:
            n_skipped += 1
            continue
        gb.add(mesh)
        n_ok += 1
    if not gb.faces:
        return None, ParkStats(n_ok=n_ok, n_skipped=n_skipped)
    return (
        MeshGroup(
            name="parks",
            material="park",
            vertices=tuple(gb.vertices),
            faces=tuple(gb.faces),
        ),
        ParkStats(n_ok=n_ok, n_skipped=n_skipped),
    )


def build_water_group(
    island_bounds: tuple[float, float, float, float],
    meters_per_unit: float,
    margin_frac: float,
    y: float,
) -> MeshGroup:
    """A single flat quad covering ``island_bounds`` expanded by ``margin_frac``.

    ``island_bounds`` is ``(minx, miny, maxx, maxy)`` in island units (e.g.
    ``island_polygon.bounds``). A box is never degenerate, so this always
    returns a group (no skip/``None`` path, unlike the other builders).
    """
    minx, miny, maxx, maxy = island_bounds
    mx = (maxx - minx) * margin_frac
    my = (maxy - miny) * margin_frac
    quad = sg.box(minx - mx, miny - my, maxx + mx, maxy + my)
    mesh, _holes_simplified = flat_cap(quad, y, meters_per_unit)
    assert mesh is not None, "a box quad is never degenerate"
    return MeshGroup(
        name="water", material="water", vertices=mesh.vertices, faces=mesh.faces
    )


# ---------------------------------------------------------------------------
# Full bake assembly
# ---------------------------------------------------------------------------


def assemble_mesh(
    *,
    island: sg.Polygon,
    lots: list[LotRecord],
    roads: list[RoadSegment],
    parks: list[sg.Polygon],
    meters_per_unit: float = DEFAULT_METERS_PER_UNIT,
    street_widths: dict[str, float] = DEFAULT_STREET_WIDTHS,  # noqa: B006 (frozen-in-effect module constant)
    street_y: float = DEFAULT_STREET_Y,
    park_y: float = DEFAULT_PARK_Y,
    ground_depth: float = DEFAULT_GROUND_DEPTH,
    water_y: float = DEFAULT_WATER_Y,
    water_margin_frac: float = DEFAULT_WATER_MARGIN_FRAC,
) -> tuple[Mesh, BakeStats]:
    """Assemble the full Stage-G1 bake: ground, water, parks, roads, buildings.

    Fixed, deterministic group emission order: ``ground``, ``water``,
    ``parks``, ``roads_*`` (alphabetical by tier), then ``block_*_buildings``
    (ascending block id) -- see the individual builders for their own
    within-group determinism/degenerate-handling rules.
    """
    groups: list[MeshGroup] = []

    ground_group, ground_holes_simplified = build_ground_group(
        island, meters_per_unit, ground_depth
    )
    if ground_group is not None:
        groups.append(ground_group)

    groups.append(
        build_water_group(island.bounds, meters_per_unit, water_margin_frac, water_y)
    )

    park_group, park_stats = build_parks_group(parks, meters_per_unit, park_y)
    if park_group is not None:
        groups.append(park_group)

    road_groups, road_stats = build_road_groups(
        roads, street_widths, meters_per_unit, street_y
    )
    groups.extend(road_groups)

    building_groups, building_stats = build_building_groups(lots, meters_per_unit)
    groups.extend(building_groups)

    mesh = Mesh(groups=tuple(groups))
    stats = BakeStats(
        buildings=building_stats,
        roads=road_stats,
        parks=park_stats,
        ground_holes_simplified=ground_holes_simplified,
    )
    return mesh, stats


# ---------------------------------------------------------------------------
# OBJ / MTL text serialization
# ---------------------------------------------------------------------------


def mesh_materials(mesh: Mesh) -> list[str]:
    """Sorted, de-duplicated material names actually used by ``mesh``."""
    return sorted({group.material for group in mesh.groups if group.faces})


def mesh_to_obj(mesh: Mesh, mtllib: str) -> str:
    """Render ``mesh`` to OBJ text (fixed ``%.4f`` floats, LF newlines).

    Vertex indices are OBJ's file-global 1-based pool, offset group-by-group
    in emission order (:attr:`Mesh.groups`) -- OBJ itself has no per-group
    vertex numbering. Empty groups (no faces) are skipped entirely.
    """
    lines = [
        "# vrcwscrape greybox mesh bake (docs/greybox-plan.md Stage G1)",
        f"mtllib {mtllib}",
        "",
    ]
    offset = 0
    for group in mesh.groups:
        if not group.faces:
            continue
        lines.append(f"g {group.name}")
        lines.append(f"usemtl {group.material}")
        for x, y, z in group.vertices:
            lines.append(f"v {x:.4f} {y:.4f} {z:.4f}")
        for a, b, c in group.faces:
            lines.append(f"f {a + offset + 1} {b + offset + 1} {c + offset + 1}")
        offset += len(group.vertices)
        lines.append("")
    return "\n".join(lines) + "\n"


def materials_to_mtl(
    materials: list[str],
    colors: dict[str, tuple[float, float, float]] = DEFAULT_MATERIAL_COLORS,  # noqa: B006
) -> str:
    """Render flat-diffuse ``newmtl`` blocks for ``materials`` (sorted input
    expected -- see :func:`mesh_materials`). Unknown material names fall back
    to a neutral mid-grey rather than raising."""
    lines = [
        "# vrcwscrape greybox mesh bake materials (docs/greybox-plan.md Stage G1)",
        "",
    ]
    for name in materials:
        r, g, b = colors.get(name, (0.6, 0.6, 0.6))
        lines.append(f"newmtl {name}")
        lines.append("Ka 0.0000 0.0000 0.0000")
        lines.append(f"Kd {r:.4f} {g:.4f} {b:.4f}")
        lines.append("Ks 0.0000 0.0000 0.0000")
        lines.append("d 1.0000")
        lines.append("illum 1")
        lines.append("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# labels.csv rows + manifest
# ---------------------------------------------------------------------------


def build_label_rows(
    lots: list[LotRecord], meters_per_unit: float
) -> list[tuple[str, str, float, float, float]]:
    """``(world_id, name, x_m, z_m, roof_y_m)`` rows, sorted by ``world_id``.

    ``name`` is ``""`` when ``LotRecord.name`` is ``None``/empty. ``roof_y_m``
    is ``LotRecord.height`` unchanged (already meters); ``x_m``/``z_m`` are
    the lot center scaled by ``meters_per_unit`` (island x -> mesh x, island
    y -> mesh z, matching every other builder's axis convention).
    """
    rows = [
        (
            lot.world_id,
            lot.name or "",
            lot.x * meters_per_unit,
            lot.y * meters_per_unit,
            lot.height,
        )
        for lot in lots
    ]
    rows.sort(key=lambda row: row[0])
    return rows


def build_mesh_manifest(
    mesh: Mesh,
    stats: BakeStats,
    *,
    meters_per_unit: float,
    bounds_m: tuple[float, float, float, float],
    street_widths: dict[str, float],
) -> dict[str, Any]:
    """``greybox_mesh_manifest.json`` payload: scale, group/material inventory,
    bounds, and the degenerate-input counters from :func:`assemble_mesh`."""
    group_inventory = {
        group.name: len(group.faces) for group in mesh.groups if group.faces
    }
    minx, miny, maxx, maxy = bounds_m
    return {
        "stage": "G1 (greybox mesh bake)",
        "description": (
            "OBJ/MTL triangle mesh baked from the Stage-G0 greybox export "
            "(docs/greybox-plan.md), grouped/materialed by semantic kind "
            "for Unity/VRChat static-batching granularity."
        ),
        "meters_per_unit": meters_per_unit,
        "coordinate_convention": (
            "Y-up meters: island x -> OBJ x, island y -> OBJ z, up -> OBJ y. "
            "Building/ground heights are already meters (r1_lots.Lot.height) "
            "-- meters_per_unit scales planar x/y only. Unity's OBJ importer "
            "flips handedness itself; this bake does not pre-flip."
        ),
        "groups": dict(sorted(group_inventory.items())),
        "materials": mesh_materials(mesh),
        "bounds_m": {
            "minx": minx,
            "miny": miny,
            "maxx": maxx,
            "maxy": maxy,
        },
        "street_widths_island_units": dict(sorted(street_widths.items())),
        "stats": {
            "buildings_ok": stats.buildings.n_ok,
            "buildings_skipped": stats.buildings.n_skipped,
            "buildings_holes_simplified": stats.buildings.n_holes_simplified,
            "roads_ok": stats.roads.n_ok,
            "roads_skipped": stats.roads.n_skipped,
            "parks_ok": stats.parks.n_ok,
            "parks_skipped": stats.parks.n_skipped,
            "ground_holes_simplified": stats.ground_holes_simplified,
        },
    }
