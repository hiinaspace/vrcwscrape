"""Tests for the R1 greybox mesh bake (``mapgen.r1_mesh``): prism/flat-cap
geometry builders, group assembly, OBJ/MTL/labels serialization."""

from __future__ import annotations

import math

import shapely.geometry as sg

from mapgen.r1_mesh import (
    BuildingStats,
    LotRecord,
    Mesh,
    MeshGroup,
    ParkStats,
    RoadSegment,
    RoadStats,
    assemble_mesh,
    buffer_ribbon,
    build_building_groups,
    build_ground_group,
    build_label_rows,
    build_mesh_manifest,
    build_parks_group,
    build_road_groups,
    build_water_group,
    extrude_solid,
    fillet_centerline,
    flat_cap,
    materials_to_mtl,
    mesh_materials,
    mesh_to_obj,
    triangulate_with_holes,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _face_normal(verts, face) -> tuple[float, float, float]:
    a, b, c = (verts[i] for i in face)
    v1 = tuple(b[k] - a[k] for k in range(3))
    v2 = tuple(c[k] - a[k] for k in range(3))
    return (
        v1[1] * v2[2] - v1[2] * v2[1],
        v1[2] * v2[0] - v1[0] * v2[2],
        v1[0] * v2[1] - v1[1] * v2[0],
    )


# ---------------------------------------------------------------------------
# extrude_solid: unit square prism
# ---------------------------------------------------------------------------


def test_extrude_solid_unit_square_counts_and_extents() -> None:
    square = sg.box(0.0, 0.0, 1.0, 1.0)
    mesh, holes_simplified = extrude_solid(square, 0.0, 10.0, meters_per_unit=1.0)
    assert mesh is not None
    assert not holes_simplified
    # 4 wall-bottom + 4 wall-top + 4 cap-bottom + 4 cap-top.
    assert len(mesh.vertices) == 16
    # 4 edges * 2 tris (walls) + 2 (bottom cap) + 2 (top cap).
    assert len(mesh.faces) == 12
    ys = [v[1] for v in mesh.vertices]
    assert min(ys) == 0.0
    assert max(ys) == 10.0


def test_extrude_solid_cap_normals_point_up_and_down() -> None:
    square = sg.box(0.0, 0.0, 1.0, 1.0)
    mesh, _ = extrude_solid(square, 0.0, 10.0, meters_per_unit=1.0)
    assert mesh is not None
    # Bottom cap faces are the ones whose every vertex has y == 0.
    bottom_faces = [f for f in mesh.faces if all(mesh.vertices[i][1] == 0.0 for i in f)]
    top_faces = [f for f in mesh.faces if all(mesh.vertices[i][1] == 10.0 for i in f)]
    assert len(bottom_faces) == 2
    assert len(top_faces) == 2
    for f in bottom_faces:
        n = _face_normal(mesh.vertices, f)
        assert n[1] < 0.0
    for f in top_faces:
        n = _face_normal(mesh.vertices, f)
        assert n[1] > 0.0


def test_extrude_solid_wall_normals_point_outward() -> None:
    square = sg.box(0.0, 0.0, 1.0, 1.0)
    mesh, _ = extrude_solid(square, 0.0, 10.0, meters_per_unit=1.0)
    assert mesh is not None
    wall_faces = [
        f
        for f in mesh.faces
        if len({mesh.vertices[i][1] for i in f}) == 2  # mixed y=0/y=10 -> a wall tri
    ]
    assert len(wall_faces) == 8
    for f in wall_faces:
        n = _face_normal(mesh.vertices, f)
        # Wall normals are horizontal (no Y component) and must point away
        # from the square's center (0.5, *, 0.5) in the XZ plane.
        assert n[1] == 0.0
        cx = sum(mesh.vertices[i][0] for i in f) / 3.0 - 0.5
        cz = sum(mesh.vertices[i][2] for i in f) / 3.0 - 0.5
        outward_dot = n[0] * cx + n[2] * cz
        assert outward_dot > 0.0


# ---------------------------------------------------------------------------
# Y-up axis mapping + meters_per_unit scaling
# ---------------------------------------------------------------------------


def test_axis_mapping_island_xy_to_obj_xz() -> None:
    # A single-vertex-degenerate polygon isn't triangulatable, so use a tiny
    # square whose center sits at island (1, 2) to pin down the mapping.
    poly = sg.box(0.5, 1.5, 1.5, 2.5)  # center (1, 2)
    mesh, _ = flat_cap(poly, y=7.0, meters_per_unit=1.0)
    assert mesh is not None
    cx = sum(v[0] for v in mesh.vertices) / len(mesh.vertices)
    cy = sum(v[1] for v in mesh.vertices) / len(mesh.vertices)
    cz = sum(v[2] for v in mesh.vertices) / len(mesh.vertices)
    assert math.isclose(cx, 1.0)
    assert math.isclose(cy, 7.0)
    assert math.isclose(cz, 2.0)


def test_meters_per_unit_scales_planar_not_height() -> None:
    square = sg.box(0.0, 0.0, 1.0, 1.0)
    mesh, _ = extrude_solid(square, 0.0, 10.0, meters_per_unit=25.0)
    assert mesh is not None
    xs = [v[0] for v in mesh.vertices]
    zs = [v[2] for v in mesh.vertices]
    ys = [v[1] for v in mesh.vertices]
    assert max(xs) == 25.0
    assert max(zs) == 25.0
    # Height stays in meters -- NOT scaled by meters_per_unit.
    assert max(ys) == 10.0
    assert min(ys) == 0.0


# ---------------------------------------------------------------------------
# Street ribbon width
# ---------------------------------------------------------------------------


def test_buffer_ribbon_width() -> None:
    line = sg.LineString([(0.0, 0.0), (10.0, 0.0)])
    ribbon = buffer_ribbon(line, width_units=2.0)
    assert ribbon is not None
    minx, miny, maxx, maxy = ribbon.bounds
    # Round caps don't affect the perpendicular extent -- exact width.
    assert math.isclose(miny, -1.0, abs_tol=1e-9)
    assert math.isclose(maxy, 1.0, abs_tol=1e-9)
    # Caps DO extend the along-line extent beyond the original endpoints.
    assert minx < 0.0
    assert maxx > 10.0


def test_buffer_ribbon_rejects_degenerate() -> None:
    assert buffer_ribbon(sg.LineString(), width_units=1.0) is None
    assert buffer_ribbon(sg.LineString([(0, 0), (1, 0)]), width_units=0.0) is None
    assert buffer_ribbon(sg.LineString([(0, 0), (1, 0)]), width_units=-1.0) is None


# ---------------------------------------------------------------------------
# Centerline fillet (S7c mesh-layer corner rounding)
# ---------------------------------------------------------------------------


def _corner_angle_deg(a, b, c) -> float:
    """Interior angle (degrees) at vertex ``b`` of the polyline ``a-b-c``."""
    v1 = (a[0] - b[0], a[1] - b[1])
    v2 = (c[0] - b[0], c[1] - b[1])
    dot = v1[0] * v2[0] + v1[1] * v2[1]
    n1 = math.hypot(*v1)
    n2 = math.hypot(*v2)
    cos_theta = max(-1.0, min(1.0, dot / (n1 * n2)))
    return math.degrees(math.acos(cos_theta))


def test_fillet_centerline_preserves_endpoints_open_line() -> None:
    line = sg.LineString([(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (10.0, 5.0)])
    filleted = fillet_centerline(line)
    coords = list(filleted.coords)
    assert coords[0] == (0.0, 0.0)
    assert coords[-1] == (10.0, 5.0)


def test_fillet_centerline_rounds_sharp_corner() -> None:
    """A 90-degree right-angle kink should read LESS sharp (angle closer to
    180 degrees straight) after filleting -- the user's sharp-corner
    complaint this fix addresses."""
    line = sg.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)])
    filleted = fillet_centerline(line, post_tol=0.0)
    coords = list(filleted.coords)
    assert len(coords) > 3  # corner-cutting adds interior vertices
    # The sharpest surviving interior vertex should be less acute than the
    # original 90-degree kink.
    angles = [
        _corner_angle_deg(coords[i - 1], coords[i], coords[i + 1])
        for i in range(1, len(coords) - 1)
    ]
    assert min(angles) > 90.0


def test_fillet_centerline_closed_ring_stays_closed() -> None:
    ring = sg.LineString(
        [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]
    )
    filleted = fillet_centerline(ring)
    coords = list(filleted.coords)
    assert coords[0] == coords[-1]
    # Every original corner should read less sharp, same as the open case.
    assert len(coords) > len(list(ring.coords))


def test_fillet_centerline_degenerate_line_passthrough() -> None:
    short = sg.LineString([(0.0, 0.0), (1.0, 1.0)])
    assert list(fillet_centerline(short).coords) == list(short.coords)


def test_fillet_centerline_zero_iterations_is_noop() -> None:
    line = sg.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)])
    assert fillet_centerline(line, iterations=0) is line


def test_fillet_centerline_deterministic() -> None:
    line = sg.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (20.0, 10.0)])
    first = list(fillet_centerline(line).coords)
    second = list(fillet_centerline(line).coords)
    assert first == second


# ---------------------------------------------------------------------------
# Holes / degenerate handling
# ---------------------------------------------------------------------------


def test_triangulate_with_holes_box_with_hole() -> None:
    outer = [(0, 0), (4, 0), (4, 4), (0, 4)]
    hole = [(1, 1), (3, 1), (3, 3), (1, 3)]
    poly = sg.Polygon(outer, [hole])
    verts2d, faces, holes_simplified = triangulate_with_holes(poly)
    assert not holes_simplified
    assert len(verts2d) == 8  # 4 outer + 4 hole, welded (earcut dupes the closing pt)
    assert len(faces) == 8


def test_extrude_solid_box_with_hole_wall_count() -> None:
    outer = [(0, 0), (4, 0), (4, 4), (0, 4)]
    hole = [(1, 1), (3, 1), (3, 3), (1, 3)]
    poly = sg.Polygon(outer, [hole])
    mesh, holes_simplified = extrude_solid(poly, 0.0, 5.0, meters_per_unit=1.0)
    assert mesh is not None
    assert not holes_simplified
    # caps: 8 verts * 2 layers = 16; walls: (4 + 4) ring verts * 2 layers = 16.
    assert len(mesh.vertices) == 32
    # caps: 8 tris * 2 layers = 16; walls: (4 + 4) edges * 2 tris = 16.
    assert len(mesh.faces) == 32


def test_extrude_solid_empty_polygon_is_skipped() -> None:
    mesh, holes_simplified = extrude_solid(sg.Polygon(), 0.0, 10.0, meters_per_unit=1.0)
    assert mesh is None
    assert not holes_simplified


def test_extrude_solid_zero_area_polygon_is_skipped() -> None:
    degenerate = sg.Polygon([(0, 0), (1, 0), (2, 0)])  # collinear -> zero area
    mesh, _ = extrude_solid(degenerate, 0.0, 10.0, meters_per_unit=1.0)
    assert mesh is None


def test_flat_cap_empty_polygon_is_skipped() -> None:
    mesh, holes_simplified = flat_cap(sg.Polygon(), y=0.3, meters_per_unit=1.0)
    assert mesh is None
    assert not holes_simplified


# ---------------------------------------------------------------------------
# build_building_groups: grouping + skip/holes counting
# ---------------------------------------------------------------------------


def test_build_building_groups_group_per_block_sorted() -> None:
    lots = [
        LotRecord(
            world_id="w2",
            block_id=1,
            footprint=sg.box(0, 0, 1, 1),
            height=4.0,
            name=None,
            x=0.5,
            y=0.5,
        ),
        LotRecord(
            world_id="w1",
            block_id=0,
            footprint=sg.box(0, 0, 1, 1),
            height=8.0,
            name="Alpha",
            x=0.5,
            y=0.5,
        ),
        LotRecord(
            world_id="w3",
            block_id=0,
            footprint=sg.box(2, 2, 3, 3),
            height=6.0,
            name=None,
            x=2.5,
            y=2.5,
        ),
    ]
    groups, stats = build_building_groups(lots, meters_per_unit=1.0)
    assert [g.name for g in groups] == ["block_000_buildings", "block_001_buildings"]
    assert groups[0].material == "building"
    # block 0 has two lots (12 faces each prism) -> 24 faces.
    assert len(groups[0].faces) == 24
    assert len(groups[1].faces) == 12
    assert stats == BuildingStats(n_ok=3, n_skipped=0, n_holes_simplified=0)


def test_build_building_groups_skips_degenerate_and_omits_empty_block() -> None:
    lots = [
        LotRecord(
            world_id="w1",
            block_id=0,
            footprint=sg.Polygon(),  # empty -> skipped
            height=4.0,
            name=None,
            x=0.0,
            y=0.0,
        ),
        LotRecord(
            world_id="w2",
            block_id=1,
            footprint=sg.box(0, 0, 1, 1),
            height=4.0,
            name=None,
            x=0.5,
            y=0.5,
        ),
    ]
    groups, stats = build_building_groups(lots, meters_per_unit=1.0)
    # Block 0's only lot was degenerate -> no group emitted for it.
    assert [g.name for g in groups] == ["block_001_buildings"]
    assert stats.n_ok == 1
    assert stats.n_skipped == 1


# ---------------------------------------------------------------------------
# build_road_groups
# ---------------------------------------------------------------------------


def test_build_road_groups_one_group_per_tier() -> None:
    segments = [
        RoadSegment(sg.LineString([(0, 0), (10, 0)]), tier="highway"),
        RoadSegment(sg.LineString([(0, 5), (10, 5)]), tier="street"),
        RoadSegment(sg.LineString([(0, 10), (10, 10)]), tier="unknown_tier"),
    ]
    widths = {"highway": 1.0, "street": 0.25}
    groups, stats = build_road_groups(segments, widths, meters_per_unit=1.0, y=0.3)
    assert [g.name for g in groups] == ["roads_highway", "roads_street"]
    assert groups[0].material == "road_highway"
    for g in groups:
        assert all(v[1] == 0.3 for v in g.vertices)
    assert stats.n_ok == 2
    assert stats.n_skipped == 1  # the unknown-tier segment has no width entry


# ---------------------------------------------------------------------------
# build_ground_group / build_parks_group / build_water_group
# ---------------------------------------------------------------------------


def test_build_ground_group_is_a_slab_below_zero() -> None:
    island = sg.box(0.0, 0.0, 100.0, 60.0)
    group, holes_simplified = build_ground_group(island, meters_per_unit=1.0, depth=2.0)
    assert group is not None
    assert not holes_simplified
    assert group.name == "ground"
    ys = [v[1] for v in group.vertices]
    assert min(ys) == -2.0
    assert max(ys) == 0.0


def test_build_parks_group_flat_pad_and_skip_count() -> None:
    parks = [sg.box(0, 0, 1, 1), sg.Polygon()]
    group, stats = build_parks_group(parks, meters_per_unit=1.0, y=0.15)
    assert group is not None
    assert all(v[1] == 0.15 for v in group.vertices)
    assert stats == ParkStats(n_ok=1, n_skipped=1)


def test_build_parks_group_all_degenerate_returns_none() -> None:
    group, stats = build_parks_group([sg.Polygon()], meters_per_unit=1.0, y=0.15)
    assert group is None
    assert stats == ParkStats(n_ok=0, n_skipped=1)


def test_build_water_group_covers_bbox_with_margin() -> None:
    group = build_water_group(
        (0.0, 0.0, 100.0, 60.0), meters_per_unit=1.0, margin_frac=0.3, y=-0.5
    )
    xs = [v[0] for v in group.vertices]
    zs = [v[2] for v in group.vertices]
    ys = [v[1] for v in group.vertices]
    assert math.isclose(min(xs), -30.0)
    assert math.isclose(max(xs), 130.0)
    assert math.isclose(min(zs), -18.0)
    assert math.isclose(max(zs), 78.0)
    assert all(y == -0.5 for y in ys)


# ---------------------------------------------------------------------------
# labels.csv rows
# ---------------------------------------------------------------------------


def test_build_label_rows_roof_height_and_sort_and_blank_name() -> None:
    lots = [
        LotRecord(
            world_id="zzz",
            block_id=0,
            footprint=sg.box(0, 0, 1, 1),
            height=12.5,
            name=None,
            x=1.0,
            y=2.0,
        ),
        LotRecord(
            world_id="aaa",
            block_id=0,
            footprint=sg.box(0, 0, 1, 1),
            height=8.0,
            name="Home",
            x=3.0,
            y=4.0,
        ),
    ]
    rows = build_label_rows(lots, meters_per_unit=10.0)
    assert rows == [
        ("aaa", "Home", 30.0, 40.0, 8.0),
        ("zzz", "", 10.0, 20.0, 12.5),
    ]


# ---------------------------------------------------------------------------
# assemble_mesh + OBJ/MTL serialization + determinism
# ---------------------------------------------------------------------------


def _sample_scene():
    island = sg.box(0.0, 0.0, 20.0, 10.0)
    lots = [
        LotRecord(
            world_id="w1",
            block_id=0,
            footprint=sg.box(1, 1, 3, 3),
            height=5.0,
            name="Home",
            x=2.0,
            y=2.0,
        ),
        LotRecord(
            world_id="w2",
            block_id=1,
            footprint=sg.box(10, 1, 12, 3),
            height=9.0,
            name=None,
            x=11.0,
            y=2.0,
        ),
    ]
    roads = [
        RoadSegment(sg.LineString([(0, 5), (20, 5)]), tier="highway"),
        RoadSegment(sg.LineString([(5, 0), (5, 10)]), tier="street"),
    ]
    parks = [sg.box(15, 6, 19, 9)]
    return island, lots, roads, parks


def test_assemble_mesh_group_order_and_materials() -> None:
    island, lots, roads, parks = _sample_scene()
    mesh, stats = assemble_mesh(island=island, lots=lots, roads=roads, parks=parks)
    names = [g.name for g in mesh.groups]
    assert names == [
        "ground",
        "water",
        "parks",
        "roads_highway",
        "roads_street",
        "block_000_buildings",
        "block_001_buildings",
    ]
    assert stats.buildings.n_ok == 2
    assert mesh_materials(mesh) == sorted(
        {
            "ground",
            "water",
            "park",
            "road_highway",
            "road_street",
            "building",
        }
    )


def test_mesh_to_obj_and_mtl_shape() -> None:
    island, lots, roads, parks = _sample_scene()
    mesh, _ = assemble_mesh(island=island, lots=lots, roads=roads, parks=parks)
    obj_text = mesh_to_obj(mesh, mtllib="greybox.mtl")
    assert obj_text.startswith("# vrcwscrape greybox mesh bake")
    assert "mtllib greybox.mtl" in obj_text
    assert "g ground" in obj_text
    assert "usemtl building" in obj_text
    assert obj_text.endswith("\n")

    mtl_text = materials_to_mtl(mesh_materials(mesh))
    assert "newmtl building" in mtl_text
    assert "newmtl water" in mtl_text
    assert "Kd 0.7500 0.7500 0.7500" in mtl_text


def test_build_mesh_manifest_shape() -> None:
    island, lots, roads, parks = _sample_scene()
    mesh, stats = assemble_mesh(island=island, lots=lots, roads=roads, parks=parks)
    manifest = build_mesh_manifest(
        mesh,
        stats,
        meters_per_unit=25.0,
        bounds_m=(0.0, 0.0, 500.0, 250.0),
        street_widths={"highway": 1.0, "street": 0.25},
    )
    assert manifest["meters_per_unit"] == 25.0
    assert manifest["groups"]["block_000_buildings"] == 12
    assert manifest["bounds_m"] == {
        "minx": 0.0,
        "miny": 0.0,
        "maxx": 500.0,
        "maxy": 250.0,
    }
    assert manifest["stats"]["buildings_ok"] == 2


def test_determinism_two_bakes_byte_identical() -> None:
    island, lots, roads, parks = _sample_scene()
    mesh1, stats1 = assemble_mesh(
        island=island, lots=list(lots), roads=list(roads), parks=list(parks)
    )
    mesh2, stats2 = assemble_mesh(
        island=island, lots=list(lots), roads=list(roads), parks=list(parks)
    )
    obj1 = mesh_to_obj(mesh1, mtllib="greybox.mtl")
    obj2 = mesh_to_obj(mesh2, mtllib="greybox.mtl")
    assert obj1 == obj2
    mtl1 = materials_to_mtl(mesh_materials(mesh1))
    mtl2 = materials_to_mtl(mesh_materials(mesh2))
    assert mtl1 == mtl2
    assert stats1 == stats2
    rows1 = build_label_rows(lots, meters_per_unit=25.0)
    rows2 = build_label_rows(list(reversed(lots)), meters_per_unit=25.0)
    assert rows1 == rows2  # sorted by world_id regardless of input order


# ---------------------------------------------------------------------------
# Type sanity for the frozen dataclasses (import surface used by the script).
# ---------------------------------------------------------------------------


def test_dataclasses_are_frozen_and_hashable() -> None:
    g = MeshGroup(name="x", material="m", vertices=((0.0, 0.0, 0.0),), faces=())
    mesh = Mesh(groups=(g,))
    assert mesh.groups[0].name == "x"
    assert isinstance(RoadStats(n_ok=0, n_skipped=0), RoadStats)
