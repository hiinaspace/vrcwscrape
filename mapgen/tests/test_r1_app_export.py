"""Tests for R1 track-W export logic (``mapgen.r1_app_export``): the
island-frame affine inverse, oriented-rect fitting, tier->kind/width mapping,
bbox filtering, and the GeoJSON feature builders."""

from __future__ import annotations

import math

import numpy as np
import pytest
import shapely.affinity
import shapely.geometry as sg

from mapgen.r1_app_export import (
    CHEN_STREET_KIND,
    CHEN_STREET_WIDTH_ISLAND_UNITS,
    IslandAffine,
    bbox_intersects,
    block_feature,
    blocks_feature_collection,
    derive_levels,
    filter_regions_by_bbox,
    invert_geom,
    invert_xy,
    island_length_to_app,
    land_feature_collection,
    landuse_feature,
    landuse_feature_collection,
    max_affine_roundtrip_error,
    oriented_rect,
    parcel_feature,
    parcels_feature_collection,
    road_feature,
    road_kind,
    road_width_island_units,
    roads_feature_collection,
)
from mapgen.r1_inputs import polygon_to_island_frame, to_island_frame

# ---------------------------------------------------------------------------
# Affine inversion
# ---------------------------------------------------------------------------


def test_invert_xy_is_exact_inverse_of_forward_transform() -> None:
    rng = np.random.default_rng(0)
    xs = rng.uniform(-50, 50, 200)
    ys = rng.uniform(-50, 50, 200)
    affine = IslandAffine(offset_x=-13.07, offset_y=8.39, scale=17.69)

    ix, iy = to_island_frame(xs, ys, affine.offset_x, affine.offset_y, affine.scale)
    ax, ay = invert_xy(ix, iy, affine)

    assert np.allclose(ax, xs, atol=1e-9)
    assert np.allclose(ay, ys, atol=1e-9)


def test_invert_geom_is_exact_inverse_of_polygon_to_island_frame() -> None:
    affine = IslandAffine(offset_x=3.5, offset_y=-2.0, scale=4.0)
    poly = sg.box(0.0, 0.0, 10.0, 6.0)

    island_poly = polygon_to_island_frame(
        poly, affine.offset_x, affine.offset_y, affine.scale
    )
    back = invert_geom(island_poly, affine)

    assert poly.equals_exact(back, tolerance=1e-9)


def test_island_length_to_app_scales_by_reciprocal_scale() -> None:
    affine = IslandAffine(offset_x=0.0, offset_y=0.0, scale=20.0)
    assert island_length_to_app(1.0, affine) == pytest.approx(0.05)


def test_max_affine_roundtrip_error_zero_on_exact_forward_transform() -> None:
    rng = np.random.default_rng(1)
    orig_x = rng.uniform(-30, 30, 50)
    orig_y = rng.uniform(-30, 30, 50)
    affine = IslandAffine(offset_x=1.2, offset_y=-4.4, scale=9.0)
    ix, iy = to_island_frame(
        orig_x, orig_y, affine.offset_x, affine.offset_y, affine.scale
    )

    err = max_affine_roundtrip_error(ix, iy, orig_x, orig_y, affine)

    assert err < 1e-9


def test_max_affine_roundtrip_error_nonzero_when_perturbed() -> None:
    affine = IslandAffine(offset_x=0.0, offset_y=0.0, scale=1.0)
    ix = np.array([1.0, 2.0])
    iy = np.array([1.0, 2.0])
    orig_x = np.array([1.0, 2.5])  # off by 0.5
    orig_y = np.array([1.0, 2.0])

    err = max_affine_roundtrip_error(ix, iy, orig_x, orig_y, affine)

    assert err == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# oriented_rect canonicalization
# ---------------------------------------------------------------------------


def test_oriented_rect_axis_aligned() -> None:
    poly = sg.box(0.0, 0.0, 10.0, 4.0)  # width 10, depth 4, angle 0

    r = oriented_rect(poly)

    assert r.cx == pytest.approx(5.0)
    assert r.cy == pytest.approx(2.0)
    assert r.width == pytest.approx(10.0)
    assert r.depth == pytest.approx(4.0)
    assert r.angle == pytest.approx(0.0, abs=1e-9)


def test_oriented_rect_width_always_gte_depth() -> None:
    poly = sg.box(0.0, 0.0, 3.0, 9.0)  # "tall" box: naive long axis is vertical

    r = oriented_rect(poly)

    assert r.width >= r.depth
    assert r.width == pytest.approx(9.0)
    assert r.depth == pytest.approx(3.0)
    # angle folded to (-pi/2, pi/2]; a vertical long axis is +-pi/2 -> +pi/2
    assert r.angle == pytest.approx(math.pi / 2)


def test_oriented_rect_angle_in_expected_range() -> None:
    for deg in range(0, 180, 5):
        poly = shapely.affinity.rotate(sg.box(-4.0, -1.0, 4.0, 1.0), deg, origin=(0, 0))
        r = oriented_rect(poly)
        assert -math.pi / 2 < r.angle <= math.pi / 2 + 1e-9


def test_oriented_rect_deterministic_under_vertex_rotation_and_reversal() -> None:
    base = shapely.affinity.rotate(sg.box(-5.0, -2.0, 5.0, 2.0), 37, origin=(1.0, -3.0))
    coords = list(base.exterior.coords[:-1])
    r0 = oriented_rect(sg.Polygon(coords))

    for k in range(1, len(coords)):
        rolled = sg.Polygon(coords[k:] + coords[:k])
        r = oriented_rect(rolled)
        assert r.cx == pytest.approx(r0.cx, abs=1e-9)
        assert r.cy == pytest.approx(r0.cy, abs=1e-9)
        assert r.width == pytest.approx(r0.width, abs=1e-9)
        assert r.depth == pytest.approx(r0.depth, abs=1e-9)
        assert r.angle == pytest.approx(r0.angle, abs=1e-9)

    reversed_poly = sg.Polygon(list(reversed(coords)))
    r_rev = oriented_rect(reversed_poly)
    assert r_rev.width == pytest.approx(r0.width, abs=1e-9)
    assert r_rev.depth == pytest.approx(r0.depth, abs=1e-9)
    assert r_rev.angle == pytest.approx(r0.angle, abs=1e-9)


def test_oriented_rect_degenerate_footprint_returns_zero_rect() -> None:
    poly = sg.Polygon([(1.0, 1.0), (1.0, 1.0), (1.0, 1.0)])  # zero-area

    r = oriented_rect(poly)

    assert r.width == 0.0
    assert r.depth == 0.0
    assert r.angle == 0.0


# ---------------------------------------------------------------------------
# tier -> kind / width mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("tier", "kind"),
    [
        ("highway", "arterial"),
        ("major", "collector"),
        ("local", "local"),
        ("ring", "arterial"),
    ],
)
def test_road_kind_mapping(tier: str, kind: str) -> None:
    assert road_kind(tier) == kind


def test_road_kind_unknown_tier_raises() -> None:
    with pytest.raises(KeyError):
        road_kind("nonsense")


@pytest.mark.parametrize(
    ("tier", "width"),
    [
        ("highway", 1.0),
        ("major", 0.7),
        ("local", 0.5),
        ("ring", 0.6),
    ],
)
def test_road_width_island_units(tier: str, width: float) -> None:
    assert road_width_island_units(tier) == pytest.approx(width)


def test_chen_street_kind_and_width_constants() -> None:
    assert CHEN_STREET_KIND == "minor"
    assert CHEN_STREET_WIDTH_ISLAND_UNITS == 0.25


# ---------------------------------------------------------------------------
# bbox filter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        ((0, 0, 10, 10), (5, 5, 15, 15), True),  # overlap
        ((0, 0, 10, 10), (10, 10, 20, 20), True),  # touching corner
        ((0, 0, 10, 10), (11, 11, 20, 20), False),  # disjoint
        ((0, 0, 10, 10), (2, 2, 4, 4), True),  # contained
    ],
)
def test_bbox_intersects(a, b, expected) -> None:
    assert bbox_intersects(a, b) is expected


def test_filter_regions_by_bbox_keeps_only_intersecting_features() -> None:
    near = {
        "type": "Feature",
        "properties": {"cluster_id": 0},
        "geometry": sg.mapping(sg.box(0, 0, 2, 2)),
    }
    far = {
        "type": "Feature",
        "properties": {"cluster_id": 1},
        "geometry": sg.mapping(sg.box(100, 100, 102, 102)),
    }
    regions = {"type": "FeatureCollection", "features": [far, near]}

    out = filter_regions_by_bbox(regions, island_bbox=(-1, -1, 5, 5))

    assert [f["properties"]["cluster_id"] for f in out["features"]] == [0]


# ---------------------------------------------------------------------------
# feature / FeatureCollection builders
# ---------------------------------------------------------------------------


def test_land_feature_collection_single_feature() -> None:
    fc = land_feature_collection(sg.box(0, 0, 1, 1))
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == 1
    assert fc["features"][0]["geometry"]["type"] == "Polygon"


def test_roads_feature_collection_sorted_by_road_id() -> None:
    line = sg.LineString([(0, 0), (1, 1)])
    feats = [
        road_feature(2, line, "arterial", 1.0, 1.4),
        road_feature(0, line, "local", 0.5, 1.4),
        road_feature(1, line, "minor", 0.25, 1.4),
    ]
    fc = roads_feature_collection(feats)
    ids = [f["properties"]["road_id"] for f in fc["features"]]
    assert ids == [0, 1, 2]
    assert fc["features"][0]["properties"]["kind"] == "local"


def test_parcels_feature_collection_props_and_no_road_id_key() -> None:
    poly = sg.box(0, 0, 1, 1)
    feats = [
        parcel_feature(1, 7, "wrld_b", "lot", poly),
        parcel_feature(0, 7, "wrld_a", "lot", poly),
    ]
    fc = parcels_feature_collection(feats)
    ids = [f["properties"]["lot_id"] for f in fc["features"]]
    assert ids == [0, 1]
    props = fc["features"][0]["properties"]
    assert props == {
        "lot_id": 0,
        "block_id": 7,
        "world_id": "wrld_a",
        "kind": "lot",
    }
    assert "road_id" not in props


def test_parcel_feature_greenspace_kind() -> None:
    poly = sg.box(0, 0, 1, 1)
    feat = parcel_feature(2, 3, "", "greenspace", poly)
    assert feat["properties"] == {
        "lot_id": 2,
        "block_id": 3,
        "world_id": "",
        "kind": "greenspace",
    }


def test_blocks_feature_collection_sorted_by_block_id() -> None:
    poly = sg.box(0, 0, 1, 1)
    feats = [block_feature(3, 12.0, 5, poly), block_feature(1, 4.0, 0, poly)]
    fc = blocks_feature_collection(feats)
    ids = [f["properties"]["block_id"] for f in fc["features"]]
    assert ids == [1, 3]


def test_landuse_feature_collection_kind_park() -> None:
    poly = sg.box(0, 0, 1, 1)
    feats = [landuse_feature(0, poly)]
    fc = landuse_feature_collection(feats)
    assert fc["features"][0]["properties"] == {"kind": "park", "landuse_id": 0}


# ---------------------------------------------------------------------------
# derive_levels
# ---------------------------------------------------------------------------


def test_derive_levels_three_tier() -> None:
    columns = [
        "world_id",
        "l0_id",
        "l0_sid",
        "l1_id",
        "l1_sid",
        "l2_id",
        "l2_sid",
        "visits",
    ]
    levels, top, sub = derive_levels(columns)
    assert levels == [0, 1, 2]
    assert top == 2
    assert sub == 1


def test_derive_levels_too_few_raises() -> None:
    with pytest.raises(ValueError, match="at least 2"):
        derive_levels(["world_id", "l0_sid"])
