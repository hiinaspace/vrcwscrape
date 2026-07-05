#!/usr/bin/env python
"""Track W (docs/greybox-plan.md) — export the G0 hybrid-island greybox as a
``web/public/*-city*``-schema dataset dir.

Reads the Stage-G0 greybox export (``mapgen/artifacts/r1/greybox/``: island/
arterials/blocks/districts/streets geojson + lots.parquet + greybox_manifest.json)
and the source DR export dir (``web/public/full-nolabs-localmap-island-toponymy/``:
app_points.parquet, worlds_meta.parquet, land.geojson, regions_l*.geojson).
Inverse-affines every geometry from the island frame back into the app/DR
frame and writes a city-schema dataset dir the existing deck.gl app already
knows how to render (``isCity`` mode in ``web/src/Map.jsx``) — so viewing the
hybrid layout needs zero web code changes, only a dataset dir + (today) a
``?data=`` resolver entry (see the script's printed report).

Run from mapgen/::
    uv run python scripts/run_r1_app_export.py

Or with custom paths::
    uv run python scripts/run_r1_app_export.py \\
        --greybox-dir artifacts/r1/greybox \\
        --source-dir ../web/public/full-nolabs-localmap-island-toponymy \\
        --out-dir ../web/public/full-nolabs-localmap-island-chen
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import shapely
import shapely.geometry

from mapgen.r1_app_export import (
    CHEN_STREET_KIND,
    CHEN_STREET_WIDTH_ISLAND_UNITS,
    IslandAffine,
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
    parcels_feature_collection,
    road_feature,
    road_kind,
    road_width_island_units,
    roads_feature_collection,
)

_REPO = Path(__file__).resolve().parents[2]
_DEFAULT_GREYBOX_DIR = Path(__file__).resolve().parents[1] / "artifacts/r1/greybox"
_DEFAULT_SOURCE_DIR = _REPO / "web/public/full-nolabs-localmap-island-toponymy"
_DEFAULT_OUT_DIR = _REPO / "web/public/full-nolabs-localmap-island-chen"

# Track-W acceptance check (docs/greybox-plan.md): inverse-affining
# lots.parquet's x/y must reproduce the source app_points' original DR
# coordinates to ~1e-3 app units.
_ROUNDTRIP_TOLERANCE = 1e-3


def _load_geojson(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _dump_geojson(fc: dict[str, Any], path: Path) -> None:
    with path.open("w") as f:
        json.dump(fc, f, indent=2, sort_keys=True)


def _rel_to_repo(path: Path) -> str:
    """Repo-relative path for manifest provenance, so the manifest doesn't
    embed a machine-specific absolute path (falls back to absolute if the
    given dir happens to live outside this checkout)."""
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(_REPO))
    except ValueError:
        return str(resolved)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Track W: export the G0 greybox as a city-schema app dataset dir"
    )
    parser.add_argument("--greybox-dir", type=Path, default=_DEFAULT_GREYBOX_DIR)
    parser.add_argument("--source-dir", type=Path, default=_DEFAULT_SOURCE_DIR)
    parser.add_argument("--out-dir", type=Path, default=_DEFAULT_OUT_DIR)
    args = parser.parse_args(argv)

    greybox_dir: Path = args.greybox_dir
    source_dir: Path = args.source_dir
    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------
    # Island-frame affine
    # -----------------------------------------------------------------
    greybox_manifest = json.loads((greybox_dir / "greybox_manifest.json").read_text())
    island_frame = greybox_manifest["island_frame"]
    affine = IslandAffine(
        offset_x=float(island_frame["offset_x"]),
        offset_y=float(island_frame["offset_y"]),
        scale=float(island_frame["scale"]),
    )
    print(
        f"Island affine: offset=({affine.offset_x:.4f}, {affine.offset_y:.4f}), "
        f"scale={affine.scale:.4f}"
    )

    # -----------------------------------------------------------------
    # Lots + source app_points join.
    #
    # ``lots.parquet`` (docs/lots-wave-plan.md slice L2) now includes
    # ``kind="greenspace"`` rows (surplus subdivision pieces, ``world_id=""``,
    # no world/building) alongside the occupied ``kind="lot"`` rows. A single
    # global ``lot_id`` is assigned here, over ALL rows (occupied +
    # greenspace) sorted by ``(world_id, district_id, lot_x, lot_y)`` --
    # ``world_id`` alone no longer disambiguates rows since every greenspace
    # row shares ``world_id=""``, so the anchor point breaks the tie
    # deterministically. ``lot_id`` is the one identifier parcels.geojson and
    # app_points.parquet (occupied rows only) share.
    # -----------------------------------------------------------------
    lots = pl.read_parquet(greybox_dir / "lots.parquet").sort(
        ["world_id", "district_id", "lot_x", "lot_y"]
    )
    n_lots = lots.height
    lots = lots.with_columns(pl.Series("lot_id", np.arange(n_lots, dtype=np.int64)))
    occupied = lots.filter(pl.col("kind") == "lot")
    n_greenspace = n_lots - occupied.height
    print(
        f"Loaded {n_lots} lots ({occupied.height} occupied, {n_greenspace} greenspace)"
    )

    source_points = pl.read_parquet(source_dir / "app_points.parquet").rename(
        {"x": "orig_x", "y": "orig_y"}
    )
    joined = occupied.select(
        [
            "world_id",
            "district_id",
            "height",
            "x",
            "y",
            "lot_x",
            "lot_y",
            "lot_id",
            "footprint_wkb",
        ]
    ).join(source_points, on="world_id", how="left")
    missing = joined.filter(pl.col("orig_x").is_null())
    if missing.height:
        raise SystemExit(
            f"{missing.height} lot world_id(s) not found in "
            f"{source_dir}/app_points.parquet (join on world_id) -- source dir "
            f"mismatch. First few: {missing['world_id'].to_list()[:5]}"
        )

    # Acceptance check: inverse-affine(lots.x, lots.y) -- the world's
    # ORIGINAL (unmoved) DR coordinate, never the assigned lot_x/lot_y --
    # must reproduce the source's original DR coordinates
    # (docs/greybox-plan.md Track W).
    err = max_affine_roundtrip_error(
        joined["x"].to_numpy(),
        joined["y"].to_numpy(),
        joined["orig_x"].to_numpy(),
        joined["orig_y"].to_numpy(),
        affine,
    )
    print(f"Affine round-trip max error: {err:.6g} app units")
    if err > _ROUNDTRIP_TOLERANCE:
        raise SystemExit(
            f"Affine round-trip error {err:.6g} exceeds tolerance "
            f"{_ROUNDTRIP_TOLERANCE:g} -- island_frame affine or lots.parquet "
            f"coordinates don't match the source app_points (docs/greybox-plan.md "
            f"Track W acceptance check FAILED)."
        )

    # -----------------------------------------------------------------
    # Assigned position (lot_x/lot_y, inverse-affined -- the world's
    # DISPLACED position, docs/lots-wave-plan.md) + building rects (oriented
    # min-rotated-rectangle of each occupied footprint, inverse-affined into
    # the app frame -- a faithful representation of the new frontage-aligned,
    # possibly lot-guard-clipped footprints).
    # -----------------------------------------------------------------
    xs, ys = invert_xy(joined["lot_x"].to_numpy(), joined["lot_y"].to_numpy(), affine)

    footprints_island = shapely.from_wkb(joined["footprint_wkb"].to_numpy())
    widths, depths, angles = [], [], []
    for footprint in footprints_island:
        footprint_app = invert_geom(footprint, affine)
        rect = oriented_rect(footprint_app)
        widths.append(rect.width)
        depths.append(rect.depth)
        angles.append(rect.angle)

    # All lots' polygons (occupied + greenspace), inverse-affined -- used by
    # parcels.geojson (every lot) and landuse.geojson (greenspace lots only).
    lot_polys_app = [
        invert_geom(poly, affine)
        for poly in shapely.from_wkb(lots["lot_wkb"].to_numpy())
    ]

    # -----------------------------------------------------------------
    # app_points.parquet
    # -----------------------------------------------------------------
    levels, top, sub = derive_levels(source_points.columns)
    level_cols = [
        f"l{i}_{suffix}" for i in levels for suffix in ("id", "name", "sid", "sname")
    ]
    base_cols = ["region", "region_name", "color", "name", "visits"]

    app_points = (
        joined.select(
            [
                "world_id",
                "orig_x",
                "orig_y",
                "district_id",
                "height",
                "lot_id",
                *level_cols,
                *base_cols,
            ]
        )
        .with_columns(
            pl.Series("x", xs, dtype=pl.Float64),
            pl.Series("y", ys, dtype=pl.Float64),
            pl.Series("building_width", widths, dtype=pl.Float64),
            pl.Series("building_depth", depths, dtype=pl.Float64),
            pl.Series("building_angle", angles, dtype=pl.Float64),
        )
        .rename({"height": "building_height", "district_id": "block_id"})
    )
    app_points = app_points.select(
        [
            "world_id",
            "x",
            "y",
            "orig_x",
            "orig_y",
            *level_cols,
            *base_cols,
            "building_angle",
            "building_width",
            "building_depth",
            "building_height",
            "lot_id",
            "block_id",
        ]
    )
    app_points.write_parquet(out_dir / "app_points.parquet")
    print(f"Wrote app_points.parquet ({app_points.height} rows)")

    # -----------------------------------------------------------------
    # worlds_meta.parquet (subset to island worlds)
    # -----------------------------------------------------------------
    worlds_meta = pl.read_parquet(source_dir / "worlds_meta.parquet")
    island_world_ids = set(occupied["world_id"].to_list())
    worlds_meta_out = worlds_meta.filter(
        pl.col("world_id").is_in(island_world_ids)
    ).sort("world_id")
    worlds_meta_out.write_parquet(out_dir / "worlds_meta.parquet")
    print(f"Wrote worlds_meta.parquet ({worlds_meta_out.height} rows)")

    # -----------------------------------------------------------------
    # land.geojson (fitBounds source: the island boundary, app frame)
    # -----------------------------------------------------------------
    island_fc = _load_geojson(greybox_dir / "island.geojson")
    island_poly_island = shapely.geometry.shape(island_fc["features"][0]["geometry"])
    island_poly_app = invert_geom(island_poly_island, affine)
    _dump_geojson(land_feature_collection(island_poly_app), out_dir / "land.geojson")
    island_bbox = island_poly_app.bounds
    print(f"Wrote land.geojson (island bbox app frame: {island_bbox})")

    # -----------------------------------------------------------------
    # regions_l*.geojson (bbox-filtered from the source dir)
    # -----------------------------------------------------------------
    for level in (top, sub):
        name = f"regions_l{level}.geojson"
        src_fc = _load_geojson(source_dir / name)
        out_fc = filter_regions_by_bbox(src_fc, island_bbox)
        _dump_geojson(out_fc, out_dir / name)
        n_kept, n_src = len(out_fc["features"]), len(src_fc["features"])
        print(f"Wrote {name} ({n_kept}/{n_src} features)")

    # -----------------------------------------------------------------
    # roads.geojson / roads_mid.geojson / roads_near.geojson
    #
    # roads.geojson = the full arterials.geojson layer (highway/major/local
    # tiers + core rings) inverse-affined; roads_mid adds the Chen local
    # streets (streets.geojson, kind "minor"); roads_near is identical to
    # roads_mid (v1 simplification per docs/greybox-plan.md Track W -- the
    # app's own client-side per-tier kind filtering in Map.jsx already
    # decides what's actually drawn at each zoom, and the existing city-mesh
    # dataset ships identical LOD content today).
    # -----------------------------------------------------------------
    arterials_fc = _load_geojson(greybox_dir / "arterials.geojson")
    arterial_features = []
    for feat in arterials_fc["features"]:
        tier = feat["properties"]["tier"]
        geom_app = invert_geom(shapely.geometry.shape(feat["geometry"]), affine)
        width = island_length_to_app(road_width_island_units(tier), affine)
        arterial_features.append(
            road_feature(
                feat["properties"]["id"],
                geom_app,
                road_kind(tier),
                width,
                geom_app.length,
            )
        )
    roads_fc = roads_feature_collection(arterial_features)
    _dump_geojson(roads_fc, out_dir / "roads.geojson")
    print(f"Wrote roads.geojson ({len(roads_fc['features'])} features)")

    street_id_offset = len(arterial_features)
    streets_fc = _load_geojson(greybox_dir / "streets.geojson")
    chen_width = island_length_to_app(CHEN_STREET_WIDTH_ISLAND_UNITS, affine)
    chen_features = []
    for feat in streets_fc["features"]:
        geom_app = invert_geom(shapely.geometry.shape(feat["geometry"]), affine)
        chen_features.append(
            road_feature(
                street_id_offset + feat["properties"]["id"],
                geom_app,
                CHEN_STREET_KIND,
                chen_width,
                geom_app.length,
            )
        )
    roads_mid_fc = roads_feature_collection(arterial_features + chen_features)
    _dump_geojson(roads_mid_fc, out_dir / "roads_mid.geojson")
    _dump_geojson(roads_mid_fc, out_dir / "roads_near.geojson")
    n_mid = len(roads_mid_fc["features"])
    print(f"Wrote roads_mid.geojson / roads_near.geojson ({n_mid} features)")

    # -----------------------------------------------------------------
    # parcels.geojson (one polygon per lot -- occupied AND greenspace -- app
    # frame; ``kind`` distinguishes "lot" (a world sits here) from
    # "greenspace" (surplus subdivision piece, no world/building) --
    # docs/lots-wave-plan.md slice L2.
    # -----------------------------------------------------------------
    lot_ids = lots["lot_id"].to_list()
    world_ids = lots["world_id"].to_list()
    district_ids = lots["district_id"].to_list()
    kinds = lots["kind"].to_list()
    parcel_features = [
        {
            "type": "Feature",
            "geometry": shapely.geometry.mapping(geom),
            "properties": {
                "lot_id": lot_id,
                "block_id": block_id,
                "world_id": world_id,
                "kind": kind,
            },
        }
        for lot_id, world_id, block_id, kind, geom in zip(
            lot_ids, world_ids, district_ids, kinds, lot_polys_app, strict=True
        )
    ]
    parcels_fc = parcels_feature_collection(parcel_features)
    _dump_geojson(parcels_fc, out_dir / "parcels.geojson")
    print(f"Wrote parcels.geojson ({len(parcels_fc['features'])} features)")

    # -----------------------------------------------------------------
    # blocks.geojson (G0 districts play the "block" role at this
    # granularity) + landuse.geojson (park districts + greenspace lots)
    # -----------------------------------------------------------------
    districts_fc = _load_geojson(greybox_dir / "districts.geojson")
    block_features = []
    park_polys_app: list[tuple[int, Any]] = []
    for feat in districts_fc["features"]:
        props = feat["properties"]
        geom_app = invert_geom(shapely.geometry.shape(feat["geometry"]), affine)
        block_features.append(
            block_feature(
                props["district_id"], geom_app.area, props["world_count"], geom_app
            )
        )
        if props["kind"] == "park":
            park_polys_app.append((props["district_id"], geom_app))
    blocks_fc = blocks_feature_collection(block_features)
    _dump_geojson(blocks_fc, out_dir / "blocks.geojson")
    print(f"Wrote blocks.geojson ({len(blocks_fc['features'])} features)")

    park_polys_app.sort(key=lambda pair: pair[0])
    # Greenspace lots (docs/lots-wave-plan.md slice L2: surplus subdivision
    # pieces inside otherwise-"fabric" districts) additionally style as
    # "park" landuse -- ``web/src/config.js`` LANDUSE only distinguishes
    # ``kind === "park"`` from everything else (developedColor), and these
    # pocket-park patches are exactly the leftover-subdivision-becomes-
    # greenspace outcome the lots wave intends to make visible.
    greenspace_polys_app = [
        (lot_id, poly)
        for lot_id, kind, poly in zip(lot_ids, kinds, lot_polys_app, strict=True)
        if kind == "greenspace"
    ]
    greenspace_polys_app.sort(key=lambda pair: pair[0])
    landuse_features = [
        landuse_feature(landuse_id, geom)
        for landuse_id, (_district_id, geom) in enumerate(park_polys_app)
    ] + [
        landuse_feature(len(park_polys_app) + i, geom)
        for i, (_lot_id, geom) in enumerate(greenspace_polys_app)
    ]
    landuse_fc = landuse_feature_collection(landuse_features)
    _dump_geojson(landuse_fc, out_dir / "landuse.geojson")
    print(
        f"Wrote landuse.geojson ({len(landuse_fc['features'])} features: "
        f"{len(park_polys_app)} park districts + {len(greenspace_polys_app)} "
        "greenspace lots)"
    )

    # -----------------------------------------------------------------
    # manifest.json (v3-style, city-schema assets)
    # -----------------------------------------------------------------
    manifest = {
        "version": 3,
        "point_count": app_points.height,
        "levels": levels,
        "top": top,
        "sub": sub,
        "assets": {
            "points": "app_points.parquet",
            "meta": "worlds_meta.parquet",
            "land": "land.geojson",
            "regions": [f"regions_l{lvl}.geojson" for lvl in (sub, top)],
            "landuse": "landuse.geojson",
            "parcels": "parcels.geojson",
            "roads": "roads.geojson",
            "roads_mid": "roads_mid.geojson",
            "roads_near": "roads_near.geojson",
            "blocks": "blocks.geojson",
        },
        "layout": "city-chen-v1",
        "building_height_units": "meters",
        "source": {
            "stage": "G0 (docs/greybox-plan.md Track W)",
            "greybox_dir": _rel_to_repo(greybox_dir),
            "source_dir": _rel_to_repo(source_dir),
            "island_frame": island_frame,
            "affine_roundtrip_max_error_app_units": err,
            "n_greenspace_lots": n_greenspace,
        },
    }
    with (out_dir / "manifest.json").open("w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    print("Wrote manifest.json")

    print(f"\nDone. Dataset dir: {out_dir}")


if __name__ == "__main__":
    main()
