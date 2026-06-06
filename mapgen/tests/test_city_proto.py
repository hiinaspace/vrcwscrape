from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import shapely

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mapgen.city_proto import (  # noqa: E402
    CropSpec,
    _assign_worlds_to_lots,
    _grid_layout,
    _render_png_from_svg_inputs,
    _render_svg,
    build_city_proto,
    select_crops,
)
from mapgen.dr_sweep import SweepPreset, _constructor_kwargs  # noqa: E402


def _synthetic_points(n: int = 180) -> pl.DataFrame:
    rng = np.random.default_rng(7)
    a = rng.normal([0.0, 0.0], [1.3, 0.9], size=(n // 2, 2))
    b = rng.normal([12.0, 3.0], [1.1, 1.4], size=(n - n // 2, 2))
    xy = np.vstack([a, b]).astype(np.float64)
    return pl.DataFrame(
        {
            "world_id": [f"wrld_{i:04d}" for i in range(len(xy))],
            "x": xy[:, 0],
            "y": xy[:, 1],
            "l0_sid": np.repeat([0, 1], [n // 2, n - n // 2]),
        }
    )


def test_select_crops_is_deterministic() -> None:
    points = _synthetic_points()
    xy = points.select("x", "y").to_numpy()
    islands = np.r_[np.zeros(90, dtype=int), np.ones(90, dtype=int)]
    first = select_crops(xy, islands, max_crops=4, radius_scale=18.0)
    second = select_crops(xy, islands, max_crops=4, radius_scale=18.0)
    assert first == second
    assert {c.kind for c in first}


def test_grid_lots_assign_every_world_without_duplicates() -> None:
    points = _synthetic_points(72)
    xy = points.select("x", "y").to_numpy()
    crop = CropSpec("crop_test", "density_center", (0.0, 0.0), 20.0, 0)
    roads, blocks, lots, *_ = _grid_layout(
        xy,
        crop,
        next_road_id=1,
        next_block_id=1,
        lot_start=1,
        zone_start=1,
        seed=42,
        max_grade=0.08,
    )
    parcels, buildings = _assign_worlds_to_lots(points, xy, lots, building_scale=0.55)
    assert parcels.height == points.height
    assert parcels["lot_id"].n_unique() == points.height
    assert buildings.height == points.height
    parcel_polys = [shapely.from_wkt(w) for w in parcels["geometry_wkt"].to_list()]
    building_polys = [shapely.from_wkt(w) for w in buildings["geometry_wkt"].to_list()]
    assert all(p.covers(b) for p, b in zip(parcel_polys, building_polys, strict=True))
    assert roads
    assert blocks


def test_render_outputs_exist(tmp_path: Path) -> None:
    points = _synthetic_points(30)
    xy = points.select("x", "y").to_numpy()
    crop = CropSpec("crop_test", "density_center", (0.0, 0.0), 20.0, 0)
    roads, blocks, lots, *_ = _grid_layout(
        xy,
        crop,
        next_road_id=1,
        next_block_id=1,
        lot_start=1,
        zone_start=1,
        seed=42,
        max_grade=0.08,
    )
    parcels, buildings = _assign_worlds_to_lots(points, xy, lots, building_scale=0.55)
    parcel_polys = [shapely.from_wkt(w) for w in parcels["geometry_wkt"].to_list()]
    building_polys = [shapely.from_wkt(w) for w in buildings["geometry_wkt"].to_list()]
    svg = tmp_path / "sheet.svg"
    png = tmp_path / "sheet.png"
    _render_svg(
        svg,
        points=xy,
        roads=roads,
        blocks=blocks,
        parcels=parcel_polys,
        buildings=building_polys,
        title="test",
    )
    _render_png_from_svg_inputs(
        png,
        points=xy,
        roads=roads,
        blocks=blocks,
        parcels=parcel_polys,
        buildings=building_polys,
    )
    assert svg.read_text().startswith("<svg")
    assert png.read_bytes().startswith(b"\x89PNG")


def test_city_proto_writes_manifest_metrics_and_artifacts(tmp_path: Path) -> None:
    points = _synthetic_points(120)
    input_path = tmp_path / "app_points.parquet"
    out_dir = tmp_path / "proto"
    points.write_parquet(input_path)
    metrics = build_city_proto(
        argparse.Namespace(
            in_dir=tmp_path,
            points=input_path,
            out_dir=out_dir,
            max_points=0,
            max_crop_worlds=80,
            max_crops=3,
            crop_radius_scale=20.0,
            island_eps_scale=5.0,
            regional_max_islands=4,
            terrain_max_grade=0.08,
            building_scale=0.55,
            land_raster_max_dim=256,
            seed=42,
        )
    )
    assert metrics["hard_metrics_pass"]
    assert (out_dir / "proto_manifest.json").exists()
    assert (out_dir / "layout_metrics.json").exists()
    assert (out_dir / "roads.geojson").exists()
    assert (out_dir / "parcels.parquet").exists()
    manifest = json.loads((out_dir / "proto_manifest.json").read_text())
    assert manifest["layout"] == "city-proto-game-compressed"


def test_dr_constructor_filters_unsupported_parameters() -> None:
    class Local:
        def __init__(self, n_components: int, n_neighbors: int, random_state: int):
            pass

    kwargs = _constructor_kwargs(
        Local,
        SweepPreset("x", 15, 0.5, 2.0, 10.0, 100, "euclidean", "pca"),
        seed=9,
    )
    assert kwargs == {"n_components": 2, "n_neighbors": 15, "random_state": 9}


def test_assignment_fails_when_lots_are_insufficient() -> None:
    points = _synthetic_points(5)
    xy = points.select("x", "y").to_numpy()
    crop = CropSpec("crop_test", "density_center", (0.0, 0.0), 20.0, 0)
    _roads, _blocks, lots, *_ = _grid_layout(
        xy,
        crop,
        next_road_id=1,
        next_block_id=1,
        lot_start=1,
        zone_start=1,
        seed=42,
        max_grade=0.08,
    )
    with pytest.raises(RuntimeError, match="not enough candidate lots"):
        _assign_worlds_to_lots(points, xy, lots[:2], building_scale=0.55)
