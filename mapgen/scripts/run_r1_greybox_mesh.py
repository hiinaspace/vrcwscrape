#!/usr/bin/env python
"""R1 stage G1 — greybox mesh bake driver: G0 export -> OBJ/MTL for Unity.

Reads a Stage-G0 greybox export (``run_r1_hybrid.py --greybox-out``:
``island.geojson``, ``arterials.geojson``, ``blocks.geojson``,
``districts.geojson``, ``streets.geojson``, ``lots.parquet``,
``greybox_manifest.json``) and bakes it into an OBJ+MTL greybox mesh — Unity
imports OBJ natively, no glTF dependency for the tracer bullet
(``docs/greybox-plan.md`` Stage G1). All geometry assembly is
``mapgen.r1_mesh`` (pure); this script only does the IO: GeoJSON/parquet ->
:mod:`mapgen.r1_mesh` record types -> OBJ/MTL/CSV/manifest files.

Outputs to mapgen/artifacts/r1/greybox_mesh/ (or --out-dir):
  greybox.obj               — the baked mesh (Y-up meters)
  greybox.mtl               — flat per-material diffuse colors
  greybox.obj.gz             — gzip of the OBJ, ONLY written if the raw OBJ
                               exceeds 50 MB (Unity side unpacks; no git-lfs)
  labels.csv                — world_id, name, x_m, z_m, roof_y_m
  greybox_mesh_manifest.json — scale, group/material inventory, bounds, stats

Run from mapgen/::
    uv run python scripts/run_r1_greybox_mesh.py
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import sys
import time
from pathlib import Path
from typing import Any

import polars as pl
import shapely.geometry as sg
from shapely import wkb as shapely_wkb

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "src") not in sys.path:
    sys.path.insert(0, str(_REPO / "src"))

from mapgen.r1_mesh import (  # noqa: E402
    DEFAULT_GROUND_DEPTH,
    DEFAULT_METERS_PER_UNIT,
    DEFAULT_PARK_Y,
    DEFAULT_STREET_WIDTHS,
    DEFAULT_STREET_Y,
    DEFAULT_WATER_MARGIN_FRAC,
    DEFAULT_WATER_Y,
    LotRecord,
    RoadSegment,
    assemble_mesh,
    build_label_rows,
    build_mesh_manifest,
    materials_to_mtl,
    mesh_materials,
    mesh_to_obj,
)

_DEFAULT_IN = _REPO / "artifacts/r1/greybox"
_DEFAULT_OUT = _REPO / "artifacts/r1/greybox_mesh"

# Above this raw OBJ size (bytes), also write a gzip copy alongside it
# (docs/greybox-plan.md Stage G1: "commit ... if < ~50 MB, otherwise gzip").
_GZIP_THRESHOLD_BYTES = 50 * 1024 * 1024

_OBJ_NAME = "greybox.obj"
_MTL_NAME = "greybox.mtl"
_LABELS_NAME = "labels.csv"
_MANIFEST_NAME = "greybox_mesh_manifest.json"


# ---------------------------------------------------------------------------
# G0 export readers
# ---------------------------------------------------------------------------


def _read_geojson(path: Path) -> list[dict[str, Any]]:
    """Load a GeoJSON FeatureCollection's ``features`` list."""
    data = json.loads(path.read_text())
    return list(data["features"])


def _read_island(in_dir: Path) -> sg.Polygon:
    features = _read_geojson(in_dir / "island.geojson")
    if not features:
        raise ValueError(f"{in_dir / 'island.geojson'} has no features")
    geom = sg.shape(features[0]["geometry"])
    if geom.geom_type != "Polygon":
        raise ValueError(f"island.geojson feature 0 is {geom.geom_type}, not Polygon")
    return geom


def _read_arterials(in_dir: Path) -> list[RoadSegment]:
    features = _read_geojson(in_dir / "arterials.geojson")
    segments = []
    for feat in features:
        geom = sg.shape(feat["geometry"])
        if geom.geom_type != "LineString":
            continue
        tier = str(feat["properties"]["tier"])
        segments.append(RoadSegment(geometry=geom, tier=tier))
    return segments


def _read_streets(in_dir: Path) -> list[RoadSegment]:
    """Per-block Chen local streets -- no tier property; all tier ``"street"``."""
    features = _read_geojson(in_dir / "streets.geojson")
    segments = []
    for feat in features:
        geom = sg.shape(feat["geometry"])
        if geom.geom_type != "LineString":
            continue
        segments.append(RoadSegment(geometry=geom, tier="street"))
    return segments


def _read_districts(in_dir: Path) -> tuple[dict[int, int], list[sg.Polygon]]:
    """Returns ``(district_id -> block_id, park polygons sorted by district_id)``."""
    features = _read_geojson(in_dir / "districts.geojson")
    features.sort(key=lambda feat: int(feat["properties"]["district_id"]))
    district_to_block: dict[int, int] = {}
    park_polys: list[sg.Polygon] = []
    for feat in features:
        props = feat["properties"]
        district_id = int(props["district_id"])
        block_id = int(props["block_id"])
        district_to_block[district_id] = block_id
        if props.get("kind") == "park":
            geom = sg.shape(feat["geometry"])
            if geom.geom_type == "Polygon":
                park_polys.append(geom)
    return district_to_block, park_polys


def _read_lots(
    in_dir: Path, district_to_block: dict[int, int]
) -> tuple[list[LotRecord], int]:
    """Decode ``lots.parquet`` into :class:`LotRecord`\\ s.

    ``kind="greenspace"`` rows (docs/lots-wave-plan.md slice L2: surplus
    subdivision pieces, ``world_id=""``, no world/building) are skipped
    entirely -- their empty ``footprint`` would be dropped by
    :func:`~mapgen.r1_mesh.build_building_groups` anyway (no building), but
    excluding them here also keeps them out of ``labels.csv`` (no
    roof label belongs on a lot with no building). ``LotRecord.x``/``y`` are
    populated from ``lot_x``/``lot_y`` -- the world's ASSIGNED lot anchor,
    not its original (possibly displaced-away-from) coordinate -- so
    ``labels.csv`` roof-label positions sit over the actual baked building.

    A lot whose ``district_id`` has no known block (should not happen given
    ``run_r1_hybrid.py``'s greybox export invariants, but this is the IO
    boundary reading someone else's export file) is skipped and counted
    rather than raising. Returns ``(lots, n_unmapped_district)``.
    """
    df = pl.read_parquet(in_dir / "lots.parquet")
    lots: list[LotRecord] = []
    n_unmapped = 0
    for row in df.iter_rows(named=True):
        if row["kind"] == "greenspace":
            continue
        district_id = int(row["district_id"])
        block_id = district_to_block.get(district_id)
        if block_id is None:
            n_unmapped += 1
            continue
        footprint = shapely_wkb.loads(bytes(row["footprint_wkb"]))
        lots.append(
            LotRecord(
                world_id=str(row["world_id"]),
                block_id=block_id,
                footprint=footprint,
                height=float(row["height"]),
                name=row["name"],
                x=float(row["lot_x"]),
                y=float(row["lot_y"]),
            )
        )
    return lots, n_unmapped


# ---------------------------------------------------------------------------
# Bake driver
# ---------------------------------------------------------------------------


def bake_greybox_mesh(
    in_dir: Path,
    out_dir: Path,
    *,
    meters_per_unit: float = DEFAULT_METERS_PER_UNIT,
    street_widths: dict[str, float] | None = None,
    street_y: float = DEFAULT_STREET_Y,
    park_y: float = DEFAULT_PARK_Y,
    ground_depth: float = DEFAULT_GROUND_DEPTH,
    water_y: float = DEFAULT_WATER_Y,
    water_margin_frac: float = DEFAULT_WATER_MARGIN_FRAC,
) -> dict[str, Any]:
    """Read the Stage-G0 export in ``in_dir``, bake, write to ``out_dir``.

    Writes ``greybox.obj``, ``greybox.mtl``, ``labels.csv``,
    ``greybox_mesh_manifest.json`` (and ``greybox.obj.gz`` if the OBJ exceeds
    ``_GZIP_THRESHOLD_BYTES``). Returns the manifest dict.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    widths = dict(DEFAULT_STREET_WIDTHS if street_widths is None else street_widths)

    island = _read_island(in_dir)
    roads = _read_arterials(in_dir) + _read_streets(in_dir)
    district_to_block, parks = _read_districts(in_dir)
    lots, n_unmapped_lots = _read_lots(in_dir, district_to_block)

    mesh, stats = assemble_mesh(
        island=island,
        lots=lots,
        roads=roads,
        parks=parks,
        meters_per_unit=meters_per_unit,
        street_widths=widths,
        street_y=street_y,
        park_y=park_y,
        ground_depth=ground_depth,
        water_y=water_y,
        water_margin_frac=water_margin_frac,
    )

    obj_text = mesh_to_obj(mesh, mtllib=_MTL_NAME)
    mtl_text = materials_to_mtl(mesh_materials(mesh))

    obj_path = out_dir / _OBJ_NAME
    obj_path.write_text(obj_text)
    (out_dir / _MTL_NAME).write_text(mtl_text)

    obj_size = obj_path.stat().st_size
    gz_path: Path | None = None
    if obj_size > _GZIP_THRESHOLD_BYTES:
        gz_path = out_dir / f"{_OBJ_NAME}.gz"
        with (
            gz_path.open("wb") as f,
            gzip.GzipFile(fileobj=f, mode="wb", mtime=0) as gz,
        ):
            gz.write(obj_text.encode("utf-8"))

    label_rows = build_label_rows(lots, meters_per_unit)
    with (out_dir / _LABELS_NAME).open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["world_id", "name", "x_m", "z_m", "roof_y_m"])
        for world_id, name, x_m, z_m, roof_y_m in label_rows:
            writer.writerow(
                [world_id, name, f"{x_m:.4f}", f"{z_m:.4f}", f"{roof_y_m:.4f}"]
            )

    minx, miny, maxx, maxy = island.bounds
    bounds_m = (
        minx * meters_per_unit,
        miny * meters_per_unit,
        maxx * meters_per_unit,
        maxy * meters_per_unit,
    )
    manifest = build_mesh_manifest(
        mesh,
        stats,
        meters_per_unit=meters_per_unit,
        bounds_m=bounds_m,
        street_widths=widths,
    )
    manifest["source"] = {
        "in_dir": str(in_dir),
        "n_lots_read": len(lots),
        "n_lots_unmapped_district": n_unmapped_lots,
        "n_road_segments_read": len(roads),
        "n_park_polygons_read": len(parks),
    }
    manifest["outputs"] = {
        "obj": _OBJ_NAME,
        "obj_gz": f"{_OBJ_NAME}.gz" if gz_path is not None else None,
        "obj_bytes": obj_size,
        "mtl": _MTL_NAME,
        "labels_csv": _LABELS_NAME,
    }
    manifest["n_triangles_total"] = sum(len(g.faces) for g in mesh.groups)
    with (out_dir / _MANIFEST_NAME).open("w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    return manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R1 stage G1: bake the Stage-G0 greybox export into OBJ/MTL"
    )
    parser.add_argument("--in-dir", type=Path, default=_DEFAULT_IN)
    parser.add_argument("--out-dir", type=Path, default=_DEFAULT_OUT)
    parser.add_argument(
        "--meters-per-unit", type=float, default=DEFAULT_METERS_PER_UNIT
    )
    parser.add_argument(
        "--street-width-highway", type=float, default=DEFAULT_STREET_WIDTHS["highway"]
    )
    parser.add_argument(
        "--street-width-major", type=float, default=DEFAULT_STREET_WIDTHS["major"]
    )
    parser.add_argument(
        "--street-width-local", type=float, default=DEFAULT_STREET_WIDTHS["local"]
    )
    parser.add_argument(
        "--street-width-ring", type=float, default=DEFAULT_STREET_WIDTHS["ring"]
    )
    parser.add_argument(
        "--street-width-street", type=float, default=DEFAULT_STREET_WIDTHS["street"]
    )
    parser.add_argument("--street-y", type=float, default=DEFAULT_STREET_Y)
    parser.add_argument("--park-y", type=float, default=DEFAULT_PARK_Y)
    parser.add_argument("--ground-depth", type=float, default=DEFAULT_GROUND_DEPTH)
    parser.add_argument("--water-y", type=float, default=DEFAULT_WATER_Y)
    parser.add_argument(
        "--water-margin-frac", type=float, default=DEFAULT_WATER_MARGIN_FRAC
    )
    args = parser.parse_args(argv)

    street_widths = {
        "highway": args.street_width_highway,
        "major": args.street_width_major,
        "local": args.street_width_local,
        "ring": args.street_width_ring,
        "street": args.street_width_street,
    }

    t0 = time.time()
    manifest = bake_greybox_mesh(
        args.in_dir,
        args.out_dir,
        meters_per_unit=args.meters_per_unit,
        street_widths=street_widths,
        street_y=args.street_y,
        park_y=args.park_y,
        ground_depth=args.ground_depth,
        water_y=args.water_y,
        water_margin_frac=args.water_margin_frac,
    )
    runtime_s = round(time.time() - t0, 2)

    print("\n--- Stage G1 greybox mesh bake ---")
    print(f"  meters_per_unit: {manifest['meters_per_unit']}")
    print(f"  triangles total: {manifest['n_triangles_total']}")
    print(f"  obj bytes:       {manifest['outputs']['obj_bytes']}")
    if manifest["outputs"]["obj_gz"]:
        print(f"  obj.gz written:  {manifest['outputs']['obj_gz']}")
    print("  groups:")
    for name, tri_count in sorted(manifest["groups"].items()):
        print(f"    {name}: {tri_count} tris")
    stats = manifest["stats"]
    print(
        f"  buildings: {stats['buildings_ok']} ok, "
        f"{stats['buildings_skipped']} skipped, "
        f"{stats['buildings_holes_simplified']} holes-simplified"
    )
    print(f"  roads: {stats['roads_ok']} ok, {stats['roads_skipped']} skipped")
    print(f"  parks: {stats['parks_ok']} ok, {stats['parks_skipped']} skipped")
    print(f"  runtime: {runtime_s}s")
    print(f"\nOutputs in: {args.out_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
