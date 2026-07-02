#!/usr/bin/env python
"""R1 probe — island selection and input layer generation.

Produces all inputs for the R1 regional probe:
  - island_boundary.geojson
  - island_points.parquet
  - fields.npz  (density, height, flow_accum, height_carved, slope)
  - density.png, terrain.png, points.png
  - inputs_manifest.json

Run from mapgen/::
    uv run python scripts/run_r1_island_inputs.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import shapely
import shapely.geometry as sg

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO = Path(__file__).resolve().parents[2]
_DATA_DIR = _REPO / "web/public/full-nolabs-localmap-island-toponymy-city-mesh"
_LAND_GEOJSON = _DATA_DIR / "land.geojson"
_POINTS_PARQUET = _DATA_DIR / "app_points.parquet"
_DEFAULT_OUT = Path(__file__).resolve().parents[1] / "artifacts/r1/inputs"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="R1 probe: island selection + input layer generation"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_DEFAULT_OUT,
        help="Output directory (default: artifacts/r1/inputs)",
    )
    parser.add_argument(
        "--island-index",
        type=int,
        default=None,
        help="Override island polygon index (0-based in land.geojson MultiPolygon)",
    )
    parser.add_argument(
        "--long-side-cells",
        type=int,
        default=384,
        help="Raster cells on the long side of the island bbox (default: 384)",
    )
    parser.add_argument(
        "--carve-k",
        type=float,
        default=0.12,
        help="Hydrological carving strength k (default: 0.12)",
    )
    parser.add_argument(
        "--height-smooth-sigma",
        type=float,
        default=6.0,
        help="Extra Gaussian sigma (cells) for the height channel only, so "
        "D8 drainage forms coherent valleys (default: 6.0)",
    )
    parser.add_argument(
        "--target-vertices",
        type=int,
        default=100,
        help="Simplification vertex budget for boundary polygon (default: 100)",
    )
    args = parser.parse_args(argv)

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading data…")
    from mapgen.r1_inputs import (
        compute_density,
        compute_fields,
        compute_polygon_stats,
        island_mask,
        island_transform,
        load_land_polygons,
        make_raster_grid,
        prepare_boundary,
        raster_coords,
        render_density_png,
        render_points_png,
        render_terrain_png,
        select_island,
        to_island_frame,
    )

    polys = load_land_polygons(_LAND_GEOJSON)
    df = pl.read_parquet(_POINTS_PARQUET)

    # -------------------------------------------------------------------
    # Island selection
    # -------------------------------------------------------------------
    print("Computing per-polygon stats…")
    stats = compute_polygon_stats(polys, df)

    chosen_idx, justification = select_island(stats, override_index=args.island_index)
    print(f"Chosen island index: {chosen_idx}")
    print(f"  {justification}")

    island_poly_src = polys[chosen_idx]

    # -------------------------------------------------------------------
    # Island frame transform
    # -------------------------------------------------------------------
    offset_x, offset_y, scale = island_transform(island_poly_src)
    print(f"Island frame: offset=({offset_x:.4f}, {offset_y:.4f}), scale={scale:.4f}")

    # -------------------------------------------------------------------
    # Filter worlds inside island (using orig_x/orig_y — DR ground truth)
    # Check: orig_x/orig_y alignment with land polygons (compare to x/y)
    # -------------------------------------------------------------------
    xs_src = df["orig_x"].to_numpy()
    ys_src = df["orig_y"].to_numpy()
    in_mask = shapely.contains_xy(island_poly_src, xs_src, ys_src)

    # Sanity: also try x,y to confirm alignment is similar
    in_mask_xy = shapely.contains_xy(
        island_poly_src, df["x"].to_numpy(), df["y"].to_numpy()
    )
    print(
        f"Points in island: orig={in_mask.sum()}, layout_xy={in_mask_xy.sum()} "
        f"(using orig_x/orig_y — DR ground truth)"
    )

    df_island = df.filter(pl.Series(in_mask))
    n_worlds = len(df_island)
    print(f"  {n_worlds} worlds inside island polygon")

    # Transform point coords to island frame (from orig_x/orig_y)
    px_f, py_f = to_island_frame(
        df_island["orig_x"].to_numpy(),
        df_island["orig_y"].to_numpy(),
        offset_x,
        offset_y,
        scale,
    )

    # -------------------------------------------------------------------
    # Boundary
    # -------------------------------------------------------------------
    print("Preparing boundary…")
    # Count dropped holes before cleaning
    n_holes_src = len(island_poly_src.interiors)
    boundary_f, n_verts, simplify_tol = prepare_boundary(
        island_poly_src,
        offset_x,
        offset_y,
        scale,
        target_vertices=args.target_vertices,
    )
    print(
        f"  Boundary: {n_verts} vertices, simplify_tol={simplify_tol:.4f}, "
        f"dropped holes={n_holes_src}"
    )

    # -------------------------------------------------------------------
    # Raster grid
    # -------------------------------------------------------------------
    x0, y0, cell, ncols, nrows = make_raster_grid(
        boundary_f, long_side_cells=args.long_side_cells
    )
    print(f"  Raster grid: {ncols}×{nrows} cells, cell={cell:.4f}")

    X, Y = raster_coords(x0, y0, cell, ncols, nrows)
    mask = island_mask(boundary_f, X, Y)

    # -------------------------------------------------------------------
    # Density raster
    # -------------------------------------------------------------------
    print("Computing density raster…")
    bandwidth = 3.0 * cell  # set here so it's logged
    density, bw_used = compute_density(
        px_f,
        py_f,
        x0,
        y0,
        cell,
        ncols,
        nrows,
        mask,
        bandwidth=bandwidth,
    )

    # -------------------------------------------------------------------
    # Terrain fields
    # -------------------------------------------------------------------
    print("Deriving terrain fields (D8 flow accumulation)…")
    fields = compute_fields(
        density,
        mask,
        carve_k=args.carve_k,
        height_smooth_sigma_cells=args.height_smooth_sigma,
    )

    # -------------------------------------------------------------------
    # Write outputs
    # -------------------------------------------------------------------

    # 1. island_boundary.geojson
    boundary_geojson: dict[str, Any] = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": sg.mapping(boundary_f),
                "properties": {
                    "source_polygon_index": chosen_idx,
                    "offset_x": offset_x,
                    "offset_y": offset_y,
                    "scale": scale,
                    "vertex_count": n_verts,
                    "simplify_tolerance": simplify_tol,
                    "dropped_holes": n_holes_src,
                },
            }
        ],
    }
    boundary_path = out_dir / "island_boundary.geojson"
    with boundary_path.open("w") as f:
        json.dump(boundary_geojson, f, indent=2)
    print(f"Wrote {boundary_path}")

    # 2. island_points.parquet — keep BOTH cluster tiers: the macro layer
    # (r1_macro.build_macro_nodes_hierarchical) seeds cities from l1 and towns
    # from l0.
    point_columns = ["world_id", "l0_id", "l0_name", "l1_id", "l1_name", "visits"]
    island_points = df_island.select(point_columns).with_columns(
        pl.Series("x", px_f),
        pl.Series("y", py_f),
    )
    points_path = out_dir / "island_points.parquet"
    island_points.write_parquet(points_path)
    print(f"Wrote {points_path} ({n_worlds} rows)")

    # 3. fields.npz
    fields_path = out_dir / "fields.npz"
    np.savez_compressed(
        fields_path,
        density=density,
        height=fields["height"],
        flow_accum=fields["flow_accum"],
        height_carved=fields["height_carved"],
        slope=fields["slope"],
        x0=np.float64(x0),
        y0=np.float64(y0),
        cell=np.float64(cell),
    )
    print(f"Wrote {fields_path}")

    # 4. Quicklook PNGs
    print("Rendering quicklook PNGs…")
    render_density_png(density, out_dir / "density.png")
    render_terrain_png(
        fields["height_carved"],
        fields["flow_accum"],
        out_dir / "terrain.png",
        island_mask_arr=mask,
    )
    render_points_png(px_f, py_f, boundary_f, out_dir / "points.png")
    print("  density.png, terrain.png, points.png")

    # 5. inputs_manifest.json
    manifest: dict[str, Any] = {
        "island_selection": {
            "stats_table": stats,
            "chosen_index": chosen_idx,
            "justification": justification,
        },
        "island_frame": {
            "offset_x": offset_x,
            "offset_y": offset_y,
            "scale": scale,
            "long_side_units": 200.0,
        },
        "boundary": {
            "source_polygon_index": chosen_idx,
            "vertex_count": n_verts,
            "simplify_tolerance": simplify_tol,
            "dropped_holes": n_holes_src,
            "target_vertices": args.target_vertices,
        },
        "points": {
            "n_worlds": n_worlds,
            "coordinate": "orig_x/orig_y (DR ground truth)",
            "columns": [*point_columns, "x", "y"],
        },
        "raster": {
            "x0": x0,
            "y0": y0,
            "cell": cell,
            "ncols": ncols,
            "nrows": nrows,
            "long_side_cells": args.long_side_cells,
        },
        "kde": {
            "method": "histogram + gaussian_filter",
            "bandwidth_island_units": bw_used,
            "bandwidth_cells": bw_used / cell,
        },
        "terrain": {
            "flow_accum_method": (
                "D8 with epsilon-ramp pit/flat resolution "
                "(epsilon proportional to distance from boundary)"
            ),
            "carve_k": args.carve_k,
            "height_smooth_sigma_cells": args.height_smooth_sigma,
            "carve_formula": (
                "height_carved = height - k * normalize(log1p(flow_accum)),"
                " clipped >= 0"
            ),
        },
    }
    manifest_path = out_dir / "inputs_manifest.json"
    with manifest_path.open("w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote {manifest_path}")

    # -------------------------------------------------------------------
    # Sanity summary
    # -------------------------------------------------------------------
    print("\n--- Sanity summary ---")
    print(f"  density max={density.max():.4f}, min inside={density[mask].min():.6f}")
    print(f"  height max={fields['height'].max():.4f}")
    fa = fields["flow_accum"]
    print(f"  flow_accum max={fa.max()}, p99={np.percentile(fa, 99):.0f}")
    hc = fields["height_carved"]
    print(f"  height_carved max={hc.max():.4f}, min(inside)={hc[mask].min():.6f}")
    print(f"  slope max={fields['slope'].max():.4f}")
    print("Done.")


if __name__ == "__main__":
    main()
