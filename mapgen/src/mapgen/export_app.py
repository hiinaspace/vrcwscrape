"""Stage E: export the interactive web app's static assets.

Turns the toponymy hierarchy artifacts + the worlds parquet into the three files
the `web/` React app loads:

  * app_points.parquet  -- one row per world: world_id, x, y, l{0..N}_id/_name,
    an integer `region` (the coarsest/continent cluster) and a stable hex `color`.
  * regions_l{lvl}.geojson  -- background polygons per cluster at a hierarchy
    level, colored by their parent continent so sub-regions share the continent
    hue. Defaults to rasterized masks for speed, with exact Shapely unions still
    available for comparison.
  * worlds_meta.parquet  -- the subset of worlds_search.parquet restricted to the
    sidebar fields, with tags cleaned for display.

Run on the GPU/ollama host where the artifacts live, then copy web/public/* back
locally.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import polars as pl
import shapely

from mapgen.common import clean_tags
from mapgen.land import write_land_geojson
from mapgen.raster_poly import estimate_cell_size, raster_region_polys
from mapgen.regions import _distinct_colors, _median_nn, _region_polys

# Sidebar fields pulled from worlds_search.parquet (first-pass; no visits-over-time
# or thumbnails yet).
META_COLS = [
    "world_id",
    "name",
    "description",
    "author_name",
    "visits",
    "favorites",
    "created_at",
    "updated_at",
    "pc_size_mb",
    "quest_size_mb",
    "scrape_status",
]


def _rgba_to_hex(c) -> str:
    r, g, b = (int(round(float(v) * 255)) for v in c[:3])
    return f"#{r:02x}{g:02x}{b:02x}"


# Generic VRChat-domain suffixes that add no information on a map label.
_LABEL_SUFFIXES = {
    "world",
    "worlds",
    "hub",
    "hubs",
    "hangout",
    "hangouts",
    "space",
    "spaces",
    "zone",
    "zones",
    "place",
    "places",
    "area",
    "areas",
    "map",
    "maps",
}


def _short_label(s: str | None, max_words: int = 3, max_chars: int = 28) -> str:
    """Clamp an LLM topic name to a terse map label: drop trailing domain
    suffixes (…Hubs/…Worlds/…Hangouts), then cap words/chars."""
    words = (s or "").strip().split()
    while len(words) > 1 and words[-1].strip(".,").lower() in _LABEL_SUFFIXES:
        words.pop()
    if len(words) > max_words:
        words = words[:max_words]
    out = " ".join(words)
    if len(out) > max_chars:
        out = out[:max_chars].rstrip() + "…"
    return out


def _discover_layers(topo_dir: Path) -> list[Path]:
    layers = sorted(
        topo_dir.glob("layer_*.parquet"),
        key=lambda p: int(p.stem.split("_")[1]),
    )
    if not layers:
        raise SystemExit(f"no layer_*.parquet in {topo_dir}")
    return layers


def _export_geojson(
    xy: np.ndarray,
    cluster_id: np.ndarray,
    cids: list[int],
    color_by_cid: dict[int, str],
    label_by_cid: dict[int, str],
    region_by_cid: dict[int, int],
    buffer_r: float,
    out_path: Path,
    *,
    method: str = "raster",
    raster_cell_size: float | None = None,
    raster_close_cells: int = 1,
    raster_simplify_cells: float = 0.75,
    raster_smooth_cells: float = 0.0,
    raster_min_area_cells: float = 4.0,
) -> None:
    if method == "union":
        polys = _region_polys(xy, cluster_id, cids, buffer_r=buffer_r)
    elif method == "raster":
        if raster_cell_size is None:
            raise ValueError("raster_cell_size is required for raster regions")
        polys = raster_region_polys(
            xy,
            cluster_id,
            cids,
            cell_size=raster_cell_size,
            radius=buffer_r,
            close_cells=raster_close_cells,
            simplify_cells=raster_simplify_cells,
            smooth_cells=raster_smooth_cells,
            min_area_cells=raster_min_area_cells,
        )
    else:
        raise ValueError(f"unknown region polygon method {method!r}")
    feats = []
    for c, geom in polys.items():
        feats.append(
            {
                "type": "Feature",
                "properties": {
                    "cluster_id": int(c),
                    "label": label_by_cid.get(c, str(c)),
                    "color": color_by_cid.get(c, "#888888"),
                    # parent continent id so the web app can recolor by a palette
                    "region": int(region_by_cid.get(c, -1)),
                    "size": int((cluster_id == c).sum()),
                },
                "geometry": json.loads(shapely.to_geojson(geom)),
            }
        )
    out_path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    print(f"  wrote {out_path} ({len(feats)} regions)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--coords", type=Path, required=True, help="coords_umap.parquet (world_id,x,y)"
    )
    ap.add_argument(
        "--topo-dir",
        type=Path,
        required=True,
        help="toponymy out dir with layer_0..N.parquet",
    )
    ap.add_argument(
        "--worlds", type=Path, required=True, help="worlds_search.parquet (metadata)"
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="destination (e.g. web/public) for the app assets",
    )
    ap.add_argument(
        "--region-level",
        type=int,
        default=None,
        help="hierarchy level used as the point 'region'/continent (default: "
        "coarsest layer index)",
    )
    ap.add_argument(
        "--geojson-levels",
        type=int,
        nargs="*",
        default=None,
        help="levels to emit region polygons for (default: the two coarsest)",
    )
    ap.add_argument("--buffer-scale", type=float, default=3.5)
    ap.add_argument(
        "--buffer-scale-level",
        action="append",
        nargs=2,
        metavar=("LEVEL", "SCALE"),
        default=[],
        help=(
            "override --buffer-scale for a specific geojson hierarchy level; "
            "repeatable, e.g. --buffer-scale-level 1 1.2"
        ),
    )
    ap.add_argument(
        "--region-polygon-method",
        choices=("raster", "union"),
        default="raster",
        help="raster is approximate and fast; union is exact Shapely point buffers",
    )
    ap.add_argument("--region-raster-max-dim", type=int, default=2048)
    ap.add_argument("--region-raster-nn-cells", type=float, default=2.0)
    ap.add_argument("--region-raster-close-cells", type=int, default=1)
    ap.add_argument("--region-raster-simplify-cells", type=float, default=0.75)
    ap.add_argument("--region-raster-smooth-cells", type=float, default=0.0)
    ap.add_argument("--region-raster-min-area-cells", type=float, default=4.0)
    ap.add_argument("--land-method", choices=("raster", "alpha"), default="raster")
    ap.add_argument(
        "--land-edge-quantile",
        type=float,
        default=0.99,
        help="Delaunay edge quantile used as land.geojson alpha-shape threshold",
    )
    ap.add_argument(
        "--land-max-edge",
        type=float,
        default=None,
        help="explicit land.geojson alpha-shape edge threshold; overrides quantile",
    )
    ap.add_argument(
        "--land-simplify-scale",
        type=float,
        default=0.35,
        help="land.geojson simplification tolerance as a fraction of max edge",
    )
    ap.add_argument(
        "--land-min-area-scale",
        type=float,
        default=0.0,
        help="drop land islands smaller than this * max_edge^2; 0 keeps all",
    )
    ap.add_argument(
        "--land-expand-scale",
        type=float,
        default=0.25,
        help="buffer final land polygons by this fraction of max edge",
    )
    ap.add_argument("--land-expand-quad-segs", type=int, default=2)
    ap.add_argument("--land-raster-max-dim", type=int, default=2048)
    ap.add_argument("--land-raster-nn-cells", type=float, default=2.0)
    ap.add_argument("--land-raster-dilate-cells", type=int, default=5)
    ap.add_argument("--land-raster-close-cells", type=int, default=2)
    ap.add_argument("--land-raster-simplify-cells", type=float, default=0.75)
    ap.add_argument("--land-raster-smooth-cells", type=float, default=0.0)
    ap.add_argument("--land-raster-min-area-cells", type=float, default=4.0)
    ap.add_argument(
        "--embeddings",
        type=Path,
        default=None,
        help="embeddings.npy (row-aligned to --embed-meta); enables soft-assigning "
        "noise points to the nearest continent so the map has no 'Unlabelled' blob",
    )
    ap.add_argument(
        "--embed-meta",
        type=Path,
        default=None,
        help="embed_meta.parquet giving the embeddings.npy row order (world_id)",
    )
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    try:
        buffer_scale_by_level = {
            int(level): float(scale) for level, scale in args.buffer_scale_level
        }
    except ValueError as err:
        raise SystemExit(
            "--buffer-scale-level expects integer LEVEL and numeric SCALE"
        ) from err

    layer_paths = _discover_layers(args.topo_dir)
    n_layers = len(layer_paths)
    region_level = args.region_level if args.region_level is not None else n_layers - 1
    geojson_levels = args.geojson_levels
    if geojson_levels is None:
        # the two coarsest, low to high (so l2 then l3 by name)
        geojson_levels = sorted({max(region_level - 1, 0), region_level})
    print(
        f"{n_layers} hierarchy layers; region_level={region_level}; "
        f"geojson levels={geojson_levels}"
    )

    # --- assemble app_points: coords + every layer's id+name ----------------
    coords = pl.read_parquet(args.coords).select("world_id", "x", "y")
    points = coords
    for li, lp in enumerate(layer_paths):
        layer = pl.read_parquet(lp)
        sel = {"world_id": layer["world_id"]}
        if "cluster_id" in layer.columns:
            sel[f"l{li}_id"] = layer["cluster_id"]
        if "topic_name" in layer.columns:
            sel[f"l{li}_name"] = layer["topic_name"]
        points = points.join(pl.DataFrame(sel), on="world_id", how="left")

    # the topo subset = worlds clustered at the region level (rest are coords-only)
    region_id_col = f"l{region_level}_id"
    points = points.filter(pl.col(region_id_col).is_not_null())

    # Embeddings aligned to the current points row order. The toponymy clusterer
    # leaves ~half the worlds as noise (-1) at every level, so we soft-assign each
    # noise world to its nearest cluster centroid in embedding space (as the static
    # regions map does). Doing this at EVERY level gives each world a full path of
    # real cluster names AND lets background polygons fill the whole region (not
    # just the core), fixing the sparse-shading look at far zoom.
    emb_aligned = None
    if args.embeddings is not None and args.embed_meta is not None:
        from mapgen.common import l2_normalize

        emb_all = l2_normalize(np.load(args.embeddings))
        emb_meta = pl.read_parquet(args.embed_meta).select("world_id")
        row_of = {w: i for i, w in enumerate(emb_meta["world_id"].to_list())}
        emb_aligned = emb_all[
            np.array([row_of[w] for w in points["world_id"].to_list()])
        ]
    else:
        print("  no --embeddings: noise stays -1 (Unlabelled), polygons core-only")

    def _soft(raw: np.ndarray) -> np.ndarray:
        from mapgen.regions import _assign_regions

        return _assign_regions(emb_aligned, raw) if emb_aligned is not None else raw

    # per hierarchy level: soft cluster id (full coverage) + short name per cluster
    level_name: dict[int, dict[int, str]] = {}
    soft_cols: list[pl.Series] = []
    for li in range(n_layers):
        id_col, name_col = f"l{li}_id", f"l{li}_name"
        if id_col not in points.columns:
            continue
        raw = points[id_col].fill_null(-1).cast(pl.Int64).to_numpy()
        soft = _soft(raw)
        nm: dict[int, str] = {}
        if name_col in points.columns:
            tmp = (
                pl.DataFrame({"_c": raw, "_n": points[name_col].to_list()})
                .filter(pl.col("_c") != -1)
                .group_by("_c")
                .agg(pl.col("_n").drop_nulls().mode().first().alias("_nm"))
            )
            nm = {
                int(r["_c"]): _short_label(r["_nm"])
                for r in tmp.iter_rows(named=True)
                if r["_nm"]
            }
        level_name[li] = nm
        soft_cols.append(pl.Series(f"l{li}_sid", soft).cast(pl.Int32))
        soft_cols.append(pl.Series(f"l{li}_sname", [nm.get(int(s), "") for s in soft]))
    points = points.with_columns(*soft_cols)
    n_noise = int((points[region_id_col].fill_null(-1).cast(pl.Int64) == -1).sum())
    if emb_aligned is not None:
        print(f"  soft-assigned {n_noise:,} noise worlds per level")

    # region = soft continent; stable color palette + name from it
    points = points.with_columns(pl.col(f"l{region_level}_sid").alias("region"))
    region_ids = sorted(int(r) for r in points["region"].unique().to_list())
    palette = _distinct_colors(len(region_ids))
    region_color = {
        rid: _rgba_to_hex(col) for rid, col in zip(region_ids, palette, strict=True)
    }
    region_name = level_name.get(region_level, {})
    points = points.with_columns(
        pl.col("region")
        .replace_strict(region_name, default="(mixed)")
        .alias("region_name"),
        pl.col("region").replace_strict(region_color, default="#888888").alias("color"),
    )

    # world title (deepest-zoom labels) + visits (label priority: popular worlds
    # get labeled first when space is tight) come from the points table
    worlds = pl.read_parquet(args.worlds)
    points = points.join(
        worlds.select("world_id", "name", "visits"), on="world_id", how="left"
    ).with_columns(pl.col("visits").fill_null(0))

    out_points = args.out_dir / "app_points.parquet"
    points.write_parquet(out_points)
    n = points.height
    print(f"  wrote {out_points} ({n:,} points, {len(region_ids)} regions)")
    levels_out = [li for li in range(n_layers) if f"l{li}_sid" in points.columns]
    out_manifest = args.out_dir / "manifest.json"
    out_manifest.write_text(
        json.dumps(
            {
                "version": 1,
                "point_count": n,
                "levels": levels_out,
                "top": levels_out[-1],
                "sub": levels_out[-2] if len(levels_out) >= 2 else levels_out[-1],
                "assets": {
                    "points": "app_points.parquet",
                    "meta": "worlds_meta.parquet",
                    "land": "land.geojson",
                    "regions": [f"regions_l{lvl}.geojson" for lvl in geojson_levels],
                },
            }
        )
    )
    print(f"  wrote {out_manifest}")

    # --- land polygon: static alpha shape of all worlds ----------------------
    xy = points.select("x", "y").to_numpy().astype("float64")
    write_land_geojson(
        xy,
        args.out_dir / "land.geojson",
        method=args.land_method,
        edge_quantile=args.land_edge_quantile,
        max_edge=args.land_max_edge,
        simplify_scale=args.land_simplify_scale,
        min_area_scale=args.land_min_area_scale,
        expand_scale=args.land_expand_scale,
        expand_quad_segs=args.land_expand_quad_segs,
        raster_max_dim=args.land_raster_max_dim,
        raster_nn_cells=args.land_raster_nn_cells,
        raster_dilate_cells=args.land_raster_dilate_cells,
        raster_close_cells=args.land_raster_close_cells,
        raster_simplify_cells=args.land_raster_simplify_cells,
        raster_smooth_cells=args.land_raster_smooth_cells,
        raster_min_area_cells=args.land_raster_min_area_cells,
    )

    # --- region polygons per requested level (from soft ids = full coverage) ---
    nn = _median_nn(xy)
    region_raster_cell = estimate_cell_size(
        xy,
        max_dim=args.region_raster_max_dim,
        median_nn=nn,
        nn_cells=args.region_raster_nn_cells,
    )
    if args.region_polygon_method == "raster":
        print(
            f"  region raster: cell_size={region_raster_cell:.5f}, "
            f"default_buffer={nn * args.buffer_scale:.5f}"
        )
    region_soft = points["region"].to_numpy()
    for lvl in geojson_levels:
        # polygons from CORE (raw) cluster ids only -> coherent 2D blobs for clean
        # continent outlines + label placement. Full-area coloring is the web app's
        # job (per-world Voronoi cells); soft ids would scatter into 2D specks here.
        id_col = f"l{lvl}_id"
        if id_col not in points.columns:
            print(f"  !! level {lvl} missing ({id_col}); skipping geojson")
            continue
        cluster_id = points[id_col].fill_null(-1).cast(pl.Int64).to_numpy()
        cids = sorted(int(c) for c in set(cluster_id.tolist()) if c != -1)
        label_by_cid = level_name.get(lvl, {})
        # color each cluster by its modal parent continent (nested coloring)
        color_by_cid: dict[int, str] = {}
        region_by_cid: dict[int, int] = {}
        modal = (
            pl.DataFrame({"_c": cluster_id, "_r": region_soft})
            .group_by("_c")
            .agg(pl.col("_r").mode().first().alias("_rr"))
        )
        for r in modal.iter_rows(named=True):
            region_by_cid[int(r["_c"])] = int(r["_rr"])
            color_by_cid[int(r["_c"])] = region_color.get(int(r["_rr"]), "#888888")
        level_buffer_scale = buffer_scale_by_level.get(lvl, args.buffer_scale)
        level_buffer_r = nn * level_buffer_scale
        if buffer_scale_by_level:
            print(
                f"  level {lvl}: buffer_scale={level_buffer_scale:g}, "
                f"buffer={level_buffer_r:.5f}"
            )
        _export_geojson(
            xy,
            cluster_id,
            cids,
            color_by_cid,
            label_by_cid,
            region_by_cid,
            level_buffer_r,
            args.out_dir / f"regions_l{lvl}.geojson",
            method=args.region_polygon_method,
            raster_cell_size=region_raster_cell,
            raster_close_cells=args.region_raster_close_cells,
            raster_simplify_cells=args.region_raster_simplify_cells,
            raster_smooth_cells=args.region_raster_smooth_cells,
            raster_min_area_cells=args.region_raster_min_area_cells,
        )

    # --- worlds_meta for the sidebar ----------------------------------------
    have = [c for c in META_COLS if c in worlds.columns]
    missing = set(META_COLS) - set(have)
    if missing:
        print(f"  note: worlds_search missing {sorted(missing)}; skipping those")
    meta = worlds.select(have).join(
        points.select("world_id"), on="world_id", how="semi"
    )
    # clean tags for display (author/content tags, prefix-stripped) if present
    if "tags" in worlds.columns:
        tag_map = worlds.select("world_id", "tags").join(
            points.select("world_id"), on="world_id", how="semi"
        )
        cleaned = [clean_tags(t) for t in tag_map["tags"].to_list()]
        tag_df = pl.DataFrame(
            {"world_id": tag_map["world_id"], "tags": pl.Series("tags", cleaned)}
        )
        meta = meta.join(tag_df, on="world_id", how="left")
    out_meta = args.out_dir / "worlds_meta.parquet"
    meta.write_parquet(out_meta)
    print(f"  wrote {out_meta} ({meta.height:,} rows, cols={meta.columns})")


if __name__ == "__main__":
    main()
